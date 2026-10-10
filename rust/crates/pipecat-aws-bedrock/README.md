# Native AWS Bedrock LLM

`AWSBedrockLLMService` is implemented by `BedrockProcessor`, backed by native
signed Bedrock `Converse` and `ConverseStream` REST APIs. It is not an OpenAI
preset. Python source is pinned to
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`; `source-manifest.json` binds service,
adapter and upstream regression inputs to SHA-256 hashes.

## Runtime

Construct `BedrockConfig::new(AwsCredentials::new(...), region)`, then
`BedrockClient::new(config)`. Credentials are injected by the application;
there are no environment/file/metadata/SSO providers, ambient proxies, automatic
credential refresh or credential persistence. Production endpoints must match
the regional Bedrock origin. Literal IP loopback origins are allowed for tests.
Redirects are disabled. Commercial/GovCloud and China endpoint suffixes are
supported; custom remote endpoints, isolated partitions and VPC endpoints are
not supported.

`BedrockClient::stream(CompletionRequest)` returns a bounded, owned
`BedrockStream`. Dropping the stream aborts HTTP work; its sticky cancellation
token explicitly cancels work. `BedrockClient::converse` uses `/converse` for
out-of-band inference. `BedrockProcessor` implements `LlmInference` with the
shared explicit-context trait, immutable per-call settings, bounded output and
no tool execution. As in the pinned Python method, nonstream inference returns
the first nonempty text block and omits tool configuration, latency and caching.

Use `CompletionRequest::new("us.amazon.nova-lite-v1:0", context)` for the
source's default model. `BedrockSettings::defaults` supplies that same model as
a sparse `LlmSettings` value. Temperature, top-p and max-tokens map into
`inferenceConfig`. `LlmSettings.base.extra` accepts `stop_sequences`, `latency`,
`effort`, `enable_prompt_caching`, and `additional_model_request_fields`.
Null clears optional values; unknown settings fail atomically. Model and
transport credentials are not interchangeable runtime settings. Other generic
sampling settings are rejected rather than silently dropped.

The processor supports context/message replacement and append, message
transforms, tools/tool choice, output skip-TTS, prompt-cache toggles, targeted
settings, service metadata, health, FIFO request admission and flow fences.
End drains owned completion tasks; interruption/Cancel abort stale tasks. The
pipeline's external assistant aggregator owns history and tool execution.
Speculative contexts are rejected until this provider has its own owner/epoch
output gate. Generation is never treated as proof of spoken/playout delivery.

## Source-to-native map

| Source contract | Native implementation / gate |
| --- | --- |
| `_build_inference_config`, settings deltas | `settings.rs`, adapter sparse/null/atomic tests |
| `_should_inject_trailing_user_message` | Frozen legacy Claude prefill patterns in `settings.rs` |
| adapter system extraction and same-role merging | `adapter.rs`, source-system regression |
| adapter tools and tool results | Converse `toolSpec`, `toolUse`, `toolResult`; indexed parallel-tool tests |
| no-tool declarations after tool history | Source `no_operation` placeholder and suppressed returned calls |
| prompt-cache checkpoints, performance and effort | `system`/`toolConfig.tools` cachePoint, `performanceConfig`, `outputConfig` |
| image/document/video and S3 sources | Strict base64 REST blobs, S3 pass-through, MIME maps, document-name sanitization |
| stream response | CRC-checked AWS EventStream, indexed content blocks and modeled exception envelopes |
| `run_inference` | Real nonstream `/converse`, first nonempty text, inference trait loopback gate |
| usage totals | Input + output + cache read + cache write; cache-only reports retained, zero totals omitted |
| native signed history | Typed AWS-specific reasoning/redacted parts and local continuation carriers |
| signature safety | Shared SigV4, fake-key independent HMAC/ARN-encoding vectors, credential Debug redaction |

`ToolChoice::None` retains the source behavior: omit `toolChoice`, leaving
Bedrock's provider default when tools are declared. It is not a guarantee that
no tools are requested. `Required` additionally maps to Bedrock's `any` choice.
A forced function must be declared. Internal no-op tool history remains replayable
but never admits an executable call.

Inline images accept data URLs. Documents and videos use `ContentPart::File`
with base64 `data`, MIME and optional filename. An explicitly supplied `s3://`
value in this Bedrock file field becomes a native `s3Location` without fetching
it locally. Arbitrary remote file URLs are rejected; callers must resolve bytes
themselves. Audio input is rejected, matching the pinned Converse adapter.

## Protocol and security

`pipecat-aws-common` contains the shared explicit credential type, SigV4
primitives and bounded EventStream codec. AWS speech preserves its public
credential/error APIs through compatibility wrappers and retains its existing
loopback/vector fixtures. Bedrock depends only on the common primitives, not
on speech transports. Bedrock signs service `bedrock`, encodes the model route
once on the wire and again in SigV4's canonical URI, including model/profile
ARN separators. Authorization and session-token headers are marked sensitive.
Errors omit raw provider bodies, URLs and credential values.

The HTTP parser validates every 12-byte prelude CRC and frame bound before
allocating the declared frame size, then verifies message CRC and headers.
Arbitrary HTTP chunk fragmentation/coalescing is supported. Truncated records,
missing message completion, mixed/unknown content unions, duplicate block/tool
identities, invalid argument JSON, out-of-order completion, unknown events and
late exceptions fail closed. Tool calls are admitted only after an explicit `tool_use` messageStop and
validated HTTP EOF, and only for the immutable request's declared tools and
forced function choice. Filtered, truncated, malformed, unknown and unoffered
tool groups fail closed. A model error after a complete tool block cannot
execute that tool. Prepared-request Debug output also redacts the entire body. Requests, events, full responses, retained output, queue
capacity, connect, idle and whole-request duration all have finite bounds.

An optional single header-timeout retry uses a fresh signature and immutable
request snapshot. Both attempts share the whole-request deadline; no retry is
performed after headers or streaming output. Retry may duplicate provider work
and is disabled by default.

Reasoning signatures and redacted byte blocks are retained in ordered,
AWS-specific native context parts, never mislabeled as Anthropic history. Only
contiguous closed signed/redacted prefixes become interruption checkpoints.
Partial signatures never enter replay history. Unsigned reasoning is preserved
at complete message boundaries. Native carriers enforce byte/block/signature
limits and reject generic wire serialization. All native providers share a
checked response-ID allocator, which fails closed on exhaustion rather than
wrapping into previously used IDs. These IDs track ownership, not cryptographic
provider authentication. Foreign providers reject AWS
history explicitly. Assistant-history reconciliation uses delivered-text
receipts and preserves exact reasoning text/signatures and redacted bytes.

## Verification and limits

Run `python3 rust/crates/pipecat-aws-bedrock/scripts/check_source.py`, then run
`cargo test -p pipecat-aws-common -p pipecat-aws-speech -p pipecat-aws-bedrock` and
the affected core/services/turns/network-bus/responses suites. Synthetic fixture
peers use fake keys and an independent bitwise CRC/HMAC implementation. They
exercise real loopback HTTP and native pipeline paths. No live AWS access,
billable call, IAM mutation or model-quality claim is part of these tests.

This crate does not implement Nova Sonic, Bedrock AgentCore, SageMaker,
InvokeModel, bidirectional audio, guardrail configuration, search-result or
citation output variants, remote file downloads, model discovery, region/model
entitlement checks, or server-side constrained JSON output. Unsupported response
variants fail explicitly. The pinned Python wrapper's invalid-file mutation of
an external context, interruption token estimates, tracing callbacks and Python
SDK constructor/deprecation compatibility are not mirrored. A failed native
request never silently deletes application-owned history. Model-specific
parameter validity and actual AWS acceptance remain provider-side checks.

Primary protocol references:

- https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ConverseStream.html
- https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_Converse.html
- https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ReasoningTextBlock.html
- https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ContentBlockDelta.html
- https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
- https://smithy.io/2.0/aws/amazon-eventstream.html
