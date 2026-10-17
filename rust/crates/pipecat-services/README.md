# pipecat-services

Native Rust HTTP streaming clients for these specific protocols:

- OpenAI Chat Completions (`POST /v1/chat/completions`)
- NVIDIA NIM Chat Completions with leading-tag and structured reasoning ([scope](NVIDIA.md))
- Sarvam Chat Completions with model capabilities, dual authentication and v1/v2 routing ([scope](SARVAM.md))
- Azure OpenAI Chat Completions (v1 or dated deployment routes; [details](AZURE.md))
- Anthropic Messages (`POST /v1/messages`)
- Gemini `streamGenerateContent` with `alt=sse`

A configurable OpenAI-compatible endpoint uses the Chat Completions protocol;
this is not evidence that every vendor's extensions are supported. No Python
process, SDK, interpreter, or runtime is invoked.

## Client API

```rust,no_run
use pipecat_services::{ClientConfig, CompletionRequest, Context, LlmClient,
    Message, Provider, Role, StreamEvent};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
// Applications supply their own credentials explicitly. Nothing reads env keys.
let client = LlmClient::new(ClientConfig::new(Provider::OpenAi, "YOUR_KEY"))?;
let request = CompletionRequest::new("YOUR_MODEL", Context::new(vec![
    Message::text(Role::User, "Hello"),
]));
let mut stream = client.stream(request)?;
while let Some(event) = stream.next().await {
    match event? {
        StreamEvent::TextDelta(text) => print!("{text}"),
        StreamEvent::ToolCall(call) => println!("Validated tool: {}", call.name),
        _ => {},
    }
}
# Ok(()) }
```

`ClientConfig.endpoint` is an exact POST URL. Gemini's default endpoint replaces
`{model}` with a validated model name. HTTPS is required except for HTTP loopback
simulation. API keys are transmitted in sensitive headers and redacted from
configuration debug output. Default clients do not follow redirects. For generic clients, a custom
`reqwest::Client` can be passed with `with_http_client`; its caller owns proxy,
redirect, and TLS configuration. Requests are not retried by default.
`RetryPolicy::OnceOnHeaderTimeout { timeout }` explicitly opts into one retry
before response headers arrive. The retry uses the same serialized body and
headers (Azure Entra tokens are refreshed per attempt), never retries streaming output or HTTP status failures, and remains
bounded by the original total request timeout. A timed-out first attempt may
already have caused billable provider work.

`stream_with_cancel` accepts a shared, sticky `CancellationToken`.
`LlmStream::cancel` or dropping the stream cancels network work. The event channel
is bounded and applies backpressure. Total request timeout includes waiting for
response headers, network reads, and a blocked consumer; idle timeout applies to
network chunk reads. A successful HTTP status must have `text/event-stream`.

Every started stream emits `Started`, zero or more deltas, the latest cumulative
`Usage` once if available, then `Finished`. A failure is an `Err` item before
`Finished`; consumers must not interpret `Finished` alone as success. Partial
usage survives explicit cancellation, timeouts, and protocol failure. Dropping
the receiver intentionally prevents delivery of remaining events.

TTFB measures the first successfully decoded model event, not the first TCP
byte. Emitting the metric requires `enable_metrics` (disabled by default). The
processor's timer starts at `process_completion` entry, including request
preparation and `ResponseStart` delivery. Plain OpenAI Chat Completions
(including shared Azure/Sarvam and other compatible presets) emits one
`ResponseProgress` for its first accepted nonempty `choices` array, including
role-only, empty-delta, and partial-tool packets. Anthropic emits it for its
first accepted `content_block_start` or `content_block_delta`, including empty
text, thinking, redacted-thinking, and tool starts. Ordinary Gemini
`streamGenerateContent` emits it for its first accepted nonempty `candidates`
array, including candidates without parts, empty text, thoughts, tool calls,
and finish-only candidates. Empty choices or candidates, usage-only records,
pings, and rejected records do not qualify. Progress is committed only after
all record validation, including aggregate tool-byte limits, succeeds.

The decoder emits progress once per response; the processor also deduplicates
TTFB across progress and content events. This is a native adaptation of the
Python adapters' repeated stop-TTFB calls. The reference Gemini adapter stops
TTFB for each nonempty candidate chunk before inspecting parts; the native
decoder instead requires the whole record to pass validation first.
Progress precedes content from the same record and has no response/history
side effects; cancellation under backpressure can deliver only a prefix of that
record's events. NVIDIA retains its existing reasoning-progress path, and Gemini
Live is unchanged.

Ordinary Gemini emits a separate `ThoughtStart`, `ThoughtDelta`, `ThoughtEnd`
sequence for each nonempty thought text Part, matching the fixed Python
`google/llm.py` baseline. Empty thought text emits no display events; whitespace
is preserved. Adjacent thought Parts remain separate even within one SSE record.
These boundaries do not commit signatures or history: the existing Gemini
checkpoint remains after all accepted Parts in that record, and completed
history still requires a successful response.

If cancellation, timeout, or failure interrupts event delivery, Gemini closes
only unclosed thought starts already queued in the bounded client channel,
before final usage, error, and finish events. Queuing is not a processor or sink receipt.
The consumer must keep draining to receive this closing boundary; dropping the
receiver or aborting a pipeline task does not guarantee a delivered thought end.
Rejected records enqueue none of their temporary display events.

SSE supports LF, CRLF, CR, comments, UTF-8 BOM, multi-line data, and arbitrary
UTF-8 network boundaries. Incomplete records, invalid UTF-8/JSON, excessive event
size, invalid tool JSON, or missing terminal markers are errors. OpenAI requires
`[DONE]`, Anthropic requires `message_stop`, and Gemini requires a final finish
reason and clean SSE EOF. Tool fragments are grouped by index and validated as
JSON objects. No returned tool is executed by this crate.

OpenAI Chat audio transcripts require an explicitly selected audio-capable model
and caller-supplied `extra` parameters such as `modalities` and `audio`; defaults
do not enable audio generation. A nonempty `delta.audio.transcript` becomes an
ordinary text delta only when the same choice has neither nonempty tool calls
nor visible content. NVIDIA applies its reasoning filter before that decision.
Empty tool arrays do not suppress transcripts; whitespace is preserved. Missing,
null or metadata-only audio objects emit no text. Unlike Python's permissive
truthiness, a selected non-object audio value or non-string transcript is a
protocol error (null is allowed). A finish reason does not discard the same
chunk's transcript or replace the required `[DONE]` marker.

Transcripts use the existing LLM text/`skip_tts` path and external assistant
aggregation, not an audio output path. Generated `data`, `id` and `expires_at`
are ignored: there is no audio decoding/playback or audio-ID history replay.
This shared wire contract does not establish audio-model support for every
compatible preset or live-provider acceptance.

## Context conversion

`Context` owns messages, optional function tools, and tool choice. Conversion is
pure and does not mutate the source. Supported inputs include text, assistant
tool calls, tool results, image URLs, and inline data with provider-specific MIME
limits. OpenAI prepends a configured system instruction; Anthropic/Gemini extract
the initial system message and allow a configured replacement. Initial system
text parts are joined with spaces. A sole system message stays as a user turn
to avoid an empty provider history. Gemini call and result IDs are preserved.
Adjacent Anthropic/Gemini roles are merged. Empty Anthropic content becomes `(empty)`.

Anthropic maps calls/results to `tool_use`/`tool_result`; Gemini maps them to
`functionCall`/`functionResponse`. Tools use the provider's actual schema, not a
common serialized shape. Gemini remote image URLs are rejected; callers may
supply inline bytes. Rich core text/image/tool parts are retained. OpenAI image
detail, WAV/MP3 input audio, files, and assistant refusals have explicit wire
forms. Gemini audio/files use typed inline data. Anthropic supports images/PDFs
and caller-provided signed thinking. Unsupported provider/part combinations,
opaque message extensions, and non-text initial system content return errors
instead of being dropped. `extra` forwards provider parameters but cannot replace
reserved fields that determine identity, context, tool routing, or streaming.

Gemini tool schemas follow the checked-in Python adapter's recursive adaptation:
union types become `anyOf`; `additionalProperties`, `x-` schema extensions, and
non-string enum constraints are removed. Parameter names remain intact, even
when they match those keywords. `adapters::adapt_gemini_tool_schema` returns the
adapted schema and explicit notes about changed or lost constraints for callers
to inspect. Other fields are retained. An explicitly empty tool list stays empty.
Scalar/list/plain-string tool results use the source's `{"value": ...}` wrapper.
Trailing model turns gain a `.` user turn unless the resolved model matches the
source's frozen legacy-prefill prefixes; this does not probe live capabilities.

Usage follows the Python regression contracts: OpenAI snapshots are not summed;
Anthropic cache input counts are included in prompt totals; Gemini thoughts
count as output and tool-use prompt tokens count as input. Cache and reasoning
counts remain optional instead of inventing measured zeros.

Anthropic `message_start` and `message_delta` usage fields are cumulative
snapshots. Each reported `input_tokens`, `output_tokens`,
`cache_read_input_tokens` or `cache_creation_input_tokens` replaces its previous
value, including zero, decreases and repeats. Missing or null fields retain the
last value. Missing/null usage leaves the snapshot unchanged; an empty start
usage object retains the existing zero-snapshot initialization, while an empty,
unknown-only or all-null delta does not create usage when none was reported.
The decoder keeps uncached input separately and recomputes native prompt totals
from that input plus both latest cache counts, using the existing saturating
unsigned-64-bit additions. Total adds output to that gross prompt. Later lower
counts can therefore recover from earlier saturation without subtracting cache
from an already saturated value.

Anthropic `output_tokens_details.thinking_tokens` updates the existing optional
`Usage.reasoning_tokens`; thinking is already included in output and is not
added to completion or total again. Missing/null `output_tokens_details` retains
the earlier reasoning count. Native validation is deliberately stricter than
Python's SDK-object/truthiness path: non-null usage must be an object, each
reported count must be a valid unsigned 64-bit integer, and a supplied details
object must contain a valid unsigned `thinking_tokens`. An empty details object,
a missing/null thinking count, wrong types, negative or out-of-range counts are
protocol errors. Unknown usage fields are ignored. The entire usage update is
validated before changing any counts or that delta's finish reason; an invalid
record poisons the decoder and leaves the last successful snapshot available
for the existing final usage report.

The pinned Python
[`_process_context` usage update](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/anthropic/llm.py#L626-L645)
provides the cumulative replacement and thinking-as-output contract. Its
[`_report_usage_metrics`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/anthropic/llm.py#L726-L756)
reports a net-of-cache prompt and includes cache in total; native retains its
existing gross prompt convention and saturation. Native also retains the latest
provider snapshot once on cancellation/failure rather than adopting Python's
cancellation-time completion estimate. This changes no public Usage fields,
core metric shape, stream completion markers or other providers' accounting.

## Sparse runtime settings

`CompletionRequest.settings` is the core `LlmSettings` type. `Setting::NotGiven`
leaves an existing field alone; `Null` explicitly clears a system instruction or
sends a JSON null for a supported optional OpenAI/Anthropic parameter; Gemini
clears and omits null generation settings, matching the Python builder. `Value`
replaces it. A null model or Anthropic token limit is invalid. Deltas merge extra fields
key by key. Temperature/top-p/penalties are range-checked, token limits must be
positive, and unsupported controls produce errors. Gemini sampling settings go
inside `generationConfig`; Anthropic uses its own supported fields.

The original convenience fields remain available. Given sparse settings take
precedence. In particular, `request.max_tokens` is the original portable budget
(mapped to OpenAI `max_completion_tokens`), while `request.settings.max_tokens`
follows the Python settings field (OpenAI `max_tokens`). A given sparse budget
supersedes the convenience budget. Provider-specific fields can use explicit
`extra` fields, subject to reserved-field and collision validation.

## Compatible-provider request presets

`ClientConfig::openai_compatible(OpenAiPresetConfig::new(preset), key)` selects
source-backed request semantics as well as the exact endpoint. `ClientConfig::ollama()` selects the local endpoint and public dummy token.
The preset's `request(context)` supplies the model default pinned in the Python source.
Defaults are source snapshots, not recommendations about currently available
models. `LlmClient::request_body` validates/serializes without making a request.

There are 20 executable presets including OpenAI itself: Baseten, Cerebras,
Crusoe, DeepSeek, Fireworks, Groq, Inception, Mistral, Nebius, Novita, NVIDIA, Ollama,
OpenRouter, Perplexity, Qwen, SambaNova, Sarvam, Together, and xAI. The deprecated `grok`
source module is the xAI re-export. Presets include developer-role handling,
provider token-field and stream-usage differences, typed reasoning/routing
options, DeepSeek reasoning stamps, Mistral/Perplexity message transformations,
OpenRouter Gemini system-message handling, and xAI reasoning usage normalization.
SDK `extra_body` fields are flattened with provider-option precedence while
reserved streaming/context fields remain protected.

Sarvam uses model-dependent routing and resolved request capability validation.
NVIDIA uses the same native wire client
with an incremental reasoning adapter. These are request presets and bounded
response adapters, not 20 complete provider SDK ports.
Citation/search metadata, tool execution, provider sessions, authentication
variants, and the individual `unsupported_features()` lists remain boundaries.
Goldens and loopback requests verify source contracts without live-provider calls.

## Pipeline integration

`LlmProcessor::new(client, request)` implements `pipecat_core::FrameProcessor`.
It consumes legacy and detailed context/message frames, `LlmRun`,
`LlmMessagesTransform`, `LlmSetTools`, `LlmSetToolChoice`, `LlmConfigureOutput`,
`LlmUpdateSettings`, and generic service updates. Context and run frames trigger
completions from either direction, including upstream tool-result continuations.
Settings targeting another service retain their direction; local targets compare
processor identity, not only names. Other unhandled frames retain their direction.
Settings/context mutations validate the body and model-dependent endpoint and
commit atomically; errors leave prior state intact. Each queued completion owns
an immutable request/output snapshot,
so updates affect only subsequently launched requests, including during retries.
`request_snapshot()` returns a clone; `update_settings()` validates and merges a
typed delta without starting inference. Message transforms run outside the state
mutex and preserve unchanged native message representations, including duplicate
occurrences. Partial selection of distinct native messages with identical core
projections is rejected when the transform cannot identify which one survived.
Tracked
background completions preserve request FIFO without blocking the processor's
control lane. Interruption aborts
the tracked task and dropping its client stream closes the HTTP connection.
End drains active work; Cancel aborts it.

Output includes response start/end, LLM text, thought display frames,
validated `FunctionCallsStarted`, optional latency/processing/usage metrics, and
nonfatal upstream categorized errors. Tool frames are emitted only after the
whole completion succeeds. Detailed usage is preserved in the metrics frame's
`llm_usage` metadata; `finish_reason` and `failed` are response-end metadata.
An external aggregator owns conversation history. The processor does not append
assistant messages or execute tools. Core interruption resets the pipeline;
there is no asynchronous response-end or final usage emission after a task has
been aborted. Direct client cancellation preserves usage; pipeline task abort
cannot deliver events from the dropped consumer.

## Verified scope and gaps

The tests use synthetic records and actual TCP loopback HTTP servers. They do
not contact paid services or prove live provider compatibility. They cover
byte-fragmented Unicode/SSE, interleaved tools, body/auth headers, early token
delivery, cancellation before connection and mid-stream, receiver drop/socket
closure, idle/header timeouts, HTTP errors, malformed/truncated streams, usage
accounting, context conversion, and real pipeline interruption.

Unsupported features are not labeled as implemented integrations:

- OpenAI Responses, Realtime, image generation, generated-audio decoding/playback
  or audio-ID history replay, STT, TTS, and legacy `function_call`; multiple
  completion candidates
- Anthropic server tools, automatic caching policies, batches,
  and Files API lifecycle
- Gemini server code execution, multimodal output, live sessions, and Files API
  lifecycle; native signed text/tool continuation is described in [GEMINI.md](GEMINI.md)
- Opaque provider-specific tools/message extensions, remote file fetching,
  tool execution/registration/timeouts, and context repair after rejected files.
  Direct executable tool handlers require a separate executor and are rejected
  instead of silently converted to schemas. Speculative contexts require the native
  turn-owner capability and integrated bounded `LlmProcessor` gate described in
  [EAGER.md](../pipecat-turns/EAGER.md); missing capabilities return an explicit
  error. Incomplete-user-turn filtering remains unsupported. Explicit Anthropic
  prompt-cache breakpoints and controls are described in [ANTHROPIC_CACHE.md](ANTHROPIC_CACHE.md)
- Authentication beyond explicitly supplied API keys and the application-supplied
  Azure token-provider mode documented in [AZURE.md](AZURE.md)
- Other Pipecat service families; compatible request presets do not imply those
  providers' STT, TTS, realtime, or SDK-specific features are implemented

Primary wire references:
[OpenAI Chat](https://developers.openai.com/api/reference/resources/chat),
[Anthropic streaming](https://platform.claude.com/docs/en/build-with-claude/streaming),
[Gemini generateContent](https://ai.google.dev/api/generate-content).
Source behavior references include `tests/test_openai_compatible_token_usage.py`,
`tests/test_google_token_usage.py`, `tests/test_google_finish_reason.py`, and
`src/pipecat/adapters/services/{open_ai,anthropic,gemini}_adapter.py` in the Python
source tree. These are selected contract ports, not the complete Python suite.

Run `cargo test -p pipecat-services` from `rust/`.

### Metadata, health, and explicit inference

`LlmProcessor` broadcasts `LlmServiceMetadata` in both directions after Start
and on a matching `ServiceSwitcherRequestMetadata` request. It reports its real
name and non-realtime capability; it does not start inference to refresh metadata.
The processor exposes a stable `ProcessorHealth` handle. While unusable, context,
run, and message-mutation work is gated; settings, metadata, system/lifecycle
frames remain available. Rejected settings proposals preserve the last valid
configuration and existing health without changing the InvalidRequest category.
Recovery is explicit through the health handle, never automatic replay.

`LlmInference::run_inference(context, InferenceOptions)` makes an independent
provider inference through the existing native client. It snapshots service
settings, replaces only the per-call context, and applies sparse per-call settings
overrides. It never changes the pipeline conversation or executes tool callbacks.
Text accumulation is bounded by `max_output_bytes` (1 MiB default, positive),
with transport/event/time limits inherited from ClientConfig. A stream error
returns an error even after partial text; a tool-only reply returns None.

The caller owns the future. Dropping it cancels its HTTP stream; pipeline
interruptions and cleanup do not own this out-of-band operation. An in-flight
call keeps its selected member and settings snapshot even when a surrounding
`pipecat-pipeline::LlmSwitcher` changes selection. The native one-shot capability
consumes bounded SSE rather than implementing a second nonstreaming provider
transport. No implicit failover/replay is performed; client retry behavior remains
whatever the caller explicitly configured.


### Anthropic signed continuation

The decoder retains opaque provider signature fragments exactly with their
indexed text/thinking/tool block order. Structural checks do not cryptographically
verify Anthropic signatures. Malformed, missing or oversized signatures poison
the response; no signature is synthesized. Concurrent thinking blocks are an
explicitly unsupported display-nesting contract. Outstanding text with successive
thoughts, empty thoughts and indexed parallel tools are supported.

Native continuation carriers accompany response starts and completed histories.
Closed contiguous prefixes use the neutral `LlmContinuationCheckpoint` frame,
queued before the corresponding real thought-end boundary. Only a successful
completed response admits tools. `LLMAssistantAggregator` consumes the carriers
and reconciles ordered history against delivered text; it is required for replay.
Wire encoders refuse unconsumed carriers and bare native checkpoint frames.
Clones retain native semantics; default summaries and Debug redact signatures.
Explicit history/request APIs necessarily expose replay content.
`request_body_for_logging` redacts signatures and inline media, not prompts,
thought text or tool arguments.

History is bounded to 1024 blocks and 8 MiB, with 1 MiB per opaque signature.
Core carriers count actual retained capacities and nested tool arguments;
byte-budget queues account carrier retention. Tool admission validates complete
batch identity before any effect, rejects mixed duplicates atomically, and
retains at most 8 MiB of signed replay history within the configured bounded
`duplicate_window`. Exact replays within that window forward completion without
repeating effects.

`tests/fixtures/anthropic_signed_history.json` records pinned Python revision
`af9f9da9d7afddaa8811c0e0251b32566c980bdf` and verified service/adapter/test hashes.
Fixtures contain synthetic opaque signatures and do not claim provider authenticity.
Real TCP/SSE tests cover native tool results in a second request, cancellation
during a later signature and rejected-delivery recovery followed by a clean
request. No Cargo test invokes Python or contacts a paid provider.

Azure requires `ClientConfig::azure(AzureConfig::new(endpoint, auth)?)` rather
than the generic preset constructor. Its private transport disables proxies,
redirects and internal retries, rejects custom clients, and redacts provider
diagnostics. See [AZURE.md](AZURE.md) for authentication and scope.

## Explicit Anthropic cache breakpoints

`AnthropicPromptCachePolicy` and `CompletionRequest::with_anthropic_prompt_cache`
opt into the fixed source's system-prefix and last-two-user-message breakpoints.
The existing `LlmEnablePromptCaching` control updates future request snapshots
without starting inference. Defaults remain unchanged; internal policy settings
never leak to provider fields. See [ANTHROPIC_CACHE.md](ANTHROPIC_CACHE.md) for TTL,
validation, signed-history preservation, source boundaries and local-only tests.

### Anthropic redacted thinking

`redacted_thinking` blocks now retain their opaque `data` string through indexed
SSE history, native continuation carriers and the assistant context, then replay
it unchanged in the next Anthropic request. They produce no thought or speech
text. See [the redacted-history contract](ANTHROPIC_REDACTED.md) for bounds,
checkpoint ownership, unsupported destinations and source differences.
