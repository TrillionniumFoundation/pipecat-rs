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

SSE supports LF, CRLF, CR, comments, UTF-8 BOM, multi-line data, and arbitrary
UTF-8 network boundaries. Incomplete records, invalid UTF-8/JSON, excessive event
size, invalid tool JSON, or missing terminal markers are errors. OpenAI requires
`[DONE]`, Anthropic requires `message_stop`, and Gemini requires a final finish
reason and clean SSE EOF. Tool fragments are grouped by index and validated as
JSON objects. No returned tool is executed by this crate.

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

- OpenAI Responses, Realtime, image/audio generation, STT, TTS, and legacy
  `function_call`; multiple completion candidates
- Anthropic server tools, redacted thinking, prompt caching policies, batches,
  and Files API lifecycle
- Gemini server code execution, multimodal output, live sessions, and Files API
  lifecycle; native signed text/tool continuation is described in [GEMINI.md](GEMINI.md)
- Opaque provider-specific tools/message extensions, remote file fetching,
  tool execution/registration/timeouts, and context repair after rejected files.
  Pipeline contexts carrying direct executable tool handlers require a separate
  executor and are rejected instead of silently dropping the handlers.
  Out-of-band `LlmInference::run_inference` advertises their schemas and returns
  text without executing tool calls or rerunning the model. Speculative contexts
  require the native turn-owner capability and integrated bounded `LlmProcessor` gate described in
  [EAGER.md](../pipecat-turns/EAGER.md); missing capabilities return an explicit
  error. Runtime prompt-caching controls and incomplete-user-turn filtering remain
  unsupported
- Authentication beyond explicitly supplied API keys and the application-supplied
  Azure token-provider mode documented in [AZURE.md](AZURE.md)
- Other Pipecat service families; compatible request presets do not imply those
  providers' STT, TTS, realtime, or SDK-specific features are implemented

Ordinary native `Provider::OpenAi` context frames, tool setters and out-of-band
inference select only the OpenAI entry in a `ToolsSchema` custom map, matching
the Python OpenAI adapter. Other adapters' declarations are ignored while the
standard function definitions and tool choice remain intact. Ordinary Gemini
paths likewise select only the Gemini entry and ignore OpenAI declarations.
Owning Gemini entries support the empty Google Search declaration described
below; other built-in tools and search configurations remain `Unsupported`.
Ordinary Direct handlers still require an executor, while
inference only advertises their definitions. Ordinary Anthropic paths ignore the
custom map entirely, matching the source adapter's standard-function-only
conversion; this forwards no opaque declarations. Public provider-agnostic conversion
and speculative/confirmed-owner context paths keep their existing
strict behavior. Message and image conversion are unchanged.

Ordinary OpenAI context frames, tool setters and inference retain owning custom
declarations in validated `OpenAiToolDeclarations`, separately from function
definitions. `DeclaredRequest` and the client's additive `request_body_declared`,
`stream_declared` and `stream_declared_with_cancel` APIs append each original
dictionary after standard functions. Existing `CompletionRequest` and `Context`
fields and client method signatures stay compatible. Declarations are limited to
64 KiB of JSON and the core argument depth limit; a custom-bearing request allows
128 standard functions plus declarations in total. Each dictionary requires a
nonempty string `type`. Empty declaration wrappers preserve the original behavior.

This advertises declarations without registering callbacks or decoding provider
managed tool responses. Nonempty declarations currently require an ordinary
OpenAI client; Azure and compatible presets remain unsupported. Reserved `extra`
fields retain their validation. Public function-only context helpers and
sealed/speculative paths retain their strict custom-schema
checks. Settings and message updates preserve admitted declarations; a complete
context or tool setter replaces them only after full request validation.
`declared_request_snapshot()` includes the full validated OpenAI declarations, while
the compatible `request_snapshot()` returns the base function context.

Ordinary Gemini context frames, setters and inference accept exactly
`{"google_search": {}}` as a custom declaration. The locked Python google-genai
2.25.0 SDK maps it to `{"googleSearch": {}}` and moves the merged standard
function group after built-in tools. Native requests follow that declaration
mapping and order, retaining the source adapter's standard-schema adaptation.
This does not establish full SDK request equality or live model support for a
search/function combination. Nonempty search options, other tool keys, Vertex
tools and arbitrary dictionary forwarding remain unsupported.

Gemini native standard-function declarations also canonicalize recognized
Schema `type` enums to their locked SDK representation, such as `OBJECT`,
`ARRAY` and `STRING`. This runs only at Schema nodes and their `properties`,
`items` and union descendants, including adapted union members. Property names,
enum values, default data and other text are retained. The public
`adapt_gemini_tool_schema` helper still returns the source adapter's SDK-input
schema and notes; universal input, other providers and unknown type strings keep
their existing behavior. This mapping does not establish server acceptance or
extend search configuration, built-ins, Vertex or managed-tool response support.

At those Schema nodes, `anyOf` and `propertyOrdering` use the locked SDK's native
field names `any_of` and `property_ordering`. Existing snake-case fields are
accepted, and their union members also receive type canonicalization. Parameter
names, ordering-list contents and default dictionaries retain their original
keys and values. Supplying both spellings of either mapped field at one node is
an error before HTTP or state publication, matching the SDK's dictionary-input
validation. The public source adapter still returns its original field names
and notes. Other Schema aliases and general SDK validation remain outside this
limited mapping; it does not establish complete request or cloud-provider parity.

The additive `GeminiToolDeclarations` and `GeminiDeclaredRequest` types and client
`request_body_gemini_declared`, `stream_gemini_declared` and
`stream_gemini_declared_with_cancel` methods retain declarations separately from
functions. Their private validated contents, count/JSON bounds and combined
128-tool limit protect publication; empty wrappers preserve existing behavior.
The original OpenAI wrapper constructor, accessors and two-part extraction are
unchanged. `gemini_declared_request_snapshot()` returns the complete Gemini
request; a provider-specific snapshot rejects the other provider's nonempty
declarations explicitly. Message/settings updates retain metadata, and complete
context/tool changes replace or clear it only after validation. Both wrappers
share the original HTTP cancellation and decoder path. Search advertisement
registers no callback and adds no grounding response decoding or execution.

`preflight_context` and `preflight_tools` synchronously validate a projected
ordinary update against the processor's current base settings and actual client.
They perform no I/O, callbacks or state mutation. Fixed MCP/Direct owners use
this before publishing standard handler selections or cleanup hooks, so supported
owning declarations can accompany actual standard tool results. The check is a
current-state snapshot; it does not make later settings updates and separate
pipeline stages one transaction. Legacy paths and switchers retain their previous
validation boundaries.

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
