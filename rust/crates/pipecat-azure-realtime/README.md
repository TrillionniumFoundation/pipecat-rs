# Azure OpenAI Realtime GA: native PCM application

`pipecat-azure-realtime` is application wiring over the existing
`pipecat-realtime` GA WebSocket actor, frame processor, context adapter and final
tool-result relay. It is **not** a second protocol implementation, Azure SDK,
Python bridge, preview client or Azure Voice Live client. The façade feature is
`pipecat/azure-realtime`.

## Configuration and authentication

```rust,ignore
use pipecat_azure_realtime::{AzureRealtimeCredential, RealtimeConfig};

let config = RealtimeConfig::azure(
    "wss://RESOURCE.openai.azure.com/openai/v1/realtime",
    "EXPLICIT-DEPLOYMENT",
    AzureRealtimeCredential::ApiKey(supplied_key),
)?;
// Or AzureRealtimeCredential::EntraBearer(supplied_existing_token).
```

The constructor returns `Result<RealtimeConfig, RealtimeError>` and validates:

- An explicit deployment, at most 256 bytes. There is no default Azure
  deployment. An outgoing session model, when present, must equal this
  deployment. Editing public configuration cannot change the connection's
  selected deployment; construct a new configuration.
- The exact `/openai/v1/realtime` endpoint path. Production requires `wss`.
  `ws` is accepted only for literal loopback IPs, for synthetic local peers.
  Endpoint strings are limited to 8192 bytes. Userinfo, fragments, unsupported
  schemes, remote plaintext, preview `/openai/realtime` and
  `/voice-live/realtime` fail validation before connecting.
- Either a query-free endpoint, to which one safely encoded `model` value is
  appended, or exactly one existing `model` query value equal to the explicit
  deployment. Empty, duplicate, conflicting or unknown query parameters,
  including `api-version`, are rejected rather than dropped or reinterpreted.
- Exactly one supplied credential variant. `ApiKey` sends only `api-key`;
  `EntraBearer` sends only `Authorization: Bearer …`. The other credential
  header and obsolete beta header are absent. Credentials must be nonblank,
  at most 16384 bytes and contain no control characters.

Both headers are marked sensitive. Configuration/credential Debug output
redacts secrets; handshake errors exclude the response body, headers and
configured URL. Typed provider errors redact the configured credential.
Redirects are not followed. There is no credential fallback, token callback,
Azure CLI, managed identity, credential chain, OAuth, minting, refresh, token
storage or reconnection. The caller owns a supplied token's lifetime.

This intentionally differs from the pinned Python subclass, which preserves
arbitrary existing query strings, merely warns for `api-version`, and selects
its async token provider ahead of a supplied key. The native contract rejects
unsupported endpoints and makes authentication selection explicit. The example
also rejects both credential environment variables instead of choosing one.

`RealtimeConfig::new(key)` remains the existing OpenAI constructor, with its
OpenAI endpoint/model, bearer header and strict query-free endpoint validation.
That existing custom-endpoint bearer path could already mechanically carry a
supplied Azure token; this adds explicit Azure validation, API-key selection
and the tested application path, not a claim that every Azure connection was absent.
The Azure app rejects an ordinary OpenAI configuration. Server-reported model
aliases do not rewrite the chosen Azure deployment.

## Public application ownership

```rust,ignore
use pipecat_azure_realtime::AzureRealtimeApp;
use pipecat_services::Context;

let app = AzureRealtimeApp::new(config, Context::default(), vec![], output)?;
let running = app.start().await?;
let sender = running.worker.sender();
```

`output` is an `Arc<dyn pipecat_core::FrameProcessor>`. The constructor builds
one real native pipeline:

```text
ingress → paired user → binding guard → End gate → shared GA realtime processor
        → provider admission ack → binding guard → native ToolExecutor → final result relay
        → caller output → paired assistant → ingress
```

It seeds the same safely projected initial context into the pair and provider
using `with_initial_context`, without requesting inference. The app's raw
pipeline is private, so its supported entry is `start()`. The returned
`AzureRunningApp` exposes `worker`, `pair`, `provider` and `outcomes`.
Its `AzureRealtimeHandle` provider handle offers only `acknowledge_playback`
and `session_joined`, not a raw sender or settings/tool mutation path.
The worker owns the pipeline and provider tasks. Each new app has a new pair,
context and connection; a stopped app is not restartable.

Start admits initial history before caller input and observes an ordered local
barrier. It is not a claim of remote readiness or provider acceptance. The
shared actor independently gates commands behind its bounded session-readiness
state. Service metadata identifies `AzureOpenAiRealtimeService`, while the
underlying implementation remains `OpenAiRealtimeProcessor`.

The bounded application profile is:

- Mono signed little-endian PCM16 at 24000 Hz in and out, with nested GA audio
  settings and audio output. `InputAudio` and `UserAudio` use the same provider
  path. Caller-supplied sample rates and channel counts must match.
- Explicit manual turns (`audio.input.turn_detection: null`) or
  `TurnDetection::ServerVad`. The app rejects semantic VAD and other output
  modalities; the shared low-level crate may have a wider independent API.
- Initial text history accepted without lossy conversion by the existing
  native adapter. Unsupported rich/opaque history fails before a connection.
  Ordinary live user-text appends are supported; history replacement,
  arbitrary transforms, raw injected provider context and tool ownership are
  not application inputs.
- Final-only, bounded, interruptible local callbacks. A new app is required
  for changed session settings, tool schemas, tool choice or history
  replacement. The ingress rejects unsupported controls instead of silently
  changing the declared application profile.

Integrity gates at both caller-facing pipeline boundaries validate native
carriers before an aggregator can turn their public payload into ordinary
context or input, including frames submitted upstream. Provider-adjacent guards
validate immutable tools/choice in pair-produced contexts in both directions.
Caller data/control input is downstream-only; direct upstream input is rejected at the tail
before the assistant can transform it. Ordinary Flush probes are the
exception because core routes their barrier round trip through both boundaries;
the reserved `assistant_turn_end` origin is rejected in both directions.
Downstream input uses an explicit
PCM/text/turn/lifecycle control allowlist, not arbitrary provider frames.
Mutating the public pair context does not authorize new tool bindings.

### Final local tools

Provide `AzureLocalTool { schema: FunctionTool, handler: RegisteredTool }`.
The schema and its registered handler must agree exactly, and the offered tool
set must equal the registered local callback set, not merely be a subset.
Schema parameters must be JSON objects; this is not full JSON Schema semantic
validation. Explicit context
tools take precedence, including an explicit empty list; otherwise session tools
are used, otherwise local schemas are offered. If context and session both
declare tools or choice, conflicting declarations fail. The supported choices
are auto, none and required, not a named function.

Callbacks must have cancellation on interruption enabled, a nonzero execution
timeout no greater than the response budget, and bounded schemas. There are
at most 128 local registrations and no more than the configured outstanding
tool limit. The native executor invokes callbacks, retains their actual
terminal receipts and exposes the outcome journal. This is process-local
admission/settlement ownership, not durable exactly-once external execution;
callbacks must themselves be cancellation-safe.

The shared relay delivers a validated final result upstream before forwarding
it to the assistant. Raw matching public result data is not proof of a valid
completion. Duplicate/conflicting receipts, unknown calls, argument/identity
mismatches and retagged carriers cannot gain settlement or inference authority.
The paired assistant owns continuation; final settlement can precede the
originating response's completion without creating a second overlapping
response. Progressive/intermediate tools, continuation checkpoints and delayed
noninterruptible callbacks are outside this app.

## Generated history, interruption and lifecycle

Assistant history is generated transcript history, not audible playback
history. The shared mapper emits `LlmText` as a non-context-appending notification
and `TtsText` for the one generated-text history update. A PCM file write and
`LlmResponseEnd` do not prove that audio was played.
Each provider transcription-completed event follows the inherited transcription
path. Deduplicating separately emitted transcript completions or replay after
reconnection is not part of this application contract.

A real playback transport must use actual validated output confirmation and
the provider's `acknowledge_playback` API for the corresponding item/content
offset. Interruption truncates only an acknowledged current playback position.
No receipt means no guessed played offset. The file example deliberately sends
no playback acknowledgments.

Manual start/stop boundaries drive local turns; server VAD owns its own
boundaries and must not also receive a local commit for the same turn.
Interruption, pre-roll, stale-generation fencing and final-tool continuation
remain the shared GA contract, not an Inworld-style approximation. See
[`AGGREGATION.md`](../pipecat-realtime/AGGREGATION.md) and
[`PREROLL.md`](../pipecat-realtime/PREROLL.md).

The app opts into a final-tool drain on the existing shared actor. End waits
at a gate immediately before the provider, leaving the provider's normal lane
free for durable native final receipts arriving from downstream. The
provider passes the gate's private local-identity admission probe, acknowledged
immediately after its normal lane and before any output or executor wait.
This finite barrier orders prior input/context actions before actor Finish;
an origin string or caller-created flush event cannot impersonate the probe.
Only the actor's actual response state and tool settlements determine completion, not
an empty callback journal or a second mirrored tool table. During draining,
final outputs remain admitted but deferred and new inference are suppressed.
Only already-announced calls are admitted to that drain; a newly announced
provider tool call after Finish is an explicit failure, not silent completion.
Unexpected peer close with an unsettled tool is a failure even after the
originating response has ended. Receive failures terminate this app; the
ordinary low-level OpenAI default behavior is unchanged.
Cancel uses the independent system lane, can supersede the bounded End wait,
closes promptly and fences obsolete output. A locally admitted write is not remote acceptance;
an ambiguous failure is never automatically retried. Startup, terminal and
cleanup failures remain failures, and the running worker must be joined.
Inspect the complete `WorkerReport` and tool journal, not only a final response
frame or an empty retained-error list. Startup retains bounded returned cleanup
errors (16 messages of at most 4096 characters) alongside the primary failure.
If core startup fails before returning a worker, a cleanup panic or timeout can
still be hidden behind core's primary error; the passive app collector observes
returned cleanup errors, not panics or aborted cleanup futures. No stronger
early-startup diagnostic guarantee is claimed.

Shared default bounds include 64 command/event slots, 1 MiB encoded messages,
192000 input bytes per append, 64 outstanding tools, 10 seconds for connection/
readiness, 5 seconds per operation, 120 seconds per active response and
2 seconds for socket close. The app rejects individual configured timeouts
above 600 seconds. Its pipeline startup envelope is twice the connection
budget plus one operation; the local startup barrier has its own finite
envelope. Shutdown uses three response budgets, two operations and socket
cleanup, rather than silently imposing core's shorter default.
These are component and phase bounds, not a single resident-memory or
end-to-end wall-clock guarantee for arbitrary caller output/callback code.

## Recorded PCM example

```text
cargo run -p pipecat-azure-realtime --example pcm_session -- --help
cargo run -p pipecat-azure-realtime --example pcm_session -- --check-config
cargo run -p pipecat-azure-realtime --example pcm_session -- --check-config --server-vad
cargo run -p pipecat-azure-realtime --example pcm_session -- --manual input.pcm output.pcm
cargo run -p pipecat-azure-realtime --example pcm_session -- --server-vad input.pcm output.pcm
```

Run these from `rust/`. Set `AZURE_REALTIME_BASE_URL` and
`AZURE_REALTIME_DEPLOYMENT`, plus exactly one of `AZURE_REALTIME_API_KEY` or
`AZURE_REALTIME_BEARER_TOKEN`. These are the only credential/configuration
variables read by the example. `--help` needs none. `--check-config` defaults
to manual mode, validates the same public constructor, opens no connection and
creates no input or output file. It does not verify a credential, entitlement,
deployment, voice or endpoint availability.

Session mode handles one recorded utterance, not a WAV decoder or device
transport. The input must be a regular, nonempty, even-length raw PCM16LE file
at most 8 MiB. Both paths are limited to 4096 bytes. Input, arguments, configuration
and application construction are checked before output creation. Output must
be a distinct new file and is created with `create_new`, never overwritten.
Existing symlink/hard-link targets cannot be overwritten. A runtime failure
can leave a new partial file; it is not deleted or reported as a successful
recording.

The example offers no tools and starts with empty text history. It sends
chunks of at most 960 bytes, paced at 20 milliseconds, while continuously
draining pipeline output, even if a response completes before the input tail.
Response completion is observed once at the actual recording output before the
paired assistant consumes `LlmResponseEnd`, not inferred from worker-boundary
frames. This local signal is not a playback receipt.

Manual mode supplies start and stop boundaries. A full normal-priority,
noninterruptible local flush round trip follows start and its generated
interruption before the first PCM frame. Another full round trip follows the
final PCM frame before the system-priority stop can commit the turn. Each
barrier has the existing finite operation budget and does not imply remote
audio acceptance. Worker output continues draining during both waits; the
response and individual operation deadlines are unchanged. Server-VAD input must include
enough end-of-speech silence; the example selects threshold 0.5, 300 ms prefix
padding and 500 ms silence, with automatic response/interruption enabled.
It neither synthesizes silence nor claims these values qualify real speech.

The feed/response deadline is the number of chunks times 20 ms, plus the
unchanged response budget and two operation budgets for manual mode's two
actual barrier phases. Server-VAD retains one operation allowance. This is
phase accounting, not an enlarged provider-response timeout.
Success requires input completion, a completed
response, nonempty aligned 24 kHz mono output, a clean joined terminal report,
joined provider actor, empty tool journal and no dropped frames.
The output is limited to 16 MiB. Output is drained and
discarded after recording rather than collected into an unbounded frame log.
Failure requests Cancel and still joins the worker; primary failure and
terminal/report failures are reported together with sanitized diagnostics.
No microphone, speaker, browser, public listener, STT/TTS sidecar or credential
discovery is used.

## Source provenance and evidence boundary

Static source reference:
[`ProfHepta/pipecat@af9f9da9d7afddaa8811c0e0251b32566c980bdf`](https://github.com/ProfHepta/pipecat/tree/af9f9da9d7afddaa8811c0e0251b32566c980bdf).

- [`services/azure/realtime/llm.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/azure/realtime/llm.py),
  lines 25–122, blob `e34de9590ea3d1151714e5f7b3339da32914ba60`: inherits
  the OpenAI GA service, selects the v1 endpoint/deployment and key or Entra
  header. The explicit query/credential differences are documented above.
- [`services/azure/common.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/azure/common.py),
  lines 9–18: async token-provider callback. Accepting a supplied token is a
  bounded subset, not an implementation of that callback.
- [`services/openai/realtime/llm.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/openai/realtime/llm.py),
  [`events.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/openai/realtime/events.py)
  and [`open_ai_realtime_adapter.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/adapters/services/open_ai_realtime_adapter.py):
  the inherited GA session/events, turn handling, paired context and tool
  result behavior implemented by the shared native engine.
- [`examples/realtime/realtime-azure.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/examples/realtime/realtime-azure.py),
  lines 98–225: the source's actual paired user/service/output/assistant,
  tools and transcript-lifecycle application, not just a socket handshake.
- [`realtime-azure-async-tool.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/examples/realtime/realtime-azure-async-tool.py)
  and [`llm-azure-realtime.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/examples/update-settings/llm/llm-azure-realtime.py):
  broader asynchronous/settings examples, not claimed by this immutable
  final-tool PCM application.

Local loopback tests with synthetic credentials can qualify native handshake,
application routing, tool receipts, output bytes and failure ownership. They
cannot qualify live Azure authentication, deployment/model/voice availability,
speech/VAD quality or physical playback. Help/config/error-guard execution
alone is not an executable end-to-end session. Consult the recorded validation
report for the exact tested source snapshot and commands; this source contract
does not certify an unrun live-provider test or full Python parity.

Explicitly outside scope: Azure Voice Live, preview APIs, REST/chat/speech SDKs,
managed identity, token refresh, live-provider acceptance, WAN/TLS proxy
qualification, reconnect/resumption, browser/PSTN/WebRTC transports, device
playback, camera/images, progressive tools, mutable session profiles, generated
projects, hosting/deployment and a full replacement of all Python Azure examples.
