# Pinned source map

Python reference: `af9f9da9d7afddaa8811c0e0251b32566c980bdf` in
`ProfHepta/pipecat`. Original module ancestor: `de78ee2ab9117614cddbca975f373044ffe1bb7d`.
The paired slice builds on canonical `3ecb3cf6351d1a80d26ff8ff2c18eb1fafd053ed`
plus the separately qualified native terminal-history and context-cause changes.
The shared ownership/deadline/PCM algorithms originated in `pipecat-realtime`;
Grok wire types, readiness, tool continuation, context and turn policy are separate.

| Python source | Native contract |
|---|---|
| `src/pipecat/services/xai/realtime/events.py` | `types.rs`: root voice/VAD, audio formats/hints, items/actions, typed events, text aliases, optional/null event fields, usage, hosted tools |
| `src/pipecat/services/xai/realtime/llm.py:740–847,946–969` | `client.rs`: bearer/model query, conversation-created configuration, updated readiness, finite deadlines |
| `llm.py:651–726,1204–1346` | Immediate result delivery, explicit upstream inference, one owed run, string encoding, source result lifecycle |
| `llm.py:397–558,971–1198` | `processor.rs`: explicit PCM rates, external-turn proposals, cumulative input transcription, DTMF, text/audio output, usage/failure lifecycle, caller-acknowledged truncation |
| `src/pipecat/adapters/services/grok_realtime_adapter.py` | Initial context history packing, leading system instruction, effective runtime-setting precedence, context-function precedence and hosted-tool retention |
| `tests/test_xai_realtime_events.py`, `tests/test_xai_realtime_user_audio.py`, `tests/test_xai_realtime_voices.py` | Synthetic WebSocket readiness/configuration/events/rates/voice/cancel/force-message regressions |
| `tests/test_realtime_intermediate_results.py`, `tests/test_realtime_tool_result_encoding.py`, `tests/test_realtime_service_tool_sync.py` | Explicit aggregator inference, native tool-result relay, sibling/dedup/async/cancellation/encoding/declaration snapshot tests |
| `tests/test_settings.py`, `tests/test_get_llm_invocation_params.py` | Typed settings and context fixtures; unsupported surfaces are rejected/documented |

Source hashes (SHA-256):

- xAI `llm.py`: `57212275b193de188f976c9559240addf5872fd5f9f0e9da9620c3139fe84eac`
- xAI `events.py`: `6c55f1d681315038668917deed07d946c78f6b2aff47d61fa4a4cf379a827f27`
- Grok adapter: `97f5861bcb2dbf513d400dd90cce53e131846940c8ad67516bc3f05244af10ba`

Deliberate differences are listed in README: reject pre-ready audio instead of
silently dropping, explicit manual-VAD null, finite fail-closed resource limits,
exact correlated errors instead of broad substring suppression, real caller
playback acknowledgments instead of wall-clock estimates, terminal health
retirement, explicit native tool registration, and no automatic reconnect/reset.
There is no claim of full Python family parity or live provider acceptance.

## Shared dependencies

The initial module ancestor rejected realtime metadata in the common user and
assistant aggregators. The current paired integration uses the common realtime
aggregation extension and bounded context-cause/original-fence APIs. Metadata
auto-activation, continuous user history, audio-only startup and explicit paired
inference are covered by actual paired WebSocket tests. Explicit Disabled cascade
semantics remain unchanged; the original provider-only fixtures select that mode.

The native ToolExecutor publishes immutable process-local terminal outcomes for
pre-entry/in-flight abort, explicit cancellation, ordinary completion, panic,
callback error and timeout. The Grok relay validates the native attachment. The
private actor command additionally checks admitted provider name/arguments and
declared interruption policy before writing any output. Raw cancellation frames
remain executor requests and cannot independently acknowledge completion.

## Typed terminal and inference mapping

| Native outcome | Provider output | Assistant input/inference policy |
|---|---|---|
| `Completed` / `CallbackReturned` | Existing typed result encoding | Existing result and caller/aggregator `run_llm` policy |
| `Failed` / `TimedOut` | `CANCELLED` | Cancellation, `run_llm=true` |
| Other `Failed` reasons | Existing failed-result JSON | Existing failed result and aggregator inference policy |
| `Cancelled` | `CANCELLED` | Original native cancellation and immutable `run_llm` |
| Raw request, arbitrary metadata, missing result | No output | Executor request only; unhandled relay requests are consumed |

Timeout mapping derives from `llm_service.py::_timeout_function_call` (2228–2253),
which broadcasts cancellation with `run_llm=True`, and xAI
`realtime/llm.py::_handle_function_call_cancel` (677–683), which sends `CANCELLED`.
The executor deliberately retains `Failed` / `TimedOut` and downstream-only
receipts; the adapter maps semantics rather than assuming identical frame shapes
or directions. Native executor → relay → assistant → upstream inference → provider
response is exercised with real pipeline/WebSocket tests. No generic bidirectional
cancellation broadcast is added.

`FunctionCallsStarted` is system priority and normally precedes cancellation.
An application adapter that reorders declarations is handled by bounded relay
buffering. Normal-lane `FunctionCallInProgress` can lag a system receipt, so the
actor retains one bounded native receipt per admitted call until policy arrives.
An explicit upstream inference request is retained during that wait. Native
terminal commands survive later sender-generation changes. Public client actions
retain their caller-controlled generation behavior. First-hop or actor queue
admission is not a provider acknowledgment, and no external effect is replayed.

Native terminal provenance is retained on the downstream pipeline frame until the
assistant consumes it. The actor serializes only provider actions, so no native
receipt is encoded onto the WebSocket. Timeout projection is restricted to the
original immutable `Failed` / `TimedOut` receipt, original name/ID, and `run_llm=true`;
it does not rewrite the executor journal's original outcome. Ordinary frame wire
encoders continue rejecting unconsumed native receipts. This contract depends on
the companion native terminal-aware assistant reconciliation slice. The
terminal-only slice originally qualified this relay separately. The paired
context-intent integration below now covers shared realtime user history and
explicit supported edits; arbitrary live replacement remains rejected.

## Paired realtime context intent and lifecycle

Static source pin remains `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
Pinned Python application code was inspected statically, never imported or executed.
No live provider was used.

- `src/pipecat/services/xai/realtime/llm.py`, SHA-256
  `57212275b193de188f976c9559240addf5872fd5f9f0e9da9620c3139fe84eac`:
  397–421 realtime/external-turn metadata; 423–452 interruption/manual stop;
  646–727 result delivery, upstream continuation and direct append semantics;
  1204–1328 reset, initial seeding, owed inference and audio readiness
- `src/pipecat/adapters/services/grok_realtime_adapter.py`, SHA-256
  `97f5861bcb2dbf513d400dd90cce53e131846940c8ad67516bc3f05244af10ba`:
  116–202 system instruction extraction and saved-history packing
- `src/pipecat/services/ai_service.py` 224–238: metadata after Start
- `src/pipecat/processors/aggregators/llm_response_universal.py`
  805–938 and 1273–1288: user-first control consumption, local-only no-run edits,
  inference context, and shared trailing history; 2103–2137 tool continuations

The native cause/fence is an explicitly qualified synchronization extension:
Python shares a live context reference, while native frames carry snapshots.
Explicit no-run edits may reach the provider earlier in the native pipeline;
only requested inference creates a response. Native receipt-required settlement
and bounded atomic command admission are stronger local integrity guarantees,
not assertions of provider acknowledgement or identical Python scheduling.

`tests/paired_realtime.rs` uses real loopback WebSockets and the actual paired
user/provider/executor/relay/output/assistant topology. It covers initial context,
audio-only binding, auto metadata, repeated identical typed input and explicit
run, held no-run edits crossing trailing snapshots, stale delayed inference,
TTFS/end flushes, server-VAD ownership, manual commit, native tool continuation,
Spoken receipt-only history, pre-wire live replacement rejection, fatal overflow,
raw matching-result rejection, sealed-cause payload/marker/gap rejection,
and relabeled native declaration rejection at provider and relay ingress. An
actual native OutputTransport with whole-context SpokenHistory attestation tests
both delayed Played and Discarded receipts in the paired topology. Client unit
tests inject deterministic
Pending-before-ready and Pending-after-admission sinks with both generation and
original invocation fences. Existing provider-only fixtures explicitly select
Disabled common aggregation; only the two deliberate ResultPolicy fixtures opt
in to caller-controlled legacy result settlement.

Independent `review_paired_boundaries.rs` uses genuine assistant emissions to
verify original scheduling fences across actor deferral while provider generation
stays unchanged. It also covers native success and timeout after interruption
without a later user/speech-stop frame supplying a scope. The sealed assistant
intent is a native snapshot-integrity extension; durable results do not directly
create a response, and existing speech/group/playback gates still govern requests.

The additional delayed-terminal controls separate interruption-surviving native
call policy from cancel-enabled calls. Pinned universal assistant reset1729–1735
retains pending asynchronous calls, result2018–2095 selects eligible late results,
and maybe_push_context2103–2137 schedules after common gates; Grok result651–675
marks received material and upstream698–709 requests inference. Native renewal is
limited to a new never-bound surviving-terminal reason at assistant admission,
with inherited scopes preserved. Replay and previously scheduled causes cannot
renew. Native false cancellation reevaluates already owed intent without creating
new intent, preserving shared group/null/result policy as the single authority.
