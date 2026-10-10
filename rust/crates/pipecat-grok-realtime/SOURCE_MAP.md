# Pinned source map

Python reference: `af9f9da9d7afddaa8811c0e0251b32566c980bdf` in
`ProfHepta/pipecat`. Native base: `de78ee2ab9117614cddbca975f373044ffe1bb7d`.
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

## Shared prerequisites

`pipecat-turns/src/user.rs::validate_control` and
`pipecat-turns/src/assistant.rs::LLMAssistantAggregator::reduce` reject realtime
metadata in this base. Common realtime aggregation is needed for source-equivalent
continuous user history, audio-only startup, and user inference, beyond the seeded
context tool/output path tested here.

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
the companion native terminal-aware assistant reconciliation slice. Common realtime
user aggregation and upstream-consumed arbitrary history reconciliation remain
separate from this provider slice.
