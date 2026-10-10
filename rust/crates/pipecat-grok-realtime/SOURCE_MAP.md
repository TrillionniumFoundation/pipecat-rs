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

`pipecat-processors/src/tools.rs::ToolExecutor::dispatch` spawns interruptible
callbacks, and `ActiveLease::drop` only removes active history. Ordinary runtime
interruption emits no `FunctionCallCancel`; only the separate speculation-denial
branch does. A reliable typed exactly-once outcome must cover pre-entry/in-flight
abort, distinguish already completed work, and preserve non-interruptible siblings.
The provider adapter cannot safely infer that outcome from a call ID or missing
placeholder. Explicit typed cancellation is tested without fabricating completion.
