# Native xAI/Grok Realtime

`pipecat-grok-realtime` implements a bounded native Rust WebSocket client and
Pipecat frame adapter for the pinned xAI voice service. It does not run Python.
The default endpoint is `wss://api.x.ai/v1/realtime`, the connection model is
`grok-voice-latest`, and the default voice is `eve`. Custom voice strings are
accepted. No API credentials are included in this crate.

## Wire and lifecycle

The client waits for `conversation.created`, sends `session.update`, then waits
for `session.updated` before admitting input audio. `AppendAudio` before readiness
returns `NotReady`; it is never buffered or replayed. The frame processor completes
setup only after this readiness acknowledgement, before pipeline frames enter. This is an explicit-error
variant of the pinned Python service's pre-ready drop policy. Other actions have
bounded queues and finite readiness deadlines.

Root-level voice/turn detection, nested audio/transcription settings, supported
PCM sample rates (8/11.025/16/22.05/24/32/44.1/48 kHz), and G.711 wire formats are
represented separately from OpenAI GA settings. The frame adapter supports mono
PCM16 with independently configured input/output rates. It does not resample.
Manual VAD sends explicit `turn_detection:null`, rather than Python's omitted
null field. Response options use `modalities`, including both text and audio.

`RealtimeClient`, `RealtimeSession`, and `RealtimeSender` expose typed actions and
events. Read `next_event` until completion to observe terminal failures. `send`
acknowledges local write/admission, not provider acceptance. A dropped send future
or write timeout can leave an ambiguous remote operation: do not blindly retry
billable response creation. No automatic retries, redirects, reconnections,
conversation replay, or provider acceptance claims are made.

`Finish` drains accepted commands and the active response. Repeating it after a
clean peer close succeeds; a stored protocol/transport failure remains an error. Interruption has a
separate control lane, invalidates queued response output, always sends cancel,
and clears the input buffer only in manual mode. Socket reads, writes, response
completion, readiness, and shutdown have finite deadlines. Event/command queues,
message/audio bytes, tool calls, response IDs, argument accumulation, and local
playback metadata are bounded. The session fails rather than forgetting a call
ID and permitting duplicate execution.

## Tools and context

Tool results are sent immediately but never create a response. `RequestInference`
represents the upstream assistant aggregator's request to consume newly delivered
material. One owed request is retained during an active response; `response.created`
consumes earlier material, and interruption clears the debt. Intermediate results
are user-message async-tool envelopes. A final output closes only its own call.
Pre-encoded string results are preserved; null results use `COMPLETED`.

The provider's function call is checked against the immutable advertised names
captured for that response. Missing IDs on Grok tool-completion events are resolved
through the original call, including its context/generation. Native frame results
also match the admitted function name and JSON arguments. Already-announced async
calls survive interruption; the tool executor owns their cancellation policy.

For the existing native tool runtime, use this order:

1. User aggregator, configured explicitly for external turns when using server VAD
2. `GrokRealtimeProcessor`
3. `pipecat_processors::tools::ToolExecutor`
4. `processor.tool_result_relay()`
5. Audio output, if needed
6. Assistant aggregator

The relay sends intermediate/final/cancel results before they reach the assistant
aggregator, including `run_llm=false` results. It never requests inference. An opaque
local frame tag prevents a result from being delivered twice when it also passed
through the main processor. Tool execution remains in `ToolExecutor`; advertising
remote MCP/search tools does not execute local tools or transmit local credentials.

The initial legacy/text-only detailed context is packed once into one user history
item when necessary. A leading system instruction is extracted, with configured
service instructions taking precedence, including successfully queued runtime
updates. `projected_session()` exposes bounded locally admitted settings, not
provider acceptance. Settings survive turn interruption; response tool admission
uses a separate snapshot of wire-applied declarations. Later downstream snapshots do not run
inference; upstream snapshots request inference. Message appends after setup send
items without running inference. Context tools override configured functions while
retaining currently projected hosted tools. Explicit tool-choice settings are rejected.

## Output and turns

Grok cumulative transcription updates replace the interim text and are sent
upstream, as are final input transcripts and valid DTMF. Server VAD emits proposed
turn frames. The external user-turn strategy owns interruption; the provider event
alone does not broadcast an interruption. Text-mode output is ordinary LLM text.
Audio transcripts provide a non-context-appending LLM view plus a TTS transcript;
these are generated text, not evidence that the user heard it.

`acknowledge_playback` accepts an application's actual output receipt for a known
current response item/content part, a monotonic position, and no more than generated
audio. New responses invalidate old receipts. Interruption truncates only that
acknowledged position; without a receipt it sends cancel without inventing one.
The receipt is a caller assertion. This crate cannot certify physical playback.
The standalone typed truncate action remains an explicit caller-controlled request.

A health transition retires the processor's live session and fences old output.
Restoring the health handle never reconnects or replays work. Construct a fresh
processor/session to reconnect. End/cancel still perform cleanup while unusable.

## Explicit boundaries

- The baseline cascade aggregators are not source-equivalent for continuous
  realtime user history, audio-only startup, or user-driven inference. A separately
  reviewed common realtime aggregation extension is a prerequisite for those
  semantics. The tool/output tests use explicitly seeded context; explicit external
  turn configuration alone does not close this gap. `service_metadata()` exposes
  the correct descriptor for capable integrations but is not broadcast automatically
- Ordinary runtime interruption currently aborts native ToolExecutor callbacks
  without emitting a typed cancellation outcome. Applications must explicitly
  deliver `FunctionCallCancel`/`CancelTool` to settle those provider calls. Reliable
  exactly-once executor cancellation outcomes are a separate shared prerequisite;
  the relay never guesses cancellation from a missing result or timeout. Explicit
  cancellations and async surviving results are covered independently
- Existing conversations cannot be replaced/reset in place. Detailed message
  append/update and message-transform controls are explicitly rejected before
  forwarding, since they have no provider-side history synchronization. Initial
  text-only detailed context support does not imply support for these edits.
  Rejection applies when the raw control reaches this processor before an earlier
  aggregator consumes it. It does not certify or reconcile a context already
  mutated upstream and converted into `LlmContext`; arbitrary context reconciliation
  remains unsupported. Route unsupported editing controls through rejection before
  any history-owning aggregator
- There is no automatic reconnect or fallback envelope for a lost call ID;
  unknown results fail closed
- Detailed context supports text parts and ordinary/direct function definitions;
  rich media, opaque provider history, grouped/custom schemas, and speculative
  inference are rejected
- Function handlers embedded in a context are not auto-registered; register them
  in the native tool executor explicitly
- Session patches preserve omitted audio settings. They do not reproduce every
  Python settings-replacement warning or compatibility alias
- Binary audio transport is rejected. Resumption opt-in may be serialized, but
  reconnect/replay/resumption is not implemented
- Raw typed events expose idle/MCP observations. MCP failures map to errors; hosted
  search/MCP execution, SIP infrastructure, billing and hosted voice quality are
  outside offline validation

`SOURCE_MAP.md` identifies the exact Python source and deliberate differences.
Tests use real local TCP/WebSocket peers and native pipelines with synthetic
provider messages. Output-backend tests exercise synthetic Played/Discarded
receipts; they do not claim a microphone, speaker, hosted account, or live model.
