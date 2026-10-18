# Native OpenAI Realtime

`pipecat-realtime` implements the OpenAI GA Realtime WebSocket protocol used by
`src/pipecat/services/openai/realtime/{llm,events}.py`. It uses the source default
`gpt-realtime-2.1` model at `wss://api.openai.com/v1/realtime`, bearer authentication,
nested `audio.input` / `audio.output` settings, and `response.output_audio.*`
events. It does not send the obsolete beta header or use a Python runtime.

## Session API

```rust,no_run
use pipecat_realtime::*;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = RealtimeConfig::new(std::env::var("OPENAI_API_KEY")?);
let mut session = RealtimeClient::new(config)?.connect().await?;
let sender = session.sender();
// Input is mono PCM16 LE at 24 kHz in this default configuration.
sender.send(Action::AppendAudio(vec![0; 4_800])).await?;
sender.send(Action::CommitAudio).await?;
sender.send(Action::CreateResponse(ResponseOptions {
    context_id: Some("application-turn-1".into()),
    ..Default::default()
})).await?;
while let Some(event) = session.next_event().await {
    let event = event?;
    if matches!(event.event, Event::ResponseDone { .. }) { break; }
}
session.shutdown().await?;
# Ok(()) }
```

`connect` establishes the WebSocket and sends `session.update`. User actions wait
for `session.updated`; session readiness has its own deadline. `send` acknowledges
local acceptance, not server acceptance. Provider errors are typed events.
`try_send` queues without waiting for I/O; a rejected queued action is reported as
a terminal stream error. Applications must continuously drain `next_event`.

The public `Action` API includes session updates, audio append/commit/clear,
conversation item creation/deletion/retrieval/truncation, response creation and
cancellation, and final/intermediate tool results. `ConversationItem`, content,
audio formats, turn detection, responses, usage, errors, and server events are
typed. JSON values are reserved for function schemas, arguments, and results.
Unknown future event kinds are ignored without retaining their payloads.

Every delivered event carries the provider event ID, session ID, conversation
ID, local context ID, and interruption generation. Response/item/content IDs are
preserved by typed event variants. One response and one deferred response are
supported per connection; further concurrent response requests fail explicitly.
Final tool outputs trigger one continuation only after all outstanding calls have
finished and the originating response has ended. Strings are sent as strings,
not double JSON-encoded. Intermediate results are inserted as typed async-tool context messages and do not finalize a call. Duplicate
final results and results for interrupted calls are discarded.

## Frame processing

See [bounded raw camera input](CAMERA.md) for sampled RGB-to-JPEG user items,
passive image handling, interruption guards and owned encoder shutdown. Camera
upload does not trigger a response or add rich images to the shared context.

See [paired realtime aggregation](AGGREGATION.md) for the required bidirectional
topology, explicit seed ownership, state-only snapshots, final-tool relay,
playback semantics and intentionally unsupported context mutations.

`OpenAiRealtimeProcessor` integrates with `pipecat-core::FrameProcessor`. Configure
pipeline input and output sample rates to 24 kHz and supply mono PCM16. Downstream
`InputAudio` and `UserAudio` share PCM validation and bounded audio append; the
original frame, including a `UserAudio` speaker ID, is forwarded unchanged.
The append protocol carries PCM only: input speaker IDs do not change transcription
identity, which remains the configured `with_user_id` value (empty by default).
Upstream audio is forwarded without appending. Resampling and capture pause/mute
belong upstream; the shared input transport can resample into the required format.
An empty direct append retains the client’s asynchronous rejection, while the shared
input transport drops empty audio before this adapter. The processor maps
input audio, input text, local end-of-turn, context/message updates, session/tool
settings, tool results, interruption, and shutdown. Unhandled frames are forwarded
in their original direction. Socket I/O and response processing run outside
`process`; the processor enqueues bounded commands without waiting for writes.

Output includes LLM response start/end, LLM text, interim/final transcriptions,
TTS audio and start/stop, tool-in-progress, usage metrics, speech activity, and
upstream error frames. Manual-turn interruption additionally uses a bounded
[pre-roll clear/replay window](PREROLL.md); standalone and server-VAD sessions
retain their existing input-buffer behavior. `RealtimeFrameMapper` is usable separately. Split PCM
samples are assembled; an incomplete final sample is rejected. A response with
multiple audio items emits one TTS start/stop pair. Frame metadata retains
session/conversation/response/item/content/context correlation.

`acknowledge_playback` accepts the output transport's actual item/content playback
position. Interruptions cancel output immediately and truncate that exact item at
that position. Generated byte counts are not treated as played audio. If there
is no acknowledgement, cancellation occurs without guessing a truncate offset.
Generation checks discard both buffered and subsequently received stale output.
Deferred responses and tool continuations retain their originating generation even
when an interruption happens during a blocked socket write.

The standalone client supports PCM, PCMU and PCMA; the frame adapter intentionally
requires PCM. Plain and lossless text detailed contexts reconcile append-only
history without recreating server-owned items. Rich multimodal/provider-specific
contexts and arbitrary replacement remain unsupported in the paired frame
adapter. Intermediate asynchronous tool continuations require the explicit
selected reporting composition below; they are unsupported on the default path. Use the standalone
typed client for custom item ownership rather than mixing untracked item actions
through a processor's sender with its shared-context reconciliation.

## Bounds, security, and recovery

Queues, serialized request size, WS frame/message size, decoded audio chunk size,
function-call state, response history, context imports, and partial PCM state are
bounded. Both input-audio frame classes reuse the client's raw-chunk and
serialized-message checks without adding another budget or changing either limit.
Connect, readiness, write, response, and shutdown have deadlines. Queue
saturation fails explicitly instead of silently dropping audio. Cancellation and
close use a separate control lane, independent of the audio queue. Dropping a
session closes its socket even if a sender remains alive. `finish` is ordered after
preceding commands and drains the active response before closing; `close` and
`shutdown` cancel promptly. Pipeline End/EndDetailed drain, while Cancel/CancelDetailed
abort. Ordered finish is preserved across interruptions while awaiting readiness.
A provider rejection releases pending response admission only when it identifies
that exact response.create request; unrelated errors cannot release live work.
The paired processor tolerates `response_cancel_not_active` only once for an
exact nested `error.event_id` matching a same-session cancel that successfully
flushed locally while targeting a known active response. The outer event ID is
never request attribution, and local flush is not provider acknowledgement.
Consuming this receipt does not release busy state, change deadlines, or create
another response: the existing response-done path owns continuation. Receipts
survive response completion and later turns, so a late old error cannot clear a
new turn. Public cancellation/interruption and internal batch cancellation use
the same cancel-only receipt recording boundary.

The receipt window is FIFO with at most 64 entries and no time-based TTL. Total
owned request/response ID payload is limited by `max_message_bytes`; fixed
record/container overhead is separate from that payload bound. Oldest entries
are evicted to satisfy either limit. A cancel still sends when its receipt is
too large to register. Missing, unknown, evicted or duplicate IDs, pending-only
cancels, and wrong codes retain fatal paired error handling. In particular,
`conversation_already_has_active_response` is not recovered: both unrecovered
control codes bypass generic pending-create recovery, with no automatic retry.
The standalone client keeps delivering raw `Event::Error` events. This is more
conservative than the pinned Python service's code-only suppression of both
errors; it is not general response-conflict recovery.

Set the pipeline shutdown deadline long enough for the configured response
deadline when graceful draining is required.

Production connections require TLS. Plaintext is accepted only for literal
loopback IPs used by local test servers, not remote hosts or DNS names. Endpoint
userinfo/query/fragment are rejected; the client creates the model query itself.
Authorization headers are marked sensitive. Debug configuration redacts secrets,
provider error echoes of the configured key are redacted, and transport/handshake
errors never include response bodies or raw headers. Redirects are not followed.
There are no implicit reconnects or retries that might duplicate paid responses
or leak/replay audio. After an ambiguous send timeout, reconcile server events
before deciding whether to retry.

## Verification and limits

Run `cargo test -p pipecat-realtime` and
`cargo clippy -p pipecat-realtime --all-targets -- -D warnings` from `rust/`.
Integration tests use actual local TCP/WebSocket servers with mock credentials;
no paid provider call or real key is used. They exercise authentication and GA
wire shapes, fragmented WS messages, chunked PCM, multi-item lifecycle,
transcriptions, IDs, tool continuation, cancellation/truncation, stale output,
errors, EOF, size limits, deadlines, backpressure, redirect safety, and core
pipeline lifecycle. Live provider compatibility has not been verified.

This crate does not implement Anthropic, Gemini Live, xAI/Grok Realtime, Azure
Realtime, or Inworld Realtime, and does not claim parity for those services.

## Token usage details

The pipeline honors `StartFrame.enable_usage_metrics` (default false), and may
carry existing provider cache/audio breakdowns in same-event usage metadata.
Public low-level mapper output remains unchanged. See
[USAGE_DETAILS.md](USAGE_DETAILS.md) for the default-output correction, numeric
validation, nullable/default distinctions, and independent RTVI opt-ins.


## Opt-in native tool progress

This feature is off by default. It is a selected native OpenAI Realtime path,
not a general change to the low-level `Action` API or the existing final-only
`OpenAiRealtimeToolResultRelay`. Use one shared `NativeToolProgressRoute` with
all six actual components, in this order, in one pipeline:

1. `OpenAiRealtimeToolProgressGate::new(route.clone())`
2. The user half of a standard `LLMContextAggregatorPair` in enabled realtime mode
3. `OpenAiRealtimeProcessor::new(client)?.with_progress_route(route.clone())`
4. `ToolExecutor::new(...)?.with_progress_route(route.clone())?`
5. `OpenAiRealtimeToolProgressRelay::new(route.clone())`
6. The same pair's assistant half, after any output/delivery processors

Call `pair.bind_tool_progress(route.clone())?` before using either half. Each
component registers its real runtime identity in setup. The provider's Start
checks ordering, unique roles, shared history and configured budgets before
connecting, seeding the provider and opening callback admission. Constructing a
route or copying public frame metadata does not grant reporting authority.
Missing or incompatible roles fail the selected composition before a callback
can run. This path does not combine with signed/speculative tool calls or the
separate tool-free realtime user-prefix receipt protocol.

Register a `pipecat_processors::tools::ReportingTool` through
`executor.register_reporting(name, ReportingTool::new(callback))?`. The callback
receives `(FunctionCall, ToolProgressReporter)`, runs in the existing owned tool
task, and returns the usual final `Result<serde_json::Value, String>`. The original
`RegisteredTool` struct and `ToolCallback` signature are unchanged; ordinary
final-only siblings can share the selected executor.

`reporter.report(value, run_llm)` synchronously returns a `ToolProgressReceipt`
only after that immutable progress event reaches its actual first-hop queue.
The returned receipt is not a final result or a transport acknowledgment.
`receipt.await_history().await` waits for the real paired assistant to append
the canonical history segment; repeated waits see the same fact. Dropping a wait
does not retract an admitted report. Provider-command admission is recorded
separately and does not prove a socket write or remote acceptance. A history
snapshot can overtake a progress frame on the provider's separate queue. In that
case, the original inference intent waits for those exact provider-queue facts;
no snapshot or fresh invocation fence is substituted. At most 32 lease intents
and one global intent are retained, with shared, charged dependency IDs. User
items and settings still enter their ordinary queue immediately: a history
commit is not a global ordering guarantee across provider item queues. There is no
independent Python-style `on_context_updated` callback, per-report task or
detached follow-on task. History waits and follow-on code consume the callback's
one original absolute execution deadline.

Progress adds one canonical system-role async-tool context item and leaves the
call unresolved. The assistant's existing run policy, activity and playback
gates choose any progress continuation. The provider admits it only with native
route/call permission and no unresolved interruptible sibling; public action
flags cannot grant that exception. It retains one active provider response and
bounded deferred selected continuations, without replaying earlier progress
items or duplicating the final function-call output. A normal reporting final
remains separate from progress, even if its JSON value equals the last report.

Selected reporting tools must have `cancel_on_interruption = false`; explicit
cancellation still stops them. Ordinary interruption preserves admitted history
facts while invalidating stale inference causes. The first completion, failure,
timeout or cancellation to freeze the tool lease fixes its terminal choice and
last admitted ordinal. Both consumers reorder against that watermark before
accepting the terminal. Cancellation prevents new reports and revokes the
call's inference permission; it cannot rewrite a final already frozen first or
turn a genuinely committed history fact into a retraction. Callback effects
remain unknown once entered and are never automatically retried.

### Limits and shutdown

The selected executor allows at most 16 concurrent slots. Every callback,
including ordinary siblings, needs a timeout from 1 ms through 300 seconds
(default 30 seconds). The route bounds active reporting leases at 16, progress
at 64 events per call and 1024 per session, and pending progress at four events
per call and 32 per session. Encoded progress JSON is capped at 16 KiB and final
JSON at 64 KiB. Borrowed iterative guards run before serialization/cloning.
Terminal capacity is reserved separately (256 KiB per call, 4 MiB aggregate),
so progress saturation cannot spend the terminal reservation. Canonical history,
provider ledgers and compact receipt metadata also have finite count/byte bounds;
these are logical retained-byte limits, not RSS promises. Compact executor
journal entries keep identity, terminal choice/watermark, effects and first-hop
admission under the existing 8 MiB budget, without retaining full terminal
payloads or transport authority.

The ingress gate holds End/EndDetailed. It closes new tool and inference
admission while already admitted callbacks may report and return within their
original deadlines. End advances only after actual progress predecessors and
terminal dispositions at both provider and assistant, not merely after the
executor journal becomes empty. The default End drain is 35 seconds and must
cover the largest registered callback timeout plus five seconds.

Set the actual `PipelineConfig.shutdown_timeout` to at least:

`end_drain_timeout + response_timeout + 2 * operation_timeout + close_timeout + 5s`

With default callback and provider settings this is **172 seconds**, so the
ordinary 30-second pipeline shutdown default is rejected for this composition.
The provider command limit must also cover 64 KiB messages and its outstanding
tool capacity must cover the selected executor. Startup validates these limits.

For a prompt selected Stop, call `route.request_stop()?`: it closes report and
inference admission, cancels actual tool tasks, and has the owned route worker
forward one Stop, including while End is still held. Once End was forwarded it
returns `AlreadyEnding` and cannot replace that terminal. A raw
`PipelineSender` Stop retains its ordinary Normal-lane admission and post-End
restriction; it does not gain priority over an already queued/held End merely
because this feature is enabled. Cancel and failure close the route promptly.
Only a successfully forwarded graceful End uses ordered provider `finish`;
Stop/Cancel/failure close instead. The selected processor retains the session
owner so cancellation of the receive task cannot discard the socket actor's join
handle. Stop preparation and Cancel/failure cleanup reclaim and await that actor;
ordinary receive completion also awaits it. `selected_session_joined()` is false
before connection and becomes true only after that actual await, including a
forced abort followed by an await. It does not distinguish clean shutdown from
failure or prove remote acceptance. If the pipeline itself forcibly aborts its
cleanup wait, that interrupted wait does not establish completion. Dropping a
journal or receipt is not evidence that those tasks joined.

The dedicated `tool_progress` integration target uses real local WebSocket,
executor, gate, relay and paired-history components, including actual actor joins
and interruption/terminal-ordering negative controls. This local qualification
does not establish live provider compatibility. Local admission/history receipts
do not establish provider acknowledgments or durable exactly-once external effects.
