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
Downstream `InputAudio` and `UserAudio` share the same bounded PCM append path,
including configured input-rate validation and live-conversation binding. The
original frame, speaker ID, metadata, routing fields, and local provenance pass
through unchanged. Upstream, output, and TTS audio do not append input PCM.
The audio pause gate forwards either input class before validation or wire
admission; resuming accepts only newly received frames. Input speaker IDs do not
change transcription identity: transcripts use the fixed `with_user_id` value,
which defaults to empty. Shared input transports can resample upstream.
Both input variants share the unusable-processor predicate. Since audio has
system priority, the sender's health-generation check rejects retired-session
appends for unpaused downstream audio, including after local health restoration.
Paused or upstream audio still follows its earlier forwarding path.

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

1. `pair.user()`, with Auto metadata activation or explicit realtime/turn policy
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

An explicit initial legacy or text-only detailed context can seed once, packed
into one user history item when necessary. A leading system instruction is
extracted; configured service instructions and successfully queued runtime updates
take precedence. Initial context tools can override configured functions while
retaining hosted tools, unless an explicit tool update was already admitted.
`projected_session()` exposes bounded locally admitted settings, not provider
acceptance. Settings survive interruption; response tool admission uses a separate
snapshot of wire-applied declarations. Explicit tool choice is unsupported.

Marked trailing contexts are always state-only. Sealed paired append/run causes
carry explicit intent independently of text equality; upstream assistant contexts
request continuation for already delivered tool results. Direct provider append
controls send items without inference only after a context/live session is bound.
See Shared realtime conversation below for the paired edit and rejection contract.

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

- The paired common realtime aggregators are included in this native topology.
  The processor automatically broadcasts `service_metadata()` after Start. Auto
  activates realtime semantics; explicit policies and Disabled remain authoritative.
  Disabled cascade mode does not provide continuous realtime history semantics
- Executor cancellation is acknowledged only by a validated process-local terminal
  receipt. A raw `FunctionCallCancel` is a request for `ToolExecutor`; the relay
  never treats that request, arbitrary metadata, silence, or a missing result as
  settlement. Public `CancelTool` remains an explicit caller-directed provider
  control; it neither cancels an executor task nor supplies a native receipt
- The native executor retains its own exactly-once local lease decision and
  first-hop admission journal. The provider actor validates admitted
  call identity and interruption policy before a socket write. The relay retains
  native provenance for the assistant, which consumes it after reconciliation. Native queue admission is not a socket
  write or proof of provider observation. No external effect is replayed
- Native `Failed` / `TimedOut` maps to provider `CANCELLED` and an assistant
  cancellation with `run_llm=true`, matching the pinned Grok timeout continuation.
  Other failures remain failed results; native cancellations preserve their
  requested inference policy. Results never independently start inference
- Native terminal commands survive later provider turn-generation changes.
  Policy and declaration reordering are bounded: a receipt can wait for its
  matching policy before wire settlement, and for declarations before assistant
  delivery. A missing declaration/policy never fabricates settlement
- Full native outcome routes and invalid/mismatched receipts fail the session
  closed. Provider closure with outstanding tools retires the pipeline and
  releases callback leases. Local cleanup cannot certify remote rollback
- The relay requires native terminal-aware assistant reconciliation. Its pipeline
  copy retains immutable outcome provenance, including the original failed timeout
  decision after a narrowly validated cancellation projection. Ordinary frame wire
  encoders reject unconsumed provenance; the actor serializes only provider commands
- Sealed native terminal payload/error/inference policy cannot be edited after
  publication. An application may explicitly consume a valid receipt and issue
  ordinary legacy result callbacks with the explicit `with_legacy_tool_results()`
  opt-in, forfeiting native terminal integrity downstream.
  The native executor does not expose arbitrary per-result `run_llm` overrides;
  immutable native results retain their publisher/default inference policy
- Existing conversations cannot be replaced/reset in place. Paired text-only
  appends and explicit runs are supported through sealed context causes. Live
  replacement/deletion/transforms are rejected after any upstream local edit but
  before provider mutation, and fail the session; repair history and construct a
  new pair/session. Raw detailed editing controls reaching the provider directly
  are unsupported. Arbitrary unattributed changed snapshots are never guessed
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

Native executor declaration batches remain attached through the relay until the
assistant consumes their ownership proof. Application adapters that opt into
legacy result mutation must explicitly validate/consume both declaration batches
and terminal receipts before forwarding ordinary frames; their downstream
results then have no native ownership or late-terminal restoration guarantee.

## Shared realtime conversation

Use `pair.user() → GrokRealtimeProcessor → ToolExecutor → tool_result_relay()
→ delivered output → pair.assistant()`. After forwarding the first Start, Grok
broadcasts its realtime metadata in both directions. Auto pairs activate and
server VAD recommends external turn strategies; explicit policies and Disabled
remain authoritative. Standalone/misordered active realtime halves are rejected
by the shared aggregator. Provider-only legacy fixtures use an explicit Disabled
pair.

Audio/response activity binds a live session separately from explicit history
seeding. Marked trailing contexts, including the very first audio-only snapshot,
are state-only and enqueue neither history nor inference. Explicit initial
context can seed once. Native context causes identify actual typed input,
appends, and run requests, so repeated identical text and intervening trailing
snapshots cannot hide or replay an explicit append. `run_llm=false` admits no
response; native history may reach the wire earlier than Python's user-first
local-only mutation. Upstream assistant tool continuations only request inference
for already delivered results, and provider tool outputs never create their own
response.

Context cause admission is durable bounded local admission, not provider
acknowledgement. Interruption cancels the original inference intent while admitted
history survives. The actor rechecks the non-owning original invocation fence at
every pending socket-ready poll, admits bytes once, and flushes admitted bytes
without retry. Deferred requests and network output retain that same fence.
Connection/health failures retire the session without replay; reset requires a
new processor and pair.

Live full replacement, deletion and arbitrary transforms require a new session.
The common user may already have applied those edits locally before Grok rejects
the operation; no provider history/settings/inference is partially admitted.
At provider ingress, invalid explicit causes fail the session so local/provider
divergence cannot silently continue. A malformed external carrier reinjected
before the user emitter can instead be rejected there before any new local
mutation or provider effect; this does not consume its valid original clone. Non-text or provider-specific history and direct detailed
message controls remain unsupported at the provider boundary; typed text-part
appends through the real pair are supported. Ordinary input text and model
output are never deduplicated by text equality. Spoken history still requires
actual application delivery receipts; response/audio/transcript generation alone
cannot certify playback.

Native pipeline settlement requires a validated ToolExecutor receipt by default.
A matching raw success/error/result frame cannot settle a native call, and raw
cancellation remains only a request. Applications deliberately managing legacy
callbacks can opt in with `with_legacy_tool_results()` before constructing the
relay; this relinquishes native terminal-integrity guarantees for raw results.
Malformed present receipts never fall back to legacy handling. The standalone
RealtimeSender API remains explicitly caller-controlled.

Lock order for socket admission/output is emission gate → ordered runtime
admission gates → health observation. Provider context/sender-state locks are not
held across those operations; cleanup removes the sender under a short separate
scope before closing it. An invocation-fence closure must not reenter core
routing/spawn/admission APIs.

Assistant tool continuations carry a distinct sealed scheduling intent from the
actual assistant invocation, with a separate bounded admission cursor. They keep
`RequestInference` gating, not unconditional response creation. An interruption
that expires that invocation prevents delayed inference even when its provider
cancel frame is intercepted. A newly eligible native terminal with `cancel_on_interruption=false` can
schedule a new assistant request after interruption even when its durable result
was delivered late. Admission binds its current assistant-local generation and
retains inherited origin restrictions. Cancel-enabled late deliveries retain
stale-inference suppression; already bound requests never renew. Intent/direction mismatch rejects before effects.

Unmarked upstream contexts without `AssistantContinuation` are rejected by the
native adapter. Direct integrations or explicitly Disabled aggregation can opt
in to `with_legacy_context_requests()`, which treats unsealed upstream requests
as caller-owned and cannot preserve their original assistant scope through
delayed delivery. This opt-in does not weaken validation of a present native
cause and is independent of legacy raw-tool-result settlement.

## Usage metrics

Pipeline token usage requires `StartFrame.enable_usage_metrics` (default false)
and a nonzero provider total. This corrects earlier unconditional emission;
the low-level mapper remains unchanged. See [USAGE_METRICS.md](USAGE_METRICS.md)
for exact source precedence, zero/null behavior, and base-only RTVI output.

## Opt-in finite final-tool application

`GrokRealtimeApp::new(config, seed, tools, output)` prepares the same production
composition used by `grok_pcm`: standard realtime user, private source-context
observer, Grok provider, guarded **real** native `ToolExecutor`, existing result
relay, trusted bounded output, and standard generated-history assistant. `start`
seeds once without inference after provider readiness. Typed `append_pcm`,
`append_text(text, run)`, `run`, `interrupt`, `end`, and `cancel` do not expose raw
frame ingress or a mutable shared context. Read-only `history()` and `outcomes()`
are explicitly conversation/lease data, never included in terminal diagnostics.

This profile is mono PCM16 at 24 kHz, JSON audio, server VAD, one fixed connection,
ordinary text seed, and immutable final-returning interruptible local tools.
Hosted tools, runtime settings/schema replacement, speculative/progressive tools,
noninterruptible callbacks, historical tool messages and playback-history mode
are not admitted. Existing lower-level progressive/intermediate/manual/G.711 and
legacy behavior remains separate and available.

The app retains exact process-local mapped declarations and original invocation
contexts. Serialized wrappers bracket real standard-aggregator invocations; only
real sealed requests inherit those contexts. The actual executor receives a
forwarding context from that original source. An immutable callback adapter checks
again at first poll before constructing the user's future. This is an admission
observation, not an atomic prohibition on every instruction after a concurrent
interruption, and not proof that effects were undone. Native receipts alone
settle entered leases. Proven stale unentered proposals are separate local
record dispositions, never invented cancellation receipts.

End seals ordinary input, then the manager owns the complete barrier operation,
so a cancelled End waiter cannot abandon it. Actual provider command/event cuts,
normal/system lane acknowledgments, native outcomes at both wire and assistant,
real output media fences and observed speech gates precede conditional actor
close. A tool output does not request inference: the real assistant's sealed
continuation remains the only tool-follow-on inference authority. A null final
result can legitimately owe no follow-on. There is no quiet-period heuristic.
One absolute session/drain deadline bounds accepted follow-on tool cycles.

Server-VAD start uses one private System-lane ordering acknowledgment through the
actual user and provider. The actor temporarily pauses only socket reads until that
roundtrip completes, so the start's real interruption cannot discard a queued stop
or transcript from the same rapid server burst. Commands, cancellation and absolute
deadlines remain live; duplicate/no-op starts acknowledge without inventing another
interruption. The marker never supplies response authority or clears speaking gates.

Retain `app.completion()` before starting when dropping/cancelling the startup or
running handle. `completion.wait()` retains and awaits the manager handle;
cancelling one waiter never takes the handle. Manager publication acquires that
retained slot before spawning, so a concurrent pre-start observer cannot detach
an unpublished task. A wait before publication can return "application not started";
retain the observer and wait again after start. Reports distinguish primary
provider/mapping classifications, first operational failure stage, independent output
cleanup failure, observed drain blockers, typed pipeline failure/cleanup classifications, positively
observed forced cleanup and actual owner joins. A joined failure remains a failure;
`UnknownAfterFailure` does not claim that every secondary forced cleanup was observable. Drop signals cancellation and does not itself certify any join.
No outcome journal or local close certifies remote cancellation or rollback.

The session timer stops admitted work; it is not a hard wall-clock bound on joined
completion. The shared pipeline applies its finite startup budget separately to
processor setup and Start readiness. Each budget is capped at the session limit,
but the manager intentionally retains startup after cancellation or expiry, then
awaits finite cleanup budgets. A blocked asynchronous trusted-output setup can
therefore finish abort/join after the session timer; this reports failure, not a
successful session or an unobserved detached cleanup.

The caller's output processor is trusted, not sandboxed. It must implement its
own bounded, cancellation-safe side effects. Its setup must return normally or
return an error, not independently terminate the not-yet-started pipeline. App-only
network receive starts at the real provider Start, after registering its original
scope, and pending sessions remain owned through cleanup. The output must forward real flow flushes only
after its preceding media reaches the chosen delivery boundary. Configuration
and retained-capacity checks apply before cloning; typed admission and conservative
history/output counters continue for the entire session. Configuration/seed/schema
backing storage has one aggregate `max_retained_bytes` limit; compact, live private
input-authentication copies have a separate aggregate limit of the same size.
Bounded queues, call ledgers, history and media retain their separately documented
count/byte limits; this is not one process-wide allocator cap. Arbitrary callback
captures and output implementation allocations remain caller responsibilities.

### Recorded PCM executable (Linux)

Run `grok_pcm --help` for exact finite arguments. No arguments, `--help` and
`--check` are inert/offline and never read the credential. Active execution needs
`--enable`, regular aligned finite `--input`, create-new `--output`, and required
`--responses N`. It reads only `GROK_PCM_API_KEY` and one of
`GROK_PCM_CONNECT_ADDRESS` (numeric IP:443, fixed xAI TLS/Host identity) or explicit
`GROK_PCM_LOOPBACK_ADDRESS` (literal loopback fixtures). `--run` explicitly requests
inference; `--echo-tool` registers one bounded deterministic final echo callback.
No shell, external I/O or hosted tool callback is offered.

The binary verifies retained directory/file identities with Linux directory-FD
and `O_NOFOLLOW` operations. Input reading, output create-new and exact bounded
PCM-buffer reservation happen **before** networking. During the live pipeline,
recording only copies into this bounded buffer; it never writes/fsyncs a file or
claims `Played`. Only after the completion observer proves the application owners
joined does final/partial PCM persist through the retained output FD. Filesystem
preflight/final persistence is outside network cancellation deadlines; no hard
real-time or durability guarantee is made. Path replacement cannot redirect a
write. App success, persistence success and actual partial bytes are distinct;
exit zero requires both the app and final recording condition.

A completed tool-only response never satisfies the required generated-speech
count. Missing speech times out. PCM EOF is not a remote commit acknowledgment
and does not prove every supplied sample was interpreted. Synthetic loopback
coverage is not live xAI/TLS/model/voice acceptance or physical playback.
