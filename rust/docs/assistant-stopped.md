# Assistant completion records and native jobs

`LLMAssistantAggregator::subscribe_stopped` registers one bounded, caller-owned
receiver before the first turn. This is the application completion boundary used
by native job callers. It is distinct from generated-token/full-response
collection, tool-context callbacks, and the existing agent-workers engine.

The immutable `AssistantTurnStopped` payload includes marker-projected committed
text, interruption status, the original turn-start timestamp, an assistant-session
ID, a monotonic turn ID, and an optional explicitly captured job generation. A
record is emitted for every real active turn, including empty tool-only and
interrupted turns. Replayed boundaries do not create new turns; identical text on
two real turns produces two records. No generated text is treated as played audio.

## Ordering and ownership

The authoritative assistant finish commits history first. Its ordered output is:

1. Nonempty context snapshot and commit timestamp
2. One `AssistantStopped` record admission
3. Nonempty assistant-turn feedback siblings
4. Any deferred inference or lifecycle terminal

Empty turns omit context/timestamp/feedback, but retain the completion record.
Paired-user interruption keeps its established interruption-before-feedback
ordering; the record follows the assistant timestamp and precedes turn feedback.
The interrupted record is data only. It supplies no `ProcessorContext`, sender,
callback execution authority, or permission to revive old work.

An ordered action shares a single-use ticket across cloned reducer outputs.
Inspection and cloning cannot multiply notification delivery. Runtime emission
admits the ticket without running user code. Direct reducer integrations may call
`admit`; receipt integrations call `admit_with_fence` with the original receipt
scope. Successful records require every captured source/receipt scope to remain
current. Interrupted history remains readable after its old scope expires, without
conferring fresh execution authority. History is never rolled back because
notification admission fails or its receiver is full or closed.

`max_records` and `max_bytes` cover reserved output tickets, queued records, and
borrowed records. Record leases are not cloneable. Text uses exact-sized immutable
UTF-8 storage; fixed per-record overhead is bounded by the count limit. Dropping
an unadmitted ticket or borrowed record releases its reservation. Copies of
`record.completion()` explicitly retained by the application are application-owned.
Payload-free `stopped_stats()` and receiver `stats()` expose rejection, staleness,
abandonment, handler failures and retained budgets. Overflow also emits a
nonfatal processor error where the surrounding output route supports error frames.

The receiver starts no tasks. `handle_next` invokes one handler inside the caller's
existing owned future, with a finite cooperative deadline. Returned errors and
counters omit handler error/panic payloads; the process panic hook still runs. A mutable receiver admits one handler at a time. Dropping that
future drops the handler and its record lease. Blocking code, caller-created
detached tasks, already transmitted external effects, and destructor panics during
external cancellation remain outside this guarantee. Calling a captured pipeline sender is subject to
that sender's own authority and cancellation contract; receiving text grants none.

## Lifecycle

End and Stop keep final record admission available after `prepare_end` closes tool
callbacks. They admit the final record, close the completion stream, then forward
the terminal. Already queued records can be drained by the caller. The pipeline
has no notification dispatcher to drain; the application owns and must join any
future in which it consumes records. This is an explicit receiver contract, not
source-compatible background callback scheduling.

Cancel closes core task admission before processor terminal handling. This API
therefore only synchronously admits its final interrupted record to the existing
bounded receiver and closes that stream. It does not spawn an asynchronous Cancel
callback. Cleanup and owner drop close admission; receiver drop discards queued
records. Repeated closure is harmless and preserves the first close reason. A
full receiver rejects the final record visibly but still closes. Terminal closure
also occurs when an unconsumed close action is dropped.

## Job identity

`AssistantJobReply::capture` binds an assistant instance to exactly one native
`JobContext` before inference starts. Binding is immutable for the entire assistant
session, including delayed first response frames and tool continuations. A second
binding is rejected even if no turn has started or a prior turn has finished.
Every replacement or concurrent job must own a fresh aggregator pair/pipeline.
The aggregator's existing terminal lifecycle is not reset for another job.

The adapter compares captured session/generation and the exact native job ID,
requester and worker. It ignores empty tool-only and interrupted completions and
returns at most one nonempty `JobResponse`. It never reads a mutable current-job
slot or sends a response outside its existing native handler owner. The runnable
`pipecat-agent-workers/examples/assistant_job.rs` uses a real HTTP `LlmProcessor`,
shared-context pair and `NativeWorker::on_job`, with a fresh session per handler.
A job cancellation drops that owned pipeline and completion receiver.

## Playback adapters and features

CLI playback acknowledgements preserve the completion action and admit it with the
original accumulated receipt fence. Common media consumers enable the additive
`pipecat-media-transport/assistant-history` feature (or the facade's
`assistant-history` feature) and use `AssistantSpokenHistorySink`.

The media ledger invokes `commit_ordered` synchronously under its invalidation
lock, then admits its ordered frames and record outside that lock with the exact
receipt fence. The adapter does not claim physical audibility, invent receipts,
or broaden source-success requirements. Its frame-only `commit` rejects before
mutation rather than silently discarding completion actions. Existing frame-only
custom sinks continue unchanged through the default `commit_ordered` projection;
they do not promise stopped-record delivery. Unsupported non-frame assistant
realtime effects fail explicitly rather than becoming a generic event channel.
Standalone media without the feature keeps turns outside its normal dependency
boundary. Registering a receiver alone cannot upgrade a custom frame-only sink.

## Verification boundary

The native fixtures exercise actual localhost HTTP, registered Rust tool
continuation, native job RPC, paired typed/VAD interruption, common-media synthetic
trusted receipts, CLI acknowledgements, clone-safe admission, count/byte limits,
handler cancellation/panic/error/timeout, stale inherited scopes and terminal
closure. Synthetic `Played` fixtures establish the ledger contract; no live
provider, hardware audibility, distributed job bus, Python compatibility wrapper,
or universal callback parity is claimed.

The handler unwind boundary includes handler construction, polling, and the
helper's own timeout-triggered destruction. If application code drops the entire
`handle_next` future, Rust runs destructors in that caller; a destructor panic
during external cancellation is outside this helper's catch boundary. A caught
panic still invokes the process panic hook.
