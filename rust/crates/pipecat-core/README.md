# pipecat-core

Native Rust frames and a bounded, bidirectional Tokio pipeline. There is no
Python interpreter, bridge, or Python runtime dependency.

```rust
use pipecat_core::{Direction, Frame, IdentityProcessor, Pipeline, PipelineConfig};
use std::sync::Arc;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let worker = Pipeline::new(vec![Arc::new(IdentityProcessor)])
    .start(PipelineConfig::default()).await?;
let sender = worker.sender();
let producer = tokio::spawn(async move {
    sender.send(Frame::text("hello"), Direction::Downstream).await?;
    sender.end().await
});
let (frames, report) = worker.collect().await?;
producer.await??;
assert!(report.errors.is_empty());
assert_eq!(frames.len(), 3); // Start, Text, End
# Ok(())
# }
```

## Traversal diagnostics

`Pipeline::with_heartbeats(HeartbeatConfig::default())` enables bounded root-sink
traversal monitoring; ordinary pipelines remain disabled. Read the latest counters
with `worker.heartbeat_monitor()`. Timeouts report stalled traversal without
cancelling business work. See [HEARTBEATS.md](HEARTBEATS.md) for pressure behavior,
composition limits, source differences and a credential-free runnable example.

## Processing and shutdown

Implement `FrameProcessor` with `async_trait`. Its `process` method receives an
owned `Frame`, `Direction`, and a cloneable `ProcessorContext`. Forward frames
with `ctx.push`; return `ProcessorError` to generate an upstream `ErrorFrame`.
Errors also remain in `WorkerReport.errors` and `worker.errors()`, even when an
upstream processor consumes them. Fatal errors, panics, internal backpressure,
shutdown timeouts, and cleanup failures make `join` return `Err`.

- Start has its own priority tier. Each processor sees Start before any other
  frame, and `Pipeline::start` waits until Start reaches the downstream boundary
- System frames have a separate lane and may run concurrently with one active
  normal frame. New normal work waits until the current system handler finishes;
  system processing remains responsive during a long normal-frame future
- Data and control frames share a FIFO normal lane
- Interruption cancels the active interruptible normal future and drops queued
  interruptible normal frames. Instance overrides and uninterruptible frames
  survive. System frames are never removed by an interruption
- End keeps normal-lane FIFO ordering at each node. Each processor runs `prepare_end`,
  drains tracked background work, then processes End. End is uninterruptible by
  default. The output End waits until all admitted upstream/downstream handlers,
  queued frames, and tracked descendants drain, and appears last at the output.
  The shutdown deadline bounds this drain. New ordinary external input is rejected after End is admitted;
  Interruption, Resume, and Cancel remain possible until completion
- Cancel bypasses normal-input backpressure, drops queued normal frames, cancels
  active normal processing and all tracked background tasks, then cleans up.
  A running system handler is not preempted by another system frame; a stuck
  asynchronous system handler is bounded by the shutdown deadline
- Stop drains like End but skips processor `cleanup`. It terminates this worker;
  there is no automatic resume/re-attach API. Owners retaining processor Arcs
  are responsible for eventual cleanup of stopped processor resources
- Dropping a running worker initiates cancellation and cleanup. `join(&mut self)`
  can be cancelled and called again. A successful join is consumed once

Use `ctx.spawn(name, cancel_on_interruption, future)` for model streaming and
other long operations. It monitors task results and panics. A task that opts
out of interruption still stops on Cancel. Keep a task ID for explicit
cancellation through `ctx.cancel_task(id)`. Implement `prepare_end` to cancel
persistent timers and flush state before End waits for background work.
Futures must cooperate with Tokio cancellation and avoid blocking runtime threads.

Interruption cutoffs are reserved at queue admission. Work accepted after a
cutoff survives that interruption; a later distinct interruption can still
cancel it. Contexts retain their causal epoch across forwarding, so an old
producer cannot relabel delayed output as fresh at an already-interrupted node.
Stale interruptible emissions are discarded and counted, while forwarding an
already-admitted system frame and opted-out background emissions remain allowed.
A deliberate `broadcast_interruption` refreshes the current invocation; context
clones captured before it keep their older epoch.

Nested pipelines can use `sender.try_send_from(frame, direction, source)` for
nonblocking ingress and `destination.forwarding_context(source)` for output
relays. Both preserve source epochs. Buffered forwarding contexts retain source
lineage and check origin/destination cutoffs under consistently ordered admission
locks. Full child lanes return explicit backpressure. Native runtime epochs are
shared across interruption clones; unrelated runtime/session resets must be
coordinated by the owner before exchanging generation-sensitive work.

Processor-owned child workers can additionally use `Pipeline::inherit_lifecycle(ctx)`.
Their End/Stop boundaries stage completion while the whole composition drains,
including reverse traffic from later `prepare_end` calls and owned tasks. A child
continues accepting nonterminal internal ingress from its exact owner processor;
ordinary external input remains closed. One shared admission lock seals all
runtimes only when queues, handlers, and task-result publication are idle. Root
End follows descendant cleanup; root cleanup still follows its normal supervisor
contract. Cancel remains available during descendant cleanup, and Stop retains
processor resources for explicit owner cleanup. Child membership is weak and
bounded to 4,096 live runtimes; completed entries are pruned during registration.
A root can report shutdown failure while previously started descendant cleanup
is still running within that child's own `shutdown_timeout`; root failure or drop
does not restart this deadline or promise successful cleanup. An inherited child
cancels if its owner terminates or disappears. Consumers must
still drain each child output stream; untracked external relay tasks do not gain
implicit ownership through lifecycle inheritance.

Native interruption clones and broadcast siblings share a logical epoch token,
including after bounded ID-history eviction. `duplicate()` starts a new logical
event. Deserialization creates a fresh local token before callers can clone the
frame. The token is runtime-only and is not serialized: remote transports must
enforce their own session/generation and replay contract. ID replay recognition
across separately decoded wire events retains the latest 512 events.

`ctx.broadcast` creates fresh paired frame IDs. `ctx.broadcast_interruption`
also interrupts the originating processor. `ctx.queue` queues at the same
processor without waiting on its own queue. A Flush frame traverses downstream,
upstream, and downstream again; the final output acknowledges the probe. It
does not wait for arbitrary work spawned after the probe or coordinate flushes
across distributed workers.

For `FlushDetailed`, the local completion event is set after reserving worker
output capacity and before publishing the returned marker. Full or already
closed output rejects admission without setting the event. Closing the receiver
after reservation does not revoke completion; the event acknowledges admission,
not consumption by the receiver.

## Backpressure contract

Each processor has independently bounded normal/system lanes and one reserved
Start slot. External `sender.send` waits for capacity. System input bypasses the
normal-input lock, so paused/full data cannot prevent Resume or Cancel.
Every external capacity retry rechecks lifecycle admission under the same lock
that closes input. End/Cancel wake pending producers to reject ordinary input;
Cancel can close input even while its own system lane is backpressured.

Internal `ctx.push` / `ctx.queue` **do not wait** on a full adjacent/self queue.
They return an error; adjacent overflow also fails the worker. Waiting internally
could deadlock when adjacent processors emit in opposite directions or when a
processor queues into its own full lane. This is an explicit fail-fast policy,
not an infinite queue or silent drop. Size capacities for expected bursts.

The output queue is bounded. Consume `recv` concurrently with producers or use
`collect`. An absent/slow consumer causes an explicit backpressure failure.
Background registration has a per-processor task limit. Error history retains at
most `error_history_capacity` records (256 by default); `total_errors` and
`evicted_errors` remain available on the worker and its report. A capacity of zero
disables history retention while preserving counts and upstream error delivery.
Native error-frame clones share runtime-scoped reporting bookkeeping: each visited
runtime records an error once, even if an upstream handler consumes it or the
retained history entry is later evicted. The bookkeeping lives with the retained
error and grows with the number of runtimes that error visits; it is not serialized.
`duplicate()` gives a fresh error identity and fresh reporting bookkeeping.
Deserialized errors also initialize this shared bookkeeping before cloning;
ordinary non-error frames do not allocate an error-reporting set.

Defaults: 128 normal frames, 64 system frames, 1 Start slot, 1024 output frames,
128 concurrent background tasks per processor; 10-second startup and 30-second
shutdown deadlines. Limits count frames/tasks, not bytes per payload. Hook
timeouts cannot preempt CPU loops or blocking code.

## Representation and compatibility

`FrameData` has typed payloads. JSON is confined to metadata, tool arguments,
results and schemas, provider raw results, and transport-specific messages.
Audio is signed PCM16 little-endian; `AudioRawFrame::new` validates complete
interleaved samples. `Clone` preserves identity; `duplicate` allocates a new ID.
Set `interruptible` before enqueueing: Rust frames use ownership instead of
Python's shared mutable objects. Each frame has a source-class name and class-local
instance counter. Local callbacks, transforms, processor references and flush
events remain executable within Rust and explicitly reject serialization.

Sealed realtime contexts retain a separate tiny identity token for each direct
tool entry, at most `MAX_REALTIME_CONTEXT_TOOL_IDENTITIES` (4,096), in addition to
the 1 MiB serialized context bound. `retained_tool_identity_count()` includes
repeated entries that share a handler. Handler clones share a token; replacing a
handler creates a distinct token. Retained causes keep those identities live
without retaining callbacks, their captures, or another context-history copy.
The tokens and direct callbacks cannot be serialized or reconstructed from wire
data. An over-bound attachment fails without changing the frame or producer sequence.

The source fixture covers all 134 Frame-derived classes at the pinned Python
revision. Tests instantiate all 161 native variants and compare priority and
interruptibility to the static Python AST export. This includes base frame
classes and deprecated worker/task aliases; it does not imply that every
consumer behavior from every Python provider has been ported.

[COMPATIBILITY.md](COMPATIBILITY.md) maps source classes and lists omitted types,
field differences, and runtime gaps. This crate does not claim full Python API
compatibility.

## Verification

```sh
cargo test -p pipecat-core
cargo clippy -p pipecat-core --all-targets -- -D warnings
cargo fmt -p pipecat-core -- --check
```

Tests use four-thread Tokio runtimes and cover stable queue priority, lifecycle
ordering, cancellation/interruption, bounded queue pressure, tracked tasks,
setup/cleanup failures and panics, broadcast IDs, and flush round trips. They
need no network or API keys.

## Global observers

Register `Arc<dyn PipelineObserver>` with `Pipeline::add_observer` or
`with_observer` before startup. Each observer has bounded independent dispatch,
explicit loss/failure counters, and owned cancellation. Setup, process-entry,
accepted-push and Start-completion timestamps are captured at runtime boundaries;
callbacks never run inline on processor paths. Summaries omit raw frame content,
metadata, errors and tool payloads. Numeric MetricsFrame records are capped at
64 per summary with an explicit omitted count.

`ObserverOptions` controls queue capacity, repeated-push filtering and graceful
drain timeout. `ObserverHandle` remains readable after shutdown. End/Stop drains
within the observer deadline; cancellation aborts outstanding callbacks. A panic
or slow observer cannot fail/backpressure the pipeline, provided callbacks yield
rather than blocking Tokio threads. `Clone` shares first-push identity, including
pre-admission clones; duplicate and decoded frames receive fresh identity. This
adds one small per-frame identity allocation, and its lazily populated group set
is freed with the frame's final clone. No event timestamps, summaries or dispatch tasks
are produced when there are no registrations.

Nested runtimes should call `Pipeline::inherit_observers(&parent_context)`, so
all branches share one observer group and clock, with root-owned dispatcher
shutdown. Runtime/processor IDs distinguish identically named processors. Use
`ProcessorContext::observer_clock` for service timers on that same timeline.
`pipecat-observers` supplies startup/transport/turn reducers, explicit measured
service timers and a bounded metric stream; see its README for detailed scope.

## Processor usability and worker policy

Every running processor has a `ProcessorHealth` handle, initially usable.
Processors access it through `ctx.health()`, `ctx.is_usable()` and
`ctx.set_usable(bool)`; applications can use `worker.processor_health(index)`.
Override `FrameProcessor::health()` with a stable shared handle to configure
health before startup or restore it without retaining a processing context.
Default handles are scoped to a runtime/processor index, even when a processor
Arc is reused. An explicitly supplied handle shares health wherever its owner
chooses to reuse it. Handles own no tasks and do not retain the worker.
An error report through an already-finished runtime context is rejected before
changing health, so an old retained context cannot poison a later run's handle.

`ctx.push_error` and returned `ProcessorError` values mark their reporting
processor unusable for Authentication, Authorization and InvalidRequest. Unknown,
RateLimit, Quota, Connectivity, Server and Application leave its health unchanged;
they do not restore an already unusable processor. Exhausted application recovery
can use `ctx.push_error_with_permanence(error, true)` without changing the error's
category. Classification is explicit: the runtime does not guess a provider cause
from arbitrary exceptions, retry requests, or reconnect a provider.

Health is updated before the error is recorded or pushed. `Frame::error_origin()`
exposes local attribution with a stable runtime/index identity, the shared live
health handle and an immutable report-time snapshot. The display name in
`ErrorFrame.processor` is never used to look up a processor. Clone and duplicate
preserve attribution; serialization excludes it and decoding always creates a
frame without local attribution. Unattributed incoming errors still reach output
and history, but cannot alter local processor health or trigger unusability
policy solely by naming a processor.

`PipelineConfig::processor_unusable_policy` selects:

- `Continue` (default): report the error and leave the worker running
- `End`: close external ordinary input, then append one End after admitted normal
  frames, run `prepare_end`, drain owned work and perform normal cleanup
- `Cancel`: close input and deliver one Cancel through the system lane, abandoning
  queued/active normal work and cancelling owned background tasks before cleanup

The policy runs only for an attributed, nonfatal error reaching the worker's
upstream boundary while the originating processor is **currently** unusable.
An upstream processor can consume the error to recover it; history still retains
the error, but the worker policy is not applied. Direct `set_usable(false)` alone
does not trigger shutdown. Each attributed runtime/processor is acted on once
per worker, even if it later recovers and fails again. Runtime identities remain
distinct across nested pipelines and repeated use of identically named processors.
`worker.unusable_processors()` exposes those identities. A local forwarded error
retains its original attribution across runtimes; wire-decoded errors do not.

Terminal admission is owned by the existing supervisor. A full input lane defers
that one request rather than waiting inside a reporting processor, blocking behind
a producer's input lock, or spawning an untracked task. The existing shutdown
deadline bounds admission and drain; full output remains an explicit backpressure
failure. Repeated errors cannot enqueue a policy loop. Fatal errors retain the
native runtime's existing `Failed` / `join() -> Err` contract under every policy;
the fatal flag alone does not imply unusability. This differs from the source's
deprecated fatal flag, which cancels its worker.

### Gating, recovery and concurrency

Plain source `FrameProcessor` continues processing while unusable; service code
chooses which operations to gate. The native trait preserves this behavior with
`process_when_unusable(frame, direction)` returning true by default. Override it
to return false only for work that must be discarded. The hook is evaluated for
normal frames at invocation entry in **both** directions. System/Start and all
terminal frames always bypass the hook, preserving lifecycle, recovery controls,
error routing and cleanup. A drop increments `dropped_frames` and emits no
process-entry observation because the processor was not invoked. A service that
must clear buffered state on unusability can instead keep receiving relevant
control frames and use `ctx.is_usable()` inside its own work path.

Health transitions are serialized and idempotent. Each actual change increments
a revision. `health.watch()` is a latest-value watch channel whose revision reveals
coalesced transitions; callers must not retain a watch borrow across an await or
setter. Pipeline observers receive `ProcessorUsabilityChanged` before the related
error push, using the existing bounded dispatch, explicit loss counters and owned
shutdown. Health observers detach when the runtime finishes.
Concurrent setters can interleave observer delivery; revisions define the
transition order, and the shared snapshot remains authoritative. Injected clocks
run outside health locks so instrumentation can safely inspect that snapshot.

The invocation-entry health check is the work-admission decision. A change after
that decision does not cancel an active call or its already-issued effects.
Restoration admits later work, never replays discarded work, and does not rescind
an End/Cancel already requested at the boundary. A delayed error may therefore
have an unusable report-time snapshot but reach the boundary after restoration;
the live boundary verdict suppresses policy, matching the source. Conversely,
a permanent error **reported after** restoration makes the processor unusable
again, even if its operation started earlier. Applications must coordinate their
old operations when changing provider settings; no automatic effect replay or
stale-operation retry is performed.

### Source mapping and current scope

- `src/pipecat/processors/frame_processor.py`: `is_usable`, `set_usable`,
  `on_usable_changed` and permanent-error reporting map to context/shared health,
  revisioned watches, bounded health observations and `push_error_with_permanence`
- `src/pipecat/pipeline/worker.py`: `ProcessorUnusablePolicy` and
  `_handle_unusable_processor` map to worker policy at the upstream boundary,
  with once-per-processor accounting and current-health checks
- `tests/test_processor_usable.py`, `test_processor_unusable_policy.py` and
  `test_processor_usable_gating.py` inform `tests/usability.rs`: permanent versus
  transient categories, forced permanence, recovery, all three policies and
  selective service-work gating. Native tests additionally cover bidirectional
  admission, full queues, cancellation, delayed errors, attribution isolation,
  observers and concurrent setters
- `examples/features/features-processor-usable.py` maps to the credential-free
  `examples/processor_usable.rs` for reporting, recovery and worker policy only.
  Provider switching itself is not implemented by this crate

`HttpTtsProcessor`, `CartesiaTtsProcessor`, `DeepgramSttProcessor`,
`ElevenLabsTtsProcessor`, `ElevenLabsSttProcessor`, and `WhisperSttProcessor`
(including its runner owner) implement scoped selective health gating and recovery.
See the `pipecat-speech`, `pipecat-streaming-speech`, `pipecat-elevenlabs`, and
`pipecat-whisper` READMEs and their usability regression tests for supported work,
settings, buffered-state cleanup, and recovery behavior. The native HTTP/SSE
`LlmProcessor` also exposes stable health and selective inference gating. Other
adapters remain unaudited; this is not a claim of workspace-wide service gating.

```sh
cargo run -p pipecat-core --example processor_usable -- continue
cargo run -p pipecat-core --example processor_usable -- end
cargo run -p pipecat-core --example processor_usable -- cancel
```

### Compound processor support

`ProcessorContext::processor_identity()` returns the genuine local
runtime/index identity used in error attribution. Compound processors should
compare it to `Frame::error_origin`, not resolve errors by a display name.
`broadcast_interruption_frame(&frame)` validates an Interruption frame, applies
one causal cutoff to this processor, and emits its two broadcast siblings. The
same frame can then be admitted to child workers to share the cutoff. It returns
false for stale invocations or already-applied interruption tokens, without
rebroadcasting. Existing interruption helpers use this implementation.

`ProcessorHealth::read_only(initial)` returns a cloneable public health handle
and a separate `ProcessorHealthController`. Only the controller can change the
view. Public setters and category-based permanent errors cannot overwrite it.
This allows compound processors to publish derived health while their own private
controller synchronously refreshes the verdict before policy-relevant errors.
The view keeps the same revision/watch/observer behavior and owns no tasks.

`ProcessorError::with_permanence_override(bool)` changes only the local health
classification of a returned error. `ProcessorContext::push_error_with_permanence_override`
accepts an `Option<bool>` for directly reported errors: None applies category
classification, true marks unusable, and false preserves the current health.
False does not restore an already unusable processor and cannot soften fatal
failure. This is useful for rejecting a settings proposal while retaining the
last valid configuration and its usability. The reported error category and wire
frame remain unchanged. Use ProcessorError constructors/builders rather than
struct literals. The older `push_error_with_permanence(error, false)` continues
to use category classification; its true argument still forces permanence.

`ProcessorContext::is_current()` observes whether the invocation and its inherited
origins still have a current causal generation. It does not reserve admission,
check worker liveness, or alter the core rule permitting a system frame to forward
itself after interruption. Compound output gates can use it to reject stale
interruptible metadata separately from that system-frame routing rule.

For an actual output safety barrier, use `push_with_causal_fence(frame, direction)`.
It validates the invocation and inherited generations under the same ordered
source/destination admission locks as routing, including same-ID system frames.
It preserves ordinary `push` semantics elsewhere, finite backpressure, and explicit
uninterruptible-frame/task survival. A separate `is_current()` check followed by `push`
is not an atomic fence and must not be used as one.

`LocalFrameTag` provides opaque process-local provenance without retaining a
processor/runtime. Marking is idempotent for one tag; a frame may carry at most
64 distinct tags, with explicit failure on overflow. Clone and duplicate retain
tags; equality and serialization ignore them, and deserialization creates an
untagged frame. Tags are suitable for private copies inside owned in-process
branches, not for trust or routing across a wire boundary or reconstructed frame.
