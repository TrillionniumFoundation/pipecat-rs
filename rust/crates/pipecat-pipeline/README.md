# pipecat-pipeline

Native parallel composition of `pipecat-core::Pipeline` workers. Each branch has
real processor setup, separate system/normal queues, background task ownership,
bidirectional routing, interruption epochs, supervision, and cleanup. No Python
runtime or list-transformation substitute is involved.

```rust
use pipecat_core::{Frame, Direction, IdentityProcessor, Pipeline, PipelineConfig};
use pipecat_pipeline::{FrameOrder, SyncParallelPipeline};
use std::sync::Arc;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let parallel = SyncParallelPipeline::with_order(
    vec![vec![Arc::new(IdentityProcessor)], vec![Arc::new(IdentityProcessor)]],
    FrameOrder::Pipeline,
)?;
let worker = Pipeline::new(vec![Arc::new(parallel)])
    .start(PipelineConfig::default()).await?;
let sender = worker.sender();
let producer = tokio::spawn(async move {
    sender.send(Frame::text("hello"), Direction::Downstream).await?;
    sender.end().await
});
let (frames, report) = worker.collect().await?;
producer.await??;
assert_eq!(frames.len(), 3); // Start, one shared Text identity, End.
assert!(report.errors.is_empty());
# Ok(())
# }
```

## Semantics

- `ParallelPipeline`: every branch receives each input immediately, and unique
  output identities merge as they reach branch bookends. A slow normal handler
  does not block another branch or the parent's system lane
- Start keeps the original frame identity and metadata. All branches finish
  processing Start before the parent emits it; any early output follows Start
- End and Cancel are emitted once, after every branch reaches its terminal
  state **and finishes cleanup**. End-buffered output precedes End. Cancel can
  supersede a pending End and cancels active branch work
- `SyncParallelPipeline`: each ordinary input is followed by one unique control
  sentinel per branch. Output is released only after all sentinels arrive in the
  input direction. `Arrival` preserves capture order; `Pipeline` uses branch
  definition order, preserving order within each branch. ID deduplication occurs
  after that ordering and resets for the next input
- Output preserves its actual direction, including output emitted in the reverse
  direction. A branch must forward unknown control frames. Dropping a sentinel
  produces an explicit synchronization timeout
- System frames bypass per-input synchronization. Start, End, Cancel, and Stop
  have lifecycle barriers. Branch-originated interruptions invalidate the parent
  and sibling branches while preserving both broadcast directions
- Relay contexts snapshot the producing branch's causal generation. Admission
  and output validate source and destination under ordered core admission locks.
  Buffered old output cannot become fresh because an interruption occurred;
  uninterruptible frames retain the core's survival semantics
- Cleanup signals all owned worker monitors, cancels live workers, and joins
  them. Dropping an outer running worker triggers the same cleanup through core
  supervision. Startup futures canceled before worker construction follow core's
  bounded asynchronous startup-cleanup/Drop contract

The FIFO sentinel is a barrier for synchronously handled output, not arbitrary
background work started by a branch. This restriction also exists in the Python
source. End uses each branch runtime's stronger background-draining lifecycle.
For per-input model streaming, finish the output inside the processor invocation,
or supply an explicit completion-aware branch processor.

## Finite limits and errors

`ParallelPipelineConfig` defaults to at most 64 branches, 4,096 buffered frames,
a 16,384-ID recent deduplication window, and a 30-second synchronization timeout.
Child queue, background, startup, shutdown, and error limits use `PipelineConfig`.
Limits count frames/IDs/tasks, not payload bytes. Applications handling large
media frames must also bound their payload sizes.

Branch ingress is nonblocking and fail-fast, like core's internal routing. A
full normal/system queue or buffer fails the parent explicitly instead of
creating an unbounded backlog or waiting in a bidirectional cycle. Account for
both an input and its sentinel when sizing synchronized branch normal queues.
Consume the outer output concurrently; core's bounded outer output may fail if
its consumer stops reading.

A failed/panicked branch fails the parent and cancels peers. Missing sentinels,
withheld lifecycle frames, and monitor cleanup have finite deadlines. A branch
that terminates without a parent lifecycle request is treated as a failure.
Nonfatal processor errors continue upstream through the normal frame path and
are recorded in the parent report even when an upstream processor consumes them.

## Source correspondence and deliberate differences

Source revision: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

- `src/pipecat/pipeline/parallel_pipeline.py`: concurrent branches, identity
  deduplication, Start-first buffering, End/Cancel-last barriers
- `src/pipecat/pipeline/sync_parallel_pipeline.py`: control sentinels, per-input
  synchronization, arrival/definition ordering, system-frame bypass

Python ParallelPipeline retains `_seen_ids` forever. Rust deliberately retains
only the configured recent window; replaying an identity after eviction may
emit it again. The window must cover expected branch skew. Both the eviction
behavior and normal duplicate suppression have runtime tests.

Python SyncParallelPipeline bypasses Start/Cancel synchronization; Rust applies
its all-branch lifecycle barrier to both compound types. Rust also forwards End
exactly once even when branch flush output precedes it, and emits reverse-
direction output in its original direction. The source's End/split-output queue
edge cases can omit End or leave reverse output pending; those behaviors are
not reproduced. Sync's original system frame is not leaked again in a later
ordinary batch.

Lifecycle input must travel downstream, as required by native core admission;
branch-generated upstream terminal output is relayed without ending the worker.

A native `SyncFrame` wraps a uniquely identified `FrameData::Control`; its exact
identity is registered privately and consumed at the bookend. It is not a new
variant in the core frame catalog. Rust frame clones preserve identity but do
not reproduce Python's shared mutable frame objects. Stop follows native core's
stop-without-cleanup contract; owners retaining a stopped compound processor
can explicitly call its `FrameProcessor::cleanup` when finished with it.

The JSON source fixture is a manually derived, pinned source contract, not an
assertion that Python was executed. The multithreaded Rust tests exercise actual
branch processors and native workers, including slow/failed branches, both
output orders and directions, same-ID mutation ordering, lifecycle flushes,
interruptions, full queues, buffer overflow, timeout, and Drop cleanup.

## Verify

```sh
cargo test -p pipecat-pipeline --locked --offline
cargo clippy -p pipecat-pipeline --all-targets --locked --offline -- -D warnings
cargo fmt -p pipecat-pipeline -- --check
```

## Service and LLM switching

`ServiceSwitcher` uses the same owned native branch workers as `ParallelPipeline`.
It is not a list transform: every member receives setup, Start, interruptions,
End/Cancel, background-work supervision, and cleanup. Directional input/output
gates select one service for ordinary work and suppress inactive metadata/output.
Use `ServiceSwitcherStrategy::Manual` (default) or `Failover`; the first member
is initially selected. Empty lists and repeated processor identities are rejected.

```rust
use pipecat_core::{Direction, IdentityProcessor, Pipeline, PipelineConfig};
use pipecat_pipeline::ServiceSwitcher;
use std::sync::Arc;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let switcher = Arc::new(ServiceSwitcher::new(vec![
    Arc::new(IdentityProcessor), Arc::new(IdentityProcessor),
])?);
let worker = Pipeline::new(vec![switcher.clone()]).start(PipelineConfig::default()).await?;
worker.sender().send(switcher.switch_frame(1).unwrap(), Direction::Downstream).await?;
worker.sender().end().await?;
worker.collect().await?;
# Ok(()) }
```

- `switch_frame(index)` creates an unambiguous local processor reference.
  `ManuallySwitchService(Name(...))` also works when exactly one member matches.
  Unknown, ambiguous, or currently unusable destinations do not switch; the
  original control frame continues in its original direction for another switcher
- Settings normally traverse only the active branch. An addressed inactive member
  receives a private duplicate; `reach_inactive_services` without an address sends
  duplicates to every inactive member. An address always takes precedence.
  Private copies are consumed on output, while the original follows the active
  service's forwarding/consumption behavior. Both directions and all four settings
  families use this rule. Configuration already admitted to a branch survives a
  switch interruption and stays FIFO ahead of its metadata refresh
- Every successful selection advances the selection revision. For direct service
  members, including selecting the same member again, it requests fresh metadata;
  `watch_selection()` exposes that revision. Only selected
  metadata escapes, including during startup failover. Startup requests wait for
  branch senders, and refreshed metadata is queued behind admitted settings
- Failover recognizes genuine local runtime/index error attribution, never display
  names or decoded metadata. Only a nonfatal upstream error from the active,
  currently unusable member switches to the next usable member, wrapping in list
  order. An inactive member's nonfatal error is consumed. Healthy transient errors
  and unrelated errors pass through; fatal failures still fail the compound worker
- Successfully recovered errors stop before the parent worker's unusability policy.
  If no switch occurs for an unusable active member, the error is reattributed to
  the switcher with category Unknown. Its health is recomputed before reporting,
  so Continue/End/Cancel judges whether *any* member remains usable. Error history
  in the reporting child remains available to inherited observers; consumed errors
  are not emitted into the parent worker's error history
- `is_usable()` derives the OR of member health. `service_health(index)` allows
  explicit application recovery; it never selects, retries, or automatically
  falls back. The parent runtime's switcher health handle is read-only: attempts
  to set it, including permanent-error classification, cannot overwrite its
  derived status. Owned member watches update aggregate notifications; `is_usable()` and policy-relevant
  reporting refresh the result without depending on watcher scheduling. Direct
  snapshots of a retained public handle are latest-known values; watcher delivery
  is asynchronous and stops at cleanup

### Switching, interruption, and effects

A **different-member** selection emits one causal interruption into the parent
and all children. This intentionally strengthens the Python switcher's filters:
old interruptible work is canceled/fenced, so reselecting a member cannot release
its previous interruptible response. Parent and children share the same epoch;
future requests are not rejected as belonging to an older independent epoch.
Already admitted normal input can be abandoned by that interruption, just as in
core. Settings copies are deliberately uninterruptible to preserve configuration.

Explicitly uninterruptible frames/tasks retain core's survival semantics. Local
cancellation cannot undo a remote request already received by a provider, a tool
already executed, or an application task that deliberately opted out. Applications
must reconcile ambiguous external outcomes before deciding to submit again.
There is no automatic replay, even when failure occurs before any visible text.

Selection watches are bounded latest-value notifications, not FIFO barriers.
A nested selection reports the child's decision; before submitting new root input
in response, observe its upstream interruption at the root boundary. Simultaneous
health changes use the live admission-time verdict; a later failure can still
invalidate an admitted operation. Selection is frozen once terminal draining
begins. End/Cancel and Drop join branch cleanup and the health watcher tasks;
Stop retains core's explicit later-cleanup contract.

### LLM capability and tools

`LlmSwitcher` accepts `Arc<dyn pipecat_services::LlmInference>` members; the native
`LlmProcessor` implements that capability. `llms()`, `active_llm()` and
`service_switcher()` expose members, selection, and health. Context frames execute
only on the selected member. Tool definitions from context, plus explicit
`LlmSetTools`/`LlmSetToolChoice`, also configure inactive members without inference.

The pinned Python `LLMSwitcher.process_frame` syncs direct-function handlers but
**does not cache or rerun context on a switch**. `ServiceSwitcher` only queues
`ServiceSwitcherRequestMetadataFrame`; `AIService` answers it with metadata.
Applications that want another response provide a fresh context or explicitly
request `LlmRun`. A native `LlmRun` uses that member's current context, which can
be older after a switch; a full fresh context is the safe conversation handoff.

Native tool execution belongs to one shared downstream
`pipecat_processors::tools::ToolExecutor`, not each provider. Register callbacks
and their explicit timeout/cancellation options once there; they remain available
for every selected LLM and share duplicate-call tracking. Source Python decorators,
`register_direct_function`, automatic executable `ToolSet::Direct` registration,
and provider-specific arbitrary `**kwargs` are not simulated. Unsupported direct
handler schemas fail explicitly rather than losing behavior or executing hidden
callbacks. Supply ordinary tool schemas plus registered native callbacks.

`run_inference(context, InferenceOptions)` makes an explicit out-of-pipeline
provider request using the member selected when first polled. It snapshots that
member's settings, applies per-call sparse overrides, and never mutates conversation
state, emits pipeline response frames, executes tools, retargets on a switch, or
retries automatically. The caller owns cancellation; dropping the future closes
its stream. It is independent of pipeline End/Cancel. The native implementation
uses the existing bounded HTTP/SSE client and accumulates text up to a configurable
limit (1 MiB default); it does not claim the Python providers' nonstreaming wire
protocols. Configured client retry policies remain explicit opt-ins.

### Source contract and local verification

`tests/fixtures/switching_contract.json` pins source files and deliberate native
boundaries at revision `af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- `pipeline/service_switcher.py` and `tests/test_service_switcher.py`: manual
  selection, ordered failover, settings routing, health, errors, and metadata
- `pipeline/llm_switcher.py` and `tests/test_llm_switcher.py`: active inference,
  context/tool continuity and explicit registration policy
- `services/ai_service.py` and `services/llm_service.py`: metadata-only refresh;
  LLM inference is not initiated by switching

`tests/switching.rs` uses actual branch processors, owned pending tasks, nested
workers, fatal failures and terminal policies. `tests/llm_switching.rs` uses real
loopback HTTP/SSE LLMs and a shared native ToolExecutor to verify no failed-request
replay or duplicate side effects, settings/tool continuity, inference snapshots,
output bounds, and cancellation. No Python runtime, credentials, or external
provider calls are used. The runnable example exercises primary failure and an
explicit fresh backup request:

```sh
cargo run -p pipecat-pipeline --example llm_switching --locked --offline
cargo test -p pipecat-pipeline --locked --offline
```

Private settings/tool-definition copies carry one opaque local provenance tag
per owning switcher, rather than an evicting identity history. A slow inactive
branch therefore cannot leak a forgotten copy even with a tiny deduplication
window or many intervening updates. Nested owners preserve each other's tags;
64 tags per frame is the explicit nesting bound. Copies must stay in the owned
in-process branch path and preserve the Frame on forwarding: serialization or
constructing a new Frame intentionally drops local provenance. Tags own no
runtime and are released with their frames.

Nested compounds support routing, interruption, health and owned cleanup. Metadata
refresh is supported for direct service members. As in the pinned Python filter,
metadata is checked against the selected member's name: a nested switcher's leaf
name differs from the compound name, and refresh requests targeting that compound
are not retargeted to a leaf. Nested leaf metadata forwarding/refresh is therefore
not claimed; configure metadata-aware services directly when it is required.
