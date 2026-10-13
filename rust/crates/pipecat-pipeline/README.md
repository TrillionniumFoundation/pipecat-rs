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
- Nested branches share one graceful commit. A child End first stages at its
  boundary while branches remain able to receive already-admitted reverse work,
  including later `prepare_end` and background output. The root closes the group
  only after every queue, active handler, and owned task result has drained;
  descendant cleanup completes before the root terminal is published
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
  bounded asynchronous startup-cleanup/Drop contract. A failed or dropped root
  can finish reporting before already-started descendant cleanup times out;
  those cleanup futures keep their own bounded child deadlines

The FIFO sentinel is a barrier for synchronously handled output, not arbitrary
background work started by a branch. This restriction also exists in the Python
source. End uses each branch runtime's stronger background-draining lifecycle.
For per-input model streaming, finish the output inside the processor invocation,
or supply an explicit completion-aware branch processor.

## Finite limits and errors

`ParallelPipelineConfig` defaults to at most 64 branches, 4,096 buffered frames,
a 16,384-ID recent deduplication window, and a 30-second synchronization timeout.
Child queue, background, startup, shutdown, and error limits use `PipelineConfig`.
A composition can contain at most 4,096 live child/root runtimes.
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
for every selected LLM and share duplicate-call tracking. For executable context
schemas, use the opt-in `DirectToolBinding` described below. Bare native LLMs still
reject direct handlers instead of silently discarding their execution behavior.
Source Python decorators, `register_direct_function` and provider-specific
arbitrary `**kwargs` are not simulated.

### Advertised application handlers

`DirectToolBinding::new(llm)` accepts the ordinary native `LlmProcessor`;
`DirectToolBinding::new_switcher(switcher)` accepts the existing `LlmSwitcher`.
Both return one owner with `stages()`: incoming projection, native inference,
shared execution, and reverse projection. Keep these stages together in that
order, with the conversation's user/assistant aggregators outside the binding.

```rust,no_run
use pipecat_core::{DirectFunction, Frame, FrameData, LocalToolFunction, ToolDefinition, ToolSet};
use pipecat_pipeline::DirectToolBinding;
use pipecat_services::LlmProcessor;
use std::sync::Arc;
# fn example(llm: Arc<LlmProcessor>, definition: ToolDefinition) -> Result<(), Box<dyn std::error::Error>> {
let function = DirectFunction {
    schema: definition,
    handler: LocalToolFunction::new(|arguments| async move { Ok(arguments) }),
};
let binding = DirectToolBinding::new(llm)?;
let pipeline = pipecat_core::Pipeline::new(binding.stages());
let tools = Frame::new(FrameData::LlmSetTools(ToolSet::Direct(vec![function])));
// Send tools or a detailed context carrying these handlers to the running pipeline.
# let _ = (pipeline, tools);
# Ok(()) }
```

The initial callback selection is empty. Each ordinary detailed context or
`LlmSetTools` frame atomically replaces it with exactly the advertised `Direct`
handlers, then forwards plain definitions to the LLM. Handler-free declarations,
empty tool sets and `NotGiven` prune selections. A later schema can re-advertise
the same handler or replace it with a new application callback. Both directions
support this update; the far bookend forwards the projected frame without pruning
again. Legacy `LlmContext` frames carry no executable handlers and prune selections
after full native validation, without rewriting the frame. Full native context
conversion and tool validation precede publication.
Fixed native LLM bindings preflight projected ordinary contexts and tool setters
with the actual LLM's current settings and client before altering handlers or
recording cleanup hooks. Supported owning OpenAI declarations and empty Gemini
Google Search declarations are advertisement
alongside standard callbacks; custom metadata grants no registration authority.
Client restrictions, reserved fields, grouped schemas and
sealed/speculative authority changes reject before altering handlers or recording
cleanup hooks; capabilities are never stripped or resealed. Switcher bindings
keep provider-agnostic strict custom-schema validation.

The single executor keeps admitted callbacks, their timeout/interruption options,
the settlement journal exposed by `outcomes()`, and its recent duplicate identities
across every update and model switch. Provider requests crossing an update use the
selection current when their tool batch is admitted. Cancellation-aware handlers
return `None` through `LocalToolFunction::new_cancellable`; successful JSON null
and callback errors retain their separate native outcomes.

Successful handlers can use `LocalToolFunction::new_with_result` or
`new_cancellable_with_result` and return
`LocalToolResult::new(value).with_run_llm(false)` to suppress a new continuation
request from that result. `true` requests continuation even while a sibling tool
is pending; ordinary speaking, active-response and playback gates still apply.
`None` retains the context consumer's default group policy. A false result policy
does not retract a continuation already requested by another result.

The flag is separate from user JSON and is sealed into the owned final result.
Existing Value callback constructors and invocation methods retain their return
types; the Value invocation methods deliberately discard result metadata.
Executors use the result-preserving invocation path. Cancellation retains its
own policy and takes priority; callback errors and oversized results use the
existing error path without a successful-result override. Options, handler
identity, snapshot selection, cleanup and result-byte bounds are unchanged.
Python parity applies here to nonempty, truthy results:
the pinned Python consumer ignores properties for falsy results, while the
existing native consumer respects explicit flags for those values as well.
Loopback HTTP tests do not qualify a cloud provider's acceptance of continuation
with a pending sibling's placeholder result.

`LocalToolFunction::new_with_reporter` supplies a `LocalToolReporter` for ordinary
live intermediate results. It defaults to `cancel_on_interruption=false`;
enabling interruption cancellation on a reporting handler is rejected. Call
`reporter.report(LocalToolResult::new(value).with_run_llm(false)).await?` while
the handler is running, then return a `LocalToolResult` for its final result.
Report success acknowledges bounded first-hop admission, before the assistant
has applied the update to shared context. Each update carries an immutable local
receipt and a monotonic sequence; the native assistant appends an async developer
message while retaining the pending call. Duplicate or retired progress cannot
append again. The result policy uses the existing inference gates.

Intermediate reports neither settle the call, release its reservation, nor
extend its original deadline. Final return, error, timeout, explicit cancellation
and task drop revoke the reporting endpoint before the single terminal receipt.
An escaped reporter then rejects updates and cannot retain the owning runtime.
Legacy handler APIs remain usable; invoking a reporting handler without an
admitted endpoint fails before running user code. Native progress must be
consumed explicitly before wire serialization. Reporting after handler return
and realtime progress mapping remain separate work.

An intermediate update can carry a context callback through
`reporter.report_with_context_updated(LocalToolCompletion::new(result)
.with_context_updated(callback)).await?`. This remains a nonterminal report:
the assistant first applies the update and commits its sequence, then schedules
the hook through the same task owner used for final callbacks. Cloned or retired
updates cannot run the hook again. Cancelling the tool revokes further reporting;
it does not cancel a hook already scheduled by the assistant. End, Cancel and
cleanup cancel those tasks. The callback is kept only in a shared frame slot
sealed to that individual progress receipt, never in the progress capability or
terminal journal. An unconsumed frame retains its captures; wire encoding rejects
the slot even after its progress receipt has been taken. Failed context admission
or inference routing does not acknowledge callback execution. Python parity here
uses nonempty results: Python skips inserting falsy intermediate values but still
schedules their hook, whereas the native consumer applies its existing value rules.

Successful final results can use `LocalToolFunction::new_with_completion` and
return `LocalToolCompletion::new(result).with_context_updated(callback)`, where
`result` is a `LocalToolResult` and `callback` is a `LocalAsyncCallback`.
The native assistant schedules the callback after applying the result and the
existing inference actions; the callback can read the committed shared context
and runs in the existing task owner without blocking pipeline traffic.
It survives ordinary interruption, while End, Cancel and cleanup close that
owner and cancel its tasks. Callback errors do not change the settled tool receipt.

The hook is bound to the final receipt in a shared one-shot frame slot. Replay
cannot invoke it again, while separate publications can reuse the same callback.
The terminal journal retains no callback; an unconsumed frame still owns its
captures until the shared slot is taken, discarded or dropped. Error, oversized,
stale and cancelled results discard the hook. Failed context admission leaves it
unconsumed; inference-route failure or shutdown does not guarantee execution.
Standalone completion invocation returns the metadata without running the hook;
legacy result/value invocation deliberately discards it. Wire serialization
rejects an unconsumed slot even after terminal provenance has been taken.

`DirectToolBindingConfig` defaults to 16 in-flight calls, a 1,024-ID duplicate
window, 64 KiB argument/identity limits, 128 advertised tools and 1,024 retained
cleanup hooks. Handler deadlines override `default_timeout`; both absent means no
execution deadline. Optional deadlines must be positive. Hooks attached with
`with_cleanup` are deduplicated by callback identity and retained even after
pruning or rebinding. Pipeline cleanup permanently closes the owner, stops future
dispatch and awaits recorded hooks within its shutdown budget, continuing after
failures and returning the first error. Core's Stop contract still retains
resources until explicit owner-stage cleanup.

Each call settles with one final result or cancellation. `cancel_on_interruption`
controls the native owned-task policy. LLM-generated `cancel_<name>` tools remain
unsupported. Cleanup runs within the owning pipeline's shutdown budget:
hook errors and panics continue to later hooks while retaining the first error.
If that outer deadline cancels cleanup, the owner requests abort of the currently
awaited hook and drops hooks not yet started. That abort request does not await
the active hook's completion or establish that its resources have closed.

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
