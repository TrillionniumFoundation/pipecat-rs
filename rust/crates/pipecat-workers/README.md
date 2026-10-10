# pipecat-workers

Executable native worker trees, bounded job RPC, shared-ID groups, and pipeline bridges.
Attach `pipecat-network-bus::WorkerNetworkAdapter` for distributed Redis/PGMQ workers.

## Example

```rust
use pipecat_workers::{JobParams, JobResponse, NativeWorker, WorkerRunner};
use serde_json::json;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let runner = WorkerRunner::new("local", 64)?;
runner.add(
    NativeWorker::new("echo")
        .active(true)
        .on_job("echo", true, |context| async move {
            JobResponse::completed(context.payload.unwrap_or_default())
        })?,
)?;
let mut job = runner.client("app").request("echo", JobParams {
    name: Some("echo".into()),
    payload: Some(json!({"hello": "world"})),
    timeout: Some(std::time::Duration::from_secs(5)),
    ..Default::default()
})?;
assert_eq!(job.result().await?.response, Some(json!({"hello": "world"})));
runner.end().await?;
# Ok(())
# }
```

## Runtime guarantees

- LocalBus provides FIFO per priority, independent bounded data/system lanes,
  atomic broadcast admission, cancellation-safe async publication, and drain-on-close
- A full data lane cannot consume a system lane's capacity. A full system lane
  can still backpressure system messages; capacity is never silently expanded
- `subscribe` observes every publication. `subscribe_target` only receives its
  addressed messages and broadcasts. Any eligible slow subscriber can backpressure
  an atomic publication; use targeted subscriptions for worker RPC
- NativeWorker has bounded in-flight jobs, inactive/active gating, named handlers,
  panic-to-error responses, concurrent jobs, and FIFO sequential handlers per name
- `NativeWorker::child` builds an owned worker tree. Parents propagate end/cancel
  and await descendant cleanup before becoming terminal; runner termination signals
  roots only. `JobContext::client()` supports nested RPC with handle-owned cancellation
- `WorkerRunner::add(&self)` permits dynamic roots while `run(false)` hosts an empty
  or changing session. `run(true)` atomically closes admission when all roots finish.
  Add racing with shutdown is either fully owned or rejected as `RunnerStopped`.
  `get` returns a cloned handle (including descendants), usable after shutdown
- Graceful End drains requests before its FIFO marker and awaits their terminal
  publications. Cancel aborts handler futures and returns urgent cancelled replies
- System-priority job cancellation can overtake a request. The request is removed
  from the pending data queue, so its handler is never started
- JobUpdateRequest uses the control lane, so an in-flight job can receive updates
  even when all execution slots are occupied. Bus closure also wakes readiness waits
- JobClient starts a background deadline covering readiness, publication, and
  execution. A timeout reports any failure to admit its remote cancellation
- Speech from a job's expected peer and unique private reply inbox is relayed
  to the logical caller before a following normal terminal response. The relay
  uses bounded admission and stops on cancellation, timeout, or handle drop.
  This applies to single jobs and group members without parsing inbox names.
  Speak has no job-ID field; its correlation is the owned inbox and expected
  peer. The in-process bus trusts publishers; source strings are not authentication
- `JobHandle::cancel` first prevents further local request admission, then awaits
  remote cancellation admission. A locally cancelled `result` reports caller
  cancellation, not proof that a remote handler has already stopped. Other terminal
  results originate from the worker. Dropping a handle attempts cancellation without blocking;
  a saturated system lane can prevent that best-effort notification
- `request_group` gives every member one shared job ID with distinct bounded reply
  subscriptions. Group events identify their worker; results retain partial successes,
  failed/cancelled statuses, and transport errors. Fail-fast cancellation is optional.
  Group drop/cancel signals all members, including those still waiting for readiness
- Progress uses a bounded ring. A slow reader gets an explicit `EventLagged` error;
  terminal results are independently retained. StreamEnd is an event; a handler's
  returned JobResponse is the terminal result
- WorkerRegistry offers atomic initial lookup plus change watches, duplicate
  suppression per runner, local lookup precedence, and removal notifications.
  Authoritative remote snapshots replace one runner only and never trigger outgoing
  local revisions; registration metadata records parent, activity, and bridging
- `BusBridgeProcessor` consumes outgoing frames and injects filtered remote frames
  after the bridge, preserving direction. `BridgeMode::Tee` is an output-only edge
  tap that also forwards locally. Lifecycle and urgent transport frames stay local.
  Imported frames carry provenance so later taps cannot echo them; new response
  frames can flow back normally. A full bus yields an explicit processor error
  instead of blocking pipeline control on cyclic backpressure
- PipelineBusInput attaches to a core PipelineSender, handling activation, TTS
  speak, bridge-filtered typed frame inputs, and end/cancel. Independent input
  lanes keep cancellation live during a blocked normal frame enqueue. Consume
  outputs and inspect `join` on the original core PipelineWorker

## Scope and compatibility

This is a bounded native implementation, not a drop-in Python API or wire port.
The Python local bus is unbounded. Native enums use serde's Rust schema, not
Python's fully-qualified `__type__` JSON envelope. The shared JobStatus string
values alone do not establish wire compatibility.

Implemented and tested: local bus concurrency, lifecycle, nested native worker
RPC, shared-ID groups, readiness/registry synchronization primitives, worker trees,
dynamic root admission, progress/stream events, and real bidirectional core pipelines.
Network worker integration and independent loopback RESP tests live in
`pipecat-network-bus`; no live Redis/PostgreSQL service qualification is implied.

Not implemented here: specialized LLM tool registration/context workers, UIWorker
snapshot injection and command execution, websocket proxy workers, Python decorators,
OS signal integration, runtime child reparenting/addition, pipeline observer/heartbeat/
flush-probe parity. These require executable behavior beyond typed UI/LLM frames.
The native runner owns `NativeWorker` trees; core pipeline workers retain their own
output/join ownership and attach through `PipelineBusInput`/`BusBridgeProcessor`.
Custom handler-spawned untracked tasks and blocking work are outside cancellation
guarantees. Dynamic hosts retain completed root handles until same-name replacement
or runner drop; applications should reuse stable worker names for bounded histories.

Remote registry removal can overtake an admitted normal response. Local disappearance
still resolves immediately as `WorkerStopped`; remote jobs keep waiting for a terminal
response, cancellation, end-to-end deadline, or adapter failure. Configure a finite
`JobParams::timeout` for distributed work. Recent cancellation-before-request markers
are bounded to `max_jobs + 2 * bus_capacity` and are not a persistent deduplication
ledger; arbitrary stale redelivery outside that window is not exactly-once execution.

An inactive NativeWorker drops normal work, matching Python's active gate. Requesters
should activate it first and configure a timeout. Bus closure is an explicit worker
failure when it prevents terminal responses, rather than a successful graceful end.
Graceful/cancel cleanup can wait on saturated reply recipients: cancellation aborts
execution immediately, while waiting for acknowledged reply admission preserves
observable delivery guarantees. Dropping WorkerRunner requests cancellation.

## Checks

```sh
cargo test -p pipecat-workers
cargo clippy -p pipecat-workers --all-targets --no-deps -- -D warnings
cargo fmt -p pipecat-workers --check
```
