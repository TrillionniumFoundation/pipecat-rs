# Opt-in root traversal heartbeats

```rust
use pipecat_core::{HeartbeatConfig, Pipeline, PipelineConfig};
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut worker = Pipeline::new(vec![])
    .with_heartbeats(HeartbeatConfig::default())
    .start(PipelineConfig::default()).await?;
let mut monitor = worker.heartbeat_monitor().unwrap();
let snapshot = monitor.changed().await?;
println!("received={}, timeouts={}", snapshot.received, snapshot.timeouts);
worker.end().await?;
while worker.recv().await.is_some() {}
worker.join().await?;
# Ok(()) }
```

Run the bounded runnable example with `cargo run -p pipecat-core --example
heartbeats`. No provider credentials, network, Python runtime or extra dependency
is needed.

## What it measures

The default pipeline has no monitor and ordinary application Heartbeat frames
keep their existing routing, queue and output semantics. Explicit configuration
creates one root monitor shared with lifecycle-owned children. Configuring a
child's own monitor is rejected. The handle owns only a latest-value watch
receiver, not the worker or a background task.

After Start reaches the root downstream sink, the existing supervisor drives an
immediate first probe and fixed-delay subsequent attempts. Normal-lane probes
traverse processors. Receipt is recorded at the root sink before application
output admission; owned probes do not consume application output slots. Reading
`worker.recv()` is not required for these receipts. A child boundary cannot
acknowledge. A receipt proves that one actual root traversal completed; it does
not prove that every task, provider request, parallel branch or business operation
is healthy. In Parallel and SyncParallel, one returning branch can mask a stalled
sibling. SyncParallel probes do not create sentinels or consume business batch
buffers; ordinary user Heartbeat frames still use its normal synchronization.

Only a live pending native observation identity with its original timestamp can
acknowledge once. Clones share that identity; repeats, `duplicate()`, public ID
reuse, serialization/reconstruction, expired probes and probes from a previous
run do not extend the deadline. A private per-run local tag scopes diagnostic
consumption and pressure handling. `ProcessorContext::is_heartbeat_probe` is a
read-only provenance query, not acknowledgement authority. This is not a security
boundary against custom processors that can deliberately forward retained clones.
A modified probe timestamp cannot acknowledge. A modified non-Heartbeat payload
is ordinary traffic and gets ordinary error/pressure semantics.

## Bounds and lifecycle

Defaults match the pinned Python source: disabled, period 1 second, timeout 10
seconds. Native interval validation permits 10ms through 24h; pending weak-token
capacity is 1..=128 (default 32). Weak tokens retain no frame payload. Slots retire
when all clones die or the timeout expires. Retiring slots, failed admission and
capacity skips do not reset the no-receipt deadline. A branch dropping a clone
does not invalidate other in-flight clones of that probe.

The latest-value watch coalesces updates and has no unbounded event queue. Counters
saturate at u64::MAX. Timeout increments repeat at the configured interval without
emitting an Error frame, changing processor usability or cancelling the worker.
No callback or automatic recovery is installed. Monitor snapshots expose sent,
received, timeout, admission-failure, capacity-skip, retired-unreceived and ignored
receipt totals, current pending count, and the last accepted round-trip duration.

Generated probes encountering a full core queue, a busy nonblocking child input
lock or full child output are softly dropped and counted. Ordinary application
frames retain fail-fast pressure behavior. This guarantee covers native core and
Parallel/SyncParallel bookkeeping, not arbitrary custom processors or external
services that interpret Heartbeat as business input. A processor may swallow,
reroute or transform probes, resulting in timeouts. Pausing processing can also
report a stall. A blocked executor cannot provide timely monitoring; the existing
supervisor's 10ms tick and scheduling delay bound neither realtime precision nor
external failure detection. Late ticks report one timeout rather than an
unbounded catch-up burst.

End admission closes new probe injection. Monitoring continues during drain until
the root terminates; pending old probes may still finish before expiry. Terminal
cleanup closes the monitor, clears pending slots and freezes counters. No extra
producer/monitor tasks survive cleanup. Start blocked before the real sink has
no active heartbeat timer. Heartbeats are diagnostics, not cancellation or idle
shutdown policy.

## Source comparison and validation

Reference: ProfHepta/pipecat at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/pipeline/worker.py`, deprecated `pipeline/task.py` shim, and
`examples/observability/observability-heartbeats.py`.

The checked-in `tests/fixtures/python_heartbeat_source.json` is the exact bounded
original-AST replay exported from those hash-pinned complete files. The evidence
bundle holds the runner, originals, provenance hashes and corruption-rejection
checks. Dependencies are deterministic stubs; this does not run the Python SDK
scheduler, network or full example. Native runtime tests use real Tokio workers
and queues separately.

Intentional native differences: weak identity/timestamp-bound receipts instead
of accepting any Heartbeat; finite retention and coalesced watch counters instead
of an unbounded receipt queue plus callbacks; supervisor integration instead of
two extra tasks; End stops new probes whereas Python continues direct pipeline
injection while draining; native composition pressure exemptions keep diagnostic
traffic out of Sync sentinels and business buffers. The source's idle observer
ignores heartbeat under its default activity tuple; this feature does not add or
claim a general native Python idle-worker policy.
