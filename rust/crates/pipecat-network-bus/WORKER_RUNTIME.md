# Native distributed worker runtime

`WorkerNetworkAdapter` attaches a `pipecat-workers::LocalBus` and `WorkerRegistry`
to an `Arc<RedisBus>` or, with feature `pgmq`, an `Arc<PgmqBus>`. Workers, job
clients, and pipeline inputs keep the same bounded local admission API. The adapter
runs actual network forwarding and registry discovery, not a separate simulated
worker implementation.

```rust,no_run
use std::{sync::Arc, time::Duration};
use pipecat_network_bus::{RedisBus, RedisConfig, WorkerNetworkAdapter, WorkerNetworkOptions};
use pipecat_workers::{JobParams, WorkerRunner};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let runner = WorkerRunner::new("unique-runner-name", 128)?;
let network = Arc::new(RedisBus::connect(RedisConfig::default()).await?);
let mut attachment = WorkerNetworkAdapter::attach(
    runner.bus(), runner.registry(), network.clone(), WorkerNetworkOptions::default(),
)?;
attachment.ready().await?;
let mut job = runner.client("application").request("remote-worker", JobParams {
    timeout: Some(Duration::from_secs(30)),
    ..Default::default()
})?;
let response = job.result().await?;
runner.end().await?;
attachment.shutdown().await?;
network.shutdown().await;
# Ok(())
# }
```

## Routing and ownership

- Each attachment owns one bounded local outbound subscription and one bounded
  transport subscription, with independent data/control receive tasks
- Local addressed consumers take precedence. Inbound injection never enters
  outbound subscriptions. A per-attachment origin field at the outer JSON envelope
  suppresses a transport's self echo without an unbounded seen-message cache
- Runner names must be unique across distributed participants. Job IDs include
  the runner namespace. Worker names remain the source protocol's addressing unit
- Registry snapshots are authoritative for one remote runner, including removals.
  Local changes announce snapshots, first contact receives one reply, and periodic
  snapshots support late joins. Remote registry updates do not re-announce themselves
- Remote runner/worker limits bound discovery memory. Optional `peer_timeout` expires
  peers that advertise periodically; it is disabled by default because source Python
  hosts do not guarantee periodic announcements
- `enable_native_control` promotes the native runtime's JobUpdateRequest lane only
  for this subscription. Raw subscriptions and `MessageEnvelope::is_system` preserve
  Python's data-priority classification. Both native lanes retain finite capacity
- Stop workers and await required job results before graceful adapter shutdown.
  Shutdown closes admission, drains already admitted outbound messages once, publishes
  an empty registry, and joins forwarding tasks. The transport remains caller-owned

## Failure semantics

A send is attempted once. A lost acknowledgement can mean a request already reached
its worker. Redis publish counts are subscriber counts; PGMQ acknowledgements prove
queue admission, not job execution. There is no exactly-once execution or automatic
job replay. Reconnection/retry decisions belong to application code with knowledge of
whether a particular operation is safe to repeat.

The attachment fails closed on transport loss, receive lag, unsupported runtime
messages, codec rejection, or PGMQ partial fanout. It closes local admission, joins
its tasks, removes its remote registrations, and exposes the failure through `watch`
and `wait`. Redis's standalone reader may reconnect, but the worker attachment does
not restart or replay requests. A caller may have obtained a valid response before
an ambiguous send failure; that response is not retroactively erased.

A registry-removal snapshot can overtake an ordinary terminal response. Remote jobs
therefore await the response, cancellation, configured deadline, or attachment failure
rather than treating absence as proof of execution failure. Use finite job timeouts;
a peer disappearing while the local Redis connection stays healthy may otherwise
leave a request waiting indefinitely. Native cancellation markers and in-flight job
suppression are bounded, not a durable deduplication ledger.

The typed native worker projection does not expose Python ready timestamps or stream
start/end metadata; immutable envelope forwarding retains those fields, but worker
consumption does not. Flush progress has a wire representation but no native worker
flush tracker and is rejected by this attachment. Specialized LLM/UI/websocket proxy
workers are separately unsupported, as listed in `pipecat-workers/README.md`.

## Validation

`tests/worker_runtime.rs` uses independent RESP2 parsing over real loopback TCP. It
runs native remote workers and shared-ID groups, streaming updates, partial failures,
late-readiness cancellation, grouped handler cancellation, saturated data admission,
registry removal, clean shutdown, and delivered-publication/late-ack ambiguity.
PGMQ adapter compilation and the existing PostgreSQL protocol tests run with
`--all-features`; these are not live Redis/PGMQ deployment qualifications.
