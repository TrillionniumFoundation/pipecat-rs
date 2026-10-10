# pipecat-network-bus

Native Rust transports for the Python Pipecat worker bus protocol. The default
build includes a strict JSON codec and Redis RESP2 pub/sub. Feature `pgmq` adds
PostgreSQL PGMQ direct and isolated backends using `tokio-postgres`.

## Redis

```rust,no_run
use pipecat_network_bus::{RedisBus, RedisConfig};
use pipecat_workers::{BusMessage, MessageData};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let bus = RedisBus::connect(RedisConfig::default()).await?;
let subscriber = bus.subscribe_target("voice")?;
bus.send(&BusMessage {
    source: "coordinator".into(),
    target: Some("voice".into()),
    data: MessageData::Speak {
        text: "Hello".into(),
        append_to_context: true,
    },
}).await?;
let envelope = subscriber.recv().await?.unwrap();
assert_eq!(envelope.target(), Some("voice"));
bus.shutdown().await;
# Ok(())
# }
```

- Connect waits for Redis's SUBSCRIBE acknowledgement
- A dedicated reader reconnects with capped exponential backoff
- Each subscriber has separate bounded system/data FIFO queues. System messages
  take priority. An overfull queue drops the incoming message and reports
  `ReceiveError::Lagged` on its next receive; admitted messages remain intact
- Redis publication counts are Redis channel subscribers, not local worker counts
- Pub/sub has no replay. Disconnections can lose messages; `events()` and
  `connection_state()` expose this boundary
- PUBLISH calls use a separate persistent, serialized connection. A failed,
  timed-out, or cancelled command discards that connection. Ambiguous publication
  is never automatically retried, since retrying could duplicate a job
- Shutdown wakes idle subscribers, closes admission, cancels reader/reconnect and
  pending send operations, and permits admitted messages to drain
- This implementation uses plain TCP. AUTH is refused unless the connected peer
  is loopback. Use a local TLS tunnel for authenticated remote Redis. Native TLS,
  Redis Cluster, Sentinel, and Unix socket URLs are not implemented

## Python wire compatibility

`PythonJsonCodec` reads/writes Python's fully qualified `__type__` / `__data__`
JSON. No type is dynamically imported, evaluated, or loaded. Unknown tagged
objects, unknown frame types, invalid enums, malformed base64, over-size input,
and unsupported native frames return explicit errors.
Unsupported-frame diagnostics contain only a static frame class name, never
conversation text, audio bytes, or tool arguments. Typed response boundaries
preserve `skip_tts`, and End/Cancel support JSON-valued reasons as in Python.

The codec supports all bus message classes in `src/pipecat/bus/messages.py`,
except `BusAddWorkerMessage`, which contains an in-memory worker reference.
`BusWorkerLocalErrorMessage` can be sent locally but is rejected if received over
the network. Use `pipecat-workers::LocalBus` for worker-reference operations.
Registry snapshots, flush progress, and base messages have explicit
`DecodedMessage` variants. Other messages project to `pipecat_workers::BusMessage`.

Important source details:

- The request field is `job_name`, not the native enum's `name`
- `JobStatus` is a Python StrEnum, so the original serializer emits lowercase
  strings. The exact tagged form `pipecat.pipeline.job_context.JobStatus` with
  uppercase member names is also accepted. Other qualified names are rejected
- Frame directions use `pipecat.processors.frame_processor.FrameDirection`
- Registry entries use `pipecat.registry.types.WorkerRegistryEntry`
- `BusJobUpdateRequestMessage` has data priority, matching the Python class
- Missing optional dataclass fields are preserved, rather than rewritten as null

`MessageEnvelope` is immutable and retains the validated original wire object.
Forward it with `send_envelope` to preserve source IDs/names, ready timestamps,
stream start/end metadata, frame fields, and unknown ordinary extensions.
`as_bus_message()` is a native projection; rebuilding an envelope from that
projection can lose source-only fields. Rich context custom tools are also retained by the typed
projection. `projection_warnings()` identifies common
such fields; it is advisory, not an exhaustive schema-difference checker.
`from_bus_message()` creates a new message identity.

The core frame allowlist has source-generated fixtures for 125 of the 134 source
frame classes, including detailed transcriptions, image/file/audio payloads,
context/messages/tools, sparse service settings, VAD/turn controls, function
calls, worker signals, and safe lifecycle/control frames. Rich native variants
preserve speculation, custom tools, thought signatures, tracing enablement,
callback-free result properties, image synchronization, and source routing flags.

This is conditional class coverage, not unrestricted payload compatibility.
Executable callbacks, processor references, strategy/analyzer objects, and
unadapted tracing contexts are explicitly rejected. Nine classes remain rejected,
including metrics (the native payload lacks source counters) and sprites (nested
frame identities are not represented). Provider-specific and null-content
context messages are rejected rather than flattened. Python's serializer drops
tuple-valued image dimensions; missing dimensions are rejected and native
encoders emit lists accepted by Python. See [the complete per-class coverage
matrix and field-level limits](FRAME_COVERAGE.md).

The typed JSON `Value` fields for job payloads and transport data retain validated
wire tags (such as bytes); they do not automatically convert to arbitrary Rust
objects. JSON syntax/field ordering/whitespace is not preserved, but the JSON
value, identities, and omitted fields are. Integer literals outside the signed
or unsigned 64-bit range return `UnsupportedInteger` before JSON parsing, rather
than silently rounding through floating point. Python non-standard NaN and
Infinity JSON literals are explicitly rejected. Call `decode` on original wire
bytes to enforce these checks; `decode_value` cannot recover precision already
lost by an upstream JSON parser. Ordinary serde_json private-marker keys remain
dictionary keys, and this crate enables no serde_json parsing features globally.


## PostgreSQL PGMQ

Enable the optional feature:

```toml
pipecat-network-bus = { path = "../pipecat-network-bus", features = ["pgmq"] }
```

```rust,no_run
# #[cfg(feature = "pgmq")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use pipecat_network_bus::{PgmqBus, PgmqConfig, PgmqMode};
let bus = PgmqBus::connect(PgmqConfig {
    mode: PgmqMode::Direct,
    channel: "session".into(),
    ..PgmqConfig::default()
}).await?;
let subscriber = bus.subscribe()?;
// Send/receive MessageEnvelope values or native worker messages.
bus.shutdown().await?;
# Ok(())
# }
```

Direct mode uses `pgmq.create`, `list_queues`, `send`, `read_with_poll`, `delete`,
and `drop_queue`. Queue names use the source's sanitization and a random UUID
suffix. Peer discovery is cached for one second. Fanout includes the sender's
queue, matching the executable Python implementation. Individual peer failures
are reported in `PublishReport.failed_peers` and invalidate the cache while
allowing the remaining sends to proceed. Sanitized channel prefixes can collide;
this mode is intended for mutually trusted peers.

Isolated mode only calls the source deployment's `public.bus_join`, `bus_publish`,
`bus_subscribe`, `bus_archive`, and `bus_leave` wrappers. The channel capability is
passed as a parameter, never interpolated into SQL. These wrappers and their
security policies must already be installed by the deployer. This crate does not
create database extensions, migrations, roles, or grants.

Two persistent connections separate long-polls from publication/acknowledgement.
All SQL values use typed bind parameters and no per-message transaction is
created. Query timeout/cancellation discards the occupied connection, attempts a
bounded PostgreSQL CancelRequest, and never returns it to the connection slot.
Cancellation cannot guarantee the server did not finish the query; ambiguous
publishes are not retried automatically. The next operation reconnects as needed.
This driver uses NoTls and accepts only loopback addresses; use a local secure
tunnel to reach a remote database.

Valid leased messages are acknowledged only after atomic admission into all
eligible bounded local queues. This means one slow subscriber can pause reading
and delay later system messages in that batch. Poison messages are reported and
acknowledged independently, matching the source. Raw JSONB is size-checked before
JSON parsing; an invalid payload or SQL NULL does not discard its valid batch
siblings. Invalid database result schemas still fail the read.
The PostgreSQL driver buffers protocol rows before these application-level
checks; the message limit is not a bound on driver/network buffering.
Acknowledgement is not application completion:
failed acknowledgements, crashes, or expired leases can cause duplicates. There
is no exactly-once guarantee or background lease extension. Choose visibility
and batch settings for your processing/backpressure budget.

Always call `shutdown()` to cancel the long-poll and remove the instance's private
queue. A failed leave can be retried by calling shutdown again. Dropping the bus
cancels I/O but does not guarantee queue removal; interrupted startup or abrupt
process termination can leave orphan queues requiring deployment cleanup.

## Verification

```sh
cargo test -p pipecat-network-bus --all-features
cargo clippy -p pipecat-network-bus --all-features --all-targets -- -D warnings
cargo fmt -p pipecat-network-bus --check
python3 crates/pipecat-network-bus/tests/generate_fixtures.py
python3 crates/pipecat-network-bus/tests/generate_frame_fixtures.py
```

The committed fixtures include 35 bus messages and 125 core frame classes,
generated from the original Python class and serializer AST definitions with
optional provider imports omitted. Context uses a small data holder and the
original context/tools adapters; VAD/base-turn parameters use actual Pydantic 2
models. The frame generator also runs the original Python decoder. Rust verifies
populated typed fields, class coverage, exact immutable forwarding, repeated
replay, malformed substitutions, field permutations, size boundaries, local-state
rejections, nonfinite values, sparse settings, and image-dimension limits.

Redis tests use actual loopback TCP and an independent RESP server. PostgreSQL
tests exercise the actual `tokio-postgres` driver against an independent scripted
PostgreSQL protocol server, including typed SQL parameters, leases/acks,
connection invalidation, and cancellation requests. These are protocol/mock
integration tests, not verification against an installed Redis server, PostgreSQL
server, PGMQ extension, isolated SQL wrapper deployment, TLS endpoint, or external
credentials. The optional connected non-loopback Redis AUTH test is ignored by
default and requires a permitted local route; it is not remote authentication
acceptance coverage.

Protocol references:
- [PGMQ SQL functions](https://pgmq.github.io/pgmq/api/sql/functions/)
- [tokio-postgres cancellation contract](https://docs.rs/tokio-postgres/0.7.18/tokio_postgres/struct.CancelToken.html)

## Native worker integration

`WorkerNetworkAdapter` connects a native `WorkerRunner` bus and registry to Redis
or PGMQ with bounded independent ingress/egress lanes, registry synchronization,
self-echo suppression, and one-shot publication. Transport loss or ambiguity closes
the attachment without replaying jobs. See [worker runtime usage and failure
semantics](WORKER_RUNTIME.md), including finite-deadline guidance and graceful shutdown.
