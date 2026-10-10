# pipecat-worker-proxy

Native WebSocket client/server attachments for `pipecat-workers`, using bounded
`pipecat-transports` sessions and the allowlisted Python `__type__` JSON codec in
`pipecat-network-bus`. No Python process is required.

## Connection and ownership

Construct `WebSocketProxyClient::new(url, bus, registry, options)` to create an
inactive client. `activate().await` opens one connection; `stop().await` tears it
down. Calling activate while connected returns `AlreadyActive`. After terminal
failure, activate explicitly creates a fresh attachment with no old send queue.

On a server, call `WebSocketProxyServer::accept_with_request_policy` with an
accepted TCP stream and a policy validating the complete HTTP upgrade request.
Use `attach` only with an already authenticated and origin-checked
`WebSocketSession`. The embedding application owns its listener, worker runner,
TLS termination, credentials and connection limits. One proxy attachment owns one
socket; it never closes the caller's shared worker bus.

`ProxyOptions::new("gateway", "local-worker", "remote-worker")` identifies the
allowed endpoints. Give concurrent attachments distinct proxy names. Subscribe
to `attachment.watch()` for retained connection/terminal state, and `events()`
for bounded diagnostics. Wait or shut down the attachment to observe errors and
cleanup completion. Dropping it requests the same asynchronous cleanup.

## Routing and lifecycle

- Server discovery announces only its configured local worker, including activity,
  parent and bridge metadata. Local revisions update discovery; worker removal
  announces an empty snapshot. Client discovery may pin `remote_runner` and rejects
  runner changes, local-runner impersonation and unexpected worker names
- Single-entry registry updates use opaque, registry-bound ownership generations.
  Sibling workers survive updates, empty discovery keeps the name exclusively
  claimed, and external mutations invalidate the old generation. Stale cleanup
  cannot remove even an identically re-registered entry
- Targeted native bus messages cross only from the configured source worker to
  its configured peer. Native job/group synthetic caller addresses are accepted
  in that worker's namespace; reply messages must match an outstanding caller/job
  pair. Source-only broadcast forwarding is opt-in for Frame and Speak messages
- Native JobClient/NativeWorker calls, responses, streamed updates, stream markers,
  update requests, cancellation and worker control messages are executable over
  real sockets. Frame direction and bridge metadata survive the source codec
- Inbound publications use LocalBus's import lane, which excludes all network
  outbound subscriptions. Imported messages cannot echo into this or another
  attached network adapter
- Normal and system messages have independent bounded inbound and outbound lanes.
  A slow normal local subscriber does not block system cancellation. Saturation,
  admission deadlines, malformed messages and unsupported/local-only wire types
  fail the connection explicitly; messages are never silently evicted
- On disconnect, tracked outbound jobs receive urgent Error responses that warn
  execution may already have happened; tracked inbound jobs receive cancellation.
  Queued outbound requests are collected in a finite cleanup snapshot. Neither
  transport admission nor a disconnect error proves remote execution status
- No automatic reconnect, request replay, durable deduplication or exactly-once
  guarantee exists. Repeated live requests are rejected, but a completed job ID
  is not retained forever. Only the application can decide whether retrying an
  operation is safe

## Authentication and wire limits

`with_headers` supports authentication and Origin headers without overriding
Host, Connection, Upgrade or Sec-WebSocket handshake fields. Custom headers
require WSS outside literal loopback/localhost. URL userinfo and fragments are
rejected. The server policy must validate authentication, the endpoint path,
Origin (including duplicate/missing values as appropriate), and authorization to
act as the configured worker. Origin is not client authentication. There is no
built-in bearer token issuer or public unauthenticated listener.

Codec, WebSocket message/frame, socket queue, local bus, inbound priority lane,
pending-job and diagnostic-event bounds are finite and configurable. Runtime
job tracking is bounded by `max_pending_jobs` per direction; teardown additionally
holds at most both local priority queues and the single rejected request.
Handshake, writes, admission, socket close and aggregate job cleanup have explicit
deadlines. A slow diagnostic receiver receives broadcast `Lagged`, and native
job-update readers receive `JobError::EventLagged`; terminal results are separate.

## Compatibility boundaries

This covers the source WebSocket proxy's routing and lifecycle with stricter
source/registry validation and finite queues. It is not a Python class API:
activation is explicit rather than a BaseWorker callback, and attach/detach is
owned by the application rather than automatic WorkerRunner registration.
Automatic discovery is server-to-client, matching the source; initiating jobs
in the reverse direction requires application-provided readiness registration.

All typed native bus variants use the shared source codec's supported frame and
message allowlist. Python base/flush messages without a native projection are
reported as `ProxyEvent::Unprojected` for an application handler. Source-only
metadata that has no native field cannot be retained through LocalBus; known
projection differences produce a diagnostic. Arbitrary custom Python classes,
custom serializers, other source-only broadcast type selections, remote object
references, reconnect leases and shared external registry ownership are not
implemented. Do not use the diagnostic ring as a reliable application data bus.

Always set a finite `JobParams::timeout` for distributed calls. If a local bus
consumer blocks cleanup past `cleanup_timeout`, terminal state exposes
`cleanup_failed`; local cancellation and outstanding reply delivery are then
best-effort. A call racing readiness removal and egress unsubscription may rely
on its end-to-end deadline. Cleanup does not destroy an unrelated shared bus to
force job completion. Applications should stop new calls before intentionally
stopping a proxy and await in-flight jobs for graceful completion.

## Validation

```sh
cargo test -p pipecat-worker-proxy --offline
cargo clippy -p pipecat-worker-proxy --all-targets --offline --no-deps -- -D warnings
cargo fmt -p pipecat-worker-proxy --check
```

Tests use actual loopback sockets, native workers, an independently written
Python-format request fixture, auth/origin denial, streaming/update/cancel RPC,
dropout/reconnect without replay, duplicate jobs, overload, diagnostic and job
lag, cleanup, concurrent registry updates and preservation of unrelated workers.
They do not qualify external deployments, live Python peers or production TLS.
