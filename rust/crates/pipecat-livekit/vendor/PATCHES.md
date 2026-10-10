# Signaling cancellation ownership

`lifecycle.patch` changes only the following:

- The `livekit` manifest uses the sibling `livekit-signaling` source
- `SignalStream` owns abort handles for its reader and writer, including while
  explicit close awaits them
- `SignalClient::drop` aborts its dispatcher, and an abort guard retains ownership
  while explicit close awaits that dispatcher
- Two signaling unit regressions cover dropping a half-open stream and aborting
  an explicit close while the peer's close operation is stalled

Bare Tokio `JoinHandle` values detach on drop. The guards ensure that a dropped
connect/reconnect future or a connect deadline cannot leave a read task, write
task, or dispatcher retaining the transport after its owning object is gone.
Normal explicit close still awaits its owned tasks. Drop requests cancellation;
release occurs when the Tokio runtime next polls those tasks. Runtime shutdown
is not an asynchronous cleanup guarantee, and foreign `WsConnection`
implementations remain responsible for their own external-resource lifecycle.

The adapter's `tests/review_contracts.rs` verifies transport release for cancel,
caller abort, and adapter timeout both before JoinResponse and after a synthetic
JoinResponse while RTC negotiation remains pending. It also verifies the SDK's
own JoinResponse timeout. This uses the SDK's official injected transport API
with an in-memory half-open connection. It does not establish a network
WebSocket handshake, an authorized server join, SFU delivery, or reconnection.

The rest of the SDK, protocol, WebRTC bindings, network transport and native
WebRTC binary are unchanged. The SDK's unbounded room-event queue is unchanged.
