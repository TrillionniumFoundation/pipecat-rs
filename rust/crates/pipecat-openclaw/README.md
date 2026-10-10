# OpenClaw Gateway

Optional native Rust Gateway protocol 4 client and typed pipeline processor,
matching the pinned Pipecat OpenClaw v2026.6.1 contract. Enable the facade's
`openclaw` feature or depend on this crate directly.

`GatewayClient::connect(config)` owns one authenticated persistent WebSocket.
`start(message, session)` returns a single-consumer `GatewayRun`; retain its
`RunHandle` for `steer` and `abort`. The handle keeps the session and current
remote run identity after the stream ends. `RunEvent::Text` contains only actual
`deltaText`, never cumulative `message`. Its ID is the matched wire alias. `RunEvent::End` preserves terminal text;
only the convenience `collect` folds deltas when completed terminal text is empty.

The client waits for `connect.challenge`, then negotiates protocol 4 with
`gateway-client`/`backend`, operator/write defaults and explicit token/password.
A valid matching `hello-ok` with protocol 4 is required. No external service is
contacted by tests. No remote tool, command, or returned prose is executed locally.

## Safety, bounds and failure semantics

- WSS is mandatory except explicitly enabled literal-loopback WS. URL userinfo,
  query and fragment are rejected. The client does not follow HTTP redirects.
  Pairing, device signatures and token-only remote deployment are not implemented
- Every wire read/write, request, run and shutdown has a finite bound. Pending
  requests, runs, aliases, unrouted frame count/bytes, queued event count/bytes,
  accumulated delta text and individual messages are bounded. Excess input fails
  observation; no unbounded spool or automatic retry is used
- Request correlation IDs are distinct from idempotency/run IDs. Server-selected
  aliases are adopted without cross-run collisions. Explicit event session keys
  must match. Session-less events are accepted for the pinned Gateway contract
- Steering switches routing before submission. Both old-ID and provisional
  replacement events are bounded until the response. Explicit rejection or a
  proven pre-write failure restores old routing and replays buffered old events.
  Timeout/disconnect after write begins is `GatewayError::OutcomeUnknown`; the
  stream fails with `RemoteOutcome::Unknown`, never restores an unproven old run
  and never retries the command. Local failure/cancellation does not prove remote
  execution stopped. A fresh connection can start new work but cannot resume runs
- Terminal storage is independent of bounded delta output, so full client output
  cannot lose the terminal. Duplicate terminal and late delta events are ignored.
  Local observation deadline/output overflow ends locally with unknown outcome;
  applications may use explicit abort, even after the stream has ended
- An independent urgent lane reserves request capacity for abort. An already
  blocked socket write cannot be preempted by another WebSocket frame; the write
  deadline closes that connection and settles waiting work. This guarantees a
  bounded outcome, not guaranteed delivery of abort to a stalled peer
- Config, client, run-handle, identity, event/result Debug and error messages are
  redacted. Framework frame Debug/serde are intentionally content-bearing.
  **Dependency logging is not redacted:** tungstenite 0.28 logs received/sent
  frames and handshake requests at TRACE, and close reasons at DEBUG. Do not
  enable wire/dependency logging where credentials or payloads must stay private.
  This crate neither changes global logging nor claims end-to-end log redaction

## Pipeline semantics

`OpenClawGatewayProcessor` consumes downstream typed Send/Steer/Abort frames.
Send emits ordinary-priority Started, Text and exactly one End while its output
is deliverable. Steer keeps one logical stream across remote identities. A second
Send stops prior observation once before a new Started. Abort is a system frame,
uses independent tracked work, and remains responsive while normal output waits.
No-op steer after terminal cannot start an unobserved replacement. Pipeline Text
uses the current addressable handle ID at consumption, matching the source
processor; it is not historical per-delta provenance across steering.

Owned output uses bounded downstream capacity waits with an output deadline.
The processor separates its lifecycle serial from remote run/session identity.
End/Stop close the active observation before pipeline terminal output. End/Stop
and Cancel attempt a deadline-bounded abort before disconnecting. Drop closes
local ownership. None invents post-terminal output or unconfirmed remote
cancellation.
Failure reports a redacted upstream error. Ordinary unrelated/upstream frames
retain their direction. OpenClaw payloads have serde support but unrelated native
Python-bus, RTVI, telephony and AudioHook codecs explicitly reject these frames.

## Qualification boundary

Native loopback peers exercise actual socket protocol and pipeline behavior.
This is not a full Pipecat/OpenClaw parity claim: no live Gateway/NemoClaw, device
pairing, remote tool execution, server-version compatibility, or production
network/security deployment has been qualified. Reconnection is explicit rather
than source automatic reconnect; no automatic resumption or retries are promised.
Unsupported terminal object shapes fail closed instead of dumping arbitrary JSON
as response text. Empty messages are rejected by local validation.
