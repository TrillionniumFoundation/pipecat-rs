# Bounded session resumption

`GeminiClient::new(config)?.with_session_resumption(ResumptionPolicy::default())?`
explicitly enables receive-side recovery. The same client can be passed to
`GeminiLiveProcessor`. Without this builder the setup and unsupported-message
behavior remain unchanged. Public configuration structs have no new fields.

The initial setup adds `sessionResumption: {}`. A valid update containing
`resumable: true` and a nonempty `newHandle` saves one opaque handle, scoped to
that session. A false update revokes the handle. Handle bytes are bounded by
`max_handle_bytes` (default 4096, maximum 65536), and the actual serialized setup
must also fit `max_message_bytes`. Tokens are never exposed in events or errors.
A malformed update fails the session before applying any bundled content.

On an eligible receive failure, the single owned connection worker drops its old
socket and connects again with `sessionResumption: {handle: ...}`. Only after
`setupComplete` may it consume queued input or provider content. A setup ACK may
include a resumption update; if it does not, the previous token is retained.
Connect/setup errors are terminal, including authentication rejection. There is
no fallback to a fresh conversation or history reseeding.

Eligible failures are receive-side I/O errors, closed-connection errors,
unexpected EOF or reset without a close handshake, and close codes 1001, 1011,
1012 or 1013. A GoAway announcement is advisory: it does not itself open a new
socket. In enabled sessions it is consumed internally, and permits recovery
when followed by a normal 1000 or unnumbered close. Explicit protocol/policy
close codes remain terminal even after GoAway. Other protocol errors, oversized
messages and all write errors are terminal. No valid handle means no recovery.

`max_reconnects` bounds total attempts over one session (default 2, valid 1–8).
There is no retry loop around a failed reconnect handshake, stability-based
counter reset, backoff task or implicit idle timeout. Each connect and setup
phase retains its configured finite deadline.

## Delivery and ownership

Already sent text, audio, video, tool responses and partially written multi-wire
actions are never replayed. This provides at-most-once **local sending** of each
admitted action, not lossless delivery or exactly-once provider behavior. A
handle may represent state older than the last input. The provider may replay
output, and native code cannot prove the completeness of restored remote state.
Callers requiring stronger delivery guarantees must not infer them from a
successful resumption handshake.

The existing bounded command queue admits new work during recovery; a full
queue returns input backpressure. Reconnection does not create a second queue.
Logical turn state, origin, manual activity, pending tools and interruption
generation remain owned by the same worker. Previously queued output remains
part of the same logical session. A pending tool result can be sent after
recovery without replaying the original tool call; repeated provider tool IDs
still fail the existing duplicate-ID check. Local-generated IDs are preserved.
Interruption during the handshake is applied before the new socket is read or
queued commands are processed, using the existing output/video/tool generation
rules. This does not retract input already accepted by the server.

Cancel bypasses queues and interrupts connect/setup waits. Normal shutdown joins
the worker. Dropping a session requests cancellation and aborts its owned worker,
with the same existing task-drop semantics as ordinary sessions. Once finish is
admitted, a new recovery is prohibited. Finish arriving during a recovery
handshake wakes that wait and terminates it with `Closed`; it does not fabricate
a graceful completion of the interrupted turn. Finish admission itself may
still succeed, with the terminal error subsequently returned by `next_event`.

## Source scope

The pinned Python implementation at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf` enables session resumption, remembers
successful handles, reconnects after receive exceptions and skips history
reseed when a handle exists. Send-side errors are fatal. Its third consecutive
receive failure is terminal, with a counter reset after a stable connection;
normal SDK receive-iterator completion is not directly a reconnect trigger.
It has no explicit GoAway branch.

Native differences are deliberate: opt-in setup, cumulative attempt bounds,
revocation on non-resumable updates, no handle-less history fallback, selective
transport-error recovery and one worker that drops the old socket before
opening its replacement. The optional wire shape also follows Google's
[session-management reference](https://ai.google.dev/gemini-api/docs/live-api/session-management).
Transparent message-index recovery and delivery acknowledgements are not
implemented. Loopback tests establish native WebSocket and lifecycle behavior;
they do not establish live model availability or provider restoration accuracy.
