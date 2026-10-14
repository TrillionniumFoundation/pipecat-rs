# Native RTVI text/LLM WebSocket client

`RtviClientProcessor` joins the existing bounded `WebSocketSession` and fixed
`RtviClientFrameSerializer` to a real native Pipeline. Construct a config with
`RtviClientConfig::new(endpoint)`, construct the processor, retain its `control()`
handle, and place the processor at the desired network boundary. The processor
is single-use, including across sequential pipeline invocations; create a new
instance and control handle for a new connection. No connection is made by the
constructor. Start is forwarded successfully before the owned connection task
is admitted.

This restricted profile is text/LLM-only. It is not the complete Python generic
WebSocket input/output transport, an audio backend, a playback-receipt source,
or a replacement for the existing browser/server transport or eval runner.

## Fixed source and native boundaries

At Python revision
[`af9f9da9d7afddaa8811c0e0251b32566c980bdf`](https://github.com/ProfHepta/pipecat/tree/af9f9da9d7afddaa8811c0e0251b32566c980bdf):

- `src/pipecat/transports/websocket/rtvi_client.py`, Git blob
  `0611db939f4c1a0c360fe0baa060b60e99460f74`, sends `client-ready` after connecting,
  recognizes the first `bot-ready`, and delegates message serialization to the
  WebSocket base. It explicitly disables WAV headers.
- `src/pipecat/transports/websocket/client.py`, Git blob
  `6f0d17d9fd4f832f515736d1b36431d4e6a2bc39`, owns the socket receive lifecycle,
  deserializes messages into frames, and serializes outgoing transport messages.

The native handshake sends the existing RTVI protocol version, id `client-ready`
and about-library `pipecat`. As in the source's readiness predicate, a JSON object
with the RTVI label and type `bot-ready` is sufficient; readiness does not imply
validation of all `ServerEvent::BotReady` fields, remote authentication or a
successful application request. Duplicate readiness messages do not start a new
run. `control().is_ready()` also checks the original invocation scope.

Before readiness, LLM packets are discarded and outbound RTVI commands are
rejected. This explicit native gating and the handshake deadline are stricter
than the Python base's unconditional message dispatch.

Inbound wire type and decoded `FrameData` are both allowlisted:

| RTVI type | Native frame |
| --- | --- |
| `bot-llm-started` | `LlmResponseStart` |
| `bot-llm-text` | `LlmText` |
| `bot-llm-stopped` | `LlmResponseEnd` |

All other messages, including function calls/results, remote interruption,
transcription, audio, UI and metrics, do not gain pipeline control authority.
Malformed/unknown packets are ignored under the existing codec profile. This
does not assert that remote text is trusted application instruction. The remote
peer remains responsible for the content it supplies.

Outbound processing is limited to preconstructed RTVI-labeled
`OutputTransportMessage` and `OutputTransportMessageUrgent`. Ordinary upstream
messages pass through; urgent messages use the source's direction-independent
urgent handling. Unsupported serializer output is not sent and the original
frame passes through. Successful local command admission consumes that command
frame. Other frames pass through unchanged. Existing native-authority and
OpenClaw serializer rejection remain in force. Named destinations and timestamped
outbound commands are rejected: this single-connection profile has no destination
routing or timed-output scheduler.

No audio encoding, resampling, WAV wrapping, PCM pacing, custom codec, automatic
reconnection, provider login or arbitrary remote tool execution is added.

## Bounded admission and causality

The endpoint is a `ws` or `wss` URL with a literal IPv4 address, no user-info or fragment,
and a 2 KiB raw/canonical limit. The canonical parsed URL is saved and used for
the actual connection, including nonstandard numeric IPv4 spelling normalization.
Hostnames and IPv6 are excluded to avoid a blocking resolver tail escaping
cancellation. The locked tokio-tungstenite 0.28 rustls connection helper strips
IPv6 brackets before constructing its socket-address string, which does not take
Tokio's direct SocketAddr parse path. This profile does not modify that shared
connection helper. TLS uses normal certificate validation; IPv4 restriction
does not relax it.
Errors/statuses contain fixed reasons, not endpoint or payload text.

Session defaults are 32 packets of at most 64 KiB in each direction. The packet
count and message size are capped, and their product cannot exceed 16 MiB per
session queue. The additional native outbox defaults to 32 commands/1 MiB, with
hard caps of 1,024 commands/16 MiB. Connection, write, close and handshake durations
must be positive and no greater than 60 seconds. These are bridge-owned budgets;
they do not bound unrelated downstream queues or the caller's original frame
allocations. JSON preflight bounds depth, node count and serialized byte cost
before the fixed codec allocates an outgoing string.

Outbox count/byte exhaustion is observable as an error and closes this run.
Downstream admission uses the existing fail-fast queue operation, so a blocked
pipeline cannot grow an unbounded receive backlog. No timing-dependent assertion
that a small loopback socket buffer is truly saturated substitutes for the
deterministic queue-limit tests.

The Start invocation fence is captured before any background work. A setup base
context is only the receiver identity: every inbound frame uses the original
`try_push_with_invocation_fence` operation, rather than a stale-check followed by
an unfenced asynchronous push. Outbound local admission intersects that original
scope with the sending frame's invocation. Waking the owner happens after runtime
and state locks are released. Bytes already admitted to the socket session cannot
be transactionally recalled when an epoch retires.

Local Interruption terminates the entire run, clears readiness and closes the
connection; the original interruption still passes through. This differs from
the source retaining its connection. The native profile does not rebind old
remote responses to a new local epoch without a response correlation protocol.

## Shutdown ownership and evidence

End, Stop, Cancel, control-close and failure close admission and discard pending
native outbox entries. `discarded_outbox_commands()` counts entries discarded
from that queue; it says nothing about an already selected or session-admitted
packet. Neither pipeline send success nor queue admission proves peer receipt.

The run stores its `WebSocketSession`; the tracked task borrows it. The private
borrowed session shutdown retains its JoinHandle if the awaiting future is
cancelled, allowing cleanup to resume the join. End/Stop prepare-end signals the
outer task, waits for its ownership guard, then joins any remaining session.
Cancel and worker Drop rely on core background drain followed by processor
cleanup. Drop requests cancellation; it does not synchronously await a socket.
Shutdown deadlines can force cancellation and are not reported as graceful
completion. Peer close, handshake/write failure and rejected task admission use
the same retained-resource cleanup path.

Tests use actual loopback TCP/WebSocket sessions and native Pipelines, plus
explicit private queue/ownership fixtures. They establish framing, scope,
backpressure and eventual owned cleanup. They do not establish remote deployment,
internet availability, real provider authorization or audio behavior.
