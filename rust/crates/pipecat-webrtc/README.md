# pipecat-webrtc

Native SmallWebRTC audio and application-message transport. The crate uses
WebRTC-rs 0.14's Tokio peer-connection API for SDP, ICE, DTLS, encrypted SRTP,
RTCP, and reliable ordered SCTP data channels. It does not use Python, aiortc,
a paid provider, a browser process, or PCM disguised as an encoded RTP codec.

## Supported behavior

- Gathered offer/answer and trickle ICE, including end-of-candidates
- Configured STUN/TURN URLs; no external ICE servers by default
- One bidirectional audio track and one application data channel per peer
- Opus via the safe `opus` bindings and bundled native libopus (default feature)
- PCMU and PCMA via `pipecat-audio`'s real G.711 codecs, including builds without Opus
- Bounded pre-open JSON buffering, ordered delivery, message-size limits, and
  SCTP buffered-byte backpressure
- Arbitrary JSON/RTVI envelopes preserved without rewriting application fields
- Source-compatible bounded raw `ping`-prefix text heartbeats, consumed without
  a pong or application event; native clients can call `send_keepalive`. Native
  liveness follows ICE/DTLS state, without Python’s three-second ping-age heuristic
- SmallWebRTC `signalling` / `trackStatus` messages for audio receiver index zero
- Bounded outgoing PCM queue with 20 ms pacing; bounded incoming events with
  explicit drop counters; RTP sequence/timestamp/SSRC attached to decoded audio
- Cancellation, explicit shutdown, queue draining, and audio interruption
- Native frame conversion and output mapping with `send_frame` / `into_frame`
- Framework-independent POST/PATCH request bodies, camelCase aliases, peer
  reuse, restart, single/multiple-session modes, and bounded registry admission

## Minimal Rust use

```rust,no_run
use pipecat_webrtc::{SmallWebRtcConfig, SmallWebRtcConnection, TransportEvent};
use serde_json::json;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let caller = SmallWebRtcConnection::new(SmallWebRtcConfig::default()).await?;
let answerer = SmallWebRtcConnection::new(SmallWebRtcConfig::default()).await?;
let offer = caller.create_offer().await?;
let answer = answerer.answer_offer(offer.sdp).await?;
caller.accept_answer(answer).await?;

// Messages may be queued before the data channel is open.
caller.send_app_message(&json!({
    "label": "rtvi-ai", "type": "client-ready", "id": "1", "data": {}
})).await?;
while let Some(event) = answerer.recv_event().await {
    if let TransportEvent::AppMessage(message) = event {
        assert_eq!(message["type"], "client-ready");
        break;
    }
}
caller.close().await?;
answerer.close().await?;
# Ok(()) }
```

`SmallWebRtcRequestHandler::handle_offer` returns both the wire answer and an
`Arc<SmallWebRtcConnection>` to connect to a pipeline. `handle_patch` accepts
candidate patches, including an empty candidate string. After handler shutdown,
new offers are rejected. The HTTP layer must
set request-body limits, authenticate callers, authorize access to each pc_id,
and remove/close completed peers. Peer identifiers are not authentication
credentials. Unknown supplied IDs are rejected rather than silently creating
a new session. Restart closes the existing peer and returns a new ID.

## PCM and lifecycle contract

`send_audio` accepts exactly 20 ms of signed little-endian 16-bit **mono** PCM:
960 samples at 48 kHz for Opus; 160 samples at 8 kHz for PCMU/PCMA. Convert sample
rate/channel count explicitly with `pipecat-audio` before sending. Opus SDP uses
`opus/48000/2`, as required by RFC 7587, even for a mono stream. Incoming Opus is
decoded to mono at 48 kHz; incoming G.711 is decoded to mono at 8 kHz.

Only the configured codec is advertised. Selecting unavailable Opus returns
`CodecUnavailable` before network setup. This makes unsupported builds fail
explicitly rather than claiming a codec they cannot encode or decode.

`send_*` returns when a bounded queue accepts the value. `try_send_*` returns
`QueueFull` without blocking. `flush(timeout)` waits until accepted output has
been handed to the WebRTC stack; it is not a remote playback acknowledgement.
The queue and SCTP buffered payload have separate bounds. `interrupt_audio`
invalidates queued audio and any packet still awaiting its pacing deadline.
`cancel` immediately wakes blocked queue producers/readers. Call `close().await`
to shut down the actual peer and join owned transport tasks. `close` is
idempotent and discards queued output; use `flush` first for graceful completion.
Canceling a close future or reaching its deadline leaves its owned cleanup running;
subsequent calls await that same cleanup. Completed close releases the native peer
and its UDP sockets even when the public connection wrapper is retained. Registry
shutdown likewise retains ownership of every admitted peer across cancellation.
`send_frame` drains on End/Stop, cancels on Cancel, and clears audio on Interruption.

Consume `recv_event` continuously. Reliable application JSON backpressures its
receive handler; audio uses drop-newest when the event queue is full. Read
`stats()` for overload, missing/late RTP, and send/receive counters. State is
available through a watch receiver; fatal asynchronous errors remain available
through `last_error()` even after cancellation. An explicit close is required
for deterministic socket cleanup; Drop provides best-effort cleanup only while
a Tokio runtime is alive.

This crate directly depends on the narrowly patched, vendored WebRTC-rs 0.14.0,
so independent applications using the local crate retain the patch without
having to copy a workspace `[patch]` table. Preserve the repository's vendor
directory when packaging or moving these local crates. Its SCTP
shutdown signal persists across incoming-channel callbacks and prevents late
association/channel admission. Its DTLS shutdown also fences late handshake and
SRTP/SRTCP session publication. Source hashes, licensing and the exact delta are
recorded in `vendor/webrtc-0.14.0/PATCHES.md` and `UPSTREAM_PROVENANCE.json`.
Only the exact upstream reset-after-close error is treated as successful shutdown,
after SCTP, DTLS and ICE report Closed; unrelated or mixed cleanup errors remain
failures.

## Scope and known gaps

This is a native audio/data implementation, not full `aiortc` feature parity.
It does not provide video, camera/screen tracks, hardware microphone/speaker
capture, browser autoplay handling, or the Python transport's ESP32 SDP munging.
Audio exposes RTP timing and drops duplicate/late packets; it does not implement
an adaptive jitter buffer, packet-loss concealment, Opus FEC recovery, or a
synchronized playout clock. Outgoing audio is mono only. Track replacement and
ICE restart on an existing peer are not exposed; the handler's restart creates
a fresh peer. Signaling JSON is not a replacement for the RTVI application
processor; its higher-level actions and lifecycle belong to `pipecat-rtvi`.

Configured TURN/STUN support is provided by WebRTC-rs. Actual TURN relay,
public-network NAT traversal, browser-client interoperability, and device
capture require separate environment-specific validation. No live provider
credentials or external ICE services are used by the tests.

`localhost_only` deliberately selects only `127.0.0.1` with an explicit
WebRTC-rs `Net::Ifs` interface catalog and real operating-system UDP sockets.
It is not the library's virtual-network simulator. It works in containers that
permit loopback UDP but restrict network-interface enumeration, without
changing host interfaces, routes, firewall, or security settings.

## Build and verification

```sh
cargo run -p pipecat-webrtc --example loopback
cargo test -p pipecat-webrtc
cargo test -p pipecat-webrtc --no-default-features
cargo clippy -p pipecat-webrtc --all-targets -- -D warnings
cargo fmt -p pipecat-webrtc --check
```

The default Opus feature needs a C/C++ toolchain, Make, and CMake to build the
bundled codec; pkg-config and an installed libopus development package are not
required. `--no-default-features` removes Opus and uses pure Rust G.711. On this
workspace's build host, official PyPI CMake 4.4.4 is installed locally; invoke
its native executable through:

```sh
CMAKE=/workspace/shared/pipecat-tools/cmake/cmake/data/bin/cmake cargo test -p pipecat-webrtc
```

This build-time path is environment-specific. It does not add a Python runtime
to the resulting application or require a system CMake installation.

## Validation

The credential-free loopback example sends an RTVI message and five encoded
Opus packets between two real peers, then prints a machine-readable JSON result.
The suite covers bidirectional Opus/PCMU/PCMA audio, RTP clocks, gathered and
trickle signaling, JSON order and pre-open buffering, invalid binary/JSON input,
queue limits, cancellation, timeouts, mute/resume, interruption, graceful drain,
registry admission/reuse/restart, codec mismatch, and explicit unavailable Opus.
The README example is compile-checked as a Rust doctest.

## Primary references

- [WebRTC-rs 0.14 source](https://github.com/webrtc-rs/webrtc/tree/v0.14.0)
- [WebRTC-rs 0.14 API](https://docs.rs/webrtc/0.14.0/webrtc/)
- [Opus Rust bindings](https://docs.rs/opus/0.4.0/opus/)
- [Opus RTP payload specification, RFC 7587](https://www.rfc-editor.org/rfc/rfc7587)
- Source compatibility: `src/pipecat/transports/smallwebrtc/{connection,request_handler,transport}.py`

## ICE close/start race under independent review

The historical combined draft gate observed a genuine UDP-candidate release
failure during concurrent shutdown before data-channel readiness. A
terminal-state publication patch is proposed in the locally vendored ICE
transport and gatherer. It fences late callback state, dial/accept mux
publication, and candidate generation after Close. The unchanged concurrent
UDP-port regression remains the required native acceptance test; **no new
full gate, live browser, NAT/TURN or hardware result is asserted by this
source-only patch**. The original SCTP/DTLS fixes are retained. Details,
upstream-file provenance and exact commands are in
`../../vendor/webrtc-0.14.0/PATCHES.md`.
