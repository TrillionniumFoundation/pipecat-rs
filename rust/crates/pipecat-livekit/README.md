# Native LiveKit transport

`pipecat-livekit` uses vendored sources of the official `livekit = 0.9.4` Rust
SDK and its native WebRTC library, with a narrow signaling task-ownership fix.
Archive checksums, the upstream license and the reviewable patch are in
[`vendor/ORIGINS.md`](vendor/ORIGINS.md) and
[`vendor/PATCHES.md`](vendor/PATCHES.md). The fixed signaling dependency is
selected by relative paths, including in independent application workspaces.
It does not invoke Python, a subprocess transport bridge, or an
application-provided mock backend.

## Supported path

- Join an existing LiveKit room with a caller-supplied participant JWT
- Publish one microphone audio track, receive each remote audio track separately
- Convert interleaved PCM16-LE with exact sample-rate/channel validation
- Preserve participant identity separately from participant and track SIDs
- Receive participant connect/active/disconnect, track subscribe/unsubscribe,
  active-speaker, reconnection, data, and SIP-DTMF events
- Send reliable or lossy data packets, optionally addressed to an identity
- Send native SIP-DTMF keypad events
- Use `LiveKitTransport::input()` / `output()` as core pipeline processors
- Explicit connection state, bounded application queues, bounded audio-reader
  count, cancellation, and awaited shutdown

`LiveKitConnection::connect(url, token, config, cancellation)` connects and
publishes the configured output track before returning a ready session. Pass
`output_format: None` for receive/data-only operation. Tokens are supplied by the
application and are not generated or persisted here. Use `wss://` outside a
trusted local development environment.

The connection's `recv()` event API supports arbitrary binary data and preserves
an unattributed server packet's `identity: None`. The pipeline input consumes
that event stream and accepts JSON transport messages; malformed JSON is an
explicit processor error. Do not simultaneously consume `recv()` yourself and
through the pipeline input. Subscribe to `subscribe_state()` for independent
terminal state, including receive-queue overflow.

Audio arrives as participant-tagged `UserAudio` frames with `transport_source`
and `livekit.track_sid`. A single-speaker STT/VAD should configure
`participant_filter` rather than silently combining different speakers. Outgoing
PCM must exactly match `output_format`; resample with `pipecat-audio` before
publication. Incoming resampling/channel conversion is performed by the SDK.

Data-frame `transport_destination` selects a participant identity, and optional
`livekit.topic` metadata selects a data topic. Audio has one room-wide published
track and rejects per-participant audio destinations.

## Playback and interruption boundary

`publish_audio` returns `SourceAccepted`: the native SDK has accepted the PCM.
It does **not** attest remote receipt, decoding, loudspeaker playback, or what a
person heard. The pipeline output therefore does not implement the shared
`OutputBackend::Played` contract, emit bot-speaking/playback acknowledgements,
or commit spoken-history text. Text/order markers passing this endpoint are not
playback acknowledgements.

Interruption invalidates pending interruptible pipeline output and clears the
native source buffer. It cannot recall RTP already handed to WebRTC, remote
jitter buffers, or sound already played. A zero source queue requires exactly
10ms audio frames; positive source queues must be multiples of 10ms. Cancellation
of a pending native capture also clears buffered source PCM.

## Ownership and limits

One owned room task coordinates the SDK event receiver, a single bounded native
audio publisher, and a capped `JoinSet` of audio readers. The native publisher
splits buffered writes into source-sized chunks and drains a cancelled capture
callback before allowing a new write. The pipeline owns separate bounded normal
and urgent output tasks. Only accepted audio publications are subscribed;
unsupported video tracks are not automatically subscribed. Application event admission is nonblocking: a full queue
closes the session with an observable failure rather than allowing unlimited
buffering. PCM, data, and event metadata sizes are checked; JSON serialization itself is
bounded. Reconnect invalidates pending interruptible audio. The SDK's audio sink queues
are bounded to its default 10 frames (roughly 100ms), dropping oldest frames on
overflow. The SDK's room event channel remains unbounded upstream of this
adapter; bursts may allocate there before this adapter can drain them. These adapter
limits are not a hard whole-process memory bound.

The room owner starts immediately after SDK join and owns source-publication
rollback. Cancelling the supplied token and awaiting connection completion
allows that rollback to finish. Dropping a post-join connect future signals the
same owner to close, but cannot synchronously await or verify the result.
Before SDK join completes, signaling tasks have abort-on-drop owners so a
cancelled connection, aborted caller or join timeout releases their transport
references once the runtime polls cancellation. This is also preserved if
signaling close itself is cancelled.

Call `close().await` for verified teardown. It clears source audio, cancels and
joins readers, closes the SDK room within the configured timeout, and joins the
room task. A cancelled close future leaves its join handle stored for another
close call to await. Repeated close calls are safe. Dropping the connection only signals
the existing owner task to perform bounded best-effort shutdown; an async Drop
cannot report or await completion. Runtime shutdown can prevent that cleanup.

## Native build

On Linux the pinned SDK requires Clang 21 or newer, C++ development tooling,
`pkg-config`, and GLib/GObject/GIO development headers. Its build script obtains
an official platform WebRTC binary from the LiveKit release below, or uses an
explicit `LK_CUSTOM_WEBRTC` directory. Hardware video acceleration is optional.
No compiler flags are overridden to weaken the SDK's libc++ ABI checks.

- [Official Rust SDK](https://github.com/livekit/rust-sdks)
- [Raw media documentation](https://docs.livekit.io/transport/media/raw-tracks/)
- [Pinned native release](https://github.com/livekit/rust-sdks/releases/tag/webrtc-89d790b)
- [Official Debian Clang 21 package](https://packages.debian.org/trixie-backports/amd64/clang-21)

Linux x86-64 build artifact: `webrtc-linux-x64-release.zip`, 156,139,058 bytes,
SHA256 `b167adad5291cea0e4d66a0454d9d52d2ad714e6b0ed70f4410317d3ebde70c5`.
Its bundled third-party licenses remain in the extracted artifact's `LICENSE.md`.

## Qualification and remaining scope

The 23 unit tests cover protocol/frame conversion, identity versus SID,
unattributed server data, DTMF codes, format/size/JSON limits, bounded queue
admission, independent urgent output, cancellation before and during signaling,
SDK HTTP validation rejection, real native source/track/sink stereo PCM delivery,
interruption callback draining, and pending-capture shutdown. A separate
integration test covers seven signaling ownership scenarios through the SDK's
injected in-memory transport API. Another covers six scenarios over real
loopback TCP/WebSockets, including an HTTP 101 upgrade and peer-observed socket
closure. Its test-only injected transport honors the host's existing loopback
proxy exclusion; it does not qualify the SDK's default proxy path. The patched
signaling crate's 47 unit tests also pass. Local HTTP fixtures exercise the SDK's
HTTP validation fallback. See
[VALIDATION.md](VALIDATION.md) for the verified commands and native provenance.
They do not establish successful server join, SFU audio/data delivery,
reconnection after network loss, hosted LiveKit compatibility, or remote
playback. An official, checksum-verified local SFU startup attempt was blocked
by denied host network-interface enumeration. No security settings were changed.
The opt-in `local_sfu` test is compiled but unexecuted on this host; on a permitted
Linux host it owns a disposable loopback-only server and short-lived fixture
tokens, with no hosted account or stored credentials. See
[local SFU qualification](qualification/LOCAL_SFU.md) for the exact limits.

Video capture/publication, screen-share selection, participant audio mixing,
metadata mutation, subscription/mute controls, token/REST helpers, and Python
API/event-handler parity are not implemented by this crate.
