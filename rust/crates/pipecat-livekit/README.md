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
- Optionally publish one fixed-size `pipecat-video` camera track from raw output images
- Opt in to bounded I420 camera/screen input as participant-tagged RGB `UserImage`
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
publishes all configured output tracks before returning a ready session. Pass
`output_format: None` and leave `video_output: None` for receive/data-only operation. Tokens are supplied by the
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
and urgent output tasks. Audio publications and explicitly enabled video publications
use the participant filter. Video is disabled by default. Non-video application event
admission is nonblocking: a full queue
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

## Optional bounded camera output

Set `video_output: Some(VideoOutputConfig { width, height, ..Default::default() })`.
It is disabled by default, creates one room-wide track named `pipecat-video`, and
publishes with SDK `LocalTrack::Video` / `TrackSource::Camera` after real room join.
Audio publication happens first when enabled; Connected/readiness waits for both.
A failed or cancelled publication rolls back the room, retaining primary and
cleanup errors. There is no separate video publisher task: synchronous native
capture and terminal source/track release share one lock. All clones share this
gate, so completed close excludes later capture and releases adapter-owned native
source/track references. An externally cancelled token is checked inside the gate;
a capture already in progress cannot be recalled. The SDK's raw-source keepalive
uses weak capture state and exits after first capture or last-source drop; this
adapter does not own/join that SDK-internal task or promise instantaneous exit.

Pipeline `OutputImage` and `publish_video` accept exact configured input dimensions.
These are not a promise of identical SDK output dimensions: native sink wants and
source adaptation may scale, crop, align or drop frames. The pinned SDK source
uses alignment 4, so native passthrough fixtures use dimensions divisible by 4;
odd/small inputs remain supported by conversion but may be adapted or dropped.
Accepted input formats:

- RGB bytes R,G,B; RGBA bytes R,G,B,A; BGRA bytes B,G,R,A; ARGB bytes A,R,G,B.
  Alpha is discarded, not composited. Explicit uppercase format is mandatory.
- I420 is a Rust extension: packed Y then U then V, with ceil(width/2) by
  ceil(height/2) chroma. Odd sizes are supported. No caller-provided strides.
- Payload length must be exact, without short or trailing bytes. Dimensions,
  allocations, source strides and native integer conversions are validated before
  FFI. Destination planes use SDK-owned initialized I420; packed colors normalize
  to RGBA bytes and use libyuv ABGR register-order conversion.
- `sync_with_audio: true` and any image `transport_destination` are rejected before
  queue admission. No rescaling, private routing, sprite rendering or scheduling.
  Pipeline PTS is not cast into SDK time: `timestamp_us=0` requests native capture
  time (and SDK timestamp alignment). This is not audio/video synchronization.

The dedicated video lane accepts 1–4 queued frames (default 4), independent of the
normal audio/data queue. Size is capped at 4096 per dimension and 2,073,600 pixels.
Raw bytes AND Vec capacity must fit `max_frame_bytes` (at most 8 MiB). Configuration
checks `(queue_capacity + 1) * max_frame_bytes + width * height * 4 + I420_bytes`
against `max_retained_bytes` (at most 64 MiB). This bounds queued/in-flight adapter
raw bytes plus one conversion scratch/native buffer; conversion is serialized,
including direct callers. Caller-owned borrowed inputs, general frame metadata,
SDK-retained frames, encoder, room events and downstream sinks are outside this
payload budget, so it is not a whole-process memory limit.

Interruption/reconnection invalidates older interruptible images at both the
pipeline epoch and native video gate; noninterruptible images survive ordinary
interruption. End closes video admission and drains the owned lane to synchronous
capture, not encoder/network/remote completion. Cancel closes the native gate and
discards all queued images, including noninterruptible ones; cleanup does likewise.
Frames already forwarded to WebRTC cannot be recalled. Upstream images pass through
without video validation or publication. Saturation is an explicit processor error.

`VideoCaptureOutcome::ForwardedToSource` means the native adapter forwarded the
frame. `AdaptationDropped` means the native adapter did not forward it, including
resolution/frame-rate adaptation or having no interested sinks; it is not a
network failure. Neither outcome proves track publication, encoding,
RTP transmission, remote receipt, rendering, or playback. Pipeline processing does
not fabricate acknowledgements for either outcome.

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

The historical 2026-10-09 qualification recorded 23 unit tests covering protocol/frame conversion, identity versus SID,
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

General native-buffer video conversion, device capture, manual image requests, source sampling, participant audio mixing,
metadata mutation, subscription/mute controls, token/REST helpers, and Python
API/event-handler parity are not implemented by this crate.

### Camera output qualification boundary

The default-disabled camera path accepts room-wide `OutputImage` /
`OutputImageDetailed`, validates exact format, dimensions and payload size, and
rejects per-participant destinations and audio synchronization before admission.
A separate bounded video lane feeds owned native I420 buffers. Typed sequential
publication preserves audio-before-video readiness and rollback ownership.

Revision 5 passed **46 independently verified crate unit tests** (L07).
Strict package all-target Clippy (`-D warnings`) and crate formatting checks
both passed on that unchanged revision (Q06, natural exit zero). Two earlier
integration tests passed on revision 4: one `review_contracts` (R02) and one real
loopback WebSocket `network_signaling` (N02). Their results are carried through
an explicit reviewed unaffected/semantic-equivalence bridge, not represented as
revision 5 executions. This is 48 distinct qualified test cases, not 48 tests
rerun on revision 5; integration scenarios and repeated historical runs are not
additional tests. `local_sfu` was compiled and linked only on revision 4 (S01),
with the same explicit carry bridge: zero runtime tests, no listing, and no
server startup. The previous netlink denial remains a boundary and was not
bypassed or retried.

Native-synthetic media checks use linked WebRTC, not stubs, but do not join a
room. Publication transaction tests use mocks, not a successful SDK publication.
There is no live-provider, joined SFU, remote video, physical device, codec/RTP,
production-readiness or full Python parity claim. Earlier whole-workspace and
vendored signaling test counts are historical and are not added to these 48.

## Opt-in incoming video subset

`LiveKitConfig::video_input = Some(VideoInputConfig::default())` enables bounded,
all-frame camera and screen-share input. It is disabled by default. The same
participant filter and opt-in predicate select both initial room publications and
later publications. Screen-share maps to `screenVideo`; other video track sources
map to `camera`, as in Python pin `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

This slice accepts concrete SDK I420 buffers only. Dimensions and pixel/stride/byte
products are checked before allocating or invoking the inspected I420-to-ABGR
conversion. ABGR output bytes are RGBA in memory, compacted in place to RGB. No
`to_i420()` copy or arbitrary native/platform conversion is attempted. Unsupported
buffers or invalid frame layouts are dropped individually, not promoted to a fatal
room error. `connection.video_input_stats()` exposes rejection/pressure-drop counts
and a bounded latest rejection string. These counters do not count downstream
retention, native sink overflow, or every lifecycle-invalidated queued frame.

Dedicated public video subscribed/unsubscribed lifecycle notifications remain
deferred; this slice adds the `Video` data event, not the full Python event API.

`LiveKitEvent::Video` maps through the production input dispatcher to
`FrameData::UserImage`. `user_id` is participant identity, `transport_source` is
`camera` or `screenVideo`, and `livekit.track_sid` is independent metadata. Format is
`RGB`; request, text, append-to-context and run-LLM fields remain absent. Native
rotation is not applied and native timestamps are not translated into pipeline PTS.

Each participant/source owns an independent SID/generation and at most one reader.
Replacement, mute, unsubscribe, participant removal and teardown invalidate the
generation, cancel the reader, close the sink and join the task before releasing
its slot. Muted subscriptions retain a bounded ownership slot but no native sink;
unmute only resumes the current SID with a new sink and generation. Old completion,
unsubscribe or unmute cannot remove/resume a replacement. Reconnect clears owned
readers and reconstructs them from current SDK remote publications. This is lifecycle
logic, not qualification of remote reconnect/media delivery.

Default limits are four owned tracks (including muted slots), one native queued
frame per reader, four pending converted frames, 4096 maximum dimension, 2,073,600
pixels, 8 MiB per conversion and 32 MiB retained converted storage. Configurable
hard maxima are 16 tracks, four native frames, 16 pending converted frames and
64 MiB retained storage. Native queue capacity must be positive: SDK `Some(0)` is
unbounded and is rejected. Native overflow keeps the latest frame; adapter pending
or byte pressure drops the newly received frame without blocking room control.

Private non-cloneable envelopes hold both pending-frame and byte permits through
the final `recv()` admission boundary. The charged size includes full RGBA scratch
capacity retained after RGB compaction, not merely RGB length. Lifecycle validity
is checked under the same lock used for invalidation at that boundary. An event
already admitted before invalidation cannot be recalled. Caller clones, public
event retention, and downstream pipeline storage are outside this adapter budget;
`ProcessorContext::push` is bounded admission, not downstream consumption. Native
sink limits apply after native allocation: decoder buffers, SDK room events and
upstream/native queues are not a whole-process or untrusted-network memory bound.
`close().await` joins owned readers and drains the private video lane; `Drop` only
requests best-effort cancellation.

Local native source/track fixtures and synthetic lifecycle tests do not establish
remote subscription, SFU, RTP, codec decoding, devices, browser, NAT/TURN or remote
reconnect behavior. Source sampling, manual image requests and full Python input
transport parity remain deferred. Existing audio, data, DTMF and outgoing-video
paths are retained. No SFU/netlink experiment is part of this slice.
