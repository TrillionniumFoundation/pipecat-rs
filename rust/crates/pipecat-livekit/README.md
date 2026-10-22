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

General native-buffer video conversion, device capture, manual image requests, participant audio mixing,
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

## Bounded active-speaker change observation (batch163)

`ActiveSpeakers` still reports every admitted SDK list, including empty and repeated
lists, with identities and ordering unchanged. `ActiveSpeakerChanged { identity }`
adds an observation when the last identity in a nonempty list differs from the
last successfully admitted change in this session. Identity is not participant
SID. Local and participant-filter-excluded identities remain eligible; no second
sort or empty-identity normalization is applied. The inspected pinned SDK sorts
ascending audio level, so its last identity is the loudest, preserving SDK ties.

One optional copied identity belongs to the room owner. Empty updates, participant
departure and automatic reconnect do not reset it; a fresh owner starts without
one. No silence, departure, callback, VAD or current-speaker lease is invented.
This follows the state selection in Python pin
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, not its asynchronous callback API.
The new event's `into_frame()` returns `None`; production pipeline input consumes
it without fabricating a frame. Direct receive and pipeline input are still
mutually exclusive consumers, not independent observation subscriptions.

Before admission or retained-state change, lists must satisfy `max_audio_tracks`
and the existing 65,536 aggregate identity UTF-8 byte limit. The derived event
validates the same single-identity byte ceiling. Only one identity copied from
validated bytes is retained, not the source String's spare capacity or a history.
SDK allocations, queued public events and downstream retention remain outside
this tracker bound; it is not a whole-process heap guarantee.

Admission is deliberately non-atomic: raw first, changed second, then update
retained state only after the changed observation is admitted. An empty or
unchanged list needs one slot. A changed list needs two: a capacity-one lane can
admit raw and then return `QueueFull`, leaving prior tracker state unchanged.
`QueueFull`/`Closed` propagates to existing terminal owner cleanup with no retry,
rollback or duplicate raw event. Queued raw history is not recalled; terminal
Error notification is best-effort. This increases control-queue pressure, and the
new public enum variant can require changes to downstream exhaustive matches.

Batch163 passed **109 fresh test cases** on the same frozen source: 107 library
tests (including ten new tests), one synthetic signaling ownership integration
test and one real loopback WebSocket signaling integration test. All had zero
failed, ignored, measured or filtered tests. Preflight and final strict package
all-target Clippy (`-D warnings`) and `fmt --check` passed; these checks and the
test results were independently verified. Final Clippy reused 373 compiler
artifacts, all marked fresh by Cargo; it was not a fresh rebuild. `local_sfu`
compiled and linked only: **zero tests executed**, with no listing or server
startup. See [VALIDATION.md](VALIDATION.md) for source binding and diagnostics.

Departure/reconnect preservation is source-qualified by room ownership, not an
executed joined-room test. The cleanup test does not qualify full room/audio/
publisher teardown. No hosted CI, SFU runtime, remote media/device validation,
full Python API parity or complete replacement is claimed.

## First-participant observation (batch164)

`FirstParticipantJoined { identity }` is an observation following the ordinary
`ParticipantConnected` for the first admitted remote join in the current armed
period. Both live joins and the initial connected snapshot use the same helper;
snapshot iteration and subsequent subscription ordering remain SDK-delivered.
Later joins, including duplicate identities, still emit their ordinary event.
There is no identity sorting, membership deduplication, empty-identity rewrite,
local-participant synthesis or media-filter eligibility restriction.

The room owner retains one bool, initially false. Departure keeps video cleanup
and audio cancellation before its ordinary `ParticipantDisconnected` admission.
Only after that admission succeeds does it sample the current unfiltered SDK
remote collection and re-arm if empty. Invalid metadata, full or closed queues do
not re-arm. Remaining remotes prevent re-arming regardless of media-reader count.
Reconnect, empty initial snapshots and unrelated observations do not reset it.
SDK membership may advance while events wait in the queue; this is a current
snapshot condition, not an event counter. The SDK's `remote_participants()` clones
its map: transient snapshot allocation and SDK membership storage are outside the
one-bool tracker bound, as are public event queues and downstream retention.

This follows state selection in Python pin
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, not Python callback completion order.
Python awaits ordinary callbacks, sets its flag before awaiting the first callback
and preserves the flag on a reused client unless an empty-room departure resets
it. Rust has a fresh owner per connection and sets true only after derived-event
admission succeeds. Python asynchronous callback completion and overlapping
initial enumeration can choose a different first identity; no deterministic
cross-language ordering or reused-client lifetime equivalence is promised.

Full raw ParticipantInfo identity/SID/name/metadata validation precedes admission;
its aggregate limit is 65,536 UTF-8 bytes. The derived identity has the same byte
ceiling. Raw-first admission is non-atomic: a one-slot queue may retain the raw
join and reject the derived observation. `QueueFull` or `Closed` propagates into
existing terminal cleanup, leaving the bool unchanged on derived failure, without
retry, rollback, duplicate raw emission or resumed owner processing. Queued raw
history remains; error notification is best-effort.

The derived event's `into_frame()` returns `None`, while ordinary joins retain
`ClientConnected` projection. It grants no callback, media, current-membership
lease or automatic application-action authority. Direct receive and pipeline
input remain mutually exclusive consumers. The public enum addition can require
downstream exhaustive-match changes.

Batch164 passed **122 actual test cases** on the unchanged source checkpoint
[`b806f9b88061afdd3449062c8264920b2f7964a6`](https://github.com/TrillionniumFoundation/pipecat-rs/commit/b806f9b88061afdd3449062c8264920b2f7964a6):
120 library tests, one synthetic signaling ownership integration test and one
real loopback WebSocket signaling integration test. All exact inventories were
freshly compiled, listed and executed with zero failed, ignored, measured or
filtered tests, and independently accepted. The library tests include normalized
first-participant admission/reset/limits, production dispatch and the owned-video
cleanup seam. Actual SDK Connected/live ingress and reconnect/reset wiring remain
source-inspected, not joined-room tests; the cleanup fixture does not qualify
full room/audio/publisher teardown.

Cold preflight and final strict package all-target Clippy (`-D warnings`) and
`fmt --check` passed. Cold Clippy reported 373 non-fresh artifacts; its all-fresh
witness matched the complete canonical inventory apart from the `fresh` field.
Final Clippy reused 373 artifacts, all fresh, with full canonical equality to
that witness; it was not another cold rebuild. `local_sfu` freshly compiled and
linked only: **zero tests executed**, no listing and no server startup.
Clippy retained 24 vendored LiveKit warnings, and each selected L/R/N/S build
also retained a linker warning about unused Clang `--no-default-config`.
These are not warning-free builds. Selected executables omit final DWARF; see
[VALIDATION.md](VALIDATION.md) for diagnostics, source binding and exact scope.

Source-recovery history, 2026-10-22: `b806f9b8` was originally published as an
honest source-only, native-untested checkpoint. The results above were obtained
afterward on that unchanged checkpoint. This documentation-only update is not a
separately compiled revision or a hosted CI result. Batch163 and older results
remain historical and are not added to the 122 tests. Full Python API/callback
parity, hosted SFU join/runtime, remote media/device/RTP and complete replacement
remain unqualified.

## Opt-in incoming video subset

`LiveKitConfig::video_input = Some(VideoInputConfig::default())` enables bounded,
camera and screen-share input (all frames unless sampling is configured). It is disabled by default. The same
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

`VideoSubscribed` and `VideoUnsubscribed` report effective selected video ownership
transitions through the existing bounded connection control-event lane. Both carry
participant identity (not participant SID), track SID and `VideoSource`. They are
observations only: `into_frame()` returns `None`; the pipeline input consumes them
without creating images, callbacks or other frames. The connection receiver and
pipeline input are not independent observer subscriptions. No timestamp, capture,
pixel arrival or downstream delivery is implied.

Subscription is observed only after ownership and the unmuted native reader/sink
are installed; an initially muted owner also counts, without a sink. A same-SID
duplicate is silent. Replacement joins the old reader before installing and
observing the new SID, without inventing an old-SID unsubscribe. A matching SDK
unsubscribe invalidates the generation and pending requests, cancels and joins the
reader, then emits. Stale or duplicate unsubscribe is silent. The pinned SDK emits
unsubscribe before unpublish, so normal unpublish yields the real unsubscribe;
a synthetic reversed order removes ownership first and suppresses the stale event.
Mute/unmute, participant departure, reader EOF/failure, cancellation, close and
reconnect teardown/reconstruction do not fabricate lifecycle observations. After a
silent reconnect rebuild, a same-SID SDK subscription is still a duplicate; a new
SID is an effective transition.

Each observation validates at most 65,536 aggregate identity/SID UTF-8 bytes and
uses the existing bounded nonblocking control queue. Queue-full or receiver-closed
errors propagate to the room owner's terminal cleanup; completed ownership changes
are not rolled back merely to retry notification. The terminal error event remains
best-effort under queue pressure. Already queued observations are historical and
are not recalled; there is no total ordering between control and video-data lanes.
Already returned images likewise remain outside recall.

Unlike Python's pinned subscription callbacks, these observations require configured
video input and a selected participant, and deduplicate effective ownership by SID.
Python emits even when video processing is disabled and does not apply these stale
SID guards. This is intentional bounded hardening, not exact callback-count parity
or the full Python event API. Local native tests exercise production normalized
handlers and terminal video cleanup without qualifying hosted-room/SFU behavior.

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
reconnect behavior. Plain pipeline image requests are opt-in as described below; full Python input
transport parity remains deferred. Existing audio, data, DTMF and outgoing-video
paths are retained. No SFU/netlink experiment is part of this slice.


### Batch162 qualification status

The bounded lifecycle observations passed **99 fresh test cases** on the same
frozen source: 97 library tests (including seven new lifecycle tests), one
synthetic signaling ownership integration test, and one real loopback WebSocket
signaling integration test. All had zero failed, ignored or filtered tests and
were independently verified. Preflight and final strict package all-target
Clippy (`-D warnings`) and formatting checks passed and were independently verified.
`local_sfu` compiled and linked only: **zero tests executed**, independently
verified. These checks do not qualify SFU runtime or remote video.

The selected test executables retain compiler debug level 1, assertions, ordinary
symbols and unwind data, but omit final non-ALLOC DWARF; source-line diagnostics
are reduced. See [VALIDATION.md](VALIDATION.md) for exact scope and retained failure.

This report records validation scope; commit publication and remote CI are separate
checks, not inferred passes. No SFU runtime or full Python parity is claimed.

### Bounded source sampling

`VideoInputConfig::sources` is a typed source map: empty means all sources/all
frames; a nonempty map captures only listed camera/screen sources. Values are
`VideoCaptureMode::AllFrames`, `FramesPerSecond(1..=1_000_000_000)`, or
`RequestOnly` when plain requests are explicitly enabled. A numeric zero rate is
still rejected; request-only is not silently enabled. Input remains opt-in and
participant filtering remains authoritative; sampling does not alter subscription.

`connection.capture_participant_video(identity, source, mode)` (also exposed on
`LiveKitTransport`) installs an explicit override synchronously, including before
subscription. Success means installed, not that the participant or track exists.
Python's default capture arguments correspond to `Camera` and
`FramesPerSecond(30)`. Overrides take precedence over the configured source map.
Updates preserve cadence and apply to subsequent sampling decisions, not already
queued/admitted images. They do not stop/restart the native reader.

The adapter reuses shared `VideoInSampler<()>`; plain request selection inserts
one private bypass token only when the original invocation and source are valid.
Sampling happens before layout/conversion and before converted-byte/pending-frame
permits. It uses monotonic arrival time from one session origin, deliberately
unlike pinned Python's `time.time()` wall clock. First periodic frames are due;
early allowance is min(50ms, half interval); stalls strictly longer than one
interval reset without catch-up bursts. Rate changes and all-frame switches retain
the existing schedule. This targets policy compatibility, not exact floating-point,
clock-jump, callback, or real network-jitter parity. A selected frame that fails
conversion or queue admission consumes its cadence opportunity.

Capture policy/schedule entries are independent per identity/source. They share
one short lock with SID/generation validity, so stale readers cannot advance a new
generation's cadence. Policies survive mute, unsubscribe, replacement and reconnect
reader teardown; participant departure removes even pre-subscription policies,
and terminal close clears policies and rejects further updates. Existing final
admission fencing, fresh unmute sinks, reader joins and pixel accounting remain.

Defaults permit 16 retained policies and 64 KiB total identity bytes, configurable
to at most 128 policies and 64 KiB. These include pre-subscription overrides and
lazy configured-source schedules, including retained unsubscribed sources. No
policy eviction occurs. Invalid updates or exhausted capacity leave prior state
unchanged. Lazy policy exhaustion rejects the frame observably, never falls back
to all-frame capture. The unconfigured all-frame path needs no policy entry.
`sampled_out_frames` counts policy/timing and pre-conversion invalidation drops,
separately from overload/rejection counts. Native upstream/decoder storage remains
outside the adapter's budget.

Callback/tool-bearing image requests, cached replay and public video lifecycle
callbacks remain deferred. Pinned Python retains successful request callbacks downstream; the separate shared
MediaInput completion-on-admission contract is not substituted here. No full Python request/callback, SFU, RTP/codec/device, or remote lifecycle parity
is claimed.
Batch160 revision 3 passed 70 fresh library tests and strict Clippy/formatting.
Two integration tests and compile-only SFU evidence are historical revision 2
executions carried through a reviewed test-only change. See
[VALIDATION.md](VALIDATION.md) for source binding, failures and scope limits.


### Plain next-observation requests

Set `VideoInputConfig::requests = Some(ImageRequestConfig::default())` to let the
pipeline `LiveKitInput` consume upstream plain `UserImageRequest` frames whose
`transport_destination` exactly equals the configured destination (default
`None`). Nonmatching destinations and downstream requests pass through unchanged.
There is no connection/direct-receiver request API. Do not concurrently consume
`connection.recv()` and the transport input; direct receive never exposes a
requested image stripped of its private constraint.

**The contract is the next post-drain native sink observation, not a newly
captured camera image.** The sole reader drains its bounded SDK queue until it
actually observes `Pending`, after capturing an accepted-request cutoff. Only
requests at or below that cutoff become eligible for subsequent observations;
requests arriving during draining require another empty-queue fence. A frame
already dequeued cannot be rebound to a later request. Each drain poll does at
most eight frame removals and yields; a source preventing the fence expires its
request rather than manufacturing freshness. Draining discards old native entries
before conversion, including periodic candidates, without advancing cadence.
The fence establishes only native sink enqueue after request acceptance and its
required empty-queue observation, not when the underlying pixels were captured.
The SDK does not expose trustworthy capture/keepalive/replay provenance. Its
keepalive can have a new timestamp, and a sender can replay pixels. Such frames
can satisfy this observation contract. No timestamp comparison, physical camera
freshness, changed-pixel, RTP, remote-delivery or inference-completion claim is
made. No adapter pixel cache or cached-response replay is implemented.

The participant must exactly match a currently unmuted configured remote source.
Omitted/empty source normalizes to camera; only `camera` and `screenVideo` are
accepted. The response keeps the original plain nested payload, text and
`append_to_context`, with canonical transport source and the actual track SID.
`run_llm` remains `None`. Tool/function/callback fields and arbitrary request
metadata, incoming transport source and broadcast sibling IDs are rejected, not
stripped or executed. Identity/SID/routing/name limits are 256 bytes; text is
8 KiB; serialized header is at most 16 KiB. Spare String capacity is included in
retained header charging. Unsupported fields/oversize are request-local diagnostics.

Requests are bounded across waiting, selection, private queueing and retained
marked carriers: at most 32 global/8 per source, 4096 accepted-ID tombstones,
64 KiB aggregate conservatively charged headers, and 8 * 786432 bytes reserved RGB
by default. Limits may be lowered; RGB reservation can be raised only up to
32 * 786432 bytes. Each request reserves the full 786432-byte core RGB ceiling;
individual dimensions are at most 1024 and at most 262144 pixels. Larger frames
fail that selected request without resize/crop or changing ordinary video limits.
The ordinary RGBA permit covers conversion/compaction temporary storage while the
separate requested-RGB reservation follows the marked image through final carrier
drop. Arbitrary downstream clones allocate pixels outside adapter accounting.
A retained expired carrier still retains its slot/header/RGB permits until drop.
This is not a global heap bound over arbitrary processors, native decoders or SDK
queues. The reader-owned queue and all native upstream allocations retain their
existing separate limits and exclusions.

Default monotonic expiry is five seconds (positive, configurable to at most sixty).
One input-owner timer prunes stale/expired requests without needing another video
frame; expiry also runs before selection and dispatch. Mute/unsubscribe,
replacement, participant exit, reconnect and terminal close invalidate matching
uncommitted sources. End/Cancel invalidates all requests and discards private
queued images rather than draining them. Policies retain the existing lifecycle
rules. A valid selected observation bypasses/reset sampling cadence; later
conversion/queue rejection consumes that opportunity but never consumes a second
request. Rejected admission, expiry-before-selection and stale-reader observations
do not advance request cadence.

A request owner is moved once into selection, then consumed by a single binding.
Private `NativeUserImageSource` constraints and the original invocation fence
survive queueing and frame clones; public IDs/metadata are not provenance. Source
closure and ordinary assistant history commit are serialized by the core source
constraint. Closure-first prevents a new compatible context commit; a commit that
already won cannot be undone. No transport lock spans routing, runtime admission
wakeups or source-close notifications. The local routing-attempt counter is
`request_dispatch_attempts`: even an `Ok` routing result can be a stale drop and
is not evidence of enqueue, consumption or remote success. `DispatchAttempted`
means neither `Delivered` nor `Succeeded`. `failed_requests` counts local
accepted-request rejection or invalidation, not end-to-end failure. A retained
request may expire or be invalidated after a downstream context commit already
won, increasing this counter without undoing that commit. Admission rejections
are separately counted by `rejected_requests`. Accepted IDs are never evicted
to allow replay. Failures use bounded counters/last-rejection text, not
callback invocation, success events or fabricated failure frames; unrelated audio
continues.

The supported active consumer is the existing ordinary idle
`LLMAssistantAggregator`, which can append RGB context and request inference when
`append_to_context` is true. Passive requests do not append context. Busy/pending
turn, signed-history and realtime restrictions remain intact. Realtime passive
video and Gemini pixel sending are not equivalent request-context fulfillment.
Already dispatched raw sinks are outside recall; compatible constrained context
consumers enforce the stronger commit-time source gate. The local-native tests
exercise the actual pipeline input, SDK source/stream and ordinary assistant,
plus deterministic cutoff/drain, expiry, retention and rejection cases. Batch161
R2 passed 90 library tests and two integration tests, all freshly executed on the
same source with zero ignored/filtered and independently accepted. The integration
scope is synthetic signaling ownership and real loopback WebSocket teardown,
not joined-room media. Strict package Clippy/formatting and SFU compile/link-only
validation also passed and were independently accepted; the latter executes zero
tests. See [VALIDATION.md](VALIDATION.md) for retained failures and source binding. No SFU/device
runtime, remote-network media or physical-camera freshness is established.
