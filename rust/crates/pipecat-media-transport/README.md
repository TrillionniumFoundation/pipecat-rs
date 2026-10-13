# Shared native media transports

`InputTransport` and `OutputTransport` are real `pipecat-core::FrameProcessor`
implementations. `MediaTransport` pairs them without performing I/O at
construction. They do not depend on the WebSocket, WebRTC, realtime-provider,
CLI playback, or Python runtimes.

## Backend contract

Applications implement `InputBackend` and/or `OutputBackend`. There is no
pretend default device. Input `start` opens the requested PCM format; `capture`
runs as a pipeline-owned task and feeds a bounded `InputIngress`. Output `start`
opens the format and registers all configured destinations before the processor
publishes `OutputTransportReady` upstream. A failed setup cannot report ready.

`OutputBackend::play_audio` returns `PlaybackReceipt::Played` only at the
backend's **actual playback acknowledgement boundary**. A successful socket
send or acceptance into a device queue is insufficient. Buffered backends must
wait for an acknowledgement or a trustworthy local playback clock. A deliberate
drop returns `Discarded`; it produces neither downstream audio nor speaking
notifications. Backends unable to determine playback completion must return an
error instead of fabricating a receipt. Network adapters can implement a weaker
queue-acceptance API separately, but cannot label it `Played` here.

Dropping an in-flight backend future must cancel unacknowledged playback;
otherwise the mandatory `interrupt(destination)` method must flush it. The
runtime cancels interruptible writes on interruptions, then awaits this flush
before delivering newer audio. A backend must not detach tasks or retain audio
that can later replay after cancellation. Capture tasks follow the same ownership
contract. Device/connection shutdown is bounded and attempted during cleanup,
including partial startup failure.

## Output flow

- Each destination has an independent bounded media queue and owned worker
- Frame-count and retained byte limits apply bounded normal-lane backpressure;
  oversized frames, admission timeouts and explicit fail-fast overload report errors
- PCM is validated, channel counts are preserved, and rate conversion uses the
  native streaming sinc resampler, including its final filter tail
- Arbitrary audio packets are reblocked into channel-aligned output chunks;
  TTS stop and End pad a final partial chunk with silence
- A chunk containing any uninterruptible samples is uninterruptible; a chunk's
  source frame type, context, metadata and destination are retained
- The default destination's filter runs first, then volume × gain, then its mixer
- Filter/mixer controls and volume/gain changes run in audio order; a control
  flushes the preceding partial PCM before changing state
- Downstream mixer controls are consumed by the selected output destination,
  including destinations without a mixer; upstream controls pass through unchanged
- Filter-empty audio skips both the mixer and device write
- Generated background audio, end silence and DTMF bypass the speech filter and
  gain; generated background does not cause bot-speaking events
- Idle background/silence is paced at the chunk period and its worker survives
  interruptions; no tight spin is possible with an immediate test sink
- `OutputControl::pause` retains queued audio and resampler state; `resume`
  continues it without replay. Pausing takes effect between writes, so it does
  not abort a write already accepted by the backend
- Processor pause/resume frames addressed to this endpoint are also supported
- Interruptions discard queued/buffered interruptible PCM and reset its
  resampler history. Uninterruptible audio and gain controls remain queued
- TTS speaking boundaries are emitted only after played audio, and stop after
  TTS stop, interruption, idle timeout or End. Explicit speech-audio frames use
  the native PCM silence threshold; plain output/background audio is not speech
- All speaking events carry the destination and paired broadcast identities
- Timestamped non-audio frames wait relative to output-worker startup, preserve
  source order, and remain interruptible
- A stalled/failed destination is written off once, emits an upstream error,
  and cannot consume later writes; other destinations remain independent
- End resumes paused destination queues, drains admitted media, flushes tails
  and end silence, joins owned workers, and then forwards the terminal frame

`write_timeout` bounds each filter, mixer and backend operation. Pipeline
`shutdown_timeout` supplies the overall drain bound. Finite volume and gain can
exceed 1; PCM scaling saturates rather than wrapping. `OutputControl::is_usable`
reports the per-destination failure state separately from transport readiness.

Native keypad signalling is optional: `send_dtmf` returns false to select the
bundled native PCM16 tones. Urgent keypad frames enter ahead of waiting media,
after the currently in-flight operation; ordinary keypad frames preserve queue
order. Both paths are interruption-aware and do not claim the bot spoke.

### Urgent transport messages

After readiness, downstream `OutputTransportMessageUrgent` calls the configured
backend's `send_message` directly, preserving the original frame and consuming
it without downstream forwarding. The destination must be registered. Upstream
urgent messages retain native passthrough behavior; unlike the pinned Python
implementation, they do not invoke the backend.

The call runs in the processor's existing System invocation, with no detached
task or retry. It bypasses the media sender's pause, PCM backlog, timestamp waits
and playback fences. It may overlap audio/image/ordinary-message operations on
the same backend `Arc`; implementations must support that concurrency or provide
their own serialization. The queued urgent-DTMF and media-fence ordering rules
are unchanged. This control path cannot preempt an explicitly paused System lane
or force a busy backend/socket to make progress.

The destination queue's ending/cancelled admission cutoff still applies, even
between `prepare_end` and the later End/Stop process call. End/Stop drain already
admitted System writes within the existing shutdown bound. Audio interruption
epochs and a media sender's graceful exit do not cancel an admitted protocol
reply. Its cancellation signal belongs to OutputTransport and is set only by
actual Cancel or cleanup. `write_timeout` bounds each call;
pipeline cancellation/cleanup can drop its future. A Cancel queued in the same
System lane can wait for the current send's timeout. Dropping a future does not
undo a partially transmitted message or prove remote cancellation. Backends must
not detach work. Failures surface as processor errors with no success forwarding
and no retry; the existing audio-specific destination write-off policy is unchanged.

There is no new retained media queue or per-message task. Existing core System
admission remains count-bounded, not a payload-byte/RSS guarantee. Successful
`send_message` completion means only that the configured backend hook completed,
not that a remote Genesys session received or accepted the reply. Public facade
tests exercise real AudioHookSerializer frames against recording backends, not
live provider sessions or physical devices.

### Image output

`OutputImage`, `OutputImageDetailed`, `UrlImage`, and `AssistantImage` use the
selected destination's owned output worker. `write_image` receives the complete
typed frame; successful completion forwards that same frame downstream, including
assistant original bytes/MIME, routing, metadata and local observation identity.
Image writes share the existing interruption, cancellation, PTS and write-timeout
rules. They do not enter the audio filter, mixer, gain or speaking-event path.

Queued image admission counts retained pixel and format allocations, URL or
assistant original-data/MIME allocations, and the frame envelope and metadata,
including spare capacity. Nested metadata uses the existing bounded-depth walker.
This is retained queue accounting, not an RSS limit or a bound on a backend's
working allocation. Fixed image scalars are count-bounded. The backend owns
image decoding and format support; shared output does not decode assistant
original bytes or validate image geometry. A successful `write_image` is the
backend's completion boundary, not an independently verified physical display.

The pinned Python implementation schedules image output and separately queues
assistant images for aggregation. Native shared output retains its explicit
backend-success-before-forward boundary; these are not identical video-task
scheduling models. Tests use synthetic bytes and recording backends, not real
cameras, displays, image codecs or provider services.

## Flow playback fences

A downstream `Flush` or `FlushDetailed` with `origin = "pipecat.flows"` and
`returning = false` is an all-destination playback fence. Admission reserves one
bounded queue slot in every destination atomically. Full destinations wait under
the normal admission policy; closed queues or expired admission deadlines reject
the whole fence. `FailFast` rejects a full destination immediately.
Existing destination workers flush preceding resampler
and partial PCM tails, await backend playback/discard receipts, and rendezvous.
The last participant forwards the original frame exactly once, retaining its
identity, payload and metadata, before releasing later media. Later urgent
media cannot jump an accepted fence. No extra task is created per fence.

A paused destination retains the fence until resumed (or End drains it).
Interruption discards cancellable media using the normal backend flush contract
and retains the non-interruptible fence. Cancellation or a failed destination
worker releases the rendezvous without issuing a success receipt. Unknown
completion after backend failure, transformation failure or fence admission failure
is fatal: an upstream error identifies the numeric frame/Flow barrier, and the
pipeline stops rather than executing Flow post-actions with an invalid receipt.
Returning or upstream Flow markers pass through without re-waiting on later
media. Other origins and ordinary controls retain their existing behavior.

## Input flow

Captured audio and `InputAudio` / `UserAudio` frames injected in either pipeline
direction use the same bounded queue, PCM validation, native streaming resampling,
filter and optional downstream passthrough. Disabled or muted input is consumed.
The original audio family, user, context and envelope survive ordinary forwarding,
including local provenance and observation identity. Explicit input format
configuration overrides the pipeline's input format.

A continuous resampling stream is keyed by input rate, transport source, audio
family, user ID, audio context and destination. Changing a key drains the old
stream with its own envelope before accepting the new samples. An empty input
packet neither enters the filter nor seeds or replaces a pending tail. Tail frames
use fresh logical identities with the last nonempty admitted envelope; their
stored template retains no PCM allocation. The configured filter is shared:
`AudioFilter` has no flush/reset hook and may carry state across users.

Input admission charges retained PCM capacity, user/context/routing strings and
nested metadata, including spare capacities. Fixed frame headers and up to 64
bytes of generated name allocation use the count bound; additional name capacity
uses the byte bound. Compact plain PCM without optional fields therefore keeps
its exact-byte boundary. Metadata accounting is conservative and depth-bounded;
overflow or excessive depth rejects admission. Rejected frames consume no queue
credit. The resulting frame is checked again after filtering, before passthrough.
This native retained-allocation rule deliberately rejects optional envelope or
reserved allocations that a PCM-length-only limit would admit. It bounds the
counted owned input payload and envelope allocations, not process RSS: popped
frames, Sinc history, envelope templates,
filter internals/return allocation, temporary conversions and allocator overhead
are separate. A filter's allocation peak occurs before its result can be checked.

`InputTransportStartAudioStreaming` starts capture once; `stream_on_start` can
delay capture until this frame. The ingress reports readiness, rejects overload,
and supports mute/resume. Input mute drops newly arriving media; output pause
retains it. Enabled input images share bounded admission and forwarding.

These are native streaming semantics. The pinned Python shared input loop has no
resampler or VAD; it filters even empty input, uses an unbounded audio queue, and
cancels pending audio at End. Native input retains its End drain and serial queued
filter controls. With an input filter, `FilterUpdateSettings` flushes preceding
input PCM, invokes the bounded filter callback, and is consumed locally in either
direction. Without an input filter, settings retain their original frame and
direction through ordinary passthrough, without an input-tail flush. The native
`FilterControl` and `FilterEnable` extensions remain queued and forwarded after
flushing preceding input PCM. Python directly invokes only settings on its input
filter; native application filters support the broader control family. No
pipeline-global flush barrier or arbitrary stateful-filter user isolation is
implied by shared input dispatch.

Graceful End stops accepting capture, cancels and joins the capture task, drains
admitted input and resampler tails, and cleans up the filter/backend. Cancellation
aborts all owned media work without forwarding unacknowledged output.

### Video sources and next-image requests

`video::VideoInSamplers` owns typed `(VideoParticipantId, VideoSourceId)` keys.
Each source passes all images for `framerate: None`, only requested images for
zero (`VideoInSourceParams::on_request_only()`), or a positive sampling rate.
The first frame starts its schedule. Scheduled samples allow the smaller of
50 ms and half an interval of jitter, do not drift with early arrivals, and do
not burst to catch up after a stall. A request takes the next frame FIFO and
restarts the schedule. Rate updates keep requests; taking requests retains
samplers, whereas participant removal and clear return requests and remove them.
The sampler accepts caller-supplied monotonic durations; ingress uses Tokio's
monotonic clock. Unlike the pinned Python source's `time.time()`, wall-clock
adjustments cannot change native sampling decisions. Rates above nanosecond
clock resolution are explicitly rejected.

Set `InputTransportConfig::video_enabled` and optional `video.sources` policies.
The backend must explicitly support configured source names through
`InputBackend::supports_video_source`, open actual capture itself, and call
`InputIngress::video_participant_connected` before its connected callback.
The ingress exposes the configured policies; registration atomically installs
them with the backend's current availability. `capture_video_source` registers
or updates a manually opened source. These methods attest backend capture;
they do not create a camera, decode video, subscribe an SFU, or start screen
capture. A backend must report source-off with `set_video_source_available`,
participant departure with `remove_video_participant`, and connection loss with
`disconnect_video`. Returning from the owned capture task also disconnects video.

Feed native `InputImage` frames through `push_video_frame(key, frame)` for
sampling and request correlation. `push(InputImage)` and `push(UserImage)` with
`request=None` provide bounded, uncorrelated capture: when ready, video-enabled
and unpaused, they forward the original frame downstream without sampling or
reconstructing its identity, payload, metadata or routing. Disabled or paused
capture drops these images. Request-bearing `UserImage` frames are rejected by
unpaused generic ingress; use the keyed raw-image path for owned requests.
Ordinary pipeline `UserImage` processing remains direction-preserving passthrough.
Correlated keyed output is `UserImage`, preserving image
bytes and envelope metadata, identifying the participant and transport source,
and carrying the request's text, append-to-context flag and tool identifiers.
No extra copy is emitted for a scheduled frame that also answers a request.
Muted frames neither consume requests nor advance the sampling schedule.

`UserImageRequest` frames matching the configured exact `request_destination`
(default: no destination) are consumed by this input. Other destinations pass
through unchanged. `request_image` supports direct bounded native admission.
Unavailable or uncaptured sources complete admitted requests with a
source-specific error. Source-off and disconnect fail still-outstanding requests
and suppress their queued images. End fails requests still waiting for capture
and drains admitted images; Cancel and cleanup fail outstanding requests.
Sampling, queue admission and lifecycle transitions are serialized: rejected
media admission cannot consume a request, and shutdown cannot orphan one.
Private per-request provenance prevents a stale queued image from satisfying a
new request even when a backend reuses its public frame ID. A preceding audio
flush/filter failure also terminates the affected image request immediately.

This input owns completion: it invokes an admitted request's local callback
once, with JSON null for successful downstream admission or an error object.
The emitted request copy has no executable callback, preventing a downstream
consumer from invoking it again. Success certifies admission to the native
pipeline, not completion of vision analysis or a hardware/remote delivery.
This is an explicit native boundary; the Python source defers successful tool
callback invocation to its image aggregator. Callback execution is timeout
bounded and errors or panics do not skip other owned callbacks. A cancelled or
timed-out application callback is not retried; its external effects cannot be
rolled back. Callbacks must be cooperative and must not block the executor.

Default bounds are 128 captured source keys, 128 pending requests, 256 KiB of
pending request strings, and 4,096 lifetime request identities. Participant,
source, function, tool and destination identifiers are limited to 256 UTF-8
bytes. Frame IDs and supplied tool-call IDs have session-lifetime replay
tombstones; disconnect does not erase them, and exhaustion fails explicitly
instead of evicting protection. Duplicate, oversize, closed or full request
admission returns an error without taking callback ownership. Correlated
images, duplicated request text, formats, routing and metadata count against
the shared media byte budget. Fixed per-request bookkeeping, bounded error
objects, replay identifiers and executing callback batches have separate
count bounds; application-owned callback captures are not introspectable.
The native tests use synthetic RGB bytes, injected backend availability and a
controlled monotonic clock; they do not qualify a camera or video codec.

## Recording and mixing

`recording::AudioBufferProcessor` records synchronized user and bot PCM tracks.
Its clock is injectable (`RecordingClock`), the default is monotonic, and gaps
above 200 ms are padded at sample boundaries. Speaking guards avoid injecting
silence into an active utterance. A mono saturated mix or stereo interleave is
available, with independent track events. Start/stop controls are idempotent;
final audio precedes the stopped event. Recording buffers, resampler-tail
reservations, turn/preroll buffers, and broadcast event history are bounded.
Slow event consumers receive Tokio's explicit `Lagged` error.

With `enable_turn_audio=false`, generic pipeline `Stop` keeps the recorder active
and preserves its primary buffers without emitting final audio or a stopped event.
The stopped worker skips processor cleanup; an owner retaining the recorder must
call `stop_recording()` or `FrameProcessor::cleanup()` to flush and finish it.
Repeated explicit stop and cleanup do not duplicate final events. This does not
promise worker resume or recorder reattachment. Explicit recording-stop controls,
End and Cancel still flush and stop. Every `enable_turn_audio=true` configuration
keeps the existing Stop drain and terminal boundary, including manual numbering
and attached sessions whose attribution has already been disabled. This boundary
is independent of whether the `turn-recording` feature is compiled.

`RecordingEvent` includes recording lifecycle, mixed and track audio, speech-run
and numbered turn events. `emit_turn_audio(turn_number)` supports manual numbering;
recording stop retains pending turn audio for that call. The optional
`turn-recording` feature adds native automatic numbering via
`Arc<AudioBufferProcessor>::attach_turn_tracker(&tracker)` before startup. Install
the recorder and register that fresh `TurnTrackingObserver` in the same pipeline.
Subscribe to recording events before starting. One recorder accepts one attachment;
manual numbered emission cannot be mixed with attachment.

Automatic recording seals existing resampled PCM prefixes at native control-frame
boundaries, then joins them to tracker events using opaque local frame identity.
One bounded native subscription serializes every source control with its turn
events. The recorder verifies that exact control prefix against its local processing
order before emitting audio; reordered, missing or extra controls fail closed.
Delayed events can consume only the sealed prefix, never newer-turn audio. Clone
preserves identity; duplicate and wire decoding create new identities regardless
of public frame IDs. Tokens contain no audio and are omitted from tracker JSON.
Recording and observation clocks need not share an epoch. User interruption
pre-roll stays on the new side; bot PCM processed after the interruption or stop
is excluded from numbered speech until a fresh bot start. These are recordings
of processor-observed PCM, not device playback receipts.

Pending boundary cuts, local controls and tracker updates are independently capped by
`event_capacity`; all delayed turn PCM shares each track's `max_turn_bytes` limit.
Weak replay tombstones for live control-frame lineages have the same independent
count bound. They are pruned only after the last native Frame dies; applications
retaining many live controls can exhaust this bound and disable attribution.
Lag, observation loss, invalidation, ambiguous/repeated live boundaries, missing
native tokens, overflow, recording errors or restarting the recording permanently
disable attribution for that attachment. `TurnAttributionLost` reports the reason;
`turn_recording_status()` retains it even if a recording-event receiver lags.
Primary mixed/track recording continues. Turn-PCM budget exhaustion preserves
the triggering primary audio when its independent track budget permits it.
Resuming numbered recording requires a fresh pair; automatic events include a
process-local `session_id` so independently numbered sessions are distinguishable.
Manual emissions have no session ID.

The subscription weakly owns the recorder and aborts on Drop. Graceful cleanup
waits up to 100 ms for accepted tracker events; cancellation, unfinished boundaries
or a slower tracker discard unattributable turn audio. Terminal events may arrive
before their recorder control frame and wait for that exact local boundary.

`mixer::SoundfileMixer` loads native PCM16 mono WAV files with bounded file count
and size. Load failures (missing files, wrong rate/channels/encoding) are available
through `load_statuses`; unavailable sounds pass through rather than impersonate
successful decoding. Enable, selection, volume and loop controls are atomic;
unknown settings fail explicitly. Mixing clips after float scaling and addition.
Like the source mixer, a loop restarts at the beginning when an entire requested
chunk does not fit the remainder. Oversized chunks wrap safely to preserve length.

## Explicit compatibility boundaries

- No OS microphone, speaker, camera, screen-capture, Daily, LiveKit or other
  provider adapter is bundled. Implementing a trait is not evidence a device was
  exercised; all device tests use explicitly identified deterministic backends
- Raw PCM16 is supported; there is no implicit channel remix. Compressed audio
  decoding and WAV formats other than PCM16 are not implemented here
- Input images can be sampled by participant/source and answer next-image
  requests; output images can be handed to an explicit backend. Actual capture,
  resizing, sprite cycling, video decoding and audio/video hardware clocks
  remain backend-specific gaps. Unsupported sprite output returns an error
- Audio timestamps are preserved as metadata; they do not reschedule speech
  ahead of previous audio. Non-audio PTS scheduling is ordered, not a timestamp
  sorting heap
- Python worker callbacks are not used. Native tracker attachment is opt-in via
  `turn-recording`; builds without it retain manual numbered emission
- Filters/mixers receive native frame controls, not arbitrary Python plugin
  objects. Stateful third-party implementations must obey cancellation and
  nonblocking async contracts
- End drains with bounded timeouts. Explicit core processor pause frames also
  pause that processor's normal lane; send its resume frame before trying to end
  a core-paused pipeline. `OutputControl::pause` does not pause the core lane,
  and therefore End can automatically drain it

## Verification

Run `cargo test --locked --offline -p pipecat-media-transport` and
`cargo clippy --locked --offline -p pipecat-media-transport --all-targets -- -D warnings`.
Repeat with `--features turn-recording` to exercise automatic numbered recording.
Contract tests exercise actual native pipelines and explicit sinks/capture sources,
plus deterministic recording clocks and WAV fixtures. They cover ordering, tails,
interruptions, queue limits, pauses, ownership, acknowledgement truthfulness,
stalled peers, destination routing, End draining, mixing and track synchronization.

## Opt-in whole-context spoken history

Default output behavior is unchanged. To connect receipt-backed playback to
assistant history, create one `SpokenHistory` per session, install its
`source_boundary()` before asynchronous TTS, and use
`OutputTransport::with_spoken_history()` after TTS. The assistant aggregator goes
after output:

```text
LLM -> SpokenSourceBoundary -> TTS -> OutputTransport -> assistant (Spoken)
```

The TTS processor must preserve the original LLM response start/end frames,
including their identities and metadata. Native streaming Cartesia does this.
The source boundary records and stamps those identities so a delayed response
end cannot certify a different synthesis context. Replacing those frames,
removing their metadata, or omitting the source boundary withholds history.

A `SpokenHistorySink` performs the actual history mutation synchronously. Enable
`assistant-history` to preserve assistant completion records as well as frames:

```rust
use std::sync::Arc;
use pipecat_media_transport::{AssistantSpokenHistorySink, SpokenHistory, SpokenHistoryConfig};
use pipecat_turns::LLMAssistantAggregator;

fn history(assistant: Arc<LLMAssistantAggregator>) -> SpokenHistory {
    SpokenHistory::new(
        SpokenHistoryConfig::default(),
        Arc::new(AssistantSpokenHistorySink::new(assistant)),
    ).expect("valid history bounds")
}
```

The facade exposes the same opt-in `assistant-history` feature. Without it,
standalone media retains its frame-only normal dependency boundary. Existing
custom sinks keep working, but returning only Push frames does not deliver an
assistant's registered completion stream. `commit_ordered` preserves the exact
frame/record order and runs notifications after releasing the ledger lock.

Construct that assistant in `AssistantTextMode::Spoken`. The callback commits
under the ledger's lock; returned effects are only follow-on notifications. Do
not defer mutation to a returned normal-lane frame, reenter the ledger from the
callback, or also submit the context's text as `spoken=true`. Opted-in output
removes upstream `spoken` metadata from TTS text.

Keep a clone of the history handle in the input/control path. Call
`history.invalidate()` **before** admitting an interruption or cancellation into
the pipeline. This is the linearization point against a synchronous commit:
either a fully verified context commits first, or invalidation prevents it.
Output and source processors also invalidate when they receive interruption,
but cannot observe a control frame still queued elsewhere. Internal sources
that broadcast interruption must use the same immediate hook to obtain this
strong ordering guarantee.

### Evidence and limits

A context is submitted to the sink only when all of these hold:

- One noninterleaved, nonempty context ID matches TTS start, text, PCM, and stop
- The original bound source response ends successfully after its final TTS stop
- Every accepted source PCM frame reaches the serialized media worker
- Resampling, chunking, padding, and every resulting PCM write finish, with
  explicit `Played` receipts and no discarded, failed, muted, or unknown writes
- Text, frame, response, and lifetime replay-protection budgets remain valid

Input frame counts are not compared with backend write counts. One source frame
may produce several output writes; several source frames may form one padded
write. A single discarded write invalidates the whole context. There is no
word-level playback claim, and partial playback commits no text.

Only one output destination is supported. Custom filters and mixers are rejected
at opt-in construction because arbitrary transforms cannot prove preservation of
speech. The built-in resampler, chunker, padding, and nonzero gain are supported.
These restrictions do not apply to output without spoken-history opt-in.

Standalone TTS without an enclosing bound successful LLM response, failed or
incomplete responses, interleaved contexts, duplicate frames, and late/reused IDs
remain uncommitted. They do not stop ordinary audio playback. Inspect
`history.status()` for rejection reasons and bounded pending state.

Default bounds are 16 pending source/output responses, 4,096 lifetime context
IDs of at most 256 bytes, 8,192 lifetime source-frame identities, 1 MiB of
buffered text strings, 8,192 evidence frames per response, and 16 follow-on frames per callback. The callback is application
code and must keep its own effects bounded and mutation atomic. Context-ID,
source-identity, or response-capacity exhaustion disables attestation for the ledger's remaining
lifetime. Tombstones are never silently evicted; interruption does not reset
replay protection. Create a new ledger only for a genuinely new session.

The native integration tests use synthetic explicit backend receipts and the
real Spoken assistant aggregator. The Cartesia cross-crate tests run the actual
native CartesiaTtsProcessor against a real loopback WebSocket carrying synthetic
provider events. They verify original LLM marker identity/metadata preservation,
matching TTS contexts and stop/end order, successful completion gated on the
last receipt and original response end, provider failure after partial PCM, and
interruption before the last receipt. The additional pipeline tests cover
source-generation fencing,
resampling/coalescing/padded tails, last-receipt gating, failed source completion,
discard and timeout, immediate and fallback interruption, cancellation, replay
and buffer bounds, and both sides of the commit/invalidation race. They do not
claim a hardware-device or live-provider acceptance test.

### Explicit pipeline-clock timing

Frames marked with `Frame::set_pipeline_pts` use the shared observer nanosecond
timeline. At admission the output pairs observer-clock now with monotonic Tokio
now and fixes the resulting deadline; custom deterministic observer clocks do
not need to wake timers. Nested pipelines inheriting observers share that clock.
Each destination records its own observer/Instant origin when its sender starts.

Future anchor, word, progress and alignment-diagnostic frames stay in the same
bounded queue and retain their count/byte reservations. Only PCM may pass that
metadata; stop, response-end and flush-fence frames remain barriers. Pause retains
the lane, interruption discards its interruptible frames, and graceful shutdown
waits for admitted timing work. Diagnostics do not produce playback receipts or
spoken-history writes. The existing independent Played-receipt requirements are
unchanged.

After the deadline, marked PTS is explicitly rebased to destination-relative time
before forwarding/backend access. Pre-origin overdue timestamps become zero;
checked arithmetic prevents wraparound. Unmarked legacy PTS retains its existing
destination-relative interpretation. For queued media, both forms reject future delays beyond
`max_future_pts` (60 seconds by default) before queue admission, so an untrusted
wire timestamp cannot indefinitely hold a graceful terminal. This extreme-future
rejection is deliberate hardening; ordinary legacy wire fixtures are unchanged.
Direct urgent transport-message dispatch preserves PTS unchanged and does not
schedule or rebase it; the serializer's independent clock-origin contract still applies.
Direct serialization of pipeline-clock-marked frames fails; callers bypassing
output must explicitly rebase them rather than silently lose their clock origin.

## Bounded output admission

`OutputTransportConfig::admission_policy` defaults to
`OutputAdmissionPolicy::Wait { timeout: Duration::from_secs(10) }`. A normal
frame waits for its destination's existing count and byte budgets rather than
discarding PCM when synthesis runs ahead of playback. Only the current
normal-lane invocation retains a waiting frame; there is no additional relay
queue. Frames larger than the destination's byte budget fail immediately.

The deadline applies to one admission attempt and includes time spent paused
or waiting for future-PTS metadata. Notifications do not extend it. A timeout
reports a recoverable admission error and invalidates pending spoken-history
attestation. Applications requiring immediate overload reporting can select
`OutputAdmissionPolicy::FailFast`. Queue budgets and backend `write_timeout`
are configured independently of admission timeout.

Interruption discards a waiting interruptible frame; uninterruptible frames
survive. Cancel or closing the queue releases waiters immediately. Pause keeps
admitted media and resume permits playback to release capacity. End and Stop
remain ordered barriers: preceding admission attempts complete or time out,
then the admitted media drains, including when output is paused. Choose the
admission timeout and pipeline shutdown timeout for the application's maximum
queued work and latency requirements.

Normal compact and detailed `pipecat.flows` flush markers use the same policy.
They acquire room in every destination atomically, without publishing partial
fences. A closed destination, oversized marker or deadline expiration fails
the fence. Urgent/system controls admitted to the media queue, including urgent
DTMF, retain nonwaiting admission and cannot jump an admitted fence. Direct
urgent transport-message dispatch is separate from this playback ordering.

Flush metadata keys, JSON values, container storage and spare capacities are
charged conservatively, as are routing strings, nonstandard origins and excess
frame-name storage. Empty controls retain a count-bounded fixed-header allowance
(64 name bytes and 16 bytes for the standard Flow origin) so they can drain a
full media-payload budget. JSON nesting beyond 128 levels fails closed. An
explicitly interruptible Flush waiting for capacity is discarded atomically if
any destination's interruption epoch advances.

Timed metadata keeps its count and byte reservation until released. PCM may
pass future metadata only where the queue's clock-lane ordering permits it.
This bypass applies only to admitted PCM: a producer that fills the entire
shared budget with future metadata can block later normal-lane PCM admission.
If that metadata becomes due after the admission deadline, waiting metadata
and PCM can time out. The wait policy does not guarantee lossless operation
for arbitrary future-metadata-first streams; applications must bound their
metadata lead as well as their PCM production.

Backend failure retains the existing one-error peer-disable behavior: the
sender drains and discards remaining audio, releasing queue capacity without
fabricating playback receipts. Only explicit `PlaybackReceipt::Played`
acknowledgements can emit played-audio events or attest spoken history.
