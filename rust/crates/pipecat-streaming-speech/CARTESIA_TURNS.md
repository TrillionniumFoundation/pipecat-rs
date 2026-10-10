# Cartesia Turns: bounded committed-turn adapter

This adapter implements the pinned Cartesia v2 Turns source contract from
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, separately from Live STT and TTS.
The native base is `edae99e6741e8675376497f8736e49e8d4203926`.
It does not establish live-provider compatibility, recognition quality, provider
billing, SDK/callback compatibility, or complete Python feature parity.

## API and wire behavior

Construct `CartesiaTurnsConfig::new(api_key)`, then `CartesiaTurnsClient::new`.
The default exact endpoint is `wss://api.cartesia.ai/stt/turns/websocket`, model
`ink-2`, mono PCM16LE at 16 kHz, and version header `2026-03-01`. The query has no
language field. Constructor model overrides are allowed; remote support is not
promised. Keyterms use source Python-compatible whitespace trimming, repeated
RFC3986 quoting, and first-overflow 100-term/1200-Unicode-scalar limits, without
Live's model-prefix eligibility restriction.

`connect()` succeeds only after JSON `connected`; HTTP upgrade alone admits no
sender. `request_id()` and `connected_result()` retain bounded acknowledgement
metadata. Unknown message types are ignored. A known turn event before readiness,
a duplicate acknowledgement, malformed JSON/type/transcript, binary provider data
or provider error terminates with a sanitized error. These fail-closed rules are
stricter than source tolerance; no undocumented turn-ID correlation is imposed.
Debug output for config/events omits credentials, endpoint and transcript content.

Sender audio is raw binary, one continuous stream, without VAD segmentation.
Empty audio produces no wire frame. PCM must be complete mono samples at the fixed
configured rate and fit the message cap. The owned PCM Vec capacity must also
fit that cap; a small payload in an oversized backing allocation is rejected
before queue admission, rather than retaining caller spare capacity. `finish()` admits one JSON `{"type":"close"}`
behind already accepted audio; the owner drains `next()` to None. There is no
`finalize` command or literal `done`. Active unexpected close is an error. Cancel
and dropped consumers cancel out-of-band, discard queued results and close within
a bounded cleanup interval. Sender clones cannot keep a dropped consumer alive.

Raw session events preserve cumulative transcripts and the provider JSON result.
Start, update, eager-end, resume and end are distinct. A `turn.update` carrying
`is_final=true` remains an update. Missing transcript means empty; whitespace is
not trimmed. Only nonempty committed end takes accumulated successful PCM usage;
empty end leaves usage for a later nonempty end or successful graceful drain.

## Processor and turn ownership

`CartesiaTurnsProcessor` fixes the speaker with `with_user_id`, controls input
passthrough with `with_audio_passthrough`, and recommends External strategies with
`with_should_interrupt`. Start precedes metadata. TTFS P99 is zero; metadata and
start/stop proposals use fresh linked broadcast siblings. Targeted metadata
requests replay the recommendation. Explicit aggregator policies still win.

Start is a system-priority proposal. The processor awaits the existing bounded
pipeline flush roundtrip before admitting subsequent turn outputs, so an External
start's interruption does not purge its immediately following transcript. The
aggregator decides interruption; the service never emits its own duplicate.
Nonempty committed end publishes usage, finalized=true English transcription,
then a normal-priority stop proposal. Interim text is cumulative preview. Eager
and resume remain raw-session observations only. There is no eager-enabled API;
`enable_eager_end_of_turn` settings updates are rejected until a separate eager
integration gate is qualified.

An empty end emits stop without an empty final/user message. The default External
stop strategy waits for final text; an empty turn may therefore close through the
aggregator's existing five-second outer watchdog, not immediately. The adapter
never fabricates empty final text to force completion.

The actor, readiness result, PCM admission, each output segment and errors are
fenced by socket serial, raw health generation and owner lifecycle. Each new PCM
call and explicit restart additionally needs its current raw invocation. Keeping
the lifecycle owner separate lets normal microphone listening survive ordinary
interruption without granting stale calls fresh authority. Already published
segments cannot be retracted; retirement prevents later segments, not rollback.
An already-started remote write cannot be made atomically reversible by local
generation fencing; queued work is checked again before each wire write.

Runtime model/language changes, unknown fields, wrong types, nonfinite/out-of-range
thresholds and invalid compound updates are rejected transactionally. Optional
null controls/keyterms clear; absent fields remain unchanged. Effective updates
while connecting/active require `restart_with_settings` and the live owning
context. Candidate validation precedes destructive retirement. Exact no-ops do
not reconnect or restore health; normalization-equivalent keyterm changes return
an explicit no-effect error. Failed replacement leaves no usable sender and keeps
the desired snapshot inspectable. There is no hidden reconnect, queued audio
replay, automatic health restoration or speaker reassignment. Mute retires the
socket; unhealthy unmute is rejected before changing its mute state. After
explicit health restoration, a repeated unmute can open a fresh socket and must
await connected acknowledgement. Health transitions never open a socket.

## Resource and accounting boundary

Defaults and hard admission bounds:

- Eight command slots and eight event slots; wire messages at most 32 KiB
- At most 64 slots, messages between 64 bytes and 1 MiB, and the checked conservative
  `(2 * queue_capacity + 4) * max_message_bytes * 128 <= 128 MiB` envelope
- The 128 multiplier budgets JSON tree, cumulative text and transient copies;
  the default estimate is 80 MiB (serialized payload envelope 640 KiB). This is
  conservative admission accounting, not a measured allocator/high-water result.
  Caller preallocation before admission and configuration-owned strings are
  outside this queue estimate
- Upgrade plus application acknowledgement: 10 seconds total by default
- Each write/event-queue admission: 10 seconds; close/drain: 2 seconds;
  bounded WebSocket cleanup: 2 seconds. Every configured limit is positive and
  at most 60 seconds. Drain includes JSON close and final event publication
- One socket actor and one processor reader task, one bounded start-barrier wait;
  no independent detached watchdog. Inactive direct sessions do not poll

The watchdog arms only after real successfully written audio and `turn.start`.
It schedules at `max(2 * last_real_chunk_duration, minimum_timeout) + 100ms` after
the previous successful send, with minimum timeout 0.5 seconds. This deadline
policy can be up to 100ms later than the source's 100ms polling observation and
never sends at or before the strict threshold. Real audio resets the real chunk
duration; silence resets send time only. Eager/resume do not disarm it. End, mute,
Cancel, retirement and admitted graceful finish stop silence admission.

Catch-up uses floor(elapsed seconds * sample rate), checked integer arithmetic,
no more than 10 seconds elapsed per catch-up by default (hard maximum 30), at most
64 bounded PCM chunks and one overall write deadline. Successfully written silence
is capped at 30 seconds per socket by default (hard maximum 300), never reset by a
new turn. Exceeding either budget terminates with an explicit error, without
inventing final text or a successful turn. Long real chunks whose next elapsed
gap exceeds the catch-up allowance also terminate; owners must size the allowance
for their input cadence. Cancellation/finish is checked between generated chunks.

Usage counts successful real and watchdog PCM writes, not queued/attempted bytes
or a provider receipt. It is emitted incrementally before nonempty finals and
once for trailing samples after successful graceful peer close. Canceled or
retired generation metrics can be discarded. This intentionally differs from the
source's attempted/submitted duration, retry/replay buffering and cancellation
flush. Live STT's keepalive accounting remains unchanged.

Other exclusions: arbitrary extra headers, dynamic source speaker changes,
resampling, generic idle keepalive, automatic source reconnect/replay, tracing
and callback API parity. Runtime tests use literal in-process loopback fixtures;
no credentials, paid provider, browser, WebRTC, LiveKit or model inference.
