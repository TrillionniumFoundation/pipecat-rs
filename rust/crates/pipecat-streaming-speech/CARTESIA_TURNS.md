# Cartesia Turns: bounded committed and opt-in eager turns

This adapter implements the pinned Cartesia v2 Turns source contract from
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, separately from Live STT and TTS.
The committed-only native base is `edae99e6741e8675376497f8736e49e8d4203926`.
Committed-only processing is the default; source-bound eager processing requires
the explicit feature and topology described below.
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
`with_should_interrupt`. Pipeline Start precedes metadata, which reports TTFS P99
as zero. Metadata uses fresh linked broadcast siblings, as do start/stop proposals
in committed-only mode. Eager-mode source-bearing proposals are downstream-only.
Targeted metadata requests replay the recommendation; metadata does not override
explicit aggregator policies.

Start is a system-priority proposal. The processor awaits the existing bounded
pipeline flush roundtrip before admitting subsequent turn outputs, so an External
start's interruption does not purge its immediately following transcript. The
aggregator decides interruption; the service never emits its own duplicate.
Nonempty committed end publishes usage (when enabled), finalized=true English
transcription, then a normal-priority stop proposal. In the default committed-only
mode, interim text is cumulative preview, while eager and resume remain raw-session
observations only. The optional eager mode below changes that projection.
`enable_eager_end_of_turn` runtime settings updates remain rejected; select
eager behavior through the processor constructor API.

An empty end emits stop without an empty final/user message. The default External
stop strategy waits for final text; an empty turn may therefore close through the
aggregator's existing five-second outer watchdog, not immediately. The adapter
never fabricates empty final text to force completion.

## Opt-in source-bound eager processing

Enable the non-default `pipecat-streaming-speech` feature
`cartesia-turns-eager` and construct
`CartesiaTurnsProcessor::new(client).with_eager_turns()`. Also set
`UserAggregatorConfig.eager` to `Some(EagerUserTurnConfig::default())` (or a
custom bounded configuration) on the native user aggregator and use the supported
`pipecat-services::LlmProcessor` topology described in
[EAGER.md](../pipecat-turns/EAGER.md). The feature alone does not enable the
processor, and the processor does not configure its consumers.
Responses, realtime and separate provider-specific inference processors are
outside this integration.

Each eager-enabled processor socket owns a local source-session authority. The
adapter settles the Start flush barrier before mapping predictions; downstream
adoption requires the owning consumer's exact Start claim. Mapped eager-end,
resume, final and stop carry exact local turn/prediction/commit receipts. A
matching final plus the matching stop claim can confirm the existing speculative
inference; held output still requires the downstream LLM stop boundary.

Destructive source retirement, replacement and cancellation revoke untransferred
rights. Active-session health-generation changes also deny stale source admission.
Successful confirmation transfers authority to the speculation owner: later
source-only revocation or source-health changes do not cancel transferred output.
The downstream owner's lifecycle, LLM output gate and native tool/flow effect
admission still apply. Buffering alone is not an external-effect safety boundary;
local cancellation cannot undo an external effect from an already admitted poll.

Successful normal reader completion at EOF preserves already-offered final/stop
receipt rights still held by native queues or consumer stages, while retiring
predictions without such a final. Adapter bookkeeping alone cannot preserve them.
Later explicit retirement can still revoke unconsumed source rights; it cannot
roll back transferred speculation or an ordinary final already consumed by the
aggregator.

In eager mode, the processor suppresses its interim-transcript output and raw
upstream proposed-speaking events. Applications lose this adapter's incremental
transcript/UI previews; ordinary speaking events admitted by the consumer remain
available. RTVI suppresses unconsumed source-bearing frames rather than stripping
their authority into a client event. No replacement interim stream is synthesized.

These identities establish local ownership, not authenticated provider turn
identity. Provider messages contain no authenticated turn key: a delayed event
on the same socket that still fits the current event sequence cannot always be
distinguished from a current event. Cross-socket retirement does not remove that
same-socket ambiguity. Automatic reconnect/replay is still absent.

Scoped native contracts are recorded in `tests/cartesia_turns_eager.rs`, its
reviewer modules and the source-aware core/turn/service tests. They do not clear
the full-workspace release gate, which remains blocked. Live-provider, browser,
physical-device and platform qualification remain separate; WebRTC work remains
stopped under the existing platform restriction.

## Session lifecycle and settings

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
