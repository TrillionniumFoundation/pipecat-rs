# Native spoken-response evaluation

The `bot-audio-transcription` feature adds an explicit library path for evaluating
actual bot PCM. `EvalSession::with_bot_transcription` takes a typed
`ScriptScenario`, `SessionOptions`, and a session-owned `BotTranscriptionConfig`.
The scenario's `response` events are produced from completed locally aggregated
STT transcripts. They are distinct from `llm_response` (reported LLM text).

This path uses the existing native Silero ONNX analyzer, OpenAI-compatible
`SpeechClient` multipart transcription, and `LLMUserAggregator` state machine.
The caller supplies the model/runtime and a bounded `SpeechConfig`; no model,
credentials or service is automatically provisioned. Live recognition quality
and provider entitlement require separate qualification.

## Explicit, default-compatible entry point

Build with `--features bot-audio-transcription`. Construct a typed scenario with
`response` text predicates, then call `with_bot_transcription(...).run(cancel)`.
The ordinary constructors still reject a literal typed `response` event. Existing
file loaders keep their text-mode `response` to `llm_response` alias; the CLI and
hosted-suite configuration do not automatically select this feature. Classifier
`eval` predicates are not accepted by this initial audio-response constructor.

`BotTranscriptionConfig::new` consumes a `SileroVadAnalyzer`, `SpeechConfig`, and
`TranscriptionRequest`. The analyzer must use 16,000 Hz and `stop_secs = 0.4`.
Received PCM must be mono little-endian PCM16 at 16,000 Hz. Other rates fail;
there is no implicit relabelling or resampling. The HTTP configuration must have
nonzero limits with timeout at most 30 seconds, WAV/audio at most 2 MiB, response
JSON at most 64 KiB, and queue capacity at most 32. The normal broad speech-client
default limits must be tightened explicitly for this adapter.

The endpoint must support captured bot audio, such as `EvalBotHost` with bot-audio
capture enabled. The session requests `capture_bot_audio=true`, omits `skip_tts`,
and requests audio responses for text input. Default text sessions do not spawn
an analyzer or change their wire settings.

## Completion and interruption

A single owned native thread feeds 40 ms PCM blocks to the real analyzer. A short
tail is zero-padded and an empty input queue supplies silence. Late scheduling
starts the next tick from the actual tick time rather than emitting missed ticks
in a burst. Only locally detected speech transitions close an STT segment.
Remote `bot-stopped-speaking` and TTS text are observations, never proof that all
PCM arrived: system messages can overtake media frames.

Closed segments are transcribed serially. Local starts and actual final transcript
text enter the existing user aggregator. The latest local stop is held until all
closed-segment HTTP operations have completed, including empty transcripts.
Only a nonempty `UserTurnStopped` result becomes a `response` event. Empty
recognition does not synthesize text or falsely satisfy a response assertion.

The event pump continues reading the socket while recognition is pending.
Expectation and session deadlines remain active. Ordinary user sends wait for
local PCM, VAD, HTTP and aggregate-turn accounting to settle; exhausting that
wait fails the evaluation. An explicit `send_after` boundary can interrupt this
wait. At the existing input boundary, the adapter advances one shared epoch,
clears old buffered audio and aggregation, and cancels/drops the old HTTP future.
Thread events and recognition results carry the epoch through publication.
A fresh reported LLM start is required before new reply PCM is admitted.

Epochs protect locally admitted work and delayed asynchronous results. They do
not authenticate a server's audio: old audio first delivered over the network
after a fresh LLM start cannot be distinguished from new PCM without a remote
identity protocol. A transcript is evidence of the received, locally segmented
audio, not a guarantee of complete synthesis or playback.

## Bounds and ownership

The session retains its existing capture admission bounds: 8 MiB and 4,096 chunks
for the whole session. The adapter additionally bounds total PCM, queued PCM,
segment duration, queued recognition work and transcript/context text. Defaults
are 8 MiB total accepted PCM, 256 KiB buffered PCM, 30 seconds per segment,
4 pending segments in addition to one HTTP request, and 64 KiB transcript text
per epoch. Each wire PCM frame is at most one second. Builder limits may tighten
these budgets within documented hard validation ceilings. The capture and
segmentation buffers are separate bounded copies; an interruption does not
refund the lifetime input admission budget. Queue or budget exhaustion fails
explicitly rather than silently clipping speech.

There is no detached transcription task. The session owns the HTTP future and
one native-analysis thread. Normal completion, failure and cancellation stop
feed, revoke HTTP work, and actually join that thread. Dropping the run future
also drops these owners. A 40 ms audio window limits inference work per call;
it is not a hard execution-time deadline for the native runtime. Model/runtime
stalls can therefore delay the synchronous final join.

The `audio_cleanup_error` result kind reports a failure returned during engine
finish. It can repeat an already reported worker failure; the kind alone does
not establish that thread joining failed. Thread joining and HTTP cancellation
are independently exercised by the ownership tests.

## Fixed-source comparison

The reference is `ProfHepta/pipecat` at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`. Its evaluation client uses paced audio,
STT segment accounting, a speech gate, and completed user-aggregator turns to
produce `response`; neither reported LLM text nor TTS text is a substitute.

Explicit native adaptations in this slice:

- Only 16 kHz mono input is accepted; the source serializer resamples wider input.
- Native uses a 600 ms speech-timeout aggregation policy. The pinned source's
  initial default is LocalSmartTurnAnalyzerV3 (subject to service strategy
  recommendations). This is not SmartTurn completion parity.
- Native retains a bounded 300 ms preroll and the real silence confirming the
  VAD stop. It does not append the source segmented-STT service's extra 0.5 s
  upload padding.
- Every local input epoch resets stale queued work and aggregation. This is a
  deliberately stronger local boundary than the source's interrupting-send
  timestamp gate, and cannot identify remote PCM that first arrives late.
- Hard input/queue/request bounds and explicit overflow failure are native
  safety limits.

Local tests must exercise real Silero inference and inspect the actual multipart
WAV before a deterministic HTTP peer returns text. That establishes model and
protocol wiring, generation fences and resource ownership; a scripted HTTP peer
is not a test of a production speech recognizer's acoustic accuracy.
