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
file loaders keep their text-mode `response` to `llm_response` alias. The CLI
selects spoken responses only with the explicit flag below, for both `run` and
compiled-child `suite`. Classifier
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

## Explicit CLI run configuration

With `bot-audio-transcription` enabled, use:

```sh
pipecat-eval run scenarios.yaml ws://127.0.0.1:8765 --bot-audio-response stt.json
```

A trusted-local JSON configuration explicitly chooses the endpoint, model,
credential environment variable and installed runtime/model files:

```json
{
  "base_url": "http://127.0.0.1:9000/v1",
  "model": "local-recognizer",
  "api_key_env": "LOCAL_STT_KEY",
  "onnx_runtime": "models/libonnxruntime.so",
  "silero_model": "models/silero_vad.onnx",
  "timeout_ms": 8000
}
```

The named environment variable must already exist. No credential is created,
printed or selected from a fallback variable. HTTPS endpoints and loopback HTTP
are supported under the existing SpeechClient URL policy; redirects are disabled.
Choosing a remote service explicitly sends received bot audio there and may incur
that service's charges. Tests use only local protocol fixtures and dummy keys.
`language` is optional; timeout is 1–30000 ms, default 8000. Unknown or duplicate
JSON fields are errors. The configuration is a regular file of at most 32 KiB.

Relative runtime/model paths resolve from the canonical configuration target
directory (including when the supplied configuration path is a symlink). Explicit
paths take precedence over runtime/model environment defaults. Runtime files are
bounded to 128 MiB and model files to 16 MiB. Assets are checked as regular files,
then the native loader reopens their canonical paths. This trusted-local path
policy is not a sandbox against concurrent directory or file replacement.
The Unix file loader refuses nonregular files without blocking on FIFOs.

All scenarios, options, recorded-input references and model instances are prepared
before the first WebSocket connection or HTTP request. The explicit mode permits
1–16 scenarios, checked before model instantiation, each with independent recurrent
state and a real preflight inference. Native loading/inference has no hard wall
clock deadline. ONNX runtime initialization is process-global: an already loaded
different runtime path is rejected, rather than switched per session. A bad later scenario or model fails the whole preparation before
any scenario runs. `--stop-bot` still requires one scenario.

Only this explicit loader preserves `expect.response` and `send_after.response`
as spoken transcript events. Explicit `llm_response` remains reported LLM text;
default loaders and CLI runs retain their prior alias and validation behavior.
Classifier predicates remain unsupported. `--recorded-audio` can be combined with
this mode, reusing its existing bounded WAV loader and outer-scenario-directory
media paths. `--bot-audio-wav` is mutually exclusive for `run`.
Cancellation, deadlines and old-generation cleanup use the existing transcription
session owners; this CLI entry does not change completion or acoustic guarantees.

## Explicit hosted suite configuration

```sh
pipecat-eval suite native-manifest.yaml --bot-audio-response stt.json
pipecat-eval suite native-manifest.yaml --recorded-audio --bot-audio-response stt.json
```

The feature-gated library entry is
`ReleaseManifest::load_audio_response(path, settings, recorded_audio)`, followed
by the existing `run_release` or `run_release_with_budget`. The settings use the
same explicit configuration described above. The compiled bot must enable bot
PCM capture; enabling STT on the evaluator does not provision TTS in the bot.
Both `response` and `send_after.response` preserve their spoken meaning. The
ordinary suite loader, text alias and process/report formats remain unchanged.

The whole plan is revalidated immediately before execution, including public
job/scenario mutations, ports, process/session options, recording budgets and
result-summary capacity. After validation, one disposable real model/preflight
inference per distinct settings instance checks runtime usability before any
child is launched. A pre-cancelled plan does not load a model or launch children.
This is a usability check at that instant, not a promise that mutable local model
files will remain available later.

Each scheduled attempt creates a fresh analyzer and recurrent state before its
own child spawn. Models are not preloaded for every repeat. An audio plan permits
at most sixteen concurrent attempts; audio per-label limits also cannot exceed
sixteen. Higher requested limits are rejected rather than silently reduced.
The default concurrency remains four; text-only plans retain their existing
maximum of thirty-two. Immutable
settings are shared, not analyzer state. If a model becomes unavailable after
preflight, the affected attempt fails through the existing process-error result
with no child PID. Subsequent attempts and result retention follow the ordinary
release scheduler policy.

Cancellation is checked before and after per-attempt model construction. Native
model loading and its preflight inference are synchronous and have no hard time
bound; cancellation cannot preempt a native call. Unlike the Python suite's
separate session subprocess, this model construction executes on a Tokio worker
and can delay other attempts sharing that runtime. Concurrent local HTTP tests
do not establish process-level pacing isolation or real-time scheduling.
Once a session runs, its
existing PCM/HTTP/analysis-thread ownership and the hosted child's teardown are
shared with the library path. Forced child termination proves process reaping,
not application-specific cleanup callbacks. Results remain in input/attempt
order and retain the existing bounded-summary behavior.

Recorded and spoken-response jobs share a 4 MiB serialized-scenario allowance
across the complete manifest, charging every logical repeat exactly once. The
loader rejects overflow before expanding jobs, and execution rechecks caller
mutations before making validation copies or loading models. A counting writer
avoids allocating a separate JSON serialization buffer. Ordinary non-recorded
text plans keep their existing bounds.

The input recording limits still apply across the complete manifest and repeats.
They are separate from each session's bot PCM/transcription limits. No audio
file export, playback, live-provider accuracy qualification or classifier judge
is implied by a successful spoken-response suite.
