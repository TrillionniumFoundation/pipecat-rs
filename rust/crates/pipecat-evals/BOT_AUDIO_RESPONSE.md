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
compiled-child `suite`. Explicit `eval` criteria on spoken `response` and
`function_call` use the configured native classifier judge (see below).

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

## Continuation-aware absence

A spoken `response` with `absent: true` follows the pinned Python matcher:
segments continuing a previously matched reply are consumed while the original
absence deadline continues. A response fails absence only when the latest
observed remote `llm_started` or `bot_started_speaking` event is strictly later
than the last successful positive expectation's completion. Any positive event
or complete function-call expectation advances that session-lifetime boundary;
judge-backed checks advance it only after acceptance. Failed checks, timeouts,
successful absence, turn resets and interruptions do not advance/reset it.

These are local ingestion times, not remote timestamps, PCM start times or
reply identities. Equality is continuation. With no observed remote start, even
an initial response is ignored by this source-compatible absence rule: passing
absence does not prove literal silence. Conversely, a newer start already
observed can cause an older queued response to fail. Local VAD starts do not
count. Ignored events remain in bounded history and judge conversation. The
text-mode alias `llm_response` and all other absent event types still reject
any matching event. Audio epochs and resource limits are unchanged.

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
one Silero analysis thread, plus the optional Smart Turn worker described below.
Normal completion, failure and cancellation stop feed, revoke HTTP work, and
actually join these threads. Dropping the run future
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
- Native defaults to a 600 ms speech-timeout aggregation policy. The pinned
  source defaults to LocalSmartTurnAnalyzerV3 (subject to service strategy
  recommendations). The optional model-backed policy below closes that model
  decision path without changing the existing native default.
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
`--recorded-audio` can be combined with
this mode, reusing its existing bounded WAV loader and outer-scenario-directory
media paths. `--bot-audio-wav` is mutually exclusive for `run`.
Cancellation, deadlines and old-generation cleanup use the existing transcription
session owners; this CLI entry does not change completion or acoustic guarantees.

For synthesized user input, enable both audio features and use the separate
`AudioToAudioScenarioFile`/`--user-speech ... --bot-audio-response ...` direct
entry described in [AUDIO_TO_AUDIO.md](AUDIO_TO_AUDIO.md). It uses one immutable
audio-response plan, frozen WAV overrides and joint owned cleanup, including
all-WAV input. This finite profile rejects Smart Turn, WAV export, images and DTMF.
Its hosted counterpart is documented in
[HOSTED_AUDIO_TO_AUDIO.md](HOSTED_AUDIO_TO_AUDIO.md). The standalone
response/recorded/suite/Smart Turn entries
documented here retain their existing behavior.

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

Cancellation is checked around per-attempt model construction. The CLI and
hosted release path use the owned loading mechanism described below; native
loading and preflight inference have no hard time bound and cannot be preempted.
Concurrent local HTTP tests do not establish process-level pacing isolation or
real-time scheduling. Once a session runs, its
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
file export, playback or live-provider accuracy qualification is implied by a
successful spoken-response suite. Semantic judgments run only when explicit
`eval` criteria and a judge configuration are supplied.

## Owned native model loading

The explicit `run` CLI and hosted release path prepare native models on owned
OS threads. Async callers wait for real thread completion and then join; a
completion notification alone is not a join. This lets Tokio timers, other
attempts and cooperative cancellation continue while a loader is blocked.
The existing synchronous `BotTranscriptionSettings::into_sessions` API remains
synchronous. Async library callers can use `into_sessions_with_cancellation`.

Cancellation stops further model and child admission. An already started native
call must still return before its owner can join and report cancellation. No
`spawn_blocking` timeout detaches a loader, and no hard cancellation deadline is
claimed. The CLI installs its signal owner before async native preparation; all
scenario, URL, option and recording validation still finishes before any model
is started, and every model is prepared before the first connection. The `run`
path retains at most sixteen independently prepared sessions. If preparation
is cancelled, the async library method returns `EvalError::Cancelled`; the CLI
uses its existing preparation-error stderr/exit-2 path, with no fabricated
scenario report. Cancellation of an already running session retains its
ordinary failed-result/exit-1 behavior. The signal task is joined on either path.

A release invocation owns a loader registry. Registration and closing are
serialized so closing cannot miss a newly started thread. Each entry retains
its handle through actual joining, including the race between task completion
and registry destruction. In audio or mixed audio/text plans, cooperative
cancellation and task errors drain active jobs; plain plans retain their
existing error cleanup path. For an external forced future drop, the registry synchronously
joins remaining loaders before its destruction returns. That fallback can
block the dropping thread until native work finishes; it is deliberately not
a responsive hard-abort mechanism.

Suite task slots include loading and completed-but-unclaimed configurations as
well as running sessions. The existing sixteen-attempt cap bounds models in
one release invocation. Disposable preflight models are built and dropped
serially before attempts start. At most 128 jobs and 128 distinct settings
objects require at most 256 registry entries; joined entries retain no model.
These are per-invocation bounds, not a process-global limit across independent
library callers.

This change isolates preparation only. The existing active-transcription
analysis thread still has a synchronous final join, and native inference or
thread teardown can delay it. Runtime initialization remains process-global
with one canonical ONNX library path. Full process isolation would be needed
to enforce hard deadlines on arbitrary native code; it is not provided here.

Loader panic errors omit the panic payload from the returned error. The ordinary
Rust panic hook can still print that payload to stderr.


## Optional Smart Turn completion

The same feature supports an explicit local Smart Turn v3 model. Add
`"smart_turn_model": "models/smart-turn-v3.2-cpu.onnx"` to the existing response
configuration. Omission or JSON null retains the 600 ms policy. The optional
asset resolves relative to the canonical configuration target's directory,
like Silero; it must be a nonempty regular file of at most 128 MiB. Configuration
validation, each disposable suite preflight and every attempt recheck it.
The existing owned loader loads and probes both models with the same explicit
runtime before network or child admission. No model is downloaded. A session
now retains one Silero model and, when enabled, one Smart Turn model; the
sixteen-session/attempt bound applies to these pairs, not sixteen total graphs.

Library callers can supply `BotTranscriptionConfig::with_smart_turn(analyzer)`.
This adapter accepts the bounded 16 kHz mono default Smart Turn policy: an
eight-second retained window, 500 ms pre-speech selection and three seconds of
sample-counted silence fallback. VAD start compensation uses the actual Silero
start setting. Maximum history is a model input window, not a completion rule.

The paced local VAD owner sends PCM and its latched speaking state through one
bounded, epoch-tagged event queue. Starting noise before a VAD start is not
speech; the stopping delay remains speech until local VAD stop. Smart Turn sees
all paced chunks, including generated silence. At local stop it analyzes the
retained utterance while HTTP transcription continues. INCOMPLETE retains that
history for subsequent speech; COMPLETE clears the analyzer buffer and latches
a completion decision. Three seconds of nonspeech samples after local VAD stop
also complete the turn. This is not a wall-clock timer from the acoustic end.

A response still requires all admitted segment HTTP receipts. The existing
held-stop adapter releases its aggregator stop only when the current endpoint
decision and those receipts are ready, with no additional 600 ms delay. New
local speech revokes a prior completion latch; epoch changes clear history and
invalidate old predictions and transcripts. Remote speaking messages do not
establish an audio boundary. The adapter deliberately retains the native
all-final HTTP gate rather than Python's physical-stop/P99 transcript fallback;
it does not claim identical source timing or identify remotely late untagged PCM.

Smart Turn retains at most 128,000 PCM16 samples (256,000 bytes) in its
eight-second history, plus a bounded prediction snapshot. The shared event
queue has sixteen entries; each additional PCM event contains one 1,280-byte
block. These are additional to the existing capture and STT buffers, and do not
include native model/runtime memory.

Smart Turn uses the existing bounded native inference owner in addition to the
Silero owner. Cancellation invalidates predictions, stops input and actually
joins both owners. Active inference teardown can block until native code
returns; this policy adds no hard cancellation deadline. Queue overflow or
model failure is an evaluation failure, not silent audio truncation. The
existing total PCM, per-segment, transcript and HTTP limits remain in force.


## Semantic judgments of spoken responses

In the explicit audio-response mode, `expect.response.eval` judges completed,
current-epoch STT text. The same existing `judge.eval` configuration used by text
sessions selects the classifier, endpoint, credential environment variable and
bounded request options. No credential, classifier service or billable request
is provisioned implicitly. Configuration and missing credentials are checked
before model loading or any connection in the CLI and release preparation paths.
The classifier's modality remains text: its input is the recognized transcript,
not raw PCM. This evaluates intended spoken meaning, not audio quality.

Library callers may inject an owned `EvalJudge` with
`EvalSession::with_bot_transcription_and_judge`. Both constructors support
semantic `response` and `function_call` checks. Parallel `llm_response` events
remain available for ordinary text predicates, but `llm_response.eval` in audio
mode is rejected before connection. The pinned Python matcher can judge that
combination; this native subset avoids combining two reply evidence sources in
one transcript. Use ordinary text mode to judge generated LLM text.

Only spoken `response` events enter this session's judge transcript. Consumed
segments are appended once; queued spoken tails are recorded before the next
user boundary. Consecutive bot segments merge into one latest reply. This keeps
the existing native full-conversation context, including segments consumed by
non-eval checks; the source matcher appends only deltas encountered by eval
checks. User recording reference text remains the user-side context and is not
sent as replacement input audio.

`Continue` waits for further spoken segments within the original expectation
budget. `No` takes one local activity snapshot: actual VAD speech, a held stop,
an open local turn, queued STT segments or in-flight STT can defer rejection to
that original deadline. Otherwise it waits the judge's bounded No grace. Pure
queued silence and periodic window bookkeeping do not extend grace. A later
speech start does not retroactively extend the snapshot. The pinned source uses
a one-time remote speaking snapshot instead; remote speaking reports are not an
audio completion authority here. Each subsequent response can produce a new
judgment over the merged reply.

Classifier requests retain the existing immutable transcript revision, cache,
request/response limits, deadline and epoch fence. While awaiting a verdict the
session continues processing audio. A session-owned pending-recognition slot
keeps an already claimed STT result across cancellation of the event pump; it
is committed once after the admitted-input fence or discarded on an old epoch.
No extra background judge task is created. Cancellation closes the owned HTTP
future and follows the existing VAD/Smart Turn/transport cleanup contract.
These protocol fixtures do not establish live recognizer/classifier accuracy.
