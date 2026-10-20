# Hosted scripted audio-to-audio suites

With `user-speech,bot-audio-transcription`, the explicit joint suite runs
Cartesia/private-cache/WAV input through a compiled local bot child, then checks
its received PCM using local Silero and configured HTTP STT. `response` remains
spoken text; `llm_response` remains reported generated text. Reference text and
bot LLM/TTS reports do not substitute for received audio.

```sh
pipecat-eval suite native-manifest.yaml \
  --user-speech tts-runtime.json --bot-audio-response stt-runtime.json
```

Both flag orders select one joint loader. Both features are required; missing
features are rejected before reading runtime configuration or credentials.
The library entry is
`ReleaseManifest::load_audio_to_audio(path, speech_settings, transcription_settings)`;
execute its returned plan with the existing `release::run_release` or
`run_release_with_budget` API. See [USER_SPEECH_SUITE.md](USER_SPEECH_SUITE.md)
for the native manifest and [BOT_AUDIO_RESPONSE.md](BOT_AUDIO_RESPONSE.md)
for explicit STT/runtime/model configuration.

“Hosted” here means the existing local compiled-child runner. It does not mean
a deployed service or a validated remote provider. The executable, runner body
and local model/runtime paths remain trusted local inputs. Nothing is downloaded
or implicitly deployed.

## Selection and preflight

Scenarios use the audio source shape in [AUDIO_TO_AUDIO.md](AUDIO_TO_AUDIO.md).
Per-turn WAV overrides precede synthesis. An all-WAV plan still selects joint
completion and cleanup, retains its shared budget and validates configuration,
but does not create a synthesis/cache owner just because a voice is present.

The joint loader preserves `expect.response` and `send_after.response`. Private
validation copies may map those predicates for existing structural validators;
the frozen scenario and its byte charge retain the original fields. Every
speech/joint job's public scenario must still match its private canonical
snapshot at execution. Plain and standalone spoken-response jobs keep their
existing public revalidation contract; they do not gain a new frozen snapshot.

All jobs are checked before any native model preflight, child launch or HTTP
operation: identity, unique ports, concurrency, bounded original scenarios,
frozen provenance, WAV coverage, judge configuration and joint session options.
Local configuration and WAV reads are part of preparation. A bad final job must
not allow an earlier valid job to start external work.

Joint jobs may mix with other joint jobs and ordinary text/recorded jobs. A joint
invocation cannot include standalone speech-only or spoken-response-only jobs;
mixing the two standalone audio profiles is also still rejected. The ordinary
session builders and their combination restrictions remain unchanged.

This finite joint profile uses Silero plus speech-timeout aggregation. It rejects
Smart Turn, images, DTMF, recording output and `--recorded-audio` (use per-turn WAV
overrides). Manifest `record: true` and `cache_dir` remain unsupported. An
explicit cache in the speech runtime retains its existing supported behavior.

## Attempts, accounting and privacy

Each attempt owns its model/analyzer state, playback, recognition epochs, socket
and direct child. After all static checks, each distinct STT settings object has
one owned model preflight; each admitted attempt then loads its own configuration.
Completed native loader threads are joined before returning their result.

The native flat scheduler and attempt-major result order are retained. Python's
coalesced `(attempt, label)` queue is not reproduced. With any spoken-response
job, invocation concurrency and each audio job's cap are at most 16. An ordinary
job's existing local cap may remain 32, subject to the global cap. Slots are not
released until session cleanup, direct-child reaping and pipe draining finish.

Existing bounds remain: 128 jobs, 128 logical spoken/recorded references, 4 MiB of
original audio scenarios, 16 MiB of loaded WAV files, 15,360,000 logical input PCM
bytes and 64 MiB retained release results. Scenario bytes are counted once per
logical job; validation copies are not charged as another scenario. WAV uses,
including repeats and cloned plans, are precharged. Every synthesized or cache-hit
use is charged against the same invocation counter; hits are not free and there
is no per-attempt reset or refund. Each session also retains its separate bot PCM,
segment, transcript, transport and request limits.

These are bounded logical allowances, not a process RSS or provider-cost cap.
Concurrent model instances and buffers can coexist. Refer to the input and STT
documents for their individual limits and cache privacy: persistent audio may
contain sensitive speech. Runtime credentials are not passed to bot children;
only the manifest's explicitly configured child environment is supplied. Avoid
putting secrets in labels, arguments, runner bodies or protocol output.

## Completion, deadlines and cancellation

The actual joint session from the direct route is used, rather than attaching
two standalone owners. Passing an early assertion does not bypass input/cache
settlement or the bounded, already-admitted bot PCM/recognition prefix. Local
send admission is not evidence of remote consumption or physical playback;
settlement also cannot prove that no future remote audio will arrive.

Model preparation is outside the startup/session timers and has no hard native
preemption deadline. The startup deadline begins after configuration preparation
and covers launch admission and WebSocket readiness. The session timer begins at
its run phase and includes the protocol handshake. Assertion deadlines include
input synthesis/cache latency. Joint playback and final-prefix settlement keep
their existing fixed deadlines; socket traffic cannot continually refresh them.

On normal return, errors or cooperative cancellation, the joint session first
revokes input/STT admission and requests producer shutdown, then awaits its
playback/socket/input owners and joins its native recognizer. The STT HTTP request
is an owned future, not a detached task; cancellation drops that request. The
hosted tail then reaps the direct child and drains its pipes before retaining
the result. Primary failures remain ahead of cleanup failures. Result retention
overflow cancels and drains active audio attempts and leaves queued attempts
unstarted.

If a bounded wait expires, retain the original pinned future, cancel its token
and await that same future. Rebuilding a plan does not finish the previous owners.
Actual future drop/abort/panic retains the weaker existing fallback guarantees;
it is not proof of awaited HTTP/socket completion or successful OS reaping.
Before spawn, prepared configuration has no active PCM worker, request or
playback; dropping it destroys prepared objects rather than joining nonexistent
asynchronous work. No process-tree sandbox or globally hard shutdown deadline
is introduced.

## Verification scope

The acceptance plan uses compiled local children, complete multi-packet input
consumption, existing local Silero assets and loopback TTS/STT/judge services with
synthetic credentials. Scripted recognizer replies test protocol and ownership
semantics, not recognition accuracy. Feature-specific compile/lint checks and
runtime targets are reported separately; no unexecuted configuration is counted
as a runtime pass. This profile does not claim complete Python suite parity,
paid-provider validation or production deployment readiness.
