# Direct two-sided timeline recording

`EvalSession::run_with_timeline_recording(cancel, TimelineRecordingLimits::default())`
returns a `TimelineRecordingRun` with the ordinary public `result` and a
`to_wav()` method. This opt-in diagnostic combines admitted user-source PCM and
validated bot PCM on one monotonic clock. The output is always 16 kHz mono PCM16.
It does not change text/event assertions or add speech recognition, audio judging,
a provider, a model, or a physical playback acknowledgement.

## CLI and library entry

Run exactly one recorded-input scenario against an audio-capable endpoint:

```sh
pipecat-eval run scenario.yaml ws://127.0.0.1:8765 \
  --recorded-audio --recording-wav conversation.wav
```

With the `user-speech` build feature, the same output accepts the existing direct
synthetic-speech, private-cache and per-turn WAV-override path:

```sh
pipecat-eval run scenario.yaml ws://127.0.0.1:8765 \
  --user-speech runtime.json --recording-wav conversation.wav
```

See [RECORDED_SCENARIOS.md](RECORDED_SCENARIOS.md) and
[USER_SPEECH.md](USER_SPEECH.md) for input schemas and runtime configuration.
`--stop-bot` remains optional. A native endpoint needs both
`EvalBotHost::with_raw_audio_input(true)` and `with_audio_output(true)`, plus an
application that consumes input PCM and actually produces bot PCM. Recording
requests capture permission and keeps TTS enabled; it does not generate bot
speech on its own.

The library accepts a session already configured with recorded input or direct
user speech. For example, after preloading a recorded scenario:

```rust,no_run
use pipecat_evals::{
    Cancellation, RecordedScenarioFile, SessionOptions, TimelineRecordingLimits,
};
# async fn example() -> Result<(), pipecat_evals::EvalError> {
let mut sessions = RecordedScenarioFile::load("scenario.yaml")?
    .into_sessions("ws://127.0.0.1:8765", SessionOptions::default())?;
let session = sessions.remove(0);
let limits = TimelineRecordingLimits::default();
session.validate_timeline_recording(limits)?;
let run = session.run_with_timeline_recording(Cancellation::new(), limits).await?;
let wav = run.to_wav()?;
// Inspect run.result even when it failed. wav may be None or a partial diagnostic.
# let _ = wav;
# Ok(()) }
```

The optional `validate_timeline_recording` check is local and creates no owner or
connection; the run method repeats it. It cannot undo model/configuration work a
caller performed earlier. Existing public option/scenario struct literals are
unchanged.

Direct recorded/user-speech sessions and direct joint audio-to-audio sessions are
supported. With both audio features enabled, select one joint scenario using
`--user-speech runtime.json --bot-audio-response stt.json --recording-wav new.wav`.
The `AudioToAudioScenarioFile` library path also accepts
`run_with_timeline_recording`, including all-WAV input plans. Recording rejects
pure text input, standalone bot-response transcription, hosted/release/suite
runs, images and DTMF. `--recording-wav` also conflicts with `--bot-audio-wav`.
CLI flag conflicts are rejected before reading scenario/runtime/model files or
creating an output temporary. Selected scenarios and destination are preflighted
before connecting or synthesizing. The legacy
[bot-only capture](BOT_AUDIO_CAPTURE.md) API and flag retain original-rate,
arrival-order concatenation and their existing limits.

## What the timeline means

The run establishes one monotonic origin before connecting. Each user clip is
timestamped when its original, complete source PCM is queued. A separate
contiguous source cursor advances only when the local transport sender admits a
wire packet successfully. Synthesis completion, cache writes, queue admission and
an attempted send alone do not qualify source bytes for the final recording.
The WAV omits unsent source suffixes, wire-idle silence and the final padded part
of a 40 ms packet. A successful local queue admission does not establish socket
delivery, remote processing or audible playback.

Bot chunks are timestamped at validated application ingest. Their future tail is
modeled as continuous, ideal sample-time playout. A validated `bot-interrupted`
event crops the not-yet-played suffix across chunks. Normal completion, failure
and explicit cancellation likewise freeze one terminal bot cutoff before cleanup;
a slow join does not extend it. Bot interruption does not cancel admitted user
speech. The last successfully admitted user-source prefix is preserved even
when its nominal duration extends beyond the bot cutoff.

There is no remote generation identifier for untagged PCM arriving after an
interruption, so such PCM is treated as new local admission. This simulation does
not measure a remote speaker or the consumption clock of an STT worker. An early
successful text assertion may stop recording before TTS finishes; there is no
implicit wait for all speech to complete.

For each side, logical time and rendered sample count are separate. Lateness
strictly greater than 200 ms inserts whole source-rate zero samples; exactly
200 ms or less advances logical time without adding silence. Each complete
side is resampled with one native resampler state and final flush. The first
surviving sample across either side defines the common output origin, omitting
pre-audio handshake silence. The later side receives leading zeros on the shared
16 kHz timeline. The mono mix saturating-adds overlap and zero-pads the shorter
track; it does not interleave stereo or truncate the longer side.

## Bounds and failures

Default limits are also the maxima; callers can request smaller positive values:

- `max_duration`: 120 seconds, including gaps and cross-side alignment
- `max_raw_bytes`: 8 MiB combined cumulative admitted raw-source PCM
- `max_records`: 4096 combined raw records

Input remains 16 kHz mono PCM16. The first accepted bot sample locks that side to
8, 16, 24 or 48 kHz; explicit non-mono channels, odd bytes and later rate changes
are rejected. Existing source, cache, wire and legacy bot-capture budgets continue
to apply independently. Cropping does not refund cumulative recording quotas.

All layout, alignment, resampled output including filter tail, mix and WAV sizes
are checked before the corresponding large allocation. Final PCM is at most
3,840,000 bytes and WAV at most 3,840,044 bytes. The renderer's conservative
temporary-capacity budget is 128 MiB, including raw data and metadata; this is
not measured process RSS or a guarantee about allocator/native-model memory.

Runtime failures remain in `run.result`. Previously qualified, bounded audio may
still be exported after cleanup. Exceeding an admission limit fails the run;
rendering overflow or an invalid final layout fails the artifact. Neither is
silently reported as a complete truncated recording. Zero surviving samples on
both sides yields `Ok(None)` and no destination file. One-sided audio is valid.

Cooperative cancellation followed by awaiting the run retires the actual input,
transport and optional speech/cache/transcription owners before returning the recording. A
dropped/aborted future, panic or killed process does not promise an artifact or a
completed asynchronous join. No additional recorder task or owner is introduced.

## Trusted-local publication (Unix)

The CLI prints the unchanged `ScriptResult` JSON on stdout before rendering or
publishing. Artifact status goes to stderr. Exit codes are 0 for a passing run,
1 for a failed/cancelled run with successful artifact handling, and 2 for
preflight/render/publication errors. An artifact error preserves an already
printed result; it cannot turn a failed evaluation into a pass.

The destination's parent must already exist and the destination must not exist,
including as a symlink. The CLI reuses the legacy WAV publisher: a private 0600
temporary in the canonical parent, bounded writing, flush and file fsync,
temporary identity verification, same-directory no-clobber hard-link publication,
destination identity verification, removal of only the owned temporary, and
parent-directory fsync. A destination created concurrently is never replaced.
The hard link does not allocate a second full payload. An empty recording removes
the owned temporary without creating the destination.

After publication, a cleanup or persistence failure explicitly reports that the
destination was created and leaves it in place. Crashes or cleanup failures can
leave a temporary file. This is a trusted-local facility, not protection against
hostile concurrent directory replacement. It creates no parent directory and
changes no existing file permissions. Non-Unix publication rejects before
connection. WAV metadata contains no transcripts, credentials or cache settings;
the PCM itself can contain sensitive conversation audio.

## Source differences and qualification scope

The source reference is `ProfHepta/pipecat` at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, particularly
`src/pipecat/evals/client_transport.py`, `src/pipecat/evals/client.py` and
`tests/test_evals_client_transport.py`.

Native recording preserves source-timestamp layout while adding successful local
input-prefix qualification, fixed per-side formats, a terminal bot cutoff and
bounded no-clobber output. Its continuous bot simulation does not reproduce
Python asyncio scheduling. The native sinc resampler is not byte-identical to
Python SOXR/resampy. Qualification uses deterministic native sample expectations
and synthetic loopback HTTP/WebSocket input and bot PCM; it does not qualify live
providers, microphones, speakers, audio quality, SmartTurn, hosted/suite
recording or full Python parity.

## Joint spoken-response evaluation

In a direct joint run, each bot packet first satisfies the shared 16 kHz mono
PCM16 contract, then reaches the bounded timeline and existing bounded
capture/transcription consumers once. Their limits remain independent. A later
consumer failure fails the evaluation and may retain an earlier admitted
diagnostic prefix; the WAV is not a claim of atomic consumer success. Generated
LLM text still cannot satisfy a spoken `response`, and interruption keeps the
existing recognition epoch fence while cropping simulated bot playout.

Successful final spoken-prefix settlement retains its original deadline. At
terminal outcome, the bot cutoff freezes before cleanup awaits, while admitted
user-source receipts remain writable until Playback joins. Joint cleanup stops
and actually joins speech, Playback, socket and transcription owners, using its
existing stop-bot-once state. Slow native/STT retirement cannot extend the tail.
The snapshot is taken only after those owners retire. No hosted or suite
recording mode is added.
