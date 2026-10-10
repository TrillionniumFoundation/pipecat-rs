# pipecat-whisper

Local speech recognition using a native CPU **whisper.cpp 1.8.3** engine through
**whisper-rs 0.16.0**. The application code is Rust with `unsafe_code = "forbid"`;
the reviewed native wrapper and C/C++ engine are dependencies. There is no Python
runtime, subprocess inference, network request, automatic download, or fabricated
recognition fallback.

## Use

Supply an existing whisper.cpp GGML model, not a Faster Whisper/CTranslate2 or MLX
model directory. Original OpenAI weights must first be converted to whisper.cpp's
format or obtained from its canonical model distribution. Model choice is explicit;
this crate does not silently substitute a smaller or English-only model.

```rust,no_run
use pipecat_core::AudioRawFrame;
use pipecat_whisper::{WhisperConfig, WhisperService, WhisperSettings};

# async fn example(pcm: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
let service = WhisperService::load(WhisperConfig::new("/models/ggml-tiny.en.bin")).await?;
let audio = AudioRawFrame::new(pcm, 16_000, 1)?;
let request = service.submit(&audio, WhisperSettings::default())?;
let transcript = request.result().await?;
println!("{}", transcript.text);
service.shutdown().await?;
# Ok(()) }
```

Input is nonempty, aligned, mono signed 16-bit little-endian PCM at 16 kHz.
Mismatched rates/channels and oversized segments are rejected; resample explicitly
before this service. Values are normalized by 32768. Sub-second segments are
zero-padded internally to one second for native decoding. `WhisperModel` is the
synchronous API for callers already on a dedicated worker thread.

`WhisperSettings` validates language, decoding width, temperature, probability
threshold and initial prompt. Regional language tags normalize to the base language;
`None` requests auto-detection for multilingual models; English-only models
select/report English without attempting unsupported language-token detection. English-only model/language mismatches are rejected
before submission. Probability filtering uses the source service's strict
`segment.no_speech_probability < threshold` rule; `None` disables filtering.
Transcripts include typed segments, millisecond timestamps, probabilities, detected
input language, and supplied audio duration. Translation changes the text to English
but leaves the language field describing the input language.

## Ownership and cancellation

- One named native thread owns one model and serializes inference
- Default capacity is four requests, including queued, running, and completed but
  uncollected replies; a full queue returns `Backpressure` immediately. Dropping
  a pending handle retains its slot until the native request exits
- Default maximum segment duration is 60 seconds. Every input and pipeline buffer
  is checked before growth; limits can be adjusted explicitly up to 600 seconds
- `PendingTranscription::cancel` and dropping a pending request set a native abort
  flag. `result().await` acknowledges the native call's exit before returning
- `finish()` closes admission; `drain().await` joins after accepted work finishes
- `shutdown().await` cancels pending/running work and joins the native thread
- Concurrent join callers and a cancelled join waiter share the same persisted
  completion result. At most one service-owned reaper thread joins the native
  worker independently of Tokio's blocking pool; service Drop also joins the
  reaper. If the operating system rejects reaper creation, an ownership guard
  joins synchronously before returning, so no native worker is detached
- Dropping the service cancels and synchronously joins; use async shutdown to avoid
  blocking an executor thread. Model loading has no native abort callback, so a
  cancelled load still waits for loading to return. Decoding abort is cooperative,
  checked at native compute boundaries, and is not a hard real-time deadline

The bounded native thread exits even if consumers drop replies. Results use one-shot
channels so a stopped consumer cannot block thread shutdown. No global log callback,
process signal handler, or system configuration is changed.

## FrameProcessor

Construct `WhisperSttProcessor::new(service, options)` and place it in a 16 kHz input
pipeline. The default `Vad` mode retains one second of pre-roll and accepts
`VadUserStartedSpeaking` / `VadUserStoppedSpeaking` (including detailed variants) in
either direction. Speech stop submits the buffered segment plus 500 ms of trailing
silence. VAD needs to be provided elsewhere in the pipeline. `AudioFrames` mode is
for input frames that each already contain a complete utterance.

Audio continues flowing while a tracked, ordered result relay emits finalized
`Transcription` frames. `UserAudio` preserves its user ID. Processing/usage metrics
are optional; processing duration includes queue wait. Empty transcripts are
suppressed unless requested. Audio passthrough defaults on. Frames retain their
incoming direction except newly generated transcripts/metrics (downstream) and
errors (upstream).

- End/Stop flush active speech and drain accepted transcripts before the terminal frame
- Cancel aborts native decoding, drops pending work and joins during cleanup
- Bot interruptions preserve user transcription, matching segmented source behavior
- STT mute clears the active buffer and suppresses new input while muted; already
  submitted segments retain their snapshotted settings
- Runtime `SttUpdateSettings` supports targeted settings, typed language deltas,
  initial prompt, temperature, probability filter (`no_speech_prob` alias accepted),
  translation, and decoding strategy. Updates validate atomically. Model changes
  and unknown settings are explicit errors. A valid effective change restores
  usability; rejected, empty, normalized-identical and other-service updates do
  not. The public `update_settings` method shares the same stable health handle
- On speech-buffer overflow or invalid input, the entire current VAD segment is
  discarded with an error. Audio is then ignored until a fresh VAD start, so a
  prefix, late tail or pre-roll cannot resurrect an incomplete utterance

The processor exposes a stable `FrameProcessor::health()` handle, also available
through `worker.processor_health(index)`. While unusable it suppresses provider
audio work but preserves configured audio passthrough, upstream traffic, settings,
mute, VAD and lifecycle controls. `Invalid` and `UnsupportedLanguage` failures of
admitted work are reported as permanent `InvalidRequest` errors. Rejected settings
proposals preserve the active configuration and prior health: they retain their
typed category but explicitly report non-permanence. Local backpressure, worker,
closed service and native failures use nonfatal `Application` and do not change
health. Core attaches the originating processor identity to reported errors.

Every health revision retires the old VAD buffer and cooperatively cancels all
accepted native requests, including a false/true pair coalesced before the relay
wakes. Their late transcripts, metrics and errors are suppressed. Recovery admits
new work and requires a fresh VAD start in segmented mode; it never replays an old
prefix or tail. Cancellation retains native capacity until the native call exits,
so immediate recovery can briefly encounter normal backpressure. Results check
their admission revision before emission; a transition after that check does not
retract an already admitted effect. End/Stop still close admission and join; a
later restoration cannot reopen this single-use processor.

This is a single-user segmentation buffer and a single-use processor. Avoid mixing
multiple simultaneous speakers into one processor instance. Pipeline system frames
can overtake queued normal audio; as with the source segmented processor, VAD
boundaries apply to audio already delivered to the processor.
`UserAudio` can also overtake ordinary settings frames. To guarantee a settings
change precedes the next utterance, observe its forwarded update frame before
submitting that utterance, or call `update_settings` directly before submission.
Unlike a reusable source processor's Stop lifecycle, this processor owns its native
service by value and closes it on Stop. Setting up the same instance again reports
`Closed`; create a new service and processor for another run.

## Build and tests

A C/C++ toolchain, CMake and libclang are needed to build the crates.io
`whisper-rs-sys` engine. The default build enables CPU only, with no GPU SDK dependency.
This repository's workspace-local environment supplies the pinned Rust toolchain
and CMake; libclang discovery can use `LIBCLANG_PATH` when necessary.

```sh
cargo test -p pipecat-whisper --offline
cargo clippy -p pipecat-whisper --all-targets --offline -- -D warnings

# Explicit provisioned model and fixture; no test downloads anything.
export PIPECAT_WHISPER_MODEL=/absolute/path/ggml-tiny.en.bin
export PIPECAT_WHISPER_FIXTURE=/absolute/path/jfk.wav
cargo test -p pipecat-whisper --offline -- --include-ignored --test-threads=1
```

The opt-in tests run real offline recognition, repeat inference, check the JFK
phrases, language/model compatibility, probability filtering, initial prompt,
queue backpressure, native abort/reuse, drop, graceful shutdown, concurrent joins,
aborted join waiters, saturated blocking-pool Drop, overflow discard/recovery,
pipeline VAD/End/Stop/Cancel, targeted settings and user IDs, unusable passthrough,
effective settings recovery, and coalesced health revisions with active/queued
native work and VAD buffers.
Ordinary unit tests validate PCM, language, configuration and cancellation without
model files, including failed reaper creation, persistent panic results and
request-slot ownership through cancellation. These checks do not establish transcription-quality equivalence with
Faster Whisper across languages, devices, datasets or model families.

### Verified external test assets

Assets are not bundled in this crate. The model's upstream model card declares MIT;
whisper.cpp distributes the JFK fixture under its repository MIT license.

- Model: [canonical ggml-tiny.en.bin](https://huggingface.co/ggerganov/whisper.cpp/resolve/main/ggml-tiny.en.bin), 77,704,715 bytes
- Published [whisper.cpp model SHA-1](https://github.com/ggml-org/whisper.cpp/blob/v1.8.3/models/README.md): `c78c86eb1a8faa21b369bcd33207cc90d64ae9df`
- Published [Hugging Face LFS SHA-256](https://huggingface.co/api/models/ggerganov/whisper.cpp/tree/main): `921e4cf8686fdd993dcd081a5da5b6c365bfde1162e72b08d75ac75289920b1f`
- Fixture: [v1.8.3 samples/jfk.wav](https://github.com/ggml-org/whisper.cpp/blob/v1.8.3/samples/jfk.wav), 352,078 bytes
- Published [Git blob ID](https://api.github.com/repos/ggml-org/whisper.cpp/contents/samples/jfk.wav?ref=v1.8.3): `3184d372cd2f8b804d3a540c70ec50d927b335d2`
- Fixture SHA-256 (locally computed, not a published upstream checksum): `59dfb9a4acb36fe2a2affc14bacbee2920ff435cb13cc314a08c13f66ba7860e`
- Upstream [MIT license](https://github.com/ggml-org/whisper.cpp/blob/v1.8.3/LICENSE)

The downloaded model was verified against both published digests, and fixture
content against Git's `blob <length>\0<bytes>` object digest. These are real weight
files; upstream `for-tests-*` files without weights are not usable substitutes.

## Source correspondence and gaps

Corresponds to `src/pipecat/services/whisper/stt.py` and the audio buffering,
pre-roll, trailing silence, finalized result and off-event-loop work in
`SegmentedSTTService`. It preserves local PCM recognition, typed language selection,
initial prompt, configurable temperature/probability filtering, settings snapshots,
metrics, finalized transcripts and model/language validation. The selective audio
gate follows `STTService.process_audio_frame`; buffered-segment retirement follows
`SegmentedSTTService`'s unusable stop path. Effective-change restoration follows
`AIService._update_settings`, with revision-based native cancellation preventing
retired work from surfacing after recovery.

Explicit gaps/differences:

- Native whisper.cpp CPU inference replaces Faster Whisper/CTranslate2 and MLX;
  their device/compute-type flags, model aliases and output-quality parity are not
  claimed. GPU/Metal/MLX-specific execution has not been exposed or validated
- Hotword boosting is unsupported and rejected in runtime deltas. Initial prompt
  is available, but is not presented as an equivalent hotword implementation
- Models do not auto-download or hot-swap. Distilled models require compatible GGML
  weights and separate quality testing; no default distilled model is implied
- No streaming partial hypotheses, word alignment, diarization, built-in VAD,
  automatic resampling, hosted Whisper API, tracing decorators or STT reconnection
  manager is implemented here. Hosted REST STT belongs to `pipecat-speech`
- The source's unbounded segment queue is deliberately bounded; overload is visible
- Multilingual inference and translation APIs are implemented but the bundled test
  recipe verifies only English tiny.en, not multilingual quality

See [vendor/whisper-rs/PATCHES.md](vendor/whisper-rs/PATCHES.md) for the small wrapper
ownership fix required by abort callbacks and repeated inference.
