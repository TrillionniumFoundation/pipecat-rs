# pipecat-vad

Native Rust Silero voice activity detection and Smart Turn v3 CPU inference.
The executable code uses Rust DSP/state machines and the native ONNX Runtime C
API through `ort`. No Python interpreter, Python wrapper, HTTP inference service,
or speech-energy substitute is used at runtime.

## Runtime and assets

Install a compatible official ONNX Runtime CPU distribution separately. This
crate pins `ort` and `ort-sys` to `2.0.0-rc.9`, requests the ONNX Runtime 1.20 C API,
and has been exercised with ONNX Runtime 1.21.0. Cargo builds use `load-dynamic`
with binary downloads disabled; they do not require the runtime to be installed.
Inference requires it. Missing runtime/model files are errors.

Production constructors require **both paths explicitly**. There are no implicit
model searches, downloads, or writes. The runtime is process-global: initialize
this crate before other `ort` consumers and use the same runtime library for all
models. An incompatible native library can leave the dependency's global loader
initialized; restart the process after correcting such an installation.

The repository includes these existing model files. A standalone deployment must
copy the chosen asset and its notices into its own deployment image and pass its
installed path; do not depend on the source repository being present.

| Model | Repository asset | SHA-256 |
| --- | --- | --- |
| Silero | `src/pipecat/audio/vad/data/silero_vad.onnx` | `597d30b3ec076608d059477bb14cfeffdf951bf5cae370d38f65d33bbfe82004` |
| Smart Turn v3.2 | `src/pipecat/audio/turn/smart_turn/data/smart-turn-v3.2-cpu.onnx` | `2bb026316b14a660486a75b1733cd3fbab8c2fd0314dc9af7be49f8cca967e4f` |

The Silero asset is the tracked Pipecat model, distributed under the upstream
[Silero project's MIT license](https://github.com/snakers4/silero-vad/blob/master/LICENSE)
(copy in `LICENSE-silero`). Pipecat source adaptations use the repository's
BSD-2-Clause license. Loudness filter/histogram equations follow
[loudness 0.2.0](https://pypi.org/project/loudness/0.2.0/), MIT, with the full notice
in `LICENSE-loudness`. Whisper preprocessing attribution and model licensing are
in [SMART_TURN.md](SMART_TURN.md) and `LICENSE-APACHE`.

## Pipeline integration

Load models during application startup, outside latency-sensitive processor
calls. If startup runs on an async executor, load them in a blocking task.

```rust,no_run
use std::sync::Arc;
use pipecat_vad::{
    RuntimeOptions, SileroModel, SileroVadAnalyzer, SileroVadProcessor,
    ProcessorOptions, VadParams,
};

let runtime = RuntimeOptions {
    library_path: "/opt/onnxruntime/lib/libonnxruntime.so".into(),
};
let model = SileroModel::load(&runtime, "/opt/models/silero_vad.onnx")?;
let analyzer = SileroVadAnalyzer::new(model, 16_000, VadParams::default())?;
let vad = Arc::new(SileroVadProcessor::new(analyzer, ProcessorOptions::default())?);
// Insert vad before the STT processor; it implements pipecat_core::FrameProcessor.
# Ok::<(), pipecat_vad::VadError>(())
```

`SileroVadProcessor` forwards input audio before asynchronously broadcasting
`VadUserStartedSpeaking`, `VadUserStoppedSpeaking`, and periodic `UserSpeaking`
frames upstream and downstream. Start and parameter updates broadcast
`SpeechControlParams`. Input must be mono, signed PCM16 little-endian, at the
analyzer's configured rate; Start must specify that same rate. Wrong rates,
stereo, malformed PCM, oversized frames, and invalid parameters fail explicitly.

One owned native thread runs inference serially. Bounded input and result queues
(default 32 frames) prevent unbounded per-frame task spawning. Incoming frames
are limited to one second by default. A full inference queue emits an explicit
processor error; applications should monitor errors and pace transport input.
`End`/`Stop` drain accepted audio before forwarding terminal frames. `Cancel`
aborts queued inference; cleanup waits for the current fixed-size inference and
joins the thread. Interruption does not discard the user's ongoing VAD stream.
The idle timer forces a stopped event after one second without input while
speaking; set `audio_idle_timeout` to zero to disable it.

## Silero behavior

- Batch size one; 256 samples at 8 kHz or 512 samples at 16 kHz (32 ms)
- PCM normalization by 32768; 32/64-sample left context; `[2,1,128]` recurrent state
- Explicit reset and automatic context/state reset on model sample-rate changes
- Analyzer reset after its first inference and every five monotonic seconds,
  matching the source's reset-after-prediction ordering
- Byte-buffering across arbitrary input boundaries; incomplete model windows are
  retained without zero-padding and discarded at session cleanup
- Source `Quiet`, `Starting`, `Speaking`, `Stopping` hysteresis, Python ties-to-even
  frame-count rounding, and confirmation after an entire incoming batch
- 400 ms rolling BS.1770 K-weighted volume with the source's 0.1 LUFS histogram
  bins, normalized from `[-110,-10]`, absolute gate at -70 LUFS, then 0.2 exponential
  smoothing; warm-up reads zero until the window is full
- Parameter changes reset hysteresis but preserve the continuous volume window,
  smoothing history, partial input, and model state

The model API returns errors on inference failure rather than silently converting
failures to silence. Unrecoverable inference errors terminate the pipeline, so a
failed inference worker cannot leave the session running without VAD. `analyze_at` accepts a monotonic elapsed time for deterministic
replay. The pipeline processor supplies its own monotonic inference clock.

The 400 ms loudness warm-up and smoothing can delay the first confirmed speech
beyond `start_secs`; this is also true of the Python source. Multi-window batches
are confirmed after the batch, so event timing depends on the input frame sizes.

Smart Turn exposes validated feature extraction, actual model inference at
16 kHz, and the bounded `SmartTurnAnalyzer` audio/cancellation policy. The
[`pipecat-turns::SmartTurnUserAggregator`](../pipecat-turns/README.md#native-smart-turn-integration)
processor integrates that analyzer with transcript/STT gating, stale-result
protection, paired conversation history, and End/Cancel cleanup. Feed it ordered
VAD decisions and audio; a high Smart Turn probability on silence does not
indicate speech and must not replace VAD gating. Soxr resampling remains outside
this implementation; see [SMART_TURN.md](SMART_TURN.md).

## Verification

From the repository root, with its pinned Rust toolchain:

```bash
cargo check --manifest-path rust/Cargo.toml -p pipecat-vad
cargo test --manifest-path rust/Cargo.toml -p pipecat-vad
cargo clippy --manifest-path rust/Cargo.toml -p pipecat-vad --all-targets -- -D warnings
```

Real model tests run by default, with no ignored or skipped inference. The
test-only runtime fallback is
`/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0`; model fallbacks are the tracked
repository paths above. Other installations must supply:

```bash
PIPECAT_ONNX_RUNTIME=/path/to/libonnxruntime.so \
PIPECAT_SILERO_MODEL=/path/to/silero_vad.onnx \
PIPECAT_SMART_TURN_MODEL=/path/to/smart-turn-v3.2-cpu.onnx \
cargo test --manifest-path rust/Cargo.toml -p pipecat-vad
```

Tests compare recurrent Silero probabilities on speech, tones, and silence at
both rates against the actual Python `SileroOnnxModel` class (absolute tolerance
`2e-6`). Loudness oracle values come from `loudness==0.2.0`. Smart Turn checks every
one of 192,000 feature values at `2e-6` and model probabilities at `1e-5`. Existing
fixtures require no Python to test. The tests also cover validation, chunk
alignment, state transitions, five-second resets, volume preservation, real
pipeline speech/idle events, End draining, Cancel cleanup, and setup failure.

Development-only fixture regeneration uses NumPy, `onnxruntime==1.21.0`, and
`loudness==0.2.0` in an isolated Python environment:

```bash
python rust/crates/pipecat-vad/tests/fixtures/smart_turn_generate_fixtures.py
python rust/crates/pipecat-vad/tests/fixtures/generate_silero_oracle.py
```

Oracle scripts record source/model hashes. Speech comes from the tracked
`scripts/provider-watch/assets/speech-16k.wav`; the 8 kHz Silero test decimates
that fixture solely to exercise the 8 kHz model API and does not claim resampler
quality. Tests establish numeric and lifecycle parity for these cases, not
recognition accuracy across languages, microphones, or deployment hardware.

The native turn integration suite in `pipecat-turns` additionally runs the actual
Smart Turn model through a pipeline with synthetic STT/LLM processors. Controlled
predictors exercise delayed completion, resumed speech before cancellation is
acknowledged, physical-stop STT deadlines, and teardown with queued reanalysis.
Run it with the same explicit runtime/model paths as this crate's model tests.
