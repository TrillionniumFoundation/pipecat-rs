# Native Smart Turn v3

`SmartTurnModel` performs CPU ONNX inference on mono PCM16 at 16 kHz. Both the
ONNX Runtime library path and model path are explicit; loading and prediction
never invoke Python or download assets. Use the runtime configuration documented
by the crate and load a compatible `smart-turn-v3.2-cpu.onnx` file.

## API

```rust,no_run
use pipecat_vad::{RuntimeOptions, SmartTurnModel};

let runtime = RuntimeOptions {
    library_path: "/path/to/libonnxruntime.so".into(),
};
let model = SmartTurnModel::load(&runtime, "/path/to/smart-turn-v3.2-cpu.onnx")?;
let result = model.predict_pcm16(&pcm16_samples, 16_000)?;
// result.probability is the model's sigmoid output.
// result.complete is true only if probability > 0.5.
# Ok::<(), pipecat_vad::VadError>(())
```

Inference uses sequential ONNX execution, one inter-op thread, one intra-op
thread, and full graph optimization. Inputs with sample rates other than 16 kHz
return an error. PCM16 is converted with a divisor of 32768. The input keeps its
last 128,000 samples; shorter input is padded at the beginning with zeroes.

`WhisperFeatureExtractor::extract` is also public. Like the Python feature helper,
it pads at the **end** and truncates to the **first** 128,000 samples. The model
wrapper applies its endpoint window first, so the two interfaces intentionally
have different windowing semantics.

## Feature computation

The native implementation follows the repository's
`src/pipecat/audio/turn/smart_turn/_whisper_features.py`:

- Float32 zero-mean/unit-variance normalization with variance epsilon `1e-7`
- Float64 periodic Hann window of 400 samples
- Centered, reflect-padded 400-point FFT with a 160-sample hop
- Float64 power and 80 Slaney-normalized mel bands spanning 0–8000 Hz
- Mel floor `1e-10`, log10 scaling, removal of the trailing frame, global
  maximum-minus-eight clipping, and `(log_mel + 4) / 4` scaling
- Row-major float32 output with shape `(80, 800)`

The normalized waveform reduction preserves NumPy's float32 pairwise summation
order. The FFT and transcendental functions may vary at floating-point rounding
precision between implementations and platforms; the parity contract is numeric
rather than a claim of universal bit identity.

## Analyzer policy

`SmartTurnAnalyzer` owns the model and reproduces the audio policy in
`BaseSmartTurn`. `SmartTurnBuffer` exposes the same policy separately with
explicit monotonic timestamps, and `SmartTurnClock` makes analyzer timing
injectable for deterministic tests.

```rust,no_run
use pipecat_vad::{EndOfTurnState, SmartTurnAnalyzer, SmartTurnParams};
# async fn example(model: pipecat_vad::SmartTurnModel, pcm: &[i16]) -> pipecat_vad::Result<()> {
let mut analyzer = SmartTurnAnalyzer::new(model, SmartTurnParams::default())?;
analyzer.update_vad_start_secs(0.2)?;
let appended = analyzer.append_pcm16(pcm, true)?;
// At a VAD stop, append subsequent audio with is_speech=false and analyze:
let analysis = analyzer.analyze_end_of_turn().await?;
if analysis.state == EndOfTurnState::Complete {
    // Pass the semantic decision to the application's turn-stop strategy.
}
analyzer.cleanup().await?;
# Ok(())
# }
```

The defaults are 3 seconds of silence, 500 ms of pre-speech audio, and an
8-second maximum model segment. The fixed model input rate is 16 kHz. The
standalone buffer and test-predictor constructor support other explicitly
configured rates; the real model rejects those rates rather than resampling.

- Speech resets accumulated silence and records the first speech timestamp
- Pre-roll begins at first speech minus `pre_speech_ms` and the VAD start delay;
  selection includes whole chunks whose timestamps meet that boundary
- Silence duration is accumulated from PCM sample counts, independent of wall
  time between frames. Reaching `stop_secs` returns COMPLETE from append and
  clears the buffer, speech trigger, start time, and silence counter
- Maximum duration selects the final samples for prediction; it never ends a
  turn on its own. Incomplete model results retain audio for later analysis
- A complete model result clears the same state. `clear()` also invalidates
  outstanding predictions for externally ended turns; VAD pre-roll settings
  survive the clear
- Empty input analysis returns INCOMPLETE without metrics. After an append has
  already completed and cleared a turn, an immediate analysis is empty: callers
  must preserve the COMPLETE returned by append
- Metrics contain the real probability and elapsed preprocessing/inference time,
  excluding queue delay. Model timeout returns COMPLETE without metrics;
  ordinary model errors propagate and never become synthetic predictions

Only the newest `max_duration_secs * sample_rate` PCM samples are retained,
which preserves the exact selected model input while bounding continuous and
unpaced audio. Chunk metadata is bounded separately (8,192 entries by default).
Excessive fragmentation fails explicitly. Invalid timings, non-monotonic input
clocks, and odd-byte PCM16 frames fail without accepting the input. A zero clock
origin is valid; an optional timestamp avoids conflating it with an unset time.

### Inference ownership and cancellation

Inference runs on one lazy, model-owning native thread with capacity for one
additional queued snapshot. Queue overflow returns an explicit error. Audio
buffering is separate from the background call: `begin_analysis()` returns a
`PendingSmartTurnAnalysis`, whose awaited result is committed with
`apply_analysis()`. This permits audio ingestion while inference runs. New
speech and external clears cancel pending work and invalidate older snapshots;
additional silence does not invalidate an otherwise current prediction.

Dropping a pending result cancels its work. Cancellation is checked before and
after bounded feature extraction, and active ONNX execution is terminated using
per-request `RunOptions`; the next request uses fresh options. An optional
`set_inference_timeout()` deadline covers both queueing and inference and follows
the source timeout completion policy. Explicit cancellation always remains an
error, even when it races with that deadline.

`cleanup().await` cancels pending jobs, closes the queue and joins the actual
native thread. It preserves audio state and restores the owned model so the
analyzer remains reusable. Cancelling the cleanup future still permits the join
task to finish; inference cannot restart until the old owner returns its model.
Dropping an analyzer also cancels and joins synchronously, so async callers
should prefer explicit cleanup.

This analyzer does not implement the full `TurnAnalyzerUserTurnStopStrategy`:
VAD event routing, final-transcript gating, STT absolute-deadline handling,
interim-transcript invalidation, and output frame signaling remain the turn
controller's responsibility. `pipecat-turns::SmartTurnUserAggregator` provides
that native processor integration and owns its asynchronous prediction/timer
supervisor; see [the integration contract](../pipecat-turns/README.md#native-smart-turn-integration).
A positive model result on silence is not a VAD result. The analyzer neither fabricates spoken acknowledgements nor emits
conversation frames.

## Test fixtures

`tests/fixtures/smart_turn_generate_fixtures.py` runs the repository's Python
feature helper and ONNX graph to generate reference data. Python dependencies
are needed only to regenerate those files, never to build, test against existing
fixtures, or run the Rust implementation.

The cases are the repository's `scripts/provider-watch/assets/speech-16k.wav`,
one second of a 440 Hz PCM16 tone, and one second of PCM16 silence. The checked-in
features contain all 64,000 values per case. Tests enforce a maximum absolute
feature error of `2e-6` and a model probability error of `1e-5`; they also check
both windowing directions, empty input, invalid float samples, sample-rate
rejection, and the `> 0.5` decision.

From the repository root:

```bash
cargo test --manifest-path rust/Cargo.toml -p pipecat-vad smart_turn

PIPECAT_ONNX_RUNTIME=/path/to/libonnxruntime.so \
PIPECAT_SMART_TURN_MODEL="$PWD/src/pipecat/audio/turn/smart_turn/data/smart-turn-v3.2-cpu.onnx" \
cargo test --manifest-path rust/Cargo.toml -p pipecat-vad \
  smart_turn_real_model_matches_python_probabilities -- --nocapture
```

The real-model integration tests run by default and requires local assets. Its
test-only fallback runtime is
`/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0`; the fallback model is the
repository asset shown above. On other systems, set the two environment
variables to compatible files. Missing or incompatible assets fail clearly;
the test does not skip inference or substitute test outputs. Production loading
always requires explicit runtime and model paths.

`generate_smart_turn_policy_oracle.py` compiles the actual tracked Python
`BaseTurnAnalyzer` and `BaseSmartTurn` class bodies unchanged, with a scripted
predictor and explicit clock. Its JSON traces record source hashes, append
states, speech/silence tracking, exact run-length-encoded model inputs,
completion/timeout clearing, and timing metrics. Rust replays those traces.
Additional native tests exercise long unpaced input, validation, bounded queues,
stale-result rejection, cancellation, timeout, cleanup and reuse. Controller
integration tests use the actual tracked model, compare probabilities with the
Python fixtures, terminate an active ONNX run, and verify subsequent reuse.

To regenerate the feature oracles in an isolated Python environment:

```bash
python -m venv /tmp/smart-turn-oracle
/tmp/smart-turn-oracle/bin/pip install numpy==2.3.5 onnxruntime==1.21.0
/tmp/smart-turn-oracle/bin/python \
  rust/crates/pipecat-vad/tests/fixtures/smart_turn_generate_fixtures.py
```

The policy oracle additionally requires Pydantic:

```bash
python rust/crates/pipecat-vad/tests/fixtures/generate_smart_turn_policy_oracle.py
```

The feature generator records dependency versions, source/model/speech SHA-256 values,
and file encodings in `tests/fixtures/smart_turn_oracle.json`. Baseline model
probabilities are speech `0.9823201894760132`, tone `0.09577557444572449`, and
silence `0.9870367050170898`.

## Provenance and licenses

The verified model is the existing repository asset at
`src/pipecat/audio/turn/smart_turn/data/smart-turn-v3.2-cpu.onnx`:

- Size: 8,679,182 bytes
- SHA-256: `2bb026316b14a660486a75b1733cd3fbab8c2fd0314dc9af7be49f8cca967e4f`
- Upstream: [Pipecat Smart Turn v3 model file](https://huggingface.co/pipecat-ai/smart-turn-v3/blob/main/smart-turn-v3.2-cpu.onnx), which publishes the same SHA-256
- Model license: BSD-2-Clause, as declared by the [model card](https://huggingface.co/pipecat-ai/smart-turn-v3/blob/main/README.md?code=true) and [Smart Turn project license](https://github.com/pipecat-ai/smart-turn/blob/main/LICENSE)

The Rust feature extraction adapts the repository's vendored Whisper feature
helper, which credits Hugging Face Transformers' `feature_extraction_whisper.py`
and `audio_utils.py`:

- Copyright 2022 The HuggingFace Inc. team
- Copyright 2023 The HuggingFace Inc. team and the librosa & torchaudio authors
- Apache License, Version 2.0; a copy is included as `LICENSE-APACHE`
- Pipecat adaptations: Copyright (c) 2024–2026, Daily, BSD-2-Clause

The Rust implementation preserves that attribution and identifies its native
Rust adaptation in the source header. The project's BSD license remains in the
repository root. The crate's combined license metadata reflects these portions.
