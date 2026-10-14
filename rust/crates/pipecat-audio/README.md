# pipecat-audio

Native Rust audio primitives for Pipecat. The runtime has no Python bridge,
NumPy, native audio library, model download, or external-service requirement.

## Coverage and contracts

| Area | Implemented behavior |
| --- | --- |
| PCM16LE | Explicit little-endian conversion, validated format and channel frames |
| Mixing | Saturating sample addition, zero-padding the shorter input |
| Gain | Float32 scaling, saturation, truncation toward zero |
| Stereo | Interleave mono channels, truncating to the shorter input |
| WAV | Python-compatible PCM16 writer, drops trailing partial channel frame; strict RIFF PCM16 reader |
| G.711 | mu-law/A-law encode/decode, exhaustively checked against CPython audioop |
| Silence | Inclusive peak threshold of 20; empty input is an error |
| Audible onset | Centered mean-removed RMS windows, signed downmix, sustained-energy gate, original defaults |
| DTMF | Original twelve 8kHz resources; separately named standard dual-sine generation |
| Stream buffering | Arbitrary byte splits, channel alignment, fixed-frame reblocking, recoverable incomplete-tail errors |
| Resampling | Independent Blackman-windowed sinc, antialiasing, phase continuity, fixed rates/channels, flush/reset |

The codec functions convert encoding only. Compose them with `SincResampler`
when sampling-rate conversion is needed. The resampler accepts interleaved PCM
with arbitrary byte splits; stateless functions reject incomplete samples unless
their documentation explicitly permits truncation.

```rust
use pipecat_audio::{ulaw_to_pcm, pcm_to_ulaw, SincResampler};

let pcm = ulaw_to_pcm(&[0xff, 0x80, 0x00]);
let encoded = pcm_to_ulaw(&pcm)?;
assert_eq!(encoded, [0xff, 0x80, 0x00]);

let mut resampler = SincResampler::new(8000, 16000, 1)?;
let mut output = resampler.process(&pcm)?;
output.extend(resampler.flush()?);
# Ok::<(), pipecat_audio::AudioError>(())
```

## Numerical and compatibility boundaries

- G.711 vectors cover all 65,536 PCM16 values for each encoder and all 256
  codewords for each decoder, including negative near-zero quantization
- The bundled 8kHz DTMF data is byte-identical to the Python resources.
  Resampled DTMF preserves duration but is not byte-identical to SOXR
- The sinc filter uses 32 zero crossings at 94% of the lower Nyquist frequency
  with a Blackman window. Input/output ratios are limited to 1/64 through 64
- Each stream retains filter history and lookahead. Start and end edges repeat
  the nearest sample; filter coefficients are normalized to preserve DC
- Output duration is rounded to the nearest frame, exact halves upward.
  `flush` emits the held tail and resets; `reset` drops the tail. There is no
  implicit wall-clock inactivity reset
- Tests require below 1 PCM-unit RMS error against a 1kHz analytic sine at
  48k→8k, 8k→48k, and 44.1k→16k, away from edges. A 10kHz tone at 48k→8k must
  be suppressed below 2 PCM-unit RMS. These are tested examples, not universal
  passband/stopband specifications or equivalence to another resampler
- Onset uses float64 accumulation of float32-downmixed PCM. The original NumPy
  implementation uses float32 reductions, so exactly threshold-adjacent cases
  may differ. Ten original-function fixture cases match exactly
- Configurations reject zero channels, non-finite gains and invalid onset
  parameters rather than relying on undefined numeric behavior

## Not implemented

This crate does not implement SOXR/resampy numerical compatibility, arbitrary-duration
or multichannel BS.1770 integrated loudness, Silero/Krisp/Smart Turn models,
RNNoise/Koala/AI-Coustics filters, background sound-file mixing, or model inference.
The silence/onset helpers are energy tests, not substitutes for these features.

## Verification

```sh
cargo test -p pipecat-audio
cargo clippy -p pipecat-audio --all-targets -- -D warnings
cargo fmt -p pipecat-audio --check
```

Fixtures and their provenance are in `tests/fixtures/README.md`.

## Bounded mono loudness

`AudioVolumeTracker` is the shared 400 ms mono BS.1770 meter extracted from
`pipecat-vad`. It supports 8/16/24/48 kHz PCM16 and retains only the latest window.
The VAD compatibility wrapper still accepts only 8/16 kHz. Measurements preserve
the libloudness 0.2.0 coefficients and histogram quantization; they are recomputed
on read, not cached. Coefficient implementation is MIT licensed; see
[LICENSE-loudness](LICENSE-loudness). Native source-derived numerical tests use
real loudness 0.2.0 vectors, not RMS approximations.
