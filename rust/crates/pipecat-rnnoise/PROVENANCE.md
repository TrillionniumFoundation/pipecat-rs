# RNNoise native provenance and compatibility boundary

## Pinned Pipecat behavior

Source baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/audio/filters/rnnoise_filter.py`, and its RNNoise/resampling/cancellation
tests. These files were inspected as text, without importing or executing Python.
The source filter creates RNNoise at 48 kHz, buffers through `denoise_chunk`,
preserves state across enable toggles, and destroys the engine on stop without
flushing. Python enabled filtering rejects per-call odd-byte PCM; Rust explicitly
extends this to a byte stream, retaining split samples until a complete native
frame is available and discarding any odd tail on stop. Per-call odd-byte
validation parity is not claimed. Non-48-kHz paths use SOXR streaming resamplers
with QQ default quality. The source tests that mock normalized float output prove
orchestration only.

The baseline locks pyrnnoise 0.4.5. The Linux wheel inspected as a ZIP archive was:

- [pyrnnoise 0.4.5 wheel](https://files.pythonhosted.org/packages/f1/8b/6498de7ef0a4670bbeae951bec9cfd2386e57ef386f53eec28e75be39790/pyrnnoise-0.4.5-py3-none-manylinux1_x86_64.whl)
- SHA-256: `c095114532f9fa21f621084c879d72c93aaa0c9dbe38b662a63577bada2c214e`
- Inspected files: `pyrnnoise-0.4.5.data/purelib/pyrnnoise/{rnnoise,pyrnnoise}.py`

In that actual wrapper, `process_mono_frame` casts int16 input directly to float32,
calls the C process function, and returns int16. `denoise_chunk` uses an int16 audio
graph and returns these int16 frames. The float values at the C boundary therefore
retain PCM amplitude; they are not normalized to ±1. The Pipecat comment saying
normalized float output does not describe this locked wrapper's output. Multiplying
native output by 32767 would be a scaling bug. Public wrapper source is also
available at [pyrnnoise/rnnoise.py](https://github.com/pengzhendong/pyrnnoise/blob/master/pyrnnoise/rnnoise.py),
but the wheel hash above, not the moving branch, pins the inspection.

## Qualified official source and built-in model

- Upstream: [Xiph RNNoise](https://github.com/xiph/rnnoise)
- Release: v0.1.1, commit `6cbfd53eb348a8d394e0757b4025c6ded34eb2b6`
- [Exact source archive](https://codeload.github.com/xiph/rnnoise/tar.gz/6cbfd53eb348a8d394e0757b4025c6ded34eb2b6)
- Archive SHA-256: `5900832175a507e1ee5d4d596ff2c2b3a4eb66c59d2a12d33095c79e673e953d`
- Built-in model: committed `src/rnn_data.c`, SHA-256
  `7b90ed18f065a34597d80b4c00e140a7c5eb6380ef8c9c6bf265384c944ae708`
- [Public C header at the pinned commit](https://github.com/xiph/rnnoise/blob/6cbfd53eb348a8d394e0757b4025c6ded34eb2b6/include/rnnoise.h)
- All qualification source inputs, including headers/model, are hashed in
  `qualification/source.sha256`

The exact source tree distributes its committed generated model together with
[root BSD-3-Clause COPYING](https://github.com/xiph/rnnoise/blob/6cbfd53eb348a8d394e0757b4025c6ded34eb2b6/COPYING).
That is the licensing evidence for this qualification profile; the generated model
has no separate per-file license declaration. No independent model-specific grant,
training-data equivalence, or newer-model licensing conclusion is asserted.
The Rust crate does not redistribute the C library or weights. It accepts an
explicit caller-provisioned library whose built-in model is covered by its hash.
Redistributors of upstream source/binaries must preserve applicable notices.

Newer RNNoise separately downloaded model weights have an unresolved upstream
license-clarification request, [Xiph issue #284](https://github.com/xiph/rnnoise/issues/284).
A code license or training-data license is not evidence of a separate weights
license. This crate does not fetch or bundle those weights. Version 0.1.1 is a
qualification profile, not current pyrnnoise 0.4.5 model/numerical parity.

## Native comparison and test data

`qualification/reference.c` is an independently written direct C ABI harness using
the exact library also selected by Rust. It generates three seconds of deterministic
PCM: harmonic sine bursts plus xorshift uniform noise, with noise-only regions.
There are no human recordings or third-party speech/noise corpora. The reference
uses explicit little-endian PCM and PCM-amplitude float buffers with saturating
truncation. The fixture, reference, and compiled library hashes are written to the
qualification directory's `identity.txt`.

The C source is unmodified; the script compiles the sources listed by upstream's
`Makefile.am` using `cc -O2 -std=gnu99 -fPIC -shared` and `-lm`. Runtime comparison
uses all output frames, including algorithmic startup latency. It does not copy
the upstream demo's initial-frame skipping into the streaming filter. Non-48-kHz
checks qualify native sinc orchestration only, never SOXR parity.
