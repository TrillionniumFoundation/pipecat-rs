# Native Piper qualification provenance

Current Piper behavior was also checked against its official v1.3.0 sources:
https://github.com/OHF-Voice/piper1-gpl/blob/v1.3.0/src/piper/config.py and
https://github.com/OHF-Voice/piper1-gpl/blob/v1.3.0/src/piper/voice.py
(default scales, NFD text mode, normalization, explicit IPA and Arabic caveats).

Qualification date: 2026-10-09. All assets below are developer-local; none are
included in the repository. Public retrieval required no new explicit legal
acceptance, account, credential, or gated-model approval.

## Official native phonemizer

- Publisher: https://github.com/rhasspy/piper-phonemize
- Release: https://github.com/rhasspy/piper-phonemize/releases/tag/2023.11.14-4
- Release target: `fccd4f335aa68ac0b72600822f34d84363daa2bf`
- Asset: `piper-phonemize_linux_x86_64.tar.gz`, 25,676,144 bytes
- Archive SHA-256: `3fb3d58b4ac42bd69d38948acdbeab335eee7e599984169d28fb0082496649ad`
- Used native library SHA-256: `3767caddfe3b90f8f65b50a8d1f66f58f52e0367ba4a385599e0aa3391446e80`
- eSpeak data fingerprint (algorithm `sha256_directory`):
  `bba750d12c77a7714b579dc8a34e205417174963cfd05a54da8edd4031f09926`
- Patched C ABI checked against release `include/espeak-ng/speak_lib.h` and actual
  exported symbols. Upstream source:
  https://github.com/rhasspy/piper-phonemize/blob/fccd4f335aa68ac0b72600822f34d84363daa2bf/src/phonemize.cpp
- Piper-phonemize source license is MIT (`licenses/piper-phonemize-MIT.txt`).
  The bundled eSpeak header specifies **GPL version 3 or later**. Its fork license:
  https://github.com/rhasspy/espeak-ng/blob/master/COPYING
- The historical release build used a moving eSpeak branch URL, so its exact
  native source commit is not asserted; archive/library/data hashes pin the
  artifact actually qualified. This is a provenance limitation, not an assertion
  of fully reproducible source-to-binary provenance.

Only native eSpeak is loaded from that archive. Its C++ Piper-phonemize wrapper,
bundled old ONNX runtime, executable, and Python bindings are not used.

## ONNX runtime

Existing workspace native Microsoft ONNX Runtime 1.21.0, API 20-compatible:
https://github.com/microsoft/onnxruntime/releases/tag/v1.21.0

Library SHA-256:
`a0df84fae0f09d9eca3d6919f685ba435f1ae6cb628e128b69bcf2a2b965504c`

Official source license: MIT, https://github.com/microsoft/onnxruntime/blob/v1.21.0/LICENSE

Pinned Rust wrappers reuse already locked dependencies: ort/ort-sys 2.0.0-rc.9,
ndarray 0.16.1, libloading 0.8.9, sha2 0.10.9, unicode-normalization 0.1.25.
Native loading has no automatic download fallback.

## Voice

- Model: `en_US-ljspeech-medium`, source sample rate 22,050 Hz
- Publisher repository: https://huggingface.co/rhasspy/piper-voices
- Pinned revision: `c10ece1aade47bb51c153c893d14e5bf8e5b7117`
- Model card:
  https://huggingface.co/rhasspy/piper-voices/blob/c10ece1aade47bb51c153c893d14e5bf8e5b7117/en/en_US/ljspeech/medium/MODEL_CARD
- ONNX bytes: 63,531,379
- ONNX SHA-256: `6f52a751e2349abe7a76735eb09dc1875298c77ea2342ffd2fef79ff81b87f22`
- Config SHA-256: `141d612cc0a95ed7efc1ca936b845c2364967f2e9217c5dbfcf69fc4d6c65860`
- The voice card declares its LJ Speech **dataset** public domain. Dataset source:
  https://keithito.com/LJ-Speech-Dataset/
- A distinct explicit license governing redistribution of these particular
  weight bytes was not established by the model card. Do not infer unrestricted
  weight redistribution from dataset status. No weights are bundled or published.

## Historical pre-reset execution, not playback

Text: “The quick brown fox jumps over the lazy dog. This speech was generated
locally by a native Rust service.”

Rust native eSpeak→Piper ONNX CPU inference generated 106,812 mono PCM16 samples
resampled to 16,000 Hz (6.67575 s). A separate native Whisper tiny.en run transcribed:
“The quick brown fox jumps over the lazy dog. His speech was generated locally by
a native rust service.”

Whisper rendered “This” as “His” and lowercased “Rust”; these are transcription
deviations,
this is independent intelligibility evidence, not exact text equality. Synthesized
samples and the transcription were generated during qualification, never canned.
This establishes local synthesis and a separate speech-recognition check only;
it does not establish physical speaker playback, end-to-end pipeline delivery,
or a playback receipt.

The final reviewed executable's generated WAV SHA-256 is
`a9c8ef25739eea099715c4a469a25974ba78d9bf593ea59f57015dba0b12f91c`.
The source model has stochastic synthesis noise, so repeated correct runs need
not produce identical durations or waveforms. The artifact hash identifies this
specific observed run, not a universal golden PCM expectation.


## Post-reset reconstruction qualification boundary

The pipeline adapter and its regression harness were reconstructed after the
2026-10-09 executor replacement from retained contracts. They are new bytes, not
an exact recovery of the lost e8754da adapter or 8027acd harness commits. Their
previous whole-workspace/native logs and WAV artifacts were lost. Prior passes
do not qualify this reconstruction; current test results must be recorded anew.
The standalone intelligibility transcript above remains historical evidence.
Pinned assets were subsequently reprovisioned from the same official sources;
all model/config/runtime/native library hashes and the eSpeak directory fingerprint
were verified again before fresh post-reset execution.

The cancellation regression has two deliberately separate cases. Active native
work signals entry before the health-flip callback; completion requires both
backend cancellation and reclaimed admission permits before cleanup. Queued work
sits behind a controlled native blocker; the stale relay must release its failed
response fence before the blocker is released, then reclaim all permits without
calling the stale backend. A callback-only flag would wrongly reject correct
queued-before-entry cancellation. No timeout inflation or retry substitutes for
these ownership assertions.

Independent recovery review also reproduced stale native ownership behind a
blocked response-start fence. Native admission precedes relay ordering, so the
health subscription must cover the predecessor wait as well as synthesis and
PCM admission. The active test observes native cancellation and returned permits
before output drainage; the queued test observes actual relay completion before
releasing its native blocker, then checks zero stale backend calls and full
permit recovery. Neither test relies on a fixed sleep as cancellation evidence.

## Fresh post-reset native pressure evidence

On 2026-10-09 the explicit local model was run with `noise_scale=0`, `noise_w=0`,
16,000 Hz mono PCM16 output, and the sentence “The native pipeline keeps every
audio sample while the recorder waits. ” repeated six times. A separate request
on the same loaded service produced the reference samples. The actual pipeline
used default core/media count capacities of 128, the 2 MiB media byte bound,
1024-sample source chunks, and a recorder delaying each write by 5 ms.

The first fresh run (23:55:27–23:55:48 UTC, processor source SHA-256
`f2aedcaf9c0cf3966f77d0e3113cbda80eeb9b90cd2c2a87b031268c4947fa32`)
produced 789,106 native bytes in 386 source chunks. The recorder received the exact
reference prefix and exactly 654 final zero-padding bytes: 789,760 bytes total,
617 output writes. No errors or dropped frames were reported. This pressures the
count bound; the utterance is below 2 MiB and does not establish byte-cap saturation.

- Native PCM SHA-256: `ae4f9c62ab9f4bc5d24d6918eb059d25dae598d464db59ff091e1d757bcf16b8`
- Recorder PCM SHA-256: `393cfbbb2c53242ad0cd7c893d839e861ede48707fb21ab126da1881916cd0c1`
- Fresh WAV SHA-256: `30ee193d49227b497a398a50390ab1d13188807b6179647f165d02230dc1103f`

These hashes were observed in new execution, not inherited as acceptance from
lost artifacts. The recorder returns `Discarded`; alignment is `Unreported` with
`Succeeded`, and spoken-history commits remain zero. This is synthesis and exact
pipeline delivery evidence, not physical speaker playback. No fresh independent
speech-recognition run is claimed. The health predecessor fix and final source
replay are separately qualified in the recovery logs; the first-run identity is
retained above rather than retroactively assigning it to changed source.

Runtime and tests never download assets. Developer-local provisioning is explicit;
no native binaries or model weights are bundled. Do not infer weight redistribution
rights from the dataset license.
