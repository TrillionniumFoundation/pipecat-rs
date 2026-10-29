# HMI native follow-up — 2026-10-29

This is a selected-path evidence record, not an overall release approval.
It continues Pipecat `cc4f72efc64bc65c02bad879077e69bc70904d9d` and Hepta
`965e1b26217378bbe7378aefd5c9c8923eb1a6e7` in isolated checkouts.
No production identity/key, thread, conversation database or tool retry is created.

## Changes

- One completed Hepta response may contain multiple AgentMessage items. The
  paired bridge now admits up to 16384 UTF-8 bytes as one once-claimed speech
  request. The local endpoint validates the whole request, renders <=4096-byte
  UTF-8/phrase segments serially, and keeps the same full identity and epoch.
  Over-limit text remains in its terminal outcome; it is never truncated.
- Full sentence PCM previously competed against a 1 MiB queue and five-second
  device write deadline. The presentation port now writes <=250ms PCM chunks,
  checks the ownership/turn/epoch fence before every write, and accumulates only
  actual CPAL callback-submission receipts. The last callback timestamp remains
  a prediction, not acoustic playback proof. Mid-response failures retain segment
  context and leave the aggregate completion unknown; no segment is auto-retried.
- Explicit native tests exercise a >5-second Chinese response, interrupt during
  submission, stale synthesis rejection and a fresh turn; another reuses actual
  Chinese Piper/Whisper models for clean/noisy fixture transcription and checks
  the retained source digest/length excludes decoder padding.

## Execution environment and boundaries

The authorized Linux device has Rust 1.99.1, ALSA 1.2.11 and ALC897 input/output.
Builds use isolated targets, two jobs and nice 10. A three-second, 16 kHz mono
physical capture returned 48000 frames, peak 2002, RMS 283.2 and 47991 nonzero
samples. This establishes a capture operation, not intelligible speech or echo
acceptance. Private microphone audio is not included in this repository.
No running Agentd process was found on this device during the bounded check.
No production owner/signing authority is inferred from matching identity labels.

## Artifact provenance

| Artifact | Pinned source / SHA-256 |
| --- | --- |
| Multilingual Whisper base | `ggerganov/whisper.cpp@5359861c739e955e79d9a303bcbc70fb988958b1`, `ggml-base.bin`; `60ed5bc3dd14eea856493d334349b405782ddcaf0028d4b5df4088345fba2efe` (upstream LFS digest) |
| Chinese Piper model | `rhasspy/piper-voices@5041a7a49ae5b3a0f30e6837769eb03fdbb231bd`, `zh/zh_CN/huayan/medium/zh_CN-huayan-medium.onnx`; `9929917bf8cabb26fd528ea44d3a6699c11e87317a14765312420be230be0f3d` (upstream LFS digest) |
| Piper config | Same pinned revision; `d521dc45504a8ccc99e325822b35946dd701840bfb07e3dbb31a40929ed6a82b` |
| ONNX runtime archive | Official Microsoft release `v1.21.0`, `onnxruntime-linux-x64-1.21.0.tgz`, upstream asset size 7585530; downloaded SHA `7485c7e7aac6501b27e353dcbe068e45c61ab51fbaf598d13970dfae669d20bf` |
| Runtime library | That archive's `libonnxruntime.so.1.21.0`; `3c8181b339025dd18899916357e74290029722e1d5a5778b4071a733f813420d` |
| eSpeak library/data | Official `rhasspy/piper-phonemize` release `2023.11.14-4`; archive `3fb3d58b4ac42bd69d38948acdbeab335eee7e599984169d28fb0082496649ad`; library `3767caddfe3b90f8f65b50a8d1f66f58f52e0367ba4a385599e0aa3391446e80`; data fingerprint `bba750d12c77a7714b579dc8a34e205417174963cfd05a54da8edd4031f09926` |
| Silero | Existing repository model; `597d30b3ec076608d059477bb14cfeffdf951bf5cae370d38f65d33bbfe82004` |

The historical runtime hash `a0df…` did not match this official release library.
Both failed comparisons were retained; the final manifest pins the actual library
from the official size-checked archive. A partial archive extraction was not used
for inference; the owned extraction directory was replaced by the complete one.
SHA pins identity; provenance comes from the named upstream sources, not from
computing a digest alone. Native eSpeak remains the explicit `espeak-gpl` opt-in.

Hepta companion change is pushed as
[`7bbdd715957e72274bc13209c4a619913e880129`](https://github.com/TrillionniumFoundation/hepta-private-ci/commit/7bbdd715957e72274bc13209c4a619913e880129).
Machine-readable observations and execution-log digests are in
[hmi-native-evidence-20261029.json](hmi-native-evidence-20261029.json).

## Acceptance status

| Gate | Observed result |
| --- | --- |
| Existing-identity HMI adapter software | 46/46 Hepta adapter tests pass through `just test --locked -p codex-hepta-codex-adapter --cargo-profile dev-small`; includes the >4096-byte multi-item response, once-only claim, source and cancellation/reconnection cases |
| Default Pipecat software tests | 23 pass (6 store, 4 presentation lifecycle, 11 session, 2 segmentation); feature suite adds two gate tests for 25 passes, with the two native tests separately executed in debug and release |
| Static checks | Pipecat all-target `local-speech` Clippy with `-D warnings` passes; Hepta scoped `just fix` and `just fmt` exit 0. The first Pipecat lint caught the new native test's `chunks_exact_mut(2)`; it was corrected to `as_chunks_mut::<2>().0` without suppressing the lint |
| Native selected-path compile | `local-speech` example checks and feature tests compile on Rust 1.99.1 |
| Real Chinese models/source binding | Explicit native test passes: four phrases, each clean and reproducible low-amplitude additive noise; all eight retained source digests/byte lengths exclude decoder padding; verified files explicitly released by the isolated test consumer |
| Chinese fixture CER | Ignoring punctuation, clean 2/47 = 4.26%; noise 1/47 = 2.13%. This tiny synthetic fixture is not human-speech or acoustic-noise acceptance |
| Debug STT + source retention latency | n=8, nearest-rank p50 3771098us, p95/p99 6017382us. No TTS/model startup/device/Hepta latency is included |
| Debug process RSS | Samples 551132, 552504, 565908, 565912, 565660, 565660, 565788, 565792 KiB; eight samples do not establish a long-run leak bound |
| Physical output callback path | Explicit `alsa:plughw:CARD=0,DEV=0`, 48 kHz mono: >5-second Chinese PCM fully submitted; interrupt pending submission, reject stale synthesis, admit a fresh turn and submit new PCM; native test passes in 50.02 seconds. No audibility or device-playback receipt is asserted |

Release qualification also passes both explicit native tests. STT plus source
retention (n=8) has nearest-rank p50 2506482us and p95/p99 4637607us. Release
fixture CER, ignoring punctuation, is 2/47 (4.26%) for both clean and noise.
RSS samples are 548200, 549508, 552796, 552808, 553912, 553916, 554112,
554112 KiB. The release physical-output/interruption test completes in 30.26
seconds. This total is test duration, not an end-to-end turn latency. No
human-speech, acoustic-echo or sustained-growth acceptance is inferred.

The first output qualification incorrectly reused the source test's capture
generation and was rejected by the existing store rotation guard. The output-only
manifest now has no capture store; that guard was not weakened. A second run
reached the fresh-turn step but the test redundantly called `open_output` after
`interrupt` had already reopened the stream. The test now uses the actual API
lifecycle. Both failures remain in the private execution logs.

Synthetic TTS-to-STT timings
are not microphone-to-Hepta-to-speaker latency. A nonempty transcript is not a
Chinese quality score. Physical echo/noise, unplug/replug, permission revocation,
model-process crash, real owner restart/reconnect and sustained resource/latency
qualification remain separate gates. Neuron shadow stays inactive until the
adopted deterministic path has passed those gates. Privacy, permissions and
emergency stop never become learned controls.

## Reproduction

From `rust`, using the explicit private manifests and immutable artifacts above:

```sh
cargo +1.99.1 test -p pipecat-hepta --features local-speech --tests --locked
PIPECAT_LOCAL_SPEECH_CONFIG=/private/capture-qualification.json cargo +1.99.1 test -p pipecat-hepta --features local-speech --lib chinese_tts_to_stt_records_clean_and_noisy_model_results --locked -- --ignored --nocapture --test-threads=1
PIPECAT_LOCAL_SPEECH_CONFIG=/private/output-qualification.json cargo +1.99.1 test -p pipecat-hepta --features local-speech --lib long_chinese_output_survives_device_deadline_and_interrupt_fences_remaining_pcm --locked -- --ignored --nocapture --test-threads=1
```

The capture test requires an existing private media root and a fresh monotone
capture generation on every reopen. The output-only test uses `media_store:null`;
it does not open capture. These isolated qualification labels and manifests are
never production device grants. Release results must be identified separately
from the debug values above.
