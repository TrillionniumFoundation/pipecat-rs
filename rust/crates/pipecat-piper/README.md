# Native local Piper TTS

This is a partial Piper implementation: a Rust-owned CPU synthesis service for
caller-provided Piper ONNX models. It uses native
ONNX Runtime and, only with explicit `espeak-gpl` opt-in, the patched native
Piper/eSpeak C phonemizer ABI. There is no Python, subprocess TTS server,
automatic model download, network client, system installation, or GPU fallback.

## Licensing boundary

New Rust service code is BSD-2-Clause; phonemization conventions adapted from
upstream Piper-phonemize carry MIT attribution in `licenses/`. The crate source
manifest therefore declares `BSD-2-Clause AND MIT`.
**The optional native eSpeak backend is GPL-3.0-or-later.** Enabling it and linking
or distributing an application with it can impose GPL obligations. Dynamic
loading does not establish a licensing exemption. This crate does **not** claim
that the complete enabled distribution is BSD-only or universally compatible.
Piper's current Python in-process implementation itself warns about GPL-3.0.
Read `PROVENANCE.md` and obtain appropriate license review before distribution.

Features:
- Default: configuration, hashing, bounded lifecycle API; loading returns an
  explicit disabled-backend error, with no native library loading
- `onnx`: CPU inference for `phoneme_type="text"` models, without eSpeak
- `espeak-gpl`: `onnx` plus explicitly supplied GPL native eSpeak library/data

No native binaries, model weights, or speech assets are bundled. Each selected
voice has its own model and dataset rights; dataset public-domain status does
not by itself establish weight redistribution rights.

## Provisioning and use

Provide a `PiperConfig` (also serializable as JSON) with:
- Nonblank `voice` identity, local `.onnx` model and JSON config `Artifact`s
- Explicit local ONNX Runtime library `Artifact`, ABI version 20 or newer compatible
- For eSpeak models: `EspeakConfig` with explicit patched native library artifact,
  `espeak-ng-data` directory and its directory fingerprint
- SHA-256 for every artifact, obtained from trusted provisioning; hashes identify
  bytes and are not a sandbox or proof that arbitrary downloaded code is safe

`PiperConfig::new(voice, model, config, runtime)` supplies conservative bounds.
Set `espeak`, and optionally `output_sample_rate`. Model-native sample rate is
preserved when output rate is absent. Sinc PCM resampling supports explicit
output rates. Native eSpeak/Piper ABI qualification currently covers Linux x86-64;
other native builds require separate qualification.

The native library must export `espeak_Initialize`, `espeak_SetVoiceByName`, and
`espeak_TextToPhonemesWithTerminator`; standard eSpeak without the Piper terminator
extension fails explicitly. Model inputs are checked for `input` (int64 rank 2),
`input_lengths` (int64 rank 1), `scales` (float32 rank 1), optional multi-speaker
`sid` (int64 rank 1), dynamic phoneme-sequence width, and float32 mono `output` (rank 3 or rank 4). Actual leading
output dimensions must all be 1. JSON dimensions, scales, phoneme IDs and special
BOS/pad/EOS mappings are validated before inference.

```rust,no_run
# async fn example(config: pipecat_piper::PiperConfig) -> Result<(), Box<dyn std::error::Error>> {
use pipecat_piper::{PiperService, PiperSettings};
let service = PiperService::load(config).await?;
let pending = service.submit("Hello world.", "utterance-1", PiperSettings::default())?;
let audio = pending.result().await?; // Signed PCM16LE mono; no playout claim.
assert_eq!(audio.num_channels, 1);
service.drain().await?;
# Ok(()) }
```

The offline example reads a complete serialized `PiperConfig`; neither it nor
native tests provision artifacts:

```sh
cargo run -p pipecat-piper --features espeak-gpl --example synthesize -- \
  /absolute/path/piper-manifest.json /absolute/path/output.wav 'Hello world.'
PIPECAT_PIPER_MANIFEST=/absolute/path/piper-manifest.json \
  cargo test -p pipecat-piper --features espeak-gpl -- --include-ignored
```

## Lifecycle, cancellation, bounds

One owned native worker serializes inference per service. Admission never waits;
a full queue returns `Backpressure`. The permit covers queued/running requests
and completed-but-uncollected results. Dropping a request cancels it but keeps
capacity occupied until its native execution really exits. Request `result()`
resolves after native acknowledgement, not merely consumer cancellation.

`finish` closes admission; `drain` finishes accepted work and joins; `shutdown`
cancels accepted work and joins. `Drop` cancels and synchronously joins, including
when an asynchronous shutdown waiter is dropped. Prefer `shutdown().await` in an
async application. Native model loading has no abort hook; cancellation waits
for loading to finish. eSpeak calls are cancellable between bounded clauses;
ONNX inference uses `RunOptions::terminate` cooperatively between graph kernels.
Neither is a hard real-time preemption guarantee.

`interrupt` increments the context generation and cancels active work. Queued
and completed older results are discarded; new-generation submissions stay open.
Callers must still fence pipeline/output frames by generation when accepting a
result, because an interruption can occur after an API result has returned.

One process-global mutex owns ALL eSpeak init, voice selection and phonemization
performed by this crate. Its first library/data identity is fixed and resident
for the process lifetime; later services may reuse it but cannot replace it.
Other code in the process must not independently call the same eSpeak global
ABI. ONNX runtime identity is also fixed; its API pointer is compared with the
explicit native library to reject an earlier incompatible `ort` consumer.

Text bytes, normalized phonemes, IDs, queue capacity, worker count, model/config
file bytes, generated PCM, and copied/resampled output are bounded. Directory
fingerprints bound entries, nesting and bytes and reject symlinks, special files
and ambiguous names. File hashes and directory data must remain immutable
between verification and native loading; filesystem mutation is not sandboxed.

**ONNX graph-internal/native allocations happen before output-size inspection.**
Input/output bounds do not make an arbitrary hostile ONNX graph memory-safe or
provide a process RSS hard limit. Use trusted pinned models/libraries/data.
Each result buffers a bounded complete utterance rather than streaming sentences
through a consumer queue. Exhausted/unknown phonemes, malformed graphs, nonfinite
output, and empty audio return errors rather than substituted or canned speech.

## Source behavior and deliberate differences

Source: `src/pipecat/services/piper/tts.py`, `PiperTTSService`.
- Voice is required and remains tied to the explicitly loaded model
- Model-config speaker/rate/default inference scales and int16 normalization are
  preserved; settings are snapshotted per request
- Runtime scale/speaker settings are applied. Loaded model/voice/language remain
  immutable; construct another service rather than silently store unapplied values
- Automatic download/force-redownload and CUDA are explicit unsupported options
- Unknown phonemes fail instead of being silently omitted
- Arabic tashkeel/g2pW preprocessing and current Piper's additional model families
  are not implemented; Arabic voices and inline IPA blocks fail explicitly rather than silently changing their interpretation
- `PiperTtsProcessor` supplies complete-utterance lifecycle, metrics and alignment
  frames; it does not perform physical speaker output or fabricate playback receipts

## Pipeline adapter

Construct `PiperTtsProcessor::new(service, settings, options)` and place a text
aggregator before it for incremental LLM tokens. The loaded service is single-use
and pipeline Start must match its output sample rate. `PiperProcessorOptions`
bounds outstanding speech contexts and PCM frame size (default 1024 samples).
Context admission is immediate and bounded; downstream PCM delivery awaits
finite core and media queue capacity through owned cancellation-aware tasks.
Ordinary unrelated processors retain their fail-fast forwarding policy; this is
not a promise of pressure propagation through arbitrary legacy pipeline hops.

Native request admission follows bounded alignment registration, before a relay
is spawned. End/Stop can close native admission and drain accepted work. Response
start, audio, completion and response end remain ordered, including cancelled
middle contexts and uninterruptible survivors. Downstream compact/detailed Flush
probes are ordered on both passes. Final-boundary routing remains the core
coordinator's responsibility. Cleanup cancels and joins the native worker.

Three-channel input preserves synthesis, display and original source text,
source-frame anchor, destination and explicit spacing. Completion emits
`Unreported` / `Succeeded` alignment diagnostics. It never emits spoken tokens
or commits spoken history from successful synthesis, including when a test
backend supplies simulated Played receipts. Output receipts are separate.

Speaker and inference-scale settings apply only to subsequent accepted contexts.
Loaded model/voice/language changes fail explicitly. An effective validated
settings change restores usability; an invalid or no-op change does not.
Zero-audio watchdog counts completed native contexts, including recoverable
backend errors, but never preflight rejection, skipped text or cancelled work.
Health revision also cancels native ownership while a relay waits for an earlier
blocked response/control fence. Health revision and causal generation fence native
result, alignment and every pending capacity-waited frame. Explicit health restoration resets the watchdog
without replaying old work.

Post-executor-reset reconstruction requires a fresh full qualification; see
PROVENANCE.md. Native tests only use explicitly supplied local assets and never
download them.
