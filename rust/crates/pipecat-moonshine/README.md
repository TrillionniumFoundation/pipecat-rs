# Native Moonshine STT

`MoonshineService` runs the vendor Moonshine engine inside one owned Rust thread.
`MoonshineSttProcessor` integrates it into the frame pipeline. No Python runtime,
SDK subprocess, microphone, network connection, or automatic download is used.

## Artifacts and platform

The audited implementation supports Linux x86_64 and the native libraries from
the official `moonshine-voice` 0.1.5 manylinux_2_34_x86_64 wheel. It does not install
or import that package. Supply its extracted `libmoonshine.so` and renamed ONNX
Runtime library through `NativeLibraries::from_wheel_directory`. Exact native
binary hashes are checked before loading sealed, immutable in-memory copies.
The wheel's ORT 1.23.2 has a distinct SONAME and symbol version; loading does not
change `ORT_DYLIB_PATH`, global providers, or the runtime used by VAD/Piper.

Provide a `ModelBundle` for each desired language/architecture. Each canonical
asset filename maps to an `Artifact` containing an existing local path and an
independently obtained SHA-256 digest. Native inference reads the verified owned
buffers, which outlive the transcriber; it never reopens model paths. The caller
is responsible for selecting trusted, correctly identified model artifacts.
Models, native libraries, and recorded speech fixtures are not included here.

English models and all streaming models are MIT licensed by Moonshine. Legacy
non-English non-streaming models use the Moonshine Community License. Such
bundles are rejected unless the caller explicitly records an independently
obtained grant with `community_license_granted`; setting this does not obtain a
license or accept terms. See the upstream LICENSE and PROVENANCE.md.

## Minimal integration

```rust,no_run
# use pipecat_moonshine::*;
# use std::sync::Arc;
# async fn example(models: Vec<ModelBundle>) -> Result<()> {
let config = MoonshineConfig::new(
    NativeLibraries::from_wheel_directory("/opt/moonshine-0.1.5"), models,
);
let service = MoonshineService::load(config).await?;
let processor = Arc::new(MoonshineSttProcessor::new(
    service, MoonshineProcessorOptions::default(),
)?);
// Place a 16 kHz mono VAD before this processor in the pipeline.
# drop(processor);
# Ok(()) }
```

Default settings are `small-streaming` and `en`. The six original architecture
names are retained. Regional languages normalize to their base, including
`fil-PH` → `tl`. The pinned vendor catalog determines availability and fallback;
for example English `base-streaming` resolves to `medium-streaming`. Missing
local artifacts for the resolved model produce an error rather than selecting
a different installed model. Language detection, translation, prompts, true
streaming partial transcripts, diarization and word timestamps are not exposed.

The processor accepts signed PCM16 little-endian mono audio at 16 kHz, keeps one
second of pre-roll, appends 500 ms of trailing silence, and transcribes when VAD
stops. `AudioFrames` mode treats each input frame as a complete segment. Audio
passthrough is enabled. Nonempty final transcripts join native lines with a
single space and trim the combined result, retain the segment's user ID, and
include an ISO timestamp, configured language, native result and metrics.

A valid settings update snapshots the resolved language/model for subsequent
segments. The owned worker reloads before that segment, loading a candidate
before freeing the old model. Failure preserves the old model, emits an error
and never labels its output as the requested new model. Settings acknowledgement
is admission, not confirmation that a native reload succeeded. Reload temporarily
holds both old and candidate models. This differs from the Python adapter's
immediate reload and possible settings/model mismatch after a failed reload.

## Lifecycle and bounds

Queue capacity counts queued, running and completed-but-uncollected requests;
overflow is immediate backpressure. Segment duration, model bytes and transcript
bytes are bounded. One ordered, pipeline-tracked relay publishes completions.
Mute and unusable health discard buffered speech; health revisions invalidate
accepted old work. A valid effective settings change can restore usability.
Bot interruptions do not cancel user speech. End/Stop drain accepted work and
also flush an active partial segment; Cancel discards it and cancels queued work.
The processor is single-use.

The C STT API has **no inference/load abort hook**. Cancellation suppresses output
and prevents later work but waits for any native call already running. `drain`
and `shutdown` join through an owned reaper; cancelling an async join waiter does
not detach native work. Last-owner Drop cancels and joins synchronously, which can
block. There is no hard wall-clock shutdown bound, crash isolation or guarantee
against a stalled native engine. Use explicit async shutdown off latency-critical
paths. Native allocator/session memory is additional to the verified model-byte
limit.

## Validation

`cargo test -p pipecat-moonshine` runs deterministic contract tests without models.
For explicit native regression fixtures, set `PIPECAT_MOONSHINE_FIXTURES` to an
external directory holding the extracted wheel files, `small-en-bundle.json` and
`tiny-en-bundle.json` serialized ModelBundles, plus the wheel's `beckett.wav` under
`moonshine_voice/assets/`. Then run:

```sh
cargo test -p pipecat-moonshine --test native_inference -- --ignored --test-threads=1
```

These tests never download anything and exercise both actual model architectures,
reload/rollback, bounded queue cancellation, owned shutdown, settings, VAD and
final transcript delivery. Tiny and small have different genuine recognition
outputs; tests compare stable spoken phrases rather than pretending equal WER.
