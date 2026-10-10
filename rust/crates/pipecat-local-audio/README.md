# Native local audio

`pipecat-local-audio` provides local device capture and speaker submission through
[CPAL 0.18.2](https://docs.rs/cpal/0.18.2/cpal/). It has no Python, PyAudio,
PortAudio, provider connection, or credential requirement.

## Capture

`LocalAudioInput` implements `pipecat_media_transport::InputBackend`.
`LocalAudioTransport::input()` returns the shared native `InputTransport`
processor, with its filtering, bounded ingress, rate conversion, mute, delayed
streaming, and pipeline-owned cleanup behavior. The wrapper does not configure
an optional filter itself; applications can construct `InputTransport` directly
with `LocalAudioInput` when they need one.

Opening prepares a native stream but does not start recording. The capture task
starts it, owns the receiver, and drops/joins it on cancellation or completion.
For standalone use, `capture_stream()` returns a `LocalAudioCapture` owner;
`next_frame().await` yields PCM16LE `InputAudio` frames. Dropping this owner stops
capture. Close the backend before opening another session.

Native callback lengths need not be 20 ms. The callback reblocks interleaved
samples into `2 * floor(sample_rate / 100)`-frame chunks, matching the Python
source's period calculation. All supported native signed/unsigned integer
sample types, including CPAL's 24-bit types, and float32/float64 convert to
PCM16LE. Finite out-of-range float samples saturate; non-finite samples fail the
stream. Incomplete channel frames fail instead of silently corrupting stereo.
A final incomplete capture chunk is discarded on shutdown.

## Qualified output

`LocalAudioTransport::output()` returns `LocalAudioOutput`, a lower-level native
speaker writer. It is **not a pipeline output processor** and deliberately does
not implement the media crate's `OutputBackend` trait.

`write_audio()` or `write_frame()` waits until every PCM sample has been copied
into native CPAL callback buffers and returns a `SubmissionReceipt`. A receipt
is not merely queue admission, but it is also **not proof of native-write
success, DAC completion, speaker output, or acoustic playback**. Its playback
end time is explicitly a CPAL prediction.

CPAL invokes the application callback before the platform writes the buffer.
For example, its ALSA backend can skip a write when `writei` returns `EAGAIN`
before making progress. Application callback completion cannot detect that
case. See CPAL's [ALSA implementation](https://docs.rs/crate/cpal/0.18.2/source/src/host/alsa/mod.rs)
and [timestamp contract](https://docs.rs/cpal/0.18.2/cpal/struct.OutputStreamTimestamp.html).

Consequently there is no conversion to `PlaybackReceipt::Played`, no automatic
bot-speaking notification, no playback fence, and no spoken-history binding.
The existing media receipt semantics are unchanged. Full pipeline output parity
requires an adapter with independently verifiable playback acknowledgements;
waiting an estimated duration or observing another callback is insufficient.

Output accepts PCM16LE with exactly the opened rate and channel count. It
preserves FIFO and interleaving and converts to the device's native sample type.
Rate conversion, remix, mixers, gain controls, DTMF generation, video, named
output destinations, and transport messages are not implemented by this writer.
Use explicit upstream audio processing when needed. Unsupported formats and
frame kinds return errors instead of falling back silently.

## Ownership and bounds

- Construction performs no device I/O. `open()` selects the requested/default
  device and requires an exact supported rate/channel configuration
- Select devices by the native IDs from `list_devices()`. Index selection uses
  CPAL's all-device enumeration and is not interchangeable with PyAudio indices
- Every capture queue and output queue has both item and byte bounds. Capture
  budgets reserve a partial callback chunk and a receiver-held chunk. Output
  budgets retain the entire partially submitted packet until completion
- Capture overflow invalidates the stream and reports an error. Output overload
  rejects admission immediately. Neither path grows an unbounded task backlog
- Output callbacks use a nonblocking queue lock; contention emits silence and
  retains queued PCM. Empty queues emit silence; sequential small writes can underrun
  a larger device period, so applications needing continuity should keep bounded
  concurrent submissions ready. Callback sample counts have explicit upper bounds
- Data-callback panics are caught before unwinding into the native driver. They
  invalidate the session and reach pending readers/writers as device errors;
  poisoned queue state can still be discarded during cleanup
- Dropping a write future cancels its remaining queued PCM. Cancellation is
  checked between interleaved frames; one frame whose check already passed may
  finish copying concurrently with cancellation. Previously copied device
  buffers can still produce sound. `interrupt()` cancels old writes,
  destroys/joins the old stream, and opens a fresh stream before accepting new
  audio. Reopen failure leaves output closed
- `close()` discards remaining queued work, wakes waiters, and is idempotent.
  No asynchronous task is spawned or detached by this crate. CPAL owns its
  platform callback workers, which are released with their streams
- Callback submission has a bounded async timeout. Native open/play/drop calls
  are synchronous; CPAL's open timeout is passed through, but not all platform
  drivers honor it. Joining a stalled driver can exceed a nominal timeout;
  this implementation favors resource ownership over detached device work

These callbacks allocate bounded capture chunks and synchronize with queues.
This is a soft-real-time adapter, not a claim of allocation-free hard-real-time
operation. Device disconnects and native stream failures are terminal for that
session; close and explicitly reopen to recover.

## Usage

In a pipeline, construct the native capture transport without opening devices:

```rust,no_run
use pipecat_local_audio::{LocalAudioTransport, LocalAudioTransportParams};
use pipecat_core::{Pipeline, PipelineConfig};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let transport = LocalAudioTransport::new(LocalAudioTransportParams::default())?;
let worker = Pipeline::new(vec![transport.input()])
    .start(PipelineConfig::default()).await?;
// Consume captured InputAudio frames through the worker or downstream processors.
worker.end().await?;
let _ = worker.collect().await?;
# Ok(()) }
```

Runnable examples:

```sh
# Read-only enumeration, with stable native IDs.
cargo run -p pipecat-local-audio --example list_devices

# Explicitly records 400 ms from the selected microphone, then submits it to
# the selected speaker. Both must support 16 kHz mono. IDs are optional;
# omitting them selects the corresponding native default device.
cargo run -p pipecat-local-audio --example capture_and_submit -- INPUT_ID OUTPUT_ID

# Only selects alsa:null. It generates zero input and discards output.
# Never substitutes a physical device when the null device is unavailable.
cargo run -p pipecat-local-audio --example null_device_smoke
```

The capture/playback example retains a fixed 20 capture chunks in memory, writes
no recording to disk, and makes no network request. Its brief best-effort output
tail delay is not a playback acknowledgement.

## Build and verification

Linux builds require ALSA development files and pkg-config; normal distributions
provide `libasound2-dev` or their corresponding ALSA development package. CPAL
uses native platform libraries on other supported systems. Cross-platform
runtime behavior has not been qualified by this crate's Linux validation.

```sh
cargo test --locked --offline -p pipecat-local-audio -j 2
cargo clippy --locked --offline -p pipecat-local-audio --all-targets -j 2 -- -D warnings
```

The unit suite executes the same conversion, bounded capture, and output
callback engine used by native streams. It does not impersonate hardware
acceptance. `VALIDATION.md` records the separately executed null-device smoke
and the remaining physical-device qualification gap.
