# Native RNNoise audio filter

`RnNoiseFilter` implements `pipecat_media_transport::AudioFilter` using Xiph's
in-process RNNoise C ABI. Enable the `native` feature and supply a trusted native
library by absolute path and SHA-256. It has no network, Python, automatic model
download, subprocess inference, or missing-backend passthrough. The default Cargo
feature set can build without RNNoise; `start` then explicitly returns an error.

```rust,no_run
use pipecat_rnnoise::{RnNoiseConfig, RnNoiseFilter};
// Hash the actual trusted library built for this deployment, including its model.
let config = RnNoiseConfig::new("/opt/audio/librnnoise.so", "a".repeat(64));
let filter = RnNoiseFilter::new(config)?;
# Ok::<(), pipecat_rnnoise::RnNoiseError>(())
```

Pass `Arc::new(filter)` to a media transport's optional `AudioFilter`. The transport
owns `start`, control, and `stop`; no media-transport feature or export is required.
The example hash is a placeholder, never an approved artifact identity.

## Formats, lifecycle, and scope

- Input/output: mono signed PCM16 little-endian. RNNoise consumes 480 samples at
  48 kHz per call. Partial samples and frames are buffered across byte partitions.
  This is an explicit byte-stream extension: enabled Python np.frombuffer rejects
  odd-byte calls, whereas Rust reassembles them. Stop discards a trailing odd byte too
- Default: reject non-48-kHz rates and all multichannel formats at start
- `RateConversion::NativeSinc`: explicitly opt into the existing native streaming
  sinc resampler for mono 8–96 kHz. Its values, lookahead, rounding, and timing are
  independent of SOXR QQ. It does not implement source resampler quality options
  or the Python resampler's 0.2-second inactivity clearing
- Disable: byte-for-byte bypass, preserving buffered samples, resampler history,
  and recurrent state. Re-enable resumes that state, as the source filter does.
  Enable controls remain writable before Start, after Stop, and after failed Start
- Start: cancel current work, revoke any uncollected reply, then create fresh
  engine and buffers; the enabled toggle remains unchanged
- Stop: cancel the active request, wait for its native call to return, discard
  incomplete frames, resampler tails, and recurrent state, then acknowledge
- Empty input: no padding or flushing. `AudioFilter` has no flush interface; stop
  intentionally discards its tail, matching the pinned Python filter. No claim of
  duration-preserving end-of-stream drainage is made
- Cancellation: abandoning an operation invalidates the stream, even if a reply
  completed but was never delivered. A new start is required. Stop is idempotent
- Start/stop atomically revoke completed PCM replies and their admission lease.
  An unpolled audio future cannot hold lifecycle shutdown hostage; late delivery
  returns cancelled. Concurrent lifecycle calls return an explicit lifecycle-busy
  error instead of waiting on another unpolled lifecycle future
- Work runs on one owned OS worker thread, never on Tokio's executor. Admission is
  limited to one request including an uncollected reply; overlapping normal calls
  return backpressure. Start/stop wait for admission. Default per-call input limit
  is 96,000 bytes, configurable within 1–192,000 bytes, including disabled bypass
- Drop closes and joins the worker. It can wait for both the shared native-call
  lock and current native work; C cannot be preempted safely. Stop releases the
  engine; the idle worker lasts until Drop

Before initialization and after stop/failure, filtering returns an explicit error.
This intentionally avoids the Python filter's silent initialization-failure
passthrough. Float outputs use PCM16 amplitude and truncate toward zero; they are
saturated outside PCM16 range and invalid nonfinite output is rejected. Saturation
is safer than NumPy's out-of-range wrapping and is not overflow numerical parity.

## Trust and native limitations

SHA-256 pins the library's bytes and its built-in model. It does not make native
code trusted, validate arbitrary ABI implementations, or pin transitive shared
libraries. The caller must trust the code/dependencies and prevent modifications
to them throughout use. Verification followed by loading is not an atomic stable
file load; replacement between them is a TOCTOU risk. No custom model file ABI is
exposed because RNNoise model formats differ across versions.

Required symbols are `rnnoise_get_frame_size`, `rnnoise_create`,
`rnnoise_process_frame`, and `rnnoise_destroy`. Frame size must be exactly 480.
The owner destroys every state before unloading its library. Native calls across
this crate are globally serialized because RNNoise 0.1.1 lazily initializes shared
FFT tables without synchronization. Independent external consumers must coordinate
their own native initialization. Upstream 0.1.1 allocation failures can fault
inside C before returning a null state; an in-process wrapper cannot contain that
fault or a malicious/broken library.

## Qualification

See [PROVENANCE.md](PROVENANCE.md) for the exact official source/model profile,
licensing evidence, and why this is not a claim of current pyrnnoise model parity.

Unit tests use synthetic engines only for orchestration. Real inference tests are
explicitly ignored by default and fail if required assets are absent when run;
they never silently skip. Supply the official source pinned by the included
SHA-256 manifest and run, from the Rust workspace:

```sh
cargo test --offline --locked -p pipecat-rnnoise
cargo test --offline --locked -p pipecat-rnnoise --features native
crates/pipecat-rnnoise/qualification/run.sh /trusted/rnnoise-source /tmp/rnnoise-qualification
```

The Linux qualification script performs no downloads. It verifies source hashes,
builds the official C library, generates clearly synthetic noisy PCM, and computes
a direct C ABI reference with that exact library. Rust must match the reference
byte-for-byte for multiple chunk partitions. Noise-only energy must fall, with a
nonzero harmonic burst output guard against fake silence. Separate native checks
cover restarts, disabling, hash/load failure, actual media-transport integration,
and opt-in non-48-kHz partition invariance. Separate synthetic ABI fixtures check
missing symbols, wrong frame size, null state, and nonfinite output; these fixtures
do not perform denoising. This is synthetic-input engine qualification, not
human-speech quality or intelligibility proof.
