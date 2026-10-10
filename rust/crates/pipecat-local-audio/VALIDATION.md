# Local audio validation

## Source and dependency provenance

- Python source: `src/pipecat/transports/local/audio.py` at baseline
  `af9f9da9d7afddaa8811c0e0251b32566c980bdf`
- Native device implementation: registry-pinned `cpal = 0.18.2`, maintained by
  [RustAudio](https://github.com/RustAudio/cpal)
- Linux native prerequisites in the validation workspace are extracted Debian
  packages, not a system installation. Official metadata:
  [libasound2-dev](https://packages.debian.org/trixie/amd64/libasound2-dev/download)
  and [libasound2t64](https://packages.debian.org/trixie/amd64/libasound2t64/download)
- Both packages are version `1.2.14-1+deb13u1`, downloaded from
  `https://deb.debian.org/debian/pool/main/a/alsa-lib/` and SHA-256 verified:
  - Development: `dfa8e8a133ef8704093d03464d86a3e86d26232baf1e3827aa030cb68c437e1c`
  - Runtime: `5495496142d57e5ad6d581ac5456ec3c87f872c109a2bfb815d154b673eebc67`

The host validation uses Rust 1.99.0 and the existing shared `rust/target` with
2 build jobs. Workspace-only ALSA setup is loaded with:

```sh
source /workspace/shared/pipecat-tools/env.sh
source /workspace/shared/pipecat-tools/alsa/env.sh
```

The latter points pkg-config at the extracted development sysroot. No system
package, device permission, audio routing, security, or network setting changes
are required for these checks.

## Deterministic tests

Verified: 28 unit tests and 1 compiled documentation example pass. Strict
all-target Clippy, formatting, and whitespace checks pass.

The unit tests cover:

- Exact device-format selection, unsupported rates/channels, malformed settings,
  no-I/O construction, missing-open errors, and unsupported video/destinations
- Variable native input callbacks, source-compatible 20-ms reblocking, PCM16LE
  conversion, stereo ordering, float clipping, unsigned centering, and rejection
  of non-finite or incomplete native samples
- Capture item/byte/callback bounds, explicit overflow, closed receivers, and
  cessation of callback admission after stream failure
- Output FIFO, native sample conversion, silence on underrun/lock contention,
  and full callback-submission gating across partial native buffers
- In-flight byte accounting, immediate queue-overload rejection, timeout,
  future-drop cancellation, blocked-writer cancellation, and no queued replay
- Cancellation and native failure injected during the callback's conversion
  stop further copying at the next interleaved-frame boundary. One frame already
  in progress can finish; copied device buffers are not retracted
- Native error propagation, first-error retention, panic containment, poisoned
  callback-state cleanup, and the distinction between submission and playback

## Executed native-device smoke

Read-only CPAL enumeration reported one device:

```text
0: Discard all samples (playback) or generate zero samples (capture)
   id=alsa:null, input=true, output=true
   default_input=false, default_output=false
```

`null_device_smoke` explicitly selected only `alsa:null`. It successfully:

1. Opened 16 kHz mono native input and captured one 320-frame all-zero chunk
2. Dropped the capture owner, confirmed the input became closed, and reopened it
3. Closed and reopened capture while retaining the old owner, then verified that
   dropping the old owner neither closed nor disrupted the new session
4. Opened native output and submitted a 320-frame PCM chunk through real callbacks
5. Interrupted/destroyed/reopened output, then submitted another 320-frame chunk
6. Closed output twice without retaining a live session
7. Opened delayed input through a real pipeline and released it on graceful End
8. Received captured audio through active pipelines, then released their native
   streams on both graceful End and Cancel
9. Ran two concurrent writers alongside 16 interrupt/close/reopen cycles, checking
   all 64 write attempts returned receipts or explicit stopped/cancelled/full errors
10. Confirmed the Linux process thread count returned to its initial baseline of 1

The extended smoke passed 10 consecutive executions. Concurrent native stress
checks lifecycle ownership; deterministic callback tests establish the precise
in-flight cancellation boundary without relying on thread scheduling.

This exercises actual CPAL/ALSA native stream construction, callback invocation,
sample movement, cancellation/reopen, and stream destruction. The null device
has no physical ADC/DAC and discards all output, so it establishes no acoustic
playback or physical-device acceptance.

## Remaining qualification

The host has no `/dev/snd` physical device access and exposes no default capture
or playback endpoint. Real microphone/speaker testing, physical unplug/replug,
OS permission prompts, latency/underrun measurements, and macOS/Windows device
behavior remain unverified.

The separate output API cannot certify the shared media `Played` boundary.
Before providing a media output adapter or spoken-history binding, a future
backend must verify actual write acceptance and an independently trustworthy
playback-completion boundary, including errors, dropped native buffers,
interruptions, and hardware/driver buffering. Callback timing estimates alone
do not satisfy that contract.
