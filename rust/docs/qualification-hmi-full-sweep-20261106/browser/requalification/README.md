# Browser Rust qualification after evidence recovery

This is a **new qualification record** on source commit
7105367d7d607653c3ab758f0131c029c4373faf / tree
19a5c08fc2cbfeabacd344668fc7b25d03a282c2. It replaces no earlier result.
The 54 native tests were existing Rust tests of the browser client; they are
not 54 new browser or device scenarios. The already retained Chromium protocol
and input suites were not rerun here.

## Observed gates

All commands ran from the independent source-browser/rust/client-templates
workspace, using the explicitly selected Rust 1.99.0 compiler and Cargo and the
dedicated target-browser directory.

| Run / phase | Command (selected absolute Cargo path omitted here) | Observed result |
| --- | --- | --- |
| r1 native tests | cargo test --locked --offline | 54 passed, 0 failed, 0 ignored; 23.763411 s |
| r1 native strict Clippy | cargo clippy --locked --offline --all-targets -- -D warnings | exit 0; 8.087912 s |
| r1 subsequent WASM preparation | official WASM standard-library download | checksum mismatch; overall r1 failed |
| r2 WASM-only strict Clippy | cargo clippy --locked --offline --target wasm32-unknown-unknown -- -D warnings | exit 0; 42.282510 s |

The first result remains [passed: false](evidence/result.json). Its two
successful phases are individually bound to raw logs and source snapshots.
The later [WASM-only result](evidence-wasm-r2/result.json) is a separate run;
it did not rerun the two native phases.

Raw gate logs:

- [Native tests](evidence/native-tests.log), SHA256
  263e5f831fa01ed351bdb8fbeef78eda299e10a93e59ebf881547097939f6b1a.
- [Native strict Clippy](evidence/native-strict-clippy.log), SHA256
  3b1db109d9a5cb34bc2d78c31e4370f02546eb007e77d5d199e6463162527fc0.
- [WASM strict Clippy](evidence-wasm-r2/wasm-strict-clippy.log), SHA256
  804ba285ac4a574b378eaca0adeeb1dea02df7291dc5828de5ed9455eee97885.

Each matching JSON file records the full argv, working directory, selected
environment, elapsed time, exit code, raw-log hash, and source before/after maps.
The 72 tracked files in the recorded browser, JPEG vendor and toolchain-file
scopes have equal hashes across all recorded before/after maps. This is the
recorded scope; the manifests do not claim to hash every registry dependency,
system library, or file on the machine.

## Failed download and separate recovery

The [read-only diagnosis](evidence/wasm-download-diagnosis.json) retained the
first bad archive unchanged: 8,338,068 bytes, SHA256
cf3ba01c16cf06744c7f188a724a801759862f637703b03a5e40feb6ca8582f8.
It has an XZ header, fails xz --test with unexpected EOF, and is shorter than
the official HTTP Content-Length of 23,152,600 bytes.
The [original preparation traceback](evidence/preparation-error.log) is kept.

r2 downloaded the same official immutable URL using 23 ordinary HTTP ranges
with at most four concurrent requests. All 23 succeeded on their first attempt.
Every range record preserves the exact request boundaries, HTTP 206 response,
Content-Range, headers, byte count and SHA256. The assembled archive has
23,152,600 bytes and SHA256
b760b0c9b08a6843c05de0098bd6802e23178ff473c4715ae57c1d679125bf22,
matching the verified [official manifest](official-wasm-component/channel-rust-1.99.0.toml)
and [manifest checksum file](official-wasm-component/channel-rust-1.99.0.toml.sha256).
Extraction and installation occurred only after the complete hash matched.

The standard library was installed into a new task-private WASM sysroot.
Only CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS selected that sysroot;
the original native compiler prefix was not an installation destination.
r2 records before/after hashes for cargo, rustc, rustdoc, rustfmt,
cargo-clippy and clippy-driver; all six binary hashes match.
This is a six-binary observation, not a full native-sysroot filesystem digest.

The native tool versions were recorded during r1. Its script calculated a
native-before dictionary but did not write that dictionary into its result
before the download failure. Consequently this archive does **not** assert
r1 native-tool binary before/after equality or backdate the r2 hashes into r1.

## Source and target ownership

The original r1 launch script required a previously nonexistent task root,
cloned a new detached source with --no-hardlinks, and set an explicit dedicated
CARGO_TARGET_DIR. r2 reused only that same source/target pair and acquired its
task-local exclusive lock. Neither phase used another checkout's target.

[target-owner.json](target-owner.json) is an index generated after completion
from the original launch scripts and results. It is not presented as an
owner marker that already existed before r1 started.

The actual runtime job IDs are:

- r1 native phases plus failed WASM preparation: 1a1b11b87e0-fc05-75d5.
- read-only failed-download diagnosis: 1a1b1281877-fc05-7657.
- r2 WASM-only recovery and Clippy: 1a1b134cc7a-fc05-76e6.
- complete text-evidence export: 1a1b139a90b-fc05-7715.

The runtime reports r1 failed and r2 succeeded, both with complete durable
output. Exact retained MCP returns are serialized under runtime-receipts;
these receipts are newly saved representations of actual tool returns.

## Archive and byte verification

[The readable index](index.json) enumerates 48 original UTF-8 files totaling
1,808,330 bytes: raw gate/preparation logs, results and in-progress receipts,
all range records, the official manifest/checksum, and the actual scripts.
Each entry retains its remote origin, byte count and SHA256.

[raw-evidence.json.gz](raw-evidence.json.gz) is the complete original export,
185,129 bytes, SHA256
a2abde0dbf3eb5998dd3d323cb3e79d9ef98d64b618775d6c28b08daee018e20.
Its JSON contains the original index and a files map with each complete text
and original hash. The same 48 files are also materialized here for direct review.

The export's eight complete transport chunks, compressed archive, index, and
all 48 file byte counts/hashes were verified locally before this directory was
assembled. See [local-verification.json](local-verification.json),
[export-receipt.json](export-receipt.json), and [backup-receipt.json](backup-receipt.json).
SHA256SUMS binds this directory's final files except itself.

The compiler components, range-part binary bodies, original truncated archive,
and compiled target binaries remain in their isolated desktop scope; they are
not copied into this text-evidence archive. Their recorded hashes and paths do
not mean their binary bytes are bundled here. This gate establishes Rust test
and static-analysis results, not physical microphone/speaker quality, actual
Chinese IME behavior, a production owner/thread/grant chain, or vendor-authenticated
cloud sessions.
