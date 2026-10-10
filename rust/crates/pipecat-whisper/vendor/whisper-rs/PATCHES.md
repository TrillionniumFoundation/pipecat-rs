# whisper-rs 0.16.0 local ownership fixes

Upstream: <https://codeberg.org/tazz4843/whisper-rs>

- Crates.io package: `whisper-rs` 0.16.0
- Published registry archive SHA-256:
  `2088172d00f936c348d6a72f488dc2660ab3f507263a195df308a3c2383229f6`
- Packaged source commit: `7558e1b72f54f2f22a53589afb77e65681834c36`
  (retained in `.cargo_vcs_info.json`)
- License: Unlicense, retained in `LICENSE`
- Unmodified native dependency: `whisper-rs-sys` 0.15.0, whose packaged whisper.cpp
  reports version 1.8.3. Runtime resolution is locked by the application workspace

This path dependency is excluded from the application workspace. It is vendored
because the application relies on repeatable allocation cleanup and native abort.
The canonical crate archive's digest was checked against the crates.io index before
copying its sources.

## Runtime changes

Only `src/whisper_params.rs` has runtime behavior changes:

1. `set_abort_callback_safe` retains its boxed callback in an `Arc<Mutex<...>>`.
   The C trampoline casts to that exact allocation type, rather than the generic
   original closure type. Cloning parameters keeps the allocation alive, replacing
   or clearing callbacks releases the corresponding owner, and the last owner drops
   the callback. Replacement installs the new native pointer/owner pair before
   dropping the previous capture, preserving validity if its destructor panics.
   Mutable invocation is serialized across parameter clones
2. Safe abort closures must be `Send`, consistent with the wrapper's existing
   `Send` implementation for `FullParams`
3. Callback panics and poisoned callback locks return `true` (abort). Rust unwinding
   does not cross the native C ABI
4. Language and initial-prompt C strings are retained as `Arc<CString>` instead of
   leaked with `CString::into_raw`. Parameter clones keep their pointer targets
   alive; replacement and final drop release the old strings

The application uses no progress/segment callbacks or built-in VAD path setter.
Those unrelated upstream methods are outside this narrow patch; this is not a full
safety audit or claim that every wrapper API has been repaired.

## Test-only and packaging changes

- Five ownership regression tests exercise captured-closure layout, cancellation,
  replacement, final drop, parameter cloning, C-string lifetime, panic containment
  and replacement after a captured destructor panics
- Upstream nightly-only benchmark imports/functions are gated behind the opt-in
  `nightly-bench` feature, so unit tests run on the project's stable Rust toolchain
- The Cargo manifest adds only that feature. Its standalone development lockfile is
  retained; the vendored `.gitignore` allows it. The Cargo registry cache marker was
  omitted. Native dependency source is unchanged

Run the focused tests from the repository's `rust` directory:

```sh
CARGO_TARGET_DIR="$PWD/target" \
CARGO_PROFILE_TEST_DEBUG=1 CARGO_PROFILE_TEST_INCREMENTAL=false \
CARGO_PROFILE_DEV_DEBUG=1 CARGO_PROFILE_DEV_INCREMENTAL=false \
cargo test --manifest-path crates/pipecat-whisper/vendor/whisper-rs/Cargo.toml \
  --offline --lib pipecat_ownership_tests
```

`pipecat-whisper` additionally tests real native abort/reuse and repeated inference
with explicit GGML model and WAV paths. These tests do not replace an upstream
sanitizer audit. `ownership.patch` records the source/test changes against the
published 0.16.0 crate, excluding development-lockfile resolution.
