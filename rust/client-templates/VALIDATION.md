# Browser-client validation record

Environment: Rust 1.99.0, wasm32-unknown-unknown, wasm-bindgen CLI 0.2.129.

## Replacement-host checks, 2026-10-10 UTC

The isolated recovered browser workspace again passed its 11 native tests,
native all-target strict Clippy, WASM strict Clippy, and formatting. The Rust
WASM release build succeeded, but the original packaging script then failed
because it hardcoded `target/` while Cargo used an exclusive external target.
`build.sh` now passes its selected target directory explicitly to Cargo and uses
the same directory for wasm-bindgen. The corrected script produced all five
static assets with `CARGO_TARGET_DIR` set to an external absolute path.

The official wasm-bindgen 0.2.129 Linux-musl archive was retrieved from
https://github.com/wasm-bindgen/wasm-bindgen/releases/download/0.2.129/wasm-bindgen-0.2.129-x86_64-unknown-linux-musl.tar.gz
and hashed as `82d12bb940e2d4e72e0d5605387fc1b8ca179044e012b620f0ce4e7440e8320e`.
No native tool binary is bundled in this source tree.

A fresh ordinary headless Chromium launch on the replacement host again exited
134 before loading `about:blank`, reporting the process-singleton local socket
failure `Operation not permitted`. The probe did not disable the browser sandbox
or change network/security settings. Therefore the build checks above still do
not constitute browser execution, microphone, visual, or playback acceptance.
The earlier detailed fixture record below is historical; the new probe verifies
the launch blocker, not those unexecuted fixture assertions.

## Passed

- `cargo test --offline --locked`: 11 native tests
- `cargo clippy --offline --locked --all-targets -- -D warnings`
- `cargo clippy --offline --locked --target wasm32-unknown-unknown -- -D warnings`
- `cargo fmt -- --check`
- `./build.sh`: optimized wasm32 build and generated browser interop bundle
- `python3 -m py_compile tests/browser_acceptance.py`

The native lifecycle tests use the same generation and queue-limit functions as
the browser implementation. They cover repeated Connect, disconnect/reconnect,
late results from microphone/start/offer/socket/peer stages, muted or disconnected
capture, 256 KiB socket backpressure, and playback time/node limits. They do not
exercise browser-owned resource cleanup.

## Blocked before browser assertions

`python3 tests/browser_acceptance.py` cannot launch Chromium in this execution
environment. Initial launch and the permitted elevated retry both terminate
before any app page loads. Relevant Chromium output:

```text
FATAL:chrome/browser/process_singleton_posix.cc:297
Check failed: . socket() failed: Operation not permitted (1)
```

The first launch also reports:

```text
chrome_crashpad_handler: --database is required
mkdir : No such file or directory (2)
```

The elevated retry's decisive error remains the denied local socket; changing
application code cannot fix that browser-launch restriction.

The supported cloud browser was separately asked to open a loopback static
server at `http://127.0.0.1:8765/` and returned `net::ERR_CONNECTION_REFUSED`.
It cannot access this execution namespace's loopback listener. No public tunnel,
security-setting change, alternate user computer, or physical-device access was
used. The temporary static listener was stopped.

Therefore the browser fixture is **unexecuted beyond launch**, no acceptance
screenshot exists, and no UI, browser microphone, speaker, WebRTC or WebSocket
end-to-end acceptance is claimed. Run the included fixture in an environment
that permits Chromium's local sockets. The fixture covers synthetic media,
actual loopback WebSocket/protobuf exchange, paired browser WebRTC SDP/data/media,
cancellation during pending permission/start/offer, reconnection, microphone
mute, queue limits, transcript safety, and browser resource cleanup.

Live providers, physical devices, TURN/WAN traversal, autoplay restrictions and
other browser engines remain separate acceptance gates even after that fixture
passes.

## Slice 45 host boundary, 2026-10-14 UTC

A fresh ordinary headless Chromium launch and one approved ordinary escalated
launch both failed before `about:blank` at the process-singleton socket with
`Operation not permitted`. Neither disabled the browser sandbox or changed
security settings. No browser application assertions, DOM interaction, screenshots,
physical media or browser end-to-end execution are claimed for this slice.
The updated acceptance fixture explicitly enables the Chromium sandbox and adds
real-form text cases for both transports; it remains unexecuted on this host.
Native shared-controller and real native transport/provider tests are separate
from browser acceptance. The complete image/receipt browser bridge is not present.
