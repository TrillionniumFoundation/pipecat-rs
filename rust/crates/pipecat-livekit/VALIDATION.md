# LiveKit native validation

Validated on 2026-10-09, Linux x86-64, Rust 1.99.0.

## Results

- Actual `livekit 0.9.4` SDK and native C++ WebRTC build and link: passed
- Crate unit tests: 23 passed, zero failures or ignored tests
- SDK ownership integration test: passed, covering seven cancellation scenarios
- Real loopback WebSocket ownership test: passed, covering six cancellation
  scenarios with HTTP 101 upgrades and peer-observed socket closure
- Opt-in real SFU integration test: compiled; intentionally ignored and not
  executed because the host denies the server's network-interface enumeration
- Vendored signaling unit tests: 47 passed, zero failures or ignored tests
- `cargo clippy -p pipecat-livekit --all-targets -- -D warnings`: passed
- Crate formatting and whitespace checks: passed
- Independent application workspace resolves exactly one LiveKit SDK and one
  signaling crate, both selecting the fixed vendored sources

The tests execute the native WebRTC source/track/audio-stream implementation,
including exact stereo sample conversion, pending-source interruption, callback
draining, and shutdown. Local TCP fixtures exercise the actual SDK's HTTP401
validation rejection and cancellation with observed HTTP-socket closure. The
SDK's WebSocket implementation uses the host's configured proxy, so those local
fixtures receive its `/rtc/v1/validate` fallback request; they do not establish
a successful WebSocket upgrade through the SDK's default transport. Proxy and
security settings were not changed.

The independent ownership regression uses the actual SDK with its official
injected `livekit-net::WsClient` API and an in-memory half-open connection. It
verifies transport-reference release on cancellation, caller abortion and
adapter timeout, both while awaiting JoinResponse and while awaiting RTC
negotiation after a synthetic JoinResponse. It also checks the SDK's own
JoinResponse timeout. These cases reproduced detached tasks in the published
signaling crate; the vendored task-ownership patch fixes them. This does not
substitute synthetic protocol events for evidence of a real server join.

The separate `network_signaling` integration test injects a real
`tokio-tungstenite 0.29` TCP/WebSocket client. It accepts only `ws://127.0.0.1`
and requires that address to be explicitly present in the existing `NO_PROXY`
when proxy variables are set. Six cases verify cancellation, caller abortion
and adapter timeout before JoinResponse and after a synthetic JoinResponse.
Every peer completes an actual HTTP 101 upgrade and observes the client's
socket close/EOF; weak-reference probes independently verify signaling
transport ownership release. There is no SFU, ICE, DTLS or RTP exchange in
these six cases. The fixture transport does not modify production dependencies,
SDK signaling, proxy variables or host settings.

The strict lint command passes for the adapter; the vendored SDK emits 23
pre-existing compiler warnings. Its unrelated source was preserved, rather than
changing upstream code or suppressing warnings to make dependency logs silent.

The official LiveKit server 1.13.9 process was invoked with explicit loopback
signaling and interface/IP filters, but exited before listening: `failed to get create network:
route ip+net: netlinkrib: operation not permitted`. This explicit host denial
was not bypassed or retried. No participant JWT was generated and no hosted
account or paid endpoint was used. The `local_sfu` test is compile-only here.
Successful room join, SFU audio/data transfer, post-join publication rollback,
full room close under server failure, and actual network reconnection remain
unverified integration paths. See [the SFU qualification record](qualification/LOCAL_SFU.md)
and [preserved startup log](qualification/server-startup-denied.log). This
qualification does not establish complete Python LiveKit transport parity.

## Reproduction on this workspace host

The build prerequisites were extracted into a workspace-only prefix. No system
packages or security settings were changed.

```sh
source /workspace/shared/pipecat-tools/env.sh
source /workspace/shared/pipecat-tools/livekit-native/env.sh
export CARGO_TARGET_DIR=/workspace/shared/pipecat-tools/livekit-target
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
cargo test -p pipecat-livekit --jobs 2 --manifest-path rust/Cargo.toml
cargo clippy -p pipecat-livekit --all-targets --jobs 2 \
  --manifest-path rust/Cargo.toml -- -D warnings
cargo fmt --check -p pipecat-livekit --manifest-path rust/Cargo.toml
cargo test --manifest-path \
  rust/crates/pipecat-livekit/vendor/livekit-signaling-0.1.5/Cargo.toml \
  --lib --locked --jobs 2
```

The target directory is separate from the default workspace target. A temporary
path used for the first build is retained as a symlink; target contents occupy
workspace storage rather than the host's smaller `/tmp` tmpfs.

## Download provenance

- Rust SDK: vendored crates.io `livekit =0.9.4` and `livekit-signaling =0.1.5`
  with the narrow ownership patch described in [vendor/PATCHES.md](vendor/PATCHES.md)
  and exact published archive checksums in [vendor/ORIGINS.md](vendor/ORIGINS.md)
- Native wrapper chain: `libwebrtc 0.3.51`, `webrtc-sys 0.3.48`,
  `webrtc-sys-build 0.3.19`
- Native archive: [official LiveKit release artifact](https://github.com/livekit/rust-sdks/releases/download/webrtc-89d790b/webrtc-linux-x64-release.zip)
- Native archive size: 156,139,058 bytes, approximately 721 MiB unpacked
- Native archive SHA256:
  `b167adad5291cea0e4d66a0454d9d52d2ad714e6b0ed70f4410317d3ebde70c5`
- C/C++ compiler: official Debian 13 backports Clang 21.1.8,
  package version `1:21.1.8-3~bpo13+2`
- GLib/GObject/GIO headers/runtime: official Debian 13 packages,
  version `2.84.4-3~deb13u5`
- Associated official Debian runtime/development dependencies: approximately
  69.8 MB compressed in total, 344 MiB unpacked

Debian files were downloaded from `https://deb.debian.org/debian/` and each was
checked against the SHA256 in the official HTTPS package index before
extraction. Exact package filenames and hashes are retained in the workspace
build-tools directory. The SDK's hermetic libc++ requirements and ABI checks
were preserved. The native artifact retains its upstream `LICENSE.md`.
