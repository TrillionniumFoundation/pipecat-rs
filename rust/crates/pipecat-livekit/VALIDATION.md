# LiveKit native validation

## Batch 159: bounded opt-in LiveKit video input — 2026-10-22

Default-disabled incoming camera/screen video uses bounded native I420 readers,
private frame/byte admission budgets, generation and track-SID ownership, mute
cancellation and awaited reader joins. Production input dispatch converts supported
frames into participant-tagged RGB `UserImage` frames. Sampling, image requests,
and public video lifecycle events remain deferred.

Revision 3 passed **60 crate unit tests** (L03, including 14 new tests),
**one review_contracts integration test** (R01), and **one network_signaling
integration test** (N01), all with natural zero exits. Revision 3 S01 compiled
and linked local_sfu only: zero runtime tests, no test listing or server startup.
Q01 strict Clippy then failed with three test-only chunks_exact_to_as_chunks
diagnostics (Clippy exit 101); formatting was never reached.

Revision 4 changes only those three RGB fixture loops to as_chunks::<3>() with
empty-remainder assertions. Production and documentation remain unchanged.
The reviewed cfg(test)-only bridge preserves applicability of revision 3 R01,
N01 and S01; these are historical executions, not fresh revision 4 runs.
Revision 4 L04 freshly reran and passed all **60 library tests** with zero
ignored or filtered tests and a natural zero exit. Together with historical
revision 3 R01/N01, this is 62 distinct qualified test cases: 60 fresh revision 4
executions and two historical revision 3 executions, not 62 revision 4 reruns.
The prior revision 3 library run is not added again. Q02 passed strict package
all-target Clippy (`-D warnings`) and package `fmt --check` on unchanged
revision 4, with natural zero exits. Static checks are not additional tests.

Native synthetic frames and loopback signaling do not establish joined-room
remote video, SFU media, codec/RTP interoperability, device operation, reconnect
media behavior, production readiness, or full Python parity. Adapter queue
bounds do not establish a whole-process memory guarantee. Resource observations
are sampled, not a kernel-enforced aggregate quota. This is a scoped package
qualification, not a new whole-workspace test gate.

### Source provenance and retained failures

The executed revision 3 full-source SHA256 is
`09c285af1d0bbc025a7429bd06297950be3a11a1dc6a40d7cc546a1f3efc2c01`.
The revision 4 full-source SHA256 is
`978e1a922551483e2a9b56f78eb432e8490ae353a8c7d7b0046e1aacc33e72ec`.
Only cfg(test) fixture code changed. The reviewed bridge carries revision 3
integration and compile-only evidence, not new executions. Revision 4 library
qualification passed freshly at L04 and strict Clippy/formatting at Q02.
The final documentation-only update is bound separately from these executions;
it does not turn historical evidence into fresh runs.

- L01 failed compilation with E0277 and executed zero tests.
- L02 compiled and ran 60 tests: 57 passed and three failed. Those failures
  remain part of the evidence, not passing tests or masked results.
- Revision 3 corrected only three wrong fixture expectations against the actual
  pinned SDK/core behavior: two captures after the final native sink had closed
  return false; downstream output saturation preserves the core Backpressure
  failure instead of graceful shutdown. Production behavior and decisive frame
  assertions were not relaxed. L03 then actually reran and passed all 60 tests.

Historical results below retain their original date and scope.


## Batch 158: bounded optional LiveKit camera output — 2026-10-21

The default-disabled camera path accepts room-wide `OutputImage` /
`OutputImageDetailed`, validates exact format, dimensions and payload size, and
rejects per-participant destinations and audio synchronization before admission.
A separate bounded video lane feeds owned native I420 buffers. Typed sequential
publication preserves audio-before-video readiness and rollback ownership.

Revision 5 passed **46 independently verified crate unit tests** (L07).
Strict package all-target Clippy (`-D warnings`) and crate formatting checks
both passed on that unchanged revision (Q06, natural exit zero). Two earlier
integration tests passed on revision 4: one `review_contracts` (R02) and one real
loopback WebSocket `network_signaling` (N02). Their results are carried through
an explicit reviewed unaffected/semantic-equivalence bridge, not represented as
revision 5 executions. This is 48 distinct qualified test cases, not 48 tests
rerun on revision 5; integration scenarios and repeated historical runs are not
additional tests. `local_sfu` was compiled and linked only on revision 4 (S01),
with the same explicit carry bridge: zero runtime tests, no listing, and no
server startup. The previous netlink denial remains a boundary and was not
bypassed or retried.

Native-synthetic media checks use linked WebRTC, not stubs, but do not join a
room. Publication transaction tests use mocks, not a successful SDK publication.
There is no live-provider, joined SFU, remote video, physical device, codec/RTP,
production-readiness or full Python parity claim. Earlier whole-workspace and
vendored signaling test counts are historical and are not added to these 48.

### Source provenance and retained failures

Revision 4 L06/R02/N02/S01 used unchanged full-source SHA256
`08a740d73378c2c9528c4760f949c969f575032b5d707e0a07a11ad105e33deb`.
Revision 5 L07 and Q06 used unchanged full-source SHA256
`5d7815b70b24e588fdeb80ece550d799c8bed2005d613106daa73d9f483f7b8a`.
The reviewed bridge carries only the unaffected revision 4 integration and
compile/link evidence, with no new execution claimed. Documentation is updated
after qualification; the publication binding checks every non-documentation
source byte against that final source and records the new documentation-inclusive
hash. Documentation hashing is not another test run.

The complete attempt history remains part of the evidence:

- L01 stopped on the diagnostic witness cap; it is not a passing run.
- L02 failed on the allocation-only 4096 baseline check before Cargo execution.
- L03 executed compilation and failed with nine errors involving SDK public vs.
  internal APIs and core image variants.
- L04 compiled and listed tests, then rejected two namespace inventory
  mismatches before executing tests.
- L05 executed 45 tests: 42 passed and three chroma-oracle tests failed.
- Revision 4 changed tests only: a pinned libyuv non-rounded packed-color chroma
  value of 239 is distinct from the raw I420 oracle's 240. L06 then passed all
  46 tests, including the explicit distinguishing oracle test.
- R01 was a withheld unused grant and N01 an unused candidate, not executed
  failures. Source-only review and test listing count as zero executed tests.

- Q01 exceeded the 10-second observer-gap limit (22.3379 seconds, pregraph
  22.3057 seconds); Cargo ended after TERM (-15), outer status 123, all owners
  closed and source unchanged. There was no accepted lint result or format run;
  the exact delay cause was not established.
- Q02 naturally failed on two actual adapter Clippy findings. Revision 5 made
  two reviewed semantic-equivalent corrections: keep independent conditions
  separated for `possible_missing_else`, and use typed four-byte chunks for
  `chunks_exact_to_as_chunks`. No other product file changed from revision 4.
- L07 actually reran and passed all 46 revision 5 library tests.
- Q03 failed at launcher proof admission (outer exit 1), before owner
  directories, Popen or Cargo; this is a prelaunch failure, not an unused grant.
- Q04 passed actual strict Clippy, but the stage then failed before formatting
  spawned because formatting RSS was classified as build RSS. The finite proof
  cap was 192 MiB, with an extra 32 MiB funded from workspace reserve; this did
  not waive other limits. Q04 is not a complete lint/format pass.
- Q05's outcome remains uncertain after an execution interruption and is not
  counted as a pass. Q06 independently completed strict Clippy and formatting
  with natural zero exits and verified closure. Historical uncertainty is not
  overwritten by that later pass.

Historical results below retain their original date and scope.


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
