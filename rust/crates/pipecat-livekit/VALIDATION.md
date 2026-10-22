# LiveKit native validation

## Batch 161: bounded plain LiveKit image requests — 2026-10-22

Opt-in pipeline `UserImageRequest` handling adds bounded plain next-observation
requests and `RequestOnly` capture. Pending/retained requests have finite count,
header and RGB budgets, monotonic expiry, invocation fencing and source lifecycle
invalidation. The ordinary idle assistant can append constrained RGB context;
tool/function/callback requests, cached-response replay and full Python request
parity remain deferred. The only dependency change is a development edge to the
existing local `pipecat-turns` crate; no registry package/version/checksum changes.

Revision 2 freshly executed **92 distinct test cases** on unchanged source:
**90 library tests** in L06 (12.84 seconds), **one review_contracts integration
test** in R01 (9.19 seconds), and **one network_signaling integration test** in
N01 (4.15 seconds). Every run passed with zero failed, ignored or filtered tests.
Each stage was independently accepted, with complete logs, natural zero exits
and verified process cleanup. These are fresh R2 executions, not carried
Batch160 evidence or extra cases from build/list phases.

The library tests include local native SDK source/stream and actual pipeline/
assistant context checks, plus deterministic cutoff/drain, expiry, retained-carrier
and rejection tests. R01 exercises synthetic signaling ownership release on
cancel, abort and timeout. N01 exercises real loopback TCP/WebSocket HTTP 101
and teardown before/after synthetic JoinResponse across six scenarios inside
one test case; it does not establish SFU, ICE, DTLS or RTP exchange.

R1 L05 remains adverse evidence: **85 passed and 5 failed**, with zero ignored
or filtered. All five new pipeline tests failed in shared fixture setup because
the initial capture assertion preceded native sink installation. R2 moves only
that positive assertion after `VideoReaders.start`; all assertions and 90 test
names remain. Production source, manifests and lockfile are unchanged from R1.
The SDK may reject capture without an interested sink; this fixture repair does
not introduce a physical-camera freshness guarantee. Prior failed/interrupted
validation and setup attempts remain preserved and add no passing test cases.

QP03 and final QF01 passed strict selected-package all-target Clippy
(`-D warnings`) and package `fmt --check` on unchanged R2, with natural zero
exits and 23 retained vendored dependency warnings. QF01 Clippy finished in
1.12 seconds; formatting exited zero. Independent review accepted QF01. These
static checks add no runtime test cases and are not a whole-workspace CI gate.

S01 compiled and linked `local_sfu` in 6.68 seconds and was independently
accepted for that scope only: zero test listing, zero runtime tests and no server
startup. An earlier prelaunch validation failure remains recorded and is not
counted as an execution or replaced by the successful attempt.
The earlier SFU startup denial, `netlinkrib: operation not permitted`,
remains a boundary, not something bypassed or retried in this slice. See the
[existing SFU qualification record](qualification/LOCAL_SFU.md).

The image contract establishes only native sink enqueue after request acceptance
and its required empty-queue fence. SDK keepalive or replayed pixels can satisfy
it, including with new timestamps; physical capture time is not established.
`DispatchAttempted` is neither `Delivered` nor `Succeeded`; a successful routing
return is not downstream fulfillment. `failed_requests` is local rejection or
invalidation accounting and may increase after a downstream context commit
already won, without undoing it. Raw sinks already dispatched are outside recall;
compatible constrained consumers enforce the commit-time gate. No full Python
parity, SFU/device runtime, remote-media operation or physical-camera freshness
is established.
Adapter budgets exclude arbitrary downstream clones and native upstream storage;
sampled resource guards are not hard aggregate memory quotas.

### Source provenance and evidence scope

Published base: `fa80bb1d1bd1fbdcf88f500221b8de268fc9d6d0` (Batch160).
QP03/L06/R01/N01/S01/QF01 bind the same R2 full 2021-file source SHA256:
`e10b176f861bc819818f194c9a5f1dd0c9fdde84fbf715e9625b4869a8c4e919`.
The documentation-only update is separate from the tested source and does not
imply another execution or changes to validated production/test source.
Historical Batch160 results below keep their original scope and counts.

## Batch 160: bounded per-source LiveKit capture policy — 2026-10-22

Opt-in video input now supports typed per-source `AllFrames` / positive bounded
FPS policies and synchronous participant/source overrides. The shared monotonic
sampler selects frames before RGB allocation and queue permits, with independent
finite policy/identity-byte budgets and atomic lifecycle/generation fencing.
Monotonic arrival time deliberately differs from pinned Python's wall clock.
Callbacks, tool image requests, cache replay and public video lifecycle events
remain deferred. The only dependency change is an edge to the existing local
`pipecat-media-transport` crate; no registry packages, versions or checksums change.

Revision 2 passed **70 library tests** (L01, including 10 new tests),
**one review_contracts integration test** (R01), and **one network_signaling
integration test** (N01), all with natural zero exits and zero ignored or filtered.
S01 compiled and linked local_sfu only: zero test listing, runtime tests or server
startup. Q01 then failed strict Clippy with one test-only
`chunks_exact_to_as_chunks` diagnostic (Cargo exit 101; original command natural
nonzero exit 123); formatting never started. These failures are retained.

Revision 3 changes only that new RGB fixture to typed `as_chunks::<3>()` and an
empty-remainder assertion, preserving its RGB predicate. Production, manifests,
lockfile and README are unchanged. The reviewed cfg(test)-only bridge carries
revision 2 R01/N01/S01 evidence, not fresh revision 3 executions. Revision 2 L01
remains historical and is not counted again. L02 built/listed successfully, but
an observer gap of 16.962 seconds exceeded the unchanged 10-second limit and
triggered SIGTERM during tests: 12 passing lines, no completed summary, unqualified.
A reviewed scanner optimization removed repeated root-stat work without changing
cadence, gap limit, accounting or caps; faster probes do not prove the historical
cause of the gap.

L03 freshly executed all **70 revision 3 library tests**, passing in 8.58 seconds
with zero ignored/filtered and natural zero exit. Its build reused the cached
ELF, not a new recompile. The maximum observed phase gap was 0.5943 seconds, below
10 seconds, with no forced interruption. Q02 passed strict package all-target
Clippy (`-D warnings`) and package `fmt --check` on unchanged revision 3, with
natural zero exits. Static checks add no tests. The count is **72 distinct test
cases**: 70 fresh revision 3 library cases plus two explicitly bridged historical
revision 2 integrations, not 72 fresh revision 3 executions. Historical L01 and
L02 partial output add no cases. Independent aggregate review accepted these
validation results. Documentation is bound separately from the executed source;
this does not claim another execution or broaden the qualified scope.

Local native synthetic streams and loopback signaling are not remote RoomEvent,
joined-room/SFU media, RTP/codec/device, network-jitter or production qualification,
and do not establish full Python parity. Adapter bounds do not bound whole-process
memory; sampled resource observations are not kernel-enforced aggregate quotas.
This is scoped package qualification, not a new whole-workspace test gate.

### Source provenance and evidence scope

The historical revision 2 L01/R01/N01 executions used full-source SHA256
`c54a524978825e40c924585c65385991a9618379478d4d6ae6cb753188e39ed4`.
Revision 2 changed only README wording from revision 1 before these runs; frozen
Rust, manifest and lockfile bytes are unchanged between those prepared revisions.
Revision 3 L03 and Q02 full-source SHA256 is
`8fc1ff1b402b435d37551330009bfcbc88896a889eb12d30c7c266325f62c160`.
The final documentation-only update requires a separate source binding and does
not imply execution of later source edits. Historical batch results below retain
their original dates and scopes, including prior failures and carry limitations.

The ten new library cases cover policy/config bounds, independent source schedules,
strict stall/rate switching, atomic policy/identity capacity and cleanup, lazy
exhaustion without fallback, stale generation/pre-conversion rejection, lifecycle
retention, simultaneous capture updates and sampling, native stream-to-production
pipeline RGB/metadata, and pressure-drop cadence/non-retroactive updates.


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
- R01 and N01 were not executed in that revision and are not counted as
  failures or passes. Source-only review and test listing add no executed tests.

- Q01 exceeded the 10-second observer-gap limit (22.3379 seconds, pregraph
  22.3057 seconds); Cargo ended after TERM (-15), outer status 123, with
  process cleanup verified and source unchanged. There was no accepted lint result or format run;
  the exact delay cause was not established.
- Q02 naturally failed on two actual adapter Clippy findings. Revision 5 made
  two reviewed semantic-equivalent corrections: keep independent conditions
  separated for `possible_missing_else`, and use typed four-byte chunks for
  `chunks_exact_to_as_chunks`. No other product file changed from revision 4.
- L07 actually reran and passed all 46 revision 5 library tests.
- Q03 failed prelaunch validation (exit 1) before Cargo started; it is not
  counted as an executed lint check.
- Q04 passed strict Clippy, but a resource-accounting classification error
  prevented formatting from starting. It is not a complete lint/format pass.
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

## Reproduction prerequisites

Use the native build prerequisites in [README.md](README.md#native-build) and
configure the pinned SDK's build environment for your platform. From the
repository root, the following commands cover the qualified package scope:

```sh
cargo test -p pipecat-livekit --lib --locked --jobs 1 --manifest-path rust/Cargo.toml
cargo test -p pipecat-livekit --test review_contracts --locked --jobs 1 --manifest-path rust/Cargo.toml
cargo test -p pipecat-livekit --test network_signaling --locked --jobs 1 --manifest-path rust/Cargo.toml
cargo test -p pipecat-livekit --test local_sfu --no-run --locked --jobs 1 --manifest-path rust/Cargo.toml
cargo clippy -p pipecat-livekit --all-targets --locked --jobs 1 \
  --manifest-path rust/Cargo.toml -- -D warnings
cargo fmt --check -p pipecat-livekit --manifest-path rust/Cargo.toml
```

The `local_sfu` command is compile-only. Running that test requires a separately
configured supported environment and is outside the qualification reported here.

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

For Linux native builds, use Clang 21 or newer, C++ development tooling,
`pkg-config` and GLib/GObject/GIO development headers from official sources.
Verify downloaded artifacts against their published checksums. Preserve the
SDK's hermetic libc++ requirements, ABI checks and bundled `LICENSE.md`.
