# LiveKit native validation

## Batch 164: first-participant observation — 2026-10-22

### Completed bounded native qualification

The actual tested source is commit
[`b806f9b88061afdd3449062c8264920b2f7964a6`](https://github.com/TrillionniumFoundation/pipecat-rs/commit/b806f9b88061afdd3449062c8264920b2f7964a6),
tree `8e6f8d31f616e609a71173eb1c3a0f4f2ab02396`. Its original publication was
explicitly source-only and native-untested; the following campaign completed
later without changing that source. This documentation-only update records those
results, not a separately compiled revision or a hosted CI result.

The publication bridge changes only this `VALIDATION.md` and the adjacent
`README.md`; all other tracked paths, bytes and modes match the tested checkpoint.
Those two documents are absent from the inspected source inclusions and existing
dep-info. Static source/build-output inspection found no checkout-VCS-derived
runtime constant. This is not filesystem tracing, a post-edit build, or a
guarantee of reproducible binary identity. Vendored README inputs are unchanged.

Linux x86-64, Rust 1.99.0, Clang/LLD 21.1.8 and official WebRTC
`webrtc-89d790b`, with the pinned SDK/native provenance recorded below:

- L: **120 library tests passed in 12.50 seconds**, including the first-participant
  admission/reset/limits, production-dispatch and owned-video cleanup cases.
- R: **one `review_contracts` test passed in 9.16 seconds**:
  `signaling_owners_release_on_cancel_abort_and_timeout`. This is synthetic
  signaling ownership, not a joined-room or media-delivery test.
- N: **one `network_signaling` test passed in 4.10 seconds**:
  `real_websocket_transport_releases_on_cancel_abort_and_timeout`. It uses real
  loopback TCP/WebSockets with unchanged proxy handling; it does not qualify
  the SDK's default proxy path, hosted rooms or remote media.
- Each L/R/N executable was freshly compiled and linked, listed against its exact
  frozen inventory and actually executed on the same unchanged source. All had
  zero failed, ignored, measured or filtered tests. Total: **122 actual executed
  test cases**. Listing, repeated checks and scenarios within a test add no cases.
- S: `local_sfu` freshly compiled and linked only. **Zero tests executed**: no
  listing, runtime or server startup. The earlier SFU/netlink restriction was
  neither retried nor bypassed; this is not SFU runtime qualification.
- QP: cold strict package all-target Clippy (`-D warnings`) passed with **373
  artifacts, all `fresh=false`**. A subsequent identical invocation reported all
  373 fresh and matched the complete canonical artifact records, ignoring only
  `fresh`. Package `fmt --check` also passed.
- QF: final strict all-target Clippy passed with **373 artifacts, all
  `fresh=true`**, and complete canonical equality to the QP all-fresh witness.
  Final package `fmt --check` passed. QF was a fresh invocation reusing cached
  artifacts, not a fresh rebuild or another execution of the 122 tests.

These phases were independently accepted. Every original command completed
naturally with exit zero; recorded cleanup, source/input checks and sampled
resource limits passed, with no recorded cause, controller error or signal.
The 2,036-file source inventory and 107,182 immutable-input records were
reverified unchanged. Before/after hashing does not establish physical
immutability between observations. Batch163 and older records below remain
historical and do not contribute to the 122-test total.

### Diagnostics, source binding and remaining scope

Strict Clippy retained **24 warnings in vendored LiveKit 0.9.4**. Each selected
L/R/N/S compilation also retained one selected-package `linker_messages` warning
that Clang `--no-default-config` was unused during compilation. Successful strict
package lint is not a warning-free dependency or native build.

The native test profile used `opt-level=0`, compiler debug level 1, debug assertions
and overflow checks. Selected ELF links used `--threads=1` and `--strip-debug`:
ordinary symbols and unwind data remain, but final non-ALLOC DWARF is omitted.
Source-line diagnostics are reduced; full-DWARF debugging parity is not claimed.
No dependency, vendor, manifest, lockfile or global profile change is included.
Resource observations are sampled, not kernel-enforced aggregate quotas, and do
not rule out between-sample peaks. Final target storage was 2,342,645,760 bytes
under the 2,560 MiB cap; this does not qualify arbitrary future cache reuse.

The first-participant contract, one-bool bound, raw-first non-atomic admission,
terminal failure handling and Python ordering/lifetime differences remain as
documented in README. Actual SDK Connected/live ingress, current membership
sampling and reconnect preservation are source-qualified, not executed joined-room
fixtures or Python callback tests. The video cleanup fixture does not qualify
full room/audio/publisher teardown. No hosted CI, SFU runtime, successful hosted
join, remote media/device/RTP, full LiveKit/Python API parity, production-readiness
or complete Rust replacement is established.

Qualification receipt SHA256:
`9a485554e9baaa5b043086cb21b61791cc1c0ad7f0e0f567ea22d31ffd35a832`.
The tested source-inventory SHA256 is
`ec93a055f28ba6a725771910476de1eefd686b220c77bcfac73082488170799f`.
The following dated source-recovery record is retained as history, superseded
only by the bounded qualification above.

### Historical source-only and recovery record

Source now adds the bounded `FirstParticipantJoined { identity }` observation,
a room-owner bool, shared live/initial join admission, and successful-departure
re-arming based on a deferred current unfiltered SDK empty-membership sample.
Raw admission remains first and non-atomic; failures are terminal without retry
or rollback. See README for Python ordering/lifetime differences and bounds.

Focused test source was added for first/later/duplicate joins, normalized initial
snapshot ordering, fresh owners, successful departure then next join, rejected
departures preserving true, media-filter independence, identity and aggregate
UTF-8 limits, raw/partial queue failure, no derived frame with ordinary join/audio
dispatch, and existing owned-video-reader cancellation/join after failure.
These are not executed Batch164 results, joined SDK room fixtures or Python
callback tests. Actual ingress, SDK sampling and reconnect preservation require
source review; the cleanup fixture cannot qualify full room/audio/publisher teardown.

Only direct rustfmt on affected Rust files and `git diff --check` were performed
for this source-only stage. No Cargo, native build/test, Clippy, integration or
SFU execution occurred. Native validation is capacity-blocked pending resource
readiness and fresh independent source/invalidation/forecast acceptance. No
retention deletion, threshold reduction, retained-artifact reuse approval,
publication, dependency, vendor, manifest or profile change is implied.

Before qualification: freeze the source and exact library inventory; perform
package formatting, preflight/final strict all-target Clippy, complete library
execution including new cases, review_contracts and network_signaling integration
regressions, and local_sfu compile/link only (zero listing/runtime/server startup),
with stage-specific capacity admission and independent verification. No new
executed test total is claimed. The accepted Batch163 **109 actual executed test
cases** below remain unchanged historical evidence, not validation of Batch164.

Recovery update — 2026-10-22: the capacity-blocked state above describes the
earlier source-only stage. The sources have been recovered, and formatting and
static checks repeated. Native dependencies and the build environment are being
re-established. There are no current Batch164 native-test, Clippy, integration or
SFU results; this recovery is not a native pass or a green CI result.

## Batch 163: bounded active-speaker change observation — 2026-10-22

`ActiveSpeakerChanged { identity }` observes a change in the last identity of a
nonempty SDK list, while `ActiveSpeakers` preserves every admitted raw list,
including empty/repeated lists, order and duplicates. The room owner retains one
bounded identity only after the derived observation is admitted. Empty updates,
departure and automatic reconnect do not reset it. Raw-first admission is not
atomic: the raw event can be queued before the derived event fails with
`QueueFull` or `Closed`. Existing terminal cleanup handles that failure without
retry or rollback. The observation has no frame projection or callback authority;
direct receive and pipeline input remain mutually exclusive consumers. The added
public enum variant can require downstream exhaustive-match updates.

Python reference remains `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/transports/livekit/transport.py`. The comparison covers state
selection, not Python asynchronous callbacks or the full transport API. See the
README for limits, ownership and compatibility details. No dependency, Cargo
manifest, lockfile, profile or vendored SDK change is part of this slice.

### Completed validation

Linux x86-64, Rust 1.99.0, with pinned SDK/WebRTC provenance as recorded below:

- L01: **107 library tests passed**, including ten new tests, in **13.21 seconds**.
- R01: **one `review_contracts` integration test passed** in **9.24 seconds**:
  `signaling_owners_release_on_cancel_abort_and_timeout`. This is synthetic
  signaling ownership, not a joined room or media-delivery test.
- N01: **one `network_signaling` integration test passed** in **4.54 seconds**:
  `real_websocket_transport_releases_on_cancel_abort_and_timeout`. It uses real
  loopback TCP/WebSockets, not a hosted room or remote media.
- All three freshly built, listed and executed their exact inventories on the
  same frozen source, with zero failed, ignored, measured or filtered tests,
  natural zero exits and verified process cleanup. Results were independently
  accepted. Total: **109 actual executed test cases**. Listing, rebuilds and
  scenarios within one test do not add cases.
- S01: `local_sfu` freshly compiled and linked, with natural zero exit and verified
  cleanup, independently accepted. **Zero tests executed**: no test listing,
  runtime or server startup. This does not qualify SFU runtime.
- QP01 preflight and QF01 final strict package all-target Clippy (`-D warnings`)
  and package `fmt --check` passed and were independently accepted. QF01 had
  natural zero exits and 373 compiler artifacts, all marked fresh by Cargo.
  This was a new lint invocation using cached artifacts, not a fresh rebuild.
  Static checks add **zero runtime tests** and do not rerun the 109 cases above.

The ten new tests exercise production normalized admission and input dispatch:
last-identity selection; raw ordering/duplicates; empty updates and fresh-owner
state; local/filter-excluded identities; count/UTF-8 limits and compact retained
identity; queue-full/closed and one-slot partial admission; direct-event validation;
no observation frame with continued media handling; and existing video-reader
cancellation/join after admission failure. Departure/reconnect preservation is
source-qualified by room ownership, not an executed joined-room test. The cleanup
fixture does not qualify full room, audio or publisher teardown.

### Diagnostics, retention and qualification limits

The selected library, integration and compile-only SFU executables retain compiler
debug level 1, debug assertions, overflow checks, unoptimized test code, ordinary
symbols and unwind data, but omit final non-ALLOC DWARF through selected linker
stripping. Source-line diagnostics are reduced; no full-debug byte equivalence or
global source/dependency debug-profile change is claimed. The retained network
signaling executable archive is temporary and not reboot/power-loss durable;
archive byte verification is not another test execution.

Historical unsuccessful or uncertain checks remain historical, not converted to
passes by this batch. Separate artifact-accounting/retention failures are not
failed test cases and do not increase the runtime count. Sampled resource
observations are not hard aggregate memory/process quotas. Final lint left product
source unchanged; it does not establish byte-identical build-cache contents.

No hosted CI, full-workspace qualification, SFU runtime, hosted-room join, remote
media, RTP/codec/device behavior, full Python parity or complete replacement is
claimed. The earlier SFU startup denial remains unbypassed. Publication, remote CI,
merge and deployment are separate from the local checks above, not inferred passes.

### Source provenance

Published base (Batch162): `3a20dd352fef728f4d2a402b3845fe3601d9e52d`.
QP01/L01/R01/N01/S01/QF01 bind the same 2023-file Rust source inventory SHA256
`70c860a835622418241241d8d76ffaa355c87af02d40e7f9b4dda86df8aa5121`.
The six-file feature comprises this crate's README, `src/connection.rs`,
`src/lib.rs`, `src/protocol.rs`, `src/video_input/lifecycle_tests.rs`, and new
`src/active_speaker_tests.rs`.

Qualified test-executable SHA256 values:
- Library: `de26102fb4a748593eeacfde2dee1f03c3280eb1251c89eefb8c4ca238abbdf7`.
- `review_contracts`: `89bc94220150add5d90cc9cbc829459f4d1461442f592b6b52e02a0ff529f754`.
- `network_signaling`: `b72a830455d135b39347f68a4343ded678965a9ab7faccae50c1fae2f7b5dd98`.
- Compile-only `local_sfu`: `dff77d5bb14c66d6205bdf120a5506242e9e65e4b3768dfcac3fbd5350c49f2a`.

The subsequent documentation-only update changes this crate's README and
VALIDATION, not code, tests, dependencies or profiles. Earlier execution binds
the inventory above, not these later documentation bytes; the final documentation
binding separately checks unchanged code and the complete post-edit inventory.
Historical validation entries below are preserved and are not new Batch163 runs.

## Batch 162: bounded video subscription observations — 2026-10-22

`LiveKitEvent::VideoSubscribed` / `VideoUnsubscribed` expose effective selected
video ownership transitions, carrying participant identity, track SID and typed
source. They use the existing bounded control-event lane; `into_frame()` returns
`None`. They confer no callback, pixel-delivery or physical-capture authority.
The pipeline input and direct connection receiver are not independent observers.

Subscription follows successful ownership/reader installation (an initially
muted owner has no sink). Same-SID duplicates are silent; same-source replacement
joins the old reader and reports the new SID without fabricating an unsubscribe.
Matching unsubscribe invalidates pending requests/private images and joins the
reader before observation. Stale/duplicate unsubscribe is silent. Mute, departure,
reader failure, close and reconnect rebuild do not invent lifecycle events.
After reconnect rebuild, same-SID follow-up remains silent and a new SID emits.
Queue-full/closed errors propagate to terminal cleanup, not unbounded buffering
or notification retry. Already queued observations and already returned images
are not recalled; separate control/data lanes have no total-order guarantee.

Python reference remains `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/transports/livekit/transport.py`. Rust requires configured input and
selected participants, unlike Python callbacks that also fire with processing
disabled. Effective source/SID deduplication and stale-unsubscribe protection are
intentional hardening, not exact Python callback-count or event-API parity.
No dependency, manifest, lockfile or vendored SDK changes are part of this slice.

### Completed validation

Linux x86-64, Rust 1.99.0, pinned native SDK/WebRTC provenance as recorded below:

- L02: **97 library tests passed**, including seven new lifecycle tests, in
  **12.53 seconds**. Zero failed, ignored, measured or filtered tests.
- R01: **one `review_contracts` integration test passed** in **9.25 seconds**:
  `signaling_owners_release_on_cancel_abort_and_timeout`. Zero failed, ignored,
  measured or filtered tests. It covers synthetic signaling ownership, not SFU
  join or network/media delivery.
- N01: **one `network_signaling` integration test passed** in **4.17 seconds**:
  `real_websocket_transport_releases_on_cancel_abort_and_timeout`. Zero failed,
  ignored, measured or filtered tests. It uses real loopback TCP/WebSockets,
  not a hosted room or SFU, and does not establish remote video delivery.
- L02, R01 and N01 built, listed and executed their complete exact test inventories
  on the same frozen source, with natural zero exits and verified process cleanup.
  All three were independently verified. This is **99 distinct executed test cases**;
  listing, repeated builds, and scenarios inside one test add no test cases.
- S01: `local_sfu` freshly compiled and linked with natural zero exit and verified
  cleanup on the same frozen source. **Zero tests executed**: no listing, test
  runtime or server startup. Compile/link-only evidence was independently verified.
- QP01 preflight strict package all-target Clippy (`-D warnings`) and package
  `fmt --check` passed on the same source and were independently accepted.
- QF01 final strict package all-target Clippy (`-D warnings`) and package
  `fmt --check` passed with natural zero exits on the same frozen source and were
  independently verified. Clippy reported 373 cached compiler artifacts, all
  marked fresh by Cargo; this was a new lint invocation, not a fresh rebuild.
  Preflight and final static checks add **zero runtime tests**.

The seven new tests exercise the production normalized lifecycle handlers with
local native fixtures: selected/filtered/disabled and muted sources; duplicate,
replacement, stale/unpublish and reconnect counts; queue pressure and receiver
closure; start/join failure without false success; join-before-unsubscribe with
request/private-image invalidation; metadata boundaries/no frame projection;
and cancellation/oversize-start/unsubscribe-pressure rejection. Existing plain
request and video regression cases remain in the 97-test run. Native fixtures do
not establish physical-device, RTP/codec, hosted-room or remote lifecycle parity.

### Retained failure and executable diagnostics

L01 failed during the library build because an allocation scan exceeded its
unchanged time guard (3.727 seconds observed). The build was terminated (SIGTERM,
phase exit -15; overall exit 123), with cleanup verified and source unchanged.
It produced no qualified test executable, listing or runtime result and counts as
zero tests, not a pass. One retry, L02, passed under the same guards; the earlier
failure remains recorded rather than reclassified or hidden.

The library, `review_contracts`, `network_signaling` and compile-only `local_sfu`
test targets were linked with target-only
`-C link-arg=-Wl,--strip-debug` (and one linker thread). Compiler debug level 1,
debug assertions, overflow checks and unoptimized test code were retained.
Audits verified ordinary symbols, unwind data and load/TLS structures, with no
final non-ALLOC DWARF; the allocated `.debug_gdb_scripts` section remains.
Source-line debugging/backtrace detail is therefore weaker. These artifacts are
not claimed to retain full DWARF or to be byte-equivalent to full-debug builds.
No global source/dependency debug-profile change is implied.

### Qualification boundaries

Historical Batch161 integration/SFU/lint results below are not new Batch162
executions or silently carried passes. The earlier SFU startup denial remains
unbypassed; no SFU/netlink runtime experiment is part of this slice. No complete
CI, full-workspace validation, production readiness, full Python parity or
physical-camera freshness is claimed. Adapter budgets exclude downstream clones
and native upstream storage; sampled resource observations are not hard aggregate
memory bounds. Commit publication and remote CI are separate checks, not inferred passes.

### Source provenance

Published base (Batch161): `d4abf356a23e5fe18e5c8395f987c831634b9181`.
QP01/L02/R01/N01/S01/QF01 bind unchanged revision-2 Rust source inventory SHA256
`e5844a09a73a9f78584b0515ccf80bcf5a2f7bf97f3af59d2cb79b3314c119f3`
(2022 files). Revision 2 corrected inventory scope only; product bytes did not
change from revision 1. The five-file feature comprises this crate's README,
`src/connection.rs`, `src/protocol.rs`, `src/video_input.rs`, and new
`src/video_input/lifecycle_tests.rs`.

Qualified test-executable SHA256 values:
- Library: `467abd519cc37ed4a034e86d5396d1686abeb02827653f9fc6eb0b5109145c12`.
- `review_contracts`: `0bb05c4cbed1bccb09b69a5fd872d16bb17b91e837d9a3519b7bd30ffa554a60`.
- `network_signaling`: `1ec44eb227657637a8c4bc64e19db31e7e9f6a20a3658a75854b50bb8a1003f3`.

Independently verified compile-only `local_sfu` executable SHA256:
`e7079dfbddd3253e071658734cdabecda8d6fcec610d049737b63159006bc0dc`.

This documentation update is separate from the tested-source inventory.
The post-test documentation update is limited to this crate's `README.md` and
`VALIDATION.md`; all non-documentation bytes must match the tested source before
release. The documentation-inclusive manifest and source bridge are maintained
in separate qualification records, not embedded in the documents they hash.
That binding is not another execution or a new test pass.
Historical results below retain their original dates and scope.

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
