# Source recovery and current qualification

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

This is a scoped addition; the older checkpoint and coverage records below are
historical, not a new whole-workspace gate. See the current
[LiveKit validation](../crates/pipecat-livekit/VALIDATION.md) for the retained attempt history.


This is a partial native Rust migration. It is not a full Python API parity,
live-provider, hardware, packaging, or production-readiness certificate.

## Executor replacement

The execution workspace was replaced during qualification on 2026-10-09 UTC.
The durable fourth source checkpoint was restored at
`4e0ff9e32852ba1fe689288f4e3b0a9ecf0abe0c`; its tree
`eee6d1eac086b1f50691284286a5118e5f9e80be` exactly matches the saved checkpoint.
The original Python baseline remains
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

Later local-only source increments and some logs did not survive. Recovered
increments use new commits. Where a complete prior file was unavailable, the
implementation or tests were reconstructed and reviewed again. Neither an old
commit identity nor an old test pass is inherited by reconstruction.

Recovered commits:

- `3bc1815b9`: bounded native continuation carrier and manual wire guards
- `2fe76f7eb`: optional native RNNoise filter and qualification tests
- `e2dfbf309`: reconstructed owned observer cleanup and serialized deadlines
- `db8cbb393`: fail-closed turn tracking and numbered audio recording
- `c88768f4d`: generation-fenced consecutive-zero-audio TTS watchdogs
- `1eec544bc`: reviewed Anthropic and Gemini native continuation
- `f503f5a55`: bounded media admission with repaired Flush epoch and metadata checks
- `38dae8804`: video/source sampling and request ownership
- `55054fc04`: owned downstream capacity admission
- `e4ebada9d`: Piper pipeline with early health cancellation
- `e6a07527e`: reconstructed explicit-auth Azure Chat transport

The separate original turn-tracker reviewer fixture was not recovered. This is
explicitly distinct from the recovered recorder review tests and newly added
observer lifecycle review tests. No source-module completion percentage follows
from these commits.

## Build-cache isolation requirement

A controlled reproduction showed stale Cargo artifacts being reused between
different copies of this workspace when they shared one target directory. The
copies had distinct source paths but coinciding compiler artifact identities.
Serializing builds alone did not prevent reuse. Consequently, early recovery
passes obtained from that shared target are provisional for source provenance.

Every distinct source checkout or validation export must have an exclusive
`CARGO_TARGET_DIR`. Do not reuse another checkout's target even after its command
finishes. Resource serialization may additionally limit concurrent native builds.
Capture source hashes before and after a gate, and accept it only if the source
remained unchanged. The obsolete shared build cache was removed; source and
validation evidence were preserved.

## Fresh, source-bound checks

All checks below used the canonical checkout's exclusive target, locked offline
dependencies, and unchanged source hashes. Counts include doctests where present.

At source tree `854ea0d73850953892f68291728d426f86f01fb9`
(`c88768f4d`):

- Eight affected packages with all features: 644 passed, 0 failed, 5 ignored
- Strict all-target Clippy and package formatting passed
- Default media transport and RNNoise checks: 128 passed, 0 failed
- Explicit native RNNoise tests: all 5 passed against the restored, hashed native
  library and same-artifact C reference fixtures

At `1eec544bc67b790085934d8c1f77b749a7a48e7f`, tree
`0ebc3f861be741ce892b77def11b0eb15254f07e`:

- Twelve affected packages with all features: 1,205 passed, 0 failed, 1 ignored
- Strict all-target Clippy and package formatting passed
- Packages: core, services, turns, processors, responses, network-bus, protocol,
  observers, media-transport, speech, streaming-speech, and elevenlabs
- The ignored test requires a non-loopback route; it was not counted as passed
- Independent signed-continuation review separately passed all 22 cases on the
  frozen recovery source, including cancellation before tool side effects

These are overlapping suites, not additive counts of unique project tests. They
do not replace a new whole-workspace qualification after all recovery increments
are integrated. Historical whole-workspace evidence in `CHECKPOINT.md` remains
historical. Browser and hosted-SFU observations from the prior host must be
rechecked before making claims about this replacement environment.

## Remaining recovery and migration

Recovered media admission passed a canonical media/flow integration gate of
248 test invocations, plus strict Clippy and formatting. Combining video exposed
a test-harness dependency defect: a synthetic test crate recompiled the private
queue without its new video dependency. All 12 independent regression bodies
were moved unchanged into the real queue module, removing 18 duplicate queue
test invocations. The repaired canonical media/flow/video gate at `38dae8804`
passed 257 tests, strict Clippy, and formatting. Flush epoch and retained metadata
defects found during independent review are fixed.

Piper and Azure were integrated after independent review and are included in the
fresh whole-workspace `ccf0dac` gate: 2,268 passed, zero failed, 46 ignored, with
strict Clippy and formatting. All 2,903 tracked repository blobs matched the
fixed commit after validation. Isolated Azure passed 151 services tests, including
28 Azure cases. Piper's final isolated all-feature run explicitly selected every
ignored native test: 54 passed, none ignored. Its core suite passed 153 tests,
with additional feature-specific gates and strict Clippy/formatting. These
overlapping counts must not be summed as unique acceptance cases.

Piper review reproduced active and queued native work retaining ownership behind
a blocked predecessor fence after a health change. The early-health-watch fix
passes both regressions; removing that fix makes both final tests fail. A fresh
real-model replay on the repaired source preserved 789,106 native PCM bytes plus
654 zero-padding bytes through slow output with default queue settings. This
exercises count pressure, not saturation of the 2 MiB media byte limit. The
backend reported Discarded, and no spoken-history commitment was emitted.

The replacement-host Rust/WASM client build is restored, with an external-target
packaging-path fix. Actual Chromium launch remains blocked before page load;
see the browser workspace's `VALIDATION.md`. These completed recovery slices do
not imply that the larger migration inventory is complete.

The broader provider, transport, example, evaluation, browser, device, and release
scope in `MIGRATION.md` remains open. Native provider signatures are opaque
continuation data, not locally verified cryptographic proofs. Audio submission or
discard is not evidence of audible playback. No runtime invokes Python, and no
model weights or native binary libraries are included in this source recovery.
