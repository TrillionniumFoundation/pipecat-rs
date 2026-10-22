# Native rewrite checkpoints — 2026-10-10

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

See the current [LiveKit validation](../crates/pipecat-livekit/VALIDATION.md) for provenance and retained failures.


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


This is a local, in-progress migration, not a completed rewrite or production
qualification. Original source baseline:
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`. Original files are preserved.

## Recovered fixed-source checkpoint: `ccf0dac` — 2026-10-10

Commit `ccf0dacbb8a1de97be0140c9c5b45f4a8315e801`, tree
`3093e67caba16e86f6e2473f532bf5eb1ef71059`, passed a fresh complete workspace
gate in a clean export with an exclusive Cargo target:

- All features, locked offline dependencies: **2,268 passed, zero failed,
  46 ignored**
- Strict all-target/all-feature Clippy and workspace formatting passed
- All 2,903 tracked repository blobs in the export matched the commit afterward

An earlier attempt was interrupted before a terminal result. Its partial log is
retained, but only the resumed terminal gate is counted. The final test log SHA256
is `1a3c55fd93a28d81304804ba1c50b7a3a53b7a8c315e8df3dda2942b2214d5e7`;
strict Clippy log SHA256 is
`b8d2aca21457372ecbf8ed1604137e78c559e03725fa5d2ad5ebc1d61ebe0b2c`.

Environment-dependent tests are not included in the 2,268 passing count. A
separate explicit native run on the same unchanged `ccf0dac` export passed all
**43 tests**, zero failed or ignored, across Whisper, Piper, RNNoise and the
runner. It used restored hash-verified models and native libraries; all 2,903
tracked blobs still matched afterward. Native test log SHA256:
`35a4743b49de2df91c542d76051c980b9721b1cabdf732f0d3aceb4d9448cd27`.
This is CPU/native inference and pipeline evidence, not physical playback or
live-provider acceptance. The remaining conditional checks include browser or
network environments that this host cannot currently validate.

The generated native application test subsequently passed with its own temporary
target directory;
`e58bd4111` prevents that test from sharing artifacts with another workspace.
This focused test-only change is distinct from the full `ccf0dac` gate.

See [RECOVERY.md](RECOVERY.md) for recovered scope, adverse findings, native
evidence boundaries, and the build-cache isolation requirement. Completion
strategy, typed payload observations, MoQ and AssemblyAI work remains outside this
fixed checkpoint until reviewed and integrated. Older checkpoint records below
are historical, not results from this replacement host.

## Fixed source checkpoint: `266412a`

Twenty-three native workspace crates, plus the separate browser Rust/WASM
workspace, implement the current runtime, voice pipeline, model inference,
media transport, worker RPC, rich contexts, Smart Turn, native Flow execution,
streaming settings/reconnection, global observations/metrics, and browser hosting.

A separate clean export of this exact commit passed:

- `cargo test --workspace --all-features --offline --locked`: **973 passed,
  one explicitly ignored environment-dependent Redis check**, zero failures
- `cargo clippy --workspace --all-targets --all-features --offline --locked --
  -D warnings`
- `cargo fmt --all -- --check`
- Separate browser workspace native tests: **11 passed**

The browser's optimized WASM build and native/wasm32 Clippy also passed on the
development host. Browser execution is blocked before application launch by a
Chromium socket permission failure; the separate cloud browser cannot access
this executor's loopback. No visual or physical-media acceptance is claimed.

The exported native test source includes original Python files read by source
parity compile-time fixtures, and tracked ONNX model assets. Rust runtime
execution never invokes Python. The host supplied native ONNX Runtime, CMake,
and cached locked dependencies as described by the main README.

A combined-workspace compile caught a name collision between the newly added
core observation enum and the existing RTVI `Observation` API. The core API is
now `PipelineObservation`; the above fixed-commit results include the correction.

Earlier shutdown, tool-continuation, Vision aggregation, and partial reserved-key
JSON codec regressions are fixed and covered. Official WebRTC 0.14 remains
vendored with provenance and exactly two modified upstream source files (SCTP
and DTLS lifecycle). No cryptographic protocol or wire format changes are included.

Flow tests exercise actual loopback LLM HTTP/SSE and Cartesia WebSocket synthesis
with an explicitly gated synthetic backend Played receipt. Post-actions wait for
inference, synthesis and that receipt; cancellation at each stage prevents stale
actions. This is not speaker hardware qualification.

The source patch archive was restored onto original baseline
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`. The resulting Git tree matched
`aabad63d155d3af20cfddad8502103fd6797c6a0` exactly. The archive is a recoverable
in-progress checkpoint, not evidence of remote publication or completion.

## Fixed source checkpoint: `7da6f75`

A clean export adds the standard RTVI WebSocket voice runner, native WebSocket
worker proxy with generation-owned registry claims, and additional HTTP speech
providers. It passed all-feature workspace tests: **1,030 passed, one explicitly
ignored Redis environment check**, zero failures. All-target/all-feature Clippy
with warnings denied and workspace formatting checks also passed. This run used
two build jobs and disabled development/test debug information to bound the
temporary build-cache footprint; test behavior and features were not skipped.

Runner integration covers real HTTP and WebSocket sockets, browser wire codecs,
stateful input resampling and synthetic STT/LLM/TTS services. Proxy tests cover
streaming jobs, cancellation, disconnect, routing and registry ownership. These
results do not establish live-provider, browser-process or physical-media parity.

## Fixed source checkpoint: `4993921`

A clean export includes the native project generator, independently reviewed
LLM/backend/UI workers, receipt-backed Spoken history, and the opt-in WebRTC
browser voice runner. **1,119 tests passed**, zero failed, and two were explicitly
ignored in the default all-feature workspace run. The generated-application
build/help test was then run explicitly and passed. The Redis authenticated
non-loopback route check remains unexecuted because its required route is absent.
All-target/all-feature Clippy with warnings denied and formatting also passed.

Four reproduced worker defects are fixed: coalesced full cancellation being
downgraded, stale buffered tool progress, a sibling tool's cancellation being
lost after peer panic, and UI speech routed only to a private job inbox rather
than the actual voice pipeline. Native Cartesia composition verifies that Spoken
history waits for both explicit backend Played receipts and the original source
response end, and withholds partial failures and interrupted contexts.

The WebRTC runner tests exercise actual Opus, ICE/DTLS/SRTP/SCTP, scoped signaling,
RTVI readiness, resampling and synthetic STT/LLM/TTS providers. Raw browser ping
messages are recognized without treating them as malformed JSON. Python's
three-second ping-age heuristic remains a documented difference; native liveness
uses peer state and configured session deadlines. Browser-process acceptance is
still blocked as described above.

The first build retry was necessary because `/tmp` is a separate 4.9 GiB tmpfs
that filled with build caches despite free space on the workspace volume. Only
inactive, reproducible caches were moved to workspace storage; source and evidence
were preserved. The successful rerun used the same locked source and features.

The recovery patch was restored onto the original baseline and its Git tree
matched `63b51541a85e0b8dc0cc464e2528f86b588f8ac8` exactly.

## Fixed source checkpoint: `1c51d84`

The clean export adds audited local Whisper, the official native LiveKit SDK
adapter with signaling task-ownership fixes, and native scripted evaluation.
All-feature workspace tests passed: **1,196 passed, zero failures, 13 explicitly
ignored**. These comprise eleven model-dependent Whisper tests, the Redis
non-loopback route check and the separate generated-application build check.
This default run does not count ignored model inference as successful execution.
Separate component reports identify the explicitly executed native-model tests.

Workspace all-target/all-feature Clippy with `-D warnings` passed. The vendored
LiveKit dependency retains 23 upstream compiler warnings; unrelated source was
not rewritten or warnings suppressed. Combined native dependencies built with
the documented workspace-local Clang/GLib/WebRTC and ONNX Runtime prerequisites.

Formatting of every native workspace package passed using `cargo fmt -- --check`
from the virtual workspace root. The earlier `cargo fmt --all -- --check` also
traversed local vendored dependencies and reported their existing upstream style
differences; that command did not pass. Vendor code is preserved for provenance,
not silently reformatted to satisfy the application formatter.

Subsequent commits fix the native Whisper eight-decoder configuration boundary
and shared streaming cancellation/response-failure fences, add audited ElevenLabs
streaming, and retain real LiveKit signaling-network qualification. The official
local SFU startup was denied before listening because the host forbids required
netlink introspection; the real-room harness is compile-only and explicitly
ignored. Successful SFU sessions remain unverified. Those later changes are not
part of this fixed-source whole-workspace result.

## Fixed source checkpoint: `688a2d8`

A clean export incorporates the audited Gemini Live client/processor, the
ElevenLabs and shared-streaming cancellation/failure fixes, the Whisper decoder
width boundary, and LiveKit signaling-network regression tests. All-feature
workspace tests passed: **1,275 passed, zero failures, 14 explicitly ignored**.
The ignored set is eleven Whisper model tests, one generated-app build test,
one Redis route test, and one real-SFU session test. Native workspace formatting
and all-target/all-feature Clippy with warnings denied also passed. Preserved
vendored SDK warnings remain distinguished from native workspace qualification.

Gemini review covers cancelled finish/teardown futures, API-key redaction,
response ordering, interrupted tool generations, and source-compatible generated
identities for missing provider tool IDs. The tests do not establish current
provider account/model availability or any live-provider acceptance.

## Fixed source checkpoint: `bb4260d`

A clean export adds the independently audited native Whisper browser runner and
MCP tools. **1,337 tests passed, zero failed, 25 were explicitly ignored** in the
all-feature workspace run. Native workspace formatting and all-target/all-feature
Clippy with warnings denied passed; vendored SDK warnings remain distinguished.
The ignored set consists of 22 model-dependent Whisper/runner checks and the
separate generated-app, Redis-route, and real-SFU qualifications. Their status
must be read separately rather than counted as passes in this default run.
The same exact source subsequently ran all 22 model-dependent checks explicitly;
all 22 passed. Only the other three named checks were excluded from that separate
run. This adds actual CPU recognition/VAD and native session ownership evidence,
not hosted-provider, hardware, or SFU qualification.

The first combined run at `b898e66` exposed a real scheduling race: core published
a returning flush marker before setting its completion event. `3a0a059` fixes it
by reserving output capacity, publishing completion, then sending through the
reserved permit. A synchronous receiver-waker regression fails on the old code.
Rejected admission never acknowledges; closure after reservation is explicitly
admitted, not a guarantee of receiver consumption. The passing run above includes
the fix and the original multi-destination media assertion without weakening it.

A first `bb4260d` compilation was intentionally interrupted before workspace disk
exhaustion. Only its inactive, root-owned reproducible build cache was removed;
source, models, dependencies and logs were preserved. The clean retry used the
same locked source/features. This interrupted attempt is not reported as a pass.

MCP review additionally covers cancelled close ownership, concurrent reopen and
admission, cancellable lifecycle-lock waits, protocol-version validation, null
arguments and fixed values, and model-facing error filters. Native fixture servers
are feature-gated. Remote side effects may precede cancellation or disconnect;
no automatic replay is not an end-to-end exactly-once guarantee.

## Fixed source checkpoint: `1d3afc0`

The next clean export includes audited identity-aware user-idle behavior, native
CPAL capture/submission-only output, and bounded YAML Flow loading. All-feature
workspace results: **1,437 passed, zero failures, 25 explicitly ignored**. Native
workspace formatting and all-target/all-feature Clippy with warnings denied pass.
The unchanged model-dependent checks remain distinguished from their explicit
execution at the earlier `bb4260d` checkpoint.

The actual Linux ALSA null-device example also ran at this exact source. It
exercised zero capture, concurrent submission, interruption/reopen, old-owner
isolation, pipeline End/Cancel and complete native thread cleanup. The device
explicitly discards output and generates zero input: this does not qualify a
microphone, speaker or audible playback. SubmissionReceipt is never Played.

User-idle review verifies typed tool identities/finality, bounded replay memory,
atomic rejected-control handling and recovery after nonfatal callback errors.
Flow review covers construction-time alias/node/scalar budgets, tag rejection,
confined descriptor-relative includes and deterministic root-ancestor symlink
replacement. Includes are Unix-only and require an explicitly trusted root.
Legacy merge keys and permissive multiline indentation remain rejected rather
than bypassing the maintained scanner.

Combined Linux validation used a workspace-only union of the verified LiveKit
and ALSA development sysroots, with a single pkg-config sysroot and no system
installation or security-setting changes. Independently sourcing their two
single-package environment files would overwrite PKG_CONFIG_SYSROOT_DIR and is
not a valid combined setup. The union only links previously verified package
files; it does not duplicate native dependencies or modify them.

## Fixed source checkpoint: `cacad65`

The next clean export adds independently reviewed processor usability, context
summaries with owned inference, and completed-aggregate text alignment utilities.
All-feature workspace tests passed: **1,552 passed, zero failures, 25 explicitly
ignored**. Native workspace formatting and all-target/all-feature Clippy with
warnings denied also passed. All 22 model-dependent Whisper/runner checks then
passed in a separate explicit run at this same source. A separate fresh generated
application also built offline and ran its credential-free help command; this
checks external-workspace dependency resolution, not a live voice session. Only
the Redis-route and real-SFU qualifications remain unexecuted from that ignored
set. No later working-tree changes inherit these runs.

The text suite replays 304 static traces from pinned source behavior and documents
one deliberate hyphenated-word boundary correction. The Rust tests consume static
fixtures rather than starting a Python runtime. Context-summary review covers
concurrent suffix preservation, tool-safe prefix boundaries, rich-message identity,
owned inference cancellation, repeated requests and provider wire budgets. Core
health review covers boundary-time policy, late report rejection, fatal-error
preservation and cancellation winning over a staged graceful terminal frame.

## Fixed source checkpoint: `6c3a8b1`

This export includes reviewed service switching, speech-adapter usability and
TOKEN promotion. All-feature workspace tests passed: **1,643 passed, zero
failures, 31 explicitly ignored**. All-target/all-feature Clippy with warnings
denied and native workspace formatting passed. A separate explicit run passed
all **28 actual-model Whisper/runner checks plus one fresh generated-application
build/help check**. Only the real-SFU and non-loopback Redis qualifications were
excluded from that separate run.

The preceding `9ae38de` export also passed its tests and Clippy, but formatting
caught three reviewer test files using the wrong Rust formatting edition.
`6c3a8b1` changes only their formatting/import ordering to the workspace's 2021
edition. The full test, strict-check and explicit-model/application runs were
repeated at the corrected source, rather than reporting the failed check as a
pass or weakening it.

Switcher review reproduced and fixed stale-system metadata admission races,
private settings-copy leaks after an evicting identity window, and terminal
admission versus failover races. Opaque bounded local tags replace the evicting
suppression history; atomic causal-fenced admission supplies the actual cutoff.
Read-only generation queries remain observations, not admission leases. Nested
Stop cleanup is exactly once; nested compound leaf metadata remains unsupported.

All six speech adapters and the Whisper runner wrapper share revision-safe work
gating and fresh recovery. Invalid/no-op settings preserve the last usable
configuration rather than spuriously recovering a failed service. Deepgram's
exhausted retry budget remains exhausted until an effective recovery action;
individual transient connectivity failures retain their classification. Original
response/flush fences remain intact. The source's consecutive-zero-audio TTS
watchdog remains an explicitly documented separate gap.

TOKEN promotion retains exact context/segment identities, transformed text
boundaries, early word replay and explicit synthesis completion. It includes
51 native source-trace replays plus native adversarial contracts. No-timestamp
completion is Unreported evidence; it does not create Played receipts or audible
history. Provider integration is subsequent work outside this snapshot.

## Fixed source checkpoint: `5c262d1`

Exact source: `5c262d16146c82114801d9dc9255a35c4f61048c`. A clean export
of **all tracked repository files**, not a working-tree copy, passed:

- All-feature workspace tests: **1,942 passed, zero failed, 40 ignored**
- All-target/all-feature Clippy with `-D warnings`: passed
- Native workspace `cargo fmt -- --check`: passed
- The same source explicitly ran **38 additional checks: 37 model/native-runtime
  checks and one fresh generated-application build/help check**, zero failures
- The two remaining ignored checks are the authenticated Redis non-loopback
  route and a real LiveKit SFU room. Neither is counted as passed

Model qualification uses the already pinned CPU ONNX Runtime, Silero/Whisper
assets and explicit local Piper manifest. Piper includes genuine text-to-PCM
execution and separately constructed tiny ONNX fixtures for adversarial ABI and
resource tests. These evidence classes are distinct; generated invariant graphs
are not a replacement for the real voice model.

An earlier exact `9dbdae6` combined run had **1,833 passed, four CLI failures and
31 ignored**, plus a formatting failure. It is not an accepted checkpoint.
The new provenance mapper carried original whitespace in text partitions but
also requested inserted word spacing, doubling spaces in assistant history.
The fix preserves aggregate-boundary spacing only on the first appendable source
partition. Five new real-loopback playback tests and the unchanged CLI, smoke
binary and real Silero assertions pass here. Workspace-edition formatting is
also corrected. The intermediate fixed `59b0ba1` workspace passed **1,917 tests**,
strict Clippy and formatting before the standalone Piper additions.

An initial limited export omitted original `evals/release/manifest.yaml`, which
one new source-rejection fixture reads. Its failure was an incomplete validation
export, not a production-code pass or defect. Re-exporting the whole tracked
source made all **73 native eval tests** pass at exact `85575aa`; the successful
whole-workspace result above includes those same source files and tests.

### New reviewed behavior in this checkpoint

- Manual/failover service switching, revision-safe speech health, TOKEN
  promotion, composable turn start/mute/stop and bounded empty-turn recovery
- Default recovery follows the source interrupted-turn prompt, keeps heard
  assistant prefixes, consumes a bounded consecutive budget, and coalesces tool
  continuation. Only genuine accepted transcript content resets that budget;
  mute, pending tools and terminal events suppress recovery
- Cartesia, HTTP and ElevenLabs TTS provenance/output-clock integration, exact
  source partitions, early/split word reports and context-owned metadata.
  Timestamp-free completion remains **Unreported**, not audible history
- Dedicated native OpenAI Responses HTTP/SSE and serialized WebSocket inference,
  with ambiguous-outcome no-replay rules and cancellation/continuation ownership
- Compiled native-bot hosting and bounded release-manifest execution. A reproduced
  concurrent-launch port-reservation inheritance race is fixed; 768 additional
  scoped stress launches were observed at the eval implementation checkpoint.
  Direct-child-only, OS reap deadlines and Windows inherited-pipe limits remain
  explicit; audio/simulation/judge modes are not implemented by this feature
- Optional standalone native Piper CPU synthesis. Independent review fixed
  normalization/defaults and a capacity-one result/admission race; 5,000 immediate
  resubmissions passed. The actual reviewed sample was understood by a separate
  native Whisper run with a documented transcription deviation. See Piper's
  `PROVENANCE.md` for artifact identities and the observed WAV hash

Piper here is a buffered native service, **not a completed pipeline TTS adapter**.
Native library/model loading is non-abortable, and bounded output copies do not
bound internal ONNX allocations. Patched eSpeak is an explicit optional GPL
backend. Its model card's public-domain dataset statement does not establish
weight-redistribution permission; model weights and native binaries are not
included in the source archive.

The reported long repetitive-alignment stall was investigated and corrected:
the exact 4,100-byte/4,101-token input exceeded the 4,096-token limit and rejected
before connecting. An old test then awaited its mock server before checking the
error. No production CPU stall was established by that observation. The new
bounded admission regression verifies rejection without a connection or leaked
TTS output. Separate unoptimized valid-input latency measurements are not a
real-time performance qualification.

## Subsequent work and remaining scope

Video-source sampling/request lifecycle, the source zero-audio TTS watchdog,
Piper's pipeline adapter and signed provider-history extensions are subsequent
work outside this fixed snapshot. They do not inherit its test results.

[COVERAGE.md](COVERAGE.md) indexes partial behavior and remaining contracts.
The immutable baseline inventory is not a current absence-of-code claim.
Remaining migration includes further provider and transport coverage, completion
markers/eager speculation and tool-effect gates, richer opt-in observations,
evaluation simulation/judges, advanced browser/client templates, every remaining
example and complete packaging/platform qualification. No browser, physical
microphone/speaker, public NAT/TURN, paid/live-provider, deployed Redis/PostgreSQL
or production acceptance is claimed. Frame, crate and passing-test counts are
not whole-project completion percentages.
