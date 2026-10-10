# Native Rust rewrite publication

The homepage README is intentionally empty. Native implementation and technical documentation are in `rust/`. Run Cargo from that directory. This is an in-progress rewrite, not a full parity or production-readiness certificate.

Source: local rewrite commit `1099816e243002175d992ea044a07738de8cea51`, based on public ProfHepta/pipecat baseline `af9f9da9d7afddaa8811c0e0251b32566c980bdf`. Native Rust implementation paths and file bytes are preserved. The original complete Python project remains available in its upstream repository; this publication does not import its workflows, agent configuration, symlinks, ordinary Python examples or development environment.

Three original Python files are retained solely because Rust source-parity tests use `include_str!` to read them as text (Azure and Grok contracts). Two existing licensed ONNX model fixtures are retained at the paths used by native VAD/Smart Turn tests; their provenance/license records are in `rust/crates/pipecat-vad/`. Rust runtime and Cargo tests do not invoke Python. Python fixture-generation utilities inside `rust/` are offline developer reference tools, not runtime dependencies.

Root and vendored licenses/notices are preserved. No personal files, login material, downloaded native SDK binaries, private model downloads, local build caches or agent work directories are included. The vendored whisper-rs upstream pull-request template is omitted from this publication; implementation and license bytes are unchanged.

## Validation boundaries

The earlier fixed source `ccf0dacbb8a1de97be0140c9c5b45f4a8315e801` passed 2,268 workspace tests with46 conditional tests ignored, strict native Clippy and formatting;43 native conditional tests also passed separately. These are historical source-bound results. A fresh historical combined gate for `2fdf27e440b4bdbfb50ba63a0e6f4d09457f7130` had2,484 passes,1 failure and46 ignored tests. It exposed a global serde raw-value feature interaction that reinterpreted literal user objects. Commit `ba4f93207` removes that feature; unchanged regression and independent feature-mutation controls now pass. The current source additionally includes native Moonshine (13 deterministic and8 actual engine tests), reviewed eager speculation (944-test slice), and Eager-integrated Bedrock (988-test slice,1 pre-existing network fixture ignored). Strict native lint/format and source hashes passed for these slices. These counts overlap; the current complete publication needs its fresh combined gate. Counts overlap and cannot be summed. The publication projection itself still requires a fresh-source gate.

Live provider accounts, actual browser/device audio, hosted SFU operation and non-loopback Redis routing are not established by local fixtures. Migration inventory tracks original scope, not percentage completion.

## Public projection portability correction

The first frozen full public-projection test run at remote commit `51e79e0e90aa4dd08dab2c0470c0e6c0cd9ea8a5` failed only the eval process-hosting target: its Python-manifest rejection test depended on an excluded parent-directory fixture. The test now uses a provenance-marked, crate-local minimal fixture and checks the specific `Unsupported` diagnostic. The isolated package gate passed 73 tests, strict all-target/all-feature Clippy and formatting. Deleting the fixture, changing the error variant, and changing the diagnostic each fail the strengthened regression. This is not yet a full-workspace pass for this updated source.

The optional facade now exposes AWS Bedrock, AWS speech, MoQ, Piper and RNNoise. Five no-I/O API tests, strict all-target/all-feature facade Clippy and format checks passed. Default dependencies exclude all five; Piper ONNX/GPL and RNNoise native backends still require separate explicit opt-in. This API-wiring gate does not claim hosted-provider or physical-device acceptance.

## Native MoQ pipeline integration

The native input/output processors now provide bounded queues, stateful audio conversion, owned shutdown, and interruption-aware publication. Independent qualification on exact base `67f9b26a269b0937cfdaa0062d09c6f786cb28b5` plus the v3 patch passed 391 core/audio/MoQ/network-bus tests, strict core/MoQ Clippy, formatting, source hashes and feature checks. One existing non-loopback network-route test was ignored; no MoQ test was ignored. Two isolated mutation controls failed as expected.

V2 was rejected before publication because a canceled middle forwarding origin could contribute buffered PCM when different origins shared an optional audio context ID. V3 enforces an allocation-free causal-scope comparison before append and flushes/drops the previous converter under its original scope. A real three-origin QUIC regression checks exact surviving Opus packets, while explicitly uninterruptible output remains supported.

Serving, reconnect/replay, hosted relay/browser interoperability, video/rich output controls and actual playback acknowledgments remain unqualified or unsupported as documented in `rust/crates/pipecat-moq/PIPELINE.md`. No playback/heard-text receipt is fabricated.

The earlier frozen public projection `51e79e0e90aa4dd08dab2c0470c0e6c0cd9ea8a5` finished with 2,602 passing workspace tests, the single subsequently fixed eval-fixture failure, and 54 ignored. Its strict Clippy, native formatting and all 51 explicitly selected native-engine cases passed. The frozen public projection `0d8b9d227730d5e82e5cd3e6f8c9815711f24312` subsequently passed 2,608 workspace tests with zero failures and 54 conditional tests ignored, strict all-target Clippy and native formatting. Explicit runs passed all 51 native-engine cases and the generated-application offline build/start check. Only the Redis non-loopback and LiveKit SFU cases remain unexercised at that checkpoint. These historical results do not certify this newly extended source as a whole.

## Native Flux and classifiers

The optional standalone Flux v2 crate passed 30 authored/independent tests, strict all-target Clippy, formatting and source hashes. Three old-source controls reproduced the corrected health/speculation, End-drain and ConfigureFailure lifecycle defects. All ten independent positive regressions are shipped. Live Deepgram service acceptance, SageMaker and Flux TTS are outside this qualification; see its crate README.

The typed LLM/Jev classifiers crate passed 36 tests (one private unit, 23 authored integrations, 12 independent regressions), strict all-target Clippy, formatting and source hashes. Four compiled old-source controls failed at their intended assertions. New bounded JSON construction, schema admission, cancellation fencing and derived-score endpoint handling are covered. The only new external dependency is h2 0.4.20; all prior locked identities/checksums are preserved. Synthetic peers exercised HTTP/1.1 and native LLM SSE, not negotiated HTTP/2, live Jev, or calibrated accuracy. Scope differences and response-schema limitations are in the crate README and SOURCE_MAP.

These exact crate files were integrated without modification, and the combined lock was checked offline. A new full-workspace qualification of this extended publication remains pending; prior checkpoint and slice counts cannot be added or transferred as whole-source acceptance.

The optional facade now exposes `classifiers` and `deepgram-flux` without changing its default dependency closure. Isolated feature and combined public-API smoke tests passed, as did strict all-target facade Clippy and formatting; independent review verified the exact six-file patch and unchanged runtime bytes. This is API wiring, not a new whole-workspace qualification.

## Native OpenAI image generation

Native bounded generation, explicit-origin credential-free hosted download, PNG/JPEG RGB decoding and shared cancellable output delivery are implemented. Independent qualification passed 433 native all-feature core/media/image tests and 6 vendor tests with zero failures/ignores, strict all-target Clippy, native formatting and source integrity. Sixteen independent regressions ship with the crate. Original JPEG and pre-slice media negative controls failed as intended.

Disabling Rayon did not prevent upstream JPEG detached threads. The direct-path, exact-version jpeg-decoder 0.3.2 dependency now unconditionally selects the existing immediate worker and synchronous conversion; only src/worker/mod.rs differs from the official archive implementation. Licenses, original member hashes and exact patch are retained. Its upstream .github workflow and appveyor.yml are omitted from this public projection, consistently with the existing workflow exclusion. These two omissions do not change implementation bytes; UPSTREAM_SHA256.json intentionally lists the complete upstream archive. Cargo registry publishing of pipecat-images is disabled to avoid path stripping restoring detached behavior; GitHub source consumers retain the patch.

The source-cat JPEG is byte-identical to the pinned public Python baseline examples/assets/cat.jpg; the flower benchmark JPEG is byte-identical to the official jpeg-decoder 0.3.2 archive. These are public upstream fixtures, not user media. Neither provider availability, other image adapters, physical rendering nor a new whole-workspace pass is claimed. The frozen fe48acc facade/Flux/classifier workspace gate runs independently and does not include this image slice.

## Soniox and subsequent qualification checkpoint

Native Soniox streaming STT and multiplexed TTS are implemented as a standalone optional crate. One TTS socket carries independently bounded/cancelable stream IDs. Source token finalization, settings reconnect/snapshots, PCM/word carry and atomic packet validation are covered. The exact crate passed 32 tests (20 authored and 12 independent real-loopback/native-pipeline/receipt tests), strict all-target Clippy and formatting. Old-source lifecycle controls and a stop-before-final mutation failed their unchanged assertions. All earlier locked packages are preserved; only one local package entry is added.

Hosted Soniox/TLS, account/model/voice availability, recognition/synthesis quality, latency and multi-context response-history certification are not established. No transport retries/replay are added. See the crate README and SOURCE_MAP for native limits and explicit session restoration.

The frozen public projection fe48acc5fd6efcc8c1e343d506090dd8bcd7df8e (tree bcaff7fdb5deea44842c22ef01088d41466bca88), before images and Soniox, has now passed 2,716 workspace tests with zero failures and 54 conditional tests ignored, strict all-target/all-feature Clippy and native formatting. All 51 selected native cases and the generated application's offline build/start acceptance passed explicitly. All 1,157 tracked blobs stayed unchanged. Redis non-loopback and LiveKit SFU cases remain unexercised. An initial unsupported cargo-test flag exited before compilation and is preserved separately from the supported successful gate. These source-bound results do not certify the later image/Soniox extensions as a whole.

## Native Grok realtime provider boundary

The native xAI/Grok realtime crate passed 53 tests, including 21 independent tests, strict all-target Clippy, formatting and source integrity. It implements source-specific handshake/settings, wire audio/transcription, tool-result timing, bounded lifecycle and receipt-backed output behavior. Function results do not automatically request a response; upstream inference remains explicit. Failed original-source controls, corrected fixture errors and prior rejected candidates remain recorded separately. Only one local package lock entry is added.

This qualification is intentionally limited to the explicitly seeded context/provider/tool/output topology. Raw detailed append/update/transform controls fail closed in either direction before forwarding; this does not undo or reconcile edits already consumed by an upstream aggregator. Common realtime aggregation, reliable executor terminal cancellation receipts, arbitrary provider-history reconciliation, optional facade wiring and live-provider acceptance are separate prerequisites. Current native shared-layer work is not included or certified by this publication.

## Shared native realtime aggregation

The paired realtime aggregation slice passed 1,308 tests with zero failures and 12 pre-existing Whisper-fixture ignores, strict all-target/all-feature Clippy across the selected 12 packages, formatting and frozen full-source hashes. Ten independent ownership controls prove release on End/Stop/Cancel, failed startup, abrupt drop, and retained action clones after successful or failed emission. Prior candidates failed adversarial stale-origin, duplicate-history, Smart Turn wrapper and runtime-retention controls before correction. The only post-gate change corrects documentation about the existing OpenAI adapter's downstream guard.

This slice provides shared final-transcript buffering, TTFS handoff, bounded pending text, state-only trailing context snapshots, causal fencing and explicit route lifetime. It does not yet qualify any provider's full realtime topology. OpenAI's downstream context handler still ignores the state-only marker; Grok's provider integration and typed executor cancellation need their own combined qualification. Smart Turn realtime wrappers, eager realtime and signed cascade carriers are explicitly unsupported. The previous full projection checkpoint remains fe48acc; these overlapping slice counts are not a new full-workspace or live-provider certificate.

## Images, Soniox and Grok facade opt-ins

Three new opt-in facade features expose the standalone image, Soniox and Grok APIs without changing the default closure. The independently reviewed six-file slice passed all 21 validation commands: ten strict all-target Clippy configurations, nine selected-feature smoke configurations, and default/all-ten facade suites (11 and 21 passing tests respectively). Formatting and exact pre/post source hashes passed; only three local facade lock edges changed. Smoke cases perform no provider I/O and do not expand provider qualification. The common aggregation and facade patches have been combined here; their separate gates do not replace the pending fresh whole-source qualification.

## Reviewed native terminal ownership and assistant history

This increment preserves immutable native executor terminal decisions through the Grok relay and common assistant. It seals result/error/inference policy, carries validated declaration-batch ownership, reconstructs terminal-first sibling groups, reconciles late admitted success without fabricating cancellation, and rejects forged native carriers at ordinary wire boundaries. Active authority is never evicted to make space. Tool-ID ledgers and signed-history mirrors have separate explicit encoded-byte/count limits; they are not one global RSS budget. See rust/crates/pipecat-turns/TOOL_HISTORY.md for exact bounds and deliberate legacy opt-out semantics.

Frozen v6 passed 1,164 tests across the selected nine-package integration scope plus 47 MCP native-fixture/doc tests, strict all-target Clippy, formatting, exact patch reconstruction and unchanged complete source hashes. All 37 independent common regressions are included. Exact-source content-seal and declaration-ownership mutations fail the intended assertions while their companion controls pass. The positive source manifest is 1c9831f31247c2fe5116c69dcbcd5996051b25c40deeee1f039257162edf3cc1; reviewed combined patch is dd9f67c663c277acd96e7b94acfd0e3ec0a0fe6a274eadd76616fe197d836c87. Earlier missing-static-fixture compilation failures are preserved as failed packaging attempts, not behavioral passes.

This is scoped qualification, not a new full-workspace certificate. The latest full projection gate remains fe48acc (2,716 tests, 54 explicit ignores, 51 native cases and a generated-app check). A pre-existing nested-parallel graceful shutdown race is now deterministically reproduced and being corrected separately. Full OpenAI/Grok realtime pairing, explicit context-cause synchronization and its concurrent history-replacement boundary remain separate unpublished work. Neither live provider, physical playback, remote exactly-once effects nor complete Python parity is claimed. README.md remains empty.

## Nested composition graceful shutdown

Child runtimes now stage End/Stop until their entire owned composition drains prior accepted work in both directions. Root success waits for descendant cleanup, counts completed-task result publication as owned work, and preserves cancellation and absolute root deadline admission. Bounded weak group membership and successful completed-child snapshot release prevent unnecessary ownership retention. Stop retains resources until explicit cleanup. Root failure/report does not imply every child's already-started bounded cleanup has stopped; retained failed-root senders can retain bounded runtime metadata.

The independently reviewed lifecycle slice passed 419 tests across 58 suites, strict all-target Clippy, formatting, source hashes, 50 original-case repeats and 20 repeated two-target adversarial suites. The original two deterministic probes fail on pristine 3ecb and pass unchanged on the final source. Two exact-source independent mutations compile but fail specifically when admitted cancellation or task-result ownership is omitted. Source manifest 4a7f2c2dda6257cf23e345f609e946216168d1283db3848447c096f6fe112d97, patch c18a725098f05705b5ce216e570a9a0894a3735a06daa2a301b2a61c4178364e. This corrects the previously described nested shutdown race; new realtime context/provider pairing remains separate unpublished work.

## Portable compressed-metadata fixture and whole-source checkpoint

The frozen pre-correction public projection (tree
633e78ae5bfbfafe5ab7ad07b0d557409a444060, canonical source
1099816e243002175d992ea044a07738de8cea51) completed its full gate with 3,008
workspace tests passing, one image-fixture failure, and 54 conditional tests
ignored. Strict all-target/all-feature Clippy, native formatting, all 51 explicit
native-engine cases, and the generated-application build/start check passed.
All 1,275 tracked blobs remained unchanged. This was a failed whole-workspace
qualification, not a release pass.

The failure occurred before service decoding: workspace feature unification via
MoQ enabled flate2 1.1.10's zlib-rs 0.6.8 backend. Its fast-compressed PNG was
20,368 bytes, while miniz_oxide 0.9.1 produced 9,712 bytes; both contained the
same valid 2 MiB text payload and RGB pixel. The test incorrectly depended on a
particular compression ratio for its compact-fixture premise.

A three-path, test-only correction now checks in the deterministic 9,712-byte
fixture and verifies its complete 2 MiB all-x text, compact encoded size and
native RGB output under the unchanged 128 KiB decoding budget. Production
code, dependency identities and safety limits are unchanged. Independent review
passed all 36 isolated image tests and all 120 image-plus-MoQ unified-backend
tests, strict scoped Clippy and formatting. An independently rebuilt control
changed only the valid fixture text to one byte; the unchanged test failed at
the exact 2,097,152-byte inflation assertion. All 1,277 candidate files remained
unchanged. The reviewed candidate tree is
ac2492fa6b4d47fe1d9e792178e651e1bcfb4a9e; this publication adds only this
qualification annotation and lifecycle-note spacing to that tree. Its Rust
files are identical. The corrected whole-workspace/native qualification is
still running; no new complete-workspace pass is claimed yet.

Canonical integrated source: 8b844ec09a9aea129d12fc8bbe53445cdcd0e119.
OpenAI/Grok paired-provider work remains a separate unpublished increment.
The root README remains empty.

## Updated complete native checkpoint

The corrected frozen projection at tree ac2492fa6b4d47fe1d9e792178e651e1bcfb4a9e completed its entire native/offline gate: 3,009 workspace tests passed, zero failed, and 54 conditional cases remained ignored in the workspace invocation. Strict all-target/all-feature Clippy and formatting passed. The explicitly enabled native-engine gate separately passed all 51 cases, and the generated application passed its build/start check. All 1,277 tracked source files and 39 native asset/tooling hashes remained unchanged. Public commit 74c7d5b959428787adb04e050c2aef20789134ed has exactly the same Rust files; its differences from the tested tree are qualification prose only. The earlier failed fixture gate remains recorded above, not erased or relabeled as passing.

This checkpoint predates the following paired-provider and Gemini additions. Those additions require their separate combined integration gate; overlapping package test counts cannot be added to the complete-checkpoint total. Hosted provider credentials, live recognition/generation quality, actual SFU operation and physical audio/video devices are separate acceptance boundaries.

## Reviewed paired OpenAI/Grok realtime inference

Explicit context edits and assistant inference now carry bounded, immutable local causes. User and assistant sequences are independent. Provider actor deferral and socket admission retain the original causal restrictions, including when only the assistant scope expires. Held inference permission cannot become fresh merely because the last provider tool result arrives. Only the shared assistant selects a genuine newly eligible native terminal reason; providers do not reproduce or weaken its group/result/cancellation policy. Durable admitted terminal history remains independent of stale inference suppression.

Common snapshot lineage is checked under the context mutation lock; equal-sized replacement cannot silently substitute another append range. Tiny handler identities prevent pointer-reuse ABA without retaining executable closures. Unconsumed causes are rejected by both serde and explicit wire encoders. Authority intersections preserve every contributing scope and remain bounded. Ordinary edits that have already changed local state cannot be truthfully rolled back by a later unsupported provider operation; failure semantics are explicit.

Exact reviewed patches: common7 4789f247321b1a2a6d964cca1ebd8ef98f1ef0446a120f002ed045bd3e09efd4; Grok provider 6514524cbf16dd4c8becd8e3a34f88f17607de3cd9f0a759685c8689de99bd15; OpenAI v15 2eaf561aa777523a6771d3bd273a54c9042e159f403d1a0ae76e0d07fa06983a. Common/Grok passed 847 selected tests; OpenAI passed 72 provider tests. Both passed strict scoped Clippy, formatting and immutable source verification. Exact-source independent mutations expose stale inference when original intersection or actor fencing is removed; the common cancellation-policy mutation and Grok deferred-fence mutation also fail their intended assertions. These are scoped gates, not live service qualification.

Full deferred asynchronous history conversion, arbitrary live replacement/transforms, signed or speculative context conversion, and unsupported multimodal/manual-VAD controls remain outside this pairing contract. The false-policy terminal-first regression only qualifies ordinary final-history settlement before an asynchronous envelope is imported, without an intervening user turn. Native false/zero/empty JSON remains literal while the pinned Python uses COMPLETED for all falsy values; null alone maps to COMPLETED here. Queue/write admission is not remote acceptance, and no implicit replay or exactly-once external effects are promised.

## Native Gemini image generation

Gemini image generation now uses the source-derived nonstreaming generateContent REST contract, IMAGE-only response configuration and sequential per-prompt requests. Each response, encoded image and total decoded output is bounded; first missing/error image stops the sequence, without retrying billable calls or discarding already delivered output. Existing native image decoding, cancellation, output ordering and owned work lifetimes are reused. OpenAI's public image API is preserved; Gemini adds no dependency or lock change.

The exact reviewed image patch d8d57ef4c47d70c94289841c1e33f3efcba6726972e7c1b9385ca898a9de6c80 passed 58 tests, strict scoped Clippy, formatting and immutable source checks. Independent controls verify aggregate budget exhaustion prevents the next HTTP request, and a fused failure releases admission even while the failed handle remains alive. Corresponding exact-source mutations compile and fail the intended controls.

A recognized safety/recitation reason in any candidate conservatively rejects the entire response, including when another candidate has valid image bytes; both orderings are tested. Deprecated negative prompts are bounded but not forwarded. The pinned default model is a compatibility string, not a model-availability claim. Actual hosted image quality, credentials/model access and physical display output remain unqualified.

## Combined paired-provider/Gemini integration gate

Frozen public candidate fc1119053161a55a5a90c50c89e9954081eac0f7, tree 0a82e3a369c9cf004ddebcb1a752eb1f8d38b614, passed the combined fourteen-package all-target/all-feature gate: 1,788 tests passed, zero failed, and one non-loopback Redis-auth route test remained explicitly ignored. Six selected doctests, fourteen five-feature facade tests, and two standalone consumer assertions passed. MCP native-stdio fixtures, media turn recording and PGMQ were enabled and exercised. Strict all-target Clippy passed for the selected packages and facade; native formatting and the unchanged source/asset checks passed. All 1,292 projected Rust file paths, modes and blobs match canonical dc7da7c59e13728f3673d7fb19c80ad2392ceea5, apart from three previously excluded vendor administrative files.

The default facade build passed. The same external consumer intentionally fails compilation when optional APIs are absent. The measured default normal-package closure increased from 192 to 193 packages solely through sha2 0.10.9 introduced by common7; no packages were removed. Image, MoQ, MCP and both realtime provider adapters remain explicit opt-ins. Gemini itself adds no dependency or lock changes. The default closure is therefore not described as byte-identical to the earlier revision.

This publication changes only the qualification prose from that tested candidate; Rust source and dependency blobs are unchanged. Canonical source: dc7da7c59e13728f3673d7fb19c80ad2392ceea5. The combined selected gate is not a fresh complete-workspace or live-provider certificate, and its counts overlap the preceding full native checkpoint. Earlier dated/incremental limitations above describe their corresponding historical slices; the newer reviewed scope and limits in these sections supersede them where explicitly stated. The root README remains empty.
