# Native Rust rewrite publication

The homepage README is intentionally empty. Native implementation and technical documentation are in `rust/`. Run Cargo from that directory. This is an in-progress rewrite, not a full parity or production-readiness certificate.

Source: local rewrite commit `f474505a20234de123a88ae7dfb4145e4873cd7a`, based on public ProfHepta/pipecat baseline `af9f9da9d7afddaa8811c0e0251b32566c980bdf`. Native Rust implementation paths and file bytes are preserved. The original complete Python project remains available in its upstream repository; this publication does not import its workflows, agent configuration, symlinks, ordinary Python examples or development environment.

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

The earlier frozen public projection `51e79e0e90aa4dd08dab2c0470c0e6c0cd9ea8a5` finished with 2,602 passing workspace tests, the single subsequently fixed eval-fixture failure, and 54 ignored. Its strict Clippy, native formatting and all 51 explicitly selected native-engine cases passed. The next full projection checkpoint `0d8b9d227730d5e82e5cd3e6f8c9815711f24312` is running separately. These historical results do not certify this newly extended source as a whole.
