# Native Rust rewrite publication

The homepage README is intentionally empty. Native implementation and technical documentation are in `rust/`. Run Cargo from that directory. This is an in-progress rewrite, not a full parity or production-readiness certificate.

Source: local rewrite commit `8682f6e154a3f5241b0e20df83660fe000571faa`, based on public ProfHepta/pipecat baseline `af9f9da9d7afddaa8811c0e0251b32566c980bdf`. Native Rust implementation paths and file bytes are preserved. The original complete Python project remains available in its upstream repository; this publication does not import its workflows, agent configuration, symlinks, ordinary Python examples or development environment.

Three original Python files are retained solely because Rust source-parity tests use `include_str!` to read them as text (Azure and Grok contracts). Two existing licensed ONNX model fixtures are retained at the paths used by native VAD/Smart Turn tests; their provenance/license records are in `rust/crates/pipecat-vad/`. Rust runtime and Cargo tests do not invoke Python. Python fixture-generation utilities inside `rust/` are offline developer reference tools, not runtime dependencies.

Root and vendored licenses/notices are preserved. No personal files, login material, downloaded native SDK binaries, private model downloads, local build caches or agent work directories are included. The vendored whisper-rs upstream pull-request template is omitted from this publication; implementation and license bytes are unchanged.

## Validation boundaries

The earlier fixed source `ccf0dacbb8a1de97be0140c9c5b45f4a8315e801` passed 2,268 workspace tests with46 conditional tests ignored, strict native Clippy and formatting;43 native conditional tests also passed separately. These are historical source-bound results. A fresh historical combined gate for `2fdf27e440b4bdbfb50ba63a0e6f4d09457f7130` had2,484 passes,1 failure and46 ignored tests. It exposed a global serde raw-value feature interaction that reinterpreted literal user objects. Commit `ba4f93207` removes that feature; unchanged regression and independent feature-mutation controls now pass. The current source additionally includes native Moonshine (13 deterministic and8 actual engine tests), reviewed eager speculation (944-test slice), and Eager-integrated Bedrock (988-test slice,1 pre-existing network fixture ignored). Strict native lint/format and source hashes passed for these slices. These counts overlap; the current complete publication needs its fresh combined gate. Counts overlap and cannot be summed. The publication projection itself still requires a fresh-source gate.

Live provider accounts, actual browser/device audio, hosted SFU operation and non-loopback Redis routing are not established by local fixtures. Migration inventory tracks original scope, not percentage completion.
