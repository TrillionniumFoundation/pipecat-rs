# Pipecat Rust

Native Rust migration of ProfHepta/pipecat, preserving the BSD-2-Clause license.

The Python implementation remains available as a behavioral reference while native
modules are implemented and tested. Rust runtime components must not invoke Python
or silently substitute mock implementations for unavailable integrations.

Source baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

See [source recovery and current qualification](docs/RECOVERY.md) for the latest
recovered increments and the mandatory per-checkout Cargo target isolation rule.

Build and validate from this directory with `cargo test --workspace --locked`.
Check formatting from this virtual-workspace directory with
`cargo fmt -- --check`. Do not use `--all` for this check: Cargo also traverses
local vendored dependencies with that flag, whose upstream formatting is
deliberately preserved to keep their narrow patches reviewable. Workspace-native
packages remain covered by the ordinary command.
Model-backed tests require a compatible native ONNX Runtime library; set both
`PIPECAT_ONNX_RUNTIME` and `ORT_DYLIB_PATH` to its absolute path. The tracked
Silero and Smart Turn model assets are used directly. These tests fail rather
than silently skip inference when required assets are unavailable. WebRTC's
default Opus feature requires a C/C++ build toolchain and CMake when no system
Opus development package is present.
The optional `pipecat-whisper` local recognizer also requires native C/C++ build
tools, CMake and libclang. It uses whisper.cpp directly; model files are explicit
local inputs and are never downloaded by the runtime. Its model-backed tests are
explicitly ignored unless selected with the documented model/fixture variables;
see `crates/pipecat-whisper/README.md` for exact commands and provenance.
The LiveKit crate uses the actual official native SDK and requires Clang 21+
and GLib development headers on Linux, plus its pinned native WebRTC artifact.
See `crates/pipecat-livekit/VALIDATION.md` for the workspace-local toolchain,
archive provenance and the distinction between native media tests, injected
signaling ownership tests and still-unverified successful SFU sessions. Building
the entire workspace includes these native requirements even if the facade's
optional LiveKit feature is not selected.
The native `pipecat-local-audio` CPAL crate requires Linux ALSA development
headers/pkg-config metadata; other platforms require their corresponding native
audio SDK. Its validation records workspace-local official-package extraction.
Callback submission is deliberately not an audible Played receipt.
See `docs/MIGRATION.md` for source coverage and outstanding acceptance criteria.
Passing isolated tests does not establish compatibility with live providers,
WebRTC transports, audio devices, or the complete Python API.

## Run a native voice agent

```sh
cargo run -p pipecat-cli --locked -- --help
cargo run -p pipecat-cli --locked -- --smoke
```

Smoke starts synthetic Deepgram/OpenAI/Cartesia services on real loopback sockets,
then runs the complete native pipeline and a protobuf WebSocket client. Playback
acknowledgements are simulated; this is not a microphone, speaker or live-provider
acceptance test. `crates/pipecat-cli/README.md` documents configured-provider use,
client messages and the local-only security boundary.

The crate readmes distinguish source-compatible behavior, bounded-runtime changes,
and remaining gaps. Current components include the core frame/task runtime, PCM /
G.711 audio, telephony/RTVI/protobuf serializers, HTTP LLM and speech clients,
streaming speech, conversation aggregation, reusable processors/tools, local worker
RPC, WebSocket transport and the voice-agent executable. Network bus, text and
model-backed VAD work has its own readmes and validation requirements. Native
SmallWebRTC additionally exercises real loopback UDP, ICE/DTLS/SRTP, SCTP data
channels and Opus/G.711 media. OpenAI Realtime is a separate WebSocket service
crate; real-provider qualification remains outstanding.
OpenAI Responses has a separate optional `responses` facade feature exposing
native HTTP/SSE, serialized WebSocket continuation and the `LlmInference`
processor. Its refusal, cancellation and retry tests use loopback peers; see
`crates/pipecat-responses/README.md` for unsupported modes and live-qualification
limits. It is not a Chat Completions or Realtime substitute.

The `pipecat` facade reexports the common components. Its optional `webrtc`,
`realtime`, `responses`, `assemblyai`, `pgmq`, `worker-proxy`, `agent-workers`, `whisper`, `livekit`, `evals`, `elevenlabs`, `gemini-live`, `mcp`, `local-audio`,
and `client-server` features expose optional transport/service and native
browser-hosting paths. Flow execution, global observers, and shared
media transport are also reexported.
Optional `aws-bedrock`, `aws-speech`, `classifiers`, `deepgram-flux`, `moq`, `piper`,
`rnnoise`, `images`, `soniox`, and `grok-realtime` features expose
the corresponding native crates. See the [facade feature guide](crates/pipecat/README.md)
for public module paths and explicit native-backend opt-ins. The image feature
retains the unpublished source-tree package and patched JPEG path dependency.
Soniox provides streaming STT and multiplexed TTS. Grok Realtime provides the
provider adapter and tool-result relay. Its documented
[paired topology](crates/pipecat-grok-realtime/README.md#shared-realtime-conversation)
includes bounded shared realtime aggregation and native executor terminal
receipts. Arbitrary context replacement/transforms, unsupported rich history and
live-provider qualification remain open. These facade exports do not establish
physical-output acceptance or full workspace parity.
The turns aggregator also admits bounded packed RGB24 `UserImage` content,
encoding it to JPEG before the existing context path. This requires no optional
`images` facade feature. See [the precise RGB contract and synchronous-latency
limitations](crates/pipecat-turns/README.md#bounded-rgb-user-image-conversion) and
[the encoder/IJG notices](third-party/README.md). Encoded PNG/JPEG and
`AssistantImage` behavior remain unchanged; this is not camera or live-provider
qualification.
The optional `classifiers` feature exposes standalone typed batches through native
LLM inference or Jev HTTP. `pipecat-evals` integrates them for
[bounded scripted text/function judging](crates/pipecat-evals/README.md#native-scripted-classifier-judging);
pipeline/UI-worker and voicemail integrations remain separate. The optional
`deepgram-flux` feature exposes Listen v2 WebSocket STT
and its native pipeline processor, separately from Listen v1. User-aggregator
configuration remains explicit and eager turn prediction is opt-in. See
[Classifiers](crates/pipecat-classifiers/README.md) and
[Deepgram Flux](crates/pipecat-deepgram-flux/README.md) for bounds, deliberate
source differences, and live-provider qualification gaps.
The optional `assemblyai` feature exposes native streaming WebSocket STT and
segmented Sync HTTP recognition, with bounded queues and owned cancellation.
See [AssemblyAI](crates/pipecat-assemblyai/README.md) for source mappings,
independent protocol tests and explicit live-provider/base-service gaps.
The optional `moonshine` facade exposes local native Moonshine batch/segmented STT.
Audited Linux x86_64 libraries and model bundles are explicit, hash-verified inputs;
no Python runtime or automatic download is used. In-flight native inference cannot
be aborted, so shutdown waits for it. See [Moonshine](crates/pipecat-moonshine/README.md)
for exact scope, licensing and real-engine qualification.
The optional ElevenLabs crate implements native realtime STT and single-context
WebSocket TTS, alongside the separate HTTP implementations in `pipecat-speech`.
Its README distinguishes actual protocol/pipeline coverage from unsupported
dialogue, shared-socket multiplexing and live-provider qualification.
Gemini Live has its own native WebSocket client, transcription mode, frame
adapter, and bounded tool-call identity lifecycle. Its protocol and cleanup tests
use real loopback sockets; advanced resumption, video, Vertex authentication and
live-provider qualification remain explicit gaps in its crate README.
MCP integrates bounded native stdio and Streamable HTTP tools with owned sessions,
explicit teardown and no automatic replay after ambiguous failures. Its test-only
server requires the `native-fixtures` feature. Applications must follow its
documented upstream SDK logging/privacy requirements. Local device audio exposes
native capture and submission-only output; real hardware remains separately
unverified.
Use `cargo test --workspace --all-features --locked` to include PGMQ protocol
tests; these are not an acceptance test against a deployed PostgreSQL/PGMQ
instance. Per-crate READMEs identify ignored environment-dependent checks.

The inventory is a fixed source-baseline index, not a percentage-complete claim.
Preserved Python sources supply independent reference fixtures and are not invoked
by Rust runtime components. Original Python examples and tests are not automatically
ported merely because their supporting Rust library now compiles.

## Rust browser client and native hosting

`client-templates/` is a separate Rust/WASM workspace, so browser dependencies do
not pull Tokio/native audio backends into WASM. Its Rust DOM, SmallWebRTC, and
protobuf WebSocket code build with `wasm32-unknown-unknown` and wasm-bindgen
0.2.129. Generated interop JavaScript is compiler output; application behavior is
Rust. Build with `cd client-templates && ./build.sh`.

`pipecat-client-server` supplies a bounded loopback HTTP host and the original
start/session proxy contracts. It can serve the five generated public assets and
supports an owned native BotApi handler instead of an upstream proxy. See its
README for limits and the runnable `serve` example. The existing voice-agent
example uses a stricter legacy playback-ack protocol; the standard RTVI browser
client is not automatically interchangeable with that example. The separate
`pipecat-runner` executable implements the browser-compatible WebSocket path:
bounded session creation, one-shot connection capabilities, native RTVI and
Deepgram/OpenAI/Cartesia orchestration, with stateful capture resampling. Its
real loopback HTTP/WebSocket integration tests use synthetic provider endpoints;
they do not replace browser or live-provider acceptance. Run
`cargo run -p pipecat-runner --locked -- --help` for configuration. Its optional
`webrtc` feature adds capability-scoped signaling, native Opus audio/data channels
and the same voice pipeline; see runner validation for tested paths and remaining
browser/device acceptance.

The WASM build and pure lifecycle/wire tests pass. Browser execution is currently
blocked before app launch by the host's Chromium socket restriction; the separate
cloud browser cannot reach this executor's loopback. No visual, microphone,
WebRTC-browser, or physical playback acceptance is claimed. Client
`VALIDATION.md` records the exact checks and limits. Browser Daily/LiveKit and
advanced React-template surfaces remain open migration work.

## Native scripted evaluation

`pipecat-evals` provides the `pipecat-eval` executable, a runnable native echo
target, bounded JSON/YAML scripted scenarios, RTVI event assertions and concurrent
endpoint suites, plus direct compiled-bot process hosting and native release
manifest execution. Source includes and alias-expanded values are budgeted during
construction. Result limits include every serialized field and pending failure
summary. [Native scripted classifier judging](crates/pipecat-evals/README.md#native-scripted-classifier-judging)
supports bounded text/function expectations. Unsupported judge configurations,
simulations, audio evaluation and Python process orchestration fail explicitly.
Child environment, output and result retention are
bounded; normal shutdown cancels and reaps the direct child. Process descendants,
OS-stuck children and Windows descendant-held pipe limitations are explicit. See
its README for commands and exact scope.


## Optional local Piper synthesis

`pipecat-piper` implements a bounded, owned native CPU synthesis service using
caller-supplied hashed ONNX artifacts. The optional `espeak-gpl` feature explicitly
loads a caller-supplied patched GPL eSpeak phonemizer; it is not enabled by
its default feature set. No model download, Python runtime or server fallback is
performed. Actual text-to-PCM execution and independent native Whisper recognition
are recorded in its `PROVENANCE.md`.

`PiperTtsProcessor` integrates bounded synthesis with native pipeline lifecycle,
alignment, metrics and owned downstream capacity admission. Early health changes
cancel active and queued work even while a prior media fence is full. Native
count-pressure tests use discarded audio, not speaker playback or heard-history
proof; see its README and the exact-source checkpoint evidence.
Native loading cannot be interrupted, and output checks do not cap internal ONNX
allocations. Model-card dataset licensing does not establish weight redistribution
rights; neither model weights nor native library binaries are bundled.
