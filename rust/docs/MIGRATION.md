# Native Rust migration inventory and acceptance plan

## Baseline and status

This document and [`inventory.json`](inventory.json) inventory `ProfHepta/pipecat` at
commit `af9f9da9d7afddaa8811c0e0251b32566c980bdf`, on the audit branch
`rewrite/rust-native-2026-10-09`. The audit date is 2026-10-09 UTC.

**This is the immutable initial inventory, not the current implementation
status.** Every baseline module started `not_migrated`; the inventory operation
itself ran no original tests or integrations. Subsequent native implementation
and verification are recorded in [COVERAGE.md](COVERAGE.md), the crate readmes,
and fixed-source [CHECKPOINT.md](CHECKPOINT.md). The original JSON statuses stay
unchanged so the initial audit is reproducible. They must not be interpreted as
asserting that current Rust implementations do not exist, or as certification
that an entire source module has achieved parity.

The scope is larger than the runtime package:

| Baseline area | Python files | Python lines |
| --- | ---: | ---: |
| Runtime/package source, including bundled CLI and eval harness | 645 | 199,570 |
| Original tests and their Python helpers | 338 | 134,009 |
| Examples | 426 | Included in the full total below |
| Repository scripts | 22 | Included in the full total below |
| Root eval factories/entry points | 2 | Included in the full total below |
| Documentation configuration | 1 | Included in the full total below |
| **All tracked Python** | **1,434** | **412,509** |

The scanner found 6,319 `test_` function definitions in the 338 test files. This
is neither pytest's collected/parameterized case count nor a pass count. All
1,434 files parsed with the standard-library AST parser, and every analyzed
file's Git blob hash matched the pinned baseline. The audit executed only its
own static-analysis code, never repository modules, imports, scripts, tests,
examples, or build hooks. It made no external API or registry requests.

There are also 219 selected non-Python contract assets in the JSON inventory:
198 template/eval-spec files, 18 audio assets, two ONNX models, and one protobuf
schema. This is a targeted contract-asset list, not a complete asset/license
inventory. In particular, `.py.jinja2` templates are not counted as Python files.

## What counts as a native rewrite

A module is complete only when its observable behavior is implemented in Rust,
its required dependencies are usable without Python, and its acceptance
evidence has been reviewed. Native C/C++/platform FFI may be appropriate for a
codec, inference engine, audio device, or proprietary SDK; it must have an
explicit ABI/lifetime/packaging contract and working native integration tests.

The following do **not** count as completion:

- Calling the original Python module through an embedded interpreter, PyO3,
  subprocess, HTTP sidecar, or a renamed Python package
- A trait or struct declaration without the baseline behavior behind it
- A placeholder, unconditional success, no-op processor, fake transcript,
  canned audio, or silently dropped unsupported frame
- Passing mocked protocol tests while claiming a live provider/device works
- Replacing a provider-specific feature with a generic OpenAI-compatible call
  when the original module exposes additional behavior
- Compiling a sample while leaving other baseline examples, CLI-generated
  projects, or eval execution dependent on Python

Python names, decorators, inheritance, and import paths need not dictate the
Rust API shape. Their behavior must be represented explicitly: typed events,
traits, registries, builders, typed configuration, and async state machines.
Removed/deprecated behavior remains in the compatibility inventory until a
separate, documented scope decision removes it. An intentionally unsupported
feature stays open and must fail explicitly rather than appear to work.

## Reading the per-module inventory

`inventory.json` contains one `modules` record for every tracked `.py` file,
including each `__init__.py`, generated protobuf module, test helper, script,
and example. The compact schema uses shared indexes rather than copying source:

- `path`, baseline `blob`, line count, group, proposed route, and explicit
  `not_migrated` status for every file
- Short source module behavior summary and 3,221 public top-level class/function
  source locators across non-test modules; class bodies, signatures, field
  defaults, events, and private implementation remain in the pinned source
- 11,350 direct internal-dependency references; shared indexes for 222 exact
  external module imports and standard-library roots; dynamic-import locations
- 3,573 original-test file references, with direct, reexport, and weaker textual
  relationships distinguished; test-function counts and mock/skip markers
- Shared requirement and route indexes; selected assets, console entry points,
  and the baseline's declared base/optional Python distributions

Module IDs are zero-based positions in `modules`, stable for this immutable
baseline. Paths remain the durable identity. `public_api` pairs name with source
line; `tests` pairs test-file ID with a relation index. The JSON's `schema` section
explains every index. No source bodies or long signatures are duplicated.

This is a complete **file inventory**, not a proof that AST extraction recovers
every Python behavior. Inherited/dynamically generated methods, monkeypatches,
private implementation side effects, and runtime reflection need source review
when a module is ported. Module docstrings and test references are evidence
locators, not substitutes for reading the implementation and assertions.

517 of 645 source modules have an identified test reference; 128 do not. These
numbers include package/reexport and textual references and do not establish
coverage percentages. An empty package module may need only layout/reexports;
a concrete unreferenced service needs new contract tests. Indirect coverage is
not inferred, and many existing tests use mocks or skip missing optional extras.

Each module's `dependencies` list records static internal module references.
It includes optional/lazy/type-only imports and may overstate runtime needs.
It is not a call graph or build DAG; traverse the graph conservatively when
checking transitive native/SDK/model requirements. No Rust SDK's existence,
suitability, license, or version was checked against an external registry.

## Dependency-first implementation order

The following is a proposed architectural order grounded in the baseline
imports. Shared schemas must be separated from active services so Python's
import cycles do not become unnecessary Rust crate cycles.

```text
P0  schema/value types + errors + clock/events/task ownership
    |
P1  full frame catalog + priority queues + processors + sequential/parallel pipelines
    | \
    |  P2a bus messages/serialization + local bus + registry + base workers/jobs
    |   |
    +-- P2b PipelineWorker + WorkerRunner + bridge + LLM/UI/proxy workers
    |
P3  PCM/resampling + VAD/turn contracts + aggregators/context/tools
    |                     |
    +-- service bases ----+-- serializers/RTVI -- base transport I/O
    |                     |                       |
P4  provider protocols + local inference      network/media/device transports
    |                     |                       |
P5  CLI/scaffolding + Rust eval harness + every example + release scenarios
    |
P6  whole-distribution packaging, deployment, performance, and live parity gates
```

P2, P3, and P4 contain independent work streams after their shared interfaces
are settled. This is not a claim that the original imports are acyclic.

| Priority | Contract and source evidence | Why it gates later work |
| --- | --- | --- |
| P0 | [`frames.py` imports](../../src/pipecat/frames/frames.py#L30), [`settings.py`](../../src/pipecat/services/settings.py), schema types in [`adapters/schemas`](../../src/pipecat/adapters/schemas), [`metrics.py`](../../src/pipecat/metrics/metrics.py), [`Language`](../../src/pipecat/transcriptions/language.py), [`BaseObject`](../../src/pipecat/utils/base_object.py#L48), [`TaskManager`](../../src/pipecat/utils/asyncio/task_manager.py#L119) | Frames already refer to settings, tools, audio parameters, metrics, and language. Define shared value contracts before importing active backends; preserve events and task cleanup semantics |
| P1 | [`Frame` and the 146 public frame-module classes](../../src/pipecat/frames/frames.py#L67), [`FrameProcessorQueue`](../../src/pipecat/processors/frame_processor.py#L131), [`FrameQueue`](../../src/pipecat/utils/frame_queue.py), [`FrameProcessor`](../../src/pipecat/processors/frame_processor.py#L194), [`Pipeline`](../../src/pipecat/pipeline/pipeline.py), [`ParallelPipeline`](../../src/pipecat/pipeline/parallel_pipeline.py#L28) | Services, audio processors, transports, and observers depend on frame identity, direction, scheduling, interruption, setup, and teardown |
| P2 | [`BusMessage`](../../src/pipecat/bus/messages.py), [`WorkerBus`](../../src/pipecat/bus/bus.py#L49), [`JSONMessageSerializer`](../../src/pipecat/bus/serializers/json.py), [`WorkerRegistry`](../../src/pipecat/registry/registry.py#L19), [`BaseWorker`](../../src/pipecat/workers/base_worker.py#L119), [`JobContext`](../../src/pipecat/pipeline/job_context.py#L351), [`PipelineWorker`](../../src/pipecat/pipeline/worker.py#L201), [`WorkerRunner`](../../src/pipecat/workers/runner.py#L83) | `PipelineWorker` subclasses `BaseWorker`; a pipeline-only runner omits the baseline multi-worker, RPC, remote-registry, and UI lifecycle |
| P3 | [`LLMContext`](../../src/pipecat/processors/aggregators/llm_context.py), [`llm_response_universal.py`](../../src/pipecat/processors/aggregators/llm_response_universal.py), [`turns`](../../src/pipecat/turns), [`STTService`](../../src/pipecat/services/stt_service.py#L53), [`TTSService`](../../src/pipecat/services/tts_service.py), [`LLMService`](../../src/pipecat/services/llm_service.py) | Provider adapters must share correct context, tool results, audio/context sequencing, mutable settings, metrics, and cancellation behavior |
| P3/P4 | [`FrameSerializer`](../../src/pipecat/serializers/base_serializer.py), [`frames.proto`](../../src/pipecat/frames/frames.proto), [`RTVIProcessor`](../../src/pipecat/processors/frameworks/rtvi/processor.py#L57), [`RTVIObserver`](../../src/pipecat/processors/frameworks/rtvi/observer.py), [`BaseInputTransport`](../../src/pipecat/transports/base_input.py#L43), [`BaseOutputTransport`](../../src/pipecat/transports/base_output.py) | Wire-level, audio, and readiness behavior must work before real provider/transport sessions can establish end-to-end parity |
| P5 | [`ProjectGenerator`](../../src/pipecat/cli/generators/project.py#L25), [`EvalSuite`](../../src/pipecat/evals/suite.py), [`release manifest`](../../evals/release/manifest.yaml), [`release prerequisites`](../../evals/release/README.md#L30) | Scaffolding currently generates `bot.py`/`server.py`, and evals spawn Python bots. A native library alone does not complete either user workflow |

### Contracts that are easy to miss

1. **Scheduling is three-tier and stable.** `StartFrame` precedes `SystemFrame`,
   which precedes other queued frames; equal-priority arrival order is stable.
   A system frame does not preempt a `process_frame` call already in progress.
   See [`FrameProcessorQueue`](../../src/pipecat/processors/frame_processor.py#L131).
2. **Interruptibility is per instance.** The current `frame.interruptible` flag,
   not just the frame class, determines survival. Queue reset preserves flagged
   items in order; terminal/control frames have differing defaults. An interrupt
   must preserve required in-flight/uninterruptible processing and still allow
   subsequent frames. See [`FrameQueue`](../../src/pipecat/utils/frame_queue.py)
   and [`test_frame_processor.py`](../../tests/test_frame_processor.py#L125).
3. **Frames carry more than payload.** IDs, names, optional nanosecond PTS,
   sibling broadcast IDs, metadata, transport source/destination, and flags are
   observable. Optional timestamps must not collapse `None` and zero without
   checking the baseline serializer behavior.
4. **Bus JSON has Python-qualified type tags.** `__type__` contains names such as
   `pipecat.frames.frames.TextFrame`; `__data__` recursively carries enums,
   base64 bytes, models, and dataclasses. Null/callable/unsupported-field rules,
   post-init fields, adapter lookup through base classes, and default adapters
   are part of the contract. Rust needs an explicit compatible type registry;
   arbitrary Python import resolution is not an acceptable native implementation.
   See [`json.py`](../../src/pipecat/bus/serializers/json.py#L83).
5. **Local and distributed bus behavior differ.** `BusLocalMessage` stays local,
   data/system queues have different priority, and network delivery has its own
   serialization/retry/lifecycle semantics. The PGMQ tests use an in-memory
   `FakePgmq`; they cannot establish real PostgreSQL/PGMQ delivery guarantees.
   See [`test_pgmq_bus.py`](../../tests/test_pgmq_bus.py#L53).
6. **Audio quality tests can be only forwarding tests.** Several SOXR tests
   replace the resampler with a fake and assert quality/parameter forwarding.
   Native parity additionally needs numerical signal, chunk-boundary, state
   reset, sample-count, clipping, channel, and rate-change fixtures. See
   [`test_soxr_resamplers.py`](../../tests/test_soxr_resamplers.py#L18).
7. **Provider names are not an implementation count.** Some directories are
   aliases or thin shared-client subclasses; others contain streaming,
   realtime, STT, TTS, LLM, image, or avatar submodules. Every module and exposed
   setting remains in scope, including explicit deprecated compatibility paths.
8. **Generated output and eval subprocesses matter.** A Rust CLI that keeps
   emitting Python-only projects, or a Rust eval wrapper that launches the
   Python harness, fails the native-rewrite criterion. Preserve the user-facing
   workflow through Rust project templates and Rust-native process launch.

## Implementation routes and external requirements

The route on a record is a starting point based on direct source evidence,
not a declaration of feasibility or completed integration. Transitive
requirements are listed separately because a pure schema/adapter can inherit
an optional provider dependency without itself making a network request.

| Route | What can be implemented directly | What must be replaced or validated |
| --- | --- | --- |
| Direct Rust behavior port | Frames, settings, enums, event/task ownership, queues, pipelines, observers, tools/context conversion, text/state-machine logic, JSON protocols | Exact defaults, ordering, errors, deprecated paths, cancellation, metrics and concurrency; Python reflection becomes an explicit Rust registry |
| Rust module layout | Empty package modules, reexport surfaces | Rust module/reexport organization and any nonempty initializer behavior; no coverage credit for empty declarations replacing real modules |
| Protocol client or supported Rust SDK | HTTP, WebSocket, SSE, gRPC/request/response adapters | Endpoint-specific streaming, authentication, reconnects, limits, tool/audio schemas, errors, usage and cancellation; verify a suitable Rust SDK or implement the protocol; live credentials required later |
| Database/broker client | Local bus contract and typed codecs | Redis/PGMQ clients, channel/routing semantics, connection loss, backpressure, delivery behavior, cleanup; tests against real local broker/database |
| Native backend/FFI | Rust orchestration and buffer ownership | SDK/codec/device ABI, threads/callbacks, platform support, licenses, native builds, error/lifetime handling; no calls through the Python binding |
| Model runtime/FFI | Pre/postprocessing and model scheduling | Model files, inference engine, tokenizer/feature parity, supported devices, dtype/shape/state, licensing and numerical fixtures |
| Transport protocol/backend | Base I/O sequencing and plain WebSocket protocol | Real peer sessions for WebRTC/MoQ, device tests for local audio, and account credentials for hosted room/avatar/telephony services |
| Application/test harness | CLI parsing, generators, scenario/results models, eval/session lifecycle, examples | Native templates/executables, process control, YAML/JSON compatibility, live bot scenarios, opt-in costly/provider tests |
| Protobuf generation | Rust bindings from the checked-in `frames.proto` | Exact field numbers, oneof handling, optional PTS, binary/JSON payload mapping, unsupported frame behavior |

### Native and model dependency decisions

- **PCM, μ-law/A-law, DTMF, volume, mixing, resampling:** port exact arithmetic
  where feasible; select and verify a native implementation for SOXR/libsndfile
  or prove an equivalent Rust algorithm against fixtures. NumPy by itself is
  not a reason to embed Python. `resampy` parity requires a real filter/kernel
  implementation, not nearest-neighbor placeholder resampling.
- **ONNX/CoreML/local model services:** Silero and Smart Turn use model assets;
  local Whisper, MLX, FunASR, Kokoro, Piper, Pocket TTS, Moonshine, and Moondream
  have distinct runtime and preprocessing requirements. Python model packages
  do not count as Rust backends. The two checked-in ONNX files are listed with
  baseline hashes; other downloaded model assets need separate provenance.
- **AIC/Koala/Krisp/RNNoise:** source imports `aic_sdk`, `pvkoala`,
  `krisp_audio`, or `pyrnnoise`. Obtain a supported native ABI or native
  implementation and verify model/license distribution. Krisp explicitly
  expects proprietary model files and can use API-key configuration; neither
  their availability nor license rights is established by this audit.
- **Local audio:** `PyAudio`/PortAudio callbacks require a native device backend,
  platform packaging, and real microphone/speaker tests. The baseline's 20 ms
  input callback size and thread-to-event-loop delivery are observable timing
  constraints, not permission to fake audio.
- **Media transports:** Daily, LiveKit, Vonage, GStreamer, aiortc/PyAV/OpenCV,
  and hosted avatar integrations require suitable native clients or full
  protocol implementations. A WebSocket substitute is not WebRTC parity.
- **MoQ:** the source already uses a Python binding to a Rust-based library.
  A Rust port must use an appropriate native API directly and preserve its
  audio/RTVI-track and legacy close-code behavior. The existing Python wrapper
  does not prove a compatible stable Rust API/version is available.
- **Cloud services:** replace Python SDK usage for OpenAI, Anthropic, AWS/Smithy,
  Google, Azure Speech, Deepgram, Groq, Mistral, Riva, Hume, Camb, Sarvam,
  Speechmatics, and other listed imports. HTTP-only providers still need their
  own protocol/behavior verification. No API call was attempted here.
- **Framework integrations:** MCP, LangChain, Strands, mem0, observability,
  tracing, and extension modules are also scope. Implement their exposed
  behavior through Rust-native protocol/interfaces rather than host their
  Python framework. An unsupported plugin is an explicit blocker.

## Acceptance matrix

At inventory creation, all rows below were **NOT RUN / NOT MIGRATED**.
These rows remain the baseline acceptance requirements; current partial native
evidence is indexed separately in [COVERAGE.md](COVERAGE.md). Original test links
are source references to port, not certification. Each row needs a Rust test identifier,
source contract mapping, fixture provenance, command/environment, outcome,
and the exact Rust revision tested before it can be closed.

| Gate | Baseline test/source evidence | Required native acceptance evidence |
| --- | --- | --- |
| A00 Inventory completeness | All 1,434 records and 219 selected assets in `inventory.json` | One explicit disposition for every module; no missing public contract hidden behind a migrated category; asset/license audit completed |
| A01 Frame/value schemas | [`test_frames_interruptible.py`](../../tests/test_frames_interruptible.py), [`test_settings.py`](../../tests/test_settings.py), [`frames.py`](../../src/pipecat/frames/frames.py) | Catalog/fields/defaults/aliases, identity and metadata, optional values, per-instance flags, type-safe conversions, settings updates |
| A02 Processor/event/task semantics | [`test_frame_processor.py`](../../tests/test_frame_processor.py), [`test_frame_queue.py`](../../tests/test_frame_queue.py), [`test_task_manager.py`](../../tests/test_task_manager.py) | Priority/FIFO, both directions, setup/start order, pause/resume/readiness, error paths, cancellation races, no leaked tasks, uninterruptible survival |
| A03 Pipeline lifecycle/parallelism | [`test_pipeline.py`](../../tests/test_pipeline.py), [`test_sync_parallel_pipeline.py`](../../tests/test_sync_parallel_pipeline.py), [`test_service_switcher.py`](../../tests/test_service_switcher.py) | Sequential/parallel routing, duplicate/sibling handling, branch synchronization, heartbeat/idle, flush, graceful end/stop/cancel, service switching |
| A04 Local bus and registry | [`test_bus.py`](../../tests/test_bus.py), [`test_registry.py`](../../tests/test_registry.py), [`test_bridge_processor.py`](../../tests/test_bridge_processor.py) | Independent subscribers, data/system priority, broadcast/target/local-only routing, subscribe/unsubscribe, remote/local registration and readiness watches |
| A05 Workers/jobs/tools/UI | [`test_base_worker.py`](../../tests/test_base_worker.py), [`test_job_group.py`](../../tests/test_job_group.py), [`test_llm_worker.py`](../../tests/test_llm_worker.py), [`test_ui_job_lifecycle.py`](../../tests/test_ui_job_lifecycle.py), [`test_pipeline_worker_ui_bridge.py`](../../tests/test_pipeline_worker_ui_bridge.py) | Activation, worker-tree/run termination, sequential/concurrent RPC, timeout/cancel/stream/fanout outcomes, decorator replacement, tool cancellation and UI event lifecycle |
| A06 Distributed bus and typed JSON | [`test_bus_json_serializer.py`](../../tests/test_bus_json_serializer.py), [`test_bus_network.py`](../../tests/test_bus_network.py), [`test_pgmq_bus.py`](../../tests/test_pgmq_bus.py), [`test_redis_bus.py`](../../tests/test_redis_bus.py) | Golden Python-qualified type tags and adapters; offline transport failures; real Redis/PGMQ process integration and graceful teardown; explicitly separate fake-broker tests |
| A07 Audio codecs/DSP | [`test_audio_utils.py`](../../tests/test_audio_utils.py), [`test_audio_volume.py`](../../tests/test_audio_volume.py), [`test_soxr_resamplers.py`](../../tests/test_soxr_resamplers.py), [`test_resampy_resampler.py`](../../tests/test_resampy_resampler.py), [`test_rnnoise_resampling.py`](../../tests/test_rnnoise_resampling.py) | Bit-exact codec fixtures where applicable; declared numerical tolerances for DSP; chunk invariance/state reset, rates/channels/sample count, silence/empty/extreme input, native backend lifecycle |
| A08 VAD and turn inference | [`test_vad_processor.py`](../../tests/test_vad_processor.py), [`test_base_smart_turn_buffer.py`](../../tests/test_base_smart_turn_buffer.py), [`test_local_smart_turn_v3_features.py`](../../tests/test_local_smart_turn_v3_features.py), [`test_local_smart_turn_v3_resample.py`](../../tests/test_local_smart_turn_v3_resample.py) | State transitions, feature tensors and resampling fixtures, model inference with verified artifacts, padding/context/window behavior, threshold/timing and platform tests |
| A09 Context/aggregation/turns/flows | [`test_llm_context.py`](../../tests/test_llm_context.py), [`test_context_aggregators_universal.py`](../../tests/test_context_aggregators_universal.py), [`test_user_turn_controller.py`](../../tests/test_user_turn_controller.py), [`test_flows_manager.py`](../../tests/test_flows_manager.py) | User/assistant commit points, interruption truncation, tool/result ordering, context summarization, mute/start/stop strategies, flow transitions and actions |
| A10 Service base behavior | [`test_llm_service.py`](../../tests/test_llm_service.py), [`test_stt_service.py`](../../tests/test_stt_service.py), [`test_tts_interruptible_service.py`](../../tests/test_tts_interruptible_service.py), [`test_tts_audio_context_hooks.py`](../../tests/test_tts_audio_context_hooks.py), [`test_tts_zero_audio_contexts.py`](../../tests/test_tts_zero_audio_contexts.py) | Streaming/segmented STT, background work, mutable settings, TTFB/usage, TTS text/audio-context sequencing, keepalive, error/retry/cancel and zero-audio outcomes |
| A11 Each provider module | Per-module `tests`; e.g. [`test_realtime_tool_sync.py`](../../tests/test_realtime_tool_sync.py), [`test_openai_realtime_audio_frames.py`](../../tests/test_openai_realtime_audio_frames.py), [`test_xai_realtime_user_audio.py`](../../tests/test_xai_realtime_user_audio.py) | Provider-specific recorded/mock protocol fixtures plus authorized live STT/TTS/LLM/realtime/tool/error/cancel tests; all exposed voice/model/language/settings options accounted for |
| A12 Protobuf and telephony serializers | [`test_protobuf_serializer.py`](../../tests/test_protobuf_serializer.py), [`test_serializers.py`](../../tests/test_serializers.py), [`test_twilio_serializer.py`](../../tests/test_twilio_serializer.py), [`test_telnyx_serializer.py`](../../tests/test_telnyx_serializer.py), [`test_exotel_serializer.py`](../../tests/test_exotel_serializer.py), [`frames.proto`](../../src/pipecat/frames/frames.proto) | Binary golden vectors; start/media/mark/clear/stop/DTMF envelopes; sample encoding/rates; malformed input; provider-specific metadata and handshake behavior |
| A13 RTVI client/server protocol | [`test_rtvi_processor.py`](../../tests/test_rtvi_processor.py), [`test_rtvi_observer_function_calls.py`](../../tests/test_rtvi_observer_function_calls.py), [`test_rtvi_observer_metrics.py`](../../tests/test_rtvi_observer_metrics.py), [`test_rtvi_ui.py`](../../tests/test_rtvi_ui.py) | Ready/version handshake, input text/audio/image/file/DTMF, function results, event filtering/order, levels/metrics, UI snapshots/jobs, actual compatible client session |
| A14 Transport/device/media | Per-module `tests`; [`transports`](../../src/pipecat/transports) | WebSocket loopback and malformed/disconnected peers; WebRTC ICE/DTLS/SRTP with real peer; hosted provider room lifecycle; MoQ relay; real OS microphone/speaker/video paths; no fake success |
| A15 CLI and generated projects | [`tests/cli`](../../tests/cli), [`ProjectGenerator`](../../src/pipecat/cli/generators/project.py), [`templates`](../../src/pipecat/cli/templates) | Arguments/help/errors and config parity, safe file generation/overwrite behavior, native project templates that build and run, all original CLI fixture cases ported |
| A16 Eval harness/scenarios | [`evals`](../../src/pipecat/evals), all mapped `test_evals_*` and `tests/cli` cases, [`release manifest`](../../evals/release/manifest.yaml) | Scripted/simulated scenario parsing, match/judge/results schema, subprocess/timeouts/concurrency/repeat/caching/recording, Rust bot process launch; real release scenarios with prerequisites |
| A17 Examples and auxiliary tools | 426 example records, 22 scripts, two eval files, one docs config | Every example has a native executable/use-case equivalent and build check; required model/network/device scenarios run explicitly; repository tooling replaced or individually justified |
| A18 Distribution and performance | [`pyproject.toml`](../../pyproject.toml), baseline runtime/data assets | No Python interpreter/site-packages needed at runtime; feature-specific builds; ABI/platform/asset/license packaging; leak/cancel stress; defined latency/memory/audio quality budgets measured against approved baseline traces |

### Evidence and completion rules

1. First port deterministic assertions and build reviewed offline fixtures.
   This audit did not generate fixtures by running Python. Any future baseline
   execution must be isolated and authorized; unknown repo scripts must not be
   run as a side effect of reading this plan.
2. Keep offline parity, native-backend integration, live-provider tests, real
   device tests, and release evals as separate reported outcomes. Skipped and
   unavailable are not passed.
3. Test cancellation/reconnect/interruption and repeated start/stop/end flows,
   not only successful one-turn audio. Use deterministic clocks for scheduling
   tests and stress tests for concurrency/lifetime behavior.
4. Verify every provider/transport feature behind its own build feature and
   failure reporting. Merely compiling a feature-disabled build does not close
   the corresponding inventory entries.
5. Update a module from `not_migrated` only with concrete Rust implementation
   paths and parity evidence. A category cannot be marked complete while one
   of its runtime or public behavior contracts remains open.
6. A baseline behavior change, dropped API, replaced model, or changed protocol
   needs an explicit compatibility decision and documented consequences.
   Security-sensitive dynamic Python imports should become safe typed lookup,
   with any intentional compatibility restriction recorded.
7. The Rust completion claim must include all 645 package files' dispositions,
   all 338 original test files' dispositions, and every example/tooling file.
   File counts alone do not measure functional parity.

## Original first runnable voice-agent milestone

Use [`06-voice-agent.py`](../../examples/getting-started/06-voice-agent.py) and
[`06a-voice-agent-local.py`](../../examples/getting-started/06a-voice-agent-local.py)
as the first end-to-end targets. Their actual chain is input → Deepgram STT →
user aggregator/Silero VAD → OpenAI LLM → Cartesia TTS → output → assistant
aggregator. This was the initial vertical-slice plan. Native speech, turn,
runner and protocol implementations now exist; deterministic loopback tests and
actual local model execution are recorded separately from live-provider/device
acceptance in [COVERAGE.md](COVERAGE.md).

The table retains the original exit criteria. Its presence does not mean each
listed implementation is still absent, nor that a passing native subset closes
its full source-module status.

| Order | Deliverable and gap to close | Concrete source and tests | Exit evidence |
| --- | --- | --- | --- |
| 1 | Streaming STT and TTS, including the shared service state machines | [`STTService`](../../src/pipecat/services/stt_service.py), [`DeepgramSTTService`](../../src/pipecat/services/deepgram/stt.py#L292), [`TTSService`](../../src/pipecat/services/tts_service.py), [`CartesiaTTSService`](../../src/pipecat/services/cartesia/tts.py#L243); [`test_stt_service.py`](../../tests/test_stt_service.py), [`test_deepgram_stt.py`](../../tests/test_deepgram_stt.py), [`test_cartesia_tts.py`](../../tests/test_cartesia_tts.py), [`test_websocket_stt_reconnect.py`](../../tests/test_websocket_stt_reconnect.py), [`test_tts_audio_context_hooks.py`](../../tests/test_tts_audio_context_hooks.py) | Stream PCM to final/interim transcripts; stream text to real PCM; reconnect/4xx/keepalive/cancel; preserve context IDs, word timestamps and interruptibility. Offline protocol tests first, then authorized live keys/endpoints |
| 2 | User/assistant context and turn management | [`LLMContext`](../../src/pipecat/processors/aggregators/llm_context.py), [`LLMUserAggregator`](../../src/pipecat/processors/aggregators/llm_response_universal.py#L574), [`LLMAssistantAggregator`](../../src/pipecat/processors/aggregators/llm_response_universal.py#L1561), [`UserTurnController`](../../src/pipecat/turns/user_turn_controller.py#L36), [`VADUserTurnStartStrategy`](../../src/pipecat/turns/user_start/vad_user_turn_start_strategy.py), [`SpeechTimeoutUserTurnStopStrategy`](../../src/pipecat/turns/user_stop/speech_timeout_user_turn_stop_strategy.py); [`test_context_aggregators_universal.py`](../../tests/test_context_aggregators_universal.py), [`test_user_turn_controller.py`](../../tests/test_user_turn_controller.py), [`test_empty_user_turn.py`](../../tests/test_empty_user_turn.py) | Multi-turn context commits at the correct times; final transcripts do not double-trigger LLM calls; barge-in truncates assistant spoken context correctly; mute/empty turns/tool results are ordered |
| 3 | Real VAD/model inference and audio I/O, not just sample utilities | [`SileroOnnxModel`](../../src/pipecat/audio/vad/silero.py#L34), [`silero_vad.onnx`](../../src/pipecat/audio/vad/data/silero_vad.onnx), [`LocalAudioTransport`](../../src/pipecat/transports/local/audio.py), [`BaseInputTransport`](../../src/pipecat/transports/base_input.py), [`BaseOutputTransport`](../../src/pipecat/transports/base_output.py); [`test_silero_vad.py`](../../tests/test_silero_vad.py), [`test_vad_processor.py`](../../tests/test_vad_processor.py), [`test_base_input_transport.py`](../../tests/test_base_input_transport.py), [`test_base_output_transport.py`](../../tests/test_base_output_transport.py) | Native ONNX execution with 8/16 kHz state/window parity; paced output and speaking events; real microphone/speaker loop on supported OS. A WAV/WebSocket replay is useful earlier but does not close device coverage |
| 4 | A native voice-agent executable using the completed chain | The two getting-started examples above, [`runner/utils.py`](../../src/pipecat/runner/utils.py), [`runner/run.py`](../../src/pipecat/runner/run.py) | One native command runs the full chain without Python; multiple user turns and interruption/cancel/disconnect work. Keep deterministic fixture run and live-provider run as different results |
| 5 | Browser voice via actual WebRTC | [`SmallWebRTCRequestHandler`](../../src/pipecat/transports/smallwebrtc/request_handler.py#L86), [`SmallWebRTCConnection`](../../src/pipecat/transports/smallwebrtc/connection.py#L256), [`RawAudioTrack`](../../src/pipecat/transports/smallwebrtc/transport.py#L99), [`SmallWebRTCTransport`](../../src/pipecat/transports/smallwebrtc/transport.py#L1027); [`test_smallwebrtc_request_handler.py`](../../tests/test_smallwebrtc_request_handler.py), [`test_smallwebrtc_transport.py`](../../tests/test_smallwebrtc_transport.py) | SDP/trickle ICE, DTLS/SRTP audio, Opus/PCM timing, RTVI data-channel messages, reconnect/renegotiation/disconnect with a real browser peer. Select and validate a Rust/native backend; WebSocket-only success is insufficient |
| 6 | Native CLI/scaffolding and executable eval loop | [`cli/main.py`](../../src/pipecat/cli/main.py), [`cli/generators/project.py`](../../src/pipecat/cli/generators/project.py), [`evals/client.py`](../../src/pipecat/evals/client.py), [`evals/session.py`](../../src/pipecat/evals/session.py), [`evals/suite.py`](../../src/pipecat/evals/suite.py); [`tests/cli`](../../tests/cli), [`test_evals_session.py`](../../tests/test_evals_session.py), [`test_evals_suite.py`](../../tests/test_evals_suite.py) | Generate/build/run native projects; launch Rust bots and judge scripted multi-turn/interrupt/tool scenarios; preserve result/timeout/repeat/recording behavior. Real release evals additionally require judge/models/bot keys |

Smart Turn, other local STT/TTS models, hosted WebRTC/avatar transports, all other
provider capabilities, every remaining example, and full release coverage stay
in scope after this vertical slice. The first speaking bot is a useful working
milestone, not evidence of a full rewrite.

## Immutable initial source category totals

The categories below partition all Python files. The zero migrated column is
the initial inventory snapshot, not a current progress measurement. Provider services include
provider context adapters and classifiers. Audio includes audio/GStreamer
processors. CLI/evals/examples includes the development runner and package test
helpers. Workers/bus includes registry and pipeline job/decorator modules;
`PipelineWorker` itself remains under core pipelines.

| Category | All files | Package files | Package lines | Package files with test references | Migrated |
| --- | ---: | ---: | ---: | ---: | ---: |
| `audio` | 44 | 44 | 6,486 | 35 | 0 |
| `cli_evals_examples` | 482 | 54 | 22,677 | 46 | 0 |
| `core_frames_pipeline` | 185 | 185 | 41,743 | 161 | 0 |
| `original_tests` | 338 | 0 | 0 | 0 | 0 |
| `provider_services` | 262 | 262 | 92,999 | 198 | 0 |
| `repository_tooling` | 23 | 0 | 0 | 0 | 0 |
| `serializers_rtvi` | 15 | 15 | 5,799 | 13 | 0 |
| `transports` | 39 | 39 | 20,183 | 28 | 0 |
| `workers_bus` | 46 | 46 | 9,683 | 36 | 0 |

## Provider directory inventory

These are the 62 directories under `src/pipecat/services`; they are not a count
of distinct vendors. SDK columns are direct Python imports, with generic HTTP,
validation, logging, and numerical packages omitted. A blank SDK column does
not mean no dependency: shared base/provider inheritance is in the JSON graph.
Every row is **not migrated**, and Rust SDK availability is unverified.

| Directory | Files / lines | Direct SDK/native imports | Source tests referenced |
| --- | ---: | --- | ---: |
| [anthropic](../../src/pipecat/services/anthropic) | 2 / 756 | `anthropic` | 11 |
| [assemblyai](../../src/pipecat/services/assemblyai) | 3 / 1,869 | Shared base / direct protocol; inspect imports | 2 |
| [asyncai](../../src/pipecat/services/asyncai) | 2 / 690 | Shared base / direct protocol; inspect imports | 1 |
| [aws](../../src/pipecat/services/aws) | 11 / 5,587 | `aiobotocore`, `aws_sdk_bedrock_runtime`, `aws_sdk_sagemaker_runtime_http2`, `botocore`, `smithy_aws_core`, `smithy_core` | 11 |
| [azure](../../src/pipecat/services/azure) | 11 / 4,683 | `azure`, `openai` | 10 |
| [baseten](../../src/pipecat/services/baseten) | 2 / 86 | Shared base / direct protocol; inspect imports | 1 |
| [bland](../../src/pipecat/services/bland) | 2 / 641 | Shared base / direct protocol; inspect imports | 1 |
| [camb](../../src/pipecat/services/camb) | 2 / 410 | `camb` | 0 |
| [cartesia](../../src/pipecat/services/cartesia) | 5 / 2,220 | Shared base / direct protocol; inspect imports | 6 |
| [cerebras](../../src/pipecat/services/cerebras) | 2 / 86 | Shared base / direct protocol; inspect imports | 1 |
| [crusoe](../../src/pipecat/services/crusoe) | 2 / 80 | Shared base / direct protocol; inspect imports | 0 |
| [deepgram](../../src/pipecat/services/deepgram) | 14 / 5,071 | `aws_sdk_sagemaker_runtime_http2`, `deepgram` | 9 |
| [deepseek](../../src/pipecat/services/deepseek) | 2 / 164 | Shared base / direct protocol; inspect imports | 2 |
| [elevenlabs](../../src/pipecat/services/elevenlabs) | 6 / 3,569 | Shared base / direct protocol; inspect imports | 6 |
| [fal](../../src/pipecat/services/fal) | 3 / 596 | Shared base / direct protocol; inspect imports | 0 |
| [fireworks](../../src/pipecat/services/fireworks) | 2 / 118 | Shared base / direct protocol; inspect imports | 0 |
| [fish](../../src/pipecat/services/fish) | 2 / 455 | `ormsgpack` | 1 |
| [funasr](../../src/pipecat/services/funasr) | 2 / 206 | `funasr` | 1 |
| [gladia](../../src/pipecat/services/gladia) | 3 / 953 | Shared base / direct protocol; inspect imports | 1 |
| [google](../../src/pipecat/services/google) | 16 / 8,258 | `google` | 21 |
| [gradium](../../src/pipecat/services/gradium) | 3 / 1,220 | Shared base / direct protocol; inspect imports | 1 |
| [grok](../../src/pipecat/services/grok) | 5 / 68 | Shared base / direct protocol; inspect imports | 0 |
| [groq](../../src/pipecat/services/groq) | 4 / 498 | `groq` | 1 |
| [heygen](../../src/pipecat/services/heygen) | 6 / 1,925 | `livekit` | 0 |
| [hume](../../src/pipecat/services/hume) | 2 / 419 | `hume` | 0 |
| [inception](../../src/pipecat/services/inception) | 2 / 117 | Shared base / direct protocol; inspect imports | 1 |
| [inworld](../../src/pipecat/services/inworld) | 5 / 3,543 | Shared base / direct protocol; inspect imports | 7 |
| [keenable](../../src/pipecat/services/keenable) | 2 / 143 | `mcp` | 1 |
| [kokoro](../../src/pipecat/services/kokoro) | 2 / 271 | `kokoro_onnx` | 2 |
| [mem0](../../src/pipecat/services/mem0) | 2 / 344 | `mem0` | 1 |
| [meta](../../src/pipecat/services/meta) | 2 / 512 | Shared base / direct protocol; inspect imports | 1 |
| [minimax](../../src/pipecat/services/minimax) | 2 / 482 | Shared base / direct protocol; inspect imports | 1 |
| [mistral](../../src/pipecat/services/mistral) | 4 / 683 | `mistralai` | 0 |
| [moondream](../../src/pipecat/services/moondream) | 2 / 225 | `intel_extension_for_pytorch`, `torch`, `transformers` | 0 |
| [moonshine](../../src/pipecat/services/moonshine) | 2 / 280 | `moonshine_voice` | 1 |
| [nebius](../../src/pipecat/services/nebius) | 2 / 79 | Shared base / direct protocol; inspect imports | 0 |
| [neuphonic](../../src/pipecat/services/neuphonic) | 2 / 636 | Shared base / direct protocol; inspect imports | 0 |
| [novita](../../src/pipecat/services/novita) | 2 / 82 | Shared base / direct protocol; inspect imports | 2 |
| [nvidia](../../src/pipecat/services/nvidia) | 7 / 3,007 | `aiobotocore`, `grpc`, `openai`, `riva` | 4 |
| [ollama](../../src/pipecat/services/ollama) | 2 / 89 | Shared base / direct protocol; inspect imports | 0 |
| [openai](../../src/pipecat/services/openai) | 15 / 8,595 | `openai` | 28 |
| [openclaw](../../src/pipecat/services/openclaw) | 4 / 1,204 | Shared base / direct protocol; inspect imports | 1 |
| [openrouter](../../src/pipecat/services/openrouter) | 2 / 226 | Shared base / direct protocol; inspect imports | 2 |
| [perplexity](../../src/pipecat/services/perplexity) | 2 / 114 | Shared base / direct protocol; inspect imports | 1 |
| [piper](../../src/pipecat/services/piper) | 2 / 324 | `piper` | 1 |
| [pocket_tts](../../src/pipecat/services/pocket_tts) | 2 / 231 | `pocket_tts`, `torch` | 1 |
| [qwen](../../src/pipecat/services/qwen) | 2 / 91 | Shared base / direct protocol; inspect imports | 0 |
| [resembleai](../../src/pipecat/services/resembleai) | 2 / 458 | Shared base / direct protocol; inspect imports | 1 |
| [rime](../../src/pipecat/services/rime) | 2 / 1,198 | Shared base / direct protocol; inspect imports | 1 |
| [sambanova](../../src/pipecat/services/sambanova) | 2 / 282 | Shared base / direct protocol; inspect imports | 2 |
| [sarvam](../../src/pipecat/services/sarvam) | 5 / 3,252 | `openai`, `sarvamai` | 4 |
| [simli](../../src/pipecat/services/simli) | 2 / 326 | `av`, `simli` | 0 |
| [smallest](../../src/pipecat/services/smallest) | 3 / 975 | Shared base / direct protocol; inspect imports | 2 |
| [soniox](../../src/pipecat/services/soniox) | 3 / 1,448 | Shared base / direct protocol; inspect imports | 3 |
| [speechify](../../src/pipecat/services/speechify) | 2 / 503 | Shared base / direct protocol; inspect imports | 1 |
| [speechmatics](../../src/pipecat/services/speechmatics) | 3 / 1,677 | `speechmatics` | 1 |
| [tavus](../../src/pipecat/services/tavus) | 2 / 290 | `daily` | 1 |
| [together](../../src/pipecat/services/together) | 4 / 1,008 | Shared base / direct protocol; inspect imports | 2 |
| [ultravox](../../src/pipecat/services/ultravox) | 2 / 857 | Shared base / direct protocol; inspect imports | 3 |
| [whisper](../../src/pipecat/services/whisper) | 4 / 1,079 | `faster_whisper`, `mlx_whisper`, `openai` | 2 |
| [xai](../../src/pipecat/services/xai) | 7 / 3,711 | `openai` | 13 |
| [xtts](../../src/pipecat/services/xtts) | 2 / 298 | Shared base / direct protocol; inspect imports | 1 |

## Complete module list

The canonical complete list is `modules` in [`inventory.json`](inventory.json),
with one record per baseline Python file. It is intentionally not duplicated as
1,434 Markdown checkboxes. To inspect a file, locate its exact `path`, resolve
its indexed dependencies/tests/routes through the schema, then open the pinned
source at its public-API line references. All 1,434 statuses are `not_migrated`.
