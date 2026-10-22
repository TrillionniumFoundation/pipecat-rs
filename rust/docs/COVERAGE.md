# Native implementation evidence and remaining scope

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

See [LiveKit validation](../crates/pipecat-livekit/VALIDATION.md) for source provenance and test scope.


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


For source recovered after the executor replacement and its fresh, narrower
qualification, read [RECOVERY.md](RECOVERY.md). The snapshot below is historical;
it is not the latest implementation status or current-host qualification.

Evidence snapshot: source commit `5c262d16146c82114801d9dc9255a35c4f61048c`,
2026-10-09 UTC. This is a partial migration, not production readiness or a
whole-module parity certificate. [MIGRATION.md](MIGRATION.md) and
[inventory.json](inventory.json) preserve the original source inventory. This
index records subsequent implementation without rewriting that historical audit.

## How to read the evidence

- An implementation link establishes where native behavior lives, not that every
  Python API, provider option, example or release workflow has been ported
- Static source-derived fixtures, synthetic protocol peers, actual local model
  execution, real devices and live providers are different evidence classes
- Whole-workspace outcomes belong to the exact commit in
  [CHECKPOINT.md](CHECKPOINT.md). Later working-tree changes do not inherit them
- Rust runtime components and Cargo tests do not invoke Python. Some committed
  fixture generators use Python offline during development to record pinned
  original behavior; normal Rust tests replay the resulting static data
- Native C/C++ SDK/model/codec bindings are documented dependencies, not Python
  fallbacks. Their ownership, licenses, toolchains and packaging remain relevant
- Counts of crates, frames, test cases or source files are not completion rates

## Current partial coverage

| Area | Native evidence entry points | Remaining boundary |
| --- | --- | --- |
| Frames, processor runtime and usability | [core](../crates/pipecat-core/README.md), including revisioned health, attributed errors and boundary-time Continue/End/Cancel policy | Revision-safe health gating exists for six speech adapters, including the Whisper runner wrapper; remaining adapters and the consecutive-zero-audio watchdog need separate integration |
| Sequential/parallel pipelines | [pipeline](../crates/pipecat-pipeline/README.md), owned branches, interruption and flush tests | Native manual/failover switching has causal-fenced admission and read-only aggregate health; inference/tool effects are not automatically replayed, and nested compound leaf metadata remains unsupported |
| Worker orchestration | [workers](../crates/pipecat-workers/README.md), [agent workers](../crates/pipecat-agent-workers/README.md), [proxy](../crates/pipecat-worker-proxy/README.md), [network bus](../crates/pipecat-network-bus/README.md) | Deployment and real broker/database qualification are distinct from injected protocol peers; the non-loopback Redis route check remains environment-dependent |
| PCM, transport and wire behavior | [audio](../crates/pipecat-audio/README.md), [media transport](../crates/pipecat-media-transport/README.md), [protocol](../crates/pipecat-protocol/README.md), [RTVI](../crates/pipecat-rtvi/README.md) | Accepted/submitted audio is not evidence of audible playback; each destination must supply its documented receipt semantics |
| Turn/context behavior | [turns](../crates/pipecat-turns/README.md), identity-aware idle detection, bounded owned context summaries, composable start/mute/stop policies and default bounded empty-turn recovery | LLM completion markers, eager speculation and tool-effect gates remain open; external verdict notification is not speculative inference, and realtime metadata is explicitly unsupported by the cascade aggregator |
| Text and alignment | [text](../crates/pipecat-text/README.md), [alignment](../crates/pipecat-text/ALIGNMENT.md), pinned static map/tracker traces, TOKEN promotion and provider/output integration | Cartesia, HTTP TTS and ElevenLabs use source/display/synthesis provenance and context-owned output timing; timestamp-free completion remains Unreported and does not populate audible history, even merely because whole audio was later submitted or played |
| HTTP and streaming services | [LLM services](../crates/pipecat-services/README.md), [speech](../crates/pipecat-speech/README.md), [streaming speech](../crates/pipecat-streaming-speech/README.md), [Responses](../crates/pipecat-responses/README.md) | Dedicated Responses HTTP/SSE and serialized WebSocket inference exists; signed Anthropic/Gemini continuation, other advanced options and the wider provider inventory remain open. Loopback checks are not live account/model acceptance |
| Realtime providers | [OpenAI Realtime](../crates/pipecat-realtime/README.md), [Gemini Live](../crates/pipecat-gemini-live/README.md), [ElevenLabs](../crates/pipecat-elevenlabs/README.md) | Crate readmes enumerate unsupported modes; advanced Gemini resumption/video/Vertex and ElevenLabs multiplexing/dialogue are not covered; no live-provider qualification |
| Local model execution | [VAD/Smart Turn](../crates/pipecat-vad/README.md), [Whisper](../crates/pipecat-whisper/README.md) and [runner](../crates/pipecat-runner/README.md), [Piper](../crates/pipecat-piper/README.md) | Actual CPU ONNX/Whisper/Piper execution exists; Piper is a standalone buffered synthesis service here, not yet a pipeline adapter. Its explicit optional GPL phonemizer, uninterruptible native load and unestablished weight-redistribution license are documented. Other local models/platforms and reusable Whisper Stop lifecycle remain open |
| WebRTC and hosted media | [SmallWebRTC](../crates/pipecat-webrtc/README.md), [LiveKit validation](../crates/pipecat-livekit/VALIDATION.md) | Native loopback peers and real signaling-socket cleanup are tested; a successful hosted SFU room, browser media and public NAT/TURN are not established; Daily/MoQ/video/avatar coverage remains open |
| Device audio | [local audio](../crates/pipecat-local-audio/README.md) | CPAL capture and submission-only output are implemented; actual Linux ALSA null-device exercise supplies zero/discard audio, not physical microphone/speaker acceptance or Played receipts |
| Flows and tools | [flows](../crates/pipecat-flows/README.md), [processors](../crates/pipecat-processors/README.md), [MCP](../crates/pipecat-mcp/README.md) | Bounded YAML includes deliberately reject executable tags, legacy merge keys and some permissive indentation; MCP legacy SSE/OAuth and other framework integrations remain open |
| CLI, generation and evals | [CLI](../crates/pipecat-cli/README.md), [project generator](../crates/pipecat-project/README.md), [evals](../crates/pipecat-evals/README.md) | Native scaffolding is a bounded preset, not all original templates; native compiled-bot hosting and bounded release manifests exist, with direct-child-only ownership and explicit OS/Windows caveats; audio/simulation/judges and full release workflows remain open |
| Browser and hosting | [Rust browser workspace](../client-templates/README.md), [client server](../crates/pipecat-client-server/README.md), [runner](../crates/pipecat-runner/README.md) | Rust/WASM build and native wire/lifecycle checks exist; actual browser execution is blocked before launch on this host; advanced UI/device/video/attachments and all original frontend templates remain scope |
| Observability and distribution | [observers](../crates/pipecat-observers/README.md), each crate's explicit errors/limits | Original exporter integrations, complete packaging/platform matrix, every example and source-file disposition remain unfinished |

Read the linked files at the snapshot commit when reproducing this table; later
working-tree changes may extend those files. Documentation that describes a
contract does not itself count as its validation.

## Prioritized remaining integration work

1. Finish the source consecutive-zero-audio TTS watchdog and native Piper pipeline
   lifecycle/alignment/metrics integration. Keep source audio-observed evidence
   separate from playable PCM and confirmed audible history. Default error
   recovery must preserve health revisions and must not replay provider work.
2. Add deterministic per-participant/source video sampling and image-request
   ownership to shared input transport. Then qualify actual camera, codec and
   hosted video backends separately. An image-frame enum is not video transport.
3. Preserve provider-specific signed continuation, beginning with Anthropic
   thinking signatures and Gemini thought-signature tool history. Generated text
   must not silently bypass receipt-backed heard-history rules.
4. Port remaining LLM completion-marker/eager strategies in reviewed stages:
   parser and deadlines, provisional context, ordered output gate, and independent
   tool-effect admission. An external stop verdict alone implements none of these.
5. Extend opt-in typed observations and conversation-turn tracking before adding
   exporter integrations. Keep raw prompts, media, credentials and tool payloads
   out of default observations; integrate existing recording rather than duplicate
   its buffering and resampling.
6. Expand genuine provider and native backend families: Azure authentication and
   routing, AWS protocols, RNNoise, MoQ, remaining local models and vendor media.
   Pin ABI/version/license provenance; a generic endpoint preset is not parity.
7. Complete frontend/application templates, examples, source-by-source
   dispositions and distribution/platform acceptance. Keep physical devices,
   live providers, SFU and deployment gates separate from deterministic tests.

These are remaining contracts at the fixed snapshot, not claims that subsequent
working-tree work has passed. The static inventory is historical. No percentage
of whole-project completion is inferred from test or crate counts.

## Current environment qualification limits

Chromium fails before app launch on the host's socket restriction. The official
local LiveKit server fails before listening because required netlink introspection
is denied. Neither failure was bypassed through relaxed security settings or a
substitute remote service. Device checks have only ALSA null audio. These are
recorded acceptance blockers, not evidence that the native interfaces are absent,
and not permission to mark the checks passed.
