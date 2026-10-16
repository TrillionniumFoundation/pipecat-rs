# Focused migration review, 2026-10-14

This is a read-only entry-point review around slice 80, not a new whole-project
qualification or a source-file completion percentage. The original inventory and
COVERAGE/CHECKPOINT snapshot remain historical. Current implementations already
include Piper pipeline ownership, signed continuation, shared video sampling,
completion/eager tool gates, RNNoise/MoQ, native judging, heartbeat/idle monitoring
and the recently qualified speech control routes. Those old open rows must not
be counted as absent features today.

Selected next user path: shared sampled images reached the native transport but
Gemini Live rejected InputImage/UserImage. The bounded RGB video path described in
[VIDEO_INPUT.md](../crates/pipecat-gemini-live/VIDEO_INPUT.md) addresses that specific
service boundary. Full pixel-format parity, live capture and provider acceptance
remain separate.

Historical slice80 candidates (status updated below):

- Rich usage to clients: services decode cache-read/write and reasoning token
  fields and retain full usage in frame metadata, but core Metric::LlmUsage and
  RTVI metrics projection expose only prompt/completion/total. The data is not
  wholly lost; the typed client-facing projection is incomplete.
- RTVI audio-level reporting: the current observer drops audio frames without
  level events. The fixed source uses a rolling, sample-rate-aware loudness
  tracker. A simple RMS helper would not establish that numerical contract.
- Scripted audio evaluation: scenario validation still rejects non-text modality
  and turn audio/image inputs. Existing native hosting, judging and text/DTMF
  checks do not establish audio replay through STT/turn/TTS.
- Gemini long sessions: resumption updates and GoAway/reconnect handling remain
  explicit unsupported/error paths. A new video input does not provide resumption.

Browser, physical audio/video, hosted SFU, WAN/TURN and live-provider qualification
are distinct acceptance work. Existing host-failure records are historical; this
review did not rerun them or assert that their environmental blockers still hold.
Project-generator transport selection needs an entry-point-specific comparison
against the already qualified generated RTC/WS browser paths before being called
a missing feature; no new scaffold gap is asserted here.

Follow-up status after the focused review: the optional cache/reasoning usage
projection and rolling RTVI audio levels now have their own qualified native
paths (see [usage details](../crates/pipecat-rtvi/USAGE_DETAILS.md) and
[audio levels](../crates/pipecat-rtvi/AUDIO_LEVELS.md)). Recorded
user audio now has a bounded library client path described in
[recorded audio](../crates/pipecat-evals/RECORDED_AUDIO.md); synthesized users,
live-provider qualification remained separate at that checkpoint.
The built-in eval host now separately offers opt-in single raw-audio reception
for that 16 kHz mono library path, while keeping generated-text output. Its actual
local receiving pipeline is covered by a PCM-byte fixture; human speech still
requires application-provided STT/VAD or a realtime model. YAML/CLI recording
loading now has an explicit Unix direct-run opt-in for bounded trusted-local
WAV bundles. Explicit hosted suite recording input now reuses the same loader and
native child ownership, with whole-manifest/repeat budgets and pre-spawn plan
revalidation; see [recorded suites](../crates/pipecat-evals/RECORDED_SUITE.md).
Synthesized users remained separate at that checkpoint. See also
[recorded scenarios](../crates/pipecat-evals/RECORDED_SCENARIOS.md). These are separate contracts, not a whole-project
completion claim.

Bot audio now also has an explicit bounded library capture path from a native
host output boundary to original-rate PCM results; see [bot audio capture](../crates/pipecat-evals/BOT_AUDIO_CAPTURE.md).
It captures only chunks received before scenario completion. Capture alone does
not transcribe or judge those chunks; live-provider acceptance remains separate. A Unix direct CLI run can
now explicitly export those received bot chunks as a bounded same-rate mono WAV,
with no-clobber publication and preserved evaluation results on artifact failure.
This bot-only arrival-order artifact omits gaps, user tracks, resampling and
mixing; it is not full Python conversation-recorder parity or audio scoring.


## Current focused status, 2026-10-15

Slices89–93 connect captured bot PCM to real local Silero segmentation, bounded
HTTP STT and completed `response` events, through explicit direct-run and hosted
suite configuration. Owned native loading preserves cooperative cancellation and
actual joins; optional Smart Turn adds local model endpointing. See
[BOT_AUDIO_RESPONSE.md](../crates/pipecat-evals/BOT_AUDIO_RESPONSE.md) for the
precise limits and source timing differences. The older rows above are not
current evidence that audio transcription is missing.

Slice94 connects actual spoken STT text to the existing semantic classifier
independently from reported LLM text, with Continue/No, interruption and
cancellation ownership verified end-to-end. Its
qualification is recorded separately; this inventory is not a whole-project
parity or live-provider claim.

Remaining substantial evaluation paths include:

- Synthesized scripted user speech: file-backed recorded inputs exist, but
  source `user.speech` synthesis and its cache contract are not connected.
- Timestamp-aligned two-sided recording: bot-only received-chunk WAV export
  omits user tracks, gaps, resampling/mixing and interruption-tail accounting.
- Scripted user-image delivery: observation exists, but scenario image loading
  and the host image-request workflow remain rejected.
- Simulated callers/personas: scripted evaluation is implemented; the source
  persona conversation, hangup and goal-scoring owner is a separate larger path.

Slice95 adds an explicit bounded Gemini receive-side session-resumption policy;
see [its contract](../crates/pipecat-gemini-live/SESSION_RESUMPTION.md). Recovery
uses a saved provider handle and never replays already sent actions. It does not
establish lossless delivery, handle-less history reseeding, transparent message
index recovery or live-provider qualification. Future gaps must be checked
against actual code and source, rather than inferred from older inventory rows.

Slice96 adds explicit typed Gemini Live thinking setup for tool-free conversation
sessions and delays a stored `turnComplete` while background reasoning remains
`IN_PROGRESS`. See [thinking completion](../crates/pipecat-gemini-live/THINKING.md)
for silence failures, local interruption attribution and session resumption.
Thinking with tools is rejected before connection. At that checkpoint,
NON_BLOCKING declarations and intermediate/silent tool responses remained a
separate provider-semantic gap; slice98's ordinary-Live subset is described below.
This thinking subset does not imply live-model
availability or reproduce Python's synthetic completion on watchdog expiry.

Slice97 addresses an Anthropic provider-compatibility failure: a legal
`redacted_thinking` content block previously aborted the native stream as
unsupported. Its opaque data now follows the ordered native history carrier and
Anthropic request replay path; see [the contract](../crates/pipecat-services/ANTHROPIC_REDACTED.md).
This is not a claim that the fixed Python stream implemented the same chain:
that source omitted these blocks, although its adapter could preserve explicitly
supplied native Anthropic messages.

Slice98 adds explicitly selected ordinary-Live NON_BLOCKING tools, intermediate
and silent results, provider cancellation and cross-interruption/resumption
ownership. See [ASYNC_TOOLS.md](../crates/pipecat-gemini-live/ASYNC_TOOLS.md) for
the exact model profiles and required matching ToolExecutor registration policy.
Thinking with tools remains rejected. Local WebSocket and production pipeline
qualification does not establish remote model acceptance or exactly-once effects.

Slice100 adds bounded manual-turn interruption audio pre-roll to the paired
OpenAI Realtime processor. See [PREROLL.md](../crates/pipecat-realtime/PREROLL.md)
for the successful-local-flush window, clear/replay/cancel/truncate order,
readiness and generation boundaries, and explicit differences from the fixed
Python service. The standalone client and server-VAD path retain their existing
behavior. This closes a provider-buffer lifecycle gap; it does not establish
remote processing acknowledgements, speech-recognition quality or reconnection.
