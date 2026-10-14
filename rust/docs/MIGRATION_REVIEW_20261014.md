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

Other concrete entry points still deserving staged work:

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
