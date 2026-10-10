# Speechmatics Agent STT native slice

## Scope

This optional provider crate implements a direct native WebSocket client, bounded
session actor, canonical settings, detailed transcription mapping and a native
FrameProcessor. It is not Speechmatics TTS or complete provider parity. No Python,
SDK process, interpreter, FFI provider SDK or live provider account is needed.

The initial audio contract is mono 16 kHz signed PCM16 little-endian. Each supplied
complete, nonempty PCM packet is sent as one binary message; there is no fixed
1024-byte packet requirement. The service/model remains responsible for inference.

## Sources

Static reference: ProfHepta/pipecat commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, especially
`src/pipecat/services/speechmatics/stt.py`, `services/stt_service.py`,
`services/stt_latency.py`, and `tests/test_speechmatics_stt.py`.

Exact locked source archives, read statically and never imported or executed:

- speechmatics-agent-stt 0.1.0, SHA-256
  `ca17027d43aab73df16ce757600027f107b07de60aa2434274a0fe0bc0889452`
- speechmatics-rt 1.1.1, SHA-256
  `c560df8edcf0af3a565310f1a744feee9a9c0bb35e665225305ae7e7343a390f`

Implementation mapping:

| Native module | Static source behavior |
| --- | --- |
| settings.rs | Pipecat stt.py 129–253, 454–469, 884–896, 1184–1299; Agent `_models.py` TurnConfig and Segment; RT `_models.py` speaker configuration |
| common.rs, client.rs | Agent `_url.py`, `_client.py` 137–199/273–327; RT `_transport.py` authentication/binary sends; `_async_client.py` recognition/end waits |
| protocol.rs | Agent `_models.py` singular Segment/TimedEvent; Pipecat stt.py 925–1034 |
| processor.rs | Pipecat stt.py 552–577, 957–1000, 1068–1115; base stt_service.py explicit-finalize flag; native core/services/turns contracts |

The pinned RT SDK overwrites its sent sequence counter with delayed AudioAdded
acknowledgments. This implementation deliberately fixes that bug: sent and ACK
counts are separate, regressions/duplicates do not lower either count, impossible
ACKs fail, and EndOfStream always carries the actual sent-message count.

## Protocol and compatibility decisions

- Default endpoint is the pinned EU2 `/v2/agent` endpoint. Paths collapse repeated
  slashes and append the exact `agent` segment when absent. Production endpoints
  require WSS; unencrypted WS is permitted only for loopback fixtures. Caller URL
  userinfo, fragments and all query strings are rejected. SDK-preserved arbitrary
  routing queries are deliberately unsupported, rather than silently discarded.
- Authorization is Bearer; each connection has a UUID X-Request-Id. Query telemetry
  honestly identifies `pipecat-rust/0.1.0` and `native-rust-agent-stt/0.1.0`, not Python.
- One StartRecognition precedes the absolute ready deadline. No audio is admitted
  until RecognitionStarted. `turn_config` is top-level; optional absent settings
  are omitted, false/zero values retained, no chunk_size field is sent.
- Audio admission is nonblocking and bounded both by packet count and total bytes.
  `send_audio` has the same pressure behavior as `try_send_audio`; it does not hide
  congestion in a waiting producer. A failed/ambiguous write is never replayed.
- Finalize is external-mode only and ordered in the same queue as audio. Its
  timestamp is sent PCM bytes / 32000, never wall time. Zero-progress/duplicate
  boundaries are rejected before creating a pending marker. No explicit arbitrary
  timestamp API is exposed. New VAD speech invalidates pending confirmation.
- `is_final` means AddSegment. `finalized` means the first nonempty final following
  an actually sent explicit finalize, once only. The wire has no request ID for
  this response: the association is a source-compatible heuristic, not proof of
  causation. Partial messages are replacement previews; segments are not joined.
- Missing/null segment/metadata objects retain SDK empty defaults; explicit wrong
  types, negative/nonfinite times and oversized values fail. Empty text is ignored;
  whitespace and punctuation are preserved. `{ts}` formats audio start seconds;
  frame.timestamp is current UTC. Templates allow only text, speaker_id, ts and
  lang plus escaped braces, without Python format specifiers or expressions.
- Supported language bases are pinned to the source; fil maps to tl. English
  en-GB/en-US/en-AU set output_locale; other English regions emit LocaleFallback
  and retain the originally configured language on frames.
- GetSpeakers has an explicit final flag. Low-level SpeakersResult is a bounded,
  redacted-Debug envelope retaining the provider object because the pin supplies
  no stronger typed speaker-result schema. The processor exposes a bounded
  subscribe_speakers subscription; lag is explicit. Opaque identifiers must not be
  interpreted as cross-account identities. Generic arbitrary commands are deferred.

## Ownership, settings and lifecycle

- There is no automatic reconnect, backoff, retry or audio replay in this slice.
  These are explicit residual gaps against Python's retry/deferred-reconnect
  behavior. Build a fresh client/processor/session after transport failure.
- Local speaker formatting updates are validated and atomically queue-admitted;
  rejected admission preserves the prior settings. Effective active wire changes
  return RequiresRestart, including language/model/partials/turn mode. No runtime
  mode change silently changes user aggregation or interruption policy.
- Use recommended_user_config when constructing the native user aggregator if
  service VAD ownership is wanted. In external mode normal caller/native VAD
  strategy choices remain in control. Source speech signals do not resolve turns;
  service StartOfTurn/EndOfTurn only propose boundaries. The user aggregator owns
  resolved starts/stops and interruptions.
- Persistent STT background work is not canceled by ordinary bot interruption.
  Session serial plus health generation fence each output. A health change closes
  the old socket; toggling health alone does not reconnect or revive that socket.
- Mute cancels the old session, drops unsent queued audio and fences callbacks.
  Unmute explicitly opens a fresh session asynchronously. Audio arriving before
  that handshake is ready reports pressure; it is not buffered/replayed. Every
  socket is a distinct epoch, so later old-socket callbacks cannot be admitted after
  unmute. Fences govern new output admission: frames already admitted to pipeline
  queues and already-started downstream effects cannot be retroactively withdrawn.
- End/Stop set a pipeline emission barrier before finish; draining transcript,
  turn and speaker callbacks are suppressed. Low-level graceful finish instead
  exposes final drain events and succeeds only after EndOfTranscript. Missing end,
  premature close and output pressure are errors, not successful completion.
  Cancel/drop close ownership without pretending a transcript was finalized.
- Finish is an atomic one-way gate shared by all sender clones. Every command
  admitted after it fails. Canceled finish admission does not close the gate.
  Dropping a session cancels its actor despite live sender clones; dropping a
  pending next() future leaves the terminal result retryable.
- Connect/ready/write/output/drain/close are bounded. Absolute ready/drain bounds
  cover busy unknown-message streams and blocked writes. The command/socket select
  only chooses cancel-safe receives; consumed provider events are emitted outside
  the select and cannot disappear when a command becomes ready.
- Usage accounts only successfully sent PCM and is emitted before a final frame,
  once per accumulated audio interval. Service-VAD processing metrics begin on
  StartOfTurn and close after the first final. External mode invents no such span.
  Metadata TTFS P99 0.74 seconds is a source preset, not a measured native result.

## Logging and security boundary

This crate does not log keys, endpoint queries, raw server errors, transcripts or
speaker identifiers. Its own configuration/error Debug and Display output is
redacted/bounded. Public transcript/event Debug contains user content and must not
be written to application logs. This is not a claim that the entire network stack is secret-proof.

Tungstenite 0.28 may log serialized authentication requests at TRACE
(`handshake/client.rs:200`), frames/messages at TRACE (`protocol/frame/mod.rs`,
`protocol/mod.rs`) and close reasons at DEBUG. Marking a HeaderValue sensitive does
not redact those serialized bytes. Hosts must suppress both `tungstenite` and
`tokio_tungstenite` log targets below WARN (or OFF). This crate does not change a
host's global logger or weaken TLS/network/runtime policies.

## Verification boundary

Native tests use synthetic localhost WebSocket and HTTP peers, plus the actual
native core pipeline, user/assistant aggregators and LLM processor. They qualify
transport/state/ordering/resource invariants, not recognition quality, latency,
regional service availability or actual acceptance of optional model knobs.

Current official docs recommend global routing and describe 8/16 kHz PCM/float,
required increasing finalize timestamps and in-session partial setting changes.
Those do not replace the pinned contract: this slice explicitly supports only
16 kHz PCM16, rejects duplicate finalize positions, and requires restart for active
wire changes. Live credentials/devices, broader audio formats, HTTP TTS, generic
commands, Python deprecated constructor compatibility, retry/backoff and provider
availability remain outside this slice.
