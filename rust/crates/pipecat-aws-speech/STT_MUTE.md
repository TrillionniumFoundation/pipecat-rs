# Transcribe STT mute routing

`SttMute(true)` and `SttMute(false)` update the AWS Transcribe processor in either
direction. The control is handled before the generic upstream forwarding branch.
Previously, that branch bypassed the upstream mute state update entirely.

The change preserves the existing native audio and control behavior:

- Controls continue to be forwarded with the original direction and envelope.
- Muted downstream `InputAudio`/`UserAudio` are not admitted to the recognition
  supervisor and do not update its current speaker or queued-audio usage.
- Existing audio-passthrough configuration still governs downstream audio frames.
- Unmuting resumes new audio, without replaying audio suppressed by mute.
- Already admitted audio, late transcripts, session draining/recovery, VAD
  controls, and current-speaker association retain their existing semantics.
- Upstream audio remains ordinary passthrough and does not change the speaker.
- This patch does not change mute state lifetime across setup or add microphone
  capture control, downstream media suppression, or a credentialed service test.

## Pinned Python contract and native differences

Reference: ProfHepta/pipecat `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
`src/pipecat/services/aws/stt.py` defines
`AWSTranscribeSTTService(WebsocketSTTService)` without overriding `process_frame`.
In `src/pipecat/services/stt_service.py`, `WebsocketSTTService` likewise inherits
that method from `STTService`. Its mute handler has no direction filter and its
muted PCM guard runs before speaker, usage, and provider send updates.

Python consumes the mute control. Native retains its existing control
passthrough rather than changing downstream application observations in this
routing fix. The source base also processes upstream PCM; native retains its
upstream audio bypass. Source reconnect buffering precedes its mute guard; native
supervisor admission/recovery follows the separately documented native contract.
Source usage timing (including closed/send-failure cases) differs from native
successfully queued usage and is not made equivalent by this patch.

The `tests/fixtures/python_aws_mute_source.json` fixture executes selected
original AST with immediate stubs,
including the original AWS send/receive methods. Its EventStream encoder is a
stub and does not prove wire encoding. The native regression instead uses a real
loopback WebSocket, independently verified SigV4 handshake/CRC EventStream,
process receipts, and late transcript delivery. Exact PCM, speaker, usage and
control-envelope assertions occur only after both fixture owners are settled.
