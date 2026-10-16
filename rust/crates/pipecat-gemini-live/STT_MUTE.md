# Dedicated transcription mute

`GeminiConfig::transcription` now handles the existing core `SttMute(bool)`
control. It consumes the control in either direction. The update is invocation-
fenced and shared by the processor's audio path. No new configuration, provider
request, activity signal, or task is created by toggling mute.

While muted, new downstream `InputAudio` and `UserAudio` bypass PCM validation
and provider admission. They are forwarded unchanged by default, or suppressed
with `with_audio_passthrough(false)`. The buffer does not retain muted audio.
Unmuting permits subsequent new audio. Upstream audio follows the same configured
passthrough setting regardless of mute. Conversation sessions neither consume nor
apply `SttMute`; their existing audio and manual-preroll behavior remains unchanged.

Mute cannot withdraw already admitted input. Interim/final provider responses
continue, as does VAD-stop `audioStreamEnd` finalization. Dedicated transcription
requires server VAD, so this control has no manual idle-preroll buffer to clear.
This control only gates recognition input. Configure microphone capture
separately; the independent [audio passthrough option](STT_AUDIO_PASSTHROUGH.md)
controls original-frame routing without pausing recognition.

A new native setup resets mute to false, including sequential reuse of the same
processor. The pinned Python base initializes mute in its constructor and does
not explicitly reset it in setup/start; this reuse behavior is an intentional
native session boundary.

## Reference and qualification

Pinned ProfHepta/pipecat `af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- `src/pipecat/services/stt_service.py`: `process_frame` consumes mute in either
  direction; `process_audio_frame` returns while muted, while the caller still
  performs configured audio passthrough.
- `src/pipecat/services/google/gemini_live/stt.py`: inherits that behavior and
  still sends finalization after its base handles VAD stop. Its receive callbacks
  do not suppress already in-flight transcription results.

The source reconnect buffering check precedes mute. Native optional
[session resumption](SESSION_RESUMPTION.md) keeps the existing bounded command
queue during a replacement handshake. Muted input is still filtered before that
queue; native does not reproduce the source pre-mute buffering path or replay
already sent audio. Native audio passthrough defaults to true and can be disabled
for both directions with `with_audio_passthrough(false)`. Provider input and
dynamic speaker-label updates remain downstream-only, unlike the source base's
audio handler without a direction guard. Conversation mode ignores this option.

The captured `tests/fixtures/python_stt_mute_source.json` fixture runs selected
original AST with immediate stubs. Loopback
WebSocket tests exercise the actual native processor, both audio families,
control consumption, invalid/oversized muted PCM passthrough, delayed utterance
finalization while muted, conversation isolation, and sequential setup. Scripted
provider responses do not qualify a live provider or a full Python scheduler.
