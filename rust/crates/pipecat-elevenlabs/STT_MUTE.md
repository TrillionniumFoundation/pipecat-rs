# Realtime STT mute routing

ElevenLabs realtime STT applies `SttMute(true)` and `SttMute(false)` before the
upstream forwarding branch. Previously that branch returned before applying an
upstream control. An upstream unmute now also restores input after a downstream
mute; this is not just a repeated-value toggle case.

The native control frame continues in its original direction with its original
envelope. This remains independent of the audio-passthrough option. Muted new
downstream `InputAudio`/`UserAudio` do not enter the provider supervisor, change
its current speaker, or accrue queued-audio usage. Unmute admits new input only.
Upstream audio still bypasses recognition. Existing admitted audio/results,
manual VAD-stop commit, current-speaker association, reconnect behavior, and mute
state lifetime across setup are unchanged. The TTS and segmented HTTP STT paths
are not modified.

This controls recognition admission, not microphone capture, downstream media
suppression, or withdrawal of already transmitted audio.

## Fixed reference

ProfHepta/pipecat `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/services/elevenlabs/stt.py`:
`ElevenLabsRealtimeSTTService` inherits `WebsocketSTTService`, and its
`process_frame` first invokes the base handler without a direction guard. The
base in `src/pipecat/services/stt_service.py` applies mute before speaker, usage,
and send handling. After that, the realtime subclass still sends manual commit
on VAD stop while muted. This contract is distinct from the segmented
`ElevenLabsSTTService` class in the same source file.

Python consumes mute controls; native retains its established control
passthrough. The source base can process upstream audio; native retains its
upstream audio bypass. Source reconnect buffering and usage timing are not made
equivalent by this one-branch routing fix.

Qualification includes a pre-fix real WebSocket negative control, followed by
exact per-commit PCM, late speaker, usage and control-envelope checks with audio
passthrough enabled and disabled. All assertions about the captured wire occur
after the runtime and peer have actually joined. Scripted provider replies do
not qualify live credentials or model quality. The source oracle uses selected
original AST with immediate stubs (captured in
`tests/fixtures/python_realtime_stt_mute_source.json`), including original base64/JSON send and
manual-commit methods, not a full Python scheduler.
