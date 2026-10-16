# Dedicated transcription audio passthrough

`GeminiLiveProcessor::with_audio_passthrough(bool)` controls forwarding of
original `InputAudio` and `UserAudio` frames in dedicated transcription mode.
It defaults to true. False suppresses both frame families in both directions;
conversation mode ignores the setting, including while conversation audio is
paused. The builder setting survives setup and sequential processor reuse.

## Routing and recognition are independent

The forwarding gate runs after the existing processing path. Disabling it does
not skip downstream PCM validation, raw/encoded size limits, provider admission,
or dynamic speaker-label updates. Valid empty downstream audio still updates the
dynamic label without sending PCM. Invalid input still returns its existing
error; a provider enqueue failure still retains the observed label and does not
grant finalization eligibility. Fixed `with_user_id(...)` overrides are unchanged.
Upstream audio is neither validated nor sent to Gemini and cannot change the
dynamic label, whether it is forwarded or suppressed.

`SttMute` remains independent: muted downstream audio skips validation, provider
admission and label updates, then follows the same passthrough setting. No muted
audio is retained for replay. False does not mute recognition, stop microphone
capture, prevent provider transmission, or withdraw previously admitted audio.
It is an output-routing option, not a privacy control.

Forwarded frames retain their identity, metadata, payload, context and direction.
Other frame families are unaffected, including output/TTS audio, transcripts,
VAD controls, metadata requests and responses, and lifecycle controls. Start
metadata ordering, VAD-stop finalization, End draining, Stop/Cancel cleanup and
the client's finish admission rules remain unchanged. No client configuration,
wire message, task or queue is added.

Downstream SmartTurn, acoustic analysis and VAD processors need PCM. Keep
passthrough enabled for them, or place them before this service when disabling
it. Transcripts and forwarded speech-boundary frames do not replace PCM for an
audio-dependent processor.

## Pinned source boundary

Reference: ProfHepta/pipecat `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/services/stt_service.py`, inherited by
`src/pipecat/services/google/gemini_live/stt.py`.

The Python base defaults `audio_passthrough` to true and conditionally forwards
audio after `process_audio_frame`, including when mute or empty input returns
early. Its audio branch has no direction guard. Native applies the forwarding
option in both directions but keeps its existing downstream-only provider
admission and dynamic-label updates. It also keeps its stricter PCM validation
and does not add Python reconnect buffering or usability lifecycle semantics.
This local routing contract does not establish live provider compatibility or
full Python service/scheduler parity.
