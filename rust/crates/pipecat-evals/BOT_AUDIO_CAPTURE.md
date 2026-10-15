# Bounded bot audio capture

`EvalSession::run_with_bot_audio(cancel, BotAudioLimits::default())` evaluates the
same scripted text/events while returning `BotAudioRun { result, chunks }`.
Each chunk contains original-rate mono little-endian signed PCM16 bytes and its
sample rate. It is a library API, not a CLI recording switch. Existing `run`,
scenario schemas, hosted suite results and text-only defaults are unchanged.

For the native loopback host, explicitly build
`EvalBotHost::with_audio_output(true)`. This only grants permission: a connection
must also request `capture_bot_audio=true`. The capture client omits `skip_tts`,
matching the pinned Python client's audio-mode query, and sends
`audio_response: true` with text inputs. The host supplies `skip_tts: false` to
its application factory and applies output configuration before readiness.
Ordinary connections still use the original text-only configuration and
capability shape. `capture_bot_audio=true` with `skip_tts=true` is rejected.
Applications must actually provide TTS or another PCM-producing processor.
Input recordings remain independently selectable; enabling output does not add STT.

## What the result proves

Capture includes only valid chunks received before the scenario ends. A passing
LLM-text assertion may end evaluation before synthesis finishes. Use an explicit
completion event appropriate to the application when later audio matters.
A `bot_stopped_speaking` fixture barrier does not establish universal provider
ordering or physical playback. There is no implicit audio-drain protocol.

The consuming API cannot reuse or share a capture buffer across sessions. It
returns after the existing session/transport/audio-input shutdown path. Runtime
failure or cancellation is reported through the ordinary failed `ScriptResult`;
previously admitted chunks remain available. Invalid capture limits return an
error before connection. Cancellation cannot retract previously transmitted or
received PCM. Dropping the run future keeps the existing owned-task cancellation
contract, but provides no returned partial result.

The result performs no resampling, STT, TTS-text reconstruction, audio judging,
WAV writing, mixing, or speaker segmentation. `tts_response` and heard-speech
`response` in the Python harness are different downstream event paths; neither
is implemented by exposing these bytes. Real human-speech evaluation still needs
an application-selected transcription/judging path and separate qualification.

## Wire, bounds and source differences

Pinned source: `ProfHepta/pipecat` at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, eval serializer and client.
The original serializer sends `eval-bot-audio` with `data.audio` (base64) and
`data.sampleRate`; its client assumes mono because there is no channel field.
Native host capture covers downstream `OutputAudio`, `TtsAudio` and
`SpeechOutputAudio` at the final pipeline boundary, after the assistant.
It uses the existing bounded event queue and invocation fence, including terminal
flush handling; input/upstream audio is not captured. Ordinary TTS and speaking
start/stop observations are enabled for an audio-enabled connection. The host
does not mark ordinary observations as having passed a physical output transport,
so this does not synthesize TTS-text playback events. No playback acknowledgement
is asserted. Arbitrary application transport envelopes remain
untrusted input and the client validates audio payloads again.

The native subset accepts 8, 16, 24 and 48 kHz, mono, nonempty complete PCM16
samples. The host rejects other channels rather than silently treating stereo as
mono. Unlike Python's decoder, the capture client retains original rates instead
of resampling to its STT rate. Base64 is strict; invalid rates, empty/odd data,
missing fields and overflow reject the whole chunk without partial admission.
Unknown fields are not retained. Capturing different rates preserves distinct
chunks rather than concatenating them into a misleading single-rate stream.

Limits default to 8 MiB and 4096 chunks; maximum configurable values are 16 MiB
and 16384 chunks, both nonzero. Encoded length is checked before decoding, then
actual bytes before insertion. Audio is kept outside `events_seen`, avoiding
base64 copies in normal results. Existing inbound wire/message/session limits
still apply. Host frame/metadata/processed budgets also apply, with a conservative
base64 wire allowance of 128 bytes for the JSON envelope before allocation; this
is a defense-in-depth bound, not an exact serialized-size calculation. Under the
current minimum message limit, the existing JSON u8-array frame budget is stricter
and rejects oversized typed PCM first. Queue overflow
is a visible pipeline failure, never silent capture truncation.

Qualification uses local generated PCM and actual native WebSocket transport.
It does not qualify a microphone, speaker, browser playback, live provider or
speech recognizer.
