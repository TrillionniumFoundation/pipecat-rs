# Bounded bot audio capture

`EvalSession::run_with_bot_audio(cancel, BotAudioLimits::default())` evaluates the
same scripted text/events while returning `BotAudioRun { result, chunks }`.
Each chunk contains original-rate mono little-endian signed PCM16 bytes and its
sample rate. The CLI can explicitly export these received chunks as a WAV (below). Existing `run`,
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
mixing, or speaker segmentation. Optional WAV export only packages received PCM. `tts_response` and heard-speech
`response` in the Python harness are different downstream event paths; neither
is implemented by exposing these bytes. Real human-speech evaluation still needs
an application-selected transcription/judging path and separate qualification.
The distinct native [spoken-response path](BOT_AUDIO_RESPONSE.md) supplies local
VAD, HTTP STT and explicit semantic judgments; capture itself does not.

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

## Explicit CLI WAV export (Unix)

Run exactly one scenario against an audio-capable endpoint:

```sh
pipecat-eval run scenario.yaml ws://127.0.0.1:8765 --bot-audio-wav received.wav
```

`--recorded-audio` and `--stop-bot` may also be supplied. All scenarios, options
and the output destination are validated before connection. Multiple scenarios
are rejected for this flag. Hosted `suite` output capture is not implemented.
The endpoint must permit capture and its application must generate PCM/TTS;
this flag does not install a speech provider. The original result JSON remains
on stdout. Artifact status/errors are printed on stderr.

`BotAudioRun::to_wav()` returns `None` for empty capture, otherwise a PCM16 mono
WAV. It revalidates all public chunks before allocating a concatenation: at most
16384 chunks and 16 MiB PCM, complete nonempty samples, one common rate among
8/16/24/48 kHz. Mixed rates are rejected. The CLI uses the default capture limit
of 8 MiB/4096 chunks. Concatenation preserves arrival order and omits timing gaps;
it is not a timeline recording. The captured bytes, concatenation and encoded
WAV may coexist (up to three PCM-sized buffers plus the WAV header).

Empty capture produces no final file and explicitly reports this on stderr.
Failed or cancelled evaluations can still export their already received valid
prefix; an otherwise successful evaluation can also have incomplete speech.
The usual evaluation exit status is preserved (0 passed, 1 failed). Invalid
export data or artifact I/O failure returns exit 2 after printing the unchanged
evaluation result, so its original failure reason remains available.

The destination's parent must already exist. Existing destinations, including
symlinks, are rejected. Preparation creates a private mode-0600 (subject to umask)
empty temporary file in the canonical parent before connecting. After the session
has closed its owners, bounded WAV writing flushes and synchronizes that file,
then publishes through a same-directory hard link that cannot replace a target
created meanwhile. The owned temporary name is removed and the parent directory
is synchronized. A post-publication failure explicitly reports that the file was
created; the final file is not deleted as rollback. Normal unwinding removes only
a temporary name with the recorded device/inode. An identification failure after
creation reports the retained temporary path; a crash, forced kill, directory
replacement or cleanup I/O failure may also leave a temporary file.

This is a trusted-local path facility, not a filesystem sandbox. Parent symlinks
are resolved, but concurrent hostile directory replacement is not secured. No
existing file permissions are changed and no output directory is created. Other
platforms reject this explicit publication mode before connection.

The pinned Python `EvalClientRecorder` can align user/bot tracks by timestamps,
insert gaps, resample/mix and overwrite a destination while creating parents.
This narrower native export intentionally implements none of those operations;
it rejects mixed-rate chunks instead of using the last rate for the whole track.
Local qualification checks generated PCM and real CLI/host WebSocket traffic,
not intelligibility, STT, physical playback or audio-response scoring.
