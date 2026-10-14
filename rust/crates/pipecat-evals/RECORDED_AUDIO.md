# Recorded user audio input (library opt-in)

`EvalSession::with_recorded_audio_inputs` closes the recording-to-RTVI evaluation
client path without changing `ScriptTurn`, YAML parsing, or default text sessions.
A recording replaces `send-text` for its indexed user turn. The scenario's `user`
text is only a reference for the optional judge; it is never injected as a
transcript into the bot. Existing final `user_transcription` and `llm_response`
assertions evaluate actual endpoint events.

```rust,no_run
use pipecat_evals::{Cancellation, EvalSession, RecordedAudio, ScenarioFile, SessionOptions};
# async fn run(wav: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
let scenario = ScenarioFile::from_json(r#"{
  "name":"recording", "turns":[{"user":"reference transcript", "expect":[
    {"event":"user_transcription", "text_contains":"hello"}
  ]}]}"#)?.scenarios.remove(0);
let clip = RecordedAudio::from_wav(wav)?;
let result = EvalSession::new(scenario, "ws://127.0.0.1:7860", SessionOptions::default())?
    .with_recorded_audio_inputs([(0, clip)])?
    .run(Cancellation::new()).await;
assert!(result.passed, "{:?}", result.failures);
# Ok(()) }
```

The endpoint must admit RTVI `raw-audio` and route it through its own STT/VAD or
realtime model. **The bundled `EvalBotHost` and CLI scenario parser remain
text-only.** The constructor does not grant that host audio capability. The
local integration fixture demonstrates a genuine WebSocket → `RtviProcessor` →
PCM recognition fixture → native `RtviObserver` → evaluation assertion path. It
recognizes fixed sample bytes, not human speech; no live STT/provider/browser or
microphone qualification is implied.

## Bounds and wire

- `RecordedAudio::from_pcm16` accepts nonempty 16 kHz mono PCM16LE; `from_wav`
  reuses the strict native format-code-1 PCM16 RIFF/WAVE reader and requires the
  same rate/channel count. Other rates, stereo, compressed WAVE, MP3/OGG/FLAC and
  synthesized user TTS are not enabled by this API.
- Each recording is at most 60 seconds / 1,920,000 PCM bytes. WAV container bytes
  are separately capped at that value plus 65,536. A session accepts at most
  128 distinct turn indices and 15,360,000 aggregate PCM bytes. Invalid indices,
  duplicates, missing/empty user references and DTMF combinations fail before
  connecting. Loading local files is the application's responsibility.
- After readiness and initial controls, one owned sender emits mono 1,280-byte
  (40 ms) frames using existing RTVI JSON `raw-audio` fields `base64Audio`,
  `sampleRate`, `numChannels`. Partial tails are zero-padded; idle ticks carry
  silence. Audio IDs use an `audio-` prefix separate from text/control IDs. Late
  scheduling does not send catch-up bursts.
- At most one clip is active and one queued. Audio output, including silence,
  separately obeys `SessionOptions.max_wire_messages` and `max_wire_bytes`, plus
  transport per-message/write/queue bounds. This outgoing audio budget is not a
  combined incoming-and-outgoing counter; incoming accounting is unchanged.
- Playback runs concurrently with receiving events and judges. Expectation
  deadlines start immediately after queue admission, not after playback. A
  successful turn also waits for its padded clip to enter the bounded transport,
  while still receiving events. This completion requirement is a native
  strengthening, not a remote playback or transcription acknowledgment.
- The existing outer session deadline/cancellation cover playback. Normal exit
  stops and actually joins the sender before finishing the socket. Task errors,
  including idle-output errors, fail the run. Cancel discards unsent samples;
  previously admitted writes may drain during socket shutdown. Dropping the run
  aborts both owners rather than detaching a free-running silence task.

The fixed Python source additionally supports broader file decoding, downmixing
and resampling. This slice pins raw-audio framing, 40 ms pacing, tail padding,
idle silence and queue-before-assertions. Its AST oracle uses controlled
collaborators; actual networking and ownership are separately tested. Fresh
sessions own fresh recording state. Native limits reject rather than truncate.
