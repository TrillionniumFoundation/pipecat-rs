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
realtime model. The built-in `EvalBotHost` accepts this input only when explicitly constructed
with `.with_raw_audio_input(true)`; its default and CLI scenario parser remain
text-only. The session constructor does not enable host capabilities. The
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

## Built-in host input opt-in

`EvalBotHost::bind(address, config).await?.with_raw_audio_input(true)` admits
single JSON `raw-audio` commands after readiness. It validates 16 kHz, mono,
PCM16/base64 before handing them to the existing `RtviProcessor`, which emits
`InputAudio` into the application's factory pipeline. A factory must provide its
own STT/VAD/realtime processing for human speech. There is no automatically
selected provider, model, credential, or resampler. `raw-audio-batch`, binary WS
packets, other rates/channels and images remain unsupported. Setting the builder
to false restores the default behavior; no public configuration fields changed.

Only the enabled host advertises `raw_audio_input: true`, `text_only: false` and
`audio_output: false`. The default capability JSON is unchanged. All host modes
still configure `skip_tts: true` before initialization finishes and produce text
events, not audio responses or playback acknowledgments. The first admitted audio
also closes subsequent context/observer-configuration admission, including silence.
Configure these controls before starting the recorded session's silence sender.

Audio obeys the existing wire byte/message, queue, session and shutdown budgets.
Decoded frames additionally obey native per-frame serialized envelope and
cumulative processor-boundary budgets. A legal base64 wire message may exceed the
numeric-array frame budget; that closes the session with an error rather than
truncating PCM. Frame bytes are counted again at every application boundary.
Normal End, Cancel, disconnect and fresh-session lifecycle use the existing host
ownership path. The opt-in does not promise delivery after cancellation.

`tests/host_recorded_audio.rs` connects the actual recorded-session client to the
actual host, with a deterministic PCM recognizer and native assistant observation.
It separately checks default compatibility, readiness and format rejection,
context/config ordering and decoded-frame budgets. This is local transport and
pipeline qualification, not a live speech-recognition quality result.
