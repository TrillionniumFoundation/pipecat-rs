# Hepta HMI media adapter

This crate provides bounded HMI presentation state and optional actual local
capture/VAD/STT/TTS/output. It does not call an LLM, execute tools, own an App
Server conversation, grant device permissions, or create a conversation database.
The existing Hepta owner authenticates the principal/device/session binding,
admits turns, retains request identity and reconciles external tool effects.

`HmiSession` binds principal, device/organ generations, logical session, turn,
utterance, monotonic sequence and output epoch. It rejects foreign/stale output
and count pressure without evicting live records. Local barge-in retires the epoch
before invoking `MediaControl`; even local teardown failure returns the request
to interrupt the existing Hepta turn. An interrupt is not a tool rollback.
Successful callback submission is a releasable terminal submission observation,
never proof of playback; later interruptions do not rewrite that historical fact.
The presentation ledger is bounded, process-local state, not durable request
deduplication. A recovered owner must establish a fresh incarnation; do not replay
old media or infer remote operation success from these records.

`local-speech` explicitly enables native Whisper CPU, Silero, Piper ONNX/eSpeak
and CPAL. The eSpeak feature loads GPL native code and requires caller-supplied
trusted artifacts; no models, libraries or device grants are bundled. Read the
Piper and Whisper provenance records. `LocalSpeech::load` hashes the Whisper and
Silero artifacts; Piper verifies its own manifest. Paths must remain immutable.
Native loading/device operations can block, so the application must host this
owner in its supervised media process rather than the CNS event loop.

Call `start_input`, continuously drain `RecognitionStream::recv`, and forward
only `FinalTranscript` into the existing Hepta thread. `SpeechStarted` can trigger
local barge-in; it does not authenticate a speaker. Whisper supplies final
transcripts, not streaming partial hypotheses. Input uses the existing bounded
16 kHz mono VAD/STT pipeline, with raw-audio passthrough disabled.

`LocalSpeechConfig` receives the exact stream identity, turn and current epoch
from the existing Hepta owner; `HmiSession::with_epoch` attaches to that same
epoch. For a subsequent admitted turn, call `attach_turn` with the exact owner
epoch, then `presentation().admit_turn`. Local barge-in raises a retirement floor
and closes output admission until a Hepta turn is attached; it does not authorize
a new utterance. A local stop and Hepta's next epoch can coincide; lower owner
epochs cannot restore old output.

For output, call `presentation().open_output()`, `synthesize(identity, text)` and
`submit(prepared)`. Prepared audio retains the complete output identity and its
creating media owner. It cannot be relabeled or submitted through another device.
The first device-write poll and interrupt share an admission fence, so a delayed
old synthesis cannot enqueue after retirement. CPAL already-copied buffers may
still sound. `SubmissionReceipt` means callback submission only. This API never
creates Played/Heard or device-observed-playback claims. Record unknown completion
honestly and retain it before releasing terminal ledger entries.

Shutdown recognition through its explicit `shutdown()` before shutting down the
engine owner. Drain its events during normal operation; a stopped consumer is an
explicit backpressure/failure case. Input errors and total error counters remain
available rather than being replaced with an empty successful session summary.
Raw `FinalTranscript` events are not yet a Hepta memory observation: the caller
must bind the captured asset digest/span and authenticated source scope before
handing the text to Hepta. No audio provenance is fabricated by this adapter.

```sh
cd rust
cargo test -p pipecat-hepta --locked
cargo check -p pipecat-hepta --features local-speech --all-targets --locked
cargo clippy -p pipecat-hepta --all-targets --all-features --locked -- -D warnings
cargo run -p pipecat-hepta --features local-speech --example local_speech -- /absolute/path/local-speech.json
```

The runnable example opens real capture/output and uses bounded stdin/stdout
JSONL. Its configuration is a serialized `LocalSpeechConfig` containing an
existing Piper manifest, hashed Whisper/Silero artifacts, device IDs and the
exact stream/turn/epoch already assigned by Hepta. There is no automatic device,
model or cloud-provider substitution after an error.

Commands are `{"type":"turn","scope":...}` and
`{"type":"speak","scope":...,"text":"..."}`, plus `interrupt` and
`shutdown`. Scope is the existing Hepta bridge's camelCase `scope` object,
including `connectionId`, `outputEpoch`, `turnId` and `utteranceId`; null
turn/utterance rejects audio. A complete scope alone is not authorization: the
trusted owner must admit `turnStarted` and check its current `permits_output`
boundary before forwarding output. An Unknown terminal can retain nonnull IDs
and must never be treated as an admission. Connection IDs are native u64 values;
the host must recreate the media owner on connection-incarnation changes.

This initial endpoint admits one `speak` per input `utteranceSequence`. Hepta
`textFinal` is per output item, so a turn with several AgentMessage items still
needs bounded owner-side aggregation or a separate segment identity protocol.
Do not pipe every `textFinal` into this endpoint or silently discard later items.
Token deltas are display-only. The example enforces session/generation/turn/epoch,
sequence high-water and presentation transitions at this command boundary.

Input events include `unbound_transcript`; they must not be piped directly to
trusted user submission. The host must retain/bind the source media first.
`turn_interrupt` requests must be relayed to the existing Hepta turn owner while
tool outcomes continue to reconcile there. The owner-process multiplexer and
source-asset writer are not supplied by this media endpoint. Each wire line is
limited to 64 KiB. Malformed input, output backpressure or a stalled/closed event
receiver retires old output before capture/native cleanup.
The example explicitly bounds Tokio teardown because its stdin OS read is not
interruptible; native model/device owners are shut down first. This does not
make a stalled native library or device operation hard-real-time bounded.

The pure state tests are authored regression cases, not native-device acceptance.
Physical microphones/speakers, Chinese speech, noise/echo, unplug/replug, resource
curves and an actual App Server/model session must be verified separately on the
configured project environment. An ALSA null endpoint does not satisfy them.
