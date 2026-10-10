# Native voice agent

`pipecat-voice-agent` is a Rust executable corresponding to the provider and
conversation pipeline in `examples/getting-started/06-voice-agent.py`:

```
WebSocket protobuf input
  → DeepgramSttProcessor
  → LLMUserAggregator
  → LlmProcessor (OpenAI streaming HTTP)
  → CartesiaTtsProcessor
  → WebSocketPlaybackOutput
  → LLMAssistantAggregator (playback-confirmed text only)
```

## Run

From the `rust` workspace with the workspace-required Rust toolchain:

```sh
cargo run -p pipecat-cli --bin pipecat-voice-agent -- --help
cargo run -p pipecat-cli --bin pipecat-voice-agent -- --smoke
cargo test -p pipecat-cli
```

The smoke run starts loopback WebSocket STT/TTS providers, a loopback HTTP SSE
LLM provider and a loopback client. It sends actual protobuf PCM bytes through
the full pipeline, validates the provider request bodies and output PCM, sends
an explicit simulated playback acknowledgement, then verifies both committed
conversation messages. It uses no credentials or paid API calls. Its synthetic
PCM and transcript do not establish recognition, synthesis, microphone,
physical-speaker or provider-live quality.

For live providers, export `DEEPGRAM_API_KEY`, `OPENAI_API_KEY`, and
`CARTESIA_API_KEY` using your normal secret manager, then run:

```sh
cargo run -p pipecat-cli --bin pipecat-voice-agent -- --bind 127.0.0.1:7860
```

The executable reads existing environment variables, does not read `.env`,
print keys, save credentials, or accept keys in command-line arguments. An
introduction is generated on connection unless `--no-intro` is supplied. Live
provider requests may incur charges on the configured accounts.

Optional variables:

| Variable | Default |
| --- | --- |
| `OPENAI_MODEL` | `gpt-4.1-mini` |
| `CARTESIA_VOICE_ID` | `86e30c1d-714b-4074-a1f2-1cb6b552fb49` |
| `PIPECAT_SYSTEM_PROMPT` | Brief, speakable voice-conversation responses |
| `PIPECAT_INPUT_SAMPLE_RATE` | `16000` |
| `PIPECAT_OUTPUT_SAMPLE_RATE` | `24000` |

Use `--once` to exit after one session. Each connection gets a fresh context and
provider session. The server handles one client at a time. `--idle-timeout-secs`
sets client inactivity shutdown; unacknowledged playback is bounded to 120
seconds, 16 contexts and 1 MiB of text. Ctrl+C cancels the active pipeline and
closes its sockets. Errors are reported without exposing provider response
bodies, URLs, prompts or credentials.

## Client protocol

Use `src/pipecat/frames/frames.proto`, represented by
`pipecat_protocol::protobuf`. Audio packets are binary WebSocket messages
containing one protobuf `Frame.AudioRawFrame`, signed little-endian PCM16,
mono, exactly matching the configured input sample rate. Output uses the same
protobuf audio shape at the configured output rate. The sample rates in each
packet are validated; no resampling is silently implied.

`TextFrame` input represents direct user text. Client-supplied
`TranscriptionFrame` input is rejected; provider transcripts are produced by
STT. Control messages can be JSON WebSocket text or JSON inside a protobuf
`MessageFrame`. Output control messages always use protobuf `MessageFrame`.

Client controls:

```json
{"type":"user-started-speaking"}
{"type":"user-stopped-speaking","duration_secs":0.2}
{"type":"interrupt"}
{"type":"playback-complete","context_id":"the-server-provided-id"}
{"type":"end"}
```

The two speaking events report actual client VAD decisions. Without them, a
final transcript starts a turn and the explicit transcription silence policy
ends it after `--turn-timeout-ms` (default 600). This is a timeout heuristic;
there is no embedded Silero or Smart Turn model. When supplying VAD events,
always send the corresponding stop event; silence cannot be inferred while a
client continues to declare speech active.

Output ordering:

1. `playback-start` supplies the context ID, sample rate, mono channel count and
   `pcm_s16le` encoding. This announces an audio stream; it does not assert that
   a physical speaker has started
2. Zero or more protobuf audio packets for that context
3. `playback-ready` indicates all synthesis for that context was delivered
4. The client drains its playback queue and only then sends
   `playback-complete` for the context ID
5. `playback-acknowledged` confirms acceptance of that acknowledgement

The client must flush queued audio on protobuf `InterruptionFrame` and discard
its interrupted context. Never acknowledge cancelled or only partly played
contexts. Duplicate, premature, stale and out-of-order acknowledgements do not
commit assistant text. An interruption invalidates pending acknowledgements.
This example only supports complete-context playback acknowledgement;
partially played words are conservatively omitted from assistant history.

Assistant text and its synthesis context are buffered at the output boundary
until a matching explicit playback acknowledgement. The output validates
context ID, generation, completion and playback order, then calls the assistant
aggregator's synchronous `confirm_playback` while holding the output state lock.
History is committed atomically before `playback-acknowledged` is sent. An
interrupt cannot overtake confirmed text waiting in a normal-priority queue.
Input interrupts immediately invalidate pending playback before asynchronous
pipeline propagation. `BotStoppedSpeaking` is only a state signal. Provider
`TtsStopped`, WebSocket enqueue and WebSocket write completion never by
themselves establish audible playback.

For applications forwarding already-confirmed text through normal queues,
`pipecat-turns` also supports `TtsText` with `metadata["spoken"] = true` followed
by a normal-priority `Flush` with origin `assistant_turn_end`. This executable
uses atomic confirmation to avoid the system-frame interruption race.

`run_session_with_input_processors` accepts per-session native processors
before STT, allowing an external model-backed VAD implementation to emit VAD
frames. Its default input path remains explicit client VAD or transcript timeout.

## Security and deployment boundary

The listener accepts loopback bind addresses only. Browser handshakes are
restricted to HTTP(S) origins with a localhost/loopback host. External origins,
`Origin: null`, duplicate origins and origins containing URL paths, queries,
credentials or fragments are rejected. Native clients without Origin are
allowed. Origin validation is not identity authentication.

This example has no user authentication, TLS termination, browser microphone
capture or authorization gateway. Do not
expose it directly to untrusted networks: a connected client can send text and
audio and incur provider charges. Deploy an authenticated TLS reverse proxy
and apply per-client quotas before offering remote access through that proxy. Playback acks
are assertions by that authenticated playback client, not acoustic proof.

## Python source migration map

| Python source | Native implementation |
| --- | --- |
| `examples/getting-started/06-voice-agent.py` | `src/lib.rs`, `src/main.rs` |
| `services/deepgram/stt.py` | `pipecat-streaming-speech::DeepgramSttProcessor` |
| `services/openai/llm.py` | `pipecat-services::LlmProcessor`, OpenAI adapter |
| `services/cartesia/tts.py` | `pipecat-streaming-speech::CartesiaTtsProcessor` |
| `processors/aggregators/llm_response_universal.py` | `pipecat-turns` pair |
| `frames/frames.proto` | `pipecat-protocol::protobuf`, `src/wire.rs` |
| Transport output / spoken context boundary | `src/playback.rs` |
| `PipelineWorker` lifecycle | `pipecat-core::PipelineWorker`, session owner |

This executable implements the WebSocket route and named providers. Daily,
LiveKit, browser WebRTC, telephony gateways, the Python runner's HTTP routes,
RTVI handshake compatibility and local media devices need separate adapters.
It is runnable native functionality, not a claim of full Python repository
feature parity.

## Optional native Silero VAD

Pass both `--vad-model /absolute/path/silero_vad.onnx` and
`--onnx-runtime /absolute/path/libonnxruntime.so` to run model-backed input VAD
before streaming STT. The model is available in the reference repository at
`src/pipecat/audio/vad/data/silero_vad.onnx`; model/license and native-library
requirements are documented in `pipecat-vad`. Input must be 8 or 16 kHz mono
PCM16LE. Loading is off the async executor and each session owns recurrent state.
No Python interpreter, implicit model download or native-library installer runs.

`smoke::run_with_vad` and the CLI integration test load the real Silero model and
process a full native inference window inside the synthetic-provider voice pipeline.
The test requires an explicit `ORT_DYLIB_PATH` outside the documented dot-host
runtime location. STT/LLM/TTS replies and remote playback acknowledgement remain
synthetic; this does not establish microphone, speaker or live-provider acceptance.
