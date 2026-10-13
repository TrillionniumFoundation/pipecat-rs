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

## Explicit chunk-v2 shared output

`--playback-protocol chunk-v2` opts into the shared `OutputTransport` and
`SpokenHistory` boundary. The default remains `legacy-v1`; `--smoke` continues to
exercise that legacy protocol. A v1 context `playback-complete` cannot unlock a
v2 PCM chunk. The existing generated browser client does not negotiate or send
these receipts and must not be described as v2-enabled. Independent video stays
unsupported: this backend does not opt into `with_live_video`.

Before loading VAD, starting providers or constructing their pipeline, the
server sends a protobuf MessageFrame containing:

```json
{"type":"playback-v2-offer","version":2,"session_id":"32-hex-digits","output_rate":24000,"channels":1,"encoding":"pcm_s16le","max_pcm_bytes":49152}
```

The client must reply within the fixed handshake deadline:

```json
{"type":"playback-v2-accept","version":2,"session_id":"the-exact-session-id","chunk_receipts":true,"cutoff_receipts":true,"flush_on_disconnect":true}
```

Acceptance promises all of the following client behavior: never replay a chunk;
report completion only after the entire identified PCM reaches the client's
playback boundary; perform the selective/all flush below; stop and discard all
queued/active session audio on disconnect. This is an application contract with
the playback client, not a server-observed physical-speaker guarantee.

Output remains binary protobuf `AudioRawFrame`. Its `id` is the monotonic chunk
ID, and its `name` is a JSON descriptor with `version:2`, `session_id`, `epoch`,
`chunk_id`, `receipt_nonce`, `pcm_bytes` and `interruptible`. The audio field is
actual PCM16LE. The descriptor binds the exact transformed chunk, including tail
padding, rather than a TTS context or original provider frame. All numeric IDs
are bounded to JavaScript's exact integer range, 1 through 2^53-1. Session IDs
and nonces are 128-bit OS randomness; entropy failure fails closed.

After the whole chunk completes, the client echoes its exact identity:

```json
{"type":"chunk-complete","version":2,"session_id":"...","epoch":1,"chunk_id":1,"receipt_nonce":"..."}
```

Only an existing registered chunk, successful local network-queue admission and
that exact client report together produce `Played`. Queue admission is not
socket-write completion. An exact report racing local enqueue resumption is
held until enqueue succeeds; a failed/revoked enqueue cannot be promoted.
Unknown, future, duplicate, wrong-session, wrong-epoch and wrong-nonce reports
cannot unlock another chunk or extend any deadline.

A server `playback-cutoff` control contains `version:2`, `session_id`, `epoch`,
`cutoff_id`, `receipt_nonce` and `all`. The client stops/discards every earlier-
epoch interruptible chunk when `all:false`; it preserves uninterruptible chunks
and their existing exact receipt identities. When `all:true`, it stops/discards
all earlier-epoch audio. It permanently rejects affected late old-epoch packets.
Only after performing that flush may it return `cutoff-complete` with all the
same fields. A selective-to-all upgrade has a fresh cutoff ID and nonce; the old
selective report cannot satisfy the stronger request. An all-audio cutoff also
sets a permanent rejected-epoch floor: newly registered uninterruptible media
cannot reuse such an old epoch while a media gate still delays advancement.

Dropping an unfinished backend playback future reliably signals the existing
session owner to perform a cutoff. Dropping `SessionSender::send` or deleting a
receipt does not revoke admitted PCM. New interruptible audio is barred until
both the exact client cutoff and two ordered private control markers through
the real shared media queue complete. The first marker only observes; only the
latest second marker can reopen admission after another media-worker interruption
check. These internal controls never travel over the wire. Old queued interruptible calls are discarded,
not relabeled with a new epoch. Already registered uninterruptible receipts
remain valid under their original tuple. The v2 input path explicitly inserts a
core Interruption before a nonempty text or VAD-start frame; subsequent
aggregator-generated interruptions are independently fenced.

`playback-v2-context-committed` reports a monotonically increasing
`committed_contexts` count only after the real shared history ledger has committed
whole-context playback. It is an observation, not an additional client ACK.
The server uses the existing assistant history adapter; no parallel legacy
playback output is installed.

`end` closes ordinary input immediately while existing receipts and cutoff
management continue under one fixed drain deadline. End has one reserved ordered
slot even when ordinary input capacity is full. `cancel`, disconnection,
backpressure exhaustion and any deadline failure retire the session and release
pending waiters. Ordinary input after End is an error. Invalid receipts and
repeated End never extend drain. One owned bounded input pump keeps receipt
reception independent of pipeline input backpressure; no per-frame tasks are
spawned. The PCM ledger permits one pending chunk (48 KiB maximum); transport
packets are 64 KiB maximum, and network-originated normal input retains at most eight frames / 256
KiB including in-flight admission, plus one reserved ordered End flag. Transport
queues and the shared output queue have separate finite budgets. Socket cutoff
also discards old interruptible input still queued in the input pump or awaiting
core admission before admitting a new interruption. This does not cancel an
already-admitted core System callback: wire InputAudio/InputText retain their
core System-lane semantics, and a held System callback delays later System
work until it returns or the pipeline terminates.

Public test/application entry points are `ReceiptSession::negotiate`, `backend`,
`run`, and `close`, plus `ReceiptOutput::new` to delegate to real shared output
while publishing the precise queue-cutoff marker. `ReceiptConfig` exposes
nonzero validated handshake/chunk/enqueue/cutoff/drain durations (at most 60s);
defaults are 5s/5s/2s/5s/15s. Tests can inject short durations without changing the
absolute-deadline semantics. Normal cooperating run/close paths explicitly await core and transport
completion. The End deadline bounds media/receipt drain and final-notice queue
admission. Subsequent transport finish/cancel uses the existing SessionConfig
per-write and close timeouts; total socket closure can exceed drain_timeout.
The close future remains owned and awaited rather than being dropped to make a
wall-clock deadline appear satisfied.

Failure cleanup has one additional fixed drain_timeout budget. If an
uncooperative pipeline exceeds it, run returns “v2 pipeline cleanup deadline
exceeded; join is unverified” after cancelling the transport; core completion is
not claimed. Emergency outer-future Drop likewise fails closed without proving
an async client flush or task join. Ctrl+C still exits serve if cleanup fails;
a typed shutdown error preserves that failure rather than a successful report.

The CLI configures its transport's 64 KiB packet and eight-packet queue limits
before acceptance. The public negotiate API receives an already-created
WebSocketSession and cannot shrink that transport's existing receive buffers;
callers must configure their own packet/queue limits. Its post-receive check is
not a retrospective allocation cap.

Qualification is limited to real loopback protocol/PCM, shared pipeline/history,
client-reported completion, cancellation and ownership. It does not establish
browser audio callbacks, live providers, physical speaker output or video I/O.

`ReceiptRunConfig.initial_frames` is a trusted native startup API. Its frame
count is bounded, but arbitrary caller-supplied metadata, native continuations
or other startup payload allocations are outside the network ingress byte
budget. The configured CLI supplies only a payload-free `LlmRun`; this API is
not an alternative untrusted client input channel.
