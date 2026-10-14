# Native browser runner

`pipecat-browser-agent` hosts the Rust/WASM client and a native voice pipeline:

RTVI 2.2 + protobuf WebSocket → stateful PCM resampler → optional native Silero
VAD → Deepgram STT (or explicit local Whisper) → user-turn aggregation → OpenAI LLM → generated-text
context → Cartesia TTS → protobuf audio.

No Python process or JavaScript server is involved. The existing
`pipecat-voice-agent` CLI and its playback-confirmed control protocol remain a
separate entry point.

## Run

From `rust/`, build the browser bundle with `bash client-templates/build.sh`, set
`DEEPGRAM_API_KEY`, `OPENAI_API_KEY`, and `CARTESIA_API_KEY` in the process
environment, then run:

```sh
cargo run -p pipecat-runner --bin pipecat-browser-agent -- \
  --assets client-templates/dist
```

Open the exact loopback URL printed by the process. `--assets` serves only the
five allowlisted browser build files and replaces their public `config.json`
in memory with a WebSocket-only configuration. Provider credentials never enter
that configuration. Without `--assets`, HTTP lifecycle routes still work.

For microphone-free WebSocket text input, explicitly opt in:

```sh
cargo run -p pipecat-runner --bin pipecat-browser-agent -- \
  --assets client-templates/dist --text-input --no-intro
```

`--text-input` requires `--assets`. Add `--webrtc` to a build with the `webrtc`
feature for microphone-free receive-only audio; without that feature the flag is
rejected before credential lookup or listener creation, in either flag order. The host replaces the supplied
`config.json` completely in memory: CLI selection wins, and asset configuration
is never merged. The flag publishes `input_mode: "text"` with WebSocket, or WebSocket plus
SmallWebRTC when `--webrtc` is selected.
Without it the existing voice configuration and transport choices remain unchanged,
even if the supplied file asks for text mode. Build files are never rewritten.

This wires the browser's existing microphone-free policy into the standard
native host. Custom static/proxy hosting may still configure `input_mode` directly.
Text mode retains bot audio replies and generated-text history. Provider/STT
initialization and required credentials are unchanged; it neither disables
server audio admission nor promises fewer provider connections or lower cost.
Text-mode RTC uses a recvonly audio transceiver with no local microphone track.
The RTVI data channel and bot audio replies remain active. Browser image/receipt
bridges and actual browser permission/capture/autoplay validation remain open.
Real native receive-only peer and injected-provider tests are separate from the
actual CLI child HTTP configuration witness; neither is browser execution.

`--help` reads no credentials. `--bind` selects a literal loopback address;
`--no-intro` waits for user speech. Optional Silero requires both `--vad-model`
and `--onnx-runtime` paths. Optional environment settings match the native CLI:
`OPENAI_MODEL`, `CARTESIA_VOICE_ID`, `PIPECAT_SYSTEM_PROMPT`,
`PIPECAT_INPUT_SAMPLE_RATE`, and `PIPECAT_OUTPUT_SAMPLE_RATE`.

Rust applications can call `pipecat_runner::run_from_env().await`, or construct
`NativeRunner` and pass it as a `BotApi` to
`pipecat_client_server::server::serve_with_api`. Applications own the HTTP
listener and should call `NativeRunner::shutdown` when their host shuts down.

## Lifecycle and wire contract

- `POST /api/start` with `{"transport":"websocket"}` returns `wsUrl`,
  `sessionId`, and `token:null`. The URL contains a random, one-use session path
  and points to an already-bound loopback listener
- The browser sends the normal RTVI `client-ready` envelope inside the original
  protobuf `MessageFrame`. `bot-ready` echoes its correlation ID and reports
  protocol version `2.2.0`
- Original PCM16 `AudioRawFrame` messages carry their actual capture sample
  rate. Mono 8–96 kHz input is statefully resampled to the configured provider
  rate. Rate changes, malformed audio, oversized audio, and injected client
  transcriptions fail closed
- Standard RTVI `send-text`, `disconnect-bot`, raw audio, and other commands
  use the native RTVI processor. The host's `/api/sessions/{sessionId}/stop`
  POST extension cancels that session only
- The default runner supports WebSocket. The opt-in SmallWebRTC feature is
  described below. Daily, LiveKit, uploads, arbitrary start-body provider
  overrides, and unscoped `/api/offer` are not implemented

Each session gets new provider clients, turn aggregators, resampler state,
conversation context, and queues. Session count, request bytes, WebSocket
messages, output waits, connect/idle/session lifetimes, and transcript growth
are bounded. A shared 4 MiB cumulative textual-frame budget caps retained
context admission, including non-running text append requests; repeated frames
are counted conservatively. Upgrade requests require both the exact allowed Origin and exact
random session path; no provider connection starts before that validation.
Invalid handshakes do not consume a session's listener. Accepted listeners are
closed so their URL cannot reconnect or attach a second browser. Missing
Origin is denied unless explicitly enabled for native clients. Origin checks
are CSRF protection, not general-purpose user authentication. This is a local
host, not an authenticated remote deployment.

`/api/start` allows an absent or empty `body` object. It rejects unimplemented
body settings instead of pretending they configured a bot. Expired unused
starts release their listener and session slot. Shutdown requests cancellation,
joins session tasks, and closes listeners and client sockets. Concurrent shutdown
callers await the same cleanup owner, which survives cancellation of a caller.
Without a started shutdown, dropping the last runner handle aborts its owned task
set; explicit shutdown is preferred. Graceful End/Stop drains the input resampler
tail before STT finishes; Cancel discards that tail for both STT providers.

## Generated text is not spoken evidence

The runner explicitly advertises `contextMode: "generated-text"` and
`playbackAcknowledgements: false`. `bot-output` updates a stable segment from
LLM generation with `spoken:false`; it never invents completed spoken status,
BotStartedSpeaking, or BotStoppedSpeaking events. `will_be_spoken` describes the
requested synthesis, not successful playback. TTS start/stop events describe
synthesis. Assistant context records generated text and may include text whose
audio was interrupted, muted, never played, or lost after generation.

The browser can play the audio, but this runner does not negotiate a playback
acknowledgement extension. Do not use its context as proof that a person heard
an utterance or use synthesis completion as a spoken-flow barrier.

## Verification

```sh
cargo test --offline -p pipecat-runner -p pipecat-transports
cargo run --offline -p pipecat-runner --bin pipecat-browser-agent -- --help
```

Integration tests use real loopback HTTP and WebSockets, the actual browser
crate's encoders/decoders and transcript projection, and synthetic
Deepgram/OpenAI/Cartesia wire servers. They check exact readiness correlation,
48→16 kHz capture conversion, 24 kHz output, generated-text semantics, fresh
contexts across two sessions, and original-source fixed protocol fixtures.
Lifecycle tests cover origin/token refusal, admission, scoped cancellation,
unused-start expiry, and host shutdown. They use no paid calls or live
credentials.

These tests do not execute a browser, grant microphone access, or verify
physical speaker playback. Browser execution in the development environment
is blocked by AF_UNIX sandbox restrictions and the cloud browser's separate
loopback network; no browser or audio-device QA result is claimed.

## Optional SmallWebRTC voice sessions

Enable the native peer feature explicitly:

```sh
cargo run -p pipecat-runner --features webrtc --bin pipecat-browser-agent -- \
  --webrtc --assets client-templates/dist
```

The default feature set and `NativeRunner::new` remain WebSocket-only, including
when another crate enables the Cargo feature. `--webrtc` or
`NativeRunner::new_with_webrtc(config, providers, WebRtcRunnerConfig::default())`
opts a host into both transports. Browser configuration then advertises
`websocket` and `smallwebrtc`.

External Cargo workspaces can enable the feature through their path dependency.
Retain the repository's `rust/vendor/webrtc-0.14.0` directory: `pipecat-webrtc`
depends directly on that audited source, including its native shutdown fixes.
See the vendor directory's `PATCHES.md` for the retained SCTP cleanup behavior.

### Signaling and ownership

The browser's source-compatible start body is accepted:

```json
{"transport":"webrtc","createDailyRoom":false,"enableDefaultIceServers":true}
```

`smallwebrtc` is also accepted as an alias. The returned `sessionId` is a
244-bit random bearer capability. Treat it as a secret and avoid logging session
URLs. Only possession of that capability and an accepted Origin authorizes the
session's HTTP operations; the answer's `pc_id` is never a credential.

- `POST /api/sessions/{sessionId}/api/offer` accepts the original
  `{"sdp":"...","type":"offer"}` body and returns `sdp`, `type:"answer"`,
  and `pc_id`
- `PATCH` to the same route accepts `{"pc_id":"...","candidates":[...]}`
  with the native SmallWebRTC candidate fields and returns `{"status":"success"}`
- Both routes use the host's exact Origin checks and request bounds. A peer
  from another session cannot be patched through this capability. The existing
  session-scoped `stop` POST also works
- A session owns one peer and accepts one successful offer. Malformed offers
  may be retried before its original connection deadline. Renegotiation,
  `restart_pc`, and peer replacement require starting a new session
- HTTP callers enqueue signaling into a bounded four-command mailbox owned by
  the session. Dropping a request does not detach negotiation. If its caller is
  gone when an answer is delivered, the peer closes; an unconnected or unused
  start expires
- WebSocket and WebRTC share one admission limit. Stop, deadlines, protocol
  failure, disconnect, and shutdown release the peer and admission slot

This loopback runner explicitly returns `iceConfig:{"iceServers":[]}` and
`icePolicy:"host-only"`. The browser's advisory default-ICE hint never enables a
public STUN service here. `createDailyRoom:true`, arbitrary provider overrides,
uploads, unscoped `/api/offer`, and external STUN/TURN configuration are rejected
or unavailable. The runner does not supply user accounts or remote-deployment
authentication. Do not expose this host through a public forwarding service.

### Native media pipeline

The server answers using the actual native ICE/DTLS/SRTP/SCTP stack and Opus
codec. Decrypted incoming RTP is decoded to mono 48 kHz PCM, statefully resampled
to the configured STT rate, and passed through the same RTVI and provider pipeline
as WebSocket sessions. Provider connections begin after the peer and data channel
are connected. Early microphone RTP before `client-ready` is discarded.

RTVI uses plain JSON on the data channel, with a 16 KiB message limit. The
source browser’s raw `ping` heartbeat is accepted separately, without a pong.
Heartbeats do not count as user activity for the conversation idle deadline.
Native liveness uses ICE/DTLS state and the runner deadlines; the Python
connection’s three-second ping-age `is_connected()` heuristic is not reproduced.
Outgoing
provider PCM is statefully resampled to the negotiated codec's rate and split
into exact 20 ms packets. TTS completion flushes the resampler and zero-pads only
the final partial packet. Interruption discards both pending PCM/filter state and
queued peer audio. Native transport pacing determines packet transmission;
transmission and TTS completion are never playback acknowledgements. Opus is the
default; the programmatic configuration also permits native PCMU/PCMA.

```sh
cargo test --offline -p pipecat-runner --features webrtc
cargo clippy --offline -p pipecat-runner --features webrtc --all-targets -- -D warnings
```

The WebRTC integration test uses the actual browser crate's start body and offer
URL builder, real loopback peer negotiation and encrypted Opus media, and
synthetic provider servers. It checks incoming 48→16 kHz conversion, outgoing
24→48 kHz packetization, data-channel readiness/transcripts, cross-session
signaling refusal, deadline cleanup, and the generated-text-only context
contract. It does not execute a browser or validate devices, external ICE/NAT,
TURN relaying, or live provider service quality.

## Optional local Whisper STT

The `whisper` Cargo feature enables the native CPU `pipecat-whisper` service.
It is off by default and does not change the Deepgram/OpenAI/Cartesia path.
Enable local recognition explicitly in the host process:

```sh
export PIPECAT_STT_PROVIDER=whisper
export PIPECAT_WHISPER_MODEL=/absolute/path/ggml-tiny.en.bin
export PIPECAT_INPUT_SAMPLE_RATE=16000
export PIPECAT_WHISPER_THREADS=2
# Still required for the configured hosted LLM and TTS:
export OPENAI_API_KEY=...
export CARTESIA_API_KEY=...
cargo run -p pipecat-runner --features whisper --bin pipecat-browser-agent -- \
  --no-intro --assets client-templates/dist \
  --vad-model /absolute/path/silero_vad.onnx \
  --onnx-runtime /absolute/path/libonnxruntime.so
```

No Deepgram API key is read or required in Whisper mode, and no Deepgram client
is constructed. The supplied model must already be a local whisper.cpp GGML file.
Nothing downloads weights, launches Python, or falls back to a hosted STT service.
Missing features, invalid selections, incompatible rates, invalid models and
incomplete segmentation configuration fail closed. Supplying Whisper host options
without selecting `PIPECAT_STT_PROVIDER=whisper` is also an error. Credentials and
model paths are never sent to the browser. The `run_from_env` entry point is
unchanged, including for applications that embed the runner.

Local recognition does not make the full conversation offline: the default LLM
and TTS still send recognized text to OpenAI and Cartesia using the explicitly
supplied credentials. Synthetic loopback providers are used only by tests.

### Segmentation and configuration

- Default local segmentation is native Silero VAD. Both file paths above are
  required. Normalized audio reaches VAD and the Whisper speech buffer at 16 kHz,
  mono PCM16; native Whisper keeps pre-roll, speech boundaries and trailing silence
- `PIPECAT_WHISPER_PRE_ROLL_MS` configures VAD-only onset retention, defaults to
  `3000`, and accepts `0..5000`. Three seconds retain up to 96,000 PCM bytes per
  quiet buffer, 64,000 more than one second, and can add up to two seconds of
  lead-in audio to each inference. This protects delayed VAD onsets at the cost
  of additional memory and inference context. Submitted speech is cleared before
  the next pre-roll buffer; it is not replayed into the next segment. Pre-roll plus
  500 ms trailing silence must fit the native segment limit. Shorter pre-roll
  deliberately trades onset protection for less retained audio
- As an explicit alternative, set `PIPECAT_WHISPER_SEGMENT_MS` to an integer
  between `1000` and `30000`, and omit the VAD flags. This accumulates continuous
  browser packets into bounded nonoverlapping windows. It does not transcribe
  individual microphone packets or estimate speech using an energy heuristic.
  Windows advance by captured sample count, so paused capture cannot complete a
  partial window until more PCM or graceful End/Stop arrives. Fixed windows add
  up to a window of latency and can split words or utterances; they are intended
  for controlled capture, not equivalent to VAD turn detection
- `PIPECAT_WHISPER_LANGUAGE` defaults to `en`; an explicit language tag selects
  another supported language, while `auto` requests detection. English-only models
  reject a non-English language. Model/language quality parity is not claimed
- `PIPECAT_WHISPER_THREADS` defaults to `4` and accepts `1..256`. Every session owns
  a separate model and native worker, so size `RunnerConfig::max_sessions` and
  inference threads for available CPU and memory
- `PIPECAT_INPUT_SAMPLE_RATE` must be `16000` in local mode. Browser capture may
  still be mono 8–96 kHz; the shared stateful resampler converts the actual rate.
  The existing output-rate setting remains independent
- Rust callers set `RunnerConfig::stt = SttProvider::Whisper(LocalWhisperConfig)`.
  `LocalWhisperConfig::new(path)` selects VAD; its `model`, `settings` and
  `segmentation` and `pre_roll_duration` fields expose native limits, decoding
  settings and onset retention explicitly

Fixed-window buffers are bounded by the selected duration. STT mute discards the
current partial window, interruption preserves user recognition, graceful End/Stop
flushes the final partial window, and cancellation discards it. The native service's
bounded inference queue returns visible backpressure rather than growing without
limit. Invalid audio or provider failure closes the session.

The model loads only after the browser's transport admission checks. Loading and
inference are owned across cancellation, expiry, setup failure, disconnect and
shutdown. `NativeRunner::shutdown()` waits for all native model owners to exit,
even when the general session shutdown timeout has forced task cancellation.
Each session also joins its own native owners before releasing its admission slot
and publishing its completion report, without waiting on another active session.
Native decoding abort is cooperative; model loading cannot be interrupted inside
the C++ loader, so cancellation/join can exceed a network deadline. No native worker
is detached to make cancellation appear faster. `active_local_stt_workers()` counts
model owners through their join; `local_stt_fixed_windows_submitted()` counts
successful fixed-window admissions, not completed transcripts or VAD segments.

The `whisper` and `webrtc` features can be combined. Both transports use the same
local STT pipeline. The model-backed runner checks below exercise the browser's
WebSocket codec; model-backed WebRTC and physical browser microphone/speaker tests
are not claimed.

### Explicit model-backed checks

Ordinary tests require no model assets and download nothing. All end-to-end recognition checks
are ignored unless explicitly selected:

```sh
cargo test --offline -p pipecat-runner --features whisper
cargo clippy --offline -p pipecat-runner --features whisper,webrtc --all-targets -- -D warnings

export PIPECAT_WHISPER_MODEL=/absolute/path/ggml-tiny.en.bin
export PIPECAT_WHISPER_FIXTURE=/absolute/path/jfk.wav
export PIPECAT_WHISPER_VAD_MODEL=/absolute/path/silero_vad.onnx
export PIPECAT_WHISPER_ONNX_RUNTIME=/absolute/path/libonnxruntime.so
cargo test --offline -p pipecat-runner --features whisper \
  --lib --test whisper_flow --test whisper_lifecycle -- --ignored --test-threads=1
```

The Whisper build requires a native C/C++ toolchain, CMake and libclang; set
`LIBCLANG_PATH` if discovery requires it. See
[pipecat-whisper's verified test assets](../pipecat-whisper/README.md#verified-external-test-assets)
for canonical model/fixture URLs, published digests, licenses and quality limits.
All recognition is native and real. Only LLM/TTS are synthetic, listen on loopback,
and use dummy credentials. Tests verify browser-codec speech reaches recognition,
recognized text reaches LLM requests, generated text reaches TTS, output PCM reaches
the browser codec, and cancellation/expiry releases joined native owners.

## Soniox hosted voice cascade (optional)

The `soniox` feature adds one explicit alternative composition:

RTVI/protobuf WebSocket → stateful capture resampler → Soniox STT → recommended
provider-endpoint user-turn strategies → native OpenAI `LlmProcessor` → optional
session-local Rust `ToolExecutor` → generated-text context → Soniox TTS →
protobuf audio. Native function results feed the same assistant aggregator and
HTTP model continuation; tool-only responses and arguments are not spoken text.

Set `SONIOX_API_KEY` and `OPENAI_API_KEY` in the native process environment, then:

```sh
PIPECAT_VOICE_CASCADE=soniox cargo run -p pipecat-runner --features soniox \
  --bin pipecat-browser-agent -- --no-intro --assets client-templates/dist
```

Without the optional browser assets, the same command hosts the loopback HTTP
lifecycle and WebSocket protocol for native clients. `--help` never reads keys.
No Deepgram or Cartesia credential is read in Soniox mode. Omitting
`PIPECAT_VOICE_CASCADE` preserves the existing default cascade and public
`NativeRunner::new(RunnerConfig, ProviderConfig)` constructor. Optional Soniox
configuration is `SONIOX_STT_MODEL`, `SONIOX_TTS_MODEL`, `SONIOX_VOICE`,
`OPENAI_MODEL`, `PIPECAT_SYSTEM_PROMPT`, `PIPECAT_INPUT_SAMPLE_RATE`, and
`PIPECAT_OUTPUT_SAMPLE_RATE`. Defaults come from the existing typed provider
adapters: 16 kHz STT and 24 kHz TTS. Soniox TTS supports 8, 16, 24, 44.1 and
48 kHz; declaring another output rate fails before accepting a session.

The pinned Python `voice-soniox.py` sample selects local Silero endpointing,
voice Nora, and spoken assistant aggregation after output. This native runner
intentionally selects provider endpointing, the existing native Soniox voice
default (overridable with `SONIOX_VOICE`), and generated-text history before
synthesis. It does not claim identical turn or spoken-history behavior.

This bounded composition uses Soniox's provider endpoint detection and its
recommended external start/stop strategies. Local VAD, Whisper settings,
`PIPECAT_STT_PROVIDER`, Cartesia voice options, and `--webrtc` fail explicitly
instead of being ignored. Other adapters and the original default cascade keep
their own turn policy. Soniox selection does not enable additional transports.
Provider endpoints can be overridden by a trusted Rust application through
`SonioxVoiceConfig`; browser requests cannot supply provider configuration.

A runnable native arithmetic-tool host uses the same runner and protocol:

```sh
cargo run -p pipecat-runner --features soniox --example soniox_voice
```

This example listens at `127.0.0.1:7860`, accepts a native client's
`POST /api/start` body `{"transport":"websocket"}`, and waits for speech after
RTVI `client-ready`. It intentionally permits missing Origin for loopback native
clients. It has no embedded microphone, browser assets, or playback device.
Only its pure local `add` callback executes; there are no external tool effects.

Rust callers create `SonioxVoiceConfig::new(soniox_key, openai_key)` or
`SonioxVoiceConfig::from_env()`, optionally attach a `SonioxToolsFactory`, then
call `NativeRunner::new_soniox`. The factory returns `SonioxSessionTools` with
existing `ToolDefinition` values and a newly constructed, bounded `ToolExecutor`.
It must be nonblocking and create new mutable callback state for every session.
A factory error or unwinding panic closes that session and releases its admission
slot. Rust's ordinary panic hook still runs; its output is controlled by the
application. This does not contain process aborts, blocking code or arbitrary
side effects. Tool definitions are limited to 32 and 64 KiB. The example uses two in-flight
calls, a 64-entry duplicate window, a 4 KiB argument bound, and the executor's
30-second default callback deadline. Cancellation cannot undo external effects
in custom callbacks; use cancellation-safe callbacks and explicit application
permissions for any such effects.

History remains explicitly `generated-text`, with `spoken:false` and no
playback acknowledgements. Transport admission and synthesis completion do not
establish audible playback. The existing Soniox receipt-ledger tests exercise a
separate trusted synthetic-playback contract; this runner does not implement
that mode or turn TTS timestamps into playback receipts.

The `soniox_config` and `soniox_cascade` suites use only synthetic local provider
payloads over real native HTTP/WebSockets. They exercise provider selection,
key isolation, tool continuation, two-turn history, fresh session state, capture
resampling, synthesis PCM, graceful termination, and blocked-startup ownership.
Live provider compatibility, browser automation, physical audibility, and the
full rewrite's release qualification remain separate.

Under an unread client socket and a provider audio flood, the existing bounded
Soniox per-stream event lane can fail closed before explicit shutdown. The native
fixture checks incomplete bounded provider progress, a failed session report,
freed admission, terminal socket closure and joined provider owners. It is not
a claim that an overflowing session remains usable or that queued audio played.

## Browser text input and incompatible CLI options

The Rust/WASM template exposes RTVI `send-text` over both existing browser
transports after matching `bot-ready`. Typed content follows the same native
RTVI interruption/flush, LLM user-context and TTS path; local browser queueing is
not a playback acknowledgement. Generated context remains generation-based.

`NativeRunner` reuses the public CLI `Options` type but is an RTVI runner, not the
CLI receipt/image endpoint. All constructors reject `playback_protocol=ChunkV2`,
`image_channel`, or `initial_image` before creating a session or starting providers.
This also applies to the Soniox, Whisper and WebRTC compositions. Default legacy
options are unchanged. Use the explicitly opted-in voice-agent CLI/native APIs
for those separate protocols; browser image/receipt integration remains open.


## Browser transport completion failures

The runner now awaits the browser transport finish result after pipeline cleanup.
A transport protocol/I/O failure cannot turn an otherwise successful WebSocket
session into `SessionSummary { successful: true, .. }` merely because its inbound
queue ended. Normal WebSocket Close and existing text/audio/voice flows keep their
normal success behavior; malformed frames and abrupt EOF are not newly tolerated.

A lone pipeline error remains unchanged. A lone transport error becomes the
session error. If both fail, `RunnerError::SessionCleanup` retains the original
pipeline error as its `primary` source and first displayed diagnostic, with the
typed `cleanup` error alongside it. `RunnerError::WebRtcCleanup` (with the webrtc
feature) retains native shutdown details when the private BrowserSession wrapper
finishes a native peer. These are additive public error-enum variants; the
transport wrapper itself is private. The actual RTC server still uses its
existing registry-owned close path and error precedence.

The public summary schema is unchanged: any error becomes `successful: false`
with the existing empty context and zero pipeline-error projection. Clients do
not receive the internal typed error diagnostics through that summary; a zero
pipeline-error count alone never proves the underlying pipeline succeeded.

Native tests exercise actual authenticated/origin-checked HTTP/WebSocket setup,
correlated RTVI readiness, graceful Close, malformed unmasked frames and abrupt
TCP EOF. Direct private-wrapper tests verify exact typed WebSocket errors after
the owned task joins; runner end-to-end tests separately verify the success flag
and every fixture owner/listener release. Error-priority/source-chain assertions
are focused combination tests, not a claim of real simultaneous pipeline and
transport faults in one end-to-end session. Existing complete native text/audio,
WS voice and Opus RTC voice programs are compatibility controls. No browser
DOM/permission/playback execution or live-provider inference is added here.
