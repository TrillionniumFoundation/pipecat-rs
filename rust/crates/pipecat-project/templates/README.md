# {{name}}

A native Rust browser voice agent: WebSocket/RTVI 2.2 protobuf audio →
Deepgram streaming STT → OpenAI chat completions → Cartesia streaming TTS.
The server owns isolated sessions and keeps provider credentials out of the browser.

## Build and check

Use Rust 1.99 or newer. This project depends on the existing local Pipecat Rust
checkout through the explicit absolute path in `Cargo.toml`; keep that checkout
in place or update the dependency path after moving it. The dependency is not
published and does not point to an unpushed remote branch.

```sh
cargo build
cargo run -- --help
```

Use `cargo build --offline` when all registry dependencies are already cached.
Commit the resulting `Cargo.lock` to pin this application's dependencies.
`--help` does not read credentials or start a listener.

## Configure and run

Provide `DEEPGRAM_API_KEY`, `OPENAI_API_KEY`, and `CARTESIA_API_KEY` through your
process environment or secret manager. `.env.example` lists the supported names
and non-secret defaults. The server does not automatically load `.env` or execute
shell configuration. Never commit credentials or put them in browser config.

{{client_instructions}}

The listener defaults to `127.0.0.1:7860`; `--bind` accepts only loopback addresses.
Use the exact URL printed by the server, since browser origins are checked.
Ctrl-C shuts down the HTTP listener and owned voice sessions. Optional
`--vad-model MODEL.onnx --onnx-runtime LIBRARY` enables native Silero with explicit
local assets. Those model/runtime files are not downloaded by this generator.
`--no-intro` waits for the user's first turn.

The browser asks for microphone permission only after Connect. Session start and
provider requests may incur provider charges. Microphone capture, speaker output,
autoplay restrictions, and real-provider credentials require separate live testing.
No physical-device or live-provider validation is implied by generation or compilation.

## Contract and limits

- `/api/start` starts the selected `websocket` transport using native RTVI/protobuf
- Conversation context records generated text; it does not confirm audible playback
- Provider selection is the implemented Deepgram/OpenAI/Cartesia cascade
- Daily, LiveKit, SmallWebRTC server sessions, telephony, realtime models, video,
  and cloud deployment are not generated; unsupported selections are errors
- Local HTTP origin checks do not replace production authentication or TLS
- `pipecat-project.json` records generation choices; it is not runtime configuration
- Python reference templates and baselines are not imported or modified
