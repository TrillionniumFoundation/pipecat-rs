# Native Soniox speech

`pipecat-soniox` implements Soniox streaming STT and multiplexed TTS in Rust using Tokio and WebSockets. There is no Python runtime, provider SDK, credential discovery, or automatic replay of paid work.

## Public interfaces

- `SonioxSttClient` / `SonioxSttSession` / cloneable `SonioxSttSender`
- `SonioxTtsClient` / `SonioxTtsStream` / cloneable `SonioxTtsSender`
- `SonioxSttProcessor` / `SonioxTtsProcessor`, implementing the existing native `FrameProcessor`
- Complete, validated `SonioxSttSettings` / `SonioxTtsSettings`, atomic JSON deltas, structured/plain STT context, and typed events/errors

STT `connect()` writes initial configuration and returns a session; it does not claim provider acceptance. Send actual `AudioRawFrame` PCM through `send_audio` / `try_send_audio` while draining `next()`. `finalize()` requests an utterance endpoint. `finish()` admits an empty **text** end-of-audio message, then the actor closes without awaiting trailing transcripts. `cancel()` discards pending events and closes without an end-of-audio message. PCM is signed 16-bit little-endian, interleaved 1–8 channels, 8–48 kHz; admission checks the actual frame rate, channel count, and sample alignment. There is no resampler or compressed-audio decoder.

A TTS client and every clone or `with_settings()` snapshot share exactly one socket. `stream("unique-id")` eagerly opens a provider stream, then its sender accepts incremental text and a single `finish()`. Up to five streams can overlap. `next()` delivers only events for that stream. Each stream retains its own language, PCM remainder, word remainder, text/audio/timestamp budgets, and deadline. Use a new, unique stream ID on the same connection, including after cancellation or failure. `audio_end` is informational; `terminated` is terminal. Config is sent once per stream before text. `cancel()` on a stream affects only that stream; `cancel()` on the client retires the socket and all streams. Cancellation is bounded by the current socket write/close timeout. A transport-failed client stays closed; create a new client explicitly to submit new work.

TTS output is mono signed PCM16 LE at 8000, 16000, 24000, 44100, or 48000 Hz. Partial samples are carried between messages and rejected at termination if incomplete. Nonempty decoded bytes count as source-audio evidence, but neither source bytes, provider timestamps, nor termination are playback receipts.

## Pipeline behavior

The STT processor has nonblocking audio admission, mute and passthrough controls, detailed raw-token transcripts, and upstream typed errors. In local-VAD mode (default), VAD stop sends `finalize` and STT proposes no turns. Provider-endpoint mode proposes one start on VAD or first real token, then emits finalized transcription before the stop proposal. Configure the existing user aggregator explicitly with `recommended_user_config()` to use those external proposals. Metadata alone does not install turn strategies. STT never emits interruption itself. The source-derived metadata latency is 0.35 seconds, not a measured native/live benchmark.

A validated STT settings change is ordered with admitted audio, closes the old socket, discards its unfinalized tokens, and opens a newly configured socket. An open provider turn is withdrawn with a stop proposal before the reset event. Earlier PCM is never replayed. Audio queued after the settings command belongs to the new socket. A failed reconnect terminates the session explicitly. There is no retry/backoff supervisor or silent resurrection following health restoration; set up a new STT session after a terminal failure.

The TTS processor uses existing `pipecat-text` aggregation, identity-preserving text channels, word alignment and progress frames, and the native zero-audio watchdog. Sentence mode is the default; token mode is available. Settings finalize the old pending text and rotate contexts while preserving their immutable settings and original/display/synthesis anchors. Synthesis/output emission stays ordered even while the next stream is pre-opened on the same socket. Empty tool-only turns cancel their eager stream without fabricated TTS start/stop or silent-context failures. Health-disabled responses retain failed source-end identity without admitting synthesis. Restored health permits a new context on a healthy shared socket. Cleanup closes the shared socket; reuse after cleanup requires a new client.

Only the native output backend's real `PlaybackReceipt`s may advance spoken history. The included receipt fixtures use the actual output queue, buffer/sink and assistant-history reconciliation. Cross-context settings rotation verifies stream identity, language, anchors and played PCM; the existing spoken-history reconciler certifies one context per response, so these fixtures do not claim multi-context history certification.

## Bounds and deliberate strictness

All queue capacities, WebSocket messages, timeouts, active/total stream counts, per-stream text/audio/timestamps, and STT retained token metadata are bounded. HTTPS/TLS WebSockets are required except explicit localhost/loopback fixtures. Endpoint credentials, query parameters and fragments are rejected; secrets and raw provider error text are excluded from Debug/error output. Unknown settings and invalid/null required fields fail atomically. JSON deltas are inspected before cloning; text queue permits are acquired before allocating command copies.

Each TTS packet stages at most two events and reserves output slots atomically after validation. Malformed packets contribute no audio or word events. Previously admitted packets drain before a provider/protocol error; cancellation discards pending events immediately. A queue capacity of at least two is needed for combined audio/timestamp packets.

Slow TTS consumers get an explicit `Backpressure` terminal error for that stream; healthy siblings keep receiving. STT output backpressures its one session, with cancellation outside the queue wait. Concurrent send/finish has one terminal admission point; no accepted text follows it. Read/finish futures can be dropped and retried before admission. A timeout or connection failure is billing-ambiguous and never retries text/audio/configuration automatically.

Unlike the permissive Python receive loops, malformed JSON/token fields, mismatched timestamp arrays, non-monotone or invalid timestamps, and malformed audio produce typed failures. Malformed stream-scoped data retires that stream; malformed connection-level data retires the socket. Error and `terminated` take precedence over attached audio/timestamps, matching the pinned TTS source. Unexpected early termination is rejected rather than reporting successful synthesis. Unknown/retired stream IDs are ignored. These are explicit protocol/security decisions, not claims of byte-for-byte Python exception handling.

## Verification boundary

Tests use real localhost TCP/WebSocket and HTTP peers, native processors, external user-turn aggregation, a synthetic HTTP LLM, and receipt-backed native audio output. PCM bytes and wire traffic are real; recognition, synthesis and LLM responses are synthetic. No live Soniox account, production TLS path, model/voice availability, quality, latency, billing, or hosted interoperability has been verified. Loopback success must not be described as live provider certification.

Run from the Rust workspace:

```sh
cargo test -p pipecat-soniox --offline --locked
cargo clippy -p pipecat-soniox --all-targets --keep-going --offline --locked -- -D warnings
cargo fmt -p pipecat-soniox --check
```

`SOURCE_MAP.md` identifies the pinned source and the behavior-to-fixture mapping.

Provider-endpoint recognition inserts one native `PipelineFlushFrame` roundtrip
at each proposed user start before it admits that turn's normal transcript and
stop. This lets the user aggregator's interruption settle without discarding a
coalesced final-text/endpoint packet. Transcript interruptibility and downstream
assistant/tool authority are unchanged. The owned wait holds no state or
emission lock, uses at most the smaller of five seconds and the configured
response timeout, and stops on lifecycle or health changes. It rechecks the
same session and health generation before continuing. This adds one pipeline
roundtrip per provider start; it is an ordering tradeoff, not a throughput claim.
The marker uses its own origin and is not a synthesis/playback-completion fence.
