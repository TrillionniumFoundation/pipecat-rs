# pipecat-gladia

Native Gladia v2 streaming STT: a bounded REST session-creation client, returned-URL WebSocket session, audio sender, typed recognition/translation/turn events, and `GladiaSttProcessor` for the native pipeline.

`GladiaSttConfig::new(api_key)` defaults to `https://api.gladia.io/v2/live`, `solaria-1`, 16 kHz mono signed 16-bit little-endian raw PCM. Nested language, preprocessing, vocabulary, spelling, translation and message-filter settings follow the source shapes. Optional fields are omitted. `enable_vad` and generic `language` are local settings; use `language_config` for the provider's wire language setting. Metadata includes the native package version under `pipecat`.

## Ownership and bounds

- `connect` performs one POST and one WebSocket handshake. It does not reconnect or automatically retry either billable session creation or submitted audio
- `try_send_audio`/`send_audio` admit complete PCM frames into bounded queues. Admission is not a delivery receipt. Both APIs return backpressure immediately when the queue or unacknowledged-audio budget is full
- Successfully submitted PCM is not retained for replay. The client retains only byte intervals and admission permits until acknowledgment. Partially acknowledged chunks retain their whole budget until the whole chunk is covered
- Wire ACK ranges have inclusive ends: `[0, 4095]` acknowledges 4096 bytes. Overlapping, duplicated, decreasing and delayed ranges are supported. Gaps never advance the contiguous prefix. Unaligned, malformed, wrong-session and out-of-range ranges report nonfatal errors without retiring audio
- Accounting is fresh for every connection generation. Up to 4096 pending chunk records and 128 disjoint ACK intervals are retained. Disabling acknowledgments is rejected before session creation because this mode requires receipts to retire its bounded audio budget
- End/Stop drains admitted PCM, sends `stop_recording`, then closes once without waiting for trailing transcripts. Cancel/drop closes without stop. Connect/read/write/finish/close are bounded. After 20 seconds of write inactivity by default, keepalive is an empty audio chunk and consumes no audio budget or usage
- Full event output fails the session explicitly; it does not block cancellation. Dropping the session cancels its owned actor, which attempts bounded close even if sender clones remain

## Security

Set both `tungstenite` and `tokio_tungstenite` logging targets to WARN or OFF for real sessions. The pinned tungstenite 0.28 dependency logs raw upgrade requests (including temporary URL tokens) and audio/transcript frames at TRACE, and provider-supplied close reasons at DEBUG. Protect any existing wire logs as credential-bearing and private audio/transcript data. This library redacts its own diagnostics and config Debug output; it does not provide dependency-wide log redaction. No global logger or workspace-wide logging feature is modified. A dependency-level sanitized logging path remains a production-hardening item.

Returned session URLs are untrusted and may contain temporary credentials. Exact scheme/host/effective-port origins must be allowlisted. WSS is required except explicit literal-loopback fixture mode. The default origin is only `wss://api.gladia.io`; a deployment returning another origin requires explicit application configuration, never wildcard matching.

DNS results are checked for nonpublic addresses and pinned for connection. Mixed public/private answers fail. TLS still verifies the original host. Both HTTP and WebSocket redirects are refused; proxies and REST retries are disabled; the REST API key is never forwarded to the WebSocket. Diagnostic errors and configuration Debug output redact credentials, endpoints and provider error bodies. HTTP bodies, WebSocket messages, URL/ID strings, handshake input, queues and settings are bounded.

## Native pipeline

`GladiaSttProcessor` emits detailed interim/final transcription with source result, language, timestamp and current input user ID. Enabled usage metrics precede the final transcript. Translation is a separate detailed frame with source-compatible empty user ID, emitted only when its language changes. Addon errors do not stop unrelated transcription.

With active-session VAD enabled, STT metadata recommends external start/stop strategies. Shared native aggregators adopt those recommendations only when the application did not configure explicit strategies. Repeated speech events are deduplicated. `should_interrupt` controls the recommendation. VAD off leaves native defaults alone. `recommended_user_config` is also available for explicit composition; it uses active settings while a session exists, otherwise the next-session settings. The source TTFS metadata default is 1.49 seconds, overridable for a deployment.

Settings deltas validate atomically and apply only to the next fresh session, including local VAD policy. `active_settings` distinguishes the active snapshot from `config_snapshot`. No dynamic reconfiguration or reconnect is implied. Processor cleanup retains accepted future settings for a subsequent setup. Health generations and lifecycle state fence old callbacks; mute suppresses audio admission and output while active.

## Deliberate limits

This is not full Gladia-family parity. Automatic resumption and the Python service's replay buffer are deliberately absent: an interrupted write may already have reached the provider, and replay could duplicate recognition or billing. No exactly-once claim is made. Applications may explicitly open a new session for new audio after a failure.

Only shared native signed 16-bit PCM is supported; A-law, μ-law and other bit depths are excluded. Non-transcription/translation addons and lifecycle events expose only their bounded type name, with payload discarded; their semantics are not implemented. Channel/speaker-separated transcript attribution, live quality/billing, hosted model access, addon availability, region-specific returned origins and real resumption remain unqualified. No provider credentials, live API, original Python runtime, remote agents or SDK backends are needed for native loopback fixtures.
