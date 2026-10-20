# {{name}}

Independent native Rust Twilio inbound voice application: Deepgram STT → OpenAI
chat completions → Cartesia TTS, with the existing native phone transport, DTMF
turn aggregation, streaming resampling and μ-law codec. No Python runtime,
browser protobuf, RTVI handshake, WAV header or browser client is involved.

## Build and run

Use Rust 1.99+ and retain the canonical local source checkout named in Cargo.toml.
This package has its own `[workspace]`; the local dependency is not published or
downloaded on your behalf. A generated Cargo.lock pins registry dependencies,
not mutable local path-dependency bytes. Preserve the source revision and any
modifications alongside qualification evidence.

```sh
cargo run -- --help
# Export the required values explicitly in your server process, then:
cargo run -- --enable --bind 127.0.0.1:7861 --no-intro
```

Without `--enable`, the command fails before reading credentials or opening a
listener. `--help` succeeds without either. The shared command retains the
standalone `pipecat-twilio-agent` help label. Browser flags such as `--assets` or
`--webrtc` are rejected, not silently mapped to a different transport.

`.env.example` lists empty Twilio and provider credentials, canonical URL
placeholders and nonsecret defaults. The application never automatically loads
or executes `.env`. Supply `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`,
`PIPECAT_TWILIO_WEBHOOK_URL`, `PIPECAT_TWILIO_MEDIA_URL`, `DEEPGRAM_API_KEY`,
`OPENAI_API_KEY` and `CARTESIA_API_KEY` explicitly. Do not commit credentials.
Provider model, voice, system prompt and sample-rate settings are the same as
the existing native runner. No hidden endpoint environment overrides are added.
`--no-intro` suppresses the initial assistant introduction.

The production main calls this package's `run()`, which uses the runner's shared
Twilio command with this package's `src/lib.rs::create_app`. The same constructor
accepts an explicit listener, TwilioConfig and ProviderConfig for native tests or
embedding. It calls `TwilioApp::new`, not a copied pipeline or browser runner.

## Canonical signed inbound routes

The listener must use a literal loopback IP. Only `POST /twilio/voice` and
`GET /twilio/media/{capability}` are served. There is no `/api/start`, browser
asset route or public remote-control endpoint.

The POST accepts bounded `application/x-www-form-urlencoded` data and returns
actual TwiML `<Connect><Stream>`. It requires one `X-Twilio-Signature` computed
against the exact configured canonical webhook URL and every decoded form
field. A valid account/call creates a short-lived, one-use capability. The
separately signed WebSocket upgrade uses the configured canonical media base
followed by `/` and that capability. Treat capability URLs like credentials:
do not log or share them.

Queries, fragments, URL credentials, duplicate form fields or signature
headers, normalization aliases and signature failures are rejected. Host,
Origin and forwarding headers never replace the trusted canonical URLs. The
phone sends `connected` (`Call`, `1.0.0`) and a matching sequence-1 `start`
before providers connect: account/call/stream identities, inbound track, mono
8000 Hz `audio/x-mulaw`. Subsequent events require matching stream identity and
increasing sequence. Pre-start media, binary frames, identity mismatches and
unsupported events fail closed. Inbound metadata cannot change providers,
endpoints, credentials or hangup policy.

A real external caller needs a separately managed TLS reverse proxy preserving
the exact external paths, form bytes and signature header. This application
does not install, configure or qualify that proxy. Plain HTTP/WS is accepted only
for literal-loopback URLs, not `localhost` or other DNS names. Real Twilio WSS
and proxy interoperability remains unqualified; do not guess URL variants or
retry signatures until one matches. See `crates/pipecat-runner/TWILIO.md` in the
selected source checkout for the complete bounded protocol/security contract.

## Calls, shutdown and permissions

Phone μ-law becomes configured mono PCM16 for STT, and synthesized PCM becomes
8 kHz μ-law output. DTMF uses the real first-key interruption and user-history
path. Context records generated text, including possibly interrupted or unheard
text; it is not proof of spoken playback. A mark is not a playback receipt.

Ctrl-C stops admission, cancels calls and awaits actual connection, pipeline and
media cleanup. The command prints sanitized counts, not phone numbers, call
identities, capability URLs, transcripts or secrets, and fails if final pipeline
joins or media/hangup/task cleanup fail. A pipeline join error can retain the
original provider/startup failure; it does not exclusively mean cleanup failed.
Embedders should explicitly await `shutdown()` rather than treating Drop as join
evidence. Local End and validated remote stop drain; Cancel/failure/disconnect
and host shutdown discard pending media according to the existing runtime.

Hangup is disabled by default (`PIPECAT_TWILIO_HANGUP=disabled`). Do not enable
`local` unless you deliberately authorize a real authenticated REST completion
request: it can end a verified call after local termination, failure or shutdown.
Validated remote stop does not make a redundant hangup request. Generation is
not authorization to activate hangup, place calls, acquire phone numbers, change
an account or deploy anything.

## Scope and source differences

This profile intentionally requires `client=none`, `eval=none` and the selected
Deepgram/OpenAI/Cartesia cascade. Browser defaults and their optional text-eval
starter are separate profiles. No phone eval, outgoing calls, recording,
SIP/transfer, caller lookup/logging, other carriers, realtime provider or cloud
deployment is generated.

Python source baseline is `ProfHepta/pipecat` at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`: project selection, cascade template,
transport macro and Twilio environment placeholders. Deliberate native
differences include separate canonical signed routes rather than `/ws`, strict
one-use call binding, explicit default-off startup, opt-in hangup, generated-text
history and no optional caller-number lookup.

Synthetic signed loopback phone/provider tests establish native wiring and
cleanup only. They do not establish live-account, PSTN/carrier, public tunnel,
TLS proxy, provider availability, microphone/speaker, physical playback or
production-readiness qualification, or full Python CLI parity.
