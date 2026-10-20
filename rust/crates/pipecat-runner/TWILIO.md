# Native Twilio inbound cascade

`pipecat-twilio-agent` is a separate, explicitly enabled entry point. The default
browser WebSocket/SmallWebRTC host is unchanged. The project generator also
offers an explicit `--transport twilio` independent application profile.
It composes the existing native Deepgram STT, user turns and DTMF aggregation,
OpenAI LLM, generated-text context, Cartesia TTS, telephony state machine,
streaming resampler and μ-law codec. No Python runtime, RTVI handshake, browser
protobuf or WAV header belongs on the phone connection.

## Run and embed

From `rust/`:

```sh
cargo run -p pipecat-runner --bin pipecat-twilio-agent -- --help
# Supply these explicitly in the host process, without committing credentials:
# TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN
# PIPECAT_TWILIO_WEBHOOK_URL=https://your-host.example/twilio/voice
# PIPECAT_TWILIO_MEDIA_URL=wss://your-host.example/twilio/media
# DEEPGRAM_API_KEY, OPENAI_API_KEY, CARTESIA_API_KEY
cargo run -p pipecat-runner --bin pipecat-twilio-agent -- \
  --enable --bind 127.0.0.1:7861 --no-intro
```

Without `--enable`, no credentials are read and no listener is opened. `--help`
also reads no credentials. Browser CLI flags are not silently accepted. The
usual hosted-provider model, voice, system instruction and sample-rate settings
from `ProviderConfig` apply. This entry selects only Deepgram/OpenAI/Cartesia.

The standalone main delegates to `twilio::command::run_from_env(TwilioApp::new)`.
Generated Twilio applications pass their shared `create_app` constructor to the
same command, which invokes it with the listener, TwilioConfig and ProviderConfig.
Both ultimately use `TwilioApp::new`, the public loopback-test constructor. Typed callers
can inject explicit endpoints and limits without any ambient-environment lookup.
`TwilioConfig::new` supplies conservative defaults; `from_lookup` reads the four
required Twilio settings above plus these optional values:

- `PIPECAT_TWILIO_HANGUP=disabled` (default) or `local`.
- `PIPECAT_TWILIO_API_BASE=https://api.twilio.com/2010-04-01` (default).

Typed configuration exposes call/connection counts, form/message/queue sizes,
connect/idle/session/I/O/replay/turn deadlines, and introduction policy. Default
limits are 8 pending-or-active calls, 32 connections, 8-frame queues, 16 KiB
forms, 64 KiB phone messages, 15-second admission/start deadlines, 5-second I/O
and pipeline phase timeouts, 300-second inactivity, 3600-second sessions, and
300-second completed-call replay retention. Provider library bounds also apply.

The listener must bind a literal loopback IP. A separately managed TLS reverse
proxy is needed for a real external caller; this app does not install, start,
configure or qualify that proxy. Product endpoints may use HTTPS/WSS; insecure
HTTP/WS is allowed only for literal loopback IPs. `localhost` and DNS names do not
qualify for insecure transport. No real account, call, provider or public-network
operation is part of the synthetic acceptance suite.

## HTTP, signatures and proxy contract

Only `POST /twilio/voice` and `GET /twilio/media/{capability}` are served. The POST
accepts exactly `application/x-www-form-urlencoded` and responds with actual
TwiML `<Connect><Stream>`. The generated 244-bit random capability is in the media
path, not query parameters. Treat URLs as credentials: do not log or share them.
All routes require the configured canonical path and reject query strings.

Both requests require exactly one valid `X-Twilio-Signature`. HMAC-SHA1 uses the
trusted configured canonical URL, and for the POST every decoded field sorted
by name, including unknown fields. The WS signature uses the configured media
base followed by `/` and that exact capability. Host, Origin, Forwarded and
X-Forwarded-* never provide identity or alter the signed URL. A proxy must preserve
the exact external path, form bytes and signature header; rewrites are unsupported.

The deliberately narrow profile requires UTF-8, valid percent encoding, no
repeated decoded parameter names, and canonical padded Base64 MACs. Malformed
forms, empty names/segments, duplicate/missing signature headers, oversized
bodies and unsupported content types fail closed. Endpoint URLs must already
have the spelling returned by the URL parser; credentials, query, fragment,
normalization aliases, host-case changes, dot segments and explicit default ports
are rejected rather than silently signed differently.

The signature algorithm has an official Twilio-vector unit test. This is not a
claim that all Twilio proxy or URL variants interoperate. In particular, voice
HTTPS port omission and WSS trailing-slash variants mentioned in Twilio's security
documentation are not guessed or retried. Real proxy/WSS interoperability remains
unqualified. Use this exact URL contract, not trial-until-a-signature-matches logic.

A valid POST must identify the configured `AC` account SID and a valid `CA` call
SID. It creates a short-lived capability bound to that call. A signed upgrade
consumes it atomically, even if the peer then fails to send a valid start. Pending
and active calls share the admission limit. Pending entries expire; consumed calls
remain excluded while active and for the configured retention after completion.
The total retained call set is bounded to four times `max_calls`; admission closes
instead of evicting replay records early. No permanent tombstones are retained.
Twilio signatures have no intrinsic freshness: replay after the stated retention
(or after an unused capability expires) is not prevented.

## Phone readiness and media

Before any provider connection, the peer must send `connected` with protocol
`Call`/version `1.0.0`, then a sequence-1 `start` with matching account, call and
stream SIDs, `tracks:["inbound"]`, and explicit 8000 Hz mono `audio/x-mulaw` format.
Every subsequent accepted event requires stream identity and a monotonically
increasing sequence. Media requires `track:"inbound"`; DTMF uses
`track:"inbound_track"`. Stop includes matching account/call IDs. Duplicate start,
pre-start media, mismatched IDs, malformed messages, unknown events, provider
errors and binary frames fail closed. Custom parameters cannot change providers,
credentials, endpoints or hangup behavior.

Input μ-law becomes configured mono PCM16 for STT. Generated text passes to the
existing TTS processor, whose PCM becomes 8 kHz μ-law. DTMF runs through the real
DTMF aggregator and user context, including its first-key interruption. Output
selects only native synthesis audio and interruption clear; arbitrary transport
messages, user PCM, RTVI and generated-text events are not sent to the phone.
Synthesis completion flushes the output resampler tail. Interruption clears its
pending filter tail. Graceful local End or validated remote stop flushes inbound
resampler lookahead before ending the pipeline and drains output. Cancel, failure,
disconnect and host shutdown discard tails and cancel queued media transport.

Mark is only a validated protocol event. It neither acknowledges audible playback
nor resets user activity. Session reports contain generated-text context, which
may include interrupted or unheard text. No spoken-history or carrier/audio-device
quality claim is made.

## Ownership, shutdown and hangup

Every connection, HTTP upgrade, pipeline and provider lifecycle remains under the
listener supervisor. Pipeline startup is retained through cancellation rather
than dropping its cleanup future; bounded provider startup may therefore delay
shutdown. All output is drained without an unbounded collection, pipeline workers
are joined, and the media transport is explicitly finished/canceled and joined.
Admission is released only after those steps and the optional hangup attempt.

Trusted embedders can call `terminate(call_sid, End|Cancel)`; no remote control API
is exposed. `shutdown()` cancels admission and calls, joins every connection task,
and returns the same retained result to concurrent/repeated callers. Canceling a
shutdown waiter does not cancel its cleanup owner. Dropping the last app handle
requests shutdown, but callers should explicitly await it for cleanup evidence.
Reports separate primary pipeline/protocol errors, failed final pipeline joins,
transport cleanup errors and hangup errors. A pipeline join failure can include
provider cleanup failure/timeout, but is not labeled as exclusively a cleanup
failure. It remains visible even when a protocol/deadline error was primary.
Shutdown's aggregate counts retain these failures; the standalone main prints
only sanitized counts, not call IDs, contexts, URLs or credentials.

Hangup is disabled by default. `local` performs one authenticated REST POST with
`Status=completed` after a fully verified start on local End/Cancel, disconnect,
provider failure or shutdown. An explicit remote stop does not issue a redundant
hangup. Invalid webhook/upgrade/start never triggers it. One connection owner
makes one attempt, with no retries or redirects, independent of termination races.
The fixed trusted API base receives only validated account/call path identifiers;
it cannot come from inbound metadata. HTTP 200 succeeds; HTTP 404 with Twilio code
20404 is treated as already ended. Other status, malformed/error response and
timeout remain reported. Secrets and ordinary call metadata are not logged.

## Differences and verification boundaries

Python source baseline: `ProfHepta/pipecat` commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, especially `runner/run.py`,
`runner/utils.py`, and `serializers/twilio.py`.

Intentional differences: fixed namespaced routes rather than `/`; mandatory
canonical request signatures and one-use call binding rather than only optional
session-token/Origin checks; strict bounded form/start/sequence profile; hangup
opt-in rather than Python serializer auto-hangup default; generated-text context,
not spoken evidence. No outgoing calls, recording, transfer/SIP, other carriers,
region/edge convenience options, generated phone eval, or broad Python
dynamic-route compatibility are provided.

Focused commands (run from `rust/`, using an installed supported Rust toolchain):

```sh
cargo test --locked --offline -p pipecat-runner --lib --test twilio_entry --test twilio_flow
cargo test --locked --offline -p pipecat-transports --test adopt_websocket
cargo test --locked --offline -p pipecat-runner --test integration --test lifecycle
cargo test --locked --offline -p pipecat-runner --features webrtc --test webrtc_flow --test webrtc_lifecycle
cargo test --locked --offline -p pipecat-protocol
cargo test --locked --offline -p pipecat --test dtmf
cargo clippy --locked --offline -p pipecat-runner -p pipecat-transports --all-targets -- -D warnings
```

The tests use explicit synthetic credentials and literal-loopback HTTP/WebSocket
provider and hangup peers. They exercise the shared app constructor, not a
second test-only pipeline. Compiled-entry checks cover default-off/help/config
behavior; they are not compiled-main voice E2E. Passing synthetic checks does not
qualify real PSTN calls, provider accounts, WAN/TLS proxies, physical audio,
production readiness or complete Python replacement.
