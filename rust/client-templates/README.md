# Rust browser client template

A standalone Rust/WASM browser client for Pipecat's SmallWebRTC and protobuf
WebSocket transports. Application state, browser transport calls, microphone
capture, audio scheduling, protocol serialization, and DOM updates live in Rust.
The only shipped JavaScript is generated wasm-bindgen interop plus the one-line
module initializer in `web/index.html`. This Cargo workspace is deliberately
independent of the native pipeline workspace: it does not import native-core,
Tokio, provider credentials, or native media libraries into the browser.

## Build and run

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.129 --locked
./build.sh
```

Use Rust 1.99 or newer. `build.sh` produces the complete static bundle in `dist/`.
Edit `web/config.json` and rebuild, or change the copied `dist/config.json`:

```json
{
  "project_name": "Pipecat Bot",
  "start_endpoint": "/api/start",
  "transports": ["smallwebrtc", "websocket"]
}
```

Serve these assets and the native start/session proxy on one loopback origin:

```sh
cd ..
cargo run -p pipecat-client-server --example serve -- \
  client-templates/dist http://127.0.0.1:7860/start 8080
```

Then open `http://127.0.0.1:8080/`. See the proxy crate's README for its supported
configuration and local-only security boundary. Direct backend URLs ending in
`/start` also work when the backend explicitly permits the browser origin via
CORS. HTTPS is required for non-loopback microphone access. A static file server
alone does not provide `/api/start`; the bot backend and proxy are separate
services. The default voice mode needs microphone permission after Connect. The explicit
Text-input mode below does not request a microphone; platform permissions
remain under the user's control.

No private API key belongs in `config.json`, HTML, WASM, or a browser URL.
Configure upstream authentication only in the native server. The browser accepts
a short-lived WebSocket connection token from `/start` and URL-encodes it while
preserving existing query parameters. Application event messages do not print
connection URLs, tokens, or SDP. Provider-supplied RTVI event data is displayed as
text and may contain conversation content; avoid recording it without consent.

## Source compatibility inventory

| Source surface | Rust implementation | Remaining limits |
| --- | --- | --- |
| `vanilla-js-vite/index.html`, `style.css` | Preserved layout, brand SVG, theme, controls, conversation and events panels | System font fallbacks replace npm font imports |
| `vanilla-js-vite/src/app.js` | Connect/Cancel/Disconnect, transport state, mic toggle, audio output, bounded event log and transcripts | No original JS SDK object/API exports |
| Three client `config.*.jinja2` files | JSON project name, selected transport list, start endpoint, transport-specific request bodies | CLI generation must explicitly copy this template; old CLI JS/TS templates are unchanged |
| SmallWebRTC | Browser RTCPeerConnection, local audio track, gathered SDP offer/session route, configured ICE servers, remote audio, data-channel RTVI, ping, track-status mute | No trickle ICE, automatic ICE restart, SDP renegotiation, or custom SDP munging; requested renegotiation closes with an actionable error |
| WebSocket | `/start`, token-aware socket URL, actual binary Pipecat protobuf framing, mono PCM16 capture, mono/stereo PCM16 playback, interruption flush | Only protobuf serializer, not provider-specific telephony protocols; no compressed-codec negotiation |
| `@pipecat-ai/client-js` RTVI events | Version 2.2.0 `client-ready`, `bot-ready`, all RTVI event JSON, final user transcription and segment-updating `bot-output` | Legacy bot-transcription-only servers need an adapter; advanced client-message/function/tool APIs are not exposed |
| Next.js `/api/start` and session routes | Separate `pipecat-client-server` Rust crate, fixed-upstream proxy and loopback host | Production authentication, session ownership, reverse proxy and deployment policy are host responsibilities |
| React/Vite and Next console components | Core voice session controls and conversation/events purpose covered by the shared static Rust UI | React/shadcn component APIs, metrics charts, camera/video, screen sharing, device picker, DTMF, codec picker, general attachments and session-info panels are not ported; bounded PNG/JPEG send-file is supported |
| Daily and LiveKit transports | Explicit configuration error before session creation | No Daily/LiveKit adapter; neither is simulated or silently mapped to another transport |

This is a runnable core voice-client scaffold, **not complete parity with the
React consoles or every provider SDK**, and not an automatically integrated
replacement for the Python CLI's generated templates.

## Text messages

After Connect and an exact current-session RTVI 2.2.0 `bot-ready`, type in the
message box and click Send. Enter and Shift+Enter add newlines; IME composition
is not intercepted. Both WebSocket protobuf and SmallWebRTC JSON use the existing
`send-text` command with immediate generation and an audio response. Connect
uses the configured input mode: the default voice mode requests microphone
access, while explicit WebSocket text mode does not.

Text is preserved literally, including Chinese, emoji and surrounding whitespace.
Blank messages and messages above 16 KiB UTF-8 are rejected rather than truncated.
The editor bounds huge drafts at 16,385 UTF-16 code units, one over the raw
ASCII ceiling, so truncated oversized pastes cannot become valid submissions.
All accepted text is preserved exactly. Each encoded request is capped at
128 KiB for WebSocket or 16 KiB for WebRTC, matching the existing receiver limits.
WebRTC therefore accepts less text when its JSON envelope or escaping consumes
that budget. The selected open transport's current
buffer plus the full encoded request must fit within 256 KiB. A busy or failed
send preserves the draft for an explicit retry. There is no application send
queue, automatic retry or reconnect replay. Disconnect clears the draft.

A successful Send clears the box synchronously, so rapid repeated submission
cannot resend the cleared draft. Intentionally typing the same text again is
allowed and gets a fresh session-scoped request ID. Status describes only local
transport queue acceptance: it is not a server acknowledgement, committed user
history, or playback receipt. No confirmed conversation row is fabricated from
Send success. This RTVI path does not implement the separate CLI chunk-v2 audio
receipt or image-channel protocols, and has no image canvas bridge. Explicit encoded-image send-file is documented below.

## Microphone-free text input

To connect without requesting microphone permission, serve this configuration:

```json
{
  "project_name": "Pipecat Bot",
  "start_endpoint": "/api/start",
  "transports": ["websocket"],
  "input_mode": "text"
}
```

The standard `pipecat-browser-agent --assets DIRECTORY` command replaces this
file in memory. For that host, add `--text-input` instead of editing the bundle:
`pipecat-browser-agent --assets client-templates/dist --text-input --no-intro`.
Without the flag, the host preserves its voice default, even if the file requests
text. The flag requires assets. Add `--webrtc` with a webrtc-enabled runner build
to offer both transports; it does not change provider credentials or STT initialization. Direct configuration above applies to custom
static/proxy hosting.

Omitted `input_mode` defaults to `voice`; explicit `voice` keeps both existing
transports. Text mode accepts WebSocket, SmallWebRTC, or both in the configured
selection. Its WebRTC audio transceiver is recvonly with no local track; incoming
audio and the bidirectional RTVI data channel remain active. Unknown modes and
unsupported transports fail validation before Connect. No transport is silently changed. Rust callers constructing `Config` directly must now provide
`input_mode: InputMode::Voice` or `InputMode::Text`.

Text mode snapshots its policy for the owned session, never calls getUserMedia
or builds a microphone capture graph, and keeps the mic button inert. It still
creates/resumes an AudioContext from Connect for bot PCM replies and interruption
handling. It is microphone-free input, not a text-only/silent response protocol.
Normal browser audio/autoplay rules still apply. Change the mode by disconnecting,
editing the served configuration and reloading; there is no live mode-switch API.

Readiness, draft limits and no-replay behavior are unchanged. Disconnect closes
the socket/peer and AudioContext even when no microphone stream exists. Native provider
setup, including configured STT, is unchanged: this option does not promise fewer
server STT connections, different credentials or lower provider cost. Actual
browser permission/capture execution remains unqualified on the current blocked
Chromium host; see VALIDATION.md.

## Ownership and bounds

A session owns its media tracks, AudioContext, capture graph, socket or peer,
callbacks, playback nodes, fetch AbortController and keepalive timer. Disconnect,
page hide, start failure, socket close, peer failure, and cancellation stop those
resources. Generation checks reject late fetch, media and negotiation completions;
a microphone stream arriving after cancellation is stopped immediately. Browsers
cannot abort their microphone permission prompt programmatically, so Cancel
invalidates its result rather than attempting to dismiss that prompt.

Start/offer HTTP requests have a 15-second deadline, ICE gathering a 10-second
deadline, and transport establishment a 20-second deadline. Outgoing microphone
frames are dropped when WebSocket bufferedAmount exceeds 256 KiB. Playback is
bounded to three seconds and 256 scheduled nodes; interruptions stop queued
sources. Binary frames and JSON messages are limited to 1 MiB; HTTP JSON size is
checked after browser response buffering. Events retain 300 rows and transcripts
retain 500 rows. Configuration and unsupported adapters fail closed.

WebSocket microphone capture uses the browser's deprecated ScriptProcessorNode
with a 2048-sample buffer and browser-native AudioContext sample rate, included in
each protobuf frame. This avoids handwritten JavaScript AudioWorklet code but
runs audio callbacks on the main thread, so latency, UI stalls, and eventual
browser removal are meaningful limitations. Production low-latency use should
supply a Rust/WASM AudioWorklet pipeline and browser compatibility testing. There
is no echo-cancellation guarantee beyond the browser's default microphone
constraints. SmallWebRTC audio uses browser-native WebRTC media tracks.

Cancellation cannot undo a bot session already created upstream. The backend
must expire abandoned sessions; the source `/start` contract provides no general
session-delete operation. Disconnect does not send an unrequested bot shutdown
command.

## Validation

```sh
cargo test --locked
cargo clippy --locked --all-targets -- -D warnings
cargo clippy --locked --target wasm32-unknown-unknown -- -D warnings
./build.sh
python3 tests/browser_acceptance.py
```

The browser fixture requires Python `aiohttp`, Python `playwright`, and system
Chromium at `/usr/bin/chromium`. It starts only loopback HTTP/WebSocket services,
uses Chromium's synthetic microphone and synthetic remote audio, and creates a
real paired RTCPeerConnection. Test-only JavaScript implements the remote fixture
and instrumentation; none is bundled in the app. Results and screenshot are
written under ignored `target/`.

Unit tests cover wire-compatible protobuf golden bytes, PCM conversion and
validation, URL/token handling, config/adapter validation, RTVI projection,
connection generations and exhaustion, cancellation/reconnection, audio-flow limits,
and the exact shared text readiness, UTF-8, request-ID and encoded-byte budgets.
See `VALIDATION.md` for the recorded passed checks and blocked browser stage.
Browser acceptance must be reported separately from live-provider, physical
microphone/speaker, WAN TURN traversal, multi-browser, autoplay-restriction and
production-auth acceptance. Those environments are not exercised by the fixture.

### Receive-only qualification boundary

The native receive-only peer rejects audio send/try-send/frame admission before
queueing, including after remote track-status enable. Real native paired tests
observe no upstream audio events and zero audio transport counters alongside
positive SCTP requests and PCMU/Opus downlink audio. These are transport-level
observations, not packet captures or a claim of zero network traffic: ICE, DTLS,
SCTP text and receiver RTCP still send. Default voice remains bidirectional.
The actual child-hosted configuration is tested separately from its consumption
by a native client with loopback provider services. The rebuilt WASM and updated
Chromium fixture do not establish executed browser permission or autoplay behavior;
that environment remains blocked as recorded in VALIDATION.md.

## Explicit PNG/JPEG image input

After the current session's exact RTVI `bot-ready`, choose one PNG or JPEG,
optionally enter a caption, then click **Send image**. File selection never
uploads by itself. This uses the existing `send-file` protocol over binary
protobuf WebSocket or SmallWebRTC JSON. The file's MIME type and signature must
match. The original encoded bytes are preserved exactly: local decoding only
validates, and does not create a canvas, resize, recompress or strip metadata.
Therefore embedded image metadata is sent along with the image. Choose files
whose contents and metadata you intend to share with the configured bot.

- Raw file: WebSocket 64 KiB; WebRTC 8 KiB.
- Caption: 16 KiB UTF-8, preserved literally. Empty captions are accepted.
- Whole encoded request, including base64, caption escaping and envelope:
  WebSocket 128 KiB; WebRTC 16 KiB. The wire limit can reject an otherwise valid
  caption/file pair. Existing 256 KiB combined transport-buffer budget applies.
- PNG: at most 4096 pixels on either side and 1,048,576 pixels total, bounded
  decoding, every chunk CRC, Adler checksum, exact compressed stream consumption
  and exact decoded scanline length. Animation and trailing data are rejected.
- JPEG: 8-bit baseline/extended sequential or complete progressive DCT; 1, 3 or 4
  components with 1–2 horizontal/vertical sampling factors. A conservative
  16 MiB work budget charges 64 bytes per pixel after 16×16 padding (at most
  262,144 padded pixels). Incomplete entropy/scans and trailing data are rejected.
  Intentional partial progressive or lossless files are outside this profile.

One owned FileReader belongs to the current session and a unique request ticket.
Clear / Cancel, changing the file or caption, and disconnect detach its callback
before aborting it. Completion rechecks session, readiness, ticket, selection,
caption, current open transport, raw bytes and current backpressure. A pending
read disables repeat submission. Failed validation or queueing preserves the
selection for an explicit retry; success clears it, and neither success nor
reconnect creates replay work. A successful send means only local transport
queue acceptance, not server delivery, model vision support, user-history commit,
or playback. No confirmed transcript row is fabricated.

Browser execution of these controls is not yet qualified on this host; see the
current `VALIDATION.md` entry. Native policy, wire and server-projection checks
must not be mistaken for executed FileReader/DOM or real-provider acceptance.
