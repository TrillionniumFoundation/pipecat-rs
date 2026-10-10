# Native Cartesia Live STT

This is a dedicated Cartesia Live speech-recognition client and pipeline
processor. It does not implement Cartesia Turns, reuse Deepgram recognition,
or reinterpret the existing Cartesia TTS client as recognition. The reference
is the original Python source at `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
read as static text only. No Python runtime, provider SDK, original test suite,
credentials or billable provider request is used in native qualification.

## API and wire contract

- `CartesiaSttConfig::new(api_key)` defaults to
  `wss://api.cartesia.ai/stt/websocket`, `ink-whisper`, `en`, mono 16 kHz signed
  16-bit little-endian PCM. Other explicit rates from 8–192 kHz are accepted as
  a native format boundary, not a claim of live provider entitlement/support.
- `CartesiaSttClient::new(config)` validates and owns its snapshot;
  `connect().await` completes the handshake, `session.sender()` yields a
  cloneable bounded producer, and `session.next().await` drains events.
- Handshake headers are `X-API-Key` and `Cartesia-Version: 2026-03-01`.
  Endpoint overrides must be exact `wss` URLs, or loopback-only `ws` URLs.
  Credentials, query strings and fragments in the endpoint are rejected.
- The query contains ordered model, language, encoding and sample_rate fields.
  Repeated `keyterm` parameters use RFC3986 UTF-8 percent encoding, including
  `%20` for spaces, `%2B` for literal plus, `%2F` for slash and `%2A` for star.
- Keyterms strip Python-compatible whitespace, drop blanks, and stop at the
  first 100-term/1200-Unicode-scalar budget overflow. Later short terms are not
  skipped ahead. Only model prefixes `ink-2` and `ink-preview` send keyterms.
  `settings.prepared_keyterms()` exposes effective terms, the overflow-drop
  count and `ignored_model`; callers can surface these diagnostics. Raw
  configuration is retained for inspection. Native input bounds are 4096 terms,
  64 KiB total keyterm UTF-8 bytes, model 4096 bytes and language 256 bytes.
- Audio is binary PCM. `finalize` sends the literal text command `finalize` and
  keeps the socket open; `finish` sends literal `done` after previously admitted
  PCM, makes further admission fail, and drains until peer close with a bounded
  deadline. A final transcript is not invented when the peer sends no text.
- `transcript` text and `is_final` map to interim/final events. A missing
  `is_final` defaults to false. Missing/empty text produces no transcript.
  Provider result JSON and optional string language are retained. Ordinary
  final transcripts have `TranscriptionFrame.finalized=false`: provider-final
  text is not a user-turn-end acknowledgement. Unknown additive event types
  are ignored; error payload contents are not copied into diagnostics.

## Processor and immutable ownership

`CartesiaSttProcessor::new(client)` forwards PCM by default; use
`with_audio_passthrough(false)` to consume it. `with_user_id` fixes one speaker
for the whole session. `InputAudio` uses that speaker; `UserAudio` must match
it, preventing pending text from being attributed to a later speaker.

The processor emits source latency metadata (0.81 seconds), interim/final
transcripts, UTC timestamps and incremental local audio-duration metrics.
Usage precedes each nonempty final. VAD stop in either direction sends
`finalize`; interruptions do not abort listening or create user-turn proposals.
End/Stop queue `done` and drain the owned task before forwarding the terminal.
Cancel, cleanup and drop cancel the owned generation without waiting for data
queue capacity. Muting retires the socket; unmuting explicitly creates a fresh
one if the live processor is usable and not ending.

`config_snapshot()` returns an owned clone. `update_settings(&SttSettings)`
and targeted native STT/generic settings frames accept only model, language
and an array-or-null keyterm field. Validation is transactional. Unknown,
null-required, wrong-type and ineffectual normalized/model-ineligible keyterm
changes are rejected. Exact no-op updates neither reconnect nor change health.
Effective changes while connecting or active return an explicit restart-required
error, leaving the exact old settings, health generation and socket intact.
Dormant changes update only the validated desired configuration; they do not
restore health or open a socket.

`restart_with_settings(delta, live_context).await` is the explicit owner API.
It accepts only the processor's bound runtime/index and a current invocation
(or its live setup context).
Muted, unusable, terminal or stale ownership is rejected before mutation. A
success result requires a newly admitted socket; canceled or rejected handshakes
return an error. Pending handshakes poll raw invocation/health admission every
100 ms; established quiet sessions check health generation every 1 second.
These cleanup timers catch true-to-true restoration without a watch transition.
Explicit cancellation and health-transition notifications still wake immediately.
Every PCM admission, transcript, metric and error is fenced independently of
the cleanup timers. A quiet session schedules at most one extra health timer
wake per second; this is design accounting, not a measured throughput claim.
It validates the entire prospective delta, then cancels the old socket and all
its admitted but undelivered work before opening one fresh socket. Validation
failure preserves the old socket. Failure after retirement returns an error
and leaves no usable sender; retry requires another explicit owner call.
The desired configuration remains inspectable after that failure. The owner
must restore health explicitly when needed; neither settings nor restart
silently restores it. End/Cancel/cleanup are terminal and cannot be reversed.
Reuse after a completed runtime requires constructing a fresh processor.

There is **no automatic reconnect, ambiguous PCM replay, outage buffering,
or background retry**. A new socket never receives old admitted PCM. Runtime
identity, local session serial, cancellation and full health-generation tokens
(including an explicit true-to-true restoration) fence late transcripts,
metrics and provider errors. Accepted enqueues are not retracted retroactively.
Publication is nonblocking: a full pipeline lane is an explicit failure, not
an unbounded event buffer. Serialized publication callbacks/observer clocks
must obey the core context API's no-synchronous-reentry contract.

## Duration accounting is deliberately narrower than the source

The native `audio_seconds` measures **successfully written caller PCM** since
the preceding nonempty final. Silence keepalives are excluded. Graceful finish
emits remaining duration after any final transcripts; failed writes, rejected
admission, cancellation and destructive restart do not invent receipts or
replay duration. This is neither a recognition-quality score nor provider
billing evidence.

The pinned Python base records caller PCM before calling the provider, and its
`WebsocketSTTService._send_keepalive` also records submitted keepalive silence.
It flushes trailing duration on stop/cancel. These are explicit parity gaps:
native usage excludes keepalive, failed/unsent PCM and canceled trailing PCM.
Keepalive **wire behavior** follows the source default: check every 30 seconds,
after 120 seconds without audio send 100 ms of zero PCM, then reset the idle
clock. Shorter positive intervals/timeouts can be configured for local tests.

## Bounds, security and remaining gaps

Input/output queues, messages, writes, handshake, graceful drain and cleanup
are bounded by `StreamingLimits`. A message limit too small for the keepalive
Native maxima are 1024 queue entries, 16 MiB per message, and 300 seconds for
each operation timeout; oversized or overflow-prone configurations are rejected.
frame is rejected up front. `try_*` calls return explicit backpressure; async
admission waits only to the write deadline. Sticky cancellation bypasses full
queues, discards buffered events and yields one cancellation result. Dropping
`next()` does not lose the terminal result; dropping the session cancels even
when sender clones remain. Unexpected close, malformed JSON/fields, binary
responses and oversize responses fail closed without reconnecting. HTTP and
provider status errors use native scoped health classification.

Config Debug and local transport/provider errors redact endpoint credentials,
API keys, keyterms, transcript text and provider response bodies. **Disable
`tungstenite` and `tokio_tungstenite` TRACE/DEBUG logging for real traffic**:
dependency logs can expose headers, URL keyterms, audio, transcripts and close
reasons. Local redaction does not sanitize dependency logging.

Deliberate source differences and exclusions:

- No source-style automatic/deferred settings reconnect or outage replay
- No bare-host URL normalization, deprecated `CartesiaLiveOptions`, arbitrary
  extra query/headers or source extra-header override precedence
- Only PCM16 mono; pipeline-rate inference and alternate encodings are absent
- Mute cancels a generation rather than merely suppressing new audio submission;
  unmute opens a fresh connection without replay
- Cancel/restart discard trailing usage and buffered output
- Unknown string language tags are preserved rather than filtered through the
  Python `Language` enum; malformed/no-type/non-JSON messages fail closed
  instead of the source's permissive logging/ignore behavior
- No source callback decorators, request-ID callback surface, tracing spans,
  processing/TTFB metric lifecycle, extra auth header modes or voice-turn API
- No Cartesia Turns protocol, remote provider acceptance, entitlement,
  recognition accuracy/latency, billable usage or production load qualification

## Static source map

| Pinned source | Native implementation / qualification |
| --- | --- |
| `services/cartesia/stt.py:40–96` | `CartesiaSttSettings`, `prepared_keyterms`; Unicode/count/overflow fixtures |
| `stt.py:186–279,374–426` | Config defaults, exact authenticated handshake, RFC3986 repeated keyterms |
| `stt.py:320–353` | Processor VAD finalize and bounded PCM submission; explicit no replay |
| `stt.py:357–372`, base `stt_service.py:696–743` | Source settings/reconnect audited; transactional rejection and explicit restart are documented replacements |
| `stt.py:428–466` | Owned done/close/cancel and bounded event loop |
| `stt.py:467–520` | Interim/final/result/language mapping and usage-before-final ordering |
| base `stt_service.py:239–270,745–769,1084–1103` | Local-duration policy and keepalive divergence above |
| `tests/test_cartesia_stt.py` | Native wire, header, keyterm, error-health and settings fixtures; original tests never executed |
| `services/stt_latency.py:45` | 0.81s metadata only, not a newly measured latency claim |

`tests/cartesia_live_stt.rs` uses real localhost WebSocket peers and exercises
exact wire fixtures, malformed/large input, deadlines, output/input saturation,
sticky cancel/drop, auth health, immutable/no-op/rejected settings, explicit
restart failure, foreign/stale ownership, restoration fences and pipeline
final/usage ordering. Passing these local fixtures is not a full rewrite,
workspace-wide, platform or live-provider acceptance gate.
