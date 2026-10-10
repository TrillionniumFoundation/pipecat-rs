# Native Gradium TTS

`GradiumTtsClient` multiplexes bounded text-to-speech requests on one WebSocket.
`GradiumTtsProcessor` adapts it to the native frame pipeline, sentence aggregation,
word alignment, settings snapshots, health generations and ordered response fences.
The facade exports it through the existing optional `gradium` feature; the default
feature/dependency closure is unchanged.

## Pinned wire

- Header authentication is `x-api-key`, with `x-api-source: pipecat`
- Every setup/text/EOS carries its exact `client_req_id`. Setup sends `voice_id`,
  `output_format: "pcm"`, `close_ws_on_eos: false`, and optional `json_config` as a
  **string**. It never borrows STT's object serializer. Defaults are the source's
  voice `_6Aslh2DxfmnRLmP` and mono PCM16LE at 48 kHz
- The source exposes model/language settings but omits them from TTS setup. This
  API rejects those updates explicitly. Only voice and optional serialized JSON
  object configuration are supported. JSON string whitespace is preserved
- Text can be written before ready, and ready may be absent. A supplied rate must
  match 48 kHz. Duplicate ready and multiple named audio/text streams fail that
  request. Empty stream-name arrays and an optional positive bounded frame size
  are accepted. Source ignores ready metadata; these validations are stricter
- Audio is base64 PCM, with one incomplete byte retained across messages.
  Incomplete terminal PCM fails; encoded audio is never relabeled PCM. Text events
  report the complete text segment with `start_s`, as in source. `stop_s` is not
  used. Negative, nonfinite or backward start times fail the request
- `finish` is TTS flush: one request-scoped `end_of_stream`, never STT `flush`.
  Admission closes immediately and its absolute deadline starts before queue
  drain. EOS is written only after all admitted text. Remote EOS before local
  EOS fails. One `Done` or error ends a stream; a later cancel cannot add another
  terminal result. Success drains the admitted event prefix
- A scoped provider error fails only that request. An unscoped error or actual
  socket failure fails all remaining requests. Provider error text, close bodies,
  endpoint addresses and API keys do not enter this crate's error messages

## Ownership and boundedness

Each client/settings family owns a single actor and socket. Stream drop/cancel
retires only its local request, drops queued output and never emits a provider
cancel message, matching source. Retired IDs cannot be reused on that connection;
late output cannot borrow a new request's identity. The total request count,
including canceled requests that may still exist remotely, is bounded until the
connection is retired. There is no transparent reconnect or text replay.

Queue counts, total admitted text per request, each JSON/audio chunk, buffered
PCM bytes, cumulative PCM/timestamp characters, live requests, lifetime request
IDs, session wire input/output bytes, incoming message count, JSON depth/nodes,
connect/write/read/finish/context/session duration are finite. Context deadlines
start at admission and cannot be renewed by traffic. Finishing an input queue is
out of band. Saturated output fails its request instead of blocking other streams;
previously admitted audio/text remain available before the error. Cancellation
alone discards that prefix. A timeout during a shared socket write/handshake fails
the connection, as it cannot establish a safe write boundary. Finish/read/context
bounds also interrupt nested socket writes and pending connection setup.

`cancel` closes all client requests immediately. `shutdown` additionally waits for
its owned actor. Dropping a shutdown future preserves actor ownership; dropping
the last owner aborts it. Processor End/Stop flushes/drains before forwarding the
terminal frame, then joins the canceled socket. Cancel/interruption/drop fence
old output using the existing native task and invocation generations. New voice
settings affect only newly created contexts; previous contexts retain their
original voice/config. A valid settings update flushes pending old-snapshot text
and rotates the native context; the pinned base service does not flush its pending
text on settings update. Invalid/no-op updates do not rotate or restore health.

Pinned Python `setup()` opens the WebSocket transport eagerly; `run_tts` delays
logical request setup until synthesis text exists. Native logical admission
matches that timing, but native transport establishment is lazy too. This stronger
no-TCP behavior for empty turns trades connection warm-up for first-text latency;
no billing behavior or live latency has been measured.

The processor snapshots settings at response admission but opens a provider
request only when its first nonempty synthesis text is ready. Empty/tool-only
responses and interruption/End before any text do not consume remote request IDs.
End after a partial sentence still flushes and synthesizes that pending text.
The processor requires a 48 kHz pipeline output rate. It preserves generated
text/alignment events separately from actual audio. An empty provider EOS does
not publish successful alignment or a successful stop: the shared zero-audio
watchdog reports it. Synthesis completion and flow flush fences are not playback
receipts and cannot establish spoken history or physical audibility.

Endpoint policy and connection machinery are shared with STT: explicit literal
loopback fixtures only, otherwise secure public endpoints; no credentials in the
URL, query strings, fragments, proxies, redirects or mixed private DNS answers.
Resolved addresses are pinned. TLS/HTTP upgrade reads are bounded independently
of message limits. Disable `tungstenite` and `tokio_tungstenite` TRACE/DEBUG for
real credentials: dependency logging can expose headers and payloads. Application
logging of public event payloads can also expose speech/audio; this is not an
end-to-end secrecy guarantee.

## Source and current documentation

Static reference: Pipecat commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/services/gradium/tts.py`, SHA-256
`4eb072ac4734fe788f32faf747c3a55fadfead59ee0f2c05f95603bef69cdf87`.
The reference implementation, SDK and Python tests are never executed/imported.

Official protocol cross-check (2026-10-10):
- https://docs.gradium.ai/api-reference/endpoint/tts-websocket
- https://docs.gradium.ai/guides/multiplexing

The current API supports object or string `json_config`, explicit PCM rates,
other codecs, model selection and extra settings. This implementation deliberately
retains the pinned string/48 kHz/setup contract. The reference and multiplexing
guide agree on `client_req_id` and raw-WebSocket base64 `audio`; some marketing
examples use different names/encodings and are not this contract.

Residuals include current-schema models/codecs/rates, source automatic reconnect,
provider tracing/event callbacks, live credential/provider qualification, full
cross-provider bot composition and physical playback/device acceptance. Native
synthetic local fixtures establish wire and lifecycle behavior only.
