# Source and wire map

Pinned source baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
Integration source: `8b844ec09a9aea129d12fc8bbe53445cdcd0e119`.
Official protocol references consulted 2026-10-10; their examples anchor wire fixtures, not live-provider qualification.

| Contract | Python baseline | Native implementation / evidence |
|---|---|---|
| Nested settings/defaults | `src/pipecat/services/gladia/config.py:16–149`, `stt.py:173–230,268–342,389–437` | `settings.rs`, `config.rs`; exact loopback request test |
| Session POST, key/region, response ID/URL | `stt.py:512–611` | `client.rs`, `security.rs`; HTTP plus actual returned-URL WS fixture |
| Future-session settings | `stt.py:440–465` | sender/processor snapshots and atomic merge; next native setup fixture |
| End versus Cancel | `stt.py:467–487,660–662` | owned finish/cancel/close; actual pipeline and busy-writer tests |
| Audio/ACK/keepalive | `stt.py:489–510,646–658,686–695,757–763`, `stt_service.py:745–775` | base64 JSON writer, interval accounting, idle empty chunk |
| Transcript/translation | `stt.py:701–749` | typed events, raw result, detailed frames and usage-before-final |
| VAD and metadata | `stt.py:359–377,621–644,750–753` | active snapshot metadata and deduplicated proposals; shared aggregator fixture |
| TTFS default | `src/pipecat/services/stt_latency.py` | 1.49 seconds; explicit native override |

Primary references:

- https://docs.gladia.io/api-reference/v2/live/init specifies POST `/v2/live`, `x-gladia-key`, region values, PCM format constraints, nested settings and returned temporary-token URL. The documented default returned origin is `wss://api.gladia.io`
- https://docs.gladia.io/api-reference/v2/live/websocket specifies base64 `audio_chunk`, `stop_recording`, transcript, speech and translation events. Its Audio Chunk Acknowledgment example uses `byte_range: [0, 4095]` with `time_range: [0, 0.256]`. For mono 8 kHz 16-bit PCM, 0.256 seconds is 4096 bytes, consistent with an inclusive endpoint
- https://github.com/gladiaio/docs/blob/1d52634f8e7fc0b4ab7768f0829a2f5d966e3505/asyncapi.yaml#L930-L972 explicitly defines audio acknowledgment byte offsets as inclusive. This is the normative basis for the range conversion, beyond the numeric example
- https://github.com/gladiaio/sdk/blob/a3841a9fbfb976bc81346faf0eb5fe0acb0b2604/packages/sdk-js/src/v2/live/session.ts#L145-L210 binds the synthesized lifecycle `session_id` to the init response `session.id` and requires acknowledgments for its buffering. Native validates a supplied wire session ID against the init ID; its own generation is independent
- https://www.iana.org/assignments/iana-ipv6-special-registry/iana-ipv6-special-registry.xhtml informs conservative rejection of IPv6 special/documentation/transition ranges in destination policy

## Intentional source corrections and exclusions

Python trims its retained audio using the ACK end as a cursor. Applying that literally to the official inclusive example retains one byte and can split PCM. Native ACK accounting converts inclusive end to an exclusive aligned prefix, unions delayed ranges, never crosses an unacknowledged gap and never trims raw samples. Invalid ranges leave accepted accounting unchanged. Partial chunk coverage conservatively keeps its full admission budget.

The source reconnects and resends retained audio. Native does not reconnect or replay. The source's full capability is therefore not claimed. Native supports signed16 shared PCM only; undocumented response payloads/addon semantics are bounded observations rather than fabricated outputs. Session setup failure cannot be reported as a successful connection.

Security deliberately exceeds the source: bounded bodies/strings/handshakes, exact origin allowlisting, public-address DNS checks plus pinning, no redirects/proxies/retries, no API-key forwarding, and no returned-URL/provider-error text in application-authored diagnostics. Pinned tungstenite 0.28 `src/handshake/client.rs:200` logs its raw upgrade request at TRACE; `src/protocol/mod.rs` also logs frames at TRACE and peer close reasons at DEBUG. Embedding applications must set both `tungstenite` and `tokio_tungstenite` targets to WARN or OFF. This dependency-wide credential/audio/transcript logging residual is documented prominently in the crate README and public module docs; a sanitized dependency logging path remains a production-hardening item. HTTP status is observable without dumping provider bodies.
