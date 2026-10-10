# pipecat-moq

Native client-side MoQ Lite 06 over HTTPS WebTransport/QUIC. Publishes and
consumes real MoQ broadcasts and tracks through the source-compatible Rust
protocol implementation. No Python interpreter or language-binding wrapper
is used.

## Supported slice

- One remote peer, with distinct publish and subscribe origins
- Explicit broadcast announcement after all publishing tracks exist
- Both plain and compressed Hang catalogs are published; plain `catalog.json`
  is consumed, alongside Opus audio and legacy
  frames carrying a QUIC-varint microsecond presentation timestamp
- Outgoing S16LE mono PCM at 48 kHz, exactly 960 samples / 20 ms per publication
- Incoming Opus/48 kHz with one or two channels, decoded to mono S16LE;
  stereo downmix sums through a wider integer before averaging
- A fixed-name `transcript.json.z` JSON-object append log, compressed using
  one raw-DEFLATE window in exactly one group
- Transport-owned sequence and random epoch fields, replay suppression,
  and internal `moq-transport/session-ending` handling
- Scoped trust roots or bundled public trust roots; verification stays enabled
- Bounded application queues and local publication acknowledgements

`MoqConnection::connect` owns the endpoint and protocol/media tasks. Its
`MoqSender` clones do not keep that owner alive. `MoqMonitor` exposes the
terminal result and application-owned task count. Dropping the connection
closes its endpoint and aborts its owned work; `wait_idle` can observe when
asynchronous Drop cleanup finishes. Explicit cancellation joins owned tasks.

`publish_audio` and `publish_json` acknowledge local publication. They do not
prove delivery, remote rendering, or speaker playback. `end` stops admission,
drains admitted publications, writes the goodbye record, finishes tracks,
and applies a bounded close grace. No `Played` receipt is synthesized.
Calling `end` after prior cancellation is idempotent terminal cleanup; its
success does not turn the earlier cancellation into a graceful drain.

## Explicit limitations

Construction rejects serving, browser-runtime, video, mutual-TLS, reconnect,
and output sample rates other than 48 kHz. Only the `moq-lite-06` subprotocol
is offered and accepted. IETF MoQ draft interoperability is not claimed.
The crate contains no browser client, signaling UI, SFU, or relay server.

The source transport's arbitrary-rate resampling, client reconnect/replay,
serve-mode certificate discovery, browser buffer reset, and full pipeline
input/output processor integration remain outside this slice. Incoming PCM
is returned at 48 kHz; source Python defaults to 16 kHz input. Callers must
explicitly resample if their pipeline requires another rate. Local pacing
and interruption cannot retract audio already transmitted to a peer.

Sequenced transcript records accept string, null, or omitted epochs. Other
epoch types are rejected explicitly. Integer sequence tokens retain their exact
magnitude up to the configured record-byte cap; bool and floating-point tokens
remain legacy unsequenced messages. Omitted and null epochs compare equally.

No live hosted relay, browser, or SFU qualification is implied by local
peer tests. The API uses one connection owner rather than the Python
transport's separate input/output holder count.

The default local bind is IPv4 (`0.0.0.0:0`); local tests qualify IPv4 loopback.
Callers targeting IPv6 must supply a compatible bind. The pinned upstream
WebTransport client tries the first resolved DNS address and provides no
Happy Eyeballs or fallback across addresses.

## Memory and protocol bounds

Defaults: 64 outgoing commands and 64 incoming events, 64 KiB compressed
and expanded transcript records, 4 MiB cumulative compressed and expanded
transcript history, and 4096 records including reserved room for goodbye.
Admission rejects queue saturation. Publication exceeding a transcript
budget terminates rather than rolling groups or discarding an undecodable
prefix. Incoming replays still consume the receive history budget.

Declared frame size is checked before this crate reads the full payload.
Upstream frame storage can allocate to its own group limit before this check.
Raw-DEFLATE
expansion uses a per-record output cap before JSON parsing. Catalog wire
bytes, reconstructed catalog bytes, rendition counts, and pre-audio update
counts are bounded. QUIC receive windows and concurrent stream counts are
finite. Audio uses the upstream ordered cursor and declared max-age window.
Catalog reception switches to newer snapshots and retains an unfinished final
frame across track completion.

These are application and protocol limits, not a strict process-wide heap
bound. Upstream `moq-net::cache::Pool` capacity is a soft target; the live
edge is protected. Its hard group limits are 32 MiB and 8192 frames. The
upstream WebTransport implementation also owns internal QUIC/HTTP3 tasks,
which terminate when the endpoint closes; the monitor counts this crate's
owned supervisor, origin/session drivers and application pumps.

## Source mapping

Original Pipecat revision: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
The relevant source is `src/pipecat/transports/moq/transport.py`, with
behavioral regression cases in `tests/test_moq_transport.py`.

| Python behavior | Native implementation |
| --- | --- |
| Separate client publish/consume origins | `connection::MoqConnection::connect` |
| Tracks exist before announce | `connection::Publication::new` |
| Opus plus Hang catalog/container | `media`, `wire`, `Publication` |
| Ordered compressed transcript log | `transcript::Publisher` |
| Epoch/sequence replay filtering | `transcript::Subscriber` |
| Internal session-ending marker | `receive_transcript` and `Publisher::finish` |
| Audio pacing and local interruption | `Publication::write` and `MoqSender::interrupt` |
| End, Cancel and cleanup | Connection owner, supervisor and owned task guards |

Pinned Python dependency lineage: `moq-rs~=0.5.1`, lock `moq-rs=0.5.1`
→ `moq-ffi=0.4.8`. The locked 4,037,619-byte source archive was verified
with SHA-256 `8a677a44f2a642c76aa82953c5bd1c4e20afdf11d1ba2e9b3f2333c336a9de5c`.
Native protocol/media versions are `moq-net=0.3.7`, `moq-json=0.5.5`,
`moq-flate=0.2.0`, `hang=0.21.8`, `web-transport-moq=1.3.1`, and
`moq-noq=1.3.1`. The official `moq-tokio=0.19.19` async-to-poll transport
adapter is used with default features disabled and a manifest-only vendored
correction disabling its unconditional Rustls defaults; no private protocol shim
or copied adapter is substituted. All seven upstream crates declare
`MIT OR Apache-2.0`.

The `serde_json/raw_value` feature preserves bounded wire tokens for sequence
classification. Cargo unifies this additive feature across the workspace; it
does not enable `arbitrary_precision` or change `Value`'s numeric representation,
and introduces no additional dependency or archive download.

The pinned direct path dependency retains the unmodified upstream Rust adapter.
See `../../vendor/moq-tokio-0.19.19/PATCHES.md` for archive hashes, license,
and exact manifest diff. The vendor is excluded from workspace-wide feature
selection. This prevents accidental AWS-LC/ring ambiguity in existing implicit
WSS and WebPki builders without installing a global crypto provider.
Registry packaging/publishing is unqualified and disabled for this crate.

Upstream source: https://github.com/moq-dev/moq/tree/moq-ffi-v0.4.8

## Validation

Run focused tests and strict linting from the Rust workspace:

```sh
cargo test -p pipecat-moq
cargo clippy -p pipecat-moq --all-targets -- -D warnings
```

Tests use real loopback UDP/QUIC and HTTP3 WebTransport, a locally generated
certificate trusted only by that test client, and independently constructed
native upstream peers. No TLS verification bypass is used.
