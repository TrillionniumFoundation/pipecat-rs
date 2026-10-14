# Optional MiniMax HTTP synthesis

Enable `pipecat-speech/minimax` or the facade's `minimax` feature. The default
feature closure is unchanged. `MiniMaxHttpClient` and `MiniMaxSynthesisRequest`
implement a provider-specific HTTP/SSE protocol; `HttpTtsBackend::MiniMax` reuses
the existing bounded TTS processor rather than a new lifecycle implementation.

## Sources and deliberate corrections

The fixed Python baseline is `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
[`services/minimax/tts.py`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/minimax/tts.py),
Git blob `d9cd8e83f2a58a98669707b9794cc1b527b5ef07`. The
[official HTTP API](https://platform.minimax.io/docs/api-reference/speech-t2a-http)
was read on 2026-10-14: status 1 means synthesis is in progress, status 2 means
completion, and `base_resp.status_code == 0` means success. With the default
`exclude_aggregated_audio=false`, the final chunk includes aggregate audio.

The original parser waits for the next `data:` substring before processing the
current record. Consequently it drops the last buffered record at EOF. It does
not check provider status codes, skips any record containing `extra_info`, and
logs/skips malformed JSON or hex. Retained executions of the unchanged original
method with in-memory HTTP/frame stubs establish these differences; they are not
a live provider or full Python service test.

This native adapter intentionally corrects those defects. Bounded SSE framing
handles fragmented records and the final record at EOF. Provider errors and
malformed payloads fail closed. Successful completion requires a terminal record,
clean EOF, nonempty incremental PCM and complete samples. Final aggregate audio
is not replayed. These are explicit adaptations, not byte-for-byte equivalence
with the original faulty parser. Early PCM already emitted before a later
failure cannot be recalled; the utterance must not be relabeled successful.

## Request and capability profile

The client takes an explicit nonempty GroupId in addition to `SpeechConfig`.
It sends bearer authentication and URL-encoded `GroupId` to `/v1/t2a_v2`, with
`stream=true`, mono PCM, bitrate 128000 and the requested rate. Defaults preserve
`speech-2.8-turbo`, `Calm_Woman`, speed 1, volume 1, pitch 0 and omitted language.
This is streaming PCM only: no compressed audio, non-streaming response, subtitle
timing, voice cloning, dictionary management or arbitrary payload passthrough.
The accepted rate set is the documented 8000, 16000, 22050, 24000, 32000 and
44100 Hz, a narrower profile than Python's unchecked rate forwarding.

Requests are immutable snapshots for each accepted utterance. Shared typed
settings updates support model, voice and language; provider-specific voice
controls remain request configuration rather than unvalidated settings extras.
The original runtime speed/emotion update example is consequently outside this
first profile. No full settings-API equivalence is claimed.
The typed emotion enum follows the nine values in the official schema read on
2026-10-14, including `calm` and `whisper`. Python's canonical settings accept an
arbitrary emotion string; its deprecated params path instead lists `neutral`
and omits those two values, despite its docstring mentioning `calm`. This native
typed profile deliberately does not copy that inconsistent deprecated whitelist.
Speed is finite in 0.5..=2, volume is finite in 0<volume<=10, and integer pitch
is -12..=12. Rejecting zero volume follows the official schema's exclusive
minimum; the original docstring's inclusive zero is not reproduced.
Prepared input changes only synthesis text on the wire. Original/display text
and its provenance remain available to the existing alignment/history pipeline.

No word or token timestamps are invented. The existing `SynthesisCompletion`
reporting produces `Unreported` text progress; even complete controlled output
Played receipts do not alone authorize audible assistant history. Socket receipt,
provider completion, output submission and physical playback are distinct.

## Stream limits and terminal rules

Each incremental audio string must contain an even number of hexadecimal
characters, with no whitespace or separators. Decoded PCM may split a sample
between events; a one-byte carry is preserved until the next event and a residual
byte at EOF fails. The ignored final aggregate is type-checked, not decoded or
hex-validated. Its entire raw representation still counts against wire and event
budgets. A final-only response has no usable incremental PCM and fails.

Errors are checked before metadata/aggregate omission. A status-1 record with an
`extra_info` key skips that record's audio, matching the original key-presence
rule, but does not establish completion. Duplicate final records or any later
data record fail; bounded comments and metadata without data may follow final.

Default independent budgets are 128 MiB cumulative wire bytes, 65,536 JSON data
records and 64 KiB per request text field. `SpeechConfig::max_json_bytes` bounds
the serialized request and each raw SSE block, including ignored/comment fields;
`max_audio_bytes` bounds all incremental decoded PCM including sample carry.
The existing queue capacity and total request timeout also apply. These are
content and queue budgets, not an exact bound on parser/network allocations.
PCM is interpreted as signed 16-bit little-endian mono, the native/source frame
profile. The archived public schema excerpt names PCM without independently
qualifying its sample width/endianness; live provider bytes remain unqualified.

## Ownership and qualification

The adapter reuses the owned bounded `ResponseStream` and existing HTTP TTS
interruption, cancellation, settings and error handling. Redirects and automatic
retries are disabled; the direct client does not use ambient proxy settings.
Errors do not include credentials, GroupId, request text or response bodies.

Tests use synthetic loopback HTTP servers and dummy credentials. They qualify
request/stream parsing, actual native pipeline behavior and controlled backend
receipts. They do not qualify a real account, model availability, pronunciation,
voice quality, physical audio playback or provider deployment.

The `pipecat-speech` integration target `minimax` checks actual HTTP requests,
source-derived language goldens, fragmented SSE/UTF-8/hex/PCM, corrected EOF,
aggregate omission, independent limits, provider/format errors, no redirect or
retry, incremental delivery, timeout, backpressure and socket closure. The
facade target `minimax_tts` with `minimax,assistant-history` drives the shared
processor, atomic settings snapshots, cancellation/interruption/reuse and real
OutputTransport callbacks. Its controlled Played receipts leave heard history
empty, as required for timestamp-free synthesis.
