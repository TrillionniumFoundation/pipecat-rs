# Speechify timestamped HTTP synthesis

Enable `pipecat-speech/speechify` or the facade's `speechify` feature. Both are
default-off and add no dependencies. The public types are
`speechify::{SpeechifyHttpClient, SpeechifySynthesisRequest, SpeechifyEvent,
SpeechifyLimits}` and `HttpTtsBackend::Speechify`; the facade exposes these through
`pipecat::speech`. Existing HTTP backends retain their completion-only reporting.
In particular, this change does not enable ElevenLabs word alignment.

## Fixed-source contract

The reference is [ProfHepta/pipecat at af9f9da9](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/speechify/tts.py),
Git blob `8af3abc008195182d26afe5105300831a8d9908d`. Its tests are blob
`ddda56289260f672d96e79e449832df36b653e54`. The tracked
`tests/fixtures/speechify_source.json` records outputs from unchanged helper
AST definitions extracted from that source. This is a fixed helper reference,
not execution of the full Python service or a live provider qualification.

`SpeechConfig::speechify(key)` selects `https://api.speechify.ai`.
One bearer-authenticated POST goes to `/v1/audio/stream/with-timestamps`, with
`Speechify-Version: 2026-09-30`. Native attribution is `Speechify-Caller:
pipecat-rs` and this crate's version, rather than the Python package identity.
Defaults are model `simba-3.2`, voice `geffen_32`, and 24 kHz. Input, model, voice
and `output_format` are always sent. Language and the two normalization boolean
options are optional; explicit false is retained. Six base language tags map to
source regional tags, other tags pass through. No live availability is claimed.

Supported mono signed PCM16 little-endian rates are 8000, 16000, 22050, 24000,
44100 and 48000. Other positive requests select 24000, and emitted frames carry
the actual 24000 rate. Pipeline setup accepts either that actual rate or the
configured requested rate; an output transport may resample. Zero is rejected.
Model, voice and language settings update future admissions atomically. Endpoint,
credentials, sample rate and normalization flags are immutable request-template
configuration, not runtime settings-frame fields.

## Words and timing

`speech.chunk` can contain audio, marks, or both; audio is emitted first. Missing
or null audio/marks are allowed. Marks recover spelling from Unicode scalar
indices into the original synthesis text, not UTF-8 byte offsets or UTF-16 code
units. Adjacent fragments merge across events and retain the first timestamp.
Source whitespace, including CPython's extra U+001C–U+001F characters, controls
flushing. An unusable span flushes the prior fragment before falling back to the
trimmed mark value. Non-word mark types are ignored.

`speech.done` does not stop reading. Later marks-only events remain valid, and
EOF flushes a pending word. A final SSE block without a blank-line terminator is
accepted locally without changing the shared SSE parser. One typed `Done` is
emitted after complete EOF, with the maximum done duration and mark end time.
Word offsets and duration are request-relative seconds.

The pipeline enables word reporting only for Speechify. Each utterance opens a
separate native alignment context. Its baseline is the maximum of the observer
clock and the preceding context boundary; request-relative offsets are added
once. Terminal duration advances that boundary over trailing silence without
inventing a word or playback receipt. The optional shared alignment completion
API preserves old behavior when passed `None`; finite, nonnegative offsets and
nanosecond additions are checked. Without audio it never turns cached words or
a duration into audio/playback evidence.

This is a native lifecycle adaptation: Python clears shared cumulative time on
its `TTSStopped` push. Native synthesis generates a stopped frame for each
utterance and keeps the continuous boundary across those utterances. An external
`TtsStopped` is forwarded normally, without resetting the timeline. Interruption,
Cancel, cleanup and successful setup replace the Speechify timeline owner, so a
retiring task cannot advance the next generation. End/Stop drain admitted work
before cleanup; a joined new run starts fresh. The source's stop-reset moment is
not claimed to be identical.

## Bounded native profile and ownership

Only HTTP 200 with the expected event-stream content type is accepted. Requests
are direct HTTP/1.1 with no redirects, automatic retries or environment proxy.
The existing bounded response owner supplies a whole-request deadline, bounded
queue and cancellation on consumer drop. Provider error bodies and credentials
are not included in diagnostics. A failure does not replay already-emitted PCM.

Unlike the source's skip/continue behavior, malformed JSON (including unknown
events), provider errors, non-object events/marks, invalid typed fields,
noncanonical base64 and non-finite/negative timestamps terminate the stream.
Zero timestamps are valid. Present word `value` must be a string even if a valid
span would make Python ignore it; null done duration is rejected rather than
using Python's zero default. Successful EOF requires `speech.done`, nonempty
audio and complete PCM samples. A terminal odd byte is rejected, not padded.
Early PCM and words can already have been emitted before a later failure; the
context then fails, rather than being relabeled successfully completed.

Defaults from `SpeechifyLimits` bound cumulative wire bytes to 128 MiB, inspected
word marks and emitted words each to 65,536, total emitted word UTF-8 bytes to
4 MiB, and request text/individual non-audio text fields to 64 KiB.
`SpeechConfig::max_json_bytes` independently bounds the serialized request and
each raw SSE block, including comments and delimiters. Its `max_audio_bytes`
bounds all decoded PCM, including a retained odd byte. Existing queue capacity,
deadline and HTTP header bounds apply. These are content/queue limits, not a
claim that all transient parser or networking memory equals one such budget.

## Deterministic acceptance

- `speechify_http`: real loopback TCP request headers/body, supported/fallback
  rates, bytewise UTF-8/SSE, fixed Python helper goldens, late marks, independent
  limits, malformed/truncated/error streams and timeout/drop/backpressure closure.
- `speechify_audio_offset` in pipecat-text: old completion behavior, zero offsets,
  no-audio semantics, checked overflow and successive duration boundaries.
- `speechify_tts` in the facade: actual HTTP/SSE through `HttpTtsProcessor`, source
  identity and three text channels, continuous tail duration, settings snapshots,
  true fallback PCM rate, failures, interruption, cancellation and joined reuse.
- `speechify_playback_history` with `speechify,assistant-history`: actual
  HTTP/SSE→TTS→OutputTransport→assistant history. Pending, partial Played,
  provider Done/EOF and a withheld source end cannot commit history. Complete
  controlled Played receipts plus the source end commit the original text once;
  a Discarded final chunk withholds the context. A third case drives actual
  24 kHz fallback PCM through the 32 kHz output resampler, checks the complete
  PCM bytes and 30 ms duration, then applies the same receipt/history gates.

The last tests use explicit synthetic receipts. They establish native receipt
and history rules, not physical acoustic playback, live authentication, hosted
voice quality or production provider behavior.
