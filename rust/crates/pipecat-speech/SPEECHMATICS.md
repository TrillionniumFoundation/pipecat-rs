# Speechmatics HTTP TTS

This optional module implements the pinned Speechmatics HTTP service using the
existing speech HTTP client, bounded response owner, PCM decoder and
`HttpTtsProcessor`. Agent STT remains in `pipecat-speechmatics`; no STT dependency
is needed for HTTP-only synthesis. Enable `pipecat-speech/speechmatics`, or the
facade's `speechmatics` feature for both adapters.

## Source mapping

The source baseline is ProfHepta/pipecat
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`. Source was inspected as text only; no
Python code, SDK, tests or scripts are imported or executed.

| Source | Native behavior |
| --- | --- |
| `services/speechmatics/tts.py:40–50,63,123–156` | Typed voice/rate/max-retries defaults and immutable per-utterance settings |
| `tts.py:172–208,291–307` | Bearer POST `/generate/{voice}`, JSON `{text}`, query output format and application identity |
| `tts.py:211–246`, `utils/network.py:11–33` | 503-only retry loop and bounded exponential delay |
| `tts.py:249–280` | Only 200 succeeds; fragmented mono PCM16 byte assembly |
| `tts.py:282–288`, native core/speech contracts | Typed terminal failures, stream cancellation, processor ownership, fences and metrics |

The official [TTS quickstart](https://docs.speechmatics.com/text-to-speech/quickstart)
documents the preview endpoint, bearer request, voice path, JSON text and
`pcm_16000`: signed 16-bit, mono, little-endian raw samples. The pin and this
adapter's endpoint contract do not establish current account availability or
hosted quality. No live-provider calls are part of deterministic validation.

## Request contract

`SpeechConfig::speechmatics(api_key)` selects
`https://preview.tts.speechmatics.com`. A caller-provided base path is retained.
The voice is encoded as one path segment; reserved characters and Unicode cannot
introduce a query or another path segment. Empty voices, dot segments and voice
strings above 1024 bytes are rejected. Defaults are voice `sarah`, sample rate
16000 and `max_retries = 5`.

The query contains exactly `output_format=pcm_RATE` and
`sm-app=pipecat-rust/<crate version>`. The application identity deliberately names
the native implementation, rather than claiming to be the Python package. Any
positive rate is passed through, matching the source's warn-but-send behavior;
the native client does not emit that Python log warning. Only 16 kHz hosted output
is documented. Other rates in fixtures verify wire/metadata handling, not hosted
support. There is no resampling or rate relabeling.

`SpeechmaticsSynthesisRequest` snapshots text, voice, rate and retry limit.
`TtsSettings.voice` and integer `TtsSettings.base.extra["max_retries"]` update only
future processor admissions, atomically. The untyped settings-frame mapping also
accepts `max_retries`. Null/negative/fractional/out-of-range retry settings and
unsupported model/language/extra fields fail without applying any part of the
delta. No-op or invalid changes do not revive unusable health. Retry range is
0–32, a deliberate finite restriction on Python's unconstrained integer setting.
Credentials, rate and endpoint are not runtime-updatable.

## Retry and ownership

The source increments its attempt counter before comparing `attempt >=
max_retries`. Accordingly, 0 and 1 both make one request, 2 permits two requests,
and the default 5 permits five requests total. Only HTTP 503 retries. Delays are
250, 500, 1000, 2000, 4000 and then 8000 milliseconds, capped at eight seconds.
`Retry-After` is not interpreted. Error bodies are dropped before waiting and are
never read or exposed. Recovered 503 attempts do not emit intermediate pipeline
`ErrorFrame`s: this is an explicit narrowing of the source's per-attempt errors.
The final failure remains observable as `SpeechError::Http(503)`.

One overall `SpeechConfig.timeout` covers all attempts, retry waits, response
reads and waits for stream-consumer capacity. There is no retry for transport
failure, malformed response, deadline or any response-body error. No request can
be replayed after audio is emitted. HTTP protocol automatic retries, environment
proxies and redirects are disabled for this provider. HTTP/1 is selected to keep
the parser's finite header bounds explicit. Other providers retain their existing
transport policy.

Dropping a stream aborts its owner; explicit cancellation discards buffered chunks
and reports one terminal cancellation. `HttpTtsProcessor` adds ordered start,
audio, stop, response and flush fences through its existing owned runtime tasks.
Interruption cancels accepted interruptible work. Health retirement, including a
coalesced disable/restore, prevents old and queued work from adopting fresh
output authority. End/Stop drain; Cancel/drop tear down owned requests. Restoring
health does not replay text.

## Format and bounds

- JSON text is checked before and after serialization against `max_json_bytes`,
  so escaping cannot bypass the bound. URL/key inputs are capped at 4096/8192 bytes
- Response headers are checked before status/retry handling: at most 64 entries
  and 16 KiB of normalized name/value bytes, counting repeated values. Before
  this semantic check, locked Hyper 1.12.0's HTTP/1 parser uses its separate finite
  limits of 100 headers and a 417792-byte read buffer. The 16 KiB limit is not a
  pre-allocation parser limit
- Only 200 accepts audio. A 201/204/206 response is a provider-status failure
- A known Content-Length exceeding `max_audio_bytes` fails before PCM emission;
  all streamed bytes, including a pending odd tail, count against that same bound
- PCM chunks are reassembled across arbitrary byte boundaries. Empty and odd-tail
  responses fail. Empty responses use `SpeechError::Protocol("empty PCM response")`,
  preserving other backends' source-compatible `EmptyAudio` completion behavior
  while preventing a Speechmatics success diagnostic. Python silently drops a final odd byte; native rejection is
  intentionally stricter. Previously emitted whole samples remain partial output,
  not successful completion, when a later byte, body read or deadline fails
- Missing Content-Type is allowed for compatible endpoints. Explicit raw types
  are `audio/pcm`, `audio/x-pcm`, and `application/octet-stream`; contradictory
  rate, channel, bit-depth, encoding or endianness parameters fail. `audio/l16`
  (network byte order), containers, compressed MIME and nonidentity content
  encodings are rejected. Duplicate Content-Type fields fail closed
- Raw PCM has no intrinsic magic or sample-rate tag; declared-format checks do
  not prove that arbitrary unlabeled provider bytes are genuine speech
- Errors and Debug output do not expose URLs, credentials, text or provider error
  bodies. A partially emitted response must always be drained to terminal status

## Output evidence and remaining scope

Source-audio watchdog evidence begins only when at least one complete sample is
admitted. Empty/odd-only responses remain silent failures. Successful HTTP
completion creates generated anchors and `Unreported` alignment diagnostics,
never `TtsText`, playback receipts or spoken-history commits. Output tests use the
real native `OutputTransport`, recording PCM and returning `Discarded`; those
submissions do not establish physical audibility.

No word timestamps, voice discovery, WAV/compressed output, voice cloning,
arbitrary headers, source event callbacks, SDK wrapper, live-provider test,
complete Speechmatics bot, runner/scaffold selection or full provider parity is
included. Explicit non-16kHz configuration remains caller responsibility.

## Test inventory

Tests use actual loopback TCP/HTTP and synthetic credentials:

- `speechmatics_http.rs`: exact encoded request/identity, byte-fragmented PCM,
  nondefault-rate metadata, 0/1/2/5 attempt limits, recovery, non-503 failures,
  strict PCM/MIME/header/JSON/audio limits, socket and retry deadlines, redirects,
  isolated-process environment proxy policy, cancellation/drop and redaction
- `speechmatics_processor.rs`: ordered lifecycle/metrics, request failure,
  passthrough/skip, interruption and queued retirement, finite admissions, output
  rate mismatch, uninterruptible flush fences, health recovery, atomic voice and
  retry snapshots, Cancel/drop and Stop drain
- `speechmatics_health.rs`: coalesced disable/restore retires in-flight and queued
  requests while retaining original response fences and admitting a fresh turn
- `speechmatics_output.rs`: real output submissions without spoken claims, blocked
  output write cancellation and bounded End settlement
- Facade `speechmatics_tts_feature.rs`: optional STT/TTS exports and construction

These tests are deterministic protocol and lifecycle evidence only. Their results
must be qualified on the exact immutable source revision with the workspace's
locked offline native validation procedure.
