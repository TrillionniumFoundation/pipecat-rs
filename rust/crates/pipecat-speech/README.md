# Native REST speech

Native asynchronous HTTP clients for the speech endpoints in this checkout. No
Python runtime or provider SDK is required. These are provider-specific protocols,
not interchangeable OpenAI-compatible presets.

## Source coverage

The source baseline is `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

| Rust client | Python source / service | Implemented wire contract |
| --- | --- | --- |
| `SpeechClient` | `services/openai/stt.py`, `tts.py` | Bearer-authenticated WAV multipart `/audio/transcriptions`; `/audio/speech` JSON, streamed raw PCM16LE at 24 kHz mono |
| `ElevenLabsHttpClient` | `services/elevenlabs/stt.py::ElevenLabsSTTService` | `xi-api-key`, WAV `audio/x-wav` multipart `/v1/speech-to-text`; `model_id`, service-native `language_code`, optional `tag_audio_events`, repeated `keyterms`, and `no_verbatim` |
| `ElevenLabsHttpClient` | `services/elevenlabs/tts.py::ElevenLabsHttpTTSService`, `tts_base.py` | `xi-api-key`, `/v1/text-to-speech/{voice}/stream/with-timestamps`; NDJSON, base64 PCM, raw and normalized character alignments; model, language, voice controls, prior text, dictionary locators, normalization, logging and latency options |
| `DeepgramHttpClient` | `services/deepgram/tts.py::DeepgramHttpTTSService` | `Authorization: Token`, `/v1/speak`, JSON text, voice/encoding/rate/container/speed/MIP query parameters; raw Linear16 or decoded G.711 |
| `SpeechmaticsHttpClient` (feature `speechmatics`) | `services/speechmatics/tts.py` | Bearer POST `/generate/{voice}`, `{text}` JSON, `output_format=pcm_RATE`, native `sm-app`; streamed mono PCM16LE and bounded 503-only retries |
| `CartesiaHttpClient` | `services/cartesia/tts.py::CartesiaHttpTTSService` | `X-API-Key`, pinned `Cartesia-Version: 2026-03-01`, `/tts/bytes`; model, transcript, `{mode: id, id: ...}` voice, language, generation controls, dictionary, raw PCM16LE format |

There is **no HTTP STT service in `services/deepgram/stt.py`**. Its STT
implementation uses WebSocket `listen.v1`. This crate does not invent a REST
replacement for that source. Native Deepgram and Cartesia streaming clients and
more extensive streaming frame processors live in `pipecat-streaming-speech`.

## Client APIs

The shared `SpeechConfig` constructors select a provider base URL and transport
limits. The provider client selects the protocol. For local tests, set `base_url`
to a loopback HTTP server. An optional base path is preserved. Voice IDs are
encoded as a single URL segment, including slashes, percent signs and Unicode;
`.` and `..` are rejected.

```rust,no_run
use pipecat_speech::{elevenlabs::*, SpeechConfig};

# async fn example() -> Result<(), pipecat_speech::SpeechError> {
let client = ElevenLabsHttpClient::new(SpeechConfig::elevenlabs("api-key"))?;
let mut request = ElevenLabsSynthesisRequest::new("Hello", "your-voice-id");
request.sample_rate = 24000;
request.language_code = Some("en".into());
let mut stream = client.synthesize(request)?;
while let Some(event) = stream.next().await {
    match event? {
        ElevenLabsEvent::Audio(audio) => { /* deliver audio.pcm to an output */ }
        ElevenLabsEvent::Alignment { alignment, normalized_alignment } => {
            /* request-relative character times, not playback acknowledgments */
        }
    }
}
# Ok(()) }
```

`SpeechClient::transcribe_pcm` and
`ElevenLabsHttpClient::transcribe_pcm` take a complete PCM16LE utterance, its true
sample rate and channel count, a typed request and a `CancellationToken`. They
validate interleaved sample alignment and construct a WAV upload. ElevenLabs STT
uses provider-native ISO 639-3 language codes (`eng` by default); its typed response
preserves text, detected language/probability and word timing/speaker fields. It
does not request diarization or claim full diarization support.

ElevenLabs TTS uses typed `ElevenLabsEvent` values. HTTP chunk boundaries may split
JSON records, Unicode, base64 data or decoded PCM samples. An entire record is
parsed and validated before its audio is emitted. Alignment arrays must have
matching lengths, ordered nonnegative starts and ends at or after each start.
The optional final NDJSON record may omit its trailing newline. Both raw and
normalized alignments are exposed; callers choose the appropriate one. Prior text
is omitted for `eleven_v3` and `eleven_v3_conversational`, as in the source.
Unsupported language/model pairs fail early instead of silently omitting language.
Request state is explicit: this low-level client does not accumulate prior text
between independent synthesis calls.

All `SpeechChunk` output is mono signed PCM16LE at the declared rate. Deepgram
`Mulaw` and `Alaw` responses are decoded, not relabeled as PCM. Rates are validated
against the provider's raw-output combinations: Linear16 at 8/16/24/32/48 kHz,
G.711 at 8/16 kHz. Deepgram's provider-specific `audio/l16` response is handled as
its documented Linear16 little-endian bytes. See the official
[media output settings](https://developers.deepgram.com/docs/tts-media-output-settings).
ElevenLabs PCM supports 8/16/22.05/24/32/44.1/48 kHz from `tts_base.py`.
Cartesia uses raw PCM16LE and accepts positive sample rates, matching the source's
configuration contract; provider acceptance of every positive rate is not implied.
No implicit resampling, channel conversion or compressed-audio relabeling occurs.

## Pipeline processor

`HttpTtsProcessor` implements `pipecat_core::FrameProcessor`. Construct it with
`HttpTtsBackend::{OpenAi, ElevenLabs, Deepgram, Cartesia}` (plus feature-gated `Speechmatics`) and a positive pending
utterance limit. The backend contains a client and typed request template; incoming
text overrides only the template text field. Templates may use an empty text field.

- Downstream `TtsSpeak`, `AggregatedText` and typed `TtsTextInput` become sequential
  synthesis requests
- Skipped aggregates and typed provenance inputs stay ordered behind preceding
  synthesis without provider requests; existing skipped aggregates retain their
  identity and typed skipped inputs become
  display aggregates with their original source intact
- Incremental `Text`/`LlmText` pass through without synthesis; place a text
  aggregator before this processor when using incremental LLM output. Unless
  explicitly marked `skip_tts`, they retain the same downstream FIFO as other
  normal frames. Explicitly skipped incremental text keeps its immediate bypass
- Upstream frames preserve their direction and do not trigger synthesis
- Requests use owned core background tasks; frame processing never awaits HTTP
- Pending utterances and ordering fences each have the configured count limit
- All other normal downstream frames, including response starts/ends and flush
  probes, join the same FIFO behind accepted synthesis. Original envelopes and
  local capabilities are forwarded intact. System and upstream frames retain
  their immediate routes; terminal drain remains owned by the core runtime
- Generic frames route synchronously when no live predecessor remains, without
  consuming a pending slot or background task. Thought/control bursts therefore
  do not depend on task scheduling to make progress. With live predecessors they
  retain the same bounded FIFO; response ends, flush probes and skipped aggregates
  keep their existing owned-task paths even when idle
- Uninterruptible forwarding fences retain every prior surviving dependency
  across interruption, even if an intervening interruptible task is canceled.
  Each dependency snapshot contains at most the two bounded pending pools.
  Outstanding snapshots, including cancellation in progress, remain bounded by
  the core background-task limit (quadratic worst-case dependency references).
  These are count/reference limits, not a new byte or RSS limit for arbitrary
  forwarded payloads and metadata
- End drains accepted work; interruption cancels active/waiting utterances and
  drops stale audio; Cancel and cleanup cancel all owned work
- Output sample rate must match the pipeline's configured rate at setup
- Emits `TtsStarted`, `TtsAudio`, `TtsStopped`, optional TTFB and usage metrics,
  and nonfatal upstream typed errors. Failed responses mark `LlmResponseEnd`;
  canceled, rejected or failed queued work marks its captured response generation
  before releasing completion fences, without changing a later response's status.
  Idle forwarding errors also mark the captured response before returning; output
  backpressure remains fatal and does not imply worker recovery
- Audio carries context and source-frame IDs. Synthesis completion does not mean
  that the output transport has played the audio

This is a narrow native passthrough-order repair, not complete source
`TTSService.process_frame` equivalence. In the pinned Python source,
`skip_tts=true` bypasses serialization for the Text family, including aggregated
text. Native skipped aggregates and `TtsTextInput` retain their existing ordered
provenance/alignment behavior; only incremental `Text`/`LlmText` retain the
explicit immediate skip bypass here. Native incremental text is not implicitly
aggregated or synthesized by this processor.

### Provenance and completion evidence

The processor uses `pipecat-text::TtsAlignment` and `TtsAlignmentFrames`, mapped
from `services/tts_service.py::_synthesize_text` and the source's aggregate/token
sequencers. `TtsTextInput` carries separate synthesis, display and original-source
channels. Only synthesis text reaches the provider. Ordinary `AggregatedText`
retains `raw_text` (including source delimiters), with its display text used for
synthesis. Complete utterances each own a fresh immutable context, so later
changes to append policy, transport destination or explicit spacing cannot alter
accepted work. Context identities identify synthesis transactions independently
of source-frame IDs, including repeated submissions of the same source frame.
Prepared-input text channels, aggregation labels and destinations are bounded
before admission, including skipped inputs.

Generated display anchors use `append_to_context = false`. Every successful HTTP
request produces an explicit `TtsAlignmentDiagnostic` with `Unreported` evidence
and `Succeeded` synthesis outcome. Its segment ID is the exact emitted anchor
frame ID; its source span counts Unicode scalars. Anchors, diagnostics and
completion boundaries share the pipeline observer clock, and later contexts do
not regress behind previous boundaries. Source frame IDs and destinations remain
attached to generated provenance and audio.

HTTP completion never produces `TtsText`, progress or a spoken-history claim.
This deliberately separates Python's completion-time text emission from audible
playback evidence. ElevenLabs HTTP raw character alignment remains available only
through its client stream; the processor does not turn it into word reports.
Failed, interrupted or health-retired requests never force textual completion.
The bounded alignment bridge validates provenance before submitting requests;
invalid mappings fail closed without echoing source content in errors.

Targeted `TtsUpdateSettings` and `ServiceUpdateSettings` frames, or
`update_settings()`, atomically update model, voice and supported language fields
for future requests. OpenAI does not accept language; Deepgram's model and voice
fields both select its voice model and cannot conflict. Other runtime fields,
credentials and output-format changes are rejected. It does not perform
sentence aggregation, VAD,
utterance segmentation, voice cloning, pronunciation transforms, callbacks,
general retry/failover (Speechmatics alone retries pre-audio 503 responses), cross-request prosody state, provider word-report
conversion or LLM-context updates. Those features require explicit integration,
not guessed provider equivalence.

## Processor usability

Provider authentication, authorization and invalid-request failures mark the
processor unusable through core's typed error policy. Transient connectivity,
rate-limit and server failures retain current health. Speech work is discarded
while unusable; lifecycle, errors, settings, passthrough frames and ordering
fences continue. A validated effective settings change restores future work.
Invalid proposals and no-op changes preserve the active configuration and its
previous usability.

Every queued utterance captures a health revision. A transition retires
in-flight and queued old-generation synthesis, including coalesced disable/restore
pairs. Recovery starts a new request and never replays discarded or ambiguously
submitted text. Retired synthesis marks its `TtsStopped` and original
`LlmResponseEnd` with `failed: true`; it does not emit usage success or claim
playout. Effects emitted before retirement are not recalled.

## Bounds and safety

- HTTPS is required except for loopback HTTP; userinfo, query and fragment in a
  base URL are rejected, as are empty/invalid header credentials
- Redirects are disabled, so authentication cannot follow a redirect to another
  destination. The Speechmatics feature retries only pre-audio 503 responses;
  other clients have no application-level retry loop
- `max_audio_bytes` limits decoded PCM and WAV uploads; G.711 expansion counts
  toward the decoded limit
- `max_json_bytes` bounds a transcription JSON response or one ElevenLabs NDJSON
  record, including its newline. Total ElevenLabs response bytes additionally
  cannot exceed `4 * max_audio_bytes + max_json_bytes`
- Queues are bounded. The timeout covers the whole response and time blocked on
  a slow consumer. Cancelling or dropping a stream cancels its owned request;
  explicit cancellation discards buffered events
- Incomplete PCM samples, empty audio, invalid base64/JSON/timing and truncated
  HTTP bodies are errors. A stream may deliver partial audio before a later error;
  callers must drain to terminal status and never assume first audio means success
- Explicit incompatible content types and conflicting `rate`/`channels` parameters
  are rejected. Missing content types remain allowed for local compatible endpoints
- Errors exclude HTTP response bodies, URLs and API keys. Configuration Debug
  redacts secrets; JSON-error Debug/Display does not echo provider-controlled text

Remaining provider features include OpenAI Realtime/audio modalities, ElevenLabs
Realtime/WebSocket and dialogue services, full STT response metadata and speaker
configuration, Cartesia alternative containers/encodings/extra headers, Deepgram
compressed containers/codecs, full base speech-service settings/events and live
provider qualification. The API deliberately has no raw arbitrary-header override
that could replace authentication or wire-format headers.

## Verification

Tests use real loopback HTTP sockets with synthetic PCM and provider responses.
They cover exact request auth, paths, query and JSON/multipart shapes, encoded voice
IDs, stream chunk boundaries, stereo STT WAV input, G.711 decode, character timing,
settings validation, malformed/truncated data, bounds, cancellation, deadlines,
redirect policy, secret redaction, pipeline ordering/lifecycle, interruption,
backpressure, rate mismatch, metrics and directionality. Source-mapped alignment
tests cover transformed Unicode provenance, exact anchor identities, raw
delimiters, destination/append/spacing isolation, ordered skipped text, bounded
admission and failure without success diagnostics. A real HTTP → processor →
`OutputTransport` test records PCM submissions while returning `Discarded` rather
than a synthetic `Played` receipt; diagnostics survive and spoken history stays
empty. No audible-playback or paid/live provider acceptance is established by
these tests.

```sh
cargo test -p pipecat-speech --offline --locked
cargo clippy -p pipecat-speech --all-targets --offline --locked -- -D warnings
```

## Completed-context audio watchdog

The default cutoff is three consecutive completed contexts without source audio.
Every silent completion emits one recoverable context error; the cutoff error
instead marks the service unusable for failover and worker Continue/End/Cancel
policy. `with_max_consecutive_zero_audio_contexts(0)` disables only the cutoff.
An audio completion or explicit `set_usable(true)` clears the streak, including
an already-usable true setter. Effective settings changes restore usability;
invalid and no-op changes do not. A restoration-generation token prevents an
old outcome from poisoning newly restored health without changing transition
revision or observer semantics. Cancelled, skipped and retired contexts do not
count. A recoverable provider error or timeout completing without audio emits
its provider error and the silent-context error; a permanent provider error
already made the service unusable, so it is not counted again.

The watchdog follows source audio-frame presence rather than PCM byte count.
Cartesia HTTP emits one source audio frame even for an empty HTTP 200 body;
ElevenLabs HTTP emits one for an explicit empty `audio_base64` value. OpenAI and
Deepgram empty response bodies do not produce source audio frames. The public
bounded clients retain their `EmptyAudio` validation errors, while the pipeline
adapter interprets empty successful exhaustion using these source distinctions.
Cartesia's native HTTP client streams whole PCM samples before the response body
finishes; its source adapter awaits the complete body. If a response later fails,
audio already emitted by the native client remains audio evidence and resets
the streak, whereas the source adapter would have emitted no frame. Strict
native content-type, malformed-payload and partial-sample rejection also remain
in effect; the watchdog does not make rejected bytes playable.
The source pads a final partial PCM sample when closing its audio context;
native clients reject it. Speechmatics instead silently discards its final odd
byte in the pinned source; its native client rejects that tail. An odd-byte-only response can therefore be source
audio while remaining native silence.
ElevenLabs HTTP validates combined audio/alignment records before admitting
their audio evidence, while the source yields audio before validating alignment.
An invalid combined record therefore counts as native silence even if it contains
an otherwise valid audio field.
The other native HTTP clients accept successful 2xx statuses; Speechmatics
requires exactly 200. An empty
non-200 body has no Cartesia source-frame evidence and reports silence; source
adapters requiring exactly 200 also report their provider-status error.
Presence evidence is internal: it does not produce an empty PCM frame, start
alignment clocks, fabricate word reports, or acknowledge Played/Spoken output.
The native adapter does not implement the source optional input-processing pause;
its tracked completion fences release normally after silent completion.

`tests/zero_audio.rs` exercises local HTTP defaults, reset/disable semantics,
provider payload distinctions, interruption/retirement, native failover without
replay, and Continue/End/Cancel policies. No live provider calls are required.


## Optional Speechmatics HTTP synthesis

Enable `pipecat-speech/speechmatics` for `speechmatics::{SpeechmaticsHttpClient,
SpeechmaticsSynthesisRequest}` and `HttpTtsBackend::Speechmatics`. The facade's
`speechmatics` feature also enables this module as `pipecat::speechmatics_tts`,
alongside its existing Agent STT module. No new dependencies enter the default
closure. See [SPEECHMATICS.md](SPEECHMATICS.md) for the pinned source mapping,
retry/format policy and qualification limits.

## Optional Speechify timestamped HTTP synthesis

Enable `pipecat-speech/speechify` (or the facade's `speechify` feature) for
`speechify::{SpeechifyHttpClient, SpeechifySynthesisRequest, SpeechifyEvent}` and
`HttpTtsBackend::Speechify`. It preserves cross-event original-text words, PCM
and terminal audio duration through the existing TTS pipeline. The feature is
default-off with no new dependencies. See [SPEECHIFY.md](SPEECHIFY.md) for the
fixed source, bounded native profile, timeline adaptation and loopback acceptance.

## Optional MiniMax HTTP synthesis

Enable `pipecat-speech/minimax` (or the facade's `minimax` feature) for
`minimax::{MiniMaxHttpClient, MiniMaxSynthesisRequest}` and
`HttpTtsBackend::MiniMax`. This default-off adapter decodes bounded streaming
hex PCM and reuses the existing TTS lifecycle without claiming word timestamps.
See [MINIMAX.md](MINIMAX.md) for the fixed source, corrected EOF/error behavior,
final aggregate handling and synthetic loopback qualification.

## Optional Deepgram WebSocket utterances

`deepgram-ws` adds the explicit `HttpTtsBackend::DeepgramWebSocket` variant and
`deepgram_ws::DeepgramWsClient`. Despite the scheduler's historical HTTP type
name, this variant performs a real WebSocket Speak/Flush/PCM/Flushed exchange.
It uses one owned socket per complete utterance, with bounded Clear/Close and
joined cleanup; it does not provide persistent token streaming or word timing.
See [DEEPGRAM_WS.md](DEEPGRAM_WS.md) for the fixed-source contract and limits.
