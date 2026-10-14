# Native ElevenLabs streaming speech

This crate ports a bounded slice of Pipecat's ElevenLabs WebSocket services from
baseline `af9f9da9d7afddaa8811c0e0251b32566c980bdf`. It uses native Rust TLS,
WebSockets, frame processors, cancellation and text aggregation. It does not run
Python. The existing `pipecat-speech` crate owns the HTTP STT/TTS implementations.

## Supported slice

- Realtime Scribe STT at `/v1/speech-to-text/realtime`, authenticated with
  `xi-api-key`; mono signed little-endian PCM16 audio, manual/VAD commit modes,
  partial/final transcripts, timestamp/language final-pair deduplication,
  source language mapping, keyterms, VAD thresholds, background filtering,
  no-verbatim and provider-side logging controls
- TTS at `/v1/text-to-speech/{voice}/multi-stream-input`, authenticated with
  `xi-api-key`; one context per independently owned socket; init with voice
  settings, incremental text, flush, `close_context`, `isFinal`, keepalive and
  `close_socket` teardown
- TTS PCM16 rates 8000, 16000, 22050, 24000, 32000, 44100 and 48000; audio frames
  split across sample boundaries are reassembled, with an error for a dangling
  final byte. STT supports the same rates except 32000
- Character alignment decoding with original-input preference, nullable
  normalized-alignment fallback, utterance-leading-space stripping, preservation
  of inter-chunk separators, partial-word stitching, final partial-word emission,
  and word timestamps relative to each context's generated audio
- `ElevenLabsSttProcessor` and `ElevenLabsTtsProcessor` integrate with native
  `FrameProcessor`. STT VAD stop commits only in manual mode; TTS interruption
  cancels owned contexts. Both expose configuration snapshots and accept typed
  or mapped service settings updates with atomic validation
- TTS source response-start/end frames retain their identities and metadata.
  End follows all preceding synthesis contexts, including settings rotations;
  provider failure adds `failed: true` to the original end. Generated audio and
  timing events indicate synthesis only, never a playback/Spoken acknowledgement
- An enclosing response retains its failure state across Flow fences and
  `TtsSpeak` context closures, including an end arriving after the context has
  already completed; the next response starts with independent failure state
- Sentence aggregation is the TTS processor default. Token aggregation disables
  provider auto mode. `append_to_context` and transport destination changes
  rotate contexts. Chinese, Japanese and Cantonese timestamp chunks preserve
  source spacing semantics alongside explicit per-aggregate spacing

Use `ElevenLabsSttClient::connect` for a realtime input session, and
`ElevenLabsTtsClient::stream(context_id)` for a synthesis context. Clone its
`sender()` for bounded admission, call `finish()` to drain accepted input, and
consume `next()` through `None` to join cleanup. `cancel()` suppresses queued
output immediately; the next read reports cancellation after bounded cleanup.
If a pending `next()` future is dropped by a timeout or select, retrying it still
joins cleanup and returns the terminal result exactly once.
Dropping a session requests cancellation and leaves only the bounded protocol
cleanup task. Dropping all senders does not implicitly finish while the session
still owns its sender; use `finish()` explicitly.

## Realtime STT input audio and speaker identity

Realtime STT `SttMute` controls apply in both directions before the generic
upstream bypass. The original native control forwarding remains unchanged.
See [STT mute routing](STT_MUTE.md) for source boundaries and verification.

The realtime processor accepts both `InputAudio` and `UserAudio`. The original
downstream frame envelope is forwarded unchanged when audio passthrough is enabled;
that switch covers both input variants, including muted and unusable input.
Upstream audio retains unconditional forwarding. Upstream, output and TTS audio
do not enter the provider input path. Valid nonempty PCM
uses the existing mono/rate validation, base64 JSON wire format and usage metrics.

By default, transcript identity is the latest input speaker consumed by the
session supervisor: `UserAudio` selects its UTF-8 user ID and plain `InputAudio`
resets it to empty. Partial and both supported final transcript variants use
that current identity. This is not per-chunk correlation or provider diarization;
provider word-level `speaker_id` remains in the result object unchanged.
`with_user_id`, including an explicitly empty string, remains a fixed native
override. Ignored per-frame IDs are neither cloned nor charged against its bound.

In dynamic mode, empty input advances identity without sending an audio chunk,
commit or usage metric. An idle processor retains at most one bounded identity
seed without opening a connection. Fixed-override empty input retains its no-op
behavior even when the command queue is full. Empty input retains the geometry
validation bypass. Muted, unusable, invalid, oversized and queue-rejected input
never adopts that frame's ID; health retirement can still discard the previous
identity. The logical lengths of PCM and dynamic UTF-8 ID bytes together must fit
`limits.max_message_bytes`; PCM also retains its independent
`max_audio_chunk_bytes` cap. This is not an allocation-capacity or process-memory
bound: frame envelopes and existing context IDs are outside this budget, and
configured fixed IDs retain their existing uncapped length contract. The lower
session separately checks the actual
base64-plus-JSON message size, so admitted PCM may still fail asynchronously at
that wire bound. No chunk splitting or resampling is added.

Command admission, supervisor consumption and provider socket effects are
separate boundaries. During a healthy settings rotation, the old session drains
with its last consumed identity; queued input changes identity only when the
replacement session consumes it. Health retirement and cleanup discard the idle
seed and queued commands. A newly created actor uses its fixed override or the
latest accepted idle seed, falling back to empty identity; it does not inherit the
retired actor's consumed identity. This bounded generation behavior differs from Python's shared
service-level `_user_id`, including its final callbacks' tracing/usage await
points. There is no automatic recovery or replay: later usable nonempty input or
an existing start-capable control can open a fresh session. HTTP/batch STT in
`pipecat-speech` is outside this realtime slice.

`tests/stt_audio_identity.rs` checks authenticated real loopback WebSockets,
manual commit barriers, raw/base64 message bounds, current-speaker consumption,
fixed overrides, queue rejection, settings drain, cancellation and health
retirement. These checks use synthetic provider responses; they do not establish
live-provider acceptance or exact PCM-to-transcript acknowledgement.

## Bounds and failure handling

`StreamingLimits` bounds input/event queue capacity, WebSocket frame/message
size, and connect/write/response/close times. TTS additionally bounds text and
alignment bytes per context and total decoded audio. STT bounds each raw audio
chunk, encoded message size, admitted session audio and outstanding commits.
Pipeline context count and pending commands are bounded; input rejection returns
backpressure instead of silently overwriting queued data. Credential headers are
sensitive, configuration Debug output is redacted, and provider error bodies are
not echoed. Unencrypted endpoints are accepted only on loopback, for tests.

Once a provider send is attempted, its effects are ambiguous on transport loss.
Nothing is replayed automatically. A failed low-level session is terminal. The
STT processor admits later usable input into a fresh session; authentication or
invalid-request failures require explicit restoration or an effective valid
settings change first. STT settings rotation
finishes the old session before opening a new one, so accepted audio is not
silently resubmitted. Manual finish waits for outstanding commit responses;
VAD finish waits for the provider's final segment and errors on timeout rather
than inventing a final transcript. Provider finals have no request IDs, so VAD
finish cannot certify an exact audio-sample acknowledgement boundary.

## Processor usability and recovery

Both processors gate provider work while core health is unusable and continue
handling lifecycle, settings, errors and passthrough controls. STT audio
passthrough remains configurable independently. Authentication, authorization
and provider invalid-request failures disable work; rate-limit, network and
server errors do not by themselves change usability. A rejected settings
proposal preserves the validated configuration and prior health. Only a
validated effective change restores usability; no-op deltas do not.

Each request/session carries a health revision. Coalesced disable/restore pairs
still retire the old generation: partial sentences and queued PCM are discarded,
old streams are dropped, and late audio/alignment/transcripts are suppressed.
Recovery starts fresh sockets/context IDs and never replays ambiguous remote
effects. TTS retains original response boundaries and failure state across
fences and recovery, marks retired stops/response ends as failed, and never
creates playback/Spoken acknowledgements. Healthy settings rotations retain
graceful draining; effects already emitted cannot be recalled.

TTS reports every completed context with no source audio as a recoverable error.
The third consecutive silent completion marks the service unusable for native
failover and worker policy. `with_max_consecutive_zero_audio_contexts(0)` disables
only that cutoff. Completed audio or explicit `set_usable(true)` resets the
streak; recoverable errors and timeouts count, while cancelled, skipped and
health-retired contexts do not. Invalid and ineffective settings do not reset it.
A restoration-generation token fences late health outcomes without changing
transition revisions or fabricating an observer transition.

Source audio presence is independent from playable PCM: Cartesia explicitly
emitting an empty chunk counts as an audio frame; ElevenLabs WebSocket audio
fields must be nonempty. Empty presence never emits PCM, establishes an alignment
clock, or acknowledges playback/Spoken. The native adapters serialize completion
fences without implementing the source optional frame-processing pause; silent
completion releases those fences and response boundaries normally.
Evidence is admitted only after native message validation. A combined
audio/alignment message with malformed alignment is rejected without audio,
whereas the source appends its audio before processing that alignment.
The source pads a final partial PCM sample at context closure; native clients
reject it, so an odd-byte-only context can count as source audio but native
silence. Source `isFinal` handling ignores audio attached to the final message;
the native client decodes valid final-message audio and resets the streak.


## Explicit limitations and source differences

This is not complete source API compatibility. Shared-socket context
multiplexing, dialogue/multi-speaker synthesis, pronunciation dictionaries,
SSML/phoneme transforms, provider model discovery, compression/MP3/μ-law,
automatic reconnect, source tracing decorators and playout/context-completion
callbacks are not implemented. Unknown settings (including these advanced
surfaces) fail validation. Unsupported TTS model/language pairs and unsupported
sample rates error instead of silently omitting a language or changing rate.
Model-language allowlists are pinned source data, not a claim about current
provider account/model availability.

Every TTS context gets its own socket, so settings updates rotate complete
snapshots rather than mutating a shared provider context. STT sends the explicit
logging boolean, including false. Malformed JSON, alignments and audio fail the
session rather than being silently ignored. Alignment clock advancement covers
all chunks, including chunks with no completed word. Audio/alignment that arrive
on an `isFinal` message are decoded before the final event. No alignment word is
treated as a playback receipt. The provenance bridge associates provider-reported
words with admitted synthesis/display/original text, while receipt-bound spoken
history reconstruction remains separate. These are deliberate robustness
boundaries of the native slice.

## Native text provenance and timing

`TtsTextInputFrame` admits separate synthesis, display and original text without
changing the provider payload. Existing `AggregatedTextFrame.raw_text` is retained
as the original channel, including formatting delimiters. Native text aggregation
announces each promoted segment once; its actual emitted frame ID is the anchor
referenced by word reports and `AggregatedTextProgressFrame`. Anchors themselves
never append to conversation history. Skipped aggregates retain their display,
original text and destination without opening a provider socket.

Character timestamps are still decoded from provider milliseconds into the
client's context-relative seconds. The processor preserves those seconds in
`audio_offset_seconds` and `audio_end_seconds` metadata and converts report starts
with checked nanosecond arithmetic. The first nonempty audio establishes an
absolute pipeline-clock baseline. Reports arriving earlier wait for that baseline;
reports arriving before token-mode sentence promotion also retain their original
timing, including reports split across aggregate boundaries. Context rotations
share a monotonic boundary, while cancellation and health-revision changes fence
the old alignment state. Original-text partitions retain their exact internal
whitespace; only each segment's first appendable part uses aggregate-boundary
spacing, so adjacent complete aggregates remain separated when requested.

Missing reports and successful synthesis with no audio produce explicit
`TtsAlignmentDiagnostic` evidence. They do not fabricate ordinary word/progress
frames. Alignment confidence and provider success are distinct from audible
playback; this processor never creates Played/Spoken receipts and does not extend
`SpokenHistory`. The native output transport rebases pipeline PTS and prevents
future text from blocking PCM queued behind it.

## Verification

`tests/fixtures/source_contract.json` records source fixtures for final-pair
handling and English/Japanese/Chinese alignment splits. Tests use synthetic
providers over actual loopback WebSockets, exercising authenticated handshakes,
wire frames, immutable settings, cleanup, cancellation, backpressure, errors and
preserved pipeline response boundaries. `tests/tts_alignment.rs` adds source-mapped
three-channel provenance, retained raw delimiters, early timing/promotion,
zero-audio diagnostics, exact anchor identity, destination/append rotation and a
real native output-path check. Its recording backend explicitly discards PCM;
no audible playback or live-provider acceptance is claimed. No provider
credentials or live calls are required.

Adversarial regressions include dropped connect/read futures, a surviving sender
after consumer drop, complete 22050 Hz PCM16 keepalives, reversed STT final pairs
with repeated identical utterances, malformed payloads without partial output,
late audio/alignment after interruption, and response failures across completed
or pending Flow fences and standalone synthesis contexts.

From the Rust workspace:

```sh
cargo test --offline -p pipecat-elevenlabs -j2
cargo clippy --offline -p pipecat-elevenlabs --all-targets -j2 -- -D warnings
cargo fmt -p pipecat-elevenlabs --check
```
