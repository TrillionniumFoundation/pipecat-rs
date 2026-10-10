# Streaming speech

Native Rust WebSocket implementations of Deepgram Listen v1 STT and Cartesia
streaming TTS. No Python process, provider SDK, credentials, or paid service is
needed for the loopback integration suite.

## Interfaces

- `DeepgramConfig::new(api_key)` and `DeepgramClient::new(config)` configure a
  mono 16 kHz signed-16-bit PCM recognizer. `connect().await` completes the
  authenticated handshake. `DeepgramSession::sender()` returns a cloneable
  producer for `send_audio`, `finalize`, and `finish`; `next().await` streams
  results. Finalize does not close the session. Finish sends CloseStream, drains
  final transcripts/metadata, and requires the peer to close before the deadline.
- `CartesiaConfig::new(api_key, voice_id)` and `CartesiaClient::new(config)`
  configure raw mono 24 kHz PCM synthesis. `stream(context_id)` opens one socket
  for one context. Its sender accepts incremental `send_text` continuations and
  `finish` sends `continue=false`. `next().await` yields audio, word timestamps,
  flush acknowledgements, and a final Done event.
- `cancel()` is sticky, discards buffered events, and bypasses full data queues.
  Dropping a session also cancels it. Cartesia sends a best-effort context cancel
  before bounded socket cleanup. A cancelled stream emits one cancellation error
  and then terminates. Sender clones do not keep a dropped consumer alive.
  Dropping a pending `next()` future does not consume the terminal result;
  retrying it still joins cleanup and reports that result exactly once.

`try_send_audio`, `try_finalize`, `try_send_text`, and `try_finish` never wait for
queue capacity. They return an explicit backpressure error. Async producers
wait only until the configured write deadline. Input and output queues, wire
message sizes, and Cartesia total text/audio are bounded. Handshake, writes,
provider drain, and cleanup have independent deadlines. Deepgram listening is
long-lived and sends KeepAlive every five seconds; silence is not itself an
error. Slow consumers cannot hold a cancelled or timed-out actor alive.

Both clients support validated `wss://` endpoints and loopback-only `ws://`
endpoints for tests/local gateways. Authentication is in sensitive HTTP headers,
never URL query parameters. Handshake redirects are rejected. Error strings and
configuration Debug output exclude credentials and upstream response bodies.

## Pipeline processors

`DeepgramSttProcessor::new(client)` forwards audio by default and emits final or
interim transcription frames, preserving provider results, language, user ID,
and UTC ISO-8601 timestamps. It honors STT mute, finalizes on VAD stop in either
direction, survives conversational interruptions, and closes/drains on End.
`with_audio_passthrough(false)` consumes input PCM instead of forwarding it.
Typed `SttUpdateSettings` and generic `ServiceUpdateSettings` frames validate
their complete prospective delta before changing the configuration. Named and
local processor targets are respected; untargeted settings are consumed. The
legacy settings mapping is also accepted. `config_snapshot()` returns an owned
snapshot, and `update_settings()` admits a typed delta without waiting for I/O.

Settings reconnects are deferred after VAD start until `UserStoppedSpeaking`,
which denotes completion of the full user turn. VAD stop only finalizes the
existing connection. The old socket's final results drain before a replacement
socket opens. Audio arriving during this work stays in the bounded supervisor
queue and is submitted, in order, when the new socket is ready.

Established sockets recover after transient transport failures. Configure this
with `with_reconnect_policy`; its default permits three recovery attempts, with
100 ms exponential delay capped at two seconds. Ten seconds of connection uptime
resets the rapid-failure budget. These bounded native defaults differ from the
source's four-second-minimum backoff and five-second stability threshold.
Handshakes rejected with 4xx, invalid protocol data, and provider errors without
an explicit 5xx status do not retry; initial setup handshakes fail directly. Set `max_attempts=0` to disable
automatic recovery. Exhausting a nonzero budget explicitly marks the processor
unusable while retaining the Connectivity error category; later audio cannot
silently reset that budget. An effective settings change or explicit restoration
admits a fresh generation. Cancel/Drop abort pending reconnect handshakes.

The submission boundary is admission to the low-level socket actor. Audio
already admitted there is never replayed after a disconnect: the provider may
already have received it, and there is no acknowledgement proving otherwise.
Audio still waiting in the supervisor queue is retained; if recovery fails,
the error reports its unsent PCM byte count. Queue overflow is an explicit
backpressure error. This is not an exactly-once transcription guarantee.

`CartesiaTtsProcessor::new(client)` streams LLM text into a context per response;
`TtsSpeak` creates a standalone context. Contexts are FIFO-serialized and their
total pending count is bounded by `limits.queue_capacity`. Provider I/O
runs in interruption-aware pipeline-owned tasks, outside `process`. End/Stop
drain accepted work, while Cancel/Interruption cancel current and queued work.
The original LLM response-end frame is held until synthesis is finished, after
TtsStopped, including provider-error paths. `skip_tts` text passes through.
Finalization uses a separate barrier so even a full text queue can finish its
accepted work. Changing `append_to_context` rotates the context rather than
applying the first text frame's choice to later text.
An enclosing response retains synthesis failure across Flow fences and
`TtsSpeak` context closures, even if its original end arrives after the context
has completed. Subsequent responses have independent failure state.

An outbound `FlushDetailed` with `origin="pipecat.flows"` and `returning=false`
closes the current utterance, including partial sentence text, and queues a
nonblocking FIFO fence. It forwards the original frame after earlier synthesis
has finished or been canceled. Returning markers pass through. Fence tasks are
bounded and survive conversational interruption; they do not acknowledge audio
playback, which still requires the output transport's genuine playout fence.

`TtsUpdateSettings` and generic settings frames support atomic typed updates,
legacy mappings, and service targeting. Model, voice, language,
`generation_config`, and `pronunciation_dict_id` are runtime-updatable. Each
accepted change flushes pending text under the old settings and starts a new
context with an immutable settings snapshot. This is deliberately stricter than
the source, which rotates only for model/voice/language changes. An LLM response
can therefore span multiple contexts; its final response-end barrier follows all
of them, even when its last context is empty or an earlier one failed.

`with_text_aggregation_mode(TextAggregationMode::Sentence)` enables native
sentence aggregation, including lookahead, abbreviations, language selection,
and protected `<spell>` spans. The adapter retains Token mode by default for
incremental API compatibility; the Python service defaults to Sentence mode.
Both modes bound accumulated text, flush at context/response/end boundaries,
and discard partial text on interruption. Pre-aggregated text and `TtsSpeak`
bypass aggregation. No socket opens for an empty or discarded partial context.

TtsStarted, TtsAudio, TtsText, and TtsStopped carry a shared context ID. TtsText
`pts` is a nanosecond offset **relative to that context's audio**, not an absolute
pipeline clock. Metadata includes `audio_offset_seconds`, optional
`audio_end_seconds`, and the context's initial `source_frame_id`. These offsets
represent generated speech; this processor never sets `metadata.spoken=true`.
Only an output transport with genuine playout acknowledgement can confirm which
words were heard. Provider SSML tags are stripped from timestamp text, and
Chinese/Japanese text preserves inter-frame spacing semantics. PCM split across
provider chunk boundaries is reassembled; a final partial sample is an error.

## Processor usability and recovery

Both adapters gate provider work using core processor health. Authentication,
authorization and active-request validation failures are permanent; connectivity,
429 and 5xx failures remain transient. Rejected settings proposals preserve the
previous configuration and health. A validated effective change restores
usability; an invalid or no-op delta does not. Settings, lifecycle, errors and
passthrough controls remain actionable; STT audio passthrough still applies to
discarded input.

Health revisions retire provider generations, including coalesced disable/restore
pairs. STT closes the old socket and discards its queue and late transcripts;
subsequent admitted audio opens a fresh socket without replaying retired audio.
TTS discards buffered text and drops the old stream, suppressing late audio and
alignment. The next context uses a fresh ID and socket. Original response
boundaries and Flow fences still drain in order; retired synthesis marks both
`TtsStopped` and its enclosing `LlmResponseEnd` as failed. No playback or Spoken
acknowledgement is manufactured. Effects already emitted cannot be recalled.

Healthy settings changes retain graceful drain-and-rotate behavior. Recovery
discards the retired generation rather than flushing old partial text or
resubmitting old PCM. This maps source STT/TTS usable work gates and AIService's
effective-change restoration onto bounded native ownership; it does not repair
credentials or retry TTS effects automatically.

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
Source context closure pads a final partial PCM sample, whereas native clients
reject it. An odd-byte-only context can therefore count as source audio but
native silence.


## Source mapping and scope

- `src/pipecat/services/deepgram/stt.py`: defaults and `_build_connect_kwargs`,
  `run_stt`, `_on_message`, `_keepalive_handler`, `_disconnect`, and VAD finalize
  map to `deepgram.rs` and `DeepgramSttProcessor`.
- `src/pipecat/services/cartesia/tts.py`: pinned API version, `_build_msg`,
  `_connect_websocket`, `flush_audio`, `_process_messages`, timestamp cleanup,
  and context cancellation map to `cartesia.rs` and `CartesiaTtsProcessor`.
- `src/pipecat/services/stt_service.py` / `tts_service.py`: the processors cover
  PCM framing, lifecycle, bounded context IDs, interruption, typed settings,
  VAD-safe reconnect supervision, sentence aggregation, basic usage/TTFB
  metrics, and nonblocking network work. They do not claim complete base-service
  parity. `settings.rs` and `stt_processor.rs` implement typed deltas and STT
  supervision; `processor.rs` owns TTS response/context barriers.
- Provider protocol references:
  [Deepgram Listen v1](https://developers.deepgram.com/reference/speech-to-text/listen-streaming)
  and [Cartesia WebSocket TTS](https://docs.cartesia.ai/api-reference/tts/websocket).

Cartesia pins `2026-03-01`, the Python service's API contract, rather than applying
the newer API's changed request schema. Defaults are `nova-3-general` and
`sonic-3.6`. Deepgram list options use repeated URL-encoded query fields.

Deliberate boundaries: low-level clients remain one-socket sessions with no
automatic replay or reconnect; reconnect supervision belongs to the STT
processor. Cartesia uses a fresh socket per context and does not implement
shared-socket multiplexing or reconnect/replay of failed synthesis. Unsupported
settings, credential/endpoint changes, and runtime audio-format changes are
rejected. Phoneme timestamps, resampling, compressed codecs, playback-aligned
word clocks, the full aggregated-frame sequencer, text-transform callbacks,
provider connection event hooks, STT turn-end TTFB timeout accounting, and
live-provider qualification are not implemented. Conversation-turn policy and
playback acknowledgement belong to their own processors/transports. API keys
remain caller-owned. Production Python is untouched; a developer-only source
oracle records one documented protected-span fix in `pipecat-text`.

## Verification

From `rust/` after sourcing the workspace toolchain:

```sh
cargo test -p pipecat-streaming-speech --offline
cargo clippy -p pipecat-streaming-speech --all-targets --offline --no-deps -- -D warnings
cargo fmt -p pipecat-streaming-speech --check
```

Tests exercise actual localhost WebSocket handshakes and media exchange,
incremental transcript/audio streaming, repeated query options, authentication,
malformed/oversized responses, PCM alignment, provider errors, connection and
response deadlines, cancellation with saturated queues, drop cleanup, redirect
credential isolation, processor draining, deferred response-end barriers, and
interruption followed by a fresh context. They simulate provider responses and
do not establish live recognition quality, voice quality, or production latency.
Additional regressions cover settings validation/targeting, old/new connection
settings, deferred reconnects, no ambiguous audio replay, buffered-audio failure
reporting, rapid-disconnect limits, cancellation during reconnect, immutable
multi-context synthesis, empty/failed response barriers, protected-span sentence
aggregation, single-slot finalization, bounded FIFO contexts, and partial-text
interruption.

### Cartesia synthesis provenance and timing

Cartesia accepts the native `FrameData::TtsTextInput(TtsTextInputFrame)` extension
for explicitly prepared synthesis/display/original-source channels. This is not
an original Python frame class. Existing `Text`, `LlmText`, `AggregatedText` and
`TtsSpeak` inputs still work; `AggregatedText.raw_text` keeps original delimiters
and markup, while its display text remains the provider request unless explicit
three-channel input is used. Completed aggregates keep their exact boundaries.
Skipped aggregates use the context's ordered reconstruction queue.

The adapter uses bounded native text alignment before dispatch. Early words wait
for first audio, word/progress frames reference their exact aggregate anchor, and
absolute PTS use the shared pipeline clock with checked unit conversion/addition.
Context tasks remain serialized, immutable-setting scoped, interruption cancelled
and health-revision fenced. Resynchronization, missing/unmatched words, and
successful timestamp-disabled synthesis emit `TtsAlignmentDiagnostic`; these
never become `TtsText`, Played receipts or spoken-history attestations. Ordinary
progress reflects text reconstruction only. Provider timestamps are not proof of
physical audibility, and duplicate reports without a provider event ID cannot be
reliably deduplicated by text or PTS.

Use native OutputTransport for timed forwarding. Its bounded clock lane lets PCM
pass future metadata while retaining anchor/word/progress order and terminal
barriers. It rebases pipeline-clocked PTS before downstream wire encoding. Direct
wire encoding of still-marked frames fails until an application explicitly
rebases. See `pipecat-text/ALIGNMENT.md` and the media-transport timing contract.
The real-loopback tests exercise these contracts without live credentials; this
slice does not establish full original TTS base-service or live-provider parity.

Timestamp-disabled successful synthesis currently does not populate audible
assistant history, even if the backend later proves complete context playback:
its text remains diagnostic only. This is a conservative integration boundary,
not full source parity. A separate future receipt-bound reconstruction policy
could combine known synthesis input with complete context playback; this slice
does not weaken SpokenHistory to infer that policy.
