# pipecat-gemini-live

Native Rust Gemini Live WebSocket client and `pipecat-core::FrameProcessor`.
This crate neither invokes Python nor aliases the Google HTTP LLM or OpenAI
Realtime implementations. It implements a defined subset of the provider
protocol; it does **not** claim full `GeminiLiveLLMService` / `GeminiSTTService`
parity. No provider credentials or live calls are needed for its tests.

## Native API

- `GeminiConfig::new(key, model)` configures a conversation;
  `GeminiConfig::transcription(key, model)` configures a dedicated transcription
  model. Model selection is explicit. These constructors do not infer current
  model availability from a name or silently change model families.
- `GeminiClient::connect()` authenticates the WebSocket, sends exactly one setup,
  and returns only after `setupComplete`. Setup errors/timeouts drop the socket;
  no task is launched before readiness. TLS is required except for literal
  loopback addresses used by tests. API keys are URL-encoded and excluded from
  configuration debug output and transport errors. Provider error messages have
  the configured key and its query-encoded forms redacted before display.
- `GeminiSession` owns the connection task and event receiver. Clonable
  `GeminiSender` accepts typed `Action` values plus optional local `Origin`.
  `next_event()` returns typed `GeminiEvent` values and reports terminal failure
  after queued events. Dropping the session aborts its owned task; `shutdown()`
  cancels and joins it. Dropped terminal-read futures retain the join handle for
  retry, and dropping a pending shutdown still aborts the owned socket task.
- `GeminiLiveProcessor` connects during pipeline setup, runs the receive loop as
  tracked processor work, handles end/cancel/stop, and preserves identity,
  metadata, payload and direction on forwarded original frames. Only supported
  downstream input frames are sent to the provider; upstream input is not replayed.
- `GeminiFrameMapper` can be used independently. It maps text/thoughts, PCM,
  transcriptions, tool calls/cancellations and available token usage into typed
  core frames. Tool execution belongs to the application.

```rust,no_run
use pipecat_core::{Frame, FrameData, Pipeline, PipelineConfig, Direction, TextFrame};
use pipecat_gemini_live::{GeminiClient, GeminiConfig, GeminiLiveProcessor};
use std::sync::Arc;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let key = std::env::var("GEMINI_API_KEY")?;
let model = std::env::var("GEMINI_LIVE_MODEL")?;
let config = GeminiConfig::new(key, model);
let service = Arc::new(GeminiLiveProcessor::new(GeminiClient::new(config)?));
let mut worker = Pipeline::new(vec![service]).start(PipelineConfig::default()).await?;
worker.sender().send(
    Frame::new(FrameData::InputText(TextFrame::new("Hello"))),
    Direction::Downstream,
).await?;
// Consume worker.recv() continuously and connect TtsAudio to an actual transport.
// Request worker.end() after the desired turn; worker.cancel() aborts immediately.
# worker.cancel("example cleanup").await?;
# worker.join().await?;
# Ok(())
# }
```

## Supported protocol

Setup supports one AUDIO or TEXT response modality; text system instructions;
voice and language code; temperature, max output tokens, top-p/top-k; basic
input/output transcription; automatic VAD sensitivities/prefix/silence settings;
and explicitly declared function tools with JSON parameter schemas. Blocking
lifecycles remain the default; [asynchronous tools](ASYNC_TOOLS.md) require
explicit per-name selection.
Settings are fixed for a connection. Neither the client nor frame processor
pretends that a settings frame updates an existing provider session.

Inputs are mono signed little-endian PCM16 (8–96 kHz, explicit configured rate),
realtime text, ordered user/model text `clientContent`, manual activity start/end,
audio-stream-end under server VAD, and final function responses. Output audio is
24 kHz mono PCM16. `audio/pcm` without rate uses the pinned Python service's
24 kHz default; other media/rates and partial PCM samples are rejected. A
WebSocket message may itself be fragmented; tungstenite reassembles it before
protocol decoding. No implicit resampling, WAV decoding or stereo downmixing occurs.

The downstream frame adapter accepts both `InputAudio` and `UserAudio`'s inner
PCM through the same validation, raw/encoded limits and manual-VAD path. It
forwards the original outer frame, retaining its envelope and local tags. The
inner audio context, including an explicitly empty context, takes precedence
over envelope metadata for admitted audio. When input is enabled, valid empty PCM
is forwarded without sending or buffering; invalid geometry/format and oversized
chunks are still rejected before that empty-input suppression. Upstream audio and output/TTS audio remain
pass-through and are not replayed to Gemini. The adapter never resamples;
applications may explicitly place the shared input transport/resampler before it.

Every supported content-bearing field and every model part in a bundled server
message is handled before `turnComplete`. `generationComplete` does not close a
turn. `interactionStatus=IDLE` is accepted. Background-work statuses are rejected
rather than misreported as a finished turn. Unknown server envelopes and unknown
model-part semantics fail explicitly. Informational extra fields within known
content/usage objects may be ignored; unsupported grounding fields are rejected.

Dedicated transcription mode maps `interimInputTranscription` to interim frames
and `inputTranscription` to final utterances, preserving reported BCP-47 language
and adding a local receipt timestamp. Conversational input transcription is
exposed as **non-final** fragments: neither model turn completion nor a local
timer is claimed to prove that an independently delivered input transcript is
finished. This differs deliberately from Python's timeout-based aggregation.
Applications needing final STT utterances should use transcription mode.
Dedicated transcription broadcasts `SttMetadata` after forwarding Start and on
matching metadata requests. Its configured TTFS P99 budget defaults to 0.60
seconds; `with_ttfs_p99_latency` allows an override or the source's `None` fallback
of 1.0 second. These are configuration values, not runtime measurements. See
[STT metadata and turn-stop budgets](STT_METADATA.md) for ordering, targeting,
the existing speech-timeout floor, and deliberate source differences.

Dedicated transcription defaults to a shared, last-observed audio user ID:
valid downstream `UserAudio` sets it from `user_id`, while `InputAudio` clears it.
The update follows PCM geometry, rate, channel and raw-size validation, but
precedes the empty-input check and provider enqueue. Valid empty audio therefore
changes the label without sending PCM; invalid, muted and upstream audio do not
change it. An enqueue rejection retains the observed label. The label is not an
admission receipt and grants no finalization/flush eligibility.

Each interim or final input transcription reads the latest shared label when
the receiver maps that event. A delayed response to an older audio chunk can
therefore carry a newer label. This is a service-level label, not per-audio
attribution or provider speaker diarization; it creates no source-frame/context
correlation or wire field. New setup starts with an empty dynamic label, including
sequential processor reuse. End draining retains the current label; stop/cancel
use the existing owned-task cleanup, and an old receiver cannot read a later
setup's label. `with_user_id(...)`, including an explicitly empty string, remains
a fixed override in both modes. Conversation mode retains its empty default,
and standalone `GeminiFrameMapper::new` retains its fixed supplied label.

The pinned Python source (`ProfHepta/pipecat`
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`)
[`STTService.process_audio_frame`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/stt_service.py#L433-L454)
checks reconnect buffering, mute and usability before updating `_user_id`, then
ignores empty audio. Gemini's
[`interim and final handlers`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/google/gemini_live/stt.py#L507-L539)
read that shared ID. Native keeps its stricter PCM validation and downstream-only
admission, explicit fixed override and per-setup reset; it does not add Python's
pre-mute reconnect buffering or usability lifecycle. Existing resumption and
flush ownership are unchanged.

## Turns, tools, correlation and interruption

Manual VAD streams only inside activity windows. The frame adapter keeps a
bounded half-second idle pre-roll (capped by the audio-message limit), sends
`activityStart` before replaying that pre-roll, and clears it on turn stop.
`with_preroll_ms` selects a fixed window; zero disables retention. This bounded
native behavior deliberately differs from the pinned Python zero-duration
`buffer[-0:]` edge, which retains the buffer. Pre-roll combines input families as
PCM bytes only; its flushed origin belongs to the activity-start frame, not a
buffered speaker or audio context. Downstream `SpeechControlParams.vad_params`
automatically sizes the idle window to `start_secs + 0.1` seconds unless
`with_preroll_ms` selected an explicit override. Server VAD receives continuous
audio. See [VAD pre-roll controls](VAD_PREROLL.md) for validation, direction,
reuse, and the pinned Python differences.
Dedicated STT additionally flushes on VAD stop frames with `audioStreamEnd`.

Conversation audio defaults to unpaused. `with_audio_input_paused` selects the
initial state; `set_audio_input_paused` changes it through a retained typed
`Arc<GeminiLiveProcessor>`. Paused downstream `InputAudio` and `UserAudio` pass
through unchanged before PCM validation, provider admission or new idle-pre-roll
retention. Existing pre-roll is preserved and can still flush at activity start;
activity controls and previously admitted work remain active. Each setup restores
the builder's initial state. Transcription mode ignores this switch and keeps its
separate `SttMute` control. See [Conversation audio pause](AUDIO_INPUT_PAUSE.md)
for an application control-handle example and concurrency/source boundaries.

Dedicated transcription mode consumes `SttMute` in either direction. While muted,
new downstream `InputAudio` and `UserAudio` retain normal frame passthrough but
are not validated or admitted to the provider. Unmuting resumes new input; no
muted audio is retained for replay. Previously admitted audio, asynchronous
transcripts, and VAD-stop finalization continue. This is recognition-input
control, not a device microphone switch or withdrawal of sent data. Conversation
mode keeps these controls as ordinary passthrough. Each native setup resets mute
to false. See [STT mute boundaries](STT_MUTE.md).

`LlmContext` supports one initial text-only seed; repeated identical snapshots
are no-ops. System instructions/tools must already match connection setup.
Evolving/replaced snapshots and detailed/non-text histories are rejected, since
blindly replaying them would duplicate the server's live history. Explicit
`LlmMessagesAppend` / `ClientContent` provide append-only history. There is no
Python mutable-context sharing or model-family-specific initial-history dance.
The application must select a model supporting the documented client-content
semantics; no artificial whitespace prompts are inserted to trigger inference.

Tool IDs and names are retained. Calls must match configured declarations.
Missing, null, or empty provider IDs receive session-owned opaque IDs in the
reserved `pipecat-gemini-local-` namespace. `FunctionCall.id_source` distinguishes
`Provider` from `LocalGenerated`; mapped frames preserve this distinction in
`gemini_tool_id_sources`. Generated IDs are local correlation, never claimed as
provider identity. As in the pinned Python service's UUID fallback, the same
generated ID is sent back in `FunctionResponse.id`. Monotonic session/call
counters prevent local ID reuse across live sessions in the process; supplied provider IDs cannot enter the reserved
namespace, even after a previous call completes. Counters and bounded call maps
avoid an unbounded completed-ID history. Repeated ID-less call messages are
distinct calls, matching the source fallback; no provider replay key exists to
deduplicate them. This does not add Vertex authentication or persistent sessions.
Pending calls are bounded; duplicate IDs and unknown/canceled/mismatched results
are errors. Omitted function arguments default to an empty JSON object. By default, local
and provider interruptions invalidate pending calls and queued stale results.
Explicit non-blocking tools use the separate lifecycle described in
[Asynchronous tools](ASYNC_TOOLS.md).
The most recent `max_pending_tools` abandoned calls are retained solely to
recognize delayed provider cancellation, with original call correlation; they
cannot block finish. Older or otherwise unknown cancellation IDs fail explicitly.
Cancellation events identify the original calls. The adapter emits
`FunctionCallsStarted`, `FunctionCallInProgress` and `FunctionCallCancel`; it
never executes, retries or undoes a function. Final scalar results are wrapped
as `{"result": ...}` by the frame adapter; native `FunctionResponse` requires
an object. Partial results and scheduled responses require explicit non-blocking selection.
Context callbacks remain rejected.

Gemini does not supply per-response IDs in this protocol. `Origin` and the
`source_frame_id`, `context_id`, `gemini_generation` frame metadata are **local
correlation**, not fabricated provider IDs. The origin active at first model
output is held through that output turn. Input transcripts intentionally have no
inferred causal origin because input transcription and model generation can
arrive independently. Realtime input ordering is provider-controlled; the client
does not promise one separate response per submitted realtime message.

`interrupt()` invalidates already queued output and suppresses ongoing output
until a provider interruption or terminal boundary. It sends no invented cancel
message. Actual audio under automatic VAD, or `activityStart` under manual VAD,
is what asks the provider to barge in. The input queue is retained so activity
start/audio/end ordering cannot be broken by a local interruption. Server
`interrupted` advances the output epoch and the adapter broadcasts a core
interruption. No user speech boundaries are guessed from provider interruption.
No playback success or played-duration estimate is fabricated: TTS lifecycle
frames describe generated audio; only a real output transport may report bot
speaking/playback completion.

## Bounds and ownership

Input and event channels are bounded. `try_send` fails immediately at capacity;
it does not create an unbounded deferred input task. Event overflow terminates
the session explicitly rather than blocking the reader (which would deadlock
cancellation). Configured WebSocket/message/audio sizes, JSON payload sizes,
context size, function count and pending-tool count bound retained state. The
single connection task preserves admitted input order. Cancel uses an independent
watch channel and can interrupt a stalled socket write. Local interruption and
frame emission share an epoch gate to avoid forwarding stale output afterward.

`finish()` stops admitting new input, drains already admitted commands, flushes
open audio activity, and waits for the current provider turn or final STT
utterance. Already outstanding function responses remain admissible during that
wait. Finish admission is cancellation-safe: input admission closes atomically
with the queued finish command, and a canceled or timed-out admission can be
retried. No new user turn should be submitted during finish. Realtime messages do
not have per-message completion acknowledgements; callers needing a particular
turn's output should observe its terminal event before starting another turn.
A finite finish deadline reports `Timeout`, never synthetic response completion.
`cancel()` / `shutdown()` discard unfinished work. A clean finish sends a close
frame; it does not claim downstream audio was played or a close handshake was
acknowledged. The pipeline owns and joins its receive work on cleanup.

## Deliberate exclusions

The following are not implemented, and no pass-through setting/action advertises
them: automatic replay, handle-less session rebuilding, Vertex authentication, ephemeral token
creation, initial-history configuration, context-window compression, thinking tools/budgets,
Google built-in tools/search/grounding and Google-specific RTVI search messages,
file input and image formats beyond the bounded RGB video path, Google File API upload/list/get/delete, live setting
updates, language-hint/adaptation options for dedicated STT, STT keepalive audio,
tracing spans, transcription aggregation timeouts, or playback-status reporting.
Related frame input, recognized unsupported server semantics, and unknown config
fields are rejected. By default `GoAway` is exposed as an event and an actionable
frame error. Explicit bounded receive-side recovery is available through
`GeminiClient::with_session_resumption`; see [Session resumption](SESSION_RESUMPTION.md)
for handle validity, delivery limitations and finish/cancel behavior. Idle network black holes remain
pending until caller cancellation or finish; there is no implicit idle timeout.

This API is intentionally explicit about these boundaries rather than presenting
Python import compatibility or provider-wide parity.

## Source and verification

Pinned reference: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
Relevant original files:

- `src/pipecat/services/google/gemini_live/llm.py`: setup, manual VAD activity
  window/pre-roll, bundled content dispatch, audio defaults and interruption
- `src/pipecat/services/google/gemini_live/stt.py`: interim/final utterances,
  server-VAD audio-stream-end flush and source-era language/adaptation settings
- `src/pipecat/services/google/gemini_live/file_api.py`: separately scoped
  resumable HTTP File API, deliberately excluded here
- `src/pipecat/services/google/{frames.py,rtvi.py}`: search/grounding frame and RTVI
  payloads, deliberately excluded here
- `tests/test_gemini_live_user_audio.py`, `tests/test_gemini_stt.py`,
  `tests/test_gemini_live_interaction_status.py`,
  `tests/test_gemini_live_tracing.py`: source behavior and explicit gaps

Protocol checks use Google's [WebSocket API reference](https://ai.google.dev/api/live)
and [Live capabilities](https://ai.google.dev/gemini-api/docs/live-api/capabilities).
These rolling docs are not a pinned model-availability guarantee. In particular,
the pinned STT source uses SDK `language_hints`, `language_auto` and
`adaptation_phrases`, while the consulted public reference describes
`languageCodes`/`customVocabulary`. Neither mapping is guessed here. Examples in
rolling docs can also name models newer than the pinned source. The model is
always caller-selected.

Fixtures under `tests/fixtures` are synthetic wire translations of these source
contracts, not captured provider traffic. See their provenance file. Loopback
servers exercise actual HTTP upgrade/authentication, readiness, native JSON and
PCM framing, WebSocket fragmentation, input/event bounds, manual activity,
transcription, tool lifecycles, turn ordering, error redaction, finish/cancel/drop,
and real core-pipeline identity/lifecycle behavior. Live compatibility remains
unverified without an explicitly authorized provider test.

```sh
source /workspace/shared/pipecat-tools/env.sh
cargo test --manifest-path rust/Cargo.toml -p pipecat-gemini-live --offline -j 2
cargo clippy --manifest-path rust/Cargo.toml -p pipecat-gemini-live --all-targets --offline -j 2 -- -D warnings
cargo fmt --manifest-path rust/Cargo.toml -p pipecat-gemini-live -- --check
```

## Sampled RGB video input

Conversation-mode downstream InputImage/UserImage now connect to realtime video
with bounded native JPEG encoding, service-wide one-second admission, pause and
owned cancellation cleanup. See [VIDEO_INPUT.md](VIDEO_INPUT.md) for exact limits,
source differences and evidence. This does not qualify live camera/browser use.

## Explicit thinking subset

[Thinking completion](THINKING.md) documents `with_thinking`: typed level setup
for tool-free conversation sessions, deferred completion during background
reasoning, bounded silence failure, and interruption/resumption ownership. The
default client remains unchanged.

## Explicit asynchronous tools

[Asynchronous tools](ASYNC_TOOLS.md) describes per-name NON_BLOCKING selection,
intermediate and final results, SILENT/WHEN_IDLE scheduling, interruption,
provider cancellation, and the bounded call-identity ledger.
