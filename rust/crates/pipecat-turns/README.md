# Native context aggregation and user turns

`pipecat-turns` provides deterministic user/assistant context state machines and
`FrameProcessor` adapters. The default controller consumes VAD decisions and uses
speech/STT deadlines. `SmartTurnUserAggregator` adds the actual native
`pipecat-vad::SmartTurnAnalyzer`, with its explicit ONNX model/runtime paths;
no Python process or substitute energy threshold participates in inference.

## API

The pair also supports explicit/auto/off [realtime trailing aggregation](REALTIME.md).
Auto is the pair default; cascade behavior remains until realtime metadata arrives.
Provider adapters must separately implement its state-only context contract.

- `LLMContextAggregatorPair::new(Context, UserAggregatorConfig, AssistantAggregatorConfig)`
  creates a shared history. `user()` and `assistant()` return `Arc` processors;
  `context().snapshot()` returns the provider-neutral services `Context`
- `LLMUserAggregator::process_at(frame, direction, monotonic_duration)` and
  `advance(monotonic_duration)` return `AggregationOutput` without doing I/O
- `UserTurnController::process(&FrameData, monotonic_duration)`, `advance`, and
  `deadline` expose the independent turn state machine
- `UserIdleController` exposes an independent one-shot waiting-for-user timer;
  `LLMUserAggregator::on_user_turn_idle` registers native asynchronous handlers
- `LLMAssistantAggregator::process_frame(frame, direction)` returns deterministic
  effects. `AggregationOutput.events` contains typed turn and ignored-result
  events; `actions` contains directed/broadcast frames and interruptions
- `AggregationOutput::emit` sends the actions through a processor context. The
  deterministic interface returns events without invoking registered handlers

Place the user half before the LLM and the assistant half after the delivery
boundary. In text mode, incoming text is considered delivered. In speech mode,
generating text, generating PCM, sending PCM, and provider synthesis completion
are not playback acknowledgements.

## Playback acknowledgement

Use `AssistantTextMode::Spoken` for a voice pipeline. A transport that knows a
complete context has actually been played should:

1. Call `begin_playback(context_id)` when registering pending audio. This blocks
   a tool-result inference while spoken filler still awaits acknowledgement
2. Validate that an acknowledgement refers to a ready, current-generation
   context, and that its order satisfies the transport's playback contract
3. While holding the same transport lock used for interruption invalidation,
   call `confirm_playback(context_id, &[AggregatedTextFrame])`. It synchronously
   commits that complete context to shared history before a concurrent user
   interruption or new user context snapshot can overtake it
4. Emit the returned actions and notify the client. Do not also submit those
   text parts through the frame interface

Playback confirmation validates all part IDs before writing, is idempotent for
recently closed IDs, and preserves future pending playback contexts. The
transport owns the truth of its playback acknowledgement; this crate cannot
prove that a physical speaker emitted audio.

The lower-level framed interface accepts only `TtsText` whose metadata contains
`"spoken": true`. A normal-lane `Flush` with `origin="assistant_turn_end"`
commits those parts. `BotStoppedSpeaking` is a system frame and can overtake
normal-lane words, so it cannot serve as a commit fence. A framed delivery
adapter must serialize acknowledgement delivery against interruption and mark
confirmed data uninterruptible; an ordinary enqueue is insufficient for an
atomic remote acknowledgement. Prefer `confirm_playback` for complete-context
network acknowledgements. Word-level partial acknowledgements require a
transport with a corresponding timestamp/generation contract.

A pair takes each user-request snapshot under the assistant state lock. Tool
results already included in that snapshot require no redundant inference;
results arriving afterward remain owed until the admitted response finishes.
Same-batch speaking boundaries and automatic interruption preserve this admission,
while an explicit interruption cancels it. Muted and unrecovered empty turns
release a pending tool continuation. Empty-turn recovery instead includes
already-completed tools in its own single inference snapshot.

A pair seals already-delivered assistant text synchronously when a new user
turn interrupts it, before constructing the user context snapshot. Standalone
halves have no such coordination: their caller must settle the assistant
interruption before submitting an immediate next-turn context.

## Implemented behavior

- VAD/transcription starts; adopted speaking frames without duplicate broadcasts;
  optional resolution of proposed start/stop frames
- Separate policy-silence and STT-latency waits. A finalized transcript can
  shorten the STT wait but never the silence floor. A subsequent interim
  hypothesis invalidates prior finalization. Resumed speech invalidates stale
  timers; the watchdog never closes audibly active speech
- No-VAD transcript inactivity fallback; final segment accumulation with correct
  spacing; interim preview replacement without committing interim hypotheses
- Bounded replay detection by frame identity and optional stable
  `metadata["transcript_id"]`; identical text with a fresh identity remains valid
  in a later turn. Repeated provider events need a stable provider segment ID
- Explicit mute/unmute suppression; empty/whitespace transcripts do not create
  messages or inference calls. A mute boundary preserves previously accepted
  final text without running inference and discards interim previews
- Session-end flush, cancellation cleanup, and one managed per-session
  turn/wake supervisor that remains dormant between deadlines. `prepare_end`
  cancels the supervisor before the runtime drains tasks
- Optional user-idle detection after bot speech, runtime timeout updates,
  interruption/function-call guards, and empty-turn re-arming. Its owned idle
  supervisor and callbacks are cancelled before End/Stop draining or on Cancel
- Bounded empty-turn recovery, enabled by default for an interrupted response;
  the exact source developer prompt requests clarification without inventing
  a user transcript. Idle empty turns remain unanswered by default
- Typed `InputText` runs immediately and interrupts pending output. Context
  append/update/run frames preserve the shared history
- Assistant streaming text/word spacing, append-to-context filtering, thought
  exclusion, terminal flush, partial delivered-context commit on interruption,
  and suppression of old acknowledged playback contexts
- Tool declarations, in-progress placeholders, ordered result replacement,
  groups that run inference once when complete, sticky deferred inference while
  a user/model/bot/playback context is active, and result-before-start rejection
- Async-tool started/intermediate/final/cancelled JSON protocol. Fast results
  settle the original placeholder; a result after a user/developer message is
  recorded as a deferred developer message. Interruptible calls become cancelled
  on interruption; the execution layer remains responsible for stopping them
- System-priority tool-start declarations cannot cause a fast result to rerun
  the LLM before normal-priority response-start/end markers arrive

## Rich frame and context contracts

Both legacy and detailed transcription, VAD, and LLM-response families drive the
same state machines. Detailed transcripts retain whitespace/append flags, and
`transcript_frames()` returns the latest 512 accepted final/interim originals,
including identities, metadata, user IDs, timestamps, language, and provider
results. Rich transcription frames are consumed just like source transcripts;
provider metadata is not appended to the spoken text. Eager transcripts and
translations never become final user messages. Muting also suppresses `UserAudio`.

`SharedContext::new_rich`, `snapshot_rich`, `append_rich`, and `replace_rich`
maintain core `LlmContextData`. `LLMContextAggregatorPair::new_rich` and
`with_shared_context` construct coordinated halves from rich/shared histories.
`frame_data()` selects a legacy frame for compatible native text histories and
`LlmContextDetailed` for rich messages, extensions, explicit empty tool lists,
and richer tool schemas. Local tool-handler identities are retained in memory;
network serialization must reject them rather than pretend they are serializable.

The existing `new`, `snapshot`, `replace`, `append`, and `frame` APIs remain.
Legacy snapshots and text-only frames explicitly reject unrepresentable content.
Supported image/audio/file/thinking/refusal parts survive native-to-rich framing.
Native `InlineData` does not identify whether bytes are an image or file in the
core schema: legacy snapshots retain it exactly, but rich snapshots and framing
require an explicit image/file conversion. Message transforms preserve every
retained native occurrence, and reject ambiguous partial selection when distinct
native messages have the same core projection. A failed transform is atomic.


`AssistantImage` has a narrow encoded-original context path for ordinary
application frames, including frames decoded by the public `PythonJsonCodec`.
Nonempty `original_data` with exact `image/png` or `image/jpeg` MIME and a matching
PNG/JPEG signature is appended immediately as one assistant-role rich image
data URL. Original encoded bytes are preserved; display RGB, size and format are
not used. Signature checks are not complete image validation: a provider may
still reject malformed data. Existing snapshots/framing and the Gemini request
adapter preserve these bytes as model-role `inlineData`; this does not establish
live-provider acceptance or a native-provider visual round trip. Native image
processors still produce `UrlImage`, and native Gemini multimodal output remains
unsupported.

This path adds a fixed native admission cap of 8,388,608 original bytes per image,
without changing `AssistantAggregatorConfig` or its existing struct literals.
Checked base64 expansion and prefix addition limit a data URL to 11,184,835 bytes
before fallible allocation. These are per-image logical-length limits, not a
SharedContext total, input allocation-capacity, decoded-pixel or RSS budget.
Callers still manage cumulative history. The codec's JSON-envelope limit remains
separate. No image decoder or new background task is involved.

An accepted image is consumed in either direction, produces no action/event,
does not flush pending assistant text, and does not run inference or invoke a
tool callback. Consequently an image received during buffered text appears in
context before that text's normal end-of-response commit. Successful admission
shares the existing last-512-frame-ID replay cache; failed validation leaves an
ID retryable. This native bounded replay policy is deliberately stricter than
Python's repeated-image append behavior. Ordinary images after End/Cancel/Stop
are ignored. Raw-only assistant images, all UserImage variants and user files
remain explicitly unsupported.

Image admission rejects enabled realtime mode and any signed-continuation
tracker retained by this assistant instance, including a completed response
still waiting for playback/reconciliation. A live image cannot carry native
continuation or tool authority, including a retyped private-carrier clone.
Completed signed prefixes are preserved when no local tracker remains. This
capability assumes one serialized context writer: it cannot detect another
aggregator's active signed continuation or protect against concurrent external
context edits. Signed-image bookmark production and continuation redesign are
outside this scope.

Detailed message append/update and local transforms honor optional `run_llm`;
`None` does not request a run. Tool-set and tool-choice changes update the actual
shared history. Source-specific rich thoughts retain the LLM identity and JSON
signature as context extensions, separately from delivered assistant text;
provider adapters that do not support those extensions report an explicit error.
Markers and `LlmAssistantPushAggregation` perform context commits. Vision response
boundaries and text use the same aggregation and replay handling as LLM responses.

Detailed function-call results honor property-level `run_llm` before frame-level
`run_llm`, final/intermediate status, group completion, and cancellation. Non-null
intermediate results default to inference when no other sibling in their group
is running; explicit `run_llm=true` overrides that group wait. Completed groups
and cancellation continuations are independent of unrelated background groups.
Response, user-turn, and playback fences still defer these continuations. Accepted
`on_context_updated` callbacks run in owned asynchronous tasks after the result
is written, never under the conversation lock. Duplicate/ignored results do not
call them. Completed task registrations are removed; End/Cancel cancels pending
callbacks. Synchronous callers receive `AggregationAction::ContextUpdated` and
must emit or otherwise execute the returned effect.

In spoken mode, response/text `skip_tts=true` is an explicit text-only delivery
path and does not await a nonexistent playback acknowledgement. Ordinary speech
still requires acknowledgement. Rich and legacy flush fences behave identically.

`SttMetadata.ttfs_p99_latency` updates the actual STT budget. Detailed VAD stop
frames supply their typed `stop_secs`; legacy VAD frames use configured
`vad_stop_duration`. A positive UNIX stop timestamp discounts transport delay
from the STT budget, while zero denotes an unspecified timestamp for deterministic
traces. Non-finite/negative VAD timings and invalid STT latencies are rejected.
The fallback default STT latency is zero and makes no provider-latency claim.

## Native Smart Turn integration

Construct a `SmartTurnAnalyzer` with an explicitly loaded native model, then use
`SmartTurnUserAggregator::new(context, user_config, analyzer)`. To retain the
assistant coordination of an existing pair, wrap `pair.user()` with
`SmartTurnUserAggregator::with_aggregator`. Configure it before pipeline setup,
and place only the wrapper in the user-aggregator position.

The wrapper has one managed async supervisor and the analyzer's bounded native
inference worker. Audio continues through the pipeline while a model result is
pending. VAD start/stop drives real audio buffering and model inference. A
COMPLETE result opens the turn gate; finalized text or the physical-stop P99
budget releases context. `wait_for_transcript=false` releases immediately on
COMPLETE. Model latency never starts a fresh STT wait, and an interim hypothesis
revokes prior finalization without extending the physical-stop deadline.

Resumed speech cancels and invalidates stale inference while preserving the
utterance buffer for reanalysis. Silence completion, no-VAD transcript fallback,
external stops, mute, watchdog, End, and Cancel all have explicit lifecycle
handling. End cancels incomplete model work and flushes accepted final text;
Cancel does not request a new response. Cleanup joins the native model worker.
Model timing/probability metadata is emitted only for actual model predictions;
silence and inference timeouts do not fabricate probabilities. Model parameters
travel in `SpeechControlParams` metadata because core `BaseTurnParams` is empty.

## User-idle detection

Set `UserAggregatorConfig::user_idle_timeout` to a nonzero `Duration` and
register `on_user_turn_idle` before starting the pipeline. The default is zero.
The timer starts when the bot stops speaking, never simply because a user turn
ended. User or bot speech cancels the timer. Function-call declarations suppress
it, including when they arrive before the bot-stop frame. A matching final
result or cancellation releases that call without assuming the bot has responded.
Accepted typed user input also closes the waiting window until the bot responds.

The native guard intentionally corrects the source controller's unkeyed counter:
it matches call IDs, names, and result arguments, ignores intermediate and
unmatched results, and deduplicates declarations. Empty declaration batches do
not imply activity. Declarations require nonempty IDs/names and object arguments,
matching the native assistant contract; conflicting duplicates are rejected.
At most 512 calls may remain pending; exceeding that limit rejects the whole
batch without evicting active guards. The last 512 completed IDs suppress replay.
Older completed IDs fall outside this bounded window, so callers must keep IDs
unique and must not replay old declarations. Well-formed source sequences retain
their original waiting-window behavior.

```rust
use pipecat_core::{Direction, Frame, FrameData, TtsSpeakFrame};
use pipecat_turns::{LLMUserAggregator, SharedContext, UserAggregatorConfig};
use std::time::Duration;

# fn example() -> Result<(), pipecat_core::ProcessorError> {
let user = LLMUserAggregator::new(SharedContext::default(), UserAggregatorConfig {
    user_idle_timeout: Duration::from_secs(5),
    ..Default::default()
});
user.on_user_turn_idle(|ctx| async move {
    ctx.push(
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("Are you still there?"))),
        Direction::Downstream,
    ).await
})?;
# Ok(())
# }
```

Each waiting window emits once. A new bot-stop frame or explicit
`wait_for_user(&ProcessorContext)` re-arms it. The aggregator also re-arms after
an empty turn closes without inference; it does not fabricate a transcript or
recovery prompt. Re-arming is suppressed while either speaker or a function
call is active. `waiting_for_user`, `function_calls_in_progress`, and
`idle_deadline` expose this state independently of the turn controller.

`UserIdleTimeoutUpdate { timeout }` uses seconds and restarts the timer from
the update time while already waiting, including when detection was disabled.
Zero or negative values disable detection. Non-finite or unrepresentable values
are rejected. Updating while awaiting a bot response never opens a new waiting
window. `wait_for_user_at` and `advance` support deterministic integration;
expiration returns `AggregationEvent::UserTurnIdle` without running handlers.

The runtime owns one idle supervisor after the first armed deadline. It sleeps
without polling when no timer is armed, reuses its task across updates, and runs
handlers outside conversation locks. Handlers may push frames or re-arm idle
detection; keep them short because handlers run in order. Nonfatal handler errors
emit an upstream error and allow later handlers and re-armed timers to continue;
fatal errors fail the pipeline. End, Stop, Cancel,
and cleanup cancel the timer and any pending handler. Include this task in
`max_background_tasks` alongside the independent turn/Smart Turn supervisor.
`SmartTurnUserAggregator::user()` exposes the same configuration and handlers.

## Cascade context summarization

Manual `LlmSummarizeContext(None)` frames work on every assistant aggregator.
Configure defaults and opt into response-start thresholds with
`LLMAssistantAggregator::with_summarization(ContextSummarizationConfig)`.
A manual frame may carry a per-request `LlmContextSummaryConfig`, including the
prompt, token budget, retained-message count, timeout, and `{summary}` template.
Successful summaries are user messages (`Conversation summary: {summary}` by
default). Flows' developer-message summary format is a separate contract.

Provide actual inference in either of two ways:

- Attach `SummaryLlm::new(LlmClient, CompletionRequest)` with
  `with_summary_llm` for a dedicated out-of-band native client
- Place `LLMContextSummaryProcessor::new(SummaryLlm)` before the assistant.
  It consumes upstream `LlmContextSummaryRequest` frames and returns correlated
  `LlmContextSummaryResult` frames downstream. The client may use the same model
  and configuration as the conversation LLM. Normal conversation frames pass
  unchanged. Other native responders can implement the same typed contract

The two assistant builders may be used in either order. A wire-level processor
reference in `LlmContextSummaryConfig::llm` is not executable and is rejected;
use the native client builder instead. Unconfigured request/result inference
reports a nonfatal timeout rather than inventing a summary.

Selection preserves all leading system messages and the requested recent suffix.
Mid-conversation system injections can be summarized. Tool groups are indivisible:
missing/pending results, asynchronous running markers, nonadjacent results,
orphan results, duplicate IDs, and overlapping groups constrain the boundary.
A reverse interval sweep is O(n log n). All leading system messages are preserved
intentionally; Python currently preserves only the first one.

Each admitted request owns an exact prefix snapshot and context lineage. Results
must match its identity and selected range. Replacing/transforming context, even
with equal values, invalidates old results. Appended messages survive compression;
apply-time boundary validation also protects new references to older tool calls.
Interruption, End, Stop, Cancel and cleanup invalidate pending results. A summary
failure never discards an unrelated live assistant response. Replacement/transform
frames also cancel the responder's obsolete HTTP calls immediately. Direct
`SharedContext::replace` calls invalidate result application and a subsequent
controller request, but cannot notify an independent responder; its old call may
occupy an admission slot until completion or timeout. Use replacement frames when
immediate cancellation across the request/result route is required.

Retained rich messages, extensions, native message distinctions, tools and tool
choice remain intact. Original native messages are remapped by index, including
forms with identical universal projections. Summary-only snapshots preserve such
native distinctions in a `pipecat_native_message` extension. The JSON transcript
includes all message fields and multimodal descriptors; this does not claim visual
or acoustic interpretation of embedded media. Executable tools are omitted from
summary requests. Provider settings that override the final tool-free request or
token budget are rejected.

The assistant reuses one owned supervisor for dedicated inference or the external
result deadline. It runs no inference inline in frame processing. The separate
responder also reuses one owned supervisor and defaults to one in-flight call;
`with_max_in_flight` sets its admission bound. In addition to the native client's bounded streaming transport, full
snapshot transcripts/prompts are limited to 16 MiB, summaries/rendered templates
to 1 MiB, request identities to 512 bytes, and timeouts to one day. Invalid input,
empty/truncated/filtered/tool-producing responses, transport failures and timeouts
leave history unchanged and permit a later request. No automatic retry contacts
an LLM after an error.

`AggregationEvent::SummaryApplied` reports original, new, summarized and preserved
message counts through the deterministic interface. `process_frame` only returns
request/result effects; callers using that interface own dispatch and deadlines.
Use the pipeline adapter for automatic inference/task lifecycle ownership.

The runnable native example requires an explicitly configured model and `--run`:

```sh
cargo run -p pipecat-turns --example context_summary -- --help
PIPECAT_SUMMARY_MODEL=your-local-model cargo run -p pipecat-turns \
  --example context_summary -- --run --dedicated --auto
```

The default provider is local Ollama. `PIPECAT_SUMMARY_PROVIDER` also accepts
`openai`, `anthropic` and `gemini`; remote providers require
`PIPECAT_SUMMARY_API_KEY`. `PIPECAT_SUMMARY_ENDPOINT` overrides the exact endpoint.
Without `--run`, the example prints usage and sends no request. Omit `--dedicated`
to exercise the responder route; omit `--auto` to use a manual trigger.

`tests/context_summary.rs` follows the selection and controller assertions in
`test_context_summarization.py` and `test_llm_context_summarizer.py`.
`tests/context_summary_runtime.rs` uses real native HTTP/SSE clients against
OpenAI, Anthropic and Gemini loopback fixtures, both routing modes, live-turn
concurrency, cancellation, teardown, provider failures and owned-task limits. `tests/review_context_summary.rs` adds
independent rich-identity, race and admission regressions.
`tests/review_summary_supervisors.rs` covers multi-request capacity, out-of-order
bidirectional correlation, replacement cancellation, repeated interruption/reuse,
and all terminal paths for concurrent HTTP calls. These tests use no
credentials, external providers or Python runtime. The four source examples under
`examples/context-summarization/` map to manual/automatic, provider selection and
dedicated-client modes; this native example does not reproduce their audio
transports, STT, TTS or live-call setup.

## Source and validation mapping

Golden assertions are traced in `tests/golden.rs` to:

- [`llm_response_universal.py`](../../../src/pipecat/processors/aggregators/llm_response_universal.py)
  and [`test_context_aggregators_universal.py`](../../../tests/test_context_aggregators_universal.py)
  for user/assistant context, spacing, mute, session-end, interruptions, tools,
  deferred results, and multiple turns
- [`user_turn_controller.py`](../../../src/pipecat/turns/user_turn_controller.py)
  and [`test_user_turn_controller.py`](../../../tests/test_user_turn_controller.py)
  for no duplicate starts/stops, stale completion guards, external signals, and
  late-transcript isolation
- [`speech_timeout_user_turn_stop_strategy.py`](../../../src/pipecat/turns/user_stop/speech_timeout_user_turn_stop_strategy.py)
  for the two-timer policy and STT finalization behavior
- [`async_tool_messages.py`](../../../src/pipecat/processors/aggregators/async_tool_messages.py)
  for the structured asynchronous tool messages
- [`test_empty_user_turn.py`](../../../tests/test_empty_user_turn.py) for the
  default interrupted recovery, optional idle prompt, retry bound, pending-tool
  exclusion, no-invented-transcript boundary and idle re-arming
- [`user_idle_controller.py`](../../../src/pipecat/turns/user_idle_controller.py)
  and [`test_user_idle_controller.py`](../../../tests/test_user_idle_controller.py)
  for waiting windows, interruption/function-call suppression, timeout updates,
  and explicit re-arming (`tests/user_idle.rs`, `tests/user_idle_runtime.rs`)

`tests/runtime.rs` uses a paused Tokio clock, exercises owned timer emission and
End/Cancel cleanup, and streams 200 transcript segments with a one-task limit.
These deterministic tests are based on source assertions. `tests/rich_frames.rs`
checks rich-frame semantics and callback lifecycle. `tests/smart_turn_runtime.rs`
uses controlled native predictors for result ordering, cancellation, and deadlines.
`tests/smart_turn_onnx_pipeline.rs` runs the actual tracked ONNX model and an audio
fixture through a native pipeline with synthetic STT/LLM processors. These tests
do not contact a live provider or claim microphone/speaker validation.

Run from `rust/`:

```sh
cargo test -p pipecat-turns --locked --offline
cargo clippy -p pipecat-turns --all-targets --locked --offline -- -D warnings
```

## Explicit compatibility scope and remaining work

This is a core cascade-turn implementation, not certification of the complete
Python modules or test suites. The migration inventory remains open. Create a fresh pair for each session;
instances closed by End, Stop, or Cancel are not restartable.

- The default remains the documented speech-timeout strategy; select the Smart
  Turn wrapper explicitly for native neural turn detection. Silero VAD is a
  separate `pipecat-vad` processor. LLM verdict strategies, speculative/eager
  inference and arbitrary strategy plugins have separate explicit native contracts.
  Supported service recommendations and paired realtime behavior are documented in
  [REALTIME.md](REALTIME.md); unsupported descriptors fail explicitly
- Explicit mute and the configurable native mute policies below are supported.
  Empty-turn recovery applies only to cascade mode; paired realtime activation
  disables it
- Raw image/file-to-context encoding needs an explicit encoder; encoded rich
  messages are supported. Full Python callback/event parity and
  provider-specific realtime speech-to-speech qualification
  remain outside this implementation
- Grouped tool results and active-response deferrals are coalesced. Arbitrary
  adjacent ungrouped append/result frames are not coalesced by queue inspection
- An actual executor must emit terminal results/cancellation and must honour
  `cancel_on_interruption`; recording cancellation does not stop external effects
- False, zero, empty string, and empty array tool results retain their JSON value;
  only null maps to `COMPLETED`. This deliberately avoids the Python truthiness
  rule that can erase legitimate falsy results
- Replay caches retain 512 identities. Providers/transports must keep stable,
  unique identifiers and enforce their own generation/lifetime checks
- Client playback acknowledgements and partial-word framing are transport
  contracts, not proof of acoustic playback. No live microphone/speaker claim
  follows from these tests

## Composable start and mute policies

`UserTurnConfig::start_strategies` is opt-in. `None` retains the native VAD,
transcription, and already-announced external-start defaults. An explicit
`UserTurnStartStrategies` list runs in order. Its default list is VAD followed
by transcription; an explicitly empty list disables starts. Stop policy remains
in the existing controller, including silence/STT budgets, watchdog and optional
native SmartTurn gating.

```rust
use pipecat_turns::{UserAggregatorConfig, UserMuteStrategy, UserTurnConfig,
    UserTurnStartStrategies, UserTurnStartStrategy};
use std::time::Duration;

let config = UserAggregatorConfig {
    turn: UserTurnConfig {
        start_strategies: Some(UserTurnStartStrategies::new(vec![
            UserTurnStartStrategy::wake_phrase(
                vec!["hey pipecat".into()], Duration::from_secs(10), false,
            )?,
            UserTurnStartStrategy::vad(),
            UserTurnStartStrategy::transcription(true),
        ])?),
        ..Default::default()
    },
    mute_strategies: vec![UserMuteStrategy::FirstSpeech, UserMuteStrategy::FunctionCall],
    ..Default::default()
};
# Ok::<(), pipecat_core::ProcessorError>(())
```

- `vad()`, `transcription(use_interim)`, `external()`, and
  `min_words(min_words, use_interim)` implement the corresponding source starts
- Minimum words counts each hypothesis independently while the bot speaks; when
  the bot is silent one word suffices. Rejected hypotheses reset accumulated
  text. A later fallback may still decide to start on that same frame
- A wake gate blocks later detectors until a final transcription matches. It
  accumulates at most 250 Unicode characters across final frames. Rejected final
  text is removed from aggregation, and only the matching frame is committed
- Wake timeout mode refreshes on transcripts, VAD starts, speaking activity, and
  a new turn. `single_activation=true` preserves the source's fixed keepalive
  timeout: turn stop does not immediately relock the gate
- Source-style `re.IGNORECASE` matching uses bounded native regexes with explicit
  letters/numbers/underscore word boundaries. Combining marks, connector
  punctuation other than underscore, and join controls are stripped; raw Rust
  regex `\w`/`\b` would differ. Whitespace includes Python's U+001C..U+001F,
  and dotted/dotless I are normalized for Python-style case equivalence. Unicode
  character tables come from the pinned Rust toolchain/regex versions, so newly
  assigned characters can differ from a Python runtime with another Unicode version
- Configuration bounds are 64 strategies, 64 total wake phrases, 250 characters
  per phrase and 64,000 total phrase bytes. Each compiled regex and DFA cache is
  limited to 64 KiB. These reject pathological configurations rather than
  creating an unbounded detector
- An external resolver consumes proposals even when an earlier wake gate blocks
  the decision. Announced speaking frames retain their original identity and
  direction; adopting one emits no duplicate boundary or interruption. Stop
  proposal resolution remains controlled by `resolve_proposals`
- `Always` mutes during every bot speech. `FirstSpeech` protects only the first
  speech. `UntilFirstBotComplete` also protects the opening wait and releases
  after an opening error before any bot audio. `FunctionCall` protects pending
  declared tools. Every policy sees every non-lifecycle frame, and results combine
  with OR, so one policy cannot hide lifecycle changes from another
- Policy admission uses the mute state at frame arrival, matching the source:
  transitions are broadcast before the triggering frame, and affect subsequent
  user frames. `Start` is forwarded before policy evaluation. To mute the first
  possible input frame itself, also set `initially_muted` and explicitly clear
  that manual mute when ready. Manual mute and policy mute are independent OR
  layers; an explicit unmute cannot disable a still-active policy
- Function-call mute deliberately preserves the stricter native tool contract:
  512 pending IDs, 512 recent completed IDs, atomic declarations, matching
  ID/name/arguments, and final-result or matching-cancel release. Intermediate,
  unknown, mismatched, or replayed results cannot silently unmute pending tools
- `AggregationEvent::UserStartStrategy` exposes wake detection/expiry, and
  `UserMuteChanged` exposes policy transitions. Deterministic callers drive
  `process_at`/`advance`; both ordinary and SmartTurn pipeline processors own
  their wake deadlines without extra per-strategy tasks. The ordinary supervisor
  remains dormant between turns to avoid task churn at low runtime task limits

Start-chain lifecycle methods and standalone controller `reset` permit fresh
sessions; aggregator cleanup cancels deadlines and clears strategy state.
Policy mute closes a current turn using the existing native mute rule: accepted
final text is preserved without immediate inference. SmartTurn invalidates any
in-flight model result and clears buffered audio at the same boundary.

`tests/fixtures/user_strategies.json` pins source provenance and manually mapped
assertions, not a Python execution baseline. Deterministic, paused-time runtime,
and independent review tests cover composition, proposals, context rejection,
tool finality, timer ownership, and SmartTurn integration. Proprietary Krisp
classification and arbitrary strategy plugins are not implemented here.
Service recommendations are limited to the native descriptors in [REALTIME.md](REALTIME.md). The bounded
unsigned LLM marker strategy and its external-completion finalizer are described
in [COMPLETION.md](COMPLETION.md); conversational inference and tool admission
remain caller-owned.

## Composable stop policies

`UserTurnConfig::stop_strategies` accepts an ordered `UserTurnStopStrategies`
list. `None` keeps the existing native default timing and frame behavior. An
empty list leaves the inactivity watchdog and terminal/mute boundaries enabled.
Explicit lists support:

- `speech_timeout(duration, wait_for_transcript)`: independent speech-silence
  and STT waits, finalized-text short circuit, interim invalidation, and
  transcript-only inactivity fallback
- `turn_analyzer(wait_for_transcript)`: an actual Smart Turn verdict plus the
  physical-stop STT budget; transcript-only input has a documented non-model
  fallback. Configure an analyzer policy when wrapping an explicit list with
  `SmartTurnUserAggregator`; unrelated policies may coexist in the same list
- `external(duration, wait_for_transcript)`: adopted external stops and consumed
  stop proposals, with late/consecutive final-transcript inactivity handling
- `external_completion()`: finalization on `UserTurnInferenceCompleted`, ignoring
  completions while speech is active

Each policy owns deterministic one-shot deadlines. The ordinary aggregator's
single supervised task drives the earliest deadline; the Smart Turn wrapper
uses its existing supervisor. A turn boundary clears every policy, even those
that did not end the turn. End/Stop draining, Cancel, mute and resumed speech
invalidate obsolete work. Timers never poll while dormant or finalize audibly
active speech. The watchdog is rearmed when an external verdict is requested.

With explicit stop policies, an adopted external stop is held while its semantic
turn remains open. It is forwarded once when that policy finalizes; another
completion policy emits its own final boundary instead. This prevents a physical
external stop from releasing a paired assistant's tool continuation prematurely.

### Deferred external-verdict notifications

`strategy.deferred()` requests an out-of-band decision instead of finalizing.
This native API deliberately has a narrower contract than Python's `deferred`:
accepted final text remains uncommitted, and **no conversational LLM inference,
output, or tool execution is started by the notification**. It is not speculative
execution or Python's automatic marker-filtered conversational inference route.

`AggregationEvent::UserTurnInferenceRequested` carries the accepted final text,
request ID, detector index and reason. Deterministic callers inspect this event
and use `complete_inference_at(id, now)` to apply a verdict. Runtime callers may
register `on_user_turn_inference_requested` to enqueue a notification to their
own external classifier. These synchronous handlers run outside conversation
locks; they must be fast and must not run model inference or block. They do not
create additional tasks. Return the request's `completion_frame()` through the
same pipeline input as speech after an actual external COMPLETE verdict. An
incomplete verdict needs no frame: the open turn awaits more evidence or its
watchdog. Keep the classifier's own calls bounded and cancel obsolete requests
in its owner; notification IDs prevent stale results from changing this turn.

Completion admits the usual final user context once, with the existing paired
assistant, tools, summary and playback invariants. New final/interim evidence,
resumed speech, reset, mute or shutdown invalidates previous IDs. Repeated
completions do nothing. Callers using bare `UserTurnInferenceCompleted` retain
source-compatible uncorrelated semantics: only the current speaking guard is
available, so delayed verdict producers should use correlated frames. Runtime
Smart Turn completions should use the pipeline frame route, which also clears
or cancels native analyzer work; the deterministic direct-completion method
has no access to a wrapper's audio worker.

```rust
use pipecat_turns::{UserTurnConfig, UserTurnStopStrategies, UserTurnStopStrategy};
use std::time::Duration;
# fn example() -> Result<(), pipecat_core::ProcessorError> {
let turn = UserTurnConfig {
    stop_strategies: Some(UserTurnStopStrategies::new(vec![
        UserTurnStopStrategy::speech_timeout(Duration::from_millis(600), true)
            .deferred(),
        UserTurnStopStrategy::external_completion(),
    ])?),
    ..Default::default()
};
# let _ = turn;
# Ok(())
# }
```

## Empty-turn recovery

`UserAggregatorConfig::empty_user_turn` defaults to
`Some(EmptyUserTurnConfig::default())`: one consecutive recovery with
`DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT`, no idle prompt. Set it to `None`
to leave all empty turns unanswered. Each optional prompt is a developer message;
`None` or an empty string disables that case. Set `max_consecutive_recoveries`
to customize the bound, or zero to disable recovery.

A turn counts as interrupted when its accepted start enables interruption and
the bot was not waiting for the user. This includes a pending response and the
opening wait before the first bot response. The classification is latched at
start: an output-side bot stop during user speech does not turn it into an idle
case. Already-announced external starts retain their native non-interrupting
contract, so they select the idle case.

Only nonempty final transcript content committed by the aggregator resets the
budget. Interim hypotheses, whitespace, rejected start-gate text, replayed
transcripts, context append/update frames, typed input and assistant responses
do not reset it. A skipped recovery consumes no budget. Muted turns, pending
declared tools, terminal End/Stop/Cancel boundaries and runtime shutdown never
start recovery. Matching final tool results or cancellation release the pending
guard; intermediate, mismatched and replayed tool events retain the existing
strict native contract.

Recovery uses the ordinary paired context-snapshot path: delivered assistant
text is sealed at interruption before the developer prompt, already-completed
tool results share the recovery's single request, and a later tool result still
owes its own continuation. No user message is invented. Without recovery, an
empty turn re-arms eligible idle detection; with recovery, the bot's response is
pending and idle remains unarmed. The existing ordinary or Smart Turn supervisor
owns timing, so recovery adds no tasks and does not bypass a model gate.

`tests/fixtures/empty_user_turn.json` pins source revision, Git blob IDs and
SHA-256 hashes, and manually maps the twelve cascade cases in
`tests/test_empty_user_turn.py`. Native deterministic, paused-time runtime and
controlled Smart Turn tests cover context ordering, budget reset, muted and
terminal turns, tool lifecycle/coalescing, idle windows and single-task reuse.
No Cargo test starts Python. Paired realtime aggregation disables recovery as
documented in [REALTIME.md](REALTIME.md); it is not provider qualification.

### Remaining strategy scope

The bounded unsigned LLM-completion marker parser and short/long automatic
re-prompts are documented in [COMPLETION.md](COMPLETION.md). Eager STT speculation,
exact/normalized matching, provisional-context output gates and native tool-effect
admission are implemented for the supported `pipecat-services::LlmProcessor`
topology in [EAGER.md](EAGER.md). Signed completion-marker filtering and eager
integration with Responses, realtime or separate provider-specific inference
processors remain unsupported. No synthetic classifier stands in for a real
verdict. Proprietary Krisp start/analyzer implementations and opaque
service-recommended strategy descriptors remain unsupported.


### Native signed history

The assistant consumes typed native provider continuation carriers. Signed
thinking is independent of delivered text: Spoken mode includes only receipt-backed
heard text, never generated but unheard text. Delivery must match an exact
provider prefix, allowing only existing trimming at committed chunk boundaries.
Interior whitespace transformations, ambiguous context changes and mismatched
provider identities fail explicitly. Rich clones preserve signatures.

Closed-prefix checkpoints preserve completed signed thoughts after a later
signature is interrupted, without admitting pending tools. Completed tool
continuation waits for pending playback before requesting the next response.
Receipt validation precedes identity consumption, permitting corrected retries.
Replacement validates the owned context suffix before mutation. Cancellation of
invalid pending delivery keeps the last valid committed history, discards the
invalid buffer, closes the scope and permits subsequent responses.

Signed scopes bound delivered chunks to 4096 and 8 MiB, playback IDs to 4096,
and closed response IDs to 512. Signatures are opaque provider data validated
for structure and bounds, not locally authenticated cryptography.

Gemini-specific Part metadata, receipt-backed truncation, strict terminal policy,
and foreign-only unsigned-history migration are described in
[the Gemini continuation contract](../pipecat-services/GEMINI.md).

## Native terminal ownership

`LLMAssistantAggregator` consumes validated `NativeToolCallOutcome` receipts at
its history boundary. The receipt seals call identity, interruption policy,
result/error/inference policy, original outcome identity, and optional complete
batch membership. Executor Started/InProgress declarations carry that same sealed batch descriptor,
establishing native active ownership before any terminal. Both declarations and
terminal frames retain their process-local proof through any provider relay; ordinary frame serialization rejects it until consumption.
A provider can use `Frame::project_native_tool_timeout_as_cancel()` only for a
validated Failed/TimedOut receipt. The projection preserves the original receipt
and permits exactly the original name/ID plus cancellation with `run_llm=true`.

A native terminal can reconstruct missing declarations and all admitted siblings.
The last sibling releases grouped inference. Results admitted before interruption
can replace only this aggregator's own provisional legacy cancellation; committed native
terminals never change. Publication identity makes duplicate clones idempotent,
including concurrent calls and changes to the public frame ID. Full batch
membership and policy must agree across all siblings. Signed declaration identity
survives response end so reconstruction preserves the original signed reasoning.
Spoken mode still waits for actual playback confirmation before inference.

Interruption keeps native-owned calls pending until an actual terminal receipt
arrives. Those active identities never evict to admit other work. Either native
declaration establishes ownership if the other is filtered; when both are lost,
the self-contained terminal reconstructs the same batch.

Raw upstream cancellation is a request and is forwarded to the executor. Raw
cancellation cannot settle a call already owned by a native batch. Legacy result,
detailed-result callbacks, and downstream legacy cancellation keep their existing
contracts. An application that validates and explicitly consumes a native receipt
and declaration batch before modifying an ordinary result opts into that legacy contract and forfeits
native restoration, immutability, and terminal-ownership guarantees downstream.

Native bookkeeping holds at most 1,024 calls and an 8 MiB conservative encoded
budget. Closed-group terminal markers retain the latest 512 entries; a live
batch's completed siblings remain until that group closes, within the same hard
call/byte bounds. Large payload Arcs can evict independently of compact sealed
publication IDs. Provisional legacy cancellations have a separate 1,024-entry/8 MiB window.
Native restoration over a legacy provisional cancellation requires that marker
to remain in this window; expired authority fails closed. Ordinary and
signed active declarations each allow 1,024 entries/8 MiB and reject new work
rather than evict unsettled authority. No bookkeeping retains a
processor or runtime. End, Stop, Cancel, and cleanup clear it. Conversation messages
are not removed by metadata eviction. Outside the identity window, a receipt
cannot adopt or overwrite an existing conversation call. Explicit context
replacement can admit a new call identity after old bookkeeping expires.

Executor admission accounts for the default consumer's policy-plus-batch storage
before callback effects. Independently saturated consumers still fail closed.
These are encoded-retention budgets, not process RSS bounds. A terminal delivered
after the assistant closes is ignored; first-hop executor admission does not
promise durable or end-to-end consumption. Cancellation never proves rollback of
external effects.

Tool metadata uses separate component limits, including copied ID keys/queues:
active pending policies and both legacy tombstone caches have explicit byte/count
bounds. Closed cancelled groups coalesce to one inference bit instead of retaining
labels during a long speech wait. Signed history's validation mirror is separately
bounded to 4,096 entries/32 MiB, measured before history mutation, with no eviction
of signed authority. Whole native batches preflight both pending ownership and
combined signed reconstruction/result additions. See [the complete state ownership
and budget table](TOOL_HISTORY.md); there is no global 8 MiB or RSS-bound claim.

## Bounded assistant completion receiver

Register `assistant.subscribe_stopped(AssistantStoppedConfig::default())` before
the first turn. `receiver.recv()` returns a non-cloneable record lease containing
immutable committed text, original start timestamp, interruption status, session
and turn identity, and an optional immutable job binding. Admission is ordered
after context/timestamp and before turn feedback, including paired interruption
and explicitly adapted playback receipts. No callback dispatcher task is created.

`handle_next` runs one cooperative, deadline-bounded handler in the caller's owned
future. Inspect `stopped_stats()` for payload-free resource and outcome counters.
End/Stop/Cancel admit their final record then close the stream; queued records
remain caller-drainable. Cancel never spawns a new async callback. A replacement
job requires a fresh assistant session. Full ordering, budgets and limits are in
[assistant completion ownership](../../docs/assistant-stopped.md).
