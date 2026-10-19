# Explicit initial-history startup

`with_initial_history` is a default-off policy for one processor-owned,
text-only initial context. It closes startup through setup acknowledgment,
validated seed admission, optional inference and model-specific first-stop
completion. It does not rebuild evolving history or provide full Python service
parity. All model names in fixtures select protocol behavior; they are not
claims that a corresponding hosted model exists.

```rust
use pipecat_gemini_live::{
    GeminiClient, GeminiConfig, GeminiError, GeminiLiveProcessor, InitialHistoryPolicy,
};

fn processor() -> Result<GeminiLiveProcessor, GeminiError> {
    let client = GeminiClient::new(GeminiConfig::new(
        "application-provided-key",
        "gemini-2.5-flash-native-audio-preview-12-2025",
    ))?
    .with_initial_history(InitialHistoryPolicy {
        inference_on_initialization: false,
    })?;
    Ok(GeminiLiveProcessor::new(client))
}
```

After pipeline Start, send an explicit `LlmContext`, including an explicitly
empty one when no initial messages are wanted. The processor sends setup first
and waits for `setupComplete` before it creates the session actor. Selected
setup includes exactly `historyConfig.initialHistoryInClientContent = true`.

## Compatibility and public controls

No fields are added to `SessionConfig`, `GeminiConfig`, or
`GeminiHistoryOptions`. Existing public configuration literals, raw Action JSON,
default seed/append/run semantics, STT, video, tools, thinking validation and raw
handle recovery keep their existing contracts.

This policy requires conversation mode. It accepts a complete `gemini-` token
with optional lowercase alphabetic infixes, canonical version `2.5` or major
`3` with optional numeric minor, and nonempty alphanumeric suffix tokens. An
optional leading `models/` is normalized. It rejects arbitrary names containing
3, embedded model names, other major versions, malformed or extra decimal
components, and noncanonical leading-zero versions. This is a deliberately
stricter subset than the pinned Python version-search expression. Model
classification establishes only local protocol selection.

Selected policy and `with_session_resumption` reject one another in either
builder order before network access. This includes paired history. Existing
raw handle resumption remains setup-only recovery without sent-action replay;
new-session recovery from evolving context is not provided by this policy.

Public `GeminiClient::connect()` rejects selected policy before opening a socket;
initialization requires the processor owner. Every actual selected session sender,
including all clones obtained through `sender()`, permanently rejects every raw
`try_send(Action, Origin)` with `Unsupported`, before payload validation or queue
admission. This includes valid and invalid audio, text, content, activity, video,
and tool actions. Initialization never unseals it. Supported processor frames
instead use private typed plans tied to the same run and generation.

Public `cancel`, `finish`, `interrupt` and `generation` controls remain available.
Finish is ordered input drain, not permission to send new raw actions and not
proof that a worker was joined. Interruption invalidates local output; it is not
remote cancellation acknowledgment.

## Whole-snapshot projection

Validation covers every original message before any seed content is admitted:

- At most 1,024 source messages; the original serialized context must fit the
  existing `max_message_bytes` limit
- Plain nonempty text only; explicit whitespace, including `" "`, is valid
- No tool calls/results, unresolved IDs, names, structured/media content or
  unsupported roles
- Every system/developer text must equal the configured setup system instruction
- Any supplied tool declarations must match setup; tool choice is absent or Auto

Projection operates on a copy. A leading system message is omitted only when
another message follows. A sole system message, all developer messages and
noninitial system messages become user turns. Ordinary user/assistant messages
become user/model turns. Original paired history is never rewritten by this
projection.

Gemini 2.5 adds a final user turn containing exactly one space when the projected
last turn is model. The resulting count still cannot exceed 1,024. Thus 1,024
source turns needing padding fail; 1,023 such turns can succeed. Gemini 3 does not
add this turn. The entire seed/completion/nudge plan is serialized and checked
before one queue admission. Both each message and their aggregate must fit
`max_message_bytes`, including padding, escaping, JSON framing and nudge bytes.

An identical accepted original snapshot is a no-op; a different one is rejected.
A failed validation or queue-full admission does not publish the seed identity,
Ready state or pending first-stop transition. Retrying after capacity recovery
can therefore initialize once with the original provenance.

## Empty context and readiness

An empty snapshot sends no client content, no realtime nudge and no automatic
greeting, even when a system instruction is configured and initialization
inference is true. It becomes Ready without a busy turn or pending first-stop
workaround. An empty snapshot is different from a snapshot containing an invalid
empty text message. To request a response, provide explicit supported seed text
or run explicitly after empty initialization.

While waiting for a seed, valid realtime audio/text/video and speaking edges are
suppressed toward the provider. Original frame forwarding and applicable input
shape checks remain. No preseed preroll/activity window or unbounded backlog is
retained, and no video is encoded for deferred replay. Run and append before
explicit initialization fail rather than implicitly selecting a seed boundary.
Termination before the seed remains legal.

Ready means successful ordered local writer admission. Gemini exposes no separate
remote acknowledgment that the history was consumed, and this API does not
invent one.

## Ordered initialization and later completion

For a nonempty projected seed:

- Gemini 2.5, inference true: padded-if-needed client content, `turnComplete=true`
- Gemini 2.5, inference false: padded-if-needed client content,
  `turnComplete=false`; retain one pending first-stop completion
- Gemini 3, inference true: client content, `turnComplete=true`, then
  `realtimeInput.text` containing exactly one space
- Gemini 3, inference false: client content, `turnComplete=false`, without nudge

Each sequence is one bounded queue command, with no other command interleaved
between its messages. This is local admission/order atomicity, not atomic network
delivery. An uncertain or partial write terminates the selected connection;
neither seed nor remainder is replayed.

A nonempty seed or first-stop transition has already published its local state
when admitted. If its queued generation becomes stale before its first write,
the connection terminates instead of silently dropping that state transition.
An empty zero-wire seed does not terminate merely because the generation changes.
Ordinary admitted run/inference-append requests retain native queued-input
semantics and their original provenance; their Gemini 3 compound write samples
the current wire generation at its first write. A later generation change
between those writes is terminal uncertainty, with no remainder replay. Ordinary
audio/activity/text and durable tool-result semantics are preserved; video keeps
its captured-generation stale-result suppression.

After silent Gemini 2.5 startup, the first `UserStoppedSpeaking` completes the
initial turn under both server and manual VAD. Manual VAD's active `activityEnd`
and the completion share one admission; failures cannot consume only half the
transition. Later stops do not repeat this workaround. Explicit run or local
interruption before that stop does not clear the pending bit, matching the pin.
An empty seed never creates that pending bit.

Explicit run after Ready sends empty `clientContent(turnComplete=true)` followed
by the Gemini 3 nudge when applicable. Append keeps native user/model roles and
its direct `run_llm` flag; silent append has no nudge, inference append adds the
Gemini 3 nudge. Python's append-to-user/false behavior plus later upstream
context-driven completion is not implemented. There is no mutable-context
reconciliation or no-context append bootstrap parity claim.

## Paired history

The optional standard paired route uses exactly the same planner before network
access and again at Start against its authenticated constructor-selected seed.
Absent seed is allowed only for the authenticated empty pair; both that case and
explicit empty seed become zero-wire Ready. Later real audio remains admissible.

Synthetic padding/nudges never enter local history. Actual input transcription
must be consumed by the actual standard user aggregator and produce its authentic
receipt before dependent assistant output is released. The exact original seed,
real user message and real assistant message appear once each. Local feedback is
consumed without replaying another provider seed. Existing finalization/barrier
behavior is retained; runtime run/append, tools, external history edits and
resumption remain rejected. The paired route is still single-use.

## Terminal ownership and evidence

Each selected run retains its session and its sole socket actor `JoinHandle`
across asynchronous setup handoff, receive processing and cleanup. The receiver
borrows that retained session and acknowledges ownership before Start returns.
The ordinary and paired selected paths share this recoverable ownership; neither
an output receipt nor a completion watch replaces the actual handle.

End first closes new input admission, finishes owned video work, orders provider
finish and drains output. Stop settles in `prepare_end`; it does not depend on a
later cleanup call. Cancel/failure requests socket cancellation, recovers the
retained session after its tracked receiver releases the guard, and joins the
actual socket actor. A blocked receiver is canceled by its exact recorded task
identity, while core retains responsibility for joining that receiver.

The ordinary selected terminal budget uses checked `finish_timeout +
write_timeout`; paired drain also uses its configured drain bound. A terminal
phase/deadline is retained across a dropped waiter. Borrowed socket shutdown keeps
the handle in the session while awaiting; on its deadline it requests abort and
then awaits that same handle. Timeout/forced outcomes are retained on reentry,
not recast as graceful success. Tokio cancellation remains cooperative; this is
not a universal real-time scheduling guarantee.

Retain a typed `Arc<GeminiLiveProcessor>` and capture
`selected_join_observer()` after Start to observe that run. The observer's
`joined()` becomes true only after the actual socket handle await resolves,
including abort-then-await. It is not set by EOF, a close frame, finished-task
status, an abort request, route completion or wrapper completion. Before
setup acknowledgment there is no actor and no observer: cancellation there proves
socket closure, not an actor join. A joined observer proves settlement, not clean
provider completion or successful playback.

Ordinary processors support sequential reuse after the old owner settles and the
previous pipeline worker has been joined. Sharing the same processor concurrently
between multiple pipelines is unsupported. Each run has separate
observer/admission identity; an old sender, receiver, video job or waiter cannot
write into or clear the new run. Paired routes cannot be reused.

## Pinned-source and test scope

Reference: ProfHepta/pipecat
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- [Gemini Live startup](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/google/gemini_live/llm.py#L1978-L2044):
  seed shape, inference choice, nudge, first-stop flag and readiness
- [Base adapter](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/adapters/base_llm_adapter.py#L449-L514):
  leading system extraction and sole-system conversion
- [Gemini adapter](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/adapters/services/gemini_adapter.py#L331-L437):
  developer/system conversion and effective-system fallback
- [Live adapter](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/adapters/services/gemini_live_adapter.py#L66-L83):
  delegation after tool-history conversion, which is outside the native subset

The empty-plus-configured-system conclusion is static call-chain analysis:
`_handle_context` first supplies init system to the adapter, but
`_create_initial_response` reprojects unchanged empty context without that
parameter and returns Ready without sending a seed. The earlier comment alone
does not establish an automatic greeting. The source manifest fixture records
this scope; no Python SDK or source-execution oracle is claimed.

Real loopback tests live in existing `live`, `vad_preroll`, `paired_history`,
`reconnect` and `review_contracts` targets. Private actor tests complement them
for deterministic queue/write/ownership boundaries. They are synthetic protocol
and local ownership witnesses, not provider traffic recordings or live response
quality certification. The pinned Python itself documents a Gemini 2.5 first-audio
limitation. No hosted endpoint, model load, SDK execution, credentials or paid API
is needed by these tests. Adding test source is not evidence that it has run;
qualification results must be reported separately for the exact source revision.
