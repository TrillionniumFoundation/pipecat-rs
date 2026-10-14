# Native reusable processors

Source baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

Ports `IdentityFilter`, `NullFilter`, `FrameFilter`, `FunctionFilter` from
`src/pipecat/processors/filters` and `GatedAggregator` from
`src/pipecat/processors/aggregators/gated.py`.

Frame matching is a Rust predicate rather than a Python class tuple. Async
FunctionFilter preserves the source's predicate invocation even for bypassed
frames or directions. Essential lifecycle frames always pass. Identity preserves
frame identity, metadata and direction. The gate preserves the source's unusual
ordering: the opening frame precedes stored frames, including the previous closing
marker; opposite-direction and system frames pass directly.

Intentional bounded-runtime differences:
- Stop passes lifecycle filters, avoiding a terminal hang
- Gates require finite capacity and report overflow explicitly
- End/Stop flush the closed gate before forwarding terminal frames
- Interruption removes interruptible buffered frames; cleanup releases all buffers

No source parity is claimed for the remaining audio/video processors, wake-word
filters, framework adapters, metrics observers, sentence/text transformers,
summarizers or other aggregators. Tests exercise the native pipeline directly.

## Function execution

`tools::ToolExecutor` connects `FunctionCallsStarted` to explicitly registered Rust
async callbacks and emits in-progress/result frames. It admits an entire batch's
placeholders before any result, with a common group identity. The assistant context
aggregator controls the next LLM request when the group completes.

Admission bounds batch size, concurrent callbacks and serialized argument bytes.
Active call IDs remain protected even after the bounded recent-ID history rotates.
Repeated IDs are suppressed, rather than executing external side effects twice.
Unknown tools, capacity failures, callback errors, panics and deadlines become
terminal matching tool results. Invalid identities or argument shapes are rejected
before registration in conversation history. This is in-memory replay protection,
not durable exactly-once execution. Application callbacks remain responsible for
argument semantics, authorization and external-service idempotency.

Interruption drops opted-in callback futures through the core task owner. It cannot
undo side effects already performed or cancel detached work created by a callback.
No tool is registered automatically, and model output cannot create new callbacks.

The facade integration test `tool_conversation` exercises a real loopback HTTP
completion with two parallel tools, both results in the shared context, and exactly
one upstream continuation producing the final assistant response.

### Tool cancellation outcomes

`ToolExecutor::outcomes()` exposes a bounded local settlement journal. Each batch
reserves all its active journal entries before publishing identities or admitting
callbacks. Active entries are never evicted; terminal entries retain the latest
`duplicate_window` receipts, with cumulative terminal, eviction and unadmitted
counters. An aggregate 8 MiB encoded-payload budget covers active and terminal
call identities and arguments; terminal receipts are evicted first, and a batch
whose active reservations cannot fit is rejected atomically before cloning call
payloads or publishing identities. A separate 8 MiB limit bounds recent ID strings.
Call ID, function name and serialized arguments are each bounded by
`max_argument_bytes`. Snapshot byte and eviction counters expose these limits.
Shared immutable call payloads avoid redundant audit copies; the journal does not
retain callback result bodies. JSON container/entry overhead is additionally
bounded by those payload and entry-count limits.

Each executor terminal frame carries `Frame::native_tool_outcome()`: immutable
call identity, interruption policy, terminal state, reason, and either
`NotStarted` or `ExternalEffectsUnknown`. A cancellation request is not a terminal
acknowledgment. Matching requests are consumed; the eventual acknowledgment keeps
an explicit request's `run_llm`, while runtime/speculation cancellation does not
request inference. Wrong-name requests for an active identity are rejected.
Unknown and already-completed requests are consumed without fabricating an
acknowledgment. Native receipt metadata describes identity and terminal state; it
does not authenticate arbitrary application edits to the callback result body.

A lease exists before spawn and passes through a bounded one-shot handoff after
spawn's admission locks are released. Rejection, abort before first poll, and
in-flight abort all settle exactly once without spawning from `Drop`. Timeout,
callback panic, and callback errors remain failed tool results. A callback's
return never proves that an external effect can be rolled back; the executor
never retries effects.

Result admission is fenced against the original invocation and inherited origins.
A stale result becomes a cancellation acknowledgment. Admitted results retain
their existing uninterruptible behavior; cancellation acknowledgments are also
uninterruptible, preserving the single terminal decision across later interruptions. End and Stop drain owned work;
Cancel/cleanup abort it. Non-interruptible siblings survive ordinary interruption.

Journal admission describes only the first downstream hop: `Admitted`, `Closed`,
`Undelivered`, or `ShutdownRace`. It never proves provider receipt. Full internal
or output queues fail the session closed instead of silently leaving a live call
pending. A closing runtime may make notification impossible; the retained receipt
still records local settlement and the admission limit. No queue retry, unbounded
channel, detached notification task, or end-to-end delivery guarantee is implied.
Providers must close pending protocol state when their session shuts down and
consume native provenance explicitly before wire encoding.

Exactly-once settlement applies to one admitted lease. Recent-ID and terminal
retention are finite: reused IDs may execute again after eviction. This is neither
durable global execution identity nor an exactly-once external-effect guarantee.

### Immutable terminal history and batch budgets

Native tool publications seal their complete result/error/run policy and receive
an immutable publication ID. Their shared `NativeToolCallBatch` records every
admitted call and interruption policy, with an internal identity independent of
public frame IDs and a fresh executor-generated group label. The same descriptor
is attached to Started and InProgress frames and retained until a native consumer
validates/takes it. A downstream executor forwards already-owned declarations
without executing them again. Unconsumed descriptors cannot cross a serializer.
Admission measures
borrowed calls before cloning and conservatively reserves the complete reachable
batch plus each common-consumer policy record per active lease. This deliberately
charges shared batches more than once so a surviving sibling remains covered.

Terminal journal retention includes reachable batch/result bytes. It evicts old
receipts or skips an oversized receipt while keeping terminal/eviction counters;
effects are never retried. Callback values are counted with a streaming writer
before making the sealed copy. Encoded values over 8 MiB (or error strings over
4 MiB before their duplicated error representation) become an explicit Failed /
ResultTooLarge outcome. External effects remain unknown if the callback entered.
A first-hop receipt still does not prove provider or assistant consumption.

The default assistant reservation also charges two additional call-ID copies per
native member (map key and completed-order identity). This cost is reserved before
callback effects, together with policy and the conservatively repeated whole batch.
Independent consumer mirrors and legacy compatibility metadata have separately
documented count/encoded-byte limits.

## DTMF aggregation

`DtmfAggregator::new(DtmfAggregatorConfig { ..Default::default() })` converts
native `InputDtmf` keypad frames into finalized `TranscriptionDetailed` frames.
Place it after a transport/RTVI decoder and before `LLMUserAggregator`. It is also
available through `pipecat::processors`. No provider, decoder, user-turn strategy
or realtime aggregation mode is replaced or implicitly reconfigured.

Defaults are a two-second idle timeout, `#` terminator, `DTMF: ` prefix, 256-digit
capacity and a 60-second maximum sequence lifetime. The terminator is included.
Each first key interrupts both directions; raw keys retain their identity and
original direction. Derived transcripts always go downstream, with empty user
ID, UTC timestamp and `finalized: true`.

The telephone keys `0–9`, `*`, `#` are accepted. Configuration rejects extended
A–D keys as terminators, zero/oversized limits, and durations outside 1ms–1h.
Prefix is limited to 4096 UTF-8 bytes and configured digit capacity to 4096.
Extended input keys and digit overflow discard the whole pending entry and emit
an error without echoing keypad content. An exact-deadline key begins a new entry.

Graceful End flushes. External interruption, Cancel, Stop and cleanup discard
pending entries; Cancel cannot preserve the Python normal-frame flush because
native urgent cancellation preempts downstream normal processing. A single owned
timer is canceled before End/Stop drains. Reuse after cleanup is supported, with
separate instances required for concurrent pipelines. Native duplicate Start is
rejected by the pipeline before processor delivery.

Self-interruption retains only exact previously admitted input scopes in a
bounded, time-limited bridge; inherited origins and external interruption fences
remain intact. At most 64 most-recent scopes are retained for the fixed maximum
sequence duration. A stale scope outside this bound cannot seed a transcript.
Output always uses the original invocation fence, including timer callbacks and
inherited owner closure. Queue saturation fails explicitly rather than silently
truncating input or retrying a possibly committed transcript.

## Full generated responses

`LlmFullResponseAggregator` is a standalone port of
`processors/aggregators/llm_response.py` at the pinned source baseline. Construct it
with a `FullResponseAggregatorConfig` and one `FullResponseCallback`. The facade
also exports it through `pipecat::processors`; no optional feature is required.
It does not read or modify context, spoken history, tools, jobs, or playback state.

LLM and Vision response-start markers enable collection without clearing existing
text. Only `LlmText` and `VisionText` append. Exact UTF-8 whitespace and markers are
preserved regardless of `append_to_context`, `skip_tts`, and inter-frame spacing
flags. Compact, detailed LLM, and Vision response-end markers each admit a
completion with `completed: true`; interruption admits `completed: false`. An
empty or inactive boundary still produces a completion. Text outside a start is
ignored. The completed flag means only that a response-end marker was observed.

Every input frame is forwarded without rebuilding its envelope, direction,
metadata, or local tags. Callback admission precedes forwarding the triggering
frame; callbacks run asynchronously and may finish in a different order. Payloads
include the triggering frame ID/direction, per-instance session ID, and boundary
sequence. Repeated identical end frames are separate boundaries. Correlation does
not grant authority to emit new pipeline work or complete a different job.

### Bounds and callback ownership

One callback is fixed at construction; there is no callback registry or pending
completion queue. Defaults are 1 MiB current-response text, 16 admitted callbacks,
8 MiB retained callback text capacity, and a ten-second callback deadline. Config
caps are 16 MiB, 1024 callbacks, 64 MiB, and sixty seconds respectively; deadline
minimum is one millisecond. Text growth is geometrically reserved but capped by
the response limit. Callback admission charges String allocation capacity rather
than just its length, so spare capacity cannot bypass the aggregate byte bound.
Empty callbacks consume count capacity. Application-retained copies and memory
allocated by application callbacks are outside these utility-owned limits.

Text overflow discards the entire affected response and reports an error once.
Until its next end/interruption, further tokens and repeated starts cannot turn
that discarded response into a successful prefix. That boundary reports the
discard and resets the buffer. Callback budget exhaustion rejects only that
boundary, clears its buffer, and reports an error. There is no implicit retry.
Both error paths still attempt to forward the original frame. Runtime task limits
and queue backpressure remain applicable: task admission errors are reported after
forwarding; a failed output enqueue fails the normal native pipeline contract.
This is not an end-to-end frame-delivery guarantee.

Callbacks use the core task owner and survive ordinary interruption, including an
interrupted partial completion delivered before a new response. End/Stop close
admission, discard unfinished text, and drain already admitted callbacks before
forwarding their lifecycle terminal. They never synthesize another completion.
A shorter core shutdown deadline can cancel the drain and fail the pipeline;
configure that deadline consistently with the callback timeout.
Cancel, cleanup, or owner drop cancel callbacks and release their reservations,
including callbacks never polled. Cancel never attempts to spawn after the runtime
closes admission. Direct cleanup is idempotent and awaits callback cancellation;
separate instances are required for concurrent pipelines. Join the previous
worker before sequential reuse, including after Stop. Session IDs are distinct.
The already-in-use setup diagnostic is not a concurrency-isolation guarantee: the
core cleanup hook has no invocation identity.

Callback errors, panics while creating or polling the callback future, and
timeouts become payload-free errors when application destructors do not panic. `stats()` retains only bounded scalar counts and current
capacity usage, including failed admission and cancelled work. The process-global
Rust panic hook may still log application panic payloads. Panicking callback-future
or panic-payload destructors are unsupported; they can reach the core task-panic
path, fail the pipeline, and expose their application panic text. Deadlines and cleanup
are cooperative: a callback that blocks its polling thread can defeat timers and
stall the runtime. Callback implementations must yield and must not detach work.
Cancellation does not roll back already performed external effects. The utility
does not retain or provide a `ProcessorContext` to application callbacks.

These are deliberate bounded-runtime differences from the source's unbounded
string and per-handler task fan-out. Native urgent interruption may invalidate a
racing old token/start invocation; such stale input cannot seed the next response.
Callback admission closes once graceful shutdown starts. Source cleanup's
unbounded wait is replaced by owned cancellation and cooperative deadlines. No
assistant-stopped callback bridge, latest-context notifier gate, generic event
system, provider integration, or audible playback parity is implied.

## Stateless text transformation

`StatelessTextTransformer::new_sync(config, transform)` accepts a synchronous
`Fn(String) -> Result<String, ProcessorError>`;
`new_async(config, AsyncTextTransform)` accepts an owned async callback. Both are
available through `pipecat::processors` without an optional feature. This is an
application-supplied hook, not a provider call or an automatic content policy.

The pinned contract is `ProfHepta/pipecat` commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/processors/text_transformer.py` lines 15–50, blob
`dffee7ac4bc3155f8f8997aa66e3057869ee1949`. Its `isinstance(TextFrame)` includes
Text, LLMText, VisionText, AggregatedText, TTSText, Transcription, Interim/Eager
Transcription, Translation, and InputTextRaw. Rust maps these ten Python classes
to fourteen enum variants, including the four detailed transcript forms.
The checked-in `tests/fixtures/text_transformer_source.json` derives the class
set from the pinned frames.py AST (blob
`79dea3e32f5097a048d03b49f48031811fa1eca6`), not from searching for fields named
`text`. The offline generator checks that exact blob; Rust tests/runtime do not
invoke Python.

Every matching input invokes the callback, including empty input. Every success
creates a **new ordinary TextFrame downstream**, including an empty string and
an upstream input. This deliberately resets ID, metadata, PTS, routing hints,
subclass and text flags to source defaults: skip_tts=None,
includes_inter_frame_spaces=false, append_to_context=true. Position this hook
with care: applying it to TTS/transcription output changes its frame semantics.
Non-text frames retain their identity, payload and direction. LLM thought text,
markers, TTSSpeak, AggregatedTextProgress and native prepared TtsTextInput are
not TextFrame descendants and are not transformed. OpenClawText is likewise a
DataFrame in pinned services/openclaw/frames.py (blob
648f4a6b22eb60e41d7db23a5d445234b454188b), so it passes through. The hook cannot return a
non-string/null value; use an empty String for the source's empty-output case.

Native semantic carriers on matching input are rejected before calling the
hook, using the existing continuation/eager/speculation/tool guard. They cannot
be silently stripped into apparently ordinary text. Nonmatching frames keep
those carriers while passing through. Successful derived output uses the
original ProcessorContext, so an interrupted old invocation cannot acquire a
fresh causal generation by constructing a new Frame.

Default limits are 1 MiB input and output UTF-8 bytes and a ten-second callback
deadline. Configurable byte limits are 1..=16 MiB and the deadline is positive
and at most sixty seconds. Over-limit input is rejected before the callback;
over-limit output is rejected whole, without truncation. These are accepted
string-size bounds, not a bound on memory allocated by application callbacks.
Callback errors propagate as supplied; timeout and caught panic use nonfatal
native processor errors, with no transformed output or automatic retry. The
core's configured error policy still decides whether to continue, end or cancel. Callback factory
and poll panics are converted to a fixed diagnostic; the global Rust panic hook
can still log the original payload. Panicking destructors are unsupported.

No task is spawned. The callback future belongs to the original process-frame
invocation and is dropped when that invocation is cancelled. Deadlines and
cancellation are cooperative: a synchronous callback or future that blocks its
polling thread cannot be preempted by this timer. Callbacks must yield, must not
detach work, and receive no ProcessorContext. Cancellation cannot undo external
effects already performed. Graceful End/Stop preserve the core's ordered-drain
contract and can wait for the bounded current callback; urgent Cancel and
interruption follow core cancellation semantics. These bounded/native-authority
rules are explicit differences from the unbounded Python callback.

### Slice 54 verification, 2026-10-14 UTC

Selected local checks use actual native Pipeline workers and injected application
callbacks, with no provider accounts or browser. Twelve focused tests cover the
source-derived class set, both directions, new identity/default fields, literal
UTF-8 and empty results, non-text passthrough, sync/async errors and factory/poll
panics, exact byte budgets, cooperative timeout and owned-future Drop,
interruption/Cancel/owner drop, system InputText causal fencing, End/Stop ordered
drain, and native speculation rejection before hook effects. The facade export
has a separate passing test. The processor package's all-feature suite passes
147 tests; strict all-target Clippy and selected formatting also pass.

An initial test incorrectly expected Stop to discard a pending callback and
hit its observation deadline. That test assumption was corrected to the core's
graceful ordered-drain contract; the processor implementation did not need a
Stop-specific behavior change. The final focused and package suites pass.
The source-AST fixture was independently regenerated and compared byte-for-byte.
These are selected source-bound checks, not a new full-workspace qualification.
