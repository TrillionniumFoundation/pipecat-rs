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


### Selected asynchronous reporting tools

`tools::ReportingTool` is a separate opt-in registration. `RegisteredTool` struct
literals and the original `ToolCallback` signature remain source compatible.
Select one `NativeToolProgressRoute` with
`ToolExecutor::with_progress_route(route)?`, then use
`register_reporting(name, ReportingTool::new(callback))?`. A reporting callback
receives `(FunctionCall, ToolProgressReporter)` and returns the same final
`Result<serde_json::Value, String>` as an ordinary callback. Its default timeout
is 30 seconds and its interruption policy is false; registration rejects an
interruptible reporting policy or a timeout outside 1 ms through 300 seconds.

The reporter's synchronous `report(value, run_llm: Option<bool>)` returns only
when that exact immutable event has been admitted to the actual runtime queue.
The returned `ToolProgressReceipt` is not a terminal outcome and does not free
the callback slot. `receipt.await_history().await` observes the actual paired
assistant's canonical history commit; provider-command admission is a distinct
fact. The callback may await the same receipt repeatedly, or drop a wait and wait
again. No independent context-updated callback, per-report task or detached
waiter is created. Follow-on user code runs in the original owned tool task.

The executor captures one absolute deadline immediately after the task receives
its admitted lease and before polling the callback. Reporting, waiting for
history and any follow-on code all consume that original budget. Neither an
intermediate result nor a receipt wait extends it. A reporter retained past final
return, timeout, explicit cancellation or route failure cannot admit new work
and does not keep a finished runtime alive.

This API requires the complete selected OpenAI Realtime topology: ingress gate,
paired user, provider, executor, progress relay and paired assistant, in that
order and in one runtime. All real components register during setup; provider
Start validates role uniqueness, shared history, configuration and cleanup
budgets before opening admission. Constructing a route or a public tool batch
does not grant a reporting lease. Unsigned non-speculative standard aggregation
is required; selected signed/speculative calls fail before callback entry.
Ordinary final-only siblings remain supported inside this composition and are
included in its finite callback and End-drain validation.

Selected route setup is one-shot. A duplicate executor-limits or role
registration latches that route as `Failed` / `InvalidProvenance` during setup,
so provider Start rejects it before connecting. This stricter opt-in composition
check requires a new route and composition after failure; the failed route
cannot be reused for a setup retry. Ordinary pipeline startup and setup-error
severity are unchanged: `Pipeline::start` may return a worker carrying setup
diagnostics, including the nonfatal duplicate executor-limits error. When
abandoning that worker, cancel it and await its actual join.

The selected executor permits at most 16 concurrent slots and finite callback
timeouts for all its registrations. The route independently enforces at most 16
active reporting leases, 64 reports per lease, 1024 per session, four pending
reports per lease and 32 per session. Encoded progress JSON is limited to 16 KiB;
selected terminal JSON is limited to 64 KiB, with borrowed iterative validation
before serialization or cloning. Each admitted call separately reserves terminal
capacity; temporary report saturation does not consume it. Full route budgets
also charge owned encoded representations, canonical history, receipt metadata
and actual provider ledgers; they are logical retained-byte bounds, not RSS
bounds. Legacy standalone final-only payload limits are unchanged.

The first final/error/cancellation/timeout to freeze the core lease wins and
captures the last successfully admitted progress ordinal. The executor releases
its locks before publishing that immutable terminal, updating the terminal
journal or cancelling the actual callback task. A later cancellation cannot
rewrite an already frozen final. Selected journal entries retain compact truthful
identity, terminal choice, predecessor watermark, effects and first-hop admission
under the existing 8 MiB journal budget. They retain neither full result/batch
payloads nor transport authority or a terminal-payload reservation; a completed
journal entry cannot pin one of the 16 live terminal reservations. Explicit
cancellation closes reporting and
revokes progress-derived inference permission, while genuinely admitted earlier
progress remains eligible for truthful consumer disposition and history receipt.
Provider and assistant independently wait for the terminal's predecessors.

End uses the selected ingress drain gate. New calls and new selected inference
stop at the gate, while admitted callbacks may report and finish within their
original deadlines. End cannot advance merely because this executor's journal
has no active callbacks; the route also requires actual predecessor and terminal
dispositions from both consumers. Drain defaults to 35 seconds and must cover
the largest registered callback timeout plus five seconds. The real pipeline
shutdown budget must additionally cover the provider's response, operation and
close timeouts plus teardown reserve; default provider settings require at least
172 seconds rather than the pipeline's ordinary 30-second default.

The processor-local `tool_progress` test target checks registration/source
compatibility and real setup rejection. Successful receipts, before-final
inference, cancellation ordering and lifecycle joins are qualified only by the
complete Realtime/paired-assistant integration target. No fixture-produced
history acknowledgment is accepted as that evidence. This is native reporting,
not Python `on_context_updated` callback-API parity or live-model qualification.

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
assistant-stopped callback bridge, generic event system, provider integration,
or audible playback parity is implied by the full-response callback utility.

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

## Generic idle-frame monitoring

`IdleFrameProcessor::new(config, filter, callback)` observes frame traffic rather
than conversational user-turn state. It is separate from `UserIdleTracker` and
runner session timeouts, and is exported by `pipecat::processors` without an
optional feature. `IdleCallback` is an owned async `Fn() -> Result<(),
ProcessorError>` future. A caller can notify its own channel or perform its own
explicitly configured action; this processor does not invent a bot prompt.

Fixed Python references at `af9f9da9d7afddaa8811c0e0251b32566c980bdf`:
- `src/pipecat/processors/idle_frame_processor.py`, blob
  `9426006be4a7b9653e0cfc39882c765ef40476ed`
- `tests/test_idle_frame_processor.py`, blob
  `22bfb7595fb49ba0e0e584915d5ae03145f4cbdc`, including the regression preserving
  activity signalled during a callback

Start starts one monitor even when that frame does not match the filter.
Repeated Start at the processor boundary does not duplicate or restart it.
The native Pipeline sender separately rejects duplicate Start ingress. Every frame is submitted for
forwarding with its original identity, metadata, semantic carriers and direction;
matching activity is signalled only after that forwarding call succeeds. Core
queue, stale-frame and shutdown policies still apply; this is not a downstream
delivery receipt. Either
direction can reset the timer. The Rust filter is `Option<FramePredicate>`:
`None` observes all frames. An explicit predicate chooses a subset and must be
pure, fast and nonblocking. It is evaluated before forwarding (without cloning
the frame), outside the state lock. Predicate panics follow the native core's
processor-panic policy. This predicate API does not automatically implement
Python's `isinstance` class/subclass list. The source's missing or empty type
list corresponds to `None`, not a predicate that always returns false.

A single pending activity bit coalesces bursts. Consuming activity starts a full
idle interval; reset timing is consumption time, not a promise about exact
arrival timestamps. On timeout, one callback is awaited. Callbacks never overlap.
Only after it finishes does the next full idle interval begin. Activity arriving
during the callback remains pending and is consumed afterward. Continued silence
therefore produces repeated callbacks spaced by callback duration plus the idle
interval, not a fixed-rate schedule. A simultaneous activity/timeout race has no
promised winner. Callback errors terminate this run's monitor and report through
the owned task's core error handling using a fixed nonfatal diagnostic (callback
error payload/category/fatal fields are not propagated); later activity and repeated Start
do not silently restart a failed callback loop.

The default idle interval is five seconds (positive, at most twenty-four hours);
the callback deadline is ten seconds (positive, at most sixty seconds). These
finite bounds are a native safety profile, not limits of the Python float API.
Callback creation/poll panics use a fixed diagnostic, but the global Rust panic
hook can still log the original payload. Deadlines and cancellation are
cooperative; blocking callback code cannot be preempted, and callback/future
or panic-payload destructors must not panic. Callbacks must not detach work.
They receive no Frame, ProcessorContext or processor object, an explicit API
adaptation that avoids granting implicit pipeline-output authority. Cancellation
does not undo external effects already performed by application code.

Exactly one supervisor is registered with the core task owner. Ordinary
interruption preserves monitoring; terminal End/Stop, Cancel and cleanup close
it, including any pending callback. `prepare_end` closes the monitor before the
core drains tracked tasks, so a recurring timer cannot deadlock graceful End or
Stop. Stop does not depend on a later cleanup hook. Closure is idempotent and
cleanup waits for the callback future to be released. These lifecycle rules are
a deliberate improvement over the Python class, which only explicitly cancels
its timer in cleanup. They do not turn a lifecycle terminal into an idle event.

Use a separate processor instance for concurrent pipelines. Sequential reuse is
supported only after the previous worker has been joined; setup creates isolated
run state, so old task cleanup cannot alter the next run. This explicitly differs
from the source's retained cancelled-task reference, which prevents restart after
cleanup. No frame history, unbounded event queue or detached timer is retained.

### Slice 55 verification, 2026-10-14 UTC

Eight real Pipeline integration tests and three ownership tests run with Tokio's
paused clock for deterministic interval assertions. They cover either-direction
activity, explicit nonmatches, interruption survival, repeated serial callbacks,
activity during callback, a full interval after completion, all five terminal
forms during waiting and active callbacks, callback failure/panic/deadline,
startup task-budget rejection and clean reuse, native continuation identity,
initial StartDetailed, internal repeated-start idempotence, unpolled future
release, and worker-owner drop in both phases. Terminal/reuse paths collect and
join workers; the owner-drop witness waits for monitor release and core cleanup.
This is runtime execution with a controlled clock, not a wall-clock latency SLA.

The selected package suites pass 115 default-feature and 158 all-feature tests;
the facade export has one test. Strict all-target Clippy and selected formatting
pass. These overlapping counts are not additive and do not qualify the entire
workspace or external provider/device behavior.

Initial fixture checks exposed three test assumptions: DetailedStart needs an
explicit payload, duplicate Start is rejected at native sender ingress, and a
fatal background-task limit fails Pipeline startup before a worker is returned.
Fixtures were corrected without relaxing these runtime boundaries. Internal
repeated-start behavior uses a labelled wrapper/private-hook witness instead;
startup rejection is checked as an error followed by successful clean reuse.
Final focused and package checks pass; the initial logs remain in the evidence
archive alongside the corrected runs.


## Latest-context notification gate

`GatedLlmContextAggregator` is a single-slot context gate, exported through
`pipecat::processors` without optional features. It differs from the generic
`GatedAggregator` FIFO: while closed, each accepted context replaces the previous
one. An application obtains this run's `current_notifier()` after pipeline setup
and calls `notify()` when its own prerequisite is ready. No provider, background
application job or global notification bus is created by the gate.

Fixed Python references from `ProfHepta/pipecat` commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:
- `src/pipecat/processors/aggregators/gated_llm_context.py`, blob
  `3dfea5c6e6d54c5e17aebf909d7c5375b18c79a2`
- `src/pipecat/utils/sync/event_notifier.py`, blob
  `d55d3fe2955b1a87179ac0dc7388f19014ba83f7`
- `src/pipecat/utils/sync/base_notifier.py`, blob
  `1cbf5ee193a9c444b5b16398042ea62dc4bf2655`

Default `start_open=false` holds the latest `LlmContext` or
`LlmContextDetailed`. `start_open=true` lets the first accepted context pass in
its original direction, then switches to notification mode. A consumed signal
releases the current slot downstream, preserving the original frame identity,
metadata and native semantic carriers. Non-context frames pass unchanged in their
original direction. The signal does not permanently open the gate.

Notifications coalesce into one pending bit, rather than counting permits.
An empty signal grants no future permission **after the supervisor consumes it**.
If a context arrives after `notify()` but before that consumption, the pending
signal may release it, as with the source EventNotifier. Notification acceptance
and payload-free snapshot counters are local diagnostics, not delivery or provider
completion receipts. The slot is taken before output admission, so an independently
arriving replacement cannot be erased by completion of the previous release.

Native frames contain value snapshots. This API does not reproduce Python shared
mutable context objects: changing an application's context after submission does
not change an already buffered message snapshot. Opaque native tools and carriers
keep their existing shared identities; this is not a deep clone of application
closure state. It also does not copy the source's
accidental duplicate Start forwarding. Native causal and lifecycle safeguards are
explicit adaptations, not claims of identical Python scheduling.

The default inspectable payload budget is 4 MiB, configurable from one byte to
64 MiB. Counting writes do not create a second serialized payload buffer. The
accounting includes public context data, metadata, envelope strings, shallow
retained state and known native continuation bytes. Direct tool schemas are
counted without serializing or replacing their native handlers. Opaque callback
captures, shared carrier internals and runtime ownership are not a whole-heap
memory guarantee. An invalid/over-budget context does not replace the previous
slot or consume the first-context bypass. No payload is truncated.

Each setup creates isolated state and one core-owned supervisor. Keep the
processor in an Arc to obtain its current run's notifier after Pipeline::start.
An old handle does not follow the processor into a new run; closed handles fail
explicitly. Use different instances for concurrent pipelines, and join a prior
worker before sequential reuse. An already-in-use setup diagnostic is not an
isolation boundary because the core cleanup hook has no invocation identity.
`start_open` is reset from configuration at each native setup, rather than
retaining Python's mutated flag across runs.

A delayed frame retains its original invocation context and causal fence.
Notification cannot refresh an old generation. Interruption discards stale
buffered work while leaving the supervisor available for fresh contexts.
End/Stop prepare_end closes the supervisor before the core drains tasks;
Cancel, cleanup and worker-owner drop also retire it. Unnotified buffered work
is discarded, never automatically flushed at shutdown. A notification selects
a frame by taking the slot under its lock. That already-selected synchronous
release may finish during closure, but closure joins it before returning or
forwarding a terminal. Cancellation cannot undo output already admitted to a
downstream queue or external provider effects.

Release uses native synchronous, fail-fast queue admission rather than Python's
awaitable push. A full destination reports an owned-task error, retires the
supervisor, drops its retained work and rejects further notifications. It does
not silently retry a potentially meaningful request. Core error policy still
decides whether the surrounding pipeline continues or terminates. Starting a
new run requires complete teardown of the previous one.

### Slice 57 verification, 2026-10-14 UTC

Thirteen integration tests exercise real native Pipeline workers: latest-only
replacement for both context variants, start-open direction and reset, consumed
empty signals, original frame metadata and opaque carrier/handler identity,
non-context passthrough, interruption cleanup, an independently retained original
fence with local clearing deliberately bypassed, every terminal form, worker
owner drop, joined reuse and stale handles, payload rejection, spawn admission
rollback, and visible full-destination failure. Four private ownership tests
cover unpolled cancellation, controlled take/replacement publication ordering,
empty-notify-before-context timing, and failed first-output admission. These
controlled private calls are not presented as a real awaitable capacity queue.

The facade's `gated_llm_context_http` test uses actual loopback HTTP/SSE through
LlmProcessor. Before notification the provider request count is zero; only the
latest of contexts A/B appears in the one received request. It verifies returned
LLM text, consumes an empty notification, retains a further C context, then proves
End discards C without another request. The server keeps accepting until pipeline
cleanup completes; both owners close and join before assertions, including the
observation-timeout path. No paid provider or external credential is used.

The processor package's default suite passes 132 tests and its all-feature suite
175; the facade HTTP test passes separately. Strict all-target Clippy passes for
the default processor/facade targets and all-feature processors; selected
formatting passes. Initial test compile assumptions
about helper APIs and an unsupported native realtime carrier plus direct-tool
schema combination were corrected without relaxing core validation. Their first
failure logs remain in the evidence alongside the final passing runs. These are
selected source-bound checks, not a new full-workspace or live-provider claim.

## Serialized asynchronous frame stream

`AsyncGeneratorProcessor` bridges the existing synchronous
`pipecat_protocol::adapters::FrameSerializer` into one application-owned async
receiver. It is available through `pipecat::processors` without an optional
feature. The bridge neither creates a transport connection nor sends data to an
external service. Applications decide how to consume the returned
`WireMessage::Text` and `WireMessage::Binary` values.

The fixed Python contract is `ProfHepta/pipecat` commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/processors/async_generator.py`, blob
`643b5eacc84c8d7ddb9b42c92e79a7ed3be5b56b`. The referenced base serializer is
`src/pipecat/serializers/base_serializer.py`, blob
`0de9e403da940e48573f8e12c57fe1bc5838f088`.

The original frame is first forwarded in its original direction, preserving ID,
metadata and native semantic carriers. Only successful forward queue admission
is followed by serialization. This is not a receipt that the next processor has
finished handling the frame. A later serialization or stream-capacity failure
cannot retract that original forwarding and must not be interpreted as "the
frame was never forwarded".

Both upstream and downstream frames are offered to the serializer, as in the
source, even if a concrete transport would normally encode only downstream data.
Place this processor at an intentional export boundary. It does not reinterpret
upstream data as a command or grant authority to reinject received bytes. Before
calling any custom serializer, the existing native continuation/eager/speculation/
tool authority guard rejects a wire copy with unconsumed native semantics.
The already-forwarded original keeps those semantics unchanged. Concrete codecs
retain their own protocol-specific validation.

Start and Interruption are ordinary serializer inputs. A None result or an empty
text/binary result is ignored; nonempty results enter the receiver's FIFO.
Interruption does not erase data that was already queued. A serialized result
from an old invocation that races interruption is rejected at publication by its
original causal fence; notification or consumer activity cannot renew that
invocation. A discarded result does not roll back mutations already performed
inside the stateful serializer; that same run continues with its current codec
state. This publication safeguard is a native adaptation. End and Cancel,
including detailed variants, are forwarded and close the stream without invoking
the serializer. They do not flush codec tails or call serializer.setup: neither
operation appears in the source processor.

Configuration limits the queued message count, each retained message allocation,
and total queued allocation bytes. Defaults are 1,024 messages, 1 MiB per message
and 4 MiB queued; maximum configurable values are 65,536 messages, 16 MiB per
message and 64 MiB queued. String/Vec spare capacity is charged, not only logical
length. These bounds apply after serialization returns. They do not constrain
codec temporary allocations, application closure captures, or messages the
consumer has already removed and retained elsewhere. No message is truncated.

A factory creates a fresh serializer and isolated session for each setup. After
Pipeline::start, `take_receiver()` returns that session's receiver once. Its
`recv()` yields queued values and then None after closure; status and a finite
error reason are separately inspectable without payloads. Old receivers never
follow the processor into a new session. Use distinct processor instances for
concurrent pipelines and join the previous worker before sequential reuse.
The setup-in-use diagnostic is not a concurrency isolation boundary because core
cleanup has no invocation identity.

The serializer's API is synchronous and stateful. Calls are serialized, and no
queue or lifecycle lock is held while application serialization runs. There is
no background dispatcher, callback task, automatic retry or promise of a
preemptive timeout. Serializers must be fast and nonblocking; cleanup cannot
undo their external effects or forcibly interrupt a blocking synchronous call.

Closure does not consume queue capacity or enqueue a sentinel. End/Cancel and
ordinary cleanup leave previously queued data available to drain before EOF.
Native Stop is offered to the serializer, then closes on every exit path,
including an error. A dropped receiver instead releases its queued data and
retires the stream. Slow consumers encounter explicit count/byte failure rather
than an unbounded backlog or indefinite producer wait. Codec, authority and
capacity failures close the wire stream, record a content-free reason when they cause the first closure, and leave
already queued data drainable. First closure wins: a later failure does not
replace an earlier clean EOF outcome. The surrounding pipeline's core
error policy remains authoritative; later frames can still be forwarded without
restarting the closed stream.

Queue closure is independent of a running serializer. An idle codec is released
on closure; an in-flight synchronous codec can only be released when it returns.
A result arriving after closure is not enqueued. Consumer Drop and the terminal close implementation do not wait on the serial
codec permit before closing the queue. However, the core runs normal and system
lanes within one actor task: a synchronous codec can delay processing a requested
Cancel. Cancel admission alone neither closes this stream nor advances its causal
generation. A codec result can therefore be queued before the actor processes
Cancel; that result remains part of the FIFO drain. EOF and codec destruction may
wait for the synchronous call to return. Interruption admission separately
advances the epoch, so its original-fence rejection is a different guarantee.
Caught factory/serializer panics use fixed diagnostics, but the global Rust
panic hook may still log application panic text. Panicking destructors and panic
payload destructors are unsupported and may reach the core panic path. These
bounds, explicit errors, Stop/cleanup closure and fresh-session factory semantics
are native adaptations to the source's unbounded shared queue.

### Slice 58 verification, 2026-10-14 UTC

Sixteen real-Pipeline tests cover original frame identity/direction and forward
ordering, text/binary/UTF-8 and None/empty results, exact count and retained
allocation budgets, spare capacity, queue failure after forwarding, full-queue
terminal closure and FIFO drain, codec error/panic, native-authority rejection
before custom serializer effects, receiver/worker drop, every terminal form,
joined reuse and old-receiver isolation, failed factory startup followed by a
fresh setup, and stale normal/system/uninterruptible invocations. A bounded
multi-threaded codec fixture proves receiver Drop can close while synchronous
serialization is running. A distinct real Cancel case proves the actual
cooperative order: queued data, the already-running codec's result, then EOF.

The facade's `async_generator_codec` test runs the production
RtviClientFrameSerializer through two real Pipeline sessions, one ended and one
cancelled. Both directions preserve original native frames and local identities.
Wire values are compared with the actual codec and independently parsed as RTVI
JSON and base64 PCM with exact sample rate/channel metadata. The fresh factory
resets codec audio IDs, while the first run's still-unread receiver remains
isolated from the second run. End/Cancel never call the codec. Owners are closed
and joined before assertions, including observation failures. This is a local
codec/pipeline witness, not a browser, network transport or playback receipt.

The processor default suite passes 148 tests and its all-feature suite 191; the
facade codec test passes separately. Strict all-target Clippy passes for the
default processor/facade targets and all-feature processors; selected formatting
passes. Initial tests incorrectly assumed graceful
End/Stop use the ordinary bounded output slot and that a requested Cancel could
preempt a synchronous codec. Those fixture assumptions were corrected to the
existing core behavior, without changing core admission or cancellation. Initial
failure logs are retained alongside final passing runs. No full-workspace or
live-provider qualification is implied.

## Producer/Consumer subscriptions

The default exports include a bounded native `ProducerProcessor` /
`ConsumerProcessor` pair for independently owned pipelines, with asynchronous
read-only filtering and owned Text/PCM transforms. See
[FRAME_SUBSCRIPTION.md](FRAME_SUBSCRIPTION.md) for the fixed Python contract,
dual-runtime causal admission, partial fanout, resource budgets, ownership and
explicit native differences.

## Frame-driven wake notifications

`WakeNotifierFilter` adds an explicit selector → asynchronous predicate →
notification → original-frame forwarding adapter, plus a fixed-session,
dual-fenced `GatedLlmContextNotifier` constructor. Existing `FunctionFilter`
closures could already express basic notifications. See
[WAKE_NOTIFIER.md](WAKE_NOTIFIER.md) for source ordering, lifecycle behavior,
cooperative cancellation and the limits of asynchronous side-effect rollback.

## Legacy participant wake-check migration

The default-off `wake-check` feature adds a bounded `WakeCheckFilter` migration
adapter for the deprecated Python filter. It retains per-participant sliding
windows, exact cross-frame concatenation and wake-prefix suffix forwarding.
Existing WakePhrase turn strategies remain recommended for new pipelines. See
[WAKE_CHECK.md](WAKE_CHECK.md) for the fixed source, Unicode profile, resource and
lifecycle adaptations, and verification scope.
