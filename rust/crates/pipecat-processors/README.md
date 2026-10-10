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
