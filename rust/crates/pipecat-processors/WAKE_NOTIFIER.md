# Frame-driven wake notification adapter

`WakeNotifierFilter` is available through the default facade. Existing
`FunctionFilter` closures could already implement a simple match-and-notify
sequence. This adds the explicit adapter contract and tests; it does not claim
that notification was previously impossible.

## Fixed source and native selection

[Python WakeNotifierFilter](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/processors/filters/wake_notifier_filter.py)
at `af9f9da9d7afddaa8811c0e0251b32566c980bdf`, blob
`f244dff438fa6d23f2f3338144ffc87020b00bf3`, specifies:

1. Match the configured frame types; nonmatches skip the asynchronous predicate.
2. Await the predicate. Only `true` invokes and awaits the notifier.
3. Forward the same frame in its original direction after successful completion.
4. Predicate/notification errors stop before forwarding. False and nonmatches
   still forward. There is no lifecycle or direction bypass in this class.

The Rust `FramePredicate` selector expresses matching explicitly. It is not an
automatic Python `isinstance` or complete inheritance mapping. The asynchronous
`AsyncFramePredicate` borrows the original frame without cloning it or handing
out mutable access to the envelope. Identity, metadata, local provenance and
native semantic carriers are forwarded intact. This adapter observes frames;
it does not serialize, reconstruct or transfer their authority to a notifier.
Opaque shared objects and application callback side effects retain their own
contracts; a borrowed envelope is not a claim that all captured state is immutable.

`WakeNotifierFilter::new(config, selector, filter, notify)` accepts an
`AsyncWakeNotification`. Each predicate and generic notification gets its own
cooperative deadline, default 10 seconds and configurable within `(0, 60]`
seconds. Callback construction/poll panics and selector panics become fixed
errors. The global panic hook may still log application panic text. A synchronous
selector or blocking callback cannot be preempted, and application callbacks must
not detach work. Panicking destructors are outside the containment guarantee.

Normal/system lane invocations may overlap. There is no global callback ordering,
new queue or background worker. Within one invocation, notification completion
precedes the original frame's push. Once notification succeeds, a later push
failure cannot roll it back and does not cause a retry.

## Generic callback boundary

The original source invocation is captured before the asynchronous predicate.
After a true result, the adapter checks that scope before starting a generic
notification. This is an observation, not a transaction with arbitrary user code.
A source interruption during an already-started notification cannot revoke its
external effects. Cancellation and timeout drop cooperative futures; they cannot
undo prior effects or terminate blocking code. Original forwarding continues to
use the existing core context and its ordinary system/uninterruptible survival
rules. A requested Cancel is not a promise to preempt a synchronous callback. Matched
Stop/Cancel frames follow the same predicate and notification sequence when their
original scopes are current; ordinary cancellation does not itself imply an
interruption epoch change.

## Fixed-session, dual-fenced gate adapter

`WakeNotifierFilter::for_gated_context` accepts one
`GatedLlmContextNotifier` from a particular gate run. Start the target Pipeline,
observe its Start, then obtain that fixed handle and start the source Pipeline.
The adapter never obtains `current_notifier()` again on each trigger, so an old
asynchronous invocation cannot silently bind to a reused gate's new session.

The gate remembers a weak raw notification scope only from actual Start and
Interruption processing. Its setup context has no invocation epoch and is not
used as a substitute. Before any asynchronous predicate, a matching frame
captures a ticket containing the intersection of the source and target's original
scopes. Each epoch is checked against its own runtime. Target Interruption updates
future tickets; it never refreshes a ticket already awaiting the predicate.

The ticket atomically sets the gate's coalescing `pending` bit only while both
original scopes are current and the fixed session is open. It invokes no user
callback in admission locks. All runtime and session locks are released before
waking the supervisor. A successfully captured ticket that later becomes stale
skips notification. Its source frame still
uses the original core forwarding policy. The two Pipelines have no total output
order: notification admission before source forwarding does not mean the target
context must reach its output before the source frame reaches its output.

Before the target has processed a real Start or Interruption, it has no typed
notification scope. Closed,
uninitialized or unrepresentable ticket capture errors are saved until a true
predicate actually requests notification. Capture failure remains an error on
that true path, even if the source subsequently expires; it is not treated as a
successfully captured stale ticket. False predicates still forward without
calling the notifier, as in the source. The original public `notify()` retains
its existing behavior.

Once admitted, a signal is not retrospectively erased by source interruption or
closure. It retains the existing gate's Event semantics: notifications coalesce,
and a signal consumed while empty creates no future ticket. A context arriving
before consumption may be selected. Do not infer one released context per true
predicate, attach one trigger's fence to the shared pending bit, or claim full
transactional rollback. The gate separately preserves and checks the original
cached context's authority and causal scope before releasing it.

## Slice 62 verification, 2026-10-14 UTC

Fifteen native tests cover explicit selector short-circuiting, borrowed identity
and native-carrier preservation, both directions, latest-context release through
two independently owned Pipelines, matched Start/Interruption/End/Stop/Cancel,
notification-before-forwarding, selector/predicate/notify failures and panic
containment, cooperative timeout, source/target interruption during the predicate,
fresh-trigger recovery, already-started generic side effects, empty-signal
consumption, fixed old handles across gate reuse, active owner-drop cleanup,
notification success followed by output backpressure without retry, and
already-admitted coalescing signals surviving later source revocation.

The missing-target-scope negative fixture uses a private wrapper that deliberately
forwards Start without invoking the inner gate's Start handler. It then invokes
the real gate on a real Interruption to establish a scope. It does not claim that
a normal direct Pipeline startup returns before its Start processing completes.
All observation/assertion failures run cancel-all then drain/join-all; cleanup
deadline failures remain test failures.

The unchanged fixed Python class AST was separately executed in a trace-only
FrameProcessor/notifier/frame stub environment. Twenty traces confirm both
directions, source subclass matching, false/nonmatch short-circuiting, errors,
and matched lifecycle ordering. This is not the full Python package or a claim
that Rust selectors automatically reproduce all Python subclass relationships.

Existing gate integration13 and private ownership4 tests pass. Processor default
and all-feature suites pass 184/227 tests, including the 15 new cases; the facade
export test and existing real loopback GatedContext HTTP/SSE regression each pass.
Strict processor all-target/all-feature Clippy, facade Clippy and selected
formatting pass. No live provider, network service credentials, browser or
hardware qualification is implied by these local fixtures.

Initial error fixtures incorrectly waited for recoverable ProcessorError to
terminate a pipeline; they now observe the error and explicitly close/join.
A blocked-Start negative fixture correctly hit core's startup timeout and was
replaced with the explicit private-wrapper injection above. Initial logs are
retained beside final passing runs. No production startup or error policy was
changed to satisfy these fixture assumptions. This slice also corrects the prior
subscription document: only the consumer-owned worker is serial; producer
callbacks can overlap across normal/system lanes.
