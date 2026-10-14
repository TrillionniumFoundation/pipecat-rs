# Bounded native Producer/Consumer subscriptions

The default facade exports `ProducerProcessor`, `ConsumerProcessor`,
`FrameSubscriptionConfig`, borrowed `AsyncFramePredicate`, owned
`FrameTransformer`, a registration handle and content-free diagnostic snapshots.
These connect independently owned native Pipelines. They are not a transport,
general broker, shared mutable frame channel, or playback acknowledgement.

## Fixed Python contract

Reference: `ProfHepta/pipecat` at `af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- [ProducerProcessor](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/processors/producer_processor.py), blob `c70c066f1ab2f643421e7fea2976b048a7265213`.
- [ConsumerProcessor](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/processors/consumer_processor.py), blob `9e68bd682cdf18e7ad27eb986f51c78b12843728`.
- `tests/test_producer_consumer.py`, blob `0fd3f862c9cef40292c6d05e9c50ba25fdffc52e`, includes six scenarios and Text-to-InputAudio transforms on either side.

Python filters each frame, transforms once per registered consumer in order,
queues just the frame, and optionally forwards the original in its original
direction. Nonmatches always pass, even with passthrough disabled. A matching
frame with no subscribers is swallowed when passthrough is disabled, without
calling the transformer. Consumer-local input passes unchanged; subscribed input
is transformed and queued to the consumer **itself**, in its configured direction.
A later fanout failure does not roll back an already queued prefix.

Python gives callbacks the same mutable object and iterates a live subscriber
list. Its queues are unbounded, do not unregister on cleanup, and consumer task
failure can leave a registered queue without a reader. Native behavior below is
an explicit bounded safety profile, not exact whole-class behavioral parity.

## Native data and callback boundary

Start the producer and observe its Start before starting consumers. Registration
happens on consumer Start, not construction. Each object is single-use; construct
fresh pairs for another run. Concurrent reuse is unsupported. Repeated direct
Start processing is idempotent, without widening the core's ingress rules.

Start, End, Stop, Cancel (including detailed forms), and Interruption bypass
callbacks and remain local. Other unmatched frames retain their native identity
and direction. Matched frames must be one of 14 text variants or five PCM
variants: Text/LlmText/VisionText/InputText/AggregatedText/TtsText, the four
transcription/translation variants and their detailed forms; InputAudio,
OutputAudio, TtsAudio, SpeechOutputAudio, or UserAudio. PCM geometry is validated.
Both transformer outputs are independently revalidated. Native semantic
authority, error-origin capabilities and pipeline-clock PTS cannot cross this
boundary, even when attached to an otherwise allowed payload. Ordinary explicit
PTS and local provenance are retained. Control, settings, tool and context frames
are not exported.

The predicate borrows a read-only frame. Before awaiting it, the producer captures
the source invocation, ordered membership and each destination invocation. Each
producer transformer receives its own owned clone of the original; one
subscriber's edits cannot affect another or original passthrough. Identity
transforms retain logical frame ID and native observation identity. Rust's typed
transformer returns a frame or an error; `None` is not a drop protocol.

Callbacks run outside runtime and local state locks, serially per consumer.
Timeouts and cancellation are cooperative. Synchronous blocking, detached work,
panicking destructors and side effects inside callbacks cannot be preempted or
rolled back. Construction/poll panics produce fixed errors; the global panic
hook may still log the application's panic payload.

## Two runtimes, two causal scopes

`ProcessorContext::try_queue_with_invocation_fence` is a restricted snapshot
self-queue entry point. It retains the original destination context and source
fence, locks their distinct runtime admission gates in a stable deduplicated
order, and compares every captured epoch with its **own** runtime. It never
compares A's epoch number with B's. Closed destination and full destination queue
are errors; stale or unavailable source ownership and obsolete scopes report
`Stale`. Existing `try_push_with_invocation_fence` behavior remains unchanged.

Local consumer Interruption refreshes the scope for future snapshots. It does
not refresh queued or transforming envelopes. Source or destination interruption
during either asynchronous transformer rejects old admission, including system
and uninterruptible data. A fresh frame can proceed once the local interruption
has been processed. This guard does not undo callback side effects.

Once the target has accepted the frame, its queue is owned by the target runtime.
Later source cancellation cannot retract that frame or effects already performed.
`buffer_admitted` means only bridge admission; `target_queued` means only target
queue admission, not target processing, network delivery, or external success.

## Bounds, errors and ownership

Defaults: at most 16 consumers, 32 pending frames per consumer, 8 MiB pending
capacity, 1 MiB per-frame capacity, JSON depth 32 and 8192 JSON nodes, and a
10-second callback deadline. Configuration has explicit positive upper limits.
Pending counts cover buffer-admitted frames and the consumer worker's in-flight
transform until target admission/drop. Producer callback clones/results before
buffer admission are callback temporary objects, excluded from that pending
budget; their returned frames still undergo per-frame validation. Bytes charge owned String/Vec
capacity rather than length, bounded JSON storage, fixed ring slots and
conservative envelope/scope/local-tag reservations. The persistent registration
context and its name are charged to the fixed base; each queued destination
context name is charged separately. Context refresh rechecks that budget. Empty
JSON maps reserve a possible retained root node. Callback temporary
allocations, allocator overhead, shared observation-identity bookkeeping and
downstream storage are outside this retained-content budget; it is not a total
process heap bound.

Full buffer, oversize payload and transformer failure are explicit errors, not
silent drops or unbounded waits. Already admitted fanout prefixes remain;
subsequent subscribers and original passthrough are skipped for that failed
invocation. A consumer worker failure closes and unregisters that subscription,
records a content-free failure count, and reports through the core's tracked
worker error path. Other subscriptions remain independent. Snapshots count stale
rejections and discarded queued work separately.

End/Stop/Cancel, cleanup and owner loss close the corresponding subscriptions.
Producer closure retires its subscriptions without injecting terminal frames into
consumer pipelines. Consumer closure unregisters only itself. Closure discards
queued bridge work and cancels cooperative callbacks; it does not drain old work
as new-session data. A callback/final admission already selected concurrently
with closure may finish. Joined close returns only after that worker has exited;
it does not retract frames already admitted to the target. `prepare_end` closes
and joins before core background drain. Spawn rejection and cancellation before
first polling also release the captured worker ownership. No terminal sentinel
needs a spare data-buffer slot.

The retained JSON accounting supports the workspace's locked serde_json 1.0.151
BTreeMap and inline-number representation. Construction performs a fail-closed
representation probe; externally unified `preserve_order` (hidden map capacity)
and `arbitrary_precision` (heap-backed numbers) profiles are unsupported. This is
a guard for that reviewed version, not an automatic proof for future serde_json
implementations. Dependency upgrades require reviewing the accounting and probe.
Target queue backpressure follows existing core policy and can terminate the
target pipeline; even an earlier `Queued` receipt does not guarantee processing
if that target itself later fails or closes.

## Slice 61 verification, 2026-10-14 UTC

Ten core tests exercise self-processing, both directions, unequal independent
runtime epochs, stale raw scopes, dead weak sources, closed/full targets,
post-admission cancellation, forbidden authority/clock/control data and opposite
lock orders. All 331 core tests pass. Nineteen subscription integration tests use
real independent Pipeline owners, including the six source shapes, all 19 allowed
payloads, both Text-to-PCM transform sides, identity and owned-mutation isolation,
registration-order partial fanout, snapshot-before-await membership, source and
destination interruption during both transforms, new-message recovery, capacity
including in-flight work, native authority/error-origin rejection, callback
failures, lifecycle and single-use behavior, task-admission rejection, unpolled
abort, and owner loss while waiting or inside a callback. Two private tests cover
JSON representation and retained empty-map roots. Both owners are cancelled,
drained and joined on observation failure; cleanup deadline failures fail tests.

Processor default/all-feature suites pass 169/212 tests; the affected Realtime
suite passes 82. The default facade export test and existing real loopback
GatedContext-to-HTTP/SSE test each pass. An isolated consumer fixture with its own
lockfile exercises actual serde_json feature unification: default accepted,
`preserve_order` rejected, and `arbitrary_precision` rejected. Core and processor
all-target strict Clippy, facade strict Clippy and selected formatting pass.
These are native synthetic tests, not live provider, browser or audio hardware
qualification.

Initial additional fixtures wrongly required a successful worker return after
startup rejection and required target processing after target-side backpressure
had already failed that pipeline. Those assumptions were corrected to the
existing core contract. One test-only late-initialization Clippy warning was
corrected. Initial diagnostics are retained separately from final passing runs.
