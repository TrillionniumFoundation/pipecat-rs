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
