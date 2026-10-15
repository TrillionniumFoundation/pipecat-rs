# Explicit asynchronous tools

`GeminiClient::with_non_blocking_tools(Vec<String>)` selects declared function
names for an asynchronous lifecycle. Existing declaration and response struct
literals remain valid. Without calling the opt-in builder, setup and the existing blocking-tool
contract remain unchanged. An empty opt-in selection is rejected.

The opt-in supports the explicitly named ordinary Live models `gemini-3.8-live`
and `gemini-2.5-flash-native-audio-preview-12-2025`, optionally prefixed by
`models/`. Unknown models and transcription sessions are rejected before
connection. Thinking configuration and tools remain mutually exclusive. Model
availability and server acceptance require separate provider validation; local
WebSocket tests establish the native wire and ownership contract only.

Selected declarations carry `behavior: "NON_BLOCKING"`. On ordinary 3.8,
unselected declarations explicitly carry `BLOCKING`; this avoids silently
changing their local lifecycle when the model defaults to non-blocking tools.
The selection is replayed with setup on an authorized session resumption.

## Updates and scheduling

Use `Action::NonBlockingToolResponses` with
`NonBlockingFunctionResponse { id, name, response, will_continue, scheduling }`.
Responses must contain JSON objects and match an active selected call already
claimed through session event delivery. Guessing an ID before consuming its
call event does not authorize a result.
`willContinue` and `scheduling` are top-level function-response fields. Unlike
the pinned Python implementation, native code does not also insert scheduling
inside the user-provided response object; any user field with that name remains
untouched.
`will_continue: true` retains that call. A successful final wire write closes
it. Every batch is validated before any write or pending-state mutation.

`ToolResponseScheduling::Silent` updates the call without requesting another
model turn. In particular a silent final response does not create a new busy
state awaiting a nonexistent turn-complete event. `WhenIdle` asks the server to
schedule a response when appropriate; it does not replace the identity of an
already active local output turn. Scheduling is a protocol request, not a local
promise about model timing.

The frame adapter maps selected calls' `FunctionCallResult.is_final` (or detailed
properties) to the inverse `will_continue`, and `run_llm: Some(false)` to SILENT;
other values select WHEN_IDLE. It wraps scalar results as `{ "result": value }`
and errors as `{ "error": message }`. Unselected calls keep the previous final,
non-scheduled response contract. Context-updated callbacks remain unsupported.

## Call ownership and interruption

A local speech interruption invalidates audio output generation, but preserves
an explicitly selected pending call's original ID, name and origin. It does not
relabel the call as belonging to the newer user input. A provider cancellation
revokes the call, including permission to send further intermediate or final
results. The single session worker determines cancellation/result ordering.

Selected calls are delivered through `NonBlockingToolCalls`, independently of
the audio-generation filter. A bounded shared admission ledger linearizes
provider revocation and consumer claim without holding a lock across an await.
A mixed provider batch is split into consecutive groups with the same delivery
policy, preserving call order in the session event stream. Pipeline system and
normal lanes retain their existing scheduling semantics. Event capacity is checked before the batch is
registered. An unclaimed revoked call is filtered; its cancellation may still be delivered
without a preceding in-progress notification. Once claimed, the call and its
subsequent cancellation retain FIFO delivery in the session stream.
At a pipeline tool owner, system cancellation may overtake normal in-progress
markers. Owners must register bounded call authority from the preceding
`FunctionCallsStarted` system frame, remember cancellation for a declared but
not-yet-started call, and atomically check that authority before launching work.
Cancellation of a genuinely unknown/already-closed ID can be an idempotent no-op;
it must not erase a declared call's terminal state and permit it to launch later.

The frame mapper emits `FunctionCallsStarted` and
`FunctionCallInProgress { cancel_on_interruption: false }`, with non-interruptible
frame delivery. The corresponding provider cancellation is also a
non-interruptible control frame. It does not synthesize a model-response start.
Application tool owners must respect the per-call cancellation flag, process
provider `FunctionCallCancel`, bound and join their tasks, and mark results that
must survive speech interruption as non-interruptible. The standard `ToolExecutor` registers authority from `FunctionCallsStarted`,
checks cancellation before entering a callback, and cancels a task even when
cancellation raced installation of its task handle. Its policy comes from
`RegisteredTool.cancel_on_interruption`, not from the provider in-progress
marker. Configure that field to `false` for each selected non-blocking tool.
The client sidecar and executor registration are separate explicit settings;
this client cannot inspect an arbitrary downstream processor or automatically
validate their agreement. A mismatched interruptible registration may cancel
the callback while the provider call remains pending, requiring explicit
cleanup or reaching the configured finish deadline. A non-interruptible
registration for an unselected tool may continue local effects after the
provider-side call has been retired; stale results remain rejected. Neither
combination is a supported asynchronous-tool configuration.

The standard executor emits its own in-progress marker; observers may also see
the original provider marker. Markers must not be treated as independent
execution requests. Callback cancellation cannot undo effects already started.
This adapter does not execute tool code or provide an external side-effect
rollback guarantee.

An opt-in session retains bounded, non-reusable selected-call identities after final or
cancellation. The cumulative selected-call-ID limit is
`min(max_pending_tools.saturating_mul(32), 4096)`. Each selected call ID is at
most 1024 bytes, and cumulative ID bytes cannot exceed `max_message_bytes`.
IDs are stored as bounded boxed strings; the count bound also limits table
entries and their structural overhead. These limits do not claim an exact
process RSS ceiling.
Exhaustion fails closed instead of evicting identities and authorizing a reused
ID. This is a session-lifetime limit, separate from concurrently pending calls.

## Finish, cancellation and reconnection

Graceful finish stops new selected-call admission but permits updates for calls already
pending. It waits for final responses or provider cancellation within the
existing finish deadline. SILENT final completion requires no new model
turn-complete event. SILENT does not start a new model turn, but an existing
turn still requires provider termination or expiry of the original finish deadline.
Session cancellation prevents new call claims; worker exit
revokes all remaining call authority. Await `GeminiSession::shutdown` to collect
the worker. A cancellation request alone is not a join acknowledgment.

A successful session resumption preserves pending selected calls and their
identity ledger. Already written progress is not replayed. A failed write is
not retried as a complete action, and partial transport delivery cannot be
rolled back. This inherits the resumption policy's bounded attempts and explicit
uncertainty about the server's last acknowledged input.

## Source and validation boundaries

The pinned Python service associates asynchronous registration with
`cancel_on_interruption=False`, retains intermediate callbacks, and carries
`will_continue` and scheduling in function responses. Native selection is
explicit and model names are deliberately narrower. The source's extended
thinking scheduling path is not offered here because the documented model
contract differs. The native bounded ledger and call-authorized delivery are
local safety mechanisms, not fields claimed to exist in Python's runtime.

The applicable fixed-source replay and real WebSocket/pipeline verification are
recorded with this change's evidence. No local fixture establishes remote model
availability, external tool side-effect safety, or provider exactly-once
execution.
