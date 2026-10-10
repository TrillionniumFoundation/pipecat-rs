# Native agent workers

Executable native LLM, backend, and UI workers built on `pipecat-services`,
`pipecat-core`, and `pipecat-workers`. No Python interpreter or callback bridge is
used. Model calls use the native HTTP/SSE client; tools are registered Rust async
functions. An application supplies provider configuration and tool implementations.

## Components

- `LlmWorker` owns an independent conversation, native streaming inference,
  concurrent tool batches, and a bounded output pipeline. `append` admits steering
  while a model/tool round is running; the next round sees the new request and
  completed tool results together. `replace` refuses to reset an in-flight turn.
- `ToolRegistry` joins explicit schemas to executable handlers. `typed` decodes
  arguments with Serde. Tool options carry timeout and interruption behavior.
  Intermediate results are real non-final function-result frames. Tool-owned
  deferred frames wait for every call in their batch, while unrelated externally
  queued frames continue through the pipeline. Deferred message updates return
  to the worker's owned context and can trigger another inference.
- `BackendLlmWorker` exposes `attach` and `message` jobs. One long-lived attachment
  receives output, thoughts, tool phases, idle, and error updates. Message jobs
  acknowledge admission with the previous `idle`/`working` state. Detach cancels
  all work, including interruption-exempt tools. An attachment generation prevents
  buffered output or a blocked transform from leaking into a later session.
- `BackendSession` performs the attachment handshake and streams typed events.
  `split_spoken` preserves note text before the first line beginning with `>>` and
  strips subsequent spoken markers. Transcript delegation flattens frontend text
  into one request; it never adopts frontend assistant messages as backend history.
- `UiWorker` exposes sequential `respond` jobs and concurrent `screen` jobs.
  Respond jobs inject the user query and accessibility snapshot, and by default
  reset mutable history. Durable system instructions survive resets. A reply
  with tool calls remains a preamble. A final reply returns `{"answer":...}`;
  an explicitly requested verbatim speech message is separate from the null job
  acknowledgement. Optional `reply` tool answers can be silent.
- UI snapshots and named events arrive as typed core RTVI frames on the native
  worker bus. Event context is injected before asynchronous handler dispatch.
  Snapshot events only update stored state. Screen helpers support selection,
  named element listing, classification, and commands. The classifier performs
  actual isolated LLM requests, validates returned references/probabilities,
  and never exposes the full page through the voice-facing `screen_tool` result.

## Ownership and limits

Read `LlmWorker::recv` concurrently with long-running work, or call `join` after
requesting `end`/`cancel`; output has bounded backpressure. Dropping a worker stops
its model/tools and pipeline. Hosted backend/UI worker cancellation also releases
bus jobs and event tasks. Cloned handles do not replace the owner.

Normal interruption cancels the current model and opted-in tools. Opted-out calls
finish and settle their context, but old response text is suppressed. Full
cancellation cancels all calls and bypasses blocked normal output. UI job fences
prevent a cancelled or completed job from answering a later one. Resource limits
cover queued commands, context bytes, model rounds, tools per round, deferred
frames, UI snapshot bytes/depth/nodes, and concurrent event handlers. Exceeding
the event-handler limit is an explicit host error rather than a self-publication
deadlock. Tool errors/timeouts settle as model-visible results; transport/protocol
failures, panics, and aggregate limits fail the worker with a recorded error.
Full cancellation remains effective if a normal interruption follows before the
driver runs. Buffered tool updates/results survive an ordinary interruption only
for opted-out calls; full cancellation invalidates them too. Invocation cancellation
notifications settle on every exit, including an abort caused by a peer-tool panic.

The worker driver emits through an owned native output pipeline; it does not
subclass Python's pipeline-worker hierarchy. Additional output processors can be
supplied to `start_with_output`. Tools execute in tracked per-round tasks, and
must yield cooperatively. Application-created detached tasks are not owned by the
worker. Schemas are advertised to the model; use typed handlers or explicit
validation for application-specific argument constraints.

## Compatibility boundaries

This is a native typed API and wire schema, not a drop-in Python object/decorator
or pickle protocol. Explicit Rust registration replaces MRO discovery. Not yet
implemented here:

- Generic worker activation/handover callbacks and dynamic tool replacement
- Independent background-tool continuation while a newer model run executes;
  steering waits for the current tool batch to settle
- Full context aggregator/audio/VAD integration, summarization, multimodal input
  aggregation, output-transform hooks for tool frames, and provider role objections
- Model-driven `cancel_<tool>` tools, provider-specific thought/signature history,
  and complete metrics accounting
- UI job-group cards/cancellation lifecycle, Jev classifier/calibrated probability
  parity, or the deprecated bundled reply tool's visual action arguments
- Python job/wire interoperability or production/live-provider qualification

The optional native reply tool handles answer data only; ordinary UI commands
remain explicit methods. Job completion waits for the model/tool round to settle,
so a tool cannot reset another job's context while its own batch is still active.
Deferred-frame ordering guarantees pipeline admission order, not an arbitrary
output device's playback receipt. A custom async output processor must supply its
own playback/drain contract. No model-token or paid-service claims are inferred
from simulated-provider tests.

## Verification

`cargo test -p pipecat-agent-workers --offline` runs deterministic loopback HTTP
providers and real native pipelines/buses. Coverage includes independent history,
streamed tool updates, deferred-vs-external frames, selective/full cancellation,
backpressure, tool timeouts, steering, backend admission/attachment/refusal,
detach/re-attach fences, UI single-flight/reset/silent and verbatim acknowledgements,
event-before-handler ordering/capacity failure, and malformed classifier output.
Independent contract tests also cover coalesced cancellation, buffered tool
progress and completed results, peer-panic cleanup, malformed typed arguments,
duplicate model call IDs, and attachment timeouts without an acknowledgement.
