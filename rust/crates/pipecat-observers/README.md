# pipecat-observers

Native Rust pipeline observation and measured metrics. There is no Python
runtime, background exporter, network connection, or implicit transcript logger.

## Register and consume

```rust
use pipecat_core::{Frame, Direction, IdentityProcessor, Pipeline, PipelineConfig,
                   ObserverOptions};
use pipecat_observers::{metrics_stream, MetricsObserver};
use std::sync::Arc;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let (publisher, mut metrics) = metrics_stream(256)?;
let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
let observer = Arc::new(MetricsObserver::new(publisher.clone()));
let health = pipeline.add_observer(observer, ObserverOptions::default());
let worker = pipeline.start(PipelineConfig::default()).await?;
let sender = worker.sender();
let producer = tokio::spawn(async move {
    sender.send(Frame::text("hello"), Direction::Downstream).await?;
    sender.end().await
});
let (_frames, report) = worker.collect().await?;
producer.await??;
assert!(report.errors.is_empty());
metrics.close(); // Drain accepted records even while a publisher is retained.
while let Some(record) = metrics.recv().await {
    println!("{}", serde_json::to_string(&record)?);
}
assert_eq!(health.stats().dropped, 0);
# Ok(())
# }
```

Consume metrics concurrently in a long-running application. The short example
fits its startup report into the bounded buffer before consuming it.

`MetricsObserver` must observe every push, because startup timing needs the
Start frame's arrival and departure at every processor. Service and turn
measurements independently filter to the first push of a logical frame.
Allocate one observer instance per root pipeline. Native nested pipelines use
`Pipeline::inherit_observers(&parent_context)`: they share the root's dispatcher,
clock, and first-push group, but retain distinct runtime/processor identities.
Only the root owns dispatcher shutdown.

## Hook and dispatch contracts

The observer API lives in `pipecat-core`: implement `PipelineObserver::on_event`
and register it with `Pipeline::add_observer`, which returns an `ObserverHandle`.
`with_observer` is the fluent alternative. Registration is explicit and happens
before startup; dynamic registration/removal is not implemented.

The runtime records timestamps before asynchronous dispatch:

- `RuntimeStarted`: before processor setup
- `ProcessorSetup`: actual entry/exit of `setup`, including failed and abandoned
  setup futures, with an explicit outcome
- `Process`: entry into `process`, after terminal preparation/task drain; this
  is not a process-completion event
- `Push`: a successful push at an adjacent queue or runtime boundary, including
  terminal staging, flush reflection and worker requests. Rejected/stale pushes
  are excluded. An already-seen interruption can be accepted idempotently by its
  destination without being processed again
- `PipelineStarted`: Start reached that runtime's downstream boundary; it does
  not wait for arbitrary work a processor performs after forwarding Start

Each registration has its own bounded queue and owned task. Runtime hooks use
`try_send`; a slow observer loses events without holding up frame processing.
Queues preserve each registration's dispatch order. Observations from independent
branches carry their original timestamps and can interleave. Callbacks must yield
instead of blocking a Tokio thread. A callback panic terminates only that
observer and is counted; it does not fail the pipeline.

`ObserverHandle::stats` reports completed callbacks, dropped enqueues, accepted
but abandoned events, and callback failures. Each delivered event also carries
a sequence and cumulative enqueue-loss count. The built-in reducer discards
incomplete timing state after a loss instead of reporting invented intervals.
A loss at shutdown with no later event is still visible in the handle.

Graceful End/Stop drains observers up to their configured drain deadlines, then
aborts and joins remaining callbacks. Cancel/failure/startup cancellation aborts
and joins them without waiting for callback progress. Dropping the hub also
aborts owned dispatch. No observer task is detached with an unbounded queue.
The metric output stream has a separate bounded channel and independent dropped
record counters; a closed/slow consumer never backpressures the runtime.

## Privacy and resource bounds

PipelineObservation is opt-in. The allowlist contains frame ID/type, direction,
interruptibility, processor identity, timing, VAD stop duration, and at most 64
numeric service metrics per frame. Excess metrics are counted. Text, audio,
images, tool arguments/results, arbitrary metadata, transport identifiers,
exceptions and error messages are never retained. Processor/model names are
application-provided labels; do not put secrets into those labels.

Every frame has one small shared identity allocation. Its observation set is
allocated only when observed; ordinary pipelines do not read observer event timestamps,
capture summaries, or run dispatcher tasks. `Clone` preserves that identity even
when cloned before admission. `duplicate()` and deserialization create a fresh
identity. Nested runtimes share first-push identity; unrelated roots each observe
one first push. The set grows with the distinct observer groups a retained frame
visits and is released with its final clone, rather than becoming a session-wide
frame-ID history.

Queue capacities count events, not bytes in application labels. Startup state
retains at most 64 runtimes and 4096 processors per runtime by default, with
explicit evictions/incomplete reports. The user-bot reducer retains a bounded
set of conversation milestones. Audio onset scanning keeps about 10 ms of mono
samples plus an incomplete interleaved frame, and stops after three seconds of
input. It does not retain or expose audio content in emitted metrics.

## What is measured

`StartupTiming` reports each processor's setup span and Start entry-to-push
interval, with setup/start phase durations. Overlapping setup work contributes
its actual span, never a sum. Missing/backwards/aborted intervals are marked
incomplete, with absent fields rather than artificial zeroes. Reports preserve
pipeline order and runtime identity. `TransportTimingReport` measures the first
bot/client connection offsets from runtime startup, including connections made
during setup. Timestamps are monotonic nanoseconds, not Unix timestamps.

`UserBotLatency` measures VAD-confirmed silence to first bot speech, subtracting
the VAD's stop duration from its first-push time. Because frame payload wall
clocks may have another epoch, they are not mixed with this clock. A producer
that delays its own VAD push includes that delay in this observation. A greeting
starts when both the root pipeline and client are ready. New user speech,
interruption, cancellation, and observed queue loss discard stale windows.
Repeated frame hops and upstream/broadcast copies do not produce extra reports.
Contributions tile the interval using observed VAD, transcript, request, text,
audio and speech boundaries. `llm_until_text` and `text_to_audio` name observed
intervals; they do not claim to isolate inference, thinking, sentence buffering,
or synthesis when those boundaries were not measured.

Existing native `MetricsFrame` numeric values are streamed once as
`ReportedService`. These preserve what the service measured; there is no synthetic
TTFB derived from a generic text frame. Invalid negative/nonfinite duration
values are excluded. Rich measured records are available through `ServiceMetrics`:

- TTFB: begin request to first actual provider output; headers, keepalives,
  stream-open and usage-only acknowledgements do not stop it
- TTFAT: same request start to first answer token or tool-call start; reasoning
  can end TTFB without ending TTFAT
- TTFA: measured TTFB plus leading silence, confirmed by sustained audible PCM
  onset. The source's centered 10 ms RMS, 1 ms hop, -40 dBFS gate, 50 ms voiced
  run and DC rejection are retained. Real lookahead is required instead of
  padding a chunk's tail, making the answer independent of chunk boundaries
- Explicit processing and text-aggregation start/stop timers
- Incremental STT submitted-audio seconds, Unicode TTS character counts, and
  provider LLM token usage with cache/reasoning/audio buckets and model identity

Provider gross/net cache accounting is preserved. Do not recompute total tokens
as prompt + completion or count TTFA/TTFAT's repeated TTFB as another sample.
No prices or costs are inferred. Initial-only TTFB policy is explicit, including
its cancellation behavior. Never-started/cancelled/incomplete operations do not
emit placeholder timings.

Use `ProcessorContext::observer_clock()` inside a service to share the pipeline's
clock, or inject the same `Arc<dyn ObserverClock>` into the pipeline and timer:

```rust
use pipecat_core::{MonotonicObserverClock, ObserverClock};
use pipecat_observers::{ServiceMetrics, ServiceOutput};
use std::sync::Arc;
let clock: Arc<dyn ObserverClock> = Arc::new(MonotonicObserverClock::default());
let mut timer = ServiceMetrics::new("llm", Some("model".into()), clock);
timer.begin_request();
// At actual provider callbacks:
assert!(timer.observe_output(ServiceOutput::Acknowledgement).is_empty());
let records = timer.observe_output(ServiceOutput::AnswerToken);
assert_eq!(records.len(), 2); // TTFB and TTFAT from measured boundaries
// Publish each record immediately with MetricsPublisher::service(timestamp, record).
// Call timer.cancel_request() on interruption/failure, or finish_request() on completion.
```

The timer is per non-overlapping service stream. Concurrent provider requests
need separate timers. Methods ending `_at` accept explicit timestamps in the
same monotonic domain for precise provider callbacks and deterministic tests.
Using the timer does not automatically modify every service adapter.

## Source scope and remaining adapters

The implementation is based on `observers/base_observer.py`,
`pipeline/worker_observer.py`, startup/user-bot/service metrics observers,
`processors/metrics/frame_processor_metrics.py`, `metrics/metrics.py`, and the
source TTFB/TTFA/TTFAT/startup/latency tests. It replaces the source's unbounded
observer queues with explicit bounded loss and owned cancellation.

RTVI's explicit taps remain separate and unchanged. OpenTelemetry, Sentry,
external collectors, raw-content loggers, source-specific callback aliases,
detailed marker/tool-call latency
attribution and dynamic observer registration are separate
adapters or remaining work. This crate does not claim complete Python observer
API parity.

## Conversation turn tracking

Register one fresh `Arc<TurnTrackingObserver>` per root pipeline with `register`.
It observes first pushes, starts turn one at Start, preserves repeated user runs
before bot speech, and rolls over at a later user start after bot speech. An active
bot marks that rollover interrupted. Bot stop arms a 2500 ms default deadline;
bot resume cancels it. Timeout duration uses the original stop timestamp.
End, Cancel and native Stop close an active turn and the stream. Cleanup without a
terminal does not invent a turn end. Bare old bot-stop frames arriving after a new
bot start cannot be correlated to their original speech and remain an explicit
source limitation.

The core observer dispatcher serializes deadlines with observations and owns
cancellation. It checks ready observations first; continuously ready queued traffic
can postpone expiration. Live queue-loss health invalidates a pending turn even
when no later event delivers its dropped count. Sequence gaps and backwards relevant
timestamps invalidate durations; subsequent numbers count observed starts only.

`Started` and `Ended` carry an optional, content-free native `boundary` token.
Timeout ends retain the triggering bot-stop token. Tokens share private logical
frame identity across native forwarding, remain distinct from replayable public
IDs, and are omitted by serde. Restored/synthetic events without a native token
cannot prove a recorder boundary.

The media transport's opt-in `turn-recording` feature supplies an owned bounded
adapter via `AudioBufferProcessor::attach_turn_tracker`. It joins these tokens to
sealed resampled PCM prefixes. Its separate `subscribe_recording()` stream serializes
every native first-push recording control before the resulting turn events;
the adapter verifies source and local control order before attribution. This stream
is independently bounded and does not change ordinary `subscribe()` events.
The adapter fails closed on loss or ambiguity. Primary recording continues and a
status/event explains disabled attribution. Manual `emit_turn_audio(number)` remains
available separately; forwarding asynchronous Ended events to it is unsafe because
current buffers can already contain later-turn audio.

## Error, function-call and speaking events

`ErrorObserver`, `FunctionCallObserver` and `SpeakingObserver` each expose a
bounded local broadcast stream of `SourceEvent<Record>`. Construct an `Arc`,
subscribe before startup, and install it with `register(&mut pipeline, options)`.
Register a fresh instance once per root pipeline. No observer starts an exporter,
opens a connection, writes logs, or retains original frames.

Default records contain no message text, exception objects, function names,
tool-call/group IDs, arguments, results, arbitrary metadata, media or provider
signatures. Error categories/health, function phases/blocking and speech layers
are structural observations. To receive content, configure it explicitly on that
observer before registration:

```rust
use pipecat_observers::{FunctionCallObserver, FunctionCallObserverConfig,
    FunctionCaptureOptions};
let observer = FunctionCallObserver::with_config(FunctionCallObserverConfig {
    capture: FunctionCaptureOptions {
        identity: true, // function name, exact tool-call ID and group ID
        arguments: true,
        results: false,
        errors: false,
    },
    ..Default::default()
})?;
# Ok::<(), &'static str>(())
```

`ErrorObserverConfig::capture_message` independently enables error text.
`Captured<T>` distinguishes `NotRequested`, `Present` and `Omitted(reason)`.
Exception type names are not available in the native error contract and are not
invented or extracted by invoking arbitrary object code. Application-configured
processor names remain descriptive labels, as in ordinary observations; never put
secrets into them.

Core `PipelineObserver::capture_policy` opts a registration into a separate
`on_captured_event` callback. Its payload is projected separately for that
registration. The shared `Arc<PipelineObservation>` and ordinary `on_event`
callbacks never contain sensitive capture, even in a hub with other opted-in
observers. Policies are sampled once when the hub starts. Capture covers ordinary
and bounded stream admission paths, but stream admission currently permits none
of the source error/function/speaking frame kinds. Failed admission emits no event.

### Bounded capture and attribution

`CaptureLimits` defaults to 4096 bytes per field, 16384 aggregate bytes, 64 calls,
1024 JSON nodes and depth 16 per pushed frame and registration. Validated hard
maxima are respectively 65536, 262144, 256, 8192 and 32. UTF-8 string sizes,
container nodes and fixed scalar costs are checked before cloning application
content; the byte budget is a content-cost bound, not JSON wire size. Container
allocation overhead is separately bounded by node/call counts. Aggregate budgets
include captured identities, arguments, results and errors together. No raw
exception, context, signature or arbitrary metadata is copied. Oversized fields
are omitted whole; IDs are never prefix-truncated into colliding correlation keys.

Function correlation retains at most 256 pending/running exact IDs and 65536 ID
bytes by default, configurable up to 4096 and 4194304. Capacity invalidation clears
old attribution before accepting fresh state. Truncated identity/batches clear
attribution and cannot establish new timing from any member of that batch.
Argument/result omissions remain explicit field omissions without changing a
known identity's lifecycle. Without identity capture, no IDs are retained and
`started_ns`/`in_progress_ns` remain absent. A call ID must identify one invocation
within a conversation, as in the source contract; reused IDs have no independent
invocation generation.

Input queue loss or a sequence gap clears incomplete call/speech state. Live
registration health also detects final loss at cancellation, even if no later
observation delivers its loss count. Events queued before a known loss cannot
reestablish attribution. Backwards observed timestamps and invalid VAD offsets
invalidate intervals instead of creating negative or artificial zero durations.
Output subscribers receive independent `RecvError::Lagged`; they must discard
incomplete histories after lag. `stats()` distinguishes events without any
subscribers from input drops. Accepted downstream End/Cancel/Stop or owned
cancellation closes the stream and clears state without inventing a call outcome
or a speech stop. Upstream terminals and rejected sequence replays do not close it.

### Source mappings and deliberate differences

- `observers/error_observer.py` and `tests/test_error_observer.py`: one error at
  first admitted push, including failures consumed by upstream recovery. Native
  `push_error` origin and its report-time health verdict take precedence over
  descriptive labels or later health restoration. Manually assembled errors
  lacking a native origin are attributed to their pusher
- `observers/function_call_observer.py` and `tests/test_function_call_observer.py`:
  one `Started` moment per requested call (pending until `InProgress`), independent
  wait/run timestamps, blocking/group metadata, final-only settlement, failure,
  cancellation and timeout distinction. `run_llm` on cancellation identifies the
  source timeout convention. Both compact and detailed native frames are covered;
  external-function frames are a separate contract. Unlike Python's defaults,
  arguments and identifiers are opt-in
- `observers/speaking_observer.py` and `tests/test_speaking_observer.py`: independent
  acoustic user speech, semantic user turns and bot speech; stops name their own
  known start, repeated starts replace only that layer, and interruption is a
  separate moment. Proposed turn frames are not speaking boundaries. Broadcast
  siblings use downstream only, while ordinary upstream pushes remain eligible

All three deduplicate native forwarding through `first_push`, without keeping a
session-wide ID history. Timestamps are monotonic first-push nanoseconds, not the
Python observers' Unix clock. Acoustic speech subtracts the VAD confirmation delay
from admission time, never mixes payload wall-clock epochs, and consequently
includes producer delay before the push. That distinction matters when comparing
these records with external recordings.

Native evidence lives in core `tests/observer_capture.rs` and observers
`tests/source_events.rs` and `tests/review_terminal_admission.rs`, alongside the
existing dispatcher lifecycle and lineage regressions. These are source-behavior
mappings, not a claim of Python callback or
wire-schema compatibility. OpenTelemetry/Sentry exporters, raw-content loggers,
external-function adapters and dynamic registration remain outside this slice.
