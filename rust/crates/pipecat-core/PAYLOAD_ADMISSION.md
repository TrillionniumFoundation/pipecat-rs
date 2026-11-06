# Pipeline payload admission

`Pipeline::with_payload_limits(PayloadLimits)` enables strict admission on the
actual runtime queues and output paths. Existing `PipelineConfig` literals remain
valid. The compatibility default leaves this profile disabled, because several
native LLM/tool continuations intentionally retain opaque local capabilities.
The Hepta local speech input (`pipecat-hepta/src/local_speech.rs`) selects the
default strict profile for CPAL → Silero VAD → Whisper. Its 20-ms input PCM,
VAD/control-parameter DTOs, metrics and transcription frames are serializable;
it attaches no opaque LLM/tool capability. Oversized transcription results fail
admission explicitly. Other production paths must select the profile before
claiming this payload bound.

```rust,ignore
let worker = Pipeline::new(processors)
    .with_payload_limits(PayloadLimits {
        max_frame_bytes: 1024 * 1024,
        normal_queue_bytes: 8 * 1024 * 1024,
        system_queue_bytes: 1024 * 1024,
    })
    .start(PipelineConfig::default())
    .await?;
```

## What the budget measures

The charged unit is the compact JSON size of the frame name, public metadata,
transport source/destination and `FrameData`, counted with a capped writer.
Measurement does not allocate a serialized copy. It covers text, byte-vector
media, sprites, files, contexts and the other serializable variants through the
same serializer. JSON byte arrays can be larger than their raw media bytes.
The counting traversal rejects nesting beyond 128 and checks arithmetic overflow.

This is **not an RSS or total allocation bound**. A String or Vec may retain spare
capacity. Processor state, current processing calls, copies, observer/error
histories, native libraries, caller-owned pending sends, caller allocations before
admission, and an output consumer retaining frames are outside queue ownership.
The profile does not make timeout preempt native code, retract device effects, or
introduce a new model/tool owner.

Frames retaining native continuation, speculation, eager-source, realtime-context,
tool-outcome/batch/progress or user-image capabilities are rejected. Serializable
payload fields containing local callbacks, processors, events or opaque objects
also fail closed. Opaque state is never silently assigned a zero charge. Local
clock stamps and fixed causal bookkeeping are not JSON payload bytes.

## Capacity, priority and release

Each processor has the configured normal/system byte budgets and existing item
limits. Start has an independent one-frame reservation. Cancellation,
interruption and urgent resume have another one-frame reservation, limited by
`max_frame_bytes`, and take precedence over the ordinary system queue. This is
necessary because input audio, image and transport payloads also have system
priority. Media cannot consume the reserved control item or bytes. A pending
control can still contend with another control; executing blocking native work
is not preempted. The compatibility profile retains the original FIFO priorities.

Thus the sum of charged queued bytes per strict processor cannot exceed
`normal_queue_bytes + system_queue_bytes + 2 * max_frame_bytes` (mathematical sum,
not an RSS estimate). The worker output remains item-bounded; every output frame
must also fit `max_frame_bytes`. A staged graceful terminal occupies at most one
additional admitted frame outside the output channel.

A queued frame owns a byte reservation. Dequeue, interruption reset, discard and
queue destruction release it. A consumer retaining the dequeued frame does not
retain a queue reservation. `FrameQueue::queued_payload_bytes()` reports the four
current charges in `(start, reserved_control, system, normal)` order. Closing a
queue wakes senders but preserves accepted items for the existing drain contract.
Lifecycle-inherited child pipelines inherit the same strict limits and cannot
replace them with a different profile.

## Distinct admission results

| Route | Item/byte capacity full | Oversize or unsupported payload |
| --- | --- | --- |
| External `PipelineSender::send().await`, `FrameQueue::send().await` | Wait for capacity; no admission lock is held across the wait | Reject immediately; never wait for impossible capacity |
| Ordinary `ctx.push`, `ctx.queue`, child `try_send_from` | Return an explicit failure without waiting inside processor routing | Reject immediately |
| Explicit owned-background `push_downstream_wait` | Wait under the existing causal/staleness contract | Reject before waiting, including when the output is full |

`QueueError::Full` represents temporary item or byte pressure.
`PayloadTooLarge` and `UnmeasurablePayload` are terminal admission failures;
external pipeline calls expose them as `PipelineError::InvalidPayload`. Internal
processor routing returns the existing fatal `ProcessorError` contract. Invalid
external cancellation is rejected before it can close input. Dropping a waiting
send admits no frame and consumes no reservation.

## Executable evidence

`tests/payload_budgets.rs` exercises exact-size accounting and metadata, audio
limits, opaque callbacks and hidden native capabilities, JSON nesting, item vs
byte pressure, pending-send drop, close/drain, interruption release, reserved
control priority with system media full, external wait vs internal failure,
cancellation, generated output rejection, owned output waits and child inheritance.
These tests prove admission behavior; they do not measure a live process's RSS or
qualify user-perceived media quality.
