//! Admission tests exercise queue ownership and real pipeline entry/exit paths.
use pipecat_core::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

const DEADLINE: Duration = Duration::from_secs(3);
fn limits(bytes: usize) -> PayloadLimits {
    PayloadLimits {
        max_frame_bytes: bytes,
        normal_queue_bytes: bytes,
        system_queue_bytes: bytes,
    }
}
fn item(frame: Frame) -> DirectedFrame {
    DirectedFrame::new(frame, Direction::Downstream)
}
fn sample() -> Frame {
    let mut frame = Frame::text("x".repeat(1200));
    frame.name = "payload".into();
    frame
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(DEADLINE, worker.recv())
        .await
        .unwrap()
        .expect("output closed")
}

#[test]
fn exact_limit_includes_routing_metadata_and_rejects_overflow() {
    let mut frame = sample();
    frame
        .metadata
        .insert("nested".into(), serde_json::json!({"caption":"中文"}));
    frame.transport_destination = Some("speaker".into());
    let size = PayloadLimits::default().measure(&frame).unwrap();
    assert_eq!(limits(size).measure(&frame), Ok(size));
    assert_eq!(
        limits(size - 1).measure(&frame),
        Err(QueueError::PayloadTooLarge)
    );
    let queue = FrameQueue::with_payload_limits(16, 4, limits(size)).unwrap();
    queue.try_send(item(frame.clone())).unwrap();
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, size));
    frame
        .metadata
        .insert("extra".into(), serde_json::json!("x".repeat(size)));
    assert_eq!(
        queue.try_send(item(frame)).unwrap_err().0,
        QueueError::PayloadTooLarge
    );
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, size));
    let retained = queue.try_recv().unwrap();
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, 0));
    assert!(matches!(retained.frame.data, FrameData::Text(_)));
}

#[test]
fn strict_profile_rejects_opaque_native_and_callback_payloads() {
    let queue = FrameQueue::with_payload_limits(4, 4, limits(4096)).unwrap();
    let callback = Frame::new(FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
        transform: LocalMessageTransform::new(Ok),
        run_llm: None,
    }));
    assert_eq!(
        queue.try_send(item(callback)).unwrap_err().0,
        QueueError::UnmeasurablePayload
    );
    let owner = SpeculationOwner::new();
    let mut native = Frame::text("small visible payload");
    native.set_native_speculation(owner.capability()).unwrap();
    assert_eq!(
        queue.try_send(item(native.clone())).unwrap_err().0,
        QueueError::UnmeasurablePayload
    );
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, 0));
    // Existing native paths explicitly retain the compatibility profile.
    FrameQueue::new(4).try_send(item(native)).unwrap();
}

#[test]
fn bounded_measurement_handles_escaped_strings_and_rejects_deep_json() {
    let mut frame = Frame::text("[{}]\\\"".repeat(100));
    assert!(PayloadLimits::default().measure(&frame).is_ok());
    let mut value = serde_json::Value::Null;
    for _ in 0..140 {
        value = serde_json::Value::Array(vec![value]);
    }
    frame.metadata.insert("deep".into(), value);
    assert_eq!(
        PayloadLimits::default().measure(&frame),
        Err(QueueError::UnmeasurablePayload)
    );
}

#[tokio::test]
async fn bytes_can_fill_before_items_and_dequeue_wakes_external_sender() {
    let queue = Arc::new(FrameQueue::with_payload_limits(16, 4, limits(2048)).unwrap());
    let frame = sample();
    let size = limits(2048).measure(&frame).unwrap();
    queue.try_send(item(frame.clone())).unwrap();
    assert_eq!(
        queue.try_send(item(frame.clone())).unwrap_err().0,
        QueueError::Full
    );
    assert_eq!(queue.len(), 1);
    let mut send = Box::pin(queue.send(item(frame)));
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut send)
        .await
        .is_err());
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, size));
    let _still_owned_by_consumer = queue.try_recv().unwrap();
    tokio::time::timeout(DEADLINE, send).await.unwrap().unwrap();
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, size));
    drop(queue.try_recv());
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, 0));
}

#[tokio::test]
async fn dropping_pending_send_or_closing_does_not_leak_reservations() {
    let queue = FrameQueue::with_payload_limits(16, 4, limits(2048)).unwrap();
    let frame = sample();
    let size = limits(2048).measure(&frame).unwrap();
    queue.try_send(item(frame.clone())).unwrap();
    let mut send = Box::pin(queue.send(item(frame.clone())));
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut send)
        .await
        .is_err());
    drop(send);
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, size));
    let mut send = Box::pin(queue.send(item(frame)));
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut send)
        .await
        .is_err());
    queue.close();
    assert_eq!(
        tokio::time::timeout(DEADLINE, send).await.unwrap(),
        Err(QueueError::Closed)
    );
    // close preserves already accepted items for the existing drain contract.
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, size));
    drop(queue.try_recv());
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, 0));
}

#[test]
fn system_and_start_have_reserved_bytes_and_interruption_releases_data() {
    let queue = FrameQueue::with_payload_limits(16, 4, limits(2048)).unwrap();
    queue.try_send(item(sample())).unwrap();
    queue.try_send(item(Frame::cancel("test"))).unwrap();
    queue
        .try_send(item(Frame::new(FrameData::Start(StartFrame::default()))))
        .unwrap();
    let (start, control, _, normal) = queue.queued_payload_bytes();
    assert!(start > 0 && control > 0 && normal > 0);
    assert!(matches!(
        queue.try_recv().unwrap().frame.data,
        FrameData::Start(_)
    ));
    assert!(matches!(
        queue.try_recv().unwrap().frame.data,
        FrameData::Cancel(_)
    ));
    queue.try_send(item(Frame::interruption())).unwrap();
    assert_eq!(queue.queued_payload_bytes().3, 0);
    while queue.try_recv().is_some() {}
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, 0));
    let mut preserved = sample();
    preserved.interruptible = false;
    queue.try_send(item(preserved)).unwrap();
    assert_eq!(queue.reset_interruptible(), 0);
    assert!(queue.queued_payload_bytes().3 > 0);
    drop(queue.try_recv());
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, 0));
}

#[test]
fn invalid_profile_is_rejected_without_constructing_queue() {
    assert!(FrameQueue::with_payload_limits(4, 4, limits(0)).is_err());
    assert!(FrameQueue::with_payload_limits(0, 4, limits(2048)).is_err());
    assert!(FrameQueue::with_payload_limits(
        4,
        4,
        PayloadLimits {
            max_frame_bytes: 2048,
            normal_queue_bytes: 1024,
            system_queue_bytes: 2048,
        }
    )
    .is_err());
}

#[tokio::test]
async fn invalid_external_cancel_is_rejected_before_input_is_closed() {
    let mut worker = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .with_payload_limits(limits(2048))
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Start(_)
    ));
    assert!(matches!(
        worker
            .sender()
            .send(Frame::cancel("x".repeat(4096)), Direction::Downstream)
            .await,
        Err(PipelineError::InvalidPayload(_))
    ));
    assert_eq!(worker.state(), WorkerState::Running);
    worker
        .sender()
        .send(Frame::text("still open"), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Text(_)
    ));
    worker.cancel("done").await.unwrap();
    assert_eq!(
        tokio::time::timeout(DEADLINE, worker.join())
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerState::Cancelled
    );
}

struct Gate {
    entered: Notify,
    release: Semaphore,
    context: Mutex<Option<ProcessorContext>>,
}
#[async_trait]
impl FrameProcessor for Gate {
    fn name(&self) -> &str {
        "payload gate"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(t) if t.text == "blocked") {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn runtime_external_wait_internal_failure_and_cancel_remain_distinct() {
    let gate = Arc::new(Gate {
        entered: Notify::new(),
        release: Semaphore::new(0),
        context: Mutex::new(None),
    });
    let mut worker = Pipeline::new(vec![gate.clone()])
        .with_payload_limits(limits(2048))
        .start(PipelineConfig::default())
        .await
        .unwrap();
    next(&mut worker).await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, gate.entered.notified())
        .await
        .unwrap();
    worker
        .sender()
        .send(sample(), Direction::Downstream)
        .await
        .unwrap();
    let sender = worker.sender();
    let mut waiting = Box::pin(sender.send(sample(), Direction::Downstream));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );
    let context = gate.context.lock().unwrap().clone().unwrap();
    let internal = tokio::time::timeout(DEADLINE, context.queue(sample(), Direction::Downstream))
        .await
        .unwrap();
    assert!(internal.unwrap_err().message.contains("full"));
    tokio::time::timeout(DEADLINE, worker.cancel("cancel despite data pressure"))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        tokio::time::timeout(DEADLINE, waiting).await.unwrap(),
        Err(PipelineError::Closed)
    ));
    assert_eq!(
        tokio::time::timeout(DEADLINE, worker.join())
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerState::Cancelled
    );
}

struct Expand;
#[async_trait]
impl FrameProcessor for Expand {
    fn name(&self) -> &str {
        "expand"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            ctx.push(Frame::text("x".repeat(4096)), direction).await
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn oversized_processor_output_is_never_admitted() {
    let mut worker = Pipeline::new(vec![Arc::new(Expand)])
        .with_payload_limits(limits(2048))
        .start(PipelineConfig::default())
        .await
        .unwrap();
    next(&mut worker).await;
    worker
        .sender()
        .send(Frame::text("expand"), Direction::Downstream)
        .await
        .unwrap();
    assert!(tokio::time::timeout(DEADLINE, worker.join())
        .await
        .unwrap()
        .is_err());
    while let Some(output) = worker.recv().await {
        assert!(limits(2048).measure(&output.frame).is_ok());
        assert!(!matches!(output.frame.data, FrameData::Text(_)));
    }
}

struct OwnedExpand {
    result: Arc<Mutex<Option<Result<(), ProcessorError>>>>,
    done: Arc<Notify>,
}
#[async_trait]
impl FrameProcessor for OwnedExpand {
    fn name(&self) -> &str {
        "owned expand"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            let child = ctx.clone();
            let result = self.result.clone();
            let done = self.done.clone();
            ctx.spawn("oversized output", true, async move {
                *result.lock().unwrap() = Some(
                    child
                        .push_downstream_wait(Frame::text("x".repeat(4096)))
                        .await,
                );
                done.notify_one();
                Ok(())
            })?;
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn owned_output_wait_rejects_oversize_even_when_output_is_full() {
    let result = Arc::new(Mutex::new(None));
    let done = Arc::new(Notify::new());
    let mut worker = Pipeline::new(vec![Arc::new(OwnedExpand {
        result: result.clone(),
        done: done.clone(),
    })])
    .with_payload_limits(limits(2048))
    .start(PipelineConfig {
        output_capacity: 1,
        ..PipelineConfig::default()
    })
    .await
    .unwrap();
    // Deliberately leave Start in the sole output slot.
    worker
        .sender()
        .send(Frame::text("expand"), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, done.notified())
        .await
        .unwrap();
    assert!(result
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .unwrap_err()
        .message
        .contains("byte limit"));
    next(&mut worker).await;
    worker.cancel("done").await.unwrap();
    tokio::time::timeout(DEADLINE, worker.join())
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn system_media_cannot_consume_control_item_or_byte_reservations() {
    let audio = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![1; 700], 16000, 1).unwrap(),
    ));
    assert_eq!(audio.priority(), FramePriority::System);
    let size = limits(2048).measure(&audio).unwrap();
    let queue = FrameQueue::with_payload_limits(1, 1, limits(2048)).unwrap();
    queue.try_send(item(audio.clone())).unwrap();
    assert_eq!(queue.try_send(item(audio)).unwrap_err().0, QueueError::Full);
    queue.try_send(item(Frame::cancel("media full"))).unwrap();
    let (_, control, system, _) = queue.queued_payload_bytes();
    assert!(control > 0);
    assert_eq!(system, size);
    assert!(matches!(
        queue.try_recv().unwrap().frame.data,
        FrameData::Cancel(_)
    ));
    assert!(matches!(
        queue.try_recv().unwrap().frame.data,
        FrameData::InputAudio(_)
    ));
    assert_eq!(queue.queued_payload_bytes(), (0, 0, 0, 0));
    let huge = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![255; 4096], 16000, 1).unwrap(),
    ));
    assert_eq!(
        queue.try_send(item(huge)).unwrap_err().0,
        QueueError::PayloadTooLarge
    );
}

#[tokio::test]
async fn child_pipeline_inherits_strict_profile_and_cannot_replace_it() {
    let gate = Arc::new(Gate {
        entered: Notify::new(),
        release: Semaphore::new(0),
        context: Mutex::new(None),
    });
    let mut parent = Pipeline::new(vec![gate.clone()])
        .with_payload_limits(limits(2048))
        .start(PipelineConfig::default())
        .await
        .unwrap();
    next(&mut parent).await;
    let context = gate.context.lock().unwrap().clone().unwrap();
    let mut child = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .inherit_lifecycle(&context)
        .start(PipelineConfig::default())
        .await
        .unwrap();
    next(&mut child).await;
    assert!(matches!(
        child
            .sender()
            .send(Frame::text("x".repeat(4096)), Direction::Downstream)
            .await,
        Err(PipelineError::InvalidPayload(_))
    ));
    assert!(matches!(
        Pipeline::new(vec![Arc::new(IdentityProcessor)])
            .inherit_lifecycle(&context)
            .with_payload_limits(limits(4096))
            .start(PipelineConfig::default())
            .await,
        Err(PipelineError::InvalidConfiguration(_))
    ));
    child.cancel("done").await.unwrap();
    tokio::time::timeout(DEADLINE, child.join())
        .await
        .unwrap()
        .unwrap();
    parent.cancel("done").await.unwrap();
    tokio::time::timeout(DEADLINE, parent.join())
        .await
        .unwrap()
        .unwrap();
}
