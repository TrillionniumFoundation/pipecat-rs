use pipecat_core::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

struct Capture {
    setup: Mutex<Option<ProcessorContext>>,
    invocation: Mutex<Option<ProcessorContext>>,
    seen: Semaphore,
}
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "capture"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.setup.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.invocation.lock().unwrap() = Some(ctx.clone());
            self.seen.add_permits(1);
        }
        ctx.push(frame, direction).await
    }
}
async fn capture() -> (PipelineWorker, Arc<Capture>, ProcessorContext) {
    let capture = Arc::new(Capture {
        setup: Mutex::new(None),
        invocation: Mutex::new(None),
        seen: Semaphore::new(0),
    });
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    worker
        .sender()
        .send(Frame::text("capture"), Direction::Downstream)
        .await
        .unwrap();
    capture.seen.acquire().await.unwrap().forget();
    worker.recv().await.unwrap();
    let context = capture.invocation.lock().unwrap().as_ref().unwrap().clone();
    (worker, capture, context)
}
async fn interrupt(worker: &mut PipelineWorker) {
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Interruption
    ));
}
async fn finish(worker: PipelineWorker) -> (Vec<DirectedFrame>, WorkerReport) {
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_ingress_checks_origin_and_destination_cutoffs() {
    for stale_origin in [true, false] {
        let (mut source, _, context) = capture().await;
        let (mut destination, _, _) = capture().await;
        if stale_origin {
            interrupt(&mut source).await;
        } else {
            interrupt(&mut destination).await;
        }
        destination
            .sender()
            .try_send_from(Frame::text("stale"), Direction::Downstream, &context)
            .unwrap();
        destination
            .sender()
            .try_send_from(
                Frame::text("kept").with_interruptible(false),
                Direction::Downstream,
                &context,
            )
            .unwrap();
        let (frames, report) = finish(destination).await;
        assert!(report.errors.is_empty());
        assert_eq!(report.dropped_frames, 1);
        let texts: Vec<_> = frames
            .iter()
            .filter_map(|f| {
                if let FrameData::Text(t) = &f.frame.data {
                    Some(t.text.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(texts, ["kept"]);
        finish(source).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffered_forwarding_context_checks_source_destination_and_nested_origins() {
    for stale_node in 0..3 {
        let (mut source, _, source_context) = capture().await;
        let (mut relay, relay_capture, _) = capture().await;
        let (mut destination, destination_capture, _) = capture().await;
        let relay_context = relay_capture
            .setup
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .forwarding_context(&source_context);
        let forwarded = destination_capture
            .setup
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .forwarding_context(&relay_context);
        match stale_node {
            0 => interrupt(&mut source).await,
            1 => interrupt(&mut relay).await,
            _ => interrupt(&mut destination).await,
        }
        forwarded
            .push(Frame::text("stale"), Direction::Downstream)
            .await
            .unwrap();
        forwarded
            .queue(Frame::text("stale-self"), Direction::Downstream)
            .await
            .unwrap();
        forwarded
            .push(
                Frame::text("kept").with_interruptible(false),
                Direction::Downstream,
            )
            .await
            .unwrap();
        assert_eq!(
            forwarded
                .spawn("stale-task", true, async {
                    panic!("stale task executed");
                    #[allow(unreachable_code)]
                    Ok(())
                })
                .unwrap(),
            0
        );
        let (frames, report) = finish(destination).await;
        assert!(report.errors.is_empty());
        assert_eq!(report.dropped_frames, 2);
        let texts: Vec<_> = frames
            .iter()
            .filter_map(|f| {
                if let FrameData::Text(t) = &f.frame.data {
                    Some(t.text.as_str())
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(texts, ["kept"]);
        finish(relay).await;
        finish(source).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_output_keeps_origin_epoch_when_returned_to_parent() {
    let (mut parent, parent_capture, parent_context) = capture().await;
    let (mut child, child_capture, _) = capture().await;
    child
        .sender()
        .try_send_from(
            Frame::text("child-input"),
            Direction::Downstream,
            &parent_context,
        )
        .unwrap();
    child_capture.seen.acquire().await.unwrap().forget();
    child.recv().await.unwrap();
    let child_context = child_capture
        .invocation
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .clone();
    let relay = parent_capture
        .setup
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .forwarding_context(&child_context);
    interrupt(&mut parent).await;
    relay
        .push(Frame::text("stale-child"), Direction::Downstream)
        .await
        .unwrap();
    let (frames, report) = finish(parent).await;
    assert!(report.errors.is_empty());
    assert_eq!(report.dropped_frames, 1);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::Text(_))));
    finish(child).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opposite_direction_bridges_use_consistent_lock_order() {
    let (left, left_capture, left_context) = capture().await;
    let (right, right_capture, right_context) = capture().await;
    let left_forward = left_capture
        .setup
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .forwarding_context(&right_context);
    let right_forward = right_capture
        .setup
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .forwarding_context(&left_context);
    let a = tokio::spawn(async move {
        for _ in 0..200 {
            left_forward
                .push(Frame::text("left"), Direction::Downstream)
                .await
                .unwrap();
        }
    });
    let b = tokio::spawn(async move {
        for _ in 0..200 {
            right_forward
                .push(Frame::text("right"), Direction::Downstream)
                .await
                .unwrap();
        }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        a.await.unwrap();
        b.await.unwrap();
    })
    .await
    .expect("opposite bridge locks deadlocked");
    let (left_frames, left_report) = finish(left).await;
    let (right_frames, right_report) = finish(right).await;
    assert!(left_report.errors.is_empty() && right_report.errors.is_empty());
    assert_eq!(left_frames.len(), 201);
    assert_eq!(right_frames.len(), 201);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_node_forwarding_context_can_broadcast_and_continue_its_invocation() {
    let (worker, capture, source) = capture().await;
    let forwarded = capture
        .setup
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .forwarding_context(&source);
    forwarded.broadcast_interruption().await.unwrap();
    forwarded
        .push(Frame::text("fresh"), Direction::Downstream)
        .await
        .unwrap();
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert!(frames
        .iter()
        .any(|f| matches!(&f.frame.data, FrameData::Text(t) if t.text == "fresh")));
}
