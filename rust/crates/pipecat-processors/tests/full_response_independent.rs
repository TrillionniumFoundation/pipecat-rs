//! Independent source-contract and lifecycle regression fixtures.
use pipecat_core::*;
use pipecat_processors::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

const WAIT: Duration = Duration::from_secs(3);

async fn receive(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(WAIT, worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn forward(worker: &mut PipelineWorker, data: FrameData, direction: Direction) -> u64 {
    let mut frame = Frame::new(data);
    frame.pts = Some(87654321);
    frame.metadata.insert(
        "independent-envelope".into(),
        serde_json::json!({"x": [1, false, "你"]}),
    );
    frame.transport_source = Some("unchanged-source".into());
    frame.transport_destination = Some("unchanged-destination".into());
    let tag = LocalFrameTag::new();
    tag.mark(&mut frame).unwrap();
    let id = frame.id;
    let expected = serde_json::to_value(&frame).unwrap();
    worker.sender().send(frame, direction).await.unwrap();
    loop {
        let output = receive(worker).await;
        if output.frame.id == id {
            assert_eq!(output.direction, direction);
            assert_eq!(serde_json::to_value(&output.frame).unwrap(), expected);
            assert!(tag.contains(&output.frame));
            return id;
        }
        assert!(matches!(output.frame.data, FrameData::Error(_)));
    }
}
fn text(s: &str) -> FrameData {
    FrameData::LlmText(TextFrame::llm(s))
}
fn start_variant(index: usize) -> FrameData {
    match index {
        0 => FrameData::LlmResponseStart,
        1 => FrameData::LlmFullResponseStart(Default::default()),
        _ => FrameData::VisionFullResponseStart(Default::default()),
    }
}
fn end_variant(index: usize) -> FrameData {
    match index {
        0 => FrameData::LlmResponseEnd,
        1 => FrameData::LlmFullResponseEnd(Default::default()),
        _ => FrameData::VisionFullResponseEnd(Default::default()),
    }
}
fn recorder() -> (
    FullResponseCallback,
    mpsc::UnboundedReceiver<FullResponseCompletion>,
) {
    let (send, recv) = mpsc::unbounded_channel();
    let callback: FullResponseCallback = Arc::new(move |completion| {
        let send = send.clone();
        Box::pin(async move {
            send.send(completion).unwrap();
            Ok(())
        })
    });
    (callback, recv)
}
async fn result(
    recv: &mut mpsc::UnboundedReceiver<FullResponseCompletion>,
) -> FullResponseCompletion {
    tokio::time::timeout(WAIT, recv.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn independently_pins_cross_variant_exact_text_and_all_inactive_boundaries() {
    let (callback, mut recv) = recorder();
    let aggregator =
        Arc::new(LlmFullResponseAggregator::new(Default::default(), callback).unwrap());
    let mut worker = Pipeline::new(vec![aggregator.clone()])
        .start(Default::default())
        .await
        .unwrap();
    assert!(receive(&mut worker).await.frame.data.is_start());
    let mut sequence = 0;
    for direction in [Direction::Upstream, Direction::Downstream] {
        for s in 0..3 {
            for e in 0..3 {
                forward(&mut worker, text("outside"), direction).await;
                forward(&mut worker, start_variant(s), direction).await;
                forward(&mut worker, text(" \tA\0"), direction).await;
                forward(&mut worker, start_variant((s + 1) % 3), direction).await;
                let mut vision = TextFrame::llm("你好\r\n🙂  ");
                vision.includes_inter_frame_spaces = false;
                vision.append_to_context = false;
                vision.skip_tts = Some(true);
                forward(&mut worker, FrameData::VisionText(vision), direction).await;
                forward(
                    &mut worker,
                    FrameData::Text(TextFrame::new("exclude text")),
                    direction,
                )
                .await;
                forward(
                    &mut worker,
                    FrameData::LlmThoughtText(TextFrame::new("exclude thought")),
                    direction,
                )
                .await;
                let id = forward(&mut worker, end_variant(e), direction).await;
                sequence += 1;
                let event = result(&mut recv).await;
                assert_eq!(event.text, " \tA\0你好\r\n🙂  ");
                assert!(event.completed);
                assert_eq!(
                    (
                        event.session_id,
                        event.sequence,
                        event.trigger_frame_id,
                        event.direction
                    ),
                    (1, sequence, id, direction)
                );
            }
        }
        for e in 0..3 {
            forward(&mut worker, text("outside after end"), direction).await;
            let id = forward(&mut worker, end_variant(e), direction).await;
            sequence += 1;
            let event = result(&mut recv).await;
            assert_eq!(event.text, "");
            assert!(event.completed);
            assert_eq!((event.sequence, event.trigger_frame_id), (sequence, id));
        }
        for _ in 0..2 {
            let id = forward(&mut worker, FrameData::Interruption, direction).await;
            sequence += 1;
            let event = result(&mut recv).await;
            assert_eq!(event.text, "");
            assert!(!event.completed);
            assert_eq!((event.sequence, event.trigger_frame_id), (sequence, id));
        }
    }
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(WAIT, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.total_errors, 0);
    assert_eq!(aggregator.stats().succeeded, sequence);
}

/// Holds the real runtime between prepare_end and its own background drain.
struct ClosedGate {
    inner: Arc<LlmFullResponseAggregator>,
    closed: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl FrameProcessor for ClosedGate {
    fn name(&self) -> &str {
        "IndependentClosedGate"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await?;
        self.closed.notify_one();
        self.release.notified().await;
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

#[tokio::test]
async fn prepare_end_closes_response_and_callback_admission_before_core_drain() {
    for terminal in [
        FrameData::End(Default::default()),
        FrameData::EndDetailed(Default::default()),
        FrameData::Stop,
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let callback: FullResponseCallback = {
            let calls = calls.clone();
            Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })
        };
        let aggregator =
            Arc::new(LlmFullResponseAggregator::new(Default::default(), callback).unwrap());
        let closed = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let gate = Arc::new(ClosedGate {
            inner: aggregator.clone(),
            closed: closed.clone(),
            release: release.clone(),
        });
        let mut worker = Pipeline::new(vec![gate])
            .start(Default::default())
            .await
            .unwrap();
        receive(&mut worker).await;
        forward(
            &mut worker,
            FrameData::LlmResponseStart,
            Direction::Downstream,
        )
        .await;
        forward(
            &mut worker,
            text("discard before draining"),
            Direction::Downstream,
        )
        .await;
        worker
            .sender()
            .send(Frame::new(terminal), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(WAIT, closed.notified()).await.unwrap();
        assert_eq!(aggregator.stats().buffered_capacity, 0);
        // Urgent processing remains live while the normal terminal is paused.
        forward(&mut worker, FrameData::Interruption, Direction::Downstream).await;
        assert_eq!(aggregator.stats().admitted, 0);
        assert_eq!(aggregator.stats().boundaries, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        release.notify_one();
        let (_, report) = tokio::time::timeout(WAIT, worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.total_errors, 0);
        assert_eq!(aggregator.stats().in_flight_callbacks, 0);
    }
}

#[tokio::test]
async fn interrupted_overflow_never_delivers_partial_and_empty_next_end_still_delivers() {
    let (callback, mut recv) = recorder();
    let aggregator = Arc::new(
        LlmFullResponseAggregator::new(
            FullResponseAggregatorConfig {
                max_response_bytes: 3,
                ..Default::default()
            },
            callback,
        )
        .unwrap(),
    );
    let mut worker = Pipeline::new(vec![aggregator.clone()])
        .start(Default::default())
        .await
        .unwrap();
    receive(&mut worker).await;
    forward(
        &mut worker,
        FrameData::LlmResponseStart,
        Direction::Downstream,
    )
    .await;
    forward(&mut worker, text("é"), Direction::Downstream).await;
    forward(&mut worker, text("é"), Direction::Downstream).await;
    forward(
        &mut worker,
        FrameData::VisionFullResponseStart(Default::default()),
        Direction::Downstream,
    )
    .await;
    forward(&mut worker, text("x"), Direction::Downstream).await;
    forward(&mut worker, FrameData::Interruption, Direction::Downstream).await;
    assert!(recv.try_recv().is_err());
    forward(
        &mut worker,
        FrameData::VisionFullResponseEnd(Default::default()),
        Direction::Downstream,
    )
    .await;
    let event = result(&mut recv).await;
    assert_eq!(event.text, "");
    assert!(event.completed);
    assert_eq!(event.sequence, 2);
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(WAIT, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.total_errors, 2);
    assert_eq!(aggregator.stats().response_overflows, 1);
    assert_eq!(aggregator.stats().discarded_responses, 1);
}

#[tokio::test]
async fn admitted_partial_callback_survives_a_later_distinct_interruption() {
    let (send, mut recv) = mpsc::unbounded_channel();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let callback: FullResponseCallback = {
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move |completion| {
            let entered = entered.clone();
            let release = release.clone();
            let send = send.clone();
            Box::pin(async move {
                if completion.sequence == 1 {
                    entered.notify_one();
                    release.notified().await;
                }
                send.send(completion).unwrap();
                Ok(())
            })
        })
    };
    let aggregator =
        Arc::new(LlmFullResponseAggregator::new(Default::default(), callback).unwrap());
    let mut worker = Pipeline::new(vec![aggregator.clone()])
        .start(Default::default())
        .await
        .unwrap();
    receive(&mut worker).await;
    forward(
        &mut worker,
        FrameData::LlmResponseStart,
        Direction::Downstream,
    )
    .await;
    forward(&mut worker, text("old partial"), Direction::Downstream).await;
    forward(&mut worker, FrameData::Interruption, Direction::Downstream).await;
    tokio::time::timeout(WAIT, entered.notified())
        .await
        .unwrap();
    forward(&mut worker, FrameData::Interruption, Direction::Downstream).await;
    let second = result(&mut recv).await;
    assert_eq!(
        (second.sequence, second.text.as_str(), second.completed),
        (2, "", false)
    );
    release.notify_one();
    let first = result(&mut recv).await;
    assert_eq!(
        (first.sequence, first.text.as_str(), first.completed),
        (1, "old partial", false)
    );
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(WAIT, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.total_errors, 0);
    assert_eq!(aggregator.stats().cancelled, 0);
    assert_eq!(aggregator.stats().succeeded, 2);
}
