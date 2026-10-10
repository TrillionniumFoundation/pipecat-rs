use pipecat_core::{
    Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig, ProcessorContext,
    ProcessorError,
};
use pipecat_workers::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("operation stalled")
}

#[tokio::test]
async fn frame_bridge_filters_and_keeps_direction_then_drains_end() {
    let mut pipeline = Pipeline::new(vec![])
        .start(Default::default())
        .await
        .unwrap();
    let bus = LocalBus::new(16).unwrap();
    let registry = WorkerRegistry::new("runner");
    let mut input = PipelineBusInput::attach(
        "pipeline",
        bus.clone(),
        registry.clone(),
        pipeline.sender(),
        PipelineInputOptions {
            active: true,
            bridges: Some(vec!["selected".into()]),
        },
    )
    .unwrap();
    let ignored = Frame::text("ignored");
    let kept = Frame::text("kept");
    for (frame, bridge) in [
        (ignored, Some("other".into())),
        (kept.clone(), Some("selected".into())),
    ] {
        bus.publish(BusMessage {
            source: "peer".into(),
            target: Some("pipeline".into()),
            data: MessageData::Frame {
                frame: Box::new(frame),
                direction: Direction::Upstream,
                bridge,
            },
        })
        .unwrap();
    }
    bus.publish(BusMessage {
        source: "runner".into(),
        target: Some("pipeline".into()),
        data: MessageData::EndWorker { reason: None },
    })
    .unwrap();
    let mut text = vec![];
    while let Some(frame) = bounded(pipeline.recv()).await {
        if matches!(frame.frame.data, FrameData::Text(_)) {
            text.push(frame);
        }
    }
    assert_eq!(text.len(), 1);
    assert_eq!(text[0].frame.id, kept.id);
    assert_eq!(text[0].direction, Direction::Upstream);
    assert_eq!(
        bounded(pipeline.join()).await.unwrap().state,
        pipecat_core::WorkerState::Ended
    );
    bounded(input.wait()).await.unwrap();
    assert!(registry.get("pipeline").is_none());
}

struct Stall {
    started: Arc<Notify>,
}
#[pipecat_core::async_trait]
impl FrameProcessor for Stall {
    fn name(&self) -> &str {
        "stall"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        context: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            self.started.notify_one();
            std::future::pending::<()>().await;
        }
        context.push(frame, direction).await
    }
}
#[tokio::test]
async fn cancel_crosses_a_blocked_pipeline_input_lane() {
    let started = Arc::new(Notify::new());
    let mut pipeline = Pipeline::new(vec![Arc::new(Stall {
        started: started.clone(),
    })])
    .start(PipelineConfig {
        queue_capacity: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    let bus = LocalBus::new(16).unwrap();
    let registry = WorkerRegistry::new("runner");
    let mut input = PipelineBusInput::attach(
        "pipeline",
        bus.clone(),
        registry,
        pipeline.sender(),
        PipelineInputOptions {
            active: true,
            ..Default::default()
        },
    )
    .unwrap();
    for n in 0..4 {
        bus.publish(BusMessage {
            source: "peer".into(),
            target: Some("pipeline".into()),
            data: MessageData::Frame {
                frame: Box::new(Frame::text(n.to_string())),
                direction: Direction::Downstream,
                bridge: None,
            },
        })
        .unwrap();
    }
    bounded(started.notified()).await;
    tokio::task::yield_now().await;
    bus.publish(BusMessage {
        source: "runner".into(),
        target: Some("pipeline".into()),
        data: MessageData::CancelWorker { reason: None },
    })
    .unwrap();
    while bounded(pipeline.recv()).await.is_some() {}
    assert_eq!(
        bounded(pipeline.join()).await.unwrap().state,
        pipecat_core::WorkerState::Cancelled
    );
    bounded(input.wait()).await.unwrap();
}
