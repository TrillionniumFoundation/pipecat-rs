mod support;
use pipecat_core::*;
use pipecat_gradium::*;
use pipecat_services::Context;
use pipecat_turns::{
    LLMContextAggregatorPair, UserAggregatorConfig, UserTurnStartStrategies, UserTurnStartStrategy,
    UserTurnStopStrategies, UserTurnStopStrategy,
};
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use support::*;
#[derive(Default)]
struct Recorder(Mutex<Vec<Frame>>);
impl Recorder {
    fn count(&self, p: impl Fn(&FrameData) -> bool) -> usize {
        self.0.lock().unwrap().iter().filter(|f| p(&f.data)).count()
    }
}
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "GradiumRecorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.0.lock().unwrap().push(frame.clone())
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn native_provider_proposals_metadata_usage_and_shared_context_compose() {
    let mut peer = support::peer().await;
    peer.config.enable_turn_detection = true;
    peer.config.enable_interruptions = false;
    let stt = Arc::new(
        GradiumSttProcessor::new(GradiumSttClient::new(peer.config.clone()).unwrap())
            .with_user_id("speaker")
            .unwrap(),
    );
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig {
            empty_user_turn: None,
            ..Default::default()
        },
        Default::default(),
    );
    let raw = Arc::new(Recorder::default());
    let output = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt, raw.clone(), pair.user(), output.clone()])
        .start(PipelineConfig {
            start: StartFrame {
                enable_usage_metrics: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    wire(&mut peer).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(pcm(2560))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    wire(&mut peer).await;
    emit(&peer, step(0.9)).await;
    emit(&peer, step(0.1)).await;
    emit(&peer, json!({"type":"text","text":"hello"})).await;
    wait(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    emit(&peer, step(0.5)).await;
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"flush","flush_id":"1"})
    );
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    emit(&peer, json!({"type":"text","text":"world"})).await;
    wait(|| !pair.context().snapshot().unwrap().messages.is_empty()).await;
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    assert_eq!(
        raw.count(|f| matches!(f, FrameData::ProposedUserStartedSpeaking)),
        1
    );
    assert_eq!(
        raw.count(|f| matches!(f, FrameData::ProposedUserStoppedSpeaking)),
        1
    );
    wait(|| output.count(|f| matches!(f, FrameData::UserStoppedSpeaking)) == 1).await;
    assert_eq!(
        output.count(|f| matches!(f, FrameData::UserStartedSpeaking)),
        1
    );
    assert_eq!(output.count(|f| matches!(f, FrameData::Interruption)), 0);
    {
        let frames = raw.0.lock().unwrap();
        let usage = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::Metrics(_)))
            .unwrap();
        let final_index = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::TranscriptionDetailed(_)))
            .unwrap();
        assert!(usage < final_index);
        let FrameData::TranscriptionDetailed(t) = &frames[final_index].data else {
            panic!()
        };
        assert_eq!(t.text.text, "hello world");
        assert_eq!(t.user_id, "speaker");
        assert!(t.finalized);
        assert!(frames.iter().any(|f|matches!(&f.data,FrameData::SttMetadata(m) if m.base.user_turn_strategies.is_some()&&m.ttfs_p99_latency==0.62)));
    }
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(wire(&mut peer).await, json!({"type":"end_of_stream"}));
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn explicit_user_strategies_override_provider_metadata() {
    let mut peer = support::peer().await;
    peer.config.enable_turn_detection = true;
    let stt = Arc::new(GradiumSttProcessor::new(
        GradiumSttClient::new(peer.config.clone()).unwrap(),
    ));
    let mut config = UserAggregatorConfig {
        empty_user_turn: None,
        ..Default::default()
    };
    config.turn.start_strategies = Some(
        UserTurnStartStrategies::new(vec![UserTurnStartStrategy::vad().with_interruptions(false)])
            .unwrap(),
    );
    config.turn.stop_strategies = Some(
        UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(
            Duration::from_millis(100),
            true,
        )])
        .unwrap(),
    );
    let pair = LLMContextAggregatorPair::new(Context::default(), config, Default::default());
    let output = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt, pair.user(), output.clone()])
        .start(Default::default())
        .await
        .unwrap();
    wire(&mut peer).await;
    emit(&peer, step(1.0)).await;
    emit(&peer, step(0.0)).await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(
        output.count(|f| matches!(f, FrameData::UserStartedSpeaking)),
        0
    );
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn end_and_stop_drain_remote_final_before_terminal() {
    for stop in [false, true] {
        let mut peer = support::peer().await;
        let stt = Arc::new(GradiumSttProcessor::new(
            GradiumSttClient::new(peer.config.clone()).unwrap(),
        ));
        let raw = Arc::new(Recorder::default());
        let worker = Pipeline::new(vec![stt, raw.clone()])
            .start(Default::default())
            .await
            .unwrap();
        wire(&mut peer).await;
        emit(&peer, json!({"type":"text","text":"terminal tail"})).await;
        wait(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
        if stop {
            worker
                .sender()
                .send(Frame::new(FrameData::Stop), Direction::Downstream)
                .await
                .unwrap()
        } else {
            worker.end().await.unwrap()
        }
        let (_, report) = bounded(worker.collect()).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        {
            let frames = raw.0.lock().unwrap();
            let final_index = frames
                .iter()
                .position(|f| matches!(f.data, FrameData::TranscriptionDetailed(_)))
                .unwrap();
            let terminal = frames
                .iter()
                .position(|f| matches!(f.data, FrameData::End(_) | FrameData::Stop))
                .unwrap();
            assert!(final_index < terminal);
        }
        bounded(peer.task).await.unwrap();
    }
}
#[tokio::test]
async fn cancel_fences_pending_trailing_text() {
    let mut peer = support::peer().await;
    let stt = Arc::new(GradiumSttProcessor::new(
        GradiumSttClient::new(peer.config.clone()).unwrap(),
    ));
    let raw = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt.clone(), raw.clone()])
        .start(Default::default())
        .await
        .unwrap();
    wire(&mut peer).await;
    stt.sender().unwrap().try_flush().unwrap();
    wire(&mut peer).await;
    emit(&peer, json!({"type":"text","text":"cancel me"})).await;
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    wait(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
    worker.cancel("fixture cancellation").await.unwrap();
    bounded(worker.collect()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(
        raw.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))),
        0
    );
    bounded(peer.task).await.unwrap();
}
