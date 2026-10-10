mod support;
use pipecat_core::*;
use pipecat_gladia::*;
use pipecat_services::Context;
use pipecat_turns::{
    LLMContextAggregatorPair, UserAggregatorConfig, UserTurnStartStrategies, UserTurnStartStrategy,
    UserTurnStopStrategies, UserTurnStopStrategy,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use support::*;
#[derive(Default)]
struct Recorder(Mutex<Vec<Frame>>);
impl Recorder {
    fn count(&self, f: impl Fn(&FrameData) -> bool) -> usize {
        self.0.lock().unwrap().iter().filter(|v| f(&v.data)).count()
    }
}
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "GladiaFixtureRecorder"
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
async fn real_client_processor_metadata_and_shared_aggregator_compose() {
    let mut peer = peer(1).await;
    peer.config.settings.enable_vad = true;
    peer.config.should_interrupt = false;
    let stt = Arc::new(
        GladiaSttProcessor::new(GladiaSttClient::new(peer.config.clone()).unwrap())
            .with_user_id("speaker-7")
            .unwrap(),
    );
    // Deliberately use native defaults: metadata must recommend external turns.
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig {
            empty_user_turn: None,
            ..Default::default()
        },
        Default::default(),
    );
    let raw = Arc::new(Recorder::default());
    let downstream = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        stt.clone(),
        raw.clone(),
        pair.user(),
        downstream.clone(),
    ])
    .start(PipelineConfig {
        start: StartFrame {
            enable_usage_metrics: true,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(pcm(640))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    next_wire(&mut peer).await;
    emit(&peer, json!({"type":"speech_start"})).await;
    emit(&peer, json!({"type":"speech_start"})).await;
    emit(&peer, transcript("draft", false)).await;
    wait(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    emit(&peer, json!({"type":"translation","data":null})).await;
    emit(&peer,json!({"type":"translation","data":{"original_language":"en","translated_utterance":{"language":"fr","text":"bonjour"}}})).await;
    let final_message = transcript("hello world", true);
    emit(&peer, final_message.clone()).await;
    emit(&peer, json!({"type":"speech_end"})).await;
    emit(&peer, json!({"type":"speech_end"})).await;
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
    wait(|| downstream.count(|f| matches!(f, FrameData::UserStoppedSpeaking)) == 1).await;
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::UserStartedSpeaking)),
        1
    );
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::Interruption)),
        0
    );
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
        assert_eq!(t.user_id, "speaker-7");
        assert_eq!(t.text.text, "hello world");
        assert_eq!(t.result.as_ref(), Some(&final_message));
        assert_eq!(t.language.as_deref(), Some("en"));
        assert!(!t.timestamp.is_empty());
        assert!(frames.iter().any(|f|matches!(&f.data,FrameData::TranslationDetailed(t)if t.user_id.is_empty()&&t.text.text=="bonjour")));
        assert!(frames.iter().any(|f|matches!(&f.data,FrameData::SttMetadata(m)if m.base.user_turn_strategies.is_some()&&m.ttfs_p99_latency==1.49)));
    }
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(report.errors[0].error.contains("translation addon"));
    assert_eq!(next_wire(&mut peer).await, json!({"type":"stop_recording"}));
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn explicit_application_strategies_win_and_vad_off_keeps_defaults() {
    for (vad, explicit) in [(true, true), (false, false)] {
        let peer = support::peer(1).await;
        let mut config = peer.config.clone();
        config.settings.enable_vad = vad;
        let stt = Arc::new(GladiaSttProcessor::new(
            GladiaSttClient::new(config).unwrap(),
        ));
        let raw = Arc::new(Recorder::default());
        let downstream = Arc::new(Recorder::default());
        let mut user = UserAggregatorConfig {
            empty_user_turn: None,
            ..Default::default()
        };
        if explicit {
            user.turn.start_strategies = Some(
                UserTurnStartStrategies::new(vec![
                    UserTurnStartStrategy::vad().with_interruptions(false)
                ])
                .unwrap(),
            );
            user.turn.stop_strategies = Some(
                UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(
                    std::time::Duration::from_millis(100),
                    true,
                )])
                .unwrap(),
            );
        }
        let pair = LLMContextAggregatorPair::new(Context::default(), user, Default::default());
        let worker = Pipeline::new(vec![stt, raw.clone(), pair.user(), downstream.clone()])
            .start(Default::default())
            .await
            .unwrap();
        emit(&peer, json!({"type":"speech_start"})).await;
        emit(&peer, transcript("fixture", false)).await;
        wait(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
        if explicit {
            assert_eq!(
                raw.count(|f| matches!(f, FrameData::ProposedUserStartedSpeaking)),
                1
            );
            assert_eq!(
                downstream.count(|f| matches!(f, FrameData::UserStartedSpeaking)),
                0
            );
        } else {
            assert_eq!(
                raw.count(|f| matches!(f, FrameData::ProposedUserStartedSpeaking)),
                0
            );
            assert!(raw.0.lock().unwrap().iter().any(|f|matches!(&f.data,FrameData::SttMetadata(m)if m.base.user_turn_strategies.is_none())));
        }
        worker.cancel("fixture cleanup").await.unwrap();
        bounded(worker.collect()).await.unwrap();
        bounded(peer.task).await.unwrap();
    }
}
#[tokio::test]
async fn future_settings_survive_cleanup_and_next_real_session() {
    let mut peer = peer(2).await;
    let processor = Arc::new(GladiaSttProcessor::new(
        GladiaSttClient::new(peer.config.clone()).unwrap(),
    ));
    let first = Pipeline::new(vec![processor.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let (_, initial) = bounded(peer.requests.recv()).await.unwrap();
    assert_eq!(initial["model"], "solaria-1");
    processor
        .sender()
        .unwrap()
        .merge_settings(
            json!({"model":"next-session","enable_vad":true})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        processor.config_snapshot().unwrap().settings.model,
        "next-session"
    );
    assert_eq!(
        processor.sender().unwrap().active_settings().model,
        "solaria-1"
    );
    first.end().await.unwrap();
    let (_, report) = bounded(first.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let second = Pipeline::new(vec![processor.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let (_, next) = bounded(peer.requests.recv()).await.unwrap();
    assert_eq!(next["model"], "next-session");
    assert!(next.get("enable_vad").is_none());
    assert!(processor.sender().unwrap().active_settings().enable_vad);
    second.end().await.unwrap();
    let (_, report) = bounded(second.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn mute_cancel_and_health_fence_provider_callbacks() {
    let peer = peer(1).await;
    let processor = Arc::new(GladiaSttProcessor::new(
        GladiaSttClient::new(peer.config.clone()).unwrap(),
    ));
    let raw = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![processor, raw.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
        .await
        .unwrap();
    wait(|| raw.count(|f| matches!(f, FrameData::SttMute(true))) == 1).await;
    emit(&peer, transcript("muted", true)).await;
    // A downstream marker witnesses completed normal-lane processing.
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(pcm(640))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    wait(|| raw.count(|f| matches!(f, FrameData::InputAudio(_))) == 1).await;
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    bounded(peer.task).await.unwrap();
    assert_eq!(
        raw.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))),
        0
    );
    worker.cancel("fixture cleanup").await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
}
#[tokio::test]
async fn end_drains_admitted_audio_and_cancel_never_sends_stop_recording() {
    for end in [true, false] {
        let mut peer = support::peer(1).await;
        let processor = Arc::new(GladiaSttProcessor::new(
            GladiaSttClient::new(peer.config.clone()).unwrap(),
        ));
        let worker = Pipeline::new(vec![processor.clone()])
            .start(Default::default())
            .await
            .unwrap();
        for _ in 0..8 {
            processor
                .sender()
                .unwrap()
                .try_send_audio(pcm(640))
                .unwrap();
        }
        if end {
            worker.end().await.unwrap()
        } else {
            worker.cancel("fixture cancellation").await.unwrap()
        }
        bounded(worker.collect()).await.unwrap();
        bounded(peer.task).await.unwrap();
        let mut audio = 0;
        let mut stop = 0;
        let mut close = 0;
        while let Some(message) = peer.wire.recv().await {
            if matches!(message, tokio_tungstenite::tungstenite::Message::Close(_)) {
                close += 1;
                if end {
                    assert_eq!(stop, 1, "graceful Close must follow stop_recording");
                }
            }
            if let tokio_tungstenite::tungstenite::Message::Text(s) = message {
                let v: serde_json::Value = serde_json::from_str(&s).unwrap();
                if v["type"] == "audio_chunk" {
                    assert_eq!(stop, 0, "no audio may follow stop_recording");
                    assert_eq!(close, 0, "no audio may follow Close");
                    audio += 1
                }
                if v["type"] == "stop_recording" {
                    stop += 1;
                    assert_eq!(audio, 8)
                }
            }
        }
        assert_eq!(stop, usize::from(end));
        let terminations = peer.terminations.lock().unwrap();
        assert_eq!(terminations.len(), 1);
        if end {
            assert_eq!(audio, 8);
            assert_eq!(close, 1, "graceful End must deliver exactly one Close");
            assert_eq!(terminations[0], PeerTermination::Close);
        } else {
            assert!(close <= 1);
            assert!(matches!(
                terminations[0],
                PeerTermination::Close | PeerTermination::Eof | PeerTermination::ReadError
            ));
            assert_eq!(close == 1, terminations[0] == PeerTermination::Close);
        }
    }
}
