use pipecat_core::*;
use pipecat_services::{Content, Context, Role};
use pipecat_turns::*;
use pipecat_vad::{
    MonotonicSmartTurnClock, SmartTurnAnalyzer, SmartTurnCancellation, SmartTurnParams,
    SmartTurnPrediction, SmartTurnPredictor,
};
use std::{sync::Arc, time::Duration};
struct NoInference;
impl SmartTurnPredictor for NoInference {
    fn predict(
        &mut self,
        _samples: &[i16],
        _rate: u32,
        _cancellation: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        panic!("deterministic policy test must not infer")
    }
}
fn analyzer() -> SmartTurnAnalyzer {
    SmartTurnAnalyzer::with_predictor(
        NoInference,
        16_000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap()
}
fn transcript(text: &str) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    })
}
fn metadata() -> FrameData {
    FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "remote".into(),
            user_turn_strategies: Some(UserTurnStrategies {
                start: Some(vec![UserTurnStrategy::External {
                    enable_interruptions: true,
                }]),
                stop: Some(vec![UserTurnStrategy::External {
                    enable_interruptions: true,
                }]),
            }),
        },
        is_realtime_service: true,
    })
}
fn history(pair: &LLMContextAggregatorPair) -> Vec<(Role, String)> {
    pair.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .map(|m| {
            (
                m.role,
                match m.content {
                    Some(Content::Text(t)) => t,
                    _ => String::new(),
                },
            )
        })
        .collect()
}
#[tokio::test]
async fn review_failed_wrapper_configuration_does_not_poison_later_realtime_activation() {
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                stop_strategies: Some(
                    UserTurnStopStrategies::new(vec![UserTurnStopStrategy::external(
                        Duration::from_millis(500),
                        true,
                    )])
                    .unwrap(),
                ),
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
        RealtimeServiceMode::Auto,
    )
    .unwrap();
    assert!(SmartTurnUserAggregator::with_aggregator(pair.user(), analyzer()).is_err());
    pair.assistant()
        .process_at(
            Frame::new(metadata()),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    assert!(pair.user().realtime_service_enabled().unwrap());
    pair.user()
        .process_at(
            Frame::new(transcript("accepted")),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    pair.assistant()
        .process_at(
            Frame::new(FrameData::LlmResponseEnd),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    assert_eq!(history(&pair), [(Role::User, "accepted".into())]);
}
