//! Smart Turn remains a cascade wrapper until its supervisor is integrated.
use pipecat_core::*;
use pipecat_services::Context;
use pipecat_turns::*;
use pipecat_vad::{
    MonotonicSmartTurnClock, SmartTurnAnalyzer, SmartTurnCancellation, SmartTurnParams,
    SmartTurnPrediction, SmartTurnPredictor,
};
use std::{sync::Arc, time::Duration};

struct UnusedPredictor;
impl SmartTurnPredictor for UnusedPredictor {
    fn predict(
        &mut self,
        _: &[i16],
        _: u32,
        _: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        panic!("deterministic policy control does not run the model")
    }
}
fn analyzer() -> SmartTurnAnalyzer {
    SmartTurnAnalyzer::with_predictor(
        UnusedPredictor,
        16_000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap()
}
fn pair(mode: RealtimeServiceMode, model: bool) -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                require_model_completion: model,
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
        mode,
    )
    .unwrap()
}
fn metadata(realtime: bool) -> Frame {
    Frame::new(FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "external-recommender".into(),
            user_turn_strategies: Some(UserTurnStrategies {
                start: Some(vec![UserTurnStrategy::External {
                    enable_interruptions: true,
                }]),
                stop: Some(vec![UserTurnStrategy::External {
                    enable_interruptions: true,
                }]),
            }),
        },
        is_realtime_service: realtime,
    }))
}
fn user(pair: &LLMContextAggregatorPair, data: FrameData, millis: u64) -> AggregationOutput {
    pair.user()
        .process_at(
            Frame::new(data),
            Direction::Downstream,
            Duration::from_millis(millis),
        )
        .unwrap()
}
fn assert_model_gate_retained(pair: &LLMContextAggregatorPair) {
    let started = user(
        pair,
        FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.2,
            timestamp: 0.0,
        }),
        1,
    );
    assert!(started
        .events
        .iter()
        .any(|event| matches!(event, AggregationEvent::UserTurnStarted)));
    user(
        pair,
        FrameData::Transcription(TranscriptionFrame {
            text: "model gated".into(),
            finalized: true,
            ..Default::default()
        }),
        2,
    );
    user(
        pair,
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.2,
            timestamp: 0.0,
        }),
        3,
    );
    pair.user().advance(Duration::from_secs(1)).unwrap();
    assert!(
        pair.context().snapshot().unwrap().messages.is_empty(),
        "metadata must not replace the acoustic completion gate"
    );
    let ended = user(pair, FrameData::End(Default::default()), 1001);
    assert!(ended.events.iter().any(|event| matches!(event, AggregationEvent::UserTurnStopped { text, .. } if text == "model gated")));
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
}

#[test]
fn explicit_scalar_model_gate_remains_authoritative_over_service_recommendations() {
    let pair = pair(RealtimeServiceMode::Auto, true);
    pair.user()
        .process_at(metadata(false), Direction::Upstream, Duration::ZERO)
        .unwrap();
    assert_model_gate_retained(&pair);
}

#[tokio::test]
async fn wrapped_auto_pair_rejects_both_metadata_orders_without_activation_or_policy_mutation() {
    for assistant_first in [false, true] {
        let pair = pair(RealtimeServiceMode::Auto, false);
        let wrapper = SmartTurnUserAggregator::with_aggregator(pair.user(), analyzer()).unwrap();
        for assistant in [assistant_first, !assistant_first] {
            let error = if assistant {
                pair.assistant()
                    .process_at(metadata(true), Direction::Downstream, Duration::ZERO)
                    .unwrap_err()
            } else {
                pair.user()
                    .process_at(metadata(true), Direction::Upstream, Duration::ZERO)
                    .unwrap_err()
            };
            assert!(error.message.contains("Smart Turn wrapper"));
            assert!(!pair.user().realtime_service_enabled().unwrap());
            assert!(!pair.assistant().realtime_service_enabled().unwrap());
            assert!(pair.context().snapshot().unwrap().messages.is_empty());
        }
        assert_model_gate_retained(&pair);
        wrapper.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn disabled_wrapped_pair_accepts_metadata_but_keeps_original_model_policy() {
    let pair = pair(RealtimeServiceMode::Disabled, false);
    let wrapper = SmartTurnUserAggregator::with_aggregator(pair.user(), analyzer()).unwrap();
    pair.assistant()
        .process_at(metadata(true), Direction::Downstream, Duration::ZERO)
        .unwrap();
    pair.user()
        .process_at(metadata(true), Direction::Upstream, Duration::ZERO)
        .unwrap();
    assert!(!pair.user().realtime_service_enabled().unwrap());
    assert_model_gate_retained(&pair);
    wrapper.cleanup().await.unwrap();
}

#[test]
fn enabled_pair_cannot_be_wrapped_or_reconfigured_as_smart_turn() {
    let pair = pair(RealtimeServiceMode::Enabled, false);
    let error = SmartTurnUserAggregator::with_aggregator(pair.user(), analyzer())
        .err()
        .expect("unsupported wrapper");
    assert!(error.message.contains("Smart Turn wrapper"));
    assert!(pair.user().realtime_service_enabled().unwrap());
    assert!(pair.assistant().realtime_service_enabled().unwrap());
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
}
