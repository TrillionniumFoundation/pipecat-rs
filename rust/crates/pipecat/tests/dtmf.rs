//! Telephony wire input through native DTMF and shared user aggregation.
use pipecat_core::*;
use pipecat_processors::{DtmfAggregator, DtmfAggregatorConfig};
use pipecat_protocol::{
    adapters::{FrameSerializer, TelephonyFrameSerializer},
    telephony::TelephonyCodec,
    WireMessage,
};
use pipecat_services::{Content, Role};
use pipecat_turns::{LLMUserAggregator, SharedContext, UserAggregatorConfig, UserTurnConfig};
use serde_json::json;
use std::{sync::Arc, time::Duration};

async fn context_after_wire_keys(
    config: DtmfAggregatorConfig,
    user_config: UserAggregatorConfig,
    keys: &[&str],
) -> (SharedContext, Vec<DirectedFrame>) {
    let shared = SharedContext::default();
    let user = Arc::new(LLMUserAggregator::new(shared.clone(), user_config));
    let mut worker = Pipeline::new(vec![Arc::new(DtmfAggregator::new(config).unwrap()), user])
        .start(Default::default())
        .await
        .unwrap();
    assert!(worker.recv().await.unwrap().frame.data.is_start());
    let mut serializer =
        TelephonyFrameSerializer::new(TelephonyCodec::twilio("native-fixture").unwrap(), 8000)
            .unwrap();
    for key in keys {
        let wire = WireMessage::Text(
            json!({"event":"dtmf","streamSid":"native-fixture","dtmf":{"digit":key}}).to_string(),
        );
        let frame = serializer
            .deserialize(&wire)
            .unwrap()
            .expect("decoded keypad frame");
        assert!(matches!(frame.data, FrameData::InputDtmf(_)));
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    let mut output = vec![];
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let next = worker.recv().await.expect("context output");
            let done = matches!(next.frame.data, FrameData::LlmContext(_));
            output.push(next);
            if done {
                break;
            }
        }
    })
    .await
    .expect("wire keypad must create a native user context");
    worker.end().await.unwrap();
    let (remaining, report) = worker.collect().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty());
    output.extend(remaining);
    (shared, output)
}
#[tokio::test]
async fn telephone_codec_to_dtmf_to_default_user_context() {
    let (shared, output) = context_after_wire_keys(
        Default::default(),
        Default::default(),
        &["0", "0", "1", "#"],
    )
    .await;
    let context = shared.snapshot().unwrap();
    assert_eq!(context.messages.len(), 1);
    assert_eq!(context.messages[0].role, Role::User);
    assert_eq!(
        context.messages[0].content,
        Some(Content::Text("DTMF: 001#".into()))
    );
    assert_eq!(
        output
            .iter()
            .filter(|frame| matches!(frame.frame.data, FrameData::InputDtmf(_)))
            .count(),
        4
    );
    assert_eq!(
        output
            .iter()
            .filter(|frame| matches!(frame.frame.data, FrameData::LlmContext(_)))
            .count(),
        1
    );
}
#[tokio::test]
async fn telephone_timeout_with_explicit_user_policy_keeps_finalized_native_input() {
    let (shared, output) = context_after_wire_keys(
        DtmfAggregatorConfig {
            idle_timeout: Duration::from_millis(20),
            prefix: "Menu: ".into(),
            ..Default::default()
        },
        UserAggregatorConfig {
            turn: UserTurnConfig {
                enable_interruptions: false,
                speech_timeout: Duration::from_millis(10),
                ..Default::default()
            },
            ..Default::default()
        },
        &["2", "2"],
    )
    .await;
    assert_eq!(
        shared.snapshot().unwrap().messages[0].content,
        Some(Content::Text("Menu: 22".into()))
    );
    // Disabling the user's turn-start policy does not suppress the source
    // DTMF processor's explicit first-key interruption.
    assert_eq!(
        output
            .iter()
            .filter(|frame| matches!(frame.frame.data, FrameData::Interruption)
                && frame.direction == Direction::Downstream)
            .count(),
        1
    );
}
