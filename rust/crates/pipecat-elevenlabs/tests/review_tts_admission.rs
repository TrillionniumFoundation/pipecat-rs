//! Over-token prepared text is rejected before any provider connection.
use pipecat_core::*;
use pipecat_elevenlabs::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;

#[tokio::test]
async fn exact_repetitive_over_token_input_fails_before_provider_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("loopback", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let worker = Pipeline::new(vec![Arc::new(ElevenLabsTtsProcessor::new(
        ElevenLabsTtsClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();

    // 4,100 bytes, but 4,101 alternating whitespace-run tokens including the
    // trailing sentinel. The default alignment tokenizer permits only 4,096.
    let text = "a b ".repeat(1025);
    assert_eq!(text.len(), 4100);
    let response_end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = response_end.id;
    for frame in [
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new(text))),
        response_end,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .expect("rejected admission must release the response barrier")
        .unwrap();

    // Inspect admission before waiting for any network event. A rejected input
    // has no provider server task whose accept or join could wait indefinitely.
    assert!(
        report
            .errors
            .iter()
            .any(|error| error.category == ErrorCategory::Application),
        "expected an alignment admission error: {:?}",
        report.errors
    );
    assert!(!frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::TtsStarted(_)
            | FrameData::TtsAudio(_)
            | FrameData::TtsText(_)
            | FrameData::AggregatedText(_)
            | FrameData::AggregatedTextProgress(_)
            | FrameData::TtsAlignmentDiagnostic(_)
    )));
    let ends: Vec<_> = frames
        .iter()
        .filter(|frame| frame.direction == Direction::Downstream && frame.frame.id == end_id)
        .collect();
    assert_eq!(ends.len(), 1);
    assert_eq!(ends[0].frame.metadata.get("failed"), Some(&json!(true)));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err(),
        "invalid text must not open a provider socket"
    );
}
