//! Requires the native speculation capability API integrated with the eager gate.
mod support;
use pipecat_aws_bedrock::*;
use pipecat_core::*;
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;
#[tokio::test]
async fn bare_and_capability_bearing_contexts_are_rejected_before_http_admission() {
    for variant in 0..5 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let processor = Arc::new(BedrockProcessor::new(
            BedrockClient::new(support::config(&listener)).unwrap(),
            support::request(),
        ));
        let legacy = LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "speculative")],
            tools: vec![],
            tool_choice: None,
        };
        let data = match variant {
            0 => FrameData::LlmRun,
            1 => FrameData::LlmContext(legacy),
            _ => FrameData::LlmContextDetailed(DetailedLlmContextFrame {
                context: legacy.into(),
                speculation: variant >= 3,
            }),
        };
        let mut frame = Frame::new(data);
        let owner = SpeculationOwner::new();
        if variant != 4 {
            frame.set_native_speculation(owner.capability()).unwrap();
        }
        let mut worker = Pipeline::new(vec![processor])
            .start(Default::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        worker.end().await.unwrap();
        let mut rejected = false;
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(output) = worker.recv().await {
                rejected |=
                    matches!(&output.frame.data,FrameData::Error(e) if e.error.contains("specul"));
                assert!(!matches!(
                    &output.frame.data,
                    FrameData::LlmResponseStart
                        | FrameData::FunctionCallsStarted(_)
                        | FrameData::LlmText(_)
                ));
            }
            worker.join().await.unwrap();
        })
        .await
        .unwrap();
        assert!(rejected, "variant {variant} was not rejected");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err(),
            "speculation must not open an HTTP connection"
        );
    }
}
