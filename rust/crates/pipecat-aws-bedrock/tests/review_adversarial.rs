mod support;
use pipecat_aws_bedrock::*;
use pipecat_core::*;
use pipecat_services::{Content, ContentPart, Message, Role};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;

#[tokio::test]
async fn blocked_or_malformed_completion_never_admits_executable_tool_calls() {
    for reason in [
        "guardrail_intervened",
        "content_filtered",
        "malformed_tool_use",
        "malformed_model_output",
        "max_tokens",
        "unknown_future_reason",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BedrockClient::new(support::config(&listener)).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            let bytes = [
                support::record("messageStart",json!({"role":"assistant"})),
                support::record("contentBlockStart",json!({"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"side-effect","name":"delete_record"}}})),
                support::record("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"toolUse":{"input":"{\"id\":7}"}}})),
                support::record("contentBlockStop",json!({"contentBlockIndex":0})),
                support::record("messageStop",json!({"stopReason":reason})),
                support::record("metadata",json!({"usage":{"inputTokens":1,"outputTokens":1,"totalTokens":2}})),
            ].concat();
            support::respond(&mut socket, "application/vnd.amazon.eventstream", &bytes, 7).await;
        });
        let mut request = support::request();
        request.context.tools = Some(vec![pipecat_services::ToolDefinition {
            name: "delete_record".into(),
            description: "side effect".into(),
            parameters: json!({"type":"object"}),
        }]);
        let processor = Arc::new(BedrockProcessor::new(client, request));
        let mut worker = Pipeline::new(vec![processor])
            .start(Default::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
        worker.end().await.unwrap();
        let frames = tokio::time::timeout(Duration::from_secs(5), async {
            let mut frames = vec![];
            while let Some(frame) = worker.recv().await {
                frames.push(frame);
            }
            worker.join().await.unwrap();
            frames
        })
        .await
        .unwrap();
        server.await.unwrap();
        assert!(
            !frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::FunctionCallsStarted(_))),
            "stop reason {reason} admitted an executable tool"
        );
    }
}

#[test]
fn prepared_request_debug_does_not_reveal_native_signatures_or_redacted_bytes() {
    let client =
        BedrockClient::new(BedrockConfig::new(support::credentials(), "us-east-1")).unwrap();
    let mut request = support::request();
    let mut signed = Message::text(Role::Assistant, "");
    signed.content = Some(Content::Parts(vec![
        ContentPart::BedrockReasoning {
            text: "private-reasoning".into(),
            signature: Some("private-signature-marker".into()),
        },
        ContentPart::BedrockRedacted {
            data: b"private-redacted-marker".to_vec(),
        },
    ]));
    request.context.messages.push(signed);
    request
        .context
        .messages
        .push(Message::text(Role::User, "continue"));
    let debug = format!("{:?}", client.prepare(&request, true).unwrap());
    assert!(
        !debug.contains("private-signature-marker"),
        "PreparedRequest leaks the native reasoning signature"
    );
    assert!(!debug.contains("private-reasoning"));
    assert!(!debug.contains("cHJpdmF0ZS1yZWRhY3RlZC1tYXJrZXI="));
}
