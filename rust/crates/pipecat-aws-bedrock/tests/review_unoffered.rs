mod support;
use pipecat_aws_bedrock::*;
use pipecat_core::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;

#[tokio::test]
async fn unoffered_tool_cannot_reach_a_broader_executor_registry() {
    let reason = "tool_use";
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
        name: "read_record".into(),
        description: "read only".into(),
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
