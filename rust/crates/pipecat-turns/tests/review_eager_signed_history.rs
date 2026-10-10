//! Signed continuation and heard-history integration through an eager output gate.
use pipecat_core::*;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator, SharedContext,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
struct ResponseEndWitness(Arc<tokio::sync::Notify>);
#[async_trait]
impl FrameProcessor for ResponseEndWitness {
    fn name(&self) -> &str {
        "ReviewResponseEndWitness"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream && matches!(frame.data, FrameData::LlmResponseEnd) {
            self.0.notify_one();
        }
        ctx.push(frame, direction).await
    }
}
async fn request(socket: &mut TcpStream) {
    let mut bytes = vec![];
    let mut buffer = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(at) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let len: usize = String::from_utf8_lossy(&bytes[..end])
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + len {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
}
#[tokio::test]
async fn review_eager_cancel_discards_signature_and_confirm_preserves_only_heard_history() {
    for confirm in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = ClientConfig::new(Provider::Anthropic, "synthetic");
        config.endpoint = format!("http://{}/messages", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            request(&mut socket).await;
            let events: Vec<Value> = vec![
                json!({"type":"message_start","message":{}}),
                json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"closed 思考"}}),
                json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque+/=="}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"never audibly played"}}),
                json!({"type":"content_block_stop","index":1}),
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
                json!({"type":"message_stop"}),
            ];
            let body: String = events
                .iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let llm = Arc::new(LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            CompletionRequest::new("fixture", Context::default()),
        ));
        let shared = SharedContext::default();
        let assistant = Arc::new(LLMAssistantAggregator::new(
            shared.clone(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Spoken,
            },
        ));
        let response_ended = Arc::new(tokio::sync::Notify::new());
        let worker = Pipeline::new(vec![
            llm.clone(),
            Arc::new(ResponseEndWitness(response_ended.clone())),
            assistant,
        ])
        .start(Default::default())
        .await
        .unwrap();
        let owner = SpeculationOwner::new();
        let mut inference = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: LlmContextData {
                messages: vec![ChatMessage::new(ChatRole::User, "provisional").into()],
                tools: ToolSet::NotGiven,
                tool_choice: None,
            },
            speculation: true,
        }));
        inference
            .set_native_speculation(owner.capability())
            .unwrap();
        worker
            .sender()
            .send(inference, Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = llm.speculation_snapshot().await;
                assert_ne!(
                    snapshot.status,
                    Some(SpeculationStatus::Cancelled),
                    "valid signed checkpoint was rejected by the output buffer: {snapshot:?}"
                );
                if snapshot.buffered_frames >= 7 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            shared.snapshot().unwrap().messages.is_empty(),
            "buffered signature or text reached live history"
        );
        let mut verdict = Frame::new(if confirm {
            assert!(owner.confirm());
            FrameData::UserStoppedSpeaking
        } else {
            owner.cancel();
            FrameData::EagerEndOfTurnCancel
        });
        verdict.set_native_speculation(owner.capability()).unwrap();
        worker
            .sender()
            .send(verdict, Direction::Downstream)
            .await
            .unwrap();
        if confirm {
            // The assistant legitimately consumes this semantic boundary;
            // witness release before the assistant rather than at worker output.
            tokio::time::timeout(Duration::from_secs(2), response_ended.notified())
                .await
                .unwrap();
        }
        worker.end().await.unwrap();
        let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let history = pipecat_services::adapters::request_body(
            Provider::Anthropic,
            &CompletionRequest::new("fixture", shared.snapshot().unwrap()),
        )
        .unwrap();
        if confirm {
            assert_eq!(
                history["messages"][0]["content"],
                json!([{"type":"thinking","thinking":"closed 思考","signature":"opaque+/=="}])
            );
            assert!(!history.to_string().contains("never audibly played"));
        } else {
            assert!(shared.snapshot().unwrap().messages.is_empty());
        }
        server.await.unwrap();
    }
}
