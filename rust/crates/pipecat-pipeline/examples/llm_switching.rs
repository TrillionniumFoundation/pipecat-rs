//! Credential-free failover with two real loopback HTTP/SSE LLM services.
use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, LlmContextFrame, Pipeline, PipelineConfig,
    ProcessorUnusablePolicy,
};
use pipecat_pipeline::{LlmSwitcher, ServiceSwitcherStrategy};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn provider(
    name: &str,
    fail: bool,
) -> Result<(Arc<LlmProcessor>, tokio::task::JoinHandle<()>), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut config = ClientConfig::new(Provider::OpenAi, "");
    config.api_key = None;
    config.endpoint = format!("http://{}/completion", listener.local_addr()?);
    config.request_timeout = Duration::from_secs(3);
    let service = Arc::new(
        LlmProcessor::new(
            LlmClient::new(config)?,
            CompletionRequest::new(
                name,
                Context::new(vec![Message::text(Role::User, "initial")]),
            ),
        )
        .with_name(name),
    );
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0; 4096];
        let header_end = loop {
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buffer[..n]);
            if let Some(end) = request.windows(4).position(|x| x == b"\r\n\r\n") {
                break end + 4;
            }
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let length: usize = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|value| value.trim().parse().unwrap())
            })
            .unwrap();
        while request.len() < header_end + length {
            let n = socket.read(&mut buffer).await.unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buffer[..n]);
        }
        let (status, kind, body) = if fail {
            (
                401,
                "application/json",
                json!({"error":{"message":"simulated authentication failure"}}).to_string(),
            )
        } else {
            (
                200,
                "text/event-stream",
                format!(
                    "data: {}\n\ndata: [DONE]\n\n",
                    json!({"choices":[{"delta":{"content":"The backup handled your fresh request."},"finish_reason":"stop"}]})
                ),
            )
        };
        socket.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    });
    Ok((service, task))
}
fn prompt(text: &str) -> Frame {
    Frame::new(FrameData::LlmContext(LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, text)],
        ..Default::default()
    }))
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (primary, primary_server) = provider("primary", true).await?;
    let (backup, backup_server) = provider("backup", false).await?;
    let switcher = Arc::new(LlmSwitcher::with_strategy(
        vec![primary, backup],
        ServiceSwitcherStrategy::Failover,
    )?);
    let mut worker = Pipeline::new(vec![switcher.clone()])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..Default::default()
        })
        .await?;
    worker
        .sender()
        .send(prompt("This attempt will fail."), Direction::Downstream)
        .await?;
    while let Some(output) = worker.recv().await {
        if matches!(&output.frame.data, FrameData::LlmServiceMetadata(m) if m.base.service_name == "backup")
        {
            println!("Primary failed; backup selected. The failed request was not replayed.");
            break;
        }
    }
    worker
        .sender()
        .send(
            prompt("A new explicitly requested turn."),
            Direction::Downstream,
        )
        .await?;
    while let Some(output) = worker.recv().await {
        match output.frame.data {
            FrameData::LlmText(text) => println!("{}", text.text),
            FrameData::LlmResponseEnd => break,
            _ => {}
        }
    }
    worker.sender().end().await?;
    let (_, report) = worker.collect().await?;
    primary_server.await?;
    backup_server.await?;
    assert!(report.errors.is_empty());
    println!("Both native services and their owned work are cleaned up.");
    Ok(())
}
