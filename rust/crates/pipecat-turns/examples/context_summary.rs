//! Native cascade compression using an explicitly configured local or remote LLM.
use pipecat_core::{
    Direction, Frame, FrameData, FrameProcessor, LlmContextSummaryConfig, Pipeline, PipelineConfig,
};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, Message, Provider, Role,
};
use pipecat_turns::{
    ContextSummarizationConfig, LLMAssistantAggregator, LLMContextSummaryProcessor, SharedContext,
    SummaryLlm,
};
use std::{error::Error, sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !args.iter().any(|arg| arg == "--run") || args.iter().any(|arg| arg == "--help") {
        println!(
            "Native context summary example\n\nSet PIPECAT_SUMMARY_MODEL and run:\n  cargo run -p pipecat-turns --example context_summary -- --run [--dedicated] [--auto]\n\nPIPECAT_SUMMARY_PROVIDER: ollama (default), openai, anthropic, gemini\nPIPECAT_SUMMARY_ENDPOINT: optional exact HTTP(S) POST endpoint\nPIPECAT_SUMMARY_API_KEY: required for remote providers\n\nWithout --run, no request is sent. --dedicated runs the same native client from the assistant; otherwise a summary responder handles request/result frames. --auto uses the message threshold on response start instead of a manual frame."
        );
        return Ok(());
    }
    let model = std::env::var("PIPECAT_SUMMARY_MODEL")
        .map_err(|_| "set PIPECAT_SUMMARY_MODEL to an installed or available model")?;
    let provider = std::env::var("PIPECAT_SUMMARY_PROVIDER").unwrap_or_else(|_| "ollama".into());
    let mut client = if provider == "ollama" {
        ClientConfig::ollama()?
    } else {
        let provider = match provider.as_str() {
            "openai" => Provider::OpenAi,
            "anthropic" => Provider::Anthropic,
            "gemini" => Provider::Gemini,
            _ => return Err("unknown PIPECAT_SUMMARY_PROVIDER".into()),
        };
        ClientConfig::new(
            provider,
            std::env::var("PIPECAT_SUMMARY_API_KEY")
                .map_err(|_| "set PIPECAT_SUMMARY_API_KEY for the chosen remote provider")?,
        )
    };
    if let Ok(endpoint) = std::env::var("PIPECAT_SUMMARY_ENDPOINT") {
        client.endpoint = endpoint;
    }
    let llm = SummaryLlm::new(
        LlmClient::new(client)?,
        CompletionRequest::new(model, Context::default()),
    );
    let context = SharedContext::new(Context::new(vec![
        Message::text(
            Role::System,
            "Be concise and preserve the user's travel requirements.",
        ),
        Message::text(
            Role::User,
            "I want to visit Paris for four days, using trains rather than flights within France.",
        ),
        Message::text(
            Role::Assistant,
            "I will keep the four-day duration and train preference in mind.",
        ),
        Message::text(Role::User, "My hotel needs to be near a Metro station."),
        Message::text(
            Role::Assistant,
            "We still need to choose dates and a hotel budget.",
        ),
    ]));
    let automatic = args.iter().any(|arg| arg == "--auto");
    let mut assistant = LLMAssistantAggregator::new(context.clone(), Default::default())
        .with_summarization(ContextSummarizationConfig {
            auto_trigger: automatic,
            max_context_tokens: None,
            max_unsummarized_messages: Some(4),
            summary: LlmContextSummaryConfig {
                min_messages_after_summary: 2,
                target_context_tokens: 256,
                summarization_timeout: 30.0,
                ..Default::default()
            },
        })?;
    let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![];
    if args.iter().any(|arg| arg == "--dedicated") {
        assistant = assistant.with_summary_llm(llm);
    } else {
        processors.push(Arc::new(LLMContextSummaryProcessor::new(llm)));
    }
    processors.push(Arc::new(assistant));
    let output = run_pipeline(processors, context, automatic, Duration::from_secs(35)).await?;
    println!("{output}");
    Ok(())
}

// Keep every fallible observation inside this scope; join before returning it.
async fn run_pipeline(
    processors: Vec<Arc<dyn FrameProcessor>>,
    context: SharedContext,
    automatic: bool,
    deadline: Duration,
) -> Result<String, Box<dyn Error>> {
    let mut worker = Pipeline::new(processors)
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .await?;
    let outcome = tokio::time::timeout(deadline, async {
        loop {
            let out = worker.recv().await.ok_or("pipeline closed before Start")?;
            if out.frame.data.is_start() {
                break;
            }
            match out.frame.data {
                FrameData::Error(error) => return Err(error.error.into()),
                FrameData::ErrorDetailed(error) => return Err(error.error.into()),
                _ => {}
            }
        }
        let trigger = if automatic {
            FrameData::LlmResponseStart
        } else {
            FrameData::LlmSummarizeContext(None)
        };
        worker
            .sender()
            .send(Frame::new(trigger), Direction::Downstream)
            .await?;
        if automatic {
            worker
                .sender()
                .send(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
                .await?;
        }
        loop {
            if context.snapshot()?.messages.len() == 4 {
                return Ok::<_, Box<dyn Error>>(serde_json::to_string_pretty(
                    &context.snapshot()?,
                )?);
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                out = worker.recv() => match out {
                    Some(out) => match out.frame.data {
                        FrameData::Error(error) => return Err(error.error.into()),
                        FrameData::ErrorDetailed(error) => return Err(error.error.into()),
                        _ => {},
                    },
                    None => return Err("pipeline closed before summary completion".into()),
                }
            }
        }
    })
    .await;
    let mut errors = Vec::new();
    let output = match outcome {
        Ok(Ok(output)) => Some(output),
        Ok(Err(error)) => {
            errors.push(error.to_string());
            None
        }
        Err(error) => {
            errors.push(format!("summary observation deadline: {error}"));
            None
        }
    };
    if !worker.state().is_finished() {
        let terminal = tokio::time::timeout(Duration::from_secs(3), async {
            if output.is_some() {
                worker.end().await
            } else {
                worker.cancel("summary example failed").await
            }
        })
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result.map_err(|error| error.to_string()));
        if let Err(error) = terminal {
            errors.push(format!("terminal admission: {error}"));
            let _ = worker.cancel("terminal admission failed").await;
        }
    }
    let joined = match tokio::time::timeout(Duration::from_secs(3), worker.join()).await {
        Ok(result) => result,
        Err(error) => {
            errors.push(format!("join deadline: {error}"));
            let _ = worker.cancel("summary join deadline").await;
            worker.join().await
        }
    };
    match joined {
        Ok(report) => {
            if report.total_errors != 0 || !report.errors.is_empty() {
                errors.push(format!("worker errors: {:?}", report.errors));
            }
            let wanted = if output.is_some() {
                pipecat_core::WorkerState::Ended
            } else {
                pipecat_core::WorkerState::Cancelled
            };
            if report.state != wanted {
                errors.push(format!("unexpected final state: {:?}", report.state));
            }
        }
        Err(error) => errors.push(format!("worker join: {error}")),
    }
    if !errors.is_empty() {
        return Err(errors.join("; ").into());
    }
    output.ok_or_else(|| "summary produced no output".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::{ProcessorContext, ProcessorError, async_trait};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    struct CleanupWitness(Arc<AtomicUsize>);
    #[async_trait]
    impl FrameProcessor for CleanupWitness {
        fn name(&self) -> &str {
            "summary-example-cleanup-witness"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            ctx.push(frame, direction).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    #[derive(Clone, Copy, Debug)]
    enum Reply {
        Good,
        HttpError,
        Streaming,
    }

    async fn case(dedicated: bool, reply: Reply) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = ClientConfig::new(Provider::OpenAi, "");
        client.api_key = None;
        client.endpoint = format!("http://{}/summary", listener.local_addr().unwrap());
        let mut server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut chunk = [0; 2048];
            let end = loop {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                assert!(bytes.len() <= 65536);
                if let Some(p) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    break p + 4;
                }
            };
            let length: usize = std::str::from_utf8(&bytes[..end])
                .unwrap()
                .lines()
                .find_map(|s| {
                    s.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse().unwrap())
                })
                .unwrap();
            assert!(length <= 65536 - end);
            while bytes.len() < end + length {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let request: serde_json::Value =
                serde_json::from_slice(&bytes[end..end + length]).unwrap();
            assert!(request["messages"].is_array());
            match reply {
                Reply::Good => {
                    let body = format!(
                        "data: {}\n\ndata: [DONE]\n\n",
                        serde_json::json!({"choices":[{"delta":{"content":"Preserved travel summary."},"finish_reason":"stop"}]})
                    );
                    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                }
                Reply::HttpError => {
                    socket.write_all(b"HTTP/1.1 500 Error\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
                }
                Reply::Streaming => {
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await.unwrap();
                    socket.flush().await.unwrap();
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert_eq!(n, 0, "cancel must close the in-flight provider socket");
                }
            }
        });
        let context = SharedContext::new(Context::new(vec![
            Message::text(Role::System, "system"),
            Message::text(Role::User, "one"),
            Message::text(Role::Assistant, "two"),
            Message::text(Role::User, "three"),
            Message::text(Role::Assistant, "four"),
        ]));
        let llm = SummaryLlm::new(
            LlmClient::new(client).unwrap(),
            CompletionRequest::new("fixture", Context::default()),
        );
        let mut assistant = LLMAssistantAggregator::new(context.clone(), Default::default())
            .with_summarization(ContextSummarizationConfig {
                auto_trigger: false,
                max_context_tokens: None,
                max_unsummarized_messages: Some(4),
                summary: LlmContextSummaryConfig {
                    min_messages_after_summary: 2,
                    target_context_tokens: 256,
                    summarization_timeout: 5.0,
                    ..Default::default()
                },
            })
            .unwrap();
        let mut processors: Vec<Arc<dyn FrameProcessor>> = Vec::new();
        if dedicated {
            assistant = assistant.with_summary_llm(llm);
        } else {
            processors.push(Arc::new(LLMContextSummaryProcessor::new(llm)));
        }
        processors.push(Arc::new(assistant));
        let cleaned = Arc::new(AtomicUsize::new(0));
        processors.push(Arc::new(CleanupWitness(cleaned.clone())));
        let deadline = if matches!(reply, Reply::Streaming) {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(3)
        };
        let outcome = run_pipeline(processors, context, false, deadline).await;
        // No assertion may skip the fixture's owner cleanup.
        let server_result = match tokio::time::timeout(Duration::from_secs(3), &mut server).await {
            Ok(result) => result,
            Err(error) => {
                server.abort();
                let _ = server.await;
                panic!("server did not terminate: {error}");
            }
        };
        server_result.unwrap();
        assert_eq!(cleaned.load(Ordering::SeqCst), 1);
        match reply {
            Reply::Good => assert!(outcome.is_ok(), "{outcome:?}"),
            Reply::Streaming => {
                let error = outcome.unwrap_err().to_string();
                assert!(
                    error.starts_with("summary observation deadline:"),
                    "{error}"
                );
                assert!(
                    !error.contains(';'),
                    "unexpected additional cleanup error: {error}"
                );
            }
            Reply::HttpError => {
                let error = outcome.unwrap_err().to_string();
                assert!(
                    error.contains("500"),
                    "expected actual HTTP failure: {error}"
                );
                for forbidden in [
                    "terminal admission:",
                    "join deadline:",
                    "worker join:",
                    "unexpected final state:",
                    "observation deadline:",
                ] {
                    assert!(
                        !error.contains(forbidden),
                        "unexpected cleanup failure: {error}"
                    );
                }
            }
        }
    }
    #[tokio::test]
    async fn responder_success_http_failure_and_streaming_deadline_join() {
        for reply in [Reply::Good, Reply::HttpError, Reply::Streaming] {
            case(false, reply).await;
        }
    }
    #[tokio::test]
    async fn dedicated_success_http_failure_and_streaming_deadline_join() {
        for reply in [Reply::Good, Reply::HttpError, Reply::Streaming] {
            case(true, reply).await;
        }
    }
}
