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
        println!("Native context summary example\n\nSet PIPECAT_SUMMARY_MODEL and run:\n  cargo run -p pipecat-turns --example context_summary -- --run [--dedicated] [--auto]\n\nPIPECAT_SUMMARY_PROVIDER: ollama (default), openai, anthropic, gemini\nPIPECAT_SUMMARY_ENDPOINT: optional exact HTTP(S) POST endpoint\nPIPECAT_SUMMARY_API_KEY: required for remote providers\n\nWithout --run, no request is sent. --dedicated runs the same native client from the assistant; otherwise a summary responder handles request/result frames. --auto uses the message threshold on response start instead of a manual frame.");
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
    let mut worker = Pipeline::new(processors)
        .start(PipelineConfig::default())
        .await?;
    while let Some(out) = worker.recv().await {
        if matches!(out.frame.data, FrameData::Start(_)) {
            break;
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
    let outcome=tokio::time::timeout(Duration::from_secs(35),async {
        loop {
            if context.snapshot()?.messages.len()==4 {return Ok::<_,Box<dyn Error>>(());}
            tokio::select! {
                _=tokio::time::sleep(Duration::from_millis(10))=>{},
                out=worker.recv()=>match out {
                    Some(out)=>if let FrameData::Error(error)=out.frame.data {return Err(error.error.into());},
                    None=>return Err("pipeline closed before summary completion".into()),
                }
            }
        }
    }).await;
    match outcome {
        Ok(Ok(())) => {
            println!("{}", serde_json::to_string_pretty(&context.snapshot()?)?);
            worker.end().await?;
            worker.join().await?;
            Ok(())
        }
        error => {
            worker.cancel("summary example failed").await?;
            worker.join().await?;
            match error {
                Ok(Err(error)) => Err(error),
                Err(_) => Err("summary deadline exceeded".into()),
                _ => unreachable!(),
            }
        }
    }
}
