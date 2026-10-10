//! Run with OPENAI_API_KEY and optionally MODEL and OPENAI_ENDPOINT. The supplied
//! topic goes through an HTTP LLM, the context aggregator, and native job RPC.
use pipecat_agent_workers::AssistantJobReply;
use pipecat_core::{Direction, Frame, FrameData, Pipeline, TextFrame};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::LLMContextAggregatorPair;
use pipecat_workers::{JobClient, JobParams, JobResponse, LocalBus, NativeWorker, WorkerRegistry};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let topic = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "Give one concise argument for reusable spacecraft".into());
    let model = std::env::var("MODEL").unwrap_or_else(|_| "gpt-4.1-mini".into());
    let mut config = ClientConfig::new(Provider::OpenAi, "assistant-job");
    config.api_key = std::env::var("OPENAI_API_KEY").ok();
    if let Ok(endpoint) = std::env::var("OPENAI_ENDPOINT") {
        config.endpoint = endpoint;
    }
    let llm = LlmClient::new(config)?;
    let bus = LocalBus::new(32)?;
    let registry = WorkerRegistry::new("assistant-job-example");
    let worker = NativeWorker::new("debater").active(true).max_jobs(1)
        .on_job("debate", true, move |job| {
            let client = llm.clone();
            let model = model.clone();
            async move {
                let run = async {
                    let pair = LLMContextAggregatorPair::new(Context::default(), Default::default(), Default::default());
                    let assistant = pair.assistant();
                    let mut completions = assistant.subscribe_stopped(Default::default())?;
                    // One fresh session and immutable binding belong to this
                    // handler; a replacement job cannot adopt its late output.
                    let mut reply = AssistantJobReply::capture(&job, &assistant, 1)?;
                    let mut pipeline = Pipeline::new(vec![pair.user(), Arc::new(LlmProcessor::new(client, CompletionRequest::new(model, Context::default()))), assistant])
                        .start(Default::default()).await?;
                    let topic = job.payload.as_ref().and_then(serde_json::Value::as_str).unwrap_or("");
                    pipeline.sender().send(Frame::new(FrameData::InputText(TextFrame::new(topic))), Direction::Downstream).await?;
                    let response = loop {
                        tokio::select! {
                            completion = completions.recv() => {
                                let completion = completion.ok_or("assistant completion stream closed")?;
                                if let Some(response) = reply.try_response(&job, completion.completion())? { break response; }
                            }
                            output = pipeline.recv() => {
                                let output = output.ok_or("pipeline closed before assistant response")?;
                                if let FrameData::Error(_) = output.frame.data { return Err("pipeline error before assistant response".into()); }
                            }
                        }
                    };
                    pipeline.end().await?;
                    let (_, report) = pipeline.collect().await?;
                    if !report.errors.is_empty() { return Err("pipeline failed during shutdown".into()); }
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(response)
                };
                match tokio::time::timeout(Duration::from_secs(30), run).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(_)) => JobResponse::error("assistant job failed"),
                    Err(_) => JobResponse::error("assistant job timed out"),
                }
            }
        })?.start(bus.clone(), registry.clone())?;
    let mut job = JobClient::new("reader", bus, registry).request(
        "debater",
        JobParams {
            name: Some("debate".into()),
            payload: Some(topic.into()),
            timeout: Some(Duration::from_secs(35)),
            ..Default::default()
        },
    )?;
    let response = job.result().await?;
    println!(
        "{:?}: {}",
        response.status,
        response.response.unwrap_or_default()
    );
    worker.end(None).await?;
    worker.wait().await?;
    Ok(())
}
