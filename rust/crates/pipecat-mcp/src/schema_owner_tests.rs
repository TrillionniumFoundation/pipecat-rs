use super::*;
use pipecat_core::{
    Direction, Frame, FrameData, Pipeline, PipelineConfig, PipelineWorker, ToolSet,
};
use pipecat_pipeline::{DirectToolBinding, DirectToolBindingConfig};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use std::time::Duration;

async fn boundary(worker: &mut PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    let marker = Frame::text("schema ownership boundary");
    let id = marker.id;
    worker
        .sender()
        .send(marker, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(output) = worker.recv().await {
            if output.frame.id == id {
                return;
            }
        }
        panic!("pipeline ended before schema ownership boundary");
    })
    .await
    .expect("schema ownership boundary stalled");
}

#[tokio::test]
async fn pruned_schema_cleanup_retains_resource_until_teardown_without_a_cycle() {
    let server = test_support::Server::start("").await;
    let config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    let client = McpClient::new(config.clone()).unwrap();
    let other_client = McpClient::new(config).unwrap();
    assert_ne!(client.schema_cleanup, other_client.schema_cleanup);
    assert_eq!(client.schema_cleanup, client.clone().schema_cleanup);
    drop(other_client);
    let weak = Arc::downgrade(&client.inner);
    let tools = client.tools().await.unwrap();
    assert!(tools.definitions().len() > 1);
    let mut provider = ClientConfig::new(Provider::OpenAi, "ownership-fixture");
    provider.endpoint = server.url.clone();
    provider.api_key = None;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(provider).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    // One resource hook serves every schema handler, even with a strict owner limit.
    let binding = DirectToolBinding::with_config(
        llm,
        DirectToolBindingConfig {
            max_cleanup_hooks: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    boundary(
        &mut worker,
        Frame::new(FrameData::LlmSetTools(ToolSet::Schema(tools.schema()))),
    )
    .await;
    boundary(
        &mut worker,
        Frame::new(FrameData::LlmSetTools(ToolSet::NotGiven)),
    )
    .await;
    drop(tools);
    drop(client);
    {
        let retained = weak
            .upgrade()
            .expect("pruning lost the resource cleanup owner");
        let state = retained.state.lock().await;
        assert!(state.requested);
        assert!(state.active.is_some());
    }
    assert_eq!(server.state.lock().unwrap().deletes, 0);
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.wait_for(|state| state.deletes == 1).await;
    assert!(
        weak.upgrade().is_none(),
        "cleanup hook retained an ownership cycle"
    );
    assert!(binding.outcomes().snapshot().active.is_empty());
    let state = server.state.lock().unwrap();
    assert_eq!(state.initializes, 1);
    assert_eq!(state.deletes, 1);
    assert!(state.calls.is_empty());
    assert!(state.completions.is_empty());
}
