//! Offline YAML graph with confined includes and a registered native Rust tool.
//! Add a LlmProcessor before FlowProcessor to consume real provider calls.
use pipecat_core::{FrameProcessor, FunctionCall, Pipeline, ToolDefinition};
use pipecat_flows::{
    Flow, FlowActionBoundary, FlowConfig, FlowIncludePolicy, FlowLoadOptions, FlowManager,
    FlowProcessor, HandlerRegistry, PipelineFlowSink, ToolHandler, ToolResult,
};
use pipecat_services::Context;
use pipecat_turns::LLMContextAggregatorPair;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Order {
    item: String,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/yaml");
    let config = FlowConfig::from_file_with_options(
        root.join("flow.yaml"),
        &FlowLoadOptions {
            includes: FlowIncludePolicy::Local { root },
            ..Default::default()
        },
    )?;
    let mut handlers = HandlerRegistry::new();
    handlers.register_tool("save_order", ToolHandler::typed(
        ToolDefinition { name:"save_order".into(), description:"Store the caller's chosen item".into(), parameters:json!({"type":"object","properties":{"item":{"type":"string"}},"required":["item"],"additionalProperties":false}) },
        |order: Order, handle| async move {
            handle.set_state("item", json!(order.item))?;
            Ok(ToolResult::configured(json!({"saved":true})))
        },
    ))?;
    let pair =
        LLMContextAggregatorPair::new(Context::default(), Default::default(), Default::default());
    let sink = Arc::new(PipelineFlowSink::new());
    let manager = FlowManager::new(Flow::new(config, handlers)?, sink.clone(), pair.context());
    let processors: Vec<Arc<dyn FrameProcessor>> = vec![
        pair.user(),
        Arc::new(FlowProcessor::new(manager.clone())),
        pair.assistant(),
        Arc::new(FlowActionBoundary::new(manager.clone())),
    ];
    let worker = Pipeline::new(processors).start(Default::default()).await?;
    sink.bind(worker.sender())?;
    manager.initialize().await?;
    let result = manager
        .execute_tools(vec![FunctionCall {
            function_name: "save_order".into(),
            tool_call_id: "example-order-1".into(),
            arguments: json!({"item":"vegetarian pizza"}),
        }])
        .await?;
    println!("Current node: {:?}", result.transitioned_to);
    println!(
        "Context: {}",
        serde_json::to_string_pretty(&pair.context().snapshot()?)?
    );
    worker.end().await?;
    let (_, report) = worker.collect().await?;
    if !report.errors.is_empty() {
        return Err(format!("pipeline errors: {:?}", report.errors).into());
    }
    Ok(())
}
