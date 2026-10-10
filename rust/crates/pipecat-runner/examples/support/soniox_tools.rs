//! Pure local arithmetic; every connection owns a new executor and counter.
use pipecat_core::ProcessorError;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_runner::SonioxSessionTools;
use pipecat_services::ToolDefinition;
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub fn tools() -> Result<SonioxSessionTools, ProcessorError> {
    let mut executor = ToolExecutor::new(2, 64, 4096)?;
    let calls = Arc::new(AtomicUsize::new(0));
    executor.register(
        "add",
        RegisteredTool::new(Arc::new(move |call| {
            let calls = calls.clone();
            Box::pin(async move {
                let a = call
                    .arguments
                    .get("a")
                    .and_then(|value| value.as_i64())
                    .ok_or("a must be an integer")?;
                let b = call
                    .arguments
                    .get("b")
                    .and_then(|value| value.as_i64())
                    .ok_or("b must be an integer")?;
                let sum = a.checked_add(b).ok_or("integer overflow")?;
                Ok(json!({"sum":sum,"session_invocation":calls.fetch_add(1, Ordering::SeqCst) + 1}))
            })
        })),
    )?;
    Ok(SonioxSessionTools {
        definitions: vec![ToolDefinition {
            name: "add".into(),
            description: "Add two integers using native Rust".into(),
            parameters: json!({"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"],"additionalProperties":false}),
        }],
        executor,
    })
}
