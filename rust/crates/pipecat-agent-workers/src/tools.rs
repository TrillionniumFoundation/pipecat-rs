use crate::AgentError;
use pipecat_core::{Direction, Frame, FrameData, FunctionCallResultFrame, PipelineSender};
use pipecat_services::{CancellationToken, ToolCall, ToolDefinition};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

type ToolFuture = Pin<Box<dyn Future<Output = Result<ToolResult, AgentError>> + Send>>;

/// Rust's explicit replacement for decorated tool metadata.
#[derive(Clone, Debug)]
pub struct ToolOptions {
    pub cancel_on_interruption: bool,
    pub timeout: Option<Duration>,
}
impl Default for ToolOptions {
    fn default() -> Self {
        Self {
            cancel_on_interruption: true,
            timeout: Some(Duration::from_secs(30)),
        }
    }
}
#[derive(Clone, Debug)]
pub struct ToolResult {
    pub result: Value,
    pub run_llm: bool,
    /// End after all tools in the round settle and their results traverse output.
    pub end: bool,
}
impl ToolResult {
    pub fn new(result: Value) -> Self {
        Self {
            result,
            run_llm: true,
            end: false,
        }
    }
    pub fn silent(result: Value) -> Self {
        Self {
            result,
            run_llm: false,
            end: false,
        }
    }
}
#[derive(Clone)]
pub struct ToolHandler {
    pub schema: ToolDefinition,
    pub options: ToolOptions,
    handler: Arc<dyn Fn(Value, ToolContext) -> ToolFuture + Send + Sync>,
}
impl ToolHandler {
    pub fn new<F, Fut>(schema: ToolDefinition, handler: F) -> Self
    where
        F: Fn(Value, ToolContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolResult, AgentError>> + Send + 'static,
    {
        Self {
            schema,
            options: ToolOptions::default(),
            handler: Arc::new(move |args, ctx| Box::pin(handler(args, ctx))),
        }
    }
    pub fn typed<T, F, Fut>(schema: ToolDefinition, handler: F) -> Self
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(T, ToolContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolResult, AgentError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        Self::new(schema, move |args, ctx| {
            let handler = handler.clone();
            async move {
                let args = serde_json::from_value(args)
                    .map_err(|e| AgentError::Failed(format!("invalid tool arguments: {e}")))?;
                handler(args, ctx).await
            }
        })
    }
    pub fn with_options(mut self, options: ToolOptions) -> Self {
        self.options = options;
        self
    }
    pub(crate) async fn call(
        &self,
        args: Value,
        ctx: ToolContext,
    ) -> Result<ToolResult, AgentError> {
        (self.handler)(args, ctx).await
    }
}
#[derive(Clone, Default)]
pub struct ToolRegistry {
    pub(crate) tools: BTreeMap<String, ToolHandler>,
}
impl ToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register(&mut self, handler: ToolHandler) -> Result<(), AgentError> {
        if handler.schema.name.is_empty()
            || !handler.schema.parameters.is_object()
            || handler.options.timeout.is_some_and(|d| {
                d.is_zero() || tokio::time::Instant::now().checked_add(d).is_none()
            })
        {
            return Err(AgentError::Config(
                "tool requires a name, object schema and valid positive timeout".into(),
            ));
        }
        if self.tools.contains_key(&handler.schema.name) {
            return Err(AgentError::Config(format!(
                "duplicate tool: {}",
                handler.schema.name
            )));
        }
        self.tools.insert(handler.schema.name.clone(), handler);
        Ok(())
    }
    pub fn schemas(&self) -> Vec<ToolDefinition> {
        self.tools.values().map(|h| h.schema.clone()).collect()
    }
}
/// Frames queued through this invocation are held until every tool in its batch
/// settles. External worker traffic does not enter this invocation-owned buffer.
#[derive(Clone)]
pub struct ToolContext {
    pub worker: String,
    pub call: ToolCall,
    pub(crate) output: PipelineSender,
    pub(crate) cancelled: CancellationToken,
    pub(crate) deferred: Arc<Mutex<Vec<(Frame, Direction)>>>,
    pub(crate) max_deferred: usize,
    pub(crate) generation: u64,
    pub(crate) full_generation: u64,
    pub(crate) survives_interruption: bool,
}
impl ToolContext {
    pub fn queue_frame(&self, frame: Frame, direction: Direction) -> Result<(), AgentError> {
        if self.cancelled.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
        let mut frames = self
            .deferred
            .lock()
            .map_err(|_| AgentError::Failed("tool frame lock poisoned".into()))?;
        if frames.len() >= self.max_deferred {
            return Err(AgentError::Capacity("deferred tool frames".into()));
        }
        frames.push((frame, direction));
        Ok(())
    }
    pub async fn update(&self, result: Value) -> Result<(), AgentError> {
        let mut frame = Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: self.call.name.clone(),
            tool_call_id: self.call.id.clone(),
            arguments: self.call.arguments.clone(),
            result,
            run_llm: Some(false),
            is_final: false,
            error: None,
        }));
        frame
            .metadata
            .insert(crate::GENERATION.into(), self.generation.into());
        frame
            .metadata
            .insert(crate::FULL_GENERATION.into(), self.full_generation.into());
        frame.metadata.insert(
            crate::SURVIVES_INTERRUPTION.into(),
            self.survives_interruption.into(),
        );
        tokio::select! { biased; _=self.cancelled.cancelled()=>Err(AgentError::Cancelled), result=self.output.send(frame,Direction::Downstream)=>result.map_err(Into::into) }
    }
    /// Resolves when this invocation settles or is stopped, including batch aborts.
    pub async fn cancelled(&self) {
        self.cancelled.cancelled().await;
    }
}
