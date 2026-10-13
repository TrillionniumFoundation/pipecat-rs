use crate::{McpClient, McpError};
use pipecat_agent_workers::{AgentError, ToolHandler, ToolOptions, ToolRegistry, ToolResult};
use pipecat_core::{
    async_trait, DirectFunction, Direction, Frame, FrameData, FrameProcessor, LocalToolFunction,
    ProcessorContext, ProcessorError, StandardTool, ToolDefinition, ToolsSchema,
};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use serde_json::Value;
use std::{fmt, sync::Arc};

/// A discovered immutable schema snapshot plus its session owner. Handlers
/// remain callable until explicitly closed or every owner has been dropped.
#[derive(Clone)]
pub struct McpTools {
    client: McpClient,
    definitions: Vec<ToolDefinition>,
}
impl fmt::Debug for McpTools {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpTools")
            .field("tool_count", &self.definitions.len())
            .finish_non_exhaustive()
    }
}
impl McpTools {
    pub(crate) fn new(client: McpClient, definitions: Vec<ToolDefinition>) -> Self {
        Self {
            client,
            definitions,
        }
    }
    pub fn definitions(&self) -> &[ToolDefinition] {
        &self.definitions
    }
    /// Frame-context schemas carrying executable LocalToolFunction handlers.
    pub fn schema(&self) -> ToolsSchema {
        let standard_tools = self
            .definitions
            .iter()
            .map(|schema| {
                let client = self.client.clone();
                let name = schema.name.clone();
                StandardTool::Direct(DirectFunction {
                    schema: schema.clone(),
                    handler: LocalToolFunction::new(move |args| {
                        let client = client.clone();
                        let name = name.clone();
                        async move {
                            client
                                .call_text(&name, args)
                                .await
                                .map(Value::String)
                                .map_err(|error| ProcessorError::new(error.to_string()))
                        }
                    }),
                })
            })
            .collect();
        ToolsSchema {
            standard_tools,
            custom_tools: None,
        }
    }
    /// Install native agent-worker callbacks atomically; duplicate registration
    /// leaves the caller's registry unchanged. Add the returned lifecycle
    /// processor to the owning pipeline to close MCP at pipeline teardown.
    pub fn register_into(&self, registry: &mut ToolRegistry) -> Result<McpLifecycle, AgentError> {
        let mut pending = registry.clone();
        for schema in &self.definitions {
            let definition = pipecat_services::ToolDefinition {
                name: schema.name.clone(),
                description: schema.description.clone(),
                parameters: schema.parameters.clone(),
            };
            let client = self.client.clone();
            let name = schema.name.clone();
            let handler = ToolHandler::new(definition, move |args, ctx| {
                let client = client.clone(); let name = name.clone();
                async move {
                    tokio::select! { biased;
                        _ = ctx.cancelled() => Err(AgentError::Cancelled),
                        result = client.call_text(&name, args) => result.map(|text| ToolResult::new(Value::String(text))).map_err(|error| match error {
                            McpError::Cancelled => AgentError::Cancelled,
                            error => AgentError::Failed(error.to_string()),
                        }),
                    }
                }
            }).with_options(ToolOptions { cancel_on_interruption: true, timeout: Some(self.client.inner.config.limits.request_timeout.saturating_add(self.client.inner.config.limits.close_timeout)) });
            pending.register(handler)?;
        }
        *registry = pending;
        Ok(self.lifecycle())
    }
    /// Executable processor with native tool-call/result frames, interruption
    /// cancellation, duplicate-ID protection, and automatic MCP cleanup.
    pub fn executor(&self) -> Result<McpToolExecutor, ProcessorError> {
        let limits = &self.client.inner.config.limits;
        let mut executor =
            ToolExecutor::new(limits.max_in_flight, 1024, limits.max_argument_bytes)?;
        for schema in &self.definitions {
            let client = self.client.clone();
            let name = schema.name.clone();
            let mut tool = RegisteredTool::new(Arc::new(move |call| {
                let client = client.clone();
                let name = name.clone();
                Box::pin(async move {
                    client
                        .call_text(&name, call.arguments)
                        .await
                        .map(Value::String)
                        .map_err(|error| error.to_string())
                })
            }));
            tool.timeout = limits.request_timeout.saturating_add(limits.close_timeout);
            executor.register(schema.name.clone(), tool)?;
        }
        Ok(McpToolExecutor {
            executor,
            lifecycle: self.lifecycle(),
        })
    }
    /// Add this pass-through processor to a pipeline using schema-attached or
    /// agent-worker handlers. Existing native registries have no cleanup hook.
    pub fn lifecycle(&self) -> McpLifecycle {
        McpLifecycle {
            client: self.client.clone(),
        }
    }
    pub async fn close(&self) -> Result<(), McpError> {
        self.client.close().await
    }
}
#[derive(Clone, Debug)]
pub struct McpLifecycle {
    client: McpClient,
}
#[async_trait]
impl FrameProcessor for McpLifecycle {
    fn name(&self) -> &str {
        "McpLifecycle"
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
        self.client
            .close()
            .await
            .map_err(|error| ProcessorError::new(error.to_string()))
    }
}
pub struct McpToolExecutor {
    executor: ToolExecutor,
    lifecycle: McpLifecycle,
}
#[async_trait]
impl FrameProcessor for McpToolExecutor {
    fn name(&self) -> &str {
        "McpToolExecutor"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if let FrameData::FunctionCallsStarted(calls) = &mut frame.data {
                // MCP permits omitted arguments, represented as null by callers.
                // The native generic executor expects an explicit object.
                for call in calls {
                    if call.arguments.is_null() {
                        call.arguments = Value::Object(Default::default());
                    }
                }
            }
        }
        self.executor.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.executor.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let result = self.executor.cleanup().await;
        self.lifecycle.cleanup().await?;
        result
    }
}
