use crate::{McpClient, McpError, SnapshotTool};
use pipecat_agent_workers::{AgentError, ToolHandler, ToolOptions, ToolRegistry, ToolResult};
use pipecat_core::{
    async_trait, DirectFunction, Direction, Frame, FrameData, FrameProcessor, LocalToolFunction,
    ProcessorContext, ProcessorError, StandardTool, ToolDefinition, ToolSet, ToolsSchema,
};
use pipecat_processors::tools::{
    CancellableRegisteredTool, ToolExecutor, ToolRegistrationSnapshot,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, RwLock},
};

/// A discovered immutable schema snapshot plus its session owner. Handlers
/// remain callable until explicitly closed or every owner has been dropped.
#[derive(Clone)]
pub struct McpTools {
    client: McpClient,
    definitions: Vec<ToolDefinition>,
    cached_schema: ToolsSchema,
    snapshot_tools: Vec<Arc<SnapshotTool>>,
}
impl fmt::Debug for McpTools {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpTools")
            .field("tool_count", &self.definitions.len())
            .finish_non_exhaustive()
    }
}
impl McpTools {
    pub(crate) fn new(client: McpClient, entries: Vec<(ToolDefinition, SnapshotTool)>) -> Self {
        let (definitions, snapshot_tools): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .map(|(definition, tool)| (definition, Arc::new(tool)))
            .unzip();
        let standard_tools = definitions
            .iter()
            .zip(&snapshot_tools)
            .map(|(schema, snapshot_tool)| {
                let snapshot_tool = snapshot_tool.clone();
                StandardTool::Direct(DirectFunction {
                    schema: schema.clone(),
                    handler: LocalToolFunction::new(move |args| {
                        let snapshot_tool = snapshot_tool.clone();
                        async move {
                            snapshot_tool
                                .call_text(args)
                                .await
                                .map(Value::String)
                                .map_err(|error| ProcessorError::new(error.to_string()))
                        }
                    }),
                })
            })
            .collect();
        Self {
            client,
            definitions,
            cached_schema: ToolsSchema {
                standard_tools,
                custom_tools: None,
            },
            snapshot_tools,
        }
    }
    pub fn definitions(&self) -> &[ToolDefinition] {
        &self.definitions
    }
    /// Executable schemas retain callback identity across clones of this snapshot.
    pub fn schema(&self) -> ToolsSchema {
        self.cached_schema.clone()
    }
    /// A fixed ordinary native-LLM binding: input projection, native inference,
    /// owned MCP execution, and upstream projection. Keep every stage in this order.
    /// This snapshot cannot bind callbacks from another discovery or resource.
    /// Realtime sealed and speculative executable contexts are unsupported.
    pub fn llm_stages(
        &self,
        llm: Arc<pipecat_services::LlmProcessor>,
    ) -> Result<Vec<Arc<dyn FrameProcessor>>, ProcessorError> {
        let projection = Arc::new(McpContextProjection::new(
            &self.cached_schema,
            self.client.inner.config.limits.max_tools,
        ));
        Ok(vec![
            Arc::new(projection.as_ref().clone()),
            llm,
            Arc::new(self.executor()?),
            projection,
        ])
    }
    /// Bind a native LLM to an explicitly updateable MCP inventory. Updating
    /// the binding does not update the caller's shared LLM context or discover
    /// tools; pass a snapshot from this same client to `McpLlmBinding::update`.
    pub fn bind_llm(
        &self,
        llm: Arc<pipecat_services::LlmProcessor>,
    ) -> Result<McpLlmBinding, ProcessorError> {
        let mut executor = self.executor()?;
        let state = Arc::new(RwLock::new(McpBindingState {
            current: Arc::new(self.binding_snapshot(&executor.executor)),
            closed: false,
        }));
        executor.binding = Some(state.clone());
        Ok(McpLlmBinding {
            client: self.client.clone(),
            state,
            llm,
            executor: Arc::new(executor),
        })
    }
    fn binding_snapshot(&self, executor: &ToolExecutor) -> McpBindingSnapshot {
        McpBindingSnapshot {
            projection: McpContextProjection::new(
                &self.cached_schema,
                self.client.inner.config.limits.max_tools,
            ),
            registrations: executor.registrations_snapshot(),
        }
    }
    /// Install native agent-worker callbacks atomically; duplicate registration
    /// leaves the caller's registry unchanged. Add the returned lifecycle
    /// processor to the owning pipeline to close MCP at pipeline teardown.
    pub fn register_into(&self, registry: &mut ToolRegistry) -> Result<McpLifecycle, AgentError> {
        let mut pending = registry.clone();
        for (schema, snapshot_tool) in self.definitions.iter().zip(&self.snapshot_tools) {
            let definition = pipecat_services::ToolDefinition {
                name: schema.name.clone(),
                description: schema.description.clone(),
                parameters: schema.parameters.clone(),
            };
            let snapshot_tool = snapshot_tool.clone();
            let handler = ToolHandler::new(definition, move |args, ctx| {
                let snapshot_tool = snapshot_tool.clone();
                async move {
                    tokio::select! { biased;
                        _ = ctx.cancelled() => Err(AgentError::Cancelled),
                        result = snapshot_tool.call_text(args) => result.map(|text| ToolResult::new(Value::String(text))).map_err(|error| match error {
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
        for (schema, snapshot_tool) in self.definitions.iter().zip(&self.snapshot_tools) {
            let snapshot_tool = snapshot_tool.clone();
            let mut tool = CancellableRegisteredTool::new(Arc::new(move |call| {
                let snapshot_tool = snapshot_tool.clone();
                Box::pin(async move {
                    match snapshot_tool.call_text(call.arguments).await {
                        Err(McpError::Cancelled) => None,
                        result => {
                            Some(result.map(Value::String).map_err(|error| error.to_string()))
                        }
                    }
                })
            }));
            tool.timeout = limits.request_timeout.saturating_add(limits.close_timeout);
            executor.register_cancellable(schema.name.clone(), tool)?;
        }
        Ok(McpToolExecutor {
            executor,
            lifecycle: self.lifecycle(),
            binding: None,
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

/// Explicit live inventory selection for one owning native LLM pipeline. Each
/// update publishes its schema identities and executable registrations together.
/// Calls are bound when admitted by the executor: an already admitted batch
/// retains its callbacks/options, while later calls use the current inventory.
/// Provider requests crossing an update are not pinned to their earlier schema.
#[derive(Clone)]
pub struct McpLlmBinding {
    client: McpClient,
    state: Arc<RwLock<McpBindingState>>,
    llm: Arc<pipecat_services::LlmProcessor>,
    executor: Arc<McpToolExecutor>,
}
impl McpLlmBinding {
    /// Input projection, native inference, owned execution, upstream projection.
    /// Keep these stages in order in a single owning pipeline.
    pub fn stages(&self) -> Vec<Arc<dyn FrameProcessor>> {
        let projection = Arc::new(McpLiveContextProjection {
            state: self.state.clone(),
        });
        vec![
            projection.clone(),
            self.llm.clone(),
            self.executor.clone(),
            projection,
        ]
    }
    /// Select a discovered snapshot from this same client without replacing the
    /// executor, active leases, duplicate history, or cleanup owner. The caller
    /// must also supply this snapshot's schema in subsequent context/tool frames.
    /// Cleanup permanently closes this binding, even if the client is reopened.
    pub fn update(&self, tools: &McpTools) -> Result<(), ProcessorError> {
        if !Arc::ptr_eq(&self.client.inner, &tools.client.inner) {
            return Err(ProcessorError::new(
                "MCP binding update belongs to another client",
            ));
        }
        let registered = tools.executor()?;
        let current = Arc::new(tools.binding_snapshot(&registered.executor));
        let mut state = self
            .state
            .write()
            .map_err(|_| ProcessorError::fatal("MCP binding state poisoned"))?;
        if state.closed {
            return Err(ProcessorError::new("MCP binding is closed"));
        }
        state.current = current;
        Ok(())
    }
}
struct McpBindingSnapshot {
    projection: McpContextProjection,
    registrations: ToolRegistrationSnapshot,
}
struct McpBindingState {
    current: Arc<McpBindingSnapshot>,
    closed: bool,
}
fn binding_snapshot(
    state: &RwLock<McpBindingState>,
) -> Result<Arc<McpBindingSnapshot>, ProcessorError> {
    let state = state
        .read()
        .map_err(|_| ProcessorError::fatal("MCP binding state poisoned"))?;
    if state.closed {
        return Err(ProcessorError::new("MCP binding is closed"));
    }
    Ok(state.current.clone())
}
struct McpLiveContextProjection {
    state: Arc<RwLock<McpBindingState>>,
}
#[async_trait]
impl FrameProcessor for McpLiveContextProjection {
    fn name(&self) -> &str {
        "McpLiveContextProjection"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let snapshot = binding_snapshot(&self.state)?;
        snapshot.projection.process(frame, direction, ctx).await
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
    binding: Option<Arc<RwLock<McpBindingState>>>,
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
        if let Some(state) = &self.binding {
            let snapshot = binding_snapshot(state)?;
            self.executor
                .process_with_registrations(frame, direction, ctx, &snapshot.registrations)
                .await
        } else {
            self.executor.process(frame, direction, ctx).await
        }
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.executor.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let Some(state) = &self.binding {
            state
                .write()
                .map_err(|_| ProcessorError::fatal("MCP binding state poisoned"))?
                .closed = true;
        }
        let result = self.executor.cleanup().await;
        self.lifecycle.cleanup().await?;
        result
    }
}

/// Both bookends share only immutable bindings. Cleanup belongs to the executor.
#[derive(Clone)]
struct McpContextProjection {
    bindings: Arc<BTreeMap<String, DirectFunction>>,
    max_tools: usize,
}
impl McpContextProjection {
    fn new(schema: &ToolsSchema, max_tools: usize) -> Self {
        let bindings = schema
            .standard_tools
            .iter()
            .filter_map(|tool| {
                if let StandardTool::Direct(function) = tool {
                    Some((function.schema.name.clone(), function.clone()))
                } else {
                    None
                }
            })
            .collect();
        Self {
            bindings: Arc::new(bindings),
            max_tools,
        }
    }
    fn bound_definition(
        &self,
        function: &DirectFunction,
    ) -> Result<ToolDefinition, ProcessorError> {
        if self.bindings.get(&function.schema.name) != Some(function) {
            return Err(ProcessorError::new(
                "executable MCP tool does not match its bound snapshot",
            ));
        }
        Ok(function.schema.clone())
    }
    fn project(&self, tools: &ToolSet) -> Result<Option<ToolSet>, ProcessorError> {
        let mut names = BTreeSet::new();
        let projected = match tools {
            ToolSet::Direct(functions) => {
                if functions.len() > self.max_tools {
                    return Err(ProcessorError::new("bound MCP tool count exceeded"));
                }
                let definitions = functions
                    .iter()
                    .map(|function| {
                        if !names.insert(function.schema.name.clone()) {
                            return Err(ProcessorError::new("duplicate bound MCP tool name"));
                        }
                        self.bound_definition(function)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                ToolSet::Functions(definitions)
            }
            ToolSet::Schema(schema) => {
                // Leave provider-specific schemas to the native service's existing
                // explicit Unsupported path, without rewriting executable state.
                if schema
                    .custom_tools
                    .as_ref()
                    .is_some_and(|map| map.values().any(|tools| !tools.is_empty()))
                    || !schema
                        .standard_tools
                        .iter()
                        .any(|tool| matches!(tool, StandardTool::Direct(_)))
                {
                    return Ok(None);
                }
                if schema.standard_tools.len() > self.max_tools {
                    return Err(ProcessorError::new("bound MCP tool count exceeded"));
                }
                let standard_tools = schema
                    .standard_tools
                    .iter()
                    .map(|tool| {
                        let definition = match tool {
                            StandardTool::Direct(function) => self.bound_definition(function)?,
                            StandardTool::Function(definition) => definition.clone(),
                        };
                        if !names.insert(definition.name.clone()) {
                            return Err(ProcessorError::new("duplicate bound MCP tool name"));
                        }
                        Ok(StandardTool::Function(definition))
                    })
                    .collect::<Result<Vec<_>, ProcessorError>>()?;
                ToolSet::Schema(ToolsSchema {
                    standard_tools,
                    custom_tools: schema.custom_tools.clone(),
                })
            }
            _ => return Ok(None),
        };
        Ok(Some(projected))
    }
    fn projection(&self, frame: &Frame) -> Result<Option<ToolSet>, ProcessorError> {
        let tools = match &frame.data {
            FrameData::LlmContextDetailed(context) => &context.context.tools,
            FrameData::LlmSetTools(tools) => tools,
            _ => return Ok(None),
        };
        let Some(projected) = self.project(tools)? else {
            return Ok(None);
        };
        if frame.native_speculation().is_some()
            || frame.native_realtime_context_cause()?.is_some()
            || matches!(&frame.data, FrameData::LlmContextDetailed(context) if context.speculation)
        {
            return Err(ProcessorError::new(
                "bound MCP executable schema cannot project sealed or speculative context",
            ));
        }
        Ok(Some(projected))
    }
}
#[async_trait]
impl FrameProcessor for McpContextProjection {
    fn name(&self) -> &str {
        "McpContextProjection"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let Some(projected) = self.projection(&frame)? {
            match &mut frame.data {
                FrameData::LlmContextDetailed(context) => context.context.tools = projected,
                FrameData::LlmSetTools(tools) => *tools = projected,
                _ => unreachable!("projection only accepts contexts and tool updates"),
            }
        }
        ctx.push(frame, direction).await
    }
}
