//! Schema-selected application callbacks with one native settlement owner.
use crate::LlmSwitcher;
use pipecat_core::{
    async_trait, DirectFunction, Direction, Frame, FrameData, FrameProcessor, LocalAsyncCallback,
    ProcessorContext, ProcessorError, StandardTool, ToolSet, ToolsSchema,
};
use pipecat_processors::tools::{ToolExecutor, ToolOutcomeJournal, ToolRegistrationSnapshot};
#[cfg(feature = "responses")]
use pipecat_responses::ResponsesProcessor;
use pipecat_services::{frame_context, Context, LlmProcessor};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::task::JoinHandle;

/// Finite admission and retained-cleanup limits for a direct-tool owner.
#[derive(Debug, Clone)]
pub struct DirectToolBindingConfig {
    pub max_in_flight: usize,
    pub duplicate_window: usize,
    pub max_argument_bytes: usize,
    pub max_tools: usize,
    pub max_cleanup_hooks: usize,
    /// Handler deadlines override this default. Two absent deadlines mean none.
    pub default_timeout: Option<Duration>,
}
impl Default for DirectToolBindingConfig {
    fn default() -> Self {
        Self {
            max_in_flight: 16,
            duplicate_window: 1024,
            max_argument_bytes: 64 * 1024,
            max_tools: 128,
            max_cleanup_hooks: 1024,
            default_timeout: None,
        }
    }
}

/// One ordinary native LLM pipeline's advertised application-handler owner.
/// Keep its stages together in order; clones share registration and cleanup state.
#[derive(Clone)]
pub struct DirectToolBinding {
    llm: Arc<dyn FrameProcessor>,
    owner: Arc<DirectExecutor>,
}
impl DirectToolBinding {
    pub fn new(llm: Arc<LlmProcessor>) -> Result<Self, ProcessorError> {
        Self::with_config(llm, DirectToolBindingConfig::default())
    }
    pub fn with_config(
        llm: Arc<LlmProcessor>,
        config: DirectToolBindingConfig,
    ) -> Result<Self, ProcessorError> {
        let native = llm.clone();
        Self::build(llm, config, Some(Preflight::Common(native)))
    }
    /// Bind native Responses inference to the same owned application-tool lifecycle.
    #[cfg(feature = "responses")]
    pub fn new_responses(llm: Arc<ResponsesProcessor>) -> Result<Self, ProcessorError> {
        Self::with_responses_config(llm, DirectToolBindingConfig::default())
    }
    /// Responses request validation precedes schema, callback and cleanup publication.
    #[cfg(feature = "responses")]
    pub fn with_responses_config(
        llm: Arc<ResponsesProcessor>,
        config: DirectToolBindingConfig,
    ) -> Result<Self, ProcessorError> {
        let native = llm.clone();
        Self::build(llm, config, Some(Preflight::Responses(native)))
    }
    pub fn new_switcher(llm: Arc<LlmSwitcher>) -> Result<Self, ProcessorError> {
        Self::with_switcher_config(llm, DirectToolBindingConfig::default())
    }
    pub fn with_switcher_config(
        llm: Arc<LlmSwitcher>,
        config: DirectToolBindingConfig,
    ) -> Result<Self, ProcessorError> {
        Self::build(llm, config, None)
    }
    fn build(
        llm: Arc<dyn FrameProcessor>,
        config: DirectToolBindingConfig,
        native: Option<Preflight>,
    ) -> Result<Self, ProcessorError> {
        if config.max_tools == 0
            || config.max_cleanup_hooks == 0
            || config
                .default_timeout
                .is_some_and(|timeout| timeout.is_zero())
        {
            return Err(ProcessorError::new(
                "direct tool limits and timeout must be positive",
            ));
        }
        let executor = ToolExecutor::new(
            config.max_in_flight,
            config.duplicate_window,
            config.max_argument_bytes,
        )?;
        let state = RwLock::new(BindingState {
            current: Arc::new(BindingSnapshot {
                tools: ToolSet::NotGiven,
                registrations: executor.registrations_snapshot(),
            }),
            cleanups: vec![],
            closed: false,
        });
        Ok(Self {
            llm,
            owner: Arc::new(DirectExecutor {
                executor,
                config,
                native,
                state,
            }),
        })
    }
    /// Incoming projection, native inference, shared execution, reverse projection.
    pub fn stages(&self) -> Vec<Arc<dyn FrameProcessor>> {
        vec![
            Arc::new(DirectProjection {
                owner: self.owner.clone(),
                entry: Direction::Downstream,
            }),
            self.llm.clone(),
            self.owner.clone(),
            Arc::new(DirectProjection {
                owner: self.owner.clone(),
                entry: Direction::Upstream,
            }),
        ]
    }
    pub fn outcomes(&self) -> ToolOutcomeJournal {
        self.owner.executor.outcomes()
    }
}

struct BindingSnapshot {
    tools: ToolSet,
    registrations: ToolRegistrationSnapshot,
}
struct BindingState {
    current: Arc<BindingSnapshot>,
    cleanups: Vec<LocalAsyncCallback>,
    closed: bool,
}
enum Preflight {
    Common(Arc<LlmProcessor>),
    #[cfg(feature = "responses")]
    Responses(Arc<ResponsesProcessor>),
}
struct DirectExecutor {
    executor: ToolExecutor,
    config: DirectToolBindingConfig,
    native: Option<Preflight>,
    state: RwLock<BindingState>,
}
impl DirectExecutor {
    fn snapshot(&self) -> Result<Arc<BindingSnapshot>, ProcessorError> {
        let state = self
            .state
            .read()
            .map_err(|_| ProcessorError::fatal("direct tool binding state poisoned"))?;
        if state.closed {
            return Err(ProcessorError::new("direct tool binding is closed"));
        }
        Ok(state.current.clone())
    }
    fn sync(&self, frame: &mut Frame) -> Result<(), ProcessorError> {
        let legacy_tools;
        let tools = match &frame.data {
            FrameData::LlmContext(context) => {
                legacy_tools = ToolSet::Functions(context.tools.clone());
                &legacy_tools
            }
            FrameData::LlmContextDetailed(context) => &context.context.tools,
            FrameData::LlmSetTools(tools) => tools,
            _ => {
                self.snapshot()?;
                return Ok(());
            }
        };
        // Even pruning an empty schema changes execution authority.
        if frame.native_speculation().is_some()
            || frame.native_realtime_context_cause()?.is_some()
            || matches!(&frame.data, FrameData::LlmContextDetailed(context) if context.speculation)
        {
            return Err(ProcessorError::new(
                "direct tool binding cannot accept sealed or speculative context",
            ));
        }
        let (projected, handlers) = project(tools, self.config.max_tools)?;
        match &frame.data {
            FrameData::LlmContext(context) => {
                frame_context::legacy_context(context)
                    .validate()
                    .map_err(|error| ProcessorError::new(error.to_string()))?;
                #[cfg(feature = "responses")]
                if let Some(Preflight::Responses(native)) = &self.native {
                    native
                        .preflight_legacy_context(context)
                        .map_err(|error| ProcessorError::new(error.to_string()))?;
                }
            }
            FrameData::LlmContextDetailed(context) => {
                let mut context = context.context.clone();
                context.tools = projected.clone();
                match &self.native {
                    Some(Preflight::Common(native)) => native.preflight_context(&context),
                    #[cfg(feature = "responses")]
                    Some(Preflight::Responses(native)) => native.preflight_context(&context),
                    None => frame_context::context(&context).map(|_| ()),
                }
                .map_err(|error| ProcessorError::new(error.to_string()))?;
            }
            _ => match &self.native {
                Some(Preflight::Common(native)) => native.preflight_tools(&projected),
                #[cfg(feature = "responses")]
                Some(Preflight::Responses(native)) => native.preflight_tools(&projected),
                None => frame_context::tools(&projected).and_then(|tools| {
                    Context {
                        tools,
                        ..Default::default()
                    }
                    .validate()
                }),
            }
            .map_err(|error| ProcessorError::new(error.to_string()))?,
        }
        let mut inventory = ToolExecutor::new(
            self.config.max_in_flight,
            self.config.duplicate_window,
            self.config.max_argument_bytes,
        )?;
        let mut cleanups = vec![];
        for function in handlers {
            if let Some(cleanup) = function.handler.cleanup_callback() {
                if !cleanups.contains(&cleanup) {
                    cleanups.push(cleanup);
                }
            }
            inventory.register_direct(
                function.schema.name,
                function.handler,
                self.config.default_timeout,
            )?;
        }
        let current = Arc::new(BindingSnapshot {
            tools: projected,
            registrations: inventory.registrations_snapshot(),
        });
        let mut state = self
            .state
            .write()
            .map_err(|_| ProcessorError::fatal("direct tool binding state poisoned"))?;
        if state.closed {
            return Err(ProcessorError::new("direct tool binding is closed"));
        }
        cleanups.retain(|cleanup| !state.cleanups.contains(cleanup));
        if state
            .cleanups
            .len()
            .checked_add(cleanups.len())
            .is_none_or(|count| count > self.config.max_cleanup_hooks)
        {
            return Err(ProcessorError::new(
                "direct tool cleanup hook limit exceeded",
            ));
        }
        // Schema publication, handler selection and cleanup ownership commit together.
        state.cleanups.extend(cleanups);
        state.current = current;
        match &mut frame.data {
            FrameData::LlmContext(_) => {}
            FrameData::LlmContextDetailed(context) => {
                context.context.tools = state.current.tools.clone()
            }
            FrameData::LlmSetTools(tools) => *tools = state.current.tools.clone(),
            _ => unreachable!("direct tool sync accepts only contexts and tool updates"),
        }
        Ok(())
    }
}

fn project(
    tools: &ToolSet,
    max_tools: usize,
) -> Result<(ToolSet, Vec<DirectFunction>), ProcessorError> {
    let count = match tools {
        ToolSet::Direct(functions) => functions.len(),
        ToolSet::Schema(schema) => schema.standard_tools.len(),
        ToolSet::Functions(functions) => functions.len(),
        _ => 0,
    };
    if count > max_tools {
        return Err(ProcessorError::new("direct tool count limit exceeded"));
    }
    let mut handlers = vec![];
    let projected = match tools {
        ToolSet::Direct(functions) => {
            handlers.extend(functions.iter().cloned());
            ToolSet::Functions(
                functions
                    .iter()
                    .map(|function| function.schema.clone())
                    .collect(),
            )
        }
        ToolSet::Schema(schema) => ToolSet::Schema(ToolsSchema {
            standard_tools: schema
                .standard_tools
                .iter()
                .map(|tool| match tool {
                    StandardTool::Direct(function) => {
                        handlers.push(function.clone());
                        StandardTool::Function(function.schema.clone())
                    }
                    StandardTool::Function(definition) => {
                        StandardTool::Function(definition.clone())
                    }
                })
                .collect(),
            custom_tools: schema.custom_tools.clone(),
        }),
        _ => tools.clone(),
    };
    Ok((projected, handlers))
}

struct DirectProjection {
    owner: Arc<DirectExecutor>,
    entry: Direction,
}
#[async_trait]
impl FrameProcessor for DirectProjection {
    fn name(&self) -> &str {
        "DirectToolProjection"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == self.entry {
            self.owner.sync(&mut frame)?;
        } else {
            self.owner.snapshot()?;
        }
        ctx.push(frame, direction).await
    }
}
#[async_trait]
impl FrameProcessor for DirectExecutor {
    fn name(&self) -> &str {
        "DirectToolExecutor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let snapshot = self.snapshot()?;
        self.executor
            .process_with_registrations(frame, direction, ctx, &snapshot.registrations)
            .await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.executor.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let cleanups = {
            let mut state = self
                .state
                .write()
                .map_err(|_| ProcessorError::fatal("direct tool binding state poisoned"))?;
            state.closed = true;
            state.current = Arc::new(BindingSnapshot {
                tools: ToolSet::NotGiven,
                registrations: self.executor.registrations_snapshot(),
            });
            std::mem::take(&mut state.cleanups)
        };
        let mut first_error = self.executor.cleanup().await.err();
        for cleanup in cleanups {
            let mut task = CleanupTask(tokio::spawn(async move { cleanup.call().await }));
            let result = match (&mut task.0).await {
                Ok(result) => result,
                Err(_) => Err(ProcessorError::new("direct tool cleanup task failed")),
            };
            if let Err(error) = result {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
/// Dropping owner cleanup requests abort of its currently awaited hook.
struct CleanupTask(JoinHandle<Result<(), ProcessorError>>);
impl Drop for CleanupTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
