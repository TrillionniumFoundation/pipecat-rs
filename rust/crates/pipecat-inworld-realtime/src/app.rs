//! Reusable final-tool PCM application topology, shared by the example and tests.
use crate::{FunctionTool, InworldClient, InworldConfig, InworldRealtimeProcessor, InworldTool};
use pipecat_core::{
    async_trait, Direction, FlushFrame, Frame, FrameData, FrameProcessor, MessagesFrame, Pipeline,
    PipelineConfig, PipelineError, PipelineWorker, ProcessorContext, ProcessorError,
};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor, ToolOutcomeJournal};
use pipecat_services::{Content, ContentPart, Context};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, UserAggregatorConfig,
};
use std::{collections::BTreeMap, sync::Arc};

/// Only final-returning, interruptible callbacks are accepted. Reporting tools,
/// backend/frontend roles and noninterruptible delayed callbacks are not routes
/// supported by the pinned Inworld service.
pub struct InworldLocalTool {
    pub schema: FunctionTool,
    pub handler: RegisteredTool,
}

/// One native session. Consuming `pipeline` starts the exact paired user →
/// Inworld → ToolExecutor → result relay → output → paired assistant topology.
/// The caller supplies an output processor and drives input frames. The default
/// assistant context records generated speech transcripts, not audible playback.
pub struct InworldApp {
    pub pipeline: Pipeline,
    pub pipeline_config: PipelineConfig,
    pub pair: LLMContextAggregatorPair,
    pub provider: Arc<InworldRealtimeProcessor>,
    pub outcomes: ToolOutcomeJournal,
}
/// Started application with its initial text context already admitted before
/// any caller input. The worker owns all pipeline and provider tasks.
pub struct InworldRunningApp {
    pub worker: PipelineWorker,
    pub pair: LLMContextAggregatorPair,
    pub provider: Arc<InworldRealtimeProcessor>,
    pub outcomes: ToolOutcomeJournal,
}

/// Check local authority before the user aggregator can turn a public payload
/// into a new context frame. Checking only at the provider would be too late for
/// a native result whose public payload was replaced with `InputText`.
struct InworldIntegrityIngress {
    max_message_bytes: usize,
}
#[async_trait]
impl FrameProcessor for InworldIntegrityIngress {
    fn name(&self) -> &str {
        "InworldIntegrityIngress"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let invalid = |_| ProcessorError::fatal("invalid native Inworld ingress carrier");
        frame.native_tool_batch().map_err(invalid)?;
        frame.native_tool_outcome().map_err(invalid)?;
        frame.native_tool_progress_admission().map_err(invalid)?;
        frame.native_tool_progress().map_err(invalid)?;
        if let Some(cause) = frame.native_realtime_context_cause().map_err(invalid)? {
            cause.validate_direction(direction).map_err(invalid)?;
        }
        let continuation = frame
            .native_llm_continuation()
            .map_err(|_| ProcessorError::fatal("invalid native Inworld continuation carrier"))?;
        if frame.has_native_tool_progress_restriction() || continuation.is_some() {
            return Err(ProcessorError::fatal(
                "Inworld does not support progressive tools or continuation checkpoints",
            ));
        }
        match &frame.data {
            FrameData::LlmSetToolChoice(_)
            | FrameData::LlmSetTools(_)
            | FrameData::LlmMessagesUpdate(_)
            | FrameData::LlmMessagesAppendDetailed(_)
            | FrameData::LlmMessagesUpdateDetailed(_)
            | FrameData::LlmMessagesTransform(_) => {
                return Err(ProcessorError::new(
                    "unsupported Inworld context control; use a new app for history replacement and session settings for registered tool selection",
                ));
            }
            FrameData::LlmMessagesAppend(messages) => {
                crate::client::bounded_serialized(messages, self.max_message_bytes)
                    .map_err(|_| ProcessorError::new("Inworld appended history exceeds bounds"))?;
                if messages.messages.iter().any(|message| {
                    message.role != pipecat_core::ChatRole::User
                        || message.name.is_some()
                        || message.tool_call_id.is_some()
                        || !message.tool_calls.is_empty()
                }) {
                    return Err(ProcessorError::new(
                        "Inworld live history appends support only ordinary user text",
                    ));
                }
            }
            FrameData::InputText(text) => {
                crate::client::bounded_serialized(text, self.max_message_bytes)
                    .map_err(|_| ProcessorError::new("Inworld input text exceeds bounds"))?;
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
}

impl InworldApp {
    /// Start and admit the initial context without inference. The startup
    /// barrier is local pipeline ordering, not an acknowledgment from Inworld.
    pub async fn start(self) -> Result<InworldRunningApp, PipelineError> {
        let deadline = tokio::time::Instant::now() + self.pipeline_config.startup_timeout;
        let mut worker = self.pipeline.start(self.pipeline_config).await?;
        let result = tokio::time::timeout_at(deadline, async {
            worker.sender().send(Frame::new(FrameData::LlmMessagesAppend(MessagesFrame { messages: vec![], run_llm: false })), Direction::Downstream).await?;
            worker.sender().send(Frame::new(FrameData::Flush(FlushFrame { returning: false, origin: Some("inworld-initial-context".into()) })), Direction::Downstream).await?;
            while let Some(frame) = worker.recv().await {
                if let FrameData::Error(_) = frame.frame.data {
                    return Err(PipelineError::Execution("Inworld initial context failed".into()));
                }
                if matches!(&frame.frame.data, FrameData::Flush(flush) if flush.origin.as_deref() == Some("inworld-initial-context")) { return Ok(()); }
            }
            Err(PipelineError::Closed)
        }).await.unwrap_or(Err(PipelineError::StartupTimeout));
        if let Err(primary) = result {
            let _ = worker.cancel("Inworld initial context failed").await;
            let _ = worker.collect().await;
            return Err(primary);
        }
        Ok(InworldRunningApp {
            worker,
            pair: self.pair,
            provider: self.provider,
            outcomes: self.outcomes,
        })
    }
    pub fn new(
        mut config: InworldConfig,
        context: Context,
        tools: Vec<InworldLocalTool>,
        output: Arc<dyn FrameProcessor>,
    ) -> Result<Self, ProcessorError> {
        crate::client::bounded_serialized(&context, config.limits.max_message_bytes)
            .map_err(|error| ProcessorError::new(error.to_string()))?;
        if context.tool_choice.is_some() || context.messages.len() > 1024 {
            return Err(ProcessorError::new(
                "Inworld context tool choice or history limit unsupported",
            ));
        }
        for message in &context.messages {
            if let Some(Content::Parts(parts)) = &message.content {
                if parts
                    .iter()
                    .any(|part| !matches!(part, ContentPart::Text { .. }))
                {
                    return Err(ProcessorError::new(
                        "Inworld initial history supports text only",
                    ));
                }
            }
        }
        if tools.len() > config.limits.max_outstanding_tools || tools.len() > 128 {
            return Err(ProcessorError::new("Inworld local tool capacity exceeded"));
        }
        let mut registered = BTreeMap::new();
        for tool in tools {
            if !tool.handler.cancel_on_interruption
                || tool.handler.timeout.is_zero()
                || tool.handler.timeout > config.limits.response_timeout
            {
                return Err(ProcessorError::new(
                    "Inworld tools must be final, interruptible and fit the response timeout",
                ));
            }
            if registered.insert(tool.schema.name.clone(), tool).is_some() {
                return Err(ProcessorError::new("duplicate Inworld local tool name"));
            }
        }
        let offered: Vec<FunctionTool> =
            if let Some(tools) = context.tools.as_ref().filter(|tools| !tools.is_empty()) {
                tools
                    .iter()
                    .map(|tool| {
                        FunctionTool::new(&tool.name, &tool.description, tool.parameters.clone())
                    })
                    .collect()
            } else if let Some(tools) = config
                .session
                .tools
                .as_ref()
                .filter(|tools| !tools.is_empty())
            {
                tools
                    .iter()
                    .map(|tool| match tool {
                        InworldTool::Function {
                            name,
                            description,
                            parameters,
                        } => FunctionTool::new(name, description, parameters.clone()),
                    })
                    .collect()
            } else {
                registered
                    .values()
                    .map(|tool| tool.schema.clone())
                    .collect()
            };
        for tool in &offered {
            if registered
                .get(&tool.name)
                .is_none_or(|entry| entry.schema != *tool)
            {
                return Err(ProcessorError::new("every offered Inworld schema requires an identical local final callback binding"));
            }
        }
        config.session.tools = Some(offered.into_iter().map(Into::into).collect());
        config
            .validate()
            .map_err(|error| ProcessorError::new(error.to_string()))?;
        let audio = config
            .session
            .audio
            .as_ref()
            .ok_or_else(|| ProcessorError::new("Inworld PCM configuration missing"))?;
        let input_rate = audio
            .input
            .as_ref()
            .ok_or_else(|| ProcessorError::new("Inworld input configuration missing"))?
            .format
            .sample_rate();
        let output_rate = audio
            .output
            .as_ref()
            .ok_or_else(|| ProcessorError::new("Inworld output configuration missing"))?
            .format
            .sample_rate();
        let registered_schemas = registered
            .values()
            .map(|tool| tool.schema.clone().into())
            .collect();
        let max_message_bytes = config.limits.max_message_bytes;
        let mut executor = ToolExecutor::new(
            config.limits.max_outstanding_tools,
            config.limits.max_session_tool_calls,
            config.limits.max_message_bytes,
        )?;
        for (_, tool) in registered {
            executor.register(tool.schema.name, tool.handler)?;
        }
        let outcomes = executor.outcomes();
        let startup_timeout = config.limits.connect_timeout * 2 + config.limits.operation_timeout;
        let shutdown_timeout = config.limits.response_timeout * 3
            + config.limits.operation_timeout * 2
            + config.limits.close_timeout;
        let provider = Arc::new(
            InworldRealtimeProcessor::new(
                InworldClient::new(config)
                    .map_err(|error| ProcessorError::new(error.to_string()))?,
            )
            .map_err(|error| ProcessorError::new(error.to_string()))?
            .with_local_tools(registered_schemas),
        );
        let relay = provider.tool_result_relay();
        let pair = LLMContextAggregatorPair::new(
            context,
            UserAggregatorConfig::default(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Text,
            },
        );
        let pipeline = Pipeline::new(vec![
            Arc::new(InworldIntegrityIngress { max_message_bytes }),
            pair.user(),
            provider.clone(),
            Arc::new(executor),
            Arc::new(relay),
            output,
            pair.assistant(),
        ]);
        let mut pipeline_config = PipelineConfig {
            startup_timeout,
            shutdown_timeout,
            ..PipelineConfig::default()
        };
        pipeline_config.start.audio_in_sample_rate = input_rate;
        pipeline_config.start.audio_out_sample_rate = output_rate;
        Ok(Self {
            pipeline,
            pipeline_config,
            pair,
            provider,
            outcomes,
        })
    }
}
