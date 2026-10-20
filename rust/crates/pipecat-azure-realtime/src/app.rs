//! Bounded, immutable final-tool PCM application; no second protocol implementation.
use pipecat_core::{
    async_trait, Direction, FlushFrame, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig,
    PipelineError, PipelineWorker, ProcessorContext, ProcessorError, ToolSet,
};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor, ToolOutcomeJournal};
use pipecat_realtime::{
    AudioFormat, FunctionTool, Modality, OpenAiRealtimeProcessor, RealtimeClient, RealtimeConfig,
    TurnDetection,
};
use pipecat_services::{Content, ContentPart, Context};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, UserAggregatorConfig,
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

/// An ordinary bounded, interruptible, final-only local callback and its exact schema.
pub struct AzureLocalTool {
    pub schema: FunctionTool,
    pub handler: RegisteredTool,
}

/// A single-session application. All socket behavior belongs to pipecat-realtime.
/// Construct a fresh app to change deployment, session settings, tools or history.
pub struct AzureRealtimeApp {
    pipeline: Pipeline,
    pipeline_config: PipelineConfig,
    pair: LLMContextAggregatorPair,
    provider: Arc<OpenAiRealtimeProcessor>,
    outcomes: ToolOutcomeJournal,
    cleanup_errors: Arc<Mutex<Vec<String>>>,
}

/// The actual pipeline worker, paired generated-text history and final-tool journal.
/// `provider.acknowledge_playback` is only for a real output playback confirmation;
/// generated PCM and file writes are not played receipts.
pub struct AzureRunningApp {
    pub worker: PipelineWorker,
    pub pair: LLMContextAggregatorPair,
    pub provider: AzureRealtimeHandle,
    pub outcomes: ToolOutcomeJournal,
}

/// Restricted handle for the app-owned provider. It deliberately exposes no
/// low-level sender, Action, context or settings mutation path.
#[derive(Clone)]
pub struct AzureRealtimeHandle(Arc<OpenAiRealtimeProcessor>);
impl AzureRealtimeHandle {
    pub fn acknowledge_playback(
        &self,
        position: pipecat_realtime::PlaybackPosition,
    ) -> Result<(), ProcessorError> {
        self.0.acknowledge_playback(position)
    }
    pub fn session_joined(&self) -> bool {
        self.0.session_joined()
    }
}

/// Retain returned cleanup errors even when Pipeline::start returns its primary
/// failure before exposing a worker. Delegation creates no extra task/owner and
/// preserves the original processor context, health and lifecycle behavior.
struct CleanupObserved {
    inner: Arc<dyn FrameProcessor>,
    errors: Arc<Mutex<Vec<String>>>,
}
#[async_trait]
impl FrameProcessor for CleanupObserved {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<pipecat_core::ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        config: &pipecat_core::StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let result = self.inner.cleanup().await;
        if let Err(error) = &result {
            let mut errors = self
                .errors
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if errors.len() < 16 {
                errors.push(
                    self.inner
                        .name()
                        .chars()
                        .take(128)
                        .chain(" cleanup: ".chars())
                        .chain(error.message.chars())
                        .take(4096)
                        .collect(),
                );
            }
        }
        result
    }
}
fn startup_failure(primary: PipelineError, errors: &Mutex<Vec<String>>) -> PipelineError {
    let errors = errors.lock().unwrap_or_else(|error| error.into_inner());
    if errors.is_empty() {
        primary
    } else {
        PipelineError::Execution(format!("{primary}; cleanup: {}", errors.join("; ")))
    }
}

struct Budget(usize);
impl Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.checked_sub(bytes.len()).ok_or_else(|| {
            std::io::Error::other("Azure application input exceeds encoded bound")
        })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn bounded(value: &impl Serialize, limit: usize) -> Result<(), ProcessorError> {
    serde_json::to_writer(Budget(limit), value)
        .map_err(|_| ProcessorError::new("Azure application input exceeds encoded bound"))
}

/// Runs before the user can turn a retagged carrier into ordinary inference.
struct IntegrityIngress {
    limit: usize,
    inbound: Direction,
}
#[async_trait]
impl FrameProcessor for IntegrityIngress {
    fn name(&self) -> &str {
        "AzureRealtimeIntegrityIngress"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let invalid = |_| ProcessorError::fatal("invalid native Azure ingress carrier");
        frame.native_tool_batch().map_err(invalid)?;
        frame.native_tool_outcome().map_err(invalid)?;
        frame.native_tool_progress_admission().map_err(invalid)?;
        frame.native_tool_progress().map_err(invalid)?;
        if let Some(cause) = frame.native_realtime_context_cause().map_err(invalid)? {
            cause.validate_direction(direction).map_err(invalid)?;
        }
        if frame
            .native_llm_continuation()
            .map_err(|_| ProcessorError::fatal("invalid native Azure continuation carrier"))?
            .is_some()
            || frame.has_native_tool_progress_restriction()
        {
            return Err(ProcessorError::fatal(
                "Azure PCM app supports only final native tools",
            ));
        }
        reject_controls(&frame.data)?;
        if direction == self.inbound {
            let flush_origin = match &frame.data {
                FrameData::Flush(flush) => flush.origin.as_deref(),
                FrameData::FlushDetailed(flush) => flush.origin.as_deref(),
                _ => None,
            };
            if flush_origin == Some("assistant_turn_end") {
                return Err(ProcessorError::fatal(
                    "reserved assistant flush cannot enter the Azure app",
                ));
            }
            // Caller input is deliberately downstream-only. Upstream provider
            // and assistant traffic originates inside the owned topology and
            // never enters through this tail boundary. Ordinary core flush
            // probes do a round trip through both boundaries; the assistant's
            // reserved finalization origin is excluded above.
            if self.inbound == Direction::Upstream
                && !matches!(
                    frame.data,
                    FrameData::Flush(_) | FrameData::FlushDetailed(_)
                )
            {
                return Err(ProcessorError::fatal(
                    "Azure app caller input must be downstream",
                ));
            }
            match &frame.data {
                FrameData::InputText(text) => bounded(text, self.limit)?,
                FrameData::LlmMessagesAppend(messages) => {
                    bounded(messages, self.limit)?;
                    if messages.messages.iter().any(|m| {
                        m.role != pipecat_core::ChatRole::User
                            || m.name.is_some()
                            || m.tool_call_id.is_some()
                            || !m.tool_calls.is_empty()
                    }) {
                        return Err(ProcessorError::new(
                            "Azure app live appends require ordinary user text",
                        ));
                    }
                }
                FrameData::Start(_)
                | FrameData::StartDetailed(_)
                | FrameData::InputAudio(_)
                | FrameData::UserAudio(_)
                | FrameData::LlmRun
                | FrameData::UserStartedSpeaking
                | FrameData::UserStoppedSpeaking
                | FrameData::UserSpeaking
                | FrameData::VadUserStartedSpeaking(_)
                | FrameData::VadUserStoppedSpeaking(_)
                | FrameData::VadUserStartedSpeakingDetailed(_)
                | FrameData::VadUserStoppedSpeakingDetailed(_)
                | FrameData::UserMuteStarted
                | FrameData::UserMuteStopped
                | FrameData::BotStartedSpeaking
                | FrameData::BotStoppedSpeaking
                | FrameData::Interruption
                | FrameData::SpeechControlParams(_)
                | FrameData::FunctionCallCancel(_)
                | FrameData::Flush(_)
                | FrameData::FlushDetailed(_)
                | FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Stop
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_) => {}
                _ => {
                    return Err(ProcessorError::fatal(
                        "unsupported Azure app input; provider-owned frames cannot be injected",
                    ))
                }
            }
        }
        ctx.push(frame, direction).await
    }
}
fn reject_controls(data: &FrameData) -> Result<(), ProcessorError> {
    if matches!(
        data,
        FrameData::LlmSetTools(_)
            | FrameData::LlmSetToolChoice(_)
            | FrameData::LlmMessagesUpdate(_)
            | FrameData::LlmMessagesAppendDetailed(_)
            | FrameData::LlmMessagesUpdateDetailed(_)
            | FrameData::LlmMessagesTransform(_)
            | FrameData::ServiceUpdateSettings(_)
            | FrameData::LlmUpdateSettings(_)
            | FrameData::LlmConfigureOutput { .. }
            | FrameData::LlmEnablePromptCaching { .. }
            | FrameData::LlmSummarizeContext(_)
            | FrameData::LlmContextSummaryResult(_)
            | FrameData::LlmContextSummaryRequest(_)
            | FrameData::LlmContinuationCheckpoint
            | FrameData::InputImage(_)
            | FrameData::UserImage(_)
            | FrameData::InputFile(_)
            | FrameData::UserFile(_)
            | FrameData::UserFileDetailed(_)
            | FrameData::VisionText(_)
    ) {
        return Err(ProcessorError::new("unsupported Azure PCM app control; construct a new app for settings, tools or history replacement"));
    }
    Ok(())
}

/// Also check pair-produced snapshots: the shared context is intentionally public
/// and mutating it must not smuggle new tools past the pre-user integrity gate.
struct BindingGuard {
    tools: ToolSet,
    choice: Option<pipecat_core::ToolChoice>,
    limit: usize,
}
#[async_trait]
impl FrameProcessor for BindingGuard {
    fn name(&self) -> &str {
        "AzureRealtimeBindingGuard"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        reject_controls(&frame.data)?;
        let settings = match &frame.data {
            FrameData::LlmContext(context) => {
                bounded(context, self.limit)?;
                Some((
                    ToolSet::Functions(context.tools.clone()),
                    &context.tool_choice,
                ))
            }
            FrameData::LlmContextDetailed(context) => {
                bounded(&context.context, self.limit)?;
                Some((context.context.tools.clone(), &context.context.tool_choice))
            }
            _ => None,
        };
        if settings.is_some_and(|(tools, choice)| tools != self.tools || *choice != self.choice) {
            return Err(ProcessorError::fatal(
                "Azure app context changed immutable local tool bindings",
            ));
        }
        ctx.push(frame, direction).await
    }
}

/// Hold End before it occupies the provider's normal lane. Durable final
/// receipts arrive from downstream and terminate at that provider, so they do
/// not depend on this blocked lane. The existing actor owns the drain condition,
/// deadlines and join; this gate creates no independent task or tool mirror.
struct FinalToolEndGate {
    provider: Arc<OpenAiRealtimeProcessor>,
    admitted: pipecat_core::LocalFlushEvent,
    timeout: Duration,
}
#[async_trait]
impl FrameProcessor for FinalToolEndGate {
    fn name(&self) -> &str {
        "AzureRealtimeFinalToolEndGate"
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        // Forwarding a prior frame into another node does not prove that node
        // consumed it. A private identity probe orders Finish after the actual
        // provider lane, but is acknowledged before executor/output processing.
        tokio::time::timeout(self.timeout, async {
            ctx.push(
                Frame::new(FrameData::FlushDetailed(pipecat_core::PipelineFlushFrame {
                    event: Some(self.admitted.clone()),
                    returning: false,
                    origin: Some("azure-provider-end-order".into()),
                }))
                .with_interruptible(false),
                Direction::Downstream,
            )
            .await
            .map_err(|error| ProcessorError::fatal(format!("provider End barrier: {error}")))?;
            self.admitted.wait().await;
            Ok::<(), ProcessorError>(())
        })
        .await
        .map_err(|_| ProcessorError::fatal("provider End admission barrier timed out"))??;
        self.provider.drain_final_tools().await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}

/// Only the gate owns this local identity. A caller-supplied origin string or
/// a distinct LocalFlushEvent cannot acknowledge the provider admission barrier.
struct ProviderAdmissionAck(pipecat_core::LocalFlushEvent);
#[async_trait]
impl FrameProcessor for ProviderAdmissionAck {
    fn name(&self) -> &str {
        "AzureRealtimeProviderAdmissionAck"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(&frame.data, FrameData::FlushDetailed(flush) if flush.event.as_ref() == Some(&self.0))
        {
            self.0.set();
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}

impl AzureRealtimeApp {
    pub fn new(
        mut config: RealtimeConfig,
        mut context: Context,
        tools: Vec<AzureLocalTool>,
        output: Arc<dyn FrameProcessor>,
    ) -> Result<Self, ProcessorError> {
        config
            .validate()
            .map_err(|e| ProcessorError::new(e.to_string()))?;
        if !config.is_azure() {
            return Err(ProcessorError::new(
                "Azure app requires explicit Azure GA configuration",
            ));
        }
        bounded(&context, config.limits.max_message_bytes)?;
        if context.messages.len() > 1024
            || tools.len() > 128
            || tools.len() > config.limits.max_outstanding_tools
        {
            return Err(ProcessorError::new(
                "Azure app history or local tool capacity exceeded",
            ));
        }
        for message in &context.messages {
            if let Some(Content::Parts(parts)) = &message.content {
                if parts
                    .iter()
                    .any(|part| !matches!(part, ContentPart::Text { .. }))
                {
                    return Err(ProcessorError::new(
                        "Azure app initial history supports text only",
                    ));
                }
            }
        }
        let audio = config
            .session
            .audio
            .as_ref()
            .ok_or_else(|| ProcessorError::new("Azure PCM audio configuration required"))?;
        let input = audio
            .input
            .as_ref()
            .ok_or_else(|| ProcessorError::new("Azure PCM input required"))?;
        let output_audio = audio
            .output
            .as_ref()
            .ok_or_else(|| ProcessorError::new("Azure PCM output required"))?;
        if input.format != (AudioFormat::Pcm { rate: 24_000 })
            || output_audio.format != (AudioFormat::Pcm { rate: 24_000 })
            || config.session.output_modalities != Some(vec![Modality::Audio])
            || matches!(
                input.turn_detection,
                Some(TurnDetection::SemanticVad { .. })
            )
        {
            return Err(ProcessorError::new(
                "Azure app requires 24000 Hz PCM audio output and manual or server VAD turns",
            ));
        }
        if [
            config.limits.connect_timeout,
            config.limits.operation_timeout,
            config.limits.response_timeout,
            config.limits.close_timeout,
        ]
        .iter()
        .any(|d| *d > Duration::from_secs(600))
        {
            return Err(ProcessorError::new(
                "Azure app timeouts must not exceed 600 seconds",
            ));
        }
        let mut registered = BTreeMap::new();
        for tool in tools {
            bounded(&tool.schema, config.limits.max_message_bytes)?;
            if tool.schema.name.trim().is_empty()
                || !tool.schema.parameters.is_object()
                || tool.schema.name.len() > 128
                || !tool.handler.cancel_on_interruption
                || tool.handler.timeout.is_zero()
                || tool.handler.timeout > config.limits.response_timeout
            {
                return Err(ProcessorError::new(
                    "Azure local tools must be named, bounded, final and interruptible",
                ));
            }
            if registered.insert(tool.schema.name.clone(), tool).is_some() {
                return Err(ProcessorError::new("duplicate Azure local tool name"));
            }
        }
        let offered: Vec<FunctionTool> = if let Some(tools) = &context.tools {
            tools
                .iter()
                .map(|tool| {
                    FunctionTool::new(&tool.name, &tool.description, tool.parameters.clone())
                })
                .collect()
        } else if let Some(tools) = &config.session.tools {
            tools.clone()
        } else {
            registered
                .values()
                .map(|tool| tool.schema.clone())
                .collect()
        };
        if config
            .session
            .tools
            .as_ref()
            .is_some_and(|tools| *tools != offered)
        {
            return Err(ProcessorError::new(
                "Azure configured and context tool schemas differ",
            ));
        }
        if offered.len() != registered.len() {
            return Err(ProcessorError::new(
                "Azure offered tools and local callback set must match exactly",
            ));
        }
        let mut names = std::collections::BTreeSet::new();
        for schema in &offered {
            if !names.insert(&schema.name)
                || registered
                    .get(&schema.name)
                    .is_none_or(|tool| tool.schema != *schema)
            {
                return Err(ProcessorError::new(
                    "every offered Azure tool requires one identical local final callback",
                ));
            }
        }
        let choice = match context.tool_choice.as_ref() {
            Some(pipecat_services::ToolChoice::Auto) => Some(pipecat_realtime::ToolChoice::Auto),
            Some(pipecat_services::ToolChoice::None) => Some(pipecat_realtime::ToolChoice::None),
            Some(pipecat_services::ToolChoice::Required) => {
                Some(pipecat_realtime::ToolChoice::Required)
            }
            Some(pipecat_services::ToolChoice::Function(_)) => {
                return Err(ProcessorError::new(
                    "Azure app named tool choice unsupported",
                ))
            }
            None => config.session.tool_choice,
        };
        if config.session.tool_choice.is_some() && config.session.tool_choice != choice {
            return Err(ProcessorError::new(
                "Azure configured and context tool choices differ",
            ));
        }
        context.tool_choice = choice.map(|choice| match choice {
            pipecat_realtime::ToolChoice::Auto => pipecat_services::ToolChoice::Auto,
            pipecat_realtime::ToolChoice::None => pipecat_services::ToolChoice::None,
            pipecat_realtime::ToolChoice::Required => pipecat_services::ToolChoice::Required,
        });
        context.tools = Some(
            offered
                .iter()
                .map(|tool| pipecat_services::ToolDefinition {
                    name: tool.name.clone(),
                    description: tool.description.clone(),
                    parameters: tool.parameters.clone(),
                })
                .collect(),
        );
        config.session.tools = Some(offered);
        config.session.tool_choice = choice;
        config
            .validate()
            .map_err(|e| ProcessorError::new(e.to_string()))?;
        bounded(&context, config.limits.max_message_bytes)?;
        let pair = LLMContextAggregatorPair::new(
            context,
            UserAggregatorConfig::default(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Text,
            },
        );
        // Uses the standard safe projection, including rejection of lossy service content.
        let seed = pair.context().snapshot_rich()?;
        let binding = Arc::new(BindingGuard {
            tools: seed.tools.clone(),
            choice: seed.tool_choice.clone(),
            limit: config.limits.max_message_bytes,
        });
        let mut executor = ToolExecutor::new(
            config.limits.max_outstanding_tools,
            1024,
            config.limits.max_message_bytes,
        )?;
        for (_, tool) in registered {
            executor.register(tool.schema.name, tool.handler)?;
        }
        let outcomes = executor.outcomes();
        let limit = config.limits.max_message_bytes;
        let barrier_timeout = config.limits.operation_timeout;
        let startup_timeout = config.limits.connect_timeout * 2 + config.limits.operation_timeout;
        let shutdown_timeout = config.limits.response_timeout * 3
            + config.limits.operation_timeout * 2
            + config.limits.close_timeout;
        let provider = Arc::new(
            OpenAiRealtimeProcessor::new(
                RealtimeClient::new(config).map_err(|e| ProcessorError::new(e.to_string()))?,
            )
            .map_err(|e| ProcessorError::new(e.to_string()))?
            .with_final_tool_app()
            .map_err(|e| ProcessorError::new(e.to_string()))?
            .with_initial_context(seed)?,
        );
        let relay = pipecat_realtime::OpenAiRealtimeToolResultRelay::new();
        let admitted = pipecat_core::LocalFlushEvent::default();
        let cleanup_errors = Arc::new(Mutex::new(Vec::new()));
        let processors: Vec<Arc<dyn FrameProcessor>> = vec![
            Arc::new(IntegrityIngress {
                limit,
                inbound: Direction::Downstream,
            }),
            pair.user(),
            binding.clone(),
            Arc::new(FinalToolEndGate {
                provider: provider.clone(),
                admitted: admitted.clone(),
                timeout: barrier_timeout,
            }),
            provider.clone(),
            Arc::new(ProviderAdmissionAck(admitted)),
            binding,
            Arc::new(executor),
            Arc::new(relay),
            output,
            pair.assistant(),
            Arc::new(IntegrityIngress {
                limit,
                inbound: Direction::Upstream,
            }),
        ];
        let pipeline = Pipeline::new(
            processors
                .into_iter()
                .map(|inner| {
                    Arc::new(CleanupObserved {
                        inner,
                        errors: cleanup_errors.clone(),
                    }) as Arc<dyn FrameProcessor>
                })
                .collect(),
        );
        let mut pipeline_config = PipelineConfig {
            startup_timeout,
            shutdown_timeout,
            ..PipelineConfig::default()
        };
        pipeline_config.start.audio_in_sample_rate = 24_000;
        pipeline_config.start.audio_out_sample_rate = 24_000;
        Ok(Self {
            pipeline,
            pipeline_config,
            pair,
            provider,
            outcomes,
            cleanup_errors,
        })
    }

    /// Setup queues the exact initial context without inference. This local
    /// ordered barrier is not a server acknowledgment; the shared actor gates
    /// all audio/history commands on its own bounded provider readiness.
    pub async fn start(self) -> Result<AzureRunningApp, PipelineError> {
        let timeout = self.pipeline_config.startup_timeout;
        let mut worker = match self.pipeline.start(self.pipeline_config).await {
            Ok(worker) => worker,
            Err(primary) => return Err(startup_failure(primary, &self.cleanup_errors)),
        };
        let startup = tokio::time::timeout(timeout, async {
            worker.sender().send(Frame::new(FrameData::Flush(FlushFrame {
                returning: false, origin: Some("azure-initial-context".into()),
            })), Direction::Downstream).await?;
            while let Some(frame) = worker.recv().await {
                match &frame.frame.data {
                    FrameData::Error(error) | FrameData::FatalError(error) => return Err(PipelineError::Execution(format!("Azure startup: {}", error.error))),
                    FrameData::ErrorDetailed(error) => return Err(PipelineError::Execution(format!("Azure startup: {}", error.error))),
                    _ => {}
                }
                if matches!(&frame.frame.data, FrameData::Flush(flush) if flush.origin.as_deref() == Some("azure-initial-context")) {
                    if worker.total_errors() != 0 {
                        return Err(PipelineError::Execution("Azure processor startup reported errors".into()));
                    }
                    return Ok(());
                }
            }
            Err(PipelineError::Closed)
        }).await.unwrap_or(Err(PipelineError::StartupTimeout));
        if let Err(primary) = startup {
            let cancel = worker.cancel("Azure initial context failed").await;
            while worker.recv().await.is_some() {}
            let joined = worker.join().await;
            // Keep the worker after a failed join: the runtime preserves cleanup
            // diagnostics even when the first fatal error remains the primary.
            let errors = worker.errors();
            let secondary = format!(
                "join: {}; retained={}; total={}; evicted={}; {}",
                joined.map_or_else(|error| error.to_string(), |_| "joined".into()),
                errors.len(),
                worker.total_errors(),
                worker.evicted_errors(),
                errors
                    .iter()
                    .map(|error| error.error.as_str())
                    .collect::<Vec<_>>()
                    .join("; ")
            );
            return Err(PipelineError::Execution(format!(
                "{primary}; cancellation: {}; cleanup: {}",
                cancel.map_or_else(|e| e.to_string(), |_| "requested".into()),
                secondary
            )));
        }
        Ok(AzureRunningApp {
            worker,
            pair: self.pair,
            provider: AzureRealtimeHandle(self.provider),
            outcomes: self.outcomes,
        })
    }
}
