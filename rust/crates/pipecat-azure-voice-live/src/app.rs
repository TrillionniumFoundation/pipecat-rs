//! Bounded native Voice Live PCM application with immutable local bindings.
use crate::{FunctionTool, VoiceLiveClient, VoiceLiveConfig, VoiceLiveProcessor};
use pipecat_core::{
    async_trait, Direction, FlushFrame, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig,
    PipelineError, PipelineWorker, ProcessorContext, ProcessorError, ToolSet,
};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor, ToolOutcomeJournal};
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
pub struct VoiceLiveLocalTool {
    pub schema: FunctionTool,
    pub handler: RegisteredTool,
}

/// A single-session application. All socket behavior belongs to the distinct Voice Live actor.
/// Construct a fresh app to change deployment, session settings, tools or history.
pub struct VoiceLiveApp {
    pipeline: Pipeline,
    pipeline_config: PipelineConfig,
    pair: LLMContextAggregatorPair,
    provider: Arc<VoiceLiveProcessor>,
    outcomes: ToolOutcomeJournal,
    cleanup_errors: Arc<Mutex<Vec<String>>>,
}

/// The actual pipeline worker, paired generated-text history and final-tool journal.
/// `provider.acknowledge_playback` is only for a real output playback confirmation;
/// generated PCM and file writes are not played receipts.
pub struct VoiceLiveRunningApp {
    pub worker: PipelineWorker,
    pub pair: VoiceLiveHistory,
    pub provider: VoiceLiveHandle,
    pub outcomes: ToolOutcomeJournal,
}

/// Read-only access to the actual paired history. Mutating the shared context
/// would bypass the explicit user-append receipts, so the app never exposes it.
#[derive(Clone)]
pub struct VoiceLiveHistory(pipecat_turns::SharedContext);
impl VoiceLiveHistory {
    pub fn context(&self) -> VoiceLiveHistoryContext {
        VoiceLiveHistoryContext(self.0.clone())
    }
}
#[derive(Clone)]
pub struct VoiceLiveHistoryContext(pipecat_turns::SharedContext);
impl VoiceLiveHistoryContext {
    pub fn snapshot(&self) -> Result<Context, ProcessorError> {
        self.0.snapshot()
    }
    pub fn snapshot_rich(&self) -> Result<pipecat_core::LlmContextData, ProcessorError> {
        self.0.snapshot_rich()
    }
}

/// Restricted handle for the app-owned provider. It deliberately exposes no
/// low-level sender, Action, context or settings mutation path.
#[derive(Clone)]
pub struct VoiceLiveHandle(Arc<VoiceLiveProcessor>);
impl VoiceLiveHandle {
    pub fn acknowledge_playback(
        &self,
        position: crate::PlaybackPosition,
    ) -> Result<(), ProcessorError> {
        self.0.acknowledge_playback(position)
    }
    pub fn session_joined(&self) -> bool {
        self.0.session_joined()
    }
    /// Real response completions observed before delivery/assistant consumption.
    /// This is not a success counter or proof of physical playback.
    pub fn responses_completed(&self) -> u64 {
        self.0.responses_completed()
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
            std::io::Error::other("Voice Live application input exceeds encoded bound")
        })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn bounded(value: &impl Serialize, limit: usize) -> Result<(), ProcessorError> {
    serde_json::to_writer(Budget(limit), value)
        .map_err(|_| ProcessorError::new("Voice Live application input exceeds encoded bound"))
}

/// Runs before the user can turn a retagged carrier into ordinary inference.
struct IntegrityIngress {
    limit: usize,
    inbound: Direction,
}
#[async_trait]
impl FrameProcessor for IntegrityIngress {
    fn name(&self) -> &str {
        "VoiceLiveIntegrityIngress"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let invalid = |_| ProcessorError::fatal("invalid native Voice Live ingress carrier");
        frame.native_tool_batch().map_err(invalid)?;
        frame.native_tool_outcome().map_err(invalid)?;
        frame.native_tool_progress_admission().map_err(invalid)?;
        frame.native_tool_progress().map_err(invalid)?;
        if let Some(cause) = frame.native_realtime_context_cause().map_err(invalid)? {
            cause.validate_direction(direction).map_err(invalid)?;
        }
        if frame
            .native_llm_continuation()
            .map_err(|_| ProcessorError::fatal("invalid native Voice Live continuation carrier"))?
            .is_some()
            || frame.has_native_tool_progress_restriction()
        {
            return Err(ProcessorError::fatal(
                "Voice Live PCM app supports only final native tools",
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
                    "reserved assistant flush cannot enter the Voice Live app",
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
                    "Voice Live app caller input must be downstream",
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
                            "Voice Live app live appends require ordinary user text",
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
                _ => return Err(ProcessorError::fatal(
                    "unsupported Voice Live app input; provider-owned frames cannot be injected",
                )),
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
        return Err(ProcessorError::new("unsupported Voice Live PCM app control; construct a new app for settings, tools or history replacement"));
    }
    Ok(())
}

/// Also check pair-produced snapshots. No paired history observation can change
/// the immutable local bindings established before the pre-user integrity gate.
struct BindingGuard {
    tools: ToolSet,
    choice: Option<pipecat_core::ToolChoice>,
    limit: usize,
}
#[async_trait]
impl FrameProcessor for BindingGuard {
    fn name(&self) -> &str {
        "VoiceLiveBindingGuard"
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
                "Voice Live app context changed immutable local tool bindings",
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
    provider: Arc<VoiceLiveProcessor>,
    admitted: pipecat_core::LocalFlushEvent,
    timeout: Duration,
}
#[async_trait]
impl FrameProcessor for FinalToolEndGate {
    fn name(&self) -> &str {
        "VoiceLiveFinalToolEndGate"
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
                    origin: Some("voice-live-provider-end-order".into()),
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
        "VoiceLiveProviderAdmissionAck"
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

/// Keep the actual native executor and its original invocation context. The
/// private weak declaration identity distinguishes a dropped queued proposal
/// from an entered native executor. Entry is conservative ownership, not proof
/// of callback execution or settlement; only the native terminal can settle.
struct GuardedToolExecutor {
    inner: ToolExecutor,
    provider: Arc<VoiceLiveProcessor>,
}
#[async_trait]
impl FrameProcessor for GuardedToolExecutor {
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
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let native_context = ctx.clone();
        let unsigned_declaration = direction == Direction::Downstream
            && matches!(frame.data, FrameData::FunctionCallsStarted(_))
            && frame.native_tool_batch()?.is_none();
        if unsigned_declaration {
            if !self.provider.enter_tool_executor(&frame)? {
                return Ok(());
            }
            // The actual executor reuses this frame for its native batch
            // declaration. Keep that positive evidence durable. This does not
            // refresh the original invocation epoch or callback task policy.
            frame.interruptible = false;
        }
        // No await precedes the native call after entry. Its first poll either
        // reserves owned leases or returns an error, which must fail closed.
        self.inner
            .process(frame, direction, &native_context)
            .await
            .map_err(|error| ProcessorError::fatal(error.to_string()))
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

impl VoiceLiveApp {
    pub fn new(
        mut config: VoiceLiveConfig,
        mut context: Context,
        tools: Vec<VoiceLiveLocalTool>,
        output: Arc<dyn FrameProcessor>,
    ) -> Result<Self, ProcessorError> {
        config
            .validate()
            .map_err(|e| ProcessorError::new(e.to_string()))?;
        bounded(&context, config.limits.max_message_bytes)?;
        if context.messages.len() > config.limits.max_history_messages.min(1024)
            || tools.len() > 128
            || tools.len() > config.limits.max_outstanding_tools
        {
            return Err(ProcessorError::new(
                "Voice Live app history or local tool capacity exceeded",
            ));
        }
        for message in &context.messages {
            if message.role == pipecat_services::Role::Tool
                || !message.tool_calls.is_empty()
                || message.tool_call_id.is_some()
                || message.name.is_some()
            {
                return Err(ProcessorError::new("Voice Live seed requires ordinary text history without historical tool authority or names"));
            }
            if let Some(Content::Parts(parts)) = &message.content {
                if parts
                    .iter()
                    .any(|part| !matches!(part, ContentPart::Text { .. }))
                {
                    return Err(ProcessorError::new(
                        "Voice Live initial history supports text only",
                    ));
                }
            }
        }
        // The typed Voice Live configuration validates the fixed PCM profile.
        let (instructions, seed_item) = seed_context(&context)?;
        // Packed history adds its source framing and escaped JSON. Validate the
        // final seed, including space for the wire envelope, before connecting.
        bounded(
            &seed_item,
            config.limits.max_message_bytes.saturating_sub(1024),
        )?;
        if config.session.instructions.is_none() {
            config.session.instructions = instructions;
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
                "Voice Live app timeouts must not exceed 600 seconds",
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
                    "Voice Live local tools must be named, bounded, final and interruptible",
                ));
            }
            if registered.insert(tool.schema.name.clone(), tool).is_some() {
                return Err(ProcessorError::new("duplicate Voice Live local tool name"));
            }
        }
        let offered: Vec<FunctionTool> = if let Some(tools) = &context.tools {
            tools
                .iter()
                .map(|tool| {
                    FunctionTool::new(&tool.name, &tool.description, tool.parameters.clone())
                })
                .collect()
        } else if !config.session.tools.is_empty() {
            config.session.tools.clone()
        } else {
            registered
                .values()
                .map(|tool| tool.schema.clone())
                .collect()
        };
        if !config.session.tools.is_empty() && config.session.tools != offered {
            return Err(ProcessorError::new(
                "Voice Live configured and context tool schemas differ",
            ));
        }
        if offered.len() != registered.len() {
            return Err(ProcessorError::new(
                "Voice Live offered tools and local callback set must match exactly",
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
                    "every offered Voice Live tool requires one identical local final callback",
                ));
            }
        }
        let choice = match context.tool_choice.as_ref() {
            Some(pipecat_services::ToolChoice::Auto) => crate::ToolChoice::Auto,
            Some(pipecat_services::ToolChoice::None) => crate::ToolChoice::None,
            Some(pipecat_services::ToolChoice::Required) => crate::ToolChoice::Required,
            Some(pipecat_services::ToolChoice::Function(_)) => {
                return Err(ProcessorError::new(
                    "Voice Live app named tool choice unsupported",
                ))
            }
            None => config.session.tool_choice,
        };
        if config.session.tool_choice != choice {
            return Err(ProcessorError::new(
                "Voice Live configured and context tool choices differ",
            ));
        }
        context.tool_choice = Some(match choice {
            crate::ToolChoice::Auto => pipecat_services::ToolChoice::Auto,
            crate::ToolChoice::None => pipecat_services::ToolChoice::None,
            crate::ToolChoice::Required => pipecat_services::ToolChoice::Required,
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
        config.session.tools = offered;
        config.session.tool_choice = choice;
        config
            .validate()
            .map_err(|e| ProcessorError::new(e.to_string()))?;
        bounded(&context, config.limits.max_message_bytes)?;
        let pair = LLMContextAggregatorPair::new(
            context,
            UserAggregatorConfig {
                // Empty-turn recovery invents a developer message, which this
                // immutable Voice Live application deliberately does not own.
                empty_user_turn: None,
                ..UserAggregatorConfig::default()
            },
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
        // Socket connect plus two distinct readiness waits must all fit inside
        // the actual outer setup owner, not race its shorter cancellation.
        let startup_timeout = config
            .limits
            .connect_timeout
            .checked_mul(3)
            .and_then(|value| value.checked_add(config.limits.operation_timeout))
            .ok_or_else(|| ProcessorError::new("Voice Live startup budget overflow"))?;
        let shutdown_timeout = config
            .limits
            .response_timeout
            .checked_mul(3)
            .and_then(|value| {
                config
                    .limits
                    .operation_timeout
                    .checked_mul(2)
                    .and_then(|operations| value.checked_add(operations))
            })
            .and_then(|value| value.checked_add(config.limits.close_timeout))
            .ok_or_else(|| ProcessorError::new("Voice Live shutdown budget overflow"))?;
        let provider = Arc::new(
            VoiceLiveProcessor::new_app(
                VoiceLiveClient::new(config).map_err(|e| ProcessorError::new(e.to_string()))?,
            )
            .map_err(|e| ProcessorError::new(e.to_string()))?
            .with_initial_context(seed, seed_item)?,
        );
        let relay = provider.tool_result_relay();
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
            Arc::new(GuardedToolExecutor {
                inner: executor,
                provider: provider.clone(),
            }),
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
    pub async fn start(self) -> Result<VoiceLiveRunningApp, PipelineError> {
        let timeout = self.pipeline_config.startup_timeout;
        let mut worker = match self.pipeline.start(self.pipeline_config).await {
            Ok(worker) => worker,
            Err(primary) => return Err(startup_failure(primary, &self.cleanup_errors)),
        };
        let startup = tokio::time::timeout(timeout, async {
            worker.sender().send(Frame::new(FrameData::Flush(FlushFrame {
                returning: false, origin: Some("voice-live-initial-context".into()),
            })), Direction::Downstream).await?;
            while let Some(frame) = worker.recv().await {
                match &frame.frame.data {
                    FrameData::Error(error) | FrameData::FatalError(error) => return Err(PipelineError::Execution(format!("Voice Live startup: {}", error.error))),
                    FrameData::ErrorDetailed(error) => return Err(PipelineError::Execution(format!("Voice Live startup: {}", error.error))),
                    _ => {}
                }
                if matches!(&frame.frame.data, FrameData::Flush(flush) if flush.origin.as_deref() == Some("voice-live-initial-context")) {
                    if worker.total_errors() != 0 {
                        return Err(PipelineError::Execution("Voice Live processor startup reported errors".into()));
                    }
                    return Ok(());
                }
            }
            Err(PipelineError::Closed)
        }).await.unwrap_or(Err(PipelineError::StartupTimeout));
        if let Err(primary) = startup {
            let cancel = worker.cancel("Voice Live initial context failed").await;
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
        Ok(VoiceLiveRunningApp {
            worker,
            pair: VoiceLiveHistory(self.pair.context()),
            provider: VoiceLiveHandle(self.provider),
            outcomes: self.outcomes,
        })
    }
}

/// Source adapter's single-item history strategy. Unlike its permissive filter,
/// unsupported content is rejected above rather than silently removed.
fn seed_context(
    context: &Context,
) -> Result<(Option<String>, Option<crate::ConversationItem>), ProcessorError> {
    fn text(message: &pipecat_services::Message) -> String {
        match &message.content {
            Some(Content::Text(text)) => text.clone(),
            Some(Content::Parts(parts)) => parts
                .iter()
                .filter_map(|part| {
                    if let ContentPart::Text { text } = part {
                        Some(text.as_str())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join(" "),
            None => String::new(),
        }
    }
    let mut messages = context.messages.clone();
    let instructions = if messages
        .first()
        .is_some_and(|message| message.role == pipecat_services::Role::System)
    {
        Some(text(&messages.remove(0)))
    } else {
        None
    };
    for message in &mut messages {
        if matches!(
            message.role,
            pipecat_services::Role::System | pipecat_services::Role::Developer
        ) {
            message.role = pipecat_services::Role::User;
        }
    }
    let content = if messages.is_empty() {
        None
    } else if messages.len() == 1 && messages[0].role == pipecat_services::Role::User {
        Some(text(&messages[0]))
    } else {
        let history = serde_json::to_string_pretty(&messages)
            .map_err(|_| ProcessorError::new("Voice Live seed cannot be encoded"))?;
        let intro = "\n        This is a previously saved conversation. Please treat this conversation history as a\n        starting point for the current conversation.";
        let trailing = "\n        This is the end of the previously saved conversation. Please continue the conversation\n        from here. If the last message is a user instruction or question, act on that instruction\n        or answer the question. If the last message is an assistant response, simply say that you\n        are ready to continue the conversation.";
        Some(format!("{intro}\n\n{history}\n\n{trailing}"))
    };
    Ok((
        instructions,
        content.map(crate::processor::user_item).transpose()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_services::{Message, Role};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct HoldDeclaration {
        arrived: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl FrameProcessor for HoldDeclaration {
        fn name(&self) -> &str {
            "HoldVoiceLiveDeclaration"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::FunctionCallsStarted(_)) {
                self.arrived.notify_one();
                self.release.notified().await;
            }
            ctx.push(frame, direction).await
        }
    }

    #[tokio::test]
    async fn stale_mapped_declaration_reaches_real_guard_without_native_admission() {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (sender, call, generation) = crate::client::delivery_tests::test_sender();
            let provider = Arc::new(
                VoiceLiveProcessor::new(
                    VoiceLiveClient::new(VoiceLiveConfig::new(
                        "ws://127.0.0.1:1/voice-live/realtime",
                        "synthetic",
                        crate::VoiceLiveCredentials::ApiKey("invented".into()),
                        "synthetic-voice",
                    ))
                    .unwrap(),
                )
                .unwrap(),
            );
            // The provider is deliberately not a pipeline node: this test uses
            // no socket, actor task, credentials or external service.
            provider.install_test_sender(sender.clone());
            let executions = Arc::new(AtomicUsize::new(0));
            let count = executions.clone();
            let mut executor = ToolExecutor::new(1, 8, 1024).unwrap();
            executor
                .register(
                    &call.function_name,
                    RegisteredTool::new(Arc::new(move |_| {
                        let count = count.clone();
                        Box::pin(async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(serde_json::json!({"unexpected":true}))
                        })
                    })),
                )
                .unwrap();
            let journal = executor.outcomes();
            let arrived = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let worker = Pipeline::new(vec![
                Arc::new(HoldDeclaration {
                    arrived: arrived.clone(),
                    release: release.clone(),
                }),
                Arc::new(GuardedToolExecutor {
                    inner: executor,
                    provider,
                }),
            ])
            .start(PipelineConfig::default())
            .await
            .unwrap();
            let frame = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
            let token = frame.observation_token();
            sender.register_tool_declaration(&frame, 0).unwrap();
            worker
                .sender()
                .send(frame, Direction::Downstream)
                .await
                .unwrap();
            arrived.notified().await;
            assert!(token.is_alive());
            // Advance only the provider generation. The core frame is still
            // live and will reach the guard, rather than being pre-dropped by
            // a pipeline interruption before the behavior under test.
            generation.store(1, Ordering::Release);
            release.notify_one();
            worker.end().await.unwrap();
            let (output, report) = worker.collect().await.unwrap();
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert_eq!(executions.load(Ordering::SeqCst), 0);
            assert!(!token.is_alive());
            assert!(output.iter().all(|item| {
                item.frame.native_tool_outcome().unwrap().is_none()
                    && !matches!(
                        item.frame.data,
                        FrameData::FunctionCallsStarted(_)
                            | FrameData::FunctionCallInProgress(_)
                            | FrameData::FunctionCallResult(_)
                            | FrameData::FunctionCallResultDetailed(_)
                            | FrameData::FunctionCallCancel(_)
                    )
            }));
            let outcomes = journal.snapshot();
            assert!(outcomes.active.is_empty());
            assert!(outcomes.terminal.is_empty());
            assert_eq!(outcomes.total_terminal, 0);
            assert_eq!(outcomes.unadmitted_terminal, 0);
        })
        .await
        .expect("real guarded executor boundary did not terminate");
    }

    #[test]
    fn source_history_pack_keeps_roles_text_and_leading_instructions() {
        let context = Context::new(vec![
            Message::text(Role::System, "instructions"),
            Message::text(Role::Developer, "developer"),
            Message::text(Role::Assistant, "回答"),
            Message::text(Role::User, "next"),
        ]);
        let (instructions, item) = seed_context(&context).unwrap();
        assert_eq!(instructions.as_deref(), Some("instructions"));
        let item = item.unwrap();
        let text = item.content.unwrap().remove(0).text.unwrap();
        assert!(text.starts_with("\n        This is a previously saved conversation."));
        assert!(text.contains("\"role\": \"user\""));
        assert!(text.contains("\"role\": \"assistant\""));
        assert!(text.contains("回答"));
        assert!(!text.contains("instructions"));
        assert!(!text.contains("\"role\": \"developer\""));
    }
    #[test]
    fn a_system_only_context_has_no_seed_item_and_text_parts_are_not_dropped() {
        let mut message = Message::text(Role::System, "");
        message.content = Some(Content::Parts(vec![
            ContentPart::Text {
                text: "first".into(),
            },
            ContentPart::Text {
                text: "second".into(),
            },
        ]));
        let (instructions, item) = seed_context(&Context::new(vec![message])).unwrap();
        assert_eq!(instructions.as_deref(), Some("first second"));
        assert!(item.is_none());
    }
}
