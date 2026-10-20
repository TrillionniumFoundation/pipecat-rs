//! PCM frame adapter for the persistent native Realtime session.
use crate::*;
use pipecat_core::{
    async_trait, AudioRawFrame, ChatRole, Direction, ErrorCategory, ErrorFrame, Frame, FrameData,
    FrameProcessor, FunctionCall, LocalFrameTag, Metric, ProcessorContext, ProcessorError, Setting,
    StartFrame, TextFrame, TranscriptionFrame, TtsStartedFrame, TtsStoppedFrame,
};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum ContextBinding {
    #[default]
    Unbound,
    Seeded,
    Live,
}
#[derive(Default)]
struct ConversationState {
    binding: ContextBinding,
    admission: pipecat_core::RealtimeContextAdmission,
    assistant_admission: pipecat_core::RealtimeContextAdmission,
    explicit_snapshot: Option<pipecat_core::LlmContextFrame>,
    explicit_tools: bool,
}

#[derive(Default)]
struct InputState {
    paused: bool,
    pending: bool,
    tail_bytes: usize,
    full_chunks: bool,
}

/// Pipeline adapter for mono PCM16 at independently configured Inworld sample rates.
/// Turn detection follows the configured session: locally driven
/// turns commit on UserStoppedSpeaking, while server VAD commits remotely.
/// Tool execution belongs to the application's tool processor.
pub struct InworldRealtimeProcessor {
    client: InworldClient,
    sender: Arc<Mutex<Option<InworldSender>>>,
    result_tag: LocalFrameTag,
    tool_delivery: Arc<Mutex<ToolDelivery>>,
    conversation: Arc<Mutex<ConversationState>>,
    connection_attempted: std::sync::atomic::AtomicBool,
    metadata_sent: std::sync::atomic::AtomicBool,
    ending: std::sync::atomic::AtomicBool,
    input_rate: u32,
    output_rate: u32,
    user_id: String,
    manual_turns: bool,
    local_tools: Option<Vec<InworldTool>>,
    input_state: Mutex<InputState>,
    health: pipecat_core::ProcessorHealth,
    health_admission: Arc<Mutex<Option<pipecat_core::ProcessorHealthGeneration>>>,
}
impl InworldRealtimeProcessor {
    pub fn new(client: InworldClient) -> Result<Self, RealtimeError> {
        let client = client.require_native_receipts();
        let audio = client.config().session.audio.as_ref();
        if audio.is_none_or(|a| a.input.is_none() || a.output.is_none()) {
            return Err(RealtimeError::Invalid(
                "frame processor requires explicit input/output audio configuration",
            ));
        }
        let input_rate = audio.unwrap().input.as_ref().unwrap().format.sample_rate();
        let output_rate = audio.unwrap().output.as_ref().unwrap().format.sample_rate();
        let manual_turns = client.config().session.is_manual();
        Ok(Self {
            client,
            sender: Arc::new(Mutex::new(None)),
            result_tag: LocalFrameTag::new(),
            tool_delivery: Arc::new(Mutex::new(ToolDelivery::default())),
            conversation: Arc::new(Mutex::new(ConversationState::default())),
            connection_attempted: std::sync::atomic::AtomicBool::new(false),
            metadata_sent: std::sync::atomic::AtomicBool::new(false),
            ending: std::sync::atomic::AtomicBool::new(false),
            input_rate,
            output_rate,
            user_id: String::new(),
            manual_turns,
            local_tools: None,
            input_state: Mutex::new(InputState::default()),
            health: pipecat_core::ProcessorHealth::default(),
            health_admission: Arc::new(Mutex::new(None)),
        })
    }
    pub(crate) fn with_local_tools(mut self, tools: Vec<InworldTool>) -> Self {
        self.local_tools = Some(tools);
        self
    }
    fn validate_local_tools(&self, session: &SessionProperties) -> Result<(), ProcessorError> {
        if let Some(registered) = &self.local_tools {
            if session
                .tools
                .as_deref()
                .unwrap_or_default()
                .iter()
                .any(|tool| !registered.contains(tool))
            {
                return Err(ProcessorError::new(
                    "Inworld application cannot offer an unbound or changed local tool schema",
                ));
            }
        }
        Ok(())
    }
    /// Place this after the native ToolExecutor and before the assistant
    /// aggregator so every result reaches Inworld before an inference request.
    pub fn tool_result_relay(&self) -> InworldToolResultRelay {
        InworldToolResultRelay {
            sender: self.sender.clone(),
            tag: self.result_tag.clone(),
            delivery: self.tool_delivery.clone(),
            limits: self.client.config().limits.clone(),
            health: self.health.clone(),
            health_admission: self.health_admission.clone(),
        }
    }
    pub fn service_metadata(&self) -> pipecat_core::LlmServiceMetadataFrame {
        pipecat_core::LlmServiceMetadataFrame {
            base: pipecat_core::ServiceMetadataFrame {
                service_name: "InworldRealtimeService".into(),
                user_turn_strategies: if self.manual_turns {
                    None
                } else {
                    Some(pipecat_core::UserTurnStrategies {
                        start: Some(vec![pipecat_core::UserTurnStrategy::External {
                            enable_interruptions: true,
                        }]),
                        stop: Some(vec![pipecat_core::UserTurnStrategy::External {
                            enable_interruptions: true,
                        }]),
                    })
                },
            },
            is_realtime_service: true,
        }
    }
    /// Pause admission and discard any partial input chunk. A full command lane
    /// fails closed rather than replaying a pre-pause tail after resume.
    pub fn set_audio_input_paused(&self, paused: bool) -> Result<(), ProcessorError> {
        let mut input = self
            .input_state
            .lock()
            .map_err(|_| ProcessorError::fatal("input state lock poisoned"))?;
        input.paused = paused;
        if paused {
            input.tail_bytes = 0;
            input.pending = input.full_chunks;
            let sender = self
                .sender
                .lock()
                .map_err(|_| ProcessorError::fatal("realtime state lock poisoned"))?
                .clone();
            if let Some(sender) = sender {
                if let Err(error) = sender.try_send(Action::DiscardBufferedAudio) {
                    sender.close();
                    return Err(processor_error(error));
                }
            }
        }
        Ok(())
    }
    pub fn with_audio_input_paused(self, paused: bool) -> Self {
        self.input_state.lock().expect("new input state").paused = paused;
        self
    }
    fn admit_audio(&self, audio: &AudioRawFrame) -> Result<bool, ProcessorError> {
        let mut input = self
            .input_state
            .lock()
            .map_err(|_| ProcessorError::fatal("input state lock poisoned"))?;
        if input.paused {
            return Ok(false);
        }
        if audio.audio.len() > self.client.config().limits.max_audio_chunk_bytes {
            return Err(processor_error(RealtimeError::TooLarge));
        }
        audio
            .validate()
            .map_err(|_| ProcessorError::new("invalid realtime PCM"))?;
        if audio.sample_rate != self.input_rate || audio.num_channels != 1 {
            return Err(ProcessorError::new(
                "Inworld input requires mono PCM16 at the configured input sample rate",
            ));
        }
        if !audio.audio.is_empty() {
            self.enqueue(Action::AppendAudio(audio.audio.clone()))?;
            let chunk_bytes = (self.input_rate as usize * 60 / 1000) * 2;
            let total = input.tail_bytes + audio.audio.len();
            input.full_chunks |= total >= chunk_bytes;
            input.tail_bytes = total % chunk_bytes;
            input.pending = true;
        }
        Ok(!audio.audio.is_empty())
    }
    fn targets_self(&self, target: &Option<pipecat_core::ProcessorReference>) -> bool {
        match target {
            None => true,
            Some(pipecat_core::ProcessorReference::Name(name)) => name == "InworldRealtimeService",
            Some(pipecat_core::ProcessorReference::Local(target)) => {
                std::ptr::addr_eq(target.0.as_ref(), self as &dyn FrameProcessor)
            }
        }
    }
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }
    /// A health transition retires the connection; restoration never replays
    /// conversation/audio/tool work. Construct a new processor to reconnect.
    pub fn health_handle(&self) -> pipecat_core::ProcessorHealth {
        self.health.clone()
    }
    pub fn sender(&self) -> Result<InworldSender, ProcessorError> {
        check_health(&self.health, &self.health_admission)?;
        self.sender
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime state lock poisoned"))?
            .clone()
            .ok_or_else(|| ProcessorError::new("realtime session is not connected"))
    }
    fn enqueue(&self, action: Action) -> Result<(), ProcessorError> {
        if let Action::UpdateSession(session) = &action {
            self.validate_local_tools(session)?;
            if let Some(audio) = &session.audio {
                if audio.input.as_ref().is_some_and(|i| {
                    i.format
                        != AudioFormat::Pcm {
                            rate: self.input_rate,
                        }
                }) || audio.output.as_ref().is_some_and(|o| {
                    o.format
                        != AudioFormat::Pcm {
                            rate: self.output_rate,
                        }
                }) {
                    return Err(ProcessorError::new(
                        "changing PCM format requires a new Inworld processor",
                    ));
                }
            }
            if session.is_manual() != self.manual_turns {
                return Err(ProcessorError::new(
                    "changing turn mode requires a new Inworld processor",
                ));
            }
        }
        self.sender()?.try_send(action).map_err(processor_error)
    }
    fn handle_context(
        &self,
        context: &pipecat_core::LlmContextFrame,
        direction: Direction,
        cause: Option<Arc<pipecat_core::NativeRealtimeContextCause>>,
        fence: Option<pipecat_core::InvocationFence>,
    ) -> Result<(), ProcessorError> {
        crate::client::bounded_serialized(context, self.client.config().limits.max_message_bytes)
            .map_err(processor_error)?;
        if context.tool_choice.is_some() {
            return Err(ProcessorError::new("Inworld does not support tool_choice"));
        }
        if context.messages.len() > 1024 {
            return Err(processor_error(RealtimeError::TooLarge));
        }
        let mut state = self
            .conversation
            .lock()
            .map_err(|_| ProcessorError::fatal("context lock poisoned"))?;
        if let Some(cause) = &cause {
            cause.validate_direction(direction)?;
            let admission =
                if cause.intent() == pipecat_core::RealtimeContextIntent::AssistantContinuation {
                    &state.assistant_admission
                } else {
                    &state.admission
                };
            if !admission.check(cause)? {
                return Ok(());
            }
            if cause.intent() == pipecat_core::RealtimeContextIntent::Replace
                && state.binding != ContextBinding::Unbound
            {
                return Err(ProcessorError::new(
                    "Inworld cannot replace, delete, or transform live history; start a new session",
                ));
            }
        }
        if direction == Direction::Upstream
            && self.ending.load(std::sync::atomic::Ordering::Acquire)
        {
            // Ordered End settles admitted tools but does not start a newly
            // requested continuation while the provider is draining. Preserve
            // sealed context admission; a continuation owed before End remains
            // owned by the actor's finite drain contract.
            if let Some(cause) = cause {
                state.assistant_admission.commit(cause);
            }
            return Ok(());
        }
        let seed = state.binding == ContextBinding::Unbound && direction == Direction::Downstream;
        let mut actions = Vec::new();
        if seed {
            let projected = self.sender()?.projected_session();
            let mut update = projected.clone();
            if !context.tools.is_empty() && !state.explicit_tools {
                update.tools = Some(
                    context
                        .tools
                        .iter()
                        .map(|t| {
                            FunctionTool::new(&t.name, &t.description, t.parameters.clone()).into()
                        })
                        .collect(),
                );
            }
            let mut messages = context.messages.clone();
            if messages.first().is_some_and(|m| m.role == ChatRole::System) {
                let system = messages.remove(0);
                if projected.instructions.is_none() {
                    update.instructions = Some(system.content);
                }
            }
            for message in &mut messages {
                if matches!(message.role, ChatRole::System | ChatRole::Developer) {
                    message.role = ChatRole::User;
                }
            }
            self.validate_local_tools(&update)?;
            actions.push(Action::UpdateSession(update.into()));
            if !messages.is_empty() {
                let text = if messages.len() == 1 && messages[0].role == ChatRole::User {
                    messages[0].content.clone()
                } else {
                    format!("This is a previously saved conversation. Please treat this conversation history as a starting point for the current conversation.\n\n{}\n\nThis is the end of the previously saved conversation. Please continue the conversation from here. If the last message is a user instruction or question, act on that instruction or answer the question. If the last message is an assistant response, simply say that you are ready to continue the conversation.", serde_json::to_string(&messages).map_err(|_| ProcessorError::new("invalid history"))?)
                };
                actions.push(Action::CreateItem {
                    item: text_item(text, Role::User),
                    previous_item_id: None,
                });
            }
            if cause.as_ref().is_none_or(|cause| cause.run_llm()) {
                actions.push(Action::CreateResponse(ResponseOptions::default()));
            }
        } else if direction == Direction::Upstream {
            actions.push(Action::RequestInference(ResponseOptions::default()));
        } else if let Some(cause) = &cause {
            if let pipecat_core::RealtimeContextIntent::Append { start, end } = cause.intent() {
                let messages = context
                    .messages
                    .get(start..end)
                    .ok_or_else(|| ProcessorError::new("Inworld append range exceeds context"))?;
                // Validate the complete operation before admitting any settings or history.
                for message in messages {
                    if message.role == ChatRole::Tool
                        || message.tool_call_id.is_some()
                        || !message.tool_calls.is_empty()
                    {
                        return Err(ProcessorError::new(
                            "explicit Inworld appends cannot impersonate native tool outcomes",
                        ));
                    }
                    if !message.content.is_empty() {
                        actions.push(Action::CreateItem {
                            item: text_item(
                                message.content.clone(),
                                if message.role == ChatRole::Assistant {
                                    Role::Assistant
                                } else {
                                    Role::User
                                },
                            ),
                            previous_item_id: None,
                        });
                    }
                }
            }
            if cause.run_llm() {
                actions.push(Action::CreateResponse(ResponseOptions::default()));
            }
        } else {
            let previous = state.explicit_snapshot.as_ref().ok_or_else(|| {
                ProcessorError::new(
                    "live Inworld contexts require an explicit native context cause",
                )
            })?;
            if previous != context {
                return Err(ProcessorError::new("unattributed live Inworld history mutation requires an explicit native context cause"));
            }
            actions.push(Action::CreateResponse(ResponseOptions::default()));
        }
        match fence {
            Some(fence) => self.sender()?.try_send_context(actions, fence),
            None => self.sender()?.try_send_batch(actions),
        }
        .map_err(processor_error)?;
        state.binding = if seed {
            ContextBinding::Seeded
        } else if state.binding == ContextBinding::Unbound {
            ContextBinding::Live
        } else {
            state.binding
        };
        if direction == Direction::Downstream && cause.is_none() {
            state.explicit_snapshot = Some(context.clone());
        }
        if let Some(cause) = cause {
            if cause.intent() == pipecat_core::RealtimeContextIntent::AssistantContinuation {
                state.assistant_admission.commit(cause);
            } else {
                state.admission.commit(cause);
            }
        }
        Ok(())
    }
}
fn bounded_detailed_context(
    context: &pipecat_core::DetailedLlmContextFrame,
    limit: usize,
) -> Result<(), ProcessorError> {
    match &context.context.tools {
        pipecat_core::ToolSet::Direct(tools) => {
            if tools.len() > 128 {
                return Err(processor_error(RealtimeError::TooLarge));
            }
            let schemas: Vec<_> = tools.iter().map(|tool| &tool.schema).collect();
            crate::client::bounded_serialized(
                &(
                    &context.context.messages,
                    schemas,
                    &context.context.tool_choice,
                    context.speculation,
                ),
                limit,
            )
            .map_err(processor_error)
        }
        _ => crate::client::bounded_serialized(context, limit).map_err(processor_error),
    }
}
fn fatal_context_error(mut error: ProcessorError) -> ProcessorError {
    error.fatal = true;
    error.message = format!("{}; local context may be retained; rebuild the pair and provider session before continuing", error.message);
    error
}
fn next_item() -> u64 {
    static ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
fn text_item(text: String, role: Role) -> ConversationItem {
    ConversationItem::Message {
        id: format!("pc_message_{}", next_item()),
        role,
        content: vec![ItemContent::InputText { text }],
    }
}
#[async_trait]
impl FrameProcessor for InworldRealtimeProcessor {
    fn name(&self) -> &str {
        "InworldRealtimeService"
    }
    fn health(&self) -> Option<pipecat_core::ProcessorHealth> {
        Some(self.health.clone())
    }
    fn process_when_unusable(&self, frame: &Frame, _: Direction) -> bool {
        !matches!(
            frame.data,
            FrameData::InputAudio(_)
                | FrameData::UserAudio(_)
                | FrameData::InputText(_)
                | FrameData::LlmRun
                | FrameData::LlmContext(_)
                | FrameData::LlmContextDetailed(_)
                | FrameData::LlmMessagesAppend(_)
                | FrameData::FunctionCallResult(_)
                | FrameData::FunctionCallResultDetailed(_)
        )
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if config.audio_in_sample_rate != self.input_rate
            || config.audio_out_sample_rate != self.output_rate
        {
            return Err(ProcessorError::new("Inworld pipeline rates must match the configured input and output formats; resample upstream/downstream"));
        }
        if !self.health.is_usable() {
            return Err(ProcessorError::new("Inworld service is unusable"));
        }
        if self
            .connection_attempted
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(ProcessorError::new("Inworld processors are single-session; construct a new processor to reconnect without replay"));
        }
        let admission = self.health.generation();
        let mut session = self.client.connect().await.map_err(processor_error)?;
        loop {
            match session.next_event().await {
                Some(Ok(InworldEvent {
                    event: Event::SessionUpdated { .. },
                    ..
                })) => break,
                Some(Ok(InworldEvent {
                    event: Event::Error { .. },
                    ..
                })) => {
                    return Err(ProcessorError::new(
                        "Inworld rejected initial session settings",
                    ))
                }
                Some(Err(error)) => return Err(processor_error(error)),
                None => return Err(ProcessorError::new("Inworld closed before readiness")),
                Some(Ok(_)) => {}
            }
        }
        if self.health.generation() != admission {
            session.shutdown().await.map_err(processor_error)?;
            return Err(ProcessorError::new("Inworld health changed during connect"));
        }
        *self
            .health_admission
            .lock()
            .map_err(|_| ProcessorError::fatal("health lock poisoned"))? = Some(admission);
        let sender = session.sender();
        sender.bind_health(self.health.clone(), admission);
        *self
            .sender
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime state lock poisoned"))? =
            Some(sender.clone());
        let output = ctx.clone();
        let user_id = self.user_id.clone();
        let conversation = self.conversation.clone();
        let output_rate = self.output_rate;
        let manual_turns = self.manual_turns;
        let enable_usage_metrics = config.enable_usage_metrics;
        let health = self.health.clone();
        let mut health_changes = health.watch();
        ctx.spawn("inworld-realtime-receive", false, async move {
            let mut mapper = RealtimeFrameMapper::with_sample_rate(user_id, output_rate)?;
            let mut generation = sender.generation();
            let primary: Result<(), ProcessorError> = async {
                loop {
                    let result = tokio::select! { biased;
                        _ = health_changes.changed() => { sender.close(); break; }
                        result = session.next_event_fenced() => result,
                    };
                    if health.generation() != admission {
                        sender.close();
                        break;
                    }
                    let Some(result) = result else {
                        break;
                    };
                    let (event, original_fence) = match result {
                        Ok(event) => (event.event, event.fence),
                        Err(e) => {
                            let mut error = error_frame(e);
                            error.fatal = true;
                            output
                                .push_error_if_health_generation(error, false, admission)
                                .await?;
                            break;
                        }
                    };
                    let interrupted = generation != sender.generation();
                    if interrupted {
                        mapper.reset();
                        generation = sender.generation();
                    }
                    if manual_turns
                        && matches!(
                            event.event,
                            Event::SpeechStarted { .. } | Event::SpeechStopped { .. }
                        )
                    {
                        continue;
                    }
                    if matches!(
                        event.event,
                        Event::ResponseCreated { .. }
                            | Event::InputTranscriptionCompleted { .. }
                            | Event::SpeechStarted { .. }
                    ) {
                        let mut state = conversation
                            .lock()
                            .map_err(|_| ProcessorError::fatal("context lock poisoned"))?;
                        if state.binding == ContextBinding::Unbound {
                            state.binding = ContextBinding::Live;
                        }
                    }
                    let frames = mapper.frames(&event).map_err(processor_error)?;
                    for frame in frames {
                        if health.generation() != admission {
                            sender.close();
                            break;
                        }
                        if event.event.response_id().is_some()
                            && event.generation != sender.generation()
                        {
                            break;
                        }
                        // Usage admission is separate from the public raw mapper.
                        // Provider zero totals are not usage observations.
                        if let FrameData::Metrics(metrics) = &frame.data {
                            if let [Metric::LlmUsage { total_tokens, .. }] = metrics.as_slice() {
                                if !enable_usage_metrics || *total_tokens == 0 {
                                    continue;
                                }
                            }
                        }
                        if matches!(frame.data, FrameData::Error(_)) {
                            if let FrameData::Error(error) = frame.data {
                                output
                                    .push_error_if_health_generation(error, false, admission)
                                    .await?;
                            }
                        } else {
                            let direction = if matches!(
                                frame.data,
                                FrameData::Transcription(_)
                                    | FrameData::InterimTranscription(_)
                                    | FrameData::ProposedUserStartedSpeaking
                                    | FrameData::ProposedUserStoppedSpeaking
                                    | FrameData::InputDtmf(_)
                            ) {
                                Direction::Upstream
                            } else {
                                Direction::Downstream
                            };
                            let generation = if event.event.response_id().is_some() {
                                event.generation
                            } else {
                                sender.generation()
                            };
                            if let Some(result) = sender
                                .while_current(generation, async {
                                    match &original_fence {
                                        Some(fence) => output.try_push_with_invocation_fence(
                                            frame, direction, fence,
                                        ),
                                        None => output.push(frame, direction).await,
                                    }
                                })
                                .await
                            {
                                result?;
                            }
                        }
                    }
                }
                Ok(())
            }
            .await;
            let joined = session.shutdown().await.map_err(processor_error);
            if let Some(cleanup) = session.cleanup_error() {
                let mut error = error_frame(cleanup);
                error.error = format!("Inworld cleanup failure: {}", error.error);
                let _ = output
                    .push_error_if_health_generation(error, false, admission)
                    .await;
            }
            primary.and(joined)
        })?;
        Ok(())
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        // Native carriers must be validated before interpreting any public payload.
        frame.native_tool_batch().map_err(fatal_context_error)?;
        frame.native_tool_outcome().map_err(fatal_context_error)?;
        let cause = frame
            .native_realtime_context_cause()
            .map_err(fatal_context_error)?
            .cloned();
        if let Some(cause) = &cause {
            cause
                .validate_direction(direction)
                .map_err(fatal_context_error)?;
        }
        if frame.data.is_start() {
            ctx.push(frame, direction).await?;
            if !self
                .metadata_sent
                .swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                ctx.broadcast(Frame::new(FrameData::LlmServiceMetadata(
                    self.service_metadata(),
                )))
                .await?;
            }
            return Ok(());
        }
        if frame.is_realtime_context_update() {
            match &frame.data {
                FrameData::LlmContext(context) => crate::client::bounded_serialized(
                    context,
                    self.client.config().limits.max_message_bytes,
                )
                .map_err(processor_error)?,
                FrameData::LlmContextDetailed(context) => bounded_detailed_context(
                    context,
                    self.client.config().limits.max_message_bytes,
                )?,
                _ => unreachable!("state-only context marker"),
            }
            {
                let mut state = self
                    .conversation
                    .lock()
                    .map_err(|_| ProcessorError::fatal("context lock poisoned"))?;
                if state.binding == ContextBinding::Unbound {
                    state.binding = ContextBinding::Live;
                }
            }
            return ctx.push(frame, direction).await;
        }
        if direction == Direction::Upstream
            && cause.is_none()
            && matches!(
                frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        {
            return Err(fatal_context_error(ProcessorError::new(
                "native upstream context requires AssistantContinuation",
            )));
        }
        let fence = match &cause {
            Some(cause) => Some(cause.inference_fence().cloned().ok_or_else(|| {
                ProcessorError::fatal("realtime context cause has no original invocation fence")
            })?),
            None => None,
        };
        // These controls have no provider-side mapping. Forwarding would let an
        // aggregator mutate local history while the live conversation stayed
        // unchanged, so reject them in either direction before any side effect.
        if matches!(
            &frame.data,
            FrameData::LlmMessagesAppendDetailed(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::LlmMessagesTransform(_)
        ) {
            return Err(ProcessorError::new(
                "Inworld detailed message edits and transforms are unsupported; start a new session with explicit context",
            ));
        }
        match &frame.data {
            FrameData::Interruption => {
                let mut input = self
                    .input_state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("input state lock poisoned"))?;
                input.pending = false;
                input.full_chunks = false;
                input.tail_bytes = 0;
                self.sender()?.interrupt();
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) | FrameData::Stop => {
                if let Ok(sender) = self.sender() {
                    sender.close();
                }
            }
            _ => {}
        }
        if let FrameData::LlmContext(context) = &frame.data {
            self.handle_context(context, direction, cause.clone(), fence.clone())
                .map_err(|error| {
                    if cause.is_some() {
                        fatal_context_error(error)
                    } else {
                        error
                    }
                })?;
        }
        if let FrameData::LlmContextDetailed(context) = &frame.data {
            let result: Result<(), ProcessorError> = (|| {
                bounded_detailed_context(context, self.client.config().limits.max_message_bytes)?;
                if context.speculation {
                    return Err(ProcessorError::new(
                        "Inworld speculative inference is unsupported",
                    ));
                }
                let mut messages = Vec::new();
                for message in &context.context.messages {
                    if !message.extra.is_empty() {
                        return Err(ProcessorError::new(
                            "provider-specific history is unsupported",
                        ));
                    }
                    let content = match &message.content {
                        pipecat_core::MessageContent::Text(text) => text.clone(),
                        pipecat_core::MessageContent::Parts(parts) => {
                            if parts
                                .iter()
                                .any(|part| !matches!(part, pipecat_core::MessagePart::Text { .. }))
                            {
                                return Err(ProcessorError::new(
                                    "Inworld history supports text parts only",
                                ));
                            }
                            parts
                                .iter()
                                .map(|part| match part {
                                    pipecat_core::MessagePart::Text { text } => text.as_str(),
                                    _ => unreachable!(),
                                })
                                .collect::<Vec<_>>()
                                .join(" ")
                        }
                    };
                    messages.push(pipecat_core::ChatMessage {
                        role: message.role,
                        content,
                        name: message.name.clone(),
                        tool_call_id: message.tool_call_id.clone(),
                        tool_calls: message.tool_calls.clone(),
                    });
                }
                let tools = match &context.context.tools {
                    pipecat_core::ToolSet::NotGiven => vec![],
                    pipecat_core::ToolSet::Functions(tools) => tools.clone(),
                    pipecat_core::ToolSet::Direct(tools) => {
                        tools.iter().map(|t| t.schema.clone()).collect()
                    }
                    _ => {
                        return Err(ProcessorError::new(
                            "Inworld detailed context requires typed function tools",
                        ))
                    }
                };
                self.handle_context(
                    &pipecat_core::LlmContextFrame {
                        messages,
                        tools,
                        tool_choice: context.context.tool_choice.clone(),
                    },
                    direction,
                    cause.clone(),
                    fence,
                )?;
                Ok(())
            })();
            result.map_err(|error| {
                if cause.is_some() {
                    fatal_context_error(error)
                } else {
                    error
                }
            })?;
        }
        if cause.is_some() {
            frame.take_native_realtime_context_cause()?;
        }
        if direction == Direction::Downstream {
            self.tool_result_relay().relay(&mut frame)?;
            match &frame.data {
                FrameData::InputAudio(audio)
                | FrameData::UserAudio(pipecat_core::UserAudioRawFrame { audio, .. }) => {
                    if self.admit_audio(audio)? {
                        let mut state = self
                            .conversation
                            .lock()
                            .map_err(|_| ProcessorError::fatal("context lock poisoned"))?;
                        if state.binding == ContextBinding::Unbound {
                            state.binding = ContextBinding::Live;
                        }
                    }
                }
                FrameData::UserStoppedSpeaking if self.manual_turns => {
                    // Duplicate local stops and ignored provider VAD cannot
                    // produce an extra commit or inference for the same audio.
                    if frame
                        .metadata
                        .get("user_context_follows")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    {
                        let mut input = self
                            .input_state
                            .lock()
                            .map_err(|_| ProcessorError::fatal("input state lock poisoned"))?;
                        if input.pending {
                            self.sender()?
                                .try_send_batch(vec![
                                    Action::CommitAudio,
                                    Action::CreateResponse(ResponseOptions::default()),
                                ])
                                .map_err(processor_error)?;
                            input.pending = false;
                            input.full_chunks = false;
                            input.tail_bytes = 0;
                        }
                    }
                }
                FrameData::InputText(text) => {
                    crate::client::bounded_serialized(
                        text,
                        self.client.config().limits.max_message_bytes,
                    )
                    .map_err(processor_error)?;
                    self.enqueue(Action::CreateItem {
                        item: ConversationItem::Message {
                            id: format!("pc_text_{}", frame.id),
                            role: Role::User,
                            content: vec![ItemContent::InputText {
                                text: text.text.clone(),
                            }],
                        },
                        previous_item_id: None,
                    })?;
                    self.enqueue(Action::CreateResponse(ResponseOptions {
                        context_id: Some(format!("pc_turn_{}", frame.id)),
                        ..Default::default()
                    }))?;
                    let mut state = self
                        .conversation
                        .lock()
                        .map_err(|_| ProcessorError::fatal("context lock poisoned"))?;
                    if state.binding == ContextBinding::Unbound {
                        state.binding = ContextBinding::Live;
                    }
                }
                FrameData::LlmRun => {
                    self.enqueue(Action::CreateResponse(ResponseOptions::default()))?;
                    let mut state = self
                        .conversation
                        .lock()
                        .map_err(|_| ProcessorError::fatal("context lock poisoned"))?;
                    if state.binding == ContextBinding::Unbound {
                        state.binding = ContextBinding::Live;
                    }
                }
                FrameData::LlmMessagesAppend(messages) => {
                    crate::client::bounded_serialized(
                        messages,
                        self.client.config().limits.max_message_bytes,
                    )
                    .map_err(processor_error)?;
                    if !self.sender()?.is_ready()
                        || self
                            .conversation
                            .lock()
                            .map_err(|_| ProcessorError::fatal("context lock poisoned"))?
                            .binding
                            == ContextBinding::Unbound
                    {
                        return ctx.push(frame, direction).await;
                    }
                    let actions = messages
                        .messages
                        .iter()
                        .filter(|m| !m.content.is_empty())
                        .map(|m| Action::CreateItem {
                            item: text_item(
                                m.content.clone(),
                                if m.role == ChatRole::Assistant {
                                    Role::Assistant
                                } else {
                                    Role::User
                                },
                            ),
                            previous_item_id: None,
                        })
                        .collect();
                    self.sender()?
                        .try_send_batch(actions)
                        .map_err(processor_error)?;
                }
                FrameData::LlmMessagesUpdate(_) => {
                    return Err(ProcessorError::new(
                        "replacing Inworld history requires a new session",
                    ))
                }
                FrameData::LlmSetToolChoice(_) => {
                    return Err(ProcessorError::new("Inworld does not support tool_choice"))
                }
                FrameData::LlmSetTools(tools) => {
                    let tools = match tools {
                        pipecat_core::ToolSet::Functions(v) => v.clone(),
                        pipecat_core::ToolSet::Direct(v) => {
                            v.iter().map(|f| f.schema.clone()).collect()
                        }
                        _ => {
                            return Err(ProcessorError::new(
                                "realtime tool updates require typed function definitions",
                            ))
                        }
                    };
                    let projected = self.sender()?.projected_session();
                    let mut update = projected.clone();
                    update.tools = Some(
                        tools
                            .iter()
                            .map(|t| {
                                FunctionTool::new(&t.name, &t.description, t.parameters.clone())
                                    .into()
                            })
                            .collect(),
                    );
                    self.enqueue(Action::UpdateSession(update.into()))?;
                    self.conversation
                        .lock()
                        .map_err(|_| ProcessorError::fatal("context lock poisoned"))?
                        .explicit_tools = true;
                }
                FrameData::LlmUpdateSettings(settings) => {
                    if !self.targets_self(&settings.service) {
                        return ctx.push(frame, direction).await;
                    }
                    crate::client::bounded_serialized(
                        settings,
                        self.client.config().limits.max_message_bytes,
                    )
                    .map_err(processor_error)?;
                    let mut update: SessionProperties =
                        match settings.settings.get("session_properties") {
                            Some(value) => serde_json::from_value(value.clone()).map_err(|_| {
                                ProcessorError::new("invalid complete Inworld session properties")
                            })?,
                            None => self.sender()?.projected_session(),
                        };
                    for (key, value) in &settings.settings {
                        match key.as_str() {
                            "session_properties" => {}
                            "system_instruction" | "instructions" => {
                                update.instructions = Some(
                                    value
                                        .as_str()
                                        .ok_or_else(|| {
                                            ProcessorError::new("instructions must be text")
                                        })?
                                        .into(),
                                );
                            }
                            "model" => {
                                update.model = Some(
                                    value
                                        .as_str()
                                        .ok_or_else(|| ProcessorError::new("model must be text"))?
                                        .into(),
                                );
                            }
                            "temperature" => {
                                update.temperature = Some(value.as_f64().ok_or_else(|| {
                                    ProcessorError::new("temperature must be a number")
                                })?);
                            }
                            _ => return Err(ProcessorError::new("unsupported Inworld setting")),
                        }
                    }
                    if let Some(delta) = &settings.delta {
                        if !delta.base.extra.is_empty()
                            || !matches!(delta.max_tokens, Setting::NotGiven)
                            || !matches!(delta.top_p, Setting::NotGiven)
                            || !matches!(delta.top_k, Setting::NotGiven)
                            || !matches!(delta.frequency_penalty, Setting::NotGiven)
                            || !matches!(delta.presence_penalty, Setting::NotGiven)
                            || !matches!(delta.seed, Setting::NotGiven)
                            || !matches!(delta.filter_incomplete_user_turns, Setting::NotGiven)
                            || !matches!(delta.user_turn_completion_config, Setting::NotGiven)
                        {
                            return Err(ProcessorError::new("unsupported Inworld LLM setting"));
                        }
                        match &delta.base.model {
                            Setting::Value(model) => update.model = Some(model.clone()),
                            Setting::Null => {
                                return Err(ProcessorError::new("Inworld model cannot be cleared"))
                            }
                            Setting::NotGiven => {}
                        }
                        match &delta.system_instruction {
                            Setting::Value(text) => update.instructions = Some(text.clone()),
                            Setting::Null => update.instructions = Some(String::new()),
                            Setting::NotGiven => {}
                        }
                        match delta.temperature {
                            Setting::Value(value) => update.temperature = Some(value),
                            Setting::Null => update.temperature = None,
                            Setting::NotGiven => {}
                        }
                    }
                    self.enqueue(Action::UpdateSession(update.into()))?;
                }
                _ => {}
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.ending
            .store(true, std::sync::atomic::Ordering::Release);
        if let Ok(sender) = self.sender() {
            sender.finish().await.map_err(processor_error)?;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let sender = {
            self.sender
                .lock()
                .map_err(|_| ProcessorError::fatal("realtime state lock poisoned"))?
                .take()
        };
        // close takes the emission gate; never retain the sender-state mutex.
        if let Some(sender) = sender {
            sender.close();
        }
        let mut state = self
            .conversation
            .lock()
            .map_err(|_| ProcessorError::fatal("context lock poisoned"))?;
        state.explicit_snapshot = None;
        state.admission = Default::default();
        Ok(())
    }
}

#[derive(Default)]
struct ToolDelivery {
    declared: HashSet<String>,
    policies: HashSet<String>,
    deferred: HashMap<String, Frame>,
}

/// Result bridge for `[InworldRealtimeProcessor, ToolExecutor, relay, assistant]`.
/// It delivers results as context without independently requesting inference.
pub struct InworldToolResultRelay {
    sender: Arc<Mutex<Option<InworldSender>>>,
    tag: LocalFrameTag,
    delivery: Arc<Mutex<ToolDelivery>>,
    limits: RealtimeLimits,
    health: pipecat_core::ProcessorHealth,
    health_admission: Arc<Mutex<Option<pipecat_core::ProcessorHealthGeneration>>>,
}
impl InworldToolResultRelay {
    fn relay(&self, frame: &mut Frame) -> Result<(), ProcessorError> {
        let native = frame.native_tool_outcome()?.cloned();
        if self.tag.contains(frame) {
            return Ok(());
        }
        if let FrameData::FunctionCallInProgress(call) = &frame.data {
            if !call.cancel_on_interruption {
                return Err(ProcessorError::fatal("Inworld supports only final interruptible tools; use InworldApp for validated composition"));
            }
            self.sender()?
                .register_tool_policy(call.clone())
                .map_err(processor_error)?;
            self.tag.mark(frame)?;
            return Ok(());
        }
        if let Some(outcome) = native {
            let output = match &frame.data {
                FrameData::FunctionCallResult(result) => result.result.clone(),
                FrameData::FunctionCallCancel(_) => serde_json::Value::Null,
                _ => unreachable!("validated native outcome"),
            };
            self.sender()?
                .send_tool_outcome(outcome.clone(), output)
                .map_err(processor_error)?;
            // The actor serializes only the provider command. Preserve native
            // ownership on the pipeline copy until the assistant reconciles it.
            if outcome.state() == pipecat_core::ToolCallTerminalState::Failed
                && outcome.reason() == pipecat_core::ToolCallEndReason::TimedOut
            {
                frame.project_native_tool_timeout_as_cancel()?;
            }
            self.tag.mark(frame)?;
            return Ok(());
        }
        if matches!(
            frame.data,
            FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_)
        ) {
            return Err(ProcessorError::fatal(
                "Inworld native tool settlement requires a validated terminal receipt",
            ));
        }

        Ok(())
    }
    fn sender(&self) -> Result<InworldSender, ProcessorError> {
        check_health(&self.health, &self.health_admission)?;
        self.sender
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime state lock poisoned"))?
            .clone()
            .ok_or_else(|| ProcessorError::new("realtime session is not connected"))
    }
}
#[async_trait]
impl FrameProcessor for InworldToolResultRelay {
    fn name(&self) -> &str {
        "InworldToolResultRelay"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame.native_tool_batch().map_err(fatal_context_error)?;
        if let Some(cause) = frame
            .native_realtime_context_cause()
            .map_err(fatal_context_error)?
        {
            cause
                .validate_direction(direction)
                .map_err(fatal_context_error)?;
        }
        frame.native_tool_outcome().map_err(fatal_context_error)?;
        if direction != Direction::Downstream {
            return ctx.push(frame, direction).await;
        }
        let native = match frame.native_tool_outcome() {
            Ok(outcome) => outcome.cloned(),
            Err(error) => {
                if let Ok(sender) = self.sender() {
                    sender.close();
                }
                return Err(ProcessorError::fatal(error.to_string()));
            }
        };
        if matches!(frame.data, FrameData::FunctionCallCancel(_))
            && native.is_none()
            && !self.tag.contains(&frame)
        {
            // Do not let an unhandled request settle the assistant's state.
            return Ok(());
        }
        if let Err(error) = self.relay(&mut frame) {
            if let Ok(sender) = self.sender() {
                sender.close();
            }
            return Err(ProcessorError::fatal(error.to_string()));
        }
        let ready = {
            let mut delivery = self
                .delivery
                .lock()
                .map_err(|_| ProcessorError::fatal("tool delivery lock poisoned"))?;
            if let Some(outcome) = native {
                let id = &outcome.call().tool_call_id;
                if !delivery.declared.contains(id)
                    || (matches!(frame.data, FrameData::FunctionCallResult(_))
                        && !delivery.policies.contains(id))
                {
                    if delivery.deferred.len() >= self.limits.max_outstanding_tools
                        && !delivery.deferred.contains_key(id)
                    {
                        return Err(ProcessorError::fatal("native tool delivery limit exceeded"));
                    }
                    delivery.deferred.entry(id.clone()).or_insert(frame);
                    return Ok(());
                }
            }
            let mut ready = Vec::new();
            if let FrameData::FunctionCallsStarted(calls) = &frame.data {
                for call in calls {
                    if delivery.declared.len() >= self.limits.max_session_tool_calls
                        && !delivery.declared.contains(&call.tool_call_id)
                    {
                        return Err(ProcessorError::fatal(
                            "native tool declaration limit exceeded",
                        ));
                    }
                    delivery.declared.insert(call.tool_call_id.clone());
                    if delivery.policies.contains(&call.tool_call_id)
                        || delivery
                            .deferred
                            .get(&call.tool_call_id)
                            .is_some_and(|frame| {
                                matches!(frame.data, FrameData::FunctionCallCancel(_))
                            })
                    {
                        if let Some(terminal) = delivery.deferred.remove(&call.tool_call_id) {
                            ready.push(terminal);
                        }
                    }
                }
            }
            if let FrameData::FunctionCallInProgress(call) = &frame.data {
                if delivery.policies.len() >= self.limits.max_session_tool_calls
                    && !delivery.policies.contains(&call.tool_call_id)
                {
                    return Err(ProcessorError::fatal("native tool policy limit exceeded"));
                }
                delivery.policies.insert(call.tool_call_id.clone());
                if delivery.declared.contains(&call.tool_call_id) {
                    if let Some(terminal) = delivery.deferred.remove(&call.tool_call_id) {
                        ready.push(terminal);
                    }
                }
            }
            ready
        };
        // Both declarations and cancellation receipts use the system lane. Enqueue
        // the declaration first even when an upstream adapter reordered them.
        ctx.push(frame, direction).await?;
        for terminal in ready {
            ctx.push(terminal, direction).await?;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        *self
            .delivery
            .lock()
            .map_err(|_| ProcessorError::fatal("tool delivery lock poisoned"))? =
            ToolDelivery::default();
        Ok(())
    }
}

/// Deterministic Inworld event-to-frame mapping, including split PCM samples and one
/// TTS lifecycle pair per response even when it contains several audio items.
pub struct RealtimeFrameMapper {
    user_id: String,
    sample_rate: u32,
    speaking: Option<String>,
    partial_samples: HashMap<(String, u32), u8>,
    input_transcripts: HashMap<String, String>,
}
impl Default for RealtimeFrameMapper {
    fn default() -> Self {
        Self {
            user_id: String::new(),
            sample_rate: 24_000,
            speaking: None,
            partial_samples: HashMap::new(),
            input_transcripts: HashMap::new(),
        }
    }
}
impl RealtimeFrameMapper {
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            ..Self::default()
        }
    }
    pub fn with_sample_rate(
        user_id: impl Into<String>,
        sample_rate: u32,
    ) -> Result<Self, ProcessorError> {
        if ![8000, 16000, 24000, 32000, 44100, 48000].contains(&sample_rate) {
            return Err(ProcessorError::new("unsupported Inworld sample rate"));
        }
        Ok(Self {
            user_id: user_id.into(),
            sample_rate,
            ..Self::default()
        })
    }
    pub fn reset(&mut self) {
        self.speaking = None;
        self.partial_samples.clear();
        self.input_transcripts.clear();
    }
    pub fn frames(&mut self, e: &InworldEvent) -> Result<Vec<Frame>, RealtimeError> {
        let mut frames = vec![];
        match &e.event {
            Event::ResponseCreated { .. } => frames.push(Frame::new(FrameData::LlmResponseStart)),
            Event::AudioDelta {
                response_id,
                item_id,
                content_index,
                audio,
                ..
            } => {
                if self.speaking.as_ref() != Some(response_id) {
                    if self.speaking.is_some() {
                        return Err(RealtimeError::Protocol("overlapping audio responses"));
                    }
                    self.speaking = Some(response_id.clone());
                    frames.push(Frame::new(FrameData::TtsStarted(TtsStartedFrame {
                        context_id: e.context_id.clone().or_else(|| Some(response_id.clone())),
                        ..Default::default()
                    })));
                }
                let key = (item_id.clone(), *content_index);
                let mut pcm = Vec::with_capacity(audio.len() + 1);
                if let Some(byte) = self.partial_samples.remove(&key) {
                    pcm.push(byte);
                }
                pcm.extend_from_slice(audio);
                if !pcm.len().is_multiple_of(2) {
                    if self.partial_samples.len() >= 64 {
                        return Err(RealtimeError::TooLarge);
                    }
                    self.partial_samples
                        .insert(key, pcm.pop().expect("odd buffer is nonempty"));
                }
                if !pcm.is_empty() {
                    frames.push(Frame::new(FrameData::TtsAudio(AudioRawFrame {
                        audio: pcm,
                        sample_rate: self.sample_rate,
                        num_channels: 1,
                        context_id: e.context_id.clone().or_else(|| Some(response_id.clone())),
                    })));
                }
            }
            Event::AudioDone {
                item_id,
                content_index,
                ..
            } => {
                if self
                    .partial_samples
                    .contains_key(&(item_id.clone(), *content_index))
                {
                    return Err(RealtimeError::Protocol(
                        "audio ends with a partial PCM sample",
                    ));
                }
            }
            Event::AudioTranscriptDelta { delta, .. } if !delta.is_empty() => {
                let mut text = TextFrame::llm(delta);
                text.append_to_context = false;
                text.skip_tts = Some(true);
                frames.push(Frame::new(FrameData::LlmText(text)));
                frames.push(Frame::new(FrameData::TtsText(
                    pipecat_core::AggregatedTextFrame {
                        text: TextFrame::llm(delta),
                        aggregated_by: "sentence".into(),
                        context_id: e
                            .context_id
                            .clone()
                            .or_else(|| e.event.response_id().map(str::to_owned)),
                        raw_text: None,
                        will_be_spoken: true,
                    },
                )));
            }
            Event::InputTranscriptionDelta { item_id, delta, .. } if !delta.is_empty() => {
                if !self.input_transcripts.contains_key(item_id)
                    && self.input_transcripts.len() >= 128
                {
                    return Err(RealtimeError::TooLarge);
                }
                let retained: usize = self.input_transcripts.values().map(String::len).sum();
                if retained.saturating_add(delta.len()) > 1024 * 1024 {
                    return Err(RealtimeError::TooLarge);
                }
                let transcript = self.input_transcripts.entry(item_id.clone()).or_default();
                transcript.push_str(delta);
                frames.push(Frame::new(FrameData::InterimTranscription(
                    TranscriptionFrame {
                        text: transcript.trim().into(),
                        user_id: self.user_id.clone(),
                        ..Default::default()
                    },
                )));
            }
            Event::InputTranscriptionCompleted {
                item_id,
                transcript,
                ..
            } => {
                self.input_transcripts.remove(item_id);
                if !transcript.trim().is_empty() {
                    frames.push(Frame::new(FrameData::Transcription(TranscriptionFrame {
                        text: transcript.trim().into(),
                        user_id: self.user_id.clone(),
                        finalized: true,
                        ..Default::default()
                    })));
                }
            }
            Event::SpeechStarted { .. } => {
                frames.push(Frame::new(FrameData::ProposedUserStartedSpeaking))
            }
            Event::SpeechStopped { .. } => {
                frames.push(Frame::new(FrameData::ProposedUserStoppedSpeaking))
            }
            Event::FunctionArgumentsDone {
                call_id,
                name,
                arguments,
                ..
            } => {
                frames.push(Frame::new(FrameData::FunctionCallsStarted(vec![
                    FunctionCall {
                        function_name: name
                            .clone()
                            .ok_or(RealtimeError::Protocol("tool name missing"))?,
                        tool_call_id: call_id.clone(),
                        arguments: serde_json::from_str(arguments)
                            .map_err(|_| RealtimeError::Protocol("invalid tool JSON"))?,
                    },
                ])));
            }
            Event::ResponseDone { response, usage } => {
                if !self.partial_samples.is_empty() {
                    return Err(RealtimeError::Protocol(
                        "response ends with a partial PCM sample",
                    ));
                }
                if self.speaking.take().is_some() {
                    frames.push(Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
                        context_id: e.context_id.clone().or_else(|| Some(response.id.clone())),
                    })));
                }
                if let Some(usage) = usage
                    .as_ref()
                    .or(response.usage.as_ref())
                    .filter(|u| u.total_tokens.is_some())
                {
                    frames.push(Frame::new(FrameData::Metrics(vec![Metric::LlmUsage {
                        processor: "InworldRealtimeService".into(),
                        prompt_tokens: usage.input_tokens.unwrap_or(0),
                        completion_tokens: usage.output_tokens.unwrap_or(0),
                        total_tokens: usage.total_tokens.unwrap_or(0),
                    }])));
                }
                if response.status == ResponseStatus::Failed {
                    frames.push(Frame::new(FrameData::Error(ErrorFrame::new(
                        "Inworld response failed",
                    ))));
                }
                frames.push(Frame::new(FrameData::LlmResponseEnd));
            }
            Event::Error { .. } => frames.push(Frame::new(FrameData::Error(ErrorFrame::new(
                "Inworld provider error",
            )))),
            _ => {}
        }
        for frame in &mut frames {
            frame
                .metadata
                .insert("realtime_event_id".into(), json!(e.event_id));
            frame
                .metadata
                .insert("realtime_generation".into(), json!(e.generation));
            for (key, value) in [
                ("session_id", e.session_id.as_deref()),
                ("conversation_id", e.conversation_id.as_deref()),
                ("context_id", e.context_id.as_deref()),
                ("response_id", e.event.response_id()),
            ] {
                if let Some(value) = value {
                    frame.metadata.insert(key.into(), json!(value));
                }
            }
            let item = match &e.event {
                Event::AudioDelta {
                    item_id,
                    content_index,
                    ..
                }
                | Event::AudioTranscriptDelta {
                    item_id,
                    content_index,
                    ..
                }
                | Event::InputTranscriptionDelta {
                    item_id,
                    content_index,
                    ..
                }
                | Event::InputTranscriptionCompleted {
                    item_id,
                    content_index,
                    ..
                } => Some((item_id, *content_index)),
                _ => None,
            };
            if let Some((item_id, content_index)) = item {
                frame.metadata.insert("item_id".into(), json!(item_id));
                frame
                    .metadata
                    .insert("content_index".into(), json!(content_index));
            }
        }
        Ok(frames)
    }
}
fn check_health(
    health: &pipecat_core::ProcessorHealth,
    admission: &Mutex<Option<pipecat_core::ProcessorHealthGeneration>>,
) -> Result<(), ProcessorError> {
    if !health.is_usable()
        || admission
            .lock()
            .map_err(|_| ProcessorError::fatal("health lock poisoned"))?
            .is_some_and(|generation| generation != health.generation())
    {
        return Err(ProcessorError::new(
            "Inworld health changed; create a new session without replay",
        ));
    }
    Ok(())
}
fn processor_error(error: RealtimeError) -> ProcessorError {
    ProcessorError::new(error.to_string()).with_category(match error {
        RealtimeError::Invalid(_) | RealtimeError::TooLarge | RealtimeError::UnknownToolCall => {
            ErrorCategory::InvalidRequest
        }
        _ => ErrorCategory::Unknown,
    })
}
fn error_frame(error: RealtimeError) -> ErrorFrame {
    let mut frame = ErrorFrame::new(error.to_string());
    frame.category = ErrorCategory::Unknown;
    frame
}
