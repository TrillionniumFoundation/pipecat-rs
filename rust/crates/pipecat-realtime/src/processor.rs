//! PCM frame adapter for the persistent native Realtime session.
use crate::context::{project, session as context_session, ContextState, HeldContinuation};
use crate::*;
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, ErrorCategory, ErrorFrame, Frame, FrameData,
    FrameProcessor, FunctionCall, InvocationFence, LlmContextData, LlmServiceMetadataFrame, Metric,
    NativeRealtimeContextCause, ProcessorContext, ProcessorError, RealtimeContextAdmission,
    RealtimeContextIntent, ServiceMetadataFrame, Setting, StartFrame, TextFrame, ToolSet,
    TranscriptionFrame, TtsStartedFrame, TtsStoppedFrame, UserTurnStrategies, UserTurnStrategy,
};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
struct AudioItem {
    generation: u64,
    bytes: u64,
    fence: Option<InvocationFence>,
}
type AudioItems = Arc<Mutex<HashMap<(String, u32), AudioItem>>>;

/// Single-session PCM adapter. Place a paired user aggregator before this service
/// and its assistant after delivery. A tool executor needs the result relay below.
pub struct OpenAiRealtimeProcessor {
    client: RealtimeClient,
    sender: Mutex<Option<RealtimeSender>>,
    playback: Arc<Mutex<Option<(u64, PlaybackPosition)>>>,
    audio_items: AudioItems,
    context: Arc<Mutex<ContextState>>,
    admission: Mutex<RealtimeContextAdmission>,
    assistant_admission: Mutex<RealtimeContextAdmission>,
    initial: Mutex<Option<LlmContextData>>,
    started: AtomicBool,
    receive_ready: Arc<tokio::sync::Notify>,
    user_id: String,
    manual_turns: bool,
}
impl OpenAiRealtimeProcessor {
    pub fn new(mut client: RealtimeClient) -> Result<Self, RealtimeError> {
        let audio = client.config().session.audio.as_ref();
        if audio.is_none_or(|a| a.input.is_none() || a.output.is_none()) {
            return Err(RealtimeError::Invalid(
                "frame processor requires explicit input/output audio configuration",
            ));
        }
        for format in [
            audio.and_then(|a| a.input.as_ref().map(|v| &v.format)),
            audio.and_then(|a| a.output.as_ref().map(|v| &v.format)),
        ]
        .into_iter()
        .flatten()
        {
            if *format != (AudioFormat::Pcm { rate: 24_000 }) {
                return Err(RealtimeError::Invalid(
                    "frame processor requires mono PCM16 at 24000 Hz",
                ));
            }
        }
        let manual_turns = audio
            .and_then(|a| a.input.as_ref())
            .is_none_or(|i| i.turn_detection.is_none());
        // The paired user's configured strategies own pipeline interruptions.
        client.pipeline_turns = true;
        let context = ContextState::with_instructions(client.config().session.instructions.clone());
        Ok(Self {
            client,
            sender: Mutex::new(None),
            playback: Arc::new(Mutex::new(None)),
            audio_items: Arc::new(Mutex::new(HashMap::new())),
            context: Arc::new(Mutex::new(context)),
            admission: Mutex::new(RealtimeContextAdmission::default()),
            assistant_admission: Mutex::new(RealtimeContextAdmission::default()),
            initial: Mutex::new(None),
            started: AtomicBool::new(false),
            receive_ready: Arc::new(tokio::sync::Notify::new()),
            user_id: String::new(),
            manual_turns,
        })
    }
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }
    /// Seed provider history at startup without running inference. Use the same
    /// initial history for the pair. A first unmarked context also explicitly
    /// seeds and runs; marked trailing writes never acquire seed ownership.
    pub fn with_initial_context(mut self, context: LlmContextData) -> Result<Self, ProcessorError> {
        crate::client::bounded_serialized(
            &context.messages,
            self.client.config().limits.max_message_bytes,
        )
        .map_err(processor_error)?;
        ContextState::default().snapshot(
            project(&context.messages)?,
            false,
            self.client.config().limits.max_message_bytes,
        )?;
        crate::client::bounded_serialized(
            &context_session(&context)?,
            self.client.config().limits.max_message_bytes,
        )
        .map_err(processor_error)?;
        self.initial = Mutex::new(Some(context));
        Ok(self)
    }
    /// Accept a transport-confirmed item/content position. Generation and actual
    /// played offset must be validated by that transport; generated PCM is no receipt.
    pub fn acknowledge_playback(&self, position: PlaybackPosition) -> Result<(), ProcessorError> {
        if position.item_id.is_empty() || position.item_id.len() > 256 {
            return Err(ProcessorError::new("invalid realtime playback item"));
        }
        let generation = self.sender()?.generation();
        let items = self
            .audio_items
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime audio items lock poisoned"))?;
        let item = items
            .get(&(position.item_id.clone(), position.content_index))
            .ok_or_else(|| ProcessorError::new("playback item was not emitted by this session"))?;
        if item.generation != generation || position.audio_end_ms > item.bytes / 48 {
            return Err(ProcessorError::new(
                "playback receipt is stale or exceeds emitted audio",
            ));
        }
        let mut playback = self
            .playback
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime playback lock poisoned"))?;
        if playback.as_ref().is_some_and(|(_, old)| {
            old.item_id == position.item_id
                && old.content_index == position.content_index
                && old.audio_end_ms > position.audio_end_ms
        }) {
            return Err(ProcessorError::new(
                "playback position cannot move backwards",
            ));
        }
        let admit = || *playback = Some((generation, position));
        if let Some(fence) = &item.fence {
            fence
                .with_current(admit)
                .ok_or_else(|| ProcessorError::new("playback receipt has stale invocation"))?;
        } else {
            admit();
        }
        Ok(())
    }
    pub fn sender(&self) -> Result<RealtimeSender, ProcessorError> {
        self.sender
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime state lock poisoned"))?
            .clone()
            .ok_or_else(|| ProcessorError::new("realtime session is not connected"))
    }
    fn enqueue(&self, action: Action) -> Result<(), ProcessorError> {
        if let Action::UpdateSession(session) = &action {
            if let Some(audio) = &session.audio {
                for format in [
                    audio.input.as_ref().map(|i| &i.format),
                    audio.output.as_ref().map(|o| &o.format),
                ]
                .into_iter()
                .flatten()
                {
                    if *format != (AudioFormat::Pcm { rate: 24_000 }) {
                        return Err(processor_error(RealtimeError::Invalid(
                            "frame processor requires PCM at 24000 Hz",
                        )));
                    }
                }
                if audio
                    .input
                    .as_ref()
                    .is_some_and(|i| i.turn_detection.is_none() != self.manual_turns)
                {
                    return Err(ProcessorError::new(
                        "changing turn detection mode requires a new realtime processor",
                    ));
                }
            }
        }
        let session_update = match &action {
            Action::UpdateSession(session) => Some((
                session.instructions.clone(),
                session.tools.is_some(),
                session.tool_choice.is_some(),
            )),
            _ => None,
        };
        self.sender()?.try_send(action).map_err(processor_error)?;
        if let Some((instructions, tools, choice)) = session_update {
            let mut state = self
                .context
                .lock()
                .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))?;
            state.session = None;
            state.tools_controlled |= tools;
            state.choice_controlled |= choice;
            if let Some(instructions) = instructions {
                state.instructions_override = Some(instructions);
            }
        }
        Ok(())
    }
    fn context(
        &self,
        context: &LlmContextData,
        state_only: bool,
        run: bool,
        continuation: bool,
        cause: Option<Arc<NativeRealtimeContextCause>>,
        mut fence: Option<InvocationFence>,
    ) -> Result<(), ProcessorError> {
        let admission = if cause
            .as_ref()
            .is_some_and(|cause| cause.intent() == RealtimeContextIntent::AssistantContinuation)
        {
            &self.assistant_admission
        } else {
            &self.admission
        };
        let mut admission = admission
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime admission lock poisoned"))?;
        let mut explicit_append = None;
        let run = if let Some(cause) = &cause {
            if !admission.check(cause)? {
                return Ok(());
            }
            fence = Some(
                cause
                    .inference_fence()
                    .ok_or_else(|| {
                        ProcessorError::new(
                            "realtime context cause lacks original invocation fence",
                        )
                    })?
                    .clone(),
            );
            match cause.intent() {
                RealtimeContextIntent::Replace => {
                    return Err(ProcessorError::new(
                        "realtime context replacement requires a new session",
                    ))
                }
                RealtimeContextIntent::Append { start, end } => {
                    explicit_append = Some((start, end))
                }
                RealtimeContextIntent::Run => {}
                RealtimeContextIntent::AssistantContinuation => {
                    if !continuation {
                        return Err(ProcessorError::new(
                            "assistant continuation requires upstream direction",
                        ));
                    }
                }
            }
            cause.run_llm()
        } else {
            run
        };
        crate::client::bounded_serialized(
            &context.messages,
            self.client.config().limits.max_message_bytes,
        )
        .map_err(processor_error)?;
        let messages = project(&context.messages)?;
        let mut update = context_session(context)?;
        let mut state = self
            .context
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))?;
        if state.instructions_override.is_some() {
            update.instructions = state.instructions_override.clone();
        }
        // Explicit controls have their own source order. Delayed history causes
        // cannot roll them back, and live caused snapshots are not settings edits.
        if cause.is_some() || continuation {
            if state.initialized || state.observed || state.tools_controlled {
                update.tools = None;
            }
            if state.initialized || state.observed || state.choice_controlled {
                update.tool_choice = None;
            }
        }
        crate::client::bounded_serialized(&update, self.client.config().limits.max_message_bytes)
            .map_err(processor_error)?;
        let mut next = state.clone();
        // This receipt reason is selected by the assistant's existing scheduling
        // policy. The adapter does not duplicate null/error/group eligibility.
        let permission_revision = cause
            .as_ref()
            .and_then(|cause| cause.assistant_native_outcome_id())
            .map(|id| next.outcome_revision(id))
            .transpose()?;
        let mut actions = next.snapshot_with_intent(
            messages,
            state_only,
            self.client.config().limits.max_message_bytes,
            explicit_append,
            cause.is_some(),
        )?;
        if !state_only {
            if next.session.as_ref() != Some(&update)
                && (update.tools.is_some()
                    || update.tool_choice.is_some()
                    || update.instructions.is_some())
            {
                actions.insert(0, Action::UpdateSession(update.clone()));
                next.session = Some(update);
            }
            if run {
                if continuation {
                    if let Some(held) = &next.continuation_owed {
                        if !permission_revision
                            .is_some_and(|revision| revision > held.result_revision)
                        {
                            let permission = fence.as_ref().ok_or_else(|| {
                                ProcessorError::new("assistant continuation lacks invocation fence")
                            })?;
                            fence = Some(held.fence.intersection(permission)?);
                        }
                    }
                }
                if continuation && next.has_open_calls() {
                    let result_revision = permission_revision.unwrap_or_else(|| {
                        next.continuation_owed
                            .as_ref()
                            .map_or(next.result_revision, |held| held.result_revision)
                    });
                    next.continuation_owed = Some(HeldContinuation {
                        fence: fence.clone().ok_or_else(|| {
                            ProcessorError::new("assistant continuation lacks invocation fence")
                        })?,
                        result_revision,
                        reevaluation_queued: false,
                    });
                } else if !continuation
                    || next.result_revision > next.continued_revision
                    || actions
                        .iter()
                        .any(|action| matches!(action, Action::CreateItem { .. }))
                {
                    actions.push(Action::CreateResponse(ResponseOptions::default()));
                    next.continued_revision = next.result_revision;
                    next.continuation_owed = None;
                }
            }
        }
        if !actions.is_empty() {
            self.sender()?
                .try_send_batch_with_fence(actions, fence)
                .map_err(processor_error)?;
        }
        *state = next;
        if let Some(cause) = cause {
            admission.commit(cause);
        }
        Ok(())
    }
    fn result(&self, frame: &Frame) -> Result<Option<InvocationFence>, ProcessorError> {
        let outcome = frame.native_tool_outcome()?.ok_or_else(|| {
            ProcessorError::new(
                "paired realtime tool settlement requires a native terminal receipt",
            )
        })?;
        let mut state = self
            .context
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))?;
        let mut next = state.clone();
        next.validate_call(outcome.call())?;
        let limit = self.client.config().limits.max_message_bytes;
        let frame = &frame.data;
        let action = match frame {
            FrameData::FunctionCallCancel(cancel) => {
                next.cancel(&cancel.tool_call_id, &cancel.function_name, limit)?
            }
            _ => {
                let (name, id, arguments, result, error, is_final) = match frame {
                    FrameData::FunctionCallResult(result) => (
                        &result.function_name,
                        &result.tool_call_id,
                        &result.arguments,
                        &result.result,
                        &result.error,
                        result.is_final,
                    ),
                    FrameData::FunctionCallResultDetailed(result) => (
                        &result.function_name,
                        &result.tool_call_id,
                        &result.arguments,
                        &result.result,
                        &result.error,
                        result.properties.as_ref().is_none_or(|p| p.is_final),
                    ),
                    _ => unreachable!("result dispatch"),
                };
                let content = if let Some(error) = error {
                    json!({"error":error}).to_string()
                } else if result.is_null() {
                    "COMPLETED".into()
                } else {
                    result.to_string()
                };
                next.result(
                    &FunctionCall {
                        function_name: name.clone(),
                        tool_call_id: id.clone(),
                        arguments: arguments.clone(),
                    },
                    content,
                    is_final,
                    limit,
                )?
            }
        };
        let fresh = action.is_some();
        if fresh {
            next.record_outcome(outcome.id(), limit)?;
        }
        if let Some(action) = action {
            self.enqueue(action)?;
        }
        let reevaluate = if fresh && !next.has_open_calls() {
            next.continuation_owed.as_mut().and_then(|held| {
                if held.reevaluation_queued || !held.fence.is_current() {
                    return None;
                }
                held.reevaluation_queued = true;
                Some(held.fence.clone())
            })
        } else {
            None
        };
        *state = next;
        Ok(reevaluate)
    }
    fn append(
        &self,
        messages: &[pipecat_core::ContextMessage],
        run: bool,
        fence: InvocationFence,
    ) -> Result<(), ProcessorError> {
        let mut state = self
            .context
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))?;
        let mut next = state.clone();
        let mut actions = next.explicit_append(
            project(messages)?,
            self.client.config().limits.max_message_bytes,
        )?;
        if run {
            actions.push(Action::CreateResponse(ResponseOptions::default()));
        }
        if !actions.is_empty() {
            self.sender()?
                .try_send_batch_with_fence(actions, Some(fence))
                .map_err(processor_error)?;
        }
        *state = next;
        Ok(())
    }
    fn close(&self) -> Result<(), ProcessorError> {
        let sender = {
            self.sender
                .lock()
                .map_err(|_| ProcessorError::fatal("realtime state lock poisoned"))?
                .take()
        };
        if let Some(sender) = sender {
            sender.close();
        }
        self.playback
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime playback lock poisoned"))?
            .take();
        *self
            .context
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))? =
            ContextState::default();
        self.audio_items
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime audio items lock poisoned"))?
            .clear();
        self.initial
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime seed lock poisoned"))?
            .take();
        *self
            .admission
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime admission lock poisoned"))? =
            RealtimeContextAdmission::default();
        *self
            .assistant_admission
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime assistant admission lock poisoned"))? =
            RealtimeContextAdmission::default();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for OpenAiRealtimeProcessor {
    fn name(&self) -> &str {
        "OpenAiRealtimeService"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(ProcessorError::new(
                "realtime processors are single-session; construct a new processor",
            ));
        }
        if config.audio_in_sample_rate != 24_000 || config.audio_out_sample_rate != 24_000 {
            return Err(ProcessorError::new("realtime pipeline requires 24000 Hz input and output; resample upstream/downstream"));
        }
        let mut session = self.client.connect().await.map_err(processor_error)?;
        let sender = session.sender();
        *self
            .sender
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime state lock poisoned"))? =
            Some(sender.clone());
        let initial = self
            .initial
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime seed lock poisoned"))?
            .take();
        if let Some(initial) = initial {
            self.context(&initial, false, false, false, None, None)?;
        }
        let output = ctx.clone();
        let user_id = self.user_id.clone();
        let context = self.context.clone();
        let audio_items = self.audio_items.clone();
        let limit = self.client.config().limits.max_message_bytes;
        let ready = self.receive_ready.clone();
        ctx.spawn("openai-realtime-receive", false, async move {
            ready.notified().await;
            let mut mapper = RealtimeFrameMapper::new(user_id);
            let mut generation = sender.generation();
            let result = async {
                while let Some(result) = session.next_event().await {
                    let event = result.map_err(processor_error)?;
                    if let Event::Error { error } = &event.event {
                        return Err(ProcessorError::fatal(format!(
                            "realtime provider rejected session state: {}",
                            error.message
                        )));
                    }
                    if generation != sender.generation() {
                        mapper.reset();
                        generation = sender.generation();
                    }
                    if event.event.response_id().is_some() && event.generation != generation {
                        continue;
                    }
                    let fence = event
                        .event
                        .response_id()
                        .and_then(|id| sender.response_fence(id));
                    let observed = sender
                        .with_current_generation(event.generation, || {
                            // Keep the same lock order as context enqueue and receipt
                            // acknowledgement: local state first, raw runtime gates last.
                            let mut state = context.lock().map_err(|_| {
                                ProcessorError::fatal("realtime context lock poisoned")
                            })?;
                            let mut items = audio_items.lock().map_err(|_| {
                                ProcessorError::fatal("realtime audio items lock poisoned")
                            })?;
                            let mut observe = || {
                                if let Event::AudioDelta {
                                    item_id,
                                    content_index,
                                    audio,
                                    ..
                                } = &event.event
                                {
                                    items.retain(|_, item| item.generation == generation);
                                    let key = (item_id.clone(), *content_index);
                                    if !items.contains_key(&key) && items.len() >= 64 {
                                        return Err(ProcessorError::new(
                                            "realtime playback item bound exceeded",
                                        ));
                                    }
                                    let item = items.entry(key).or_insert_with(|| AudioItem {
                                        generation,
                                        bytes: 0,
                                        fence: fence.clone(),
                                    });
                                    item.bytes = item
                                        .bytes
                                        .checked_add(audio.len() as u64)
                                        .ok_or_else(|| {
                                            ProcessorError::new("realtime audio duration overflow")
                                        })?;
                                }
                                state.observe(&event.event, limit)?;
                                mapper.frames(&event).map_err(processor_error)
                            };
                            match fence.as_ref() {
                                Some(fence) => fence.with_current(observe).transpose(),
                                None => observe().map(Some),
                            }
                        })
                        .transpose()?
                        .flatten();
                    let Some(frames) = observed else {
                        continue;
                    };
                    for frame in frames {
                        if let FrameData::Error(error) = frame.data {
                            output.push_error(error).await?;
                            continue;
                        }
                        let direction = if matches!(
                            frame.data,
                            FrameData::Transcription(_)
                                | FrameData::InterimTranscription(_)
                                | FrameData::ProposedUserStartedSpeaking
                                | FrameData::ProposedUserStoppedSpeaking
                        ) {
                            Direction::Upstream
                        } else {
                            Direction::Downstream
                        };
                        if event.event.response_id().is_some() {
                            if let Some(result) = sender
                                .while_current(event.generation, async {
                                    match fence.as_ref() {
                                        Some(fence) => output.try_push_with_invocation_fence(
                                            frame, direction, fence,
                                        ),
                                        None => output.try_push(frame, direction),
                                    }
                                })
                                .await
                            {
                                result?;
                            }
                        } else {
                            output.push(frame, direction).await?;
                        }
                    }
                }
                Ok(())
            }
            .await;
            // Always close the owned socket, including mapping/admission failures.
            let shutdown = session.shutdown().await.map_err(processor_error);
            result.and(shutdown)
        })?;
        Ok(())
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        // Validate before even looking at the state-only marker. A copied or
        // modified sealed frame must never acquire fresh inference authority.
        let cause = frame.take_native_realtime_context_cause()?;
        if let Some(cause) = &cause {
            cause.validate_direction(direction)?;
        } else if direction == Direction::Upstream
            && matches!(
                frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
            && !frame.is_realtime_context_update()
        {
            return Err(ProcessorError::new(
                "upstream realtime inference requires original assistant continuation cause",
            ));
        }
        frame.native_tool_batch()?;
        project_timeout(&mut frame)?;
        if matches!(
            frame.data,
            FrameData::Start(_) | FrameData::StartDetailed(_)
        ) {
            ctx.push(frame, direction).await?;
            let external = UserTurnStrategy::External {
                enable_interruptions: true,
            };
            ctx.broadcast(Frame::new(FrameData::LlmServiceMetadata(
                LlmServiceMetadataFrame {
                    base: ServiceMetadataFrame {
                        service_name: self.name().into(),
                        user_turn_strategies: (!self.manual_turns).then(|| UserTurnStrategies {
                            start: Some(vec![external.clone()]),
                            stop: Some(vec![external]),
                        }),
                    },
                    is_realtime_service: true,
                },
            )))
            .await?;
            self.receive_ready.notify_one();
            return Ok(());
        }
        match &frame.data {
            FrameData::Interruption => {
                let sender = self.sender()?;
                let generation = sender.generation();
                let position = self
                    .playback
                    .lock()
                    .map_err(|_| ProcessorError::fatal("realtime playback lock poisoned"))?
                    .take()
                    .filter(|(epoch, _)| *epoch == generation)
                    .map(|(_, position)| position);
                sender.interrupt(position);
                self.context
                    .lock()
                    .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))?
                    .interrupted();
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) | FrameData::Stop => {
                self.close()?
            }
            FrameData::BotStoppedSpeaking => {
                self.playback
                    .lock()
                    .map_err(|_| ProcessorError::fatal("realtime playback lock poisoned"))?
                    .take();
                self.audio_items
                    .lock()
                    .map_err(|_| ProcessorError::fatal("realtime audio items lock poisoned"))?
                    .clear();
            }
            FrameData::LlmContext(context) => {
                frame
                    .require_no_native_llm_continuation()
                    .map_err(|e| ProcessorError::new(e.to_string()))?;
                self.context(
                    &context.clone().into(),
                    frame.is_realtime_context_update(),
                    true,
                    direction == Direction::Upstream,
                    cause,
                    Some(ctx.invocation_fence()?),
                )?;
                return Ok(());
            }
            FrameData::LlmContextDetailed(context) => {
                frame
                    .require_no_native_llm_continuation()
                    .map_err(|e| ProcessorError::new(e.to_string()))?;
                if context.speculation {
                    return Err(ProcessorError::new(
                        "speculative realtime context is unsupported",
                    ));
                }
                self.context(
                    &context.context,
                    frame.is_realtime_context_update(),
                    true,
                    direction == Direction::Upstream,
                    cause,
                    Some(ctx.invocation_fence()?),
                )?;
                return Ok(());
            }
            FrameData::FunctionCallResult(_)
            | FrameData::FunctionCallResultDetailed(_)
            | FrameData::FunctionCallCancel(_) => {
                if matches!(frame.data, FrameData::FunctionCallCancel(_))
                    && frame.native_tool_outcome()?.is_none()
                {
                    // A request belongs to the executor; only its admitted
                    // native terminal acknowledgment settles provider history.
                    return ctx.push(frame, direction).await;
                }
                if let Some(original) = self.result(&frame)? {
                    // Admission is checked now, but the held original also
                    // remains in ContextState until a permission returns and
                    // its intersection reaches actual socket start_send.
                    ctx.try_push_with_invocation_fence(
                        Frame::new(FrameData::LlmRun),
                        Direction::Downstream,
                        &original,
                    )?;
                }
                if direction == Direction::Upstream {
                    return Ok(());
                }
            }
            _ => {}
        }
        if direction == Direction::Downstream {
            match &frame.data {
                FrameData::InputImage(_)
                | FrameData::LlmConfigureOutput { .. }
                | FrameData::LlmEnablePromptCaching { .. }
                | FrameData::LlmSummarizeContext(_)
                | FrameData::LlmContextSummaryRequest(_) => {
                    return Err(ProcessorError::new("this realtime adapter does not support image, output-mode, prompt-cache or summarization controls"));
                }
                FrameData::InputAudio(audio)
                | FrameData::UserAudio(pipecat_core::UserAudioRawFrame { audio, .. }) => {
                    audio
                        .validate()
                        .map_err(|_| ProcessorError::new("invalid realtime PCM"))?;
                    if audio.sample_rate != 24_000 || audio.num_channels != 1 {
                        return Err(ProcessorError::new(
                            "realtime requires mono PCM16 at 24000 Hz",
                        ));
                    }
                    self.enqueue(Action::AppendAudio(audio.audio.clone()))?;
                }
                FrameData::UserStoppedSpeaking if self.manual_turns => self
                    .sender()?
                    .try_send_batch_with_fence(
                        vec![
                            Action::CommitAudio,
                            Action::CreateResponse(ResponseOptions::default()),
                        ],
                        Some(ctx.invocation_fence()?),
                    )
                    .map_err(processor_error)?,
                FrameData::InputText(text) => {
                    if !text.append_to_context {
                        return Err(ProcessorError::new(
                            "realtime InputText requires append_to_context",
                        ));
                    }
                    let mut state = self
                        .context
                        .lock()
                        .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))?;
                    let mut next = state.clone();
                    let mut actions = next.explicit_input(
                        &text.text,
                        self.client.config().limits.max_message_bytes,
                    )?;
                    actions.push(Action::CreateResponse(ResponseOptions {
                        context_id: Some(format!("pc_turn_{}", frame.id)),
                        ..Default::default()
                    }));
                    self.sender()?
                        .try_send_batch_with_fence(actions, Some(ctx.invocation_fence()?))
                        .map_err(processor_error)?;
                    *state = next;
                    return Ok(());
                }
                FrameData::LlmRun => {
                    self.sender()?
                        .try_send_batch_with_fence(
                            vec![Action::CreateResponse(ResponseOptions::default())],
                            Some(ctx.invocation_fence()?),
                        )
                        .map_err(processor_error)?;
                    return Ok(());
                }
                FrameData::LlmMessagesAppend(messages) | FrameData::LlmMessagesUpdate(messages) => {
                    if matches!(frame.data, FrameData::LlmMessagesAppend(_)) {
                        self.append(
                            &messages
                                .messages
                                .iter()
                                .cloned()
                                .map(Into::into)
                                .collect::<Vec<_>>(),
                            messages.run_llm,
                            ctx.invocation_fence()?,
                        )?;
                        return Ok(());
                    }
                    let mut history = if matches!(frame.data, FrameData::LlmMessagesAppend(_)) {
                        self.context
                            .lock()
                            .map_err(|_| ProcessorError::fatal("realtime context lock poisoned"))?
                            .history
                            .clone()
                    } else {
                        vec![]
                    };
                    history.extend(messages.messages.clone());
                    self.context(
                        &LlmContextData {
                            messages: history.into_iter().map(Into::into).collect(),
                            tools: ToolSet::NotGiven,
                            tool_choice: None,
                        },
                        false,
                        messages.run_llm,
                        false,
                        None,
                        Some(ctx.invocation_fence()?),
                    )?;
                    return Ok(());
                }
                FrameData::LlmMessagesAppendDetailed(messages)
                | FrameData::LlmMessagesUpdateDetailed(messages) => {
                    if matches!(frame.data, FrameData::LlmMessagesAppendDetailed(_)) {
                        self.append(
                            &messages.messages,
                            messages.run_llm == Some(true),
                            ctx.invocation_fence()?,
                        )?;
                        return Ok(());
                    }
                    let mut history =
                        if matches!(frame.data, FrameData::LlmMessagesAppendDetailed(_)) {
                            self.context
                                .lock()
                                .map_err(|_| {
                                    ProcessorError::fatal("realtime context lock poisoned")
                                })?
                                .history
                                .iter()
                                .cloned()
                                .map(Into::into)
                                .collect()
                        } else {
                            vec![]
                        };
                    history.extend(messages.messages.clone());
                    self.context(
                        &LlmContextData {
                            messages: history,
                            tools: ToolSet::NotGiven,
                            tool_choice: None,
                        },
                        false,
                        messages.run_llm == Some(true),
                        false,
                        None,
                        Some(ctx.invocation_fence()?),
                    )?;
                    return Ok(());
                }
                FrameData::LlmMessagesTransform(_) => {
                    return Err(ProcessorError::new(
                        "realtime message transforms require a new session",
                    ))
                }
                FrameData::LlmSetTools(tools) => {
                    self.enqueue(Action::UpdateSession(context_session(&LlmContextData {
                        messages: vec![],
                        tools: tools.clone(),
                        tool_choice: None,
                    })?))?
                }
                FrameData::LlmSetToolChoice(choice) => {
                    self.enqueue(Action::UpdateSession(context_session(&LlmContextData {
                        messages: vec![],
                        tools: ToolSet::NotGiven,
                        tool_choice: Some(choice.clone()),
                    })?))?
                }
                FrameData::LlmUpdateSettings(settings) => {
                    let targets_self = match &settings.service {
                        None => true,
                        Some(pipecat_core::ProcessorReference::Name(name)) => name == self.name(),
                        Some(pipecat_core::ProcessorReference::Local(target)) => {
                            std::ptr::addr_eq(target.0.as_ref(), self as &dyn FrameProcessor)
                        }
                    };
                    if !targets_self {
                        return ctx.push(frame, direction).await;
                    }
                    if settings
                        .settings
                        .keys()
                        .any(|key| key != "session_properties")
                        || (settings.delta.is_some() && !settings.settings.is_empty())
                    {
                        return Err(ProcessorError::new("realtime settings require one typed delta or session_properties update"));
                    }
                    let mut update = SessionProperties::new();
                    if let Some(delta) = &settings.delta {
                        let mut unsupported = delta.clone();
                        unsupported.base.model = Setting::NotGiven;
                        unsupported.system_instruction = Setting::NotGiven;
                        unsupported.max_tokens = Setting::NotGiven;
                        if unsupported != pipecat_core::LlmSettings::default()
                            || matches!(delta.base.model, Setting::Null)
                            || matches!(delta.max_tokens, Setting::Null)
                        {
                            return Err(ProcessorError::new(
                                "unsupported realtime setting or reset",
                            ));
                        }
                        if let Setting::Value(model) = &delta.base.model {
                            update.model = Some(model.clone());
                        }
                        match &delta.system_instruction {
                            Setting::Value(text) => update.instructions = Some(text.clone()),
                            Setting::Null => update.instructions = Some(String::new()),
                            _ => {}
                        }
                        if let Setting::Value(tokens) = delta.max_tokens {
                            update.max_output_tokens = Some(TokenLimit::Tokens(
                                u32::try_from(tokens)
                                    .map_err(|_| ProcessorError::new("max tokens overflow"))?,
                            ));
                        }
                    }
                    if let Some(properties) = settings.settings.get("session_properties") {
                        update = serde_json::from_value(properties.clone()).map_err(|_| {
                            ProcessorError::new("invalid typed realtime session properties")
                        })?;
                    }
                    self.enqueue(Action::UpdateSession(update))?;
                }
                _ => {}
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Ok(sender) = self.sender() {
            sender.finish().await.map_err(processor_error)?;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.close()
    }
}

/// Place immediately after a native tool executor and before delivery/assistant.
/// Results reach the provider before the assistant requests an upstream response;
/// continuation remains owned by the assistant's activity and playback gates.
#[derive(Default)]
pub struct OpenAiRealtimeToolResultRelay;
impl OpenAiRealtimeToolResultRelay {
    pub fn new() -> Self {
        Self
    }
}
#[async_trait]
impl FrameProcessor for OpenAiRealtimeToolResultRelay {
    fn name(&self) -> &str {
        "OpenAiRealtimeToolResultRelay"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame.native_tool_batch()?;
        project_timeout(&mut frame)?;
        if direction == Direction::Downstream
            && (matches!(
                frame.data,
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_)
            ) || matches!(frame.data, FrameData::FunctionCallCancel(_))
                && frame.native_tool_outcome()?.is_some())
        {
            ctx.push(frame.clone(), Direction::Upstream).await?;
        }
        ctx.push(frame, direction).await
    }
}
fn project_timeout(frame: &mut Frame) -> Result<(), ProcessorError> {
    if matches!(
        frame.data,
        FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_)
    ) && frame.native_tool_outcome()?.is_none()
    {
        return Err(ProcessorError::new(
            "paired realtime tool settlement requires a native terminal receipt",
        ));
    }
    if frame
        .native_tool_outcome()?
        .is_some_and(|outcome| outcome.reason() == pipecat_core::ToolCallEndReason::TimedOut)
        && !matches!(frame.data, FrameData::FunctionCallCancel(_))
    {
        frame.project_native_tool_timeout_as_cancel()?;
    }
    Ok(())
}
/// Deterministic GA event-to-frame mapping, including split PCM samples and one
/// TTS lifecycle pair per response even when it contains several audio items.
#[derive(Default)]
pub struct RealtimeFrameMapper {
    user_id: String,
    speaking: Option<String>,
    partial_samples: HashMap<(String, u32), u8>,
}
impl RealtimeFrameMapper {
    pub fn new(user_id: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            ..Self::default()
        }
    }
    pub fn reset(&mut self) {
        self.speaking = None;
        self.partial_samples.clear();
    }
    pub fn frames(&mut self, e: &RealtimeEvent) -> Result<Vec<Frame>, RealtimeError> {
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
                if pcm.len() % 2 != 0 {
                    if self.partial_samples.len() >= 64 {
                        return Err(RealtimeError::TooLarge);
                    }
                    self.partial_samples
                        .insert(key, pcm.pop().expect("odd buffer is nonempty"));
                }
                if !pcm.is_empty() {
                    frames.push(Frame::new(FrameData::TtsAudio(AudioRawFrame {
                        audio: pcm,
                        sample_rate: 24_000,
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
            Event::TextDelta { delta, .. } => {
                frames.push(Frame::new(FrameData::LlmText(TextFrame::llm(delta))))
            }
            Event::AudioTranscriptDelta {
                delta, response_id, ..
            } => {
                let mut notification = TextFrame::llm(delta);
                notification.append_to_context = false;
                frames.push(Frame::new(FrameData::LlmText(notification)));
                frames.push(Frame::new(FrameData::TtsText(
                    pipecat_core::AggregatedTextFrame {
                        text: TextFrame::llm(delta),
                        aggregated_by: "sentence".into(),
                        context_id: e.context_id.clone().or_else(|| Some(response_id.clone())),
                        raw_text: None,
                        will_be_spoken: true,
                    },
                )));
            }
            Event::InputTranscriptionDelta { delta, .. } => frames.push(Frame::new(
                FrameData::InterimTranscription(TranscriptionFrame {
                    text: delta.clone(),
                    user_id: self.user_id.clone(),
                    ..Default::default()
                }),
            )),
            Event::InputTranscriptionCompleted { transcript, .. } => {
                frames.push(Frame::new(FrameData::Transcription(TranscriptionFrame {
                    text: transcript.clone(),
                    user_id: self.user_id.clone(),
                    finalized: true,
                    ..Default::default()
                })))
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
            Event::ResponseDone { response } => {
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
                if let Some(usage) = &response.usage {
                    frames.push(Frame::new(FrameData::Metrics(vec![Metric::LlmUsage {
                        processor: "OpenAiRealtimeService".into(),
                        prompt_tokens: usage.input_tokens,
                        completion_tokens: usage.output_tokens,
                        total_tokens: usage.total_tokens,
                    }])));
                }
                if response.status == ResponseStatus::Failed {
                    frames.push(Frame::new(FrameData::Error(ErrorFrame::new(
                        response
                            .status_details
                            .as_ref()
                            .and_then(|d| d.error.as_ref())
                            .map(|e| e.message.clone())
                            .unwrap_or_else(|| "realtime response failed".into()),
                    ))));
                }
                frames.push(Frame::new(FrameData::LlmResponseEnd));
            }
            Event::Error { error } | Event::InputTranscriptionFailed { error, .. } => frames.push(
                Frame::new(FrameData::Error(ErrorFrame::new(error.message.clone()))),
            ),
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
                | Event::TextDelta {
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
fn processor_error(error: RealtimeError) -> ProcessorError {
    ProcessorError::new(error.to_string()).with_category(match error {
        RealtimeError::Invalid(_) | RealtimeError::TooLarge | RealtimeError::UnknownToolCall => {
            ErrorCategory::InvalidRequest
        }
        _ => ErrorCategory::Unknown,
    })
}
