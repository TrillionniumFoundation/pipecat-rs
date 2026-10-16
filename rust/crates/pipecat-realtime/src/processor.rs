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
use tokio::time::Instant;
struct AudioItem {
    generation: u64,
    bytes: u64,
    fence: Option<InvocationFence>,
}
type AudioItems = Arc<Mutex<HashMap<(String, u32), AudioItem>>>;

#[derive(Default)]
struct VideoInput {
    last_sample: Option<Instant>,
    sequence: u64,
}

/// Single-session PCM adapter. Place a paired user aggregator before this service
/// and its assistant after delivery. A tool executor needs the result relay below.
///
/// Raw camera input is bounded packed RGB with one-second sampling and detail
/// "auto". Encoding is owned but awaited in the System lane: local controls
/// wait for that bounded work, with no hard cancellation deadline. Interruption
/// retires an image once admitted to this service queue; queued Cancel alone
/// does not, so a preceding image may write before Cancel is processed.
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
    audio_preroll_override: bool,
    video: crate::video::VideoEncoder,
    video_input: Mutex<VideoInput>,
    video_paused: AtomicBool,
    start_video_paused: bool,
    video_closed: AtomicBool,
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
        if manual_turns && client.config().limits.max_audio_chunk_bytes < 2 {
            return Err(RealtimeError::Invalid(
                "manual PCM preroll requires a full sample per audio chunk",
            ));
        }
        // The paired user's configured strategies own pipeline interruptions.
        client.pipeline_turns = true;
        client.audio_preroll_bytes = manual_turns.then_some(24_000);
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
            audio_preroll_override: false,
            video: Default::default(),
            video_input: Default::default(),
            video_paused: AtomicBool::new(false),
            start_video_paused: false,
            video_closed: AtomicBool::new(false),
        })
    }
    /// Initial camera pause state. The processor remains single-session.
    pub fn with_video_input_paused(mut self, paused: bool) -> Self {
        self.start_video_paused = paused;
        self.video_paused = AtomicBool::new(paused);
        self
    }
    /// Pause new camera samples. Already admitted encoding/queued work is not
    /// retracted; unpausing neither resets the one-second sampling window nor
    /// reopens a closed processor. This subset uses image detail "auto".
    pub fn set_video_input_paused(&self, paused: bool) {
        self.video_paused.store(paused, Ordering::SeqCst);
    }
    async fn send_video(
        &self,
        image: &pipecat_core::ImageRawFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.video_paused.load(Ordering::SeqCst) || self.video_closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        let sender = self.sender()?;
        let generation = sender.generation();
        let fence = ctx.invocation_fence()?;
        let selected = fence
            .with_current(|| {
                if self.video_closed.load(Ordering::SeqCst)
                    || self.video_paused.load(Ordering::SeqCst)
                {
                    return Ok(None);
                }
                let mut state = self
                    .video_input
                    .lock()
                    .map_err(|_| ProcessorError::fatal("realtime video input lock poisoned"))?;
                let now = Instant::now();
                if state.last_sample.is_some_and(|last| {
                    now.duration_since(last) < std::time::Duration::from_secs(1)
                }) {
                    return Ok(None);
                }
                let sequence = state
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| ProcessorError::fatal("realtime camera identity exhausted"))?;
                // Source sampling advances before image conversion; invalid/failed
                // samples still occupy this interval. Pause never resets it.
                state.last_sample = Some(now);
                state.sequence = sequence;
                Ok::<_, ProcessorError>(Some(format!("pc_camera_{sequence}")))
            })
            .transpose()?
            .flatten();
        let Some(id) = selected else {
            return Ok(());
        };
        let limit = self.client.config().limits.max_message_bytes;
        let reject = |error| processor_error(error).with_permanence_override(false);
        let max_jpeg = crate::video::jpeg_budget(limit).map_err(reject)?;
        let jpeg = self.video.encode(image, max_jpeg).await.map_err(reject)?;
        if self.video_closed.load(Ordering::SeqCst)
            || generation != sender.generation()
            || !fence.is_current()
        {
            return Ok(());
        }
        let action = crate::video::camera_action(jpeg, id).map_err(reject)?;
        sender
            .try_send_camera(action, generation, fence)
            .map_err(reject)?;
        Ok(())
    }
    /// Pin the manual-turn PCM16/24000mono replay suffix, overriding VAD auto-size.
    /// Requires a finite positive duration of at least one sample and at most 5 s.
    /// Server-VAD sessions remain unchanged. Captured bytes reflect local flush,
    /// not provider ACK; interruption preserves the rolling suffix across commits.
    pub fn with_user_audio_preroll_secs(mut self, seconds: f64) -> Result<Self, RealtimeError> {
        let bytes = crate::client::preroll_bytes_for_duration(seconds)?;
        if self.manual_turns {
            self.client.audio_preroll_bytes = Some(bytes);
        }
        self.audio_preroll_override = true;
        Ok(self)
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
        self.video_closed.store(true, Ordering::SeqCst);
        self.video.cancel();
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
        self.video.reset().await.map_err(processor_error)?;
        self.video_paused
            .store(self.start_video_paused, Ordering::SeqCst);
        self.video_closed.store(false, Ordering::SeqCst);
        *self
            .video_input
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime video input lock poisoned"))? =
            VideoInput::default();
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
            FrameData::SpeechControlParams(params)
                if self.manual_turns && !self.audio_preroll_override =>
            {
                if let Some(vad) = &params.vad_params {
                    if !vad.start_secs.is_finite() || vad.start_secs < 0.0 {
                        return Err(processor_error(RealtimeError::Invalid(
                            "invalid VAD start duration",
                        )));
                    }
                    let bytes = crate::client::preroll_bytes_for_duration(vad.start_secs + 0.3)
                        .map_err(processor_error)?;
                    self.sender()?.resize_audio_preroll(bytes);
                }
            }
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
                FrameData::InputImage(image) => self.send_video(image, ctx).await?,
                FrameData::UserImage(image) => {
                    // Reject rich/callback authority before pause or sampling can
                    // hide it. A passive camera sample is consumed here: forwarding
                    // it would enter the paired assistant's rich-image handler.
                    frame
                        .require_no_native_llm_continuation()
                        .map_err(|error| ProcessorError::new(error.to_string()))?;
                    if image.append_to_context == Some(true)
                        || image.run_llm == Some(true)
                        || image.request.as_ref().is_some_and(|request| {
                            request.append_to_context == Some(true)
                                || request.function_name.is_some()
                                || request.tool_call_id.is_some()
                                || request.result_callback.is_some()
                        })
                    {
                        return Err(ProcessorError::new(
                            "realtime camera requires passive UserImage without context, inference or tool/callback requests",
                        ));
                    }
                    self.send_video(&image.image, ctx).await?;
                    return Ok(());
                }
                FrameData::LlmConfigureOutput { .. }
                | FrameData::LlmEnablePromptCaching { .. }
                | FrameData::LlmSummarizeContext(_)
                | FrameData::LlmContextSummaryRequest(_) => {
                    return Err(ProcessorError::new(
                        "this realtime adapter does not support output-mode, prompt-cache or summarization controls",
                    ));
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
        self.video_closed.store(true, Ordering::SeqCst);
        // Stop does not run cleanup. A dropped encode waiter still owns its
        // blocking job here, which must join before this terminal completes.
        let encoded = self.video.finish().await.map_err(processor_error);
        if let Ok(sender) = self.sender() {
            sender.finish().await.map_err(processor_error)?;
        }
        encoded
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let closed = self.close();
        let encoded = self.video.finish().await.map_err(processor_error);
        closed?;
        encoded
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

#[cfg(test)]
mod camera_ownership_tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use pipecat_core::{ImageRawFrame, Pipeline, PipelineConfig, WorkerState};
    use std::time::Duration;
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::{error::ProtocolError, Message};

    #[derive(Clone, Copy, Debug)]
    enum Terminal {
        ProviderEnd,
        RuntimeEnd,
        Stop,
        Cancel,
    }

    async fn blocked_encoding_terminal(terminal: Terminal) {
        let wait = Duration::from_secs(5);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("camera-owned-encode-fixture");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        config.limits.operation_timeout = Duration::from_secs(2);
        let mut peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let mut ws = tokio_tungstenite::accept_async(tcp)
                .await
                .map_err(|e| e.to_string())?;
            ws.next()
                .await
                .ok_or("missing session update")?
                .map_err(|e| e.to_string())?;
            ws.send(Message::Text(json!({"event_id":"camera-ready","type":"session.updated","session":{"id":"camera-owned-session","type":"realtime"}}).to_string().into())).await.map_err(|e| e.to_string())?;
            let mut messages = Vec::<serde_json::Value>::new();
            while let Some(message) = ws.next().await {
                let message = match message {
                    Err(tokio_tungstenite::tungstenite::Error::Protocol(
                        ProtocolError::ResetWithoutClosingHandshake,
                    )) if matches!(terminal, Terminal::Cancel) => return Ok(messages),
                    result => result.map_err(|e| e.to_string())?,
                };
                match message {
                    Message::Close(_) => {
                        match ws.flush().await {
                            Ok(())
                            | Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed) => {}
                            Err(error) => return Err(error.to_string()),
                        }
                        return Ok(messages);
                    }
                    Message::Text(text) => {
                        if messages.len() >= 16 {
                            return Err("unbounded camera fixture traffic".into());
                        }
                        messages.push(serde_json::from_str(&text).map_err(|e| e.to_string())?);
                    }
                    _ => {}
                }
            }
            if matches!(terminal, Terminal::Cancel) {
                Ok::<_, String>(messages)
            } else {
                Err("graceful camera terminal ended without Close".into())
            }
        });
        let processor =
            Arc::new(OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
        let gate = processor.video.test_gate();
        let created = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_secs(2),
                start: StartFrame {
                    audio_in_sample_rate: 24_000,
                    audio_out_sample_rate: 24_000,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await;
        let mut worker = match created {
            Ok(worker) => worker,
            Err(error) => {
                gate.release();
                peer.abort();
                let _ = peer.await;
                panic!("camera pipeline start: {error}");
            }
        };
        let observed = timeout(wait, async {
            while !worker
                .recv()
                .await
                .ok_or("missing Start")?
                .frame
                .data
                .is_start()
            {}
            worker
                .sender()
                .send(
                    Frame::new(FrameData::InputImage(ImageRawFrame {
                        image: vec![31; 12],
                        size: (2, 2),
                        format: Some("RGB".into()),
                    })),
                    Direction::Downstream,
                )
                .await
                .map_err(|e| e.to_string())?;
            while !gate.entered() {
                tokio::task::yield_now().await;
            }
            match terminal {
                Terminal::ProviderEnd => {
                    processor
                        .sender()
                        .map_err(|e| e.to_string())?
                        .interrupt(None);
                    worker.end().await.map_err(|e| e.to_string())?;
                }
                Terminal::RuntimeEnd | Terminal::Stop => {
                    worker
                        .sender()
                        .send(Frame::interruption(), Direction::Downstream)
                        .await
                        .map_err(|e| e.to_string())?;
                    // This fixture has the service at index 0. Returning from
                    // send proves Interruption reached this exact service queue
                    // and retired its raw fence, despite the occupied System lane.
                    if matches!(terminal, Terminal::Stop) {
                        worker
                            .sender()
                            .send(Frame::new(FrameData::Stop), Direction::Downstream)
                            .await
                            .map_err(|e| e.to_string())?;
                    } else {
                        worker.end().await.map_err(|e| e.to_string())?;
                    }
                    // System image processing is FIFO and is not preempted.
                    // The normal End/Stop lane also waits for System completion.
                }
                Terminal::Cancel => {
                    worker
                        .cancel("cancel blocked camera encoder")
                        .await
                        .map_err(|e| e.to_string())?;
                    // Cancel admission closes pipeline input, but its service
                    // callback cannot run until this System image completes.
                }
            }
            Ok::<_, String>(
                !worker.has_finished()
                    && !gate.exited()
                    && !processor.video_closed.load(Ordering::SeqCst),
            )
        })
        .await;
        // Release even on observation failure; never strand a blocking pool job.
        gate.release();
        if !matches!(observed, Ok(Ok(true))) {
            let _ = timeout(wait, worker.cancel("camera observation failed")).await;
        }
        let mut join_timeout = None;
        let joined = match timeout(wait, worker.join()).await {
            Ok(result) => result,
            Err(error) => {
                let _ = timeout(wait, worker.cancel("camera join deadline")).await;
                join_timeout = Some(error.to_string());
                worker.join().await
            }
        };
        let remote = match timeout(wait, &mut peer).await {
            Ok(result) => result,
            Err(error) => {
                peer.abort();
                let _ = peer.await;
                panic!("camera peer deadline: {error}");
            }
        };
        assert!(
            matches!(observed, Ok(Ok(true))),
            "{terminal:?}: {observed:?}"
        );
        assert!(
            join_timeout.is_none(),
            "camera join deadline: {join_timeout:?}"
        );
        let report = joined.unwrap();
        assert_eq!(
            report.state,
            match terminal {
                Terminal::Stop => WorkerState::Stopped,
                Terminal::Cancel => WorkerState::Cancelled,
                _ => WorkerState::Ended,
            }
        );
        assert_eq!(report.total_errors, 0, "{:?}", report.errors);
        assert!(
            gate.exited(),
            "blocking job must exit before terminal completes"
        );
        assert!(
            processor.video.test_idle().await,
            "the terminal must leave no owned blocking encoder job"
        );
        let wire = remote.unwrap().unwrap();
        let camera_count = wire
            .iter()
            .filter(|value| value["type"] == "conversation.item.create")
            .count();
        assert!(
            !wire.iter().any(|value| value["type"] == "response.create"),
            "unexpected inference: {wire:?}"
        );
        if matches!(terminal, Terminal::Cancel) {
            // Cancel does not retire the raw fence at queue admission. The
            // preceding image can write once before the Cancel callback closes
            // the sender; there is no retry or later new image admission.
            assert!(camera_count <= 1, "repeated camera before Cancel: {wire:?}");
        } else {
            assert_eq!(camera_count, 0, "retired camera reached wire: {wire:?}");
        }
    }
    #[tokio::test]
    async fn camera_old_provider_epoch_encoding_is_not_relabelled_after_completion() {
        blocked_encoding_terminal(Terminal::ProviderEnd).await;
    }
    #[tokio::test]
    async fn camera_system_interruption_waits_for_encoding_then_end_joins_owner() {
        blocked_encoding_terminal(Terminal::RuntimeEnd).await;
    }
    #[tokio::test]
    async fn camera_system_encoding_completes_before_stop_without_cleanup() {
        blocked_encoding_terminal(Terminal::Stop).await;
    }
    #[tokio::test]
    async fn camera_cancel_waits_for_system_encoding_then_closes_and_joins() {
        blocked_encoding_terminal(Terminal::Cancel).await;
    }
}
