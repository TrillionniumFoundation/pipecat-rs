use crate::*;
use pipecat_core::{
    async_trait, AggregatedTextFrame, AudioRawFrame, ChatMessage, ChatRole, Direction,
    ErrorCategory, ErrorFrame, Frame, FrameData, FrameProcessor, FunctionCallCancelFrame,
    FunctionCallInProgressFrame, LlmContextFrame, Metric, ProcessorContext, ProcessorError,
    StartFrame, TextFrame, TranscriptionFrame, TtsStartedFrame, TtsStoppedFrame,
};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};
use tokio::time::Instant;

/// Persistent Gemini Live service. Original frames pass through unchanged.
/// PCM is mono; resample explicitly elsewhere in the pipeline. Context seeding
/// is one-time because the server already retains later live conversation.
pub struct GeminiLiveProcessor {
    client: GeminiClient,
    sender: Mutex<Option<GeminiSender>>,
    state: Mutex<InputState>,
    preroll_bytes: usize,
    explicit_preroll: bool,
    user_id: String,
    video: crate::video::VideoEncoder,
    video_paused: AtomicBool,
    start_video_paused: bool,
}
#[derive(Default)]
struct InputState {
    activity: bool,
    stt_muted: bool,
    preroll: VecDeque<u8>,
    preroll_bytes: usize,
    initial_context: Option<LlmContextFrame>,
    last_video: Option<Instant>,
}
impl GeminiLiveProcessor {
    pub fn new(client: GeminiClient) -> Self {
        let preroll_bytes = (client.config().input_sample_rate as usize)
            .min(client.config().limits.max_audio_bytes)
            / 2
            * 2;
        Self {
            client,
            sender: Mutex::new(None),
            state: Mutex::new(InputState {
                preroll_bytes,
                ..Default::default()
            }),
            preroll_bytes,
            explicit_preroll: false,
            user_id: String::new(),
            video: Default::default(),
            video_paused: AtomicBool::new(false),
            start_video_paused: false,
        }
    }
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }
    /// Initial pause state, restored on each setup. Paused frames still pass through.
    pub fn with_video_input_paused(mut self, paused: bool) -> Self {
        self.start_video_paused = paused;
        self.video_paused = AtomicBool::new(paused);
        self
    }
    /// Pause new image admissions without retracting already queued provider work.
    pub fn set_video_input_paused(&self, paused: bool) {
        self.video_paused.store(paused, Ordering::SeqCst);
    }
    async fn send_video(
        &self,
        image: &pipecat_core::ImageRawFrame,
        origin: Origin,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.video_paused.load(Ordering::SeqCst) {
            return Ok(());
        }
        let sender = match self.sender() {
            Ok(sender) => sender,
            Err(_) => return Ok(()),
        };
        if sender.is_finishing() {
            return Ok(());
        }
        let generation = sender.generation();
        let fence = ctx.invocation_fence()?;
        let admitted = fence
            .with_current(|| {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                let now = Instant::now();
                if state.last_video.is_some_and(|last| {
                    now.duration_since(last) < std::time::Duration::from_secs(1)
                }) {
                    return Ok::<bool, ProcessorError>(false);
                }
                state.last_video = Some(now);
                Ok(true)
            })
            .transpose()?
            .unwrap_or(false);
        if !admitted {
            return Ok(());
        }
        let max = self.client.config().limits.max_message_bytes;
        // Reserve exact JSON/base64 overhead before encoding, not after queue admission.
        let overhead = serde_json::to_vec(
            &json!({"realtimeInput":{"video":{"mimeType":"image/jpeg","data":""}}}),
        )
        .map_err(|_| ProcessorError::new("video JSON"))?
        .len();
        let max_jpeg = max.saturating_sub(overhead) / 4 * 3;
        let jpeg = self
            .video
            .encode(image, max_jpeg)
            .await
            .map_err(|e| processor_error(e).with_permanence_override(false))?;
        // The blocking result cannot authorize a retired invocation or provider generation.
        fence
            .with_current(|| sender.try_send_video_at_generation(jpeg, origin, generation))
            .transpose()
            .map_err(processor_error)?;
        Ok(())
    }
    /// Explicit fixed local pre-roll, including zero. This overrides subsequent
    /// SpeechControlParams updates; without an override VAD start time + 100 ms
    /// determines the local window. No provider setting is changed.
    pub fn with_preroll_ms(mut self, milliseconds: u32) -> Result<Self, GeminiError> {
        let bytes =
            u64::from(milliseconds) * u64::from(self.client.config().input_sample_rate) * 2 / 1000;
        if bytes > self.client.config().limits.max_audio_bytes as u64 {
            return Err(GeminiError::TooLarge);
        }
        self.preroll_bytes = bytes as usize / 2 * 2;
        self.explicit_preroll = true;
        self.state = Mutex::new(InputState {
            preroll_bytes: self.preroll_bytes,
            ..Default::default()
        });
        Ok(self)
    }
    fn update_preroll(
        &self,
        control: &pipecat_core::SpeechControlParamsFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let proposal_error = |error| processor_error(error).with_permanence_override(false);
        // Source explicit overrides ignore even malformed controls.
        if self.explicit_preroll {
            return Ok(());
        }
        let Some(vad) = &control.vad_params else {
            return Ok(());
        };
        if !vad.start_secs.is_finite() || vad.start_secs < 0.0 {
            return Err(proposal_error(GeminiError::Invalid(
                "VAD start_secs must be finite and nonnegative",
            )));
        }
        let config = self.client.config();
        let raw_bytes =
            ((vad.start_secs + 0.1) * f64::from(config.input_sample_rate) * 2.0).floor();
        if !raw_bytes.is_finite() || raw_bytes > config.limits.max_audio_bytes as f64 {
            return Err(proposal_error(GeminiError::TooLarge));
        }
        // Preserve whole PCM16 samples, unlike an odd Python byte slice.
        let bytes = raw_bytes as usize / 2 * 2;
        // Validate the exact base64 JSON size without allocating a PCM window.
        let overhead = serde_json::to_vec(&json!({"realtimeInput":{"audio":{"mimeType":format!("audio/pcm;rate={}",config.input_sample_rate),"data":""}}}))
            .map_err(|_| proposal_error(GeminiError::Invalid("JSON")))?.len();
        let encoded = bytes
            .checked_add(2)
            .and_then(|n| (n / 3).checked_mul(4))
            .and_then(|n| n.checked_add(overhead));
        if encoded.is_none_or(|n| n > config.limits.max_message_bytes) {
            return Err(proposal_error(GeminiError::TooLarge));
        }
        ctx.invocation_fence()?
            .with_current(|| {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                state.preroll_bytes = bytes;
                // Shrink immediately rather than retaining excess until the next input.
                while state.preroll.len() > bytes {
                    state.preroll.pop_front();
                }
                Ok(())
            })
            .ok_or_else(|| ProcessorError::new("stale Gemini preroll control"))?
    }
    pub fn sender(&self) -> Result<GeminiSender, ProcessorError> {
        self.sender
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini state lock poisoned"))?
            .clone()
            .ok_or_else(|| ProcessorError::new("Gemini session is not ready"))
    }
    fn enqueue(&self, action: Action, origin: Origin) -> Result<(), ProcessorError> {
        self.sender()?
            .try_send(action, origin)
            .map_err(processor_error)
    }
    fn origin(frame: &Frame) -> Origin {
        Origin {
            frame_id: Some(frame.id),
            context_id: frame
                .metadata
                .get("context_id")
                .and_then(|v| v.as_str())
                .map(str::to_owned),
        }
    }
    fn seed_context(
        &self,
        context: &LlmContextFrame,
        origin: Origin,
    ) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
        if let Some(existing) = &state.initial_context {
            return if existing == context {
                Ok(())
            } else {
                Err(processor_error(GeminiError::Unsupported(
                    "replacing or replaying evolving context; use explicit append-only ClientContent",
                )))
            };
        }
        if context.messages.len() > 1024
            || serde_json::to_vec(context)
                .map_err(|_| ProcessorError::new("invalid context"))?
                .len()
                > self.client.config().limits.max_message_bytes
        {
            return Err(processor_error(GeminiError::TooLarge));
        }
        let configured = &self.client.config().session;
        if !context.tools.is_empty()
            && (context.tools.len() != configured.tools.len()
                || !context.tools.iter().zip(&configured.tools).all(|(a, b)| {
                    a.name == b.name
                        && a.description == b.description
                        && a.parameters == b.parameters
                }))
        {
            return Err(processor_error(GeminiError::Unsupported(
                "tools must be declared before connecting",
            )));
        }
        if context
            .tool_choice
            .as_ref()
            .is_some_and(|choice| *choice != pipecat_core::ToolChoice::Auto)
        {
            return Err(processor_error(GeminiError::Unsupported("tool choice")));
        }
        let mut turns = Vec::new();
        for message in &context.messages {
            if matches!(message.role, ChatRole::System | ChatRole::Developer) {
                if configured.system_instruction.as_deref() != Some(message.content.as_str()) {
                    return Err(processor_error(GeminiError::Unsupported(
                        "system instruction must match connection setup",
                    )));
                }
            } else {
                turns.push(text_turn(message)?);
            }
        }
        self.enqueue(
            Action::ClientContent {
                turns,
                turn_complete: true,
            },
            origin,
        )?;
        state.initial_context = Some(context.clone());
        Ok(())
    }
}
fn text_turn(message: &ChatMessage) -> Result<TextTurn, ProcessorError> {
    if !message.tool_calls.is_empty() || message.tool_call_id.is_some() || message.name.is_some() {
        return Err(processor_error(GeminiError::Unsupported(
            "non-text context history",
        )));
    }
    let role = match message.role {
        ChatRole::User => Role::User,
        ChatRole::Assistant => Role::Model,
        _ => {
            return Err(processor_error(GeminiError::Unsupported(
                "only user/model text can be appended",
            )));
        }
    };
    Ok(TextTurn {
        role,
        text: message.content.clone(),
    })
}
fn tool_result(
    id: &str,
    name: &str,
    result: &serde_json::Value,
    final_result: bool,
    run_llm: Option<bool>,
    error: Option<&str>,
    non_blocking: bool,
) -> Result<Action, ProcessorError> {
    if !non_blocking && (!final_result || run_llm == Some(false)) {
        return Err(processor_error(GeminiError::Unsupported(
            "partial or scheduled tool responses",
        )));
    }
    let response = if let Some(error) = error {
        json!({"error":error})
    } else if result.is_object() {
        result.clone()
    } else {
        json!({"result":result})
    };
    if non_blocking {
        return Ok(Action::NonBlockingToolResponses(vec![
            NonBlockingFunctionResponse {
                id: id.into(),
                name: name.into(),
                response,
                will_continue: !final_result,
                scheduling: if run_llm == Some(false) {
                    ToolResponseScheduling::Silent
                } else {
                    ToolResponseScheduling::WhenIdle
                },
            },
        ]));
    }
    Ok(Action::ToolResponses(vec![FunctionResponse {
        id: id.into(),
        name: name.into(),
        response,
    }]))
}
#[async_trait]
impl FrameProcessor for GeminiLiveProcessor {
    fn name(&self) -> &str {
        "GeminiLiveProcessor"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if config.audio_in_sample_rate != self.client.config().input_sample_rate
            || (self.client.config().session.modality == Modality::Audio
                && config.audio_out_sample_rate != 24_000)
        {
            return Err(processor_error(GeminiError::Invalid(
                "pipeline sample rates differ from Gemini config (output must be 24000 Hz)",
            )));
        }
        *self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))? = InputState {
            preroll_bytes: self.preroll_bytes,
            ..Default::default()
        };
        self.video.reset().await.map_err(processor_error)?;
        self.video_paused
            .store(self.start_video_paused, Ordering::SeqCst);
        let mut session = self.client.connect().await.map_err(processor_error)?;
        let sender = session.sender();
        *self
            .sender
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini state lock poisoned"))? =
            Some(sender.clone());
        let output = ctx.clone();
        let user_id = self.user_id.clone();
        let mode = self.client.config().mode;
        let emit_metrics = config.enable_usage_metrics;
        ctx.spawn("gemini-live-receive", false, async move {
            let mut mapper = GeminiFrameMapper::new(mode, user_id);
            let mut generation = sender.generation();
            while let Some(result) = session.next_event().await {
                let event = match result {
                    Ok(event) => event,
                    Err(error) => {
                        output.push_error(error_frame(error)).await?;
                        break;
                    }
                };
                if generation != sender.generation() {
                    mapper.reset();
                    generation = sender.generation();
                }
                if matches!(event.event, Event::Interrupted) {
                    mapper.reset();
                    output.broadcast_interruption().await?;
                    continue;
                }
                if matches!(event.event, Event::Usage(_)) && !emit_metrics {
                    continue;
                }
                let is_output = event.event.is_output();
                for frame in mapper.frames(&event) {
                    if let FrameData::Error(error) = &frame.data {
                        output.push_error(error.clone()).await?;
                    } else if is_output {
                        if let Some(result) = sender
                            .while_current(
                                event.generation,
                                output.push(frame, Direction::Downstream),
                            )
                            .await
                        {
                            result?;
                        }
                    } else {
                        output.push(frame, Direction::Downstream).await?;
                    }
                }
            }
            session.shutdown().await.map_err(processor_error)
        })?;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let (SessionMode::Transcription, FrameData::SttMute(muted)) =
            (self.client.config().mode, &frame.data)
        {
            ctx.invocation_fence()?
                .with_current(|| {
                    self.state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?
                        .stt_muted = *muted;
                    Ok::<(), ProcessorError>(())
                })
                .ok_or_else(|| ProcessorError::new("stale Gemini STT mute control"))??;
            return Ok(());
        }
        if matches!(frame.data, FrameData::Interruption) {
            self.sender()?.interrupt().await;
        }
        if frame.data.is_cancel() || matches!(frame.data, FrameData::Stop) {
            self.video.cancel();
            if let Ok(sender) = self.sender() {
                sender.cancel();
            }
        }
        if direction == Direction::Downstream {
            let origin = Self::origin(&frame);
            let manual = self.client.config().session.activity_detection.disabled;
            match &frame.data {
                FrameData::SpeechControlParams(control) => self.update_preroll(control, ctx)?,
                FrameData::InputAudio(audio)
                | FrameData::UserAudio(pipecat_core::UserAudioRawFrame { audio, .. }) => {
                    let muted = if self.client.config().mode == SessionMode::Transcription {
                        self.state
                            .lock()
                            .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?
                            .stt_muted
                    } else {
                        false
                    };
                    if muted {
                        // Like STTService: suppress new recognition input, retaining
                        // the native audio passthrough and already admitted results.
                        return ctx.push(frame, direction).await;
                    }
                    audio
                        .validate()
                        .map_err(|_| ProcessorError::new("invalid Gemini PCM"))?;
                    if audio.num_channels != 1
                        || audio.sample_rate != self.client.config().input_sample_rate
                    {
                        return Err(processor_error(GeminiError::Invalid(
                            "input must match configured mono PCM16 rate",
                        )));
                    }
                    if audio.audio.len() > self.client.config().limits.max_audio_bytes {
                        return Err(processor_error(GeminiError::TooLarge));
                    }
                    if !audio.audio.is_empty() {
                        let mut state = self
                            .state
                            .lock()
                            .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                        if manual && !state.activity {
                            // Evict before extending, so a large input never enlarges the ring.
                            let slice = &audio.audio
                                [audio.audio.len().saturating_sub(state.preroll_bytes)..];
                            while state.preroll.len() + slice.len() > state.preroll_bytes {
                                state.preroll.pop_front();
                            }
                            state.preroll.extend(slice);
                        } else {
                            let mut origin = origin;
                            if audio.context_id.is_some() {
                                origin.context_id = audio.context_id.clone();
                            }
                            self.enqueue(Action::Audio(audio.audio.clone()), origin)?;
                        }
                    }
                }
                FrameData::UserStartedSpeaking if manual => {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                    if !state.activity {
                        self.enqueue(
                            Action::ActivityStart {
                                preroll: state.preroll.iter().copied().collect(),
                            },
                            origin,
                        )?;
                        state.preroll.clear();
                        state.activity = true;
                    }
                }
                FrameData::UserStoppedSpeaking if manual => {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                    if state.activity {
                        self.enqueue(Action::ActivityEnd, origin)?;
                        state.activity = false;
                    }
                    state.preroll.clear();
                }
                FrameData::VadUserStoppedSpeaking(_)
                | FrameData::VadUserStoppedSpeakingDetailed(_)
                    if self.client.config().mode == SessionMode::Transcription =>
                {
                    self.enqueue(Action::AudioStreamEnd, origin)?;
                }
                FrameData::InputText(text) => {
                    self.enqueue(Action::RealtimeText(text.text.clone()), origin)?;
                }
                FrameData::LlmContext(context) => self.seed_context(context, origin)?,
                FrameData::LlmMessagesAppend(messages) => {
                    if messages.messages.len() > 1024 {
                        return Err(processor_error(GeminiError::TooLarge));
                    }
                    let turns = messages
                        .messages
                        .iter()
                        .map(text_turn)
                        .collect::<Result<_, _>>()?;
                    self.enqueue(
                        Action::ClientContent {
                            turns,
                            turn_complete: messages.run_llm,
                        },
                        origin,
                    )?;
                }
                FrameData::LlmRun => {
                    self.enqueue(
                        Action::ClientContent {
                            turns: vec![],
                            turn_complete: true,
                        },
                        origin,
                    )?;
                }
                FrameData::FunctionCallResult(result) => {
                    let action = tool_result(
                        &result.tool_call_id,
                        &result.function_name,
                        &result.result,
                        result.is_final,
                        result.run_llm,
                        result.error.as_deref(),
                        self.client.is_non_blocking_tool(&result.function_name),
                    )?;
                    self.enqueue(action, origin)?;
                }
                FrameData::FunctionCallResultDetailed(result) => {
                    if result
                        .properties
                        .as_ref()
                        .is_some_and(|p| p.on_context_updated.is_some())
                    {
                        return Err(processor_error(GeminiError::Unsupported(
                            "tool context callbacks",
                        )));
                    }
                    let action = tool_result(
                        &result.tool_call_id,
                        &result.function_name,
                        &result.result,
                        result.properties.as_ref().is_none_or(|p| p.is_final),
                        result
                            .properties
                            .as_ref()
                            .and_then(|p| p.run_llm)
                            .or(result.run_llm),
                        result.error.as_deref(),
                        self.client.is_non_blocking_tool(&result.function_name),
                    )?;
                    self.enqueue(action, origin)?;
                }
                FrameData::InputImage(image)
                    if self.client.config().mode == SessionMode::Conversation =>
                {
                    self.send_video(image, origin, ctx).await?;
                }
                FrameData::UserImage(image)
                    if self.client.config().mode == SessionMode::Conversation =>
                {
                    self.send_video(&image.image, origin, ctx).await?;
                }
                FrameData::InputImage(_)
                | FrameData::InputFile(_)
                | FrameData::UserFile(_)
                | FrameData::UserFileDetailed(_)
                | FrameData::UserImage(_) => {
                    return Err(processor_error(GeminiError::Unsupported(
                        "video, image, or file input",
                    )));
                }
                FrameData::LlmContextDetailed(_)
                | FrameData::LlmMessagesUpdate(_)
                | FrameData::LlmMessagesAppendDetailed(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::LlmMessagesTransform(_) => {
                    return Err(processor_error(GeminiError::Unsupported(
                        "context replacement or non-text detailed context",
                    )));
                }
                FrameData::LlmUpdateSettings(_)
                | FrameData::SttUpdateSettings(_)
                | FrameData::ServiceUpdateSettings(_)
                | FrameData::LlmSetTools(_)
                | FrameData::LlmSetToolChoice(_) => {
                    return Err(processor_error(GeminiError::Unsupported(
                        "live configuration changes require a new session",
                    )));
                }
                FrameData::FunctionCallCancel(_) => {
                    return Err(processor_error(GeminiError::Unsupported(
                        "client-side tool cancellation results; send an explicit final error result",
                    )));
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
        self.video.cancel();
        if let Some(sender) = self
            .sender
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini state lock poisoned"))?
            .take()
        {
            sender.cancel();
        }
        self.video.finish().await.map_err(processor_error)?;
        Ok(())
    }
}

/// Deterministic event-to-frame mapping; generation completion is never reported
/// as playback completion. No BotStarted/StoppedSpeaking is synthesized.
pub struct GeminiFrameMapper {
    mode: SessionMode,
    user_id: String,
    response_open: bool,
    audio_open: bool,
}
impl GeminiFrameMapper {
    pub fn new(mode: SessionMode, user_id: impl Into<String>) -> Self {
        Self {
            mode,
            user_id: user_id.into(),
            response_open: false,
            audio_open: false,
        }
    }
    pub fn reset(&mut self) {
        self.response_open = false;
        self.audio_open = false;
    }
    pub fn frames(&mut self, event: &GeminiEvent) -> Vec<Frame> {
        let mut data = Vec::new();
        let context_id = event.origin.context_id.clone();
        if self.mode == SessionMode::Conversation
            && matches!(
                event.event,
                Event::Text { .. }
                    | Event::Audio { .. }
                    | Event::OutputTranscription(_)
                    | Event::ToolCalls(_)
            )
            && !self.response_open
        {
            self.response_open = true;
            data.push(FrameData::LlmResponseStart);
        }
        match &event.event {
            Event::Text { text, thought } if self.mode == SessionMode::Conversation => {
                if *thought {
                    data.extend([
                        FrameData::LlmThoughtStart,
                        FrameData::LlmThoughtText(TextFrame::llm(text)),
                        FrameData::LlmThoughtEnd,
                    ]);
                } else {
                    data.push(FrameData::LlmText(TextFrame::llm(text)));
                }
            }
            Event::Audio { pcm, sample_rate } if self.mode == SessionMode::Conversation => {
                if !self.audio_open {
                    self.audio_open = true;
                    data.push(FrameData::TtsStarted(TtsStartedFrame {
                        context_id: context_id.clone(),
                        append_to_context: true,
                    }));
                }
                data.push(FrameData::TtsAudio(AudioRawFrame {
                    audio: pcm.clone(),
                    sample_rate: *sample_rate,
                    num_channels: 1,
                    context_id: context_id.clone(),
                }));
            }
            Event::InputTranscription {
                transcript,
                is_final,
            } => {
                let frame = TranscriptionFrame {
                    text: transcript.text.clone(),
                    user_id: self.user_id.clone(),
                    timestamp: chrono::Utc::now().to_rfc3339(),
                    language: transcript.language_code.clone(),
                    result: Some(json!(transcript)),
                    finalized: *is_final,
                };
                data.push(if *is_final {
                    FrameData::Transcription(frame)
                } else {
                    FrameData::InterimTranscription(frame)
                });
            }
            Event::OutputTranscription(transcript) if self.mode == SessionMode::Conversation => {
                let mut text = TextFrame::llm(&transcript.text);
                text.skip_tts = Some(true);
                data.push(FrameData::TtsText(AggregatedTextFrame {
                    text,
                    aggregated_by: "word".into(),
                    context_id: context_id.clone(),
                    raw_text: None,
                    will_be_spoken: true,
                }));
            }
            Event::TurnComplete => {
                if self.audio_open {
                    data.push(FrameData::TtsStopped(TtsStoppedFrame { context_id }));
                }
                if self.response_open {
                    data.push(FrameData::LlmResponseEnd);
                }
                self.reset();
            }
            Event::Interrupted => {
                self.reset();
                data.push(FrameData::Interruption);
            }
            Event::ToolCalls(calls) | Event::NonBlockingToolCalls(calls) => {
                data.push(FrameData::FunctionCallsStarted(
                    calls
                        .iter()
                        .map(|call| pipecat_core::FunctionCall {
                            function_name: call.name.clone(),
                            tool_call_id: call.id.clone(),
                            arguments: call.args.clone(),
                        })
                        .collect(),
                ));
                for call in calls {
                    data.push(FrameData::FunctionCallInProgress(
                        FunctionCallInProgressFrame {
                            function_name: call.name.clone(),
                            tool_call_id: call.id.clone(),
                            arguments: call.args.clone(),
                            cancel_on_interruption: !matches!(
                                event.event,
                                Event::NonBlockingToolCalls(_)
                            ),
                            group_id: None,
                        },
                    ));
                }
            }
            Event::ToolCallsCancelled(calls) | Event::NonBlockingToolCallsCancelled(calls) => {
                for call in calls {
                    data.push(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                        function_name: call.name.clone(),
                        tool_call_id: call.id.clone(),
                        run_llm: false,
                    }));
                }
            }
            Event::Usage(usage) => {
                if let (Some(prompt_tokens), Some(completion_tokens), Some(total_tokens)) = (
                    usage.prompt_token_count,
                    usage.response_token_count,
                    usage.total_token_count,
                ) {
                    data.push(FrameData::Metrics(vec![Metric::LlmUsage {
                        processor: "GeminiLiveProcessor".into(),
                        prompt_tokens,
                        completion_tokens,
                        total_tokens,
                    }]));
                }
            }
            Event::GoAway { .. } => {
                data.push(FrameData::Error(error_frame(GeminiError::Unsupported(
                    "server announced disconnect; session resumption is unavailable",
                ))))
            }
            _ => {}
        }
        data.into_iter()
            .map(|data| {
                let mut frame = Frame::new(data);
                if matches!(
                    event.event,
                    Event::NonBlockingToolCalls(_) | Event::NonBlockingToolCallsCancelled(_)
                ) {
                    frame.interruptible = false;
                }
                frame
                    .metadata
                    .insert("gemini_generation".into(), json!(event.generation));
                if let Event::ToolCalls(calls)
                | Event::ToolCallsCancelled(calls)
                | Event::NonBlockingToolCalls(calls)
                | Event::NonBlockingToolCallsCancelled(calls) = &event.event
                {
                    let selected_id = match &frame.data {
                        FrameData::FunctionCallInProgress(call) => Some(call.tool_call_id.as_str()),
                        FrameData::FunctionCallCancel(call) => Some(call.tool_call_id.as_str()),
                        _ => None,
                    };
                    if selected_id.is_some()
                        || matches!(frame.data, FrameData::FunctionCallsStarted(_))
                    {
                        let sources: serde_json::Map<String, serde_json::Value> = calls
                            .iter()
                            .filter(|call| selected_id.is_none_or(|id| id == call.id))
                            .map(|call| (call.id.clone(), json!(call.id_source)))
                            .collect();
                        frame
                            .metadata
                            .insert("gemini_tool_id_sources".into(), json!(sources));
                    }
                }
                if let Some(id) = event.origin.frame_id {
                    frame.metadata.insert("source_frame_id".into(), json!(id));
                }
                if let Some(id) = &event.origin.context_id {
                    frame.metadata.insert("context_id".into(), json!(id));
                }
                frame
            })
            .collect()
    }
}
fn processor_error(error: GeminiError) -> ProcessorError {
    let frame = error_frame(error);
    ProcessorError::new(frame.error).with_category(frame.category)
}
fn error_frame(error: GeminiError) -> ErrorFrame {
    let category = match &error {
        GeminiError::Http(code) => ErrorCategory::from_http_status(*code),
        GeminiError::Provider { code, .. } => u16::try_from(*code)
            .map(ErrorCategory::from_http_status)
            .unwrap_or(ErrorCategory::Server),
        GeminiError::Invalid(_) | GeminiError::Unsupported(_) | GeminiError::Protocol(_) => {
            ErrorCategory::InvalidRequest
        }
        GeminiError::TooLarge | GeminiError::Backpressure(_) => ErrorCategory::Application,
        _ => ErrorCategory::Connectivity,
    };
    ErrorFrame {
        error: error.to_string(),
        fatal: false,
        processor: None,
        category,
    }
}

#[cfg(test)]
mod video_ownership_tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use pipecat_core::{ImageRawFrame, Pipeline, PipelineConfig, WorkerState};
    use std::{sync::Arc, time::Duration};
    use tokio::time::timeout;
    use tokio_tungstenite::tungstenite::Message;

    async fn blocked_encoding_terminal(interrupt: bool) {
        let wait = Duration::from_secs(5);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = GeminiConfig::new("fixture-key", "video-fixture");
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let mut peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let mut ws = tokio_tungstenite::accept_async(tcp)
                .await
                .map_err(|e| e.to_string())?;
            ws.next()
                .await
                .ok_or("no setup")?
                .map_err(|e| e.to_string())?;
            ws.send(Message::Text(
                json!({"setupComplete":{}}).to_string().into(),
            ))
            .await
            .map_err(|e| e.to_string())?;
            let mut messages = Vec::new();
            while let Some(message) = ws.next().await {
                let message = match message {
                    Err(tokio_tungstenite::tungstenite::Error::Protocol(
                        tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
                    )) if !interrupt => return Ok(messages),
                    result => result.map_err(|e| e.to_string())?,
                };
                match message {
                    Message::Close(_) => {
                        let _ = ws.flush().await;
                        return Ok::<_, String>(messages);
                    }
                    Message::Text(t) => messages.push(t.to_string()),
                    _ => {}
                }
            }
            if interrupt {
                Err("no close".into())
            } else {
                Ok(messages)
            }
        });
        let p = Arc::new(GeminiLiveProcessor::new(GeminiClient::new(config).unwrap()));
        let gate = p.video.test_gate();
        let created = Pipeline::new(vec![p.clone()])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_secs(2),
                start: StartFrame {
                    audio_out_sample_rate: 24_000,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await;
        let mut worker = match created {
            Ok(w) => w,
            Err(e) => {
                gate.release();
                peer.abort();
                let _ = peer.await;
                panic!("start {e}");
            }
        };
        let observed = timeout(wait, async {
            while !worker.recv().await.ok_or("no Start")?.frame.data.is_start() {}
            worker
                .sender()
                .send(
                    Frame::new(FrameData::InputImage(ImageRawFrame {
                        image: vec![1; 12],
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
            if interrupt {
                p.sender().map_err(|e| e.to_string())?.interrupt().await;
            } else {
                worker
                    .cancel("blocked video encode")
                    .await
                    .map_err(|e| e.to_string())?;
            }
            Ok::<_, String>(())
        })
        .await;
        // Always release the bounded test-only barrier, even on observation failure.
        gate.release();
        if interrupt && matches!(observed, Ok(Ok(()))) {
            let ended = timeout(wait, worker.end()).await;
            if !matches!(ended, Ok(Ok(()))) {
                let _ = timeout(wait, worker.cancel("End deadline")).await;
            }
        }
        if !matches!(observed, Ok(Ok(()))) {
            let _ = timeout(wait, worker.cancel("observation failed")).await;
        }
        let mut join_timeout = None;
        let joined = match timeout(wait, worker.join()).await {
            Ok(r) => r,
            Err(e) => {
                let _ = timeout(wait, worker.cancel("join timeout")).await;
                join_timeout = Some(e.to_string());
                worker.join().await
            }
        };
        let remote = match timeout(wait, &mut peer).await {
            Ok(r) => r,
            Err(e) => {
                peer.abort();
                let _ = peer.await;
                panic!("peer joined after {e}");
            }
        };
        assert!(join_timeout.is_none(), "{join_timeout:?}");
        assert!(matches!(observed, Ok(Ok(()))), "{observed:?}");
        let report = joined.unwrap();
        assert_eq!(
            report.state,
            if interrupt {
                WorkerState::Ended
            } else {
                WorkerState::Cancelled
            }
        );
        assert_eq!(report.total_errors, 0);
        assert!(remote.unwrap().unwrap().is_empty());
        assert!(gate.exited());
        assert!(p.video.test_idle().await);
    }
    #[tokio::test]
    async fn cancel_during_blocked_encoding_joins_cpu_and_peer_without_late_video() {
        blocked_encoding_terminal(false).await;
    }
    #[tokio::test]
    async fn external_sender_interrupt_cannot_relabel_old_encoded_image() {
        blocked_encoding_terminal(true).await;
    }
}
