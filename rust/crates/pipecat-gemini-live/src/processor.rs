pub(crate) mod selected;
use crate::*;
use pipecat_core::{
    async_trait, AggregatedTextFrame, AudioRawFrame, ChatMessage, ChatRole, Direction,
    ErrorCategory, ErrorFrame, Frame, FrameData, FrameProcessor, FunctionCallCancelFrame,
    FunctionCallInProgressFrame, LlmContextFrame, Metric, ProcessorContext, ProcessorError,
    ProcessorReference, ServiceMetadataFrame, StartFrame, SttMetadataFrame, TextFrame,
    TranscriptionFrame, TtsStartedFrame, TtsStoppedFrame,
};
pub(crate) use selected::SelectedSessionOwner;
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::time::Instant;

/// Persistent Gemini Live service. Forwarded original frames stay unchanged.
/// PCM is mono; resample explicitly elsewhere in the pipeline. Context seeding
/// is one-time because the server already retains later live conversation.
pub struct GeminiLiveProcessor {
    client: GeminiClient,
    sender: Mutex<Option<GeminiSender>>,
    state: Mutex<InputState>,
    selected_owner: Mutex<Option<Arc<SelectedSessionOwner>>>,
    selected_join: Mutex<Option<SelectedJoinObserver>>,
    next_run: AtomicU64,
    selected_setup: tokio::sync::Mutex<()>,
    #[cfg(test)]
    receiver_handoff: Mutex<Option<SelectedReceiveGate>>,
    preroll_bytes: usize,
    explicit_preroll: bool,
    user_id: Option<String>,
    audio_passthrough: bool,
    stt_ttfs_p99_latency: Option<f64>,
    audio_paused: AtomicBool,
    start_audio_paused: bool,
    video: crate::video::VideoEncoder,
    video_paused: AtomicBool,
    start_video_paused: bool,
    #[cfg(feature = "paired-history")]
    history: Option<Arc<crate::history::SelectedHistory>>,
}
#[cfg(test)]
struct SelectedReceiveGate {
    reached: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
}
#[derive(Default)]
struct InputState {
    activity: bool,
    selected: Option<Arc<SelectedSessionOwner>>,
    pending_first_stop: bool,
    stt_muted: bool,
    // Each setup owns a new label; a retired receiver cannot read the next run.
    stt_user_id: Arc<Mutex<String>>,
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
            selected_owner: Mutex::new(None),
            selected_join: Mutex::new(None),
            next_run: AtomicU64::new(1),
            selected_setup: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            receiver_handoff: Mutex::new(None),
            state: Mutex::new(InputState {
                preroll_bytes,
                ..Default::default()
            }),
            preroll_bytes,
            explicit_preroll: false,
            user_id: None,
            audio_passthrough: true,
            stt_ttfs_p99_latency: Some(0.60),
            audio_paused: AtomicBool::new(false),
            start_audio_paused: false,
            video: Default::default(),
            video_paused: AtomicBool::new(false),
            start_video_paused: false,
            #[cfg(feature = "paired-history")]
            history: None,
        }
    }
    #[cfg(feature = "paired-history")]
    pub(crate) fn with_selected_history(
        mut self,
        history: Arc<crate::history::SelectedHistory>,
    ) -> Self {
        self.history = Some(history);
        self
    }

    #[cfg(feature = "paired-history")]
    async fn start_selected_history(
        &self,
        history: &Arc<crate::history::SelectedHistory>,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        // This validates the exact successful gate/user/service/assistant
        // setup identities and initial context before any provider connection.
        let initial_seed = history.begin_service_start(ctx)?;
        if self.client.initial_history_policy().is_some() {
            let run_id = self.next_run_id()?;
            let session = self
                .client
                .connect_paired_history_owned(run_id)
                .await
                .map_err(processor_error)?;
            let owner = SelectedSessionOwner::new(
                run_id,
                session,
                history.selected_drain_timeout(),
                Some(ctx),
            );
            self.publish_owner(owner.clone());
            history.bind_initial_owner(owner.clone());
            let seed = initial_seed.unwrap_or_default();
            let admitted =
                history.admit_initial_seed(|| self.seed_context(&seed, Origin::default()));
            if let Err(error) = &admitted {
                owner.record_failure(error.message.clone());
                let joined = owner.settle(false).await;
                return selected::combine(admitted, joined);
            }
            let started = history
                .start_receive_owned(
                    owner.clone(),
                    self.user_id.clone().unwrap_or_default(),
                    config.enable_usage_metrics,
                )
                .await;
            if let Err(error) = &started {
                owner.record_failure(error.message.clone());
                let joined = owner.settle(false).await;
                return selected::combine(started, joined);
            }
            return Ok(());
        }
        let mut session = self
            .client
            .connect_paired_history()
            .await
            .map_err(processor_error)?;
        // The shared startup gate linearizes cancellation against this short
        // synchronous publication/seed admission. The closure does not await
        // or enter history state; provider I/O stays in the session worker.
        let admitted = history.admit_initial_seed(|| {
            *self
                .sender
                .lock()
                .map_err(|_| ProcessorError::fatal("Gemini state lock poisoned"))? =
                Some(session.sender());
            if let Some(seed) = initial_seed.filter(|seed| !seed.messages.is_empty()) {
                // This is the constructor-selected initial seed, not a live
                // context frame or a local user-history feedback notification.
                self.seed_context(&seed, Origin::default())?;
            }
            Ok::<(), ProcessorError>(())
        });
        if let Err(error) = admitted {
            // The receiver has not taken ownership yet: settle the actual
            // local handle before returning the selected startup failure.
            match tokio::time::timeout(
                self.client.config().limits.finish_timeout,
                session.cancel_and_join(),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(join_error)) => {
                    return Err(ProcessorError::fatal(format!(
                        "{}; Gemini startup worker join failed: {}",
                        error.message, join_error
                    )));
                }
                Err(_) => {
                    return Err(ProcessorError::fatal(format!(
                        "{}; Gemini startup worker forced abort after join timeout",
                        error.message
                    )));
                }
            }
            return Err(error);
        }
        history
            .start_receive(
                session,
                self.user_id.clone().unwrap_or_default(),
                config.enable_usage_metrics,
            )
            .await
    }
    async fn start_initial_history(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let run_id = self.next_run_id()?;
        let budget = self.selected_terminal_budget()?;
        let session = self
            .client
            .connect_owned(run_id)
            .await
            .map_err(processor_error)?;
        let owner = SelectedSessionOwner::new(run_id, session, budget, Some(ctx));
        // No await occurs between the returned actor and recoverable publication.
        self.publish_owner(owner.clone());
        let (ownership, acknowledged) = tokio::sync::oneshot::channel();
        let receiver = owner.clone();
        let output = ctx.clone();
        let user_id = self.user_id.clone().unwrap_or_default();
        let emit_metrics = config.enable_usage_metrics;
        #[cfg(test)]
        let handoff = self.receiver_handoff.lock().unwrap().take();
        let spawned = ctx.spawn("gemini-live-receive", false, async move {
            #[cfg(test)]
            if let Some(gate) = handoff {
                let _ = gate.reached.send(());
                let _ = gate.release.await;
            }
            let mut session = receiver.session.lock().await;
            let result = if ownership.send(()).is_err() {
                Err(ProcessorError::fatal(
                    "Gemini selected receive handoff was cancelled",
                ))
            } else {
                receive_initial_history(&receiver, &mut session, &output, user_id, emit_metrics)
                    .await
            };
            receiver.record_result(&result);
            let joined = session.selected_shutdown().await.map_err(processor_error);
            let result = selected::combine(result, joined);
            receiver.complete(&result);
            result
        });
        let start = match spawned {
            Ok(id) => {
                owner.bind_receiver(ctx.clone(), id);
                acknowledged.await.map_err(|_| {
                    ProcessorError::fatal(
                        "Gemini selected receive owner ended before startup handoff",
                    )
                })
            }
            Err(error) => Err(error),
        };
        if let Err(error) = &start {
            owner.record_failure(error.message.clone());
            let cleanup = owner.settle(false).await;
            return selected::combine(start, cleanup);
        }
        Ok(())
    }
    /// Fixed transcription label, including an explicitly empty string.
    /// Without an override, dedicated STT follows the latest valid downstream
    /// audio's user ID; conversation mode keeps its empty default.
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }
    /// Forward original dedicated STT `InputAudio` and `UserAudio` frames in
    /// either direction. Defaults to true; conversation mode ignores this option.
    /// Disabling forwarding does not pause recognition, validation or speaker
    /// label updates. Place audio-dependent processors before this service when
    /// disabled. The setting is retained across setup and sequential reuse.
    pub fn with_audio_passthrough(mut self, value: bool) -> Self {
        self.audio_passthrough = value;
        self
    }
    async fn forward_original(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.client.config().mode == SessionMode::Transcription
            && !self.audio_passthrough
            && matches!(
                frame.data,
                FrameData::InputAudio(_) | FrameData::UserAudio(_)
            )
        {
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
    /// Dedicated STT's configured P99 time-to-final-transcript budget in seconds.
    /// Defaults to `Some(0.60)` from the pinned Python service; this is not a
    /// measurement. `None` selects its base-service fallback of 1.0 second.
    /// Finite overrides in 0..=300 seconds are accepted. Conversation mode ignores
    /// this setting. The configured value is retained across setup and reuse.
    pub fn with_ttfs_p99_latency(mut self, value: Option<f64>) -> Result<Self, GeminiError> {
        if value.is_some_and(|value| !value.is_finite() || !(0.0..=300.0).contains(&value)) {
            return Err(GeminiError::Invalid("TTFS latency"));
        }
        self.stt_ttfs_p99_latency = value;
        Ok(self)
    }
    async fn stt_metadata(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        ctx.broadcast(Frame::new(FrameData::SttMetadata(SttMetadataFrame {
            base: ServiceMetadataFrame {
                service_name: self.name().into(),
                user_turn_strategies: None,
            },
            ttfs_p99_latency: self.stt_ttfs_p99_latency.unwrap_or(1.0),
        })))
        .await
    }
    fn targets_self(&self, target: &ProcessorReference) -> bool {
        match target {
            ProcessorReference::Name(name) => name == self.name(),
            ProcessorReference::Local(target) => {
                // Compare identity without depending on trait-object vtables.
                std::ptr::addr_eq(target.0.as_ref(), self as &dyn FrameProcessor)
            }
        }
    }
    /// Initial conversation-audio pause state, restored on each setup.
    /// Paused frames still pass through. Transcription mode ignores this setting
    /// and uses the existing `SttMute` frame control instead.
    pub fn with_audio_input_paused(mut self, paused: bool) -> Self {
        self.start_audio_paused = paused;
        self.audio_paused = AtomicBool::new(paused);
        self
    }
    /// Pause new conversation audio before validation, sending or pre-roll retention.
    /// Transcription mode ignores this setting. Existing pre-roll and activity
    /// controls remain intact, so activity start can still flush buffered audio.
    /// A frame that already observed the unpaused state can finish admission;
    /// this does not retract that frame or already queued provider work.
    pub fn set_audio_input_paused(&self, paused: bool) {
        self.audio_paused.store(paused, Ordering::SeqCst);
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
        if self.client.initial_history_policy().is_some() {
            return self.send_selected_video(image, origin, ctx).await;
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
    async fn send_selected_video(
        &self,
        image: &pipecat_core::ImageRawFrame,
        origin: Origin,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let fence = ctx.invocation_fence()?;
        let captured = fence
            .with_current(|| {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                let Some(owner) = state.selected.clone() else {
                    return Ok(None);
                };
                if !owner.accepts_input() || state.initial_context.is_none() {
                    return Ok(None);
                }
                let now = Instant::now();
                if state.last_video.is_some_and(|last| {
                    now.duration_since(last) < std::time::Duration::from_secs(1)
                }) {
                    return Ok(None);
                }
                state.last_video = Some(now);
                let generation = owner.sender.generation();
                Ok::<_, ProcessorError>(Some((owner, generation)))
            })
            .transpose()?
            .flatten();
        let Some((owner, generation)) = captured else {
            return Ok(());
        };
        let overhead = serde_json::to_vec(
            &json!({"realtimeInput":{"video":{"mimeType":"image/jpeg","data":""}}}),
        )
        .map_err(|_| ProcessorError::new("video JSON"))?
        .len();
        let max_jpeg = self
            .client
            .config()
            .limits
            .max_message_bytes
            .saturating_sub(overhead)
            / 4
            * 3;
        let jpeg = self
            .video
            .encode(image, max_jpeg)
            .await
            .map_err(|error| processor_error(error).with_permanence_override(false))?;
        fence
            .with_current(|| {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                if !state
                    .selected
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &owner))
                    || !owner.accepts_input()
                    || state.initial_context.is_none()
                    || owner.sender.generation() != generation
                {
                    return Ok(());
                }
                let plan = owner
                    .sender
                    .plan_video(owner.run_id, jpeg, origin, generation)
                    .map_err(processor_error)?;
                owner
                    .sender
                    .try_send_selected(plan)
                    .map_err(processor_error)
            })
            .transpose()?;
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
        if self.client.initial_history_policy().is_some() {
            let state = self
                .state
                .lock()
                .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
            return self.enqueue_locked(&state, action, origin);
        }
        self.sender()?
            .try_send(action, origin)
            .map_err(processor_error)
    }
    fn enqueue_locked(
        &self,
        state: &InputState,
        action: Action,
        origin: Origin,
    ) -> Result<(), ProcessorError> {
        let Some(owner) = &state.selected else {
            return self
                .sender()?
                .try_send(action, origin)
                .map_err(processor_error);
        };
        if !owner.accepts_input()
            && !matches!(
                action,
                Action::ToolResponses(_) | Action::NonBlockingToolResponses(_)
            )
        {
            return Err(processor_error(GeminiError::Closed));
        }
        if state.initial_context.is_none() {
            return Err(processor_error(GeminiError::Unsupported(
                "explicit initial context is required before run or append",
            )));
        }
        let sender = &owner.sender;
        let id = owner.run_id;
        let generation = sender.generation();
        // No Action bypass exists at the sender. Each internal operation builds
        // its own typed, bounded plan, tied to this run and sampled generation.
        let plan = match action {
            Action::Audio(pcm) => sender.plan_audio(id, pcm, origin, generation),
            Action::VideoJpeg(jpeg) => sender.plan_video(id, jpeg, origin, generation),
            Action::RealtimeText(text) => sender.plan_realtime_text(id, text, origin, generation),
            Action::ClientContent {
                turns,
                turn_complete,
            } if turns.is_empty() && turn_complete => {
                sender.plan_completion(id, origin, generation)
            }
            Action::ClientContent {
                turns,
                turn_complete,
            } => sender.plan_content(id, turns, turn_complete, origin, generation),
            Action::ActivityStart { preroll } => {
                sender.plan_activity_start(id, preroll, origin, generation)
            }
            Action::ActivityEnd => sender.plan_first_stop(id, true, false, origin, generation),
            Action::AudioStreamEnd => sender.plan_audio_stream_end(id, origin, generation),
            Action::ToolResponses(responses) => {
                sender.plan_tool_responses(id, responses, origin, generation)
            }
            Action::NonBlockingToolResponses(responses) => {
                sender.plan_non_blocking_tool_responses(id, responses, origin, generation)
            }
        }
        .map_err(processor_error)?;
        sender.try_send_selected(plan).map_err(processor_error)
    }
    fn enqueue_owned(
        &self,
        owner: &Arc<SelectedSessionOwner>,
        action: Action,
        origin: Origin,
    ) -> Result<(), ProcessorError> {
        let state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
        if !state
            .selected
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, owner))
        {
            return Err(processor_error(GeminiError::Closed));
        }
        self.enqueue_locked(&state, action, origin)
    }
    fn tool_action(&self, frame: &FrameData) -> Result<Option<Action>, ProcessorError> {
        match frame {
            FrameData::FunctionCallResult(result) => tool_result(
                &result.tool_call_id,
                &result.function_name,
                &result.result,
                result.is_final,
                result.run_llm,
                result.error.as_deref(),
                self.client.is_non_blocking_tool(&result.function_name),
            )
            .map(Some),
            FrameData::FunctionCallResultDetailed(result) => {
                if result
                    .properties
                    .as_ref()
                    .is_some_and(|properties| properties.on_context_updated.is_some())
                {
                    return Err(processor_error(GeminiError::Unsupported(
                        "tool context callbacks",
                    )));
                }
                tool_result(
                    &result.tool_call_id,
                    &result.function_name,
                    &result.result,
                    result
                        .properties
                        .as_ref()
                        .is_none_or(|properties| properties.is_final),
                    result
                        .properties
                        .as_ref()
                        .and_then(|properties| properties.run_llm)
                        .or(result.run_llm),
                    result.error.as_deref(),
                    self.client.is_non_blocking_tool(&result.function_name),
                )
                .map(Some)
            }
            _ => Ok(None),
        }
    }
    fn stop_selected(&self, origin: Origin) -> Result<(), ProcessorError> {
        let manual = self.client.config().session.activity_detection.disabled;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
        let owner = state
            .selected
            .as_ref()
            .ok_or_else(|| ProcessorError::new("Gemini session is not ready"))?;
        let activity_end = manual && state.activity;
        let complete = state.pending_first_stop;
        if activity_end || complete {
            if !owner.accepts_input() {
                return Err(processor_error(GeminiError::Closed));
            }
            let plan = owner
                .sender
                .plan_first_stop(
                    owner.run_id,
                    activity_end,
                    complete,
                    origin,
                    owner.sender.generation(),
                )
                .map_err(processor_error)?;
            // One admission covers activityEnd and completion. Queue
            // rejection preserves both activity and first-stop state.
            owner
                .sender
                .try_send_selected(plan)
                .map_err(processor_error)?;
            state.activity = false;
            state.pending_first_stop = false;
        }
        state.preroll.clear();
        Ok(())
    }
    fn waiting_for_seed(&self) -> Result<bool, ProcessorError> {
        if self.client.initial_history_policy().is_none() {
            return Ok(false);
        }
        let state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
        Ok(state.initial_context.is_none())
    }
    /// Read-only evidence for the most recently created selected socket actor.
    /// A joined observer includes abort-then-await and is not a clean-close claim.
    pub fn selected_join_observer(&self) -> Option<SelectedJoinObserver> {
        self.selected_join
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn selected_session_joined(&self) -> bool {
        self.selected_join_observer()
            .is_some_and(|observer| observer.joined())
    }
    fn current_owner(&self) -> Option<Arc<SelectedSessionOwner>> {
        self.selected_owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    fn is_current_owner(&self, owner: &Arc<SelectedSessionOwner>) -> bool {
        self.current_owner()
            .as_ref()
            .is_some_and(|current| current.run_id == owner.run_id && Arc::ptr_eq(current, owner))
    }
    fn publish_owner(&self, owner: Arc<SelectedSessionOwner>) {
        // The selected setup mutex serializes connect/publication; setup settles
        // the previous run before entering connect. Paired Start is single-use.
        // Recover poisoned slots here so the newly created actor is retained
        // before any possible error-return or await.
        *self
            .selected_owner
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(owner.clone());
        *self.selected_join.lock().unwrap_or_else(|e| e.into_inner()) = Some(owner.joined.clone());
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .selected = Some(owner.clone());
        *self.sender.lock().unwrap_or_else(|e| e.into_inner()) = Some(owner.sender.clone());
    }
    fn retire_owner(&self, owner: &Arc<SelectedSessionOwner>) {
        if !owner.joined.joined() {
            return;
        }
        let mut slot = self
            .selected_owner
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if slot
            .as_ref()
            .is_some_and(|current| current.run_id == owner.run_id && Arc::ptr_eq(current, owner))
        {
            *slot = None;
        }
    }
    fn next_run_id(&self) -> Result<u64, ProcessorError> {
        self.next_run
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |id| id.checked_add(1))
            .map_err(|_| ProcessorError::fatal("Gemini run identity exhausted"))
    }
    fn selected_terminal_budget(&self) -> Result<std::time::Duration, ProcessorError> {
        self.client
            .config()
            .limits
            .finish_timeout
            .checked_add(self.client.config().limits.write_timeout)
            .ok_or_else(|| ProcessorError::fatal("Gemini terminal budget overflow"))
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
        if self.client.initial_history_policy().is_some() {
            // Full original-snapshot validation and wire sizing precede mutable
            // admission. A leading system message cannot hide an invalid tail.
            let captured = {
                let state = self
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
                state
                    .selected
                    .clone()
                    .ok_or_else(|| ProcessorError::new("Gemini session is not ready"))?
            };
            let seed = self
                .client
                .plan_initial_history(context)
                .map_err(processor_error)?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
            if !state
                .selected
                .as_ref()
                .is_some_and(|owner| Arc::ptr_eq(owner, &captured))
            {
                return Err(processor_error(GeminiError::Closed));
            }
            if let Some(existing) = &state.initial_context {
                return if existing == context {
                    Ok(())
                } else {
                    Err(processor_error(GeminiError::Unsupported(
                        "replacing or replaying evolving context; use explicit append-only ClientContent",
                    )))
                };
            }
            let owner = state
                .selected
                .as_ref()
                .ok_or_else(|| ProcessorError::new("Gemini session is not ready"))?;
            if !owner.accepts_input() {
                return Err(processor_error(GeminiError::Closed));
            }
            let pending = seed.pending_first_stop();
            let generation = owner.sender.generation();
            let plan = owner
                .sender
                .plan_seed(owner.run_id, seed, origin, generation)
                .map_err(processor_error)?;
            owner
                .sender
                .try_send_selected(plan)
                .map_err(processor_error)?;
            state.initial_context = Some(context.clone());
            state.pending_first_stop = pending;
            return Ok(());
        }
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
/// Apply the selected route's live-input exclusions in either direction, after
/// authentic local feedback/marker identities have had their narrow handling.
/// The mandatory ingress uses the same check before the user can mutate state.
#[cfg(feature = "paired-history")]
pub(crate) fn reject_selected_live_frame(frame: &Frame) -> Result<(), ProcessorError> {
    frame
        .require_no_native_llm_continuation()
        .map_err(|error| ProcessorError::fatal(error.to_string()))?;
    if matches!(
        frame.data,
        FrameData::InputText(_)
            | FrameData::LlmRun
            | FrameData::LlmContext(_)
            | FrameData::LlmContextDetailed(_)
            | FrameData::LlmMessagesAppend(_)
            | FrameData::LlmMessagesUpdate(_)
            | FrameData::LlmMessagesAppendDetailed(_)
            | FrameData::LlmMessagesUpdateDetailed(_)
            | FrameData::LlmMessagesTransform(_)
            | FrameData::LlmSetTools(_)
            | FrameData::LlmSetToolChoice(_)
            | FrameData::LlmContextSummaryRequest(_)
            | FrameData::LlmContextSummaryResult(_)
            | FrameData::LlmSummarizeContext(_)
            | FrameData::LlmContinuationCheckpoint
            | FrameData::FunctionCallsStarted(_)
            | FrameData::FunctionCallsStartedDetailed(_)
            | FrameData::FunctionCallInProgress(_)
            | FrameData::FunctionCallResult(_)
            | FrameData::FunctionCallResultDetailed(_)
            | FrameData::FunctionCallCancel(_)
            | FrameData::ExternalFunctionCallsStarted(_)
            | FrameData::ExternalFunctionCallInProgress(_)
            | FrameData::ExternalFunctionCallResult(_)
            | FrameData::ExternalFunctionCallCancel(_)
            | FrameData::InputImage(_)
            | FrameData::UserImage(_)
            | FrameData::AssistantImage(_)
            | FrameData::UserImageRequest(_)
            | FrameData::InputFile(_)
            | FrameData::UserFile(_)
            | FrameData::UserFileDetailed(_)
            | FrameData::Transcription(_)
            | FrameData::TranscriptionDetailed(_)
            | FrameData::InterimTranscription(_)
            | FrameData::InterimTranscriptionDetailed(_)
            | FrameData::EagerTranscription(_)
            | FrameData::EagerTranscriptionDetailed(_)
            | FrameData::Translation(_)
            | FrameData::TranslationDetailed(_)
            | FrameData::LlmUpdateSettings(_)
            | FrameData::SttUpdateSettings(_)
            | FrameData::ServiceUpdateSettings(_)
    ) {
        return Err(ProcessorError::fatal(
            "paired history rejects live text/history edits, tools, image/file input and foreign transcription",
        ));
    }
    Ok(())
}

fn validate_suppressed_video(image: &pipecat_core::ImageRawFrame) -> Result<(), ProcessorError> {
    let (width, height) = image.size;
    if image.format.as_deref() != Some("RGB")
        || width == 0
        || height == 0
        || width > 2048
        || height > 2048
    {
        return Err(processor_error(GeminiError::Invalid(
            "video requires packed RGB, dimensions 1..=2048",
        )));
    }
    let required = (width as usize)
        .checked_mul(height as usize)
        .filter(|pixels| *pixels <= 262_144)
        .and_then(|pixels| pixels.checked_mul(3))
        .ok_or_else(|| processor_error(GeminiError::TooLarge))?;
    if image.image.len() > 262_144 * 3 {
        return Err(processor_error(GeminiError::TooLarge));
    }
    if image.image.len() < required {
        return Err(processor_error(GeminiError::Invalid("short RGB image")));
    }
    Ok(())
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
impl GeminiLiveProcessor {
    async fn process_frame(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        #[cfg(feature = "paired-history")]
        if let Some(history) = &self.history {
            // Exact known native identities are validated before inspecting
            // variants. Authentic feedback and the one End input fence are
            // consumed here; neither can seed another provider request.
            if history.inspect_service_frame(&frame, direction)? {
                return Ok(());
            }
            reject_selected_live_frame(&frame)?;
            let start = match &frame.data {
                FrameData::Start(config) => Some(config),
                FrameData::StartDetailed(config) => Some(&config.configuration),
                _ => None,
            };
            if let Some(config) = start {
                if direction != Direction::Downstream {
                    return Err(ProcessorError::fatal(
                        "paired history Start must be downstream",
                    ));
                }
                self.start_selected_history(history, config, ctx).await?;
                return self.forward_original(frame, direction, ctx).await;
            }
            if frame.data.is_cancel()
                || matches!(
                    frame.data,
                    FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop
                )
            {
                // The earlier mandatory gate has already settled the shared
                // owner. A direct bypass must not claim a cooperative join.
                history.prepare_service_end().await?;
                return self.forward_original(frame, direction, ctx).await;
            }
            if history.service_input_closed() {
                match &frame.data {
                    FrameData::InputAudio(_)
                    | FrameData::UserAudio(_)
                    | FrameData::SpeechControlParams(_) => {
                        return Err(ProcessorError::fatal(
                            "paired history application input is closed",
                        ));
                    }
                    FrameData::UserStartedSpeaking | FrameData::UserStoppedSpeaking => {
                        // A timer effect may have been extracted before the
                        // user's marker quiescence and delivered afterwards.
                        // Preserve local observation, never late provider VAD.
                        return self.forward_original(frame, direction, ctx).await;
                    }
                    _ => {}
                }
            }
        }
        if self.client.config().mode == SessionMode::Transcription {
            if frame.data.is_start() {
                ctx.push(frame, direction).await?;
                return self.stt_metadata(ctx).await;
            }
            if let FrameData::ServiceSwitcherRequestMetadata(target) = &frame.data {
                if self.targets_self(target) {
                    self.stt_metadata(ctx).await?;
                }
                return ctx.push(frame, direction).await;
            }
        }
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
            if self.waiting_for_seed()? {
                let suppressed = match &frame.data {
                    FrameData::InputAudio(audio)
                    | FrameData::UserAudio(pipecat_core::UserAudioRawFrame { audio, .. }) => {
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
                        true
                    }
                    FrameData::InputText(text) => {
                        // Plan only for shape/serialized-size validation. Never
                        // enqueue, open an activity window or keep a backlog.
                        let state = self
                            .state
                            .lock()
                            .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                        if let Some(owner) = &state.selected {
                            owner
                                .sender
                                .plan_realtime_text(
                                    owner.run_id,
                                    text.text.clone(),
                                    origin.clone(),
                                    owner.sender.generation(),
                                )
                                .map_err(processor_error)?;
                        }
                        true
                    }
                    FrameData::InputImage(image) => {
                        validate_suppressed_video(image)?;
                        true
                    }
                    FrameData::UserImage(image) => {
                        validate_suppressed_video(&image.image)?;
                        true
                    }
                    FrameData::UserStartedSpeaking
                    | FrameData::UserStoppedSpeaking
                    | FrameData::VadUserStartedSpeaking(_)
                    | FrameData::VadUserStoppedSpeaking(_)
                    | FrameData::VadUserStartedSpeakingDetailed(_)
                    | FrameData::VadUserStoppedSpeakingDetailed(_) => true,
                    _ => false,
                };
                if suppressed {
                    return self.forward_original(frame, direction, ctx).await;
                }
            }
            match &frame.data {
                FrameData::SpeechControlParams(control) => self.update_preroll(control, ctx)?,
                FrameData::InputAudio(audio)
                | FrameData::UserAudio(pipecat_core::UserAudioRawFrame { audio, .. }) => {
                    let suppress_audio = match self.client.config().mode {
                        SessionMode::Transcription => {
                            self.state
                                .lock()
                                .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?
                                .stt_muted
                        }
                        // The load is the pause admission boundary. A setter does
                        // not retract frames that already observed an unpaused state.
                        SessionMode::Conversation => self.audio_paused.load(Ordering::SeqCst),
                    };
                    if suppress_audio {
                        // Apply configured forwarding without validating, retaining
                        // or sending new input. Prior work is unaffected.
                        return self.forward_original(frame, direction, ctx).await;
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
                    let dynamic_stt_user_id = self.client.config().mode
                        == SessionMode::Transcription
                        && self.user_id.is_none();
                    let admit_audio =
                        || {
                            if dynamic_stt_user_id {
                                let state = self.state.lock().map_err(|_| {
                                    ProcessorError::fatal("Gemini input lock poisoned")
                                })?;
                                *state.stt_user_id.lock().map_err(|_| {
                                    ProcessorError::fatal("Gemini STT label lock poisoned")
                                })? = match &frame.data {
                                    FrameData::UserAudio(user_audio) => user_audio.user_id.clone(),
                                    _ => String::new(),
                                };
                            }
                            // Valid empty input still updates the observed label. A later
                            // enqueue error does not roll it back or authorize a flush.
                            if !audio.audio.is_empty() {
                                let mut state = self.state.lock().map_err(|_| {
                                    ProcessorError::fatal("Gemini input lock poisoned")
                                })?;
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
                                    self.enqueue_locked(
                                        &state,
                                        Action::Audio(audio.audio.clone()),
                                        origin,
                                    )?;
                                }
                            }
                            Ok::<(), ProcessorError>(())
                        };
                    if dynamic_stt_user_id {
                        ctx.invocation_fence()?
                            .with_current(admit_audio)
                            .ok_or_else(|| ProcessorError::new("stale Gemini STT audio"))??;
                    } else {
                        admit_audio()?;
                    }
                }
                FrameData::UserStartedSpeaking if manual => {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                    if !state.activity {
                        self.enqueue_locked(
                            &state,
                            Action::ActivityStart {
                                preroll: state.preroll.iter().copied().collect(),
                            },
                            origin,
                        )?;
                        state.preroll.clear();
                        state.activity = true;
                    }
                }
                FrameData::UserStoppedSpeaking
                    if self.client.initial_history_policy().is_some() =>
                {
                    self.stop_selected(origin)?;
                }
                FrameData::UserStoppedSpeaking if manual => {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?;
                    if state.activity {
                        self.enqueue_locked(&state, Action::ActivityEnd, origin)?;
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
        self.forward_original(frame, direction, ctx).await
    }
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
        let _selected_setup = if self.client.initial_history_policy().is_some() {
            Some(self.selected_setup.lock().await)
        } else {
            None
        };
        if config.audio_in_sample_rate != self.client.config().input_sample_rate
            || (self.client.config().session.modality == Modality::Audio
                && config.audio_out_sample_rate != 24_000)
        {
            return Err(processor_error(GeminiError::Invalid(
                "pipeline sample rates differ from Gemini config (output must be 24000 Hz)",
            )));
        }
        #[cfg(feature = "paired-history")]
        if self.client.initial_history_policy().is_some() {
            if let Some(history) = &self.history {
                self.client
                    .validate_paired_history()
                    .map_err(processor_error)?;
                // A rejected second paired setup must not reset the first run.
                history.bind_service(ctx)?;
            }
        }
        if self.client.initial_history_policy().is_some() {
            if let Some(owner) = self.current_owner() {
                let result = owner.settle(false).await;
                self.retire_owner(&owner);
                result?;
            }
        }
        *self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))? = InputState {
            preroll_bytes: self.preroll_bytes,
            ..Default::default()
        };
        self.audio_paused
            .store(self.start_audio_paused, Ordering::SeqCst);
        self.video.reset().await.map_err(processor_error)?;
        self.video_paused
            .store(self.start_video_paused, Ordering::SeqCst);
        #[cfg(feature = "paired-history")]
        if let Some(history) = &self.history {
            self.client
                .validate_paired_history()
                .map_err(processor_error)?;
            // Publish only after every selected service setup check succeeds.
            // Keep the real None-epoch setup route; Start must not replace it.
            if self.client.initial_history_policy().is_none() {
                history.bind_service(ctx)?;
            }
            return Ok(());
        }
        if self.client.initial_history_policy().is_some() {
            return self.start_initial_history(config, ctx).await;
        }
        let mut session = self.client.connect().await.map_err(processor_error)?;
        let sender = session.sender();
        *self
            .sender
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini state lock poisoned"))? =
            Some(sender.clone());
        let output = ctx.clone();
        let user_id = self.user_id.clone().unwrap_or_default();
        let mode = self.client.config().mode;
        let stt_user_id = if mode == SessionMode::Transcription && self.user_id.is_none() {
            Some(
                self.state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("Gemini input lock poisoned"))?
                    .stt_user_id
                    .clone(),
            )
        } else {
            None
        };
        let emit_metrics = config.enable_usage_metrics;
        ctx.spawn("gemini-live-receive", false, async move {
            let mut mapper = GeminiFrameMapper::new(mode, user_id);
            let mut generation = sender.generation();
            while let Some(result) = session.next_event_with_details().await {
                let received = match result {
                    Ok(received) => received,
                    Err(error) => {
                        output.push_error(error_frame(error)).await?;
                        break;
                    }
                };
                let event = received.event;
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
                if matches!(event.event, Event::InputTranscription { .. }) {
                    if let Some(user_id) = &stt_user_id {
                        // A service-level label at receipt, not per-audio attribution.
                        mapper.user_id = user_id
                            .lock()
                            .map_err(|_| ProcessorError::fatal("Gemini STT label lock poisoned"))?
                            .clone();
                    }
                }
                for mut frame in mapper.frames(&event) {
                    if let Some(details) = &received.usage_details {
                        attach_usage_details(&mut frame, details);
                    }
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
        let selected = self.client.initial_history_policy().is_some();
        let captured = if selected { self.current_owner() } else { None };
        if captured
            .as_ref()
            .is_some_and(|owner| !owner.belongs_to(ctx))
        {
            return Err(ProcessorError::new("stale Gemini selected setup context"));
        }
        if frame.data.is_cancel() {
            if let Some(owner) = &captured {
                owner.sender.cancel();
            }
        }
        #[cfg(feature = "paired-history")]
        let ordinary = self.history.is_none();
        #[cfg(not(feature = "paired-history"))]
        let ordinary = true;
        let tool_settlement = selected
            && ordinary
            && direction == Direction::Downstream
            && matches!(
                frame.data,
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_)
            );
        // Selected setup/ordinary frame/terminal lifetimes cannot cross runs.
        // Authorized tool settlements are the sole exception: End may need
        // those results while it holds this gate and drains the same actor.
        let _lifecycle = if selected && !tool_settlement {
            Some(self.selected_setup.lock().await)
        } else {
            None
        };
        if selected {
            let current = self.current_owner();
            let same = match (&captured, &current) {
                (Some(old), Some(current)) => {
                    Arc::ptr_eq(old, current) && old.run_id == current.run_id
                }
                (None, None) => true,
                _ => false,
            };
            if !same || current.as_ref().is_some_and(|owner| !owner.belongs_to(ctx)) {
                return Err(ProcessorError::new("stale Gemini selected run"));
            }
        }
        let result = if tool_settlement {
            match (&captured, self.tool_action(&frame.data)) {
                (Some(owner), Ok(Some(action))) => {
                    match self.enqueue_owned(owner, action, Self::origin(&frame)) {
                        Ok(()) => self.forward_original(frame, direction, ctx).await,
                        Err(error) => Err(error),
                    }
                }
                (_, Err(error)) => Err(error),
                _ => Err(ProcessorError::new("Gemini session is not ready")),
            }
        } else {
            self.process_frame(frame, direction, ctx).await
        };
        #[cfg(feature = "paired-history")]
        if let (Some(history), Err(error)) = (&self.history, &result) {
            // Record the exact route owner's primary before cleanup can be
            // cancelled, including Start which publishes its owner in this call.
            history.record_failure(error.message.clone());
            // Locally detected selected failures close releases and settle the
            // actual provider owner before fatal propagation when possible.
            if let Err(shutdown) = history.abort_service().await {
                return Err(ProcessorError::fatal(format!(
                    "{}; paired history shutdown failed: {}",
                    error.message, shutdown.message
                )));
            }
            let mut error = error.clone();
            error.fatal = true;
            return Err(error);
        }
        if result.as_ref().is_err_and(|error| error.fatal) {
            if let Some(owner) = captured {
                owner.record_result(&result);
                let settled = owner.settle(false).await;
                return selected::combine(result, settled);
            }
        }
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Some(owner) = self.current_owner() {
            if !owner.belongs_to(ctx) {
                return Err(ProcessorError::new(
                    "stale Gemini selected terminal context",
                ));
            }
            let closed = owner.close_input();
            let _lifecycle = self.selected_setup.lock().await;
            if !self.is_current_owner(&owner) {
                return selected::combine(closed, owner.settle(false).await);
            }
            let deadline = owner.terminal_deadline();
            let encoded = match deadline {
                Ok(deadline) => {
                    match tokio::time::timeout_at(deadline, self.video.finish()).await {
                        Ok(result) => result.map_err(processor_error),
                        Err(_) => Err(ProcessorError::fatal(
                            "Gemini selected video settlement deadline exceeded",
                        )),
                    }
                }
                Err(error) => Err(error),
            };
            if let Err(error) = &encoded {
                owner.record_failure(error.message.clone());
            }
            #[cfg(feature = "paired-history")]
            let route = if let Some(history) = &self.history {
                history.prepare_service_end().await
            } else {
                Ok(())
            };
            #[cfg(not(feature = "paired-history"))]
            let route = Ok(());
            owner.record_result(&route);
            let graceful = closed.is_ok() && encoded.is_ok() && route.is_ok();
            let joined = owner.settle(graceful).await;
            return selected::combine(
                selected::combine(closed, encoded),
                selected::combine(route, joined),
            );
        }
        #[cfg(feature = "paired-history")]
        if let Some(history) = &self.history {
            // End/Stop share this hook. Only the gate knows which terminal was
            // consumed and has already completed its bounded owner protocol.
            return history.prepare_service_end().await;
        }
        if let Ok(sender) = self.sender() {
            sender.finish().await.map_err(processor_error)?;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let Some(owner) = self.current_owner() {
            let closed = owner.close_input();
            // Cancellation must reach this exact session even while End owns
            // the lifecycle gate. Never wait for the gate before the watch.
            owner.sender.cancel();
            let _lifecycle = self.selected_setup.lock().await;
            if !self.is_current_owner(&owner) {
                return selected::combine(closed, owner.settle(false).await);
            }
            self.video.cancel();
            let encoded = match owner.terminal_deadline() {
                Ok(deadline) => {
                    match tokio::time::timeout_at(deadline, self.video.finish()).await {
                        Ok(result) => result.map_err(processor_error),
                        Err(_) => Err(ProcessorError::fatal(
                            "Gemini selected video settlement deadline exceeded",
                        )),
                    }
                }
                Err(error) => Err(error),
            };
            if let Err(error) = &encoded {
                owner.record_failure(error.message.clone());
            }
            #[cfg(feature = "paired-history")]
            let route = if let Some(history) = &self.history {
                history.cleanup_service().await
            } else {
                Ok(())
            };
            #[cfg(not(feature = "paired-history"))]
            let route = Ok(());
            owner.record_result(&route);
            let joined = owner.settle(false).await;
            self.retire_owner(&owner);
            return selected::combine(
                selected::combine(closed, encoded),
                selected::combine(route, joined),
            );
        }
        self.video.cancel();
        if let Some(sender) = self
            .sender
            .lock()
            .map_err(|_| ProcessorError::fatal("Gemini state lock poisoned"))?
            .take()
        {
            sender.cancel();
        }
        #[cfg(feature = "paired-history")]
        if let Some(history) = &self.history {
            // Core can reach cleanup after aborting every owned background
            // task. This releases scopes without falsely awaiting that lost
            // inner owner; normal terminal joins happened at the gate.
            let result = history.cleanup_service().await;
            self.video.finish().await.map_err(processor_error)?;
            return result;
        }
        self.video.finish().await.map_err(processor_error)?;
        Ok(())
    }
}

async fn receive_initial_history(
    owner: &SelectedSessionOwner,
    session: &mut GeminiSession,
    output: &ProcessorContext,
    user_id: String,
    emit_metrics: bool,
) -> Result<(), ProcessorError> {
    let sender = &owner.sender;
    let mut mapper = GeminiFrameMapper::new(SessionMode::Conversation, user_id);
    let mut generation = sender.generation();
    while let Some(received) = session.next_event_with_details().await {
        let received = match received {
            Ok(received) => received,
            Err(error) => {
                let message = error.to_string();
                owner.record_failure(message.clone());
                let emitted = output.push_error(error_frame(error)).await;
                return selected::combine(Err(ProcessorError::fatal(message)), emitted);
            }
        };
        if generation != sender.generation() {
            mapper.reset();
            generation = sender.generation();
        }
        let event = received.event;
        if matches!(event.event, Event::Interrupted) {
            mapper.reset();
            output.broadcast_interruption().await?;
            continue;
        }
        if matches!(event.event, Event::Usage(_)) && !emit_metrics {
            continue;
        }
        let is_output = event.event.is_output();
        for mut frame in mapper.frames(&event) {
            if let Some(details) = &received.usage_details {
                attach_usage_details(&mut frame, details);
            }
            if let FrameData::Error(error) = &frame.data {
                output.push_error(error.clone()).await?;
            } else if is_output {
                if let Some(result) = sender
                    .while_current(event.generation, output.push(frame, Direction::Downstream))
                    .await
                {
                    result?;
                }
            } else {
                output.push(frame, Direction::Downstream).await?;
            }
        }
    }
    Ok(())
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
                let mut llm_text = text.clone();
                llm_text.append_to_context = false;
                data.push(FrameData::LlmText(llm_text));
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
// Runtime-only enrichment leaves standalone public mapper behavior unchanged.
pub(crate) fn attach_usage_details(frame: &mut Frame, details: &crate::protocol::UsageDetails) {
    let FrameData::Metrics(metrics) = &frame.data else {
        return;
    };
    let [Metric::LlmUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens,
        ..
    }] = metrics.as_slice()
    else {
        return;
    };
    let mut usage = json!({
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
    });
    let mut reported = false;
    for (key, count) in [
        ("cache_read_input_tokens", details.cache_read_input_tokens),
        ("reasoning_tokens", details.reasoning_tokens),
        ("input_audio_tokens", details.input_audio_tokens),
        ("output_audio_tokens", details.output_audio_tokens),
        (
            "cache_read_input_audio_tokens",
            details.cache_read_input_audio_tokens,
        ),
        ("tool_use_prompt_tokens", details.tool_use_prompt_tokens),
    ] {
        if let Some(count) = count {
            usage[key] = json!(count);
            reported = true;
        }
    }
    if reported {
        frame.metadata.insert("llm_usage".into(), usage);
    }
}

pub(crate) fn processor_error(error: GeminiError) -> ProcessorError {
    let frame = error_frame(error);
    ProcessorError::new(frame.error).with_category(frame.category)
}
pub(crate) fn error_frame(error: GeminiError) -> ErrorFrame {
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

#[cfg(all(test, feature = "paired-history"))]
mod selected_control_tests {
    use super::*;

    #[test]
    fn external_transcript_and_tool_families_are_rejected_without_session_access() {
        let transcript = TranscriptionFrame {
            text: "foreign producer".into(),
            finalized: true,
            ..Default::default()
        };
        let detailed = pipecat_core::DetailedTranscriptionFrame {
            text: TextFrame::new("foreign producer"),
            user_id: String::new(),
            timestamp: String::new(),
            language: None,
            result: None,
            finalized: true,
        };
        for data in [
            FrameData::InputText(TextFrame::new("typed")),
            FrameData::LlmRun,
            FrameData::LlmContext(LlmContextFrame::default()),
            FrameData::LlmSetTools(pipecat_core::ToolSet::NotGiven),
            FrameData::LlmSetToolChoice(pipecat_core::ToolChoice::Auto),
            FrameData::LlmContinuationCheckpoint,
            FrameData::FunctionCallsStarted(vec![]),
            FrameData::FunctionCallsStartedDetailed(vec![]),
            FrameData::ExternalFunctionCallsStarted(vec![]),
            FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "foreign".into(),
                tool_call_id: "foreign".into(),
                run_llm: false,
            }),
            FrameData::Transcription(transcript.clone()),
            FrameData::InterimTranscription(transcript.clone()),
            FrameData::EagerTranscription(transcript.clone()),
            FrameData::Translation(transcript),
            FrameData::TranscriptionDetailed(detailed.clone()),
            FrameData::InterimTranscriptionDetailed(detailed.clone()),
            FrameData::EagerTranscriptionDetailed(detailed.clone()),
            FrameData::TranslationDetailed(detailed),
        ] {
            let mut frame = Frame::new(data);
            // Public flags cannot exempt a foreign frame from the selected
            // route's pre-consumer/live-service mutation check.
            frame
                .metadata
                .insert("realtime_context_update".into(), json!(true));
            assert!(reject_selected_live_frame(&frame).unwrap_err().fatal);
        }
    }

    #[test]
    fn independent_audio_vad_mute_and_terminal_controls_remain_admissible() {
        for data in [
            FrameData::InputAudio(AudioRawFrame {
                audio: vec![0, 0],
                sample_rate: 16_000,
                num_channels: 1,
                context_id: None,
            }),
            FrameData::UserStartedSpeaking,
            FrameData::UserStoppedSpeaking,
            FrameData::UserMuteStarted,
            FrameData::UserMuteStopped,
            FrameData::SttMute(true),
            FrameData::Interruption,
            FrameData::Start(StartFrame::default()),
            FrameData::End(pipecat_core::EndFrame::default()),
            FrameData::Stop,
            FrameData::Cancel(pipecat_core::CancelFrame::default()),
        ] {
            reject_selected_live_frame(&Frame::new(data)).unwrap();
        }
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

#[cfg(test)]
mod initial_history_owner_tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use pipecat_core::{Pipeline, PipelineConfig};
    use serde_json::Value;
    use std::time::Duration;
    use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
    use tokio_tungstenite::tungstenite::Message;

    const WAIT: Duration = Duration::from_secs(3);

    async fn listener() -> (String, mpsc::Receiver<Value>, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let (sent, received) = mpsc::channel(32);
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let setup = socket.next().await.unwrap().unwrap();
            assert!(setup.is_text());
            socket
                .send(Message::Text(
                    json!({"setupComplete":{}}).to_string().into(),
                ))
                .await
                .unwrap();
            while let Some(message) = socket.next().await {
                match message {
                    Ok(Message::Text(message)) => {
                        sent.send(serde_json::from_str(&message).unwrap())
                            .await
                            .unwrap();
                    }
                    Ok(Message::Close(_)) => {
                        let _ = socket.flush().await;
                        return;
                    }
                    Err(_) => return,
                    _ => {}
                }
            }
        });
        (endpoint, received, peer)
    }
    fn client(endpoint: String, manual: bool) -> GeminiClient {
        let mut config =
            GeminiConfig::new("fixture", "gemini-2.5-flash-native-audio-preview-12-2025");
        config.endpoint = endpoint;
        config.session.activity_detection.disabled = manual;
        config.session.modality = Modality::Text;
        config.limits.queue_capacity = 4;
        config.limits.finish_timeout = Duration::from_millis(200);
        config.limits.write_timeout = Duration::from_millis(200);
        GeminiClient::new(config)
            .unwrap()
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: false,
            })
            .unwrap()
    }
    async fn owned(
        manual: bool,
        id: u64,
    ) -> (
        Arc<GeminiLiveProcessor>,
        Arc<SelectedSessionOwner>,
        mpsc::Receiver<Value>,
        JoinHandle<()>,
    ) {
        let (endpoint, received, peer) = listener().await;
        let processor = Arc::new(GeminiLiveProcessor::new(client(endpoint, manual)));
        let session = processor.client.connect_owned(id).await.unwrap();
        let owner = SelectedSessionOwner::new(id, session, Duration::from_millis(400), None);
        processor.publish_owner(owner.clone());
        (processor, owner, received, peer)
    }
    fn context() -> LlmContextFrame {
        LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "original seed")],
            ..Default::default()
        }
    }
    async fn next(received: &mut mpsc::Receiver<Value>) -> Value {
        timeout(WAIT, received.recv()).await.unwrap().unwrap()
    }
    async fn settle(owner: &SelectedSessionOwner, peer: JoinHandle<()>) {
        timeout(WAIT, owner.settle(false)).await.unwrap().unwrap();
        assert!(owner.joined.joined());
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn rejected_seed_has_no_readiness_and_equal_retry_has_one_admission() {
        let (processor, owner, mut received, peer) = owned(false, 1).await;
        let held = owner.sender.test_hold_queue().unwrap();
        assert!(processor
            .seed_context(&context(), Origin::default())
            .is_err());
        {
            let state = processor.state.lock().unwrap();
            assert!(state.initial_context.is_none());
            assert!(!state.pending_first_stop);
        }
        drop(held);
        processor
            .seed_context(&context(), Origin::default())
            .unwrap();
        assert_eq!(
            next(&mut received).await["clientContent"]["turnComplete"],
            false
        );
        let held = owner.sender.test_hold_queue().unwrap();
        // Equal accepted source never needs a queue slot, even under pressure.
        processor
            .seed_context(&context(), Origin::default())
            .unwrap();
        let mut replacement = context();
        replacement.messages[0].content.clear();
        let error = processor
            .seed_context(&replacement, Origin::default())
            .unwrap_err();
        assert!(error.message.contains("replacing or replaying"));
        assert_eq!(
            processor.state.lock().unwrap().initial_context,
            Some(context())
        );
        drop(held);
        processor
            .enqueue(
                Action::RealtimeText("wire witness".into()),
                Origin::default(),
            )
            .unwrap();
        assert_eq!(
            next(&mut received).await["realtimeInput"]["text"],
            "wire witness"
        );
        settle(&owner, peer).await;
    }
    #[tokio::test]
    async fn first_stop_queue_rejection_preserves_activity_pending_and_preroll() {
        let (processor, owner, mut received, peer) = owned(true, 2).await;
        processor
            .seed_context(&context(), Origin::default())
            .unwrap();
        next(&mut received).await;
        processor
            .enqueue(Action::ActivityStart { preroll: vec![] }, Origin::default())
            .unwrap();
        assert!(next(&mut received).await["realtimeInput"]["activityStart"].is_object());
        {
            let mut state = processor.state.lock().unwrap();
            state.activity = true;
            state.preroll.extend([1, 2]);
        }
        let held = owner.sender.test_hold_queue().unwrap();
        assert!(processor.stop_selected(Origin::default()).is_err());
        {
            let state = processor.state.lock().unwrap();
            assert!(state.activity && state.pending_first_stop);
            assert_eq!(
                state.preroll.iter().copied().collect::<Vec<_>>(),
                vec![1, 2]
            );
        }
        drop(held);
        processor.stop_selected(Origin::default()).unwrap();
        assert!(next(&mut received).await["realtimeInput"]["activityEnd"].is_object());
        assert_eq!(
            next(&mut received).await["clientContent"]["turnComplete"],
            true
        );
        {
            let state = processor.state.lock().unwrap();
            assert!(!state.activity && !state.pending_first_stop && state.preroll.is_empty());
        }
        processor.stop_selected(Origin::default()).unwrap();
        processor
            .enqueue(Action::RealtimeText("after stop".into()), Origin::default())
            .unwrap();
        assert_eq!(
            next(&mut received).await["realtimeInput"]["text"],
            "after stop"
        );
        settle(&owner, peer).await;
    }
    #[tokio::test]
    async fn closed_first_stop_leaves_the_whole_local_transition_unchanged() {
        let (processor, owner, mut received, peer) = owned(false, 3).await;
        processor
            .seed_context(&context(), Origin::default())
            .unwrap();
        next(&mut received).await;
        owner.close_input().unwrap();
        assert!(processor.stop_selected(Origin::default()).is_err());
        assert!(processor.state.lock().unwrap().pending_first_stop);
        settle(&owner, peer).await;
    }
    #[tokio::test]
    async fn cancelled_owner_wait_is_reenterable_and_old_cleanup_cannot_clear_new_run() {
        let (processor, first, _received, peer) = owned(false, 4).await;
        let first_observer = processor.selected_join_observer().unwrap();
        let held_session = first.session.lock().await;
        assert!(timeout(Duration::from_millis(10), first.settle(false))
            .await
            .is_err());
        assert!(!first_observer.joined());
        assert!(Arc::ptr_eq(&processor.current_owner().unwrap(), &first));
        drop(held_session);
        settle(&first, peer).await;
        let (_second_processor, second, _received, peer) = owned(false, 5).await;
        processor.publish_owner(second.clone());
        processor.retire_owner(&first);
        assert!(Arc::ptr_eq(&processor.current_owner().unwrap(), &second));
        assert!(first_observer.joined());
        assert!(!processor.selected_join_observer().unwrap().joined());
        assert!(matches!(
            first
                .sender
                .try_send(Action::RealtimeText("stale".into()), Origin::default()),
            Err(GeminiError::Unsupported(_))
        ));
        settle(&second, peer).await;
    }

    struct OccupyReceiverSlot {
        processor: Arc<GeminiLiveProcessor>,
        setup_result: Mutex<Option<Result<(), ProcessorError>>>,
    }
    #[async_trait]
    impl FrameProcessor for OccupyReceiverSlot {
        fn name(&self) -> &str {
            "OccupyReceiverSlot"
        }
        async fn setup(
            &self,
            config: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            ctx.spawn(
                "occupied slot",
                false,
                std::future::pending::<Result<(), ProcessorError>>(),
            )?;
            let result = self.processor.setup(config, ctx).await;
            *self.setup_result.lock().unwrap() = Some(result.clone());
            result
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
            self.processor.cleanup().await
        }
    }
    #[tokio::test]
    async fn receive_spawn_rejection_retains_and_awaits_the_actual_actor() {
        let (endpoint, _received, peer) = listener().await;
        let processor = Arc::new(GeminiLiveProcessor::new(client(endpoint, false)));
        let fixture = Arc::new(OccupyReceiverSlot {
            processor: processor.clone(),
            setup_result: Mutex::new(None),
        });
        // Core reports setup failures on the started worker; the direct setup
        // result, not Pipeline::start's return type, witnesses spawn rejection.
        let mut worker = timeout(
            WAIT,
            Pipeline::new(vec![fixture.clone()]).start(PipelineConfig {
                max_background_tasks: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .unwrap();
        let rejected = fixture
            .setup_result
            .lock()
            .unwrap()
            .clone()
            .unwrap()
            .unwrap_err();
        assert!(rejected.fatal);
        assert_eq!(rejected.message, "background task limit reached");
        assert!(worker
            .errors()
            .iter()
            .any(|error| error.fatal && error.error == rejected.message));
        assert!(processor.selected_session_joined());
        let _ = worker.cancel("spawn rejection observed").await;
        let joined = timeout(WAIT, worker.join()).await.unwrap();
        assert!(joined.unwrap_err().to_string().contains(&rejected.message));
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
    struct CaptureContext(Mutex<Option<ProcessorContext>>);
    #[async_trait]
    impl FrameProcessor for CaptureContext {
        fn name(&self) -> &str {
            "CaptureSelectedContext"
        }
        async fn setup(
            &self,
            _: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            *self.0.lock().unwrap() = Some(ctx.clone());
            Ok(())
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
    async fn cancelled_handoff(release_receiver: bool, abort_cleanup: bool) {
        let (endpoint, _received, peer) = listener().await;
        let processor = Arc::new(GeminiLiveProcessor::new(client(endpoint, false)));
        let capture = Arc::new(CaptureContext(Mutex::new(None)));
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let ctx = capture.0.lock().unwrap().clone().unwrap();
        let (entered, entry) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        *processor.receiver_handoff.lock().unwrap() = Some(SelectedReceiveGate {
            reached: entered,
            release: released,
        });
        let starting = processor.clone();
        let startup = tokio::spawn(async move {
            starting
                .start_initial_history(&StartFrame::default(), &ctx)
                .await
        });
        timeout(WAIT, entry).await.unwrap().unwrap();
        let owner = processor.current_owner().unwrap();
        let observer = processor.selected_join_observer().unwrap();
        assert!(!observer.joined());
        startup.abort();
        assert!(startup.await.unwrap_err().is_cancelled());
        if release_receiver {
            let shutdown_hold =
                abort_cleanup.then(|| owner.sender.test_pause_before_selected_join());
            release.send(()).unwrap();
            if let Some((entered, release_join)) = shutdown_hold {
                timeout(WAIT, entered).await.unwrap().unwrap();
                assert!(!observer.joined());
                // Abort the actual receiver after it knows the dropped-ACK
                // error and cancels the socket, but before it awaits that actor.
                let error = timeout(WAIT, owner.settle(false))
                    .await
                    .unwrap()
                    .unwrap_err();
                assert!(error.message.contains("handoff was cancelled"));
                assert!(observer.joined());
                // The actor itself ended cleanly; only the retained owner can
                // preserve this locally detected primary across receiver abort.
                owner
                    .session
                    .lock()
                    .await
                    .selected_shutdown()
                    .await
                    .unwrap();
                assert!(owner
                    .settle(false)
                    .await
                    .unwrap_err()
                    .message
                    .contains("handoff was cancelled"));
                drop(release_join);
            }
            // The wrapper acquires the retained session, observes the dropped
            // ownership ACK, and joins that actor before releasing its guard.
            let result = timeout(WAIT, async {
                loop {
                    if observer.joined() {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                owner.settle(false).await
            })
            .await
            .unwrap();
            assert!(result
                .unwrap_err()
                .message
                .contains("handoff was cancelled"));
        } else {
            // Cleanup cancels the exact registered wrapper while it is still
            // waiting before ACK, then borrows and joins the retained actor.
            timeout(WAIT, processor.cleanup()).await.unwrap().unwrap();
            drop(release);
        }
        assert!(observer.joined());
        let _ = worker.cancel("selected handoff test completed").await;
        timeout(WAIT, worker.join()).await.unwrap().unwrap();
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn cancelled_start_before_receiver_ack_remains_recoverable() {
        cancelled_handoff(false, false).await;
    }
    #[tokio::test]
    async fn dropped_receiver_ack_joins_actor_and_preserves_handoff_failure() {
        cancelled_handoff(true, false).await;
    }
    #[tokio::test]
    async fn aborted_receiver_cleanup_preserves_dropped_ack_primary_and_real_join() {
        cancelled_handoff(true, true).await;
    }
    #[tokio::test]
    async fn cancelled_process_cleanup_preserves_fatal_forwarding_failure() {
        let (processor, owner, _received, peer) = owned(false, 6).await;
        let capture = Arc::new(CaptureContext(Mutex::new(None)));
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(PipelineConfig {
                output_capacity: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let ctx = capture.0.lock().unwrap().clone().unwrap();
        // The unconsumed real Start fills the one-slot output lane. Forwarding
        // the valid preseed frame creates a local fatal error, not a socket error.
        let held = owner.session.lock().await;
        {
            let processing = processor.process(
                Frame::new(FrameData::InputText(TextFrame::new(
                    "valid suppressed input",
                ))),
                Direction::Downstream,
                &ctx,
            );
            tokio::pin!(processing);
            assert!(matches!(
                futures_util::poll!(&mut processing),
                std::task::Poll::Pending
            ));
            assert!(!owner.accepts_input());
            assert!(!owner.joined.joined());
        }
        drop(held);
        let failure = timeout(WAIT, owner.settle(false))
            .await
            .unwrap()
            .unwrap_err();
        assert!(failure.message.contains("worker output queue is full"));
        assert!(owner.joined.joined());
        owner
            .session
            .lock()
            .await
            .selected_shutdown()
            .await
            .unwrap();
        assert!(owner
            .settle(false)
            .await
            .unwrap_err()
            .message
            .contains("worker output queue is full"));
        assert!(timeout(WAIT, worker.join())
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("worker output queue is full"));
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn held_old_runtime_seed_and_stop_cannot_mutate_a_restarted_run() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let mut records = Vec::new();
            for run in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
                socket.next().await.unwrap().unwrap();
                socket
                    .send(Message::Text(
                        json!({"setupComplete":{}}).to_string().into(),
                    ))
                    .await
                    .unwrap();
                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Text(message)) => {
                            records.push((run, serde_json::from_str::<Value>(&message).unwrap()))
                        }
                        Ok(Message::Close(_)) => {
                            let _ = socket.flush().await;
                            break;
                        }
                        Err(_) => break,
                        _ => {}
                    }
                }
            }
            records
        });
        let processor = Arc::new(GeminiLiveProcessor::new(client(endpoint, false)));
        let first_capture = Arc::new(CaptureContext(Mutex::new(None)));
        let mut first_worker = Pipeline::new(vec![first_capture.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let first_ctx = first_capture.0.lock().unwrap().clone().unwrap();
        processor
            .setup(&StartFrame::default(), &first_ctx)
            .await
            .unwrap();
        processor
            .process(
                Frame::new(FrameData::LlmContext(context())),
                Direction::Downstream,
                &first_ctx,
            )
            .await
            .unwrap();
        let first_observer = processor.selected_join_observer().unwrap();
        let (release, held) = tokio::sync::oneshot::channel();
        let delayed = processor.clone();
        let old_ctx = first_ctx.clone();
        let stale = tokio::spawn(async move {
            held.await.unwrap();
            let seed = delayed
                .process(
                    Frame::new(FrameData::LlmContext(LlmContextFrame {
                        messages: vec![ChatMessage::new(ChatRole::User, "stale seed")],
                        ..Default::default()
                    })),
                    Direction::Downstream,
                    &old_ctx,
                )
                .await;
            let terminal = delayed.prepare_end(&old_ctx).await;
            let stop = delayed
                .process(Frame::new(FrameData::Stop), Direction::Downstream, &old_ctx)
                .await;
            (seed, terminal, stop)
        });
        processor.prepare_end(&first_ctx).await.unwrap();
        processor.cleanup().await.unwrap();
        assert!(first_observer.joined());
        let second_capture = Arc::new(CaptureContext(Mutex::new(None)));
        let mut second_worker = Pipeline::new(vec![second_capture.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let second_ctx = second_capture.0.lock().unwrap().clone().unwrap();
        processor
            .setup(&StartFrame::default(), &second_ctx)
            .await
            .unwrap();
        let second_owner = processor.current_owner().unwrap();
        let second_observer = processor.selected_join_observer().unwrap();
        release.send(()).unwrap();
        let (seed, terminal, stop) = timeout(WAIT, stale).await.unwrap().unwrap();
        assert!(seed.is_err() && terminal.is_err() && stop.is_err());
        assert!(Arc::ptr_eq(
            &processor.current_owner().unwrap(),
            &second_owner
        ));
        assert!(processor.state.lock().unwrap().initial_context.is_none());
        assert!(!second_observer.joined());
        assert!(!processor.sender().unwrap().is_finishing());
        processor
            .process(
                Frame::new(FrameData::LlmContext(LlmContextFrame {
                    messages: vec![ChatMessage::new(ChatRole::User, "fresh seed")],
                    ..Default::default()
                })),
                Direction::Downstream,
                &second_ctx,
            )
            .await
            .unwrap();
        processor.prepare_end(&second_ctx).await.unwrap();
        processor.cleanup().await.unwrap();
        assert!(second_observer.joined());
        let _ = first_worker.cancel("old run checked").await;
        let _ = second_worker.cancel("new run checked").await;
        timeout(WAIT, first_worker.join()).await.unwrap().unwrap();
        timeout(WAIT, second_worker.join()).await.unwrap().unwrap();
        let records = timeout(WAIT, peer).await.unwrap().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, 0);
        assert_eq!(
            records[0].1["clientContent"]["turns"][0]["parts"][0]["text"],
            "original seed"
        );
        assert_eq!(records[1].0, 1);
        assert_eq!(
            records[1].1["clientContent"]["turns"][0]["parts"][0]["text"],
            "fresh seed"
        );
    }
}
