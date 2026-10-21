//! PCM frame adapter for the persistent native Realtime session.
use crate::context::{project, session as context_session, ContextState, HeldContinuation};
use crate::*;
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, ErrorCategory, ErrorFrame, Frame, FrameData,
    FrameProcessor, FunctionCall, InvocationFence, LlmContextData, LlmServiceMetadataFrame, Metric,
    NativeRealtimeContextCause, NativeToolProgressFailure, NativeToolProgressProviderLimits,
    NativeToolProgressRole, NativeToolProgressRoute, NativeToolProgressRouteState,
    ProcessorContext, ProcessorError, RealtimeContextAdmission, RealtimeContextIntent,
    ServiceMetadataFrame, Setting, StartFrame, TextFrame, ToolSet, TranscriptionFrame,
    TtsStartedFrame, TtsStoppedFrame, UserTurnStrategies, UserTurnStrategy,
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
#[path = "processor_progress.rs"]
mod progress_consumer;
use progress_consumer::ProgressProvider;
#[path = "processor_deferred.rs"]
mod progress_deferred;
use progress_deferred::DeferredProvider;
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

/// Observed connection state, not inference acceptance or playback evidence.
/// Ready means the first actual `session.updated` event (no request-ID match).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RealtimeConnectionState {
    Connecting,
    Ready,
    Closed,
}
struct ConnectionStateGuard(Option<tokio::sync::watch::Sender<RealtimeConnectionState>>);
impl Drop for ConnectionStateGuard {
    fn drop(&mut self) {
        if let Some(state) = &self.0 {
            state.send_replace(RealtimeConnectionState::Closed);
        }
    }
}

// This closed whitelist contains no response-bearing event or executable control.
fn is_session_input(event: &Event) -> bool {
    matches!(
        event,
        Event::SpeechStarted { .. }
            | Event::SpeechStopped { .. }
            | Event::InputTranscriptionDelta { .. }
            | Event::InputTranscriptionCompleted { .. }
    )
}

fn observe_generation<T>(
    sender: &RealtimeSender,
    generation: u64,
    session_input: bool,
    observe: impl FnOnce() -> T,
) -> Option<T> {
    if session_input {
        sender.with_open_session(observe)
    } else {
        sender.with_current_generation(generation, observe)
    }
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
    connection_state: tokio::sync::watch::Sender<RealtimeConnectionState>,
    progress_provider: Mutex<ProgressProvider>,
    deferred_provider: Mutex<DeferredProvider>,
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
    preserve_session_input: bool,
    manual_turns: bool,
    audio_preroll_override: bool,
    video: crate::video::VideoEncoder,
    video_input: Mutex<VideoInput>,
    video_paused: AtomicBool,
    start_video_paused: bool,
    video_closed: AtomicBool,
    progress_route: Option<NativeToolProgressRoute>,
    progress_start: Mutex<Option<StartFrame>>,
    progress_output: Mutex<Option<ProcessorContext>>,
    selected_session: Mutex<Option<Arc<tokio::sync::Mutex<RealtimeSession>>>>,
    selected_join_observer: Mutex<Option<Arc<AtomicBool>>>,
    final_tool_terminal: Arc<AtomicBool>,
    final_tool_draining: AtomicBool,
    final_tool_result: Arc<Mutex<Option<Result<(), ProcessorError>>>>,
}
impl OpenAiRealtimeProcessor {
    /// A watch subscription cannot lose readiness/closure wakeups. Closed is
    /// terminal, including failed setup and an aborted receive owner.
    pub fn subscribe_connection_state(
        &self,
    ) -> tokio::sync::watch::Receiver<RealtimeConnectionState> {
        self.connection_state.subscribe()
    }
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
            connection_state: tokio::sync::watch::channel(RealtimeConnectionState::Connecting).0,
            progress_provider: Mutex::new(ProgressProvider::default()),
            deferred_provider: Mutex::new(DeferredProvider::default()),
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
            preserve_session_input: false,
            manual_turns,
            audio_preroll_override: false,
            video: Default::default(),
            video_input: Default::default(),
            video_paused: AtomicBool::new(false),
            start_video_paused: false,
            video_closed: AtomicBool::new(false),
            progress_route: None,
            progress_start: Mutex::new(None),
            progress_output: Mutex::new(None),
            selected_session: Mutex::new(None),
            selected_join_observer: Mutex::new(None),
            final_tool_terminal: Arc::new(AtomicBool::new(false)),
            final_tool_draining: AtomicBool::new(false),
            final_tool_result: Arc::new(Mutex::new(None)),
        })
    }
    /// Opt in to an application-owned final-tool End gate placed immediately
    /// before this provider. Its wait must not block the downstream executor's
    /// upstream native receipts, which terminate at this provider.
    ///
    /// End drains already announced tools without starting a new response.
    /// Newly announced callbacks after Finish and unexpected provider EOF fail
    /// closed. The default OpenAI and selected-progress lifecycles are unchanged.
    pub fn with_final_tool_app(mut self) -> Result<Self, RealtimeError> {
        if self.progress_route.is_some() {
            return Err(RealtimeError::Invalid(
                "final-tool application mode cannot use a selected-progress route",
            ));
        }
        self.client.final_tool_app = true;
        Ok(self)
    }
    /// Drain the real socket actor for the application's intentional End.
    /// This must run in a gate before the provider, not in the same lane that
    /// delivers its native tool receipts. Cancel uses the independent system
    /// lane and cleanup retains the actual actor owner if this wait is aborted.
    pub async fn drain_final_tools(&self) -> Result<(), ProcessorError> {
        if !self.client.final_tool_app {
            return Err(ProcessorError::fatal(
                "final-tool drain requires explicit application mode",
            ));
        }
        let limits = &self.client.config().limits;
        let budget = limits
            .response_timeout
            .checked_mul(2)
            .and_then(|value| value.checked_add(limits.operation_timeout))
            .and_then(|value| value.checked_add(limits.close_timeout))
            .ok_or_else(|| ProcessorError::fatal("final-tool drain deadline overflow"))?;
        let sender = self.sender()?;
        self.final_tool_terminal.store(true, Ordering::Release);
        let first = !self.final_tool_draining.swap(true, Ordering::AcqRel);
        let drain = async {
            if first {
                if let Err(error) = sender.finish().await {
                    publish_final_tool_result(
                        &self.final_tool_result,
                        Err(ProcessorError::fatal(error.to_string())),
                    );
                    sender.close();
                }
            }
            // The receive owner holds this same mutex until its final result
            // is published. A joined-observer bit alone is not enough.
            let joined = self.join_selected_session().await;
            let received = self
                .final_tool_result
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
                .unwrap_or_else(|| {
                    Err(ProcessorError::fatal(
                        "final-tool receive owner ended without a terminal result",
                    ))
                });
            with_cleanup_result(received, joined, "provider cleanup")
        };
        match tokio::time::timeout(budget, drain).await {
            Ok(result) => result,
            Err(_) => {
                sender.close();
                let error = ProcessorError::fatal("final-tool drain timed out");
                publish_final_tool_result(&self.final_tool_result, Err(error.clone()));
                // Returning fatal lets core abort a blocked receive wrapper,
                // then cleanup reclaims the still-published actual actor.
                Err(error)
            }
        }
    }
    /// Opt in to one actual same-runtime reporting composition. Role/configuration
    /// validation happens before the selected Start opens its provider socket.
    pub fn with_progress_route(mut self, route: NativeToolProgressRoute) -> Self {
        self.context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .selected = Some(crate::context::SelectedContext::new(route.clone()));
        self.progress_route = Some(route);
        self
    }
    fn progress_limits(&self) -> NativeToolProgressProviderLimits {
        let limits = &self.client.config().limits;
        NativeToolProgressProviderLimits {
            response_timeout: limits.response_timeout,
            operation_timeout: limits.operation_timeout,
            close_timeout: limits.close_timeout,
            max_message_bytes: limits.max_message_bytes,
            max_in_flight: limits.max_outstanding_tools,
        }
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
    /// Preserve provider-authored user input across bot-response interruption.
    /// Only speech boundaries and final/interim input transcripts are exempt
    /// from response generation retirement. Runtime termination, bounded queues,
    /// and every assistant response fence remain unchanged. Disabled by default.
    pub fn with_interruption_resilient_user_input(mut self) -> Self {
        self.preserve_session_input = true;
        self
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
        if let Some(selected) = &mut next.selected {
            selected.prepare(&context.messages)?;
        }
        let selected_permission = self
            .progress_route
            .as_ref()
            .map(|route| route.inference_permission());
        let reasons = cause
            .as_ref()
            .map_or(&[][..], |cause| cause.assistant_native_progress_reasons());
        let selected_alternatives = if reasons.is_empty() {
            None
        } else {
            let emission = fence
                .as_ref()
                .ok_or_else(|| ProcessorError::fatal("selected context lacks emission scope"))?;
            let mut alternatives = Vec::with_capacity(reasons.len());
            for reason in reasons {
                if reason.is_allowed() {
                    alternatives.push(crate::client::SelectedAlternative::intersect(
                        reason.permission().clone(),
                        emission,
                        reason.publication_fence(),
                    )?);
                }
            }
            Some(alternatives)
        };
        let draining =
            self.client.final_tool_app && self.final_tool_draining.load(Ordering::Acquire);
        if draining {
            next.continuation_owed = None;
        }
        let run = run
            && !draining
            && selected_permission.as_ref().is_none_or(|p| p.is_allowed())
            && selected_alternatives.as_ref().is_none_or(|v| !v.is_empty());
        let permission_revision = if reasons.is_empty() {
            cause
                .as_ref()
                .and_then(|cause| cause.assistant_native_outcome_id())
                .map(|id| next.outcome_revision(id))
                .transpose()?
        } else {
            reasons
                .iter()
                .filter_map(|reason| next.progress_revision(reason.event_id()).ok())
                .max()
        };
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
                if continuation && next.selected.is_none() && next.has_open_calls() {
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
                    || selected_alternatives.is_some()
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
        if draining && continuation {
            // The final native receipt may already have made the actor fully
            // settled. Keep the validated paired history/cause below, but a
            // late assistant continuation cannot update a closed session or
            // reopen inference. Never silently accept a new provider item.
            if actions
                .iter()
                .any(|action| matches!(action, Action::CreateItem { .. }))
            {
                return Err(ProcessorError::fatal(
                    "final-tool drain cannot import new provider history",
                ));
            }
            actions.clear();
        }
        if !actions.is_empty() {
            if let Some(route) = &self.progress_route {
                let prerequisites = route.pending_provider_history(&context.messages)?;
                self.queue_or_defer(
                    actions,
                    fence,
                    selected_permission,
                    selected_alternatives,
                    prerequisites,
                )?;
            } else {
                self.sender()?
                    .try_send_batch_with_fence(actions, fence)
                    .map_err(processor_error)?;
            }
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
            if outcome.progress_terminal().is_some() {
                next.selected_terminal(outcome.id(), limit)?;
            }
        }
        if let Some(action) = action {
            self.enqueue(action)?;
        }
        let reevaluate = if fresh
            && !next.has_open_calls()
            && !(self.client.final_tool_app && self.final_tool_draining.load(Ordering::Acquire))
        {
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
            self.send_inference_batch(actions, Some(fence))?;
        }
        *state = next;
        Ok(())
    }
    async fn connect_and_receive(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        // A persistent receive owner has the actual setup lifetime, not the
        // initial Start invocation's epoch. Individual responses still carry
        // their original provider generation and inference fences below.
        let output = if self.progress_route.is_some() {
            self.progress_output
                .lock()
                .map_err(|_| ProcessorError::fatal("realtime selected output lock poisoned"))?
                .take()
                .ok_or_else(|| ProcessorError::fatal("selected receive lacks setup context"))?
        } else {
            ctx.clone()
        };
        let mut setup_guard = ConnectionStateGuard(Some(self.connection_state.clone()));
        let session = self.client.connect().await.map_err(processor_error)?;
        let sender = session.sender();
        let joined = session.join_observer();
        let session = Arc::new(tokio::sync::Mutex::new(session));
        // Publish the recovery owner for every profile before any further await,
        // including an error shutdown or one-shot handoff. Cancel may abort
        // ordinary paired setup just as it can abort selected-route Start.
        *self
            .selected_session
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(session.clone());
        *self
            .selected_join_observer
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(joined);
        if self
            .progress_route
            .as_ref()
            .is_some_and(NativeToolProgressRoute::is_closed)
        {
            return with_cleanup_result(
                Err(ProcessorError::fatal(
                    "selected route closed during provider connection",
                )),
                session
                    .lock()
                    .await
                    .shutdown()
                    .await
                    .map_err(processor_error),
                "provider cleanup",
            );
        }
        let prepare = (|| {
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
            Ok::<_, ProcessorError>(())
        })();
        if let Err(error) = prepare {
            return with_cleanup_result(
                Err(error),
                session
                    .lock()
                    .await
                    .shutdown()
                    .await
                    .map_err(processor_error),
                "provider cleanup",
            );
        }
        let user_id = self.user_id.clone();
        let preserve_session_input = self.preserve_session_input;
        let context = self.context.clone();
        let audio_items = self.audio_items.clone();
        let limit = self.client.config().limits.max_message_bytes;
        let ready = self.receive_ready.clone();
        let enable_usage_metrics = config.enable_usage_metrics;
        let progress_route = self.progress_route.clone();
        let service_name = self.client.config().service_name();
        let final_tool_app = self.client.final_tool_app;
        let final_tool_terminal = self.final_tool_terminal.clone();
        let final_tool_result = self.final_tool_result.clone();
        let (handoff, receive) =
            tokio::sync::oneshot::channel::<Arc<tokio::sync::Mutex<RealtimeSession>>>();
        let (owned, ownership) = tokio::sync::oneshot::channel::<()>();
        let connection_state = self.connection_state.clone();
        let receive_guard = ConnectionStateGuard(Some(connection_state.clone()));
        let task = ctx.spawn("openai-realtime-receive", false, async move {
            let _connection_guard = receive_guard;
            let Ok(owner) = receive.await else { return Ok(()); };
            let mut session = owner.lock().await;
            let _ = owned.send(());
            let closed = wait_progress_route_closed(progress_route.as_ref());
            tokio::pin!(closed);
            let mut mapper = RealtimeFrameMapper::new(user_id);
            mapper.service_name = service_name;
            let mut generation = sender.generation();
            let result = async {
                tokio::select! {
                    biased;
                    _ = &mut closed => return selected_receive_close_result(progress_route.as_ref()),
                    _ = ready.notified() => (),
                }
                loop {
                    let result = tokio::select! {
                        biased;
                        _ = &mut closed => return selected_receive_close_result(progress_route.as_ref()),
                        result = session.next_received() => result,
                    };
                    let Some(result) = result else {
                        // Select the EOF meaning now. Later shutdown awaits or
                        // a concurrent End cannot relabel an unexpected close.
                        if progress_route.as_ref().is_some_and(|route| matches!(
                            route.state(), NativeToolProgressRouteState::Open | NativeToolProgressRouteState::Draining
                        )) {
                            return Err(ProcessorError::fatal("selected provider ended before route shutdown"));
                        }
                        if final_tool_app && !final_tool_terminal.load(Ordering::Acquire) {
                            return Err(ProcessorError::fatal(
                                "final-tool provider ended before intentional shutdown",
                            ));
                        }
                        break;
                    };
                    let received = result.map_err(processor_error)?;
                    let event = received.event;
                    if matches!(&event.event, Event::SessionUpdated { .. }) {
                        connection_state.send_if_modified(|state| {
                            if *state == RealtimeConnectionState::Connecting {
                                *state = RealtimeConnectionState::Ready;
                                true
                            } else { false }
                        });
                    }
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
                    let session_input = preserve_session_input && is_session_input(&event.event);
                    let observed = observe_generation(&sender, event.generation, session_input, || {
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
                    for mut frame in frames {
                        if matches!(frame.data, FrameData::FunctionCallsStarted(_))
                            && progress_route.as_ref().is_some_and(|route| !route.accepts_new_calls())
                        {
                            if let Some(route) = &progress_route {
                                route.close(NativeToolProgressFailure::ProviderRejected);
                            }
                            return Err(ProcessorError::fatal(
                                "provider announced a new selected tool call after admission closed",
                            ));
                        }
                        if let FrameData::Metrics(metrics) = &frame.data {
                            if let [Metric::LlmUsage {
                                prompt_tokens,
                                completion_tokens,
                                total_tokens,
                                ..
                            }] = metrics.as_slice()
                            {
                                if !enable_usage_metrics {
                                    continue;
                                }
                                if let Some(details) = received.usage_details {
                                    if let Some(value) = details.metadata(
                                        *prompt_tokens,
                                        *completion_tokens,
                                        *total_tokens,
                                    ) {
                                        frame.metadata.insert("llm_usage".into(), value);
                                    }
                                }
                            }
                        }
                        if let FrameData::Error(error) = frame.data {
                            if final_tool_app {
                                return Err(ProcessorError::fatal(error.error));
                            }
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
                        if session_input && direction == Direction::Upstream {
                            frame.interruptible = false;
                        }
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
            // Join first, then release the recoverable owner before route
            // revocation or reporting a fatal selected transport failure.
            let shutdown = session.shutdown().await.map_err(processor_error);
            let mut result = with_cleanup_result(result, shutdown, "provider cleanup");
            if final_tool_app {
                if let Err(error) = &mut result {
                    error.fatal = true;
                }
                publish_final_tool_result(&final_tool_result, result.clone());
            }
            drop(session);
            if let Some(route) = &progress_route {
                if let Err(error) = result {
                    route.close(NativeToolProgressFailure::ProviderRejected);
                    return Err(ProcessorError::fatal(error.to_string()));
                }
                return Ok(());
            }
            result
        });
        match task {
            Ok(_) => {
                setup_guard.0.take();
                if let Err(session) = handoff.send(session) {
                    return with_cleanup_result(
                        Err(ProcessorError::fatal(
                            "realtime receive owner closed before handoff",
                        )),
                        session
                            .lock()
                            .await
                            .shutdown()
                            .await
                            .map_err(processor_error),
                        "provider cleanup",
                    );
                }
                // Ensure graceful End cannot acquire the session before its
                // receive owner and turn ordered finish into an early close.
                ownership.await.map_err(|_| {
                    ProcessorError::fatal("realtime receive owner ended before startup handoff")
                })
            }
            Err(error) => with_cleanup_result(
                Err(error),
                session
                    .lock()
                    .await
                    .shutdown()
                    .await
                    .map_err(processor_error),
                "provider cleanup",
            ),
        }
    }
    /// Whether this connection's actual socket-actor JoinHandle was
    /// awaited. False before connection. True also covers a forced abort followed
    /// by its actual join; it does not claim a clean close or remote acceptance.
    pub fn session_joined(&self) -> bool {
        self.selected_join_observer
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .is_some_and(|joined| joined.load(Ordering::Acquire))
    }
    /// Compatibility alias for selected-progress users.
    pub fn selected_session_joined(&self) -> bool {
        self.session_joined()
    }
    async fn join_selected_session(&self) -> Result<(), ProcessorError> {
        let owner = self
            .selected_session
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        let Some(owner) = owner else {
            return Ok(());
        };
        // No std state lock spans this await. Keep the processor reference until
        // actual shutdown completes so aborting a waiter cannot detach the actor.
        let result = owner.lock().await.shutdown().await.map_err(processor_error);
        self.selected_session
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        result
    }
    fn close(&self) -> Result<(), ProcessorError> {
        if self.client.final_tool_app {
            self.final_tool_terminal.store(true, Ordering::Release);
        }
        self.progress_output
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        *self
            .deferred_provider
            .lock()
            .map_err(|_| ProcessorError::fatal("deferred provider lock poisoned"))? =
            DeferredProvider::default();
        *self
            .progress_provider
            .lock()
            .map_err(|_| ProcessorError::fatal("progress consumer lock poisoned"))? =
            ProgressProvider::default();
        if let Some(route) = &self.progress_route {
            route.close(NativeToolProgressFailure::Closed);
        }
        self.progress_start
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime selected Start lock poisoned"))?
            .take();
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
async fn wait_progress_route_closed(route: Option<&NativeToolProgressRoute>) {
    let Some(route) = route else {
        std::future::pending::<()>().await;
        return;
    };
    let mut changes = route.subscribe();
    loop {
        if route.is_closed() {
            return;
        }
        if changes.changed().await.is_err() {
            return;
        }
    }
}
fn selected_receive_close_result(
    route: Option<&NativeToolProgressRoute>,
) -> Result<(), ProcessorError> {
    if route.is_some_and(|route| route.state() == NativeToolProgressRouteState::Failed) {
        Err(ProcessorError::fatal("selected tool progress route failed"))
    } else {
        Ok(())
    }
}

#[async_trait]
impl FrameProcessor for OpenAiRealtimeProcessor {
    fn name(&self) -> &str {
        self.client.config().service_name()
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut state_guard = ConnectionStateGuard(Some(self.connection_state.clone()));
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(ProcessorError::new(
                "realtime processors are single-session; construct a new processor",
            ));
        }
        if self.client.final_tool_app && self.progress_route.is_some() {
            return Err(ProcessorError::fatal(
                "final-tool application mode cannot use a selected-progress route",
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
        let result =
            if let Some(route) = &self.progress_route {
                route.register_role(NativeToolProgressRole::Provider, ctx, None)?;
                *self.progress_start.lock().map_err(|_| {
                    ProcessorError::fatal("realtime selected Start lock poisoned")
                })? = Some(config.clone());
                *self.progress_output.lock().map_err(|_| {
                    ProcessorError::fatal("realtime selected output lock poisoned")
                })? = Some(ctx.clone());
                Ok(())
            } else {
                self.connect_and_receive(config, ctx).await
            };
        if result.is_ok() {
            state_guard.0.take();
        }
        result
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let Some(selected_frame) =
            self.consume_progress(frame, direction, ctx)
                .map_err(|error| {
                    if let Some(route) = &self.progress_route {
                        route.close(pipecat_core::NativeToolProgressFailure::ProviderRejected);
                        ProcessorError::fatal(error.to_string())
                    } else {
                        error
                    }
                })?
        else {
            return Ok(());
        };
        frame = selected_frame;
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
            if let Some(route) = &self.progress_route {
                let config = self
                    .progress_start
                    .lock()
                    .map_err(|_| ProcessorError::fatal("realtime selected Start lock poisoned"))?
                    .take()
                    .ok_or_else(|| {
                        ProcessorError::fatal("selected provider Start lacks actual setup")
                    })?;
                let limits = self.progress_limits();
                if let Err(error) = route.validate_configuration(ctx, limits) {
                    self.progress_output
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take();
                    route.close(NativeToolProgressFailure::InvalidProvenance);
                    return Err(error);
                }
                if let Err(error) = self.connect_and_receive(&config, ctx).await {
                    route.close(NativeToolProgressFailure::ProviderRejected);
                    return Err(error);
                }
                if let Err(error) = route.validate_and_open(ctx, limits) {
                    route.close(NativeToolProgressFailure::InvalidProvenance);
                    return Err(error);
                }
            }
            let forwarded = async {
                ctx.push(frame, direction).await?;
                let external = UserTurnStrategy::External {
                    enable_interruptions: true,
                };
                ctx.broadcast(Frame::new(FrameData::LlmServiceMetadata(
                    LlmServiceMetadataFrame {
                        base: ServiceMetadataFrame {
                            service_name: self.name().into(),
                            user_turn_strategies: (!self.manual_turns).then(|| {
                                UserTurnStrategies {
                                    start: Some(vec![external.clone()]),
                                    stop: Some(vec![external]),
                                }
                            }),
                        },
                        is_realtime_service: true,
                    },
                )))
                .await
            }
            .await;
            if let Err(error) = forwarded {
                if let Some(route) = &self.progress_route {
                    route.close(NativeToolProgressFailure::QueueClosed);
                }
                return Err(error);
            }
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
                if let Some(selected_cause) = cause.as_ref().filter(|cause| {
                    !cause.assistant_native_progress_reasons().is_empty()
                        || cause
                            .assistant_selected_native_terminal_route_id()
                            .is_some()
                }) {
                    self.progress_route
                        .as_ref()
                        .ok_or_else(|| {
                            ProcessorError::fatal("selected context without provider route")
                        })?
                        .validate_consumed_progress_context(ctx, &frame, selected_cause)?;
                } else {
                    frame
                        .require_no_native_llm_continuation()
                        .map_err(|e| ProcessorError::new(e.to_string()))?;
                }
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
                if let Some(selected_cause) = cause.as_ref().filter(|cause| {
                    !cause.assistant_native_progress_reasons().is_empty()
                        || cause
                            .assistant_selected_native_terminal_route_id()
                            .is_some()
                }) {
                    self.progress_route
                        .as_ref()
                        .ok_or_else(|| {
                            ProcessorError::fatal("selected context without provider route")
                        })?
                        .validate_consumed_progress_context(ctx, &frame, selected_cause)?;
                } else {
                    frame
                        .require_no_native_llm_continuation()
                        .map_err(|e| ProcessorError::new(e.to_string()))?;
                }
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
                FrameData::UserStoppedSpeaking if self.manual_turns => self.send_inference_batch(
                    vec![
                        Action::CommitAudio,
                        Action::CreateResponse(ResponseOptions::default()),
                    ],
                    Some(ctx.invocation_fence()?),
                )?,
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
                    self.send_inference_batch(actions, Some(ctx.invocation_fence()?))?;
                    *state = next;
                    return Ok(());
                }
                FrameData::LlmRun => {
                    self.send_inference_batch(
                        vec![Action::CreateResponse(ResponseOptions::default())],
                        Some(ctx.invocation_fence()?),
                    )?;
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
        if self.client.final_tool_app {
            let encoded = self.video.finish().await.map_err(processor_error);
            let drained = self.drain_final_tools().await;
            return with_cleanup_result(drained, encoded, "encoder cleanup");
        }
        let graceful = self
            .progress_route
            .as_ref()
            .is_none_or(|route| route.state() == NativeToolProgressRouteState::EndForwarded);
        if !graceful {
            if let Ok(sender) = self.sender() {
                sender.close();
            }
        }
        let encoded = self.video.finish().await.map_err(processor_error);
        let finished = if graceful {
            match self.sender() {
                Ok(sender) => sender.finish().await.map_err(processor_error),
                Err(_) => Ok(()),
            }
        } else {
            Ok(())
        };
        if finished.is_err() {
            if let Ok(sender) = self.sender() {
                sender.close();
            }
        }
        // Stop skips cleanup. Even encoding/finish errors must not bypass the
        // actual inner actor join; core subsequently joins the receive wrapper.
        let joined = self.join_selected_session().await;
        with_cleanup_result(
            with_cleanup_result(finished, encoded, "encoder cleanup"),
            joined,
            "provider cleanup",
        )
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.connection_state
            .send_replace(RealtimeConnectionState::Closed);
        let closed = self.close();
        let encoded = self.video.finish().await.map_err(processor_error);
        // Cancel/Failed abort the receive task before cleanup. Its async mutex
        // guard drops, while this processor still owns the recoverable session.
        let joined = self.join_selected_session().await;
        with_cleanup_result(
            with_cleanup_result(closed, encoded, "encoder cleanup"),
            joined,
            "provider cleanup",
        )
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
pub(crate) fn project_timeout(frame: &mut Frame) -> Result<(), ProcessorError> {
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
pub struct RealtimeFrameMapper {
    user_id: String,
    service_name: &'static str,
    speaking: Option<String>,
    partial_samples: HashMap<(String, u32), u8>,
}
impl Default for RealtimeFrameMapper {
    fn default() -> Self {
        Self {
            user_id: String::new(),
            service_name: "OpenAiRealtimeService",
            speaking: None,
            partial_samples: HashMap::new(),
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
                        processor: self.service_name.into(),
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
fn with_cleanup_result(
    primary: Result<(), ProcessorError>,
    cleanup: Result<(), ProcessorError>,
    label: &'static str,
) -> Result<(), ProcessorError> {
    match (primary, cleanup) {
        (Err(mut primary), Err(cleanup)) => {
            primary.message = format!("{}; {label}: {}", primary.message, cleanup.message);
            primary.fatal |= cleanup.fatal;
            Err(primary)
        }
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}
fn publish_final_tool_result(
    target: &Mutex<Option<Result<(), ProcessorError>>>,
    result: Result<(), ProcessorError>,
) {
    let mut target = target.lock().unwrap_or_else(|error| error.into_inner());
    *target = Some(match target.take() {
        Some(Err(primary)) => with_cleanup_result(Err(primary), result, "receive completion"),
        _ => result,
    });
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

#[cfg(test)]
mod cleanup_error_tests {
    use super::*;

    #[test]
    fn failed_primary_and_cleanup_are_both_preserved() {
        let primary =
            ProcessorError::new("primary rejected").with_category(ErrorCategory::InvalidRequest);
        let cleanup = ProcessorError::fatal("close timed out");
        let error =
            with_cleanup_result(Err(primary), Err(cleanup), "provider cleanup").unwrap_err();
        assert_eq!(
            error.message,
            "primary rejected; provider cleanup: close timed out"
        );
        assert_eq!(error.category, ErrorCategory::InvalidRequest);
        assert!(error.fatal);
    }
}

#[cfg(test)]
mod connection_state_tests {
    use super::*;
    use pipecat_core::{Pipeline, PipelineConfig};
    #[test]
    fn receive_owner_drop_closes_state_and_late_update_cannot_revive_it() {
        let (state, observed) = tokio::sync::watch::channel(RealtimeConnectionState::Connecting);
        let guard = ConnectionStateGuard(Some(state.clone()));
        state.send_if_modified(|current| {
            *current = RealtimeConnectionState::Ready;
            true
        });
        assert_eq!(*observed.borrow(), RealtimeConnectionState::Ready);
        drop(guard);
        state.send_if_modified(|current| {
            if *current == RealtimeConnectionState::Connecting {
                *current = RealtimeConnectionState::Ready;
                true
            } else {
                false
            }
        });
        assert_eq!(*observed.borrow(), RealtimeConnectionState::Closed);
        assert_eq!(*state.subscribe().borrow(), RealtimeConnectionState::Closed);
    }
    #[tokio::test]
    async fn setup_failure_before_connection_publishes_closed_and_joins_pipeline() {
        let mut config = RealtimeConfig::new("dummy-key");
        config.endpoint = "ws://127.0.0.1:9/unopened".into();
        let processor =
            Arc::new(OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
        let state = processor.subscribe_connection_state();
        let mut worker = Pipeline::new(vec![processor])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        assert_eq!(*state.borrow(), RealtimeConnectionState::Closed); // default pipeline is not 24 kHz
        worker.cancel("setup failure observed").await.unwrap();
        worker.collect().await.unwrap();
    }
}

#[cfg(test)]
mod session_input_tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn opt_in_input_observation_survives_response_generation_but_not_close() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("dummy-loopback-key");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(message) = socket.next().await {
                if matches!(
                    message.unwrap(),
                    tokio_tungstenite::tungstenite::Message::Close(_)
                ) {
                    socket.flush().await.unwrap();
                    break;
                }
            }
        });
        let client = RealtimeClient::new(config).unwrap();
        assert!(
            !OpenAiRealtimeProcessor::new(client.clone())
                .unwrap()
                .preserve_session_input
        );
        assert!(
            OpenAiRealtimeProcessor::new(client.clone())
                .unwrap()
                .with_interruption_resilient_user_input()
                .preserve_session_input
        );
        let mut session = client.connect().await.unwrap();
        let sender = session.sender();
        let captured_generation = sender.generation();
        let inputs = [
            json!({"type":"input_audio_buffer.speech_started","audio_start_ms":0,"item_id":"user"}),
            json!({"type":"input_audio_buffer.speech_stopped","audio_end_ms":1,"item_id":"user"}),
            json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"user","content_index":0,"delta":"partial"}),
            json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"user","content_index":0,"transcript":"final"}),
        ].map(|value| serde_json::from_value::<Event>(value).unwrap());
        sender.interrupt(None);
        assert_ne!(sender.generation(), captured_generation);
        for event in &inputs {
            assert!(is_session_input(event));
            assert!(event.response_id().is_none());
            assert_eq!(
                observe_generation(&sender, captured_generation, false, || 1),
                None
            );
            assert_eq!(
                observe_generation(&sender, captured_generation, is_session_input(event), || 1),
                Some(1)
            );
        }
        for value in [
            json!({"type":"session.updated","session":{"id":"s","type":"realtime"}}),
            json!({"type":"response.created","response":{"id":"r","status":"in_progress","output":[]}}),
            json!({"type":"response.output_text.delta","response_id":"r","item_id":"a","output_index":0,"content_index":0,"delta":"old"}),
            json!({"type":"response.output_audio.delta","response_id":"r","item_id":"a","output_index":0,"content_index":0,"delta":"AAA="}),
            json!({"type":"conversation.item.input_audio_transcription.failed","item_id":"user","content_index":0,"error":{"type":"server_error","message":"failed"}}),
        ] {
            let event: Event = serde_json::from_value(value).unwrap();
            assert!(!is_session_input(&event));
            assert_eq!(
                observe_generation(&sender, captured_generation, is_session_input(&event), || 1),
                None
            );
        }
        sender.close();
        for event in &inputs {
            assert_eq!(
                observe_generation(&sender, sender.generation(), is_session_input(event), || 1),
                None
            );
        }
        timeout(Duration::from_secs(2), session.shutdown())
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
    }
}
