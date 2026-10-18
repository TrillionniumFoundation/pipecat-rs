//! Opt-in, tool-free audio conversation with receipts from the standard pair.
mod actor;
mod finalizer;

use crate::{GeminiClient, GeminiLiveProcessor, GeminiSession};
use pipecat_core::{
    async_trait, ChatRole, Direction, FlushFrame, Frame, FrameData, FrameProcessor,
    LlmContextFrame, ProcessorContext, ProcessorError, StartFrame,
};
use pipecat_turns::{LLMContextAggregatorPair, RealtimeUserHistoryEndpoint};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::watch;

/// Finite limits for one explicit paired audio session. The feature is off by default.
#[derive(Clone, Debug)]
pub struct GeminiHistoryOptions {
    pub user_id: String,
    /// Required, exact legacy text seed when the supplied pair is not empty.
    pub initial_seed: Option<LlmContextFrame>,
    pub max_fragment_bytes: usize,
    pub max_suffix_bytes: usize,
    pub max_queued_final_bytes: usize,
    pub max_held_events: usize,
    pub max_held_bytes: usize,
    pub consumer_timeout: Duration,
    pub drain_timeout: Duration,
}
impl Default for GeminiHistoryOptions {
    fn default() -> Self {
        Self {
            user_id: String::new(),
            initial_seed: None,
            max_fragment_bytes: 64 * 1024,
            max_suffix_bytes: 64 * 1024,
            max_queued_final_bytes: 256 * 1024,
            max_held_events: 128,
            max_held_bytes: 4 * 1024 * 1024,
            consumer_timeout: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(30),
        }
    }
}
fn error(message: impl Into<String>) -> ProcessorError {
    ProcessorError::fatal(message)
}
impl GeminiHistoryOptions {
    fn validate(&self) -> Result<(), ProcessorError> {
        if self.user_id.len() > 4096
            || self.max_fragment_bytes == 0
            || self.max_fragment_bytes > 64 * 1024
            || self.max_suffix_bytes == 0
            || self.max_suffix_bytes > 64 * 1024
            || self.max_queued_final_bytes == 0
            || self.max_queued_final_bytes > 256 * 1024
            || self.max_held_events == 0
            || self.max_held_events > 128
            || self.max_held_bytes == 0
            || self.max_held_bytes > 4 * 1024 * 1024
            || self.consumer_timeout.is_zero()
            || self.consumer_timeout > Duration::from_secs(60)
            || self.drain_timeout.is_zero()
            || self.drain_timeout > Duration::from_secs(120)
        {
            return Err(error("invalid paired-history bounds"));
        }
        Ok(())
    }
}

/// Owns the exact gate/user/service/delivery/assistant composition. Single use.
pub struct GeminiHistoryRoute {
    shared: Arc<SelectedHistory>,
    user: Arc<dyn FrameProcessor>,
    service: Arc<dyn FrameProcessor>,
    assistant: Arc<dyn FrameProcessor>,
}
impl GeminiHistoryRoute {
    pub fn new(
        client: GeminiClient,
        pair: LLMContextAggregatorPair,
        mut options: GeminiHistoryOptions,
    ) -> Result<Self, ProcessorError> {
        options.validate()?;
        client
            .validate_paired_history()
            .map_err(crate::processor::processor_error)?;
        let endpoint = pair.reserve_realtime_user_history()?;
        validate_seed(&client, &endpoint, options.initial_seed.as_ref())?;
        options.user_id = options.user_id.as_str().to_owned();
        if options.initial_seed.is_some() {
            options.initial_seed = Some(endpoint.initial_seed()?);
        }
        let (control, _) = watch::channel(HistoryControl::Open);
        let (completion, _) = watch::channel(None);
        let (marker, _) = watch::channel(false);
        let shared = Arc::new(SelectedHistory {
            endpoint,
            options,
            client: client.clone(),
            control,
            completion,
            marker,
            setup: Mutex::new(None),
            task_id: Mutex::new(None),
            startup: Mutex::new(()),
            started: AtomicBool::new(false),
            running: AtomicBool::new(false),
            input_closed: AtomicBool::new(false),
            draining: AtomicBool::new(false),
        });
        let service = Arc::new(
            GeminiLiveProcessor::new(client)
                .with_user_id(shared.options.user_id.clone())
                .with_selected_history(shared.clone()),
        );
        Ok(Self {
            shared,
            user: pair.user(),
            service,
            assistant: pair.assistant(),
        })
    }
    pub fn into_processors(
        self,
        delivery: Vec<Arc<dyn FrameProcessor>>,
    ) -> Vec<Arc<dyn FrameProcessor>> {
        let mut result: Vec<Arc<dyn FrameProcessor>> =
            vec![Arc::new(HistoryGate(self.shared)), self.user, self.service];
        result.extend(delivery);
        result.push(self.assistant);
        result
    }
}
fn validate_seed(
    client: &GeminiClient,
    endpoint: &RealtimeUserHistoryEndpoint,
    seed: Option<&LlmContextFrame>,
) -> Result<(), ProcessorError> {
    let actual = endpoint.initial_seed()?;
    if !actual.tools.is_empty() || actual.tool_choice.is_some() {
        return Err(error(
            "paired history excludes context tools and tool choice",
        ));
    }
    if actual.messages.len() > 1024 {
        return Err(error("paired history seed exceeds 1024 messages"));
    }
    for message in &actual.messages {
        if !message.tool_calls.is_empty()
            || message.tool_call_id.is_some()
            || message.name.is_some()
        {
            return Err(error("paired history seed requires plain text messages"));
        }
        match message.role {
            ChatRole::User | ChatRole::Assistant => {}
            ChatRole::System | ChatRole::Developer
                if client.config().session.system_instruction.as_deref()
                    == Some(message.content.as_str()) => {}
            _ => {
                return Err(error(
                    "paired history seed role or system instruction mismatch",
                ))
            }
        }
    }
    if serde_json::to_vec(&actual)
        .map_err(|e| error(e.to_string()))?
        .len()
        > client.config().limits.max_message_bytes
    {
        return Err(error("paired history seed exceeds provider message bound"));
    }
    if seed.is_some_and(|seed| seed != &actual) || (seed.is_none() && !actual.messages.is_empty()) {
        return Err(error(
            "paired history requires an explicit exact initial seed",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum HistoryControl {
    Open,
    Drain,
    Cancel,
}
pub(crate) struct SelectedHistory {
    pub(super) endpoint: RealtimeUserHistoryEndpoint,
    pub(super) options: GeminiHistoryOptions,
    pub(super) control: watch::Sender<HistoryControl>,
    client: GeminiClient,
    setup: Mutex<Option<ProcessorContext>>,
    task_id: Mutex<Option<u64>>,
    startup: Mutex<()>,
    started: AtomicBool,
    running: AtomicBool,
    input_closed: AtomicBool,
    draining: AtomicBool,
    marker: watch::Sender<bool>,
    completion: watch::Sender<Option<Result<(), String>>>,
}
impl SelectedHistory {
    pub(crate) fn bind_service(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut setup = self
            .setup
            .lock()
            .map_err(|_| error("history setup lock poisoned"))?;
        if setup.is_some() {
            self.endpoint.close_releases()?;
            return Err(error("paired history route cannot be reused"));
        }
        self.endpoint.bind_service_setup(ctx)?;
        *setup = Some(ctx.clone());
        Ok(())
    }
    pub(crate) fn begin_service_start(
        &self,
        ctx: &ProcessorContext,
    ) -> Result<Option<LlmContextFrame>, ProcessorError> {
        self.endpoint.validate_start(ctx)?;
        validate_seed(
            &self.client,
            &self.endpoint,
            self.options.initial_seed.as_ref(),
        )?;
        if self.started.swap(true, Ordering::SeqCst) {
            return Err(error("paired history Start repeated"));
        }
        Ok(self.options.initial_seed.clone())
    }
    pub(crate) async fn start_receive(
        self: &Arc<Self>,
        session: GeminiSession,
        user_id: String,
        emit_metrics: bool,
    ) -> Result<(), ProcessorError> {
        let output = self
            .setup
            .lock()
            .map_err(|_| error("history setup lock poisoned"))?
            .clone()
            .ok_or_else(|| error("missing history setup route"))?;
        let owner = self.clone();
        let route = output.clone();
        let session_slot = Arc::new(Mutex::new(Some(session)));
        let task_slot = session_slot.clone();
        self.running.store(true, Ordering::SeqCst);
        let spawn = output.spawn("gemini-paired-history-receive", false, async move {
            let session = task_slot
                .lock()
                .map_err(|_| error("history session slot poisoned"))?
                .take()
                .ok_or_else(|| error("history session already owned"))?;
            let result = actor::run(owner.clone(), session, route, user_id, emit_metrics).await;
            owner.completion.send_replace(Some(
                result.as_ref().map(|_| ()).map_err(ToString::to_string),
            ));
            owner.running.store(false, Ordering::SeqCst);
            result
        });
        if let Err(ref spawn_error) = spawn {
            self.running.store(false, Ordering::SeqCst);
            let pending = session_slot
                .lock()
                .map_err(|_| error("history session slot poisoned"))?
                .take();
            if let Some(mut session) = pending {
                match tokio::time::timeout(self.options.drain_timeout, session.cancel_and_join()).await {
                    Ok(result)=>result.map_err(crate::processor::processor_error)?,
                    Err(_)=>return Err(error("history spawn rejection cleanup deadline; inner worker abort requested without joined proof")),
                }
            }
            self.completion
                .send_replace(Some(Err(spawn_error.to_string())));
            return Err(error(spawn_error.to_string()));
        }
        *self
            .task_id
            .lock()
            .map_err(|_| error("history task slot poisoned"))? =
            Some(spawn.expect("successful spawn checked"));
        Ok(())
    }
    pub(crate) fn inspect_service_frame(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<bool, ProcessorError> {
        if self.endpoint.validate_end_marker(frame, direction)? {
            self.input_closed.store(true, Ordering::SeqCst);
            self.marker.send_replace(true);
            return Ok(true);
        }
        if self.endpoint.accept_local_context(frame, direction)? {
            return Ok(true);
        }
        validate_control(frame)?;
        Ok(false)
    }
    pub(crate) fn admit_initial_seed<T>(
        &self,
        action: impl FnOnce() -> Result<T, ProcessorError>,
    ) -> Result<T, ProcessorError> {
        let _gate = self
            .startup
            .lock()
            .map_err(|_| error("history startup gate poisoned"))?;
        if self.service_cancelled() {
            return Err(error(
                "paired history cancelled before initial seed admission",
            ));
        }
        action()
    }
    pub(crate) fn service_cancelled(&self) -> bool {
        *self.control.borrow() == HistoryControl::Cancel
    }
    pub(crate) fn service_input_closed(&self) -> bool {
        self.input_closed.load(Ordering::SeqCst)
    }
    async fn completion_result(&self) -> Result<(), ProcessorError> {
        let mut done = self.completion.subscribe();
        loop {
            if let Some(result) = done.borrow_and_update().clone() {
                return result.map_err(error);
            }
            done.changed()
                .await
                .map_err(|_| error("history completion owner disappeared"))?;
        }
    }
    async fn wait_completion(&self) -> Result<(), ProcessorError> {
        match tokio::time::timeout(self.options.drain_timeout, self.completion_result()).await {
            Ok(result) => result,
            Err(_) => {
                self.cancel()?;
                let setup = self
                    .setup
                    .lock()
                    .map_err(|_| error("history setup lock poisoned"))?
                    .clone();
                let id = *self
                    .task_id
                    .lock()
                    .map_err(|_| error("history task slot poisoned"))?;
                if let (Some(setup), Some(id)) = (setup, id) {
                    setup.cancel_task(id);
                }
                let message="paired history cooperative join deadline exceeded; receive owner forced-aborted, inner join unproven";
                self.completion.send_replace(Some(Err(message.into())));
                Err(error(message))
            }
        }
    }
    pub(crate) async fn prepare_service_end(&self) -> Result<(), ProcessorError> {
        self.wait_completion().await
    }
    pub(crate) async fn abort_service(&self) -> Result<(), ProcessorError> {
        self.cancel()?;
        if self.running.load(Ordering::SeqCst) {
            self.wait_completion().await?;
        }
        Ok(())
    }
    pub(crate) async fn cleanup_service(&self) -> Result<(), ProcessorError> {
        self.cancel()?;
        self.setup
            .lock()
            .map_err(|_| error("history setup lock poisoned"))?
            .take();
        Ok(())
    }
    fn cancel(&self) -> Result<(), ProcessorError> {
        {
            let _gate = self
                .startup
                .lock()
                .map_err(|_| error("history startup gate poisoned"))?;
            self.input_closed.store(true, Ordering::SeqCst);
            self.control.send_replace(HistoryControl::Cancel);
        }
        self.endpoint.close_releases()?;
        Ok(())
    }
    async fn drain(&self) -> Result<(), ProcessorError> {
        if self.draining.swap(true, Ordering::SeqCst) {
            return Err(error("paired history End already started"));
        }
        self.endpoint.begin_draining()?;
        let marker = Frame::new(FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("gemini_history_end_input".into()),
        }))
        .with_interruptible(false);
        self.endpoint
            .register_end_marker(&marker, Direction::Downstream)?;
        if !self.endpoint.admit_end_marker(&marker)? {
            return Err(error("history End input route closed"));
        }
        let operation = async {
            let mut seen = self.marker.subscribe();
            while !*seen.borrow_and_update() {
                seen.changed()
                    .await
                    .map_err(|_| error("history input fence owner disappeared"))?;
            }
            self.control.send_replace(HistoryControl::Drain);
            self.wait_completion().await
        };
        match tokio::time::timeout(self.options.drain_timeout, operation).await {
            Ok(result) => result,
            Err(_) => {
                self.cancel()?;
                self.wait_completion().await?;
                Err(error("paired history drain deadline exceeded"))
            }
        }
    }
}
struct HistoryGate(Arc<SelectedHistory>);
#[async_trait]
impl FrameProcessor for HistoryGate {
    fn name(&self) -> &str {
        "GeminiHistoryGate"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.0.endpoint.bind_gate_setup(ctx)
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let Err(error) = validate_control(&frame) {
            self.0.abort_service().await?;
            return Err(error);
        }
        if direction == Direction::Downstream {
            if self.0.draining.load(Ordering::SeqCst)
                && matches!(
                    frame.data,
                    FrameData::InputAudio(_)
                        | FrameData::UserAudio(_)
                        | FrameData::UserStartedSpeaking
                        | FrameData::UserStoppedSpeaking
                        | FrameData::SpeechControlParams(_)
                        | FrameData::VadUserStartedSpeaking(_)
                        | FrameData::VadUserStoppedSpeaking(_)
                        | FrameData::VadUserStartedSpeakingDetailed(_)
                        | FrameData::VadUserStoppedSpeakingDetailed(_)
                        | FrameData::ProposedUserStartedSpeaking
                        | FrameData::ProposedUserStoppedSpeaking
                        | FrameData::UserSpeaking
                )
            {
                self.0.abort_service().await?;
                return Err(error(
                    "paired history application input arrived after End began",
                ));
            }
            match &frame.data {
                FrameData::End(_) | FrameData::EndDetailed(_) => self.0.drain().await?,
                data if data.is_cancel() || matches!(data, FrameData::Stop) => {
                    self.0.cancel()?;
                    if self.0.running.load(Ordering::SeqCst) {
                        self.0.wait_completion().await?;
                    }
                }
                _ => {}
            }
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cancel()
    }
}
fn validate_control(frame: &Frame) -> Result<(), ProcessorError> {
    crate::processor::reject_selected_live_frame(frame)
}
