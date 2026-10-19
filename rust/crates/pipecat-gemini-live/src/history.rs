//! Opt-in, tool-free audio conversation with receipts from the standard pair.
mod actor;
mod finalizer;

use crate::processor::SelectedSessionOwner;
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
    service: Arc<GeminiLiveProcessor>,
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
            selected_owner: Mutex::new(None),
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
    /// Access the initial-history selected service for lifecycle observation.
    /// Its public sender remains sealed for every raw Action. The default
    /// paired route deliberately exposes no service/sender bypass of its gate.
    pub fn service(&self) -> Option<Arc<GeminiLiveProcessor>> {
        self.shared
            .client
            .initial_history_policy()
            .is_some()
            .then(|| self.service.clone())
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
    if client.initial_history_policy().is_some() {
        // Same whole-snapshot projection/wire planner as ordinary initialization,
        // including extracted-message metadata and synthetic overhead bounds.
        client
            .plan_initial_history(&actual)
            .map_err(crate::processor::processor_error)?;
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
    selected_owner: Mutex<Option<Arc<SelectedSessionOwner>>>,
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
    pub(crate) fn selected_drain_timeout(&self) -> Duration {
        self.options.drain_timeout
    }
    pub(crate) fn bind_initial_owner(&self, owner: Arc<SelectedSessionOwner>) {
        *self
            .selected_owner
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(owner);
    }
    fn initial_owner(&self) -> Option<Arc<SelectedSessionOwner>> {
        self.selected_owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub(crate) fn record_failure(&self, message: impl Into<String>) {
        if let Some(owner) = self.initial_owner() {
            owner.record_failure(message);
        }
    }
    pub(crate) async fn start_receive_owned(
        self: &Arc<Self>,
        selected: Arc<SelectedSessionOwner>,
        user_id: String,
        emit_metrics: bool,
    ) -> Result<(), ProcessorError> {
        let output = self
            .setup
            .lock()
            .map_err(|_| error("history setup lock poisoned"))?
            .clone()
            .ok_or_else(|| error("missing history setup route"))?;
        let history = self.clone();
        let receiver = selected.clone();
        let route = output.clone();
        let (ownership, acknowledged) = tokio::sync::oneshot::channel();
        self.running.store(true, Ordering::SeqCst);
        let spawn = output.spawn("gemini-paired-history-receive", false, async move {
            let mut session = receiver.session.lock().await;
            let result = if ownership.send(()).is_err() {
                Err(error("paired history selected ownership handoff cancelled"))
            } else {
                actor::run_borrowed(history.clone(), &mut session, route, user_id, emit_metrics)
                    .await
            };
            receiver.record_result(&result);
            // A handoff failure also reaches real borrowed shutdown. The actor's
            // own normal/error paths already joined and preserve their outcome.
            let joined = session
                .selected_shutdown()
                .await
                .map_err(crate::processor::processor_error);
            let result = crate::processor::selected::combine(result, joined);
            receiver.complete(&result);
            history.completion.send_replace(Some(
                result.as_ref().map(|_| ()).map_err(|e| e.message.clone()),
            ));
            history.running.store(false, Ordering::SeqCst);
            result
        });
        let start = match spawn {
            Ok(id) => {
                selected.bind_receiver(output, id);
                *self.task_id.lock().unwrap_or_else(|e| e.into_inner()) = Some(id);
                acknowledged.await.map_err(|_| {
                    error("paired history selected receiver ended before ownership handoff")
                })
            }
            Err(error) => {
                self.running.store(false, Ordering::SeqCst);
                Err(error)
            }
        };
        if let Err(error) = &start {
            selected.record_failure(error.message.clone());
            let joined = selected.settle(false).await;
            let result = crate::processor::selected::combine(start, joined);
            self.completion.send_replace(Some(
                result.as_ref().map(|_| ()).map_err(|e| e.message.clone()),
            ));
            return result;
        }
        Ok(())
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
        if let Some(owner) = self.initial_owner() {
            if !self.running.load(Ordering::SeqCst) && owner.joined.joined() {
                return owner.settle(false).await;
            }
            let deadline = owner.terminal_deadline()?;
            let route = match tokio::time::timeout_at(deadline, self.completion_result()).await {
                Ok(result) => result,
                Err(_) => {
                    owner.record_failure("paired history selected drain deadline exceeded");
                    crate::processor::selected::combine(
                        Err(error("paired history selected drain deadline exceeded")),
                        self.cancel(),
                    )
                }
            };
            if let Err(error) = &route {
                owner.record_failure(error.message.clone());
            }
            let joined = owner.settle(false).await;
            return crate::processor::selected::combine(route, joined);
        }
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
        let cancelled = self.cancel();
        if let Some(owner) = self.initial_owner() {
            return crate::processor::selected::combine(cancelled, owner.settle(false).await);
        }
        cancelled?;
        if self.running.load(Ordering::SeqCst) {
            self.wait_completion().await?;
        }
        Ok(())
    }
    pub(crate) async fn cleanup_service(&self) -> Result<(), ProcessorError> {
        let cancelled = self.cancel();
        if let Some(owner) = self.initial_owner() {
            return crate::processor::selected::combine(cancelled, owner.settle(false).await);
        }
        cancelled?;
        self.setup
            .lock()
            .map_err(|_| error("history setup lock poisoned"))?
            .take();
        Ok(())
    }
    fn cancel(&self) -> Result<(), ProcessorError> {
        let result = self.cancel_inner();
        if let Some(owner) = self.initial_owner() {
            owner.record_result(&result);
        }
        result
    }
    fn cancel_inner(&self) -> Result<(), ProcessorError> {
        {
            let _gate = self
                .startup
                .lock()
                .map_err(|_| error("history startup gate poisoned"))?;
            self.input_closed.store(true, Ordering::SeqCst);
            self.control.send_replace(HistoryControl::Cancel);
        }
        if let Some(owner) = self.initial_owner() {
            let closed = owner.close_input();
            owner.sender.cancel();
            closed?;
        }
        self.endpoint.close_releases()?;
        Ok(())
    }
    async fn drain(&self) -> Result<(), ProcessorError> {
        let result = self.drain_inner().await;
        if let (Some(owner), Err(error)) = (self.initial_owner(), &result) {
            owner.record_failure(error.message.clone());
            let cancelled = self.cancel();
            let joined = owner.settle(false).await;
            return crate::processor::selected::combine(
                result,
                crate::processor::selected::combine(cancelled, joined),
            );
        }
        result
    }
    async fn drain_inner(&self) -> Result<(), ProcessorError> {
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
        if let Some(owner) = self.initial_owner() {
            let deadline = owner.terminal_deadline()?;
            return match tokio::time::timeout_at(deadline, operation).await {
                Ok(result) => {
                    if result.is_err() {
                        owner.record_result(&result);
                        let cancelled = self.cancel();
                        let joined = owner.settle(false).await;
                        crate::processor::selected::combine(
                            result,
                            crate::processor::selected::combine(cancelled, joined),
                        )
                    } else {
                        result
                    }
                }
                Err(_) => {
                    owner.record_failure("paired history selected drain deadline exceeded");
                    let cancelled = self.cancel();
                    let joined = owner.settle(false).await;
                    crate::processor::selected::combine(
                        Err(error("paired history selected drain deadline exceeded")),
                        crate::processor::selected::combine(cancelled, joined),
                    )
                }
            };
        }
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
            self.0.record_failure(error.message.clone());
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
                let failure = error("paired history application input arrived after End began");
                self.0.record_failure(failure.message.clone());
                self.0.abort_service().await?;
                return Err(failure);
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

#[cfg(test)]
mod initialization_error_tests {
    use super::*;
    use crate::{GeminiConfig, GeminiError, InitialHistoryPolicy};
    use futures_util::{SinkExt, StreamExt};
    use pipecat_core::{Pipeline, PipelineConfig, PipelineWorker, TextFrame};
    use pipecat_turns::{
        AssistantAggregatorConfig, RealtimeServiceMode, SharedContext, UserAggregatorConfig,
    };
    use serde_json::{json, Value};
    use tokio::{sync::oneshot, task::JoinHandle, time::timeout};
    use tokio_tungstenite::tungstenite::Message;

    const WAIT: Duration = Duration::from_secs(3);

    async fn fixture(
        max_fragment_bytes: usize,
    ) -> (GeminiHistoryRoute, oneshot::Sender<Value>, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = GeminiConfig::new("fixture", "gemini-3.8-live");
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        config.limits.finish_timeout = Duration::from_millis(200);
        config.limits.write_timeout = Duration::from_millis(200);
        let client = GeminiClient::new(config)
            .unwrap()
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: false,
            })
            .unwrap();
        let pair = LLMContextAggregatorPair::with_shared_context_and_realtime_mode(
            SharedContext::default(),
            UserAggregatorConfig {
                empty_user_turn: None,
                ..Default::default()
            },
            AssistantAggregatorConfig::default(),
            RealtimeServiceMode::Enabled,
        )
        .unwrap();
        let route = GeminiHistoryRoute::new(
            client,
            pair,
            GeminiHistoryOptions {
                max_fragment_bytes,
                drain_timeout: Duration::from_millis(400),
                ..Default::default()
            },
        )
        .unwrap();
        let (send, mut packet) = oneshot::channel::<Value>();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let setup = socket.next().await.unwrap().unwrap();
            assert!(
                serde_json::from_str::<Value>(setup.to_text().unwrap()).unwrap()["setup"]
                    .is_object()
            );
            socket
                .send(Message::Text(
                    json!({"setupComplete":{}}).to_string().into(),
                ))
                .await
                .unwrap();
            let mut pending_packet = true;
            loop {
                tokio::select! {
                    value = &mut packet, if pending_packet => {
                        pending_packet = false;
                        if let Ok(value) = value {
                            socket.send(Message::Text(value.to_string().into())).await.unwrap();
                        }
                    }
                    message = socket.next() => match message {
                        Some(Ok(Message::Close(_))) | None => {
                            let _ = socket.flush().await;
                            break;
                        }
                        Some(Err(_)) => break,
                        Some(Ok(Message::Text(text))) => panic!("unexpected selected input: {text}"),
                        _ => {}
                    }
                }
            }
        });
        (route, send, peer)
    }

    struct CaptureContext(Mutex<Option<ProcessorContext>>);
    #[async_trait]
    impl FrameProcessor for CaptureContext {
        fn name(&self) -> &str {
            "CaptureHistoryErrorContext"
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
    async fn retained_owner(
        history: &Arc<SelectedHistory>,
    ) -> (Arc<SelectedSessionOwner>, ProcessorContext, PipelineWorker) {
        let capture = Arc::new(CaptureContext(Mutex::new(None)));
        let worker = Pipeline::new(vec![capture.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let ctx = capture.0.lock().unwrap().clone().unwrap();
        history.bind_service(&ctx).unwrap();
        let session = history
            .client
            .connect_paired_history_owned(1)
            .await
            .unwrap();
        let owner = SelectedSessionOwner::new(1, session, Duration::from_millis(400), Some(&ctx));
        history.bind_initial_owner(owner.clone());
        (owner, ctx, worker)
    }
    async fn assert_retained_primary(owner: &SelectedSessionOwner, expected: &str) {
        let failure = timeout(WAIT, owner.settle(false))
            .await
            .unwrap()
            .unwrap_err();
        assert!(failure.message.contains(expected), "{}", failure.message);
        assert!(owner.joined.joined());
        // The actual socket actor was cancelled cleanly. Its own outcome cannot
        // supply the local primary that the aborted receiver/process had known.
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
            .contains(expected));
    }
    #[tokio::test]
    async fn aborted_paired_handoff_cleanup_retains_primary_before_actual_join() {
        let (route, _packet, peer) = fixture(64 * 1024).await;
        let history = route.shared.clone();
        let (owner, _ctx, mut worker) = retained_owner(&history).await;
        let held = owner.session.lock().await;
        let (entered, release_join) = owner.sender.test_pause_before_selected_join();
        {
            let startup = history.start_receive_owned(owner.clone(), String::new(), false);
            tokio::pin!(startup);
            assert!(matches!(
                futures_util::poll!(&mut startup),
                std::task::Poll::Pending
            ));
        }
        drop(held);
        timeout(WAIT, entered).await.unwrap().unwrap();
        assert!(!owner.joined.joined());
        assert_retained_primary(&owner, "ownership handoff cancelled").await;
        drop(release_join);
        let _ = worker.cancel("paired handoff test completed").await;
        timeout(WAIT, worker.join()).await.unwrap().unwrap();
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn aborted_paired_actor_cleanup_retains_local_history_bound_failure() {
        let (route, packet, peer) = fixture(3).await;
        let history = route.shared.clone();
        let mut worker = Pipeline::new(route.into_processors(vec![]))
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let owner = history.initial_owner().unwrap();
        let (entered, release_join) = owner.sender.test_pause_before_selected_join();
        packet.send(json!({"serverContent":{"inputTranscription":{"text":"valid protocol but too long for local history"}}})).unwrap();
        timeout(WAIT, entered).await.unwrap().unwrap();
        assert!(!owner.joined.joined());
        let expected = GeminiError::TooLarge.to_string();
        assert_retained_primary(&owner, &expected).await;
        drop(release_join);
        let _ = worker.cancel("paired local error observed").await;
        assert!(timeout(WAIT, worker.join())
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains(&expected));
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
    #[tokio::test]
    async fn cancelled_history_gate_cleanup_retains_rejected_input_primary() {
        let (route, _packet, peer) = fixture(64 * 1024).await;
        let history = route.shared.clone();
        let (owner, ctx, mut worker) = retained_owner(&history).await;
        let held = owner.session.lock().await;
        let gate = HistoryGate(history);
        {
            let rejected = gate.process(
                Frame::new(FrameData::InputText(TextFrame::new("forbidden live text"))),
                Direction::Downstream,
                &ctx,
            );
            tokio::pin!(rejected);
            assert!(matches!(
                futures_util::poll!(&mut rejected),
                std::task::Poll::Pending
            ));
            assert!(!owner.accepts_input());
            assert!(!owner.joined.joined());
        }
        drop(held);
        assert_retained_primary(&owner, "paired history rejects live text/history edits").await;
        let _ = worker.cancel("paired gate error observed").await;
        timeout(WAIT, worker.join()).await.unwrap().unwrap();
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
}
