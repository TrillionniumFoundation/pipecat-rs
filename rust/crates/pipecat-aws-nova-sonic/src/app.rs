use crate::{
    config::{encode, InputPlan},
    protocol::{Action, Mapper},
    transport::{Transport, TransportConfig, TransportLimits, TransportSender},
    NovaSonicConfig,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, ErrorFrame, FlushFrame, Frame, FrameData, FrameProcessor, Pipeline,
    PipelineConfig, PipelineError, PipelineWorker, ProcessorContext, ProcessorError, StartFrame,
    TranscriptionFrame,
};
use pipecat_services::{Content, Context};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, CommittedUserPrefix, LLMContextAggregatorPair,
    RealtimeServiceMode, RealtimeUserHistoryEndpoint, SharedContext, UserAggregatorConfig,
};
use serde_json::json;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime},
};
use tokio::{sync::watch, time::Instant};
type PendingFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
fn error(message: impl Into<String>) -> ProcessorError {
    ProcessorError::fatal(message)
}
fn combine(
    a: Result<(), ProcessorError>,
    b: Result<(), ProcessorError>,
) -> Result<(), ProcessorError> {
    match (a, b) {
        (Err(mut a), Err(b)) => {
            a.message.push_str(&format!("; cleanup: {}", b.message));
            Err(a)
        }
        (Err(a), _) => Err(a),
        (_, b) => b,
    }
}
// Keep both the originating failure and cleanup truth; TransportReport::result
// intentionally selects one error for lower-level callers and is not enough here.
fn transport_outcome(report: &crate::transport::TransportReport) -> Result<(), ProcessorError> {
    if report.primary.is_none()
        && report.cleanup.is_none()
        && !report.forced_abort
        && report.writer_joined
        && report.reader_joined
        && report.connection_joined
    {
        return Ok(());
    }
    Err(error(format!("Nova transport: primary={}; cleanup={}; forced_abort={}; writer_joined={}; reader_joined={}; connection_joined={}",
        report.primary.as_ref().map_or_else(||"none".into(),ToString::to_string),
        report.cleanup.as_ref().map_or_else(||"none".into(),ToString::to_string),
        report.forced_abort,report.writer_joined,report.reader_joined,report.connection_joined)))
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Control {
    Open,
    Drain,
    Cancel,
}
struct State {
    endpoint: RealtimeUserHistoryEndpoint,
    config: NovaSonicConfig,
    plan: Mutex<Option<InputPlan>>,
    setup: Mutex<Option<ProcessorContext>>,
    sender: Mutex<Option<TransportSender>>,
    transport_completion: Mutex<Option<crate::transport::TransportCompletionHandle>>,
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Result<(), ProcessorError>>>>,
    control: watch::Sender<Control>,
    ready: watch::Sender<Option<Result<(), String>>>,
    marker: watch::Sender<bool>,
    result: Mutex<Option<Result<(), ProcessorError>>>,
    started: AtomicBool,
    draining: AtomicBool,
    input_closed: AtomicBool,
    receiver_joined: AtomicBool,
    transport_joined: AtomicBool,
    #[cfg(test)]
    panic_receiver: AtomicBool,
    #[cfg(test)]
    recovery_pause: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    responses: AtomicU64,
    prompt: String,
    audio: String,
}
impl State {
    fn cancel(&self) {
        self.input_closed.store(true, Ordering::SeqCst);
        self.control.send_replace(Control::Cancel);
        if let Some(sender) = self
            .sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            sender.cancel();
        }
        let _ = self.endpoint.close_releases();
    }
    async fn ready(&self) -> Result<(), ProcessorError> {
        let mut r = self.ready.subscribe();
        loop {
            if let Some(v) = r.borrow_and_update().clone() {
                return v.map_err(error);
            }
            r.changed()
                .await
                .map_err(|_| error("Nova startup owner closed"))?;
        }
    }
    async fn joined(&self) -> Result<(), ProcessorError> {
        // This retained mutex serializes receiver and transport observations. A
        // cancelled observer never removes a pending JoinHandle or cached result.
        let mut slot = self.task.lock().await;
        if let Some(task) = slot.as_mut() {
            let result = task.await.unwrap_or_else(|_| {
                Err(error("Nova receiver task failed; primary outcome unproven"))
            });
            *self.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
            slot.take();
            self.receiver_joined.store(true, Ordering::SeqCst);
        }
        // Recovery must be resumable even when the receiver slot is already empty.
        // Keep the completion in retained state until both report and primary/cleanup
        // result are cached synchronously, with no intervening cancellation point.
        if !self.transport_joined.load(Ordering::SeqCst) {
            let completion = self
                .transport_completion
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            if let Some(completion) = completion {
                self.cancel();
                #[cfg(test)]
                {
                    let pause = self.recovery_pause.lock().unwrap().clone();
                    if let Some(pause) = pause {
                        pause.acquire().await.expect("test recovery gate").forget();
                    }
                }
                let report = completion.wait().await;
                self.transport_joined.store(
                    report.writer_joined && report.reader_joined && report.connection_joined,
                    Ordering::SeqCst,
                );
                let mut result = self.result.lock().unwrap_or_else(|e| e.into_inner());
                *result = Some(combine(
                    result.clone().unwrap_or(Ok(())),
                    transport_outcome(&report),
                ));
                self.transport_completion
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
            }
        }
        self.result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or(Ok(()))
    }
    async fn drain(&self) -> Result<(), ProcessorError> {
        if self.draining.swap(true, Ordering::SeqCst) {
            return self.joined().await;
        }
        let outcome = async {
            self.endpoint.begin_draining()?;
            let marker = Frame::new(FrameData::Flush(FlushFrame {
                returning: false,
                origin: Some("gemini_history_end_input".into()),
            }))
            .with_interruptible(false);
            self.endpoint
                .register_end_marker(&marker, Direction::Downstream)?;
            if !self.endpoint.admit_end_marker(&marker)? {
                return Err(error("Nova End fence admission closed"));
            }
            let mut seen = self.marker.subscribe();
            while !*seen.borrow_and_update() {
                seen.changed()
                    .await
                    .map_err(|_| error("Nova End fence owner closed"))?;
            }
            self.control.send_replace(Control::Drain);
            self.joined().await
        };
        match tokio::time::timeout(self.config.limits.drain_timeout, outcome).await {
            Ok(v) => v,
            Err(_) => {
                self.cancel();
                combine(
                    Err(error("Nova End drain deadline exceeded")),
                    self.joined().await,
                )
            }
        }
    }
}
/// Read-only generated conversation history from the exact application pair.
#[derive(Clone)]
pub struct NovaSonicHistory(SharedContext);
impl NovaSonicHistory {
    pub fn snapshot(&self) -> Result<Context, ProcessorError> {
        self.0.snapshot()
    }
    pub fn snapshot_rich(&self) -> Result<pipecat_core::LlmContextData, ProcessorError> {
        self.0.snapshot_rich()
    }
}
/// Observation and joined shutdown only. No sender, mutable history or tools escape.
#[derive(Clone)]
pub struct NovaSonicHandle(Arc<State>);
impl NovaSonicHandle {
    pub fn responses_completed(&self) -> u64 {
        self.0.responses.load(Ordering::SeqCst)
    }
    pub fn session_joined(&self) -> bool {
        self.0.receiver_joined.load(Ordering::SeqCst)
            && self.0.transport_joined.load(Ordering::SeqCst)
    }
    pub async fn wait_joined(&self) -> Result<(), ProcessorError> {
        self.0.joined().await
    }
}
/// One finite standard-pair PCM application. The output processor is application-owned.
pub struct NovaSonicApp {
    pipeline: Pipeline,
    config: PipelineConfig,
    history: NovaSonicHistory,
    state: Arc<State>,
    cleanup_errors: Arc<Mutex<Vec<String>>>,
}
/// Dropping the app cancels its session even if an input handle remains alive.
pub struct NovaSonicRunningApp {
    pub worker: NovaSonicWorker,
    pub history: NovaSonicHistory,
    pub provider: NovaSonicHandle,
}
impl Drop for NovaSonicRunningApp {
    fn drop(&mut self) {
        self.provider.0.cancel();
    }
}
impl NovaSonicApp {
    pub fn new(
        config: NovaSonicConfig,
        context: Context,
        output: Arc<dyn FrameProcessor>,
    ) -> Result<Self, ProcessorError> {
        Self::build(config, context, output, |user| user)
    }
    #[cfg(test)]
    pub(crate) fn new_with_user_wrapper(
        config: NovaSonicConfig,
        context: Context,
        output: Arc<dyn FrameProcessor>,
        wrap: impl FnOnce(Arc<dyn FrameProcessor>) -> Arc<dyn FrameProcessor>,
    ) -> Result<Self, ProcessorError> {
        Self::build(config, context, output, wrap)
    }
    fn build(
        config: NovaSonicConfig,
        context: Context,
        output: Arc<dyn FrameProcessor>,
        wrap: impl FnOnce(Arc<dyn FrameProcessor>) -> Arc<dyn FrameProcessor>,
    ) -> Result<Self, ProcessorError> {
        config.validate()?;
        if context.tools.as_ref().is_some_and(|t| !t.is_empty())
            || context.tool_choice.is_some()
            || context.messages.len() > config.limits.max_history_messages
        {
            return Err(error(
                "Nova app is strictly tool-free with bounded initial history",
            ));
        }
        for m in &context.messages {
            if !matches!(m.content, Some(Content::Text(_)) | None)
                || m.name.is_some()
                || m.tool_call_id.is_some()
                || !m.tool_calls.is_empty()
            {
                return Err(error("Nova initial history requires ordinary plain text"));
            }
        }
        serde_json::to_writer(
            ByteBudget(config.limits.max_history_bytes * 2 + 16 * 1024),
            &context,
        )
        .map_err(|_| error("Nova seed encoded budget exceeded"))?;
        let canonical = serde_json::to_vec(&context)
            .map_err(|_| error("Nova seed serialization failed"))?
            .into_boxed_slice();
        drop(context);
        let context: Context = serde_json::from_slice(&canonical)
            .map_err(|_| error("Nova seed reconstruction failed"))?;
        drop(canonical);
        let pair = LLMContextAggregatorPair::with_realtime_mode(
            context,
            UserAggregatorConfig {
                empty_user_turn: None,
                user_idle_timeout: Duration::ZERO,
                ..Default::default()
            },
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Text,
            },
            RealtimeServiceMode::Enabled,
        )?;
        let endpoint = pair.reserve_realtime_user_history()?;
        let plan = InputPlan::new(&config, &endpoint.initial_seed()?)?;
        let (control, _) = watch::channel(Control::Open);
        let (ready, _) = watch::channel(None);
        let (marker, _) = watch::channel(false);
        let state = Arc::new(State {
            endpoint,
            config: config.clone(),
            plan: Mutex::new(Some(InputPlan {
                prompt: plan.prompt.clone(),
                audio: plan.audio.clone(),
                startup: plan.startup,
                finish: plan.finish,
            })),
            setup: Mutex::new(None),
            sender: Mutex::new(None),
            transport_completion: Mutex::new(None),
            task: tokio::sync::Mutex::new(None),
            control,
            ready,
            marker,
            result: Mutex::new(None),
            started: AtomicBool::new(false),
            draining: AtomicBool::new(false),
            input_closed: AtomicBool::new(false),
            receiver_joined: AtomicBool::new(false),
            transport_joined: AtomicBool::new(false),
            #[cfg(test)]
            panic_receiver: AtomicBool::new(false),
            #[cfg(test)]
            recovery_pause: Mutex::new(None),
            responses: AtomicU64::new(0),
            prompt: plan.prompt.clone(),
            audio: plan.audio.clone(),
        });
        let cleanup_errors = Arc::new(Mutex::new(Vec::new()));
        let processors: Vec<Arc<dyn FrameProcessor>> = vec![
            Arc::new(Gate(state.clone())),
            wrap(pair.user()),
            Arc::new(Provider(state.clone())),
            output,
            pair.assistant(),
            Arc::new(Tail),
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
        let config = PipelineConfig {
            queue_capacity: 128,
            system_queue_capacity: 64,
            output_capacity: 256,
            startup_timeout: config.limits.connect_timeout + config.limits.operation_timeout,
            shutdown_timeout: config.limits.drain_timeout + config.limits.close_timeout * 3,
            ..Default::default()
        };
        Ok(Self {
            pipeline,
            config,
            history: NovaSonicHistory(pair.context()),
            state,
            cleanup_errors,
        })
    }
    pub async fn start(self) -> Result<NovaSonicRunningApp, PipelineError> {
        struct Guard(Arc<State>, bool);
        impl Drop for Guard {
            fn drop(&mut self) {
                if self.1 {
                    self.0.cancel();
                }
            }
        }
        let mut guard = Guard(self.state.clone(), true);
        let mut worker = match self.pipeline.start(self.config).await {
            Ok(v) => v,
            Err(e) => {
                self.state.cancel();
                let joined = self.state.joined().await;
                return Err(PipelineError::Execution(format!(
                    "{e}; Nova cleanup: {}; processor cleanup: {}",
                    joined.map_or_else(|e| e.to_string(), |_| "joined".into()),
                    self.cleanup_errors
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .join("; ")
                )));
            }
        };
        if let Err(primary) = self.state.ready().await {
            self.state.cancel();
            let _ = worker.cancel("Nova startup failed").await;
            while worker.recv().await.is_some() {}
            let joined = worker.join().await;
            let owned = self.state.joined().await;
            return Err(PipelineError::Execution(format!(
                "{}; pipeline cleanup: {}; session cleanup: {}",
                primary.message,
                joined.map_or_else(|e| e.to_string(), |_| "joined".into()),
                owned.map_or_else(|e| e.to_string(), |_| "joined".into())
            )));
        }
        guard.1 = false;
        Ok(NovaSonicRunningApp {
            worker: NovaSonicWorker {
                inner: worker,
                config: Arc::new(self.state.config.clone()),
            },
            history: self.history,
            provider: NovaSonicHandle(self.state),
        })
    }
}
fn reject_native(frame: &Frame) -> Result<(), ProcessorError> {
    if frame.native_tool_batch()?.is_some()
        || frame.native_tool_outcome()?.is_some()
        || frame.native_tool_progress_admission()?.is_some()
        || frame.native_tool_progress()?.is_some()
        || frame
            .native_llm_continuation()
            .map_err(|_| error("invalid Nova continuation carrier"))?
            .is_some()
        || frame.has_native_tool_progress_restriction()
        || frame.native_speculation().is_some()
        || frame.native_eager_source().is_some()
    {
        return Err(error("Nova app rejects native tools and continuation"));
    }
    frame.native_realtime_context_cause()?;
    if frame
        .native_user_image_constraint(Direction::Downstream)?
        .is_some()
        || frame
            .native_user_image_context(Direction::Downstream)?
            .is_some()
    {
        return Err(error("Nova app rejects native image carriers"));
    }
    Ok(())
}
fn public_ingress(frame: &Frame) -> Result<(), ProcessorError> {
    reject_native(frame)?;
    if frame.native_realtime_context_cause()?.is_some() {
        return Err(error("Nova caller cannot inject paired context authority"));
    }
    match &frame.data {
        FrameData::Start(_)
        | FrameData::StartDetailed(_)
        | FrameData::InputAudio(_)
        | FrameData::UserAudio(_)
        | FrameData::End(_)
        | FrameData::EndDetailed(_)
        | FrameData::Cancel(_)
        | FrameData::CancelDetailed(_)
        | FrameData::Stop => Ok(()),
        FrameData::Flush(f)
            if f.origin.as_deref() != Some("gemini_history_end_input")
                && f.origin.as_deref() != Some("assistant_turn_end") =>
        {
            Ok(())
        }
        _ => Err(error(
            "Nova PCM app rejects live text, tools, settings and provider-owned input",
        )),
    }
}
struct Gate(Arc<State>);
#[async_trait]
impl FrameProcessor for Gate {
    fn name(&self) -> &str {
        "NovaSonicHistoryGate"
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
        if direction == Direction::Downstream {
            if let Err(e) = validate_input(&frame, direction, &self.0.config) {
                self.0.cancel();
                return combine(Err(e), self.0.joined().await);
            }
            if self.0.draining.load(Ordering::SeqCst)
                && matches!(
                    frame.data,
                    FrameData::InputAudio(_) | FrameData::UserAudio(_)
                )
            {
                self.0.cancel();
                return Err(error("Nova input arrived after End began"));
            }
            match &frame.data {
                FrameData::End(_) | FrameData::EndDetailed(_) => self.0.drain().await?,
                d if d.is_cancel() || matches!(d, FrameData::Stop) => {
                    self.0.cancel();
                    self.0.joined().await?;
                }
                _ => {}
            }
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cancel();
        self.0.joined().await
    }
}
struct Tail;
#[async_trait]
impl FrameProcessor for Tail {
    fn name(&self) -> &str {
        "NovaSonicTailIntegrity"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        reject_native(&frame)?;
        if direction == Direction::Upstream
            && !matches!(
                frame.data,
                FrameData::Flush(_) | FrameData::FlushDetailed(_)
            )
        {
            return Err(error("Nova caller input must be downstream"));
        }
        if direction == Direction::Upstream {
            public_ingress(&frame)?;
        }
        ctx.push(frame, direction).await
    }
}
struct Provider(Arc<State>);
#[async_trait]
impl FrameProcessor for Provider {
    fn name(&self) -> &str {
        "NovaSonic"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if start.audio_in_sample_rate != 16000 || start.audio_out_sample_rate != 24000 {
            return Err(error("Nova pipeline sample rates must be 16000/24000"));
        }
        self.0.endpoint.bind_service_setup(ctx)?;
        *self
            .0
            .setup
            .lock()
            .map_err(|_| error("Nova setup lock poisoned"))? = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        reject_native(&frame)?;
        if self.0.endpoint.validate_end_marker(&frame, direction)? {
            self.0.input_closed.store(true, Ordering::SeqCst);
            self.0.marker.send_replace(true);
            return Ok(());
        }
        if self.0.endpoint.accept_local_context(&frame, direction)? {
            return Ok(());
        }
        if matches!(
            frame.data,
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
        ) {
            return Err(error("Nova rejects unauthenticated context snapshots"));
        }
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        match &frame.data {
            data if data.is_start() => {
                if self.0.started.swap(true, Ordering::SeqCst) {
                    return Err(error("Nova session cannot restart"));
                }
                let setup = self
                    .0
                    .setup
                    .lock()
                    .map_err(|_| error("Nova setup lock poisoned"))?
                    .clone()
                    .ok_or_else(|| error("Nova missing setup"))?;
                self.0.endpoint.validate_start(&setup)?;
                let plan = self
                    .0
                    .plan
                    .lock()
                    .map_err(|_| error("Nova input plan lock poisoned"))?
                    .take()
                    .ok_or_else(|| error("Nova startup plan already used"))?;
                let owner = self.0.clone();
                let report_ctx = setup.clone();
                let mut slot = self.0.task.lock().await;
                *slot = Some(tokio::spawn(async move {
                    let result = std::panic::AssertUnwindSafe(run(owner.clone(), setup, plan))
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| {
                            Err(error("Nova receiver task failed; primary outcome unproven"))
                        });
                    if owner.ready.borrow().is_none() {
                        owner
                            .ready
                            .send_replace(Some(result.clone().map_err(|e| e.message)));
                    }
                    if let Err(e) = &result {
                        let mut f = ErrorFrame::new(e.message.clone());
                        f.fatal = true;
                        let _ = report_ctx.push_error(f).await;
                    }
                    result
                }));
                drop(slot);
                self.0.ready().await?;
                ctx.push(frame, direction).await
            }
            FrameData::InputAudio(_) | FrameData::UserAudio(_) => {
                let audio = audio(&frame.data).ok_or_else(|| error("Nova missing PCM payload"))?;
                if self.0.input_closed.load(Ordering::SeqCst) {
                    return Err(error("Nova session input is closed"));
                }
                self.0.config.audio(audio)?;
                let sender = self
                    .0
                    .sender
                    .lock()
                    .map_err(|_| error("Nova sender lock poisoned"))?
                    .clone()
                    .ok_or_else(|| error("Nova sender unavailable"))?;
                let message = encode(
                    json!({"event":{"audioInput":{"promptName":self.0.prompt,"contentName":self.0.audio,"content":STANDARD.encode(&audio.audio)}}}),
                    &self.0.config.protocol_limits(),
                )?;
                sender
                    .send(message)
                    .await
                    .map_err(|_| error("Nova PCM send failed"))
            }
            FrameData::End(_) | FrameData::EndDetailed(_) => {
                self.0.joined().await?;
                ctx.push(frame, direction).await
            }
            data if data.is_cancel() || matches!(data, FrameData::Stop) => {
                self.0.cancel();
                self.0.joined().await?;
                ctx.push(frame, direction).await
            }
            FrameData::UserStartedSpeaking
            | FrameData::UserStoppedSpeaking
            | FrameData::UserSpeaking
            | FrameData::Interruption
            | FrameData::BotStartedSpeaking
            | FrameData::BotStoppedSpeaking => Ok(()),
            FrameData::Flush(_) | FrameData::FlushDetailed(_) => ctx.push(frame, direction).await,
            _ => Err(error("unsupported Nova service control")),
        }
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.0.joined().await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cancel();
        let result = self.0.joined().await;
        self.0
            .sender
            .lock()
            .map_err(|_| error("Nova sender lock poisoned"))?
            .take();
        self.0
            .setup
            .lock()
            .map_err(|_| error("Nova setup lock poisoned"))?
            .take();
        result
    }
}
struct Held {
    frame: Frame,
    through: u64,
    bytes: usize,
}
struct Prefix {
    text: String,
    registered: u64,
    committed: u64,
    pending: Option<PendingFuture<Result<CommittedUserPrefix, ProcessorError>>>,
    deadline: Option<Instant>,
    target: u64,
    held: VecDeque<Held>,
    held_bytes: usize,
}
impl Prefix {
    fn new() -> Self {
        Self {
            text: String::new(),
            registered: 0,
            committed: 0,
            pending: None,
            deadline: None,
            target: 0,
            held: VecDeque::new(),
            held_bytes: 0,
        }
    }
    fn final_text(&mut self, text: String) -> Result<(), ProcessorError> {
        if text.trim().is_empty() {
            return Ok(());
        }
        let n = self.text.len() + text.len() + usize::from(!self.text.is_empty());
        if n > 64 * 1024 {
            return Err(error("Nova pending user transcript exceeds bound"));
        }
        if !self.text.is_empty() {
            self.text.push(' ');
        }
        self.text.push_str(text.trim());
        Ok(())
    }
    fn boundary(&mut self, state: &State, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if !self.text.trim().is_empty() {
            let frame = Frame::new(FrameData::Transcription(TranscriptionFrame {
                text: std::mem::take(&mut self.text),
                user_id: String::new(),
                timestamp: String::new(),
                language: None,
                result: None,
                finalized: true,
            }))
            .with_interruptible(false);
            self.registered = state.endpoint.register_final(&frame, Direction::Upstream)?;
            if !state.endpoint.admit(ctx, frame, Direction::Upstream)? {
                return Err(error("Nova user prefix admission closed"));
            }
        }
        self.request(state)
    }
    fn request(&mut self, state: &State) -> Result<(), ProcessorError> {
        if self.pending.is_none() && self.registered > self.committed {
            self.target = self.registered;
            self.pending = Some(Box::pin(state.endpoint.commit_through(self.target)?.wait()));
            self.deadline = Some(Instant::now() + state.config.limits.consumer_timeout);
        }
        Ok(())
    }
    fn receipt(&mut self, state: &State, r: CommittedUserPrefix) -> Result<(), ProcessorError> {
        state.endpoint.validate_receipt(&r)?;
        if r.through() != self.target || r.through() <= self.committed || !r.effects_admitted() {
            return Err(error("Nova received an invalid user prefix receipt"));
        }
        self.committed = r.through();
        self.pending = None;
        self.deadline = None;
        self.request(state)
    }
    fn output(&mut self, state: &State, frame: Frame) -> Result<(), ProcessorError> {
        let bytes = match &frame.data {
            FrameData::TtsAudio(a) => a.audio.capacity(),
            FrameData::LlmText(t) => t.text.capacity(),
            FrameData::TtsText(t) => t.text.text.capacity(),
            _ => 0,
        } + std::mem::size_of::<Held>() * 2
            + 512; // Owned frame, VecDeque allocation slack and small generated fields.
        if self.held.len() >= state.config.limits.max_held_events
            || self
                .held_bytes
                .checked_add(bytes)
                .is_none_or(|n| n > state.config.limits.max_held_bytes)
        {
            return Err(error("Nova dependent output exceeds retained budget"));
        }
        self.held_bytes += bytes;
        self.held.push_back(Held {
            frame,
            through: self.registered,
            bytes,
        });
        Ok(())
    }
    fn release(&mut self, state: &State, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        while self
            .held
            .front()
            .is_some_and(|h| h.through <= self.committed)
        {
            let h = self.held.pop_front().expect("front checked");
            self.held_bytes -= h.bytes;
            if !state.endpoint.admit(ctx, h.frame, Direction::Downstream)? {
                return Err(error("Nova output admission closed"));
            }
        }
        Ok(())
    }
    fn interrupt(&mut self) {
        self.held.clear();
        self.held_bytes = 0;
    }
    async fn wait(&mut self) -> Result<CommittedUserPrefix, ProcessorError> {
        match &mut self.pending {
            Some(f) => f.as_mut().await,
            None => std::future::pending().await,
        }
    }
}
async fn run(
    state: Arc<State>,
    ctx: ProcessorContext,
    plan: InputPlan,
) -> Result<(), ProcessorError> {
    if *state.control.borrow() == Control::Cancel {
        return Err(error("Nova startup cancelled before transport admission"));
    }
    let admitted = Instant::now();
    let limits = &state.config.limits;
    let mut mapper = Mapper::new(plan.prompt.clone(), state.config.protocol_limits())
        .map_err(|e| error(e.to_string()))?;
    let endpoint = state.config.url()?;
    let (headers, signer) = pipecat_aws_common::auth::signed_event_stream_post_headers(
        &endpoint,
        &state.config.region,
        &state.config.credentials,
        SystemTime::now(),
        "bedrock",
        limits.max_event_bytes + 128,
    )
    .map_err(|_| error("Nova signing configuration failed"))?;
    let transport_limits = TransportLimits {
        session_timeout: limits.session_timeout,
        connect_timeout: limits.connect_timeout,
        write_timeout: limits.operation_timeout,
        idle_timeout: limits.response_timeout,
        consumer_timeout: limits.consumer_timeout,
        close_timeout: limits.close_timeout,
        input_queue_events: limits.queue_events,
        input_queue_bytes: limits.queue_bytes,
        max_input_frame_bytes: limits.max_event_bytes,
        response_queue_events: limits.queue_events,
        response_queue_bytes: limits.queue_bytes,
        max_response_frame_bytes: limits.max_event_bytes,
        ..Default::default()
    };
    let mut transport = Transport::start(
        TransportConfig {
            endpoint,
            headers,
            connect_address: state.config.connect_address,
            limits: transport_limits,
        },
        plan.startup,
        signer,
    )
    .map_err(|_| error("Nova HTTP2 transport admission failed"))?;
    let sender = transport.sender();
    *state
        .sender
        .lock()
        .map_err(|_| error("Nova sender lock poisoned"))? = Some(sender.clone());
    let completion = transport.completion();
    *state
        .transport_completion
        .lock()
        .map_err(|_| error("Nova completion lock poisoned"))? = Some(completion.clone());
    let mut response = transport
        .take_receiver()
        .ok_or_else(|| error("Nova response receiver already consumed"))?;
    let mut control = state.control.subscribe();
    let startup = if *control.borrow() == Control::Cancel {
        Err(error("Nova startup cancelled"))
    } else {
        tokio::select! {biased;_ = control.changed()=>Err(error("Nova startup cancelled")),r=transport.ready()=>r.map_err(|e|error(format!("Nova HTTP2 startup: {e}")))}
    };
    state
        .ready
        .send_replace(Some(startup.clone().map_err(|e| e.message)));
    let mut transport_eof = false;
    let outcome = match startup {
        Err(e) => Err(e),
        Ok(()) => {
            let mut prefix = Prefix::new();
            let mut ending: Option<PendingFuture<Result<(), crate::transport::TransportError>>> =
                None;
            let mut finish = Some(plan.finish);
            let mut eof = false;
            let mut end_admitted = false;
            let early = admitted + limits.session_timeout - limits.drain_timeout;
            let loop_result:Result<(),ProcessorError>=async{loop{
                let requested=*control.borrow_and_update();
                if requested==Control::Cancel{break Ok(());}
                if requested==Control::Drain&&ending.is_none()&&!end_admitted{
                    let sender=sender.clone();let events=finish.take().ok_or_else(||error("Nova End repeated"))?;ending=Some(Box::pin(async move{sender.end(events).await}));
                }
                if requested==Control::Open&&Instant::now()>=early{break Err(error("Nova finite session input deadline reached; automatic continuation unsupported"));}
                prefix.release(&state,&ctx)?;
                if eof&&prefix.pending.is_none()&&prefix.held.is_empty(){if requested!=Control::Drain||!end_admitted{break Err(error("Nova response EOF before graceful End"));}break Ok(());}
                let deadline=prefix.deadline.unwrap_or(early);
                tokio::select!{biased;
                    r=control.changed()=>{r.map_err(|_|error("Nova control owner closed"))?;},
                    _=tokio::time::sleep_until(deadline),if prefix.deadline.is_some()||requested==Control::Open=>{if prefix.deadline.is_some(){break Err(error("Nova user receipt deadline exceeded"));}},
                    r=prefix.wait(),if prefix.pending.is_some()=>{prefix.receipt(&state,r?)?;},
                    r=async{match &mut ending{Some(f)=>f.as_mut().await,None=>std::future::pending().await}},if ending.is_some()=>{r.map_err(|e|error(format!("Nova upload End: {e}")))?;ending=None;end_admitted=true;},
                    message=response.recv(),if !eof=>match message{
                        None=>{transport_eof=true;mapper.finish().map_err(|e|error(e.to_string()))?;prefix.boundary(&state,&ctx)?;eof=true;},
                        Some(bytes)=>{
                            #[cfg(test)]
                            assert!(!state.panic_receiver.load(Ordering::SeqCst), "injected Nova receiver panic");
                            let nova=crate::protocol::decode_response(&bytes,&state.config.protocol_limits()).map_err(|e|error(e.to_string()))?;
                            let actions=mapper.map_nova_json(&nova).map_err(|e|error(e.to_string()))?;
                            if actions.len()>state.config.limits.max_held_events || actions.iter().map(Action::retained_bytes).sum::<usize>()>state.config.limits.max_held_bytes {return Err(error("Nova mapped event batch exceeds retained budget"));}
                            for action in actions{match action{
                                Action::UserFinal(text)=>prefix.final_text(text)?,Action::Boundary=>prefix.boundary(&state,&ctx)?,
                                Action::Output(frame)=>prefix.output(&state,*frame)?,Action::Interrupt=>{prefix.interrupt();if !interrupt(&state,&ctx).await? {return Ok(());}},
                                Action::ResponseCompleted=>{state.responses.fetch_add(1,Ordering::SeqCst);},Action::Usage(u)=>{let frame=Frame::new(FrameData::Metrics(vec![pipecat_core::Metric::LlmUsage{processor:"NovaSonic".into(),prompt_tokens:u.prompt_tokens,completion_tokens:u.completion_tokens,total_tokens:u.total_tokens}]));if !state.endpoint.admit(&ctx,frame,Direction::Downstream)? {return Err(error("Nova usage admission closed"));}},
                            }}
                        }
                    }
                }
            }}.await;
            loop_result
        }
    };
    // No consumer or cleanup waiter takes the actual transport JoinHandle away.
    // Cancellation interrupts queued uploads; it never pretends to emit a terminal.
    if outcome.is_err() || *state.control.borrow() == Control::Cancel {
        transport.cancel();
    }
    drop(response);
    drop(sender);
    state
        .sender
        .lock()
        .map_err(|_| error("Nova sender lock poisoned"))?
        .take();
    let report = completion.wait().await;
    state.transport_joined.store(
        report.writer_joined && report.reader_joined && report.connection_joined,
        Ordering::SeqCst,
    );
    let joined = if *state.control.borrow() == Control::Cancel
        && report.primary == Some(crate::transport::TransportError::Cancelled)
        && report.cleanup.is_none()
        && !report.forced_abort
        && state.transport_joined.load(Ordering::SeqCst)
    {
        Ok(())
    } else {
        transport_outcome(&report)
    };
    state
        .transport_completion
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take();
    let closed = state.endpoint.close_releases();
    // Only after the transport report is joined do we distinguish clean protocol
    // EOF from a stream that closed because its writer/reader/connection failed.
    // This does not block the active read/control lane waiting for task completion.
    let result = if transport_eof
        && report.primary.is_some()
        && report.primary != Some(crate::transport::TransportError::Cancelled)
    {
        combine(joined, outcome)
    } else {
        combine(outcome, joined)
    };
    combine(result, closed)
}

// Delegation preserves original processor identities and exact pair topology.
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
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
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
        if let Err(e) = &result {
            let mut errors = self.errors.lock().unwrap_or_else(|e| e.into_inner());
            if errors.len() < 16 {
                errors.push(
                    format!("{}: {}", self.inner.name(), e.message)
                        .chars()
                        .take(4096)
                        .collect(),
                );
            }
        }
        result
    }
}
#[cfg(test)]
#[path = "app_tests.rs"]
mod tests;

async fn interrupt(state: &State, ctx: &ProcessorContext) -> Result<bool, ProcessorError> {
    let mut control = state.control.subscribe();
    let broadcast = ctx.broadcast_interruption();
    tokio::pin!(broadcast);
    let deadline = tokio::time::sleep(state.config.limits.operation_timeout);
    tokio::pin!(deadline);
    loop {
        if *control.borrow_and_update() == Control::Cancel {
            return Ok(false);
        }
        tokio::select! {biased;
            changed=control.changed()=>{changed.map_err(|_|error("Nova interruption control owner closed"))?;},
            _=&mut deadline=>return Err(error("Nova interruption output admission timed out")),
            result=&mut broadcast=>return result.map(|_|true),
        }
    }
}

fn audio(data: &FrameData) -> Option<&pipecat_core::AudioRawFrame> {
    match data {
        FrameData::InputAudio(a) => Some(a),
        FrameData::UserAudio(a) => Some(&a.audio),
        _ => None,
    }
}
struct ByteBudget(usize);
impl std::io::Write for ByteBudget {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(b.len())
            .ok_or_else(|| std::io::Error::other("Nova public ingress byte limit"))?;
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn validate_input(
    frame: &Frame,
    direction: Direction,
    config: &NovaSonicConfig,
) -> Result<(), ProcessorError> {
    public_ingress(frame)?;
    if frame.name.len() > 256
        || frame.metadata.len() > 64
        || frame
            .transport_source
            .as_ref()
            .is_some_and(|s| s.len() > 256)
        || frame
            .transport_destination
            .as_ref()
            .is_some_and(|s| s.len() > 256)
    {
        return Err(error("Nova public ingress metadata bound exceeded"));
    }
    let mut pending: Vec<(&serde_json::Value, usize)> =
        frame.metadata.values().map(|v| (v, 0)).collect();
    let mut nodes = 0usize;
    if frame.metadata.keys().any(|k| k.len() > 128) {
        return Err(error("Nova metadata key bound exceeded"));
    }
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if nodes > 256 || depth > 8 {
            return Err(error("Nova public metadata depth or node bound exceeded"));
        }
        match value {
            serde_json::Value::String(v) if v.len() > 8192 => {
                return Err(error("Nova public metadata string bound exceeded"))
            }
            serde_json::Value::Array(v) => {
                if v.len() > 256 || pending.len() + v.len() > 256 {
                    return Err(error("Nova public metadata array bound exceeded"));
                }
                pending.extend(v.iter().map(|v| (v, depth + 1)));
            }
            serde_json::Value::Object(v) => {
                if v.len() > 64 || pending.len() + v.len() > 256 || v.keys().any(|k| k.len() > 128)
                {
                    return Err(error("Nova public metadata object bound exceeded"));
                }
                pending.extend(v.values().map(|v| (v, depth + 1)));
            }
            _ => {}
        }
    }
    if direction == Direction::Upstream
        && !matches!(
            frame.data,
            FrameData::Flush(_) | FrameData::FlushDetailed(_)
        )
    {
        return Err(error("Nova caller input must be downstream"));
    }
    if let Some(a) = audio(&frame.data) {
        config.audio(a)?;
    }
    serde_json::to_writer(ByteBudget(config.limits.max_event_bytes), frame)
        .map_err(|_| error("Nova public ingress encoded byte budget exceeded"))?;
    Ok(())
}
/// Bounded input to the actual pipeline. Validation precedes queue ownership.
#[derive(Clone)]
pub struct NovaSonicInput {
    inner: pipecat_core::PipelineSender,
    config: Arc<NovaSonicConfig>,
}
impl NovaSonicInput {
    pub async fn send(&self, frame: Frame, direction: Direction) -> Result<(), PipelineError> {
        let frame = normalize_input(frame, direction, &self.config)
            .map_err(|e| PipelineError::Execution(e.message))?;
        self.inner.send(frame, direction).await
    }
    pub async fn end(&self) -> Result<(), PipelineError> {
        self.inner.end().await
    }
    pub async fn cancel(&self, reason: impl Into<String>) -> Result<(), PipelineError> {
        let reason = reason.into();
        if reason.len() > 4096 {
            return Err(PipelineError::InvalidConfiguration(
                "Nova cancellation reason exceeds bound".into(),
            ));
        }
        self.inner
            .cancel(reason.into_boxed_str().into_string())
            .await
    }
    pub fn state(&self) -> pipecat_core::WorkerState {
        self.inner.state()
    }
}
/// Restricted worker observation/lifecycle; no raw sender or mutable pair escape.
pub struct NovaSonicWorker {
    inner: PipelineWorker,
    config: Arc<NovaSonicConfig>,
}
impl NovaSonicWorker {
    pub fn sender(&self) -> NovaSonicInput {
        NovaSonicInput {
            inner: self.inner.sender(),
            config: self.config.clone(),
        }
    }
    pub async fn recv(&mut self) -> Option<pipecat_core::DirectedFrame> {
        self.inner.recv().await
    }
    pub fn try_recv(
        &mut self,
    ) -> Result<pipecat_core::DirectedFrame, tokio::sync::mpsc::error::TryRecvError> {
        self.inner.try_recv()
    }
    pub async fn end(&self) -> Result<(), PipelineError> {
        self.sender().end().await
    }
    pub async fn cancel(&self, reason: impl Into<String>) -> Result<(), PipelineError> {
        self.sender().cancel(reason).await
    }
    pub async fn join(&mut self) -> Result<pipecat_core::WorkerReport, PipelineError> {
        self.inner.join().await
    }
    pub fn state(&self) -> pipecat_core::WorkerState {
        self.inner.state()
    }
    pub fn has_finished(&self) -> bool {
        self.inner.has_finished()
    }
    pub fn errors(&self) -> Vec<ErrorFrame> {
        self.inner.errors()
    }
    pub fn total_errors(&self) -> u64 {
        self.inner.total_errors()
    }
    pub fn evicted_errors(&self) -> u64 {
        self.inner.evicted_errors()
    }
}

// Ordinary caller data is copied into bounded fresh allocations before any await
// can transfer queue ownership. Spare Vec/String capacity and opaque process-local
// tags are not retained. Exact registered native identities never use this path.
fn normalize_input(
    frame: Frame,
    direction: Direction,
    config: &NovaSonicConfig,
) -> Result<Frame, ProcessorError> {
    validate_input(&frame, direction, config)?;
    let bytes = serde_json::to_vec(&frame)
        .map_err(|_| error("Nova public input serialization failed"))?
        .into_boxed_slice();
    drop(frame);
    let frame = serde_json::from_slice(&bytes)
        .map_err(|_| error("Nova public input reconstruction failed"))?;
    Ok(frame)
}
