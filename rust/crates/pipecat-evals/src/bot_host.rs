//! Explicitly selected, loopback-only evaluation of application pipelines.
//!
//! This endpoint is separate from the capability/Origin-protected browser host.
//! Each connection owns a fresh factory result. Factories must not detach work;
//! slow processing belongs in the pipeline's bounded task manager.
use crate::serializer::{decode_control, EvalControl, ObserverConfig, ReportLevel};
#[cfg(feature = "user-images")]
use crate::user_image_input::{ImageInputLimits, UserImageInput};
use crate::Cancellation;
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_core::*;
use pipecat_protocol::rtvi::{ClientCommand, Envelope};
use pipecat_rtvi::{
    Observation, RtviConfig, RtviHandle, RtviObserver, RtviObserverParams, RtviProcessor,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_turns::SharedContext;
use serde::Serialize;
use serde_json::{json, Map, Value};
use std::{
    collections::VecDeque,
    future::Future,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc, time::Instant};

#[derive(Debug, thiserror::Error)]
pub enum BotHostError {
    #[error("invalid eval host request/configuration: {0}")]
    Invalid(String),
    #[error("eval host limit exceeded: {0}")]
    Limit(&'static str),
    #[error("eval host {0} timed out")]
    Timeout(&'static str),
    #[error("eval client disconnected")]
    Disconnected,
    #[error("eval host cancelled")]
    Cancelled,
    #[error("eval-cancel during startup")]
    StartupEvalCancel,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error(transparent)]
    Processor(#[from] ProcessorError),
    #[error(transparent)]
    Transport(#[from] pipecat_transports::TransportError),
    #[error(transparent)]
    Rtvi(#[from] pipecat_rtvi::RtviError),
    #[error(transparent)]
    Eval(#[from] crate::EvalError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct EvalBotHostConfig {
    pub transport: SessionConfig,
    /// Shutdown timeout is additionally capped per installed processor so
    /// serial core cleanup fits the host shutdown budget.
    pub pipeline: PipelineConfig,
    pub event_capacity: usize,
    pub max_processors: usize,
    pub max_context_messages: usize,
    pub max_context_bytes: usize,
    /// Cumulative bytes measured at each application processor boundary.
    pub max_processed_bytes: usize,
    pub max_processed_frames: usize,
    pub max_wire_bytes: usize,
    pub max_wire_messages: usize,
    pub max_connections: usize,
    pub startup_timeout: Duration,
    pub session_timeout: Duration,
    pub host_timeout: Duration,
    pub shutdown_timeout: Duration,
}
impl Default for EvalBotHostConfig {
    fn default() -> Self {
        Self {
            transport: SessionConfig {
                queue_capacity: 16,
                max_message_bytes: 128 * 1024,
                connect_timeout: Duration::from_secs(5),
                write_timeout: Duration::from_secs(2),
                close_timeout: Duration::from_secs(1),
            },
            pipeline: PipelineConfig {
                queue_capacity: 16,
                system_queue_capacity: 16,
                output_capacity: 32,
                max_background_tasks: 64,
                error_history_capacity: 16,
                startup_timeout: Duration::from_secs(5),
                shutdown_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            event_capacity: 64,
            max_processors: 32,
            max_context_messages: 512,
            max_context_bytes: 512 * 1024,
            max_processed_bytes: 32 * 1024 * 1024,
            max_processed_frames: 65_536,
            max_wire_bytes: 8 * 1024 * 1024,
            max_wire_messages: 8192,
            max_connections: 128,
            startup_timeout: Duration::from_secs(10),
            session_timeout: Duration::from_secs(600),
            host_timeout: Duration::from_secs(3600),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}
impl EvalBotHostConfig {
    fn validate(&self) -> Result<(), BotHostError> {
        let counts = [
            self.transport.queue_capacity,
            self.event_capacity,
            self.max_processors,
            self.max_context_messages,
            self.max_context_bytes,
            self.max_processed_bytes,
            self.max_processed_frames,
            self.max_wire_bytes,
            self.max_wire_messages,
            self.max_connections,
            self.pipeline.queue_capacity,
            self.pipeline.system_queue_capacity,
            self.pipeline.output_capacity,
            self.pipeline.max_background_tasks,
            self.pipeline.error_history_capacity,
        ];
        let times = [
            self.transport.connect_timeout,
            self.transport.write_timeout,
            self.transport.close_timeout,
            self.pipeline.startup_timeout,
            self.pipeline.shutdown_timeout,
            self.startup_timeout,
            self.session_timeout,
            self.host_timeout,
            self.shutdown_timeout,
        ];
        if self.transport.max_message_bytes < 1024
            || self.transport.max_message_bytes > 1024 * 1024
            || counts.iter().any(|n| *n == 0 || *n > 64 * 1024 * 1024)
            || times
                .iter()
                .any(|d| d.is_zero() || *d > Duration::from_secs(86_400))
        {
            return Err(BotHostError::Invalid("positive finite limits required; wire frames must be 1 KiB..1 MiB and deadlines <=24h".into()));
        }
        Ok(())
    }
}

/// Supplied before construction, including before any application greeting.
#[derive(Debug, Clone, Copy)]
pub struct EvalBotSettings {
    pub skip_tts: bool,
}

/// The application owns its conversation, tools and processor composition.
/// `assistant` is observed after it processes each frame, so response completion
/// is published only after aggregation has committed its context effects.
/// All objects must be fresh per create call; processors must honor skip_tts.
pub struct EvalBotPipeline {
    pub context: SharedContext,
    pub processors: Vec<Arc<dyn FrameProcessor>>,
    pub assistant: Arc<dyn FrameProcessor>,
    /// Optional explicit greeting prompt. This begins inference immediately
    /// after readiness; consequently eval-context is unavailable for this session.
    pub introduction: Option<String>,
}
#[async_trait]
pub trait EvalBotFactory: Send + Sync {
    /// Cancellation drops this future. Do not spawn detached factory work or
    /// perform blocking I/O; retain all background work in the returned pipeline.
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalBotExit {
    /// Ordinary disconnect: the host can accept a fresh scenario.
    Disconnected,
    /// Client requested disconnect callbacks: this host exits after cleanup.
    TriggerDisconnect,
    /// eval-cancel cancels the current pipeline and stops the host.
    EvalCancel,
    /// Application End/Stop/disconnect-bot ends this session, keeping the host.
    Ended,
}
#[derive(Debug, Default)]
pub struct EvalBotHostReport {
    pub connections: usize,
    pub sessions: usize,
    pub last_exit: Option<EvalBotExit>,
    pub rejected_handshakes: usize,
}

/// No listener/session task is spawned by this host. The caller owns `serve`.
/// Cooperative cancellation joins the pipeline and WebSocket owners. Dropping
/// `serve` invokes their cancellation guards; it never keeps the listener alive.
pub struct EvalBotHost {
    listener: TcpListener,
    config: EvalBotHostConfig,
    raw_audio_input: bool,
    audio_output: bool,
    #[cfg(feature = "user-images")]
    user_images: Option<ImageInputLimits>,
}
impl EvalBotHost {
    pub async fn bind(
        address: SocketAddr,
        config: EvalBotHostConfig,
    ) -> Result<Self, BotHostError> {
        config.validate()?;
        if !address.ip().is_loopback() {
            return Err(BotHostError::Invalid(
                "eval host requires a literal loopback IP".into(),
            ));
        }
        let socket = if address.is_ipv4() {
            tokio::net::TcpSocket::new_v4()?
        } else {
            tokio::net::TcpSocket::new_v6()?
        };
        socket.bind(address)?;
        Ok(Self {
            listener: socket.listen(1)?,
            config,
            raw_audio_input: false,
            audio_output: false,
            #[cfg(feature = "user-images")]
            user_images: None,
        })
    }
    /// Accept single RTVI `raw-audio` messages containing 16 kHz mono PCM16.
    /// The application factory must supply its own audio processing/STT pipeline.
    /// Output stays text-only unless separately enabled; batch/binary input is unsupported.
    pub fn with_raw_audio_input(mut self, enabled: bool) -> Self {
        self.raw_audio_input = enabled;
        self
    }
    /// Permit per-connection bot PCM capture. Only a capture_bot_audio=true
    /// handshake enables synthesis; ordinary connections remain text-only.
    pub fn with_audio_output(mut self, enabled: bool) -> Self {
        self.audio_output = enabled;
        self
    }

    /// Enable bounded, passive PNG/JPEG registrations and actual upstream
    /// plain UserImageRequest responses. No worker is created until a session
    /// starts; ordinary hosts do not create image workers.
    #[cfg(feature = "user-images")]
    pub fn with_user_images(mut self, limits: ImageInputLimits) -> Result<Self, BotHostError> {
        limits.validate()?;
        self.user_images = Some(limits);
        Ok(self)
    }

    pub fn local_addr(&self) -> Result<SocketAddr, BotHostError> {
        Ok(self.listener.local_addr()?)
    }
    pub async fn serve(
        self,
        factory: &dyn EvalBotFactory,
        cancel: Cancellation,
    ) -> Result<EvalBotHostReport, BotHostError> {
        let mut report = EvalBotHostReport::default();
        let deadline = Instant::now() + self.config.host_timeout;
        loop {
            if report.connections == self.config.max_connections {
                return Err(BotHostError::Limit("connections"));
            }
            let (stream, peer) = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(report),
                _ = tokio::time::sleep_until(deadline) => return Err(BotHostError::Timeout("host")),
                result = self.listener.accept() => result?,
            };
            report.connections += 1;
            if !peer.ip().is_loopback() {
                continue;
            }
            let flags = Arc::new(Mutex::new(None));
            let captured = flags.clone();
            let allow_audio = self.audio_output;
            let authority = self.local_addr()?.to_string();
            let accepted = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(report),
                _ = tokio::time::sleep_until(deadline) => return Err(BotHostError::Timeout("host")),
                result = WebSocketSession::accept_with_request_policy(stream, self.config.transport.clone(), move |request| {
                    // Eval is never callable by a browser, even a localhost page.
                    if request.headers().contains_key("origin") || request.uri().path() != "/" || request.uri().scheme().is_some() || request.uri().authority().is_some() { return false; }
                    let mut hosts = request.headers().get_all("host").iter();
                    if hosts.next().and_then(|v| v.to_str().ok()) != Some(authority.as_str()) || hosts.next().is_some() { return false; }
                    let mut trigger = false;
                    let mut skip_seen = false;
                    let mut trigger_seen = false;
                    let mut capture_audio = false;
                    for (key, value) in url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes()) {
                        match key.as_ref() {
                            "skip_tts" if !skip_seen && value == "true" => skip_seen = true,
                            "capture_bot_audio" if allow_audio && !capture_audio && value == "true" => capture_audio = true,
                            "trigger_disconnect" if !trigger_seen && matches!(value.as_ref(), "true" | "false") => { trigger_seen = true; trigger = value == "true"; }
                            _ => return false,
                        }
                    }
                    if capture_audio && skip_seen { return false; }
                    *captured.lock().unwrap() = Some((trigger, capture_audio));
                    true
                }) => result,
            };
            let socket = match accepted {
                Ok(socket) => socket,
                Err(_) => {
                    report.rejected_handshakes += 1;
                    continue;
                }
            };
            let (trigger, capture_audio) = flags.lock().unwrap().take().unwrap_or((false, false));
            let result = self
                .session(socket, factory, &cancel, deadline, capture_audio)
                .await;
            report.sessions += 1;
            let exit = match result {
                Ok(EvalBotExit::Disconnected) if trigger => EvalBotExit::TriggerDisconnect,
                Ok(exit) => exit,
                Err(BotHostError::Disconnected) if trigger => EvalBotExit::TriggerDisconnect,
                Err(BotHostError::Disconnected) => EvalBotExit::Disconnected,
                Err(BotHostError::Cancelled) => return Ok(report),
                Err(BotHostError::StartupEvalCancel) => EvalBotExit::EvalCancel,
                Err(error) => return Err(error),
            };
            report.last_exit = Some(exit);
            if matches!(
                exit,
                EvalBotExit::TriggerDisconnect | EvalBotExit::EvalCancel
            ) {
                return Ok(report);
            }
        }
    }
    async fn session(
        &self,
        mut socket: WebSocketSession,
        factory: &dyn EvalBotFactory,
        cancel: &Cancellation,
        host_deadline: Instant,
        capture_audio: bool,
    ) -> Result<EvalBotExit, BotHostError> {
        let config = &self.config;
        let deadline = (Instant::now() + config.session_timeout).min(host_deadline);
        let startup = (Instant::now() + config.startup_timeout).min(deadline);
        let mut pending = VecDeque::new();
        let plan = during_startup(
            factory.create(EvalBotSettings {
                skip_tts: !capture_audio,
            }),
            &mut socket,
            &mut pending,
            &self.listener,
            config,
            cancel,
            startup,
        )
        .await;
        let plan = match plan {
            Ok(plan) => plan,
            Err(error) => {
                close_socket(socket, config).await?;
                return Err(error);
            }
        };
        if plan.processors.len() + 1 > config.max_processors {
            close_socket(socket, config).await?;
            return Err(BotHostError::Limit("application processors"));
        }
        let mut capabilities = json!({"eval":true,"text_only":true,"context_mode":"generated-text","playback_acknowledgements":false}).as_object().unwrap().clone();
        if self.raw_audio_input {
            capabilities.insert("text_only".into(), false.into());
            capabilities.insert("raw_audio_input".into(), true.into());
            capabilities.insert("audio_output".into(), false.into());
        }
        if capture_audio {
            capabilities.insert("text_only".into(), false.into());
            capabilities.insert("audio_output".into(), true.into());
        }
        #[cfg(feature = "user-images")]
        if self.user_images.is_some() {
            capabilities.insert("user_image_input".into(), true.into());
        }
        let rtvi = Arc::new(RtviProcessor::new(RtviConfig {
            command_capacity: config.transport.queue_capacity.max(2),
            event_capacity: config.event_capacity,
            max_message_bytes: config.transport.max_message_bytes,
            flush_timeout: config.startup_timeout,
            capabilities: Some(capabilities),
            ..Default::default()
        })?);
        let handle = rtvi.handle();
        let observer = Arc::new(RtviObserver::new(
            RtviObserverParams {
                bot_output_enabled: capture_audio,
                bot_speaking_enabled: capture_audio,
                bot_tts_enabled: capture_audio,
                max_message_bytes: config.transport.max_message_bytes,
                max_pending_text_bytes: config.max_context_bytes,
                dedup_capacity: config.max_processed_frames.min(4096),
                ..Default::default()
            },
            Some(handle.clone()),
        )?);
        let (events_tx, mut events) = mpsc::channel(config.event_capacity);
        #[cfg(feature = "user-images")]
        let images = match self
            .user_images
            .clone()
            .map(UserImageInput::new)
            .transpose()
        {
            Ok(images) => images.map(Arc::new),
            Err(error) => {
                close_socket(socket, config).await?;
                return Err(error);
            }
        };
        let budget = Arc::new(Budget {
            counts: Mutex::new((0, 0)),
            output_open: AtomicBool::new(true),
            config: config.clone(),
            context: plan.context.clone(),
            #[cfg(feature = "user-images")]
            images: images.clone(),
        });
        if let Err(error) = budget.context() {
            close_socket(socket, config).await?;
            return Err(error);
        }
        let owners = Arc::new(ProcessorOwners::default());
        let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![rtvi];
        #[cfg(feature = "user-images")]
        if let Some(images) = &images {
            processors.push(images.clone());
        }
        for processor in plan.processors {
            processors.push(Arc::new(Boundary {
                inner: processor,
                budget: budget.clone(),
                observer: None,
                capture: None,
                _owner: owners.track(),
            }));
        }
        processors.push(Arc::new(Boundary {
            inner: plan.assistant,
            budget: budget.clone(),
            observer: Some((observer.clone(), events_tx.clone())),
            capture: None,
            _owner: owners.track(),
        }));
        // Initialization must arrive at the tail before readiness or introductions.
        if capture_audio {
            processors.push(Arc::new(Boundary {
                inner: Arc::new(IdentityProcessor),
                budget: budget.clone(),
                observer: None,
                capture: Some(events_tx),
                _owner: owners.track(),
            }));
        } else {
            drop(events_tx);
            processors.push(Arc::new(IdentityProcessor));
        }
        let mut pipeline_config = config.pipeline.clone();
        let cleanup_slice = config.shutdown_timeout
            / u32::try_from(processors.len() + 2).map_err(|_| BotHostError::Limit("processors"))?;
        if cleanup_slice.is_zero() {
            drop(processors);
            close_socket(socket, config).await?;
            return Err(BotHostError::Invalid(
                "shutdown budget is too small for processor count".into(),
            ));
        }
        pipeline_config.shutdown_timeout = pipeline_config.shutdown_timeout.min(cleanup_slice);
        let mut worker = match during_startup(
            async { Ok(Pipeline::new(processors).start(pipeline_config).await?) },
            &mut socket,
            &mut pending,
            &self.listener,
            config,
            cancel,
            startup,
        )
        .await
        {
            Ok(worker) => worker,
            Err(error) => {
                let closed = close_socket(socket, config).await;
                owners.wait_empty(config.shutdown_timeout).await?;
                closed?;
                return Err(error);
            }
        };
        let result = self
            .drive(
                &mut worker,
                &mut socket,
                &mut pending,
                &mut events,
                &handle,
                &observer,
                &budget,
                plan.introduction,
                capture_audio,
                cancel,
                startup,
                deadline,
            )
            .await;
        // Close all output admission first. No queued old event is sent during cleanup.
        budget.output_open.store(false, Ordering::SeqCst);
        events.close();
        let graceful = matches!(&result, Ok(EvalBotExit::Ended));
        #[cfg(feature = "user-images")]
        let pre_cancel_requested = images.is_some() && !worker.has_finished();
        #[cfg(not(feature = "user-images"))]
        let pre_cancel_requested = false;
        // Admit pipeline cancellation before closing the image source.
        // Keep every cleanup owner even if cancellation admission itself fails.
        let image_cancel_result = if pre_cancel_requested {
            match tokio::time::timeout(
                config.shutdown_timeout,
                worker.cancel("eval image session closed"),
            )
            .await
            {
                Ok(Ok(())) | Ok(Err(PipelineError::Closed)) => Ok(()),
                Ok(Err(error)) => Err(BotHostError::Pipeline(error)),
                Err(_) => Err(BotHostError::Timeout(
                    "image pipeline cancellation admission",
                )),
            }
        } else {
            Ok(())
        };
        #[cfg(feature = "user-images")]
        if let Some(images) = &images {
            images.close();
        }
        let image_cleanup = async {
            #[cfg(feature = "user-images")]
            if let Some(images) = &images {
                images.shutdown_joined().await?;
            }
            Ok::<(), BotHostError>(())
        };
        let (cleanup, transport_cleanup, image_cleanup) = tokio::join!(
            drain_worker(
                &mut worker,
                config,
                pre_cancel_requested && image_cancel_result.is_ok()
            ),
            async {
                if graceful {
                    tokio::time::timeout(config.shutdown_timeout, socket.finish())
                        .await
                        .map_err(|_| BotHostError::Timeout("graceful socket drain"))??;
                } else {
                    close_socket(socket, config).await?;
                }
                Ok::<(), BotHostError>(())
            },
            image_cleanup
        );
        drop(worker);
        owners.wait_empty(config.shutdown_timeout).await?;
        cleanup?;
        transport_cleanup?;
        image_cleanup?;
        image_cancel_result?;
        result
    }
    #[allow(clippy::too_many_arguments)]
    async fn drive(
        &self,
        worker: &mut PipelineWorker,
        socket: &mut WebSocketSession,
        pending: &mut VecDeque<Packet>,
        events: &mut mpsc::Receiver<ObservedEvent>,
        handle: &RtviHandle,
        observer: &RtviObserver,
        budget: &Budget,
        introduction: Option<String>,
        capture_audio: bool,
        cancel: &Cancellation,
        startup: Instant,
        deadline: Instant,
    ) -> Result<EvalBotExit, BotHostError> {
        let config = &self.config;
        let sender = worker.sender();
        tokio::time::timeout_at(
            startup,
            sender.send(
                Frame::new(FrameData::LlmConfigureOutput {
                    skip_tts: !capture_audio,
                }),
                Direction::Downstream,
            ),
        )
        .await
        .map_err(|_| BotHostError::Timeout("initial output configuration"))??;
        // LlmProcessor consumes output configuration. A subsequent normal-lane
        // native flush proves it has applied configuration before readiness.
        let initialized_barrier = LocalFlushEvent::default();
        tokio::time::timeout_at(
            startup,
            sender.send(
                Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                    event: Some(initialized_barrier.clone()),
                    returning: false,
                    origin: Some("eval-text-initialized".into()),
                })),
                Direction::Downstream,
            ),
        )
        .await
        .map_err(|_| BotHostError::Timeout("initialization barrier admission"))??;
        let mut initialized = false;
        let mut ready = false;
        let mut bot_ready_sent = false;
        let mut input_admitted = false;
        let mut wire_bytes = 0usize;
        let mut wire_messages = 0usize;
        let mut introduction = introduction;
        let mut events_open = true;
        loop {
            if Instant::now() >= deadline {
                return Err(BotHostError::Timeout("session"));
            }
            if !bot_ready_sent && Instant::now() >= startup {
                return Err(BotHostError::Timeout("readiness"));
            }
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(BotHostError::Cancelled),
                _ = tokio::time::sleep_until(deadline) => return Err(BotHostError::Timeout("session")),
                _ = tokio::time::sleep_until(startup), if !bot_ready_sent => return Err(BotHostError::Timeout("readiness")),
                _ = initialized_barrier.wait(), if !initialized => initialized = true,
                packet = async { if initialized { if let Some(packet) = pending.pop_front() { return Some(packet); } } socket.recv().await } => {
                    let Some(packet) = packet else { return Ok(EvalBotExit::Disconnected); };
                    if !initialized {
                        let Packet::Text(text) = &packet else { return Err(BotHostError::Invalid("text WebSocket frames only".into())); };
                        validate_envelope(text.as_bytes(), config.transport.max_message_bytes)?;
                        if matches!(decode_control(text.as_bytes(), config.transport.max_message_bytes)?, EvalControl::Cancel) { return Ok(EvalBotExit::EvalCancel); }
                        if pending.len() == config.transport.queue_capacity { return Err(BotHostError::Limit("startup input queue")); }
                        pending.push_back(packet); continue;
                    }
                    let Packet::Text(text) = packet else { return Err(BotHostError::Invalid("text WebSocket frames only".into())); };
                    wire_bytes = wire_bytes.checked_add(text.len()).ok_or(BotHostError::Limit("wire bytes"))?;
                    wire_messages += 1;
                    if wire_bytes > config.max_wire_bytes || wire_messages > config.max_wire_messages { return Err(BotHostError::Limit("wire input")); }
                    validate_envelope(text.as_bytes(), config.transport.max_message_bytes)?;
                    match decode_control(text.as_bytes(), config.transport.max_message_bytes)? {
                        EvalControl::Cancel => return Ok(EvalBotExit::EvalCancel),
                        EvalControl::Image { bytes, format } => {
                            #[cfg(feature = "user-images")]
                            if let Some(images) = &budget.images {
                                if !ready { return Err(BotHostError::Invalid("eval images require client readiness".into())); }
                                // Passive atomic replacement in this socket reader. It neither
                                // closes context/config admission nor launches any inference.
                                images.register_wire(bytes, &format)?;
                                continue;
                            }
                            #[cfg(not(feature = "user-images"))]
                            let _ = (bytes, format);
                            return Err(BotHostError::Invalid("eval images are unsupported by the text host".into()));
                        }
                        EvalControl::Context(messages) => {
                            if !ready || input_admitted { return Err(BotHostError::Invalid("context must precede the first input or introduction".into())); }
                            seed_context(&budget.context, messages, config)?;
                        }
                        EvalControl::Configure(options) => {
                            if !ready || input_admitted { return Err(BotHostError::Invalid("observer configuration must precede the first input or introduction".into())); }
                            configure_observer(observer, options)?;
                        }
                        EvalControl::Rtvi(mut message) => {
                            match message.message_type.as_str() {
                                "client-ready" if !ready => { message.client_command().map_err(crate::EvalError::from)?; handle.handle_message(message)?; handle.set_bot_ready(None)?; ready = true; if introduction.is_some() { input_admitted = true; } }
                                "send-text" if ready => {
                                    input_admitted = true;
                                    let data = message.data.as_mut().and_then(Value::as_object_mut).ok_or_else(|| BotHostError::Invalid("send-text requires data".into()))?;
                                    let options = data.entry("options").or_insert_with(|| json!({})).as_object_mut().ok_or_else(|| BotHostError::Invalid("invalid send-text options".into()))?;
                                    if options.get("audio_response").is_some_and(|v| v != &Value::Bool(capture_audio)) { return Err(BotHostError::Invalid("audio responses are unsupported".into())); }
                                    options.insert("audio_response".into(), capture_audio.into());
                                    budget.context()?;
                                    handle.handle_message(message)?;
                                }
                                "raw-audio" if ready && self.raw_audio_input => {
                                    let ClientCommand::RawAudio(audio) = message.client_command().map_err(crate::EvalError::from)? else {
                                        return Err(BotHostError::Invalid("expected raw-audio".into()));
                                    };
                                    if audio.sample_rate != 16_000 || audio.num_channels != 1 {
                                        return Err(BotHostError::Invalid("raw audio requires 16 kHz mono PCM16".into()));
                                    }
                                    // Validate before closing context/config admission. Wire size is
                                    // already bounded; native frame budgets apply again downstream.
                                    audio.decode().map_err(crate::EvalError::from)?;
                                    input_admitted = true;
                                    budget.context()?;
                                    handle.handle_message(message)?;
                                }
                                "disconnect-bot" if ready => { tokio::time::timeout(config.transport.write_timeout, worker.end()).await.map_err(|_| BotHostError::Timeout("End admission"))??; }
                                _ => return Err(BotHostError::Invalid(if self.raw_audio_input { "only client-ready, send-text, raw-audio, disconnect-bot and eval controls are supported" } else { "only client-ready, send-text, disconnect-bot and eval controls are supported" }.into())),
                            }
                        }
                    }
                }
                extra = self.listener.accept() => { drop(extra?); },
                event = events.recv(), if events_open => match event {
                    Some(ObservedEvent::Envelope { envelope, fence }) => {
                        if !initialized { return Err(BotHostError::Invalid("application emitted before text configuration".into())); }
                        if fence.is_current() { send_envelope(socket, envelope, config, cancel, deadline, capture_audio).await?; }
                    }
                    Some(ObservedEvent::Flush(ack)) => { let _ = ack.send(()); }
                    None => events_open = false,
                },
                output = worker.recv() => match output {
                    None => return Ok(EvalBotExit::Ended),
                    Some(output) => {
                        if let Some(error) = output.frame.data.as_error_frame() { return Err(BotHostError::Invalid(format!("application pipeline reported: {}", error.error))); }
                        if let FrameData::OutputTransportMessageUrgent(value) | FrameData::OutputTransportMessage(value) = output.frame.data {
                            let envelope: Envelope = serde_json::from_value(value)?;
                            let bot_ready = envelope.message_type == "bot-ready";
                            send_envelope(socket, envelope, config, cancel, deadline, capture_audio).await?;
                            if bot_ready { bot_ready_sent = true; if let Some(prompt) = introduction.take() { budget.context.append_rich(ChatMessage::new(ChatRole::Developer, prompt).into())?; budget.context()?; tokio::time::timeout(config.transport.write_timeout, sender.send(Frame::new(FrameData::LlmRun), Direction::Downstream)).await.map_err(|_| BotHostError::Timeout("introduction admission"))??; } }
                        }
                    }
                },
            }
        }
    }
}

async fn during_startup<T>(
    future: impl Future<Output = Result<T, BotHostError>>,
    socket: &mut WebSocketSession,
    pending: &mut VecDeque<Packet>,
    listener: &TcpListener,
    config: &EvalBotHostConfig,
    cancel: &Cancellation,
    deadline: Instant,
) -> Result<T, BotHostError> {
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(BotHostError::Cancelled),
            _ = tokio::time::sleep_until(deadline) => return Err(BotHostError::Timeout("startup")),
            extra = listener.accept() => { drop(extra?); },
            packet = socket.recv() => match packet {
                Some(packet) => {
                    let Packet::Text(text) = &packet else { return Err(BotHostError::Invalid("text WebSocket frames only".into())); };
                    validate_envelope(text.as_bytes(), config.transport.max_message_bytes)?;
                    if matches!(decode_control(text.as_bytes(), config.transport.max_message_bytes)?, EvalControl::Cancel) { return Err(BotHostError::StartupEvalCancel); }
                    if pending.len() == config.transport.queue_capacity { return Err(BotHostError::Limit("startup input queue")); }
                    pending.push_back(packet);
                }
                None => return Err(BotHostError::Disconnected),
            },
            result = &mut future => return result,
        }
    }
}
async fn drain_worker(
    worker: &mut PipelineWorker,
    config: &EvalBotHostConfig,
    cancel_admitted: bool,
) -> Result<(), BotHostError> {
    tokio::time::timeout(config.shutdown_timeout, async {
        if !worker.has_finished() && !cancel_admitted {
            if let Err(error) = worker.cancel("eval session closed").await {
                if error != PipelineError::Closed {
                    return Err(error.into());
                }
            }
        }
        while worker.recv().await.is_some() {}
        let report = worker.join().await?;
        if report.total_errors > 0 {
            return Err(BotHostError::Invalid(
                "application pipeline failed during cleanup".into(),
            ));
        }
        Ok(())
    })
    .await
    .map_err(|_| BotHostError::Timeout("pipeline cleanup"))?
}
async fn close_socket(
    socket: WebSocketSession,
    config: &EvalBotHostConfig,
) -> Result<(), BotHostError> {
    match tokio::time::timeout(config.shutdown_timeout, socket.cancel())
        .await
        .map_err(|_| BotHostError::Timeout("socket cancellation"))?
    {
        Ok(()) | Err(pipecat_transports::TransportError::Closed) => Ok(()),
        Err(error) if peer_disconnect(&error) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
// Peer resets during cancellation are ordinary disconnects; write/close
// deadlines, task failures and protocol errors remain visible.
fn peer_disconnect(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut error = Some(error);
    while let Some(current) = error {
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            return matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::NotConnected
            );
        }
        error = current.source();
    }
    false
}
async fn send_envelope(
    socket: &WebSocketSession,
    envelope: Envelope,
    config: &EvalBotHostConfig,
    cancel: &Cancellation,
    deadline: Instant,
    capture_audio: bool,
) -> Result<(), BotHostError> {
    // A text host never claims physical playback, even if an application emits
    // speaking or synthesis observations by mistake.
    if envelope.message_type == "eval-bot-image"
        || (!capture_audio
            && matches!(
                envelope.message_type.as_str(),
                "bot-started-speaking"
                    | "bot-stopped-speaking"
                    | "bot-output"
                    | "bot-tts-text"
                    | "bot-tts-started"
                    | "bot-tts-stopped"
                    | "eval-bot-audio"
                    | "eval-bot-image"
            ))
    {
        return Err(BotHostError::Invalid(
            "non-text bot output on text eval host".into(),
        ));
    }
    let encoded = envelope.encode().map_err(crate::EvalError::from)?;
    let sender = socket.sender();
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(BotHostError::Cancelled),
        result = tokio::time::timeout_at(deadline.min(Instant::now() + config.transport.write_timeout), sender.send(Packet::Text(encoded))) => result.map_err(|_| BotHostError::Timeout("event output"))?.map_err(Into::into),
    }
}

enum ObservedEvent {
    Envelope {
        envelope: Envelope,
        fence: InvocationFence,
    },
    Flush(tokio::sync::oneshot::Sender<()>),
}
struct Budget {
    output_open: AtomicBool,
    counts: Mutex<(usize, usize)>,
    config: EvalBotHostConfig,
    context: SharedContext,
    #[cfg(feature = "user-images")]
    images: Option<Arc<UserImageInput>>,
}
impl Budget {
    fn context(&self) -> Result<(), BotHostError> {
        let context = self.context.snapshot_rich()?;
        if context.messages.len() > self.config.max_context_messages {
            return Err(BotHostError::Limit("context messages"));
        }
        measure(&context, self.config.max_context_bytes)?;
        Ok(())
    }
    fn admit(&self, frame: &Frame) -> Result<(), ProcessorError> {
        // Terminal cleanup remains admissible after exhaustion.
        if frame.is_terminal() || frame.data.is_start() {
            return Ok(());
        }
        let limit = self.config.transport.max_message_bytes;
        #[cfg(feature = "user-images")]
        let native_media = self
            .images
            .as_ref()
            .map(|images| images.media_bytes(frame))
            .transpose()?
            .flatten();
        #[cfg(not(feature = "user-images"))]
        let native_media: Option<usize> = None;
        // Local flush authority is forwarded intact, never encoded or recreated.
        let measured = (|| -> Result<usize, BotHostError> {
            if let Some(bytes) = native_media {
                return Ok(bytes);
            }
            let payload_bytes = match &frame.data {
                FrameData::FlushDetailed(flush) => {
                    measure(&(&flush.origin, flush.returning), limit)?.saturating_add(64)
                }
                FrameData::FunctionCallResultDetailed(result) => {
                    let mut projection = result.clone();
                    if let Some(properties) = &mut projection.properties {
                        properties.on_context_updated = None;
                    }
                    measure(&projection, limit)?.saturating_add(64)
                }
                _ => measure(&frame.data, limit)?,
            };
            Ok(payload_bytes.saturating_add(measure(&frame.metadata, limit)?))
        })();
        let bytes = measured
            .map_err(processor_error)?
            .saturating_add(frame.native_llm_continuation_retained_bytes())
            .saturating_add(
                frame
                    .native_tool_batch()?
                    .map_or(0, |batch| batch.retained_bytes()),
            )
            .saturating_add(
                frame
                    .native_tool_outcome()?
                    .map_or(0, |outcome| outcome.retained_bytes()),
            );
        if native_media.is_none() && bytes > limit {
            return Err(ProcessorError::fatal(
                "eval frame envelope byte limit exceeded",
            ));
        }
        let mut counts = self.counts.lock().unwrap();
        counts.0 = counts.0.saturating_add(1);
        counts.1 = counts.1.saturating_add(bytes);
        if counts.0 > self.config.max_processed_frames || counts.1 > self.config.max_processed_bytes
        {
            return Err(ProcessorError::fatal(
                "eval processed frame budget exceeded",
            ));
        }
        drop(counts);
        self.context().map_err(processor_error)
    }
}
struct Boundary {
    _owner: ProcessorOwner,
    inner: Arc<dyn FrameProcessor>,
    budget: Arc<Budget>,
    observer: Option<(Arc<RtviObserver>, mpsc::Sender<ObservedEvent>)>,
    capture: Option<mpsc::Sender<ObservedEvent>>,
}
#[async_trait]
impl FrameProcessor for Boundary {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        config: &StartFrame,
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
        #[cfg(feature = "user-images")]
        if let Some(images) = &self.budget.images {
            if !images.validate_frame(&frame, direction)? {
                return Ok(());
            }
        } else if frame.native_user_image_constraint(direction)?.is_some()
            || frame.native_user_image_context(direction)?.is_some()
        {
            return Err(ProcessorError::fatal("eval image input is not enabled"));
        }
        self.budget.admit(&frame)?;
        let terminal = frame.is_terminal();
        // Keep original invocation authority live until already admitted events
        // have entered the bounded transport. The host then finishes its writer;
        // Cancel deliberately bypasses both drains.
        if terminal && !frame.data.is_cancel() && self.budget.output_open.load(Ordering::SeqCst) {
            if let Some(tx) = self
                .observer
                .as_ref()
                .map(|(_, tx)| tx)
                .or(self.capture.as_ref())
            {
                let (ack, received) = tokio::sync::oneshot::channel();
                tokio::time::timeout(self.budget.config.shutdown_timeout, async {
                    tx.send(ObservedEvent::Flush(ack))
                        .await
                        .map_err(|_| ProcessorError::fatal("eval event drain closed"))?;
                    received
                        .await
                        .map_err(|_| ProcessorError::fatal("eval event drain abandoned"))
                })
                .await
                .map_err(|_| ProcessorError::fatal("eval event drain timed out"))??;
            }
        }
        let observed = self
            .observer
            .as_ref()
            .filter(|_| {
                !terminal
                    && !matches!(
                        frame.data,
                        FrameData::FlushDetailed(_)
                            | FrameData::UserImage(_)
                            | FrameData::InputImage(_)
                            | FrameData::OutputImage(_)
                    )
            })
            .map(|_| {
                let mut observed = frame.clone();
                if let FrameData::FunctionCallResultDetailed(result) = &mut observed.data {
                    if let Some(properties) = &mut result.properties {
                        properties.on_context_updated = None;
                    }
                }
                observed
            });
        let captured_audio = if self.capture.is_some() && direction == Direction::Downstream {
            match &frame.data {
                FrameData::OutputAudio(audio)
                | FrameData::TtsAudio(audio)
                | FrameData::SpeechOutputAudio(audio) => {
                    if audio.num_channels != 1 {
                        return Err(ProcessorError::fatal(
                            "bot audio capture requires mono PCM16",
                        ));
                    }
                    crate::bot_audio::validate_pcm(&audio.audio, audio.sample_rate)
                        .map_err(processor_error)?;
                    let encoded_len = audio.audio.len().div_ceil(3).saturating_mul(4);
                    if encoded_len.saturating_add(128)
                        > self.budget.config.transport.max_message_bytes
                    {
                        return Err(ProcessorError::fatal("bot audio wire byte limit exceeded"));
                    }
                    Some(Envelope {
                        label: pipecat_protocol::rtvi::MESSAGE_LABEL.into(),
                        message_type: "eval-bot-audio".into(),
                        id: None,
                        data: Some(
                            json!({"audio":STANDARD.encode(&audio.audio),"sampleRate":audio.sample_rate}),
                        ),
                    })
                }
                _ => None,
            }
        } else {
            None
        };
        let fence = ctx.invocation_fence()?;
        self.inner.process(frame, direction, ctx).await?;
        if let (Some(tx), Some(envelope)) = (&self.capture, captured_audio) {
            if fence.is_current()
                && self.budget.output_open.load(Ordering::SeqCst)
                && tx
                    .try_send(ObservedEvent::Envelope {
                        envelope,
                        fence: fence.clone(),
                    })
                    .is_err()
                && self.budget.output_open.load(Ordering::SeqCst)
            {
                return Err(ProcessorError::fatal(
                    "eval audio capture queue full or closed",
                ));
            }
        }
        if !terminal {
            self.budget.context().map_err(processor_error)?;
        }
        if let (Some((observer, tx)), Some(frame)) = (&self.observer, observed) {
            if fence.is_current() && self.budget.output_open.load(Ordering::SeqCst) {
                for envelope in
                    observer.observe(&frame, Observation::new(self.name(), direction))?
                {
                    if tx
                        .try_send(ObservedEvent::Envelope {
                            envelope,
                            fence: fence.clone(),
                        })
                        .is_err()
                        && self.budget.output_open.load(Ordering::SeqCst)
                    {
                        return Err(ProcessorError::fatal("eval observer queue full or closed"));
                    }
                }
            }
        }
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let mut outcome = CleanupOutcome {
            owners: self._owner.0.clone(),
            success: false,
        };
        let result = self.inner.cleanup().await;
        outcome.success = result.is_ok();
        result
    }
}
fn processor_error(error: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::fatal(error.to_string())
}
fn measure(value: &impl Serialize, limit: usize) -> Result<usize, BotHostError> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self.bytes.saturating_add(bytes.len());
            if self.bytes > self.limit {
                return Err(std::io::Error::other("eval serialized byte limit exceeded"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| BotHostError::Limit("serialized bytes"))?;
    Ok(counter.bytes)
}
fn seed_context(
    context: &SharedContext,
    messages: Vec<Map<String, Value>>,
    config: &EvalBotHostConfig,
) -> Result<(), BotHostError> {
    if messages.len() > config.max_context_messages {
        return Err(BotHostError::Limit("context messages"));
    }
    measure(&messages, config.max_context_bytes)?;
    let mut replacement = context.snapshot_rich()?;
    replacement.messages = messages
        .into_iter()
        .map(|message| {
            if message.len() != 2
                || !message.contains_key("role")
                || !message.get("content").is_some_and(Value::is_string)
            {
                return Err(BotHostError::Invalid(
                    "seed messages require only role and text content".into(),
                ));
            }
            let role: ChatRole = serde_json::from_value(message["role"].clone())?;
            if role == ChatRole::Tool {
                return Err(BotHostError::Invalid(
                    "seeded tool results require a native tool history and are unsupported".into(),
                ));
            }
            Ok(ChatMessage::new(role, message["content"].as_str().unwrap()).into())
        })
        .collect::<Result<_, BotHostError>>()?;
    measure(&replacement, config.max_context_bytes)?;
    context.replace_rich(replacement)?;
    Ok(())
}
fn configure_observer(observer: &RtviObserver, config: ObserverConfig) -> Result<(), BotHostError> {
    if config
        .function_call_report_level
        .as_ref()
        .is_some_and(|levels| {
            levels.len() > 128
                || levels
                    .keys()
                    .any(|name| name.is_empty() || name.len() > 256)
        })
    {
        return Err(BotHostError::Limit(
            "observer function report configuration",
        ));
    }
    let frame = Frame::new(FrameData::RtviConfigureObserver(
        RtviConfigureObserverFrame {
            function_call_report_level: config.function_call_report_level.map(|levels| {
                levels
                    .into_iter()
                    .map(|(name, level)| {
                        (
                            name,
                            match level {
                                ReportLevel::None => RtviFunctionCallReportLevel::None,
                                ReportLevel::Name => RtviFunctionCallReportLevel::Name,
                                ReportLevel::Full => RtviFunctionCallReportLevel::Full,
                            },
                        )
                    })
                    .collect()
            }),
            vad_user_speaking_enabled: config.vad_user_speaking,
            bot_llm_marker_enabled: config.llm_markers,
        },
    ));
    observer.observe(
        &frame,
        Observation::new("EvalControl", Direction::Downstream),
    )?;
    Ok(())
}

fn validate_envelope(bytes: &[u8], limit: usize) -> Result<(), BotHostError> {
    let message = Envelope::parse_with_limit(bytes, limit).map_err(crate::EvalError::from)?;
    if message
        .id
        .as_ref()
        .is_none_or(|id| id.is_empty() || id.len() > 512)
        || message.message_type.len() > 128
        || message
            .data
            .as_ref()
            .is_some_and(|value| !value.is_object() && !value.is_null())
    {
        return Err(BotHostError::Invalid(
            "bounded nonempty message ID and object data required".into(),
        ));
    }
    Ok(())
}

#[derive(Default)]
struct ProcessorOwners {
    count: AtomicUsize,
    failed_cleanup: AtomicUsize,
    changed: tokio::sync::Notify,
}
impl ProcessorOwners {
    fn track(self: &Arc<Self>) -> ProcessorOwner {
        self.count.fetch_add(1, Ordering::SeqCst);
        ProcessorOwner(self.clone())
    }
    async fn wait_empty(&self, deadline: Duration) -> Result<(), BotHostError> {
        tokio::time::timeout(deadline, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.count.load(Ordering::SeqCst) == 0 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| BotHostError::Timeout("processor ownership cleanup"))?;
        if self.failed_cleanup.load(Ordering::SeqCst) > 0 {
            return Err(BotHostError::Invalid(
                "application cleanup failed, panicked or exceeded its deadline".into(),
            ));
        }
        Ok(())
    }
}
struct ProcessorOwner(Arc<ProcessorOwners>);
impl Drop for ProcessorOwner {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_waiters();
    }
}

struct CleanupOutcome {
    owners: Arc<ProcessorOwners>,
    success: bool,
}
impl Drop for CleanupOutcome {
    fn drop(&mut self) {
        if !self.success {
            self.owners.failed_cleanup.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn undrained_observer_queue_reports_exact_overflow_and_releases_owner() {
        let config = EvalBotHostConfig {
            event_capacity: 1,
            ..Default::default()
        };
        let budget = Arc::new(Budget {
            counts: Mutex::new((0, 0)),
            output_open: AtomicBool::new(true),
            config: config.clone(),
            context: SharedContext::default(),
            #[cfg(feature = "user-images")]
            images: None,
        });
        let observer = Arc::new(RtviObserver::new(RtviObserverParams::default(), None).unwrap());
        let owners = Arc::new(ProcessorOwners::default());
        let (tx, receiver) = mpsc::channel(1);
        let boundary = Arc::new(Boundary {
            inner: Arc::new(IdentityProcessor),
            budget,
            observer: Some((observer, tx)),
            capture: None,
            _owner: owners.track(),
        });
        let mut worker = Pipeline::new(vec![boundary])
            .start(config.pipeline)
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmResponseStart),
                Direction::Downstream,
            )
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmText(TextFrame::new("second event"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        // Deliberately retain the event receiver without polling it.
        tokio::time::timeout(Duration::from_secs(2), async {
            while worker.recv().await.is_some() {}
        })
        .await
        .unwrap();
        assert!(worker
            .errors()
            .iter()
            .any(|error| error.error.contains("eval observer queue full")));
        assert!(worker.join().await.is_err());
        drop(worker);
        drop(receiver);
        owners.wait_empty(Duration::from_secs(1)).await.unwrap();
    }
    #[cfg(feature = "user-images")]
    #[tokio::test]
    async fn requested_rgb_has_independent_native_budget_without_expanding_wire_json() {
        let images = Arc::new(UserImageInput::new(ImageInputLimits::default()).unwrap());
        let mut encoded = Vec::new();
        {
            let mut png = png::Encoder::new(&mut encoded, 512, 512);
            png.set_color(png::ColorType::Rgb);
            png.set_depth(png::BitDepth::Eight);
            png.write_header()
                .unwrap()
                .write_image_data(&[255u8, 0, 17].repeat(512 * 512))
                .unwrap();
        }
        images
            .register(encoded.into_boxed_slice().into_vec(), "image/png")
            .unwrap();
        let mut worker = Pipeline::new(vec![images.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
                    user_id: "user".into(),
                    text: Some("what is shown".into()),
                    append_to_context: Some(true),
                    video_source: None,
                    function_name: None,
                    tool_call_id: None,
                    result_callback: None,
                })),
                Direction::Upstream,
            )
            .await
            .unwrap();
        let frame = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let output = worker.recv().await.unwrap();
                if matches!(output.frame.data, FrameData::UserImage(_)) {
                    break output.frame;
                }
            }
        })
        .await
        .unwrap();
        let config = EvalBotHostConfig::default();
        assert!(measure(&frame.data, config.transport.max_message_bytes).is_err());
        let budget = Budget {
            output_open: AtomicBool::new(true),
            counts: Mutex::new((0, 0)),
            config: config.clone(),
            context: SharedContext::default(),
            images: Some(images.clone()),
        };
        budget.admit(&frame).unwrap();
        assert!(budget.counts.lock().unwrap().1 >= 512 * 512 * 3);
        let oversized_json = Frame::new(FrameData::LlmText(TextFrame::new(
            "x".repeat(config.transport.max_message_bytes),
        )));
        assert!(budget.admit(&oversized_json).is_err());
        let strict = Budget {
            output_open: AtomicBool::new(true),
            counts: Mutex::new((0, 0)),
            config: EvalBotHostConfig {
                max_processed_bytes: 512 * 512 * 3 - 1,
                ..config
            },
            context: SharedContext::default(),
            images: Some(images.clone()),
        };
        assert!(strict.admit(&frame).is_err());
        drop(frame);
        worker.cancel("test complete").await.unwrap();
        while worker.recv().await.is_some() {}
        let _ = worker.join().await;
        images.shutdown_joined().await.unwrap();
    }
}
