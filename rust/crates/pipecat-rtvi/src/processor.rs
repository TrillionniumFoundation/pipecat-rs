use crate::{bounded_size, check_json_depth, error, transport_frame, wire::*};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_core::*;
use serde_json::{Map, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{broadcast, mpsc};

#[derive(Debug, Clone)]
pub struct RtviConfig {
    pub command_capacity: usize,
    pub event_capacity: usize,
    pub max_message_bytes: usize,
    pub max_audio_batch: usize,
    pub max_dtmf_buttons: usize,
    pub flush_timeout: Duration,
    /// Explicit session capabilities; omitted when the host has not provided them.
    pub capabilities: Option<Map<String, Value>>,
}
impl Default for RtviConfig {
    fn default() -> Self {
        Self {
            command_capacity: 64,
            event_capacity: 64,
            max_message_bytes: 1024 * 1024,
            max_audio_batch: 128,
            max_dtmf_buttons: 128,
            flush_timeout: Duration::from_secs(5),
            capabilities: None,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RtviSessionState {
    pub client_ready: bool,
    pub bot_ready: bool,
    pub client_ready_id: Option<String>,
    pub client_version: Option<String>,
    pub about_client: Option<AboutClient>,
    pub legacy: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub enum RtviEvent {
    BotStarted,
    ClientReady(RtviSessionState),
    ClientMessage(RtviClientMessageFrame),
    UiMessage(Envelope),
    Closed,
}
#[derive(Debug, thiserror::Error)]
pub enum RtviError {
    #[error("RTVI processor is not running")]
    Closed,
    #[error("RTVI command queue is full")]
    Full,
    #[error("invalid RTVI message: {0}")]
    Invalid(String),
}
enum Work {
    Message(Envelope),
    BotReady(Option<Map<String, Value>>),
    Server(Envelope),
}
struct Shared {
    tx: mpsc::Sender<Work>,
    running: AtomicBool,
    state: Mutex<RtviSessionState>,
    events: broadcast::Sender<RtviEvent>,
    config: RtviConfig,
    skip_tts: AtomicBool,
}
/// Application control is a bounded queue, never an untracked task or callback.
#[derive(Clone)]
pub struct RtviHandle {
    shared: Arc<Shared>,
}
impl RtviHandle {
    pub fn state(&self) -> RtviSessionState {
        self.shared.state.lock().unwrap().clone()
    }
    /// Receivers explicitly see `Lagged` when they cannot keep up. Frame delivery
    /// remains independent of optional application event subscribers.
    pub fn subscribe(&self) -> broadcast::Receiver<RtviEvent> {
        self.shared.events.subscribe()
    }
    fn enqueue(&self, work: Work) -> Result<(), RtviError> {
        if !self.shared.running.load(Ordering::Acquire) {
            return Err(RtviError::Closed);
        }
        self.shared.tx.try_send(work).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => RtviError::Full,
            mpsc::error::TrySendError::Closed(_) => RtviError::Closed,
        })
    }
    pub fn handle_message(&self, message: Envelope) -> Result<(), RtviError> {
        if let Some(data) = &message.data {
            check_json_depth(data).map_err(|e| RtviError::Invalid(e.to_string()))?;
        }

        bounded_size(&message, self.shared.config.max_message_bytes)
            .map_err(|e| RtviError::Invalid(e.to_string()))?;
        message
            .validate()
            .map_err(|e| RtviError::Invalid(e.to_string()))?;
        if message.id.is_none() {
            return Err(RtviError::Invalid("RTVI id is required".into()));
        }
        if message.id.as_ref().is_some_and(|id| id.len() > 512) || message.message_type.len() > 128
        {
            return Err(RtviError::Invalid(
                "RTVI ID/type length limit exceeded".into(),
            ));
        }
        let rejection = Envelope::response(
            message.id.as_deref().unwrap_or_default(),
            &ServerEvent::ErrorResponse(ErrorResponseData {
                error: "Invalid request".into(),
            }),
        )
        .map_err(|e| RtviError::Invalid(e.to_string()))?;
        bounded_size(&rejection, self.shared.config.max_message_bytes).map_err(|_| {
            RtviError::Invalid("RTVI correlation ID leaves no room for a response".into())
        })?;
        if message
            .data
            .as_ref()
            .is_some_and(|data| !data.is_object() && !data.is_null())
        {
            return Err(RtviError::Invalid(
                "RTVI client data must be an object or null".into(),
            ));
        }
        self.enqueue(Work::Message(message))
    }
    pub fn handle_bytes(&self, bytes: &[u8]) -> Result<(), RtviError> {
        let envelope = Envelope::parse_with_limit(bytes, self.shared.config.max_message_bytes)
            .map_err(|e| RtviError::Invalid(e.to_string()))?;
        self.handle_message(envelope)
    }
    /// If client-ready has not arrived, defer emission until its correlation ID exists.
    pub fn set_bot_ready(&self, about: Option<Map<String, Value>>) -> Result<(), RtviError> {
        bounded_size(&about, self.shared.config.max_message_bytes / 2)
            .map_err(|e| RtviError::Invalid(e.to_string()))?;
        bot_ready_envelope(&self.shared, &self.state(), &about)
            .map_err(|e| RtviError::Invalid(e.to_string()))?;
        self.enqueue(Work::BotReady(about))
    }
    pub fn send_server_message(&self, data: Value) -> Result<(), RtviError> {
        self.send_event(None, ServerEvent::ServerMessage(data))
    }
    pub fn send_server_response(
        &self,
        request: &RtviClientMessageFrame,
        data: Value,
    ) -> Result<(), RtviError> {
        self.send_event(
            Some(&request.msg_id),
            ServerEvent::ServerResponse(CustomMessageData {
                t: request.message_type.clone(),
                d: Some(data),
            }),
        )
    }
    pub fn send_error_response(
        &self,
        id: &str,
        message: impl Into<String>,
    ) -> Result<(), RtviError> {
        self.send_event(
            Some(id),
            ServerEvent::ErrorResponse(ErrorResponseData {
                error: message.into(),
            }),
        )
    }
    pub fn send_event(&self, id: Option<&str>, event: ServerEvent) -> Result<(), RtviError> {
        let envelope = match id {
            Some(id) => Envelope::response(id, &event),
            None => Envelope::server(&event),
        }
        .map_err(|e| RtviError::Invalid(e.to_string()))?;
        bounded_size(&envelope, self.shared.config.max_message_bytes)
            .map_err(|e| RtviError::Invalid(e.to_string()))?;
        self.enqueue(Work::Server(envelope))
    }
}
pub struct RtviProcessor {
    handle: RtviHandle,
    receiver: Mutex<Option<mpsc::Receiver<Work>>>,
    task: Mutex<Option<u64>>,
}
impl RtviProcessor {
    pub fn new(config: RtviConfig) -> Result<Self, RtviError> {
        if config.command_capacity == 0
            || config.event_capacity == 0
            || config.max_message_bytes < 1024
            || config.max_audio_batch == 0
            || config.max_dtmf_buttons == 0
            || config.flush_timeout.is_zero()
        {
            return Err(RtviError::Invalid(
                "capacities and flush timeout must be positive; message limit must be >=1024"
                    .into(),
            ));
        }
        bounded_size(&config.capabilities, config.max_message_bytes / 2)
            .map_err(|e| RtviError::Invalid(e.to_string()))?;
        let (tx, rx) = mpsc::channel(config.command_capacity);
        let (events, _) = broadcast::channel(config.event_capacity);
        Ok(Self {
            handle: RtviHandle {
                shared: Arc::new(Shared {
                    tx,
                    events,
                    config,
                    running: AtomicBool::new(false),
                    state: Mutex::new(RtviSessionState::default()),
                    skip_tts: AtomicBool::new(false),
                }),
            },
            receiver: Mutex::new(Some(rx)),
            task: Mutex::new(None),
        })
    }
    pub fn handle(&self) -> RtviHandle {
        self.handle.clone()
    }
    fn stop(&self, ctx: &ProcessorContext) {
        self.handle.shared.running.store(false, Ordering::Release);
        if let Some(task) = self.task.lock().unwrap().take() {
            ctx.cancel_task(task);
        }
    }
}
#[async_trait]
impl FrameProcessor for RtviProcessor {
    fn name(&self) -> &str {
        "RTVIProcessor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() {
            ctx.push(frame, direction).await?;
            let mut rx = self
                .receiver
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| error("RTVI processor cannot be restarted"))?;
            let shared = self.handle.shared.clone();
            let worker_ctx = ctx.clone();
            let task = ctx.spawn("rtvi-messages", false, async move {
                let mut about = None;
                while let Some(work) = rx.recv().await {
                    match work {
                        Work::Message(message) => {
                            handle_command(&shared, message, &worker_ctx, &about).await?
                        }
                        Work::BotReady(value) => {
                            // Readiness requests queued earlier can change the final
                            // envelope size before this configuration is applied.
                            let state = shared.state.lock().unwrap().clone();
                            if bot_ready_envelope(&shared, &state, &value).is_err() {
                                let message = "Invalid bot-ready configuration: response exceeds message size limit";
                                if let Some(id) = state.client_ready_id.as_deref() {
                                    reject(&shared, &worker_ctx, id, message.into()).await?;
                                } else {
                                    send_event(
                                        &shared,
                                        &worker_ctx,
                                        None,
                                        ServerEvent::Error(ErrorData {
                                            error: message.into(),
                                            fatal: false,
                                        }),
                                    )
                                    .await?;
                                }
                                continue;
                            }
                            about = value;
                            shared.state.lock().unwrap().bot_ready = true;
                            send_bot_ready(&shared, &worker_ctx, &about).await?;
                        }
                        Work::Server(envelope) => send(&shared, &worker_ctx, envelope).await?,
                    }
                }
                Ok(())
            })?;
            *self.task.lock().unwrap() = Some(task);
            self.handle.shared.running.store(true, Ordering::Release);
            let _ = self.handle.shared.events.send(RtviEvent::BotStarted);
            return Ok(());
        }
        if frame.is_terminal() {
            self.stop(ctx);
        }
        if let Some(err) = frame.data.as_error_frame() {
            send_event(
                &self.handle.shared,
                ctx,
                None,
                ServerEvent::Error(ErrorData {
                    error: err.error,
                    fatal: err.fatal,
                }),
            )
            .await?;
        }
        if let FrameData::LlmConfigureOutput { skip_tts } = &frame.data {
            self.handle
                .shared
                .skip_tts
                .store(*skip_tts, Ordering::Release);
        }
        if let FrameData::InputTransportMessage(value) = &frame.data {
            if value.get("label").and_then(Value::as_str) != Some(MESSAGE_LABEL) {
                return Ok(());
            }
            if check_json_depth(value)
                .and_then(|()| bounded_size(value, self.handle.shared.config.max_message_bytes))
                .is_err()
            {
                return send_event(
                    &self.handle.shared,
                    ctx,
                    None,
                    ServerEvent::Error(ErrorData {
                        error: "Invalid RTVI transport message: payload too large or deeply nested"
                            .into(),
                        fatal: false,
                    }),
                )
                .await;
            }
            let result = serde_json::from_value::<Envelope>(value.clone())
                .map_err(|e| RtviError::Invalid(e.to_string()))
                .and_then(|message| self.handle.handle_message(message));
            if let Err(e) = result {
                return send_event(
                    &self.handle.shared,
                    ctx,
                    None,
                    ServerEvent::Error(ErrorData {
                        error: format!("Invalid RTVI transport message: {e}"),
                        fatal: false,
                    }),
                )
                .await;
            }
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.stop(ctx);
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.handle.shared.running.store(false, Ordering::Release);
        let _ = self.handle.shared.events.send(RtviEvent::Closed);
        Ok(())
    }
}
async fn send(
    shared: &Shared,
    ctx: &ProcessorContext,
    envelope: Envelope,
) -> Result<(), ProcessorError> {
    ctx.push(
        transport_frame(envelope, shared.config.max_message_bytes)?,
        Direction::Downstream,
    )
    .await
}
async fn send_event(
    shared: &Shared,
    ctx: &ProcessorContext,
    id: Option<&str>,
    event: ServerEvent,
) -> Result<(), ProcessorError> {
    let envelope = match id {
        Some(id) => Envelope::response(id, &event),
        None => Envelope::server(&event),
    }
    .map_err(error)?;
    send(shared, ctx, envelope).await
}
async fn reject(
    shared: &Shared,
    ctx: &ProcessorContext,
    id: &str,
    message: String,
) -> Result<(), ProcessorError> {
    let mut envelope = Envelope::response(
        id,
        &ServerEvent::ErrorResponse(ErrorResponseData { error: message }),
    )
    .map_err(error)?;
    if bounded_size(&envelope, shared.config.max_message_bytes).is_err() {
        // Admission reserves room for this diagnostic even when JSON escaping
        // makes the correlation ID much larger than its UTF-8 representation.
        envelope = Envelope::response(
            id,
            &ServerEvent::ErrorResponse(ErrorResponseData {
                error: "Invalid request".into(),
            }),
        )
        .map_err(error)?;
    }
    send(shared, ctx, envelope).await
}
async fn send_bot_ready(
    shared: &Shared,
    ctx: &ProcessorContext,
    about: &Option<Map<String, Value>>,
) -> Result<(), ProcessorError> {
    let state = shared.state.lock().unwrap().clone();
    if !state.bot_ready || !state.client_ready {
        return Ok(());
    }
    send(shared, ctx, bot_ready_envelope(shared, &state, about)?).await
}
fn bot_ready_envelope(
    shared: &Shared,
    state: &RtviSessionState,
    about: &Option<Map<String, Value>>,
) -> Result<Envelope, ProcessorError> {
    let version = negotiate_version(state.client_version.as_deref());
    let about = about.clone().or_else(|| {
        Some(Map::from_iter([
            ("library".into(), Value::String("pipecat-rs".into())),
            (
                "library_version".into(),
                Value::String(env!("CARGO_PKG_VERSION").into()),
            ),
        ]))
    });
    let envelope = Envelope::response(
        state.client_ready_id.as_deref().unwrap_or_default(),
        &ServerEvent::BotReady(BotReadyData {
            version: version.advertised,
            about,
            capabilities: shared.config.capabilities.clone(),
        }),
    )
    .map_err(error)?;
    bounded_size(&envelope, shared.config.max_message_bytes)?;
    Ok(envelope)
}
async fn push(ctx: &ProcessorContext, data: FrameData) -> Result<(), ProcessorError> {
    ctx.push(Frame::new(data), Direction::Downstream).await
}
async fn append_input(
    shared: &Shared,
    ctx: &ProcessorContext,
    data: FrameData,
    run: bool,
    audio: bool,
    flush: bool,
) -> Result<(), ProcessorError> {
    if run {
        ctx.broadcast_interruption().await?;
        if flush {
            let event = LocalFlushEvent::default();
            push(
                ctx,
                FrameData::FlushDetailed(PipelineFlushFrame {
                    event: Some(event.clone()),
                    returning: false,
                    origin: Some(ctx.name().into()),
                }),
            )
            .await?;
            tokio::time::timeout(shared.config.flush_timeout, event.wait())
                .await
                .map_err(|_| error("RTVI interruption flush timed out"))?;
        }
    }
    let previous = shared.skip_tts.load(Ordering::Acquire);
    let toggle = previous == audio;
    if toggle {
        push(ctx, FrameData::LlmConfigureOutput { skip_tts: !audio }).await?;
    }
    push(ctx, data).await?;
    if toggle {
        push(ctx, FrameData::LlmConfigureOutput { skip_tts: previous }).await?;
    }
    Ok(())
}
async fn handle_command(
    shared: &Shared,
    mut envelope: Envelope,
    ctx: &ProcessorContext,
    about: &Option<Map<String, Value>>,
) -> Result<(), ProcessorError> {
    let id = envelope.id.clone().unwrap_or_default();
    let original_data_url = if envelope.message_type == "send-file" {
        envelope
            .data
            .as_ref()
            .and_then(|data| data.pointer("/file/source/bytes"))
            .and_then(Value::as_str)
            .filter(|bytes| bytes.starts_with("data:"))
            .map(str::to_owned)
    } else {
        None
    };

    // Source send-file tolerates data URLs. The generic wire validator accepts
    // raw base64; normalize only the bytes field before using that validator.
    if envelope.message_type == "send-file" {
        if let Some(Value::String(bytes)) = envelope
            .data
            .as_mut()
            .and_then(|d| d.pointer_mut("/file/source/bytes"))
        {
            if bytes.starts_with("data:") {
                if let Some((header, body)) = bytes.split_once(',') {
                    if header.ends_with(";base64") {
                        *bytes = body.to_owned();
                    }
                }
            }
        }
    }
    let supported = [
        "client-ready",
        "disconnect-bot",
        "client-message",
        "send-text",
        "send-file",
        "raw-audio",
        "raw-audio-batch",
        "dtmf",
        "llm-function-call-result",
        "ui-event",
        "ui-snapshot",
        "ui-cancel-job-group",
    ];
    if !supported.contains(&envelope.message_type.as_str()) {
        return reject(
            shared,
            ctx,
            &id,
            format!("Unsupported type {}", envelope.message_type),
        )
        .await;
    }
    let command = match envelope.client_command() {
        Ok(command) => command,
        Err(_) => {
            return reject(
                shared,
                ctx,
                &id,
                "Invalid message: payload validation failed".into(),
            )
            .await
        }
    };
    let result: Result<(), ProcessorError> = async {
        match command {
            ClientCommand::ClientReady(data) => {
                let version = negotiate_version(data.version.as_deref());
                let state = {
                    let mut state = shared.state.lock().unwrap().clone();
                    state.client_ready = true;
                    state.client_ready_id = Some(id.clone());
                    if let Some(parsed) = data.version.as_deref().and_then(|value| {
                        let parts = value
                            .split('.')
                            .map(str::parse::<u32>)
                            .collect::<Result<Vec<_>, _>>()
                            .ok()?;
                        (parts.len() == 3)
                            .then(|| format!("{}.{}.{}", parts[0], parts[1], parts[2]))
                    }) {
                        state.client_version = Some(parsed);
                    }
                    state.about_client = data.about;
                    state.legacy = negotiate_version(state.client_version.as_deref()).legacy;
                    state
                };
                bot_ready_envelope(shared, &state, about)?;
                if let Some(warning) = version.warning {
                    reject(shared, ctx, &id, warning).await?;
                }
                push(ctx, FrameData::InputTransportStartAudioStreaming).await?;
                *shared.state.lock().unwrap() = state.clone();
                let _ = shared.events.send(RtviEvent::ClientReady(state));
                send_bot_ready(shared, ctx, about).await?;
            }
            ClientCommand::DisconnectBot => {
                push(ctx, FrameData::EndWorker(ReasonFrame::default())).await?
            }
            ClientCommand::ClientMessage(data) => {
                let request = RtviClientMessageFrame {
                    msg_id: id.clone(),
                    message_type: data.t,
                    data: data.d,
                };
                push(ctx, FrameData::RtviClientMessage(request.clone())).await?;
                let _ = shared.events.send(RtviEvent::ClientMessage(request));
            }
            ClientCommand::SendText(data) => {
                let options = data.options.unwrap_or_default();
                append_input(
                    shared,
                    ctx,
                    FrameData::LlmMessagesAppend(MessagesFrame {
                        messages: vec![ChatMessage::new(ChatRole::User, data.content)],
                        run_llm: options.run_immediately,
                    }),
                    options.run_immediately,
                    options.audio_response,
                    true,
                )
                .await?;
            }
            ClientCommand::SendFile(data) => {
                let (run, audio, custom_options) = data
                    .options
                    .map(|o| (o.run_immediately, o.audio_response, o.custom_options))
                    .unwrap_or((true, true, None));
                let (source, kind, image) = match data.file.source {
                    FileSource::Bytes {
                        bytes,
                        width,
                        height,
                    } => {
                        let image = if data.file.format.starts_with("image/") {
                            Some(ImageRawFrame {
                                image: STANDARD.decode(&bytes).map_err(error)?,
                                size: (width.unwrap_or(0), height.unwrap_or(0)),
                                format: Some(data.file.format.clone()),
                            })
                        } else {
                            None
                        };
                        (
                            original_data_url.unwrap_or_else(|| {
                                format!("data:{};base64,{bytes}", data.file.format)
                            }),
                            FileSourceType::Bytes,
                            image,
                        )
                    }
                    FileSource::Url { url } => (url, FileSourceType::Url, None),
                };
                let frame = if let Some(image) = image {
                    FrameData::UserImage(UserImageRawFrame {
                        image,
                        user_id: String::new(),
                        text: Some(data.content),
                        append_to_context: Some(true),
                        request: None,
                        run_llm: Some(run),
                    })
                } else {
                    FrameData::UserFileDetailed(DetailedUserFileRawFrame {
                        file: FileRawFrame {
                            file: source,
                            source_type: kind,
                            filename: data.file.name,
                            format: data.file.format,
                        },
                        user_id: String::new(),
                        text: data.content,
                        append_to_context: Some(true),
                        custom_options: custom_options.map(|m| m.into_iter().collect()),
                        run_llm: Some(run),
                    })
                };
                append_input(shared, ctx, frame, run, audio, false).await?;
            }
            ClientCommand::RawAudio(data) => {
                push(
                    ctx,
                    FrameData::InputAudio(
                        AudioRawFrame::new(
                            data.decode().map_err(error)?,
                            data.sample_rate,
                            data.num_channels,
                        )
                        .map_err(error)?,
                    ),
                )
                .await?
            }
            ClientCommand::RawAudioBatch(data) => {
                if data.base64_audio_batch.len() > shared.config.max_audio_batch {
                    return Err(error("audio batch limit exceeded"));
                }
                for bytes in data.decode().map_err(error)? {
                    push(
                        ctx,
                        FrameData::InputAudio(
                            AudioRawFrame::new(bytes, data.sample_rate, data.num_channels)
                                .map_err(error)?,
                        ),
                    )
                    .await?;
                }
            }
            ClientCommand::Dtmf(data) => {
                if data.buttons.len() > shared.config.max_dtmf_buttons {
                    return Err(error("DTMF sequence limit exceeded"));
                }
                for digit in data.buttons {
                    push(ctx, FrameData::InputDtmf(digit.into())).await?;
                }
            }
            ClientCommand::LlmFunctionCallResult(data) => {
                if !data.result.is_object() && !data.result.is_string() {
                    return Err(error("function result must be object or string"));
                }
                push(
                    ctx,
                    FrameData::FunctionCallResult(FunctionCallResultFrame {
                        function_name: data.function_name,
                        tool_call_id: data.tool_call_id,
                        arguments: Value::Object(data.arguments),
                        result: data.result,
                        run_llm: None,
                        is_final: true,
                        error: None,
                    }),
                )
                .await?;
            }
            ClientCommand::UiEvent(data) => {
                push(
                    ctx,
                    FrameData::RtviUiEvent(RtviUiEventFrame {
                        msg_id: id.clone(),
                        event: data.event,
                        payload: data.payload,
                    }),
                )
                .await?;
                let _ = shared.events.send(RtviEvent::UiMessage(envelope));
            }
            ClientCommand::UiSnapshot(data) => {
                push(
                    ctx,
                    FrameData::RtviUiSnapshot(RtviUiSnapshotFrame {
                        msg_id: id.clone(),
                        tree: serde_json::to_value(data.tree).map_err(error)?,
                    }),
                )
                .await?;
                let _ = shared.events.send(RtviEvent::UiMessage(envelope));
            }
            ClientCommand::UiCancelJobGroup(data) => {
                push(
                    ctx,
                    FrameData::RtviUiCancelJobGroup(RtviUiCancelJobGroupFrame {
                        msg_id: id.clone(),
                        job_id: data.job_id,
                        reason: data.reason,
                    }),
                )
                .await?;
                let _ = shared.events.send(RtviEvent::UiMessage(envelope));
            }
        }
        Ok(())
    }
    .await;
    if let Err(e) = result {
        reject(
            shared,
            ctx,
            &id,
            format!("Exception processing message: {e}"),
        )
        .await?;
    }
    Ok(())
}
