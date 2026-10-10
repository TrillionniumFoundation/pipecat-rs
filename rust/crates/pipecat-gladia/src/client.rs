// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    protocol::{Accounting, Protocol},
    security, CancellationToken, GladiaError, GladiaSttConfig, GladiaSttEvent, GladiaSttSettings,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use serde_json::{json, Map, Value};
use std::{
    fmt, io,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{timeout, Instant},
};
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};
type Socket = WebSocketStream<MaybeTlsStream<HandshakeBoundedStream>>;
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
#[derive(Debug, Clone)]
pub struct GladiaSttClient {
    config: GladiaSttConfig,
}
impl GladiaSttClient {
    pub fn new(config: GladiaSttConfig) -> Result<Self, GladiaError> {
        config.validate()?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &GladiaSttConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<GladiaSttSession, GladiaError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<GladiaSttSession, GladiaError> {
        let (mut socket, id) = tokio::select! {biased;_=cancel.cancelled()=>return Err(GladiaError::Cancelled),result=timeout(self.config.limits.connect_timeout,connect(&self.config))=>result.map_err(|_|GladiaError::Timeout("session setup"))??};
        let generation = NEXT_GENERATION
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| GladiaError::TooLarge)?;
        let (tx, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let (event_tx, event_rx) = mpsc::channel(self.config.limits.queue_capacity);
        let (completion_tx, completion) = watch::channel(None);
        let finish = CancellationToken::new();
        let finished = Arc::new(Mutex::new(false));
        let config = self.config.clone();
        let actor_cancel = cancel.clone();
        let actor_finish = finish.clone();
        let actor_id = id.clone();
        let task = tokio::spawn(async move {
            let result = tokio::select! {biased;_=actor_cancel.cancelled()=>Err(GladiaError::Cancelled),result=run(&mut socket,rx,&event_tx,&config,generation,actor_id,&actor_finish)=>result};
            // Closing is owned by this task and attempted exactly once.
            let close = timeout(config.limits.close_timeout, socket.close(None)).await;
            let result = match (result, close) {
                (Ok(()), Err(_)) => Err(GladiaError::Timeout("close")),
                (other, _) => other,
            };
            completion_tx.send_replace(Some(result.clone()));
            result
        });
        let sender = GladiaSttSender {
            tx,
            cancel: cancel.clone(),
            finish,
            finished,
            completion,
            budget: Arc::new(Semaphore::new(self.config.limits.max_buffered_audio_bytes)),
            config: Arc::new(Mutex::new(self.config.clone())),
            active_config: Arc::new(self.config.clone()),
        };
        Ok(GladiaSttSession {
            sender,
            events: event_rx,
            cancel,
            task,
            ended: false,
            id,
            generation,
        })
    }
}
async fn connect(config: &GladiaSttConfig) -> Result<(Socket, String), GladiaError> {
    let url = security::init_url(config)?;
    let addresses = security::addresses(&url, config.allow_loopback).await?;
    let http = reqwest::Client::builder()
        .no_proxy()
        .retry(reqwest::retry::never())
        .redirect(reqwest::redirect::Policy::none())
        .timeout(config.limits.connect_timeout)
        .connect_timeout(config.limits.connect_timeout)
        .resolve_to_addrs(url.host_str().ok_or(GladiaError::Endpoint)?, &addresses)
        .build()
        .map_err(|_| GladiaError::Transport)?;
    let mut key = reqwest::header::HeaderValue::from_str(&config.api_key)
        .map_err(|_| GladiaError::Invalid("API key"))?;
    key.set_sensitive(true);
    let mut response = http
        .post(url)
        .header("X-Gladia-Key", key)
        .json(&config.session_request()?)
        .send()
        .await
        .map_err(|_| GladiaError::Transport)?;
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|n| n > config.limits.max_http_body_bytes as u64)
    {
        return Err(GladiaError::TooLarge);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| GladiaError::Transport)? {
        if bytes.len().saturating_add(chunk.len()) > config.limits.max_http_body_bytes {
            return Err(GladiaError::TooLarge);
        }
        bytes.extend_from_slice(&chunk)
    }
    if !status.is_success() {
        return Err(GladiaError::Http(status.as_u16()));
    }
    let response: Value =
        serde_json::from_slice(&bytes).map_err(|_| GladiaError::Protocol("session response"))?;
    let id = response
        .get("id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control))
        .ok_or(GladiaError::Protocol("session identity"))?
        .to_owned();
    let url = security::session_url(
        response
            .get("url")
            .and_then(Value::as_str)
            .ok_or(GladiaError::Protocol("session endpoint"))?,
        config,
    )?;
    let addresses = security::addresses(&url, config.allow_loopback).await?;
    let stream = TcpStream::connect(addresses.as_slice())
        .await
        .map_err(|_| GladiaError::Transport)?;
    stream
        .set_nodelay(true)
        .map_err(|_| GladiaError::Transport)?;
    let handshaking = Arc::new(AtomicBool::new(true));
    let stream = HandshakeBoundedStream {
        inner: stream,
        handshaking: handshaking.clone(),
        read: 0,
    };
    let request = url
        .as_str()
        .into_client_request()
        .map_err(|_| GladiaError::Endpoint)?;
    // Only standard WebSocket headers. Never forward the REST API key or cookies.
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(config.limits.max_message_bytes))
        .max_frame_size(Some(config.limits.max_message_bytes))
        .write_buffer_size(0)
        .max_write_buffer_size(config.limits.max_message_bytes + 1024);
    // client_async_tls does not follow HTTP redirects and never consults proxies.
    let (socket, _) =
        tokio_tungstenite::client_async_tls_with_config(request, stream, Some(ws_config), None)
            .await
            .map_err(transport)?;
    handshaking.store(false, Ordering::Release);
    Ok((socket, id))
}
struct Audio {
    frame: AudioRawFrame,
    permit: OwnedSemaphorePermit,
}
#[derive(Clone)]
pub struct GladiaSttSender {
    tx: mpsc::Sender<Audio>,
    cancel: CancellationToken,
    finish: CancellationToken,
    finished: Arc<Mutex<bool>>,
    completion: watch::Receiver<Option<Result<(), GladiaError>>>,
    budget: Arc<Semaphore>,
    config: Arc<Mutex<GladiaSttConfig>>,
    active_config: Arc<GladiaSttConfig>,
}
impl fmt::Debug for GladiaSttSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GladiaSttSender")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}
impl GladiaSttSender {
    pub fn validate_audio(&self, audio: &AudioRawFrame) -> Result<(), GladiaError> {
        let c = &self.active_config;
        if audio.sample_rate != c.sample_rate
            || audio.num_channels != c.channels
            || audio.audio.is_empty()
        {
            return Err(GladiaError::Invalid("PCM format"));
        }
        if !audio
            .audio
            .len()
            .is_multiple_of(usize::from(c.channels) * 2)
        {
            return Err(GladiaError::PartialSample);
        }
        if audio.audio.len() > c.limits.max_audio_chunk_bytes {
            return Err(GladiaError::TooLarge);
        }
        Ok(())
    }
    /// Immediate bounded admission; success means queued, not server acknowledged.
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<(), GladiaError> {
        self.validate_audio(&audio)?;
        let finished = self.finished.lock().map_err(|_| GladiaError::Closed)?;
        if *finished || self.cancel.is_cancelled() {
            return Err(GladiaError::Closed);
        }
        let permit = self
            .budget
            .clone()
            .try_acquire_many_owned(audio.audio.len() as u32)
            .map_err(|_| GladiaError::Backpressure)?;
        self.tx
            .try_send(Audio {
                frame: audio,
                permit,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => GladiaError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => GladiaError::Closed,
            })
    }
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<(), GladiaError> {
        self.try_send_audio(audio)
    }
    /// Stored atomically for a future session only. Active wire and VAD snapshots
    /// remain unchanged. No reconnect or replay occurs.
    pub fn merge_settings(&self, delta: &Map<String, Value>) -> Result<(), GladiaError> {
        if self.is_closed() {
            return Err(GladiaError::Closed);
        }
        let mut config = self.config.lock().map_err(|_| GladiaError::Closed)?;
        let mut candidate = config.clone();
        candidate.settings = config.settings.merge(delta)?;
        candidate.validate()?;
        *config = candidate;
        Ok(())
    }
    pub fn config_snapshot(&self) -> Result<GladiaSttConfig, GladiaError> {
        self.config
            .lock()
            .map(|c| c.clone())
            .map_err(|_| GladiaError::Closed)
    }
    pub fn active_settings(&self) -> &GladiaSttSettings {
        &self.active_config.settings
    }
    /// Drain admitted PCM then send stop_recording and close, without waiting for
    /// trailing transcripts. Returns after the owned writer and close attempt end.
    pub async fn finish(&self) -> Result<(), GladiaError> {
        {
            let mut finished = self.finished.lock().map_err(|_| GladiaError::Closed)?;
            *finished = true;
            self.finish.cancel();
        }
        let mut completion = self.completion.clone();
        let result = timeout(
            self.active_config.limits.finish_timeout + self.active_config.limits.close_timeout,
            async {
                loop {
                    if let Some(result) = completion.borrow_and_update().clone() {
                        return result;
                    }
                    completion
                        .changed()
                        .await
                        .map_err(|_| GladiaError::Closed)?;
                }
            },
        )
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                self.cancel();
                Err(GladiaError::Timeout("finish"))
            }
        }
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub fn is_closed(&self) -> bool {
        self.cancel.is_cancelled()
            || self.tx.is_closed()
            || self.finished.lock().map_or(true, |v| *v)
    }
}
pub struct GladiaSttSession {
    sender: GladiaSttSender,
    events: mpsc::Receiver<GladiaSttEvent>,
    cancel: CancellationToken,
    task: JoinHandle<Result<(), GladiaError>>,
    ended: bool,
    id: String,
    generation: u64,
}
impl GladiaSttSession {
    pub fn sender(&self) -> GladiaSttSender {
        self.sender.clone()
    }
    pub fn session_id(&self) -> &str {
        &self.id
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub async fn next(&mut self) -> Option<Result<GladiaSttEvent, GladiaError>> {
        if self.ended {
            return None;
        }
        if self.cancel.is_cancelled() {
            self.events.close();
            while self.events.try_recv().is_ok() {}
        } else {
            tokio::select! {biased;_=self.cancel.cancelled()=>{self.events.close();while self.events.try_recv().is_ok(){}},event=self.events.recv()=>{if let Some(event)=event{return Some(Ok(event))}}}
        }
        let result = (&mut self.task).await;
        self.ended = true;
        match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => Some(Err(GladiaError::Transport)),
        }
    }
}
impl Drop for GladiaSttSession {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
async fn run(
    socket: &mut Socket,
    mut input: mpsc::Receiver<Audio>,
    events: &mpsc::Sender<GladiaSttEvent>,
    config: &GladiaSttConfig,
    generation: u64,
    id: String,
    finish: &CancellationToken,
) -> Result<(), GladiaError> {
    let mut protocol = Protocol {
        accounting: Accounting::new(generation, config.channels),
        speaking: false,
        usage_bytes: 0,
        session_id: id,
        vad: config.settings.enable_vad,
        bytes_per_second: f64::from(config.sample_rate) * f64::from(config.channels) * 2.0,
    };
    let mut last_write = Instant::now();
    let mut last_read = Instant::now();
    loop {
        tokio::select! {
            _=finish.cancelled()=>{
                return timeout(config.limits.finish_timeout,async {
                    input.close();while let Some(audio)=input.recv().await{write_audio(socket,audio,&mut protocol,config).await?;}
                    write(socket,Message::Text(json!({"type":"stop_recording"}).to_string().into()),config).await
                }).await.map_err(|_|GladiaError::Timeout("finish drain"))?;
            },
            message=socket.next()=>{
                last_read=Instant::now();
                match message {
                    Some(Ok(Message::Text(text)))=>{
                        let result=serde_json::from_str::<Value>(&text).map_err(|_|GladiaError::Protocol("JSON")).and_then(|v|{crate::settings::validate_value(&v,0)?;protocol.message(v)});
                        match result {Ok(Some(event))=>emit(events,event)?,Ok(None)=>(),Err(error)=>emit(events,GladiaSttEvent::Error(error))?}
                    },
                    Some(Ok(Message::Ping(bytes)))=>{write(socket,Message::Pong(bytes),config).await?;},
                    Some(Ok(Message::Pong(_)))=>(),
                    Some(Ok(Message::Close(_)))|None=>return Err(GladiaError::Closed),
                    Some(Ok(_))=>emit(events,GladiaSttEvent::Error(GladiaError::Protocol("expected JSON text")))?,
                    Some(Err(error))=>return Err(transport(error)),
                }
            },
            audio=input.recv()=>{let Some(audio)=audio else{return Err(GladiaError::Closed)};write_audio(socket,audio,&mut protocol,config).await?;last_write=Instant::now();},
            _=tokio::time::sleep_until(last_read+config.limits.read_timeout)=>return Err(GladiaError::Timeout("read")),
            _=tokio::time::sleep_until(last_write+config.limits.keepalive_interval)=>{write(socket,Message::Text(json!({"type":"audio_chunk","data":{"chunk":""}}).to_string().into()),config).await?;last_write=Instant::now();},
        }
    }
}
async fn write_audio(
    socket: &mut Socket,
    audio: Audio,
    protocol: &mut Protocol,
    config: &GladiaSttConfig,
) -> Result<(), GladiaError> {
    let bytes = audio.frame.audio.len();
    let text = json!({"type":"audio_chunk","data":{"chunk":STANDARD.encode(&audio.frame.audio)}})
        .to_string();
    write(socket, Message::Text(text.into()), config).await?;
    protocol.accounting.submit(bytes, audio.permit)?;
    protocol.usage_bytes = protocol
        .usage_bytes
        .checked_add(bytes as u64)
        .ok_or(GladiaError::TooLarge)?;
    Ok(())
}
fn emit(events: &mpsc::Sender<GladiaSttEvent>, event: GladiaSttEvent) -> Result<(), GladiaError> {
    events.try_send(event).map_err(|e| match e {
        mpsc::error::TrySendError::Full(_) => GladiaError::Backpressure,
        mpsc::error::TrySendError::Closed(_) => GladiaError::Closed,
    })
}
async fn write(
    socket: &mut Socket,
    message: Message,
    config: &GladiaSttConfig,
) -> Result<(), GladiaError> {
    timeout(config.limits.write_timeout, socket.send(message))
        .await
        .map_err(|_| GladiaError::Timeout("write"))?
        .map_err(transport)
}
fn transport(error: tokio_tungstenite::tungstenite::Error) -> GladiaError {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::Http(r) => GladiaError::Http(r.status().as_u16()),
        Error::Capacity(_) => GladiaError::TooLarge,
        Error::ConnectionClosed | Error::AlreadyClosed => GladiaError::Closed,
        _ => GladiaError::Transport,
    }
}
/// Caps TLS plus HTTP upgrade bytes before tungstenite's message limit applies.
struct HandshakeBoundedStream {
    inner: TcpStream,
    handshaking: Arc<AtomicBool>,
    read: usize,
}
impl AsyncRead for HandshakeBoundedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        this.read = this.read.saturating_add(buf.filled().len() - before);
        if this.handshaking.load(Ordering::Acquire) && this.read > 256 * 1024 {
            return Poll::Ready(Err(io::Error::other("handshake limit")));
        }
        result
    }
}
impl AsyncWrite for HandshakeBoundedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
