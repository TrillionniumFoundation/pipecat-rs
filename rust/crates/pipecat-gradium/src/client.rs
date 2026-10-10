// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    protocol::{validate_json, Protocol},
    security, CancellationToken, GradiumError, GradiumSttConfig, GradiumSttEvent,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_audio::SincResampler;
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
pub(crate) type Socket = WebSocketStream<MaybeTlsStream<HandshakeBoundedStream>>;
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
#[derive(Debug, Clone)]
pub struct GradiumSttClient {
    config: GradiumSttConfig,
}
impl GradiumSttClient {
    pub fn new(config: GradiumSttConfig) -> Result<Self, GradiumError> {
        config.validate()?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &GradiumSttConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<GradiumSttSession, GradiumError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<GradiumSttSession, GradiumError> {
        let mut socket = tokio::select! {biased;_=cancel.cancelled()=>return Err(GradiumError::Cancelled),r=timeout(self.config.limits.connect_timeout,connect(&self.config))=>r.map_err(|_|GradiumError::Timeout("setup"))??};
        let generation = NEXT_GENERATION
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| GradiumError::TooLarge)?;
        let (tx, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let (event_tx, event_rx) = mpsc::channel(self.config.limits.queue_capacity);
        let (completion_tx, completion) = watch::channel(None);
        let finish = CancellationToken::new();
        let admission = Arc::new(Mutex::new(Admission::default()));
        let current = Arc::new(Mutex::new(self.config.clone()));
        let stats = Arc::new(Statistics::default());
        let failed = Arc::new(AtomicBool::new(false));
        let sender = GradiumSttSender {
            tx,
            cancel: cancel.clone(),
            finish: finish.clone(),
            admission: admission.clone(),
            completion,
            budget: Arc::new(Semaphore::new(self.config.limits.max_buffered_audio_bytes)),
            config: current.clone(),
            active: Arc::new(self.config.clone()),
            stats: stats.clone(),
            failed: failed.clone(),
        };
        let config = self.config.clone();
        let actor_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let result = tokio::select! {biased;_=actor_cancel.cancelled()=>Err(GradiumError::Cancelled),error=finish_guard(&finish,&admission)=>Err(error),r=timeout(config.limits.session_timeout,run(&mut socket,rx,&event_tx,&config,&finish,&admission,&current,&stats))=>r.unwrap_or(Err(GradiumError::Timeout("session")))};
            failed.store(result.is_err(), Ordering::Release);
            // An expired operation is fenced immediately. Do not spend an
            // additional close window after an absolute deadline has elapsed.
            let result = if matches!(result, Err(GradiumError::Timeout(_))) {
                result
            } else {
                let finish_at = admission.lock().ok().and_then(|a| a.finish_deadline);
                let close_at = finish_at
                    .map_or(Instant::now() + config.limits.close_timeout, |at| {
                        at.min(Instant::now() + config.limits.close_timeout)
                    });
                let close = tokio::time::timeout_at(close_at, socket.close(None)).await;
                match (result, close) {
                    (Ok(()), Err(_)) => Err(GradiumError::Timeout("close")),
                    (result, _) => result,
                }
            };
            completion_tx.send_replace(Some(result.clone()));
            result
        });
        Ok(GradiumSttSession {
            sender,
            events: event_rx,
            cancel,
            task,
            ended: false,
            generation,
        })
    }
}
pub(crate) async fn connect_socket(
    endpoint: &str,
    api_key: &str,
    allow_loopback: bool,
    limits: &crate::GradiumLimits,
) -> Result<Socket, GradiumError> {
    let url = security::endpoint_url(endpoint, allow_loopback)?;
    let addresses = security::addresses(&url, allow_loopback).await?;
    let stream = TcpStream::connect(addresses.as_slice())
        .await
        .map_err(|_| GradiumError::Transport)?;
    stream
        .set_nodelay(true)
        .map_err(|_| GradiumError::Transport)?;
    let handshaking = Arc::new(AtomicBool::new(true));
    let stream = HandshakeBoundedStream {
        inner: stream,
        handshaking: handshaking.clone(),
        read: 0,
    };
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| GradiumError::Endpoint)?;
    let mut key = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(api_key)
        .map_err(|_| GradiumError::Invalid("API key"))?;
    key.set_sensitive(true);
    request.headers_mut().insert("x-api-key", key);
    request.headers_mut().insert(
        "x-api-source",
        tokio_tungstenite::tungstenite::http::HeaderValue::from_static("pipecat"),
    );
    let ws = WebSocketConfig::default()
        .max_message_size(Some(limits.max_message_bytes))
        .max_frame_size(Some(limits.max_message_bytes))
        .write_buffer_size(0)
        .max_write_buffer_size(limits.max_message_bytes + 1024);
    let (socket, _) =
        tokio_tungstenite::client_async_tls_with_config(request, stream, Some(ws), None)
            .await
            .map_err(transport)?;
    handshaking.store(false, Ordering::Release);
    Ok(socket)
}
async fn connect(c: &GradiumSttConfig) -> Result<Socket, GradiumError> {
    let mut socket = connect_socket(&c.endpoint, &c.api_key, c.allow_loopback, &c.limits).await?;
    write(
        &mut socket,
        Message::Text(c.setup_message().to_string().into()),
        c,
        None,
    )
    .await?;
    match socket.next().await {
        Some(Ok(Message::Text(text))) => {
            let v: Value =
                serde_json::from_str(&text).map_err(|_| GradiumError::Protocol("ready JSON"))?;
            validate_json(&v, 0, &mut 0)?;
            match v.get("type").and_then(Value::as_str) {
                Some("ready") => (),
                Some("error") => return Err(GradiumError::Provider),
                _ => return Err(GradiumError::Protocol("expected ready")),
            }
            if v.get("sample_rate")
                .is_some_and(|n| n.as_u64() != Some(u64::from(c.wire_sample_rate())))
                || v.get("frame_size").is_some_and(|n| {
                    n.as_u64() != Some(u64::from(c.wire_sample_rate() * 80 / 1000))
                })
            {
                return Err(GradiumError::Protocol("ready PCM geometry"));
            }
            if v.get("client_req_id").is_some() {
                return Err(GradiumError::Protocol("multiplexing unsupported"));
            }
        }
        Some(Err(e)) => return Err(transport(e)),
        _ => return Err(GradiumError::Protocol("expected ready")),
    }
    Ok(socket)
}
async fn finish_guard(finish: &CancellationToken, admission: &Mutex<Admission>) -> GradiumError {
    finish.cancelled().await;
    let deadline = match admission.lock() {
        Ok(a) => a.finish_deadline,
        Err(_) => None,
    };
    match deadline {
        Some(at) => {
            tokio::time::sleep_until(at).await;
            GradiumError::Timeout("finish")
        }
        None => GradiumError::Closed,
    }
}
#[derive(Default)]
struct Admission {
    finished: bool,
    flush: bool,
    finish_deadline: Option<Instant>,
}
struct Audio {
    frame: AudioRawFrame,
    _permit: OwnedSemaphorePermit,
}
enum Command {
    Audio(Audio),
    Flush,
}
#[derive(Default)]
struct Statistics {
    accepted: AtomicU64,
    sent: AtomicU64,
    confirmed: AtomicU64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GradiumStatistics {
    /// Original-rate bytes admitted locally. Admission is not remote completion.
    pub accepted_input_bytes: u64,
    /// Resampled bytes successfully written to the socket, possibly ambiguous on failure.
    pub sent_wire_bytes: u64,
    /// Bytes covered by a matching flush acknowledgment or final EOS.
    pub confirmed_wire_bytes: u64,
}
#[derive(Clone)]
pub struct GradiumSttSender {
    tx: mpsc::Sender<Command>,
    cancel: CancellationToken,
    finish: CancellationToken,
    admission: Arc<Mutex<Admission>>,
    completion: watch::Receiver<Option<Result<(), GradiumError>>>,
    budget: Arc<Semaphore>,
    config: Arc<Mutex<GradiumSttConfig>>,
    active: Arc<GradiumSttConfig>,
    stats: Arc<Statistics>,
    failed: Arc<AtomicBool>,
}
impl fmt::Debug for GradiumSttSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GradiumSttSender")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}
impl GradiumSttSender {
    pub fn validate_audio(&self, audio: &AudioRawFrame) -> Result<(), GradiumError> {
        if audio.sample_rate != self.active.sample_rate
            || audio.num_channels != 1
            || audio.audio.is_empty()
        {
            return Err(GradiumError::Invalid("PCM format"));
        }
        if audio.audio.len() > self.active.limits.max_audio_chunk_bytes {
            return Err(GradiumError::TooLarge);
        }
        Ok(())
    }
    /// Bounded immediate admission. Odd byte splits are retained by the resampler;
    /// an incomplete final sample fails EOS/finish instead of fabricating audio.
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<(), GradiumError> {
        self.validate_audio(&audio)?;
        let state = self.admission.lock().map_err(|_| GradiumError::Closed)?;
        if state.finished
            || self.cancel.is_cancelled()
            || self.failed.load(Ordering::Acquire)
            || self.tx.is_closed()
        {
            return Err(GradiumError::Closed);
        }
        let count = audio.audio.len() as u64;
        let accepted = self
            .stats
            .accepted
            .load(Ordering::Relaxed)
            .checked_add(count)
            .ok_or(GradiumError::TooLarge)?;
        if accepted > self.active.limits.max_session_input_bytes {
            return Err(GradiumError::TooLarge);
        }
        let permit = self
            .budget
            .clone()
            .try_acquire_many_owned(count as u32)
            .map_err(|_| GradiumError::Backpressure)?;
        self.tx
            .try_send(Command::Audio(Audio {
                frame: audio,
                _permit: permit,
            }))
            .map_err(queue_error)?;
        self.stats.accepted.store(accepted, Ordering::Release);
        Ok(())
    }
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<(), GradiumError> {
        self.try_send_audio(audio)
    }
    pub fn try_flush(&self) -> Result<(), GradiumError> {
        if self.active.enable_turn_detection {
            return Err(GradiumError::Invalid("provider turn detection owns flush"));
        }
        let mut state = self.admission.lock().map_err(|_| GradiumError::Closed)?;
        if state.finished
            || self.cancel.is_cancelled()
            || self.failed.load(Ordering::Acquire)
            || self.tx.is_closed()
        {
            return Err(GradiumError::Closed);
        }
        if state.flush {
            return Err(GradiumError::Busy);
        }
        self.tx.try_send(Command::Flush).map_err(queue_error)?;
        state.flush = true;
        Ok(())
    }
    /// Turn settings are atomic and visible on the next step. Connection-bound
    /// changes are rejected; create a new session explicitly without replay.
    pub fn merge_settings(&self, delta: &Map<String, Value>) -> Result<(), GradiumError> {
        if self.is_closed() {
            return Err(GradiumError::Closed);
        }
        let mut config = self.config.lock().map_err(|_| GradiumError::Closed)?;
        let next = config.settings.merge(delta)?;
        if !config.settings.same_connection(&next) {
            return Err(GradiumError::RequiresRestart);
        }
        config.settings = next;
        Ok(())
    }
    pub fn config_snapshot(&self) -> Result<GradiumSttConfig, GradiumError> {
        self.config
            .lock()
            .map(|c| c.clone())
            .map_err(|_| GradiumError::Closed)
    }
    pub fn statistics(&self) -> GradiumStatistics {
        GradiumStatistics {
            accepted_input_bytes: self.stats.accepted.load(Ordering::Acquire),
            sent_wire_bytes: self.stats.sent.load(Ordering::Acquire),
            confirmed_wire_bytes: self.stats.confirmed.load(Ordering::Acquire),
        }
    }
    /// Drain admitted audio, send EOS and await the provider's terminal reply.
    /// Consumers must keep draining events while this future is pending.
    pub async fn finish(&self) -> Result<(), GradiumError> {
        let deadline = {
            let mut state = self.admission.lock().map_err(|_| GradiumError::Closed)?;
            state.finished = true;
            let deadline = *state
                .finish_deadline
                .get_or_insert_with(|| Instant::now() + self.active.limits.finish_timeout);
            self.finish.cancel();
            deadline
        };
        let mut completion = self.completion.clone();
        let result = tokio::time::timeout_at(deadline, async {
            loop {
                if let Some(result) = completion.borrow_and_update().clone() {
                    return result;
                }
                completion
                    .changed()
                    .await
                    .map_err(|_| GradiumError::Closed)?
            }
        })
        .await;
        match result {
            Ok(result) => result,
            Err(_) => {
                self.cancel();
                Err(GradiumError::Timeout("finish"))
            }
        }
    }
    pub fn cancel(&self) {
        self.cancel.cancel()
    }
    pub fn is_closed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
            || self.cancel.is_cancelled()
            || self.tx.is_closed()
            || self.admission.lock().map_or(true, |s| s.finished)
    }
}
fn queue_error<T>(e: mpsc::error::TrySendError<T>) -> GradiumError {
    match e {
        mpsc::error::TrySendError::Full(_) => GradiumError::Backpressure,
        mpsc::error::TrySendError::Closed(_) => GradiumError::Closed,
    }
}
pub struct GradiumSttSession {
    sender: GradiumSttSender,
    events: mpsc::Receiver<GradiumSttEvent>,
    cancel: CancellationToken,
    task: JoinHandle<Result<(), GradiumError>>,
    ended: bool,
    generation: u64,
}
impl GradiumSttSession {
    pub fn sender(&self) -> GradiumSttSender {
        self.sender.clone()
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn cancel(&self) {
        self.cancel.cancel()
    }
    pub async fn next(&mut self) -> Option<Result<GradiumSttEvent, GradiumError>> {
        if self.ended {
            return None;
        }
        if !self.sender.failed.load(Ordering::Acquire) {
            tokio::select! {biased;_=self.cancel.cancelled()=>(),event=self.events.recv()=>{
                if !self.sender.failed.load(Ordering::Acquire)&&!self.cancel.is_cancelled(){if let Some(event)=event{return Some(Ok(event))}}
            }}
        }
        self.events.close();
        while self.events.try_recv().is_ok() {}
        self.ended = true;
        match (&mut self.task).await {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(Err(e)),
            Err(_) => Some(Err(GradiumError::Transport)),
        }
    }
}
impl Drop for GradiumSttSession {
    fn drop(&mut self) {
        self.cancel.cancel()
    }
}
#[allow(clippy::too_many_arguments)]
async fn run(
    socket: &mut Socket,
    mut input: mpsc::Receiver<Command>,
    events: &mpsc::Sender<GradiumSttEvent>,
    c: &GradiumSttConfig,
    finish: &CancellationToken,
    admission: &Arc<Mutex<Admission>>,
    current: &Arc<Mutex<GradiumSttConfig>>,
    stats: &Statistics,
) -> Result<(), GradiumError> {
    let mut protocol = Protocol::new();
    let mut resampler = SincResampler::new(c.sample_rate, c.wire_sample_rate(), 1)
        .map_err(|_| GradiumError::Invalid("resampler"))?;
    let mut tail = Vec::new();
    let mut last_read = Instant::now();
    let mut finishing = None;
    let mut output_bytes = 0u64;
    let mut received_messages = 0u64;
    loop {
        tick(&mut protocol, c, events, admission, finishing, last_read)?;
        let final_at = protocol.pending.as_ref().and_then(|p| p.final_at);
        let flush_deadline = protocol
            .pending
            .as_ref()
            .filter(|p| p.final_at.is_none())
            .map(|p| p.deadline);
        tokio::select! {
            _=deadline(finishing)=>return Err(GradiumError::Timeout("finish")),
            _=deadline(flush_deadline)=>return Err(GradiumError::Timeout("flush acknowledgment")),
            _=deadline(final_at)=>{tick(&mut protocol,c,events,admission,finishing,last_read)?;},
            _=finish.cancelled(),if finishing.is_none()=>{finishing=Some(admission.lock().map_err(|_|GradiumError::Closed)?.finish_deadline.ok_or(GradiumError::Closed)?);input.close();},
            _=tokio::time::sleep_until(last_read+c.limits.read_timeout)=>return Err(GradiumError::Timeout("read")),
            message=socket.next()=>{
                tick(&mut protocol,c,events,admission,finishing,last_read)?;
                last_read=Instant::now();
                if let Some(Ok(message))=&message {
                    received_messages=received_messages.checked_add(1).ok_or(GradiumError::TooLarge)?;
                    output_bytes=output_bytes.checked_add(message.len().max(1) as u64).ok_or(GradiumError::TooLarge)?;
                    if received_messages>c.limits.max_session_messages||output_bytes>c.limits.max_session_output_bytes{return Err(GradiumError::TooLarge)}
                }
                match message{
                    Some(Ok(Message::Text(text)))=>{
                        let value=serde_json::from_str(&text).map_err(|_|GradiumError::Protocol("JSON"))?;tick(&mut protocol,c,events,admission,finishing,last_read)?;let turn=current.lock().map_err(|_|GradiumError::Closed)?.settings.turn.clone();
                        let outcome=protocol.message(value,c,&turn)?;stats.confirmed.store(protocol.confirmed_bytes,Ordering::Release);
                        for event in outcome.events{emit(events,event)?}
                        if outcome.ended{return Ok(())}
                        if outcome.flush{
                            admission.lock().map_err(|_|GradiumError::Closed)?.flush=true;
                            let id=protocol.start_flush(c)?;
                            write(socket,Message::Text(json!({"type":"flush","flush_id":id.to_string()}).to_string().into()),c,wire_deadline(&protocol,finishing,last_read,c)).await?;
                        }
                    },
                    Some(Ok(Message::Ping(bytes)))=>{write(socket,Message::Pong(bytes),c,wire_deadline(&protocol,finishing,last_read,c)).await?;},Some(Ok(Message::Pong(_)))=>(),
                    Some(Ok(Message::Close(_)))|None=>return Err(GradiumError::Closed),Some(Err(e))=>return Err(transport(e)),_=>return Err(GradiumError::Protocol("expected JSON text")),
                }
            },
            command=input.recv(),if !protocol.eos_sent=>{
                tick(&mut protocol,c,events,admission,finishing,last_read)?;
                match command{
                Some(Command::Audio(audio))=>{
                    // Bound synchronous filter work between cancellation/deadline
                    // observations even at the largest admitted frame/ratio.
                    for bytes in audio.frame.audio.chunks(2048){
                        tick(&mut protocol,c,events,admission,finishing,last_read)?;
                        let data=resampler.process(bytes).map_err(|_|GradiumError::Invalid("PCM resampling"))?;tail.extend(data);
                        let limit=wire_deadline(&protocol,finishing,last_read,c);write_chunks(socket,&mut tail,false,&mut protocol,c,stats,limit).await?;
                        tokio::task::yield_now().await;
                    }
                },
                Some(Command::Flush)=>{let id=protocol.start_flush(c)?;write(socket,Message::Text(json!({"type":"flush","flush_id":id.to_string()}).to_string().into()),c,wire_deadline(&protocol,finishing,last_read,c)).await?;},
                None=>{
                    if finishing.is_none(){return Err(GradiumError::Closed)}
                    let limit=wire_deadline(&protocol,finishing,last_read,c);drain_tail(socket,&mut resampler,&mut tail,&mut protocol,c,stats,limit).await?;
                    write(socket,Message::Text(json!({"type":"end_of_stream"}).to_string().into()),c,wire_deadline(&protocol,finishing,last_read,c)).await?;protocol.eos_sent=true;
                },
            }},
        }
    }
}
fn tick(
    p: &mut Protocol,
    c: &GradiumSttConfig,
    events: &mpsc::Sender<GradiumSttEvent>,
    admission: &Mutex<Admission>,
    finishing: Option<Instant>,
    last_read: Instant,
) -> Result<(), GradiumError> {
    let now = Instant::now();
    if finishing.is_some_and(|at| now >= at) {
        return Err(GradiumError::Timeout("finish"));
    }
    if p.pending
        .as_ref()
        .is_some_and(|p| p.final_at.is_none() && now >= p.deadline)
    {
        return Err(GradiumError::Timeout("flush acknowledgment"));
    }
    if now >= last_read + c.limits.read_timeout {
        return Err(GradiumError::Timeout("read"));
    }
    if p.pending
        .as_ref()
        .and_then(|p| p.final_at)
        .is_some_and(|at| now >= at)
    {
        if let Some(event) = p.finalize_flush(c) {
            emit(events, event)?
        }
        admission.lock().map_err(|_| GradiumError::Closed)?.flush = false;
    }
    Ok(())
}
async fn deadline(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}
#[allow(clippy::too_many_arguments)]
async fn drain_tail(
    socket: &mut Socket,
    resampler: &mut SincResampler,
    tail: &mut Vec<u8>,
    p: &mut Protocol,
    c: &GradiumSttConfig,
    stats: &Statistics,
    limit: Option<(Instant, &'static str)>,
) -> Result<(), GradiumError> {
    tail.extend(resampler.flush().map_err(|_| GradiumError::PartialSample)?);
    write_chunks(socket, tail, true, p, c, stats, limit).await
}
async fn write_chunks(
    socket: &mut Socket,
    tail: &mut Vec<u8>,
    flush: bool,
    p: &mut Protocol,
    c: &GradiumSttConfig,
    stats: &Statistics,
    limit: Option<(Instant, &'static str)>,
) -> Result<(), GradiumError> {
    let chunk = (c.wire_sample_rate() * 80 / 1000 * 2) as usize;
    let mut sent = 0;
    while tail.len() - sent >= chunk || (flush && sent < tail.len()) {
        let end = (sent + chunk).min(tail.len());
        let bytes = &tail[sent..end];
        write(
            socket,
            Message::Text(
                json!({"type":"audio","audio":STANDARD.encode(bytes)})
                    .to_string()
                    .into(),
            ),
            c,
            limit,
        )
        .await?;
        p.sent_bytes = p
            .sent_bytes
            .checked_add(bytes.len() as u64)
            .ok_or(GradiumError::TooLarge)?;
        stats.sent.store(p.sent_bytes, Ordering::Release);
        sent = end;
    }
    tail.drain(..sent);
    Ok(())
}
fn emit(
    events: &mpsc::Sender<GradiumSttEvent>,
    event: GradiumSttEvent,
) -> Result<(), GradiumError> {
    events.try_send(event).map_err(queue_error)
}
fn wire_deadline(
    p: &Protocol,
    finishing: Option<Instant>,
    last_read: Instant,
    c: &GradiumSttConfig,
) -> Option<(Instant, &'static str)> {
    let mut deadline = (last_read + c.limits.read_timeout, "read");
    if let Some(at) = finishing {
        if at < deadline.0 {
            deadline = (at, "finish")
        }
    }
    if let Some(pending) = &p.pending {
        let bound = match pending.final_at {
            Some(at) => (at, "trailing finalization"),
            None => (pending.deadline, "flush acknowledgment"),
        };
        if bound.0 < deadline.0 {
            deadline = bound
        }
    }
    Some(deadline)
}
async fn write(
    socket: &mut Socket,
    message: Message,
    c: &GradiumSttConfig,
    limit: Option<(Instant, &'static str)>,
) -> Result<(), GradiumError> {
    let own = (Instant::now() + c.limits.write_timeout, "write");
    let bound = limit.filter(|(at, _)| *at < own.0).unwrap_or(own);
    if Instant::now() >= bound.0 {
        return Err(GradiumError::Timeout(bound.1));
    }
    tokio::time::timeout_at(bound.0, socket.send(message))
        .await
        .map_err(|_| GradiumError::Timeout(bound.1))?
        .map_err(transport)
}
pub(crate) fn transport(error: tokio_tungstenite::tungstenite::Error) -> GradiumError {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::Http(r) => GradiumError::Http(r.status().as_u16()),
        Error::Capacity(_) => GradiumError::TooLarge,
        Error::ConnectionClosed | Error::AlreadyClosed => GradiumError::Closed,
        _ => GradiumError::Transport,
    }
}
/// Caps TLS plus HTTP upgrade bytes before tungstenite's message limit applies.
pub(crate) struct HandshakeBoundedStream {
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
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn stalled_native_tcp_write_obeys_shorter_absolute_operation_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (release, hold) = tokio::sync::oneshot::channel::<()>();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _socket = socket;
            let _ = hold.await;
        });
        let stream = TcpStream::connect(address).await.unwrap();
        let stream = HandshakeBoundedStream {
            inner: stream,
            handshaking: Arc::new(AtomicBool::new(false)),
            read: 0,
        };
        let mut socket = WebSocketStream::from_raw_socket(
            MaybeTlsStream::Plain(stream),
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let mut config = GradiumSttConfig::new("fixture");
        config.limits.write_timeout = Duration::from_secs(2);
        // Far beyond ordinary loopback TCP buffers; the peer never reads.
        let message = Message::Binary(vec![0; 16 * 1024 * 1024].into());
        let start = Instant::now();
        let result = write(
            &mut socket,
            message,
            &config,
            Some((start + Duration::from_millis(40), "flush acknowledgment")),
        )
        .await;
        assert_eq!(result, Err(GradiumError::Timeout("flush acknowledgment")));
        assert!(start.elapsed() < Duration::from_millis(500));
        drop(socket);
        release.send(()).unwrap();
        peer.await.unwrap();
    }
    #[test]
    fn nested_writes_choose_earliest_flush_finalization_finish_or_read_bound() {
        let c = GradiumSttConfig::new("fixture");
        let now = Instant::now();
        let mut protocol = Protocol::new();
        let mut config = c.clone();
        config.limits.flush_timeout = Duration::from_millis(100);
        protocol.start_flush(&config).unwrap();
        let bound =
            wire_deadline(&protocol, Some(now + Duration::from_millis(50)), now, &c).unwrap();
        assert_eq!(bound.1, "finish");
        let bound = wire_deadline(&protocol, None, now, &c).unwrap();
        assert_eq!(bound.1, "flush acknowledgment");
        protocol.pending.as_mut().unwrap().final_at = Some(now + Duration::from_millis(20));
        assert_eq!(
            wire_deadline(&protocol, None, now, &c).unwrap().1,
            "trailing finalization"
        );
    }
}
#[cfg(test)]
mod finish_tests {
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn dropped_finish_future_still_bounds_an_already_stalled_writer() {
        let listener = tokio::net::TcpSocket::new_v4().unwrap();
        listener.set_recv_buffer_size(1024).unwrap();
        listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listener.listen(4).unwrap();
        let address = listener.local_addr().unwrap();
        let (release, hold) = tokio::sync::oneshot::channel::<()>();
        let peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(json!({"type":"ready"}).to_string().into()))
                .await
                .unwrap();
            let _ = hold.await;
            drop(socket);
        });
        let mut config = GradiumSttConfig::new("fixture");
        config.endpoint = format!("ws://{address}/asr");
        config.allow_loopback = true;
        config.limits.queue_capacity = 128;
        config.limits.max_buffered_audio_bytes = 4 * 1024 * 1024;
        config.limits.write_timeout = Duration::from_secs(2);
        config.limits.finish_timeout = Duration::from_millis(80);
        let mut session = GradiumSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let sender = session.sender();
        let audio = AudioRawFrame {
            audio: vec![0; 64 * 1024],
            sample_rate: 16000,
            num_channels: 1,
            context_id: None,
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match sender.try_send_audio(audio.clone()) {
                    Ok(()) => (),
                    Err(GradiumError::Backpressure) => {
                        let before = sender.statistics().sent_wire_bytes;
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        if sender.statistics().sent_wire_bytes == before {
                            break;
                        }
                    }
                    Err(error) => panic!("unexpected admission error: {error}"),
                }
            }
        })
        .await
        .expect("writer did not become stalled");
        assert!(sender.statistics().accepted_input_bytes > sender.statistics().sent_wire_bytes);
        let before = Instant::now();
        let mut finish = Box::pin(sender.finish());
        assert!(tokio::time::timeout(Duration::from_millis(5), &mut finish)
            .await
            .is_err());
        drop(finish);
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(500), session.next())
                .await
                .unwrap()
                .unwrap(),
            Err(GradiumError::Timeout("finish"))
        );
        assert!(before.elapsed() < Duration::from_millis(500));
        release.send(()).unwrap();
        peer.await.unwrap();
    }
}

#[cfg(test)]
#[path = "review_client.rs"]
mod independent_review;
