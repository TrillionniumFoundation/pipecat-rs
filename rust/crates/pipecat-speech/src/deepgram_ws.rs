//! Bounded, complete-utterance Deepgram WebSocket TTS.
//!
//! Unlike the Python service's persistent socket, every synthesis gets its own
//! connection, one Speak and one Flush. Untagged PCM can never cross utterances.
//! This opt-in native profile is PCM16LE mono only, with no word timestamps,
//! reconnect, live text appends, or server-clear acknowledgement guarantee.
use crate::{
    deepgram::{DeepgramEncoding, DeepgramSynthesisRequest},
    SpeechChunk, SpeechError,
};
use futures_util::{FutureExt, SinkExt, StreamExt};
use pipecat_services::CancellationToken;
use reqwest::Url;
use serde::Deserialize;
use serde_json::json;
use std::{
    fmt,
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex as AsyncMutex},
    task::JoinHandle,
};
use tokio_tungstenite::{
    tungstenite::{
        client::IntoClientRequest,
        http::{HeaderValue, Request},
        protocol::WebSocketConfig,
        Error as WsError, Message,
    },
    MaybeTlsStream, WebSocketStream,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone)]
pub struct DeepgramWsLimits {
    pub queue_capacity: usize,
    /// Includes cancelled actors while their bounded cleanup is still running.
    pub max_sessions: usize,
    pub max_text_bytes: usize,
    pub max_message_bytes: usize,
    pub max_audio_bytes: usize,
    /// Aggregate incoming application/control payload bytes, not just audio.
    pub max_wire_bytes: usize,
    pub max_messages: usize,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    /// Whole connected utterance, including backpressure and normal close.
    pub response_timeout: Duration,
    /// Total cancellation/error cleanup budget, not a per-step timeout.
    pub close_timeout: Duration,
}
impl Default for DeepgramWsLimits {
    fn default() -> Self {
        Self {
            queue_capacity: 8,
            max_sessions: 4,
            max_text_bytes: 16 * 1024,
            max_message_bytes: 1024 * 1024,
            max_audio_bytes: 25 * 1024 * 1024,
            max_wire_bytes: 32 * 1024 * 1024,
            max_messages: 8192,
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(60),
            close_timeout: Duration::from_secs(2),
        }
    }
}
impl DeepgramWsLimits {
    fn validate(&self) -> Result<(), SpeechError> {
        if [
            self.queue_capacity,
            self.max_sessions,
            self.max_text_bytes,
            self.max_message_bytes,
            self.max_audio_bytes,
            self.max_wire_bytes,
            self.max_messages,
        ]
        .contains(&0)
            || [
                self.connect_timeout,
                self.write_timeout,
                self.response_timeout,
                self.close_timeout,
            ]
            .iter()
            .any(|v| v.is_zero() || *v > Duration::from_secs(86400))
        {
            return Err(SpeechError::Invalid("Deepgram WS positive bounded limits"));
        }
        if self.queue_capacity > 65536
            || self.max_sessions > 256
            || self.max_text_bytes > 1024 * 1024
            || self.max_message_bytes > 16 * 1024 * 1024
            || self.max_audio_bytes > 64 * 1024 * 1024
            || self.max_wire_bytes > 128 * 1024 * 1024
            || self.max_messages > 65536
            || self
                .queue_capacity
                .checked_add(2)
                .and_then(|n| n.checked_mul(self.max_message_bytes))
                .and_then(|n| n.checked_mul(self.max_sessions))
                .is_none_or(|n| n > 128 * 1024 * 1024)
        {
            return Err(SpeechError::TooLarge);
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct DeepgramWsConfig {
    /// Only the canonical endpoint or loopback is accepted; no redirects.
    pub endpoint: String,
    pub api_key: String,
    pub limits: DeepgramWsLimits,
}
impl DeepgramWsConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.deepgram.com/v1/speak".into(),
            api_key: api_key.into(),
            limits: DeepgramWsLimits::default(),
        }
    }
    fn endpoint(&self) -> Result<Url, SpeechError> {
        self.limits.validate()?;
        if self.endpoint.len() > 4096 || self.api_key.len() > 16384 {
            return Err(SpeechError::TooLarge);
        }
        if self.api_key.is_empty() {
            return Err(SpeechError::Invalid("empty API key"));
        }
        let _ = authorization(&self.api_key)?;
        let url = Url::parse(&self.endpoint).map_err(|_| SpeechError::Invalid("endpoint URL"))?;
        let host = url.host_str().unwrap_or_default().trim_matches(['[', ']']);
        let loopback =
            host == "localhost" || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback());
        if !matches!(url.scheme(), "ws" | "wss")
            || host.is_empty()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || (!loopback
                && !(url.scheme() == "wss"
                    && host == "api.deepgram.com"
                    && url.path() == "/v1/speak"
                    && url.port().is_none()))
        {
            return Err(SpeechError::Invalid(
                "Deepgram WS endpoint must be canonical or loopback",
            ));
        }
        Ok(url)
    }
}
impl fmt::Debug for DeepgramWsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepgramWsConfig")
            .field("api_key", &"[redacted]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

fn authorization(key: &str) -> Result<HeaderValue, SpeechError> {
    let mut header = HeaderValue::from_str(&format!("Token {key}"))
        .map_err(|_| SpeechError::Invalid("API key header"))?;
    header.set_sensitive(true);
    Ok(header)
}

#[derive(Default)]
struct Registry {
    sealed: bool,
    // Preserve only a flag when a failed completed actor is harvested. A
    // dropped stream must not hide JoinError from the eventual owner cleanup.
    join_failed: bool,
    runs: Vec<Arc<Run>>,
}
impl Registry {
    fn reap_finished(&mut self) {
        self.runs.retain(|run| {
            if run.reap_finished() {
                self.join_failed |= run.join_failed.load(Ordering::Acquire);
                false
            } else {
                true
            }
        });
    }
}
struct Owner {
    config: DeepgramWsConfig,
    registry: Mutex<Registry>,
    cleanup: AsyncMutex<()>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        let registry = self.registry.get_mut().unwrap_or_else(|e| e.into_inner());
        let runs = std::mem::take(&mut registry.runs);
        for run in runs {
            run.cancel.cancel();
        }
    }
}
struct Run {
    cancel: CancellationToken,
    task: AsyncMutex<Option<JoinHandle<()>>>,
    terminal: Mutex<Option<Result<(), SpeechError>>>,
    join_failed: AtomicBool,
    source_audio: AtomicBool,
}
impl Run {
    fn observe_join(&self, result: Result<(), tokio::task::JoinError>) {
        if result.is_err() {
            self.join_failed.store(true, Ordering::Release);
            *self.terminal.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(Err(SpeechError::Transport));
        }
    }
    // Only completed handles are polled synchronously; a live actor is never
    // aborted or detached to create space. JoinError is observed before removal.
    fn reap_finished(&self) -> bool {
        let Ok(mut task) = self.task.try_lock() else {
            return false;
        };
        let Some(handle) = task.as_mut() else {
            return true;
        };
        if !handle.is_finished() {
            return false;
        }
        if let Some(result) = (&mut *handle).now_or_never() {
            self.observe_join(result);
            *task = None;
            true
        } else {
            false
        }
    }
    async fn join(&self) -> Result<(), SpeechError> {
        let mut task = self.task.lock().await;
        if let Some(handle) = task.as_mut() {
            // The handle stays owned inside Run across await. Cancellation of
            // next/shutdown/cleanup releases the guard, not the JoinHandle.
            let result = handle.await;
            self.observe_join(result);
            *task = None;
        }
        if self.join_failed.load(Ordering::Acquire) {
            Err(SpeechError::Transport)
        } else {
            Ok(())
        }
    }
    fn take_terminal(&self) -> Result<(), SpeechError> {
        let result = self
            .terminal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .unwrap_or(Err(SpeechError::Transport));
        // An explicitly cancelled reader discards queued PCM. Even if the
        // actor already succeeded, that discarded output is not a success.
        if self.cancel.is_cancelled() {
            Err(SpeechError::Cancelled)
        } else {
            result
        }
    }
}

/// Clones share an actor registry. Use fork_owner for an independently managed
/// processor. Drop only requests cancellation; cleanup is the joined boundary.
#[derive(Clone)]
pub struct DeepgramWsClient {
    owner: Arc<Owner>,
}
impl DeepgramWsClient {
    pub fn new(config: DeepgramWsConfig) -> Result<Self, SpeechError> {
        config.endpoint()?;
        Ok(Self::from_validated(config))
    }
    fn from_validated(config: DeepgramWsConfig) -> Self {
        Self {
            owner: Arc::new(Owner {
                config,
                registry: Mutex::new(Registry::default()),
                cleanup: AsyncMutex::new(()),
            }),
        }
    }
    pub fn config(&self) -> &DeepgramWsConfig {
        &self.owner.config
    }
    /// Same immutable transport policy, independent admission and cancellation.
    pub fn fork_owner(&self) -> Self {
        Self::from_validated(self.config().clone())
    }
    pub fn synthesize(
        &self,
        request: DeepgramSynthesisRequest,
    ) -> Result<DeepgramWsStream, SpeechError> {
        request.validate()?;
        let config = self.config();
        if request.encoding != DeepgramEncoding::Linear16 {
            return Err(SpeechError::Invalid(
                "Deepgram WS supports only mono linear16",
            ));
        }
        if request.text.len() > config.limits.max_text_bytes || request.voice.len() > 256 {
            return Err(SpeechError::TooLarge);
        }
        let mut url = config.endpoint()?;
        url.query_pairs_mut()
            .append_pair("model", &request.voice)
            .append_pair("encoding", "linear16")
            .append_pair("sample_rate", &request.sample_rate.to_string());
        if let Some(speed) = request.speed {
            url.query_pairs_mut()
                .append_pair("speed", &speed.to_string());
        }
        if let Some(mip) = request.mip_opt_out {
            url.query_pairs_mut()
                .append_pair("mip_opt_out", &mip.to_string());
        }
        let mut upgrade = url
            .as_str()
            .into_client_request()
            .map_err(|_| SpeechError::Invalid("WebSocket request"))?;
        upgrade
            .headers_mut()
            .insert("authorization", authorization(&config.api_key)?);
        let speak = json!({"type":"Speak", "text":request.text}).to_string();
        if speak.len() > config.limits.max_message_bytes {
            return Err(SpeechError::TooLarge);
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| SpeechError::Invalid("Tokio runtime required"))?;
        let mut registry = self
            .owner
            .registry
            .lock()
            .map_err(|_| SpeechError::Transport)?;
        registry.reap_finished();
        if registry.sealed {
            return Err(SpeechError::Invalid("Deepgram WS owner is shut down"));
        }
        if registry.runs.len() >= config.limits.max_sessions {
            return Err(SpeechError::Invalid("Deepgram WS actor capacity exhausted"));
        }
        let run = Arc::new(Run {
            cancel: CancellationToken::new(),
            task: AsyncMutex::new(None),
            terminal: Mutex::new(None),
            join_failed: AtomicBool::new(false),
            source_audio: AtomicBool::new(false),
        });
        let (tx, rx) = mpsc::channel(config.limits.queue_capacity);
        let owned_run = run.clone();
        let limits = config.limits.clone();
        let task = runtime.spawn(async move {
            let result = actor(upgrade, speak, request.sample_rate, &limits, &owned_run, tx).await;
            *owned_run.terminal.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
        });
        // The Run is not yet externally reachable, so no one can hold this lock.
        *run.task.try_lock().expect("new run task lock") = Some(task);
        registry.runs.push(run.clone());
        Ok(DeepgramWsStream {
            rx,
            run,
            ended: false,
        })
    }
    pub fn active_sessions(&self) -> usize {
        let mut registry = self
            .owner
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        registry.reap_finished();
        registry.runs.len()
    }
    pub fn cancel_all(&self) {
        let runs = self
            .owner
            .registry
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .runs
            .clone();
        for run in runs {
            run.cancel.cancel();
        }
    }
    /// Permanently seal admission and join all actors. This future is retryable:
    /// dropping it leaves every handle in its Run. A fresh owner explicitly
    /// reopens service; clones of a retired owner can never admit new work.
    pub async fn cleanup(&self) -> Result<(), SpeechError> {
        let _cleanup = self.owner.cleanup.lock().await;
        let runs = {
            let mut registry = self
                .owner
                .registry
                .lock()
                .map_err(|_| SpeechError::Transport)?;
            registry.sealed = true;
            registry.runs.clone()
        };
        for run in &runs {
            run.cancel.cancel();
        }
        let mut failed = false;
        for run in &runs {
            failed |= run.join().await.is_err();
        }
        let mut registry = self
            .owner
            .registry
            .lock()
            .map_err(|_| SpeechError::Transport)?;
        registry.join_failed |= failed;
        registry.runs.clear();
        if registry.join_failed {
            Err(SpeechError::Transport)
        } else {
            Ok(())
        }
    }
}

/// Dedicated stream: it deliberately does not use ResponseStream's abort-on-drop.
/// Drop signals the registered actor; only shutdown/owner cleanup proves joined.
pub struct DeepgramWsStream {
    rx: mpsc::Receiver<SpeechChunk>,
    run: Arc<Run>,
    ended: bool,
}
impl DeepgramWsStream {
    pub(crate) fn source_audio_observed(&self) -> bool {
        self.run.source_audio.load(Ordering::Acquire)
    }
    pub fn cancel(&self) {
        self.run.cancel.cancel();
    }
    fn discard(&mut self) {
        self.rx.close();
        while self.rx.try_recv().is_ok() {}
    }
    pub async fn next(&mut self) -> Option<Result<SpeechChunk, SpeechError>> {
        if self.ended {
            return None;
        }
        tokio::select! { biased;
            _ = self.run.cancel.cancelled() => self.discard(),
            chunk = self.rx.recv() => if let Some(chunk) = chunk { return Some(Ok(chunk)); }
        }
        let joined = self.run.join().await;
        let result = joined.and_then(|()| self.run.take_terminal());
        self.ended = true;
        result.err().map(Err)
    }
    pub async fn shutdown(&mut self) -> Result<(), SpeechError> {
        self.cancel();
        self.discard();
        self.run.join().await?;
        if self.ended {
            return Ok(());
        }
        let result = self.run.take_terminal();
        self.ended = true;
        result
    }
}
impl Drop for DeepgramWsStream {
    fn drop(&mut self) {
        self.run.cancel.cancel();
    }
}

fn transport(error: WsError) -> SpeechError {
    match error {
        WsError::Http(response) => SpeechError::Http(response.status().as_u16()),
        WsError::Capacity(_) | WsError::WriteBufferFull(_) => SpeechError::TooLarge,
        _ => SpeechError::Transport,
    }
}
async fn write(
    socket: &mut Socket,
    message: Message,
    limits: &DeepgramWsLimits,
) -> Result<(), SpeechError> {
    tokio::time::timeout(limits.write_timeout, socket.send(message))
        .await
        .map_err(|_| SpeechError::Timeout)?
        .map_err(transport)
}
fn command(kind: &str) -> Message {
    Message::Text(json!({"type":kind}).to_string().into())
}
async fn actor(
    upgrade: Request<()>,
    speak: String,
    rate: u32,
    limits: &DeepgramWsLimits,
    run: &Run,
    tx: mpsc::Sender<SpeechChunk>,
) -> Result<(), SpeechError> {
    let ws_config = WebSocketConfig::default()
        .write_buffer_size(0)
        .max_write_buffer_size(limits.max_message_bytes * 2 + 1024)
        .max_message_size(Some(limits.max_message_bytes))
        .max_frame_size(Some(limits.max_message_bytes));
    // tokio-tungstenite's connect helper returns non-101 responses directly; it
    // does not follow Location redirects or forward credentials to another host.
    let connected = tokio::select! { biased;
        _ = run.cancel.cancelled() => return Err(SpeechError::Cancelled),
        result = tokio::time::timeout(limits.connect_timeout,
            tokio_tungstenite::connect_async_with_config(upgrade, Some(ws_config), true)) => result,
    };
    let (mut socket, _) = connected
        .map_err(|_| SpeechError::Timeout)?
        .map_err(transport)?;
    let result = tokio::select! { biased;
        _ = run.cancel.cancelled() => Err(SpeechError::Cancelled),
        result = tokio::time::timeout(limits.response_timeout,
            utterance(&mut socket, speak, rate, limits, run, tx)) => result.unwrap_or(Err(SpeechError::Timeout)),
    };
    if result.is_err() {
        cancel_close(&mut socket, limits).await;
    }
    result
}
#[derive(Deserialize)]
#[serde(tag = "type")]
enum Control {
    Metadata,
    Flushed,
    Warning,
    Cleared,
    Error,
}
fn control(text: &str) -> Result<Control, SpeechError> {
    serde_json::from_str(text).map_err(|_| SpeechError::Protocol("invalid Deepgram WS control"))
}
async fn utterance(
    socket: &mut Socket,
    speak: String,
    rate: u32,
    limits: &DeepgramWsLimits,
    run: &Run,
    tx: mpsc::Sender<SpeechChunk>,
) -> Result<(), SpeechError> {
    write(socket, Message::Text(speak.into()), limits).await?;
    write(socket, command("Flush"), limits).await?;
    let mut carry = None;
    let mut audio_bytes = 0;
    let mut wire_bytes = 0;
    let mut messages = 0;
    let mut metadata = false;
    loop {
        let message = socket
            .next()
            .await
            .ok_or(SpeechError::Protocol("peer closed before Flushed"))?
            .map_err(transport)?;
        crate::common::checked_total(&mut messages, 1, limits.max_messages)?;
        crate::common::checked_total(&mut wire_bytes, message.len(), limits.max_wire_bytes)?;
        match message {
            Message::Binary(bytes) => {
                crate::common::checked_total(
                    &mut audio_bytes,
                    bytes.len(),
                    limits.max_audio_bytes,
                )?;
                let mut pcm = Vec::with_capacity(bytes.len() + usize::from(carry.is_some()));
                if let Some(byte) = carry.take() {
                    pcm.push(byte);
                }
                pcm.extend_from_slice(&bytes);
                if pcm.len() % 2 != 0 {
                    carry = pcm.pop();
                }
                if !pcm.is_empty() {
                    run.source_audio.store(true, Ordering::Release);
                    tx.send(SpeechChunk {
                        pcm,
                        sample_rate: rate,
                        num_channels: 1,
                    })
                    .await
                    .map_err(|_| SpeechError::ConsumerClosed)?;
                }
            }
            Message::Text(text) => match control(&text)? {
                Control::Metadata if !metadata && audio_bytes == 0 => metadata = true,
                Control::Warning => {}
                Control::Flushed => {
                    if carry.is_some() {
                        return Err(SpeechError::PartialSample);
                    }
                    if audio_bytes == 0 {
                        return Err(SpeechError::EmptyAudio);
                    }
                    break;
                }
                Control::Error => return Err(SpeechError::Protocol("Deepgram WS provider error")),
                _ => return Err(SpeechError::Protocol("out-of-order Deepgram WS control")),
            },
            Message::Ping(_) => {
                tokio::time::timeout(limits.write_timeout, socket.flush())
                    .await
                    .map_err(|_| SpeechError::Timeout)?
                    .map_err(transport)?;
            }
            Message::Pong(_) => {}
            Message::Close(_) => return Err(SpeechError::Protocol("peer closed before Flushed")),
            Message::Frame(_) => return Err(SpeechError::Protocol("unexpected WebSocket frame")),
        }
    }
    // No application data is valid after Flushed. Drain the close handshake so
    // duplicate terminals and late untagged PCM cannot silently count as success.
    tokio::time::timeout(limits.close_timeout, async {
        write(socket, command("Close"), limits).await?;
        write(socket, Message::Close(None), limits).await?;
        loop {
            let message = match socket.next().await {
                Some(Ok(message)) => message,
                _ => {
                    return Err(SpeechError::Protocol(
                        "peer closed without close acknowledgement",
                    ))
                }
            };
            crate::common::checked_total(&mut messages, 1, limits.max_messages)?;
            crate::common::checked_total(&mut wire_bytes, message.len(), limits.max_wire_bytes)?;
            match message {
                Message::Close(_) => return Ok(()),
                Message::Ping(_) | Message::Pong(_) => socket.flush().await.map_err(transport)?,
                _ => return Err(SpeechError::Protocol("data after Flushed")),
            }
        }
    })
    .await
    .map_err(|_| SpeechError::Timeout)?
}
async fn cancel_close(socket: &mut Socket, limits: &DeepgramWsLimits) {
    // Clear being written is only local write completion, never proof that the
    // service has cleared a buffer. Isolation is supplied by retiring the socket.
    // This is in the registered actor, not a detached cleanup task.
    let _ = tokio::time::timeout(limits.close_timeout, async {
        // A peer-initiated Close has queued tungstenite's reply; subsequent
        // application sends may reject without flushing that automatic reply.
        let _ = socket.flush().await;
        let _ = write(socket, command("Clear"), limits).await;
        let _ = write(socket, command("Close"), limits).await;
        let _ = socket.close(None).await;
        for _ in 0..limits.max_messages {
            match socket.next().await {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                _ => {}
            }
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn harvested_panic_remains_visible_to_owner_cleanup() {
        let client = DeepgramWsClient::new(DeepgramWsConfig::new("synthetic")).unwrap();
        let run = Arc::new(Run {
            cancel: CancellationToken::new(),
            task: AsyncMutex::new(None),
            terminal: Mutex::new(None),
            join_failed: AtomicBool::new(false),
            source_audio: AtomicBool::new(false),
        });
        let task = tokio::spawn(async {
            panic!("synthetic actor panic");
        });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        *run.task.lock().await = Some(task);
        client.owner.registry.lock().unwrap().runs.push(run);
        // No stream or other Run reference remains. Harvest the completed task
        // before cleanup; its panic payload must not be retained or forgotten.
        assert_eq!(client.active_sessions(), 0);
        assert!(matches!(
            client.cleanup().await,
            Err(SpeechError::Transport)
        ));
        assert!(matches!(
            client.cleanup().await,
            Err(SpeechError::Transport)
        ));
    }

    #[test]
    fn authorization_is_sensitive_and_redacted() {
        let header = authorization("synthetic-secret").unwrap();
        assert!(header.is_sensitive());
        assert!(!format!("{header:?}").contains("synthetic-secret"));
    }
}
