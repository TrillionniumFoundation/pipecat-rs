//! A single-use, bounded HTTP/2 duplex connection. No retry, proxy, pool or reconnect.
//! Hosted HTTPS requires one caller-resolved socket address; runtime DNS is unsupported.
//! The regional URL remains the HTTP authority and verified TLS server identity.
//!
//! The upload signer has exactly one owner. Startup upload and response-head reading run
//! concurrently. The retained supervisor owns and joins the writer, reader and h2 driver;
//! cancelling a completion waiter does not drop any of those handles. A response handed
//! to the caller leaves the transport byte budget and becomes the caller's responsibility.

use bytes::Bytes;
use h2::{client, Reason, SendStream};
use http::{HeaderMap, Request, StatusCode};
use pipecat_aws_common::{auth::EventStreamSigner, eventstream};
use std::{
    fmt,
    future::{poll_fn, Future},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{mpsc, oneshot, watch, Mutex, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{sleep_until, timeout_at, Instant},
};
use url::Url;

const MIME: &str = "application/vnd.amazon.eventstream";
const MAX_FRAME: usize = 1024 * 1024;
const MAX_QUEUE_BYTES: usize = 16 * 1024 * 1024;
const H2_FRAME: u32 = 16 * 1024;

/// Only static categories and HTTP status codes are retained, never URLs, headers,
/// credentials, peer exception text, TLS diagnostics or audio/transcript contents.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("invalid SageMaker STT duplex transport configuration: {0}")]
    Invalid(&'static str),
    #[error("SageMaker STT duplex input exceeds its bounded budget")]
    TooLarge,
    #[error("SageMaker STT duplex session was cancelled")]
    Cancelled,
    #[error("SageMaker STT duplex input is closed")]
    Closed,
    #[error("SageMaker STT duplex connection failed")]
    Connect,
    #[error("SageMaker STT duplex TLS verification or HTTP/2 ALPN failed")]
    Tls,
    #[error("SageMaker STT duplex HTTP/2 operation failed")]
    Http2,
    #[error("SageMaker STT duplex peer returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("SageMaker STT duplex response has an invalid content type")]
    ContentType,
    #[error("SageMaker STT duplex EventStream frame is malformed")]
    Framing,
    #[error("SageMaker STT duplex response ended inside an EventStream frame")]
    Truncated,
    #[error("SageMaker STT duplex response ended before upload was closed")]
    EarlyEof,
    #[error("SageMaker STT duplex response has unsupported trailers")]
    Trailers,
    #[error("SageMaker STT duplex signing failed")]
    Signing,
    #[error("SageMaker STT duplex connect deadline elapsed")]
    ConnectTimeout,
    #[error("SageMaker STT duplex write deadline elapsed")]
    WriteTimeout,
    #[error("SageMaker STT duplex response idle deadline elapsed")]
    IdleTimeout,
    #[error("SageMaker STT duplex response consumer deadline elapsed")]
    ConsumerTimeout,
    #[error("SageMaker STT duplex graceful close deadline elapsed")]
    CloseTimeout,
    #[error("SageMaker STT duplex absolute session deadline elapsed")]
    SessionTimeout,
    #[error("SageMaker STT duplex response consumer was dropped")]
    ConsumerClosed,
    #[error("SageMaker STT duplex owned task failed to return normally")]
    TaskFailed,
    #[error("SageMaker STT duplex cleanup required a forced task abort")]
    ForcedAbort,
}

#[derive(Clone, Debug)]
pub struct TransportLimits {
    pub session_timeout: Duration,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub idle_timeout: Duration,
    pub consumer_timeout: Duration,
    pub close_timeout: Duration,
    pub input_queue_events: usize,
    pub input_queue_bytes: usize,
    pub max_input_frame_bytes: usize,
    pub response_queue_events: usize,
    pub response_queue_bytes: usize,
    pub max_response_frame_bytes: usize,
    pub startup_max_events: usize,
    pub startup_max_bytes: usize,
    pub h2_window_bytes: u32,
}
impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            session_timeout: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(90),
            consumer_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(10),
            input_queue_events: 16,
            input_queue_bytes: 2 * 1024 * 1024,
            max_input_frame_bytes: 256 * 1024,
            response_queue_events: 16,
            response_queue_bytes: 2 * 1024 * 1024,
            max_response_frame_bytes: 256 * 1024,
            startup_max_events: 128,
            startup_max_bytes: 1024 * 1024,
            h2_window_bytes: 64 * 1024,
        }
    }
}
impl TransportLimits {
    pub(crate) fn validate(&self) -> Result<(), TransportError> {
        if self.session_timeout.is_zero() || self.session_timeout > Duration::from_secs(420) {
            return Err(TransportError::Invalid("finite session deadline"));
        }
        for timeout in [
            self.connect_timeout,
            self.write_timeout,
            self.idle_timeout,
            self.consumer_timeout,
            self.close_timeout,
        ] {
            if timeout.is_zero() || timeout > self.session_timeout {
                return Err(TransportError::Invalid("finite operation deadline"));
            }
        }
        for (events, bytes, frame) in [
            (
                self.input_queue_events,
                self.input_queue_bytes,
                self.max_input_frame_bytes,
            ),
            (
                self.response_queue_events,
                self.response_queue_bytes,
                self.max_response_frame_bytes,
            ),
        ] {
            if !(1..=256).contains(&events)
                || !(16..=MAX_FRAME).contains(&frame)
                || bytes < frame
                || bytes > MAX_QUEUE_BYTES
            {
                return Err(TransportError::Invalid("queue byte and count bounds"));
            }
        }
        if !(1..=512).contains(&self.startup_max_events)
            || self.startup_max_bytes < 16
            || self.startup_max_bytes > MAX_QUEUE_BYTES
            || !(1024..=1024 * 1024).contains(&self.h2_window_bytes)
        {
            return Err(TransportError::Invalid("startup or HTTP/2 bounds"));
        }
        Ok(())
    }
}

pub(crate) struct TransportConfig {
    pub endpoint: Url,
    /// Exactly one caller-resolved IP route for hosted HTTPS. No runtime DNS.
    /// Literal-loopback endpoints derive their route from the URL instead.
    pub connect_address: Option<SocketAddr>,
    pub headers: HeaderMap,
    pub limits: TransportLimits,
}
impl fmt::Debug for TransportConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransportConfig")
            .field("endpoint", &"[validated endpoint]")
            .field("connect_address", &"[explicit route]")
            .field("headers", &"[redacted]")
            .field("limits", &self.limits)
            .finish()
    }
}
impl TransportConfig {
    fn validate(&mut self) -> Result<(), TransportError> {
        self.limits.validate()?;
        let url = &self.endpoint;
        let host = url.host_str().ok_or(TransportError::Invalid("endpoint"))?;
        let local = host
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
        let regional = host
            .strip_prefix("runtime.sagemaker.")
            .and_then(|rest| rest.strip_suffix(".amazonaws.com"))
            .is_some_and(|region| {
                pipecat_aws_common::auth::region(region).is_ok()
                    && host
                        == format!(
                            "runtime.sagemaker.{region}.{}",
                            pipecat_aws_common::auth::domain(region)
                        )
            });
        let path = url.path();
        let model = path
            .strip_prefix("/endpoints/")
            .and_then(|p| p.strip_suffix("/invocations-bidirectional-stream"));
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !(local && matches!(url.scheme(), "http" | "https")
                || regional && url.scheme() == "https" && url.port_or_known_default() == Some(8443))
            || !model.is_some_and(|m| {
                !m.is_empty()
                    && m.len() <= 63
                    && m.as_bytes()[0].is_ascii_alphanumeric()
                    && m.as_bytes()[m.len() - 1].is_ascii_alphanumeric()
                    && m.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            })
        {
            return Err(TransportError::Invalid("endpoint"));
        }
        self.socket_address()?;
        let authority = pipecat_aws_common::auth::host(url);
        if self.headers.len() > 8
            || self.headers.get("host").and_then(|v| v.to_str().ok()) != Some(authority.as_str())
            || self
                .headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                != Some(MIME)
            || !self.headers.contains_key("authorization")
            || !self.headers.contains_key("x-amz-date")
        {
            return Err(TransportError::Invalid("signed request headers"));
        }
        for (name, value) in self.headers.iter_mut() {
            if !matches!(
                name.as_str(),
                "host"
                    | "content-type"
                    | "authorization"
                    | "x-amz-date"
                    | "x-amz-security-token"
                    | "x-amzn-sagemaker-model-invocation-path"
                    | "x-amzn-sagemaker-model-query-string"
            ) || value.len() > 32 * 1024
            {
                return Err(TransportError::Invalid("signed request headers"));
            }
            if matches!(
                name.as_str(),
                "authorization"
                    | "x-amz-security-token"
                    | "x-amzn-sagemaker-model-invocation-path"
                    | "x-amzn-sagemaker-model-query-string"
            ) {
                value.set_sensitive(true);
            }
        }
        for name in [
            "host",
            "content-type",
            "authorization",
            "x-amz-date",
            "x-amz-security-token",
            "x-amzn-sagemaker-model-invocation-path",
            "x-amzn-sagemaker-model-query-string",
        ] {
            if self.headers.get_all(name).iter().count() > 1 {
                return Err(TransportError::Invalid("duplicate signed request header"));
            }
        }
        Ok(())
    }

    fn socket_address(&self) -> Result<SocketAddr, TransportError> {
        let host = self
            .endpoint
            .host_str()
            .ok_or(TransportError::Invalid("endpoint"))?;
        let port = self
            .endpoint
            .port_or_known_default()
            .ok_or(TransportError::Invalid("endpoint port"))?;
        if port == 0 {
            return Err(TransportError::Invalid("endpoint port"));
        }
        if let Ok(ip) = host.trim_matches(['[', ']']).parse::<IpAddr>() {
            if !ip.is_loopback() || self.connect_address.is_some() {
                return Err(TransportError::Invalid(
                    "literal loopback route cannot be overridden",
                ));
            }
            return Ok(SocketAddr::new(ip, port));
        }
        let address = self.connect_address.ok_or(TransportError::Invalid(
            "hosted HTTPS requires one caller-resolved IP address",
        ))?;
        if self.endpoint.scheme() != "https"
            || port != 8443
            || address.port() != port
            || address.ip().is_unspecified()
            || address.ip().is_multicast()
            || matches!(address.ip(), IpAddr::V4(ip) if ip.is_broadcast())
            || matches!(address, SocketAddr::V6(ip) if ip.scope_id() != 0 || ip.flowinfo() != 0)
        {
            return Err(TransportError::Invalid("hosted HTTPS IP address plan"));
        }
        Ok(address)
    }
}

#[derive(Clone)]
struct Signal(watch::Sender<bool>);
impl Signal {
    fn new() -> Self {
        Self(watch::channel(false).0)
    }
    fn fire(&self) {
        self.0.send_replace(true);
    }
    fn is_set(&self) -> bool {
        *self.0.borrow()
    }
    async fn notified(&self) {
        let mut rx = self.0.subscribe();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

#[derive(Clone, Default)]
struct Readiness {
    startup_written: bool,
    response_accepted: bool,
    active_at: Option<Instant>,
    failure: Option<TransportError>,
}

struct Input {
    frames: Vec<Box<[u8]>>,
    end: bool,
    pcm_bytes: usize,
    written: oneshot::Sender<()>,
    _bytes: OwnedSemaphorePermit,
}
struct Admission {
    closed: bool,
}

/// Cloneable input admission, not an owner that can keep a dropped session alive.
#[derive(Clone)]
pub(crate) struct TransportSender {
    uploaded_pcm: Arc<AtomicU64>,
    keepalive_attempts: Arc<AtomicU64>,
    keepalive_written: Arc<AtomicU64>,
    input: mpsc::Sender<Input>,
    bytes: Arc<Semaphore>,
    admission: Arc<Mutex<Admission>>,
    timer_fence: Arc<std::sync::Mutex<bool>>,
    cancel: Signal,
    closing: watch::Sender<Option<Instant>>,
    limits: TransportLimits,
}
impl fmt::Debug for TransportSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TransportSender([bounded input])")
    }
}
impl TransportSender {
    /// Cancels independently of queue admission or a blocked response consumer.
    pub fn cancel(&self) {
        self.cancel.fire();
    }
    /// Returns when one bounded input is admitted, not when the peer processes it.
    #[cfg(test)]
    pub async fn send(&self, inner: Vec<u8>) -> Result<(), TransportError> {
        self.enqueue(vec![inner], false, 0, || {}).await.map(|_| ())
    }
    /// Atomically fences accepted inputs. Exactly these final SageMaker STT events, then one
    /// signed empty message, are ordered behind those inputs. A cancelled admission
    /// future either did not change state or already enqueued the complete fence.
    pub async fn end(&self, final_inner: Vec<Vec<u8>>) -> Result<(), TransportError> {
        if final_inner.len() > 3 {
            return Err(TransportError::TooLarge);
        }
        self.enqueue(final_inner, true, 0, || {}).await.map(|_| ())
    }
    pub fn uploaded_pcm_bytes(&self) -> u64 {
        self.uploaded_pcm.load(Ordering::Acquire)
    }
    pub fn keepalive_counts(&self) -> (u64, u64) {
        (
            self.keepalive_attempts.load(Ordering::Acquire),
            self.keepalive_written.load(Ordering::Acquire),
        )
    }
    pub async fn send_audio(
        &self,
        frames: Vec<Vec<u8>>,
        pcm_bytes: usize,
    ) -> Result<oneshot::Receiver<()>, TransportError> {
        if frames.len() != 1 {
            return Err(TransportError::Invalid("one raw PCM event"));
        }
        self.enqueue(frames, false, pcm_bytes, || {}).await
    }
    #[cfg(test)]
    pub async fn send_control(
        &self,
        frame: Vec<u8>,
    ) -> Result<oneshot::Receiver<()>, TransportError> {
        self.send_control_with_admission(frame, || {}).await
    }
    pub async fn send_control_with_admission(
        &self,
        frame: Vec<u8>,
        on_admit: impl FnOnce() + Send,
    ) -> Result<oneshot::Receiver<()>, TransportError> {
        self.enqueue(vec![frame], false, 0, on_admit).await
    }
    /// LOCAL full signed DATA admission, never a peer-processing acknowledgement.
    pub async fn written(&self, receipt: oneshot::Receiver<()>) -> Result<(), TransportError> {
        bounded(
            &self.cancel,
            Instant::now() + self.limits.write_timeout,
            TransportError::WriteTimeout,
            receipt,
        )
        .await?
        .map_err(|_| TransportError::Closed)
    }
    async fn enqueue(
        &self,
        frames: Vec<Vec<u8>>,
        end: bool,
        pcm_bytes: usize,
        on_admit: impl FnOnce() + Send,
    ) -> Result<oneshot::Receiver<()>, TransportError> {
        if self.cancel.is_set() {
            return Err(TransportError::Cancelled);
        }
        let mut total = 0usize;
        for frame in &frames {
            validate_input(frame, self.limits.max_input_frame_bytes)?;
            total = total
                .checked_add(frame.len())
                .ok_or(TransportError::TooLarge)?;
        }
        if total > self.limits.input_queue_bytes {
            return Err(TransportError::TooLarge);
        }
        // Discard caller excess allocation capacity before any admission wait.
        let frames = frames.into_iter().map(Vec::into_boxed_slice).collect();
        let deadline = Instant::now() + self.limits.write_timeout;
        // Check the close fence before waiting for bytes held by the current
        // final command. The writer never needs this admission lock to progress.
        let mut admission = bounded(
            &self.cancel,
            deadline,
            TransportError::WriteTimeout,
            self.admission.lock(),
        )
        .await?;
        if admission.closed {
            return Err(TransportError::Closed);
        }
        let permit = bounded(
            &self.cancel,
            deadline,
            TransportError::WriteTimeout,
            self.bytes.clone().acquire_many_owned(total as u32),
        )
        .await?
        .map_err(|_| TransportError::Closed)?;
        let queue = bounded(
            &self.cancel,
            deadline,
            TransportError::WriteTimeout,
            self.input.reserve(),
        )
        .await?
        .map_err(|_| TransportError::Closed)?;
        if end {
            admission.closed = true;
            *self.timer_fence.lock().unwrap_or_else(|e| e.into_inner()) = true;
            self.closing.send_replace(Some(Instant::now()));
        }
        let (written, receipt) = oneshot::channel();
        // No fallible step or await between callback and reserved queue send.
        on_admit();
        queue.send(Input {
            frames,
            end,
            pcm_bytes,
            written,
            _bytes: permit,
        });
        Ok(receipt)
    }
}

struct ResponseFrame {
    data: Vec<u8>,
    _bytes: OwnedSemaphorePermit,
}
pub(crate) struct TransportReceiver {
    output: mpsc::Receiver<ResponseFrame>,
    cancel: Signal,
}
impl TransportReceiver {
    /// The returned complete CRC-checked frame is now charged to the caller's
    /// protocol/output budget, not to this transport queue.
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.output.recv().await.map(|frame| frame.data)
    }
}
impl Drop for TransportReceiver {
    fn drop(&mut self) {
        if !self.output.is_closed() {
            self.cancel.fire();
        }
    }
}

/// All `*_joined` fields mean the real task handle was awaited. They are false
/// when setup failed before that task existed. Forced abort is never success.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TransportReport {
    pub primary: Option<TransportError>,
    pub cleanup: Option<TransportError>,
    pub writer_joined: bool,
    pub reader_joined: bool,
    pub connection_joined: bool,
    pub forced_abort: bool,
}
impl TransportReport {
    #[cfg(test)]
    pub fn result(&self) -> Result<(), TransportError> {
        match self.primary.as_ref().or(self.cleanup.as_ref()) {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}
struct CompletionState {
    owner: Option<JoinHandle<TransportReport>>,
    report: Option<TransportReport>,
}
#[derive(Clone)]
pub(crate) struct TransportCompletionHandle(Arc<Mutex<CompletionState>>);
impl TransportCompletionHandle {
    /// Await by mutable borrow inside retained state. Dropping this future leaves
    /// the same join handle in place for another/concurrent waiter.
    pub async fn wait(&self) -> TransportReport {
        let mut state = self.0.lock().await;
        if let Some(report) = &state.report {
            return report.clone();
        }
        let report = match state.owner.as_mut() {
            Some(owner) => match owner.await {
                Ok(report) => report,
                Err(_) => TransportReport {
                    primary: Some(TransportError::TaskFailed),
                    ..TransportReport::default()
                },
            },
            None => TransportReport {
                primary: Some(TransportError::TaskFailed),
                ..TransportReport::default()
            },
        };
        state.owner.take();
        state.report = Some(report.clone());
        report
    }
}

pub(crate) struct Transport {
    sender: TransportSender,
    receiver: Option<TransportReceiver>,
    #[cfg(test)]
    ready: watch::Receiver<Readiness>,
    completion: TransportCompletionHandle,
}
impl fmt::Debug for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Transport([owned HTTP/2 session])")
    }
}
impl Transport {
    /// Validates all provided startup frames and limits before spawning any I/O.
    pub fn start(
        mut config: TransportConfig,
        startup: Vec<Vec<u8>>,
        signer: EventStreamSigner,
    ) -> Result<Self, TransportError> {
        config.validate()?;
        if startup.len() > config.limits.startup_max_events {
            return Err(TransportError::TooLarge);
        }
        let mut total = 0usize;
        for inner in &startup {
            validate_input(inner, config.limits.max_input_frame_bytes)?;
            total = total
                .checked_add(inner.len())
                .ok_or(TransportError::TooLarge)?;
        }
        if total > config.limits.startup_max_bytes {
            return Err(TransportError::TooLarge);
        }
        let limits = config.limits.clone();
        let cancel = Signal::new();
        let (closing, close_rx) = watch::channel(None);
        let (input, input_rx) = mpsc::channel(limits.input_queue_events);
        let (output, output_rx) = mpsc::channel(limits.response_queue_events);
        let (ready_tx, _ready) = watch::channel(Readiness::default());
        let uploaded_pcm = Arc::new(AtomicU64::new(0));
        let keepalive_attempts = Arc::new(AtomicU64::new(0));
        let keepalive_written = Arc::new(AtomicU64::new(0));
        let timer_fence = Arc::new(std::sync::Mutex::new(false));
        let sender = TransportSender {
            uploaded_pcm: uploaded_pcm.clone(),
            keepalive_attempts: keepalive_attempts.clone(),
            keepalive_written: keepalive_written.clone(),
            input,
            bytes: Arc::new(Semaphore::new(limits.input_queue_bytes)),
            admission: Arc::new(Mutex::new(Admission { closed: false })),
            timer_fence: timer_fence.clone(),
            cancel: cancel.clone(),
            closing,
            limits,
        };
        let receiver = Some(TransportReceiver {
            output: output_rx,
            cancel: cancel.clone(),
        });
        let startup = startup.into_iter().map(Vec::into_boxed_slice).collect();
        let admitted = Instant::now();
        let owner = tokio::spawn(supervise(
            config,
            startup,
            signer,
            input_rx,
            output,
            ready_tx,
            close_rx,
            cancel,
            admitted,
            uploaded_pcm,
            timer_fence,
            keepalive_attempts,
            keepalive_written,
        ));
        let completion = TransportCompletionHandle(Arc::new(Mutex::new(CompletionState {
            owner: Some(owner),
            report: None,
        })));
        Ok(Self {
            sender,
            receiver,
            #[cfg(test)]
            ready: _ready,
            completion,
        })
    }
    pub fn sender(&self) -> TransportSender {
        self.sender.clone()
    }
    pub fn take_receiver(&mut self) -> Option<TransportReceiver> {
        self.receiver.take()
    }
    pub fn completion(&self) -> TransportCompletionHandle {
        self.completion.clone()
    }
    pub fn cancel(&self) {
        self.sender.cancel.fire();
    }
    #[cfg(test)]
    pub async fn ready(&self) -> Result<(), TransportError> {
        let mut ready = self.ready.clone();
        loop {
            {
                let state = ready.borrow_and_update();
                if let Some(error) = &state.failure {
                    return Err(error.clone());
                }
                if state.startup_written && state.response_accepted {
                    return Ok(());
                }
            }
            ready.changed().await.map_err(|_| TransportError::Closed)?;
        }
    }
}
impl Drop for Transport {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn validate_input(inner: &[u8], max: usize) -> Result<(), TransportError> {
    if inner.len() > max {
        return Err(TransportError::TooLarge);
    }
    eventstream::decode(inner, max).map_err(|_| TransportError::Framing)?;
    Ok(())
}
async fn bounded<T>(
    cancel: &Signal,
    deadline: Instant,
    error: TransportError,
    future: impl Future<Output = T>,
) -> Result<T, TransportError> {
    tokio::select! {
        biased;
        _ = cancel.notified() => Err(TransportError::Cancelled),
        _ = sleep_until(deadline) => Err(error),
        result = future => Ok(result),
    }
}
trait DuplexIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> DuplexIo for T {}
type Io = Box<dyn DuplexIo>;

async fn connect(config: &TransportConfig) -> Result<Io, TransportError> {
    let host = config
        .endpoint
        .host_str()
        .ok_or(TransportError::Invalid("endpoint"))?
        .trim_matches(['[', ']']);
    // SocketAddr's Tokio ToSocketAddrs implementation is immediate: unlike
    // (&str, u16), it cannot spawn an uninterruptible hidden getaddrinfo job.
    // The hosted certificate name remains the original regional URL hostname.
    let socket = TcpStream::connect(config.socket_address()?)
        .await
        .map_err(|_| TransportError::Connect)?;
    socket
        .set_nodelay(true)
        .map_err(|_| TransportError::Connect)?;
    if config.endpoint.scheme() == "http" {
        return Ok(Box::new(socket));
    }
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| TransportError::Tls)?
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![b"h2".to_vec()];
    let name = rustls::pki_types::ServerName::try_from(host.to_owned())
        .map_err(|_| TransportError::Tls)?;
    let tls = tokio_rustls::TlsConnector::from(Arc::new(tls))
        .connect(name, socket)
        .await
        .map_err(|_| TransportError::Tls)?;
    if tls.get_ref().1.alpn_protocol() != Some(b"h2".as_slice()) {
        return Err(TransportError::Tls);
    }
    Ok(Box::new(tls))
}

#[allow(clippy::too_many_arguments)]
async fn supervise(
    config: TransportConfig,
    startup: Vec<Box<[u8]>>,
    signer: EventStreamSigner,
    input: mpsc::Receiver<Input>,
    output: mpsc::Sender<ResponseFrame>,
    ready: watch::Sender<Readiness>,
    mut closing: watch::Receiver<Option<Instant>>,
    cancel: Signal,
    admitted: Instant,
    uploaded_pcm: Arc<AtomicU64>,
    timer_fence: Arc<std::sync::Mutex<bool>>,
    keepalive_attempts: Arc<AtomicU64>,
    keepalive_written: Arc<AtomicU64>,
) -> TransportReport {
    let session_deadline = admitted + config.limits.session_timeout;
    let setup_deadline = (admitted + config.limits.connect_timeout).min(session_deadline);
    let setup = async {
        let io = connect(&config).await?;
        let mut builder = client::Builder::new();
        builder
            .enable_push(false)
            .initial_window_size(config.limits.h2_window_bytes)
            .initial_connection_window_size(config.limits.h2_window_bytes)
            .max_frame_size(H2_FRAME)
            .max_header_list_size(32 * 1024)
            .max_concurrent_streams(1)
            .initial_max_send_streams(0)
            .max_concurrent_reset_streams(2)
            .max_local_error_reset_streams(Some(2))
            .max_pending_accept_reset_streams(2)
            .max_send_buffer_size(H2_FRAME as usize);
        let (mut client, mut connection) = builder
            .handshake::<_, Bytes>(io)
            .await
            .map_err(|_| TransportError::Http2)?;
        // The handshake only writes our preface. Drive this still-local owner
        // until the peer's initial SETTINGS replace the zero stream sentinel.
        // Otherwise startup DATA can use the default 65535-byte window before
        // a smaller advertised window is applied. No request, response wait or
        // spawned task exists during this cancel/connect-deadline-bounded gate.
        poll_fn(|cx| {
            if Pin::new(&mut connection).poll(cx).is_ready() {
                return std::task::Poll::Ready(Err(TransportError::Http2));
            }
            if client.current_max_send_streams() == 0 {
                return std::task::Poll::Pending;
            }
            client.poll_ready(cx).map_err(|_| TransportError::Http2)
        })
        .await?;
        let mut request = Request::builder()
            .method("POST")
            .uri(config.endpoint.as_str())
            .version(http::Version::HTTP_2)
            .body(())
            .map_err(|_| TransportError::Invalid("request"))?;
        *request.headers_mut() = config.headers.clone();
        let (response, upload) = client
            .send_request(request, false)
            .map_err(|_| TransportError::Http2)?;
        // No reusable SendRequest reference may prevent the driver from closing.
        drop(client);
        Ok::<_, TransportError>((connection, response, upload))
    };
    let (connection, response, upload) = match bounded(
        &cancel,
        setup_deadline,
        TransportError::ConnectTimeout,
        setup,
    )
    .await
    {
        Ok(Ok(parts)) => parts,
        Ok(Err(error)) | Err(error) => {
            cancel.fire();
            ready.send_modify(|state| state.failure = Some(error.clone()));
            return TransportReport {
                primary: Some(error),
                ..TransportReport::default()
            };
        }
    };
    let uploaded = Signal::new();
    let final_admission = Signal::new();
    let finish_writer = Signal::new();
    let (writer_cause, _) = watch::channel(None);
    let mut connection =
        tokio::spawn(async move { connection.await.map_err(|_| TransportError::Http2) });
    let mut writer = tokio::spawn(write_upload(
        upload,
        signer,
        startup,
        input,
        config.limits.clone(),
        ready.clone(),
        uploaded.clone(),
        final_admission.clone(),
        finish_writer.clone(),
        cancel.clone(),
        writer_cause.clone(),
        uploaded_pcm,
        timer_fence,
        keepalive_attempts,
        keepalive_written,
    ));
    let mut reader = tokio::spawn(read_response(
        response,
        output,
        config.limits.clone(),
        ready.clone(),
        cancel.clone(),
    ));
    let mut report = TransportReport::default();
    let mut close_deadline = None;
    loop {
        let drain_deadline = close_deadline.unwrap_or(session_deadline);
        tokio::select! {
            biased;
            _ = cancel.notified() => { report.primary = Some(TransportError::Cancelled); break; }
            _ = sleep_until(session_deadline) => { report.primary = Some(TransportError::SessionTimeout); break; }
            _ = sleep_until(drain_deadline), if close_deadline.is_some() => { report.primary = Some(TransportError::CloseTimeout); break; }
            changed = closing.changed(), if close_deadline.is_none() => {
                if changed.is_ok() {
                    close_deadline = (*closing.borrow_and_update()).map(|time| time + config.limits.close_timeout);
                }
            }
            result = &mut reader, if !report.reader_joined => {
                report.reader_joined = true;
                match task_result(result) {
                    Ok(()) if uploaded.is_set() => finish_writer.fire(),
                    // The h2 driver and a fast peer may finish between final
                    // send_data admission and its completion signal on another
                    // runtime worker. Await that exact in-flight state; a failed
                    // final admission is still reported by the writer task.
                    Ok(()) if final_admission.is_set() => {},
                    Ok(()) => { report.primary = Some(TransportError::EarlyEof); break; }
                    Err(error) => { report.primary = Some(preserve_writer_cause(error, &writer_cause)); break; }
                }
            }
            _ = uploaded.notified(), if report.reader_joined && !finish_writer.is_set() => finish_writer.fire(),
            result = &mut writer, if !report.writer_joined => {
                report.writer_joined = true;
                if let Err(error) = task_result(result) { report.primary = Some(error); break; }
            }
            result = &mut connection, if !report.connection_joined => {
                report.connection_joined = true;
                if let Err(error) = task_result(result) { report.primary = Some(preserve_writer_cause(error, &writer_cause)); break; }
            }
        }
        if report.writer_joined && report.reader_joined && report.connection_joined {
            break;
        }
    }
    if let Some(error) = &report.primary {
        ready.send_modify(|state| state.failure = Some(error.clone()));
        cancel.fire();
    }
    // Cooperative cancellation resets the stream in the writer, drops receive
    // ownership in the reader, then lets h2 flush/close. A finite cleanup grace
    // owns and awaits every handle even if the absolute media deadline elapsed.
    let cleanup_deadline = Instant::now() + config.limits.close_timeout;
    for (handle, joined) in [
        (&mut writer, &mut report.writer_joined),
        (&mut reader, &mut report.reader_joined),
        (&mut connection, &mut report.connection_joined),
    ] {
        if *joined {
            continue;
        }
        match timeout_at(cleanup_deadline, &mut *handle).await {
            Ok(result) => {
                *joined = true;
                if let Err(error) = task_result(result) {
                    if error != TransportError::Cancelled && report.cleanup.is_none() {
                        report.cleanup = Some(error);
                    }
                }
            }
            Err(_) => {
                handle.abort();
                // abort is a request; only this await proves the task has ended.
                let _ = handle.await;
                *joined = true;
                report.forced_abort = true;
                report.cleanup = Some(TransportError::ForcedAbort);
            }
        }
    }
    if let Some(error) = report.primary.as_ref().or(report.cleanup.as_ref()) {
        ready.send_modify(|state| state.failure = Some(error.clone()));
    }
    report
}
fn preserve_writer_cause(
    observed: TransportError,
    writer_cause: &watch::Sender<Option<TransportError>>,
) -> TransportError {
    // A local upload failure is recorded before resetting h2. The reader or
    // driver may observe that reset before the writer JoinHandle becomes ready;
    // their generic h2 error must not replace the specific initiating failure.
    // Non-h2 reader failures remain their own independently meaningful causes.
    if observed == TransportError::Http2 {
        if let Some(cause) = &*writer_cause.borrow() {
            return cause.clone();
        }
    }
    observed
}

fn task_result(
    result: Result<Result<(), TransportError>, tokio::task::JoinError>,
) -> Result<(), TransportError> {
    result.map_err(|_| TransportError::TaskFailed)?
}

#[allow(clippy::too_many_arguments)]
async fn write_upload(
    mut stream: SendStream<Bytes>,
    mut signer: EventStreamSigner,
    startup: Vec<Box<[u8]>>,
    mut input: mpsc::Receiver<Input>,
    limits: TransportLimits,
    ready: watch::Sender<Readiness>,
    uploaded: Signal,
    final_admission: Signal,
    finish: Signal,
    cancel: Signal,
    failure: watch::Sender<Option<TransportError>>,
    uploaded_pcm: Arc<AtomicU64>,
    timer_fence: Arc<std::sync::Mutex<bool>>,
    keepalive_attempts: Arc<AtomicU64>,
    keepalive_written: Arc<AtomicU64>,
) -> Result<(), TransportError> {
    let result = async {
        for inner in startup {
            let signed = signer
                .sign_message(&inner, SystemTime::now())
                .map_err(|_| TransportError::Signing)?;
            send_wire(&mut stream, signed, false, &limits, &cancel, None).await?;
        }
        ready.send_modify(|state| state.startup_written = true);
        let mut active = ready.subscribe();
        let mut due = None;
        let mut active_seen = false;
        loop {
            if !active_seen {
                if let Some(at) = active.borrow_and_update().active_at {
                    due = Some(at + Duration::from_secs(5));
                    active_seen = true;
                }
            }
            // End fences timer starts under the same short timer fence. A timer
            // already signed before End may finish, always before CloseStream.
            if *timer_fence.lock().unwrap_or_else(|e| e.into_inner()) { due = None; active_seen = true; }
            let timer = due.unwrap_or_else(|| Instant::now() + limits.session_timeout);
            let command = tokio::select! {
                biased;
                _ = cancel.notified() => return Err(TransportError::Cancelled),
                _ = sleep_until(timer), if due.is_some() => {
                    let signed = {
                    let fence = timer_fence.lock().unwrap_or_else(|e| e.into_inner());
                    if *fence { due = None; continue; }
                    keepalive_attempts.fetch_add(1, Ordering::Release);
                    let inner = crate::protocol::control("KeepAlive", limits.max_input_frame_bytes)
                        .map_err(|_| TransportError::TooLarge)?;
                    signer.sign_message(&inner, SystemTime::now()).map_err(|_| TransportError::Signing)?
                    };
                    send_wire(&mut stream, signed, false, &limits, &cancel, None).await?;
                    keepalive_written.fetch_add(1, Ordering::Release);
                    due = Some(Instant::now() + Duration::from_secs(5));
                    continue;
                }
                changed = active.changed(), if !active_seen => {
                    changed.map_err(|_| TransportError::Closed)?;
                    continue;
                }
                command = input.recv() => command.ok_or(TransportError::Closed)?,
            };
            for (index, inner) in command.frames.iter().enumerate() {
                let signed = signer
                    .sign_message(inner, SystemTime::now())
                    .map_err(|_| TransportError::Signing)?;
                send_wire(&mut stream, signed, false, &limits, &cancel, None).await?;
                if index == 0 {
                    uploaded_pcm.fetch_add(command.pcm_bytes as u64, Ordering::Release);
                }
            }
            let _ = command.written.send(());
            if command.end {
                let terminal = signer
                    .sign_empty(SystemTime::now())
                    .map_err(|_| TransportError::Signing)?;
                send_wire(
                    &mut stream,
                    terminal,
                    true,
                    &limits,
                    &cancel,
                    Some((&final_admission, &uploaded)),
                )
                .await?;
                // Retain the reset-capable half while draining the response.
                return tokio::select! {
                    biased;
                    _ = cancel.notified() => Err(TransportError::Cancelled),
                    _ = finish.notified() => Ok(()),
                };
            }
        }
    }
    .await;
    if let Err(error) = &result {
        // Publish the original cause before the reset can wake another task.
        failure.send_replace(Some(error.clone()));
        stream.send_reset(Reason::CANCEL);
    }
    result
}

async fn send_wire(
    stream: &mut SendStream<Bytes>,
    frame: Vec<u8>,
    end: bool,
    limits: &TransportLimits,
    cancel: &Signal,
    terminal: Option<(&Signal, &Signal)>,
) -> Result<(), TransportError> {
    // The signed envelope adds 83 bytes to the bounded inner frame. The signer
    // enforces its independent complete-frame bound as well.
    if frame.len() > limits.max_input_frame_bytes.saturating_add(128) || frame.is_empty() {
        return Err(TransportError::TooLarge);
    }
    let deadline = Instant::now() + limits.write_timeout;
    let mut offset = 0;
    while offset < frame.len() {
        let wanted = (frame.len() - offset).min(H2_FRAME as usize);
        stream.reserve_capacity(wanted);
        let capacity_result = bounded(
            cancel,
            deadline,
            TransportError::WriteTimeout,
            poll_fn(|cx| {
                // poll_capacity notifies about increases, not current credit:
                // a previous poll may have consumed that notification while
                // usable capacity remains. Keep this fast path inside bounded
                // so every DATA admission still checks cancel and deadline.
                let available = stream.capacity();
                if available > 0 {
                    return std::task::Poll::Ready(Some(Ok(available)));
                }
                // A zero notification cannot authorize DATA or fail a session.
                // Re-poll to register the next capacity wakeup. Current h2
                // filters zero itself; this also preserves correct behavior if
                // a capacity-change race is exposed by another compatible patch.
                for _ in 0..2 {
                    match stream.poll_capacity(cx) {
                        std::task::Poll::Ready(Some(Ok(0))) => continue,
                        state => return state,
                    }
                }
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }),
        )
        .await
        .and_then(|state| {
            state
                .ok_or(TransportError::Http2)?
                .map_err(|_| TransportError::Http2)
        });
        let capacity = capacity_result?;
        let count = capacity.min(wanted);
        if count == 0 {
            return Err(TransportError::Http2);
        }
        // A queued short slice must not retain the entire signed-frame backing
        // allocation. h2 owns only this exact bounded DATA copy.
        let part = Bytes::copy_from_slice(&frame[offset..offset + count]);
        offset += count;
        let last = end && offset == frame.len();
        if last {
            if let Some((admitting, _)) = terminal {
                admitting.fire();
            }
        }
        let sent = stream
            .send_data(part, last)
            .map_err(|_| TransportError::Http2);
        sent?;
        if last {
            if let Some((_, uploaded)) = terminal {
                uploaded.fire();
            }
        }
    }
    stream.reserve_capacity(0);
    Ok(())
}

async fn read_response(
    mut response: client::ResponseFuture,
    output: mpsc::Sender<ResponseFrame>,
    limits: TransportLimits,
    ready: watch::Sender<Readiness>,
    cancel: Signal,
) -> Result<(), TransportError> {
    let response = bounded(
        &cancel,
        Instant::now() + limits.connect_timeout,
        TransportError::ConnectTimeout,
        poll_fn(|cx| match response.poll_informational(cx) {
            std::task::Poll::Ready(Some(Ok(info))) => {
                std::task::Poll::Ready(Err(TransportError::HttpStatus(info.status().as_u16())))
            }
            std::task::Poll::Ready(Some(Err(_))) => {
                std::task::Poll::Ready(Err(TransportError::Http2))
            }
            // Only poll the final response when the library has exposed it.
            // Its ordinary Future otherwise silently discards informational
            // headers. This finite profile rejects them explicitly instead.
            std::task::Poll::Ready(None) => std::pin::Pin::new(&mut response)
                .poll(cx)
                .map(|result| result.map_err(|_| TransportError::Http2)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }),
    )
    .await??;
    if response.status() != StatusCode::OK {
        return Err(TransportError::HttpStatus(response.status().as_u16()));
    }
    let content_type = response.headers().get_all("content-type");
    if content_type.iter().count() != 1
        || content_type.iter().next().and_then(|v| v.to_str().ok()) != Some(MIME)
    {
        return Err(TransportError::ContentType);
    }
    ready.send_modify(|state| {
        state.response_accepted = true;
        state.active_at = Some(Instant::now());
    });
    let bytes = Arc::new(Semaphore::new(limits.response_queue_bytes));
    let mut stream = response.into_body();
    let mut prelude = [0u8; 12];
    let mut prelude_used = 0usize;
    let mut message: Option<(Vec<u8>, usize, OwnedSemaphorePermit)> = None;
    while let Some(data) = bounded(
        &cancel,
        Instant::now() + limits.idle_timeout,
        TransportError::IdleTimeout,
        stream.data(),
    )
    .await?
    {
        let data = data.map_err(|_| TransportError::Http2)?;
        // h2's explicitly negotiated max DATA payload is H2_FRAME. Do not let
        // a future dependency change silently widen retained scratch storage.
        if data.len() > H2_FRAME as usize {
            return Err(TransportError::Framing);
        }
        let mut offset = 0;
        while offset < data.len() {
            if message.is_none() {
                let count = (12 - prelude_used).min(data.len() - offset);
                prelude[prelude_used..prelude_used + count]
                    .copy_from_slice(&data[offset..offset + count]);
                offset += count;
                prelude_used += count;
                stream
                    .flow_control()
                    .release_capacity(count)
                    .map_err(|_| TransportError::Http2)?;
                if prelude_used != 12 {
                    continue;
                }
                let expected =
                    eventstream::validate_prelude(&prelude, limits.max_response_frame_bytes)
                        .map_err(|_| TransportError::Framing)?;
                let permit = bounded(
                    &cancel,
                    Instant::now() + limits.consumer_timeout,
                    TransportError::ConsumerTimeout,
                    bytes.clone().acquire_many_owned(expected as u32),
                )
                .await?
                .map_err(|_| TransportError::ConsumerClosed)?;
                let mut frame = Vec::with_capacity(expected);
                frame.extend_from_slice(&prelude);
                message = Some((frame, expected, permit));
            }
            if let Some((frame, expected, _)) = message.as_mut() {
                let count = (*expected - frame.len()).min(data.len() - offset);
                frame.extend_from_slice(&data[offset..offset + count]);
                offset += count;
                stream
                    .flow_control()
                    .release_capacity(count)
                    .map_err(|_| TransportError::Http2)?;
                if frame.len() == *expected {
                    let (frame, _, permit) = message.take().ok_or(TransportError::Framing)?;
                    eventstream::decode(&frame, limits.max_response_frame_bytes)
                        .map_err(|_| TransportError::Framing)?;
                    bounded(
                        &cancel,
                        Instant::now() + limits.consumer_timeout,
                        TransportError::ConsumerTimeout,
                        output.send(ResponseFrame {
                            data: frame,
                            _bytes: permit,
                        }),
                    )
                    .await?
                    .map_err(|_| TransportError::ConsumerClosed)?;
                    prelude_used = 0;
                }
            }
        }
    }
    if prelude_used != 0 || message.is_some() {
        return Err(TransportError::Truncated);
    }
    let trailers = bounded(
        &cancel,
        Instant::now() + limits.idle_timeout,
        TransportError::IdleTimeout,
        stream.trailers(),
    )
    .await?
    .map_err(|_| TransportError::Http2)?;
    if trailers.is_some_and(|headers| !headers.is_empty()) {
        return Err(TransportError::Trailers);
    }
    Ok(())
}

// Owned transport-level negative probes use synthetic startup and short deadlines;
// they do not establish the public zero-startup/real KeepAlive application contract.
#[cfg(test)]
mod tests {
    use super::*;
    use h2::{server, RecvStream};
    use http::Response;
    use pipecat_aws_common::{auth::signed_sagemaker_event_stream_post_headers, AwsCredentials};
    use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot, time::timeout};

    fn inner(payload: &[u8]) -> Vec<u8> {
        eventstream::encode(
            &[
                (":message-type", "event"),
                (":event-type", "PayloadPart"),
                (":content-type", "application/octet-stream"),
                ("DataType", "UTF8"),
            ],
            payload,
            MAX_FRAME,
        )
        .unwrap()
    }
    fn small_limits() -> TransportLimits {
        TransportLimits {
            session_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            write_timeout: Duration::from_secs(2),
            idle_timeout: Duration::from_secs(3),
            consumer_timeout: Duration::from_secs(2),
            close_timeout: Duration::from_secs(2),
            ..TransportLimits::default()
        }
    }
    fn config(endpoint: Url, limits: TransportLimits) -> (TransportConfig, EventStreamSigner) {
        let credentials = AwsCredentials::new(
            "SYNTHETICKEY",
            "synthetic-not-real-secret",
            Some("synthetic-token".into()),
        )
        .unwrap();
        let (headers, signer) = signed_sagemaker_event_stream_post_headers(
            &endpoint,
            "us-east-1",
            &credentials,
            SystemTime::now(),
            limits.max_input_frame_bytes + 128,
            "v1/listen",
            "model=nova-3&language=en",
        )
        .unwrap();
        (
            TransportConfig {
                endpoint,
                connect_address: None,
                headers,
                limits,
            },
            signer,
        )
    }
    async fn peer<T, F, Fut>(window: u32, action: F) -> (Url, JoinHandle<T>)
    where
        T: Send + 'static,
        F: FnOnce(Request<RecvStream>, server::SendResponse<Bytes>) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let url = Url::parse(&format!(
            "http://{address}/endpoints/test-endpoint/invocations-bidirectional-stream"
        ))
        .unwrap();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(listener);
            let mut builder = server::Builder::new();
            builder
                .initial_window_size(window)
                .initial_connection_window_size(window);
            let mut connection = builder.handshake(socket).await.unwrap();
            let (request, respond) = connection.accept().await.unwrap().unwrap();
            assert_eq!(request.version(), http::Version::HTTP_2);
            assert_eq!(request.method(), "POST");
            assert_eq!(request.headers().get("content-type").unwrap(), MIME);
            assert!(!request.headers().contains_key("transfer-encoding"));
            assert!(!request.headers().contains_key("connection"));
            assert!(!request.headers().contains_key("x-amz-content-sha256"));
            let action = action(request, respond);
            tokio::pin!(action);
            let mut result = None;
            let mut connection_done = false;
            while result.is_none() || !connection_done {
                tokio::select! {
                    value = &mut action, if result.is_none() => result = Some(value),
                    next = connection.accept(), if !connection_done => {
                        match next {
                            Some(Ok(_)) => panic!("unexpected second HTTP/2 request"),
                            Some(Err(_)) | None => connection_done = true,
                        }
                    }
                }
            }
            result.unwrap()
        });
        (url, task)
    }
    async fn peer_send(stream: &mut SendStream<Bytes>, bytes: &[u8], end: bool) {
        if bytes.is_empty() {
            stream.send_data(Bytes::new(), end).unwrap();
            return;
        }
        let mut bytes = Bytes::copy_from_slice(bytes);
        while !bytes.is_empty() {
            let wanted = bytes.len().min(H2_FRAME as usize);
            stream.reserve_capacity(wanted);
            let capacity = poll_fn(|cx| {
                let available = stream.capacity();
                if available > 0 {
                    std::task::Poll::Ready(Some(Ok(available)))
                } else {
                    stream.poll_capacity(cx)
                }
            })
            .await
            .unwrap()
            .unwrap();
            let part = bytes.split_to(wanted.min(capacity));
            stream.send_data(part, end && bytes.is_empty()).unwrap();
        }
    }
    async fn collect_upload(body: &mut RecvStream, output: &mut Vec<u8>) {
        while let Some(bytes) = body.data().await {
            let bytes = bytes.unwrap();
            assert!(output.len() + bytes.len() < 4 * MAX_FRAME);
            output.extend_from_slice(&bytes);
            body.flow_control().release_capacity(bytes.len()).unwrap();
        }
    }
    fn uploaded_inner(mut bytes: &[u8]) -> Vec<Vec<u8>> {
        let mut messages = Vec::new();
        let mut terminals = 0;
        while !bytes.is_empty() {
            assert!(bytes.len() >= 12);
            let size = eventstream::validate_prelude(&bytes[..12], MAX_FRAME + 128).unwrap();
            let frame = eventstream::decode(&bytes[..size], MAX_FRAME + 128).unwrap();
            assert!(
                matches!(frame.headers.get(":chunk-signature"), Some(eventstream::HeaderValue::Bytes(value)) if value.len() == 32)
            );
            assert!(matches!(
                frame.headers.get(":date"),
                Some(eventstream::HeaderValue::Timestamp(_))
            ));
            if frame.payload.is_empty() {
                terminals += 1;
                assert_eq!(size, bytes.len(), "terminal must be last");
            } else {
                eventstream::decode(frame.payload, MAX_FRAME).unwrap();
                messages.push(frame.payload.to_vec());
            }
            bytes = &bytes[size..];
        }
        assert_eq!(terminals, 1);
        messages
    }
    async fn joined(completion: TransportCompletionHandle) -> TransportReport {
        let report = timeout(Duration::from_secs(4), completion.wait())
            .await
            .unwrap();
        assert!(report.writer_joined, "{report:?}");
        assert!(report.reader_joined, "{report:?}");
        assert!(report.connection_joined, "{report:?}");
        assert!(!report.forced_abort, "{report:?}");
        report
    }
    fn assert_connection_preface_without_request(bytes: &[u8]) {
        assert!(bytes.len() < 16 * 1024);
        assert!(bytes.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"));
        let mut frames = &bytes[24..];
        let mut settings = 0;
        while !frames.is_empty() {
            assert!(frames.len() >= 9);
            let size =
                ((frames[0] as usize) << 16) | ((frames[1] as usize) << 8) | frames[2] as usize;
            assert!(frames.len() >= 9 + size);
            // Only initial connection control is permitted, never request
            // HEADERS or DATA admitted against the unnegotiated window.
            assert!(matches!(frames[3], 4 | 8), "early stream frame");
            assert_eq!(&frames[5..9], &[0, 0, 0, 0]);
            settings += usize::from(frames[3] == 4);
            frames = &frames[9 + size..];
        }
        assert_eq!(settings, 1);
    }

    #[tokio::test]
    async fn upload_uses_existing_credit_without_waiting_for_a_new_notification() {
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            let mut uploaded = Vec::new();
            collect_upload(&mut request.into_body(), &mut uploaded).await;
            peer_send(&mut output, &[], true).await;
            uploaded
        })
        .await;
        let limits = small_limits();
        let (config, mut signer) = config(url, limits.clone());
        let payload = inner(b"already assigned upload credit");
        let frame = signer.sign_message(&payload, SystemTime::now()).unwrap();
        let terminal = signer.sign_empty(SystemTime::now()).unwrap();
        let socket = TcpStream::connect(config.socket_address().unwrap())
            .await
            .unwrap();
        let (mut client, connection) = client::handshake(socket).await.unwrap();
        let connection = tokio::spawn(connection);
        let mut request = Request::builder()
            .method("POST")
            .uri(config.endpoint.as_str())
            .body(())
            .unwrap();
        *request.headers_mut() = config.headers;
        let (response, mut stream) = client.send_request(request, false).unwrap();
        drop(client);
        let mut response = timeout(Duration::from_secs(3), response)
            .await
            .unwrap()
            .unwrap()
            .into_body();

        stream.reserve_capacity(frame.len());
        let assigned = timeout(
            Duration::from_secs(3),
            poll_fn(|cx| stream.poll_capacity(cx)),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(assigned, frame.len());
        assert_eq!(stream.capacity(), frame.len());
        // Consume the increase notification but retain every credited byte.
        // With an unchanged reservation, h2 intentionally has no new wakeup.
        assert!(poll_fn(|cx| std::task::Poll::Ready(stream.poll_capacity(cx).is_pending())).await);
        let cancel = Signal::new();
        send_wire(&mut stream, frame, false, &limits, &cancel, None)
            .await
            .unwrap();
        send_wire(&mut stream, terminal, true, &limits, &cancel, None)
            .await
            .unwrap();
        drop(stream);
        timeout(Duration::from_secs(3), async {
            // The peer's empty END_STREAM DATA is legal and may be returned
            // separately before EOF. It must never contain response bytes.
            while let Some(bytes) = response.data().await {
                assert!(bytes.unwrap().is_empty());
            }
            assert!(response.trailers().await.unwrap().is_none());
        })
        .await
        .unwrap();
        drop(response);
        timeout(Duration::from_secs(3), connection)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let uploaded = timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(uploaded_inner(&uploaded), vec![payload]);
    }

    #[tokio::test]
    async fn withheld_initial_settings_times_out_before_request_or_child_tasks() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!(
            "http://{}/endpoints/test-endpoint/invocations-bidirectional-stream",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(listener);
            // Deliberately never send the server connection preface/SETTINGS.
            // Capture the bounded client's bytes until setup drops the socket.
            let mut bytes = Vec::new();
            socket
                .take(16 * 1024)
                .read_to_end(&mut bytes)
                .await
                .unwrap();
            assert_connection_preface_without_request(&bytes);
        });
        let mut limits = small_limits();
        limits.connect_timeout = Duration::from_millis(150);
        let (config, signer) = config(endpoint, limits);
        let transport = Transport::start(config, vec![inner(b"startup")], signer).unwrap();
        let report = timeout(Duration::from_secs(3), transport.completion().wait())
            .await
            .unwrap();
        assert_eq!(report.primary, Some(TransportError::ConnectTimeout));
        assert!(!report.writer_joined);
        assert!(!report.reader_joined);
        assert!(!report.connection_joined);
        assert!(!report.forced_abort);
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn cancellation_during_withheld_initial_settings_drops_socket_and_joins_owner() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!(
            "http://{}/endpoints/test-endpoint/invocations-bidirectional-stream",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let (observed, preface_observed) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            drop(listener);
            let mut preface = [0u8; 24];
            socket.read_exact(&mut preface).await.unwrap();
            assert_eq!(&preface, b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");
            let mut header = [0u8; 9];
            socket.read_exact(&mut header).await.unwrap();
            assert_eq!(header[3], 4, "first client frame must be SETTINGS");
            let size =
                ((header[0] as usize) << 16) | ((header[1] as usize) << 8) | header[2] as usize;
            assert!(size <= 1024);
            let mut payload = vec![0u8; size];
            socket.read_exact(&mut payload).await.unwrap();
            let mut bytes = preface.to_vec();
            bytes.extend_from_slice(&header);
            bytes.extend_from_slice(&payload);
            // Synchronize cancellation after real accepted TCP and the complete
            // client preface, while this peer still withholds its SETTINGS.
            observed.send(()).unwrap();
            socket
                .take(16 * 1024)
                .read_to_end(&mut bytes)
                .await
                .unwrap();
            assert_connection_preface_without_request(&bytes);
        });
        let (config, signer) = config(endpoint, small_limits());
        let transport = Transport::start(config, vec![inner(b"startup")], signer).unwrap();
        timeout(Duration::from_secs(1), preface_observed)
            .await
            .unwrap()
            .unwrap();
        transport.cancel();
        let report = timeout(Duration::from_secs(1), transport.completion().wait())
            .await
            .unwrap();
        assert_eq!(report.primary, Some(TransportError::Cancelled));
        assert_eq!(report.cleanup, None);
        assert!(!report.writer_joined);
        assert!(!report.reader_joined);
        assert!(!report.connection_joined);
        assert!(!report.forced_abort);
        timeout(Duration::from_secs(1), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn negotiated_small_window_supports_large_startup_before_head_and_later_input() {
        let startup: Vec<_> = (0..6)
            .map(|index| inner(&vec![b'a' + index; 1200]))
            .collect();
        let startup_bytes: usize = startup.iter().map(|frame| frame.len() + 83).sum();
        assert!(startup_bytes > 4096);
        let (url, peer) = peer(4096, move |request, mut respond| async move {
            let mut body = request.into_body();
            let mut uploaded = Vec::new();
            while uploaded.len() < startup_bytes {
                let bytes = body.data().await.unwrap().unwrap();
                uploaded.extend_from_slice(&bytes);
                assert!(uploaded.len() <= startup_bytes);
                body.flow_control().release_capacity(bytes.len()).unwrap();
            }
            // Return the head only after the whole multi-window startup, not
            // merely the first DATA. Startup cannot depend on HTTP readiness.
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            collect_upload(&mut body, &mut uploaded).await;
            peer_send(&mut output, &[], true).await;
            uploaded
        })
        .await;
        let live = inner(b"live PCM after multi-window startup");
        let mut expected = startup.clone();
        expected.push(live.clone());
        let (config, signer) = config(url, small_limits());
        let mut transport = Transport::start(config, startup, signer).unwrap();
        let mut receiver = transport.take_receiver().unwrap();
        transport.ready().await.unwrap();
        transport.sender().send(live).await.unwrap();
        transport.sender().end(vec![]).await.unwrap();
        assert!(receiver.recv().await.is_none());
        assert_eq!(joined(transport.completion()).await.result(), Ok(()));
        let uploaded = timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(uploaded_inner(&uploaded), expected);
    }

    #[tokio::test]
    async fn startup_precedes_response_head_and_duplex_stays_open_until_terminal() {
        let answer = inner(br#"{"bytes":"e30="}"#);
        let expected_answer = answer.clone();
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let mut body = request.into_body();
            // This peer cannot return a response until actual request DATA arrives.
            let first = body.data().await.unwrap().unwrap();
            assert!(!first.is_empty());
            let mut uploaded = first.to_vec();
            body.flow_control().release_capacity(first.len()).unwrap();
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            peer_send(&mut output, &answer, false).await;
            collect_upload(&mut body, &mut uploaded).await;
            peer_send(&mut output, &[], true).await;
            uploaded
        })
        .await;
        let startup = inner(b"startup");
        let later = inner(b"later live PCM input");
        let finals = vec![
            inner(b"contentEnd"),
            inner(b"promptEnd"),
            inner(b"sessionEnd"),
        ];
        let expected = [vec![startup.clone(), later.clone()], finals.clone()].concat();
        let (config, signer) = config(url, small_limits());
        let mut transport = Transport::start(config, vec![startup], signer).unwrap();
        let mut receiver = transport.take_receiver().unwrap();
        transport.ready().await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), expected_answer);
        transport.sender().send(later).await.unwrap();
        transport.sender().end(finals).await.unwrap();
        assert_eq!(
            transport.sender().send(inner(b"late")).await,
            Err(TransportError::Closed)
        );
        assert!(receiver.recv().await.is_none());
        // EOF-consuming receiver drop must not convert successful joins to cancel.
        drop(receiver);
        let report = joined(transport.completion()).await;
        assert_eq!(report.result(), Ok(()));
        let uploaded = timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(uploaded_inner(&uploaded), expected);
    }

    #[tokio::test]
    async fn response_frames_split_at_every_byte_and_coalesced_are_exact() {
        // PayloadPart + octet-stream + DataType=UTF8 has 121 framing/header
        // bytes. Keep each fixture below the existing tiny-DATA guard (124,
        // 127, 126 bytes) while still splitting every header/payload/CRC byte.
        let messages = vec![inner(b"one"), inner(b"second"), inner(b"third")];
        let expected = messages.clone();
        let bytes = messages.concat();
        let (consumed, mut acknowledgements) = mpsc::channel(1);
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            // Every prelude, header, payload and CRC byte boundary is exercised.
            // Acknowledge each complete frame before sending the next one. This
            // tests segmentation, not an eager hundreds-of-tiny-DATA flood that
            // intentionally exceeds h2's framing-overhead protection budget.
            for message in &messages {
                assert!(message.len() < 128);
                for byte in message {
                    peer_send(&mut output, &[*byte], false).await;
                }
                acknowledgements.recv().await.unwrap();
            }
            // Multiple complete messages in a single DATA frame as well.
            peer_send(&mut output, &bytes, false).await;
            collect_upload(&mut request.into_body(), &mut Vec::new()).await;
            peer_send(&mut output, &[], true).await;
        })
        .await;
        let (config, signer) = config(url, small_limits());
        let mut transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        let mut receiver = transport.take_receiver().unwrap();
        transport.ready().await.unwrap();
        for message in &expected {
            assert_eq!(&receiver.recv().await.unwrap(), message);
            consumed.send(()).await.unwrap();
        }
        for message in &expected {
            assert_eq!(&receiver.recv().await.unwrap(), message);
        }
        transport.sender().end(vec![]).await.unwrap();
        assert!(receiver.recv().await.is_none());
        assert_eq!(joined(transport.completion()).await.result(), Ok(()));
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn stalled_upload_does_not_block_response_or_cancellation() {
        let response_frame = inner(b"response while upload window is exhausted");
        let expected = response_frame.clone();
        let (trigger, release) = oneshot::channel();
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let _held_unread_upload = request.into_body();
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            release.await.unwrap();
            peer_send(&mut output, &response_frame, false).await;
            assert_eq!(
                poll_fn(|cx| output.poll_reset(cx)).await.unwrap(),
                Reason::CANCEL
            );
        })
        .await;
        let (config, signer) = config(url, small_limits());
        let mut transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        let mut receiver = transport.take_receiver().unwrap();
        transport.ready().await.unwrap();
        transport
            .sender()
            .send(inner(&vec![b'x'; 128 * 1024]))
            .await
            .unwrap();
        trigger.send(()).unwrap();
        assert_eq!(receiver.recv().await.unwrap(), expected);
        transport.cancel();
        let report = joined(transport.completion()).await;
        assert_eq!(report.primary, Some(TransportError::Cancelled));
        assert!(report.cleanup.is_none(), "{report:?}");
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn blocked_upload_and_pending_end_resume_after_controlled_peer_release() {
        const WINDOW: usize = 1024;
        // A full-window startup guarantees that releasing it schedules a
        // WINDOW_UPDATE before the live-media hold, without timing guesses.
        let startup = inner(&vec![b's'; WINDOW - inner(&[]).len() - 83]);
        assert_eq!(startup.len() + 83, WINDOW);
        let media = vec![inner(&vec![b'a'; 16 * 1024]), inner(&vec![b'b'; 4 * 1024])];
        let finals = vec![
            inner(b"contentEnd"),
            inner(b"promptEnd"),
            inner(b"sessionEnd"),
        ];
        let expected = [vec![startup.clone()], media.clone(), finals.clone()].concat();
        let input_bytes = media.iter().map(Vec::len).sum();
        let (blocked, upload_blocked) = oneshot::channel();
        let (release, peer_release) = oneshot::channel();
        let (url, peer) = peer(WINDOW as u32, move |request, mut respond| async move {
            let mut body = request.into_body();
            let mut uploaded = Vec::new();
            while uploaded.len() < WINDOW {
                let bytes = body.data().await.unwrap().unwrap();
                uploaded.extend_from_slice(&bytes);
                assert!(uploaded.len() <= WINDOW);
            }
            body.flow_control().release_capacity(WINDOW).unwrap();
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();

            // Read a complete live-media window but deliberately retain every
            // flow-control credit. The larger accepted media cannot finish.
            while uploaded.len() < 2 * WINDOW {
                let bytes = body.data().await.unwrap().unwrap();
                uploaded.extend_from_slice(&bytes);
                assert!(uploaded.len() <= 2 * WINDOW);
            }
            assert_eq!(body.flow_control().used_capacity(), WINDOW);
            assert_eq!(body.flow_control().available_capacity(), 0);
            blocked.send(()).unwrap();
            peer_release.await.unwrap();
            body.flow_control().release_capacity(WINDOW).unwrap();
            collect_upload(&mut body, &mut uploaded).await;
            peer_send(&mut output, &[], true).await;
            uploaded
        })
        .await;
        let limits = TransportLimits {
            input_queue_bytes: input_bytes,
            max_input_frame_bytes: media[0].len(),
            ..small_limits()
        };
        let (config, signer) = config(url, limits);
        let mut transport = Transport::start(config, vec![startup], signer).unwrap();
        let mut receiver = transport.take_receiver().unwrap();
        transport.ready().await.unwrap();
        let sender = transport.sender();
        for frame in media {
            sender.send(frame).await.unwrap();
        }
        timeout(Duration::from_secs(1), upload_blocked)
            .await
            .unwrap()
            .unwrap();
        // Both commands were admitted; their permits remain held by the
        // blocked writer and queued command. End must wait behind them, not
        // bypass or discard accepted media to manufacture a graceful close.
        assert_eq!(sender.bytes.available_permits(), 0);
        let end = sender.end(finals);
        tokio::pin!(end);
        assert!(poll_fn(|cx| std::task::Poll::Ready(end.as_mut().poll(cx).is_pending())).await);
        release.send(()).unwrap();
        timeout(Duration::from_secs(3), end.as_mut())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            sender.send(inner(b"late")).await,
            Err(TransportError::Closed)
        );
        assert!(receiver.recv().await.is_none());
        assert_eq!(joined(transport.completion()).await.result(), Ok(()));
        let uploaded = timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(uploaded_inner(&uploaded), expected);
    }

    #[tokio::test]
    async fn saturated_response_storage_and_cancelled_waiter_keep_real_joins() {
        let (sent, received) = oneshot::channel();
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let _upload = request.into_body();
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            for _ in 0..3 {
                peer_send(&mut output, &inner(&[b'x'; 400]), false).await;
            }
            sent.send(()).unwrap();
            assert_eq!(
                poll_fn(|cx| output.poll_reset(cx)).await.unwrap(),
                Reason::CANCEL
            );
        })
        .await;
        let limits = TransportLimits {
            response_queue_events: 1,
            response_queue_bytes: 1024,
            max_response_frame_bytes: 1024,
            ..small_limits()
        };
        let (config, signer) = config(url, limits);
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        transport.ready().await.unwrap();
        received.await.unwrap();
        let completion = transport.completion();
        let cancelled_waiter = tokio::spawn({
            let completion = completion.clone();
            async move { completion.wait().await }
        });
        tokio::task::yield_now().await;
        cancelled_waiter.abort();
        assert!(cancelled_waiter.await.unwrap_err().is_cancelled());
        transport.cancel();
        let second = completion.clone();
        let (first_report, second_report) = tokio::join!(joined(completion), second.wait());
        assert_eq!(first_report, second_report);
        assert_eq!(first_report.primary, Some(TransportError::Cancelled));
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_transport_cancels_even_with_retained_sender_and_receiver() {
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let _upload = request.into_body();
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            assert_eq!(
                poll_fn(|cx| output.poll_reset(cx)).await.unwrap(),
                Reason::CANCEL
            );
        })
        .await;
        let (config, signer) = config(url, small_limits());
        let mut transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        let _receiver = transport.take_receiver().unwrap();
        let sender = transport.sender();
        let completion = transport.completion();
        transport.ready().await.unwrap();
        drop(transport);
        assert_eq!(
            joined(completion).await.primary,
            Some(TransportError::Cancelled)
        );
        assert_eq!(
            sender.send(inner(b"after owner drop")).await,
            Err(TransportError::Cancelled)
        );
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn rejected_status_and_mime_are_sanitized_and_joined_without_retry() {
        for (status, mime, expected) in [
            (400, MIME, TransportError::HttpStatus(400)),
            (302, MIME, TransportError::HttpStatus(302)),
            (200, "application/json", TransportError::ContentType),
        ] {
            let (url, peer) = peer(1024, move |request, mut respond| async move {
                let _upload = request.into_body();
                let mut output = respond
                    .send_response(
                        Response::builder()
                            .status(status)
                            .header("content-type", mime)
                            .header("location", "https://untrusted.invalid/secret")
                            .body(())
                            .unwrap(),
                        false,
                    )
                    .unwrap();
                let _ = poll_fn(|cx| output.poll_reset(cx)).await;
            })
            .await;
            let (config, signer) = config(url, small_limits());
            let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
            assert_eq!(transport.ready().await, Err(expected.clone()));
            assert_eq!(joined(transport.completion()).await.primary, Some(expected));
            timeout(Duration::from_secs(3), peer)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn truncated_response_eof_at_every_wire_layer_is_not_success() {
        let message = inner(b"response payload");
        for length in [1, 4, 8, 11, 12, 15, 30, message.len() - 1] {
            let fragment = message[..length].to_vec();
            let (url, peer) = peer(1024, move |request, mut respond| async move {
                let mut output = respond
                    .send_response(
                        Response::builder()
                            .header("content-type", MIME)
                            .body(())
                            .unwrap(),
                        false,
                    )
                    .unwrap();
                collect_upload(&mut request.into_body(), &mut Vec::new()).await;
                peer_send(&mut output, &fragment, true).await;
            })
            .await;
            let (config, signer) = config(url, small_limits());
            let mut transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
            let mut receiver = transport.take_receiver().unwrap();
            transport.ready().await.unwrap();
            transport.sender().end(vec![]).await.unwrap();
            assert!(receiver.recv().await.is_none());
            assert_eq!(
                joined(transport.completion()).await.primary,
                Some(TransportError::Truncated)
            );
            timeout(Duration::from_secs(3), peer)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn malformed_crc_and_oversize_prelude_fail_before_output() {
        let frame = inner(b"good payload");
        let mut bad_prelude = frame.clone();
        bad_prelude[8] ^= 1;
        let mut bad_message_crc = frame;
        *bad_message_crc.last_mut().unwrap() ^= 1;
        let oversize_prelude = inner(&[0; 2048])[..12].to_vec();
        for bad in [bad_prelude, bad_message_crc, oversize_prelude] {
            let (url, peer) = peer(4096, move |request, mut respond| async move {
                let _upload = request.into_body();
                let mut output = respond
                    .send_response(
                        Response::builder()
                            .header("content-type", MIME)
                            .body(())
                            .unwrap(),
                        false,
                    )
                    .unwrap();
                peer_send(&mut output, &bad, false).await;
                let _ = poll_fn(|cx| output.poll_reset(cx)).await;
            })
            .await;
            let limits = TransportLimits {
                max_response_frame_bytes: 1024,
                ..small_limits()
            };
            let (config, signer) = config(url, limits);
            let mut transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
            let mut receiver = transport.take_receiver().unwrap();
            assert!(receiver.recv().await.is_none());
            assert_eq!(
                joined(transport.completion()).await.primary,
                Some(TransportError::Framing)
            );
            timeout(Duration::from_secs(3), peer)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn response_eof_before_end_is_not_graceful_completion() {
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let _upload = request.into_body();
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            peer_send(&mut output, &[], true).await;
            let _ = poll_fn(|cx| output.poll_reset(cx)).await;
        })
        .await;
        let (config, signer) = config(url, small_limits());
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        assert_eq!(
            joined(transport.completion()).await.primary,
            Some(TransportError::EarlyEof)
        );
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn stalled_upload_write_timeout_resets_and_joins_without_replay() {
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let _upload = request.into_body();
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            assert_eq!(
                poll_fn(|cx| output.poll_reset(cx)).await.unwrap(),
                Reason::CANCEL
            );
        })
        .await;
        let limits = TransportLimits {
            write_timeout: Duration::from_millis(100),
            ..small_limits()
        };
        let (config, signer) = config(url, limits);
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        transport.ready().await.unwrap();
        transport
            .sender()
            .send(inner(&vec![b'x'; 128 * 1024]))
            .await
            .unwrap();
        assert_eq!(
            joined(transport.completion()).await.primary,
            Some(TransportError::WriteTimeout)
        );
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn end_drain_deadline_resets_even_after_upload_half_close() {
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let mut output = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            collect_upload(&mut request.into_body(), &mut Vec::new()).await;
            assert_eq!(
                poll_fn(|cx| output.poll_reset(cx)).await.unwrap(),
                Reason::CANCEL
            );
        })
        .await;
        let limits = TransportLimits {
            close_timeout: Duration::from_millis(200),
            ..small_limits()
        };
        let (config, signer) = config(url, limits);
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        transport.ready().await.unwrap();
        transport.sender().end(vec![]).await.unwrap();
        assert_eq!(
            joined(transport.completion()).await.primary,
            Some(TransportError::CloseTimeout)
        );
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn tls_handshake_cancel_drops_socket_without_h2_or_plaintext_fallback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "https://{}/endpoints/test-endpoint/invocations-bidirectional-stream",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let (seen, received) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0u8; 4096];
            let first = socket.read(&mut bytes).await.unwrap();
            assert!(first > 0);
            assert_ne!(&bytes[..first.min(3)], b"PRI");
            seen.send(()).unwrap();
            loop {
                if socket.read(&mut bytes).await.unwrap() == 0 {
                    break;
                }
            }
        });
        let (config, signer) = config(url, small_limits());
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        received.await.unwrap();
        transport.cancel();
        let report = timeout(Duration::from_secs(3), transport.completion().wait())
            .await
            .unwrap();
        assert_eq!(report.primary, Some(TransportError::Cancelled));
        assert!(!report.writer_joined && !report.reader_joined && !report.connection_joined);
        assert!(!report.forced_abort);
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn absolute_and_idle_deadlines_are_terminal_and_joined() {
        for absolute in [false, true] {
            let (url, peer) = peer(1024, move |request, mut respond| async move {
                let _upload = request.into_body();
                let mut output = respond
                    .send_response(
                        Response::builder()
                            .header("content-type", MIME)
                            .body(())
                            .unwrap(),
                        false,
                    )
                    .unwrap();
                assert_eq!(
                    poll_fn(|cx| output.poll_reset(cx)).await.unwrap(),
                    Reason::CANCEL
                );
            })
            .await;
            let limits = if absolute {
                TransportLimits {
                    session_timeout: Duration::from_millis(250),
                    connect_timeout: Duration::from_millis(250),
                    write_timeout: Duration::from_millis(250),
                    idle_timeout: Duration::from_millis(250),
                    consumer_timeout: Duration::from_millis(250),
                    close_timeout: Duration::from_millis(250),
                    ..small_limits()
                }
            } else {
                TransportLimits {
                    idle_timeout: Duration::from_millis(100),
                    ..small_limits()
                }
            };
            let (config, signer) = config(url, limits);
            let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
            transport.ready().await.unwrap();
            let expected = if absolute {
                TransportError::SessionTimeout
            } else {
                TransportError::IdleTimeout
            };
            assert_eq!(joined(transport.completion()).await.primary, Some(expected));
            timeout(Duration::from_secs(3), peer)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn informational_head_is_rejected_without_silent_promotion_to_readiness() {
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            respond
                .send_informational(Response::builder().status(100).body(()).unwrap())
                .unwrap();
            let mut body = request.into_body();
            while let Some(data) = body.data().await {
                match data {
                    Ok(data) => body.flow_control().release_capacity(data.len()).unwrap(),
                    Err(_) => break,
                }
            }
        })
        .await;
        let (config, signer) = config(url, small_limits());
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        assert_eq!(
            transport.ready().await,
            Err(TransportError::HttpStatus(100))
        );
        assert_eq!(
            joined(transport.completion()).await.primary,
            Some(TransportError::HttpStatus(100))
        );
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn peer_refused_stream_is_failure_not_a_retry_signal() {
        let (url, peer) = peer(1024, move |request, mut respond| async move {
            let _upload = request.into_body();
            respond.send_reset(Reason::REFUSED_STREAM);
        })
        .await;
        let (config, signer) = config(url, small_limits());
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        assert_eq!(
            joined(transport.completion()).await.primary,
            Some(TransportError::Http2)
        );
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn goaway_is_terminal_without_connection_replacement() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/endpoints/test-endpoint/invocations-bidirectional-stream",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut connection = server::handshake(socket).await.unwrap();
            let request = connection.accept().await.unwrap().unwrap();
            connection.abrupt_shutdown(Reason::INTERNAL_ERROR);
            drop(request);
            while connection.accept().await.is_some() {}
            assert!(timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err());
        });
        let (config, signer) = config(url, small_limits());
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        assert_eq!(
            joined(transport.completion()).await.primary,
            Some(TransportError::Http2)
        );
        timeout(Duration::from_secs(3), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn immediate_cancel_before_setup_creates_no_socket_or_child_tasks() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/endpoints/test-endpoint/invocations-bidirectional-stream",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let (config, signer) = config(url, small_limits());
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        transport.cancel();
        let report = transport.completion().wait().await;
        assert_eq!(report.primary, Some(TransportError::Cancelled));
        assert!(!report.writer_joined && !report.reader_joined && !report.connection_joined);
        assert!(timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn post_end_send_rejects_before_waiting_for_saturated_byte_budget() {
        let (input, mut queued) = mpsc::channel(1);
        let (closing, _) = watch::channel(None);
        let frame = inner(b"terminal event owns the complete byte budget");
        let limits = TransportLimits {
            input_queue_bytes: frame.len(),
            max_input_frame_bytes: frame.len(),
            ..small_limits()
        };
        let uploaded_pcm = Arc::new(AtomicU64::new(0));
        let sender = TransportSender {
            uploaded_pcm: uploaded_pcm.clone(),
            keepalive_attempts: Arc::new(AtomicU64::new(0)),
            keepalive_written: Arc::new(AtomicU64::new(0)),
            timer_fence: Arc::new(std::sync::Mutex::new(false)),
            input,
            bytes: Arc::new(Semaphore::new(frame.len())),
            admission: Arc::new(Mutex::new(Admission { closed: false })),
            cancel: Signal::new(),
            closing,
            limits,
        };
        sender.end(vec![frame.clone()]).await.unwrap();
        let _held_terminal_command = queued.recv().await.unwrap();
        assert_eq!(sender.bytes.available_permits(), 0);
        assert_eq!(
            timeout(Duration::from_millis(50), sender.send(frame.clone()))
                .await
                .unwrap(),
            Err(TransportError::Closed)
        );
        assert_eq!(
            timeout(Duration::from_millis(50), sender.end(vec![frame]))
                .await
                .unwrap(),
            Err(TransportError::Closed)
        );
    }

    #[test]
    fn hosted_routes_require_one_explicit_valid_address_without_changing_identity() {
        let url = Url::parse("https://runtime.sagemaker.us-east-1.amazonaws.com:8443/endpoints/test-endpoint/invocations-bidirectional-stream").unwrap();
        let (mut config, _) = config(url, small_limits());
        assert_eq!(
            config.validate(),
            Err(TransportError::Invalid(
                "hosted HTTPS requires one caller-resolved IP address"
            ))
        );
        for address in [
            "0.0.0.0:8443",
            "224.0.0.1:8443",
            "255.255.255.255:8443",
            "[::]:8443",
            "[ff02::1]:8443",
            "203.0.113.10:80",
        ] {
            config.connect_address = Some(address.parse().unwrap());
            assert!(config.validate().is_err(), "invalid address plan accepted");
        }
        let address: SocketAddr = "203.0.113.10:8443".parse().unwrap();
        config.connect_address = Some(address);
        config.validate().unwrap();
        assert_eq!(config.socket_address().unwrap(), address);
        assert_eq!(
            config.endpoint.host_str(),
            Some("runtime.sagemaker.us-east-1.amazonaws.com")
        );
        assert_eq!(
            config.headers.get("host").unwrap(),
            "runtime.sagemaker.us-east-1.amazonaws.com:8443"
        );
        assert!(!format!("{config:?}").contains("203.0.113.10"));
    }

    #[test]
    fn literal_loopback_routes_are_direct_and_cannot_be_overridden() {
        for (url, expected) in [
            (
                "http://127.0.0.1:12345/endpoints/test-endpoint/invocations-bidirectional-stream",
                "127.0.0.1:12345",
            ),
            (
                "https://[::1]:23456/endpoints/test-endpoint/invocations-bidirectional-stream",
                "[::1]:23456",
            ),
        ] {
            let (mut config, _) = config(Url::parse(url).unwrap(), small_limits());
            config.validate().unwrap();
            assert_eq!(
                config.socket_address().unwrap(),
                expected.parse::<SocketAddr>().unwrap()
            );
            config.connect_address = Some("127.0.0.1:8443".parse().unwrap());
            assert_eq!(
                config.validate(),
                Err(TransportError::Invalid(
                    "literal loopback route cannot be overridden"
                ))
            );
        }
    }

    #[tokio::test]
    async fn supplied_hosted_address_can_cancel_before_connect_without_dns() {
        // TEST-NET-3 is only a synthetic route value. Immediate cancellation on
        // this single-thread runtime occurs before the supervisor is polled.
        let url = Url::parse("https://runtime.sagemaker.us-east-1.amazonaws.com:8443/endpoints/test-endpoint/invocations-bidirectional-stream").unwrap();
        let (mut config, signer) = config(url, small_limits());
        config.connect_address = Some("203.0.113.10:8443".parse().unwrap());
        let transport = Transport::start(config, vec![inner(b"start")], signer).unwrap();
        transport.cancel();
        let report = timeout(Duration::from_secs(1), transport.completion().wait())
            .await
            .unwrap();
        assert_eq!(report.primary, Some(TransportError::Cancelled));
        assert!(!report.writer_joined && !report.reader_joined && !report.connection_joined);
        assert!(!report.forced_abort && report.cleanup.is_none());
    }

    #[test]
    fn configuration_is_bounded_and_debug_does_not_expose_authentication() {
        let url = Url::parse(
            "http://127.0.0.1:1/endpoints/test-endpoint/invocations-bidirectional-stream",
        )
        .unwrap();
        let (mut valid, _) = config(url, small_limits());
        valid.validate().unwrap();
        let rendered = format!("{valid:?}");
        for secret in [
            "SYNTHETICKEY",
            "synthetic-not-real-secret",
            "synthetic-token",
            "Credential=",
        ] {
            assert!(!rendered.contains(secret));
        }
        valid.endpoint = Url::parse(
            "http://example.com/endpoints/test-endpoint/invocations-bidirectional-stream",
        )
        .unwrap();
        assert!(valid.validate().is_err());
        for timeout in [Duration::ZERO, Duration::from_secs(421)] {
            let limits = TransportLimits {
                session_timeout: timeout,
                ..TransportLimits::default()
            };
            assert!(limits.validate().is_err());
        }
        for queue in [0, 257] {
            let limits = TransportLimits {
                input_queue_events: queue,
                ..TransportLimits::default()
            };
            assert!(limits.validate().is_err());
        }
        let limits = TransportLimits {
            response_queue_bytes: MAX_QUEUE_BYTES + 1,
            ..TransportLimits::default()
        };
        assert!(limits.validate().is_err());
    }

    #[tokio::test]
    async fn timer_end_race_has_no_control_after_close_and_dropped_receipt_is_safe() {
        let (url, peer) = peer(1024, |request, mut respond| async move {
            let mut body = request.into_body();
            let mut stream = respond
                .send_response(
                    Response::builder()
                        .header("content-type", MIME)
                        .body(())
                        .unwrap(),
                    false,
                )
                .unwrap();
            let mut raw = vec![];
            collect_upload(&mut body, &mut raw).await;
            peer_send(&mut stream, &[], true).await;
            uploaded_inner(&raw)
        })
        .await;
        let limits = TransportLimits {
            session_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(20),
            ..small_limits()
        };
        let (config, signer) = config(url, limits);
        let mut transport = Transport::start(config, vec![], signer).unwrap();
        let mut receiver = transport.take_receiver().unwrap();
        transport.ready().await.unwrap();
        let sender = transport.sender();
        let discarded = sender
            .send_control(inner(br#"{"type":"Finalize"}"#))
            .await
            .unwrap();
        drop(discarded); // Receipt is not the retained writer owner.
        let due = transport.ready.borrow().active_at.unwrap() + Duration::from_secs(5);
        sleep_until(due).await;
        sender
            .end(vec![inner(br#"{"type":"CloseStream"}"#)])
            .await
            .unwrap();
        assert_eq!(
            sender.send(inner(b"late")).await,
            Err(TransportError::Closed)
        );
        assert!(receiver.recv().await.is_none());
        assert_eq!(joined(transport.completion()).await.result(), Ok(()));
        let messages = peer.await.unwrap();
        assert_eq!(messages.first().unwrap(), &inner(br#"{"type":"Finalize"}"#));
        assert_eq!(
            messages.last().unwrap(),
            &inner(br#"{"type":"CloseStream"}"#)
        );
        assert!(messages.len() <= 3);
        if messages.len() == 3 {
            assert_eq!(messages[1], inner(br#"{"type":"KeepAlive"}"#));
        }
    }
}
