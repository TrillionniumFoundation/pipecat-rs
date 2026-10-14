//! One callback owns one complete physical image socket. Cancellation drops it;
//! no split sink, hidden writer, graceful close or replay may flush old pixels.
use super::{nonce, MAX_ID};
use crate::AgentError;
use futures_util::{SinkExt, StreamExt};
use pipecat_core::{Frame, FrameData, ImageRawFrame, ProcessorError};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::Instant,
};
use tokio_tungstenite::{
    tungstenite::{protocol::WebSocketConfig, Message},
    WebSocketStream,
};

pub const IMAGE_MAX_PIXELS_BYTES: usize = 8 * 1024 * 1024;
pub const IMAGE_HEADER_BYTES: usize = 512;
const PAIR_BYTES: usize = 1024;
const MAGIC: &[u8; 8] = b"PCIMG001";
type Socket = WebSocketStream<ImageIo>;
type PairFuture = Pin<Box<dyn Future<Output = Result<(Socket, Offer), ()>> + Send + Sync>>;

/// Bounded native loopback image mechanism. The CLI enables it only by opt-in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageChannelConfig {
    pub max_pixel_bytes: usize,
    pub max_dimension: u32,
    pub pairing_timeout: Duration,
    pub operation_timeout: Duration,
    pub disposal_timeout: Duration,
}
impl Default for ImageChannelConfig {
    fn default() -> Self {
        Self {
            max_pixel_bytes: IMAGE_MAX_PIXELS_BYTES,
            max_dimension: 4096,
            pairing_timeout: Duration::from_secs(2),
            operation_timeout: Duration::from_secs(5),
            disposal_timeout: Duration::from_secs(2),
        }
    }
}
impl ImageChannelConfig {
    pub fn validate(&self) -> Result<(), AgentError> {
        if self.max_pixel_bytes == 0
            || self.max_pixel_bytes > IMAGE_MAX_PIXELS_BYTES
            || self.max_dimension == 0
            || self.max_dimension > 4096
            || [
                self.pairing_timeout,
                self.operation_timeout,
                self.disposal_timeout,
            ]
            .into_iter()
            .any(|d| d.is_zero() || d > Duration::from_secs(60))
        {
            return Err(AgentError::Config("invalid image channel budgets"));
        }
        Ok(())
    }
}
/// Bounded ownership observations, not delivery/display acknowledgements.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImageChannelStatus {
    pub closed: bool,
    pub owner_alive: bool,
    pub pairing: bool,
    pub idle: bool,
    pub incarnation: u64,
    pub request_id: Option<u64>,
    pub leased: bool,
    pub open_streams: usize,
    /// Actual underlying TCP poll_write returned Pending while leased for image I/O.
    pub tcp_write_pending: u64,
    pub tcp_image_bytes_written: u64,
}
#[derive(Clone)]
struct Offer {
    incarnation: u64,
    token: String,
}
struct Request {
    id: u64,
    incarnation: Option<u64>,
}
struct State {
    closed: bool,
    owner_alive: bool,
    pairing: bool,
    offer: Offer,
    consumed: bool,
    rotate: bool,
    idle: Option<Socket>,
    request: Option<Request>,
    next_id: u64,
}
#[derive(Default)]
struct Stats {
    streams: AtomicUsize,
    pending: AtomicU64,
    bytes: AtomicU64,
}
pub(super) struct Images {
    session_id: String,
    address: String,
    config: ImageChannelConfig,
    state: Mutex<State>,
    stats: Arc<Stats>,
    pub changed: Notify,
}
/// Kept inside ReceiptSession and polled by that session's existing owner loop.
pub(super) struct ImageOwner {
    pub shared: Arc<Images>,
    listener: Option<Arc<TcpListener>>,
    pairing: Option<PairFuture>,
}
impl ImageOwner {
    pub fn new(
        listener: TcpListener,
        config: ImageChannelConfig,
        session_id: String,
    ) -> Result<Self, AgentError> {
        config.validate()?;
        let address = listener.local_addr()?;
        if !address.ip().is_loopback() {
            return Err(AgentError::Config("image listener must be loopback"));
        }
        let shared = Arc::new(Images {
            session_id,
            address: address.to_string(),
            config,
            state: Mutex::new(State {
                closed: false,
                owner_alive: true,
                pairing: false,
                offer: Offer {
                    incarnation: 1,
                    token: nonce()?,
                },
                consumed: false,
                rotate: false,
                idle: None,
                request: None,
                next_id: 0,
            }),
            stats: Arc::new(Stats::default()),
            changed: Notify::new(),
        });
        Ok(Self {
            shared,
            listener: Some(Arc::new(listener)),
            pairing: None,
        })
    }
    /// Advances only finite local state; a pending accept/upgrade retains its own
    /// absolute deadline when the outer audio select cancels next_event().
    pub fn advance(&mut self) -> Result<Option<Value>, AgentError> {
        if self.shared.state.lock().unwrap().closed {
            if self.listener.is_some() {
                self.close();
            }
            return Ok(None);
        }
        let rotate = self.shared.state.lock().unwrap().rotate;
        let fresh = if rotate { Some(nonce()?) } else { None };
        let notice = {
            let mut s = self.shared.state.lock().unwrap();
            if s.closed {
                return Ok(None);
            }
            if let Some(token) = fresh {
                if s.rotate
                    && !s.request.as_ref().is_some_and(|r| r.incarnation.is_some())
                    && s.idle.is_none()
                {
                    s.offer.incarnation = s
                        .offer
                        .incarnation
                        .checked_add(1)
                        .filter(|id| *id <= MAX_ID)
                        .ok_or(AgentError::Protocol("image incarnation exhausted"))?;
                    s.offer.token = token;
                    s.rotate = false;
                    s.consumed = false;
                    Some(self.shared.offer_locked(&s))
                } else {
                    None
                }
            } else {
                None
            }
        };
        if self.pairing.is_none() {
            let offer = {
                let mut s = self.shared.state.lock().unwrap();
                if s.closed
                    || s.consumed
                    || s.rotate
                    || s.idle.is_some()
                    || s.request.as_ref().is_some_and(|r| r.incarnation.is_some())
                {
                    None
                } else {
                    s.pairing = true;
                    Some(s.offer.clone())
                }
            };
            if let Some(offer) = offer {
                let listener = self.listener.as_ref().expect("live image listener").clone();
                self.pairing = Some(pair(listener, self.shared.clone(), offer));
            }
        }
        Ok(notice)
    }
    pub async fn next_event(&mut self) {
        let Some(pairing) = self.pairing.as_mut() else {
            std::future::pending::<()>().await;
            return;
        };
        let result = pairing.await;
        self.pairing = None;
        let mut rejected = None;
        {
            let mut s = self.shared.state.lock().unwrap();
            s.pairing = false;
            if let Ok((socket, offer)) = result {
                // Late handshake completion cannot resurrect a stopped or replaced incarnation.
                if !s.closed
                    && !s.rotate
                    && !s.consumed
                    && s.idle.is_none()
                    && !s.request.as_ref().is_some_and(|r| r.incarnation.is_some())
                    && s.offer.incarnation == offer.incarnation
                    && s.offer.token == offer.token
                {
                    s.consumed = true;
                    s.idle = Some(socket);
                } else {
                    rejected = Some(socket);
                }
            }
        }
        drop(rejected);
        self.shared.changed.notify_waiters();
    }
    pub fn close(&mut self) {
        self.shared.close();
        // Dispose accepted/pairing socket and listener before reporting owner settlement.
        self.pairing = None;
        self.listener = None;
        let mut s = self.shared.state.lock().unwrap();
        s.pairing = false;
        s.owner_alive = false;
        drop(s);
        self.shared.changed.notify_waiters();
    }
}
impl Drop for ImageOwner {
    fn drop(&mut self) {
        self.close();
    }
}

impl Images {
    fn offer_locked(&self, state: &State) -> Value {
        json!({"type":"image-channel-offer", "version":1, "session_id":self.session_id,
            "destination":null, "incarnation":state.offer.incarnation, "pairing_nonce":state.offer.token,
            "address":self.address, "max_pixel_bytes":self.config.max_pixel_bytes,
            "max_dimension":self.config.max_dimension, "encoding":"PCIMG001"})
    }
    pub fn offer(&self) -> Value {
        self.offer_locked(&self.state.lock().unwrap())
    }
    pub fn status(&self) -> ImageChannelStatus {
        let s = self.state.lock().unwrap();
        ImageChannelStatus {
            closed: s.closed,
            owner_alive: s.owner_alive,
            pairing: s.pairing,
            idle: s.idle.is_some(),
            incarnation: s.offer.incarnation,
            request_id: s.request.as_ref().map(|r| r.id),
            leased: s.request.as_ref().is_some_and(|r| r.incarnation.is_some()),
            open_streams: self.stats.streams.load(Ordering::SeqCst),
            tcp_write_pending: self.stats.pending.load(Ordering::SeqCst),
            tcp_image_bytes_written: self.stats.bytes.load(Ordering::SeqCst),
        }
    }
    pub fn close(&self) {
        let idle = {
            let mut s = self.state.lock().unwrap();
            s.closed = true;
            s.idle.take()
        };
        drop(idle); // Never flush a potentially ambiguous stream during cancellation.
        self.changed.notify_waiters();
    }
    pub async fn settled(&self) -> Result<(), ProcessorError> {
        let deadline = Instant::now() + self.config.disposal_timeout;
        loop {
            if Instant::now() >= deadline {
                return Err(ProcessorError::new(
                    "image disposal deadline exceeded; join is unverified",
                ));
            }
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let status = self.status();
            if !status.owner_alive && status.request_id.is_none() && status.open_streams == 0 {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| {
                    ProcessorError::new("image disposal deadline exceeded; join is unverified")
                })?;
        }
    }
    pub async fn write(self: &Arc<Self>, frame: &Frame) -> Result<(), ProcessorError> {
        let pixels = validate(frame, &self.config)?;
        let deadline = Instant::now() + self.config.operation_timeout;
        let id = {
            let mut s = self.state.lock().unwrap();
            if s.closed {
                return Err(ProcessorError::new("image admission is closed"));
            }
            if s.request.is_some() {
                return Err(ProcessorError::new("image callback budget exhausted"));
            }
            s.next_id = s
                .next_id
                .checked_add(1)
                .filter(|id| *id <= MAX_ID)
                .ok_or_else(|| ProcessorError::new("image request identity exhausted"))?;
            let id = s.next_id;
            s.request = Some(Request {
                id,
                incarnation: None,
            });
            id
        };
        let mut lease = Lease {
            shared: self.clone(),
            id,
            socket: None,
        };
        let incarnation = loop {
            if Instant::now() >= deadline {
                return Err(ProcessorError::new(
                    "image operation timed out waiting for pairing",
                ));
            }
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut s = self.state.lock().unwrap();
                if s.closed {
                    return Err(ProcessorError::new("image admission is closed"));
                }
                if let Some(mut socket) = s.idle.take() {
                    let incarnation = s.offer.incarnation;
                    s.request
                        .as_mut()
                        .expect("registered image request")
                        .incarnation = Some(incarnation);
                    socket.get_mut().image = true;
                    lease.socket = Some(socket);
                    break incarnation;
                }
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| {
                    ProcessorError::new("image operation timed out waiting for pairing")
                })?;
        };
        let header = serde_json::to_vec(&json!({"version":1,"session_id":self.session_id,
            "destination":null,"incarnation":incarnation,"request_id":id,
            "width":pixels.size.0,"height":pixels.size.1,"format":pixels.format,
            "interruptible":frame.interruptible,"pixel_bytes":pixels.image.len()}))
        .map_err(|_| ProcessorError::new("image header encoding failed"))?;
        if header.len() > IMAGE_HEADER_BYTES {
            return Err(ProcessorError::new("image header exceeds budget"));
        }
        let size = 12usize
            .checked_add(header.len())
            .and_then(|n| n.checked_add(pixels.image.len()))
            .ok_or_else(|| ProcessorError::new("encoded image size overflow"))?;
        let mut message = Vec::with_capacity(size);
        message.extend_from_slice(MAGIC);
        message.extend_from_slice(&(header.len() as u32).to_be_bytes());
        message.extend_from_slice(&header);
        message.extend_from_slice(&pixels.image);
        // The future borrows the guard-owned whole socket. Any return/drop first
        // drops this send future, then the guard disposes the socket without polling.
        let result = {
            let send = lease
                .socket
                .as_mut()
                .expect("checked out socket")
                .send(Message::Binary(message.into()));
            tokio::pin!(send);
            loop {
                if Instant::now() >= deadline {
                    break Err(ProcessorError::new("image physical write timed out"));
                }
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.state.lock().unwrap().closed {
                    break Err(ProcessorError::new("image session closed during write"));
                }
                tokio::select! { biased;
                    _=&mut changed=>{},
                    _=tokio::time::sleep_until(deadline)=>break Err(ProcessorError::new("image physical write timed out")),
                    result=&mut send=>break result.map_err(|_|ProcessorError::new("image physical write failed")),
                }
            }
        };
        result?;
        lease.complete()
    }
}
struct Lease {
    shared: Arc<Images>,
    id: u64,
    socket: Option<Socket>,
}
impl Lease {
    fn complete(&mut self) -> Result<(), ProcessorError> {
        let mut s = self.shared.state.lock().unwrap();
        if s.closed {
            return Err(ProcessorError::new(
                "image session closed after local write",
            ));
        }
        let mut socket = self.socket.take().expect("successful image owns stream");
        socket.get_mut().image = false;
        s.idle = Some(socket);
        s.request = None;
        drop(s);
        self.shared.changed.notify_waiters();
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        // Explicit ordering is essential: Rust calls Drop before dropping fields.
        let owned = self.socket.is_some();
        drop(self.socket.take());
        let mut s = self.shared.state.lock().unwrap();
        if s.request.as_ref().is_some_and(|r| r.id == self.id) {
            s.request = None;
            if owned && !s.closed {
                s.rotate = true;
            }
        }
        drop(s);
        self.shared.changed.notify_waiters();
    }
}
fn validate<'a>(
    frame: &'a Frame,
    config: &ImageChannelConfig,
) -> Result<&'a ImageRawFrame, ProcessorError> {
    frame
        .require_no_native_llm_continuation()
        .map_err(|_| ProcessorError::new("unconsumed native continuation"))?;
    frame
        .require_wire_pts()
        .map_err(|_| ProcessorError::new("pipeline PTS needs explicit rebase"))?;
    if frame.transport_destination.is_some() {
        return Err(ProcessorError::new(
            "image channel has one default destination",
        ));
    }
    let pixels = match &frame.data {
        FrameData::OutputImage(i) => i,
        FrameData::OutputImageDetailed(i) => &i.image,
        FrameData::UrlImage(i) => &i.image.image,
        FrameData::AssistantImage(i) => &i.image.image,
        _ => return Err(ProcessorError::new("unsupported image frame variant")),
    };
    let channels = match pixels.format.as_deref() {
        Some("RGB") => 3usize,
        Some("RGBA") => 4usize,
        _ => return Err(ProcessorError::new("image channel requires RGB or RGBA")),
    };
    let (width, height) = pixels.size;
    let size = (width as usize)
        .checked_mul(height as usize)
        .and_then(|n| n.checked_mul(channels));
    if width == 0
        || height == 0
        || width > config.max_dimension
        || height > config.max_dimension
        || size != Some(pixels.image.len())
        || pixels.image.len() > config.max_pixel_bytes
    {
        return Err(ProcessorError::new(
            "image dimensions, length or payload budget is invalid",
        ));
    }
    Ok(pixels)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Pair {
    #[serde(rename = "type")]
    kind: String,
    version: u8,
    session_id: String,
    destination: Value,
    incarnation: u64,
    pairing_nonce: String,
}
fn pair(listener: Arc<TcpListener>, shared: Arc<Images>, offer: Offer) -> PairFuture {
    Box::pin(async move {
        // Idle accept has no task or timer churn. Once a peer arrives, one deadline
        // covers HTTP upgrade, pair receive and pair confirmation write.
        let (stream, address) = listener.accept().await.map_err(|_| ())?;
        if !address.ip().is_loopback() {
            return Err(());
        }
        let deadline = Instant::now() + shared.config.pairing_timeout;
        let max_write = shared
            .config
            .max_pixel_bytes
            .checked_add(IMAGE_HEADER_BYTES + 12 + 1024)
            .ok_or(())?;
        let config = WebSocketConfig::default()
            .read_buffer_size(4096)
            .write_buffer_size(0)
            .max_write_buffer_size(max_write)
            .max_message_size(Some(PAIR_BYTES))
            .max_frame_size(Some(PAIR_BYTES));
        let io = ImageIo::new(stream, shared.stats.clone());
        tokio::time::timeout_at(deadline, async {
            let mut socket = tokio_tungstenite::accept_async_with_config(io, Some(config))
                .await
                .map_err(|_| ())?;
            let Message::Text(text) = socket.next().await.ok_or(())?.map_err(|_| ())? else {
                return Err(());
            };
            if text.len() > PAIR_BYTES {
                return Err(());
            }
            let pair: Pair = serde_json::from_str(&text).map_err(|_| ())?;
            if pair.kind != "image-channel-pair"
                || pair.version != 1
                || pair.session_id != shared.session_id
                || !pair.destination.is_null()
                || pair.incarnation != offer.incarnation
                || pair.pairing_nonce != offer.token
            {
                return Err(());
            }
            // Installation rechecks this offer after confirmation; confirmation is
            // not permission to install a stale socket after a concurrent stop.
            socket
                .send(Message::Text(
                    json!({"type":"image-channel-paired","version":1,
                "session_id":shared.session_id,"incarnation":offer.incarnation})
                    .to_string()
                    .into(),
                ))
                .await
                .map_err(|_| ())?;
            Ok((socket, offer))
        })
        .await
        .map_err(|_| ())?
    })
}
struct ImageIo {
    stream: Option<TcpStream>,
    stats: Arc<Stats>,
    image: bool,
}
impl ImageIo {
    fn new(stream: TcpStream, stats: Arc<Stats>) -> Self {
        stats.streams.fetch_add(1, Ordering::SeqCst);
        Self {
            stream: Some(stream),
            stats,
            image: false,
        }
    }
}
impl Drop for ImageIo {
    fn drop(&mut self) {
        drop(self.stream.take());
        self.stats.streams.fetch_sub(1, Ordering::SeqCst);
    }
}
impl AsyncRead for ImageIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("live image TCP")).poll_read(cx, buf)
    }
}
impl AsyncWrite for ImageIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(self.stream.as_mut().expect("live image TCP")).poll_write(cx, buf);
        if self.image {
            match result {
                Poll::Pending => {
                    self.stats.pending.fetch_add(1, Ordering::SeqCst);
                }
                Poll::Ready(Ok(n)) => {
                    self.stats.bytes.fetch_add(n as u64, Ordering::SeqCst);
                }
                _ => {}
            }
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("live image TCP")).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().expect("live image TCP")).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::ImageRawFrame;
    #[tokio::test]
    async fn ready_notifications_do_not_extend_fixed_wait_or_disposal_deadlines() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut owner = ImageOwner::new(
            listener,
            ImageChannelConfig {
                operation_timeout: Duration::from_millis(30),
                disposal_timeout: Duration::from_millis(30),
                ..Default::default()
            },
            "a".repeat(32),
        )
        .unwrap();
        let shared = owner.shared.clone();
        let frame = Frame::new(FrameData::OutputImage(ImageRawFrame {
            image: vec![1; 12],
            size: (2, 2),
            format: Some("RGB".into()),
        }));
        let start = Instant::now();
        let storm = async {
            while start.elapsed() < Duration::from_millis(100) {
                shared.changed.notify_waiters();
                tokio::task::yield_now().await;
            }
        };
        let operation = async {
            let result = shared.write(&frame).await;
            (result, start.elapsed())
        };
        let ((result, elapsed), ()) = tokio::join!(operation, storm);
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("timed out waiting for pairing"));
        assert!(elapsed < Duration::from_millis(100));
        let start = Instant::now();
        let storm = async {
            while start.elapsed() < Duration::from_millis(100) {
                shared.changed.notify_waiters();
                tokio::task::yield_now().await;
            }
        };
        let disposal = async {
            let result = shared.settled().await;
            (result, start.elapsed())
        };
        let ((result, elapsed), ()) = tokio::join!(disposal, storm);
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("join is unverified"));
        assert!(elapsed < Duration::from_millis(100));
        owner.close();
        shared.settled().await.unwrap();
    }
    #[tokio::test]
    async fn invalid_image_configuration_drops_supplied_listener() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        assert!(ImageOwner::new(
            listener,
            ImageChannelConfig {
                max_pixel_bytes: usize::MAX,
                ..Default::default()
            },
            "a".repeat(32)
        )
        .is_err());
        assert!(TcpStream::connect(address).await.is_err());
    }
}
