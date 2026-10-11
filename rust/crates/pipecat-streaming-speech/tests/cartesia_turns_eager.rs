//! Real Cartesia WebSocket events through aggregation, HTTP inference and effects.
//! Peers are loopback protocol fixtures; no provider credentials or models are used.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_streaming_speech::*;
use pipecat_turns::*;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, Semaphore},
    task::{JoinHandle, JoinSet},
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
};

async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("Cartesia eager fixture deadline")
}

async fn wait(mut predicate: impl FnMut() -> bool) {
    within(async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

struct Batch {
    messages: Vec<Message>,
    flushed: oneshot::Sender<()>,
}

#[cfg_attr(not(feature = "cartesia-turns-eager"), allow(dead_code))]
struct Connection {
    request: Request,
    batches: mpsc::Sender<Batch>,
    wire: mpsc::Receiver<Message>,
    auto_close: Arc<AtomicBool>,
}

impl Connection {
    async fn messages(&self, messages: Vec<Message>) {
        let (flushed, done) = oneshot::channel();
        within(self.batches.send(Batch { messages, flushed }))
            .await
            .unwrap();
        within(done).await.unwrap();
    }

    async fn batch(&self, values: Vec<Value>) {
        self.messages(
            values
                .into_iter()
                .map(|v| Message::Text(v.to_string().into()))
                .collect(),
        )
        .await;
    }

    async fn event(&self, value: Value) {
        self.batch(vec![value]).await;
    }

    async fn wire(&mut self) -> Message {
        within(self.wire.recv()).await.expect("Cartesia wire event")
    }
}

struct WsPeer {
    client: CartesiaTurnsClient,
    connections: mpsc::Receiver<Connection>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Drop for WsPeer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.task.abort();
    }
}

impl WsPeer {
    #[allow(clippy::result_large_err)]
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = CartesiaTurnsConfig::new("loopback-cartesia-key");
        config.endpoint = format!(
            "ws://{}/stt/turns/websocket",
            listener.local_addr().unwrap()
        );
        config.limits.connect_timeout = Duration::from_secs(2);
        config.limits.write_timeout = Duration::from_secs(2);
        config.limits.close_timeout = Duration::from_secs(2);
        let (tx, connections) = mpsc::channel(16);
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break; };
                        let tx = tx.clone();
                        children.spawn(async move {
                            let captured = Arc::new(Mutex::new(None));
                            let capture = captured.clone();
                            let Ok(mut socket) = accept_hdr_async(stream, move |request: &Request, response: Response| {
                                *capture.lock().unwrap() = Some(request.clone());
                                Ok(response)
                            }).await else { return; };
                            let request = captured.lock().unwrap().take().unwrap();
                            let (batches, mut commands) = mpsc::channel::<Batch>(32);
                            let (tx_wire, wire) = mpsc::channel(128);
                            let auto_close = Arc::new(AtomicBool::new(true));
                            let close = auto_close.clone();
                            if tx.send(Connection { request, batches, wire, auto_close }).await.is_err() {
                                return;
                            }
                            loop {
                                tokio::select! {
                                    batch = commands.recv() => {
                                        let Some(batch) = batch else { return; };
                                        for message in batch.messages {
                                            if socket.feed(message).await.is_err() { return; }
                                        }
                                        if socket.flush().await.is_err() { return; }
                                        let _ = batch.flushed.send(());
                                    }
                                    message = socket.next() => {
                                        let Some(Ok(message)) = message else { return; };
                                        let closed = matches!(message, Message::Close(_));
                                        let finish = message.to_text().is_ok_and(|text| text == r#"{"type":"close"}"#);
                                        if tx_wire.send(message).await.is_err() { return; }
                                        if closed { let _ = socket.flush().await; return; }
                                        if finish && close.load(Ordering::Acquire) {
                                            let _ = socket.close(None).await;
                                            return;
                                        }
                                    }
                                }
                            }
                        });
                    }
                    _ = children.join_next(), if !children.is_empty() => (),
                }
            }
            children.shutdown().await;
        });
        Self {
            client: CartesiaTurnsClient::new(config).unwrap(),
            connections,
            stop: Some(stop),
            task,
        }
    }

    async fn connection(&mut self) -> Connection {
        let connection = within(self.connections.recv()).await.unwrap();
        assert_eq!(connection.request.uri().path(), "/stt/turns/websocket");
        assert_eq!(
            connection.request.headers()["x-api-key"],
            "loopback-cartesia-key"
        );
        assert_eq!(
            connection.request.headers()["cartesia-version"],
            "2026-03-01"
        );
        // Deliberately reused across all sockets. Provider IDs are diagnostics.
        connection
            .event(json!({"type":"connected","request_id":"reused-id"}))
            .await;
        connection
    }
}

type HttpRequest = (Value, oneshot::Sender<String>);

struct HttpPeer {
    endpoint: String,
    requests: mpsc::Receiver<HttpRequest>,
    count: Arc<AtomicUsize>,
    automatic: Arc<Mutex<Option<String>>>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Drop for HttpPeer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        self.task.abort();
    }
}

impl HttpPeer {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::channel(32);
        let count = Arc::new(AtomicUsize::new(0));
        let counter = count.clone();
        let automatic = Arc::new(Mutex::new(None::<String>));
        let automatic_reply = automatic.clone();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => {
                        let Ok((mut socket, _)) = accepted else { break; };
                        let tx = tx.clone();
                        let count = counter.clone();
                        let automatic = automatic_reply.clone();
                        children.spawn(async move {
                            let request = read_request(&mut socket).await;
                            count.fetch_add(1, Ordering::SeqCst);
                            let (send, receive) = oneshot::channel();
                            if tx.send((request, send)).await.is_err() { return; }
                            let preset = automatic.lock().unwrap().clone();
                            let body = match preset {
                                Some(body) => body,
                                None => match receive.await { Ok(body) => body, Err(_) => return },
                            };
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()
                            );
                            let _ = socket.write_all(response.as_bytes()).await;
                        });
                    }
                    _ = children.join_next(), if !children.is_empty() => (),
                }
            }
            children.shutdown().await;
        });
        Self {
            endpoint,
            requests,
            count,
            task,
            automatic,
            stop: Some(stop),
        }
    }

    async fn request(&mut self) -> HttpRequest {
        within(self.requests.recv())
            .await
            .expect("real HTTP request")
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    let header_end = loop {
        let read = socket.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0);
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(offset) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break offset + 4;
        }
    };
    let length: usize = String::from_utf8_lossy(&bytes[..header_end])
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse().unwrap())
        })
        .expect("HTTP request content length");
    while bytes.len() < header_end + length {
        let read = socket.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0);
        bytes.extend_from_slice(&buffer[..read]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
}

fn answer(text: &str) -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({
            "choices":[{"delta":{"content":text},"finish_reason":"stop"}]
        })
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PauseAt {
    #[cfg(feature = "cartesia-turns-eager")]
    Prediction,
    #[cfg(feature = "cartesia-turns-eager")]
    Context,
    #[cfg(feature = "cartesia-turns-eager")]
    Final,
    #[cfg(feature = "cartesia-turns-eager")]
    Stop,
    #[cfg(feature = "cartesia-turns-eager")]
    Boundary,
    #[cfg(feature = "cartesia-turns-eager")]
    Receipts,
    #[cfg(feature = "cartesia-turns-eager")]
    StartFlush,
    #[cfg(feature = "cartesia-turns-eager")]
    PredictionAndReceipts,
}

struct Probe {
    name: &'static str,
    frames: Mutex<Vec<DirectedFrame>>,
    pause: Option<PauseAt>,
    armed: AtomicBool,
    arrived: Semaphore,
    proceed: Semaphore,
}

impl Probe {
    fn new(name: &'static str, pause: Option<PauseAt>) -> Self {
        Self {
            name,
            frames: Mutex::new(Vec::new()),
            pause,
            armed: AtomicBool::new(pause.is_some()),
            arrived: Semaphore::new(0),
            proceed: Semaphore::new(0),
        }
    }

    fn count(&self, predicate: impl Fn(&FrameData) -> bool) -> usize {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter(|f| f.direction == Direction::Downstream && predicate(&f.frame.data))
            .count()
    }

    fn observed(&self, id: u64) -> bool {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .any(|f| f.direction == Direction::Downstream && f.frame.id == id)
    }

    #[cfg(feature = "cartesia-turns-eager")]
    async fn paused(&self) {
        within(self.arrived.acquire()).await.unwrap().forget();
    }

    #[cfg(feature = "cartesia-turns-eager")]
    fn release(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.proceed.add_permits(1);
    }

    #[cfg(feature = "cartesia-turns-eager")]
    fn native(&self, predicate: impl Fn(&NativeEagerSource) -> bool) -> Frame {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|f| {
                f.frame
                    .native_eager_source()
                    .filter(|v| predicate(v))
                    .map(|_| f.frame.clone())
            })
            .expect("genuine Cartesia source carrier")
    }

    #[cfg(feature = "cartesia-turns-eager")]
    fn capability(&self) -> NativeSpeculation {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|f| {
                matches!(f.frame.data, FrameData::LlmContextDetailed(_))
                    .then(|| f.frame.native_speculation().cloned())
                    .flatten()
            })
            .expect("adopted speculative context")
    }
}

#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        self.name
    }

    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.frames
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        #[cfg(feature = "cartesia-turns-eager")]
        if direction == Direction::Downstream
            && matches!(
                self.pause,
                Some(PauseAt::Receipts | PauseAt::PredictionAndReceipts)
            )
            && self.armed.load(Ordering::SeqCst)
            && (matches!(
                frame.native_eager_source(),
                Some(NativeEagerSource::Final(_) | NativeEagerSource::Stop { .. })
            ) || (self.pause == Some(PauseAt::PredictionAndReceipts)
                && matches!(
                    frame.native_eager_source(),
                    Some(NativeEagerSource::Prediction(_))
                )))
        {
            // Retain genuine native receipts for explicit redelivery without
            // blocking unrelated system or ordinary traffic at this recorder.
            self.arrived.add_permits(1);
            return Ok(());
        }
        let matches = match self.pause {
            #[cfg(feature = "cartesia-turns-eager")]
            Some(PauseAt::Prediction) => matches!(frame.data, FrameData::EagerTranscription(_)),
            #[cfg(feature = "cartesia-turns-eager")]
            Some(PauseAt::Context) => {
                matches!(&frame.data, FrameData::LlmContextDetailed(v) if v.speculation)
            }
            #[cfg(feature = "cartesia-turns-eager")]
            Some(PauseAt::Final) => matches!(
                frame.native_eager_source(),
                Some(NativeEagerSource::Final(_))
            ),
            #[cfg(feature = "cartesia-turns-eager")]
            Some(PauseAt::Stop) => matches!(
                frame.native_eager_source(),
                Some(NativeEagerSource::Stop { .. })
            ),
            #[cfg(feature = "cartesia-turns-eager")]
            Some(PauseAt::Boundary) => matches!(frame.data, FrameData::UserStoppedSpeaking),
            #[cfg(feature = "cartesia-turns-eager")]
            Some(PauseAt::Receipts | PauseAt::PredictionAndReceipts) => false,
            #[cfg(feature = "cartesia-turns-eager")]
            Some(PauseAt::StartFlush) => matches!(&frame.data, FrameData::FlushDetailed(f)
                if f.origin.as_deref() == Some("pipecat.cartesia-turns.turn-start") && !f.returning),
            #[cfg(not(feature = "cartesia-turns-eager"))]
            Some(_) => false,
            None => false,
        };
        if direction == Direction::Downstream && matches && self.armed.swap(false, Ordering::SeqCst)
        {
            self.arrived.add_permits(1);
            self.proceed.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}

struct Owner {
    processor: Arc<CartesiaTurnsProcessor>,
    context: Mutex<Option<ProcessorContext>>,
}

#[async_trait]
impl FrameProcessor for Owner {
    fn name(&self) -> &str {
        self.processor.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.processor.health()
    }

    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        self.processor.setup(start, ctx).await
    }

    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.processor.process(frame, direction, ctx).await
    }

    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.processor.prepare_end(ctx).await
    }

    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.processor.cleanup().await
    }
}

/// Delegates to the actual aggregator; the labeled flush advances its real clock.
struct ClockedUser {
    user: Arc<LLMUserAggregator>,
    applied: Mutex<Vec<u64>>,
    context: Mutex<Option<ProcessorContext>>,
}

#[async_trait]
impl FrameProcessor for ClockedUser {
    fn name(&self) -> &str {
        self.user.name()
    }

    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        self.user.setup(start, ctx).await
    }

    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let id = frame.id;
        if direction == Direction::Downstream
            && matches!(&frame.data, FrameData::FlushDetailed(v) if v.origin.as_deref() == Some("cartesia-eager-clock"))
        {
            let millis = frame.metadata["advance_ms"].as_u64().unwrap();
            self.user
                .advance(Duration::from_millis(millis))?
                .emit(ctx)
                .await?;
            ctx.push(frame, direction).await?;
        } else if direction == Direction::Downstream
            && frame.metadata.contains_key("cartesia-exact-time-ms")
        {
            let millis = frame.metadata["cartesia-exact-time-ms"].as_u64().unwrap();
            self.user
                .process_at(frame, direction, Duration::from_millis(millis))?
                .emit(ctx)
                .await?;
        } else {
            self.user.process(frame, direction, ctx).await?;
        }
        self.applied.lock().unwrap().push(id);
        Ok(())
    }

    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.user.prepare_end(ctx).await
    }

    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.user.cleanup().await
    }
}

#[cfg_attr(not(feature = "cartesia-turns-eager"), allow(dead_code))]
struct Harness {
    source: Arc<Owner>,
    upstream: Arc<Probe>,
    before: Arc<Probe>,
    after: Arc<Probe>,
    output: Arc<Probe>,
    settled: Arc<Probe>,
    user: Arc<ClockedUser>,
    pair: LLMContextAggregatorPair,
    llm: Arc<LlmProcessor>,
    worker: PipelineWorker,
    connection: Connection,
}

impl Harness {
    async fn new(
        ws: &mut WsPeer,
        http: &HttpPeer,
        eager_source: bool,
        user_config: UserAggregatorConfig,
        pause: Option<PauseAt>,
        provider: Provider,
        tools: Option<Arc<dyn FrameProcessor>>,
    ) -> Self {
        let processor = CartesiaTurnsProcessor::new(ws.client.clone())
            .with_user_id("provider-speaker")
            .unwrap()
            .with_should_interrupt(false);
        #[cfg(feature = "cartesia-turns-eager")]
        let processor = if eager_source {
            processor.with_eager_turns()
        } else {
            processor
        };
        #[cfg(not(feature = "cartesia-turns-eager"))]
        assert!(!eager_source, "eager builder is feature gated");
        let source = Arc::new(Owner {
            processor: Arc::new(processor),
            context: Mutex::new(None),
        });
        let upstream = Arc::new(Probe::new("CartesiaUpstream", None));
        #[cfg(feature = "cartesia-turns-eager")]
        let before_pause = pause.filter(|p| {
            matches!(
                p,
                PauseAt::Prediction
                    | PauseAt::Final
                    | PauseAt::Stop
                    | PauseAt::Receipts
                    | PauseAt::StartFlush
                    | PauseAt::PredictionAndReceipts
            )
        });
        #[cfg(not(feature = "cartesia-turns-eager"))]
        let before_pause = pause;
        #[cfg(feature = "cartesia-turns-eager")]
        let after_pause = pause.filter(|p| matches!(p, PauseAt::Context | PauseAt::Boundary));
        #[cfg(not(feature = "cartesia-turns-eager"))]
        let after_pause = pause;
        let before = Arc::new(Probe::new("CartesiaBeforeAdoption", before_pause));
        let after = Arc::new(Probe::new("CartesiaAfterAdoption", after_pause));
        let output = Arc::new(Probe::new("CartesiaCompletionOutput", None));
        let settled = Arc::new(Probe::new("CartesiaSettledHistory", None));
        let pair =
            LLMContextAggregatorPair::new(Context::default(), user_config, Default::default());
        let user = Arc::new(ClockedUser {
            user: pair.user(),
            applied: Mutex::new(Vec::new()),
            context: Mutex::new(None),
        });
        let mut config = ClientConfig::new(provider, "loopback-llm-key");
        config.endpoint = http.endpoint.clone();
        let llm = Arc::new(LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            CompletionRequest::new(
                if provider == Provider::Gemini {
                    "gemini-3-pro"
                } else {
                    "loopback"
                },
                Context::default(),
            ),
        ));
        let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![
            upstream.clone(),
            source.clone(),
            before.clone(),
            user.clone(),
            after.clone(),
            llm.clone(),
        ];
        if let Some(tools) = tools {
            processors.push(tools);
        }
        processors.extend([
            output.clone() as Arc<dyn FrameProcessor>,
            pair.assistant(),
            settled.clone(),
        ]);
        let starting = tokio::spawn(async move {
            Pipeline::new(processors)
                .start(PipelineConfig {
                    start: StartFrame {
                        enable_usage_metrics: true,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
                .unwrap()
        });
        let connection = ws.connection().await;
        let result = Self {
            source,
            upstream,
            before,
            after,
            output,
            settled,
            user,
            pair,
            llm,
            worker: within(starting).await.unwrap(),
            connection,
        };
        result.barrier(false).await;
        result
    }

    async fn send(&self, frame: Frame) {
        within(self.worker.sender().send(frame, Direction::Downstream))
            .await
            .unwrap();
    }

    async fn barrier(&self, completions: bool) {
        let frame = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            event: None,
            returning: false,
            origin: Some(
                if completions {
                    "pipecat.flows"
                } else {
                    "cartesia-eager-fixture"
                }
                .into(),
            ),
        }));
        let id = frame.id;
        self.send(frame).await;
        wait(|| {
            if completions {
                self.settled.observed(id)
            } else {
                self.after.observed(id)
            }
        })
        .await;
    }

    async fn audio(&mut self) {
        self.send(Frame::new(FrameData::InputAudio(
            AudioRawFrame::new(vec![7; 320], 16000, 1).unwrap(),
        )))
        .await;
        assert_eq!(
            self.connection.wire().await,
            Message::Binary(vec![7; 320].into())
        );
    }

    async fn start(&self) {
        let prior = self.before.count(|v| {
            matches!(v, FrameData::FlushDetailed(f)
            if f.origin.as_deref() == Some("pipecat.cartesia-turns.turn-start") && !f.returning)
        });
        self.connection
            .event(json!({"type":"turn.start","request_id":"reused-id","turn_id":"untrusted"}))
            .await;
        wait(|| {
            self.before.count(|v| {
                matches!(v, FrameData::FlushDetailed(f)
            if f.origin.as_deref() == Some("pipecat.cartesia-turns.turn-start") && !f.returning)
            }) > prior
        })
        .await;
        let event = self
            .before
            .frames
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|f| match &f.frame.data {
                FrameData::FlushDetailed(flush)
                    if flush.origin.as_deref() == Some("pipecat.cartesia-turns.turn-start") =>
                {
                    flush.event.clone()
                }
                _ => None,
            })
            .unwrap();
        within(event.wait()).await;
        self.barrier(false).await;
    }

    fn user_history(&self) -> Vec<String> {
        self.pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.role == pipecat_services::Role::User)
            .map(|m| match &m.content {
                Some(pipecat_services::Content::Text(text)) => text.clone(),
                value => panic!("unexpected user content: {value:?}"),
            })
            .collect()
    }

    async fn finish(self, allow_provider_error: bool) {
        self.worker.end().await.unwrap();
        let (_, report) = within(self.worker.collect()).await.unwrap();
        if !allow_provider_error {
            assert!(report.errors.is_empty(), "{:?}", report.errors);
        }
        for frame in self.after.frames.lock().unwrap().iter() {
            assert!(
                frame.frame.native_eager_source().is_none(),
                "source carrier escaped aggregation"
            );
        }
    }
}

/// This control runs both with the feature absent and with the builder unused.
#[tokio::test]
async fn ordinary_constructor_keeps_committed_only_websocket_to_http_behavior() {
    for eager_consumer in [false, true] {
        let mut ws = WsPeer::new().await;
        let mut http = HttpPeer::new().await;
        let config = UserAggregatorConfig {
            eager: eager_consumer.then(EagerUserTurnConfig::default),
            ..Default::default()
        };
        let mut harness =
            Harness::new(&mut ws, &http, false, config, None, Provider::OpenAi, None).await;
        harness.audio().await;
        harness.start().await;
        *http.automatic.lock().unwrap() = Some(answer("Unexpected speculative control response."));
        harness
            .connection
            .batch(vec![
                json!({"type":"turn.eager_end","transcript":"must not speculate"}),
                json!({"type":"turn.resume"}),
                json!({"type":"turn.update","transcript":"reader-settled"}),
            ])
            .await;
        wait(|| {
            harness.before.count(
                |v| matches!(v, FrameData::InterimTranscription(t) if t.text == "reader-settled"),
            ) == 1
                || harness
                    .before
                    .count(|v| matches!(v, FrameData::EagerTranscription(_)))
                    > 0
                || !harness.worker.errors().is_empty()
        })
        .await;
        harness.barrier(true).await;
        let unexpected = http.count();
        let eager_frames = harness
            .before
            .count(|v| matches!(v, FrameData::EagerTranscription(_)));
        let control_errors = harness.worker.errors().len();
        if unexpected != 0 || eager_frames != 0 || control_errors != 0 {
            harness.finish(true).await;
            panic!("ordinary constructor speculated: requests={unexpected}, eager_frames={eager_frames}, errors={control_errors}");
        }
        *http.automatic.lock().unwrap() = None;
        assert_eq!(
            harness.before.count(|v| matches!(
                v,
                FrameData::EagerTranscription(_) | FrameData::EagerEndOfTurnCancel
            )),
            0
        );
        assert!(harness
            .before
            .frames
            .lock()
            .unwrap()
            .iter()
            .all(|v| v.frame.native_eager_source().is_none()));
        for directed in harness.before.frames.lock().unwrap().iter() {
            if let FrameData::SttMetadata(metadata) = &directed.frame.data {
                assert_eq!(
                    metadata.base.user_turn_strategies,
                    Some(UserTurnStrategies {
                        start: Some(vec![UserTurnStrategy::External {
                            enable_interruptions: false
                        }]),
                        stop: Some(vec![UserTurnStrategy::External {
                            enable_interruptions: false
                        }]),
                    })
                );
            }
        }
        let result =
            json!({"type":"turn.end","transcript":"Actual final.","language":"fr","extra":17});
        harness.connection.event(result.clone()).await;
        let (request, reply) = http.request().await;
        assert_eq!(request["messages"][0]["content"], "Actual final.");
        reply.send(answer("Committed.")).unwrap();
        harness.barrier(true).await;
        assert_eq!(harness.user_history(), ["Actual final."]);
        assert_eq!(http.count(), 1);
        assert_eq!(
            harness
                .output
                .count(|v| matches!(v, FrameData::LlmText(t) if t.text == "Committed.")),
            1
        );
        {
            let frames = harness.before.frames.lock().unwrap();
            let final_index = frames
                .iter()
                .position(|f| matches!(f.frame.data, FrameData::Transcription(_)))
                .unwrap();
            let stop_index = frames
                .iter()
                .position(|f| matches!(f.frame.data, FrameData::ProposedUserStoppedSpeaking))
                .unwrap();
            assert!(final_index < stop_index);
            let FrameData::Transcription(final_text) = &frames[final_index].frame.data else {
                unreachable!()
            };
            assert_eq!(final_text.user_id, "provider-speaker");
            assert_eq!(final_text.language.as_deref(), Some("en"));
            assert!(final_text.finalized);
            assert!(!final_text.timestamp.is_empty());
            assert_eq!(final_text.result.as_ref(), Some(&result));
        }
        assert!(harness
            .upstream
            .frames
            .lock()
            .unwrap()
            .iter()
            .any(|f| f.direction == Direction::Upstream
                && matches!(f.frame.data, FrameData::ProposedUserStartedSpeaking)));
        harness.finish(false).await;
    }
}

#[cfg(feature = "cartesia-turns-eager")]
mod eager {
    use super::*;
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};

    fn config(interruptions: bool) -> UserAggregatorConfig {
        UserAggregatorConfig {
            eager: Some(Default::default()),
            turn: UserTurnConfig {
                enable_interruptions: interruptions,
                ..Default::default()
            },
            // The default interrupted-turn recovery is deliberately enabled.
            ..Default::default()
        }
    }

    fn delta(value: Value) -> SttSettings {
        SttSettings {
            base: ServiceSettings {
                extra: serde_json::from_value(value).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn new_id(mut frame: Frame) -> Frame {
        frame.id = Frame::text("fresh-public-frame-id").id;
        frame
    }

    impl Harness {
        fn owner_context(&self) -> ProcessorContext {
            self.source.context.lock().unwrap().clone().unwrap()
        }

        async fn predict(&self, text: &str) {
            self.connection
                .event(json!({"type":"turn.eager_end","transcript":text,"request_id":"reused-id"}))
                .await;
        }

        async fn end_turn(&self, text: &str) {
            self.connection
                .event(json!({"type":"turn.end","transcript":text,"request_id":"reused-id"}))
                .await;
        }

        fn prediction(&self) -> NativeEagerPrediction {
            let frame = self
                .before
                .native(|v| matches!(v, NativeEagerSource::Prediction(_)));
            match frame.native_eager_source().unwrap() {
                NativeEagerSource::Prediction(v) => v.clone(),
                _ => unreachable!(),
            }
        }

        fn turn(&self) -> NativeEagerTurn {
            let frame = self
                .before
                .native(|v| matches!(v, NativeEagerSource::Start(_)));
            match frame.native_eager_source().unwrap() {
                NativeEagerSource::Start(v) => v.clone(),
                _ => unreachable!(),
            }
        }

        async fn send_applied(&self, frame: Frame) {
            let id = frame.id;
            let prior = self
                .user
                .applied
                .lock()
                .unwrap()
                .iter()
                .filter(|&&v| v == id)
                .count();
            self.send(frame).await;
            wait(|| {
                self.user
                    .applied
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|&&v| v == id)
                    .count()
                    > prior
            })
            .await;
        }

        async fn non_speaking(&self) {
            self.send_applied(Frame::new(FrameData::VadUserStoppedSpeaking(
                VadSpeakingFrame {
                    duration_secs: 0.0,
                    timestamp: 0.0,
                },
            )))
            .await;
            assert!(
                self.pair.user().deadline().unwrap().is_some(),
                "watchdog must be armed"
            );
        }

        async fn advance(&self, millis: u64) {
            let mut frame = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                event: None,
                returning: false,
                origin: Some("cartesia-eager-clock".into()),
            }));
            frame.metadata.insert("advance_ms".into(), millis.into());
            self.send_applied(frame).await;
            self.barrier(true).await;
        }

        async fn buffered(&self, minimum: usize) {
            within(async {
                while self.llm.speculation_snapshot().await.buffered_frames < minimum {
                    tokio::task::yield_now().await;
                }
            })
            .await;
        }

        async fn pending_fixture_barrier(
            &self,
            completions: bool,
            stage: &str,
        ) -> Result<(), String> {
            let marker = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                event: None,
                returning: false,
                origin: Some(
                    if completions {
                        "pipecat.flows"
                    } else {
                        "cartesia-eager-fixture"
                    }
                    .into(),
                ),
            }));
            let id = marker.id;
            tokio::time::timeout(Duration::from_secs(5), async {
                self.worker
                    .sender()
                    .send(marker, Direction::Downstream)
                    .await
                    .map_err(|error| format!("cartesia-eager/{stage}: marker enqueue: {error}"))?;
                loop {
                    let observed = if completions {
                        self.settled.observed(id)
                    } else {
                        self.after.observed(id)
                    };
                    if observed {
                        return Ok(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(|_| {
                format!(
                    "cartesia-eager/{stage}: {} marker deadline",
                    if completions { "completion" } else { "pre-LLM" }
                )
            })?
        }

        async fn settle_or_observe_pending_request(
            &self,
            http: &HttpPeer,
            prior_requests: usize,
            stage: &str,
        ) -> Result<Option<SpeculationStatus>, String> {
            // Settle adoption before inspecting the actual latest capability.
            // A post-LLM completion marker would be retained while it is Pending.
            self.pending_fixture_barrier(false, stage).await?;
            let capability = self
                .after
                .frames
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find_map(|frame| {
                    matches!(frame.frame.data, FrameData::LlmContextDetailed(_))
                        .then(|| frame.frame.native_speculation().cloned())
                        .flatten()
                });
            let status = capability.as_ref().map(NativeSpeculation::status);
            if status == Some(SpeculationStatus::Pending) {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while http.count() <= prior_requests
                        || self.llm.speculation_snapshot().await.buffered_frames < 3
                    {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .map_err(|_| format!("cartesia-eager/{stage}: Pending adoption HTTP/response progression deadline"))?;
                // The real request is the forbidden effect. Buffered frames
                // also witness response progression, not decoder completion:
                // the pre-LLM marker may contribute one retained frame.
            } else {
                self.pending_fixture_barrier(true, stage).await?;
            }
            Ok(status)
        }

        async fn finish_pending_fixture(
            mut self,
            ws: &mut WsPeer,
            http: &mut HttpPeer,
            allow_provider_error: bool,
        ) -> Vec<String> {
            async fn drain(
                worker: &mut PipelineWorker,
                allow_provider_error: bool,
            ) -> Result<(), String> {
                while worker.recv().await.is_some() {}
                let report = worker.join().await.map_err(|error| error.to_string())?;
                if !allow_provider_error && !report.errors.is_empty() {
                    return Err(format!("unexpected worker errors: {:?}", report.errors));
                }
                Ok(())
            }
            self.before.release();
            self.after.release();
            let mut failures = Vec::new();
            match tokio::time::timeout(Duration::from_secs(5), self.worker.end()).await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => failures.push(format!("End enqueue: {error}")),
                Err(_) => failures.push("End enqueue deadline".into()),
            }
            match tokio::time::timeout(
                Duration::from_secs(5),
                drain(&mut self.worker, allow_provider_error),
            )
            .await
            {
                Ok(Ok(())) => (),
                Ok(Err(error)) => failures.push(format!("worker join: {error}")),
                Err(_) => {
                    failures.push("worker join deadline".into());
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        self.worker.cancel("pending fixture cleanup"),
                    )
                    .await
                    {
                        Ok(Ok(())) => (),
                        Ok(Err(error)) => failures.push(format!("worker cancel: {error}")),
                        Err(_) => failures.push("worker cancel deadline".into()),
                    }
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        drain(&mut self.worker, allow_provider_error),
                    )
                    .await
                    {
                        Ok(Ok(())) => (),
                        Ok(Err(error)) => failures.push(format!("cancelled worker join: {error}")),
                        Err(_) => failures.push("cancelled worker join deadline".into()),
                    }
                }
            }
            if self
                .after
                .frames
                .lock()
                .unwrap()
                .iter()
                .any(|frame| frame.frame.native_eager_source().is_some())
            {
                failures.push("source carrier escaped aggregation".into());
            }
            // A successful server join now includes shutdown of its JoinSet,
            // which cancels and joins every child connection before returning.
            if let Some(stop) = http.stop.take() {
                let _ = stop.send(());
            }
            if let Some(stop) = ws.stop.take() {
                let _ = stop.send(());
            }
            for (name, task) in [
                ("HTTP peer", &mut http.task),
                ("WebSocket peer", &mut ws.task),
            ] {
                match tokio::time::timeout(Duration::from_secs(5), &mut *task).await {
                    Ok(Ok(())) => (),
                    Ok(Err(error)) => failures.push(format!("{name} join: {error}")),
                    Err(_) => {
                        failures.push(format!(
                            "{name} shutdown deadline; child joins not established"
                        ));
                        task.abort();
                        match tokio::time::timeout(Duration::from_secs(5), task).await {
                            Ok(Ok(())) => (),
                            Ok(Err(error)) if error.is_cancelled() => (),
                            Ok(Err(error)) => {
                                failures.push(format!("aborted {name} join: {error}"))
                            }
                            Err(_) => failures.push(format!("aborted {name} join deadline")),
                        }
                    }
                }
            }
            failures
        }

        async fn fail_pending_fixture_after_cleanup(
            self,
            mut ws: WsPeer,
            mut http: HttpPeer,
            stage_failure: String,
        ) -> ! {
            let failures = self.finish_pending_fixture(&mut ws, &mut http, true).await;
            panic!("{stage_failure}; cleanup outcomes: {failures:?}");
        }

        fn no_history(&self) {
            assert!(
                self.pair.context().snapshot().unwrap().messages.is_empty(),
                "provisional history escaped"
            );
        }

        fn no_output(&self) {
            assert_eq!(
                self.completion_outputs(),
                0,
                "held text, signature or tool intent escaped"
            );
        }

        fn completion_outputs(&self) -> usize {
            self.output.count(|v| {
                matches!(
                    v,
                    FrameData::LlmText(_)
                        | FrameData::LlmThoughtText(_)
                        | FrameData::LlmContinuationCheckpoint
                        | FrameData::FunctionCallInProgress(_)
                )
            })
        }

        fn quiet(&self) -> bool {
            self.pair.context().snapshot().unwrap().messages.is_empty()
                && self.completion_outputs() == 0
        }

        async fn fail_after_cleanup(self, message: String) -> ! {
            self.finish(true).await;
            panic!("{message}");
        }

        async fn restart(&mut self, ws: &mut WsPeer) {
            let source = self.source.processor.clone();
            let context = self.owner_context();
            let replacement = tokio::spawn(async move {
                source
                    .restart_with_settings(&delta(json!({"keyterm":["replacement"]})), &context)
                    .await
            });
            self.connection = ws.connection().await;
            within(replacement).await.unwrap().unwrap();
        }

        async fn mute_source(&self) {
            self.source
                .processor
                .process(
                    Frame::new(FrameData::SttMute(true)),
                    Direction::Downstream,
                    &self.owner_context(),
                )
                .await
                .unwrap();
        }

        async fn finish_source(&mut self) {
            // No pipeline terminal is sent: aggregation and inference stay current.
            self.source
                .processor
                .prepare_end(&self.owner_context())
                .await
                .unwrap();
            assert_eq!(
                self.connection.wire().await.to_text().unwrap(),
                r#"{"type":"close"}"#
            );
        }
    }

    #[tokio::test]
    async fn provider_start_authority_is_required_for_one_settled_inference() {
        let mut ws = WsPeer::new().await;
        let http = HttpPeer::new().await;
        *http.automatic.lock().unwrap() = Some(answer("Source-owned answer."));
        let harness = Harness::new(
            &mut ws,
            &http,
            true,
            config(true),
            None,
            Provider::OpenAi,
            None,
        )
        .await;
        // Preserve the actual provider Start flush roundtrip independently of
        // whether the consumer received its required native Start authority.
        harness.start().await;
        harness.predict("source owned").await;
        wait(|| {
            harness
                .before
                .count(|v| matches!(v, FrameData::EagerTranscription(_)))
                == 1
        })
        .await;
        let prediction = harness
            .before
            .native(|v| matches!(v, NativeEagerSource::Prediction(_)));
        wait(|| {
            harness
                .user
                .applied
                .lock()
                .unwrap()
                .contains(&prediction.id)
        })
        .await;
        harness.end_turn("source owned").await;
        wait(|| {
            harness.before.frames.lock().unwrap().iter().any(|f| {
                matches!(
                    f.frame.native_eager_source(),
                    Some(NativeEagerSource::Stop {
                        commit: Some(_),
                        ..
                    })
                )
            })
        })
        .await;
        let stop = harness.before.native(|v| {
            matches!(
                v,
                NativeEagerSource::Stop {
                    commit: Some(_),
                    ..
                }
            )
        });
        wait(|| harness.user.applied.lock().unwrap().contains(&stop.id)).await;
        // Settlement does not require a request to exist. A missing Start
        // reaches the count assertions rather than timing out awaiting HTTP.
        harness.barrier(true).await;
        let requests = http.count();
        let history = harness.user_history();
        let outputs = harness
            .output
            .count(|v| matches!(v, FrameData::LlmText(t) if t.text == "Source-owned answer."));
        let starts = harness
            .before
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter(|f| {
                matches!(
                    f.frame.native_eager_source(),
                    Some(NativeEagerSource::Start(_))
                )
            })
            .count();
        harness.finish(false).await;
        assert_eq!(starts, 1, "provider Start authority was not delivered");
        assert_eq!(
            requests, 1,
            "exact source Start must authorize one actual HTTP inference"
        );
        assert_eq!(history, ["source owned"]);
        assert_eq!(outputs, 1);
    }

    #[tokio::test]
    async fn exact_and_normalized_matches_commit_once_and_release_only_after_boundary() {
        for (policy, eager, final_text) in [
            (
                EagerMatchPolicy::Exact,
                "Keep raw  spacing",
                "Keep raw  spacing",
            ),
            (
                EagerMatchPolicy::Normalized,
                "its ＲＥＡＤＹ",
                "It's ready!",
            ),
        ] {
            for interruptions in [false, true] {
                let mut ws = WsPeer::new().await;
                let mut http = HttpPeer::new().await;
                let mut user_config = config(interruptions);
                user_config.eager =
                    Some(EagerUserTurnConfig::new(policy, Duration::from_secs(5), 65536).unwrap());
                let mut harness = Harness::new(
                    &mut ws,
                    &http,
                    true,
                    user_config,
                    Some(PauseAt::Boundary),
                    Provider::OpenAi,
                    None,
                )
                .await;
                harness.audio().await;
                harness.start().await;
                assert_eq!(
                    harness
                        .output
                        .count(|v| matches!(v, FrameData::Interruption)),
                    usize::from(interruptions)
                );
                harness.predict(eager).await;
                let (request, reply) = http.request().await;
                assert_eq!(request["messages"][0]["content"], eager);
                let capability = harness.after.capability();
                let prediction = harness.prediction();
                assert_eq!(capability.status(), SpeculationStatus::Pending);
                harness.no_history();
                reply.send(answer("One answer.")).unwrap();
                harness.buffered(3).await;
                harness.no_output();
                harness.end_turn(final_text).await;
                harness.after.paused().await;
                assert_eq!(capability.status(), SpeculationStatus::Confirmed);
                assert_eq!(harness.user_history(), [final_text]);
                harness.no_output();
                let final_frame = harness
                    .before
                    .native(|v| matches!(v, NativeEagerSource::Final(_)));
                let stop = harness
                    .before
                    .native(|v| matches!(v, NativeEagerSource::Stop { .. }));
                let NativeEagerSource::Final(commit) = final_frame.native_eager_source().unwrap()
                else {
                    unreachable!()
                };
                let NativeEagerSource::Stop {
                    turn,
                    commit: Some(stop_commit),
                } = stop.native_eager_source().unwrap()
                else {
                    unreachable!()
                };
                assert_eq!(commit, stop_commit);
                assert_eq!(turn, prediction.turn());
                assert!(commit.matches_prediction(&prediction));
                assert_eq!(commit.payload(), &final_frame.data);
                assert!(!stop.is_system(), "final must precede normal-priority stop");
                assert!(harness.upstream.frames.lock().unwrap().iter().all(|f| {
                    f.frame.native_eager_source().is_none()
                        && !(f.direction == Direction::Upstream
                            && matches!(
                                f.frame.data,
                                FrameData::ProposedUserStartedSpeaking
                                    | FrameData::ProposedUserStoppedSpeaking
                            ))
                }));
                harness.after.release();
                harness.barrier(true).await;
                assert_eq!(capability.status(), SpeculationStatus::Released);
                assert_eq!(http.count(), 1);
                assert_eq!(
                    harness
                        .output
                        .count(|v| matches!(v, FrameData::LlmText(t) if t.text == "One answer.")),
                    1
                );
                for replay in [final_frame, stop] {
                    harness.send_applied(new_id(replay)).await;
                }
                harness.barrier(true).await;
                assert_eq!(http.count(), 1);
                assert_eq!(harness.user_history(), [final_text]);
                harness.finish(false).await;
            }
        }
    }

    #[tokio::test]
    async fn coalesced_events_mint_one_exact_receipt_and_preserve_source_payload_order() {
        let mut ws = WsPeer::new().await;
        let mut http = HttpPeer::new().await;
        let mut harness = Harness::new(
            &mut ws,
            &http,
            true,
            config(true),
            Some(PauseAt::Final),
            Provider::OpenAi,
            None,
        )
        .await;
        harness.audio().await;
        let result = json!({"type":"turn.end","transcript":"Coalesced!","language":"fr","extra":{"provider":17}});
        harness
            .connection
            .batch(vec![
                json!({"type":"turn.start","transcript":"not committed"}),
                json!({"type":"turn.eager_end","transcript":"coalesced"}),
                result.clone(),
            ])
            .await;
        harness.before.paused().await;
        let (request, reply) = http.request().await;
        assert_eq!(request["messages"][0]["content"], "coalesced");
        harness.no_history();
        reply.send(answer("Coalesced answer.")).unwrap();
        harness.buffered(3).await;
        harness.no_output();
        let final_frame = harness
            .before
            .native(|v| matches!(v, NativeEagerSource::Final(_)));
        let FrameData::Transcription(text) = &final_frame.data else {
            unreachable!()
        };
        assert_eq!(text.text, "Coalesced!");
        assert_eq!(text.user_id, "provider-speaker");
        assert!(!text.timestamp.is_empty());
        assert!(text.finalized);
        assert_eq!(text.language.as_deref(), Some("en"));
        assert_eq!(text.result.as_ref(), Some(&result));
        assert!(
            serde_json::to_value(&final_frame).is_err(),
            "unconsumed authority must not serialize"
        );
        harness.before.release();
        harness.barrier(true).await;
        {
            let frames = harness.before.frames.lock().unwrap();
            let usage = frames.iter().position(|f| matches!(&f.frame.data, FrameData::Metrics(values)
            if values.iter().any(|m| matches!(m, Metric::SttUsage { audio_seconds, .. } if *audio_seconds > 0.0)))).unwrap();
            let final_index = frames
                .iter()
                .position(|f| f.frame.id == final_frame.id)
                .unwrap();
            let stop = frames
                .iter()
                .position(|f| {
                    matches!(
                        f.frame.native_eager_source(),
                        Some(NativeEagerSource::Stop { .. })
                    )
                })
                .unwrap();
            assert!(usage < final_index && final_index < stop);
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(
                        f.frame.native_eager_source(),
                        Some(NativeEagerSource::Start(_))
                    ))
                    .count(),
                1
            );
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(
                        f.frame.native_eager_source(),
                        Some(NativeEagerSource::Prediction(_))
                    ))
                    .count(),
                1
            );
        }
        assert_eq!(http.count(), 1);
        assert_eq!(harness.user_history(), ["Coalesced!"]);
        harness.finish(false).await;
    }

    #[tokio::test]
    async fn mismatched_final_and_resume_retry_current_final_once_on_the_same_socket() {
        for resume in [false, true] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            let harness = Harness::new(
                &mut ws,
                &http,
                true,
                config(true),
                None,
                Provider::OpenAi,
                None,
            )
            .await;
            harness.start().await;
            harness.predict("provisional").await;
            let (_, reply) = http.request().await;
            reply.send(answer("Discard this.")).unwrap();
            harness.buffered(3).await;
            let capability = harness.after.capability();
            if resume {
                harness
                    .connection
                    .event(json!({"type":"turn.resume"}))
                    .await;
                wait(|| !harness.prediction().is_live()).await;
                harness.barrier(false).await;
                assert_eq!(capability.status(), SpeculationStatus::Cancelled);
            }
            harness.end_turn("Actual final.").await;
            let (request, reply) = http.request().await;
            assert_eq!(request["messages"][0]["content"], "Actual final.");
            reply.send(answer("Actual answer.")).unwrap();
            harness.barrier(true).await;
            assert_eq!(capability.status(), SpeculationStatus::Cancelled);
            assert_eq!(http.count(), 2);
            assert_eq!(harness.user_history(), ["Actual final."]);
            assert_eq!(
                harness
                    .output
                    .count(|v| matches!(v, FrameData::LlmText(t) if t.text == "Discard this.")),
                0
            );
            harness.start().await;
            harness.end_turn("Next real turn.").await;
            let (request, reply) = http.request().await;
            assert_eq!(
                request["messages"].as_array().unwrap().last().unwrap()["content"],
                "Next real turn."
            );
            reply.send(answer("Still listening.")).unwrap();
            harness.barrier(true).await;
            assert_eq!(http.count(), 3);
            assert!(
                ws.connections.try_recv().is_err(),
                "resume replaced the listening socket"
            );
            harness.finish(false).await;
        }
    }

    #[tokio::test]
    async fn repeated_prediction_and_late_exact_cancel_cannot_cancel_replacement() {
        for resume_old in [false, true] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            let harness = Harness::new(
                &mut ws,
                &http,
                true,
                config(true),
                None,
                Provider::OpenAi,
                None,
            )
            .await;
            harness.start().await;
            harness.predict("old").await;
            let (_, old_reply) = http.request().await;
            let old_frame = harness
                .before
                .native(|v| matches!(v, NativeEagerSource::Prediction(_)));
            let old_prediction = match old_frame.native_eager_source().unwrap() {
                NativeEagerSource::Prediction(prediction) => prediction.clone(),
                _ => unreachable!(),
            };
            let old_capability = harness.after.capability();
            // Repeated EagerEnd revokes the old prediction without emitting a
            // Cancel frame. Obtain a genuine exact Cancel only through Resume.
            let cancel = if resume_old {
                harness
                    .connection
                    .event(json!({"type":"turn.resume"}))
                    .await;
                wait(|| {
                    harness.before.frames.lock().unwrap().iter().any(|frame| {
                        matches!(
                            frame.frame.native_eager_source(),
                            Some(NativeEagerSource::Cancel(prediction))
                                if prediction == &old_prediction
                        )
                    })
                })
                .await;
                let cancel = harness.before.native(|source| {
                    matches!(source, NativeEagerSource::Cancel(prediction) if prediction == &old_prediction)
                });
                harness.barrier(false).await;
                Some(cancel)
            } else {
                None
            };
            harness.predict("replacement").await;
            let (_, reply) = http.request().await;
            let replacement = harness.after.capability();
            let old_status = old_capability.status();
            for frame in cancel.into_iter().chain(std::iter::once(old_frame)) {
                harness.send_applied(new_id(frame)).await;
            }
            harness.barrier(false).await;
            let pending_status = replacement.status();
            let assert_pending = || {
                assert_ne!(old_capability, replacement);
                assert_eq!(old_status, SpeculationStatus::Cancelled);
                assert_eq!(pending_status, SpeculationStatus::Pending);
            };
            if old_capability == replacement
                || old_status != SpeculationStatus::Cancelled
                || pending_status != SpeculationStatus::Pending
            {
                // Do not wait for speculative buffering after a failed
                // ownership assertion. Service both held requests and join.
                let _ = old_reply.send(answer("Unexpected old response."));
                let _ = reply.send(answer("Unexpected replacement response."));
                harness.finish(true).await;
                assert_pending();
                return;
            }
            drop(old_reply);
            reply.send(answer("Only replacement.")).unwrap();
            harness.buffered(3).await;
            harness.end_turn("replacement").await;
            harness.barrier(true).await;
            let released_status = replacement.status();
            let requests = http.count();
            let history = harness.user_history();
            let outputs = harness.output.count(|v| matches!(v, FrameData::LlmText(_)));
            harness.finish(false).await;
            assert_pending();
            assert_eq!(released_status, SpeculationStatus::Released);
            assert_eq!(requests, 2);
            assert_eq!(history, ["replacement"]);
            assert_eq!(outputs, 1);
        }
    }

    #[tokio::test]
    async fn retirement_overtakes_real_prediction_before_adoption_and_before_request() {
        for pause in [PauseAt::Prediction, PauseAt::Context] {
            for retirement in ["resume", "restart", "aggregator-mute", "provider-error"] {
                let mut ws = WsPeer::new().await;
                let mut http = HttpPeer::new().await;
                *http.automatic.lock().unwrap() =
                    Some(answer("Unexpected retired-source response."));
                let mut harness = Harness::new(
                    &mut ws,
                    &http,
                    true,
                    config(true),
                    Some(pause),
                    Provider::OpenAi,
                    None,
                )
                .await;
                harness.start().await;
                let prior_requests = http.count();
                harness.predict("must stay queued").await;
                if pause == PauseAt::Prediction {
                    harness.before.paused().await;
                } else {
                    harness.after.paused().await;
                }
                let prediction = harness.prediction();
                assert!(prediction.is_live());
                match retirement {
                    "resume" => {
                        harness
                            .connection
                            .event(json!({"type":"turn.resume"}))
                            .await;
                        wait(|| {
                            harness.before.frames.lock().unwrap().iter().any(|f| {
                                matches!(
                                    f.frame.native_eager_source(),
                                    Some(NativeEagerSource::Cancel(current)) if current == &prediction
                                )
                            })
                        })
                        .await;
                        let cancel = harness.before.native(|source| {
                            matches!(source, NativeEagerSource::Cancel(current) if current == &prediction)
                        });
                        // The system-priority exact Cancel must be consumed
                        // before the held normal-priority prediction is released.
                        let applied = tokio::time::timeout(Duration::from_secs(5), async {
                            while !harness.user.applied.lock().unwrap().contains(&cancel.id) {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .map_err(|_| {
                            "cartesia-eager/queued-retirement: exact Cancel application deadline"
                                .to_string()
                        });
                        if let Err(error) = applied {
                            harness
                                .fail_pending_fixture_after_cleanup(ws, http, error)
                                .await;
                        }
                    }
                    "restart" => harness.restart(&mut ws).await,
                    // SttMute also reaches aggregation, whose authority is
                    // stronger than source retirement alone.
                    "aggregator-mute" => harness.mute_source().await,
                    "provider-error" => {
                        harness
                            .connection
                            .event(json!({"type":"error","message":"private provider detail"}))
                            .await;
                        wait(|| !harness.worker.errors().is_empty()).await;
                    }
                    _ => unreachable!(),
                }
                let retired = !prediction.is_live();
                let cancelled = pause != PauseAt::Context
                    || harness.after.capability().status() == SpeculationStatus::Cancelled;
                if pause == PauseAt::Prediction {
                    harness.before.release();
                } else {
                    harness.after.release();
                }
                let observed_status = match harness
                    .settle_or_observe_pending_request(&http, prior_requests, "queued-retirement")
                    .await
                {
                    Ok(status) => status,
                    Err(error) => {
                        harness
                            .fail_pending_fixture_after_cleanup(ws, http, error)
                            .await
                    }
                };
                let count = http.count();
                let quiet = harness.quiet();
                let delivered = pause != PauseAt::Prediction
                    || harness.user.applied.lock().unwrap().contains(
                        &harness
                            .before
                            .native(|v| matches!(v, NativeEagerSource::Prediction(_)))
                            .id,
                    );
                let cleanup = harness
                    .finish_pending_fixture(
                        &mut ws,
                        &mut http,
                        retirement == "provider-error"
                            || observed_status == Some(SpeculationStatus::Pending),
                    )
                    .await;
                assert!(
                    retired && cancelled,
                    "{retirement} did not retire exact pending authority; cleanup: {cleanup:?}"
                );
                assert_eq!(
                    count, 0,
                    "{retirement} at queued admission made a request: {observed_status:?}"
                );
                assert!(quiet, "{retirement} exposed provisional history or output");
                assert!(
                    delivered,
                    "the queued carrier must reach the consumer, not merely be purged"
                );
                assert!(cleanup.is_empty(), "queued-retirement cleanup: {cleanup:?}");
            }
        }
    }

    #[tokio::test]
    async fn staged_final_retirement_blocks_fallback_and_default_recovery() {
        for stop_first in [false, true] {
            let mut ws = WsPeer::new().await;
            let http = HttpPeer::new().await;
            *http.automatic.lock().unwrap() = Some(answer("Unexpected staged-final recovery."));
            let mut harness = Harness::new(
                &mut ws,
                &http,
                true,
                config(true),
                Some(PauseAt::Stop),
                Provider::OpenAi,
                None,
            )
            .await;
            harness.start().await;
            harness.non_speaking().await;
            harness.end_turn("retired staged final").await;
            harness.before.paused().await;
            let final_frame = harness
                .before
                .native(|v| matches!(v, NativeEagerSource::Final(_)));
            wait(|| {
                harness
                    .user
                    .applied
                    .lock()
                    .unwrap()
                    .contains(&final_frame.id)
            })
            .await;
            let stop = harness
                .before
                .native(|v| matches!(v, NativeEagerSource::Stop { .. }));
            let turn = harness.turn();
            assert!(turn.is_live());
            harness.restart(&mut ws).await;
            let retired = !turn.is_live();
            harness.before.release();
            let duplicates = if stop_first {
                [stop, final_frame]
            } else {
                [final_frame, stop]
            };
            for frame in duplicates {
                harness.send_applied(new_id(frame)).await;
            }
            harness.advance(6000).await;
            let count = http.count();
            let quiet = harness.quiet();
            harness.finish(false).await;
            assert!(retired, "restart left staged source authority live");
            assert_eq!(
                count, 0,
                "retired source admitted user or developer fallback"
            );
            assert!(quiet, "retired staged final exposed history or output");
        }
    }

    #[tokio::test]
    async fn no_prediction_watchdog_has_a_healthy_default_recovery_control() {
        for disposition in ["healthy", "restart", "normal-eof", "empty-end"] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            if matches!(disposition, "restart" | "normal-eof") {
                *http.automatic.lock().unwrap() = Some(answer("Unexpected no-final recovery."));
            }
            let mut harness = Harness::new(
                &mut ws,
                &http,
                true,
                config(true),
                None,
                Provider::OpenAi,
                None,
            )
            .await;
            harness.start().await;
            let turn = harness.turn();
            harness.non_speaking().await;
            match disposition {
                "restart" => harness.restart(&mut ws).await,
                "normal-eof" => {
                    harness.finish_source().await;
                    wait(|| !turn.is_active()).await;
                }
                "empty-end" => {
                    harness.end_turn("").await;
                    wait(|| {
                        harness
                            .before
                            .count(|v| matches!(v, FrameData::ProposedUserStoppedSpeaking))
                            == 1
                    })
                    .await;
                    harness.barrier(false).await;
                }
                "healthy" => (),
                _ => unreachable!(),
            }
            // Hold the actual HTTP reply while advancing; do not join an active request here.
            let mut clock = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                event: None,
                returning: false,
                origin: Some("cartesia-eager-clock".into()),
            }));
            clock.metadata.insert("advance_ms".into(), 6000.into());
            harness.send_applied(clock).await;
            if matches!(disposition, "healthy" | "empty-end") {
                let (request, reply) = http.request().await;
                assert_eq!(request["messages"][0]["role"], "developer");
                assert_eq!(
                    request["messages"][0]["content"],
                    DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT
                );
                reply.send(answer("Please repeat.")).unwrap();
                harness.barrier(true).await;
                assert_eq!(http.count(), 1);
            } else {
                harness.barrier(true).await;
                let count = http.count();
                let quiet = harness.quiet();
                if count != 0 || !quiet || turn.is_live() {
                    harness.fail_after_cleanup(format!("retired no-prediction {disposition} recovered: requests={count}, quiet={quiet}")).await;
                }
            }
            harness.finish(false).await;
        }
    }

    #[tokio::test]
    async fn minted_final_survives_source_only_normal_eof_before_consumer_delivery() {
        let mut ws = WsPeer::new().await;
        let mut http = HttpPeer::new().await;
        let mut harness = Harness::new(
            &mut ws,
            &http,
            true,
            config(true),
            Some(PauseAt::Final),
            Provider::OpenAi,
            None,
        )
        .await;
        harness.start().await;
        harness.predict("preserved").await;
        let (_, reply) = http.request().await;
        reply.send(answer("Preserved answer.")).unwrap();
        harness.buffered(3).await;
        harness.end_turn("preserved").await;
        harness.before.paused().await;
        let turn = harness.turn();
        let final_frame = harness
            .before
            .native(|v| matches!(v, NativeEagerSource::Final(_)));
        harness.finish_source().await;
        wait(|| !turn.is_active()).await;
        assert!(turn.is_live(), "normal completion lost an offered final");
        let NativeEagerSource::Final(receipt) = final_frame.native_eager_source().unwrap() else {
            unreachable!()
        };
        assert!(receipt.is_live());
        harness.no_history();
        harness.before.release();
        harness.barrier(true).await;
        assert_eq!(http.count(), 1);
        assert_eq!(harness.user_history(), ["preserved"]);
        assert_eq!(
            harness
                .output
                .count(|v| matches!(v, FrameData::LlmText(t) if t.text == "Preserved answer.")),
            1
        );
        harness.finish(false).await;
    }

    fn tool_response(id: &str) -> String {
        format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{
                "delta":{"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":"book","arguments":"{}"}}]},
                "finish_reason":"tool_calls"
            }]})
        )
    }

    fn tools() -> (Arc<ToolExecutor>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let constructed = Arc::new(AtomicUsize::new(0));
        let polled = Arc::new(AtomicUsize::new(0));
        let constructions = constructed.clone();
        let polls = polled.clone();
        let mut executor = ToolExecutor::new(1, 8, 4096).unwrap();
        executor
            .register(
                "book",
                RegisteredTool::new(Arc::new(move |_| {
                    constructions.fetch_add(1, Ordering::SeqCst);
                    let polls = polls.clone();
                    Box::pin(async move {
                        polls.fetch_add(1, Ordering::SeqCst);
                        Ok(json!({"booked":true}))
                    })
                })),
            )
            .unwrap();
        (Arc::new(executor), constructed, polled)
    }

    #[tokio::test]
    async fn pending_tool_is_discarded_and_current_final_retries_one_real_effect() {
        let mut ws = WsPeer::new().await;
        let mut http = HttpPeer::new().await;
        let (tools, constructed, polled) = tools();
        let harness = Harness::new(
            &mut ws,
            &http,
            true,
            config(true),
            None,
            Provider::OpenAi,
            Some(tools),
        )
        .await;
        harness.start().await;
        harness.predict("book").await;
        http.request()
            .await
            .1
            .send(tool_response("pending-book"))
            .unwrap();
        let capability = harness.after.capability();
        wait(|| capability.status() == SpeculationStatus::Cancelled).await;
        harness.barrier(true).await;
        assert_eq!(constructed.load(Ordering::SeqCst), 0);
        assert_eq!(polled.load(Ordering::SeqCst), 0);
        harness.no_history();
        harness.no_output();
        assert!(harness.prediction().turn().is_live());
        harness.end_turn("book").await;
        http.request()
            .await
            .1
            .send(tool_response("committed-book"))
            .unwrap();
        wait(|| polled.load(Ordering::SeqCst) == 1).await;
        let (continuation, reply) = http.request().await;
        assert!(continuation["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "tool" && m["tool_call_id"] == "committed-book"));
        reply.send(answer("Booked.")).unwrap();
        harness.barrier(true).await;
        assert_eq!(constructed.load(Ordering::SeqCst), 1);
        assert_eq!(polled.load(Ordering::SeqCst), 1);
        assert_eq!(http.count(), 3);
        assert_eq!(harness.user_history(), ["book"]);
        harness.finish(false).await;
    }

    #[tokio::test]
    async fn transferred_tool_survives_stt_restart_but_waits_for_boundary_release() {
        let mut ws = WsPeer::new().await;
        let mut http = HttpPeer::new().await;
        let (tools, constructed, polled) = tools();
        let mut harness = Harness::new(
            &mut ws,
            &http,
            true,
            config(true),
            Some(PauseAt::Boundary),
            Provider::OpenAi,
            Some(tools),
        )
        .await;
        harness.start().await;
        harness.predict("book").await;
        let (_, reply) = http.request().await;
        harness.end_turn("book").await;
        harness.after.paused().await;
        let capability = harness.after.capability();
        assert_eq!(capability.status(), SpeculationStatus::Confirmed);
        harness.restart(&mut ws).await;
        assert_eq!(capability.status(), SpeculationStatus::Confirmed);
        assert!(capability.source_is_current());
        reply.send(tool_response("transferred-book")).unwrap();
        harness.buffered(3).await;
        assert_eq!(constructed.load(Ordering::SeqCst), 0);
        assert_eq!(polled.load(Ordering::SeqCst), 0);
        harness.after.release();
        wait(|| polled.load(Ordering::SeqCst) == 1).await;
        http.request()
            .await
            .1
            .send(answer("Transferred booking."))
            .unwrap();
        harness.barrier(true).await;
        assert_eq!(constructed.load(Ordering::SeqCst), 1);
        assert_eq!(polled.load(Ordering::SeqCst), 1);
        assert_eq!(http.count(), 2);
        harness.finish(false).await;
    }

    fn signed_response(provider: Provider) -> (String, Value, usize) {
        if provider == Provider::Gemini {
            let parts = json!([
                {"text":"● exact ","thoughtSignature":"opaque+/=="},
                {"text":"text"},
                {"text":"","thoughtSignature":"tail+/=="}
            ]);
            (
                format!(
                    "data: {}\n\n",
                    json!({
                        "candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]
                    })
                ),
                parts,
                5,
            )
        } else {
            let events = [
                json!({"type":"message_start","message":{}}),
                json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"closed\n思考"}}),
                json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque+/"}}),
                json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"bytes=="}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"Ready."}}),
                json!({"type":"content_block_stop","index":1}),
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
                json!({"type":"message_stop"}),
            ];
            (
                events
                    .iter()
                    .map(|event| format!("data: {event}\n\n"))
                    .collect(),
                json!([
                    {"type":"thinking","thinking":"closed\n思考","signature":"opaque+/bytes=="},
                    {"type":"text","text":"Ready."}
                ]),
                7,
            )
        }
    }

    #[tokio::test]
    async fn both_signed_adapters_preserve_exact_next_request_after_transfer_and_discard_before_it()
    {
        for provider in [Provider::Anthropic, Provider::Gemini] {
            for transfer in [false, true] {
                let mut ws = WsPeer::new().await;
                let mut http = HttpPeer::new().await;
                let (response, expected, buffered) = signed_response(provider);
                let mut harness = Harness::new(
                    &mut ws,
                    &http,
                    true,
                    config(true),
                    Some(if transfer {
                        PauseAt::Boundary
                    } else {
                        PauseAt::Stop
                    }),
                    provider,
                    None,
                )
                .await;
                harness.start().await;
                harness.predict("its ready").await;
                http.request().await.1.send(response.clone()).unwrap();
                harness.buffered(buffered).await;
                let capability = harness.after.capability();
                harness.no_history();
                harness.no_output();
                harness.end_turn("It's ready!").await;
                if transfer {
                    harness.after.paused().await;
                } else {
                    harness.before.paused().await;
                }
                assert_eq!(
                    capability.status(),
                    if transfer {
                        SpeculationStatus::Confirmed
                    } else {
                        SpeculationStatus::Pending
                    }
                );
                harness.restart(&mut ws).await;
                let status_after_restart = capability.status();
                if !transfer {
                    *http.automatic.lock().unwrap() = Some(plain_response(provider));
                }
                if transfer {
                    harness.after.release();
                } else {
                    harness.before.release();
                }
                harness.barrier(true).await;
                let expected_status = if transfer {
                    SpeculationStatus::Confirmed
                } else {
                    SpeculationStatus::Cancelled
                };
                if status_after_restart != expected_status {
                    harness.fail_after_cleanup(format!("signed source retirement status: {status_after_restart:?}, expected {expected_status:?}")).await;
                }
                if transfer {
                    wait(|| harness.pair.context().snapshot().unwrap().messages.len() > 1).await;
                    assert_eq!(capability.status(), SpeculationStatus::Released);
                    harness.send(Frame::new(FrameData::LlmRun)).await;
                    let (request, reply) = http.request().await;
                    if provider == Provider::Gemini {
                        assert_eq!(
                            request["contents"][0]["parts"],
                            json!([{"text":"It's ready!"}])
                        );
                        assert_eq!(request["contents"][1]["parts"], expected);
                    } else {
                        assert_eq!(
                            request["messages"][0]["content"],
                            json!([{"type":"text","text":"It's ready!"}])
                        );
                        assert_eq!(request["messages"][1]["content"], expected);
                    }
                    reply.send(response).unwrap();
                    harness.barrier(true).await;
                    assert_eq!(http.count(), 2);
                } else {
                    let count = http.count();
                    let buffered = harness.llm.speculation_snapshot().await.buffered_frames;
                    if count != 1 || buffered != 0 || !harness.quiet() {
                        harness
                            .fail_after_cleanup(format!(
                                "retired signed output: requests={count}, buffered={buffered}"
                            ))
                            .await;
                    }
                    assert!(harness.output.frames.lock().unwrap().iter().all(|f| f
                        .frame
                        .native_llm_continuation()
                        .unwrap()
                        .is_none()));
                    *http.automatic.lock().unwrap() = None;
                    harness.start().await;
                    harness.end_turn("Clean next turn").await;
                    let (request, reply) = http.request().await;
                    let body = request.to_string();
                    for canceled in [
                        "opaque+/",
                        "tail+/==",
                        "● exact",
                        "思考",
                        "its ready",
                        "It's ready!",
                    ] {
                        assert!(
                            !body.contains(canceled),
                            "retired source leaked {canceled} into next request"
                        );
                    }
                    reply.send(plain_response(provider)).unwrap();
                    harness.barrier(true).await;
                    assert_eq!(http.count(), 2);
                }
                harness.finish(false).await;
            }
        }
    }

    #[tokio::test]
    async fn rejected_and_noop_settings_preserve_live_prediction_and_socket() {
        let mut ws = WsPeer::new().await;
        let mut http = HttpPeer::new().await;
        let harness = Harness::new(
            &mut ws,
            &http,
            true,
            config(true),
            None,
            Provider::OpenAi,
            None,
        )
        .await;
        harness.start().await;
        harness.predict("settings unchanged").await;
        let (_, reply) = http.request().await;
        let prediction = harness.prediction();
        let initial = harness.source.processor.config_snapshot().unwrap().settings;
        for invalid in [
            json!({"enable_eager_end_of_turn":true}),
            json!({"keyterm":["must not install"],"enable_eager_end_of_turn":false}),
        ] {
            assert!(harness
                .source
                .processor
                .update_settings(&delta(invalid.clone()))
                .is_err());
            assert!(harness
                .source
                .processor
                .restart_with_settings(&delta(invalid), &harness.owner_context())
                .await
                .is_err());
            assert!(prediction.is_live());
            assert_eq!(
                harness.source.processor.config_snapshot().unwrap().settings,
                initial
            );
        }
        harness
            .source
            .processor
            .restart_with_settings(&SttSettings::default(), &harness.owner_context())
            .await
            .unwrap();
        assert!(prediction.is_live());
        assert!(ws.connections.try_recv().is_err());
        reply.send(answer("Settings preserved.")).unwrap();
        harness.buffered(3).await;
        harness.end_turn("settings unchanged").await;
        harness.barrier(true).await;
        assert_eq!(http.count(), 1);
        harness.finish(false).await;
    }

    #[tokio::test]
    async fn builder_without_eager_consumer_errors_without_ordinary_inference() {
        let mut ws = WsPeer::new().await;
        let http = HttpPeer::new().await;
        *http.automatic.lock().unwrap() = Some(answer("Unexpected unsupported-consumer response."));
        let harness = Harness::new(
            &mut ws,
            &http,
            true,
            UserAggregatorConfig::default(),
            None,
            Provider::OpenAi,
            None,
        )
        .await;
        harness
            .connection
            .batch(vec![
                json!({"type":"turn.start"}),
                json!({"type":"turn.eager_end","transcript":"unsupported provisional"}),
            ])
            .await;
        wait(|| {
            harness
                .worker
                .errors()
                .iter()
                .any(|e| e.error.contains("require an eager user aggregator"))
        })
        .await;
        harness.barrier(true).await;
        let count = http.count();
        let quiet = harness.quiet();
        harness.finish(true).await;
        assert_eq!(count, 0);
        assert!(quiet);
    }

    #[tokio::test]
    async fn live_never_predicted_receipt_cannot_close_or_change_an_unbound_current_turn() {
        for stop_first in [false, true] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            let harness = Harness::new(
                &mut ws,
                &http,
                true,
                config(true),
                Some(PauseAt::Receipts),
                Provider::OpenAi,
                None,
            )
            .await;
            harness.start().await;
            let old_turn = harness.turn();
            harness.end_turn("A is still live").await;
            harness.before.paused().await;
            harness.before.paused().await;
            let final_frame = harness
                .before
                .native(|v| matches!(v, NativeEagerSource::Final(_)));
            let stop = harness
                .before
                .native(|v| matches!(v, NativeEagerSource::Stop { .. }));
            harness.before.release();
            let mut unbound_start = Frame::new(FrameData::ProposedUserStartedSpeaking);
            unbound_start
                .metadata
                .insert("independent-unbound-turn".into(), true.into());
            harness.send_applied(unbound_start).await;
            harness.barrier(false).await;
            let mut text = Frame::new(FrameData::Transcription(TranscriptionFrame {
                text: "B ordinary final".into(),
                user_id: "typed-input".into(),
                timestamp: "typed-1".into(),
                finalized: true,
                ..Default::default()
            }));
            text.metadata
                .insert("independent-unbound-turn".into(), true.into());
            harness.send_applied(text).await;
            let deadline = harness.pair.user().deadline().unwrap();
            assert!(
                old_turn.is_live(),
                "this case must isolate consumer ownership from source liveness"
            );
            *http.automatic.lock().unwrap() = Some(answer("Unexpected foreign receipt response."));
            let ordered = if stop_first {
                [stop, final_frame]
            } else {
                [final_frame, stop]
            };
            for frame in ordered {
                harness.send_applied(new_id(frame)).await;
            }
            harness.barrier(true).await;
            let count = http.count();
            if count != 0
                || !harness.quiet()
                || harness.pair.user().deadline().unwrap() != deadline
                || harness
                    .output
                    .count(|v| matches!(v, FrameData::UserStoppedSpeaking))
                    != 0
            {
                harness
                    .fail_after_cleanup(format!(
                        "live non-current receipt changed B: requests={count}"
                    ))
                    .await;
            }
            *http.automatic.lock().unwrap() = None;
            harness
                .send_applied(Frame::new(FrameData::ProposedUserStoppedSpeaking))
                .await;
            let (request, reply) = http.request().await;
            assert_eq!(request["messages"][0]["content"], "B ordinary final");
            reply.send(answer("B only.")).unwrap();
            harness.barrier(true).await;
            assert_eq!(http.count(), 1);
            assert_eq!(harness.user_history(), ["B ordinary final"]);
            harness.finish(false).await;
        }
    }

    #[tokio::test]
    async fn empty_eager_events_withdraw_exact_prediction_without_closing_the_turn() {
        for empty in ["", " \t\n"] {
            for prior_prediction in [false, true] {
                let mut ws = WsPeer::new().await;
                let mut http = HttpPeer::new().await;
                let harness = Harness::new(
                    &mut ws,
                    &http,
                    true,
                    config(true),
                    None,
                    Provider::OpenAi,
                    None,
                )
                .await;
                harness.start().await;
                let prior = if prior_prediction {
                    harness.predict("withdraw this").await;
                    let (_, reply) = http.request().await;
                    Some((harness.prediction(), harness.after.capability(), reply))
                } else {
                    None
                };
                harness.predict(empty).await;
                if let Some((prediction, capability, reply)) = prior {
                    wait(|| !prediction.is_live()).await;
                    assert_eq!(capability.status(), SpeculationStatus::Cancelled);
                    drop(reply);
                }
                harness.predict("current nonempty").await;
                let (request, reply) = http.request().await;
                assert_eq!(request["messages"][0]["content"], "current nonempty");
                assert_eq!(http.count(), if prior_prediction { 2 } else { 1 });
                assert_eq!(harness.before.count(|v| matches!(v, FrameData::EagerTranscription(t) if t.text.trim().is_empty())), 0);
                reply.send(answer("Current answer.")).unwrap();
                harness.buffered(3).await;
                harness.end_turn("current nonempty").await;
                harness.barrier(true).await;
                assert_eq!(harness.user_history(), ["current nonempty"]);
                assert_eq!(
                    harness.output.count(|v| matches!(v, FrameData::LlmText(_))),
                    1
                );
                harness.finish(false).await;
            }
        }
    }

    #[tokio::test]
    async fn orphan_and_duplicate_provider_events_are_inert_and_eager_interims_are_suppressed() {
        let mut ws = WsPeer::new().await;
        let mut http = HttpPeer::new().await;
        let harness = Harness::new(
            &mut ws,
            &http,
            true,
            config(true),
            None,
            Provider::OpenAi,
            None,
        )
        .await;
        *http.automatic.lock().unwrap() = Some(answer("Unexpected orphan response."));
        harness
            .connection
            .batch(vec![
                json!({"type":"turn.eager_end","transcript":"orphan prediction"}),
                json!({"type":"turn.end","transcript":"orphan final"}),
                json!({"type":"turn.resume"}),
            ])
            .await;
        harness.start().await;
        harness.barrier(true).await;
        let count = http.count();
        if count != 0
            || !harness.quiet()
            || harness.before.count(|v| {
                matches!(
                    v,
                    FrameData::EagerTranscription(_) | FrameData::Transcription(_)
                )
            }) != 0
        {
            harness
                .fail_after_cleanup(format!(
                    "orphan provider events created authority: requests={count}"
                ))
                .await;
        }
        *http.automatic.lock().unwrap() = None;
        harness.connection.batch(vec![
            json!({"type":"turn.update","transcript":"stale interim must not escape","request_id":"same"}),
            json!({"type":"turn.end","transcript":"First legitimate final","request_id":"same"}),
        ]).await;
        http.request()
            .await
            .1
            .send(answer("First answer."))
            .unwrap();
        harness.barrier(true).await;
        *http.automatic.lock().unwrap() = Some(answer("Unexpected duplicate response."));
        harness.connection.batch(vec![
            json!({"type":"turn.end","transcript":"duplicate end","request_id":"same"}),
            json!({"type":"turn.eager_end","transcript":"stray prediction","request_id":"same"}),
            json!({"type":"turn.update","transcript":"out of turn interim"}),
        ]).await;
        harness.start().await;
        harness.barrier(true).await;
        let count = http.count();
        if count != 1
            || harness.before.count(|v| {
                matches!(
                    v,
                    FrameData::InterimTranscription(_) | FrameData::EagerTranscription(_)
                )
            }) != 0
            || harness
                .before
                .count(|v| matches!(v, FrameData::Transcription(_)))
                != 1
        {
            harness
                .fail_after_cleanup(format!(
                    "duplicate or interim provider events escaped: requests={count}"
                ))
                .await;
        }
        *http.automatic.lock().unwrap() = None;
        harness.end_turn("Second legitimate final").await;
        let (request, reply) = http.request().await;
        assert_eq!(
            request["messages"].as_array().unwrap().last().unwrap()["content"],
            "Second legitimate final"
        );
        reply.send(answer("Second answer.")).unwrap();
        harness.barrier(true).await;
        assert_eq!(http.count(), 2);
        assert_eq!(
            harness.user_history(),
            ["First legitimate final", "Second legitimate final"]
        );
        let final_turn = harness.turn();
        wait(|| {
            harness.before.frames.lock().unwrap().iter().any(|frame| {
                matches!(
                    frame.frame.native_eager_source(),
                    Some(NativeEagerSource::Stop { turn, .. }) if turn == &final_turn
                )
            })
        })
        .await;
        // The downstream completion barrier does not settle the upstream
        // recorder lane. After the native Stop, settle that normal FIFO too.
        let upstream_marker = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            event: None,
            returning: false,
            origin: Some("cartesia-eager-upstream-settled".into()),
        }));
        let upstream_marker_id = upstream_marker.id;
        let owner_context = harness.owner_context();
        within(owner_context.push(upstream_marker, Direction::Upstream))
            .await
            .unwrap();
        wait(|| {
            harness.upstream.frames.lock().unwrap().iter().any(|frame| {
                frame.direction == Direction::Upstream && frame.frame.id == upstream_marker_id
            })
        })
        .await;
        let upstream_suppressed = harness.upstream.frames.lock().unwrap().iter().all(|f| {
            !(f.direction == Direction::Upstream
                && matches!(
                    f.frame.data,
                    FrameData::ProposedUserStartedSpeaking | FrameData::ProposedUserStoppedSpeaking
                ))
        });
        harness.finish(false).await;
        assert!(upstream_suppressed);
    }

    fn plain_response(provider: Provider) -> String {
        if provider == Provider::Gemini {
            format!(
                "data: {}\n\n",
                json!({"candidates":[{
                    "content":{"parts":[{"text":"Current answer only."}]},"finishReason":"STOP"
                }]})
            )
        } else {
            [
                json!({"type":"message_start","message":{}}),
                json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"Current answer only."}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
                json!({"type":"message_stop"}),
            ].iter().map(|event| format!("data: {event}\n\n")).collect()
        }
    }

    #[tokio::test]
    async fn both_signed_adapters_discard_mismatch_resume_and_empty_final_before_next_request() {
        for provider in [Provider::Anthropic, Provider::Gemini] {
            for withdrawal in ["mismatch", "resume", "empty-final"] {
                let mut ws = WsPeer::new().await;
                let mut http = HttpPeer::new().await;
                let harness =
                    Harness::new(&mut ws, &http, true, config(true), None, provider, None).await;
                harness.start().await;
                if withdrawal == "empty-final" {
                    harness.non_speaking().await;
                }
                harness.predict("discard provisional signature").await;
                let (signed, _, buffered) = signed_response(provider);
                http.request().await.1.send(signed).unwrap();
                harness.buffered(buffered).await;
                let capability = harness.after.capability();
                harness.no_history();
                harness.no_output();
                if withdrawal == "resume" {
                    harness
                        .connection
                        .event(json!({"type":"turn.resume"}))
                        .await;
                    wait(|| !harness.prediction().is_live()).await;
                }
                harness
                    .end_turn(if withdrawal == "empty-final" {
                        ""
                    } else {
                        "Actual current final"
                    })
                    .await;
                if withdrawal == "empty-final" {
                    wait(|| {
                        harness.before.frames.lock().unwrap().iter().any(|f| {
                            matches!(
                                f.frame.native_eager_source(),
                                Some(NativeEagerSource::Stop { commit: None, .. })
                            )
                        })
                    })
                    .await;
                    let stop = harness
                        .before
                        .native(|v| matches!(v, NativeEagerSource::Stop { commit: None, .. }));
                    wait(|| harness.user.applied.lock().unwrap().contains(&stop.id)).await;
                    // Empty-stop recovery is the default configured policy.
                    let mut clock = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                        event: None,
                        returning: false,
                        origin: Some("cartesia-eager-clock".into()),
                    }));
                    clock.metadata.insert("advance_ms".into(), 6000.into());
                    harness.send_applied(clock).await;
                }
                let (current_request, reply) = http.request().await;
                assert_eq!(capability.status(), SpeculationStatus::Cancelled);
                let current_json = current_request.to_string();
                assert!(!current_json.contains("opaque+/"));
                assert!(!current_json.contains("tail+/=="));
                assert!(!current_json.contains("● exact"));
                assert!(!current_json.contains("思考"));
                reply.send(plain_response(provider)).unwrap();
                harness.barrier(true).await;
                if withdrawal == "empty-final" {
                    assert!(harness.user_history().is_empty());
                } else {
                    assert_eq!(harness.user_history(), ["Actual current final"]);
                }
                assert_eq!(
                    harness
                        .output
                        .count(|v| matches!(v, FrameData::LlmThoughtText(_))),
                    0
                );
                harness.send(Frame::new(FrameData::LlmRun)).await;
                let (next, reply) = http.request().await;
                let next_json = next.to_string();
                assert!(next_json.contains("Current answer only."));
                for canceled in [
                    "opaque+/",
                    "tail+/==",
                    "● exact",
                    "思考",
                    "discard provisional signature",
                ] {
                    assert!(
                        !next_json.contains(canceled),
                        "{withdrawal} leaked {canceled} into next provider request"
                    );
                }
                reply.send(plain_response(provider)).unwrap();
                harness.barrier(true).await;
                assert_eq!(http.count(), 3);
                harness.finish(false).await;
            }
        }
    }

    #[tokio::test]
    async fn source_end_retires_unsettled_start_before_reader_release_or_close_cleanup() {
        for interruptions in [false, true] {
            for retirement in [false, true] {
                let mut ws = WsPeer::new().await;
                let mut http = HttpPeer::new().await;
                *http.automatic.lock().unwrap() = Some(answer("Healthy watchdog control."));
                let mut user_config = config(interruptions);
                if !interruptions {
                    // Keep default interrupted recovery and make the otherwise
                    // identical non-interrupting healthy control observable.
                    user_config.empty_user_turn.as_mut().unwrap().idle_prompt =
                        Some(DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT.into());
                }
                let mut harness = Harness::new(
                    &mut ws,
                    &http,
                    true,
                    user_config,
                    Some(PauseAt::StartFlush),
                    Provider::OpenAi,
                    None,
                )
                .await;
                harness.connection.event(json!({"type":"turn.start"})).await;
                harness.before.paused().await;
                wait(|| {
                    harness
                        .output
                        .count(|v| matches!(v, FrameData::UserStartedSpeaking))
                        == 1
                })
                .await;
                harness.non_speaking().await;
                let turn = harness.turn();
                assert!(turn.is_active());
                if retirement {
                    harness.connection.auto_close.store(false, Ordering::SeqCst);
                    harness.finish_source().await;
                }
                let live_before_release = turn.is_live();
                // Advance the actual user state machine downstream of the held
                // source flush. Its scoped binding and armed watchdog are intact.
                let context = harness.user.context.lock().unwrap().clone().unwrap();
                harness
                    .user
                    .user
                    .advance(Duration::from_millis(6000))
                    .unwrap()
                    .emit(&context)
                    .await
                    .unwrap();
                let marker = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                    event: None,
                    returning: false,
                    origin: Some("pipecat.flows".into()),
                }));
                let marker_id = marker.id;
                context.push(marker, Direction::Downstream).await.unwrap();
                wait(|| harness.settled.observed(marker_id)).await;
                let count = http.count();
                let history = harness.pair.context().snapshot().unwrap();
                let healthy_request = if retirement {
                    None
                } else {
                    Some(http.request().await.0)
                };
                // Release and join before assertions used by cutoff mutants.
                harness.before.release();
                if retirement {
                    harness
                        .connection
                        .messages(vec![Message::Close(None)])
                        .await;
                }
                harness.finish(false).await;
                if retirement {
                    assert!(
                        !live_before_release,
                        "prepare_end left unresolved Start live until reader cleanup"
                    );
                    assert_eq!(
                        count, 0,
                        "unsettled retired Start admitted default recovery"
                    );
                    assert!(history.messages.is_empty());
                } else {
                    assert!(live_before_release);
                    assert_eq!(
                        count, 1,
                        "healthy sibling did not exercise watchdog recovery"
                    );
                    assert_eq!(healthy_request.unwrap()["messages"][0]["role"], "developer");
                }
            }
        }
    }

    #[tokio::test]
    async fn helper_proof_alone_cannot_retain_a_prediction_after_normal_eof() {
        for retain_native_receipts in [false, true] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            *http.automatic.lock().unwrap() = Some(answer("Native receipt retained."));
            let mut harness = Harness::new(
                &mut ws,
                &http,
                true,
                config(true),
                Some(PauseAt::PredictionAndReceipts),
                Provider::OpenAi,
                None,
            )
            .await;
            harness.start().await;
            let turn = harness.turn();
            let prior_requests = http.count();
            harness.predict("offered").await;
            harness.before.paused().await;
            let prediction = harness
                .before
                .native(|v| matches!(v, NativeEagerSource::Prediction(_)));
            harness.end_turn("offered").await;
            harness.before.paused().await;
            harness.before.paused().await;
            let receipts = retain_native_receipts.then(|| {
                [
                    harness
                        .before
                        .native(|v| matches!(v, NativeEagerSource::Final(_))),
                    harness
                        .before
                        .native(|v| matches!(v, NativeEagerSource::Stop { .. })),
                ]
            });
            // This recorder swallowed the genuine receipts. No downstream
            // recorder or consumer holds one; drop every retained native clone.
            harness.before.frames.lock().unwrap().retain(|f| {
                !matches!(
                    f.frame.native_eager_source(),
                    Some(NativeEagerSource::Final(_) | NativeEagerSource::Stop { .. })
                )
            });
            harness.finish_source().await;
            wait(|| !turn.is_active()).await;
            let live_after_eof = turn.is_live();
            harness.before.release();
            harness.send_applied(prediction).await;
            if let Some(receipts) = receipts {
                for receipt in receipts {
                    harness.send_applied(receipt).await;
                }
                let (request, _) = http.request().await;
                assert_eq!(request["messages"][0]["content"], "offered");
            }
            let observed_status = match harness
                .settle_or_observe_pending_request(&http, prior_requests, "helper-only-normal-eof")
                .await
            {
                Ok(status) => status,
                Err(error) => {
                    harness
                        .fail_pending_fixture_after_cleanup(ws, http, error)
                        .await
                }
            };
            let count = http.count();
            let history = harness.user_history();
            let cleanup = harness
                .finish_pending_fixture(
                    &mut ws,
                    &mut http,
                    observed_status == Some(SpeculationStatus::Pending),
                )
                .await;
            assert_eq!(
                live_after_eof, retain_native_receipts,
                "helper-only-normal-eof cleanup: {cleanup:?}"
            );
            assert_eq!(count, usize::from(retain_native_receipts));
            assert_eq!(
                history,
                if retain_native_receipts {
                    vec!["offered".to_string()]
                } else {
                    vec![]
                }
            );
            assert!(
                cleanup.is_empty(),
                "helper-only-normal-eof cleanup: {cleanup:?}"
            );
        }
    }

    #[tokio::test]
    async fn source_health_generation_restoration_never_revives_queued_or_pending_work() {
        for false_then_true in [false, true] {
            for phase in ["prediction", "context", "http", "final"] {
                let mut ws = WsPeer::new().await;
                let mut http = HttpPeer::new().await;
                let pause = match phase {
                    "prediction" => Some(PauseAt::Prediction),
                    "context" => Some(PauseAt::Context),
                    "final" => Some(PauseAt::Final),
                    _ => None,
                };
                let mut harness = Harness::new(
                    &mut ws,
                    &http,
                    true,
                    config(true),
                    pause,
                    Provider::OpenAi,
                    None,
                )
                .await;
                harness.start().await;
                harness.predict("stale generation").await;
                let reply = match phase {
                    "prediction" => {
                        harness.before.paused().await;
                        None
                    }
                    "context" => {
                        harness.after.paused().await;
                        None
                    }
                    _ => Some(http.request().await.1),
                };
                let prediction = harness.prediction();
                assert!(prediction.is_live());
                if phase == "final" {
                    harness.end_turn("stale generation").await;
                    harness.before.paused().await;
                }
                let health = harness.source.processor.health().unwrap();
                let generation = health.generation();
                if false_then_true {
                    health.set_usable(false);
                }
                health.set_usable(true);
                assert_ne!(health.generation(), generation);
                let retired_without_cleanup = !prediction.is_live();
                *http.automatic.lock().unwrap() =
                    Some(answer("Unexpected stale-generation fallback."));
                if let Some(reply) = reply {
                    let _ = reply.send(answer("Stale generation output."));
                }
                match phase {
                    "prediction" | "final" => harness.before.release(),
                    "context" => harness.after.release(),
                    _ => (),
                }
                harness.barrier(true).await;
                let count = http.count();
                if !retired_without_cleanup
                    || count != usize::from(matches!(phase, "http" | "final"))
                    || !harness.quiet()
                {
                    harness.fail_after_cleanup(format!("stale generation admitted work: phase={phase}, requests={count}, retired={retired_without_cleanup}")).await;
                }
                *http.automatic.lock().unwrap() = None;
                harness.restart(&mut ws).await;
                harness.start().await;
                harness.end_turn("Fresh generation").await;
                http.request()
                    .await
                    .1
                    .send(answer("Fresh answer."))
                    .unwrap();
                harness.barrier(true).await;
                assert_eq!(harness.user_history(), ["Fresh generation"]);
                assert_eq!(
                    harness
                        .output
                        .count(|v| matches!(v, FrameData::LlmText(t) if t.text == "Fresh answer.")),
                    1
                );
                harness.finish(false).await;
            }
        }
    }

    #[tokio::test]
    async fn actor_skipped_start_retires_no_final_before_eof_and_preserves_installed_receipt() {
        for installed_receipt in [false, true] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            *http.automatic.lock().unwrap() = Some(answer("Only installed A."));
            let mut harness = Harness::new(
                &mut ws,
                &http,
                true,
                config(true),
                Some(PauseAt::PredictionAndReceipts),
                Provider::OpenAi,
                None,
            )
            .await;
            harness.start().await;
            // A genuine mapped prediction witnesses that the reader installed
            // Ready after its flush acknowledgment, without adopting inference.
            harness.predict("ready witness").await;
            harness.before.paused().await;
            harness.non_speaking().await;
            let turn = harness.turn();
            let receipts = if installed_receipt {
                harness.end_turn("Installed A final").await;
                harness.before.paused().await;
                harness.before.paused().await;
                Some([
                    harness
                        .before
                        .native(|v| matches!(v, NativeEagerSource::Final(_))),
                    harness
                        .before
                        .native(|v| matches!(v, NativeEagerSource::Stop { .. })),
                ])
            } else {
                None
            };
            harness.connection.auto_close.store(false, Ordering::SeqCst);
            harness.finish_source().await;
            harness
                .connection
                .messages(vec![
                    Message::Text(
                        json!({"type":"turn.start","transcript":"B skipped"})
                            .to_string()
                            .into(),
                    ),
                    Message::Ping(vec![31].into()),
                ])
                .await;
            assert_eq!(
                harness.connection.wire().await,
                Message::Pong(vec![31].into())
            );
            let live_after_skipped_start = turn.is_live();
            harness
                .connection
                .messages(vec![
                    Message::Text(
                        json!({"type":"turn.end","transcript":"B forbidden tail"})
                            .to_string()
                            .into(),
                    ),
                    Message::Ping(vec![32].into()),
                ])
                .await;
            assert_eq!(
                harness.connection.wire().await,
                Message::Pong(vec![32].into())
            );
            if !installed_receipt {
                harness.advance(6000).await;
            }
            let before_release_count = http.count();
            harness.before.release();
            harness
                .connection
                .messages(vec![Message::Close(None)])
                .await;
            wait(|| !turn.is_active()).await;
            if let Some(receipts) = receipts {
                for receipt in receipts {
                    harness.send_applied(receipt).await;
                }
                let (request, _) = http.request().await;
                assert_eq!(request["messages"][0]["content"], "Installed A final");
            }
            harness.barrier(true).await;
            let count = http.count();
            let history = harness.user_history();
            harness.finish(false).await;
            assert_eq!(live_after_skipped_start, installed_receipt);
            assert_eq!(
                before_release_count, 0,
                "skipped Start left no-final recovery active before EOF"
            );
            assert_eq!(count, usize::from(installed_receipt));
            assert_eq!(
                history,
                if installed_receipt {
                    vec!["Installed A final".to_string()]
                } else {
                    vec![]
                }
            );
        }
    }

    #[tokio::test]
    async fn delayed_genuine_usage_and_old_receipts_do_not_retarget_current_turn_or_recovery() {
        for restart in [false, true] {
            for recover in [false, true] {
                let mut ws = WsPeer::new().await;
                let mut http = HttpPeer::new().await;
                *http.automatic.lock().unwrap() = Some(answer("B response."));
                let mut harness = Harness::new(
                    &mut ws,
                    &http,
                    true,
                    config(true),
                    Some(PauseAt::Receipts),
                    Provider::OpenAi,
                    None,
                )
                .await;
                harness.audio().await;
                harness.start().await;
                let old_turn = harness.turn();
                harness.end_turn("A withheld final").await;
                harness.before.paused().await;
                harness.before.paused().await;
                let old_final = harness
                    .before
                    .native(|v| matches!(v, NativeEagerSource::Final(_)));
                let old_stop = harness
                    .before
                    .native(|v| matches!(v, NativeEagerSource::Stop { .. }));
                let usage = harness.before.frames.lock().unwrap().iter().find_map(|f| {
                    matches!(&f.frame.data, FrameData::Metrics(values) if values.iter().any(|m|
                        matches!(m, Metric::SttUsage { audio_seconds, .. } if *audio_seconds > 0.0)))
                        .then(|| f.frame.clone())
                }).expect("actual Cartesia PCM usage metric");
                harness.before.release();
                if restart {
                    harness.restart(&mut ws).await;
                }
                harness.start().await;
                let current = harness.turn();
                assert_ne!(old_turn, current, "provider IDs cannot be native identity");
                assert!(!old_turn.is_live());
                assert!(current.is_live());
                let mut stopped = Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                    duration_secs: 0.0,
                    timestamp: 0.0,
                }));
                stopped
                    .metadata
                    .insert("cartesia-exact-time-ms".into(), 1000.into());
                harness.send_applied(stopped).await;
                let deadline = harness.pair.user().deadline().unwrap();
                let idle = harness.pair.user().idle_deadline().unwrap();
                let interim = harness.pair.user().interim_text().unwrap();
                let history = harness.pair.context().snapshot().unwrap().messages;
                assert!(deadline.is_some());
                for frame in [old_stop, old_final] {
                    harness.send_applied(new_id(frame)).await;
                }
                // Compare a delayed real A metric with an ordinary same-time
                // clock input at B. Neither is permitted to reset B's state.
                for mut frame in [Frame::new(FrameData::Metrics(vec![])), new_id(usage)] {
                    frame
                        .metadata
                        .insert("cartesia-exact-time-ms".into(), 1000.into());
                    harness.send_applied(frame).await;
                    if harness.pair.user().deadline().unwrap() != deadline
                        || harness.pair.user().idle_deadline().unwrap() != idle
                        || harness.pair.user().interim_text().unwrap() != interim
                        || harness.pair.context().snapshot().unwrap().messages != history
                        || !current.is_live()
                    {
                        harness
                            .fail_after_cleanup("delayed genuine usage retargeted B state".into())
                            .await;
                    }
                }
                harness.barrier(true).await;
                let count = http.count();
                if count != 0 {
                    harness
                        .fail_after_cleanup(format!(
                            "old usage or receipts admitted a request: {count}"
                        ))
                        .await;
                }
                if recover {
                    harness.advance(7000).await;
                } else {
                    harness.end_turn("B exact final").await;
                }
                let (request, _) = http.request().await;
                harness.barrier(true).await;
                if recover {
                    assert_eq!(request["messages"][0]["role"], "developer");
                    assert_eq!(
                        request["messages"][0]["content"],
                        DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT
                    );
                    assert!(harness.user_history().is_empty());
                } else {
                    assert_eq!(request["messages"][0]["content"], "B exact final");
                    assert_eq!(harness.user_history(), ["B exact final"]);
                }
                assert_eq!(http.count(), 1);
                harness.finish(false).await;
            }
        }
    }

    #[tokio::test]
    async fn provider_wire_oversize_is_a_destructive_fault_without_unbound_fallback() {
        for event_kind in ["turn.eager_end", "turn.end"] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            let mut harness = Harness::new(
                &mut ws,
                &http,
                true,
                config(true),
                None,
                Provider::OpenAi,
                None,
            )
            .await;
            harness.start().await;
            harness.predict("valid pending prediction").await;
            let (_, reply) = http.request().await;
            let prediction = harness.prediction();
            let overflow = json!({"type":event_kind,
                "transcript":"x".repeat(ws.client.config().limits.max_message_bytes + 1)});
            assert!(overflow.to_string().len() > ws.client.config().limits.max_message_bytes);
            harness.connection.event(overflow).await;
            wait(|| !harness.worker.errors().is_empty()).await;
            let retired = !prediction.is_live();
            *http.automatic.lock().unwrap() = Some(answer("Unexpected oversize fallback."));
            let _ = reply.send(answer("Discard overflow predecessor."));
            harness.barrier(true).await;
            let count = http.count();
            if !retired || count != 1 || !harness.quiet() {
                harness
                    .fail_after_cleanup(format!(
                        "oversize source admitted fallback: retired={retired}, requests={count}"
                    ))
                    .await;
            }
            assert_eq!(
                harness.before.count(|v| matches!(v,
                FrameData::Transcription(t) | FrameData::EagerTranscription(t)
                    if t.text.len() > ws.client.config().limits.max_message_bytes)),
                0
            );
            assert!(harness
                .worker
                .errors()
                .iter()
                .all(|error| !error.error.contains("loopback-cartesia-key")
                    && !error.error.contains("xxxxxxxx")));
            *http.automatic.lock().unwrap() = None;
            harness.restart(&mut ws).await;
            harness.start().await;
            harness.end_turn("Healthy replacement").await;
            http.request()
                .await
                .1
                .send(answer("Replacement answer."))
                .unwrap();
            harness.barrier(true).await;
            assert_eq!(harness.user_history(), ["Healthy replacement"]);
            assert_eq!(http.count(), 2);
            harness.finish(true).await;
        }
    }

    #[tokio::test]
    async fn retained_native_record_capacity_fault_retires_the_provider_source() {
        let mut ws = WsPeer::new().await;
        let mut http = HttpPeer::new().await;
        *http.automatic.lock().unwrap() = Some(answer("Unexpected capacity-fault response."));
        let mut harness = Harness::new(
            &mut ws,
            &http,
            true,
            config(true),
            Some(PauseAt::PredictionAndReceipts),
            Provider::OpenAi,
            None,
        )
        .await;
        harness.start().await;
        // Core allows 64 total retained records. One is the real native Start;
        // each genuine captured prediction retains another passive record.
        for index in 0..63 {
            harness
                .predict(&format!("retained prediction {index}"))
                .await;
            harness.before.paused().await;
        }
        let last = harness.prediction();
        assert!(last.is_live());
        harness.predict("capacity overflow").await;
        wait(|| !harness.worker.errors().is_empty()).await;
        let retired = !last.is_live();
        harness.barrier(true).await;
        let count = http.count();
        let predictions = harness
            .before
            .count(|v| matches!(v, FrameData::EagerTranscription(_)));
        if !retired
            || count != 0
            || predictions != 63
            || !harness.quiet()
            || !harness
                .worker
                .errors()
                .iter()
                .any(|e| e.error.contains("capacity"))
        {
            harness.before.release();
            harness.fail_after_cleanup(format!("capacity fault admission: retired={retired}, requests={count}, predictions={predictions}")).await;
        }
        harness.before.release();
        *http.automatic.lock().unwrap() = None;
        harness.restart(&mut ws).await;
        harness.start().await;
        harness.end_turn("Fresh bounded source").await;
        http.request()
            .await
            .1
            .send(answer("Capacity recovered."))
            .unwrap();
        harness.barrier(true).await;
        assert_eq!(http.count(), 1);
        assert_eq!(harness.user_history(), ["Fresh bounded source"]);
        harness.finish(true).await;
    }

    struct PendingEffect {
        polls: Arc<AtomicUsize>,
        effects: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
        ready: Arc<AtomicBool>,
        waker: Arc<futures_util::task::AtomicWaker>,
    }

    impl std::future::Future for PendingEffect {
        type Output = Result<Value, String>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            self.waker.register(cx.waker());
            self.polls.fetch_add(1, Ordering::SeqCst);
            if self.ready.load(Ordering::SeqCst) {
                self.effects.fetch_add(1, Ordering::SeqCst);
                std::task::Poll::Ready(Ok(json!({"booked":true})))
            } else {
                std::task::Poll::Pending
            }
        }
    }

    impl Drop for PendingEffect {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn actual_pending_tool_survives_stt_retirement_but_llm_owner_stops_later_effect_polling()
    {
        for retire_llm in [false, true] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            let constructions = Arc::new(AtomicUsize::new(0));
            let polls = Arc::new(AtomicUsize::new(0));
            let effects = Arc::new(AtomicUsize::new(0));
            let dropped = Arc::new(AtomicUsize::new(0));
            let ready = Arc::new(AtomicBool::new(false));
            let waker = Arc::new(futures_util::task::AtomicWaker::new());
            let mut tools = ToolExecutor::new(1, 8, 4096).unwrap();
            let mut registered = RegisteredTool::new(Arc::new({
                let constructions = constructions.clone();
                let polls = polls.clone();
                let effects = effects.clone();
                let dropped = dropped.clone();
                let ready = ready.clone();
                let waker = waker.clone();
                move |_| {
                    constructions.fetch_add(1, Ordering::SeqCst);
                    Box::pin(PendingEffect {
                        polls: polls.clone(),
                        effects: effects.clone(),
                        dropped: dropped.clone(),
                        ready: ready.clone(),
                        waker: waker.clone(),
                    })
                }
            }));
            registered.cancel_on_interruption = false;
            tools.register("book", registered).unwrap();
            let tools = Arc::new(tools);
            let mut harness = Harness::new(
                &mut ws,
                &http,
                true,
                config(true),
                Some(PauseAt::Boundary),
                Provider::OpenAi,
                Some(tools.clone()),
            )
            .await;
            harness.start().await;
            harness.predict("book after transfer").await;
            let (_, reply) = http.request().await;
            harness.end_turn("book after transfer").await;
            harness.after.paused().await;
            let capability = harness.after.capability();
            assert_eq!(capability.status(), SpeculationStatus::Confirmed);
            reply.send(tool_response("pending-poll-book")).unwrap();
            harness.buffered(3).await;
            harness.after.release();
            wait(|| polls.load(Ordering::SeqCst) > 0).await;
            assert_eq!(constructions.load(Ordering::SeqCst), 1);
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            harness.restart(&mut ws).await;
            assert_eq!(
                dropped.load(Ordering::SeqCst),
                0,
                "STT retired transferred effect authority"
            );
            let admitted_polls = polls.load(Ordering::SeqCst);
            if retire_llm {
                let health = harness.llm.health().unwrap();
                health.set_usable(false);
                health.set_usable(true);
            }
            ready.store(true, Ordering::SeqCst);
            waker.wake();
            wait(|| dropped.load(Ordering::SeqCst) == 1).await;
            wait(|| tools.outcomes().snapshot().active.is_empty()).await;
            if !retire_llm {
                http.request()
                    .await
                    .1
                    .send(answer("Effect completed."))
                    .unwrap();
            }
            harness.barrier(true).await;
            let observed_polls = polls.load(Ordering::SeqCst);
            let observed_effects = effects.load(Ordering::SeqCst);
            let count = http.count();
            harness.finish(false).await;
            if retire_llm {
                assert_eq!(
                    observed_polls, admitted_polls,
                    "retired LLM admitted another callback poll"
                );
                assert_eq!(observed_effects, 0);
                assert_eq!(count, 1);
            } else {
                assert!(observed_polls > admitted_polls);
                assert_eq!(observed_effects, 1);
                assert_eq!(count, 2);
            }
        }
    }

    async fn settle_update_prefix(
        harness: &mut Harness,
        raw_capacity: usize,
        updates: bool,
        ping: u8,
    ) {
        let mut messages = Vec::new();
        if updates {
            // The actual event queue has C = limits.queue_capacity slots (8 in
            // this peer). Emitting 2*C+1 Updates (17 here) before Ping requires
            // at least C+1 raw dequeues. One reader maps synchronously between
            // dequeue and its next await, so at least C mappings have completed
            // before Pong. These tests explicitly use a current-thread runtime.
            // No prediction or speaking control supplies this witness, and
            // Pong does not acknowledge the entire Update batch.
            for index in 0..(2 * raw_capacity + 1) {
                messages.push(Message::Text(
                    json!({
                        "type":"turn.update",
                        "transcript":format!("suppressed partial {ping} {index}"),
                    })
                    .to_string()
                    .into(),
                ));
            }
        }
        messages.push(Message::Ping(vec![ping].into()));
        harness.connection.messages(messages).await;
        assert_eq!(
            harness.connection.wire().await,
            Message::Pong(vec![ping].into())
        );
        // Any incorrectly emitted interim from that prefix precedes this FIFO
        // marker. The later native Final/Stop witnesses the entire batch drain.
        harness.barrier(true).await;
    }

    fn received_bodies(http: &mut HttpPeer) -> Vec<Value> {
        let mut bodies = Vec::new();
        while let Ok((body, _)) = http.requests.try_recv() {
            bodies.push(body);
        }
        bodies
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_only_long_speech_needs_no_interim_refresh_before_exact_final() {
        for updates in [false, true] {
            let mut ws = WsPeer::new().await;
            let mut http = HttpPeer::new().await;
            *http.automatic.lock().unwrap() = Some(answer("One long-speech answer."));
            let mut user_config = config(true);
            user_config.turn.stop_timeout = Duration::from_secs(5);
            let raw_capacity = ws.client.config().limits.queue_capacity;
            let mut harness = Harness::new(
                &mut ws,
                &http,
                true,
                user_config,
                Some(PauseAt::Receipts),
                Provider::OpenAi,
                None,
            )
            .await;
            harness.start().await;
            let mut speaking_deadlines = vec![harness.pair.user().deadline().unwrap()];
            for (now, ping) in [(6000, 41), (12000, 42), (20000, 43)] {
                // This is real aggregator state-machine time, not a sleep.
                harness.advance(now).await;
                settle_update_prefix(&mut harness, raw_capacity, updates, ping).await;
                speaking_deadlines.push(harness.pair.user().deadline().unwrap());
            }
            harness.end_turn("Long provider-owned final").await;
            harness.before.paused().await;
            harness.before.paused().await;
            let final_frame = harness
                .before
                .native(|v| matches!(v, NativeEagerSource::Final(_)));
            let stop = harness.before.native(|v| {
                matches!(
                    v,
                    NativeEagerSource::Stop {
                        commit: Some(_),
                        ..
                    }
                )
            });
            harness.barrier(true).await;
            let requests_while_speaking = http.count();
            let history_while_speaking = harness.pair.context().snapshot().unwrap().messages;
            let outputs_while_speaking = harness.completion_outputs();
            let stopped_while_speaking = harness
                .output
                .count(|v| matches!(v, FrameData::UserStoppedSpeaking));
            let interim_traffic = harness
                .before
                .count(|v| matches!(v, FrameData::InterimTranscription(_)));
            let interim_text = harness.pair.user().interim_text().unwrap();
            let transcript_ring = harness.pair.user().transcript_frames().unwrap();
            harness.before.release();
            harness.send_applied(final_frame).await;
            harness.send_applied(stop).await;
            harness.barrier(true).await;
            harness.advance(40000).await;
            let requests = http.count();
            let bodies = received_bodies(&mut http);
            let history = harness.user_history();
            let outputs = harness.output.count(
                |v| matches!(v, FrameData::LlmText(t) if t.text == "One long-speech answer."),
            );
            let stops = harness
                .output
                .count(|v| matches!(v, FrameData::UserStoppedSpeaking));
            harness.finish(false).await;
            assert!(
                speaking_deadlines.iter().all(Option::is_none),
                "audible provider-only speech acquired a watchdog deadline: {speaking_deadlines:?}"
            );
            assert_eq!(
                requests_while_speaking, 0,
                "long speaking turn inferred before its final"
            );
            assert!(history_while_speaking.is_empty());
            assert_eq!(outputs_while_speaking, 0);
            assert_eq!(stopped_while_speaking, 0);
            assert_eq!(interim_traffic, 0);
            assert!(interim_text.is_empty());
            assert!(
                transcript_ring.is_empty(),
                "suppressed Updates entered the transcript ring"
            );
            assert_eq!(requests, 1);
            assert_eq!(bodies.len(), 1);
            assert_eq!(bodies[0]["messages"][0]["role"], "user");
            assert_eq!(
                bodies[0]["messages"][0]["content"],
                "Long provider-owned final"
            );
            assert_eq!(history, ["Long provider-owned final"]);
            assert_eq!(outputs, 1);
            assert_eq!(stops, 1);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mixed_vad_updates_do_not_extend_timeout_and_late_source_final_cannot_duplicate_recovery(
    ) {
        for updates in [false, true] {
            for late_final in [false, true] {
                let mut ws = WsPeer::new().await;
                let mut http = HttpPeer::new().await;
                *http.automatic.lock().unwrap() = Some(answer("One mixed-input answer."));
                let mut user_config = config(true);
                user_config.turn.stop_timeout = Duration::from_secs(5);
                let raw_capacity = ws.client.config().limits.queue_capacity;
                let mut harness = Harness::new(
                    &mut ws,
                    &http,
                    true,
                    user_config,
                    None,
                    Provider::OpenAi,
                    None,
                )
                .await;
                harness.start().await;
                let turn = harness.turn();
                let mut vad_stop =
                    Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                        duration_secs: 0.0,
                        timestamp: 0.0,
                    }));
                vad_stop
                    .metadata
                    .insert("cartesia-exact-time-ms".into(), 1000.into());
                vad_stop
                    .metadata
                    .insert("mixed-input-vad-stop".into(), true.into());
                harness.send_applied(vad_stop).await;
                let mut deadlines = vec![harness.pair.user().deadline().unwrap()];
                harness.advance(3000).await;
                settle_update_prefix(&mut harness, raw_capacity, updates, 51).await;
                deadlines.push(harness.pair.user().deadline().unwrap());
                harness.advance(5999).await;
                settle_update_prefix(&mut harness, raw_capacity, updates, 52).await;
                deadlines.push(harness.pair.user().deadline().unwrap());
                let requests_before_edge = http.count();
                let stops_before_edge = harness
                    .output
                    .count(|v| matches!(v, FrameData::UserStoppedSpeaking));
                if late_final {
                    // Healthy control: the default interrupted prompt becomes
                    // due exactly 5s after the labeled VAD stop, despite Updates.
                    harness.advance(6000).await;
                }
                let requests_at_edge = http.count();
                let deadline_at_edge = harness.pair.user().deadline().unwrap();
                let live_before_final = turn.is_live();
                if late_final {
                    harness.advance(6001).await;
                }
                // Before: t=5999. After: t=6001, following a settled recovery.
                // These are actual provider End events, not injected finals.
                harness.end_turn("Mixed-input final").await;
                wait(|| {
                    harness.before.frames.lock().unwrap().iter().any(|f| {
                        matches!(
                            f.frame.native_eager_source(),
                            Some(NativeEagerSource::Stop {
                                commit: Some(_),
                                ..
                            })
                        )
                    })
                })
                .await;
                let final_frame = harness
                    .before
                    .native(|v| matches!(v, NativeEagerSource::Final(_)));
                let stop = harness.before.native(|v| {
                    matches!(
                        v,
                        NativeEagerSource::Stop {
                            commit: Some(_),
                            ..
                        }
                    )
                });
                wait(|| harness.user.applied.lock().unwrap().contains(&stop.id)).await;
                harness.barrier(true).await;
                for frame in [final_frame, stop] {
                    harness.send_applied(new_id(frame)).await;
                }
                harness.advance(20000).await;
                let requests = http.count();
                let bodies = received_bodies(&mut http);
                let history = harness.user_history();
                let messages = harness.pair.context().snapshot().unwrap().messages;
                let outputs = harness.output.count(
                    |v| matches!(v, FrameData::LlmText(t) if t.text == "One mixed-input answer."),
                );
                let stops = harness
                    .output
                    .count(|v| matches!(v, FrameData::UserStoppedSpeaking));
                let interim_traffic = harness
                    .before
                    .count(|v| matches!(v, FrameData::InterimTranscription(_)));
                let ring_has_interim = harness
                    .pair
                    .user()
                    .transcript_frames()
                    .unwrap()
                    .iter()
                    .any(|f| matches!(f.data, FrameData::InterimTranscription(_)));
                let final_deadline = harness.pair.user().deadline().unwrap();
                harness.finish(false).await;
                assert!(
                    deadlines
                        .iter()
                        .all(|deadline| *deadline == Some(Duration::from_millis(6000))),
                    "suppressed Updates refreshed the VAD-stop watchdog: {deadlines:?}"
                );
                assert_eq!(requests_before_edge, 0);
                assert_eq!(stops_before_edge, 0);
                assert_eq!(requests_at_edge, usize::from(late_final));
                assert_eq!(
                    deadline_at_edge,
                    if late_final {
                        None
                    } else {
                        Some(Duration::from_millis(6000))
                    }
                );
                assert!(
                    live_before_final,
                    "late rejection must use consumer turn ownership, not source retirement"
                );
                assert_eq!(requests, 1, "mixed input produced duplicate inference");
                assert_eq!(bodies.len(), 1);
                assert_eq!(outputs, 1);
                assert_eq!(stops, 1);
                assert_eq!(interim_traffic, 0);
                assert!(!ring_has_interim);
                assert!(final_deadline.is_none());
                if late_final {
                    assert_eq!(bodies[0]["messages"][0]["role"], "developer");
                    assert_eq!(
                        bodies[0]["messages"][0]["content"],
                        DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT
                    );
                    assert!(
                        history.is_empty(),
                        "late source final was committed after watchdog recovery"
                    );
                    assert_eq!(
                        messages
                            .iter()
                            .filter(|m| m.role == pipecat_services::Role::Developer)
                            .count(),
                        1
                    );
                } else {
                    assert_eq!(bodies[0]["messages"][0]["role"], "user");
                    assert_eq!(bodies[0]["messages"][0]["content"], "Mixed-input final");
                    assert_eq!(history, ["Mixed-input final"]);
                    assert_eq!(
                        messages
                            .iter()
                            .filter(|m| m.role == pipecat_services::Role::Developer)
                            .count(),
                        0
                    );
                }
            }
        }
    }

    include!("reviewer/observations.rs");
    include!("reviewer/crosssocket_effects.rs");
}

include!("reviewer/default.rs");
