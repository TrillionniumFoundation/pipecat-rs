//! Real pipeline and loopback HTTP transport; synthetic audio and verdicts only.
//! These tests do not load ONNX or qualify a deployed Smart Turn service.
#![cfg(feature = "http-smart-turn")]

use pipecat::{
    core::*,
    services::{Content, Context},
    turns::{SharedContext, SmartTurnUserAggregator, UserAggregatorConfig, UserTurnConfig},
    vad::{HttpSmartTurnConfig, HttpSmartTurnPredictor, SmartTurnParams},
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);
const PCM: [i16; 4] = [i16::MIN, -1, 0, i16::MAX];

#[derive(Debug)]
struct Exchange {
    headers: String,
    body: Vec<u8>,
    peer_closed_without_reply: bool,
}

struct Request {
    reply: oneshot::Sender<(u8, f64)>,
    closed: oneshot::Receiver<bool>,
}

struct ActiveConnection(Arc<AtomicUsize>);
impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn read_request(socket: &mut TcpStream) -> Result<(String, Vec<u8>), String> {
    const LIMIT: usize = 64 * 1024;
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let header_end = loop {
        let count = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("HTTP peer closed before request headers".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > LIMIT {
            return Err("loopback request exceeded fixture limit".into());
        }
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec()).map_err(|e| e.to_string())?;
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>())
        })
        .ok_or("missing request Content-Length")?
        .map_err(|e| e.to_string())?;
    if length > LIMIT - header_end {
        return Err("loopback request body exceeded fixture limit".into());
    }
    while bytes.len() < header_end + length {
        let count = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("HTTP peer closed before request body".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > LIMIT {
            return Err("loopback request exceeded fixture limit".into());
        }
    }
    Ok((headers, bytes[header_end..header_end + length].to_vec()))
}

async fn exchange(
    mut socket: TcpStream,
    requests: &mpsc::UnboundedSender<Request>,
) -> Result<Exchange, String> {
    let (headers, body) = read_request(&mut socket).await?;
    let (reply, response) = oneshot::channel();
    let (closed, observed_close) = oneshot::channel();
    requests
        .send(Request {
            reply,
            closed: observed_close,
        })
        .map_err(|_| "request observation receiver closed")?;
    let mut probe = [0; 1];
    let verdict = tokio::select! {
        response = async {
            // Dropping a test's reply handle leaves the peer waiting, so only
            // actual client EOF can count as cancellation evidence.
            match response.await {
                Ok(verdict) => verdict,
                Err(_) => std::future::pending().await,
            }
        } => Some(response),
        result = socket.read(&mut probe) => {
            match result {
                Ok(0) => None,
                Ok(_) => return Err("unexpected bytes after complete HTTP request".into()),
                Err(error) if matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted) => None,
                Err(error) => return Err(error.to_string()),
            }
        }
    };
    if let Some((prediction, probability)) = verdict {
        let body = serde_json::json!({"prediction": prediction, "probability": probability});
        let body = body.to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket
            .write_all(response.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        socket.shutdown().await.map_err(|e| e.to_string())?;
    }
    let peer_closed_without_reply = verdict.is_none();
    let _ = closed.send(peer_closed_without_reply);
    Ok(Exchange {
        headers,
        body,
        peer_closed_without_reply,
    })
}

async fn serve(
    listener: TcpListener,
    requests: mpsc::UnboundedSender<Request>,
    active: Arc<AtomicUsize>,
    mut stop: oneshot::Receiver<()>,
) -> Result<Vec<Exchange>, String> {
    let mut exchanges = Vec::new();
    loop {
        let (socket, _) = tokio::select! {
            biased;
            _ = &mut stop => return Ok(exchanges),
            result = listener.accept() => result.map_err(|e| e.to_string())?,
        };
        active.fetch_add(1, Ordering::SeqCst);
        let guard = ActiveConnection(active.clone());
        let completed = tokio::select! {
            biased;
            _ = &mut stop => return Ok(exchanges),
            result = exchange(socket, &requests) => result?,
        };
        drop(guard);
        exchanges.push(completed);
    }
}

#[derive(Default)]
struct Tap(Mutex<Vec<DirectedFrame>>);
#[async_trait]
impl FrameProcessor for Tap {
    fn name(&self) -> &str {
        "http-smart-turn-tap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0
            .lock()
            .map_err(|_| ProcessorError::fatal("tap poisoned"))?
            .push(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}

struct Fixture {
    user: Arc<SmartTurnUserAggregator>,
    tap: Arc<Tap>,
    worker: PipelineWorker,
    requests: mpsc::UnboundedReceiver<Request>,
    active: Arc<AtomicUsize>,
    stop: oneshot::Sender<()>,
    server: JoinHandle<Result<Vec<Exchange>, String>>,
    pending_close: Option<oneshot::Receiver<bool>>,
}

async fn join_server(
    server: &mut JoinHandle<Result<Vec<Exchange>, String>>,
) -> Result<Vec<Exchange>, String> {
    match timeout(WAIT, &mut *server).await {
        Ok(result) => result.map_err(|e| e.to_string())?,
        Err(_) => {
            server.abort();
            let _ = server.await;
            Err("HTTP fixture deadline; server aborted and joined".into())
        }
    }
}

impl Fixture {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/predict", listener.local_addr().unwrap());
        let config =
            HttpSmartTurnConfig::new(endpoint, 16_000, SmartTurnParams::default()).unwrap();
        let analyzer = HttpSmartTurnPredictor::new(config)
            .unwrap()
            .into_analyzer()
            .unwrap();
        let user = Arc::new(SmartTurnUserAggregator::new(
            SharedContext::new(Context::default()),
            UserAggregatorConfig {
                turn: UserTurnConfig {
                    enable_interruptions: false,
                    stt_timeout: Duration::from_secs(30),
                    stop_timeout: Duration::from_secs(30),
                    ..Default::default()
                },
                ..Default::default()
            },
            analyzer,
        ));
        let tap = Arc::new(Tap::default());
        let (send_requests, requests) = mpsc::unbounded_channel();
        let (stop, receive_stop) = oneshot::channel();
        let active = Arc::new(AtomicUsize::new(0));
        let mut server = tokio::spawn(serve(listener, send_requests, active.clone(), receive_stop));
        let started = Pipeline::new(vec![user.clone(), tap.clone()])
            .start(PipelineConfig {
                startup_timeout: Duration::from_secs(2),
                shutdown_timeout: Duration::from_secs(2),
                max_background_tasks: 1,
                ..Default::default()
            })
            .await;
        let worker = match started {
            Ok(worker) => worker,
            Err(error) => {
                let _ = stop.send(());
                let joined = join_server(&mut server).await;
                panic!("pipeline startup failed: {error}; server joined: {joined:?}");
            }
        };
        Self {
            user,
            tap,
            worker,
            requests,
            active,
            stop,
            server,
            pending_close: None,
        }
    }

    async fn request(&mut self) -> Result<Request, String> {
        self.requests
            .recv()
            .await
            .ok_or_else(|| "HTTP server stopped before request".into())
    }

    async fn finish(mut self, observed: Result<(), String>, cancel: bool) -> Vec<Exchange> {
        // Keep ownership across observation deadlines. Cancel closes pending
        // HTTP work; the real worker and server are always explicitly joined.
        let cancelled = cancel || observed.is_err();
        let terminal = timeout(WAIT, async {
            if cancelled {
                self.worker.cancel("loopback Smart Turn cleanup").await
            } else {
                self.worker.end().await
            }
        })
        .await;
        let admission_failed = !matches!(&terminal, Ok(Ok(())));
        if admission_failed {
            let _ = timeout(WAIT, self.worker.cancel("terminal admission deadline")).await;
        }
        let joined = timeout(WAIT, async {
            while self.worker.recv().await.is_some() {}
            self.worker.join().await
        })
        .await;
        let report = match joined {
            Ok(result) => result.map_err(|e| e.to_string()),
            Err(_) => {
                let _ = timeout(WAIT, self.worker.cancel("pipeline join deadline")).await;
                let result = self.worker.join().await;
                Err(format!(
                    "pipeline deadline; cancelled and joined: {result:?}"
                ))
            }
        };
        // Observe remote EOF before stopping the listener, or the fixture's
        // own shutdown could hide a client socket that never closed.
        let peer_closed = match self.pending_close.take() {
            Some(closed) => Some(timeout(WAIT, closed).await),
            None => None,
        };
        let _ = self.stop.send(());
        let served = join_server(&mut self.server).await;
        assert_eq!(self.active.load(Ordering::SeqCst), 0);
        assert!(!admission_failed, "terminal admission: {terminal:?}");
        let report = report.expect("pipeline must join");
        let served = served.expect("HTTP server must join");
        observed.expect("pipeline observations must succeed");
        if let Some(result) = peer_closed {
            assert!(result
                .expect("peer EOF deadline")
                .expect("peer EOF observation"));
        }
        assert_eq!(
            report.state,
            if cancelled {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(self.user.buffered_samples().await, 0);
        let frames = self.tap.0.lock().unwrap();
        let contexts: Vec<_> = frames
            .iter()
            .filter_map(|frame| match &frame.frame.data {
                FrameData::LlmContext(context) => Some(context),
                _ => None,
            })
            .collect();
        let metrics: Vec<_> = frames
            .iter()
            .filter(|frame| matches!(frame.frame.data, FrameData::Metrics(_)))
            .collect();
        if cancelled {
            assert!(
                contexts.is_empty(),
                "cancel must never release a late LLM context"
            );
            assert!(
                metrics.is_empty(),
                "cancelled predictions must not emit metrics"
            );
        } else {
            assert_eq!(
                contexts.len(),
                1,
                "including graceful End, exactly one context is released"
            );
            assert_eq!(contexts[0].messages.len(), 1);
            assert_eq!(contexts[0].messages[0].role, ChatRole::User);
            assert_eq!(contexts[0].messages[0].content, "synthetic utterance");
            assert_eq!(
                self.user.context().frame().unwrap().messages,
                contexts[0].messages
            );
            assert_eq!(
                self.user.context().snapshot().unwrap().messages[0].content,
                Some(Content::Text("synthetic utterance".into()))
            );
            assert_eq!(metrics.len(), served.len());
            for (index, frame) in metrics.iter().enumerate() {
                let last = index + 1 == metrics.len();
                let data = &frame.frame.metadata["smart_turn"];
                assert_eq!(data["is_complete"], last);
                let probability = data["probability"].as_f64().unwrap();
                assert!((probability - if last { 0.01 } else { 0.99 }).abs() < 1e-6);
                let elapsed = data["e2e_processing_time_ms"].as_f64().unwrap();
                assert!(elapsed.is_finite() && elapsed >= 0.0);
                match &frame.frame.data {
                    FrameData::Metrics(values) => assert!(values.iter().any(|metric| matches!(metric,
                        Metric::Processing { processor, seconds } if processor == "BaseSmartTurn" && seconds.is_finite() && *seconds >= 0.0))),
                    _ => unreachable!(),
                }
            }
        }
        served
    }
}

async fn send(worker: &PipelineWorker, frame: Frame) -> Result<(), String> {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .map_err(|e| e.to_string())
}

async fn receive_until(
    worker: &mut PipelineWorker,
    predicate: impl Fn(&FrameData) -> bool,
) -> Result<(), String> {
    while let Some(output) = worker.recv().await {
        if let FrameData::Error(error) = &output.frame.data {
            return Err(format!("pipeline error: {}", error.error));
        }
        if predicate(&output.frame.data) {
            return Ok(());
        }
    }
    Err("pipeline ended before expected frame".into())
}

async fn barrier(worker: &mut PipelineWorker) -> Result<(), String> {
    send(worker, Frame::text("http-smart-turn-normal-queue-barrier")).await?;
    receive_until(worker, |data| matches!(data, FrameData::Text(text) if text.text == "http-smart-turn-normal-queue-barrier")).await
}

fn start() -> Frame {
    Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    }))
}
fn stop() -> Frame {
    Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    }))
}
fn audio() -> Frame {
    let bytes: Vec<u8> = PCM
        .repeat(80)
        .into_iter()
        .flat_map(i16::to_le_bytes)
        .collect();
    Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(bytes, 16_000, 1).unwrap(),
    ))
}
fn transcript() -> Frame {
    Frame::new(FrameData::Transcription(TranscriptionFrame {
        text: "synthetic utterance".into(),
        finalized: true,
        ..Default::default()
    }))
}
async fn begin(fixture: &mut Fixture) -> Result<(), String> {
    receive_until(&mut fixture.worker, FrameData::is_start).await?;
    send(&fixture.worker, start()).await?;
    send(&fixture.worker, audio()).await?;
    barrier(&mut fixture.worker).await?;
    send(&fixture.worker, stop()).await
}
fn check_empty(fixture: &Fixture) -> Result<(), String> {
    if fixture
        .user
        .context()
        .snapshot()
        .map_err(|e| e.to_string())?
        .messages
        .is_empty()
    {
        Ok(())
    } else {
        Err("context released before complete prediction and final transcript".into())
    }
}
fn reply(request: Request, prediction: u8, probability: f64) -> Result<(), String> {
    request
        .reply
        .send((prediction, probability))
        .map_err(|_| "HTTP request already closed".into())
}
fn deadline(result: Result<Result<(), String>, tokio::time::error::Elapsed>) -> Result<(), String> {
    result
        .map_err(|_| "pipeline observation deadline".to_owned())
        .and_then(|result| result)
}

fn assert_npy(exchange: &Exchange, copies: usize) {
    assert!(exchange.headers.starts_with("POST /predict HTTP/1.1\r\n"));
    assert!(!exchange
        .headers
        .to_ascii_lowercase()
        .contains("authorization:"));
    assert_eq!(&exchange.body[..8], b"\x93NUMPY\x01\x00");
    let length = u16::from_le_bytes([exchange.body[8], exchange.body[9]]) as usize;
    let header = std::str::from_utf8(&exchange.body[10..10 + length]).unwrap();
    assert!(header.contains("'<f4'"), "{header}");
    assert!(header.contains("'fortran_order': False"), "{header}");
    assert!(
        header.contains(&format!("'shape': ({},)", 320 * copies)),
        "{header}"
    );
    assert!(!header.contains("sample_rate"));
    assert!(header.ends_with('\n'));
    let data = &exchange.body[10 + length..];
    assert_eq!(data.len(), 320 * copies * 4);
    for (value, expected) in data.as_chunks::<4>().0.iter().zip(PCM.repeat(80 * copies)) {
        assert_eq!(f32::from_le_bytes(*value), f32::from(expected) / 32768.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authoritative_complete_releases_final_transcript_and_metrics_through_pipeline() {
    let mut fixture = Fixture::new().await;
    let observed = deadline(
        timeout(WAIT, async {
            begin(&mut fixture).await?;
            let request = fixture.request().await?;
            reply(request, 1, 0.01)?;
            receive_until(&mut fixture.worker, |data| {
                matches!(data, FrameData::Metrics(_))
            })
            .await?;
            check_empty(&fixture)?;
            send(&fixture.worker, transcript()).await?;
            receive_until(&mut fixture.worker, |data| {
                matches!(data, FrameData::LlmContext(_))
            })
            .await
        })
        .await,
    );
    let exchanges = fixture.finish(observed, false).await;
    assert_eq!(exchanges.len(), 1);
    assert!(!exchanges[0].peer_closed_without_reply);
    assert_npy(&exchanges[0], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn high_probability_incomplete_waits_for_a_later_authoritative_complete() {
    let mut fixture = Fixture::new().await;
    let observed = deadline(
        timeout(WAIT, async {
            begin(&mut fixture).await?;
            let request = fixture.request().await?;
            send(&fixture.worker, transcript()).await?;
            barrier(&mut fixture.worker).await?;
            reply(request, 0, 0.99)?;
            receive_until(&mut fixture.worker, |data| {
                matches!(data, FrameData::Metrics(_))
            })
            .await?;
            barrier(&mut fixture.worker).await?;
            check_empty(&fixture)?;
            if fixture.user.buffered_samples().await != 320 {
                return Err("incomplete prediction lost audio".into());
            }
            send(&fixture.worker, stop()).await?;
            let request = fixture.request().await?;
            reply(request, 1, 0.01)?;
            receive_until(&mut fixture.worker, |data| {
                matches!(data, FrameData::LlmContext(_))
            })
            .await
        })
        .await,
    );
    let exchanges = fixture.finish(observed, false).await;
    assert_eq!(exchanges.len(), 2);
    for exchange in &exchanges {
        assert!(!exchange.peer_closed_without_reply);
        assert_npy(exchange, 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_speech_and_cancel_close_pending_http_without_late_context() {
    let mut fixture = Fixture::new().await;
    let observed = deadline(
        timeout(WAIT, async {
            begin(&mut fixture).await?;
            let first = fixture.request().await?;
            send(&fixture.worker, transcript()).await?;
            barrier(&mut fixture.worker).await?;
            // VAD resume invalidates model work. A raw Interruption alone does not
            // cancel the non-interruptible supervisor; observe its actual delivery.
            send(&fixture.worker, start()).await?;
            send(&fixture.worker, Frame::interruption()).await?;
            receive_until(&mut fixture.worker, |data| {
                matches!(data, FrameData::Interruption)
            })
            .await?;
            barrier(&mut fixture.worker).await?;
            if !first
                .closed
                .await
                .map_err(|_| "first socket observation lost")?
            {
                return Err("resumed speech did not close the pending HTTP socket".into());
            }
            check_empty(&fixture)?;
            send(&fixture.worker, audio()).await?;
            barrier(&mut fixture.worker).await?;
            send(&fixture.worker, stop()).await?;
            let second = fixture.request().await?;
            // Cancel admission is not an epoch fence. Hold the request unanswered
            // until finish() processes Cancel and joins the actual owned worker.
            drop(second.reply);
            fixture.pending_close = Some(second.closed);
            Ok(())
        })
        .await,
    );
    let exchanges = fixture.finish(observed, true).await;
    assert_eq!(exchanges.len(), 2);
    assert!(exchanges
        .iter()
        .all(|exchange| exchange.peer_closed_without_reply));
    assert_npy(&exchanges[0], 1);
    assert_npy(&exchanges[1], 2);
}
