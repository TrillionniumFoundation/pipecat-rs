//! Latest-context gating through a real Pipeline and loopback HTTP/SSE LLM.
//! The synthetic credential never leaves the loopback listener.
use pipecat::{
    core::*,
    processors::{
        GatedLlmContextAggregator, GatedLlmContextAggregatorConfig, GatedLlmContextNotifier,
        GatedLlmContextSnapshot,
    },
    services::{ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider},
};
use serde_json::{json, Value};
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
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);
const OLD_CONTEXT: &str = "superseded-context-A";
const LATEST_CONTEXT: &str = "latest-context-B";
const UNRELEASED_CONTEXT: &str = "unnotified-context-C";
const ANSWER: &str = "only the latest context reached the LLM";

#[derive(Default)]
struct ServerCounts {
    requests: AtomicUsize,
    active_connections: AtomicUsize,
}

struct ActiveConnection(Arc<ServerCounts>);
impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn read_request(socket: &mut TcpStream) -> Result<Value, String> {
    const MAX_REQUEST_BYTES: usize = 64 * 1024;
    let mut bytes = vec![];
    let mut chunk = [0; 4096];
    let header_end = loop {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("HTTP connection closed before request headers".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err("fixture HTTP request exceeded its byte limit".into());
        }
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let length = String::from_utf8_lossy(&bytes[..header_end])
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>())
        })
        .ok_or("missing request Content-Length")?
        .map_err(|e| e.to_string())?;
    if length > MAX_REQUEST_BYTES - header_end {
        return Err("fixture HTTP body exceeded its byte limit".into());
    }
    while bytes.len() < header_end + length {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("HTTP connection closed before request body".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err("fixture HTTP request exceeded its byte limit".into());
        }
    }
    serde_json::from_slice(&bytes[header_end..header_end + length]).map_err(|e| e.to_string())
}

async fn serve(
    listener: Arc<TcpListener>,
    counts: Arc<ServerCounts>,
    mut stop: oneshot::Receiver<()>,
) -> Result<Vec<Value>, String> {
    let mut bodies = vec![];
    loop {
        let (mut socket, _) = tokio::select! {
            biased;
            _ = &mut stop => return Ok(bodies),
            accepted = listener.accept() => accepted.map_err(|e| e.to_string())?,
        };
        counts.active_connections.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveConnection(counts.clone());
        // Keep accepting until the pipeline has joined, so a duplicate or an
        // erroneous End flush is observed rather than hidden by a one-shot server.
        let body = tokio::select! {
            biased;
            _ = &mut stop => return Ok(bodies),
            result = async {
                let body = read_request(&mut socket).await?;
                counts.requests.fetch_add(1, Ordering::SeqCst);
                let event = json!({"choices":[{"delta":{"content":ANSWER},"finish_reason":"stop"}]});
                let data = format!("data: {event}\n\ndata: [DONE]\n\n");
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{data}",
                    data.len()
                );
                socket.write_all(response.as_bytes()).await.map_err(|e| e.to_string())?;
                socket.shutdown().await.map_err(|e| e.to_string())?;
                Ok::<Value, String>(body)
            } => result?,
        };
        bodies.push(body);
    }
}

#[derive(Default)]
struct Tap {
    frames: Mutex<Vec<DirectedFrame>>,
}

#[async_trait]
impl FrameProcessor for Tap {
    fn name(&self) -> &str {
        "gated-context-http-tap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.frames
            .lock()
            .map_err(|_| ProcessorError::fatal("HTTP fixture tap poisoned"))?
            .push(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}

fn context(text: &str) -> Frame {
    Frame::new(FrameData::LlmContext(LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, text)],
        ..Default::default()
    }))
}

async fn receive_until(
    worker: &mut PipelineWorker,
    matches: impl Fn(&Frame) -> bool,
) -> Result<(), String> {
    while let Some(output) = worker.recv().await {
        if let FrameData::Error(error) = &output.frame.data {
            return Err(format!("pipeline emitted an error: {}", error.error));
        }
        if matches(&output.frame) {
            return Ok(());
        }
    }
    Err("pipeline ended before the expected observation".into())
}

async fn barrier(worker: &mut PipelineWorker) -> Result<(), String> {
    let marker = Frame::text("same-normal-queue-barrier");
    let id = marker.id;
    worker
        .sender()
        .send(marker, Direction::Downstream)
        .await
        .map_err(|e| e.to_string())?;
    // Text and LlmContext share the normal queue. Reaching the output through
    // the Tap proves that all preceding input contexts were actually processed.
    receive_until(worker, |frame| frame.id == id).await
}

async fn join_server(
    server: &mut JoinHandle<Result<Vec<Value>, String>>,
) -> Result<Vec<Value>, String> {
    match timeout(WAIT, &mut *server).await {
        Ok(result) => result.map_err(|e| e.to_string())?,
        Err(_) => {
            server.abort();
            let _ = server.await;
            Err("loopback server deadline; aborted and joined".into())
        }
    }
}

async fn finish_worker(
    worker: &mut PipelineWorker,
    graceful: bool,
) -> Result<WorkerReport, String> {
    let terminal = timeout(WAIT, async {
        if graceful {
            worker.end().await
        } else {
            worker.cancel("HTTP gate observation failed").await
        }
    })
    .await
    .map_err(|_| "pipeline terminal admission deadline".to_owned())
    .and_then(|result| result.map_err(|e| e.to_string()));
    if terminal.is_err() {
        let _ = timeout(WAIT, worker.cancel("HTTP gate terminal admission failed")).await;
    }
    let joined = match timeout(WAIT, async {
        while worker.recv().await.is_some() {}
        worker.join().await
    })
    .await
    {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(_) => {
            let _ = timeout(WAIT, worker.cancel("HTTP gate collection deadline")).await;
            // Keep owning the actual PipelineWorker while joining its
            // supervisor. Its configured shutdown deadline bounds cleanup;
            // never drop a timed-out collection task and detach the worker.
            let result = worker.join().await;
            Err(format!(
                "pipeline collection deadline; cancelled and joined: {result:?}"
            ))
        }
    };
    terminal.and(joined)
}

#[derive(Debug)]
struct Observation {
    pending: GatedLlmContextSnapshot,
    released: GatedLlmContextSnapshot,
    empty_notification: GatedLlmContextSnapshot,
    before_end: GatedLlmContextSnapshot,
    requests: [usize; 4],
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_latest_notified_context_reaches_real_http_llm_and_end_discards_pending_context() {
    let gate = Arc::new(
        GatedLlmContextAggregator::new(GatedLlmContextAggregatorConfig {
            start_open: false,
            max_context_bytes: 4 * 1024 * 1024,
        })
        .unwrap(),
    );
    // These imports and calls also exercise the public pipecat facade.
    assert_eq!(gate.name(), "GatedLlmContextAggregator");
    assert!(gate.current_notifier().is_err());
    let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic-loopback-only");
    config.endpoint = format!("http://{}/completion", listener.local_addr().unwrap());
    config.request_timeout = Duration::from_secs(2);
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("synthetic-model", Context::default()),
    ));
    let tap = Arc::new(Tap::default());
    let counts = Arc::new(ServerCounts::default());
    let (stop_tx, stop_rx) = oneshot::channel();
    let mut server = tokio::spawn(serve(listener.clone(), counts.clone(), stop_rx));
    let started = Pipeline::new(vec![gate.clone(), llm, tap.clone()])
        .start(PipelineConfig {
            startup_timeout: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .await;
    let mut worker = match started {
        Ok(worker) => worker,
        Err(error) => {
            let _ = stop_tx.send(());
            let server_result = join_server(&mut server).await;
            drop(listener);
            panic!("pipeline startup failed: {error}; server joined: {server_result:?}");
        }
    };
    let notifier: Result<GatedLlmContextNotifier, _> = gate.current_notifier();

    // Only observations are deadline-bound. Listener, server handle, and the
    // PipelineWorker remain owned here on every timeout or returned error.
    let observed: Result<Observation, String> = timeout(WAIT, async {
        let notifier = notifier.as_ref().map_err(|e| e.to_string())?;
        for text in [OLD_CONTEXT, LATEST_CONTEXT] {
            worker
                .sender()
                .send(context(text), Direction::Downstream)
                .await
                .map_err(|e| e.to_string())?;
        }
        barrier(&mut worker).await?;
        let pending = notifier.snapshot();
        let before_notify = counts.requests.load(Ordering::SeqCst);

        notifier.notify().map_err(|e| e.to_string())?;
        receive_until(&mut worker, |frame| {
            matches!(frame.data, FrameData::LlmResponseEnd)
        })
        .await?;
        let released = notifier.snapshot();
        let after_release = counts.requests.load(Ordering::SeqCst);

        notifier.notify().map_err(|e| e.to_string())?;
        // Await actual consumption, not a sleep that could leave an empty
        // signal pending and accidentally release the later C context.
        let empty_notification = loop {
            let snapshot = notifier.snapshot();
            if snapshot.notifications_consumed > released.notifications_consumed {
                break snapshot;
            }
            tokio::task::yield_now().await;
        };
        barrier(&mut worker).await?;
        let after_empty = counts.requests.load(Ordering::SeqCst);

        worker
            .sender()
            .send(context(UNRELEASED_CONTEXT), Direction::Downstream)
            .await
            .map_err(|e| e.to_string())?;
        barrier(&mut worker).await?;
        Ok(Observation {
            pending,
            released,
            empty_notification,
            before_end: notifier.snapshot(),
            requests: [
                before_notify,
                after_release,
                after_empty,
                counts.requests.load(Ordering::SeqCst),
            ],
        })
    })
    .await
    .map_err(|_| "HTTP gate observation deadline".to_owned())
    .and_then(|result| result);

    // Cleanup precedes every assertion about the live scenario. Keep serving
    // until End and all owned completion work have joined, to expose duplicates.
    let finished = finish_worker(&mut worker, observed.is_ok()).await;
    let _ = stop_tx.send(());
    let served = join_server(&mut server).await;
    drop(listener);

    assert_eq!(counts.active_connections.load(Ordering::SeqCst), 0);
    let report = finished.expect("HTTP gate pipeline must close and join");
    let bodies = served.expect("loopback server must close and join");
    let observed = observed.expect("real pipeline gate observations must complete");
    let notifier = notifier.expect("setup must expose the facade notifier");
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(observed.requests, [0, 1, 1, 1]);
    assert!(observed.pending.context_pending);
    assert!(!observed.pending.closed);
    assert!(observed.pending.supervisor_running);
    assert_eq!(observed.pending.notifications_consumed, 0);
    assert!(!observed.released.context_pending);
    assert_eq!(observed.released.notifications_consumed, 1);
    assert!(!observed.empty_notification.context_pending);
    assert!(!observed.empty_notification.notification_pending);
    assert_eq!(observed.empty_notification.notifications_consumed, 2);
    assert!(observed.before_end.context_pending);
    assert_eq!(observed.before_end.notifications_consumed, 2);
    assert_eq!(
        notifier.snapshot(),
        GatedLlmContextSnapshot {
            closed: true,
            context_pending: false,
            notification_pending: false,
            notifications_consumed: 2,
            supervisor_running: false,
        }
    );
    assert!(
        notifier.notify().is_err(),
        "the old notifier must be retired"
    );
    assert!(gate.current_notifier().is_err());
    assert_eq!(counts.requests.load(Ordering::SeqCst), 1);
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0]["messages"],
        json!([{"role":"user", "content":LATEST_CONTEXT}])
    );
    let body = bodies[0].to_string();
    assert!(!body.contains(OLD_CONTEXT));
    assert!(!body.contains(UNRELEASED_CONTEXT));

    let frames = tap.frames.lock().unwrap();
    let texts: Vec<_> = frames
        .iter()
        .filter_map(|output| match &output.frame.data {
            FrameData::LlmText(text) => Some((output.direction, text.text.as_str())),
            _ => None,
        })
        .collect();
    assert_eq!(texts, [(Direction::Downstream, ANSWER)]);
    for matches in [
        (|data: &FrameData| matches!(data, FrameData::LlmResponseStart)) as fn(&FrameData) -> bool,
        |data: &FrameData| matches!(data, FrameData::LlmResponseEnd),
    ] {
        assert_eq!(
            frames
                .iter()
                .filter(|output| matches(&output.frame.data))
                .count(),
            1
        );
    }
    assert!(!frames
        .iter()
        .any(|output| matches!(output.frame.data, FrameData::LlmContext(_))));
}
