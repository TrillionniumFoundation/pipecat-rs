//! Deprecated WakeCheck migration profile through real user aggregation and HTTP/SSE.
//! No provider credentials or services are used outside the loopback listener.
//! Only ordinary final transcription variants are gated here. Interim and eager
//! variants bypass this filter and retain their separate downstream policies.
#![cfg(feature = "wake-check")]

use pipecat::{
    core::*,
    processors::{WakeCheckFilter, WakeCheckFilterConfig},
    services::{
        ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
    },
    turns::{LLMUserAggregator, SharedContext, UserAggregatorConfig, UserTurnConfig},
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
const ANSWER: &str = "the admitted user text reached the loopback LLM";

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
        // Serve until the actual PipelineWorker joins, including any erroneous
        // duplicate inference or End flush. A one-request server would hide it.
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
                Ok::<_, String>(body)
            } => result?,
        };
        bodies.push(body);
    }
}

#[derive(Default)]
struct Tap {
    frames: Mutex<Vec<DirectedFrame>>,
}
impl Tap {
    fn snapshot(&self) -> Result<Vec<DirectedFrame>, String> {
        self.frames
            .lock()
            .map(|frames| frames.clone())
            .map_err(|_| "wake conversation tap poisoned".into())
    }
}
#[async_trait]
impl FrameProcessor for Tap {
    fn name(&self) -> &str {
        "wake-conversation-tap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.frames
            .lock()
            .map_err(|_| ProcessorError::fatal("wake conversation tap poisoned"))?
            .push(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
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
    matches: impl Fn(&DirectedFrame) -> bool,
) -> Result<(), String> {
    while let Some(output) = worker.recv().await {
        if let FrameData::Error(error) = &output.frame.data {
            return Err(format!("pipeline emitted an error: {}", error.error));
        }
        if matches(&output) {
            return Ok(());
        }
    }
    Err("pipeline ended before the expected observation".into())
}

async fn barrier(worker: &mut PipelineWorker) -> Result<(), String> {
    let marker = Frame::text("wake-conversation-normal-queue-barrier");
    let id = marker.id;
    send(worker, marker).await?;
    // Text, both ordinary transcription variants, and emitted contexts share
    // the normal queue. No wall-clock sleep stands in for actual consumption.
    receive_until(worker, |output| {
        output.direction == Direction::Downstream && output.frame.id == id
    })
    .await
}

async fn join_server(
    server: &mut JoinHandle<Result<Vec<Value>, String>>,
) -> Result<Vec<Value>, String> {
    match timeout(WAIT, &mut *server).await {
        Ok(result) => result.map_err(|e| e.to_string())?,
        Err(_) => {
            server.abort();
            let joined = server.await;
            Err(format!(
                "loopback server deadline; aborted and joined: {joined:?}"
            ))
        }
    }
}

async fn finish_worker(
    worker: &mut PipelineWorker,
    graceful: bool,
    errors: &mut Vec<String>,
) -> Option<WorkerReport> {
    let admitted = timeout(WAIT, async {
        if graceful {
            worker.end().await
        } else {
            worker.cancel("wake conversation observation failed").await
        }
    })
    .await;
    let terminal_failed = match admitted {
        Ok(Ok(())) => false,
        other => {
            errors.push(format!("pipeline terminal admission failed: {other:?}"));
            true
        }
    };
    if terminal_failed {
        if let other @ (Err(_) | Ok(Err(_))) = timeout(
            WAIT,
            worker.cancel("wake conversation terminal admission failed"),
        )
        .await
        {
            errors.push(format!("pipeline fallback cancellation failed: {other:?}"));
        }
    }
    let joined = match timeout(WAIT, async {
        while worker.recv().await.is_some() {}
        worker.join().await
    })
    .await
    {
        Ok(result) => result,
        Err(_) => {
            errors.push("pipeline collection deadline".into());
            if let other @ (Err(_) | Ok(Err(_))) =
                timeout(WAIT, worker.cancel("wake conversation collection deadline")).await
            {
                errors.push(format!(
                    "pipeline collection cancellation failed: {other:?}"
                ));
            }
            // Keep owning and joining the actual worker supervisor. Its
            // configured shutdown deadline bounds owned-task cleanup; dropping
            // a separate timed-out collector would instead detach that work.
            worker.join().await
        }
    };
    match joined {
        Ok(report) => Some(report),
        Err(error) => {
            errors.push(format!("pipeline join failed: {error}"));
            None
        }
    }
}

fn is_context(frame: &DirectedFrame) -> bool {
    matches!(
        frame.frame.data,
        FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
    )
}

#[derive(Debug)]
struct Observation {
    closed_requests: usize,
    closed_context: Context,
    closed_emissions: usize,
    after_response_requests: usize,
}

async fn assert_conversation(hidden: Vec<Frame>, current: Frame, expected_text: &str) {
    let filter =
        Arc::new(WakeCheckFilter::new(WakeCheckFilterConfig::default(), ["hey bot"]).unwrap());
    let shared = SharedContext::default();
    let user = Arc::new(LLMUserAggregator::new(
        shared.clone(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                // Ordinary no-VAD final transcripts commit immediately with
                // the standard controller's silence floor explicitly zero.
                // Hidden final transcripts therefore cannot merely be waiting
                // for a timer or a manually constructed context to be emitted.
                speech_timeout: Duration::ZERO,
                enable_interruptions: false,
                ..Default::default()
            },
            empty_user_turn: None,
            ..Default::default()
        },
    ));
    let admitted_tap = Arc::new(Tap::default());
    let context_tap = Arc::new(Tap::default());
    let response_tap = Arc::new(Tap::default());
    let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic-loopback-only");
    config.endpoint = format!("http://{}/completion", listener.local_addr().unwrap());
    config.request_timeout = Duration::from_secs(2);
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("synthetic-model", Context::default()),
    ));
    let counts = Arc::new(ServerCounts::default());
    let (stop_tx, stop_rx) = oneshot::channel();
    let mut server = tokio::spawn(serve(listener.clone(), counts.clone(), stop_rx));
    let started = Pipeline::new(vec![
        filter,
        admitted_tap.clone(),
        user,
        context_tap.clone(),
        llm,
        response_tap.clone(),
    ])
    .start(PipelineConfig {
        startup_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(1),
        ..Default::default()
    })
    .await;
    let mut worker = match started {
        Ok(worker) => worker,
        Err(error) => {
            let _ = stop_tx.send(());
            let joined = join_server(&mut server).await;
            drop(listener);
            panic!("pipeline startup failed: {error}; server joined: {joined:?}");
        }
    };

    // Observation futures borrow these handles. A deadline never drops the
    // worker or the server's JoinHandle, including on an erroneous HTTP request.
    let observed: Result<Observation, String> = timeout(WAIT, async {
        for frame in &hidden {
            send(&worker, frame.clone()).await?;
        }
        barrier(&mut worker).await?;
        // This explicit boundary must not flush hidden transcripts either.
        // First drain the normal queue so a priority stop cannot overtake them.
        send(&worker, Frame::new(FrameData::UserStoppedSpeaking)).await?;
        barrier(&mut worker).await?;
        let closed_requests = counts.requests.load(Ordering::SeqCst);
        let closed_context = shared.snapshot().map_err(|e| e.to_string())?;
        let closed_emissions = context_tap
            .snapshot()?
            .iter()
            .filter(|f| is_context(f))
            .count();

        send(&worker, current.clone()).await?;
        receive_until(&mut worker, |output| {
            output.direction == Direction::Downstream
                && matches!(output.frame.data, FrameData::LlmResponseEnd)
        })
        .await?;
        barrier(&mut worker).await?;
        Ok(Observation {
            closed_requests,
            closed_context,
            closed_emissions,
            after_response_requests: counts.requests.load(Ordering::SeqCst),
        })
    })
    .await
    .map_err(|_| "wake conversation observation deadline".to_owned())
    .and_then(|result| result);

    // Collect every cleanup outcome before asserting anything about the live
    // scenario. Keep accepting requests through End and the worker's join.
    let mut errors = vec![];
    let report = finish_worker(&mut worker, observed.is_ok(), &mut errors).await;
    let _ = stop_tx.send(());
    let bodies = match join_server(&mut server).await {
        Ok(bodies) => Some(bodies),
        Err(error) => {
            errors.push(error);
            None
        }
    };
    drop(listener);
    if let Err(error) = &observed {
        errors.push(error.clone());
    }
    let active_connections = counts.active_connections.load(Ordering::SeqCst);
    if active_connections != 0 {
        errors.push(format!(
            "{active_connections} loopback connections remain active"
        ));
    }
    assert!(errors.is_empty(), "all cleanup has been joined: {errors:?}");

    let report = report.unwrap();
    let bodies = bodies.unwrap();
    let observed = observed.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.total_errors, 0);
    assert_eq!(observed.closed_requests, 0);
    assert!(observed.closed_context.messages.is_empty());
    assert_eq!(observed.closed_emissions, 0);
    assert_eq!(observed.after_response_requests, 1);
    assert_eq!(counts.requests.load(Ordering::SeqCst), 1);
    assert_eq!(bodies.len(), 1);
    assert_eq!(
        bodies[0]["messages"],
        json!([{"role":"user", "content":expected_text}])
    );
    assert_eq!(
        shared.snapshot().unwrap().messages,
        [Message::text(Role::User, expected_text)]
    );

    let admitted = admitted_tap.snapshot().unwrap();
    assert!(hidden
        .iter()
        .all(|hidden| !admitted.iter().any(|output| output.frame.id == hidden.id)));
    let released: Vec<_> = admitted
        .iter()
        .filter(|output| {
            matches!(
                output.frame.data,
                FrameData::Transcription(_) | FrameData::TranscriptionDetailed(_)
            )
        })
        .collect();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].direction, Direction::Downstream);
    assert_eq!(
        released[0].frame.observation_token(),
        current.observation_token()
    );
    let mut expected = current;
    match &mut expected.data {
        FrameData::Transcription(text) => text.text = expected_text.into(),
        FrameData::TranscriptionDetailed(text) => text.text.text = expected_text.into(),
        _ => panic!("fixture current frame must be an ordinary transcription"),
    }
    // Includes current frame identity, user/timestamp/language/result, envelope
    // metadata, and detailed TextFrame flags; only the text is rewritten.
    assert_eq!(released[0].frame, expected);
    let contexts = context_tap.snapshot().unwrap();
    assert_eq!(contexts.iter().filter(|f| is_context(f)).count(), 1);
    assert!(contexts
        .iter()
        .filter(|f| is_context(f))
        .all(|frame| frame.direction == Direction::Downstream));
    let response = response_tap.snapshot().unwrap();
    assert!(!response.iter().any(is_context));
    let response_parts: Vec<_> = response
        .iter()
        .filter_map(|output| match &output.frame.data {
            FrameData::LlmResponseStart => Some((output.direction, "start", "")),
            FrameData::LlmText(text) => Some((output.direction, "text", text.text.as_str())),
            FrameData::LlmResponseEnd => Some((output.direction, "end", "")),
            _ => None,
        })
        .collect();
    assert_eq!(
        response_parts,
        [
            (Direction::Downstream, "start", ""),
            (Direction::Downstream, "text", ANSWER),
            (Direction::Downstream, "end", ""),
        ]
    );
}

fn transcription(text: &str, user_id: &str, segment: &str, detailed: bool) -> Frame {
    let timestamp = format!("2026-10-14T15:00:00Z/{segment}");
    let language = Some("en".into());
    let result = Some(json!({"synthetic_segment":segment, "confidence":0.99}));
    let mut frame = Frame::new(if detailed {
        FrameData::TranscriptionDetailed(DetailedTranscriptionFrame {
            text: TextFrame {
                skip_tts: Some(true),
                includes_inter_frame_spaces: true,
                ..TextFrame::new(text)
            },
            user_id: user_id.into(),
            timestamp,
            language,
            result,
            finalized: true,
        })
    } else {
        FrameData::Transcription(TranscriptionFrame {
            text: text.into(),
            user_id: user_id.into(),
            timestamp,
            language,
            result,
            finalized: true,
        })
    });
    frame.pts = Some(42_000);
    frame.transport_source = Some("synthetic-stt".into());
    frame
        .metadata
        .insert("transcript_id".into(), segment.into());
    frame
        .metadata
        .insert("fixture_metadata".into(), json!({"segment":segment}));
    frame
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_transcripts_never_reach_context_or_http_then_wake_keeps_only_matched_suffix() {
    assert_conversation(
        vec![
            transcription("hidden first speaker. ", "alice", "alice-hidden", false),
            transcription("hidden second speaker. ", "bob", "bob-hidden", false),
        ],
        transcription(
            "前置闲聊: HEY BOT 回答这个问题。",
            "alice",
            "current-compact",
            false,
        ),
        "HEY BOT 回答这个问题。",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_wake_has_no_inserted_separator_and_current_detailed_frame_drives_real_http() {
    assert_conversation(
        vec![
            transcription("前置闲聊: hey", "alice", "alice-partial", true),
            transcription("private unrelated words", "bob", "bob-hidden", true),
        ],
        transcription("bot 回答这个问题。", "alice", "current-detailed", true),
        "heybot 回答这个问题。",
    )
    .await;
}
