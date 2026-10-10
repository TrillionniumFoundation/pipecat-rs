use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_deepgram_flux::*;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
#[derive(Default)]
struct Recorder(Mutex<Vec<Frame>>);
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "FluxRecorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.0.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}
impl Recorder {
    fn count(&self, predicate: impl Fn(&FrameData) -> bool) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|f| predicate(&f.data))
            .count()
    }
}
async fn wait(mut test: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while !test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition");
}
async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut buf = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
        if let Some(p) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..end]);
    let len: usize = headers
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + len {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
    }
    serde_json::from_slice(&bytes[end..end + len]).unwrap()
}
type Requests = mpsc::Receiver<(Value, oneshot::Sender<String>)>;
async fn llm_server() -> (String, Requests, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/stream", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(async move {
        let mut children = tokio::task::JoinSet::new();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let tx = tx.clone();
            children.spawn(async move{let body=read_request(&mut socket).await;let(reply,result)=oneshot::channel::<String>();if tx.send((body,reply)).await.is_err(){return;}let Ok(body)=result.await else{return;};let http=format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());let _=socket.write_all(http.as_bytes()).await;});
        }
    });
    (url, rx, task)
}
async fn flux_server(
    eager: bool,
) -> (FluxClient, mpsc::Sender<Value>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = FluxConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/listen", listener.local_addr().unwrap());
    config.eager_end_of_turn = eager;
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_secs(1);
    let (tx, mut rx) = mpsc::channel::<Value>(8);
    let task = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        socket
            .send(Message::Text(
                json!({"type":"Connected"}).to_string().into(),
            ))
            .await
            .unwrap();
        loop {
            tokio::select! {value=rx.recv()=>{let Some(value)=value else{break;};if socket.send(Message::Text(value.to_string().into())).await.is_err(){break;}},value=socket.next()=>match value{Some(Ok(Message::Text(text)))if text.contains("CloseStream")=>{let _=socket.close(None).await;break;},Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,_=>()}}
        }
    });
    (FluxClient::new(config).unwrap(), tx, task)
}
fn turn(event: &str, text: &str) -> Value {
    json!({"type":"TurnInfo","event":event,"turn_index":0,"transcript":text,"words":[{"word":text,"confidence":0.9}],"languages":["es"]})
}
fn answer(text: &str) -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":text},"finish_reason":"stop"}]})
    )
}
async fn request(requests: &mut Requests) -> (Value, oneshot::Sender<String>) {
    tokio::time::timeout(Duration::from_secs(4), requests.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn buffered(llm: &LlmProcessor) {
    tokio::time::timeout(Duration::from_secs(4), async {
        while llm.speculation_snapshot().await.buffered_frames < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn finish(
    worker: PipelineWorker,
    flux: tokio::task::JoinHandle<()>,
    llm: tokio::task::JoinHandle<()>,
) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    tokio::time::timeout(Duration::from_secs(2), flux)
        .await
        .unwrap()
        .unwrap();
    llm.abort();
    let _ = llm.await;
}

// Independent review cases: frozen source is untouched. Real Flux WebSocket,
// user/assistant aggregators, HTTP LLM, and native ToolExecutor are composed.
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use std::sync::atomic::{AtomicUsize, Ordering};
fn tool_answer() -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call1","type":"function","function":{"name":"book","arguments":"{}"}}]},"finish_reason":"tool_calls"}]})
    )
}
fn llm_at(url: String) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = url;
    Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ))
}
#[tokio::test]
async fn review_flux_tool_effect_requires_committed_final_and_executes_once() {
    let (client, flux, flux_task) = flux_server(true).await;
    let (url, mut requests, llm_task) = llm_server().await;
    let stt = Arc::new(FluxSttProcessor::new(client));
    let mut cfg = stt.recommended_user_config();
    cfg.empty_user_turn = None;
    let pair = LLMContextAggregatorPair::new(Context::default(), cfg, Default::default());
    let llm = llm_at(url);
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = effects.clone();
    let mut tools = ToolExecutor::new(4, 16, 4096).unwrap();
    tools
        .register(
            "book",
            RegisteredTool::new(Arc::new(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!({"booked":true})) })
            })),
        )
        .unwrap();
    let record = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        stt,
        pair.user(),
        llm,
        Arc::new(tools),
        record.clone(),
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    flux.send(turn("StartOfTurn", "")).await.unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::UserStartedSpeaking)) == 1).await;
    flux.send(turn("EagerEndOfTurn", "book")).await.unwrap();
    let (body, reply) = request(&mut requests).await;
    assert_eq!(body["messages"][0]["content"], "book");
    reply.send(tool_answer()).unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::EagerEndOfTurnCancel)) >= 1).await;
    assert_eq!(
        effects.load(Ordering::SeqCst),
        0,
        "provisional provider text must never authorize tool effects"
    );
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    assert!(requests.try_recv().is_err());
    flux.send(turn("EndOfTurn", "book")).await.unwrap();
    request(&mut requests).await.1.send(tool_answer()).unwrap();
    wait(|| effects.load(Ordering::SeqCst) == 1).await;
    request(&mut requests)
        .await
        .1
        .send(answer("Booked."))
        .unwrap();
    wait(|| record.count(|f| matches!(f,FrameData::LlmText(t)if t.text=="Booked.")) == 1).await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    finish(worker, flux_task, llm_task).await;
}
#[tokio::test]
async fn review_health_transition_revokes_existing_flux_speculation_immediately() {
    let (client, flux, flux_task) = flux_server(true).await;
    let (url, mut requests, llm_task) = llm_server().await;
    let stt = Arc::new(FluxSttProcessor::new(client));
    let health = stt.health().unwrap();
    let mut cfg = stt.recommended_user_config();
    cfg.empty_user_turn = None;
    let pair = LLMContextAggregatorPair::new(Context::default(), cfg, Default::default());
    let llm = llm_at(url);
    let record = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        stt,
        pair.user(),
        llm.clone(),
        record.clone(),
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    flux.send(turn("StartOfTurn", "")).await.unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::UserStartedSpeaking)) == 1).await;
    flux.send(turn("EagerEndOfTurn", "owned prediction"))
        .await
        .unwrap();
    request(&mut requests)
        .await
        .1
        .send(answer("STALE"))
        .unwrap();
    buffered(&llm).await;
    assert!(llm.speculation_snapshot().await.buffered_frames >= 3);
    health.set_usable(false);
    health.set_usable(true);
    tokio::time::timeout(Duration::from_secs(1), flux_task)
        .await
        .unwrap()
        .unwrap();
    let revoked = tokio::time::timeout(Duration::from_millis(500), async {
        while llm.speculation_snapshot().await.buffered_frames != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let snapshot = llm.speculation_snapshot().await;
    worker.cancel("done").await.unwrap();
    let _ = worker.collect().await.unwrap();
    llm_task.abort();
    let _ = llm_task.await;
    assert!(
        revoked.is_ok(),
        "closed STT retained a live prediction until unrelated expiry: {snapshot:?}"
    );
    assert_eq!(record.count(|f| matches!(f, FrameData::LlmText(_))), 0);
}
#[tokio::test]
async fn review_end_drain_retains_final_but_never_creates_late_speculation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = FluxConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/listen", listener.local_addr().unwrap());
    config.eager_end_of_turn = true;
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_secs(1);
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        for value in [json!({"type":"Connected"}), turn("StartOfTurn", "")] {
            ws.send(Message::Text(value.to_string().into()))
                .await
                .unwrap();
        }
        match ws.next().await {
            Some(Ok(Message::Text(text))) if text.contains("CloseStream") => (),
            other => panic!("unexpected before CloseStream: {other:?}"),
        }
        for value in [
            turn("EagerEndOfTurn", "late prediction"),
            turn("EndOfTurn", "drained final"),
        ] {
            ws.send(Message::Text(value.to_string().into()))
                .await
                .unwrap();
        }
        ws.close(None).await.unwrap();
    });
    let stt = Arc::new(FluxSttProcessor::new(FluxClient::new(config).unwrap()));
    let record = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt, record.clone()])
        .start(Default::default())
        .await
        .unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::ProposedUserStartedSpeaking)) == 1).await;
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        record.count(|f| matches!(f, FrameData::EagerTranscriptionDetailed(_))),
        0,
        "End must prevent a new prediction while draining provider final"
    );
    assert_eq!(
        record.count(
            |f| matches!(f,FrameData::TranscriptionDetailed(t)if t.text.text=="drained final")
        ),
        1
    );
}
#[derive(Default)]
struct ErrorSeen(AtomicUsize);
#[async_trait]
impl FrameProcessor for ErrorSeen {
    fn name(&self) -> &str {
        "ReviewErrorSeen"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Error(_)) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn review_configure_rejection_is_recoverable_and_next_update_reaches_provider() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = FluxConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/listen", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    let (seen, got) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        ws.send(Message::Text(
            json!({"type":"Connected"}).to_string().into(),
        ))
        .await
        .unwrap();
        let first = ws.next().await.unwrap().unwrap();
        assert!(first.to_text().unwrap().contains("Configure"));
        ws.send(Message::Text(json!({"type":"ConfigureFailure","error_code":"INVALID_CONFIGURATION","description":"rejected threshold"}).to_string().into())).await.unwrap();
        match ws.next().await {
            Some(Ok(Message::Text(text))) if text.contains("Configure") => {
                seen.send(serde_json::from_str::<Value>(&text).unwrap())
                    .ok();
                ws.send(Message::Text(
                    json!({"type":"ConfigureSuccess"}).to_string().into(),
                ))
                .await
                .unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    if message.is_close() {
                        break;
                    }
                }
            }
            _ => (),
        }
    });
    let stt = Arc::new(FluxSttProcessor::new(FluxClient::new(config).unwrap()));
    let health = stt.health().unwrap();
    let errors = Arc::new(ErrorSeen::default());
    let worker = Pipeline::new(vec![errors.clone(), stt.clone()])
        .start(Default::default())
        .await
        .unwrap();
    stt.sender()
        .unwrap()
        .merge_settings(json!({"eot_threshold":0.8}).as_object().unwrap())
        .unwrap();
    wait(|| errors.0.load(Ordering::SeqCst) > 0).await;
    let remained_usable = health.is_usable();
    let mut next_reached = false;
    if remained_usable {
        stt.sender()
            .unwrap()
            .merge_settings(json!({"eot_threshold":0.9}).as_object().unwrap())
            .unwrap();
        if let Ok(Ok(body)) = tokio::time::timeout(Duration::from_secs(1), got).await {
            next_reached = body["thresholds"]["eot_threshold"] == 0.9;
        }
    }
    worker.cancel("review done").await.unwrap();
    let _ = worker.collect().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    assert!(
        remained_usable,
        "ConfigureFailure unexpectedly changed health to unusable"
    );
    assert!(
        next_reached,
        "recoverable rejection prevented next settings from reaching provider"
    );
}
#[tokio::test]
async fn review_mute_blocks_real_audio_and_predictions_then_stop_closes_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = FluxConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/listen", listener.local_addr().unwrap());
    config.eager_end_of_turn = true;
    config.limits.close_timeout = Duration::from_millis(100);
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        ws.send(Message::Text(
            json!({"type":"Connected"}).to_string().into(),
        ))
        .await
        .unwrap();
        let input = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            input,
            Message::Binary(vec![7u8; 320].into()),
            "muted PCM leaked to provider"
        );
        for value in [
            turn("StartOfTurn", ""),
            turn("EagerEndOfTurn", "unmuted prediction"),
        ] {
            ws.send(Message::Text(value.to_string().into()))
                .await
                .unwrap();
        }
        let ending = ws.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(ending.to_text().unwrap()).unwrap()["type"],
            "CloseStream"
        );
        ws.close(None).await.unwrap();
    });
    let stt = Arc::new(FluxSttProcessor::new(FluxClient::new(config).unwrap()));
    let record = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt, record.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
        .await
        .unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::SttMute(true))) == 1).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(vec![1u8; 320], 16000, 1).unwrap(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::InputAudio(_))) == 1).await;
    worker
        .sender()
        .send(Frame::new(FrameData::SttMute(false)), Direction::Downstream)
        .await
        .unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::SttMute(false))) == 1).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(vec![7u8; 320], 16000, 1).unwrap(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::EagerTranscriptionDetailed(_))) == 1).await;
    worker
        .sender()
        .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
        .await
        .unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::EagerEndOfTurnCancel)) == 1).await;
    worker
        .sender()
        .send(Frame::new(FrameData::Stop), Direction::Downstream)
        .await
        .unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.state, WorkerState::Stopped);
}

#[tokio::test]
async fn review_non_draining_output_fails_boundedly_without_blocking_lifecycle_lock() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = FluxConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/listen", listener.local_addr().unwrap());
    config.eager_end_of_turn = true;
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.queue_capacity = 1;
    config.limits.write_timeout = Duration::from_millis(100);
    let (release, ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        ws.send(Message::Text(
            json!({"type":"Connected"}).to_string().into(),
        ))
        .await
        .unwrap();
        ready.await.unwrap();
        for value in [
            turn("StartOfTurn", ""),
            turn("EagerEndOfTurn", "consumer is stalled"),
        ] {
            if ws
                .send(Message::Text(value.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
        while let Some(Ok(message)) = ws.next().await {
            if message.is_close() {
                break;
            }
        }
    });
    let stt = Arc::new(FluxSttProcessor::new(FluxClient::new(config).unwrap()));
    // Start and STTMetadata occupy two output slots. Both start proposals fill
    // the remaining two; the eager frame must fail immediately, not await the
    // consumer while holding the provider/lifecycle emission mutex.
    let mut worker = Pipeline::new(vec![stt])
        .start(PipelineConfig {
            output_capacity: 4,
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .await
        .unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_millis(500), async {
        while !worker.has_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("provider emission blocked on non-draining consumer");
    assert_eq!(worker.state(), WorkerState::Failed);
    // Deliberately never call recv/try_recv/collect: teardown must not depend on
    // releasing consumer capacity. All lifecycle submissions remain bounded.
    for frame in [
        Frame::new(FrameData::SttMute(true)),
        Frame::end(),
        Frame::cancel("stalled consumer"),
    ] {
        assert!(tokio::time::timeout(
            Duration::from_millis(200),
            worker.sender().send(frame, Direction::Downstream)
        )
        .await
        .is_ok());
    }
    let report = tokio::time::timeout(Duration::from_secs(1), worker.join())
        .await
        .expect("cleanup deadlocked under output pressure");
    assert!(
        matches!(report, Err(PipelineError::Backpressure(_))),
        "{report:?}"
    );
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
