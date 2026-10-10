//! Native loopback qualification: real HTTP inference, user turns, gate and effects.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider, SpeculationLimits,
};
use pipecat_turns::*;
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
    sync::{mpsc, oneshot},
};
async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut buf = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
        if let Some(p) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
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
async fn server() -> (String, Requests, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/stream", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let tx = tx.clone();
            tasks.spawn(async move {let request=read_request(&mut socket).await;let(send,recv)=oneshot::channel::<String>();if tx.send((request,send)).await.is_err(){return;}let Ok(body)=recv.await else{return;};let reply=format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());let _=socket.write_all(reply.as_bytes()).await;});
        }
    });
    (url, rx, task)
}
fn answer(text: &str) -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":text},"finish_reason":"stop"}]})
    )
}
fn tool() -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call1","type":"function","function":{"name":"book","arguments":"{}"}}]},"finish_reason":"tool_calls"}]})
    )
}
#[derive(Default)]
struct Recorder(Mutex<Vec<Frame>>);
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "EagerRecorder"
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
fn transcript(kind: fn(TranscriptionFrame) -> FrameData, text: &str) -> Frame {
    Frame::new(kind(TranscriptionFrame {
        text: text.into(),
        user_id: "u".into(),
        timestamp: "0".into(),
        language: None,
        result: None,
        finalized: true,
    }))
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn wait(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition");
}
async fn request(requests: &mut Requests) -> (Value, oneshot::Sender<String>) {
    tokio::time::timeout(Duration::from_secs(5), requests.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn buffered(llm: &LlmProcessor, n: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while llm.speculation_snapshot().await.buffered_frames < n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn start_prediction(worker: &PipelineWorker, recorder: &Recorder, text: &str) {
    send(worker, Frame::new(FrameData::ProposedUserStartedSpeaking)).await;
    wait(|| {
        recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|f| matches!(f.data, FrameData::Interruption))
    })
    .await;
    send(worker, transcript(FrameData::EagerTranscription, text)).await;
}
async fn finalize(worker: &PipelineWorker, user: &LLMUserAggregator, text: &str) {
    let count = user.transcript_frames().unwrap().len();
    send(worker, transcript(FrameData::Transcription, text)).await;
    wait(|| user.transcript_frames().unwrap().len() > count).await;
    send(worker, Frame::new(FrameData::ProposedUserStoppedSpeaking)).await;
}
fn make_llm(url: String, limits: SpeculationLimits) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = url;
    Arc::new(
        LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            CompletionRequest::new("test", Context::default()),
        )
        .with_speculation_limits(limits)
        .unwrap(),
    )
}
fn pair() -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig {
            eager: Some(Default::default()),
            empty_user_turn: None,
            ..Default::default()
        },
        Default::default(),
    )
}
async fn finish(worker: PipelineWorker, server: tokio::task::JoinHandle<()>) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.abort();
    let _ = server.await;
}
#[tokio::test]
async fn inference_starts_early_response_waits_and_final_history_is_exact() {
    let (url, mut requests, server) = server().await;
    let llm = make_llm(url, Default::default());
    let pair = pair();
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        pair.user(),
        llm.clone(),
        recorder.clone(),
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    start_prediction(&worker, &recorder, "its ready").await;
    let (body, reply) = request(&mut requests).await;
    assert_eq!(body["messages"][0]["content"], "its ready");
    reply.send(answer("Ready.")).unwrap();
    buffered(&llm, 3).await;
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    assert!(!recorder
        .0
        .lock()
        .unwrap()
        .iter()
        .any(|f| matches!(f.data, FrameData::LlmText(_))));
    finalize(&worker, &pair.user(), "It's ready!").await;
    wait(|| {
        recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|f| matches!(f.data, FrameData::LlmResponseEnd))
    })
    .await;
    {
        let frames = recorder.0.lock().unwrap();
        let stop = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::UserStoppedSpeaking))
            .unwrap();
        let text = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::LlmText(_)))
            .unwrap();
        assert!(stop < text);
    }
    assert_eq!(
        llm.request_snapshot().unwrap().context.messages[0].content,
        Some(pipecat_services::Content::Text("It's ready!".into()))
    );
    assert!(requests.try_recv().is_err());
    finish(worker, server).await;
}
#[tokio::test]
async fn mismatch_withdraws_buffer_and_runs_exactly_one_fresh_inference() {
    let (url, mut requests, server) = server().await;
    let llm = make_llm(url, Default::default());
    let pair = pair();
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        pair.user(),
        llm.clone(),
        recorder.clone(),
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    start_prediction(&worker, &recorder, "cancel").await;
    request(&mut requests)
        .await
        .1
        .send(answer("Wrong."))
        .unwrap();
    buffered(&llm, 3).await;
    finalize(&worker, &pair.user(), "reschedule").await;
    let (body, reply) = request(&mut requests).await;
    assert_eq!(body["messages"][0]["content"], "reschedule");
    reply.send(answer("Correct.")).unwrap();
    wait(|| {
        recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|f| matches!(&f.data,FrameData::LlmText(t) if t.text=="Correct."))
    })
    .await;
    assert!(!recorder
        .0
        .lock()
        .unwrap()
        .iter()
        .any(|f| matches!(&f.data,FrameData::LlmText(t) if t.text=="Wrong.")));
    finish(worker, server).await;
}
#[tokio::test]
async fn tool_intent_cancels_pending_prediction_before_callback_and_final_runs_tool_once() {
    let (url, mut requests, server) = server().await;
    let llm = make_llm(url, Default::default());
    let pair = pair();
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
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        pair.user(),
        llm.clone(),
        Arc::new(tools),
        recorder.clone(),
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    start_prediction(&worker, &recorder, "book").await;
    request(&mut requests).await.1.send(tool()).unwrap();
    wait(|| {
        recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|f| matches!(f.data, FrameData::EagerEndOfTurnCancel))
    })
    .await;
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    finalize(&worker, &pair.user(), "book").await;
    request(&mut requests).await.1.send(tool()).unwrap();
    wait(|| effects.load(Ordering::SeqCst) == 1).await;
    request(&mut requests)
        .await
        .1
        .send(answer("Done."))
        .unwrap();
    wait(|| {
        recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|f| matches!(&f.data,FrameData::LlmText(t) if t.text=="Done."))
    })
    .await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    finish(worker, server).await;
}
#[tokio::test]
async fn confirmed_mid_inference_tool_executes_once() {
    let (url, mut requests, server) = server().await;
    let llm = make_llm(url, Default::default());
    let pair = pair();
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = effects.clone();
    let mut tools = ToolExecutor::new(4, 16, 4096).unwrap();
    tools
        .register(
            "book",
            RegisteredTool::new(Arc::new(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!({})) })
            })),
        )
        .unwrap();
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        pair.user(),
        llm.clone(),
        Arc::new(tools),
        recorder.clone(),
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    start_prediction(&worker, &recorder, "book").await;
    let (_, reply) = request(&mut requests).await;
    finalize(&worker, &pair.user(), "Book!").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while llm.speculation_snapshot().await.status != Some(SpeculationStatus::Released) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    reply.send(tool()).unwrap();
    wait(|| effects.load(Ordering::SeqCst) == 1).await;
    request(&mut requests)
        .await
        .1
        .send(answer("Done."))
        .unwrap();
    wait(|| {
        recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|f| matches!(&f.data,FrameData::LlmText(t) if t.text=="Done."))
    })
    .await;
    finish(worker, server).await;
}
#[tokio::test]
async fn finite_buffer_overflow_withdraws_and_end_cancels_stalled_speculation() {
    for overflow in [false, true] {
        let (url, mut requests, server) = server().await;
        let llm = make_llm(
            url,
            SpeculationLimits {
                max_frames: 1,
                max_bytes: 4096,
            },
        );
        let pair = pair();
        let recorder = Arc::new(Recorder::default());
        let worker = Pipeline::new(vec![
            pair.user(),
            llm.clone(),
            recorder.clone(),
            pair.assistant(),
        ])
        .start(Default::default())
        .await
        .unwrap();
        start_prediction(&worker, &recorder, "hello").await;
        let (_, reply) = request(&mut requests).await;
        if overflow {
            reply.send(answer("Too much")).unwrap();
            wait(|| {
                recorder
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|f| matches!(f.data, FrameData::EagerEndOfTurnCancel))
            })
            .await;
            assert_eq!(llm.speculation_snapshot().await.buffered_frames, 0);
        } else {
            let _keep_stalled = reply;
            finish(worker, server).await;
            continue;
        }
        finish(worker, server).await;
    }
}
#[tokio::test]
async fn signed_gemini_history_and_marker_bytes_survive_hold_and_release() {
    let (url, mut requests, server) = server().await;
    let mut config = ClientConfig::new(Provider::Gemini, "synthetic");
    config.endpoint = url;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("gemini-3-pro", Context::default()),
    ));
    let pair = pair();
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        pair.user(),
        llm.clone(),
        recorder.clone(),
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    start_prediction(&worker, &recorder, "hello").await;
    let (_, reply) = request(&mut requests).await;
    let parts = json!([{"text":"● exact ","thoughtSignature":"opaque+/=="},{"text":"text"},{"text":"","thoughtSignature":"tail+/=="}]);
    reply
        .send(format!(
            "data: {}\n\n",
            json!({"candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]})
        ))
        .unwrap();
    buffered(&llm, 5).await;
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    finalize(&worker, &pair.user(), "Hello!").await;
    wait(|| pair.context().snapshot().unwrap().messages.len() > 1).await;
    let context = pair.context().snapshot().unwrap();
    let body = pipecat_services::adapters::request_body(
        Provider::Gemini,
        &CompletionRequest::new("gemini-3-pro", context),
    )
    .unwrap();
    assert_eq!(body["contents"][1]["parts"], parts);
    finish(worker, server).await;
}
