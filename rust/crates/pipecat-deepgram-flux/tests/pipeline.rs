use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_deepgram_flux::*;
use pipecat_services::{
    ClientConfig, CompletionRequest, Content, Context, LlmClient, LlmProcessor, Provider,
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

#[tokio::test]
async fn actual_flux_wire_drives_early_inference_and_matching_final_releases_once() {
    let (client, flux, flux_task) = flux_server(true).await;
    let (url, mut requests, llm_task) = llm_server().await;
    let stt = Arc::new(
        FluxSttProcessor::new(client)
            .with_user_id("user-7")
            .unwrap(),
    );
    let mut user_config = stt.recommended_user_config();
    user_config.empty_user_turn = None;
    let pair = LLMContextAggregatorPair::new(Context::default(), user_config, Default::default());
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = url;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let recorder = Arc::new(Recorder::default());
    let raw = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        stt,
        raw.clone(),
        pair.user(),
        llm.clone(),
        recorder.clone(),
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    flux.send(turn("StartOfTurn", "")).await.unwrap();
    wait(|| recorder.count(|f| matches!(f, FrameData::UserStartedSpeaking)) == 1).await;
    flux.send(turn("Update", "its")).await.unwrap();
    flux.send(turn("EagerEndOfTurn", "its ready"))
        .await
        .unwrap();
    let (body, reply) = request(&mut requests).await;
    assert_eq!(body["messages"][0]["content"], "its ready");
    reply.send(answer("Ready.")).unwrap();
    buffered(&llm).await;
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    assert_eq!(recorder.count(|f| matches!(f, FrameData::LlmText(_))), 0);
    flux.send(turn("EndOfTurn", "It's ready!")).await.unwrap();
    wait(|| recorder.count(|f| matches!(f, FrameData::LlmResponseEnd)) == 1).await;
    assert_eq!(
        llm.request_snapshot().unwrap().context.messages[0].content,
        Some(Content::Text("It's ready!".into()))
    );
    assert!(requests.try_recv().is_err());
    {
        let frames = raw.0.lock().unwrap();
        let final_frame = frames
            .iter()
            .find_map(|f| {
                if let FrameData::TranscriptionDetailed(t) = &f.data {
                    Some(t)
                } else {
                    None
                }
            })
            .unwrap();
        assert!(final_frame.finalized);
        assert_eq!(final_frame.user_id, "user-7");
        assert_eq!(final_frame.language.as_deref(), Some("es"));
        assert_eq!(final_frame.result.as_ref().unwrap()["event"], "EndOfTurn");
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.data, FrameData::TranscriptionDetailed(_)))
                .count(),
            1
        );
    }
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
    finish(worker, flux_task, llm_task).await;
}
#[tokio::test]
async fn resumed_or_mismatched_final_revokes_old_response_and_runs_fresh_inference() {
    for resumed in [false, true] {
        let (client, flux, flux_task) = flux_server(true).await;
        let (url, mut requests, llm_task) = llm_server().await;
        let stt = Arc::new(FluxSttProcessor::new(client));
        let mut user_config = stt.recommended_user_config();
        user_config.empty_user_turn = None;
        let pair =
            LLMContextAggregatorPair::new(Context::default(), user_config, Default::default());
        let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
        config.endpoint = url;
        let llm = Arc::new(LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            CompletionRequest::new("fixture", Context::default()),
        ));
        let recorder = Arc::new(Recorder::default());
        let worker = Pipeline::new(vec![
            stt,
            pair.user(),
            llm.clone(),
            recorder.clone(),
            pair.assistant(),
        ])
        .start(Default::default())
        .await
        .unwrap();
        flux.send(turn("StartOfTurn", "")).await.unwrap();
        wait(|| recorder.count(|f| matches!(f, FrameData::UserStartedSpeaking)) == 1).await;
        flux.send(turn("EagerEndOfTurn", "old guess"))
            .await
            .unwrap();
        let (_, reply) = request(&mut requests).await;
        reply.send(answer("STALE")).unwrap();
        buffered(&llm).await;
        if resumed {
            flux.send(turn("TurnResumed", "old guess")).await.unwrap();
            wait(|| recorder.count(|f| matches!(f, FrameData::EagerEndOfTurnCancel)) >= 1).await;
        }
        let final_text = if resumed {
            "old guess"
        } else {
            "different final"
        };
        flux.send(turn("EndOfTurn", final_text)).await.unwrap();
        let (body, reply) = request(&mut requests).await;
        assert_eq!(body["messages"][0]["content"], final_text);
        reply.send(answer("FRESH")).unwrap();
        wait(|| recorder.count(|f| matches!(f,FrameData::LlmText(t)if t.text=="FRESH")) == 1).await;
        assert_eq!(
            recorder.count(|f| matches!(f,FrameData::LlmText(t)if t.text=="STALE")),
            0
        );
        finish(worker, flux_task, llm_task).await;
    }
}
#[tokio::test]
async fn disabled_eager_updates_do_not_create_transcriptions_or_llm_requests() {
    let (client, flux, flux_task) = flux_server(false).await;
    let (url, mut requests, llm_task) = llm_server().await;
    let stt = Arc::new(FluxSttProcessor::new(client));
    let mut user_config = stt.recommended_user_config();
    user_config.empty_user_turn = None;
    let pair = LLMContextAggregatorPair::new(Context::default(), user_config, Default::default());
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = url;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        stt,
        recorder.clone(),
        pair.user(),
        llm,
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    flux.send(turn("StartOfTurn", "")).await.unwrap();
    flux.send(turn("Update", "display only")).await.unwrap();
    flux.send(turn("EagerEndOfTurn", "disabled prediction"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(requests.try_recv().is_err());
    assert_eq!(
        recorder.count(|f| matches!(
            f,
            FrameData::EagerTranscriptionDetailed(_)
                | FrameData::InterimTranscriptionDetailed(_)
                | FrameData::TranscriptionDetailed(_)
        )),
        0
    );
    flux.send(turn("EndOfTurn", "committed")).await.unwrap();
    let (_, reply) = request(&mut requests).await;
    reply.send(answer("ok")).unwrap();
    wait(|| !pair.context().snapshot().unwrap().messages.is_empty()).await;
    finish(worker, flux_task, llm_task).await;
}
#[tokio::test]
async fn pipeline_cancel_cleans_up_provider_and_rejects_late_output() {
    let (client, flux, flux_task) = flux_server(true).await;
    let stt = Arc::new(FluxSttProcessor::new(client));
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt.clone(), recorder.clone()])
        .start(Default::default())
        .await
        .unwrap();
    flux.send(turn("StartOfTurn", "")).await.unwrap();
    flux.send(turn("EagerEndOfTurn", "pending")).await.unwrap();
    wait(|| recorder.count(|f| matches!(f, FrameData::EagerTranscriptionDetailed(_))) == 1).await;
    worker.cancel("test").await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(stt.sender().is_err());
    tokio::time::timeout(Duration::from_secs(2), flux_task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        recorder.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))),
        0
    );
}

#[tokio::test]
async fn filtered_final_revokes_prediction_without_committing_missing_confidence() {
    let (client, flux, flux_task) = flux_server(true).await;
    let (url, mut requests, llm_task) = llm_server().await;
    let stt = Arc::new(FluxSttProcessor::new(client));
    let mut user_config = stt.recommended_user_config();
    user_config.empty_user_turn = None;
    let pair = LLMContextAggregatorPair::new(Context::default(), user_config, Default::default());
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = url;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        stt.clone(),
        pair.user(),
        llm.clone(),
        recorder.clone(),
        pair.assistant(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    stt.sender()
        .unwrap()
        .merge_settings(json!({"min_confidence":0.9}).as_object().unwrap())
        .unwrap();
    flux.send(turn("StartOfTurn", "")).await.unwrap();
    wait(|| recorder.count(|f| matches!(f, FrameData::UserStartedSpeaking)) == 1).await;
    flux.send(turn("EagerEndOfTurn", "discard")).await.unwrap();
    let (_, reply) = request(&mut requests).await;
    reply.send(answer("MUST NOT ESCAPE")).unwrap();
    buffered(&llm).await;
    flux.send(json!({"type":"TurnInfo","event":"EndOfTurn","turn_index":0,"transcript":"discard"}))
        .await
        .unwrap();
    wait(|| recorder.count(|f| matches!(f, FrameData::EagerEndOfTurnCancel)) >= 1).await;
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    assert_eq!(recorder.count(|f| matches!(f, FrameData::LlmText(_))), 0);
    assert!(requests.try_recv().is_err());
    finish(worker, flux_task, llm_task).await;
}
#[tokio::test]
async fn health_change_closes_provider_and_does_not_forward_stale_final() {
    let (client, flux, flux_task) = flux_server(true).await;
    let stt = Arc::new(FluxSttProcessor::new(client));
    let health = stt.health().unwrap();
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt, recorder.clone()])
        .start(Default::default())
        .await
        .unwrap();
    flux.send(turn("StartOfTurn", "")).await.unwrap();
    wait(|| recorder.count(|f| matches!(f, FrameData::ProposedUserStartedSpeaking)) == 1).await;
    health.set_usable(false);
    health.set_usable(true);
    let _ = flux.send(turn("EndOfTurn", "STALE")).await;
    tokio::time::timeout(Duration::from_secs(2), flux_task)
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        recorder.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))),
        0
    );
    worker.cancel("done").await.unwrap();
    let _ = worker.collect().await.unwrap();
}
