//! Real localhost Cartesia FIFO regressions; no provider credentials or audio hardware.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

const WAIT: Duration = Duration::from_secs(4);

struct Admission {
    inner: CartesiaTtsProcessor,
    admitted: mpsc::UnboundedSender<u64>,
}
#[async_trait]
impl FrameProcessor for Admission {
    fn name(&self) -> &str {
        self.inner.name()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let id = frame.id;
        let result = self.inner.process(frame, direction, ctx).await;
        let _ = self.admitted.send(id);
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

async fn admitted(rx: &mut mpsc::UnboundedReceiver<u64>, id: u64) {
    tokio::time::timeout(WAIT, async { while rx.recv().await.unwrap() != id {} })
        .await
        .expect("processor did not admit input");
}
async fn read(ws: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        match tokio::time::timeout(WAIT, ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(bytes) => ws.send(Message::Pong(bytes)).await.unwrap(),
            other => panic!("unexpected websocket message {other:?}"),
        }
    }
}
async fn respond(ws: &mut WebSocketStream<TcpStream>, context: &Value, word: &str) {
    for event in [
        json!({"type":"chunk","context_id":context,"data":"AQACAA=="}),
        json!({"type":"timestamps","context_id":context,"word_timestamps":{"words":[word],"start":[0.0],"end":[0.1]}}),
        json!({"type":"done","context_id":context}),
    ] {
        ws.send(Message::Text(event.to_string().into()))
            .await
            .unwrap();
    }
}
fn speak(text: &str) -> Frame {
    Frame::new(FrameData::TtsSpeak(TtsSpeakFrame {
        text: text.into(),
        append_to_context: false,
    }))
}
fn marker(timestamp: u64) -> Frame {
    Frame::new(FrameData::Heartbeat { timestamp })
}
async fn start(listener: &TcpListener) -> (PipelineWorker, mpsc::UnboundedReceiver<u64>) {
    let mut config = CartesiaConfig::new("local-fixture", "voice");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::unbounded_channel();
    let worker = Pipeline::new(vec![Arc::new(Admission {
        inner: CartesiaTtsProcessor::new(CartesiaClient::new(config).unwrap()),
        admitted: tx,
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    (worker, rx)
}
async fn observe_window(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let mut out = vec![];
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    while let Ok(Some(frame)) = tokio::time::timeout_at(deadline, worker.recv()).await {
        out.push(frame);
    }
    out
}
async fn finish(worker: PipelineWorker, mut frames: Vec<DirectedFrame>) -> Vec<DirectedFrame> {
    worker.sender().end().await.unwrap();
    let (rest, report) = tokio::time::timeout(WAIT, worker.collect())
        .await
        .unwrap()
        .unwrap();
    frames.extend(rest);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    frames
}

#[tokio::test]
async fn normal_markers_follow_held_speech_and_precede_the_next_context() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut worker, mut admissions) = start(&listener).await;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let first = read(&mut ws).await;
        assert_eq!(first["transcript"], "Alpha.");
        assert_eq!(read(&mut ws).await["continue"], false);
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        respond(&mut ws, &first["context_id"], "Alpha.").await;
        let _ = tokio::time::timeout(WAIT, ws.next()).await.unwrap();
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let second = read(&mut ws).await;
        assert_eq!(second["transcript"], "Bravo.");
        assert_ne!(second["context_id"], first["context_id"]);
        assert_eq!(read(&mut ws).await["continue"], false);
        respond(&mut ws, &second["context_id"], "Bravo.").await;
        let _ = tokio::time::timeout(WAIT, ws.next()).await.unwrap();
    });
    worker
        .sender()
        .send(speak("Alpha."), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(WAIT, ready_rx).await.unwrap().unwrap();
    let tag = LocalFrameTag::new();
    let mut first = marker(7).with_interruptible(false);
    first.pts = Some(123);
    first.transport_destination = Some("fixture-out".into());
    first.broadcast_sibling_id = Some(456);
    first
        .metadata
        .insert("nested".into(), json!({"message":[1,2,3]}));
    tag.mark(&mut first).unwrap();
    let first_id = first.id;
    let expected = first.clone();
    worker
        .sender()
        .send(first, Direction::Downstream)
        .await
        .unwrap();
    admitted(&mut admissions, first_id).await;
    worker
        .sender()
        .send(speak("Bravo."), Direction::Downstream)
        .await
        .unwrap();
    let second = marker(8);
    let second_id = second.id;
    worker
        .sender()
        .send(second, Direction::Downstream)
        .await
        .unwrap();
    admitted(&mut admissions, second_id).await;
    let early = observe_window(&mut worker).await;
    let leaked = early
        .iter()
        .any(|f| f.frame.id == first_id || f.frame.id == second_id);
    release_tx.send(()).unwrap();
    let frames = finish(worker, early).await;
    server.await.unwrap();
    assert!(
        !leaked,
        "normal marker overtook held synthesis before any audio"
    );
    let first_pos = frames.iter().position(|f| f.frame.id == first_id).unwrap();
    let second_pos = frames.iter().position(|f| f.frame.id == second_id).unwrap();
    let stops: Vec<_> = frames
        .iter()
        .enumerate()
        .filter(|(_, f)| matches!(f.frame.data, FrameData::TtsStopped(_)))
        .map(|(i, _)| i)
        .collect();
    let starts: Vec<_> = frames
        .iter()
        .enumerate()
        .filter(|(_, f)| matches!(f.frame.data, FrameData::TtsStarted(_)))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(stops.len(), 2);
    assert_eq!(starts.len(), 2);
    assert!(stops[0] < first_pos && first_pos < starts[1] && stops[1] < second_pos);
    let actual = &frames[first_pos];
    assert_eq!(actual.direction, Direction::Downstream);
    assert_eq!(actual.frame, expected);
    assert_eq!(
        actual.frame.observation_token(),
        expected.observation_token()
    );
    assert_eq!(frames.iter().filter(|f| f.frame.id == first_id).count(), 1);
    assert_eq!(frames.iter().filter(|f| f.frame.id == second_id).count(), 1);
    assert!(tag.contains(&actual.frame));
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| {
            if let FrameData::TtsAudio(a) = &f.frame.data {
                Some(&a.audio)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(audio, vec![&vec![1, 0, 2, 0], &vec![1, 0, 2, 0]]);
}

#[tokio::test]
async fn marker_does_not_finalize_an_open_context_or_block_later_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut worker, mut admissions) = start(&listener).await;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let first = read(&mut ws).await;
        assert_eq!(first["transcript"], "Alpha ");
        ready_tx.send(()).unwrap();
        let second = read(&mut ws).await;
        assert_eq!(second["transcript"], "Bravo.");
        assert_eq!(second["context_id"], first["context_id"]);
        assert_eq!(read(&mut ws).await["continue"], false);
        release_rx.await.unwrap();
        respond(&mut ws, &first["context_id"], "Alpha Bravo.").await;
        let _ = tokio::time::timeout(WAIT, ws.next()).await.unwrap();
    });
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmResponseStart),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmText(TextFrame::llm("Alpha "))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(WAIT, ready_rx).await.unwrap().unwrap();
    let frame = marker(9);
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    admitted(&mut admissions, id).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmText(TextFrame::llm("Bravo."))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
        .await
        .unwrap();
    let early = observe_window(&mut worker).await;
    let leaked = early.iter().any(|f| f.frame.id == id);
    release_tx.send(()).unwrap();
    let frames = finish(worker, early).await;
    server.await.unwrap();
    assert!(!leaked, "marker overtook an open context");
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsStarted(_)))
            .count(),
        1
    );
    let stopped = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
        .unwrap();
    let marker = frames.iter().position(|f| f.frame.id == id).unwrap();
    assert!(stopped < marker);
}

#[tokio::test]
async fn upstream_system_and_explicit_skip_bypass_a_held_context() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut worker, mut admissions) = start(&listener).await;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut ws).await;
        let _ = read(&mut ws).await;
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        respond(&mut ws, &request["context_id"], "Alpha.").await;
        let _ = tokio::time::timeout(WAIT, ws.next()).await.unwrap();
    });
    worker
        .sender()
        .send(speak("Alpha."), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(WAIT, ready_rx).await.unwrap().unwrap();
    let upstream = marker(10);
    let up_id = upstream.id;
    let urgent = Frame::new(FrameData::OutputTransportMessageUrgent(
        json!({"control":true}),
    ));
    let urgent_id = urgent.id;
    let mut skipped = TextFrame::new("display only");
    skipped.skip_tts = Some(true);
    let skipped = Frame::new(FrameData::Text(skipped));
    let skipped_id = skipped.id;
    for (frame, direction) in [
        (upstream, Direction::Upstream),
        (urgent, Direction::Downstream),
        (skipped, Direction::Downstream),
    ] {
        let id = frame.id;
        worker.sender().send(frame, direction).await.unwrap();
        admitted(&mut admissions, id).await;
    }
    let early = observe_window(&mut worker).await;
    let passed = [up_id, urgent_id, skipped_id].map(|id| early.iter().any(|f| f.frame.id == id));
    release_tx.send(()).unwrap();
    let _ = finish(worker, early).await;
    server.await.unwrap();
    assert_eq!(passed, [true, true, true]);
}
