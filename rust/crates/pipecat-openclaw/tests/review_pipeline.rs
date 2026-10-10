#[path = "peer.rs"]
#[allow(dead_code)]
mod peer;
use futures_util::StreamExt;
use peer::*;
use pipecat_core::*;
use pipecat_openclaw::*;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{oneshot, Notify};

struct BlockingRecorder {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Notify,
    frames: Mutex<Vec<Frame>>,
}
#[async_trait]
impl FrameProcessor for BlockingRecorder {
    fn name(&self) -> &str {
        "OpenClawReviewBlockingRecorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if matches!(frame.data, FrameData::OpenClawStarted(_)) {
                if let Some(entered) = self.entered.lock().unwrap().take() {
                    let _ = entered.send(());
                }
                self.release.notified().await;
            }
            self.frames.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}
fn config(url: String) -> GatewayConfig {
    GatewayConfig {
        url,
        allow_loopback_cleartext: true,
        limits: GatewayLimits {
            request_timeout: Duration::from_millis(400),
            run_timeout: Duration::from_secs(3),
            close_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        ..Default::default()
    }
}
async fn wait(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(DEADLINE, async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pipeline condition deadline");
}

#[tokio::test]
async fn review_blocked_normal_output_keeps_abort_responsive_and_event_terminal_unique() {
    let (listener, url) = listener().await;
    let (emit, emission) = oneshot::channel();
    let (sent, sent_rx) = oneshot::channel();
    let (aborted, abort_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let start = request(&mut ws).await;
        assert_eq!(start["method"], "chat.send");
        ok(&mut ws, &start, json!({"runId":"pipeline-run"})).await;
        receive(emission).await;
        for text in ["one", "two", "three"] {
            send(&mut ws, chat("pipeline-run", "delta", text)).await;
        }
        sent.send(()).unwrap();
        let abort = request(&mut ws).await;
        assert_eq!(abort["method"], "chat.abort");
        ok(&mut ws, &abort, json!({"aborted":false})).await;
        send(&mut ws, chat("pipeline-run", "final", "onetwothree")).await;
        aborted.send(()).unwrap();
        while let Some(message) = ws.next().await {
            match message {
                Ok(tokio_tungstenite::tungstenite::Message::Text(_)) => {
                    panic!("steer after terminal must be no-op")
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_) => break,
                _ => (),
            }
        }
    });
    let (entered, blocked) = oneshot::channel();
    let recorder = Arc::new(BlockingRecorder {
        entered: Mutex::new(Some(entered)),
        release: Notify::new(),
        frames: Mutex::new(vec![]),
    });
    let processor = Arc::new(OpenClawGatewayProcessor::new(config(url)).unwrap());
    let worker = Pipeline::new(vec![processor, recorder.clone()])
        .start(PipelineConfig {
            queue_capacity: 1,
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .unwrap();
    let sender = worker.sender();
    sender
        .send(
            Frame::new(FrameData::OpenClawSend(OpenClawSendFrame {
                message: "pipeline request".into(),
                session_key: None,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    receive(blocked).await;
    emit.send(()).unwrap();
    receive(sent_rx).await;
    sender
        .send(
            Frame::new(FrameData::OpenClawAbort(OpenClawAbortFrame::default())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    receive(abort_rx).await;
    recorder.release.notify_one();
    wait(|| {
        recorder
            .frames
            .lock()
            .unwrap()
            .iter()
            .any(|f| matches!(f.data, FrameData::OpenClawEnd(_)))
    })
    .await;
    sender
        .send(
            Frame::new(FrameData::OpenClawSteer(OpenClawSteerFrame {
                message: "must not start replacement".into(),
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(DEADLINE, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(
        report.errors.is_empty(),
        "unexpected pressure errors: {:?}",
        report.errors
    );
    {
        let frames = recorder.frames.lock().unwrap();
        let data: Vec<_> = frames
            .iter()
            .filter_map(|f| match &f.data {
                FrameData::OpenClawStarted(_) => Some("started".to_string()),
                FrameData::OpenClawText(t) => Some(t.text.clone()),
                FrameData::OpenClawEnd(e) => {
                    assert_eq!(e.status, RunStatus::Completed);
                    Some("ended".to_string())
                }
                _ => None,
            })
            .collect();
        assert_eq!(data, ["started", "one", "two", "three", "ended"]);
    }
    done(server).await;
}

#[tokio::test]
async fn review_pipeline_early_provisional_alias_uses_adopted_addressable_identity() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let start = request(&mut ws).await;
        let provisional = start["params"]["idempotencyKey"].as_str().unwrap();
        send(
            &mut ws,
            chat(provisional, "delta", "early provisional delta"),
        )
        .await;
        ok(&mut ws, &start, json!({"runId":"server-canonical"})).await;
        send(
            &mut ws,
            chat("server-canonical", "final", "canonical final"),
        )
        .await;
        while let Some(message) = ws.next().await {
            if matches!(
                message,
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_)
            ) {
                break;
            }
        }
    });
    let processor = Arc::new(OpenClawGatewayProcessor::new(config(url)).unwrap());
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let next = tokio::time::timeout(DEADLINE, worker.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(next.frame.data, FrameData::Start(_)));
    worker
        .sender()
        .send(
            Frame::new(FrameData::OpenClawSend(OpenClawSendFrame {
                message: "request".into(),
                session_key: None,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let started = tokio::time::timeout(DEADLINE, worker.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(started.frame.data,FrameData::OpenClawStarted(s) if s.run_id=="server-canonical")
    );
    let text = tokio::time::timeout(DEADLINE, worker.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(text.frame.data,FrameData::OpenClawText(t) if t.run_id=="server-canonical" && t.text=="early provisional delta")
    );
    let end = tokio::time::timeout(DEADLINE, worker.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(end.frame.data,FrameData::OpenClawEnd(e) if e.run_id=="server-canonical" && e.status==RunStatus::Completed)
    );
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(DEADLINE, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty());
    done(server).await;
}
