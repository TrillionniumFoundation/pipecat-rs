mod common;
use common::*;
use pipecat_core::*;
use pipecat_openclaw::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn send_frame(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn started(config: GatewayConfig) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![Arc::new(
        OpenClawGatewayProcessor::new(config).unwrap(),
    )])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Start(_)
    ));
    worker
}
#[tokio::test]
async fn typed_pipeline_send_text_end_then_noop_steer() {
    let (config, peer) = server(|mut peer| async move {
        challenge(&mut peer).await;
        let req = recv(&mut peer).await;
        response(&mut peer, &req, json!({"runId":"pipeline"})).await;
        chat(&mut peer, "pipeline", "delta", "delta").await;
        chat(&mut peer, "pipeline", "final", "whole").await;
        wait_close(&mut peer).await;
    })
    .await;
    let mut worker = started(config).await;
    send_frame(
        &worker,
        FrameData::OpenClawSend(OpenClawSendFrame {
            message: "hello".into(),
            session_key: None,
        }),
    )
    .await;
    assert!(
        matches!(next(&mut worker).await.frame.data,FrameData::OpenClawStarted(s) if s.run_id=="pipeline")
    );
    assert!(
        matches!(next(&mut worker).await.frame.data,FrameData::OpenClawText(s) if s.text=="delta")
    );
    assert!(
        matches!(next(&mut worker).await.frame.data,FrameData::OpenClawEnd(s) if s.text=="whole" && s.status==RunStatus::Completed)
    );
    send_frame(
        &worker,
        FrameData::OpenClawSteer(OpenClawSteerFrame {
            message: "noop".into(),
        }),
    )
    .await;
    worker.end().await.unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    worker.join().await.unwrap();
    peer.await.unwrap();
}
#[tokio::test]
async fn second_send_ends_previous_once_then_starts_new() {
    let (config, peer) = server(|mut peer| async move {
        challenge(&mut peer).await;
        let req = recv(&mut peer).await;
        response(&mut peer, &req, json!({"runId":"one"})).await;
        let abort = recv(&mut peer).await;
        assert_eq!(abort["method"], "chat.abort");
        chat(&mut peer, "one", "aborted", "").await;
        response(&mut peer, &abort, json!({"aborted":true})).await;
        let req = recv(&mut peer).await;
        assert_eq!(req["params"]["message"], "second");
        response(&mut peer, &req, json!({"runId":"two"})).await;
        chat(&mut peer, "one", "final", "late").await;
        chat(&mut peer, "two", "final", "second result").await;
        wait_close(&mut peer).await;
    })
    .await;
    let mut worker = started(config).await;
    send_frame(
        &worker,
        FrameData::OpenClawSend(OpenClawSendFrame {
            message: "first".into(),
            session_key: None,
        }),
    )
    .await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::OpenClawStarted(_)
    ));
    send_frame(
        &worker,
        FrameData::OpenClawSend(OpenClawSendFrame {
            message: "second".into(),
            session_key: None,
        }),
    )
    .await;
    assert!(
        matches!(next(&mut worker).await.frame.data,FrameData::OpenClawEnd(e) if e.run_id=="one" && e.status==RunStatus::Cancelled)
    );
    assert!(
        matches!(next(&mut worker).await.frame.data,FrameData::OpenClawStarted(s) if s.run_id=="two")
    );
    assert!(
        matches!(next(&mut worker).await.frame.data,FrameData::OpenClawEnd(e) if e.run_id=="two" && e.status==RunStatus::Completed)
    );
    worker.end().await.unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    worker.join().await.unwrap();
    peer.await.unwrap();
}
#[tokio::test]
async fn pipeline_steer_and_abort_preserve_one_lifecycle() {
    let (config, peer) = server(|mut peer| async move {
        challenge(&mut peer).await;
        let req = recv(&mut peer).await;
        response(&mut peer, &req, json!({"runId":"one"})).await;
        let steer = recv(&mut peer).await;
        assert_eq!(steer["method"], "sessions.steer");
        chat(&mut peer, "one", "aborted", "").await;
        chat(&mut peer, "replacement", "delta", "new text").await;
        response(
            &mut peer,
            &steer,
            json!({"status":"started","runId":"replacement"}),
        )
        .await;
        let abort = recv(&mut peer).await;
        assert_eq!(abort["method"], "chat.abort");
        assert_eq!(abort["params"]["runId"], "replacement");
        response(&mut peer, &abort, json!({"aborted":true})).await;
        chat(&mut peer, "replacement", "aborted", "").await;
        wait_close(&mut peer).await;
    })
    .await;
    let mut worker = started(config).await;
    send_frame(
        &worker,
        FrameData::OpenClawSend(OpenClawSendFrame {
            message: "first".into(),
            session_key: None,
        }),
    )
    .await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::OpenClawStarted(_)
    ));
    send_frame(
        &worker,
        FrameData::OpenClawSteer(OpenClawSteerFrame {
            message: "second".into(),
        }),
    )
    .await;
    assert!(
        matches!(next(&mut worker).await.frame.data,FrameData::OpenClawText(e) if e.run_id=="replacement")
    );
    send_frame(
        &worker,
        FrameData::OpenClawAbort(OpenClawAbortFrame::default()),
    )
    .await;
    assert!(
        matches!(next(&mut worker).await.frame.data,FrameData::OpenClawEnd(e) if e.run_id=="replacement"&&e.status==RunStatus::Cancelled)
    );
    worker.end().await.unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    worker.join().await.unwrap();
    peer.await.unwrap();
}
#[tokio::test]
async fn rejected_send_reports_upstream_without_started() {
    let (config, peer) = server(|mut peer| async move {
        challenge(&mut peer).await;
        let req = recv(&mut peer).await;
        send(
            &mut peer,
            json!({"type":"res","id":req["id"],"ok":false,"error":{"message":"secret"}}),
        )
        .await;
        wait_close(&mut peer).await;
    })
    .await;
    let mut worker = started(config).await;
    send_frame(
        &worker,
        FrameData::OpenClawSend(OpenClawSendFrame {
            message: "first".into(),
            session_key: None,
        }),
    )
    .await;
    let item = next(&mut worker).await;
    assert_eq!(item.direction, Direction::Upstream);
    assert!(matches!(item.frame.data,FrameData::Error(e) if !e.error.contains("secret")));
    worker.end().await.unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    worker.join().await.unwrap();
    peer.await.unwrap();
}
#[tokio::test]
async fn end_stop_cancel_and_drop_close_owned_socket() {
    for mode in 0..4 {
        let (config, peer) = server(|mut peer| async move {
            challenge(&mut peer).await;
            let req = recv(&mut peer).await;
            response(&mut peer, &req, json!({"runId":"pending"})).await;
            wait_close(&mut peer).await;
        })
        .await;
        let mut worker = started(config).await;
        send_frame(
            &worker,
            FrameData::OpenClawSend(OpenClawSendFrame {
                message: "first".into(),
                session_key: None,
            }),
        )
        .await;
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::OpenClawStarted(_)
        ));
        match mode {
            0 => {
                worker.end().await.unwrap();
                assert!(matches!(
                    next(&mut worker).await.frame.data,
                    FrameData::OpenClawEnd(_)
                ));
                assert!(matches!(
                    next(&mut worker).await.frame.data,
                    FrameData::End(_)
                ));
                worker.join().await.unwrap();
            }
            1 => {
                send_frame(&worker, FrameData::Stop).await;
                assert!(matches!(
                    next(&mut worker).await.frame.data,
                    FrameData::OpenClawEnd(_)
                ));
                assert!(matches!(
                    next(&mut worker).await.frame.data,
                    FrameData::Stop
                ));
                worker.join().await.unwrap();
            }
            2 => {
                worker.cancel("test shutdown").await.unwrap();
                worker.join().await.unwrap();
            }
            _ => drop(worker),
        }
        tokio::time::timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
    }
}
