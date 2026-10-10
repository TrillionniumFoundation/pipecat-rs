use pipecat_core::{Direction, Frame, FrameData, Pipeline, PipelineWorker};
use pipecat_workers::*;
use std::{sync::Arc, time::Duration};
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("operation stalled")
}
async fn text(pipeline: &mut PipelineWorker) -> pipecat_core::DirectedFrame {
    loop {
        let frame = bounded(pipeline.recv()).await.unwrap();
        if matches!(frame.frame.data, FrameData::Text(_)) {
            return frame;
        }
    }
}
async fn finish(mut pipeline: PipelineWorker) {
    pipeline.sender().end().await.unwrap();
    while bounded(pipeline.recv()).await.is_some() {}
    bounded(pipeline.join()).await.unwrap();
}
#[tokio::test]
async fn real_bidirectional_bridges_consume_local_frames_inject_remote_and_do_not_echo() {
    let bus = LocalBus::new(32).unwrap();
    let audit = bus.subscribe("audit").unwrap();
    let bridge = |worker: &str, peer: &str| {
        Arc::new(BusBridgeProcessor::new(
            format!("bridge-{worker}"),
            bus.clone(),
            BusBridgeOptions {
                target_worker: Some(peer.into()),
                bridge: Some("session".into()),
                ..BusBridgeOptions::new(worker)
            },
        ))
    };
    let mut a = Pipeline::new(vec![bridge("a", "b")])
        .start(Default::default())
        .await
        .unwrap();
    let mut b = Pipeline::new(vec![bridge("b", "a")])
        .start(Default::default())
        .await
        .unwrap();
    a.sender()
        .send(Frame::text("from-a"), Direction::Downstream)
        .await
        .unwrap();
    let received = text(&mut b).await;
    assert_eq!(received.direction, Direction::Downstream);
    assert!(matches!(received.frame.data, FrameData::Text(ref value) if value.text == "from-a"));
    b.sender()
        .send(Frame::text("from-b"), Direction::Upstream)
        .await
        .unwrap();
    let received = text(&mut a).await;
    assert_eq!(received.direction, Direction::Upstream);
    assert!(matches!(received.frame.data, FrameData::Text(ref value) if value.text == "from-b"));
    finish(a).await;
    finish(b).await;
    let mut frames = 0;
    while let Some(message) = audit.try_recv().unwrap() {
        if let MessageData::Frame { frame, .. } = message.data {
            assert!(matches!(frame.data, FrameData::Text(_)));
            frames += 1;
        }
    }
    assert_eq!(frames, 2);
}
#[tokio::test]
async fn inbound_frames_crossing_a_second_tee_are_not_rebroadcast() {
    let bus = LocalBus::new(16).unwrap();
    let audit = bus.subscribe("audit").unwrap();
    let first = Arc::new(BusBridgeProcessor::new(
        "inbound",
        bus.clone(),
        BusBridgeOptions::new("local"),
    ));
    let tee = Arc::new(BusBridgeProcessor::new(
        "outbound",
        bus.clone(),
        BusBridgeOptions {
            mode: BridgeMode::Tee,
            ..BusBridgeOptions::new("local")
        },
    ));
    let mut pipeline = Pipeline::new(vec![first, tee])
        .start(Default::default())
        .await
        .unwrap();
    bus.publish(BusMessage {
        source: "remote".into(),
        target: Some("local".into()),
        data: MessageData::Frame {
            frame: Box::new(Frame::text("incoming")),
            direction: Direction::Downstream,
            bridge: None,
        },
    })
    .unwrap();
    // The tee is output-only; the consuming bridge injects once.
    let _ = text(&mut pipeline).await;
    finish(pipeline).await;
    let mut frames = 0;
    while let Some(message) = audit.try_recv().unwrap() {
        if matches!(message.data, MessageData::Frame { .. }) {
            frames += 1;
        }
    }
    assert_eq!(frames, 1);
}
#[tokio::test]
async fn bridge_filters_sources_targets_tags_and_passes_excluded_frames_locally() {
    let bus = LocalBus::new(32).unwrap();
    let audit = bus.subscribe("audit").unwrap();
    let bridge = Arc::new(
        BusBridgeProcessor::new(
            "bridge",
            bus.clone(),
            BusBridgeOptions {
                target_worker: Some("peer".into()),
                bridge: Some("selected".into()),
                ..BusBridgeOptions::new("local")
            },
        )
        .exclude(|frame| matches!(&frame.data, FrameData::Text(text) if text.text == "excluded")),
    );
    let mut pipeline = Pipeline::new(vec![bridge])
        .start(Default::default())
        .await
        .unwrap();
    for (source, target, bridge, content) in [
        ("local", "local", "selected", "own"),
        ("other", "local", "selected", "wrong-source"),
        ("peer", "other", "selected", "wrong-target"),
        ("peer", "local", "other", "wrong-bridge"),
        ("peer", "local", "selected", "accepted"),
    ] {
        bus.publish(BusMessage {
            source: source.into(),
            target: Some(target.into()),
            data: MessageData::Frame {
                frame: Box::new(Frame::text(content)),
                direction: Direction::Downstream,
                bridge: Some(bridge.into()),
            },
        })
        .unwrap();
    }
    let received = text(&mut pipeline).await;
    assert!(matches!(received.frame.data, FrameData::Text(ref text) if text.text == "accepted"));
    pipeline
        .sender()
        .send(Frame::text("excluded"), Direction::Downstream)
        .await
        .unwrap();
    let received = text(&mut pipeline).await;
    assert!(matches!(received.frame.data, FrameData::Text(ref text) if text.text == "excluded"));
    finish(pipeline).await;
    let mut messages = 0;
    while audit.try_recv().unwrap().is_some() {
        messages += 1;
    }
    assert_eq!(messages, 5);
}
