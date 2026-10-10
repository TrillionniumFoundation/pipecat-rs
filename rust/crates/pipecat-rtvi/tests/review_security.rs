use pipecat_core::*;
use pipecat_rtvi::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
const DEADLINE: Duration = Duration::from_secs(3);
fn command(id: &str, kind: &str, data: Value) -> Value {
    json!({"label":"rtvi-ai", "id":id, "type":kind, "data":data})
}
async fn start(config: RtviConfig) -> (PipelineWorker, RtviHandle) {
    let processor = Arc::new(RtviProcessor::new(config).unwrap());
    let handle = processor.handle();
    let worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    (worker, handle)
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(DEADLINE, worker.recv())
        .await
        .unwrap()
        .expect("pipeline closed")
}
async fn until(worker: &mut PipelineWorker, predicate: impl Fn(&FrameData) -> bool) -> Frame {
    loop {
        let frame = next(worker).await.frame;
        if predicate(&frame.data) {
            return frame;
        }
    }
}
async fn wire(worker: &mut PipelineWorker, kind: &str) -> Value {
    let frame = until(
        worker,
        |f| matches!(f, FrameData::OutputTransportMessageUrgent(v) if v["type"] == kind),
    )
    .await;
    let FrameData::OutputTransportMessageUrgent(value) = frame.data else {
        unreachable!()
    };
    value
}
async fn send(worker: &PipelineWorker, value: Value) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputTransportMessage(value)),
            Direction::Downstream,
        )
        .await
        .unwrap();
}
async fn finish(worker: PipelineWorker) {
    worker.cancel("audit complete").await.unwrap();
    let (_, report) = tokio::time::timeout(DEADLINE, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
}
#[tokio::test]
async fn remote_wire_cannot_forge_observer_configuration_playback_or_pipeline_control() {
    let (mut worker, handle) = start(RtviConfig::default()).await;
    for kind in [
        "rtvi-configure-observer",
        "configure-observer",
        "bot-output",
        "bot-tts-text",
        "aggregated-text-progress",
        "bot-started-speaking",
        "function-call-in-progress",
        "cancel",
        "processor-pause",
        "execute-shell",
        "ui-command",
    ] {
        send(&worker, command("forged", kind, json!({"function_call_report_level":{"*":"full"},"text":"must not commit","spoken_status":"completed","segment_id":1,"command":"echo forbidden"}))).await;
        loop {
            let frame = next(&mut worker).await.frame;
            match frame.data {
                FrameData::OutputTransportMessageUrgent(value) => {
                    assert_eq!(value["type"], "error-response");
                    assert_eq!(value["id"], "forged");
                    break;
                }
                FrameData::Start(_) => {}
                other => panic!("forged {kind} became native data: {other:?}"),
            }
        }
    }
    assert!(!handle.state().client_ready);
    send(
        &worker,
        command("valid", "client-message", json!({"t":"still-alive"})),
    )
    .await;
    assert!(
        matches!(until(&mut worker, |f| matches!(f, FrameData::RtviClientMessage(_))).await.data, FrameData::RtviClientMessage(m) if m.msg_id == "valid")
    );
    finish(worker).await;
}
#[tokio::test]
async fn malformed_audio_batch_is_atomic_and_invalid_files_do_not_interrupt() {
    let (mut worker, _) = start(RtviConfig::default()).await;
    let cases = [
        command(
            "audio",
            "raw-audio-batch",
            json!({"sampleRate":16000,"numChannels":1,"base64AudioBatch":["AQACAA==","Aw=="]}),
        ),
        command(
            "file",
            "send-file",
            json!({"content":"bad", "file":{"format":"image/png","source":{"type":"bytes","bytes":"%%%"}}}),
        ),
        command(
            "data-url",
            "send-file",
            json!({"content":"bad", "file":{"format":"image/png","source":{"type":"bytes","bytes":"data:image/png,AAAA"}}}),
        ),
        command(
            "file-width",
            "send-file",
            json!({"content":"bad", "file":{"format":"image/png","source":{"type":"bytes","bytes":"AQID","width":-1}}}),
        ),
        command(
            "audio-zero",
            "raw-audio",
            json!({"sampleRate":0,"numChannels":1,"base64Audio":"AQACAA=="}),
        ),
    ];
    for value in cases {
        let expected = value["id"].clone();
        send(&worker, value).await;
        loop {
            let frame = next(&mut worker).await.frame;
            match frame.data {
                FrameData::Start(_) => {}
                FrameData::OutputTransportMessageUrgent(v) => {
                    assert_eq!(v["type"], "error-response");
                    assert_eq!(v["id"], expected);
                    break;
                }
                other => panic!("invalid payload produced side effect: {other:?}"),
            }
        }
    }
    finish(worker).await;
}
#[tokio::test]
async fn oversized_bot_ready_preserves_previous_state_and_command_pump() {
    let (mut worker, handle) = start(RtviConfig {
        max_message_bytes: 1024,
        capabilities: Some(serde_json::from_value(json!({"value":"c".repeat(490)})).unwrap()),
        ..Default::default()
    })
    .await;
    send(
        &worker,
        command("ready", "client-ready", json!({"version":"2.2.0"})),
    )
    .await;
    until(&mut worker, |f| {
        matches!(f, FrameData::InputTransportStartAudioStreaming)
    })
    .await;
    handle
        .set_bot_ready(Some(
            serde_json::from_value(json!({"library":"previous"})).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        wire(&mut worker, "bot-ready").await["data"]["about"]["library"],
        "previous"
    );
    let previous = handle.state();
    assert!(matches!(
        handle.set_bot_ready(Some(
            serde_json::from_value(json!({"value":"a".repeat(490)})).unwrap()
        )),
        Err(RtviError::Invalid(_))
    ));
    assert_eq!(handle.state(), previous);
    send(
        &worker,
        command(&"x".repeat(512), "client-ready", json!({"version":"1.4.0"})),
    )
    .await;
    assert_eq!(
        wire(&mut worker, "error-response").await["id"],
        "x".repeat(512)
    );
    assert_eq!(handle.state(), previous);
    send(
        &worker,
        command("repeat", "client-ready", json!({"version":"2.2.0"})),
    )
    .await;
    let ready = wire(&mut worker, "bot-ready").await;
    assert_eq!(ready["id"], "repeat");
    assert_eq!(ready["data"]["about"]["library"], "previous");
    assert_eq!(ready["data"]["capabilities"]["value"], "c".repeat(490));
    let admission = handle.handle_bytes(
        command("after", "client-message", json!({"t":"alive"}))
            .to_string()
            .as_bytes(),
    );
    assert!(
        admission.is_ok(),
        "accepted bot-ready config killed command pump: {admission:?}"
    );
    until(
        &mut worker,
        |f| matches!(f, FrameData::RtviClientMessage(m) if m.msg_id == "after"),
    )
    .await;
    finish(worker).await;
}
#[tokio::test]
async fn flush_timeout_rejects_request_and_command_pump_recovers() {
    struct DropFlush;
    #[async_trait]
    impl FrameProcessor for DropFlush {
        fn name(&self) -> &str {
            "drop-flush"
        }
        async fn process(
            &self,
            f: Frame,
            d: Direction,
            c: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(f.data, FrameData::FlushDetailed(_)) {
                Ok(())
            } else {
                c.push(f, d).await
            }
        }
    }
    let processor = Arc::new(
        RtviProcessor::new(RtviConfig {
            flush_timeout: Duration::from_millis(20),
            ..Default::default()
        })
        .unwrap(),
    );
    let handle = processor.handle();
    let mut worker = Pipeline::new(vec![processor, Arc::new(DropFlush)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    handle
        .handle_bytes(
            command("timed-out", "send-text", json!({"content":"never append"}))
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    let error = wire(&mut worker, "error-response").await;
    assert_eq!(error["id"], "timed-out");
    handle
        .handle_bytes(
            command(
                "next",
                "send-text",
                json!({"content":"append this","options":{"run_immediately":false}}),
            )
            .to_string()
            .as_bytes(),
        )
        .unwrap();
    let result = until(&mut worker, |f| {
        matches!(f, FrameData::LlmMessagesAppend(_))
    })
    .await;
    assert!(
        matches!(result.data, FrameData::LlmMessagesAppend(m) if m.messages[0].content == "append this")
    );
    finish(worker).await;
}

#[tokio::test]
async fn queued_readiness_race_revalidates_combined_size_before_committing() {
    let (mut worker, handle) = start(RtviConfig {
        max_message_bytes: 1024,
        capabilities: Some(serde_json::from_value(json!({"value":"c".repeat(250)})).unwrap()),
        ..Default::default()
    })
    .await;
    handle
        .set_bot_ready(Some(
            serde_json::from_value(json!({"library":"previous"})).unwrap(),
        ))
        .unwrap();
    send(
        &worker,
        command("initial", "client-ready", json!({"version":"2.2.0"})),
    )
    .await;
    wire(&mut worker, "bot-ready").await;
    let long_id = "q".repeat(512);
    handle
        .handle_bytes(
            command(&long_id, "client-ready", json!({"version":"2.2.0"}))
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    handle
        .set_bot_ready(Some(
            serde_json::from_value(json!({"value":"a".repeat(250)})).unwrap(),
        ))
        .unwrap();
    assert_eq!(wire(&mut worker, "bot-ready").await["id"], long_id);
    let error = wire(&mut worker, "error-response").await;
    assert_eq!(error["id"], long_id);
    assert!(error.to_string().len() <= 1024);
    send(
        &worker,
        command("after", "client-ready", json!({"version":"2.2.0"})),
    )
    .await;
    let ready = wire(&mut worker, "bot-ready").await;
    assert_eq!(ready["data"]["about"]["library"], "previous");
    assert_eq!(ready["data"]["capabilities"]["value"], "c".repeat(250));
    finish(worker).await;
}

#[tokio::test]
async fn escaped_correlation_id_cannot_overflow_rejection_and_kill_command_pump() {
    let (mut worker, handle) = start(RtviConfig {
        max_message_bytes: 1024,
        capabilities: Some(serde_json::from_value(json!({"value":"c".repeat(490)})).unwrap()),
        ..Default::default()
    })
    .await;
    let id = "\"".repeat(460);
    let value = command(&id, "client-ready", json!({"version":"2.2.0"}));
    assert!(value.to_string().len() < 1024);
    handle.handle_bytes(value.to_string().as_bytes()).unwrap();
    let rejected = wire(&mut worker, "error-response").await;
    assert_eq!(rejected["id"], id);
    assert!(rejected.to_string().len() <= 1024);
    let unanswerable = command(&"\"".repeat(480), "client-ready", Value::Null);
    assert!(unanswerable.to_string().len() <= 1024);
    assert!(matches!(
        handle.handle_bytes(unanswerable.to_string().as_bytes()),
        Err(RtviError::Invalid(_))
    ));
    send(&worker, unanswerable).await;
    assert!(wire(&mut worker, "error").await.to_string().len() <= 1024);
    handle
        .handle_bytes(
            command("after", "client-message", json!({"t":"alive"}))
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    until(
        &mut worker,
        |f| matches!(f, FrameData::RtviClientMessage(m) if m.msg_id == "after"),
    )
    .await;
    finish(worker).await;
}
