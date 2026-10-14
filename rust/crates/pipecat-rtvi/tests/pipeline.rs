use pipecat_core::*;
use pipecat_rtvi::{wire::*, *};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .expect("frame timeout")
        .expect("pipeline closed")
}
async fn until(worker: &mut PipelineWorker, f: impl Fn(&FrameData) -> bool) -> Frame {
    loop {
        let frame = next(worker).await.frame;
        if f(&frame.data) {
            return frame;
        }
    }
}
async fn wire_until(worker: &mut PipelineWorker, kind: &str) -> Value {
    let frame = until(
        worker,
        |d| matches!(d, FrameData::OutputTransportMessageUrgent(v) if v["type"] == kind),
    )
    .await;
    let FrameData::OutputTransportMessageUrgent(v) = frame.data else {
        unreachable!()
    };
    v
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
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
fn message(id: &str, kind: &str, data: Value) -> Value {
    json!({"label":"rtvi-ai","id":id,"type":kind,"data":data})
}

#[tokio::test]
async fn handshake_is_correlated_and_waits_for_both_sides() {
    let (mut worker, handle) = start(RtviConfig {
        capabilities: Some(serde_json::from_value(json!({"audio":{"send":true}})).unwrap()),
        ..Default::default()
    })
    .await;
    let mut events = handle.subscribe();
    handle
        .set_bot_ready(Some(
            serde_json::from_value(json!({"library":"native-test"})).unwrap(),
        ))
        .unwrap();
    send(
        &worker,
        message(
            "client-42",
            "client-ready",
            json!({"version":"2.2.0","about":{"library":"test"}}),
        ),
    )
    .await;
    let response = wire_until(&mut worker, "bot-ready").await;
    assert_eq!(
        response,
        json!({"label":"rtvi-ai","id":"client-42","type":"bot-ready","data":{"version":"2.2.0","about":{"library":"native-test"},"capabilities":{"audio":{"send":true}}}})
    );
    assert!(
        matches!(events.recv().await.unwrap(), RtviEvent::ClientReady(s) if s.client_ready && s.bot_ready)
    );
    send(
        &worker,
        message("repeat", "client-ready", json!({"version":"1.4.0"})),
    )
    .await;
    let response = wire_until(&mut worker, "bot-ready").await;
    assert_eq!(response["id"], "repeat");
    assert_eq!(response["data"]["version"], "1.4.0");
    assert!(handle.state().legacy);
    finish(worker).await;
    assert!(matches!(handle.set_bot_ready(None), Err(RtviError::Closed)));
}
#[tokio::test]
async fn missing_invalid_and_incompatible_versions_warn_but_connect() {
    for (input, expected) in [
        (json!({}), "Client version unknown."),
        (
            json!({"version":"x.y"}),
            "Invalid client version format (x.y).",
        ),
        (
            json!({"version":"7.2.0"}),
            "RTVI version 7.2.0 is not compatible with server protocol 2.2.0.",
        ),
    ] {
        let (mut worker, handle) = start(RtviConfig::default()).await;
        handle.set_bot_ready(None).unwrap();
        send(&worker, message("v", "client-ready", input)).await;
        let warning = wire_until(&mut worker, "error-response").await;
        assert_eq!(warning["id"], "v");
        assert_eq!(
            warning["data"]["error"],
            format!("{expected} Compatibility issues may occur.")
        );
        assert_eq!(
            wire_until(&mut worker, "bot-ready").await["data"]["version"],
            "2.2.0"
        );
        finish(worker).await;
    }
}
#[tokio::test]
async fn text_interrupts_flushes_appends_and_restores_audio_configuration() {
    let (mut worker, _) = start(RtviConfig::default()).await;
    send(
        &worker,
        message(
            "t",
            "send-text",
            json!({"content":"hello","options":{"audio_response":false}}),
        ),
    )
    .await;
    let mut kinds = vec![];
    loop {
        let directed = next(&mut worker).await;
        match directed.frame.data {
            FrameData::Interruption => kinds.push(if directed.direction == Direction::Downstream {
                "interrupt-down"
            } else {
                "interrupt-up"
            }),
            FrameData::FlushDetailed(probe) => {
                assert!(probe.returning);
                assert!(probe.event.unwrap().is_set());
                kinds.push("flushed");
            }
            FrameData::LlmConfigureOutput { skip_tts: true } => kinds.push("mute"),
            FrameData::LlmConfigureOutput { skip_tts: false } => {
                kinds.push("restore");
                break;
            }
            FrameData::LlmMessagesAppend(m) => {
                assert_eq!(m.messages[0].content, "hello");
                assert!(m.run_llm);
                kinds.push("append");
            }
            _ => {}
        }
    }
    assert_eq!(
        kinds,
        vec![
            "interrupt-down",
            "interrupt-up",
            "flushed",
            "mute",
            "append",
            "restore"
        ]
    );
    send(
        &worker,
        message(
            "t2",
            "send-text",
            json!({"content":"queued","options":{"run_immediately":false}}),
        ),
    )
    .await;
    let frame = until(&mut worker, |d| {
        matches!(d, FrameData::LlmMessagesAppend(_))
    })
    .await;
    assert!(matches!(frame.data, FrameData::LlmMessagesAppend(m) if !m.run_llm));
    finish(worker).await;
}
#[tokio::test]
async fn audio_batch_dtmf_and_tool_results_become_native_frames_in_order() {
    let (mut worker, _) = start(RtviConfig::default()).await;
    send(
        &worker,
        message(
            "a",
            "raw-audio-batch",
            json!({"base64AudioBatch":["AQACAA==","AwAEAA=="],"sampleRate":16000,"numChannels":1}),
        ),
    )
    .await;
    let audio1 = until(&mut worker, |d| matches!(d, FrameData::InputAudio(_))).await;
    let audio2 = until(&mut worker, |d| matches!(d, FrameData::InputAudio(_))).await;
    assert!(matches!(audio1.data, FrameData::InputAudio(a) if a.audio == [1,0,2,0]));
    assert!(matches!(audio2.data, FrameData::InputAudio(a) if a.audio == [3,0,4,0]));
    send(
        &worker,
        message("d", "dtmf", json!({"buttons":["1","*","#"]})),
    )
    .await;
    for expected in [KeypadEntry::One, KeypadEntry::Star, KeypadEntry::Pound] {
        assert!(
            matches!(until(&mut worker, |d| matches!(d, FrameData::InputDtmf(_))).await.data, FrameData::InputDtmf(k) if k == expected)
        );
    }
    send(&worker, message("f", "llm-function-call-result", json!({"function_name":"weather","tool_call_id":"tool-8","arguments":{"city":"Paris"},"result":{"temp":18}}))).await;
    assert!(
        matches!(until(&mut worker, |d| matches!(d, FrameData::FunctionCallResult(_))).await.data, FrameData::FunctionCallResult(r) if r.tool_call_id == "tool-8" && r.result["temp"] == 18 && r.is_final)
    );
    finish(worker).await;
}
#[tokio::test]
async fn file_data_urls_and_remote_urls_are_framed_without_fetching() {
    let (mut worker, _) = start(RtviConfig::default()).await;
    send(&worker, message("f", "send-file", json!({"content":"inspect","file":{"format":"image/png","source":{"type":"bytes","bytes":"data:image/png;base64,AQID","width":2,"height":3}},"options":{"run_immediately":false}}))).await;
    assert!(
        matches!(until(&mut worker, |d| matches!(d, FrameData::UserImage(_))).await.data, FrameData::UserImage(i) if i.image.image == [1,2,3] && i.image.size == (2,3) && i.run_llm == Some(false))
    );
    send(&worker, message("f2", "send-file", json!({"content":"read","file":{"format":"application/pdf","name":"file.pdf","source":{"type":"url","url":"https://example.invalid/private.pdf"}},"options":{"run_immediately":false,"custom_options":{"page":2}}}))).await;
    assert!(
        matches!(until(&mut worker, |d| matches!(d, FrameData::UserFileDetailed(_))).await.data, FrameData::UserFileDetailed(f) if f.file.source_type == FileSourceType::Url && f.custom_options.as_ref().unwrap()["page"] == 2)
    );
    finish(worker).await;
}
#[tokio::test]
async fn custom_and_ui_messages_preserve_correlation_and_do_not_execute() {
    let (mut worker, handle) = start(RtviConfig::default()).await;
    send(
        &worker,
        message(
            "m",
            "client-message",
            json!({"t":"weather","d":{"city":"Paris"}}),
        ),
    )
    .await;
    let frame = until(&mut worker, |d| {
        matches!(d, FrameData::RtviClientMessage(_))
    })
    .await;
    let FrameData::RtviClientMessage(request) = frame.data else {
        panic!()
    };
    handle
        .send_server_response(&request, json!({"temp":18}))
        .unwrap();
    assert_eq!(
        wire_until(&mut worker, "server-response").await,
        json!({"label":"rtvi-ai","id":"m","type":"server-response","data":{"t":"weather","d":{"temp":18}}})
    );
    send(
        &worker,
        message(
            "u",
            "ui-event",
            json!({"event":"click","payload":{"ref":"button-1"}}),
        ),
    )
    .await;
    assert!(
        matches!(until(&mut worker, |d| matches!(d, FrameData::RtviUiEvent(_))).await.data, FrameData::RtviUiEvent(f) if f.msg_id == "u" && f.event == "click")
    );
    send(
        &worker,
        message(
            "s",
            "ui-snapshot",
            json!({"tree":{"root":{"ref":"0","role":"document","children":[]},"captured_at":42}}),
        ),
    )
    .await;
    assert!(
        matches!(until(&mut worker, |d| matches!(d, FrameData::RtviUiSnapshot(_))).await.data, FrameData::RtviUiSnapshot(f) if f.tree["captured_at"] == 42)
    );
    send(
        &worker,
        message(
            "c",
            "ui-cancel-job-group",
            json!({"job_id":"job","reason":"clicked stop"}),
        ),
    )
    .await;
    assert!(
        matches!(until(&mut worker, |d| matches!(d, FrameData::RtviUiCancelJobGroup(_))).await.data, FrameData::RtviUiCancelJobGroup(f) if f.job_id == "job")
    );
    finish(worker).await;
}
#[tokio::test]
async fn invalid_payload_unknown_type_oversize_and_foreign_messages_are_bounded() {
    let (mut worker, handle) = start(RtviConfig {
        max_message_bytes: 1024,
        max_audio_batch: 2,
        max_dtmf_buttons: 2,
        ..Default::default()
    })
    .await;
    for (kind, data) in [
        (
            "raw-audio",
            json!({"base64Audio":"%%%","sampleRate":16000,"numChannels":1}),
        ),
        ("send-text", json!({"content":5})),
        ("ui-snapshot", json!({"tree":{}})),
        ("dtmf", json!({"buttons":["1","2","3"]})),
        (
            "raw-audio-batch",
            json!({"base64AudioBatch":["AAAA","AAAA","AAAA"],"sampleRate":16000,"numChannels":1}),
        ),
    ] {
        send(&worker, message("bad", kind, data)).await;
        assert_eq!(wire_until(&mut worker, "error-response").await["id"], "bad");
    }
    send(
        &worker,
        message("unknown", "execute-shell", json!({"command":"ls"})),
    )
    .await;
    assert_eq!(
        wire_until(&mut worker, "error-response").await["data"]["error"],
        "Unsupported type execute-shell"
    );
    send(
        &worker,
        message("huge", "send-text", json!({"content":"x".repeat(2048)})),
    )
    .await;
    assert!(wire_until(&mut worker, "error").await["data"]["error"]
        .as_str()
        .unwrap()
        .contains("too large"));
    assert!(matches!(
        handle.handle_bytes(b"not JSON"),
        Err(RtviError::Invalid(_))
    ));
    send(
        &worker,
        json!({"label":"other","id":"ignored","type":"client-ready"}),
    )
    .await;
    send(
        &worker,
        message("after", "client-message", json!({"t":"ok"})),
    )
    .await;
    assert!(
        matches!(until(&mut worker, |d| matches!(d, FrameData::RtviClientMessage(_))).await.data, FrameData::RtviClientMessage(f) if f.msg_id == "after")
    );
    finish(worker).await;
}
#[tokio::test]
async fn disconnect_ends_actual_pipeline_and_cancel_closes_handle() {
    let (worker, handle) = start(RtviConfig::default()).await;
    handle
        .handle_message(Envelope::client("bye", &ClientCommand::DisconnectBot).unwrap())
        .unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    let (worker, handle) = start(RtviConfig::default()).await;
    worker.cancel("test").await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(matches!(
        handle.handle_bytes(b"{\"label\":\"rtvi-ai\",\"id\":\"a\",\"type\":\"disconnect-bot\"}"),
        Err(RtviError::Closed)
    ));
}
#[tokio::test]
async fn concurrent_requests_keep_their_ids_and_bounded_queue_reports_pressure() {
    let (mut worker, handle) = start(RtviConfig {
        command_capacity: 2,
        ..Default::default()
    })
    .await;
    // No await gives the bounded queue no chance to drain on this runtime.
    handle.send_error_response("1", "one").unwrap();
    handle.send_error_response("2", "two").unwrap();
    assert!(matches!(
        handle.send_error_response("3", "three"),
        Err(RtviError::Full)
    ));
    assert_eq!(wire_until(&mut worker, "error-response").await["id"], "1");
    assert_eq!(wire_until(&mut worker, "error-response").await["id"], "2");
    let mut jobs = vec![];
    for id in 0..12 {
        let handle = handle.clone();
        jobs.push(tokio::spawn(async move {
            loop {
                match handle.send_error_response(&format!("req-{id}"), "value") {
                    Ok(()) => break,
                    Err(RtviError::Full) => tokio::task::yield_now().await,
                    e => panic!("{e:?}"),
                }
            }
        }));
    }
    let mut ids = std::collections::BTreeSet::new();
    for _ in 0..12 {
        ids.insert(
            wire_until(&mut worker, "error-response").await["id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    for job in jobs {
        job.await.unwrap();
    }
    assert_eq!(ids.len(), 12);
    finish(worker).await;
}

struct HoldFlush {
    entered: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl FrameProcessor for HoldFlush {
    fn name(&self) -> &str {
        "hold-flush"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::FlushDetailed(_)) {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn cancellation_during_interruption_flush_never_appends_or_hangs() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let rtvi = Arc::new(RtviProcessor::new(RtviConfig::default()).unwrap());
    let handle = rtvi.handle();
    let worker = Pipeline::new(vec![
        rtvi,
        Arc::new(HoldFlush {
            entered: entered.clone(),
        }),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    handle
        .handle_bytes(
            message("pending", "send-text", json!({"content":"must not append"}))
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    worker.cancel("stop during flush").await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::LlmMessagesAppend(_))));
    assert!(matches!(
        handle.send_server_message(json!({})),
        Err(RtviError::Closed)
    ));
}
#[tokio::test]
async fn bad_version_after_legacy_ready_retains_negotiated_version() {
    let (mut worker, handle) = start(RtviConfig::default()).await;
    handle.set_bot_ready(None).unwrap();
    send(
        &worker,
        message("old", "client-ready", json!({"version":"1.04.00"})),
    )
    .await;
    assert_eq!(
        wire_until(&mut worker, "bot-ready").await["data"]["version"],
        "1.4.0"
    );
    send(
        &worker,
        message("invalid", "client-ready", json!({"version":"oops"})),
    )
    .await;
    assert_eq!(
        wire_until(&mut worker, "error-response").await["id"],
        "invalid"
    );
    assert_eq!(
        wire_until(&mut worker, "bot-ready").await["data"]["version"],
        "1.4.0"
    );
    finish(worker).await;
}

#[tokio::test]
async fn json_shape_depth_id_and_type_limits_reject_without_mutating_session() {
    let (mut worker, handle) = start(RtviConfig::default()).await;
    for value in [
        message("v", "client-ready", json!([])),
        message(&"x".repeat(513), "client-ready", json!({"version":"2.2.0"})),
        message("v", &"x".repeat(129), json!({})),
        json!({"label":"rtvi-ai","type":"client-ready"}),
    ] {
        send(&worker, value).await;
        let error = wire_until(&mut worker, "error").await;
        assert_eq!(error["data"]["fatal"], false);
        assert!(!handle.state().client_ready);
    }
    let mut nested = json!({"end":true});
    for _ in 0..70 {
        nested = json!({"next":nested});
    }
    let encoded = message("deep", "client-message", json!({"t":"deep","d":nested}));
    assert!(matches!(
        handle.handle_bytes(encoded.to_string().as_bytes()),
        Err(RtviError::Invalid(_))
    ));
    send(&worker, encoded).await;
    wire_until(&mut worker, "error").await;
    assert!(!handle.state().client_ready);
    finish(worker).await;
}

#[tokio::test]
async fn embedded_non_image_data_url_is_preserved_verbatim() {
    let (mut worker, _) = start(RtviConfig::default()).await;
    send(&worker,message("file","send-file",json!({"content":"read","file":{"format":"application/octet-stream","source":{"type":"bytes","bytes":"data:text/plain;base64,aGVsbG8="}},"options":{"run_immediately":false}}))).await;
    let frame = until(&mut worker, |d| matches!(d, FrameData::UserFileDetailed(_))).await;
    assert!(
        matches!(frame.data,FrameData::UserFileDetailed(f) if f.file.file == "data:text/plain;base64,aGVsbG8=" && f.file.format == "application/octet-stream")
    );
    finish(worker).await;
}

/// The browser workspace independently asserts both real wire encoders produce
/// this shared fixture. This test qualifies native projection, not a live browser.
#[tokio::test]
async fn browser_original_image_fixture_crosses_rtvi_without_reencoding() {
    let value: Value = serde_json::from_str(include_str!(
        "../../../client-templates/tests/fixtures/send-file.json"
    ))
    .unwrap();
    let (mut worker, _) = start(RtviConfig::default()).await;
    send(&worker, value.clone()).await;
    let frame = until(&mut worker, |d| matches!(d, FrameData::UserImage(_))).await;
    let FrameData::UserImage(image) = frame.data else {
        unreachable!()
    };
    assert_eq!(
        image.image.image,
        include_bytes!("../../../client-templates/tests/fixtures/baseline.jpg")
    );
    assert_eq!(image.image.size, (17, 13));
    assert_eq!(image.image.format.as_deref(), Some("image/jpeg"));
    assert_eq!(image.text.as_deref(), value["data"]["content"].as_str());
    assert_eq!(image.run_llm, Some(true));
    finish(worker).await;
}
