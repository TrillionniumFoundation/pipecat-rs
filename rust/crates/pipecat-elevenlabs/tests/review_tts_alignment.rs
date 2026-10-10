//! Independent regressions for prepared-text metadata and fail-closed timing.
//! Synthetic provider peers use real loopback WebSockets; no playback is claimed.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_elevenlabs::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

type Socket = WebSocketStream<TcpStream>;

async fn read(ws: &mut Socket) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

async fn write(ws: &mut Socket, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}

async fn setup() -> (TcpListener, Arc<ElevenLabsTtsProcessor>, PipelineWorker) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("loopback", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    let processor = Arc::new(ElevenLabsTtsProcessor::new(
        ElevenLabsTtsClient::new(config).unwrap(),
    ));
    let worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    (listener, processor, worker)
}

async fn finish(worker: PipelineWorker) -> (Vec<Frame>, WorkerReport) {
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    (
        frames
            .into_iter()
            .filter(|f| f.direction == Direction::Downstream)
            .map(|f| f.frame)
            .collect(),
        report,
    )
}

async fn successful_response(ws: &mut Socket, context: &Value, words: &str) {
    let chars: Vec<_> = words.chars().map(|c| c.to_string()).collect();
    let starts: Vec<_> = (0..chars.len()).map(|i| i * 10).collect();
    write(ws, json!({"contextId":context,"alignment":{"chars":chars,"charStartTimesMs":starts,"charDurationsMs":vec![10;chars.len()]}})).await;
    write(
        ws,
        json!({"contextId":context,"audio":"AQACAA==","isFinal":true}),
    )
    .await;
    assert_eq!(read(ws).await, json!({"close_socket":true}));
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn exact_service_name_remains_a_valid_settings_target() {
    let (_listener, processor, worker) = setup().await;
    assert_eq!(processor.name(), "ElevenLabsTTSService");
    send(
        &worker,
        Frame::new(FrameData::TtsUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame {
                service: Some(ProcessorReference::Name("ElevenLabsTTSService".into())),
                delta: Some(TtsSettings {
                    voice: Setting::Value("updated".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ))),
    )
    .await;
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(processor.config_snapshot().unwrap().voice_id, "updated");
}

#[tokio::test]
async fn skipped_prepared_input_preserves_explicit_spacing_and_source() {
    let (listener, _processor, worker) = setup().await;
    let mut input = TtsTextInputFrame::new("skip");
    input.original_text = "<code>skip</code>".into();
    input.skip_tts = true;
    input.includes_inter_frame_spaces = false;
    input.append_to_context = false;
    let frame = Frame::new(FrameData::TtsTextInput(input));
    let source = frame.id;
    send(&worker, frame).await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let skipped = frames
        .iter()
        .find(|f| matches!(f.data, FrameData::AggregatedText(_)))
        .unwrap();
    let FrameData::AggregatedText(text) = &skipped.data else {
        unreachable!()
    };
    assert!(
        !text.text.includes_inter_frame_spaces,
        "explicit skipped spacing is immutable"
    );
    assert!(!text.text.append_to_context && !text.will_be_spoken);
    assert_eq!(text.raw_text.as_deref(), Some("<code>skip</code>"));
    if let Some(id) = skipped.metadata.get("source_frame_id") {
        assert_eq!(*id, json!(source));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn invalid_original_mapping_fails_before_opening_provider_socket() {
    let (listener, _processor, worker) = setup().await;
    let mut input = TtsTextInputFrame::new("speech");
    input.original_text = "**".into();
    send(&worker, Frame::new(FrameData::LlmResponseStart)).await;
    send(&worker, Frame::new(FrameData::TtsTextInput(input))).await;
    send(&worker, Frame::new(FrameData::LlmResponseEnd)).await;
    let (frames, report) = finish(worker).await;
    assert!(!report.errors.is_empty());
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::TtsStarted(_)
            | FrameData::TtsAudio(_)
            | FrameData::TtsText(_)
            | FrameData::AggregatedText(_)
            | FrameData::TtsAlignmentDiagnostic(_)
    )));
    assert!(frames
        .iter()
        .any(|f| matches!(f.data, FrameData::LlmResponseEnd)
            && f.metadata.get("failed") == Some(&json!(true))));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn every_prepared_aggregate_has_exact_anchor_identity_without_false_source_claims() {
    let (listener, _processor, worker) = setup().await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let context = read(&mut ws).await["context_id"].clone();
        assert_eq!(read(&mut ws).await["text"], "one");
        assert_eq!(read(&mut ws).await["text"], "two");
        assert_eq!(read(&mut ws).await["flush"], true);
        assert_eq!(read(&mut ws).await["close_context"], true);
        successful_response(&mut ws, &context, "one two ").await;
    });
    let mut sources = vec![];
    for word in ["one", "two"] {
        let mut input = TtsTextInputFrame::new(word);
        input.original_text = format!("**{word}**");
        let frame = Frame::new(FrameData::TtsTextInput(input));
        sources.push(frame.id);
        send(&worker, frame).await;
    }
    let (frames, report) = finish(worker).await;
    server.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let anchors: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.data, FrameData::AggregatedText(_)))
        .collect();
    assert_eq!(anchors.len(), 2);
    for (anchor, source) in anchors.iter().zip(sources) {
        if let Some(id) = anchor.metadata.get("source_frame_id") {
            assert_eq!(
                *id,
                json!(source),
                "optional source identity must name its own input"
            );
        }
        let words: Vec<_> = frames
            .iter()
            .filter(|f| f.metadata.get("segment_id") == Some(&json!(anchor.id)))
            .collect();
        assert_eq!(words.len(), 1);
        if let Some(id) = words[0].metadata.get("source_frame_id") {
            assert_eq!(*id, json!(source));
        }
        let FrameData::AggregatedText(anchor_text) = &anchor.data else {
            unreachable!()
        };
        let FrameData::TtsText(word) = &words[0].data else {
            unreachable!()
        };
        assert_eq!(word.context_id, anchor_text.context_id);
        assert_eq!(word.raw_text, anchor_text.raw_text);
        let progress = frames
            .iter()
            .find_map(|frame| match &frame.data {
                FrameData::AggregatedTextProgress(progress) if progress.segment_id == anchor.id => {
                    Some(progress)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(progress.context_id, anchor_text.context_id);
        assert_eq!(progress.accumulated_text, anchor_text.text.text);
    }
}

#[tokio::test]
async fn prepared_anchor_retains_explicit_no_inter_frame_spaces() {
    let (listener, _processor, worker) = setup().await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let context = read(&mut ws).await["context_id"].clone();
        read(&mut ws).await;
        assert_eq!(read(&mut ws).await["flush"], true);
        assert_eq!(read(&mut ws).await["close_context"], true);
        successful_response(&mut ws, &context, "word ").await;
    });
    let mut input = TtsTextInputFrame::new("word");
    input.includes_inter_frame_spaces = false;
    send(&worker, Frame::new(FrameData::TtsTextInput(input))).await;
    let (frames, report) = finish(worker).await;
    server.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let anchor = frames
        .iter()
        .find_map(|f| match &f.data {
            FrameData::AggregatedText(t) => Some(t),
            _ => None,
        })
        .unwrap();
    assert!(!anchor.text.includes_inter_frame_spaces);
}

#[tokio::test]
async fn overflowing_early_timestamp_retires_context_without_success_evidence() {
    let (listener, _processor, worker) = setup().await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let context = read(&mut ws).await["context_id"].clone();
        read(&mut ws).await;
        assert_eq!(read(&mut ws).await["flush"], true);
        assert_eq!(read(&mut ws).await["close_context"], true);
        write(&mut ws, json!({"contextId":context,"alignment":{"chars":["x"," "],"charStartTimesMs":[2e13,2e13],"charDurationsMs":[1,1]}})).await;
        assert_eq!(read(&mut ws).await["close_context"], true);
        assert_eq!(read(&mut ws).await["close_socket"], true);
        ws.close(None).await.unwrap();
    });
    send(&worker, Frame::new(FrameData::LlmResponseStart)).await;
    send(
        &worker,
        Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new("x"))),
    )
    .await;
    send(&worker, Frame::new(FrameData::LlmResponseEnd)).await;
    let (frames, report) = finish(worker).await;
    server.await.unwrap();
    assert!(!report.errors.is_empty());
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::TtsText(_)
            | FrameData::TtsAlignmentDiagnostic(_)
            | FrameData::AggregatedTextProgress(_)
    )));
    for frame in frames
        .iter()
        .filter(|f| matches!(f.data, FrameData::TtsStopped(_) | FrameData::LlmResponseEnd))
    {
        assert_eq!(frame.metadata.get("failed"), Some(&json!(true)));
    }
}

#[tokio::test]
async fn one_provider_word_split_between_aggregates_retains_end_timing_on_both_parts() {
    let (listener, _processor, worker) = setup().await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let context = read(&mut ws).await["context_id"].clone();
        assert_eq!(read(&mut ws).await["text"], "foo");
        assert_eq!(read(&mut ws).await["text"], "bar");
        assert_eq!(read(&mut ws).await["flush"], true);
        assert_eq!(read(&mut ws).await["close_context"], true);
        successful_response(&mut ws, &context, "foobar ").await;
    });
    for word in ["foo", "bar"] {
        send(
            &worker,
            Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new(word))),
        )
        .await;
    }
    let (frames, report) = finish(worker).await;
    server.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let words: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.data, FrameData::TtsText(_)))
        .collect();
    assert_eq!(words.len(), 2);
    assert_eq!(words[0].pts, words[1].pts);
    assert_ne!(
        words[0].metadata["segment_id"],
        words[1].metadata["segment_id"]
    );
    for frame in words {
        assert_eq!(
            frame.metadata.get("audio_offset_seconds"),
            Some(&json!(0.0))
        );
        let end = frame
            .metadata
            .get("audio_end_seconds")
            .and_then(Value::as_f64);
        assert!(
            end.is_some_and(|end| (end - 0.06).abs() < 1e-9),
            "split provider report lost its end metadata: {end:?}"
        );
    }
}

#[tokio::test]
async fn handled_resynchronization_diagnostics_do_not_exhaust_pending_word_timing_budget() {
    let count = pipecat_text::TtsAlignmentLimits::default()
        .sequencer
        .max_buffered_words
        + 1;
    let groups: Vec<_> = (0..count)
        .step_by(32)
        .map(|start| (count - start).min(32))
        .collect();
    let server_groups = groups.clone();
    let (listener, _processor, mut worker) = setup().await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let context = read(&mut ws).await["context_id"].clone();
        for (index, count) in server_groups.into_iter().enumerate() {
            assert_eq!(read(&mut ws).await["text"], "a b ".repeat(count));
            if index == 0 {
                write(&mut ws, json!({"contextId":context,"audio":"AQACAA=="})).await;
            }
            let text = "b ".repeat(count);
            let chars: Vec<_> = text.chars().map(|c| c.to_string()).collect();
            let starts: Vec<_> = (0..chars.len()).map(|i| i * 10).collect();
            write(&mut ws, json!({"contextId":context,"alignment":{"chars":chars,"charStartTimesMs":starts,"charDurationsMs":vec![10;chars.len()]}})).await;
        }
        assert_eq!(read(&mut ws).await["flush"], true);
        assert_eq!(read(&mut ws).await["close_context"], true);
        write(&mut ws, json!({"contextId":context,"isFinal":true})).await;
        assert_eq!(read(&mut ws).await["close_socket"], true);
        ws.close(None).await.unwrap();
    });
    send(&worker, Frame::new(FrameData::LlmResponseStart)).await;
    let mut diagnostic_count = 0;
    for group in groups {
        send(
            &worker,
            Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new(
                "a b ".repeat(group),
            ))),
        )
        .await;
        let target = diagnostic_count + group;
        while diagnostic_count < target {
            let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
                .await
                .unwrap()
                .unwrap()
                .frame;
            assert!(!matches!(
                frame.data,
                FrameData::Error(_) | FrameData::TtsText(_)
            ));
            if matches!(&frame.data, FrameData::TtsAlignmentDiagnostic(d) if d.evidence == TtsAlignmentEvidence::Resynchronized)
            {
                diagnostic_count += 1;
            }
        }
    }
    send(&worker, Frame::new(FrameData::LlmResponseEnd)).await;
    let (frames, report) = finish(worker).await;
    server.await.unwrap();
    assert!(
        report.errors.is_empty(),
        "handled reports must release timing budget: {:?}",
        report.errors
    );
    assert_eq!(diagnostic_count, count);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.data, FrameData::TtsText(_))));
    assert!(
        frames
            .iter()
            .any(|f| matches!(f.data, FrameData::LlmResponseEnd)
                && !f.metadata.contains_key("failed"))
    );
}
