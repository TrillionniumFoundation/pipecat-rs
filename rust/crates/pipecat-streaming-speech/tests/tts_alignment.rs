//! Native TTS provenance and source-derived word timing over real loopback WS.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

type Socket = WebSocketStream<TcpStream>;
async fn read(socket: &mut Socket) -> Value {
    let msg = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(msg.to_text().unwrap()).unwrap()
}
async fn write(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn setup(timestamps: bool) -> (TcpListener, PipelineWorker) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("test", "voice");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    config.add_timestamps = timestamps;
    let worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    (listener, worker)
}
async fn finish(worker: PipelineWorker) -> Vec<Frame> {
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    frames
        .into_iter()
        .filter(|f| f.direction == Direction::Downstream)
        .map(|f| f.frame)
        .collect()
}
async fn audio(socket: &mut Socket, id: &Value) {
    write(
        socket,
        json!({"type":"chunk","context_id":id,"data":"AQACAA=="}),
    )
    .await;
}
async fn words(socket: &mut Socket, id: &Value, words: &[&str], starts: &[f64]) {
    write(socket, json!({"type":"timestamps","context_id":id,"word_timestamps":{"words":words,"start":starts}})).await;
}
async fn done(socket: &mut Socket, id: &Value) {
    write(socket, json!({"type":"done","context_id":id})).await;
    let _ = socket.next().await;
}

#[tokio::test]
async fn prepared_provenance_early_words_anchor_progress_and_source_partition() {
    let (listener, worker) = setup(true).await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request["transcript"], "Pay five dollars");
        assert_eq!(read(&mut socket).await["continue"], false);
        words(
            &mut socket,
            &request["context_id"],
            &["Pay", "five", "dollars"],
            &[0.0, 0.1, 0.2],
        )
        .await;
        audio(&mut socket, &request["context_id"]).await;
        done(&mut socket, &request["context_id"]).await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    let mut input = TtsTextInputFrame::new("Pay $5");
    input.synthesis_text = "Pay five dollars".into();
    input.original_text = "Pay <b>$5</b>".into();
    send(&worker, FrameData::TtsTextInput(input)).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let frames = finish(worker).await;
    let anchor = frames
        .iter()
        .find(|f| matches!(&f.data, FrameData::AggregatedText(t) if t.will_be_spoken))
        .unwrap();
    assert!(
        matches!(&anchor.data, FrameData::AggregatedText(t) if t.text.text == "Pay $5" && t.raw_text.as_deref() == Some("Pay <b>$5</b>") && !t.text.append_to_context)
    );
    let reports: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.data, FrameData::TtsText(_)))
        .collect();
    assert_eq!(reports.len(), 3);
    let raw: String = reports
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsText(t) => t.raw_text.as_deref(),
            _ => None,
        })
        .collect();
    assert_eq!(raw, "Pay <b>$5</b>");
    assert!(reports
        .iter()
        .all(|f| f.metadata["segment_id"] == json!(anchor.id)));
    assert!(matches!(&reports[1].data, FrameData::TtsText(t) if !t.text.append_to_context));
    let progress: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::AggregatedTextProgress(p) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(progress.len(), 2);
    assert!(progress.iter().all(|p| p.segment_id == anchor.id));
    assert_eq!(progress.last().unwrap().accumulated_text, "Pay $5");
    let last_word = frames
        .iter()
        .rposition(|f| matches!(f.data, FrameData::TtsText(_)))
        .unwrap();
    let first_audio = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::TtsAudio(_)))
        .unwrap();
    assert!(last_word < first_audio);
    assert_eq!(
        reports[2].pts.unwrap() - reports[0].pts.unwrap(),
        200_000_000
    );
    let end = frames
        .iter()
        .find(|f| matches!(f.data, FrameData::LlmResponseEnd))
        .unwrap();
    assert!(end.pts.unwrap() > reports[2].pts.unwrap());
    assert!(!frames
        .iter()
        .any(|f| matches!(f.data, FrameData::TtsAlignmentDiagnostic(_))));
    server.await.unwrap();
}
#[tokio::test]
async fn missing_words_remain_diagnostics_and_ordinary_raw_text_is_preserved() {
    let (listener, worker) = setup(true).await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let req = read(&mut socket).await;
        assert_eq!(read(&mut socket).await["continue"], false);
        audio(&mut socket, &req["context_id"]).await;
        words(&mut socket, &req["context_id"], &["Hello"], &[0.0]).await;
        done(&mut socket, &req["context_id"]).await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::AggregatedText(AggregatedTextFrame {
            text: TextFrame::new("Hello world"),
            aggregated_by: "sentence".into(),
            context_id: None,
            raw_text: Some("**Hello world**".into()),
            will_be_spoken: false,
        }),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let frames = finish(worker).await;
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::TtsText(_)))
            .count(),
        1
    );
    let diagnostic = frames
        .iter()
        .find_map(|f| match &f.data {
            FrameData::TtsAlignmentDiagnostic(d) => Some(d),
            _ => None,
        })
        .unwrap();
    assert_eq!(diagnostic.evidence, TtsAlignmentEvidence::Unreported);
    assert_eq!(diagnostic.synthesis_outcome, TtsSynthesisOutcome::Succeeded);
    assert!(diagnostic
        .original_text
        .as_deref()
        .unwrap()
        .contains("world**"));
    server.await.unwrap();
}
#[tokio::test]
async fn timestamp_disabled_success_is_explicit_without_spoken_word_frames() {
    let (listener, worker) = setup(false).await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let req = read(&mut socket).await;
        assert_eq!(req["add_timestamps"], false);
        assert_eq!(read(&mut socket).await["continue"], false);
        audio(&mut socket, &req["context_id"]).await;
        done(&mut socket, &req["context_id"]).await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::TtsTextInput(TtsTextInputFrame::new("Hello")),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let frames = finish(worker).await;
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::TtsText(_) | FrameData::AggregatedTextProgress(_)
    )));
    assert!(frames.iter().any(|f| matches!(&f.data, FrameData::TtsAlignmentDiagnostic(d) if d.evidence == TtsAlignmentEvidence::Unreported && d.synthesis_outcome == TtsSynthesisOutcome::Succeeded && d.original_text.as_deref() == Some("Hello"))));
    server.await.unwrap();
}
#[tokio::test]
async fn token_words_before_sentence_promotion_keep_early_time_and_anchor_order() {
    let (listener, mut worker) = setup(true).await;
    let (ready, early) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let req = read(&mut socket).await;
        assert_eq!(req["transcript"], "Hello");
        words(&mut socket, &req["context_id"], &["Hello"], &[0.0]).await;
        audio(&mut socket, &req["context_id"]).await;
        ready.send(()).unwrap();
        let req2 = read(&mut socket).await;
        assert_eq!(req2["transcript"], " world");
        assert_eq!(read(&mut socket).await["continue"], false);
        words(&mut socket, &req["context_id"], &["world"], &[0.1]).await;
        done(&mut socket, &req["context_id"]).await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Hello"))).await;
    early.await.unwrap();
    let mut before = vec![];
    loop {
        let f = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap()
            .frame;
        let is_audio = matches!(f.data, FrameData::TtsAudio(_));
        before.push(f);
        if is_audio {
            break;
        }
    }
    assert!(!before
        .iter()
        .any(|f| matches!(f.data, FrameData::TtsText(_))));
    send(&worker, FrameData::LlmText(TextFrame::llm(" world"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let frames = finish(worker).await;
    let anchor = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::AggregatedText(_)))
        .unwrap();
    let hello = frames
        .iter()
        .position(|f| matches!(&f.data, FrameData::TtsText(t) if t.text.text == "Hello"))
        .unwrap();
    assert!(anchor < hello);
    assert_eq!(frames[anchor].pts, frames[hello].pts);
    server.await.unwrap();
}

#[tokio::test]
async fn destination_changes_rotate_contexts_and_keep_explicit_spacing() {
    let (listener, worker) = setup(true).await;
    let server = tokio::spawn(async move {
        let mut contexts = vec![];
        for word in ["first", "second"] {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let req = read(&mut socket).await;
            contexts.push(req["context_id"].clone());
            assert_eq!(req["transcript"], word);
            assert_eq!(read(&mut socket).await["continue"], false);
            audio(&mut socket, &req["context_id"]).await;
            words(&mut socket, &req["context_id"], &[word], &[0.0]).await;
            done(&mut socket, &req["context_id"]).await;
        }
        assert_ne!(contexts[0], contexts[1]);
    });
    send(&worker, FrameData::LlmResponseStart).await;
    for (word, destination, spaces) in [("first", "left", true), ("second", "right", false)] {
        let mut text = TtsTextInputFrame::new(word);
        text.includes_inter_frame_spaces = spaces;
        let mut frame = Frame::new(FrameData::TtsTextInput(text));
        frame.transport_destination = Some(destination.into());
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    send(&worker, FrameData::LlmResponseEnd).await;
    let frames = finish(worker).await;
    let mut destination = None;
    for frame in &frames {
        match &frame.data {
            FrameData::TtsStarted(_) => destination = frame.transport_destination.as_deref(),
            FrameData::TtsAudio(_)
            | FrameData::TtsStopped(_)
            | FrameData::AggregatedText(_)
            | FrameData::AggregatedTextProgress(_) => {
                assert_eq!(frame.transport_destination.as_deref(), destination)
            }
            FrameData::TtsText(text) => {
                assert_eq!(frame.transport_destination.as_deref(), destination);
                assert_eq!(
                    text.text.includes_inter_frame_spaces,
                    text.text.text == "first"
                );
            }
            _ => {}
        }
    }
    server.await.unwrap();
}

#[tokio::test]
async fn leading_skipped_aggregate_is_visible_before_any_provider_request_or_response_end() {
    let (_listener, mut worker) = setup(true).await;
    send(&worker, FrameData::LlmResponseStart).await;
    let mut input = TtsTextInputFrame::new("code");
    input.skip_tts = true;
    input.aggregated_by = "code".into();
    send(&worker, FrameData::TtsTextInput(input)).await;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), worker.recv())
            .await
            .unwrap()
            .unwrap()
            .frame;
        assert!(!matches!(frame.data, FrameData::TtsStarted(_)));
        if let FrameData::AggregatedText(text) = frame.data {
            assert_eq!(text.aggregated_by, "code");
            assert!(!text.will_be_spoken);
            assert_eq!(text.text.skip_tts, Some(true));
            break;
        }
    }
    send(&worker, FrameData::LlmResponseEnd).await;
    finish(worker).await;
}
