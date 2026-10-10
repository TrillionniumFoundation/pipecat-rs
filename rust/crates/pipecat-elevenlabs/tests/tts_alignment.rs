//! Source-derived TTS provenance, character-clock units and native output ordering.
//! Synthetic providers run on real loopback sockets; discarded PCM is never Played.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_elevenlabs::*;
use pipecat_text::AggregationType;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
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
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn setup(mode: AggregationType) -> (TcpListener, PipelineWorker) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("loopback", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    let worker = Pipeline::new(vec![Arc::new(
        ElevenLabsTtsProcessor::new(ElevenLabsTtsClient::new(config).unwrap())
            .with_text_aggregation_mode(mode),
    )])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    (listener, worker)
}
async fn finish(worker: PipelineWorker) -> Vec<Frame> {
    finish_with_silent_errors(worker, 0).await
}
async fn finish_with_silent_errors(worker: PipelineWorker, silent_errors: usize) -> Vec<Frame> {
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.errors.len(), silent_errors, "{:?}", report.errors);
    assert!(report
        .errors
        .iter()
        .all(|error| error.error.contains("completed with no audio")));
    frames
        .into_iter()
        .filter(|f| f.direction == Direction::Downstream)
        .map(|f| f.frame)
        .collect()
}
async fn finalization(ws: &mut Socket) {
    assert_eq!(read(ws).await["flush"], true);
    assert_eq!(read(ws).await["close_context"], true);
}
async fn audio(ws: &mut Socket, context: &Value) {
    write(ws, json!({"contextId": context, "audio": "AQACAA=="})).await;
}
async fn alignment(ws: &mut Socket, context: &Value, text: &str, start_ms: u64, step_ms: u64) {
    let chars: Vec<_> = text.chars().map(|c| c.to_string()).collect();
    let starts: Vec<_> = (0..chars.len())
        .map(|i| start_ms + i as u64 * step_ms)
        .collect();
    write(ws,json!({"contextId":context,"alignment":{"chars":chars,"charStartTimesMs":starts,"charDurationsMs":vec![step_ms;chars.len()]}})).await;
}
async fn done(ws: &mut Socket, context: &Value) {
    write(ws, json!({"contextId":context,"isFinal":true})).await;
    assert_eq!(read(ws).await, json!({"close_socket":true}));
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn prepared_channels_early_reports_keep_exact_anchor_and_millisecond_units() {
    let (listener, worker) = setup(AggregationType::Sentence).await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let init = read(&mut ws).await;
        assert_eq!(read(&mut ws).await["text"], "Pay five dollars");
        finalization(&mut ws).await;
        alignment(&mut ws, &init["context_id"], "Pay five dollars ", 250, 10).await;
        audio(&mut ws, &init["context_id"]).await;
        done(&mut ws, &init["context_id"]).await;
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
    let words: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.data, FrameData::TtsText(_)))
        .collect();
    assert_eq!(words.len(), 3);
    assert!(words
        .iter()
        .all(|f| f.metadata["segment_id"] == json!(anchor.id)));
    let raw: String = words
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsText(t) => t.raw_text.as_deref(),
            _ => None,
        })
        .collect();
    assert_eq!(raw, "Pay <b>$5</b>");
    assert_eq!(words[0].metadata["audio_offset_seconds"], 0.25);
    assert_eq!(words[0].metadata["audio_end_seconds"], 0.28);
    assert_eq!(words[2].pts.unwrap() - words[0].pts.unwrap(), 90_000_000);
    assert_eq!(words[0].pts.unwrap() - anchor.pts.unwrap(), 250_000_000);
    let progress: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::AggregatedTextProgress(p) => Some(p),
            _ => None,
        })
        .collect();
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
    let end = frames
        .iter()
        .find(|f| matches!(f.data, FrameData::LlmResponseEnd))
        .unwrap();
    assert!(end.pts.unwrap() > words[2].pts.unwrap());
    server.await.unwrap();
}

#[tokio::test]
async fn missing_word_reports_are_diagnostics_with_delimiters_preserved() {
    let (listener, worker) = setup(AggregationType::Sentence).await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let init = read(&mut ws).await;
        assert_eq!(read(&mut ws).await["text"], "Hello world");
        finalization(&mut ws).await;
        audio(&mut ws, &init["context_id"]).await;
        alignment(&mut ws, &init["context_id"], "Hello ", 0, 10).await;
        done(&mut ws, &init["context_id"]).await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::AggregatedText(AggregatedTextFrame {
            text: TextFrame::new("Hello world"),
            aggregated_by: "markdown".into(),
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
    let anchor = frames
        .iter()
        .find(|f| matches!(f.data, FrameData::AggregatedText(_)))
        .unwrap();
    assert!(
        matches!(&anchor.data, FrameData::AggregatedText(t) if t.aggregated_by == "markdown" && t.raw_text.as_deref() == Some("**Hello world**"))
    );
    let diagnostic = frames
        .iter()
        .find_map(|f| match &f.data {
            FrameData::TtsAlignmentDiagnostic(d) => Some(d),
            _ => None,
        })
        .unwrap();
    assert_eq!(diagnostic.segment_id, Some(anchor.id));
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
async fn no_audio_drops_early_words_and_does_not_invent_spoken_progress() {
    let (listener, worker) = setup(AggregationType::Sentence).await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let init = read(&mut ws).await;
        read(&mut ws).await;
        finalization(&mut ws).await;
        alignment(&mut ws, &init["context_id"], "Hello ", 0, 1).await;
        done(&mut ws, &init["context_id"]).await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::TtsTextInput(TtsTextInputFrame::new("Hello")),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let frames = finish_with_silent_errors(worker, 1).await;
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::TtsText(_) | FrameData::AggregatedTextProgress(_) | FrameData::TtsAudio(_)
    )));
    assert!(frames.iter().any(|f|matches!(&f.data,FrameData::TtsAlignmentDiagnostic(d) if d.evidence == TtsAlignmentEvidence::Unreported && d.synthesis_outcome == TtsSynthesisOutcome::Succeeded)));
    server.await.unwrap();
}

#[tokio::test]
async fn token_reports_before_promotion_retain_start_and_end_metadata() {
    let (listener, mut worker) = setup(AggregationType::Token).await;
    let (ready, early) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let init = read(&mut ws).await;
        assert_eq!(read(&mut ws).await["text"], "Hello");
        alignment(&mut ws, &init["context_id"], "Hello ", 0, 10).await;
        audio(&mut ws, &init["context_id"]).await;
        ready.send(()).unwrap();
        assert_eq!(read(&mut ws).await["text"], " world");
        finalization(&mut ws).await;
        alignment(&mut ws, &init["context_id"], "world ", 0, 10).await;
        done(&mut ws, &init["context_id"]).await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Hello"))).await;
    early.await.unwrap();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap()
            .frame;
        assert!(!matches!(frame.data, FrameData::TtsText(_)));
        if matches!(frame.data, FrameData::TtsAudio(_)) {
            break;
        }
    }
    send(&worker, FrameData::LlmText(TextFrame::llm(" world"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let frames = finish(worker).await;
    let anchor = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::AggregatedText(_)))
        .unwrap();
    let hello = frames
        .iter()
        .position(|f| matches!(&f.data,FrameData::TtsText(t) if t.text.text=="Hello"))
        .unwrap();
    assert!(anchor < hello);
    assert_eq!(frames[anchor].pts, frames[hello].pts);
    assert_eq!(frames[hello].metadata["audio_end_seconds"], 0.05);
    server.await.unwrap();
}

#[tokio::test]
async fn destination_and_append_changes_rotate_immutable_contexts_and_keep_spacing() {
    let (listener, worker) = setup(AggregationType::Sentence).await;
    let server = tokio::spawn(async move {
        let mut contexts = vec![];
        for text in ["first", "second", "third"] {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let init = read(&mut ws).await;
            contexts.push(init["context_id"].clone());
            assert_eq!(read(&mut ws).await["text"], text);
            finalization(&mut ws).await;
            audio(&mut ws, &init["context_id"]).await;
            alignment(&mut ws, &init["context_id"], &format!("{text} "), 0, 1).await;
            done(&mut ws, &init["context_id"]).await;
        }
        assert!(contexts.windows(2).all(|w| w[0] != w[1]));
    });
    send(&worker, FrameData::LlmResponseStart).await;
    for (word, destination, append, spaces) in [
        ("first", "left", true, true),
        ("second", "right", true, false),
        ("third", "right", false, true),
    ] {
        let mut input = TtsTextInputFrame::new(word);
        input.append_to_context = append;
        input.includes_inter_frame_spaces = spaces;
        let mut frame = Frame::new(FrameData::TtsTextInput(input));
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
    let mut previous_stop = 0;
    for frame in &frames {
        match &frame.data {
            FrameData::TtsStarted(_) => destination = frame.transport_destination.as_deref(),
            FrameData::TtsText(text) => {
                assert_eq!(frame.transport_destination.as_deref(), destination);
                assert_eq!(
                    text.text.includes_inter_frame_spaces,
                    text.text.text != "second"
                );
                assert_eq!(text.text.append_to_context, text.text.text != "third");
                assert!(frame.pts.unwrap() >= previous_stop);
            }
            FrameData::TtsStopped(_) => {
                assert_eq!(frame.transport_destination.as_deref(), destination);
                previous_stop = frame.pts.unwrap();
            }
            FrameData::TtsAudio(_)
            | FrameData::AggregatedText(_)
            | FrameData::AggregatedTextProgress(_) => {
                assert_eq!(frame.transport_destination.as_deref(), destination)
            }
            _ => {}
        }
    }
    server.await.unwrap();
}

#[tokio::test]
async fn skipped_only_context_is_visible_without_a_provider_connection() {
    let (listener, mut worker) = setup(AggregationType::Sentence).await;
    send(&worker, FrameData::LlmResponseStart).await;
    let mut input = TtsTextInputFrame::new("code");
    input.skip_tts = true;
    input.original_text = "```code```".into();
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
            assert!(!text.will_be_spoken);
            assert_eq!(text.text.skip_tts, Some(true));
            assert_eq!(text.raw_text.as_deref(), Some("```code```"));
            break;
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
    send(&worker, FrameData::LlmResponseEnd).await;
    finish(worker).await;
}

struct Clock(tokio::time::Instant);
impl ObserverClock for Clock {
    fn now_ns(&self) -> u64 {
        9_000_000_000 + self.0.elapsed().as_nanos() as u64
    }
}
struct DiscardBackend {
    frames: std::sync::Mutex<Vec<Frame>>,
    entered: tokio::sync::Semaphore,
}
#[async_trait]
impl pipecat_media_transport::OutputBackend for DiscardBackend {
    async fn start(
        &self,
        _: pipecat_audio::PcmFormat,
        _: &[Option<String>],
    ) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(
        &self,
        frame: &Frame,
    ) -> Result<pipecat_media_transport::PlaybackReceipt, ProcessorError> {
        self.frames.lock().unwrap().push(frame.clone());
        self.entered.add_permits(1);
        Ok(pipecat_media_transport::PlaybackReceipt::Discarded)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
struct Probe {
    frames: std::sync::Mutex<Vec<Frame>>,
    name: &'static str,
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        self.name
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.frames.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn real_output_path_preserves_anchors_rebases_timing_and_never_claims_discarded_pcm_played() {
    use pipecat_media_transport::*;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("loopback", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.sample_rate = 8000;
    config.limits.close_timeout = Duration::from_millis(100);
    let backend = Arc::new(DiscardBackend {
        frames: Default::default(),
        entered: tokio::sync::Semaphore::new(0),
    });
    let before = Arc::new(Probe {
        frames: Default::default(),
        name: "before",
    });
    let after = Arc::new(Probe {
        frames: Default::default(),
        name: "after",
    });
    let output = Arc::new(
        OutputTransport::new(
            "output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                chunk_duration: Duration::from_millis(10),
                auto_silence: false,
                end_silence: Duration::ZERO,
                destinations: vec!["speaker".into()],
                ..Default::default()
            },
            backend.clone(),
            None,
            Default::default(),
        )
        .unwrap(),
    );
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_out_sample_rate = 8000;
    let worker = Pipeline::new(vec![
        Arc::new(ElevenLabsTtsProcessor::new(
            ElevenLabsTtsClient::new(config).unwrap(),
        )),
        before.clone(),
        output,
        after.clone(),
    ])
    .with_observer_clock(Arc::new(Clock(tokio::time::Instant::now())))
    .start(pipeline_config)
    .await
    .unwrap();
    let (sent, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let init = read(&mut ws).await;
        assert_eq!(read(&mut ws).await["text"], "Hello world");
        finalization(&mut ws).await;
        // Future words precede a complete native 10 ms PCM output chunk.
        alignment(&mut ws, &init["context_id"], "Hello world ", 0, 50).await;
        write(
            &mut ws,
            json!({"contextId":init["context_id"],"audio":STANDARD.encode([0xe8,0x03].repeat(80))}),
        )
        .await;
        sent.send(()).unwrap();
        done(&mut ws, &init["context_id"]).await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    let mut input = TtsTextInputFrame::new("Hello world");
    input.original_text = "**Hello world**".into();
    input.includes_inter_frame_spaces = true;
    let mut frame = Frame::new(FrameData::TtsTextInput(input));
    frame.transport_destination = Some("speaker".into());
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    send(&worker, FrameData::LlmResponseEnd).await;
    received.await.unwrap();
    tokio::time::timeout(Duration::from_millis(200), backend.entered.acquire())
        .await
        .expect("future text blocked PCM submission")
        .unwrap()
        .forget();
    let frames = finish(worker).await;
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::BotStartedSpeaking | FrameData::BotSpeaking | FrameData::BotStoppedSpeaking
    )));
    server.await.unwrap();
    let before = before.frames.lock().unwrap();
    let after = after.frames.lock().unwrap();
    let anchor = after
        .iter()
        .find(|f| matches!(&f.data,FrameData::AggregatedText(t) if t.will_be_spoken))
        .unwrap();
    let words: Vec<_> = after
        .iter()
        .filter(|f| matches!(f.data, FrameData::TtsText(_)))
        .collect();
    assert_eq!(words.len(), 2);
    assert!(words
        .iter()
        .all(|f| f.metadata["segment_id"] == json!(anchor.id)
            && f.transport_destination.as_deref() == Some("speaker")));
    assert!(words.iter().all(|f| f.pts.unwrap() < 1_000_000_000));
    assert_eq!(words[1].pts.unwrap() - words[0].pts.unwrap(), 300_000_000);
    assert_eq!(words[1].metadata["audio_offset_seconds"], 0.3);
    let original = before.iter().find(|f| f.id == words[0].id).unwrap();
    assert!(original.pts.unwrap() >= 9_000_000_000);
    let stop = after
        .iter()
        .position(|f| matches!(f.data, FrameData::TtsStopped(_)))
        .unwrap();
    let last_word = after
        .iter()
        .rposition(|f| matches!(f.data, FrameData::TtsText(_)))
        .unwrap();
    assert!(last_word < stop);
    assert!(!backend.frames.lock().unwrap().is_empty());
}
