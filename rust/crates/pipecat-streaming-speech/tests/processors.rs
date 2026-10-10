use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test]
async fn provider_error_preserves_the_response_end_barrier() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("test-key", "voice");
    config.endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = receive_json(&mut socket).await;
        socket
            .send(Message::Text(
                json!({"type":"error","context_id":request["context_id"],"status_code":429})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        loop {
            let message = receive_json(&mut socket).await;
            if message.get("cancel") == Some(&json!(true)) {
                break;
            }
        }
    });
    let mut worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    next(&mut worker).await;
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
            Frame::new(FrameData::LlmText(TextFrame::llm("hello"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let mut stopped = false;
    while !stopped {
        let frame = next(&mut worker).await;
        if let FrameData::Error(error) = frame.frame.data {
            assert_eq!(error.category, ErrorCategory::RateLimit);
            assert_eq!(frame.direction, Direction::Upstream);
        } else if matches!(frame.frame.data, FrameData::TtsStopped(_)) {
            stopped = true;
        }
    }
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmText(TextFrame::llm(" ignored"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
        .await
        .unwrap();
    worker.sender().end().await.unwrap();
    let mut ended = false;
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
    {
        if matches!(frame.frame.data, FrameData::LlmResponseEnd) {
            assert_eq!(frame.frame.metadata.get("failed"), Some(&json!(true)));
            ended = true;
        }
        assert!(!matches!(
            frame.frame.data,
            FrameData::TtsAudio(_) | FrameData::TtsStarted(_)
        ));
    }
    assert!(ended);
    let report = worker.join().await.unwrap();
    assert_eq!(report.errors.len(), 2);
    assert!(report.errors[1].error.contains("completed with no audio"));
    server.await.unwrap();
}

async fn receive_json(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> Value {
    let message = socket.next().await.unwrap().unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn next(worker: &mut pipecat_core::PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .expect("pipeline event timed out")
        .unwrap()
}

#[tokio::test]
async fn stt_pipeline_streams_transcripts_survives_interruption_and_drains_on_end() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramConfig::new("test-key");
    config.endpoint = format!("ws://{}/v1/listen", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(matches!(socket.next().await, Some(Ok(Message::Binary(_)))));
        socket.send(Message::Text(json!({"type":"Results","is_final":true,"channel":{"alternatives":[{"transcript":"hello","languages":["en"]}]}}).to_string().into())).await.unwrap();
        assert_eq!(receive_json(&mut socket).await, json!({"type":"Finalize"}));
        socket.send(Message::Text(json!({"type":"Results","is_final":true,"from_finalize":true,"channel":{"alternatives":[{"transcript":"","languages":["en"]}]}}).to_string().into())).await.unwrap();
        assert_eq!(
            receive_json(&mut socket).await,
            json!({"type":"CloseStream"})
        );
        socket.close(None).await.unwrap();
    });
    let stt =
        DeepgramSttProcessor::new(DeepgramClient::new(config).unwrap()).with_user_id("speaker");
    let mut worker = Pipeline::new(vec![Arc::new(stt)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Start(_)
    ));
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(vec![0; 640], 16000, 1).unwrap(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let mut found = false;
    while !found {
        match next(&mut worker).await.frame.data {
            FrameData::Transcription(transcript) => {
                assert_eq!(transcript.text, "hello");
                assert_eq!(transcript.user_id, "speaker");
                found = true;
            }
            FrameData::InputAudio(_) => {}
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.02,
                timestamp: 1.0,
            })),
            Direction::Upstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::end(), Direction::Downstream)
        .await
        .unwrap();
    while worker.recv().await.is_some() {}
    let report = worker.join().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
}

#[tokio::test]
async fn tts_pipeline_streams_words_with_context_offsets_and_defers_response_end() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("test-key", "voice");
    config.endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let first = receive_json(&mut socket).await;
        assert_eq!(first["transcript"], "Hello ");
        let context = first["context_id"].clone();
        let second = receive_json(&mut socket).await;
        assert_eq!(second["transcript"], "world.");
        assert_eq!(second["context_id"], context);
        let finish = receive_json(&mut socket).await;
        assert_eq!(finish["continue"], false);
        tokio::time::sleep(Duration::from_millis(30)).await;
        for message in [
            json!({"type":"chunk","context_id":context,"data":"AAAAAA=="}),
            json!({"type":"timestamps","context_id":context,"word_timestamps":{"words":["Hello","world."],"start":[0.0,0.5],"end":[0.4,0.9]}}),
            json!({"type":"done","context_id":context}),
        ] {
            socket
                .send(Message::Text(message.to_string().into()))
                .await
                .unwrap();
        }
        let _ = socket.next().await;
    });
    let processor = CartesiaTtsProcessor::new(CartesiaClient::new(config).unwrap());
    let mut worker = Pipeline::new(vec![Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let _ = next(&mut worker).await;
    for frame in [
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::LlmText(TextFrame::llm("Hello "))),
        Frame::new(FrameData::LlmText(TextFrame::llm("world."))),
        Frame::new(FrameData::LlmResponseEnd),
        Frame::end(),
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    let mut context = None;
    let mut stopped = false;
    let mut response_ended = false;
    let mut word_count = 0;
    let mut baseline = None;
    let mut audio = false;
    while let Some(directed) = tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
    {
        let frame = directed.frame;
        match frame.data {
            FrameData::LlmResponseStart => {}
            FrameData::AggregatedText(text) => {
                assert!(text.will_be_spoken && !text.text.append_to_context);
                baseline = frame.pts;
            }
            FrameData::AggregatedTextProgress(_) => {}
            FrameData::TtsStarted(start) => context = start.context_id,
            FrameData::TtsAudio(chunk) => {
                assert_eq!(chunk.context_id, context);
                audio = true;
            }
            FrameData::TtsText(text) => {
                assert_eq!(text.context_id, context);
                assert_ne!(frame.metadata.get("spoken"), Some(&json!(true)));
                assert_eq!(
                    frame.pts,
                    Some(baseline.unwrap() + if word_count == 0 { 0 } else { 500_000_000 })
                );
                word_count += 1;
            }
            FrameData::TtsStopped(stop) => {
                assert_eq!(stop.context_id, context);
                assert!(audio);
                stopped = true;
            }
            FrameData::LlmResponseEnd => {
                assert!(stopped);
                response_ended = true;
            }
            FrameData::End(_) => assert!(response_ended),
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    assert_eq!(word_count, 2);
    assert!(response_ended);
    assert!(worker.join().await.unwrap().errors.is_empty());
    server.await.unwrap();
}

#[tokio::test]
async fn tts_interruption_cancels_provider_and_allows_the_next_turn() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("test-key", "voice");
    config.endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
    let (started_tx, started_rx) = oneshot::channel();
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut warmup = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let warmup_request = receive_json(&mut warmup).await;
        assert_eq!(warmup_request["transcript"], "warmup");
        assert_eq!(receive_json(&mut warmup).await["continue"], false);
        warmup
            .send(Message::Text(
                json!({"type":"done","context_id":warmup_request["context_id"]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = receive_json(&mut first).await;
        assert_ne!(warmup_request["context_id"], request["context_id"]);
        started_tx.send(()).unwrap();
        assert_eq!(
            receive_json(&mut first).await,
            json!({"context_id":request["context_id"],"cancel":true})
        );
        // Late provider audio from the interrupted socket must not escape.
        let _ = first
            .send(Message::Text(
                json!({"type":"chunk","context_id":request["context_id"],"data":"//8="})
                    .to_string()
                    .into(),
            ))
            .await;
        cancelled_tx.send(()).unwrap();
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request2 = receive_json(&mut second).await;
        assert_ne!(request["context_id"], request2["context_id"]);
        assert_eq!(request2["transcript"], "fresh");
        assert_eq!(receive_json(&mut second).await["continue"], false);
        for value in [
            json!({"type":"chunk","context_id":request2["context_id"],"data":"AQACAA=="}),
            json!({"type":"done","context_id":request2["context_id"]}),
        ] {
            second
                .send(Message::Text(value.to_string().into()))
                .await
                .unwrap();
        }
        let _ = second.next().await;
    });
    let mut worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    next(&mut worker).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("warmup"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    while !matches!(next(&mut worker).await.frame.data, FrameData::Error(_)) {}
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
            Frame::new(FrameData::LlmText(TextFrame::llm("stale"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    started_rx.await.unwrap();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    while !matches!(next(&mut worker).await.frame.data, FrameData::Interruption) {}
    tokio::time::timeout(Duration::from_secs(2), cancelled_rx)
        .await
        .unwrap()
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("fresh"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::end(), Direction::Downstream)
        .await
        .unwrap();
    let mut audio = vec![];
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
    {
        if let FrameData::TtsAudio(chunk) = frame.frame.data {
            audio.extend(chunk.audio);
        }
    }
    assert_eq!(audio, vec![1, 0, 2, 0]);
    let report = worker.join().await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert!(report.errors[0].error.contains("completed with no audio"));
    server.await.unwrap();
}

struct DetailedStart;
#[async_trait]
impl FrameProcessor for DetailedStart {
    fn name(&self) -> &str {
        "DetailedStart"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::Start(configuration) = &frame.data {
            frame.data = FrameData::StartDetailed(DetailedStartFrame {
                configuration: configuration.clone(),
                enable_tracing: true,
                tracing_context: Some(TracingContext {
                    conversation_id: Some("speech-conversation".into()),
                    ..Default::default()
                }),
            });
        }
        ctx.push(frame, direction).await
    }
}

fn graceful_terminals() -> [Frame; 2] {
    [
        Frame::new(FrameData::EndDetailed(ReasonFrame {
            reason: Some(json!({"kind": "completed", "turn": 7})),
        })),
        Frame::new(FrameData::Stop),
    ]
}

#[tokio::test]
#[allow(clippy::result_large_err)] // WebSocket handshake callback uses the library's response type.
async fn detailed_start_and_graceful_terminals_preserve_stt_settings_and_final_transcript() {
    for terminal in graceful_terminals() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = DeepgramConfig::new("test-key");
        config.endpoint = format!("ws://{}/v1/listen", listener.local_addr().unwrap());
        config.sample_rate = 8000;
        config.channels = 2;
        config.language = Some("es".into());
        let server = tokio::spawn(async move {
            let mut socket = tokio_tungstenite::accept_hdr_async(
                listener.accept().await.unwrap().0,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    let url = url::Url::parse(&format!("ws://localhost{}", request.uri())).unwrap();
                    let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
                    assert_eq!(query["sample_rate"], "8000");
                    assert_eq!(query["channels"], "2");
                    assert_eq!(query["language"], "es");
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let message = socket.next().await.unwrap().unwrap();
            assert!(matches!(message, Message::Binary(bytes) if bytes.len() == 640));
            assert_eq!(
                receive_json(&mut socket).await,
                json!({"type":"CloseStream"})
            );
            socket.send(Message::Text(json!({"type":"Results","is_final":true,"speech_final":true,"channel":{"alternatives":[{"transcript":"hola","languages":["es"]}]}}).to_string().into())).await.unwrap();
            socket.close(None).await.unwrap();
        });
        let processor = Arc::new(
            DeepgramSttProcessor::new(DeepgramClient::new(config).unwrap())
                .with_user_id("speaker-es")
                .with_audio_passthrough(false),
        );
        let mut pipeline_config = PipelineConfig::default();
        pipeline_config.start.audio_in_sample_rate = 8000;
        pipeline_config.start.enable_usage_metrics = true;
        pipeline_config
            .start_metadata
            .insert("session".into(), "speech".into());
        let mut worker = Pipeline::new(vec![Arc::new(DetailedStart), processor.clone()])
            .start(pipeline_config)
            .await
            .unwrap();
        let start = next(&mut worker).await.frame;
        assert_eq!(start.metadata["session"], "speech");
        assert!(matches!(&start.data, FrameData::StartDetailed(s)
            if s.configuration.audio_in_sample_rate == 8000
                && s.configuration.enable_usage_metrics && s.enable_tracing
                && s.tracing_context.as_ref().unwrap().conversation_id.as_deref() == Some("speech-conversation")));
        worker
            .sender()
            .send(
                Frame::new(FrameData::InputAudio(
                    AudioRawFrame::new(vec![0; 640], 8000, 2).unwrap(),
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        worker
            .sender()
            .send(terminal.clone(), Direction::Downstream)
            .await
            .unwrap();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            report.state,
            if matches!(terminal.data, FrameData::Stop) {
                WorkerState::Stopped
            } else {
                WorkerState::Ended
            }
        );
        assert_eq!(frames.last().unwrap().frame, terminal);
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::InputAudio(_))));
        assert!(frames.iter().any(|f| matches!(&f.frame.data, FrameData::Transcription(t)
            if t.text == "hola" && t.user_id == "speaker-es" && t.language.as_deref() == Some("es"))));
        assert!(frames.iter().any(|f| matches!(&f.frame.data, FrameData::Metrics(metrics)
            if metrics.iter().any(|m| matches!(m, Metric::SttUsage { audio_seconds, .. } if (*audio_seconds - 0.02).abs() < 1e-9)))));
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        processor.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn detailed_start_and_graceful_terminals_drain_unfinished_tts_and_preserve_settings() {
    for terminal in graceful_terminals() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = CartesiaConfig::new("test-key", "zh-voice");
        config.endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
        config.sample_rate = 48000;
        config.language = Some("zh".into());
        config.generation_config = Some(json!({"speed": 0.9}));
        let server = tokio::spawn(async move {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let request = receive_json(&mut socket).await;
            assert_eq!(request["transcript"], "你好啊");
            assert_eq!(request["language"], "zh");
            assert_eq!(request["voice"]["id"], "zh-voice");
            assert_eq!(request["output_format"]["sample_rate"], 48000);
            assert_eq!(request["generation_config"], json!({"speed": 0.9}));
            let finish = receive_json(&mut socket).await;
            assert_eq!(finish["context_id"], request["context_id"]);
            assert_eq!(finish["continue"], false);
            for message in [
                json!({"type":"chunk","context_id":request["context_id"],"data":"AQACAA=="}),
                json!({"type":"timestamps","context_id":request["context_id"],"word_timestamps":{"words":["你好啊"],"start":[0.25],"end":[0.75]}}),
                json!({"type":"done","context_id":request["context_id"]}),
            ] {
                socket
                    .send(Message::Text(message.to_string().into()))
                    .await
                    .unwrap();
            }
            let _ = socket.next().await;
        });
        let processor = Arc::new(CartesiaTtsProcessor::new(
            CartesiaClient::new(config).unwrap(),
        ));
        let mut pipeline_config = PipelineConfig::default();
        pipeline_config.start.audio_out_sample_rate = 48000;
        pipeline_config.start.enable_metrics = true;
        pipeline_config.start.enable_usage_metrics = true;
        let mut worker = Pipeline::new(vec![Arc::new(DetailedStart), processor.clone()])
            .start(pipeline_config)
            .await
            .unwrap();
        assert!(
            matches!(next(&mut worker).await.frame.data, FrameData::StartDetailed(s)
            if s.configuration.audio_out_sample_rate == 48000 && s.configuration.enable_usage_metrics && s.enable_tracing)
        );
        let mut text = TextFrame::llm("你好啊");
        text.append_to_context = false;
        let source = Frame::new(FrameData::LlmText(text));
        worker
            .sender()
            .send(source.clone(), Direction::Downstream)
            .await
            .unwrap();
        worker
            .sender()
            .send(terminal.clone(), Direction::Downstream)
            .await
            .unwrap();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            report.state,
            if matches!(terminal.data, FrameData::Stop) {
                WorkerState::Stopped
            } else {
                WorkerState::Ended
            }
        );
        assert_eq!(frames.last().unwrap().frame, terminal);
        let started = frames.iter().position(|f| matches!(&f.frame.data, FrameData::TtsStarted(start) if !start.append_to_context)).unwrap();
        let audio = frames.iter().position(|f| matches!(&f.frame.data, FrameData::TtsAudio(audio) if audio.audio == [1, 0, 2, 0] && audio.sample_rate == 48000)).unwrap();
        let stopped = frames
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
            .unwrap();
        assert!(started < audio && audio < stopped && stopped < frames.len() - 1);
        let word = frames
            .iter()
            .find(|f| matches!(f.frame.data, FrameData::TtsText(_)))
            .unwrap();
        let baseline = frames
            .iter()
            .find(|f| matches!(f.frame.data, FrameData::AggregatedText(_)))
            .unwrap()
            .frame
            .pts
            .unwrap();
        assert_eq!(word.frame.pts, Some(baseline + 250_000_000));
        assert_eq!(word.frame.metadata["audio_offset_seconds"], json!(0.25));
        assert_eq!(word.frame.metadata["source_frame_id"], source.id);
        assert!(matches!(&word.frame.data, FrameData::TtsText(t)
            if t.text.text == "你好啊" && !t.text.append_to_context && t.text.includes_inter_frame_spaces));
        assert!(frames
            .iter()
            .any(|f| matches!(&f.frame.data, FrameData::Metrics(metrics)
            if metrics.iter().any(|m| matches!(m, Metric::TtsUsage { characters: 3, .. })))));
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        processor.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn detailed_cancel_closes_stt_without_graceful_drain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramConfig::new("test-key");
    config.endpoint = format!("ws://{}/v1/listen", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(matches!(
            socket.next().await,
            Some(Ok(Message::Close(_))) | None
        ));
    });
    let mut worker = Pipeline::new(vec![Arc::new(DeepgramSttProcessor::new(
        DeepgramClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    next(&mut worker).await;
    let cancel = Frame::new(FrameData::CancelDetailed(ReasonFrame {
        reason: Some(json!({"kind": "disconnected"})),
    }));
    worker
        .sender()
        .send(cancel.clone(), Direction::Downstream)
        .await
        .unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(frames.last().unwrap().frame, cancel);
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn detailed_cancel_cancels_active_tts_and_suppresses_late_provider_audio() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("test-key", "voice");
    config.endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
    let (started_tx, started_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = receive_json(&mut socket).await;
        started_tx.send(()).unwrap();
        assert_eq!(
            receive_json(&mut socket).await,
            json!({"context_id":request["context_id"],"cancel":true})
        );
        let _ = socket
            .send(Message::Text(
                json!({"type":"chunk","context_id":request["context_id"],"data":"//8="})
                    .to_string()
                    .into(),
            ))
            .await;
    });
    let mut worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    next(&mut worker).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmText(TextFrame::llm("stale"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), started_rx)
        .await
        .unwrap()
        .unwrap();
    let cancel = Frame::new(FrameData::CancelDetailed(ReasonFrame {
        reason: Some(json!({"kind": "disconnected"})),
    }));
    worker
        .sender()
        .send(cancel.clone(), Direction::Downstream)
        .await
        .unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsAudio(_))));
    assert_eq!(frames.last().unwrap().frame, cancel);
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn detailed_vad_stop_requests_stt_finalization_and_keeps_its_payload() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramConfig::new("test-key");
    config.endpoint = format!("ws://{}/v1/listen", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(receive_json(&mut socket).await, json!({"type":"Finalize"}));
        socket.send(Message::Text(json!({"type":"Results","is_final":true,"from_finalize":true,"channel":{"alternatives":[{"transcript":"final words"}]}}).to_string().into())).await.unwrap();
        assert_eq!(
            receive_json(&mut socket).await,
            json!({"type":"CloseStream"})
        );
        socket.close(None).await.unwrap();
    });
    let mut worker = Pipeline::new(vec![Arc::new(DeepgramSttProcessor::new(
        DeepgramClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    next(&mut worker).await;
    let stopped = Frame::new(FrameData::VadUserStoppedSpeakingDetailed(
        VadUserStoppedSpeakingFrame {
            stop_secs: 0.12,
            timestamp: 17.0,
        },
    ));
    worker
        .sender()
        .send(stopped.clone(), Direction::Upstream)
        .await
        .unwrap();
    let mut forwarded = false;
    let mut final_transcript = false;
    while !forwarded || !final_transcript {
        let next = next(&mut worker).await;
        match next.frame.data {
            FrameData::VadUserStoppedSpeakingDetailed(_) => {
                assert_eq!(next.frame, stopped);
                assert_eq!(next.direction, Direction::Upstream);
                forwarded = true;
            }
            FrameData::Transcription(t) => {
                assert_eq!(t.text, "final words");
                assert!(t.finalized);
                final_transcript = true;
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
