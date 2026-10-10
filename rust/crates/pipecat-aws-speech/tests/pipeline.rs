#[allow(dead_code)]
mod support;
use futures_util::{SinkExt, StreamExt};
use pipecat_aws_speech::*;
use pipecat_core::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tokio_tungstenite::{accept_async, tungstenite::Message};
fn credentials() -> AwsCredentials {
    AwsCredentials::new("AKIDEXAMPLE", "SYNTHETICSECRET", None).unwrap()
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn drain(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut out = vec![];
        while let Some(frame) = worker.recv().await {
            out.push(frame);
        }
        let report = worker.join().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        out
    })
    .await
    .unwrap()
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn polly_server(
    count: usize,
) -> (PollyConfig, tokio::task::JoinHandle<Vec<serde_json::Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = PollyConfig::new(credentials(), "us-east-1");
    config.endpoint = format!("http://{}/v1/speech", listener.local_addr().unwrap());
    config.sample_rate = 16000;
    let server = tokio::spawn(async move {
        let mut bodies = vec![];
        for _ in 0..count {
            let (mut socket, _) = listener.accept().await.unwrap();
            bodies.push(support::read_request(&mut socket).await.json());
            tokio::time::sleep(Duration::from_millis(10)).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: audio/pcm\r\nContent-Length: 640\r\nConnection: close\r\n\r\n").await.unwrap();
            socket.write_all(&vec![0; 640]).await.unwrap();
        }
        bodies
    });
    (config, server)
}
fn pipeline_config() -> PipelineConfig {
    let mut c = PipelineConfig::default();
    c.start.audio_out_sample_rate = 16000;
    c.start.enable_metrics = true;
    c.start.enable_usage_metrics = true;
    c
}
#[tokio::test]
async fn polly_aggregates_sentences_preserves_source_destination_and_end_barrier() {
    let (config, server) = polly_server(1).await;
    let mut worker = Pipeline::new(vec![Arc::new(
        PollyTtsProcessor::new(PollyClient::new(config).unwrap(), 8).unwrap(),
    )])
    .start(pipeline_config())
    .await
    .unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    let mut text = TextFrame::llm("Hello ");
    text.append_to_context = false;
    text.includes_inter_frame_spaces = true;
    let mut first = Frame::new(FrameData::LlmText(text.clone()));
    let source = first.id;
    first.transport_destination = Some("speaker-a".into());
    worker
        .sender()
        .send(first, Direction::Downstream)
        .await
        .unwrap();
    text.text = "world.".into();
    let mut second = Frame::new(FrameData::LlmText(text));
    second.transport_destination = Some("speaker-a".into());
    worker
        .sender()
        .send(second, Direction::Downstream)
        .await
        .unwrap();
    send(&worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    let mut order = vec![];
    let mut audio = false;
    let mut usage = false;
    for output in frames {
        match output.frame.data {
            FrameData::TtsStarted(start) => {
                assert!(!start.append_to_context);
                order.push("start");
            }
            FrameData::TtsAudio(pcm) => {
                audio = true;
                assert_eq!(pcm.audio.len(), 640);
                assert_eq!(
                    output.frame.transport_destination.as_deref(),
                    Some("speaker-a")
                );
                assert_eq!(
                    output.frame.metadata.get("source_frame_id"),
                    Some(&json!(source))
                );
                order.push("audio");
            }
            FrameData::TtsStopped(_) => order.push("stop"),
            FrameData::LlmResponseEnd => order.push("response-end"),
            FrameData::End(_) => order.push("end"),
            FrameData::Metrics(metrics) => {
                usage |= metrics
                    .iter()
                    .any(|m| matches!(m, Metric::TtsUsage { characters: 12, .. }))
            }
            FrameData::TtsText(_) => panic!("synthesis is not playout acknowledgement"),
            _ => {}
        }
    }
    assert!(audio && usage);
    assert_eq!(order, ["start", "audio", "stop", "response-end", "end"]);
    let bodies = server.await.unwrap();
    assert!(bodies[0]["Text"].as_str().unwrap().contains("Hello world."));
}
#[tokio::test]
async fn polly_settings_snapshot_applies_atomically_to_next_utterance() {
    let (config, server) = polly_server(2).await;
    let processor = Arc::new(PollyTtsProcessor::new(PollyClient::new(config).unwrap(), 8).unwrap());
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(pipeline_config())
        .await
        .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("one"))).await;
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [
                ("voice".into(), json!("Matthew")),
                ("language".into(), json!("en-GB")),
                ("engine".into(), json!("standard")),
                ("pitch".into(), json!("high")),
            ]
            .into(),
            service: Some(ProcessorReference::Name("AWSPollyTTSService".into())),
            ..Default::default()
        })),
    )
    .await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("two"))).await;
    worker.end().await.unwrap();
    drain(&mut worker).await;
    let bodies = server.await.unwrap();
    assert_eq!(bodies[0]["VoiceId"], "Joanna");
    assert_eq!(bodies[1]["VoiceId"], "Matthew");
    assert!(bodies[1]["Text"].as_str().unwrap().contains("pitch='high'"));
    let mut bad = TtsSettings {
        voice: Setting::Value("Brian".into()),
        ..Default::default()
    };
    bad.base.extra.insert("unknown".into(), json!(true));
    assert!(processor.update_settings(&bad).is_err());
}
#[tokio::test]
async fn transcribe_pipeline_survives_interruption_uses_provider_segmentation_and_drains_end() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = TranscribeConfig::new(credentials(), "us-east-1");
    config.endpoint = format!(
        "ws://{}/stream-transcription-websocket",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let bytes = socket.next().await.unwrap().unwrap().into_data();
        assert_eq!(
            eventstream::decode(&bytes, 1024 * 1024)
                .unwrap()
                .payload
                .len(),
            640
        );
        let bytes = socket.next().await.unwrap().unwrap().into_data();
        assert!(eventstream::decode(&bytes, 1024 * 1024)
            .unwrap()
            .payload
            .is_empty());
        socket.send(Message::Binary(eventstream::encode(&[(":message-type","event"),(":event-type","TranscriptEvent"),(":content-type","application/json")],json!({"Transcript":{"Results":[{"ResultId":"final","IsPartial":false,"Alternatives":[{"Transcript":"done"}]}]}}).to_string().as_bytes(),1024*1024).unwrap().into())).await.unwrap();
        socket.close(None).await.unwrap();
    });
    let processor =
        TranscribeSttProcessor::new(TranscribeClient::new(config).unwrap()).with_user_id("speaker");
    assert_eq!(processor.name(), "AWSTranscribeSTTService");
    let mut worker = Pipeline::new(vec![Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    next(&mut worker).await;
    send(&worker, FrameData::Interruption).await;
    next(&mut worker).await;
    send(
        &worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![0; 640], 16000, 1).unwrap()),
    )
    .await;
    send(
        &worker,
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.02,
            timestamp: 1.0,
        }),
    )
    .await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    let mut transcript = false;
    let mut ended = false;
    for output in frames {
        match output.frame.data {
            FrameData::Transcription(t) => {
                assert_eq!(t.text, "done");
                assert_eq!(t.user_id, "speaker");
                assert!(t.finalized);
                assert!(!ended);
                transcript = true;
            }
            FrameData::End(_) => ended = true,
            _ => {}
        }
    }
    assert!(transcript && ended);
    server.await.unwrap();
}

#[tokio::test]
async fn transcribe_settings_rotate_connection_after_vad_stop_and_drain_old_final() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = TranscribeConfig::new(credentials(), "us-east-1");
    config.endpoint = format!(
        "ws://{}/stream-transcription-websocket",
        listener.local_addr().unwrap()
    );
    let (audio_seen, audio_ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let data = first.next().await.unwrap().unwrap().into_data();
        assert_eq!(
            eventstream::decode(&data, 1024 * 1024)
                .unwrap()
                .payload
                .len(),
            640
        );
        audio_seen.send(()).unwrap();
        let data = first.next().await.unwrap().unwrap().into_data();
        assert!(eventstream::decode(&data, 1024 * 1024)
            .unwrap()
            .payload
            .is_empty());
        first.send(Message::Binary(eventstream::encode(&[(":message-type","event"),(":event-type","TranscriptEvent"),(":content-type","application/json")],json!({"Transcript":{"Results":[{"IsPartial":false,"Alternatives":[{"Transcript":"old final"}]}]}}).to_string().as_bytes(),1024*1024).unwrap().into())).await.unwrap();
        first.close(None).await.unwrap();
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let data = second.next().await.unwrap().unwrap().into_data();
        assert_eq!(
            eventstream::decode(&data, 1024 * 1024)
                .unwrap()
                .payload
                .len(),
            640
        );
        let data = second.next().await.unwrap().unwrap().into_data();
        assert!(eventstream::decode(&data, 1024 * 1024)
            .unwrap()
            .payload
            .is_empty());
        second.close(None).await.unwrap();
    });
    let processor = Arc::new(TranscribeSttProcessor::new(
        TranscribeClient::new(config).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(
        &worker,
        FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        }),
    )
    .await;
    while !matches!(
        next(&mut worker).await.frame.data,
        FrameData::VadUserStartedSpeaking(_)
    ) {}
    send(
        &worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![0; 640], 16000, 1).unwrap()),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), audio_ready)
        .await
        .unwrap()
        .unwrap();
    send(
        &worker,
        FrameData::SttUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [
                ("language".into(), json!("fr")),
                ("partial_results_stability".into(), json!("low")),
            ]
            .into(),
            service: Some(ProcessorReference::Name("AWSTranscribeSTTService".into())),
            ..Default::default()
        })),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while processor.config_snapshot().unwrap().language != "fr" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    send(
        &worker,
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.02,
            timestamp: 1.0,
        }),
    )
    .await;
    send(
        &worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![1; 640], 16000, 1).unwrap()),
    )
    .await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    let final_old = frames
        .iter()
        .find_map(|f| {
            if let FrameData::Transcription(t) = &f.frame.data {
                Some(t)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(final_old.text, "old final");
    assert_eq!(final_old.language.as_deref(), Some("en-US"));
    assert_eq!(processor.config_snapshot().unwrap().language, "fr");
    assert_eq!(
        processor
            .config_snapshot()
            .unwrap()
            .partial_results_stability,
        PartialResultsStability::Low
    );
    server.await.unwrap();
}

#[tokio::test]
async fn transcribe_reconnects_without_replaying_already_submitted_audio() {
    use tokio::sync::oneshot;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = TranscribeConfig::new(credentials(), "us-east-1");
    config.endpoint = format!(
        "ws://{}/stream-transcription-websocket",
        listener.local_addr().unwrap()
    );
    let (ready, connected) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let data = first.next().await.unwrap().unwrap().into_data();
        assert!(eventstream::decode(&data, 1024 * 1024)
            .unwrap()
            .payload
            .iter()
            .all(|b| *b == 1));
        first.close(None).await.unwrap();
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        ready.send(()).unwrap();
        let data = second.next().await.unwrap().unwrap().into_data();
        assert!(eventstream::decode(&data, 1024 * 1024)
            .unwrap()
            .payload
            .iter()
            .all(|b| *b == 2));
        let data = second.next().await.unwrap().unwrap().into_data();
        assert!(eventstream::decode(&data, 1024 * 1024)
            .unwrap()
            .payload
            .is_empty());
        second.close(None).await.unwrap();
    });
    let mut worker = Pipeline::new(vec![Arc::new(
        TranscribeSttProcessor::new(TranscribeClient::new(config).unwrap()).with_reconnect_policy(
            ReconnectPolicy {
                max_attempts: 2,
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            },
        ),
    )])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(
        &worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![1; 640], 16000, 1).unwrap()),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), connected)
        .await
        .unwrap()
        .unwrap();
    send(
        &worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![2; 640], 16000, 1).unwrap()),
    )
    .await;
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while worker.recv().await.is_some() {}
        let report = worker.join().await.unwrap();
        assert_eq!(report.errors.len(), 1);
        assert_eq!(report.errors[0].category, ErrorCategory::Connectivity);
    })
    .await
    .unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn unrelated_targeted_settings_do_not_flush_polly_sentence() {
    let (config, server) = polly_server(1).await;
    let mut worker = Pipeline::new(vec![Arc::new(
        PollyTtsProcessor::new(PollyClient::new(config).unwrap(), 8).unwrap(),
    )])
    .start(pipeline_config())
    .await
    .unwrap();
    send(&worker, FrameData::LlmText(TextFrame::llm("Hello "))).await;
    let foreign = Frame::new(FrameData::TtsUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            service: Some(ProcessorReference::Name("AnotherTts".into())),
            settings: [("voice".into(), json!("Brian"))].into(),
            ..Default::default()
        },
    )));
    let foreign_id = foreign.id;
    worker
        .sender()
        .send(foreign, Direction::Downstream)
        .await
        .unwrap();
    send(&worker, FrameData::LlmText(TextFrame::llm("world."))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    assert!(frames.iter().any(|f| f.frame.id == foreign_id));
    let bodies = server.await.unwrap();
    assert_eq!(bodies.len(), 1);
    assert_eq!(bodies[0]["VoiceId"], "Joanna");
    assert!(bodies[0]["Text"].as_str().unwrap().contains("Hello world."));
}
