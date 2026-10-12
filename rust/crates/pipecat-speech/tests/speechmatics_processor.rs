#![cfg(feature = "speechmatics")]
//! Core runtime and HTTP ownership tests using complete synthetic utterances.
#[allow(dead_code)]
mod support;
use pipecat_core::*;
use pipecat_speech::{speechmatics::*, *};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

fn processor(config: SpeechConfig, limit: usize) -> Arc<HttpTtsProcessor> {
    Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Speechmatics {
                client: SpeechmaticsHttpClient::new(config).unwrap(),
                request: SpeechmaticsSynthesisRequest {
                    sample_rate: 24000,
                    ..SpeechmaticsSynthesisRequest::new("")
                },
            },
            limit,
        )
        .unwrap(),
    )
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn drain(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut frames = vec![];
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
        worker.join().await.unwrap();
        frames
    })
    .await
    .unwrap()
}
async fn reply(socket: &mut TcpStream, pcm: &[u8]) {
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", pcm.len()).as_bytes()).await.unwrap();
    socket.write_all(pcm).await.unwrap();
}
#[tokio::test]
async fn complete_utterances_are_sequential_and_end_waits_for_audio_and_metrics() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut texts = vec![];
        for pcm in [vec![1, 2], vec![3, 4]] {
            let (mut socket, _) = listener.accept().await.unwrap();
            texts.push(
                support::read_request(&mut socket).await.json()["text"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
            reply(&mut socket, &pcm).await;
        }
        texts
    });
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.enable_metrics = true;
    pipeline_config.start.enable_usage_metrics = true;
    let mut worker = Pipeline::new(vec![processor(config, 2)])
        .start(pipeline_config)
        .await
        .unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("first"))).await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame {
            text: "second".into(),
            append_to_context: false,
        }),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    let mut order = vec![];
    let mut pcm: Vec<u8> = vec![];
    let mut ttfb = 0;
    let mut characters = vec![];
    for output in &frames {
        match &output.frame.data {
            FrameData::TtsStarted(start) => {
                order.push("start");
                assert!(start
                    .context_id
                    .as_deref()
                    .unwrap()
                    .starts_with("http-tts-"));
            }
            FrameData::TtsAudio(audio) => {
                order.push("audio");
                pcm.extend(&audio.audio);
                assert!(output.frame.metadata.contains_key("source_frame_id"));
            }
            FrameData::TtsStopped(_) => order.push("stop"),
            FrameData::LlmResponseEnd => order.push("response-end"),
            FrameData::End(_) => order.push("end"),
            FrameData::Metrics(metrics) => {
                for metric in metrics {
                    match metric {
                        Metric::Ttfb { .. } => ttfb += 1,
                        Metric::TtsUsage {
                            characters: count, ..
                        } => characters.push(*count),
                        _ => {}
                    }
                }
            }
            FrameData::TtsText(_) => panic!("synthesis must not claim that text was spoken"),
            FrameData::Error(error) => panic!("{}", error.error),
            _ => {}
        }
    }
    assert_eq!(
        order,
        [
            "start",
            "audio",
            "stop",
            "start",
            "audio",
            "stop",
            "response-end",
            "end"
        ]
    );
    assert_eq!(pcm, [1, 2, 3, 4]);
    assert_eq!(ttfb, 2);
    assert_eq!(characters, [5, 6]);
    assert_eq!(server.await.unwrap(), ["first", "second"]);
}
#[tokio::test]
async fn provider_failure_is_upstream_nonfatal_and_response_end_is_marked_failed() {
    let (config, _, server) = support::server(
        "429 Too Many Requests",
        "application/json",
        vec![b"{\"message\":\"secret\"}".to_vec()],
        false,
        Duration::ZERO,
    )
    .await;
    let mut worker = Pipeline::new(vec![processor(config, 4)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("hello"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    let error = frames
        .iter()
        .find(|f| matches!(f.frame.data, FrameData::Error(_)))
        .unwrap();
    assert_eq!(error.direction, Direction::Upstream);
    if let FrameData::Error(error) = &error.frame.data {
        assert!(!error.fatal);
        assert_eq!(error.category, ErrorCategory::RateLimit);
        assert!(!error.error.contains("secret"));
    }
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsStarted(_)))
            .count(),
        1
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
            .count(),
        1
    );
    let end = frames
        .iter()
        .find(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
        .unwrap();
    assert_eq!(end.frame.metadata.get("failed"), Some(&true.into()));
    server.await.unwrap();
}
#[tokio::test]
async fn upstream_and_skip_tts_frames_pass_through_without_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let mut worker = Pipeline::new(vec![processor(config, 4)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("upstream"))),
            Direction::Upstream,
        )
        .await
        .unwrap();
    let mut text = TextFrame::new("skip");
    text.skip_tts = Some(true);
    send(
        &worker,
        FrameData::AggregatedText(AggregatedTextFrame {
            text,
            aggregated_by: "sentence".into(),
            context_id: None,
            raw_text: None,
            will_be_spoken: false,
        }),
    )
    .await;
    send(&worker, FrameData::LlmText(TextFrame::new("incremental"))).await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    assert!(frames
        .iter()
        .any(|f| f.direction == Direction::Upstream
            && matches!(f.frame.data, FrameData::TtsSpeak(_))));
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::AggregatedText(_))));
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::LlmText(_))));
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsStarted(_))));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn interruption_closes_active_http_discards_waiting_utterance_and_allows_new_turn() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let (captured, capture) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let text = support::read_request(&mut first).await.json()["text"].clone();
        captured.send(text).unwrap();
        let mut byte = [0];
        let count = tokio::time::timeout(Duration::from_secs(2), first.read(&mut byte))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count, 0);
        let (mut second, _) = listener.accept().await.unwrap();
        let text = support::read_request(&mut second).await.json()["text"].clone();
        reply(&mut second, &[9, 0]).await;
        text
    });
    let mut worker = Pipeline::new(vec![processor(config, 4)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("old"))).await;
    assert_eq!(capture.await.unwrap(), "old");
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("queued"))).await;
    let mut accepted = TextFrame::new("accepted");
    accepted.skip_tts = Some(true);
    send(&worker, FrameData::Text(accepted)).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::Text(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    send(&worker, FrameData::Interruption).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::Interruption) {
                break;
            }
        }
    })
    .await
    .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("new"))).await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| {
            if let FrameData::TtsAudio(audio) = &f.frame.data {
                Some(audio.audio.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(audio, [vec![9, 0]]);
    let diagnostics: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAlignmentDiagnostic(d) => Some(d),
            _ => None,
        })
        .collect();
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].original_text.as_deref(), Some("new"));
    assert_eq!(server.await.unwrap(), "new");
}
#[tokio::test]
async fn pending_utterances_are_bounded_and_error_is_recoverable() {
    let (config, capture, server) = support::server(
        "200 OK",
        "audio/pcm",
        vec![vec![0, 0]],
        false,
        Duration::from_millis(80),
    )
    .await;
    let mut worker = Pipeline::new(vec![processor(config, 1)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("first"))).await;
    capture.await.unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("overflow"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    assert!(frames
        .iter()
        .any(|f| matches!(&f.frame.data, FrameData::Error(e) if e.error.contains("limit"))));
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsAudio(_)))
            .count(),
        1
    );
    assert_eq!(
        frames
            .iter()
            .find(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
            .unwrap()
            .frame
            .metadata
            .get("failed"),
        Some(&true.into())
    );
    server.await.unwrap();
}
#[tokio::test]
async fn pipeline_rejects_mismatched_output_rate_at_setup() {
    let mut config = PipelineConfig::default();
    config.start.audio_out_sample_rate = 16000;
    let result = Pipeline::new(vec![processor(SpeechConfig::speechmatics("key"), 2)])
        .start(config)
        .await;
    let mut worker = result.unwrap();
    assert!(worker.join().await.is_err());
    assert!(worker
        .errors()
        .iter()
        .any(|error| error.fatal && error.category == ErrorCategory::InvalidRequest));
}

#[tokio::test]
async fn uninterruptible_flush_waits_for_synthesis_and_survives_interruption() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let (captured, capture) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        captured.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let mut worker = Pipeline::new(vec![processor(config, 2)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("hello"))).await;
    capture.await.unwrap();
    let event = LocalFlushEvent::default();
    let mut flush = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        event: Some(event.clone()),
        returning: false,
        origin: Some("test".into()),
    }));
    flush.interruptible = false;
    worker
        .sender()
        .send(flush, Direction::Downstream)
        .await
        .unwrap();
    let mut accepted = TextFrame::new("accepted");
    accepted.skip_tts = Some(true);
    send(&worker, FrameData::Text(accepted)).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::Text(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(!event.is_set());
    send(&worker, FrameData::Interruption).await;
    tokio::time::timeout(Duration::from_secs(2), event.wait())
        .await
        .unwrap();
    worker.end().await.unwrap();
    drain(&mut worker).await;
    server.await.unwrap();
}

#[tokio::test]
async fn permanent_http_failure_gates_requests_until_effective_settings_recover() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        assert_eq!(
            support::read_request(&mut first).await.json()["text"],
            "rejected"
        );
        first
            .write_all(
                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        let request = support::read_request(&mut second).await;
        assert_eq!(request.json()["text"], "fresh");
        assert_eq!(request.url().path(), "/generate/new-voice");
        reply(&mut second, &[1, 2]).await;
    });
    let processor = processor(config, 8);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("rejected"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(frame.frame.data, FrameData::TtsStopped(_)) {
            assert_eq!(frame.frame.metadata["failed"], true);
        }
        if matches!(frame.frame.data, FrameData::LlmResponseEnd) {
            assert_eq!(frame.frame.metadata["failed"], true);
            break;
        }
    }
    assert!(!health.is_usable());
    assert!(!processor.update_settings(&TtsSettings::default()).unwrap());
    assert!(processor
        .update_settings(&TtsSettings {
            voice: Setting::Null,
            ..Default::default()
        })
        .is_err());
    assert!(!health.is_usable());
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("discarded")),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!matches!(
            frame.frame.data,
            FrameData::TtsAudio(_) | FrameData::TtsStarted(_)
        ));
        if matches!(frame.frame.data, FrameData::LlmResponseEnd) {
            assert_eq!(frame.frame.metadata["failed"], true);
            break;
        }
    }
    // Exercise the frame-based recovery control while the service is unusable.
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            delta: Some(TtsSettings {
                voice: Setting::Value("new-voice".into()),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("fresh"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    assert!(health.is_usable());
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsAudio(_))));
    assert!(frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
        .all(|f| !f.frame.metadata.contains_key("failed")));
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn voice_and_retry_updates_are_atomic_and_only_affect_new_admissions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let (ready_tx, ready) = oneshot::channel();
    let (release_tx, release) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut requests = vec![];
        let (mut socket, _) = listener.accept().await.unwrap();
        requests.push(support::read_request(&mut socket).await);
        ready_tx.send(()).unwrap();
        release.await.unwrap();
        socket
            .write_all(b"HTTP/1.1 503 Busy\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        drop(socket);
        for status in [503, 503, 200] {
            let (mut socket, _) = listener.accept().await.unwrap();
            requests.push(support::read_request(&mut socket).await);
            socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\x01\x02").as_bytes()).await.unwrap();
        }
        requests
    });
    let processor = Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Speechmatics {
                client: SpeechmaticsHttpClient::new(config).unwrap(),
                request: SpeechmaticsSynthesisRequest {
                    sample_rate: 24000,
                    max_retries: 0,
                    ..SpeechmaticsSynthesisRequest::new("")
                },
            },
            8,
        )
        .unwrap(),
    );
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("old"))).await;
    ready.await.unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("queued"))).await;
    let mut accepted = TextFrame::new("admission marker");
    accepted.skip_tts = Some(true);
    send(&worker, FrameData::Text(accepted)).await;
    while !matches!(
        tokio::time::timeout(Duration::from_secs(2), worker.recv())
            .await
            .unwrap()
            .unwrap()
            .frame
            .data,
        FrameData::Text(_)
    ) {}
    let mut bad = TtsSettings {
        voice: Setting::Value("bad-voice".into()),
        ..Default::default()
    };
    bad.base
        .extra
        .insert("max_retries".into(), serde_json::json!(33));
    assert!(processor.update_settings(&bad).is_err());
    assert!(!processor
        .update_settings(&TtsSettings {
            voice: Setting::Value("sarah".into()),
            ..Default::default()
        })
        .unwrap());
    let mut update = std::collections::BTreeMap::new();
    update.insert("max_retries".into(), serde_json::json!(2));
    update.insert("voice".into(), serde_json::json!("new-voice"));
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: update,
            ..Default::default()
        })),
    )
    .await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("future"))).await;
    release_tx.send(()).unwrap();
    worker.end().await.unwrap();
    let frames = drain(&mut worker).await;
    let requests = server.await.unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests
            .iter()
            .map(|r| r.json()["text"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>(),
        ["old", "queued", "future", "future"]
    );
    assert_eq!(
        requests
            .iter()
            .map(|r| r.url().path().to_owned())
            .collect::<Vec<_>>(),
        [
            "/generate/sarah",
            "/generate/sarah",
            "/generate/new-voice",
            "/generate/new-voice"
        ]
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsAudio(_)))
            .count(),
        1
    );
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
}
#[tokio::test]
async fn cancel_and_owner_drop_close_blocked_http_without_late_output() {
    for drop_owner in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::speechmatics("key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let (ready_tx, ready) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            ready_tx.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        let mut worker = Pipeline::new(vec![processor(config, 4)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("old"))).await;
        ready.await.unwrap();
        send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("queued"))).await;
        if drop_owner {
            drop(worker);
        } else {
            worker.cancel("test").await.unwrap();
            let frames = drain(&mut worker).await;
            assert!(!frames.iter().any(|f| matches!(
                f.frame.data,
                FrameData::TtsAudio(_) | FrameData::TtsAlignmentDiagnostic(_)
            )));
        }
        server.await.unwrap();
    }
}
#[tokio::test]
async fn stop_drains_synthesis_before_terminal_frame() {
    let (config, _, server) = support::server(
        "200 OK",
        "audio/pcm",
        vec![vec![1, 2]],
        false,
        Duration::from_millis(25),
    )
    .await;
    let mut worker = Pipeline::new(vec![processor(config, 4)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("tail"))).await;
    send(&worker, FrameData::Stop).await;
    let frames = drain(&mut worker).await;
    let audio = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::TtsAudio(_)))
        .unwrap();
    let stop = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::Stop))
        .unwrap();
    assert!(audio < stop);
    server.await.unwrap();
}
#[tokio::test]
async fn empty_and_partial_pcm_have_failed_terminals_without_success_diagnostics() {
    for chunks in [vec![], vec![vec![1]], vec![vec![1, 2, 3]]] {
        let (config, _, server) =
            support::server("200 OK", "audio/pcm", chunks, false, Duration::ZERO).await;
        let mut pipeline = PipelineConfig::default();
        pipeline.start.enable_usage_metrics = true;
        let mut worker = Pipeline::new(vec![processor(config, 4)])
            .start(pipeline)
            .await
            .unwrap();
        send(&worker, FrameData::LlmResponseStart).await;
        send(
            &worker,
            FrameData::TtsSpeak(TtsSpeakFrame::new("not successful")),
        )
        .await;
        send(&worker, FrameData::LlmResponseEnd).await;
        worker.end().await.unwrap();
        let frames = drain(&mut worker).await;
        assert!(frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::Error(_))));
        for terminal in frames.iter().filter(|f| {
            matches!(
                f.frame.data,
                FrameData::TtsStopped(_) | FrameData::LlmResponseEnd
            )
        }) {
            assert_eq!(
                terminal.frame.metadata.get("failed"),
                Some(&serde_json::json!(true))
            );
        }
        assert!(!frames.iter().any(|f|matches!(&f.frame.data,FrameData::TtsAlignmentDiagnostic(d) if d.synthesis_outcome==TtsSynthesisOutcome::Succeeded)));
        assert!(!frames.iter().any(|f|matches!(&f.frame.data,FrameData::Metrics(metrics) if metrics.iter().any(|m|matches!(m,Metric::TtsUsage{..})))));
        server.await.unwrap();
    }
}
