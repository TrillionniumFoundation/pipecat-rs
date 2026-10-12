//! HTTP provider generation retirement never replays already accepted text.
#[allow(dead_code)]
mod support;
use pipecat_core::*;
use pipecat_speech::{cartesia::*, *};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::test]
async fn coalesced_http_retirement_drops_inflight_audio_and_queued_utterance() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("test-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut old, _) = listener.accept().await.unwrap();
        assert_eq!(
            support::read_request(&mut old).await.json()["transcript"],
            "old"
        );
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let _ = old.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\x01\x02").await;
        drop(old);
        let (mut fresh, _) = listener.accept().await.unwrap();
        assert_eq!(
            support::read_request(&mut fresh).await.json()["transcript"],
            "fresh"
        );
        fresh.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\x03\x04").await.unwrap();
    });
    let processor = Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Cartesia {
                client: CartesiaHttpClient::new(config).unwrap(),
                request: CartesiaSynthesisRequest::new("", "voice"),
            },
            8,
        )
        .unwrap(),
    );
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let old_start = Frame::new(FrameData::LlmResponseStart);
    let old_start_id = old_start.id;
    let old_end = Frame::new(FrameData::LlmResponseEnd);
    let old_end_id = old_end.id;
    let mut text = TextFrame::new("accepted queue marker");
    text.skip_tts = Some(true);
    let marker = Frame::new(FrameData::Text(text));
    let marker_id = marker.id;
    for frame in [
        old_start,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("old"))),
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("must not replay"))),
        old_end,
        marker,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    let mut frames = vec![];
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        let found = frame.frame.id == marker_id;
        frames.push(frame);
        if found {
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(3), ready_rx)
        .await
        .unwrap()
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    release_tx.send(()).unwrap();
    let fresh_start = Frame::new(FrameData::LlmResponseStart);
    let fresh_start_id = fresh_start.id;
    let fresh_end = Frame::new(FrameData::LlmResponseEnd);
    let fresh_end_id = fresh_end.id;
    let flush = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        origin: Some("pipecat.flows".into()),
        ..Default::default()
    }));
    let flush_id = flush.id;
    for frame in [
        flush,
        fresh_start,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("fresh"))),
        fresh_end,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    worker.end().await.unwrap();
    let (tail, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    frames.extend(tail);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(health.is_usable());
    let flushes: Vec<_> = frames.iter().filter(|f| f.frame.id == flush_id).collect();
    assert_eq!(flushes.len(), 1);
    assert!(matches!(&flushes[0].frame.data, FrameData::FlushDetailed(f) if f.returning));
    for id in [old_start_id, old_end_id, fresh_start_id, fresh_end_id] {
        assert_eq!(frames.iter().filter(|f| f.frame.id == id).count(), 1);
    }
    assert_eq!(
        frames
            .iter()
            .find(|f| f.frame.id == old_end_id)
            .unwrap()
            .frame
            .metadata
            .get("failed"),
        Some(&json!(true))
    );
    assert!(!frames
        .iter()
        .find(|f| f.frame.id == fresh_end_id)
        .unwrap()
        .frame
        .metadata
        .contains_key("failed"));
    let stops: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
        .collect();
    assert_eq!(stops.len(), 2);
    assert_eq!(stops[0].frame.metadata.get("failed"), Some(&json!(true)));
    assert!(!stops[1].frame.metadata.contains_key("failed"));
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAudio(a) => Some(a.audio.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(audio, [vec![3, 4]]);
    let diagnostics: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAlignmentDiagnostic(d) => Some(d),
            _ => None,
        })
        .collect();
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].original_text.as_deref(), Some("fresh"));
    assert_eq!(diagnostics[0].evidence, TtsAlignmentEvidence::Unreported);
    assert_eq!(
        diagnostics[0].synthesis_outcome,
        TtsSynthesisOutcome::Succeeded
    );
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn transient_http_and_transport_failures_keep_provider_usable_for_later_work() {
    for (status, category) in [
        (Some("429 Too Many Requests"), ErrorCategory::RateLimit),
        (Some("503 Service Unavailable"), ErrorCategory::Server),
        (None, ErrorCategory::Connectivity),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::cartesia("test-key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut failed, _) = listener.accept().await.unwrap();
            assert_eq!(
                support::read_request(&mut failed).await.json()["transcript"],
                "fails"
            );
            if let Some(status) = status {
                failed.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}").as_bytes()).await.unwrap();
            }
            drop(failed);
            let (mut fresh, _) = listener.accept().await.unwrap();
            assert_eq!(
                support::read_request(&mut fresh).await.json()["transcript"],
                "fresh"
            );
            fresh.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\x03\x04").await.unwrap();
        });
        let processor = Arc::new(
            HttpTtsProcessor::new(
                HttpTtsBackend::Cartesia {
                    client: CartesiaHttpClient::new(config).unwrap(),
                    request: CartesiaSynthesisRequest::new("", "voice"),
                },
                8,
            )
            .unwrap(),
        );
        let mut worker = Pipeline::new(vec![processor])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let health = worker.processor_health(0).unwrap();
        let end = Frame::new(FrameData::LlmResponseEnd);
        let end_id = end.id;
        for frame in [
            Frame::new(FrameData::LlmResponseStart),
            Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("fails"))),
            end,
        ] {
            worker
                .sender()
                .send(frame, Direction::Downstream)
                .await
                .unwrap();
        }
        let mut frames = vec![];
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
                .await
                .unwrap()
                .unwrap();
            let found = frame.frame.id == end_id;
            frames.push(frame);
            if found {
                break;
            }
        }
        assert!(health.is_usable(), "{status:?} must be transient");
        assert_eq!(health.snapshot().revision, 0);
        assert_eq!(
            frames.last().unwrap().frame.metadata.get("failed"),
            Some(&json!(true))
        );
        for frame in [
            Frame::new(FrameData::LlmResponseStart),
            Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("fresh"))),
            Frame::new(FrameData::LlmResponseEnd),
        ] {
            worker
                .sender()
                .send(frame, Direction::Downstream)
                .await
                .unwrap();
        }
        worker.end().await.unwrap();
        let (tail, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
            .await
            .unwrap()
            .unwrap();
        frames.extend(tail);
        assert_eq!(report.errors.len(), 2, "{status:?}: {:?}", report.errors);
        assert!(report.errors[1].error.contains("completed with no audio"));
        assert_eq!(report.errors[0].category, category);
        let audio: Vec<_> = frames
            .iter()
            .filter_map(|f| match &f.frame.data {
                FrameData::TtsAudio(a) => Some(a.audio.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(audio, [vec![3, 4]]);
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn invalid_settings_preserve_health_category_and_last_valid_http_config() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("test-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let (verify_tx, verify_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = support::read_request(&mut socket).await.json();
        assert_eq!(request["transcript"], "old configuration works");
        assert_eq!(request["model_id"], "old-model");
        assert_eq!(request["voice"]["id"], "old-voice");
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\x03\x04").await.unwrap();
        drop(socket);
        verify_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err(),
            "invalid or ineffective update restarted an unusable provider"
        );
    });
    let mut request = CartesiaSynthesisRequest::new("", "old-voice");
    request.model = "old-model".into();
    let processor = Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Cartesia {
                client: CartesiaHttpClient::new(config).unwrap(),
                request,
            },
            8,
        )
        .unwrap(),
    );
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    let invalid_typed = || {
        Frame::new(FrameData::TtsUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame {
                delta: Some(TtsSettings {
                    base: ServiceSettings {
                        model: Setting::Value("must-not-apply".into()),
                        ..Default::default()
                    },
                    voice: Setting::Null,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )))
    };
    let invalid_mapping = Frame::new(FrameData::TtsUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            settings: [("model".into(), json!(42))].into(),
            ..Default::default()
        },
    )));
    let marker = Frame::text("settings proposals processed");
    let marker_id = marker.id;
    for frame in [invalid_typed(), invalid_mapping, marker] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    let mut frames = vec![];
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        let found = frame.frame.id == marker_id;
        frames.push(frame);
        if found {
            break;
        }
    }
    assert!(health.is_usable());
    assert_eq!(health.snapshot().revision, 0);
    assert_eq!(frames.iter().filter(|f| matches!(&f.frame.data, FrameData::Error(e) if e.category == ErrorCategory::InvalidRequest)).count(), 2);
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    for frame in [
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new(
            "old configuration works",
        ))),
        end,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        let found = frame.frame.id == end_id;
        frames.push(frame);
        if found {
            break;
        }
    }
    assert!(!frames.last().unwrap().frame.metadata.contains_key("failed"));
    assert!(health.is_usable());
    health.set_usable(false);
    let no_op = Frame::new(FrameData::TtsUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            delta: Some(TtsSettings {
                voice: Setting::Value("old-voice".into()),
                ..Default::default()
            }),
            ..Default::default()
        },
    )));
    let empty = Frame::new(FrameData::TtsUpdateSettings(Box::default()));
    let marker = Frame::text("unusable settings proposals processed");
    let marker_id = marker.id;
    for frame in [
        invalid_typed(),
        no_op,
        empty,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("must stay skipped"))),
        marker,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        let found = frame.frame.id == marker_id;
        frames.push(frame);
        if found {
            break;
        }
    }
    assert!(!health.is_usable());
    assert_eq!(health.snapshot().revision, 1);
    verify_tx.send(()).unwrap();
    worker.end().await.unwrap();
    let (tail, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    frames.extend(tail);
    assert_eq!(report.errors.len(), 3, "{:?}", report.errors);
    assert!(report
        .errors
        .iter()
        .all(|e| e.category == ErrorCategory::InvalidRequest));
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAudio(a) => Some(a.audio.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(audio, [vec![3, 4]]);
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
