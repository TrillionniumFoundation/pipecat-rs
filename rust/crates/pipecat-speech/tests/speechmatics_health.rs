#![cfg(feature = "speechmatics")]
//! HTTP provider generation retirement never replays already accepted text.
#[allow(dead_code)]
mod support;
use pipecat_core::*;
use pipecat_speech::{speechmatics::*, *};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::test]
async fn coalesced_http_retirement_drops_inflight_audio_and_queued_utterance() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("test-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut old, _) = listener.accept().await.unwrap();
        assert_eq!(support::read_request(&mut old).await.json()["text"], "old");
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let _ = old.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\x01\x02").await;
        drop(old);
        let (mut fresh, _) = listener.accept().await.unwrap();
        assert_eq!(
            support::read_request(&mut fresh).await.json()["text"],
            "fresh"
        );
        fresh.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\x03\x04").await.unwrap();
    });
    let processor = Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Speechmatics {
                client: SpeechmaticsHttpClient::new(config).unwrap(),
                request: SpeechmaticsSynthesisRequest {
                    sample_rate: 24000,
                    ..SpeechmaticsSynthesisRequest::new("")
                },
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
    let marker = Frame::text("accepted queue marker");
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
