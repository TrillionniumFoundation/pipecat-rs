//! Independent bounds checks for the no-audio prepared-input path.
#[allow(dead_code)]
mod support;
use pipecat_core::*;
use pipecat_speech::{cartesia::*, *};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::test]
async fn skipped_prepared_inputs_cannot_bypass_alignment_admission_limits() {
    let limits = pipecat_text::TtsAlignmentLimits::default();
    for oversized_original in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::cartesia("loopback");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let processor = Arc::new(
            HttpTtsProcessor::new(
                HttpTtsBackend::Cartesia {
                    client: CartesiaHttpClient::new(config).unwrap(),
                    request: CartesiaSynthesisRequest::new("", "voice"),
                },
                4,
            )
            .unwrap(),
        );
        let worker = Pipeline::new(vec![processor])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let mut input = TtsTextInputFrame::new("skipped");
        input.skip_tts = true;
        if oversized_original {
            input.original_text =
                "x".repeat(limits.sequencer.sequencer.alignment.max_text_bytes + 1);
        } else {
            input.aggregated_by = "x".repeat(129);
        }
        for data in [
            FrameData::LlmResponseStart,
            FrameData::TtsTextInput(input),
            FrameData::LlmResponseEnd,
        ] {
            worker
                .sender()
                .send(Frame::new(data), Direction::Downstream)
                .await
                .unwrap();
        }
        worker.sender().end().await.unwrap();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(
            !report.errors.is_empty(),
            "skipped provenance must remain bounded; oversized_original={oversized_original}"
        );
        assert!(!frames.iter().any(|f| matches!(
            f.frame.data,
            FrameData::AggregatedText(_)
                | FrameData::TtsAudio(_)
                | FrameData::TtsText(_)
                | FrameData::TtsAlignmentDiagnostic(_)
        )));
        assert!(frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::LlmResponseEnd)
                && f.frame.metadata.get("failed") == Some(&json!(true))));
        assert!(
            tokio::time::timeout(Duration::from_millis(25), listener.accept())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn repeated_source_frame_still_opens_distinct_synthesis_contexts() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("loopback");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(
                support::read_request(&mut socket).await.json()["transcript"],
                "repeat"
            );
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 4\r\nConnection: close\r\n\r\n\x01\x00\x02\x00").await.unwrap();
        }
    });
    let processor = Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Cartesia {
                client: CartesiaHttpClient::new(config).unwrap(),
                request: CartesiaSynthesisRequest::new("", "voice"),
            },
            4,
        )
        .unwrap(),
    );
    let worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let frame = Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new("repeat")));
    let source_id = frame.id;
    for frame in [frame.clone(), frame] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let anchors: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::AggregatedText(text) => Some((&f.frame, text)),
            _ => None,
        })
        .collect();
    assert_eq!(anchors.len(), 2);
    assert_ne!(anchors[0].0.id, anchors[1].0.id);
    assert_ne!(
        anchors[0].1.context_id, anchors[1].1.context_id,
        "source frame identity is not synthesis context identity"
    );
    for (anchor, text) in anchors {
        assert_eq!(anchor.metadata["source_frame_id"], source_id);
        assert!(frames.iter().any(|f| matches!(&f.frame.data, FrameData::TtsAlignmentDiagnostic(d) if d.segment_id == Some(anchor.id) && Some(d.context_id.clone()) == text.context_id)));
    }
}
