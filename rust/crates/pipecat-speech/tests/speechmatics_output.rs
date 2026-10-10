#![cfg(feature = "speechmatics")]
//! Source mapping: services/tts_service.py::_synthesize_text separates display,
//! original and prepared text; serialized completion follows its audio context.
//! Native HTTP completion is deliberately Unreported, never a playback receipt.
//! All provider requests use real loopback HTTP and synthetic PCM, not live APIs.
#[allow(dead_code)]
mod support;
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_speech::{speechmatics::*, *};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

fn processor(config: SpeechConfig) -> Arc<HttpTtsProcessor> {
    Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Speechmatics {
                client: SpeechmaticsHttpClient::new(config).unwrap(),
                request: SpeechmaticsSynthesisRequest {
                    sample_rate: 24000,
                    ..SpeechmaticsSynthesisRequest::new("")
                },
            },
            16,
        )
        .unwrap(),
    )
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn finish(worker: PipelineWorker) -> (Vec<Frame>, WorkerReport) {
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
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
fn assert_no_spoken(frames: &[Frame]) {
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::TtsText(_) | FrameData::AggregatedTextProgress(_)
    )));
    assert!(!frames
        .iter()
        .any(|f| f.metadata.get("spoken") == Some(&json!(true))));
}

// Record actual OutputTransport submissions and explicitly decline playback
// acknowledgement. No synthetic Played receipt enters the history path.
#[derive(Default)]
struct RecordingDiscard(Mutex<Vec<Frame>>);
#[async_trait]
impl OutputBackend for RecordingDiscard {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.0.lock().unwrap().push(frame.clone());
        Ok(PlaybackReceipt::Discarded)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
struct NeverCommit;
impl SpokenHistorySink for NeverCommit {
    fn commit(&self, _: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        panic!("HTTP completion is not evidence of spoken text")
    }
}

#[tokio::test]
async fn real_output_path_preserves_diagnostics_but_never_commits_spoken_history() {
    let pcm = [1, 0].repeat(480);
    let (config, request, server) = support::server(
        "200 OK",
        "application/octet-stream",
        vec![pcm.clone()],
        false,
        Duration::ZERO,
    )
    .await;
    let recording = Arc::new(RecordingDiscard::default());
    let history =
        SpokenHistory::new(SpokenHistoryConfig::default(), Arc::new(NeverCommit)).unwrap();
    let source = Arc::new(history.source_boundary("HTTP source").unwrap());
    let output = Arc::new(
        OutputTransport::new(
            "HTTP output",
            OutputTransportConfig {
                auto_silence: false,
                chunk_duration: Duration::from_millis(20),
                end_silence: Duration::ZERO,
                ..Default::default()
            },
            recording.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let worker = Pipeline::new(vec![source, processor(config), output])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let start = Frame::new(FrameData::LlmResponseStart);
    let start_id = start.id;
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    for frame in [
        start,
        Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new(
            "unreported",
        ))),
        end,
    ] {
        send(&worker, frame).await;
    }
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(request.await.unwrap().json()["text"], "unreported");
    let sent: Vec<u8> = recording
        .0
        .lock()
        .unwrap()
        .iter()
        .flat_map(|f| match &f.data {
            FrameData::TtsAudio(a) | FrameData::OutputAudio(a) => a.audio.clone(),
            _ => panic!("not PCM"),
        })
        .collect();
    assert_eq!(sent, pcm);
    let anchor = frames
        .iter()
        .find(|f| matches!(f.data, FrameData::AggregatedText(_)))
        .unwrap();
    assert!(frames.iter().any(|f| matches!(&f.data, FrameData::TtsAlignmentDiagnostic(d) if d.segment_id == Some(anchor.id) && d.synthesis_outcome == TtsSynthesisOutcome::Succeeded && d.evidence == TtsAlignmentEvidence::Unreported)));
    for id in [start_id, end_id] {
        assert_eq!(frames.iter().filter(|f| f.id == id).count(), 1);
    }
    assert_no_spoken(&frames);
    assert_eq!(history.status().committed_contexts, 0);
    server.await.unwrap();
}

#[derive(Default)]
struct BlockedOutput {
    entered: tokio::sync::Notify,
    active: std::sync::atomic::AtomicUsize,
}
struct ActiveWrite<'a>(&'a std::sync::atomic::AtomicUsize);
impl Drop for ActiveWrite<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[async_trait]
impl OutputBackend for BlockedOutput {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.active
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _active = ActiveWrite(&self.active);
        self.entered.notify_one();
        std::future::pending().await
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
#[tokio::test]
async fn blocked_output_cancel_and_end_deadline_settle_owned_writes() {
    for cancel in [true, false] {
        let (config, _, server) = support::server(
            "200 OK",
            "audio/pcm",
            vec![[1, 0].repeat(4800)],
            false,
            Duration::ZERO,
        )
        .await;
        let backend = Arc::new(BlockedOutput::default());
        let output = Arc::new(
            OutputTransport::new(
                "bounded output",
                OutputTransportConfig {
                    auto_silence: false,
                    chunk_duration: Duration::from_millis(20),
                    queue_capacity: 2,
                    queue_byte_capacity: 1920,
                    write_timeout: Duration::from_millis(100),
                    end_silence: Duration::ZERO,
                    ..Default::default()
                },
                backend.clone(),
                None,
                BTreeMap::new(),
            )
            .unwrap(),
        );
        let worker = Pipeline::new(vec![processor(config), output])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        send(
            &worker,
            Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("bounded"))),
        )
        .await;
        tokio::time::timeout(Duration::from_secs(2), backend.entered.notified())
            .await
            .unwrap();
        if cancel {
            worker.cancel("blocked output").await.unwrap();
        } else {
            worker.end().await.unwrap();
        }
        let (frames, _report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(backend.active.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(!frames.iter().any(|frame| matches!(
            frame.frame.data,
            FrameData::TtsText(_) | FrameData::AggregatedTextProgress(_)
        )));
        server.await.unwrap();
    }
}
