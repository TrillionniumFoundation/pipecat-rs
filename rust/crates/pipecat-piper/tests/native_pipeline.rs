//! Explicit offline native qualification. This test never provisions model assets.
#![cfg(feature = "espeak-gpl")]
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_piper::{
    sha256_bytes, PiperConfig, PiperProcessorOptions, PiperService, PiperSettings,
    PiperTtsProcessor,
};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

struct SlowDiscard {
    pcm: Mutex<Vec<u8>>,
    calls: AtomicUsize,
}
#[async_trait]
impl OutputBackend for SlowDiscard {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        let audio = match &frame.data {
            FrameData::TtsAudio(audio)
            | FrameData::OutputAudio(audio)
            | FrameData::SpeechOutputAudio(audio) => audio,
            _ => return Err(ProcessorError::new("unexpected non-audio write")),
        };
        self.pcm.lock().unwrap().extend_from_slice(&audio.audio);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(PlaybackReceipt::Discarded)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
#[derive(Default)]
struct History(AtomicUsize);
impl SpokenHistorySink for History {
    fn commit(&self, _: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }
}
fn write_wav(path: &std::path::Path, rate: u32, pcm: &[u8]) {
    let bytes = u32::try_from(pcm.len()).unwrap();
    let mut wav = Vec::with_capacity(pcm.len() + 44);
    wav.extend(b"RIFF");
    wav.extend((bytes + 36).to_le_bytes());
    wav.extend(b"WAVEfmt ");
    wav.extend(16u32.to_le_bytes());
    wav.extend(1u16.to_le_bytes());
    wav.extend(1u16.to_le_bytes());
    wav.extend(rate.to_le_bytes());
    wav.extend((rate * 2).to_le_bytes());
    wav.extend(2u16.to_le_bytes());
    wav.extend(16u16.to_le_bytes());
    wav.extend(b"data");
    wav.extend(bytes.to_le_bytes());
    wav.extend(pcm);
    std::fs::write(path, &wav).unwrap();
    eprintln!("WAV bytes={} sha256={}", wav.len(), sha256_bytes(&wav));
}
#[tokio::test]
#[ignore = "requires explicit pinned local Piper/ORT/GPL eSpeak assets; no download"]
async fn genuine_native_long_utterance_crosses_default_capacity_slow_output_exactly() {
    let manifest =
        std::env::var_os("PIPECAT_PIPER_MANIFEST").expect("explicit local Piper manifest required");
    let config: PiperConfig = serde_json::from_slice(&std::fs::read(manifest).unwrap()).unwrap();
    let service = PiperService::load(config).await.unwrap();
    let sample_rate = service.sample_rate();
    let settings = PiperSettings {
        noise_scale: Some(0.0),
        noise_w: Some(0.0),
        ..Default::default()
    };
    let text = "The native pipeline keeps every audio sample while the recorder waits. ".repeat(6);
    let expected = service
        .submit(&text, "reference", settings.clone())
        .unwrap()
        .result()
        .await
        .unwrap()
        .pcm;
    assert!(
        expected.len() > 128 * 1024 * 2,
        "qualification must pressure default core/media queues"
    );
    assert!(expected
        .as_chunks::<2>()
        .0
        .iter()
        .any(|sample| i16::from_le_bytes([sample[0], sample[1]]).unsigned_abs() > 100));
    let source_chunks = expected.len().div_ceil(1024 * 2);
    let processor = Arc::new(
        PiperTtsProcessor::new(service, settings, PiperProcessorOptions::default()).unwrap(),
    );
    let recorder = Arc::new(SlowDiscard {
        pcm: Mutex::new(Vec::new()),
        calls: AtomicUsize::new(0),
    });
    let sink = Arc::new(History::default());
    let history = SpokenHistory::new(SpokenHistoryConfig::default(), sink.clone()).unwrap();
    let output = OutputTransport::new(
        "slow-discard",
        OutputTransportConfig {
            sample_rate: Some(sample_rate),
            auto_silence: false,
            end_silence: Duration::ZERO,
            ..Default::default()
        },
        recorder.clone(),
        None,
        BTreeMap::new(),
    )
    .unwrap()
    .with_spoken_history(history.clone())
    .unwrap();
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_out_sample_rate = sample_rate;
    pipeline_config.shutdown_timeout = Duration::from_secs(90);
    assert_eq!(pipeline_config.queue_capacity, 128);
    let worker = Pipeline::new(vec![
        Arc::new(history.source_boundary("source").unwrap()),
        processor,
        Arc::new(output),
    ])
    .start(pipeline_config)
    .await
    .unwrap();
    for data in [
        FrameData::LlmResponseStart,
        FrameData::TtsSpeak(TtsSpeakFrame::new(text)),
        FrameData::LlmResponseEnd,
    ] {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(100), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.dropped_frames, 0);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
    assert!(frames.iter().any(|f| matches!(&f.frame.data, FrameData::TtsAlignmentDiagnostic(d) if d.evidence == TtsAlignmentEvidence::Unreported && d.synthesis_outcome == TtsSynthesisOutcome::Succeeded)));
    assert_eq!(history.status().committed_contexts, 0);
    assert_eq!(sink.0.load(Ordering::SeqCst), 0);
    let actual = recorder.pcm.lock().unwrap();
    assert_eq!(
        &actual[..expected.len().min(actual.len())],
        &expected[..expected.len().min(actual.len())]
    );
    assert!(actual.len() >= expected.len());
    let chunk_bytes = sample_rate as usize * 40 / 1000 * 2;
    let padded_len = expected.len().next_multiple_of(chunk_bytes);
    assert_eq!(actual.len(), padded_len);
    assert!(actual[expected.len()..].iter().all(|byte| *byte == 0));
    eprintln!("native_bytes={} source_chunks={} native_sha256={} backend_bytes={} backend_chunks={} backend_sha256={} padding={} receipt=Discarded", expected.len(), source_chunks, sha256_bytes(&expected), actual.len(), recorder.calls.load(Ordering::SeqCst), sha256_bytes(&actual), actual.len() - expected.len());
    if let Some(path) = std::env::var_os("PIPECAT_PIPER_PIPELINE_WAV") {
        write_wav(std::path::Path::new(&path), sample_rate, &actual);
    }
}
