//! Explicit real-engine tests; provision local assets and run with --test-threads=1.
//! Timeouts are test-stall watchdogs, not native cancellation latency guarantees.
use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, Pipeline, PipelineConfig, VadSpeakingFrame,
};
use pipecat_moonshine::*;
use std::path::PathBuf;
use std::{sync::Arc, time::Duration};
fn config() -> MoonshineConfig {
    let root = PathBuf::from(
        std::env::var_os("PIPECAT_MOONSHINE_FIXTURES")
            .expect("explicit fixture directory required"),
    );
    let models = ["small-en-bundle.json", "tiny-en-bundle.json"]
        .into_iter()
        .map(|file| serde_json::from_slice(&std::fs::read(root.join(file)).unwrap()).unwrap())
        .collect();
    MoonshineConfig::new(NativeLibraries::from_wheel_directory(root), models)
}
fn audio() -> AudioRawFrame {
    let root = PathBuf::from(std::env::var_os("PIPECAT_MOONSHINE_FIXTURES").unwrap());
    let wav = std::fs::read(root.join("moonshine_voice/assets/beckett.wav")).unwrap();
    let parsed = pipecat_audio::read_pcm16_wav(&wav).unwrap();
    AudioRawFrame::new(
        parsed.pcm.to_vec(),
        parsed.format.sample_rate(),
        parsed.format.channels(),
    )
    .unwrap()
}
#[test]
#[ignore = "requires explicitly supplied hash-verified native/model fixtures"]
fn actual_small_and_tiny_transcribe_same_fixture_and_reload() {
    let mut model = MoonshineModel::load(config()).unwrap();
    let audio = audio();
    let settings = MoonshineSettings::default();
    for settings in [
        settings.clone(),
        MoonshineSettings {
            model: Model::Tiny,
            ..settings.clone()
        },
        settings,
    ] {
        let transcript = model
            .transcribe(&audio, &settings, &Cancellation::new())
            .unwrap();
        eprintln!("{:?}: {}", settings.model, transcript.text);
        assert!(transcript.text.to_ascii_lowercase().contains("ever tried"));
        assert!(transcript.text.to_ascii_lowercase().contains("no matter"));
        assert_eq!(transcript.language.as_str(), "en");
        assert_eq!(transcript.model, settings.model);
        assert!(!transcript.lines.is_empty());
    }
}
#[tokio::test]
#[ignore = "requires explicitly supplied hash-verified native/model fixtures"]
async fn native_worker_transcribes_and_joins() {
    let service = MoonshineService::load(config()).await.unwrap();
    let result = service
        .submit(&audio(), MoonshineSettings::default())
        .unwrap()
        .result()
        .await
        .unwrap();
    assert!(result.text.to_ascii_lowercase().contains("fail better"));
    service.drain().await.unwrap();
    assert!(!service.is_running());
}

fn settings() -> MoonshineSettings {
    MoonshineSettings::default()
}
fn verify(text: &str) {
    assert!(text.to_ascii_lowercase().contains("ever tried"), "{text}");
    assert!(text.to_ascii_lowercase().contains("no matter"), "{text}");
}
#[tokio::test]
#[ignore = "requires explicitly supplied hash-verified native/model fixtures"]
async fn bounded_worker_cancel_recovery_drop_and_graceful_join() {
    let mut config = config();
    config.queue_capacity = 1;
    let service = MoonshineService::load(config).await.unwrap();
    let audio = audio();
    let first = service.submit(&audio, settings()).unwrap();
    assert!(matches!(
        service.submit(&audio, settings()),
        Err(MoonshineError::Backpressure)
    ));
    // Cancellation suppresses output; the owned worker waits for native return.
    tokio::time::sleep(Duration::from_millis(50)).await;
    first.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(120), first.result())
            .await
            .unwrap(),
        Err(MoonshineError::Cancelled)
    ));
    let second = service.submit(&audio, settings()).unwrap();
    verify(&second.result().await.unwrap().text);
    let dropped = service.submit(&audio, settings()).unwrap();
    let cancellation = dropped.cancellation();
    drop(dropped);
    assert!(cancellation.is_cancelled());
    // Dropping a queued request may briefly occupy the bounded command lane.
    loop {
        match service.submit(&audio, settings()) {
            Ok(final_request) => {
                service.finish().unwrap();
                verify(&final_request.result().await.unwrap().text);
                break;
            }
            Err(MoonshineError::Backpressure) => tokio::time::sleep(Duration::from_millis(1)).await,
            Err(error) => panic!("{error}"),
        }
    }
    service.drain().await.unwrap();
    assert!(!service.is_running());
    assert!(matches!(
        service.submit(&audio, settings()),
        Err(MoonshineError::Closed)
    ));
}
#[tokio::test]
#[ignore = "requires explicitly supplied hash-verified native/model fixtures"]
async fn frame_processor_drains_real_transcript_before_end() {
    let service = MoonshineService::load(config()).await.unwrap();
    let processor = Arc::new(
        MoonshineSttProcessor::new(
            service,
            MoonshineProcessorOptions {
                settings: settings(),
                segmentation: SegmentationMode::AudioFrames,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(audio())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(120), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{report:?}");
    let transcript = frames.iter().position(|f| matches!(&f.frame.data, FrameData::Transcription(t) if { verify(&t.text); t.finalized && t.language.as_deref() == Some("en") })).unwrap();
    let end = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::End(_)))
        .unwrap();
    assert!(transcript < end);
    assert!(!processor.is_running());
}
#[tokio::test]
#[ignore = "requires explicitly supplied hash-verified native/model fixtures"]
async fn vad_partial_segment_flushes_on_end_and_cancel_joins() {
    let service = MoonshineService::load(config()).await.unwrap();
    let processor = Arc::new(
        MoonshineSttProcessor::new(
            service,
            MoonshineProcessorOptions {
                settings: settings(),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let sender = worker.sender();
    sender
        .send(
            Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                duration_secs: 0.2,
                timestamp: 0.0,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    sender
        .send(
            Frame::new(FrameData::InputAudio(audio())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{report:?}");
    assert!(frames.iter().any(
        |f| matches!(&f.frame.data, FrameData::Transcription(t) if { verify(&t.text); true })
    ));
    assert!(!processor.is_running());

    let service = MoonshineService::load(config()).await.unwrap();
    let processor = Arc::new(
        MoonshineSttProcessor::new(
            service,
            MoonshineProcessorOptions {
                settings: settings(),
                segmentation: SegmentationMode::AudioFrames,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(audio())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    worker.cancel("native cancellation test").await.unwrap();
    let (frames, _) = tokio::time::timeout(Duration::from_secs(120), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::Transcription(_))));
    assert!(!processor.is_running());
}

#[tokio::test]
#[ignore = "requires explicitly supplied hash-verified native/model fixtures"]
async fn concurrent_shutdown_and_aborted_waiter_share_native_join() {
    let service = Arc::new(MoonshineService::load(config()).await.unwrap());
    let pending = service.submit(&audio(), settings()).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let first_service = service.clone();
    let first = tokio::spawn(async move { first_service.drain().await });
    // Graceful drain starts an owned join while real inference is still running.
    tokio::time::sleep(Duration::from_millis(20)).await;
    first.abort();
    let _ = first.await;
    let (second, third) = tokio::join!(service.shutdown(), service.shutdown());
    second.unwrap();
    third.unwrap();
    assert!(!service.is_running());
    assert!(matches!(
        pending.result().await,
        Err(MoonshineError::Cancelled)
    ));
    service.drain().await.unwrap();
}

#[test]
#[ignore = "requires explicitly supplied hash-verified native/model fixtures"]
fn failed_reload_preserves_loaded_model_and_never_mislabels_output() {
    let mut config = config();
    config
        .models
        .iter_mut()
        .find(|b| b.model == Model::Tiny)
        .unwrap()
        .files[0]
        .artifact
        .sha256 = "0".repeat(64);
    let mut model = MoonshineModel::load(config).unwrap();
    let tiny = MoonshineSettings {
        model: Model::Tiny,
        ..Default::default()
    };
    assert!(matches!(
        model.transcribe(&audio(), &tiny, &Cancellation::new()),
        Err(MoonshineError::HashMismatch)
    ));
    assert_eq!(model.settings(), &MoonshineSettings::default());
    verify(
        &model
            .transcribe(
                &audio(),
                &MoonshineSettings::default(),
                &Cancellation::new(),
            )
            .unwrap()
            .text,
    );
}

#[tokio::test]
#[ignore = "requires explicitly supplied hash-verified native/model fixtures"]
async fn targeted_settings_are_atomic_and_capture_the_effective_model() {
    use pipecat_core::{
        FrameProcessor, ProcessorReference, ServiceUpdateSettingsFrame, Setting, SttSettings,
        UserAudioRawFrame,
    };
    let service = MoonshineService::load(config()).await.unwrap();
    let processor = Arc::new(
        MoonshineSttProcessor::new(
            service,
            MoonshineProcessorOptions {
                segmentation: SegmentationMode::AudioFrames,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let original = processor.settings().unwrap();
    assert!(processor
        .update_settings(MoonshineSettings {
            language: "es".parse().unwrap(),
            ..original.clone()
        })
        .is_err());
    assert_eq!(processor.settings().unwrap(), original);
    let health = processor.health().unwrap();
    health.set_usable(false);
    let unusable = health.snapshot();
    processor.update_settings(original.clone()).unwrap();
    assert_eq!(health.snapshot(), unusable);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let sender = worker.sender();
    let update = ServiceUpdateSettingsFrame::<SttSettings> {
        service: Some(ProcessorReference::Name("Other".into())),
        settings: [("not_supported".into(), true.into())]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    sender
        .send(
            Frame::new(FrameData::SttUpdateSettings(Box::new(update))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let update = ServiceUpdateSettingsFrame {
        settings: [("model".into(), "tiny".into())].into_iter().collect(),
        delta: Some(SttSettings {
            language: Setting::Value("en-US".into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    sender
        .send(
            Frame::new(FrameData::SttUpdateSettings(Box::new(update))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(20), worker.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(&frame.frame.data, FrameData::SttUpdateSettings(update) if update.service.is_none())
        {
            break;
        }
    }
    assert!(health.is_usable());
    assert_eq!(processor.settings().unwrap().model, Model::Tiny);
    sender
        .send(
            Frame::new(FrameData::UserAudio(UserAudioRawFrame {
                audio: audio(),
                user_id: "speaker-1".into(),
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{report:?}");
    let transcript = frames
        .iter()
        .find_map(|f| {
            if let FrameData::Transcription(t) = &f.frame.data {
                Some(t)
            } else {
                None
            }
        })
        .unwrap();
    verify(&transcript.text);
    assert!(transcript.finalized);
    assert_eq!(transcript.user_id, "speaker-1");
    assert_eq!(transcript.language.as_deref(), Some("en"));
    assert_eq!(transcript.result.as_ref().unwrap()["model"], "tiny");
    assert!(!processor.is_running());
}
