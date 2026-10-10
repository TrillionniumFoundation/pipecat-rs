//! Explicit offline native-model checks. Run with --ignored after provisioning
//! the model/fixture described in README; these tests never fetch assets.
use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, Pipeline, PipelineConfig, VadSpeakingFrame,
};
use pipecat_whisper::{
    Cancellation, DecodingStrategy, SegmentationMode, WhisperConfig, WhisperError, WhisperModel,
    WhisperProcessorOptions, WhisperService, WhisperSettings, WhisperSttProcessor,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
fn config() -> WhisperConfig {
    WhisperConfig::new(PathBuf::from(
        std::env::var_os("PIPECAT_WHISPER_MODEL")
            .expect("set PIPECAT_WHISPER_MODEL to an explicit GGML file"),
    ))
}
fn audio() -> AudioRawFrame {
    let path = std::env::var_os("PIPECAT_WHISPER_FIXTURE")
        .expect("set PIPECAT_WHISPER_FIXTURE to upstream JFK WAV");
    let data = std::fs::read(path).unwrap();
    let wav = pipecat_audio::read_pcm16_wav(&data).unwrap();
    AudioRawFrame::new(
        wav.pcm.to_vec(),
        wav.format.sample_rate(),
        wav.format.channels(),
    )
    .unwrap()
}
fn settings() -> WhisperSettings {
    WhisperSettings {
        strategy: DecodingStrategy::Greedy { best_of: 1 },
        ..Default::default()
    }
}
fn verify(text: &str) {
    let text = text.to_ascii_lowercase();
    assert!(
        text.contains("ask not what your country can do for you"),
        "actual transcript: {text}"
    );
    assert!(
        text.contains("ask what you can do for your country"),
        "actual transcript: {text}"
    );
}
#[test]
#[ignore = "requires explicit local GGML model and JFK audio fixture"]
fn real_model_repeated_transcription_and_language_guard() {
    let mut model = WhisperModel::load(config()).unwrap();
    assert!(!model.is_multilingual());
    let audio = audio();
    for _ in 0..3 {
        let result = model
            .transcribe(&audio, &settings(), &Cancellation::new())
            .unwrap();
        verify(&result.text);
        assert_eq!(result.language.unwrap().as_str(), "en");
        assert!(!result.segments.is_empty());
        assert!(result
            .segments
            .iter()
            .all(|s| s.start_ms <= s.end_ms && s.no_speech_probability.is_finite()));
    }
    assert!(matches!(
        model.transcribe(
            &audio,
            &WhisperSettings {
                language: Some("es".parse().unwrap()),
                ..settings()
            },
            &Cancellation::new()
        ),
        Err(WhisperError::UnsupportedLanguage(_))
    ));
    let cancel = Cancellation::new();
    cancel.cancel();
    assert!(matches!(
        model.transcribe(&audio, &settings(), &cancel),
        Err(WhisperError::Cancelled)
    ));
}
#[tokio::test]
#[ignore = "requires explicit local GGML model and JFK audio fixture"]
async fn bounded_worker_abort_recovery_drop_and_graceful_join() {
    let mut config = config();
    config.queue_capacity = 1;
    let service = WhisperService::load(config).await.unwrap();
    let audio = audio();
    let first = service.submit(&audio, settings()).unwrap();
    assert!(matches!(
        service.submit(&audio, settings()),
        Err(WhisperError::Backpressure)
    ));
    // Let real decoding enter the native call, then abort it cooperatively.
    tokio::time::sleep(Duration::from_millis(50)).await;
    first.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(10), first.result())
            .await
            .unwrap(),
        Err(WhisperError::Cancelled)
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
            Err(WhisperError::Backpressure) => tokio::time::sleep(Duration::from_millis(1)).await,
            Err(error) => panic!("{error}"),
        }
    }
    service.drain().await.unwrap();
    assert!(!service.is_running());
    assert!(matches!(
        service.submit(&audio, settings()),
        Err(WhisperError::Closed)
    ));
}
#[tokio::test]
#[ignore = "requires explicit local GGML model and JFK audio fixture"]
async fn frame_processor_drains_real_transcript_before_end() {
    let service = WhisperService::load(config()).await.unwrap();
    let processor = Arc::new(
        WhisperSttProcessor::new(
            service,
            WhisperProcessorOptions {
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
    let (frames, report) = tokio::time::timeout(Duration::from_secs(30), worker.collect())
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
#[ignore = "requires explicit local GGML model and JFK audio fixture"]
async fn vad_partial_segment_flushes_on_end_and_cancel_joins() {
    let service = WhisperService::load(config()).await.unwrap();
    let processor = Arc::new(
        WhisperSttProcessor::new(
            service,
            WhisperProcessorOptions {
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

    let service = WhisperService::load(config()).await.unwrap();
    let processor = Arc::new(
        WhisperSttProcessor::new(
            service,
            WhisperProcessorOptions {
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
    let (frames, _) = tokio::time::timeout(Duration::from_secs(10), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::Transcription(_))));
    assert!(!processor.is_running());
}

#[tokio::test]
#[ignore = "requires explicit local GGML model and JFK audio fixture"]
async fn concurrent_shutdown_and_aborted_waiter_share_native_join() {
    let service = Arc::new(WhisperService::load(config()).await.unwrap());
    let pending = service.submit(&audio(), settings()).unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let first_service = service.clone();
    let first = tokio::spawn(async move { first_service.drain().await });
    // Graceful drain starts an owned join while real inference is still running.
    tokio::time::sleep(Duration::from_millis(20)).await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let (second, third) = tokio::join!(service.shutdown(), service.shutdown());
    second.unwrap();
    third.unwrap();
    assert!(!service.is_running());
    assert!(matches!(
        pending.result().await,
        Err(WhisperError::Cancelled)
    ));
    service.drain().await.unwrap();
}

#[test]
#[ignore = "requires explicit local GGML model and JFK audio fixture"]
fn real_model_probability_filter_auto_language_and_initial_prompt() {
    let mut model = WhisperModel::load(config()).unwrap();
    let filter = WhisperSettings {
        no_speech_probability: Some(0.0),
        ..settings()
    };
    let empty = model
        .transcribe(&audio(), &filter, &Cancellation::new())
        .unwrap();
    assert!(empty.text.is_empty());
    assert!(empty.segments.is_empty());
    let prompt = WhisperSettings {
        language: None,
        no_speech_probability: None,
        initial_prompt: Some("This is a public address by President Kennedy.".into()),
        ..settings()
    };
    let result = model
        .transcribe(&audio(), &prompt, &Cancellation::new())
        .unwrap();
    verify(&result.text);
    assert_eq!(result.language.unwrap().as_str(), "en");
}

#[tokio::test]
#[ignore = "requires explicit local GGML model and JFK audio fixture"]
async fn processor_updates_are_atomic_targeted_and_emit_final_empty_results() {
    use pipecat_core::{
        ProcessorReference, ServiceUpdateSettingsFrame, Setting, SttSettings, UserAudioRawFrame,
    };
    let service = WhisperService::load(config()).await.unwrap();
    let processor = Arc::new(
        WhisperSttProcessor::new(
            service,
            WhisperProcessorOptions {
                settings: settings(),
                segmentation: SegmentationMode::AudioFrames,
                push_empty_transcripts: true,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let before = processor.settings().unwrap();
    assert!(processor
        .update_settings(WhisperSettings {
            language: Some("es".parse().unwrap()),
            ..settings()
        })
        .is_err());
    assert_eq!(processor.settings().unwrap(), before);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let sender = worker.sender();
    let unrelated = ServiceUpdateSettingsFrame::<SttSettings> {
        service: Some(ProcessorReference::Name("AnotherSttService".into())),
        settings: [("unsupported".into(), true.into())].into_iter().collect(),
        ..Default::default()
    };
    sender
        .send(
            Frame::new(FrameData::SttUpdateSettings(Box::new(unrelated))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let update = ServiceUpdateSettingsFrame {
        settings: [("no_speech_prob".into(), 0.0.into())]
            .into_iter()
            .collect(),
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
    // UserAudio is a System frame and may overtake ordinary settings frames.
    // Observe the applied update before delivering the next utterance.
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), worker.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(&frame.frame.data, FrameData::SttUpdateSettings(update) if update.service.is_none())
        {
            break;
        }
    }
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
    assert!(frames.iter().any(|f| matches!(&f.frame.data, FrameData::Transcription(t) if t.text.is_empty() && t.finalized && t.user_id == "speaker-1")));
    assert_eq!(
        processor.settings().unwrap().language.unwrap().as_str(),
        "en"
    );
    assert_eq!(
        processor.settings().unwrap().no_speech_probability,
        Some(0.0)
    );
    assert!(!processor.is_running());
}
