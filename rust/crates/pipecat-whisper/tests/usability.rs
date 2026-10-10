//! Health integration against explicit local assets; no test fetches models.
use pipecat_core::{
    AudioRawFrame, DirectedFrame, Direction, ErrorCategory, Frame, FrameData, FrameProcessor,
    Pipeline, PipelineConfig, PipelineWorker, ProcessorReference, ServiceUpdateSettingsFrame,
    Setting, SttSettings, UserAudioRawFrame, VadSpeakingFrame,
};
use pipecat_whisper::{
    DecodingStrategy, SegmentationMode, WhisperConfig, WhisperProcessorOptions, WhisperService,
    WhisperSettings, WhisperSttProcessor,
};
use std::{sync::Arc, time::Duration};

async fn processor(mode: SegmentationMode) -> Arc<WhisperSttProcessor> {
    let config = WhisperConfig::new(
        std::env::var_os("PIPECAT_WHISPER_MODEL").expect("explicit local model required"),
    );
    Arc::new(
        WhisperSttProcessor::new(
            WhisperService::load(config).await.unwrap(),
            WhisperProcessorOptions {
                segmentation: mode,
                trailing_silence: Duration::ZERO,
                push_empty_transcripts: true,
                settings: WhisperSettings {
                    no_speech_probability: Some(0.0),
                    strategy: DecodingStrategy::Greedy { best_of: 1 },
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

fn audio(user: &str, samples: usize) -> Frame {
    Frame::new(FrameData::UserAudio(UserAudioRawFrame {
        audio: AudioRawFrame::new(vec![0; samples * 2], 16_000, 1).unwrap(),
        user_id: user.into(),
    }))
}

fn vad(start: bool) -> Frame {
    let details = VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    };
    Frame::new(if start {
        FrameData::VadUserStartedSpeaking(details)
    } else {
        FrameData::VadUserStoppedSpeaking(details)
    })
}

fn update(update: ServiceUpdateSettingsFrame<SttSettings>) -> Frame {
    Frame::new(FrameData::SttUpdateSettings(Box::new(update)))
}

async fn receive(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(20), worker.recv())
        .await
        .expect("pipeline stalled")
        .expect("pipeline closed")
}

async fn forward(
    worker: &mut PipelineWorker,
    frame: Frame,
    direction: Direction,
) -> Vec<DirectedFrame> {
    let id = frame.id;
    worker.sender().send(frame, direction).await.unwrap();
    let mut frames = vec![];
    loop {
        let frame = receive(worker).await;
        let reached = frame.frame.id == id;
        if reached {
            assert_eq!(frame.direction, direction);
        }
        frames.push(frame);
        if reached {
            return frames;
        }
    }
}

#[tokio::test]
#[ignore = "requires explicit local GGML model"]
async fn public_settings_recover_only_on_valid_effective_change() {
    let processor = processor(SegmentationMode::AudioFrames).await;
    let health = processor.health().unwrap();
    let original = processor.settings().unwrap();
    health.set_usable(false);
    let unusable = health.snapshot();
    processor.update_settings(original.clone()).unwrap();
    assert_eq!(health.snapshot(), unusable);
    assert!(processor
        .update_settings(WhisperSettings {
            temperature: -1.0,
            ..original.clone()
        })
        .is_err());
    assert_eq!(processor.settings().unwrap(), original);
    assert_eq!(health.snapshot(), unusable);
    let changed = WhisperSettings {
        temperature: 0.1,
        ..original
    };
    processor.update_settings(changed.clone()).unwrap();
    assert!(health.is_usable());
    let worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.processor_health(0).unwrap().set_usable(false);
    assert!(
        !health.is_usable(),
        "pre-start and running handles must be shared"
    );
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    assert!(processor
        .update_settings(WhisperSettings {
            temperature: 0.2,
            ..changed
        })
        .is_err());
    assert!(
        !health.is_usable(),
        "settings cannot reopen a finished service"
    );
    assert!(!processor.is_running());
}

#[tokio::test]
#[ignore = "requires explicit local GGML model"]
async fn unusable_audio_passthrough_controls_and_targeted_recovery() {
    let processor = processor(SegmentationMode::AudioFrames).await;
    let health = processor.health().unwrap();
    health.set_usable(false);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut seen = vec![];
    let invalid_audio = Frame::new(FrameData::InputAudio(AudioRawFrame {
        audio: vec![1],
        sample_rate: 8_000,
        num_channels: 2,
        context_id: None,
    }));
    seen.extend(forward(&mut worker, invalid_audio, Direction::Downstream).await);
    seen.extend(forward(&mut worker, audio("gated", 16_000), Direction::Downstream).await);
    seen.extend(
        forward(
            &mut worker,
            Frame::text("upstream-control"),
            Direction::Upstream,
        )
        .await,
    );
    seen.extend(
        forward(
            &mut worker,
            Frame::new(FrameData::SttMute(true)),
            Direction::Downstream,
        )
        .await,
    );
    let unusable = health.snapshot();
    for settings in [
        ServiceUpdateSettingsFrame {
            service: Some(ProcessorReference::Name("other".into())),
            settings: [("temperature".into(), 0.1.into())].into_iter().collect(),
            ..Default::default()
        },
        ServiceUpdateSettingsFrame::default(),
        ServiceUpdateSettingsFrame {
            settings: [("no_speech_prob".into(), 0.0.into())]
                .into_iter()
                .collect(),
            ..Default::default()
        },
        ServiceUpdateSettingsFrame {
            delta: Some(SttSettings {
                language: Setting::Value("en-US".into()),
                ..Default::default()
            }),
            ..Default::default()
        },
    ] {
        seen.extend(forward(&mut worker, update(settings), Direction::Downstream).await);
        assert_eq!(health.snapshot(), unusable);
    }
    let invalid = update(ServiceUpdateSettingsFrame {
        settings: [("temperature".into(), (-1.0).into())]
            .into_iter()
            .collect(),
        ..Default::default()
    });
    worker
        .sender()
        .send(invalid, Direction::Downstream)
        .await
        .unwrap();
    loop {
        let error = receive(&mut worker).await;
        if let FrameData::Error(details) = &error.frame.data {
            assert_eq!(details.category, ErrorCategory::InvalidRequest);
            assert_eq!(error.direction, Direction::Upstream);
            let origin = error.frame.error_origin().unwrap();
            assert_eq!(origin.processor.name.as_ref(), "WhisperSttProcessor");
            assert_eq!(origin.reported_health, unusable);
            seen.push(error);
            break;
        }
        seen.push(error);
    }
    assert_eq!(processor.settings().unwrap().temperature, 0.0);
    assert_eq!(health.snapshot(), unusable);
    seen.extend(
        forward(
            &mut worker,
            update(ServiceUpdateSettingsFrame {
                settings: [("temperature".into(), 0.1.into())].into_iter().collect(),
                ..Default::default()
            }),
            Direction::Downstream,
        )
        .await,
    );
    assert!(health.is_usable());
    let restored = health.snapshot();
    worker
        .sender()
        .send(
            update(ServiceUpdateSettingsFrame {
                settings: [("temperature".into(), 0.2.into())].into_iter().collect(),
                delta: Some(SttSettings {
                    language: Setting::Value("es".into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            Direction::Downstream,
        )
        .await
        .unwrap();
    loop {
        let error = receive(&mut worker).await;
        if let FrameData::Error(details) = &error.frame.data {
            assert_eq!(details.category, ErrorCategory::InvalidRequest);
            assert_eq!(
                error.frame.error_origin().unwrap().reported_health,
                restored
            );
            seen.push(error);
            break;
        }
        seen.push(error);
    }
    assert_eq!(health.snapshot(), restored);
    assert_eq!(processor.settings().unwrap().temperature, 0.1);
    assert_eq!(
        processor.settings().unwrap().language.unwrap().as_str(),
        "en"
    );
    seen.extend(
        forward(
            &mut worker,
            audio("upstream-pass", 16_000),
            Direction::Upstream,
        )
        .await,
    );
    seen.extend(
        forward(
            &mut worker,
            audio("still-muted", 16_000),
            Direction::Downstream,
        )
        .await,
    );
    seen.extend(
        forward(
            &mut worker,
            Frame::new(FrameData::SttMute(false)),
            Direction::Downstream,
        )
        .await,
    );
    seen.extend(forward(&mut worker, audio("fresh", 16_000), Direction::Downstream).await);
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    seen.extend(frames);
    assert_eq!(report.errors.len(), 2);
    let users: Vec<_> = seen
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Transcription(t) => Some(t.user_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(users, ["fresh"]);
    assert!(!processor.is_running());
}

#[tokio::test]
#[ignore = "requires explicit local GGML model"]
async fn coalesced_recovery_discards_vad_prefix_and_tail() {
    let processor = processor(SegmentationMode::Vad).await;
    let health = processor.health().unwrap();
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    forward(&mut worker, vad(true), Direction::Downstream).await;
    forward(
        &mut worker,
        audio("retired-prefix", 8_000),
        Direction::Downstream,
    )
    .await;
    health.set_usable(false);
    health.set_usable(true);
    let mut seen = forward(&mut worker, vad(false), Direction::Downstream).await;
    seen.extend(
        forward(
            &mut worker,
            audio("retired-tail", 8_000),
            Direction::Downstream,
        )
        .await,
    );
    seen.extend(forward(&mut worker, vad(true), Direction::Upstream).await);
    seen.extend(forward(&mut worker, audio("fresh", 16_000), Direction::Downstream).await);
    seen.extend(forward(&mut worker, vad(false), Direction::Upstream).await);
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    seen.extend(frames);
    assert!(report.errors.is_empty(), "{report:?}");
    let transcripts: Vec<_> = seen
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Transcription(t) => Some(t),
            _ => None,
        })
        .collect();
    assert_eq!(transcripts.len(), 1);
    assert_eq!(transcripts[0].user_id, "fresh");
    assert_eq!(
        transcripts[0].result.as_ref().unwrap()["audio_duration_seconds"],
        1.0
    );
}

#[tokio::test]
#[ignore = "requires explicit local GGML model and JFK audio fixture"]
async fn coalesced_recovery_retires_active_and_queued_native_results() {
    let processor = processor(SegmentationMode::AudioFrames).await;
    let health = processor.health().unwrap();
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let data = std::fs::read(
        std::env::var_os("PIPECAT_WHISPER_FIXTURE").expect("explicit local JFK WAV required"),
    )
    .unwrap();
    let wav = pipecat_audio::read_pcm16_wav(&data).unwrap();
    for user in ["retired-active", "retired-queued"] {
        let mut frame = audio(user, 1);
        if let FrameData::UserAudio(audio) = &mut frame.data {
            audio.audio.audio = wav.pcm.to_vec();
        }
        let seen = forward(&mut worker, frame, Direction::Downstream).await;
        assert!(
            !seen
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::Transcription(_))),
            "native result completed before retirement was exercised"
        );
    }
    health.set_usable(false);
    health.set_usable(true);
    let mut seen = forward(&mut worker, audio("fresh", 16_000), Direction::Downstream).await;
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(20), worker.collect())
        .await
        .unwrap()
        .unwrap();
    seen.extend(frames);
    assert!(report.errors.is_empty(), "{report:?}");
    let users: Vec<_> = seen
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::Transcription(t) => Some(t.user_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(users, ["fresh"]);
    assert_eq!(
        seen.iter()
            .filter(|f| matches!(f.frame.data, FrameData::Metrics(_)))
            .count(),
        1
    );
    assert!(health.is_usable());
    assert!(!processor.is_running());
}

#[tokio::test]
#[ignore = "requires explicit local GGML model"]
async fn terminal_frames_cleanup_while_unusable_without_flushing_speech() {
    for terminal in [
        Frame::end(),
        Frame::new(FrameData::Stop),
        Frame::cancel("test"),
    ] {
        let stopped = matches!(terminal.data, FrameData::Stop);
        let processor = processor(SegmentationMode::Vad).await;
        let health = processor.health().unwrap();
        let mut worker = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        forward(&mut worker, vad(true), Direction::Downstream).await;
        forward(
            &mut worker,
            audio("discard-on-end", 16_000),
            Direction::Downstream,
        )
        .await;
        health.set_usable(false);
        let terminal_id = terminal.id;
        worker
            .sender()
            .send(terminal, Direction::Downstream)
            .await
            .unwrap();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(20), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{report:?}");
        assert!(frames.iter().any(|f| f.frame.id == terminal_id));
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::Transcription(_))));
        assert!(!processor.is_running());
        assert!(!health.is_usable());
        if stopped {
            health.set_usable(true);
            let restart = Pipeline::new(vec![processor.clone()])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            assert!(
                restart
                    .errors()
                    .iter()
                    .any(|error| error.error.contains("service is closed")),
                "processor setup must reject a second run"
            );
            restart.end().await.unwrap();
            let (frames, _) = restart.collect().await.unwrap();
            assert!(!frames
                .iter()
                .any(|frame| matches!(frame.frame.data, FrameData::Transcription(_))));
            assert!(!processor.is_running());
        }
    }
}
