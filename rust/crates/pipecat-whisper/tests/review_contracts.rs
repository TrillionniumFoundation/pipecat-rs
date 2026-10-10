//! Independent offline lifecycle/segmentation regressions. These never fetch assets.
use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, Pipeline, PipelineConfig, UserAudioRawFrame,
    VadSpeakingFrame,
};
use pipecat_whisper::{
    DecodingStrategy, WhisperConfig, WhisperProcessorOptions, WhisperService, WhisperSettings,
    WhisperSttProcessor,
};
use std::{future::Future, sync::Arc, task::Poll, time::Duration};

fn config() -> WhisperConfig {
    WhisperConfig::new(std::env::var_os("PIPECAT_WHISPER_MODEL").expect("explicit model required"))
}

#[test]
fn clearing_abort_callback_is_unwind_safe_when_capture_drop_panics() {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("captured destructor failed");
        }
    }
    let capture = PanicOnDrop;
    let mut params =
        whisper_rs::FullParams::new(whisper_rs::SamplingStrategy::Greedy { best_of: 1 });
    params.set_abort_callback_safe(move || {
        let _keep_alive = &capture;
        false
    });
    let clearing = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        params.set_abort_callback_safe::<_, fn() -> bool>(None::<fn() -> bool>);
    }));
    assert!(clearing.is_err());
    // Check the wrapper's public Debug view without invoking any dangling FFI
    // pointer. A caught destructor panic must leave native pointers cleared.
    let state = format!("{params:?}");
    assert!(state.contains("abort_callback: None"), "{state}");
    assert!(state.contains("abort_callback_user_data: 0x0"), "{state}");
}

#[test]
#[ignore = "requires explicit local GGML model"]
fn drop_after_cancelled_join_does_not_depend_on_blocking_pool_capacity() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let completed = runtime.block_on(async {
        let service = WhisperService::load(config()).await.unwrap();
        // Occupy the only blocking-pool worker before scheduling the async join.
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (service_tx, service_rx) = std::sync::mpsc::channel::<WhisperService>();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            let service = service_rx.recv().unwrap();
            drop(service);
            let _ = dropped_tx.send(());
        });
        started_rx.await.unwrap();
        // Poll once to move join ownership off the service, then cancel the waiter.
        let mut drain = Box::pin(service.drain());
        std::future::poll_fn(|cx| {
            let _ = drain.as_mut().poll(cx);
            Poll::Ready(())
        })
        .await;
        drop(drain);
        service_tx
            .send(service)
            .unwrap_or_else(|_| panic!("drop worker exited"));
        tokio::time::timeout(Duration::from_secs(2), dropped_rx)
            .await
            .is_ok()
    });
    // A failure should be observable rather than hanging the test runner at drop.
    runtime.shutdown_background();
    assert!(
        completed,
        "last-owner Drop deadlocked behind its own queued join task"
    );
}

#[tokio::test]
#[ignore = "requires explicit local GGML model"]
async fn vad_overflow_discards_remainder_through_stop() {
    check_vad_discard(&[24_000, 24_000, 1_600], "segment overflow").await;
}

#[tokio::test]
#[ignore = "requires explicit local GGML model"]
async fn rejected_oversized_frame_discards_the_active_prefix() {
    check_vad_discard(&[24_000, 40_000], "audio exceeds").await;
}

async fn check_vad_discard(sample_counts: &[usize], expected_error: &str) {
    let mut config = config();
    config.max_segment_duration = Duration::from_secs(2);
    let processor = Arc::new(
        WhisperSttProcessor::new(
            WhisperService::load(config).await.unwrap(),
            WhisperProcessorOptions {
                pre_roll_duration: Duration::from_millis(100),
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
    );
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let sender = worker.sender();
    sender
        .send(
            Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                duration_secs: 0.0,
                timestamp: 0.0,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    // UserAudio and VAD frames share the FIFO system lane.
    for sample_count in sample_counts {
        sender
            .send(
                Frame::new(FrameData::UserAudio(UserAudioRawFrame {
                    audio: AudioRawFrame::new(vec![0; sample_count * 2], 16_000, 1).unwrap(),
                    user_id: "overflowed-speaker".into(),
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    sender
        .send(
            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.0,
                timestamp: 0.0,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    // A late tail after Stop must not be reused as pre-roll for the next speech.
    let tail = Frame::new(FrameData::UserAudio(UserAudioRawFrame {
        audio: AudioRawFrame::new(vec![0; 3_200], 16_000, 1).unwrap(),
        user_id: "discarded-tail".into(),
    }));
    let tail_id = tail.id;
    sender.send(tail, Direction::Downstream).await.unwrap();
    while tokio::time::timeout(Duration::from_secs(2), worker.recv())
        .await
        .unwrap()
        .unwrap()
        .frame
        .id
        != tail_id
    {}
    // Invalid audio is a permanent request error. Recovery is explicit and
    // admits only a new utterance after the discarded tail has passed through.
    let health = worker.processor_health(0).unwrap();
    assert!(!health.is_usable());
    health.set_usable(true);
    sender
        .send(
            Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                duration_secs: 0.0,
                timestamp: 0.0,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    sender
        .send(
            Frame::new(FrameData::UserAudio(UserAudioRawFrame {
                audio: AudioRawFrame::new(vec![0; 32_000], 16_000, 1).unwrap(),
                user_id: "fresh-utterance".into(),
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    sender
        .send(
            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.0,
                timestamp: 0.0,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(20), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(
        report
            .errors
            .iter()
            .any(|error| error.error.contains(expected_error)),
        "{report:?}"
    );
    let transcripts: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Transcription(transcript) => Some(transcript),
            _ => None,
        })
        .collect();
    assert_eq!(
        transcripts.len(),
        1,
        "only the fresh utterance should emit a transcript: {transcripts:?}"
    );
    assert_eq!(transcripts[0].user_id, "fresh-utterance");
    assert_eq!(
        transcripts[0].result.as_ref().unwrap()["audio_duration_seconds"],
        1.0,
        "discarded audio must not become the fresh utterance's pre-roll"
    );
    assert!(!processor.is_running());
}
