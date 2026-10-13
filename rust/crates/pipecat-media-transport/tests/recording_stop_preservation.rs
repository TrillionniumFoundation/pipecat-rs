//! Generic pipeline Stop preserves ordinary recording for an explicit owner flush.
//! Source: Python AudioBufferProcessor and PipelineWorker at af9f9da9.
//! This contract does not assert worker resume or recorder reattachment.
use std::sync::Arc;
use std::time::Duration;

use pipecat_audio::encode_pcm16_le;
use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig,
    ProcessorError, StartFrame, WorkerState,
};
use pipecat_media_transport::recording::{
    AudioBufferConfig, AudioBufferProcessor, RecordingClock, RecordingEvent,
};
use tokio::sync::broadcast;

struct FixedClock;
impl RecordingClock for FixedClock {
    fn now(&self) -> Duration {
        Duration::ZERO
    }
}

fn plain_config() -> AudioBufferConfig {
    AudioBufferConfig {
        sample_rate: Some(16000),
        num_channels: 1,
        buffer_size: 0,
        enable_turn_audio: false,
        auto_start_recording: true,
        ..Default::default()
    }
}

fn recorder(config: AudioBufferConfig) -> Arc<AudioBufferProcessor> {
    Arc::new(AudioBufferProcessor::with_clock(config, Arc::new(FixedClock)).unwrap())
}

fn output_audio() -> FrameData {
    FrameData::OutputAudio(AudioRawFrame::new(encode_pcm16_le(&[100, -100]), 16000, 1).unwrap())
}

#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
    recording: bool,
    has_audio: bool,
    bytes: (usize, usize),
    merged: Vec<u8>,
}

fn snapshot(recorder: &AudioBufferProcessor) -> Result<Snapshot, ProcessorError> {
    Ok(Snapshot {
        recording: recorder.is_recording()?,
        has_audio: recorder.has_audio()?,
        bytes: recorder.buffered_bytes()?,
        merged: recorder.merge_audio_buffers()?,
    })
}

fn drain(
    receiver: &mut broadcast::Receiver<RecordingEvent>,
) -> Result<Vec<RecordingEvent>, String> {
    let mut events = Vec::new();
    loop {
        match receiver.try_recv() {
            Ok(event) => events.push(event),
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                return Ok(events);
            }
            Err(error) => return Err(format!("unexpected recording event loss: {error}")),
        }
    }
}

fn retained() -> Snapshot {
    Snapshot {
        recording: true,
        has_audio: true,
        bytes: (0, 4),
        merged: encode_pcm16_le(&[100, -100]),
    }
}

fn empty() -> Snapshot {
    Snapshot {
        recording: false,
        has_audio: false,
        bytes: (0, 0),
        merged: vec![],
    }
}

fn final_events() -> Vec<RecordingEvent> {
    vec![
        RecordingEvent::AudioData {
            audio: encode_pcm16_le(&[100, -100]).into(),
            sample_rate: 16000,
            num_channels: 1,
        },
        RecordingEvent::TrackAudioData {
            user: encode_pcm16_le(&[0, 0]).into(),
            bot: encode_pcm16_le(&[100, -100]).into(),
            sample_rate: 16000,
            num_channels: 1,
        },
        RecordingEvent::RecordingStopped,
    ]
}

#[tokio::test]
async fn plain_direct_stop_retains_pcm_until_explicit_owner_stop() {
    let recorder = recorder(plain_config());
    let mut events = recorder.subscribe();
    let processing = (|| -> Result<(), ProcessorError> {
        recorder.handle_frame(&FrameData::Start(StartFrame::default()))?;
        recorder.handle_frame(&output_audio())?;
        recorder.handle_frame(&FrameData::Stop)
    })();
    let after_stop = snapshot(&recorder);
    let stop_events = drain(&mut events);
    let explicit_stop = recorder.stop_recording();
    let after_explicit_stop = snapshot(&recorder);
    let owner_events = drain(&mut events);
    let repeated_stop = recorder.stop_recording();
    let cleanup = recorder.cleanup().await;
    let remaining_events = drain(&mut events);

    // Owner cleanup happens before every semantic assertion, including old-code failures.
    processing.unwrap();
    explicit_stop.unwrap();
    repeated_stop.unwrap();
    cleanup.unwrap();
    assert_eq!(after_stop.unwrap(), retained());
    assert_eq!(stop_events.unwrap(), [RecordingEvent::RecordingStarted]);
    assert_eq!(after_explicit_stop.unwrap(), empty());
    assert_eq!(owner_events.unwrap(), final_events());
    assert!(remaining_events.unwrap().is_empty());
}

#[tokio::test]
async fn plain_pipeline_stop_collect_retains_forwarded_pcm_until_explicit_owner_stop() {
    let recorder = recorder(plain_config());
    let mut events = recorder.subscribe();
    let audio = Frame::new(output_audio());
    let expected_audio = audio.clone();
    let stop = Frame::new(FrameData::Stop);
    let expected_stop = stop.clone();
    let execution = tokio::time::timeout(Duration::from_secs(5), async {
        let mut worker = Pipeline::new(vec![recorder.clone()])
            .start(PipelineConfig::default())
            .await
            .map_err(|error| error.to_string())?;
        let start = worker.recv().await.ok_or("missing Start")?;
        worker
            .sender()
            .send(audio, Direction::Downstream)
            .await
            .map_err(|error| error.to_string())?;
        // Receiving the original audio establishes processing before Stop admission.
        let forwarded = worker.recv().await.ok_or("missing forwarded audio")?;
        worker
            .sender()
            .send(stop, Direction::Downstream)
            .await
            .map_err(|error| error.to_string())?;
        let (tail, report) = worker.collect().await.map_err(|error| error.to_string())?;
        Ok::<_, String>((start, forwarded, tail, report))
    })
    .await;
    let after_stop = snapshot(&recorder);
    let stop_events = drain(&mut events);
    let explicit_stop = recorder.stop_recording();
    let after_explicit_stop = snapshot(&recorder);
    let owner_events = drain(&mut events);
    let repeated_stop = recorder.stop_recording();
    let cleanup = recorder.cleanup().await;
    let remaining_events = drain(&mut events);

    // Inspect captured Stop state only after the owner has finished cleanup.
    explicit_stop.unwrap();
    repeated_stop.unwrap();
    cleanup.unwrap();
    let (start, forwarded, tail, report) = execution.expect("bounded Stop collection").unwrap();
    assert!(matches!(start.frame.data, FrameData::Start(_)));
    assert_eq!(start.direction, Direction::Downstream);
    assert_eq!(forwarded.frame, expected_audio);
    assert_eq!(forwarded.direction, Direction::Downstream);
    assert_eq!(report.state, WorkerState::Stopped);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.total_errors, 0);
    assert_eq!(report.dropped_frames, 0);
    assert_eq!(
        tail.len(),
        1,
        "Stop must be forwarded exactly once: {tail:?}"
    );
    assert_eq!(tail[0].frame, expected_stop);
    assert_eq!(tail[0].direction, Direction::Downstream);
    assert_eq!(after_stop.unwrap(), retained());
    assert_eq!(stop_events.unwrap(), [RecordingEvent::RecordingStarted]);
    assert_eq!(after_explicit_stop.unwrap(), empty());
    assert_eq!(owner_events.unwrap(), final_events());
    assert!(remaining_events.unwrap().is_empty());
}

#[tokio::test]
async fn explicit_recording_stop_end_and_cancel_keep_exact_final_events() {
    for enable_turn_audio in [false, true] {
        for terminal in [
            FrameData::AudioBufferStopRecording,
            Frame::end().data,
            Frame::cancel("requested cancellation").data,
            FrameData::EndDetailed(pipecat_core::ReasonFrame { reason: None }),
            FrameData::EndDetailed(pipecat_core::ReasonFrame {
                reason: Some("requested end".into()),
            }),
            FrameData::CancelDetailed(pipecat_core::ReasonFrame { reason: None }),
            FrameData::CancelDetailed(pipecat_core::ReasonFrame {
                reason: Some("requested cancellation".into()),
            }),
        ] {
            let recorder = recorder(AudioBufferConfig {
                enable_turn_audio,
                ..plain_config()
            });
            let mut events = recorder.subscribe();
            let processing = (|| -> Result<(), ProcessorError> {
                recorder.handle_frame(&FrameData::Start(StartFrame::default()))?;
                recorder.handle_frame(&output_audio())?;
                recorder.handle_frame(&terminal)
            })();
            let after_terminal = snapshot(&recorder);
            let terminal_events = drain(&mut events);
            let repeated_terminal = recorder.handle_frame(&terminal);
            let explicit_stop = recorder.stop_recording();
            let cleanup = recorder.cleanup().await;
            let repeated_events = drain(&mut events);

            processing.unwrap();
            repeated_terminal.unwrap();
            explicit_stop.unwrap();
            cleanup.unwrap();
            assert_eq!(after_terminal.unwrap(), empty(), "{terminal:?}");
            let mut expected = vec![RecordingEvent::RecordingStarted];
            expected.extend(final_events());
            assert_eq!(terminal_events.unwrap(), expected, "{terminal:?}");
            assert!(repeated_events.unwrap().is_empty(), "{terminal:?}");
        }
    }
}

#[tokio::test]
async fn plain_stop_owner_cleanup_flushes_retained_audio_once() {
    let recorder = recorder(plain_config());
    let mut events = recorder.subscribe();
    let processing = (|| -> Result<(), ProcessorError> {
        recorder.handle_frame(&FrameData::Start(StartFrame::default()))?;
        recorder.handle_frame(&output_audio())?;
        recorder.handle_frame(&FrameData::Stop)
    })();
    let before_cleanup = snapshot(&recorder);
    let before_events = drain(&mut events);
    let cleanup = recorder.cleanup().await;
    let after_cleanup = snapshot(&recorder);
    let cleanup_events = drain(&mut events);
    let second_cleanup = recorder.cleanup().await;
    let explicit_stop = recorder.stop_recording();
    let repeated_events = drain(&mut events);

    processing.unwrap();
    cleanup.unwrap();
    second_cleanup.unwrap();
    explicit_stop.unwrap();
    assert_eq!(before_cleanup.unwrap(), retained());
    assert_eq!(before_events.unwrap(), [RecordingEvent::RecordingStarted]);
    assert_eq!(after_cleanup.unwrap(), empty());
    assert_eq!(cleanup_events.unwrap(), final_events());
    assert!(repeated_events.unwrap().is_empty());
}

#[tokio::test]
async fn plain_stop_while_already_idle_emits_no_recording_events() {
    let recorder = recorder(AudioBufferConfig {
        auto_start_recording: false,
        ..plain_config()
    });
    let mut events = recorder.subscribe();
    let processing = (|| -> Result<(), ProcessorError> {
        recorder.handle_frame(&FrameData::Start(StartFrame::default()))?;
        recorder.handle_frame(&output_audio())?;
        recorder.handle_frame(&FrameData::Stop)?;
        recorder.handle_frame(&FrameData::Stop)
    })();
    let after_stop = snapshot(&recorder);
    let explicit_stop = recorder.stop_recording();
    let cleanup = recorder.cleanup().await;
    let all_events = drain(&mut events);

    processing.unwrap();
    explicit_stop.unwrap();
    cleanup.unwrap();
    assert_eq!(after_stop.unwrap(), empty());
    assert!(all_events.unwrap().is_empty());
}

#[tokio::test]
async fn manual_turn_audio_stop_still_flushes_and_retains_the_numbered_cut() {
    let recorder = recorder(AudioBufferConfig {
        enable_turn_audio: true,
        ..plain_config()
    });
    let mut events = recorder.subscribe();
    let processing = (|| -> Result<(), ProcessorError> {
        recorder.handle_frame(&FrameData::Start(StartFrame::default()))?;
        recorder.handle_frame(&FrameData::BotStartedSpeaking)?;
        recorder.handle_frame(&output_audio())?;
        recorder.handle_frame(&FrameData::Stop)
    })();
    let after_stop = snapshot(&recorder);
    let stop_events = drain(&mut events);
    let emit = recorder.emit_turn_audio(7);
    let numbered_events = drain(&mut events);
    let explicit_stop = recorder.stop_recording();
    let cleanup = recorder.cleanup().await;
    let repeated_events = drain(&mut events);

    processing.unwrap();
    emit.unwrap();
    explicit_stop.unwrap();
    cleanup.unwrap();
    assert_eq!(after_stop.unwrap(), empty());
    let mut expected = vec![RecordingEvent::RecordingStarted];
    expected.extend(final_events());
    assert_eq!(stop_events.unwrap(), expected);
    assert_eq!(
        numbered_events.unwrap(),
        [RecordingEvent::BotTurnAudio(
            pipecat_media_transport::recording::TurnAudioData {
                turn_number: 7,
                session_id: None,
                audio: encode_pcm16_le(&[100, -100]).into(),
                sample_rate: 16000,
                num_channels: 1,
            }
        )]
    );
    assert!(repeated_events.unwrap().is_empty());
}

#[cfg(feature = "turn-recording")]
mod attached {
    use super::*;
    use pipecat_core::{
        DirectedFrame, IdentityProcessor, ObserverClock, ObserverOptions, PipelineWorker,
        WorkerReport,
    };
    use pipecat_media_transport::recording::{
        TurnAudioData, TurnRecordingLoss, TurnRecordingStatus,
    };
    use pipecat_observers::{TurnTrackingConfig, TurnTrackingObserver};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct ObservationClock(AtomicU64);
    impl ObserverClock for ObservationClock {
        fn now_ns(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn tracker() -> Arc<TurnTrackingObserver> {
        Arc::new(
            TurnTrackingObserver::with_config(TurnTrackingConfig {
                // These tests resolve native boundaries; no silence timer is needed.
                turn_end_timeout: Duration::from_secs(60),
                ..Default::default()
            })
            .unwrap(),
        )
    }

    async fn start(
        recorder: &Arc<AudioBufferProcessor>,
        tracker: &Arc<TurnTrackingObserver>,
        clock: Arc<ObservationClock>,
    ) -> Result<PipelineWorker, String> {
        let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor), recorder.clone()])
            .with_observer_clock(clock);
        tracker
            .register(
                &mut pipeline,
                ObserverOptions {
                    capacity: 512,
                    ..Default::default()
                },
            )
            .map_err(str::to_owned)?;
        let mut worker = pipeline
            .start(PipelineConfig::default())
            .await
            .map_err(|error| error.to_string())?;
        let start = worker.recv().await.ok_or("missing Start")?;
        if !matches!(start.frame.data, FrameData::Start(_))
            || start.direction != Direction::Downstream
        {
            return Err(format!("unexpected pipeline Start: {start:?}"));
        }
        Ok(worker)
    }

    async fn forward(worker: &mut PipelineWorker, data: FrameData) -> Result<(), String> {
        let frame = Frame::new(data);
        let expected = frame.clone();
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .map_err(|error| error.to_string())?;
        let output = worker.recv().await.ok_or("missing forwarded frame")?;
        if output.frame != expected || output.direction != Direction::Downstream {
            return Err(format!("changed frame or direction: {output:?}"));
        }
        Ok(())
    }

    async fn stop(
        worker: PipelineWorker,
    ) -> Result<(Frame, Vec<DirectedFrame>, WorkerReport), String> {
        let frame = Frame::new(FrameData::Stop);
        let expected = frame.clone();
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .map_err(|error| error.to_string())?;
        let (tail, report) = worker.collect().await.map_err(|error| error.to_string())?;
        Ok((expected, tail, report))
    }

    fn assert_stopped(expected: Frame, tail: Vec<DirectedFrame>, report: WorkerReport) {
        assert_eq!(report.state, WorkerState::Stopped);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.total_errors, 0);
        assert_eq!(report.dropped_frames, 0);
        assert_eq!(tail.len(), 1, "Stop must be forwarded once: {tail:?}");
        assert_eq!(tail[0].frame, expected);
        assert_eq!(tail[0].direction, Direction::Downstream);
    }

    fn primary(events: &[RecordingEvent]) -> Vec<RecordingEvent> {
        events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    RecordingEvent::RecordingStarted
                        | RecordingEvent::AudioData { .. }
                        | RecordingEvent::TrackAudioData { .. }
                        | RecordingEvent::RecordingStopped
                )
            })
            .cloned()
            .collect()
    }

    fn numbered_event(user: bool, number: u64, session: u64, samples: &[i16]) -> RecordingEvent {
        let data = TurnAudioData {
            turn_number: number,
            session_id: Some(session),
            audio: encode_pcm16_le(samples).into(),
            sample_rate: 16000,
            num_channels: 1,
        };
        if user {
            RecordingEvent::UserTurnAudio(data)
        } else {
            RecordingEvent::BotTurnAudio(data)
        }
    }

    #[tokio::test]
    async fn plain_configuration_rejects_tracker_attachment() {
        let recorder = recorder(plain_config());
        let tracker = tracker();
        let attachment = recorder.attach_turn_tracker(&tracker);
        let status = recorder.turn_recording_status();
        let cleanup = recorder.cleanup().await;
        tracker.cleanup().await;

        cleanup.unwrap();
        assert!(attachment.is_err());
        assert_eq!(status.unwrap(), TurnRecordingStatus::Unattached);
    }

    #[tokio::test]
    async fn attached_stop_keeps_numbered_cuts_and_primary_terminal_order() {
        let recorder = recorder(AudioBufferConfig {
            enable_turn_audio: true,
            event_capacity: 256,
            ..plain_config()
        });
        let mut events = recorder.subscribe();
        let tracker = tracker();
        let session = recorder.attach_turn_tracker(&tracker).unwrap();
        let clock = Arc::new(ObservationClock(AtomicU64::new(100)));
        let execution = tokio::time::timeout(Duration::from_secs(5), async {
            let mut worker = start(&recorder, &tracker, clock).await?;
            for (user_samples, bot_samples) in [([11, 12], [21, 22]), ([31, 32], [41, 42])] {
                forward(&mut worker, FrameData::UserStartedSpeaking).await?;
                forward(
                    &mut worker,
                    FrameData::InputAudio(
                        AudioRawFrame::new(encode_pcm16_le(&user_samples), 16000, 1)
                            .map_err(|error| error.to_string())?,
                    ),
                )
                .await?;
                forward(&mut worker, FrameData::UserStoppedSpeaking).await?;
                forward(&mut worker, FrameData::BotStartedSpeaking).await?;
                forward(
                    &mut worker,
                    FrameData::OutputAudio(
                        AudioRawFrame::new(encode_pcm16_le(&bot_samples), 16000, 1)
                            .map_err(|error| error.to_string())?,
                    ),
                )
                .await?;
                forward(&mut worker, FrameData::BotStoppedSpeaking).await?;
            }
            let stopped = stop(worker).await?;
            let state = snapshot(&recorder).map_err(|error| error.to_string())?;
            // Poll an explicit terminal condition under the outer timeout. Merely
            // yielding a fixed number of times cannot qualify delayed attribution.
            let status = loop {
                let status = recorder
                    .turn_recording_status()
                    .map_err(|error| error.to_string())?;
                if !matches!(status, TurnRecordingStatus::Attached { .. }) {
                    break status;
                }
                tokio::task::yield_now().await;
            };
            let emitted = drain(&mut events)?;
            Ok::<_, String>((stopped, state, status, emitted))
        })
        .await;
        let explicit_stop = recorder.stop_recording();
        let cleanup = recorder.cleanup().await;
        tracker.cleanup().await;
        let final_status = recorder.turn_recording_status();
        let repeated_events = drain(&mut events);

        explicit_stop.unwrap();
        cleanup.unwrap();
        let ((expected, tail, report), state, status, emitted) = execution
            .expect("bounded attached Stop and attribution")
            .unwrap();
        assert_stopped(expected, tail, report);
        assert_eq!(state, empty());
        assert_eq!(
            status,
            TurnRecordingStatus::Closed {
                session_id: session
            }
        );
        assert_eq!(final_status.unwrap(), status);
        assert_eq!(
            primary(&emitted),
            [
                RecordingEvent::RecordingStarted,
                RecordingEvent::AudioData {
                    audio: encode_pcm16_le(&[32, 34, 72, 74]).into(),
                    sample_rate: 16000,
                    num_channels: 1,
                },
                RecordingEvent::TrackAudioData {
                    user: encode_pcm16_le(&[11, 12, 31, 32]).into(),
                    bot: encode_pcm16_le(&[21, 22, 41, 42]).into(),
                    sample_rate: 16000,
                    num_channels: 1,
                },
                RecordingEvent::RecordingStopped,
            ]
        );
        let numbered: Vec<_> = emitted
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    RecordingEvent::UserTurnAudio(_) | RecordingEvent::BotTurnAudio(_)
                )
            })
            .cloned()
            .collect();
        assert_eq!(
            numbered,
            [
                numbered_event(true, 1, session, &[11, 12]),
                numbered_event(false, 1, session, &[21, 22]),
                numbered_event(true, 2, session, &[31, 32]),
                numbered_event(false, 2, session, &[41, 42]),
            ]
        );
        assert!(!emitted
            .iter()
            .any(|event| matches!(event, RecordingEvent::TurnAttributionLost(_))));
        assert!(repeated_events.unwrap().is_empty());
    }

    #[tokio::test]
    async fn invalidated_attachment_still_flushes_and_stops_primary_recording() {
        let recorder = recorder(AudioBufferConfig {
            enable_turn_audio: true,
            event_capacity: 256,
            ..plain_config()
        });
        let mut events = recorder.subscribe();
        let tracker = tracker();
        let session = recorder.attach_turn_tracker(&tracker).unwrap();
        let clock = Arc::new(ObservationClock(AtomicU64::new(100)));
        let execution = tokio::time::timeout(Duration::from_secs(5), async {
            let mut worker = start(&recorder, &tracker, clock.clone()).await?;
            // An intentionally regressing observation clock invalidates the real
            // registered tracker, without private state or synthetic turn events.
            clock.0.store(0, Ordering::SeqCst);
            forward(&mut worker, FrameData::UserStartedSpeaking).await?;
            let status = loop {
                let status = recorder
                    .turn_recording_status()
                    .map_err(|error| error.to_string())?;
                if matches!(status, TurnRecordingStatus::Disabled { .. }) {
                    break status;
                }
                tokio::task::yield_now().await;
            };
            forward(&mut worker, output_audio()).await?;
            let before_stop = snapshot(&recorder).map_err(|error| error.to_string())?;
            let stopped = stop(worker).await?;
            let after_stop = snapshot(&recorder).map_err(|error| error.to_string())?;
            let emitted = drain(&mut events)?;
            Ok::<_, String>((status, before_stop, stopped, after_stop, emitted))
        })
        .await;
        let explicit_stop = recorder.stop_recording();
        let cleanup = recorder.cleanup().await;
        tracker.cleanup().await;
        let final_status = recorder.turn_recording_status();
        let repeated_events = drain(&mut events);

        explicit_stop.unwrap();
        cleanup.unwrap();
        let (status, before_stop, (expected, tail, report), after_stop, emitted) = execution
            .expect("bounded invalidation and attached Stop")
            .unwrap();
        assert_eq!(
            status,
            TurnRecordingStatus::Disabled {
                session_id: session,
                reason: TurnRecordingLoss::TrackerInvalidated,
            }
        );
        assert_eq!(before_stop, retained());
        assert_stopped(expected, tail, report);
        assert_eq!(after_stop, empty());
        assert_eq!(final_status.unwrap(), status);
        let mut expected = vec![RecordingEvent::RecordingStarted];
        expected.extend(final_events());
        assert_eq!(primary(&emitted), expected);
        let lost: Vec<_> = emitted
            .iter()
            .filter(|event| matches!(event, RecordingEvent::TurnAttributionLost(_)))
            .collect();
        assert_eq!(
            lost,
            [&RecordingEvent::TurnAttributionLost(
                TurnRecordingLoss::TrackerInvalidated
            )]
        );
        assert!(!emitted.iter().any(|event| matches!(
            event,
            RecordingEvent::UserTurnAudio(_) | RecordingEvent::BotTurnAudio(_)
        )));
        assert!(repeated_events.unwrap().is_empty());
    }
}
