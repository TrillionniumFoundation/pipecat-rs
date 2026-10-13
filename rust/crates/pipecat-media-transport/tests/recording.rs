use std::sync::{Arc, Mutex};
use std::time::Duration;

use pipecat_audio::{decode_pcm16_le, encode_pcm16_le, resample_pcm16};
use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, Pipeline, PipelineConfig, StartFrame,
};
use pipecat_media_transport::recording::{
    AudioBufferConfig, AudioBufferProcessor, RecordingClock, RecordingEvent,
};
use tokio::sync::broadcast;

#[derive(Default)]
struct Clock(Mutex<Duration>);
impl Clock {
    fn set(&self, millis: u64) {
        *self.0.lock().unwrap() = Duration::from_millis(millis);
    }
}
impl RecordingClock for Clock {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
}
fn configured() -> AudioBufferConfig {
    AudioBufferConfig {
        sample_rate: Some(16000),
        num_channels: 2,
        ..Default::default()
    }
}
fn recorder(config: AudioBufferConfig) -> (AudioBufferProcessor, Arc<Clock>) {
    let clock = Arc::new(Clock::default());
    (
        AudioBufferProcessor::with_clock(config, clock.clone()).unwrap(),
        clock,
    )
}
fn audio(user: bool, samples: &[i16]) -> FrameData {
    at_rate(user, samples, 16000)
}
fn at_rate(user: bool, samples: &[i16], rate: u32) -> FrameData {
    let frame = AudioRawFrame::new(encode_pcm16_le(samples), rate, 1).unwrap();
    if user {
        FrameData::InputAudio(frame)
    } else {
        FrameData::OutputAudio(frame)
    }
}
fn drain(receiver: &mut broadcast::Receiver<RecordingEvent>) -> Vec<RecordingEvent> {
    let mut events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        events.push(event);
    }
    events
}
fn tracks(events: &[RecordingEvent]) -> (Vec<i16>, Vec<i16>) {
    for event in events {
        if let RecordingEvent::TrackAudioData { user, bot, .. } = event {
            return (
                decode_pcm16_le(user).unwrap(),
                decode_pcm16_le(bot).unwrap(),
            );
        }
    }
    panic!("missing track event")
}

#[test]
fn default_is_idle_auto_start_uses_pipeline_rate_and_manual_start_needs_rate() {
    let (processor, _) = recorder(AudioBufferConfig::default());
    assert!(processor.start_recording().is_err());
    processor
        .handle_frame(&FrameData::Start(StartFrame::default()))
        .unwrap();
    assert!(!processor.is_recording().unwrap());
    processor.handle_frame(&audio(true, &[1, 2])).unwrap();
    assert!(!processor.has_audio().unwrap());
    let (processor, _) = recorder(AudioBufferConfig {
        auto_start_recording: true,
        ..Default::default()
    });
    let mut events = processor.subscribe();
    processor
        .handle_frame(&FrameData::Start(StartFrame {
            audio_out_sample_rate: 8000,
            ..Default::default()
        }))
        .unwrap();
    assert_eq!(processor.sample_rate().unwrap(), 8000);
    assert!(processor.is_recording().unwrap());
    assert_eq!(events.try_recv().unwrap(), RecordingEvent::RecordingStarted);
}

#[test]
fn repeated_start_preserves_audio_and_stop_flushes_before_stopped_once() {
    let (processor, _) = recorder(configured());
    let mut events = processor.subscribe();
    processor.stop_recording().unwrap();
    processor.start_recording().unwrap();
    processor.handle_frame(&audio(true, &[100, -100])).unwrap();
    processor.start_recording().unwrap();
    processor.stop_recording().unwrap();
    processor.stop_recording().unwrap();
    let events = drain(&mut events);
    assert_eq!(events.len(), 4);
    assert_eq!(events[0], RecordingEvent::RecordingStarted);
    assert_eq!(events[3], RecordingEvent::RecordingStopped);
    assert_eq!(tracks(&events), (vec![100, -100], vec![0, 0]));
    assert!(!processor.has_audio().unwrap());
    assert!(!processor.is_recording().unwrap());
    if let RecordingEvent::AudioData {
        audio,
        num_channels,
        ..
    } = &events[1]
    {
        assert_eq!(*num_channels, 2);
        assert_eq!(decode_pcm16_le(audio).unwrap(), [100, 0, -100, 0]);
    } else {
        panic!("expected audio before tracks");
    }
}

#[test]
fn user_and_bot_only_threshold_flush_aligns_tracks() {
    for user in [true, false] {
        let (processor, _) = recorder(AudioBufferConfig {
            buffer_size: 4,
            ..configured()
        });
        let mut events = processor.subscribe();
        processor.start_recording().unwrap();
        processor.handle_frame(&audio(user, &[100, -100])).unwrap();
        let (left, right) = tracks(&drain(&mut events));
        assert_eq!(if user { left.clone() } else { right.clone() }, [100, -100]);
        assert_eq!(if user { right } else { left }, [0, 0]);
        assert_eq!(processor.buffered_bytes().unwrap(), (0, 0));
    }
}

#[test]
fn mono_mix_saturates_and_stereo_merge_pads_before_stop() {
    let (processor, _) = recorder(AudioBufferConfig {
        num_channels: 1,
        ..configured()
    });
    let mut events = processor.subscribe();
    processor.start_recording().unwrap();
    processor
        .handle_frame(&audio(true, &[30000, -30000]))
        .unwrap();
    processor
        .handle_frame(&audio(false, &[30000, -30000]))
        .unwrap();
    processor.stop_recording().unwrap();
    let events = drain(&mut events);
    assert!(events.iter().any(|event| matches!(event, RecordingEvent::AudioData { audio, num_channels:1, .. } if decode_pcm16_le(audio).unwrap() == [32767,-32768])));
    let (processor, _) = recorder(configured());
    processor.start_recording().unwrap();
    processor.handle_frame(&audio(true, &[10])).unwrap();
    assert_eq!(
        decode_pcm16_le(&processor.merge_audio_buffers().unwrap()).unwrap(),
        [10, 0]
    );
}

#[test]
fn speaking_guards_prevent_mid_utterance_silence_and_resume_after_stop() {
    for user in [true, false] {
        let (processor, _) = recorder(configured());
        processor.start_recording().unwrap();
        processor
            .handle_frame(&if user {
                FrameData::BotStartedSpeaking
            } else {
                FrameData::UserStartedSpeaking
            })
            .unwrap();
        processor.handle_frame(&audio(!user, &[1])).unwrap();
        processor.handle_frame(&audio(user, &[2, 3, 4])).unwrap();
        processor.handle_frame(&audio(user, &[5])).unwrap();
        let lengths = processor.buffered_bytes().unwrap();
        assert_eq!(if user { lengths.1 } else { lengths.0 }, 2);
        processor
            .handle_frame(&if user {
                FrameData::BotStoppedSpeaking
            } else {
                FrameData::UserStoppedSpeaking
            })
            .unwrap();
        processor.handle_frame(&audio(user, &[6])).unwrap();
        let lengths = processor.buffered_bytes().unwrap();
        assert_eq!(if user { lengths.1 } else { lengths.0 }, 8);
    }
}

#[test]
fn wall_clock_gap_is_sample_aligned_and_symmetric() {
    for user in [true, false] {
        let (processor, clock) = recorder(configured());
        let mut events = processor.subscribe();
        processor.start_recording().unwrap();
        clock.set(5000);
        processor.handle_frame(&audio(user, &[7; 160])).unwrap();
        assert_eq!(
            processor.buffered_bytes().unwrap(),
            if user { (320, 0) } else { (0, 320) }
        );
        clock.set(6000);
        processor.handle_frame(&audio(user, &[9; 160])).unwrap();
        processor.stop_recording().unwrap();
        let (left, right) = tracks(&drain(&mut events));
        let active = if user { left } else { right };
        assert_eq!(active.len(), 16160);
        assert_eq!(&active[..160], &[7; 160]);
        assert!(active[160..16000].iter().all(|sample| *sample == 0));
        assert_eq!(&active[16000..], &[9; 160]);
    }
}

#[test]
fn jitter_and_backwards_clock_do_not_add_silence() {
    let (processor, clock) = recorder(configured());
    processor.start_recording().unwrap();
    clock.set(500);
    processor.handle_frame(&audio(true, &[1; 160])).unwrap();
    clock.set(600);
    processor.handle_frame(&audio(true, &[1; 160])).unwrap();
    clock.set(0);
    processor.handle_frame(&audio(true, &[1; 160])).unwrap();
    assert_eq!(processor.buffered_bytes().unwrap().0, 960);
}

#[test]
fn opposite_audio_and_flush_advance_gap_reference_without_double_counting() {
    for user in [true, false] {
        let (processor, clock) = recorder(configured());
        processor.start_recording().unwrap();
        processor.handle_frame(&audio(user, &[1; 160])).unwrap();
        clock.set(1000);
        processor.handle_frame(&audio(!user, &[1; 160])).unwrap();
        let before = processor.buffered_bytes().unwrap();
        clock.set(2000);
        processor.handle_frame(&audio(user, &[1; 160])).unwrap();
        let after = processor.buffered_bytes().unwrap();
        assert_eq!(
            if user {
                after.0 - before.0
            } else {
                after.1 - before.1
            },
            32000
        );
        processor.flush().unwrap();
        clock.set(2100);
        processor.handle_frame(&audio(user, &[1; 160])).unwrap();
        assert_eq!(
            if user {
                processor.buffered_bytes().unwrap().0
            } else {
                processor.buffered_bytes().unwrap().1
            },
            320
        );
    }
}

#[test]
fn stop_restart_clears_timestamps_speaking_and_resampler_state() {
    let (processor, clock) = recorder(configured());
    processor.start_recording().unwrap();
    processor
        .handle_frame(&FrameData::BotStartedSpeaking)
        .unwrap();
    processor.handle_frame(&audio(true, &[1])).unwrap();
    processor.stop_recording().unwrap();
    clock.set(100000);
    processor.start_recording().unwrap();
    processor.handle_frame(&audio(true, &[2, 3])).unwrap();
    processor.handle_frame(&audio(true, &[4])).unwrap();
    assert_eq!(processor.buffered_bytes().unwrap(), (6, 4));
}

#[test]
fn explicit_recording_stop_end_and_cancel_variants_stop_recording() {
    for terminal in [
        FrameData::AudioBufferStopRecording,
        Frame::end().data,
        Frame::cancel("test").data,
        FrameData::EndDetailed(pipecat_core::ReasonFrame { reason: None }),
        FrameData::CancelDetailed(pipecat_core::ReasonFrame { reason: None }),
    ] {
        let (processor, _) = recorder(configured());
        processor
            .handle_frame(&FrameData::AudioBufferStartRecording)
            .unwrap();
        processor.handle_frame(&audio(true, &[1])).unwrap();
        processor.handle_frame(&terminal).unwrap();
        assert!(!processor.is_recording().unwrap());
        assert!(!processor.has_audio().unwrap());
    }
}

#[test]
fn limits_reject_excessive_frames_gaps_and_malformed_audio_without_growing_buffers() {
    let (processor, clock) = recorder(AudioBufferConfig {
        max_track_bytes: 8,
        ..configured()
    });
    processor.start_recording().unwrap();
    processor.handle_frame(&audio(true, &[1, 2])).unwrap();
    assert!(processor.handle_frame(&audio(true, &[3, 4, 5])).is_err());
    assert_eq!(processor.buffered_bytes().unwrap().0, 4);
    clock.set(100000);
    assert!(processor.handle_frame(&audio(true, &[3])).is_err());
    assert_eq!(processor.buffered_bytes().unwrap().0, 4);
    let invalid = FrameData::InputAudio(AudioRawFrame {
        audio: vec![1],
        sample_rate: 16000,
        num_channels: 1,
        context_id: None,
    });
    assert!(processor.handle_frame(&invalid).is_err());
    let stereo = FrameData::InputAudio(AudioRawFrame::new(vec![0; 4], 16000, 2).unwrap());
    assert!(processor.handle_frame(&stereo).is_err());
}

#[test]
fn resampling_is_streaming_and_tail_is_emitted_before_stopped() {
    let (processor, _) = recorder(configured());
    let mut events = processor.subscribe();
    processor.start_recording().unwrap();
    let samples: Vec<i16> = (0..400).map(|index| (index * 29) as i16).collect();
    for chunk in samples.chunks(80) {
        processor.handle_frame(&at_rate(true, chunk, 8000)).unwrap();
    }
    processor.stop_recording().unwrap();
    let events = drain(&mut events);
    let (user, _) = tracks(&events);
    assert_eq!(
        encode_pcm16_le(&user),
        resample_pcm16(&encode_pcm16_le(&samples), 8000, 16000, 1).unwrap()
    );
    assert_eq!(events.last().unwrap(), &RecordingEvent::RecordingStopped);
}

#[test]
fn sample_rate_change_drains_old_resampler_and_empty_input_is_ignored() {
    let (processor, _) = recorder(configured());
    let mut events = processor.subscribe();
    processor.start_recording().unwrap();
    processor
        .handle_frame(&at_rate(true, &[5; 80], 8000))
        .unwrap();
    processor.handle_frame(&at_rate(true, &[], 24000)).unwrap();
    processor
        .handle_frame(&at_rate(true, &[7; 80], 16000))
        .unwrap();
    processor.stop_recording().unwrap();
    let (user, _) = tracks(&drain(&mut events));
    assert_eq!(user.len(), 240);
    assert!(user[..160].iter().all(|sample| *sample == 5));
    assert!(user[160..].iter().all(|sample| *sample == 7));
}

#[test]
fn bounded_events_report_lag_instead_of_blocking() {
    let (processor, _) = recorder(AudioBufferConfig {
        event_capacity: 2,
        buffer_size: 2,
        ..configured()
    });
    let mut events = processor.subscribe();
    processor.start_recording().unwrap();
    for _ in 0..10 {
        processor.handle_frame(&audio(true, &[1])).unwrap();
    }
    processor.stop_recording().unwrap();
    assert!(matches!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(_))
    ));
    assert_eq!(processor.buffered_bytes().unwrap(), (0, 0));
}

#[test]
fn turns_collect_runs_and_preserve_user_barge_in_for_next_turn() {
    let (processor, _) = recorder(AudioBufferConfig {
        enable_turn_audio: true,
        ..configured()
    });
    assert_eq!(
        processor.automatic_turn_tracking_supported(),
        cfg!(feature = "turn-recording")
    );
    let mut events = processor.subscribe();
    processor.start_recording().unwrap();
    for samples in [&[1, 2][..], &[3][..]] {
        processor
            .handle_frame(&FrameData::UserStartedSpeaking)
            .unwrap();
        processor.handle_frame(&audio(true, samples)).unwrap();
        processor
            .handle_frame(&FrameData::UserStoppedSpeaking)
            .unwrap();
    }
    processor
        .handle_frame(&FrameData::BotStartedSpeaking)
        .unwrap();
    processor.handle_frame(&audio(false, &[4, 5])).unwrap();
    processor
        .handle_frame(&FrameData::UserStartedSpeaking)
        .unwrap();
    processor.handle_frame(&audio(true, &[6, 7])).unwrap();
    processor.emit_turn_audio(10).unwrap();
    let output = drain(&mut events);
    assert!(output.iter().any(|event| matches!(event, RecordingEvent::UserTurnAudio(turn) if turn.turn_number == 10 && decode_pcm16_le(&turn.audio).unwrap() == [1,2,3])));
    assert!(output.iter().any(|event| matches!(event, RecordingEvent::BotTurnAudio(turn) if turn.turn_number == 10 && decode_pcm16_le(&turn.audio).unwrap() == [4,5])));
    processor
        .handle_frame(&FrameData::UserStoppedSpeaking)
        .unwrap();
    processor.emit_turn_audio(11).unwrap();
    let output = drain(&mut events);
    assert!(output.iter().any(|event| matches!(event, RecordingEvent::UserTurnAudio(turn) if turn.turn_number == 11 && decode_pcm16_le(&turn.audio).unwrap() == [6,7])));
    assert!(!output
        .iter()
        .any(|event| matches!(event, RecordingEvent::BotTurnAudio(_))));
}

#[test]
fn turn_limit_is_explicit_and_stop_preserves_audio_for_caller_supplied_turn() {
    let (processor, _) = recorder(AudioBufferConfig {
        enable_turn_audio: true,
        max_turn_bytes: 4,
        ..configured()
    });
    let mut events = processor.subscribe();
    processor.start_recording().unwrap();
    processor
        .handle_frame(&FrameData::UserStartedSpeaking)
        .unwrap();
    processor.handle_frame(&audio(true, &[1, 2])).unwrap();
    assert!(processor.handle_frame(&audio(true, &[3])).is_err());
    processor.stop_recording().unwrap();
    processor.emit_turn_audio(1).unwrap();
    let output = drain(&mut events);
    assert!(output.iter().any(|event| matches!(event, RecordingEvent::UserTurnAudio(turn) if decode_pcm16_le(&turn.audio).unwrap() == [1,2])));
}

#[tokio::test]
async fn real_pipeline_forwards_frames_and_directions_and_flushes_on_end() {
    let processor = Arc::new(
        AudioBufferProcessor::new(AudioBufferConfig {
            auto_start_recording: true,
            ..configured()
        })
        .unwrap(),
    );
    let mut events = processor.subscribe();
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    let frame = Frame::new(audio(true, &[8, 9]));
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Upstream)
        .await
        .unwrap();
    let output = worker.recv().await.unwrap();
    assert_eq!(output.frame.id, id);
    assert_eq!(output.direction, Direction::Upstream);
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let output = drain(&mut events);
    assert_eq!(tracks(&output).0, [8, 9]);
    assert_eq!(output.last().unwrap(), &RecordingEvent::RecordingStopped);
}

#[test]
fn pending_resampler_tail_is_reserved_before_accepting_audio() {
    for turn_audio in [false, true] {
        let (processor, _) = recorder(AudioBufferConfig {
            max_track_bytes: if turn_audio { 1000 } else { 4 },
            max_turn_bytes: 4,
            enable_turn_audio: turn_audio,
            ..configured()
        });
        let mut events = processor.subscribe();
        processor.start_recording().unwrap();
        processor
            .handle_frame(&FrameData::UserStartedSpeaking)
            .unwrap();
        processor.handle_frame(&at_rate(true, &[5], 8000)).unwrap();
        assert!(processor.handle_frame(&at_rate(true, &[7], 8000)).is_err());
        processor.stop_recording().unwrap();
        assert_eq!(tracks(&drain(&mut events)).0, [5, 5]);
    }
}

#[test]
fn idle_user_preroll_stays_bounded_and_is_kept_before_speech() {
    let (processor, _) = recorder(AudioBufferConfig {
        enable_turn_audio: true,
        max_turn_bytes: 6,
        ..configured()
    });
    let mut events = processor.subscribe();
    processor.start_recording().unwrap();
    for sample in 0..10 {
        processor.handle_frame(&audio(true, &[sample])).unwrap();
    }
    processor
        .handle_frame(&FrameData::UserStartedSpeaking)
        .unwrap();
    processor
        .handle_frame(&FrameData::UserStoppedSpeaking)
        .unwrap();
    processor.emit_turn_audio(1).unwrap();
    assert!(drain(&mut events).iter().any(|event| matches!(event, RecordingEvent::UserTurnAudio(turn) if decode_pcm16_le(&turn.audio).unwrap() == [7,8,9])));
}

#[test]
fn speech_start_does_not_assign_a_prior_bot_resampler_tail_to_the_new_run() {
    let (processor, _) = recorder(AudioBufferConfig {
        enable_turn_audio: true,
        max_turn_bytes: 4,
        ..configured()
    });
    let mut events = processor.subscribe();
    processor.start_recording().unwrap();
    processor
        .handle_frame(&at_rate(false, &[5; 10], 8000))
        .unwrap();
    processor
        .handle_frame(&FrameData::BotStartedSpeaking)
        .unwrap();
    processor.handle_frame(&audio(false, &[7, 8])).unwrap();
    processor.stop_recording().unwrap();
    processor.emit_turn_audio(1).unwrap();
    assert!(drain(&mut events).iter().any(|event| matches!(event, RecordingEvent::BotTurnAudio(turn) if decode_pcm16_le(&turn.audio).unwrap() == [7,8])));
}

#[tokio::test]
async fn real_pipeline_recording_controls_are_forwarded_with_identity() {
    let processor = Arc::new(AudioBufferProcessor::new(configured()).unwrap());
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    for control in [
        FrameData::AudioBufferStartRecording,
        FrameData::AudioBufferStopRecording,
    ] {
        let frame = Frame::new(control);
        let expected = frame.clone();
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        let received = worker.recv().await.unwrap();
        assert_eq!(received.frame, expected);
        assert_eq!(received.direction, Direction::Downstream);
    }
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}

#[test]
fn detailed_start_configuration_controls_auto_start() {
    let (processor, _) = recorder(AudioBufferConfig {
        auto_start_recording: true,
        ..Default::default()
    });
    processor
        .handle_frame(&FrameData::StartDetailed(
            pipecat_core::DetailedStartFrame {
                configuration: StartFrame {
                    audio_out_sample_rate: 8000,
                    ..Default::default()
                },
                enable_tracing: false,
                tracing_context: None,
            },
        ))
        .unwrap();
    assert!(processor.is_recording().unwrap());
    assert_eq!(processor.sample_rate().unwrap(), 8000);
}
