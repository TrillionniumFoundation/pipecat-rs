#![cfg(feature = "turn-recording")]
//! Native pipeline fixtures derived from test_audio_buffer_processor.py at
//! af9f9da9d7afddaa8811c0e0251b32566c980bdf. Samples represent observed PCM,
//! never a claim that a transport or device played the audio.
use pipecat_audio::{decode_pcm16_le, encode_pcm16_le};
use pipecat_core::*;
use pipecat_media_transport::recording::*;
use pipecat_observers::*;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{broadcast, Notify};

fn config() -> AudioBufferConfig {
    AudioBufferConfig {
        sample_rate: Some(16000),
        enable_turn_audio: true,
        auto_start_recording: true,
        event_capacity: 256,
        ..Default::default()
    }
}
fn audio(user: bool, samples: &[i16]) -> FrameData {
    let audio = AudioRawFrame::new(encode_pcm16_le(samples), 16000, 1).unwrap();
    if user {
        FrameData::InputAudio(audio)
    } else {
        FrameData::OutputAudio(audio)
    }
}
async fn send_frame(worker: &mut PipelineWorker, frame: Frame) {
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    let output = worker.recv().await.unwrap();
    assert_eq!(output.frame.id, id);
}
async fn send(worker: &mut PipelineWorker, frame: FrameData) {
    send_frame(worker, Frame::new(frame)).await;
}
async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}
async fn user(worker: &mut PipelineWorker, samples: &[i16]) {
    send(worker, FrameData::UserStartedSpeaking).await;
    send(worker, audio(true, samples)).await;
    send(worker, FrameData::UserStoppedSpeaking).await;
}
async fn bot(worker: &mut PipelineWorker, samples: &[i16]) {
    send(worker, FrameData::BotStartedSpeaking).await;
    send(worker, audio(false, samples)).await;
    send(worker, FrameData::BotStoppedSpeaking).await;
}
fn drain(receiver: &mut broadcast::Receiver<RecordingEvent>) -> Vec<RecordingEvent> {
    let mut out = vec![];
    loop {
        match receiver.try_recv() {
            Ok(event) => out.push(event),
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                return out
            }
            Err(error) => panic!("unexpected recording event loss: {error}"),
        }
    }
}
fn numbered(events: &[RecordingEvent]) -> Vec<(bool, u64, Vec<i16>)> {
    events
        .iter()
        .filter_map(|event| {
            let (user, data) = match event {
                RecordingEvent::UserTurnAudio(data) => (true, data),
                RecordingEvent::BotTurnAudio(data) => (false, data),
                _ => return None,
            };
            assert!(data.session_id.is_some());
            assert_eq!(data.sample_rate, 16000);
            assert_eq!(data.num_channels, 1);
            Some((
                user,
                data.turn_number,
                decode_pcm16_le(&data.audio).unwrap(),
            ))
        })
        .collect()
}
async fn start(
    recorder: &Arc<AudioBufferProcessor>,
    tracker: &Arc<TurnTrackingObserver>,
    capacity: usize,
) -> PipelineWorker {
    recorder.attach_turn_tracker(tracker).unwrap();
    // First-push observation may precede the recorder at the next processor.
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor), recorder.clone()]);
    tracker
        .register(
            &mut pipeline,
            ObserverOptions {
                capacity,
                ..Default::default()
            },
        )
        .unwrap();
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    settle().await;
    worker
}
async fn end(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

#[tokio::test]
async fn joins_runs_numbers_two_turns_and_stops_recording_before_terminal() {
    let recorder = Arc::new(AudioBufferProcessor::new(config()).unwrap());
    let mut events = recorder.subscribe();
    let tracker = Arc::new(TurnTrackingObserver::new());
    let mut worker = start(&recorder, &tracker, 512).await;
    user(&mut worker, &[1, 2]).await;
    user(&mut worker, &[3]).await;
    bot(&mut worker, &[4]).await;
    bot(&mut worker, &[5]).await;
    user(&mut worker, &[6, 7]).await;
    bot(&mut worker, &[8, 9]).await;
    send(&mut worker, FrameData::AudioBufferStopRecording).await;
    end(worker).await;
    assert_eq!(
        numbered(&drain(&mut events)),
        vec![
            (true, 1, vec![1, 2, 3]),
            (false, 1, vec![4, 5]),
            (true, 2, vec![6, 7]),
            (false, 2, vec![8, 9])
        ]
    );
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Closed { .. }
    ));
}
struct DelayedTracker {
    tracker: Arc<TurnTrackingObserver>,
    open: AtomicBool,
    wake: Notify,
}
impl DelayedTracker {
    fn release(&self) {
        self.open.store(true, Ordering::SeqCst);
        self.wake.notify_waiters();
    }
}
#[async_trait]
impl PipelineObserver for DelayedTracker {
    async fn on_event(&self, event: ObservationEvent) {
        while !self.open.load(Ordering::SeqCst) {
            let wake = self.wake.notified();
            if self.open.load(Ordering::SeqCst) {
                break;
            }
            wake.await;
        }
        self.tracker.on_event(event).await;
    }
    fn next_deadline(&self) -> Option<tokio::time::Instant> {
        self.tracker.next_deadline()
    }
    async fn on_timeout(&self) {
        self.tracker.on_timeout().await;
    }
    fn cancel(&self) {
        self.tracker.cancel();
    }
}
#[tokio::test]
async fn delayed_observer_uses_old_cut_not_new_turn_audio_and_keeps_barge_preroll() {
    let recorder = Arc::new(AudioBufferProcessor::new(config()).unwrap());
    let mut events = recorder.subscribe();
    let tracker = Arc::new(TurnTrackingObserver::new());
    recorder.attach_turn_tracker(&tracker).unwrap();
    let gate = Arc::new(DelayedTracker {
        tracker,
        open: AtomicBool::new(false),
        wake: Notify::new(),
    });
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor), recorder.clone()]);
    pipeline.add_observer(
        gate.clone(),
        ObserverOptions {
            capacity: 512,
            observe_every_push: false,
            ..Default::default()
        },
    );
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    user(&mut worker, &[1]).await;
    send(&mut worker, FrameData::BotStartedSpeaking).await;
    send(&mut worker, audio(false, &[2])).await;
    send(&mut worker, audio(true, &[3])).await; // open microphone pre-roll
    send(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(false, &[99])).await; // late queued bot PCM
    send(&mut worker, FrameData::BotStoppedSpeaking).await;
    send(&mut worker, audio(true, &[4])).await;
    send(&mut worker, FrameData::UserStoppedSpeaking).await;
    bot(&mut worker, &[5]).await;
    assert!(numbered(&drain(&mut events)).is_empty());
    gate.release();
    settle().await;
    let mut all = drain(&mut events);
    end(worker).await;
    all.extend(drain(&mut events));
    assert_eq!(
        numbered(&all),
        vec![
            (true, 1, vec![1]),
            (false, 1, vec![2]),
            (true, 2, vec![3, 4]),
            (false, 2, vec![5])
        ]
    );
    assert!(!all
        .iter()
        .any(|event| matches!(event, RecordingEvent::TurnAttributionLost(_))));
}
#[tokio::test(start_paused = true)]
async fn timeout_cuts_at_bot_stop_with_independent_recording_clock() {
    struct FixedClock;
    impl RecordingClock for FixedClock {
        fn now(&self) -> Duration {
            Duration::from_secs(9999)
        }
    }
    let recorder =
        Arc::new(AudioBufferProcessor::with_clock(config(), Arc::new(FixedClock)).unwrap());
    let mut events = recorder.subscribe();
    let tracker = Arc::new(TurnTrackingObserver::new());
    let mut worker = start(&recorder, &tracker, 512).await;
    user(&mut worker, &[1]).await;
    bot(&mut worker, &[2]).await;
    send(&mut worker, audio(true, &[3])).await;
    send(&mut worker, audio(false, &[99])).await;
    settle().await;
    tokio::time::advance(Duration::from_millis(2500)).await;
    settle().await;
    assert_eq!(
        numbered(&drain(&mut events)),
        vec![(true, 1, vec![1]), (false, 1, vec![2])]
    );
    user(&mut worker, &[4]).await;
    end(worker).await;
    assert_eq!(numbered(&drain(&mut events)), vec![(true, 2, vec![3, 4])]);
}
#[tokio::test]
async fn receiver_lag_permanently_disables_attribution_without_stopping_primary() {
    let recorder = Arc::new(AudioBufferProcessor::new(config()).unwrap());
    let mut events = recorder.subscribe();
    let tracker = Arc::new(
        TurnTrackingObserver::with_config(TurnTrackingConfig {
            event_capacity: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    let mut worker = start(&recorder, &tracker, 512).await;
    user(&mut worker, &[1]).await;
    bot(&mut worker, &[2]).await;
    let boundary = Frame::new(FrameData::UserStartedSpeaking);
    send_frame(&mut worker, boundary.clone()).await;
    settle().await;
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled {
            reason: TurnRecordingLoss::TrackerLagged,
            ..
        }
    ));
    send(&mut worker, audio(true, &[3])).await;
    send(&mut worker, FrameData::UserStoppedSpeaking).await;
    send_frame(&mut worker, boundary).await; // delayed old lineage after loss
    let mut reused = Frame::new(FrameData::UserStartedSpeaking);
    reused.id = 1;
    send_frame(&mut worker, reused).await;
    send(&mut worker, audio(true, &[4])).await;
    end(worker).await;
    let all = drain(&mut events);
    assert!(numbered(&all).is_empty());
    assert_eq!(
        all.iter()
            .filter(|event| matches!(event, RecordingEvent::TurnAttributionLost(_)))
            .count(),
        1
    );
    assert!(all.iter().any(|event| matches!(event,RecordingEvent::TrackAudioData { user,.. } if decode_pcm16_le(user).unwrap().contains(&4))));
}
#[tokio::test]
async fn pipeline_observation_loss_disables_without_inventing_a_turn() {
    let recorder = Arc::new(AudioBufferProcessor::new(config()).unwrap());
    let mut events = recorder.subscribe();
    let tracker = Arc::new(TurnTrackingObserver::new());
    let mut worker = start(&recorder, &tracker, 1).await;
    user(&mut worker, &[1]).await;
    settle().await;
    assert!(tracker.stats().observations_dropped > 0);
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled {
            reason: TurnRecordingLoss::ObservationLoss,
            ..
        }
    ));
    end(worker).await;
    assert!(numbered(&drain(&mut events)).is_empty());
}
#[tokio::test]
async fn fresh_native_controls_may_reuse_public_ids_but_live_clones_are_rejected() {
    let recorder = Arc::new(AudioBufferProcessor::new(config()).unwrap());
    let mut events = recorder.subscribe();
    let tracker = Arc::new(TurnTrackingObserver::new());
    let mut worker = start(&recorder, &tracker, 512).await;
    for payload in [
        FrameData::UserStartedSpeaking,
        audio(true, &[1]),
        FrameData::UserStoppedSpeaking,
        FrameData::BotStartedSpeaking,
        audio(false, &[2]),
        FrameData::BotStoppedSpeaking,
        FrameData::UserStartedSpeaking,
        audio(true, &[3]),
        FrameData::UserStoppedSpeaking,
    ] {
        let mut frame = Frame::new(payload);
        frame.id = 77;
        send_frame(&mut worker, frame).await;
    }
    end(worker).await;
    assert_eq!(
        numbered(&drain(&mut events)),
        vec![(true, 1, vec![1]), (false, 1, vec![2]), (true, 2, vec![3])]
    );
    let recorder = Arc::new(AudioBufferProcessor::new(config()).unwrap());
    let tracker = Arc::new(TurnTrackingObserver::new());
    let mut worker = start(&recorder, &tracker, 512).await;
    let boundary = Frame::new(FrameData::UserStartedSpeaking);
    send_frame(&mut worker, boundary.clone()).await;
    send_frame(&mut worker, boundary).await;
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled {
            reason: TurnRecordingLoss::RepeatedBoundary,
            ..
        }
    ));
    end(worker).await;
}
#[tokio::test]
async fn bounded_cuts_fail_closed_and_primary_no_longer_has_turn_byte_limit() {
    let recorder = Arc::new(
        AudioBufferProcessor::new(AudioBufferConfig {
            event_capacity: 2,
            max_turn_bytes: 2,
            ..config()
        })
        .unwrap(),
    );
    let tracker = Arc::new(TurnTrackingObserver::new());
    let mut worker = start(&recorder, &tracker, 512).await;
    send(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, FrameData::UserStartedSpeaking).await;
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled {
            reason: TurnRecordingLoss::BoundaryCapacity,
            ..
        }
    ));
    send(&mut worker, audio(true, &[1, 2, 3, 4])).await;
    assert_eq!(recorder.buffered_bytes().unwrap().0, 8);
    end(worker).await;
}
#[tokio::test]
async fn one_attachment_no_manual_numbering_and_fresh_sessions_are_distinct() {
    let tracker = Arc::new(TurnTrackingObserver::new());
    let first = Arc::new(AudioBufferProcessor::new(config()).unwrap());
    let second = Arc::new(AudioBufferProcessor::new(config()).unwrap());
    let first_id = first.attach_turn_tracker(&tracker).unwrap();
    let second_id = second.attach_turn_tracker(&tracker).unwrap();
    assert_ne!(first_id, second_id);
    assert!(first.attach_turn_tracker(&tracker).is_err());
    assert!(first.emit_turn_audio(1).is_err());
    assert!(first.handle_frame(&FrameData::UserStartedSpeaking).is_err());
    tracker.cancel();
    settle().await;
    assert!(first
        .attach_turn_tracker(&TurnTrackingObserver::new())
        .is_err());
}
#[tokio::test]
async fn cancel_and_drop_do_not_emit_later_numbered_audio() {
    for cancel in [false, true] {
        let recorder = Arc::new(AudioBufferProcessor::new(config()).unwrap());
        let mut events = recorder.subscribe();
        let tracker = Arc::new(TurnTrackingObserver::new());
        let mut worker = start(&recorder, &tracker, 512).await;
        user(&mut worker, &[1]).await;
        send(&mut worker, FrameData::BotStartedSpeaking).await;
        send(&mut worker, audio(false, &[2])).await;
        if cancel {
            worker.cancel("test cancellation").await.unwrap();
            worker.collect().await.unwrap();
        } else {
            drop(worker);
        }
        settle().await;
        let before = numbered(&drain(&mut events));
        assert!(before.len() <= 2); // a delivered Cancel may legitimately close its cut
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(numbered(&drain(&mut events)).is_empty());
        assert!(tracker.stats().closed);
        assert!(!tracker.stats().timer_pending);
    }
}
