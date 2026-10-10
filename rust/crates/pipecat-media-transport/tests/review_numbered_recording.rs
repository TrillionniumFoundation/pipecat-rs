#![cfg(feature = "turn-recording")]
//! Adversarial native-lineage and PCM-retention regression fixtures.
use pipecat_audio::{decode_pcm16_le, encode_pcm16_le};
use pipecat_core::*;
use pipecat_media_transport::recording::*;
use pipecat_observers::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

struct FixedClock;
impl RecordingClock for FixedClock {
    fn now(&self) -> Duration {
        Duration::ZERO
    }
}
fn config() -> AudioBufferConfig {
    AudioBufferConfig {
        sample_rate: Some(16000),
        enable_turn_audio: true,
        auto_start_recording: true,
        event_capacity: 256,
        ..Default::default()
    }
}
fn audio(user: bool, samples: &[i16]) -> Frame {
    let audio = AudioRawFrame::new(encode_pcm16_le(samples), 16000, 1).unwrap();
    Frame::new(if user {
        FrameData::InputAudio(audio)
    } else {
        FrameData::OutputAudio(audio)
    })
}
async fn settle() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}
async fn send(worker: &mut PipelineWorker, frame: Frame) {
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(worker.recv().await.unwrap().frame.id, id);
    settle().await;
}
async fn control(worker: &mut PipelineWorker, data: FrameData) {
    send(worker, Frame::new(data)).await;
}
async fn start(
    config: AudioBufferConfig,
) -> (
    Arc<AudioBufferProcessor>,
    PipelineWorker,
    broadcast::Receiver<RecordingEvent>,
) {
    let recorder =
        Arc::new(AudioBufferProcessor::with_clock(config, Arc::new(FixedClock)).unwrap());
    let receiver = recorder.subscribe();
    let tracker = Arc::new(TurnTrackingObserver::new());
    recorder.attach_turn_tracker(&tracker).unwrap();
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor), recorder.clone()]);
    tracker
        .register(
            &mut pipeline,
            ObserverOptions {
                capacity: 512,
                ..Default::default()
            },
        )
        .unwrap();
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    settle().await;
    (recorder, worker, receiver)
}
async fn finish(worker: PipelineWorker) {
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}
fn drain(receiver: &mut broadcast::Receiver<RecordingEvent>) -> Vec<RecordingEvent> {
    let mut result = Vec::new();
    loop {
        match receiver.try_recv() {
            Ok(event) => result.push(event),
            Err(broadcast::error::TryRecvError::Closed | broadcast::error::TryRecvError::Empty) => {
                return result
            }
            Err(error) => panic!("unexpected recording event loss: {error}"),
        }
    }
}

#[tokio::test]
async fn replay_of_old_live_boundary_after_cut_retirement_disables_numbering() {
    let (recorder, mut worker, _events) = start(config()).await;
    let old = Frame::new(FrameData::UserStartedSpeaking);
    send(&mut worker, old.clone()).await;
    send(&mut worker, audio(true, &[1])).await;
    control(&mut worker, FrameData::UserStoppedSpeaking).await;
    control(&mut worker, FrameData::BotStartedSpeaking).await;
    send(&mut worker, audio(false, &[2])).await;
    control(&mut worker, FrameData::BotStoppedSpeaking).await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(true, &[3])).await;
    control(&mut worker, FrameData::UserStoppedSpeaking).await;
    // The old cut has been consumed, but its native logical frame is still live.
    send(&mut worker, old).await;
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled {
            reason: TurnRecordingLoss::RepeatedBoundary,
            ..
        }
    ));
    finish(worker).await;
}

#[tokio::test]
async fn replayed_bot_start_cannot_attribute_late_old_bot_pcm_to_new_turn() {
    let (recorder, mut worker, mut events) = start(config()).await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(true, &[1])).await;
    control(&mut worker, FrameData::UserStoppedSpeaking).await;
    let old_start = Frame::new(FrameData::BotStartedSpeaking);
    send(&mut worker, old_start.clone()).await;
    send(&mut worker, audio(false, &[2])).await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(true, &[3])).await;
    control(&mut worker, FrameData::UserStoppedSpeaking).await;
    send(&mut worker, old_start).await;
    send(&mut worker, audio(false, &[99])).await;
    control(&mut worker, FrameData::BotStoppedSpeaking).await;
    finish(worker).await;
    let emitted = drain(&mut events);
    assert!(!emitted.iter().any(|event| matches!(event, RecordingEvent::BotTurnAudio(data) if data.turn_number == 2 && decode_pcm16_le(&data.audio).unwrap().contains(&99))), "replayed old BotStartedSpeaking created bot PCM for a turn the tracker never observed speaking: {emitted:?}");
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled { .. }
    ));
}

#[tokio::test]
async fn numbered_pcm_cap_disables_attribution_without_dropping_primary_trigger_frame() {
    let (recorder, mut worker, mut events) = start(AudioBufferConfig {
        max_turn_bytes: 2,
        ..config()
    })
    .await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(true, &[1])).await;
    send(&mut worker, audio(true, &[2])).await;
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled { .. }
    ));
    recorder.flush().unwrap();
    let primary: Vec<_> = drain(&mut events)
        .into_iter()
        .filter_map(|event| match event {
            RecordingEvent::TrackAudioData { user, .. } => Some(decode_pcm16_le(&user).unwrap()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(primary, [1, 2]);
    finish(worker).await;
}

/// Preserve native tokens while delivering two rollover controls in reverse.
#[derive(Default)]
struct ReverseRollover {
    count: std::sync::atomic::AtomicUsize,
    held: std::sync::Mutex<Option<Frame>>,
    held_ready: tokio::sync::Notify,
}
#[async_trait]
impl FrameProcessor for ReverseRollover {
    fn name(&self) -> &str {
        "reverse-rollover"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::UserStartedSpeaking) {
            let number = self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if number == 1 {
                *self.held.lock().unwrap() = Some(frame);
                self.held_ready.notify_one();
                return Ok(());
            }
            if number == 2 {
                ctx.push(frame, direction).await?;
                let held = self.held.lock().unwrap().take().unwrap();
                return ctx.push(held, direction).await;
            }
        }
        ctx.push(frame, direction).await
    }
}

struct GateThirdUserStart {
    tracker: Arc<TurnTrackingObserver>,
    count: std::sync::atomic::AtomicUsize,
    open: std::sync::atomic::AtomicBool,
    wake: tokio::sync::Notify,
}
#[async_trait]
impl PipelineObserver for GateThirdUserStart {
    async fn on_event(&self, event: ObservationEvent) {
        if matches!(&*event.observation, PipelineObservation::Push { frame, first_push: true, .. } if frame.kind == "UserStartedSpeakingFrame")
            && self.count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 2
        {
            while !self.open.load(std::sync::atomic::Ordering::SeqCst) {
                let wake = self.wake.notified();
                if self.open.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                wake.await;
            }
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

async fn reversed_rollover(delayed_observer: bool) {
    let recorder =
        Arc::new(AudioBufferProcessor::with_clock(config(), Arc::new(FixedClock)).unwrap());
    let mut events = recorder.subscribe();
    let tracker = Arc::new(TurnTrackingObserver::new());
    recorder.attach_turn_tracker(&tracker).unwrap();
    let reverse = Arc::new(ReverseRollover::default());
    let mut pipeline = Pipeline::new(vec![
        Arc::new(IdentityProcessor),
        reverse.clone(),
        recorder.clone(),
    ]);
    let gate = Arc::new(GateThirdUserStart {
        tracker,
        count: std::sync::atomic::AtomicUsize::new(0),
        open: std::sync::atomic::AtomicBool::new(!delayed_observer),
        wake: tokio::sync::Notify::new(),
    });
    pipeline.add_observer(
        gate.clone(),
        ObserverOptions {
            capacity: 1024,
            observe_every_push: false,
            ..Default::default()
        },
    );
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    settle().await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(true, &[1])).await;
    control(&mut worker, FrameData::UserStoppedSpeaking).await;
    control(&mut worker, FrameData::BotStartedSpeaking).await;
    send(&mut worker, audio(false, &[2])).await;
    control(&mut worker, FrameData::BotStoppedSpeaking).await;
    let delayed = Frame::new(FrameData::UserStartedSpeaking);
    let delayed_id = delayed.id;
    worker
        .sender()
        .send(delayed, Direction::Downstream)
        .await
        .unwrap();
    reverse.held_ready.notified().await;
    settle().await;
    control(&mut worker, FrameData::BotStartedSpeaking).await;
    send(&mut worker, audio(false, &[22])).await;
    control(&mut worker, FrameData::BotStoppedSpeaking).await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    assert_eq!(worker.recv().await.unwrap().frame.id, delayed_id);
    settle().await;
    let emitted = drain(&mut events);
    assert!(!emitted.iter().any(|event| matches!(event, RecordingEvent::BotTurnAudio(data) if data.turn_number == 1 && decode_pcm16_le(&data.audio).unwrap().contains(&22))), "a later turn was included in turn one after native controls were reordered: {emitted:?}");
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled {
            reason: TurnRecordingLoss::InvalidBoundaryOrder,
            ..
        }
    ));
    gate.open.store(true, std::sync::atomic::Ordering::SeqCst);
    gate.wake.notify_waiters();
    finish(worker).await;
}

#[tokio::test]
async fn reversed_native_rollover_cuts_fail_closed_before_emitting_mixed_turn_audio() {
    reversed_rollover(false).await;
}

#[tokio::test]
async fn reordered_controls_fail_closed_even_before_later_tracker_event_arrives() {
    reversed_rollover(true).await;
}

#[tokio::test]
async fn terminal_end_closes_both_partial_runs_at_the_native_boundary() {
    let (recorder, mut worker, mut events) = start(config()).await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(true, &[11, 12])).await;
    control(&mut worker, FrameData::BotStartedSpeaking).await;
    send(&mut worker, audio(false, &[21, 22])).await;
    finish(worker).await;
    let mut user = Vec::new();
    let mut bot = Vec::new();
    for event in drain(&mut events) {
        match event {
            RecordingEvent::UserTurnAudio(data) => {
                assert_eq!(data.turn_number, 1);
                user.extend(decode_pcm16_le(&data.audio).unwrap());
            }
            RecordingEvent::BotTurnAudio(data) => {
                assert_eq!(data.turn_number, 1);
                bot.extend(decode_pcm16_le(&data.audio).unwrap());
            }
            RecordingEvent::TurnAttributionLost(reason) => {
                panic!("terminal boundary lost: {reason:?}")
            }
            _ => {}
        }
    }
    assert_eq!(user, [11, 12]);
    assert_eq!(bot, [21, 22]);
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Closed { .. }
    ));
}

#[tokio::test]
async fn recording_restart_keeps_primary_recording_but_never_restores_numbering() {
    let (recorder, mut worker, mut events) = start(config()).await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(true, &[1])).await;
    control(&mut worker, FrameData::AudioBufferStopRecording).await;
    control(&mut worker, FrameData::AudioBufferStartRecording).await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(true, &[2])).await;
    finish(worker).await;
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled {
            reason: TurnRecordingLoss::RecordingRestarted,
            ..
        }
    ));
    let emitted = drain(&mut events);
    assert!(!emitted.iter().any(|event| matches!(
        event,
        RecordingEvent::UserTurnAudio(_) | RecordingEvent::BotTurnAudio(_)
    )));
    let primary: Vec<_> = emitted
        .iter()
        .filter_map(|event| match event {
            RecordingEvent::TrackAudioData { user, .. } => Some(decode_pcm16_le(user).unwrap()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(primary, [1, 2]);
    assert_eq!(
        emitted
            .iter()
            .filter(|event| matches!(event, RecordingEvent::TurnAttributionLost(_)))
            .count(),
        1
    );
}

#[derive(Default)]
struct SwapMiddleControls(std::sync::Mutex<Option<Frame>>);
#[async_trait]
impl FrameProcessor for SwapMiddleControls {
    fn name(&self) -> &str {
        "swap-middle-controls"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::UserStoppedSpeaking) {
            *self.0.lock().unwrap() = Some(frame);
            return Ok(());
        }
        if matches!(frame.data, FrameData::BotStartedSpeaking) {
            ctx.push(frame, direction).await?;
            let held = self.0.lock().unwrap().take().unwrap();
            return ctx.push(held, direction).await;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn control_permutation_is_rejected_even_when_boundary_predecessor_is_restored() {
    let recorder =
        Arc::new(AudioBufferProcessor::with_clock(config(), Arc::new(FixedClock)).unwrap());
    let mut events = recorder.subscribe();
    let tracker = Arc::new(TurnTrackingObserver::new());
    recorder.attach_turn_tracker(&tracker).unwrap();
    let mut pipeline = Pipeline::new(vec![
        Arc::new(IdentityProcessor),
        Arc::new(SwapMiddleControls::default()),
        recorder.clone(),
    ]);
    tracker
        .register(
            &mut pipeline,
            ObserverOptions {
                capacity: 1024,
                ..Default::default()
            },
        )
        .unwrap();
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    settle().await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    send(&mut worker, audio(true, &[1])).await;
    let stopped = Frame::new(FrameData::UserStoppedSpeaking);
    let stopped_id = stopped.id;
    worker
        .sender()
        .send(stopped, Direction::Downstream)
        .await
        .unwrap();
    control(&mut worker, FrameData::BotStartedSpeaking).await;
    assert_eq!(worker.recv().await.unwrap().frame.id, stopped_id);
    send(&mut worker, audio(false, &[2])).await;
    control(&mut worker, FrameData::BotStoppedSpeaking).await;
    control(&mut worker, FrameData::UserStartedSpeaking).await;
    finish(worker).await;
    assert!(matches!(
        recorder.turn_recording_status().unwrap(),
        TurnRecordingStatus::Disabled {
            reason: TurnRecordingLoss::InvalidBoundaryOrder,
            ..
        }
    ));
    assert!(!drain(&mut events).iter().any(|event| matches!(
        event,
        RecordingEvent::UserTurnAudio(_) | RecordingEvent::BotTurnAudio(_)
    )));
}
