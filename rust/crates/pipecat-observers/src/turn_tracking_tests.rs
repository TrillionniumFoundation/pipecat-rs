use super::*;
use pipecat_core::{
    Direction, IdentityProcessor, ObservedFrame, ObservedProcessor, PipelineConfig,
};
use tokio::sync::Semaphore;

fn event(sequence: u64, kind: &'static str, dropped_before: u64) -> ObservationEvent {
    ObservationEvent {
        sequence,
        dropped_before,
        observation: Arc::new(PipelineObservation::Push {
            source: ObservedProcessor {
                runtime_id: 1,
                index: 0,
                name: "fixture".into(),
            },
            destination: None,
            frame: ObservedFrame {
                token: None,
                id: sequence,
                kind,
                interruptible: true,
                vad_stop_secs: None,
                metrics: vec![],
                omitted_metrics: 0,
            },
            direction: Direction::Downstream,
            first_push: true,
            timestamp_ns: sequence,
        }),
    }
}
struct GateObserver(Semaphore);
#[async_trait]
impl PipelineObserver for GateObserver {
    async fn on_event(&self, _: ObservationEvent) {
        self.0.add_permits(1);
        std::future::pending::<()>().await;
    }
}
#[tokio::test(start_paused = true)]
async fn live_loss_at_deadline_invalidates_without_a_later_delivered_observation() {
    let tracker = TurnTrackingObserver::new();
    let mut receiver = tracker.subscribe();
    tracker.on_event(event(0, "StartFrame", 0)).await;
    tracker
        .on_event(event(1, "BotStartedSpeakingFrame", 0))
        .await;
    tracker
        .on_event(event(2, "BotStoppedSpeakingFrame", 0))
        .await;
    assert!(tracker.next_deadline().is_some());
    // Obtain a real handle with queue loss, but no subsequent delivered event.
    let gate = Arc::new(GateObserver(Semaphore::new(0)));
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    let health = pipeline.add_observer(
        gate.clone(),
        ObserverOptions {
            capacity: 1,
            ..Default::default()
        },
    );
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    gate.0.acquire().await.unwrap().forget();
    let dropped = health.stats().dropped;
    assert!(dropped > 0);
    tracker.shared.state.lock().unwrap().health = Some(health);
    tokio::time::advance(Duration::from_millis(2500)).await;
    tracker.on_timeout().await;
    assert!(matches!(
        receiver.try_recv().unwrap(),
        TurnTrackingEvent::Started { turn_number: 1, .. }
    ));
    assert_eq!(
        receiver.try_recv().unwrap(),
        TurnTrackingEvent::ObservationLoss {
            dropped_total: dropped,
            invalidated_turn: Some(1)
        }
    );
    assert!(receiver.try_recv().is_err());
    assert!(tracker.next_deadline().is_none());
    // Older already-queued observations cannot resurrect pre-loss history.
    tracker
        .on_event(event(3, "UserStartedSpeakingFrame", 0))
        .await;
    assert!(receiver.try_recv().is_err());
    tracker
        .on_event(event(4 + dropped, "UserStartedSpeakingFrame", dropped))
        .await;
    assert_eq!(
        receiver.try_recv().unwrap(),
        TurnTrackingEvent::Started {
            boundary: None,
            turn_number: 2,
            timestamp_ns: 4 + dropped,
            numbering_complete: false
        }
    );
    assert!(receiver.try_recv().is_err());
    tracker.cleanup().await;
    worker.cancel("fixture complete").await.unwrap();
    worker.collect().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn late_stop_with_old_timestamp_after_interruption_is_suppressed() {
    let tracker = TurnTrackingObserver::new();
    let mut receiver = tracker.subscribe();
    tracker.on_event(event(0, "StartFrame", 0)).await;
    tracker
        .on_event(event(1, "BotStartedSpeakingFrame", 0))
        .await;
    tracker
        .on_event(event(2, "UserStartedSpeakingFrame", 0))
        .await;
    let mut late = event(3, "BotStoppedSpeakingFrame", 0);
    if let PipelineObservation::Push { timestamp_ns, .. } = Arc::make_mut(&mut late.observation) {
        *timestamp_ns = 1;
    }
    tracker.on_event(late).await;
    assert_eq!(tracker.stats().active_turn, Some(2));
    assert!(tracker.stats().numbering_complete);
    assert!(tracker.next_deadline().is_none());
    assert!(matches!(
        receiver.try_recv().unwrap(),
        TurnTrackingEvent::Started { .. }
    ));
    assert!(matches!(
        receiver.try_recv().unwrap(),
        TurnTrackingEvent::Ended {
            was_interrupted: true,
            ..
        }
    ));
    assert!(matches!(
        receiver.try_recv().unwrap(),
        TurnTrackingEvent::Started { turn_number: 2, .. }
    ));
    assert!(receiver.try_recv().is_err());
    tracker.cleanup().await;
}
