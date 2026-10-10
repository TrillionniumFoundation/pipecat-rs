use pipecat_core::*;
use pipecat_observers::*;
use std::sync::Arc;

fn event(sequence: u64, direction: Direction, kind: &'static str) -> ObservationEvent {
    ObservationEvent {
        sequence,
        dropped_before: 0,
        observation: Arc::new(PipelineObservation::Push {
            source: ObservedProcessor {
                runtime_id: 1,
                index: 0,
                name: "review".into(),
            },
            destination: None,
            frame: ObservedFrame {
                id: sequence,
                token: None,
                kind,
                interruptible: false,
                vad_stop_secs: None,
                metrics: vec![],
                omitted_metrics: 0,
            },
            direction,
            first_push: true,
            timestamp_ns: sequence,
        }),
    }
}

macro_rules! review_observer {
    ($upstream:ident, $replay:ident, $observer:ty) => {
        #[tokio::test]
        async fn $upstream() {
            for kind in ["EndFrame", "StopFrame", "CancelFrame"] {
                let observer = <$observer>::new();
                observer.on_event(event(0, Direction::Upstream, kind)).await;
                assert!(
                    !observer.stats().closed,
                    "upstream {kind} cannot terminate a live pipeline observer"
                );
                observer
                    .on_event(event(1, Direction::Downstream, kind))
                    .await;
                assert!(observer.stats().closed);
            }
        }
        #[tokio::test]
        async fn $replay() {
            for kind in ["EndFrame", "StopFrame", "CancelFrame"] {
                let observer = <$observer>::new();
                observer
                    .on_event(event(0, Direction::Downstream, "Fixture"))
                    .await;
                observer
                    .on_event(event(0, Direction::Downstream, kind))
                    .await;
                assert!(
                    !observer.stats().closed,
                    "rejected replay {kind} cannot close observer"
                );
                observer
                    .on_event(event(1, Direction::Downstream, kind))
                    .await;
                assert!(observer.stats().closed);
            }
        }
    };
}
review_observer!(
    upstream_does_not_close_error,
    replay_does_not_close_error,
    ErrorObserver
);
review_observer!(
    upstream_does_not_close_function,
    replay_does_not_close_function,
    FunctionCallObserver
);
review_observer!(
    upstream_does_not_close_speaking,
    replay_does_not_close_speaking,
    SpeakingObserver
);
