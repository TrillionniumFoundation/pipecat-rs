use pipecat_core::Frame;
use std::collections::HashSet;

#[test]
fn native_observation_identity_ignores_public_ids_and_is_not_wire_data() {
    let frame = Frame::text("private content");
    let token = frame.observation_token();
    assert!(token.is_alive());
    assert_eq!(token, frame.clone().observation_token());
    let mut duplicate = frame.duplicate();
    duplicate.id = frame.id;
    assert_ne!(token, duplicate.observation_token());
    let encoded = serde_json::to_string(&frame).unwrap();
    assert!(!encoded.contains("observation"));
    let decoded: Frame = serde_json::from_str(&encoded).unwrap();
    assert_eq!(frame.id, decoded.id);
    assert_ne!(token, decoded.observation_token());
    let mut reused = Frame::text("other content");
    reused.id = frame.id;
    assert_ne!(token, reused.observation_token());
    let mut tokens = HashSet::new();
    tokens.insert(token.clone());
    assert!(tokens.contains(&frame.observation_token()));
    drop(frame);
    assert!(!token.is_alive());
    assert_eq!(format!("{token:?}"), "FrameObservationToken(<local>)");
    assert_ne!(token, Frame::text("").observation_token());
}

#[tokio::test]
async fn pipeline_captures_native_identity_before_forwarding_without_public_id_dedup() {
    use pipecat_core::*;
    use std::sync::{Arc, Mutex};
    #[derive(Default)]
    struct Collector(Mutex<Vec<ObservationEvent>>);
    #[async_trait]
    impl PipelineObserver for Collector {
        async fn on_event(&self, event: ObservationEvent) {
            self.0.lock().unwrap().push(event);
        }
    }
    let collector = Arc::new(Collector::default());
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    pipeline.add_observer(collector.clone(), ObserverOptions::default());
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    let frame = Frame::new(FrameData::UserStartedSpeaking);
    let token = frame.observation_token();
    let mut duplicate = frame.duplicate();
    duplicate.id = frame.id;
    let decoded: Frame = serde_json::from_str(&serde_json::to_string(&frame).unwrap()).unwrap();
    for frame in [
        frame.clone(),
        frame.clone(),
        duplicate.clone(),
        decoded.clone(),
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        worker.recv().await.unwrap();
    }
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    let events = collector.0.lock().unwrap();
    let tokens: Vec<_> = events
        .iter()
        .filter_map(|event| match &*event.observation {
            PipelineObservation::Push {
                frame: observed,
                first_push: true,
                ..
            } if observed.kind == "UserStartedSpeakingFrame" => {
                assert_eq!(observed.id, frame.id);
                Some(observed.token.clone().unwrap())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        tokens,
        [
            token,
            duplicate.observation_token(),
            decoded.observation_token()
        ]
    );
}
