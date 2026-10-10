use pipecat_core::{
    Direction, ErrorFrame, Frame, FrameData, IdentityProcessor, Pipeline, PipelineConfig,
};
use std::sync::Arc;

#[tokio::test]
async fn cloned_deserialized_errors_keep_identity_when_history_is_disabled() {
    let encoded =
        serde_json::to_value(Frame::new(FrameData::Error(ErrorFrame::new("observed")))).unwrap();
    let decoded: Frame = serde_json::from_value(encoded.clone()).unwrap();
    assert_eq!(serde_json::to_value(&decoded).unwrap(), encoded);
    let worker = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .start(PipelineConfig {
            error_history_capacity: 0,
            ..Default::default()
        })
        .await
        .unwrap();
    let sender = worker.sender();
    sender
        .send(decoded.clone(), Direction::Downstream)
        .await
        .unwrap();
    sender.send(decoded, Direction::Downstream).await.unwrap();
    sender.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(
        report.total_errors, 1,
        "two clones are the same logical error"
    );
    assert_eq!(report.evicted_errors, 1);
}
