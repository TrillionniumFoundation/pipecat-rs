//! Outward admission failure must not undo committed history or leak a record lease.
use pipecat_core::*;
use pipecat_services::Content;
use pipecat_turns::*;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn saturated_outward_context_path_abandons_notification_without_rollback() {
    let assistant = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        Default::default(),
    ));
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let mut worker = Pipeline::new(vec![assistant.clone()])
        .start(PipelineConfig {
            output_capacity: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Start(_)
    ));
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::llm("history survives output saturation")),
        FrameData::LlmResponseEnd,
    ] {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(3), worker.join())
        .await
        .expect("output saturation must settle");
    assert!(matches!(result, Err(PipelineError::Backpressure(_))));
    assert_eq!(
        assistant.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("history survives output saturation".into()))
    );
    assert!(
        receiver.try_recv().is_none(),
        "notification cannot overtake failed context/timestamp admission"
    );
    let stats = receiver.stats();
    assert_eq!(stats.reserved, 1);
    assert_eq!(stats.abandoned, 1);
    assert_eq!(stats.delivered, 0);
    assert_eq!(stats.retained_records, 0);
    assert_eq!(stats.retained_bytes, 0);
}
