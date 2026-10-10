use futures_util::FutureExt;
use pipecat_core::*;
use pipecat_turns::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
struct PanicWhenCancelled;
impl Future for PanicWhenCancelled {
    type Output = Result<(), ProcessorError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for PanicWhenCancelled {
    fn drop(&mut self) {
        panic!("synthetic handler destructor panic");
    }
}
#[tokio::test]
async fn timeout_destructor_panic_is_reported_without_losing_committed_history_or_budget() {
    let assistant = LLMAssistantAggregator::new(SharedContext::default(), Default::default());
    let mut receiver = assistant
        .subscribe_stopped(AssistantStoppedConfig {
            handler_timeout: Duration::from_millis(100),
            ..Default::default()
        })
        .unwrap();
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::llm("committed")),
        FrameData::LlmResponseEnd,
    ] {
        let output = assistant
            .process_frame(Frame::new(data), Direction::Downstream)
            .unwrap();
        for action in output.actions {
            if let AggregationAction::AssistantStopped(action) = action {
                action.admit().unwrap();
            }
        }
    }
    let before = assistant.context().frame_data().unwrap();
    let result = std::panic::AssertUnwindSafe(receiver.handle_next(|_| PanicWhenCancelled))
        .catch_unwind()
        .await;
    assert!(
        result.unwrap().is_err(),
        "the helper must catch timeout-triggered destructor panic"
    );
    let stats = receiver.stats();
    assert_eq!(stats.handler_panics, 1);
    assert_eq!(stats.handler_timeouts, 0);
    assert_eq!(stats.in_flight_handlers, 0);
    assert_eq!(stats.retained_records, 0);
    assert_eq!(stats.retained_bytes, 0);
    assert_eq!(assistant.context().frame_data().unwrap(), before);
}
