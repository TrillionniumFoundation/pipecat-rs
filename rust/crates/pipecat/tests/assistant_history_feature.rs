#![cfg(feature = "assistant-history")]

use pipecat::media_transport::{AssistantSpokenHistorySink, SpokenHistorySink};
use pipecat::turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator, SharedContext,
};
use std::sync::Arc;

#[test]
fn assistant_history_adapter_is_available_through_facade() {
    let assistant = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let sink: Arc<dyn SpokenHistorySink> = Arc::new(AssistantSpokenHistorySink::new(assistant));
    assert_eq!(Arc::strong_count(&sink), 1);
}
