//! Consolidated first-model-event TTFB graph, using existing direct dependencies.
//! Turns audio/transcript ownership and private service units run separately.
#[path = "anthropic_usage_updates.rs"]
mod anthropic_usage_regressions;
#[path = "../../pipecat-services/tests/review_azure_chat.rs"]
mod azure_regressions;
#[path = "../../pipecat-services/tests/provider_first_progress.rs"]
mod first_progress;
#[path = "../../pipecat-services/tests/nvidia_reasoning.rs"]
mod nvidia_regressions;
#[path = "../../pipecat-services/tests/review_sarvam_llm.rs"]
mod sarvam_regressions;
