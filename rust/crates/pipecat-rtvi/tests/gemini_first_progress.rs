//! Ordinary Gemini candidate progress and its existing wire/history regressions.
//! Reuse the RTVI dependency graph without changing the prior provider fixture.
#[path = "../../pipecat-services/tests/gemini_first_progress.rs"]
mod first_progress;
#[path = "../../pipecat-services/tests/gemini_signatures.rs"]
mod gemini_signatures;
#[path = "../../pipecat-services/tests/http_simulated.rs"]
mod http_regressions;
#[path = "../../pipecat-services/tests/review_gemini_part_order.rs"]
mod part_order;
#[path = "../../pipecat-services/tests/protocol_contracts.rs"]
mod protocol_contracts;
#[path = "../../pipecat-services/tests/runtime_processor.rs"]
mod runtime_regressions;
#[path = "../../pipecat-services/tests/review_gemini_terminal_boundaries.rs"]
mod terminal_boundaries;
