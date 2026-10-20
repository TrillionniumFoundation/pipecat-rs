//! Azure OpenAI Realtime GA application wiring over the shared native GA actor.
//! Explicit credentials are supplied by the caller; no token discovery or refresh.
mod app;
pub use app::*;
pub use pipecat_realtime::*;
