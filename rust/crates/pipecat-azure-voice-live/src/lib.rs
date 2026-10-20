//! Azure Voice Live's distinct flat-session WebSocket protocol and bounded native PCM app.
//! No Azure OpenAI alias, token provider, reconnect, resampling or progressive tools.
mod app;
mod client;
mod config;
mod processor;
mod types;
pub use app::*;
pub use client::*;
pub use config::*;
pub use processor::*;
pub use types::*;
