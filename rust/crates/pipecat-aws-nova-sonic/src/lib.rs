//! Finite, tool-free Nova Sonic speech over native signed HTTP/2 EventStream.
//!
//! The application owns one exact standard user/assistant pair. Its history is
//! generated text traversing the output path, never a claim of audible playback.
//! Tools, continuation, reconnect, live text/settings and implicit resampling are
//! intentionally unsupported. Only explicit credentials are accepted.
mod app;
mod config;
mod protocol;
mod transport;
pub use app::{
    NovaSonicApp, NovaSonicHandle, NovaSonicHistory, NovaSonicInput, NovaSonicRunningApp,
    NovaSonicWorker,
};
pub use config::{NovaSonicConfig, NovaSonicLimits};
pub use pipecat_aws_common::AwsCredentials;
