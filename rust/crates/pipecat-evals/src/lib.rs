//! Native, bounded RTVI scripted behavioral evaluations. No Python runtime or fallback.
#![forbid(unsafe_code)]
#[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
mod audio_to_audio_scenario;
#[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
pub use audio_to_audio_scenario::AudioToAudioScenarioFile;
mod bot_audio;
#[cfg(feature = "bot-audio-transcription")]
mod bot_transcription;
#[cfg(feature = "bot-audio-transcription")]
mod bot_transcription_settings;
#[cfg(feature = "bot-audio-transcription")]
mod native_loader;
pub use bot_audio::{BotAudioChunk, BotAudioLimits, BotAudioRun};
#[cfg(feature = "bot-audio-transcription")]
pub use bot_transcription::BotTranscriptionConfig;
#[cfg(feature = "bot-audio-transcription")]
pub use bot_transcription_settings::BotTranscriptionSettings;
pub mod bot_host;
mod bounded_yaml;
pub mod events;
pub mod judge;
pub mod matcher;
pub mod process;
mod recorded_audio;
mod recorded_scenario;
#[cfg(feature = "user-speech")]
mod user_speech;
#[cfg(feature = "user-speech")]
mod user_speech_cache;
#[cfg(feature = "user-speech")]
mod user_speech_scenario;
pub use recorded_scenario::RecordedScenarioFile;
#[cfg(feature = "user-speech")]
pub use user_speech::{CartesiaUserSpeechSettings, CartesiaUserSpeechVoice};
#[cfg(feature = "user-speech")]
pub use user_speech_cache::UserSpeechCachePolicy;
#[cfg(feature = "user-speech")]
pub use user_speech_scenario::UserSpeechScenarioFile;
pub mod release;
pub use recorded_audio::RecordedAudio;
pub mod scenario;
pub mod serializer;
pub mod session;
pub mod suite;
pub use judge::{EvalJudge, JudgeConfig, JudgeDecision, JudgeOptions, JudgeSettings, JudgeVerdict};
pub use scenario::{
    Expectation, FunctionCall, ScenarioFile, ScriptScenario, ScriptTurn, SendAfter,
};
pub use session::{Cancellation, EvalSession, SessionOptions};
#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    #[error("evaluation judge: {0}")]
    Judge(pipecat_classifiers::ClassifierError),
    #[error("invalid scenario: {0}")]
    Scenario(String),
    #[error("unsupported native evaluation feature: {0}")]
    Unsupported(String),
    #[error("evaluation limit exceeded: {0}")]
    Limit(&'static str),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("file I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] pipecat_protocol::ProtocolError),
    #[error("transport: {0}")]
    Transport(#[from] pipecat_transports::TransportError),
    #[error("{0} timed out")]
    Timeout(&'static str),
    #[error("evaluation cancelled")]
    Cancelled,
    #[error("bot disconnected before evaluation completed")]
    Disconnected,
}

#[cfg(feature = "user-images")]
mod user_image_input;
#[cfg(feature = "user-images")]
mod user_images;
#[cfg(feature = "user-images")]
pub use user_image_input::ImageInputLimits;
#[cfg(feature = "user-images")]
pub use user_images::{EncodedUserImage, UserImageScenarioFile};
