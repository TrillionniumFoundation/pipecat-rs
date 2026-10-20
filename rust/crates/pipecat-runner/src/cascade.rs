//! Explicit whole-cascade selection; browser input cannot change this choice.
use crate::{ProviderConfig, RunnerError};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VoiceCascade {
    #[default]
    Default,
    Soniox,
}
impl VoiceCascade {
    pub fn from_env() -> Result<Self, RunnerError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }
    pub fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self, RunnerError> {
        match get("PIPECAT_VOICE_CASCADE").as_deref().unwrap_or("default") {
            "default" => {
                if ["SONIOX_STT_MODEL", "SONIOX_TTS_MODEL", "SONIOX_VOICE"]
                    .iter()
                    .any(|key| get(key).is_some())
                {
                    return Err(RunnerError::CascadeConfiguration(
                        "Soniox options require PIPECAT_VOICE_CASCADE=soniox",
                    ));
                }
                Ok(Self::Default)
            }
            "soniox" => {
                #[cfg(not(feature = "soniox"))]
                {
                    Err(RunnerError::CascadeConfiguration(
                        "Soniox cascade requires the soniox Cargo feature",
                    ))
                }
                #[cfg(feature = "soniox")]
                {
                    if [
                        "PIPECAT_STT_PROVIDER",
                        "PIPECAT_WHISPER_MODEL",
                        "PIPECAT_WHISPER_SEGMENT_MS",
                        "PIPECAT_WHISPER_THREADS",
                        "PIPECAT_WHISPER_LANGUAGE",
                        "PIPECAT_WHISPER_PRE_ROLL_MS",
                        "CARTESIA_VOICE_ID",
                    ]
                    .iter()
                    .any(|key| get(key).is_some())
                    {
                        return Err(RunnerError::CascadeConfiguration(
                            "Soniox cascade does not accept legacy STT or Cartesia options",
                        ));
                    }
                    Ok(Self::Soniox)
                }
            }
            _ => Err(RunnerError::CascadeConfiguration(
                "PIPECAT_VOICE_CASCADE must be default or soniox",
            )),
        }
    }
}

pub(crate) struct SessionParts {
    pub stt: std::sync::Arc<dyn pipecat_core::FrameProcessor>,
    pub tts: std::sync::Arc<dyn pipecat_core::FrameProcessor>,
    pub llm: pipecat_services::ClientConfig,
    pub user_config: pipecat_turns::UserAggregatorConfig,
    pub initial_context: pipecat_services::Context,
    pub tools: Vec<std::sync::Arc<dyn pipecat_core::FrameProcessor>>,
    pub model: String,
    pub system_prompt: String,
}

#[derive(Clone)]
pub(crate) enum SessionProviders {
    Default(Box<ProviderConfig>),
    #[cfg(feature = "soniox")]
    Soniox(Box<crate::SonioxVoiceConfig>),
}
impl SessionProviders {
    pub fn input_rate(&self) -> u32 {
        match self {
            Self::Default(config) => config.input_rate,
            #[cfg(feature = "soniox")]
            Self::Soniox(config) => config.stt.sample_rate,
        }
    }
    pub fn output_rate(&self) -> u32 {
        match self {
            Self::Default(config) => config.output_rate,
            #[cfg(feature = "soniox")]
            Self::Soniox(config) => config.tts.sample_rate,
        }
    }
    #[cfg(feature = "webrtc")]
    pub fn legacy(&self) -> Option<&ProviderConfig> {
        match self {
            Self::Default(config) => Some(config),
            #[cfg(feature = "soniox")]
            Self::Soniox(_) => None,
        }
    }
}

/// Shared hosted provider construction; transport-specific readiness and output
/// remain outside the cascade. Construction does not connect provider sockets.
pub(crate) fn hosted_parts(
    providers: &ProviderConfig,
    turn_timeout: std::time::Duration,
) -> Result<SessionParts, RunnerError> {
    use pipecat_streaming_speech::{DeepgramClient, DeepgramConfig, DeepgramSttProcessor};
    let mut stt = DeepgramConfig::new(providers.deepgram_key.clone());
    stt.endpoint = providers.deepgram_endpoint.clone();
    stt.sample_rate = providers.input_rate;
    stt.channels = 1;
    hosted_parts_with_stt(
        providers,
        turn_timeout,
        std::sync::Arc::new(DeepgramSttProcessor::new(
            DeepgramClient::new(stt).map_err(|_| RunnerError::Provider)?,
        )),
    )
}
pub(crate) fn hosted_parts_with_stt(
    providers: &ProviderConfig,
    turn_timeout: std::time::Duration,
    stt: std::sync::Arc<dyn pipecat_core::FrameProcessor>,
) -> Result<SessionParts, RunnerError> {
    use pipecat_services::{ClientConfig, Context, Provider};
    use pipecat_streaming_speech::{CartesiaClient, CartesiaConfig, CartesiaTtsProcessor};
    use pipecat_turns::{UserAggregatorConfig, UserTurnConfig};
    use std::sync::Arc;
    let mut tts = CartesiaConfig::new(providers.cartesia_key.clone(), providers.voice_id.clone());
    tts.endpoint = providers.cartesia_endpoint.clone();
    tts.sample_rate = providers.output_rate;
    let mut llm = ClientConfig::new(Provider::OpenAi, providers.openai_key.clone());
    llm.endpoint = providers.openai_endpoint.clone();
    Ok(SessionParts {
        stt,
        tts: Arc::new(CartesiaTtsProcessor::new(
            CartesiaClient::new(tts).map_err(|_| RunnerError::Provider)?,
        )),
        llm,
        user_config: UserAggregatorConfig {
            turn: UserTurnConfig {
                speech_timeout: turn_timeout,
                ..Default::default()
            },
            ..Default::default()
        },
        initial_context: Context::default(),
        tools: vec![],
        model: providers.model.clone(),
        system_prompt: providers.system_prompt.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_is_explicit_and_feature_disabled_is_exact() {
        assert_eq!(
            VoiceCascade::from_lookup(|_| None).unwrap(),
            VoiceCascade::Default
        );
        assert_eq!(VoiceCascade::from_lookup(|key| (key == "PIPECAT_VOICE_CASCADE").then(|| "auto".into())).unwrap_err().to_string(), "invalid voice cascade configuration: PIPECAT_VOICE_CASCADE must be default or soniox");
        assert_eq!(VoiceCascade::from_lookup(|key| (key == "SONIOX_VOICE").then(|| "voice".into())).unwrap_err().to_string(), "invalid voice cascade configuration: Soniox options require PIPECAT_VOICE_CASCADE=soniox");
        #[cfg(not(feature = "soniox"))]
        assert_eq!(
            VoiceCascade::from_lookup(
                |key| (key == "PIPECAT_VOICE_CASCADE").then(|| "soniox".into())
            )
            .unwrap_err()
            .to_string(),
            "invalid voice cascade configuration: Soniox cascade requires the soniox Cargo feature"
        );
    }
}
