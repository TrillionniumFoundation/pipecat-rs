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
