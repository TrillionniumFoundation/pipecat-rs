//! Explicit speech-provider selection. Browser requests cannot change providers.
use crate::{ProviderConfig, RunnerError};

#[derive(Clone, Debug, Default)]
pub enum SttProvider {
    #[default]
    Deepgram,
    #[cfg(feature = "whisper")]
    Whisper(crate::LocalWhisperConfig),
}
impl SttProvider {
    pub fn from_env() -> Result<Self, RunnerError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }
    pub fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self, RunnerError> {
        match get("PIPECAT_STT_PROVIDER").as_deref().unwrap_or("deepgram") {
            "deepgram" => {
                if [
                    "PIPECAT_WHISPER_MODEL",
                    "PIPECAT_WHISPER_SEGMENT_MS",
                    "PIPECAT_WHISPER_THREADS",
                    "PIPECAT_WHISPER_LANGUAGE",
                    "PIPECAT_WHISPER_PRE_ROLL_MS",
                ]
                .iter()
                .any(|key| get(key).is_some())
                {
                    return Err(RunnerError::SttConfiguration(
                        "Whisper options require PIPECAT_STT_PROVIDER=whisper",
                    ));
                }
                Ok(Self::Deepgram)
            }
            "whisper" => {
                #[cfg(not(feature = "whisper"))]
                {
                    Err(RunnerError::SttConfiguration(
                        "local STT requires the whisper Cargo feature",
                    ))
                }
                #[cfg(feature = "whisper")]
                {
                    let path = get("PIPECAT_WHISPER_MODEL")
                        .filter(|s| !s.is_empty())
                        .ok_or(RunnerError::SttConfiguration(
                            "PIPECAT_WHISPER_MODEL must name an existing GGML file",
                        ))?;
                    let mut config = crate::LocalWhisperConfig::new(path);
                    if let Some(value) = get("PIPECAT_WHISPER_SEGMENT_MS") {
                        let millis = value.parse::<u64>().map_err(|_| {
                            RunnerError::SttConfiguration(
                                "PIPECAT_WHISPER_SEGMENT_MS must be an integer from 1000 to 30000",
                            )
                        })?;
                        config.segmentation = crate::WhisperSegmentation::FixedWindow(
                            std::time::Duration::from_millis(millis),
                        );
                    }
                    if let Some(value) = get("PIPECAT_WHISPER_PRE_ROLL_MS") {
                        if matches!(
                            config.segmentation,
                            crate::WhisperSegmentation::FixedWindow(_)
                        ) {
                            return Err(RunnerError::SttConfiguration(
                                "PIPECAT_WHISPER_PRE_ROLL_MS only applies to VAD segmentation",
                            ));
                        }
                        let millis = value.parse::<u64>().ok().filter(|n| *n <= 5000).ok_or(
                            RunnerError::SttConfiguration(
                                "PIPECAT_WHISPER_PRE_ROLL_MS must be an integer from 0 to 5000",
                            ),
                        )?;
                        config.pre_roll_duration = std::time::Duration::from_millis(millis);
                    }
                    if let Some(value) = get("PIPECAT_WHISPER_THREADS") {
                        config.model.inference_threads = value.parse().map_err(|_| {
                            RunnerError::SttConfiguration(
                                "PIPECAT_WHISPER_THREADS must be an integer from 1 to 256",
                            )
                        })?;
                    }
                    if let Some(value) = get("PIPECAT_WHISPER_LANGUAGE") {
                        config.settings.language = if value == "auto" {
                            None
                        } else {
                            Some(value.parse()?)
                        };
                    }
                    config.validate()?;
                    Ok(Self::Whisper(config))
                }
            }
            _ => Err(RunnerError::SttConfiguration(
                "PIPECAT_STT_PROVIDER must be deepgram or whisper",
            )),
        }
    }
    pub fn providers_from_env(&self) -> Result<ProviderConfig, RunnerError> {
        self.providers_from_lookup(|key| std::env::var(key).ok())
    }
    /// Load hosted LLM/TTS credentials without requiring unused STT credentials.
    pub fn providers_from_lookup(
        &self,
        get: impl FnMut(&str) -> Option<String>,
    ) -> Result<ProviderConfig, RunnerError> {
        match self {
            Self::Deepgram => ProviderConfig::from_lookup(get).map_err(|_| RunnerError::SttConfiguration("Deepgram mode requires DEEPGRAM_API_KEY, OPENAI_API_KEY, CARTESIA_API_KEY and valid sample rates")),
            #[cfg(feature = "whisper")]
            Self::Whisper(_) => {
                let mut get = get;
                fn required(get: &mut impl FnMut(&str) -> Option<String>, key: &str) -> Result<String, RunnerError> {
                    get(key).filter(|s| !s.trim().is_empty()).ok_or(RunnerError::SttConfiguration("local STT still requires OPENAI_API_KEY and CARTESIA_API_KEY"))
                }
                let input_rate = rate(get("PIPECAT_INPUT_SAMPLE_RATE"), 16000)?;
                if input_rate != 16000 { return Err(RunnerError::SttConfiguration("Whisper requires PIPECAT_INPUT_SAMPLE_RATE=16000 (browser capture is resampled)")); }
                Ok(ProviderConfig {
                    deepgram_key: String::new(), deepgram_endpoint: String::new(),
                    openai_key: required(&mut get, "OPENAI_API_KEY")?,
                    cartesia_key: required(&mut get, "CARTESIA_API_KEY")?,
                    openai_endpoint: "https://api.openai.com/v1/chat/completions".into(),
                    cartesia_endpoint: "wss://api.cartesia.ai/tts/websocket".into(),
                    model: get("OPENAI_MODEL").unwrap_or_else(|| "gpt-4.1-mini".into()),
                    voice_id: get("CARTESIA_VOICE_ID").unwrap_or_else(|| "86e30c1d-714b-4074-a1f2-1cb6b552fb49".into()),
                    system_prompt: get("PIPECAT_SYSTEM_PROMPT").unwrap_or_else(|| pipecat_cli::config::DEFAULT_SYSTEM.into()),
                    input_rate,
                    output_rate: rate(get("PIPECAT_OUTPUT_SAMPLE_RATE"), 24000)?,
                })
            }
        }
    }
}
#[cfg(feature = "whisper")]
fn rate(value: Option<String>, default: u32) -> Result<u32, RunnerError> {
    value.map_or(Ok(default), |s| {
        s.parse()
            .ok()
            .filter(|n| (8000..=48000).contains(n))
            .ok_or(RunnerError::SttConfiguration(
                "sample rate must be an integer from 8000 to 48000",
            ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selection_is_explicit_and_closed() {
        assert!(matches!(
            SttProvider::from_lookup(|_| None).unwrap(),
            SttProvider::Deepgram
        ));
        assert!(
            SttProvider::from_lookup(|k| (k == "PIPECAT_STT_PROVIDER").then(|| "auto".into()))
                .is_err()
        );
        assert!(SttProvider::from_lookup(
            |k| (k == "PIPECAT_WHISPER_MODEL").then(|| "/models/local.bin".into())
        )
        .is_err());
        assert!(SttProvider::from_lookup(
            |k| (k == "PIPECAT_STT_PROVIDER").then(|| "whisper".into())
        )
        .is_err());
        assert!(SttProvider::Deepgram
            .providers_from_lookup(
                |k| matches!(k, "OPENAI_API_KEY" | "CARTESIA_API_KEY").then(|| "test".into())
            )
            .is_err());
    }
    #[cfg(feature = "whisper")]
    #[test]
    fn vad_pre_roll_environment_is_bounded_and_not_silently_ignored() {
        let lookup = |key: &str, value: &str, fixed: bool| match key {
            "PIPECAT_STT_PROVIDER" => Some("whisper".into()),
            "PIPECAT_WHISPER_MODEL" => {
                Some(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml").into())
            }
            "PIPECAT_WHISPER_PRE_ROLL_MS" => Some(value.into()),
            "PIPECAT_WHISPER_SEGMENT_MS" if fixed => Some("1000".into()),
            _ => None,
        };
        for value in ["-1", "5001", "1.5", "invalid"] {
            assert!(SttProvider::from_lookup(|key| lookup(key, value, false)).is_err());
        }
        for value in ["0", "3000", "5000"] {
            assert!(SttProvider::from_lookup(|key| lookup(key, value, false)).is_ok());
        }
        assert!(SttProvider::from_lookup(|key| lookup(key, "3000", true)).is_err());
    }
    #[cfg(feature = "whisper")]
    #[test]
    fn local_mode_requires_only_configured_hosted_credentials() {
        let local = SttProvider::Whisper(crate::LocalWhisperConfig::new("unused"));
        let keys = |key: &str| {
            assert_ne!(
                key, "DEEPGRAM_API_KEY",
                "local mode must not read unused STT credentials"
            );
            matches!(key, "OPENAI_API_KEY" | "CARTESIA_API_KEY").then(|| "synthetic".into())
        };
        let config = local.providers_from_lookup(keys).unwrap();
        assert!(config.deepgram_key.is_empty());
        assert!(config.deepgram_endpoint.is_empty());
        assert_eq!(config.input_rate, 16000);
        assert!(local.providers_from_lookup(|_| None).is_err());
        assert!(local
            .providers_from_lookup(|k| if k == "PIPECAT_INPUT_SAMPLE_RATE" {
                Some("48000".into())
            } else {
                keys(k)
            })
            .is_err());
        let hosted = SttProvider::Deepgram
            .providers_from_lookup(|k| {
                if k == "DEEPGRAM_API_KEY" {
                    Some("test".into())
                } else {
                    keys(k)
                }
            })
            .unwrap();
        assert_eq!(config.model, hosted.model);
        assert_eq!(config.voice_id, hosted.voice_id);
        assert_eq!(config.system_prompt, hosted.system_prompt);
        assert_eq!(config.openai_endpoint, hosted.openai_endpoint);
        assert_eq!(config.cartesia_endpoint, hosted.cartesia_endpoint);
    }
}
