//! Explicit CLI and environment configuration. Secrets never appear in Debug.
use crate::{
    receipt_output::{ImageChannelConfig, PlaybackProtocol, ReceiptConfig},
    AgentError,
};
use std::{fmt, net::SocketAddr, path::PathBuf, time::Duration};

pub use crate::startup_image::StartupImage;

pub const HELP: &str = "pipecat-voice-agent [OPTIONS]\n\nNative WebSocket protobuf → Deepgram STT → OpenAI LLM → Cartesia TTS agent.\n\nOPTIONS\n  --help                     Show this help without reading credentials\n  --smoke                    Run local synthetic providers and a full socket roundtrip\n  --bind ADDRESS             Listen address (default 127.0.0.1:7860)\n  --once                     Exit after one client disconnects\n  --vad-model PATH           Enable native Silero using this ONNX model\n  --onnx-runtime PATH        Explicit ONNX Runtime library (required with --vad-model)\n  --playback-protocol MODE   legacy-v1 (default) or explicitly negotiated chunk-v2\n  --image-channel            Enable separate bounded images (requires chunk-v2)\n  --initial-image-rgb PATH    One trusted raw RGB8 file per session (requires images)\n  --initial-image-size WxH    Exact dimensions for --initial-image-rgb\n  --no-intro                 Wait for the user instead of generating an introduction\n  --turn-timeout-ms N         Transcript silence timeout (default 600)\n  --idle-timeout-secs N       Client inactivity limit (default 300)\n\nREQUIRED ENVIRONMENT (except --help / --smoke)\n  DEEPGRAM_API_KEY  OPENAI_API_KEY  CARTESIA_API_KEY\n\nOPTIONAL ENVIRONMENT\n  OPENAI_MODEL                default gpt-4.1-mini\n  CARTESIA_VOICE_ID           default 86e30c1d-714b-4074-a1f2-1cb6b552fb49\n  PIPECAT_SYSTEM_PROMPT       Spoken-response system instruction\n  PIPECAT_INPUT_SAMPLE_RATE   PCM16 input Hz (default 16000, mono)\n  PIPECAT_OUTPUT_SAMPLE_RATE  PCM16 output Hz (default 24000, mono)\n\nAudio clients must send playback-complete only after actual remote playback.\nNo microphone, local speaker, Daily, or LiveKit is embedded. Native Silero is optional and requires the two explicit file paths.\nBind to loopback by default; use an authenticated TLS proxy for remote access.\n";

pub const DEFAULT_SYSTEM: &str = "You are a helpful assistant in a voice conversation. Your responses will be spoken aloud, so avoid emojis, bullet points, or other formatting that cannot be spoken. Respond to what the user said in a creative, helpful, and brief way.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Help,
    Smoke,
    Serve(Box<Options>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VadPaths {
    pub model: PathBuf,
    pub runtime: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    pub playback_protocol: PlaybackProtocol,
    pub receipt: ReceiptConfig,
    pub image_channel: Option<ImageChannelConfig>,
    pub initial_image: Option<StartupImage>,
    pub vad: Option<VadPaths>,
    pub bind: SocketAddr,
    pub once: bool,
    pub introduce: bool,
    pub turn_timeout: Duration,
    pub idle_timeout: Duration,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            playback_protocol: PlaybackProtocol::LegacyV1,
            receipt: ReceiptConfig::default(),
            image_channel: None,
            initial_image: None,
            vad: None,
            bind: SocketAddr::from(([127, 0, 0, 1], 7860)),
            once: false,
            introduce: true,
            turn_timeout: Duration::from_millis(600),
            idle_timeout: Duration::from_secs(300),
        }
    }
}
impl Options {
    /// Also enforced for native callers that do not use Command::parse.
    pub fn validate(&self) -> Result<(), AgentError> {
        if !self.bind.ip().is_loopback() {
            return Err(AgentError::Config("listener must use a loopback address"));
        }
        if self.image_channel.is_some() && self.playback_protocol != PlaybackProtocol::ChunkV2 {
            return Err(AgentError::Config(
                "--image-channel requires --playback-protocol chunk-v2",
            ));
        }
        if let Some(config) = &self.image_channel {
            config.validate()?;
            if let Some(image) = &self.initial_image {
                image.validate(config)?;
            }
        } else if self.initial_image.is_some() {
            return Err(AgentError::Config(
                "initial RGB image requires --image-channel",
            ));
        }
        if self.playback_protocol == PlaybackProtocol::ChunkV2 {
            self.receipt.validate()?;
        }
        Ok(())
    }
}
impl Command {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, AgentError> {
        let mut args = args.into_iter();
        let mut options = Options::default();
        let mut smoke = false;
        let mut vad_model = None;
        let mut onnx_runtime = None;
        let mut image_path = None;
        let mut image_size = None;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => return Ok(Self::Help),
                "--smoke" => smoke = true,
                "--once" => options.once = true,
                "--playback-protocol" => {
                    options.playback_protocol = match args.next().as_deref() {
                        Some("legacy-v1") => PlaybackProtocol::LegacyV1,
                        Some("chunk-v2") => PlaybackProtocol::ChunkV2,
                        _ => {
                            return Err(AgentError::Config(
                                "--playback-protocol needs legacy-v1 or chunk-v2",
                            ))
                        }
                    };
                }
                "--image-channel" => options.image_channel = Some(ImageChannelConfig::default()),
                "--initial-image-rgb" => {
                    image_path = Some(PathBuf::from(
                        args.next()
                            .ok_or(AgentError::Config("--initial-image-rgb needs a file path"))?,
                    ));
                }
                "--initial-image-size" => {
                    let value = args.next().ok_or(AgentError::Config(
                        "--initial-image-size needs WIDTHxHEIGHT",
                    ))?;
                    image_size = Some(
                        value
                            .split_once('x')
                            .and_then(|(w, h)| {
                                Some((w.parse::<u32>().ok()?, h.parse::<u32>().ok()?))
                            })
                            .ok_or(AgentError::Config(
                                "--initial-image-size needs WIDTHxHEIGHT",
                            ))?,
                    );
                }
                "--vad-model" => {
                    vad_model = Some(PathBuf::from(
                        args.next()
                            .ok_or(AgentError::Config("--vad-model needs a file path"))?,
                    ))
                }
                "--onnx-runtime" => {
                    onnx_runtime = Some(PathBuf::from(
                        args.next()
                            .ok_or(AgentError::Config("--onnx-runtime needs a library path"))?,
                    ))
                }
                "--no-intro" => options.introduce = false,
                "--bind" => {
                    options.bind = args
                        .next()
                        .ok_or(AgentError::Config("--bind needs an address"))?
                        .parse()
                        .map_err(|_| AgentError::Config("invalid --bind address"))?
                }
                "--turn-timeout-ms" => {
                    options.turn_timeout = Duration::from_millis(number(
                        args.next(),
                        "--turn-timeout-ms needs a positive integer",
                    )?)
                }
                "--idle-timeout-secs" => {
                    options.idle_timeout = Duration::from_secs(number(
                        args.next(),
                        "--idle-timeout-secs needs a positive integer",
                    )?)
                }
                _ => return Err(AgentError::Config("unknown option; use --help")),
            }
        }
        if !options.bind.ip().is_loopback() {
            return Err(AgentError::Config("--bind must use a loopback address; remote access requires an authenticated local proxy"));
        }
        options.vad = match (vad_model, onnx_runtime) {
            (None, None) => None,
            (Some(model), Some(runtime)) if !smoke => Some(VadPaths { model, runtime }),
            (Some(_), Some(_)) => return Err(AgentError::Config(
                "--smoke uses synthetic audio; select native VAD with a configured serve session",
            )),
            _ => {
                return Err(AgentError::Config(
                    "--vad-model and --onnx-runtime must be supplied together",
                ))
            }
        };
        if smoke
            && (options.image_channel.is_some() || image_path.is_some() || image_size.is_some())
        {
            return Err(AgentError::Config("--smoke does not accept image options"));
        }
        options.validate()?;
        match (image_path, image_size) {
            (None, None) => {}
            (Some(path), Some(size)) if options.image_channel.is_some() => {
                options.initial_image = Some(StartupImage::load_rgb(path, size)?);
                options.validate()?;
            }
            (Some(_), Some(_)) => {
                return Err(AgentError::Config(
                    "initial RGB image requires --image-channel",
                ))
            }
            _ => {
                return Err(AgentError::Config(
                    "--initial-image-rgb and --initial-image-size must be supplied together",
                ))
            }
        }
        if smoke && options.playback_protocol != PlaybackProtocol::LegacyV1 {
            return Err(AgentError::Config(
                "--smoke exercises legacy-v1; chunk-v2 requires its explicit client protocol",
            ));
        }
        Ok(if smoke {
            Self::Smoke
        } else {
            Self::Serve(Box::new(options))
        })
    }
}
fn number(value: Option<String>, message: &'static str) -> Result<u64, AgentError> {
    value
        .and_then(|s| s.parse().ok())
        .filter(|n| *n > 0 && *n <= 86_400_000)
        .ok_or(AgentError::Config(message))
}
#[derive(Clone)]
pub struct ProviderConfig {
    pub deepgram_key: String,
    pub openai_key: String,
    pub cartesia_key: String,
    pub deepgram_endpoint: String,
    pub openai_endpoint: String,
    pub cartesia_endpoint: String,
    pub model: String,
    pub voice_id: String,
    pub system_prompt: String,
    pub input_rate: u32,
    pub output_rate: u32,
}
impl fmt::Debug for ProviderConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProviderConfig")
            .field("credentials", &"[redacted]")
            .field("endpoints", &"[configured]")
            .field("input_rate", &self.input_rate)
            .field("output_rate", &self.output_rate)
            .finish_non_exhaustive()
    }
}
impl ProviderConfig {
    pub fn from_env() -> Result<Self, AgentError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }
    pub fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self, AgentError> {
        fn required(
            get: &mut impl FnMut(&str) -> Option<String>,
            key: &str,
            error: &'static str,
        ) -> Result<String, AgentError> {
            get(key)
                .filter(|v| !v.trim().is_empty())
                .ok_or(AgentError::Config(error))
        }
        fn rate(
            get: &mut impl FnMut(&str) -> Option<String>,
            key: &str,
            default: u32,
        ) -> Result<u32, AgentError> {
            match get(key) {
                None => Ok(default),
                Some(v) => {
                    v.parse()
                        .ok()
                        .filter(|n| (8000..=48000).contains(n))
                        .ok_or(AgentError::Config(
                            "sample rate must be an integer from 8000 to 48000",
                        ))
                }
            }
        }
        Ok(Self {
            deepgram_key: required(&mut get, "DEEPGRAM_API_KEY", "DEEPGRAM_API_KEY is required")?,
            openai_key: required(&mut get, "OPENAI_API_KEY", "OPENAI_API_KEY is required")?,
            cartesia_key: required(&mut get, "CARTESIA_API_KEY", "CARTESIA_API_KEY is required")?,
            deepgram_endpoint: "wss://api.deepgram.com/v1/listen".into(),
            openai_endpoint: "https://api.openai.com/v1/chat/completions".into(),
            cartesia_endpoint: "wss://api.cartesia.ai/tts/websocket".into(),
            model: get("OPENAI_MODEL").unwrap_or_else(|| "gpt-4.1-mini".into()),
            voice_id: get("CARTESIA_VOICE_ID")
                .unwrap_or_else(|| "86e30c1d-714b-4074-a1f2-1cb6b552fb49".into()),
            system_prompt: get("PIPECAT_SYSTEM_PROMPT").unwrap_or_else(|| DEFAULT_SYSTEM.into()),
            input_rate: rate(&mut get, "PIPECAT_INPUT_SAMPLE_RATE", 16000)?,
            output_rate: rate(&mut get, "PIPECAT_OUTPUT_SAMPLE_RATE", 24000)?,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn help_and_smoke_need_no_credentials() {
        assert_eq!(Command::parse(["--help".into()]).unwrap(), Command::Help);
        assert_eq!(Command::parse(["--smoke".into()]).unwrap(), Command::Smoke);
        assert!(ProviderConfig::from_lookup(|_| None).is_err());
    }
    #[test]
    fn options_are_strict_and_bounded() {
        assert!(Command::parse(["--wat".into()]).is_err());
        assert!(Command::parse(["--turn-timeout-ms".into(), "0".into()]).is_err());
        assert!(Command::parse(["--bind".into(), "bad-secret".into()])
            .unwrap_err()
            .to_string()
            .find("bad-secret")
            .is_none());
    }
    #[test]
    fn config_debug_redacts_secrets_and_prompt() {
        let c = ProviderConfig::from_lookup(|_| Some("SECRET".into()));
        assert!(c.is_err());
        let c = ProviderConfig::from_lookup(|key| key.ends_with("KEY").then(|| "SECRET".into()))
            .unwrap();
        assert!(!format!("{c:?}").contains("SECRET"));
    }
}
