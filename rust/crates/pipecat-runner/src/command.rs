//! Loopback-only browser host. Provider credentials stay in the native process.
use crate::{NativeRunner, Options, RunnerConfig, SttProvider, VoiceCascade};
use pipecat_client_server::server::{serve_with_api, AssetBundle, ServerConfig};
use std::{path::PathBuf, sync::Arc};

const HELP: &str = "pipecat-browser-agent [--bind 127.0.0.1:7860] [--assets DIRECTORY] [--no-intro] [--realtime openai] [--webrtc] [--text-input] [--vad-model PATH --onnx-runtime PATH]\n\nNative HTTP /api/start + isolated RTVI 2.2/protobuf WebSocket voice sessions.\nDefault environment: DEEPGRAM_API_KEY, OPENAI_API_KEY, CARTESIA_API_KEY.\nIntegrated speech-to-speech: --features realtime and --realtime openai; only OPENAI_API_KEY.\nOptional OPENAI_REALTIME_MODEL, OPENAI_REALTIME_VOICE, PIPECAT_SYSTEM_PROMPT.\nRealtime uses WebSocket, server VAD and 24000 Hz mono PCM; cascade/local STT/VAD/rate overrides and --webrtc are rejected.\nAlternative: --features soniox and PIPECAT_VOICE_CASCADE=soniox, SONIOX_API_KEY, OPENAI_API_KEY.\nSoniox supports WebSocket with provider endpoint detection; local STT/VAD and --webrtc are rejected.\nOptional SONIOX_STT_MODEL, SONIOX_TTS_MODEL, SONIOX_VOICE; history remains generated text.\nLocal STT: --features whisper; PIPECAT_STT_PROVIDER=whisper, PIPECAT_WHISPER_MODEL=/existing/ggml.bin.\nLocal STT needs Silero paths or explicit PIPECAT_WHISPER_SEGMENT_MS=1000..30000; no Deepgram key.\nOPENAI_API_KEY and CARTESIA_API_KEY remain required; no models auto-download.\nOptional PIPECAT_WHISPER_THREADS=1..256, PIPECAT_WHISPER_LANGUAGE=en|auto|language-tag, PIPECAT_WHISPER_PRE_ROLL_MS=0..5000 (VAD, default 3000).\nOptional provider/model/voice/rate environment matches pipecat-voice-agent.\n--text-input requires --assets and selects microphone-free input; --webrtc adds receive-only browser audio (requires the webrtc feature).\nThe host replaces asset config.json in memory; omitted --text-input preserves voice. Provider/STT credentials and initialization are unchanged.\nWebSocket is the default; --webrtc also enables native SmallWebRTC (requires the webrtc Cargo feature).\nContext and bot-output represent generated text, never confirmed audio playback.\nBrowser automation and physical microphone/speaker validation are separate tests.\n";
/// Start the native browser runner from command-line flags and provider environment.
/// `--help` does not read credentials. This owns the HTTP host and all sessions
/// until SIGINT, then joins their cleanup.
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    run(CommandDefaults::default()).await
}

#[derive(Default)]
struct CommandDefaults {
    webrtc: bool,
    #[cfg(feature = "webrtc")]
    factory: Option<WebRtcFactory>,
}

#[cfg(feature = "webrtc")]
type WebRtcFactory = fn(
    RunnerConfig,
    crate::ProviderConfig,
    crate::WebRtcRunnerConfig,
) -> Result<NativeRunner, crate::RunnerError>;

/// Start a SmallWebRTC application using its shared constructor. The selected
/// transport is enabled without flags and is the browser default. The constructor
/// receives explicit configuration, permitting credential-free local integration.
#[cfg(feature = "webrtc")]
pub async fn run_webrtc_from_env(factory: WebRtcFactory) -> Result<(), Box<dyn std::error::Error>> {
    run(CommandDefaults {
        webrtc: true,
        factory: Some(factory),
    })
    .await
}

async fn run(defaults: CommandDefaults) -> Result<(), Box<dyn std::error::Error>> {
    let mut options = Options::default();
    let mut assets = None;
    let mut webrtc = defaults.webrtc;
    let mut text_input = false;
    let mut realtime = false;
    let mut args = std::env::args().skip(1);
    let mut vad = None;
    let mut runtime = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                if defaults.webrtc {
                    print!("{}", HELP.replace("WebSocket is the default; --webrtc also enables native SmallWebRTC (requires the webrtc Cargo feature).", "This application enables SmallWebRTC by default; WebSocket remains available."));
                } else {
                    print!("{HELP}");
                }
                return Ok(());
            }
            "--bind" => options.bind = args.next().ok_or("--bind requires address")?.parse()?,
            "--assets" => {
                assets = Some(PathBuf::from(
                    args.next().ok_or("--assets requires directory")?,
                ))
            }
            "--no-intro" => options.introduce = false,
            "--webrtc" => webrtc = true,
            "--realtime" => {
                if realtime || args.next().as_deref() != Some("openai") {
                    return Err("--realtime requires exactly one openai selection".into());
                }
                realtime = true;
            }
            "--text-input" => text_input = true,
            "--vad-model" => {
                vad = Some(PathBuf::from(
                    args.next().ok_or("--vad-model requires path")?,
                ))
            }
            "--onnx-runtime" => {
                runtime = Some(PathBuf::from(
                    args.next().ok_or("--onnx-runtime requires path")?,
                ))
            }
            _ => return Err("unknown argument; use --help".into()),
        }
    }
    if text_input && assets.is_none() {
        return Err("--text-input requires --assets DIRECTORY".into());
    }
    options.vad = match (vad, runtime) {
        (None, None) => None,
        (Some(model), Some(runtime)) => Some(pipecat_cli::config::VadPaths { model, runtime }),
        _ => return Err("both VAD model and runtime are required".into()),
    };
    if !options.bind.ip().is_loopback() {
        return Err("listener must be loopback".into());
    }
    #[cfg(not(feature = "webrtc"))]
    if webrtc {
        return Err("--webrtc requires building pipecat-runner with --features webrtc".into());
    }
    if realtime && (webrtc || options.vad.is_some()) {
        return Err("--realtime openai supports WebSocket and server VAD only".into());
    }
    let (stt, providers) = if realtime {
        #[cfg(feature = "realtime")]
        {
            (
                SttProvider::default(),
                crate::cascade::SessionProviders::Realtime(Box::new(
                    crate::OpenAiRealtimeVoiceConfig::from_env()?,
                )),
            )
        }
        #[cfg(not(feature = "realtime"))]
        {
            return Err(
                "--realtime requires building pipecat-runner with --features realtime".into(),
            );
        }
    } else {
        let cascade = VoiceCascade::from_env()?;
        if cascade == VoiceCascade::Soniox && (webrtc || options.vad.is_some()) {
            return Err(crate::RunnerError::CascadeConfiguration("Soniox cascade supports WebSocket provider endpoint detection only; --webrtc and local VAD are unsupported").into());
        }
        let stt = match cascade {
            VoiceCascade::Default => SttProvider::from_env()?,
            VoiceCascade::Soniox => SttProvider::default(),
        };
        let providers = match cascade {
            VoiceCascade::Default => {
                crate::cascade::SessionProviders::Default(Box::new(stt.providers_from_env()?))
            }
            #[cfg(feature = "soniox")]
            VoiceCascade::Soniox => crate::cascade::SessionProviders::Soniox(Box::new(
                crate::SonioxVoiceConfig::from_env()?,
            )),
            #[cfg(not(feature = "soniox"))]
            VoiceCascade::Soniox => {
                return Err("Soniox cascade requires the soniox Cargo feature".into());
            }
        };
        (stt, providers)
    };
    let listener = tokio::net::TcpListener::bind(options.bind).await?;
    let address = listener.local_addr()?;
    let origin = format!("http://{address}");
    let runner_config = RunnerConfig {
        bind_ip: address.ip(),
        allowed_origins: vec![origin.clone()],
        options,
        stt,
        ..Default::default()
    };
    let runner = Arc::new(match providers {
        crate::cascade::SessionProviders::Default(providers) => {
            #[cfg(feature = "webrtc")]
            {
                if webrtc {
                    defaults.factory.unwrap_or(NativeRunner::new_with_webrtc)(
                        runner_config,
                        *providers,
                        crate::WebRtcRunnerConfig::default(),
                    )?
                } else {
                    NativeRunner::new(runner_config, *providers)?
                }
            }
            #[cfg(not(feature = "webrtc"))]
            {
                NativeRunner::new(runner_config, *providers)?
            }
        }
        #[cfg(feature = "realtime")]
        crate::cascade::SessionProviders::Realtime(providers) => {
            NativeRunner::new_openai_realtime(runner_config, *providers)?
        }
        #[cfg(feature = "soniox")]
        crate::cascade::SessionProviders::Soniox(providers) => {
            NativeRunner::new_soniox(runner_config, *providers)?
        }
    });
    let transports = if defaults.webrtc {
        vec!["smallwebrtc", "websocket"]
    } else if webrtc {
        vec!["websocket", "smallwebrtc"]
    } else {
        vec!["websocket"]
    };
    let mut browser_config = serde_json::json!({"project_name":"Pipecat Rust Voice Agent","start_endpoint":"/api/start","transports":transports});
    if realtime {
        browser_config["voice_mode"] = serde_json::json!("openai-realtime");
        browser_config["input_sample_rate"] = serde_json::json!(24000);
        browser_config["output_sample_rate"] = serde_json::json!(24000);
        browser_config["playback_acknowledgements"] = serde_json::json!(false);
    }
    if text_input {
        browser_config["input_mode"] = serde_json::Value::String("text".into());
    }
    let config = ServerConfig {
        assets: assets
            .map(|directory| {
                AssetBundle::from_directory(&directory)?
                    .with_browser_config(browser_config)
                    .map(Arc::new)
            })
            .transpose()?,
        allowed_origins: vec![origin.clone()],
        ..Default::default()
    };
    println!("Native browser agent at {origin}; generated-text context, no playback confirmations");
    let result = serve_with_api(listener, runner.clone(), config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await;
    runner.shutdown().await;
    result?;
    Ok(())
}
