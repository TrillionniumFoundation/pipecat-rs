//! Loopback-only browser host. Provider credentials stay in the native process.
use crate::{NativeRunner, Options, RunnerConfig, SttProvider};
use pipecat_client_server::server::{serve_with_api, AssetBundle, ServerConfig};
use std::{path::PathBuf, sync::Arc};

const HELP:&str="pipecat-browser-agent [--bind 127.0.0.1:7860] [--assets DIRECTORY] [--no-intro] [--webrtc] [--vad-model PATH --onnx-runtime PATH]\n\nNative HTTP /api/start + isolated RTVI 2.2/protobuf WebSocket voice sessions.\nDefault environment: DEEPGRAM_API_KEY, OPENAI_API_KEY, CARTESIA_API_KEY.\nLocal STT: --features whisper; PIPECAT_STT_PROVIDER=whisper, PIPECAT_WHISPER_MODEL=/existing/ggml.bin.\nLocal STT needs Silero paths or explicit PIPECAT_WHISPER_SEGMENT_MS=1000..30000; no Deepgram key.\nOPENAI_API_KEY and CARTESIA_API_KEY remain required; no models auto-download.\nOptional PIPECAT_WHISPER_THREADS=1..256, PIPECAT_WHISPER_LANGUAGE=en|auto|language-tag, PIPECAT_WHISPER_PRE_ROLL_MS=0..5000 (VAD, default 3000).\nOptional provider/model/voice/rate environment matches pipecat-voice-agent.\nWebSocket is the default; --webrtc also enables native SmallWebRTC (requires the webrtc Cargo feature).\nContext and bot-output represent generated text, never confirmed audio playback.\nBrowser automation and physical microphone/speaker validation are separate tests.\n";
/// Start the native browser runner from command-line flags and provider environment.
/// `--help` does not read credentials. This owns the HTTP host and all sessions
/// until SIGINT, then joins their cleanup.
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let mut options = Options::default();
    let mut assets = None;
    let mut webrtc = false;
    let mut args = std::env::args().skip(1);
    let mut vad = None;
    let mut runtime = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                print!("{HELP}");
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
    let stt = SttProvider::from_env()?;
    let providers = stt.providers_from_env()?;
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
    #[cfg(feature = "webrtc")]
    let runner = Arc::new(if webrtc {
        NativeRunner::new_with_webrtc(
            runner_config,
            providers,
            crate::WebRtcRunnerConfig::default(),
        )?
    } else {
        NativeRunner::new(runner_config, providers)?
    });
    #[cfg(not(feature = "webrtc"))]
    let runner = Arc::new(NativeRunner::new(runner_config, providers)?);
    let transports = if webrtc {
        vec!["websocket", "smallwebrtc"]
    } else {
        vec!["websocket"]
    };
    let config = ServerConfig {
        assets: assets
            .map(|directory| {
                AssetBundle::from_directory(&directory)?
                    .with_browser_config(serde_json::json!({"project_name":"Pipecat Rust Voice Agent","start_endpoint":"/api/start","transports":transports}))
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
