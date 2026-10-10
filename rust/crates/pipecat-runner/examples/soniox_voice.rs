//! Native hosted Soniox voice cascade with a session-local Rust arithmetic tool.
#[path = "support/soniox_tools.rs"]
mod arithmetic;
use pipecat_client_server::server::{serve_with_api, ServerConfig};
use pipecat_runner::{NativeRunner, Options, RunnerConfig, SonioxVoiceConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("--help") {
        println!("soniox_voice: set SONIOX_API_KEY and OPENAI_API_KEY, then connect a native RTVI/protobuf WebSocket client through POST http://127.0.0.1:7860/api/start. Pure Rust add tool; generated-text context, no playback acknowledgements.");
        return Ok(());
    }
    if std::env::args().len() != 1 {
        return Err("unknown argument; use --help".into());
    }
    let mut providers = SonioxVoiceConfig::from_env()?;
    providers.session_tools = Some(Arc::new(arithmetic::tools));
    providers
        .system_prompt
        .push_str(" Use the add tool for integer addition.");
    let runner = Arc::new(NativeRunner::new_soniox(
        RunnerConfig {
            allowed_origins: vec!["http://127.0.0.1:7860".into()],
            // Literal-loopback native clients can omit Origin; this is not a remote host.
            allow_missing_origin: true,
            options: Options {
                introduce: false,
                ..Default::default()
            },
            ..Default::default()
        },
        providers,
    )?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:7860").await?;
    println!("Soniox voice + Rust add tool at http://127.0.0.1:7860; generated-text history, no playback acknowledgements");
    let result = serve_with_api(
        listener,
        runner.clone(),
        ServerConfig {
            allowed_origins: vec!["http://127.0.0.1:7860".into()],
            ..Default::default()
        },
        async {
            let _ = tokio::signal::ctrl_c().await;
        },
    )
    .await;
    runner.shutdown().await;
    result?;
    Ok(())
}
