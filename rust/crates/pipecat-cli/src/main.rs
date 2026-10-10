use pipecat_cli::{
    config::{Command, ProviderConfig, HELP},
    AgentError,
};
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), AgentError> {
    match Command::parse(std::env::args().skip(1))? {
        Command::Help => print!("{HELP}"),
        Command::Smoke => {
            let result = pipecat_cli::smoke::run().await?;
            println!("PASS: local synthetic STT → user turn → streamed LLM → TTS → protobuf WebSocket audio → playback-acknowledged assistant context ({} PCM bytes)",result.audio_bytes);
            println!("No paid provider calls, microphone capture, or physical speaker playback were used.");
        }
        Command::Serve(options) => pipecat_cli::serve(options, ProviderConfig::from_env()?).await?,
    }
    Ok(())
}
