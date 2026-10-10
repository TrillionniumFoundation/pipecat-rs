//! Real native text bot for a configured loopback OpenAI-compatible model.
//! cargo run -p pipecat-evals --features native-bot-example --example native_text_bot -- --port 7860 --llm-url http://127.0.0.1:11434/v1/chat/completions --model local-model
#[path = "support/text_app.rs"]
mod text_app;
use pipecat_evals::{
    bot_host::{EvalBotHost, EvalBotHostConfig},
    Cancellation,
};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut port = 7860u16;
    let mut endpoint = None;
    let mut model = None;
    let mut introduce = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => port = args.next().ok_or("--port requires a value")?.parse()?,
            "--llm-url" => endpoint = Some(args.next().ok_or("--llm-url requires a value")?),
            "--model" => model = Some(args.next().ok_or("--model requires a value")?),
            "--introduce" => introduce = true,
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }
    let mut bot = text_app::TextBot::new(
        endpoint.ok_or("--llm-url is required")?,
        model.ok_or("--model is required")?,
    )?;
    bot.introduce = introduce;
    let host = EvalBotHost::bind(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
        EvalBotHostConfig::default(),
    )
    .await?;
    eprintln!("native text eval host: ws://{}", host.local_addr()?);
    let cancel = Cancellation::new();
    let service = host.serve(&bot, cancel.clone());
    tokio::pin!(service);
    let report = tokio::select! {
        result = &mut service => result?,
        signal = tokio::signal::ctrl_c() => { signal?; cancel.cancel(); service.await? }
    };
    eprintln!("native text eval host closed: {report:?}");
    Ok(())
}
