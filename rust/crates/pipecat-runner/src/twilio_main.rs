use pipecat_runner::{
    twilio::{TwilioApp, TwilioConfig},
    ProviderConfig,
};
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut enable = false;
    let mut no_intro = false;
    let mut bind: SocketAddr = "127.0.0.1:7861".parse()?;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!(
                    "pipecat-twilio-agent --enable [--bind 127.0.0.1:7861] [--no-intro]\n\nDisabled by default. No credentials are read without --enable.\nRequired: TWILIO_ACCOUNT_SID TWILIO_AUTH_TOKEN PIPECAT_TWILIO_WEBHOOK_URL\nPIPECAT_TWILIO_MEDIA_URL DEEPGRAM_API_KEY OPENAI_API_KEY CARTESIA_API_KEY\nOptional: PIPECAT_TWILIO_HANGUP=disabled|local PIPECAT_TWILIO_API_BASE\nSee rust/crates/pipecat-runner/TWILIO.md for exact URL/signature and proxy boundaries."
                );
                return Ok(());
            }
            "--enable" => enable = true,
            "--no-intro" => no_intro = true,
            "--bind" => {
                bind = args
                    .next()
                    .ok_or("--bind requires a literal loopback address")?
                    .parse()?
            }
            _ => return Err("unknown Twilio argument".into()),
        }
    }
    if !enable {
        return Err("Twilio entry is disabled; pass --enable explicitly".into());
    }
    if !bind.ip().is_loopback() {
        return Err("Twilio listener must be literal loopback".into());
    }
    let mut config = TwilioConfig::from_lookup(|key| std::env::var(key).ok())?;
    config.introduce = !no_intro;
    let providers = ProviderConfig::from_lookup(|key| std::env::var(key).ok())
        .map_err(|_| "invalid hosted provider configuration")?;
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let address = listener.local_addr()?;
    let app = TwilioApp::new(listener, config, providers)?;
    println!("Twilio listener on {address}; use only the configured canonical proxy URLs");
    tokio::signal::ctrl_c().await?;
    let report = app.shutdown().await?;
    println!(
        "Twilio shutdown: joined_connections={} failed_calls={} pipeline_join_errors={} media_cleanup_errors={} hangup_errors={} task_errors={}",
        report.joined_connections,
        report.failed_calls,
        report.pipeline_join_errors,
        report.media_cleanup_errors,
        report.hangup_errors,
        report.task_errors,
    );
    if report.failed_cleanup() {
        return Err("Twilio shutdown completed with cleanup failures".into());
    }
    Ok(())
}
