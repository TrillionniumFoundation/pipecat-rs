//! Shared native Twilio inbound cascade, used by the production entry point.
use pipecat_runner::{
    twilio::{TwilioApp, TwilioConfig, TwilioError},
    ProviderConfig,
};
use tokio::net::TcpListener;

/// Explicit configuration also supports synthetic literal-loopback phone tests.
/// This is the production constructor, not a separate test transport or pipeline.
pub fn create_app(
    listener: TcpListener,
    config: TwilioConfig,
    providers: ProviderConfig,
) -> Result<TwilioApp, TwilioError> {
    TwilioApp::new(listener, config, providers)
}

/// Default-off command, explicit credentials, canonical routes and joined shutdown.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    pipecat_runner::twilio::command::run_from_env(create_app).await
}
