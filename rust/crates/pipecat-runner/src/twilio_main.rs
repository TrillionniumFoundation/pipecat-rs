use pipecat_runner::twilio::{command, TwilioApp};

#[tokio::main]
async fn main() {
    if let Err(error) = command::run_from_env(TwilioApp::new).await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
