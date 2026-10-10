#[tokio::main]
async fn main() {
    if let Err(error) = pipecat_runner::run_from_env().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
