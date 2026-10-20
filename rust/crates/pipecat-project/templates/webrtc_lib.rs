//! Shared SmallWebRTC cascade construction, used by the production entry point.
use pipecat_runner::{NativeRunner, ProviderConfig, RunnerConfig, RunnerError, WebRtcRunnerConfig};

/// Explicit provider endpoints and transport limits also support local native tests.
pub fn create_runner(
    config: RunnerConfig,
    providers: ProviderConfig,
    webrtc: WebRtcRunnerConfig,
) -> Result<NativeRunner, RunnerError> {
    NativeRunner::new_with_webrtc(config, providers, webrtc)
}

/// Read the normal provider environment and serve until Ctrl-C, joining sessions.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    pipecat_runner::run_webrtc_from_env(create_runner).await
}
