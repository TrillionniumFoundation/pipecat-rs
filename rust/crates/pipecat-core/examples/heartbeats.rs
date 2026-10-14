//! Bounded, credential-free root traversal diagnostics.
use pipecat_core::{HeartbeatConfig, Pipeline, PipelineConfig};
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut worker = Pipeline::new(vec![])
        .with_heartbeats(HeartbeatConfig {
            period: Duration::from_millis(20),
            ..Default::default()
        })
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        })
        .await?;
    let mut monitor = worker.heartbeat_monitor().expect("configured root");
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        while monitor.snapshot().received < 3 {
            monitor.changed().await?;
        }
        Ok::<_, tokio::sync::watch::error::RecvError>(())
    })
    .await;
    // Always own cleanup before returning a monitoring failure.
    let cancelled = worker.cancel("example complete").await;
    let joined = worker.join().await;
    cancelled?;
    joined?;
    result??;
    let snapshot = monitor.snapshot();
    println!(
        "received={}, timeouts={}, active={}",
        snapshot.received, snapshot.timeouts, snapshot.active
    );
    Ok(())
}
