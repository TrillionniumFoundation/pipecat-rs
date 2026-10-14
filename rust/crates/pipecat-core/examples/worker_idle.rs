//! Bounded, credential-free root-wide notify-only idle monitoring.
use pipecat_core::{IdleTimeoutAction, Pipeline, PipelineConfig, WorkerIdleConfig};
use std::time::Duration;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut worker = Pipeline::new(vec![])
        .with_idle_timeout(WorkerIdleConfig {
            timeout: Duration::from_millis(50),
            action: IdleTimeoutAction::NotifyOnly,
            ..Default::default()
        })
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        })
        .await?;
    let mut monitor = worker.idle_monitor().expect("explicit root policy");
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        while monitor.snapshot().timeouts < 2 {
            monitor.changed().await?;
        }
        Ok::<_, tokio::sync::watch::error::RecvError>(())
    })
    .await;
    let cancelled = worker.cancel("example complete").await;
    let joined = worker.join().await;
    cancelled?;
    joined?;
    result??;
    let s = monitor.snapshot();
    println!(
        "activities={}, timeouts={}, active={}, coverage_lost={}",
        s.activities, s.timeouts, s.active, s.coverage_lost
    );
    Ok(())
}
