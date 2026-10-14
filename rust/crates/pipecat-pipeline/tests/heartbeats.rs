use pipecat_core::*;
use pipecat_pipeline::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Semaphore;
const WAIT: Duration = Duration::from_secs(3);
struct BranchGate {
    probes: AtomicUsize,
    sentinels: AtomicUsize,
    release: Semaphore,
}
#[async_trait]
impl FrameProcessor for BranchGate {
    fn name(&self) -> &str {
        "branch gate"
    }
    async fn process(
        &self,
        f: Frame,
        d: Direction,
        c: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if c.is_heartbeat_probe(&f) {
            self.probes.fetch_add(1, Ordering::SeqCst);
            self.release.acquire().await.unwrap().forget();
        }
        if f.name.starts_with("SyncFrame#") {
            self.sentinels.fetch_add(1, Ordering::SeqCst);
        }
        c.push(f, d).await
    }
}
async fn scenario(sync: bool, healthy_sibling: bool) {
    let gate = Arc::new(BranchGate {
        probes: AtomicUsize::new(0),
        sentinels: AtomicUsize::new(0),
        release: Semaphore::new(0),
    });
    let mut branches: Vec<Branch> = vec![vec![gate.clone()]];
    if healthy_sibling {
        branches.push(vec![Arc::new(IdentityProcessor)]);
    }
    let config = ParallelPipelineConfig {
        branch: PipelineConfig {
            queue_capacity: 1,
            shutdown_timeout: Duration::from_millis(300),
            ..Default::default()
        },
        buffered_frames: 1,
        synchronization_timeout: Duration::from_millis(500),
        ..Default::default()
    };
    let processor: Arc<dyn FrameProcessor> = if sync {
        Arc::new(SyncParallelPipeline::with_config(branches, FrameOrder::Pipeline, config).unwrap())
    } else {
        Arc::new(ParallelPipeline::with_config(branches, config).unwrap())
    };
    let mut worker = Pipeline::new(vec![processor])
        .with_heartbeats(HeartbeatConfig {
            period: Duration::from_millis(20),
            timeout: Duration::from_millis(100),
            max_pending: 8,
        })
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_millis(700),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut monitor = worker.heartbeat_monitor().unwrap();
    let result = tokio::time::timeout(WAIT, async {
        loop {
            let s = monitor.snapshot();
            if s.admission_failures >= 3
                && (if healthy_sibling {
                    s.received >= 3
                } else {
                    s.timeouts >= 2
                })
            {
                break;
            }
            monitor.changed().await.map_err(|e| e.to_string())?;
        }
        if worker.state() != WorkerState::Running || !worker.errors().is_empty() {
            return Err("diagnostics failed business runtime".into());
        }
        if gate.sentinels.load(Ordering::SeqCst) != 0 {
            return Err("owned probes created Sync sentinels".into());
        }
        if !healthy_sibling && monitor.snapshot().received != 0 {
            return Err("child boundary counted as root receipt".into());
        }
        // Release all held branch probes. Fresh probes must recover root progress.
        let before = monitor.snapshot().received;
        gate.release.add_permits(1000);
        while monitor.snapshot().received <= before {
            monitor.changed().await.map_err(|e| e.to_string())?;
        }
        if !worker.errors().is_empty() {
            return Err("recovery produced errors".into());
        }
        Ok::<_, String>(())
    })
    .await;
    let cleanup_errors = cleanup(&mut worker).await;
    assert!(
        cleanup_errors.is_empty(),
        "cleanup: {cleanup_errors:?}; scenario: {result:?}"
    );
    result
        .expect("scenario timed out")
        .expect("scenario failed");
}
async fn cleanup(worker: &mut PipelineWorker) -> Vec<String> {
    let mut cleanup_errors = Vec::new();
    if !worker.has_finished() {
        match tokio::time::timeout(WAIT, worker.cancel("test cleanup")).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if worker.has_finished() => {
                let _ = error;
            }
            other => cleanup_errors.push(format!("cancel: {other:?}")),
        }
    }
    let joined = match tokio::time::timeout(WAIT, worker.join()).await {
        Ok(result) => Some(result),
        Err(_) => {
            cleanup_errors.push("initial join timeout".into());
            let cancel = tokio::time::timeout(WAIT, worker.cancel("join timeout fallback")).await;
            if !matches!(cancel, Ok(Ok(()))) && !worker.has_finished() {
                cleanup_errors.push(format!("fallback cancel: {cancel:?}"));
            }
            // The core's finite shutdown timeout bounds cancellation cleanup.
            // Retain ownership until its supervisor actually joins.
            Some(worker.join().await)
        }
    };
    match joined {
        Some(Ok(report)) if report.errors.is_empty() => {}
        other => cleanup_errors.push(format!("join: {other:?}")),
    }
    cleanup_errors
}
#[tokio::test]
async fn parallel_child_saturated_ingress_soft_drops_only_diagnostics() {
    scenario(false, false).await;
}
#[tokio::test]
async fn sync_parallel_probe_avoids_sentinel_and_business_batch_pressure() {
    scenario(true, false).await;
}
#[tokio::test]
async fn one_healthy_branch_can_mask_a_stalled_sibling() {
    scenario(true, true).await;
}

#[tokio::test]
async fn ordinary_heartbeat_still_uses_sync_sentinel_and_application_output() {
    let gate = Arc::new(BranchGate {
        probes: AtomicUsize::new(0),
        sentinels: AtomicUsize::new(0),
        release: Semaphore::new(1000),
    });
    let parallel = SyncParallelPipeline::new(vec![vec![gate.clone()]]).unwrap();
    let mut worker = Pipeline::new(vec![Arc::new(parallel)])
        .with_heartbeats(HeartbeatConfig::default())
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_millis(700),
            ..Default::default()
        })
        .await
        .unwrap();
    let result = tokio::time::timeout(WAIT, async {
        worker.recv().await.ok_or("missing Start")?;
        worker
            .sender()
            .send(
                Frame::new(FrameData::Heartbeat { timestamp: 42 }),
                Direction::Downstream,
            )
            .await
            .map_err(|e| e.to_string())?;
        let output = worker.recv().await.ok_or("missing user Heartbeat")?;
        if !matches!(output.frame.data, FrameData::Heartbeat { timestamp: 42 }) {
            return Err("wrong application output".into());
        }
        if gate.sentinels.load(Ordering::SeqCst) != 1 {
            return Err("user heartbeat bypassed Sync".into());
        }
        Ok::<_, String>(())
    })
    .await;
    let errors = cleanup(&mut worker).await;
    assert!(
        errors.is_empty(),
        "cleanup: {errors:?}; scenario: {result:?}"
    );
    result.expect("scenario timeout").expect("scenario failed");
}
