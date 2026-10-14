use pipecat_core::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Semaphore;
const WAIT: Duration = Duration::from_secs(3);
fn hb() -> HeartbeatConfig {
    HeartbeatConfig {
        period: Duration::from_millis(20),
        timeout: Duration::from_millis(100),
        max_pending: 4,
    }
}
fn cfg() -> PipelineConfig {
    PipelineConfig {
        queue_capacity: 2,
        output_capacity: 8,
        shutdown_timeout: Duration::from_millis(500),
        ..Default::default()
    }
}
type Case<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
async fn run(
    pipeline: Pipeline,
    config: PipelineConfig,
    case: impl for<'a> FnOnce(&'a mut PipelineWorker) -> Case<'a>,
) {
    let mut worker = pipeline.start(config).await.unwrap();
    let result = tokio::time::timeout(WAIT, case(&mut worker)).await;
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
macro_rules! check {
    ($condition:expr) => {
        if !$condition {
            return Err(stringify!($condition).into());
        }
    };
}
async fn until(
    m: &mut HeartbeatMonitor,
    predicate: impl Fn(&HeartbeatSnapshot) -> bool,
) -> Result<HeartbeatSnapshot, String> {
    loop {
        let s = m.snapshot();
        if predicate(&s) {
            return Ok(s);
        }
        m.changed().await.map_err(|e| e.to_string())?;
    }
}
struct Gate {
    blocked: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
    heartbeats_only: bool,
}
impl Gate {
    fn new(heartbeats_only: bool) -> Self {
        Self {
            blocked: AtomicBool::new(true),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            heartbeats_only,
        }
    }
}
#[async_trait]
impl FrameProcessor for Gate {
    fn name(&self) -> &str {
        "gate"
    }
    async fn process(
        &self,
        f: Frame,
        d: Direction,
        c: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let target = if self.heartbeats_only {
            c.is_heartbeat_probe(&f)
        } else {
            matches!(f.data, FrameData::Text(_))
        };
        if target && self.blocked.load(Ordering::SeqCst) {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        c.push(f, d).await
    }
}
#[tokio::test]
async fn default_disabled_and_application_heartbeat_is_ordinary_output() {
    run(Pipeline::new(vec![]), cfg(), |w| {
        Box::pin(async move {
            check!(w.heartbeat_monitor().is_none());
            w.recv().await.ok_or("no Start")?;
            w.sender()
                .send(
                    Frame::new(FrameData::Heartbeat { timestamp: 42 }),
                    Direction::Downstream,
                )
                .await
                .map_err(|e| e.to_string())?;
            check!(matches!(
                w.recv().await.ok_or("no heartbeat")?.frame.data,
                FrameData::Heartbeat { timestamp: 42 }
            ));
            Ok(())
        })
    })
    .await;
}
#[tokio::test]
async fn real_sink_receipts_do_not_require_application_output_consumption() {
    let config = PipelineConfig {
        output_capacity: 1,
        ..cfg()
    };
    run(Pipeline::new(vec![]).with_heartbeats(hb()), config, |w| {
        Box::pin(async move {
            let mut m = w.heartbeat_monitor().ok_or("no monitor")?;
            let s = until(&mut m, |s| s.received >= 3).await?;
            check!(s.timeouts == 0 && w.state() == WorkerState::Running && w.errors().is_empty());
            check!(w.recv().await.ok_or("missing Start")?.frame.data.is_start());
            check!(w.try_recv().is_err());
            Ok(())
        })
    })
    .await;
}
#[tokio::test]
async fn stalled_probe_times_out_repeatedly_then_recovers_without_error() {
    let gate = Arc::new(Gate::new(true));
    let g = gate.clone();
    run(
        Pipeline::new(vec![gate]).with_heartbeats(hb()),
        cfg(),
        move |w| {
            Box::pin(async move {
                let mut m = w.heartbeat_monitor().ok_or("no monitor")?;
                until(&mut m, |s| s.timeouts >= 2 && s.admission_failures > 0).await?;
                check!(w.state() == WorkerState::Running && w.errors().is_empty());
                g.blocked.store(false, Ordering::SeqCst);
                g.release.add_permits(1);
                until(&mut m, |s| s.received > 0).await?;
                check!(w.errors().is_empty());
                Ok(())
            })
        },
    )
    .await;
}
#[tokio::test]
async fn interior_queue_pressure_is_diagnostic_only() {
    let gate = Arc::new(Gate::new(false));
    let g = gate.clone();
    run(
        Pipeline::new(vec![Arc::new(IdentityProcessor), gate]).with_heartbeats(hb()),
        cfg(),
        move |w| {
            Box::pin(async move {
                w.recv().await.ok_or("no Start")?;
                w.sender()
                    .send(Frame::text("hold"), Direction::Downstream)
                    .await
                    .map_err(|e| e.to_string())?;
                g.entered
                    .acquire()
                    .await
                    .map_err(|e| e.to_string())?
                    .forget();
                let mut m = w.heartbeat_monitor().ok_or("no monitor")?;
                until(&mut m, |s| s.timeouts >= 1 && s.admission_failures > 0).await?;
                check!(w.state() == WorkerState::Running && w.errors().is_empty());
                g.blocked.store(false, Ordering::SeqCst);
                g.release.add_permits(1);
                until(&mut m, |s| s.received > 0).await?;
                Ok(())
            })
        },
    )
    .await;
}
#[tokio::test]
async fn end_stops_probes_but_monitors_until_terminal_and_then_freezes() {
    let gate = Arc::new(Gate::new(true));
    let g = gate.clone();
    run(
        Pipeline::new(vec![gate]).with_heartbeats(hb()),
        cfg(),
        move |w| {
            Box::pin(async move {
                w.recv().await.ok_or("no Start")?;
                g.entered
                    .acquire()
                    .await
                    .map_err(|e| e.to_string())?
                    .forget();
                w.end().await.map_err(|e| e.to_string())?;
                let mut m = w.heartbeat_monitor().ok_or("no monitor")?;
                let s = until(&mut m, |s| s.draining).await?;
                let after = until(&mut m, |s| s.timeouts >= 2).await?;
                check!(s.sent == after.sent);
                g.blocked.store(false, Ordering::SeqCst);
                g.release.add_permits(1);
                while w.recv().await.is_some() {}
                until(&mut m, |s| !s.active).await?;
                let before = m.snapshot();
                tokio::time::sleep(Duration::from_millis(120)).await;
                check!(
                    before == m.snapshot()
                        && before.pending == 0
                        && w.state() == WorkerState::Ended
                );
                Ok(())
            })
        },
    )
    .await;
}

#[tokio::test]
async fn pause_can_report_a_stall_and_resume_recovers() {
    run(
        Pipeline::new(vec![Arc::new(IdentityProcessor)]).with_heartbeats(hb()),
        cfg(),
        |w| {
            Box::pin(async move {
                w.recv().await.ok_or("no Start")?;
                let mut m = w.heartbeat_monitor().ok_or("no monitor")?;
                until(&mut m, |s| s.received > 0).await?;
                w.sender()
                    .send(
                        Frame::new(FrameData::ProcessorPauseUrgent {
                            processor: "IdentityProcessor".into(),
                        }),
                        Direction::Downstream,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                w.recv().await.ok_or("no pause")?;
                until(&mut m, |s| s.timeouts > 0).await?;
                let count = m.snapshot().received;
                w.sender()
                    .send(
                        Frame::new(FrameData::ProcessorResume {
                            processor: "IdentityProcessor".into(),
                        }),
                        Direction::Downstream,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                until(&mut m, |s| s.received > count).await?;
                check!(w.errors().is_empty());
                Ok(())
            })
        },
    )
    .await;
}
struct StartGate {
    entered: Semaphore,
    release: Semaphore,
    probes: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl FrameProcessor for StartGate {
    fn name(&self) -> &str {
        "start gate"
    }
    async fn process(
        &self,
        f: Frame,
        d: Direction,
        c: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if f.data.is_start() {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        if c.is_heartbeat_probe(&f) {
            self.probes.fetch_add(1, Ordering::SeqCst);
        }
        c.push(f, d).await
    }
}
#[tokio::test]
async fn monitor_starts_only_after_start_reaches_the_real_sink() {
    let gate = Arc::new(StartGate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        probes: std::sync::atomic::AtomicUsize::new(0),
    });
    let g = gate.clone();
    let mut start = tokio::spawn(async move {
        Pipeline::new(vec![g])
            .with_heartbeats(hb())
            .start(cfg())
            .await
    });
    let entered = tokio::time::timeout(WAIT, gate.entered.acquire()).await;
    if let Ok(Ok(permit)) = entered {
        permit.forget();
    }
    tokio::time::sleep(Duration::from_millis(230)).await;
    let blocked = !start.is_finished() && gate.probes.load(Ordering::SeqCst) == 0;
    gate.release.add_permits(1);
    let started = tokio::time::timeout(WAIT, &mut start).await;
    let mut worker = match started {
        Ok(Ok(Ok(worker))) => worker,
        other => {
            if other.is_err() {
                start.abort();
                let _ = start.await;
            }
            panic!(
                "startup result: {}",
                if other.is_err() { "timeout" } else { "failed" }
            );
        }
    };
    let snapshot = worker.heartbeat_monitor().unwrap().snapshot();
    let cleanup_errors = cleanup(&mut worker).await;
    assert!(cleanup_errors.is_empty(), "{cleanup_errors:?}");
    assert!(blocked && snapshot.timeouts == 0);
}

struct CaptureProbe {
    context: std::sync::Mutex<Option<ProcessorContext>>,
    probe: std::sync::Mutex<Option<Frame>>,
    arrived: Semaphore,
}
#[async_trait]
impl FrameProcessor for CaptureProbe {
    fn name(&self) -> &str {
        "capture probe"
    }
    async fn setup(&self, _: &StartFrame, c: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(c.clone());
        Ok(())
    }
    async fn process(
        &self,
        f: Frame,
        d: Direction,
        c: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if c.is_heartbeat_probe(&f) {
            *self.probe.lock().unwrap() = Some(f);
            self.arrived.add_permits(1);
            Ok(())
        } else {
            c.push(f, d).await
        }
    }
}
#[tokio::test]
async fn child_busy_input_and_output_pressure_never_acknowledge_or_fail_root() {
    let capture = Arc::new(CaptureProbe {
        context: std::sync::Mutex::new(None),
        probe: std::sync::Mutex::new(None),
        arrived: Semaphore::new(0),
    });
    let cap = capture.clone();
    run(Pipeline::new(vec![capture]).with_heartbeats(hb()),cfg(),move|w|Box::pin(async move {
        cap.arrived.acquire().await.map_err(|e|e.to_string())?.forget();
        let context=cap.context.lock().unwrap().as_ref().ok_or("no context")?.clone();
        let probe=cap.probe.lock().unwrap().as_ref().ok_or("no probe")?.clone();
        let rejected=Pipeline::new(vec![]).inherit_lifecycle(&context).with_heartbeats(hb()).start(cfg()).await;
        check!(matches!(rejected,Err(PipelineError::InvalidConfiguration(_))));
        let gate=Arc::new(Gate::new(false));
        let mut child=Pipeline::new(vec![gate.clone()]).inherit_lifecycle(&context).start(PipelineConfig{queue_capacity:1,output_capacity:1,..cfg()}).await.map_err(|e|e.to_string())?;
        let child_result=tokio::time::timeout(WAIT,async {
            check!(child.heartbeat_monitor().is_none());
            // Start occupies the sole child output slot. A real owned probe
            // reaching this child boundary is dropped without root acknowledgement.
            child.sender().try_send_from(probe.clone(),Direction::Downstream,&context).map_err(|e|e.to_string())?;
            let mut monitor=w.heartbeat_monitor().ok_or("no root monitor")?;
            until(&mut monitor,|s|s.admission_failures>0).await?;
            check!(monitor.snapshot().received==0 && child.errors().is_empty());
            child.recv().await.ok_or("missing child Start")?;
            child.sender().send(Frame::text("block"),Direction::Downstream).await.map_err(|e|e.to_string())?;
            gate.entered.acquire().await.map_err(|e|e.to_string())?.forget();
            child.sender().send(Frame::text("queued"),Direction::Downstream).await.map_err(|e|e.to_string())?;
            let sender=child.sender();
            let mut pending=Box::pin(sender.send(Frame::text("waiting"),Direction::Downstream));
            // One explicit poll acquires the uncontended input lock then waits
            // for the already-full normal lane; no detached task or timing guess.
            let is_pending=std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())).await;
            check!(is_pending);
            let ordinary=child.sender().try_send_from(Frame::new(FrameData::Heartbeat{timestamp:7}),Direction::Downstream,&context);
            let diagnostic=child.sender().try_send_from(probe,Direction::Downstream,&context);
            drop(pending);
            check!(matches!(ordinary,Err(PipelineError::Backpressure(ref text)) if text=="input admission is busy"));
            check!(diagnostic.is_ok());
            until(&mut monitor,|s|s.timeouts>0).await?;
            check!(monitor.snapshot().received==0 && child.state()==WorkerState::Running && w.errors().is_empty());
            Ok(())
        }).await;
        let child_cleanup=cleanup(&mut child).await;
        check!(child_cleanup.is_empty());
        child_result.map_err(|_|"child scenario timeout".to_string())?
    })).await;
}
#[test]
fn pinned_source_defaults_match_native_opt_in_defaults() {
    let source: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/python_heartbeat_source.json")).unwrap();
    assert_eq!(source["defaults"]["enable_heartbeats"], false);
    assert_eq!(
        source["defaults"]["heartbeats_period_secs"].as_f64(),
        Some(HeartbeatConfig::default().period.as_secs_f64())
    );
    assert_eq!(
        source["defaults"]["heartbeats_monitor_secs"].as_f64(),
        Some(HeartbeatConfig::default().timeout.as_secs_f64())
    );
}
