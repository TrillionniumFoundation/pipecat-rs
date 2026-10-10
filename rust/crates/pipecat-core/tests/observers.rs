//! Adversarial runtime observer contracts, driven by gates rather than sleeps.
use pipecat_core::*;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("observer must not stall the runtime")
}

async fn permit(semaphore: &Semaphore) {
    bounded(semaphore.acquire()).await.unwrap().forget();
}

struct RecordingObserver {
    events: Mutex<Vec<ObservationEvent>>,
    received: Semaphore,
}

impl RecordingObserver {
    fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            received: Semaphore::new(0),
        }
    }
    fn events(&self) -> Vec<ObservationEvent> {
        self.events.lock().unwrap().clone()
    }
    async fn wait_for(&self, predicate: impl Fn(&ObservationEvent) -> bool) {
        bounded(async {
            loop {
                if self.events.lock().unwrap().iter().any(&predicate) {
                    break;
                }
                self.received.acquire().await.unwrap().forget();
            }
        })
        .await;
    }
}

#[async_trait]
impl PipelineObserver for RecordingObserver {
    async fn on_event(&self, event: ObservationEvent) {
        self.events.lock().unwrap().push(event);
        self.received.add_permits(1);
    }
}

#[derive(Default)]
struct TickClock(AtomicU64);
impl ObserverClock for TickClock {
    fn now_ns(&self) -> u64 {
        self.0.fetch_add(10, Ordering::SeqCst)
    }
}

struct TimedIdentity {
    name: &'static str,
    clock: Arc<TickClock>,
}
#[async_trait]
impl FrameProcessor for TimedIdentity {
    fn name(&self) -> &str {
        self.name
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.clock.0.fetch_add(100, Ordering::SeqCst);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.clock.0.fetch_add(100, Ordering::SeqCst);
        ctx.push(frame, direction).await
    }
}

async fn finish(worker: PipelineWorker) -> WorkerReport {
    bounded(worker.end()).await.unwrap();
    bounded(worker.collect()).await.unwrap().1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setup_process_and_accepted_pushes_have_causal_order_and_injected_timestamps() {
    let clock = Arc::new(TickClock::default());
    let observer = Arc::new(RecordingObserver::new());
    let mut pipeline = Pipeline::new(vec![
        Arc::new(TimedIdentity {
            name: "first",
            clock: clock.clone(),
        }),
        Arc::new(TimedIdentity {
            name: "second",
            clock: clock.clone(),
        }),
    ])
    .with_observer_clock(clock);
    let handle = pipeline.add_observer(observer.clone(), ObserverOptions::default());
    let mut worker = bounded(pipeline.start(PipelineConfig::default()))
        .await
        .unwrap();
    let start = bounded(worker.recv()).await.unwrap().frame.id;
    let down = Frame::text("down");
    let down_id = down.id;
    bounded(worker.sender().send(down, Direction::Downstream))
        .await
        .unwrap();
    assert_eq!(bounded(worker.recv()).await.unwrap().frame.id, down_id);
    let up = Frame::text("up");
    let up_id = up.id;
    bounded(worker.sender().send(up, Direction::Upstream))
        .await
        .unwrap();
    assert_eq!(bounded(worker.recv()).await.unwrap().frame.id, up_id);
    assert_eq!(finish(worker).await.state, WorkerState::Ended);
    let events = observer.events();
    let runtime = match *events[0].observation {
        PipelineObservation::RuntimeStarted {
            runtime_id,
            timestamp_ns: 0,
        } => runtime_id,
        ref other => panic!("unexpected first event: {other:?}"),
    };
    let setups: Vec<_> = events
        .iter()
        .filter_map(|event| match &*event.observation {
            PipelineObservation::ProcessorSetup {
                processor,
                started_ns,
                finished_ns,
                outcome,
            } => Some((processor, started_ns, finished_ns, outcome)),
            _ => None,
        })
        .collect();
    assert_eq!(setups.len(), 2);
    for (index, (processor, start, end, outcome)) in setups.iter().enumerate() {
        assert_eq!(processor.runtime_id, runtime);
        assert_eq!(processor.index, index);
        assert_eq!(**outcome, SetupOutcome::Completed);
        assert_eq!(**end - **start, 110);
    }
    for (id, direction, order) in [
        (start, Direction::Downstream, [0, 1]),
        (down_id, Direction::Downstream, [0, 1]),
        (up_id, Direction::Upstream, [1, 0]),
    ] {
        let trace: Vec<_> = events
            .iter()
            .filter_map(|event| match &*event.observation {
                PipelineObservation::Process {
                    processor,
                    frame,
                    direction: d,
                    timestamp_ns,
                } if frame.id == id => {
                    assert_eq!(*d, direction);
                    Some((false, processor.index, *timestamp_ns))
                }
                PipelineObservation::Push {
                    source,
                    destination,
                    frame,
                    direction: d,
                    timestamp_ns,
                    ..
                } if frame.id == id => {
                    assert_eq!(*d, direction);
                    assert_eq!(
                        destination.as_ref().map(|p| p.index),
                        (source.index == order[0]).then_some(order[1])
                    );
                    Some((true, source.index, *timestamp_ns))
                }
                _ => None,
            })
            .collect();
        assert_eq!(trace.len(), 4);
        assert_eq!(
            trace
                .iter()
                .map(|(push, index, _)| (*push, *index))
                .collect::<Vec<_>>(),
            vec![
                (false, order[0]),
                (true, order[0]),
                (false, order[1]),
                (true, order[1])
            ]
        );
        assert!(trace.windows(2).all(|w| w[0].2 < w[1].2));
        assert_eq!(trace[1].2 - trace[0].2, 110);
        assert_eq!(trace[3].2 - trace[2].2, 110);
    }
    let final_start_push = events.iter().position(|e| matches!(&*e.observation, PipelineObservation::Push { frame, destination: None, .. } if frame.id == start)).unwrap();
    let started = events.iter().position(|e| matches!(&*e.observation, PipelineObservation::PipelineStarted { runtime_id, .. } if *runtime_id == runtime)).unwrap();
    assert!(started > final_start_push);
    for (sequence, event) in events.iter().enumerate() {
        assert_eq!(event.sequence, sequence as u64);
        assert_eq!(event.dropped_before, 0);
    }
    assert_eq!(
        handle.stats(),
        ObserverStats {
            delivered: events.len() as u64,
            dropped: 0,
            abandoned: 0,
            failures: 0
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clones_before_admission_share_first_push_but_duplicate_and_serde_restart_it() {
    let every = Arc::new(RecordingObserver::new());
    let first = Arc::new(RecordingObserver::new());
    let mut pipeline = Pipeline::new(vec![
        Arc::new(IdentityProcessor),
        Arc::new(IdentityProcessor),
    ]);
    pipeline.add_observer(every.clone(), ObserverOptions::default());
    pipeline.add_observer(
        first.clone(),
        ObserverOptions {
            observe_every_push: false,
            ..Default::default()
        },
    );
    let mut worker = bounded(pipeline.start(PipelineConfig::default()))
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let original = Frame::text("same logical frame");
    let id = original.id;
    let before_admission_a = original.clone();
    let before_admission_b = original.clone();
    for frame in [before_admission_a, before_admission_b] {
        bounded(worker.sender().send(frame, Direction::Downstream))
            .await
            .unwrap();
        bounded(worker.recv()).await.unwrap();
    }
    let duplicated = original.duplicate();
    let duplicate_id = duplicated.id;
    assert_ne!(duplicate_id, id);
    bounded(worker.sender().send(duplicated, Direction::Downstream))
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let json = serde_json::to_string(&original).unwrap();
    assert!(!json.contains("observed_runtimes"));
    let deserialized: Frame = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.id, id);
    for frame in [deserialized.clone(), deserialized] {
        bounded(worker.sender().send(frame, Direction::Downstream))
            .await
            .unwrap();
        bounded(worker.recv()).await.unwrap();
    }
    finish(worker).await;
    let flags: Vec<_> = every
        .events()
        .iter()
        .filter_map(|e| match &*e.observation {
            PipelineObservation::Push {
                frame, first_push, ..
            } if frame.id == id => Some(*first_push),
            _ => None,
        })
        .collect();
    assert_eq!(
        flags,
        vec![true, false, false, false, true, false, false, false]
    );
    assert_eq!(every.events().iter().filter(|e| matches!(&*e.observation, PipelineObservation::Push { frame, first_push: true, .. } if frame.id == duplicate_id)).count(), 1);
    let filtered = first.events();
    assert!(filtered.iter().all(|e| !matches!(
        &*e.observation,
        PipelineObservation::Push {
            first_push: false,
            ..
        }
    )));
    assert_eq!(
        filtered
            .iter()
            .filter(
                |e| matches!(&*e.observation, PipelineObservation::Push { frame, .. } if frame.id == id)
            )
            .count(),
        2
    );
    for (sequence, event) in filtered.iter().enumerate() {
        assert_eq!(event.sequence, sequence as u64);
    }
}

struct CallbackDrop(Arc<AtomicUsize>);
impl Drop for CallbackDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct GatedObserver {
    first: AtomicBool,
    entered: Semaphore,
    release: Semaphore,
    completed: Semaphore,
    dropped_callbacks: Arc<AtomicUsize>,
    events: Mutex<Vec<ObservationEvent>>,
}
impl GatedObserver {
    fn new() -> Self {
        Self {
            first: AtomicBool::new(true),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            completed: Semaphore::new(0),
            dropped_callbacks: Arc::new(AtomicUsize::new(0)),
            events: Mutex::new(Vec::new()),
        }
    }
}
#[async_trait]
impl PipelineObserver for GatedObserver {
    async fn on_event(&self, event: ObservationEvent) {
        if self.first.swap(false, Ordering::SeqCst) {
            let _guard = CallbackDrop(self.dropped_callbacks.clone());
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        self.events.lock().unwrap().push(event);
        self.completed.add_permits(1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_observer_queue_never_backpressures_processors_and_reports_loss() {
    let observer = Arc::new(GatedObserver::new());
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    let handle = pipeline.add_observer(
        observer.clone(),
        ObserverOptions {
            capacity: 2,
            ..Default::default()
        },
    );
    let mut worker = bounded(pipeline.start(PipelineConfig::default()))
        .await
        .unwrap();
    permit(&observer.entered).await;
    bounded(worker.recv()).await.unwrap();
    for _ in 0..16 {
        bounded(worker.sender().send(
            Frame::text("output without waiting for observer"),
            Direction::Downstream,
        ))
        .await
        .unwrap();
        bounded(worker.recv()).await.unwrap();
    }
    assert!(handle.stats().dropped > 0);
    assert_eq!(handle.stats().delivered, 0);
    observer.release.add_permits(1);
    for _ in 0..3 {
        permit(&observer.completed).await;
    }
    let marker = Frame::text("after queue loss");
    let marker_id = marker.id;
    bounded(worker.sender().send(marker, Direction::Downstream))
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    finish(worker).await;
    let events = observer.events.lock().unwrap();
    assert!(events.iter().any(|e| e.dropped_before > 0 && matches!(&*e.observation, PipelineObservation::Process { frame, .. } | PipelineObservation::Push { frame, .. } if frame.id == marker_id)));
    assert!(events.windows(2).any(|w| w[1].sequence > w[0].sequence + 1));
    assert!(events
        .windows(2)
        .all(|w| w[1].dropped_before >= w[0].dropped_before));
    let stats = handle.stats();
    assert_eq!(stats.delivered, events.len() as u64);
    assert_eq!(stats.failures, 0);
    assert_eq!(stats.abandoned, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_aborts_blocked_callbacks_instead_of_waiting_for_graceful_timeout() {
    let observer = Arc::new(GatedObserver::new());
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    let handle = pipeline.add_observer(
        observer.clone(),
        ObserverOptions {
            capacity: 64,
            drain_timeout: Duration::from_secs(60),
            ..Default::default()
        },
    );
    let mut worker = bounded(pipeline.start(PipelineConfig::default()))
        .await
        .unwrap();
    permit(&observer.entered).await;
    bounded(worker.recv()).await.unwrap();
    bounded(worker.cancel("cancel while observer is pending"))
        .await
        .unwrap();
    let report = bounded(worker.collect()).await.unwrap().1;
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(observer.dropped_callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(handle.stats().delivered, 0);
    assert!(handle.stats().abandoned >= 1);
    assert_eq!(handle.stats().failures, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_end_drains_healthy_observers_and_bounds_blocked_callbacks() {
    let healthy = Arc::new(RecordingObserver::new());
    let blocked = [
        Arc::new(GatedObserver::new()),
        Arc::new(GatedObserver::new()),
    ];
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    let healthy_handle = pipeline.add_observer(healthy.clone(), ObserverOptions::default());
    let handles: Vec<_> = blocked
        .iter()
        .map(|observer| {
            pipeline.add_observer(
                observer.clone(),
                ObserverOptions {
                    capacity: 64,
                    drain_timeout: Duration::from_millis(50),
                    ..Default::default()
                },
            )
        })
        .collect();
    let mut worker = bounded(pipeline.start(PipelineConfig::default()))
        .await
        .unwrap();
    for observer in &blocked {
        permit(&observer.entered).await;
    }
    bounded(worker.recv()).await.unwrap();
    assert_eq!(finish(worker).await.state, WorkerState::Ended);
    for (observer, handle) in blocked.iter().zip(handles) {
        assert_eq!(observer.dropped_callbacks.load(Ordering::SeqCst), 1);
        assert!(handle.stats().abandoned >= 1);
        assert_eq!(handle.stats().delivered, 0);
        assert_eq!(handle.stats().failures, 0);
    }
    assert_eq!(healthy_handle.stats().abandoned, 0);
    assert_eq!(healthy_handle.stats().dropped, 0);
    assert!(healthy.events().iter().any(|e| matches!(&*e.observation, PipelineObservation::Push { frame, destination: None, .. } if frame.kind == "EndFrame")));
}

struct PanicObserver;
#[async_trait]
impl PipelineObserver for PanicObserver {
    async fn on_event(&self, _: ObservationEvent) {
        panic!("observer callback failure");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_panic_is_isolated_counted_and_does_not_break_another_observer() {
    let healthy = Arc::new(RecordingObserver::new());
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    let bad = pipeline.add_observer(Arc::new(PanicObserver), ObserverOptions::default());
    let good = pipeline.add_observer(healthy.clone(), ObserverOptions::default());
    let mut worker = bounded(pipeline.start(PipelineConfig::default()))
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let frame = Frame::text("after panic");
    let id = frame.id;
    bounded(worker.sender().send(frame, Direction::Downstream))
        .await
        .unwrap();
    assert_eq!(bounded(worker.recv()).await.unwrap().frame.id, id);
    let report = finish(worker).await;
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty());
    assert_eq!(bad.stats().failures, 1);
    assert_eq!(bad.stats().delivered, 0);
    assert!(bad.stats().abandoned >= 1);
    assert_eq!(good.stats().failures, 0);
    assert_eq!(good.stats().dropped, 0);
    assert_eq!(good.stats().abandoned, 0);
    assert!(healthy.events().iter().any(
        |e| matches!(&*e.observation, PipelineObservation::Push { frame, .. } if frame.id == id)
    ));
}

struct OverflowProcessor {
    accepted: Frame,
    rejected: Frame,
    allow_failure: Semaphore,
}
#[async_trait]
impl FrameProcessor for OverflowProcessor {
    fn name(&self) -> &str {
        "overflow"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            ctx.push(self.accepted.clone(), direction).await?;
            self.allow_failure.acquire().await.unwrap().forget();
            ctx.push(self.rejected.clone(), direction).await
        } else {
            ctx.push(frame, direction).await
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_boundary_admission_never_publishes_a_successful_push() {
    let observer = Arc::new(RecordingObserver::new());
    let processor = Arc::new(OverflowProcessor {
        accepted: Frame::text("accepted"),
        rejected: Frame::text("rejected"),
        allow_failure: Semaphore::new(0),
    });
    let mut pipeline = Pipeline::new(vec![processor.clone()]);
    pipeline.add_observer(observer.clone(), ObserverOptions::default());
    let mut worker = bounded(pipeline.start(PipelineConfig {
        output_capacity: 1,
        ..Default::default()
    }))
    .await
    .unwrap();
    bounded(worker.recv()).await.unwrap();
    bounded(
        worker
            .sender()
            .send(Frame::text("trigger"), Direction::Downstream),
    )
    .await
    .unwrap();
    observer.wait_for(|event| matches!(&*event.observation, PipelineObservation::Push { frame, .. } if frame.id == processor.accepted.id)).await;
    processor.allow_failure.add_permits(1);
    assert!(matches!(
        bounded(worker.join()).await,
        Err(PipelineError::Backpressure(_))
    ));
    let events = observer.events();
    assert_eq!(events.iter().filter(|e| matches!(&*e.observation, PipelineObservation::Push { frame, first_push: true, .. } if frame.id == processor.accepted.id)).count(), 1);
    assert!(!events.iter().any(|e| matches!(&*e.observation, PipelineObservation::Push { frame, .. } if frame.id == processor.rejected.id)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observations_exclude_raw_content_metadata_and_transport_identifiers() {
    let observer = Arc::new(RecordingObserver::new());
    let pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .with_observer(observer.clone(), ObserverOptions::default());
    let mut worker = bounded(pipeline.start(PipelineConfig::default()))
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let transcription = Frame::new(FrameData::Transcription(TranscriptionFrame {
        text: "PRIVATE_TRANSCRIPT_71".into(),
        user_id: "PRIVATE_USER_72".into(),
        timestamp: "PRIVATE_WALL_TIME_73".into(),
        result: Some(serde_json::json!({"secret": "PRIVATE_PROVIDER_RESULT_74"})),
        ..Default::default()
    }));
    let audio = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(b"PRIVATE_AUDIO_7500".to_vec(), 16000, 1).unwrap(),
    ));
    let error = Frame::new(FrameData::Error(ErrorFrame::new("PRIVATE_ERROR_76")));
    for mut frame in [Frame::text("PRIVATE_TEXT_77"), transcription, audio, error] {
        frame.name = "PRIVATE_FRAME_NAME_78".into();
        frame
            .metadata
            .insert("private".into(), serde_json::json!("PRIVATE_METADATA_79"));
        frame.transport_source = Some("PRIVATE_SOURCE_80".into());
        frame.transport_destination = Some("PRIVATE_DESTINATION_81".into());
        frame.pts = Some(9_876_543_210);
        bounded(worker.sender().send(frame, Direction::Downstream))
            .await
            .unwrap();
        bounded(worker.recv()).await.unwrap();
    }
    finish(worker).await;
    let events = observer.events();
    let debug = format!("{events:?}");
    for secret in [
        "PRIVATE_",
        "9_876_543_210",
        "9876543210",
        "80, 82, 73, 86, 65, 84, 69",
    ] {
        assert!(
            !debug.contains(secret),
            "observer retained private payload: {secret}"
        );
    }
    assert!(events.iter().any(|e| matches!(&*e.observation, PipelineObservation::Process { frame, .. } if frame.kind == "TranscriptionFrame")));
    assert!(events.iter().any(|e| matches!(&*e.observation, PipelineObservation::Process { frame, .. } if frame.kind == "InputAudioRawFrame")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reported_metric_snapshots_are_bounded_and_keep_explicit_loss_count() {
    let observer = Arc::new(RecordingObserver::new());
    let pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .with_observer(observer.clone(), ObserverOptions::default());
    let mut worker = bounded(pipeline.start(PipelineConfig::default()))
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let frame = Frame::new(FrameData::Metrics(
        (0..100)
            .map(|i| Metric::Ttfb {
                processor: "service".into(),
                seconds: i as f64,
            })
            .collect(),
    ));
    let id = frame.id;
    bounded(worker.sender().send(frame, Direction::Downstream))
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    finish(worker).await;
    for event in observer.events() {
        if let PipelineObservation::Process { frame, .. }
        | PipelineObservation::Push { frame, .. } = &*event.observation
        {
            if frame.id == id {
                assert_eq!(frame.metrics.len(), 64);
                assert_eq!(frame.omitted_metrics, 36);
            }
        }
    }
}

struct SetupFailure(bool);
#[async_trait]
impl FrameProcessor for SetupFailure {
    fn name(&self) -> &str {
        "setup-failure"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        assert!(!self.0, "setup panic");
        Err(ProcessorError::new("setup error"))
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setup_failure_and_panic_have_explicit_distinct_outcomes() {
    for panic in [false, true] {
        let observer = Arc::new(RecordingObserver::new());
        let pipeline = Pipeline::new(vec![Arc::new(SetupFailure(panic))])
            .with_observer(observer.clone(), ObserverOptions::default());
        let worker = bounded(pipeline.start(PipelineConfig::default()))
            .await
            .unwrap();
        assert_eq!(finish(worker).await.state, WorkerState::Ended);
        let outcomes: Vec<_> = observer
            .events()
            .iter()
            .filter_map(|e| match &*e.observation {
                PipelineObservation::ProcessorSetup { outcome, .. } => Some(*outcome),
                _ => None,
            })
            .collect();
        assert_eq!(
            outcomes,
            vec![if panic {
                SetupOutcome::Aborted
            } else {
                SetupOutcome::Failed
            }]
        );
    }
}

#[tokio::test]
async fn zero_capacity_observer_is_rejected_before_spawning_dispatch() {
    let mut pipeline = Pipeline::new(vec![]);
    let handle = pipeline.add_observer(
        Arc::new(PanicObserver),
        ObserverOptions {
            capacity: 0,
            ..Default::default()
        },
    );
    assert!(matches!(
        pipeline.start(PipelineConfig::default()).await,
        Err(PipelineError::InvalidConfiguration(_))
    ));
    assert_eq!(
        handle.stats(),
        ObserverStats {
            delivered: 0,
            dropped: 0,
            abandoned: 0,
            failures: 0
        }
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disabled_observers_skip_all_injected_clock_reads() {
    let clock = Arc::new(TickClock::default());
    let pipeline = Pipeline::new(vec![
        Arc::new(IdentityProcessor),
        Arc::new(IdentityProcessor),
    ])
    .with_observer_clock(clock.clone());
    let mut worker = bounded(pipeline.start(PipelineConfig::default()))
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    for frame in [
        Frame::text("normal"),
        Frame::new(FrameData::InputText(TextFrame::new("system"))),
    ] {
        bounded(worker.sender().send(frame, Direction::Downstream))
            .await
            .unwrap();
        bounded(worker.recv()).await.unwrap();
    }
    assert_eq!(finish(worker).await.state, WorkerState::Ended);
    assert_eq!(
        clock.0.load(Ordering::SeqCst),
        0,
        "disabled observers must not read the observation clock"
    );
}
