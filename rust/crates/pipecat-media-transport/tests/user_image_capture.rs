//! Synthetic application capture through the public shared input transport.
//! These tests do not open a physical camera or attest provider transport I/O.
use async_trait::async_trait;
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, Mutex as AsyncMutex},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);
type TestResult<T> = Result<T, String>;
async fn bounded<T, E: std::fmt::Display>(
    label: &str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    timeout(WAIT, future)
        .await
        .map_err(|_| format!("{label}: deadline"))?
        .map_err(|error| format!("{label}: {error}"))
}
struct CaptureCommand {
    frames: Vec<Frame>,
    result: oneshot::Sender<Vec<TestResult<()>>>,
}
struct Backend {
    started: AtomicUsize,
    captures: AtomicUsize,
    active: AtomicUsize,
    stopped: AtomicUsize,
    commands: AsyncMutex<mpsc::Receiver<CaptureCommand>>,
}
struct ActiveCapture<'a>(&'a AtomicUsize);
impl Drop for ActiveCapture<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl InputBackend for Backend {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn capture(&self, ingress: InputIngress) -> Result<(), ProcessorError> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveCapture(&self.active);
        let mut commands = self.commands.lock().await;
        while let Some(command) = commands.recv().await {
            // Admission occurs inside the actual processor-owned capture task.
            let results = command
                .frames
                .into_iter()
                .map(|frame| ingress.push(frame).map_err(|error| error.to_string()))
                .collect();
            let _ = command.result.send(results);
        }
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[derive(Default)]
struct Sink {
    frames: Mutex<Vec<DirectedFrame>>,
}
#[async_trait]
impl FrameProcessor for Sink {
    fn name(&self) -> &str {
        "captured-image-sink"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.frames
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}
struct Case {
    worker: PipelineWorker,
    ingress: InputIngress,
    commands: mpsc::Sender<CaptureCommand>,
    backend: Arc<Backend>,
    sink: Arc<Sink>,
}
impl Case {
    async fn start(config: InputTransportConfig) -> TestResult<Self> {
        let (commands, receive) = mpsc::channel(1);
        let backend = Arc::new(Backend {
            started: AtomicUsize::new(0),
            captures: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
            commands: AsyncMutex::new(receive),
        });
        let input = Arc::new(
            InputTransport::new("application-image-capture", config, backend.clone(), None)
                .map_err(|error| error.to_string())?,
        );
        let ingress = input.ingress();
        let sink = Arc::new(Sink::default());
        let worker = Pipeline::new(vec![input, sink.clone()])
            .start(PipelineConfig {
                startup_timeout: Duration::from_secs(2),
                shutdown_timeout: Duration::from_secs(2),
                ..Default::default()
            })
            .await
            .map_err(|error| error.to_string())?;
        Ok(Self {
            worker,
            ingress,
            commands,
            backend,
            sink,
        })
    }
    async fn capture(&self, frames: Vec<Frame>) -> TestResult<Vec<TestResult<()>>> {
        let (result, receive) = oneshot::channel();
        bounded(
            "application capture command",
            self.commands.send(CaptureCommand { frames, result }),
        )
        .await?;
        bounded("capture admission result", receive).await
    }
    async fn finish(self) -> TestResult<(Vec<DirectedFrame>, WorkerReport)> {
        finish(self.worker).await
    }
}
async fn finish(worker: PipelineWorker) -> TestResult<(Vec<DirectedFrame>, WorkerReport)> {
    let sender = worker.sender();
    let terminal = bounded("End", sender.end()).await;
    if terminal.is_err() {
        let _ = bounded("Cancel failed End", sender.cancel("capture End failed")).await;
    }
    let mut collection = tokio::spawn(worker.collect());
    let collected = match timeout(WAIT, &mut collection).await {
        Ok(result) => result
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string())),
        Err(_) => {
            let _ = bounded(
                "Cancel collection",
                sender.cancel("capture collect deadline"),
            )
            .await;
            match timeout(WAIT, &mut collection).await {
                Ok(result) => Err(format!(
                    "collection deadline; cancelled and joined: {result:?}"
                )),
                Err(_) => {
                    collection.abort();
                    let _ = collection.await;
                    Err("collection deadline after cancellation; aborted and joined".into())
                }
            }
        }
    };
    terminal.and(collected)
}
fn config() -> InputTransportConfig {
    InputTransportConfig {
        audio_enabled: false,
        video_enabled: true,
        operation_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}
fn image(user: bool, pixel: u8) -> Frame {
    let image = ImageRawFrame {
        image: vec![pixel, pixel.wrapping_add(1), pixel.wrapping_add(2)],
        size: (1, 1),
        format: Some("RGB".into()),
    };
    let mut frame = Frame::new(if user {
        FrameData::UserImage(UserImageRawFrame {
            image,
            user_id: "streaming-participant".into(),
            text: Some("application image context".into()),
            append_to_context: Some(true),
            request: None,
            run_llm: Some(false),
        })
    } else {
        FrameData::InputImage(image)
    });
    frame.name = format!("application-image-{user}-{pixel}");
    frame.set_pipeline_pts(123_456);
    frame.transport_source = Some("application-camera".into());
    frame.transport_destination = Some("vision-consumer".into());
    frame.interruptible = false;
    frame.broadcast_sibling_id = Some(987_654);
    frame.metadata.insert(
        "capture".into(),
        serde_json::json!({"source": "application", "values": [7, null, true]}),
    );
    frame
}
fn images(frames: &[DirectedFrame]) -> Vec<&DirectedFrame> {
    frames
        .iter()
        .filter(|frame| {
            matches!(
                frame.frame.data,
                FrameData::InputImage(_) | FrameData::UserImage(_)
            )
        })
        .collect()
}
fn assert_preserved(actual: &DirectedFrame, expected: &Frame, tag: &LocalFrameTag) {
    assert_eq!(actual.direction, Direction::Downstream);
    assert_eq!(actual.frame, *expected);
    assert_eq!(actual.frame.id, expected.id);
    assert_eq!(
        actual.frame.observation_token(),
        expected.observation_token()
    );
    assert_eq!(actual.frame.metadata, expected.metadata);
    assert_eq!(actual.frame.transport_source, expected.transport_source);
    assert_eq!(
        actual.frame.transport_destination,
        expected.transport_destination
    );
    assert_eq!(actual.frame.has_pipeline_pts(), expected.has_pipeline_pts());
    assert!(tag.contains(&actual.frame));
}
fn assert_stopped(backend: &Backend, report: &WorkerReport) {
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.captures.load(Ordering::SeqCst), 1);
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn application_backend_captures_unrequested_user_image_without_reconstruction() {
    let tag = LocalFrameTag::new();
    let mut user = image(true, 10);
    let mut plain = image(false, 20);
    tag.mark(&mut user).unwrap();
    tag.mark(&mut plain).unwrap();
    let case = Case::start(config()).await.expect("start capture pipeline");
    let backend = case.backend.clone();
    let sink = case.sink.clone();
    let ingress = case.ingress.clone();
    // Keep both results, including a rejected UserImage; the positive plain
    // control and graceful terminal path do not depend on UserImage admission.
    let admitted = case.capture(vec![user.clone(), plain.clone()]).await;
    let collected = case.finish().await;
    let (frames, report) = collected.expect("End drains and joins owned capture");
    assert_stopped(&backend, &report);
    assert_eq!(ingress.pending_image_requests(), 0);
    assert!(!ingress.is_ready());
    let admitted = admitted.expect("capture task returned both admission results");
    eprintln!(
        "capture admissions after cleanup: {admitted:?}; images={}",
        images(&frames).len()
    );
    assert_eq!(admitted.len(), 2);
    assert!(
        admitted[1].is_ok(),
        "plain InputImage control: {:?}",
        admitted[1]
    );
    let plain_received: Vec<_> = images(&frames)
        .into_iter()
        .filter(|frame| frame.frame.id == plain.id)
        .collect();
    assert_eq!(plain_received.len(), 1);
    assert_preserved(plain_received[0], &plain, &tag);
    assert!(
        admitted[0].is_ok(),
        "unrequested UserImage capture: {:?}",
        admitted[0]
    );
    let received = images(&frames);
    assert_eq!(received.len(), 2);
    assert_preserved(received[0], &user, &tag);
    assert_preserved(received[1], &plain, &tag);
    let recorded = sink.frames.lock().unwrap();
    let at_sink = images(&recorded);
    assert_eq!(at_sink.len(), 2);
    assert_preserved(at_sink[0], &user, &tag);
    assert_preserved(at_sink[1], &plain, &tag);
}

// Additive controls: the public baseline above remains byte-for-byte unchanged.
use pipecat_media_transport::video::{
    VideoInSourceParams, VideoParticipantId, VideoSourceId, VideoSourceKey,
};
use tokio::sync::Semaphore;

#[tokio::test]
async fn capture_video_gates_drop_disabled_and_paused_then_accept_after_resume() {
    for disabled in [false, true] {
        let mut settings = config();
        settings.video_enabled = !disabled;
        let case = Case::start(settings).await.expect("start gated capture");
        let backend = case.backend.clone();
        let ingress = case.ingress.clone();
        let ready = case.capture(Vec::new()).await;
        ingress.pause();
        let paused = case.capture(vec![image(true, 30), image(false, 31)]).await;
        ingress.resume();
        let tag = LocalFrameTag::new();
        let mut user = image(true, 32);
        let mut plain = image(false, 33);
        let marked = [tag.mark(&mut user), tag.mark(&mut plain)];
        let resumed = case.capture(vec![user.clone(), plain.clone()]).await;
        let collected = case.finish().await;
        let (frames, report) = collected.expect("gated capture joined");
        assert_stopped(&backend, &report);
        ready.expect("capture readiness barrier");
        assert!(marked.iter().all(Result::is_ok));
        assert!(paused.unwrap().iter().all(Result::is_ok));
        assert!(resumed.unwrap().iter().all(Result::is_ok));
        assert_eq!(ingress.pending_image_requests(), 0);
        let received = images(&frames);
        if disabled {
            assert!(received.is_empty());
        } else {
            assert_eq!(received.len(), 2);
            assert_preserved(received[0], &user, &tag);
            assert_preserved(received[1], &plain, &tag);
        }
    }
}

struct Gate {
    entered: Semaphore,
    release: Semaphore,
    active: AtomicUsize,
    started: AtomicUsize,
    stopped: AtomicUsize,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            active: AtomicUsize::new(0),
            started: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
        })
    }
}
#[async_trait]
impl AudioFilter for Gate {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn filter(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        Ok(audio)
    }
    async fn control(&self, _: &FrameData) -> Result<(), ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveCapture(&self.active);
        self.entered.add_permits(1);
        self.release
            .acquire()
            .await
            .map_err(|error| ProcessorError::new(error.to_string()))?
            .forget();
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct EndProbe {
    input: Arc<InputTransport>,
    ended: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for EndProbe {
    fn name(&self) -> &str {
        self.input.name()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.input.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.input.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let result = self.input.prepare_end(ctx).await;
        self.ended.add_permits(1);
        result
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.input.cleanup().await
    }
}
async fn controlled(
    settings: InputTransportConfig,
    gate: Option<Arc<Gate>>,
) -> TestResult<(Case, Arc<Semaphore>, Vec<TestResult<()>>)> {
    let (commands, receive) = mpsc::channel(1);
    let backend = Arc::new(Backend {
        started: AtomicUsize::new(0),
        captures: AtomicUsize::new(0),
        active: AtomicUsize::new(0),
        stopped: AtomicUsize::new(0),
        commands: AsyncMutex::new(receive),
    });
    let input = Arc::new(
        InputTransport::new(
            "controlled-image-capture",
            settings,
            backend.clone(),
            gate.map(|gate| gate as Arc<dyn AudioFilter>),
        )
        .map_err(|error| error.to_string())?,
    );
    let ingress = input.ingress();
    // Readiness takes precedence even when capture is also paused or disabled.
    ingress.pause();
    let before = [image(true, 40), image(false, 41)]
        .into_iter()
        .map(|frame| ingress.push(frame).map_err(|error| error.to_string()))
        .collect();
    ingress.resume();
    let ended = Arc::new(Semaphore::new(0));
    let sink = Arc::new(Sink::default());
    let worker = Pipeline::new(vec![
        Arc::new(EndProbe {
            input,
            ended: ended.clone(),
        }),
        sink.clone(),
    ])
    .start(PipelineConfig {
        startup_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(2),
        ..Default::default()
    })
    .await
    .map_err(|error| error.to_string())?;
    Ok((
        Case {
            worker,
            ingress,
            commands,
            backend,
            sink,
        },
        ended,
        before,
    ))
}
async fn hold_worker(case: &Case, gate: &Gate) -> TestResult<()> {
    case.capture(Vec::new()).await?;
    bounded(
        "enqueue held filter control",
        case.worker
            .sender()
            .send(Frame::new(FrameData::FilterControl), Direction::Downstream),
    )
    .await?;
    bounded("owned media worker entered barrier", gate.entered.acquire())
        .await?
        .forget();
    Ok(())
}
async fn receive_ids(
    worker: &mut PipelineWorker,
    wanted: &[u64],
) -> TestResult<Vec<DirectedFrame>> {
    let mut missing: std::collections::BTreeSet<_> = wanted.iter().copied().collect();
    timeout(WAIT, async {
        let mut observed = Vec::new();
        while !missing.is_empty() {
            let frame = worker
                .recv()
                .await
                .ok_or("pipeline closed before image barrier")?;
            missing.remove(&frame.frame.id);
            observed.push(frame);
        }
        Ok(observed)
    })
    .await
    .map_err(|_| "image traversal barrier deadline".to_owned())?
}
async fn collect_terminal(
    worker: PipelineWorker,
) -> TestResult<(Vec<DirectedFrame>, WorkerReport)> {
    let sender = worker.sender();
    let mut collection = tokio::spawn(worker.collect());
    match timeout(WAIT, &mut collection).await {
        Ok(result) => result
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string()),
        Err(_) => {
            let _ = bounded(
                "Cancel terminal collection",
                sender.cancel("terminal deadline"),
            )
            .await;
            match timeout(WAIT, &mut collection).await {
                Ok(result) => Err(format!(
                    "terminal deadline; cancelled and joined: {result:?}"
                )),
                Err(_) => {
                    collection.abort();
                    let _ = collection.await;
                    Err("terminal deadline after cancellation; aborted and joined".into())
                }
            }
        }
    }
}
fn assert_gate_stopped(gate: &Gate) {
    assert_eq!(gate.active.load(Ordering::SeqCst), 0);
    assert_eq!(gate.started.load(Ordering::SeqCst), 1);
    assert_eq!(gate.stopped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn capture_readiness_precedes_pause_and_disabled_gates() {
    for disabled in [false, true] {
        let mut settings = config();
        settings.video_enabled = !disabled;
        let (case, _, before) = controlled(settings, None)
            .await
            .expect("start readiness control");
        let backend = case.backend.clone();
        let ingress = case.ingress.clone();
        let ready = case.capture(Vec::new()).await;
        let collected = case.finish().await;
        let after = [image(true, 42), image(false, 43)].map(|frame| ingress.push(frame));
        let (frames, report) = collected.expect("readiness control joined");
        assert_stopped(&backend, &report);
        ready.expect("capture ready barrier");
        for result in before {
            assert_eq!(result.unwrap_err(), "input transport is not ready");
        }
        for result in after {
            assert_eq!(result.unwrap_err().message, "input transport is not ready");
        }
        assert!(images(&frames).is_empty());
    }
}

#[tokio::test]
async fn capture_count_bound_rejects_then_recovers_on_the_owned_worker() {
    let mut settings = config();
    settings.queue_capacity = 2;
    let gate = Gate::new();
    let (mut case, _, _) = controlled(settings, Some(gate.clone()))
        .await
        .expect("start count control");
    let backend = case.backend.clone();
    let expected = [image(false, 50), image(true, 51), image(false, 52)];
    let scenario: TestResult<_> = async {
        hold_worker(&case, &gate).await?;
        let admissions = case.capture(expected.to_vec()).await?;
        // The bounded queue cannot drain while the owned worker holds this gate.
        gate.release.add_permits(1);
        let accepted: Vec<_> = expected
            .iter()
            .zip(&admissions)
            .filter(|(_, result)| result.is_ok())
            .map(|(frame, _)| frame.id)
            .collect();
        let observed = receive_ids(&mut case.worker, &accepted).await?;
        let recovery = case.capture(vec![image(true, 53)]).await?;
        Ok((admissions, observed, recovery))
    }
    .await;
    gate.release.add_permits(1);
    let collected = case.finish().await;
    let (remaining, report) = collected.expect("count control joined");
    assert_stopped(&backend, &report);
    assert_gate_stopped(&gate);
    let (admissions, mut observed, recovery) = scenario.expect("held count and recovery scenario");
    observed.extend(remaining);
    assert!(admissions[0].is_ok());
    assert!(admissions[1].is_ok(), "{:?}", admissions[1]);
    assert_eq!(
        admissions[2].as_ref().unwrap_err(),
        "bounded media queue is full"
    );
    assert!(recovery[0].is_ok(), "{:?}", recovery[0]);
    assert_eq!(images(&observed).len(), 3);
}
fn budget_image(user: bool, bytes: usize) -> Frame {
    let raw = ImageRawFrame {
        image: vec![1; bytes],
        size: (1, 1),
        format: None,
    };
    let mut frame = Frame::new(if user {
        FrameData::UserImage(UserImageRawFrame {
            image: raw,
            user_id: "u".into(),
            text: None,
            append_to_context: None,
            request: None,
            run_llm: None,
        })
    } else {
        FrameData::InputImage(raw)
    });
    frame.name = "budget".into();
    frame
}

#[tokio::test]
async fn capture_byte_bound_charges_user_fields_envelope_and_recovers() {
    let mut settings = config();
    settings.queue_capacity = 2;
    settings.queue_byte_capacity = 200;
    let gate = Gate::new();
    let (mut case, _, _) = controlled(settings, Some(gate.clone()))
        .await
        .expect("start byte control");
    let backend = case.backend.clone();
    let mut oversized = Vec::new();
    for field in [
        "pixels",
        "user",
        "text",
        "format",
        "name",
        "source",
        "destination",
        "metadata",
        "nested-null",
    ] {
        let mut frame = budget_image(true, 3);
        let FrameData::UserImage(user) = &mut frame.data else {
            unreachable!()
        };
        match field {
            "pixels" => user.image.image = vec![0; 256],
            "user" => user.user_id = "u".repeat(256),
            "text" => user.text = Some("t".repeat(256)),
            "format" => user.image.format = Some("f".repeat(256)),
            "name" => frame.name = "n".repeat(256),
            "source" => frame.transport_source = Some("s".repeat(256)),
            "destination" => frame.transport_destination = Some("d".repeat(256)),
            "metadata" => {
                frame
                    .metadata
                    .insert("m".into(), serde_json::json!("x".repeat(256)));
            }
            _ => {
                frame.metadata.insert(
                    "m".into(),
                    serde_json::json!(vec![serde_json::Value::Null; 256]),
                );
            }
        }
        oversized.push(frame);
    }
    let plain = budget_image(false, 120);
    let user = budget_image(true, 120);
    let scenario: TestResult<_> = async {
        hold_worker(&case, &gate).await?;
        let large = case.capture(oversized).await?;
        let admissions = case.capture(vec![plain.clone(), user.clone()]).await?;
        gate.release.add_permits(1);
        let observed = receive_ids(&mut case.worker, &[plain.id]).await?;
        let recovery = case.capture(vec![user.clone()]).await?;
        Ok((large, admissions, observed, recovery))
    }
    .await;
    gate.release.add_permits(1);
    let collected = case.finish().await;
    let (remaining, report) = collected.expect("byte control joined");
    assert_stopped(&backend, &report);
    assert_gate_stopped(&gate);
    let (large, admissions, mut observed, recovery) =
        scenario.expect("held byte and recovery scenario");
    observed.extend(remaining);
    for result in large {
        assert_eq!(result.unwrap_err(), "bounded media queue is full");
    }
    assert!(admissions[0].is_ok());
    assert_eq!(
        admissions[1].as_ref().unwrap_err(),
        "bounded media queue is full"
    );
    assert!(recovery[0].is_ok(), "{:?}", recovery[0]);
    let received = images(&observed);
    assert_eq!(received.len(), 2);
    assert_eq!(received[1].frame.id, user.id);
    assert_eq!(
        received[1].frame.observation_token(),
        user.observation_token()
    );
}

#[tokio::test]
async fn end_drains_held_capture_and_cancel_discards_it_without_detached_tasks() {
    for cancel in [false, true] {
        let mut settings = config();
        settings.queue_capacity = 2;
        let gate = Gate::new();
        let (case, ended, _) = controlled(settings, Some(gate.clone()))
            .await
            .expect("start terminal control");
        let backend = case.backend.clone();
        let ingress = case.ingress.clone();
        let tag = LocalFrameTag::new();
        let mut user = image(true, 60);
        let mut plain = image(false, 61);
        let marked = [tag.mark(&mut user), tag.mark(&mut plain)];
        let scenario: TestResult<_> = async {
            hold_worker(&case, &gate).await?;
            let admitted = case.capture(vec![user.clone(), plain.clone()]).await?;
            if cancel {
                bounded(
                    "Cancel held capture",
                    case.worker.sender().cancel("cancel capture test"),
                )
                .await?;
            } else {
                bounded("End held capture", case.worker.sender().end()).await?;
                bounded("input prepare_end completed", ended.acquire())
                    .await?
                    .forget();
                // End sealed the real queue while the worker still holds its gate.
                gate.release.add_permits(1);
            }
            Ok(admitted)
        }
        .await;
        if scenario.is_err() {
            gate.release.add_permits(1);
            let _ = bounded(
                "Cancel failed terminal scenario",
                case.worker.sender().cancel("scenario failed"),
            )
            .await;
        }
        let collected = collect_terminal(case.worker).await;
        gate.release.add_permits(1);
        let after = ingress.push(image(true, 62));
        let (frames, report) = collected.expect("held terminal pipeline joined");
        assert!(marked.iter().all(Result::is_ok));
        assert!(scenario.unwrap().iter().all(Result::is_ok));
        assert_gate_stopped(&gate);
        assert_eq!(backend.active.load(Ordering::SeqCst), 0);
        assert_eq!(backend.started.load(Ordering::SeqCst), 1);
        assert_eq!(backend.captures.load(Ordering::SeqCst), 1);
        assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
        assert_eq!(ingress.pending_image_requests(), 0);
        assert!(after.is_err());
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let received = images(&frames);
        if cancel {
            assert_eq!(report.state, WorkerState::Cancelled);
            assert!(received.is_empty());
        } else {
            assert_eq!(report.state, WorkerState::Ended);
            assert_eq!(received.len(), 2);
            assert_preserved(received[0], &user, &tag);
            assert_preserved(received[1], &plain, &tag);
        }
    }
}

#[tokio::test]
async fn request_bearing_capture_cannot_take_keyed_request_ownership() {
    let mut case = Case::start(config())
        .await
        .expect("start request ownership control");
    let backend = case.backend.clone();
    let ingress = case.ingress.clone();
    let callbacks = Arc::new(Mutex::new(Vec::new()));
    let callback_log = callbacks.clone();
    let request = UserImageRequestFrame {
        user_id: "owned-user".into(),
        text: Some("owned question".into()),
        append_to_context: Some(true),
        video_source: Some("camera".into()),
        function_name: Some("look".into()),
        tool_call_id: Some("owned-call".into()),
        result_callback: Some(LocalResultCallback::new(move |value| {
            callback_log.lock().unwrap().push(value);
            async { Ok(()) }
        })),
    };
    let key = VideoSourceKey::new(
        VideoParticipantId::new("owned-user").unwrap(),
        VideoSourceId::camera(),
    );
    let mut forged = image(true, 70);
    let FrameData::UserImage(payload) = &mut forged.data else {
        unreachable!()
    };
    payload.request = Some(Box::new(request.clone()));
    let unrequested = image(true, 71);
    let owned = image(false, 72);
    let scenario: TestResult<_> = async {
        case.capture(Vec::new()).await?;
        ingress
            .capture_video_source(key.clone(), VideoInSourceParams::on_request_only(), true)
            .map_err(|error| error.to_string())?;
        ingress
            .request_image(Frame::new(FrameData::UserImageRequest(request)))
            .map_err(|error| error.to_string())?;
        let wrong = case
            .capture(vec![
                forged.clone(),
                Frame::new(FrameData::OutputImage(ImageRawFrame {
                    image: vec![1, 2, 3],
                    size: (1, 1),
                    format: None,
                })),
                Frame::text("wrong capture family"),
            ])
            .await?;
        let generic = case.capture(vec![unrequested.clone()]).await?;
        // Wait only for admitted frames so old rejection still reaches cleanup.
        let observed = if generic[0].is_ok() {
            receive_ids(&mut case.worker, &[unrequested.id]).await?
        } else {
            Vec::new()
        };
        let pending_before_keyed = ingress.pending_image_requests();
        let callbacks_before_keyed = callbacks.lock().unwrap().len();
        let keyed_user = ingress.push_video_frame(&key, unrequested.clone());
        let keyed_plain = ingress.push_video_frame(&key, owned.clone());
        Ok((
            wrong,
            generic,
            observed,
            pending_before_keyed,
            callbacks_before_keyed,
            keyed_user,
            keyed_plain,
        ))
    }
    .await;
    let collected = case.finish().await;
    let (remaining, report) = collected.expect("request ownership pipeline joined");
    assert_stopped(&backend, &report);
    let (wrong, generic, mut observed, pending, called, keyed_user, keyed_plain) =
        scenario.expect("request ownership scenario");
    observed.extend(remaining);
    assert!(wrong.iter().all(Result::is_err), "{wrong:?}");
    assert!(generic[0].is_ok(), "{:?}", generic[0]);
    assert_eq!(pending, 1);
    assert_eq!(called, 0);
    assert!(keyed_user.is_err());
    assert!(keyed_plain.is_ok());
    assert_eq!(ingress.pending_image_requests(), 0);
    assert_eq!(*callbacks.lock().unwrap(), vec![serde_json::Value::Null]);
    let received = images(&observed);
    assert_eq!(received.len(), 2);
    assert_eq!(received[0].frame, unrequested);
    assert_eq!(
        received[0].frame.observation_token(),
        unrequested.observation_token()
    );
    assert_eq!(received[1].frame.id, owned.id);
    let FrameData::UserImage(image) = &received[1].frame.data else {
        panic!("expected keyed UserImage")
    };
    assert_eq!(image.user_id, "owned-user");
    assert_eq!(image.text.as_deref(), Some("owned question"));
    assert_eq!(image.append_to_context, Some(true));
    assert_eq!(image.run_llm, None);
    assert_eq!(
        image.request.as_ref().unwrap().tool_call_id.as_deref(),
        Some("owned-call")
    );
    assert!(image.request.as_ref().unwrap().result_callback.is_none());
    assert_eq!(
        received[1].frame.transport_source.as_deref(),
        Some("camera")
    );
    assert!(received.iter().all(|frame| frame.frame.id != forged.id));
}

#[tokio::test]
async fn direct_user_image_process_keeps_both_directions_even_when_capture_is_disabled_and_paused()
{
    let mut settings = config();
    settings.video_enabled = false;
    let mut case = Case::start(settings)
        .await
        .expect("start passthrough control");
    let backend = case.backend.clone();
    let ingress = case.ingress.clone();
    ingress.pause();
    let tag = LocalFrameTag::new();
    let mut expected = Vec::new();
    let callbacks = Arc::new(AtomicUsize::new(0));
    for direction in [Direction::Downstream, Direction::Upstream] {
        for correlated in [false, true] {
            let mut frame = image(
                true,
                if direction == Direction::Downstream {
                    80
                } else {
                    81
                },
            );
            if correlated {
                let callback_count = callbacks.clone();
                let FrameData::UserImage(user) = &mut frame.data else {
                    unreachable!()
                };
                user.request = Some(Box::new(UserImageRequestFrame {
                    user_id: "pipeline-only-user".into(),
                    text: None,
                    append_to_context: None,
                    video_source: Some("camera".into()),
                    function_name: None,
                    tool_call_id: None,
                    result_callback: Some(LocalResultCallback::new(move |_| {
                        callback_count.fetch_add(1, Ordering::SeqCst);
                        async { Ok(()) }
                    })),
                }));
            }
            let marked = tag.mark(&mut frame);
            expected.push((frame, direction, marked));
        }
    }
    let scenario: TestResult<_> = async {
        case.capture(Vec::new()).await?;
        for (frame, direction, _) in &expected {
            bounded(
                "direct UserImage injection",
                case.worker.sender().send(frame.clone(), *direction),
            )
            .await?;
        }
        receive_ids(
            &mut case.worker,
            &expected
                .iter()
                .map(|(frame, _, _)| frame.id)
                .collect::<Vec<_>>(),
        )
        .await
    }
    .await;
    let collected = case.finish().await;
    let (remaining, report) = collected.expect("direct passthrough pipeline joined");
    assert_stopped(&backend, &report);
    let mut observed = scenario.expect("direct images reached their original direction boundary");
    observed.extend(remaining);
    let received = images(&observed);
    assert_eq!(received.len(), expected.len());
    assert_eq!(ingress.pending_image_requests(), 0);
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    for (frame, direction, marked) in expected {
        assert!(marked.is_ok());
        let actual = received
            .iter()
            .find(|item| item.frame.id == frame.id)
            .unwrap();
        assert_eq!(actual.direction, direction);
        assert_eq!(actual.frame, frame);
        assert_eq!(actual.frame.observation_token(), frame.observation_token());
        assert_eq!(actual.frame.has_pipeline_pts(), frame.has_pipeline_pts());
        assert!(tag.contains(&actual.frame));
    }
}
