//! Synthetic application capture through the public shared input transport.
//! These tests do not open a physical camera or attest provider transport I/O.
use pipecat::{
    audio::PcmFormat, core::*, media_transport::*, network_bus::PythonJsonCodec,
    workers::MessageData,
};
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
    let class = if user {
        "UserImageRawFrame"
    } else {
        "InputImageRawFrame"
    };
    let mut data = serde_json::json!({
        "id": 900_000 + u64::from(pixel),
        "name": format!("{class}#application-wire-{pixel}"),
        "pts": 123_456,
        "metadata": {"capture": {"source": "application-wire", "values": [7, null, true]}},
        "transport_source": "application-camera",
        "transport_destination": "vision-consumer",
        "interruptible": false,
        "broadcast_sibling_id": 987_654,
        "image": {"__type__": "bytes", "__data__": "AQID"},
        "size": [1, 1],
        "format": "RGB"
    });
    if user {
        data["user_id"] = serde_json::json!("streaming-participant");
        data["text"] = serde_json::json!("application image context");
        data["append_to_context"] = serde_json::json!(true);
        data["run_llm"] = serde_json::json!(false);
        data["request"] = serde_json::Value::Null;
    }
    let wire = serde_json::json!({
        "__type__": "pipecat.bus.messages.BusFrameMessage",
        "__data__": {
            "source": "capture-worker",
            "target": "vision-worker",
            "frame": {"__type__": format!("pipecat.frames.frames.{class}"), "__data__": data},
            "direction": {
                "__type__": "pipecat.processors.frame_processor.FrameDirection",
                "__data__": "DOWNSTREAM"
            },
            "bridge": "media"
        }
    });
    let bytes = serde_json::to_vec(&wire).unwrap();
    let envelope = PythonJsonCodec::default().decode(&bytes).unwrap();
    let MessageData::Frame {
        frame, direction, ..
    } = &envelope.as_bus_message().unwrap().data
    else {
        panic!("codec did not return a typed image frame");
    };
    assert_eq!(*direction, Direction::Downstream);
    assert_eq!(frame.data.source_class_name(), class);
    // Native identity starts at decode; preservation is checked from this point.
    (**frame).clone()
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
async fn public_codec_to_backend_capture_preserves_unrequested_user_image() {
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

// Additive facade controls use the same public codec and owned capture task.
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
