//! Actual shared input capture/injection composed with the public RTVI tap.
use pipecat::{
    audio::PcmFormat,
    core::*,
    media_transport::{InputBackend, InputIngress, InputTransport, InputTransportConfig},
    rtvi::{RtviObserver, RtviObserverParams},
};
use serde_json::json;
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

const WAIT: Duration = Duration::from_secs(10);
const TAP: &str = "shared-input-rtvi-tap";
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
    frame: Frame,
    admitted: oneshot::Sender<TestResult<()>>,
}
struct Backend {
    started: AtomicUsize,
    stopped: AtomicUsize,
    captures: AtomicUsize,
    active: AtomicUsize,
    format: Mutex<Option<PcmFormat>>,
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
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        *self.format.lock().unwrap() = Some(format);
        Ok(())
    }
    async fn capture(&self, ingress: InputIngress) -> Result<(), ProcessorError> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveCapture(&self.active);
        let mut commands = self.commands.lock().await;
        while let Some(command) = commands.recv().await {
            let result = ingress
                .push(command.frame)
                .map_err(|error| error.to_string());
            let _ = command.admitted.send(result);
        }
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[derive(Default)]
struct Recorder {
    frames: Mutex<Vec<DirectedFrame>>,
}
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "rtvi-audio-recorder"
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
    capture: mpsc::Sender<CaptureCommand>,
    backend: Arc<Backend>,
    before: Arc<Recorder>,
    after: Arc<Recorder>,
}
impl Case {
    async fn start() -> TestResult<Self> {
        let (capture, commands) = mpsc::channel(4);
        let backend = Arc::new(Backend {
            started: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
            captures: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            format: Mutex::new(None),
            commands: AsyncMutex::new(commands),
        });
        let input = Arc::new(
            InputTransport::new(
                "shared-rtvi-input",
                InputTransportConfig::default(),
                backend.clone(),
                None,
            )
            .map_err(|error| error.to_string())?,
        );
        let ingress = input.ingress();
        let observer = Arc::new(
            RtviObserver::new(
                RtviObserverParams {
                    max_message_bytes: 1024,
                    ..Default::default()
                },
                None,
            )
            .map_err(|error| error.to_string())?,
        );
        let before = Arc::new(Recorder::default());
        let after = Arc::new(Recorder::default());
        let mut config = PipelineConfig::default();
        config.start.audio_in_sample_rate = 16_000;
        let worker = Pipeline::new(vec![
            input,
            before.clone(),
            Arc::new(observer.tap(TAP)),
            after.clone(),
        ])
        .start(config)
        .await
        .map_err(|error| error.to_string())?;
        Ok(Self {
            worker,
            ingress,
            capture,
            backend,
            before,
            after,
        })
    }
    async fn submit(&self, frame: Frame, capture: bool) -> TestResult<()> {
        if capture {
            let (admitted, result) = oneshot::channel();
            bounded(
                "capture command",
                self.capture.send(CaptureCommand { frame, admitted }),
            )
            .await?;
            bounded("capture admission", result).await?
        } else {
            bounded(
                "injection",
                self.worker.sender().send(frame, Direction::Downstream),
            )
            .await
        }
    }
}
async fn receive(
    worker: &mut PipelineWorker,
    observed: &mut Vec<DirectedFrame>,
    wanted: impl Fn(&DirectedFrame) -> bool,
) -> TestResult<()> {
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.ok_or("pipeline closed before marker")?;
            let found = wanted(&frame);
            observed.push(frame);
            if found {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| "pipeline marker deadline".to_owned())?
}
async fn finish(
    worker: PipelineWorker,
    graceful: bool,
) -> TestResult<(Vec<DirectedFrame>, WorkerReport)> {
    let sender = worker.sender();
    let terminal = if graceful {
        bounded("End", sender.end()).await
    } else {
        bounded(
            "Cancel failed scenario",
            sender.cancel("RTVI scenario failed"),
        )
        .await
    };
    if terminal.is_err() {
        let _ = bounded(
            "Cancel failed terminal",
            sender.cancel("RTVI terminal failed"),
        )
        .await;
    }
    let mut collection = tokio::spawn(worker.collect());
    let collected = match timeout(WAIT, &mut collection).await {
        Ok(result) => result
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string()),
        Err(_) => {
            let _ = bounded("Cancel collection", sender.cancel("RTVI collect deadline")).await;
            match timeout(WAIT, &mut collection).await {
                Ok(result) => Err(format!(
                    "collection timed out; cancelled and joined: {result:?}"
                )),
                Err(_) => {
                    collection.abort();
                    let _ = collection.await;
                    Err("collection timed out after cancellation; aborted and joined".into())
                }
            }
        }
    };
    terminal.and(collected)
}
fn audio(user: bool, bytes: usize) -> Frame {
    let mut audio = AudioRawFrame::new(vec![0; bytes], 16_000, 1).unwrap();
    audio.context_id = Some("input-context".into());
    Frame::new(if user {
        FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: "alice".into(),
        })
    } else {
        FrameData::InputAudio(audio)
    })
}
fn assert_frame(frame: &DirectedFrame, expected: &Frame, tag: Option<&LocalFrameTag>) {
    assert_eq!(frame.direction, Direction::Downstream);
    let frame = &frame.frame;
    assert_eq!(frame.data, expected.data);
    assert_eq!(frame.id, expected.id);
    assert_eq!(frame.name, expected.name);
    assert_eq!(frame.pts, expected.pts);
    assert_eq!(frame.has_pipeline_pts(), expected.has_pipeline_pts());
    assert_eq!(frame.metadata, expected.metadata);
    assert_eq!(frame.transport_source, expected.transport_source);
    assert_eq!(frame.transport_destination, expected.transport_destination);
    assert_eq!(frame.broadcast_sibling_id, expected.broadcast_sibling_id);
    assert_eq!(frame.interruptible, expected.interruptible);
    assert_eq!(frame.observation_token(), expected.observation_token());
    if let Some(tag) = tag {
        assert!(tag.contains(frame));
    }
}
fn matching(frames: &[DirectedFrame], id: u64) -> Vec<&DirectedFrame> {
    frames.iter().filter(|frame| frame.frame.id == id).collect()
}
async fn assert_shared_user_audio(capture: bool) {
    let mut user = audio(true, 640);
    user.name = "shared-user-pcm".into();
    user.set_pipeline_pts(12345);
    user.transport_source = Some("microphone".into());
    user.transport_destination = Some("consumer".into());
    user.broadcast_sibling_id = Some(987654);
    user.interruptible = false;
    user.metadata.insert("capture".into(), json!(capture));
    let tag = LocalFrameTag::new();
    tag.mark(&mut user).unwrap();
    let marker = audio(false, 2);
    let control = Frame::new(FrameData::UserStartedSpeaking);
    let mut case = Case::start()
        .await
        .expect("start public shared input and RTVI tap");
    let mut observed = Vec::new();
    let scenario: TestResult<()> = async {
        timeout(WAIT, async {
            while !case.ingress.is_ready() || case.backend.captures.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        }).await.map_err(|_| "shared input readiness deadline".to_owned())?;
        case.submit(user.clone(), capture).await?;
        case.submit(marker.clone(), capture).await?;
        // A plain input packet shares the actual media queue and survives the old tap.
        receive(&mut case.worker, &mut observed, |frame| frame.frame.id == marker.id).await?;
        bounded("subsequent control", case.worker.sender().send(control.clone(), Direction::Downstream)).await?;
        receive(&mut case.worker, &mut observed, |frame| {
            matches!(&frame.frame.data, FrameData::OutputTransportMessageUrgent(value) if value["type"] == "user-started-speaking")
        }).await
    }.await;
    let health = case.worker.processor_health(2);
    let Case {
        worker,
        ingress,
        capture: capture_commands,
        backend,
        before,
        after,
    } = case;
    let finished = finish(worker, scenario.is_ok()).await;
    drop(capture_commands);
    // Every parity assertion is after terminal processing, capture cancellation, and join.
    let (remaining, report) = finished.expect("fully collect and join public graph");
    observed.extend(remaining);
    assert!(scenario.is_ok(), "{scenario:?}");
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.captures.load(Ordering::SeqCst), 1);
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        *backend.format.lock().unwrap(),
        Some(PcmFormat::new(16_000, 1).unwrap())
    );
    assert!(!ingress.is_ready());
    assert_eq!(
        ingress.push(audio(true, 2)).unwrap_err().message,
        "input transport is not ready"
    );
    assert!(health.expect("public tap health").is_usable());
    let before = before.frames.lock().unwrap();
    let after = after.frames.lock().unwrap();
    let incoming = matching(&before, user.id);
    assert_eq!(incoming.len(), 1);
    assert_frame(incoming[0], &user, Some(&tag));
    for frames in [&*before, &*after, &observed] {
        let markers = matching(frames, marker.id);
        assert_eq!(markers.len(), 1);
        assert_frame(markers[0], &marker, None);
        let controls = matching(frames, control.id);
        assert_eq!(controls.len(), 1);
        assert_frame(controls[0], &control, None);
    }
    assert_eq!(observed.iter().filter(|frame| matches!(&frame.frame.data,
        FrameData::OutputTransportMessageUrgent(value) if value["type"] == "user-started-speaking"
    )).count(), 1);
    let outgoing = matching(&after, user.id);
    eprintln!(
        "capture={capture}: state={:?}, input_user={}, output_user={}, errors={:?}",
        report.state,
        incoming.len(),
        outgoing.len(),
        report.errors
    );
    if !report.errors.is_empty() {
        // Pin the precise old failure rather than accepting a timeout or fatal error as red.
        assert_eq!(
            report.errors,
            vec![ErrorFrame {
                error: "RTVI payload too large".into(),
                fatal: false,
                processor: Some(TAP.into()),
                category: ErrorCategory::Unknown,
            }]
        );
        assert!(outgoing.is_empty());
        assert!(matching(&observed, user.id).is_empty());
    }
    assert!(
        report.errors.is_empty(),
        "valid user PCM must not become an RTVI JSON error: {:?}",
        report.errors
    );
    assert_eq!(outgoing.len(), 1);
    assert_frame(outgoing[0], &user, Some(&tag));
    let output = matching(&observed, user.id);
    assert_eq!(output.len(), 1);
    assert_frame(output[0], &user, Some(&tag));
}
#[tokio::test]
async fn shared_backend_capture_preserves_user_audio_through_rtvi_tap() {
    assert_shared_user_audio(true).await;
}
#[tokio::test]
async fn shared_pipeline_injection_preserves_user_audio_through_rtvi_tap() {
    assert_shared_user_audio(false).await;
}

#[tokio::test]
async fn shared_capture_cancel_stops_capture_and_rejects_further_audio() {
    let mut case = Case::start()
        .await
        .expect("start shared capture cancellation case");
    let user = audio(true, 640);
    let marker = audio(false, 2);
    let mut observed = Vec::new();
    let scenario: TestResult<()> = async {
        timeout(WAIT, async {
            while !case.ingress.is_ready() || case.backend.captures.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "capture readiness deadline".to_owned())?;
        case.submit(user.clone(), true).await?;
        case.submit(marker.clone(), true).await?;
        receive(&mut case.worker, &mut observed, |frame| {
            frame.frame.id == marker.id
        })
        .await
    }
    .await;
    let Case {
        worker,
        ingress,
        capture,
        backend,
        before,
        after,
    } = case;
    let finished = finish(worker, false).await;
    drop(capture);
    let (remaining, report) = finished.expect("cancel and join shared capture");
    observed.extend(remaining);
    assert!(scenario.is_ok(), "{scenario:?}");
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.captures.load(Ordering::SeqCst), 1);
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert!(!ingress.is_ready());
    assert_eq!(
        ingress.push(audio(true, 2)).unwrap_err().message,
        "input transport is not ready"
    );
    let before = before.frames.lock().unwrap();
    let after = after.frames.lock().unwrap();
    for frames in [&*before, &*after, &observed] {
        for expected in [&user, &marker] {
            let matching = matching(frames, expected.id);
            assert_eq!(matching.len(), 1);
            assert_frame(matching[0], expected, None);
        }
    }
}
