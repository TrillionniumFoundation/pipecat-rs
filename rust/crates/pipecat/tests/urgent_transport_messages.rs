//! Public Genesys replies must reach the configured shared-output backend.
//! Recording backends attest hook completion only, not a socket or remote receipt.
use pipecat::{
    audio::PcmFormat,
    core::*,
    media_transport::{OutputBackend, OutputTransport, OutputTransportConfig, PlaybackReceipt},
    protocol::{adapters::FrameSerializer, genesys::AudioHookSerializer, WireMessage},
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::time::timeout;

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

#[derive(Default)]
struct Backend {
    messages: Mutex<Vec<Frame>>,
    starts: Mutex<Vec<Vec<Option<String>>>>,
    stopped: AtomicUsize,
    audio: AtomicUsize,
    images: AtomicUsize,
}
#[async_trait]
impl OutputBackend for Backend {
    async fn start(
        &self,
        _: PcmFormat,
        destinations: &[Option<String>],
    ) -> Result<(), ProcessorError> {
        self.starts.lock().unwrap().push(destinations.to_vec());
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.audio.fetch_add(1, Ordering::SeqCst);
        Ok(PlaybackReceipt::Played)
    }
    async fn write_image(&self, _: &Frame) -> Result<(), ProcessorError> {
        self.images.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn send_message(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.messages.lock().unwrap().push(frame.clone());
        Ok(())
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn output(backend: Arc<Backend>, destinations: Vec<String>) -> Arc<OutputTransport> {
    Arc::new(
        OutputTransport::new(
            "audiohook-output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                chunk_duration: Duration::from_millis(10),
                destinations,
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: Duration::from_secs(1),
                ..Default::default()
            },
            backend,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    )
}

fn genesys_replies() -> Vec<Frame> {
    let mut serializer = AudioHookSerializer::new("initial", Default::default()).unwrap();
    let requests = [
        json!({"version":"2","type":"open","seq":1,"id":"urgent-session",
            "parameters":{"media":[{"type":"audio","format":"PCMU","channels":["external"],"rate":8000}]}}),
        json!({"version":"2","type":"ping","seq":2,"id":"urgent-session",
            "position":"PT10.5S","parameters":{}}),
        json!({"version":"2","type":"close","seq":3,"id":"urgent-session",
            "position":"PT11S","parameters":{"reason":"disconnect"}}),
    ];
    let expected = [
        json!({"version":"2","type":"opened","seq":1,"clientseq":1,"id":"urgent-session",
            "parameters":{"startPaused":false,"media":[{"type":"audio","format":"PCMU","channels":["external"],"rate":8000}]}}),
        json!({"version":"2","type":"pong","seq":2,"clientseq":2,"id":"urgent-session",
            "position":"PT10.500S","parameters":{}}),
        json!({"version":"2","type":"closed","seq":3,"clientseq":3,"id":"urgent-session",
            "position":"PT11.000S","parameters":{}}),
    ];
    requests
        .into_iter()
        .zip(expected)
        .map(|(request, expected)| {
            // These are the producer's actual returned frames, not replacement fixtures.
            let frame = serializer
                .deserialize(&WireMessage::Text(request.to_string()))
                .unwrap()
                .expect("Genesys control request must produce a reply");
            assert_eq!(
                frame.data,
                FrameData::OutputTransportMessageUrgent(expected)
            );
            assert!(frame.is_system());
            frame
        })
        .collect()
}

async fn receive_until(
    worker: &mut PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
    label: &str,
    matches: impl Fn(&DirectedFrame) -> bool,
) -> TestResult<()> {
    bounded(label, async {
        loop {
            let item = worker
                .recv()
                .await
                .ok_or_else(|| format!("{label}: output closed"))?;
            let found = matches(&item);
            frames.push(item);
            if found {
                return Ok::<(), String>(());
            }
        }
    })
    .await
}

async fn ready(worker: &mut PipelineWorker, frames: &mut Vec<DirectedFrame>) -> TestResult<()> {
    receive_until(worker, frames, "output ready", |item| {
        item.direction == Direction::Upstream
            && matches!(item.frame.data, FrameData::OutputTransportReady)
    })
    .await
}

async fn system_barrier(
    worker: &mut PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
) -> TestResult<()> {
    // BotConnected is an unchanged System passthrough. The public core system
    // lane processes it after earlier urgent messages in FIFO order. Receiving
    // it observes completion of their process calls, even on the broken source.
    let marker = Frame::new(FrameData::BotConnected);
    bounded(
        "send system marker",
        worker.sender().send(marker.clone(), Direction::Downstream),
    )
    .await?;
    receive_until(worker, frames, "system marker", |item| {
        item.direction == Direction::Downstream
            && item.frame.observation_token() == marker.observation_token()
    })
    .await
}

async fn finish(
    mut worker: PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
) -> TestResult<WorkerReport> {
    let ended = bounded("end", worker.end()).await;
    if ended.is_err() {
        let _ = bounded("fallback cancel", worker.cancel("end failed")).await;
    }
    let drained = bounded("drain", async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
        Ok::<(), String>(())
    })
    .await;
    if drained.is_err() {
        let _ = bounded("fallback cancel", worker.cancel("drain failed")).await;
    }
    let joined = bounded("join", worker.join()).await;
    if joined.is_err() {
        let _ = bounded("fallback cancel", worker.cancel("join failed")).await;
        let _ = bounded("fallback join", worker.join()).await;
    }
    let report = joined?;
    ended?;
    drained?;
    Ok(report)
}

fn assert_clean(backend: &Backend, report: &WorkerReport, frames: &[DirectedFrame]) {
    assert_eq!(backend.starts.lock().unwrap().len(), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.audio.load(Ordering::SeqCst), 0);
    assert_eq!(backend.images.load(Ordering::SeqCst), 0);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(
        frames
            .iter()
            .filter(|item| matches!(
                item.frame.data,
                FrameData::End(_) | FrameData::EndDetailed(_)
            ))
            .count(),
        1
    );
    assert!(!frames.iter().any(|item| matches!(
        item.frame.data,
        FrameData::BotStartedSpeaking | FrameData::BotStoppedSpeaking | FrameData::BotSpeaking
    )));
}

fn assert_same(actual: &Frame, expected: &Frame) {
    assert_eq!(actual, expected);
    assert_eq!(actual.observation_token(), expected.observation_token());
}

fn assert_consumed(frames: &[DirectedFrame], expected: &Frame) {
    assert!(
        !frames.iter().any(|item| item.frame.id == expected.id),
        "urgent reply must be consumed after backend delivery, not forwarded"
    );
}

fn assert_ordinary_forwarded(frames: &[DirectedFrame], expected: &Frame) {
    let returned: Vec<_> = frames
        .iter()
        .filter(|item| item.frame.id == expected.id)
        .collect();
    assert_eq!(
        returned.len(),
        1,
        "ordinary control must forward exactly once"
    );
    assert_eq!(returned[0].direction, Direction::Downstream);
    assert_same(&returned[0].frame, expected);
}

#[tokio::test]
async fn genesys_urgent_replies_reach_public_output_backend_once() {
    let expected = genesys_replies();
    let ordinary = Frame::new(FrameData::OutputTransportMessage(
        json!({"control":"ordinary"}),
    ));
    let backend = Arc::new(Backend::default());
    let mut worker = bounded(
        "start",
        Pipeline::new(vec![output(backend.clone(), vec![])]).start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let mut frames = vec![];
    let operations = async {
        ready(&mut worker, &mut frames).await?;
        for frame in &expected {
            bounded(
                "send urgent",
                worker.sender().send(frame.clone(), Direction::Downstream),
            )
            .await?;
        }
        system_barrier(&mut worker, &mut frames).await?;
        bounded(
            "send ordinary",
            worker
                .sender()
                .send(ordinary.clone(), Direction::Downstream),
        )
        .await
    }
    .await;
    let finished = finish(worker, &mut frames).await;
    // End, output drain, join and backend stop precede all delivery assertions.
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    operations.unwrap();
    let report = finished.unwrap();
    assert_clean(&backend, &report, &frames);
    assert_eq!(*backend.starts.lock().unwrap(), vec![vec![None]]);
    let messages = backend.messages.lock().unwrap();
    let ordinary_writes: Vec<_> = messages
        .iter()
        .filter(|frame| frame.id == ordinary.id)
        .collect();
    assert_eq!(
        ordinary_writes.len(),
        1,
        "ordinary message positive control"
    );
    assert_same(ordinary_writes[0], &ordinary);
    assert_ordinary_forwarded(&frames, &ordinary);
    let urgent: Vec<_> = messages
        .iter()
        .filter(|frame| matches!(frame.data, FrameData::OutputTransportMessageUrgent(_)))
        .collect();
    assert_eq!(
        urgent.len(),
        expected.len(),
        "Genesys opened/pong/closed replies never reached the configured backend"
    );
    for (actual, expected) in urgent.into_iter().zip(&expected) {
        assert_same(actual, expected);
        assert_consumed(&frames, expected);
    }
    assert_eq!(messages.len(), 4);
}

#[tokio::test]
async fn named_urgent_reply_bypasses_media_pause_and_pts() {
    let mut expected = genesys_replies().remove(1);
    expected.transport_destination = Some("control".into());
    expected.transport_source = Some("genesys".into());
    expected
        .metadata
        .insert("trace".into(), json!({"sequence":2}));
    expected.pts = Some(u64::MAX);
    let mut ordinary = Frame::new(FrameData::OutputTransportMessage(
        json!({"control":"queued"}),
    ));
    ordinary.transport_destination = Some("control".into());
    let backend = Arc::new(Backend::default());
    let output = output(backend.clone(), vec!["control".into()]);
    let control = output.control();
    let mut worker = bounded(
        "start",
        Pipeline::new(vec![output]).start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let mut frames = vec![];
    let mut before_resume = vec![];
    let operations = async {
        ready(&mut worker, &mut frames).await?;
        control
            .pause(Some("control"))
            .map_err(|error| error.to_string())?;
        bounded(
            "send ordinary",
            worker
                .sender()
                .send(ordinary.clone(), Direction::Downstream),
        )
        .await?;
        bounded(
            "send urgent",
            worker
                .sender()
                .send(expected.clone(), Direction::Downstream),
        )
        .await?;
        system_barrier(&mut worker, &mut frames).await?;
        before_resume = backend.messages.lock().unwrap().clone();
        Ok::<(), String>(())
    }
    .await;
    // Resume even after a failed observation, allowing normal End/drain cleanup.
    let resumed = control.resume(Some("control"));
    let finished = finish(worker, &mut frames).await;
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    operations.unwrap();
    resumed.unwrap();
    let report = finished.unwrap();
    assert_clean(&backend, &report, &frames);
    assert_eq!(
        *backend.starts.lock().unwrap(),
        vec![vec![None, Some("control".into())]]
    );
    let messages = backend.messages.lock().unwrap();
    let ordinary_writes: Vec<_> = messages
        .iter()
        .filter(|frame| frame.id == ordinary.id)
        .collect();
    assert_eq!(
        ordinary_writes.len(),
        1,
        "resumed ordinary message positive control"
    );
    assert_same(ordinary_writes[0], &ordinary);
    assert_ordinary_forwarded(&frames, &ordinary);
    assert_eq!(
        before_resume.len(),
        1,
        "urgent control reply must reach backend before media resume, regardless of PTS"
    );
    assert_same(&before_resume[0], &expected);
    assert_eq!(
        messages.len(),
        2,
        "each message must be delivered exactly once"
    );
    assert_same(&messages[0], &expected);
    assert_same(&messages[1], &ordinary);
    assert_consumed(&frames, &expected);
}

// Additional candidate controls. The two frozen baseline functions above are unchanged.
#[derive(Clone, Copy)]
enum MessageMode {
    Ready,
    Error,
    Block,
}
struct ControlledBackend {
    mode: MessageMode,
    block_audio: bool,
    calls: Mutex<Vec<Frame>>,
    completed: Mutex<Vec<Frame>>,
    message_entered: tokio::sync::Semaphore,
    message_release: tokio::sync::Semaphore,
    audio_entered: tokio::sync::Semaphore,
    audio_release: tokio::sync::Semaphore,
    active: AtomicUsize,
    dropped: AtomicUsize,
    audio_completed: AtomicUsize,
    started: AtomicUsize,
    stopped: AtomicUsize,
}
impl ControlledBackend {
    fn new(mode: MessageMode, block_audio: bool) -> Self {
        Self {
            mode,
            block_audio,
            calls: Mutex::new(vec![]),
            completed: Mutex::new(vec![]),
            message_entered: tokio::sync::Semaphore::new(0),
            message_release: tokio::sync::Semaphore::new(0),
            audio_entered: tokio::sync::Semaphore::new(0),
            audio_release: tokio::sync::Semaphore::new(0),
            active: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            audio_completed: AtomicUsize::new(0),
            started: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
        }
    }
}
struct ActiveMessage<'a> {
    backend: &'a ControlledBackend,
    returned: bool,
}
impl Drop for ActiveMessage<'_> {
    fn drop(&mut self) {
        self.backend.active.fetch_sub(1, Ordering::SeqCst);
        if !self.returned {
            self.backend.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }
}
#[async_trait]
impl OutputBackend for ControlledBackend {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.audio_entered.add_permits(1);
        if self.block_audio {
            self.audio_release
                .acquire()
                .await
                .map_err(|error| ProcessorError::new(error.to_string()))?
                .forget();
        }
        self.audio_completed.fetch_add(1, Ordering::SeqCst);
        Ok(PlaybackReceipt::Played)
    }
    async fn send_message(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let mut active = ActiveMessage {
            backend: self,
            returned: false,
        };
        self.calls.lock().unwrap().push(frame.clone());
        self.message_entered.add_permits(1);
        match self.mode {
            MessageMode::Ready => {}
            MessageMode::Error => {
                active.returned = true;
                return Err(ProcessorError::new("intentional urgent send failure"));
            }
            MessageMode::Block => self
                .message_release
                .acquire()
                .await
                .map_err(|error| ProcessorError::new(error.to_string()))?
                .forget(),
        }
        self.completed.lock().unwrap().push(frame.clone());
        active.returned = true;
        Ok(())
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn controlled_output(
    backend: Arc<ControlledBackend>,
    write_timeout: Duration,
) -> Arc<OutputTransport> {
    Arc::new(
        OutputTransport::new(
            "guarded-output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                chunk_duration: Duration::from_millis(10),
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout,
                ..Default::default()
            },
            backend,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    )
}
async fn permit(label: &str, semaphore: &tokio::sync::Semaphore) -> TestResult<()> {
    bounded(label, semaphore.acquire()).await?.forget();
    Ok(())
}
async fn finish_requested(
    mut worker: PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
) -> TestResult<WorkerReport> {
    let drained = bounded("terminal drain", async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
        Ok::<(), String>(())
    })
    .await;
    if drained.is_err() {
        let _ = bounded("fallback cancel", worker.cancel("terminal drain failed")).await;
    }
    let joined = bounded("terminal join", worker.join()).await;
    if joined.is_err() {
        let _ = bounded("fallback cancel", worker.cancel("terminal join failed")).await;
        let _ = bounded("fallback join", worker.join()).await;
    }
    let report = joined?;
    drained?;
    Ok(report)
}
fn assert_controlled_stopped(backend: &ControlledBackend) {
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
}
fn assert_no_urgent_forward(frames: &[DirectedFrame], expected: &Frame) {
    assert!(!frames
        .iter()
        .any(|item| item.frame.observation_token() == expected.observation_token()));
}

#[tokio::test]
async fn urgent_upstream_passes_through_and_unknown_downstream_is_rejected() {
    let upstream = genesys_replies().remove(1);
    let mut unknown = genesys_replies().remove(1);
    unknown.transport_destination = Some("unregistered".into());
    let backend = Arc::new(Backend::default());
    let mut worker = bounded(
        "start",
        Pipeline::new(vec![output(backend.clone(), vec![])]).start(PipelineConfig::default()),
    )
    .await
    .unwrap();
    let mut frames = vec![];
    let operations = async {
        ready(&mut worker, &mut frames).await?;
        bounded(
            "upstream",
            worker.sender().send(upstream.clone(), Direction::Upstream),
        )
        .await?;
        bounded(
            "unknown destination",
            worker.sender().send(unknown.clone(), Direction::Downstream),
        )
        .await?;
        system_barrier(&mut worker, &mut frames).await
    }
    .await;
    let finished = finish(worker, &mut frames).await;
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    operations.unwrap();
    let report = finished.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(backend.messages.lock().unwrap().is_empty());
    assert_eq!(report.errors.len(), 1);
    assert!(report.errors[0]
        .error
        .contains("unknown output destination"));
    let returned: Vec<_> = frames
        .iter()
        .filter(|item| item.frame.id == upstream.id)
        .collect();
    assert_eq!(returned.len(), 1);
    assert_eq!(returned[0].direction, Direction::Upstream);
    assert_same(&returned[0].frame, &upstream);
    assert_no_urgent_forward(&frames, &unknown);
}

#[tokio::test]
async fn urgent_send_overlaps_blocked_audio_on_the_same_backend() {
    let expected = genesys_replies().remove(1);
    let backend = Arc::new(ControlledBackend::new(MessageMode::Ready, true));
    let output = controlled_output(backend.clone(), Duration::from_secs(2));
    let mut worker = bounded(
        "start",
        Pipeline::new(vec![output]).start(PipelineConfig::default()),
    )
    .await
    .unwrap();
    let mut frames = vec![];
    let mut audio_completed_at_message = usize::MAX;
    let mut completed_before_audio_release = vec![];
    let operations = async {
        ready(&mut worker, &mut frames).await?;
        let audio = Frame::new(FrameData::OutputAudio(
            AudioRawFrame::new(vec![0; 160], 8000, 1).unwrap(),
        ));
        bounded("audio", worker.sender().send(audio, Direction::Downstream)).await?;
        permit("audio entered", &backend.audio_entered).await?;
        bounded(
            "urgent",
            worker
                .sender()
                .send(expected.clone(), Direction::Downstream),
        )
        .await?;
        system_barrier(&mut worker, &mut frames).await?;
        audio_completed_at_message = backend.audio_completed.load(Ordering::SeqCst);
        completed_before_audio_release = backend.completed.lock().unwrap().clone();
        Ok::<(), String>(())
    }
    .await;
    backend.audio_release.add_permits(1);
    let finished = finish(worker, &mut frames).await;
    assert_controlled_stopped(&backend);
    operations.unwrap();
    let report = finished.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(audio_completed_at_message, 0);
    assert_eq!(completed_before_audio_release.len(), 1);
    assert_same(&completed_before_audio_release[0], &expected);
    assert_eq!(backend.audio_completed.load(Ordering::SeqCst), 1);
    let calls = backend.calls.lock().unwrap();
    let completed = backend.completed.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(completed.len(), 1);
    assert_same(&calls[0], &expected);
    assert_same(&completed[0], &expected);
    assert_no_urgent_forward(&frames, &expected);
}

#[tokio::test]
async fn urgent_backend_error_and_timeout_are_bounded_and_never_retried() {
    for mode in [MessageMode::Error, MessageMode::Block] {
        let expected = genesys_replies().remove(1);
        let backend = Arc::new(ControlledBackend::new(mode, false));
        let output = controlled_output(backend.clone(), Duration::from_millis(30));
        let mut worker = bounded(
            "start",
            Pipeline::new(vec![output]).start(PipelineConfig::default()),
        )
        .await
        .unwrap();
        let mut frames = vec![];
        let operations = async {
            ready(&mut worker, &mut frames).await?;
            bounded(
                "urgent",
                worker
                    .sender()
                    .send(expected.clone(), Direction::Downstream),
            )
            .await?;
            system_barrier(&mut worker, &mut frames).await
        }
        .await;
        let finished = finish(worker, &mut frames).await;
        assert_controlled_stopped(&backend);
        operations.unwrap();
        let report = finished.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors[0].error.contains(match mode {
            MessageMode::Error => "intentional urgent send failure",
            MessageMode::Block => "media backend operation timed out",
            MessageMode::Ready => unreachable!(),
        }));
        let calls = backend.calls.lock().unwrap();
        assert_eq!(calls.len(), 1, "ambiguous send must never be retried");
        assert_same(&calls[0], &expected);
        assert!(backend.completed.lock().unwrap().is_empty());
        assert_eq!(
            backend.dropped.load(Ordering::SeqCst),
            usize::from(matches!(mode, MessageMode::Block))
        );
        assert_no_urgent_forward(&frames, &expected);
    }
}

// The public runtime's System lane prevents normal dequeues while active.
// Capture its real context to call public transport lifecycle methods directly
// in these two explicit concurrency controls, not via a fictitious normal lane.
struct ContextOutput {
    output: Arc<OutputTransport>,
    context: Mutex<Option<ProcessorContext>>,
}
impl ContextOutput {
    fn new(output: Arc<OutputTransport>) -> Self {
        Self {
            output,
            context: Mutex::new(None),
        }
    }
    fn context(&self) -> TestResult<ProcessorContext> {
        self.context
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| "output context not set".to_string())
    }
}
#[async_trait]
impl FrameProcessor for ContextOutput {
    fn name(&self) -> &str {
        self.output.name()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        self.output.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.output.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.output.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.output.cleanup().await
    }
}

#[tokio::test]
async fn admitted_urgent_write_survives_media_epoch_and_graceful_end() {
    let expected = genesys_replies().remove(1);
    let backend = Arc::new(ControlledBackend::new(MessageMode::Block, false));
    let output = controlled_output(backend.clone(), Duration::from_secs(2));
    let measured = Arc::new(ContextOutput::new(output.clone()));
    let mut worker = bounded(
        "start",
        Pipeline::new(vec![measured.clone()]).start(PipelineConfig::default()),
    )
    .await
    .unwrap();
    let mut frames = vec![];
    let mut active_after_prepare = usize::MAX;
    let operations = async {
        ready(&mut worker, &mut frames).await?;
        bounded(
            "urgent",
            worker
                .sender()
                .send(expected.clone(), Direction::Downstream),
        )
        .await?;
        permit("message entered", &backend.message_entered).await?;
        let ctx = measured.context()?;
        // Direct public lifecycle calls using the actual output's context.
        // They can overlap its blocked System send; another queued processor
        // invocation would not enter while that System call is active.
        bounded(
            "direct media interruption",
            output.process(Frame::interruption(), Direction::Downstream, &ctx),
        )
        .await?;
        bounded("direct prepare end", output.prepare_end(&ctx)).await?;
        active_after_prepare = backend.active.load(Ordering::SeqCst);
        Ok::<(), String>(())
    }
    .await;
    backend.message_release.add_permits(1);
    let finished = finish(worker, &mut frames).await;
    assert_controlled_stopped(&backend);
    operations.unwrap();
    let report = finished.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(active_after_prepare, 1);
    assert_eq!(backend.dropped.load(Ordering::SeqCst), 0);
    let calls = backend.calls.lock().unwrap();
    let completed = backend.completed.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(completed.len(), 1);
    assert_same(&completed[0], &expected);
    assert_no_urgent_forward(&frames, &expected);
}

#[tokio::test]
async fn prepare_end_rejects_late_urgent_before_ready_is_cleared() {
    let expected = genesys_replies().remove(1);
    let backend = Arc::new(ControlledBackend::new(MessageMode::Ready, false));
    let output = controlled_output(backend.clone(), Duration::from_secs(1));
    let measured = Arc::new(ContextOutput::new(output.clone()));
    let mut worker = bounded(
        "start",
        Pipeline::new(vec![measured.clone()]).start(PipelineConfig::default()),
    )
    .await
    .unwrap();
    let mut frames = vec![];
    let mut ready_at_cutoff = false;
    let operations = async {
        ready(&mut worker, &mut frames).await?;
        let ctx = measured.context()?;
        bounded("direct prepare end", output.prepare_end(&ctx)).await?;
        ready_at_cutoff = output.control().is_ready();
        // Actual public Pipeline admission remains open; output's destination
        // is already ending, while its later process(End) has not run.
        bounded(
            "late urgent",
            worker
                .sender()
                .send(expected.clone(), Direction::Downstream),
        )
        .await?;
        system_barrier(&mut worker, &mut frames).await
    }
    .await;
    let finished = finish(worker, &mut frames).await;
    assert_controlled_stopped(&backend);
    operations.unwrap();
    let report = finished.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(
        ready_at_cutoff,
        "test must exercise queue.ending before ready=false"
    );
    assert_eq!(report.errors.len(), 1);
    assert!(report.errors[0].error.contains("media queue is closed"));
    assert!(backend.calls.lock().unwrap().is_empty());
    assert_no_urgent_forward(&frames, &expected);
    bounded("idempotent cleanup", output.cleanup())
        .await
        .unwrap();
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_a_stalled_urgent_send_leaves_no_owned_write_or_retry() {
    let expected = genesys_replies().remove(1);
    let backend = Arc::new(ControlledBackend::new(MessageMode::Block, false));
    let output = controlled_output(backend.clone(), Duration::from_millis(30));
    let mut worker = bounded(
        "start",
        Pipeline::new(vec![output]).start(PipelineConfig::default()),
    )
    .await
    .unwrap();
    let mut frames = vec![];
    let operations = async {
        ready(&mut worker, &mut frames).await?;
        bounded(
            "urgent",
            worker
                .sender()
                .send(expected.clone(), Direction::Downstream),
        )
        .await?;
        permit("message entered", &backend.message_entered).await?;
        bounded("cancel", worker.cancel("cancel owned protocol write")).await
    }
    .await;
    let finished = if operations.is_ok() {
        finish_requested(worker, &mut frames).await
    } else {
        finish(worker, &mut frames).await
    };
    assert_controlled_stopped(&backend);
    operations.unwrap();
    let report = finished.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    // Same-lane Cancel waits behind this call, so its own write_timeout may
    // produce an error first. No instantaneous remote cancellation is claimed.
    assert!(report
        .errors
        .iter()
        .all(|error| error.error.contains("media backend operation timed out")));
    assert_eq!(backend.calls.lock().unwrap().len(), 1);
    assert!(backend.completed.lock().unwrap().is_empty());
    assert_eq!(backend.dropped.load(Ordering::SeqCst), 1);
    assert_no_urgent_forward(&frames, &expected);
}
