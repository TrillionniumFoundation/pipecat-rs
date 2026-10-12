//! Real shared-output dispatch and admission, with cleanup before regression assertions.
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::timeout};

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
async fn until(label: &str, ready: impl Fn() -> bool) -> TestResult<()> {
    timeout(WAIT, async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| format!("{label}: deadline"))
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Written(u64),
    Forwarded(u64),
}
struct Backend {
    writes: Mutex<Vec<Frame>>,
    entered: Mutex<BTreeSet<u64>>,
    blocked: Mutex<BTreeSet<u64>>,
    failures: Mutex<BTreeSet<u64>>,
    events: Arc<Mutex<Vec<Event>>>,
    gate: Semaphore,
    active: AtomicUsize,
    started: AtomicUsize,
    stopped: AtomicUsize,
    audio: AtomicUsize,
    interrupts: Mutex<Vec<Option<String>>>,
}
impl Backend {
    fn new(events: Arc<Mutex<Vec<Event>>>) -> Self {
        Self {
            writes: Mutex::new(vec![]),
            entered: Mutex::new(BTreeSet::new()),
            blocked: Mutex::new(BTreeSet::new()),
            failures: Mutex::new(BTreeSet::new()),
            events,
            gate: Semaphore::new(0),
            active: AtomicUsize::new(0),
            started: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
            audio: AtomicUsize::new(0),
            interrupts: Mutex::new(vec![]),
        }
    }
    fn has_entered(&self, id: u64) -> bool {
        self.entered.lock().unwrap().contains(&id)
    }
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl OutputBackend for Backend {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.audio.fetch_add(1, Ordering::SeqCst);
        Ok(PlaybackReceipt::Played)
    }
    async fn write_image(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active(&self.active);
        self.entered.lock().unwrap().insert(frame.id);
        let blocked = self.blocked.lock().unwrap().contains(&frame.id);
        if blocked {
            self.gate
                .acquire()
                .await
                .map_err(|error| ProcessorError::new(error.to_string()))?
                .forget();
        }
        if self.failures.lock().unwrap().contains(&frame.id) {
            return Err(ProcessorError::new("intentional image backend failure"));
        }
        self.writes.lock().unwrap().push(frame.clone());
        self.events.lock().unwrap().push(Event::Written(frame.id));
        Ok(())
    }
    async fn interrupt(&self, destination: Option<&str>) -> Result<(), ProcessorError> {
        self.interrupts
            .lock()
            .unwrap()
            .push(destination.map(str::to_owned));
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
// Every invocation is delegated to the actual OutputTransport. This records its
// completed admission result, unlike PipelineSender::send's ingress receipt.
struct MeasuredOutput {
    output: Arc<OutputTransport>,
    admissions: Mutex<BTreeMap<u64, Result<(), ProcessorError>>>,
}
#[async_trait]
impl FrameProcessor for MeasuredOutput {
    fn name(&self) -> &str {
        self.output.name()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.output.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let id = frame.id;
        let result = self.output.process(frame, direction, ctx).await;
        self.admissions.lock().unwrap().insert(id, result.clone());
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.output.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.output.cleanup().await
    }
}
struct Recorder {
    events: Arc<Mutex<Vec<Event>>>,
}
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "assistant-image-recorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.events.lock().unwrap().push(Event::Forwarded(frame.id));
        }
        ctx.push(frame, direction).await
    }
}
fn config() -> OutputTransportConfig {
    OutputTransportConfig {
        sample_rate: Some(8000),
        chunk_duration: Duration::from_millis(10),
        queue_capacity: 4,
        queue_byte_capacity: 16 * 1024,
        admission_policy: OutputAdmissionPolicy::FailFast,
        destinations: vec!["video".into()],
        auto_silence: false,
        end_silence: Duration::ZERO,
        write_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}
struct Case {
    worker: PipelineWorker,
    measured: Arc<MeasuredOutput>,
    backend: Arc<Backend>,
    events: Arc<Mutex<Vec<Event>>>,
}
impl Case {
    async fn start(config: OutputTransportConfig) -> TestResult<Self> {
        Self::with_filter(config, None).await
    }
    async fn with_filter(
        config: OutputTransportConfig,
        filter: Option<Arc<dyn AudioFilter>>,
    ) -> TestResult<Self> {
        let events = Arc::new(Mutex::new(vec![]));
        let backend = Arc::new(Backend::new(events.clone()));
        let output = Arc::new(
            OutputTransport::new(
                "assistant-image-output",
                config,
                backend.clone(),
                filter,
                BTreeMap::new(),
            )
            .map_err(|error| error.to_string())?,
        );
        let measured = Arc::new(MeasuredOutput {
            output,
            admissions: Mutex::new(BTreeMap::new()),
        });
        let worker = bounded(
            "start",
            Pipeline::new(vec![
                measured.clone(),
                Arc::new(Recorder {
                    events: events.clone(),
                }),
            ])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_secs(3),
                ..Default::default()
            }),
        )
        .await?;
        Ok(Self {
            worker,
            measured,
            backend,
            events,
        })
    }
    async fn ready(&self) -> TestResult<()> {
        until("output ready", || self.measured.output.control().is_ready()).await
    }
    async fn send(&self, frame: Frame, direction: Direction) -> TestResult<()> {
        bounded("send", self.worker.sender().send(frame, direction)).await
    }
    async fn receipt(&self, id: u64) -> TestResult<Result<(), ProcessorError>> {
        until("output admission", || {
            self.measured.admissions.lock().unwrap().contains_key(&id)
        })
        .await?;
        Ok(self.measured.admissions.lock().unwrap()[&id].clone())
    }
    async fn finish(mut self, cancel: bool) -> TestResult<(Vec<DirectedFrame>, WorkerReport)> {
        let terminal = if cancel {
            bounded("cancel", self.worker.cancel("image test cleanup")).await
        } else {
            bounded("end", self.worker.end()).await
        };
        if terminal.is_err() {
            let _ = bounded("fallback cancel", self.worker.cancel("terminal failed")).await;
        }
        let joined = bounded("join", self.worker.join()).await;
        if joined.is_err() {
            let _ = bounded("fallback cancel", self.worker.cancel("join failed")).await;
            let _ = bounded("fallback join", self.worker.join()).await;
        }
        let report = joined?;
        terminal?;
        let mut frames = vec![];
        while let Ok(frame) = self.worker.try_recv() {
            frames.push(frame);
        }
        Ok((frames, report))
    }
}
fn image() -> ImageRawFrame {
    ImageRawFrame {
        image: vec![1, 2, 3],
        size: (1, 1),
        format: Some("RGB".into()),
    }
}
fn assistant() -> Frame {
    Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
        image: OutputImageRawFrame {
            image: image(),
            sync_with_audio: true,
        },
        original_data: Some(vec![0x89, 0x50, 0x4e, 0x47]),
        original_mime_type: Some("image/png".into()),
    }))
}
fn ordinary() -> Frame {
    Frame::new(FrameData::OutputImage(image()))
}
fn assert_clean(backend: &Backend) {
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(backend.audio.load(Ordering::SeqCst), 0);
}
fn assert_preserved(actual: &Frame, expected: &Frame) {
    assert_eq!(actual, expected);
    assert_eq!(actual.observation_token(), expected.observation_token());
}
fn downstream(frames: &[DirectedFrame], id: u64) -> Vec<&Frame> {
    frames
        .iter()
        .filter(|item| item.direction == Direction::Downstream && item.frame.id == id)
        .map(|item| &item.frame)
        .collect()
}

#[tokio::test]
async fn assistant_image_reaches_backend_with_original_envelope() {
    let case = Case::start(config()).await.unwrap();
    let backend = case.backend.clone();
    let events = case.events.clone();
    let ordinary = ordinary();
    let mut assistant = assistant();
    assistant.name = "assistant-display-image".into();
    assistant.pts = Some(0);
    assistant.transport_source = Some("generated-image".into());
    assistant.broadcast_sibling_id = Some(991);
    assistant.interruptible = false;
    assistant.metadata.insert(
        "origin".into(),
        serde_json::json!({"content": ["context", null], "generation": 7}),
    );
    let mut routed = assistant.duplicate();
    routed.transport_destination = Some("video".into());
    let upstream = assistant.duplicate();
    let operations = async {
        case.ready().await?;
        for frame in [&ordinary, &assistant, &routed] {
            case.send(frame.clone(), Direction::Downstream).await?;
        }
        case.send(upstream.clone(), Direction::Upstream).await
    }
    .await;
    let finished = case.finish(false).await;
    assert_clean(&backend);
    operations.unwrap();
    let (frames, report) = finished.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.state, WorkerState::Ended);
    let writes = backend.writes.lock().unwrap();
    assert_eq!(writes.len(), 3, "assistant image never reached write_image");
    for expected in [&ordinary, &assistant, &routed] {
        let written: Vec<_> = writes
            .iter()
            .filter(|frame| frame.id == expected.id)
            .collect();
        assert_eq!(written.len(), 1);
        assert_preserved(written[0], expected);
        let forwarded = downstream(&frames, expected.id);
        assert_eq!(forwarded.len(), 1);
        assert_preserved(forwarded[0], expected);
        let events = events.lock().unwrap();
        assert!(
            events
                .iter()
                .position(|event| *event == Event::Written(expected.id))
                < events
                    .iter()
                    .position(|event| *event == Event::Forwarded(expected.id))
        );
    }
    assert!(!writes.iter().any(|frame| frame.id == upstream.id));
    let returned: Vec<_> = frames
        .iter()
        .filter(|item| item.direction == Direction::Upstream && item.frame.id == upstream.id)
        .collect();
    assert_eq!(returned.len(), 1);
    assert_preserved(&returned[0].frame, &upstream);
}

#[tokio::test]
async fn assistant_original_bytes_cannot_bypass_real_output_queue_budget() {
    let mut config = config();
    config.queue_byte_capacity = 1024;
    let case = Case::start(config).await.unwrap();
    let backend = case.backend.clone();
    let control = ordinary();
    backend.blocked.lock().unwrap().insert(control.id);
    let mut oversized = assistant();
    if let FrameData::AssistantImage(image) = &mut oversized.data {
        let mut original = Vec::with_capacity(4096);
        original.push(9);
        image.original_data = Some(original);
    }
    let id = oversized.id;
    let operations = async {
        case.ready().await?;
        case.send(control.clone(), Direction::Downstream).await?;
        until("ordinary image backend entered", || {
            backend.has_entered(control.id)
        })
        .await?;
        // Move the allocation into the real pipeline; cloning would erase spare capacity.
        case.send(oversized, Direction::Downstream).await?;
        case.receipt(id).await
    }
    .await;
    backend.gate.add_permits(1);
    let finished = case.finish(false).await;
    assert_clean(&backend);
    let receipt = operations.unwrap();
    let (frames, report) = finished.unwrap();
    assert!(
        receipt.is_err(),
        "AssistantImage owned allocation was admitted above budget"
    );
    assert!(receipt.unwrap_err().message.contains("bounded media queue"));
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(report.errors[0].error.contains("bounded media queue"));
    assert!(downstream(&frames, id).is_empty());
    let writes = backend.writes.lock().unwrap();
    assert_eq!(writes.len(), 1);
    assert_preserved(&writes[0], &control);
}

async fn rejected_after_real_admission(frame: Frame, label: &str) {
    let mut config = config();
    config.queue_byte_capacity = 1024;
    let case = Case::start(config).await.unwrap();
    let backend = case.backend.clone();
    let control = ordinary();
    backend.blocked.lock().unwrap().insert(control.id);
    let id = frame.id;
    let operations = async {
        case.ready().await?;
        case.send(control.clone(), Direction::Downstream).await?;
        until("ordinary image backend entered", || {
            backend.has_entered(control.id)
        })
        .await?;
        case.send(frame, Direction::Downstream).await?;
        case.receipt(id).await
    }
    .await;
    backend.gate.add_permits(1);
    let finished = case.finish(false).await;
    assert_clean(&backend);
    let receipt = operations.unwrap();
    let (frames, report) = finished.unwrap();
    assert!(receipt.is_err(), "{label}: oversized image was admitted");
    assert!(receipt.unwrap_err().message.contains("bounded media queue"));
    assert_eq!(report.errors.len(), 1, "{label}: {:?}", report.errors);
    assert!(report.errors[0].error.contains("bounded media queue"));
    assert!(downstream(&frames, id).is_empty(), "{label}");
    let writes = backend.writes.lock().unwrap();
    assert_eq!(writes.len(), 1, "{label}");
    assert_preserved(&writes[0], &control);
}

#[tokio::test]
async fn assistant_payload_and_envelope_allocations_share_the_image_budget() {
    for field in [
        "pixels",
        "format",
        "original",
        "mime",
        "name",
        "source",
        "destination",
        "key",
        "value",
        "array",
        "deep",
    ] {
        let mut frame = assistant();
        match field {
            "pixels" | "format" | "original" | "mime" => {
                let FrameData::AssistantImage(image) = &mut frame.data else {
                    unreachable!()
                };
                match field {
                    "pixels" => image.image.image.image.reserve(4096),
                    "format" => image.image.image.format.as_mut().unwrap().reserve(4096),
                    "original" => image.original_data.as_mut().unwrap().reserve(4096),
                    "mime" => image.original_mime_type.as_mut().unwrap().reserve(4096),
                    _ => unreachable!(),
                }
            }
            "name" => frame.name.reserve(4096),
            "source" => {
                let mut source = String::with_capacity(4096);
                source.push('s');
                frame.transport_source = Some(source);
            }
            "destination" => {
                let mut destination = String::with_capacity(4096);
                destination.push_str("video");
                frame.transport_destination = Some(destination);
            }
            "key" => {
                let mut key = String::with_capacity(4096);
                key.push('k');
                frame.metadata.insert(key, serde_json::Value::Null);
            }
            "value" => {
                let mut value = String::with_capacity(4096);
                value.push('v');
                frame.metadata.insert("k".into(), value.into());
            }
            "array" => {
                let mut values = Vec::with_capacity(4096);
                values.push(serde_json::Value::Null);
                frame
                    .metadata
                    .insert("k".into(), serde_json::Value::Array(values));
            }
            "deep" => {
                let mut value = serde_json::Value::Null;
                for _ in 0..=NativeLlmContinuation::MAX_ARGUMENT_DEPTH {
                    value = serde_json::json!([value]);
                }
                frame.metadata.insert("k".into(), value);
            }
            _ => unreachable!(),
        }
        rejected_after_real_admission(frame, field).await;
    }
}

#[derive(Default)]
struct RejectAudioFilter {
    calls: AtomicUsize,
    stopped: AtomicUsize,
}
#[async_trait]
impl AudioFilter for RejectAudioFilter {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn filter(&self, _: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(ProcessorError::new("image must not enter PCM filter"))
    }
    async fn control(&self, _: &FrameData) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn typed_images_bypass_audio_filter_and_preserve_backend_owned_formats() {
    let filter = Arc::new(RejectAudioFilter::default());
    let case = Case::with_filter(config(), Some(filter.clone()))
        .await
        .unwrap();
    let backend = case.backend.clone();
    let mut unusual = assistant();
    if let FrameData::AssistantImage(image) = &mut unusual.data {
        image.image.image.image.clear();
        image.image.image.size = (0, 17);
        image.image.image.format = Some("backend-owned-format".into());
        image.image.sync_with_audio = false;
        image.original_data = None;
        image.original_mime_type = None;
    }
    let expected = vec![
        ordinary(),
        Frame::new(FrameData::OutputImageDetailed(OutputImageRawFrame {
            image: image(),
            sync_with_audio: true,
        })),
        Frame::new(FrameData::UrlImage(UrlImageRawFrame {
            image: OutputImageRawFrame {
                image: image(),
                sync_with_audio: false,
            },
            url: Some("https://images.example/preserved".into()),
        })),
        assistant(),
        unusual,
    ];
    let operations = async {
        case.ready().await?;
        for frame in &expected {
            case.send(frame.clone(), Direction::Downstream).await?;
            until("image backend entered", || backend.has_entered(frame.id)).await?;
        }
        Ok::<_, String>(())
    }
    .await;
    let finished = case.finish(false).await;
    assert_clean(&backend);
    operations.unwrap();
    let (frames, report) = finished.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(filter.calls.load(Ordering::SeqCst), 0);
    assert_eq!(filter.stopped.load(Ordering::SeqCst), 1);
    let writes = backend.writes.lock().unwrap();
    assert_eq!(writes.len(), expected.len());
    for (written, expected) in writes.iter().zip(&expected) {
        assert_preserved(written, expected);
        assert_eq!(downstream(&frames, expected.id).len(), 1);
    }
    assert!(!frames.iter().any(|item| matches!(
        item.frame.data,
        FrameData::BotStartedSpeaking | FrameData::BotStoppedSpeaking | FrameData::BotSpeaking
    )));
}

#[tokio::test]
async fn assistant_forwarding_waits_for_backend_completion() {
    let case = Case::start(config()).await.unwrap();
    let backend = case.backend.clone();
    let events = case.events.clone();
    let expected = assistant();
    let next = ordinary();
    backend.blocked.lock().unwrap().insert(expected.id);
    let operations = async {
        case.ready().await?;
        case.send(expected.clone(), Direction::Downstream).await?;
        until("assistant backend entered", || {
            backend.has_entered(expected.id)
        })
        .await?;
        case.send(next.clone(), Direction::Downstream).await?;
        let admitted = case.receipt(next.id).await?;
        Ok::<_, String>((
            admitted,
            events
                .lock()
                .unwrap()
                .contains(&Event::Forwarded(expected.id)),
            backend.has_entered(next.id),
        ))
    }
    .await;
    backend.gate.add_permits(1);
    let finished = case.finish(false).await;
    assert_clean(&backend);
    let (admitted, forwarded_early, next_entered_early) = operations.unwrap();
    admitted.unwrap();
    let (frames, report) = finished.unwrap();
    assert!(!forwarded_early);
    assert!(!next_entered_early);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(*backend.writes.lock().unwrap(), [expected.clone(), next]);
    assert_eq!(downstream(&frames, expected.id).len(), 1);
}

#[tokio::test]
async fn end_resumes_paused_assistant_queue_and_drains_once() {
    let case = Case::start(config()).await.unwrap();
    let backend = case.backend.clone();
    let expected = assistant();
    let operations = async {
        case.ready().await?;
        case.measured
            .output
            .control()
            .pause(None)
            .map_err(|error| error.to_string())?;
        case.send(expected.clone(), Direction::Downstream).await?;
        let admitted = case.receipt(expected.id).await?;
        Ok::<_, String>((admitted, backend.has_entered(expected.id)))
    }
    .await;
    let finished = case.finish(false).await;
    assert_clean(&backend);
    let (admitted, entered_while_paused) = operations.unwrap();
    admitted.unwrap();
    let (frames, report) = finished.unwrap();
    assert!(!entered_while_paused);
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        backend.writes.lock().unwrap().as_slice(),
        std::slice::from_ref(&expected)
    );
    assert_eq!(downstream(&frames, expected.id).len(), 1);
}

#[tokio::test]
async fn interruption_cancels_assistant_write_and_preserves_uninterruptible_successor() {
    let case = Case::start(config()).await.unwrap();
    let backend = case.backend.clone();
    let cancelled = assistant();
    let mut survivor = assistant();
    survivor.interruptible = false;
    backend.blocked.lock().unwrap().insert(cancelled.id);
    let operations = async {
        case.ready().await?;
        case.send(cancelled.clone(), Direction::Downstream).await?;
        until("assistant backend entered", || {
            backend.has_entered(cancelled.id)
        })
        .await?;
        case.send(survivor.clone(), Direction::Downstream).await?;
        case.receipt(survivor.id)
            .await?
            .map_err(|error| error.to_string())?;
        let interruption = Frame::new(FrameData::Interruption);
        let id = interruption.id;
        case.send(interruption, Direction::Downstream).await?;
        case.receipt(id).await?.map_err(|error| error.to_string())?;
        until("surviving image backend entered", || {
            backend.has_entered(survivor.id)
        })
        .await
    }
    .await;
    let finished = case.finish(false).await;
    assert_clean(&backend);
    operations.unwrap();
    let (frames, report) = finished.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(downstream(&frames, cancelled.id).is_empty());
    assert_eq!(downstream(&frames, survivor.id).len(), 1);
    assert_eq!(*backend.writes.lock().unwrap(), [survivor]);
    assert!(backend
        .interrupts
        .lock()
        .unwrap()
        .iter()
        .any(Option::is_none));
}

#[tokio::test]
async fn cancel_joins_inflight_assistant_write_without_forwarding_queued_images() {
    let case = Case::start(config()).await.unwrap();
    let backend = case.backend.clone();
    let first = assistant();
    let next = assistant();
    backend.blocked.lock().unwrap().insert(first.id);
    let operations = async {
        case.ready().await?;
        case.send(first.clone(), Direction::Downstream).await?;
        until("assistant backend entered", || {
            backend.has_entered(first.id)
        })
        .await?;
        case.send(next.clone(), Direction::Downstream).await?;
        case.receipt(next.id)
            .await?
            .map_err(|error| error.to_string())
    }
    .await;
    let finished = case.finish(true).await;
    assert_clean(&backend);
    operations.unwrap();
    let (frames, report) = finished.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(backend.writes.lock().unwrap().is_empty());
    assert!(downstream(&frames, first.id).is_empty());
    assert!(downstream(&frames, next.id).is_empty());
}

#[tokio::test]
async fn assistant_backend_error_and_timeout_do_not_forward_failed_images() {
    for blocked in [false, true] {
        let case = Case::start(config()).await.unwrap();
        let backend = case.backend.clone();
        let failed = assistant();
        let next = ordinary();
        if blocked {
            backend.blocked.lock().unwrap().insert(failed.id);
        } else {
            backend.failures.lock().unwrap().insert(failed.id);
        }
        let operations = async {
            case.ready().await?;
            case.send(failed.clone(), Direction::Downstream).await?;
            case.send(next.clone(), Direction::Downstream).await
        }
        .await;
        let finished = case.finish(false).await;
        assert_clean(&backend);
        operations.unwrap();
        let (frames, report) = finished.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        let message = if blocked {
            "media backend operation timed out"
        } else {
            "intentional image backend failure"
        };
        assert!(report.errors[0].error.contains(message));
        assert!(downstream(&frames, failed.id).is_empty());
        assert_eq!(downstream(&frames, next.id).len(), 1);
        assert_eq!(*backend.writes.lock().unwrap(), [next]);
    }
}

#[tokio::test]
async fn assistant_unknown_destination_is_rejected_without_affecting_default_output() {
    let case = Case::start(config()).await.unwrap();
    let backend = case.backend.clone();
    let mut unknown = assistant();
    unknown.transport_destination = Some("missing".into());
    let next = assistant();
    let operations = async {
        case.ready().await?;
        case.send(unknown.clone(), Direction::Downstream).await?;
        case.send(next.clone(), Direction::Downstream).await
    }
    .await;
    let finished = case.finish(false).await;
    assert_clean(&backend);
    operations.unwrap();
    let (frames, report) = finished.unwrap();
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(report.errors[0]
        .error
        .contains("unknown output destination"));
    assert!(downstream(&frames, unknown.id).is_empty());
    assert_eq!(downstream(&frames, next.id).len(), 1);
    assert_eq!(*backend.writes.lock().unwrap(), [next]);
}

#[tokio::test(start_paused = true)]
async fn assistant_legacy_pts_waits_on_the_existing_output_clock() {
    let case = Case::start(config()).await.unwrap();
    let backend = case.backend.clone();
    let mut expected = assistant();
    expected.pts = Some(500_000_000);
    let operations = async {
        case.ready().await?;
        case.send(expected.clone(), Direction::Downstream).await?;
        let admitted = case.receipt(expected.id).await?;
        tokio::time::advance(Duration::from_millis(250)).await;
        let early = backend.has_entered(expected.id);
        tokio::time::advance(Duration::from_millis(250)).await;
        until("scheduled image backend entered", || {
            backend.has_entered(expected.id)
        })
        .await?;
        Ok::<_, String>((admitted, early))
    }
    .await;
    let finished = case.finish(false).await;
    assert_clean(&backend);
    let (admitted, early) = operations.unwrap();
    admitted.unwrap();
    let (frames, report) = finished.unwrap();
    assert!(!early);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(*backend.writes.lock().unwrap(), [expected.clone()]);
    let forwarded = downstream(&frames, expected.id);
    assert_eq!(forwarded.len(), 1);
    assert_preserved(forwarded[0], &expected);
}
