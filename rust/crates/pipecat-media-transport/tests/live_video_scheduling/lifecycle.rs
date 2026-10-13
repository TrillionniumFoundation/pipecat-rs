//! Public two-lane contract witnesses. Every admission receipt comes from the
//! real OutputTransport::process, and every held operation is its actual backend
//! future. Paused-time observation windows are ordering probes, not benchmarks.
use async_trait::async_trait;
use pipecat_audio::{encode_pcm16_le, PcmFormat};
use pipecat_core::*;
use pipecat_media_transport::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::{timeout, Instant},
};

const WAIT: Duration = Duration::from_secs(5);
const WINDOW: Duration = Duration::from_millis(25);
const NAME: &str = "live-video-contract-output";
type ResultOf<T> = Result<T, String>;

async fn bounded<T, E: std::fmt::Display>(
    label: &str,
    future: impl Future<Output = Result<T, E>>,
) -> ResultOf<T> {
    timeout(WAIT, future)
        .await
        .map_err(|_| format!("{label}: deadline"))?
        .map_err(|e| format!("{label}: {e}"))
}
async fn changed_until(notify: &Notify, condition: impl Fn() -> bool) {
    loop {
        let changed = notify.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if condition() {
            return;
        }
        changed.await;
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    EnterImage(u64),
    Image(u64),
    EnterAudio(Option<String>),
    Audio(Option<String>),
    EnterMessage(u64),
    Message(u64),
    Forward(u64),
    DropImage(u64),
}
#[derive(Default)]
struct Trace {
    events: Mutex<Vec<Event>>,
    changed: Notify,
}
impl Trace {
    fn add(&self, event: Event) {
        self.events.lock().unwrap().push(event);
        self.changed.notify_waiters();
    }
    fn contains(&self, event: &Event) -> bool {
        self.events.lock().unwrap().contains(event)
    }
    async fn wait(&self, event: Event) -> ResultOf<()> {
        timeout(WAIT, changed_until(&self.changed, || self.contains(&event)))
            .await
            .map_err(|_| format!("event {event:?}: deadline"))
    }
    async fn within(&self, event: Event, duration: Duration) -> bool {
        timeout(
            duration,
            changed_until(&self.changed, || self.contains(&event)),
        )
        .await
        .is_ok()
    }
}
struct Backend {
    trace: Arc<Trace>,
    supported: bool,
    fail_start: AtomicBool,
    fail_audio: AtomicBool,
    image_gates: Mutex<BTreeMap<u64, Arc<Semaphore>>>,
    audio_gates: Mutex<BTreeMap<Option<String>, Arc<Semaphore>>>,
    message_gates: Mutex<BTreeMap<u64, Arc<Semaphore>>>,
    failures: Mutex<BTreeSet<u64>>,
    images: Mutex<Vec<Frame>>,
    calls: Mutex<Vec<(u64, Instant, Option<String>)>>,
    audio: Mutex<Vec<Frame>>,
    interrupts: Mutex<Vec<Option<String>>>,
    active: AtomicUsize,
    image_active: Mutex<BTreeMap<Option<String>, usize>>,
    image_peak: Mutex<BTreeMap<Option<String>, usize>>,
    starts: AtomicUsize,
    stops: AtomicUsize,
}
impl Backend {
    fn new(trace: Arc<Trace>, supported: bool) -> Self {
        Self {
            trace,
            supported,
            fail_start: AtomicBool::new(false),
            fail_audio: AtomicBool::new(false),
            image_gates: Mutex::new(BTreeMap::new()),
            audio_gates: Mutex::new(BTreeMap::new()),
            message_gates: Mutex::new(BTreeMap::new()),
            failures: Mutex::new(BTreeSet::new()),
            images: Mutex::new(vec![]),
            calls: Mutex::new(vec![]),
            audio: Mutex::new(vec![]),
            interrupts: Mutex::new(vec![]),
            active: AtomicUsize::new(0),
            image_active: Mutex::new(BTreeMap::new()),
            image_peak: Mutex::new(BTreeMap::new()),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        }
    }
    fn hold_image(&self, id: u64) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        self.image_gates.lock().unwrap().insert(id, gate.clone());
        gate
    }
    fn hold_audio(&self, destination: Option<&str>) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        self.audio_gates
            .lock()
            .unwrap()
            .insert(destination.map(str::to_owned), gate.clone());
        gate
    }
    fn release_all(&self) {
        for gate in self
            .image_gates
            .lock()
            .unwrap()
            .values()
            .chain(self.audio_gates.lock().unwrap().values())
            .chain(self.message_gates.lock().unwrap().values())
        {
            gate.add_permits(128);
        }
    }
    fn written(&self, id: u64) -> bool {
        self.images.lock().unwrap().iter().any(|f| f.id == id)
    }
}
struct Active<'a> {
    backend: &'a Backend,
    image: Option<(u64, Option<String>)>,
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.backend.active.fetch_sub(1, Ordering::SeqCst);
        if let Some((id, destination)) = &self.image {
            *self
                .backend
                .image_active
                .lock()
                .unwrap()
                .get_mut(destination)
                .unwrap() -= 1;
            self.backend.trace.add(Event::DropImage(*id));
        }
    }
}
#[async_trait]
impl OutputBackend for Backend {
    fn supports_independent_video(&self) -> bool {
        self.supported
    }
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        if self.fail_start.load(Ordering::SeqCst) {
            Err(ProcessorError::new("intentional start failure"))
        } else {
            Ok(())
        }
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active {
            backend: self,
            image: None,
        };
        let destination = frame.transport_destination.clone();
        self.trace.add(Event::EnterAudio(destination.clone()));
        let gate = self.audio_gates.lock().unwrap().get(&destination).cloned();
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .map_err(|e| ProcessorError::new(e.to_string()))?
                .forget();
        }
        if self.fail_audio.load(Ordering::SeqCst) {
            return Err(ProcessorError::new("intentional audio failure"));
        }
        self.audio.lock().unwrap().push(frame.clone());
        self.trace.add(Event::Audio(destination));
        Ok(PlaybackReceipt::Played)
    }
    async fn write_image(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let destination = frame.transport_destination.clone();
        {
            let mut active = self.image_active.lock().unwrap();
            let count = active.entry(destination.clone()).or_default();
            *count += 1;
            let mut peak = self.image_peak.lock().unwrap();
            let high = peak.entry(destination.clone()).or_default();
            *high = (*high).max(*count);
        }
        let _active = Active {
            backend: self,
            image: Some((frame.id, destination.clone())),
        };
        self.calls
            .lock()
            .unwrap()
            .push((frame.id, Instant::now(), destination));
        self.trace.add(Event::EnterImage(frame.id));
        let gate = self.image_gates.lock().unwrap().get(&frame.id).cloned();
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .map_err(|e| ProcessorError::new(e.to_string()))?
                .forget();
        }
        if self.failures.lock().unwrap().contains(&frame.id) {
            return Err(ProcessorError::new("intentional live image failure"));
        }
        self.images.lock().unwrap().push(frame.clone());
        self.trace.add(Event::Image(frame.id));
        Ok(())
    }
    async fn send_message(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active {
            backend: self,
            image: None,
        };
        self.trace.add(Event::EnterMessage(frame.id));
        let gate = self.message_gates.lock().unwrap().get(&frame.id).cloned();
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .map_err(|e| ProcessorError::new(e.to_string()))?
                .forget();
        }
        self.trace.add(Event::Message(frame.id));
        Ok(())
    }
    async fn interrupt(&self, destination: Option<&str>) -> Result<(), ProcessorError> {
        self.interrupts
            .lock()
            .unwrap()
            .push(destination.map(str::to_owned));
        self.trace.changed.notify_waiters();
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        self.trace.changed.notify_waiters();
        Ok(())
    }
}
struct Measured {
    output: Arc<OutputTransport>,
    starts: Mutex<BTreeSet<u64>>,
    receipts: Mutex<BTreeMap<u64, Result<(), ProcessorError>>>,
    changed: Notify,
    ready: AtomicBool,
    prepared: AtomicBool,
    context: Mutex<Option<ProcessorContext>>,
}
#[async_trait]
impl FrameProcessor for Measured {
    fn name(&self) -> &str {
        NAME
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
        let id = frame.id;
        let start = frame.data.is_start();
        self.starts.lock().unwrap().insert(id);
        self.changed.notify_waiters();
        let result = self.output.process(frame, direction, ctx).await;
        self.receipts.lock().unwrap().insert(id, result.clone());
        if start && result.is_ok() {
            self.ready.store(true, Ordering::SeqCst);
        }
        self.changed.notify_waiters();
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let result = self.output.prepare_end(ctx).await;
        self.prepared.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
        result
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.output.cleanup().await
    }
}
struct Recorder(Arc<Trace>);
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "live-video-contract-recorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.0.add(Event::Forward(frame.id));
        }
        ctx.push(frame, direction).await
    }
}
struct Clock(Instant);
impl ObserverClock for Clock {
    fn now_ns(&self) -> u64 {
        9_000_000_000 + self.0.elapsed().as_nanos() as u64
    }
}
fn config() -> OutputTransportConfig {
    OutputTransportConfig {
        sample_rate: Some(8_000),
        channels: 1,
        chunk_duration: Duration::from_millis(10),
        queue_capacity: 16,
        queue_byte_capacity: 64 * 1024,
        admission_policy: OutputAdmissionPolicy::FailFast,
        auto_silence: false,
        end_silence: Duration::ZERO,
        write_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}
fn live_config() -> LiveVideoOutputConfig {
    LiveVideoOutputConfig {
        queue_capacity: 16,
        queue_byte_capacity: 64 * 1024,
        framerate: None,
    }
}
fn make_output(cfg: OutputTransportConfig, backend: Arc<Backend>) -> OutputTransport {
    OutputTransport::new(NAME, cfg, backend, None, BTreeMap::new()).unwrap()
}
struct Case {
    worker: PipelineWorker,
    measured: Arc<Measured>,
    backend: Arc<Backend>,
    trace: Arc<Trace>,
    control: OutputControl,
    clock: Arc<Clock>,
}
impl Case {
    async fn start(cfg: OutputTransportConfig, video: LiveVideoOutputConfig) -> ResultOf<Self> {
        Self::start_paused(cfg, video, None).await
    }
    async fn start_paused(
        cfg: OutputTransportConfig,
        video: LiveVideoOutputConfig,
        before_builder: Option<bool>,
    ) -> ResultOf<Self> {
        let trace = Arc::new(Trace::default());
        let backend = Arc::new(Backend::new(trace.clone(), true));
        let output = make_output(cfg, backend.clone());
        let control = output.control();
        if let Some(paused) = before_builder {
            control.pause(None).map_err(|e| e.to_string())?;
            if !paused {
                control.resume(None).map_err(|e| e.to_string())?;
            }
        }
        let output = Arc::new(output.with_live_video(video).map_err(|e| e.to_string())?);
        let measured = Arc::new(Measured {
            output,
            starts: Mutex::new(BTreeSet::new()),
            receipts: Mutex::new(BTreeMap::new()),
            changed: Notify::new(),
            ready: AtomicBool::new(false),
            prepared: AtomicBool::new(false),
            context: Mutex::new(None),
        });
        let clock = Arc::new(Clock(Instant::now()));
        let worker = bounded(
            "start",
            Pipeline::new(vec![measured.clone(), Arc::new(Recorder(trace.clone()))])
                .with_observer_clock(clock.clone())
                .start(PipelineConfig {
                    shutdown_timeout: WAIT,
                    ..Default::default()
                }),
        )
        .await?;
        let case = Self {
            worker,
            measured,
            backend,
            trace,
            control,
            clock,
        };
        timeout(
            WAIT,
            changed_until(&case.measured.changed, || {
                case.measured.ready.load(Ordering::SeqCst)
            }),
        )
        .await
        .map_err(|_| "ready deadline")?;
        Ok(case)
    }
    async fn send(&self, frame: Frame) -> ResultOf<()> {
        bounded(
            "ingress",
            self.worker.sender().send(frame, Direction::Downstream),
        )
        .await
    }
    async fn admit(&self, frame: Frame) -> ResultOf<()> {
        let id = frame.id;
        self.send(frame).await?;
        self.receipt(id).await?.map_err(|e| e.to_string())
    }
    async fn receipt(&self, id: u64) -> ResultOf<Result<(), ProcessorError>> {
        timeout(
            WAIT,
            changed_until(&self.measured.changed, || {
                self.measured.receipts.lock().unwrap().contains_key(&id)
            }),
        )
        .await
        .map_err(|_| format!("admission {id}: deadline"))?;
        Ok(self.measured.receipts.lock().unwrap()[&id].clone())
    }
    async fn begun(&self, id: u64) -> ResultOf<()> {
        timeout(
            WAIT,
            changed_until(&self.measured.changed, || {
                self.measured.starts.lock().unwrap().contains(&id)
            }),
        )
        .await
        .map_err(|_| "process begin deadline".into())
    }
    async fn interrupt(&self, destination: Option<&str>) -> ResultOf<()> {
        let before = self.backend.interrupts.lock().unwrap().len();
        let mut frame = Frame::interruption();
        frame.transport_destination = destination.map(str::to_owned);
        self.admit(frame).await?;
        timeout(
            WAIT,
            changed_until(&self.trace.changed, || {
                self.backend.interrupts.lock().unwrap().len() > before
            }),
        )
        .await
        .map_err(|_| "interrupt hook deadline".into())
    }
    async fn finish(self, terminal: Terminal) -> Finished {
        self.finish_inner(terminal, true).await
    }
    async fn finish_existing(self, terminal: Terminal) -> Finished {
        self.finish_inner(terminal, false).await
    }
    async fn finish_inner(mut self, terminal: Terminal, send_terminal: bool) -> Finished {
        if terminal != Terminal::Cancel {
            self.backend.release_all();
        }
        let sent = if !send_terminal {
            Ok(())
        } else {
            match terminal {
                Terminal::End => bounded("End", self.worker.end()).await,
                Terminal::Stop => self.send(Frame::new(FrameData::Stop)).await,
                Terminal::Cancel => {
                    bounded("Cancel", self.worker.cancel("live-video test cleanup")).await
                }
            }
        };
        if sent.is_err() {
            let _ = self.worker.cancel("terminal failed").await;
        }
        let joined = bounded("join", self.worker.join()).await;
        if joined.is_err() {
            let _ = self.worker.cancel("join failed").await;
            let _ = bounded("fallback join", self.worker.join()).await;
        }
        let stops_before_owner_cleanup = self.backend.stops.load(Ordering::SeqCst);
        let owner_cleanup = if terminal == Terminal::Stop {
            bounded("owner cleanup", self.measured.output.cleanup()).await
        } else {
            Ok(())
        };
        let mut frames = vec![];
        while let Ok(frame) = self.worker.try_recv() {
            frames.push(frame);
        }
        Finished {
            backend: self.backend,
            control: self.control,
            frames,
            joined,
            sent,
            owner_cleanup,
            stops_before_owner_cleanup,
            terminal,
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Terminal {
    End,
    Stop,
    Cancel,
}
struct Finished {
    backend: Arc<Backend>,
    control: OutputControl,
    frames: Vec<DirectedFrame>,
    joined: ResultOf<WorkerReport>,
    sent: ResultOf<()>,
    owner_cleanup: ResultOf<()>,
    stops_before_owner_cleanup: usize,
    terminal: Terminal,
}
impl Finished {
    fn clean(&self) {
        assert_eq!(
            self.backend.active.load(Ordering::SeqCst),
            0,
            "backend future survived join"
        );
        assert_eq!(self.backend.starts.load(Ordering::SeqCst), 1);
        assert_eq!(self.backend.stops.load(Ordering::SeqCst), 1);
        if self.terminal == Terminal::Stop {
            assert_eq!(
                self.stops_before_owner_cleanup, 0,
                "Stop must retain owner cleanup"
            );
        }
        assert!(!self.control.is_ready());
        assert!(self.sent.is_ok(), "{:?}", self.sent);
        assert!(self.owner_cleanup.is_ok(), "{:?}", self.owner_cleanup);
        assert!(self.joined.is_ok(), "{:?}", self.joined);
        assert!(
            self.backend
                .image_peak
                .lock()
                .unwrap()
                .values()
                .all(|peak| *peak <= 1),
            "per-destination images overlapped"
        );
    }
    fn normal(&self) {
        self.clean();
        let report = self.joined.as_ref().unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            report.state,
            match self.terminal {
                Terminal::End => WorkerState::Ended,
                Terminal::Stop => WorkerState::Stopped,
                Terminal::Cancel => WorkerState::Cancelled,
            }
        );
    }
    fn count(&self, id: u64) -> usize {
        self.frames
            .iter()
            .filter(|f| f.direction == Direction::Downstream && f.frame.id == id)
            .count()
    }
}
fn raw_image() -> ImageRawFrame {
    ImageRawFrame {
        image: vec![1, 2, 3],
        size: (1, 1),
        format: Some("RGB".into()),
    }
}
fn image(sync: bool) -> Frame {
    Frame::new(FrameData::OutputImageDetailed(OutputImageRawFrame {
        image: raw_image(),
        sync_with_audio: sync,
    }))
}
fn variants() -> Vec<Frame> {
    vec![
        Frame::new(FrameData::OutputImage(raw_image())),
        image(false),
        Frame::new(FrameData::UrlImage(UrlImageRawFrame {
            image: OutputImageRawFrame {
                image: raw_image(),
                sync_with_audio: false,
            },
            url: Some("https://images.example/live".into()),
        })),
        Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
            image: OutputImageRawFrame {
                image: raw_image(),
                sync_with_audio: false,
            },
            original_data: Some(vec![0x89, 0x50, 0x4e, 0x47]),
            original_mime_type: Some("image/png".into()),
        })),
    ]
}
fn pcm(samples: usize, destination: Option<&str>) -> Frame {
    let mut frame = Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(encode_pcm16_le(&vec![500; samples]), 8_000, 1).unwrap(),
    ));
    frame.transport_destination = destination.map(str::to_owned);
    frame
}
fn fence(interruptible: bool) -> Frame {
    let mut frame = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        origin: Some("pipecat.flows".into()),
        returning: false,
        event: Some(LocalFlushEvent::default()),
    }));
    frame.interruptible = interruptible;
    frame
        .metadata
        .insert("pipecat.flows.barrier".into(), 41.into());
    frame
}
fn flush_event(frame: &Frame) -> LocalFlushEvent {
    if let FrameData::FlushDetailed(flush) = &frame.data {
        flush.event.clone().unwrap()
    } else {
        unreachable!()
    }
}

async fn wait_fence_receipt(event: &LocalFlushEvent) -> ResultOf<()> {
    timeout(WAIT, event.wait())
        .await
        .map_err(|_| "returning fence receipt deadline".into())
}

#[test]
fn builder_rejects_missing_capability_invalid_and_repeated_configuration_without_io() {
    let backend = Arc::new(Backend::new(Arc::new(Trace::default()), false));
    assert!(make_output(config(), backend.clone())
        .with_live_video(live_config())
        .is_err());
    assert_eq!(backend.starts.load(Ordering::SeqCst), 0);
    let backend = Arc::new(Backend::new(Arc::new(Trace::default()), true));
    for video in [
        LiveVideoOutputConfig {
            queue_capacity: 0,
            ..live_config()
        },
        LiveVideoOutputConfig {
            queue_byte_capacity: 0,
            ..live_config()
        },
        LiveVideoOutputConfig {
            framerate: Some(0),
            ..live_config()
        },
        LiveVideoOutputConfig {
            framerate: Some(1_000_000_001),
            ..live_config()
        },
        LiveVideoOutputConfig {
            queue_capacity: usize::MAX,
            ..live_config()
        },
        LiveVideoOutputConfig {
            queue_byte_capacity: usize::MAX,
            ..live_config()
        },
    ] {
        assert!(
            make_output(config(), backend.clone())
                .with_live_video(video)
                .is_err(),
            "accepted invalid config {video:?}"
        );
    }
    let enabled = make_output(config(), backend.clone())
        .with_live_video(live_config())
        .unwrap();
    assert!(enabled.with_live_video(live_config()).is_err());
    assert_eq!(backend.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn held_live_image_allows_plain_pcm_and_preserves_success_before_forward() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let held = image(false);
    let gate = case.backend.hold_image(held.id);
    let trace = case.trace.clone();
    let operations = async {
        case.admit(held.clone()).await?;
        trace.wait(Event::EnterImage(held.id)).await?;
        case.admit(pcm(80, None)).await?;
        trace.wait(Event::Audio(None)).await?;
        Ok::<_, String>((
            trace.contains(&Event::Image(held.id)),
            trace.contains(&Event::Forward(held.id)),
        ))
    }
    .await;
    gate.add_permits(1);
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(operations.unwrap(), (false, false));
    assert_eq!(finished.count(held.id), 1);
    let events = trace.events.lock().unwrap();
    assert!(
        events.iter().position(|e| *e == Event::Image(held.id))
            < events.iter().position(|e| *e == Event::Forward(held.id))
    );
}

#[tokio::test(start_paused = true)]
async fn all_live_variants_preserve_envelopes_and_upstream_bypasses_backend() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let mut expected = variants();
    for (index, frame) in expected.iter_mut().enumerate() {
        frame.name = format!("image-envelope-{index}");
        frame.pts = Some(0);
        frame.transport_source = Some("generator".into());
        frame.broadcast_sibling_id = Some(700 + index as u64);
        frame.interruptible = false;
        frame.metadata.insert(
            "nested".into(),
            serde_json::json!({"v": [1, "source", null]}),
        );
    }
    let upstream: Vec<_> = expected.iter().map(Frame::duplicate).collect();
    let operations = async {
        for frame in &expected {
            case.admit(frame.clone()).await?;
            case.trace.wait(Event::Image(frame.id)).await?;
        }
        for frame in &upstream {
            bounded(
                "upstream",
                case.worker
                    .sender()
                    .send(frame.clone(), Direction::Upstream),
            )
            .await?;
            case.receipt(frame.id).await?.map_err(|e| e.to_string())?;
        }
        Ok::<_, String>(())
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    operations.unwrap();
    assert_eq!(*finished.backend.images.lock().unwrap(), expected);
    for frame in &expected {
        assert_eq!(finished.count(frame.id), 1);
        let actual = &finished
            .frames
            .iter()
            .find(|f| f.frame.id == frame.id)
            .unwrap()
            .frame;
        assert_eq!(actual, frame);
        assert_eq!(actual.observation_token(), frame.observation_token());
    }
    for frame in &upstream {
        assert!(!finished.backend.written(frame.id));
        assert_eq!(
            finished
                .frames
                .iter()
                .filter(|f| f.direction == Direction::Upstream && f.frame == *frame)
                .count(),
            1
        );
    }
    assert!(finished.backend.audio.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn image_gate_serializes_true_and_false_without_blocking_earlier_pcm() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let first = image(false);
    let sync = image(true);
    let last = image(false);
    let gate = case.backend.hold_image(first.id);
    let operations = async {
        case.admit(first.clone()).await?;
        case.trace.wait(Event::EnterImage(first.id)).await?;
        case.admit(pcm(80, None)).await?;
        case.trace.wait(Event::Audio(None)).await?;
        case.admit(sync.clone()).await?;
        case.admit(last.clone()).await?;
        let sync_early = case.trace.within(Event::EnterImage(sync.id), WINDOW).await;
        let last_early = case.trace.within(Event::EnterImage(last.id), WINDOW).await;
        Ok::<_, String>((sync_early, last_early))
    }
    .await;
    gate.add_permits(1);
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(operations.unwrap(), (false, false));
    for frame in [&first, &sync, &last] {
        assert_eq!(finished.count(frame.id), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn image_gate_wait_and_backend_share_one_absolute_timeout() {
    let mut cfg = config();
    cfg.write_timeout = Duration::from_millis(500);
    let case = Case::start(cfg, live_config()).await.unwrap();
    let first = image(false);
    let waiting = image(true);
    let release_first = case.backend.hold_image(first.id);
    case.backend.hold_image(waiting.id);
    let operations = async {
        case.admit(first.clone()).await?;
        case.trace.wait(Event::EnterImage(first.id)).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        case.admit(waiting.clone()).await?;
        let began = Instant::now();
        let premature = case
            .trace
            .within(Event::EnterImage(waiting.id), Duration::from_millis(300))
            .await;
        release_first.add_permits(1);
        case.trace.wait(Event::EnterImage(waiting.id)).await?;
        let dropped = case
            .trace
            .within(Event::DropImage(waiting.id), Duration::from_millis(205))
            .await;
        Ok::<_, String>((premature, dropped, began.elapsed()))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    let (premature, dropped, elapsed) = operations.unwrap();
    assert!(!premature);
    assert!(dropped, "gate acquisition incorrectly reset write_timeout");
    assert!(elapsed <= Duration::from_millis(505));
    assert!(finished.backend.written(first.id));
    assert!(!finished.backend.written(waiting.id));
    assert_eq!(finished.count(waiting.id), 0);
    assert!(!finished.joined.as_ref().unwrap().errors.is_empty());
}

#[tokio::test(start_paused = true)]
async fn interruption_revokes_gate_waiters_without_replay_and_keeps_workers_alive() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let held = image(false);
    let waiting = image(true);
    let queued = image(false);
    let fresh_live = image(false);
    let fresh_sync = image(true);
    case.backend.hold_image(held.id);
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await?;
        case.admit(waiting.clone()).await?;
        case.admit(queued.clone()).await?;
        let before = case
            .trace
            .within(Event::EnterImage(waiting.id), WINDOW)
            .await;
        case.interrupt(None).await?;
        case.trace.wait(Event::DropImage(held.id)).await?;
        case.admit(fresh_live.clone()).await?;
        case.trace.wait(Event::Image(fresh_live.id)).await?;
        case.admit(fresh_sync.clone()).await?;
        case.trace.wait(Event::Image(fresh_sync.id)).await?;
        Ok::<_, String>(before)
    }
    .await;
    let interruptions_before_cleanup = case.backend.interrupts.lock().unwrap().clone();
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(
        finished.backend.interrupts.lock().unwrap().len(),
        interruptions_before_cleanup.len() + 1,
        "cleanup must interrupt each configured destination once"
    );
    assert!(!operations.unwrap());
    for frame in [&held, &waiting, &queued] {
        assert_eq!(finished.count(frame.id), 0);
        assert!(!finished.backend.written(frame.id));
    }
    assert!(!finished
        .backend
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|call| call.0 == waiting.id || call.0 == queued.id));
    assert_eq!(interruptions_before_cleanup.as_slice(), &[None]);
    assert_eq!(finished.count(fresh_live.id), 1);
    assert_eq!(finished.count(fresh_sync.id), 1);
}

#[tokio::test(start_paused = true)]
async fn uninterruptible_live_current_and_queued_images_survive_epoch_change() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let mut held = image(false);
    held.interruptible = false;
    let mut queued = image(false);
    queued.interruptible = false;
    let gate = case.backend.hold_image(held.id);
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await?;
        case.admit(queued.clone()).await?;
        case.interrupt(None).await?;
        Ok::<_, String>(case.trace.within(Event::DropImage(held.id), WINDOW).await)
    }
    .await;
    gate.add_permits(1);
    let interruptions_before_cleanup = case.backend.interrupts.lock().unwrap().clone();
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(
        finished.backend.interrupts.lock().unwrap().len(),
        interruptions_before_cleanup.len() + 1,
        "cleanup must interrupt each configured destination once"
    );
    assert!(!operations.unwrap());
    assert_eq!(finished.count(held.id), 1);
    assert_eq!(finished.count(queued.id), 1);
    assert_eq!(interruptions_before_cleanup.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn named_destination_gate_pause_and_interruption_are_isolated() {
    let mut cfg = config();
    cfg.destinations = vec!["other".into()];
    let case = Case::start(cfg, live_config()).await.unwrap();
    let held = image(false);
    case.backend.hold_image(held.id);
    let mut other = image(false);
    other.transport_destination = Some("other".into());
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await?;
        case.control.pause(None).map_err(|e| e.to_string())?;
        case.admit(other.clone()).await?;
        case.trace.wait(Event::Image(other.id)).await?;
        case.admit(pcm(80, Some("other"))).await?;
        case.trace.wait(Event::Audio(Some("other".into()))).await?;
        case.interrupt(Some("other")).await?;
        let held_dropped = case.trace.within(Event::DropImage(held.id), WINDOW).await;
        case.control.resume(None).map_err(|e| e.to_string())?;
        Ok::<_, String>(held_dropped)
    }
    .await;
    let interruptions_before_cleanup = case.backend.interrupts.lock().unwrap().clone();
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(
        finished.backend.interrupts.lock().unwrap().len(),
        interruptions_before_cleanup.len() + 2,
        "cleanup must interrupt each configured destination once"
    );
    assert!(!operations.unwrap());
    assert_eq!(finished.count(held.id), 1);
    assert_eq!(finished.count(other.id), 1);
    assert_eq!(
        interruptions_before_cleanup.as_slice(),
        &[Some("other".into())]
    );
}

#[tokio::test(start_paused = true)]
async fn unknown_destination_rejects_live_sync_and_control_without_backend_effects() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let mut frames = vec![
        image(false),
        image(true),
        pcm(80, None),
        Frame::interruption(),
    ];
    for frame in &mut frames {
        frame.transport_destination = Some("missing".into());
    }
    let operations = async {
        let mut rejected = vec![];
        for frame in &frames {
            case.send(frame.clone()).await?;
            rejected.push(case.receipt(frame.id).await?.is_err());
        }
        Ok::<_, String>(rejected)
    }
    .await;
    let pause = case.control.pause(Some("missing"));
    let resume = case.control.resume(Some("missing"));
    let interruptions_before_cleanup = case.backend.interrupts.lock().unwrap().clone();
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    assert_eq!(
        finished.backend.interrupts.lock().unwrap().len(),
        interruptions_before_cleanup.len() + 1,
        "cleanup must interrupt each configured destination once"
    );
    assert!(operations.unwrap().iter().all(|v| *v));
    assert!(pause.is_err());
    assert!(resume.is_err());
    assert!(finished.backend.images.lock().unwrap().is_empty());
    assert!(finished.backend.audio.lock().unwrap().is_empty());
    assert!(interruptions_before_cleanup.is_empty());
}

#[tokio::test(start_paused = true)]
async fn count_budgets_are_additive_and_live_failfast_never_replaces_frames() {
    let mut cfg = config();
    cfg.queue_capacity = 1;
    let mut video = live_config();
    video.queue_capacity = 1;
    let case = Case::start(cfg, video).await.unwrap();
    let held = image(false);
    let queued = image(false);
    let rejected = image(false);
    case.backend.hold_image(held.id);
    case.backend.hold_audio(None);
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await?;
        case.admit(pcm(80, None)).await?;
        case.trace.wait(Event::EnterAudio(None)).await?;
        case.admit(queued.clone()).await?;
        case.admit(pcm(80, None)).await?;
        case.send(rejected.clone()).await?;
        case.receipt(rejected.id).await
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    assert!(operations.unwrap().is_err());
    assert_eq!(finished.backend.audio.lock().unwrap().len(), 2);
    assert_eq!(finished.count(held.id), 1);
    assert_eq!(finished.count(queued.id), 1);
    assert_eq!(finished.count(rejected.id), 0);
}

#[tokio::test(start_paused = true)]
async fn live_byte_budget_is_independent_of_count_and_media_capacity() {
    let mut video = live_config();
    video.queue_byte_capacity = 2048;
    let case = Case::start(config(), video).await.unwrap();
    let held = image(false);
    case.backend.hold_image(held.id);
    let mut queued = image(false);
    let FrameData::OutputImageDetailed(data) = &mut queued.data else {
        unreachable!()
    };
    data.image.image = vec![3; 1024];
    let rejected = queued.duplicate();
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await?;
        case.admit(queued.clone()).await?;
        case.send(rejected.clone()).await?;
        let result = case.receipt(rejected.id).await?;
        case.admit(pcm(80, None)).await?;
        case.trace.wait(Event::Audio(None)).await?;
        Ok::<_, String>(result)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    assert!(operations.unwrap().is_err());
    assert_eq!(finished.count(queued.id), 1);
    assert_eq!(finished.count(rejected.id), 0);
}

fn reserve_field(field: &str) -> Frame {
    let mut frame = variants().pop().unwrap();
    match field {
        "pixels" | "format" | "original" | "mime" => {
            let FrameData::AssistantImage(data) = &mut frame.data else {
                unreachable!()
            };
            match field {
                "pixels" => data.image.image.image.reserve(4096),
                "format" => data.image.image.format.as_mut().unwrap().reserve(4096),
                "original" => data.original_data.as_mut().unwrap().reserve(4096),
                "mime" => data.original_mime_type.as_mut().unwrap().reserve(4096),
                _ => unreachable!(),
            }
        }
        "compact" => {
            let mut data = raw_image();
            data.image.reserve(4096);
            frame = Frame::new(FrameData::OutputImage(data));
        }
        "typed" => {
            frame = image(false);
            let FrameData::OutputImageDetailed(data) = &mut frame.data else {
                unreachable!()
            };
            data.image.image.reserve(4096);
        }
        "url" => {
            frame = variants().remove(2);
            let FrameData::UrlImage(data) = &mut frame.data else {
                unreachable!()
            };
            data.url.as_mut().unwrap().reserve(4096);
        }
        "name" => frame.name.reserve(4096),
        "source" => {
            let mut value = String::with_capacity(4096);
            value.push('s');
            frame.transport_source = Some(value);
        }
        "destination" => {
            let mut value = String::with_capacity(4096);
            value.push_str("video");
            frame.transport_destination = Some(value);
        }
        "key" => {
            let mut value = String::with_capacity(4096);
            value.push('k');
            frame.metadata.insert(value, serde_json::Value::Null);
        }
        "value" => {
            let mut value = String::with_capacity(4096);
            value.push('v');
            frame.metadata.insert("k".into(), value.into());
        }
        "array" => {
            let mut value = Vec::with_capacity(4096);
            value.push(serde_json::Value::Null);
            frame
                .metadata
                .insert("k".into(), serde_json::Value::Array(value));
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
    frame
}
#[tokio::test(start_paused = true)]
async fn selected_live_budget_charges_all_variants_spare_allocations_and_metadata() {
    for field in [
        "compact",
        "typed",
        "url",
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
        let mut cfg = config();
        cfg.destinations = vec!["video".into()];
        cfg.admission_policy = OutputAdmissionPolicy::Wait {
            timeout: Duration::from_secs(1),
        };
        let mut video = live_config();
        video.queue_byte_capacity = 1024;
        let case = Case::start(cfg, video).await.unwrap();
        let oversized = reserve_field(field);
        let id = oversized.id;
        // Move the original allocation. clone()/duplicate() can shrink spare capacity.
        let operations = async {
            let start = Instant::now();
            case.send(oversized).await?;
            let receipt = case.receipt(id).await?;
            Ok::<_, String>((receipt, start.elapsed()))
        }
        .await;
        let finished = case.finish(Terminal::End).await;
        finished.clean();
        let (receipt, elapsed) = operations.unwrap();
        assert!(receipt.is_err(), "{field} escaped live byte accounting");
        assert_eq!(
            elapsed,
            Duration::ZERO,
            "{field}: oversize waited for admission"
        );
        assert_eq!(finished.count(id), 0);
        assert!(finished.backend.images.lock().unwrap().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn full_live_wait_blocks_shared_normal_admission_then_resumes_without_drop() {
    let mut cfg = config();
    cfg.admission_policy = OutputAdmissionPolicy::Wait {
        timeout: Duration::from_secs(1),
    };
    let mut video = live_config();
    video.queue_capacity = 1;
    let case = Case::start(cfg, video).await.unwrap();
    let first = image(false);
    let second = image(false);
    let third = image(false);
    let audio = pcm(80, None);
    let gate = case.backend.hold_image(first.id);
    let operations = async {
        case.admit(first.clone()).await?;
        case.trace.wait(Event::EnterImage(first.id)).await?;
        case.admit(second.clone()).await?;
        case.send(third.clone()).await?;
        case.begun(third.id).await?;
        case.send(audio.clone()).await?;
        let early_audio = case.trace.within(Event::Audio(None), WINDOW).await;
        let receipt_pending = !case
            .measured
            .receipts
            .lock()
            .unwrap()
            .contains_key(&third.id);
        gate.add_permits(1);
        case.receipt(third.id).await?.map_err(|e| e.to_string())?;
        case.receipt(audio.id).await?.map_err(|e| e.to_string())?;
        case.trace.wait(Event::Audio(None)).await?;
        Ok::<_, String>((early_audio, receipt_pending))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(operations.unwrap(), (false, true));
    for frame in [&first, &second, &third] {
        assert_eq!(finished.count(frame.id), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn live_wait_timeout_is_absolute_across_pause_notifications() {
    let mut cfg = config();
    cfg.admission_policy = OutputAdmissionPolicy::Wait {
        timeout: Duration::from_millis(200),
    };
    let mut video = live_config();
    video.queue_capacity = 1;
    let case = Case::start(cfg, video).await.unwrap();
    let held = image(false);
    let queued = image(false);
    let waiting = image(false);
    case.backend.hold_image(held.id);
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await?;
        case.admit(queued.clone()).await?;
        case.send(waiting.clone()).await?;
        case.begun(waiting.id).await?;
        let began = Instant::now();
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            case.control.pause(None).map_err(|e| e.to_string())?;
            case.control.resume(None).map_err(|e| e.to_string())?;
        }
        let receipt = case.receipt(waiting.id).await?;
        Ok::<_, String>((receipt, began.elapsed()))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    let (receipt, elapsed) = operations.unwrap();
    assert!(receipt.is_err());
    assert_eq!(elapsed, Duration::from_millis(200));
    assert_eq!(finished.count(waiting.id), 0);
    assert_eq!(finished.count(queued.id), 1);
}

#[tokio::test(start_paused = true)]
async fn control_cloned_before_builder_preserves_pause_and_resume_on_both_lanes() {
    for prepaused in [true, false] {
        let case = Case::start_paused(config(), live_config(), Some(prepaused))
            .await
            .unwrap();
        let frame = image(false);
        let operations = async {
            case.admit(frame.clone()).await?;
            case.admit(pcm(80, None)).await?;
            let image_early = case.trace.within(Event::Image(frame.id), WINDOW).await;
            let audio_early = case.trace.within(Event::Audio(None), WINDOW).await;
            case.control.resume(None).map_err(|e| e.to_string())?;
            case.trace.wait(Event::Image(frame.id)).await?;
            case.trace.wait(Event::Audio(None)).await?;
            Ok::<_, String>((image_early, audio_early))
        }
        .await;
        let finished = case.finish(Terminal::End).await;
        finished.normal();
        assert_eq!(operations.unwrap(), (!prepaused, !prepaused));
    }
}

#[tokio::test(start_paused = true)]
async fn direct_control_pause_is_resumed_by_end_and_stop_drain() {
    for terminal in [Terminal::End, Terminal::Stop] {
        let case = Case::start_paused(config(), live_config(), Some(true))
            .await
            .unwrap();
        let frame = image(false);
        let operations = async {
            case.admit(frame.clone()).await?;
            case.admit(pcm(80, None)).await?;
            Ok::<_, String>(case.trace.within(Event::Image(frame.id), WINDOW).await)
        }
        .await;
        let finished = case.finish(terminal).await;
        finished.normal();
        assert!(!operations.unwrap());
        assert_eq!(finished.count(frame.id), 1);
        assert_eq!(finished.backend.audio.lock().unwrap().len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn late_prebuilder_control_pause_cannot_reclose_end_or_stop_drain() {
    for terminal in [Terminal::End, Terminal::Stop] {
        let case = Case::start(config(), live_config()).await.unwrap();
        let held = image(false);
        let queued = image(false);
        case.backend.hold_image(held.id);
        let operations = async {
            case.admit(held.clone()).await?;
            case.trace.wait(Event::EnterImage(held.id)).await?;
            case.admit(queued.clone()).await?;
            case.admit(pcm(80, None)).await?;
            match terminal {
                Terminal::End => bounded("End ingress", case.worker.end()).await?,
                Terminal::Stop => case.send(Frame::new(FrameData::Stop)).await?,
                Terminal::Cancel => unreachable!(),
            }
            timeout(
                WAIT,
                changed_until(&case.measured.changed, || {
                    case.measured.prepared.load(Ordering::SeqCst)
                }),
            )
            .await
            .map_err(|_| "prepare_end deadline")?;
            // This is the control clone created before with_live_video.
            case.control.pause(None).map_err(|e| e.to_string())?;
            let context = case
                .measured
                .context
                .lock()
                .unwrap()
                .clone()
                .ok_or("missing context")?;
            let rejected = case
                .measured
                .output
                .process(image(false), Direction::Downstream, &context)
                .await;
            Ok::<_, String>(rejected)
        }
        .await;
        let finished = case.finish_existing(terminal).await;
        finished.normal();
        assert!(
            operations.unwrap().is_err(),
            "closed live admission reopened"
        );
        assert_eq!(finished.count(held.id), 1);
        assert_eq!(finished.count(queued.id), 1);
        assert_eq!(finished.backend.audio.lock().unwrap().len(), 1);
    }
}

#[tokio::test(start_paused = true)]
async fn ordinary_and_urgent_processor_pause_require_addressed_resume_before_end() {
    for urgent in [false, true] {
        for terminal in [Terminal::End, Terminal::Stop] {
            let case = Case::start(config(), live_config()).await.unwrap();
            let pause = Frame::new(if urgent {
                FrameData::ProcessorPauseUrgent {
                    processor: NAME.into(),
                }
            } else {
                FrameData::ProcessorPause {
                    processor: NAME.into(),
                }
            });
            let frame = image(false);
            case.backend.hold_image(frame.id);
            let terminal_frame = match terminal {
                Terminal::End => Frame::end(),
                Terminal::Stop => Frame::new(FrameData::Stop),
                Terminal::Cancel => unreachable!(),
            };
            let operations = async {
                case.admit(pause.clone()).await?;
                case.trace.wait(Event::Forward(pause.id)).await?;
                case.send(frame.clone()).await?;
                // Normal terminal ingress has succeeded before the observation.
                // A supported administrative Resume may still enter afterward.
                case.send(terminal_frame.clone()).await?;
                let premature_image = case.trace.within(Event::EnterImage(frame.id), WINDOW).await;
                let normal_started = case.measured.starts.lock().unwrap().contains(&frame.id);
                let terminal_started = case
                    .measured
                    .starts
                    .lock()
                    .unwrap()
                    .contains(&terminal_frame.id);
                let terminal_prepared = case.measured.prepared.load(Ordering::SeqCst);
                let worker_finished = case.worker.has_finished();
                let resume = Frame::new(FrameData::ProcessorResumeUrgent {
                    processor: NAME.into(),
                });
                case.admit(resume.clone()).await?;
                case.trace.wait(Event::Forward(resume.id)).await?;
                case.receipt(frame.id).await?.map_err(|e| e.to_string())?;
                case.trace.wait(Event::EnterImage(frame.id)).await?;
                timeout(
                    WAIT,
                    changed_until(&case.measured.changed, || {
                        case.measured.prepared.load(Ordering::SeqCst)
                    }),
                )
                .await
                .map_err(|_| "queued terminal did not reach prepare_end after Resume")?;
                Ok::<_, String>((
                    premature_image,
                    normal_started,
                    terminal_started,
                    terminal_prepared,
                    worker_finished,
                ))
            }
            .await;
            // Release the intentional image hold, drain the already queued
            // terminal, and retain Stop's required explicit owner cleanup.
            let finished = case.finish_existing(terminal).await;
            finished.normal();
            assert_eq!(operations.unwrap(), (false, false, false, false, false));
            assert_eq!(finished.count(frame.id), 1);
            assert_eq!(finished.count(terminal_frame.id), 1);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn cancel_revokes_active_live_write_even_when_processor_normal_lane_is_paused() {
    for urgent in [false, true] {
        let case = Case::start(config(), live_config()).await.unwrap();
        let mut held = image(false);
        held.interruptible = false;
        case.backend.hold_image(held.id);
        let pause = Frame::new(if urgent {
            FrameData::ProcessorPauseUrgent {
                processor: NAME.into(),
            }
        } else {
            FrameData::ProcessorPause {
                processor: NAME.into(),
            }
        });
        let operations = async {
            case.admit(held.clone()).await?;
            case.trace.wait(Event::EnterImage(held.id)).await?;
            case.admit(pause.clone()).await?;
            case.trace.wait(Event::Forward(pause.id)).await
        }
        .await;
        let finished = case.finish(Terminal::Cancel).await;
        finished.normal();
        operations.unwrap();
        assert!(!finished.backend.written(held.id));
        assert_eq!(finished.count(held.id), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn cancel_revokes_live_write_gate_pts_and_fps_waits_without_survivors() {
    for mode in ["write", "gate", "pts", "fps"] {
        let mut video = live_config();
        if mode == "fps" {
            video.framerate = Some(1);
        }
        let case = Case::start(config(), video).await.unwrap();
        let held = image(false);
        let mut pending = image(mode == "gate");
        let operations = async {
            if mode == "write" || mode == "gate" {
                case.backend.hold_image(held.id);
                case.admit(held.clone()).await?;
                case.trace.wait(Event::EnterImage(held.id)).await?;
            }
            if mode == "fps" {
                case.admit(held.clone()).await?;
                case.trace.wait(Event::Image(held.id)).await?;
            }
            if mode == "pts" {
                pending.set_pipeline_pts(case.clock.now_ns() + 1_000_000_000);
            }
            case.admit(pending.clone()).await?;
            let entered = if mode == "write" {
                false
            } else {
                case.trace
                    .within(Event::EnterImage(pending.id), WINDOW)
                    .await
            };
            Ok::<_, String>(entered)
        }
        .await;
        let finished = case.finish(Terminal::Cancel).await;
        finished.normal();
        assert!(!operations.unwrap(), "{mode}: wait not established");
        assert_eq!(finished.count(pending.id), 0);
        if mode == "write" || mode == "gate" {
            assert_eq!(finished.count(held.id), 0);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn dropping_pipeline_worker_cancels_owned_image_future_and_runs_cleanup() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let held = image(false);
    case.backend.hold_image(held.id);
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await
    }
    .await;
    let backend = case.backend.clone();
    let trace = case.trace.clone();
    let control = case.control.clone();
    drop(case.worker);
    let cleanup = timeout(
        WAIT,
        changed_until(&trace.changed, || backend.stops.load(Ordering::SeqCst) == 1),
    )
    .await;
    operations.unwrap();
    assert!(cleanup.is_ok());
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert!(!backend.written(held.id));
    assert!(!control.is_ready());
}

#[tokio::test(start_paused = true)]
async fn all_destination_all_lane_fence_waits_for_pcm_or_image_and_releases_successors_once() {
    for hold_image in [false, true] {
        let mut cfg = config();
        cfg.destinations = vec!["other".into()];
        let case = Case::start(cfg, live_config()).await.unwrap();
        let mut prior = image(false);
        prior.transport_destination = Some("other".into());
        let gate = if hold_image {
            case.backend.hold_image(prior.id)
        } else {
            case.backend.hold_audio(Some("other"))
        };
        let marker = fence(false);
        let receipt = flush_event(&marker);
        let later = image(false);
        let operations = async {
            case.admit(prior.clone()).await?;
            case.admit(pcm(80, Some("other"))).await?;
            if hold_image {
                case.trace.wait(Event::EnterImage(prior.id)).await?;
            } else {
                case.trace
                    .wait(Event::EnterAudio(Some("other".into())))
                    .await?;
            }
            case.admit(marker.clone()).await?;
            case.admit(later.clone()).await?;
            case.admit(pcm(80, None)).await?;
            let early_marker = case.trace.within(Event::Forward(marker.id), WINDOW).await;
            let early_live = case.trace.contains(&Event::EnterImage(later.id));
            let early_media = case.trace.contains(&Event::EnterAudio(None));
            let early_receipt = receipt.is_set();
            gate.add_permits(8);
            case.trace.wait(Event::Forward(marker.id)).await?;
            case.trace.wait(Event::Image(later.id)).await?;
            case.trace.wait(Event::Audio(None)).await?;
            wait_fence_receipt(&receipt).await?;
            Ok::<_, String>((early_marker, early_live, early_media, early_receipt))
        }
        .await;
        let trace = case.trace.clone();
        let finished = case.finish(Terminal::End).await;
        finished.normal();
        assert_eq!(operations.unwrap(), (false, false, false, false));
        assert!(receipt.is_set());
        assert_eq!(finished.count(marker.id), 1);
        assert_eq!(finished.count(prior.id), 1);
        assert_eq!(finished.count(later.id), 1);
        let events = trace.events.lock().unwrap();
        let at = |event| events.iter().position(|e| *e == event).unwrap();
        assert!(at(Event::Image(prior.id)) < at(Event::Forward(marker.id)));
        assert!(at(Event::Audio(Some("other".into()))) < at(Event::Forward(marker.id)));
        assert!(at(Event::Forward(marker.id)) < at(Event::Image(later.id)));
        assert!(at(Event::Forward(marker.id)) < at(Event::Audio(None)));
    }
}

#[tokio::test(start_paused = true)]
async fn full_live_lane_rejects_fence_atomically_without_orphaning_media() {
    let mut video = live_config();
    video.queue_capacity = 1;
    let case = Case::start(config(), video).await.unwrap();
    let held = image(false);
    let queued = image(false);
    case.backend.hold_image(held.id);
    let marker = fence(false);
    let event = flush_event(&marker);
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await?;
        case.admit(queued.clone()).await?;
        // Capture the public transport's fatal admission error directly so
        // core's intentional fatal-error shutdown cannot mask queue atomicity.
        let context = case
            .measured
            .context
            .lock()
            .unwrap()
            .clone()
            .ok_or("missing context")?;
        let rejected = case
            .measured
            .output
            .process(marker.clone(), Direction::Downstream, &context)
            .await;
        case.admit(pcm(80, None)).await?;
        case.trace.wait(Event::Audio(None)).await?;
        Ok::<_, String>(rejected)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    assert!(operations.unwrap().unwrap_err().fatal);
    assert!(!event.is_set());
    assert_eq!(finished.count(marker.id), 0);
    assert_eq!(finished.count(queued.id), 1);
    assert_eq!(finished.backend.audio.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn fence_epoch_revocation_and_uninterruptible_survival_keep_both_lanes_usable() {
    for tail_flush in [false, true] {
        for named in [false, true] {
            for interruptible in [false, true] {
                let mut cfg = config();
                cfg.destinations = vec!["other".into()];
                let case = Case::start(cfg, live_config()).await.unwrap();
                let destination = if named { Some("other") } else { None };
                let mut held = image(false);
                held.transport_destination = destination.map(str::to_owned);
                if tail_flush {
                    case.backend.hold_audio(destination);
                } else {
                    case.backend.hold_image(held.id);
                }
                let marker = fence(interruptible);
                let event = flush_event(&marker);
                let mut successor = image(false);
                successor.transport_destination = destination.map(str::to_owned);
                let operations = async {
                    if tail_flush {
                        case.admit(pcm(40, destination)).await?;
                    } else {
                        case.admit(held.clone()).await?;
                        case.trace.wait(Event::EnterImage(held.id)).await?;
                    }
                    case.admit(marker.clone()).await?;
                    // Partial PCM enters play_audio only because the real media
                    // worker popped the fence and began its padded-tail flush.
                    if tail_flush {
                        case.trace
                            .wait(Event::EnterAudio(destination.map(str::to_owned)))
                            .await?;
                    }
                    let early_marker = case.trace.within(Event::Forward(marker.id), WINDOW).await;
                    case.interrupt(destination).await?;
                    case.backend.release_all();
                    case.admit(successor.clone()).await?;
                    case.trace.wait(Event::Image(successor.id)).await?;
                    case.admit(pcm(80, destination)).await?;
                    case.trace
                        .wait(Event::Audio(destination.map(str::to_owned)))
                        .await?;
                    let mut sync = image(true);
                    sync.transport_destination = destination.map(str::to_owned);
                    let sync_id = sync.id;
                    case.admit(sync).await?;
                    case.trace.wait(Event::Image(sync_id)).await?;
                    if !interruptible {
                        wait_fence_receipt(&event).await?;
                    }
                    Ok::<_, String>((early_marker, sync_id))
                }
                .await;
                let interruptions_before_cleanup = case.backend.interrupts.lock().unwrap().clone();
                let finished = case.finish(Terminal::End).await;
                finished.normal();
                assert_eq!(
                    finished.backend.interrupts.lock().unwrap().len(),
                    interruptions_before_cleanup.len() + 2,
                    "cleanup must interrupt each configured destination once"
                );
                let (early, sync_id) = operations.unwrap();
                assert!(!early);
                assert_eq!(event.is_set(), !interruptible);
                assert_eq!(finished.count(marker.id), usize::from(!interruptible));
                assert_eq!(finished.count(successor.id), 1);
                assert_eq!(finished.count(sync_id), 1);
                assert_eq!(
                    interruptions_before_cleanup.len(),
                    if named { 1 } else { 2 }
                );
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn paused_live_fence_drains_on_end_and_cancel_never_publishes_success() {
    for cancel in [false, true] {
        let mut cfg = config();
        cfg.destinations = vec!["paused".into()];
        let case = Case::start(cfg, live_config()).await.unwrap();
        case.control.pause(Some("paused")).unwrap();
        let mut prior = image(false);
        prior.transport_destination = Some("paused".into());
        let marker = fence(false);
        let event = flush_event(&marker);
        let operations = async {
            case.admit(prior.clone()).await?;
            case.admit(marker.clone()).await?;
            Ok::<_, String>(case.trace.within(Event::Forward(marker.id), WINDOW).await)
        }
        .await;
        let finished = case
            .finish(if cancel {
                Terminal::Cancel
            } else {
                Terminal::End
            })
            .await;
        finished.normal();
        assert!(!operations.unwrap());
        assert_eq!(event.is_set(), !cancel);
        assert_eq!(finished.count(marker.id), usize::from(!cancel));
        assert_eq!(finished.count(prior.id), usize::from(!cancel));
    }
}

#[tokio::test(start_paused = true)]
async fn genuine_live_image_failure_makes_later_fence_fatal_without_success() {
    let mut case = Case::start(config(), live_config()).await.unwrap();
    let failed = image(false);
    let marker = fence(false);
    let event = flush_event(&marker);
    case.backend.failures.lock().unwrap().insert(failed.id);
    let operations = async {
        case.admit(failed.clone()).await?;
        case.trace.wait(Event::DropImage(failed.id)).await?;
        case.admit(marker.clone()).await?;
        timeout(WAIT, async {
            while !case.worker.has_finished() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| "fatal fence deadline")?;
        Ok::<_, String>(())
    }
    .await;
    if operations.is_err() {
        let _ = case.worker.cancel("fatal fence test cleanup").await;
    }
    let joined = bounded("fatal fence join", case.worker.join()).await;
    let errors = case.worker.errors();
    let mut frames = vec![];
    while let Ok(frame) = case.worker.try_recv() {
        frames.push(frame);
    }
    assert_eq!(case.backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(case.backend.stops.load(Ordering::SeqCst), 1);
    operations.unwrap();
    assert!(joined.is_err());
    assert!(
        errors.iter().any(|e| e.fatal && e.error.contains("fence")),
        "{errors:?}"
    );
    assert!(!event.is_set());
    assert!(!frames
        .iter()
        .any(|f| f.frame.id == marker.id || f.frame.id == failed.id));
    assert!(!case.backend.written(failed.id));
}

#[tokio::test(start_paused = true)]
async fn dropping_worker_aborts_a_pending_all_lane_fence_without_receipt() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let held = image(false);
    case.backend.hold_image(held.id);
    let marker = fence(false);
    let event = flush_event(&marker);
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await?;
        case.admit(marker.clone()).await?;
        Ok::<_, String>(case.trace.within(Event::Forward(marker.id), WINDOW).await)
    }
    .await;
    let backend = case.backend.clone();
    let trace = case.trace.clone();
    drop(case.worker);
    let cleanup = timeout(
        WAIT,
        changed_until(&trace.changed, || backend.stops.load(Ordering::SeqCst) == 1),
    )
    .await;
    assert!(cleanup.is_ok());
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert!(!operations.unwrap());
    assert!(!event.is_set());
    assert!(!trace.contains(&Event::Forward(marker.id)));
}

#[tokio::test(start_paused = true)]
async fn pipeline_pts_uses_shared_origins_rebases_both_image_lanes_and_preserves_envelopes() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let mut live = image(false);
    let mut sync = image(true);
    let pts = case.clock.now_ns() + 300_000_000;
    live.set_pipeline_pts(pts);
    sync.set_pipeline_pts(pts);
    live.metadata
        .insert("keep".into(), serde_json::json!({"nested": [1, 2]}));
    let operations = async {
        case.admit(live.clone()).await?;
        case.admit(sync.clone()).await?;
        let early = case
            .trace
            .within(Event::EnterImage(live.id), Duration::from_millis(250))
            .await;
        case.trace.wait(Event::Image(live.id)).await?;
        case.trace.wait(Event::Image(sync.id)).await?;
        Ok::<_, String>(early)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert!(!operations.unwrap());
    let images = finished.backend.images.lock().unwrap();
    for expected in [&live, &sync] {
        let actual = images.iter().find(|f| f.id == expected.id).unwrap();
        assert_eq!(actual.pts, Some(300_000_000));
        assert!(!actual.has_pipeline_pts());
        assert_eq!(actual.metadata, expected.metadata);
        assert_eq!(actual.data, expected.data);
        assert_eq!(finished.count(expected.id), 1);
        let forwarded = &finished
            .frames
            .iter()
            .find(|f| f.frame.id == expected.id)
            .unwrap()
            .frame;
        assert_eq!(forwarded, actual);
        assert!(!forwarded.has_pipeline_pts());
    }
    let calls = finished.backend.calls.lock().unwrap();
    assert_eq!(
        calls[0].1, calls[1].1,
        "the two lanes created distinct startup origins"
    );
}

#[tokio::test(start_paused = true)]
async fn overdue_pipeline_pts_rebases_to_zero_and_legacy_pts_remains_relative() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let mut overdue = image(false);
    overdue.set_pipeline_pts(0);
    let mut legacy = image(false);
    legacy.pts = Some(200_000_000);
    let operations = async {
        case.admit(overdue.clone()).await?;
        case.trace.wait(Event::Image(overdue.id)).await?;
        case.admit(legacy.clone()).await?;
        let early = case
            .trace
            .within(Event::Image(legacy.id), Duration::from_millis(100))
            .await;
        case.trace.wait(Event::Image(legacy.id)).await?;
        Ok::<_, String>(early)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert!(!operations.unwrap());
    let images = finished.backend.images.lock().unwrap();
    assert_eq!(images[0].pts, Some(0));
    assert!(!images[0].has_pipeline_pts());
    assert_eq!(images[1], legacy);
}

#[tokio::test(start_paused = true)]
async fn future_live_head_preserves_fifo_even_when_later_image_is_immediate() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let mut future = image(false);
    future.set_pipeline_pts(case.clock.now_ns() + 300_000_000);
    let now = image(false);
    let operations = async {
        case.admit(future.clone()).await?;
        case.admit(now.clone()).await?;
        let early = case
            .trace
            .within(Event::Image(now.id), Duration::from_millis(250))
            .await;
        case.trace.wait(Event::Image(now.id)).await?;
        Ok::<_, String>(early)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert!(!operations.unwrap());
    let images = finished.backend.images.lock().unwrap();
    assert_eq!(
        images.iter().map(|f| f.id).collect::<Vec<_>>(),
        vec![future.id, now.id]
    );
}

#[tokio::test(start_paused = true)]
async fn pipeline_pts_horizon_and_overflow_are_rejected_before_live_admission() {
    let mut cfg = config();
    cfg.max_future_pts = Duration::from_millis(100);
    let case = Case::start(cfg, live_config()).await.unwrap();
    let mut horizon = image(false);
    horizon.set_pipeline_pts(case.clock.now_ns() + 200_000_000);
    let mut extreme = image(false);
    extreme.set_pipeline_pts(u64::MAX);
    let operations = async {
        let mut rejected = vec![];
        for frame in [&horizon, &extreme] {
            case.send(frame.clone()).await?;
            rejected.push(case.receipt(frame.id).await?.is_err());
        }
        Ok::<_, String>(rejected)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    assert_eq!(operations.unwrap(), vec![true, true]);
    assert!(finished.backend.images.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn unpaced_live_images_are_immediate_without_repeating_still_images() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let first = image(false);
    let second = image(false);
    let operations = async {
        let began = Instant::now();
        case.admit(first.clone()).await?;
        case.trace.wait(Event::Image(first.id)).await?;
        case.admit(second.clone()).await?;
        case.trace.wait(Event::Image(second.id)).await?;
        let elapsed = began.elapsed();
        tokio::time::sleep(Duration::from_millis(250)).await;
        Ok::<_, String>(elapsed)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(operations.unwrap(), Duration::ZERO);
    assert_eq!(finished.backend.images.lock().unwrap().len(), 2);
    assert_eq!(finished.backend.calls.lock().unwrap().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn fps_cap_first_is_immediate_uses_ceil_period_and_never_catches_up_after_held_write() {
    let mut video = live_config();
    video.framerate = Some(3);
    let case = Case::start(config(), video).await.unwrap();
    let first = image(false);
    let second = image(false);
    let third = image(false);
    let gate = case.backend.hold_image(first.id);
    let operations = async {
        let began = Instant::now();
        case.admit(first.clone()).await?;
        case.trace.wait(Event::EnterImage(first.id)).await?;
        let first_delay = began.elapsed();
        case.admit(second.clone()).await?;
        case.admit(third.clone()).await?;
        tokio::time::sleep(Duration::from_millis(800)).await;
        gate.add_permits(1);
        case.trace.wait(Event::Image(second.id)).await?;
        let catchup = case
            .trace
            .within(Event::EnterImage(third.id), Duration::from_millis(300))
            .await;
        case.trace.wait(Event::Image(third.id)).await?;
        Ok::<_, String>((first_delay, catchup))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(operations.unwrap(), (Duration::ZERO, false));
    let calls = finished.backend.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(calls[1].1.duration_since(calls[0].1) >= Duration::from_millis(800));
    assert!(calls[2].1.duration_since(calls[1].1) >= Duration::from_nanos(333_333_334));
}

#[tokio::test(start_paused = true)]
async fn pts_and_fps_use_the_later_deadline_and_do_not_consume_write_timeout() {
    let mut cfg = config();
    cfg.write_timeout = Duration::from_millis(50);
    let mut video = live_config();
    video.framerate = Some(2);
    let case = Case::start(cfg, video).await.unwrap();
    let first = image(false);
    let mut second = image(false);
    second.set_pipeline_pts(case.clock.now_ns() + 800_000_000);
    let third = image(false);
    let operations = async {
        case.admit(first.clone()).await?;
        case.trace.wait(Event::Image(first.id)).await?;
        case.admit(second.clone()).await?;
        case.admit(third.clone()).await?;
        case.trace.wait(Event::Image(third.id)).await
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    operations.unwrap();
    let calls = finished.backend.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(calls[1].1.duration_since(calls[0].1) >= Duration::from_millis(800));
    assert!(calls[2].1.duration_since(calls[1].1) >= Duration::from_millis(500));
}

#[tokio::test(start_paused = true)]
async fn interruption_resets_live_pacing_and_true_images_do_not_consume_live_slots() {
    let mut video = live_config();
    video.framerate = Some(1);
    let case = Case::start(config(), video).await.unwrap();
    let first = image(false);
    let sync = image(true);
    let fresh = image(false);
    let operations = async {
        case.admit(first.clone()).await?;
        case.trace.wait(Event::Image(first.id)).await?;
        let began = Instant::now();
        case.admit(sync.clone()).await?;
        case.trace.wait(Event::Image(sync.id)).await?;
        let sync_delay = began.elapsed();
        tokio::time::sleep(WINDOW).await;
        case.interrupt(None).await?;
        let began = Instant::now();
        case.admit(fresh.clone()).await?;
        case.trace.wait(Event::Image(fresh.id)).await?;
        Ok::<_, String>((sync_delay, began.elapsed()))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(operations.unwrap(), (Duration::ZERO, Duration::ZERO));
    assert_eq!(finished.count(first.id), 1);
    assert_eq!(finished.count(sync.id), 1);
    assert_eq!(finished.count(fresh.id), 1);
}

#[tokio::test(start_paused = true)]
async fn pause_resume_after_long_idle_starts_new_live_period_without_catchup() {
    let mut video = live_config();
    video.framerate = Some(10);
    let case = Case::start(config(), video).await.unwrap();
    let first = image(false);
    let second = image(false);
    let third = image(false);
    let operations = async {
        case.admit(first.clone()).await?;
        case.trace.wait(Event::Image(first.id)).await?;
        case.control.pause(None).map_err(|e| e.to_string())?;
        case.admit(second.clone()).await?;
        case.admit(third.clone()).await?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let early = case.trace.contains(&Event::EnterImage(second.id));
        case.control.resume(None).map_err(|e| e.to_string())?;
        case.trace.wait(Event::Image(second.id)).await?;
        let burst = case
            .trace
            .within(Event::Image(third.id), Duration::from_millis(75))
            .await;
        case.trace.wait(Event::Image(third.id)).await?;
        Ok::<_, String>((early, burst))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(operations.unwrap(), (false, false));
    let calls = finished.backend.calls.lock().unwrap();
    assert!(calls[2].1.duration_since(calls[1].1) >= Duration::from_millis(100));
}

#[tokio::test(start_paused = true)]
async fn start_failure_never_announces_ready_or_starts_image_workers() {
    let trace = Arc::new(Trace::default());
    let backend = Arc::new(Backend::new(trace, true));
    backend.fail_start.store(true, Ordering::SeqCst);
    let output = Arc::new(
        make_output(config(), backend.clone())
            .with_live_video(live_config())
            .unwrap(),
    );
    let control = output.control();
    let result = bounded(
        "failed start",
        Pipeline::new(vec![output.clone()]).start(PipelineConfig::default()),
    )
    .await;
    let cleanup = bounded("failed start owner cleanup", output.cleanup()).await;
    assert!(result.is_err());
    cleanup.unwrap();
    assert!(!control.is_ready());
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert!(backend.images.lock().unwrap().is_empty());
    assert_eq!(backend.starts.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn duplicate_public_start_is_rejected_without_spawning_additional_workers() {
    let case = Case::start(config(), live_config()).await.unwrap();
    let context = case.measured.context.lock().unwrap().clone().unwrap();
    let duplicate = case
        .measured
        .output
        .process(
            Frame::new(FrameData::Start(StartFrame::default())),
            Direction::Downstream,
            &context,
        )
        .await;
    let first = image(false);
    let second = image(false);
    case.backend.hold_image(first.id);
    let operations = async {
        case.admit(first.clone()).await?;
        case.trace.wait(Event::EnterImage(first.id)).await?;
        case.admit(second.clone()).await?;
        Ok::<_, String>(
            case.trace
                .within(Event::EnterImage(second.id), WINDOW)
                .await,
        )
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert!(duplicate.is_err());
    assert!(!operations.unwrap());
    assert_eq!(finished.count(first.id), 1);
    assert_eq!(finished.count(second.id), 1);
}

#[tokio::test(start_paused = true)]
async fn image_flag_selects_its_own_lane_byte_budget_in_both_directions() {
    let mut cfg = config();
    cfg.queue_byte_capacity = 1024;
    let case = Case::start(cfg, live_config()).await.unwrap();
    let mut live = image(false);
    let FrameData::OutputImageDetailed(data) = &mut live.data else {
        unreachable!()
    };
    data.image.image = vec![7; 2048];
    let mut sync = live.duplicate();
    let FrameData::OutputImageDetailed(data) = &mut sync.data else {
        unreachable!()
    };
    data.sync_with_audio = true;
    let operations = async {
        case.admit(live.clone()).await?;
        case.trace.wait(Event::Image(live.id)).await?;
        case.send(sync.clone()).await?;
        case.receipt(sync.id).await
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    assert!(operations.unwrap().is_err());
    assert_eq!(finished.count(live.id), 1);
    assert_eq!(finished.count(sync.id), 0);
}

#[tokio::test(start_paused = true)]
async fn live_wait_interruption_revokes_only_interruptible_pending_admission() {
    for interruptible in [false, true] {
        let mut cfg = config();
        cfg.admission_policy = OutputAdmissionPolicy::Wait {
            timeout: Duration::from_secs(1),
        };
        let mut video = live_config();
        video.queue_capacity = 1;
        let case = Case::start(cfg, video).await.unwrap();
        let mut held = image(false);
        held.interruptible = false;
        let mut queued = image(false);
        queued.interruptible = false;
        let mut waiting = image(false);
        waiting.interruptible = interruptible;
        let gate = case.backend.hold_image(held.id);
        let operations = async {
            case.admit(held.clone()).await?;
            case.trace.wait(Event::EnterImage(held.id)).await?;
            case.admit(queued.clone()).await?;
            case.send(waiting.clone()).await?;
            case.begun(waiting.id).await?;
            // The core can revoke an interruptible invocation itself; the
            // wrapper need not receive a completed result for that cancellation.
            case.interrupt(None).await?;
            let premature = case.trace.within(Event::Image(waiting.id), WINDOW).await;
            gate.add_permits(1);
            if !interruptible {
                case.receipt(waiting.id).await?.map_err(|e| e.to_string())?;
                case.trace.wait(Event::Image(waiting.id)).await?;
            }
            Ok::<_, String>(premature)
        }
        .await;
        let finished = case.finish(Terminal::End).await;
        finished.normal();
        assert!(!operations.unwrap());
        assert_eq!(finished.count(held.id), 1);
        assert_eq!(finished.count(queued.id), 1);
        assert_eq!(finished.count(waiting.id), usize::from(!interruptible));
    }
}

#[tokio::test(start_paused = true)]
async fn new_epoch_uninterruptible_gate_attempt_sets_the_next_live_pacing_slot() {
    let mut video = live_config();
    video.framerate = Some(10);
    let case = Case::start(config(), video).await.unwrap();
    let mut held_sync = image(true);
    held_sync.interruptible = false;
    let mut waiting = image(false);
    waiting.interruptible = false;
    let next = image(false);
    let gate = case.backend.hold_image(held_sync.id);
    let operations = async {
        case.admit(held_sync.clone()).await?;
        case.trace.wait(Event::EnterImage(held_sync.id)).await?;
        case.admit(waiting.clone()).await?;
        let before = case
            .trace
            .within(Event::EnterImage(waiting.id), WINDOW)
            .await;
        // Epoch publication is independent of the media-owned interrupt hook.
        // The uninterruptible synchronous write must release before that
        // worker can reach its hook.
        case.admit(Frame::interruption()).await?;
        gate.add_permits(1);
        case.trace.wait(Event::Image(waiting.id)).await?;
        case.admit(next.clone()).await?;
        let burst = case
            .trace
            .within(Event::EnterImage(next.id), Duration::from_millis(75))
            .await;
        case.trace.wait(Event::Image(next.id)).await?;
        Ok::<_, String>((before, burst))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(operations.unwrap(), (false, false));
    let calls = finished.backend.calls.lock().unwrap();
    let at = |id| calls.iter().find(|call| call.0 == id).unwrap().1;
    assert!(at(next.id).duration_since(at(waiting.id)) >= Duration::from_millis(100));
}

#[tokio::test(start_paused = true)]
async fn end_drains_when_empty_media_or_empty_video_lane_finishes_first() {
    for live_held in [false, true] {
        let case = Case::start(config(), live_config()).await.unwrap();
        let held = image(false);
        if live_held {
            case.backend.hold_image(held.id);
        } else {
            case.backend.hold_audio(None);
        }
        let operations = async {
            if live_held {
                case.admit(held.clone()).await?;
                case.trace.wait(Event::EnterImage(held.id)).await?;
            } else {
                case.admit(pcm(80, None)).await?;
                case.trace.wait(Event::EnterAudio(None)).await?;
            }
            bounded("End", case.worker.end()).await?;
            timeout(
                WAIT,
                changed_until(&case.measured.changed, || {
                    case.measured.prepared.load(Ordering::SeqCst)
                }),
            )
            .await
            .map_err(|_| "prepare deadline")?;
            tokio::time::sleep(WINDOW).await;
            Ok::<_, String>(case.backend.active.load(Ordering::SeqCst))
        }
        .await;
        let finished = case.finish_existing(Terminal::End).await;
        finished.normal();
        assert_eq!(
            operations.unwrap(),
            1,
            "a sibling lane ending cancelled held delivery"
        );
        if live_held {
            assert_eq!(finished.count(held.id), 1);
        } else {
            assert_eq!(finished.backend.audio.lock().unwrap().len(), 1);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn locally_owned_urgent_send_survives_prepare_end_and_real_cancel_revokes_it() {
    for cancel in [false, true] {
        let case = Case::start(config(), live_config()).await.unwrap();
        let held = image(false);
        case.backend.hold_image(held.id);
        let urgent = Frame::new(FrameData::OutputTransportMessageUrgent(
            serde_json::json!({"live-video-control": true}),
        ));
        let gate = Arc::new(Semaphore::new(0));
        case.backend
            .message_gates
            .lock()
            .unwrap()
            .insert(urgent.id, gate.clone());
        let output = case.measured.output.clone();
        let context = case.measured.context.lock().unwrap().clone().unwrap();
        let mut sending = Box::pin(output.process(urgent.clone(), Direction::Downstream, &context));
        let operations = async {
            case.admit(held.clone()).await?; case.trace.wait(Event::EnterImage(held.id)).await?;
            tokio::select! {
                result = &mut sending => return Err(format!("urgent finished before hold: {result:?}")),
                entered = case.trace.wait(Event::EnterMessage(urgent.id)) => entered?,
            }
            if cancel {
                // The test owns this future. A core actor abort cannot satisfy
                // cancellation: OutputTransport's own signal must resolve it.
                bounded("real output Cancel", output.process(Frame::cancel("local urgent"), Direction::Downstream, &context)).await?;
            } else {
                bounded("prepare_end", output.prepare_end(&context)).await?;
                tokio::time::sleep(WINDOW).await;
                gate.add_permits(1);
            }
            bounded("local urgent completion", &mut sending).await?;
            Ok::<_, String>(case.trace.contains(&Event::Message(urgent.id)))
        }.await;
        drop(sending);
        gate.add_permits(1);
        let finished = if cancel {
            case.finish_existing(Terminal::Cancel).await
        } else {
            case.finish(Terminal::End).await
        };
        finished.normal();
        assert_eq!(operations.unwrap(), !cancel);
        assert_eq!(finished.count(held.id), usize::from(!cancel));
    }
}

#[tokio::test(start_paused = true)]
async fn full_live_wait_fence_admits_all_lanes_together_after_capacity_returns() {
    let mut cfg = config();
    cfg.admission_policy = OutputAdmissionPolicy::Wait {
        timeout: Duration::from_secs(1),
    };
    let mut video = live_config();
    video.queue_capacity = 1;
    let case = Case::start(cfg, video).await.unwrap();
    let held = image(false);
    let queued = image(false);
    let marker = fence(false);
    let receipt = flush_event(&marker);
    let audio = pcm(80, None);
    let gate = case.backend.hold_image(held.id);
    let operations = async {
        case.admit(held.clone()).await?;
        case.trace.wait(Event::EnterImage(held.id)).await?;
        case.admit(queued.clone()).await?;
        case.send(marker.clone()).await?;
        case.begun(marker.id).await?;
        case.send(audio.clone()).await?;
        let premature = case.trace.within(Event::Audio(None), WINDOW).await;
        let fence_pending = !case
            .measured
            .receipts
            .lock()
            .unwrap()
            .contains_key(&marker.id);
        gate.add_permits(1);
        case.receipt(marker.id).await?.map_err(|e| e.to_string())?;
        case.receipt(audio.id).await?.map_err(|e| e.to_string())?;
        wait_fence_receipt(&receipt).await?;
        case.trace.wait(Event::Audio(None)).await?;
        Ok::<_, String>((premature, fence_pending))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(operations.unwrap(), (false, true));
    assert_eq!(finished.count(marker.id), 1);
    assert_eq!(finished.count(queued.id), 1);
}

#[tokio::test(start_paused = true)]
async fn failed_live_backend_attempt_still_consumes_its_fps_slot() {
    let mut video = live_config();
    video.framerate = Some(10);
    let case = Case::start(config(), video).await.unwrap();
    let failed = image(false);
    let next = image(false);
    case.backend.failures.lock().unwrap().insert(failed.id);
    let operations = async {
        case.admit(failed.clone()).await?;
        case.trace.wait(Event::DropImage(failed.id)).await?;
        case.admit(next.clone()).await?;
        let early = case
            .trace
            .within(Event::EnterImage(next.id), Duration::from_millis(75))
            .await;
        case.trace.wait(Event::Image(next.id)).await?;
        case.admit(pcm(80, None)).await?;
        case.trace.wait(Event::Audio(None)).await?;
        Ok::<_, String>(early)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    assert!(!operations.unwrap());
    assert_eq!(finished.count(failed.id), 0);
    assert_eq!(finished.count(next.id), 1);
    let calls = finished.backend.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert!(calls[1].1.duration_since(calls[0].1) >= Duration::from_millis(100));
}

#[tokio::test(start_paused = true)]
async fn partial_worker_registration_failure_never_publishes_ready_or_leaves_owned_work() {
    let trace = Arc::new(Trace::default());
    let backend = Arc::new(Backend::new(trace, true));
    let output = Arc::new(
        make_output(config(), backend.clone())
            .with_live_video(live_config())
            .unwrap(),
    );
    let control = output.control();
    let started = bounded(
        "limited task start",
        Pipeline::new(vec![output.clone()]).start(PipelineConfig {
            max_background_tasks: 1,
            shutdown_timeout: WAIT,
            ..Default::default()
        }),
    )
    .await;
    let mut frames = vec![];
    let failure = match started {
        Err(_) => true,
        Ok(mut worker) => {
            let result = bounded("registration failure join", worker.join()).await;
            if result.is_err() && !worker.has_finished() {
                let _ = worker.cancel("registration test cleanup").await;
                let _ = bounded("registration fallback join", worker.join()).await;
            }
            while let Ok(frame) = worker.try_recv() {
                frames.push(frame);
            }
            result.is_err()
        }
    };
    let cleanup = bounded("registration owner cleanup", output.cleanup()).await;
    cleanup.unwrap();
    assert!(failure);
    assert!(!control.is_ready());
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(backend.stops.load(Ordering::SeqCst), 1);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::OutputTransportReady)));
}

#[tokio::test(start_paused = true)]
async fn unusable_media_cannot_attest_fence_even_when_live_image_succeeded() {
    let mut case = Case::start(config(), live_config()).await.unwrap();
    case.backend.fail_audio.store(true, Ordering::SeqCst);
    let live = image(false);
    let marker = fence(false);
    let event = flush_event(&marker);
    let operations = async {
        case.admit(pcm(80, None)).await?;
        case.trace.wait(Event::EnterAudio(None)).await?;
        case.admit(live.clone()).await?;
        case.trace.wait(Event::Image(live.id)).await?;
        case.admit(marker.clone()).await?;
        timeout(WAIT, async {
            while !case.worker.has_finished() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .map_err(|_| "unusable fence deadline")?;
        Ok::<_, String>(())
    }
    .await;
    if operations.is_err() {
        let _ = case.worker.cancel("unusable fence cleanup").await;
    }
    let joined = bounded("unusable fence join", case.worker.join()).await;
    let errors = case.worker.errors();
    let mut frames = vec![];
    while let Ok(frame) = case.worker.try_recv() {
        frames.push(frame);
    }
    assert_eq!(case.backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(case.backend.stops.load(Ordering::SeqCst), 1);
    operations.unwrap();
    assert!(joined.is_err());
    assert!(
        errors.iter().any(|e| e.fatal && e.error.contains("fence")),
        "{errors:?}"
    );
    assert!(case.backend.written(live.id));
    assert!(!event.is_set());
    assert!(!frames.iter().any(|f| f.frame.id == marker.id));
}
