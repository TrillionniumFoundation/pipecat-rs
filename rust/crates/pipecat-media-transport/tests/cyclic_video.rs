//! Real-pipeline witnesses for the opt-in non-live image cycle. Tiny RGB backend
//! receipts attest only completion of this synthetic callback, never a physical
//! display or independently played PCM. Paused time tests ordering and cadence.
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
const PERIOD: Duration = Duration::from_millis(100);
const WINDOW: Duration = Duration::from_millis(25);
const NAME: &str = "cyclic-video-output";
type TestResult<T> = Result<T, String>;

async fn bounded<T, E: std::fmt::Display>(
    label: &str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    timeout(WAIT, future)
        .await
        .map_err(|_| format!("{label}: deadline"))?
        .map_err(|e| format!("{label}: {e}"))
}
async fn until(notify: &Notify, condition: impl Fn() -> bool) {
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
async fn observed(label: &str, notify: &Notify, condition: impl Fn() -> bool) -> TestResult<()> {
    timeout(WAIT, until(notify, condition))
        .await
        .map_err(|_| format!("{label}: deadline"))
}

#[derive(Clone, Debug)]
struct Draw {
    frame: Frame,
    color: u8,
    at: Instant,
    completed: Option<Instant>,
    dropped: bool,
}
#[derive(Default)]
struct Trace {
    draws: Mutex<Vec<Draw>>,
    forwarded: Mutex<Vec<Frame>>,
    changed: Notify,
}
impl Trace {
    fn calls(&self, color: u8) -> usize {
        self.draws
            .lock()
            .unwrap()
            .iter()
            .filter(|d| d.color == color)
            .count()
    }
    fn completed(&self, color: u8) -> usize {
        self.draws
            .lock()
            .unwrap()
            .iter()
            .filter(|d| d.color == color && d.completed.is_some())
            .count()
    }
    async fn wait_calls(&self, color: u8, count: usize) -> TestResult<()> {
        observed("image calls", &self.changed, || self.calls(color) >= count).await
    }
    async fn wait_completed(&self, color: u8, count: usize) -> TestResult<()> {
        observed("image completions", &self.changed, || {
            self.completed(color) >= count
        })
        .await
    }
    fn forward_count(&self, id: u64) -> usize {
        self.forwarded
            .lock()
            .unwrap()
            .iter()
            .filter(|f| {
                f.id == id && !matches!(&f.data, FrameData::FlushDetailed(flush) if flush.returning)
            })
            .count()
    }
}
struct Backend {
    trace: Arc<Trace>,
    supported: bool,
    image_gates: Mutex<BTreeMap<(u8, usize), Arc<Semaphore>>>,
    audio_gate: Mutex<Option<Arc<Semaphore>>>,
    failures: Mutex<BTreeSet<u8>>,
    audio_entered: AtomicUsize,
    audio_completed: AtomicUsize,
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
            image_gates: Mutex::new(BTreeMap::new()),
            audio_gate: Mutex::new(None),
            failures: Mutex::new(BTreeSet::new()),
            audio_entered: AtomicUsize::new(0),
            audio_completed: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            image_active: Mutex::new(BTreeMap::new()),
            image_peak: Mutex::new(BTreeMap::new()),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        }
    }
    fn hold_image(&self, color: u8, occurrence: usize) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        self.image_gates
            .lock()
            .unwrap()
            .insert((color, occurrence), gate.clone());
        gate
    }
    fn hold_audio(&self) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.audio_gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    fn release_all(&self) {
        for gate in self.image_gates.lock().unwrap().values() {
            gate.add_permits(64);
        }
        if let Some(gate) = self.audio_gate.lock().unwrap().as_ref() {
            gate.add_permits(64);
        }
    }
}
struct Active<'a> {
    backend: &'a Backend,
    image: Option<(usize, Option<String>)>,
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.backend.active.fetch_sub(1, Ordering::SeqCst);
        if let Some((index, destination)) = &self.image {
            self.backend.trace.draws.lock().unwrap()[*index].dropped = true;
            *self
                .backend
                .image_active
                .lock()
                .unwrap()
                .get_mut(destination)
                .unwrap() -= 1;
        }
        self.backend.trace.changed.notify_waiters();
    }
}
fn color(frame: &Frame) -> u8 {
    match &frame.data {
        FrameData::OutputImage(image) => image.image[0],
        FrameData::OutputImageDetailed(image) => image.image.image[0],
        other => panic!("backend received non-raw image: {other:?}"),
    }
}
#[async_trait]
impl OutputBackend for Backend {
    fn supports_independent_video(&self) -> bool {
        self.supported
    }
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active {
            backend: self,
            image: None,
        };
        self.audio_entered.fetch_add(1, Ordering::SeqCst);
        self.trace.changed.notify_waiters();
        let gate = self.audio_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .map_err(|e| ProcessorError::new(e.to_string()))?
                .forget();
        }
        self.audio_completed.fetch_add(1, Ordering::SeqCst);
        self.trace.changed.notify_waiters();
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
            let peak = peak.entry(destination.clone()).or_default();
            *peak = (*peak).max(*count);
        }
        let color = color(frame);
        let (index, occurrence) = {
            let mut draws = self.trace.draws.lock().unwrap();
            let occurrence = draws.iter().filter(|d| d.color == color).count() + 1;
            let index = draws.len();
            draws.push(Draw {
                frame: frame.clone(),
                color,
                at: Instant::now(),
                completed: None,
                dropped: false,
            });
            (index, occurrence)
        };
        let _active = Active {
            backend: self,
            image: Some((index, destination)),
        };
        self.trace.changed.notify_waiters();
        let gate = self
            .image_gates
            .lock()
            .unwrap()
            .get(&(color, occurrence))
            .cloned();
        if let Some(gate) = gate {
            gate.acquire()
                .await
                .map_err(|e| ProcessorError::new(e.to_string()))?
                .forget();
        }
        if self.failures.lock().unwrap().contains(&color) {
            return Err(ProcessorError::new("intentional cyclic-image failure"));
        }
        self.trace.draws.lock().unwrap()[index].completed = Some(Instant::now());
        self.trace.changed.notify_waiters();
        Ok(())
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        self.trace.changed.notify_waiters();
        Ok(())
    }
}

// All inputs go through the real Pipeline. The adapter only observes admission;
// named expected validation errors are captured instead of terminating the
// pipeline, allowing a rejected replacement's old active cycle to be inspected.
struct Measured {
    output: Arc<OutputTransport>,
    receipts: Mutex<BTreeMap<u64, Result<(), ProcessorError>>>,
    expected_rejections: Mutex<BTreeSet<u64>>,
    ready: AtomicBool,
    prepared: AtomicBool,
    changed: Notify,
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
        let result = self.output.process(frame, direction, ctx).await;
        self.receipts.lock().unwrap().insert(id, result.clone());
        if start && result.is_ok() {
            self.ready.store(true, Ordering::SeqCst);
        }
        self.changed.notify_waiters();
        if self.expected_rejections.lock().unwrap().contains(&id) {
            Ok(())
        } else {
            result
        }
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
        "cyclic-video-recorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.0.forwarded.lock().unwrap().push(frame.clone());
            self.0.changed.notify_waiters();
        }
        ctx.push(frame, direction).await
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
fn cyclic_config() -> CyclicVideoOutputConfig {
    CyclicVideoOutputConfig {
        queue_capacity: 16,
        queue_byte_capacity: 64 * 1024,
        retained_image_capacity: 64,
        retained_byte_capacity: 256 * 1024,
        framerate: 10,
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
}
impl Case {
    async fn start(cfg: OutputTransportConfig, video: CyclicVideoOutputConfig) -> TestResult<Self> {
        Self::start_paused(cfg, video, false).await
    }
    async fn start_paused(
        cfg: OutputTransportConfig,
        video: CyclicVideoOutputConfig,
        paused: bool,
    ) -> TestResult<Self> {
        let trace = Arc::new(Trace::default());
        let backend = Arc::new(Backend::new(trace.clone(), true));
        let output = make_output(cfg, backend.clone());
        let control = output.control();
        if paused {
            control.pause(None).map_err(|e| e.to_string())?;
        }
        let output = Arc::new(output.with_cyclic_video(video).map_err(|e| e.to_string())?);
        let measured = Arc::new(Measured {
            output,
            receipts: Mutex::new(BTreeMap::new()),
            expected_rejections: Mutex::new(BTreeSet::new()),
            ready: AtomicBool::new(false),
            prepared: AtomicBool::new(false),
            changed: Notify::new(),
        });
        let worker = bounded(
            "pipeline start",
            Pipeline::new(vec![measured.clone(), Arc::new(Recorder(trace.clone()))]).start(
                PipelineConfig {
                    shutdown_timeout: WAIT,
                    ..Default::default()
                },
            ),
        )
        .await?;
        let case = Self {
            worker,
            measured,
            backend,
            trace,
            control,
        };
        let ready = observed("output ready", &case.measured.changed, || {
            case.measured.ready.load(Ordering::SeqCst)
        })
        .await;
        if let Err(problem) = ready {
            let _ = case.finish(Terminal::Cancel).await;
            return Err(problem);
        }
        Ok(case)
    }
    async fn send(&self, frame: Frame) -> TestResult<()> {
        bounded(
            "pipeline ingress",
            self.worker.sender().send(frame, Direction::Downstream),
        )
        .await
    }
    async fn receipt(&self, id: u64) -> TestResult<Result<(), ProcessorError>> {
        observed("output admission", &self.measured.changed, || {
            self.measured.receipts.lock().unwrap().contains_key(&id)
        })
        .await?;
        Ok(self.measured.receipts.lock().unwrap()[&id].clone())
    }
    async fn admit(&self, frame: Frame) -> TestResult<()> {
        let id = frame.id;
        self.send(frame).await?;
        self.receipt(id).await?.map_err(|e| e.to_string())
    }
    async fn reject(&self, frame: Frame) -> TestResult<bool> {
        let id = frame.id;
        self.measured.expected_rejections.lock().unwrap().insert(id);
        self.send(frame).await?;
        Ok(self.receipt(id).await?.is_err())
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
                    bounded("Cancel", self.worker.cancel("cyclic-video cleanup")).await
                }
            }
        };
        if sent.is_err() {
            let _ = bounded("fallback cancel", self.worker.cancel("terminal failed")).await;
        }
        let joined = bounded("worker join", self.worker.join()).await;
        if joined.is_err() && !self.worker.has_finished() {
            let _ = bounded("fallback cancel", self.worker.cancel("join failed")).await;
            let _ = bounded("fallback join", self.worker.join()).await;
        }
        let stopped_before_cleanup = self.backend.stops.load(Ordering::SeqCst);
        let cleanup = bounded("owner cleanup", self.measured.output.cleanup()).await;
        Finished {
            backend: self.backend,
            trace: self.trace,
            control: self.control,
            sent,
            joined,
            cleanup,
            terminal,
            stopped_before_cleanup,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Terminal {
    End,
    Stop,
    Cancel,
}
struct Finished {
    backend: Arc<Backend>,
    trace: Arc<Trace>,
    control: OutputControl,
    sent: TestResult<()>,
    joined: TestResult<WorkerReport>,
    cleanup: TestResult<()>,
    terminal: Terminal,
    stopped_before_cleanup: usize,
}
impl Finished {
    fn clean(&self) {
        assert_eq!(
            self.backend.active.load(Ordering::SeqCst),
            0,
            "owned backend callback survived cleanup"
        );
        assert!(
            self.trace.draws.lock().unwrap().iter().all(|d| d.dropped),
            "image callback destructor missing"
        );
        assert_eq!(self.backend.starts.load(Ordering::SeqCst), 1);
        assert_eq!(self.backend.stops.load(Ordering::SeqCst), 1);
        assert!(!self.control.is_ready());
        assert!(self.sent.is_ok(), "{:?}", self.sent);
        assert!(self.cleanup.is_ok(), "{:?}", self.cleanup);
        assert!(
            self.backend
                .image_peak
                .lock()
                .unwrap()
                .values()
                .all(|p| *p <= 1),
            "overlapping image callbacks for one destination"
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
}
fn raw(value: u8) -> ImageRawFrame {
    ImageRawFrame {
        image: vec![value, 0, 0],
        size: (1, 1),
        format: Some("RGB".into()),
    }
}
fn image(value: u8, sync: bool) -> Frame {
    Frame::new(FrameData::OutputImageDetailed(OutputImageRawFrame {
        image: raw(value),
        sync_with_audio: sync,
    }))
}
fn sprite(values: &[u8]) -> Frame {
    Frame::new(FrameData::Sprite(SpriteFrame {
        images: values
            .iter()
            .enumerate()
            .map(|(i, value)| OutputImageRawFrame {
                image: raw(*value),
                sync_with_audio: i % 2 == 0,
            })
            .collect(),
    }))
}
fn pcm() -> Frame {
    Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(encode_pcm16_le(&[500; 80]), 8_000, 1).unwrap(),
    ))
}
fn fence() -> (Frame, LocalFlushEvent) {
    let event = LocalFlushEvent::default();
    let mut frame = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        origin: Some("pipecat.flows".into()),
        returning: false,
        event: Some(event.clone()),
    }));
    frame.interruptible = false;
    frame
        .metadata
        .insert("pipecat.flows.barrier".into(), 64.into());
    (frame, event)
}
async fn fence_done(event: &LocalFlushEvent) -> TestResult<()> {
    timeout(WAIT, event.wait())
        .await
        .map_err(|_| "finite flush deadline".into())
}

#[tokio::test(start_paused = true)]
async fn sprite_cycles_and_replacement_singleton_repeats_without_reforwarding() {
    let case = Case::start(config(), cyclic_config()).await.unwrap();
    let ab = sprite(&[1, 2]);
    let c = Frame::new(FrameData::OutputImage(raw(3)));
    let observation = async {
        case.admit(ab.clone()).await?;
        case.trace.wait_completed(2, 2).await?;
        case.admit(c.clone()).await?;
        case.trace.wait_completed(3, 3).await
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    observation.unwrap();
    assert_eq!(finished.trace.forward_count(ab.id), 1);
    assert_eq!(finished.trace.forward_count(c.id), 1);
    let draws = finished.trace.draws.lock().unwrap();
    assert_eq!(
        draws[..4].iter().map(|d| d.color).collect::<Vec<_>>(),
        [1, 2, 1, 2]
    );
    let replacement = draws.iter().position(|d| d.color == 3).unwrap();
    assert!(draws[replacement..].iter().all(|d| d.color == 3));
    for pair in draws[..4].windows(2).chain(draws[replacement..].windows(2)) {
        assert!(
            pair[1].at.duration_since(pair[0].completed.unwrap()) >= PERIOD,
            "cycle caught up instead of delaying after completion"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn replacement_cancels_held_old_repeat_and_late_release_cannot_revive_it() {
    let case = Case::start(config(), cyclic_config()).await.unwrap();
    let old = image(1, false);
    let next = image(2, false);
    let gate = case.backend.hold_image(1, 2);
    let observation = async {
        case.admit(old.clone()).await?;
        case.trace.wait_calls(1, 2).await?;
        case.admit(next.clone()).await?;
        case.trace.wait_completed(2, 1).await?;
        let cancelled = case
            .trace
            .draws
            .lock()
            .unwrap()
            .iter()
            .filter(|d| d.color == 1)
            .nth(1)
            .unwrap()
            .dropped;
        gate.add_permits(1);
        case.trace.wait_completed(2, 3).await?;
        Ok::<_, String>(cancelled)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert!(observation.unwrap(), "replacement left an old repeat alive");
    assert_eq!(finished.trace.calls(1), 2);
    assert_eq!(finished.trace.completed(1), 1);
    assert_eq!(finished.trace.forward_count(old.id), 1);
    assert_eq!(finished.trace.forward_count(next.id), 1);
}

#[tokio::test(start_paused = true)]
async fn sync_activation_waits_for_pcm_receipt_but_false_image_crosses_held_pcm() {
    for sync in [false, true] {
        let case = Case::start(config(), cyclic_config()).await.unwrap();
        let gate = case.backend.hold_audio();
        let picture = image(1, sync);
        let observation = async {
            case.admit(pcm()).await?;
            observed("PCM entered", &case.trace.changed, || {
                case.backend.audio_entered.load(Ordering::SeqCst) > 0
            })
            .await?;
            case.admit(picture.clone()).await?;
            let early = timeout(
                WINDOW,
                until(&case.trace.changed, || case.trace.completed(1) > 0),
            )
            .await
            .is_ok();
            let audio_early = case.backend.audio_completed.load(Ordering::SeqCst);
            gate.add_permits(8);
            case.trace.wait_completed(1, 2).await?;
            Ok::<_, String>((early, audio_early))
        }
        .await;
        let finished = case.finish(Terminal::End).await;
        finished.normal();
        assert_eq!(observation.unwrap(), (!sync, 0));
        assert_eq!(finished.trace.forward_count(picture.id), 1);
        assert!(finished.backend.audio_completed.load(Ordering::SeqCst) > 0);
    }
}

#[tokio::test(start_paused = true)]
async fn pause_before_builder_and_long_pause_resume_do_not_start_callbacks_or_burst() {
    let case = Case::start_paused(config(), cyclic_config(), true)
        .await
        .unwrap();
    let observation = async {
        case.admit(sprite(&[1, 2])).await?;
        tokio::time::sleep(PERIOD * 4).await;
        let initial = case.trace.draws.lock().unwrap().len();
        case.control.resume(None).map_err(|e| e.to_string())?;
        case.trace.wait_completed(2, 1).await?;
        case.control.pause(None).map_err(|e| e.to_string())?;
        let before = case.trace.draws.lock().unwrap().len();
        tokio::time::sleep(PERIOD * 20).await;
        let paused = case.trace.draws.lock().unwrap().len();
        let resumed_at = Instant::now();
        case.control.resume(None).map_err(|e| e.to_string())?;
        case.trace.wait_completed(2, 3).await?;
        Ok::<_, String>((initial, before, paused, resumed_at))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    let (initial, before, paused, resumed_at) = observation.unwrap();
    assert_eq!(initial, 0);
    assert_eq!(before, paused);
    let draws = finished.trace.draws.lock().unwrap();
    let resumed = draws
        .iter()
        .filter(|d| d.at >= resumed_at)
        .collect::<Vec<_>>();
    assert!(resumed.len() >= 3);
    assert!(resumed
        .windows(2)
        .all(|p| p[1].at.duration_since(p[0].completed.unwrap()) >= PERIOD));
}

#[tokio::test(start_paused = true)]
async fn finite_flush_waits_for_first_draw_and_sync_before_flush_does_not_deadlock() {
    for sync in [false, true] {
        let case = Case::start(config(), cyclic_config()).await.unwrap();
        let picture = image(1, sync);
        let gate = case.backend.hold_image(1, 1);
        let (marker, event) = fence();
        let observation = async {
            case.admit(picture.clone()).await?;
            case.trace.wait_calls(1, 1).await?;
            case.admit(marker.clone()).await?;
            // A draw slower than two periods must not accumulate repeat debt.
            tokio::time::sleep(PERIOD * 2 + WINDOW).await;
            let early = event.is_set();
            let early_forward = case.trace.forward_count(marker.id);
            gate.add_permits(1);
            fence_done(&event).await?;
            case.trace.wait_completed(1, 3).await?;
            Ok::<_, String>((early, early_forward))
        }
        .await;
        let finished = case.finish(Terminal::End).await;
        finished.normal();
        assert_eq!(observation.unwrap(), (false, 0));
        assert!(event.is_set());
        assert_eq!(finished.trace.forward_count(marker.id), 1);
        assert_eq!(finished.trace.forward_count(picture.id), 1);
        let draws = finished.trace.draws.lock().unwrap();
        assert!(draws[0].completed.unwrap().duration_since(draws[0].at) > PERIOD * 2);
        for pair in draws.windows(2) {
            assert!(
                pair[1].at.duration_since(pair[0].completed.unwrap()) >= PERIOD,
                "slow draw completion triggered a catch-up repeat"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn end_stop_cancel_release_every_owned_callback_and_stop_repetition() {
    for terminal in [Terminal::End, Terminal::Stop, Terminal::Cancel] {
        let case = Case::start(config(), cyclic_config()).await.unwrap();
        case.backend.hold_image(1, 2);
        let observation = async {
            case.admit(image(1, false)).await?;
            case.trace.wait_calls(1, 2).await
        }
        .await;
        let finished = case.finish(terminal).await;
        finished.normal();
        observation.unwrap();
        if terminal == Terminal::Stop {
            assert_eq!(finished.stopped_before_cleanup, 0);
        }
        let count = finished.trace.calls(1);
        tokio::time::sleep(PERIOD * 5).await;
        assert_eq!(finished.trace.calls(1), count);
        if terminal == Terminal::Cancel {
            assert_eq!(finished.trace.completed(1), 1);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn dropping_pipeline_worker_cancels_held_draw_and_aborts_fence() {
    let case = Case::start(config(), cyclic_config()).await.unwrap();
    case.backend.hold_image(1, 1);
    case.backend.hold_audio();
    let (marker, event) = fence();
    let observation = async {
        case.admit(pcm()).await?;
        observed("PCM entered before drop", &case.trace.changed, || {
            case.backend.audio_entered.load(Ordering::SeqCst) > 0
        })
        .await?;
        case.admit(image(1, false)).await?;
        case.trace.wait_calls(1, 1).await?;
        case.admit(marker).await
    }
    .await;
    let backend = case.backend.clone();
    let trace = case.trace.clone();
    let control = case.control.clone();
    drop(case.worker);
    let cleanup = observed("drop cleanup", &trace.changed, || {
        backend.stops.load(Ordering::SeqCst) == 1
    })
    .await;
    // Explicit fallback cleanup also runs when an observation deadline fails.
    let owner_cleanup = bounded("drop owner cleanup", case.measured.output.cleanup()).await;
    observation.unwrap();
    cleanup.unwrap();
    owner_cleanup.unwrap();
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert!(!control.is_ready());
    assert!(trace
        .draws
        .lock()
        .unwrap()
        .iter()
        .all(|d| d.dropped && d.completed.is_none()));
    assert!(!event.is_set());
}

#[tokio::test(start_paused = true)]
async fn destinations_are_independent_but_runtime_interruption_retires_original_scopes() {
    let mut cfg = config();
    cfg.destinations = vec!["other".into()];
    let case = Case::start(cfg, cyclic_config()).await.unwrap();
    let default = sprite(&[1, 2]);
    let mut other = sprite(&[3, 4]);
    other.transport_destination = Some("other".into());
    let fresh_default = image(5, false);
    let mut fresh_other = image(6, true);
    fresh_other.transport_destination = Some("other".into());
    let observation = async {
        case.admit(default.clone()).await?;
        case.admit(other.clone()).await?;
        case.trace.wait_completed(2, 2).await?;
        case.trace.wait_completed(4, 2).await?;
        case.control
            .pause(Some("other"))
            .map_err(|e| e.to_string())?;
        let other_count = case.trace.calls(3) + case.trace.calls(4);
        let default_count = case.trace.calls(1);
        case.trace.wait_calls(1, default_count + 2).await?;
        let paused_other = case.trace.calls(3) + case.trace.calls(4);
        case.control
            .resume(Some("other"))
            .map_err(|e| e.to_string())?;
        let named_before_resume = case.trace.calls(3);
        case.trace.wait_calls(3, named_before_resume + 1).await?;
        let mut interruption = Frame::interruption();
        interruption.transport_destination = Some("other".into());
        case.admit(interruption).await?;
        // Native core Interruption invalidates the original invocation globally.
        // Targeted transport queue signaling does not authorize a fresh scope
        // for the other destination's old retained frames.
        let retired = case.trace.draws.lock().unwrap().len();
        tokio::time::sleep(PERIOD * 3).await;
        let after_retirement = case.trace.draws.lock().unwrap().len();
        case.admit(fresh_default.clone()).await?;
        case.admit(fresh_other.clone()).await?;
        case.trace.wait_completed(5, 2).await?;
        case.trace.wait_completed(6, 2).await?;
        case.admit(Frame::interruption()).await?;
        let all = case.trace.draws.lock().unwrap().len();
        tokio::time::sleep(PERIOD * 3).await;
        Ok::<_, String>((
            other_count,
            paused_other,
            retired,
            after_retirement,
            all,
            case.trace.draws.lock().unwrap().len(),
        ))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    let (before, paused, retired, after_retirement, all, later) = observation.unwrap();
    assert_eq!(before, paused);
    assert_eq!(retired, after_retirement);
    assert_eq!(all, later);
    assert_eq!(finished.trace.forward_count(default.id), 1);
    assert_eq!(finished.trace.forward_count(other.id), 1);
    assert_eq!(finished.trace.forward_count(fresh_default.id), 1);
    assert_eq!(finished.trace.forward_count(fresh_other.id), 1);
    for draw in finished.trace.draws.lock().unwrap().iter() {
        assert_eq!(
            draw.frame.transport_destination.as_deref(),
            if [3, 4, 6].contains(&draw.color) {
                Some("other")
            } else {
                None
            }
        );
    }
}

#[test]
fn builders_reject_invalid_bounds_missing_capability_and_both_live_cyclic_orders() {
    let backend = Arc::new(Backend::new(Arc::new(Trace::default()), false));
    assert!(make_output(config(), backend.clone())
        .with_cyclic_video(cyclic_config())
        .is_err());
    assert_eq!(backend.starts.load(Ordering::SeqCst), 0);
    let backend = Arc::new(Backend::new(Arc::new(Trace::default()), true));
    for bad in [
        CyclicVideoOutputConfig {
            queue_capacity: 0,
            ..cyclic_config()
        },
        CyclicVideoOutputConfig {
            queue_byte_capacity: 0,
            ..cyclic_config()
        },
        CyclicVideoOutputConfig {
            retained_image_capacity: 0,
            ..cyclic_config()
        },
        CyclicVideoOutputConfig {
            retained_byte_capacity: 0,
            ..cyclic_config()
        },
        CyclicVideoOutputConfig {
            framerate: 0,
            ..cyclic_config()
        },
        CyclicVideoOutputConfig {
            framerate: 1_000_000_001,
            ..cyclic_config()
        },
        CyclicVideoOutputConfig {
            queue_capacity: usize::MAX,
            ..cyclic_config()
        },
        CyclicVideoOutputConfig {
            queue_byte_capacity: usize::MAX,
            ..cyclic_config()
        },
        CyclicVideoOutputConfig {
            retained_image_capacity: usize::MAX,
            ..cyclic_config()
        },
        CyclicVideoOutputConfig {
            retained_byte_capacity: usize::MAX,
            ..cyclic_config()
        },
    ] {
        assert!(
            make_output(config(), backend.clone())
                .with_cyclic_video(bad)
                .is_err(),
            "accepted {bad:?}"
        );
    }
    let live = LiveVideoOutputConfig {
        queue_capacity: 16,
        queue_byte_capacity: 64 * 1024,
        framerate: None,
    };
    assert!(make_output(config(), backend.clone())
        .with_live_video(live)
        .unwrap()
        .with_cyclic_video(cyclic_config())
        .is_err());
    assert!(make_output(config(), backend.clone())
        .with_cyclic_video(cyclic_config())
        .unwrap()
        .with_live_video(live)
        .is_err());
    assert!(make_output(config(), backend.clone())
        .with_cyclic_video(cyclic_config())
        .unwrap()
        .with_cyclic_video(cyclic_config())
        .is_err());
    assert_eq!(backend.starts.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn empty_sprite_metadata_pts_and_native_authority_cannot_replace_valid_cycle() {
    let case = Case::start(config(), cyclic_config()).await.unwrap();
    let valid = image(1, false);
    let mut metadata = image(2, false);
    metadata
        .metadata
        .insert("pipecat.cyclic.authorized".into(), true.into());
    let mut pts = image(3, false);
    pts.pts = Some(0);
    let mut pipeline_pts = image(4, false);
    pipeline_pts.set_pipeline_pts(0);
    let mut authority = Frame::new(FrameData::LlmResponseStart);
    authority
        .set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::start(
                NativeLlmContinuation::next_response_id().unwrap(),
                LlmContinuationProvider::Anthropic,
            )
            .unwrap(),
        ))
        .unwrap();
    // An attached capability remains native even after a public payload change.
    authority.data = image(5, false).data;
    let url = Frame::new(FrameData::UrlImage(UrlImageRawFrame {
        image: OutputImageRawFrame {
            image: raw(6),
            sync_with_audio: false,
        },
        url: Some("https://images.example/cycle".into()),
    }));
    let assistant = Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
        image: OutputImageRawFrame {
            image: raw(7),
            sync_with_audio: false,
        },
        original_data: Some(vec![1, 2, 3]),
        original_mime_type: Some("image/png".into()),
    }));
    let invalid = vec![
        sprite(&[]),
        metadata,
        pts,
        pipeline_pts,
        authority,
        url,
        assistant,
    ];
    let invalid_ids = invalid.iter().map(|f| f.id).collect::<Vec<_>>();
    let observation = async {
        case.admit(valid.clone()).await?;
        case.trace.wait_completed(1, 1).await?;
        let mut rejected = Vec::new();
        for frame in invalid {
            rejected.push(case.reject(frame).await?);
        }
        let count = case.trace.completed(1);
        case.trace.wait_completed(1, count + 2).await?;
        Ok::<_, String>(rejected)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert!(observation.unwrap().iter().all(|rejected| *rejected));
    assert!(finished
        .trace
        .draws
        .lock()
        .unwrap()
        .iter()
        .all(|d| d.color == 1));
    assert!(invalid_ids
        .iter()
        .all(|id| finished.trace.forward_count(*id) == 0));
    assert_eq!(finished.trace.forward_count(valid.id), 1);
}

#[tokio::test(start_paused = true)]
async fn retained_image_and_byte_budgets_include_active_plus_replacement_and_keep_old_cycle() {
    for byte_bound in [false, true] {
        let mut video = cyclic_config();
        if byte_bound {
            video.retained_byte_capacity = 32 * 1024;
        } else {
            video.retained_image_capacity = 4;
        }
        let case = Case::start(config(), video).await.unwrap();
        let oversized = if byte_bound {
            let mut value = raw(2);
            value.image.reserve_exact(24 * 1024);
            Frame::new(FrameData::OutputImage(value))
        } else {
            sprite(&[2, 3])
        };
        let oversized_id = oversized.id;
        let observation = async {
            case.admit(image(1, false)).await?;
            case.trace.wait_completed(1, 1).await?;
            case.admit(oversized).await?;
            observed("retained budget rejection", &case.trace.changed, || {
                case.worker.total_errors() > 0
            })
            .await?;
            let count = case.trace.completed(1);
            case.trace.wait_completed(1, count + 2).await?;
            // Replacing with an affordable image proves the rejected snapshot
            // released its charge and did not clear the prior active state.
            case.admit(image(4, false)).await?;
            case.trace.wait_completed(4, 2).await
        }
        .await;
        let finished = case.finish(Terminal::End).await;
        finished.clean();
        observation.unwrap();
        assert!(finished
            .joined
            .as_ref()
            .unwrap()
            .errors
            .iter()
            .any(|e| e.error.contains("budget")));
        assert_eq!(finished.trace.calls(2), 0);
        assert_eq!(finished.trace.calls(3), 0);
        assert_eq!(finished.trace.forward_count(oversized_id), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn queue_byte_budget_counts_sprite_pixel_and_spare_vector_allocations() {
    let case = Case::start(config(), cyclic_config()).await.unwrap();
    let mut pixels = raw(2);
    pixels.image.reserve_exact(128 * 1024);
    let pixels = Frame::new(FrameData::Sprite(SpriteFrame {
        images: vec![OutputImageRawFrame {
            image: pixels,
            sync_with_audio: false,
        }],
    }));
    let mut images = Vec::with_capacity(4_096);
    images.push(OutputImageRawFrame {
        image: raw(3),
        sync_with_audio: false,
    });
    let spare = Frame::new(FrameData::Sprite(SpriteFrame { images }));
    let observation = async {
        case.admit(image(1, false)).await?;
        case.trace.wait_completed(1, 1).await?;
        let pixels_rejected = case.reject(pixels).await?;
        let spare_rejected = case.reject(spare).await?;
        let count = case.trace.completed(1);
        case.trace.wait_completed(1, count + 2).await?;
        Ok::<_, String>((pixels_rejected, spare_rejected))
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.normal();
    assert_eq!(observation.unwrap(), (true, true));
    assert_eq!(finished.trace.calls(2), 0);
    assert_eq!(finished.trace.calls(3), 0);
}

#[tokio::test(start_paused = true)]
async fn backend_failure_or_timeout_never_turns_queued_activation_into_successful_flush() {
    for timeout_case in [false, true] {
        for sync in [false, true] {
            let mut cfg = config();
            cfg.write_timeout = Duration::from_millis(200);
            let mut case = Case::start(cfg, cyclic_config()).await.unwrap();
            if timeout_case {
                case.backend.hold_image(1, 1);
            } else {
                case.backend.failures.lock().unwrap().insert(1);
            }
            let picture = image(1, sync);
            let (marker, event) = fence();
            let observation = async {
                case.admit(picture.clone()).await?;
                case.trace.wait_calls(1, 1).await?;
                observed("failed draw destructed", &case.trace.changed, || {
                    case.trace.draws.lock().unwrap()[0].dropped
                })
                .await?;
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
            if observation.is_err() {
                let _ = bounded(
                    "failed-case cancel",
                    case.worker.cancel("fence failure cleanup"),
                )
                .await;
            }
            let joined = bounded("failed-case join", case.worker.join()).await;
            let cleanup =
                bounded("failed-case owner cleanup", case.measured.output.cleanup()).await;
            observation.unwrap();
            cleanup.unwrap();
            assert!(joined.is_err());
            assert!(!event.is_set());
            assert_eq!(case.trace.forward_count(marker.id), 0);
            assert_eq!(case.trace.forward_count(picture.id), 0);
            assert_eq!(case.trace.completed(1), 0);
            assert_eq!(case.backend.active.load(Ordering::SeqCst), 0);
            assert_eq!(case.backend.stops.load(Ordering::SeqCst), 1);
            assert!(case
                .worker
                .errors()
                .iter()
                .any(|e| e.fatal && e.error.contains("fence")));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn paused_sync_first_draw_and_fence_drain_on_end_and_stop_without_reclosing_gate() {
    for terminal in [Terminal::End, Terminal::Stop] {
        let case = Case::start_paused(config(), cyclic_config(), true)
            .await
            .unwrap();
        let gate = case.backend.hold_image(1, 1);
        let picture = image(1, true);
        let (marker, event) = fence();
        let observation = async {
            case.admit(picture.clone()).await?;
            case.admit(marker.clone()).await?;
            match terminal {
                Terminal::End => bounded("queued End", case.worker.end()).await?,
                Terminal::Stop => case.send(Frame::new(FrameData::Stop)).await?,
                Terminal::Cancel => unreachable!(),
            }
            case.trace.wait_calls(1, 1).await?;
            case.control.pause(None).map_err(|e| e.to_string())?;
            tokio::time::sleep(WINDOW).await;
            let early = event.is_set();
            gate.add_permits(1);
            fence_done(&event).await?;
            Ok::<_, String>(early)
        }
        .await;
        // A terminal is already queued. On any observation failure, explicitly
        // cancel rather than waiting for the held callback test to time out.
        if observation.is_err() {
            let _ = bounded(
                "drain fallback cancel",
                case.worker.cancel("drain observation failed"),
            )
            .await;
        }
        let finished = case.finish_existing(terminal).await;
        finished.normal();
        assert!(!observation.unwrap());
        assert_eq!(finished.trace.completed(1), 1);
        assert_eq!(finished.trace.forward_count(picture.id), 1);
        assert_eq!(finished.trace.forward_count(marker.id), 1);
        assert!(event.is_set());
    }
}

#[tokio::test(start_paused = true)]
async fn cancel_drops_pcm_first_draw_and_queued_sync_activation_without_fence_success() {
    let case = Case::start(config(), cyclic_config()).await.unwrap();
    case.backend.hold_audio();
    case.backend.hold_image(1, 1);
    let sync = image(2, true);
    let (marker, event) = fence();
    let observation = async {
        case.admit(pcm()).await?;
        observed("PCM entered before cancel", &case.trace.changed, || {
            case.backend.audio_entered.load(Ordering::SeqCst) > 0
        })
        .await?;
        case.admit(image(1, false)).await?;
        case.trace.wait_calls(1, 1).await?;
        case.admit(sync.clone()).await?;
        case.admit(marker.clone()).await
    }
    .await;
    let finished = case.finish(Terminal::Cancel).await;
    finished.normal();
    observation.unwrap();
    assert_eq!(finished.backend.audio_completed.load(Ordering::SeqCst), 0);
    assert_eq!(finished.trace.completed(1), 0);
    assert_eq!(finished.trace.calls(2), 0);
    assert_eq!(finished.trace.forward_count(sync.id), 0);
    assert_eq!(finished.trace.forward_count(marker.id), 0);
    assert!(!event.is_set());
}

#[tokio::test(start_paused = true)]
async fn native_error_origin_cannot_be_laundered_into_a_cyclic_raw_image() {
    let mut case = Case::start(config(), cyclic_config()).await.unwrap();
    case.backend.failures.lock().unwrap().insert(9);
    let observation = async {
        case.admit(image(9, false)).await?;
        let mut carrier = timeout(WAIT, async {
            while let Some(directed) = case.worker.recv().await {
                if directed.frame.error_origin().is_some() {
                    return Ok(directed.frame);
                }
            }
            Err("pipeline closed before native error carrier".to_string())
        })
        .await
        .map_err(|_| "native error carrier deadline")??;
        carrier.data = image(2, false).data;
        carrier.metadata.clear();
        carrier.pts = None;
        case.admit(image(1, false)).await?;
        case.trace.wait_completed(1, 1).await?;
        let rejected = case.reject(carrier).await?;
        let count = case.trace.completed(1);
        case.trace.wait_completed(1, count + 2).await?;
        Ok::<_, String>(rejected)
    }
    .await;
    let finished = case.finish(Terminal::End).await;
    finished.clean();
    assert!(observation.unwrap());
    assert_eq!(finished.trace.calls(2), 0);
    assert!(finished
        .joined
        .as_ref()
        .unwrap()
        .errors
        .iter()
        .any(|e| e.error.contains("intentional")));
}

#[tokio::test(start_paused = true)]
async fn partial_worker_registration_failure_never_publishes_ready_or_starts_draws() {
    let trace = Arc::new(Trace::default());
    let backend = Arc::new(Backend::new(trace.clone(), true));
    let output = Arc::new(
        make_output(config(), backend.clone())
            .with_cyclic_video(cyclic_config())
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
    let mut frames = Vec::new();
    let failed = match started {
        Err(_) => true,
        Ok(mut worker) => {
            let result = bounded("registration failure join", worker.join()).await;
            if result.is_err() && !worker.has_finished() {
                let _ = bounded(
                    "registration fallback cancel",
                    worker.cancel("registration cleanup"),
                )
                .await;
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
    assert!(failed);
    assert!(!control.is_ready());
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(backend.stops.load(Ordering::SeqCst), 1);
    assert!(trace.draws.lock().unwrap().is_empty());
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::OutputTransportReady)));
}

#[tokio::test(start_paused = true)]
async fn interruption_retires_held_first_draw_without_old_ack_or_late_completion_revival() {
    for sync in [false, true] {
        let case = Case::start(config(), cyclic_config()).await.unwrap();
        let gate = case.backend.hold_image(1, 1);
        let mut old = image(1, sync);
        // Cyclic native scope retirement is strict even when a public frame
        // would ordinarily survive transport interruption as uninterruptible.
        old.interruptible = false;
        let fresh = image(2, sync);
        let (marker, event) = fence();
        let observation = async {
            case.admit(old.clone()).await?;
            case.trace.wait_calls(1, 1).await?;
            case.admit(Frame::interruption()).await?;
            observed(
                "interrupted first draw destructed",
                &case.trace.changed,
                || case.trace.draws.lock().unwrap()[0].dropped,
            )
            .await?;
            let old_forwarded = case.trace.forward_count(old.id);
            gate.add_permits(1);
            case.admit(fresh.clone()).await?;
            case.trace.wait_completed(2, 2).await?;
            case.admit(marker.clone()).await?;
            fence_done(&event).await?;
            Ok::<_, String>(old_forwarded)
        }
        .await;
        let finished = case.finish(Terminal::End).await;
        finished.normal();
        assert_eq!(observation.unwrap(), 0);
        assert_eq!(finished.trace.calls(1), 1);
        assert_eq!(finished.trace.completed(1), 0);
        assert_eq!(finished.trace.forward_count(old.id), 0);
        assert_eq!(finished.trace.forward_count(fresh.id), 1);
        assert_eq!(finished.trace.forward_count(marker.id), 1);
        assert!(event.is_set());
    }
}

#[tokio::test(start_paused = true)]
async fn repeat_failure_or_timeout_prevents_flush_success_after_a_valid_first_ack() {
    for timeout_case in [false, true] {
        for sync in [false, true] {
            let mut cfg = config();
            cfg.write_timeout = Duration::from_millis(200);
            let mut case = Case::start(cfg, cyclic_config()).await.unwrap();
            if timeout_case {
                case.backend.hold_image(1, 2);
            }
            let picture = image(1, sync);
            let (marker, event) = fence();
            let observation = async {
                case.admit(picture.clone()).await?;
                case.trace.wait_completed(1, 1).await?;
                observed("first activation acknowledged", &case.trace.changed, || {
                    case.trace.forward_count(picture.id) == 1
                })
                .await?;
                if !timeout_case {
                    case.backend.failures.lock().unwrap().insert(1);
                }
                case.trace.wait_calls(1, 2).await?;
                observed("failed repeat destructed", &case.trace.changed, || {
                    case.trace
                        .draws
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|d| d.color == 1)
                        .nth(1)
                        .is_some_and(|d| d.dropped)
                })
                .await?;
                case.admit(marker.clone()).await?;
                timeout(WAIT, async {
                    while !case.worker.has_finished() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .map_err(|_| "failed-repeat fence deadline")?;
                Ok::<_, String>(())
            }
            .await;
            if observation.is_err() {
                let _ = bounded(
                    "failed-repeat cancel",
                    case.worker.cancel("repeat failure cleanup"),
                )
                .await;
            }
            let joined = bounded("failed-repeat join", case.worker.join()).await;
            let cleanup = bounded(
                "failed-repeat owner cleanup",
                case.measured.output.cleanup(),
            )
            .await;
            observation.unwrap();
            cleanup.unwrap();
            assert!(joined.is_err());
            assert!(!event.is_set());
            assert_eq!(case.trace.forward_count(marker.id), 0);
            assert_eq!(case.trace.forward_count(picture.id), 1);
            assert_eq!(case.trace.calls(1), 2);
            assert_eq!(case.trace.completed(1), 1);
            assert!(case.trace.draws.lock().unwrap().iter().all(|d| d.dropped));
            assert_eq!(case.backend.active.load(Ordering::SeqCst), 0);
            assert_eq!(case.backend.stops.load(Ordering::SeqCst), 1);
            assert!(case
                .worker
                .errors()
                .iter()
                .any(|e| e.fatal && e.error.contains("fence")));
        }
    }
}
