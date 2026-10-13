//! Actual assistant-history adapter, PCM receipts, and opt-in live-image lanes.
//! Synthetic cancellation-safe backend; no physical-device playback is claimed.
//! This module is included only with the `assistant-history` feature.
#![cfg(feature = "assistant-history")]

use async_trait::async_trait;
use pipecat_audio::{encode_pcm16_le, PcmFormat};
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_services::{Content, Context};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator, SharedContext,
};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(3);
const OBSERVE: Duration = Duration::from_millis(100);
const NAME: &str = "live-video-history-output";
const IMAGE_ERROR: &str = "intentional live-image history write failure";
type TestResult<T> = Result<T, String>;

async fn bounded<T, E: std::fmt::Display>(
    label: &str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    timeout(WAIT, future)
        .await
        .map_err(|_| format!("{label}: test deadline"))?
        .map_err(|error| format!("{label}: {error}"))
}

async fn permit(label: &str, semaphore: &Semaphore) -> TestResult<()> {
    bounded(label, semaphore.acquire()).await?.forget();
    Ok(())
}

struct Active<'a>(&'a AtomicUsize);
impl<'a> Active<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Backend {
    fail_image: bool,
    image_entered: Semaphore,
    image_release: Semaphore,
    control_entered: Semaphore,
    control_release: Semaphore,
    attempts: Mutex<Vec<Frame>>,
    images: Mutex<Vec<Frame>>,
    played: Mutex<Vec<Frame>>,
    active: AtomicUsize,
    starts: AtomicUsize,
    stops: AtomicUsize,
}
impl Backend {
    fn new(fail_image: bool) -> Self {
        Self {
            fail_image,
            image_entered: Semaphore::new(0),
            image_release: Semaphore::new(0),
            control_entered: Semaphore::new(0),
            control_release: Semaphore::new(0),
            attempts: Mutex::new(vec![]),
            images: Mutex::new(vec![]),
            played: Mutex::new(vec![]),
            active: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        }
    }
    fn release(&self) {
        self.image_release.add_permits(8);
        self.control_release.add_permits(8);
    }
}
#[async_trait]
impl OutputBackend for Backend {
    fn supports_independent_video(&self) -> bool {
        true
    }
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        let _active = Active::new(&self.active);
        self.played.lock().unwrap().push(frame.clone());
        Ok(PlaybackReceipt::Played)
    }
    async fn write_image(&self, frame: &Frame) -> Result<(), ProcessorError> {
        let _active = Active::new(&self.active);
        self.attempts.lock().unwrap().push(frame.clone());
        self.image_entered.add_permits(1);
        self.image_release
            .acquire()
            .await
            .map_err(|error| ProcessorError::new(error.to_string()))?
            .forget();
        if self.fail_image {
            return Err(ProcessorError::new(IMAGE_ERROR));
        }
        self.images.lock().unwrap().push(frame.clone());
        Ok(())
    }
    async fn send_message(&self, _: &Frame) -> Result<(), ProcessorError> {
        let _active = Active::new(&self.active);
        self.control_entered.add_permits(1);
        self.control_release
            .acquire()
            .await
            .map_err(|error| ProcessorError::new(error.to_string()))?
            .forget();
        Ok(())
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// Record completed actual OutputTransport::process calls. Ingress send alone
// cannot prove admission to either owned output worker.
struct MeasuredOutput {
    output: OutputTransport,
    admissions: Mutex<BTreeMap<u64, Result<(), ProcessorError>>>,
    changed: Notify,
    ready: Semaphore,
}
impl MeasuredOutput {
    async fn admitted(&self, id: u64) -> TestResult<()> {
        bounded("completed output admission", async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let result = self.admissions.lock().unwrap().get(&id).cloned();
                if let Some(result) = result {
                    return result;
                }
                changed.await;
            }
        })
        .await
    }
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
        let start = frame.data.is_start();
        let result = self.output.process(frame, direction, ctx).await;
        if direction == Direction::Downstream {
            self.admissions.lock().unwrap().insert(id, result.clone());
            self.changed.notify_waiters();
            if start && result.is_ok() {
                self.ready.add_permits(1);
            }
        }
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.output.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.output.cleanup().await
    }
}

// The source boundary also invalidates history on Error. Hold that propagation
// so a missing live-worker invalidation cannot accidentally pass the test.
struct ErrorGate {
    history: SpokenHistory,
    entered: Semaphore,
    release: Semaphore,
    at_report: Mutex<Vec<SpokenHistoryStatus>>,
    active: AtomicUsize,
}
#[async_trait]
impl FrameProcessor for ErrorGate {
    fn name(&self) -> &str {
        "live-video-history-error-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Upstream && matches!(frame.data, FrameData::Error(_)) {
            let _active = Active::new(&self.active);
            self.at_report.lock().unwrap().push(self.history.status());
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|error| ProcessorError::new(error.to_string()))?
                .forget();
        }
        ctx.push(frame, direction).await
    }
}

// This sits immediately after the real transport. The assistant consumes source
// ends, so a probe after the assistant would never see their original identity.
#[derive(Default)]
struct Recorder {
    frames: Mutex<Vec<Frame>>,
    changed: Notify,
}
impl Recorder {
    async fn seen(&self, id: u64) -> TestResult<Frame> {
        self.seen_matching(|frame| frame.id == id).await
    }
    async fn seen_matching(&self, matches: impl Fn(&Frame) -> bool) -> TestResult<Frame> {
        bounded("original downstream frame", async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let frame = self
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|f| matches(f))
                    .cloned();
                if let Some(frame) = frame {
                    return Ok::<_, ProcessorError>(frame);
                }
                changed.await;
            }
        })
        .await
    }
}
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "live-video-history-recorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.frames.lock().unwrap().push(frame.clone());
            self.changed.notify_waiters();
        }
        ctx.push(frame, direction).await
    }
}

struct Fixture {
    worker: PipelineWorker,
    measured: Arc<MeasuredOutput>,
    recorder: Arc<Recorder>,
    error_gate: Arc<ErrorGate>,
    history: SpokenHistory,
    shared: SharedContext,
    control: OutputControl,
    backend: Arc<Backend>,
}
async fn fixture(fail_image: bool) -> Fixture {
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let history = SpokenHistory::new(
        SpokenHistoryConfig::default(),
        Arc::new(AssistantSpokenHistorySink::new(assistant.clone())),
    )
    .unwrap();
    let source = Arc::new(
        history
            .source_boundary("live-video-history-source")
            .unwrap(),
    );
    let backend = Arc::new(Backend::new(fail_image));
    let output = OutputTransport::new(
        NAME,
        OutputTransportConfig {
            sample_rate: Some(8000),
            channels: 1,
            chunk_duration: Duration::from_millis(10),
            queue_capacity: 32,
            queue_byte_capacity: 256 * 1024,
            auto_silence: false,
            end_silence: Duration::ZERO,
            write_timeout: Duration::from_secs(30),
            ..Default::default()
        },
        backend.clone(),
        None,
        BTreeMap::new(),
    )
    .unwrap()
    .with_spoken_history(history.clone())
    .unwrap()
    .with_live_video(LiveVideoOutputConfig {
        queue_capacity: 8,
        queue_byte_capacity: 64 * 1024,
        framerate: None,
    })
    .unwrap();
    let control = output.control();
    let measured = Arc::new(MeasuredOutput {
        output,
        admissions: Mutex::new(BTreeMap::new()),
        changed: Notify::new(),
        ready: Semaphore::new(0),
    });
    let recorder = Arc::new(Recorder::default());
    let error_gate = Arc::new(ErrorGate {
        history: history.clone(),
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        at_report: Mutex::new(vec![]),
        active: AtomicUsize::new(0),
    });
    let worker = bounded(
        "pipeline start",
        Pipeline::new(vec![
            source,
            error_gate.clone(),
            measured.clone(),
            recorder.clone(),
            assistant,
        ])
        .start(PipelineConfig {
            startup_timeout: WAIT,
            shutdown_timeout: WAIT,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    Fixture {
        worker,
        measured,
        recorder,
        error_gate,
        history,
        shared,
        control,
        backend,
    }
}

async fn send(f: &Fixture, frame: Frame) -> TestResult<u64> {
    let id = frame.id;
    bounded(
        "pipeline send",
        f.worker.sender().send(frame, Direction::Downstream),
    )
    .await?;
    f.measured.admitted(id).await?;
    Ok(id)
}
fn image() -> Frame {
    let mut frame = Frame::new(FrameData::OutputImageDetailed(OutputImageRawFrame {
        image: ImageRawFrame {
            image: vec![255, 0, 0],
            size: (1, 1),
            format: Some("RGB".into()),
        },
        sync_with_audio: false,
    }));
    frame
        .metadata
        .insert("history-live-image".into(), true.into());
    frame
}
fn flow_fence() -> Frame {
    let mut frame = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("pipecat.flows".into()),
        returning: false,
    }));
    frame.interruptible = false;
    frame
        .metadata
        .insert("history-live-fence".into(), true.into());
    frame
}
async fn spoken_body(f: &Fixture, id: &str, content: &str) -> TestResult<()> {
    send(f, Frame::new(FrameData::LlmResponseStart)).await?;
    send(
        f,
        Frame::new(FrameData::TtsStarted(TtsStartedFrame {
            context_id: Some(id.into()),
            append_to_context: true,
        })),
    )
    .await?;
    send(
        f,
        Frame::new(FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new(content),
            context_id: Some(id.into()),
            aggregated_by: "word".into(),
            raw_text: None,
            will_be_spoken: true,
        })),
    )
    .await?;
    let mut pcm = AudioRawFrame::new(encode_pcm16_le(&[1000; 80]), 8000, 1)
        .map_err(|error| error.to_string())?;
    pcm.context_id = Some(id.into());
    send(f, Frame::new(FrameData::TtsAudio(pcm))).await?;
    send(
        f,
        Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
            context_id: Some(id.into()),
        })),
    )
    .await?;
    Ok(())
}
fn contents(shared: &SharedContext) -> Vec<String> {
    shared
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .filter_map(|message| match message.content {
            Some(Content::Text(text)) => Some(text),
            _ => None,
        })
        .collect()
}

// Always remove every intentional hold and join before behavioral assertions.
async fn finish(f: Fixture) -> (Vec<DirectedFrame>, WorkerReport) {
    f.backend.release();
    f.error_gate.release.add_permits(8);
    let end = bounded("normal End", f.worker.end()).await;
    let collected = timeout(WAIT + WAIT, f.worker.collect()).await;
    assert_eq!(f.backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(f.error_gate.active.load(Ordering::SeqCst), 0);
    assert_eq!(f.backend.starts.load(Ordering::SeqCst), 1);
    assert_eq!(f.backend.stops.load(Ordering::SeqCst), 1);
    assert!(!f.control.is_ready());
    end.unwrap();
    let (frames, report) = collected.unwrap().unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(
        frames
            .iter()
            .filter(|f| {
                f.direction == Direction::Downstream
                    && matches!(f.frame.data, FrameData::End(_) | FrameData::EndDetailed(_))
            })
            .count(),
        1
    );
    (frames, report)
}

#[tokio::test(start_paused = true)]
async fn held_live_image_allows_actual_spoken_commit_but_holds_flow_fence() {
    let f = fixture(false).await;
    let expected_image = image();
    let expected_fence = flow_fence();
    let backend = f.backend.clone();
    let history = f.history.clone();
    let shared = f.shared.clone();
    let observation: TestResult<_> = async {
        permit("output ready", &f.measured.ready).await?;
        send(&f, expected_image.clone()).await?;
        permit("held live image entered", &backend.image_entered).await?;
        spoken_body(&f, "heard-with-live-image", "Heard while image waits").await?;
        let end_id = send(&f, Frame::new(FrameData::LlmResponseEnd)).await?;
        // OutputTransport commits through the real sink before forwarding the
        // source-end to this recorder; the assistant later consumes that end.
        f.recorder.seen(end_id).await?;
        let committed_while_image_held = contents(&f.shared);
        let status_while_image_held = f.history.status();
        let played_while_image_held = backend.played.lock().unwrap().len();
        let images_while_held = backend.images.lock().unwrap().len();
        let active_while_held = backend.active.load(Ordering::SeqCst);
        send(&f, expected_fence.clone()).await?;
        // This waits a bounded scheduling window after completed real fence
        // admission. One yield or ingress acceptance is not negative evidence.
        let fence_held = timeout(OBSERVE, f.recorder.seen(expected_fence.id))
            .await
            .is_err();
        backend.image_release.add_permits(1);
        // Complete the core's outward/upstream/returning round trip before End
        // closes its source lane. The final boundary is checked after collect.
        f.recorder
            .seen_matching(|frame| {
                frame.id == expected_fence.id
                    && matches!(&frame.data, FrameData::Flush(flush) if flush.returning)
            })
            .await?;
        Ok((
            committed_while_image_held,
            status_while_image_held,
            played_while_image_held,
            images_while_held,
            active_while_held,
            fence_held,
        ))
    }
    .await;
    let (frames, report) = finish(f).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let (committed, status, played, images, active, fence_held) = observation.unwrap();
    assert_eq!(committed, ["Heard while image waits"]);
    assert_eq!(status.committed_contexts, 1);
    assert_eq!(status.pending_responses, 0);
    assert_eq!(played, 1, "one full PCM chunk really returned Played");
    assert_eq!(images, 0);
    assert_eq!(active, 1, "the live backend future is still held");
    assert!(fence_held, "Flow fence passed an unfinished live image");
    assert_eq!(contents(&shared), committed);
    assert_eq!(history.status().committed_contexts, 1);
    assert_eq!(
        backend.images.lock().unwrap().as_slice(),
        std::slice::from_ref(&expected_image)
    );
    let original_images: Vec<_> = frames
        .iter()
        .filter(|f| f.direction == Direction::Downstream && f.frame.id == expected_image.id)
        .collect();
    assert_eq!(original_images.len(), 1);
    assert_eq!(original_images[0].frame, expected_image);
    let fences: Vec<_> = frames
        .iter()
        .filter(|f| f.direction == Direction::Downstream && f.frame.id == expected_fence.id)
        .collect();
    assert_eq!(fences.len(), 1);
    let mut expected_returning = expected_fence.clone();
    if let FrameData::Flush(flush) = &mut expected_returning.data {
        flush.returning = true;
    }
    assert_eq!(fences[0].frame, expected_returning);
    assert_eq!(
        fences[0].frame.observation_token(),
        expected_fence.observation_token()
    );
    let image_position = frames
        .iter()
        .position(|f| f.frame.id == expected_image.id)
        .unwrap();
    let fence_position = frames
        .iter()
        .position(|f| f.frame.id == expected_fence.id)
        .unwrap();
    assert!(image_position < fence_position);
}

#[tokio::test(start_paused = true)]
async fn live_image_write_error_invalidates_pending_before_source_end_and_keeps_committed() {
    let f = fixture(true).await;
    let expected_image = image();
    let backend = f.backend.clone();
    let history = f.history.clone();
    let shared = f.shared.clone();
    let recorder = f.recorder.clone();
    let error_gate = f.error_gate.clone();
    let observation: TestResult<_> = async {
        permit("output ready", &f.measured.ready).await?;
        spoken_body(&f, "already-committed", "Previously heard").await?;
        let first_end = send(&f, Frame::new(FrameData::LlmResponseEnd)).await?;
        f.recorder.seen(first_end).await?;
        let committed_before_failure = contents(&f.shared);
        let committed_status = f.history.status();

        spoken_body(&f, "must-be-withheld", "Must remain uncommitted").await?;
        // This media-lane backend control is after the Played PCM and TTS stop.
        // It holds the already-admitted source-end without holding live images.
        send(
            &f,
            Frame::new(FrameData::OutputTransportMessage(serde_json::json!({
                "hold-source-end": true
            }))),
        )
        .await?;
        permit("media control held", &backend.control_entered).await?;
        let pending_end = send(&f, Frame::new(FrameData::LlmResponseEnd)).await?;
        send(&f, expected_image.clone()).await?;
        permit("failing live image entered", &backend.image_entered).await?;
        let pending_before_failure = f.history.status();
        let played_before_failure = backend.played.lock().unwrap().len();
        backend.image_release.add_permits(1);
        permit(
            "live failure reported before source propagation",
            &error_gate.entered,
        )
        .await?;
        let at_report = error_gate.at_report.lock().unwrap().clone();
        let after_failure = f.history.status();
        let source_end_still_held = !recorder
            .frames
            .lock()
            .unwrap()
            .iter()
            .any(|frame| frame.id == pending_end);

        // Keep upstream Error propagation blocked even while the source-end is
        // handled: only the live worker can have discarded this pending proof.
        backend.control_release.add_permits(1);
        recorder.seen(pending_end).await?;
        let after_source_end = f.history.status();
        let contents_after_source_end = contents(&f.shared);
        Ok((
            committed_before_failure,
            committed_status,
            pending_before_failure,
            played_before_failure,
            at_report,
            after_failure,
            source_end_still_held,
            after_source_end,
            contents_after_source_end,
        ))
    }
    .await;
    let (frames, report) = finish(f).await;
    let (
        committed_before,
        committed_status,
        pending_before,
        played,
        at_report,
        after_failure,
        source_end_held,
        after_end,
        contents_after_end,
    ) = observation.unwrap();
    assert_eq!(committed_before, ["Previously heard"]);
    assert_eq!(committed_status.committed_contexts, 1);
    assert_eq!(pending_before.pending_responses, 1);
    assert_eq!(pending_before.committed_contexts, 1);
    assert_eq!(played, 2, "both genuine PCM writes returned Played");
    assert_eq!(at_report.len(), 1);
    assert_eq!(at_report[0].pending_responses, 0);
    assert_eq!(at_report[0].pending_source_responses, 0);
    assert_eq!(at_report[0].withheld_contexts, 1);
    assert_eq!(at_report[0].committed_contexts, 1);
    assert!(at_report[0].last_rejection.is_some());
    assert_eq!(after_failure.pending_responses, 0);
    assert!(
        source_end_held,
        "source-end completed before the failure witness"
    );
    assert_eq!(after_end.committed_contexts, 1);
    assert_eq!(contents_after_end, committed_before);
    assert_eq!(contents(&shared), committed_before);
    assert_eq!(history.status().committed_contexts, 1);
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(report.errors[0].error.contains(IMAGE_ERROR));
    assert_eq!(backend.attempts.lock().unwrap().len(), 1);
    assert!(backend.images.lock().unwrap().is_empty());
    assert!(!frames.iter().any(|f| f.frame.id == expected_image.id));
    assert!(!recorder
        .frames
        .lock()
        .unwrap()
        .iter()
        .any(|f| f.id == expected_image.id));
}
