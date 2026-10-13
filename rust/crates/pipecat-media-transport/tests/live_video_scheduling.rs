//! Candidate version of the frozen public baseline: the backend explicitly
//! declares independent-video capability and the two original probes opt in via
//! with_live_video. All original recording/cleanup assertions are retained.
//! A separate legacy control keeps the unchanged constructor's serialization.
//! A paused-clock window is an ordering probe, not a device latency benchmark.
use async_trait::async_trait;
use pipecat_audio::{encode_pcm16_le, PcmFormat};
use pipecat_core::*;
use pipecat_media_transport::*;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::timeout};

const WAIT: Duration = Duration::from_secs(3);
const OBSERVE: Duration = Duration::from_millis(100);
const RATE: u32 = 16_000;
const SAMPLES: usize = 640;
const NAME: &str = "live-video-scheduling-output";

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

struct Backend {
    audio_entered: Semaphore,
    audio_release: Semaphore,
    image_completed: Semaphore,
    pcm_released: AtomicBool,
    image_before_pcm_release: AtomicBool,
    active: AtomicUsize,
    starts: AtomicUsize,
    stops: AtomicUsize,
    audio_completed: Mutex<Vec<Frame>>,
    images: Mutex<Vec<Frame>>,
}
impl Backend {
    fn new() -> Self {
        Self {
            audio_entered: Semaphore::new(0),
            audio_release: Semaphore::new(0),
            image_completed: Semaphore::new(0),
            pcm_released: AtomicBool::new(false),
            image_before_pcm_release: AtomicBool::new(false),
            active: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            audio_completed: Mutex::new(Vec::new()),
            images: Mutex::new(Vec::new()),
        }
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
    fn supports_independent_video(&self) -> bool {
        true
    }
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active(&self.active);
        self.audio_entered.add_permits(1);
        self.audio_release
            .acquire()
            .await
            .map_err(|error| ProcessorError::new(error.to_string()))?
            .forget();
        self.audio_completed.lock().unwrap().push(frame.clone());
        Ok(PlaybackReceipt::Played)
    }
    async fn write_image(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = Active(&self.active);
        self.images.lock().unwrap().push(frame.clone());
        self.image_before_pcm_release
            .store(!self.pcm_released.load(Ordering::SeqCst), Ordering::SeqCst);
        self.image_completed.add_permits(1);
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

// This wrapper records completion of the real public OutputTransport admission,
// not just PipelineSender ingress acceptance. All execution is delegated.
struct MeasuredOutput {
    output: Arc<OutputTransport>,
    image_id: u64,
    ready: Semaphore,
    admitted: Semaphore,
    admission: Mutex<Option<Result<(), ProcessorError>>>,
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
        let is_start = frame.data.is_start();
        let image = frame.id == self.image_id && direction == Direction::Downstream;
        let result = self.output.process(frame, direction, ctx).await;
        if is_start && result.is_ok() {
            self.ready.add_permits(1);
        }
        if image {
            *self.admission.lock().unwrap() = Some(result.clone());
            self.admitted.add_permits(1);
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

struct Observation {
    before_pcm_release: bool,
    audio_completed_while_held: usize,
    image_completed_while_held: usize,
}
struct Finished {
    backend: Arc<Backend>,
    control: OutputControl,
    expected_image: Frame,
    observation: TestResult<Observation>,
    end: TestResult<()>,
    collected: TestResult<(Vec<DirectedFrame>, WorkerReport)>,
}
impl Finished {
    fn assert_clean(self) -> Observation {
        // These assertions occur only after releasing the PCM and collecting
        // the worker, so the intentionally failing behavior leaves no task alive.
        assert_eq!(self.backend.active.load(Ordering::SeqCst), 0);
        assert_eq!(self.backend.starts.load(Ordering::SeqCst), 1);
        assert_eq!(self.backend.stops.load(Ordering::SeqCst), 1);
        assert!(!self.control.is_ready());
        self.end.unwrap();
        let (frames, report) = self.collected.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.total_errors, 0);
        assert_eq!(
            self.backend.images.lock().unwrap().as_slice(),
            std::slice::from_ref(&self.expected_image)
        );
        let images: Vec<_> = frames
            .iter()
            .filter(|item| {
                item.direction == Direction::Downstream && item.frame.id == self.expected_image.id
            })
            .collect();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].frame, self.expected_image);
        assert_eq!(
            frames
                .iter()
                .filter(|item| {
                    item.direction == Direction::Downstream
                        && matches!(
                            item.frame.data,
                            FrameData::End(_) | FrameData::EndDetailed(_)
                        )
                })
                .count(),
            1
        );
        assert!(!self.backend.audio_completed.lock().unwrap().is_empty());
        let observation = self.observation.unwrap();
        assert_eq!(observation.audio_completed_while_held, 0);
        assert_eq!(
            observation.image_completed_while_held,
            usize::from(observation.before_pcm_release)
        );
        assert_eq!(
            self.backend.image_before_pcm_release.load(Ordering::SeqCst),
            observation.before_pcm_release
        );
        observation
    }
}

async fn observe(sync_with_audio: bool) -> Finished {
    observe_mode(sync_with_audio, true).await
}

async fn observe_mode(sync_with_audio: bool, independent: bool) -> Finished {
    let backend = Arc::new(Backend::new());
    let mut expected_image = Frame::new(FrameData::OutputImageDetailed(OutputImageRawFrame {
        image: ImageRawFrame {
            image: vec![255, 0, 0],
            size: (1, 1),
            format: Some("RGB".into()),
        },
        sync_with_audio,
    }));
    expected_image.metadata.insert(
        "witness".into(),
        serde_json::json!({"sync_with_audio": sync_with_audio, "source": "public-api"}),
    );
    let output = OutputTransport::new(
        NAME,
        OutputTransportConfig {
            sample_rate: Some(RATE),
            channels: 1,
            chunk_duration: Duration::from_millis(40),
            queue_capacity: 8,
            queue_byte_capacity: 64 * 1024,
            auto_silence: false,
            end_silence: Duration::ZERO,
            write_timeout: Duration::from_secs(10),
            ..Default::default()
        },
        backend.clone(),
        None,
        BTreeMap::new(),
    )
    .unwrap();
    let output = if independent {
        output
            .with_live_video(LiveVideoOutputConfig {
                queue_capacity: 8,
                queue_byte_capacity: 64 * 1024,
                framerate: None,
            })
            .unwrap()
    } else {
        output
    };
    let output = Arc::new(output);
    let control = output.control();
    let measured = Arc::new(MeasuredOutput {
        output,
        image_id: expected_image.id,
        ready: Semaphore::new(0),
        admitted: Semaphore::new(0),
        admission: Mutex::new(None),
    });
    let worker = bounded(
        "pipeline start",
        Pipeline::new(vec![measured.clone()]).start(PipelineConfig {
            shutdown_timeout: WAIT,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let sender = worker.sender();
    let observation = async {
        bounded("output ready", measured.ready.acquire())
            .await?
            .forget();
        if !control.is_ready() {
            return Err("output completed Start without readiness".into());
        }
        let audio = AudioRawFrame::new(encode_pcm16_le(&vec![1000; SAMPLES]), RATE, 1)
            .map_err(|error| error.to_string())?;
        bounded(
            "audio ingress",
            sender.send(
                Frame::new(FrameData::OutputAudio(audio)),
                Direction::Downstream,
            ),
        )
        .await?;
        bounded("PCM backend entered", backend.audio_entered.acquire())
            .await?
            .forget();
        bounded(
            "image ingress",
            sender.send(expected_image.clone(), Direction::Downstream),
        )
        .await?;
        bounded("actual image admission", measured.admitted.acquire())
            .await?
            .forget();
        measured
            .admission
            .lock()
            .unwrap()
            .clone()
            .ok_or("image admission result missing")?
            .map_err(|error| error.to_string())?;
        let before_pcm_release = match timeout(OBSERVE, backend.image_completed.acquire()).await {
            Ok(Ok(permit)) => {
                permit.forget();
                true
            }
            Ok(Err(error)) => return Err(format!("image completion semaphore: {error}")),
            Err(_) => false,
        };
        Ok(Observation {
            before_pcm_release,
            audio_completed_while_held: backend.audio_completed.lock().unwrap().len(),
            image_completed_while_held: backend.images.lock().unwrap().len(),
        })
    }
    .await;
    // Never assert the feature observation while a backend future is held.
    // Extra permits allow any legitimate audio tail to finish during End.
    backend.pcm_released.store(true, Ordering::SeqCst);
    backend.audio_release.add_permits(32);
    let end = bounded("normal End admission", worker.end()).await;
    let collected = bounded("normal End drain and join", worker.collect()).await;
    Finished {
        backend,
        control,
        expected_image,
        observation,
        end,
        collected,
    }
}

#[tokio::test(start_paused = true)]
async fn sync_true_waits_for_pcm() {
    let observation = observe(true).await.assert_clean();
    assert!(
        !observation.before_pcm_release,
        "sync=true must remain behind the preceding held PCM playback"
    );
}

#[tokio::test(start_paused = true)]
async fn false_can_complete_while_pcm_is_held() {
    let observation = observe(false).await.assert_clean();
    assert!(
        observation.before_pcm_release,
        "sync=false image did not complete while admitted PCM remained held; cleanup already joined"
    );
}

#[tokio::test(start_paused = true)]
async fn legacy_false_remains_serial() {
    let observation = observe_mode(false, false).await.assert_clean();
    assert!(
        !observation.before_pcm_release,
        "the unchanged constructor must retain serial false-image output"
    );
}

#[cfg(feature = "assistant-history")]
#[path = "live_video_scheduling/history.rs"]
mod history;
#[path = "live_video_scheduling/lifecycle.rs"]
mod lifecycle;
