use crate::{analyzer::validate_params, SileroVadAnalyzer, VadAnalysis, VadParams, VadState};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
    SpeechControlParamsFrame, StartFrame, VadSpeakingFrame,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct ProcessorOptions {
    /// Bounded number of frames waiting for the one inference thread.
    pub queue_capacity: usize,
    /// Limits the memory and CPU cost of one incoming frame (default one second).
    pub max_frame_duration: Duration,
    pub speech_activity_period: Duration,
    /// Zero disables idle detection.
    pub audio_idle_timeout: Duration,
}
impl Default for ProcessorOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 32,
            max_frame_duration: Duration::from_secs(1),
            speech_activity_period: Duration::from_millis(200),
            audio_idle_timeout: Duration::from_secs(1),
        }
    }
}
enum Command {
    Audio { bytes: Vec<u8> },
    Params(VadParams),
}
enum Completion {
    Audio(crate::Result<VadAnalysis>),
    Params(VadParams),
}
struct Worker {
    analyzer: Option<SileroVadAnalyzer>,
    sender: Option<mpsc::Sender<Command>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// Audio is forwarded immediately. One owned native thread runs serial model
/// inference and a tracked async task broadcasts its decisions in both directions.
/// End drains accepted audio; Cancel drops pending work. Cleanup joins the thread.
pub struct SileroVadProcessor {
    worker: Mutex<Worker>,
    options: ProcessorOptions,
    sample_rate: u32,
    initial_params: VadParams,
    abort: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    ending: Arc<AtomicBool>,
    last_audio: Arc<Mutex<Instant>>,
}
impl SileroVadProcessor {
    pub fn new(analyzer: SileroVadAnalyzer, options: ProcessorOptions) -> crate::Result<Self> {
        if options.queue_capacity == 0
            || options.queue_capacity > tokio::sync::Semaphore::MAX_PERMITS
            || options.max_frame_duration.is_zero()
            || options.max_frame_duration > Duration::from_secs(60)
        {
            return Err(crate::VadError::InvalidParameter(
                "VAD requires a valid bounded queue capacity and frame limit of at most 60 seconds",
            ));
        }
        Ok(Self {
            sample_rate: analyzer.sample_rate(),
            initial_params: analyzer.params().clone(),
            worker: Mutex::new(Worker {
                analyzer: Some(analyzer),
                sender: None,
                thread: None,
            }),
            options,
            abort: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicBool::new(false)),
            ending: Arc::new(AtomicBool::new(false)),
            last_audio: Arc::new(Mutex::new(Instant::now())),
        })
    }
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    fn lock(&self) -> std::result::Result<std::sync::MutexGuard<'_, Worker>, ProcessorError> {
        self.worker
            .lock()
            .map_err(|_| ProcessorError::fatal("VAD worker lock poisoned"))
    }
    fn enqueue(&self, command: Command) -> std::result::Result<(), ProcessorError> {
        let worker = self.lock()?;
        worker
            .sender
            .as_ref()
            .ok_or_else(|| ProcessorError::new("VAD is not running"))?
            .try_send(command)
            .map_err(|error| ProcessorError::new(format!("VAD inference queue: {error}")))
    }
    fn close(&self, abort: bool) -> std::result::Result<(), ProcessorError> {
        self.ending.store(true, Ordering::SeqCst);
        if abort {
            self.abort.store(true, Ordering::SeqCst);
        }
        self.lock()?.sender.take();
        Ok(())
    }
}
fn speaking_frame(start: bool, params: &VadParams) -> Frame {
    let payload = VadSpeakingFrame {
        duration_secs: if start {
            params.start_secs
        } else {
            params.stop_secs
        },
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
    };
    Frame::new(if start {
        FrameData::VadUserStartedSpeaking(payload)
    } else {
        FrameData::VadUserStoppedSpeaking(payload)
    })
}
fn control_params(params: VadParams) -> Frame {
    Frame::new(FrameData::SpeechControlParams(SpeechControlParamsFrame {
        vad_params: Some(params),
        turn_params: None,
    }))
}
#[async_trait]
impl FrameProcessor for SileroVadProcessor {
    fn name(&self) -> &str {
        "SileroVadProcessor"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if config.audio_in_sample_rate != self.sample_rate {
            return Err(ProcessorError::fatal(
                "VAD analyzer sample rate must match pipeline input rate",
            ));
        }
        let (tx, mut rx) = mpsc::channel(self.options.queue_capacity);
        let (results_tx, mut results_rx) = mpsc::channel(self.options.queue_capacity);
        let mut worker = self.lock()?;
        let mut analyzer = worker
            .analyzer
            .take()
            .ok_or_else(|| ProcessorError::fatal("VAD processor cannot be reused after setup"))?;
        let abort = self.abort.clone();
        let running = self.running.clone();
        let handle = std::thread::Builder::new()
            .name("pipecat-silero".into())
            .spawn(move || {
                struct RunningGuard(Arc<AtomicBool>);
                impl Drop for RunningGuard {
                    fn drop(&mut self) {
                        self.0.store(false, Ordering::SeqCst);
                    }
                }
                running.store(true, Ordering::SeqCst);
                let _guard = RunningGuard(running);
                let origin = std::time::Instant::now();
                while let Some(command) = rx.blocking_recv() {
                    if abort.load(Ordering::SeqCst) {
                        break;
                    }
                    let completion = match command {
                        Command::Audio { bytes } => Completion::Audio(
                            analyzer.analyze_interruptible_at(&bytes, origin.elapsed(), || {
                                abort.load(Ordering::SeqCst)
                            }),
                        ),
                        Command::Params(params) => match analyzer.set_params(params.clone()) {
                            Ok(()) => Completion::Params(params),
                            Err(error) => Completion::Audio(Err(error)),
                        },
                    };
                    if abort.load(Ordering::SeqCst) || results_tx.blocking_send(completion).is_err()
                    {
                        break;
                    }
                }
            })
            .map_err(|error| ProcessorError::fatal(format!("cannot start VAD thread: {error}")))?;
        worker.sender = Some(tx);
        worker.thread = Some(handle);
        drop(worker);
        let ctx = ctx.clone();
        let task_ctx = ctx.clone();
        let last_audio = self.last_audio.clone();
        let ending = self.ending.clone();
        let options = self.options.clone();
        let mut params = self.initial_params.clone();
        ctx.spawn("silero-decisions", false, async move {
            let mut speaking = false;
            let mut last_activity: Option<Instant> = None;
            let tick = if options.audio_idle_timeout.is_zero() {
                Duration::from_secs(3600)
            } else {
                (options.audio_idle_timeout / 4).max(Duration::from_millis(1))
            };
            let mut timer = tokio::time::interval(tick);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    completion = results_rx.recv() => match completion {
                        None => break,
                        Some(Completion::Params(updated)) => {
                            params = updated;
                            task_ctx.broadcast(control_params(params.clone())).await?;
                        }
                        Some(Completion::Audio(result)) => {
                            let result = result.map_err(|error| {
                                ProcessorError::fatal(format!("VAD inference: {error}"))
                            })?;
                            match result.state {
                                VadState::Speaking if !speaking => {
                                    speaking = true;
                                    task_ctx.broadcast(speaking_frame(true, &params)).await?;
                                }
                                VadState::Quiet if speaking => {
                                    speaking = false;
                                    task_ctx.broadcast(speaking_frame(false, &params)).await?;
                                }
                                _ => {}
                            }
                            if speaking && last_activity.is_none_or(|last| {
                                last.elapsed() >= options.speech_activity_period
                            }) {
                                last_activity = Some(Instant::now());
                                task_ctx.broadcast(Frame::new(FrameData::UserSpeaking)).await?;
                            }
                        }
                    },
                    _ = timer.tick(), if !options.audio_idle_timeout.is_zero()
                        && !ending.load(Ordering::SeqCst) => {
                        let idle = last_audio.lock()
                            .map_err(|_| ProcessorError::fatal("VAD idle lock poisoned"))?
                            .elapsed() >= options.audio_idle_timeout;
                        if speaking && idle {
                            speaking = false;
                            task_ctx.broadcast(speaking_frame(false, &params)).await?;
                        }
                    }
                }
            }
            Ok(())
        })?;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if direction != Direction::Downstream {
            return ctx.push(frame, direction).await;
        }
        match &frame.data {
            FrameData::InputAudio(audio) => {
                audio
                    .validate()
                    .map_err(|error| ProcessorError::new(error.to_string()))?;
                if audio.sample_rate != self.sample_rate || audio.num_channels != 1 {
                    return Err(ProcessorError::new(
                        "VAD input requires mono PCM at its configured sample rate",
                    ));
                }
                let max_samples = (self.options.max_frame_duration.as_secs_f64()
                    * f64::from(self.sample_rate))
                .floor() as usize;
                if audio.audio.len() / 2 > max_samples {
                    return Err(ProcessorError::new(
                        "VAD input frame exceeds configured duration limit",
                    ));
                }
                let bytes = audio.audio.clone();
                *self
                    .last_audio
                    .lock()
                    .map_err(|_| ProcessorError::fatal("VAD idle lock poisoned"))? = Instant::now();
                ctx.push(frame, direction).await?;
                self.enqueue(Command::Audio { bytes })
            }
            FrameData::VadParamsUpdate(params) => {
                validate_params(params).map_err(|error| ProcessorError::new(error.to_string()))?;
                let params = params.clone();
                ctx.push(frame, direction).await?;
                self.enqueue(Command::Params(params))
            }
            FrameData::Start(_) | FrameData::StartDetailed(_) => {
                ctx.push(frame, direction).await?;
                ctx.broadcast(control_params(self.initial_params.clone()))
                    .await
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                self.close(true)?;
                ctx.push(frame, direction).await
            }
            FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop => {
                self.close(false)?;
                ctx.push(frame, direction).await
            }
            _ => ctx.push(frame, direction).await,
        }
    }
    async fn prepare_end(
        &self,
        _ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        self.close(false)
    }
    async fn cleanup(&self) -> std::result::Result<(), ProcessorError> {
        self.close(true)?;
        let handle = {
            let mut worker = self.lock()?;
            worker.analyzer.take();
            worker.thread.take()
        };
        if let Some(handle) = handle {
            tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|error| ProcessorError::fatal(format!("VAD thread join: {error}")))?
                .map_err(|_| ProcessorError::fatal("VAD inference thread panicked"))?;
        }
        Ok(())
    }
}
impl Drop for SileroVadProcessor {
    fn drop(&mut self) {
        self.abort.store(true, Ordering::SeqCst);
        if let Ok(worker) = self.worker.get_mut() {
            worker.sender.take();
        }
    }
}
