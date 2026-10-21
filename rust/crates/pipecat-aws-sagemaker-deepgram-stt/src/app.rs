use crate::{
    processor::Shared, Error, Progress, SageMakerSttConfig, SageMakerSttProcessor, TransportReport,
};
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, Pipeline,
    PipelineConfig, PipelineError, ProcessorContext, ProcessorError, ProcessorUnusablePolicy,
    StartFrame, WorkerState,
};
use serde::Serialize;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::watch;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Terminal {
    CompletedWithTranscript,
    CompletedWithoutTranscript,
    PartialFailure,
    Cancelled,
    Failed,
}
#[derive(Clone, Debug)]
pub struct Report {
    pub terminal: Terminal,
    pub progress: Progress,
    pub primary: Option<Error>,
    pub transport: Option<TransportReport>,
    pub pipeline_joined: bool,
    pub output_cleaned: bool,
    pub output_cleanup_failed: bool,
    pub pipeline_failed: bool,
}
impl Report {
    pub fn is_success(&self) -> bool {
        matches!(
            self.terminal,
            Terminal::CompletedWithTranscript | Terminal::CompletedWithoutTranscript
        )
    }
}
struct CompletionState {
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Report>>>,
    report: Mutex<Option<Report>>,
    shared: Arc<Shared>,
    control: watch::Sender<u8>,
}
#[derive(Clone)]
pub struct Completion(Arc<CompletionState>);
impl Completion {
    pub fn progress(&self) -> Progress {
        self.0.shared.progress.borrow().clone()
    }
    /// The retained JoinHandle is never taken across an await. Cancelling one
    /// waiter leaves the same owner for the next/concurrent waiter.
    pub async fn wait(&self) -> Report {
        let mut task = self.0.task.lock().await;
        if let Some(handle) = task.as_mut() {
            let report = match handle.await {
                Ok(report) => report,
                Err(_) => Report {
                    terminal: Terminal::Failed,
                    progress: self.progress(),
                    primary: Some(Error::Task),
                    transport: None,
                    pipeline_joined: false,
                    output_cleaned: false,
                    output_cleanup_failed: false,
                    pipeline_failed: true,
                },
            };
            *self.0.report.lock().unwrap_or_else(|e| e.into_inner()) = Some(report);
            task.take();
        }
        self.0
            .report
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .expect("joined report cached")
            .clone()
    }
}
pub struct RunningApp {
    completion: Completion,
}
impl RunningApp {
    pub fn completion(&self) -> Completion {
        self.completion.clone()
    }
    pub fn cancel(&self) {
        self.completion.0.shared.cancel();
        self.completion.0.control.send_replace(1);
    }
    pub fn stop(&self) {
        self.completion.0.shared.cancel();
        self.completion.0.control.send_replace(2);
    }
    pub async fn finish(&self) -> Report {
        self.completion.wait().await
    }
}
impl Drop for RunningApp {
    fn drop(&mut self) {
        self.cancel();
    }
}
/// A complete bounded PCM buffer is validated before any pipeline or network side
/// effect. Input is unpaced finite-file PCM, not a real-time microphone stream.
pub struct SageMakerSttApp {
    config: SageMakerSttConfig,
    pcm: Box<[u8]>,
    output: Arc<dyn FrameProcessor>,
    timeline: Box<[crate::TimelineMarker]>,
}
impl SageMakerSttApp {
    pub fn new(
        config: SageMakerSttConfig,
        pcm: Vec<u8>,
        output: Arc<dyn FrameProcessor>,
    ) -> Result<Self, Error> {
        config.validate_pcm(&pcm)?;
        if config.origin.is_none() && config.address.is_none() {
            return Err(Error::Invalid("explicit hosted route required"));
        }
        crate::Timeline::default().validate(
            pcm.len().div_ceil(config.limits.chunk_bytes),
            &config.limits,
        )?;
        crate::protocol::end(&config.limits)?;
        Ok(Self {
            config,
            pcm: pcm.into_boxed_slice(),
            output,
            timeline: Box::new([]),
        })
    }
    pub fn with_timeline(mut self, timeline: crate::Timeline) -> Result<Self, Error> {
        timeline.validate(
            self.pcm.len().div_ceil(self.config.limits.chunk_bytes),
            &self.config.limits,
        )?;
        self.timeline = timeline.0.into_boxed_slice();
        Ok(self)
    }
    pub fn start(self) -> Result<RunningApp, Error> {
        let provider = Arc::new(SageMakerSttProcessor::new(self.config.clone())?);
        let shared = provider.shared.clone();
        shared
            .progress
            .send_modify(|p| p.pcm_bytes_captured = self.pcm.len() as u64);
        let output = Arc::new(OutputGuard {
            inner: self.output,
            shared: shared.clone(),
            timeout: self.config.limits.output_timeout,
            cleaned: AtomicBool::new(false),
            cleanup_failed: AtomicBool::new(false),
        });
        let pipeline = Pipeline::new(vec![provider, output.clone()]);
        let config = PipelineConfig {
            queue_capacity: 4,
            output_capacity: 4,
            max_background_tasks: 2,
            shutdown_timeout: self.config.limits.duplex.session_timeout
                + self.config.limits.duplex.close_timeout * 3
                + self.config.limits.output_timeout * 3,
            startup_timeout: self.config.limits.output_timeout
                + self.config.limits.duplex.connect_timeout,
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            start: StartFrame {
                audio_in_sample_rate: self.config.settings.sample_rate_hz,
                ..Default::default()
            },
            ..Default::default()
        };
        let (control, mut controls) = watch::channel(0u8);
        let owner_shared = shared.clone();
        let task = tokio::spawn(async move {
            let mut primary = None;
            let mut pipeline_joined = false;
            let mut pipeline_failed = true;
            let mut ended = false;
            match pipeline.start(config).await {
                Ok(mut worker) => {
                    let sender = worker.sender();
                    let mut seen = owner_shared.progress.subscribe();
                    let feed = async {
                        for (index, chunk) in
                            self.pcm.chunks(self.config.limits.chunk_bytes).enumerate()
                        {
                            if *owner_shared.cancelled.borrow() {
                                return Err(Error::Cancelled);
                            }
                            sender
                                .send(
                                    Frame::new(FrameData::InputAudio(
                                        AudioRawFrame::new(
                                            chunk.to_vec(),
                                            self.config.settings.sample_rate_hz,
                                            1,
                                        )
                                        .map_err(|_| Error::Invalid("PCM"))?,
                                    )),
                                    Direction::Downstream,
                                )
                                .await
                                .map_err(|_| Error::Task)?;
                            loop {
                                let p = seen.borrow_and_update().clone();
                                if let Some(error) = p.primary {
                                    return Err(error);
                                }
                                if p.chunks_written > index as u64 {
                                    break;
                                }
                                seen.changed().await.map_err(|_| Error::Task)?;
                            }
                            if let Some(marker) =
                                self.timeline.iter().find(|m| m.after_chunk == index + 1)
                            {
                                // The previous notification follows the actual one-shot
                                // writer receipt, not merely queue admission.
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    marker.pause_ms,
                                ))
                                .await;
                                if marker.vad_stopped {
                                    let before = seen.borrow().finalize_written;
                                    sender
                                        .send(
                                            Frame::new(FrameData::VadUserStoppedSpeakingDetailed(
                                                Default::default(),
                                            )),
                                            Direction::Downstream,
                                        )
                                        .await
                                        .map_err(|_| Error::Task)?;
                                    loop {
                                        let p = seen.borrow_and_update().clone();
                                        if let Some(error) = p.primary {
                                            return Err(error);
                                        }
                                        if p.finalize_written > before {
                                            break;
                                        }
                                        seen.changed().await.map_err(|_| Error::Task)?;
                                    }
                                }
                            }
                        }
                        sender.end().await.map_err(|_| Error::Task)
                    };
                    tokio::pin!(feed);
                    let mut feed_done = false;
                    let mut controls_open = true;
                    if *owner_shared.cancelled.borrow() {
                        feed_done = true;
                        let _ = sender.cancel("finite STT cancelled before startup").await;
                    }
                    loop {
                        tokio::select! {biased;
                            changed=controls.changed(),if controls_open=>{
                                if changed.is_err(){controls_open=false;}
                                let command=*controls.borrow_and_update();if command!=0 || !controls_open{
                                    feed_done=true;owner_shared.cancel();
                                    if command==2{if sender.send(Frame::new(FrameData::Stop),Direction::Downstream).await.is_err(){let _=sender.cancel("finite STT stopped during End").await;}}
                                    else{let _=sender.cancel("finite STT local cancellation").await;}
                                }
                            }
                            result=&mut feed,if !feed_done=>{feed_done=true;if let Err(error)=result{primary=Some(error);owner_shared.cancel();let _=sender.cancel("finite STT admission failed").await;}}
                            frame=worker.recv()=>{if frame.is_none(){break;}}
                        }
                    }
                    let joined = worker.join().await;
                    // An ordinary returned failure follows the core supervisor's
                    // real actor/background/cleanup joins. ProcessorPanic also
                    // represents a failed supervisor await, so keep that case
                    // conservatively unknown rather than inventing ownership.
                    pipeline_joined = !matches!(
                        joined,
                        Err(PipelineError::ProcessorPanic(_) | PipelineError::AlreadyJoined)
                    );
                    pipeline_failed = joined.is_err() || worker.total_errors() != 0;
                    ended = worker.state() == WorkerState::Ended;
                }
                Err(_) => {
                    primary = Some(Error::Task);
                    owner_shared.cancel();
                }
            }
            // Core intentionally omits processor cleanup on Stop. This manager
            // independently retains the transport and caller-output cleanup.
            if !ended {
                owner_shared.cancel();
            }
            let transport = owner_shared.transport_report().await;
            let _ = output.cleanup().await;
            let progress = owner_shared.progress.borrow().clone();
            let transport_error = transport
                .as_ref()
                .and_then(|r| r.primary.as_ref().or(r.cleanup.as_ref()))
                .cloned()
                .map(Error::Transport);
            primary = progress.primary.clone().or(primary).or(transport_error);
            let output_cleaned = output.cleaned.load(Ordering::Acquire);
            let output_cleanup_failed = output.cleanup_failed.load(Ordering::Acquire);
            let transport_ok = transport.as_ref().is_some_and(|r| {
                r.primary.is_none()
                    && r.cleanup.is_none()
                    && r.writer_joined
                    && r.reader_joined
                    && r.connection_joined
                    && !r.forced_abort
            });
            let success = ended
                && pipeline_joined
                && !pipeline_failed
                && primary.is_none()
                && transport_ok
                && progress.response_task_returned
                && output_cleaned
                && !output_cleanup_failed
                && progress.pcm_bytes_admitted == progress.pcm_bytes_captured
                && progress.pcm_bytes_uploaded == progress.pcm_bytes_captured;
            let cancelled = *controls.borrow() != 0;
            let terminal = if success {
                if progress.delivered_final_frames + progress.delivered_interim_frames > 0 {
                    Terminal::CompletedWithTranscript
                } else {
                    Terminal::CompletedWithoutTranscript
                }
            } else if cancelled {
                Terminal::Cancelled
            } else if progress.delivered_final_frames + progress.delivered_interim_frames > 0 {
                Terminal::PartialFailure
            } else {
                Terminal::Failed
            };
            Report {
                terminal,
                progress,
                primary,
                transport,
                pipeline_joined,
                output_cleaned,
                output_cleanup_failed,
                pipeline_failed,
            }
        });
        Ok(RunningApp {
            completion: Completion(Arc::new(CompletionState {
                task: tokio::sync::Mutex::new(Some(task)),
                report: Mutex::new(None),
                shared,
                control,
            })),
        })
    }
}
struct OutputGuard {
    inner: Arc<dyn FrameProcessor>,
    shared: Arc<Shared>,
    timeout: std::time::Duration,
    cleaned: AtomicBool,
    cleanup_failed: AtomicBool,
}
impl OutputGuard {
    async fn call(
        &self,
        future: impl std::future::Future<Output = Result<(), ProcessorError>> + Send,
    ) -> Result<(), ProcessorError> {
        match tokio::time::timeout(
            self.timeout,
            std::panic::AssertUnwindSafe(future).catch_unwind(),
        )
        .await
        {
            Ok(Ok(Ok(()))) => Ok(()),
            _ => {
                self.shared.fail(Error::Output);
                Err(ProcessorError::fatal("finite STT output failed"))
            }
        }
    }
}
#[async_trait]
impl FrameProcessor for OutputGuard {
    fn name(&self) -> &str {
        "FiniteSttOutputGuard"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.call(self.inner.setup(config, ctx)).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let kind = match &frame.data {
            FrameData::TranscriptionDetailed(_) => 2,
            FrameData::InterimTranscriptionDetailed(_) => 1,
            _ => 0,
        };
        self.call(self.inner.process(frame, direction, ctx)).await?;
        self.shared.progress.send_modify(|p| match kind {
            2 => p.delivered_final_frames += 1,
            1 => p.delivered_interim_frames += 1,
            _ => {}
        });
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.call(self.inner.prepare_end(ctx)).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if self.cleaned.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let result = self.call(self.inner.cleanup()).await;
        if result.is_err() {
            self.cleanup_failed.store(true, Ordering::Release);
        }
        result
    }
}
