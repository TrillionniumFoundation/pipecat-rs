use crate::{
    processor::{changed_control, Shared},
    Error, Progress, SageMakerBidiTtsConfig, SageMakerBidiTtsProcessor, TransportError,
    TransportReport,
};
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig,
    PipelineError, ProcessorContext, ProcessorError, ProcessorUnusablePolicy, StartFrame,
    TtsSpeakFrame, WorkerState,
};
use serde::Serialize;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::{sync::watch, time::Instant};
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Terminal {
    CompletedWithAudio,
    CompletedWithoutAudio,
    CompletedWithInterruption,
    Cancelled,
    PartialFailure,
    Failed,
}
#[derive(Clone, Debug)]
pub struct UtteranceReport {
    pub context_id: String,
    pub progress: Progress,
    pub transport: Option<TransportReport>,
    pub pipeline_joined: bool,
    pub pipeline_failed: bool,
    pub output_cleaned: bool,
    pub output_cleanup_failed: bool,
}
impl UtteranceReport {
    pub fn is_success(&self) -> bool {
        let p = &self.progress;
        let transport_ok = if p.skipped {
            self.transport.is_none()
        } else {
            self.transport.as_ref().is_some_and(|t| {
                t.writer_joined
                    && t.reader_joined
                    && t.connection_joined
                    && !t.forced_abort
                    && t.cleanup.is_none()
                    && (t.primary.is_none()
                        || (p.interrupted && t.primary == Some(TransportError::Cancelled)))
            })
        };
        self.pipeline_joined
            && !self.pipeline_failed
            && self.output_cleaned
            && !self.output_cleanup_failed
            && p.primary.is_none()
            && p.response_task_returned
            && transport_ok
            && (p.skipped || p.interrupted || p.speech_completed)
    }
}
#[derive(Clone, Debug)]
pub struct Report {
    pub terminal: Terminal,
    pub utterances: Vec<UtteranceReport>,
    pub primary: Option<Error>,
    pub manager_joined: bool,
}
impl Report {
    pub fn is_success(&self) -> bool {
        self.manager_joined
            && self.primary.is_none()
            && matches!(
                self.terminal,
                Terminal::CompletedWithAudio
                    | Terminal::CompletedWithoutAudio
                    | Terminal::CompletedWithInterruption
            )
    }
}
struct State {
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<Report>>>,
    report: Mutex<Option<Report>>,
    control: watch::Sender<(u64, u8)>,
    active: Mutex<Option<Arc<Shared>>>,
}
#[derive(Clone)]
pub struct Completion(Arc<State>);
impl Completion {
    pub fn progress(&self) -> Option<Progress> {
        self.0
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.progress.borrow().clone())
    }
    pub async fn wait(&self) -> Report {
        let mut task = self.0.task.lock().await;
        if let Some(handle) = task.as_mut() {
            let report = match handle.await {
                Ok(mut r) => {
                    r.manager_joined = true;
                    r
                }
                Err(_) => Report {
                    terminal: Terminal::Failed,
                    utterances: Vec::new(),
                    primary: Some(Error::Task),
                    manager_joined: false,
                },
            };
            *self.0.report.lock().unwrap_or_else(|e| e.into_inner()) = Some(report);
            task.take();
        }
        self.0
            .report
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .expect("retained completion report")
    }
}
pub struct RunningApp {
    completion: Completion,
}
impl RunningApp {
    pub fn completion(&self) -> Completion {
        self.completion.clone()
    }
    pub fn interrupt(&self) {
        // Immediate local audio fence, then the manager routes a genuine native
        // Interruption frame. Never interrupts a future not-yet-active context.
        if let Some(s) = self
            .completion
            .0
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            s.interrupt();
            self.completion.0.control.send_modify(|v| {
                if v.1 < 2 {
                    v.0 += 1;
                    v.1 = 1;
                }
            });
        }
    }
    fn terminal_control(&self, code: u8) {
        let active = self
            .completion
            .0
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(s) = active.as_ref() {
            s.cancel();
        }
        self.completion.0.control.send_modify(|v| {
            v.0 += 1;
            v.1 = code;
        });
        drop(active);
    }
    pub fn cancel(&self) {
        self.terminal_control(2);
    }
    pub fn stop(&self) {
        self.terminal_control(3);
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
/// All text and wire budgets are validated before output setup or sockets.
/// Each utterance gets its own native pipeline and owned H2 session, with real
/// joins between utterances. The caller output is setup/cleaned per utterance.
pub struct SageMakerBidiTtsApp {
    config: SageMakerBidiTtsConfig,
    texts: Vec<String>,
    output: Arc<dyn FrameProcessor>,
}
impl SageMakerBidiTtsApp {
    pub fn new(
        config: SageMakerBidiTtsConfig,
        texts: Vec<String>,
        output: Arc<dyn FrameProcessor>,
    ) -> Result<Self, Error> {
        config.validate()?;
        if config.origin.is_none() && config.address.is_none() {
            return Err(Error::Invalid("explicit hosted route required"));
        }
        if texts.is_empty() || texts.len() > config.limits.max_utterances {
            return Err(Error::Invalid("finite utterance count"));
        }
        for text in &texts {
            crate::protocol::startup(text, &config.settings, &config.limits)?;
        }
        crate::protocol::control("session.end", &config.limits)?;
        crate::protocol::control("input_text.done", &config.limits)?;
        Ok(Self {
            config,
            texts,
            output,
        })
    }
    pub fn start(self) -> Result<RunningApp, Error> {
        let state = Arc::new(State {
            task: tokio::sync::Mutex::new(None),
            report: Mutex::new(None),
            control: watch::channel((0, 0)).0,
            active: Mutex::new(None),
        });
        let owner = state.clone();
        let task = tokio::spawn(async move {
            let deadline = Instant::now() + self.config.limits.app_timeout;
            let mut reports = Vec::new();
            let mut primary = None;
            let mut delivered = 0u64;
            let mut controls = owner.control.subscribe();
            for (index, text) in self.texts.into_iter().enumerate() {
                if controls.borrow().1 >= 2 {
                    primary = Some(Error::Cancelled);
                    break;
                }
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    primary = Some(Error::Deadline);
                    break;
                }
                let mut config = self.config.clone();
                config.limits.duplex.session_timeout =
                    config.limits.duplex.session_timeout.min(left);
                for value in [
                    &mut config.limits.duplex.connect_timeout,
                    &mut config.limits.duplex.write_timeout,
                    &mut config.limits.duplex.idle_timeout,
                    &mut config.limits.duplex.consumer_timeout,
                    &mut config.limits.duplex.close_timeout,
                    &mut config.limits.output_timeout,
                    &mut config.limits.interruption_grace,
                ] {
                    *value = (*value).min(left);
                }
                let remaining =
                    (self.config.limits.max_output_bytes as u64).saturating_sub(delivered);
                if remaining == 0 {
                    primary = Some(Error::TooLarge);
                    break;
                }
                config.limits.max_output_bytes = remaining as usize;
                let id = format!("sagemaker-bidi-{}", index + 1);
                let provider = match SageMakerBidiTtsProcessor::new(config.clone(), id.clone()) {
                    Ok(p) => Arc::new(p),
                    Err(e) => {
                        primary = Some(e);
                        break;
                    }
                };
                let shared = provider.shared.clone();
                let output = Arc::new(OutputGuard {
                    inner: self.output.clone(),
                    shared: shared.clone(),
                    timeout: config.limits.output_timeout,
                    cleaned: AtomicBool::new(false),
                    cleanup_failed: AtomicBool::new(false),
                });
                // Clear old interruption notification before exposing the new context.
                {
                    let mut active = owner.active.lock().unwrap_or_else(|e| e.into_inner());
                    if controls.borrow_and_update().1 >= 2 {
                        primary = Some(Error::Cancelled);
                        break;
                    }
                    *active = Some(shared.clone());
                }
                let report = session(
                    config,
                    text,
                    id,
                    provider,
                    output,
                    shared,
                    &mut controls,
                    deadline,
                )
                .await;
                *owner.active.lock().unwrap_or_else(|e| e.into_inner()) = None;
                delivered += report.progress.pcm_bytes_delivered;
                let success = report.is_success();
                if !success {
                    primary = report
                        .progress
                        .primary
                        .clone()
                        .or_else(|| {
                            report
                                .transport
                                .as_ref()
                                .and_then(|r| r.primary.clone().or(r.cleanup.clone()))
                                .map(Error::Transport)
                        })
                        .or(Some(Error::Task));
                }
                reports.push(report);
                if !success {
                    break;
                }
            }
            let cancelled = owner.control.borrow().1 >= 2;
            let terminal = if cancelled {
                Terminal::Cancelled
            } else if primary.is_some() {
                if delivered > 0 {
                    Terminal::PartialFailure
                } else {
                    Terminal::Failed
                }
            } else if reports.iter().any(|r| r.progress.interrupted) {
                Terminal::CompletedWithInterruption
            } else if delivered > 0 {
                Terminal::CompletedWithAudio
            } else {
                Terminal::CompletedWithoutAudio
            };
            Report {
                terminal,
                utterances: reports,
                primary,
                manager_joined: false,
            }
        });
        // No await occurs between spawn and retaining the real handle.
        *state.task.try_lock().map_err(|_| Error::Task)? = Some(task);
        Ok(RunningApp {
            completion: Completion(state),
        })
    }
}
#[allow(clippy::too_many_arguments)]
async fn session(
    config: SageMakerBidiTtsConfig,
    text: String,
    id: String,
    provider: Arc<SageMakerBidiTtsProcessor>,
    output: Arc<OutputGuard>,
    shared: Arc<Shared>,
    controls: &mut watch::Receiver<(u64, u8)>,
    deadline: Instant,
) -> UtteranceReport {
    let pipeline = Pipeline::new(vec![provider, output.clone()]);
    let pc = PipelineConfig {
        queue_capacity: 4,
        output_capacity: 4,
        max_background_tasks: 2,
        shutdown_timeout: config.limits.duplex.session_timeout
            + config.limits.duplex.close_timeout * 3
            + config.limits.output_timeout * 3,
        startup_timeout: config.limits.output_timeout,
        processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
        start: StartFrame {
            audio_out_sample_rate: config.settings.sample_rate_hz,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut pipeline_joined = false;
    let mut pipeline_failed = true;
    match pipeline.start(pc).await {
        Ok(mut worker) => {
            let sender = worker.sender();
            let mut seen = shared.progress.subscribe();
            let feed = async {
                sender
                    .send(
                        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new(text))),
                        Direction::Downstream,
                    )
                    .await
                    .map_err(|_| Error::Task)?;
                loop {
                    let p = seen.borrow_and_update().clone();
                    if p.response_task_returned {
                        break;
                    }
                    if let Some(e) = p.primary {
                        return Err(e);
                    }
                    seen.changed().await.map_err(|_| Error::Task)?;
                }
                sender.end().await.map_err(|_| Error::Task)
            };
            tokio::pin!(feed);
            let mut feed_done = false;
            let mut timing = true;
            loop {
                tokio::select! {biased;
                    changed=controls.changed()=>{
                        let code=if changed.is_err(){2}else{controls.borrow_and_update().1};
                        if code==1{shared.interrupt();let _=sender.send(Frame::interruption(),Direction::Downstream).await;}
                        else if code>=2{shared.cancel();feed_done=true;if code==3{let _=sender.send(Frame::new(FrameData::Stop),Direction::Downstream).await;}else{let _=sender.cancel("finite TTS cancellation").await;}}
                    },
                    _=tokio::time::sleep_until(deadline),if timing=>{timing=false;feed_done=true;shared.fail(Error::Deadline);let _=sender.cancel("finite TTS deadline").await;},
                    result=&mut feed,if !feed_done=>{feed_done=true;if let Err(e)=result{shared.fail(e);let _=sender.cancel("finite TTS admission failed").await;}},
                    frame=worker.recv()=>if frame.is_none(){break;},
                }
            }
            let joined = worker.join().await;
            pipeline_joined = !matches!(
                joined,
                Err(PipelineError::ProcessorPanic(_) | PipelineError::AlreadyJoined)
            );
            pipeline_failed = joined.is_err()
                || worker.total_errors() != 0
                || worker.state() != WorkerState::Ended;
        }
        _ => shared.fail(Error::Task),
    }
    if !shared.progress.borrow().response_task_returned {
        shared.cancel();
    }
    let transport = shared.transport_report().await;
    let _ = output.cleanup().await;
    let progress = shared.progress.borrow().clone();
    UtteranceReport {
        context_id: id,
        progress,
        transport,
        pipeline_joined,
        pipeline_failed,
        output_cleaned: output.cleaned.load(Ordering::Acquire),
        output_cleanup_failed: output.cleanup_failed.load(Ordering::Acquire),
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
                Err(ProcessorError::fatal("finite TTS output failed"))
            }
        }
    }
}
#[async_trait]
impl FrameProcessor for OutputGuard {
    fn name(&self) -> &str {
        "FiniteBidiTtsOutputGuard"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.call(self.inner.setup(start, ctx)).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let audio_bytes = if let FrameData::TtsAudio(a) = &frame.data {
            a.audio.len()
        } else {
            0
        };
        let contextual = matches!(
            &frame.data,
            FrameData::TtsAudio(_) | FrameData::TtsStarted(_) | FrameData::TtsStopped(_)
        );
        if contextual {
            let mut control = self.shared.control.subscribe();
            if *control.borrow() != 0 || !ctx.is_current() {
                return Ok(());
            }
            tokio::select! {biased;
                _=changed_control(&mut control)=>return Ok(()),
                result=self.call(self.inner.process(frame,direction,ctx))=>result?,
            }
            self.shared
                .progress
                .send_modify(|p| p.pcm_bytes_delivered += audio_bytes as u64);
            Ok(())
        } else {
            self.call(self.inner.process(frame, direction, ctx)).await
        }
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
