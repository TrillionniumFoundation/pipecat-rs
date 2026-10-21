use crate::{
    processor::Shared, protocol, Error, SageMakerClient, SageMakerConfig, SageMakerLimits,
    SageMakerProcessor, SageMakerProgress, Utterance,
};
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig,
    PipelineError, PipelineWorker, ProcessorContext, ProcessorError, ProcessorUnusablePolicy,
    StartFrame, TtsSpeakFrame, WorkerState,
};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// Finite caller-supplied complete utterances, validated in full before any pipeline/network
/// effects. No mutable history, raw frame sender or tools escape this boundary.
pub struct SageMakerTtsApp {
    config: SageMakerConfig,
    utterances: Vec<Utterance>,
    output: Arc<dyn FrameProcessor>,
}
/// Terminal local report. A failed/cancelled run can have partial caller output.
#[derive(Clone, Debug)]
pub struct SageMakerReport {
    pub state: WorkerState,
    pub progress: SageMakerProgress,
    pub pipeline_joined: bool,
    pub primary: Option<Error>,
    pub pipeline_error: Option<String>,
    pub first_pipeline_error: Option<String>,
    pub pipeline_errors: Vec<String>,
    pub cleanup_errors: Vec<String>,
    pub total_pipeline_errors: u64,
    pub evicted_pipeline_errors: u64,
    pub forced_cleanup: bool,
    pub join_unproven: bool,
}
impl SageMakerReport {
    pub fn is_success(&self) -> bool {
        self.state == WorkerState::Ended
            && self.pipeline_joined
            && self.primary.is_none()
            && self.pipeline_error.is_none()
            && self.cleanup_errors.is_empty()
            && self.total_pipeline_errors == 0
            && self.progress.admitted == self.progress.completed_at_eof
            && self.progress.admitted == self.progress.owners_released
            && self.progress.utterances_seen == self.progress.admitted + self.progress.skipped
            && self.progress.failed == 0
            && self.progress.cancelled_or_aborted == 0
            && !self.forced_cleanup
            && !self.join_unproven
    }
}
#[derive(Clone, Copy)]
enum Control {
    Run,
    Cancel,
    Stop,
}
struct CompletionState {
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<SageMakerReport>>>,
    result: Mutex<Option<SageMakerReport>>,
    control: watch::Sender<Control>,
    provider: Arc<Shared>,
}
/// Cloneable read-only completion; cancelled/concurrent waiters retain the actual
/// manager JoinHandle until it has been awaited and its result cached.
#[derive(Clone)]
pub struct SageMakerCompletion(Arc<CompletionState>);
impl SageMakerCompletion {
    pub fn progress(&self) -> SageMakerProgress {
        self.0.provider.progress.borrow().clone()
    }
    pub async fn wait(&self) -> SageMakerReport {
        let mut slot = self.0.task.lock().await;
        if let Some(task) = slot.as_mut() {
            let report = task.await.unwrap_or_else(|_| SageMakerReport {
                state: WorkerState::Failed,
                progress: self.progress(),
                pipeline_joined: false,
                primary: Some(Error::Task),
                pipeline_error: Some("application owner failed; pipeline join unproven".into()),
                first_pipeline_error: None,
                pipeline_errors: vec![],
                cleanup_errors: vec![],
                total_pipeline_errors: 0,
                evicted_pipeline_errors: 0,
                forced_cleanup: false,
                join_unproven: true,
            });
            *self.0.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(report);
            slot.take();
        }
        self.0
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .expect("completion is cached before handle removal")
            .clone()
    }
}
/// Dropping this owner cancels local work. Retain `completion()` to prove join.
pub struct SageMakerRunningApp {
    completion: SageMakerCompletion,
}
impl SageMakerRunningApp {
    pub fn completion(&self) -> SageMakerCompletion {
        self.completion.clone()
    }
    pub fn cancel(&self) {
        self.completion.0.provider.cancel();
        self.completion.0.control.send_replace(Control::Cancel);
    }
    pub fn stop(&self) {
        self.completion.0.provider.cancel();
        self.completion.0.control.send_replace(Control::Stop);
    }
    pub async fn finish(&self) -> SageMakerReport {
        self.completion.wait().await
    }
}
impl Drop for SageMakerRunningApp {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl SageMakerTtsApp {
    pub fn new(
        config: SageMakerConfig,
        utterances: Vec<Utterance>,
        output: Arc<dyn FrameProcessor>,
    ) -> Result<Self, Error> {
        config.validate()?;
        if utterances.is_empty() || utterances.len() > config.limits().max_utterances {
            return Err(Error::Invalid("finite nonempty utterance list"));
        }
        let mut total = 0usize;
        let mut pending_payload_bytes = 0usize;
        let mut normalized = Vec::with_capacity(utterances.len());
        for utterance in &utterances {
            if let Some(payload) =
                protocol::payload(&utterance.text, config.settings(), config.limits())?
            {
                pending_payload_bytes = pending_payload_bytes
                    .checked_add(payload.capacity())
                    .ok_or(Error::TooLarge)?;
                if pending_payload_bytes > config.limits().max_pending_bytes {
                    return Err(Error::TooLarge);
                }
            }
            total = total
                .checked_add(utterance.text.len())
                .ok_or(Error::TooLarge)?;
            if total > config.limits().max_pending_bytes {
                return Err(Error::TooLarge);
            }
            normalized.push(Utterance::new(utterance.text.as_str()));
        }
        Ok(Self {
            config,
            utterances: normalized,
            output,
        })
    }
    /// Starts a retained manager without an internal await. Startup itself is
    /// owned by that manager: dropping a running handle never drops Pipeline.start.
    pub async fn start(self) -> Result<SageMakerRunningApp, Error> {
        let provider = Arc::new(SageMakerProcessor::new(SageMakerClient::new(
            self.config.clone(),
        )?));
        let shared = provider.shared();
        let bounded_output = BoundedOutput::new(self.output, self.config.limits(), shared.clone());
        let diagnostics = bounded_output.diagnostics.clone();
        let pipeline = Pipeline::new(vec![
            provider.ingress_guard(),
            provider,
            bounded_output.clone(),
        ]);
        let pc = PipelineConfig {
            queue_capacity: self.config.limits().max_pending_invocations + 4,
            output_capacity: 8,
            max_background_tasks: self.config.limits().max_pending_invocations + 2,
            shutdown_timeout: (self.config.limits().total_timeout
                + self.config.limits().output_timeout * 2)
                .saturating_mul(self.utterances.len() as u32)
                + self.config.limits().shutdown_timeout * 4,
            startup_timeout: self.config.limits().output_timeout
                + self.config.limits().shutdown_timeout,
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            start: StartFrame {
                audio_out_sample_rate: self.config.settings().sample_rate_hz,
                ..Default::default()
            },
            ..Default::default()
        };
        let (control, receiver) = watch::channel(Control::Run);
        let manager_shared = shared.clone();
        let task = tokio::spawn(async move {
            // Never cancel this startup future. The caller's bounded setup callback
            // observes cancellation itself, and the manager owns all subsequent joins.
            match pipeline.start(pc).await {
                Ok(worker) => {
                    manage(
                        worker,
                        self.utterances,
                        self.config,
                        manager_shared,
                        receiver,
                        diagnostics,
                        bounded_output,
                    )
                    .await
                }
                Err(_) => SageMakerReport {
                    state: WorkerState::Failed,
                    progress: manager_shared.progress.borrow().clone(),
                    pipeline_joined: false,
                    primary: Some(Error::Task),
                    pipeline_error: Some(
                        "pipeline startup failed; join receipt unavailable".into(),
                    ),
                    first_pipeline_error: None,
                    pipeline_errors: vec![],
                    cleanup_errors: vec![],
                    total_pipeline_errors: 0,
                    evicted_pipeline_errors: 0,
                    forced_cleanup: false,
                    join_unproven: true,
                },
            }
        });
        let state = Arc::new(CompletionState {
            task: tokio::sync::Mutex::new(Some(task)),
            result: Mutex::new(None),
            control,
            provider: shared,
        });
        Ok(SageMakerRunningApp {
            completion: SageMakerCompletion(state),
        })
    }
}

async fn manage(
    mut worker: PipelineWorker,
    utterances: Vec<Utterance>,
    config: SageMakerConfig,
    shared: Arc<Shared>,
    mut control: watch::Receiver<Control>,
    diagnostics: Arc<Mutex<OutputDiagnostics>>,
    output: Arc<BoundedOutput>,
) -> SageMakerReport {
    let sender = worker.sender();
    let mut seen = shared.progress.subscribe();
    let feeding = async {
        for (index, utterance) in utterances.into_iter().enumerate() {
            loop {
                if *shared.cancel.borrow() {
                    return sender.end().await.map_err(|_| Error::Task);
                }
                let p = seen.borrow_and_update().clone();
                if p.admitted - p.owners_released < config.limits().max_pending_invocations as u64 {
                    break;
                }
                seen.changed().await.map_err(|_| Error::Task)?;
            }
            if *shared.cancel.borrow() {
                return sender.end().await.map_err(|_| Error::Task);
            }
            sender
                .send(
                    Frame::new(FrameData::TtsSpeak(TtsSpeakFrame {
                        text: utterance.text,
                        append_to_context: false,
                    })),
                    Direction::Downstream,
                )
                .await
                .map_err(|_| Error::Task)?;
            loop {
                if seen.borrow_and_update().utterances_seen > index as u64 {
                    break;
                }
                seen.changed().await.map_err(|_| Error::Task)?;
            }
        }
        sender.end().await.map_err(|_| Error::Task)
    };
    tokio::pin!(feeding);
    let mut feeding_done = false;
    let mut control_open = true;
    let mut primary = None;
    if *shared.cancel.borrow() {
        feeding_done = true;
        let command = *control.borrow_and_update();
        match command {
            Control::Stop => {
                let _ = sender
                    .send(Frame::new(FrameData::Stop), Direction::Downstream)
                    .await;
            }
            _ => {
                let _ = sender.cancel("SageMaker cancelled during startup").await;
            }
        }
    }
    loop {
        tokio::select! {
            biased;
            changed = control.changed(), if control_open => {
                if changed.is_err() { feeding_done = true; control_open = false; shared.cancel(); let _ = sender.cancel("SageMaker app owner closed").await; }
                else { let command = *control.borrow_and_update(); match command {
                    Control::Run => {},
                    Control::Cancel => { feeding_done = true; shared.cancel(); let _ = sender.cancel("SageMaker app cancelled locally").await; },
                    Control::Stop => { feeding_done = true; shared.cancel(); if sender.send(Frame::new(FrameData::Stop), Direction::Downstream).await.is_err() { let _ = sender.cancel("SageMaker Stop cancels an already staged End").await; } },
                } }
            }
            result = &mut feeding, if !feeding_done => { feeding_done = true; if let Err(error) = result { primary = Some(error); shared.cancel(); let _ = sender.cancel("SageMaker app admission failed").await; } }
            frame = worker.recv() => { if frame.is_none() { break; } }
        }
    }
    let joined = worker.join().await;
    let errors_before = worker.errors();
    let total = worker.total_errors();
    let evicted = worker.evicted_errors();
    let state = worker.state();
    if state == WorkerState::Stopped {
        // Core intentionally skips processor cleanup on Stop; this finite app owns
        // its caller output and closes it exactly once after joining that runtime.
        let _ = output.cleanup().await;
    }
    let progress = shared.progress.borrow().clone();
    let diagnostics = diagnostics.lock().unwrap_or_else(|e| e.into_inner());
    let pipeline_errors: Vec<_> = errors_before
        .into_iter()
        .map(|_| "SageMaker pipeline error".to_owned())
        .collect();
    let mut cleanup_errors = diagnostics.cleanup.clone();
    cleanup_errors.extend(
        shared
            .cleanup
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(ToString::to_string),
    );
    let forced_cleanup = diagnostics.forced_cleanup
        || matches!(progress.primary, Some(Error::Timeout("connection close")))
        || matches!(joined, Err(PipelineError::ShutdownTimeout));
    let join_unproven = matches!(joined, Err(PipelineError::ProcessorPanic(_)));
    SageMakerReport {
        state,
        primary: progress
            .primary
            .clone()
            .or(primary)
            .or_else(|| diagnostics.primary.as_ref().map(|_| Error::Output)),
        progress,
        pipeline_joined: !join_unproven,
        pipeline_error: joined
            .err()
            .map(|_| "SageMaker pipeline join failed".into()),
        first_pipeline_error: diagnostics
            .primary
            .clone()
            .or_else(|| pipeline_errors.first().cloned()),
        pipeline_errors,
        cleanup_errors,
        forced_cleanup,
        join_unproven,
        total_pipeline_errors: total,
        evicted_pipeline_errors: evicted,
    }
}

// The caller output remains the actual pipeline processor, with independently
// bounded work and cleanup. No output task or sender escapes to the caller.
#[derive(Default)]
struct OutputDiagnostics {
    primary: Option<String>,
    cleanup: Vec<String>,
    forced_cleanup: bool,
}
pub(crate) struct BoundedOutput {
    inner: Arc<dyn FrameProcessor>,
    shared: Arc<Shared>,
    process_timeout: std::time::Duration,
    cleanup_timeout: std::time::Duration,
    diagnostics: Arc<Mutex<OutputDiagnostics>>,
    cleaned: std::sync::atomic::AtomicBool,
}
impl BoundedOutput {
    pub(crate) fn new(
        inner: Arc<dyn FrameProcessor>,
        limits: &SageMakerLimits,
        shared: Arc<Shared>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            shared,
            process_timeout: limits.output_timeout,
            cleanup_timeout: limits.shutdown_timeout,
            diagnostics: Arc::new(Mutex::new(OutputDiagnostics::default())),
            cleaned: std::sync::atomic::AtomicBool::new(false),
        })
    }
    async fn call(
        &self,
        phase: &'static str,
        cleanup: bool,
        future: impl std::future::Future<Output = Result<(), ProcessorError>> + Send,
    ) -> Result<(), ProcessorError> {
        let limit = if cleanup {
            self.cleanup_timeout
        } else {
            self.process_timeout
        };
        let outcome =
            tokio::time::timeout(limit, std::panic::AssertUnwindSafe(future).catch_unwind()).await;
        let reason = match outcome {
            Ok(Ok(Ok(()))) => return Ok(()),
            Ok(Ok(Err(_))) => "failed",
            Ok(Err(_)) => "panicked",
            Err(_) => {
                if cleanup {
                    self.diagnostics
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .forced_cleanup = true;
                }
                "deadline elapsed"
            }
        };
        let message = format!("SageMaker caller output {phase} {reason}");
        let mut diagnostics = self.diagnostics.lock().unwrap_or_else(|e| e.into_inner());
        if cleanup {
            diagnostics.cleanup.push(message.clone());
        } else if diagnostics.primary.is_none() {
            diagnostics.primary = Some(message.clone());
            self.shared.progress.send_modify(|p| {
                if p.primary.is_none() {
                    p.primary = Some(Error::Output);
                }
            });
        }
        Err(ProcessorError::fatal(message))
    }
}
#[async_trait]
impl FrameProcessor for BoundedOutput {
    fn name(&self) -> &str {
        "SageMakerBoundedCallerOutput"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut cancelled = self.shared.cancel.subscribe();
        tokio::select! {biased;
            _=crate::processor::wait_cancel(&mut cancelled)=>return Err(ProcessorError::new("SageMaker output setup cancelled")),
            result=self.call("setup",false,self.inner.setup(start,ctx))=>result?,
        }
        self.shared
            .output
            .installed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let generated = direction == Direction::Downstream
            && matches!(
                frame.data,
                FrameData::TtsStarted(_) | FrameData::TtsAudio(_) | FrameData::TtsStopped(_)
            );
        let id = frame.id;
        let process = self.call("process", false, self.inner.process(frame, direction, ctx));
        let mut cancelled = self.shared.cancel.subscribe();
        let result = if generated {
            tokio::select! {
                biased;
                _ = crate::processor::wait_cancel(&mut cancelled) => {
                    // Stop is not allowed to wait for a held caller future to fail
                    // its deadline. Drop that local work without inventing an error.
                    self.shared.output.acknowledge(id, false);
                    return Ok(());
                }
                result = process => result,
            }
        } else {
            process.await
        };
        if generated {
            self.shared.output.acknowledge(id, result.is_ok());
        }
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.call("prepare-end", true, self.inner.prepare_end(ctx))
            .await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if self.cleaned.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return Ok(());
        }
        self.shared
            .output
            .installed
            .store(false, std::sync::atomic::Ordering::Release);
        self.call("cleanup", true, self.inner.cleanup()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finite_constructor_does_not_retain_spare_caller_capacities() {
        struct NoOutput;
        #[async_trait]
        impl FrameProcessor for NoOutput {
            fn name(&self) -> &str {
                "capacity-only-no-effects"
            }
            async fn process(
                &self,
                _: Frame,
                _: Direction,
                _: &ProcessorContext,
            ) -> Result<(), ProcessorError> {
                panic!("constructor must not run caller output")
            }
        }
        let mut text = String::with_capacity(1_048_576);
        text.push_str("Hi");
        let mut utterances = Vec::with_capacity(4096);
        utterances.push(Utterance::new(text));
        let mut voice = String::with_capacity(1_048_576);
        voice.push_str("voice");
        let mut language = String::with_capacity(1_048_576);
        language.push_str("en-US");
        let config = SageMakerConfig::new(
            "us-east-1",
            "test",
            pipecat_aws_common::AwsCredentials::new("KEY", "secret", None).unwrap(),
        )
        .unwrap()
        .with_loopback_endpoint("http://127.0.0.1:1")
        .unwrap()
        .with_settings(crate::SynthesisSettings {
            voice_name: voice,
            language_code: language,
            sample_rate_hz: 24_000,
        })
        .unwrap()
        .with_limits(SageMakerLimits {
            max_pending_bytes: 256,
            ..Default::default()
        })
        .unwrap();
        let app = SageMakerTtsApp::new(config, utterances, Arc::new(NoOutput)).unwrap();
        assert_eq!(app.utterances.len(), 1);
        assert_eq!(app.utterances.capacity(), 1);
        assert_eq!(app.utterances[0].text.capacity(), 2);
        let settings = app.config.settings();
        assert_eq!(settings.voice_name.capacity(), settings.voice_name.len());
        assert_eq!(
            settings.language_code.capacity(),
            settings.language_code.len()
        );
        let payload = protocol::payload(&app.utterances[0].text, settings, app.config.limits())
            .unwrap()
            .unwrap();
        assert_eq!(payload.capacity(), payload.len());
        assert!(payload.capacity() <= app.config.limits().max_pending_bytes);
    }
    #[tokio::test]
    async fn manager_join_error_never_manufactures_pipeline_join_receipt() {
        let config = SageMakerConfig::new(
            "us-east-1",
            "test",
            pipecat_aws_common::AwsCredentials::new("KEY", "secret", None).unwrap(),
        )
        .unwrap()
        .with_loopback_endpoint("http://127.0.0.1:1")
        .unwrap();
        let provider = SageMakerProcessor::new(SageMakerClient::new(config).unwrap());
        // This deliberately panicked manager owns NO pipeline. The observer must
        // still report its missing receipt as unknown, never infer a clean join.
        let task: tokio::task::JoinHandle<SageMakerReport> =
            tokio::spawn(async { panic!("synthetic manager failure") });
        let completion = SageMakerCompletion(Arc::new(CompletionState {
            task: tokio::sync::Mutex::new(Some(task)),
            result: Mutex::new(None),
            control: watch::channel(Control::Run).0,
            provider: provider.shared(),
        }));
        let report = completion.wait().await;
        assert!(!report.is_success());
        assert!(!report.pipeline_joined);
        assert!(report.join_unproven);
        assert_eq!(report.primary, Some(Error::Task));
        let repeat = completion.wait().await;
        assert!(repeat.join_unproven);
    }
}
