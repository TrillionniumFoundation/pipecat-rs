use crate::{
    default_payload, processor::Shared, AgentCoreClient, AgentCoreConfig, AgentCoreLimits,
    AgentCoreProcessor, AgentCoreProgress, Error,
};
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, DetailedLlmContextFrame, Direction, Frame, FrameData, FrameProcessor,
    LlmContextData, Pipeline, PipelineConfig, PipelineWorker, ProcessorContext, ProcessorError,
    ProcessorUnusablePolicy, StartFrame, WorkerState,
};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// Finite caller-supplied contexts, validated in full before any pipeline/network
/// effects. No mutable history, raw frame sender or tools escape this boundary.
pub struct AgentCoreTextApp {
    config: AgentCoreConfig,
    contexts: Vec<LlmContextData>,
    output: Arc<dyn FrameProcessor>,
}
/// Terminal local report. A failed/cancelled run can have partial caller output.
#[derive(Clone, Debug)]
pub struct AgentCoreReport {
    pub state: WorkerState,
    pub progress: AgentCoreProgress,
    pub pipeline_joined: bool,
    pub primary: Option<Error>,
    pub pipeline_error: Option<String>,
    pub first_pipeline_error: Option<String>,
    pub pipeline_errors: Vec<String>,
    pub cleanup_errors: Vec<String>,
    pub total_pipeline_errors: u64,
    pub evicted_pipeline_errors: u64,
}
impl AgentCoreReport {
    pub fn is_success(&self) -> bool {
        self.state == WorkerState::Ended
            && self.pipeline_joined
            && self.primary.is_none()
            && self.pipeline_error.is_none()
            && self.cleanup_errors.is_empty()
            && self.total_pipeline_errors == 0
            && self.progress.admitted == self.progress.completed_at_eof
            && self.progress.admitted == self.progress.owners_released
    }
}
#[derive(Clone, Copy)]
enum Control {
    Run,
    Cancel,
    Stop,
}
struct CompletionState {
    task: tokio::sync::Mutex<Option<tokio::task::JoinHandle<AgentCoreReport>>>,
    result: Mutex<Option<AgentCoreReport>>,
    control: watch::Sender<Control>,
    provider: Arc<Shared>,
}
/// Cloneable read-only completion; cancelled/concurrent waiters retain the actual
/// manager JoinHandle until it has been awaited and its result cached.
#[derive(Clone)]
pub struct AgentCoreCompletion(Arc<CompletionState>);
impl AgentCoreCompletion {
    pub fn progress(&self) -> AgentCoreProgress {
        self.0.provider.progress.borrow().clone()
    }
    pub async fn wait(&self) -> AgentCoreReport {
        let mut slot = self.0.task.lock().await;
        if let Some(task) = slot.as_mut() {
            let report = task.await.unwrap_or_else(|_| AgentCoreReport {
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
pub struct AgentCoreRunningApp {
    completion: AgentCoreCompletion,
}
impl AgentCoreRunningApp {
    pub fn completion(&self) -> AgentCoreCompletion {
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
    pub async fn finish(&self) -> AgentCoreReport {
        self.completion.wait().await
    }
}
impl Drop for AgentCoreRunningApp {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl AgentCoreTextApp {
    pub fn new(
        config: AgentCoreConfig,
        contexts: Vec<LlmContextData>,
        output: Arc<dyn FrameProcessor>,
    ) -> Result<Self, Error> {
        config.validate()?;
        if contexts.is_empty() || contexts.len() > config.limits().max_contexts {
            return Err(Error::Invalid("finite nonempty context list"));
        }
        let mut total = 0usize;
        for context in &contexts {
            default_payload(context, config.limits())?;
            // Validate retained context allocation without preserving caller capacity.
            let bytes = serde_json::to_vec(context).map_err(|_| Error::Invalid("context"))?;
            total = total.checked_add(bytes.len()).ok_or(Error::TooLarge)?;
            if total > config.limits().max_pending_bytes {
                return Err(Error::TooLarge);
            }
        }
        // Clone from bounded lengths, never retain excess input String/Vec capacity.
        let normalized = contexts.iter().map(Clone::clone).collect();
        Ok(Self {
            config,
            contexts: normalized,
            output,
        })
    }
    pub async fn start(self) -> Result<AgentCoreRunningApp, Error> {
        let provider = Arc::new(AgentCoreProcessor::new(AgentCoreClient::new(
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
            queue_capacity: self.config.limits().max_pending_invocations + 2,
            output_capacity: 32,
            max_background_tasks: self.config.limits().max_pending_invocations + 2,
            shutdown_timeout: self
                .config
                .limits()
                .total_timeout
                .saturating_mul(self.contexts.len() as u32)
                + self.config.limits().shutdown_timeout,
            startup_timeout: self.config.limits().connect_timeout,
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..Default::default()
        };
        let worker = pipeline.start(pc).await.map_err(|_| Error::Task)?;
        let (control, receiver) = watch::channel(Control::Run);
        let task = tokio::spawn(manage(
            worker,
            self.contexts,
            self.config,
            shared.clone(),
            receiver,
            diagnostics,
            bounded_output,
        ));
        // No cancellation point exists between spawning and retaining the owner.
        let state = Arc::new(CompletionState {
            task: tokio::sync::Mutex::new(Some(task)),
            result: Mutex::new(None),
            control,
            provider: shared,
        });
        Ok(AgentCoreRunningApp {
            completion: AgentCoreCompletion(state),
        })
    }
}
async fn manage(
    mut worker: PipelineWorker,
    contexts: Vec<LlmContextData>,
    config: AgentCoreConfig,
    shared: Arc<Shared>,
    mut control: watch::Receiver<Control>,
    diagnostics: Arc<Mutex<OutputDiagnostics>>,
    output: Arc<BoundedOutput>,
) -> AgentCoreReport {
    let sender = worker.sender();
    let mut seen = shared.progress.subscribe();
    let feeding = async {
        for (index, context) in contexts.into_iter().enumerate() {
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
                    Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
                        context,
                        speculation: false,
                    })),
                    Direction::Downstream,
                )
                .await
                .map_err(|_| Error::Task)?;
            loop {
                if seen.borrow_and_update().contexts_seen > index as u64 {
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
    loop {
        tokio::select! {
            biased;
            changed = control.changed(), if control_open => {
                if changed.is_err() { feeding_done = true; control_open = false; shared.cancel(); let _ = sender.cancel("AgentCore app owner closed").await; }
                else { let command = *control.borrow_and_update(); match command {
                    Control::Run => {},
                    Control::Cancel => { feeding_done = true; shared.cancel(); let _ = sender.cancel("AgentCore app cancelled locally").await; },
                    Control::Stop => { feeding_done = true; shared.cancel(); if sender.send(Frame::new(FrameData::Stop), Direction::Downstream).await.is_err() { let _ = sender.cancel("AgentCore Stop cancels an already staged End").await; } },
                } }
            }
            result = &mut feeding, if !feeding_done => { feeding_done = true; if let Err(error) = result { primary = Some(error); shared.cancel(); let _ = sender.cancel("AgentCore app admission failed").await; } }
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
    let pipeline_errors: Vec<_> = errors_before.into_iter().map(|e| e.error).collect();
    AgentCoreReport {
        state,
        primary: progress
            .primary
            .clone()
            .or(primary)
            .or_else(|| diagnostics.primary.as_ref().map(|_| Error::Output)),
        progress,
        pipeline_joined: true,
        pipeline_error: joined.err().map(|e| e.to_string()),
        first_pipeline_error: diagnostics
            .primary
            .clone()
            .or_else(|| pipeline_errors.first().cloned()),
        pipeline_errors,
        cleanup_errors: diagnostics.cleanup.clone(),
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
}
pub(crate) struct BoundedOutput {
    inner: Arc<dyn FrameProcessor>,
    shared: Arc<Shared>,
    process_timeout: std::time::Duration,
    cleanup_timeout: std::time::Duration,
    diagnostics: Arc<Mutex<OutputDiagnostics>>,
}
impl BoundedOutput {
    pub(crate) fn new(
        inner: Arc<dyn FrameProcessor>,
        limits: &AgentCoreLimits,
        shared: Arc<Shared>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            shared,
            process_timeout: limits.output_timeout,
            cleanup_timeout: limits.shutdown_timeout,
            diagnostics: Arc::new(Mutex::new(OutputDiagnostics::default())),
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
            Err(_) => "deadline elapsed",
        };
        let message = format!("AgentCore caller output {phase} {reason}");
        let mut diagnostics = self.diagnostics.lock().unwrap_or_else(|e| e.into_inner());
        if cleanup {
            diagnostics.cleanup.push(message.clone());
        } else if diagnostics.primary.is_none() {
            diagnostics.primary = Some(message.clone());
        }
        Err(ProcessorError::fatal(message))
    }
}
#[async_trait]
impl FrameProcessor for BoundedOutput {
    fn name(&self) -> &str {
        "AgentCoreBoundedCallerOutput"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.call("setup", false, self.inner.setup(start, ctx))
            .await?;
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
                FrameData::LlmResponseStart | FrameData::LlmText(_) | FrameData::LlmResponseEnd
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
        self.shared
            .output
            .installed
            .store(false, std::sync::atomic::Ordering::Release);
        self.call("cleanup", true, self.inner.cleanup()).await
    }
}
