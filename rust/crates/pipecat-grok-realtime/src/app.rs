//! Finite, opt-in, generated-history Grok PCM application.
//!
//! One standard realtime pair owns inference scheduling. The application owns
//! admission and causal shutdown, but never invents tool outcomes or continuation
//! frames. The trusted output stage must forward real flow flushes after its own
//! preceding media is delivered; recording is not evidence of audible playback.
use crate::{app_state::*, *};
use pipecat_core::{
    async_trait, AudioRawFrame, ChatMessage, ChatRole, Direction, Frame, FrameData, FrameProcessor,
    LocalFlushEvent, MessagesFrame, Pipeline, PipelineConfig, PipelineFlushFrame, PipelineSender,
    ProcessorContext, ProcessorError, StartFrame, WorkerState,
};
use pipecat_processors::tools::ToolExecutor;
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, RealtimeServiceMode,
    UserAggregatorConfig,
};
use std::{
    fmt, mem,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{oneshot, watch, Mutex as AsyncMutex},
    task::JoinHandle,
    time::{timeout, Instant},
};

/// Terminal network/pipeline disposition, without raw provider or callback text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrokAppTermination {
    Ended,
    Cancelled,
    Failed,
}
/// Sanitized native pipeline failure classification, without processor text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrokPipelineFailure {
    Configuration,
    StartupTimeout,
    ShutdownTimeout,
    Closed,
    Admission,
    Panic,
    Execution,
    JoinUnavailable,
}
impl GrokPipelineFailure {
    fn classify(error: &pipecat_core::PipelineError) -> Self {
        use pipecat_core::PipelineError as E;
        match error {
            E::InvalidConfiguration(_) => Self::Configuration,
            E::StartupTimeout => Self::StartupTimeout,
            E::ShutdownTimeout => Self::ShutdownTimeout,
            E::Closed => Self::Closed,
            E::DuplicateStart | E::TerminalUpstream | E::Backpressure(_) => Self::Admission,
            E::ProcessorPanic(_) => Self::Panic,
            E::Execution(_) => Self::Execution,
            E::AlreadyJoined => Self::JoinUnavailable,
        }
    }
}
/// Shared pipeline cleanup detail can be masked by an earlier primary failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrokPipelineCleanup {
    Clean,
    ForcedTimeout,
    /// Owners were joined, but the shared API does not expose every secondary abort.
    UnknownAfterFailure,
}
/// Bounded terminal diagnostics. Counts are local observations, not remote effect
/// acknowledgments. Successful recording persistence is an integration concern.
#[derive(Clone, Debug)]
pub struct GrokAppReport {
    pub termination: GrokAppTermination,
    pub primary: Option<GrokAppError>,
    pub secondary_failure: bool,
    pub first_failure_stage: Option<GrokAppFailureStage>,
    pub mapping_primary: Option<RealtimeError>,
    pub cleanup_output_failed: bool,
    pub drain_gates: GrokAppDrainGates,
    pub provider_primary: Option<RealtimeError>,
    pub provider_cleanup: Option<RealtimeError>,
    /// True when a forced actor or pipeline timeout is positively observed.
    /// False does not imply no forced pipeline cleanup; inspect pipeline_cleanup.
    pub forced_cleanup: bool,
    pub pipeline_failure: Option<GrokPipelineFailure>,
    pub pipeline_cleanup: GrokPipelineCleanup,
    pub pipeline_joined: bool,
    pub provider_joined: bool,
    pub generated_speech_responses: u64,
    pub provider_events_emitted: u64,
    pub provider_events_mapped: u64,
    pub pending_frames: usize,
    pub pending_aggregator_invocations: usize,
    pub mapped_tool_calls: usize,
    pub settled_tool_calls: usize,
    pub output_bytes: usize,
    joined: bool,
}
impl GrokAppReport {
    pub fn successful(&self) -> bool {
        self.termination == GrokAppTermination::Ended
            && self.primary.is_none()
            && !self.secondary_failure
            && !self.forced_cleanup
            && self.joined
    }
    pub fn owners_joined(&self) -> bool {
        self.joined
    }
}
#[derive(Clone)]
enum Control {
    Running,
    End,
}
struct CompletionOwner {
    task: AsyncMutex<Option<JoinHandle<GrokAppReport>>>,
    report: Mutex<Option<Result<GrokAppReport, GrokAppError>>>,
    state: Arc<AppState>,
    history: pipecat_turns::SharedContext,
    outcomes: pipecat_processors::tools::ToolOutcomeJournal,
}
/// Retain before starting if cancellation or dropping the running handle must be
/// observed. A cancelled waiter never takes/detaches the manager JoinHandle.
#[derive(Clone)]
pub struct GrokAppCompletion(Arc<CompletionOwner>);
impl fmt::Debug for GrokAppCompletion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrokAppCompletion").finish_non_exhaustive()
    }
}
impl GrokAppCompletion {
    /// A bounded, read-only clone. Mutating the returned value cannot change the
    /// live standard pair. Contains conversation data; not included in reports.
    pub fn history(&self) -> Result<pipecat_core::LlmContextFrame, GrokAppError> {
        self.0
            .history
            .snapshot()
            .and_then(|context| pipecat_turns::context_to_frame(&context))
            .map_err(|_| GrokAppError::Failed("history snapshot"))
    }
    /// Genuine native lease receipts, not provider acknowledgment or effect proof.
    pub fn outcomes(&self) -> pipecat_processors::tools::ToolOutcomeSnapshot {
        self.0.outcomes.snapshot()
    }
    pub async fn wait(&self) -> Result<GrokAppReport, GrokAppError> {
        let mut slot = self.0.task.lock().await;
        #[cfg(test)]
        if let Some(hooks) = self.0.state.test_hooks() {
            hooks.completion_wait.pause(&self.0.state).await;
        }
        if let Some(task) = slot.as_mut() {
            let result = (&mut *task)
                .await
                .map_err(|_| GrokAppError::Failed("application owner panic"));
            *self.0.report.lock().unwrap_or_else(|e| e.into_inner()) = Some(result.clone());
            slot.take();
            return result;
        }
        self.0
            .report
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or(Err(GrokAppError::Invalid("application not started")))
    }
}
struct CancelOnDrop {
    state: Arc<AppState>,
    armed: bool,
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.state.cancel();
        }
    }
}

/// Prepared single-use application. Construction validates and allocates only;
/// it does not connect, invoke callbacks, or initialize the output stage.
pub struct GrokRealtimeApp {
    config: GrokAppConfig,
    pipeline: Option<Pipeline>,
    provider: Arc<GrokRealtimeProcessor>,
    user: Arc<AggregatorBoundary>,
    user_tap: Arc<ContextTap>,
    assistant: Arc<AggregatorBoundary>,
    assistant_tap: Arc<ContextTap>,
    output: Arc<BoundedOutput>,
    completion: GrokAppCompletion,
}
impl GrokRealtimeApp {
    #[cfg(test)]
    pub(crate) fn set_test_hooks(self, hooks: Arc<crate::app_tests::TestHooks>) -> Self {
        self.completion.0.state.install_test_hooks(hooks);
        self
    }
    pub fn new(
        mut config: GrokAppConfig,
        mut seed: Vec<ChatMessage>,
        mut tools: Vec<GrokLocalTool>,
        output: Arc<dyn FrameProcessor>,
    ) -> Result<Self, GrokAppError> {
        let outer_bytes = tools
            .capacity()
            .checked_mul(mem::size_of::<GrokLocalTool>());
        if outer_bytes.is_none_or(|bytes| bytes > config.limits.max_retained_bytes) {
            crate::app_config::discard_invalid_inputs(
                &mut config.connection.session,
                &mut seed,
                &mut tools,
            );
            return Err(GrokAppError::Invalid("tool vector retained capacity"));
        }
        // Charge spare allocation separately, then compact before retaining it.
        let spare =
            (tools.capacity() - tools.len()).saturating_mul(mem::size_of::<GrokLocalTool>());
        let original_limit = config.limits.max_retained_bytes;
        config.limits.max_retained_bytes = original_limit
            .checked_sub(spare)
            .ok_or(GrokAppError::Invalid("tool vector retained capacity"))?;
        let validated = config.validate_owned(&mut seed, &mut tools);
        config.limits.max_retained_bytes = original_limit;
        validated?;
        tools.shrink_to_fit();
        let records = config
            .connection
            .limits
            .max_session_tool_calls
            .saturating_mul(8)
            .saturating_add(config.limits.queue_capacity.saturating_mul(32));
        let state = AppState::new(records, config.limits.max_tool_result_bytes);
        state
            .configure_history_limits(
                &config.limits,
                serde_json::to_vec(&seed)
                    .map_err(|_| GrokAppError::Invalid("seed representation"))?
                    .len(),
                seed.len(),
            )
            .map_err(|_| GrokAppError::Invalid("history bounds"))?;
        let context = pipecat_services::Context::new(
            seed.iter()
                .map(pipecat_services::frame_context::legacy_message)
                .collect(),
        );
        let pair = LLMContextAggregatorPair::with_realtime_mode(
            context,
            UserAggregatorConfig {
                empty_user_turn: None,
                eager: None,
                user_idle_timeout: Duration::ZERO,
                ..UserAggregatorConfig::default()
            },
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Text,
            },
            RealtimeServiceMode::Enabled,
        )
        .map_err(|_| GrokAppError::Invalid("standard realtime pair"))?;
        let client = RealtimeClient::new(config.connection.clone())
            .map_err(|_| GrokAppError::Invalid("connection"))?
            .for_app(state.clone(), config.resolved_address);
        let provider = Arc::new(
            GrokRealtimeProcessor::new(client)
                .map_err(|_| GrokAppError::Invalid("PCM provider"))?
                .for_app(state.clone()),
        );
        state
            .bind_provider_health(provider.health_handle())
            .map_err(|_| GrokAppError::Invalid("provider health binding"))?;
        let relay = Arc::new(provider.tool_result_relay());
        let mut executor = ToolExecutor::new(
            config.connection.limits.max_outstanding_tools,
            config.connection.limits.max_session_tool_calls,
            config.limits.max_tool_argument_bytes,
        )
        .map_err(|_| GrokAppError::Invalid("native executor limits"))?;
        for tool in tools {
            executor
                .register(
                    tool.schema.name,
                    wrap_final_tool(tool.handler, state.clone()),
                )
                .map_err(|_| GrokAppError::Invalid("native tool binding"))?;
        }
        let completion = GrokAppCompletion(Arc::new(CompletionOwner {
            task: AsyncMutex::new(None),
            report: Mutex::new(None),
            state: state.clone(),
            history: pair.context(),
            outcomes: executor.outcomes(),
        }));
        let user = Arc::new(AggregatorBoundary::user(pair.user(), state.clone()));
        let user_tap = Arc::new(ContextTap::user(state.clone()));
        let assistant = Arc::new(AggregatorBoundary::assistant(
            pair.assistant(),
            state.clone(),
        ));
        let assistant_tap = Arc::new(ContextTap::assistant(state.clone()));
        let output = Arc::new(BoundedOutput {
            inner: Arc::new(OutputBoundary::new(output, state.clone())),
            state: state.clone(),
            bytes: Mutex::new(0),
            limit: config.limits.max_output_bytes,
        });
        let stages: Vec<Arc<dyn FrameProcessor>> = vec![
            user.clone(),
            user_tap.clone(),
            provider.clone(),
            Arc::new(GuardedExecutor::new(executor, state)),
            relay,
            output.clone(),
            assistant_tap.clone(),
            assistant.clone(),
        ];
        Ok(Self {
            config,
            pipeline: Some(Pipeline::new(stages)),
            provider,
            user,
            user_tap,
            assistant,
            assistant_tap,
            output,
            completion,
        })
    }
    pub fn completion(&self) -> GrokAppCompletion {
        self.completion.clone()
    }
    pub async fn start(self) -> Result<GrokRunningApp, GrokAppError> {
        let completion = self.completion.clone();
        let state = completion.0.state.clone();
        let mut cancellation = CancelOnDrop {
            state: state.clone(),
            armed: true,
        };
        let limits = self.config.limits.clone();
        let input_source = self.user.clone();
        let (control, changes) = watch::channel(Control::Running);
        let (ready_tx, ready_rx) = oneshot::channel();
        // The manager retains the completion owner until its own body ends. Dropping
        // every API handle signals cancellation, not an abort of startup or cleanup.
        // A completion observer is public before start. Acquire its slot before
        // spawning so a concurrent/cancelled waiter can never strand an unpublished
        // JoinHandle. There is no suspension point between spawn and publication.
        let mut slot = completion.0.task.lock().await;
        let retention = completion.clone();
        let task = tokio::spawn(async move {
            let _retained = retention;
            self.manage(changes, ready_tx).await
        });
        *slot = Some(task);
        drop(slot);
        match ready_rx.await {
            Ok(Ok(sender)) => {
                cancellation.armed = false;
                Ok(GrokRunningApp {
                    sender,
                    control,
                    completion,
                    input: AsyncMutex::new(InputAdmission::default()),
                    sealed: AtomicBool::new(false),
                    interruptions: AtomicUsize::new(0),
                    control_admission: Mutex::new(()),
                    input_source,
                    limits,
                })
            }
            _ => {
                state.cancel();
                let report = completion.wait().await?;
                Err(report.primary.unwrap_or(GrokAppError::Failed("startup")))
            }
        }
    }
    async fn manage(
        mut self,
        mut control: watch::Receiver<Control>,
        ready: oneshot::Sender<Result<PipelineSender, GrokAppError>>,
    ) -> GrokAppReport {
        let state = self.completion.0.state.clone();
        let limits = &self.config.limits;
        let mut cfg = PipelineConfig {
            queue_capacity: limits.queue_capacity,
            system_queue_capacity: limits.queue_capacity,
            output_capacity: limits.output_event_capacity,
            error_history_capacity: limits.output_event_capacity,
            max_background_tasks: self
                .config
                .connection
                .limits
                .max_outstanding_tools
                .saturating_add(16),
            startup_timeout: self
                .config
                .connection
                .limits
                .connect_timeout
                .saturating_add(self.config.connection.limits.operation_timeout)
                .min(limits.session_timeout),
            shutdown_timeout: limits.shutdown_timeout,
            ..PipelineConfig::default()
        };
        cfg.start.audio_in_sample_rate = 24_000;
        cfg.start.audio_out_sample_rate = 24_000;
        cfg.processor_unusable_policy = pipecat_core::ProcessorUnusablePolicy::Cancel;
        let deadline = Instant::now() + limits.session_timeout;
        // Never select/drop Pipeline::start: its startup cleanup owner must return.
        let pipeline = self.pipeline.take().expect("single application startup");
        let starting = pipeline.start(cfg);
        tokio::pin!(starting);
        let mut startup_timed_out = false;
        let started = tokio::select! { biased;
            _ = tokio::time::sleep_until(deadline) => { startup_timed_out = true; state.cancel(); starting.await }
            started = &mut starting => started,
        };
        let mut primary;
        let mut secondary = false;
        let mut pipeline_joined = true;
        let mut pipeline_failure = None;
        let mut pipeline_cleanup = GrokPipelineCleanup::Clean;
        let mut ended = false;
        if let Ok(mut worker) = started {
            let sender = worker.sender();
            let bootstrap = async {
                sender
                    .send(
                        state
                            .admit_input(Frame::new(FrameData::LlmMessagesAppend(MessagesFrame {
                                messages: vec![],
                                run_llm: false,
                            })))
                            .map_err(|_| GrokAppError::Failed("initial input integrity"))?,
                        Direction::Downstream,
                    )
                    .await
                    .map_err(|_| GrokAppError::Failed("initial context admission"))?;
                let (cut, event) = state
                    .make_input_cut()
                    .map_err(|_| GrokAppError::Failed("initial context cut"))?;
                sender
                    .send(cut, Direction::Downstream)
                    .await
                    .map_err(|_| GrokAppError::Failed("initial context cut admission"))?;
                event.wait().await;
                self.provider
                    .sender()
                    .map_err(|_| GrokAppError::Failed("provider readiness"))?
                    .app_checkpoint()
                    .await
                    .map_err(|_| GrokAppError::Failed("initial context command cut"))?;
                Ok::<(), GrokAppError>(())
            };
            tokio::pin!(bootstrap);
            let startup = loop {
                tokio::select! { biased;
                    _ = state.cancelled() => break Err(GrokAppError::Cancelled),
                    _ = tokio::time::sleep_until(deadline) => break Err(GrokAppError::Timeout("session startup")),
                    result = &mut bootstrap => break result,
                    frame = worker.recv() => if frame.is_none() || worker.total_errors() > 0 || state.snapshot().failed { break Err(GrokAppError::Failed("pipeline startup")); },
                }
            };
            if startup.is_ok() {
                let _ = ready.send(Ok(sender.clone()));
                let running = async {
                    loop {
                        if state.snapshot().failed {
                            return Err(GrokAppError::Failed("application processing"));
                        }
                        let current = control.borrow_and_update().clone();
                        if let Control::End = current {
                            return timeout(limits.drain_timeout, self.drain(&sender))
                                .await
                                .map_err(|_| GrokAppError::Timeout("causal drain"))?;
                        }
                        tokio::select! { biased;
                            _ = state.cancelled() => return Err(GrokAppError::Cancelled),
                            _ = tokio::time::sleep_until(deadline) => return Err(GrokAppError::Timeout("session")),
                            changed = control.changed() => if changed.is_err() { return Err(GrokAppError::Cancelled); },
                        }
                    }
                };
                tokio::pin!(running);
                primary = loop {
                    tokio::select! { biased;
                        _ = state.cancelled() => break Some(GrokAppError::Cancelled),
                        _ = tokio::time::sleep_until(deadline) => break Some(GrokAppError::Timeout("session")),
                        result = &mut running => { ended = result.is_ok(); break result.err(); },
                        frame = worker.recv() => if frame.is_none() || worker.total_errors() > 0 || state.snapshot().failed { break Some(GrokAppError::Failed("pipeline processing")); },
                    }
                };
            } else {
                primary = startup.err();
                let _ = ready.send(Err(primary
                    .clone()
                    .unwrap_or(GrokAppError::Failed("startup"))));
            }
            if ended && sender.end().await.is_err() {
                primary = Some(GrokAppError::Failed("pipeline End admission"));
                ended = false;
            }
            if !ended {
                state.cancel();
                if sender.cancel("Grok application stopped").await.is_err()
                    && !worker.has_finished()
                {
                    secondary = true;
                }
            }
            // Drain the bounded channel continuously until every processor cleanup
            // completes, then await the retained pipeline supervisor handle.
            while worker.recv().await.is_some() {}
            match worker.join().await {
                Ok(report) => {
                    if report.state != WorkerState::Ended && ended {
                        ended = false;
                        primary.get_or_insert(GrokAppError::Failed("pipeline terminal state"));
                    }
                    if report.total_errors > 0 {
                        pipeline_cleanup = GrokPipelineCleanup::UnknownAfterFailure;
                        secondary = true;
                        primary
                            .get_or_insert(GrokAppError::Failed("pipeline processing or cleanup"));
                    }
                }
                Err(error) => {
                    // join() awaits and takes its retained supervisor before it
                    // returns an execution/panic/timeout error. Failure is not
                    // detached ownership. Only AlreadyJoined lacks this proof.
                    pipeline_joined = error != pipecat_core::PipelineError::AlreadyJoined;
                    let failure = GrokPipelineFailure::classify(&error);
                    pipeline_cleanup = if failure == GrokPipelineFailure::ShutdownTimeout {
                        GrokPipelineCleanup::ForcedTimeout
                    } else {
                        GrokPipelineCleanup::UnknownAfterFailure
                    };
                    pipeline_failure = Some(failure);
                    secondary = true;
                    primary.get_or_insert(GrokAppError::Failed("pipeline execution or cleanup"));
                }
            }
        } else {
            let error = started.err().expect("failed startup");
            pipeline_failure = Some(GrokPipelineFailure::classify(&error));
            pipeline_cleanup = GrokPipelineCleanup::UnknownAfterFailure;
            // Closed may originate at the shared initial Start send before its
            // join path. App-owned receive starts only after Start, preventing
            // that path; an integration which violates the trusted setup
            // contract cannot be reported as proved joined.
            pipeline_joined = error != pipecat_core::PipelineError::Closed;
            primary = Some(GrokAppError::Failed("pipeline startup"));
            let _ = ready.send(Err(primary.clone().unwrap()));
        }
        if startup_timed_out {
            primary = Some(GrokAppError::Timeout("session startup"));
        }
        let mut actor_joined = true;
        let mut forced = false;
        let mut provider_primary = self.provider.app_primary();
        let mut provider_cleanup = None;
        if let Some(owner) = state.session() {
            if owner.join(limits.shutdown_timeout).await.is_err() {
                primary.get_or_insert(GrokAppError::Failed("provider session"));
            }
            actor_joined = owner.joined();
            forced = owner.forced();
            if let Some(error) = owner.primary() {
                provider_primary.get_or_insert(error);
                primary.get_or_insert(GrokAppError::Failed("provider primary"));
            }
            provider_cleanup = owner.cleanup();
            secondary |= provider_cleanup.is_some();
        }
        let snapshot = state.snapshot();
        if pipeline_joined && actor_joined {
            state.release_runtime_contexts();
        }
        if snapshot.failed {
            primary.get_or_insert(GrokAppError::Failed("application frontier"));
        }
        GrokAppReport {
            termination: if ended && primary.is_none() {
                GrokAppTermination::Ended
            } else if primary == Some(GrokAppError::Cancelled) {
                GrokAppTermination::Cancelled
            } else {
                GrokAppTermination::Failed
            },
            primary,
            secondary_failure: secondary,
            first_failure_stage: snapshot.first_failure_stage,
            mapping_primary: self.provider.app_mapping_primary(),
            cleanup_output_failed: snapshot.cleanup_output_failed,
            drain_gates: snapshot.drain_gates,
            provider_primary,
            provider_cleanup,
            forced_cleanup: forced || pipeline_cleanup == GrokPipelineCleanup::ForcedTimeout,
            pipeline_failure,
            pipeline_cleanup,
            pipeline_joined,
            provider_joined: actor_joined,
            generated_speech_responses: snapshot.speech_responses,
            provider_events_emitted: snapshot.emitted,
            provider_events_mapped: snapshot.mapped,
            pending_frames: snapshot.pending_frames,
            pending_aggregator_invocations: snapshot.pending_invocations,
            mapped_tool_calls: snapshot.mapped_calls,
            settled_tool_calls: snapshot.settled_calls,
            output_bytes: *self.output.bytes.lock().unwrap_or_else(|e| e.into_inner()),
            joined: pipeline_joined && actor_joined,
        }
    }
    async fn drain(&self, sender: &PipelineSender) -> Result<(), GrokAppError> {
        let state = &self.completion.0.state;
        let (cut, event) = state
            .make_input_cut()
            .map_err(|_| GrokAppError::Failed("input seal"))?;
        sender
            .send(cut, Direction::Downstream)
            .await
            .map_err(|_| GrokAppError::Failed("input seal admission"))?;
        event.wait().await;
        let provider = self
            .provider
            .sender()
            .map_err(|_| GrokAppError::Failed("drain provider"))?;
        loop {
            let mut changes = state.subscribe();
            let before = provider
                .app_checkpoint()
                .await
                .map_err(|_| GrokAppError::Failed("actor checkpoint"))?;
            state
                .wait_mapped(before.emitted)
                .await
                .map_err(|_| GrokAppError::Failed("receive event cut"))?;
            let flushed = LocalFlushEvent::default();
            sender
                .send(
                    state
                        .admit_input(
                            Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                                event: Some(flushed.clone()),
                                returning: false,
                                origin: Some("pipecat.flows".into()),
                            }))
                            .with_interruptible(false),
                        )
                        .map_err(|_| GrokAppError::Failed("media input integrity"))?,
                    Direction::Downstream,
                )
                .await
                .map_err(|_| GrokAppError::Failed("media fence admission"))?;
            flushed.wait().await;
            state
                .note_output_flushed(before.emitted)
                .map_err(|_| GrokAppError::Failed("output media debt"))?;
            for boundary in [&self.user, &self.assistant] {
                let id = boundary
                    .begin_cut()
                    .await
                    .map_err(|_| GrokAppError::Failed("aggregator cut"))?;
                state
                    .wait_cut(id)
                    .await
                    .map_err(|_| GrokAppError::Failed("aggregator lane cut"))?;
            }
            for tap in [&self.user_tap, &self.assistant_tap] {
                let id = tap
                    .begin_cut()
                    .await
                    .map_err(|_| GrokAppError::Failed("context cut"))?;
                state
                    .wait_cut(id)
                    .await
                    .map_err(|_| GrokAppError::Failed("context lane cut"))?;
            }
            let after = provider
                .app_checkpoint()
                .await
                .map_err(|_| GrokAppError::Failed("actor frontier"))?;
            if after.idle
                && state.frontier_clear()
                && provider
                    .app_close(after.revision, after.emitted)
                    .await
                    .map_err(|_| GrokAppError::Failed("actor close certificate"))?
            {
                return Ok(());
            }
            if state.snapshot().failed {
                return Err(GrokAppError::Failed("causal frontier"));
            }
            // Wait only for a real state transition; this is not a quiet period.
            changes
                .changed()
                .await
                .map_err(|_| GrokAppError::Failed("frontier observer"))?;
        }
    }
}
#[derive(Default)]
struct InputAdmission {
    sealed: bool,
    bytes: usize,
    messages: usize,
}
/// Typed, finite input owner. No raw sender, arbitrary frame injection, mutable
/// schemas, or shared history escapes the application.
pub struct GrokRunningApp {
    sender: PipelineSender,
    control: watch::Sender<Control>,
    completion: GrokAppCompletion,
    input: AsyncMutex<InputAdmission>,
    sealed: AtomicBool,
    interruptions: AtomicUsize,
    control_admission: Mutex<()>,
    input_source: Arc<AggregatorBoundary>,
    limits: GrokAppLimits,
}
impl GrokRunningApp {
    pub fn completion(&self) -> GrokAppCompletion {
        self.completion.clone()
    }
    pub fn history(&self) -> Result<pipecat_core::LlmContextFrame, GrokAppError> {
        self.completion.history()
    }
    pub fn outcomes(&self) -> pipecat_processors::tools::ToolOutcomeSnapshot {
        self.completion.outcomes()
    }
    pub async fn append_pcm(&self, bytes: &[u8]) -> Result<(), GrokAppError> {
        if bytes.is_empty()
            || !bytes.len().is_multiple_of(2)
            || bytes.len() > self.limits.max_audio_chunk_bytes
        {
            return Err(GrokAppError::Invalid("PCM chunk"));
        }
        let mut input = self.input.lock().await;
        self.reserve(&mut input, bytes.len())?;
        #[cfg(test)]
        if let Some(hooks) = self.completion.0.state.test_hooks() {
            hooks.input.pause(&self.completion.0.state).await;
        }
        self.sender
            .send(
                self.completion
                    .0
                    .state
                    .admit_input(Frame::new(FrameData::InputAudio(AudioRawFrame {
                        audio: bytes.to_vec(),
                        sample_rate: 24_000,
                        num_channels: 1,
                        context_id: None,
                    })))
                    .map_err(|_| GrokAppError::Failed("PCM input integrity"))?,
                Direction::Downstream,
            )
            .await
            .map_err(|_| GrokAppError::Failed("PCM admission"))
    }
    pub async fn append_text(&self, text: &str, run: bool) -> Result<(), GrokAppError> {
        if text.is_empty() || text.len() > self.limits.max_text_bytes {
            return Err(GrokAppError::Invalid("text input"));
        }
        let mut input = self.input.lock().await;
        self.reserve(&mut input, text.len())?;
        self.sender
            .send(
                self.completion
                    .0
                    .state
                    .admit_input(Frame::new(FrameData::LlmMessagesAppend(MessagesFrame {
                        messages: vec![ChatMessage {
                            role: ChatRole::User,
                            content: text.into(),
                            name: None,
                            tool_call_id: None,
                            tool_calls: vec![],
                        }],
                        run_llm: run,
                    })))
                    .map_err(|_| GrokAppError::Failed("text input integrity"))?,
                Direction::Downstream,
            )
            .await
            .map_err(|_| GrokAppError::Failed("text admission"))
    }
    pub async fn run(&self) -> Result<(), GrokAppError> {
        let mut input = self.input.lock().await;
        self.reserve(&mut input, 0)?;
        self.sender
            .send(
                self.completion
                    .0
                    .state
                    .admit_input(Frame::new(FrameData::LlmRun))
                    .map_err(|_| GrokAppError::Failed("run input integrity"))?,
                Direction::Downstream,
            )
            .await
            .map_err(|_| GrokAppError::Failed("run admission"))
    }
    fn reserve(&self, input: &mut InputAdmission, bytes: usize) -> Result<(), GrokAppError> {
        let snapshot = self.completion.0.state.snapshot();
        if self.sealed.load(Ordering::Acquire)
            || input.sealed
            || snapshot.cancelled
            || snapshot.failed
        {
            return Err(GrokAppError::Closed);
        }
        if input.messages >= self.limits.max_input_messages
            || input
                .bytes
                .checked_add(bytes)
                .is_none_or(|sum| sum > self.limits.max_input_bytes)
        {
            return Err(GrokAppError::Invalid("total input bound"));
        }
        input.messages += 1;
        input.bytes += bytes;
        Ok(())
    }
    pub async fn wait_for_speech_responses(
        &self,
        count: usize,
        limit: Duration,
    ) -> Result<(), GrokAppError> {
        if count == 0 || count > 64 || limit.is_zero() || limit > self.limits.session_timeout {
            return Err(GrokAppError::Invalid("speech completion condition"));
        }
        let state = &self.completion.0.state;
        timeout(limit, async {
            let mut changed = state.subscribe();
            loop {
                let snapshot = state.snapshot();
                if snapshot.failed {
                    return Err(GrokAppError::Failed("speech response"));
                }
                if snapshot.cancelled {
                    return Err(GrokAppError::Cancelled);
                }
                if snapshot.speech_responses >= count as u64 {
                    return Ok(());
                }
                changed.changed().await.map_err(|_| GrokAppError::Closed)?;
            }
        })
        .await
        .map_err(|_| GrokAppError::Timeout("speech response"))?
    }
    /// Interruption bypasses a backpressured normal-input operation. It does not
    /// fabricate cancellation settlement or authorize playback truncation.
    pub async fn interrupt(&self) -> Result<(), GrokAppError> {
        let _admission = self
            .control_admission
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.sealed.load(Ordering::Acquire) || self.completion.0.state.snapshot().cancelled {
            return Err(GrokAppError::Closed);
        }
        if self.interruptions.fetch_add(1, Ordering::AcqRel) >= self.limits.max_input_messages {
            return Err(GrokAppError::Invalid("interruption count"));
        }
        let source = self
            .input_source
            .input_context()
            .ok_or(GrokAppError::Closed)?;
        // Synchronous nonblocking admission shares the seal linearization mutex.
        // It cannot linger behind normal-input backpressure and cross the cut.
        self.sender
            .try_send_from(
                self.completion
                    .0
                    .state
                    .admit_input(Frame::interruption())
                    .map_err(|_| GrokAppError::Failed("interruption input integrity"))?,
                Direction::Downstream,
                &source,
            )
            .map_err(|_| GrokAppError::Failed("interruption admission"))
    }
    pub async fn end(&self) -> Result<GrokAppReport, GrokAppError> {
        {
            // Cancellation before acquiring the input lock has no admission
            // effect. Once the seal changes, manager publication is synchronous.
            let mut input = self.input.lock().await;
            let _admission = self
                .control_admission
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !input.sealed {
                self.sealed.store(true, Ordering::Release);
                input.sealed = true;
                self.control.send_replace(Control::End);
            }
        }
        self.completion.wait().await
    }
    pub async fn cancel(&self) -> Result<GrokAppReport, GrokAppError> {
        self.completion.0.state.cancel();
        self.completion.wait().await
    }
}
impl Drop for GrokRunningApp {
    fn drop(&mut self) {
        self.completion.0.state.cancel();
    }
}
struct OutputSetupObservation {
    state: Arc<AppState>,
    returned: bool,
}
impl Drop for OutputSetupObservation {
    fn drop(&mut self) {
        if !self.returned {
            // Core can abort and join a timed-out trusted setup future. Keep
            // its local stage visible without parsing a generic ErrorFrame.
            self.state
                .note_failure_stage(GrokAppFailureStage::OutputSetup);
        }
    }
}
struct BoundedOutput {
    inner: Arc<dyn FrameProcessor>,
    state: Arc<AppState>,
    bytes: Mutex<usize>,
    limit: usize,
}
#[async_trait]
impl FrameProcessor for BoundedOutput {
    fn name(&self) -> &str {
        "GrokAppBoundedOutput"
    }
    fn health(&self) -> Option<pipecat_core::ProcessorHealth> {
        self.inner.health()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut observation = OutputSetupObservation {
            state: self.state.clone(),
            returned: false,
        };
        let result = self.inner.setup(start, ctx).await;
        observation.returned = true;
        if result.is_err() {
            self.state
                .note_failure_stage(GrokAppFailureStage::OutputSetup);
        }
        result
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if let FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) = &frame.data {
                let mut bytes = self.bytes.lock().unwrap_or_else(|e| e.into_inner());
                if audio.audio.capacity() > self.limit
                    || bytes
                        .checked_add(audio.audio.len())
                        .is_none_or(|value| value > self.limit)
                {
                    self.state
                        .note_failure_stage(GrokAppFailureStage::OutputProcess);
                    return Err(ProcessorError::fatal("application output byte bound"));
                }
                *bytes += audio.audio.len();
            }
        }
        let result = self.inner.process(frame, direction, ctx).await;
        if result.is_err() {
            self.state
                .note_failure_stage(GrokAppFailureStage::OutputProcess);
        }
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let result = self.inner.prepare_end(ctx).await;
        if result.is_err() {
            self.state
                .note_failure_stage(GrokAppFailureStage::OutputPrepareEnd);
        }
        result
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let result = self.inner.cleanup().await;
        if result.is_err() {
            self.state
                .note_cleanup_failure(GrokAppFailureStage::OutputCleanup);
        }
        result
    }
}
