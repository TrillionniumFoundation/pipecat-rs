use crate::{
    config::render_node, ActionHandler, ActionManager, ContextStrategy, Flow, FlowError, FlowSink,
    FunctionConfig, LlmAdapter, NextStep, SummaryProvider, ToolOptions, ToolResult,
};
use pipecat_core::{
    DetailedFunctionCallResultFrame, DetailedMessagesFrame, Frame, FrameData, FunctionCall,
    FunctionCallCancelFrame, FunctionCallInProgressFrame, LlmSettings, ServiceUpdateSettingsFrame,
    Setting, ToolSet,
};
use pipecat_services::CancellationToken;
use pipecat_turns::SharedContext;
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowStatus {
    New,
    Running,
    Ending,
    Ended,
    Cancelled,
}
#[derive(Clone)]
pub struct ManagerOptions {
    pub context_strategy: ContextStrategy,
    pub summary_provider: Option<Arc<dyn SummaryProvider>>,
    pub summary_timeout: Duration,
    pub max_parallel_tools: usize,
    pub tool_timeout: Option<Duration>,
}
impl Default for ManagerOptions {
    fn default() -> Self {
        Self {
            context_strategy: ContextStrategy::Append,
            summary_provider: None,
            summary_timeout: Duration::from_secs(5),
            max_parallel_tools: 32,
            tool_timeout: None,
        }
    }
}
struct ActiveCall {
    call: FunctionCall,
    cancel: CancellationToken,
    cancel_on_interruption: bool,
}
struct State {
    status: FlowStatus,
    generation: u64,
    node: Option<String>,
    functions: BTreeMap<String, FunctionConfig>,
    values: Map<String, Value>,
    node_cancel: CancellationToken,
    active: BTreeMap<String, ActiveCall>,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
    abandoned: Vec<FunctionCall>,
}
struct Inner {
    flow: Flow,
    sink: Arc<dyn FlowSink>,
    context: SharedContext,
    options: ManagerOptions,
    actions: ActionManager,
    state: Mutex<State>,
    transition: tokio::sync::Mutex<()>,
    initialization: tokio::sync::Mutex<()>,
    batch: AtomicBool,
    shutdown: CancellationToken,
}
#[derive(Clone)]
pub struct FlowManager {
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct FlowHandle {
    inner: Arc<Inner>,
    generation: u64,
    cancel: CancellationToken,
}
impl FlowHandle {
    pub fn check_active(&self) -> Result<(), FlowError> {
        if self.cancel.is_cancelled() || self.inner.shutdown.is_cancelled() {
            return Err(FlowError::Cancelled);
        }
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
        if state.generation != self.generation {
            return Err(FlowError::Stale);
        }
        if state.status != FlowStatus::Running {
            return Err(FlowError::Closed);
        }
        Ok(())
    }
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
    pub fn state(&self) -> Result<Map<String, Value>, FlowError> {
        self.check_active()?;
        Ok(self
            .inner
            .state
            .lock()
            .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?
            .values
            .clone())
    }
    pub fn set_state(&self, key: impl Into<String>, value: Value) -> Result<(), FlowError> {
        self.update_state(|state| {
            state.insert(key.into(), value);
        })
    }
    /// A synchronous critical section supports atomic read-modify-write between
    /// parallel tools. Do not call other handle methods from this closure.
    pub fn update_state<F: FnOnce(&mut Map<String, Value>)>(
        &self,
        update: F,
    ) -> Result<(), FlowError> {
        self.check_active()?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
        if state.generation != self.generation || self.cancel.is_cancelled() {
            return Err(FlowError::Stale);
        }
        update(&mut state.values);
        Ok(())
    }
    pub fn current_node(&self) -> Result<Option<String>, FlowError> {
        self.check_active()?;
        Ok(self
            .inner
            .state
            .lock()
            .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?
            .node
            .clone())
    }
    pub fn context(&self) -> SharedContext {
        self.inner.context.clone()
    }
    pub(crate) fn action_handler(&self, name: &str) -> Option<ActionHandler> {
        self.inner.flow.handlers.actions.get(name).cloned()
    }
    pub(crate) async fn cancellable<T>(
        &self,
        future: impl Future<Output = Result<T, FlowError>>,
    ) -> Result<T, FlowError> {
        self.check_active()?;
        tokio::select! { biased; _ = self.inner.shutdown.cancelled() => Err(FlowError::Cancelled), _ = self.cancel.cancelled() => Err(FlowError::Cancelled), result = future => result }
    }
    pub async fn queue_frame(&self, frame: Frame) -> Result<(), FlowError> {
        self.cancellable(self.inner.sink.queue_frame(frame)).await
    }
    pub async fn drain(&self) -> Result<(), FlowError> {
        self.cancellable(self.inner.sink.drain()).await
    }
    pub(crate) async fn end(&self) -> Result<(), FlowError> {
        self.queue_frame(Frame::end()).await?;
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
        if state.status == FlowStatus::Running {
            state.status = FlowStatus::Ending;
        }
        for call in state.active.values() {
            call.cancel.cancel();
        }
        state.node_cancel.cancel();
        self.inner.actions.clear_deferred_post_actions()?;
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ToolOutcome {
    pub call: FunctionCall,
    pub result: Value,
    pub cancelled: bool,
    pub error: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BatchOutcome {
    pub tools: Vec<ToolOutcome>,
    pub transitioned_to: Option<String>,
    pub run_llm: bool,
}
struct InitializationGuard {
    inner: Arc<Inner>,
    finished: bool,
}
impl Drop for InitializationGuard {
    fn drop(&mut self) {
        if !self.finished {
            if let Ok(mut state) = self.inner.state.lock() {
                if state.status == FlowStatus::Running {
                    state.status = FlowStatus::New;
                }
                state.node_cancel.cancel();
            }
        }
    }
}
struct BatchGuard {
    inner: Arc<Inner>,
    calls: Vec<FunctionCall>,
    finished: bool,
}
impl Drop for BatchGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.state.lock() {
            for call in &self.calls {
                if let Some(active) = state.active.remove(&call.tool_call_id) {
                    active.cancel.cancel();
                    if !self.finished {
                        state.abandoned.push(call.clone());
                    }
                }
            }
        }
        self.inner.batch.store(false, Ordering::SeqCst);
    }
}
impl FlowManager {
    pub fn new(flow: Flow, sink: Arc<dyn FlowSink>, context: SharedContext) -> Self {
        Self::with_options(flow, sink, context, ManagerOptions::default())
            .expect("default manager options are valid")
    }
    pub fn with_options(
        flow: Flow,
        sink: Arc<dyn FlowSink>,
        context: SharedContext,
        options: ManagerOptions,
    ) -> Result<Self, FlowError> {
        if options.max_parallel_tools == 0
            || options.summary_timeout.is_zero()
            || options.tool_timeout.is_some_and(|t| t.is_zero())
        {
            return Err(FlowError::Config(
                "tool limits and timeouts must be positive".into(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                flow,
                sink,
                context,
                options,
                actions: ActionManager::new(),
                state: Mutex::new(State {
                    status: FlowStatus::New,
                    generation: 0,
                    node: None,
                    functions: BTreeMap::new(),
                    values: Map::new(),
                    node_cancel: CancellationToken::new(),
                    active: BTreeMap::new(),
                    seen: HashSet::new(),
                    seen_order: VecDeque::new(),
                    abandoned: vec![],
                }),
                transition: tokio::sync::Mutex::new(()),
                initialization: tokio::sync::Mutex::new(()),
                batch: AtomicBool::new(false),
                shutdown: CancellationToken::new(),
            }),
        })
    }
    pub fn status(&self) -> FlowStatus {
        self.inner
            .state
            .lock()
            .map(|s| s.status)
            .unwrap_or(FlowStatus::Cancelled)
    }
    pub fn generation(&self) -> u64 {
        self.inner
            .state
            .lock()
            .map(|s| s.generation)
            .unwrap_or(u64::MAX)
    }
    pub fn current_node(&self) -> Option<String> {
        self.inner.state.lock().ok().and_then(|s| s.node.clone())
    }
    pub fn context(&self) -> SharedContext {
        self.inner.context.clone()
    }
    pub fn state(&self) -> Result<Map<String, Value>, FlowError> {
        Ok(self
            .inner
            .state
            .lock()
            .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?
            .values
            .clone())
    }
    pub fn set_state(&self, key: impl Into<String>, value: Value) -> Result<(), FlowError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
        if !matches!(state.status, FlowStatus::New | FlowStatus::Running) {
            return Err(FlowError::Closed);
        }
        state.values.insert(key.into(), value);
        Ok(())
    }
    pub fn action_manager(&self) -> &ActionManager {
        &self.inner.actions
    }
    pub fn register_action(
        &self,
        name: impl Into<String>,
        handler: ActionHandler,
    ) -> Result<(), FlowError> {
        self.inner.actions.register_action(name, handler)
    }
    fn handle(&self) -> Result<FlowHandle, FlowError> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
        if state.status == FlowStatus::New {
            return Err(FlowError::NotInitialized);
        }
        if state.status != FlowStatus::Running {
            return Err(FlowError::Closed);
        }
        Ok(FlowHandle {
            inner: self.inner.clone(),
            generation: state.generation,
            cancel: state.node_cancel.clone(),
        })
    }
    pub async fn initialize(&self) -> Result<(), FlowError> {
        self.initialize_at(Some(&self.inner.flow.config.initial_node))
            .await
    }
    pub async fn initialize_at(&self, initial: Option<&str>) -> Result<(), FlowError> {
        let _initialization = self.inner.initialization.lock().await;
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
            match state.status {
                FlowStatus::Running => return Ok(()),
                FlowStatus::New => state.status = FlowStatus::Running,
                _ => return Err(FlowError::Closed),
            }
        }
        let mut guard = InitializationGuard {
            inner: self.inner.clone(),
            finished: false,
        };
        if let Some(name) = initial {
            if let Err(error) = self.set_node(name).await {
                if let Ok(mut state) = self.inner.state.lock() {
                    if state.status == FlowStatus::Running {
                        state.status = FlowStatus::New;
                    }
                }
                return Err(error);
            }
        }
        guard.finished = true;
        Ok(())
    }
    pub async fn set_node(&self, name: &str) -> Result<(), FlowError> {
        self.set_node_inner(name, None, None).await
    }
    /// Deprecated Python RESET_WITH_SUMMARY behavior with actual inference.
    /// Missing, failed, empty, or timed-out summaries fall back to append.
    pub async fn set_node_with_summary(&self, name: &str, prompt: &str) -> Result<(), FlowError> {
        if prompt.is_empty() {
            return Err(FlowError::Config("summary_prompt is required".into()));
        }
        self.set_node_inner(name, Some(prompt), None).await
    }
    async fn reconcile_abandoned(&self) -> Result<(), FlowError> {
        let calls = {
            std::mem::take(
                &mut self
                    .inner
                    .state
                    .lock()
                    .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?
                    .abandoned,
            )
        };
        for call in calls {
            self.inner.sink.queue_frame(cancel_frame(&call)).await?;
        }
        Ok(())
    }
    async fn set_node_inner(
        &self,
        name: &str,
        summary_prompt: Option<&str>,
        expected: Option<u64>,
    ) -> Result<(), FlowError> {
        let _transition = self.inner.transition.lock().await;
        let old_handle = self.handle()?;
        if expected.is_some_and(|generation| generation != old_handle.generation) {
            return Err(FlowError::Stale);
        }
        let node = render_node(self.inner.flow.node(name)?.clone(), &self.state()?)?;
        self.inner.actions.prepare(&node.pre_actions, &old_handle)?;
        self.inner
            .actions
            .prepare(&node.post_actions, &old_handle)?;
        self.reconcile_abandoned().await?;
        let cancelled_calls = {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
            state.node_cancel.cancel();
            state.node_cancel = CancellationToken::new();
            state.generation += 1;
            let calls: Vec<_> = state
                .active
                .values()
                .map(|active| {
                    active.cancel.cancel();
                    active.call.clone()
                })
                .collect();
            state.active.clear();
            calls
        };
        let handle = self.handle()?;
        self.inner.actions.clear_deferred_post_actions()?;
        for call in cancelled_calls {
            handle.queue_frame(cancel_frame(&call)).await?;
        }
        self.inner
            .actions
            .execute_actions(&node.pre_actions, handle.clone())
            .await?;
        if matches!(self.status(), FlowStatus::Ending | FlowStatus::Ended) {
            return Ok(());
        }
        handle.check_active()?;
        let mut strategy = node
            .context_strategy
            .unwrap_or(self.inner.options.context_strategy);
        let mut messages = vec![];
        if let Some(prompt) = summary_prompt {
            strategy = ContextStrategy::Append;
            if let Some(provider) = &self.inner.options.summary_provider {
                let context = self.inner.context.snapshot_rich()?;
                let cancel = CancellationToken::new();
                let summary = handle
                    .cancellable(async {
                        Ok(tokio::time::timeout(
                            self.inner.options.summary_timeout,
                            provider.generate_summary(prompt, context, cancel.clone()),
                        )
                        .await)
                    })
                    .await?;
                cancel.cancel();
                if let Ok(Ok(Some(summary))) = summary {
                    if !summary.is_empty() {
                        strategy = ContextStrategy::Reset;
                        messages.push(LlmAdapter::format_summary_message(&summary));
                    }
                }
            }
        }
        messages.extend(node.task_messages.iter().map(|m| m.to_context()));
        let functions: BTreeMap<_, _> = self
            .inner
            .flow
            .config
            .global_functions
            .iter()
            .chain(&node.functions)
            .map(|f| (f.name.clone(), f.clone()))
            .collect();
        if let Some(role) = node.role_message.filter(|s| !s.is_empty()) {
            handle
                .queue_frame(
                    Frame::new(FrameData::LlmUpdateSettings(Box::new(
                        ServiceUpdateSettingsFrame {
                            delta: Some(LlmSettings {
                                system_instruction: Setting::Value(role),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )))
                    .with_interruptible(false),
                )
                .await?;
        }
        let update = DetailedMessagesFrame {
            messages,
            run_llm: Some(false),
        };
        handle
            .queue_frame(
                Frame::new(match strategy {
                    ContextStrategy::Append => FrameData::LlmMessagesAppendDetailed(update),
                    ContextStrategy::Reset => FrameData::LlmMessagesUpdateDetailed(update),
                })
                .with_interruptible(false),
            )
            .await?;
        let tools = if functions.is_empty() {
            ToolSet::NotGiven
        } else {
            ToolSet::Functions(
                self.inner
                    .flow
                    .config
                    .global_functions
                    .iter()
                    .chain(&node.functions)
                    .map(|function| self.inner.flow.schema(function))
                    .collect(),
            )
        };
        handle
            .queue_frame(Frame::new(FrameData::LlmSetTools(tools)).with_interruptible(false))
            .await?;
        handle.drain().await?;
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
            if state.generation != handle.generation || state.status != FlowStatus::Running {
                return Err(FlowError::Stale);
            }
            state.node = Some(name.into());
            state.functions = functions;
        }
        if node.respond_immediately {
            handle.queue_frame(Frame::new(FrameData::LlmRun)).await?;
            self.inner
                .actions
                .execute_post_actions(&node.post_actions, handle)
                .await?;
        } else {
            self.inner
                .actions
                .schedule_deferred_post_actions(node.post_actions)?;
        }
        Ok(())
    }
    /// Call only for a downstream bot-stop event from the current generation.
    /// Action-generated bot-stop events do not discharge deferred post-actions.
    pub async fn bot_stopped_speaking(&self) -> Result<(), FlowError> {
        self.bot_stopped_speaking_at(self.generation()).await
    }
    pub(crate) async fn bot_stopped_speaking_at(&self, generation: u64) -> Result<(), FlowError> {
        if self.inner.actions.ongoing_actions() != 0 {
            return Ok(());
        }
        let _transition = self.inner.transition.lock().await;
        let handle = self.handle()?;
        if handle.generation != generation {
            return Ok(());
        }
        let actions = self.inner.actions.take_deferred()?;
        self.inner.actions.execute_actions(&actions, handle).await
    }
    pub fn interrupt(&self) {
        if let Ok(state) = self.inner.state.lock() {
            for call in state.active.values() {
                if call.cancel_on_interruption {
                    call.cancel.cancel();
                }
            }
        }
    }
    /// Stops tools and waiting actions immediately. Also send a pipeline Cancel
    /// frame when the caller owns the pipeline lifecycle.
    pub fn cancel(&self) {
        self.inner.shutdown.cancel();
        if let Ok(mut state) = self.inner.state.lock() {
            if state.status != FlowStatus::Ended {
                state.status = FlowStatus::Cancelled;
            }
            state.generation += 1;
            state.node_cancel.cancel();
            for call in state.active.values() {
                call.cancel.cancel();
            }
        }
        let _ = self.inner.actions.clear_deferred_post_actions();
    }
    pub fn mark_ended(&self) {
        self.cancel();
        if let Ok(mut state) = self.inner.state.lock() {
            state.status = FlowStatus::Ended;
        }
    }
    pub fn observe_frame(&self, frame: &Frame) {
        self.inner.sink.observe_frame(frame);
    }
    pub async fn execute_tools(&self, calls: Vec<FunctionCall>) -> Result<BatchOutcome, FlowError> {
        self.execute_tools_inner(calls, false).await
    }
    pub(crate) async fn execute_started_tools(
        &self,
        calls: Vec<FunctionCall>,
    ) -> Result<BatchOutcome, FlowError> {
        // Retransmitted declarations must not execute or cancel a live call a
        // second time. Public execute_tools still reports duplicate admission.
        let calls: Vec<_> = {
            let state = self
                .inner
                .state
                .lock()
                .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
            calls
                .into_iter()
                .filter(|call| !state.seen.contains(&call.tool_call_id))
                .collect()
        };
        if calls.is_empty() {
            return Ok(BatchOutcome::default());
        }
        let result = self.execute_tools_inner(calls.clone(), true).await;
        if result.is_err() {
            // A rejected declaration must not leave the assistant waiting for
            // results that no handler will ever produce.
            if let Ok(handle) = self.handle() {
                for call in calls {
                    let live = self
                        .inner
                        .state
                        .lock()
                        .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?
                        .active
                        .contains_key(&call.tool_call_id);
                    if !live {
                        handle.queue_frame(cancel_frame(&call)).await?;
                    }
                }
            }
        }
        result
    }
    async fn execute_tools_inner(
        &self,
        calls: Vec<FunctionCall>,
        already_started: bool,
    ) -> Result<BatchOutcome, FlowError> {
        if calls.is_empty() {
            return Ok(BatchOutcome::default());
        }
        if calls.len() > self.inner.options.max_parallel_tools {
            return Err(FlowError::Config(
                "tool batch exceeds max_parallel_tools".into(),
            ));
        }
        let handle = self.handle()?;
        if self
            .inner
            .batch
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(FlowError::Busy);
        }
        let mut guard = BatchGuard {
            inner: self.inner.clone(),
            calls: calls.clone(),
            finished: false,
        };
        self.reconcile_abandoned().await?;
        let functions = {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
            let mut ids = HashSet::new();
            for call in &calls {
                if call.tool_call_id.is_empty()
                    || !ids.insert(&call.tool_call_id)
                    || state.seen.contains(&call.tool_call_id)
                    || !call.arguments.is_object()
                {
                    return Err(FlowError::Tool(
                        "calls require fresh unique ids and object arguments".into(),
                    ));
                }
                if !state.functions.contains_key(&call.function_name) {
                    return Err(FlowError::Tool(format!(
                        "tool '{}' is not available in the current node",
                        call.function_name
                    )));
                }
            }
            for call in &calls {
                let function = &state.functions[&call.function_name];
                let cancel_on_interruption = if function.transition_only {
                    false
                } else {
                    self.inner.flow.handlers.tools[&call.function_name]
                        .options
                        .cancel_on_interruption
                };
                state.active.insert(
                    call.tool_call_id.clone(),
                    ActiveCall {
                        call: call.clone(),
                        cancel: CancellationToken::new(),
                        cancel_on_interruption,
                    },
                );
                state.seen.insert(call.tool_call_id.clone());
                state.seen_order.push_back(call.tool_call_id.clone());
                while state.seen_order.len() > 4096 {
                    if let Some(id) = state.seen_order.pop_front() {
                        state.seen.remove(&id);
                    }
                }
            }
            state.functions.clone()
        };
        if !already_started {
            let mut frame = Frame::new(FrameData::FunctionCallsStarted(calls.clone()));
            frame
                .metadata
                .insert("pipecat.flows.managed_batch".into(), true.into());
            handle.queue_frame(frame).await?;
        }
        let mut jobs = tokio::task::JoinSet::new();
        let mut jobs_by_id = BTreeMap::new();
        for call in &calls {
            let function = functions[&call.function_name].clone();
            let options = if function.transition_only {
                ToolOptions::default()
            } else {
                self.inner.flow.handlers.tools[&call.function_name]
                    .options
                    .clone()
            };
            let cancel = {
                let state = self
                    .inner
                    .state
                    .lock()
                    .map_err(|_| FlowError::Pipeline("flow lock poisoned".into()))?;
                if state.generation != handle.generation {
                    return Err(FlowError::Stale);
                }
                state
                    .active
                    .get(&call.tool_call_id)
                    .ok_or(FlowError::Stale)?
                    .cancel
                    .clone()
            };
            handle
                .queue_frame(Frame::new(FrameData::FunctionCallInProgress(
                    FunctionCallInProgressFrame {
                        function_name: call.function_name.clone(),
                        tool_call_id: call.tool_call_id.clone(),
                        arguments: call.arguments.clone(),
                        cancel_on_interruption: options.cancel_on_interruption,
                        group_id: None,
                    },
                )))
                .await?;
            let tool_handle = FlowHandle {
                inner: self.inner.clone(),
                generation: handle.generation,
                cancel,
            };
            let this = self.clone();
            let call = call.clone();
            let failed_call = call.clone();
            let job = jobs.spawn(async move {
                let future = this.invoke(&call, &function, tool_handle.clone());
                let result = tool_handle
                    .cancellable(async {
                        match options.timeout.or(this.inner.options.tool_timeout) {
                            Some(timeout) => tokio::time::timeout(timeout, future)
                                .await
                                .map_err(|_| FlowError::Tool("tool timed out".into()))?,
                            None => future.await,
                        }
                    })
                    .await;
                (call, result)
            });
            jobs_by_id.insert(job.id(), failed_call);
        }
        let mut outcome = BatchOutcome::default();
        let mut transition = None;
        let mut run = false;
        while let Some(job) = jobs.join_next_with_id().await {
            let (call, result) = match job {
                Ok((id, value)) => {
                    jobs_by_id.remove(&id);
                    value
                }
                Err(error) => {
                    let call = jobs_by_id
                        .remove(&error.id())
                        .expect("tracked task identity");
                    (
                        call,
                        Err(FlowError::Tool(format!("tool task failed: {error}"))),
                    )
                }
            };
            handle.check_active()?;
            let (result, next, error, cancelled) = match result {
                Ok(result) => (
                    if result.result.is_null() {
                        json!({"status":"acknowledged"})
                    } else {
                        result.result
                    },
                    result.next,
                    None,
                    false,
                ),
                Err(FlowError::Cancelled | FlowError::Stale) => {
                    (Value::Null, NextStep::NoResponse, None, true)
                }
                Err(error) => {
                    let error = error.to_string();
                    (
                        json!({"status":"error","error":error}),
                        NextStep::Stay,
                        Some(error),
                        false,
                    )
                }
            };
            if cancelled {
                handle.queue_frame(cancel_frame(&call)).await?;
            } else {
                match next {
                    NextStep::Node(node) => transition = Some(node),
                    NextStep::NoResponse => {}
                    _ => run = true,
                }
                handle
                    .queue_frame(Frame::new(FrameData::FunctionCallResultDetailed(
                        DetailedFunctionCallResultFrame {
                            function_name: call.function_name.clone(),
                            tool_call_id: call.tool_call_id.clone(),
                            arguments: call.arguments.clone(),
                            result: result.clone(),
                            run_llm: Some(false),
                            properties: None,
                            error: error.clone(),
                        },
                    )))
                    .await?;
            }
            if let Ok(mut state) = self.inner.state.lock() {
                state.active.remove(&call.tool_call_id);
            }
            outcome.tools.push(ToolOutcome {
                call,
                result,
                error,
                cancelled,
            });
        }
        handle.drain().await?;
        guard.finished = true;
        if let Some(node) = transition {
            self.set_node_inner(&node, None, Some(handle.generation))
                .await?;
            outcome.transitioned_to = Some(node);
        } else if run {
            handle.queue_frame(Frame::new(FrameData::LlmRun)).await?;
            outcome.run_llm = true;
        }
        Ok(outcome)
    }
    async fn invoke(
        &self,
        call: &FunctionCall,
        function: &FunctionConfig,
        handle: FlowHandle,
    ) -> Result<ToolResult, FlowError> {
        let response = if function.transition_only {
            ToolResult::configured(Value::Null)
        } else {
            self.inner.flow.handlers.tools[&call.function_name]
                .call(call.arguments.clone(), handle)
                .await?
        };
        if response.next == NextStep::NoResponse {
            return Ok(response);
        }
        let next = if self.inner.flow.configured || function.transition_only {
            if response.next != NextStep::Configured {
                return Err(FlowError::Tool(format!(
                    "configured tool '{}' must return Configured or NoResponse",
                    call.function_name
                )));
            }
            match &function.transition_to {
                Some(transition) => transition
                    .destination(&response.result)?
                    .map_or(NextStep::Stay, NextStep::Node),
                None => NextStep::Stay,
            }
        } else {
            if response.next == NextStep::Configured {
                return Err(FlowError::Tool(
                    "Configured is invalid outside a configured flow".into(),
                ));
            }
            if let NextStep::Node(name) = &response.next {
                self.inner.flow.node(name)?;
            }
            response.next
        };
        Ok(ToolResult {
            result: response.result,
            next,
        })
    }
}
fn cancel_frame(call: &FunctionCall) -> Frame {
    Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
        function_name: call.function_name.clone(),
        tool_call_id: call.tool_call_id.clone(),
        run_llm: false,
    }))
    .with_interruptible(false)
}
