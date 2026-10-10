//! Waiting-for-user detection, independent of speech-finalization deadlines.
use pipecat_core::{ErrorFrame, FrameData, FunctionCall, ProcessorContext, ProcessorError};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

const MAX_PENDING_CALLS: usize = 512;
const RECENT_COMPLETED_CALLS: usize = 512;

/// A deterministic, one-shot idle timer. A bot stop or `wait_for_user` opens
/// the waiting window; speech and function calls close it. Finishing a user
/// turn or function call alone does not imply that the bot has responded.
#[derive(Debug, Clone)]
pub struct UserIdleController {
    timeout: Duration,
    waiting_for_user: bool,
    bot_speaking: bool,
    user_turn_in_progress: bool,
    function_calls: BTreeMap<String, FunctionCall>,
    completed_calls: VecDeque<String>,
    deadline: Option<Duration>,
    now: Duration,
}
impl UserIdleController {
    /// Zero disables detection, while still tracking the waiting window.
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            waiting_for_user: false,
            bot_speaking: false,
            user_turn_in_progress: false,
            function_calls: BTreeMap::new(),
            completed_calls: VecDeque::new(),
            deadline: None,
            now: Duration::ZERO,
        }
    }
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
    pub fn waiting_for_user(&self) -> bool {
        self.waiting_for_user
    }
    pub fn function_calls_in_progress(&self) -> bool {
        !self.function_calls.is_empty()
    }
    pub fn deadline(&self) -> Option<Duration> {
        self.deadline
    }

    pub fn process(&mut self, data: &FrameData, now: Duration) -> Result<(), ProcessorError> {
        // Validate before changing state, including the clock.
        let timeout = match data {
            FrameData::UserIdleTimeoutUpdate { timeout } => Some(timeout_duration(*timeout)?),
            _ => None,
        };
        let declarations = self.declarations(data)?;
        self.now = self.now.max(now);
        if let Some(timeout) = timeout {
            self.timeout = timeout;
            if timeout.is_zero() {
                self.stop();
            } else if self.waiting_for_user {
                self.start();
            }
            return Ok(());
        }
        match data {
            FrameData::BotStoppedSpeaking => {
                self.bot_speaking = false;
                if !self.user_turn_in_progress && self.function_calls.is_empty() {
                    self.waiting_for_user = true;
                    self.start();
                }
            }
            FrameData::BotStartedSpeaking => {
                self.bot_speaking = true;
                self.waiting_for_user = false;
                self.stop();
            }
            FrameData::UserStartedSpeaking => {
                self.user_turn_in_progress = true;
                self.waiting_for_user = false;
                self.stop();
            }
            FrameData::UserStoppedSpeaking => self.user_turn_in_progress = false,
            FrameData::FunctionCallsStarted(_) | FrameData::FunctionCallsStartedDetailed(_) => {
                self.start_calls(declarations.expect("declaration frame"));
            }
            FrameData::FunctionCallResult(result) if result.is_final => {
                self.finish_call(
                    &result.tool_call_id,
                    &result.function_name,
                    Some(&result.arguments),
                );
            }
            FrameData::FunctionCallResultDetailed(result)
                if result
                    .properties
                    .as_ref()
                    .is_none_or(|properties| properties.is_final) =>
            {
                self.finish_call(
                    &result.tool_call_id,
                    &result.function_name,
                    Some(&result.arguments),
                );
            }
            FrameData::FunctionCallCancel(cancel) => {
                self.finish_call(&cancel.tool_call_id, &cancel.function_name, None);
            }
            FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_) => self.stop(),
            _ => {}
        }
        Ok(())
    }
    /// Re-arm after a turn the bot will not answer, such as an empty turn.
    pub fn wait_for_user(&mut self, now: Duration) {
        self.now = self.now.max(now);
        if self.bot_speaking || self.user_turn_in_progress || !self.function_calls.is_empty() {
            return;
        }
        self.waiting_for_user = true;
        self.start();
    }
    /// Return true once when the current deadline expires. No polling loop or
    /// callback is implicit; the owner chooses how to deliver the event.
    pub fn advance(&mut self, now: Duration) -> bool {
        self.now = self.now.max(now);
        if self.deadline.is_some_and(|deadline| self.now >= deadline) {
            self.deadline = None;
            true
        } else {
            false
        }
    }
    /// Cancel the current timer without changing the configured timeout.
    pub fn stop(&mut self) {
        self.deadline = None;
    }
    fn start(&mut self) {
        self.deadline = (!self.timeout.is_zero()).then(|| self.now.saturating_add(self.timeout));
    }
    fn declarations(
        &self,
        data: &FrameData,
    ) -> Result<Option<BTreeMap<String, FunctionCall>>, ProcessorError> {
        let calls: Vec<FunctionCall> = match data {
            FrameData::FunctionCallsStarted(calls) => calls.clone(),
            FrameData::FunctionCallsStartedDetailed(calls) => calls
                .iter()
                .map(|call| FunctionCall {
                    function_name: call.function_name.clone(),
                    tool_call_id: call.tool_call_id.clone(),
                    arguments: serde_json::Value::Object(
                        call.arguments.clone().into_iter().collect(),
                    ),
                })
                .collect(),
            _ => return Ok(None),
        };
        let mut declarations = BTreeMap::new();
        for call in calls {
            if call.tool_call_id.is_empty()
                || call.function_name.is_empty()
                || !call.arguments.is_object()
            {
                return Err(ProcessorError::new(
                    "tool call requires id, name, and object arguments",
                ));
            }
            if self.completed_calls.contains(&call.tool_call_id) {
                continue;
            }
            if let Some(old) = self
                .function_calls
                .get(&call.tool_call_id)
                .or_else(|| declarations.get(&call.tool_call_id))
            {
                if old != &call {
                    return Err(ProcessorError::new("conflicting duplicate tool call id"));
                }
                continue;
            }
            if self.function_calls.len() + declarations.len() >= MAX_PENDING_CALLS {
                return Err(ProcessorError::new(
                    "user idle pending tool call limit reached",
                ));
            }
            declarations.insert(call.tool_call_id.clone(), call);
        }
        Ok(Some(declarations))
    }
    fn start_calls(&mut self, calls: BTreeMap<String, FunctionCall>) {
        if calls.is_empty() {
            return;
        }
        self.waiting_for_user = false;
        self.function_calls.extend(calls);
        self.stop();
    }
    fn finish_call(&mut self, id: &str, name: &str, arguments: Option<&serde_json::Value>) {
        if !self.function_calls.get(id).is_some_and(|call| {
            call.function_name == name
                && arguments.is_none_or(|arguments| arguments == &call.arguments)
        }) {
            return;
        }
        self.function_calls.remove(id);
        self.completed_calls.push_back(id.to_owned());
        if self.completed_calls.len() > RECENT_COMPLETED_CALLS {
            self.completed_calls.pop_front();
        }
    }
}
impl Default for UserIdleController {
    fn default() -> Self {
        Self::new(Duration::ZERO)
    }
}
pub(crate) fn timeout_duration(seconds: f64) -> Result<Duration, ProcessorError> {
    if !seconds.is_finite() {
        return Err(ProcessorError::new("user idle timeout must be finite"));
    }
    Duration::try_from_secs_f64(seconds.max(0.0))
        .map_err(|_| ProcessorError::new("user idle timeout exceeds the supported duration"))
}

type IdleFuture = Pin<Box<dyn Future<Output = Result<(), ProcessorError>> + Send>>;
type IdleHandler = Arc<dyn Fn(ProcessorContext) -> IdleFuture + Send + Sync>;
struct IdleState {
    controller: UserIdleController,
    handlers: Vec<IdleHandler>,
    stopped: bool,
    task_id: Option<u64>,
}

/// Owns one tracked supervisor, including event handlers. Keeping the dormant
/// supervisor avoids task churn on frequent bot-stop and timeout-update frames.
pub(crate) struct UserIdleTracker {
    state: Arc<Mutex<IdleState>>,
    changed: Arc<Notify>,
    origin: Instant,
}
impl UserIdleTracker {
    pub(crate) fn new(timeout: Duration, origin: Instant) -> Self {
        Self {
            state: Arc::new(Mutex::new(IdleState {
                controller: UserIdleController::new(timeout),
                handlers: Vec::new(),
                stopped: false,
                task_id: None,
            })),
            changed: Arc::new(Notify::new()),
            origin,
        }
    }
    fn lock(&self) -> Result<MutexGuard<'_, IdleState>, ProcessorError> {
        lock(&self.state)
    }
    pub(crate) fn on_idle<F, Fut>(&self, handler: F) -> Result<(), ProcessorError>
    where
        F: Fn(ProcessorContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ProcessorError>> + Send + 'static,
    {
        self.lock()?
            .handlers
            .push(Arc::new(move |ctx| Box::pin(handler(ctx))));
        Ok(())
    }
    pub(crate) fn process(&self, data: &FrameData, now: Duration) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        let deadline = state.controller.deadline();
        if !state.stopped {
            state.controller.process(data, now)?;
        }
        if state.controller.deadline() != deadline {
            self.changed.notify_one();
        }
        Ok(())
    }
    /// Preflight before an owning processor emits policy transitions. A failed
    /// tool declaration must not mutate unrelated mute or turn state.
    pub(crate) fn validate(&self, data: &FrameData) -> Result<(), ProcessorError> {
        let state = self.lock()?;
        if !state.stopped {
            if let FrameData::UserIdleTimeoutUpdate { timeout } = data {
                timeout_duration(*timeout)?;
            }
            state.controller.declarations(data)?;
        }
        Ok(())
    }
    pub(crate) fn wait_for_user(&self, now: Duration) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if !state.stopped {
            state.controller.wait_for_user(now);
        }
        self.changed.notify_one();
        Ok(())
    }
    pub(crate) fn waiting_for_user(&self) -> Result<bool, ProcessorError> {
        let state = self.lock()?;
        Ok(!state.stopped && state.controller.waiting_for_user())
    }
    pub(crate) fn function_calls_in_progress(&self) -> Result<bool, ProcessorError> {
        Ok(self.lock()?.controller.function_calls_in_progress())
    }
    pub(crate) fn deadline(&self) -> Result<Option<Duration>, ProcessorError> {
        Ok(self.lock()?.controller.deadline())
    }
    pub(crate) fn advance(&self, now: Duration) -> Result<bool, ProcessorError> {
        let mut state = self.lock()?;
        Ok(!state.stopped && state.controller.advance(now))
    }
    pub(crate) fn stop(&self, ctx: Option<&ProcessorContext>) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        state.stopped = true;
        state.controller.stop();
        if let (Some(ctx), Some(id)) = (ctx, state.task_id) {
            ctx.cancel_task(id);
        }
        self.changed.notify_one();
        Ok(())
    }
    pub(crate) fn schedule(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if state.stopped || state.task_id.is_some() || state.controller.deadline().is_none() {
            return Ok(());
        }
        let shared = self.state.clone();
        let changed = self.changed.clone();
        let output = ctx.clone();
        let origin = self.origin;
        state.task_id = Some(ctx.spawn("user-idle-deadline", false, async move {
            let _guard = IdleTaskGuard(shared.clone());
            loop {
                let deadline = {
                    let state = lock(&shared)?;
                    if state.stopped {
                        return Ok(());
                    }
                    state.controller.deadline()
                };
                tokio::select! {
                    _ = changed.notified() => continue,
                    _ = async {
                        match deadline {
                            Some(deadline) => match origin.checked_add(deadline) {
                                Some(deadline) => tokio::time::sleep_until(deadline).await,
                                None => std::future::pending::<()>().await,
                            },
                            None => std::future::pending::<()>().await,
                        }
                    } => {}
                }
                let handlers = {
                    let mut state = lock(&shared)?;
                    if state.stopped {
                        return Ok(());
                    }
                    // Re-read under the state lock: a simultaneous update or
                    // speech boundary may have invalidated the sleeping deadline.
                    if !state.controller.advance(origin.elapsed()) {
                        continue;
                    }
                    state.handlers.clone()
                };
                for handler in handlers {
                    if lock(&shared)?.stopped {
                        return Ok(());
                    }
                    let callback = handler(output.clone());
                    tokio::pin!(callback);
                    loop {
                        tokio::select! {
                            biased;
                            _ = changed.notified() => {
                                if lock(&shared)?.stopped { return Ok(()); }
                            }
                            result = &mut callback => {
                                if let Err(error) = result {
                                    if error.fatal {
                                        return Err(error);
                                    }
                                    output.push_error(ErrorFrame {
                                        error: error.message,
                                        fatal: false,
                                        processor: None,
                                        category: error.category,
                                    }).await?;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        })?);
        Ok(())
    }
}
fn lock(state: &Mutex<IdleState>) -> Result<MutexGuard<'_, IdleState>, ProcessorError> {
    state
        .lock()
        .map_err(|_| ProcessorError::fatal("user idle controller lock poisoned"))
}
struct IdleTaskGuard(Arc<Mutex<IdleState>>);
impl Drop for IdleTaskGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.lock() {
            state.task_id = None;
        }
    }
}
