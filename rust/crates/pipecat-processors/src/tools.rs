//! Registered native function callbacks with bounded admission and typed results.
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, FunctionCall,
    FunctionCallInProgressFrame, FunctionCallResultFrame, LlmContinuationBlock,
    NativeLlmContinuation, ProcessorContext, ProcessorError,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;

pub type ToolFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>;
pub type ToolCallback = Arc<dyn Fn(FunctionCall) -> ToolFuture + Send + Sync>;
#[derive(Clone)]
pub struct RegisteredTool {
    pub callback: ToolCallback,
    pub timeout: Duration,
    pub cancel_on_interruption: bool,
}
impl RegisteredTool {
    pub fn new(callback: ToolCallback) -> Self {
        Self {
            callback,
            timeout: Duration::from_secs(30),
            cancel_on_interruption: true,
        }
    }
}
/// Duplicate prevention is bounded to the latest `duplicate_window` call IDs.
/// Callbacks must be cancellation-safe; detached tasks/external side effects cannot be undone.
pub struct ToolExecutor {
    tools: BTreeMap<String, RegisteredTool>,
    slots: Arc<Semaphore>,
    seen: Arc<Mutex<History>>,
    duplicate_window: usize,
    max_argument_bytes: usize,
    max_batch: usize,
}
impl ToolExecutor {
    pub fn new(
        max_in_flight: usize,
        duplicate_window: usize,
        max_argument_bytes: usize,
    ) -> Result<Self, ProcessorError> {
        if max_in_flight == 0 || duplicate_window == 0 || max_argument_bytes == 0 {
            return Err(ProcessorError::new("tool limits must be positive"));
        }
        Ok(Self {
            tools: BTreeMap::new(),
            slots: Arc::new(Semaphore::new(max_in_flight)),
            seen: Arc::new(Mutex::new(History::default())),
            duplicate_window,
            max_argument_bytes,
            max_batch: max_in_flight,
        })
    }
    pub fn register(
        &mut self,
        name: impl Into<String>,
        tool: RegisteredTool,
    ) -> Result<(), ProcessorError> {
        let name = name.into();
        if name.is_empty() || tool.timeout.is_zero() || self.tools.contains_key(&name) {
            return Err(ProcessorError::new(
                "invalid or duplicate tool registration",
            ));
        }
        self.tools.insert(name, tool);
        Ok(())
    }
    fn remember(&self, call: &FunctionCall) -> Result<bool, ProcessorError> {
        let id = call.tool_call_id.as_str();
        let mut seen = self
            .seen
            .lock()
            .map_err(|_| ProcessorError::fatal("tool history poisoned"))?;
        if seen.active.contains_key(id) || seen.recent.iter().any(|known| known == id) {
            return Ok(false);
        }
        if seen.recent.len() == self.duplicate_window {
            seen.recent.pop_front();
        }
        seen.recent.push_back(id.into());
        seen.active.insert(
            id.into(),
            ActiveCall {
                function_name: call.function_name.clone(),
                task: None,
            },
        );
        Ok(true)
    }
    fn remember_signed(
        &self,
        calls: &[FunctionCall],
        history: Arc<NativeLlmContinuation>,
    ) -> Result<bool, ProcessorError> {
        let mut seen = self
            .seen
            .lock()
            .map_err(|_| ProcessorError::fatal("tool history poisoned"))?;
        let retained: std::collections::BTreeSet<_> = seen
            .active
            .keys()
            .chain(seen.recent.iter())
            .cloned()
            .collect();
        let live: Vec<_> = seen.signed.iter().filter(|(_, history)| history.blocks().iter().any(|block| matches!(block, LlmContinuationBlock::ToolCall(call) if retained.contains(&call.tool_call_id)))).map(|(id, history)| (*id, history.clone())).collect();
        if let Some((_, previous)) = live.iter().find(|(id, _)| *id == history.response_id()) {
            return if previous.as_ref() == history.as_ref() {
                Ok(false)
            } else {
                Err(ProcessorError::new("conflicting signed response identity"))
            };
        }
        if calls
            .iter()
            .any(|call| retained.contains(&call.tool_call_id))
        {
            return Err(ProcessorError::new(
                "partially duplicate or conflicting signed tool batch",
            ));
        }
        let bytes = live
            .iter()
            .try_fold(history.retained_bytes(), |bytes, (_, entry)| {
                bytes.checked_add(entry.retained_bytes())
            })
            .filter(|bytes| *bytes <= NativeLlmContinuation::MAX_RETAINED_BYTES);
        if bytes.is_none() {
            return Err(ProcessorError::new(
                "aggregate signed tool history exceeds byte limit",
            ));
        }
        // Only publish identities once all effects and retained history are admissible.
        seen.signed = live.into_iter().collect();
        seen.signed.insert(history.response_id(), history);
        for call in calls {
            if seen.recent.len() == self.duplicate_window {
                seen.recent.pop_front();
            }
            seen.recent.push_back(call.tool_call_id.clone());
            seen.active.insert(
                call.tool_call_id.clone(),
                ActiveCall {
                    function_name: call.function_name.clone(),
                    task: None,
                },
            );
        }
        Ok(true)
    }
    async fn dispatch(
        &self,
        call: FunctionCall,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let lease = ActiveLease {
            id: call.tool_call_id.clone(),
            history: self.seen.clone(),
        };
        let Some(tool) = self.tools.get(&call.function_name).cloned() else {
            return emit_result(call, Err("tool is not registered".into()), ctx).await;
        };
        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return emit_result(call, Err("tool concurrency capacity exceeded".into()), ctx)
                    .await
            }
        };
        let output = ctx.clone();
        let call_id = call.tool_call_id.clone();
        let task_id = ctx.spawn("tool-call", tool.cancel_on_interruption, async move {
            let _permit = permit;
            let _lease = lease;
            let outcome = tokio::time::timeout(
                tool.timeout,
                std::panic::AssertUnwindSafe(async { (tool.callback)(call.clone()).await })
                    .catch_unwind(),
            )
            .await;
            let outcome = match outcome {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Err("tool callback panicked".into()),
                Err(_) => Err("tool call timed out".into()),
            };
            emit_result(call, outcome, &output).await
        })?;
        if let Some(active) = self
            .seen
            .lock()
            .map_err(|_| ProcessorError::fatal("tool history poisoned"))?
            .active
            .get_mut(&call_id)
        {
            active.task = Some(task_id);
        }
        Ok(())
    }
}
async fn emit_result(
    call: FunctionCall,
    result: Result<Value, String>,
    ctx: &ProcessorContext,
) -> Result<(), ProcessorError> {
    let (result, error) = match result {
        Ok(result) => (result, None),
        Err(error) => (json!({"error":error}), Some(error)),
    };
    ctx.push(
        Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: call.function_name,
            tool_call_id: call.tool_call_id,
            arguments: call.arguments,
            result,
            run_llm: None,
            is_final: true,
            error,
        })),
        Direction::Downstream,
    )
    .await
}
#[async_trait]
impl FrameProcessor for ToolExecutor {
    fn name(&self) -> &str {
        "ToolExecutor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let signed = frame
            .native_llm_continuation()
            .map_err(|error| ProcessorError::new(error.to_string()))?
            .cloned();
        if let FrameData::FunctionCallCancel(cancel) = &frame.data {
            let task = {
                let history = self
                    .seen
                    .lock()
                    .map_err(|_| ProcessorError::fatal("tool history poisoned"))?;
                history
                    .active
                    .get(&cancel.tool_call_id)
                    .filter(|active| active.function_name == cancel.function_name)
                    .and_then(|active| active.task)
            };
            if let Some(task) = task {
                ctx.cancel_task(task);
            }
        }
        let calls = if direction == Direction::Downstream {
            match &frame.data {
                FrameData::FunctionCallsStarted(calls) => Some(calls.clone()),
                _ => None,
            }
        } else {
            None
        };
        let Some(calls) = calls else {
            return ctx.push(frame, direction).await;
        };
        if calls.len() > self.max_batch {
            return Err(ProcessorError::new("tool batch exceeds configured limit"));
        }
        for call in &calls {
            if call.tool_call_id.is_empty()
                || call.function_name.is_empty()
                || !call.arguments.is_object()
            {
                return Err(ProcessorError::new(
                    "invalid tool call identity or arguments",
                ));
            }
            if serde_json::to_vec(&call.arguments)
                .map_err(|e| ProcessorError::new(e.to_string()))?
                .len()
                > self.max_argument_bytes
            {
                return Err(ProcessorError::new(
                    "tool arguments exceed configured limit",
                ));
            }
        }
        let mut fresh = Vec::new();
        if let Some(history) = signed {
            if !self.remember_signed(&calls, history)? {
                return ctx.push(frame, direction).await;
            }
            fresh = calls;
        } else {
            for call in calls {
                if self.remember(&call)? {
                    fresh.push(call);
                }
            }
        }
        if fresh.is_empty() {
            return Ok(());
        }
        // Guards cover failure before callbacks are spawned as well as dropped futures.
        let guards: Vec<_> = fresh
            .iter()
            .map(|call| ActiveLease {
                id: call.tool_call_id.clone(),
                history: self.seen.clone(),
            })
            .collect();
        let group_id = format!("tool-batch-{}", frame.id);
        let mut started = frame;
        started.data = FrameData::FunctionCallsStarted(fresh.clone());
        ctx.push(started, direction).await?;
        // Admit all placeholders before any completion can reach the aggregator.
        for call in &fresh {
            ctx.push(
                Frame::new(FrameData::FunctionCallInProgress(
                    FunctionCallInProgressFrame {
                        function_name: call.function_name.clone(),
                        tool_call_id: call.tool_call_id.clone(),
                        arguments: call.arguments.clone(),
                        cancel_on_interruption: self
                            .tools
                            .get(&call.function_name)
                            .is_none_or(|tool| tool.cancel_on_interruption),
                        group_id: Some(group_id.clone()),
                    },
                )),
                Direction::Downstream,
            )
            .await?;
        }
        for (call, mut guard) in fresh.into_iter().zip(guards) {
            self.dispatch(call, ctx).await?;
            guard.id.clear(); // Ownership moved to dispatch's lease.
        }
        Ok(())
    }
}
#[derive(Default)]
struct History {
    recent: VecDeque<String>,
    active: BTreeMap<String, ActiveCall>,
    signed: BTreeMap<u64, Arc<NativeLlmContinuation>>,
}
struct ActiveCall {
    function_name: String,
    task: Option<u64>,
}
struct ActiveLease {
    id: String,
    history: Arc<Mutex<History>>,
}
impl Drop for ActiveLease {
    fn drop(&mut self) {
        if let Ok(mut history) = self.history.lock() {
            history.active.remove(&self.id);
        }
    }
}
