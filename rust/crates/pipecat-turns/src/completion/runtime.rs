//! Tracked deadline ownership and consumption-time guards for automatic prompts.
use super::{LLMTurnCompletionConfig, LLMTurnCompletionStrategy};
use crate::{context::text_message, AggregationAction, AggregationOutput};
use pipecat_core::{
    async_trait, ChatRole, Direction, Frame, FrameData, FrameProcessor, LlmMessagesTransformFrame,
    LocalMessageTransform, ProcessorContext, ProcessorError,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

struct State {
    strategy: LLMTurnCompletionStrategy,
    epoch: u64,
    task_id: Option<u64>,
    output: Option<ProcessorContext>,
    closing: bool,
}
impl State {
    fn take_ready_prompt(
        &mut self,
        now: Duration,
    ) -> Result<Option<(AggregationOutput, ProcessorContext, u64)>, ProcessorError> {
        if self.closing || self.strategy.is_closed() {
            return Ok(None);
        }
        let output = self.strategy.advance(now)?;
        if output.actions.is_empty() {
            return Ok(None);
        }
        let target = self
            .output
            .take()
            .ok_or_else(|| ProcessorError::fatal("completion deadline has no emission context"))?;
        Ok(Some((output, target, self.epoch)))
    }
}
/// Place after an explicitly unsigned LLM and before its assistant aggregator.
/// One tracked supervisor waits for all deadlines. Terminal preparation cancels
/// it before task draining. No provider, tool, or detached Tokio tasks are owned.
pub struct LLMTurnCompletionProcessor {
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
    origin: Instant,
}
impl LLMTurnCompletionProcessor {
    pub fn new(config: LLMTurnCompletionConfig) -> Result<Self, ProcessorError> {
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                strategy: LLMTurnCompletionStrategy::new(config)?,
                epoch: 0,
                task_id: None,
                output: None,
                closing: false,
            })),
            changed: Arc::new(Notify::new()),
            origin: Instant::now(),
        })
    }
    fn close(&self, ctx: Option<&ProcessorContext>) -> Result<(), ProcessorError> {
        let mut state = lock(&self.state)?;
        state.strategy.stop();
        state.closing = true;
        state.output = None;
        if let (Some(ctx), Some(id)) = (ctx, state.task_id) {
            ctx.cancel_task(id);
        }
        self.changed.notify_one();
        Ok(())
    }
    fn schedule(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = lock(&self.state)?;
        if state.closing
            || state.strategy.is_closed()
            || state.task_id.is_some()
            || state.strategy.deadline().is_none()
        {
            return Ok(());
        }
        let shared = self.state.clone();
        let changed = self.changed.clone();
        let origin = self.origin;
        state.task_id = Some(ctx.spawn("llm-completion-deadline", false, async move {
            let _guard = TaskGuard(shared.clone());
            loop {
                let notified = changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let deadline = {
                    let state = lock(&shared)?;
                    if state.closing || state.strategy.is_closed() {
                        return Ok(());
                    }
                    state.strategy.deadline()
                };
                tokio::select! {
                    biased;
                    _ = &mut notified => {},
                    _ = async {
                        match deadline.and_then(|d| origin.checked_add(d)) {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => {}
                }
                let ready = {
                    let mut state = lock(&shared)?;
                    state.take_ready_prompt(origin.elapsed())?
                };
                let Some((mut output, target, epoch)) = ready else {
                    continue;
                };
                guard_prompt(&mut output, &shared, epoch);
                let emit = output.emit(&target);
                tokio::pin!(emit);
                loop {
                    let notified = changed.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    {
                        let state = lock(&shared)?;
                        if state.closing || state.strategy.is_closed() {
                            return Ok(());
                        }
                        if state.epoch != epoch {
                            break;
                        }
                    }
                    tokio::select! {
                        biased;
                        result = &mut emit => { result?; break; }
                        _ = &mut notified => continue,
                    }
                }
            }
        })?);
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for LLMTurnCompletionProcessor {
    fn name(&self) -> &str {
        "LLMTurnCompletionProcessor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let output = {
            let mut state = lock(&self.state)?;
            let result = state
                .strategy
                .process_frame(frame, direction, self.origin.elapsed());
            state.epoch = state.strategy.retry_epoch;
            state.output = state.strategy.deadline().map(|_| ctx.clone());
            self.changed.notify_one();
            result?
        };
        self.schedule(ctx)?;
        output.emit(ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        // End's fallback/report must still be emitted by process; only disarm
        // deadlines here, before the runtime drains owned background work.
        let mut state = lock(&self.state)?;
        state.strategy.cancel_deadline();
        state.closing = true;
        state.epoch = state.epoch.saturating_add(1);
        state.output = None;
        if let Some(id) = state.task_id {
            ctx.cancel_task(id);
        }
        self.changed.notify_one();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.close(None)
    }
}
impl Drop for LLMTurnCompletionProcessor {
    fn drop(&mut self) {
        let _ = self.close(None);
    }
}
fn lock(state: &Mutex<State>) -> Result<MutexGuard<'_, State>, ProcessorError> {
    state
        .lock()
        .map_err(|_| ProcessorError::fatal("completion strategy lock poisoned"))
}
struct TaskGuard(Arc<Mutex<State>>);
impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.lock() {
            state.task_id = None;
        }
    }
}
fn guard_prompt(output: &mut AggregationOutput, state: &Arc<Mutex<State>>, epoch: u64) {
    for action in &mut output.actions {
        let AggregationAction::Push(directed) = action else {
            continue;
        };
        let FrameData::LlmMessagesAppend(messages) = &directed.frame.data else {
            continue;
        };
        let prompt = messages.messages[0].content.clone();
        let owner = Arc::downgrade(state);
        let consumed = AtomicBool::new(false);
        directed.frame.data = FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
            transform: LocalMessageTransform::new(move |mut messages| {
                let owner = owner.upgrade().ok_or_else(stale_prompt)?;
                let state = lock(&owner)?;
                if state.closing
                    || state.strategy.is_closed()
                    || state.epoch != epoch
                    || consumed.swap(true, Ordering::SeqCst)
                {
                    return Err(stale_prompt());
                }
                messages.push(text_message(ChatRole::Developer, prompt.clone()));
                Ok(messages)
            }),
            run_llm: Some(true),
        });
    }
}
fn stale_prompt() -> ProcessorError {
    ProcessorError::new("discarded stale or replayed automatic completion prompt")
        .with_permanence_override(false)
}

#[cfg(test)]
mod review_tests;
