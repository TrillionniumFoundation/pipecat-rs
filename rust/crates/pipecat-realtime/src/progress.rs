//! Opt-in local lifetime owners for native asynchronous tool reporting.
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, NativeToolProgressFailure as Failure,
    NativeToolProgressRole as Role, NativeToolProgressRoute, NativeToolProgressRouteState as State,
    ProcessorContext, ProcessorError, StartFrame,
};
use tokio::time::Instant;

/// Ingress terminal gate. Place before the selected real user aggregator.
/// `request_stop` belongs to the shared route; ordinary PipelineSender Stop keeps
/// its existing Normal-lane/post-End contract.
pub struct OpenAiRealtimeToolProgressGate {
    route: NativeToolProgressRoute,
}
impl OpenAiRealtimeToolProgressGate {
    pub fn new(route: NativeToolProgressRoute) -> Self {
        Self { route }
    }
}
#[async_trait]
impl FrameProcessor for OpenAiRealtimeToolProgressGate {
    fn name(&self) -> &str {
        "OpenAiRealtimeToolProgressGate"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.route.register_role(Role::Gate, ctx, None)?;
        self.route.register_gate_context(ctx)
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame.native_tool_batch()?;
        frame.native_tool_progress()?;
        frame.native_tool_progress_admission()?;
        frame.native_tool_outcome()?;
        if direction == Direction::Downstream {
            match &frame.data {
                FrameData::End(_) | FrameData::EndDetailed(_) => {
                    self.route.hold_end(frame)?;
                    return Ok(());
                }
                FrameData::Stop => {
                    self.route.request_stop()?;
                    return Ok(());
                }
                FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                    self.route.close(Failure::Cancelled)
                }
                _ => {}
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.route.mark_closed();
        Ok(())
    }
}

/// Stateful selected relay with one supervised receipt/deadline owner.
/// The legacy unit result relay remains unchanged.
pub struct OpenAiRealtimeToolProgressRelay {
    route: NativeToolProgressRoute,
}
impl OpenAiRealtimeToolProgressRelay {
    pub fn new(route: NativeToolProgressRoute) -> Self {
        Self { route }
    }
}
struct OwnerDrop {
    route: NativeToolProgressRoute,
    completed: bool,
}
impl Drop for OwnerDrop {
    fn drop(&mut self) {
        if !self.completed {
            self.route.close(Failure::OwnerDropped);
        }
    }
}
#[async_trait]
impl FrameProcessor for OpenAiRealtimeToolProgressRelay {
    fn name(&self) -> &str {
        "OpenAiRealtimeToolProgressRelay"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.route.register_role(Role::Relay, ctx, None)?;
        let route = self.route.clone();
        ctx.spawn("tool-progress-receipts", false, async move {
            let mut guard = OwnerDrop { route: route.clone(), completed: false };
            let mut changes = route.subscribe();
            loop {
                route.expire(Instant::now());
                let _ = route.take_dispositions();
                match route.state() {
                    State::Failed => return Err(ProcessorError::fatal(format!("native tool progress route failed: {:?}", route.failure()))),
                    State::Cancelled | State::Closed => { guard.completed = true; return Ok(()); }
                    State::Stopping => {
                        if let Some(enqueue) = route.forward_held_terminal()? {
                            enqueue.finish()?;
                            guard.completed = true;
                            return Ok(());
                        }
                    }
                    State::Draining if route.is_drained() => {
                        if let Some(enqueue) = route.forward_held_terminal()? {
                            enqueue.finish()?;
                            guard.completed = true;
                            return Ok(());
                        }
                    }
                    State::EndForwarded => { guard.completed = true; return Ok(()); }
                    _ => {}
                }
                if let Some(deadline) = route.next_deadline() {
                    tokio::select! { biased;
                        changed = changes.changed() => { if changed.is_err() { return Err(ProcessorError::fatal("tool progress owner signal closed")); } }
                        _ = tokio::time::sleep_until(deadline) => {}
                    }
                } else if changes.changed().await.is_err() {
                    return Err(ProcessorError::fatal("tool progress owner signal closed"));
                }
            }
        })?;
        Ok(())
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame.native_tool_batch()?;
        let admission = frame.native_tool_progress_admission()?.cloned();
        let progress = frame.native_tool_progress()?.cloned();
        let outcome = frame.native_tool_outcome()?.cloned();
        if let Some(admission) = &admission {
            self.route.validate_admission(ctx, admission)?;
        }
        if let Some(progress) = &progress {
            self.route.validate_admission(ctx, progress.admission())?;
        }
        if let Some(terminal) = outcome
            .as_ref()
            .and_then(|outcome| outcome.progress_terminal())
        {
            self.route.validate_admission(ctx, terminal.admission())?;
        }
        let owned = admission.is_some()
            || progress.is_some()
            || outcome
                .as_ref()
                .is_some_and(|outcome| outcome.progress_terminal().is_some());
        if owned && self.route.is_closed() {
            return Ok(());
        }
        if matches!(
            frame.data,
            FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            self.route.close(Failure::Cancelled);
        }
        if direction == Direction::Downstream && owned {
            if progress.is_none() {
                super::processor::project_timeout(&mut frame)?;
            }
            ctx.push(frame.clone(), Direction::Upstream).await?;
        } else if direction == Direction::Downstream
            && matches!(
                frame.data,
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_)
            )
        {
            return Err(ProcessorError::fatal(
                "selected tool result lacks real admitted lease",
            ));
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        if matches!(self.route.state(), State::Open | State::Draining) {
            return Err(ProcessorError::fatal(
                "selected terminal bypassed its ingress gate",
            ));
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.route.mark_closed();
        Ok(())
    }
}
