use crate::{default_payload, transport, AgentCoreConfig, Error};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
};
use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

/// One explicit immutable client. It performs no work until a context is admitted.
#[derive(Clone, Debug)]
pub struct AgentCoreClient {
    pub(crate) config: AgentCoreConfig,
}
impl AgentCoreClient {
    pub fn new(config: AgentCoreConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self { config })
    }
}
/// Counts are local ownership facts, not hosted execution or cancellation receipts.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentCoreProgress {
    pub contexts_seen: u64,
    pub admitted: u64,
    pub completed_at_eof: u64,
    pub owners_released: u64,
    pub failed: u64,
    pub cancelled_or_aborted: u64,
    pub primary: Option<Error>,
}
pub(crate) async fn wait_cancel(receiver: &mut watch::Receiver<bool>) {
    loop {
        let cancelled = *receiver.borrow_and_update();
        if cancelled || receiver.changed().await.is_err() {
            return;
        }
    }
}
pub(crate) struct OutputFlow {
    pub installed: AtomicBool,
    pending: Mutex<Option<(u64, watch::Sender<Option<bool>>)>>,
}
impl OutputFlow {
    fn new() -> Self {
        Self {
            installed: AtomicBool::new(false),
            pending: Mutex::new(None),
        }
    }
    pub fn begin(&self, id: u64) -> Result<OutputTicket<'_>, Error> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.is_some() {
            return Err(Error::Output);
        }
        let (sender, receiver) = watch::channel(None);
        *pending = Some((id, sender));
        Ok(OutputTicket {
            flow: self,
            id,
            receiver,
        })
    }
    pub fn acknowledge(&self, id: u64, success: bool) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending
            .as_ref()
            .is_some_and(|(expected, _)| *expected == id)
        {
            if let Some((_, sender)) = pending.take() {
                sender.send_replace(Some(success));
            }
        }
    }
}
pub(crate) struct OutputTicket<'a> {
    flow: &'a OutputFlow,
    id: u64,
    receiver: watch::Receiver<Option<bool>>,
}
impl OutputTicket<'_> {
    pub async fn wait(&mut self) -> Result<(), Error> {
        loop {
            if let Some(success) = *self.receiver.borrow_and_update() {
                return if success { Ok(()) } else { Err(Error::Output) };
            }
            self.receiver.changed().await.map_err(|_| Error::Output)?;
        }
    }
}
impl Drop for OutputTicket<'_> {
    fn drop(&mut self) {
        let mut pending = self.flow.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending
            .as_ref()
            .is_some_and(|(expected, _)| *expected == self.id)
        {
            pending.take();
        }
    }
}
pub(crate) struct Shared {
    pub progress: watch::Sender<AgentCoreProgress>,
    pub cancel: watch::Sender<bool>,
    pub output: OutputFlow,
}
impl Shared {
    fn new() -> Self {
        Self {
            progress: watch::channel(AgentCoreProgress::default()).0,
            cancel: watch::channel(false).0,
            output: OutputFlow::new(),
        }
    }
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }
}
struct Owner {
    shared: Arc<Shared>,
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    finished: bool,
}
impl Owner {
    fn finish(&mut self, result: &Result<(), Error>) {
        self.finished = true;
        self.shared.progress.send_modify(|p| match result {
            Ok(()) => p.completed_at_eof += 1,
            Err(Error::Cancelled) => {
                p.cancelled_or_aborted += 1;
                if p.primary.is_none() {
                    p.primary = Some(Error::Cancelled);
                }
            }
            Err(error) => {
                p.failed += 1;
                if p.primary.is_none() {
                    p.primary = Some(error.clone());
                }
            }
        });
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.progress.send_modify(|p| {
            p.owners_released += 1;
            if !self.finished {
                p.cancelled_or_aborted += 1;
            }
        });
    }
}
/// Default-schema text processor. Every network future is owned by the core task
/// registry; End joins it, interruption/Cancel abort it and destroy its socket.
/// Use `ingress_guard()` immediately upstream for Stop-before-drain semantics.
pub struct AgentCoreProcessor {
    client: AgentCoreClient,
    shared: Arc<Shared>,
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    fifo: Arc<tokio::sync::Mutex<()>>,
}
impl AgentCoreProcessor {
    pub fn new(client: AgentCoreClient) -> Self {
        let limits = client.config.limits();
        Self {
            count: Arc::new(Semaphore::new(limits.max_pending_invocations)),
            bytes: Arc::new(Semaphore::new(limits.max_pending_bytes)),
            client,
            shared: Arc::new(Shared::new()),
            fifo: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    pub fn progress(&self) -> AgentCoreProgress {
        self.shared.progress.borrow().clone()
    }
    pub fn cancel(&self) {
        self.shared.cancel();
    }
    pub fn ingress_guard(&self) -> Arc<dyn FrameProcessor> {
        Arc::new(Ingress(self.shared.clone()))
    }
    /// Wrap the immediately downstream caller output. Each generated frame is
    /// acknowledged only after its bounded processing completes. Required before
    /// admitting an invocation; the finite app installs this automatically.
    pub fn output_guard(&self, output: Arc<dyn FrameProcessor>) -> Arc<dyn FrameProcessor> {
        crate::app::BoundedOutput::new(output, self.client.config.limits(), self.shared.clone())
    }
    pub(crate) fn shared(&self) -> Arc<Shared> {
        self.shared.clone()
    }
    async fn admit(
        &self,
        payload: Option<Vec<u8>>,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if *self.shared.cancel.borrow() {
            self.shared.progress.send_modify(|p| p.contexts_seen += 1);
            return Err(
                ProcessorError::new(Error::Cancelled.to_string()).with_permanence_override(false)
            );
        }
        let Some(payload) = payload else {
            self.shared.progress.send_modify(|p| p.contexts_seen += 1);
            return Ok(());
        };
        if !self.shared.output.installed.load(Ordering::Acquire) {
            return Err(ProcessorError::fatal(
                "AgentCore requires its immediately downstream output guard",
            ));
        }
        let count =
            self.count.clone().try_acquire_owned().map_err(|_| {
                ProcessorError::fatal("AgentCore pending invocation count exceeded")
            })?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(payload.capacity().max(1) as u32)
            .map_err(|_| ProcessorError::fatal("AgentCore pending invocation bytes exceeded"))?;
        let mut lock = Box::pin(self.fifo.clone().lock_owned());
        // Poll synchronously at admission, preserving arrival order before spawn.
        let acquired = poll_fn(|cx| Poll::Ready(lock.as_mut().poll(cx))).await;
        let client = self.client.clone();
        let shared = self.shared.clone();
        let output = ctx.clone();
        let mut cancelled = shared.cancel.subscribe();
        let mut owner = Owner {
            shared: shared.clone(),
            _count: count,
            _bytes: bytes,
            finished: false,
        };
        self.shared.progress.send_modify(|p| {
            p.admitted += 1;
            p.contexts_seen += 1;
        });
        ctx.spawn("agentcore-invocation", true, async move {
            let work = async {
                let _fifo = match acquired {
                    Poll::Ready(guard) => guard,
                    Poll::Pending => lock.await,
                };
                if !output.is_current() || *shared.cancel.borrow() {
                    return Err(Error::Cancelled);
                }
                let result =
                    transport::invoke(&client.config, payload, &output, &shared.output).await;
                // Publish the failure fence while still owning the FIFO guard.
                // No queued invocation can acquire its position before this.
                if let Err(error) = &result {
                    if *error != Error::Cancelled {
                        shared.progress.send_modify(|p| {
                            if p.primary.is_none() {
                                p.primary = Some(error.clone());
                            }
                        });
                        shared.cancel();
                    }
                }
                result
            };
            let result = tokio::select! {
                biased;
                _ = wait_cancel(&mut cancelled) => Err(Error::Cancelled),
                result = work => result,
            };
            owner.finish(&result);
            // Preserve an already admitted text prefix through normal output drain.
            // Fail closed for later invocations, but do not turn a protocol error
            // into an asynchronous Cancel that races the accepted prefix.
            match result {
                Err(Error::Cancelled) => Ok(()),
                other => other.map_err(|e| {
                    ProcessorError::new(e.to_string()).with_permanence_override(false)
                }),
            }
        })?;
        Ok(())
    }
}
struct Ingress(Arc<Shared>);
#[async_trait]
impl FrameProcessor for Ingress {
    fn name(&self) -> &str {
        "AgentCoreLifecycleIngress"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && (matches!(frame.data, FrameData::Stop) || frame.data.is_cancel())
        {
            self.0.cancel();
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cancel();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for AgentCoreProcessor {
    fn name(&self) -> &str {
        "AWSAgentCoreProcessor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame.require_no_native_llm_continuation().map_err(|_| {
            ProcessorError::fatal("AgentCore does not accept native authority carriers")
        })?;
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        let payload = match &frame.data {
            FrameData::LlmContext(context) => {
                let limits = self.client.config.limits();
                if context.messages.len() > limits.max_messages
                    || !context.tools.is_empty()
                    || context.tool_choice.is_some()
                {
                    return Err(ProcessorError::fatal(
                        "AgentCore legacy context bounds or tools",
                    ));
                }
                let mut total = 0usize;
                for message in &context.messages {
                    if message.content.len() > limits.max_string_bytes
                        || message.name.is_some()
                        || message.tool_call_id.is_some()
                        || !message.tool_calls.is_empty()
                    {
                        return Err(ProcessorError::fatal(
                            "AgentCore legacy message bounds or metadata",
                        ));
                    }
                    total = total.saturating_add(message.content.len());
                    if total > limits.max_context_bytes {
                        return Err(ProcessorError::fatal("AgentCore legacy context byte bound"));
                    }
                }
                default_payload(&context.clone().into(), limits)
            }
            FrameData::LlmContextDetailed(context) => {
                if context.speculation {
                    return Err(ProcessorError::fatal(
                        "AgentCore speculation is unsupported",
                    ));
                }
                default_payload(&context.context, self.client.config.limits())
            }
            FrameData::LlmRun
            | FrameData::LlmMessagesAppend(_)
            | FrameData::LlmMessagesUpdate(_)
            | FrameData::LlmMessagesAppendDetailed(_)
            | FrameData::LlmMessagesUpdateDetailed(_)
            | FrameData::LlmMessagesTransform(_)
            | FrameData::LlmSetTools(_)
            | FrameData::LlmSetToolChoice(_)
            | FrameData::LlmEnablePromptCaching { .. }
            | FrameData::LlmConfigureOutput { .. }
            | FrameData::LlmUpdateSettings(_)
            | FrameData::ServiceUpdateSettings(_)
            | FrameData::FunctionCallsStarted(_)
            | FrameData::FunctionCallsStartedDetailed(_)
            | FrameData::FunctionCallInProgress(_)
            | FrameData::FunctionCallResult(_)
            | FrameData::FunctionCallResultDetailed(_)
            | FrameData::FunctionCallCancel(_) => {
                return Err(ProcessorError::fatal(
                    "AgentCore tools and context/settings controls are unsupported",
                ))
            }
            _ => return ctx.push(frame, direction).await,
        }
        .map_err(|e| ProcessorError::fatal(e.to_string()))?;
        self.admit(payload, ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.shared.cancel();
        Ok(())
    }
}

#[cfg(test)]
mod credit_tests {
    use super::*;
    #[tokio::test]
    async fn dropped_output_credit_rejects_late_acknowledgement() {
        let flow = OutputFlow::new();
        let old = flow.begin(11).unwrap();
        assert!(flow.begin(12).is_err());
        drop(old);
        let mut fresh = flow.begin(12).unwrap();
        flow.acknowledge(11, true);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), fresh.wait())
                .await
                .is_err()
        );
        flow.acknowledge(12, true);
        fresh.wait().await.unwrap();
        drop(fresh);
        assert!(flow.begin(13).is_ok());
    }
}
