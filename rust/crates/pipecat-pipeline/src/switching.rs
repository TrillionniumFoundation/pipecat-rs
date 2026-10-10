//! Service selection over owned native parallel branches.
use super::{Branch, Engine, OwnedTask, ParallelPipelineConfig};
use pipecat_core::{
    async_trait, Direction, ErrorCategory, ErrorFrame, Frame, FrameData, FrameProcessor,
    LocalFrameTag, LocalProcessorReference, ObservedProcessor, ProcessorContext, ProcessorError,
    ProcessorHealth, ProcessorHealthController, ProcessorReference, StartFrame, ToolSet,
};
use pipecat_services::{InferenceOptions, LlmInference, ServiceError};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ServiceSwitcherStrategy {
    #[default]
    Manual,
    /// Only an attributed error from the active, currently unusable member
    /// triggers failover. Healthy transient errors do not switch providers.
    Failover,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwitchReason {
    Initial,
    Manual,
    Failover,
}
/// Latest-value selection notification. Revisions reveal coalesced events.
/// This acknowledges selection, not metadata receipt or a root interruption
/// barrier. A service may expose no metadata; nested leaf metadata is unsupported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceSelection {
    pub active_index: usize,
    pub revision: u64,
    pub reason: SwitchReason,
}

/// Route work to one service while retaining real owned workers for all members.
/// A successful switch interrupts in-flight work; it never replays input.
/// Metadata refresh applies to direct service members. Nested compound members
/// support routing and health, but do not retarget/forward leaf metadata.
pub struct ServiceSwitcher {
    pub(crate) engine: Arc<Engine>,
    pub(crate) routing: Arc<SwitchRouting>,
}
impl ServiceSwitcher {
    pub fn new(services: Vec<Arc<dyn FrameProcessor>>) -> Result<Self, ProcessorError> {
        Self::with_strategy(services, ServiceSwitcherStrategy::Manual)
    }
    pub fn with_strategy(
        services: Vec<Arc<dyn FrameProcessor>>,
        strategy: ServiceSwitcherStrategy,
    ) -> Result<Self, ProcessorError> {
        Self::with_config(services, strategy, ParallelPipelineConfig::default())
    }
    pub fn with_config(
        services: Vec<Arc<dyn FrameProcessor>>,
        strategy: ServiceSwitcherStrategy,
        config: ParallelPipelineConfig,
    ) -> Result<Self, ProcessorError> {
        Self::build(services, strategy, config, false)
    }
    fn build(
        services: Vec<Arc<dyn FrameProcessor>>,
        strategy: ServiceSwitcherStrategy,
        config: ParallelPipelineConfig,
        llm: bool,
    ) -> Result<Self, ProcessorError> {
        if services.is_empty() {
            return Err(ProcessorError::new(
                "service switcher needs at least one service",
            ));
        }
        for (i, service) in services.iter().enumerate() {
            if services[..i]
                .iter()
                .any(|prior| Arc::ptr_eq(prior, service))
            {
                return Err(ProcessorError::new(
                    "a service cannot occupy multiple switcher branches",
                ));
            }
        }
        let health: Vec<_> = services
            .iter()
            .map(|s| s.health().unwrap_or_default())
            .collect();
        let (aggregate, aggregate_owner) =
            ProcessorHealth::read_only(health.iter().any(ProcessorHealth::is_usable));
        let (selection, _) = watch::channel(ServiceSelection {
            active_index: 0,
            revision: 0,
            reason: SwitchReason::Initial,
        });
        let routing = Arc::new(SwitchRouting {
            state: Mutex::new(RoutingState {
                identities: vec![None; services.len()],
                metadata_pending: false,
                pending_errors: vec![],
            }),
            services,
            health,
            aggregate,
            aggregate_owner,
            selection,
            strategy,
            private_copy_tag: LocalFrameTag::new(),
            llm,
        });
        let branches: Vec<Branch> = (0..routing.services.len())
            .map(|index| {
                vec![Arc::new(Member {
                    routing: Arc::downgrade(&routing),
                    service: routing.services[index].clone(),
                    index,
                }) as Arc<dyn FrameProcessor>]
            })
            .collect();
        let engine = Engine::new(branches, config, None, Some(routing.clone()))?;
        Ok(Self { engine, routing })
    }
    pub fn services(&self) -> &[Arc<dyn FrameProcessor>] {
        &self.routing.services
    }
    pub fn active_index(&self) -> usize {
        self.routing.active()
    }
    pub fn active_service(&self) -> Arc<dyn FrameProcessor> {
        self.routing.services[self.active_index()].clone()
    }
    pub fn usable_services(&self) -> Vec<usize> {
        (0..self.routing.services.len())
            .filter(|&index| self.routing.member_usable(index))
            .collect()
    }
    pub fn is_usable(&self) -> bool {
        self.routing.refresh_health();
        self.routing.aggregate.is_usable()
    }
    /// Restore an individual member after application recovery. This does not
    /// select it, retry work, or rescind a worker shutdown already requested.
    pub fn service_health(&self, index: usize) -> Option<ProcessorHealth> {
        self.routing.health.get(index).cloned()
    }
    pub fn watch_selection(&self) -> watch::Receiver<ServiceSelection> {
        self.routing.selection.subscribe()
    }
    pub fn watch_health(&self) -> watch::Receiver<pipecat_core::ProcessorHealthSnapshot> {
        self.routing.refresh_health();
        self.routing.aggregate.watch()
    }
    /// Local references remain unambiguous even when member names are equal.
    pub fn switch_frame(&self, index: usize) -> Option<Frame> {
        self.services().get(index).map(|service| {
            Frame::new(FrameData::ManuallySwitchService(ProcessorReference::Local(
                LocalProcessorReference(service.clone()),
            )))
        })
    }
}
#[async_trait]
impl FrameProcessor for ServiceSwitcher {
    fn name(&self) -> &str {
        "ServiceSwitcher"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.routing.refresh_health();
        Some(self.routing.aggregate.clone())
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.engine.state.lock().unwrap().context = Some(ctx.clone());
        self.routing.watch_members(&self.engine);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.engine.process(frame, direction, ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let result = self.engine.cleanup().await;
        self.routing.clear_runtime_state();
        result
    }
}

/// LLM selection with shared tool definitions and explicit inference admission.
///
/// Fresh context or an explicit LlmRun is required for another inference. No
/// cached context, failed request, or emitted tool call is automatically replayed.
/// Place one native ToolExecutor after this processor to share callback handlers,
/// cancellation options and duplicate-call tracking across all member LLMs.
pub struct LlmSwitcher {
    inner: ServiceSwitcher,
    llms: Vec<Arc<dyn LlmInference>>,
}
impl LlmSwitcher {
    pub fn new(llms: Vec<Arc<dyn LlmInference>>) -> Result<Self, ProcessorError> {
        Self::with_strategy(llms, ServiceSwitcherStrategy::Manual)
    }
    pub fn with_strategy(
        llms: Vec<Arc<dyn LlmInference>>,
        strategy: ServiceSwitcherStrategy,
    ) -> Result<Self, ProcessorError> {
        Self::with_config(llms, strategy, ParallelPipelineConfig::default())
    }
    pub fn with_config(
        llms: Vec<Arc<dyn LlmInference>>,
        strategy: ServiceSwitcherStrategy,
        config: ParallelPipelineConfig,
    ) -> Result<Self, ProcessorError> {
        let processors = llms
            .iter()
            .map(|llm| llm.clone() as Arc<dyn FrameProcessor>)
            .collect();
        let inner = ServiceSwitcher::build(processors, strategy, config, true)?;
        Ok(Self { inner, llms })
    }
    pub fn service_switcher(&self) -> &ServiceSwitcher {
        &self.inner
    }
    pub fn active_llm(&self) -> Arc<dyn LlmInference> {
        self.llms[self.inner.active_index()].clone()
    }
    pub fn llms(&self) -> &[Arc<dyn LlmInference>] {
        &self.llms
    }
    /// Use the member selected when this future is first polled. The member
    /// snapshots its current settings; later switches neither retarget nor
    /// retry this call. Caller cancellation owns the out-of-band request.
    pub async fn run_inference(
        &self,
        context: pipecat_core::LlmContextData,
        options: InferenceOptions,
    ) -> Result<Option<String>, ServiceError> {
        let index = self.inner.active_index();
        if !self.inner.routing.member_usable(index) {
            return Err(ServiceError::InvalidRequest(
                "active LLM is not usable".into(),
            ));
        }
        self.llms[index].run_inference(context, options).await
    }
}
#[async_trait]
impl FrameProcessor for LlmSwitcher {
    fn name(&self) -> &str {
        "LlmSwitcher"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        FrameProcessor::health(&self.inner)
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.process(frame, direction, ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

struct RoutingState {
    identities: Vec<Option<ObservedProcessor>>,
    metadata_pending: bool,
    pending_errors: Vec<ErrorFrame>,
}
pub(crate) struct SwitchRouting {
    services: Vec<Arc<dyn FrameProcessor>>,
    health: Vec<ProcessorHealth>,
    aggregate: ProcessorHealth,
    aggregate_owner: ProcessorHealthController,
    selection: watch::Sender<ServiceSelection>,
    strategy: ServiceSwitcherStrategy,
    private_copy_tag: LocalFrameTag,
    llm: bool,
    state: Mutex<RoutingState>,
}
impl SwitchRouting {
    pub(crate) fn active(&self) -> usize {
        self.selection.borrow().active_index
    }
    fn member_usable(&self, index: usize) -> bool {
        // A nested compound can refresh its derived view synchronously here.
        self.services[index]
            .health()
            .unwrap_or_else(|| self.health[index].clone())
            .is_usable()
    }
    fn refresh_health(&self) {
        self.aggregate_owner
            .set_usable((0..self.services.len()).any(|index| self.member_usable(index)));
    }
    fn resolve(&self, target: &ProcessorReference) -> Option<usize> {
        let matches: Vec<_> = self
            .services
            .iter()
            .enumerate()
            .filter_map(|(index, service)| {
                let matches = match target {
                    ProcessorReference::Local(reference) => Arc::ptr_eq(service, &reference.0),
                    ProcessorReference::Name(name) => service.name() == name,
                };
                matches.then_some(index)
            })
            .collect();
        (matches.len() == 1).then(|| matches[0])
    }
    fn watch_members(self: &Arc<Self>, engine: &Arc<Engine>) {
        for health in &self.health {
            let mut health = health.watch();
            let mut shutdown = engine.shutdown.subscribe();
            let weak = Arc::downgrade(self);
            engine
                .tasks
                .lock()
                .unwrap()
                .push(OwnedTask(tokio::spawn(async move {
                    loop {
                        if *shutdown.borrow() {
                            break;
                        }
                        if let Some(routing) = weak.upgrade() {
                            routing.refresh_health();
                        } else {
                            break;
                        }
                        tokio::select! {
                            biased;
                            _ = shutdown.changed() => {},
                            result = health.changed() => { if result.is_err() { break; } },
                        }
                    }
                })));
        }
    }
    fn clear_runtime_state(&self) {
        let mut state = self.state.lock().unwrap();
        state.pending_errors.clear();
        state.metadata_pending = false;
        state.identities.fill(None);
    }
    pub(crate) fn input_targets(
        &self,
        frame: &Frame,
    ) -> Result<Vec<(usize, Frame)>, ProcessorError> {
        let active = self.active();
        let mut active_frame = frame.clone();
        if settings_frame(frame) {
            active_frame.interruptible = false;
        }
        let mut targets = vec![(active, active_frame)];
        for index in 0..self.services.len() {
            if index != active && self.settings_target(frame, index) {
                let mut copy = frame.duplicate().with_interruptible(false);
                self.private_copy_tag.mark(&mut copy)?;
                targets.push((index, copy));
            }
        }
        Ok(targets)
    }
    fn settings_target(&self, frame: &Frame, index: usize) -> bool {
        let routing = match &frame.data {
            FrameData::ServiceUpdateSettings(s) => Some((&s.service, s.reach_inactive_services)),
            FrameData::LlmUpdateSettings(s) => Some((&s.service, s.reach_inactive_services)),
            FrameData::TtsUpdateSettings(s) => Some((&s.service, s.reach_inactive_services)),
            FrameData::SttUpdateSettings(s) => Some((&s.service, s.reach_inactive_services)),
            FrameData::LlmSetTools(_) | FrameData::LlmSetToolChoice(_) if self.llm => return true,
            _ => None,
        };
        routing.is_some_and(|(target, all)| match target {
            Some(target) => self.resolve(target) == Some(index),
            None => all,
        })
    }
    pub(crate) fn context_tools(&self, frame: &Frame) -> Result<Option<Frame>, ProcessorError> {
        if !self.llm {
            return Ok(None);
        }
        let tools = match &frame.data {
            FrameData::LlmContext(context) => ToolSet::Functions(context.tools.clone()),
            FrameData::LlmContextDetailed(context) => context.context.tools.clone(),
            _ => return Ok(None),
        };
        let mut frame = Frame::new(FrameData::LlmSetTools(tools));
        self.private_copy_tag.mark(&mut frame)?;
        Ok(Some(frame))
    }
    pub(crate) fn gate_input(&self, branch: usize, frame: &Frame) -> bool {
        lifecycle(frame)
            || self.active() == branch
            || self.settings_target(frame, branch)
            || (settings_frame(frame) && !frame.interruptible)
    }
    /// Called under Engine.emission, serializing switches with branch output.
    pub(crate) async fn switch_to(
        &self,
        engine: &Engine,
        target: usize,
        reason: SwitchReason,
        ctx: &ProcessorContext,
    ) -> Result<bool, ProcessorError> {
        if !self.member_usable(target) || engine.state.lock().unwrap().terminal.is_some() {
            return Ok(false);
        }
        let old = self.active();
        if old != target && engine.state.lock().unwrap().started {
            let frame = Frame::interruption();
            if !ctx.broadcast_interruption_frame(&frame).await? {
                return Ok(false);
            }
            // This exact token gives parent and children one causal cutoff.
            engine.state.lock().unwrap().interruptions.insert(frame.id);
            engine.state.lock().unwrap().seen.insert(frame.id);
            engine.fanout(&frame, Direction::Downstream, ctx)?;
        }
        self.selection.send_modify(|selection| {
            selection.active_index = target;
            selection.revision = selection.revision.saturating_add(1);
            selection.reason = reason;
        });
        self.refresh_health();
        self.request_metadata(engine, target, ctx)?;
        Ok(true)
    }
    fn request_metadata(
        &self,
        engine: &Engine,
        target: usize,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let sender = engine.state.lock().unwrap().senders.get(target).cloned();
        if let Some(sender) = sender {
            let request = Frame::new(FrameData::ServiceSwitcherRequestMetadata(
                ProcessorReference::Local(LocalProcessorReference(self.services[target].clone())),
            ));
            sender
                .try_send_from(request, Direction::Downstream, ctx)
                .map_err(super::branch_admission)?;
        } else {
            self.state.lock().unwrap().metadata_pending = true;
        }
        Ok(())
    }
    pub(crate) fn flush_pending_metadata(
        &self,
        engine: &Engine,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let pending = std::mem::take(&mut self.state.lock().unwrap().metadata_pending);
        if pending {
            self.request_metadata(engine, self.active(), ctx)?;
        }
        Ok(())
    }
    pub(crate) async fn manual(
        &self,
        engine: &Engine,
        frame: &Frame,
        ctx: &ProcessorContext,
    ) -> Result<bool, ProcessorError> {
        let FrameData::ManuallySwitchService(target) = &frame.data else {
            return Ok(false);
        };
        let Some(index) = self.resolve(target) else {
            return Ok(false);
        };
        self.switch_to(engine, index, SwitchReason::Manual, ctx)
            .await
    }
    /// Returns false when the output was consumed by switching or a closed gate.
    pub(crate) async fn capture(
        &self,
        engine: &Engine,
        branch: usize,
        frame: &Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<bool, ProcessorError> {
        self.refresh_health();
        if let Some(error) = frame.data.as_error_frame() {
            if error.fatal {
                return Ok(true);
            }
            let member = frame.error_origin().and_then(|origin| {
                self.state
                    .lock()
                    .unwrap()
                    .identities
                    .iter()
                    .position(|identity| identity.as_ref() == Some(&origin.processor))
            });
            if direction == Direction::Upstream {
                if let Some(member) = member {
                    if member != self.active() {
                        return Ok(false);
                    }
                    if !self.member_usable(member) {
                        if self.strategy == ServiceSwitcherStrategy::Failover {
                            for offset in 1..self.services.len() {
                                let next = (member + offset) % self.services.len();
                                if self
                                    .switch_to(engine, next, SwitchReason::Failover, ctx)
                                    .await?
                                {
                                    return Ok(false);
                                }
                            }
                        }
                        self.refresh_health();
                        let mut error = ErrorFrame::new(format!(
                            "{} can no longer do its job: {}",
                            self.services[member].name(),
                            error.error
                        ));
                        error.category = ErrorCategory::Unknown;
                        if engine.state.lock().unwrap().started {
                            ctx.push_error(error).await?;
                        } else {
                            let mut state = self.state.lock().unwrap();
                            if state.pending_errors.len() >= engine.config.buffered_frames {
                                return Err(ProcessorError::fatal(
                                    "switcher startup error buffer is full",
                                ));
                            }
                            state.pending_errors.push(error);
                        }
                        return Ok(false);
                    }
                }
            }
        }
        Ok(self.output_allowed(branch, frame))
    }
    pub(crate) async fn report_pending_errors(
        &self,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let errors = std::mem::take(&mut self.state.lock().unwrap().pending_errors);
        for error in errors {
            self.refresh_health();
            ctx.push_error(error).await?;
        }
        Ok(())
    }
    pub(crate) fn output_allowed(&self, branch: usize, frame: &Frame) -> bool {
        if let Some(error) = frame.data.as_error_frame() {
            if error.fatal {
                return true;
            }
            let member = frame.error_origin().and_then(|origin| {
                self.state
                    .lock()
                    .unwrap()
                    .identities
                    .iter()
                    .position(|identity| identity.as_ref() == Some(&origin.processor))
            });
            return member.is_none_or(|member| member == self.active());
        }
        if self.private_copy_tag.contains(frame) {
            return false;
        }
        // Configuration must continue through the rest of the outer pipeline
        // even if its selected service changes before forwarding the original.
        if settings_frame(frame) {
            return true;
        }
        if self.active() != branch {
            return false;
        }
        if let FrameData::ServiceSwitcherRequestMetadata(target) = &frame.data {
            if self.resolve(target) == Some(self.active()) {
                return false;
            }
        }
        let metadata = match &frame.data {
            FrameData::ServiceMetadata(m) => Some(m.service_name.as_str()),
            FrameData::SttMetadata(m) => Some(m.base.service_name.as_str()),
            FrameData::LlmServiceMetadata(m) => Some(m.base.service_name.as_str()),
            _ => None,
        };
        metadata.is_none_or(|name| name == self.services[self.active()].name())
    }
}
fn settings_frame(frame: &Frame) -> bool {
    matches!(
        frame.data,
        FrameData::ServiceUpdateSettings(_)
            | FrameData::LlmUpdateSettings(_)
            | FrameData::TtsUpdateSettings(_)
            | FrameData::SttUpdateSettings(_)
            | FrameData::LlmSetTools(_)
            | FrameData::LlmSetToolChoice(_)
    )
}
pub(crate) fn lifecycle(frame: &Frame) -> bool {
    frame.data.is_start() || frame.is_terminal() || matches!(frame.data, FrameData::Interruption)
}
struct Member {
    routing: Weak<SwitchRouting>,
    service: Arc<dyn FrameProcessor>,
    index: usize,
}
#[async_trait]
impl FrameProcessor for Member {
    fn name(&self) -> &str {
        self.service.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.routing
            .upgrade()
            .map(|routing| routing.health[self.index].clone())
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.routing.upgrade().is_some_and(|routing| {
            routing.services[self.index].process_when_unusable(frame, direction)
        })
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let routing = self
            .routing
            .upgrade()
            .ok_or_else(|| ProcessorError::fatal("switcher owner dropped"))?;
        {
            let mut state = routing.state.lock().unwrap();
            state.identities[self.index] = Some(ctx.processor_identity());
        }
        routing.services[self.index].setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let routing = self
            .routing
            .upgrade()
            .ok_or_else(|| ProcessorError::fatal("switcher owner dropped"))?;
        if routing.gate_input(self.index, &frame) {
            routing.services[self.index]
                .process(frame, direction, ctx)
                .await
        } else {
            Ok(())
        }
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let routing = self
            .routing
            .upgrade()
            .ok_or_else(|| ProcessorError::fatal("switcher owner dropped"))?;
        routing.services[self.index].prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let Some(routing) = self.routing.upgrade() {
            routing.services[self.index].cleanup().await
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::{Pipeline, PipelineConfig, ServiceMetadataFrame};
    use std::time::Duration;

    struct SavedInvocation(Mutex<Option<ProcessorContext>>);
    #[async_trait]
    impl FrameProcessor for SavedInvocation {
        fn name(&self) -> &str {
            "saved"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::Text(_)) {
                *self.0.lock().unwrap() = Some(ctx.clone());
            }
            ctx.push(frame, direction).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.0.lock().unwrap().take();
            Ok(())
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn branch_capture_and_lifecycle_buffer_drop_stale_system_metadata_after_reactivation() {
        let service = Arc::new(SavedInvocation(Mutex::new(None)));
        let switcher = Arc::new(
            ServiceSwitcher::new(vec![
                service.clone(),
                Arc::new(pipecat_core::IdentityProcessor),
            ])
            .unwrap(),
        );
        let mut worker = Pipeline::new(vec![switcher.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(Frame::text("save"), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let out = worker.recv().await.unwrap();
                if matches!(out.frame.data, FrameData::Text(_)) {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let stale = service.0.lock().unwrap().as_ref().unwrap().clone();
        let context = switcher
            .engine
            .state
            .lock()
            .unwrap()
            .context
            .clone()
            .unwrap();
        {
            let _emission = switcher.engine.emission.lock().await;
            switcher
                .routing
                .switch_to(&switcher.engine, 1, SwitchReason::Manual, &context)
                .await
                .unwrap();
            switcher
                .routing
                .switch_to(&switcher.engine, 0, SwitchReason::Manual, &context)
                .await
                .unwrap();
        }
        // Wait for the cutoff to arrive at the member, independently of tasks.
        tokio::time::timeout(Duration::from_secs(3), async {
            while stale.is_current() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut metadata = Frame::new(FrameData::ServiceMetadata(ServiceMetadataFrame {
            service_name: "saved".into(),
            user_turn_strategies: None,
        }));
        metadata.metadata.insert("stale".into(), true.into());
        switcher
            .engine
            .capture(0, metadata.clone(), Direction::Downstream, &stale)
            .await
            .unwrap();
        // Buffered system outputs need the same explicit fence on final emission.
        switcher
            .engine
            .emit(vec![super::super::Output {
                branch: 0,
                item: pipecat_core::DirectedFrame::new(metadata, Direction::Downstream),
                context: context.forwarding_context(&stale),
            }])
            .await
            .unwrap();
        worker.sender().end().await.unwrap();
        let (outputs, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
        assert!(outputs
            .iter()
            .all(|out| !out.frame.metadata.contains_key("stale")));
    }
}
