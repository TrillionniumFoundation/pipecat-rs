//! Bridges native completion streams into interruption-aware core pipelines.
use crate::{frame_context, settings};
use crate::{CompletionRequest, LlmClient, ServiceError, StreamEvent};
use pipecat_core::{
    async_trait, Direction, ErrorCategory, ErrorFrame, Frame, FrameData, FrameProcessor,
    FunctionCall, LlmContextData, LlmContinuationBlock, LlmContinuationProvider,
    LlmServiceMetadataFrame, LlmSettings, Metric, NativeLlmContinuation, ProcessorContext,
    ProcessorError, ProcessorHealth, ProcessorReference, ServiceMetadataFrame, TextFrame,
};
use std::future::{poll_fn, Future};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use tokio::time::Instant;

/// Per-call overrides and finite accumulation limit for out-of-band inference.
#[derive(Debug, Clone)]
pub struct InferenceOptions {
    pub settings: LlmSettings,
    pub max_output_bytes: usize,
}
impl Default for InferenceOptions {
    fn default() -> Self {
        Self {
            settings: LlmSettings::default(),
            max_output_bytes: 1024 * 1024,
        }
    }
}
/// Explicit-context inference capability for a native LLM processor.
/// Calls perform provider inference when configured; they never execute tools.
#[async_trait]
pub trait LlmInference: FrameProcessor {
    async fn run_inference(
        &self,
        context: LlmContextData,
        options: InferenceOptions,
    ) -> Result<Option<String>, ServiceError>;
}

/// A text/context LLM processor. Slow network work is owned by the pipeline.
///
/// Context frames and message updates replace/extend the request context. An
/// external context aggregator owns conversation history; this processor does
/// not implicitly append assistant text or execute tool calls. Completions are
/// serialized, so overlapping context frames cannot interleave their output.
pub struct LlmProcessor {
    health: ProcessorHealth,
    name: String,
    client: LlmClient,
    state: Mutex<State>,
    completion_lock: Arc<tokio::sync::Mutex<()>>,
    should_report_ttfb: Arc<AtomicBool>,
    speculation_gate: crate::speculation::OutputGate,
}
#[derive(Clone)]
struct State {
    request: CompletionRequest,
    skip_tts: Option<bool>,
    speculation: Option<pipecat_core::NativeSpeculation>,
}
enum Consumption {
    Forward,
    Consumed,
    Run(Box<State>),
}
impl LlmProcessor {
    pub fn new(client: LlmClient, request: CompletionRequest) -> Self {
        Self {
            health: ProcessorHealth::default(),
            name: "LlmService".into(),
            client,
            state: Mutex::new(State {
                request,
                skip_tts: None,
                speculation: None,
            }),
            completion_lock: Arc::new(tokio::sync::Mutex::new(())),
            should_report_ttfb: Arc::new(AtomicBool::new(true)),
            speculation_gate: crate::speculation::OutputGate::new(Default::default()),
        }
    }
    /// Current bounded retention, for diagnostics without exposing generated text.
    pub async fn speculation_snapshot(&self) -> crate::SpeculationSnapshot {
        self.speculation_gate.snapshot().await
    }
    /// Configure finite eager output retention before starting the pipeline.
    pub fn with_speculation_limits(
        mut self,
        limits: crate::SpeculationLimits,
    ) -> Result<Self, ProcessorError> {
        self.speculation_gate = crate::speculation::OutputGate::new(limits.validate()?);
        Ok(self)
    }
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
    /// A clone is independent from pending and future frame updates.
    pub fn request_snapshot(&self) -> Result<CompletionRequest, ServiceError> {
        self.state
            .lock()
            .map(|state| state.request.clone())
            .map_err(|_| ServiceError::InvalidRequest("LLM state lock poisoned".into()))
    }
    /// Validate the entire prospective request before committing an atomic delta.
    pub fn update_settings(&self, delta: &LlmSettings) -> Result<(), ServiceError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ServiceError::InvalidRequest("LLM state lock poisoned".into()))?;
        let mut candidate = state.request.clone();
        settings::merge_settings(&mut candidate.settings, delta);
        self.client.request_body(&candidate)?;
        state.request = candidate;
        Ok(())
    }
    async fn metadata(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        ctx.broadcast(Frame::new(FrameData::LlmServiceMetadata(
            LlmServiceMetadataFrame {
                base: ServiceMetadataFrame {
                    service_name: self.name.clone(),
                    user_turn_strategies: None,
                },
                is_realtime_service: false,
            },
        )))
        .await
    }
    fn targets_self(&self, target: &Option<ProcessorReference>) -> bool {
        match target {
            None => true,
            Some(ProcessorReference::Name(name)) => name == &self.name,
            Some(ProcessorReference::Local(target)) => {
                // Trait-object vtables can differ across codegen units even
                // when both references point to the same processor.
                std::ptr::addr_eq(target.0.as_ref(), self as &dyn FrameProcessor)
            }
        }
    }
    fn consume(&self, frame: &Frame) -> Result<Consumption, ServiceError> {
        if let FrameData::LlmEnablePromptCaching { enable } = &frame.data {
            let mut delta = LlmSettings::default();
            delta
                .base
                .extra
                .insert("enable_prompt_caching".into(), serde_json::json!(enable));
            self.update_settings(&delta)?;
            return Ok(Consumption::Consumed);
        }
        if let FrameData::LlmUpdateSettings(update) = &frame.data {
            if !self.targets_self(&update.service) {
                return Ok(Consumption::Forward);
            }
            let delta = match &update.delta {
                Some(delta) => delta.clone(),
                None => settings::settings_from_mapping(&update.settings)?,
            };
            self.update_settings(&delta)?;
            return Ok(Consumption::Consumed);
        }
        if let FrameData::ServiceUpdateSettings(update) = &frame.data {
            if !self.targets_self(&update.service) {
                return Ok(Consumption::Forward);
            }
            let delta = match &update.delta {
                Some(delta) => settings::service_delta(delta),
                None => settings::settings_from_mapping(&update.settings)?,
            };
            self.update_settings(&delta)?;
            return Ok(Consumption::Consumed);
        }
        if let FrameData::LlmMessagesTransform(transform) = &frame.data {
            let original = self.request_snapshot()?.context.messages;
            let input: Vec<_> = original
                .iter()
                .map(frame_context::to_core_message)
                .collect();
            // Application code must not run under the state mutex: transforms
            // may inspect the service or update independent settings.
            let transformed = transform
                .transform
                .apply(input.clone())
                .map_err(|e| ServiceError::InvalidRequest(e.to_string()))?;
            let messages = restore_native_messages(&original, &input, &transformed)?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| ServiceError::InvalidRequest("LLM state lock poisoned".into()))?;
            let mut next = state.clone();
            next.request.context.messages = messages;
            self.client.request_body(&next.request)?;
            *state = next.clone();
            return Ok(if transform.run_llm.unwrap_or(false) {
                Consumption::Run(Box::new(next))
            } else {
                Consumption::Consumed
            });
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ServiceError::InvalidRequest("LLM state lock poisoned".into()))?;
        let mut next = state.clone();
        let run = match &frame.data {
            FrameData::LlmContext(context) => {
                if frame.native_speculation().is_some() {
                    return Err(ServiceError::InvalidRequest(
                        "native speculation requires a speculative detailed context".into(),
                    ));
                }
                next.request.context = frame_context::legacy_context(context);
                true
            }
            FrameData::LlmContextDetailed(context) => {
                if !context.speculation && frame.native_speculation().is_some() {
                    return Err(ServiceError::InvalidRequest(
                        "speculation capability on ordinary context".into(),
                    ));
                }
                if context.speculation {
                    let capability = frame
                        .native_speculation()
                        .ok_or_else(|| {
                            ServiceError::Unsupported(
                                "speculative context requires its native turn owner".into(),
                            )
                        })?
                        .clone();
                    if !capability.admits_completion() {
                        return Ok(Consumption::Consumed);
                    }
                    next.speculation = Some(capability);
                }
                let committed = next
                    .speculation
                    .as_ref()
                    .and_then(pipecat_core::NativeSpeculation::confirmed_context);
                next.request.context =
                    frame_context::context(committed.as_ref().unwrap_or(&context.context))?;
                true
            }
            FrameData::LlmRun => return Ok(Consumption::Run(Box::new(next))),
            FrameData::LlmMessagesAppend(messages) => {
                next.request
                    .context
                    .messages
                    .extend(messages.messages.iter().map(frame_context::legacy_message));
                messages.run_llm
            }
            FrameData::LlmMessagesUpdate(messages) => {
                next.request.context.messages = messages
                    .messages
                    .iter()
                    .map(frame_context::legacy_message)
                    .collect();
                messages.run_llm
            }
            FrameData::LlmMessagesAppendDetailed(messages) => {
                next.request.context.messages.extend(
                    messages
                        .messages
                        .iter()
                        .map(frame_context::message)
                        .collect::<Result<Vec<_>, _>>()?,
                );
                messages.run_llm.unwrap_or(false)
            }
            FrameData::LlmMessagesUpdateDetailed(messages) => {
                next.request.context.messages = messages
                    .messages
                    .iter()
                    .map(frame_context::message)
                    .collect::<Result<_, _>>()?;
                messages.run_llm.unwrap_or(false)
            }
            FrameData::LlmSetTools(tools) => {
                next.request.context.tools = frame_context::tools(tools)?;
                false
            }
            FrameData::LlmSetToolChoice(choice) => {
                next.request.context.tool_choice = Some(frame_context::tool_choice(choice));
                false
            }
            FrameData::LlmConfigureOutput { skip_tts } => {
                state.skip_tts = Some(*skip_tts);
                return Ok(Consumption::Consumed);
            }
            _ => return Ok(Consumption::Forward),
        };
        self.client.request_body(&next.request)?;
        if let Some(capability) = &next.speculation {
            if !capability.claim_inference() {
                return Ok(Consumption::Consumed);
            }
            // The provisional user message must never replace the live request.
        } else {
            *state = next.clone();
        }
        Ok(if run {
            Consumption::Run(Box::new(next))
        } else {
            Consumption::Consumed
        })
    }
    async fn launch(&self, state: State, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Some(capability) = &state.speculation {
            if !capability.bind_admission(ctx.clone(), ctx.health().generation()) {
                return Ok(());
            }
        }
        self.speculation_gate
            .begin(state.speculation.clone())
            .await?;
        let client = self.client.clone();
        let should_report_ttfb = self.should_report_ttfb.clone();
        let output = CompletionOutput {
            context: ctx.clone(),
            gate: self.speculation_gate.clone(),
            speculation: state.speculation.clone(),
            health_generation: ctx.health().generation(),
        };
        let mut guard = Box::pin(self.completion_lock.clone().lock_owned());
        // Reserve the FIFO position before background task scheduling can reorder it.
        let acquired = poll_fn(|cx| Poll::Ready(guard.as_mut().poll(cx))).await;
        ctx.spawn("llm-completion", true, async move {
            let completion = async {
                if !output.admits_completion() {
                    return Ok(());
                }
                let _guard = match acquired {
                    Poll::Ready(guard) => guard,
                    Poll::Pending => guard.await,
                };
                // A source can retire while this completion waits in the FIFO.
                if !output.admits_completion() {
                    return Ok(());
                }
                process_completion(
                    client,
                    state.request,
                    state.skip_tts,
                    output.clone(),
                    should_report_ttfb,
                )
                .await
            };
            let result = output.guard_completion(completion).await;
            let result = match result {
                Err(error) if output.speculation.is_some() => {
                    output
                        .speculation
                        .as_ref()
                        .expect("speculative output")
                        .cancel();
                    output
                        .push_error(ErrorFrame {
                            error: error.message,
                            fatal: error.fatal,
                            processor: None,
                            category: error.category,
                        })
                        .await?;
                    Ok(())
                }
                result => result,
            };
            output.gate.discard_cancelled().await?;
            if let Some(capability) = &output.speculation {
                if capability.status() == pipecat_core::SpeculationStatus::Cancelled {
                    crate::speculation::withdraw(&output.context, capability.clone()).await?;
                }
            }
            result
        })?;
        Ok(())
    }
}

/// Equal core projections can conceal distinct native forms, such as inline
/// images and files. Keep every retained occurrence, and reject partial
/// selection when the projection cannot identify which original survived.
fn restore_native_messages(
    original: &[crate::Message],
    input: &[pipecat_core::ContextMessage],
    transformed: &[pipecat_core::ContextMessage],
) -> Result<Vec<crate::Message>, ServiceError> {
    let mut consumed = vec![false; input.len()];
    transformed
        .iter()
        .map(|message| {
            let indices: Vec<_> = input
                .iter()
                .enumerate()
                .filter_map(|(index, prior)| (prior == message).then_some(index))
                .collect();
            let Some(&first) = indices.first() else {
                return frame_context::message(message);
            };
            if indices.iter().all(|&index| original[index] == original[first]) {
                return Ok(original[first].clone());
            }
            if transformed.iter().filter(|other| *other == message).count() != indices.len() {
                return Err(ServiceError::Unsupported(
                    "message transform ambiguously selects distinct native content with identical core projections".into(),
                ));
            }
            let index = indices
                .into_iter()
                .find(|&index| !consumed[index])
                .expect("retained occurrence counts match");
            consumed[index] = true;
            Ok(original[index].clone())
        })
        .collect()
}

#[async_trait]
impl FrameProcessor for LlmProcessor {
    fn name(&self) -> &str {
        &self.name
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    fn process_when_unusable(&self, frame: &Frame, _: Direction) -> bool {
        !matches!(
            frame.data,
            FrameData::LlmContext(_)
                | FrameData::LlmContextDetailed(_)
                | FrameData::LlmRun
                | FrameData::LlmMessagesAppend(_)
                | FrameData::LlmMessagesUpdate(_)
                | FrameData::LlmMessagesAppendDetailed(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::LlmMessagesTransform(_)
        )
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() {
            ctx.push(frame, direction).await?;
            return self.metadata(ctx).await;
        }
        if let FrameData::ServiceSwitcherRequestMetadata(target) = &frame.data {
            if self.targets_self(&Some(target.clone())) {
                self.metadata(ctx).await?;
            }
            return ctx.push(frame, direction).await;
        }
        if direction == Direction::Downstream
            && matches!(&frame.data, FrameData::FlushDetailed(flush)
                if !flush.returning && flush.origin.as_deref() == Some("pipecat.flows"))
        {
            let output = ctx.clone();
            let gate = self.speculation_gate.clone();
            let mut guard = Box::pin(self.completion_lock.clone().lock_owned());
            // Join the same FIFO as completions at admission, before spawning.
            // Waiting inside process() would prevent interruption/terminal work.
            let acquired = poll_fn(|cx| Poll::Ready(guard.as_mut().poll(cx))).await;
            ctx.spawn("llm-flow-fence", false, async move {
                let _guard = match acquired {
                    Poll::Ready(guard) => guard,
                    Poll::Pending => guard.await,
                };
                gate.push(frame, direction, &output, None).await
            })?;
            return Ok(());
        }
        if direction == Direction::Downstream
            && matches!(frame.data, FrameData::UserStoppedSpeaking)
        {
            if let Some(capability) = frame.native_speculation() {
                if let Some(context) = capability
                    .confirmed_context()
                    .filter(|_| capability.status() == pipecat_core::SpeculationStatus::Confirmed)
                {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("LLM state lock poisoned"))?;
                    let mut next = state.clone();
                    next.request.context = frame_context::context(&context)
                        .map_err(|error| ProcessorError::new(error.to_string()))?;
                    self.client
                        .request_body(&next.request)
                        .map_err(|error| ProcessorError::new(error.to_string()))?;
                    *state = next;
                }
            }
        }
        if matches!(
            frame.data,
            FrameData::EagerEndOfTurnCancel
                | FrameData::UserStoppedSpeaking
                | FrameData::UserStartedSpeaking
                | FrameData::Interruption
        ) || frame.is_terminal()
        {
            return self.speculation_gate.control(frame, direction, ctx).await;
        }
        match self.consume(&frame) {
            Ok(Consumption::Run(state)) => self.launch(*state, ctx).await?,
            Ok(Consumption::Forward) => {
                self.speculation_gate
                    .push(frame, direction, ctx, None)
                    .await?
            }
            Ok(Consumption::Consumed) => {}
            Err(error) => {
                if frame
                    .native_speculation()
                    .is_some_and(|capability| !capability.source_is_current())
                {
                    return Ok(());
                }
                let proposal = matches!(
                    frame.data,
                    FrameData::LlmUpdateSettings(_)
                        | FrameData::ServiceUpdateSettings(_)
                        | FrameData::LlmEnablePromptCaching { .. }
                );
                ctx.push_error_with_permanence_override(
                    error_frame(error),
                    proposal.then_some(false),
                )
                .await?;
            }
        }
        Ok(())
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.speculation_gate.close().await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.speculation_gate.close().await
    }
}

#[async_trait]
impl LlmInference for LlmProcessor {
    /// Snapshot current settings and use only the supplied context. Dropping
    /// this future drops its stream and cancels the owned HTTP request. A
    /// subsequent switch/settings update does not change an admitted call.
    async fn run_inference(
        &self,
        context: LlmContextData,
        options: InferenceOptions,
    ) -> Result<Option<String>, ServiceError> {
        if options.max_output_bytes == 0 {
            return Err(ServiceError::InvalidRequest(
                "inference output limit must be positive".into(),
            ));
        }
        if !self.health.is_usable() {
            return Err(ServiceError::InvalidRequest(
                "LLM service is not usable".into(),
            ));
        }
        let mut request = self.request_snapshot()?;
        request.context = frame_context::context(&context)?;
        settings::merge_settings(&mut request.settings, &options.settings);
        let mut stream = self.client.stream(request)?;
        let mut text = String::new();
        while let Some(event) = stream.next().await {
            if let StreamEvent::TextDelta(delta) = event? {
                if delta.len() > options.max_output_bytes.saturating_sub(text.len()) {
                    return Err(ServiceError::Protocol(
                        "inference text exceeds output limit".into(),
                    ));
                }
                text.push_str(&delta);
            }
        }
        Ok((!text.is_empty()).then_some(text))
    }
}

#[derive(Clone)]
struct CompletionOutput {
    context: ProcessorContext,
    gate: crate::speculation::OutputGate,
    speculation: Option<pipecat_core::NativeSpeculation>,
    health_generation: pipecat_core::ProcessorHealthGeneration,
}
impl CompletionOutput {
    async fn guard_completion<F>(&self, completion: F) -> Result<(), ProcessorError>
    where
        F: Future<Output = Result<(), ProcessorError>>,
    {
        let Some(capability) = &self.speculation else {
            return completion.await;
        };
        let mut changed = capability.watch();
        let mut health_changed = self.context.health().watch();
        tokio::pin!(completion);
        let guarded = poll_fn(|cx| {
            // Check on every poll, including FIFO waits. A future may wake
            // without a watch change or another pass through the outer loop.
            if !self.admits_completion() {
                return Poll::Ready(Ok(()));
            }
            completion.as_mut().poll(cx)
        });
        tokio::pin!(guarded);
        loop {
            if !self.admits_completion() {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = changed.changed() => {},
                _ = health_changed.changed() => {},
                result = &mut guarded => return result,
            }
        }
    }

    fn admits_completion(&self) -> bool {
        let Some(capability) = &self.speculation else {
            return true;
        };
        if !self.context.is_current()
            || self.context.health().generation() != self.health_generation
            || !capability.admits_completion()
        {
            capability.cancel();
            return false;
        }
        true
    }
    fn configuration(&self) -> &pipecat_core::StartFrame {
        self.context.configuration()
    }
    fn name(&self) -> &str {
        self.context.name()
    }
    async fn push(&self, frame: Frame, direction: Direction) -> Result<(), ProcessorError> {
        if !self.admits_completion() {
            return Ok(());
        }
        self.gate
            .push(frame, direction, &self.context, self.speculation.as_ref())
            .await
    }
    async fn push_error(&self, error: ErrorFrame) -> Result<(), ProcessorError> {
        // Failure reporting can follow this completion's own cancellation.
        // It must still reject retired sources without hiding current errors.
        if self
            .speculation
            .as_ref()
            .is_some_and(|capability| !capability.source_is_current())
        {
            return Ok(());
        }
        self.context
            .push_error_if_health_generation(error, false, self.health_generation)
            .await
            .map(|_| ())
    }
    async fn admit_tool_intent(&self) -> Result<bool, ProcessorError> {
        let Some(capability) = &self.speculation else {
            return Ok(true);
        };
        if !self.context.is_current()
            || self.context.health().generation() != self.health_generation
        {
            capability.cancel();
        }
        if capability.admit_tool_intent() {
            return Ok(true);
        }
        crate::speculation::withdraw(&self.context, capability.clone()).await?;
        Ok(false)
    }
}

async fn process_completion(
    client: LlmClient,
    request: CompletionRequest,
    skip_tts: Option<bool>,
    ctx: CompletionOutput,
    should_report_ttfb: Arc<AtomicBool>,
) -> Result<(), ProcessorError> {
    let started = Instant::now();
    let mut first_output_elapsed = None;
    let mut answer_reported = false;
    let mut thinking = false;
    let mut explicit_thinking = false;
    let mut failed = false;
    let mut calls = vec![];
    let mut continuation = None;
    let anthropic = client.provider() == crate::Provider::Anthropic;
    let effective_model = match &request.settings.base.model {
        pipecat_core::Setting::Value(model) => model,
        _ => &request.model,
    };
    let gemini_requires_signature = client.provider() == crate::Provider::Gemini
        && crate::adapters::gemini_requires_tool_signature(effective_model);
    let history_provider = match client.provider() {
        crate::Provider::Anthropic => Some(LlmContinuationProvider::Anthropic),
        crate::Provider::Gemini => Some(LlmContinuationProvider::Gemini),
        _ => None,
    };
    let response_id = NativeLlmContinuation::next_response_id()
        .map_err(|error| ProcessorError::new(error.to_string()))?;
    let mut start = response_boundary(FrameData::LlmResponseStart, skip_tts);
    if let Some(provider) = history_provider {
        start
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(response_id, provider)
                    .map_err(|error| ProcessorError::new(error.to_string()))?,
            ))
            .map_err(|error| ProcessorError::new(error.to_string()))?;
    }
    ctx.push(start, Direction::Downstream).await?;
    // Pushing the boundary may yield while the source retires. Do not even
    // construct a provider stream after that admission has been lost.
    if !ctx.admits_completion() {
        return Ok(());
    }
    // Claim only when this request actually starts, after FIFO admission and
    // the response-start boundary. Disabled or cancelled queued work must not
    // consume the instance's initial report. Once exhausted, later Start frames
    // or switching back to every-request reporting do not rearm the collector.
    let report_timing = ctx.configuration().enable_metrics
        && should_report_ttfb
            .compare_exchange(
                true,
                !ctx.configuration().report_only_initial_ttfb,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok();
    let mut stream = match client.stream(request) {
        Ok(stream) => stream,
        Err(error) => {
            ctx.push_error(error_frame(error)).await?;
            let mut end = response_boundary(FrameData::LlmResponseEnd, skip_tts);
            end.metadata.insert("failed".into(), true.into());
            ctx.push(end, Direction::Downstream).await?;
            return Ok(());
        }
    };
    while let Some(event) = stream.next().await {
        // Capture this event before any output await. Publishing TTFB must not
        // inflate the same event's answer-time breakdown. This is processor
        // observation time, not a raw network-arrival timestamp.
        let elapsed = started.elapsed();
        if first_output_elapsed.is_none()
            && matches!(
                &event,
                Ok(StreamEvent::ResponseProgress
                    | StreamEvent::TextDelta(_)
                    | StreamEvent::ThoughtDelta(_)
                    | StreamEvent::ToolCall(_))
            )
        {
            first_output_elapsed = Some(elapsed);
            if report_timing {
                ctx.push(
                    Frame::new(FrameData::Metrics(vec![Metric::Ttfb {
                        processor: ctx.name().into(),
                        seconds: elapsed.as_secs_f64(),
                    }])),
                    Direction::Downstream,
                )
                .await?;
            }
        }
        if !answer_reported
            && matches!(
                &event,
                Ok(StreamEvent::TextDelta(_)
                    | StreamEvent::ToolCallDelta { .. }
                    | StreamEvent::ToolCall(_))
            )
        {
            // A tool start answers while arguments are still incomplete. This
            // measurement never admits or executes a tool. Empty text deltas
            // remain valid answers where the provider emits them.
            answer_reported = true;
            if let Some(first) = first_output_elapsed.filter(|_| report_timing) {
                ctx.push(
                    Frame::new(FrameData::Metrics(vec![Metric::Ttfat {
                        processor: ctx.name().into(),
                        ttfat: elapsed.as_secs_f64(),
                        ttfb: first.as_secs_f64(),
                        thinking_time: elapsed.saturating_sub(first).as_secs_f64(),
                    }])),
                    Direction::Downstream,
                )
                .await?;
            }
        }
        match event {
            Ok(
                StreamEvent::Started
                | StreamEvent::ResponseProgress
                | StreamEvent::ToolCallDelta { .. },
            ) => {}
            Ok(StreamEvent::ThoughtStart | StreamEvent::ThoughtEnd) => {
                explicit_thinking = true;
                let data = if matches!(event, Ok(StreamEvent::ThoughtStart)) {
                    FrameData::LlmThoughtStart
                } else {
                    FrameData::LlmThoughtEnd
                };
                ctx.push(Frame::new(data), Direction::Downstream).await?;
            }
            Ok(StreamEvent::GeminiCheckpoint(messages)) => {
                match native_gemini_history(response_id, messages, true) {
                    Ok(history) => {
                        let mut frame = Frame::new(FrameData::LlmContinuationCheckpoint);
                        frame
                            .set_native_llm_continuation(Arc::new(history))
                            .map_err(|error| ProcessorError::new(error.to_string()))?;
                        ctx.push(frame, Direction::Downstream).await?;
                    }
                    Err(error) => {
                        failed = true;
                        ctx.push_error(error_frame(error)).await?;
                    }
                }
            }
            Ok(StreamEvent::GeminiHistory(messages)) => {
                match native_gemini_history(response_id, messages, false) {
                    Ok(history) => {
                        if gemini_requires_signature
                            && history
                                .blocks()
                                .iter()
                                .any(|b| matches!(b, LlmContinuationBlock::ToolCall(_)))
                            && !history
                                .blocks()
                                .iter()
                                .find_map(|b| match b {
                                    LlmContinuationBlock::GeminiToolMetadata {
                                        signature, ..
                                    } => Some(signature.is_some()),
                                    _ => None,
                                })
                                .unwrap_or(false)
                        {
                            failed = true;
                            ctx.push_error(error_frame(ServiceError::Protocol(
                                "native Gemini tool group is missing its signed anchor".into(),
                            )))
                            .await?;
                        } else {
                            continuation = Some(Arc::new(history));
                        }
                    }
                    Err(error) => {
                        failed = true;
                        ctx.push_error(error_frame(error)).await?;
                    }
                }
            }
            Ok(StreamEvent::AnthropicThoughtStart) => {
                ctx.push(
                    Frame::new(FrameData::LlmThoughtStart),
                    Direction::Downstream,
                )
                .await?;
                thinking = true;
            }
            Ok(StreamEvent::AnthropicThoughtEnd) => {
                ctx.push(Frame::new(FrameData::LlmThoughtEnd), Direction::Downstream)
                    .await?;
                thinking = false;
            }
            Ok(StreamEvent::AnthropicCheckpoint(messages)) => {
                match native_history(response_id, messages, true) {
                    Ok(history) => {
                        let mut frame = Frame::new(FrameData::LlmContinuationCheckpoint);
                        frame
                            .set_native_llm_continuation(Arc::new(history))
                            .map_err(|error| ProcessorError::new(error.to_string()))?;
                        ctx.push(frame, Direction::Downstream).await?;
                    }
                    Err(error) => {
                        failed = true;
                        ctx.push_error(error_frame(error)).await?;
                    }
                }
            }
            Ok(StreamEvent::AnthropicHistory(messages)) => {
                match native_history(response_id, messages, false) {
                    Ok(history) => continuation = Some(Arc::new(history)),
                    Err(error) => {
                        failed = true;
                        ctx.push_error(error_frame(error)).await?;
                    }
                }
            }
            Ok(StreamEvent::TextDelta(text)) => {
                if thinking && !anthropic && !explicit_thinking {
                    ctx.push(Frame::new(FrameData::LlmThoughtEnd), Direction::Downstream)
                        .await?;
                    thinking = false;
                }
                let mut text = TextFrame::llm(text);
                text.skip_tts = skip_tts;
                ctx.push(Frame::new(FrameData::LlmText(text)), Direction::Downstream)
                    .await?;
            }
            Ok(StreamEvent::ThoughtDelta(text)) => {
                if !thinking && !explicit_thinking {
                    ctx.push(
                        Frame::new(FrameData::LlmThoughtStart),
                        Direction::Downstream,
                    )
                    .await?;
                    thinking = true;
                }
                let mut text = TextFrame::llm(text);
                text.skip_tts = Some(true);
                text.append_to_context = false;
                ctx.push(
                    Frame::new(FrameData::LlmThoughtText(text)),
                    Direction::Downstream,
                )
                .await?;
            }
            Ok(StreamEvent::ToolCall(call)) => {
                if !ctx.admit_tool_intent().await? {
                    return Ok(());
                }
                calls.push(FunctionCall {
                    function_name: call.name,
                    tool_call_id: call.id,
                    arguments: call.arguments,
                });
            }
            Ok(StreamEvent::Usage(usage)) => {
                if ctx.configuration().enable_usage_metrics {
                    let mut frame = Frame::new(FrameData::Metrics(vec![Metric::LlmUsage {
                        processor: ctx.name().into(),
                        prompt_tokens: usage.prompt_tokens,
                        completion_tokens: usage.completion_tokens,
                        total_tokens: usage.total_tokens,
                    }]));
                    frame.metadata.insert(
                        "llm_usage".into(),
                        serde_json::to_value(usage)
                            .map_err(|e| ProcessorError::new(e.to_string()))?,
                    );
                    ctx.push(frame, Direction::Downstream).await?;
                }
            }
            Err(error) => {
                failed = true;
                ctx.push_error(error_frame(error)).await?;
            }
            Ok(StreamEvent::Finished { reason }) => {
                if thinking && !explicit_thinking {
                    ctx.push(Frame::new(FrameData::LlmThoughtEnd), Direction::Downstream)
                        .await?;
                }
                if let Some(history) = &continuation {
                    let ordered: Vec<_> = history
                        .blocks()
                        .iter()
                        .filter_map(|block| match block {
                            LlmContinuationBlock::ToolCall(call) => Some(call.clone()),
                            _ => None,
                        })
                        .collect();
                    if ordered.len() != calls.len()
                        || ordered.iter().any(|call| !calls.contains(call))
                    {
                        failed = true;
                        ctx.push_error(error_frame(ServiceError::Protocol(
                            "signed tool history does not match completion".into(),
                        )))
                        .await?;
                    } else {
                        calls = ordered;
                    }
                }
                if !failed && !calls.is_empty() {
                    let mut frame =
                        Frame::new(FrameData::FunctionCallsStarted(std::mem::take(&mut calls)));
                    if let Some(history) = continuation.take() {
                        frame
                            .set_native_llm_continuation(history)
                            .map_err(|error| ProcessorError::new(error.to_string()))?;
                    }
                    if let Some(capability) = &ctx.speculation {
                        frame.set_native_speculation(capability.clone())?;
                    }
                    ctx.push(frame, Direction::Downstream).await?;
                }
                if ctx.configuration().enable_metrics {
                    ctx.push(
                        Frame::new(FrameData::Metrics(vec![Metric::Processing {
                            processor: ctx.name().into(),
                            seconds: started.elapsed().as_secs_f64(),
                        }])),
                        Direction::Downstream,
                    )
                    .await?;
                }
                let mut frame = response_boundary(FrameData::LlmResponseEnd, skip_tts);
                if !failed {
                    if let Some(history) = continuation.take() {
                        frame
                            .set_native_llm_continuation(history)
                            .map_err(|error| ProcessorError::new(error.to_string()))?;
                    }
                }
                if let Some(reason) = reason {
                    frame.metadata.insert(
                        "finish_reason".into(),
                        serde_json::to_value(reason)
                            .map_err(|e| ProcessorError::new(e.to_string()))?,
                    );
                }
                frame.metadata.insert("failed".into(), failed.into());
                ctx.push(frame, Direction::Downstream).await?;
                return Ok(());
            }
        }
    }
    Err(ProcessorError::new(
        "LLM stream closed without a terminal event",
    ))
}
fn error_frame(error: ServiceError) -> ErrorFrame {
    let category = match &error {
        ServiceError::Http { status, .. } => ErrorCategory::from_http_status(*status),
        ServiceError::InvalidRequest(_) | ServiceError::Unsupported(_) => {
            ErrorCategory::InvalidRequest
        }
        ServiceError::Transport(_) | ServiceError::Timeout => ErrorCategory::Connectivity,
        _ => ErrorCategory::Application,
    };
    ErrorFrame {
        error: error.to_string(),
        category,
        ..ErrorFrame::new("")
    }
}
fn response_boundary(data: FrameData, skip_tts: Option<bool>) -> Frame {
    let mut frame = Frame::new(data);
    if let Some(skip) = skip_tts {
        frame.metadata.insert("skip_tts".into(), skip.into());
    }
    frame
}

fn native_history(
    response_id: u64,
    messages: Vec<crate::Message>,
    checkpoint: bool,
) -> Result<NativeLlmContinuation, ServiceError> {
    let mut blocks = Vec::with_capacity(messages.len());
    for message in messages {
        if message.role != crate::Role::Assistant {
            return Err(ServiceError::Protocol(
                "non-assistant signed continuation".into(),
            ));
        }
        match message.content {
            Some(crate::Content::Text(text)) if message.tool_calls.is_empty() => {
                blocks.push(LlmContinuationBlock::Text(text))
            }
            Some(crate::Content::Parts(parts))
                if parts.len() == 1 && message.tool_calls.is_empty() =>
            {
                match parts.into_iter().next().expect("one thinking block") {
                    crate::ContentPart::RedactedThinking { data } => {
                        blocks.push(LlmContinuationBlock::RedactedThinking(data))
                    }
                    crate::ContentPart::Thinking {
                        text,
                        signature: Some(signature),
                    } => blocks.push(LlmContinuationBlock::Thinking { text, signature }),
                    _ => {
                        return Err(ServiceError::Protocol(
                            "invalid signed continuation part".into(),
                        ))
                    }
                }
            }
            None if message.tool_calls.len() == 1 => {
                let call = message
                    .tool_calls
                    .into_iter()
                    .next()
                    .expect("one tool call");
                blocks.push(LlmContinuationBlock::ToolCall(FunctionCall {
                    function_name: call.name,
                    tool_call_id: call.id,
                    arguments: call.arguments,
                }));
            }
            _ => {
                return Err(ServiceError::Protocol(
                    "ambiguous signed continuation block".into(),
                ))
            }
        }
    }
    let make = if checkpoint {
        NativeLlmContinuation::checkpoint
    } else {
        NativeLlmContinuation::new
    };
    make(response_id, LlmContinuationProvider::Anthropic, blocks)
        .map_err(|error| ServiceError::Protocol(error.to_string()))
}

fn native_gemini_history(
    response_id: u64,
    messages: Vec<crate::Message>,
    checkpoint: bool,
) -> Result<NativeLlmContinuation, ServiceError> {
    let mut blocks = vec![];
    for message in messages {
        if message.role != crate::Role::Assistant {
            return Err(ServiceError::Protocol(
                "non-assistant Gemini history".into(),
            ));
        }
        let Some(crate::Content::Parts(mut parts)) = message.content else {
            return Err(ServiceError::Protocol(
                "invalid Gemini history content".into(),
            ));
        };
        if parts.len() != 1 {
            return Err(ServiceError::Protocol(
                "ambiguous Gemini history Part".into(),
            ));
        }
        match parts.remove(0) {
            crate::ContentPart::GeminiText {
                text,
                thought,
                signature,
                incomplete: false,
            } if message.tool_calls.is_empty() => blocks.push(LlmContinuationBlock::GeminiText {
                text,
                thought,
                signature,
            }),
            crate::ContentPart::GeminiToolMetadata {
                tool_call_id,
                signature,
            } if message.tool_calls.len() == 1 && message.tool_calls[0].id == tool_call_id => {
                let call = message.tool_calls.into_iter().next().unwrap();
                blocks.push(LlmContinuationBlock::ToolCall(FunctionCall {
                    function_name: call.name,
                    tool_call_id: call.id,
                    arguments: call.arguments,
                }));
                blocks.push(LlmContinuationBlock::GeminiToolMetadata {
                    tool_call_id,
                    signature,
                });
            }
            _ => {
                return Err(ServiceError::Protocol(
                    "invalid Gemini history bookmark".into(),
                ))
            }
        }
    }
    let make = if checkpoint {
        NativeLlmContinuation::checkpoint
    } else {
        NativeLlmContinuation::new
    };
    make(response_id, LlmContinuationProvider::Gemini, blocks)
        .map_err(|error| ServiceError::Protocol(error.to_string()))
}

#[cfg(test)]
mod source_error_tests {
    use super::*;
    use pipecat_core::{EagerSourceSessionOwner, NativeEagerConsumer, Pipeline, SpeculationOwner};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct ErrorAfterCancel {
        source: Option<EagerSourceSessionOwner>,
        owner: SpeculationOwner,
        retire_source: bool,
    }

    #[async_trait]
    impl FrameProcessor for ErrorAfterCancel {
        fn name(&self) -> &str {
            "SelfCancelledProviderError"
        }

        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            context: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::LlmRun) {
                let capability = self.owner.capability();
                assert!(capability.claim_inference());
                assert!(capability.bind_admission(context.clone(), context.health().generation()));
                let output = CompletionOutput {
                    context: context.clone(),
                    gate: crate::speculation::OutputGate::new(Default::default()),
                    speculation: Some(capability.clone()),
                    health_generation: context.health().generation(),
                };
                capability.cancel();
                if self.retire_source {
                    self.source.as_ref().unwrap().revoke();
                }
                assert!(!capability.admits_completion());
                return output
                    .push_error(ErrorFrame::new("provider failed after cancellation"))
                    .await;
            }
            context.push(frame, direction).await
        }
    }

    #[tokio::test]
    async fn self_cancel_preserves_current_and_legacy_errors_but_not_retired_source_errors() {
        for mode in ["legacy", "current source", "retired source"] {
            let source = (mode != "legacy").then(|| EagerSourceSessionOwner::new(4096).unwrap());
            let owner = match &source {
                Some(source) => {
                    let turn = source.begin_turn().unwrap();
                    let consumer = NativeEagerConsumer::new();
                    assert!(turn.claim_start(&consumer));
                    let prediction = source
                        .predict(
                            &turn,
                            FrameData::EagerTranscription(pipecat_core::TranscriptionFrame {
                                text: "pending".into(),
                                ..Default::default()
                            }),
                        )
                        .unwrap();
                    SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap()
                }
                None => SpeculationOwner::new(),
            };
            let mut worker = Pipeline::new(vec![Arc::new(ErrorAfterCancel {
                source,
                owner,
                retire_source: mode == "retired source",
            })])
            .start(Default::default())
            .await
            .unwrap();
            worker
                .sender()
                .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
                .await
                .unwrap();
            let barrier = Frame::new(FrameData::Flush(pipecat_core::FlushFrame {
                returning: false,
                origin: Some("error-admission".into()),
            }));
            let id = barrier.id;
            worker
                .sender()
                .send(barrier, Direction::Downstream)
                .await
                .unwrap();
            let errors = tokio::time::timeout(Duration::from_secs(5), async {
                let mut errors = 0;
                loop {
                    let frame = worker.recv().await.unwrap();
                    errors += usize::from(matches!(frame.frame.data, FrameData::Error(_)));
                    if frame.frame.id == id {
                        return errors;
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(errors, usize::from(mode != "retired source"), "{mode}");
            worker.end().await.unwrap();
            worker.collect().await.unwrap();
        }
    }

    #[derive(Default)]
    struct PollSource {
        health: ProcessorHealth,
        owners: Mutex<Option<(EagerSourceSessionOwner, SpeculationOwner)>>,
    }

    impl PollSource {
        fn capability(&self) -> pipecat_core::NativeSpeculation {
            self.owners.lock().unwrap().as_ref().unwrap().1.capability()
        }
    }

    #[async_trait]
    impl FrameProcessor for PollSource {
        fn name(&self) -> &str {
            "IndependentCompletionSource"
        }

        fn health(&self) -> Option<ProcessorHealth> {
            Some(self.health.clone())
        }

        async fn setup(
            &self,
            _: &pipecat_core::StartFrame,
            context: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            let source = EagerSourceSessionOwner::from_context(4096, context)?;
            let turn = source.begin_turn()?;
            let consumer = NativeEagerConsumer::new();
            assert!(turn.claim_start(&consumer));
            let prediction = source.predict(
                &turn,
                FrameData::EagerTranscription(pipecat_core::TranscriptionFrame {
                    text: "pending".into(),
                    ..Default::default()
                }),
            )?;
            let owner =
                SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
            *self.owners.lock().unwrap() = Some((source, owner));
            Ok(())
        }

        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            context: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            context.push(frame, direction).await
        }
    }

    struct QuietSourceChange {
        source_health: ProcessorHealth,
        polls: Arc<AtomicUsize>,
    }

    impl Future for QuietSourceChange {
        type Output = Result<(), ProcessorError>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
        ) -> Poll<Self::Output> {
            if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
                // Same-value source restoration changes authority but sends no
                // watch value. The LLM's independent health also stays current.
                assert!(!self.source_health.set_usable(true));
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    struct GuardedPollProbe {
        source: Arc<PollSource>,
        polls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl FrameProcessor for GuardedPollProbe {
        fn name(&self) -> &str {
            "ProductionCompletionPollGuard"
        }

        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            context: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::LlmRun) {
                let capability = self.source.capability();
                assert!(capability.claim_inference());
                assert!(capability.bind_admission(context.clone(), context.health().generation()));
                let output = CompletionOutput {
                    context: context.clone(),
                    gate: crate::speculation::OutputGate::new(Default::default()),
                    speculation: Some(capability),
                    health_generation: context.health().generation(),
                };
                return output
                    .guard_completion(QuietSourceChange {
                        source_health: self.source.health.clone(),
                        polls: self.polls.clone(),
                    })
                    .await;
            }
            context.push(frame, direction).await
        }
    }

    #[tokio::test]
    async fn self_waking_completion_rechecks_source_inside_suspended_select() {
        let source = Arc::new(PollSource::default());
        let polls = Arc::new(AtomicUsize::new(0));
        let mut worker = Pipeline::new(vec![
            source.clone(),
            Arc::new(GuardedPollProbe {
                source: source.clone(),
                polls: polls.clone(),
            }),
        ])
        .start(Default::default())
        .await
        .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
        let barrier = Frame::new(FrameData::Flush(pipecat_core::FlushFrame {
            returning: false,
            origin: Some("inner-poll-guard".into()),
        }));
        let id = barrier.id;
        worker
            .sender()
            .send(barrier, Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while worker.recv().await.unwrap().frame.id != id {}
        })
        .await
        .unwrap();
        assert_eq!(
            polls.load(Ordering::SeqCst),
            1,
            "completion was polled after silent source retirement inside the suspended select"
        );
        assert!(!source.capability().source_is_current());
        worker.end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }
}

#[cfg(test)]
mod initial_metric_tests {
    use super::*;
    use crate::{ClientConfig, Context, Message, Provider, Role};
    use pipecat_core::{FlushFrame, Pipeline, PipelineConfig, StartFrame};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn first_queued_request_cancelled_before_fifo_does_not_consume_initial_timing() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut config = ClientConfig::new(Provider::OpenAi, "synthetic-key");
            config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
            config.retry = crate::RetryPolicy::Never;
            let processor = Arc::new(LlmProcessor::new(
                LlmClient::new(config).unwrap(),
                CompletionRequest::new(
                    "fixture",
                    Context::new(vec![Message::text(Role::User, "hello")]),
                ),
            ));
            // A private deterministic gate proves the *first* request is queued.
            // No public test hook, timeout-based scheduling guess or earlier
            // consumed report can hide an admission-time claim regression.
            let guard = processor.completion_lock.lock().await;
            let mut worker = Pipeline::new(vec![processor.clone()])
                .start(PipelineConfig {
                    start: StartFrame {
                        enable_metrics: true,
                        report_only_initial_ttfb: true,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
                .unwrap();
            worker
                .sender()
                .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
                .await
                .unwrap();
            let barrier = Frame::new(FrameData::Flush(FlushFrame {
                returning: false,
                origin: Some("initial-metric-fifo-test".into()),
            }));
            let barrier_id = barrier.id;
            worker.sender().send(barrier, Direction::Downstream).await.unwrap();
            loop {
                let output = worker.recv().await.unwrap();
                assert!(!matches!(output.frame.data, FrameData::LlmResponseStart));
                if output.frame.id == barrier_id {
                    break;
                }
            }
            assert!(processor.should_report_ttfb.load(Ordering::Relaxed));
            let interruption = Frame::new(FrameData::Interruption);
            let interruption_id = interruption.id;
            worker.sender().send(interruption, Direction::Downstream).await.unwrap();
            while worker.recv().await.unwrap().frame.id != interruption_id {}
            assert!(processor.should_report_ttfb.load(Ordering::Relaxed));
            drop(guard);

            let peer = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 4096];
                let (header_end, length) = loop {
                    let n = socket.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&buffer[..n]);
                    assert!(request.len() <= 65536);
                    if let Some(position) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        let end = position + 4;
                        let header = std::str::from_utf8(&request[..end]).unwrap();
                        assert!(header.starts_with("POST /stream HTTP/1.1\r\n"));
                        let length = header.lines().find_map(|line| {
                            line.to_ascii_lowercase().strip_prefix("content-length:")
                                .map(|value| value.trim().parse::<usize>().unwrap())
                        }).unwrap();
                        assert!(length <= 65536 - end);
                        break (end, length);
                    }
                };
                while request.len() < header_end + length {
                    let n = socket.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&buffer[..n]);
                    assert!(request.len() <= 65536);
                }
                let body: serde_json::Value = serde_json::from_slice(&request[header_end..header_end + length]).unwrap();
                assert_eq!(body["messages"][0]["content"], "hello");
                let response = b"data: {\"choices\":[{\"delta\":{\"content\":\"answer\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";
                let headers = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", response.len());
                socket.write_all(headers.as_bytes()).await.unwrap();
                socket.write_all(response).await.unwrap();
                socket.shutdown().await.unwrap();
            });
            worker.sender().send(Frame::new(FrameData::LlmRun), Direction::Downstream).await.unwrap();
            let (mut ttfb, mut ttfat, mut processing) = (0, 0, 0);
            let mut answer = String::new();
            loop {
                match worker.recv().await.unwrap().frame.data {
                    FrameData::Metrics(metrics) => {
                        for metric in metrics {
                            match metric {
                                Metric::Ttfb { .. } => ttfb += 1,
                                Metric::Ttfat { .. } => ttfat += 1,
                                Metric::Processing { .. } => processing += 1,
                                _ => {}
                            }
                        }
                    }
                    FrameData::LlmText(text) => answer.push_str(&text.text),
                    FrameData::LlmResponseEnd => break,
                    FrameData::Error(error) => panic!("unexpected error: {}", error.error),
                    _ => {}
                }
            }
            assert_eq!((ttfb, ttfat, processing), (1, 1, 1));
            assert_eq!(answer, "answer");
            assert!(!processor.should_report_ttfb.load(Ordering::Relaxed));
            peer.await.unwrap();
            worker.end().await.unwrap();
            let (_, report) = worker.collect().await.unwrap();
            assert!(report.errors.is_empty(), "{:?}", report.errors);
        })
        .await
        .expect("causal FIFO cancellation test exceeded its bound");
    }
}
