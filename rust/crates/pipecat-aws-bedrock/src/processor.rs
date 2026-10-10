//! Interruption-aware Bedrock processor, explicit-context inference and native history.
use crate::{BedrockClient, CompletionRequest, ServiceError};
use pipecat_core::{
    async_trait, Direction, ErrorCategory, ErrorFrame, Frame, FrameData, FrameProcessor,
    FunctionCall, LlmContextData, LlmServiceMetadataFrame, LlmSettings, Metric, ProcessorContext,
    ProcessorError, ProcessorHealth, ProcessorReference, ServiceMetadataFrame, TextFrame,
};
use pipecat_services::{frame_context, settings, InferenceOptions, LlmInference};
use std::{
    future::{poll_fn, Future},
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::time::Instant;
/// A text/context LLM processor. Slow network work is owned by the pipeline.
///
/// Context frames and message updates replace/extend the request context. An
/// external context aggregator owns conversation history; this processor does
/// not implicitly append assistant text or execute tool calls. Completions are
/// serialized, so overlapping context frames cannot interleave their output.
pub struct BedrockProcessor {
    health: ProcessorHealth,
    name: String,
    client: BedrockClient,
    state: Mutex<State>,
    completion_lock: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Clone)]
struct State {
    request: CompletionRequest,
    skip_tts: Option<bool>,
}
enum Consumption {
    Forward,
    Consumed,
    Run(Box<State>),
}
impl BedrockProcessor {
    pub fn new(client: BedrockClient, request: CompletionRequest) -> Self {
        Self {
            health: ProcessorHealth::default(),
            name: "AWSBedrockLLMService".into(),
            client,
            state: Mutex::new(State {
                request,
                skip_tts: None,
            }),
            completion_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
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
        if let Some(capability) = frame.native_speculation() {
            capability.cancel();
            return Err(ServiceError::Unsupported(
                "Bedrock native speculation requires a provider output gate".into(),
            ));
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
                Some(delta) => LlmSettings {
                    base: delta.clone(),
                    ..Default::default()
                },
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
                next.request.context = frame_context::legacy_context(context);
                true
            }
            FrameData::LlmContextDetailed(context) => {
                if context.speculation {
                    return Err(ServiceError::Unsupported(
                        "speculative context requires an output gate".into(),
                    ));
                }
                next.request.context = frame_context::context(&context.context)?;
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
            FrameData::LlmEnablePromptCaching { enable } => {
                next.request
                    .settings
                    .base
                    .extra
                    .insert("enable_prompt_caching".into(), (*enable).into());
                false
            }
            _ => return Ok(Consumption::Forward),
        };
        self.client.request_body(&next.request)?;
        *state = next.clone();
        Ok(if run {
            Consumption::Run(Box::new(next))
        } else {
            Consumption::Consumed
        })
    }
    async fn launch(&self, state: State, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let client = self.client.clone();
        let output = ctx.clone();
        let mut guard = Box::pin(self.completion_lock.clone().lock_owned());
        // Reserve the FIFO position before background task scheduling can reorder it.
        let acquired = poll_fn(|cx| Poll::Ready(guard.as_mut().poll(cx))).await;
        ctx.spawn("llm-completion", true, async move {
            let _guard = match acquired {
                Poll::Ready(guard) => guard,
                Poll::Pending => guard.await,
            };
            process_completion(client, state.request, state.skip_tts, output).await
        })?;
        Ok(())
    }
}

/// Equal core projections can conceal distinct native forms, such as inline
/// images and files. Keep every retained occurrence, and reject partial
/// selection when the projection cannot identify which original survived.
fn restore_native_messages(
    original: &[pipecat_services::Message],
    input: &[pipecat_core::ContextMessage],
    transformed: &[pipecat_core::ContextMessage],
) -> Result<Vec<pipecat_services::Message>, ServiceError> {
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
impl FrameProcessor for BedrockProcessor {
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
            let mut guard = Box::pin(self.completion_lock.clone().lock_owned());
            // Join the same FIFO as completions at admission, before spawning.
            // Waiting inside process() would prevent interruption/terminal work.
            let acquired = poll_fn(|cx| Poll::Ready(guard.as_mut().poll(cx))).await;
            ctx.spawn("llm-flow-fence", false, async move {
                let _guard = match acquired {
                    Poll::Ready(guard) => guard,
                    Poll::Pending => guard.await,
                };
                output.push(frame, direction).await
            })?;
            return Ok(());
        }
        match self.consume(&frame) {
            Ok(Consumption::Run(state)) => self.launch(*state, ctx).await?,
            Ok(Consumption::Forward) => ctx.push(frame, direction).await?,
            Ok(Consumption::Consumed) => {}
            Err(error) => {
                let proposal = matches!(
                    frame.data,
                    FrameData::LlmUpdateSettings(_) | FrameData::ServiceUpdateSettings(_)
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
}

#[async_trait]
impl LlmInference for BedrockProcessor {
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
        self.client
            .converse(request, options.max_output_bytes)
            .await
    }
}

async fn process_completion(
    client: BedrockClient,
    request: CompletionRequest,
    skip_tts: Option<bool>,
    ctx: ProcessorContext,
) -> Result<(), ProcessorError> {
    use crate::protocol::{BedrockBlock, BedrockEvent};
    let noop = client
        .prepare(&request, true)
        .map_err(|e| ProcessorError::new(e.to_string()))?
        .noop_tool;
    let admitted_tools: std::collections::BTreeSet<_> = request
        .context
        .tools
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    let forced_tool = match &request.context.tool_choice {
        Some(pipecat_services::ToolChoice::Function(name)) => Some(name.clone()),
        _ => None,
    };
    let started = Instant::now();
    let mut ttfb = false;
    let mut thinking = false;
    let response_id =
        pipecat_core::NativeLlmContinuation::next_response_id().map_err(native_error)?;
    let mut start = boundary(FrameData::LlmResponseStart, skip_tts);
    start
        .set_native_llm_continuation(Arc::new(
            pipecat_core::NativeLlmContinuation::start(
                response_id,
                pipecat_core::LlmContinuationProvider::AwsBedrock,
            )
            .map_err(native_error)?,
        ))
        .map_err(native_error)?;
    ctx.push(start, Direction::Downstream).await?;
    let mut stream = match client.stream(request) {
        Ok(v) => v,
        Err(e) => {
            ctx.push_error(error_frame(e)).await?;
            return end_failed(&ctx, skip_tts).await;
        }
    };
    while let Some(event) = stream.next().await {
        if !ttfb && matches!(&event, Ok(BedrockEvent::ContentStarted)) {
            ttfb = true;
            if ctx.configuration().enable_metrics {
                ctx.push(
                    Frame::new(FrameData::Metrics(vec![Metric::Ttfb {
                        processor: ctx.name().into(),
                        seconds: started.elapsed().as_secs_f64(),
                    }])),
                    Direction::Downstream,
                )
                .await?;
            }
        }
        match event {
            Ok(BedrockEvent::Started | BedrockEvent::ContentStarted) => {}
            Ok(BedrockEvent::TextDelta(text)) => {
                if thinking {
                    ctx.push(Frame::new(FrameData::LlmThoughtEnd), Direction::Downstream)
                        .await?;
                    thinking = false;
                }
                let mut text = TextFrame::llm(text);
                text.skip_tts = skip_tts;
                ctx.push(Frame::new(FrameData::LlmText(text)), Direction::Downstream)
                    .await?;
            }
            Ok(BedrockEvent::ThoughtDelta(text)) => {
                if !thinking {
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
            Ok(BedrockEvent::Checkpoint(blocks)) => {
                let history = match native_history(response_id, blocks, true, noop) {
                    Ok(history) => history,
                    Err(error) => {
                        ctx.push_error(ErrorFrame::new(error.to_string())).await?;
                        if thinking {
                            ctx.push(Frame::new(FrameData::LlmThoughtEnd), Direction::Downstream)
                                .await?;
                        }
                        return end_failed(&ctx, skip_tts).await;
                    }
                };
                let mut frame = Frame::new(FrameData::LlmContinuationCheckpoint);
                frame
                    .set_native_llm_continuation(Arc::new(history))
                    .map_err(native_error)?;
                ctx.push(frame, Direction::Downstream).await?;
            }
            Ok(BedrockEvent::Usage(usage)) => {
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
                            .map_err(|_| ProcessorError::new("invalid Bedrock usage"))?,
                    );
                    ctx.push(frame, Direction::Downstream).await?;
                }
            }
            Ok(BedrockEvent::Completed { blocks, reason }) => {
                if thinking {
                    ctx.push(Frame::new(FrameData::LlmThoughtEnd), Direction::Downstream)
                        .await?;
                }
                if !noop
                    && blocks.iter().any(|block| {
                        matches!(block, BedrockBlock::Tool(call)
                    if !admitted_tools.contains(&call.name)
                        || forced_tool.as_ref().is_some_and(|name| name != &call.name))
                    })
                {
                    ctx.push_error(error_frame(ServiceError::Protocol(
                        "Bedrock returned a tool outside the admitted request declarations".into(),
                    )))
                    .await?;
                    return end_failed(&ctx, skip_tts).await;
                }
                let mut history = Some(Arc::new(
                    match native_history(response_id, blocks.clone(), false, noop) {
                        Ok(history) => history,
                        Err(error) => {
                            ctx.push_error(ErrorFrame::new(error.to_string())).await?;
                            return end_failed(&ctx, skip_tts).await;
                        }
                    },
                ));
                let calls: Vec<_> = blocks
                    .into_iter()
                    .filter_map(|b| match b {
                        BedrockBlock::Tool(c) if !noop => Some(FunctionCall {
                            tool_call_id: c.id,
                            function_name: c.name,
                            arguments: c.arguments,
                        }),
                        _ => None,
                    })
                    .collect();
                if !calls.is_empty() {
                    let mut frame = Frame::new(FrameData::FunctionCallsStarted(calls));
                    frame
                        .set_native_llm_continuation(history.take().expect("history"))
                        .map_err(native_error)?;
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
                let mut end = boundary(FrameData::LlmResponseEnd, skip_tts);
                if let Some(history) = history {
                    end.set_native_llm_continuation(history)
                        .map_err(native_error)?;
                }
                end.metadata.insert(
                    "finish_reason".into(),
                    serde_json::to_value(reason)
                        .map_err(|_| ProcessorError::new("invalid finish reason"))?,
                );
                end.metadata.insert("failed".into(), false.into());
                ctx.push(end, Direction::Downstream).await?;
                return Ok(());
            }
            Err(error) => {
                if thinking {
                    ctx.push(Frame::new(FrameData::LlmThoughtEnd), Direction::Downstream)
                        .await?;
                }
                ctx.push_error(error_frame(error)).await?;
                return end_failed(&ctx, skip_tts).await;
            }
        }
    }
    ctx.push_error(error_frame(ServiceError::Protocol(
        "Bedrock stream closed without a terminal event".into(),
    )))
    .await?;
    end_failed(&ctx, skip_tts).await
}
fn boundary(data: FrameData, skip_tts: Option<bool>) -> Frame {
    let mut frame = Frame::new(data);
    if let Some(skip) = skip_tts {
        frame.metadata.insert("skip_tts".into(), skip.into());
    }
    frame
}
async fn end_failed(ctx: &ProcessorContext, skip_tts: Option<bool>) -> Result<(), ProcessorError> {
    let mut end = boundary(FrameData::LlmResponseEnd, skip_tts);
    end.metadata.insert("failed".into(), true.into());
    ctx.push(end, Direction::Downstream).await
}
fn error_frame(error: ServiceError) -> ErrorFrame {
    let category = match &error {
        ServiceError::Http {
            status: 401 | 403, ..
        } => ErrorCategory::Connectivity,
        ServiceError::Http { status, .. } => ErrorCategory::from_http_status(*status),
        ServiceError::InvalidRequest(_) | ServiceError::Unsupported(_) => {
            ErrorCategory::InvalidRequest
        }
        ServiceError::Transport(_) | ServiceError::Timeout => ErrorCategory::Connectivity,
        ServiceError::Provider { kind, .. } if kind == "throttlingException" => {
            ErrorCategory::from_http_status(429)
        }
        ServiceError::Provider { kind, .. } if kind == "validationException" => {
            ErrorCategory::InvalidRequest
        }
        _ => ErrorCategory::Application,
    };
    ErrorFrame {
        error: error.to_string(),
        category,
        ..ErrorFrame::new("")
    }
}

fn native_error(error: pipecat_core::NativeLlmContinuationError) -> ProcessorError {
    ProcessorError::new(error.to_string())
}
fn native_history(
    id: u64,
    blocks: Vec<crate::protocol::BedrockBlock>,
    checkpoint: bool,
    noop: bool,
) -> Result<pipecat_core::NativeLlmContinuation, ProcessorError> {
    use crate::protocol::BedrockBlock;
    use pipecat_core::{LlmContinuationBlock, LlmContinuationProvider, NativeLlmContinuation};
    let blocks = blocks
        .into_iter()
        .filter_map(|b| {
            Some(match b {
                BedrockBlock::Text(t) => LlmContinuationBlock::Text(t),
                BedrockBlock::Reasoning { text, signature } => {
                    LlmContinuationBlock::BedrockReasoning { text, signature }
                }
                BedrockBlock::Redacted(data) => LlmContinuationBlock::BedrockRedacted(data),
                BedrockBlock::Tool(c) => {
                    if noop {
                        return None;
                    }
                    LlmContinuationBlock::ToolCall(FunctionCall {
                        tool_call_id: c.id,
                        function_name: c.name,
                        arguments: c.arguments,
                    })
                }
            })
        })
        .collect();
    if checkpoint {
        NativeLlmContinuation::checkpoint(id, LlmContinuationProvider::AwsBedrock, blocks)
    } else {
        NativeLlmContinuation::new(id, LlmContinuationProvider::AwsBedrock, blocks)
    }
    .map_err(native_error)
}
