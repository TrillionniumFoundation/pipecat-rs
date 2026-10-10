// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Pipeline-owned Responses inference with immutable admitted requests.
use crate::*;
use pipecat_core::{
    async_trait, Direction, ErrorCategory, ErrorFrame, Frame, FrameData, FrameProcessor,
    FunctionCall, LlmContextData, LlmServiceMetadataFrame, Metric, ProcessorContext,
    ProcessorError, ProcessorHealth, ProcessorReference, ServiceMetadataFrame, TextFrame,
};
use pipecat_services::{InferenceOptions, LlmInference};
use std::{
    future::{poll_fn, Future},
    sync::{Arc, Mutex},
    task::Poll,
};

#[derive(Clone)]
pub enum ResponsesClient {
    Http(ResponsesHttpClient),
    WebSocket(ResponsesWebSocketClient),
}
impl ResponsesClient {
    fn stream(&self, request: ResponsesRequest) -> Result<ResponsesStream, ServiceError> {
        match self {
            Self::Http(client) => client.stream(request),
            Self::WebSocket(client) => client.stream(request),
        }
    }
    fn validate(&self, request: &ResponsesRequest) -> Result<(), ServiceError> {
        match self {
            Self::Http(client) => client.request_body(request)?,
            Self::WebSocket(client) => client.request_body(request)?,
        };
        Ok(())
    }
}
#[derive(Clone)]
struct State {
    request: ResponsesRequest,
    skip_tts: Option<bool>,
}
pub struct ResponsesProcessor {
    name: String,
    health: ProcessorHealth,
    client: ResponsesClient,
    inference: ResponsesHttpClient,
    state: Mutex<State>,
    gate: Arc<tokio::sync::Mutex<()>>,
}
impl ResponsesProcessor {
    pub fn http(config: ResponsesConfig, request: ResponsesRequest) -> Result<Self, ServiceError> {
        let client = ResponsesHttpClient::new(config)?;
        Self::new(ResponsesClient::Http(client.clone()), client, request)
    }
    pub fn websocket(
        config: ResponsesConfig,
        request: ResponsesRequest,
    ) -> Result<Self, ServiceError> {
        Self::new(
            ResponsesClient::WebSocket(ResponsesWebSocketClient::new(config.clone())?),
            ResponsesHttpClient::new(config)?,
            request,
        )
    }
    fn new(
        client: ResponsesClient,
        inference: ResponsesHttpClient,
        request: ResponsesRequest,
    ) -> Result<Self, ServiceError> {
        client.validate(&request)?;
        Ok(Self {
            name: "OpenAIResponsesLLMService".into(),
            health: ProcessorHealth::default(),
            client,
            inference,
            state: Mutex::new(State {
                request,
                skip_tts: None,
            }),
            gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
    pub fn request_snapshot(&self) -> Result<ResponsesRequest, ServiceError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| state_error())?
            .request
            .clone())
    }
    pub fn update_settings(&self, delta: &ResponsesSettings) -> Result<(), ServiceError> {
        let mut state = self.state.lock().map_err(|_| state_error())?;
        let mut request = state.request.clone();
        request.settings.merge(delta);
        self.client.validate(&request)?;
        state.request = request;
        Ok(())
    }
    fn targets(&self, target: &Option<ProcessorReference>) -> bool {
        match target {
            None => true,
            Some(ProcessorReference::Name(name)) => name == &self.name,
            Some(ProcessorReference::Local(target)) => {
                std::ptr::addr_eq(target.0.as_ref(), self as &dyn FrameProcessor)
            }
        }
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
    fn consume(&self, frame: &Frame) -> Result<Option<Option<State>>, ServiceError> {
        if let FrameData::LlmUpdateSettings(update) = &frame.data {
            if !self.targets(&update.service) {
                return Ok(None);
            }
            let llm = match &update.delta {
                Some(delta) => delta.clone(),
                None => pipecat_services::settings::settings_from_mapping(&update.settings)
                    .map_err(|_| {
                        ServiceError::InvalidRequest(
                            "invalid Responses runtime settings mapping".into(),
                        )
                    })?,
            };
            self.update_settings(&ResponsesSettings::from_llm(&llm)?)?;
            return Ok(Some(None));
        }
        if let FrameData::ServiceUpdateSettings(update) = &frame.data {
            if !self.targets(&update.service) {
                return Ok(None);
            }
            let llm = match &update.delta {
                Some(delta) => pipecat_core::LlmSettings {
                    base: delta.clone(),
                    ..Default::default()
                },
                None => pipecat_services::settings::settings_from_mapping(&update.settings)
                    .map_err(|_| {
                        ServiceError::InvalidRequest(
                            "invalid Responses runtime settings mapping".into(),
                        )
                    })?,
            };
            self.update_settings(&ResponsesSettings::from_llm(&llm)?)?;
            return Ok(Some(None));
        }
        let mut state = self.state.lock().map_err(|_| state_error())?;
        let mut next = state.clone();
        let run = match &frame.data {
            FrameData::LlmRun => true,
            FrameData::LlmContext(context) => {
                next.request.context = ResponsesContext::try_from(
                    &pipecat_services::frame_context::legacy_context(context),
                )?;
                true
            }
            FrameData::LlmContextDetailed(context) => {
                if context.speculation {
                    return Err(ServiceError::Unsupported(
                        "speculative Responses context requires an output gate".into(),
                    ));
                }
                next.request.context = ResponsesContext::from_core(&context.context)?;
                true
            }
            FrameData::LlmMessagesAppend(messages) | FrameData::LlmMessagesUpdate(messages) => {
                let converted = ResponsesContext::try_from(&pipecat_services::Context::new(
                    messages
                        .messages
                        .iter()
                        .map(pipecat_services::frame_context::legacy_message)
                        .collect(),
                ))?;
                if matches!(&frame.data, FrameData::LlmMessagesUpdate(_)) {
                    next.request.context.items.clear();
                }
                next.request.context.items.extend(converted.items);
                messages.run_llm
            }
            FrameData::LlmMessagesAppendDetailed(messages)
            | FrameData::LlmMessagesUpdateDetailed(messages) => {
                let converted = ResponsesContext::from_core(&LlmContextData {
                    messages: messages.messages.clone(),
                    tools: pipecat_core::ToolSet::NotGiven,
                    tool_choice: None,
                })?;
                if matches!(&frame.data, FrameData::LlmMessagesUpdateDetailed(_)) {
                    next.request.context.items.clear();
                }
                next.request.context.items.extend(converted.items);
                messages.run_llm.unwrap_or(false)
            }
            FrameData::LlmSetTools(tools) => {
                next.request.context.tools = pipecat_services::frame_context::tools(tools)?;
                false
            }
            FrameData::LlmSetToolChoice(choice) => {
                next.request.context.tool_choice =
                    Some(pipecat_services::frame_context::tool_choice(choice));
                false
            }
            FrameData::LlmConfigureOutput { skip_tts } => {
                next.skip_tts = Some(*skip_tts);
                false
            }
            FrameData::LlmEnablePromptCaching { .. } | FrameData::LlmMessagesTransform(_) => {
                return Err(ServiceError::Unsupported(
                    "Responses runtime prompt caching or context transform".into(),
                ))
            }
            _ => return Ok(None),
        };
        self.client.validate(&next.request)?;
        *state = next.clone();
        Ok(Some(run.then_some(next)))
    }
    async fn launch(&self, state: State, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let client = self.client.clone();
        let ctx_output = ctx.clone();
        let mut guard = Box::pin(self.gate.clone().lock_owned());
        let acquired = poll_fn(|cx| Poll::Ready(guard.as_mut().poll(cx))).await;
        ctx.spawn("responses-inference", true, async move {
            let _guard = match acquired {
                Poll::Ready(guard) => guard,
                Poll::Pending => guard.await,
            };
            completion(client, state, ctx_output).await
        })?;
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for ResponsesProcessor {
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let ResponsesClient::WebSocket(client) = &self.client {
            client.close().await;
        }
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    fn process_when_unusable(&self, frame: &Frame, _: Direction) -> bool {
        !matches!(
            frame.data,
            FrameData::LlmRun
                | FrameData::LlmContext(_)
                | FrameData::LlmContextDetailed(_)
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
            if self.targets(&Some(target.clone())) {
                self.metadata(ctx).await?;
            }
            return ctx.push(frame, direction).await;
        }
        if direction == Direction::Downstream
            && matches!(&frame.data, FrameData::FlushDetailed(flush) if !flush.returning && flush.origin.as_deref() == Some("pipecat.flows"))
        {
            let output = ctx.clone();
            let mut guard = Box::pin(self.gate.clone().lock_owned());
            let acquired = poll_fn(|cx| Poll::Ready(guard.as_mut().poll(cx))).await;
            ctx.spawn("responses-flow-fence", false, async move {
                let _guard = match acquired {
                    Poll::Ready(guard) => guard,
                    Poll::Pending => guard.await,
                };
                output.push(frame, direction).await
            })?;
            return Ok(());
        }
        match self.consume(&frame) {
            Ok(Some(Some(state))) => self.launch(state, ctx).await?,
            Ok(Some(None)) => {}
            Ok(None) => ctx.push(frame, direction).await?,
            Err(error) => {
                let proposal = matches!(
                    &frame.data,
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
impl LlmInference for ResponsesProcessor {
    async fn run_inference(
        &self,
        context: LlmContextData,
        options: InferenceOptions,
    ) -> Result<Option<String>, ServiceError> {
        if !self.health.is_usable() {
            return Err(ServiceError::InvalidRequest(
                "Responses service is not usable".into(),
            ));
        }
        if options.max_output_bytes == 0 {
            return Err(ServiceError::InvalidRequest(
                "inference output limit must be positive".into(),
            ));
        }
        let mut request = self.request_snapshot()?;
        request.context = ResponsesContext::from_core(&context)?;
        request
            .settings
            .merge(&ResponsesSettings::from_llm(&options.settings)?);
        // Common inference max_tokens is an explicit one-shot override.
        match &options.settings.max_tokens {
            pipecat_core::Setting::Value(tokens) => {
                request.settings.max_completion_tokens = pipecat_core::Setting::Value(*tokens);
                request.settings.llm.max_tokens = pipecat_core::Setting::NotGiven;
            }
            pipecat_core::Setting::Null => {
                request.settings.max_completion_tokens = pipecat_core::Setting::Null;
                request.settings.llm.max_tokens = pipecat_core::Setting::NotGiven;
            }
            pipecat_core::Setting::NotGiven => {}
        }
        let text = self
            .inference
            .run_inference(&request, CancellationToken::new())
            .await?;
        if text
            .as_ref()
            .is_some_and(|text| text.len() > options.max_output_bytes)
        {
            return Err(ServiceError::Protocol(
                "inference text exceeds output limit".into(),
            ));
        }
        Ok(text)
    }
}
async fn completion(
    client: ResponsesClient,
    state: State,
    ctx: ProcessorContext,
) -> Result<(), ProcessorError> {
    let start = std::time::Instant::now();
    let mut ttfb = false;
    let mut thought = false;
    let mut failed = false;
    let mut calls = vec![];
    let mut calls_safe = true;
    ctx.push(
        boundary(FrameData::LlmResponseStart, state.skip_tts),
        Direction::Downstream,
    )
    .await?;
    let mut stream = match client.stream(state.request) {
        Ok(stream) => stream,
        Err(error) => {
            ctx.push_error(error_frame(error)).await?;
            let mut end = boundary(FrameData::LlmResponseEnd, state.skip_tts);
            end.metadata.insert("failed".into(), true.into());
            ctx.push(end, Direction::Downstream).await?;
            return Ok(());
        }
    };
    while let Some(event) = stream.recv().await {
        if !ttfb
            && matches!(
                &event,
                Ok(ResponseEvent::TextDelta(_)
                    | ResponseEvent::ThoughtDelta(_)
                    | ResponseEvent::ToolCallDelta { .. })
            )
        {
            ttfb = true;
            if ctx.configuration().enable_metrics {
                ctx.push(
                    Frame::new(FrameData::Metrics(vec![Metric::Ttfb {
                        processor: ctx.name().into(),
                        seconds: start.elapsed().as_secs_f64(),
                    }])),
                    Direction::Downstream,
                )
                .await?;
            }
        }
        match event {
            Ok(ResponseEvent::TextDelta(text)) => {
                let mut text = TextFrame::llm(text);
                text.skip_tts = state.skip_tts;
                ctx.push(Frame::new(FrameData::LlmText(text)), Direction::Downstream)
                    .await?;
            }
            Ok(ResponseEvent::ThoughtStart) => {
                thought = true;
                ctx.push(
                    Frame::new(FrameData::LlmThoughtStart),
                    Direction::Downstream,
                )
                .await?;
            }
            Ok(ResponseEvent::ThoughtDelta(text)) => {
                let mut text = TextFrame::llm(text);
                text.skip_tts = Some(true);
                text.append_to_context = false;
                ctx.push(
                    Frame::new(FrameData::LlmThoughtText(text)),
                    Direction::Downstream,
                )
                .await?;
            }
            Ok(ResponseEvent::ThoughtEnd) => {
                thought = false;
                ctx.push(Frame::new(FrameData::LlmThoughtEnd), Direction::Downstream)
                    .await?;
            }
            Ok(ResponseEvent::ReasoningItem(item)) => {
                ctx.push(
                    Frame::new(FrameData::LlmMessagesAppendDetailed(
                        pipecat_core::DetailedMessagesFrame {
                            messages: vec![item.to_core_message()],
                            run_llm: Some(false),
                        },
                    )),
                    Direction::Downstream,
                )
                .await?
            }
            Ok(ResponseEvent::ToolCall(call)) => calls.push(FunctionCall {
                function_name: call.name,
                tool_call_id: call.id,
                arguments: call.arguments,
            }),
            Ok(ResponseEvent::Usage(usage)) if ctx.configuration().enable_usage_metrics => {
                let mut frame = Frame::new(FrameData::Metrics(vec![Metric::LlmUsage {
                    processor: ctx.name().into(),
                    prompt_tokens: usage.prompt_tokens,
                    completion_tokens: usage.completion_tokens,
                    total_tokens: usage.total_tokens,
                }]));
                frame.metadata.insert(
                    "llm_usage".into(),
                    serde_json::to_value(usage).expect("usage serializable"),
                );
                ctx.push(frame, Direction::Downstream).await?;
            }
            Err(error) => {
                failed = true;
                calls_safe &= matches!(&error, ServiceError::Provider { kind, .. } if matches!(kind.as_str(), "response_failed" | "response_incomplete"));
                ctx.push_error(error_frame(error)).await?;
            }
            Ok(ResponseEvent::Finished {
                status,
                response_id,
                model,
            }) => {
                if thought {
                    ctx.push(Frame::new(FrameData::LlmThoughtEnd), Direction::Downstream)
                        .await?;
                }
                // Only validated, complete calls were admitted. A later failed
                // parallel item cannot turn partial arguments into an empty call.
                if !calls.is_empty() && calls_safe && status != ResponseStatus::Cancelled {
                    ctx.push(
                        Frame::new(FrameData::FunctionCallsStarted(calls)),
                        Direction::Downstream,
                    )
                    .await?;
                }
                if ctx.configuration().enable_metrics {
                    ctx.push(
                        Frame::new(FrameData::Metrics(vec![Metric::Processing {
                            processor: ctx.name().into(),
                            seconds: start.elapsed().as_secs_f64(),
                        }])),
                        Direction::Downstream,
                    )
                    .await?;
                }
                let mut end = boundary(FrameData::LlmResponseEnd, state.skip_tts);
                end.metadata.insert(
                    "failed".into(),
                    (failed || status != ResponseStatus::Completed).into(),
                );
                if let Some(id) = response_id {
                    end.metadata.insert("response_id".into(), id.into());
                }
                if let Some(model) = model {
                    end.metadata.insert("model".into(), model.into());
                }
                ctx.push(end, Direction::Downstream).await?;
                return Ok(());
            }
            _ => {}
        }
    }
    Err(ProcessorError::new(
        "Responses stream closed without a terminal event",
    ))
}
fn state_error() -> ServiceError {
    ServiceError::InvalidRequest("Responses state lock poisoned".into())
}
fn boundary(data: FrameData, skip: Option<bool>) -> Frame {
    let mut frame = Frame::new(data);
    if let Some(skip) = skip {
        frame.metadata.insert("skip_tts".into(), skip.into());
    }
    frame
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
