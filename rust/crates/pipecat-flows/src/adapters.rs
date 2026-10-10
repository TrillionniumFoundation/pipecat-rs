use crate::FlowError;
use async_trait::async_trait;
use pipecat_core::{ChatRole, ContextMessage, LlmContextData, Setting};
use pipecat_services::{
    frame_context, CancellationToken, CompletionRequest, Context, LlmClient, Message, Role,
    StreamEvent,
};

#[async_trait]
pub trait SummaryProvider: Send + Sync {
    async fn generate_summary(
        &self,
        prompt: &str,
        context: LlmContextData,
        cancel: CancellationToken,
    ) -> Result<Option<String>, FlowError>;
}
/// Real out-of-band native LLM inference. It does not synthesize summaries
/// locally or reuse a tool-capable conversation request.
pub struct LlmAdapter {
    client: LlmClient,
    request: CompletionRequest,
}
impl LlmAdapter {
    pub fn new(client: LlmClient, request: CompletionRequest) -> Self {
        Self { client, request }
    }
    pub fn format_summary_message(summary: &str) -> ContextMessage {
        crate::FlowMessage::new(
            ChatRole::Developer,
            format!("Here's a summary of the conversation:\n{summary}"),
        )
        .to_context()
    }
}
#[async_trait]
impl SummaryProvider for LlmAdapter {
    async fn generate_summary(
        &self,
        prompt: &str,
        context: LlmContextData,
        cancel: CancellationToken,
    ) -> Result<Option<String>, FlowError> {
        let messages = context
            .messages
            .iter()
            .map(frame_context::message)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| FlowError::Pipeline(e.to_string()))?;
        let history =
            serde_json::to_string(&messages).map_err(|e| FlowError::Pipeline(e.to_string()))?;
        let mut request = self.request.clone();
        request.context = Context::new(vec![Message::text(
            Role::Developer,
            format!("Conversation history: {history}"),
        )]);
        request.system_instruction = Some(prompt.into());
        request.settings.system_instruction = Setting::Value(prompt.into());
        let mut stream = self
            .client
            .stream_with_cancel(request, cancel.clone())
            .map_err(|e| FlowError::Pipeline(e.to_string()))?;
        let mut summary = String::new();
        loop {
            let event = tokio::select! { biased; _ = cancel.cancelled() => return Err(FlowError::Cancelled), event = stream.next() => event };
            match event {
                Some(Ok(StreamEvent::TextDelta(text))) => summary.push_str(&text),
                Some(Ok(StreamEvent::ToolCall(_))) => {
                    return Err(FlowError::Tool(
                        "summary inference returned a tool call".into(),
                    ))
                }
                Some(Err(error)) => return Err(FlowError::Pipeline(error.to_string())),
                None => break,
                _ => {}
            }
        }
        Ok((!summary.trim().is_empty()).then_some(summary))
    }
}
