//! Stateless, bounded transformation of the pinned Python TextFrame family.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
};
use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc, time::Duration};

/// An owned, cooperatively cancellable transform. Hooks must not block the
/// executor or detach work: cancellation drops this future, not external tasks.
pub type AsyncTextTransform = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<String, ProcessorError>> + Send>>
        + Send
        + Sync,
>;

/// Resource limits measured in UTF-8 bytes, without truncation.
#[derive(Clone, Debug)]
pub struct StatelessTextTransformerConfig {
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
    /// Cooperative deadline including callback construction and polling.
    pub callback_timeout: Duration,
}
impl Default for StatelessTextTransformerConfig {
    fn default() -> Self {
        Self {
            max_input_bytes: 1024 * 1024,
            max_output_bytes: 1024 * 1024,
            callback_timeout: Duration::from_secs(10),
        }
    }
}
impl StatelessTextTransformerConfig {
    fn validate(&self) -> Result<(), ProcessorError> {
        if self.max_input_bytes == 0
            || self.max_input_bytes > 16 * 1024 * 1024
            || self.max_output_bytes == 0
            || self.max_output_bytes > 16 * 1024 * 1024
            || self.callback_timeout.is_zero()
            || self.callback_timeout > Duration::from_secs(60)
        {
            return Err(ProcessorError::new(
                "invalid stateless text transformer configuration",
            ));
        }
        Ok(())
    }
}

/// Emits a fresh ordinary text frame downstream, even for empty results or
/// upstream input. Original identity, metadata and text subclass fields are not
/// retained. Other frame kinds pass through unchanged. No background task is
/// created; the original invocation's cancellation and causal fence apply.
/// Native continuation/eager/tool carriers cannot be converted to plain text.
pub struct StatelessTextTransformer {
    config: StatelessTextTransformerConfig,
    transform: AsyncTextTransform,
}
impl StatelessTextTransformer {
    pub fn new_sync<F>(
        config: StatelessTextTransformerConfig,
        transform: F,
    ) -> Result<Self, ProcessorError>
    where
        F: Fn(String) -> Result<String, ProcessorError> + Send + Sync + 'static,
    {
        let transform = Arc::new(transform);
        Self::new_async(
            config,
            Arc::new(move |text| {
                let transform = transform.clone();
                Box::pin(async move { transform(text) })
            }),
        )
    }
    pub fn new_async(
        config: StatelessTextTransformerConfig,
        transform: AsyncTextTransform,
    ) -> Result<Self, ProcessorError> {
        config.validate()?;
        Ok(Self { config, transform })
    }
}
fn text(data: &FrameData) -> Option<&str> {
    match data {
        FrameData::Text(f)
        | FrameData::LlmText(f)
        | FrameData::InputText(f)
        | FrameData::VisionText(f) => Some(&f.text),
        FrameData::AggregatedText(f) | FrameData::TtsText(f) => Some(&f.text.text),
        FrameData::Transcription(f)
        | FrameData::InterimTranscription(f)
        | FrameData::EagerTranscription(f)
        | FrameData::Translation(f) => Some(&f.text),
        FrameData::TranscriptionDetailed(f)
        | FrameData::InterimTranscriptionDetailed(f)
        | FrameData::EagerTranscriptionDetailed(f)
        | FrameData::TranslationDetailed(f) => Some(&f.text.text),
        _ => None,
    }
}
#[async_trait]
impl FrameProcessor for StatelessTextTransformer {
    fn name(&self) -> &str {
        "StatelessTextTransformer"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let Some(input) = text(&frame.data) else {
            return ctx.push(frame, direction).await;
        };
        frame.require_no_native_llm_continuation().map_err(|_| {
            ProcessorError::new("text transform cannot consume native semantic authority")
        })?;
        if input.len() > self.config.max_input_bytes {
            return Err(ProcessorError::new(
                "text transform input byte limit exceeded",
            ));
        }
        let input = input.to_owned();
        let operation = AssertUnwindSafe(async { (self.transform)(input).await }).catch_unwind();
        let output = tokio::time::timeout(self.config.callback_timeout, operation)
            .await
            .map_err(|_| ProcessorError::new("text transform callback timed out"))?
            .map_err(|_| ProcessorError::new("text transform callback panicked"))??;
        if output.len() > self.config.max_output_bytes {
            return Err(ProcessorError::new(
                "text transform output byte limit exceeded",
            ));
        }
        ctx.push(Frame::text(output), Direction::Downstream).await
    }
}
