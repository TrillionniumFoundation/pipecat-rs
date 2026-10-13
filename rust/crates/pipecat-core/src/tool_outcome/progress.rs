use super::{encoded_bytes, NativeToolCallBatch, ToolOutcomeAdmission, ToolOutcomePublisher};
use crate::{
    Direction, Frame, FrameData, FunctionCall, FunctionCallResultFrame, LocalAsyncCallback,
    LocalToolResult, ProcessorContext, ProcessorError,
};
use std::sync::Arc;

/// Immutable provenance for one nonterminal update from an admitted async call.
/// Public frame fields and wire decoding cannot manufacture this capability.
#[derive(Debug, Clone, PartialEq)]
pub struct NativeToolCallProgress {
    id: u64,
    call: Arc<FunctionCall>,
    batch: Arc<NativeToolCallBatch>,
    sequence: u64,
    result: LocalToolResult,
    bytes: usize,
}
impl NativeToolCallProgress {
    /// Original publication identity, preserved even when a frame is duplicated.
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn call(&self) -> &FunctionCall {
        &self.call
    }
    pub fn batch(&self) -> Option<&Arc<NativeToolCallBatch>> {
        Some(&self.batch)
    }
    pub fn cancel_on_interruption(&self) -> bool {
        false
    }
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    /// Conservative encoded retention, including shared batch and result identity.
    pub fn retained_bytes(&self) -> usize {
        self.batch.retained_bytes().saturating_add(self.bytes)
    }
    pub(crate) fn validate(&self, data: &FrameData) -> Result<(), ProcessorError> {
        let valid = self.sequence > 0
            && self
                .batch
                .members()
                .iter()
                .any(|(call, policy)| call == &self.call && !*policy)
            && matches!(data, FrameData::FunctionCallResult(result)
                if !result.is_final
                    && result.error.is_none()
                    && result.function_name == self.call.function_name
                    && result.tool_call_id == self.call.tool_call_id
                    && result.arguments == self.call.arguments
                    && result.result == self.result.value
                    && result.run_llm == self.result.run_llm);
        if valid {
            Ok(())
        } else {
            Err(ProcessorError::new(
                "native tool progress does not match frame",
            ))
        }
    }
}

/// A synchronous first-hop publisher for nonterminal async tool results.
/// The executor separately owns active-call revocation and sequence ordering.
/// This publisher neither settles a call nor releases its admission reservation.
pub struct ToolProgressPublisher {
    context: ProcessorContext,
    call: Arc<FunctionCall>,
    batch: Arc<NativeToolCallBatch>,
}
impl ToolOutcomePublisher {
    pub fn progress_publisher(&self) -> Result<ToolProgressPublisher, ProcessorError> {
        let batch = self.batch.as_ref().ok_or_else(|| {
            ProcessorError::new("native tool progress requires an admitted batch")
        })?;
        if self.cancel_on_interruption
            || !batch
                .members()
                .iter()
                .any(|(call, policy)| call == &self.call && !*policy)
        {
            return Err(ProcessorError::new(
                "native tool progress requires an async batch member",
            ));
        }
        Ok(ToolProgressPublisher {
            context: self.context.clone(),
            call: self.call.clone(),
            batch: batch.clone(),
        })
    }
}
impl ToolProgressPublisher {
    pub fn publish_progress(
        &self,
        result: LocalToolResult,
        sequence: u64,
    ) -> Result<ToolOutcomeAdmission, ProcessorError> {
        self.publish_progress_with_context_updated(result, sequence, None)
    }
    /// Carry an intermediate context-update hook without running it or settling
    /// the call. Failed or closed first-hop publications discard the hook.
    pub fn publish_progress_with_context_updated(
        &self,
        result: LocalToolResult,
        sequence: u64,
        on_context_updated: Option<LocalAsyncCallback>,
    ) -> Result<ToolOutcomeAdmission, ProcessorError> {
        if sequence == 0 {
            return Err(ProcessorError::new(
                "native tool progress sequence must be positive",
            ));
        }
        // Measure every public result field and receipt sequence before cloning
        // arguments or payload. The batch is already validated and bounded.
        #[derive(serde::Serialize)]
        struct BorrowedResult<'a> {
            function_name: &'a str,
            tool_call_id: &'a str,
            arguments: &'a serde_json::Value,
            result: &'a serde_json::Value,
            run_llm: Option<bool>,
            is_final: bool,
            error: Option<&'a str>,
            sequence: u64,
        }
        let bytes = encoded_bytes(
            &BorrowedResult {
                function_name: &self.call.function_name,
                tool_call_id: &self.call.tool_call_id,
                arguments: &self.call.arguments,
                result: &result.value,
                run_llm: result.run_llm,
                is_final: false,
                error: None,
                sequence,
            },
            NativeToolCallBatch::MAX_RETAINED_BYTES,
        )?;
        let ctx = &self.context;
        ctx.with_admission(&ctx.runtime, || {
            if ctx.runtime.is_finished() {
                return Ok(ToolOutcomeAdmission::Closed);
            }
            let mut frame = Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
                function_name: self.call.function_name.clone(),
                tool_call_id: self.call.tool_call_id.clone(),
                arguments: self.call.arguments.clone(),
                result: result.value.clone(),
                run_llm: result.run_llm,
                is_final: false,
                error: None,
            }))
            .with_interruptible(false);
            let progress = Arc::new(NativeToolCallProgress {
                id: frame.id,
                call: self.call.clone(),
                batch: self.batch.clone(),
                sequence,
                result,
                bytes,
            });
            frame.attach_tool_progress(progress.clone());
            if let Some(callback) = on_context_updated {
                frame.attach_tool_progress_context_updated(progress, callback);
            }
            // Only fixed non-interruptible async policy reaches this path.
            // Runtime control revocation is checked by the owning executor.
            let admission =
                match ctx
                    .runtime
                    .route_unlocked(ctx.index, None, frame, Direction::Downstream)
                {
                    Ok(()) if ctx.runtime.is_finished() => ToolOutcomeAdmission::ShutdownRace,
                    Ok(()) => ToolOutcomeAdmission::Admitted,
                    Err(error) => {
                        if !ctx.runtime.is_finished() {
                            ctx.runtime.fail(crate::PipelineError::Execution(format!(
                                "tool progress was not admitted: {error}"
                            )));
                        }
                        ToolOutcomeAdmission::Undelivered(error.to_string())
                    }
                };
            Ok(admission)
        })
    }
}
