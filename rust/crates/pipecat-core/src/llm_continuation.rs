//! Validated, process-local provider history awaiting native context consumption.
use crate::{FrameData, FunctionCall};
use serde_json::Value;
use std::collections::BTreeSet;
use std::fmt;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT_RESPONSE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LlmContinuationProvider {
    Anthropic,
    Gemini,
    AwsBedrock,
}

/// Ordered provider blocks. Signatures are never included in debug output.
#[derive(Clone, PartialEq)]
pub enum LlmContinuationBlock {
    BedrockReasoning {
        text: String,
        signature: Option<String>,
    },
    BedrockRedacted(Vec<u8>),
    Text(String),
    Thinking {
        text: String,
        signature: String,
    },
    ToolCall(FunctionCall),
    GeminiText {
        text: String,
        thought: bool,
        signature: Option<String>,
    },
    GeminiToolMetadata {
        tool_call_id: String,
        signature: Option<String>,
    },
}
impl fmt::Debug for LlmContinuationBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BedrockReasoning { text, signature } => f
                .debug_struct("BedrockReasoning")
                .field("text_bytes", &text.len())
                .field("signature_bytes", &signature.as_ref().map(String::len))
                .finish(),
            Self::BedrockRedacted(data) => f
                .debug_struct("BedrockRedacted")
                .field("bytes", &data.len())
                .finish(),
            Self::Text(text) => f.debug_struct("Text").field("bytes", &text.len()).finish(),
            Self::Thinking { text, signature } => f
                .debug_struct("Thinking")
                .field("text_bytes", &text.len())
                .field("signature_bytes", &signature.len())
                .field("signature", &"[redacted]")
                .finish(),
            Self::ToolCall(_) => f.debug_tuple("ToolCall").field(&"[redacted]").finish(),
            Self::GeminiText {
                text,
                thought,
                signature,
            } => f
                .debug_struct("GeminiText")
                .field("text_bytes", &text.len())
                .field("thought", thought)
                .field("signature_bytes", &signature.as_ref().map(String::len))
                .finish(),
            Self::GeminiToolMetadata { signature, .. } => f
                .debug_struct("GeminiToolMetadata")
                .field("signature_bytes", &signature.as_ref().map(String::len))
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeLlmContinuationError {
    #[error("native LLM continuation requires a nonzero response ID")]
    InvalidResponseId,
    #[error("native LLM response identity allocator is exhausted")]
    ResponseIdExhausted,
    #[error("native LLM continuation exceeds its block limit")]
    TooManyBlocks,
    #[error("native LLM continuation exceeds its retained-byte limit")]
    TooManyBytes,
    #[error("native LLM continuation requires a nonempty, bounded thinking signature")]
    InvalidSignature,
    #[error("native LLM continuation contains an invalid provider block or bookmark")]
    InvalidProviderBlock,
    #[error("native LLM checkpoint requires a closed signed block")]
    InvalidCheckpoint,
    #[error("native LLM checkpoint frame requires its matching local carrier")]
    MissingCarrier,
    #[error("native LLM continuation requires valid, unique tool identities and object arguments")]
    InvalidToolCall,
    #[error("native LLM continuation tool arguments exceed the nesting limit")]
    ArgumentsTooDeep,
    #[error("native LLM continuation does not match the frame kind or tool calls")]
    FrameMismatch,
    #[error("frame already carries a native LLM continuation")]
    AlreadyAttached,
    #[error("native LLM continuation must be consumed before wire encoding")]
    UnconsumedOnWire,
    #[error("native speculation capability must be consumed before wire encoding")]
    UnconsumedSpeculation,
    #[error("native tool outcome must be consumed before wire encoding")]
    UnconsumedToolOutcome,
    #[error("native tool batch must be consumed before wire encoding")]
    UnconsumedToolBatch,
}

/// Immutable response identity and ordered provider history, shared by frame
/// clones. A start marker and a completed snapshot are distinct native phases.
/// This type deliberately implements neither serialization nor deserialization.
#[derive(Clone, PartialEq)]
pub struct NativeLlmContinuation {
    response_id: u64,
    provider: LlmContinuationProvider,
    phase: ContinuationPhase,
    blocks: Vec<LlmContinuationBlock>,
    retained_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContinuationPhase {
    Start,
    Checkpoint,
    Completed,
}
impl fmt::Debug for NativeLlmContinuation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeLlmContinuation")
            .field("response_id", &self.response_id)
            .field("provider", &self.provider)
            .field("phase", &self.phase)
            .field("block_count", &self.blocks.len())
            .field("retained_bytes", &self.retained_bytes)
            .finish_non_exhaustive()
    }
}
impl NativeLlmContinuation {
    /// Allocates a process-local response identity shared by all native providers.
    /// IDs distinguish responses; they are not cryptographic authentication.
    pub fn next_response_id() -> Result<u64, NativeLlmContinuationError> {
        allocate_response_id(&NEXT_RESPONSE_ID)
    }

    pub const MAX_BLOCKS: usize = 1024;
    pub const MAX_RETAINED_BYTES: usize = 8 * 1024 * 1024;
    pub const MAX_SIGNATURE_BYTES: usize = 1024 * 1024;
    pub const MAX_ARGUMENT_DEPTH: usize = 128;

    pub fn start(
        response_id: u64,
        provider: LlmContinuationProvider,
    ) -> Result<Self, NativeLlmContinuationError> {
        let mut value = Self::new(response_id, provider, vec![])?;
        value.phase = ContinuationPhase::Start;
        Ok(value)
    }

    pub fn new(
        response_id: u64,
        provider: LlmContinuationProvider,
        blocks: Vec<LlmContinuationBlock>,
    ) -> Result<Self, NativeLlmContinuationError> {
        if response_id == 0 {
            return Err(NativeLlmContinuationError::InvalidResponseId);
        }
        if blocks.len() > Self::MAX_BLOCKS {
            return Err(NativeLlmContinuationError::TooManyBlocks);
        }
        let mut bytes = size_of::<Self>() + 2 * size_of::<usize>();
        charge_product(
            &mut bytes,
            blocks.capacity(),
            size_of::<LlmContinuationBlock>(),
        )?;
        let mut ids = BTreeSet::new();
        for (index, block) in blocks.iter().enumerate() {
            match block {
                LlmContinuationBlock::BedrockReasoning { text, signature } => {
                    if provider != LlmContinuationProvider::AwsBedrock {
                        return Err(NativeLlmContinuationError::InvalidProviderBlock);
                    }
                    charge(&mut bytes, text.capacity())?;
                    gemini_signature_bytes(signature, &mut bytes)?;
                }
                LlmContinuationBlock::BedrockRedacted(data) => {
                    if provider != LlmContinuationProvider::AwsBedrock || data.is_empty() {
                        return Err(NativeLlmContinuationError::InvalidProviderBlock);
                    }
                    charge(&mut bytes, data.capacity())?;
                }

                LlmContinuationBlock::Text(text) => {
                    if !matches!(
                        provider,
                        LlmContinuationProvider::Anthropic | LlmContinuationProvider::AwsBedrock
                    ) {
                        return Err(NativeLlmContinuationError::InvalidProviderBlock);
                    }
                    charge(&mut bytes, text.capacity())?;
                }
                LlmContinuationBlock::Thinking { text, signature } => {
                    if provider != LlmContinuationProvider::Anthropic {
                        return Err(NativeLlmContinuationError::InvalidProviderBlock);
                    }
                    if signature.is_empty() || signature.capacity() > Self::MAX_SIGNATURE_BYTES {
                        return Err(NativeLlmContinuationError::InvalidSignature);
                    }
                    charge(&mut bytes, text.capacity())?;
                    charge(&mut bytes, signature.capacity())?;
                }
                LlmContinuationBlock::ToolCall(call) => {
                    if provider == LlmContinuationProvider::Gemini
                        && !matches!(blocks.get(index + 1), Some(LlmContinuationBlock::GeminiToolMetadata { tool_call_id, .. }) if tool_call_id == &call.tool_call_id)
                    {
                        return Err(NativeLlmContinuationError::InvalidProviderBlock);
                    }
                    charge(&mut bytes, call.function_name.capacity())?;
                    charge(&mut bytes, call.tool_call_id.capacity())?;
                    if !valid_identity(&call.tool_call_id)
                        || !valid_identity(&call.function_name)
                        || !call.arguments.is_object()
                        || !ids.insert(call.tool_call_id.as_str())
                    {
                        return Err(NativeLlmContinuationError::InvalidToolCall);
                    }
                    argument_bytes(&call.arguments, 0, &mut bytes)?;
                }
                LlmContinuationBlock::GeminiText {
                    text, signature, ..
                } => {
                    if provider != LlmContinuationProvider::Gemini {
                        return Err(NativeLlmContinuationError::InvalidProviderBlock);
                    }
                    charge(&mut bytes, text.capacity())?;
                    gemini_signature_bytes(signature, &mut bytes)?;
                }
                LlmContinuationBlock::GeminiToolMetadata {
                    tool_call_id,
                    signature,
                } => {
                    if provider != LlmContinuationProvider::Gemini
                        || index == 0
                        || !matches!(&blocks[index - 1], LlmContinuationBlock::ToolCall(call) if &call.tool_call_id == tool_call_id)
                    {
                        return Err(NativeLlmContinuationError::InvalidProviderBlock);
                    }
                    charge(&mut bytes, tool_call_id.capacity())?;
                    gemini_signature_bytes(signature, &mut bytes)?;
                }
            }
        }
        Ok(Self {
            response_id,
            provider,
            phase: ContinuationPhase::Completed,
            blocks,
            retained_bytes: bytes,
        })
    }

    pub fn checkpoint(
        response_id: u64,
        provider: LlmContinuationProvider,
        blocks: Vec<LlmContinuationBlock>,
    ) -> Result<Self, NativeLlmContinuationError> {
        let mut value = Self::new(response_id, provider, blocks)?;
        if !value.blocks.iter().any(|block| {
            matches!(
                block,
                LlmContinuationBlock::BedrockReasoning {
                    signature: Some(_),
                    ..
                } | LlmContinuationBlock::BedrockRedacted(_)
                    | LlmContinuationBlock::Thinking { .. }
                    | LlmContinuationBlock::GeminiText {
                        signature: Some(_),
                        ..
                    }
                    | LlmContinuationBlock::GeminiToolMetadata {
                        signature: Some(_),
                        ..
                    }
            )
        }) {
            return Err(NativeLlmContinuationError::InvalidCheckpoint);
        }
        value.phase = ContinuationPhase::Checkpoint;
        Ok(value)
    }
    pub fn is_checkpoint(&self) -> bool {
        self.phase == ContinuationPhase::Checkpoint
    }
    pub fn response_id(&self) -> u64 {
        self.response_id
    }
    pub fn provider(&self) -> LlmContinuationProvider {
        self.provider
    }
    pub fn is_complete(&self) -> bool {
        self.phase == ContinuationPhase::Completed
    }
    pub fn blocks(&self) -> &[LlmContinuationBlock] {
        &self.blocks
    }
    /// Conservative carrier-only accounting, including spare capacities and
    /// container overhead. This does not bound or include the containing frame's
    /// own payload or metadata. Each queue reference reserves the full amount.
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn validate_frame(
        &self,
        data: &FrameData,
    ) -> Result<(), NativeLlmContinuationError> {
        let mut calls = self.blocks.iter().filter_map(|block| match block {
            LlmContinuationBlock::ToolCall(call) => Some(call),
            _ => None,
        });
        let matches = match data {
            FrameData::LlmResponseStart => self.phase == ContinuationPhase::Start,
            FrameData::LlmContinuationCheckpoint => self.is_checkpoint(),
            FrameData::LlmResponseEnd => self.is_complete() && calls.next().is_none(),
            FrameData::FunctionCallsStarted(frame_calls) => {
                self.is_complete() && !frame_calls.is_empty() && calls.eq(frame_calls.iter())
            }
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            Err(NativeLlmContinuationError::FrameMismatch)
        }
    }
}
fn gemini_signature_bytes(
    signature: &Option<String>,
    bytes: &mut usize,
) -> Result<(), NativeLlmContinuationError> {
    if let Some(signature) = signature {
        if signature.is_empty() || signature.capacity() > NativeLlmContinuation::MAX_SIGNATURE_BYTES
        {
            return Err(NativeLlmContinuationError::InvalidSignature);
        }
        charge(bytes, signature.capacity())?;
    }
    Ok(())
}
fn valid_identity(value: &str) -> bool {
    !value.is_empty()
        && !value
            .chars()
            .any(|ch| ch.is_whitespace() || ch.is_control())
}
fn charge(bytes: &mut usize, count: usize) -> Result<(), NativeLlmContinuationError> {
    *bytes = bytes
        .checked_add(count)
        .filter(|total| *total <= NativeLlmContinuation::MAX_RETAINED_BYTES)
        .ok_or(NativeLlmContinuationError::TooManyBytes)?;
    Ok(())
}
fn charge_product(
    bytes: &mut usize,
    count: usize,
    size: usize,
) -> Result<(), NativeLlmContinuationError> {
    charge(
        bytes,
        count
            .checked_mul(size)
            .ok_or(NativeLlmContinuationError::TooManyBytes)?,
    )
}
fn argument_bytes(
    value: &Value,
    depth: usize,
    bytes: &mut usize,
) -> Result<(), NativeLlmContinuationError> {
    if depth > NativeLlmContinuation::MAX_ARGUMENT_DEPTH {
        return Err(NativeLlmContinuationError::ArgumentsTooDeep);
    }
    match value {
        Value::String(text) => charge(bytes, text.capacity())?,
        Value::Array(values) => {
            charge_product(bytes, values.capacity(), size_of::<Value>())?;
            for value in values {
                argument_bytes(value, depth + 1, bytes)?;
            }
        }
        Value::Object(values) => {
            // A sparsely populated B-tree node still retains all its slots.
            // Charging a full node per entry also covers map/index overhead.
            let entry = 12 * (size_of::<(String, Value)>() + size_of::<usize>());
            charge_product(bytes, values.len(), entry)?;
            for (key, value) in values {
                charge(bytes, key.capacity())?;
                argument_bytes(value, depth + 1, bytes)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn allocate_response_id(next: &AtomicU64) -> Result<u64, NativeLlmContinuationError> {
    next.try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        if value == 0 {
            None
        } else {
            value.checked_add(1)
        }
    })
    .map_err(|_| NativeLlmContinuationError::ResponseIdExhausted)
}
#[cfg(test)]
mod response_identity_tests {
    use super::*;
    #[test]
    fn exhaustion_is_sticky_and_never_wraps() {
        let next = AtomicU64::new(u64::MAX - 1);
        assert_eq!(allocate_response_id(&next).unwrap(), u64::MAX - 1);
        for _ in 0..2 {
            assert_eq!(
                allocate_response_id(&next),
                Err(NativeLlmContinuationError::ResponseIdExhausted)
            );
        }
        assert_eq!(next.load(Ordering::Relaxed), u64::MAX);
        assert!(allocate_response_id(&AtomicU64::new(0)).is_err());
    }
}
