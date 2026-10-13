use super::{Frame, FrameData, LocalAsyncCallback};
use crate::{NativeToolCallOutcome, ProcessorError, ToolCallTerminalState};
use std::sync::{Arc, Mutex};

/// Frame-only callback ownership. The retained terminal outcome has no back-reference.
#[derive(Debug, Clone)]
pub(super) struct ContextUpdatedBinding {
    outcome: Arc<NativeToolCallOutcome>,
    callback: Arc<Mutex<Option<LocalAsyncCallback>>>,
}
impl PartialEq for ContextUpdatedBinding {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.outcome, &other.outcome) && Arc::ptr_eq(&self.callback, &other.callback)
    }
}
impl ContextUpdatedBinding {
    fn validate_bound(
        &self,
        data: &FrameData,
        timeout_projection: bool,
    ) -> Result<(), ProcessorError> {
        if timeout_projection || self.outcome.state() != ToolCallTerminalState::Completed {
            return Err(ProcessorError::new(
                "context-update callback requires a completed tool outcome",
            ));
        }
        self.outcome.validate(data, false)
    }
    pub(super) fn validate(
        &self,
        outcome: &NativeToolCallOutcome,
        data: &FrameData,
        timeout_projection: bool,
    ) -> Result<(), ProcessorError> {
        self.validate_bound(data, timeout_projection)?;
        if outcome.id() != self.outcome.id() || outcome != self.outcome.as_ref() {
            return Err(ProcessorError::new(
                "context-update callback does not match a completed tool outcome",
            ));
        }
        outcome.validate(data, false)
    }
}
impl Frame {
    pub(crate) fn attach_tool_context_updated(
        &mut self,
        outcome: Arc<NativeToolCallOutcome>,
        callback: LocalAsyncCallback,
    ) {
        self.native_tool_context_updated = Some(ContextUpdatedBinding {
            outcome,
            callback: Arc::new(Mutex::new(Some(callback))),
        });
    }

    /// Inspect only the immutable callback binding identity, without cloning or
    /// running its callable. The bound outcome still validates the sealed body
    /// after the terminal receipt has been taken; this ID alone cannot consume it.
    pub fn native_tool_context_updated(&self) -> Result<Option<u64>, ProcessorError> {
        let Some(binding) = &self.native_tool_context_updated else {
            return Ok(None);
        };
        if self.native_tool_progress.is_some() {
            return Err(ProcessorError::new(
                "nonterminal tool progress cannot carry a context-update callback",
            ));
        }
        binding.validate_bound(&self.data, self.native_tool_timeout_projected)?;
        self.native_tool_outcome()?;
        Ok(Some(binding.outcome.id()))
    }

    /// Transfer the callback after consuming its matching successful result.
    /// Validation precedes either slot mutation. Clones share one callable;
    /// every frame's carrier must still be explicitly consumed before encoding.
    pub fn take_native_tool_context_updated(
        &mut self,
        outcome: &NativeToolCallOutcome,
    ) -> Result<Option<LocalAsyncCallback>, ProcessorError> {
        let Some(binding) = &self.native_tool_context_updated else {
            return Ok(None);
        };
        if self.native_tool_progress.is_some() {
            return Err(ProcessorError::new(
                "nonterminal tool progress cannot carry a context-update callback",
            ));
        }
        binding.validate(outcome, &self.data, self.native_tool_timeout_projected)?;
        if let Some(attached) = self.native_tool_outcome()? {
            if attached.id() != outcome.id() {
                return Err(ProcessorError::new(
                    "context-update callback does not match the frame's tool outcome",
                ));
            }
        }
        let callback = binding
            .callback
            .lock()
            .map_err(|_| ProcessorError::fatal("context-update callback state poisoned"))?
            .take();
        self.native_tool_context_updated = None;
        Ok(callback)
    }
}
