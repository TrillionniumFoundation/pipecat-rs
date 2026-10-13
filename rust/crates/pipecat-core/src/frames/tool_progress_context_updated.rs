use super::{Frame, FrameData, LocalAsyncCallback};
use crate::{NativeToolCallProgress, ProcessorError};
use std::sync::{Arc, Mutex};

/// Frame-only progress notification. Progress receipts do not retain this slot.
#[derive(Debug, Clone)]
pub(super) struct ProgressContextUpdatedBinding {
    progress: Arc<NativeToolCallProgress>,
    callback: Arc<Mutex<Option<LocalAsyncCallback>>>,
}
impl PartialEq for ProgressContextUpdatedBinding {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.progress, &other.progress) && Arc::ptr_eq(&self.callback, &other.callback)
    }
}
impl ProgressContextUpdatedBinding {
    fn validate_bound(&self, data: &FrameData) -> Result<(), ProcessorError> {
        self.progress.validate(data)
    }
    pub(super) fn validate(
        &self,
        progress: &NativeToolCallProgress,
        data: &FrameData,
    ) -> Result<(), ProcessorError> {
        self.validate_bound(data)?;
        if progress.id() != self.progress.id() || progress != self.progress.as_ref() {
            return Err(ProcessorError::new(
                "context-update callback does not match tool progress",
            ));
        }
        progress.validate(data)
    }
}
impl Frame {
    pub(crate) fn attach_tool_progress_context_updated(
        &mut self,
        progress: Arc<NativeToolCallProgress>,
        callback: LocalAsyncCallback,
    ) {
        self.native_tool_progress_context_updated = Some(ProgressContextUpdatedBinding {
            progress,
            callback: Arc::new(Mutex::new(Some(callback))),
        });
    }

    fn validate_progress_callback_kind(&self) -> Result<(), ProcessorError> {
        if self.native_tool_outcome.is_some()
            || self.native_tool_context_updated.is_some()
            || self.native_tool_batch.is_some()
            || self.native_tool_timeout_projected
        {
            return Err(ProcessorError::new(
                "progress context-update callback cannot carry terminal or batch receipts",
            ));
        }
        Ok(())
    }

    /// Inspect the sealed intermediate publication ID without cloning its hook.
    /// The bound progress still validates after its separate receipt is taken.
    pub fn native_tool_progress_context_updated(&self) -> Result<Option<u64>, ProcessorError> {
        let Some(binding) = &self.native_tool_progress_context_updated else {
            return Ok(None);
        };
        self.validate_progress_callback_kind()?;
        binding.validate_bound(&self.data)?;
        self.native_tool_progress()?;
        Ok(Some(binding.progress.id()))
    }

    /// Transfer a verified intermediate hook exactly once across frame clones.
    /// Validation leaves both slots untouched on failure; every carrier must be
    /// consumed explicitly before wire encoding, including an emptied shared slot.
    pub fn take_native_tool_progress_context_updated(
        &mut self,
        progress: &NativeToolCallProgress,
    ) -> Result<Option<LocalAsyncCallback>, ProcessorError> {
        let Some(binding) = &self.native_tool_progress_context_updated else {
            return Ok(None);
        };
        self.validate_progress_callback_kind()?;
        binding.validate(progress, &self.data)?;
        self.native_tool_progress()?;
        let callback = binding
            .callback
            .lock()
            .map_err(|_| ProcessorError::fatal("progress context-update callback state poisoned"))?
            .take();
        self.native_tool_progress_context_updated = None;
        Ok(callback)
    }
}
