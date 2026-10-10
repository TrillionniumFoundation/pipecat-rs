use crate::StreamingError;
use pipecat_core::{ErrorCategory, ErrorFrame, ProcessorError};
pub(crate) fn error_frame(error: StreamingError) -> ErrorFrame {
    let category = match error {
        StreamingError::Http(401 | 403) => ErrorCategory::Connectivity,
        StreamingError::Provider {
            status: Some(401 | 403),
        } => ErrorCategory::Connectivity,
        StreamingError::Http(status)
        | StreamingError::Provider {
            status: Some(status),
        } => ErrorCategory::from_http_status(status),
        StreamingError::Invalid(_) => ErrorCategory::InvalidRequest,
        StreamingError::Transport | StreamingError::Timeout(_) | StreamingError::Closed => {
            ErrorCategory::Connectivity
        }
        _ => ErrorCategory::Application,
    };
    ErrorFrame {
        error: error.to_string(),
        category,
        ..ErrorFrame::new("")
    }
}
pub(crate) fn processor_error(error: StreamingError) -> ProcessorError {
    let frame = error_frame(error);
    ProcessorError::new(frame.error).with_category(frame.category)
}
pub(crate) fn timestamp() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}
