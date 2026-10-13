//! Narrow unsigned PDF ingress: encoding/header validation, not PDF sanitization.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use pipecat_core::{
    ChatRole, ContextMessage, DetailedUserFileRawFrame, FileSourceType, MessageContent,
    MessagePart, ProcessorError,
};

// Per-file logical lengths, not aggregate context, text/name, capacity or RSS.
const MAX_FILE_BYTES: usize = 8 * 1024 * 1024;
const PREFIX: &str = "data:application/pdf;base64,";
const MAX_ENCODED_BYTES: usize = 4 * MAX_FILE_BYTES.div_ceil(3);
const MAX_DATA_URL_BYTES: usize = MAX_ENCODED_BYTES + PREFIX.len();

/// Ownership, state and append/run flags are checked by the assistant first.
pub(crate) fn user_message(
    file: &DetailedUserFileRawFrame,
) -> Result<ContextMessage, ProcessorError> {
    if file.file.source_type != FileSourceType::Bytes || file.file.format != "application/pdf" {
        return Err(ProcessorError::new(
            "encoded user file requires application/pdf Bytes",
        ));
    }
    // Bound the existing input before decoding or cloning any of its contents.
    if file.file.file.len() > MAX_DATA_URL_BYTES {
        return Err(ProcessorError::new(
            "encoded user file exceeds 8388608 decoded bytes",
        ));
    }
    let encoded = file.file.file.strip_prefix(PREFIX).ok_or_else(|| {
        ProcessorError::new("encoded user file requires the exact PDF base64 data URL header")
    })?;
    if encoded.is_empty() || encoded.len() > MAX_ENCODED_BYTES {
        return Err(ProcessorError::new(
            "encoded user file requires nonempty bounded base64",
        ));
    }
    let decoded_bound = encoded
        .len()
        .checked_add(3)
        .and_then(|length| (length / 4).checked_mul(3))
        .ok_or_else(|| ProcessorError::new("encoded user file size overflow"))?;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(decoded_bound)
        .map_err(|_| ProcessorError::new("encoded user file allocation failed"))?;
    decoded.resize(decoded_bound, 0);
    // STANDARD requires canonical padding and zero unused trailing bits. No
    // re-encoding allocation is necessary to reject noncanonical input.
    let decoded_len = STANDARD
        .decode_slice(encoded, &mut decoded)
        .map_err(|_| ProcessorError::new("encoded user file requires canonical STANDARD base64"))?;
    if decoded_len == 0 || decoded_len > MAX_FILE_BYTES {
        return Err(ProcessorError::new(
            "encoded user file requires 1..=8388608 decoded bytes",
        ));
    }
    if !decoded[..decoded_len].starts_with(b"%PDF-") {
        return Err(ProcessorError::new(
            "encoded user file requires a PDF signature",
        ));
    }
    drop(decoded);
    let mut parts = Vec::new();
    parts
        .try_reserve_exact(2)
        .map_err(|_| ProcessorError::new("encoded user file allocation failed"))?;
    if !file.text.is_empty() {
        parts.push(MessagePart::Text {
            text: copy(&file.text)?,
        });
    }
    parts.push(MessagePart::File {
        data: copy(encoded)?,
        mime_type: "application/pdf".into(),
        filename: Some(copy(file.file.filename.as_deref().unwrap_or(""))?),
    });
    Ok(ContextMessage {
        role: ChatRole::User,
        content: MessageContent::Parts(parts),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: Default::default(),
    })
}

fn copy(value: &str) -> Result<String, ProcessorError> {
    let mut copied = String::new();
    copied
        .try_reserve_exact(value.len())
        .map_err(|_| ProcessorError::new("encoded user file allocation failed"))?;
    copied.push_str(value);
    Ok(copied)
}
