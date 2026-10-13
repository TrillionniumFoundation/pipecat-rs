//! Narrow unsigned encoded-image ingress; no pixel decoding or provider receipt.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use pipecat_core::{
    AssistantImageRawFrame, ChatRole, ContextMessage, MessageContent, MessagePart, ProcessorError,
    UserImageRawFrame,
};

// Native admission policy for one frame's encoded logical byte length. This is
// not a SharedContext total, input Vec capacity, image-decoder, or RSS budget.
const MAX_ORIGINAL_BYTES: usize = 8 * 1024 * 1024;
// Longest permitted prefix is "data:image/jpeg;base64," (23 bytes).
const MAX_DATA_URL_BYTES: usize = 4 * MAX_ORIGINAL_BYTES.div_ceil(3) + 23;

pub(crate) fn message(image: &AssistantImageRawFrame) -> Result<ContextMessage, ProcessorError> {
    let original = image.original_data.as_deref().ok_or_else(|| {
        ProcessorError::new("assistant image requires encoded original_data; raw image context conversion is not supported")
    })?;
    let url = data_url(original, image.original_mime_type.as_deref(), true)?;
    Ok(ContextMessage {
        role: ChatRole::Assistant,
        content: MessageContent::Parts(vec![MessagePart::Image { url, detail: None }]),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: Default::default(),
    })
}

/// Flags, ownership and idle-state admission are checked by the assistant first.
pub(crate) fn user_message(image: &UserImageRawFrame) -> Result<ContextMessage, ProcessorError> {
    let url = data_url(&image.image.image, image.image.format.as_deref(), false)?;
    let mut parts = Vec::with_capacity(2);
    if let Some(text) = image.text.as_ref().filter(|text| !text.is_empty()) {
        parts.push(MessagePart::Text { text: text.clone() });
    }
    parts.push(MessagePart::Image { url, detail: None });
    Ok(ContextMessage {
        role: ChatRole::User,
        content: MessageContent::Parts(parts),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: Default::default(),
    })
}

fn data_url(
    original: &[u8],
    mime: Option<&str>,
    assistant_original: bool,
) -> Result<String, ProcessorError> {
    // Preserve the existing AssistantImage diagnostics while naming UserImage's
    // actual encoded field. These labels never contain caller-supplied text.
    let (owner, field) = if assistant_original {
        ("assistant image", "original_data")
    } else {
        ("user image", "image")
    };
    if original.is_empty() || original.len() > MAX_ORIGINAL_BYTES {
        return Err(ProcessorError::new(format!(
            "{owner} {field} must contain 1..=8388608 bytes",
        )));
    }
    // MIME matching selects a fixed prefix: unbounded caller MIME is never copied.
    let prefix = match mime {
        Some("image/png") if original.starts_with(b"\x89PNG\r\n\x1a\n") => "data:image/png;base64,",
        Some("image/jpeg") if original.starts_with(b"\xff\xd8\xff") => "data:image/jpeg;base64,",
        _ => {
            return Err(ProcessorError::new(format!(
                "{owner} requires image/png or image/jpeg MIME with a matching signature",
            )))
        }
    };
    // Signature admission is not complete image validation. The downstream
    // provider may reject malformed data that has the expected signature.
    let data_url_bytes = base64::encoded_len(original.len(), true)
        .and_then(|encoded| prefix.len().checked_add(encoded))
        .filter(|bytes| *bytes <= MAX_DATA_URL_BYTES)
        .ok_or_else(|| ProcessorError::new(format!("{owner} data URL exceeds byte limit")))?;
    let mut url = String::new();
    url.try_reserve_exact(data_url_bytes)
        .map_err(|_| ProcessorError::new(format!("{owner} data URL allocation failed")))?;
    url.push_str(prefix);
    STANDARD.encode_string(original, &mut url);
    Ok(url)
}
