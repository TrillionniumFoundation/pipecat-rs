//! Bounded rendering plan for a single owner-authorized speech request.
const MAX_REQUEST_BYTES: usize = 16 * 1024;
const MAX_SEGMENT_BYTES: usize = 4096;

/// Validate the entire request before any synthesis. Segments preserve every
/// byte and use one utterance identity; they are not independent Hepta turns.
pub fn speech_segments(text: &str) -> Result<Vec<&str>, &'static str> {
    if text.trim().is_empty()
        || text.len() > MAX_REQUEST_BYTES
        || text.contains('\0')
        || text.contains("[[")
        || text.contains("]]")
    {
        return Err("speech must be nonblank, NUL/IPA-free, and at most 16384 bytes");
    }
    let mut remaining = text;
    let mut segments = Vec::new();
    while !remaining.is_empty() {
        let mut end = remaining.len().min(MAX_SEGMENT_BYTES);
        while !remaining.is_char_boundary(end) {
            end -= 1;
        }
        // Prefer a phrase boundary in the latter half, avoiding tiny chunks.
        if end < remaining.len() {
            if let Some((offset, ch)) =
                remaining[..end].char_indices().rev().find(|(offset, ch)| {
                    *offset >= end / 2 && (ch.is_whitespace() || "。！？；.!?;".contains(*ch))
                })
            {
                end = offset + ch.len_utf8();
            }
        }
        segments.push(&remaining[..end]);
        remaining = &remaining[end..];
    }
    Ok(segments)
}
