//! Narrow unsigned encoded-image ingress plus bounded RGB/RGBX/YCbCr/L/CMYK/1 image encoding.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use pipecat_core::{
    AssistantImageRawFrame, ChatRole, ContextMessage, ImageRawFrame, MessageContent, MessagePart,
    ProcessorError, UserImageRawFrame,
};

// Native admission policy for one frame's encoded logical byte length. This is
// not a SharedContext total, input Vec capacity, image-decoder, or RSS budget.
const MAX_ORIGINAL_BYTES: usize = 8 * 1024 * 1024;
// Packed mode 1 has a separate work/storage cap on its expanded L8 pixels.
const MAX_MONO_EXPANDED_BYTES: usize = 8 * 1024 * 1024;
// Longest permitted prefix is "data:image/jpeg;base64," (23 bytes).
const MAX_DATA_URL_BYTES: usize = 4 * MAX_ORIGINAL_BYTES.div_ceil(3) + 23;

pub(crate) fn message(image: &AssistantImageRawFrame) -> Result<ContextMessage, ProcessorError> {
    // Match Python truthiness, not a rescue after original validation fails.
    // Unselected originals are ignored, including their storage/byte length.
    let url = match (
        image
            .original_data
            .as_deref()
            .filter(|data| !data.is_empty()),
        image
            .original_mime_type
            .as_deref()
            .filter(|mime| !mime.is_empty()),
    ) {
        (Some(original), Some(mime)) => {
            data_url(original, Some(mime), "assistant image", "original_data")?
        }
        _ => {
            if matches!(
                image.image.image.format.as_deref(),
                Some("RGB" | "RGBX" | "YCbCr" | "L" | "CMYK" | "1")
            ) {
                let jpeg = raw_jpeg(&image.image.image, "assistant image")?;
                data_url(&jpeg, Some("image/jpeg"), "assistant image", "image")?
            } else {
                data_url(
                    &image.image.image.image,
                    image.image.image.format.as_deref(),
                    "assistant image",
                    "image",
                )?
            }
        }
    };
    Ok(ContextMessage {
        role: ChatRole::Assistant,
        content: MessageContent::Parts(vec![MessagePart::Image { url, detail: None }]),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: Default::default(),
    })
}

/// Flags, ownership and state admission are checked by the assistant first.
pub(crate) fn user_message(image: &UserImageRawFrame) -> Result<ContextMessage, ProcessorError> {
    let url = if matches!(
        image.image.format.as_deref(),
        Some("RGB" | "RGBX" | "YCbCr" | "L" | "CMYK" | "1")
    ) {
        let jpeg = raw_jpeg(&image.image, "user image")?;
        data_url(&jpeg, Some("image/jpeg"), "user image", "image")?
    } else {
        data_url(
            &image.image.image,
            image.image.format.as_deref(),
            "user image",
            "image",
        )?
    };
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
    owner: &'static str,
    field: &'static str,
) -> Result<String, ProcessorError> {
    // Fixed call-site labels preserve encoded-original/UserImage diagnostics
    // without mislabeling generated assistant JPEGs or copying caller metadata.
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
            )));
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

// RGB, RGBX, YCbCr, L and CMYK are packed top-down RGB24, RGB+ignored-byte,
// full-range Y/Cb/Cr triplets, L8 and ordinary C/M/Y/K quads.
// YCbCr is already in the JPEG color domain; Cmyk inverts all four channels.
// No row padding. Input length and required prefix have independent caps;
// bounded surplus matches Image.frombytes.
// Owner labels are fixed at the two call sites, never copied from frame metadata.
fn raw_jpeg(image: &ImageRawFrame, owner: &'static str) -> Result<Vec<u8>, ProcessorError> {
    // Packed bits are a distinct layout, never a fictitious bytes-per-pixel mode.
    if image.format.as_deref() == Some("1") {
        return packed_mono_jpeg(image, owner);
    }
    let (mode, bytes_per_pixel, color) = match image.format.as_deref() {
        Some("RGB") => ("RGB", 3, jpeg_encoder::ColorType::Rgb),
        // Rgba is only the encoder layout: exact public RGBA is still rejected.
        Some("RGBX") => ("RGBX", 4, jpeg_encoder::ColorType::Rgba),
        Some("YCbCr") => ("YCbCr", 3, jpeg_encoder::ColorType::Ycbcr),
        Some("L") => ("L", 1, jpeg_encoder::ColorType::Luma),
        Some("CMYK") => ("CMYK", 4, jpeg_encoder::ColorType::Cmyk),
        _ => {
            return Err(ProcessorError::new(format!(
                "{owner} raw pixels require exact RGB, RGBX, YCbCr, L, CMYK or 1"
            )))
        }
    };
    let (width, height) = image.size;
    if width == 0 || height == 0 || width > 65_500 || height > 65_500 {
        return Err(ProcessorError::new(format!(
            "{owner} {mode} dimensions must be 1..=65500",
        )));
    }
    let required = usize::try_from(width)
        .ok()
        .and_then(|width| {
            usize::try_from(height)
                .ok()
                .and_then(|height| width.checked_mul(height))
        })
        .and_then(|pixels| pixels.checked_mul(bytes_per_pixel))
        .filter(|bytes| *bytes <= MAX_ORIGINAL_BYTES)
        .ok_or_else(|| ProcessorError::new(format!("{owner} {mode} pixel bytes exceed 8388608")))?;
    if image.image.len() > MAX_ORIGINAL_BYTES {
        return Err(ProcessorError::new(format!(
            "{owner} {mode} buffer exceeds 8388608 bytes",
        )));
    }
    if image.image.len() < required {
        return Err(ProcessorError::new(format!(
            "{owner} {mode} buffer is shorter than its dimensions",
        )));
    }
    let mut output = CappedJpegWriter::default();
    let mut encoder = jpeg_encoder::Encoder::new(&mut output, 75);
    // For CMYK the encoder applies this factor to K, with C/M/Y fixed at 1x1.
    // All four must be 1x1; preserve the existing factor for every other mode.
    encoder.set_sampling_factor(if mode == "CMYK" {
        jpeg_encoder::SamplingFactor::F_1_1
    } else {
        jpeg_encoder::SamplingFactor::F_2_2
    });
    encoder.set_progressive(false);
    encoder.set_optimized_huffman_tables(false);
    // Dimensions fit u16 by the stricter compatibility bound above. Prevalidation
    // also protects the encoder's internal usize input-size multiplication.
    encoder
        .encode(&image.image[..required], width as u16, height as u16, color)
        .map_err(|_| {
            ProcessorError::new(format!(
                "{owner} {mode} JPEG encoding failed or exceeded its output budget"
            ))
        })?;
    Ok(output.bytes)
}

// Exact mode 1: MSB-first rows, one set bit becomes 255 and one clear bit 0.
// Unused low bits in each final row byte and bounded trailing bytes are ignored.
fn packed_mono_jpeg(image: &ImageRawFrame, owner: &'static str) -> Result<Vec<u8>, ProcessorError> {
    let (width, height) = image.size;
    if width == 0 || height == 0 || width > 65_500 || height > 65_500 {
        return Err(ProcessorError::new(format!(
            "{owner} 1 dimensions must be 1..=65500",
        )));
    }
    let dimensions = usize::try_from(width)
        .ok()
        .zip(usize::try_from(height).ok());
    let expanded_bytes = dimensions
        .and_then(|(width, height)| width.checked_mul(height))
        .filter(|bytes| *bytes <= MAX_MONO_EXPANDED_BYTES)
        .ok_or_else(|| {
            ProcessorError::new(format!("{owner} 1 expanded pixel bytes exceed 8388608"))
        })?;
    let (width_usize, height_usize) = dimensions.ok_or_else(|| {
        ProcessorError::new(format!("{owner} 1 expanded pixel bytes exceed 8388608"))
    })?;
    let row_bytes = width_usize.checked_add(7).map(|width| width / 8);
    let required = row_bytes
        .and_then(|row| row.checked_mul(height_usize))
        .filter(|bytes| *bytes <= MAX_ORIGINAL_BYTES)
        .ok_or_else(|| {
            ProcessorError::new(format!("{owner} 1 packed pixel bytes exceed 8388608"))
        })?;
    if image.image.len() > MAX_ORIGINAL_BYTES {
        return Err(ProcessorError::new(format!(
            "{owner} 1 buffer exceeds 8388608 bytes",
        )));
    }
    if image.image.len() < required {
        return Err(ProcessorError::new(format!(
            "{owner} 1 buffer is shorter than its dimensions",
        )));
    }
    let row_bytes = row_bytes.ok_or_else(|| {
        ProcessorError::new(format!("{owner} 1 packed pixel bytes exceed 8388608"))
    })?;
    let mut expanded = Vec::new();
    expanded
        .try_reserve_exact(expanded_bytes)
        .map_err(|_| ProcessorError::new(format!("{owner} 1 expansion allocation failed")))?;
    for row in image.image[..required].chunks_exact(row_bytes) {
        for x in 0..width_usize {
            expanded.push(if row[x / 8] & (0x80 >> (x % 8)) != 0 {
                255
            } else {
                0
            });
        }
    }
    let mut output = CappedJpegWriter::default();
    let mut encoder = jpeg_encoder::Encoder::new(&mut output, 75);
    encoder.set_sampling_factor(jpeg_encoder::SamplingFactor::F_2_2);
    encoder.set_progressive(false);
    encoder.set_optimized_huffman_tables(false);
    encoder
        .encode(
            &expanded,
            width as u16,
            height as u16,
            jpeg_encoder::ColorType::Luma,
        )
        .map_err(|_| {
            ProcessorError::new(format!(
                "{owner} 1 JPEG encoding failed or exceeded its output budget"
            ))
        })?;
    // Release the only full-image expansion before the caller creates base64.
    drop(expanded);
    Ok(output.bytes)
}

#[derive(Default)]
struct CappedJpegWriter {
    bytes: Vec<u8>,
}
impl std::io::Write for CappedJpegWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let needed = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_ORIGINAL_BYTES)
            .ok_or_else(|| std::io::Error::other("JPEG output exceeds 8388608 bytes"))?;
        // Both limit and allocation failure are checked before any bytes append.
        // Bounded geometric requests avoid reallocating for each small write;
        // actual allocator capacity/RSS remains outside this logical byte cap.
        if needed > self.bytes.capacity() {
            let desired = needed
                .max(self.bytes.capacity().saturating_mul(2))
                .min(MAX_ORIGINAL_BYTES);
            self.bytes
                .try_reserve_exact(desired - self.bytes.len())
                .map_err(|_| std::io::Error::other("JPEG output allocation failed"))?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod rgb_tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn capped_writer_rejects_before_append_and_accepts_exact_limit() {
        let mut small = CappedJpegWriter::default();
        for byte in 0..8 {
            small.write_all(&[byte]).unwrap();
        }
        assert_eq!(small.bytes, (0..8).collect::<Vec<u8>>());
        let small_capacity = small.bytes.capacity();
        assert_eq!(small.write(&[]).unwrap(), 0);
        assert_eq!(small.bytes.capacity(), small_capacity);
        let mut writer = CappedJpegWriter {
            bytes: vec![7; MAX_ORIGINAL_BYTES - 1],
        };
        let capacity_before = writer.bytes.capacity();
        assert!(writer.write_all(&[8, 9]).is_err());
        assert_eq!(writer.bytes.capacity(), capacity_before);
        assert_eq!(writer.bytes.len(), MAX_ORIGINAL_BYTES - 1);
        assert!(writer.bytes.iter().all(|byte| *byte == 7));
        writer.write_all(&[8]).unwrap();
        assert_eq!(writer.bytes.len(), MAX_ORIGINAL_BYTES);
        assert_eq!(writer.bytes.last(), Some(&8));
        assert!(writer.write_all(&[9]).is_err());
        assert_eq!(writer.bytes.len(), MAX_ORIGINAL_BYTES);
        assert_eq!(writer.write(&[]).unwrap(), 0);
        writer.flush().unwrap();
    }
}
