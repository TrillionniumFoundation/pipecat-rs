//! Explicit, bounded original-image submission. Decoding validates input only;
//! the wire payload always preserves the original encoded bytes.
use crate::text_entry::{
    TextEntry, TextPayload, TextRequest, MAX_BUFFERED_BYTES, MAX_TEXT_BYTES, MAX_TEXT_WIRE_BYTES,
    MAX_WEBRTC_TEXT_WIRE_BYTES,
};
use crate::{protocol, Transport};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::io::Cursor;

pub const MAX_WS_IMAGE_BYTES: usize = 64 * 1024;
pub const MAX_RTC_IMAGE_BYTES: usize = 8 * 1024;
pub const MAX_IMAGE_PIXELS: usize = 1024 * 1024;
const MAX_DECODE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub struct ImageEntry {
    readiness: TextEntry,
    session: u64,
    next: u64,
    active: Option<u64>,
}
impl ImageEntry {
    pub fn new(session: u64) -> Self {
        Self {
            readiness: TextEntry::new(session),
            session,
            next: 1,
            active: None,
        }
    }
    pub fn accept_ready(&mut self, value: &Value) -> bool {
        self.readiness.accept_ready(value)
    }
    pub fn is_ready(&self) -> bool {
        self.readiness.is_ready()
    }
    pub fn is_current(&self, ticket: u64) -> bool {
        self.is_ready() && self.active == Some(ticket)
    }
    pub fn cancel(&mut self) {
        self.active = None;
    }
    pub fn close(&mut self) {
        self.cancel();
        self.readiness.close();
    }
    pub fn begin(&mut self, size: u64, mime: &str, transport: Transport) -> Result<u64, String> {
        if !self.is_ready() {
            return Err("Wait for this session's bot-ready before sending an image".into());
        }
        let limit = image_limit(transport)?;
        if size == 0 || size > limit as u64 {
            return Err("Image file exceeds this transport's limit (WebSocket 64 KiB; WebRTC 8 KiB), or is empty".into());
        }
        if !matches!(mime, "image/png" | "image/jpeg") {
            return Err("Choose a PNG or JPEG image with a matching MIME type".into());
        }
        if self.active.is_some() {
            return Err("An image read is already pending; cancel it before retrying".into());
        }
        let next = self
            .next
            .checked_add(1)
            .ok_or("Image request IDs exhausted")?;
        let ticket = self.next;
        self.next = next;
        self.active = Some(ticket);
        Ok(ticket)
    }
    pub fn prepare(
        &self,
        ticket: u64,
        bytes: &[u8],
        mime: &str,
        caption: &str,
        transport: Transport,
        buffered: u64,
    ) -> Result<TextRequest, String> {
        if !self.is_current(ticket) {
            return Err("Image selection or session is no longer current".into());
        }
        if bytes.is_empty() || bytes.len() > image_limit(transport)? {
            return Err("Image file size is outside the transport limit".into());
        }
        if caption.len() > MAX_TEXT_BYTES {
            return Err("Image caption exceeds 16 KiB UTF-8".into());
        }
        let (width, height) = validate_image(bytes, mime)?;
        let id = format!("image-{}-{ticket}", self.session);
        let value = json!({"id":id,"label":"rtvi-ai","type":"send-file","data":{
            "content":caption,"file":{"format":mime,"source":{"type":"bytes","bytes":STANDARD.encode(bytes),"width":width,"height":height}},
            "options":{"run_immediately":true,"audio_response":true}
        }});
        let (payload, limit) = match transport {
            Transport::WebSocket => (
                TextPayload::WebSocket(protocol::encode_message(&value)),
                MAX_TEXT_WIRE_BYTES,
            ),
            Transport::SmallWebRtc => (
                TextPayload::WebRtc(value.to_string()),
                MAX_WEBRTC_TEXT_WIRE_BYTES,
            ),
            _ => return Err("Image transport is unsupported".into()),
        };
        if payload.len() > limit {
            return Err("Encoded image request exceeds this transport's wire limit".into());
        }
        if buffered
            .checked_add(payload.len() as u64)
            .is_none_or(|n| n > MAX_BUFFERED_BYTES)
        {
            return Err("Transport is busy; image kept for explicit retry".into());
        }
        Ok(TextRequest { id, payload })
    }
}
fn image_limit(transport: Transport) -> Result<usize, String> {
    match transport {
        Transport::WebSocket => Ok(MAX_WS_IMAGE_BYTES),
        Transport::SmallWebRtc => Ok(MAX_RTC_IMAGE_BYTES),
        _ => Err("Image transport is unsupported".into()),
    }
}
fn dimensions(width: u32, height: u32) -> Result<(), String> {
    if width == 0
        || height == 0
        || width > 4096
        || height > 4096
        || (width as usize)
            .checked_mul(height as usize)
            .is_none_or(|n| n > MAX_IMAGE_PIXELS)
    {
        return Err("Image exceeds the 4096-side or 1-megapixel decode budget".into());
    }
    Ok(())
}
pub fn validate_image(bytes: &[u8], mime: &str) -> Result<(u32, u32), String> {
    if bytes.len() > MAX_WS_IMAGE_BYTES {
        return Err("Image exceeds the encoded input budget".into());
    }
    match mime {
        "image/png" if bytes.starts_with(b"\x89PNG\r\n\x1a\n") => validate_png(bytes),
        "image/jpeg" if bytes.starts_with(&[0xff, 0xd8, 0xff]) => {
            preflight_jpeg(bytes)?;
            let mut cursor = Cursor::new(bytes);
            let mut decoder = jpeg_decoder::Decoder::new(&mut cursor);
            decoder.set_max_decoding_buffer_size(MAX_DECODE_BYTES);
            decoder.set_strict_mode(true);
            decoder.read_info().map_err(|_| "Invalid JPEG header")?;
            let info = decoder.info().ok_or("Missing JPEG dimensions")?;
            dimensions(u32::from(info.width), u32::from(info.height))?;
            decoder
                .decode()
                .map_err(|_| "Invalid or incomplete JPEG image")?;
            drop(decoder);
            if cursor.position() != bytes.len() as u64 {
                return Err("Trailing JPEG data is not accepted".into());
            }
            Ok((u32::from(info.width), u32::from(info.height)))
        }
        _ => Err("Only PNG/JPEG files with matching signatures are accepted".into()),
    }
}
fn validate_png(bytes: &[u8]) -> Result<(u32, u32), String> {
    let invalid = || "Invalid, incomplete, or oversized PNG image".to_string();
    if bytes.len() < 33 || &bytes[12..16] != b"IHDR" || bytes[8..12] != 13u32.to_be_bytes() {
        return Err(invalid());
    }
    let width = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
    let height = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
    dimensions(width, height)?;
    let mut offset = 8usize;
    let mut ended = false;
    let mut compressed = Vec::new();
    while offset < bytes.len() {
        let header = bytes.get(offset..offset + 8).ok_or_else(invalid)?;
        let n = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        let end = offset
            .checked_add(n)
            .and_then(|n| n.checked_add(12))
            .ok_or_else(invalid)?;
        let chunk = bytes.get(offset + 4..end).ok_or_else(invalid)?;
        let (data, crc) = chunk.split_at(chunk.len() - 4);
        if crc32fast::hash(data) != u32::from_be_bytes(crc.try_into().unwrap()) {
            return Err("PNG checksum mismatch".into());
        }
        match &header[4..] {
            b"acTL" | b"fcTL" | b"fdAT" => {
                return Err("Animated PNG files are not supported".into())
            }
            b"IDAT" => compressed.extend_from_slice(&data[4..]),
            b"IEND" => {
                if n != 0 || end != bytes.len() {
                    return Err(invalid());
                }
                ended = true;
            }
            _ => {}
        }
        offset = end;
    }
    if !ended || compressed.is_empty() {
        return Err(invalid());
    }
    // Independently require exactly one complete zlib stream with validated
    // Adler checksum. PNG decoders may otherwise tolerate trailing IDAT bytes.
    let channels = match bytes[25] {
        0 | 3 => 1usize,
        2 => 3,
        4 => 2,
        6 => 4,
        _ => return Err(invalid()),
    };
    let depth = usize::from(bytes[24]);
    if !matches!(depth, 1 | 2 | 4 | 8 | 16) {
        return Err(invalid());
    }
    let row = |w: usize, h: usize| -> usize {
        if w == 0 || h == 0 {
            0
        } else {
            ((w * channels * depth).div_ceil(8) + 1) * h
        }
    };
    let expected = match bytes[28] {
        0 => row(width as usize, height as usize),
        1 => [
            (0, 0, 8, 8),
            (4, 0, 8, 8),
            (0, 4, 4, 8),
            (2, 0, 4, 4),
            (0, 2, 2, 4),
            (1, 0, 2, 2),
            (0, 1, 1, 2),
        ]
        .iter()
        .map(|&(x, y, dx, dy)| {
            row(
                (width as usize).saturating_sub(x).div_ceil(dx),
                (height as usize).saturating_sub(y).div_ceil(dy),
            )
        })
        .sum(),
        _ => return Err(invalid()),
    };
    if expected >= MAX_DECODE_BYTES {
        return Err(invalid());
    }
    let mut inflater = flate2::Decompress::new(true);
    let mut scanlines = vec![0; expected + 1];
    let status = inflater
        .decompress(&compressed, &mut scanlines, flate2::FlushDecompress::Finish)
        .map_err(|_| invalid())?;
    if status != flate2::Status::StreamEnd
        || inflater.total_in() != compressed.len() as u64
        || inflater.total_out() != expected as u64
    {
        return Err(invalid());
    }
    drop(scanlines);
    let mut decoder = png::Decoder::new_with_limits(
        Cursor::new(bytes),
        png::Limits {
            bytes: MAX_DECODE_BYTES,
        },
    );
    decoder.set_ignore_text_chunk(true);
    decoder.set_ignore_iccp_chunk(true);
    let mut reader = decoder.read_info().map_err(|_| invalid())?;
    let size = reader
        .output_buffer_size()
        .filter(|n| *n <= MAX_DECODE_BYTES)
        .ok_or_else(invalid)?;
    let mut pixels = vec![0; size];
    reader.next_frame(&mut pixels).map_err(|_| invalid())?;
    reader.finish().map_err(|_| invalid())?;
    Ok((width, height))
}

// Bound JPEG component sampling and progressive coefficient storage before the
// decoder can allocate a frame. Encoded data and marker scans are already capped.
fn preflight_jpeg(bytes: &[u8]) -> Result<(), String> {
    let bad = || "Unsupported or oversized JPEG frame".to_string();
    let mut offset = 2usize;
    loop {
        if bytes.get(offset) != Some(&0xff) {
            return Err(bad());
        }
        while bytes.get(offset) == Some(&0xff) {
            offset += 1;
        }
        let marker = *bytes.get(offset).ok_or_else(bad)?;
        offset += 1;
        if matches!(marker, 0x00 | 0xd8 | 0xd9 | 0xda | 0xd0..=0xd7) {
            return Err(bad());
        }
        let size = bytes.get(offset..offset + 2).ok_or_else(bad)?;
        let size = usize::from(u16::from_be_bytes(size.try_into().unwrap()));
        if size < 2 {
            return Err(bad());
        }
        let data = bytes.get(offset + 2..offset + size).ok_or_else(bad)?;
        if (0xc0..=0xcf).contains(&marker) && ![0xc4, 0xc8, 0xcc].contains(&marker) {
            if !matches!(marker, 0xc0..=0xc2) || data.len() < 6 || data[0] != 8 {
                return Err(bad());
            }
            let width = u32::from(u16::from_be_bytes([data[3], data[4]]));
            let height = u32::from(u16::from_be_bytes([data[1], data[2]]));
            dimensions(width, height)?;
            let count = usize::from(data[5]);
            if !matches!(count, 1 | 3 | 4) || data.len() != 6 + count * 3 {
                return Err(bad());
            }
            for part in data[6..].as_chunks::<3>().0 {
                if !(1..=2).contains(&(part[1] >> 4)) || !(1..=2).contains(&(part[1] & 15)) {
                    return Err(bad());
                }
            }
            // Deliberately conservative padded storage budget covers coefficient
            // planes, IDCT samples and final pixels, including skinny frames.
            let padded_pixels =
                (width as usize).div_ceil(16) * 16 * (height as usize).div_ceil(16) * 16;
            if padded_pixels
                .checked_mul(64)
                .is_none_or(|n| n > MAX_DECODE_BYTES)
            {
                return Err(bad());
            }
            return Ok(());
        }
        offset += size;
    }
}
