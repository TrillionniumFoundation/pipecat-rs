//! Strict in-memory RIFF/WAVE PCM16 reader.

use crate::{AudioError, PcmFormat, Result};

/// Borrowed PCM data and validated format from a RIFF/WAVE container.
#[derive(Debug, Clone, Copy)]
pub struct PcmWav<'a> {
    pub format: PcmFormat,
    pub pcm: &'a [u8],
}

fn invalid() -> AudioError {
    AudioError::InvalidParameter("invalid or unsupported PCM16 RIFF/WAVE container")
}
fn u16le(data: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([data[offset], data[offset + 1]])
}
fn u32le(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

/// Read ordinary format-code-1 PCM16 WAVE. Unknown chunks and their odd-byte
/// padding are skipped. Compressed, extensible, RF64, duplicate format/data
/// chunks, malformed lengths, and incomplete channel frames are rejected.
pub fn read_pcm16_wav(bytes: &[u8]) -> Result<PcmWav<'_>> {
    if bytes.len() < 12 || &bytes[..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return Err(invalid());
    }
    let end = (u32le(bytes, 4) as usize)
        .checked_add(8)
        .ok_or(AudioError::SizeOverflow)?;
    if end < 12 || end > bytes.len() {
        return Err(invalid());
    }
    let mut position = 12usize;
    let mut format = None;
    let mut pcm = None;
    while position < end {
        if end - position < 8 {
            return Err(invalid());
        }
        let id = &bytes[position..position + 4];
        let length = u32le(bytes, position + 4) as usize;
        let start = position + 8;
        let next = start.checked_add(length).ok_or(AudioError::SizeOverflow)?;
        if next > end {
            return Err(invalid());
        }
        let chunk = &bytes[start..next];
        if id == b"fmt " {
            if format.is_some() || length < 16 || u16le(chunk, 0) != 1 || u16le(chunk, 14) != 16 {
                return Err(invalid());
            }
            let value = PcmFormat::new(u32le(chunk, 4), u16le(chunk, 2))?;
            let byte_rate = value
                .sample_rate()
                .checked_mul(value.frame_bytes() as u32)
                .ok_or(AudioError::SizeOverflow)?;
            if usize::from(u16le(chunk, 12)) != value.frame_bytes() || u32le(chunk, 8) != byte_rate
            {
                return Err(invalid());
            }
            format = Some(value);
        } else if id == b"data" {
            if pcm.is_some() {
                return Err(invalid());
            }
            pcm = Some(chunk);
        }
        position = next
            .checked_add(length % 2)
            .ok_or(AudioError::SizeOverflow)?;
        if position > end {
            return Err(invalid());
        }
    }
    let format = format.ok_or_else(invalid)?;
    let pcm = pcm.ok_or_else(invalid)?;
    format.validate(pcm)?;
    Ok(PcmWav { format, pcm })
}
