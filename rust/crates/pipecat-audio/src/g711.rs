//! G.711 codecs with the same PCM16 quantization as CPython `audioop`.
//!
//! Encoding fixtures enumerate every signed 16-bit sample. Decoding fixtures
//! enumerate every byte. Conversions do not resample or change channel order.

use crate::{decode_pcm16_le, Result};

/// Encode a signed PCM16 sample using G.711 mu-law (no optional zero trap).
pub fn encode_ulaw(sample: i16) -> u8 {
    let mut pcm = i32::from(sample) >> 2;
    let mask = if pcm < 0 {
        pcm = -pcm;
        0x7f
    } else {
        0xff
    };
    pcm = pcm.min(8159) + 33;
    let segment = [0x3f, 0x7f, 0xff, 0x1ff, 0x3ff, 0x7ff, 0xfff, 0x1fff]
        .iter()
        .position(|&end| pcm <= end)
        .unwrap_or(8);
    let encoded = if segment >= 8 {
        0x7f
    } else {
        ((segment as u8) << 4) | (((pcm >> (segment + 1)) & 0x0f) as u8)
    };
    encoded ^ mask
}

pub fn decode_ulaw(code: u8) -> i16 {
    let u = !code;
    let magnitude = ((i32::from(u & 0x0f) << 3) + 132) << ((u & 0x70) >> 4);
    if u & 0x80 != 0 {
        (132 - magnitude) as i16
    } else {
        (magnitude - 132) as i16
    }
}

pub fn encode_alaw(sample: i16) -> u8 {
    let mut pcm = i32::from(sample) >> 3;
    let mask = if pcm >= 0 {
        0xd5
    } else {
        pcm = -pcm - 1;
        0x55
    };
    let segment = [0x1f, 0x3f, 0x7f, 0xff, 0x1ff, 0x3ff, 0x7ff, 0xfff]
        .iter()
        .position(|&end| pcm <= end)
        .unwrap_or(8);
    let encoded = if segment >= 8 {
        0x7f
    } else {
        ((segment as u8) << 4) | (((pcm >> if segment < 2 { 1 } else { segment }) & 0x0f) as u8)
    };
    encoded ^ mask
}

pub fn decode_alaw(code: u8) -> i16 {
    let a = code ^ 0x55;
    let segment = (a & 0x70) >> 4;
    let mut magnitude = i32::from(a & 0x0f) << 4;
    magnitude += if segment == 0 { 8 } else { 264 };
    if segment > 1 {
        magnitude <<= segment - 1;
    }
    if a & 0x80 != 0 {
        magnitude as i16
    } else {
        -magnitude as i16
    }
}

pub fn pcm_to_ulaw(pcm: &[u8]) -> Result<Vec<u8>> {
    Ok(decode_pcm16_le(pcm)?.into_iter().map(encode_ulaw).collect())
}

pub fn ulaw_to_pcm(ulaw: &[u8]) -> Vec<u8> {
    ulaw.iter()
        .flat_map(|&code| decode_ulaw(code).to_le_bytes())
        .collect()
}

pub fn pcm_to_alaw(pcm: &[u8]) -> Result<Vec<u8>> {
    Ok(decode_pcm16_le(pcm)?.into_iter().map(encode_alaw).collect())
}

pub fn alaw_to_pcm(alaw: &[u8]) -> Vec<u8> {
    alaw.iter()
        .flat_map(|&code| decode_alaw(code).to_le_bytes())
        .collect()
}
