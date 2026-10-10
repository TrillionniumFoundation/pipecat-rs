//! Telephone keypad entries, bundled Pipecat tones, and explicit tone synthesis.

use std::f64::consts::TAU;
use std::{fmt, str::FromStr};

use crate::{read_pcm16_wav, resample_pcm16, AudioError, PcmFormat, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeypadEntry {
    One,
    Two,
    Three,
    Four,
    Five,
    Six,
    Seven,
    Eight,
    Nine,
    Zero,
    Pound,
    Star,
}

impl KeypadEntry {
    pub const ALL: [Self; 12] = [
        Self::One,
        Self::Two,
        Self::Three,
        Self::Four,
        Self::Five,
        Self::Six,
        Self::Seven,
        Self::Eight,
        Self::Nine,
        Self::Zero,
        Self::Pound,
        Self::Star,
    ];

    pub fn as_char(self) -> char {
        match self {
            Self::One => '1',
            Self::Two => '2',
            Self::Three => '3',
            Self::Four => '4',
            Self::Five => '5',
            Self::Six => '6',
            Self::Seven => '7',
            Self::Eight => '8',
            Self::Nine => '9',
            Self::Zero => '0',
            Self::Pound => '#',
            Self::Star => '*',
        }
    }

    /// Low-row and high-column frequencies in Hz.
    pub fn frequencies(self) -> (u32, u32) {
        match self {
            Self::One => (697, 1209),
            Self::Two => (697, 1336),
            Self::Three => (697, 1477),
            Self::Four => (770, 1209),
            Self::Five => (770, 1336),
            Self::Six => (770, 1477),
            Self::Seven => (852, 1209),
            Self::Eight => (852, 1336),
            Self::Nine => (852, 1477),
            Self::Star => (941, 1209),
            Self::Zero => (941, 1336),
            Self::Pound => (941, 1477),
        }
    }

    fn wav(self) -> &'static [u8] {
        match self {
            Self::One => include_bytes!("../assets/dtmf-1.wav"),
            Self::Two => include_bytes!("../assets/dtmf-2.wav"),
            Self::Three => include_bytes!("../assets/dtmf-3.wav"),
            Self::Four => include_bytes!("../assets/dtmf-4.wav"),
            Self::Five => include_bytes!("../assets/dtmf-5.wav"),
            Self::Six => include_bytes!("../assets/dtmf-6.wav"),
            Self::Seven => include_bytes!("../assets/dtmf-7.wav"),
            Self::Eight => include_bytes!("../assets/dtmf-8.wav"),
            Self::Nine => include_bytes!("../assets/dtmf-9.wav"),
            Self::Zero => include_bytes!("../assets/dtmf-0.wav"),
            Self::Pound => include_bytes!("../assets/dtmf-pound.wav"),
            Self::Star => include_bytes!("../assets/dtmf-star.wav"),
        }
    }
}

impl fmt::Display for KeypadEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_char())
    }
}

impl TryFrom<char> for KeypadEntry {
    type Error = AudioError;
    fn try_from(value: char) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|key| key.as_char() == value)
            .ok_or(AudioError::InvalidKeypadEntry(value))
    }
}

impl FromStr for KeypadEntry {
    type Err = AudioError;
    fn from_str(value: &str) -> Result<Self> {
        let mut chars = value.chars();
        let first = chars.next().ok_or(AudioError::InvalidParameter(
            "DTMF entry must contain one character",
        ))?;
        if chars.next().is_some() {
            return Err(AudioError::InvalidParameter(
                "DTMF entry must contain one character",
            ));
        }
        Self::try_from(first)
    }
}

/// Load the original 500ms Pipecat DTMF resource. 8kHz output is byte-identical
/// to the bundled PCM. Other rates use this crate's sinc resampler, not SOXR.
pub fn load_dtmf_audio(button: KeypadEntry, sample_rate: u32) -> Result<Vec<u8>> {
    let wav = read_pcm16_wav(button.wav())?;
    resample_pcm16(
        wav.pcm,
        wav.format.sample_rate(),
        sample_rate,
        wav.format.channels(),
    )
}

/// Generate a rectangular-envelope, equal-amplitude pair of DTMF sine waves.
/// This is independent synthesis; use `load_dtmf_audio` for the original assets.
/// `amplitude` in 0..=1 bounds the combined peak relative to 32767.
pub fn generate_dtmf(
    button: KeypadEntry,
    sample_rate: u32,
    duration_ms: u32,
    amplitude: f64,
) -> Result<Vec<u8>> {
    PcmFormat::new(sample_rate, 1)?;
    if !amplitude.is_finite() || !(0.0..=1.0).contains(&amplitude) {
        return Err(AudioError::InvalidParameter(
            "DTMF amplitude must be finite and in 0..=1",
        ));
    }
    let (low, high) = button.frequencies();
    if sample_rate <= high * 2 {
        return Err(AudioError::InvalidParameter(
            "sample rate must exceed twice the highest DTMF frequency",
        ));
    }
    let frames = u64::from(sample_rate) * u64::from(duration_ms) / 1000;
    let bytes = usize::try_from(frames)
        .ok()
        .and_then(|n| n.checked_mul(2))
        .ok_or(AudioError::SizeOverflow)?;
    let mut pcm = Vec::new();
    pcm.try_reserve_exact(bytes)
        .map_err(|_| AudioError::SizeOverflow)?;
    for i in 0..frames {
        let t = i as f64 / f64::from(sample_rate);
        let value = ((TAU * f64::from(low) * t).sin() + (TAU * f64::from(high) * t).sin())
            * amplitude
            * 32767.0
            / 2.0;
        pcm.extend_from_slice(&(value.round() as i16).to_le_bytes());
    }
    Ok(pcm)
}
