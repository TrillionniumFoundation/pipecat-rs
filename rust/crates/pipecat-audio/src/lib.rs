//! Native audio primitives for signed 16-bit little-endian PCM.
//!
//! Codec functions preserve sampling rate. [`SincResampler`] explicitly converts
//! rates with a windowed-sinc filter; it does not emulate SOXR. Audio energy
//! utilities do not perform speech recognition or neural voice activity detection.

pub mod dtmf;
pub mod g711;
pub mod onset;
pub mod pcm;
pub mod resample;
pub mod stream;
pub mod volume;
pub mod wav;

pub use dtmf::{generate_dtmf, load_dtmf_audio, KeypadEntry};
pub use g711::{
    alaw_to_pcm, decode_alaw, decode_ulaw, encode_alaw, encode_ulaw, pcm_to_alaw, pcm_to_ulaw,
    ulaw_to_pcm,
};
pub use onset::{detect_speech_onset, detect_speech_onset_with_config, SpeechOnsetConfig};
pub use pcm::{
    decode_pcm16_le, encode_pcm16_le, exp_smoothing, interleave_stereo_audio, is_silence,
    mix_audio, pcm_to_wav, scale_audio, PcmFormat, SPEAKING_THRESHOLD,
};
pub use resample::{resample_pcm16, SincResampler};
pub use stream::{PcmChunker, PcmStreamDecoder};
pub use volume::AudioVolumeTracker;
pub use wav::{read_pcm16_wav, PcmWav};

/// Invalid audio data or configuration. Errors never silently reinterpret channels.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum AudioError {
    #[error("sample rate must be positive")]
    InvalidSampleRate,
    #[error("channel count must be positive and fit a PCM16 WAV block alignment")]
    InvalidChannels,
    #[error("PCM byte length {length} is not a multiple of frame size {frame_bytes}")]
    IncompleteFrame { length: usize, frame_bytes: usize },
    #[error("audio buffer is empty")]
    EmptyAudio,
    #[error("{0}")]
    InvalidParameter(&'static str),
    #[error("audio size exceeds the supported range")]
    SizeOverflow,
    #[error("invalid DTMF keypad entry: {0}")]
    InvalidKeypadEntry(char),
}

pub type Result<T> = std::result::Result<T, AudioError>;
