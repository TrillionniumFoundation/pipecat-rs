//! Real audio codecs. PCM is always little-endian signed 16-bit mono.
use bytes::Bytes;
use pipecat_core::AudioRawFrame;
use serde::{Deserialize, Serialize};
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters};

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AudioCodec {
    Opus,
    Pcmu,
    Pcma,
}

impl Default for AudioCodec {
    fn default() -> Self {
        if cfg!(feature = "opus") {
            Self::Opus
        } else {
            Self::Pcmu
        }
    }
}

impl AudioCodec {
    pub fn sample_rate(self) -> u32 {
        match self {
            Self::Opus => 48_000,
            Self::Pcmu | Self::Pcma => 8_000,
        }
    }

    pub fn mime_type(self) -> &'static str {
        match self {
            Self::Opus => "audio/opus",
            Self::Pcmu => "audio/PCMU",
            Self::Pcma => "audio/PCMA",
        }
    }

    pub fn available(self) -> bool {
        self != Self::Opus || cfg!(feature = "opus")
    }

    pub(crate) fn parameters(self) -> RTCRtpCodecParameters {
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: self.mime_type().into(),
                clock_rate: self.sample_rate(),
                // RFC 7587 requires opus/48000/2 in SDP, including mono streams.
                channels: if self == Self::Opus { 2 } else { 1 },
                sdp_fmtp_line: if self == Self::Opus {
                    "minptime=10;useinbandfec=0;stereo=0;sprop-stereo=0".into()
                } else {
                    String::new()
                },
                ..Default::default()
            },
            payload_type: match self {
                Self::Opus => 111,
                Self::Pcmu => 0,
                Self::Pcma => 8,
            },
            ..Default::default()
        }
    }

    pub(crate) fn validate_audio(self, audio: &AudioRawFrame) -> Result<()> {
        audio
            .validate()
            .map_err(|e| Error::InvalidAudio(e.to_string()))?;
        if audio.sample_rate != self.sample_rate() || audio.num_channels != 1 {
            return Err(Error::InvalidAudio(format!(
                "{} requires {} Hz mono PCM; resample explicitly",
                self.mime_type(),
                self.sample_rate()
            )));
        }
        // Fixed 20 ms packets keep pacing and G.711 MTU limits deterministic.
        if audio.num_frames() != self.sample_rate() as usize / 50 {
            return Err(Error::InvalidAudio(
                "send_audio requires exactly 20 ms of PCM".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) enum Encoder {
    Pcmu,
    Pcma,
    #[cfg(feature = "opus")]
    Opus(opus::Encoder),
}
impl Encoder {
    pub(crate) fn new(codec: AudioCodec) -> Result<Self> {
        match codec {
            AudioCodec::Pcmu => Ok(Self::Pcmu),
            AudioCodec::Pcma => Ok(Self::Pcma),
            AudioCodec::Opus => {
                #[cfg(feature = "opus")]
                {
                    Ok(Self::Opus(
                        opus::Encoder::new(48_000, opus::Channels::Mono, opus::Application::Voip)
                            .map_err(|e| Error::Codec(e.to_string()))?,
                    ))
                }
                #[cfg(not(feature = "opus"))]
                {
                    Err(Error::CodecUnavailable(AudioCodec::Opus))
                }
            }
        }
    }
    pub(crate) fn encode(&mut self, audio: &[u8]) -> Result<Bytes> {
        let data =
            match self {
                Self::Pcmu => pipecat_audio::pcm_to_ulaw(audio)
                    .map_err(|e| Error::InvalidAudio(e.to_string()))?,
                Self::Pcma => pipecat_audio::pcm_to_alaw(audio)
                    .map_err(|e| Error::InvalidAudio(e.to_string()))?,
                #[cfg(feature = "opus")]
                Self::Opus(encoder) => {
                    let samples = pipecat_audio::decode_pcm16_le(audio)
                        .map_err(|e| Error::InvalidAudio(e.to_string()))?;
                    encoder
                        .encode_vec(&samples, 1275)
                        .map_err(|e| Error::Codec(e.to_string()))?
                }
            };
        Ok(Bytes::from(data))
    }
}

pub(crate) enum Decoder {
    Pcmu,
    Pcma,
    #[cfg(feature = "opus")]
    Opus(opus::Decoder),
}
impl Decoder {
    pub(crate) fn new(codec: AudioCodec) -> Result<Self> {
        match codec {
            AudioCodec::Pcmu => Ok(Self::Pcmu),
            AudioCodec::Pcma => Ok(Self::Pcma),
            AudioCodec::Opus => {
                #[cfg(feature = "opus")]
                {
                    Ok(Self::Opus(
                        opus::Decoder::new(48_000, opus::Channels::Mono)
                            .map_err(|e| Error::Codec(e.to_string()))?,
                    ))
                }
                #[cfg(not(feature = "opus"))]
                {
                    Err(Error::CodecUnavailable(AudioCodec::Opus))
                }
            }
        }
    }
    pub(crate) fn decode(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        if payload.is_empty() {
            return Err(Error::InvalidAudio("empty RTP audio payload".into()));
        }
        match self {
            Self::Pcmu => Ok(pipecat_audio::ulaw_to_pcm(payload)),
            Self::Pcma => Ok(pipecat_audio::alaw_to_pcm(payload)),
            #[cfg(feature = "opus")]
            Self::Opus(decoder) => {
                let mut samples = vec![0i16; 5760]; // Maximum Opus packet duration: 120 ms.
                let count = decoder
                    .decode(payload, &mut samples, false)
                    .map_err(|e| Error::Codec(e.to_string()))?;
                Ok(pipecat_audio::encode_pcm16_le(&samples[..count]))
            }
        }
    }
}
