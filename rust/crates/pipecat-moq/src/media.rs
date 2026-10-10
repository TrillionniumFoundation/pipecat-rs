use crate::{AudioRawFrame, Error, Result};
use bytes::Bytes;

pub(crate) const RATE: u32 = 48_000;
pub(crate) const SAMPLES: usize = 960;
pub(crate) const MAX_PACKET: usize = 4096;

pub(crate) fn validate(audio: &AudioRawFrame) -> Result<()> {
    if audio.sample_rate != RATE || audio.num_channels != 1 || audio.audio.len() != SAMPLES * 2 {
        return Err(Error::InvalidAudio(
            "requires exactly 20 ms of 48000 Hz mono S16 little-endian PCM",
        ));
    }
    Ok(())
}
pub(crate) struct Encoder(opus::Encoder);
impl Encoder {
    pub fn new() -> Result<Self> {
        let mut encoder = opus::Encoder::new(RATE, opus::Channels::Mono, opus::Application::Voip)
            .map_err(Error::media)?;
        encoder
            .set_bitrate(opus::Bitrate::Bits(32_000))
            .map_err(Error::media)?;
        Ok(Self(encoder))
    }
    pub fn encode(&mut self, audio: &AudioRawFrame) -> Result<Bytes> {
        validate(audio)?;
        let samples: Vec<i16> = audio
            .audio
            .as_chunks::<2>()
            .0
            .iter()
            .map(|x| i16::from_le_bytes([x[0], x[1]]))
            .collect();
        let mut packet = [0u8; MAX_PACKET];
        let size = self.0.encode(&samples, &mut packet).map_err(Error::media)?;
        Ok(Bytes::copy_from_slice(&packet[..size]))
    }
}
pub(crate) struct Decoder {
    opus: opus::Decoder,
    channels: usize,
}
impl Decoder {
    pub fn new(config: &hang::catalog::AudioConfig) -> Result<Self> {
        if config.codec != hang::catalog::AudioCodec::Opus
            || config.sample_rate != RATE
            || !matches!(config.channel_count, 1 | 2)
            || config.broadcast.is_some()
            || config.container != hang::catalog::Container::Legacy
        {
            return Err(Error::Unsupported(
                "peer audio requires local legacy Opus/48000 with one or two channels",
            ));
        }
        let channels = config.channel_count as usize;
        let opus = opus::Decoder::new(
            RATE,
            if channels == 1 {
                opus::Channels::Mono
            } else {
                opus::Channels::Stereo
            },
        )
        .map_err(Error::media)?;
        Ok(Self { opus, channels })
    }
    pub fn decode(&mut self, packet: &[u8]) -> Result<AudioRawFrame> {
        if packet.is_empty() || packet.len() > MAX_PACKET {
            return Err(Error::Limit("Opus packet"));
        }
        // Opus permits at most 120 ms in a packet, including stereo.
        let mut decoded = vec![0i16; 5760 * self.channels];
        let frames = self
            .opus
            .decode(packet, &mut decoded, false)
            .map_err(Error::media)?;
        decoded.truncate(frames * self.channels);
        let mut pcm = Vec::with_capacity(frames * 2);
        for sample in decoded.chunks_exact(self.channels) {
            let mono = sample.iter().map(|x| i32::from(*x)).sum::<i32>() / self.channels as i32;
            pcm.extend_from_slice(&(mono as i16).to_le_bytes());
        }
        AudioRawFrame::new(pcm, RATE, 1).map_err(Error::media)
    }
}
