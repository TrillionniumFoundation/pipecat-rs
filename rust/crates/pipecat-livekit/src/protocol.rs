use std::borrow::Cow;

use livekit::webrtc::audio_frame::AudioFrame;
use pipecat_audio::{decode_pcm16_le, encode_pcm16_le, PcmFormat};
use pipecat_core::{AudioRawFrame, Frame, FrameData, KeypadEntry, UserAudioRawFrame};
use serde_json::Value;

use crate::{Error, Result};

/// Room identity and participant SID are deliberately separate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParticipantInfo {
    pub identity: String,
    pub sid: String,
    pub name: String,
    pub metadata: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Reconnecting,
    Closed,
    Failed(String),
}

#[derive(Debug, Clone)]
pub enum LiveKitEvent {
    Connected {
        identity: String,
        room: String,
    },
    ParticipantConnected(ParticipantInfo),
    ParticipantActive(ParticipantInfo),
    ParticipantDisconnected(ParticipantInfo),
    AudioSubscribed {
        identity: String,
        track_sid: String,
    },
    AudioUnsubscribed {
        identity: String,
        track_sid: String,
    },
    Audio {
        identity: String,
        track_sid: String,
        audio: AudioRawFrame,
    },
    Video {
        identity: String,
        track_sid: String,
        source: crate::VideoSource,
        image: pipecat_core::ImageRawFrame,
    },
    Data {
        payload: Vec<u8>,
        identity: Option<String>,
        topic: Option<String>,
        reliable: bool,
    },
    Dtmf {
        digit: KeypadEntry,
        identity: Option<String>,
    },
    ActiveSpeakers(Vec<String>),
    Reconnecting,
    Reconnected,
    Disconnected {
        reason: String,
    },
    Error(String),
}
impl LiveKitEvent {
    /// Convert native events into pipeline frames without rewriting participant identities.
    /// Binary data remains available on this event API; pipeline messages must be JSON.
    pub fn into_frame(self) -> Result<Option<Frame>> {
        let mut source = None;
        let mut metadata = Vec::new();
        let data = match self {
            Self::Connected { identity, room } => {
                metadata.push(("livekit.identity", Value::String(identity)));
                metadata.push(("livekit.room", Value::String(room)));
                FrameData::BotConnected
            }
            Self::ParticipantConnected(info) => {
                source = Some(info.identity.clone());
                metadata.push(("livekit.participant_sid", Value::String(info.sid)));
                FrameData::ClientConnected
            }
            Self::Audio {
                identity,
                track_sid,
                audio,
            } => {
                source = Some(identity.clone());
                metadata.push(("livekit.track_sid", Value::String(track_sid)));
                FrameData::UserAudio(UserAudioRawFrame {
                    audio,
                    user_id: identity,
                })
            }
            Self::Video {
                identity,
                track_sid,
                source: video_source,
                image,
            } => {
                source = Some(video_source.as_str().into());
                metadata.push(("livekit.track_sid", Value::String(track_sid)));
                FrameData::UserImage(pipecat_core::UserImageRawFrame {
                    image,
                    user_id: identity,
                    text: None,
                    append_to_context: None,
                    request: None,
                    run_llm: None,
                })
            }
            Self::Data {
                payload,
                identity,
                topic,
                reliable,
            } => {
                source = identity;
                if let Some(topic) = topic {
                    metadata.push(("livekit.topic", Value::String(topic)));
                }
                metadata.push(("livekit.reliable", Value::Bool(reliable)));
                FrameData::InputTransportMessage(serde_json::from_slice(&payload)?)
            }
            Self::Dtmf { digit, identity } => {
                source = identity;
                FrameData::InputDtmf(digit)
            }
            Self::Error(message) => FrameData::Error(pipecat_core::ErrorFrame::new(message)),
            _ => return Ok(None),
        };
        let mut frame = Frame::new(data);
        frame.transport_source = source;
        for (key, value) in metadata {
            frame.metadata.insert(key.into(), value);
        }
        Ok(Some(frame))
    }
}

/// This is admission to the native source, never proof that any participant heard audio.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceAccepted {
    pub samples_per_channel: u32,
}

pub(crate) fn to_sdk_audio(
    audio: &AudioRawFrame,
    format: PcmFormat,
    limit: usize,
) -> Result<AudioFrame<'static>> {
    audio.validate().map_err(|e| Error::Audio(e.to_string()))?;
    if audio.sample_rate != format.sample_rate() || audio.num_channels != format.channels() {
        return Err(Error::Audio(
            "PCM sample rate and channels must match the configured output source".into(),
        ));
    }
    if audio.audio.len() > limit {
        return Err(Error::Limit("audio frame bytes"));
    }
    let samples_per_channel =
        u32::try_from(audio.num_frames()).map_err(|_| Error::Limit("audio sample count"))?;
    let data = decode_pcm16_le(&audio.audio).map_err(|e| Error::Audio(e.to_string()))?;
    Ok(AudioFrame {
        data: Cow::Owned(data),
        sample_rate: audio.sample_rate,
        num_channels: audio.num_channels.into(),
        samples_per_channel,
    })
}

pub(crate) fn from_sdk_audio(audio: AudioFrame<'_>, limit: usize) -> Result<AudioRawFrame> {
    let channels = u16::try_from(audio.num_channels)
        .map_err(|_| Error::Audio("invalid native channel count".into()))?;
    if channels == 0
        || audio.sample_rate == 0
        || audio.data.len() != audio.samples_per_channel as usize * channels as usize
    {
        return Err(Error::Audio(
            "native PCM sample count does not match channels".into(),
        ));
    }
    if audio.data.len() > limit / 2 {
        return Err(Error::Limit("audio frame bytes"));
    }
    AudioRawFrame::new(encode_pcm16_le(&audio.data), audio.sample_rate, channels)
        .map_err(|e| Error::Audio(e.to_string()))
}

pub(crate) fn dtmf_code(digit: KeypadEntry) -> u32 {
    match digit.as_char() {
        '0'..='9' => digit.as_char() as u32 - '0' as u32,
        '*' => 10,
        '#' => 11,
        'A' => 12,
        'B' => 13,
        'C' => 14,
        'D' => 15,
        _ => unreachable!("KeypadEntry is exhaustive"),
    }
}
pub(crate) fn dtmf_digit(code: u32) -> Option<KeypadEntry> {
    "0123456789*#ABCD"
        .chars()
        .nth(code as usize)
        .and_then(|c| KeypadEntry::try_from(c).ok())
}

pub(crate) fn validate_event(event: &LiveKitEvent) -> Result<()> {
    let text_size = match event {
        LiveKitEvent::Connected { identity, room } => identity.len().saturating_add(room.len()),
        LiveKitEvent::ParticipantConnected(p)
        | LiveKitEvent::ParticipantActive(p)
        | LiveKitEvent::ParticipantDisconnected(p) => p
            .identity
            .len()
            .saturating_add(p.sid.len())
            .saturating_add(p.name.len())
            .saturating_add(p.metadata.len()),
        LiveKitEvent::Video {
            identity,
            track_sid,
            ..
        }
        | LiveKitEvent::AudioSubscribed {
            identity,
            track_sid,
        }
        | LiveKitEvent::AudioUnsubscribed {
            identity,
            track_sid,
        }
        | LiveKitEvent::Audio {
            identity,
            track_sid,
            ..
        } => identity.len().saturating_add(track_sid.len()),
        LiveKitEvent::Data {
            identity, topic, ..
        } => identity
            .as_ref()
            .map_or(0, String::len)
            .saturating_add(topic.as_ref().map_or(0, String::len)),
        LiveKitEvent::Dtmf { identity, .. } => identity.as_ref().map_or(0, String::len),
        LiveKitEvent::ActiveSpeakers(speakers) => speakers
            .iter()
            .fold(0usize, |size, s| size.saturating_add(s.len())),
        LiveKitEvent::Disconnected { reason } | LiveKitEvent::Error(reason) => reason.len(),
        _ => 0,
    };
    if text_size > 65_536 {
        return Err(Error::Limit("event metadata bytes"));
    }
    Ok(())
}

pub(crate) fn bounded_json(value: &impl serde::Serialize, limit: usize) -> Result<Vec<u8>> {
    struct Buffer {
        bytes: Vec<u8>,
        limit: usize,
        exceeded: bool,
    }
    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                self.exceeded = true;
                return Err(std::io::Error::other("JSON exceeds configured byte limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut buffer = Buffer {
        bytes: Vec::new(),
        limit,
        exceeded: false,
    };
    let result = serde_json::to_writer(&mut buffer, value);
    if buffer.exceeded {
        return Err(Error::Limit("serialized JSON bytes"));
    }
    result?;
    Ok(buffer.bytes)
}
