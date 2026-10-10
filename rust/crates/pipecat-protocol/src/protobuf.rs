//! Field tags mirror `src/pipecat/frames/frames.proto`. No protoc or Python
//! runtime is needed to read and write these protobuf messages.
use crate::{check_size, ProtocolError, DEFAULT_MAX_MESSAGE_BYTES};
use prost::Message;
#[derive(Clone, PartialEq, Message)]
pub struct TextFrame {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub text: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct AudioRawFrame {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(bytes = "vec", tag = "3")]
    pub audio: Vec<u8>,
    #[prost(uint32, tag = "4")]
    pub sample_rate: u32,
    #[prost(uint32, tag = "5")]
    pub num_channels: u32,
    #[prost(uint64, optional, tag = "6")]
    pub pts: Option<u64>,
}
#[derive(Clone, PartialEq, Message)]
pub struct TranscriptionFrame {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub text: String,
    #[prost(string, tag = "4")]
    pub user_id: String,
    #[prost(string, tag = "5")]
    pub timestamp: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct MessageFrame {
    #[prost(string, tag = "1")]
    pub data: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct InterruptionFrame {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub name: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct Frame {
    #[prost(oneof = "frame::Kind", tags = "1,2,3,4,5")]
    pub frame: Option<frame::Kind>,
}
pub mod frame {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Text(super::TextFrame),
        #[prost(message, tag = "2")]
        Audio(super::AudioRawFrame),
        #[prost(message, tag = "3")]
        Transcription(super::TranscriptionFrame),
        #[prost(message, tag = "4")]
        Message(super::MessageFrame),
        #[prost(message, tag = "5")]
        Interruption(super::InterruptionFrame),
    }
}
impl Frame {
    pub fn parse(bytes: &[u8]) -> Result<Self, ProtocolError> {
        Self::parse_with_limit(bytes, DEFAULT_MAX_MESSAGE_BYTES)
    }
    pub fn parse_with_limit(bytes: &[u8], limit: usize) -> Result<Self, ProtocolError> {
        check_size(bytes.len(), limit)?;
        let frame = Self::decode(bytes)?;
        frame.validate()?;
        Ok(frame)
    }
    pub fn encode_checked(&self) -> Result<Vec<u8>, ProtocolError> {
        self.validate()?;
        check_size(self.encoded_len(), DEFAULT_MAX_MESSAGE_BYTES)?;
        Ok(self.encode_to_vec())
    }
    fn validate(&self) -> Result<(), ProtocolError> {
        match &self.frame {
            None => Err(ProtocolError::MissingField("protobuf frame oneof")),
            Some(frame::Kind::Message(message)) => {
                serde_json::from_str::<serde_json::Value>(&message.data)?;
                Ok(())
            }
            Some(frame::Kind::Audio(audio))
                if audio.sample_rate == 0
                    || audio.num_channels == 0
                    || !(audio.audio.len() as u64)
                        .is_multiple_of(u64::from(audio.num_channels) * 2) =>
            {
                Err(ProtocolError::InvalidField("protobuf PCM audio format"))
            }
            _ => Ok(()),
        }
    }
}
