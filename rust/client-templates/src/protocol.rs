//! Wire-compatible subset of src/pipecat/frames/frames.proto, without native-core dependencies.
use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub struct Frame {
    #[prost(oneof = "frame::Body", tags = "1, 2, 3, 4, 5")]
    pub body: Option<frame::Body>,
}
pub mod frame {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Body {
        #[prost(message, tag = "1")]
        Text(super::Text),
        #[prost(message, tag = "2")]
        Audio(super::Audio),
        #[prost(message, tag = "3")]
        Transcription(super::Transcription),
        #[prost(message, tag = "4")]
        Message(super::JsonMessage),
        #[prost(message, tag = "5")]
        Interruption(super::Interruption),
    }
}
#[derive(Clone, PartialEq, Message)]
pub struct Text {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub text: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct Audio {
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
pub struct Transcription {
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
pub struct JsonMessage {
    #[prost(string, tag = "1")]
    pub data: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct Interruption {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub name: String,
}

pub const MAX_FRAME_BYTES: usize = 1 << 20;
pub fn decode(bytes: &[u8]) -> Result<Frame, String> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err("WebSocket frame exceeds 1 MiB".into());
    }
    let frame = Frame::decode(bytes).map_err(|_| "invalid protobuf frame")?;
    if frame.body.is_none() {
        return Err("protobuf frame lacks a body".into());
    }
    Ok(frame)
}
pub fn encode_message(message: &serde_json::Value) -> Vec<u8> {
    Frame {
        body: Some(frame::Body::Message(JsonMessage {
            data: message.to_string(),
        })),
    }
    .encode_to_vec()
}
pub fn encode_audio(samples: &[f32], rate: u32) -> Vec<u8> {
    let audio = samples
        .iter()
        .flat_map(|x| {
            let value = if x.is_finite() { x.clamp(-1., 1.) } else { 0. };
            ((value * if value < 0. { 32768. } else { 32767. }) as i16).to_le_bytes()
        })
        .collect();
    Frame {
        body: Some(frame::Body::Audio(Audio {
            id: 0,
            name: String::new(),
            audio,
            sample_rate: rate,
            num_channels: 1,
            pts: None,
        })),
    }
    .encode_to_vec()
}
pub fn audio_channels(audio: &Audio) -> Result<Vec<Vec<f32>>, String> {
    if !(8000..=96000).contains(&audio.sample_rate)
        || !(1..=2).contains(&audio.num_channels)
        || audio.audio.is_empty()
        || audio.audio.len() > MAX_FRAME_BYTES
        || !audio
            .audio
            .len()
            .is_multiple_of(audio.num_channels as usize * 2)
    {
        return Err("invalid PCM16 audio format".into());
    }
    let mut channels = vec![Vec::new(); audio.num_channels as usize];
    for (i, chunk) in audio.audio.as_chunks::<2>().0.iter().enumerate() {
        channels[i % audio.num_channels as usize]
            .push(i16::from_le_bytes([chunk[0], chunk[1]]) as f32 / 32768.);
    }
    Ok(channels)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn golden_protobuf_message() {
        assert_eq!(
            encode_message(&serde_json::json!({})),
            vec![0x22, 4, 0x0a, 2, b'{', b'}']
        );
        assert!(matches!(
            decode(&[0x2a, 0]).unwrap().body,
            Some(frame::Body::Interruption(_))
        ));
        assert!(decode(&[]).is_err());
        assert!(decode(&[0xff]).is_err());
    }
    #[test]
    fn pcm16_conversion() {
        let decoded = decode(&encode_audio(&[-1., 0., 1., f32::NAN], 16000)).unwrap();
        let Some(frame::Body::Audio(audio)) = decoded.body else {
            panic!()
        };
        assert_eq!(audio.audio, vec![0, 128, 0, 0, 255, 127, 0, 0]);
        assert_eq!(audio_channels(&audio).unwrap()[0][0], -1.);
        let mut invalid = audio;
        invalid.audio.push(0);
        assert!(audio_channels(&invalid).is_err());
    }
}
