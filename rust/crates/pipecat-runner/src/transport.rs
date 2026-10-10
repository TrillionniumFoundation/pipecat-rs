//! Transport-independent pipeline edges. WebRTC carries JSON and encoded RTP;
//! WebSocket retains the original protobuf framing.
use crate::{wire, RunnerError};
use pipecat_core::{Frame, FrameData, ProcessorError};
use pipecat_transports::{Packet, SessionSender, WebSocketSession};
use serde_json::Value;
use std::time::Duration;

pub enum BrowserSession {
    WebSocket(WebSocketSession),
    #[cfg(feature = "webrtc")]
    WebRtc(std::sync::Arc<pipecat_webrtc::SmallWebRtcConnection>),
}
impl BrowserSession {
    pub fn sender(&self) -> BrowserSender {
        match self {
            Self::WebSocket(session) => BrowserSender::WebSocket(session.sender()),
            #[cfg(feature = "webrtc")]
            Self::WebRtc(peer) => BrowserSender::WebRtc(std::sync::Arc::new(
                crate::webrtc::AudioOutput::new(peer.clone()),
            )),
        }
    }
    pub fn is_realtime_audio(&self) -> bool {
        match self {
            Self::WebSocket(_) => false,
            #[cfg(feature = "webrtc")]
            Self::WebRtc(_) => true,
        }
    }
    pub fn output_sample_rate(&self, provider_rate: u32) -> u32 {
        match self {
            Self::WebSocket(_) => provider_rate,
            #[cfg(feature = "webrtc")]
            Self::WebRtc(peer) => peer
                .config()
                .audio_codec
                .expect("runner audio codec")
                .sample_rate(),
        }
    }
    pub fn message_limit(&self) -> usize {
        match self {
            Self::WebSocket(_) => 128 * 1024,
            #[cfg(feature = "webrtc")]
            Self::WebRtc(peer) => peer.config().max_message_bytes,
        }
    }
    pub async fn recv_frame(&mut self) -> Option<Result<Frame, RunnerError>> {
        match self {
            Self::WebSocket(session) => session.recv().await.map(wire::decode),
            #[cfg(feature = "webrtc")]
            Self::WebRtc(peer) => loop {
                use pipecat_webrtc::TransportEvent;
                match peer.recv_event().await? {
                    TransportEvent::AppMessage(value) => {
                        break Some(wire::decode(Packet::Text(value.to_string())));
                    }
                    TransportEvent::Audio(audio) => break Some(Ok(audio.into_frame())),
                    TransportEvent::DataChannelClosed => break None,
                    TransportEvent::Error(_) => {
                        break Some(Err(RunnerError::Protocol("WebRTC media failed")));
                    }
                    _ => {}
                }
            },
        }
    }
    pub async fn finish(self) {
        match self {
            Self::WebSocket(session) => {
                let _ = session.finish().await;
            }
            #[cfg(feature = "webrtc")]
            Self::WebRtc(peer) => {
                let _ = peer.close().await;
            }
        }
    }
}
#[derive(Clone)]
pub enum BrowserSender {
    WebSocket(SessionSender),
    #[cfg(feature = "webrtc")]
    WebRtc(std::sync::Arc<crate::webrtc::AudioOutput>),
}
fn error(e: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::new(e.to_string())
}
async fn send(sender: &SessionSender, packet: Packet) -> Result<(), ProcessorError> {
    tokio::time::timeout(Duration::from_secs(5), sender.send(packet))
        .await
        .map_err(|_| error("browser output backpressure deadline"))?
        .map_err(error)
}
impl BrowserSender {
    pub async fn message(&self, value: Value) -> Result<(), ProcessorError> {
        match self {
            Self::WebSocket(sender) => send(sender, wire::message(value).map_err(error)?).await,
            #[cfg(feature = "webrtc")]
            Self::WebRtc(output) => {
                tokio::time::timeout(Duration::from_secs(5), output.peer.send_app_message(&value))
                    .await
                    .map_err(|_| error("WebRTC output backpressure deadline"))?
                    .map_err(error)
            }
        }
    }
    pub async fn frame(&self, frame: &Frame) -> Result<(), ProcessorError> {
        match self {
            Self::WebSocket(sender) => {
                let packet = match &frame.data {
                    FrameData::OutputTransportMessage(value)
                    | FrameData::OutputTransportMessageUrgent(value) => {
                        Some(wire::message(value.clone()).map_err(error)?)
                    }
                    FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) => {
                        Some(wire::audio(frame, audio).map_err(error)?)
                    }
                    FrameData::Interruption => Some(
                        wire::packet(pipecat_protocol::protobuf::frame::Kind::Interruption(
                            pipecat_protocol::protobuf::InterruptionFrame {
                                id: frame.id,
                                name: format!("InterruptionFrame#{}", frame.id),
                            },
                        ))
                        .map_err(error)?,
                    ),
                    _ => None,
                };
                if let Some(packet) = packet {
                    send(sender, packet).await?;
                }
                Ok(())
            }
            #[cfg(feature = "webrtc")]
            Self::WebRtc(output) => {
                tokio::time::timeout(Duration::from_secs(5), output.frame(frame))
                    .await
                    .map_err(|_| error("WebRTC output backpressure deadline"))?
            }
        }
    }
}
