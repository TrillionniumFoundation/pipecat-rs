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
    pub async fn finish(self) -> Result<(), RunnerError> {
        match self {
            Self::WebSocket(session) => session.finish().await.map_err(Into::into),
            #[cfg(feature = "webrtc")]
            Self::WebRtc(peer) => peer.close().await.map_err(Into::into),
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
    #[cfg(feature = "realtime")]
    pub async fn packet_fenced(
        &self,
        packet: Packet,
        fence: pipecat_core::InvocationFence,
    ) -> Result<(), ProcessorError> {
        match self {
            Self::WebSocket(sender) => {
                tokio::time::timeout(Duration::from_secs(5), sender.send_fenced(packet, fence))
                    .await
                    .map_err(|_| error("browser output backpressure deadline"))?
                    .map_err(error)
            }
            #[cfg(feature = "webrtc")]
            Self::WebRtc(_) => Err(error("realtime browser voice supports WebSocket only")),
        }
    }
    #[cfg(feature = "realtime")]
    pub async fn message_fenced(
        &self,
        value: Value,
        fence: pipecat_core::InvocationFence,
    ) -> Result<(), ProcessorError> {
        self.packet_fenced(wire::message(value).map_err(error)?, fence)
            .await
    }

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
        // Public frame bodies can change without consuming their native authority.
        if frame.native_eager_source().is_some() {
            return Err(error("unconsumed native eager source"));
        }
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

#[cfg(test)]
mod native_eager_source_boundary_tests {
    use super::*;
    use pipecat_core::{
        AudioRawFrame, Direction, EagerSourceSessionOwner, LlmContinuationProvider,
        NativeEagerConsumer, NativeEagerSource, NativeLlmContinuation, TranscriptionFrame,
    };
    use pipecat_protocol::protobuf::{self as pb, frame::Kind};
    use pipecat_transports::SessionConfig;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    async fn sessions() -> (WebSocketSession, WebSocketSession) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (client, server) = tokio::join!(
            WebSocketSession::connect(&url, SessionConfig::default()),
            async {
                let (stream, _) = listener.accept().await.unwrap();
                WebSocketSession::accept(stream, SessionConfig::default()).await
            }
        );
        (client.unwrap(), server.unwrap())
    }

    async fn finish(client: WebSocketSession, server: WebSocketSession) {
        let (client, server) = tokio::join!(client.finish(), server.finish());
        client.unwrap();
        server.unwrap();
    }

    fn expected_packet(frame: &Frame) -> Packet {
        match &frame.data {
            FrameData::OutputTransportMessage(value)
            | FrameData::OutputTransportMessageUrgent(value) => {
                wire::message(value.clone()).unwrap()
            }
            FrameData::OutputAudio(audio) | FrameData::TtsAudio(audio) => {
                wire::audio(frame, audio).unwrap()
            }
            FrameData::Interruption => wire::packet(Kind::Interruption(pb::InterruptionFrame {
                id: frame.id,
                name: format!("InterruptionFrame#{}", frame.id),
            }))
            .unwrap(),
            _ => panic!("expected an outgoing frame"),
        }
    }

    fn source_frames() -> (Vec<EagerSourceSessionOwner>, Vec<Frame>) {
        let owner = EagerSourceSessionOwner::new(4096).unwrap();
        let turn = owner.begin_turn().unwrap();
        let transcript = TranscriptionFrame {
            text: "native source text".into(),
            ..Default::default()
        };
        let prediction = owner
            .predict(&turn, FrameData::EagerTranscription(transcript.clone()))
            .unwrap();
        let commit = owner
            .offer_final(&turn, FrameData::Transcription(transcript))
            .unwrap();
        let empty_owner = EagerSourceSessionOwner::new(4096).unwrap();
        let empty_turn = empty_owner.begin_turn().unwrap();
        empty_owner.offer_empty_stop(&empty_turn).unwrap();
        let roles = [
            (
                FrameData::ProposedUserStoppedSpeaking,
                NativeEagerSource::Stop {
                    turn: empty_turn,
                    commit: None,
                },
            ),
            (
                FrameData::UserStartedSpeaking,
                NativeEagerSource::Start(turn.clone()),
            ),
            (
                prediction.payload().clone(),
                NativeEagerSource::Prediction(prediction.clone()),
            ),
            (
                FrameData::EagerEndOfTurnCancel,
                NativeEagerSource::Cancel(prediction),
            ),
            (
                commit.payload().clone(),
                NativeEagerSource::Final(commit.clone()),
            ),
            (
                FrameData::UserStoppedSpeaking,
                NativeEagerSource::Stop {
                    turn,
                    commit: Some(commit),
                },
            ),
        ];
        let frames = roles
            .into_iter()
            .map(|(data, source)| {
                let mut frame = Frame::new(data);
                frame.set_native_eager_source(source).unwrap();
                frame
            })
            .collect();
        (vec![owner, empty_owner], frames)
    }

    fn output_bodies() -> Vec<FrameData> {
        let value = serde_json::json!({"type": "server-message", "data": "visible"});
        let audio = AudioRawFrame::new(vec![0; 4], 16000, 1).unwrap();
        vec![
            FrameData::OutputTransportMessage(value.clone()),
            FrameData::OutputTransportMessageUrgent(value),
            FrameData::Interruption,
            FrameData::OutputAudio(audio.clone()),
            FrameData::TtsAudio(audio),
        ]
    }

    #[tokio::test]
    async fn all_source_roles_reject_original_and_mutated_bodies_before_packet_conversion() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (client, mut server) = sessions().await;
            let sender = BrowserSender::WebSocket(client.sender());
            let (_owner, frames) = source_frames();
            for frame in frames {
                assert_eq!(
                    sender.frame(&frame).await.unwrap_err().message,
                    "unconsumed native eager source"
                );
                for body in output_bodies() {
                    let mut mutated = frame.clone();
                    mutated.data = body;
                    // Audio would otherwise fail its timing check during conversion.
                    mutated.set_pipeline_pts(100);
                    assert_eq!(
                        sender.frame(&mutated).await.unwrap_err().message,
                        "unconsumed native eager source"
                    );
                    assert!(mutated.native_eager_source().is_some());
                    assert!(mutated
                        .take_native_eager_source(Direction::Downstream)
                        .is_err());
                }
            }
            let sentinel = Frame::new(FrameData::OutputTransportMessage(
                serde_json::json!({"type": "server-message", "data": "sentinel"}),
            ));
            sender.frame(&sentinel).await.unwrap();
            // A same-sender FIFO sentinel witnesses that every rejected frame sent nothing.
            assert_eq!(server.recv().await, Some(expected_packet(&sentinel)));
            drop(sender);
            finish(client, server).await;
        })
        .await
        .expect("native eager source boundary test timed out");
    }

    #[tokio::test]
    async fn legitimately_consumed_final_and_stop_allow_ordinary_packets() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (client, mut server) = sessions().await;
            let sender = BrowserSender::WebSocket(client.sender());
            let (_owner, mut frames) = source_frames();
            let consumer = NativeEagerConsumer::new();
            let mut stop = frames.pop().unwrap();
            let mut final_frame = frames.pop().unwrap();
            let NativeEagerSource::Final(commit) = final_frame
                .take_native_eager_source(Direction::Downstream)
                .unwrap()
                .unwrap()
            else {
                panic!("expected final source receipt");
            };
            let NativeEagerSource::Stop {
                turn,
                commit: Some(stop_commit),
            } = stop
                .take_native_eager_source(Direction::Downstream)
                .unwrap()
                .unwrap()
            else {
                panic!("expected stop source receipt");
            };
            assert_eq!(commit, stop_commit);
            assert!(turn.claim_start(&consumer));
            assert!(commit.claim_final(&consumer));
            assert!(stop_commit.claim_stop(&consumer));
            assert!(commit.consume_fallback(&consumer));
            for mut frame in [final_frame, stop] {
                assert!(frame.native_eager_source().is_none());
                sender.frame(&frame).await.unwrap();
                for body in output_bodies() {
                    frame.data = body;
                    sender.frame(&frame).await.unwrap();
                    assert_eq!(server.recv().await, Some(expected_packet(&frame)));
                }
            }
            drop(sender);
            finish(client, server).await;
        })
        .await
        .expect("consumed native source control timed out");
    }

    #[tokio::test]
    async fn ordinary_frames_keep_existing_packet_selection() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (client, mut server) = sessions().await;
            let sender = BrowserSender::WebSocket(client.sender());
            sender
                .frame(&Frame::new(FrameData::UserStoppedSpeaking))
                .await
                .unwrap();
            for body in output_bodies() {
                let frame = Frame::new(body);
                sender.frame(&frame).await.unwrap();
                assert_eq!(server.recv().await, Some(expected_packet(&frame)));
            }
            drop(sender);
            finish(client, server).await;
        })
        .await
        .expect("ordinary browser packet control timed out");
    }

    #[tokio::test]
    async fn source_guard_does_not_change_unrelated_native_history_behavior() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (client, mut server) = sessions().await;
            let sender = BrowserSender::WebSocket(client.sender());
            let mut frame = Frame::new(FrameData::LlmResponseEnd);
            frame
                .set_native_llm_continuation(Arc::new(
                    NativeLlmContinuation::new(1, LlmContinuationProvider::Anthropic, vec![])
                        .unwrap(),
                ))
                .unwrap();
            sender.frame(&frame).await.unwrap();
            for body in output_bodies() {
                frame.data = body;
                let result = sender.frame(&frame).await;
                if matches!(
                    frame.data,
                    FrameData::OutputAudio(_) | FrameData::TtsAudio(_)
                ) {
                    assert!(result.is_err());
                } else {
                    result.unwrap();
                    assert_eq!(server.recv().await, Some(expected_packet(&frame)));
                }
            }
            drop(sender);
            finish(client, server).await;
        })
        .await
        .expect("unrelated native history control timed out");
    }
}
// Append as a distinct test-only module to the frozen review tree's transport.rs.
// Calls the actual BrowserSender::frame with native loopback WebSocket sessions.
#[cfg(test)]
mod independent_source_boundary_review {
    use super::*;
    use pipecat_core::{EagerSourceSessionOwner, NativeEagerSource, TranscriptionFrame};
    use pipecat_protocol::protobuf::{self as pb, frame::Kind};
    use pipecat_transports::SessionConfig;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn review_sender_rejects_carriers_before_body_dispatch_and_keeps_ordinary_packets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            WebSocketSession::accept(socket, SessionConfig::default())
                .await
                .unwrap()
        };
        let client = WebSocketSession::connect(&url, SessionConfig::default());
        let (server, client) = tokio::join!(server, client);
        let mut client = client.unwrap();
        let sender = BrowserSender::WebSocket(server.sender());
        let source = EagerSourceSessionOwner::new(4096).unwrap();
        let turn = source.begin_turn().unwrap();
        let prediction = source
            .predict(
                &turn,
                FrameData::EagerTranscription(TranscriptionFrame {
                    text: "scoped".into(),
                    ..Default::default()
                }),
            )
            .unwrap();
        let receipt = source
            .offer_final(
                &turn,
                FrameData::Transcription(TranscriptionFrame {
                    text: "scoped".into(),
                    finalized: true,
                    ..Default::default()
                }),
            )
            .unwrap();
        let empty_source = EagerSourceSessionOwner::new(4096).unwrap();
        let empty_turn = empty_source.begin_turn().unwrap();
        empty_source.offer_empty_stop(&empty_turn).unwrap();
        for (payload, authority) in [
            (
                FrameData::ProposedUserStartedSpeaking,
                NativeEagerSource::Start(turn.clone()),
            ),
            (
                FrameData::UserStartedSpeaking,
                NativeEagerSource::Start(turn.clone()),
            ),
            (
                FrameData::ProposedUserStoppedSpeaking,
                NativeEagerSource::Stop {
                    turn: empty_turn,
                    commit: None,
                },
            ),
            (
                prediction.payload().clone(),
                NativeEagerSource::Prediction(prediction.clone()),
            ),
            (
                FrameData::EagerEndOfTurnCancel,
                NativeEagerSource::Cancel(prediction),
            ),
            (
                receipt.payload().clone(),
                NativeEagerSource::Final(receipt.clone()),
            ),
            (
                FrameData::UserStoppedSpeaking,
                NativeEagerSource::Stop {
                    turn: turn.clone(),
                    commit: Some(receipt.clone()),
                },
            ),
            (
                FrameData::ProposedUserStoppedSpeaking,
                NativeEagerSource::Stop {
                    turn: turn.clone(),
                    commit: Some(receipt),
                },
            ),
        ] {
            let mut guarded = Frame::new(payload);
            guarded.set_native_eager_source(authority).unwrap();
            assert!(
                sender.frame(&guarded).await.is_err(),
                "ignored source role was not rejected"
            );
            for body in [
                FrameData::OutputTransportMessage(serde_json::json!({"review": "ordinary"})),
                FrameData::OutputTransportMessageUrgent(serde_json::json!({"review": "urgent"})),
                FrameData::Interruption,
            ] {
                let mut mutated = guarded.clone();
                mutated.data = body.clone();
                assert!(
                    sender.frame(&mutated).await.is_err(),
                    "manual packet branch shed source authority"
                );
                let sentinel = serde_json::json!({"review_sentinel": true});
                sender.message(sentinel.clone()).await.unwrap();
                let observed = tokio::time::timeout(Duration::from_secs(2), client.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    observed,
                    wire::message(sentinel).unwrap(),
                    "rejected carrier emitted before FIFO sentinel"
                );
                sender.frame(&Frame::new(body.clone())).await.unwrap();
                let packet = tokio::time::timeout(Duration::from_secs(2), client.recv())
                    .await
                    .unwrap()
                    .unwrap();
                let Packet::Binary(bytes) = packet else {
                    panic!("expected protobuf packet")
                };
                let kind = pb::Frame::parse(&bytes).unwrap().frame.unwrap();
                match body {
                    FrameData::Interruption => assert!(matches!(kind, Kind::Interruption(_))),
                    FrameData::OutputTransportMessage(expected)
                    | FrameData::OutputTransportMessageUrgent(expected) => {
                        let Kind::Message(message) = kind else {
                            panic!("expected message packet")
                        };
                        assert_eq!(
                            serde_json::from_str::<Value>(&message.data).unwrap(),
                            expected
                        );
                    }
                    _ => unreachable!(),
                }
            }
        }
        drop(sender);
        let (left, right) = tokio::join!(server.finish(), client.finish());
        left.unwrap();
        right.unwrap();
    }
}

#[cfg(test)]
#[path = "transport_cleanup_tests.rs"]
mod cleanup_tests;
