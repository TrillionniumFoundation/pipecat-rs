//! Real loopback sockets through protobuf codecs and the native processor runtime.
//! This validates transport plumbing, not a live provider or microphone.
use pipecat::{core::*, transports::*, wire};
use std::{sync::Arc, time::Duration};
struct Echo;
#[async_trait]
impl FrameProcessor for Echo {
    fn name(&self) -> &str {
        "echo"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::InputAudio(audio) = frame.data {
            frame.data = FrameData::OutputAudio(audio);
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn native_audio_frames_roundtrip_over_websocket_pipeline() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut session = WebSocketSession::accept(socket, SessionConfig::default())
                .await
                .unwrap();
            let mut worker = Pipeline::new(vec![Arc::new(Echo)])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            let Packet::Binary(bytes) = session.recv().await.unwrap() else {
                panic!("binary protobuf required")
            };
            let decoded = wire::decode(&bytes).unwrap();
            assert!(matches!(decoded.data, FrameData::InputAudio(_)));
            worker
                .sender()
                .send(decoded, Direction::Downstream)
                .await
                .unwrap();
            while let Some(output) = worker.recv().await {
                if matches!(output.frame.data, FrameData::OutputAudio(_)) {
                    session
                        .sender()
                        .send(Packet::Binary(
                            wire::encode(&output.frame).unwrap().unwrap().into(),
                        ))
                        .await
                        .unwrap();
                    break;
                }
            }
            worker.end().await.unwrap();
            let (_, report) = worker.collect().await.unwrap();
            assert!(report.errors.is_empty());
            session.finish().await.unwrap();
        });
        let mut client =
            WebSocketSession::connect(&format!("ws://{addr}"), SessionConfig::default())
                .await
                .unwrap();
        let pcm = pipecat::audio::encode_pcm16_le(&[0, 1000, -1000, i16::MAX, i16::MIN]);
        let mut frame = Frame::new(FrameData::OutputAudio(
            AudioRawFrame::new(pcm.clone(), 16000, 1).unwrap(),
        ));
        frame.pts = Some(1234);
        client
            .sender()
            .send(Packet::Binary(
                wire::encode(&frame).unwrap().unwrap().into(),
            ))
            .await
            .unwrap();
        let Packet::Binary(bytes) = client.recv().await.unwrap() else {
            panic!("binary protobuf required")
        };
        let decoded = wire::decode(&bytes).unwrap();
        assert_eq!(decoded.id, frame.id);
        assert_eq!(decoded.pts, Some(1234));
        match decoded.data {
            FrameData::InputAudio(a) => assert_eq!(a.audio, pcm),
            _ => panic!("audio expected"),
        };
        client.finish().await.unwrap();
        server.await.unwrap();
    })
    .await
    .expect("pipeline must complete within deadline");
}
