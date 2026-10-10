//! Adapter-level regression for explicitly supplied aggregate spacing.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

async fn read(socket: &mut WebSocketStream<TcpStream>) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

#[tokio::test]
async fn spoken_and_skipped_prepared_anchors_keep_explicit_false_spacing() {
    for skipped in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = CartesiaConfig::new("loopback", "voice");
        config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
        let worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
            CartesiaClient::new(config).unwrap(),
        ))])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let server = tokio::spawn(async move {
            if skipped {
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), listener.accept())
                        .await
                        .is_err()
                );
                return;
            }
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let request = read(&mut socket).await;
            let context = &request["context_id"];
            assert_eq!(request["transcript"], "word");
            assert_eq!(read(&mut socket).await["continue"], false);
            for value in [
                json!({"type":"timestamps","context_id":context,"word_timestamps":{"words":["word"],"start":[0.0]}}),
                json!({"type":"chunk","context_id":context,"data":"AQACAA=="}),
                json!({"type":"done","context_id":context}),
            ] {
                socket
                    .send(Message::Text(value.to_string().into()))
                    .await
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(3), socket.next())
                .await
                .unwrap();
        });
        let mut input = TtsTextInputFrame::new("word");
        input.original_text = "<b>word</b>".into();
        input.includes_inter_frame_spaces = false;
        input.skip_tts = skipped;
        worker
            .sender()
            .send(
                Frame::new(FrameData::TtsTextInput(input)),
                Direction::Downstream,
            )
            .await
            .unwrap();
        worker.sender().end().await.unwrap();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let anchors: Vec<_> = frames
            .iter()
            .filter_map(|f| match &f.frame.data {
                FrameData::AggregatedText(t) => Some(t),
                _ => None,
            })
            .collect();
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].raw_text.as_deref(), Some("<b>word</b>"));
        assert_eq!(anchors[0].will_be_spoken, !skipped);
        assert!(!anchors[0].text.includes_inter_frame_spaces);
    }
}
