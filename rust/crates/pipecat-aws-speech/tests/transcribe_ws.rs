use futures_util::{SinkExt, StreamExt};
use pipecat_aws_speech::*;
use pipecat_core::AudioRawFrame;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};
fn config() -> TranscribeConfig {
    let mut c = TranscribeConfig::new(
        AwsCredentials::new(
            "AKIDEXAMPLE",
            "SYNTHETICSECRET",
            Some("SESSIONTOKEN".into()),
        )
        .unwrap(),
        "us-east-1",
    );
    c.limits.response_timeout = Duration::from_millis(200);
    c.limits.close_timeout = Duration::from_millis(100);
    c.limits.write_timeout = Duration::from_millis(200);
    c
}
async fn make_listener() -> (TcpListener, TranscribeConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = config();
    c.endpoint = format!(
        "ws://{}/stream-transcription-websocket",
        listener.local_addr().unwrap()
    );
    (listener, c)
}
fn pcm(rate: u32, n: usize) -> AudioRawFrame {
    AudioRawFrame {
        audio: vec![0; n],
        sample_rate: rate,
        num_channels: 1,
        context_id: None,
    }
}
fn message(value: Value) -> Message {
    Message::Binary(
        eventstream::encode(
            &[
                (":message-type", "event"),
                (":event-type", "TranscriptEvent"),
                (":content-type", "application/json"),
            ],
            value.to_string().as_bytes(),
            1024 * 1024,
        )
        .unwrap()
        .into(),
    )
}
async fn audio(socket: &mut WebSocketStream<TcpStream>) -> Vec<u8> {
    let bytes = socket.next().await.unwrap().unwrap().into_data();
    let event = eventstream::decode(&bytes, 1024 * 1024).unwrap();
    assert_eq!(event.header(":event-type"), Some("AudioEvent"));
    event.payload.to_vec()
}
#[tokio::test]
// Tungstenite fixes the callback error type to its full HTTP response.
#[allow(clippy::result_large_err)]
async fn transcribe_real_handshake_pcm_partial_final_and_binary_finish() {
    let (listener, c) = make_listener().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |request: &Request, response: Response| {
                assert_eq!(request.uri().path(), "/stream-transcription-websocket");
                let query = request.uri().query().unwrap();
                assert!(query.contains("X-Amz-Signature="));
                assert!(query.contains("X-Amz-Credential=AKIDEXAMPLE%2F"));
                assert!(query.contains("language-code=en-US"));
                assert_eq!(request.headers()["origin"], "https://localhost");
                Ok(response)
            },
        )
        .await
        .unwrap();
        assert_eq!(audio(&mut socket).await.len(), 640);
        socket.send(message(json!({"Transcript":{"Results":[{"ResultId":"p","IsPartial":true,"Alternatives":[{"Transcript":"hello"}]},{"ResultId":"f","IsPartial":false,"Alternatives":[{"Transcript":"hello world"}]}]}}))).await.unwrap();
        assert!(audio(&mut socket).await.is_empty());
        socket.close(None).await.unwrap();
    });
    let mut session = TranscribeClient::new(c).unwrap().connect().await.unwrap();
    session.sender().send_audio(pcm(16000, 640)).await.unwrap();
    for (text, is_final) in [("hello", false), ("hello world", true)] {
        let TranscribeEvent::Transcript(t) = session.next().await.unwrap().unwrap();
        assert_eq!(t.text, text);
        assert_eq!(t.is_final, is_final);
        assert_eq!(t.language.as_deref(), Some("en-US"));
        assert!(t.result.get("ResultId").is_some());
    }
    session.sender().finish().await.unwrap();
    assert!(session.next().await.is_none());
    assert!(matches!(
        session.sender().try_send_audio(pcm(16000, 2)),
        Err(StreamingError::Closed)
    ));
    server.await.unwrap();
}
#[tokio::test]
// Tungstenite fixes the callback error type to its full HTTP response.
#[allow(clippy::result_large_err)]
async fn transcribe_resamples_actual_audio_to_signed_rate() {
    let (listener, mut c) = make_listener().await;
    c.sample_rate = 24000;
    let server = tokio::spawn(async move {
        let mut socket = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |request: &Request, response: Response| {
                assert!(request.uri().query().unwrap().contains("sample-rate=16000"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        let mut total = 0;
        loop {
            let bytes = audio(&mut socket).await;
            if bytes.is_empty() {
                break;
            }
            total += bytes.len();
            assert_eq!(bytes.len() % 2, 0);
        }
        assert_eq!(total, 3200);
        socket.close(None).await.unwrap();
    });
    let mut session = TranscribeClient::new(c).unwrap().connect().await.unwrap();
    session.sender().send_audio(pcm(24000, 4800)).await.unwrap();
    session.sender().finish().await.unwrap();
    assert!(session.next().await.is_none());
    server.await.unwrap();
}
#[tokio::test]
async fn transcribe_provider_exception_is_redacted_and_bad_crc_is_terminal() {
    for corrupt in [false, true] {
        let (listener, c) = make_listener().await;
        let server = tokio::spawn(async move {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let mut bytes = eventstream::encode(
                &[
                    (":message-type", "exception"),
                    (":exception-type", "UnrecognizedClientException"),
                    (":content-type", "application/json"),
                ],
                br#"{"Message":"SESSIONTOKEN SYNTHETICSECRET"}"#,
                1024,
            )
            .unwrap();
            if corrupt {
                bytes[8] ^= 1;
            }
            socket.send(Message::Binary(bytes.into())).await.unwrap();
            let _ = socket.next().await;
        });
        let mut session = TranscribeClient::new(c).unwrap().connect().await.unwrap();
        let error = session.next().await.unwrap().unwrap_err();
        assert!(!format!("{error:?}").contains("SECRET"));
        assert!(if corrupt {
            matches!(error, StreamingError::Protocol(_))
        } else {
            matches!(error, StreamingError::Provider { status: Some(403) })
        });
        assert!(session.next().await.is_none());
        server.await.unwrap();
    }
}
#[tokio::test]
async fn transcribe_finish_has_deadline_and_drop_sends_close() {
    let (listener, c) = make_listener().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(audio(&mut socket).await.is_empty());
        assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
    });
    let mut session = TranscribeClient::new(c).unwrap().connect().await.unwrap();
    session.sender().finish().await.unwrap();
    assert!(matches!(
        session.next().await,
        Some(Err(StreamingError::Timeout(_)))
    ));
    server.await.unwrap();
    let (listener, c) = make_listener().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
    });
    let session = TranscribeClient::new(c).unwrap().connect().await.unwrap();
    drop(session);
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn transcribe_rejects_mislabeled_audio_and_oversized_queues_without_sending() {
    let (listener, mut c) = make_listener().await;
    c.max_audio_chunk_bytes = 640;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(audio(&mut socket).await.is_empty());
        socket.close(None).await.unwrap();
    });
    let mut session = TranscribeClient::new(c).unwrap().connect().await.unwrap();
    assert!(session.sender().send_audio(pcm(24000, 100)).await.is_err());
    assert!(session.sender().send_audio(pcm(16000, 3)).await.is_err());
    assert!(session.sender().send_audio(pcm(16000, 642)).await.is_err());
    session.sender().finish().await.unwrap();
    assert!(session.next().await.is_none());
    server.await.unwrap();
}
