use pipecat_transports::{Packet, SessionConfig, TransportError, WebSocketSession};
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::test]
async fn adoption_requires_preconfigured_frame_limits_and_preserves_enforcement() {
    tokio::time::timeout(Duration::from_secs(5), async {
        for limit in [None, Some(2048), Some(1024)] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (socket, peer) = listener.accept().await.unwrap();
                assert!(peer.ip().is_loopback());
                let stream = tokio_tungstenite::accept_async_with_config(
                    socket,
                    Some(
                        SessionConfig {
                            max_message_bytes: 1024,
                            ..Default::default()
                        }
                        .websocket_config()
                        .unwrap()
                        .max_message_size(limit)
                        .max_frame_size(limit),
                    ),
                )
                .await
                .unwrap();
                let adopted = WebSocketSession::from_websocket(
                    stream,
                    SessionConfig {
                        max_message_bytes: 1024,
                        ..Default::default()
                    },
                );
                if limit != Some(1024) {
                    assert!(matches!(adopted, Err(TransportError::InvalidConfig)));
                } else {
                    let mut adopted = adopted.unwrap();
                    assert!(adopted.recv().await.is_none());
                    assert!(adopted.finish_and_join().await.is_err());
                }
            });
            let mut client = WebSocketSession::connect(&endpoint, SessionConfig::default())
                .await
                .unwrap();
            if limit == Some(1024) {
                client
                    .sender()
                    .send(Packet::Text("x".repeat(2048)))
                    .await
                    .unwrap();
            }
            assert!(client.recv().await.is_none());
            let _ = client.cancel_and_join().await;
            server.await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn valid_framing_cannot_mask_unbounded_or_oversized_write_buffers() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let config = SessionConfig {
            max_message_bytes: 1024,
            ..Default::default()
        };
        let bounded = config.websocket_config().unwrap();
        for max in [usize::MAX, usize::MAX - bounded.write_buffer_size] {
            assert!(SessionConfig {
                max_message_bytes: max,
                ..Default::default()
            }
            .websocket_config()
            .is_err());
        }
        for websocket_config in [
            bounded.max_write_buffer_size(usize::MAX),
            bounded.max_write_buffer_size(bounded.max_write_buffer_size + 1),
            bounded.write_buffer_size(bounded.write_buffer_size + 1),
        ] {
            assert_eq!(websocket_config.max_message_size, Some(1024));
            assert_eq!(websocket_config.max_frame_size, Some(1024));
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("ws://{}", listener.local_addr().unwrap());
            let config = config.clone();
            let server = tokio::spawn(async move {
                let (socket, peer) = listener.accept().await.unwrap();
                assert!(peer.ip().is_loopback());
                let stream =
                    tokio_tungstenite::accept_async_with_config(socket, Some(websocket_config))
                        .await
                        .unwrap();
                assert!(matches!(
                    WebSocketSession::from_websocket(stream, config),
                    Err(TransportError::InvalidConfig)
                ));
            });
            let mut client = WebSocketSession::connect(&endpoint, SessionConfig::default())
                .await
                .unwrap();
            assert!(client.recv().await.is_none());
            let _ = client.cancel_and_join().await;
            server.await.unwrap();
        }
    })
    .await
    .unwrap();
}
