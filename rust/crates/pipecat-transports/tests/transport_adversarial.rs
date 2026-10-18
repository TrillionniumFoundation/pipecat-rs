use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use pipecat_transports::{Packet, SessionConfig, TransportError, WebSocketSession};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn active_receiver_survives_coalesced_burst() {
    tokio::time::timeout(Duration::from_secs(5), async {
        const COUNT: usize = 128;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            for i in 0..COUNT {
                websocket
                    .feed(Message::Text(i.to_string().into()))
                    .await
                    .unwrap();
            }
            websocket.flush().await.unwrap();
            let _ = websocket.next().await;
        });
        let mut session =
            WebSocketSession::connect(&format!("ws://{addr}"), SessionConfig::default())
                .await
                .unwrap();
        let mut received = Vec::new();
        while let Some(packet) = session.recv().await {
            received.push(packet);
            if received.len() == COUNT {
                break;
            }
        }
        let finish = session.finish().await;
        peer.await.unwrap();
        assert_eq!(
            received.len(),
            COUNT,
            "an actively draining receiver was disconnected: {finish:?}"
        );
        finish.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ping_is_answered_without_application_outbound_traffic() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            websocket
                .send(Message::Ping(Bytes::from_static(b"keep-alive")))
                .await
                .unwrap();
            assert_eq!(
                websocket.next().await.unwrap().unwrap(),
                Message::Pong(Bytes::from_static(b"keep-alive"))
            );
            websocket.close(None).await.unwrap();
            let _ = websocket.next().await;
        });
        let mut session =
            WebSocketSession::connect(&format!("ws://{addr}"), SessionConfig::default())
                .await
                .unwrap();
        assert_eq!(session.recv().await, None);
        session.finish().await.unwrap();
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn drop_closes_socket_and_invalidates_sender_clones() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            websocket.next().await
        });
        let session = WebSocketSession::connect(&format!("ws://{addr}"), SessionConfig::default())
            .await
            .unwrap();
        let sender = session.sender();
        drop(session);
        tokio::task::yield_now().await;
        assert!(matches!(
            sender.send(Packet::Text("late".into())).await,
            Err(TransportError::Closed)
        ));
        assert!(peer
            .await
            .unwrap()
            .is_none_or(|result| result.is_err() || matches!(result, Ok(Message::Close(_)))));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancel_interrupts_a_blocked_write_and_closes_sender_admission() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (release, hold) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let _ = hold.await;
            drop(websocket);
        });
        let config = SessionConfig {
            queue_capacity: 1,
            max_message_bytes: 32 * 1024 * 1024,
            write_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        let sender = session.sender();
        let payload = Bytes::from(vec![1u8; 16 * 1024 * 1024]);
        sender.send(Packet::Binary(payload.clone())).await.unwrap();
        sender.send(Packet::Binary(payload.clone())).await.unwrap();
        let third = sender.send(Packet::Binary(payload));
        tokio::pin!(third);
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut third)
            .await
            .is_err());
        tokio::time::timeout(Duration::from_millis(250), session.cancel())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(third.await, Err(TransportError::Closed)));
        let _ = release.send(());
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn write_timeout_is_reported_and_all_senders_are_closed() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (release, hold) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let _ = hold.await;
            drop(websocket);
        });
        let config = SessionConfig {
            max_message_bytes: 32 * 1024 * 1024,
            write_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let mut session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        let sender = session.sender();
        sender
            .send(Packet::Binary(Bytes::from(vec![1u8; 16 * 1024 * 1024])))
            .await
            .unwrap();
        assert_eq!(session.recv().await, None);
        assert!(matches!(
            session.finish().await,
            Err(TransportError::Timeout("write"))
        ));
        assert!(matches!(
            sender.send(Packet::Text("late".into())).await,
            Err(TransportError::Closed)
        ));
        let _ = release.send(());
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn inbound_limit_is_enforced_even_for_a_permissive_peer() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            websocket.send(Message::Text("12345".into())).await.unwrap();
            let _ = websocket.next().await;
        });
        let config = SessionConfig {
            max_message_bytes: 4,
            ..Default::default()
        };
        let mut session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        assert_eq!(session.recv().await, None);
        assert!(matches!(
            session.finish().await,
            Err(TransportError::WebSocket(
                tokio_tungstenite::tungstenite::Error::Capacity(_)
            ))
        ));
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn both_directions_progress_with_small_bounded_queues() {
    tokio::time::timeout(Duration::from_secs(5), async {
        const COUNT: usize = 256;
        async fn exchange(
            mut session: WebSocketSession,
            outgoing: &'static str,
            incoming: &'static str,
        ) {
            let sender = session.sender();
            let send = async {
                for i in 0..COUNT {
                    sender
                        .send(Packet::Text(format!("{outgoing}:{i}")))
                        .await
                        .unwrap();
                }
            };
            let recv = async {
                for i in 0..COUNT {
                    assert_eq!(
                        session.recv().await,
                        Some(Packet::Text(format!("{incoming}:{i}")))
                    );
                }
            };
            tokio::join!(send, recv);
            session.finish().await.unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = SessionConfig {
            queue_capacity: 4,
            ..Default::default()
        };
        let server_config = config.clone();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let session = WebSocketSession::accept(socket, server_config)
                .await
                .unwrap();
            exchange(session, "server", "client").await;
        });
        let session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        exchange(session, "client", "server").await;
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn peer_close_interrupts_a_blocked_write_within_close_timeout() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (close, ready_to_close) = tokio::sync::oneshot::channel();
        let (release, hold) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            ready_to_close.await.unwrap();
            websocket.close(None).await.unwrap();
            let _ = hold.await;
        });
        let config = SessionConfig {
            max_message_bytes: 32 * 1024 * 1024,
            write_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let mut session = WebSocketSession::connect(&format!("ws://{addr}"), config).await.unwrap();
        session.sender().send(Packet::Binary(Bytes::from(vec![1u8; 16 * 1024 * 1024]))).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        close.send(()).unwrap();
        assert_eq!(session.recv().await, None);
        let finish = tokio::time::timeout(Duration::from_millis(250), session.finish()).await;
        let _ = release.send(());
        peer.await.unwrap();
        assert!(finish.is_ok(), "peer close remained blocked behind the 30s write timeout instead of applying the 50ms close timeout");
    }).await.unwrap();
}

#[tokio::test]
async fn receiving_progresses_while_outgoing_socket_is_stalled() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (send, ready_to_send) = tokio::sync::oneshot::channel();
        let (release, hold) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            ready_to_send.await.unwrap();
            websocket
                .send(Message::Text("independent incoming data".into()))
                .await
                .unwrap();
            let _ = hold.await;
        });
        let config = SessionConfig {
            max_message_bytes: 32 * 1024 * 1024,
            write_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let mut session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        session
            .sender()
            .send(Packet::Binary(Bytes::from(vec![1u8; 16 * 1024 * 1024])))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        send.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(250), session.recv())
                .await
                .unwrap(),
            Some(Packet::Text("independent incoming data".into()))
        );
        session.cancel().await.unwrap();
        let _ = release.send(());
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancellation_does_not_wait_for_a_full_inbound_queue() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (sent, was_sent) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            for _ in 0..4 {
                websocket
                    .feed(Message::Text("inbound".into()))
                    .await
                    .unwrap();
            }
            websocket.flush().await.unwrap();
            sent.send(()).unwrap();
            let _ = websocket.next().await;
        });
        let config = SessionConfig {
            queue_capacity: 1,
            ..Default::default()
        };
        let session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        let sender = session.sender();
        was_sent.await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        tokio::time::timeout(Duration::from_millis(250), session.cancel())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            sender.send(Packet::Text("late".into())).await,
            Err(TransportError::Closed)
        ));
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_the_finish_future_aborts_its_owned_session_task() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (release, hold) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let _ = hold.await;
            drop(websocket);
        });
        let config = SessionConfig {
            queue_capacity: 1,
            max_message_bytes: 32 * 1024 * 1024,
            write_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        let sender = session.sender();
        sender
            .send(Packet::Binary(Bytes::from(vec![1u8; 16 * 1024 * 1024])))
            .await
            .unwrap();
        let finishing = tokio::spawn(session.finish());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!finishing.is_finished());
        finishing.abort();
        assert!(finishing.await.unwrap_err().is_cancelled());
        assert!(matches!(
            sender.send(Packet::Text("late".into())).await,
            Err(TransportError::Closed)
        ));
        let _ = release.send(());
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancel_interrupts_pong_flush_when_peer_never_reads() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (sent, was_sent) = tokio::sync::oneshot::channel();
        let (release, hold) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let ping = Bytes::from(vec![1u8; 125]);
            for _ in 0..60_000 {
                websocket.feed(Message::Ping(ping.clone())).await.unwrap();
            }
            websocket.flush().await.unwrap();
            sent.send(()).unwrap();
            let _ = hold.await;
        });
        let config = SessionConfig {
            max_message_bytes: 32 * 1024 * 1024,
            write_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        was_sent.await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let cancel = tokio::time::timeout(Duration::from_millis(250), session.cancel()).await;
        let _ = release.send(());
        peer.await.unwrap();
        assert!(
            cancel.is_ok(),
            "cancel waited behind the blocked pong flush and its 30s write timeout"
        );
        cancel.unwrap().unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn inbound_progresses_with_a_saturated_bounded_pong_buffer() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (release, hold) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let ping = Bytes::from(vec![1u8; 125]);
            for _ in 0..60_000 {
                websocket.feed(Message::Ping(ping.clone())).await.unwrap();
            }
            websocket
                .send(Message::Text("after ping flood".into()))
                .await
                .unwrap();
            let _ = hold.await;
        });
        let config = SessionConfig {
            queue_capacity: 1,
            max_message_bytes: 1024,
            write_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let mut session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        assert_eq!(
            session.recv().await,
            Some(Packet::Text("after ping flood".into()))
        );
        tokio::time::timeout(Duration::from_millis(250), session.cancel())
            .await
            .unwrap()
            .unwrap();
        let _ = release.send(());
        peer.await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn peer_close_interrupts_a_blocked_pong_flush() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (sent, was_sent) = tokio::sync::oneshot::channel();
        let (close, ready_to_close) = tokio::sync::oneshot::channel();
        let (release, hold) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let ping = Bytes::from(vec![1u8; 125]);
            for _ in 0..60_000 {
                websocket.feed(Message::Ping(ping.clone())).await.unwrap();
            }
            websocket.flush().await.unwrap();
            sent.send(()).unwrap();
            ready_to_close.await.unwrap();
            websocket.close(None).await.unwrap();
            let _ = hold.await;
        });
        let config = SessionConfig {
            max_message_bytes: 32 * 1024 * 1024,
            write_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_millis(50),
            ..Default::default()
        };
        let mut session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        was_sent.await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        close.send(()).unwrap();
        assert_eq!(session.recv().await, None);
        let finish = tokio::time::timeout(Duration::from_millis(250), session.finish()).await;
        let _ = release.send(());
        peer.await.unwrap();
        assert!(matches!(
            finish.unwrap(),
            Err(TransportError::Timeout("close"))
        ));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancelled_graceful_wait_escalates_and_joins_same_blocked_socket_owner() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (release, hold) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let websocket = tokio_tungstenite::accept_async(socket).await.unwrap();
            let _ = hold.await;
            drop(websocket);
        });
        let config = SessionConfig {
            queue_capacity: 1,
            max_message_bytes: 32 * 1024 * 1024,
            write_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let mut session = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        let sender = session.sender();
        let payload = Bytes::from(vec![1u8; 16 * 1024 * 1024]);
        sender.send(Packet::Binary(payload.clone())).await.unwrap();
        sender.send(Packet::Binary(payload.clone())).await.unwrap();
        let third = sender.send(Packet::Binary(payload));
        tokio::pin!(third);
        let blocked = tokio::time::timeout(Duration::from_millis(50), &mut third)
            .await
            .is_err();
        let timed_out = tokio::time::timeout(Duration::from_millis(50), session.finish_and_join())
            .await
            .is_err();
        let joined = session.cancel_and_join().await;
        let late = sender.send(Packet::Text("late".into())).await;
        let third_result = third.await;
        let repeated = session.finish_and_join().await;
        let _ = release.send(());
        peer.await.unwrap();
        assert!(blocked);
        assert!(timed_out, "the graceful wait must retain its held task");
        assert!(
            joined.is_ok(),
            "cancellation must actually await the same owner"
        );
        assert!(repeated.is_ok());
        assert!(matches!(late, Err(TransportError::Closed)));
        assert!(matches!(third_result, Err(TransportError::Closed)));
    })
    .await
    .unwrap();
}
