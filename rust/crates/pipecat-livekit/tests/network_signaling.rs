//! Socket-level ownership regression. The peer completes a real WebSocket 101
//! handshake but never provides an SFU/ICE/DTLS session.
#[path = "support/loopback_ws.rs"]
mod loopback_ws;

use futures_util::{SinkExt, StreamExt};
use pipecat_livekit::{Error, LiveKitConfig, LiveKitConnection};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::tungstenite::{error::ProtocolError, Error as WsError, Message};
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};

fn synthetic_join_response() -> Vec<u8> {
    fn field(number: u8, value: &[u8]) -> Vec<u8> {
        let mut bytes = vec![(number << 3) | 2, value.len() as u8];
        bytes.extend_from_slice(value);
        bytes
    }
    let mut participant = field(1, b"PA_fixture");
    participant.extend(field(2, b"fixture"));
    let mut room = field(1, b"RM_fixture");
    room.extend(field(2, b"fixture"));
    let mut join = field(1, &room);
    join.extend(field(2, &participant));
    join.extend([10 << 3, 60, 11 << 3, 30]);
    field(1, &join)
}

#[derive(Clone, Copy, Debug)]
enum Stop {
    Cancel,
    Abort,
    Timeout,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_websocket_transport_releases_on_cancel_abort_and_timeout() {
    let client = Arc::new(loopback_ws::LoopbackClient::default());
    livekit_net::set_ws_client(client.clone());
    for after_join in [false, true] {
        for stop in [Stop::Cancel, Stop::Abort, Stop::Timeout] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (ready_tx, ready_rx) = oneshot::channel();
            let peer = AbortOnDropHandle::new(tokio::spawn(async move {
                let (stream, remote) = listener.accept().await.unwrap();
                assert!(remote.ip().is_loopback());
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                if after_join {
                    ws.send(Message::Binary(synthetic_join_response().into()))
                        .await
                        .unwrap();
                    timeout(Duration::from_secs(3), ws.next())
                        .await
                        .expect("SDK must start real signaling after JoinResponse")
                        .unwrap()
                        .unwrap();
                }
                let _ = ready_tx.send(());
                // A Close frame or EOF/reset is actual network teardown evidence.
                timeout(Duration::from_secs(5), async {
                    while let Some(message) = ws.next().await {
                        match message {
                            Ok(Message::Close(_)) => break,
                            Err(
                                WsError::ConnectionClosed
                                | WsError::AlreadyClosed
                                | WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake),
                            ) => break,
                            Err(WsError::Io(error))
                                if matches!(
                                    error.kind(),
                                    std::io::ErrorKind::UnexpectedEof
                                        | std::io::ErrorKind::ConnectionReset
                                        | std::io::ErrorKind::BrokenPipe
                                ) =>
                            {
                                break
                            }
                            Err(error) => panic!(
                                "unexpected WebSocket error is not teardown evidence: {error}"
                            ),
                            Ok(Message::Ping(data)) => {
                                let _ = ws.send(Message::Pong(data)).await;
                            }
                            _ => {}
                        }
                    }
                })
                .await
                .expect("fixture peer must observe client socket closing");
            }));
            let cancel = CancellationToken::new();
            let trigger = cancel.clone();
            let connect = AbortOnDropHandle::new(tokio::spawn(async move {
                LiveKitConnection::connect(
                    &format!("ws://{address}"),
                    "invalid-local-fixture-token",
                    LiveKitConfig {
                        output_format: None,
                        connect_timeout: Duration::from_secs(2),
                        ..Default::default()
                    },
                    cancel,
                )
                .await
            }));
            timeout(Duration::from_secs(3), ready_rx)
                .await
                .unwrap()
                .unwrap();
            match stop {
                Stop::Cancel => {
                    trigger.cancel();
                    assert!(matches!(connect.await.unwrap(), Err(Error::Closed)));
                }
                Stop::Abort => {
                    connect.abort();
                    assert!(connect.await.err().unwrap().is_cancelled());
                }
                Stop::Timeout => assert!(matches!(
                    timeout(Duration::from_secs(4), connect)
                        .await
                        .unwrap()
                        .unwrap(),
                    Err(Error::Timeout("room connect"))
                )),
            }
            client.assert_released().await;
            peer.await.unwrap();
            eprintln!("real WebSocket 101 + socket teardown: {stop:?}, synthetic JoinResponse={after_join}");
        }
    }
}
