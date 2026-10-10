use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    time::Duration,
};

use async_trait::async_trait;
use livekit_net::{Header, TransportError, WsClient, WsConnectResult, WsConnection};
use pipecat_livekit::{Error, LiveKitConfig, LiveKitConnection};
use tokio::{
    sync::{mpsc, Notify},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

struct Probe {
    connection: Weak<HalfOpenConnection>,
    reader_started: Arc<Notify>,
    signal_sent: Arc<Notify>,
    release_peer: CancellationToken,
    closed: Arc<AtomicBool>,
}
struct HalfOpenClient {
    opened: mpsc::UnboundedSender<Probe>,
    send_join: Arc<AtomicBool>,
}
struct HalfOpenConnection {
    reader_started: Arc<Notify>,
    signal_sent: Arc<Notify>,
    release_peer: CancellationToken,
    closed: Arc<AtomicBool>,
    pending_join: AtomicBool,
}
#[async_trait]
impl WsClient for HalfOpenClient {
    async fn connect(
        &self,
        _: String,
        _: Vec<Header>,
        _: u64,
    ) -> Result<WsConnectResult, TransportError> {
        let connection = Arc::new(HalfOpenConnection {
            reader_started: Arc::new(Notify::new()),
            signal_sent: Arc::new(Notify::new()),
            release_peer: CancellationToken::new(),
            closed: Arc::new(AtomicBool::new(false)),
            pending_join: AtomicBool::new(self.send_join.load(Ordering::SeqCst)),
        });
        self.opened
            .send(Probe {
                connection: Arc::downgrade(&connection),
                reader_started: connection.reader_started.clone(),
                signal_sent: connection.signal_sent.clone(),
                release_peer: connection.release_peer.clone(),
                closed: connection.closed.clone(),
            })
            .unwrap();
        Ok(WsConnectResult { connection })
    }
}
#[async_trait]
impl WsConnection for HalfOpenConnection {
    async fn send(&self, _: Vec<u8>) -> Result<(), TransportError> {
        self.signal_sent.notify_one();
        Ok(())
    }
    async fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
        if self.pending_join.swap(false, Ordering::SeqCst) {
            return Ok(Some(synthetic_join_response()));
        }
        self.reader_started.notify_one();
        self.release_peer.cancelled().await;
        Ok(None)
    }
    async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.release_peer.cancel();
    }
}

/// Minimal protobuf SignalResponse.Join: room 1, participant 2, ping timeout
/// 10 and ping interval 11. No SDP answer, ICE peer, authentication or SFU.
fn synthetic_join_response() -> Vec<u8> {
    fn field(number: u8, value: &[u8]) -> Vec<u8> {
        assert!(value.len() < 128);
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
    AbortCaller,
    Timeout,
    SdkTimeout,
}

/// Exercise real SDK task ownership through its official injected transport API.
/// The two stages are awaiting JoinResponse and awaiting RTC negotiation after a
/// synthetic JoinResponse. Neither establishes a real room join or SFU delivery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signaling_owners_release_on_cancel_abort_and_timeout() {
    let (opened, mut probes) = mpsc::unbounded_channel();
    let send_join = Arc::new(AtomicBool::new(false));
    livekit_net::set_ws_client(Arc::new(HalfOpenClient {
        opened,
        send_join: send_join.clone(),
    }));
    let mut failures = Vec::new();
    for after_join in [false, true] {
        send_join.store(after_join, Ordering::SeqCst);
        for stop in [
            Stop::Cancel,
            Stop::AbortCaller,
            Stop::Timeout,
            Stop::SdkTimeout,
        ] {
            if after_join && matches!(stop, Stop::SdkTimeout) {
                continue;
            }
            let cancel = CancellationToken::new();
            let trigger = cancel.clone();
            let connect = tokio::spawn(async move {
                LiveKitConnection::connect(
                    "ws://fixture.invalid",
                    "invalid-test-token",
                    LiveKitConfig {
                        connect_timeout: Duration::from_secs(if matches!(stop, Stop::SdkTimeout) {
                            20
                        } else {
                            2
                        }),
                        output_format: None,
                        ..Default::default()
                    },
                    cancel,
                )
                .await
            });
            let probe = timeout(Duration::from_secs(5), probes.recv())
                .await
                .unwrap()
                .unwrap();
            timeout(Duration::from_secs(1), probe.reader_started.notified())
                .await
                .unwrap();
            if after_join {
                timeout(Duration::from_secs(1), probe.signal_sent.notified())
                    .await
                    .expect("SDK must have consumed JoinResponse and started its signalling task");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            match stop {
                Stop::Cancel => {
                    trigger.cancel();
                    assert!(matches!(connect.await.unwrap(), Err(Error::Closed)));
                }
                Stop::AbortCaller => {
                    connect.abort();
                    assert!(connect.await.err().expect("aborted caller").is_cancelled());
                }
                Stop::Timeout => {
                    assert!(matches!(
                        timeout(Duration::from_secs(5), connect)
                            .await
                            .unwrap()
                            .unwrap(),
                        Err(Error::Timeout("room connect"))
                    ));
                }
                Stop::SdkTimeout => {
                    assert!(matches!(
                        timeout(Duration::from_secs(10), connect).await.unwrap().unwrap(),
                        Err(Error::Sdk(message)) if message.contains("failed to receive")
                    ));
                }
            }
            let released = timeout(Duration::from_secs(1), async {
                while probe.connection.upgrade().is_some() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .is_ok();
            if !released {
                failures.push(format!(
                    "{stop:?}, after JoinResponse={after_join}: retained transport, close called={}",
                    probe.closed.load(Ordering::SeqCst)
                ));
            }
            // Release the fixture peer even on failure, then verify test cleanup.
            probe.release_peer.cancel();
            timeout(Duration::from_secs(2), async {
                while probe.connection.upgrade().is_some() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("fixture cleanup must release its own transport");
        }
    }
    assert!(
        failures.is_empty(),
        "SDK signalling ownership failures: {failures:#?}"
    );
}
