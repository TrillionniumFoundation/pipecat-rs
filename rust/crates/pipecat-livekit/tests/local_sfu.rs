//! Opt-in real-SFU qualification. This test is compiled by default and executed
//! only with --ignored and PIPECAT_LIVEKIT_SERVER pointing to the pinned binary.
//! It never connects to an existing service or uses account credentials.
#[path = "support/loopback_ws.rs"]
mod loopback_ws;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use pipecat_audio::PcmFormat;
use pipecat_core::{AudioRawFrame, FrameData};
use pipecat_livekit::{ConnectionState, LiveKitConfig, LiveKitConnection, LiveKitEvent};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    net::{TcpListener, UdpSocket},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const DEADLINE: Duration = Duration::from_secs(10);
const BINARY_SHA256: &str = "0fbb1246f555fc3d27ae92530273f53d9463bbccd93c7edb398e0c6ccfe1536b";

struct Server {
    child: Child,
    directory: PathBuf,
    port: u16,
}
impl Server {
    async fn start() -> Self {
        let executable = std::env::var_os("PIPECAT_LIVEKIT_SERVER")
            .expect("set PIPECAT_LIVEKIT_SERVER to official Linux amd64 livekit-server 1.13.9");
        let binary = fs::read(&executable).expect("read pinned official server binary");
        assert_eq!(
            format!("{:x}", Sha256::digest(binary)),
            BINARY_SHA256,
            "fixture executes only the checksum-verified official Linux amd64 1.13.9 binary"
        );
        let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
        let rtc_udp = UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = tcp.local_addr().unwrap().port();
        let directory = std::env::temp_dir().join(format!(
            "pipecat-livekit-sfu-{}-{}",
            std::process::id(),
            now()
        ));
        fs::create_dir(&directory).unwrap();
        let config = directory.join("loopback.yaml");
        // RTC TCP mux ignores the signal-server bind address; keep it disabled.
        fs::write(&config, format!(
            "port: {port}\nbind_addresses: [127.0.0.1]\nrtc:\n  node_ip: 127.0.0.1\n  use_external_ip: false\n  enable_loopback_candidate: true\n  interfaces:\n    includes: [lo]\n  ips:\n    includes: [127.0.0.1/32]\n  tcp_port: 0\n  udp_port: {}\n  stun_servers: [127.0.0.1:{}]\nlogging:\n  level: info\n",
            rtc_udp.local_addr().unwrap().port(), rtc_udp.local_addr().unwrap().port()
        )).unwrap();
        let log = File::create(directory.join("server.log")).unwrap();
        drop((tcp, rtc_udp));
        let child = Command::new(executable)
            .args(["--dev", "--config"])
            .arg(config)
            // Exclude unrelated service/account configuration; proxy environment
            // and host network/security settings are deliberately unchanged.
            .env_remove("LIVEKIT_CONFIG")
            .env_remove("LIVEKIT_KEYS")
            .env_remove("REDIS_HOST")
            .env_remove("REDIS_PASSWORD")
            .env_remove("NODE_IP")
            .env_remove("UDP_PORT")
            .env_remove("TURN_CERT")
            .env_remove("TURN_KEY")
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .expect("start local fixture server");
        let mut server = Self {
            child,
            directory,
            port,
        };
        timeout(DEADLINE, async {
            loop {
                if let Some(status) = server.child.try_wait().unwrap() {
                    panic!(
                        "local SFU exited {status}: {}",
                        fs::read_to_string(server.directory.join("server.log")).unwrap()
                    );
                }
                if let Ok(mut stream) = tokio::net::TcpStream::connect(("127.0.0.1", port)).await {
                    stream
                        .write_all(
                            b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .unwrap();
                    let mut response = [0u8; 256];
                    let size = timeout(Duration::from_secs(1), stream.read(&mut response))
                        .await
                        .unwrap()
                        .unwrap();
                    if response[..size].starts_with(b"HTTP/1.1 200") {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("bounded local SFU startup");
        server
    }
    fn url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = fs::remove_dir_all(&self.directory);
    }
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Public LiveKit dev-mode fixture secret, room-scoped and expired after 2 min.
/// No token or real credential is read, logged or persisted.
fn token(identity: &str, room: &str) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&serde_json::json!({
        "iss": "devkey", "sub": identity, "nbf": now() - 1, "exp": now() + 120,
        "video": {"roomJoin": true, "room": room, "canPublish": true, "canSubscribe": true, "canPublishData": true}
    })).unwrap());
    let body = format!("{header}.{payload}");
    let mut hmac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    hmac.update(body.as_bytes());
    format!(
        "{body}.{}",
        URL_SAFE_NO_PAD.encode(hmac.finalize().into_bytes())
    )
}
async fn connect(
    server: &Server,
    room: &str,
    identity: &str,
    cancel: CancellationToken,
) -> LiveKitConnection {
    let connection = LiveKitConnection::connect(
        &server.url(),
        &token(identity, room),
        LiveKitConfig {
            input_format: PcmFormat::new(16000, 1).unwrap(),
            output_format: Some(PcmFormat::new(24000, 1).unwrap()),
            queue_capacity: 2048,
            connect_timeout: DEADLINE,
            operation_timeout: DEADLINE,
            close_timeout: Duration::from_secs(3),
            ..Default::default()
        },
        cancel,
    )
    .await
    .expect("real SFU join and audio publication");
    assert_eq!(connection.state(), ConnectionState::Connected);
    assert_eq!(connection.identity(), identity);
    assert_eq!(connection.room_name(), room);
    connection
}
async fn event(
    connection: &LiveKitConnection,
    matches: impl Fn(&LiveKitEvent) -> bool,
) -> LiveKitEvent {
    timeout(DEADLINE, async {
        loop {
            let event = connection.recv().await.expect("live event stream");
            if let LiveKitEvent::Error(error) = &event {
                panic!("adapter failure: {error}");
            }
            if matches(&event) {
                return event;
            }
        }
    })
    .await
    .expect("expected real SFU event within deadline")
}
async fn data(from: &LiveKitConnection, to: &LiveKitConnection, payload: &[u8], reliable: bool) {
    from.publish_data(
        payload.to_vec(),
        Some(&to.identity()),
        Some("local-qualification".into()),
        reliable,
    )
    .await
    .unwrap();
    let received = event(
        to,
        |e| matches!(e, LiveKitEvent::Data { payload: p, .. } if p == payload),
    )
    .await;
    if let LiveKitEvent::Data {
        payload: received,
        identity,
        topic,
        reliable: received_reliable,
    } = received
    {
        assert_eq!(received, payload);
        assert_eq!(identity.as_deref(), Some(from.identity().as_str()));
        assert_eq!(topic.as_deref(), Some("local-qualification"));
        assert_eq!(received_reliable, reliable);
    }
}
async fn audio(from: &LiveKitConnection, to: &LiveKitConnection) {
    let identity = from.identity();
    // A generated 440Hz tone passes through native Opus encode, SFU, native
    // decode and input resampling. Lossy encoding is not byte-exact PCM parity.
    let samples: Vec<u8> = (0..19_200)
        .flat_map(|i| {
            let value =
                (8_000.0 * (i as f64 * 440.0 * std::f64::consts::TAU / 24_000.0).sin()) as i16;
            value.to_le_bytes()
        })
        .collect();
    let frame = AudioRawFrame::new(samples, 24000, 1).unwrap();
    let send = from.publish_audio(&frame);
    let receive = async {
        let mut nonzero_frames = 0;
        while nonzero_frames < 10 {
            let received = event(
                to,
                |e| matches!(e, LiveKitEvent::Audio { identity: i, .. } if i == &identity),
            )
            .await;
            if let LiveKitEvent::Audio {
                track_sid, audio, ..
            } = &received
            {
                assert!(!track_sid.is_empty());
                assert_eq!(audio.sample_rate, 16000);
                assert_eq!(audio.num_channels, 1);
                assert!(!audio.audio.is_empty());
                audio.validate().unwrap();
                if audio
                    .audio
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .any(|b| i16::from_le_bytes([b[0], b[1]]).unsigned_abs() > 100)
                {
                    nonzero_frames += 1;
                }
            }
            let frame = received.into_frame().unwrap().unwrap();
            assert_eq!(frame.transport_source.as_deref(), Some(identity.as_str()));
            assert!(
                matches!(&frame.data, FrameData::UserAudio(audio) if audio.user_id == identity)
            );
            assert!(frame.metadata.contains_key("livekit.track_sid"));
        }
    };
    let (accepted, ()) = timeout(DEADLINE, async { tokio::join!(send, receive) })
        .await
        .unwrap();
    assert_eq!(accepted.unwrap().samples_per_channel, 19200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires official pinned LiveKit server and permission for local network interface enumeration"]
async fn native_participants_exchange_audio_data_cancel_leave_rejoin_and_release() {
    // Do not mint a token or install the injected client until startup succeeds.
    let server = Server::start().await;
    let client = Arc::new(loopback_ws::LoopbackClient::default());
    livekit_net::set_ws_client(client.clone());
    timeout(Duration::from_secs(60), async {
        let room = format!("pipecat-local-{}", now());
        let a_cancel = CancellationToken::new();
        let a = connect(&server, &room, "native-a", a_cancel.clone()).await;
        let b = connect(&server, &room, "native-b", CancellationToken::new()).await;
        event(
            &a,
            |e| matches!(e, LiveKitEvent::ParticipantConnected(p) if p.identity == "native-b"),
        )
        .await;
        assert!(b
            .participants()
            .iter()
            .any(|p| p.identity == "native-a" && p.sid != p.identity));
        event(&a, |e| matches!(e, LiveKitEvent::AudioSubscribed { identity, .. } if identity == "native-b")).await;
        event(&b, |e| matches!(e, LiveKitEvent::AudioSubscribed { identity, .. } if identity == "native-a")).await;
        data(&a, &b, b"reliable a-to-b", true).await;
        data(&b, &a, b"lossy b-to-a", false).await;
        audio(&a, &b).await;
        audio(&b, &a).await;
        a.interrupt_audio();
        a_cancel.cancel();
        timeout(DEADLINE, a.close()).await.unwrap().unwrap();
        assert_eq!(a.state(), ConnectionState::Closed);
        event(
            &b,
            |e| matches!(e, LiveKitEvent::ParticipantDisconnected(p) if p.identity == "native-a"),
        )
        .await;
        assert!(a.publish_data(vec![0], None, None, true).await.is_err());
        drop(a);
        let rejoined = connect(&server, &room, "native-a", CancellationToken::new()).await;
        event(
            &b,
            |e| matches!(e, LiveKitEvent::ParticipantConnected(p) if p.identity == "native-a"),
        )
        .await;
        data(&rejoined, &b, b"after same-identity rejoin", true).await;
        timeout(DEADLINE, rejoined.close()).await.unwrap().unwrap();
        timeout(DEADLINE, rejoined.close()).await.unwrap().unwrap();
        timeout(DEADLINE, b.close()).await.unwrap().unwrap();
        assert_eq!(rejoined.state(), ConnectionState::Closed);
        assert_eq!(b.state(), ConnectionState::Closed);
        drop((rejoined, b));
        client.assert_released().await;
    })
    .await
    .expect("bounded complete SFU qualification");
    let port = server.port;
    drop(server);
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err(),
        "owned fixture process must stop listening"
    );
}
