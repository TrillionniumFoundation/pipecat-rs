use super::*;
use bytes::Bytes;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinSet;

const DEADLINE: Duration = Duration::from_secs(10);
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future)
        .await
        .expect("test deadline")
}
fn audio() -> AudioRawFrame {
    let pcm: Vec<u8> = (0..960)
        .flat_map(|n| (((n % 80) as i16 - 40) * 200).to_le_bytes())
        .collect();
    AudioRawFrame::new(pcm, 48_000, 1).unwrap()
}
fn compressed() -> moq_json::stream::Config {
    let mut config = moq_json::stream::Config::default();
    config.compression = moq_json::Compression::Deflate;
    config
}

struct Listener {
    endpoint: moq_noq::Endpoint,
    server: web_transport_moq::Server,
}
impl Listener {
    fn new() -> (MoqConfig, Self) {
        let certificate =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
                .unwrap();
        let der = certificate.cert.der().clone();
        let key = rustls::pki_types::PrivatePkcs8KeyDer::from(certificate.key_pair.serialize_der());
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![der.clone()], key.into())
        .unwrap();
        tls.alpn_protocols = vec![b"h3".to_vec()];
        tls.max_early_data_size = u32::MAX;
        let quic = moq_noq::crypto::rustls::QuicServerConfig::try_from(tls).unwrap();
        let endpoint = moq_noq::Endpoint::server(
            moq_noq::ServerConfig::with_crypto(Arc::new(quic)),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let mut config = MoqConfig::new(format!(
            "https://127.0.0.1:{}/moq?opaque=test",
            endpoint.local_addr().unwrap().port()
        ));
        config.trust = TlsTrust::Roots(vec![der.to_vec()]);
        let server = web_transport_moq::Server::new(endpoint.clone());
        (config, Self { endpoint, server })
    }
    async fn accept(mut self, announce: bool, channels: u32) -> Peer {
        let request = self.server.accept().await.expect("WebTransport request");
        assert_eq!(request.connect().url.path(), "/moq");
        assert_eq!(request.connect().url.query(), Some("opaque=test"));
        assert_eq!(request.connect().protocols, vec!["moq-lite-06"]);
        let transport = request
            .respond(web_transport_moq::proto::ConnectResponse::OK.with_protocol("moq-lite-06"))
            .await
            .unwrap();
        let (publish, publish_driver) = moq_net::origin::Producer::new(Default::default());
        let (subscribe, subscribe_driver) = moq_net::origin::Producer::new(Default::default());
        let broadcast = publish.create_broadcast("pipecat/request").unwrap();
        let transcript_track = broadcast.create_track("transcript.json.z", None).unwrap();
        let transcript = moq_json::stream::Producer::new(transcript_track.clone(), compressed());
        let audio_track = broadcast
            .create_track(
                "microphone",
                hang::container::track_info(hang::catalog::PRIORITY.audio),
            )
            .unwrap();
        let mut catalog = hang::catalog::Catalog::default();
        catalog
            .audio
            .insert(
                "microphone",
                hang::catalog::AudioConfig::new(hang::catalog::AudioCodec::Opus, 48_000, channels),
            )
            .unwrap();
        let mut catalog_out = moq_json::snapshot::Producer::new(
            broadcast
                .create_track("catalog.json", hang::catalog::Catalog::default_track_info())
                .unwrap(),
            Default::default(),
        );
        catalog_out.update(&catalog).unwrap();
        if announce {
            broadcast.announce(Default::default()).unwrap();
        }
        let (session, driver) = moq_net::Server::new()
            .with_publisher(&publish)
            .with_subscriber(subscribe.clone())
            .with_versions(wire::version().into())
            .accept(
                tokio::time::Instant::now().into_std(),
                moq_tokio::transport::Session::new(transport),
            )
            .await
            .unwrap();
        let mut jobs = JoinSet::new();
        jobs.spawn(async move {
            moq_net::time::run(publish_driver).await;
        });
        jobs.spawn(async move {
            moq_net::time::run(subscribe_driver).await;
        });
        jobs.spawn(async move {
            moq_net::time::run(driver).await;
        });
        Peer {
            endpoint: self.endpoint,
            jobs,
            session,
            _publish: publish,
            subscribe,
            _broadcast: broadcast,
            transcript,
            transcript_track,
            audio_track,
            _catalog: catalog_out,
        }
    }
}
struct Peer {
    endpoint: moq_noq::Endpoint,
    jobs: JoinSet<()>,
    session: moq_net::Session,
    _publish: moq_net::origin::Producer,
    subscribe: moq_net::origin::Producer,
    _broadcast: moq_net::broadcast::Producer,
    transcript: moq_json::stream::Producer<Value>,
    transcript_track: moq_net::track::Producer,
    audio_track: moq_net::track::Producer,
    _catalog: moq_json::snapshot::Producer<hang::catalog::Catalog>,
}
impl Peer {
    async fn remote(&self) -> moq_net::broadcast::Consumer {
        bounded(
            self.subscribe
                .consume()
                .routed_broadcast("pipecat/response"),
        )
        .await
        .unwrap()
    }
    async fn incoming_json(&self) -> moq_json::stream::Consumer<Value> {
        let remote = self.remote().await;
        let track = bounded(remote.track("transcript.json.z").unwrap().subscribe(None))
            .await
            .unwrap();
        moq_json::stream::Consumer::new(track, compressed())
    }
    fn send_audio(&self, channels: u32, timestamp: u64) {
        let channel_mode = if channels == 1 {
            opus::Channels::Mono
        } else {
            opus::Channels::Stereo
        };
        let mut encoder =
            opus::Encoder::new(48_000, channel_mode, opus::Application::Voip).unwrap();
        let pcm: Vec<i16> = (0..960)
            .flat_map(|n| (0..channels).map(move |_| ((n % 80) as i16 - 40) * 200))
            .collect();
        let mut packet = [0u8; 4096];
        let size = encoder.encode(&pcm, &mut packet).unwrap();
        let frame = hang::container::Frame {
            timestamp: moq_net::Timestamp::from_micros(timestamp).unwrap(),
            payload: Bytes::copy_from_slice(&packet[..size]),
        };
        let mut group = self.audio_track.append_group().unwrap();
        frame.write_to(&mut group).unwrap();
        group.finish().unwrap();
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.session.abort(moq_net::Error::Cancel);
        self.endpoint.close(0u32.into(), b"fixture closed");
        self.jobs.abort_all();
    }
}
async fn pair(mutator: impl FnOnce(&mut MoqConfig), channels: u32) -> (MoqConnection, Peer) {
    let (mut config, listener) = Listener::new();
    mutator(&mut config);
    let accepting = tokio::spawn(listener.accept(true, channels));
    let connection = bounded(MoqConnection::connect(config)).await.unwrap();
    let peer = bounded(accepting).await.unwrap();
    (connection, peer)
}
async fn message(connection: &mut MoqConnection) -> Value {
    bounded(async {
        loop {
            match connection.recv().await.expect("connection event") {
                MoqEvent::Message(message) => return message,
                MoqEvent::PeerConnected => {}
                other => panic!("unexpected event: {other:?}"),
            }
        }
    })
    .await
}

#[tokio::test]
async fn real_quic_bidirectional_compressed_json_and_epoch_dedup() {
    let (mut connection, mut peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    assert!(matches!(
        bounded(connection.recv()).await,
        Some(MoqEvent::PeerConnected)
    ));
    let sender = connection.sender();
    let mut incoming = peer.incoming_json().await;
    sender
        .publish_json(json!({"label":"rtvi", "type":"bot-ready", "seq":999, "epoch":"caller"}))
        .await
        .unwrap();
    let record = bounded(incoming.next()).await.unwrap().unwrap();
    assert_eq!(record["seq"], 0);
    assert_ne!(record["epoch"], "caller");
    sender.publish_json(json!({"n":2})).await.unwrap();
    let second = bounded(incoming.next()).await.unwrap().unwrap();
    assert_eq!(second["seq"], 1);
    assert_eq!(second["epoch"], record["epoch"]);
    for value in [
        json!({"seq":0,"epoch":"a","n":1}),
        json!({"seq":0,"epoch":"a","n":"replay"}),
        json!({"seq":1,"epoch":"a","n":2}),
        json!({"seq":0,"epoch":"b","n":3}),
    ] {
        peer.transcript.append(&value).unwrap();
    }
    for n in [1, 2, 3] {
        assert_eq!(message(&mut connection).await, json!({"n":n}));
    }
    connection.cancel().await.unwrap();
    assert_eq!(connection.monitor().active_tasks(), 0);
}

#[tokio::test]
async fn real_hang_opus_audio_is_encoded_and_stereo_input_is_downmixed() {
    let (mut connection, peer) = pair(|_| {}, 2).await;
    bounded(connection.recv()).await.unwrap();
    let remote = peer.remote().await;
    let mut catalog = bounded(hang::catalog::Catalog::<()>::subscribe(&remote))
        .await
        .unwrap();
    let catalog = bounded(catalog.next()).await.unwrap().unwrap();
    let config = catalog.audio.renditions.get("bot-audio").unwrap();
    assert_eq!(config.codec, hang::catalog::AudioCodec::Opus);
    assert_eq!((config.sample_rate, config.channel_count), (48_000, 1));
    let mut outgoing = bounded(remote.track("bot-audio").unwrap().subscribe(None))
        .await
        .unwrap();
    connection.sender().publish_audio(audio()).await.unwrap();
    let mut group = bounded(outgoing.recv_group()).await.unwrap().unwrap();
    let frame = bounded(group.read_frame()).await.unwrap().unwrap();
    let encoded = hang::container::Frame::decode(frame.payload).unwrap();
    assert_eq!(encoded.timestamp.value(), 0);
    let mut decoder = opus::Decoder::new(48_000, opus::Channels::Mono).unwrap();
    let mut samples = [0i16; 5760];
    assert_eq!(
        decoder
            .decode(&encoded.payload, &mut samples, false)
            .unwrap(),
        960
    );
    assert!(samples.iter().any(|s| *s != 0));
    peer.send_audio(2, 123_456);
    let MoqEvent::Audio {
        audio,
        timestamp_us,
    } = bounded(connection.recv()).await.unwrap()
    else {
        panic!("expected audio")
    };
    assert_eq!(
        (
            audio.sample_rate,
            audio.num_channels,
            audio.audio.len(),
            timestamp_us
        ),
        (48_000, 1, 1920, 123_456)
    );
    assert!(audio.audio.iter().any(|s| *s != 0));
    connection.cancel().await.unwrap();
}

#[tokio::test]
async fn end_sends_goodbye_and_joins_but_does_not_report_playback() {
    let (mut connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let sender = connection.sender();
    let mut incoming = peer.incoming_json().await;
    sender.publish_json(json!({"n":0})).await.unwrap();
    assert_eq!(bounded(incoming.next()).await.unwrap().unwrap()["n"], 0);
    let ending = tokio::spawn(async move {
        let result = connection.end().await;
        (connection, result)
    });
    let goodbye = bounded(incoming.next()).await.unwrap().unwrap();
    assert_eq!(goodbye["label"], "moq-transport");
    assert_eq!(goodbye["type"], "session-ending");
    assert_eq!(goodbye["seq"], 1);
    let (connection, result) = bounded(ending).await.unwrap();
    result.unwrap();
    assert_eq!(connection.monitor().active_tasks(), 0);
    assert_eq!(sender.publish_json(json!({})).await, Err(Error::Closed));
}

#[tokio::test]
async fn cancel_with_full_incoming_queue_and_retained_sender_joins_every_task() {
    let (mut connection, mut peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.limits.incoming_queue = 1;
        },
        1,
    )
    .await;
    let sender = connection.sender();
    let mut monitor = connection.monitor();
    for n in 0..20 {
        peer.transcript.append(&json!({"n":n})).unwrap();
    }
    bounded(connection.cancel()).await.unwrap();
    bounded(monitor.wait_idle()).await;
    assert_eq!(monitor.active_tasks(), 0);
    bounded(monitor.wait_closed()).await.unwrap();
    assert_eq!(sender.publish_json(json!({})).await, Err(Error::Closed));
}

#[tokio::test]
async fn drop_aborts_tasks_even_when_senders_and_monitors_are_retained() {
    let (connection, _peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let sender = connection.sender();
    let monitor = connection.monitor();
    drop(connection);
    bounded(monitor.wait_idle()).await;
    assert_eq!(monitor.active_tasks(), 0);
    assert_eq!(sender.publish_json(json!({})).await, Err(Error::Closed));
}

#[tokio::test]
async fn absent_peer_has_a_bounded_deadline() {
    let (mut config, listener) = Listener::new();
    config.limits.connect_timeout = Duration::from_millis(150);
    let accepting = tokio::spawn(listener.accept(false, 1));
    let result = bounded(MoqConnection::connect(config)).await;
    assert!(matches!(result, Err(Error::Timeout("peer announcement"))));
    drop(bounded(accepting).await.unwrap());
}

#[tokio::test]
async fn untrusted_certificate_is_rejected_without_bypassing_tls() {
    let (mut config, listener) = Listener::new();
    config.trust = TlsTrust::WebPki;
    let accepting = tokio::spawn(listener.accept(true, 1));
    assert!(matches!(
        bounded(MoqConnection::connect(config)).await,
        Err(Error::Network(_))
    ));
    accepting.abort();
    let _ = accepting.await;
}

#[tokio::test]
async fn missing_selected_moq_protocol_is_rejected() {
    let (config, mut listener) = Listener::new();
    let server = tokio::spawn(async move {
        let request = listener.server.accept().await.unwrap();
        let transport = request.ok().await.unwrap();
        transport.closed().await;
    });
    assert!(matches!(
        bounded(MoqConnection::connect(config)).await,
        Err(Error::Unsupported(_))
    ));
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn expanded_record_bomb_is_rejected_by_bounded_deflate_before_json_parse() {
    let (mut connection, peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.limits.expanded_record_bytes = 1024;
        },
        1,
    )
    .await;
    let plain = serde_json::to_vec(&json!({"padding":"x".repeat(8192)})).unwrap();
    let compressed = moq_flate::Encoder::new().frame(&plain);
    assert!(compressed.len() < 1024);
    let mut group = peer.transcript_track.append_group().unwrap();
    group
        .write_frame(moq_net::Timestamp::now(), compressed)
        .unwrap();
    let mut monitor = connection.monitor();
    assert_eq!(
        bounded(monitor.wait_closed()).await,
        Err(Error::Limit("expanded JSON record"))
    );
    bounded(connection.cancel()).await.unwrap();
}

#[tokio::test]
async fn oversized_wire_frame_and_second_transcript_group_fail_explicitly() {
    for rolled in [false, true] {
        let (mut connection, mut peer) = pair(
            |c| {
                c.audio_in_enabled = false;
                c.limits.compressed_record_bytes = 1024;
            },
            1,
        )
        .await;
        if rolled {
            peer.transcript.append(&json!({"n":0})).unwrap();
            assert_eq!(message(&mut connection).await, json!({"n":0}));
        }
        let mut group = peer.transcript_track.append_group().unwrap();
        group
            .write_frame(
                moq_net::Timestamp::now(),
                if rolled {
                    Bytes::from_static(b"invalid")
                } else {
                    Bytes::from(vec![0; 2048])
                },
            )
            .unwrap();
        let error = bounded(connection.monitor().wait_closed())
            .await
            .unwrap_err();
        if rolled {
            assert!(
                matches!(&error, Error::Protocol(message) if message.contains("second group")),
                "{error:?}"
            );
        } else {
            assert_eq!(error, Error::Limit("wire frame"));
        }
        connection.cancel().await.unwrap();
    }
}

#[tokio::test]
async fn transcript_record_budget_fails_without_rolling_or_silent_loss() {
    let (mut connection, peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.limits.transcript_records = 3;
        },
        1,
    )
    .await;
    let sender = connection.sender();
    let mut incoming = peer.incoming_json().await;
    for n in 0..2 {
        sender.publish_json(json!({"n":n})).await.unwrap();
        assert_eq!(bounded(incoming.next()).await.unwrap().unwrap()["n"], n);
    }
    assert_eq!(
        sender.publish_json(json!({"n":2})).await,
        Err(Error::Limit("transcript history"))
    );
    assert_eq!(
        bounded(connection.monitor().wait_closed()).await,
        Err(Error::Limit("transcript history"))
    );
    connection.cancel().await.unwrap();
}

#[tokio::test]
async fn outgoing_audio_is_format_checked_and_bounded_queue_reports_saturation() {
    let (mut connection, _peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.limits.outgoing_queue = 1;
        },
        1,
    )
    .await;
    let sender = connection.sender();
    assert!(matches!(
        sender
            .publish_audio(AudioRawFrame::new(vec![0; 640], 16_000, 1).unwrap())
            .await,
        Err(Error::InvalidAudio(_))
    ));
    let mut jobs = JoinSet::new();
    for _ in 0..32 {
        let sender = sender.clone();
        jobs.spawn(async move { sender.publish_audio(audio()).await });
    }
    let mut saturation = 0;
    while let Some(result) = bounded(jobs.join_next()).await {
        if result.unwrap() == Err(Error::Backpressure) {
            saturation += 1;
        }
    }
    assert!(saturation > 0);
    connection.cancel().await.unwrap();
}

#[tokio::test]
async fn unsupported_modes_and_bad_configuration_fail_before_network_io() {
    for mode in [MoqMode::Serve, MoqMode::Browser] {
        let mut config = MoqConfig::new("not a URL");
        config.mode = mode;
        assert!(matches!(
            MoqConnection::connect(config).await,
            Err(Error::Unsupported(_))
        ));
    }
    let mut config = MoqConfig::new("not a URL");
    config.trust = TlsTrust::MutualTls;
    assert!(matches!(
        MoqConnection::connect(config).await,
        Err(Error::Unsupported("mutual TLS"))
    ));
    for which in 0..3 {
        let mut config = MoqConfig::new("not a URL");
        match which {
            0 => config.video_enabled = true,
            1 => config.reconnect = true,
            _ => config.audio_out_sample_rate = 16_000,
        }
        assert!(matches!(
            MoqConnection::connect(config).await,
            Err(Error::Unsupported(_))
        ));
    }
    for url in [
        "http://127.0.0.1",
        "https://user:secret@localhost/",
        "https://localhost/#fragment",
    ] {
        assert!(MoqConfig::new(url).validate().is_err());
    }
}

#[test]
fn json_expansion_and_transcript_metadata_rules_have_exact_limits() {
    assert_eq!(
        transcript::json_bytes(&json!({"a":"123"}), 11)
            .unwrap()
            .len(),
        11
    );
    assert!(transcript::json_bytes(&json!({"a":"123"}), 10).is_err());
    let mut source = moq_json::stream::Encoder::<Value>::new(compressed());
    let mut decoder = transcript::Subscriber::new(Default::default());
    for (value, expected) in [
        (
            json!({"seq":true,"epoch":"a","n":0}),
            transcript::Record::Message(json!({"seq":true,"epoch":"a","n":0})),
        ),
        (
            json!({"seq":0,"epoch":"a","n":1}),
            transcript::Record::Message(json!({"n":1})),
        ),
        (
            json!({"seq":0,"epoch":"a","n":1}),
            transcript::Record::Replay,
        ),
        (
            json!({"seq":0,"epoch":"b","n":2}),
            transcript::Record::Message(json!({"n":2})),
        ),
        (
            json!({"label":"moq-transport","type":"session-ending"}),
            transcript::Record::Goodbye,
        ),
    ] {
        let record = source.encode(&value).unwrap();
        assert_eq!(decoder.decode(record.payload()).unwrap(), expected);
        record.commit();
    }
}

#[tokio::test]
async fn interruption_invalidates_pending_audio_but_preserves_json_and_session() {
    let (mut connection, peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.limits.outgoing_queue = 64;
        },
        1,
    )
    .await;
    let sender = connection.sender();
    let mut json = peer.incoming_json().await;
    let mut jobs = JoinSet::new();
    for _ in 0..32 {
        let sender = sender.clone();
        jobs.spawn(async move { sender.publish_audio(audio()).await });
    }
    // All tasks enter admission before the actor can pace an entire queue.
    tokio::task::yield_now().await;
    sender.interrupt().await.unwrap();
    let mut interrupted = 0;
    while let Some(result) = bounded(jobs.join_next()).await {
        match result.unwrap() {
            Ok(()) => {}
            Err(Error::Interrupted) => interrupted += 1,
            other => panic!("unexpected publication result {other:?}"),
        }
    }
    assert!(interrupted > 0);
    sender
        .publish_json(json!({"after":"interrupt"}))
        .await
        .unwrap();
    assert_eq!(
        bounded(json.next()).await.unwrap().unwrap()["after"],
        "interrupt"
    );
    sender.publish_audio(audio()).await.unwrap();
    connection.cancel().await.unwrap();
}

#[tokio::test]
async fn end_deadline_cancels_a_long_audio_drain_and_joins() {
    let (mut connection, _peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.limits.outgoing_queue = 64;
            c.limits.end_timeout = Duration::from_millis(80);
            c.limits.close_grace = Duration::ZERO;
        },
        1,
    )
    .await;
    let sender = connection.sender();
    let monitor = connection.monitor();
    let mut jobs = JoinSet::new();
    for _ in 0..32 {
        let sender = sender.clone();
        jobs.spawn(async move { sender.publish_audio(audio()).await });
    }
    tokio::task::yield_now().await;
    assert_eq!(
        bounded(connection.end()).await,
        Err(Error::Timeout("end drain"))
    );
    bounded(monitor.wait_idle()).await;
    assert_eq!(monitor.active_tasks(), 0);
    while bounded(jobs.join_next()).await.is_some() {}
}

#[tokio::test]
async fn incoming_total_expanded_budget_counts_replayed_records() {
    let (mut connection, mut peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.limits.transcript_bytes = 4096;
            c.limits.expanded_record_bytes = 2048;
            c.limits.incoming_queue = 16;
        },
        1,
    )
    .await;
    let value = json!({"seq":0,"epoch":"peer","padding":"x".repeat(1500)});
    for _ in 0..3 {
        peer.transcript.append(&value).unwrap();
    }
    assert_eq!(
        bounded(connection.monitor().wait_closed()).await,
        Err(Error::Limit("incoming expanded transcript history"))
    );
    connection.cancel().await.unwrap();
}

#[tokio::test]
async fn peer_goodbye_is_internal_and_a_closed_peer_cannot_report_end_drain_success() {
    let (mut connection, mut peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    peer.transcript
        .append(&json!({"label":"moq-transport","type":"session-ending"}))
        .unwrap();
    assert_eq!(
        bounded(connection.monitor().wait_closed()).await,
        Err(Error::Closed)
    );
    assert!(matches!(
        bounded(connection.recv()).await,
        Some(MoqEvent::PeerConnected)
    ));
    assert!(bounded(connection.recv()).await.is_none());
    assert_eq!(connection.end().await, Err(Error::Closed));
}

#[test]
fn unsigned_sequence_watermarks_do_not_overflow_signed_integer_range() {
    let mut source = moq_json::stream::Encoder::<Value>::new(compressed());
    let mut decoder = transcript::Subscriber::new(Default::default());
    let value = json!({"seq":u64::MAX,"epoch":"large","n":1});
    for replay in [false, true] {
        let record = source.encode(&value).unwrap();
        let actual = decoder.decode(record.payload()).unwrap();
        record.commit();
        assert_eq!(
            actual,
            if replay {
                transcript::Record::Replay
            } else {
                transcript::Record::Message(json!({"n":1}))
            }
        );
    }
}

#[tokio::test]
async fn cancelling_connect_while_waiting_for_peer_closes_the_transport() {
    let (config, listener) = Listener::new();
    let connecting = tokio::spawn(MoqConnection::connect(config));
    let peer = bounded(listener.accept(false, 1)).await;
    connecting.abort();
    assert!(matches!(bounded(connecting).await, Err(error) if error.is_cancelled()));
    bounded(peer.session.closed()).await;
}

#[tokio::test]
async fn backwards_audio_timestamp_terminates_the_session() {
    let (mut connection, peer) = pair(|_| {}, 1).await;
    assert!(matches!(
        bounded(connection.recv()).await,
        Some(MoqEvent::PeerConnected)
    ));
    peer.send_audio(1, 40_000);
    assert!(matches!(
        bounded(connection.recv()).await,
        Some(MoqEvent::Audio {
            timestamp_us: 40_000,
            ..
        })
    ));
    peer.send_audio(1, 20_000);
    assert_eq!(
        bounded(connection.monitor().wait_closed()).await,
        Err(Error::Protocol("audio timestamp moved backwards".into()))
    );
    connection.cancel().await.unwrap();
}

#[tokio::test]
async fn unsupported_peer_audio_channels_fail_explicitly() {
    let (mut connection, _peer) = pair(|_| {}, 3).await;
    assert!(matches!(
        bounded(connection.monitor().wait_closed()).await,
        Err(Error::Unsupported(_))
    ));
    connection.cancel().await.unwrap();
}

mod review_regressions;

#[test]
fn sequenced_epoch_types_are_explicit_and_legacy_records_are_unchanged() {
    for epoch in [json!(false), json!(1), json!([]), json!({})] {
        let mut compressor = moq_flate::Encoder::new();
        let mut decoder = transcript::Subscriber::new(Default::default());
        let legacy = json!({"seq":false,"epoch":epoch});
        assert_eq!(
            decoder
                .decode(&compressor.frame(&serde_json::to_vec(&legacy).unwrap()))
                .unwrap(),
            transcript::Record::Message(legacy)
        );
        let sequenced = serde_json::to_vec(&json!({"seq":0,"epoch":epoch})).unwrap();
        assert_eq!(
            decoder.decode(&compressor.frame(&sequenced)),
            Err(Error::Protocol(
                "sequenced epoch must be a string or null".into()
            ))
        );
    }
}

#[test]
fn escaped_sequence_keys_and_json_whitespace_keep_replay_filtering() {
    let mut compressor = moq_flate::Encoder::new();
    let mut decoder = transcript::Subscriber::new(Default::default());
    let wire = br#"{ "\u0073eq" :  18446744073709551616 , "epoch" : null, "n":1 }"#;
    assert_eq!(
        decoder.decode(&compressor.frame(wire)).unwrap(),
        transcript::Record::Message(json!({"n":1}))
    );
    assert_eq!(
        decoder.decode(&compressor.frame(wire)).unwrap(),
        transcript::Record::Replay
    );
}
