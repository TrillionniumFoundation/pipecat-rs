use super::*;

#[tokio::test]
async fn review_goodbye_reserved_after_exact_record_budget() {
    let (mut connection, peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.limits.transcript_records = 2;
        },
        1,
    )
    .await;
    let mut incoming = peer.incoming_json().await;
    connection
        .sender()
        .publish_json(json!({"payload":"last application record"}))
        .await
        .unwrap();
    assert_eq!(bounded(incoming.next()).await.unwrap().unwrap()["seq"], 0);
    let ending = tokio::spawn(async move {
        let result = connection.end().await;
        (connection, result)
    });
    let goodbye = bounded(incoming.next()).await.unwrap().unwrap();
    assert_eq!(goodbye["seq"], 1);
    assert_eq!(goodbye["type"], "session-ending");
    let (connection, result) = bounded(ending).await.unwrap();
    result.unwrap();
    assert_eq!(connection.monitor().active_tasks(), 0);
}

#[tokio::test]
async fn review_cancelled_end_then_drop_closes_real_peer_and_all_owned_tasks() {
    let (mut connection, peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.limits.outgoing_queue = 64;
        },
        1,
    )
    .await;
    let sender = connection.sender();
    let monitor = connection.monitor();
    let mut writes = JoinSet::new();
    for _ in 0..32 {
        let sender = sender.clone();
        writes.spawn(async move { sender.publish_audio(audio()).await });
    }
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1), connection.end())
            .await
            .is_err()
    );
    drop(connection);
    bounded(monitor.wait_idle()).await;
    bounded(peer.session.closed()).await;
    assert_eq!(monitor.active_tasks(), 0);
    assert_eq!(sender.publish_json(json!({})).await, Err(Error::Closed));
    while let Some(write) = bounded(writes.join_next()).await {
        let _ = write.unwrap();
    }
}

#[tokio::test]
async fn review_reserved_control_and_rejected_values_do_not_poison_session() {
    let (mut connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let sender = connection.sender();
    for value in [
        json!([]),
        json!(null),
        json!({"label":"moq-transport","type":"session-ending"}),
    ] {
        assert!(matches!(
            sender.publish_json(value).await,
            Err(Error::InvalidConfig(_))
        ));
    }
    let mut incoming = peer.incoming_json().await;
    sender
        .publish_json(json!({"seq":0,"epoch":"spoofed","valid":true}))
        .await
        .unwrap();
    let value = bounded(incoming.next()).await.unwrap().unwrap();
    assert_ne!(value["epoch"], "spoofed");
    assert_eq!(value["seq"], 0);
    assert_eq!(value["valid"], true);
    connection.cancel().await.unwrap();
}

#[tokio::test]
async fn review_bad_audio_payload_closes_and_joins_without_decoded_audio() {
    let (mut connection, peer) = pair(|_| {}, 1).await;
    assert!(matches!(
        bounded(connection.recv()).await,
        Some(MoqEvent::PeerConnected)
    ));
    let frame = hang::container::Frame {
        timestamp: moq_net::Timestamp::from_micros(0).unwrap(),
        payload: Bytes::from_static(&[0xff]),
    };
    let mut group = peer.audio_track.append_group().unwrap();
    frame.write_to(&mut group).unwrap();
    group.finish().unwrap();
    assert!(matches!(
        bounded(connection.monitor().wait_closed()).await,
        Err(Error::Media(_))
    ));
    assert!(bounded(connection.recv()).await.is_none());
    connection.cancel().await.unwrap();
    assert_eq!(connection.monitor().active_tasks(), 0);
}

#[tokio::test]
async fn review_wire_header_is_rejected_before_missing_payload_wait() {
    let (origin, _driver) = moq_net::origin::Producer::new(Default::default());
    let broadcast = origin.create_broadcast("review").unwrap();
    let track = broadcast.create_track("wire", None).unwrap();
    let mut subscriber = track.subscribe(moq_net::track::Subscription::default());
    let mut group = track.append_group().unwrap();
    let _incomplete = group
        .create_frame(moq_net::frame::Info {
            timestamp: moq_net::Timestamp::now(),
            size: 1025,
        })
        .unwrap();
    let mut received = bounded(subscriber.recv_group()).await.unwrap().unwrap();
    let frame = bounded(received.next_frame()).await.unwrap().unwrap();
    assert_eq!(
        bounded(wire::payload(frame, 1024)).await,
        Err(Error::Limit("wire frame"))
    );
}

#[tokio::test]
async fn review_catalog_reconstruction_is_bounded_across_small_patches() {
    let (origin, _driver) = moq_net::origin::Producer::new(Default::default());
    let broadcast = origin.create_broadcast("review").unwrap();
    let track = broadcast.create_track("catalog.json", None).unwrap();
    let mut group = track.append_group().unwrap();
    group
        .write_frame(moq_net::Timestamp::now(), Bytes::from_static(b"{}"))
        .unwrap();
    for n in 0..3 {
        let patch = serde_json::to_vec(&json!({ format!("key{n}"): "x".repeat(500) })).unwrap();
        assert!(patch.len() < 1024);
        group
            .write_frame(moq_net::Timestamp::now(), Bytes::from(patch))
            .unwrap();
    }
    let result = bounded(wire::audio_catalog(&broadcast.consume(), 1024)).await;
    assert!(matches!(result, Err(Error::Limit("reconstructed catalog"))));
}

#[tokio::test]
async fn review_catalog_empty_snapshot_can_be_completed_by_audio_delta() {
    let (origin, _driver) = moq_net::origin::Producer::new(Default::default());
    let broadcast = origin.create_broadcast("review").unwrap();
    let track = broadcast.create_track("catalog.json", None).unwrap();
    let mut group = track.append_group().unwrap();
    group
        .write_frame(moq_net::Timestamp::now(), Bytes::from_static(b"{}"))
        .unwrap();
    let mut catalog = hang::catalog::Catalog::<()>::default();
    catalog
        .audio
        .insert(
            "speech",
            hang::catalog::AudioConfig::new(hang::catalog::AudioCodec::Opus, 48_000, 1),
        )
        .unwrap();
    group
        .write_frame(
            moq_net::Timestamp::now(),
            Bytes::from(serde_json::to_vec(&catalog).unwrap()),
        )
        .unwrap();
    let (name, config) = bounded(wire::audio_catalog(&broadcast.consume(), 1024))
        .await
        .unwrap();
    assert_eq!(name, "speech");
    assert_eq!(config.sample_rate, 48_000);
}

#[test]
fn review_upper_u64_integer_is_not_forwarded_as_legacy_metadata() {
    let wire_json = br#"{"seq":18446744073709551616,"epoch":"large","n":1}"#;
    let mut compressor = moq_flate::Encoder::new();
    let mut decoder = transcript::Subscriber::new(Default::default());
    let first = decoder.decode(&compressor.frame(wire_json));
    assert!(
        matches!(&first, Ok(transcript::Record::Message(value)) if value == &json!({"n":1})),
        "oversized integer was silently reclassified as legacy metadata: {first:?}"
    );
    let second = decoder.decode(&compressor.frame(wire_json));
    assert_eq!(second, Ok(transcript::Record::Replay));
}

#[test]
fn review_absent_and_null_epoch_share_one_deduplication_watermark() {
    let mut compressor = moq_flate::Encoder::new();
    let mut decoder = transcript::Subscriber::new(Default::default());
    assert_eq!(
        decoder.decode(&compressor.frame(br#"{"seq":0,"n":1}"#)),
        Ok(transcript::Record::Message(json!({"n":1})))
    );
    assert_eq!(
        decoder.decode(&compressor.frame(br#"{"seq":0,"epoch":null,"n":2}"#)),
        Ok(transcript::Record::Replay)
    );
    assert_eq!(
        decoder.decode(&compressor.frame(br#"{"seq":0,"n":3}"#)),
        Ok(transcript::Record::Replay)
    );
}

#[tokio::test]
async fn review_new_catalog_snapshot_supersedes_unfinished_old_frame() {
    let (origin, _driver) = moq_net::origin::Producer::new(Default::default());
    let broadcast = origin.create_broadcast("review").unwrap();
    let track = broadcast.create_track("catalog.json", None).unwrap();
    let mut old_group = track.append_group().unwrap();
    let _incomplete = old_group
        .create_frame(moq_net::frame::Info {
            timestamp: moq_net::Timestamp::from_micros(0).unwrap(),
            size: 2,
        })
        .unwrap();
    let consumer = broadcast.consume();
    let mut reading = Box::pin(wire::audio_catalog(&consumer, 1024));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(reading.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    let mut replacement = track.append_group().unwrap();
    let mut catalog = hang::catalog::Catalog::<()>::default();
    catalog
        .audio
        .insert(
            "replacement",
            hang::catalog::AudioConfig::new(hang::catalog::AudioCodec::Opus, 48_000, 1),
        )
        .unwrap();
    replacement
        .write_frame(
            moq_net::Timestamp::from_micros(20_000).unwrap(),
            Bytes::from(serde_json::to_vec(&catalog).unwrap()),
        )
        .unwrap();
    replacement.finish().unwrap();
    let result = bounded(reading).await;
    assert!(
        matches!(&result, Ok((name, _)) if name == "replacement"),
        "superseded partial catalog prevented recovery: {result:?}"
    );
}

#[test]
fn review_sequence_lexical_boundaries_preserve_integer_fencing() {
    let mut compressor = moq_flate::Encoder::new();
    let mut decoder = transcript::Subscriber::new(Default::default());
    for (wire, expected) in [
        (
            r#"{"seq":-0,"epoch":"a","n":0}"#,
            transcript::Record::Message(json!({"n":0})),
        ),
        (r#"{"seq":0,"epoch":"a","n":1}"#, transcript::Record::Replay),
        (
            r#"{"seq":-18446744073709551616,"epoch":"a","n":2}"#,
            transcript::Record::Replay,
        ),
        (
            r#"{"seq":1.0,"epoch":"ignored","n":3}"#,
            transcript::Record::Message(json!({"seq":1.0,"epoch":"ignored","n":3})),
        ),
        (
            r#"{"seq":1e3,"epoch":"ignored","n":4}"#,
            transcript::Record::Message(json!({"seq":1000.0,"epoch":"ignored","n":4})),
        ),
        (
            r#"{"seq":false,"epoch":"ignored","n":5}"#,
            transcript::Record::Message(json!({"seq":false,"epoch":"ignored","n":5})),
        ),
        (r#"{"seq":0,"epoch":"a","n":6}"#, transcript::Record::Replay),
        (
            r#"{"seq":18446744073709551616,"epoch":"a","n":7}"#,
            transcript::Record::Message(json!({"n":7})),
        ),
        (
            r#"{"seq":18446744073709551615,"epoch":"a","n":8}"#,
            transcript::Record::Replay,
        ),
        (
            r#"{"seq":18446744073709551617,"epoch":"a","n":9}"#,
            transcript::Record::Message(json!({"n":9})),
        ),
    ] {
        assert_eq!(
            decoder.decode(&compressor.frame(wire.as_bytes())).unwrap(),
            expected,
            "wire record {wire}"
        );
    }
}

#[test]
fn review_sequence_magnitude_beyond_f64_range_is_bounded_decimal_metadata() {
    let mut compressor = moq_flate::Encoder::new();
    let mut decoder = transcript::Subscriber::new(Default::default());
    let integer = "9".repeat(1000);
    let wire = format!(r#"{{"seq":{integer},"epoch":"a","n":1}}"#);
    assert_eq!(
        decoder.decode(&compressor.frame(wire.as_bytes())).unwrap(),
        transcript::Record::Message(json!({"n":1}))
    );
    assert_eq!(
        decoder.decode(&compressor.frame(wire.as_bytes())).unwrap(),
        transcript::Record::Replay
    );
}

#[tokio::test]
async fn review_webtransport_handshake_timeout_closes_pending_server_request() {
    let (mut config, mut listener) = Listener::new();
    config.limits.connect_timeout = Duration::from_millis(100);
    let connecting = tokio::spawn(MoqConnection::connect(config));
    let request = bounded(listener.server.accept()).await.unwrap();
    assert!(matches!(
        bounded(connecting).await.unwrap(),
        Err(Error::Timeout("WebTransport handshake"))
    ));
    if let Ok(transport) = bounded(
        request.respond(web_transport_moq::proto::ConnectResponse::OK.with_protocol("moq-lite-06")),
    )
    .await
    {
        bounded(transport.closed()).await;
    }
    listener.endpoint.close(0u32.into(), b"test finished");
}

#[tokio::test]
async fn review_catalog_track_finish_does_not_truncate_pending_last_frame() {
    let (origin, _driver) = moq_net::origin::Producer::new(Default::default());
    let broadcast = origin.create_broadcast("review").unwrap();
    let track = broadcast.create_track("catalog.json", None).unwrap();
    let mut group = track.append_group().unwrap();
    let mut catalog = hang::catalog::Catalog::<()>::default();
    catalog
        .audio
        .insert(
            "last",
            hang::catalog::AudioConfig::new(hang::catalog::AudioCodec::Opus, 48_000, 1),
        )
        .unwrap();
    let data = Bytes::from(serde_json::to_vec(&catalog).unwrap());
    let mut unfinished = group
        .create_frame(moq_net::frame::Info {
            timestamp: moq_net::Timestamp::from_micros(0).unwrap(),
            size: data.len() as u64,
        })
        .unwrap();
    let consumer = broadcast.consume();
    let mut reading = Box::pin(wire::audio_catalog(&consumer, 1024));
    std::future::poll_fn(|cx| {
        assert!(std::future::Future::poll(reading.as_mut(), cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    track.finish().unwrap();
    std::future::poll_fn(|cx| {
        assert!(
            std::future::Future::poll(reading.as_mut(), cx).is_pending(),
            "track final sequence prematurely truncated its pending frame"
        );
        std::task::Poll::Ready(())
    })
    .await;
    unfinished.write(data).unwrap();
    unfinished.finish().unwrap();
    group.finish().unwrap();
    let (name, _) = bounded(reading).await.unwrap();
    assert_eq!(name, "last");
}
