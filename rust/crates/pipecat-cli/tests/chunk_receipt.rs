//! Client-reported chunk receipts through real synthetic-provider WS/HTTP,
//! public CLI run_session, shared output, protobuf PCM, and shared history.
//! No physical playback or generated-browser support is inferred.
#[path = "support/chunk_fixture.rs"]
mod fixture;
use fixture::*;
use pipecat_cli::{
    config::Options,
    receipt_output::{PlaybackProtocol, ReceiptConfig},
};
use pipecat_protocol::protobuf::{self as pb, frame::Kind};
use pipecat_services::{Content, Role};
use pipecat_transports::{Packet, WebSocketSession};
use serde_json::{json, Value};
use std::{sync::atomic::Ordering, time::Duration};
use tokio::time::Instant;

fn options() -> Options {
    Options {
        introduce: false,
        turn_timeout: Duration::from_millis(20),
        playback_protocol: PlaybackProtocol::ChunkV2,
        receipt: ReceiptConfig {
            handshake_timeout: Duration::from_millis(500),
            chunk_timeout: Duration::from_secs(2),
            enqueue_timeout: Duration::from_millis(500),
            cutoff_timeout: Duration::from_millis(500),
            drain_timeout: Duration::from_secs(2),
        },
        ..Default::default()
    }
}
fn require(condition: bool, message: impl Into<String>) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn check_closed(result: TestResult<()>, closed: Closed, success: bool) {
    // Every assertion runs after fixture.close has joined its owner and peers.
    eprintln!("provider terminal outcomes: {:?}", closed.provider_exits);
    assert!(
        closed.cleanup_errors.is_empty(),
        "cleanup failures: {:?}; observation={result:?}",
        closed.cleanup_errors
    );
    assert!(
        result.is_ok(),
        "loopback observation: {result:?}; server={:?}",
        closed.server.as_ref().err()
    );
    if success {
        let report = closed
            .server
            .expect("public CLI session succeeds after exact client receipts");
        assert_eq!(
            report.pipeline.total_errors, 0,
            "{:?}",
            report.pipeline.errors
        );
        assert_eq!(
            closed.contacts, 3,
            "actual STT, LLM and TTS must all be contacted"
        );
        let assistant: Vec<_> = report
            .context
            .messages
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .collect();
        assert_eq!(assistant.len(), 1);
        assert_eq!(assistant[0].content, Some(Content::Text(ASSISTANT.into())));
    }
}
async fn offer(client: &mut WebSocketSession) -> TestResult<Value> {
    let value = control(next(client).await?)?;
    require(
        value["type"] == "playback-v2-offer",
        format!("v2 must negotiate before providers: {value}"),
    )?;
    require(
        value["version"] == 2
            && value["output_rate"] == 24000
            && value["channels"] == 1
            && value["encoding"] == "pcm_s16le",
        "offer PCM/version differs",
    )?;
    require(
        value["session_id"]
            .as_str()
            .is_some_and(|id| id.len() == 32),
        "offer must carry 128-bit session identity",
    )?;
    require(value["max_pcm_bytes"] == 49152, "offer PCM bound differs")?;
    Ok(value)
}
fn acceptance(offer: &Value) -> Value {
    json!({"type":"playback-v2-accept","version":2,"session_id":offer["session_id"],"chunk_receipts":true,"cutoff_receipts":true,"flush_on_disconnect":true})
}
async fn negotiate(fixture: &mut Fixture) -> TestResult<Value> {
    let value = offer(fixture.client()?).await?;
    require(
        fixture.provider_contacts.load(Ordering::SeqCst) == 0,
        "provider contacted before application negotiation",
    )?;
    send(fixture.client()?, acceptance(&value)).await?;
    Ok(value)
}
fn receipt(meta: &Value) -> Value {
    json!({"type":"chunk-complete","version":2,"session_id":meta["session_id"],"epoch":meta["epoch"],"chunk_id":meta["chunk_id"],"receipt_nonce":meta["receipt_nonce"]})
}
fn cutoff_receipt(mut value: Value) -> Value {
    value["type"] = json!("cutoff-complete");
    value
}
async fn audio(
    client: &mut WebSocketSession,
    session: &Value,
) -> TestResult<(pb::AudioRawFrame, Value)> {
    loop {
        match next(client).await? {
            Kind::Audio(audio) => {
                let meta: Value = serde_json::from_str(&audio.name).map_err(|e| e.to_string())?;
                require(
                    audio.sample_rate == 24000 && audio.num_channels == 1,
                    "output PCM format differs",
                )?;
                require(
                    meta["version"] == 2 && meta["session_id"] == session["session_id"],
                    "audio identity/version mismatch",
                )?;
                require(
                    meta["chunk_id"].as_u64() == Some(audio.id),
                    "protobuf id must match chunk id",
                )?;
                require(
                    meta["pcm_bytes"].as_u64() == Some(audio.audio.len() as u64),
                    "descriptor PCM count differs",
                )?;
                require(
                    meta["receipt_nonce"]
                        .as_str()
                        .is_some_and(|id| id.len() == 32),
                    "chunk must carry a 128-bit nonce",
                )?;
                return Ok((audio, meta));
            }
            Kind::Message(message) => {
                let value: Value =
                    serde_json::from_str(&message.data).map_err(|e| e.to_string())?;
                if matches!(
                    value["type"].as_str(),
                    Some("playback-v2-context-committed" | "playback-cutoff" | "playback-ready")
                ) {
                    return Err(format!("premature control while awaiting PCM: {value}"));
                }
            }
            other => return Err(format!("unexpected output before chunk: {other:?}")),
        }
    }
}
async fn quiet(client: &mut WebSocketSession, duration: Duration) -> TestResult<()> {
    let deadline = Instant::now() + duration;
    loop {
        match tokio::time::timeout_at(deadline, client.recv()).await {
            Err(_) => return Ok(()),
            Ok(None) => return Err("session closed while an exact receipt remained pending".into()),
            Ok(Some(Packet::Binary(bytes))) => {
                match pb::Frame::parse(&bytes).map_err(|e| e.to_string())?.frame {
                    Some(Kind::Message(message)) => {
                        let value: Value =
                            serde_json::from_str(&message.data).map_err(|e| e.to_string())?;
                        if !matches!(value["type"].as_str(), Some("metrics")) {
                            return Err(format!(
                                "progress before client-reported completion: {value}"
                            ));
                        }
                    }
                    other => return Err(format!("PCM/other progress before receipt: {other:?}")),
                }
            }
            Ok(Some(_)) => return Err("output was not protobuf".into()),
        }
    }
}
async fn committed(client: &mut WebSocketSession, session: &Value) -> TestResult<()> {
    let value = control(next(client).await?)?;
    require(
        value["type"] == "playback-v2-context-committed"
            && value["version"] == 2
            && value["session_id"] == session["session_id"]
            && value["committed_contexts"] == 1,
        format!("history did not commit exactly once: {value}"),
    )
}
async fn terminal(client: &mut WebSocketSession, require_commit: bool) -> TestResult<()> {
    bounded(async {
        let mut committed = false;
        let mut controls = 0;
        while let Some(packet) = client.recv().await {
            let Packet::Binary(bytes) = packet else {
                return Err("terminal output was not protobuf".into());
            };
            let Some(Kind::Message(message)) =
                pb::Frame::parse(&bytes).map_err(|e| e.to_string())?.frame
            else {
                return Err("PCM or frame after final chunk/End".into());
            };
            let value: Value = serde_json::from_str(&message.data).map_err(|e| e.to_string())?;
            controls += 1;
            require(controls <= 8, "unbounded terminal controls")?;
            match value["type"].as_str() {
                Some("playback-cutoff") => send(client, cutoff_receipt(value)).await?,
                Some("playback-v2-context-committed") => {
                    require(
                        !committed && value["committed_contexts"] == 1,
                        "duplicate terminal history commit",
                    )?;
                    committed = true;
                }
                _ => return Err(format!("unexpected terminal control: {value}")),
            }
        }
        require(
            !require_commit || committed,
            "End lost the acknowledged source/history boundary",
        )
    })
    .await
}
async fn finish(client: &mut WebSocketSession) -> TestResult<()> {
    send(client, json!({"type":"end"})).await?;
    terminal(client, false).await
}
fn chunk_bytes(index: usize) -> Vec<u8> {
    let pcm = pcm();
    let start = index * CHUNK_BYTES;
    let mut expected = pcm[start..(start + CHUNK_BYTES).min(pcm.len())].to_vec();
    expected.resize(CHUNK_BYTES, 0);
    expected
}

#[tokio::test]
async fn exact_client_report_per_transformed_chunk_unblocks_real_cli_history() {
    let mut fixture = Fixture::start_gated(options(), true)
        .await
        .expect("fixture");
    let result: TestResult<()> = async {
        let session = negotiate(&mut fixture).await?;
        start_user(fixture.client()?).await?;
        let mut previous = None;
        for index in 0..3 {
            let (pcm, meta) = audio(fixture.client()?, &session).await?;
            require(
                pcm.audio == chunk_bytes(index),
                format!("actual transformed PCM/padded tail differs for chunk {index}"),
            )?;
            if let Some(id) = previous {
                require(pcm.id > id, "chunk IDs must increase")?;
            }
            previous = Some(pcm.id);
            if index < 2 {
                require(
                    fixture.tts_done_sent.load(Ordering::SeqCst) == 0,
                    "chunk receipt required provider done/TtsStopped",
                )?;
            }
            if index == 1 {
                fixture.release_tts_done();
            }
            quiet(fixture.client()?, Duration::from_millis(40)).await?;
            send(fixture.client()?, receipt(&meta)).await?;
        }
        committed(fixture.client()?, &session).await?;
        finish(fixture.client()?).await
    }
    .await;
    let closed = fixture.close().await;
    check_closed(result, closed, true);
}

#[tokio::test]
async fn wrong_future_duplicate_session_epoch_and_nonce_reports_never_advance_pcm() {
    let mut fixture = Fixture::start(options()).await.expect("fixture");
    let result: TestResult<()> = async {
        let session = negotiate(&mut fixture).await?;
        start_user(fixture.client()?).await?;
        let (_, first) = audio(fixture.client()?, &session).await?;
        let valid = receipt(&first);
        let mut wrongs = Vec::new();
        for (field, value) in [
            ("session_id", json!("00000000000000000000000000000000")),
            (
                "epoch",
                json!(first["epoch"].as_u64().ok_or("missing epoch")? + 1),
            ),
            (
                "chunk_id",
                json!(first["chunk_id"].as_u64().ok_or("missing chunk id")? + 1),
            ),
            ("receipt_nonce", json!("00000000000000000000000000000000")),
        ] {
            let mut wrong = valid.clone();
            wrong[field] = value;
            wrongs.push(wrong);
        }
        for wrong in wrongs {
            send(fixture.client()?, wrong).await?;
            quiet(fixture.client()?, Duration::from_millis(20)).await?;
        }
        send(fixture.client()?, valid.clone()).await?;
        let (_, second) = audio(fixture.client()?, &session).await?;
        send(fixture.client()?, valid).await?;
        quiet(fixture.client()?, Duration::from_millis(40)).await?;
        send(fixture.client()?, receipt(&second)).await?;
        let (_, third) = audio(fixture.client()?, &session).await?;
        send(fixture.client()?, receipt(&third)).await?;
        committed(fixture.client()?, &session).await?;
        finish(fixture.client()?).await
    }
    .await;
    let closed = fixture.close().await;
    check_closed(result, closed, true);
}

#[tokio::test]
async fn explicit_v2_rejects_missing_wrong_and_incomplete_negotiation_before_providers() {
    for mode in [
        "missing",
        "wrong-version",
        "wrong-session",
        "missing-field",
        "unaccepted-flush",
        "unknown-field",
    ] {
        let mut selected = options();
        selected.receipt.handshake_timeout = Duration::from_millis(100);
        let mut fixture = Fixture::start(selected).await.expect("fixture");
        let result: TestResult<()> = async {
            let value = offer(fixture.client()?).await?;
            let mut accept = acceptance(&value);
            match mode {
                "missing" => {}
                "wrong-version" => accept["version"] = json!(1),
                "wrong-session" => accept["session_id"] = json!("00000000000000000000000000000000"),
                "missing-field" => {
                    accept
                        .as_object_mut()
                        .ok_or("accept object")?
                        .remove("chunk_receipts");
                }
                "unaccepted-flush" => accept["flush_on_disconnect"] = json!(false),
                "unknown-field" => accept["invented"] = json!(true),
                _ => return Err("unknown test mode".into()),
            }
            if mode != "missing" {
                send(fixture.client()?, accept).await?;
            }
            let packets = fixture.wait_closed().await?;
            require(packets.is_empty(), "failed negotiation emitted output")
        }
        .await;
        let closed = fixture.close_after_abort().await;
        assert!(
            closed.cleanup_errors.is_empty(),
            "{mode}: {:?}",
            closed.cleanup_errors
        );
        assert!(result.is_ok(), "{mode}: {result:?}");
        assert!(
            closed.server.is_err(),
            "{mode}: missing/wrong negotiation accepted"
        );
        assert_eq!(
            closed.contacts, 0,
            "{mode}: provider contacted before negotiation"
        );
    }
}

#[tokio::test]
async fn legacy_context_ack_cannot_satisfy_an_explicit_v2_chunk() {
    let mut fixture = Fixture::start(options()).await.expect("fixture");
    let result: TestResult<()> = async {
        let session = negotiate(&mut fixture).await?;
        start_user(fixture.client()?).await?;
        let (_, meta) = audio(fixture.client()?, &session).await?;
        send(
            fixture.client()?,
            json!({"type":"playback-complete","context_id":meta["chunk_id"].to_string()}),
        )
        .await?;
        let packets = fixture.wait_closed().await?;
        require(
            !packets.iter().any(|p| matches!(p, Kind::Audio(_))),
            "legacy ACK released more PCM",
        )
    }
    .await;
    let closed = fixture.close_after_abort().await;
    assert!(
        closed.server.is_err(),
        "wrong protocol must retire v2 session"
    );
    check_closed(result, closed, false);
}

#[tokio::test]
async fn invalid_reports_cannot_extend_the_original_chunk_deadline() {
    let mut selected = options();
    selected.receipt.chunk_timeout = Duration::from_millis(250);
    let mut fixture = Fixture::start(selected).await.expect("fixture");
    let result: TestResult<()> = async {
        let session = negotiate(&mut fixture).await?;
        start_user(fixture.client()?).await?;
        let (_, meta) = audio(fixture.client()?, &session).await?;
        let start = Instant::now();
        let mut wrong = receipt(&meta); wrong["chunk_id"] = json!(meta["chunk_id"].as_u64().ok_or("chunk id")? + 1);
        let mut tick = tokio::time::interval(Duration::from_millis(30));
        let deadline = Instant::now() + Duration::from_millis(1100);
        let client = fixture.client()?;
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return Err("invalid reports extended chunk deadline".into()),
                _ = tick.tick() => { let _ = send(client, wrong.clone()).await; },
                packet = client.recv() => match packet {
                    None => break,
                    Some(Packet::Binary(bytes)) => {
                        if let Some(Kind::Audio(_)) = pb::Frame::parse(&bytes).map_err(|e| e.to_string())?.frame { return Err("future ACK released PCM".into()); }
                    }
                    Some(_) => return Err("non-protobuf terminal output".into()),
                }
            }
        }
        require(start.elapsed() < Duration::from_millis(1000), "chunk deadline restarted")
    }.await;
    let closed = fixture.close_after_abort().await;
    assert!(closed.server.is_err(), "missing exact completion must fail");
    check_closed(result, closed, false);
}

#[tokio::test]
async fn end_keeps_receipt_input_alive_until_all_previously_admitted_audio_finishes() {
    let mut fixture = Fixture::start(options()).await.expect("fixture");
    let result: TestResult<()> = async {
        let session = negotiate(&mut fixture).await?;
        start_user(fixture.client()?).await?;
        let (first_pcm, first) = audio(fixture.client()?, &session).await?;
        require(first_pcm.audio == chunk_bytes(0), "first PCM differs")?;
        send(fixture.client()?, json!({"type":"end"})).await?;
        quiet(fixture.client()?, Duration::from_millis(40)).await?;
        send(fixture.client()?, receipt(&first)).await?;
        for index in 1..3 {
            let (pcm, meta) = audio(fixture.client()?, &session).await?;
            require(
                pcm.audio == chunk_bytes(index),
                "End changed preadmitted chunk bytes",
            )?;
            send(fixture.client()?, receipt(&meta)).await?;
        }
        terminal(fixture.client()?, true).await
    }
    .await;
    let closed = fixture.close().await;
    check_closed(result, closed, true);
}

#[tokio::test]
async fn end_duplicate_traffic_cannot_extend_the_fixed_drain_deadline() {
    let mut selected = options();
    selected.receipt.drain_timeout = Duration::from_millis(250);
    let mut fixture = Fixture::start(selected).await.expect("fixture");
    let result: TestResult<()> = async {
        let session = negotiate(&mut fixture).await?;
        start_user(fixture.client()?).await?;
        let (_, first) = audio(fixture.client()?, &session).await?;
        send(fixture.client()?, json!({"type":"end"})).await?;
        let start = Instant::now();
        let mut wrong = receipt(&first); wrong["chunk_id"] = json!(first["chunk_id"].as_u64().ok_or("chunk id")? + 1);
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        let deadline = Instant::now() + Duration::from_millis(1100);
        let client = fixture.client()?;
        loop { tokio::select! {
            _ = tokio::time::sleep_until(deadline) => return Err("End deadline reset by traffic".into()),
            _ = tick.tick() => { let _ = send(client, wrong.clone()).await; },
            packet = client.recv() => match packet {
                None => break,
                Some(Packet::Binary(bytes)) => {
                    if let Some(Kind::Audio(_)) = pb::Frame::parse(&bytes).map_err(|e| e.to_string())?.frame { return Err("future report advanced End PCM".into()); }
                }
                Some(_) => return Err("non-protobuf output".into()),
            }
        }}
        require(start.elapsed() < Duration::from_millis(1000), "fixed drain deadline was extended")
    }.await;
    let closed = fixture.close_after_abort().await;
    assert!(
        closed.server.is_err(),
        "End without pending report must fail closed"
    );
    check_closed(result, closed, false);
}

#[tokio::test]
async fn end_rejects_new_ordinary_input_and_new_negotiation() {
    for renegotiate in [false, true] {
        let mut fixture = Fixture::start(options()).await.expect("fixture");
        let result: TestResult<()> = async {
            let session = negotiate(&mut fixture).await?;
            start_user(fixture.client()?).await?;
            let _ = audio(fixture.client()?, &session).await?;
            send(fixture.client()?, json!({"type":"end"})).await?;
            if renegotiate {
                send(fixture.client()?, acceptance(&session)).await?;
            } else {
                start_user(fixture.client()?).await?;
            }
            let packets = fixture.wait_closed().await?;
            require(
                !packets.iter().any(|p| matches!(p, Kind::Audio(_))),
                "post-End input admitted more PCM",
            )
        }
        .await;
        let closed = fixture.close_after_abort().await;
        assert!(closed.server.is_err(), "post-End input should fail closed");
        check_closed(result, closed, false);
    }
}

#[tokio::test]
async fn interruption_requires_exact_cutoff_before_session_reuse() {
    let mut fixture = Fixture::start(options()).await.expect("fixture");
    let result: TestResult<()> = async {
        let session = negotiate(&mut fixture).await?;
        start_user(fixture.client()?).await?;
        let (_, first) = audio(fixture.client()?, &session).await?;
        send(fixture.client()?, json!({"type":"interrupt"})).await?;
        let cutoff = control(next(fixture.client()?).await?)?;
        require(
            cutoff["type"] == "playback-cutoff"
                && cutoff["session_id"] == session["session_id"]
                && cutoff["all"] == false,
            format!("wrong ordinary cutoff: {cutoff}"),
        )?;
        require(
            cutoff["epoch"].as_u64() > first["epoch"].as_u64(),
            "cutoff did not move epoch",
        )?;
        send(fixture.client()?, receipt(&first)).await?;
        let mut wrong = cutoff_receipt(cutoff.clone());
        wrong["receipt_nonce"] = json!("00000000000000000000000000000000");
        send(fixture.client()?, wrong).await?;
        quiet(fixture.client()?, Duration::from_millis(60)).await?;
        send(fixture.client()?, cutoff_receipt(cutoff)).await?;
        send(fixture.client()?, json!({"type":"end"})).await?;
        terminal(fixture.client()?, false).await
    }
    .await;
    // Interruption deliberately cancels the in-progress provider stream.
    // Its exact peer-reset outcome is allowed and retained; joins still must succeed.
    let closed = fixture.close_after_abort().await;
    assert!(closed.server.is_ok(), "{:?}", closed.server.as_ref().err());
    if let Ok(report) = &closed.server {
        assert!(
            !report
                .context
                .messages
                .iter()
                .any(|m| m.role == Role::Assistant),
            "revoked client report committed speech"
        );
    }
    check_closed(result, closed, false);
}

#[tokio::test]
async fn missing_cutoff_report_times_out_and_joins_the_owner() {
    let mut selected = options();
    selected.receipt.cutoff_timeout = Duration::from_millis(150);
    let mut fixture = Fixture::start(selected).await.expect("fixture");
    let result: TestResult<()> = async {
        let session = negotiate(&mut fixture).await?;
        start_user(fixture.client()?).await?;
        let (_, first) = audio(fixture.client()?, &session).await?;
        send(fixture.client()?, json!({"type":"interrupt"})).await?;
        let cutoff = control(next(fixture.client()?).await?)?;
        require(cutoff["type"] == "playback-cutoff", "missing cutoff")?;
        send(fixture.client()?, receipt(&first)).await?;
        let packets = fixture.wait_closed().await?;
        require(
            !packets.iter().any(|p| matches!(p, Kind::Audio(_))),
            "old chunk ACK bypassed required cutoff",
        )
    }
    .await;
    let closed = fixture.close_after_abort().await;
    assert!(
        closed.server.is_err(),
        "missing flush report must retire session"
    );
    check_closed(result, closed, false);
}

#[tokio::test]
async fn cancel_and_disconnect_settle_handshake_chunk_and_cutoff_stages() {
    for phase in ["handshake", "negotiated", "chunk", "cutoff"] {
        for disconnect in [false, true] {
            let mut fixture = Fixture::start(options()).await.expect("fixture");
            let result: TestResult<()> = async {
                if phase == "handshake" {
                    let _ = offer(fixture.client()?).await?;
                } else {
                    let session = negotiate(&mut fixture).await?;
                    if phase != "negotiated" {
                        start_user(fixture.client()?).await?;
                        let _ = audio(fixture.client()?, &session).await?;
                        if phase == "cutoff" {
                            send(fixture.client()?, json!({"type":"interrupt"})).await?;
                            require(
                                control(next(fixture.client()?).await?)?["type"]
                                    == "playback-cutoff",
                                "missing cutoff before cancellation",
                            )?;
                        }
                    }
                }
                if !disconnect {
                    send(fixture.client()?, json!({"type":"cancel"})).await?;
                    let packets = fixture.wait_closed().await?;
                    require(
                        !packets.iter().any(|p| matches!(p, Kind::Audio(_))),
                        "Cancel released PCM",
                    )?;
                }
                Ok(())
            }
            .await;
            let closed = fixture.close_after_abort().await;
            assert!(
                closed.cleanup_errors.is_empty(),
                "{phase}, disconnect={disconnect}: {:?}",
                closed.cleanup_errors
            );
            assert!(
                result.is_ok(),
                "{phase}, disconnect={disconnect}: {result:?}"
            );
            if let Ok(report) = &closed.server {
                assert!(
                    !report
                        .context
                        .messages
                        .iter()
                        .any(|m| m.role == Role::Assistant),
                    "cancelled/unreported audio committed history"
                );
            }
        }
    }
}

#[tokio::test]
async fn default_legacy_public_smoke_is_unchanged() {
    let report = pipecat_cli::smoke::run()
        .await
        .expect("old real-socket smoke");
    assert_eq!(report.audio_bytes, 8);
}

#[test]
fn intentional_peer_abort_classifies_only_typed_transport_shutdowns() {
    use pipecat_transports::TransportError;
    let io = |kind, text| TransportError::WebSocket(std::io::Error::new(kind, text).into());
    assert!(
        peer_exit(io(std::io::ErrorKind::ConnectionReset, "synthetic reset"))
            .starts_with("expected peer shutdown: ")
    );
    for error in [
        io(
            std::io::ErrorKind::Other,
            "Connection reset by peer (os error 104)",
        ),
        io(
            std::io::ErrorKind::InvalidData,
            "Connection reset without closing handshake",
        ),
        io(std::io::ErrorKind::Other, "Connection closed normally"),
        io(
            std::io::ErrorKind::Other,
            "Trying to work with closed connection",
        ),
        io(std::io::ErrorKind::BrokenPipe, "synthetic broken pipe"),
        TransportError::Timeout("provider join"),
        TransportError::Backpressure,
        TransportError::TooLarge,
        TransportError::InvalidConfig,
    ] {
        assert!(!peer_exit(error).starts_with("expected peer shutdown: "));
    }
}
