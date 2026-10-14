//! Typed native RTVI -> real loopback transport -> shared browser projection.
//! No paid providers, browser DOM, external signaling, STUN, or TURN are involved.
use pipecat_browser_client::{protocol as browser_wire, toasts::Toasts};
use pipecat_protocol::{
    protobuf,
    rtvi::{Envelope, ServerEvent, UiCommandData},
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
#[cfg(feature = "webrtc")]
use pipecat_webrtc::{SmallWebRtcConfig, SmallWebRtcConnection, TransportEvent};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::net::TcpListener;

type Outcome<T = ()> = Result<T, String>;
const DEADLINE: Duration = Duration::from_secs(20);
const TITLE: &str = "你好 <script>literal</script> 🦀";
const SUBTITLE: &str = "<img src=x onerror=alert(1)>";
const DESCRIPTION: &str = "A & B <b>plain text</b>";
const IMAGE: &str = "https://remote.invalid/tracker.png?private=never-fetch";
fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn ensure(condition: bool, message: &str) -> Outcome {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn toast(payload: Value) -> UiCommandData {
    UiCommandData {
        command: "toast".into(),
        payload: Some(payload),
    }
}
enum Peer {
    Ws(WebSocketSession),
    #[cfg(feature = "webrtc")]
    Rtc(SmallWebRtcConnection),
}
impl Peer {
    async fn send(&self, envelope: &Envelope) -> Outcome {
        let text = envelope.encode().map_err(error)?;
        match self {
            Self::Ws(socket) => {
                // Native protobuf encoder; the receiving endpoint uses the browser decoder.
                let bytes = protobuf::Frame {
                    frame: Some(protobuf::frame::Kind::Message(protobuf::MessageFrame {
                        data: text,
                    })),
                }
                .encode_checked()
                .map_err(error)?;
                socket
                    .sender()
                    .send(Packet::Binary(bytes.into()))
                    .await
                    .map_err(error)
            }
            #[cfg(feature = "webrtc")]
            Self::Rtc(peer) => peer
                .send_app_message(&serde_json::from_str::<Value>(&text).map_err(error)?)
                .await
                .map_err(error),
        }
    }
    async fn recv(&mut self) -> Outcome<Value> {
        match self {
            Self::Ws(socket) => {
                let Packet::Binary(bytes) = socket
                    .recv()
                    .await
                    .ok_or("WebSocket closed before message")?
                else {
                    return Err("expected browser protobuf binary message".into());
                };
                let Some(browser_wire::frame::Body::Message(message)) =
                    browser_wire::decode(&bytes).map_err(error)?.body
                else {
                    return Err("expected browser message frame".into());
                };
                serde_json::from_str(&message.data).map_err(error)
            }
            #[cfg(feature = "webrtc")]
            Self::Rtc(peer) => loop {
                match peer
                    .recv_event()
                    .await
                    .ok_or("WebRTC closed before message")?
                {
                    TransportEvent::AppMessage(value) => return Ok(value),
                    TransportEvent::Error(error) => return Err(error),
                    _ => {}
                }
            },
        }
    }
    async fn close(self) -> Outcome {
        // These APIs explicitly close and join owned transport workers and have
        // configured I/O/close deadlines. Do not cancel their cleanup futures.
        match self {
            Self::Ws(socket) => socket.finish().await.map_err(error),
            #[cfg(feature = "webrtc")]
            Self::Rtc(peer) => peer.close().await.map_err(error),
        }
    }
}
async fn received(sender: &Peer, receiver: &mut Peer, event: UiCommandData) -> Outcome<Value> {
    let envelope = Envelope::server(&ServerEvent::UiCommand(event)).map_err(error)?;
    let expected = serde_json::to_value(&envelope).map_err(error)?;
    sender.send(&envelope).await?;
    let actual = receiver.recv().await?;
    ensure(
        actual == expected,
        "native typed envelope changed across actual transport",
    )?;
    Ok(actual)
}
async fn apply(
    sender: &Peer,
    receiver: &mut Peer,
    model: &mut Toasts,
    now: u64,
    payload: Value,
) -> Outcome<Value> {
    let actual = received(sender, receiver, toast(payload)).await?;
    ensure(
        model.apply(7, now, &actual)?,
        "toast did not change projection",
    )?;
    Ok(actual)
}
async fn observe(sender: &Peer, receiver: &mut Peer) -> Outcome {
    let mut model = Toasts::default();
    model.begin(7);
    let original = apply(
        sender,
        receiver,
        &mut model,
        100,
        json!({
            "title":TITLE, "subtitle":SUBTITLE,
            "description":DESCRIPTION, "image_url":IMAGE
        }),
    )
    .await?;
    ensure(
        original["data"]["payload"]["image_url"] == IMAGE,
        "native image URL lost on wire",
    )?;
    let a = &model.entries()[0];
    let a_id = a.id;
    ensure(
        a.title == TITLE
            && a.subtitle.as_deref() == Some(SUBTITLE)
            && a.description.as_deref() == Some(DESCRIPTION),
        "toast text was changed or interpreted",
    )?;
    ensure(
        a.image_omitted && a.expires_at_ms == 5100,
        "image omission/default duration changed",
    )?;
    ensure(
        !format!("{a:?}").contains(IMAGE),
        "remote URL retained in browser projection",
    )?;
    apply(
        sender,
        receiver,
        &mut model,
        100,
        json!({"title":"B", "duration_ms":1}),
    )
    .await?;
    apply(
        sender,
        receiver,
        &mut model,
        100,
        json!({"title":"C", "duration_ms":30000}),
    )
    .await?;
    apply(sender, receiver, &mut model, 100, json!({"title":"D"})).await?;
    ensure(model.entries().len() == 4, "four toasts were not retained")?;
    let b_id = model.entries()[1].id;
    let c_id = model.entries()[2].id;
    ensure(
        model.entries()[1].expires_at_ms == 101 && model.entries()[2].expires_at_ms == 30100,
        "explicit duration bounds changed",
    )?;
    ensure(
        !model.entries()[1].image_omitted,
        "missing image marked omitted",
    )?;
    // Validate before capacity eviction, including every duration type/boundary.
    let before = model.entries().to_vec();
    for duration in [
        json!(0),
        json!(-1),
        json!(30001),
        json!(1.5),
        json!("1000"),
        json!(true),
    ] {
        let invalid = received(
            sender,
            receiver,
            toast(json!({"title":"bad", "duration_ms":duration})),
        )
        .await?;
        ensure(
            model.apply(7, 100, &invalid).is_err(),
            "invalid duration accepted",
        )?;
        ensure(
            model.entries() == before.as_slice(),
            "invalid duration mutated or evicted existing toast",
        )?;
    }
    for (field, value) in [
        ("title", "x".repeat(513)),
        ("subtitle", "x".repeat(1025)),
        ("description", "x".repeat(4097)),
        ("image_url", "x".repeat(2049)),
    ] {
        let mut payload = json!({"title":"bad"});
        payload[field] = json!(value);
        let invalid = received(sender, receiver, toast(payload)).await?;
        ensure(
            model.apply(7, 100, &invalid).is_err() && model.entries() == before.as_slice(),
            "overlong field accepted or evicted existing toast",
        )?;
    }
    let oversized = received(
        sender,
        receiver,
        toast(json!({"title":"bad", "extra":"x".repeat(8192)})),
    )
    .await?;
    ensure(
        model.apply(7, 100, &oversized).is_err() && model.entries() == before.as_slice(),
        "oversized wire accepted or evicted existing toast",
    )?;
    for command in [
        "navigate",
        "scroll_to",
        "highlight",
        "focus",
        "click",
        "set_input_value",
        "select_text",
    ] {
        let unrelated = received(
            sender,
            receiver,
            UiCommandData {
                command: command.into(),
                payload: Some(json!({"title":"bad", "url":IMAGE})),
            },
        )
        .await?;
        ensure(
            !model.apply(7, 100, &unrelated)? && model.entries() == before.as_slice(),
            "unrelated UI command changed toasts",
        )?;
    }
    apply(sender, receiver, &mut model, 100, json!({"title":"E"})).await?;
    ensure(
        model.entries().len() == 4 && !model.entries().iter().any(|t| t.id == a_id),
        "bounded toast capacity did not evict oldest",
    )?;
    ensure(
        !model.expire(6, 30100) && !model.dismiss(6, b_id),
        "stale epoch mutated toasts",
    )?;
    ensure(!model.expire(7, 100), "toast expired before deadline")?;
    ensure(
        model.expire(7, 101) && !model.entries().iter().any(|t| t.id == b_id),
        "exact-deadline expiry failed",
    )?;
    ensure(
        model.dismiss(7, c_id) && !model.dismiss(7, c_id),
        "explicit dismissal failed",
    )?;
    apply(
        sender,
        receiver,
        &mut model,
        200,
        json!({"title":"duplicate", "duration_ms":10}),
    )
    .await?;
    apply(
        sender,
        receiver,
        &mut model,
        200,
        json!({"title":"duplicate", "duration_ms":10}),
    )
    .await?;
    let duplicates: Vec<_> = model
        .entries()
        .iter()
        .filter(|t| t.title == "duplicate")
        .collect();
    ensure(
        duplicates.len() == 2 && duplicates[0].id != duplicates[1].id,
        "duplicate notifications were deduplicated",
    )?;
    ensure(
        !model.dismiss(7, b_id) && !model.expire(6, 210),
        "old callback affected newer notifications",
    )?;
    ensure(
        model.expire(7, 210) && !model.entries().iter().any(|t| t.title == "duplicate"),
        "duplicate notifications did not expire",
    )?;
    model.begin(8);
    ensure(model.entries().is_empty(), "reconnect retained old toasts")?;
    ensure(
        !model.apply(7, 300, &original)? && !model.expire(7, u64::MAX) && !model.dismiss(7, a_id),
        "old epoch callback accepted",
    )?;
    ensure(
        model.apply(8, 300, &original)? && model.entries()[0].expires_at_ms == 5300,
        "new epoch rejected valid toast",
    )?;
    let new_id = model.entries()[0].id;
    // Even when a reconnect reuses the local ID, old-epoch callbacks are inert.
    ensure(
        !model.dismiss(7, new_id) && !model.expire(7, u64::MAX) && model.entries().len() == 1,
        "stale callback affected current-session ID",
    )?;
    model.close();
    ensure(
        model.entries().is_empty()
            && !model.apply(8, 400, &original)?
            && !model.expire(8, u64::MAX)
            && !model.dismiss(8, new_id),
        "closed model accepted delayed callback",
    )?;
    model.begin(9);
    let optional_nulls = received(
        sender,
        receiver,
        toast(json!({
            "title":"null optional fields", "subtitle":null, "description":null,
            "image_url":null, "duration_ms":null
        })),
    )
    .await?;
    ensure(
        model.apply(9, 600, &optional_nulls)?,
        "null optional fields rejected",
    )?;
    let null_toast = &model.entries()[0];
    ensure(
        null_toast.subtitle.is_none()
            && null_toast.description.is_none()
            && !null_toast.image_omitted
            && null_toast.expires_at_ms == 5600,
        "optional null fields changed source defaults",
    )?;
    let boundary = received(
        sender,
        receiver,
        toast(json!({
            "title":"x".repeat(512), "subtitle":"x".repeat(1024),
            "description":"x".repeat(4096), "image_url":"x".repeat(2048), "duration_ms":1
        })),
    )
    .await?;
    ensure(
        model.apply(9, 600, &boundary)? && model.entries().len() == 2,
        "valid field byte boundaries rejected",
    )?;
    ensure(
        model.entries()[1].title.len() == 512 && model.entries()[1].image_omitted,
        "boundary text or omitted-image marker lost",
    )?;
    model.close();
    Ok(())
}
async fn close_owned(peer: Option<Peer>) -> Outcome {
    if let Some(peer) = peer {
        peer.close().await
    } else {
        Ok(())
    }
}

#[tokio::test]
async fn native_toasts_cross_real_websocket_into_browser_model() {
    let (mut server, mut client) = (None, None);
    let observation = tokio::time::timeout(DEADLINE, async {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(error)?;
        let url = format!("ws://{}", listener.local_addr().map_err(error)?);
        let config = SessionConfig {
            connect_timeout: Duration::from_secs(3),
            write_timeout: Duration::from_secs(3),
            close_timeout: Duration::from_secs(3),
            ..Default::default()
        };
        // No detached accept task: both successful endpoints are retained even
        // when the other handshake fails or the observation deadline expires.
        let (accepted, connected) = tokio::join!(
            async {
                let (stream, _) = listener.accept().await.map_err(error)?;
                server = Some(Peer::Ws(
                    WebSocketSession::accept(stream, config.clone())
                        .await
                        .map_err(error)?,
                ));
                Ok::<(), String>(())
            },
            async {
                client = Some(Peer::Ws(
                    WebSocketSession::connect(&url, config.clone())
                        .await
                        .map_err(error)?,
                ));
                Ok::<(), String>(())
            }
        );
        accepted?;
        connected?;
        observe(
            server.as_ref().ok_or("missing server")?,
            client.as_mut().ok_or("missing client")?,
        )
        .await
    })
    .await;
    // Run every cleanup before asserting any observation or cleanup outcome.
    let (server_close, client_close) = tokio::join!(close_owned(server), close_owned(client));
    assert!(server_close.is_ok() && client_close.is_ok(), "socket close/join: server={server_close:?}; client={client_close:?}; observation={observation:?}");
    assert!(
        matches!(observation, Ok(Ok(()))),
        "observation: {observation:?}"
    );
}

#[cfg(feature = "webrtc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_toasts_cross_real_webrtc_into_browser_model() {
    let (mut server, mut client) = (None, None);
    let observation = tokio::time::timeout(DEADLINE, async {
        let config = SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: None,
            close_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        server = Some(Peer::Rtc(
            SmallWebRtcConnection::new(config.clone())
                .await
                .map_err(error)?,
        ));
        client = Some(Peer::Rtc(
            SmallWebRtcConnection::new(config).await.map_err(error)?,
        ));
        let (Some(Peer::Rtc(a)), Some(Peer::Rtc(b))) = (&server, &client) else {
            return Err("missing owned peers".into());
        };
        let offer = a.create_offer().await.map_err(error)?;
        ensure(
            offer.sdp.contains("127.0.0.1")
                && !offer.sdp.contains(" typ srflx")
                && !offer.sdp.contains(" typ relay"),
            "expected loopback-only ICE",
        )?;
        let answer = b.answer_offer(offer.sdp).await.map_err(error)?;
        a.accept_answer(answer).await.map_err(error)?;
        let (a_ready, b_ready) = tokio::join!(
            a.wait_data_channel(Duration::from_secs(5)),
            b.wait_data_channel(Duration::from_secs(5))
        );
        a_ready.map_err(error)?;
        b_ready.map_err(error)?;
        observe(
            server.as_ref().ok_or("missing server")?,
            client.as_mut().ok_or("missing client")?,
        )
        .await
    })
    .await;
    let (server_close, client_close) = tokio::join!(close_owned(server), close_owned(client));
    assert!(server_close.is_ok() && client_close.is_ok(), "peer close/join: server={server_close:?}; client={client_close:?}; observation={observation:?}");
    assert!(
        matches!(observation, Ok(Ok(()))),
        "observation: {observation:?}"
    );
}
