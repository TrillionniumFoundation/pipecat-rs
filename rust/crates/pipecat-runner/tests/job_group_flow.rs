//! Typed native RTVI -> real loopback transport -> shared browser projection.
//! No paid providers, browser DOM, external signaling, STUN, or TURN are involved.
use pipecat_browser_client::{job_groups::JobGroups, protocol as browser_wire};
use pipecat_protocol::{
    protobuf,
    rtvi::{Envelope, ServerEvent, UiJobGroupData},
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
#[cfg(feature = "webrtc")]
use pipecat_webrtc::{SmallWebRtcConfig, SmallWebRtcConnection, TransportEvent};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::net::TcpListener;

type Outcome<T = ()> = Result<T, String>;
const DEADLINE: Duration = Duration::from_secs(20);
const STATUS: &str = "vendor:partial/未知 <script>literal</script>";
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
fn started(id: &str, cancellable: bool) -> UiJobGroupData {
    UiJobGroupData::GroupStarted {
        job_id: id.into(),
        workers: Some(vec!["worker".into()]),
        label: Some(format!("组 {id} <b>literal</b>")),
        cancellable,
        at: 41,
    }
}
fn update(id: &str, at: u64) -> UiJobGroupData {
    UiJobGroupData::JobUpdate {
        job_id: id.into(),
        worker_name: "worker".into(),
        data: Some(
            json!({"html":"<img src=x onerror=alert(1)>","unicode":"你好 🦀","items":[null,7,true]}),
        ),
        at,
    }
}
fn completed(id: &str) -> UiJobGroupData {
    UiJobGroupData::JobCompleted {
        job_id: id.into(),
        worker_name: "worker".into(),
        status: STATUS.into(),
        response: Some(json!(["<script>literal</script>", {"answer":"世界", "ok":false}])),
        at: 3,
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
async fn received(sender: &Peer, receiver: &mut Peer, event: UiJobGroupData) -> Outcome<Value> {
    let envelope = Envelope::server(&ServerEvent::UiJobGroup(event)).map_err(error)?;
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
    model: &mut JobGroups,
    event: UiJobGroupData,
    changed: bool,
) -> Outcome<Value> {
    let actual = received(sender, receiver, event).await?;
    ensure(
        model.apply(7, &actual)? == changed,
        "unexpected projection transition",
    )?;
    Ok(actual)
}
async fn observe(sender: &Peer, receiver: &mut Peer) -> Outcome {
    let mut model = JobGroups::default();
    model.begin(7);
    apply(sender, receiver, &mut model, started("a", true), true).await?;
    apply(sender, receiver, &mut model, started("b", false), true).await?;
    let update_a = apply(sender, receiver, &mut model, update("a", 90), true).await?;
    apply(sender, receiver, &mut model, update("b", 12), true).await?;
    let completed_a = apply(sender, receiver, &mut model, completed("a"), true).await?;
    ensure(model.groups().len() == 2, "interleaved groups lost")?;
    let a = &model.groups()[0];
    let b = &model.groups()[1];
    ensure(
        a.job_id == "a" && b.job_id == "b" && a.cancellable && !b.cancellable,
        "group identities/metadata mixed",
    )?;
    ensure(
        a.label.as_deref() == Some("组 a <b>literal</b>") && a.at == 41,
        "start metadata changed",
    )?;
    ensure(
        a.workers[0].name == "worker" && a.workers[0].at == 3,
        "timestamps incorrectly reordered events",
    )?;
    ensure(
        a.workers[0].status.as_deref() == Some(STATUS) && b.workers[0].status.is_none(),
        "arbitrary terminal status lost or mixed",
    )?;
    ensure(
        a.workers[0].data == Some(update_a["data"]["data"].to_string()),
        "JSON data is not exact plaintext serialization",
    )?;
    ensure(
        a.workers[0].response == Some(completed_a["data"]["response"].to_string()),
        "JSON response is not exact plaintext serialization",
    )?;
    let terminal_worker = a.clone();
    apply(sender, receiver, &mut model, update("a", 999), false).await?;
    ensure(
        model.groups()[0] == terminal_worker,
        "late update revived terminal worker",
    )?;
    apply(
        sender,
        receiver,
        &mut model,
        UiJobGroupData::GroupCompleted {
            job_id: "a".into(),
            at: 1,
        },
        true,
    )
    .await?;
    let terminal_group = model.groups()[0].clone();
    apply(sender, receiver, &mut model, update("a", 1000), false).await?;
    apply(sender, receiver, &mut model, started("a", true), false).await?;
    ensure(
        model.groups()[0] == terminal_group && terminal_group.completed && terminal_group.at == 1,
        "late event revived completed group",
    )?;
    ensure(
        !model.groups()[1].completed,
        "completion leaked into interleaved group",
    )?;
    apply(sender, receiver, &mut model, completed("b"), true).await?;
    apply(
        sender,
        receiver,
        &mut model,
        UiJobGroupData::GroupCompleted {
            job_id: "b".into(),
            at: 2,
        },
        true,
    )
    .await?;
    ensure(
        model.groups().iter().all(|group| group.completed) && !model.is_incomplete(),
        "complete projection unexpectedly missing data",
    )?;
    model.begin(8);
    ensure(
        model.groups().is_empty() && !model.is_incomplete(),
        "epoch rollover retained old groups",
    )?;
    let fresh = received(sender, receiver, started("a", true)).await?;
    ensure(
        !model.apply(7, &fresh)? && model.groups().is_empty(),
        "old epoch accepted after reconnect",
    )?;
    ensure(
        model.apply(8, &fresh)? && model.groups().len() == 1 && !model.groups()[0].completed,
        "fresh epoch cannot reuse group ID",
    )?;
    model.close();
    ensure(
        model.groups().is_empty() && !model.apply(8, &fresh)?,
        "closed model accepted delayed callback",
    )
}
async fn close_owned(peer: Option<Peer>) -> Outcome {
    if let Some(peer) = peer {
        peer.close().await
    } else {
        Ok(())
    }
}

#[tokio::test]
async fn native_job_groups_cross_real_websocket_into_browser_model() {
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
async fn native_job_groups_cross_real_webrtc_into_browser_model() {
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
