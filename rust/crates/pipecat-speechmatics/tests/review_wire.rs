//! Independent Agent websocket lifecycle and counter regressions.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use pipecat_speechmatics::*;
use serde_json::{json, Value};
use std::{future::Future, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        handshake::server::{Callback, ErrorResponse, Request, Response},
        Message,
    },
};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("Speechmatics wire fixture stalled")
}
fn audio(n: usize) -> AudioRawFrame {
    AudioRawFrame::new(vec![0; n], 16000, 1).unwrap()
}
async fn event(session: &mut SpeechmaticsSttSession) -> SpeechmaticsSttEvent {
    bounded(session.next()).await.unwrap().unwrap()
}
async fn transcript(session: &mut SpeechmaticsSttSession) -> SpeechmaticsTranscript {
    loop {
        if let SpeechmaticsSttEvent::Transcript(t) = event(session).await {
            return t;
        }
    }
}
struct VerifyHandshake;
impl Callback for VerifyHandshake {
    fn on_request(self, request: &Request, response: Response) -> Result<Response, ErrorResponse> {
        assert_eq!(request.uri().path(), "/v2/agent");
        assert_eq!(request.headers()["authorization"], "Bearer fixture-secret");
        let id = request.headers()["x-request-id"].to_str().unwrap();
        assert!(!id.is_empty());
        let uri = request.uri().to_string();
        assert!(uri.contains("sm-app="));
        assert!(uri.contains("sm-sdk="));
        assert!(!uri.contains("python"));
        Ok(response)
    }
}
struct Peer {
    config: SpeechmaticsSttConfig,
    send: mpsc::Sender<Value>,
    wire: mpsc::Receiver<Message>,
    task: tokio::task::JoinHandle<()>,
}
async fn peer(capacity: usize) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechmaticsSttConfig::new("fixture-secret");
    config.endpoint = format!("ws://{}/v2//", listener.local_addr().unwrap());
    config.limits.queue_capacity = capacity;
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_millis(500);
    config.limits.output_timeout = Duration::from_millis(500);
    let (send, mut input) = mpsc::channel::<Value>(32);
    let (tx, wire) = mpsc::channel(32);
    let task = tokio::spawn(async move {
        let mut socket = accept_hdr_async(listener.accept().await.unwrap().0, VerifyHandshake)
            .await
            .unwrap();
        let start = socket.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(start.to_text().unwrap()).unwrap()["message"],
            "StartRecognition"
        );
        socket
            .send(Message::Text(
                json!({"message":"RecognitionStarted","id":"fixture-session"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        loop {
            tokio::select! {
                value=input.recv()=>{let Some(value)=value else{break};if socket.send(Message::Text(value.to_string().into())).await.is_err(){break}},
                value=socket.next()=>match value{Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,Some(Ok(message))=>{
                    let eos=message.to_text().ok().and_then(|s|serde_json::from_str::<Value>(s).ok()).is_some_and(|v|v["message"]=="EndOfStream");
                    if tx.send(message).await.is_err(){break}
                    if eos{let _=socket.send(Message::Text(json!({"message":"EndOfTranscript"}).to_string().into())).await;}
                }}
            }
        }
    });
    Peer {
        config,
        send,
        wire,
        task,
    }
}

#[tokio::test]
async fn review_delayed_ack_never_replaces_sent_count_in_end_of_stream() {
    let mut peer = peer(16).await;
    let mut session = SpeechmaticsSttClient::new(peer.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    assert!(matches!(
        event(&mut session).await,
        SpeechmaticsSttEvent::Ready { .. }
    ));
    for len in [640, 960, 320] {
        sender.try_send_audio(audio(len)).unwrap();
    }
    for expected in 1..=3 {
        assert!(
            matches!(event(&mut session).await,SpeechmaticsSttEvent::AudioSent{sent_seq,..}if sent_seq==expected)
        );
    }
    peer.send
        .send(json!({"message":"AudioAdded","seq_no":1}))
        .await
        .unwrap();
    assert!(matches!(
        event(&mut session).await,
        SpeechmaticsSttEvent::AudioAdded { acked_seq: 1 }
    ));
    assert_eq!(sender.snapshot().sent_seq, 3);
    assert_eq!(sender.snapshot().acked_seq, 1);
    peer.send
        .send(json!({"message":"AudioAdded","seq_no":0}))
        .await
        .unwrap();
    assert!(matches!(
        event(&mut session).await,
        SpeechmaticsSttEvent::AudioAdded { acked_seq: 1 }
    ));
    sender.finish().await.unwrap();
    for len in [640, 960, 320] {
        assert_eq!(
            bounded(peer.wire.recv()).await.unwrap(),
            Message::Binary(vec![0; len].into())
        );
    }
    let eos = bounded(peer.wire.recv()).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(eos.to_text().unwrap()).unwrap(),
        json!({"message":"EndOfStream","last_seq_no":3}),
        "last_seq_no must count sent messages independently of delayed ACKs"
    );
    assert!(sender.try_send_audio(audio(640)).is_err());
    assert!(sender.try_start_turn().is_err());
    assert!(sender.try_finalize().is_err());
    assert!(matches!(
        event(&mut session).await,
        SpeechmaticsSttEvent::EndOfTranscript
    ));
    assert!(bounded(session.next()).await.is_none());
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_finalize_boundary_and_final_segment_are_independent_of_finalized_confirmation() {
    let mut peer = peer(16).await;
    let mut session = SpeechmaticsSttClient::new(peer.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    event(&mut session).await;
    sender.try_send_audio(audio(640)).unwrap();
    sender.try_finalize().unwrap();
    sender.try_send_audio(audio(960)).unwrap();
    assert_eq!(
        bounded(peer.wire.recv()).await.unwrap(),
        Message::Binary(vec![0; 640].into())
    );
    let finalize = bounded(peer.wire.recv()).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(finalize.to_text().unwrap()).unwrap(),
        json!({"message":"ForceEndOfUtterance","timestamp":0.02})
    );
    assert_eq!(
        bounded(peer.wire.recv()).await.unwrap(),
        Message::Binary(vec![0; 960].into())
    );
    peer.send
        .send(json!({"message":"AddPartialSegment","segment":{"transcript":"preview"}}))
        .await
        .unwrap();
    peer.send
        .send(json!({"message":"AddSegment","segment":null,"metadata":null}))
        .await
        .unwrap();
    peer.send.send(json!({"message":"AddSegment","segment":{"transcript":"first","speaker":"S1"},"metadata":{"start_time":0.0,"end_time":0.02}})).await.unwrap();
    peer.send
        .send(json!({"message":"AddSegment","segment":{"transcript":"second"}}))
        .await
        .unwrap();
    let partial = transcript(&mut session).await;
    assert!(!partial.is_final && !partial.finalized);
    assert_eq!(partial.text, "preview");
    let first = transcript(&mut session).await;
    assert!(first.is_final && first.finalized);
    assert_eq!(first.text, "first");
    assert_eq!(first.speaker.as_deref(), Some("S1"));
    assert_eq!(first.end_time, 0.02);
    let second = transcript(&mut session).await;
    assert!(second.is_final && !second.finalized);
    assert_eq!(second.text, "second");
    sender.try_finalize().unwrap();
    bounded(peer.wire.recv()).await.unwrap();
    sender.try_start_turn().unwrap();
    peer.send
        .send(json!({"message":"AddSegment","segment":{"transcript":"new turn"}}))
        .await
        .unwrap();
    let fresh = transcript(&mut session).await;
    assert!(fresh.is_final && !fresh.finalized);
    session.cancel();
    assert_eq!(
        bounded(session.next()).await.unwrap().unwrap_err(),
        StreamingError::Cancelled
    );
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_full_event_queue_does_not_let_audio_command_erase_consumed_transcript() {
    let peer = peer(1).await;
    let mut session = SpeechmaticsSttClient::new(peer.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    // Ready occupies the sole output slot. A final then waits for output admission.
    peer.send
        .send(json!({"message":"AddSegment","segment":{"transcript":"must survive pressure"}}))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    session.sender().try_send_audio(audio(640)).unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(
        event(&mut session).await,
        SpeechmaticsSttEvent::Ready { .. }
    ));
    let next = event(&mut session).await;
    assert!(
        matches!(next,SpeechmaticsSttEvent::Transcript(ref t)if t.text=="must survive pressure"),
        "a ready command erased an already consumed provider event: {next:?}"
    );
    assert!(matches!(
        event(&mut session).await,
        SpeechmaticsSttEvent::AudioSent { .. }
    ));
    session.cancel();
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_unknown_message_flood_cannot_extend_recognition_ready_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechmaticsSttConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/agent", listener.local_addr().unwrap());
    config.limits.ready_timeout = Duration::from_millis(80);
    config.limits.close_timeout = Duration::from_millis(20);
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        socket.next().await.unwrap().unwrap();
        loop {
            tokio::select! {_=tokio::time::sleep(Duration::from_millis(5))=>{if socket.send(Message::Text(json!({"message":"UnknownFixture"}).to_string().into())).await.is_err(){break}},value=socket.next()=>if !matches!(value,Some(Ok(Message::Pong(_)))){break}}
        }
    });
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        SpeechmaticsSttClient::new(config).unwrap().connect(),
    )
    .await
    .expect("ready deadline was extended by unrelated messages");
    let error = match result {
        Ok(_) => panic!("unready provider accepted"),
        Err(e) => e,
    };
    assert!(matches!(error, StreamingError::Timeout(_)));
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn review_dropped_connect_and_session_own_socket_despite_live_sender() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechmaticsSttConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/agent", listener.local_addr().unwrap());
    config.limits.ready_timeout = Duration::from_secs(30);
    let (seen, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        socket.next().await.unwrap().unwrap();
        seen.send(()).unwrap();
        while let Some(Ok(m)) = socket.next().await {
            if m.is_close() {
                break;
            }
        }
    });
    let client = SpeechmaticsSttClient::new(config).unwrap();
    let connect = tokio::spawn(async move { client.connect().await });
    received.await.unwrap();
    connect.abort();
    let _ = connect.await;
    bounded(server).await.unwrap();
    let peer = peer(4).await;
    let session = SpeechmaticsSttClient::new(peer.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    drop(session);
    bounded(peer.task).await.unwrap();
    assert!(sender.try_send_audio(audio(640)).is_err());
}

#[tokio::test]
async fn review_cancel_interrupts_output_pressure_and_terminal_next_stays_retryable() {
    let peer = peer(1).await;
    let mut session = SpeechmaticsSttClient::new(peer.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    peer.send
        .send(json!({"message":"AddSegment","segment":{"transcript":"queued private text"}}))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    session.cancel();
    // The actor may still be in its bounded close. Dropping next cannot consume its terminal state.
    let first = tokio::time::timeout(Duration::ZERO, session.next()).await;
    match first {
        Ok(Some(Err(error))) => assert_eq!(error, StreamingError::Cancelled),
        Err(_) => assert_eq!(
            tokio::time::timeout(Duration::from_millis(300), session.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err(),
            StreamingError::Cancelled
        ),
        other => panic!("cancel exposed queued output or lost its terminal error: {other:?}"),
    }
    assert!(bounded(session.next()).await.is_none());
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_cancel_preempts_blocked_audio_write_and_does_not_wait_write_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechmaticsSttConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/agent", listener.local_addr().unwrap());
    config.limits.max_message_bytes = 4 * 1024 * 1024;
    config.limits.max_buffered_audio_bytes = 8 * 1024 * 1024;
    config.limits.write_timeout = Duration::from_secs(30);
    config.limits.close_timeout = Duration::from_millis(30);
    let (release, hold) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Text(
                json!({"message":"RecognitionStarted"}).to_string().into(),
            ))
            .await
            .unwrap();
        let _ = hold.await;
        drop(socket);
    });
    let mut session = SpeechmaticsSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    event(&mut session).await;
    session
        .sender()
        .try_send_audio(audio(4 * 1024 * 1024))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    session.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(400), session.next())
            .await
            .expect("cancellation waited for the thirty-second write timeout")
            .unwrap()
            .unwrap_err(),
        StreamingError::Cancelled
    );
    release.send(()).unwrap();
    bounded(server).await.unwrap();
}
