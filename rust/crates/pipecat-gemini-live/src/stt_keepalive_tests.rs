//! Private actor boundary tests still use real loopback WebSockets.
use super::*;
use std::time::Duration;

#[tokio::test]
async fn keepalive_write_failure_is_terminal_and_never_reconnects() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GeminiConfig::transcription("fixture-key", "fixture-model");
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        assert!(matches!(ws.next().await, Some(Ok(Message::Text(_)))));
        ws.send(Message::Text(r#"{"setupComplete":{}}"#.into()))
            .await
            .unwrap();
        // WriteBufferFull happens before any partial silence reaches this socket.
        assert!(matches!(
            timeout(Duration::from_secs(2), ws.next()).await.unwrap(),
            None | Some(Err(_))
        ));
        assert!(timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err());
    });
    let (socket, _) = open_socket(
        &config,
        None,
        None,
        None,
        &HashSet::new(),
        false,
        ReceivePolicy::Raw,
    )
    .await
    .unwrap();
    // Only setupComplete has crossed this stream, and it was fully consumed.
    // Rewrap the same TCP connection with a bounded failing write codec.
    let socket = WebSocketStream::from_raw_socket(
        socket.into_inner(),
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        Some(
            WebSocketConfig::default()
                .write_buffer_size(0)
                .max_write_buffer_size(1),
        ),
    )
    .await;
    let (tx, rx) = mpsc::channel(2);
    let (events, _event_rx) = mpsc::channel(2);
    let (control_tx, control) = watch::channel(Control::default());
    let sender = GeminiSender {
        config: Arc::new(config),
        tx,
        control: control_tx,
        generation: Arc::new(AtomicU64::new(0)),
        finishing: Arc::new(AtomicBool::new(false)),
        gate: Arc::new(Mutex::new(())),
        admission: Arc::new(std::sync::Mutex::new(())),
        tool_admissions: Arc::new(std::sync::Mutex::new(ToolAdmissions::default())),
        non_blocking_tools: Arc::new(HashSet::new()),
        selected: None,
    };
    let keepalive = SttKeepalive::new(
        SttKeepalivePolicy {
            idle_timeout: Duration::from_millis(1),
            check_interval: Duration::from_millis(1),
        },
        &sender.config,
    )
    .unwrap();
    let observer = sender.clone();
    let worker = tokio::spawn(run(
        socket,
        rx,
        events,
        control,
        sender,
        WorkerOptions {
            policy: Some(ResumptionPolicy::default()),
            handle: Some("cannot-retry-write".into()),
            thinking: None,
            receive_policy: ReceivePolicy::Raw,
            stt_keepalive: Some(keepalive),
        },
    ));
    assert_eq!(
        timeout(Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap(),
        Err(GeminiError::Transport)
    );
    assert!(observer.finishing.load(Ordering::SeqCst));
    assert_eq!(
        observer.try_send(Action::Audio(vec![0; 2]), Origin::default()),
        Err(GeminiError::Closed)
    );
    peer.await.unwrap();
}

type Peer = WebSocketStream<tokio::net::TcpStream>;
const WAIT: Duration = Duration::from_secs(3);

async fn ready_actor() -> (
    Socket,
    Peer,
    GeminiSender,
    mpsc::Receiver<Command>,
    mpsc::Sender<ReceivedEvent>,
    mpsc::Receiver<ReceivedEvent>,
    watch::Receiver<Control>,
    WorkerOptions,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GeminiConfig::transcription("fixture-key", "fixture-model");
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let config = Arc::new(config);
    let connecting_config = config.clone();
    let connecting = tokio::spawn(async move {
        open_socket(
            &connecting_config,
            None,
            None,
            None,
            &HashSet::new(),
            false,
            ReceivePolicy::Raw,
        )
        .await
        .unwrap()
        .0
    });
    let (tcp, _) = listener.accept().await.unwrap();
    let mut peer = tokio_tungstenite::accept_async(tcp).await.unwrap();
    assert!(matches!(peer.next().await, Some(Ok(Message::Text(_)))));
    peer.send(Message::Text(r#"{"setupComplete":{}}"#.into()))
        .await
        .unwrap();
    let socket = connecting.await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    let (events, event_rx) = mpsc::channel(4);
    let (control_tx, control) = watch::channel(Control::default());
    let sender = GeminiSender {
        config,
        tx,
        control: control_tx,
        generation: Arc::new(AtomicU64::new(0)),
        finishing: Arc::new(AtomicBool::new(false)),
        gate: Arc::new(Mutex::new(())),
        admission: Arc::new(std::sync::Mutex::new(())),
        tool_admissions: Arc::new(std::sync::Mutex::new(ToolAdmissions::default())),
        non_blocking_tools: Arc::new(HashSet::new()),
        selected: None,
    };
    let mut keepalive = SttKeepalive::new(
        SttKeepalivePolicy {
            idle_timeout: Duration::from_millis(200),
            check_interval: Duration::from_millis(20),
        },
        &sender.config,
    )
    .unwrap();
    // These are actual actor clock inputs, not sleep-based guesses that a timer is due.
    keepalive.last_audio = Instant::now() - Duration::from_secs(1);
    keepalive.next_check = keepalive.last_audio;
    let options = WorkerOptions {
        policy: None,
        handle: None,
        thinking: None,
        receive_policy: ReceivePolicy::Raw,
        stt_keepalive: Some(keepalive),
    };
    (socket, peer, sender, rx, events, event_rx, control, options)
}

#[tokio::test]
async fn already_due_timer_loses_to_queued_finish_without_stream_end() {
    let (socket, mut peer, sender, commands, events, mut event_rx, control, options) =
        ready_actor().await;
    assert!(options.stt_keepalive.as_ref().unwrap().next_check < Instant::now());
    sender.finish().await.unwrap();
    let actor = tokio::spawn(run(socket, commands, events, control, sender, options));
    assert!(matches!(
        timeout(WAIT, peer.next()).await.unwrap(),
        Some(Ok(Message::Close(_)))
    ));
    timeout(WAIT, actor).await.unwrap().unwrap().unwrap();
    assert!(event_rx.recv().await.is_none());
}

#[tokio::test]
async fn already_due_timer_loses_to_real_admitted_audio_and_resets_idle() {
    use base64::Engine;
    let (socket, mut peer, sender, commands, events, _event_rx, control, options) =
        ready_actor().await;
    assert!(options.stt_keepalive.as_ref().unwrap().next_check < Instant::now());
    sender
        .try_send(Action::Audio(vec![1, 2, 3, 4]), Origin::default())
        .unwrap();
    let actor = tokio::spawn(run(
        socket,
        commands,
        events,
        control,
        sender.clone(),
        options,
    ));
    let message = timeout(WAIT, peer.next()).await.unwrap().unwrap().unwrap();
    let value: serde_json::Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(value["realtimeInput"]["audio"]["data"].as_str().unwrap())
            .unwrap(),
        vec![1, 2, 3, 4]
    );
    assert!(timeout(Duration::from_millis(100), peer.next())
        .await
        .is_err());
    sender.cancel();
    timeout(WAIT, actor).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn cancel_wins_over_already_due_timer_before_any_write() {
    let (socket, mut peer, sender, commands, events, _event_rx, control, options) =
        ready_actor().await;
    sender.cancel();
    let actor = tokio::spawn(run(socket, commands, events, control, sender, options));
    timeout(WAIT, actor).await.unwrap().unwrap().unwrap();
    assert!(matches!(
        timeout(WAIT, peer.next()).await.unwrap(),
        None | Some(Err(_))
    ));
}

#[tokio::test]
async fn cancellation_joins_due_keepalive_on_backpressured_real_socket() {
    use std::{future::Future, task::Poll};
    let (mut socket, _peer, sender, commands, events, _event_rx, control, options) =
        ready_actor().await;
    // The peer never reads after setup. Fill this same actual TCP/WebSocket
    // sink, with a strict 16 MiB cap, until an observed send remains pending.
    // Dropping that send future retains its buffered frame in the sink.
    let mut pending = false;
    for _ in 0..256 {
        match timeout(
            Duration::from_millis(10),
            socket.send(Message::Binary(vec![0; 65_536].into())),
        )
        .await
        {
            Ok(result) => result.unwrap(),
            Err(_) => {
                pending = true;
                break;
            }
        }
    }
    assert!(
        pending,
        "fixture did not establish real transport backpressure"
    );
    let observer = sender.clone();
    let actor = tokio::spawn(async move {
        let mut running = Box::pin(run(socket, commands, events, control, sender, options));
        // Expired timer, empty commands and a silent peer make the first actor
        // turn attempt its keepalive on the already blocked writer. Poll it
        // before cancelling rather than merely cancelling a spawned task.
        std::future::poll_fn(|cx| {
            assert!(running.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        observer.cancel();
        running.await
    });
    timeout(Duration::from_secs(1), actor)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Buffered bytes were admitted before cancellation and may reach the peer;
    // this test claims bounded cancellation/join, not byte retraction.
}

#[tokio::test]
async fn cancel_interrupts_observed_pending_silence_write_and_joins_owner() {
    use std::{future::Future, task::Poll};
    let (mut socket, _peer, sender, _commands, _events, _event_rx, mut control, options) =
        ready_actor().await;
    let mut pending = false;
    for _ in 0..256 {
        match timeout(
            Duration::from_millis(10),
            socket.send(Message::Binary(vec![0; 65_536].into())),
        )
        .await
        {
            Ok(result) => result.unwrap(),
            Err(_) => {
                pending = true;
                break;
            }
        }
    }
    assert!(
        pending,
        "fixture did not establish real transport backpressure"
    );
    let silence = Message::Text(options.stt_keepalive.unwrap().message.into());
    let owner = tokio::spawn(async move {
        let mut writing = Box::pin(write(
            &mut socket,
            silence,
            &mut control,
            Duration::from_secs(2),
        ));
        // Unlike the actor fixture, Pending here directly observes the exact
        // synthetic-payload write helper used by Wake::Keepalive.
        std::future::poll_fn(|cx| {
            assert!(writing.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        sender.cancel();
        writing.await
    });
    assert_eq!(
        timeout(Duration::from_secs(1), owner)
            .await
            .unwrap()
            .unwrap(),
        Err(GeminiError::Closed)
    );
}
