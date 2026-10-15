//! Real loopback thinking contracts. No provider credentials or model execution.
//! Every assertion follows owned session and peer cleanup; no detached workers.
use futures_util::{SinkExt, StreamExt};
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    tungstenite::{
        error::ProtocolError,
        handshake::server::Request,
        protocol::{frame::coding::CloseCode, CloseFrame},
        Error as WsError, Message,
    },
    WebSocketStream,
};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Ws = WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(5);
const KEY: &str = "synthetic-thinking-key";

enum Step {
    Send(Value),
    Read,
    Wait(Arc<Notify>),
    Signal(Arc<Notify>),
    Delay(Duration),
    PingPong,
    HeartbeatUntilClosed,
    Close,
    Closed,
}

#[derive(Debug, Default)]
struct Connection {
    path: String,
    query: Option<String>,
    authorization: bool,
    setup: Value,
    wire: Vec<Value>,
    before_ack: usize,
    ack: bool,
    pongs: usize,
    closed: bool,
    reset: bool,
}

#[derive(Debug, Default)]
struct Trace {
    connections: Vec<Connection>,
    completed: usize,
    extra_connections: usize,
    error: Option<String>,
}

struct Peer {
    finished: Arc<Notify>,
    stop: Arc<Notify>,
    task: JoinHandle<Trace>,
}

fn signal() -> Arc<Notify> {
    Arc::new(Notify::new())
}

async fn signalled(signal: &Notify) -> Result<(), Error> {
    timeout(WAIT, signal.notified()).await?;
    Ok(())
}

fn config() -> GeminiConfig {
    let mut config = GeminiConfig::new(KEY, "source-model");
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.setup_timeout = Duration::from_secs(2);
    config.limits.write_timeout = Duration::from_secs(1);
    config.limits.finish_timeout = Duration::from_secs(1);
    config
}

fn policy(duration: Duration) -> ThinkingPolicy {
    ThinkingPolicy {
        level: ThinkingLevel::High,
        silence_timeout: duration,
    }
}

fn thinking(config: GeminiConfig, duration: Duration) -> Result<GeminiClient, GeminiError> {
    GeminiClient::new(config)?.with_thinking(policy(duration))
}

fn origin(id: u64) -> Origin {
    Origin {
        frame_id: Some(id),
        context_id: Some(format!("thinking-{id}")),
    }
}

fn ready() -> Step {
    Step::Send(json!({"setupComplete":{}}))
}

fn usage(marker: u64) -> Step {
    Step::Send(json!({"usageMetadata":{"totalTokenCount":marker}}))
}

fn held(marker: u64) -> Step {
    Step::Send(json!({
        "serverContent":{"interactionStatus":"IN_PROGRESS","turnComplete":true},
        "usageMetadata":{"totalTokenCount":marker}
    }))
}

fn idle(status: &str) -> Step {
    Step::Send(json!({"serverContent":{"interactionStatus":status}}))
}

fn done() -> Step {
    Step::Send(json!({"serverContent":{"turnComplete":true}}))
}

fn model(text: &str) -> Step {
    Step::Send(json!({"serverContent":{"modelTurn":{"parts":[{"text":text}]},"turnComplete":true}}))
}

fn resume_handle() -> Step {
    Step::Send(json!({"sessionResumptionUpdate":{"resumable":true,"newHandle":"thinking-handle"}}))
}

fn disconnected(error: &WsError) -> bool {
    matches!(
        error,
        WsError::ConnectionClosed
            | WsError::AlreadyClosed
            | WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
    ) || matches!(error, WsError::Io(error) if matches!(error.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::UnexpectedEof))
}

fn record(connection: &mut Connection, value: Value) {
    if !connection.ack {
        connection.before_ack += 1;
    }
    connection.wire.push(value);
}

async fn read(ws: &mut Ws, connection: &mut Connection) -> Result<(), Error> {
    loop {
        match timeout(WAIT, ws.next()).await?.ok_or("EOF before JSON")?? {
            Message::Text(text) => {
                record(connection, serde_json::from_str(&text)?);
                return Ok(());
            }
            Message::Binary(bytes) => {
                record(connection, serde_json::from_slice(&bytes)?);
                return Ok(());
            }
            Message::Ping(bytes) => ws.send(Message::Pong(bytes)).await?,
            Message::Pong(_) => connection.pongs += 1,
            _ => return Err("expected JSON".into()),
        }
    }
}

async fn closed_frame(
    frame: Option<Result<Message, WsError>>,
    ws: &mut Ws,
    connection: &mut Connection,
) -> Result<bool, Error> {
    match frame {
        None => connection.reset = true,
        Some(Err(error)) if disconnected(&error) => connection.reset = true,
        Some(Err(error)) => return Err(error.into()),
        Some(Ok(Message::Close(_))) => {
            connection.closed = true;
            match timeout(WAIT, ws.flush()).await? {
                Ok(()) => {}
                Err(error) if disconnected(&error) => {}
                Err(error) => return Err(error.into()),
            }
        }
        Some(Ok(Message::Pong(_))) => {
            connection.pongs += 1;
            return Ok(false);
        }
        Some(Ok(Message::Ping(bytes))) => {
            ws.send(Message::Pong(bytes)).await?;
            return Ok(false);
        }
        Some(Ok(Message::Text(text))) => {
            record(connection, serde_json::from_str(&text)?);
            return Ok(false);
        }
        Some(Ok(Message::Binary(bytes))) => {
            record(connection, serde_json::from_slice(&bytes)?);
            return Ok(false);
        }
        _ => return Err("unexpected WebSocket frame".into()),
    }
    Ok(true)
}

async fn closed(ws: &mut Ws, connection: &mut Connection) -> Result<(), Error> {
    loop {
        let frame = timeout(WAIT, ws.next()).await?;
        if closed_frame(frame, ws, connection).await? {
            return Ok(());
        }
    }
}

async fn heartbeat(ws: &mut Ws, connection: &mut Connection) -> Result<(), Error> {
    let mut interval = tokio::time::interval(Duration::from_millis(50));
    loop {
        tokio::select! {
            frame = ws.next() => {
                if closed_frame(frame, ws, connection).await? { return Ok(()); }
            }
            _ = interval.tick() => {
                match ws.send(Message::Ping(vec![96].into())).await {
                    Ok(()) => {}
                    Err(error) if disconnected(&error) => {
                        connection.reset = true;
                        return Ok(());
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
}

#[allow(clippy::result_large_err)]
async fn plans(
    listener: &TcpListener,
    plans: Vec<Vec<Step>>,
    trace: &mut Trace,
    finished: &Notify,
) -> Result<(), Error> {
    for plan in plans {
        let (tcp, _) = timeout(WAIT, listener.accept()).await??;
        let mut connection = Connection::default();
        let mut ws = timeout(
            WAIT,
            tokio_tungstenite::accept_hdr_async(tcp, |request: &Request, response| {
                connection.path = request.uri().path().into();
                connection.query = request.uri().query().map(str::to_owned);
                connection.authorization = request.headers().contains_key("authorization");
                Ok(response)
            }),
        )
        .await??;
        connection.setup = match timeout(WAIT, ws.next()).await?.ok_or("missing setup")?? {
            Message::Text(text) => serde_json::from_str(&text)?,
            Message::Binary(bytes) => serde_json::from_slice(&bytes)?,
            _ => return Err("setup was not first JSON".into()),
        };
        trace.connections.push(connection);
        let connection = trace.connections.last_mut().ok_or("missing connection")?;
        for step in plan {
            match step {
                Step::Send(value) => {
                    connection.ack |= value.get("setupComplete").is_some();
                    timeout(WAIT, ws.send(Message::Text(value.to_string().into()))).await??;
                }
                Step::Read => read(&mut ws, connection).await?,
                Step::Wait(signal) => signalled(&signal).await?,
                Step::Signal(signal) => signal.notify_one(),
                Step::Delay(duration) => sleep(duration).await,
                Step::PingPong => {
                    ws.send(Message::Ping(vec![9, 6].into())).await?;
                    loop {
                        match timeout(WAIT, ws.next()).await?.ok_or("missing Pong")?? {
                            Message::Pong(bytes) if bytes.as_ref() == [9, 6] => {
                                connection.pongs += 1;
                                break;
                            }
                            Message::Text(text) => record(connection, serde_json::from_str(&text)?),
                            Message::Binary(bytes) => {
                                record(connection, serde_json::from_slice(&bytes)?)
                            }
                            _ => return Err("unexpected frame while waiting for Pong".into()),
                        }
                    }
                }
                Step::HeartbeatUntilClosed => heartbeat(&mut ws, connection).await?,
                Step::Close => {
                    ws.send(Message::Close(Some(CloseFrame {
                        code: CloseCode::Restart,
                        reason: "synthetic thinking recovery".into(),
                    })))
                    .await?;
                }
                Step::Closed => closed(&mut ws, connection).await?,
            }
        }
        drop(ws);
        trace.completed += 1;
    }
    finished.notify_one();
    // Keep the listener owned until the session joins; count forbidden retries.
    loop {
        let (extra, _) = listener.accept().await?;
        trace.extra_connections += 1;
        drop(extra);
    }
}

async fn peer(
    planned: Vec<Vec<Step>>,
    mut config: GeminiConfig,
) -> Result<(Peer, GeminiConfig), Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    config.endpoint = format!("ws://{}/live", listener.local_addr()?);
    let finished = signal();
    let completed = finished.clone();
    let stop = signal();
    let stopping = stop.clone();
    let task = tokio::spawn(async move {
        let mut trace = Trace::default();
        let error = {
            let work = plans(&listener, planned, &mut trace, &completed);
            tokio::pin!(work);
            tokio::select! { biased;
                result = &mut work => result.err().map(|error| error.to_string()),
                _ = stopping.notified() => None,
            }
        };
        trace.error = error;
        trace
    });
    Ok((
        Peer {
            finished,
            stop,
            task,
        },
        config,
    ))
}

async fn cleanup(
    session: Option<GeminiSession>,
    peer: Peer,
) -> (Result<(), Error>, Result<Trace, Error>) {
    let joined = match session {
        Some(session) => {
            let shutdown = session.shutdown();
            tokio::pin!(shutdown);
            match timeout(WAIT, &mut shutdown).await {
                Ok(result) => result.map_err(Into::into),
                Err(_) => {
                    let _ = shutdown.await;
                    Err("session deadline; subsequently joined".into())
                }
            }
        }
        None => Ok(()),
    };
    let _ = signalled(&peer.finished).await;
    peer.stop.notify_one();
    let mut owner = peer.task;
    let trace = match timeout(WAIT, &mut owner).await {
        Ok(result) => result.map_err(Into::into),
        Err(_) => {
            owner.abort();
            let _ = owner.await;
            Err("peer deadline; aborted and joined".into())
        }
    };
    (joined, trace)
}

fn checked(result: (Result<(), Error>, Result<Trace, Error>), count: usize) -> Trace {
    eprintln!("Settled thinking owners: {result:#?}");
    result.0.unwrap();
    let trace = result.1.unwrap();
    assert!(trace.error.is_none(), "{trace:#?}");
    assert_eq!(trace.connections.len(), count, "{trace:#?}");
    assert_eq!(trace.completed, count, "{trace:#?}");
    assert_eq!(trace.extra_connections, 0, "{trace:#?}");
    for connection in &trace.connections {
        assert_eq!(connection.path, "/live");
        assert_eq!(
            connection.query.as_deref(),
            Some("key=synthetic-thinking-key")
        );
        assert!(!connection.authorization);
        assert_eq!(connection.before_ack, 0, "{trace:#?}");
    }
    trace
}

async fn next(session: &mut GeminiSession) -> Result<GeminiEvent, Error> {
    Ok(timeout(WAIT, session.next_event())
        .await?
        .ok_or("missing event")??)
}

async fn terminal(
    session: &mut GeminiSession,
) -> Result<(Vec<GeminiEvent>, Option<GeminiError>), Error> {
    let mut events = Vec::new();
    loop {
        match timeout(WAIT, session.next_event()).await? {
            Some(Ok(event)) => events.push(event),
            Some(Err(error)) => return Ok((events, Some(error))),
            None => return Ok((events, None)),
        }
    }
}

fn text(sender: &GeminiSender, id: u64) -> Result<(), Error> {
    Ok(sender.try_send(Action::RealtimeText(format!("request-{id}")), origin(id))?)
}

fn is_usage(event: &GeminiEvent, marker: u64) -> bool {
    matches!(&event.event, Event::Usage(value) if value.total_token_count == Some(marker))
}

#[tokio::test]
async fn typed_levels_are_exact_setup_and_default_wire_is_unchanged() {
    let mut baseline: Option<Value> = None;
    for level in [
        None,
        Some(ThinkingLevel::Low),
        Some(ThinkingLevel::Medium),
        Some(ThinkingLevel::High),
    ] {
        let (peer, config) = peer(
            vec![vec![ready(), Step::Read, done(), Step::Closed]],
            config(),
        )
        .await
        .unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            let client = GeminiClient::new(config)?;
            let client = match level {
                Some(level) => client.with_thinking(ThinkingPolicy {
                    level,
                    ..ThinkingPolicy::default()
                })?,
                None => client,
            };
            session = Some(client.connect().await?);
            let session = session.as_mut().unwrap();
            text(&session.sender(), 1)?;
            let complete = next(session).await?;
            session.sender().finish().await?;
            Ok((complete, terminal(session).await?))
        }
        .await;
        let trace = checked(cleanup(session, peer).await, 1);
        let (complete, end) = work.unwrap();
        assert_eq!(complete.event, Event::TurnComplete);
        assert_eq!(complete.origin, origin(1));
        assert!(end.0.is_empty());
        assert_eq!(end.1, None);
        let setup = &trace.connections[0].setup;
        if let Some(level) = level {
            let mut expected = baseline.clone().unwrap();
            expected["setup"]["generationConfig"]["thinkingConfig"] =
                json!({"thinkingLevel":level});
            assert_eq!(*setup, expected);
        } else {
            assert!(setup["setup"]["generationConfig"]
                .get("thinkingConfig")
                .is_none());
            baseline = Some(setup.clone());
        }
    }
}

#[test]
fn thinking_limits_tools_and_transcription_are_rejected_before_connect() {
    for (duration, valid) in [
        (Duration::ZERO, false),
        (Duration::from_nanos(1), true),
        (Duration::from_secs(86400), true),
        (Duration::from_secs(86401), false),
        (Duration::MAX, false),
    ] {
        let result = thinking(config(), duration);
        assert_eq!(result.is_ok(), valid);
        if !valid {
            assert!(matches!(
                result,
                Err(GeminiError::Invalid("thinking silence timeout"))
            ));
        }
    }
    let mut tools = config();
    tools.session.tools.push(FunctionDeclaration {
        name: "lookup".into(),
        description: "synthetic lookup".into(),
        parameters: json!({"type":"object"}),
    });
    for config in [tools, GeminiConfig::transcription(KEY, "source-model")] {
        assert!(matches!(
            thinking(config, Duration::from_secs(1)),
            Err(GeminiError::Unsupported(
                "thinking requires conversation mode without tools"
            ))
        ));
    }
    assert_eq!(ThinkingPolicy::default().level, ThinkingLevel::Low);
    assert_eq!(
        ThinkingPolicy::default().silence_timeout,
        Duration::from_secs(30)
    );
}

#[tokio::test]
async fn in_progress_holds_turn_but_generation_and_usage_remain_observable() {
    for status in ["IDLE", "REQUIRES_ACTION"] {
        let release = signal();
        let (peer, config) = peer(vec![vec![ready(), Step::Read,
            Step::Send(json!({"serverContent":{"interactionStatus":"IN_PROGRESS","modelTurn":{"parts":[{"text":"reason","thought":true}]},"generationComplete":true,"turnComplete":true},"usageMetadata":{"totalTokenCount":1}})),
            Step::Wait(release.clone()), idle(status), usage(2), idle(status), usage(3), Step::Closed,
        ]], config()).await.unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(thinking(config, Duration::from_secs(2))?.connect().await?);
            let session = session.as_mut().unwrap();
            text(&session.sender(), 11)?;
            let before = vec![
                next(session).await?,
                next(session).await?,
                next(session).await?,
            ];
            release.notify_one();
            let after = vec![
                next(session).await?,
                next(session).await?,
                next(session).await?,
            ];
            session.sender().finish().await?;
            Ok((before, after, terminal(session).await?))
        }
        .await;
        checked(cleanup(session, peer).await, 1);
        let (before, after, end) = work.unwrap();
        assert!(
            matches!(&before[0].event, Event::Text { text, thought: true } if text == "reason")
        );
        assert_eq!(before[1].event, Event::GenerationComplete);
        assert!(is_usage(&before[2], 1));
        assert_eq!(after[0].event, Event::TurnComplete);
        assert_eq!(after[0].origin, origin(11));
        assert_eq!(after[0].generation, 0);
        assert!(is_usage(&after[1], 2));
        assert!(is_usage(&after[2], 3));
        assert!(end.0.is_empty());
        assert_eq!(end.1, None);
    }
}

#[tokio::test]
async fn repeated_holds_preserve_origin_and_statusless_completion_emits_once() {
    for status in [Some("IDLE"), None, Some("INTERACTION_STATUS_UNSPECIFIED")] {
        let release = signal();
        let preserve_first = status == Some("IDLE");
        let mut final_content = if preserve_first {
            json!({})
        } else {
            json!({"turnComplete":true})
        };
        if let Some(status) = status {
            final_content["interactionStatus"] = json!(status);
        }
        let mut plan = vec![ready(), Step::Read, held(1)];
        if preserve_first {
            plan.push(Step::Read);
        }
        plan.extend([
            held(2),
            Step::Wait(release.clone()),
            Step::Send(json!({"serverContent":final_content})),
            idle("IDLE"),
            usage(3),
            Step::Closed,
        ]);
        let (peer, config) = peer(vec![plan], config()).await.unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(thinking(config, Duration::from_secs(2))?.connect().await?);
            let session = session.as_mut().unwrap();
            text(&session.sender(), 12)?;
            let one = next(session).await?;
            if preserve_first {
                text(&session.sender(), 14)?;
            }
            let two = next(session).await?;
            release.notify_one();
            let complete = next(session).await?;
            let marker = next(session).await?;
            session.sender().finish().await?;
            Ok((one, two, complete, marker, terminal(session).await?))
        }
        .await;
        let trace = checked(cleanup(session, peer).await, 1);
        let (one, two, complete, marker, end) = work.unwrap();
        assert!(is_usage(&one, 1) && is_usage(&two, 2) && is_usage(&marker, 3));
        assert_eq!(complete.event, Event::TurnComplete);
        assert_eq!(complete.origin, origin(12));
        assert_eq!(complete.generation, 0);
        assert_eq!(
            trace.connections[0].wire.len(),
            if preserve_first { 2 } else { 1 }
        );
        assert!(end.0.is_empty());
        assert_eq!(end.1, None);
    }
}

#[tokio::test]
async fn unexpected_tools_and_unknown_status_fail_closed_without_releasing_hold() {
    let cases = [
        (
            true,
            json!({"serverContent":{"interactionStatus":"FUTURE_STATUS","turnComplete":true}}),
            GeminiError::Unsupported("background reasoning interaction status"),
        ),
        (
            true,
            json!({"serverContent":{"interactionStatus":"IDLE"},"toolCall":{"functionCalls":[{"id":"unexpected","name":"lookup","args":{}}]}}),
            GeminiError::Protocol("undeclared function call"),
        ),
        (
            false,
            json!({"serverContent":{"interactionStatus":"IN_PROGRESS","turnComplete":true}}),
            GeminiError::Unsupported("background reasoning interaction status"),
        ),
        (
            false,
            json!({"serverContent":{"interactionStatus":"REQUIRES_ACTION","turnComplete":true}}),
            GeminiError::Unsupported("background reasoning interaction status"),
        ),
    ];
    for (enabled, fault, expected) in cases {
        let release = signal();
        let (peer, config) = peer(
            vec![vec![
                ready(),
                Step::Read,
                if enabled { held(1) } else { usage(1) },
                Step::Wait(release.clone()),
                Step::Send(fault),
                Step::Closed,
            ]],
            config(),
        )
        .await
        .unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            let client = GeminiClient::new(config)?;
            let client = if enabled {
                client.with_thinking(ThinkingPolicy::default())?
            } else {
                client
            };
            session = Some(client.connect().await?);
            let session = session.as_mut().unwrap();
            text(&session.sender(), 13)?;
            let marker = next(session).await?;
            release.notify_one();
            Ok((marker, terminal(session).await?))
        }
        .await;
        checked(cleanup(session, peer).await, 1);
        let (marker, end) = work.unwrap();
        assert!(is_usage(&marker, 1));
        assert!(
            end.0.is_empty(),
            "held completion or tool escaped failure: {:?}",
            end.0
        );
        assert_eq!(end.1, Some(expected));
    }
}

#[tokio::test]
async fn local_interrupt_drops_hold_and_idle_does_not_finish_new_busy_input() {
    let release = signal();
    let (peer, config) = peer(
        vec![vec![
            ready(),
            Step::Read,
            held(1),
            Step::Read,
            idle("IDLE"),
            usage(2),
            Step::Wait(release.clone()),
            model("new answer"),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(thinking(config, Duration::from_secs(2))?.connect().await?);
        let session = session.as_mut().unwrap();
        let sender = session.sender();
        text(&sender, 20)?;
        let old = next(session).await?;
        sender.interrupt().await;
        text(&sender, 21)?;
        let drained = next(session).await?;
        sender.finish().await?;
        // Finish is admitted after the peer acknowledged new input and sent
        // IDLE-only. With the new TC withheld, early EOF is an observable bug.
        let no_early_terminal = timeout(Duration::from_millis(80), session.next_event())
            .await
            .is_err();
        release.notify_one();
        let output = next(session).await?;
        let complete = next(session).await?;
        Ok((
            old,
            drained,
            no_early_terminal,
            output,
            complete,
            terminal(session).await?,
        ))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 1);
    let (old, drained, no_early_terminal, output, complete, end) = work.unwrap();
    assert!(is_usage(&old, 1) && is_usage(&drained, 2));
    assert!(no_early_terminal);
    assert!(matches!(&output.event, Event::Text { text, .. } if text == "new answer"));
    assert_eq!(complete.event, Event::TurnComplete);
    for event in [output, complete] {
        assert_eq!(event.origin, origin(21));
        assert_eq!(event.generation, 1);
    }
    assert_eq!(trace.connections[0].wire.len(), 2);
    assert!(end.0.is_empty());
    assert_eq!(end.1, None);
}

#[tokio::test]
async fn held_completion_does_not_block_audio_or_manual_activity_start() {
    for manual in [false, true] {
        let mut config = config();
        config.session.activity_detection.disabled = manual;
        let reads = if manual { 3 } else { 1 };
        let mut plan = vec![ready(), Step::Read, held(1)];
        plan.extend((0..reads).map(|_| Step::Read));
        plan.extend([idle("IDLE"), Step::Closed]);
        let (peer, config) = peer(vec![plan], config).await.unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(thinking(config, Duration::from_secs(2))?.connect().await?);
            let session = session.as_mut().unwrap();
            let sender = session.sender();
            text(&sender, 30)?;
            let marker = next(session).await?;
            if manual {
                sender.try_send(Action::ActivityStart { preroll: vec![] }, origin(31))?;
            }
            sender.try_send(Action::Audio(vec![0, 0, 1, 0]), origin(31))?;
            if manual {
                sender.try_send(Action::ActivityEnd, origin(31))?;
            }
            let complete = next(session).await?;
            sender.finish().await?;
            Ok((marker, complete, terminal(session).await?))
        }
        .await;
        let trace = checked(cleanup(session, peer).await, 1);
        let (marker, complete, end) = work.unwrap();
        assert!(is_usage(&marker, 1));
        assert_eq!(complete.event, Event::TurnComplete);
        assert_eq!(complete.origin, origin(30));
        let wire = &trace.connections[0].wire;
        assert_eq!(wire.len(), reads + 1);
        assert!(wire
            .iter()
            .any(|value| value["realtimeInput"].get("audio").is_some()));
        if manual {
            assert!(wire[1]["realtimeInput"].get("activityStart").is_some());
            assert!(wire[3]["realtimeInput"].get("activityEnd").is_some());
        }
        assert!(end.0.is_empty());
        assert_eq!(end.1, None);
    }
}

#[tokio::test]
async fn valid_application_messages_refresh_the_held_silence_watchdog() {
    let release = signal();
    let mut plan = vec![ready(), Step::Read, held(1), Step::Wait(release.clone())];
    for marker in 2..=5 {
        plan.extend([Step::Delay(Duration::from_millis(200)), usage(marker)]);
    }
    plan.extend([idle("IDLE"), Step::Closed]);
    let (peer, config) = peer(vec![plan], config()).await.unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(
            thinking(config, Duration::from_millis(500))?
                .connect()
                .await?,
        );
        let session = session.as_mut().unwrap();
        text(&session.sender(), 40)?;
        let mut observed = vec![next(session).await?];
        release.notify_one();
        for _ in 2..=5 {
            observed.push(next(session).await?);
        }
        let complete = next(session).await?;
        session.sender().finish().await?;
        Ok((observed, complete, terminal(session).await?))
    }
    .await;
    checked(cleanup(session, peer).await, 1);
    let (observed, complete, end) = work.unwrap();
    for (index, event) in observed.iter().enumerate() {
        assert!(is_usage(event, index as u64 + 1));
    }
    assert_eq!(complete.event, Event::TurnComplete);
    assert_eq!(complete.origin, origin(40));
    assert!(end.0.is_empty());
    assert_eq!(end.1, None);
}

#[tokio::test]
async fn websocket_ping_does_not_refresh_silence_and_timeout_never_synthesizes_end() {
    let release = signal();
    let (peer, config) = peer(
        vec![vec![
            ready(),
            Step::Read,
            held(1),
            Step::Wait(release.clone()),
            Step::HeartbeatUntilClosed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(
            thinking(config, Duration::from_millis(300))?
                .connect()
                .await?,
        );
        let session = session.as_mut().unwrap();
        text(&session.sender(), 50)?;
        let marker = next(session).await?;
        release.notify_one();
        Ok((marker, terminal(session).await?))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 1);
    let (marker, end) = work.unwrap();
    assert!(is_usage(&marker, 1));
    assert!(trace.connections[0].pongs > 0);
    assert!(end.0.is_empty());
    assert_eq!(end.1, Some(GeminiError::Timeout("thinking silence")));
    assert!(trace.connections[0].closed || trace.connections[0].reset);
}

#[tokio::test]
async fn finish_and_cancel_while_held_settle_the_actual_worker() {
    for mode in ["graceful", "finish-timeout", "cancel"] {
        let release = signal();
        let mut plan = vec![ready(), Step::Read, held(1), Step::Wait(release.clone())];
        if mode == "graceful" {
            plan.push(idle("IDLE"));
        }
        plan.push(Step::Closed);
        let mut config = config();
        config.limits.finish_timeout = Duration::from_millis(150);
        let (peer, config) = peer(vec![plan], config).await.unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(thinking(config, Duration::from_secs(2))?.connect().await?);
            let session = session.as_mut().unwrap();
            let sender = session.sender();
            text(&sender, 60)?;
            let marker = next(session).await?;
            if mode == "cancel" {
                sender.cancel();
            } else {
                sender.finish().await?;
            }
            release.notify_one();
            Ok((marker, terminal(session).await?))
        }
        .await;
        let trace = checked(cleanup(session, peer).await, 1);
        let (marker, end) = work.unwrap();
        assert!(is_usage(&marker, 1));
        if mode == "graceful" {
            assert_eq!(end.0.len(), 1);
            assert_eq!(end.0[0].event, Event::TurnComplete);
            assert_eq!(end.0[0].origin, origin(60));
            assert_eq!(end.1, None);
        } else {
            assert!(end.0.is_empty());
            assert_eq!(
                end.1,
                if mode == "cancel" {
                    None
                } else {
                    Some(GeminiError::Timeout("finish"))
                }
            );
        }
        assert!(trace.connections[0].closed || trace.connections[0].reset);
    }
}

#[tokio::test]
async fn recovery_preserves_complete_thinking_setup_and_held_origin() {
    let close = signal();
    let replacement = signal();
    let acknowledge = signal();
    let (peer, config) = peer(
        vec![
            vec![
                ready(),
                Step::Read,
                resume_handle(),
                held(1),
                Step::Wait(close.clone()),
                Step::Close,
            ],
            vec![
                Step::PingPong,
                Step::Signal(replacement.clone()),
                Step::Wait(acknowledge.clone()),
                ready(),
                idle("IDLE"),
                Step::Closed,
            ],
        ],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(
            thinking(config, Duration::from_secs(2))?
                .with_session_resumption(ResumptionPolicy::default())?
                .connect()
                .await?,
        );
        let session = session.as_mut().unwrap();
        text(&session.sender(), 70)?;
        let marker = next(session).await?;
        close.notify_one();
        signalled(&replacement).await?;
        acknowledge.notify_one();
        let complete = next(session).await?;
        session.sender().finish().await?;
        Ok((marker, complete, terminal(session).await?))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 2);
    let (marker, complete, end) = work.unwrap();
    assert!(is_usage(&marker, 1));
    assert_eq!(complete.event, Event::TurnComplete);
    assert_eq!(complete.origin, origin(70));
    assert_eq!(complete.generation, 0);
    let mut expected = trace.connections[0].setup.clone();
    assert_eq!(
        expected["setup"]["generationConfig"]["thinkingConfig"],
        json!({"thinkingLevel":"HIGH"})
    );
    expected["setup"]["sessionResumption"] = json!({"handle":"thinking-handle"});
    assert_eq!(trace.connections[1].setup, expected);
    assert_eq!(trace.connections[0].wire.len(), 1);
    assert!(trace.connections[1].wire.is_empty());
    assert_eq!(trace.connections[1].pongs, 1);
    assert!(end.0.is_empty());
    assert_eq!(end.1, None);
}

#[tokio::test]
async fn recovery_handshake_cannot_restart_the_original_silence_deadline() {
    let close = signal();
    let replacement = signal();
    let (peer, config) = peer(
        vec![
            vec![
                ready(),
                Step::Read,
                resume_handle(),
                held(1),
                Step::Wait(close.clone()),
                Step::Close,
            ],
            vec![
                Step::PingPong,
                Step::Signal(replacement.clone()),
                Step::HeartbeatUntilClosed,
            ],
        ],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(
            thinking(config, Duration::from_millis(600))?
                .with_session_resumption(ResumptionPolicy::default())?
                .connect()
                .await?,
        );
        let session = session.as_mut().unwrap();
        text(&session.sender(), 80)?;
        let marker = next(session).await?;
        // The usage barrier establishes a real held turn before elapsed time is
        // spent. The replacement handshake has only the original remainder.
        sleep(Duration::from_millis(400)).await;
        close.notify_one();
        signalled(&replacement).await?;
        let end = timeout(Duration::from_millis(400), terminal(session)).await??;
        Ok((marker, end))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 2);
    let (marker, end) = work.unwrap();
    assert!(is_usage(&marker, 1));
    assert!(end.0.is_empty());
    assert_eq!(end.1, Some(GeminiError::Timeout("thinking silence")));
    assert!(!trace.connections[1].ack);
    assert!(trace.connections[1].pongs > 0);
    assert!(trace.connections[1].closed || trace.connections[1].reset);
}

#[tokio::test]
async fn interrupt_during_stalled_recovery_cancels_the_old_held_deadline() {
    let close = signal();
    let replacement = signal();
    let acknowledge = signal();
    let (peer, config) = peer(
        vec![
            vec![
                ready(),
                Step::Read,
                resume_handle(),
                held(1),
                Step::Wait(close.clone()),
                Step::Close,
            ],
            vec![
                Step::PingPong,
                Step::Signal(replacement.clone()),
                Step::Wait(acknowledge.clone()),
                ready(),
                Step::Read,
                idle("IDLE"),
                model("after interrupt"),
                Step::Closed,
            ],
        ],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(
            thinking(config, Duration::from_millis(250))?
                .with_session_resumption(ResumptionPolicy::default())?
                .connect()
                .await?,
        );
        let session = session.as_mut().unwrap();
        let sender = session.sender();
        text(&sender, 90)?;
        let marker = next(session).await?;
        close.notify_one();
        signalled(&replacement).await?;
        // Setup plus a real Ping/Pong proves the recovery future is running;
        // interrupt must invalidate its held generation and copied timer too.
        sender.interrupt().await;
        let old_deadline_quiet = timeout(Duration::from_millis(400), session.next_event())
            .await
            .is_err();
        acknowledge.notify_one();
        text(&sender, 91)?;
        let output = next(session).await?;
        let complete = next(session).await?;
        sender.finish().await?;
        Ok((
            marker,
            old_deadline_quiet,
            output,
            complete,
            terminal(session).await?,
        ))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 2);
    let (marker, old_deadline_quiet, output, complete, end) = work.unwrap();
    assert!(is_usage(&marker, 1));
    assert!(old_deadline_quiet);
    assert!(matches!(&output.event, Event::Text { text, .. } if text == "after interrupt"));
    assert_eq!(complete.event, Event::TurnComplete);
    for event in [output, complete] {
        assert_eq!(event.origin, origin(91));
        assert_eq!(event.generation, 1);
    }
    assert_eq!(trace.connections[1].wire.len(), 1);
    assert_eq!(
        trace.connections[1].wire[0]["realtimeInput"]["text"],
        "request-91"
    );
    assert!(end.0.is_empty());
    assert_eq!(end.1, None);
}
