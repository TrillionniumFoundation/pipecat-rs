//! Real loopback Gemini recovery contracts. No Google connection or provider model.
//! Protocol barriers replace sleeps; session/peer owners settle before assertions.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::{
    AudioRawFrame, ChatMessage, ChatRole, DirectedFrame, Direction, Frame, FrameData,
    LlmContextFrame, Pipeline, PipelineConfig, PipelineWorker, WorkerReport, WorkerState,
};
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    task::JoinHandle,
    time::timeout,
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
const KEY: &str = "synthetic-resumption-key";

#[derive(Clone)]
enum Step {
    Send(Value),
    Raw(&'static str),
    Read,
    Signal(Arc<Notify>),
    Wait(Arc<Notify>),
    Ping,
    Pong,
    Close(Option<u16>),
    Drop,
    Closed,
}
struct Plan {
    steps: Vec<Step>,
    reject: bool,
}
impl Plan {
    fn new(steps: Vec<Step>) -> Self {
        Self {
            steps,
            reject: false,
        }
    }
    fn reject() -> Self {
        Self {
            steps: vec![],
            reject: true,
        }
    }
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
fn notify() -> Arc<Notify> {
    Arc::new(Notify::new())
}
async fn signalled(signal: &Notify) -> Result<(), Error> {
    timeout(WAIT, signal.notified()).await?;
    Ok(())
}
fn ready() -> Step {
    Step::Send(json!({"setupComplete":{}}))
}
fn done() -> Step {
    Step::Send(json!({"serverContent":{"turnComplete":true}}))
}
fn update(handle: &str) -> Step {
    Step::Send(json!({"sessionResumptionUpdate":{"resumable":true,"newHandle":handle}}))
}
fn usage(marker: u64) -> Step {
    Step::Send(json!({"usageMetadata":{"totalTokenCount":marker}}))
}
fn model(text: &str, complete: bool) -> Step {
    Step::Send(
        json!({"serverContent":{"modelTurn":{"parts":[{"text":text}]},"turnComplete":complete}}),
    )
}
fn config() -> GeminiConfig {
    let mut config = GeminiConfig::new(KEY, "source-model");
    config.limits.connect_timeout = Duration::from_secs(1);
    config.limits.setup_timeout = Duration::from_secs(1);
    config.limits.write_timeout = Duration::from_secs(1);
    config.limits.finish_timeout = Duration::from_secs(1);
    config
}
fn client(config: GeminiConfig) -> Result<GeminiClient, GeminiError> {
    GeminiClient::new(config)?.with_session_resumption(ResumptionPolicy::default())
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
async fn message(ws: &mut Ws) -> Result<Option<Message>, Error> {
    match timeout(WAIT, ws.next()).await? {
        Some(Ok(message)) => Ok(Some(message)),
        Some(Err(error)) => Err(error.into()),
        None => Ok(None),
    }
}
fn record(trace: &mut Connection, value: Value) {
    if !trace.ack {
        trace.before_ack += 1;
    }
    trace.wire.push(value);
}
async fn read(ws: &mut Ws, trace: &mut Connection) -> Result<(), Error> {
    loop {
        match message(ws).await?.ok_or("peer EOF before JSON")? {
            Message::Text(text) => {
                record(trace, serde_json::from_str(&text)?);
                return Ok(());
            }
            Message::Binary(bytes) => {
                record(trace, serde_json::from_slice(&bytes)?);
                return Ok(());
            }
            Message::Ping(bytes) => ws.send(Message::Pong(bytes)).await?,
            Message::Pong(_) => {}
            _ => return Err("expected JSON data".into()),
        }
    }
}
async fn closed(ws: &mut Ws, trace: &mut Connection) -> Result<(), Error> {
    loop {
        match timeout(WAIT, ws.next()).await? {
            None => {
                trace.reset = true;
                return Ok(());
            }
            Some(Err(error)) if disconnected(&error) => {
                trace.reset = true;
                return Ok(());
            }
            Some(Err(error)) => return Err(error.into()),
            Some(Ok(Message::Close(_))) => {
                trace.closed = true;
                match timeout(WAIT, ws.flush()).await? {
                    Ok(()) => {}
                    Err(error) if disconnected(&error) => {}
                    Err(error) => return Err(error.into()),
                }
                return Ok(());
            }
            Some(Ok(Message::Text(text))) => record(trace, serde_json::from_str(&text)?),
            Some(Ok(Message::Binary(bytes))) => record(trace, serde_json::from_slice(&bytes)?),
            Some(Ok(Message::Ping(bytes))) => ws.send(Message::Pong(bytes)).await?,
            Some(Ok(Message::Pong(_))) => {}
            _ => return Err("unexpected peer frame before close".into()),
        }
    }
}
#[allow(clippy::result_large_err)]
async fn plans(
    listener: &TcpListener,
    plans: Vec<Plan>,
    trace: &mut Trace,
    finished: &Notify,
) -> Result<(), Error> {
    for plan in plans {
        let (tcp, _) = timeout(WAIT, listener.accept()).await??;
        let mut connection = Connection::default();
        let accepted = timeout(
            WAIT,
            tokio_tungstenite::accept_hdr_async(tcp, |request: &Request, response| {
                connection.path = request.uri().path().into();
                connection.query = request.uri().query().map(str::to_owned);
                connection.authorization = request.headers().contains_key("authorization");
                if plan.reject {
                    return Err(tokio_tungstenite::tungstenite::http::Response::builder()
                        .status(401)
                        .body(Some("synthetic authentication rejection".into()))
                        .unwrap());
                }
                Ok(response)
            }),
        )
        .await?;
        if plan.reject {
            if accepted.is_ok() {
                return Err("rejection fixture accepted WebSocket".into());
            }
            trace.connections.push(connection);
            trace.completed += 1;
            continue;
        }
        let mut ws = accepted?;
        let setup = message(&mut ws).await?.ok_or("missing setup")?;
        connection.setup = match setup {
            Message::Text(text) => serde_json::from_str(&text)?,
            Message::Binary(bytes) => serde_json::from_slice(&bytes)?,
            _ => return Err("first frame was not setup JSON".into()),
        };
        trace.connections.push(connection);
        let connection = trace.connections.last_mut().unwrap();
        for step in plan.steps {
            match step {
                Step::Send(value) => {
                    if value.get("setupComplete").is_some() {
                        connection.ack = true;
                    }
                    timeout(WAIT, ws.send(Message::Text(value.to_string().into()))).await??;
                }
                Step::Raw(text) => {
                    timeout(WAIT, ws.send(Message::Text(text.into()))).await??;
                }
                Step::Read => read(&mut ws, connection).await?,
                Step::Signal(signal) => signal.notify_one(),
                Step::Wait(signal) => signalled(&signal).await?,
                Step::Ping => {
                    ws.send(Message::Ping(vec![91, 95].into())).await?;
                }
                Step::Pong => loop {
                    match message(&mut ws).await?.ok_or("missing handshake pong")? {
                        Message::Pong(bytes) if bytes.as_ref() == [91, 95] => break,
                        Message::Text(text) => record(connection, serde_json::from_str(&text)?),
                        Message::Binary(bytes) => {
                            record(connection, serde_json::from_slice(&bytes)?)
                        }
                        _ => return Err("unexpected handshake pong frame".into()),
                    }
                },
                Step::Close(code) => {
                    let frame = code.map(|code| CloseFrame {
                        code: CloseCode::from(code),
                        reason: "synthetic peer close".into(),
                    });
                    timeout(WAIT, ws.send(Message::Close(frame))).await??;
                }
                Step::Drop => break,
                Step::Closed => closed(&mut ws, connection).await?,
            }
        }
        drop(ws);
        trace.completed += 1;
    }
    finished.notify_one();
    // Retain the listener until the client owner has joined. Any additional
    // attempt is counted and closed, rather than hidden by listener teardown.
    loop {
        let (extra, _) = listener.accept().await?;
        trace.extra_connections += 1;
        drop(extra);
    }
}
async fn peer(planned: Vec<Plan>, mut config: GeminiConfig) -> Result<(Peer, GeminiConfig), Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    config.endpoint = format!("ws://{}/live", listener.local_addr()?);
    let finished = notify();
    let completed = finished.clone();
    let stop = notify();
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
async fn settle<T>(mut task: JoinHandle<T>) -> Result<T, Error> {
    match timeout(WAIT, &mut task).await {
        Ok(result) => Ok(result?),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("peer owner deadline; aborted and joined".into())
        }
    }
}
async fn cleanup(
    session: Option<GeminiSession>,
    peer: Peer,
) -> (Result<(), Error>, Result<Trace, Error>) {
    let session = match session {
        Some(session) => {
            let shutdown = session.shutdown();
            tokio::pin!(shutdown);
            match timeout(WAIT, &mut shutdown).await {
                Ok(result) => result.map_err(Into::into),
                Err(_) => {
                    // Keep ownership of the cancellation/join future. A deadline
                    // is a fixture failure, never permission to detach its worker.
                    let _ = shutdown.await;
                    Err("session shutdown deadline; subsequently joined".into())
                }
            }
        }
        None => Ok(()),
    };
    // Let the peer observe the actual close before stopping its listener.
    let _ = signalled(&peer.finished).await;
    peer.stop.notify_one();
    let trace = settle(peer.task).await;
    (session, trace)
}
fn checked(cleanup: (Result<(), Error>, Result<Trace, Error>), connections: usize) -> Trace {
    eprintln!("Settled Gemini resumption owners: {cleanup:#?}");
    cleanup.0.unwrap();
    let trace = cleanup.1.unwrap();
    assert!(trace.error.is_none(), "{trace:#?}");
    assert_eq!(trace.connections.len(), connections, "{trace:#?}");
    assert_eq!(
        trace.completed, connections,
        "peer plans did not reach owned terminal barriers: {trace:#?}"
    );
    assert_eq!(trace.extra_connections, 0, "{trace:#?}");
    for connection in &trace.connections {
        assert_eq!(connection.path, "/live");
        assert_eq!(
            connection.query.as_deref(),
            Some("key=synthetic-resumption-key")
        );
        assert!(!connection.authorization);
        assert_eq!(
            connection.before_ack, 0,
            "commands reached wire before setupComplete"
        );
    }
    trace
}
async fn next(session: &mut GeminiSession) -> Result<GeminiEvent, Error> {
    Ok(timeout(WAIT, session.next_event())
        .await?
        .ok_or("missing Gemini event")??)
}
async fn terminal(session: &mut GeminiSession) -> Result<Option<GeminiError>, Error> {
    loop {
        match timeout(WAIT, session.next_event()).await? {
            Some(Ok(_)) => {}
            Some(Err(error)) => return Ok(Some(error)),
            None => return Ok(None),
        }
    }
}
fn text(sender: &GeminiSender, value: &str) -> Result<(), Error> {
    Ok(sender.try_send(Action::RealtimeText(value.into()), Origin::default())?)
}
fn origin(id: u64, context: &str) -> Origin {
    Origin {
        frame_id: Some(id),
        context_id: Some(context.into()),
    }
}
fn wire_texts(trace: &Connection) -> Vec<&str> {
    trace
        .wire
        .iter()
        .filter_map(|value| value["realtimeInput"]["text"].as_str())
        .collect()
}

#[test]
fn policy_bounds_are_checked_before_connect() {
    for (reconnects, bytes, valid) in [
        (0, 4096, false),
        (9, 4096, false),
        (2, 0, false),
        (2, 65537, false),
        (1, 1, true),
        (8, 65536, true),
    ] {
        let result = GeminiClient::new(config())
            .unwrap()
            .with_session_resumption(ResumptionPolicy {
                max_reconnects: reconnects,
                max_handle_bytes: bytes,
            });
        assert_eq!(result.is_ok(), valid, "{reconnects}/{bytes}");
        if !valid {
            assert!(matches!(
                result,
                Err(GeminiError::Invalid("session resumption limits"))
            ));
        }
    }
    assert_eq!(ResumptionPolicy::default().max_reconnects, 2);
    assert_eq!(ResumptionPolicy::default().max_handle_bytes, 4096);
}

#[tokio::test]
async fn default_wire_and_unsupported_resumption_are_unchanged() {
    for (steps, expected) in [
        (vec![ready(), Step::Read, done(), Step::Closed], None),
        (
            vec![ready(), update("not-opted-in"), Step::Closed],
            Some(GeminiError::Unsupported("session resumption")),
        ),
    ] {
        let successful = expected.is_none();
        let (peer, config) = peer(vec![Plan::new(steps)], config()).await.unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(GeminiClient::new(config)?.connect().await?);
            let session = session.as_mut().unwrap();
            if successful {
                text(&session.sender(), "default request")?;
                let event = next(session).await?;
                if event.event != Event::TurnComplete {
                    return Err("expected turn completion".into());
                }
                session.sender().finish().await?;
            }
            terminal(session).await
        }
        .await;
        let trace = checked(cleanup(session, peer).await, 1);
        assert_eq!(work.unwrap(), expected);
        assert!(trace.connections[0].setup["setup"]
            .get("sessionResumption")
            .is_none());
        assert_eq!(
            wire_texts(&trace.connections[0]),
            if successful {
                vec!["default request"]
            } else {
                vec![]
            }
        );
        assert!(trace.connections[0].closed || trace.connections[0].reset);
    }
}

#[tokio::test]
async fn latest_handle_goaway_advisory_no_replay_and_clone_isolation() {
    let replacement = notify();
    let (peer, config) = peer(
        vec![
            Plan::new(vec![
                ready(),
                Step::Read,
                update("old"),
                Step::Send(
                    json!({"sessionResumptionUpdate":{"resumable":true,"newHandle":"latest"},
                "goAway":{"timeLeft":"30s"},"usageMetadata":{"totalTokenCount":95}}),
                ),
                // Reading this request proves GoAway alone did not replace this socket.
                Step::Read,
                Step::Close(Some(1000)),
            ]),
            Plan::new(vec![
                Step::Signal(replacement.clone()),
                ready(),
                Step::Read,
                model("recovered", true),
                Step::Closed,
            ]),
            Plan::new(vec![ready(), Step::Read, done(), Step::Closed]),
        ],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        let client = client(config)?;
        session = Some(client.connect().await?);
        let first = session.as_mut().unwrap();
        text(&first.sender(), "already written")?;
        let marker = next(first).await?;
        text(&first.sender(), "after advisory")?;
        signalled(&replacement).await?;
        text(&first.sender(), "after replacement")?;
        let response = next(first).await?;
        let complete = next(first).await?;
        first.sender().finish().await?;
        let end = terminal(first).await?;
        // next_event reached terminal and joined the first worker before replacement.
        session.take().unwrap().shutdown().await?;
        session = Some(client.clone().connect().await?);
        let independent = session.as_mut().unwrap();
        text(&independent.sender(), "independent session")?;
        let independent_event = next(independent).await?;
        independent.sender().finish().await?;
        let independent_end = terminal(independent).await?;
        Ok((
            marker,
            response,
            complete,
            end,
            independent_event,
            independent_end,
        ))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 3);
    let (marker, response, complete, end, independent, independent_end) = work.unwrap();
    assert!(matches!(
        marker.event,
        Event::Usage(Usage {
            total_token_count: Some(95),
            ..
        })
    ));
    assert!(matches!(response.event, Event::Text { ref text, .. } if text == "recovered"));
    assert_eq!(complete.event, Event::TurnComplete);
    assert_eq!(independent.event, Event::TurnComplete);
    assert_eq!((end, independent_end), (None, None));
    assert_eq!(
        trace.connections[0].setup["setup"]["sessionResumption"],
        json!({})
    );
    assert_eq!(
        trace.connections[1].setup["setup"]["sessionResumption"],
        json!({"handle":"latest"})
    );
    assert_eq!(
        trace.connections[2].setup["setup"]["sessionResumption"],
        json!({})
    );
    assert_eq!(
        wire_texts(&trace.connections[0]),
        ["already written", "after advisory"]
    );
    assert_eq!(wire_texts(&trace.connections[1]), ["after replacement"]);
    assert_eq!(wire_texts(&trace.connections[2]), ["independent session"]);
}

#[tokio::test]
async fn recovery_ack_holds_bounded_commands_then_flushes_fifo_once() {
    let setup = notify();
    let admitted = notify();
    let pong = notify();
    let ack = notify();
    let mut config = config();
    config.limits.queue_capacity = 2;
    let (peer, config) = peer(
        vec![
            Plan::new(vec![
                ready(),
                Step::Read,
                update("bounded"),
                Step::Close(Some(1012)),
            ]),
            Plan::new(vec![
                Step::Signal(setup.clone()),
                Step::Wait(admitted.clone()),
                Step::Ping,
                Step::Pong,
                Step::Signal(pong.clone()),
                Step::Wait(ack.clone()),
                ready(),
                Step::Read,
                Step::Read,
                done(),
                Step::Closed,
            ]),
        ],
        config,
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(config)?.connect().await?);
        let session = session.as_mut().unwrap();
        let sender = session.sender();
        text(&sender, "written before loss")?;
        signalled(&setup).await?;
        text(&sender, "queued one")?;
        text(&sender, "queued two")?;
        let full = sender.try_send(
            Action::RealtimeText("must not fit".into()),
            Origin::default(),
        );
        admitted.notify_one();
        signalled(&pong).await?;
        let still_full = sender.try_send(
            Action::RealtimeText("still must not fit".into()),
            Origin::default(),
        );
        ack.notify_one();
        let complete = next(session).await?;
        sender.finish().await?;
        Ok((full, still_full, complete, terminal(session).await?))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 2);
    let (full, still_full, complete, end) = work.unwrap();
    assert_eq!(full, Err(GeminiError::Backpressure("input")));
    assert_eq!(still_full, full);
    assert_eq!(complete.event, Event::TurnComplete);
    assert_eq!(end, None);
    assert_eq!(wire_texts(&trace.connections[0]), ["written before loss"]);
    assert_eq!(
        wire_texts(&trace.connections[1]),
        ["queued one", "queued two"]
    );
}

#[tokio::test]
async fn absent_revoked_and_invalid_handles_fail_closed() {
    let cases = [
        (None, GeminiError::Closed),
        (Some(json!({"resumable":false})), GeminiError::Closed),
        (
            Some(json!({"resumable":false,"newHandle":"replacement"})),
            GeminiError::Closed,
        ),
        (
            Some(json!({"resumable":true})),
            GeminiError::Protocol("resumable update requires a handle"),
        ),
        (
            Some(json!({"resumable":true,"newHandle":""})),
            GeminiError::Protocol("resumable update requires a handle"),
        ),
        (
            Some(json!({"resumable":false,"newHandle":"012345678901234567890123456789012"})),
            GeminiError::TooLarge,
        ),
        (
            Some(json!({"resumable":true,"newHandle":"012345678901234567890123456789012"})),
            GeminiError::TooLarge,
        ),
    ];
    for (change, expected) in cases {
        let mut steps = vec![ready()];
        if let Some(change) = change {
            steps.push(update("valid"));
            steps.push(Step::Send(
                json!({"sessionResumptionUpdate":change,"usageMetadata":{"totalTokenCount":1}}),
            ));
        }
        if expected == GeminiError::Closed {
            steps.push(Step::Close(Some(1012)));
        } else {
            steps.push(Step::Closed);
        }
        let (peer, config) = peer(vec![Plan::new(steps)], config()).await.unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(
                GeminiClient::new(config)?
                    .with_session_resumption(ResumptionPolicy {
                        max_handle_bytes: 16,
                        ..ResumptionPolicy::default()
                    })?
                    .connect()
                    .await?,
            );
            terminal(session.as_mut().unwrap()).await
        }
        .await;
        let trace = checked(cleanup(session, peer).await, 1);
        assert_eq!(work.unwrap(), Some(expected));
        assert!(trace.connections[0].wire.is_empty());
    }
}

#[tokio::test]
async fn close_codes_and_goaway_are_classified_before_retrying() {
    for (goaway, code, recover) in [
        (false, Some(1000), false),
        (false, None, false),
        (false, Some(1001), true),
        (false, Some(1011), true),
        (false, Some(1012), true),
        (false, Some(1013), true),
        (true, Some(1000), true),
        (true, None, true),
        (true, Some(1002), false),
        (true, Some(1003), false),
        (true, Some(1008), false),
    ] {
        let mut first = vec![ready(), update("close-handle")];
        if goaway {
            first.push(Step::Send(json!({"goAway":{"timeLeft":"1s"}})));
        }
        first.push(Step::Close(code));
        let mut plans = vec![Plan::new(first)];
        if recover {
            plans.push(Plan::new(vec![ready(), done(), Step::Closed]));
        }
        let (peer, config) = peer(plans, config()).await.unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(client(config)?.connect().await?);
            let session = session.as_mut().unwrap();
            if recover {
                let event = next(session).await?;
                if event.event != Event::TurnComplete {
                    return Err("expected recovered turn".into());
                }
                session.sender().finish().await?;
            }
            terminal(session).await
        }
        .await;
        let trace = checked(cleanup(session, peer).await, if recover { 2 } else { 1 });
        assert_eq!(
            work.unwrap(),
            if recover {
                None
            } else {
                Some(GeminiError::Closed)
            },
            "{goaway}/{code:?}"
        );
        if recover {
            assert_eq!(
                trace.connections[1].setup["setup"]["sessionResumption"]["handle"],
                "close-handle"
            );
        }
    }
}

#[tokio::test]
async fn abnormal_eof_retains_handle_until_cumulative_attempt_budget_exhausts() {
    let (peer, config) = peer(
        vec![
            Plan::new(vec![ready(), update("retained"), Step::Drop]),
            Plan::new(vec![ready(), Step::Drop]),
            Plan::new(vec![ready(), Step::Close(Some(1012))]),
        ],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(config)?.connect().await?);
        terminal(session.as_mut().unwrap()).await
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 3);
    assert_eq!(
        work.unwrap(),
        Some(GeminiError::Protocol(
            "session resumption attempts exhausted"
        ))
    );
    for connection in &trace.connections[1..] {
        assert_eq!(
            connection.setup["setup"]["sessionResumption"],
            json!({"handle":"retained"})
        );
        assert!(connection.wire.is_empty());
    }
}

#[tokio::test]
async fn recovery_handshake_timeout_auth_and_parse_errors_are_not_retried() {
    for case in 0..5 {
        let replacement = match case {
            0 => Plan::new(vec![Step::Closed]),
            1 => Plan::reject(),
            2 => Plan::new(vec![model("early content", false), Step::Closed]),
            3 => Plan::new(vec![ready(), Step::Raw("{broken"), Step::Closed]),
            _ => Plan::new(vec![Step::Send(json!({"setupComplete":[]})), Step::Closed]),
        };
        let expected = match case {
            0 => GeminiError::Timeout("setup"),
            1 => GeminiError::Http(401),
            2 => GeminiError::Protocol("expected setupComplete before content"),
            3 => GeminiError::Protocol("invalid JSON"),
            _ => GeminiError::Protocol("setupComplete must be an object"),
        };
        let mut config = config();
        config.limits.setup_timeout = Duration::from_millis(100);
        let (peer, config) = peer(
            vec![
                Plan::new(vec![
                    ready(),
                    update("one-attempt-only"),
                    Step::Close(Some(1012)),
                ]),
                replacement,
            ],
            config,
        )
        .await
        .unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(client(config)?.connect().await?);
            terminal(session.as_mut().unwrap()).await
        }
        .await;
        let trace = checked(cleanup(session, peer).await, 2);
        let actual = work.unwrap();
        assert_eq!(actual, Some(expected), "case {case}");
        assert!(!actual.unwrap().to_string().contains(KEY));
        assert!(trace
            .connections
            .iter()
            .all(|connection| connection.wire.is_empty()));
        if case != 1 {
            assert!(trace.connections[1].closed || trace.connections[1].reset);
        }
    }
}

#[tokio::test]
async fn cancellation_and_finish_wake_held_recovery_handshake_and_join() {
    for finish in [false, true] {
        let setup = notify();
        let (peer, config) = peer(
            vec![
                Plan::new(vec![ready(), update("waiting"), Step::Close(Some(1012))]),
                Plan::new(vec![Step::Signal(setup.clone()), Step::Closed]),
            ],
            config(),
        )
        .await
        .unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(client(config)?.connect().await?);
            let session = session.as_mut().unwrap();
            signalled(&setup).await?;
            let sender = session.sender();
            if finish {
                sender.finish().await?;
            } else {
                sender.cancel();
            }
            terminal(session).await
        }
        .await;
        let trace = checked(cleanup(session, peer).await, 2);
        assert_eq!(
            work.unwrap(),
            if finish {
                Some(GeminiError::Closed)
            } else {
                None
            }
        );
        assert!(trace.connections[1].closed || trace.connections[1].reset);
        assert!(trace.connections[1].wire.is_empty());
    }
}

#[tokio::test]
async fn admitted_finish_disables_reconnection_even_with_valid_handle() {
    let release = notify();
    let (peer, config) = peer(
        vec![Plan::new(vec![
            ready(),
            Step::Read,
            update("valid"),
            usage(8),
            Step::Wait(release.clone()),
            Step::Close(Some(1012)),
        ])],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(config)?.connect().await?);
        let session = session.as_mut().unwrap();
        text(&session.sender(), "busy until provider closes")?;
        let marker = next(session).await?;
        session.sender().finish().await?;
        let rejected = session
            .sender()
            .try_send(Action::RealtimeText("after End".into()), Origin::default());
        release.notify_one();
        Ok((marker, rejected, terminal(session).await?))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 1);
    let (marker, rejected, end) = work.unwrap();
    assert!(matches!(marker.event, Event::Usage(_)));
    assert_eq!(rejected, Err(GeminiError::Closed));
    assert_eq!(end, Some(GeminiError::Closed));
    assert_eq!(
        wire_texts(&trace.connections[0]),
        ["busy until provider closes"]
    );
}

fn declaration() -> FunctionDeclaration {
    FunctionDeclaration {
        name: "lookup".into(),
        description: "synthetic lookup".into(),
        parameters: json!({"type":"object"}),
    }
}
fn calls() -> Step {
    Step::Send(json!({"toolCall":{"functionCalls":[{"name":"lookup","args":{"number":95}}]}}))
}
fn only_call(event: &GeminiEvent) -> Result<FunctionCall, Error> {
    match &event.event {
        Event::ToolCalls(calls) if calls.len() == 1 => Ok(calls[0].clone()),
        _ => Err("expected one tool call".into()),
    }
}
fn respond(sender: &GeminiSender, call: &FunctionCall) -> Result<(), Error> {
    Ok(sender.try_send(
        Action::ToolResponses(vec![FunctionResponse {
            id: call.id.clone(),
            name: call.name.clone(),
            response: json!({"found":true}),
        }]),
        origin(99, "tool response"),
    )?)
}

#[tokio::test]
async fn pending_tools_origins_generations_and_local_ids_survive_once() {
    let setup = notify();
    let ack = notify();
    let mut config = config();
    config.session.tools = vec![declaration()];
    let (peer, config) = peer(
        vec![
            Plan::new(vec![
                ready(),
                Step::Read,
                calls(),
                update("tools"),
                Step::Close(Some(1012)),
            ]),
            Plan::new(vec![
                Step::Signal(setup.clone()),
                Step::Wait(ack.clone()),
                ready(),
                Step::Read,
                calls(),
                Step::Read,
                model("tools complete", true),
                Step::Closed,
            ]),
        ],
        config,
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(config)?.connect().await?);
        let session = session.as_mut().unwrap();
        let sender = session.sender();
        sender.try_send(
            Action::RealtimeText("find it".into()),
            origin(95, "original input"),
        )?;
        let before = next(session).await?;
        let first = only_call(&before)?;
        signalled(&setup).await?;
        // The original pending call is answered while the second ACK is held.
        respond(&sender, &first)?;
        ack.notify_one();
        let after = next(session).await?;
        let second = only_call(&after)?;
        respond(&sender, &second)?;
        let response = next(session).await?;
        let complete = next(session).await?;
        sender.finish().await?;
        Ok((
            before,
            after,
            first,
            second,
            response,
            complete,
            sender.generation(),
            terminal(session).await?,
        ))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 2);
    let (before, after, first, second, response, complete, generation, end) = work.unwrap();
    assert_eq!(first.id_source, ToolCallIdSource::LocalGenerated);
    assert_eq!(second.id_source, ToolCallIdSource::LocalGenerated);
    assert_ne!(first.id, second.id);
    assert_eq!(first.args, json!({"number":95}));
    for event in [&before, &after, &response, &complete] {
        assert_eq!(event.origin, origin(95, "original input"));
        assert_eq!(event.generation, 0);
    }
    assert_eq!(generation, 0);
    assert_eq!(end, None);
    assert_eq!(wire_texts(&trace.connections[0]), ["find it"]);
    assert_eq!(trace.connections[1].wire.len(), 2);
    for (wire, call) in trace.connections[1].wire.iter().zip([first, second]) {
        assert_eq!(
            wire,
            &json!({"toolResponse":{"functionResponses":[{"id":call.id,"name":"lookup","response":{"found":true}}]}})
        );
    }
}

#[tokio::test]
async fn interruption_during_recovery_retires_previous_generation_tool() {
    let setup = notify();
    let ack = notify();
    let mut config = config();
    config.session.tools = vec![declaration()];
    let (peer, config) = peer(
        vec![
            Plan::new(vec![
                ready(),
                Step::Read,
                calls(),
                update("interrupt"),
                Step::Close(Some(1012)),
            ]),
            Plan::new(vec![
                Step::Signal(setup.clone()),
                Step::Wait(ack.clone()),
                ready(),
                Step::Closed,
            ]),
        ],
        config,
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(config)?.connect().await?);
        let session = session.as_mut().unwrap();
        let sender = session.sender();
        sender.try_send(Action::RealtimeText("old request".into()), origin(1, "old"))?;
        let event = next(session).await?;
        let call = only_call(&event)?;
        signalled(&setup).await?;
        sender.interrupt().await;
        // Admission is intentionally permitted; the worker rejects the retired
        // call against its retained pending-tool/generation state after ACK.
        respond(&sender, &call)?;
        ack.notify_one();
        Ok((event, sender.generation(), terminal(session).await?))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 2);
    let (event, generation, end) = work.unwrap();
    assert_eq!(event.generation, 0);
    assert_eq!(generation, 1);
    assert_eq!(
        end,
        Some(GeminiError::Invalid(
            "unknown, canceled, or mismatched tool result"
        ))
    );
    assert!(trace.connections[1].wire.is_empty());
    assert!(trace.connections[1].closed || trace.connections[1].reset);
}

#[tokio::test]
async fn transcription_resumes_audio_then_emits_final_before_finish() {
    let mut config = config();
    config.mode = SessionMode::Transcription;
    // Use the validated STT configuration rather than conversation-only setup.
    config.session = GeminiConfig::transcription(KEY, "source-model").session;
    let (peer, config) = peer(vec![
        Plan::new(vec![ready(), Step::Read,
            Step::Send(json!({"serverContent":{"interimInputTranscription":{"text":"hello","languageCode":"en-US"}}})),
            update("stt"), Step::Drop]),
        Plan::new(vec![ready(), usage(2), Step::Read,
            Step::Send(json!({"serverContent":{"inputTranscription":{"text":"hello there","languageCode":"en-US"}}})), Step::Closed]),
    ], config).await.unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(config)?.connect().await?);
        let session = session.as_mut().unwrap();
        session
            .sender()
            .try_send(Action::Audio(vec![0, 0, 1, 0]), origin(12, "stt"))?;
        let interim = next(session).await?;
        let recovered = next(session).await?;
        // Usage follows the replacement ACK, so finish cannot race the handshake.
        session.sender().finish().await?;
        let final_event = next(session).await?;
        Ok((interim, recovered, final_event, terminal(session).await?))
    }
    .await;
    let trace = checked(cleanup(session, peer).await, 2);
    let (interim, recovered, final_event, end) = work.unwrap();
    assert!(matches!(
        interim.event,
        Event::InputTranscription {
            is_final: false,
            ..
        }
    ));
    assert!(matches!(recovered.event, Event::Usage(_)));
    assert!(
        matches!(&final_event.event, Event::InputTranscription { transcript, is_final: true } if transcript.text == "hello there")
    );
    let mut mapper = GeminiFrameMapper::new(SessionMode::Transcription, "alice");
    let frames = mapper.frames(&final_event);
    assert!(
        matches!(&frames[0].data, FrameData::Transcription(t) if t.text == "hello there" && t.finalized && t.user_id == "alice")
    );
    assert_eq!(end, None);
    assert_eq!(trace.connections[0].wire.len(), 1);
    assert_eq!(
        trace.connections[1].wire,
        [json!({"realtimeInput":{"audioStreamEnd":true}})]
    );
    assert_eq!(
        trace.connections[1].setup["setup"]["sessionResumption"]["handle"],
        "stt"
    );
}

fn audio_event(data: &str, complete: bool) -> Step {
    Step::Send(
        json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{
        "mimeType":"audio/pcm;rate=24000","data":data}}]},"turnComplete":complete}}),
    )
}
async fn output_until(
    worker: &mut PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
    predicate: impl Fn(&Frame) -> bool,
) -> Result<(), Error> {
    loop {
        let frame = timeout(WAIT, worker.recv())
            .await?
            .ok_or("pipeline output closed early")?;
        let found = predicate(&frame.frame);
        frames.push(frame);
        if found {
            return Ok(());
        }
    }
}
async fn join_pipeline(worker: &mut PipelineWorker, graceful: bool) -> Result<WorkerReport, Error> {
    let admission = if graceful {
        worker.end().await
    } else {
        worker.cancel("fixture cleanup").await
    };
    // Even failed terminal admission must not bypass the owned supervisor join.
    let joined = match timeout(WAIT, worker.join()).await {
        Ok(result) => result.map_err(Into::into),
        Err(_) => {
            let _ = worker.cancel("fixture join deadline").await;
            let _ = worker.join().await;
            Err("pipeline join deadline; subsequently joined".into())
        }
    };
    admission?;
    joined
}

#[tokio::test]
async fn pipeline_recovery_preserves_manual_activity_context_and_continuous_tts() {
    let seeded = notify();
    let replacement = notify();
    let mut config = config();
    config.session.activity_detection.disabled = true;
    let (peer, config) = peer(
        vec![
            Plan::new(vec![
                ready(),
                Step::Read,
                done(),
                Step::Signal(seeded.clone()),
                Step::Read,
                Step::Read,
                audio_event("AQACAA==", false),
                update("pipeline"),
                Step::Close(Some(1012)),
            ]),
            Plan::new(vec![
                Step::Signal(replacement.clone()),
                ready(),
                Step::Read,
                Step::Read,
                audio_event("AwAEAA==", true),
                Step::Closed,
            ]),
            // Reuse the processor in a new pipeline: the new session starts empty,
            // while context seeding resets exactly once at the true lifecycle edge.
            Plan::new(vec![
                ready(),
                Step::Read,
                model("fresh lifecycle", true),
                Step::Closed,
            ]),
        ],
        config,
    )
    .await
    .unwrap();
    let mut worker = None;
    let mut frames = Vec::new();
    let mut reused_frames = Vec::new();
    let mut first_report = None;
    let context = Frame::new(FrameData::LlmContext(LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "seed once")],
        ..Default::default()
    }));
    let mut old_audio = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![1, 0, 2, 0], 16000, 1).unwrap(),
    ));
    old_audio
        .metadata
        .insert("context_id".into(), json!("spoken-turn"));
    let mut new_audio = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![3, 0, 4, 0], 16000, 1).unwrap(),
    ));
    new_audio
        .metadata
        .insert("context_id".into(), json!("later-input"));
    let work: Result<(), Error> = async {
        let processor = Arc::new(GeminiLiveProcessor::new(client(config)?));
        worker = Some(
            Pipeline::new(vec![processor.clone()])
                .start(PipelineConfig::default())
                .await?,
        );
        let running = worker.as_mut().unwrap();
        let sender = running.sender();
        sender.send(context.clone(), Direction::Downstream).await?;
        signalled(&seeded).await?;
        sender
            .send(
                Frame::new(FrameData::UserStartedSpeaking),
                Direction::Downstream,
            )
            .await?;
        sender
            .send(old_audio.clone(), Direction::Downstream)
            .await?;
        output_until(running, &mut frames, |f| {
            matches!(f.data, FrameData::TtsAudio(_))
        })
        .await?;
        signalled(&replacement).await?;
        sender.send(context.clone(), Direction::Downstream).await?;
        // Wait for repeated context to traverse the same processor before audio.
        output_until(running, &mut frames, |f| f == &context).await?;
        sender
            .send(new_audio.clone(), Direction::Downstream)
            .await?;
        output_until(running, &mut frames, |f| f == &new_audio).await?;
        sender
            .send(
                Frame::new(FrameData::UserStoppedSpeaking),
                Direction::Downstream,
            )
            .await?;
        output_until(running, &mut frames, |f| {
            matches!(f.data, FrameData::LlmResponseEnd)
        })
        .await?;
        first_report = Some(join_pipeline(running, true).await?);
        worker.take();
        worker = Some(
            Pipeline::new(vec![processor])
                .start(PipelineConfig::default())
                .await?,
        );
        let running = worker.as_mut().unwrap();
        running
            .sender()
            .send(context.clone(), Direction::Downstream)
            .await?;
        output_until(running, &mut reused_frames, |f| {
            matches!(f.data, FrameData::LlmResponseEnd)
        })
        .await?;
        Ok(())
    }
    .await;
    let second_report = match worker.as_mut() {
        Some(worker) => join_pipeline(worker, work.is_ok()).await,
        None => Err("missing pipeline worker".into()),
    };
    drop(worker);
    let trace = checked(cleanup(None, peer).await, 3);
    work.unwrap();
    for report in [first_report.unwrap(), second_report.unwrap()] {
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report.errors.is_empty(), "{report:?}");
    }
    assert_eq!(
        trace.connections[0].wire,
        [
            json!({"clientContent":{"turns":[{"role":"user","parts":[{"text":"seed once"}]}],"turnComplete":true}}),
            json!({"realtimeInput":{"activityStart":{}}}),
            json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":"AQACAA=="}}}),
        ]
    );
    assert_eq!(
        trace.connections[1].wire,
        [
            json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":"AwAEAA=="}}}),
            json!({"realtimeInput":{"activityEnd":{}}}),
        ]
    );
    assert_eq!(
        trace.connections[2].wire,
        [trace.connections[0].wire[0].clone()]
    );
    assert_eq!(
        trace.connections[1].setup["setup"]["sessionResumption"]["handle"],
        "pipeline"
    );
    assert_eq!(
        trace.connections[2].setup["setup"]["sessionResumption"],
        json!({})
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsStarted(_)))
            .count(),
        1
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
            .count(),
        1
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::LlmResponseStart))
            .count(),
        1
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
            .count(),
        1
    );
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAudio(audio) => Some((f, audio)),
            _ => None,
        })
        .collect();
    assert_eq!(audio.len(), 2);
    assert_eq!(audio[0].1.audio, [1, 0, 2, 0]);
    assert_eq!(audio[1].1.audio, [3, 0, 4, 0]);
    for frame in frames.iter().filter(|f| {
        matches!(
            f.frame.data,
            FrameData::TtsStarted(_) | FrameData::TtsAudio(_) | FrameData::TtsStopped(_)
        )
    }) {
        assert_eq!(frame.direction, Direction::Downstream);
        assert_eq!(frame.frame.metadata["source_frame_id"], json!(old_audio.id));
        assert_eq!(frame.frame.metadata["context_id"], "spoken-turn");
        assert_eq!(frame.frame.metadata["gemini_generation"], 0);
    }
    assert!(reused_frames
        .iter()
        .any(|f| matches!(&f.frame.data, FrameData::LlmText(t) if t.text == "fresh lifecycle")));
}

#[tokio::test]
async fn stalled_write_is_terminal_and_never_replayed_or_reconnected() {
    let release = notify();
    let mut config = config();
    config.limits.max_message_bytes = 16 * 1024 * 1024;
    config.limits.write_timeout = Duration::from_millis(50);
    let (peer, config) = peer(
        vec![Plan::new(vec![
            ready(),
            update("write-is-not-safe-to-retry"),
            usage(95),
            Step::Wait(release.clone()),
            Step::Closed,
        ])],
        config,
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(config)?.connect().await?);
        let session = session.as_mut().unwrap();
        let marker = next(session).await?;
        // The peer keeps the connection open but does not read. This valid
        // payload exceeds loopback socket buffering; the write's own deadline
        // is the causal barrier, with no timed guess about entering send().
        text(&session.sender(), &"x".repeat(15 * 1024 * 1024))?;
        Ok((marker, terminal(session).await?))
    }
    .await;
    release.notify_one();
    let trace = checked(cleanup(session, peer).await, 1);
    let (marker, end) = work.unwrap();
    assert!(matches!(marker.event, Event::Usage(_)));
    assert_eq!(end, Some(GeminiError::Timeout("write")));
    assert!(trace.connections[0].closed || trace.connections[0].reset);
    assert!(
        trace.connections[0].wire.is_empty(),
        "partial write must not be interpreted as a full message"
    );
}
