//! Ordinary Live non-blocking tools over real loopback WebSockets and pipelines.
//! The small caller-owned task processor below is a test owner, not ToolExecutor.
//! A separate final matrix exercises the production ToolExecutor and its journal.
//! Every peer, session, pipeline and tracked task is joined before assertions.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::{
    async_trait, DirectedFrame, Direction, Frame, FrameData, FrameProcessor,
    FunctionCallResultFrame, Metric, Pipeline, PipelineConfig, PipelineWorker, ProcessorContext,
    ProcessorError, TextFrame, ToolCallEffectStatus, ToolCallEndReason, ToolCallTerminalState,
    WorkerReport, WorkerState,
};
use pipecat_gemini_live::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
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
const MAX_TRACE: usize = 128;
const KEY: &str = "synthetic-async-tools-key";

#[derive(Clone)]
enum Step {
    Send(Value),
    Read,
    Signal(Arc<Notify>),
    Wait(Arc<Notify>),
    /// A Pong proves the worker processed preceding incoming packets without
    /// requiring the application to consume its event queue.
    RoundTrip,
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
    ack: bool,
    before_ack: usize,
    pongs: usize,
    closed: bool,
}
#[derive(Debug, Default)]
struct Trace {
    connections: Vec<Connection>,
    completed: usize,
    extras: usize,
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
fn usage(n: u64) -> Step {
    Step::Send(
        json!({"usageMetadata":{"promptTokenCount":1,"responseTokenCount":1,"totalTokenCount":n}}),
    )
}
fn model(text: &str) -> Step {
    Step::Send(json!({"serverContent":{"modelTurn":{"parts":[{"text":text}]},"turnComplete":true}}))
}
fn calls(items: &[(&str, &str)]) -> Step {
    Step::Send(
        json!({"toolCall":{"functionCalls":items.iter().map(|(id,name)|
        json!({"id":id,"name":name,"args":{"number":98}})).collect::<Vec<_>>()}}),
    )
}
fn cancel(ids: &[&str]) -> Step {
    Step::Send(json!({"toolCallCancellation":{"ids":ids}}))
}
fn declaration(name: &str) -> FunctionDeclaration {
    FunctionDeclaration {
        name: name.into(),
        description: format!("synthetic {name}"),
        parameters: json!({"type":"object","properties":{"number":{"type":"integer"}}}),
    }
}
fn config() -> GeminiConfig {
    let mut c = GeminiConfig::new(KEY, "gemini-3.8-live");
    c.session.tools = vec![declaration("lookup"), declaration("blocking")];
    c.limits.connect_timeout = Duration::from_secs(1);
    c.limits.setup_timeout = Duration::from_secs(1);
    c.limits.write_timeout = Duration::from_secs(1);
    c.limits.finish_timeout = Duration::from_secs(1);
    c
}
fn client(c: GeminiConfig) -> Result<GeminiClient, GeminiError> {
    GeminiClient::new(c)?.with_non_blocking_tools(vec!["lookup".into()])
}
fn origin(id: u64, context: &str) -> Origin {
    Origin {
        frame_id: Some(id),
        context_id: Some(context.into()),
    }
}
fn response(
    id: &str,
    continuing: bool,
    scheduling: ToolResponseScheduling,
) -> NonBlockingFunctionResponse {
    let mut payload = json!({"stage":if continuing {"progress"} else {"final"}});
    if continuing {
        payload["scheduling"] = json!("application-value");
    }
    NonBlockingFunctionResponse {
        id: id.into(),
        name: "lookup".into(),
        response: payload,
        will_continue: continuing,
        scheduling,
    }
}
fn respond(
    sender: &GeminiSender,
    id: &str,
    continuing: bool,
    scheduling: ToolResponseScheduling,
) -> Result<(), Error> {
    sender.try_send(
        Action::NonBlockingToolResponses(vec![response(id, continuing, scheduling)]),
        origin(98, "tool owner"),
    )?;
    Ok(())
}
fn disconnected(e: &WsError) -> bool {
    matches!(
        e,
        WsError::ConnectionClosed
            | WsError::AlreadyClosed
            | WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
    ) || matches!(e, WsError::Io(e) if matches!(e.kind(), std::io::ErrorKind::ConnectionReset |
        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::UnexpectedEof))
}
async fn message(ws: &mut Ws) -> Result<Option<Message>, Error> {
    match timeout(WAIT, ws.next()).await? {
        Some(Ok(m)) => Ok(Some(m)),
        Some(Err(e)) => Err(e.into()),
        None => Ok(None),
    }
}
fn record(c: &mut Connection, v: Value) -> Result<(), Error> {
    if c.wire.len() == MAX_TRACE {
        return Err("wire trace capacity".into());
    }
    if !c.ack {
        c.before_ack += 1;
    }
    c.wire.push(v);
    Ok(())
}
async fn read(ws: &mut Ws, c: &mut Connection) -> Result<(), Error> {
    for _ in 0..MAX_TRACE {
        match message(ws).await?.ok_or("peer EOF before JSON")? {
            Message::Text(t) => {
                record(c, serde_json::from_str(&t)?)?;
                return Ok(());
            }
            Message::Binary(b) => {
                record(c, serde_json::from_slice(&b)?)?;
                return Ok(());
            }
            Message::Ping(b) => {
                timeout(WAIT, ws.send(Message::Pong(b))).await??;
            }
            Message::Pong(_) => {}
            _ => return Err("expected JSON data".into()),
        }
    }
    Err("peer read frame capacity".into())
}
async fn closed(ws: &mut Ws, c: &mut Connection) -> Result<(), Error> {
    for _ in 0..MAX_TRACE {
        match timeout(WAIT, ws.next()).await? {
            None => {
                c.closed = true;
                return Ok(());
            }
            Some(Err(e)) if disconnected(&e) => {
                c.closed = true;
                return Ok(());
            }
            Some(Err(e)) => return Err(e.into()),
            Some(Ok(Message::Close(_))) => {
                c.closed = true;
                match timeout(WAIT, ws.flush()).await? {
                    Ok(()) => {}
                    Err(e) if disconnected(&e) => {}
                    Err(e) => return Err(e.into()),
                }
                return Ok(());
            }
            Some(Ok(Message::Text(t))) => record(c, serde_json::from_str(&t)?)?,
            Some(Ok(Message::Binary(b))) => record(c, serde_json::from_slice(&b)?)?,
            Some(Ok(Message::Ping(b))) => {
                timeout(WAIT, ws.send(Message::Pong(b))).await??;
            }
            Some(Ok(Message::Pong(_))) => {}
            _ => return Err("unexpected peer frame".into()),
        }
    }
    Err("close frame capacity".into())
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
        let mut c = Connection::default();
        let mut ws = timeout(
            WAIT,
            tokio_tungstenite::accept_hdr_async(tcp, |r: &Request, response| {
                c.path = r.uri().path().into();
                c.query = r.uri().query().map(str::to_owned);
                c.authorization = r.headers().contains_key("authorization");
                Ok(response)
            }),
        )
        .await??;
        c.setup = match message(&mut ws).await?.ok_or("missing setup")? {
            Message::Text(t) => serde_json::from_str(&t)?,
            Message::Binary(b) => serde_json::from_slice(&b)?,
            _ => return Err("setup was not JSON".into()),
        };
        trace.connections.push(c);
        let c = trace
            .connections
            .last_mut()
            .ok_or("missing connection trace")?;
        for step in plan {
            match step {
                Step::Send(v) => {
                    if v.get("setupComplete").is_some() {
                        c.ack = true;
                    }
                    timeout(WAIT, ws.send(Message::Text(v.to_string().into()))).await??;
                }
                Step::Read => read(&mut ws, c).await?,
                Step::Signal(n) => n.notify_one(),
                Step::Wait(n) => signalled(&n).await?,
                Step::RoundTrip => {
                    timeout(WAIT, ws.send(Message::Ping(vec![98, 1].into()))).await??;
                    let mut found = false;
                    for _ in 0..MAX_TRACE {
                        match message(&mut ws).await?.ok_or("missing worker pong")? {
                            Message::Pong(b) if b.as_ref() == [98, 1] => {
                                c.pongs += 1;
                                found = true;
                                break;
                            }
                            Message::Text(t) => record(c, serde_json::from_str(&t)?)?,
                            Message::Binary(b) => record(c, serde_json::from_slice(&b)?)?,
                            Message::Ping(b) => {
                                timeout(WAIT, ws.send(Message::Pong(b))).await??;
                            }
                            _ => return Err("unexpected worker pong frame".into()),
                        }
                    }
                    if !found {
                        return Err("worker pong frame capacity".into());
                    }
                }
                Step::Close => {
                    timeout(
                        WAIT,
                        ws.send(Message::Close(Some(CloseFrame {
                            code: CloseCode::from(1012),
                            reason: "synthetic resumable close".into(),
                        }))),
                    )
                    .await??;
                }
                Step::Closed => closed(&mut ws, c).await?,
            }
        }
        trace.completed += 1;
    }
    finished.notify_one();
    loop {
        let (extra, _) = listener.accept().await?;
        trace.extras += 1;
        drop(extra);
        if trace.extras == MAX_TRACE {
            return Err("extra connection capacity".into());
        }
    }
}
async fn peer(planned: Vec<Vec<Step>>, mut c: GeminiConfig) -> Result<(Peer, GeminiConfig), Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    c.endpoint = format!("ws://{}/live", listener.local_addr()?);
    let finished = notify();
    let completed = finished.clone();
    let stop = notify();
    let stopping = stop.clone();
    let task = tokio::spawn(async move {
        let mut trace = Trace::default();
        let error = {
            let work = plans(&listener, planned, &mut trace, &completed);
            tokio::pin!(work);
            tokio::select! { biased; result=&mut work => result.err().map(|e|e.to_string()), _=stopping.notified()=>None }
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
        c,
    ))
}
async fn finish_peer(peer: Peer) -> Result<Trace, Error> {
    let _ = signalled(&peer.finished).await;
    peer.stop.notify_one();
    let mut task = peer.task;
    match timeout(WAIT, &mut task).await {
        Ok(r) => Ok(r?),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("peer deadline; aborted and joined".into())
        }
    }
}
async fn cleanup(
    session: Option<GeminiSession>,
    peer: Peer,
) -> (Result<(), Error>, Result<Trace, Error>) {
    let joined = match session {
        None => Ok(()),
        Some(s) => {
            let join = s.shutdown();
            tokio::pin!(join);
            match timeout(WAIT, &mut join).await {
                Ok(r) => r.map_err(Into::into),
                Err(_) => {
                    let _ = join.await;
                    Err("session deadline; subsequently joined".into())
                }
            }
        }
    };
    (joined, finish_peer(peer).await)
}
fn checked(result: (Result<(), Error>, Result<Trace, Error>), count: usize) -> Trace {
    eprintln!("Joined async-tool owners: {result:#?}");
    result.0.unwrap();
    let trace = result.1.unwrap();
    assert!(trace.error.is_none(), "{trace:#?}");
    assert_eq!(trace.connections.len(), count, "{trace:#?}");
    assert_eq!(trace.completed, count, "{trace:#?}");
    assert_eq!(trace.extras, 0, "{trace:#?}");
    for c in &trace.connections {
        assert_eq!(c.path, "/live");
        assert_eq!(c.query.as_deref(), Some("key=synthetic-async-tools-key"));
        assert!(!c.authorization);
        assert_eq!(c.before_ack, 0, "wire before setupComplete");
    }
    trace
}
async fn next(s: &mut GeminiSession) -> Result<GeminiEvent, Error> {
    Ok(timeout(WAIT, s.next_event())
        .await?
        .ok_or("missing session event")??)
}
async fn until_usage(s: &mut GeminiSession, n: u64) -> Result<Vec<GeminiEvent>, Error> {
    let mut out = vec![];
    for _ in 0..MAX_TRACE {
        let e = next(s).await?;
        let done = matches!(&e.event,Event::Usage(u) if u.total_token_count==Some(n));
        out.push(e);
        if done {
            return Ok(out);
        }
    }
    Err("event trace capacity".into())
}
async fn terminal(s: &mut GeminiSession) -> Result<Option<GeminiError>, Error> {
    for _ in 0..MAX_TRACE {
        match timeout(WAIT, s.next_event()).await? {
            Some(Ok(_)) => {}
            Some(Err(e)) => return Ok(Some(e)),
            None => return Ok(None),
        }
    }
    Err("terminal event capacity".into())
}
async fn rejected_result(
    s: &mut GeminiSession,
    action: Action,
) -> Result<Option<GeminiError>, Error> {
    match s.sender().try_send(action, Origin::default()) {
        Err(error) => Ok(Some(error)),
        Ok(()) => terminal(s).await,
    }
}
fn tool_wire(t: &Connection) -> Vec<&Value> {
    t.wire
        .iter()
        .filter(|v| v.get("toolResponse").is_some())
        .collect()
}
fn call_ids(e: &GeminiEvent) -> Result<Vec<String>, Error> {
    match &e.event {
        Event::NonBlockingToolCalls(c) => Ok(c.iter().map(|c| c.id.clone()).collect()),
        _ => Err("expected non-blocking call group".into()),
    }
}

#[tokio::test]
async fn default_and_selected_declarations_preserve_legacy_wire() {
    let mut traces = vec![];
    for selected in [false, true] {
        let (p, c) = peer(
            vec![vec![
                ready(),
                calls(&[("legacy", "blocking")]),
                Step::Read,
                done(),
                usage(1),
                Step::Closed,
            ]],
            config(),
        )
        .await
        .unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            let c = if selected {
                client(c)?
            } else {
                GeminiClient::new(c)?
            };
            session = Some(c.connect().await?);
            let s = session.as_mut().unwrap();
            let event = next(s).await?;
            s.sender().try_send(
                Action::ToolResponses(vec![FunctionResponse {
                    id: "legacy".into(),
                    name: "blocking".into(),
                    response: json!({"legacy":true}),
                }]),
                Origin::default(),
            )?;
            until_usage(s, 1).await?;
            s.sender().finish().await?;
            Ok((event, terminal(s).await?))
        }
        .await;
        let trace = checked(cleanup(session, p).await, 1);
        let (event, end) = work.unwrap();
        assert!(matches!(event.event, Event::ToolCalls(_)));
        assert_eq!(end, None);
        assert_eq!(
            tool_wire(&trace.connections[0]),
            [
                &json!({"toolResponse":{"functionResponses":[{"id":"legacy","name":"blocking","response":{"legacy":true}}]}})
            ]
        );
        traces.push(trace);
    }
    let default = &traces[0].connections[0].setup;
    let selected = &traces[1].connections[0].setup;
    let declarations = &selected["setup"]["tools"][0]["functionDeclarations"];
    assert_eq!(declarations[0]["behavior"], "NON_BLOCKING");
    assert_eq!(declarations[1]["behavior"], "BLOCKING");
    let mut stripped = selected.clone();
    for index in [0, 1] {
        assert!(default["setup"]["tools"][0]["functionDeclarations"][index]
            .get("behavior")
            .is_none());
        stripped["setup"]["tools"][0]["functionDeclarations"][index]
            .as_object_mut()
            .unwrap()
            .remove("behavior");
    }
    assert_eq!(
        &stripped, default,
        "the opt-in changes only declaration behavior fields"
    );
}

#[tokio::test]
async fn when_idle_intermediates_and_final_keep_one_call_identity() {
    let (p, c) = peer(
        vec![vec![
            ready(),
            calls(&[("progress", "lookup")]),
            Step::Read,
            usage(1),
            Step::Read,
            usage(2),
            Step::Read,
            done(),
            usage(3),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        let e = next(s).await?;
        for (marker, continuing) in [(1, true), (2, true), (3, false)] {
            respond(
                &s.sender(),
                "progress",
                continuing,
                ToolResponseScheduling::WhenIdle,
            )?;
            until_usage(s, marker).await?;
        }
        // The shared call ledger can reject before admission; a queued result
        // is still checked by the worker. Neither may write this closed ID.
        let rejection = rejected_result(
            s,
            Action::NonBlockingToolResponses(vec![response(
                "progress",
                false,
                ToolResponseScheduling::WhenIdle,
            )]),
        )
        .await?;
        Ok((e, rejection))
    }
    .await;
    let trace = checked(cleanup(session, p).await, 1);
    let (e, end) = work.unwrap();
    assert_eq!(call_ids(&e).unwrap(), ["progress"]);
    assert!(matches!(end, Some(GeminiError::Invalid(_))));
    let wire = tool_wire(&trace.connections[0]);
    assert_eq!(wire.len(), 3);
    for (v, continuing) in wire.iter().zip([true, true, false]) {
        assert_eq!(
            v["toolResponse"]["functionResponses"],
            json!([response(
                "progress",
                continuing,
                ToolResponseScheduling::WhenIdle
            )])
        );
    }
    assert_eq!(
        wire[0]["toolResponse"]["functionResponses"][0]["scheduling"],
        "WHEN_IDLE"
    );
    assert_eq!(
        wire[0]["toolResponse"]["functionResponses"][0]["response"]["scheduling"],
        "application-value"
    );
}

#[tokio::test]
async fn silent_final_finishes_without_another_model_turn() {
    let (p, c) = peer(
        vec![vec![
            ready(),
            Step::Read,
            calls(&[("silent", "lookup")]),
            done(),
            usage(1),
            Step::Read,
            usage(2),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        s.sender()
            .try_send(Action::RealtimeText("begin".into()), origin(1, "old"))?;
        let events = until_usage(s, 1).await?;
        respond(&s.sender(), "silent", false, ToolResponseScheduling::Silent)?;
        until_usage(s, 2).await?;
        s.sender().finish().await?;
        Ok((events, terminal(s).await?))
    }
    .await;
    let trace = checked(cleanup(session, p).await, 1);
    let (events, end) = work.unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e.event, Event::NonBlockingToolCalls(_))));
    assert_eq!(
        end, None,
        "SILENT must not require a fabricated second model completion"
    );
    assert_eq!(
        tool_wire(&trace.connections[0]),
        [
            &json!({"toolResponse":{"functionResponses":[response("silent",false,ToolResponseScheduling::Silent)]}})
        ]
    );
    // Finish closes new work, but a claimed call remains usable until its
    // final response or the finite finish deadline. No new model turn is sent.
    let (p, c) = peer(
        vec![vec![
            ready(),
            calls(&[("finish-existing", "lookup")]),
            done(),
            usage(1),
            Step::Read,
            usage(2),
            Step::Read,
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        until_usage(s, 1).await?;
        s.sender().finish().await?;
        respond(
            &s.sender(),
            "finish-existing",
            true,
            ToolResponseScheduling::Silent,
        )?;
        until_usage(s, 2).await?;
        respond(
            &s.sender(),
            "finish-existing",
            false,
            ToolResponseScheduling::Silent,
        )?;
        terminal(s).await
    }
    .await;
    let trace = checked(cleanup(session, p).await, 1);
    assert_eq!(work.unwrap(), None);
    let wire = tool_wire(&trace.connections[0]);
    assert_eq!(wire.len(), 2);
    for (value, continuing) in wire.iter().zip([true, false]) {
        assert_eq!(
            value["toolResponse"]["functionResponses"],
            json!([response(
                "finish-existing",
                continuing,
                ToolResponseScheduling::Silent
            )])
        );
    }
}

#[tokio::test]
async fn silent_old_result_preserves_new_input_origin_before_output_begins() {
    let (p, c) = peer(
        vec![vec![
            ready(),
            Step::Read,
            calls(&[("old", "lookup")]),
            done(),
            usage(1),
            Step::Read,
            usage(2),
            Step::Read,
            model("new answer"),
            usage(3),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        s.sender()
            .try_send(Action::RealtimeText("old input".into()), origin(1, "old"))?;
        let old = until_usage(s, 1).await?;
        s.sender()
            .try_send(Action::RealtimeText("new input".into()), origin(2, "new"))?;
        until_usage(s, 2).await?; // Actual new-input write, before any new output.
        respond(&s.sender(), "old", false, ToolResponseScheduling::Silent)?;
        let new = until_usage(s, 3).await?;
        s.sender().finish().await?;
        Ok((old, new, terminal(s).await?))
    }
    .await;
    let trace = checked(cleanup(session, p).await, 1);
    let (old, new, end) = work.unwrap();
    let call = old
        .iter()
        .find(|e| matches!(e.event, Event::NonBlockingToolCalls(_)))
        .unwrap();
    assert_eq!(call.origin, origin(1, "old"));
    let outputs: Vec<_> = new
        .iter()
        .filter(|e| matches!(e.event, Event::Text { .. } | Event::TurnComplete))
        .collect();
    assert_eq!(outputs.len(), 2);
    for e in outputs {
        assert_eq!(e.origin, origin(2, "new"));
    }
    assert_eq!(end, None);
    assert_eq!(tool_wire(&trace.connections[0]).len(), 1);
    // Manual activity can be active before it is busy or has audio. Produce
    // the first output before another Audio/ActivityEnd can repair its origin.
    let mut manual = config();
    manual.session.activity_detection.disabled = true;
    let (p, c) = peer(
        vec![vec![
            ready(),
            Step::Read,
            calls(&[("old-manual", "lookup")]),
            done(),
            usage(1),
            Step::Read,
            usage(2),
            Step::Read,
            model("manual answer"),
            usage(3),
            Step::Read,
            done(),
            usage(4),
            Step::Closed,
        ]],
        manual,
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        s.sender().try_send(
            Action::RealtimeText("old manual input".into()),
            origin(11, "old-manual"),
        )?;
        until_usage(s, 1).await?;
        s.sender().try_send(
            Action::ActivityStart { preroll: vec![] },
            origin(12, "manual-active"),
        )?;
        until_usage(s, 2).await?;
        respond(
            &s.sender(),
            "old-manual",
            false,
            ToolResponseScheduling::WhenIdle,
        )?;
        let output = until_usage(s, 3).await?;
        s.sender()
            .try_send(Action::ActivityEnd, origin(12, "manual-active"))?;
        until_usage(s, 4).await?;
        s.sender().finish().await?;
        Ok((output, terminal(s).await?))
    }
    .await;
    let trace = checked(cleanup(session, p).await, 1);
    let (output, end) = work.unwrap();
    let outputs: Vec<_> = output
        .iter()
        .filter(|e| matches!(e.event, Event::Text { .. } | Event::TurnComplete))
        .collect();
    assert_eq!(outputs.len(), 2);
    for event in outputs {
        assert_eq!(event.origin, origin(12, "manual-active"));
    }
    assert_eq!(end, None);
    assert_eq!(
        trace.connections[0].wire[1],
        json!({"realtimeInput":{"activityStart":{}}})
    );
    assert_eq!(
        trace.connections[0].wire[2]["toolResponse"]["functionResponses"][0]["scheduling"],
        "WHEN_IDLE"
    );
}

#[tokio::test]
async fn local_interrupt_preserves_selected_identity_and_retires_blocking_call() {
    let (p, c) = peer(
        vec![vec![
            ready(),
            Step::Read,
            calls(&[("keep", "lookup"), ("retire", "blocking")]),
            usage(1),
            Step::Read,
            usage(2),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        let sender = s.sender();
        sender.try_send(Action::RealtimeText("old input".into()), origin(1, "old"))?;
        let events = until_usage(s, 1).await?;
        sender.interrupt().await;
        respond(&sender, "keep", false, ToolResponseScheduling::Silent)?;
        until_usage(s, 2).await?;
        sender.try_send(
            Action::ToolResponses(vec![FunctionResponse {
                id: "retire".into(),
                name: "blocking".into(),
                response: json!({}),
            }]),
            Origin::default(),
        )?;
        Ok((events, sender.generation(), terminal(s).await?))
    }
    .await;
    let trace = checked(cleanup(session, p).await, 1);
    let (events, generation, end) = work.unwrap();
    let selected = events
        .iter()
        .find(|e| matches!(e.event, Event::NonBlockingToolCalls(_)))
        .unwrap();
    assert_eq!(call_ids(selected).unwrap(), ["keep"]);
    assert_eq!(selected.origin, origin(1, "old"));
    assert_eq!(selected.generation, 0);
    assert!(events
        .iter()
        .any(|e| matches!(&e.event,Event::ToolCalls(c) if c.len()==1 && c[0].id=="retire")));
    assert_eq!(generation, 1);
    assert!(matches!(end, Some(GeminiError::Invalid(_))));
    let wire = tool_wire(&trace.connections[0]);
    assert_eq!(wire.len(), 1);
    assert_eq!(
        wire[0]["toolResponse"]["functionResponses"][0]["id"],
        "keep"
    );
}

#[tokio::test]
async fn provider_cancellation_rejects_intermediate_and_final_results() {
    for continuing in [true, false] {
        let trigger = notify();
        let (p, c) = peer(
            vec![vec![
                ready(),
                calls(&[("cancelled", "lookup")]),
                Step::Wait(trigger.clone()),
                cancel(&["cancelled"]),
                usage(1),
                Step::Closed,
            ]],
            config(),
        )
        .await
        .unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(client(c)?.connect().await?);
            let s = session.as_mut().unwrap();
            let first = next(s).await?;
            trigger.notify_one();
            let cancelled = until_usage(s, 1).await?;
            let rejection = rejected_result(
                s,
                Action::NonBlockingToolResponses(vec![response(
                    "cancelled",
                    continuing,
                    ToolResponseScheduling::Silent,
                )]),
            )
            .await?;
            Ok((first, cancelled, rejection))
        }
        .await;
        let trace = checked(cleanup(session, p).await, 1);
        let (first, cancelled, end) = work.unwrap();
        assert_eq!(call_ids(&first).unwrap(), ["cancelled"]);
        assert!(cancelled.iter().any(|e|matches!(&e.event,Event::NonBlockingToolCallsCancelled(c) if c.len()==1 && c[0].id=="cancelled")));
        assert!(matches!(end, Some(GeminiError::Invalid(_))));
        assert!(tool_wire(&trace.connections[0]).is_empty());
    }
}

#[tokio::test]
async fn resumed_pending_call_keeps_intermediate_state_without_replay() {
    let setup = notify();
    let ack = notify();
    let (p,c)=peer(vec![
        vec![ready(),Step::Read,calls(&[("resumed","lookup")]),Step::Read,
            Step::Send(json!({"sessionResumptionUpdate":{"resumable":true,"newHandle":"async-handle"}})),Step::Close],
        vec![Step::Signal(setup.clone()),Step::Wait(ack.clone()),ready(),Step::Read,done(),usage(2),Step::Closed],
    ],config()).await.unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(
            client(c)?
                .with_session_resumption(ResumptionPolicy::default())?
                .connect()
                .await?,
        );
        let s = session.as_mut().unwrap();
        let sender = s.sender();
        sender.try_send(
            Action::RealtimeText("original request".into()),
            origin(7, "original"),
        )?;
        let call = next(s).await?;
        respond(&sender, "resumed", true, ToolResponseScheduling::Silent)?;
        signalled(&setup).await?;
        sender.interrupt().await;
        respond(&sender, "resumed", false, ToolResponseScheduling::Silent)?;
        ack.notify_one();
        until_usage(s, 2).await?;
        sender.finish().await?;
        Ok((call, sender.generation(), terminal(s).await?))
    }
    .await;
    let trace = checked(cleanup(session, p).await, 2);
    let (call, generation, end) = work.unwrap();
    assert_eq!(call_ids(&call).unwrap(), ["resumed"]);
    assert_eq!(call.origin, origin(7, "original"));
    assert_eq!(generation, 1);
    assert_eq!(end, None);
    assert_eq!(
        trace.connections[1].setup["setup"]["sessionResumption"]["handle"],
        "async-handle"
    );
    assert_eq!(trace.connections[0].wire.len(), 2);
    assert_eq!(trace.connections[1].wire.len(), 1);
    for (connection, continuing) in trace.connections.iter().zip([true, false]) {
        assert_eq!(
            tool_wire(connection),
            [
                &json!({"toolResponse":{"functionResponses":[response("resumed",continuing,ToolResponseScheduling::Silent)]}})
            ]
        );
    }
}

#[tokio::test]
async fn invalid_policies_and_response_boundaries_never_reach_wire() {
    for names in [
        vec![],
        vec!["missing".into()],
        vec!["lookup".into(), "lookup".into()],
        vec![String::new()],
    ] {
        assert!(GeminiClient::new(config())
            .unwrap()
            .with_non_blocking_tools(names)
            .is_err());
    }
    assert!(client(config())
        .unwrap()
        .with_thinking(ThinkingPolicy::default())
        .is_err());
    let mut unsupported = config();
    unsupported.session.model = "unrecognized-live-model".into();
    assert!(client(unsupported).is_err());
    let valid = response("valid", false, ToolResponseScheduling::Silent);
    let wire = serde_json::to_value(&valid).unwrap();
    assert_eq!(
        serde_json::from_value::<NonBlockingFunctionResponse>(wire.clone()).unwrap(),
        valid
    );
    for bad in [
        json!({"id":"x","name":"lookup","response":{},"willContinue":false}),
        json!({"id":"x","name":"lookup","response":{},"willContinue":false,"scheduling":"INTERRUPT"}),
        json!({"id":"x","name":"lookup","response":{},"willContinue":false,"scheduling":"SILENT","extra":true}),
    ] {
        assert!(serde_json::from_value::<NonBlockingFunctionResponse>(bad).is_err());
    }
    let mut small = config();
    small.limits.max_message_bytes = 4096;
    small.limits.max_audio_bytes = 1024;
    small.limits.max_pending_tools = 4;
    let (p, c) = peer(
        vec![vec![
            ready(),
            calls(&[("valid", "lookup")]),
            usage(1),
            Step::Read,
            usage(2),
            Step::Closed,
        ]],
        small,
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        until_usage(s, 1).await?;
        let mut empty_id = valid.clone();
        empty_id.id.clear();
        let mut empty_name = valid.clone();
        empty_name.name.clear();
        let mut scalar = valid.clone();
        scalar.response = json!(false);
        let mut large = valid.clone();
        large.response = json!({"large":"x".repeat(4096)});
        let batches = vec![
            vec![],
            vec![valid.clone(), valid.clone()],
            vec![empty_id],
            vec![empty_name],
            vec![scalar],
            vec![large],
            (0..5)
                .map(|i| response(&format!("id{i}"), false, ToolResponseScheduling::Silent))
                .collect(),
        ];
        let rejected: Vec<_> = batches
            .into_iter()
            .map(|b| {
                s.sender()
                    .try_send(Action::NonBlockingToolResponses(b), Origin::default())
                    .is_err()
            })
            .collect();
        respond(&s.sender(), "valid", false, ToolResponseScheduling::Silent)?;
        until_usage(s, 2).await?;
        s.sender().finish().await?;
        Ok((rejected, terminal(s).await?))
    }
    .await;
    let trace = checked(cleanup(session, p).await, 1);
    let (rejected, end) = work.unwrap();
    assert_eq!(rejected, [true; 7]);
    assert_eq!(end, None);
    assert_eq!(
        trace.connections[0].wire,
        [json!({"toolResponse":{"functionResponses":[valid]}})]
    );
    // Correctly shaped but unauthorized IDs/names are call-ledger/worker-state
    // errors. Each probe owns a separate session and records its empty wire.
    for (id, name, tool) in [
        ("missing", "lookup", "lookup"),
        ("valid", "blocking", "lookup"),
        ("valid", "blocking", "blocking"),
    ] {
        let (p, c) = peer(
            vec![vec![
                ready(),
                calls(&[("valid", tool)]),
                usage(1),
                Step::Closed,
            ]],
            config(),
        )
        .await
        .unwrap();
        let mut session = None;
        let work: Result<_, Error> = async {
            session = Some(client(c)?.connect().await?);
            let s = session.as_mut().unwrap();
            until_usage(s, 1).await?;
            let mut r = response(id, false, ToolResponseScheduling::Silent);
            r.name = name.into();
            let admission = s
                .sender()
                .try_send(Action::NonBlockingToolResponses(vec![r]), Origin::default());
            match admission {
                Ok(()) => Ok(terminal(s).await?),
                Err(e) => Ok(Some(e)),
            }
        }
        .await;
        let trace = checked(cleanup(session, p).await, 1);
        assert!(matches!(work.unwrap(), Some(GeminiError::Invalid(_))));
        assert!(trace.connections[0].wire.is_empty());
    }
    let trigger = notify();
    let (p, c) = peer(
        vec![vec![
            ready(),
            calls(&[("finish-held", "lookup")]),
            usage(1),
            Step::Wait(trigger.clone()),
            calls(&[("finish-new", "lookup")]),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        until_usage(s, 1).await?;
        s.sender().finish().await?;
        trigger.notify_one();
        let mut events = vec![];
        for _ in 0..MAX_TRACE {
            match timeout(WAIT, s.next_event()).await? {
                Some(Ok(event)) => events.push(event),
                Some(Err(error)) => return Ok((events, Some(error))),
                None => return Ok((events, None)),
            }
        }
        Err("finish event capacity".into())
    }
    .await;
    let trace = checked(cleanup(session, p).await, 1);
    let (events, end) = work.unwrap();
    assert!(matches!(end, Some(GeminiError::Protocol(_))));
    assert!(!events.iter().any(|event|matches!(&event.event,Event::NonBlockingToolCalls(calls) if calls.iter().any(|call|call.id=="finish-new"))));
    assert!(trace.connections[0].wire.is_empty());
}

#[tokio::test]
async fn queued_unclaimed_calls_survive_local_interrupt() {
    let queued = notify();
    let (p, c) = peer(
        vec![vec![
            ready(),
            calls(&[("queued-a", "lookup"), ("queued-b", "lookup")]),
            Step::RoundTrip,
            Step::Signal(queued.clone()),
            Step::Read,
            usage(1),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        let sender = s.sender();
        signalled(&queued).await?;
        sender.interrupt().await;
        let event = next(s).await?;
        sender.try_send(
            Action::NonBlockingToolResponses(
                ["queued-a", "queued-b"]
                    .iter()
                    .map(|id| response(id, false, ToolResponseScheduling::Silent))
                    .collect(),
            ),
            Origin::default(),
        )?;
        until_usage(s, 1).await?;
        sender.finish().await?;
        Ok((event, terminal(s).await?))
    }
    .await;
    let trace = checked(cleanup(session, p).await, 1);
    let (event, end) = work.unwrap();
    assert_eq!(call_ids(&event).unwrap(), ["queued-a", "queued-b"]);
    assert_eq!(event.generation, 0);
    assert_eq!(end, None);
    assert_eq!(trace.connections[0].pongs, 1);
    assert_eq!(tool_wire(&trace.connections[0]).len(), 1);
}

#[tokio::test]
async fn provider_revocation_filters_unclaimed_queued_calls() {
    let queued = notify();
    let (p, c) = peer(
        vec![vec![
            ready(),
            calls(&[("revoked", "lookup")]),
            cancel(&["revoked"]),
            Step::RoundTrip,
            Step::Signal(queued.clone()),
            usage(1),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let mut session = None;
    let mut worker = None;
    let signals = OwnerSignals::new();
    let mut frames = vec![];
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        signalled(&queued).await?;
        let events = until_usage(s, 1).await?;
        s.sender().finish().await?;
        let end = terminal(s).await?;
        // A revocation can reach a caller that never received the call. Its
        // cancellation path must be idempotent and must not start an owner.
        worker = Some(
            Pipeline::new(vec![Arc::new(ToolOwner::new(signals.clone()))])
                .start(pipeline_config())
                .await?,
        );
        let w = worker.as_mut().unwrap();
        let mut mapper = GeminiFrameMapper::new(SessionMode::Conversation, "fixture");
        for event in &events {
            for frame in mapper.frames(event) {
                if matches!(frame.data, FrameData::FunctionCallCancel(_)) {
                    for _ in 0..2 {
                        w.sender()
                            .send(frame.clone(), Direction::Downstream)
                            .await?;
                        output_until(w, &mut frames, |f| {
                            matches!(f.data, FrameData::FunctionCallCancel(_))
                        })
                        .await?;
                    }
                }
            }
        }
        Ok((events, end))
    }
    .await;
    let joined = match worker.as_mut() {
        Some(w) => join_pipeline(w, work.is_ok()).await,
        None => Err("pipeline did not start".into()),
    };
    let trace = checked(cleanup(session, p).await, 1);
    let (events, end) = work.unwrap();
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty());
    let owner = signals.snapshot();
    assert!(owner.launched.is_empty());
    assert!(owner.started.is_empty());
    assert!(owner.emitted.is_empty());
    assert_eq!(
        owner.cancelled,
        [("revoked".to_owned(), false), ("revoked".to_owned(), false)]
    );
    assert!(
        !events.iter().any(|e| matches!(
            e.event,
            Event::NonBlockingToolCalls(_) | Event::ToolCalls(_)
        )),
        "revoked call was handed to the application: {events:#?}"
    );
    assert!(events.iter().any(|e|matches!(&e.event,Event::NonBlockingToolCallsCancelled(c) if c.len()==1 && c[0].id=="revoked")));
    assert_eq!(end, None);
    assert!(trace.connections[0].wire.is_empty());
    assert_eq!(trace.connections[0].pongs, 1);
}

#[derive(Clone, Debug, Default)]
struct OwnerTrace {
    groups: Vec<Vec<String>>,
    launched: Vec<(String, bool)>,
    started: Vec<String>,
    exited: Vec<(String, bool)>,
    cancelled: Vec<(String, bool)>,
    retired: Vec<String>,
    skipped: Vec<String>,
    emitted: Vec<String>,
}
struct OwnerSignals {
    trace: Mutex<OwnerTrace>,
    changed: Notify,
    release: Arc<Semaphore>,
}
impl OwnerSignals {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            trace: Mutex::new(OwnerTrace::default()),
            changed: Notify::new(),
            release: Arc::new(Semaphore::new(0)),
        })
    }
    fn snapshot(&self) -> OwnerTrace {
        self.trace.lock().unwrap().clone()
    }
}
async fn owner_until(
    owner: &OwnerSignals,
    predicate: impl Fn(&OwnerTrace) -> bool,
) -> Result<(), Error> {
    timeout(WAIT, async {
        loop {
            let changed = owner.changed.notified();
            if predicate(&owner.snapshot()) {
                return;
            }
            changed.await;
        }
    })
    .await?;
    Ok(())
}
struct OwnedCall {
    name: String,
    arguments: Value,
    task: Option<u64>,
    retired: bool,
}
type OwnerRegistry = Arc<Mutex<HashMap<String, OwnedCall>>>;
struct TaskExit {
    owner: Arc<OwnerSignals>,
    registry: OwnerRegistry,
    id: String,
    completed: bool,
}
impl Drop for TaskExit {
    fn drop(&mut self) {
        if let Some(call) = self.registry.lock().unwrap().get_mut(&self.id) {
            call.retired = true;
        }
        self.owner
            .trace
            .lock()
            .unwrap()
            .exited
            .push((self.id.clone(), self.completed));
        self.owner.changed.notify_one();
    }
}
/// A bounded caller-owned asynchronous operation, not a standard ToolExecutor.
/// System Started registers authority before Normal InProgress can spawn work.
/// System Cancel retires even a declared-but-not-started call. One mutex covers
/// authorization, synchronous ctx.spawn, and handle registration, so cancellation
/// cannot fall between those steps. Pipeline join is the actual task join.
struct ToolOwner {
    signals: Arc<OwnerSignals>,
    registry: OwnerRegistry,
}
impl ToolOwner {
    fn new(signals: Arc<OwnerSignals>) -> Self {
        Self {
            signals,
            registry: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
#[async_trait]
impl FrameProcessor for ToolOwner {
    fn name(&self) -> &str {
        "bounded-caller-owned-tool"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            match &frame.data {
                FrameData::FunctionCallsStarted(calls) => {
                    let mut registry = self.registry.lock().unwrap();
                    let mut batch = std::collections::HashSet::new();
                    if registry.len() + calls.len() > 8
                        || calls.iter().any(|call| {
                            registry.contains_key(&call.tool_call_id)
                                || !batch.insert(call.tool_call_id.as_str())
                        })
                    {
                        return Err(ProcessorError::fatal(
                            "fixture declaration capacity or duplicate ID",
                        ));
                    }
                    for call in calls {
                        registry.insert(
                            call.tool_call_id.clone(),
                            OwnedCall {
                                name: call.function_name.clone(),
                                arguments: call.arguments.clone(),
                                task: None,
                                retired: false,
                            },
                        );
                    }
                    let mut trace = self.signals.trace.lock().unwrap();
                    if trace.groups.len() == 8 {
                        return Err(ProcessorError::fatal("fixture group capacity"));
                    }
                    trace
                        .groups
                        .push(calls.iter().map(|c| c.tool_call_id.clone()).collect());
                    self.signals.changed.notify_one();
                }
                FrameData::FunctionCallInProgress(call) => {
                    let mut registry = self.registry.lock().unwrap();
                    let authority = registry
                        .get_mut(&call.tool_call_id)
                        .ok_or_else(|| ProcessorError::fatal("fixture call lacks declaration"))?;
                    if authority.name != call.function_name || authority.arguments != call.arguments
                    {
                        return Err(ProcessorError::fatal("fixture call identity mismatch"));
                    }
                    if authority.retired {
                        let mut trace = self.signals.trace.lock().unwrap();
                        if trace.skipped.len() == 8 {
                            return Err(ProcessorError::fatal("fixture skipped receipt capacity"));
                        }
                        trace.skipped.push(call.tool_call_id.clone());
                        self.signals.changed.notify_one();
                    } else {
                        if authority.task.is_some() {
                            return Err(ProcessorError::fatal("fixture duplicate InProgress"));
                        }
                        let call = call.clone();
                        let id = call.tool_call_id.clone();
                        let owner = self.signals.clone();
                        let task_registry = self.registry.clone();
                        let output = ctx.clone();
                        let metadata = frame.metadata.clone();
                        let policy = call.cancel_on_interruption;
                        // No await occurs between authorization, spawn and storing
                        // the cancellation handle under this same registry lock.
                        let task =
                            ctx.spawn(format!("owned-tool-{id}"), policy, async move {
                                let mut exit = TaskExit {
                                    owner: owner.clone(),
                                    registry: task_registry.clone(),
                                    id: call.tool_call_id.clone(),
                                    completed: false,
                                };
                                owner
                                    .trace
                                    .lock()
                                    .unwrap()
                                    .started
                                    .push(call.tool_call_id.clone());
                                owner.changed.notify_one();
                                let permit =
                                    owner.release.clone().acquire_owned().await.map_err(|_| {
                                        ProcessorError::fatal("fixture release closed")
                                    })?;
                                permit.forget();
                                let mut result = Frame::new(FrameData::FunctionCallResult(
                                    FunctionCallResultFrame {
                                        function_name: call.function_name,
                                        tool_call_id: call.tool_call_id.clone(),
                                        arguments: call.arguments,
                                        result: json!({"owned":true}),
                                        run_llm: Some(false),
                                        is_final: true,
                                        error: None,
                                    },
                                ))
                                .with_interruptible(policy);
                                // The caller owns result survival as well as task survival.
                                result.metadata = metadata;
                                {
                                    let mut registry = task_registry.lock().unwrap();
                                    let authority =
                                        registry.get_mut(&call.tool_call_id).ok_or_else(|| {
                                            ProcessorError::fatal("fixture authority disappeared")
                                        })?;
                                    if authority.retired {
                                        return Ok(());
                                    }
                                    authority.retired = true;
                                    output.try_push(result, Direction::Upstream)?;
                                }
                                owner.trace.lock().unwrap().emitted.push(call.tool_call_id);
                                exit.completed = true;
                                Ok(())
                            })?;
                        authority.task = Some(task);
                        self.signals
                            .trace
                            .lock()
                            .unwrap()
                            .launched
                            .push((id, policy));
                        self.signals.changed.notify_one();
                    }
                }
                FrameData::FunctionCallCancel(call) => {
                    let mut registry = self.registry.lock().unwrap();
                    let mut known = false;
                    let cancelled = if let Some(authority) = registry.get_mut(&call.tool_call_id) {
                        known = true;
                        authority.retired = true;
                        authority.task.is_some_and(|id| ctx.cancel_task(id))
                    } else {
                        false
                    };
                    let mut trace = self.signals.trace.lock().unwrap();
                    if trace.cancelled.len() == 8 {
                        return Err(ProcessorError::fatal("fixture cancel capacity"));
                    }
                    if known {
                        trace.retired.push(call.tool_call_id.clone());
                    }
                    trace.cancelled.push((call.tool_call_id.clone(), cancelled));
                    self.signals.changed.notify_one();
                }
                _ => {}
            }
        }
        ctx.push(frame, direction).await
    }
}
/// The service's public result contract is Downstream. This explicit router
/// returns a caller task's Upstream result to that same service Downstream.
struct ResultLoopback;
#[async_trait]
impl FrameProcessor for ResultLoopback {
    fn name(&self) -> &str {
        "explicit-tool-result-loopback"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let direction = if direction == Direction::Upstream
            && matches!(frame.data, FrameData::FunctionCallResult(_))
        {
            Direction::Downstream
        } else {
            direction
        };
        ctx.push(frame, direction).await
    }
}
async fn output_until(
    worker: &mut PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
    predicate: impl Fn(&Frame) -> bool,
) -> Result<(), Error> {
    for _ in 0..MAX_TRACE {
        let frame = timeout(WAIT, worker.recv())
            .await?
            .ok_or("pipeline output closed early")?;
        let found = predicate(&frame.frame);
        frames.push(frame);
        if found {
            return Ok(());
        }
    }
    Err("pipeline frame capacity".into())
}
async fn join_pipeline(worker: &mut PipelineWorker, graceful: bool) -> Result<WorkerReport, Error> {
    let admission = if graceful {
        timeout(WAIT, worker.end()).await
    } else {
        timeout(WAIT, worker.cancel("fixture cleanup")).await
    };
    if !matches!(&admission, Ok(Ok(()))) {
        let _ = worker.cancel("fixture terminal admission failure").await;
    }
    let joined = match timeout(WAIT, worker.join()).await {
        Ok(r) => r.map_err(Into::into),
        Err(_) => {
            let _ = worker.cancel("fixture join deadline").await;
            let _ = worker.join().await;
            Err("pipeline deadline; subsequently joined".into())
        }
    };
    admission??;
    joined
}
fn pipeline_config() -> PipelineConfig {
    let mut c = PipelineConfig::default();
    c.start.enable_usage_metrics = true;
    c.max_background_tasks = 16;
    c
}
fn input(text: &str) -> Frame {
    Frame::new(FrameData::InputText(TextFrame::new(text)))
}
fn usage_frame(frame: &Frame, n: u64) -> bool {
    matches!(&frame.data,FrameData::Metrics(metrics) if metrics.iter().any(|m|matches!(m,Metric::LlmUsage{total_tokens,..} if *total_tokens==n)))
}

#[tokio::test]
async fn pipeline_interrupt_keeps_selected_owner_and_cancels_default_owner() {
    let written = notify();
    let (p, c) = peer(
        vec![vec![
            ready(),
            Step::Read,
            calls(&[("keep-owner", "lookup"), ("stop-owner", "blocking")]),
            done(),
            usage(1),
            Step::Read,
            Step::Signal(written.clone()),
            usage(2),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let signals = OwnerSignals::new();
    let mut worker = None;
    let mut frames = vec![];
    let work: Result<_, Error> = async {
        let gemini = Arc::new(GeminiLiveProcessor::new(client(c)?));
        worker = Some(
            Pipeline::new(vec![
                Arc::new(ResultLoopback),
                gemini,
                Arc::new(ToolOwner::new(signals.clone())),
            ])
            .start(pipeline_config())
            .await?,
        );
        let w = worker.as_mut().unwrap();
        w.sender()
            .send(input("start owners"), Direction::Downstream)
            .await?;
        owner_until(&signals, |t| t.started.len() == 2).await?;
        w.sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await?;
        output_until(w, &mut frames, |f| {
            matches!(f.data, FrameData::Interruption)
        })
        .await?;
        owner_until(&signals, |t| {
            t.exited.iter().any(|(id, _)| id == "stop-owner")
        })
        .await?;
        signals.release.add_permits(1);
        signalled(&written).await?;
        owner_until(&signals, |t| t.exited.len() == 2).await?;
        output_until(w, &mut frames, |f| usage_frame(f, 2)).await?;
        Ok(())
    }
    .await;
    let joined = match worker.as_mut() {
        Some(w) => join_pipeline(w, work.is_ok()).await,
        None => Err("pipeline did not start".into()),
    };
    let trace = checked((Ok(()), finish_peer(p).await), 1);
    let owner = signals.snapshot();
    work.unwrap();
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{report:#?}");
    assert!(owner.launched.contains(&("keep-owner".into(), false)));
    assert!(owner.launched.contains(&("stop-owner".into(), true)));
    assert!(owner.exited.contains(&("keep-owner".into(), true)));
    assert!(owner.exited.contains(&("stop-owner".into(), false)));
    assert_eq!(owner.emitted, ["keep-owner"]);
    let wire = tool_wire(&trace.connections[0]);
    assert_eq!(wire.len(), 1);
    assert_eq!(
        wire[0],
        &json!({"toolResponse":{"functionResponses":[{"id":"keep-owner","name":"lookup","response":{"owned":true},"willContinue":false,"scheduling":"SILENT"}]}})
    );
}

#[tokio::test]
async fn pipeline_provider_cancel_exits_selected_owner_without_result() {
    let trigger = notify();
    let (p, c) = peer(
        vec![vec![
            ready(),
            Step::Read,
            calls(&[("cancel-owner", "lookup")]),
            Step::Wait(trigger.clone()),
            cancel(&["cancel-owner"]),
            done(),
            usage(1),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let signals = OwnerSignals::new();
    let mut worker = None;
    let mut frames = vec![];
    let work: Result<_, Error> = async {
        worker = Some(
            Pipeline::new(vec![
                Arc::new(ResultLoopback),
                Arc::new(GeminiLiveProcessor::new(client(c)?)),
                Arc::new(ToolOwner::new(signals.clone())),
            ])
            .start(pipeline_config())
            .await?,
        );
        let w = worker.as_mut().unwrap();
        w.sender()
            .send(input("cancel owner"), Direction::Downstream)
            .await?;
        owner_until(&signals, |t| t.started.len() == 1).await?;
        // The selected owner first survives an actual local interruption.
        w.sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await?;
        output_until(w, &mut frames, |f| {
            matches!(f.data, FrameData::Interruption)
        })
        .await?;
        trigger.notify_one();
        output_until(w, &mut frames, |f| {
            matches!(f.data, FrameData::FunctionCallCancel(_))
        })
        .await?;
        owner_until(&signals, |t| t.exited.len() == 1).await?;
        // A permit after the provider's cancellation cannot revive the task.
        signals.release.add_permits(1);
        output_until(w, &mut frames, |f| usage_frame(f, 1)).await?;
        Ok(())
    }
    .await;
    let joined = match worker.as_mut() {
        Some(w) => join_pipeline(w, work.is_ok()).await,
        None => Err("pipeline did not start".into()),
    };
    let trace = checked((Ok(()), finish_peer(p).await), 1);
    let owner = signals.snapshot();
    work.unwrap();
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{report:#?}");
    assert_eq!(owner.launched, [("cancel-owner".to_owned(), false)]);
    assert_eq!(owner.cancelled, [("cancel-owner".to_owned(), true)]);
    assert_eq!(owner.exited, [("cancel-owner".to_owned(), false)]);
    assert!(owner.emitted.is_empty());
    assert!(tool_wire(&trace.connections[0]).is_empty());
    assert!(
        !frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::LlmResponseStart)),
        "non-blocking control delivery must not manufacture a response"
    );
    // The provider may cancel after System Started registered the declaration
    // but before the caller's Normal InProgress reaches its dispatch point.
    let trigger = notify();
    let entered = notify();
    let release = notify();
    let (p, c) = peer(
        vec![vec![
            ready(),
            Step::Read,
            calls(&[("cancel-before-start", "lookup")]),
            Step::Wait(trigger.clone()),
            cancel(&["cancel-before-start"]),
            done(),
            usage(1),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let signals = OwnerSignals::new();
    let mut worker = None;
    let mut frames = vec![];
    let work:Result<_,Error>=async {
        worker=Some(Pipeline::new(vec![Arc::new(ResultLoopback),Arc::new(GeminiLiveProcessor::new(client(c)?)),
            Arc::new(GroupGate{id:"cancel-before-start",native_only:false,entered:entered.clone(),release:release.clone()}),
            Arc::new(ToolOwner::new(signals.clone()))]).start(pipeline_config()).await?);
        let w=worker.as_mut().unwrap(); w.sender().send(input("cancel before caller dispatch"),Direction::Downstream).await?;
        signalled(&entered).await?;
        output_until(w,&mut frames,|f|matches!(&f.data,FrameData::FunctionCallsStarted(calls) if calls.iter().any(|c|c.tool_call_id=="cancel-before-start"))).await?;
        trigger.notify_one();
        output_until(w,&mut frames,|f|matches!(&f.data,FrameData::FunctionCallCancel(c) if c.tool_call_id=="cancel-before-start")).await?;
        release.notify_one();
        output_until(w,&mut frames,|f|matches!(&f.data,FrameData::FunctionCallInProgress(c) if c.tool_call_id=="cancel-before-start")).await?;
        if !frames.iter().any(|f|usage_frame(&f.frame,1)) {output_until(w,&mut frames,|f|usage_frame(f,1)).await?;}
        signals.release.add_permits(1);
        Ok(())
    }.await;
    release.notify_one();
    let joined = match worker.as_mut() {
        Some(w) => join_pipeline(w, work.is_ok()).await,
        None => Err("pipeline did not start".into()),
    };
    let trace = checked((Ok(()), finish_peer(p).await), 1);
    let owner = signals.snapshot();
    work.unwrap();
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{report:#?}");
    assert_eq!(owner.groups, [vec!["cancel-before-start".to_owned()]]);
    assert_eq!(owner.retired, ["cancel-before-start"]);
    assert_eq!(owner.skipped, ["cancel-before-start"]);
    assert_eq!(owner.cancelled, [("cancel-before-start".to_owned(), false)]);
    assert!(owner.launched.is_empty());
    assert!(owner.started.is_empty());
    assert!(owner.exited.is_empty());
    assert!(owner.emitted.is_empty());
    assert!(tool_wire(&trace.connections[0]).is_empty());
}

struct GroupGate {
    id: &'static str,
    native_only: bool,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl FrameProcessor for GroupGate {
    fn name(&self) -> &str {
        "claimed-group-delivery-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::FunctionCallInProgress(call) if call.tool_call_id==self.id)
            && (!self.native_only || frame.native_tool_batch()?.is_some())
        {
            self.entered.notify_one();
            timeout(WAIT, self.release.notified())
                .await
                .map_err(|_| ProcessorError::fatal("group gate deadline"))?;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn claimed_group_survives_interrupt_before_pipeline_delivery() {
    let (p, c) = peer(
        vec![vec![
            ready(),
            calls(&[("claim-a", "lookup"), ("claim-b", "lookup")]),
            Step::Read,
            usage(1),
            Step::Closed,
        ]],
        config(),
    )
    .await
    .unwrap();
    let entered = notify();
    let release = notify();
    let signals = OwnerSignals::new();
    let mut session = None;
    let mut worker = None;
    let mut outputs = vec![];
    let work: Result<_, Error> = async {
        session = Some(client(c)?.connect().await?);
        let s = session.as_mut().unwrap();
        let event = next(s).await?; // The public next_event claim has completed.
        s.sender().interrupt().await;
        let mut mapper = GeminiFrameMapper::new(SessionMode::Conversation, "fixture");
        let frames = mapper.frames(&event);
        worker = Some(
            Pipeline::new(vec![
                Arc::new(GroupGate {
                    id: "claim-a",
                    native_only: false,
                    entered: entered.clone(),
                    release: release.clone(),
                }),
                Arc::new(ToolOwner::new(signals.clone())),
            ])
            .start(pipeline_config())
            .await?,
        );
        let w = worker.as_mut().unwrap();
        // This is a public session -> mapper -> actual pipeline composition,
        // not a private receive-loop hook. Hold the first NORMAL InProgress
        // invocation, not the SYSTEM group declaration: the independent system
        // loop must remain able to deliver the actual interruption receipt.
        for frame in frames.get(..2).ok_or("incomplete mapped group")? {
            w.sender()
                .send(frame.clone(), Direction::Downstream)
                .await?;
        }
        signalled(&entered).await?;
        for frame in frames.iter().skip(2) {
            w.sender()
                .send(frame.clone(), Direction::Downstream)
                .await?;
        }
        w.sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await?;
        output_until(w, &mut outputs, |f| {
            matches!(f.data, FrameData::Interruption)
        })
        .await?;
        release.notify_one();
        owner_until(&signals, |t| t.started.len() == 2).await?;
        signals.release.add_permits(2);
        owner_until(&signals, |t| t.exited.len() == 2).await?;
        // Return the two actual owner outputs to the public session API. The
        // end-to-end GeminiLiveProcessor result mapping is tested separately.
        for _ in 0..2 {
            output_until(w, &mut outputs, |f| {
                matches!(f.data, FrameData::FunctionCallResult(_))
            })
            .await?;
        }
        s.sender().try_send(
            Action::NonBlockingToolResponses(
                ["claim-a", "claim-b"]
                    .iter()
                    .map(|id| response(id, false, ToolResponseScheduling::Silent))
                    .collect(),
            ),
            Origin::default(),
        )?;
        until_usage(s, 1).await?;
        s.sender().finish().await?;
        Ok((event, frames, terminal(s).await?))
    }
    .await;
    release.notify_one();
    let joined = match worker.as_mut() {
        Some(w) => join_pipeline(w, work.is_ok()).await,
        None => Err("pipeline did not start".into()),
    };
    let trace = checked(cleanup(session, p).await, 1);
    let owner = signals.snapshot();
    let (event, frames, end) = work.unwrap();
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{report:#?}");
    assert_eq!(call_ids(&event).unwrap(), ["claim-a", "claim-b"]);
    assert_eq!(frames.len(), 3);
    assert!(frames.iter().all(|f| !f.interruptible));
    assert_eq!(
        owner.groups,
        [vec!["claim-a".to_owned(), "claim-b".to_owned()]]
    );
    assert_eq!(owner.started.len(), 2);
    assert!(owner.launched.iter().all(|(_, cancel)| !*cancel));
    assert!(owner.exited.iter().all(|(_, complete)| *complete));
    assert_eq!(owner.emitted.len(), 2);
    assert_eq!(end, None);
    assert_eq!(tool_wire(&trace.connections[0]).len(), 1);
}

/// Native ToolExecutor publishes Downstream. Return that exact native result
/// once, through the explicit pre-Gemini loopback, without altering its outcome
/// or run_llm policy. The second Downstream visit is consumed, not re-looped.
struct StandardResultReturn {
    sent: Arc<AtomicUsize>,
    returned: Arc<AtomicUsize>,
}
#[async_trait]
impl FrameProcessor for StandardResultReturn {
    fn name(&self) -> &str {
        "one-shot-native-result-return"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(frame.data, FrameData::FunctionCallResult(_))
            && frame.native_tool_outcome()?.is_some()
        {
            if frame.metadata.get("async_tools_result_return") == Some(&json!(true)) {
                self.returned.fetch_add(1, Ordering::SeqCst);
                return Ok(());
            }
            frame
                .metadata
                .insert("async_tools_result_return".into(), json!(true));
            self.sent.fetch_add(1, Ordering::SeqCst);
            return ctx.push(frame, Direction::Upstream).await;
        }
        ctx.push(frame, direction).await
    }
}
struct CallbackExit {
    signals: Arc<OwnerSignals>,
    id: String,
    returned: bool,
}
impl Drop for CallbackExit {
    fn drop(&mut self) {
        self.signals
            .trace
            .lock()
            .unwrap()
            .exited
            .push((self.id.clone(), self.returned));
        self.signals.changed.notify_one();
    }
}

#[tokio::test]
async fn production_executor_cancellation_survival_and_policy_mismatch() {
    // 0: provider cancellation overtakes a held Normal generated InProgress.
    // 1: aligned false registration survives local interruption and really
    //    returns its native result through Gemini to the WebSocket.
    // 2: mismatched true registration is cancelled locally; Gemini's selected
    //    pending call remains unresolved and finish explicitly times out.
    for scenario in 0..3 {
        let trigger = notify();
        let entered = notify();
        let release = notify();
        let mut plan = vec![ready(), Step::Read, calls(&[("standard-call", "lookup")])];
        if scenario == 0 {
            plan.extend([
                Step::Wait(trigger.clone()),
                cancel(&["standard-call"]),
                done(),
                usage(1),
                Step::Closed,
            ]);
        } else if scenario == 1 {
            plan.extend([done(), usage(1), Step::Read, done(), usage(2), Step::Closed]);
        } else {
            plan.extend([done(), usage(1), Step::Closed]);
        }
        let (p, c) = peer(vec![plan], config()).await.unwrap();
        let signals = OwnerSignals::new();
        let effects = Arc::new(AtomicUsize::new(0));
        let sent = Arc::new(AtomicUsize::new(0));
        let returned = Arc::new(AtomicUsize::new(0));
        let mut worker = None;
        let mut journal = None;
        let mut frames: Vec<DirectedFrame> = vec![];
        let work:Result<_,Error>=async {
            let mut executor=ToolExecutor::new(1,8,4096)?;
            let callback_signals=signals.clone(); let effect_count=effects.clone();
            let mut registration=RegisteredTool::new(Arc::new(move |call| {
                let signals=callback_signals.clone(); let effects=effect_count.clone();
                Box::pin(async move {
                    let mut exit=CallbackExit {signals:signals.clone(),id:call.tool_call_id.clone(),returned:false};
                    signals.trace.lock().unwrap().started.push(call.tool_call_id);
                    signals.changed.notify_one();
                    let permit=signals.release.clone().acquire_owned().await.map_err(|_|"callback release closed".to_owned())?;
                    permit.forget();
                    // A synthetic external-effect marker, intentionally after
                    // the gate. Cancellation does not claim a real rollback.
                    effects.fetch_add(1,Ordering::SeqCst);
                    exit.returned=true;
                    Ok(json!({"standard":true}))
                })
            }));
            registration.cancel_on_interruption=scenario==2;
            registration.timeout=WAIT;
            executor.register("lookup",registration)?;
            journal=Some(executor.outcomes());
            let gemini=Arc::new(GeminiLiveProcessor::new(client(c)?));
            worker=Some(Pipeline::new(vec![
                Arc::new(ResultLoopback),gemini.clone(),Arc::new(executor),
                Arc::new(GroupGate {id:"standard-call",native_only:true,entered:entered.clone(),release:release.clone()}),
                Arc::new(StandardResultReturn{sent:sent.clone(),returned:returned.clone()}),
            ]).start(pipeline_config()).await?);
            let w=worker.as_mut().unwrap(); w.sender().send(input("standard executor"),Direction::Downstream).await?;
            signalled(&entered).await?;
            owner_until(&signals,|trace|trace.started.len()==1).await?;
            if scenario==0 {
                // Started has registered and the real callback has entered.
                // The generated Normal InProgress remains blocked downstream,
                // while provider cancellation follows the independent System lane.
                trigger.notify_one();
            } else {
                if !frames.iter().any(|f|usage_frame(&f.frame,1)) {output_until(w,&mut frames,|f|usage_frame(f,1)).await?;}
                w.sender().send(Frame::interruption(),Direction::Downstream).await?;
                output_until(w,&mut frames,|f|matches!(f.data,FrameData::Interruption)).await?;
            }
            if scenario!=1 {
                let cancelled=|f:&Frame|matches!(&f.data,FrameData::FunctionCallCancel(c) if c.tool_call_id=="standard-call")
                    && f.native_tool_outcome().ok().flatten().is_some();
                if !frames.iter().any(|f|cancelled(&f.frame)) {output_until(w,&mut frames,cancelled).await?;}
                owner_until(&signals,|trace|trace.exited.len()==1).await?;
            }
            release.notify_one();
            // Only the provider-cancel scenario needs the held native marker
            // as its overtaking receipt. Local-interruption scenarios establish
            // lifecycle through the entered callback, native journal and wire.
            if scenario==0 {
                let progress=|f:&Frame|matches!(&f.data,FrameData::FunctionCallInProgress(c) if c.tool_call_id=="standard-call" && c.group_id.is_some());
                if !frames.iter().any(|f|progress(&f.frame)) {output_until(w,&mut frames,progress).await?;}
            }
            // Releasing a cancelled callback's gate cannot restart it.
            signals.release.add_permits(1);
            if scenario==1 {
                owner_until(&signals,|trace|trace.exited.len()==1).await?;
                output_until(w,&mut frames,|f|usage_frame(f,2)).await?;
            } else if scenario==0 {
                if !frames.iter().any(|f|usage_frame(&f.frame,1)) {output_until(w,&mut frames,|f|usage_frame(f,1)).await?;}
            } else {
                gemini.sender()?.finish().await?;
                let expected=GeminiError::Timeout("finish").to_string();
                output_until(w,&mut frames,|f|matches!(&f.data,FrameData::Error(error) if error.error==expected)).await?;
            }
            Ok(())
        }.await;
        release.notify_one();
        let joined = match worker.as_mut() {
            Some(w) => join_pipeline(w, work.is_ok()).await,
            None => Err("pipeline did not start".into()),
        };
        let trace = checked((Ok(()), finish_peer(p).await), 1);
        let callback = signals.snapshot();
        work.unwrap();
        let report = joined.unwrap();
        let snapshot = journal.unwrap().snapshot();
        assert_eq!(
            report.state,
            WorkerState::Ended,
            "scenario {scenario}: {report:#?}"
        );
        if scenario == 2 {
            assert!(report
                .errors
                .iter()
                .any(|error| error.error == GeminiError::Timeout("finish").to_string()));
            assert!(
                report
                    .errors
                    .iter()
                    .all(|error| error.error == GeminiError::Timeout("finish").to_string()),
                "{report:#?}"
            );
        } else {
            assert!(report.errors.is_empty(), "scenario {scenario}: {report:#?}");
        }
        assert_eq!(callback.started, ["standard-call"]);
        assert_eq!(
            callback.exited,
            [("standard-call".to_owned(), scenario == 1)]
        );
        assert_eq!(effects.load(Ordering::SeqCst), usize::from(scenario == 1));
        assert!(snapshot.active.is_empty());
        assert_eq!(snapshot.total_terminal, 1);
        assert_eq!(snapshot.terminal.len(), 1);
        assert_eq!(snapshot.unadmitted_terminal, 0);
        let outcome = &snapshot.terminal[0].outcome;
        assert_eq!(outcome.call().tool_call_id, "standard-call");
        assert_eq!(outcome.cancel_on_interruption(), scenario == 2);
        // The real callback definitely entered before either cancellation.
        // Zero gated effects does not turn its native status into NotStarted.
        assert_eq!(
            outcome.effects(),
            ToolCallEffectStatus::ExternalEffectsUnknown
        );
        assert_eq!(
            outcome.state(),
            if scenario == 1 {
                ToolCallTerminalState::Completed
            } else {
                ToolCallTerminalState::Cancelled
            }
        );
        assert_eq!(
            outcome.reason(),
            match scenario {
                0 => ToolCallEndReason::ExplicitCancel,
                1 => ToolCallEndReason::CallbackReturned,
                _ => ToolCallEndReason::TaskDropped,
            }
        );
        assert_eq!(sent.load(Ordering::SeqCst), usize::from(scenario == 1));
        assert_eq!(returned.load(Ordering::SeqCst), usize::from(scenario == 1));
        let wire = tool_wire(&trace.connections[0]);
        if scenario == 1 {
            assert_eq!(
                wire,
                [
                    &json!({"toolResponse":{"functionResponses":[{"id":"standard-call","name":"lookup","response":{"standard":true},"willContinue":false,"scheduling":"WHEN_IDLE"}]}})
                ]
            );
        } else {
            assert!(wire.is_empty(), "scenario {scenario}: {wire:#?}");
        }
    }
}
