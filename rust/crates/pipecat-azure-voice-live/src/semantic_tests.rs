//! Private native semantic-admission acceptance against synthetic loopback peers.
//! These tests exercise actor ownership, not Azure VAD quality or device playback.
use super::*;
use futures_util::FutureExt;
use pipecat_core::{Direction, Pipeline, PipelineConfig, PipelineWorker};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use std::panic::AssertUnwindSafe;
use tokio::net::TcpListener;

const WAIT: Duration = Duration::from_secs(3);
const QUIET: Duration = Duration::from_millis(35);
type Body<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

enum PeerCommand {
    Send(Value, oneshot::Sender<Result<(), String>>),
    Stop,
}
enum Observed {
    Wire(Value),
    Closed,
}
struct Peer {
    input: mpsc::Sender<PeerCommand>,
    output: mpsc::Receiver<Observed>,
    task: Option<JoinHandle<Result<(), String>>>,
}
impl Peer {
    async fn bind() -> (Self, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/voice-live/realtime",
            listener.local_addr().unwrap()
        );
        let (input, mut commands) = mpsc::channel(32);
        let (events, output) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let (tcp, address) = timeout(WAIT, listener.accept())
                .await
                .map_err(|_| "synthetic accept timed out".to_owned())?
                .map_err(|e| e.to_string())?;
            if !address.ip().is_loopback() {
                return Err("synthetic peer received non-loopback connection".into());
            }
            let mut socket = tokio_tungstenite::accept_async(tcp)
                .await
                .map_err(|e| e.to_string())?;
            socket
                .send(Message::Text(
                    json!({"type":"session.created","session":{"id":"synthetic"}})
                        .to_string()
                        .into(),
                ))
                .await
                .map_err(|e| e.to_string())?;
            let update = timeout(WAIT, socket.next())
                .await
                .map_err(|_| "session update timed out".to_owned())?
                .ok_or("client closed before session update")?
                .map_err(|e| e.to_string())?;
            let update: Value = serde_json::from_str(update.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            if update["type"] != "session.update"
                || update["session"]["turn_detection"]["type"] != "azure_semantic_vad"
            {
                return Err("expected Voice Live semantic session update".into());
            }
            socket
                .send(Message::Text(
                    json!({"type":"session.updated","session":{}})
                        .to_string()
                        .into(),
                ))
                .await
                .map_err(|e| e.to_string())?;
            loop {
                tokio::select! {
                    command = commands.recv() => match command {
                        Some(PeerCommand::Send(value, reply)) => {
                            let result = socket.send(Message::Text(value.to_string().into()))
                                .await.map_err(|e| e.to_string());
                            let failed = result.is_err();
                            let _ = reply.send(result);
                            if failed { return Ok(()); }
                        }
                        Some(PeerCommand::Stop) | None => return Ok(()),
                    },
                    incoming = socket.next() => match incoming {
                        Some(Ok(Message::Text(text))) => {
                            let value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
                            if events.send(Observed::Wire(value)).await.is_err() { return Ok(()); }
                        }
                        Some(Ok(Message::Ping(bytes))) => {
                            socket.send(Message::Pong(bytes)).await.map_err(|e| e.to_string())?;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                            let _ = events.send(Observed::Closed).await;
                            let _ = socket.flush().await;
                            return Ok(());
                        }
                        Some(Ok(_)) => return Err("client emitted non-JSON application data".into()),
                    }
                }
            }
        });
        (
            Self {
                input,
                output,
                task: Some(task),
            },
            endpoint,
        )
    }
    async fn send(&self, value: Value) {
        let (reply, received) = oneshot::channel();
        timeout(WAIT, self.input.send(PeerCommand::Send(value, reply)))
            .await
            .unwrap()
            .unwrap();
        timeout(WAIT, received).await.unwrap().unwrap().unwrap();
    }
    async fn read(&mut self) -> Value {
        match timeout(WAIT, self.output.recv()).await.unwrap().unwrap() {
            Observed::Wire(value) => value,
            Observed::Closed => panic!("peer closed before expected client request"),
        }
    }
    async fn quiet(&mut self) {
        assert!(
            timeout(QUIET, self.output.recv()).await.is_err(),
            "unexpected client inference, clear, result, or premature close"
        );
    }
    async fn closed(&mut self) {
        assert!(matches!(
            timeout(WAIT, self.output.recv()).await.unwrap().unwrap(),
            Observed::Closed
        ));
    }
    async fn join(&mut self) -> Result<(), String> {
        let _ = self.input.try_send(PeerCommand::Stop);
        let mut task = self.task.take().ok_or("peer was already joined")?;
        match timeout(WAIT, &mut task).await {
            Ok(join) => join.map_err(|e| e.to_string())?,
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err("peer required abort before verified join".into())
            }
        }
    }
}
struct Case {
    client: VoiceLiveClient,
    peer: Peer,
    session: Option<VoiceLiveSession>,
    executor: Option<PipelineWorker>,
}
impl Case {
    async fn ready(&mut self) {
        self.session = Some(self.client.connect().await.unwrap());
        assert!(matches!(self.next().await.event, Event::Ready));
    }
    fn session(&mut self) -> &mut VoiceLiveSession {
        self.session.as_mut().unwrap()
    }
    fn sender(&mut self) -> VoiceLiveSender {
        self.session().sender()
    }
    async fn next(&mut self) -> VoiceLiveEvent {
        timeout(WAIT, self.session().next_event())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }
    async fn no_event(&mut self) {
        assert!(
            timeout(QUIET, self.session().next_event()).await.is_err(),
            "provider output crossed unacknowledged native semantic admission"
        );
    }
    fn marker(&mut self) -> LocalFlushEvent {
        self.sender().semantic_admission_barrier().unwrap().unwrap()
    }
    async fn speech_start(&mut self) -> LocalFlushEvent {
        self.peer
            .send(json!({"type":"input_audio_buffer.speech_started","item_id":"synthetic-user"}))
            .await;
        assert!(matches!(self.next().await.event, Event::SpeechStarted));
        let marker = self.marker();
        assert!(!marker.is_set());
        marker
    }
    async fn started(&mut self, id: &str) {
        self.peer
            .send(json!({"type":"response.created","response":{"id":id}}))
            .await;
        self.peer
            .send(json!({"type":"response.output_item.added","response_id":id,
                "item":{"id":"assistant","type":"message","role":"assistant"}}))
            .await;
        assert!(
            matches!(self.next().await.event, Event::ResponseStarted { response_id } if response_id == id)
        );
    }
    async fn complete(&mut self, id: &str) {
        self.peer
            .send(json!({"type":"response.done","response":{"id":id,"status":"completed"}}))
            .await;
        assert!(
            matches!(self.next().await.event, Event::ResponseDone { response_id, .. } if response_id == id)
        );
    }
    async fn terminal(&mut self) -> Result<(), VoiceLiveError> {
        let result = timeout(WAIT, self.sender().wait_finished()).await.unwrap();
        assert!(self.session().session_joined());
        result
    }
}
async fn run_case(
    gated: bool,
    operation_timeout: Duration,
    response_timeout: Duration,
    body: impl for<'a> FnOnce(&'a mut Case) -> Body<'a>,
) {
    let (peer, endpoint) = Peer::bind().await;
    let mut config = VoiceLiveConfig::new(
        endpoint,
        "synthetic-semantic-model",
        crate::VoiceLiveCredentials::ApiKey("invented-semantic-secret-never-live".into()),
        "synthetic-voice",
    );
    config.session.turn_detection = crate::TurnDetection::semantic_vad();
    config.session.tools.push(crate::FunctionTool::new(
        "lookup",
        "Synthetic native callback",
        json!({"type":"object","properties":{"key":{"type":"string"}}}),
    ));
    config.limits.connect_timeout = Duration::from_secs(1);
    config.limits.operation_timeout = operation_timeout;
    config.limits.response_timeout = response_timeout;
    config.limits.close_timeout = Duration::from_millis(100);
    let client = VoiceLiveClient::new(config).unwrap();
    let mut case = Case {
        client: if gated {
            client.with_native_semantic_admission()
        } else {
            client
        },
        peer,
        session: None,
        executor: None,
    };
    let outcome = timeout(
        Duration::from_secs(15),
        AssertUnwindSafe(body(&mut case)).catch_unwind(),
    )
    .await;
    // Cleanup ownership remains outside the assertion future. Real executor,
    // socket actor, and peer are joined even when an assertion panics.
    let executor_join = if let Some(worker) = case.executor.as_mut() {
        timeout(WAIT, async {
            if !worker.has_finished() {
                let _ = worker.cancel("synthetic semantic fixture cleanup").await;
            }
            while worker.recv().await.is_some() {}
            worker.join().await
        })
        .await
        .map_err(|_| "executor cleanup timed out".to_owned())
        .and_then(|result| result.map(|_| ()).map_err(|e| e.to_string()))
    } else {
        Ok(())
    };
    let actor_join = if let Some(session) = case.session.as_mut() {
        let _ = session.shutdown().await;
        session.session_joined()
    } else {
        true
    };
    let peer_join = case.peer.join().await;
    assert_eq!(executor_join, Ok(()), "native executor was not joined");
    assert!(actor_join, "socket actor was not joined");
    assert_eq!(peer_join, Ok(()), "synthetic peer was not joined");
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!("semantic fixture exceeded bounded case deadline"),
    }
}
fn primary(error: &VoiceLiveError) -> &VoiceLiveError {
    match error {
        VoiceLiveError::Cleanup { primary, .. } => primary,
        other => other,
    }
}

#[tokio::test]
async fn adjacent_start_stop_and_response_wait_for_each_exact_native_marker() {
    run_case(true, WAIT, WAIT, |case| Box::pin(async move {
        case.ready().await;
        case.peer.send(json!({"type":"input_audio_buffer.speech_started","item_id":"u"})).await;
        case.peer.send(json!({"type":"input_audio_buffer.speech_stopped","item_id":"u"})).await;
        case.peer.send(json!({"type":"response.created","response":{"id":"adjacent"}})).await;
        case.peer.send(json!({"type":"response.output_item.added","response_id":"adjacent",
            "item":{"id":"assistant","type":"message","role":"assistant"}})).await;
        assert!(matches!(case.next().await.event, Event::SpeechStarted));
        let start = case.marker();
        case.no_event().await;
        let sender = case.sender();
        let wrong = LocalFlushEvent::default();
        assert!(!sender.acknowledge_semantic_barrier(&wrong).unwrap());
        assert!(!wrong.is_set() && !start.is_set());
        case.no_event().await;
        // In one synchronous turn, publish the native interruption and return
        // its exact barrier. The actor must catch up generation before reading.
        sender.interrupt(None);
        assert!(sender.acknowledge_semantic_barrier(&start).unwrap());
        let stopped = case.next().await;
        assert_eq!(stopped.generation, 1);
        assert!(matches!(stopped.event, Event::SpeechStopped));
        let stop = case.marker();
        assert_ne!(start, stop);
        assert!(!sender.acknowledge_semantic_barrier(&start).unwrap());
        case.no_event().await;
        assert!(sender.acknowledge_semantic_barrier(&stop).unwrap());
        let started = case.next().await;
        assert_eq!(started.generation, 1);
        assert!(matches!(started.event, Event::ResponseStarted { response_id } if response_id == "adjacent"));
        case.complete("adjacent").await;
        case.peer.quiet().await;
    })).await;
}

#[tokio::test]
async fn pending_create_cannot_write_before_interruption_and_exact_admission_return() {
    run_case(true, WAIT, WAIT, |case| {
        Box::pin(async move {
            case.ready().await;
            let marker = case.speech_start().await;
            let sender = case.sender();
            sender.send(Action::CreateResponse).await.unwrap();
            case.peer.quiet().await;
            sender.interrupt(None);
            // This is a fresh turn's request after interruption. Its command can be
            // acknowledged, but inference remains blocked by native admission.
            sender.send(Action::CreateResponse).await.unwrap();
            case.peer.quiet().await;
            assert!(!sender
                .acknowledge_semantic_barrier(&LocalFlushEvent::default())
                .unwrap());
            case.peer.quiet().await;
            assert!(sender.acknowledge_semantic_barrier(&marker).unwrap());
            let create = case.peer.read().await;
            assert_eq!(create["type"], "response.create");
            assert_eq!(create.as_object().unwrap().len(), 2);
            case.started("fresh").await;
            case.complete("fresh").await;
            case.peer.quiet().await; // No replay of the pre-interruption request.
        })
    })
    .await;
}

#[tokio::test]
async fn missing_native_marker_has_a_bounded_specific_terminal_timeout_and_real_join() {
    run_case(true, Duration::from_millis(150), WAIT, |case| {
        Box::pin(async move {
            case.ready().await;
            let marker = case.speech_start().await;
            let error = case.terminal().await.unwrap_err();
            assert_eq!(
                primary(&error),
                &VoiceLiveError::Timeout("native semantic admission")
            );
            assert!(!marker.is_set());
            case.peer.closed().await;
        })
    })
    .await;
}

#[tokio::test]
async fn cancel_bypasses_pending_semantic_admission_without_claiming_acknowledgment() {
    run_case(true, WAIT, WAIT, |case| {
        Box::pin(async move {
            case.ready().await;
            let marker = case.speech_start().await;
            let sender = case.sender();
            sender.close();
            case.terminal().await.unwrap();
            assert!(!marker.is_set());
            assert_eq!(
                sender.try_send(Action::CreateResponse),
                Err(VoiceLiveError::Closed)
            );
            case.peer.closed().await;
        })
    })
    .await;
}

#[tokio::test]
async fn finish_abandons_pending_marker_and_unwritten_debt_without_forging_its_ack() {
    run_case(true, WAIT, WAIT, |case| {
        Box::pin(async move {
            case.ready().await;
            let marker = case.speech_start().await;
            let sender = case.sender();
            sender.send(Action::CreateResponse).await.unwrap();
            case.peer.quiet().await;
            sender.finish().await.unwrap();
            case.terminal().await.unwrap();
            assert!(sender.semantic_admission_barrier().unwrap().is_none());
            assert!(!marker.is_set());
            // A late return is swallowed by identity, not converted into a claim
            // that the native round trip completed before Finish.
            assert!(sender.acknowledge_semantic_barrier(&marker).unwrap());
            assert!(!marker.is_set());
            assert!(!sender
                .acknowledge_semantic_barrier(&LocalFlushEvent::default())
                .unwrap());
            case.peer.closed().await;
        })
    })
    .await;
}

#[tokio::test]
async fn finish_pending_marker_drains_existing_response_and_suppresses_new_speech_admission() {
    run_case(true, WAIT, WAIT, |case| {
        Box::pin(async move {
            case.ready().await;
            case.started("active").await;
            let marker = case.speech_start().await;
            let sender = case.sender();
            sender.finish().await.unwrap();
            assert!(!marker.is_set());
            assert!(sender.semantic_admission_barrier().unwrap().is_none());
            case.peer
                .send(json!({"type":"input_audio_buffer.speech_stopped","item_id":"late"}))
                .await;
            case.complete("active").await; // No SpeechStopped event may precede it.
            case.terminal().await.unwrap();
            assert!(sender.acknowledge_semantic_barrier(&marker).unwrap());
            assert!(!marker.is_set());
            case.peer.closed().await;
        })
    })
    .await;
}

#[tokio::test]
async fn finish_with_missing_existing_completion_fails_at_response_drain_not_abandoned_marker() {
    run_case(
        true,
        Duration::from_secs(1),
        Duration::from_millis(250),
        |case| {
            Box::pin(async move {
                case.ready().await;
                case.started("never-completes").await;
                let marker = case.speech_start().await;
                case.sender().finish().await.unwrap();
                let error = case.terminal().await.unwrap_err();
                assert!(matches!(
                    primary(&error),
                    VoiceLiveError::Timeout("response" | "final drain")
                ));
                assert!(!marker.is_set());
                assert!(case
                    .sender()
                    .semantic_admission_barrier()
                    .unwrap()
                    .is_none());
                case.peer.closed().await;
            })
        },
    )
    .await;
}

#[tokio::test]
async fn ordinary_low_level_semantic_client_remains_ungated() {
    run_case(false, WAIT, WAIT, |case| {
        Box::pin(async move {
            case.ready().await;
            for event_type in [
                "input_audio_buffer.speech_started",
                "input_audio_buffer.speech_stopped",
            ] {
                case.peer
                    .send(json!({"type":event_type,"item_id":"u"}))
                    .await;
            }
            assert!(matches!(case.next().await.event, Event::SpeechStarted));
            assert!(case
                .sender()
                .semantic_admission_barrier()
                .unwrap()
                .is_none());
            assert!(matches!(case.next().await.event, Event::SpeechStopped));
            assert!(case
                .sender()
                .semantic_admission_barrier()
                .unwrap()
                .is_none());
            case.started("ungated").await;
            case.complete("ungated").await;
            case.peer.quiet().await;
        })
    })
    .await;
}

#[tokio::test]
async fn finish_pending_marker_delivers_real_native_tool_terminal_then_joins_without_new_inference()
{
    run_case(true, WAIT, WAIT, |case| Box::pin(async move {
        case.ready().await;
        case.peer.send(json!({"type":"response.created","response":{"id":"tool-response"}})).await;
        case.peer.send(json!({"type":"response.output_item.added","response_id":"tool-response",
            "item":{"id":"tool-item","type":"function_call","call_id":"native-call","name":"lookup","arguments":""}})).await;
        case.peer.send(json!({"type":"response.function_call_arguments.done","response_id":"tool-response",
            "item_id":"tool-item","call_id":"native-call","arguments":"{\"key\":\"native\"}"})).await;
        let event = case.next().await;
        let Event::ToolCall { call, .. } = event.event else {
            panic!("expected verified native call admission");
        };
        let sender = case.sender();
        let declaration = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
        sender.register_tool_declaration(&declaration, event.generation).unwrap();
        assert!(sender.enter_tool_executor(&declaration).unwrap());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
        executor.register("lookup", RegisteredTool::new(Arc::new({
            let entered = entered.clone();
            let release = release.clone();
            move |call| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(json!({"native_result":call.arguments["key"]}))
                })
            }
        }))).unwrap();
        let outcomes = executor.outcomes();
        case.executor = Some(Pipeline::new(vec![Arc::new(executor)])
            .start(PipelineConfig::default()).await.unwrap());
        case.executor.as_ref().unwrap().sender()
            .send(declaration, Direction::Downstream).await.unwrap();
        timeout(WAIT, entered.notified()).await.unwrap();
        assert_eq!(outcomes.snapshot().active.len(), 1);
        let marker = case.speech_start().await;
        sender.finish().await.unwrap();
        assert!(!marker.is_set());
        case.peer.send(json!({"type":"input_audio_buffer.speech_stopped","item_id":"late"})).await;
        case.peer.send(json!({"type":"response.done","response":{"id":"tool-response","status":"completed",
            "output":[{"id":"tool-item","type":"function_call","call_id":"native-call","name":"lookup",
                "arguments":"{\"key\":\"native\"}"}]}})).await;
        assert!(matches!(case.next().await.event, Event::ResponseDone { response_id, .. } if response_id == "tool-response"));
        assert!(timeout(QUIET, sender.wait_finished()).await.is_err(),
            "Finish must still own the genuinely executing native callback");
        release.notify_one();
        let terminal = timeout(WAIT, async {
            loop {
                let directed = case.executor.as_mut().unwrap().recv().await.unwrap();
                if directed.frame.native_tool_outcome().unwrap().is_some() {
                    break directed.frame;
                }
            }
        }).await.unwrap();
        assert_eq!(terminal.native_tool_outcome().unwrap().unwrap().state(), ToolCallTerminalState::Completed);
        sender.try_send_native_result(&terminal).unwrap();
        let output = case.peer.read().await;
        assert_eq!(output["type"], "conversation.item.create");
        assert_eq!(output["item"]["type"], "function_call_output");
        assert_eq!(output["item"]["call_id"], "native-call");
        assert_eq!(
            serde_json::from_str::<Value>(output["item"]["output"].as_str().unwrap()).unwrap(),
            json!({"native_result":"native"})
        );
        case.terminal().await.unwrap();
        assert!(!marker.is_set());
        assert!(sender.acknowledge_semantic_barrier(&marker).unwrap());
        assert!(!marker.is_set());
        assert!(outcomes.snapshot().active.is_empty());
        assert_eq!(outcomes.snapshot().total_terminal, 1);
        case.peer.closed().await; // No response.create follows final settlement.
        case.executor.as_ref().unwrap().end().await.unwrap();
    })).await;
}
