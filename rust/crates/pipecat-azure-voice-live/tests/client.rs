//! Synthetic literal-loopback acceptance of the owned Voice Live socket client.
//! No live account, physical playback, or fabricated native tool receipt is used.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{FutureExt, SinkExt, StreamExt};
use pipecat_azure_voice_live::{
    Action, ConversationItem, Event, FunctionTool, InputAudioNoiseReduction, PlaybackPosition,
    ResponseStatus, TurnDetection, VoiceLiveClient, VoiceLiveConfig, VoiceLiveCredentials,
    VoiceLiveError, VoiceLiveEvent, VoiceLiveSession, API_VERSION,
};
use serde_json::{json, Value};
use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::tungstenite::{handshake::server::Request, Message};

const WAIT: Duration = Duration::from_secs(3);
const QUIET: Duration = Duration::from_millis(35);
const CASE_LIMIT: Duration = Duration::from_secs(15);
const SECRET: &str = "invented-voice-live-test-secret-never-an-account";
const MODEL: &str = "synthetic model &+=?/#%";
type CaseFuture<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

enum PeerCommand {
    Send(Message, oneshot::Sender<Result<(), String>>),
    Disconnect(oneshot::Sender<()>),
    Stop,
}
enum PeerEvent {
    Handshake(Box<Request>),
    Wire(Value),
    Closed,
}
struct Peer {
    commands: mpsc::Sender<PeerCommand>,
    events: mpsc::Receiver<PeerEvent>,
    pongs: mpsc::Receiver<Vec<u8>>,
    task: Option<JoinHandle<Result<(), String>>>,
}
impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite's handshake callback signature.
    async fn bind() -> (Self, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "ws://{}/voice-live/realtime",
            listener.local_addr().unwrap()
        );
        let (commands, mut receive) = mpsc::channel(64);
        let (events, output) = mpsc::channel(64);
        let (pongs, received_pongs) = mpsc::channel(8);
        let task = tokio::spawn(async move {
            let (stream, address) = timeout(WAIT, listener.accept())
                .await
                .map_err(|_| "synthetic accept timeout".to_owned())?
                .map_err(|e| e.to_string())?;
            if !address.ip().is_loopback() {
                return Err("test peer accepted a non-loopback address".into());
            }
            let mut request = None;
            let mut socket =
                tokio_tungstenite::accept_hdr_async(stream, |req: &Request, response| {
                    request = Some(req.clone());
                    Ok(response)
                })
                .await
                .map_err(|e| e.to_string())?;
            events
                .send(PeerEvent::Handshake(Box::new(request.unwrap())))
                .await
                .map_err(|_| "test receiver closed".to_owned())?;
            loop {
                tokio::select! {
                    command = receive.recv() => match command {
                        Some(PeerCommand::Send(message, reply)) => {
                            let result = socket.send(message).await.map_err(|e| e.to_string());
                            let failed = result.is_err();
                            let _ = reply.send(result);
                            if failed { return Ok(()); }
                        }
                        Some(PeerCommand::Disconnect(reply)) => {
                            drop(socket);
                            let _ = reply.send(());
                            return Ok(());
                        }
                        Some(PeerCommand::Stop) | None => return Ok(()),
                    },
                    message = socket.next() => match message {
                        Some(Ok(Message::Text(text))) => {
                            let value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
                            if events.send(PeerEvent::Wire(value)).await.is_err() { return Ok(()); }
                        }
                        Some(Ok(Message::Ping(bytes))) => {
                            socket.send(Message::Pong(bytes)).await.map_err(|e| e.to_string())?;
                        }
                        Some(Ok(Message::Pong(bytes))) => {
                            if pongs.send(bytes.to_vec()).await.is_err() { return Ok(()); }
                        }
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                            let _ = events.send(PeerEvent::Closed).await;
                            let _ = socket.flush().await;
                            return Ok(());
                        }
                        Some(Ok(_)) => return Err("client sent non-JSON application data".into()),
                    }
                }
            }
        });
        (
            Self {
                commands,
                events: output,
                pongs: received_pongs,
                task: Some(task),
            },
            endpoint,
        )
    }
    async fn send_message(&self, message: Message) {
        let (reply, received) = oneshot::channel();
        timeout(WAIT, self.commands.send(PeerCommand::Send(message, reply)))
            .await
            .expect("peer command timeout")
            .expect("peer exited before sending fixture");
        timeout(WAIT, received)
            .await
            .expect("peer write timeout")
            .expect("peer dropped write acknowledgment")
            .expect("peer fixture write failed");
    }
    async fn send(&self, value: Value) {
        self.send_message(Message::Text(value.to_string().into()))
            .await;
    }
    async fn processed_barrier(&mut self) {
        let payload = b"synthetic-actor-processing-barrier";
        self.send_message(Message::Ping(payload.to_vec().into()))
            .await;
        assert_eq!(
            timeout(WAIT, self.pongs.recv()).await.unwrap().unwrap(),
            payload
        );
    }
    async fn event(&mut self) -> PeerEvent {
        timeout(WAIT, self.events.recv())
            .await
            .expect("client wire timeout")
            .expect("peer event stream ended")
    }
    async fn handshake(&mut self) -> Request {
        match self.event().await {
            PeerEvent::Handshake(request) => *request,
            _ => panic!("expected HTTP handshake before application data"),
        }
    }
    async fn read(&mut self) -> Value {
        match self.event().await {
            PeerEvent::Wire(value) => value,
            _ => panic!("expected JSON client request"),
        }
    }
    async fn quiet(&mut self) {
        assert!(
            timeout(QUIET, self.events.recv()).await.is_err(),
            "unexpected client request or premature transport close"
        );
    }
    async fn closed(&mut self) {
        assert!(matches!(self.event().await, PeerEvent::Closed));
    }
    async fn disconnect(&self) {
        let (reply, received) = oneshot::channel();
        self.commands
            .send(PeerCommand::Disconnect(reply))
            .await
            .unwrap();
        timeout(WAIT, received).await.unwrap().unwrap();
    }
    /// Called after every case, including assertion panic or timeout. A timed-out
    /// peer is aborted AND awaited, never detached from the test runtime.
    async fn join(&mut self) -> Result<(), String> {
        let _ = self.commands.try_send(PeerCommand::Stop);
        let Some(mut task) = self.task.take() else {
            return Err("peer was already joined".into());
        };
        match timeout(WAIT, &mut task).await {
            Ok(result) => result.map_err(|e| e.to_string())?,
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
    request: Option<Request>,
    update: Option<Value>,
}
impl Case {
    async fn ready(&mut self) {
        let client = self.client.clone();
        let setup = async {
            self.request = Some(self.peer.handshake().await);
            self.peer
                .send(json!({"type":"session.created","session":{"id":"synthetic-session"}}))
                .await;
            self.update = Some(self.peer.read().await);
            self.peer.send(json!({"type":"session.updated","session":{"id":"synthetic-session","model":"server-canonical-alias"}})).await;
        };
        let (result, ()) = tokio::join!(client.connect(), setup);
        self.session = Some(result.expect("synthetic session readiness failed"));
        assert!(matches!(self.next().await.event, Event::Ready));
    }
    fn session(&mut self) -> &mut VoiceLiveSession {
        self.session.as_mut().expect("ready session")
    }
    async fn next(&mut self) -> VoiceLiveEvent {
        timeout(WAIT, self.session().next_event())
            .await
            .expect("client event timeout")
            .expect("client event stream ended")
            .expect("client failed before expected event")
    }
    async fn send(&mut self, action: Action) {
        self.session().sender().send(action).await.unwrap();
    }
    async fn create(&mut self, response_id: &str) -> String {
        self.send(Action::CreateResponse).await;
        let request = self.peer.read().await;
        assert_eq!(request["type"], "response.create");
        assert_eq!(request.as_object().unwrap().len(), 2);
        assert!(request.get("response").is_none());
        let event_id = request["event_id"].as_str().unwrap().to_owned();
        self.peer
            .send(json!({"type":"response.created","response":{"id":response_id}}))
            .await;
        event_id
    }
    async fn announce_message(&self, response_id: &str, item_id: &str) {
        self.peer
            .send(
                json!({"type":"response.output_item.added","response_id":response_id,
            "item":{"id":item_id,"type":"message","role":"assistant"}}),
            )
            .await;
    }
    async fn started(&mut self, response_id: &str, item_id: &str) {
        self.create(response_id).await;
        self.announce_message(response_id, item_id).await;
        assert!(
            matches!(self.next().await.event, Event::ResponseStarted { response_id: id } if id == response_id)
        );
    }
    async fn complete(&mut self, response_id: &str) {
        self.peer
            .send(
                json!({"type":"response.done","response":{"id":response_id,"status":"completed"}}),
            )
            .await;
        assert!(
            matches!(self.next().await.event, Event::ResponseDone { response_id: id, status: ResponseStatus::Completed, .. } if id == response_id)
        );
    }
    async fn failed(&mut self) -> VoiceLiveError {
        let error = timeout(WAIT, self.session().sender().wait_finished())
            .await
            .expect("failed actor did not terminate")
            .expect_err("invalid provider traffic must fail closed");
        assert!(
            self.session().session_joined(),
            "terminal must follow actual actor join"
        );
        if let VoiceLiveError::Cleanup { primary, cleanup } = error {
            assert_eq!(self.session().cleanup_error(), Some(*cleanup));
            *primary
        } else {
            error
        }
    }
}
async fn run_case(
    configure: impl FnOnce(&mut VoiceLiveConfig),
    body: impl for<'a> FnOnce(&'a mut Case) -> CaseFuture<'a>,
) {
    let (peer, endpoint) = Peer::bind().await;
    let mut config = VoiceLiveConfig::new(
        endpoint,
        MODEL,
        VoiceLiveCredentials::ApiKey(SECRET.into()),
        "synthetic-Azure-voice",
    );
    config.limits.connect_timeout = Duration::from_secs(1);
    config.limits.operation_timeout = Duration::from_secs(1);
    config.limits.response_timeout = Duration::from_secs(4);
    config.limits.close_timeout = Duration::from_millis(100);
    configure(&mut config);
    let mut case = Case {
        client: VoiceLiveClient::new(config).unwrap(),
        peer,
        session: None,
        request: None,
        update: None,
    };
    let outcome = timeout(CASE_LIMIT, AssertUnwindSafe(body(&mut case)).catch_unwind()).await;
    let actor_join = if let Some(session) = case.session.as_mut() {
        let _ = session.shutdown().await;
        session.session_joined()
    } else {
        true
    };
    let peer_join = case.peer.join().await;
    assert!(actor_join, "case cleanup did not join socket actor");
    assert_eq!(peer_join, Ok(()), "case cleanup did not join loopback peer");
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!("synthetic client case exceeded its bounded deadline"),
    }
}
fn offered_tool(config: &mut VoiceLiveConfig) {
    config.session.tools.push(FunctionTool::new(
        "lookup",
        "Synthetic local lookup",
        json!({"type":"object","properties":{"key":{"type":"string"}}}),
    ));
}
fn function_item(item_id: &str, call_id: &str) -> Value {
    json!({"id":item_id,"type":"function_call","call_id":call_id,"name":"lookup","arguments":""})
}
fn audio(response_id: &str, item_id: &str, content_index: u32, bytes: &[u8]) -> Value {
    json!({"type":"response.audio.delta","response_id":response_id,"item_id":item_id,
        "content_index":content_index,"delta":STANDARD.encode(bytes)})
}
fn position(item_id: &str, content_index: u32, audio_end_ms: u32) -> PlaybackPosition {
    PlaybackPosition {
        item_id: item_id.into(),
        content_index,
        audio_end_ms,
    }
}
fn primary(error: &VoiceLiveError) -> &VoiceLiveError {
    match error {
        VoiceLiveError::Cleanup { primary, .. } => primary,
        other => other,
    }
}

#[tokio::test]
async fn actual_handshakes_use_exclusive_key_or_bearer_and_flat_manual_session() {
    for bearer in [false, true] {
        run_case(
            |config| {
                if bearer {
                    config.credentials = VoiceLiveCredentials::EntraBearer(SECRET.into());
                }
                config.session.instructions = Some("Synthetic instructions".into());
                config.session.input_audio_noise_reduction =
                    Some(InputAudioNoiseReduction::default());
            },
            move |case| {
                Box::pin(async move {
                    case.ready().await;
                    let request = case.request.as_ref().unwrap();
                    assert_eq!(request.uri().path(), "/voice-live/realtime");
                    let url = url::Url::parse(&format!("ws://127.0.0.1{}", request.uri())).unwrap();
                    assert_eq!(
                        url.query_pairs().collect::<Vec<_>>(),
                        vec![
                            ("api-version".into(), API_VERSION.into()),
                            ("model".into(), MODEL.into())
                        ]
                    );
                    let (selected, excluded, expected) = if bearer {
                        ("authorization", "api-key", format!("Bearer {SECRET}"))
                    } else {
                        ("api-key", "authorization", SECRET.into())
                    };
                    assert_eq!(request.headers()[selected], expected);
                    assert_eq!(request.headers().get_all(selected).iter().count(), 1);
                    assert!(!request.headers().contains_key(excluded));
                    assert!(!request.headers().contains_key("openai-beta"));
                    assert!(!request.headers().contains_key("api-version"));
                    let update = case.update.as_ref().unwrap();
                    assert_eq!(update["type"], "session.update");
                    assert!(update["event_id"].as_str().is_some());
                    let session = &update["session"];
                    assert_eq!(session["modalities"], json!(["text", "audio"]));
                    assert_eq!(
                        session["voice"],
                        json!({"type":"azure-standard","name":"synthetic-Azure-voice"})
                    );
                    assert_eq!(session["input_audio_format"], "pcm16");
                    assert_eq!(session["input_audio_sampling_rate"], 24_000);
                    assert_eq!(session["output_audio_format"], "pcm16");
                    assert_eq!(
                        session["input_audio_transcription"],
                        json!({"model":"azure-speech"})
                    );
                    assert_eq!(
                        session["input_audio_noise_reduction"],
                        json!({"type":"azure_deep_noise_suppression"})
                    );
                    assert_eq!(session.get("turn_detection"), Some(&Value::Null));
                    for ga_field in ["model", "type", "audio", "output_modalities"] {
                        assert!(
                            session.get(ga_field).is_none(),
                            "unexpected GA field: {ga_field}"
                        );
                    }
                    assert!(!format!("{:?}", case.client).contains(SECRET));
                    case.create("manual").await;
                    case.complete("manual").await;
                })
            },
        )
        .await;
    }
}

#[tokio::test]
async fn readiness_waits_for_both_announcements_without_premature_client_work() {
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                let client = case.client.clone();
                let connect = client.connect();
                tokio::pin!(connect);
                {
                    let setup = async {
                        let request = case.peer.handshake().await;
                        assert_eq!(request.uri().path(), "/voice-live/realtime");
                        case.peer.quiet().await;
                        case.peer
                            .send(json!({"type":"session.created","session":{}}))
                            .await;
                        assert_eq!(case.peer.read().await["type"], "session.update");
                        case.peer.quiet().await;
                    };
                    tokio::pin!(setup);
                    tokio::select! {
                        () = &mut setup => {},
                        _ = &mut connect => panic!("connect returned before session.updated"),
                    }
                }
                // Releasing this acknowledgment is the only path to an owned ready session.
                case.peer
                    .send(json!({"type":"session.updated","session":{}}))
                    .await;
                case.session = Some(timeout(WAIT, connect).await.unwrap().unwrap());
                assert!(matches!(case.next().await.event, Event::Ready));
                case.peer.quiet().await;
            })
        },
    )
    .await;
}

#[tokio::test]
async fn out_of_order_readiness_and_pre_ready_provider_work_fail_closed() {
    for first in [
        json!({"type":"session.updated","session":{}}),
        json!({"type":"response.created","response":{"id":"too-early"}}),
    ] {
        run_case(
            |_| {},
            move |case| {
                Box::pin(async move {
                    let client = case.client.clone();
                    let setup = async {
                        case.peer.handshake().await;
                        case.peer.send(first).await;
                    };
                    let (result, ()) = tokio::join!(client.connect(), setup);
                    assert!(matches!(result, Err(error) if matches!(primary(&error), VoiceLiveError::Protocol(_))));
                    case.peer.closed().await;
                })
            },
        )
        .await;
    }
}

#[tokio::test]
async fn semantic_vad_settings_are_explicit_and_provider_owned_response_is_allowed() {
    run_case(|config| config.session.turn_detection = TurnDetection::semantic_vad(), |case| Box::pin(async move {
        case.ready().await;
        assert_eq!(case.update.as_ref().unwrap()["session"]["turn_detection"], json!({
            "type":"azure_semantic_vad", "silence_duration_ms":500, "create_response":true,
            "interrupt_response":true, "auto_truncate":false, "remove_filler_words":true
        }));
        assert!(matches!(case.session().sender().send(Action::CommitAudio).await, Err(VoiceLiveError::Invalid(_))));
        case.peer.send(json!({"type":"input_audio_buffer.speech_started","item_id":"user-a"})).await;
        assert!(matches!(case.next().await.event, Event::SpeechStarted));
        case.peer.send(json!({"type":"input_audio_buffer.speech_stopped","item_id":"user-a"})).await;
        assert!(matches!(case.next().await.event, Event::SpeechStopped));
        case.peer.send(json!({"type":"response.created","response":{"id":"server-owned"}})).await;
        case.announce_message("server-owned", "assistant").await;
        assert!(matches!(case.next().await.event, Event::ResponseStarted { response_id } if response_id == "server-owned"));
        case.complete("server-owned").await;
        case.peer.quiet().await;
    })).await;
}

#[tokio::test]
async fn manual_audio_commit_and_text_items_preserve_exact_bytes_and_ordinary_create() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        case.send(Action::CommitAudio).await;
        case.peer.quiet().await;
        let pcm = vec![0x00, 0x80, 0xff, 0x7f, 0x12, 0x34];
        case.send(Action::AppendAudio(pcm.clone())).await;
        let append = case.peer.read().await;
        assert_eq!(append["type"], "input_audio_buffer.append");
        assert_eq!(STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(), pcm);
        case.send(Action::CommitAudio).await;
        let commit = case.peer.read().await;
        assert_eq!(commit["type"], "input_audio_buffer.commit");
        case.send(Action::CommitAudio).await;
        case.peer.quiet().await;
        case.send(Action::CreateItem(ConversationItem::user_text("你好, synthetic text"))).await;
        let item = case.peer.read().await;
        assert_eq!(item["item"], json!({"type":"message","role":"user","content":[{"type":"input_text","text":"你好, synthetic text"}]}));
        let id = case.create("plain").await;
        assert_ne!(append["event_id"], commit["event_id"]);
        assert_ne!(commit["event_id"], item["event_id"]);
        assert_ne!(item["event_id"], id);
        case.complete("plain").await;
    })).await;
}

#[tokio::test]
async fn unique_optional_id_output_binds_only_to_the_announced_response_and_item() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        case.started("response", "message").await;
        case.peer.send(json!({"type":"response.audio.delta","delta":STANDARD.encode([1,2,3,4])})).await;
        assert!(matches!(case.next().await.event, Event::Audio { response_id, item_id, content_index:0, audio } if response_id == "response" && item_id == "message" && audio == [1,2,3,4]));
        case.peer.send(json!({"type":"response.audio_transcript.delta","delta":"spoken"})).await;
        assert!(matches!(case.next().await.event, Event::Text { response_id, item_id, delta, audio_transcript:true } if response_id == "response" && item_id == "message" && delta == "spoken"));
        case.peer.send(json!({"type":"response.text.delta","delta":"written"})).await;
        assert!(matches!(case.next().await.event, Event::Text { audio_transcript:false, delta, .. } if delta == "written"));
        case.peer.send(json!({"type":"response.audio.done"})).await;
        case.peer.send(json!({"type":"response.done","response":{"status":"completed"}})).await;
        assert!(matches!(case.next().await.event, Event::ResponseDone { response_id, .. } if response_id == "response"));
    })).await;
}

#[tokio::test]
async fn omitted_ambiguous_unknown_and_mismatched_output_identities_fail_closed() {
    for invalid in [
        json!({"type":"response.audio.delta","response_id":"r","delta":"AQI="}),
        json!({"type":"response.text.delta","response_id":"unknown","item_id":"a","delta":"bad"}),
        json!({"type":"response.audio.delta","response_id":"r","item_id":"unknown","delta":"AQI="}),
    ] {
        run_case(
            |_| {},
            move |case| {
                Box::pin(async move {
                    case.ready().await;
                    case.started("r", "a").await;
                    case.announce_message("r", "b").await;
                    case.peer.send(invalid).await;
                    assert!(matches!(case.failed().await, VoiceLiveError::Protocol(_)));
                })
            },
        )
        .await;
    }
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                case.ready().await;
                case.started("old", "old-item").await;
                case.complete("old").await;
                case.started("new", "new-item").await;
                case.peer.send(audio("new", "old-item", 0, &[1, 2])).await;
                assert!(matches!(case.failed().await, VoiceLiveError::Protocol(_)));
            })
        },
    )
    .await;
}

#[tokio::test]
async fn duplicate_message_announcements_start_once_and_multi_item_audio_stays_distinct() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        case.started("r", "a").await;
        case.announce_message("r", "a").await;
        case.peer.send(json!({"type":"conversation.item.created","item":{"id":"a","type":"message","role":"assistant"},"previous_item_id":""})).await;
        case.announce_message("r", "b").await;
        case.peer.send(audio("r", "a", 0, &[1,2,3,4])).await;
        case.peer.send(audio("r", "b", 1, &[5,6])).await;
        case.peer.send(audio("r", "a", 0, &[7,8])).await;
        for (expected_item, expected_index, expected_audio) in [("a",0,vec![1,2,3,4]), ("b",1,vec![5,6]), ("a",0,vec![7,8])] {
            assert!(matches!(case.next().await.event, Event::Audio { item_id, content_index, audio, .. }
                if item_id == expected_item && content_index == expected_index && audio == expected_audio));
        }
        case.complete("r").await;
    })).await;
}

#[tokio::test]
async fn duplicate_function_announcements_and_completed_arguments_admit_one_tool_event() {
    run_case(offered_tool, |case| Box::pin(async move {
        case.ready().await;
        case.create("r").await;
        let item = function_item("function-item", "call");
        case.peer.send(json!({"type":"response.output_item.added","response_id":"r","item":item})).await;
        case.peer.send(json!({"type":"conversation.item.created","item":item})).await;
        let done = json!({"type":"response.function_call_arguments.done","arguments":"{\"key\":\"x\"}"});
        case.peer.send(done.clone()).await;
        case.peer.send(done).await;
        assert!(matches!(case.next().await.event, Event::ToolCall { response_id, call }
            if response_id == "r" && call.tool_call_id == "call" && call.function_name == "lookup" && call.arguments == json!({"key":"x"})));
        // A response completion is a deterministic barrier behind both duplicates.
        // It must be the next event, not a second callback admission.
        case.complete("r").await;
        case.peer.quiet().await;
        // Settlement belongs to the native pipeline acceptance tests; no public
        // function output is used to fabricate a terminal executor receipt here.
    })).await;
}

#[tokio::test]
async fn interrupted_emitted_but_unmapped_call_is_discarded_without_inventing_settlement() {
    run_case(offered_tool, |case| {
        Box::pin(async move {
            case.ready().await;
            case.create("old").await;
            case.peer
                .send(json!({
                    "type":"response.output_item.added",
                    "response_id":"old",
                    "item":function_item("unmapped-item","unmapped-call")
                }))
                .await;
            case.peer
                .send(json!({
                    "type":"response.function_call_arguments.done",
                    "response_id":"old",
                    "item_id":"unmapped-item",
                    "call_id":"unmapped-call",
                    "arguments":"{\"key\":\"never-executed\"}"
                }))
                .await;
            // TCP/WebSocket order plus the actor's Pong proves args.done was
            // processed and ToolCall emitted. Nothing has read that event, so it
            // cannot have been mapped into a native executor declaration.
            case.peer.processed_barrier().await;
            case.session().sender().interrupt(None);
            assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
            assert_eq!(case.peer.read().await["type"], "response.cancel");
            case.peer
                .send(json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":"input-after-interruption",
                    "transcript":"fresh turn"
                }))
                .await;
            // This first post-readiness read must discard the stale ToolCall,
            // not deliver it or demand a terminal receipt for nonexistent work.
            let event = case.next().await;
            assert_eq!(event.generation, 1);
            assert!(matches!(
                event.event,
                Event::Transcript { item_id, text, is_final:true }
                    if item_id == "input-after-interruption" && text == "fresh turn"
            ));
            case.peer.quiet().await;
            // Only now send completion: the stale event has provably been
            // discarded, but its retained identity must still validate output.
            case.peer
                .send(json!({
                    "type":"response.done",
                    "response":{
                        "id":"old","status":"cancelled",
                        "output":[{
                            "id":"unmapped-item","type":"function_call",
                            "call_id":"unmapped-call","name":"lookup",
                            "arguments":"{\"key\":\"never-executed\"}"
                        }]
                    }
                }))
                .await;
            case.create("fresh").await;
            case.complete("fresh").await;
            // create() checks the next request is exactly response.create. Any
            // invented function_call_output would fail that assertion above.
            case.peer.quiet().await;
        })
    })
    .await;
}

#[tokio::test]
async fn ambiguous_and_contradictory_function_completions_fail_before_callback_admission() {
    for invalid in [
        json!({"type":"response.function_call_arguments.done","arguments":"{}"}),
        json!({"type":"response.function_call_arguments.done","item_id":"one","call_id":"second","arguments":"{}"}),
        json!({"type":"response.function_call_arguments.done","item_id":"one","name":"other","arguments":"{}"}),
        json!({"type":"response.function_call_arguments.done","item_id":"one","arguments":"[]"}),
        json!({"type":"response.function_call_arguments.done","call_id":"unannounced","arguments":"{}"}),
    ] {
        run_case(offered_tool, move |case| Box::pin(async move {
            case.ready().await;
            case.create("r").await;
            for (item, call) in [("one","first"), ("two","second")] {
                case.peer.send(json!({"type":"response.output_item.added","response_id":"r","item":function_item(item,call)})).await;
            }
            case.peer.send(invalid).await;
            assert!(matches!(case.failed().await, VoiceLiveError::Protocol(_)));
            assert!(matches!(case.session().next_event().await, Some(Err(error)) if matches!(primary(&error), VoiceLiveError::Protocol(_))));
        })).await;
    }
}

#[tokio::test]
async fn transcript_finals_are_deduplicated_and_changed_final_is_not_silently_rewritten() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        for delta in ["hel", "lo"] {
            case.peer.send(json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"input","delta":delta})).await;
        }
        let final_event = json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"input","transcript":" hello "});
        case.peer.send(final_event.clone()).await;
        case.peer.send(final_event).await;
        case.peer.send(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"next-input","transcript":"next"})).await;
        for (item,text,is_final) in [("input","hel",false),("input","hello",false),("input","hello",true),("next-input","next",true)] {
            assert!(matches!(case.next().await.event, Event::Transcript { item_id, text: actual, is_final: terminal }
                if item_id == item && actual == text && terminal == is_final));
        }
        case.peer.send(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"input","transcript":"changed"})).await;
        assert!(matches!(case.failed().await, VoiceLiveError::Protocol(_)));
    })).await;
}

#[tokio::test]
async fn only_matching_response_rejection_releases_pending_request_and_owed_continuation() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        case.send(Action::CreateResponse).await;
        let first = case.peer.read().await;
        case.send(Action::CreateResponse).await;
        for event_id in [Some("unrelated"), None] {
            let mut error = json!({"type":"error","error":{"code":"synthetic-rejection","message":SECRET}});
            if let Some(event_id) = event_id { error["error"]["event_id"] = json!(event_id); }
            case.peer.send(error).await;
            let event = case.next().await.event;
            assert!(matches!(&event, Event::ProviderError { code:Some(code), fatal:false, .. } if code == "provider_request_error"));
            assert!(!format!("{event:?}").contains(SECRET));
            case.peer.quiet().await;
        }
        case.peer.send(json!({"type":"error","error":{"code":"synthetic-rejection","event_id":first["event_id"],"message":SECRET}})).await;
        assert!(matches!(case.next().await.event, Event::ProviderError { code:Some(code), fatal:false, .. } if code == "response_request_rejected"));
        let second = case.peer.read().await;
        assert_eq!(second["type"], "response.create");
        assert_ne!(first["event_id"], second["event_id"]);
        case.peer.send(json!({"type":"response.created","response":{"id":"continued"}})).await;
        case.complete("continued").await;
        case.peer.quiet().await;
    })).await;
}

#[tokio::test]
async fn active_collision_does_not_release_pending_request_or_duplicate_continuation() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        case.send(Action::CreateResponse).await;
        let first = case.peer.read().await;
        case.send(Action::CreateResponse).await;
        case.peer.send(json!({"type":"error","error":{"code":"conversation_already_has_active_response","event_id":first["event_id"],"message":SECRET}})).await;
        case.peer.quiet().await;
        case.peer.send(json!({"type":"response.created","response":{"id":"already-active"}})).await;
        case.complete("already-active").await;
        let next = case.peer.read().await;
        assert_eq!(next["type"], "response.create");
        assert_ne!(next["event_id"], first["event_id"]);
        case.peer.send(json!({"type":"response.created","response":{"id":"owed"}})).await;
        case.complete("owed").await;
        case.peer.quiet().await;
    })).await;
}

#[tokio::test]
async fn failed_response_reports_sanitized_error_and_runs_one_owed_continuation() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        case.started("failed", "message").await;
        case.send(Action::CreateResponse).await;
        case.send(Action::CreateResponse).await;
        case.peer.quiet().await;
        case.peer.send(json!({"type":"response.done","response":{"id":"failed","status":"failed","status_details":{"error":{"message":SECRET}}}})).await;
        assert!(matches!(case.next().await.event, Event::ResponseDone { status:ResponseStatus::Failed, .. }));
        let failure = case.next().await.event;
        assert!(matches!(&failure, Event::ProviderError { code:Some(code), fatal:false, .. } if code == "response_failed"));
        assert!(!format!("{failure:?}").contains(SECRET));
        assert_eq!(case.peer.read().await["type"], "response.create");
        case.peer.send(json!({"type":"response.created","response":{"id":"owed"}})).await;
        case.complete("owed").await;
        case.peer.quiet().await;
    })).await;
}

#[tokio::test]
async fn manual_interrupt_without_response_clears_and_restores_only_bounded_preroll() {
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                case.ready().await;
                case.send(Action::SetPrerollBytes(4)).await;
                case.send(Action::AppendAudio(vec![1, 2, 3, 4, 5, 6])).await;
                assert_eq!(case.peer.read().await["type"], "input_audio_buffer.append");
                let sender = case.session().sender();
                sender.interrupt(None);
                assert_eq!(sender.generation(), 1);
                assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
                let replay = case.peer.read().await;
                assert_eq!(replay["type"], "input_audio_buffer.append");
                assert_eq!(
                    STANDARD.decode(replay["audio"].as_str().unwrap()).unwrap(),
                    [3, 4, 5, 6]
                );
                case.peer.quiet().await; // No response.cancel or invented truncate.
                case.send(Action::CommitAudio).await;
                assert_eq!(case.peer.read().await["type"], "input_audio_buffer.commit");
                case.send(Action::ClearAudio).await;
                assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
                sender.interrupt(None);
                assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
                case.peer.quiet().await; // Explicit clear also cleared retained preroll.
            })
        },
    )
    .await;
}

#[tokio::test]
async fn queued_pcm_precedes_manual_interrupt_clear_and_retains_its_suffix_without_a_flush() {
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                case.ready().await;
                let sender = case.session().sender();
                // There is deliberately no await or caller-side flush between
                // admission and interruption. The actor must own this ordering.
                sender.try_send(Action::SetPrerollBytes(4)).unwrap();
                sender
                    .try_send(Action::AppendAudio(vec![1, 2, 3, 4, 5, 6]))
                    .unwrap();
                sender.try_send(Action::CreateResponse).unwrap();
                sender.interrupt(None);
                let original = case.peer.read().await;
                assert_eq!(original["type"], "input_audio_buffer.append");
                assert_eq!(
                    STANDARD
                        .decode(original["audio"].as_str().unwrap())
                        .unwrap(),
                    [1, 2, 3, 4, 5, 6]
                );
                assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
                let replay = case.peer.read().await;
                assert_eq!(replay["type"], "input_audio_buffer.append");
                assert_eq!(
                    STANDARD.decode(replay["audio"].as_str().unwrap()).unwrap(),
                    [3, 4, 5, 6]
                );
                // Queued stale response creation is fenced; admitted caller PCM
                // is not mistaken for stale provider output.
                case.peer.quiet().await;
                case.create("after-interruption").await;
                case.complete("after-interruption").await;
            })
        },
    )
    .await;
}

#[tokio::test]
async fn interrupt_before_response_announcement_cancels_and_fences_late_output() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        case.send(Action::CreateResponse).await;
        assert_eq!(case.peer.read().await["type"], "response.create");
        case.session().sender().interrupt(None);
        assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
        assert_eq!(case.peer.read().await["type"], "response.cancel");
        case.send(Action::CreateResponse).await;
        case.peer.quiet().await;
        case.peer.send(json!({"type":"response.created","response":{"id":"stale"}})).await;
        case.announce_message("stale", "stale-item").await;
        case.peer.send(audio("stale","stale-item",0,&[1,2])).await;
        case.peer.send(json!({"type":"response.done","response":{"id":"stale","status":"cancelled"}})).await;
        assert_eq!(case.peer.read().await["type"], "response.create");
        case.peer.send(json!({"type":"response.created","response":{"id":"current"}})).await;
        case.announce_message("current","current-item").await;
        let event = case.next().await;
        assert_eq!(event.generation, 1);
        assert!(matches!(event.event, Event::ResponseStarted { response_id } if response_id == "current"));
        case.complete("current").await;
    })).await;
}

#[tokio::test]
async fn semantic_interrupt_never_clears_or_replays_input_and_cancels_only_active_response() {
    run_case(|config| config.session.turn_detection = TurnDetection::semantic_vad(), |case| Box::pin(async move {
        case.ready().await;
        case.send(Action::AppendAudio(vec![1,2,3,4])).await;
        assert_eq!(case.peer.read().await["type"], "input_audio_buffer.append");
        case.session().sender().interrupt(None);
        case.peer.quiet().await;
        case.peer.send(json!({"type":"response.created","response":{"id":"server"}})).await;
        case.announce_message("server","message").await;
        assert!(matches!(case.next().await.event, Event::ResponseStarted { .. }));
        case.session().sender().interrupt(None);
        assert_eq!(case.peer.read().await["type"], "response.cancel");
        case.peer.quiet().await;
        case.peer.send(audio("server","message",0,&[1,2])).await;
        case.peer.send(json!({"type":"response.done","response":{"id":"server","status":"cancelled"}})).await;
        case.peer.send(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"live-input","transcript":"still live"})).await;
        assert!(matches!(case.next().await.event, Event::Transcript { item_id, .. } if item_id == "live-input"));
    })).await;
}

#[tokio::test]
async fn playback_acknowledgments_are_known_monotonic_bounded_and_authorize_exact_truncation() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        case.started("r","audio-item").await;
        case.peer.send(audio("r","audio-item",2,&[0;480])).await;
        assert!(matches!(case.next().await.event, Event::Audio { audio, .. } if audio.len() == 480));
        let sender = case.session().sender();
        assert!(sender.acknowledge_playback(position("unknown",2,1)).is_err());
        assert!(sender.acknowledge_playback(position("audio-item",0,1)).is_err());
        assert!(sender.acknowledge_playback(position("audio-item",2,11)).is_err());
        sender.acknowledge_playback(position("audio-item",2,3)).unwrap();
        sender.acknowledge_playback(position("audio-item",2,7)).unwrap();
        assert!(sender.acknowledge_playback(position("audio-item",2,6)).is_err());
        sender.interrupt(Some(position("audio-item",2,7)));
        assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
        assert_eq!(case.peer.read().await["type"], "response.cancel");
        let truncate = case.peer.read().await;
        assert_eq!(truncate["type"], "conversation.item.truncate");
        assert_eq!(truncate["item_id"], "audio-item");
        assert_eq!(truncate["content_index"], 2);
        assert_eq!(truncate["audio_end_ms"], 7);
        assert!(sender.acknowledge_playback(position("audio-item",2,8)).is_err());
        case.peer.quiet().await;
    })).await;
}

#[tokio::test]
async fn generated_audio_without_playback_receipt_never_invents_a_truncation() {
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                case.ready().await;
                case.started("r", "item").await;
                case.peer.send(audio("r", "item", 0, &[0; 480])).await;
                assert!(matches!(case.next().await.event, Event::Audio { .. }));
                case.session().sender().interrupt(None);
                assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
                assert_eq!(case.peer.read().await["type"], "response.cancel");
                case.peer.quiet().await;
            })
        },
    )
    .await;
}

#[tokio::test]
async fn supplied_playback_without_an_actual_acknowledgment_fails_instead_of_truncating() {
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                case.ready().await;
                case.started("r", "item").await;
                case.peer.send(audio("r", "item", 0, &[0; 480])).await;
                assert!(matches!(case.next().await.event, Event::Audio { .. }));
                case.session()
                    .sender()
                    .interrupt(Some(position("item", 0, 5)));
                assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
                assert_eq!(case.peer.read().await["type"], "response.cancel");
                assert!(matches!(case.failed().await, VoiceLiveError::Invalid(_)));
                case.peer.closed().await; // No truncate may precede the terminal close.
            })
        },
    )
    .await;
}

#[tokio::test]
async fn omitted_response_completion_after_interruption_cannot_complete_a_new_response() {
    run_case(|_| {}, |case| Box::pin(async move {
        case.ready().await;
        case.started("old","old-item").await;
        case.session().sender().interrupt(None);
        assert_eq!(case.peer.read().await["type"], "input_audio_buffer.clear");
        assert_eq!(case.peer.read().await["type"], "response.cancel");
        case.peer.send(json!({"type":"response.done","response":{"id":"old","status":"cancelled"}})).await;
        case.started("new","new-item").await;
        case.peer.send(json!({"type":"response.done","response":{"status":"completed"}})).await;
        assert!(matches!(case.failed().await, VoiceLiveError::Protocol(_)),
            "an omitted stale completion must not acquire the current response's identity");
        assert!(matches!(case.session().next_event().await, Some(Err(error)) if matches!(primary(&error), VoiceLiveError::Protocol(_))),
            "no false completion event may escape before the correlation failure");
    })).await;
}

#[tokio::test]
async fn malformed_json_base64_pcm_content_and_message_bounds_fail_closed() {
    for bad in [
        Message::Text("{malformed synthetic input".into()),
        Message::Binary(vec![1,2].into()),
        Message::Text(json!({"type":"response.audio.delta","response_id":"r","item_id":"item","delta":"%%%%"}).to_string().into()),
        Message::Text(audio("r","item",0,&[1,2,3]).to_string().into()),
        Message::Text(audio("r","item",64,&[1,2]).to_string().into()),
        Message::Text(audio("r","item",0,&[0;98]).to_string().into()),
        Message::Text(json!({"type":"synthetic.unknown","payload":"x".repeat(3_000)}).to_string().into()),
    ] {
        run_case(|config| {
            config.limits.max_audio_chunk_bytes = 96;
            config.limits.max_message_bytes = 2_048;
        }, move |case| Box::pin(async move {
            case.ready().await;
            case.started("r","item").await;
            case.peer.send_message(bad).await;
            let error = case.failed().await;
            assert!(matches!(error, VoiceLiveError::Protocol(_) | VoiceLiveError::TooLarge));
            assert!(!format!("{error:?}").contains("malformed synthetic input"));
            assert!(matches!(case.session().next_event().await, Some(Err(_))));
        })).await;
    }
}

#[tokio::test]
async fn unknown_event_budget_does_not_treat_ga_event_names_as_voice_live_progress() {
    run_case(|config| config.limits.max_unknown_events = 2, |case| Box::pin(async move {
        case.ready().await;
        case.started("r","item").await;
        for event_type in ["response.output_audio.delta","conversation.item.added","unknown.exhausted"] {
            case.peer.send(json!({"type":event_type,"response_id":"r","item_id":"item","delta":"AQI="})).await;
        }
        assert!(matches!(case.failed().await, VoiceLiveError::Protocol(_)));
        assert!(matches!(case.session().next_event().await, Some(Err(error)) if matches!(primary(&error), VoiceLiveError::Protocol(_))));
    })).await;
}

#[tokio::test]
async fn transport_eof_is_terminal_and_does_not_reconnect() {
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                case.ready().await;
                let sender = case.session().sender();
                case.peer.disconnect().await;
                let error = case.failed().await;
                assert!(matches!(
                    error,
                    VoiceLiveError::Closed | VoiceLiveError::Transport
                ));
                assert!(matches!(
                    sender.send(Action::CreateResponse).await,
                    Err(VoiceLiveError::Closed)
                ));
                assert!(matches!(case.session().next_event().await, Some(Err(_))));
                assert!(case.session().next_event().await.is_none());
            })
        },
    )
    .await;
}

#[tokio::test]
async fn owner_drop_with_retained_sender_still_joins_the_socket_actor() {
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                case.ready().await;
                let sender = case.session().sender();
                let retained = sender.clone();
                drop(case.session.take().unwrap());
                timeout(WAIT, sender.wait_finished())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    retained.try_send(Action::CreateResponse),
                    Err(VoiceLiveError::Closed)
                );
                case.peer.closed().await;
            })
        },
    )
    .await;
}

#[tokio::test]
async fn cancelled_shutdown_waiter_preserves_owned_actor_join_and_rejects_retained_sender() {
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                case.ready().await;
                let sender = case.session().sender();
                {
                    let shutdown = case.session().shutdown();
                    tokio::pin!(shutdown);
                    // Exactly one poll starts close, then drop the waiter regardless of
                    // whether fast loopback cleanup already completed on that poll.
                    let _ = futures_util::poll!(&mut shutdown);
                }
                timeout(WAIT, sender.wait_finished())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(case.session().session_joined());
                assert_eq!(
                    sender.try_send(Action::CreateResponse),
                    Err(VoiceLiveError::Closed)
                );
                case.session().shutdown().await.unwrap();
                case.peer.closed().await;
            })
        },
    )
    .await;
}

#[tokio::test]
async fn clean_finish_and_repeated_shutdown_publish_only_after_actual_join() {
    run_case(
        |_| {},
        |case| {
            Box::pin(async move {
                case.ready().await;
                let sender = case.session().sender();
                sender.finish().await.unwrap();
                timeout(WAIT, sender.wait_finished())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(case.session().session_joined());
                assert!(case.session().next_event().await.is_none());
                case.session().shutdown().await.unwrap();
                case.session().shutdown().await.unwrap();
                case.peer.closed().await;
            })
        },
    )
    .await;
}
