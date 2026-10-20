//! Synthetic, loopback-only acceptance of Azure's explicit GA connection profile.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, Metric, Pipeline, PipelineConfig,
    ProcessorContext, ProcessorError, StartFrame,
};
use pipecat_realtime::{
    Action, AzureRealtimeCredential, Event, OpenAiRealtimeProcessor, RealtimeClient,
    RealtimeConfig, RealtimeError, RealtimeSession, SessionProperties,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{
    tungstenite::{handshake::server::Request, Message},
    WebSocketStream,
};

const WAIT: Duration = Duration::from_secs(5);
const SECRET: &str = "synthetic-azure-secret-never-live";
const DEPLOYMENT: &str = "explicit-deployment";
type Ws = WebSocketStream<TcpStream>;

fn credential(bearer: bool) -> AzureRealtimeCredential {
    if bearer {
        AzureRealtimeCredential::EntraBearer(SECRET.into())
    } else {
        AzureRealtimeCredential::ApiKey(SECRET.into())
    }
}
fn endpoint(listener: &TcpListener) -> String {
    format!("ws://{}/openai/v1/realtime", listener.local_addr().unwrap())
}
fn configured(endpoint: impl Into<String>, bearer: bool) -> RealtimeConfig {
    let mut config = RealtimeConfig::azure(endpoint, DEPLOYMENT, credential(bearer)).unwrap();
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.operation_timeout = Duration::from_secs(2);
    config.limits.close_timeout = Duration::from_millis(200);
    config
}
async fn read(ws: &mut Ws) -> Value {
    let message = timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn send(ws: &mut Ws, mut value: Value) {
    value["event_id"] = json!("synthetic-event");
    timeout(WAIT, ws.send(Message::Text(value.to_string().into())))
        .await
        .unwrap()
        .unwrap();
}
async fn ready(ws: &mut Ws) {
    send(
        ws,
        json!({"type":"session.updated","session":{"id":"azure-session","type":"realtime","model":"server-underlying-model-alias"}}),
    )
    .await;
}
async fn next(session: &mut RealtimeSession) -> Event {
    timeout(WAIT, session.next_event())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .event
}
async fn close_peer(ws: &mut Ws) {
    let message = timeout(WAIT, ws.next()).await.unwrap();
    assert!(matches!(message, Some(Ok(Message::Close(_))) | None));
    let _ = ws.flush().await;
}
fn assert_request(req: &Request, bearer: bool, deployment: &str) {
    assert_eq!(req.uri().path(), "/openai/v1/realtime");
    let parsed = url::Url::parse(&format!("ws://loopback{}", req.uri())).unwrap();
    assert_eq!(
        parsed.query_pairs().collect::<Vec<_>>(),
        vec![("model".into(), deployment.into())]
    );
    if bearer {
        assert_eq!(req.headers()["authorization"], format!("Bearer {SECRET}"));
        assert!(!req.headers().contains_key("api-key"));
        assert_eq!(req.headers().get_all("authorization").iter().count(), 1);
    } else {
        assert_eq!(req.headers()["api-key"], SECRET);
        assert!(!req.headers().contains_key("authorization"));
        assert_eq!(req.headers().get_all("api-key").iter().count(), 1);
    }
    assert!(!req.headers().contains_key("openai-beta"));
    assert!(!req.headers().contains_key("api-version"));
}

#[tokio::test]
#[allow(clippy::result_large_err)] // tungstenite specifies the callback error type.
async fn both_credentials_and_query_forms_use_exactly_one_selected_header_and_model() {
    // Reserved query characters are deployment data, never extra parameters.
    let deployment = "deployment &+=?/#% name";
    for bearer in [false, true] {
        for preselected in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut url = url::Url::parse(&endpoint(&listener)).unwrap();
            if preselected {
                url.query_pairs_mut().append_pair("model", deployment);
            }
            let peer = tokio::spawn(async move {
                let (tcp, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
                let mut ws = tokio_tungstenite::accept_hdr_async(tcp, |req: &Request, response| {
                    assert_request(req, bearer, deployment);
                    Ok(response)
                })
                .await
                .unwrap();
                let update = read(&mut ws).await;
                assert_eq!(update["type"], "session.update");
                assert_eq!(update["session"]["type"], "realtime");
                assert_eq!(update["session"]["model"], deployment);
                assert_eq!(
                    update["session"]["audio"]["input"]["format"]["rate"],
                    24_000
                );
                assert!(update["session"]["audio"]["input"]["turn_detection"].is_null());
                assert!(update["session"].get("modalities").is_none());
                ready(&mut ws).await;
                // A canonical server model alias must not rewrite the deployment.
                let update = read(&mut ws).await;
                assert_eq!(update["session"]["model"], deployment);
                close_peer(&mut ws).await;
            });
            let config =
                RealtimeConfig::azure(url.as_str(), deployment, credential(bearer)).unwrap();
            let mut session = RealtimeClient::new(config)
                .unwrap()
                .connect()
                .await
                .unwrap();
            assert!(matches!(
                next(&mut session).await,
                Event::SessionUpdated { .. }
            ));
            let mut update = SessionProperties::new();
            update.model = Some("server-underlying-model-alias".into());
            assert!(matches!(
                session
                    .sender()
                    .send(Action::UpdateSession(update.clone()))
                    .await,
                Err(RealtimeError::Invalid(
                    "model cannot change during a session"
                ))
            ));
            update.model = Some(deployment.into());
            session
                .sender()
                .send(Action::UpdateSession(update))
                .await
                .unwrap();
            timeout(WAIT, session.shutdown()).await.unwrap().unwrap();
            timeout(WAIT, peer).await.unwrap().unwrap();
        }
    }
}

#[tokio::test]
async fn malformed_or_mutated_azure_configuration_never_reaches_a_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let valid = endpoint(&listener);
    let authority = format!("ws://{}", listener.local_addr().unwrap());
    let rejected = [
        format!("{valid}?"),
        format!("{valid}?model="),
        format!("{valid}?model=wrong"),
        format!("{valid}?model={DEPLOYMENT}&model={DEPLOYMENT}"),
        format!("{valid}?model={DEPLOYMENT}&"),
        format!("{valid}?&model={DEPLOYMENT}"),
        format!("{valid}?api-version=2025-01-01-preview"),
        format!("{valid}?deployment={DEPLOYMENT}"),
        format!("{valid}?api-key=never-send"),
        format!("{valid}?model={DEPLOYMENT}&access_token=never-send"),
        format!("{valid}?model={DEPLOYMENT}&unknown=value"),
        format!("{authority}/openai/realtime"),
        format!("{authority}/voice-live/realtime"),
        format!("{authority}/openai/./v1/realtime"),
        format!("{authority}/openai/other/../v1/realtime"),
        format!("{valid}/"),
        format!("{valid}#fragment"),
        format!(
            "ws://user:secret@{}/openai/v1/realtime",
            listener.local_addr().unwrap()
        ),
        format!(
            "ws://user@{}/openai/v1/realtime",
            listener.local_addr().unwrap()
        ),
        format!(
            "ws://@{}/openai/v1/realtime",
            listener.local_addr().unwrap()
        ),
        format!(" {valid}"),
        format!("{valid}\n"),
        "https://resource.example/openai/v1/realtime".into(),
        "ws://resource.example/openai/v1/realtime".into(),
        "ws://localhost/openai/v1/realtime".into(),
        "ws://127.1/openai/v1/realtime".into(),
        "ws://2130706433/openai/v1/realtime".into(),
        "ws://0x7f000001/openai/v1/realtime".into(),
        "wss://resource.example/openai/v1/realtime?model=other".into(),
        "not-a-url-with-private-content".into(),
        format!("{valid}?{}", "x".repeat(8192)),
    ];
    for invalid in rejected {
        for bearer in [false, true] {
            let error =
                RealtimeConfig::azure(&invalid, DEPLOYMENT, credential(bearer)).unwrap_err();
            assert!(!error.to_string().contains(&invalid));
            assert!(!format!("{error:?}").contains(SECRET));
            let mut config = configured(&valid, bearer);
            config.endpoint = invalid.clone();
            assert!(RealtimeClient::new(config).is_err());
        }
    }
    for bearer in [false, true] {
        let mut config = configured(&valid, bearer);
        config.model = "changed-deployment".into();
        config.session.model = Some("changed-deployment".into());
        assert!(RealtimeClient::new(config).is_err());
        let mut config = configured(&valid, bearer);
        config.session.model = Some("different-session-model".into());
        assert!(RealtimeClient::new(config).is_err());
        let mut config = configured(&valid, bearer);
        config.session.model = None;
        assert!(RealtimeClient::new(config).is_ok());
    }
    assert!(timeout(Duration::from_millis(40), listener.accept())
        .await
        .is_err());
}

#[test]
fn azure_inputs_are_bounded_and_all_debug_surfaces_redact_credentials() {
    let endpoint = "wss://synthetic-resource.example/openai/v1/realtime";
    for bearer in [false, true] {
        let auth = credential(bearer);
        assert!(!format!("{auth:?}").contains(SECRET));
        let config = RealtimeConfig::azure(endpoint, SECRET, auth).unwrap();
        assert!(!format!("{config:?}").contains(SECRET));
        assert!(!format!("{config:?}").contains(endpoint));
        let client = RealtimeClient::new(config).unwrap();
        assert!(!format!("{client:?}").contains(SECRET));
        for secret in [
            "".into(),
            "   ".into(),
            "a\tb".into(),
            "a\rb".into(),
            "a\nb".into(),
            "a\0b".into(),
            "a\u{7f}b".into(),
            "x".repeat(16_385),
        ] {
            let auth = if bearer {
                AzureRealtimeCredential::EntraBearer(secret)
            } else {
                AzureRealtimeCredential::ApiKey(secret)
            };
            assert!(RealtimeConfig::azure(endpoint, DEPLOYMENT, auth).is_err());
        }
        for deployment in ["".into(), "   ".into(), "x\ny".into(), "x".repeat(257)] {
            assert!(RealtimeConfig::azure(endpoint, deployment, credential(bearer)).is_err());
        }
        assert!(RealtimeConfig::azure(endpoint, "x".repeat(256), credential(bearer)).is_ok());
        let auth = if bearer {
            AzureRealtimeCredential::EntraBearer("x".repeat(16_384))
        } else {
            AzureRealtimeCredential::ApiKey("x".repeat(16_384))
        };
        assert!(RealtimeConfig::azure(endpoint, DEPLOYMENT, auth).is_ok());
    }
    assert!(RealtimeConfig::azure(
        "ws://[::1]/openai/v1/realtime",
        DEPLOYMENT,
        credential(false)
    )
    .is_ok());
}

#[test]
fn openai_default_and_strict_query_contract_are_unchanged() {
    let mut config = RealtimeConfig::new("openai-synthetic-key");
    assert!(!config.is_azure());
    assert_eq!(config.service_name(), "OpenAiRealtimeService");
    assert_eq!(config.endpoint, "wss://api.openai.com/v1/realtime");
    assert_eq!(config.model, "gpt-realtime-2.1");
    assert!(config.session.model.is_none());
    config.validate().unwrap();
    config.endpoint.push_str("?model=gpt-realtime-2.1");
    assert!(config.validate().is_err());
    let azure = configured("wss://synthetic-resource.example/openai/v1/realtime", false);
    assert!(azure.is_azure());
    assert_eq!(azure.service_name(), "AzureOpenAiRealtimeService");
}

#[tokio::test]
async fn either_credential_is_redacted_in_every_typed_provider_error() {
    for bearer in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = configured(endpoint(&listener), bearer);
        let peer = tokio::spawn(async move {
            let (tcp, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            assert_eq!(read(&mut ws).await["type"], "session.update");
            ready(&mut ws).await;
            let error = json!({"type":SECRET,"message":format!("echo {SECRET}"),"code":SECRET,"param":SECRET,"event_id":SECRET});
            send(&mut ws, json!({"type":"error","error":error})).await;
            send(&mut ws, json!({"type":"conversation.item.input_audio_transcription.failed","item_id":"input","content_index":0,"error":error})).await;
            send(&mut ws, json!({"type":"response.created","response":{"id":"response","status":"in_progress","output":[],"status_details":{"type":"failed","error":error}}})).await;
            send(&mut ws, json!({"type":"response.done","response":{"id":"response","status":"failed","output":[],"status_details":{"type":"failed","error":error}}})).await;
            close_peer(&mut ws).await;
        });
        let mut session = RealtimeClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        assert!(matches!(
            next(&mut session).await,
            Event::SessionUpdated { .. }
        ));
        for index in 0..4 {
            let event = next(&mut session).await;
            let error = match (&event, index) {
                (Event::Error { error }, 0)
                | (Event::InputTranscriptionFailed { error, .. }, 1) => error,
                (Event::ResponseCreated { response }, 2)
                | (Event::ResponseDone { response }, 3) => response
                    .status_details
                    .as_ref()
                    .unwrap()
                    .error
                    .as_ref()
                    .unwrap(),
                _ => panic!("unexpected event at {index}: {event:?}"),
            };
            assert_eq!(error.kind, "[REDACTED]");
            assert_eq!(error.message, "echo [REDACTED]");
            assert_eq!(error.code.as_deref(), Some("[REDACTED]"));
            assert_eq!(error.param.as_deref(), Some("[REDACTED]"));
            assert_eq!(error.event_id.as_deref(), Some("[REDACTED]"));
            assert!(!format!("{event:?}").contains(SECRET));
        }
        timeout(WAIT, session.shutdown()).await.unwrap().unwrap();
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn azure_http_errors_expose_only_status_and_never_redirect_retry_or_fallback() {
    for bearer in [false, true] {
        for status in [302, 401] {
            let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let config = configured(endpoint(&listener), bearer);
            let destination_url = endpoint(&destination);
            let task = tokio::spawn(async move {
                let (mut tcp, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
                let mut buffer = [0; 8192];
                let mut used = 0;
                while !buffer[..used].windows(4).any(|w| w == b"\r\n\r\n") {
                    let count = timeout(WAIT, tcp.read(&mut buffer[used..]))
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(count > 0);
                    used += count;
                    assert!(used < buffer.len());
                }
                let request = std::str::from_utf8(&buffer[..used]).unwrap().to_lowercase();
                assert_eq!(request.contains("\r\nauthorization:"), bearer);
                assert_eq!(request.contains("\r\napi-key:"), !bearer);
                let response = format!("HTTP/1.1 {status} Rejected\r\nLocation: {destination_url}\r\nX-Private: {SECRET}\r\nContent-Length: {}\r\n\r\n{SECRET}", SECRET.len());
                tcp.write_all(response.as_bytes()).await.unwrap();
                drop(tcp);
                assert!(timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err());
            });
            let error = match RealtimeClient::new(config).unwrap().connect().await {
                Err(error) => error,
                Ok(_) => panic!("unexpected handshake acceptance"),
            };
            assert_eq!(error, RealtimeError::Http(status));
            assert_eq!(
                error.to_string(),
                format!("realtime handshake returned HTTP {status}")
            );
            assert!(!format!("{error:?}").contains(SECRET));
            assert!(timeout(Duration::from_millis(50), destination.accept())
                .await
                .is_err());
            timeout(WAIT, task).await.unwrap().unwrap();
        }
    }
}

#[tokio::test]
async fn azure_processor_metadata_usage_and_actor_join_identify_the_explicit_profile() {
    for bearer in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = configured(endpoint(&listener), bearer);
        let peer = tokio::spawn(async move {
            let (tcp, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            assert_eq!(read(&mut ws).await["type"], "session.update");
            ready(&mut ws).await;
            assert_eq!(read(&mut ws).await["type"], "response.create");
            send(&mut ws, json!({"type":"response.created","response":{"id":"response","status":"in_progress","output":[]}})).await;
            send(&mut ws, json!({"type":"response.done","response":{"id":"response","status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}})).await;
            close_peer(&mut ws).await;
        });
        let processor =
            Arc::new(OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
        assert_eq!(processor.name(), "AzureOpenAiRealtimeService");
        assert!(!processor.session_joined());
        let mut config = PipelineConfig::default();
        config.start.audio_in_sample_rate = 24_000;
        config.start.audio_out_sample_rate = 24_000;
        config.start.enable_usage_metrics = true;
        let mut worker = Pipeline::new(vec![processor.clone()])
            .start(config)
            .await
            .unwrap();
        processor
            .sender()
            .unwrap()
            .send(Action::CreateResponse(Default::default()))
            .await
            .unwrap();
        let mut metadata = false;
        let mut usage = false;
        timeout(WAIT, async {
            loop {
                let frame = worker.recv().await.unwrap().frame;
                match frame.data {
                    FrameData::LlmServiceMetadata(value) => {
                        assert_eq!(value.base.service_name, "AzureOpenAiRealtimeService");
                        metadata = true;
                    }
                    FrameData::Metrics(metrics) => {
                        for metric in metrics {
                            if let Metric::LlmUsage {
                                processor,
                                total_tokens,
                                ..
                            } = metric
                            {
                                assert_eq!(processor, "AzureOpenAiRealtimeService");
                                assert_eq!(total_tokens, 5);
                                usage = true;
                            }
                        }
                    }
                    FrameData::LlmResponseEnd => break,
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        assert!(metadata && usage);
        worker.end().await.unwrap();
        let report = timeout(WAIT, worker.join()).await.unwrap().unwrap();
        assert_eq!(report.total_errors, 0, "{:?}", report.errors);
        assert!(processor.session_joined());
        assert!(processor.selected_session_joined());
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn ordinary_azure_cancel_before_readiness_joins_actor_with_retained_sender() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configured(endpoint(&listener), false);
    let (observed, ready) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (tcp, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        assert_eq!(read(&mut ws).await["type"], "session.update");
        observed.send(()).unwrap();
        // No session.updated: cancellation must not wait for provider readiness.
        close_peer(&mut ws).await;
    });
    let processor =
        Arc::new(OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 24_000;
    config.start.audio_out_sample_rate = 24_000;
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(config)
        .await
        .unwrap();
    timeout(WAIT, ready).await.unwrap().unwrap();
    let retained = processor.sender().unwrap();
    worker
        .cancel("synthetic pre-readiness cancellation")
        .await
        .unwrap();
    let report = timeout(WAIT, worker.join()).await.unwrap().unwrap();
    assert_eq!(report.state, pipecat_core::WorkerState::Cancelled);
    assert_eq!(report.total_errors, 0, "{:?}", report.errors);
    assert!(processor.session_joined());
    assert_eq!(
        retained.try_send(Action::AppendAudio(vec![0, 0])),
        Err(RealtimeError::Closed)
    );
    timeout(WAIT, peer).await.unwrap().unwrap();
}

struct RejectSetup;
#[async_trait]
impl FrameProcessor for RejectSetup {
    fn name(&self) -> &str {
        "SyntheticRejectSetup"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        Err(ProcessorError::fatal(
            "synthetic downstream setup rejection",
        ))
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn ordinary_azure_downstream_startup_failure_still_joins_published_actor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configured(endpoint(&listener), true);
    let peer = tokio::spawn(async move {
        let (tcp, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        assert_eq!(read(&mut ws).await["type"], "session.update");
        close_peer(&mut ws).await;
    });
    let processor =
        Arc::new(OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 24_000;
    config.start.audio_out_sample_rate = 24_000;
    let result = timeout(
        WAIT,
        Pipeline::new(vec![processor.clone(), Arc::new(RejectSetup)]).start(config),
    )
    .await
    .unwrap();
    // Core reports collected setup errors after reaching Running and may
    // return the failed worker. Neither that API shape nor a direct start
    // failure is evidence of cleanup until its actual owner is joined.
    let failure =
        match result {
            Ok(mut worker) => {
                let failure = timeout(WAIT, worker.join())
                    .await
                    .unwrap()
                    .expect_err("fatal setup rejection must remain a pipeline failure");
                assert_eq!(worker.state(), pipecat_core::WorkerState::Failed);
                assert!(worker.errors().iter().any(|error| error.fatal
                    && error.error.contains("synthetic downstream setup rejection")));
                failure
            }
            Err(failure) => failure,
        };
    assert!(failure
        .to_string()
        .contains("synthetic downstream setup rejection"));
    assert!(processor.session_joined());
    timeout(WAIT, peer).await.unwrap().unwrap();
}
