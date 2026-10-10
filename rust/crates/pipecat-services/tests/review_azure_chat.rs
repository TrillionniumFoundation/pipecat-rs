//! Independent protocol review: synthetic loopback HTTP, no credentials or live Azure.
use pipecat_services::*;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};

const SECRET: &str = "PRIVATE-AZURE-REVIEW-SENTINEL";
const DONE: &str = "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";

fn request() -> CompletionRequest {
    CompletionRequest::new(
        "original-deployment",
        Context::new(vec![Message::text(Role::User, SECRET)]),
    )
}
fn config(listener: &TcpListener, v1: bool, auth: AzureAuth) -> ClientConfig {
    let url = format!(
        "http://{}{}",
        listener.local_addr().unwrap(),
        if v1 { "/openai/v1/" } else { "/" }
    );
    let mut config = ClientConfig::azure(AzureConfig::new(url, auth).unwrap());
    config.request_timeout = Duration::from_secs(2);
    config.idle_timeout = Duration::from_secs(1);
    config.retry = RetryPolicy::OnceOnHeaderTimeout {
        timeout: Duration::from_millis(70),
    };
    config
}
fn auth(calls: Arc<AtomicUsize>) -> AzureAuth {
    AzureAuth::EntraTokenProvider(AzureTokenProvider::new(move || {
        let index = calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok(format!("synthetic-token-{index}")) }
    }))
}
async fn read_request(socket: &mut TcpStream) -> (String, Vec<u8>) {
    timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        let mut buffer = [0; 2048];
        let end = loop {
            let n = socket.read(&mut buffer).await.unwrap();
            assert_ne!(n, 0);
            bytes.extend_from_slice(&buffer[..n]);
            if let Some(pos) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
        let length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        while bytes.len() < end + length {
            let n = socket.read(&mut buffer).await.unwrap();
            assert_ne!(n, 0);
            bytes.extend_from_slice(&buffer[..n]);
        }
        (headers, bytes[end..end + length].to_vec())
    })
    .await
    .unwrap()
}
async fn respond(socket: &mut TcpStream, status: &str, headers: &str, body: &str) {
    socket
        .write_all(
            format!("HTTP/1.1 {status}\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}", body.len()).as_bytes(),
        )
        .await
        .unwrap();
}
async fn collect(mut stream: LlmStream) -> Vec<Result<StreamEvent, ServiceError>> {
    timeout(Duration::from_secs(3), async {
        let mut result = vec![];
        while let Some(event) = stream.next().await {
            result.push(event);
        }
        result
    })
    .await
    .unwrap()
}
fn terminal(events: &[Result<StreamEvent, ServiceError>]) {
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Ok(StreamEvent::Finished { .. })))
            .count(),
        1,
        "{events:?}"
    );
    assert!(matches!(
        events.last(),
        Some(Ok(StreamEvent::Finished { .. }))
    ));
}
fn private_diagnostics(events: &[Result<StreamEvent, ServiceError>]) {
    for error in events.iter().filter_map(|event| event.as_ref().err()) {
        assert!(!format!("{error:?} {error}").contains(SECRET));
    }
}
async fn closed(socket: &mut TcpStream) {
    let mut buffer = [0; 1];
    assert_eq!(
        timeout(Duration::from_secs(1), socket.read(&mut buffer))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn resolved_model_is_identical_in_encoded_route_and_immutable_retry_body() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let client = LlmClient::new(config(&listener, false, auth(calls.clone()))).unwrap();
    let mut original = request();
    original.settings.base.model = Setting::Value("new/%2e%2e/\\route ?#☃".into());
    original.settings.temperature = Setting::Value(0.4);
    original.settings.max_tokens = Setting::Value(19);
    original.system_instruction = Some("Keep request immutable".into());
    let before = serde_json::to_value(&original).unwrap();
    let expected_body = client.request_body(&original).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let stream = client.stream(original.clone()).unwrap();
    original.settings.base.model = Setting::Value("later-request".into());
    original.context = Context::new(vec![Message::text(Role::User, "later-context")]);
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let first_request = read_request(&mut first).await;
        let (mut second, _) = listener.accept().await.unwrap();
        let second_request = read_request(&mut second).await;
        assert_eq!(first_request.1, second_request.1);
        assert_eq!(
            first_request.0.lines().next(),
            second_request.0.lines().next()
        );
        assert!(first_request.0.starts_with("POST /openai/deployments/new%2F%252e%252e%2F%5Croute%20%3F%23%E2%98%83/chat/completions?api-version=2025-04-01-preview HTTP/1.1"), "{}", first_request.0);
        assert!(first_request.0.contains("Bearer synthetic-token-0"));
        assert!(second_request.0.contains("Bearer synthetic-token-1"));
        assert_eq!(
            serde_json::from_slice::<Value>(&second_request.1).unwrap(),
            expected_body
        );
        closed(&mut first).await;
        respond(&mut second, "200 OK", "", DONE).await;
    });
    let events = collect(stream).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    terminal(&events);
    assert_eq!(before["model"], "original-deployment");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.await.unwrap();
}

#[tokio::test]
async fn slow_successful_token_callback_does_not_consume_header_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let provider = AzureAuth::EntraTokenProvider(AzureTokenProvider::new(move || {
        callback_calls.fetch_add(1, Ordering::SeqCst);
        async {
            tokio::time::sleep(Duration::from_millis(160)).await;
            Ok("late-valid-token".into())
        }
    }));
    let client = LlmClient::new(config(&listener, true, provider)).unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (headers, _) = read_request(&mut socket).await;
        assert!(headers.contains("Bearer late-valid-token"));
        respond(&mut socket, "200 OK", "", DONE).await;
        assert!(timeout(Duration::from_millis(150), listener.accept())
            .await
            .is_err());
    });
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    terminal(&events);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.await.unwrap();
}

#[tokio::test]
async fn invalid_token_callback_values_are_redacted_before_any_network_attempt() {
    for value in [
        format!("{SECRET}\r\ninjected: value"),
        format!("{SECRET} "),
        String::new(),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let callback_calls = calls.clone();
        let provider = AzureAuth::EntraTokenProvider(AzureTokenProvider::new(move || {
            callback_calls.fetch_add(1, Ordering::SeqCst);
            let value = value.clone();
            async move { Ok(value) }
        }));
        let client = LlmClient::new(config(&listener, true, provider)).unwrap();
        let events = collect(client.stream(request()).unwrap()).await;
        assert_eq!(events.iter().filter(|event| event.is_err()).count(), 1);
        assert!(events
            .iter()
            .any(|event| matches!(event, Err(ServiceError::InvalidRequest(_)))));
        private_diagnostics(&events);
        terminal(&events);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn retry_token_failure_has_one_network_attempt_and_one_redacted_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let provider = AzureAuth::EntraTokenProvider(AzureTokenProvider::new(move || {
        let attempt = callback_calls.fetch_add(1, Ordering::SeqCst);
        async move {
            if attempt == 0 {
                Ok("first-valid-token".into())
            } else {
                Err(ServiceError::Http {
                    status: 401,
                    message: SECRET.into(),
                    retry_after: Some(SECRET.into()),
                })
            }
        }
    }));
    let client = LlmClient::new(config(&listener, true, provider)).unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        closed(&mut socket).await;
        assert!(timeout(Duration::from_millis(160), listener.accept())
            .await
            .is_err());
    });
    let events = collect(client.stream(request()).unwrap()).await;
    assert_eq!(events.iter().filter(|event| event.is_err()).count(), 1);
    assert!(events
        .iter()
        .any(|event| matches!(event, Err(ServiceError::Provider { .. }))));
    private_diagnostics(&events);
    terminal(&events);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.await.unwrap();
}

#[tokio::test]
async fn every_retry_after_value_is_suppressed_and_error_body_is_never_awaited() {
    for value in [
        "987654321",
        "00002",
        "Wed, 21 Oct 2026 07:28:00 GMT",
        SECRET,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let client = LlmClient::new(config(&listener, false, auth(calls.clone()))).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            // A huge declared body is deliberately never sent; failure must be decided from headers.
            socket.write_all(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Length: 1000000\r\nRetry-After: {value}\r\n\r\n").as_bytes()).await.unwrap();
            closed(&mut socket).await;
            assert!(timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_err());
        });
        let events = timeout(
            Duration::from_millis(700),
            collect(client.stream(request()).unwrap()),
        )
        .await
        .unwrap();
        let errors: Vec<_> = events
            .iter()
            .filter_map(|event| event.as_ref().err())
            .collect();
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            ServiceError::Http {
                status: 429,
                retry_after: None,
                ..
            }
        ));
        private_diagnostics(&events);
        terminal(&events);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn partial_usage_survives_protocol_failure_without_private_finish_or_error_leak() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let client = LlmClient::new(config(&listener, true, auth(calls.clone()))).unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        let body = format!("data: {{\"choices\":[{{\"delta\":{{\"content\":\"safe partial\"}},\"finish_reason\":\"{SECRET}\"}}],\"usage\":{{\"prompt_tokens\":7,\"completion_tokens\":2,\"total_tokens\":9}}}}\n\ndata: {{\"error\":{{\"type\":\"{SECRET}\",\"message\":\"{SECRET}\"}}}}\n\n");
        respond(&mut socket, "200 OK", "", &body).await;
        assert!(timeout(Duration::from_millis(80), listener.accept())
            .await
            .is_err());
    });
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events
        .iter()
        .any(|event| matches!(event, Ok(StreamEvent::TextDelta(text)) if text == "safe partial")));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Ok(StreamEvent::Usage(_))))
            .count(),
        1
    );
    assert!(events.iter().any(|event| matches!(
        event,
        Ok(StreamEvent::Usage(Usage {
            total_tokens: 9,
            ..
        }))
    )));
    assert_eq!(
        events.last().unwrap().as_ref().unwrap(),
        &StreamEvent::Finished {
            reason: Some(FinishReason::Other("azure_other".into()))
        }
    );
    assert!(!format!("{events:?}").contains(SECRET));
    private_diagnostics(&events);
    terminal(&events);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.await.unwrap();
}

#[tokio::test]
async fn body_cancel_idle_and_total_deadline_close_socket_without_retry() {
    for mode in ["cancel", "idle", "total"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut configuration = config(&listener, true, auth(calls.clone()));
        if mode == "idle" {
            configuration.idle_timeout = Duration::from_millis(130);
        }
        if mode == "total" {
            configuration.request_timeout = Duration::from_millis(200);
        }
        let client = LlmClient::new(configuration).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1000000\r\n\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\n\n").await.unwrap();
            closed(&mut socket).await;
            assert!(timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err());
        });
        let mut stream = client.stream(request()).unwrap();
        assert!(matches!(
            stream.next().await,
            Some(Ok(StreamEvent::Started))
        ));
        assert!(matches!(
            stream.next().await,
            Some(Ok(StreamEvent::TextDelta(_)))
        ));
        if mode == "cancel" {
            stream.cancel();
        }
        let events = collect(stream).await;
        assert_eq!(events.iter().filter(|event| event.is_err()).count(), 1);
        assert!(events.iter().any(|event| if mode == "cancel" {
            matches!(event, Err(ServiceError::Cancelled))
        } else {
            matches!(event, Err(ServiceError::Timeout))
        }));
        assert!(events.iter().any(|event| matches!(
            event,
            Ok(StreamEvent::Usage(Usage {
                total_tokens: 3,
                ..
            }))
        )));
        terminal(&events);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn backpressure_cancellation_releases_socket_and_keeps_single_terminal_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut configuration = config(&listener, false, auth(calls.clone()));
    configuration.channel_capacity = 1;
    let client = LlmClient::new(configuration).unwrap();
    let (sent_tx, sent_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        let body = format!("data: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":4,\"completion_tokens\":1,\"total_tokens\":5}}}}\n\n{}", "data: {\"choices\":[{\"delta\":{\"content\":\"delta\"}}]}\n\n".repeat(100));
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1000000\r\n\r\n{body}").as_bytes()).await.unwrap();
        sent_tx.send(()).unwrap();
        closed(&mut socket).await;
        assert!(timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err());
    });
    let stream = client.stream(request()).unwrap();
    sent_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    stream.cancel();
    let events = collect(stream).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Err(ServiceError::Cancelled)))
            .count(),
        1
    );
    assert!(
        events
            .iter()
            .filter(|event| matches!(event, Ok(StreamEvent::TextDelta(_))))
            .count()
            <= 1
    );
    assert!(events.iter().any(|event| matches!(
        event,
        Ok(StreamEvent::Usage(Usage {
            total_tokens: 5,
            ..
        }))
    )));
    terminal(&events);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.await.unwrap();
}

#[tokio::test]
async fn dropping_active_body_stream_closes_connection_and_does_not_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let client = LlmClient::new(config(&listener, true, auth(calls.clone()))).unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1000000\r\n\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n").await.unwrap();
        closed(&mut socket).await;
        assert!(timeout(Duration::from_millis(130), listener.accept())
            .await
            .is_err());
    });
    let mut stream = client.stream(request()).unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Ok(StreamEvent::Started))
    ));
    assert!(matches!(
        stream.next().await,
        Some(Ok(StreamEvent::TextDelta(_)))
    ));
    drop(stream);
    server.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn preserving_method_redirects_never_send_keys_or_tokens_to_destination() {
    for v1 in [false, true] {
        for status in ["307 Temporary Redirect", "308 Permanent Redirect"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let configuration = config(&listener, v1, AzureAuth::ApiKey(SECRET.into()));
            let client = LlmClient::new(configuration).unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                let header = format!(
                    "Location: http://{}/{SECRET}\r\n",
                    destination.local_addr().unwrap()
                );
                respond(&mut socket, status, &header, SECRET).await;
                assert!(timeout(Duration::from_millis(100), destination.accept())
                    .await
                    .is_err());
                assert!(timeout(Duration::from_millis(80), listener.accept())
                    .await
                    .is_err());
            });
            let events = collect(client.stream(request()).unwrap()).await;
            assert!(events.iter().any(|event| matches!(
                event,
                Err(ServiceError::Http {
                    status: 307 | 308,
                    ..
                })
            )));
            assert!(!format!("{events:?}").contains(SECRET));
            terminal(&events);
            server.await.unwrap();
        }
    }
}

#[test]
fn azure_and_generic_openai_share_wire_body_without_sharing_validation_diagnostics() {
    let azure = LlmClient::new(ClientConfig::azure(
        AzureConfig::new(
            "https://private-review-resource.invalid/openai/v1",
            AzureAuth::ApiKey(SECRET.into()),
        )
        .unwrap(),
    ))
    .unwrap();
    let generic = LlmClient::new(ClientConfig::new(Provider::OpenAi, "synthetic")).unwrap();
    let mut req = request();
    req.settings.temperature = Setting::Null;
    req.settings.max_tokens = Setting::Value(33);
    req.extra
        .insert("response_format".into(), json!({"type":"json_object"}));
    req.system_instruction = Some("Keep data private".into());
    assert_eq!(
        azure.request_body(&req).unwrap(),
        generic.request_body(&req).unwrap()
    );
    assert!(!format!("{azure:?}").contains("private-review-resource"));
    assert!(!format!("{azure:?}").contains(SECRET));
    req.context.messages[0].role = Role::Tool;
    let azure_error = azure.request_body(&req).unwrap_err();
    let generic_error = generic.request_body(&req).unwrap_err();
    assert_eq!(
        azure_error.to_string(),
        "invalid request: invalid Azure request"
    );
    assert_ne!(azure_error.to_string(), generic_error.to_string());
}
