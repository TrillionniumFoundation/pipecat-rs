//! Source-derived Azure contracts exercised with synthetic loopback HTTP only.
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

async fn read_request(socket: &mut TcpStream) -> (String, Vec<u8>) {
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    (headers, bytes[end..end + length].to_vec())
}
async fn respond(socket: &mut TcpStream, status: &str, extra: &str, body: &str) {
    socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n{extra}\r\n{body}", body.len()).as_bytes()).await.unwrap();
}
fn success() -> &'static str {
    "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3}}\n\ndata: [DONE]\n\n"
}
fn request() -> CompletionRequest {
    CompletionRequest::new(
        "test-deployment",
        Context::new(vec![Message::text(Role::User, "hello")]),
    )
}
fn config(listener: &TcpListener, v1: bool, auth: AzureAuth) -> ClientConfig {
    let endpoint = format!(
        "http://{}{}",
        listener.local_addr().unwrap(),
        if v1 { "/openai/v1/" } else { "" }
    );
    let mut config = ClientConfig::azure(AzureConfig::new(endpoint, auth).unwrap());
    config.request_timeout = Duration::from_secs(3);
    config.idle_timeout = Duration::from_secs(1);
    config
}
async fn collect(mut stream: LlmStream) -> Vec<Result<StreamEvent, ServiceError>> {
    timeout(Duration::from_secs(4), async {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event);
        }
        events
    })
    .await
    .unwrap()
}
fn assert_success(events: &[Result<StreamEvent, ServiceError>]) {
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert_eq!(
        events.first().unwrap().as_ref().unwrap(),
        &StreamEvent::Started
    );
    assert_eq!(
        events.last().unwrap().as_ref().unwrap(),
        &StreamEvent::Finished {
            reason: Some(FinishReason::Stop)
        }
    );
}
fn rotating(calls: Arc<AtomicUsize>) -> AzureAuth {
    AzureAuth::EntraTokenProvider(AzureTokenProvider::new(move || {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        async move { Ok(format!("token-{n}")) }
    }))
}

#[test]
fn python_source_contracts_are_pinned() {
    let source = include_str!("../../../../src/pipecat/services/azure/llm.py");
    assert!(source.contains("V1_ENDPOINT_PATH = \"/openai/v1\""));
    assert!(source.contains("DATED_API_VERSION = \"2025-04-01-preview\""));
    assert!(source.contains("api_key=self._token_provider or api_key"));
    assert!(source.contains("azure_ad_token_provider=self._token_provider"));
    assert!(
        include_str!("../../../../src/pipecat/services/azure/common.py")
            .contains("Callable[[], Awaitable[str]]")
    );
    assert_eq!(AZURE_DATED_API_VERSION, "2025-04-01-preview");
}

#[tokio::test]
async fn v1_and_dated_key_and_entra_route_and_authenticate() {
    for v1 in [true, false] {
        for entra in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let auth = if entra {
                rotating(calls.clone())
            } else {
                AzureAuth::ApiKey("synthetic-key".into())
            };
            let c = config(&listener, v1, auth);
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let captured = read_request(&mut socket).await;
                respond(&mut socket, "200 OK", "", success()).await;
                captured
            });
            let client = LlmClient::new(c).unwrap();
            let body = client.request_body(&request()).unwrap();
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "pure validation cannot acquire credentials"
            );
            let events = collect(client.stream(request()).unwrap()).await;
            assert_success(&events);
            let (headers, sent) = server.await.unwrap();
            let headers = headers.to_ascii_lowercase();
            assert!(headers.starts_with(if v1 { "post /openai/v1/chat/completions http/1.1" } else { "post /openai/deployments/test-deployment/chat/completions?api-version=2025-04-01-preview http/1.1" }), "{headers}");
            let key = if entra { "token-0" } else { "synthetic-key" };
            if entra || v1 {
                assert!(headers.contains(&format!("authorization: bearer {key}\r\n")));
                assert!(!headers.contains("api-key:"));
            } else {
                assert!(headers.contains(&format!("api-key: {key}\r\n")));
                assert!(!headers.contains("authorization:"));
            }
            assert!(!headers.contains("x-api-key:"));
            assert_eq!(serde_json::from_slice::<Value>(&sent).unwrap(), body);
            assert_eq!(body["model"], "test-deployment");
            assert_eq!(body["stream_options"], json!({"include_usage":true}));
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(entra));
            assert!(events.iter().any(|e| matches!(
                e,
                Ok(StreamEvent::Usage(Usage {
                    total_tokens: 3,
                    ..
                }))
            )));
        }
    }
}

#[tokio::test]
async fn dated_deployment_is_one_encoded_segment_and_version_is_explicit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let azure = AzureConfig::new(
        format!("http://{}", listener.local_addr().unwrap()),
        AzureAuth::ApiKey("key".into()),
    )
    .unwrap()
    .with_api_version("2024-10-21")
    .unwrap();
    let client = LlmClient::new(ClientConfig::azure(azure)).unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let captured = read_request(&mut socket).await;
        respond(&mut socket, "200 OK", "", success()).await;
        captured
    });
    let mut req = request();
    req.model = "nested/../deploy %?#".into();
    assert_success(&collect(client.stream(req).unwrap()).await);
    let (headers, body) = server.await.unwrap();
    assert!(headers.starts_with("POST /openai/deployments/nested%2F..%2Fdeploy%20%25%3F%23/chat/completions?api-version=2024-10-21 HTTP/1.1"), "{headers}");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["model"],
        "nested/../deploy %?#"
    );
}

#[test]
fn rejects_ambiguous_urls_credentials_and_mixed_client_configuration() {
    for endpoint in [
        "",
        " https://resource.invalid",
        "https://resource.invalid\n",
        "http://resource.invalid",
        "ftp://resource.invalid",
        "https:////resource.invalid",
        "https://resource.invalid/x/../",
        "https://user@resource.invalid",
        "https://@resource.invalid",
        "https://resource.invalid?secret",
        "https://resource.invalid#secret",
        "https://resource.invalid/openai/v1?api-version=x",
        "https://resource.invalid/openai/deployments/x",
        "https://resource.invalid/%2e",
        "https://resource.invalid\\openai\\v1",
        "https://resource.invalid//openai/v1",
    ] {
        assert!(
            AzureConfig::new(endpoint, AzureAuth::ApiKey("key".into())).is_err(),
            "{endpoint}"
        );
    }
    for endpoint in [
        "https://resource.invalid",
        "https://resource.invalid/",
        "https://resource.invalid/openai/v1/",
        "http://localhost:42",
        "http://127.0.0.1:42",
        "http://[::1]:42/openai/v1",
    ] {
        assert!(
            AzureConfig::new(endpoint, AzureAuth::ApiKey("key".into())).is_ok(),
            "{endpoint}"
        );
    }
    for key in ["", "has space", "secret\nheader", "\tkey"] {
        assert!(
            AzureConfig::new("https://resource.invalid", AzureAuth::ApiKey(key.into())).is_err()
        );
    }
    let config = ClientConfig::azure(
        AzureConfig::new("https://resource.invalid", AzureAuth::ApiKey("key".into())).unwrap(),
    );
    assert!(LlmClient::with_http_client(config.clone(), reqwest::Client::new()).is_err());
    for index in 0..4 {
        let mut c = config.clone();
        match index {
            0 => c.endpoint = "https://other.invalid".into(),
            1 => c.api_key = Some("other".into()),
            2 => c.provider = Provider::Anthropic,
            _ => {
                c.openai_preset = Some(presets::OpenAiPresetConfig::new(
                    presets::OpenAiPreset::OpenAi,
                ))
            }
        };
        assert!(LlmClient::new(c).is_err());
    }
    let client = LlmClient::new(config).unwrap();
    for deployment in ["", " ", ".", "..", "secret\n"] {
        let mut req = request();
        req.model = deployment.into();
        assert!(client.request_body(&req).is_err(), "{deployment}");
    }
}

#[tokio::test]
async fn retry_refreshes_entra_but_payload_and_path_stay_immutable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut c = config(&listener, false, rotating(calls.clone()));
    c.retry = RetryPolicy::OnceOnHeaderTimeout {
        timeout: Duration::from_millis(100),
    };
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let original = read_request(&mut first).await;
        let (mut second, _) = listener.accept().await.unwrap();
        let retried = read_request(&mut second).await;
        assert_eq!(original.1, retried.1);
        assert_eq!(original.0.lines().next(), retried.0.lines().next());
        assert!(original.0.contains("Bearer token-0"));
        assert!(retried.0.contains("Bearer token-1"));
        let mut b = [0];
        assert_eq!(first.read(&mut b).await.unwrap(), 0);
        respond(&mut second, "200 OK", "", success()).await;
        assert!(timeout(Duration::from_millis(120), listener.accept())
            .await
            .is_err());
    });
    assert_success(&collect(LlmClient::new(c).unwrap().stream(request()).unwrap()).await);
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn token_callback_wait_is_cancellable_and_shares_total_deadline_without_retry() {
    for cancelled in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let entered2 = entered.clone();
        let calls2 = calls.clone();
        let auth = AzureAuth::EntraTokenProvider(AzureTokenProvider::new(move || {
            calls2.fetch_add(1, Ordering::SeqCst);
            entered2.notify_one();
            async { std::future::pending().await }
        }));
        let mut c = config(&listener, true, auth);
        c.request_timeout = Duration::from_millis(160);
        c.retry = RetryPolicy::OnceOnHeaderTimeout {
            timeout: Duration::from_millis(20),
        };
        let cancel = CancellationToken::new();
        let stream = LlmClient::new(c)
            .unwrap()
            .stream_with_cancel(request(), cancel.clone())
            .unwrap();
        entered.notified().await;
        if cancelled {
            cancel.cancel();
        }
        let events = collect(stream).await;
        assert!(events.iter().any(|e| if cancelled {
            matches!(e, Err(ServiceError::Cancelled))
        } else {
            matches!(e, Err(ServiceError::Timeout))
        }));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn pre_cancelled_and_dropped_streams_do_not_acquire_or_retain_tokens() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let c = config(&listener, true, rotating(calls.clone()));
    let cancel = CancellationToken::new();
    cancel.cancel();
    let events = collect(
        LlmClient::new(c)
            .unwrap()
            .stream_with_cancel(request(), cancel)
            .unwrap(),
    )
    .await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Cancelled))));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let (drop_tx, drop_rx) = oneshot::channel();
    let sender = Arc::new(std::sync::Mutex::new(Some(drop_tx)));
    struct Guard(Option<oneshot::Sender<()>>);
    impl Drop for Guard {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let auth = AzureAuth::EntraTokenProvider(AzureTokenProvider::new(move || {
        let guard = Guard(sender.lock().unwrap().take());
        async move {
            let _guard = guard;
            std::future::pending().await
        }
    }));
    let stream = LlmClient::new(config(&listener, true, auth))
        .unwrap()
        .stream(request())
        .unwrap();
    tokio::task::yield_now().await;
    drop(stream);
    timeout(Duration::from_secs(1), drop_rx)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn azure_diagnostics_never_reflect_private_provider_or_callback_text() {
    const SECRET: &str = "PRIVATE-SENTINEL-123";
    for (status, extra, body, expected_retry) in [
        ("429 Too Many Requests", "Retry-After: PRIVATE-SENTINEL-123\r\n", "PRIVATE-SENTINEL-123", None),
        ("429 Too Many Requests", "Retry-After: 00002\r\n", "PRIVATE-SENTINEL-123", None),
        ("429 Too Many Requests", "Retry-After: 99999999999999999999999999999\r\n", "PRIVATE-SENTINEL-123", None),
        ("200 OK", "", "data: {\"error\":{\"type\":\"PRIVATE-SENTINEL-123\",\"message\":\"PRIVATE-SENTINEL-123\"}}\n\n", None),
        ("200 OK", "", "data: {\"PRIVATE-SENTINEL-123\": invalid}\n\n", None),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap(); let c = config(&listener,true,AzureAuth::ApiKey(SECRET.into()));
        assert!(!format!("{c:?}").contains(SECRET)); assert!(!format!("{:?}",c.azure).contains(SECRET));
        let server = tokio::spawn(async move { let (mut socket, _) = listener.accept().await.unwrap(); read_request(&mut socket).await; respond(&mut socket,status,extra,body).await; });
        let client = LlmClient::new(c).unwrap(); assert!(!format!("{client:?}").contains(SECRET));
        let events = collect(client.stream(request()).unwrap()).await;
        assert!(!format!("{events:?}").contains(SECRET));
        let errors: Vec<_> = events.iter().filter_map(|e| e.as_ref().err()).collect(); assert_eq!(errors.len(),1);
        assert!(!errors[0].to_string().contains(SECRET));
        if let ServiceError::Http { retry_after, .. } = errors[0] { assert_eq!(retry_after.as_deref(),expected_retry); }
        server.await.unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let auth = AzureAuth::EntraTokenProvider(AzureTokenProvider::new(|| async {
        Err(ServiceError::Transport(SECRET.into()))
    }));
    let events = collect(
        LlmClient::new(config(&listener, true, auth))
            .unwrap()
            .stream(request())
            .unwrap(),
    )
    .await;
    assert!(!format!("{events:?}").contains(SECRET));
    assert!(events.iter().any(Result::is_err));
    assert!(timeout(Duration::from_millis(30), listener.accept())
        .await
        .is_err());
}

#[tokio::test]
async fn unknown_finish_reason_is_redacted_without_altering_generic_openai() {
    for azure in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c = if azure {
            config(&listener, true, AzureAuth::ApiKey("key".into()))
        } else {
            let mut c = ClientConfig::new(Provider::OpenAi, "key");
            c.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
            c
        };
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            respond(&mut socket,"200 OK","","data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"PRIVATE-FINISH\"}]}\n\ndata: [DONE]\n\n").await;
        });
        let events = collect(LlmClient::new(c).unwrap().stream(request()).unwrap()).await;
        assert_eq!(
            events.last().unwrap().as_ref().unwrap(),
            &StreamEvent::Finished {
                reason: Some(FinishReason::Other(
                    if azure {
                        "azure_other"
                    } else {
                        "PRIVATE-FINISH"
                    }
                    .into()
                ))
            }
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn callback_that_cancels_before_returning_cannot_send_a_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cancel = CancellationToken::new();
    let callback_cancel = cancel.clone();
    let auth = AzureAuth::EntraTokenProvider(AzureTokenProvider::new(move || {
        callback_cancel.cancel();
        async { Ok("token".into()) }
    }));
    let events = collect(
        LlmClient::new(config(&listener, true, auth))
            .unwrap()
            .stream_with_cancel(request(), cancel)
            .unwrap(),
    )
    .await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Cancelled))));
    assert!(timeout(Duration::from_millis(50), listener.accept())
        .await
        .is_err());
}

#[tokio::test]
async fn cancellation_during_headers_stops_retry_and_closes_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut c = config(&listener, true, rotating(calls.clone()));
    c.retry = RetryPolicy::OnceOnHeaderTimeout {
        timeout: Duration::from_millis(150),
    };
    let (ready_tx, ready_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        ready_tx.send(()).unwrap();
        let mut b = [0];
        assert_eq!(socket.read(&mut b).await.unwrap(), 0);
        assert!(timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err());
    });
    let cancel = CancellationToken::new();
    let stream = LlmClient::new(c)
        .unwrap()
        .stream_with_cancel(request(), cancel.clone())
        .unwrap();
    ready_rx.await.unwrap();
    cancel.cancel();
    let events = collect(stream).await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Cancelled))));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn total_deadline_includes_second_token_and_does_not_reset_on_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls2 = calls.clone();
    let auth = AzureAuth::EntraTokenProvider(AzureTokenProvider::new(move || {
        let n = calls2.fetch_add(1, Ordering::SeqCst);
        async move {
            if n > 0 {
                std::future::pending::<()>().await;
            }
            Ok("token".into())
        }
    }));
    let mut c = config(&listener, true, auth);
    c.request_timeout = Duration::from_millis(200);
    c.retry = RetryPolicy::OnceOnHeaderTimeout {
        timeout: Duration::from_millis(80),
    };
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        let mut b = [0];
        assert_eq!(socket.read(&mut b).await.unwrap(), 0);
        assert!(timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err());
    });
    let start = tokio::time::Instant::now();
    let events = collect(LlmClient::new(c).unwrap().stream(request()).unwrap()).await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Timeout))));
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn status_and_stream_errors_never_retry_or_forward_redirect_credentials() {
    for (status, body) in [
        ("302 Found", ""),
        ("429 Too Many Requests", "private"),
        (
            "200 OK",
            "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut c = config(&listener, true, rotating(calls.clone()));
        c.retry = RetryPolicy::OnceOnHeaderTimeout {
            timeout: Duration::from_millis(50),
        };
        let location = format!(
            "Location: http://{}/stolen\r\n",
            destination.local_addr().unwrap()
        );
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            respond(&mut socket, status, &location, body).await;
            assert!(timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err());
            assert!(timeout(Duration::from_millis(50), destination.accept())
                .await
                .is_err());
        });
        let events = collect(LlmClient::new(c).unwrap().stream(request()).unwrap()).await;
        assert!(events.iter().any(Result::is_err));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[test]
fn azure_strict_url_rejection_preserves_generic_whitespace_compatibility() {
    let endpoint = " \nhttps://resource.invalid/v1/chat/completions\t";
    let mut generic = ClientConfig::new(Provider::OpenAi, "key");
    generic.endpoint = endpoint.into();
    assert!(LlmClient::new(generic)
        .unwrap()
        .request_body(&request())
        .is_ok());
    assert!(AzureConfig::new(
        " \nhttps://resource.invalid/openai/v1\t",
        AzureAuth::ApiKey("key".into())
    )
    .is_err());
}

// Environment mutation is isolated in a subprocess so parallel tests cannot
// alter each other's proxy configuration.
#[test]
fn azure_ignores_ambient_proxy_environment() {
    if std::env::var_os("PIPECAT_AZURE_PROXY_CHILD").is_some() {
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "azure_proxy_child", "--nocapture"])
        .env("PIPECAT_AZURE_PROXY_CHILD", "1")
        .env("HTTP_PROXY", "http://127.0.0.1:1")
        .env("http_proxy", "http://127.0.0.1:1")
        .env("HTTPS_PROXY", "http://127.0.0.1:1")
        .env("https_proxy", "http://127.0.0.1:1")
        .env("ALL_PROXY", "http://127.0.0.1:1")
        .env("all_proxy", "http://127.0.0.1:1")
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[tokio::test]
async fn azure_proxy_child() {
    if std::env::var_os("PIPECAT_AZURE_PROXY_CHILD").is_none() {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c = config(&listener, true, AzureAuth::ApiKey("key".into()));
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        respond(&mut socket, "200 OK", "", success()).await;
    });
    assert_success(&collect(LlmClient::new(c).unwrap().stream(request()).unwrap()).await);
    timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn numeric_credential_reflected_in_retry_after_is_not_a_diagnostic() {
    const NUMERIC_SECRET: &str = "987654321";
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c = config(&listener, false, AzureAuth::ApiKey(NUMERIC_SECRET.into()));
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        respond(
            &mut socket,
            "429 Too Many Requests",
            "Retry-After: 987654321\r\n",
            NUMERIC_SECRET,
        )
        .await;
    });
    let events = collect(LlmClient::new(c).unwrap().stream(request()).unwrap()).await;
    assert!(!format!("{events:?}").contains(NUMERIC_SECRET));
    let error = events
        .iter()
        .find_map(|event| event.as_ref().err())
        .unwrap();
    assert!(!error.to_string().contains(NUMERIC_SECRET));
    assert!(matches!(
        error,
        ServiceError::Http {
            status: 429,
            retry_after: None,
            ..
        }
    ));
    server.await.unwrap();
}
