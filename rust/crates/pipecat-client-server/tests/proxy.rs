// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use pipecat_client_server::{ClientProxy, ProxyError, ProxyLimits, SessionMethod};
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn mock(response: &str) -> (String, tokio::task::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/prefix/start", listener.local_addr().unwrap());
    let response = response.to_owned();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = Vec::new();
        loop {
            let mut chunk = [0; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..n]);
            if let Some(split) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&bytes[..split]).to_ascii_lowercase();
                let length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap_or("0")
                    .parse::<usize>()
                    .unwrap();
                if bytes.len() >= split + 4 + length {
                    break;
                }
            }
            assert!(bytes.len() < 65536);
        }
        socket.write_all(response.as_bytes()).await.unwrap();
        String::from_utf8(bytes).unwrap()
    });
    (endpoint, task)
}
fn ok(body: &str) -> String {
    format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
}

#[tokio::test]
async fn start_keeps_credentials_server_side_and_forwards_json() {
    let (url, task) = mock(&ok(r#"{"wsUrl":"wss://example.test/voice"}"#)).await;
    let proxy = ClientProxy::new(&url, Some("private-test-token"), ProxyLimits::default()).unwrap();
    assert!(!format!("{proxy:?}").contains("private-test-token"));
    let result = proxy
        .start(&json!({"transport":"websocket"}))
        .await
        .unwrap();
    assert_eq!(result["wsUrl"], "wss://example.test/voice");
    let request = task.await.unwrap();
    assert!(request.starts_with("POST /prefix/start HTTP/1.1"));
    assert!(request.contains("authorization: Bearer private-test-token"));
    assert!(request.ends_with(r#"{"transport":"websocket"}"#));
}

#[tokio::test]
async fn session_patch_preserves_base_and_escapes_unicode_segment() {
    let (url, task) = mock(&ok(r#"{"ok":true}"#)).await;
    let proxy = ClientProxy::new(&url, None, ProxyLimits::default()).unwrap();
    proxy
        .session(
            SessionMethod::Patch,
            "session 1",
            &["api", "offer"],
            &json!({"sdp":"x"}),
        )
        .await
        .unwrap();
    let request = task.await.unwrap();
    assert!(request.starts_with("PATCH /prefix/sessions/session%201/api/offer HTTP/1.1"));
    assert!(!request.contains("authorization:"));
}

#[tokio::test]
async fn traversal_and_arbitrary_destination_are_rejected_before_network() {
    let proxy = ClientProxy::new("http://127.0.0.1:1/start", None, ProxyLimits::default()).unwrap();
    for id in ["", ".", "..", "a/b", "a\\b", "%2e%2e", "a?b", "x#y", "\n"] {
        assert_eq!(
            proxy
                .session(SessionMethod::Post, id, &["offer"], &json!({}))
                .await,
            Err(ProxyError::Path)
        );
    }
    assert_eq!(
        proxy
            .session(SessionMethod::Post, "ok", &[], &json!({}))
            .await,
        Err(ProxyError::Path)
    );
}

#[test]
fn configuration_rejects_remote_plaintext_credentials_and_unbounded_limits() {
    for url in [
        "http://remote.example/start",
        "file:///start",
        "https://user:pw@example.test/start",
        "https://example.test/start?secret=x",
        "https://example.test/start#fragment",
        "https://example.test/restart",
        "not a url",
    ] {
        assert!(matches!(
            ClientProxy::new(url, None, ProxyLimits::default()),
            Err(ProxyError::Configuration)
        ));
    }
    for url in [
        "https://example.test/nested/start",
        "http://localhost:7860/start",
        "http://[::1]:7860/start",
    ] {
        assert!(ClientProxy::new(url, None, ProxyLimits::default()).is_ok());
    }
    assert!(ClientProxy::new(
        "https://example.test/start",
        Some("x\r\ny"),
        ProxyLimits::default()
    )
    .is_err());
    assert!(ClientProxy::new(
        "https://example.test/start",
        None,
        ProxyLimits {
            concurrent_requests: 0,
            ..Default::default()
        }
    )
    .is_err());
}

#[tokio::test]
async fn request_size_rejected_without_connecting() {
    let proxy = ClientProxy::new(
        "http://127.0.0.1:1/start",
        None,
        ProxyLimits {
            request_bytes: 3,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        proxy.start(&json!({"x":"large"})).await,
        Err(ProxyError::RequestTooLarge)
    );
}

#[tokio::test]
async fn response_bounds_apply_to_declared_and_chunked_lengths() {
    for response in [ok(r#"{"x":"too long"}"#), "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n8\r\n12345678\r\n0\r\n\r\n".to_owned()] {
        let (url, task) = mock(&response).await;
        let proxy = ClientProxy::new(&url, None, ProxyLimits { response_bytes: 4, ..Default::default() }).unwrap();
        assert_eq!(proxy.start(&json!({})).await, Err(ProxyError::ResponseTooLarge));
        task.await.unwrap();
    }
}

#[tokio::test]
async fn redirects_and_sensitive_upstream_errors_do_not_leak() {
    for (response, expected) in [
        ("HTTP/1.1 302 Found\r\nLocation: https://other.invalid/secret\r\nContent-Length: 0\r\n\r\n".to_owned(), ProxyError::Status(302)),
        (ok(r#"{"error":"secret provider error"}"#), ProxyError::Upstream),
        (ok("private non-json"), ProxyError::Json),
        (ok("null"), ProxyError::Upstream),
    ] {
        let (url, task) = mock(&response).await;
        let proxy = ClientProxy::new(&url, Some("private-token"), ProxyLimits::default()).unwrap();
        let error = proxy.start(&json!({})).await.unwrap_err();
        assert_eq!(error, expected);
        assert!(!error.to_string().contains("secret"));
        task.await.unwrap();
    }
}

#[tokio::test]
async fn source_falsy_error_values_remain_successful() {
    for body in [
        r#"{"error":null}"#,
        r#"{"error":false}"#,
        r#"{"error":0}"#,
        r#"{"error":""}"#,
    ] {
        let (url, task) = mock(&ok(body)).await;
        assert!(ClientProxy::new(&url, None, ProxyLimits::default())
            .unwrap()
            .start(&json!({}))
            .await
            .is_ok());
        task.await.unwrap();
    }
}

#[tokio::test]
async fn timeout_covers_response_and_releases_concurrency_permit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/start", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let mut buf = [0; 4096];
        assert!(first.read(&mut buf).await.unwrap() > 0);
        let (mut second, _) = listener.accept().await.unwrap();
        assert!(second.read(&mut buf).await.unwrap() > 0);
        second.write_all(ok("{}").as_bytes()).await.unwrap();
    });
    let proxy = ClientProxy::new(
        &endpoint,
        None,
        ProxyLimits {
            concurrent_requests: 1,
            deadline: Duration::from_millis(100),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(proxy.start(&json!({})).await, Err(ProxyError::Deadline));
    assert!(proxy.start(&json!({})).await.is_ok());
    server.await.unwrap();
}
