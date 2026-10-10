//! Retry and cancellation boundaries against actual loopback HTTP sockets.
use pipecat_services::*;
use serde_json::Value;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{timeout, Instant},
};

async fn read_request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = vec![];
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
                .map(|n| n.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    (
        headers,
        serde_json::from_slice(&bytes[end..end + length]).unwrap(),
    )
}
fn request() -> CompletionRequest {
    CompletionRequest::new(
        "synthetic",
        Context::new(vec![Message::text(Role::User, "hello")]),
    )
}
fn config(listener: &TcpListener) -> ClientConfig {
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    config.request_timeout = Duration::from_secs(2);
    config.retry = RetryPolicy::OnceOnHeaderTimeout {
        timeout: Duration::from_millis(300),
    };
    config
}
async fn collect(mut stream: LlmStream) -> Vec<Result<StreamEvent, ServiceError>> {
    timeout(Duration::from_secs(3), async {
        let mut events = vec![];
        while let Some(event) = stream.next().await {
            events.push(event);
        }
        events
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn header_retry_uses_remaining_total_deadline_and_closes_both_attempts() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config(&listener);
    config.request_timeout = Duration::from_millis(500);
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let original = read_request(&mut first).await;
        let (mut second, _) = listener.accept().await.unwrap();
        let second_started = Instant::now();
        let retried = read_request(&mut second).await;
        assert_eq!(
            original, retried,
            "body and headers must be immutable across retry"
        );
        let mut byte = [0];
        assert_eq!(first.read(&mut byte).await.unwrap(), 0);
        assert_eq!(second.read(&mut byte).await.unwrap(), 0);
        assert!(
            second_started.elapsed() < Duration::from_millis(375),
            "retry must not get a fresh 500 ms budget"
        );
        assert!(timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err());
    });
    let events = collect(LlmClient::new(config).unwrap().stream(request()).unwrap()).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Err(ServiceError::Timeout)))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Ok(StreamEvent::Started)))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Ok(StreamEvent::Finished { .. })))
            .count(),
        1
    );
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn explicit_cancellation_before_headers_prevents_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let (ready, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        ready.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        assert!(timeout(Duration::from_millis(400), listener.accept())
            .await
            .is_err());
    });
    let cancel = CancellationToken::new();
    let stream = LlmClient::new(config)
        .unwrap()
        .stream_with_cancel(request(), cancel.clone())
        .unwrap();
    timeout(Duration::from_secs(2), wait)
        .await
        .unwrap()
        .unwrap();
    cancel.cancel();
    let events = collect(stream).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Err(ServiceError::Cancelled)))
            .count(),
        1
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, Err(ServiceError::Timeout))));
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn received_headers_prevent_retry_even_before_any_sse_data() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config(&listener);
    config.idle_timeout = Duration::from_millis(80);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        assert!(timeout(Duration::from_millis(350), listener.accept())
            .await
            .is_err());
    });
    let events = collect(LlmClient::new(config).unwrap().stream(request()).unwrap()).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Err(ServiceError::Timeout)))
            .count(),
        1
    );
    assert!(!events
        .iter()
        .any(|event| matches!(event, Ok(StreamEvent::TextDelta(_)))));
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn retryable_http_status_does_not_activate_header_timeout_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        assert!(timeout(Duration::from_millis(350), listener.accept())
            .await
            .is_err());
    });
    let events = collect(LlmClient::new(config).unwrap().stream(request()).unwrap()).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Err(ServiceError::Http { status: 503, .. })))
            .count(),
        1
    );
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
