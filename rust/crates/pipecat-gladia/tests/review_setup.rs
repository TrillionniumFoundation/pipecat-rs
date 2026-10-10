// Independent returned-endpoint and bounded-setup gates. Synthetic credentials,
// real HTTP/WS sockets, no live provider or external DNS destination.
mod support;
use pipecat_gladia::*;
use serde_json::json;
use std::time::{Duration, Instant};
use support::*;
use tokio::{io::AsyncWriteExt, net::TcpListener};

#[tokio::test]
async fn review_allowlisted_dns_loopback_remains_forbidden_before_handshake() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = ws.local_addr().unwrap().port();
    let mut config = GladiaSttConfig::new("synthetic-review-key");
    config.endpoint = format!("http://{}/v2/live", http.local_addr().unwrap());
    config.allowed_session_origins = vec![format!("wss://localhost:{port}")];
    config.allow_loopback = true;
    let server = tokio::spawn(async move {
        let (mut socket, _) = http.accept().await.unwrap();
        request(&mut socket).await;
        respond(&mut socket,201,&json!({"id":"fixture","url":format!("wss://localhost:{port}/v2/live?token=synthetic-review-token")}).to_string()).await;
    });
    let error = GladiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .err()
        .unwrap();
    assert_eq!(error, GladiaError::Endpoint);
    assert!(
        tokio::time::timeout(Duration::from_millis(75), ws.accept())
            .await
            .is_err(),
        "literal-loopback permission must not allow DNS answers to bypass public-address policy"
    );
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn review_chunked_http_body_is_bounded_without_content_length() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GladiaSttConfig::new("synthetic-review-key");
    config.endpoint = format!("http://{}/v2/live", http.local_addr().unwrap());
    config.allow_loopback = true;
    config.limits.max_http_body_bytes = 256;
    let server = tokio::spawn(async move {
        let (mut socket, _) = http.accept().await.unwrap();
        request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
        let chunk = "sensitive-provider-body".repeat(20);
        let _ = socket
            .write_all(format!("{:x}\r\n{chunk}\r\n0\r\n\r\n", chunk.len()).as_bytes())
            .await;
    });
    let error = GladiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .err()
        .unwrap();
    assert_eq!(error, GladiaError::TooLarge);
    assert!(!format!("{error:?}").contains("sensitive-provider-body"));
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn review_rest_and_ws_upgrade_stalls_obey_whole_setup_deadline() {
    for stall_upgrade in [false, true] {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_addr = ws.local_addr().unwrap();
        let mut config = GladiaSttConfig::new("synthetic-review-key");
        config.endpoint = format!("http://{}/v2/live", http.local_addr().unwrap());
        config.allowed_session_origins = vec![format!("ws://{ws_addr}")];
        config.allow_loopback = true;
        config.limits.connect_timeout = Duration::from_millis(75);
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = http.accept().await.unwrap();
            request(&mut socket).await;
            if stall_upgrade {
                respond(&mut socket,201,&json!({"id":"fixture","url":format!("ws://{ws_addr}/v2/live?token=synthetic-review-token")}).to_string()).await;
                let (mut socket, _) = ws.accept().await.unwrap();
                request(&mut socket).await;
                let _ = release_rx.await;
                drop(socket);
            } else {
                let _ = release_rx.await;
                drop(socket);
            }
        });
        let start = Instant::now();
        let error = GladiaSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .err()
            .unwrap();
        assert!(
            matches!(
                error,
                GladiaError::Timeout("session setup") | GladiaError::Transport
            ),
            "setup phase {stall_upgrade}: {error:?}"
        );
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(!format!("{error:?}").contains("synthetic-review-token"));
        let _ = release.send(());
        bounded(server).await.unwrap();
    }
}
