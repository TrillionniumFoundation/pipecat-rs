// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
//! Independent adversarial checks over actual TCP sockets.
use pipecat_client_server::{
    server::{serve, ServerConfig},
    ClientProxy, ProxyLimits,
};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

const ORIGIN: &str = "http://localhost:3000";
const WAIT: Duration = Duration::from_secs(2);

async fn launch(
    endpoint: &str,
    limits: ProxyLimits,
) -> (SocketAddr, oneshot::Sender<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let proxy = ClientProxy::new(endpoint, Some("private-review-token"), limits).unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        serve(
            listener,
            proxy,
            ServerConfig {
                allowed_origins: vec![ORIGIN.to_owned()],
                request_deadline: Duration::from_secs(10),
                ..Default::default()
            },
            async {
                let _ = stopped.await;
            },
        )
        .await
        .unwrap();
    });
    (addr, stop, task)
}

async fn send(addr: SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(WAIT, stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(response).unwrap()
}

fn request(path: &str, extra_headers: &str, body: &str) -> String {
    format!("POST {path} HTTP/1.1\r\nHost: localhost:3000\r\nOrigin: {ORIGIN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra_headers}\r\n{body}", body.len())
}

async fn read_headers(stream: &mut TcpStream) {
    let mut bytes = Vec::new();
    loop {
        let mut buf = [0; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buf[..n]);
        if bytes.windows(4).any(|part| part == b"\r\n\r\n") {
            return;
        }
        assert!(bytes.len() < 65536);
    }
}

#[tokio::test]
async fn hostile_origins_and_encoded_paths_never_reach_upstream() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/start", upstream.local_addr().unwrap());
    let (addr, stop, server) = launch(&endpoint, ProxyLimits::default()).await;
    let duplicate = send(
        addr,
        &request("/api/start", "Origin: http://evil.test\r\n", "{}"),
    )
    .await;
    assert!(duplicate.starts_with("HTTP/1.1 403"), "{duplicate}");
    let joined = send(
        addr,
        &request("/api/start", "", "{}").replace(ORIGIN, "http://localhost:3000, http://evil.test"),
    )
    .await;
    assert!(joined.starts_with("HTTP/1.1 403"), "{joined}");
    for path in [
        "/api/sessions/%2e%2e/offer",
        "/api/sessions/id/%2fstart",
        "/api/sessions/%252e%252e/offer",
        "/api/sessions/id/%ff",
        "/api/sessions/id//offer",
        "/api/sessions/id/offer?url=http://evil.test",
    ] {
        let response = send(addr, &request(path, "", "{}")).await;
        assert!(response.starts_with("HTTP/1.1 400"), "{path}: {response}");
        assert!(!response.contains("private-review-token"));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), upstream.accept())
            .await
            .is_err()
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn chunked_request_limit_is_enforced_without_upstream() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/start", upstream.local_addr().unwrap());
    let (addr, stop, server) = launch(
        &endpoint,
        ProxyLimits {
            request_bytes: 4,
            ..Default::default()
        },
    )
    .await;
    let response = send(addr, &format!("POST /api/start HTTP/1.1\r\nHost: localhost:3000\r\nOrigin: {ORIGIN}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n3\r\n{{\"x\r\n3\r\n\":1\r\n1\r\n}}\r\n0\r\n\r\n")).await;
    assert!(response.starts_with("HTTP/1.1 413"), "{response}");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), upstream.accept())
            .await
            .is_err()
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn downstream_disconnect_cancels_upstream_and_frees_proxy_permit() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/start", upstream.local_addr().unwrap());
    let (addr, stop, server) = launch(
        &endpoint,
        ProxyLimits {
            concurrent_requests: 1,
            ..Default::default()
        },
    )
    .await;
    let mut first_client = TcpStream::connect(addr).await.unwrap();
    first_client
        .write_all(request("/api/start", "", "{}").as_bytes())
        .await
        .unwrap();
    let (mut first_upstream, _) = tokio::time::timeout(WAIT, upstream.accept())
        .await
        .unwrap()
        .unwrap();
    read_headers(&mut first_upstream).await;
    drop(first_client);
    let mut buf = [0; 1024];
    loop {
        let n = tokio::time::timeout(WAIT, first_upstream.read(&mut buf))
            .await
            .expect("upstream remained alive after downstream disconnected")
            .unwrap();
        if n == 0 {
            break;
        }
    }
    let client = tokio::spawn(async move { send(addr, &request("/api/start", "", "{}")).await });
    let (mut second_upstream, _) = tokio::time::timeout(WAIT, upstream.accept())
        .await
        .expect("proxy permit not released")
        .unwrap();
    read_headers(&mut second_upstream).await;
    second_upstream
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
        .await
        .unwrap();
    let response = client.await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    stop.send(()).unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn dropping_server_future_cancels_all_owned_connection_tasks() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/start", upstream.local_addr().unwrap());
    let (addr, _stop, server) = launch(&endpoint, ProxyLimits::default()).await;
    let mut client = TcpStream::connect(addr).await.unwrap();
    client
        .write_all(request("/api/start", "", "{}").as_bytes())
        .await
        .unwrap();
    let (mut pending_upstream, _) = tokio::time::timeout(WAIT, upstream.accept())
        .await
        .unwrap()
        .unwrap();
    read_headers(&mut pending_upstream).await;
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    let mut buf = [0; 1024];
    loop {
        let n = tokio::time::timeout(WAIT, pending_upstream.read(&mut buf))
            .await
            .expect("upstream survived server cancellation")
            .unwrap();
        if n == 0 {
            break;
        }
    }
    assert_eq!(
        tokio::time::timeout(WAIT, client.read(&mut buf))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[cfg(unix)]
#[test]
fn asset_bundle_rejects_symlink_escape_and_total_bytes_over_cap() {
    use pipecat_client_server::server::AssetBundle;
    let root = std::env::temp_dir().join(format!("pipecat-review-assets-{}", std::process::id()));
    let outside =
        std::env::temp_dir().join(format!("pipecat-review-outside-{}", std::process::id()));
    std::fs::create_dir_all(root.join("pkg")).unwrap();
    std::fs::write(&outside, "not public").unwrap();
    for name in [
        "style.css",
        "config.json",
        "pkg/pipecat_browser_client.js",
        "pkg/pipecat_browser_client_bg.wasm",
    ] {
        std::fs::write(root.join(name), "public").unwrap();
    }
    std::os::unix::fs::symlink(&outside, root.join("index.html")).unwrap();
    assert!(AssetBundle::from_directory(&root).is_err());
    std::fs::remove_file(root.join("index.html")).unwrap();
    let huge = std::fs::File::create(root.join("index.html")).unwrap();
    huge.set_len((32 << 20) + 1).unwrap();
    assert!(AssetBundle::from_directory(&root).is_err());
    drop(huge);
    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_file(outside).unwrap();
}

#[tokio::test]
async fn connection_cap_rejects_extra_client_and_deadline_cancels_upstream() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/start", upstream.local_addr().unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let proxy = ClientProxy::new(&endpoint, None, ProxyLimits::default()).unwrap();
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        serve(
            listener,
            proxy,
            ServerConfig {
                allowed_origins: vec![ORIGIN.to_owned()],
                max_connections: 1,
                request_deadline: Duration::from_millis(500),
                ..Default::default()
            },
            async {
                let _ = stopped.await;
            },
        )
        .await
        .unwrap();
    });
    let mut first = TcpStream::connect(addr).await.unwrap();
    first
        .write_all(request("/api/start", "", "{}").as_bytes())
        .await
        .unwrap();
    let (mut pending_upstream, _) = tokio::time::timeout(WAIT, upstream.accept())
        .await
        .unwrap()
        .unwrap();
    read_headers(&mut pending_upstream).await;
    let mut extra = TcpStream::connect(addr).await.unwrap();
    let mut buf = [0; 1024];
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(200), extra.read(&mut buf))
            .await
            .expect("extra connection waited rather than refusing admission")
            .unwrap(),
        0
    );
    assert_eq!(
        tokio::time::timeout(WAIT, first.read(&mut buf))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    loop {
        let n = tokio::time::timeout(WAIT, pending_upstream.read(&mut buf))
            .await
            .expect("upstream survived server request deadline")
            .unwrap();
        if n == 0 {
            break;
        }
    }
    let response = send(addr, &request("/api/start", "", "invalid")).await;
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    stop.send(()).unwrap();
    server.await.unwrap();
}
