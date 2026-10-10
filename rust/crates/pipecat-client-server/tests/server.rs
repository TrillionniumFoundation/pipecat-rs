// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use pipecat_client_server::{
    server::{serve, AssetBundle, ServerConfig},
    ClientProxy, ProxyLimits,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
async fn launch(
    upstream: &str,
    mut config: ServerConfig,
) -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    config.allowed_origins.push(origin.clone());
    let proxy = ClientProxy::new(
        upstream,
        None,
        ProxyLimits {
            request_bytes: 64,
            ..Default::default()
        },
    )
    .unwrap();
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        serve(listener, proxy, config, async {
            let _ = rx.await;
        })
        .await
        .unwrap();
    });
    (origin, tx, task)
}
#[tokio::test]
async fn native_route_forwards_only_expected_method_and_origin() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}/start", upstream.local_addr().unwrap());
    let upstream_task = tokio::spawn(async move {
        let (mut socket, _) = upstream.accept().await.unwrap();
        let mut bytes = [0; 4096];
        let n = socket.read(&mut bytes).await.unwrap();
        let request = String::from_utf8_lossy(&bytes[..n]);
        assert!(request.starts_with("POST /start HTTP/1.1"));
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
            )
            .await
            .unwrap();
    });
    let (origin, stop, server) = launch(&upstream_url, ServerConfig::default()).await;
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .post(format!("{origin}/api/start"))
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .post(format!("{origin}/api/start"))
            .header("Origin", "https://evil.example")
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .get(format!("{origin}/api/start"))
            .header("Origin", &origin)
            .send()
            .await
            .unwrap()
            .status(),
        405
    );
    assert_eq!(
        client
            .post(format!("{origin}/api/start"))
            .header("Origin", &origin)
            .body("{}")
            .send()
            .await
            .unwrap()
            .status(),
        415
    );
    assert_eq!(
        client
            .post(format!("{origin}/api/start"))
            .header("Origin", &origin)
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    assert_eq!(
        client
            .post(format!("{origin}/api/start"))
            .header("Origin", &origin)
            .json(&serde_json::json!({"x":"a".repeat(128)}))
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    let response = client
        .post(format!("{origin}/api/start"))
        .header("Origin", &origin)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap(),
        serde_json::json!({"ok":true})
    );
    upstream_task.await.unwrap();
    stop.send(()).unwrap();
    server.await.unwrap();
}
#[tokio::test]
async fn shutdown_cancels_slow_request_body_and_releases_listener() {
    let (origin, stop, server) = launch("http://127.0.0.1:1/start", ServerConfig::default()).await;
    let address = origin.strip_prefix("http://").unwrap();
    let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
    socket.write_all(format!("POST /api/start HTTP/1.1\r\nHost: {address}\r\nOrigin: {origin}\r\nContent-Type: application/json\r\nContent-Length: 50\r\n\r\n{{").as_bytes()).await.unwrap();
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    let rebound = TcpListener::bind(address).await.unwrap();
    drop(rebound);
}
#[tokio::test]
async fn server_refuses_external_listener() {
    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let proxy = ClientProxy::new("http://127.0.0.1:1/start", None, ProxyLimits::default()).unwrap();
    assert!(serve(listener, proxy, ServerConfig::default(), async {})
        .await
        .is_err());
}
#[tokio::test]
async fn fixed_assets_have_correct_mime_without_filesystem_routes() {
    let root = std::env::temp_dir().join(format!("pipecat-assets-{}", std::process::id()));
    std::fs::create_dir_all(root.join("pkg")).unwrap();
    for name in [
        "index.html",
        "style.css",
        "config.json",
        "pkg/pipecat_browser_client.js",
        "pkg/pipecat_browser_client_bg.wasm",
    ] {
        std::fs::write(root.join(name), format!("asset:{name}")).unwrap();
    }
    std::fs::write(root.join("secret.txt"), "not public").unwrap();
    let assets = AssetBundle::from_directory(&root).unwrap();
    assert!(assets
        .clone()
        .with_browser_config(serde_json::json!({"x":"x".repeat(16384)}))
        .is_err());
    assert!(assets
        .clone()
        .with_browser_config(serde_json::Value::Null)
        .is_err());
    let assets = assets.with_browser_config(serde_json::json!({"project_name":"Native","start_endpoint":"/api/start","transports":["websocket"]})).unwrap();
    std::fs::remove_dir_all(&root).unwrap(); // Served bytes must already be owned.
    let (origin, stop, server) = launch(
        "http://127.0.0.1:1/start",
        ServerConfig {
            assets: Some(Arc::new(assets)),
            ..Default::default()
        },
    )
    .await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{origin}/pkg/pipecat_browser_client_bg.wasm"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "application/wasm");
    let config = client
        .get(format!("{origin}/config.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(config.headers()["cache-control"], "no-store");
    assert_eq!(
        config.json::<serde_json::Value>().await.unwrap()["transports"],
        serde_json::json!(["websocket"])
    );
    assert!(response.text().await.unwrap().contains("asset:"));
    assert_eq!(
        client
            .get(format!("{origin}/secret.txt"))
            .header("Origin", &origin)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    stop.send(()).unwrap();
    server.await.unwrap();
}

struct NativeBot(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl pipecat_client_server::server::BotApi for NativeBot {
    fn max_request_bytes(&self) -> usize {
        64
    }
    async fn start(
        &self,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, pipecat_client_server::ProxyError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(body.clone())
    }
    async fn session(
        &self,
        _: pipecat_client_server::SessionMethod,
        id: &str,
        path: &[&str],
        _: &serde_json::Value,
    ) -> Result<serde_json::Value, pipecat_client_server::ProxyError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(serde_json::json!({"id":id,"path":path}))
    }
}
#[tokio::test]
async fn native_handler_uses_same_bounds_and_path_validation_as_proxy() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let bot = Arc::new(NativeBot(std::sync::atomic::AtomicUsize::new(0)));
    let api = bot.clone();
    let (stop, rx) = oneshot::channel();
    let config = ServerConfig {
        allowed_origins: vec![origin.clone()],
        ..Default::default()
    };
    let server = tokio::spawn(async move {
        pipecat_client_server::server::serve_with_api(listener, api, config, async {
            let _ = rx.await;
        })
        .await
        .unwrap();
    });
    let client = reqwest::Client::new();
    for path in [
        "/api/sessions/id/%252e%252e",
        "/api/sessions/id/api%2foffer",
    ] {
        assert_eq!(
            client
                .post(format!("{origin}{path}"))
                .header("Origin", &origin)
                .json(&serde_json::json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    assert_eq!(bot.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    let response = client
        .patch(format!("{origin}/api/sessions/id/api/offer"))
        .header("Origin", &origin)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<serde_json::Value>().await.unwrap()["path"],
        serde_json::json!(["api", "offer"])
    );
    assert_eq!(bot.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    stop.send(()).unwrap();
    server.await.unwrap();
}
