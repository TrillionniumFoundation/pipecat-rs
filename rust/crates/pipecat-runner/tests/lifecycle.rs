use pipecat_client_server::{server::BotApi, ProxyError, SessionMethod};
use pipecat_runner::{NativeRunner, Options, ProviderConfig, RunnerConfig};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn providers(address: std::net::SocketAddr) -> ProviderConfig {
    ProviderConfig {
        deepgram_key: "synthetic-only".into(),
        openai_key: "synthetic-only".into(),
        cartesia_key: "synthetic-only".into(),
        deepgram_endpoint: format!("ws://{address}/stt"),
        openai_endpoint: format!("http://{address}/llm"),
        cartesia_endpoint: format!("ws://{address}/tts"),
        model: "synthetic".into(),
        voice_id: "synthetic".into(),
        system_prompt: "test".into(),
        input_rate: 16000,
        output_rate: 24000,
    }
}
fn address(response: &Value) -> std::net::SocketAddr {
    let url = url::Url::parse(response["wsUrl"].as_str().unwrap()).unwrap();
    format!("{}:{}", url.host_str().unwrap(), url.port().unwrap())
        .parse()
        .unwrap()
}

#[tokio::test]
async fn concurrent_shutdown_callers_both_wait_for_owned_session_tasks() {
    use std::{future::Future, task::Poll};

    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let runner = NativeRunner::new(
        RunnerConfig::default(),
        providers(provider.local_addr().unwrap()),
    )
    .unwrap();
    let response = runner
        .start(&json!({"transport":"websocket"}))
        .await
        .unwrap();
    let first = runner.shutdown();
    let second = runner.shutdown();
    tokio::pin!(first, second);
    // Poll both callers before the current-thread runtime can schedule cleanup.
    std::future::poll_fn(|cx| {
        assert!(first.as_mut().poll(cx).is_pending());
        assert!(
            second.as_mut().poll(cx).is_pending(),
            "second shutdown returned before the owned listener task exited"
        );
        Poll::Ready(())
    })
    .await;
    tokio::join!(first, second);
    assert_eq!(runner.active_sessions(), 0);
    assert!(TcpStream::connect(address(&response)).await.is_err());
}

#[tokio::test]
async fn cancelled_shutdown_waiter_does_not_abandon_owned_cleanup() {
    use std::{future::Future, task::Poll};

    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let runner = NativeRunner::new(
        RunnerConfig::default(),
        providers(provider.local_addr().unwrap()),
    )
    .unwrap();
    let mut reports = runner.subscribe();
    let response = runner
        .start(&json!({"transport":"websocket"}))
        .await
        .unwrap();
    {
        let first = runner.shutdown();
        tokio::pin!(first);
        std::future::poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    let second = runner.shutdown();
    tokio::pin!(second);
    std::future::poll_fn(|cx| {
        assert!(
            second.as_mut().poll(cx).is_pending(),
            "cancelled shutdown lost its cleanup owner"
        );
        Poll::Ready(())
    })
    .await;
    second.await;
    assert_eq!(runner.active_sessions(), 0);
    assert!(TcpStream::connect(address(&response)).await.is_err());
    let report = reports
        .try_recv()
        .expect("accepted session must publish completion");
    assert_eq!(report.session_id, response["sessionId"].as_str().unwrap());
}

#[tokio::test]
async fn admission_is_bounded_stop_is_session_scoped_and_shutdown_joins_listeners() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runner = NativeRunner::new(
            RunnerConfig {
                max_sessions: 2,
                allowed_origins: vec!["http://127.0.0.1:7860".into()],
                ..Default::default()
            },
            providers(provider.local_addr().unwrap()),
        )
        .unwrap();
        let mut reports = runner.subscribe();
        assert!(matches!(
            runner.start(&json!({"transport":"webrtc"})).await,
            Err(ProxyError::InvalidRequest)
        ));
        assert!(matches!(
            runner
                .start(&json!({"transport":"websocket","body":{"secret":"ignored"}}))
                .await,
            Err(ProxyError::InvalidRequest)
        ));
        let first = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        let second = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        assert_ne!(first["sessionId"], second["sessionId"]);
        assert_ne!(first["wsUrl"], second["wsUrl"]);
        assert_eq!(runner.active_sessions(), 2);
        assert!(matches!(
            runner.start(&json!({"transport":"websocket"})).await,
            Err(ProxyError::Busy)
        ));
        runner
            .session(
                SessionMethod::Post,
                first["sessionId"].as_str().unwrap(),
                &["stop"],
                &json!({}),
            )
            .await
            .unwrap();
        let report = reports.recv().await.unwrap();
        assert_eq!(report.session_id, first["sessionId"].as_str().unwrap());
        assert_eq!(runner.active_sessions(), 1);
        assert!(TcpStream::connect(address(&first)).await.is_err());
        // The other session still owns a listening socket until host shutdown.
        let alive = TcpStream::connect(address(&second)).await.unwrap();
        drop(alive);
        runner.shutdown().await;
        assert_eq!(runner.active_sessions(), 0);
        assert!(TcpStream::connect(address(&second)).await.is_err());
        assert!(matches!(
            runner.start(&json!({"transport":"websocket"})).await,
            Err(ProxyError::Closed)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), provider.accept())
                .await
                .is_err()
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn wrong_origin_and_wrong_token_never_open_provider_connections() {
    tokio::time::timeout(Duration::from_secs(5),async {
        let provider=TcpListener::bind("127.0.0.1:0").await.unwrap();let allowed="http://127.0.0.1:7860";
        let runner=NativeRunner::new(RunnerConfig{allowed_origins:vec![allowed.into()],options:Options{introduce:false,..Default::default()},..Default::default()},providers(provider.local_addr().unwrap())).unwrap();
        let response=runner.start(&json!({"transport":"websocket"})).await.unwrap();let url=url::Url::parse(response["wsUrl"].as_str().unwrap()).unwrap();
        for (origin,path) in [("https://untrusted.example",url.path()),(allowed,"/ws-client/wrong"),("",url.path())] {
            let mut socket=TcpStream::connect(address(&response)).await.unwrap();let header=if origin.is_empty(){String::new()}else{format!("Origin: {origin}\r\n")};
            socket.write_all(format!("GET {path} HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n{header}\r\n",address(&response)).as_bytes()).await.unwrap();let mut bytes=Vec::new();socket.read_to_end(&mut bytes).await.unwrap();assert!(String::from_utf8_lossy(&bytes).starts_with("HTTP/1.1 403"));
        }
        assert_eq!(runner.active_sessions(),1);assert!(tokio::time::timeout(Duration::from_millis(20),provider.accept()).await.is_err());runner.shutdown().await;
    }).await.unwrap();
}

#[tokio::test]
async fn unused_start_expires_without_leaking_a_listener_or_session_slot() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runner = NativeRunner::new(
            RunnerConfig {
                connect_timeout: Duration::from_millis(30),
                max_sessions: 1,
                ..Default::default()
            },
            providers(provider.local_addr().unwrap()),
        )
        .unwrap();
        let mut reports = runner.subscribe();
        let first = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        let report = reports.recv().await.unwrap();
        assert!(!report.successful);
        assert_eq!(runner.active_sessions(), 0);
        assert!(TcpStream::connect(address(&first)).await.is_err());
        let second = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        assert_ne!(first["sessionId"], second["sessionId"]);
        runner.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn stop_cancels_a_handshaken_pipeline_and_closes_its_socket() {
    use pipecat_browser_client::{client_ready, protocol};
    use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
    tokio::time::timeout(Duration::from_secs(5), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_address = provider.local_addr().unwrap();
        let provider_task = tokio::spawn(async move {
            let (stream, _) = provider.accept().await.unwrap();
            let mut session = WebSocketSession::accept(stream, SessionConfig::default())
                .await
                .unwrap();
            while session.recv().await.is_some() {}
            let _ = session.finish().await;
        });
        let runner = NativeRunner::new(
            RunnerConfig {
                allow_missing_origin: true,
                options: Options {
                    introduce: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            providers(provider_address),
        )
        .unwrap();
        let mut reports = runner.subscribe();
        let response = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        let mut client = WebSocketSession::connect(
            response["wsUrl"].as_str().unwrap(),
            SessionConfig::default(),
        )
        .await
        .unwrap();
        client
            .sender()
            .send(Packet::Binary(
                protocol::encode_message(&client_ready(1)).into(),
            ))
            .await
            .unwrap();
        let Packet::Binary(bytes) = client.recv().await.unwrap() else {
            panic!()
        };
        let protocol::frame::Body::Message(message) =
            protocol::decode(&bytes).unwrap().body.unwrap()
        else {
            panic!()
        };
        assert_eq!(
            serde_json::from_str::<Value>(&message.data).unwrap()["type"],
            "bot-ready"
        );
        runner
            .session(
                SessionMethod::Post,
                response["sessionId"].as_str().unwrap(),
                &["stop"],
                &json!({}),
            )
            .await
            .unwrap();
        while client.recv().await.is_some() {}
        let _ = client.finish().await;
        let report = reports.recv().await.unwrap();
        assert!(report.successful);
        assert!(report.generated_context.messages.is_empty());
        assert_eq!(runner.active_sessions(), 0);
        runner.shutdown().await;
        provider_task.await.unwrap();
    })
    .await
    .unwrap();
}
