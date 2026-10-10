//! Setup callback panic must fail one session closed without leaking admission.
#![cfg(feature = "soniox")]
use pipecat_browser_client::{client_ready, protocol};
use pipecat_client_server::{server::BotApi, SessionMethod};
use pipecat_processors::tools::ToolExecutor;
use pipecat_runner::{NativeRunner, Options, RunnerConfig, SonioxSessionTools, SonioxVoiceConfig};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::net::TcpListener;

#[tokio::test]
async fn independent_factory_panic_reports_failure_frees_slot_and_allows_replacement() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let stt = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let llm = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SonioxVoiceConfig::new("independent-soniox", "independent-openai");
        config.stt.endpoint = format!("ws://{}/stt", stt.local_addr().unwrap());
        config.tts.endpoint = format!("ws://{}/tts", tts.local_addr().unwrap());
        config.llm.endpoint = format!("http://{}/llm", llm.local_addr().unwrap());
        config.stt.limits.close_timeout = Duration::from_millis(100);
        let calls = Arc::new(AtomicUsize::new(0));
        let created = calls.clone();
        config.session_tools = Some(Arc::new(move || {
            if created.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("independent factory panic");
            }
            Ok(SonioxSessionTools {
                definitions: vec![],
                executor: ToolExecutor::new(1, 4, 1024)?,
            })
        }));
        let runner = NativeRunner::new_soniox(
            RunnerConfig {
                allow_missing_origin: true,
                max_sessions: 1,
                options: Options {
                    introduce: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            config,
        )
        .unwrap();
        let mut reports = runner.subscribe();
        let first = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        let mut client =
            WebSocketSession::connect(first["wsUrl"].as_str().unwrap(), SessionConfig::default())
                .await
                .unwrap();
        assert!(client.recv().await.is_none());
        let _ = client.finish().await;
        let report = reports.recv().await.unwrap();
        assert_eq!(report.session_id, first["sessionId"].as_str().unwrap());
        assert!(!report.successful);
        assert_eq!(
            runner.active_sessions(),
            0,
            "panic leaked the only session slot"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), stt.accept())
                .await
                .is_err(),
            "factory failure opened STT"
        );
        let provider = tokio::spawn(async move {
            let mut session =
                WebSocketSession::accept(stt.accept().await.unwrap().0, SessionConfig::default())
                    .await
                    .unwrap();
            let Packet::Text(config) = session.recv().await.unwrap() else {
                panic!()
            };
            assert_eq!(
                serde_json::from_str::<Value>(&config).unwrap()["enable_endpoint_detection"],
                true
            );
            while session.recv().await.is_some() {}
            let _ = session.finish().await;
        });
        let second = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        let mut client =
            WebSocketSession::connect(second["wsUrl"].as_str().unwrap(), SessionConfig::default())
                .await
                .unwrap();
        client
            .sender()
            .send(Packet::Binary(
                protocol::encode_message(&client_ready(7)).into(),
            ))
            .await
            .unwrap();
        loop {
            let Packet::Binary(bytes) = client.recv().await.unwrap() else {
                panic!()
            };
            if let Some(protocol::frame::Body::Message(msg)) =
                protocol::decode(&bytes).unwrap().body
            {
                if serde_json::from_str::<Value>(&msg.data).unwrap()["type"] == "bot-ready" {
                    break;
                }
            }
        }
        runner
            .session(
                SessionMethod::Post,
                second["sessionId"].as_str().unwrap(),
                &["stop"],
                &json!({}),
            )
            .await
            .unwrap();
        while client.recv().await.is_some() {}
        let _ = client.finish().await;
        let report = reports.recv().await.unwrap();
        assert!(report.successful);
        assert_eq!(runner.active_sessions(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        runner.shutdown().await;
        provider.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), llm.accept())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), tts.accept())
                .await
                .is_err()
        );
    })
    .await
    .expect("factory panic abandoned session cleanup");
}
