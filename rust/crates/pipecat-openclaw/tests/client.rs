mod common;
use common::*;
use pipecat_openclaw::*;
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn exact_handshake_verbatim_send_early_server_alias_and_terminal_once() {
    let (config, peer) = server(|mut peer| async move {
        assert!(
            tokio::time::timeout(Duration::from_millis(25), recv(&mut peer))
                .await
                .is_err()
        );
        let hello = challenge(&mut peer).await;
        assert_eq!(hello["params"]["minProtocol"], 4);
        assert_eq!(hello["params"]["maxProtocol"], 4);
        assert_eq!(hello["params"]["client"]["id"], "gateway-client");
        assert_eq!(hello["params"]["client"]["mode"], "backend");
        assert_eq!(hello["params"]["role"], "operator");
        assert_eq!(hello["params"]["scopes"], json!(["operator.write"]));
        assert_eq!(
            hello["params"]["auth"],
            json!({"token":"test-secret-token","password":"test-secret-password"})
        );
        let req = recv(&mut peer).await;
        assert_eq!(req["method"], "chat.send");
        assert_eq!(req["params"]["message"], "  raw\ntext  ");
        assert_eq!(req["params"]["sessionKey"], "session-special");
        assert_ne!(req["id"], req["params"]["idempotencyKey"]);
        chat(&mut peer, "server-run", "delta", "one").await;
        chat(&mut peer, "server-run", "final", "whole").await;
        response(&mut peer, &req, json!({"runId":"server-run"})).await;
        chat(&mut peer, "server-run", "delta", "late").await;
        chat(&mut peer, "server-run", "final", "duplicate").await;
        let abort = recv(&mut peer).await;
        assert_eq!(abort["method"], "chat.abort");
        assert_eq!(
            abort["params"],
            json!({"sessionKey":"session-special","runId":"server-run"})
        );
        response(&mut peer, &abort, json!({"aborted":false})).await;
        wait_close(&mut peer).await;
    })
    .await;
    let client = GatewayClient::connect(config).await.unwrap();
    let mut run = client
        .start("  raw\ntext  ", Some("session-special"))
        .await
        .unwrap();
    assert_eq!(run.identity().run_id, "server-run");
    assert_eq!(
        event(&mut run).await,
        RunEvent::Text {
            run_id: "server-run".into(),
            text: "one".into()
        }
    );
    let terminal = end(&mut run).await;
    assert_eq!(terminal.text, "whole");
    assert_eq!(terminal.status, RunStatus::Completed);
    assert_eq!(terminal.remote_outcome, RemoteOutcome::Settled);
    assert!(run.next().await.is_none());
    assert!(!client.abort(&run.handle()).await.unwrap());
    client.disconnect().await;
    peer.await.unwrap();
}
#[tokio::test]
async fn successful_steer_follows_replacement_before_old_abort() {
    let (config, peer) = server(|mut peer| async move {
        challenge(&mut peer).await;
        let start = recv(&mut peer).await;
        response(&mut peer, &start, json!({"runId":"old"})).await;
        let steer = recv(&mut peer).await;
        assert_eq!(steer["method"], "sessions.steer");
        assert_eq!(steer["params"]["key"], "specific");
        assert_eq!(steer["params"]["message"], "follow up");
        chat(&mut peer, "old", "aborted", "").await;
        chat(&mut peer, "new", "delta", "replacement").await;
        response(
            &mut peer,
            &steer,
            json!({"status":"started","interruptedActiveRun":true,"runId":"new"}),
        )
        .await;
        chat(&mut peer, "new", "final", "").await;
        wait_close(&mut peer).await;
    })
    .await;
    let client = GatewayClient::connect(config).await.unwrap();
    let mut run = client.start("start", Some("specific")).await.unwrap();
    client.steer(&run.handle(), "follow up").await.unwrap();
    assert_eq!(
        event(&mut run).await,
        RunEvent::Text {
            run_id: "new".into(),
            text: "replacement".into()
        }
    );
    let result = end(&mut run).await;
    assert_eq!(result.status, RunStatus::Completed);
    assert!(result.text.is_empty());
    assert_eq!(result.run_id, "new");
    client.disconnect().await;
    peer.await.unwrap();
}
#[tokio::test]
async fn explicit_steer_rejection_restores_old_events() {
    let (config,peer)=server(|mut peer|async move{
        challenge(&mut peer).await;let start=recv(&mut peer).await;response(&mut peer,&start,json!({"runId":"old"})).await;
        let steer=recv(&mut peer).await;chat(&mut peer,"old","delta","old text").await;chat(&mut peer,"old","final","old final").await;
        send(&mut peer,json!({"type":"res","id":steer["id"],"ok":false,"error":{"message":"do not leak server-secret"}})).await;wait_close(&mut peer).await;
    }).await;
    let client = GatewayClient::connect(config).await.unwrap();
    let mut run = client.start("start", None).await.unwrap();
    let error = client.steer(&run.handle(), "follow up").await.unwrap_err();
    assert_eq!(error, GatewayError::Rejected("sessions.steer"));
    assert!(!error.to_string().contains("server-secret"));
    assert_eq!(run.identity().run_id, "old");
    assert_eq!(
        event(&mut run).await,
        RunEvent::Text {
            run_id: "old".into(),
            text: "old text".into()
        }
    );
    assert_eq!(end(&mut run).await.text, "old final");
    client.disconnect().await;
    peer.await.unwrap();
}
#[tokio::test]
async fn withheld_steer_response_is_unknown_and_never_restores_or_retries() {
    let (config, peer) = server(|mut peer| async move {
        challenge(&mut peer).await;
        let start = recv(&mut peer).await;
        response(&mut peer, &start, json!({"runId":"old"})).await;
        let steer = recv(&mut peer).await;
        assert_eq!(steer["method"], "sessions.steer");
        chat(&mut peer, "old", "aborted", "").await;
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), recv(&mut peer))
                .await
                .is_err()
        );
        wait_close(&mut peer).await;
    })
    .await;
    let client = GatewayClient::connect(config).await.unwrap();
    let mut run = client.start("start", None).await.unwrap();
    assert_eq!(
        client.steer(&run.handle(), "follow up").await.unwrap_err(),
        GatewayError::OutcomeUnknown("sessions.steer")
    );
    assert_ne!(run.identity().run_id, "old");
    let result = end(&mut run).await;
    assert_eq!(result.status, RunStatus::Failed);
    assert_eq!(result.remote_outcome, RemoteOutcome::Unknown);
    tokio::time::sleep(Duration::from_millis(220)).await;
    client.disconnect().await;
    peer.await.unwrap();
}
#[tokio::test]
async fn cumulative_delta_and_wrong_session_are_not_emitted() {
    let (config,peer)=server(|mut peer|async move{
        challenge(&mut peer).await;let req=recv(&mut peer).await;response(&mut peer,&req,json!({"runId":"run"})).await;
        send(&mut peer,json!({"type":"event","event":"chat","payload":{"runId":"run","state":"delta","message":"cumulative"}})).await;
        send(&mut peer,json!({"type":"event","event":"chat","payload":{"runId":"run","sessionKey":"wrong","state":"final","message":"wrong final"}})).await;
        chat(&mut peer,"run","final","right").await;wait_close(&mut peer).await;
    }).await;
    let client = GatewayClient::connect(config).await.unwrap();
    let mut run = client.start("start", None).await.unwrap();
    assert_eq!(end(&mut run).await.text, "right");
    client.disconnect().await;
    peer.await.unwrap();
}
#[tokio::test]
async fn saturated_output_has_a_terminal_and_abort_still_roundtrips() {
    let (mut config, peer) = server(|mut peer| async move {
        challenge(&mut peer).await;
        let req = recv(&mut peer).await;
        response(&mut peer, &req, json!({"runId":"run"})).await;
        for _ in 0..8 {
            chat(&mut peer, "run", "delta", "a").await;
        }
        let abort = recv(&mut peer).await;
        assert_eq!(abort["method"], "chat.abort");
        response(&mut peer, &abort, json!({"aborted":true})).await;
        wait_close(&mut peer).await;
    })
    .await;
    config.limits.max_output_events = 1;
    let client = GatewayClient::connect(config).await.unwrap();
    let mut run = client.start("start", None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(client.abort(&run.handle()).await.unwrap());
    let terminal = end(&mut run).await;
    assert_eq!(terminal.status, RunStatus::Failed);
    assert_eq!(terminal.remote_outcome, RemoteOutcome::Unknown);
    client.disconnect().await;
    peer.await.unwrap();
}
#[tokio::test]
async fn local_run_deadline_terminates_silent_run() {
    let (mut config, peer) = server(|mut peer| async move {
        challenge(&mut peer).await;
        let req = recv(&mut peer).await;
        response(&mut peer, &req, json!({"runId":"silent"})).await;
        wait_close(&mut peer).await;
    })
    .await;
    config.limits.run_timeout = Duration::from_millis(60);
    let client = GatewayClient::connect(config).await.unwrap();
    let mut run = client.start("start", None).await.unwrap();
    let result = end(&mut run).await;
    assert_eq!(result.status, RunStatus::Failed);
    assert_eq!(result.remote_outcome, RemoteOutcome::Unknown);
    client.disconnect().await;
    peer.await.unwrap();
}
#[tokio::test]
async fn disconnected_reader_and_dropped_client_settle_live_runs() {
    for drop_owner in [false, true] {
        let (config, peer) = server(move |mut peer| async move {
            challenge(&mut peer).await;
            let req = recv(&mut peer).await;
            response(&mut peer, &req, json!({"runId":"run"})).await;
            if drop_owner {
                wait_close(&mut peer).await;
            }
        })
        .await;
        let client = GatewayClient::connect(config).await.unwrap();
        let mut run = client.start("start", None).await.unwrap();
        drop(client);
        let result = end(&mut run).await;
        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.remote_outcome, RemoteOutcome::Unknown);
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn handshake_rejects_bad_hello_and_times_out_missing_challenge() {
    let (config, peer) = server(|mut peer| async move {
        send(
            &mut peer,
            json!({"type":"event","event":"connect.challenge"}),
        )
        .await;
        let req = recv(&mut peer).await;
        response(&mut peer, &req, json!({"type":"hello-ok","protocol":3})).await;
    })
    .await;
    assert_eq!(
        GatewayClient::connect(config).await.unwrap_err(),
        GatewayError::Protocol("hello protocol")
    );
    peer.await.unwrap();
    let (mut config, peer) = server(|mut peer| async move {
        wait_close(&mut peer).await;
    })
    .await;
    config.limits.connect_timeout = Duration::from_millis(40);
    assert!(matches!(
        GatewayClient::connect(config).await,
        Err(GatewayError::Timeout(_))
    ));
    peer.await.unwrap();
}
#[tokio::test]
async fn malformed_terminal_is_failed_not_completed() {
    let (config,peer)=server(|mut peer|async move{challenge(&mut peer).await;let req=recv(&mut peer).await;response(&mut peer,&req,json!({"runId":"run"})).await;send(&mut peer,json!({"type":"event","event":"chat","payload":{"runId":"run","state":"final","message":17}})).await;wait_close(&mut peer).await;}).await;
    let client = GatewayClient::connect(config).await.unwrap();
    let mut run = client.start("start", None).await.unwrap();
    assert_eq!(end(&mut run).await.status, RunStatus::Failed);
    client.disconnect().await;
    peer.await.unwrap();
}
#[tokio::test]
async fn unknown_frame_and_byte_bounds_fail_without_hanging() {
    for byte_limit in [false, true] {
        let (mut config, peer) = server(|mut peer| async move {
            challenge(&mut peer).await;
            let _req = recv(&mut peer).await;
            for _ in 0..3 {
                chat(&mut peer, "unrouted", "delta", "data").await;
            }
            wait_close(&mut peer).await;
        })
        .await;
        if byte_limit {
            config.limits.max_unrouted_bytes = 1;
        } else {
            config.limits.max_unrouted_frames = 1;
        }
        let client = GatewayClient::connect(config).await.unwrap();
        assert!(matches!(
            client.start("start", None).await,
            Err(GatewayError::OutcomeUnknown("chat.send"))
        ));
        client.disconnect().await;
        peer.await.unwrap();
    }
}
#[test]
fn url_policy_and_own_diagnostics_redact_secrets() {
    let config = GatewayConfig {
        token: Some("secret-t".into()),
        password: Some("secret-p".into()),
        ..Default::default()
    };
    assert!(config.validate().is_err());
    let mut config = GatewayConfig {
        allow_loopback_cleartext: true,
        ..config
    };
    assert!(config.validate().is_ok());
    let diagnostic = format!("{config:?}");
    assert!(!diagnostic.contains("secret-t"));
    assert!(!diagnostic.contains("127.0.0.1"));
    for url in [
        "ws://localhost:18789",
        "ws://example.com",
        "wss://name:pass@example.com",
        "wss://example.com?token=secret",
    ] {
        config.url = url.into();
        assert!(config.validate().is_err());
    }
    config.url = "wss://example.com/gateway".into();
    assert!(config.validate().is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_event_flood_cannot_starve_normal_admission_or_response() {
    use futures_util::{SinkExt, StreamExt};
    let (config,peer)=server(|mut peer|async move{
        challenge(&mut peer).await;
        let (mut sink,mut source)=peer.split();
        let flood=tokio::spawn(async move{
            let noise=json!({"type":"event","event":"unrelated"}).to_string();
            for _ in 0..1000 {if sink.send(tokio_tungstenite::tungstenite::Message::Text(noise.clone().into())).await.is_err(){return;}}
            let req:serde_json::Value=serde_json::from_str(source.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
            assert_eq!(req["method"],"chat.send");
            sink.send(tokio_tungstenite::tungstenite::Message::Text(json!({"type":"res","id":req["id"],"ok":true,"payload":{"runId":"run"}}).to_string().into())).await.unwrap();
            sink.send(tokio_tungstenite::tungstenite::Message::Text(json!({"type":"event","event":"chat","payload":{"runId":"run","state":"final","message":"finished"}}).to_string().into())).await.unwrap();
            while let Some(Ok(_))=source.next().await{}
        });
        flood.await.unwrap();
    }).await;
    let client = GatewayClient::connect(config).await.unwrap();
    let mut run = tokio::time::timeout(Duration::from_secs(1), client.start("start", None))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(end(&mut run).await.text, "finished");
    client.disconnect().await;
    peer.await.unwrap();
}
