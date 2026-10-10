//! Independent real WebSocket regressions for identity, replay, and termination.
#[path = "peer.rs"]
mod peer;
use peer::*;
use pipecat_openclaw::*;
use serde_json::json;
use std::time::Duration;
use tokio::sync::oneshot;

fn config(url: String) -> GatewayConfig {
    GatewayConfig {
        url,
        allow_loopback_cleartext: true,
        session_key: "review-session".into(),
        token: Some("review-secret-token".into()),
        password: Some("review-secret-password".into()),
        limits: GatewayLimits {
            connect_timeout: Duration::from_millis(600),
            request_timeout: Duration::from_millis(200),
            write_timeout: Duration::from_millis(150),
            close_timeout: Duration::from_millis(100),
            run_timeout: Duration::from_millis(600),
            ..Default::default()
        },
        ..Default::default()
    }
}
async fn next(run: &mut GatewayRun) -> RunEvent {
    tokio::time::timeout(DEADLINE, run.next())
        .await
        .expect("run event deadline")
        .expect("run unexpectedly ended")
}
async fn result(run: &mut GatewayRun) -> RunResult {
    loop {
        if let RunEvent::End(r) = next(run).await {
            return r;
        }
    }
}
async fn close(mut ws: Socket) {
    while let Some(message) = futures_util::StreamExt::next(&mut ws).await {
        match message {
            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) | Err(_) => break,
            _ => (),
        }
    }
}

#[tokio::test]
async fn review_early_terminal_adopts_identity_without_deadlock_or_duplicate_terminal() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let req = request(&mut ws).await;
        assert_eq!(req["method"], "chat.send");
        send(&mut ws, chat("server-selected", "final", "first terminal")).await;
        send(&mut ws, chat("server-selected", "delta", "late delta")).await;
        send(
            &mut ws,
            chat("server-selected", "final", "duplicate terminal"),
        )
        .await;
        ok(&mut ws, &req, json!({"runId":"server-selected"})).await;
        close(ws).await;
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    let mut run = tokio::time::timeout(DEADLINE, client.start("verbatim", None))
        .await
        .expect("early terminal deadlocked acceptance")
        .unwrap();
    let first = next(&mut run).await;
    let RunEvent::End(end) = first else {
        panic!("late delta escaped terminal fence")
    };
    assert_eq!(end.run_id, "server-selected");
    assert_eq!(end.status, RunStatus::Completed);
    assert_eq!(end.text, "first terminal");
    assert!(run.next().await.is_none());
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_request_id_is_not_a_run_alias() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let req = request(&mut ws).await;
        send(
            &mut ws,
            chat(
                req["id"].as_str().unwrap(),
                "final",
                "request id contamination",
            ),
        )
        .await;
        ok(&mut ws, &req, json!({"runId":"real-run"})).await;
        send(&mut ws, chat("real-run", "final", "real answer")).await;
        close(ws).await;
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    let mut run = client.start("hello", None).await.unwrap();
    let end = result(&mut run).await;
    assert_eq!(end.status, RunStatus::Completed);
    assert_eq!(end.text, "real answer");
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_explicit_wrong_session_cannot_complete_matching_run() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let req = request(&mut ws).await;
        ok(&mut ws, &req, json!({"runId":"same-run"})).await;
        let mut wrong = chat("same-run", "final", "wrong session secret");
        wrong["payload"]["sessionKey"] = "other-session".into();
        send(&mut ws, wrong).await;
        let mut right = chat("same-run", "final", "correct session answer");
        right["payload"]["sessionKey"] = "review-session".into();
        send(&mut ws, right).await;
        close(ws).await;
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    let mut run = client.start("hello", None).await.unwrap();
    let end = result(&mut run).await;
    assert_eq!(end.status, RunStatus::Completed);
    assert_eq!(end.text, "correct session answer");
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_rejected_steer_restores_and_replays_old_terminal() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let req = request(&mut ws).await;
        ok(&mut ws, &req, json!({"runId":"old-run"})).await;
        let steer = request(&mut ws).await;
        assert_eq!(steer["method"], "sessions.steer");
        send(
            &mut ws,
            chat("old-run", "final", "old terminal during negotiation"),
        )
        .await;
        reject(&mut ws, &steer).await;
        close(ws).await;
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    let mut run = client.start("first", None).await.unwrap();
    let error = client
        .steer(&run.handle(), "replacement")
        .await
        .unwrap_err();
    assert!(matches!(error, GatewayError::Rejected(_)));
    assert!(!format!("{error:?} {error}").contains("private-rejected-content"));
    assert_eq!(run.identity().run_id, "old-run");
    let end = result(&mut run).await;
    assert_eq!(end.status, RunStatus::Completed);
    assert_eq!(end.text, "old terminal during negotiation");
    assert!(run.next().await.is_none());
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_steer_post_write_disconnect_does_not_restore_old_identity() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let req = request(&mut ws).await;
        ok(&mut ws, &req, json!({"runId":"old-run"})).await;
        let steer = request(&mut ws).await;
        assert_eq!(steer["method"], "sessions.steer");
        send(&mut ws, chat("old-run", "aborted", "old aborted")).await;
        futures_util::SinkExt::close(&mut ws).await.unwrap();
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    let mut run = client.start("first", None).await.unwrap();
    let error = client
        .steer(&run.handle(), "replacement")
        .await
        .unwrap_err();
    assert!(
        matches!(error, GatewayError::OutcomeUnknown(_)),
        "{error:?}"
    );
    assert_ne!(run.identity().run_id, "old-run");
    let end = result(&mut run).await;
    assert_eq!(end.status, RunStatus::Failed);
    assert_eq!(end.remote_outcome, RemoteOutcome::Unknown);
    assert!(run.next().await.is_none());
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_successful_steer_ignores_old_abort_and_keeps_early_new_delta() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let req = request(&mut ws).await;
        ok(&mut ws, &req, json!({"runId":"old-run"})).await;
        let steer = request(&mut ws).await;
        send(&mut ws, chat("old-run", "aborted", "superseded")).await;
        send(&mut ws, chat("replacement-server-id", "delta", "new delta")).await;
        ok(
            &mut ws,
            &steer,
            json!({"status":"started","interruptedActiveRun":true,"runId":"replacement-server-id"}),
        )
        .await;
        send(&mut ws, chat("replacement-server-id", "final", "new final")).await;
        close(ws).await;
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    let mut run = client.start("first", None).await.unwrap();
    client.steer(&run.handle(), "replacement").await.unwrap();
    assert!(
        matches!(next(&mut run).await,RunEvent::Text {run_id,text} if run_id=="replacement-server-id" && text=="new delta")
    );
    let end = result(&mut run).await;
    assert_eq!(end.status, RunStatus::Completed);
    assert_eq!(end.run_id, "replacement-server-id");
    assert_eq!(end.text, "new final");
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_server_alias_collision_fails_without_stealing_first_run() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let first = request(&mut ws).await;
        ok(&mut ws, &first, json!({"runId":"collision"})).await;
        let second = request(&mut ws).await;
        ok(&mut ws, &second, json!({"runId":"collision"})).await;
        close(ws).await;
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    let mut first = client.start("first", None).await.unwrap();
    let second = client.start("second", Some("other-session")).await;
    assert!(matches!(second, Err(GatewayError::OutcomeUnknown(_))));
    let end = result(&mut first).await;
    assert_eq!(end.status, RunStatus::Failed);
    assert_eq!(end.remote_outcome, RemoteOutcome::Unknown);
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_cumulative_delta_is_silent_and_absent_final_text_stays_empty() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let req = request(&mut ws).await;
        ok(&mut ws, &req, json!({"runId":"run"})).await;
        let mut cumulative = chat("run", "delta", "cumulative only");
        cumulative["payload"]
            .as_object_mut()
            .unwrap()
            .remove("deltaText");
        send(&mut ws, cumulative).await;
        send(&mut ws, chat("run", "delta", "real delta")).await;
        send(
            &mut ws,
            json!({"type":"event","event":"chat","payload":{"runId":"run","state":"final"}}),
        )
        .await;
        close(ws).await;
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    let mut run = client.start("hello", None).await.unwrap();
    assert!(matches!(next(&mut run).await,RunEvent::Text{text,..} if text=="real delta"));
    let end = result(&mut run).await;
    assert_eq!(end.status, RunStatus::Completed);
    assert_eq!(end.text, "");
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_malformed_terminal_cannot_become_success() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let req = request(&mut ws).await;
        ok(&mut ws, &req, json!({"runId":"run"})).await;
        send(&mut ws,json!({"type":"event","event":"chat","payload":{"runId":"run","state":"final","message":false}})).await;
        close(ws).await;
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    let mut run = client.start("hello", None).await.unwrap();
    let end = result(&mut run).await;
    assert_eq!(end.status, RunStatus::Failed);
    assert_eq!(end.remote_outcome, RemoteOutcome::Unknown);
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_full_output_still_allows_abort_and_one_terminal() {
    let (listener, url) = listener().await;
    let (sent, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let req = request(&mut ws).await;
        ok(&mut ws, &req, json!({"runId":"run"})).await;
        send(&mut ws, chat("run", "delta", "one")).await;
        send(&mut ws, chat("run", "delta", "two")).await;
        sent.send(()).unwrap();
        let abort = request(&mut ws).await;
        assert_eq!(abort["method"], "chat.abort");
        assert_eq!(abort["params"]["runId"], "run");
        ok(&mut ws, &abort, json!({"aborted":false})).await;
        close(ws).await;
    });
    let mut cfg = config(url);
    cfg.limits.max_output_events = 1;
    let client = GatewayClient::connect(cfg).await.unwrap();
    let mut run = client.start("hello", None).await.unwrap();
    receive(received).await;
    assert!(!client.abort(&run.handle()).await.unwrap());
    assert!(matches!(next(&mut run).await,RunEvent::Text{text,..} if text=="one"));
    let end = result(&mut run).await;
    assert_eq!(end.status, RunStatus::Failed);
    assert_eq!(end.remote_outcome, RemoteOutcome::Unknown);
    assert!(run.next().await.is_none());
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_local_run_deadline_and_last_owner_drop_settle_without_remote_terminal() {
    for drop_owner in [false, true] {
        let (listener, url) = listener().await;
        let server = tokio::spawn(async move {
            let mut ws = hello(listener).await;
            let req = request(&mut ws).await;
            ok(&mut ws, &req, json!({"runId":"run"})).await;
            close(ws).await;
        });
        let mut cfg = config(url);
        cfg.limits.run_timeout = Duration::from_millis(70);
        let client = GatewayClient::connect(cfg).await.unwrap();
        let mut run = client.start("hello", None).await.unwrap();
        if drop_owner {
            drop(client);
        } else {
            let end = result(&mut run).await;
            assert_eq!(end.status, RunStatus::Failed);
            assert_eq!(end.remote_outcome, RemoteOutcome::Unknown);
            assert!(end.text.contains("deadline"));
            client.disconnect().await;
        }
        if drop_owner {
            let end = result(&mut run).await;
            assert_eq!(end.status, RunStatus::Failed);
            assert_eq!(end.remote_outcome, RemoteOutcome::Unknown);
        }
        assert!(run.next().await.is_none());
        done(server).await;
    }
}

#[tokio::test]
async fn review_credentials_follow_challenge_and_exact_correlated_hello_is_required() {
    for defect in ["none", "wrong-id", "wrong-protocol", "rejected"] {
        let (listener, url) = listener().await;
        let server = tokio::spawn(async move {
            let mut ws = accept(listener).await;
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(40),
                    futures_util::StreamExt::next(&mut ws)
                )
                .await
                .is_err(),
                "credentials sent before challenge"
            );
            send(&mut ws, json!({"type":"event","event":"connect.challenge"})).await;
            let req = request(&mut ws).await;
            let p = &req["params"];
            assert_eq!(p["client"]["id"], "gateway-client");
            assert_eq!(p["client"]["mode"], "backend");
            assert_eq!(p["role"], "operator");
            assert_eq!(p["scopes"], json!(["operator.write"]));
            assert_eq!(
                p["auth"],
                json!({"token":"review-secret-token","password":"review-secret-password"})
            );
            let mut response = json!({"type":"res","id":req["id"],"ok":true,"payload":{"type":"hello-ok","protocol":4}});
            match defect {
                "wrong-id" => response["id"] = "unrelated-request".into(),
                "wrong-protocol" => response["payload"]["protocol"] = 3.into(),
                "rejected" => {
                    response["ok"] = false.into();
                    response["error"] = json!({"message":"review-secret-token"});
                }
                _ => (),
            }
            send(&mut ws, response).await;
            close(ws).await;
        });
        let cfg = config(url);
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("review-secret"));
        assert!(!debug.contains("review-session"));
        match GatewayClient::connect(cfg).await {
            Ok(client) => {
                assert_eq!(defect, "none");
                client.disconnect().await;
            }
            Err(error) => {
                assert_ne!(defect, "none");
                assert!(!format!("{error:?} {error}").contains("review-secret"));
            }
        }
        done(server).await;
    }
}

#[tokio::test]
async fn review_withheld_challenge_and_dropped_connect_future_close_owned_socket() {
    for drop_future in [false, true] {
        let (listener, url) = listener().await;
        let server = tokio::spawn(async move {
            let ws = accept(listener).await;
            close(ws).await;
        });
        let mut cfg = config(url);
        cfg.limits.connect_timeout = Duration::from_millis(80);
        if drop_future {
            assert!(
                tokio::time::timeout(Duration::from_millis(30), GatewayClient::connect(cfg))
                    .await
                    .is_err()
            );
        } else {
            assert!(matches!(
                GatewayClient::connect(cfg).await,
                Err(GatewayError::Timeout(_))
            ));
        }
        done(server).await;
    }
}

#[test]
fn review_cleartext_is_explicit_literal_loopback_and_public_diagnostics_are_redacted() {
    for (url, allow, valid) in [
        ("ws://127.0.0.1:18789", false, false),
        ("ws://127.0.0.1:18789", true, true),
        ("ws://[::1]:18789", true, true),
        ("ws://localhost:18789", true, false),
        ("ws://example.org:18789", true, false),
        ("wss://example.org/gateway", false, true),
        ("wss://user:secret@example.org/", false, false),
        ("wss://example.org/?token=secret", false, false),
        ("wss://example.org/#secret", false, false),
    ] {
        let mut cfg = config(url.into());
        cfg.allow_loopback_cleartext = allow;
        assert_eq!(cfg.validate().is_ok(), valid, "URL policy: {url}");
    }
    let identity = RunIdentity {
        run_id: "secret-run".into(),
        session_key: "secret-session".into(),
    };
    let end = RunResult {
        run_id: "secret-run".into(),
        status: RunStatus::Failed,
        text: "secret-text".into(),
        remote_outcome: RemoteOutcome::Unknown,
    };
    let event = RunEvent::Text {
        run_id: "secret-run".into(),
        text: "secret-text".into(),
    };
    assert!(!format!("{identity:?} {end:?} {event:?}").contains("secret-"));
}

#[tokio::test]
async fn review_stalled_native_peer_bounds_large_write_and_concurrent_abort() {
    // A per-fixture receive buffer pins actual socket pressure; no global
    // network settings or external service is involved.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let (resume, resumed) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = hello(listener).await;
        let old = request(&mut ws).await;
        ok(&mut ws, &old, json!({"runId":"existing-run"})).await;
        receive(resumed).await;
        let mut sends = 0;
        let mut aborts = 0;
        while let Some(Ok(message)) = futures_util::StreamExt::next(&mut ws).await {
            if let tokio_tungstenite::tungstenite::Message::Text(text) = message {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                match value["method"].as_str() {
                    Some("chat.send") => sends += 1,
                    Some("chat.abort") => aborts += 1,
                    _ => panic!("unexpected request"),
                }
            } else if message.is_close() {
                break;
            }
        }
        assert!(sends <= 1, "large request was retried");
        assert!(aborts <= 1, "abort was retried");
    });
    let mut cfg = config(url);
    cfg.limits.max_text_bytes = 16 * 1024 * 1024;
    cfg.limits.max_message_bytes = 32 * 1024 * 1024;
    cfg.limits.write_timeout = Duration::from_millis(80);
    cfg.limits.request_timeout = Duration::from_secs(2);
    cfg.limits.run_timeout = Duration::from_secs(3);
    let client = GatewayClient::connect(cfg).await.unwrap();
    let mut old = client.start("existing", None).await.unwrap();
    let handle = old.handle();
    let (large, abort) = tokio::time::timeout(DEADLINE, async {
        tokio::join!(
            client.start("x".repeat(16 * 1024 * 1024), Some("large-session")),
            client.abort(&handle)
        )
    })
    .await
    .expect("stalled write left public operations unbounded");
    assert!(
        matches!(large, Err(GatewayError::OutcomeUnknown(_))),
        "large write was not classified as ambiguous"
    );
    assert!(
        matches!(
            abort,
            Err(GatewayError::OutcomeUnknown(_) | GatewayError::BeforeWrite(_))
        ),
        "stalled abort must not claim cancellation"
    );
    let end = result(&mut old).await;
    assert_eq!(end.status, RunStatus::Failed);
    assert_eq!(end.remote_outcome, RemoteOutcome::Unknown);
    resume.send(()).unwrap();
    client.disconnect().await;
    done(server).await;
}

#[tokio::test]
async fn review_handshake_and_idle_ping_receive_native_pong_without_application_writes() {
    let (listener, url) = listener().await;
    let (checked, check) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept(listener).await;
        send(&mut ws, json!({"type":"event","event":"connect.challenge"})).await;
        let connect = request(&mut ws).await;
        let hello_ping = vec![1u8, 2, 3];
        futures_util::SinkExt::send(
            &mut ws,
            tokio_tungstenite::tungstenite::Message::Ping(hello_ping.clone().into()),
        )
        .await
        .unwrap();
        let pong = tokio::time::timeout(DEADLINE, futures_util::StreamExt::next(&mut ws))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(pong,tokio_tungstenite::tungstenite::Message::Pong(data) if data.as_ref()==hello_ping.as_slice())
        );
        ok(&mut ws, &connect, json!({"type":"hello-ok","protocol":4})).await;
        let idle_ping = vec![4u8, 5, 6];
        futures_util::SinkExt::send(
            &mut ws,
            tokio_tungstenite::tungstenite::Message::Ping(idle_ping.clone().into()),
        )
        .await
        .unwrap();
        let pong = tokio::time::timeout(DEADLINE, futures_util::StreamExt::next(&mut ws))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(pong,tokio_tungstenite::tungstenite::Message::Pong(data) if data.as_ref()==idle_ping.as_slice())
        );
        checked.send(()).unwrap();
        close(ws).await;
    });
    let client = GatewayClient::connect(config(url)).await.unwrap();
    receive(check).await;
    client.disconnect().await;
    done(server).await;
}
