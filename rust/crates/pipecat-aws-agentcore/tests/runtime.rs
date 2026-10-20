//! Acceptance through the actual public app and processor, never a replacement
//! HTTP demo. All credentials are synthetic; all socket peers are literal loopback.
mod common;
use common::*;
use pipecat_aws_agentcore::{
    AgentCoreClient, AgentCoreProcessor, AgentCoreReport, AgentCoreTextApp, Error,
};
use pipecat_core::{
    async_trait, ChatMessage, ChatRole, DetailedLlmContextFrame, Direction, Frame, FrameData,
    FrameProcessor, LlmContextData, LlmContinuationProvider, MessageContent, MessagePart,
    NativeLlmContinuation, Pipeline, PipelineConfig, ProcessorContext, ProcessorError,
    ProcessorUnusablePolicy, WorkerState,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    sync::{oneshot, Notify, Semaphore},
    time::timeout,
};

fn joined(report: &AgentCoreReport) {
    assert!(report.pipeline_joined, "{report:?}");
    assert_eq!(
        report.progress.admitted, report.progress.owners_released,
        "{report:?}"
    );
}
async fn finish(app: &pipecat_aws_agentcore::AgentCoreRunningApp) -> AgentCoreReport {
    timeout(WAIT, app.finish())
        .await
        .expect("actual app did not join")
}
#[tokio::test]
async fn real_json_wire_last_message_exact_signature_and_socket_join() {
    let listener = listener().await;
    let config = config(&listener);
    let host = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        verify_request(&request, &host, " \nlast λ \t", ACCESS, SECRET, Some(TOKEN));
        response(
            &mut socket,
            "Application/JSON; charset=\"UTF-8\"",
            "{\"ignored\":[1,{\"value\":true}],\"response\":\"answer 雪\"}".as_bytes(),
            1,
        )
        .await;
        peer_eof(&mut socket).await;
        no_retry(&listener).await;
    });
    let output = recording();
    let mut input = context(" \nlast λ \t");
    input
        .messages
        .insert(0, ChatMessage::new(ChatRole::System, "not replayed").into());
    input.messages.insert(
        1,
        ChatMessage::new(ChatRole::Assistant, "not appended").into(),
    );
    let app = AgentCoreTextApp::new(config, vec![input], output.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let report = finish(&app).await;
    assert!(report.is_success(), "{report:?}");
    joined(&report);
    assert_eq!(report.progress.completed_at_eof, 1);
    assert_eq!(
        output.snapshot(),
        [Event::Start, Event::Text("answer 雪".into()), Event::End]
    );
    peer.await.unwrap();
}

#[tokio::test]
async fn source_sse_independent_lines_fragmented_utf8_and_unterminated_final_line() {
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        let body = "\u{feff}:comment\r\nevent: ignored\r\n\r\ndata: {}\ndata:{\"response\":null}\ndata: {\"response\":\"\"}\ndata: {\"response\":\"雪\"}\r\ndata:{\"response\":\" second\"}\ndata: {\"response\":\" final\"}";
        response(&mut socket, "text/event-stream", body.as_bytes(), 1).await;
        peer_eof(&mut socket).await;
    });
    let output = recording();
    let app = AgentCoreTextApp::new(config, vec![context("prompt")], output.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let report = finish(&app).await;
    assert!(report.is_success(), "{report:?}");
    assert_eq!(
        output.snapshot(),
        [
            Event::Start,
            Event::Text("雪".into()),
            Event::Text(" second".into()),
            Event::Text(" final".into()),
            Event::End
        ]
    );
    peer.await.unwrap();
}

#[tokio::test]
async fn quiet_gap_is_not_eof_or_permission_to_start_queued_request() {
    let listener = listener().await;
    let config = config(&listener);
    let (continue_tx, continue_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let one = read_request(&mut first).await;
        head(&mut first, "text/event-stream").await;
        chunk(&mut first, b"data: {\"response\":\"early\"}\n").await;
        continue_rx.await.unwrap();
        // A complete gap segment is not invocation EOF. Even an admitted second
        // context must not have opened a second connection while this peer waits.
        no_retry(&listener).await;
        chunk(&mut first, b"data: {\"response\":\"late\"}\n").await;
        end_chunks(&mut first).await;
        peer_eof(&mut first).await;
        let (mut second, _) = listener.accept().await.unwrap();
        let two = read_request(&mut second).await;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&one.body).unwrap(),
            serde_json::json!({"prompt":"first"})
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&two.body).unwrap(),
            serde_json::json!({"prompt":"second"})
        );
        assert_ne!(one.headers[SESSION], two.headers[SESSION]);
        response(
            &mut second,
            "application/json",
            br#"{"response":"next"}"#,
            3,
        )
        .await;
        peer_eof(&mut second).await;
        no_retry(&listener).await;
    });
    let output = recording();
    let app = AgentCoreTextApp::new(
        config,
        vec![context("first"), context("second")],
        output.clone(),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    output.wait_len(3).await;
    assert_eq!(
        output.snapshot(),
        [Event::Start, Event::Text("early".into()), Event::End]
    );
    assert_eq!(app.completion().progress().completed_at_eof, 0);
    assert!(
        timeout(Duration::from_millis(100), app.finish())
            .await
            .is_err(),
        "End returned at quiet gap instead of HTTP EOF"
    );
    continue_tx.send(()).unwrap();
    let report = finish(&app).await;
    assert!(report.is_success(), "{report:?}");
    assert_eq!(report.progress.admitted, 2);
    assert_eq!(
        output.snapshot(),
        [
            Event::Start,
            Event::Text("early".into()),
            Event::End,
            Event::Start,
            Event::Text("late".into()),
            Event::End,
            Event::Start,
            Event::Text("next".into()),
            Event::End
        ]
    );
    peer.await.unwrap();
}

#[tokio::test]
async fn json_waits_for_actual_http_eof_before_output() {
    let listener = listener().await;
    let config = config(&listener);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        head(&mut socket, "application/json").await;
        chunk(&mut socket, br#"{"response":"complete JSON but not EOF"}"#).await;
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        end_chunks(&mut socket).await;
        peer_eof(&mut socket).await;
    });
    let output = recording();
    let app = AgentCoreTextApp::new(config, vec![context("prompt")], output.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    ready_rx.await.unwrap();
    assert!(timeout(Duration::from_millis(80), app.finish())
        .await
        .is_err());
    assert!(output.snapshot().is_empty());
    release_tx.send(()).unwrap();
    assert!(finish(&app).await.is_success());
    peer.await.unwrap();
}

#[tokio::test]
async fn independent_apps_have_isolated_credentials_outputs_and_sessions() {
    let listener_a = listener().await;
    let listener_b = listener().await;
    let config_a = config(&listener_a);
    let config_b = config_with_credentials(
        &listener_b,
        "SECONDTESTACCESS",
        "second-synthetic-secret",
        None,
    );
    let a_host = listener_a.local_addr().unwrap().to_string();
    let b_host = listener_b.local_addr().unwrap().to_string();
    let first = tokio::spawn(async move {
        let (mut socket, _) = listener_a.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        verify_request(&request, &a_host, "A", ACCESS, SECRET, Some(TOKEN));
        response(
            &mut socket,
            "application/json",
            br#"{"response":"only-A"}"#,
            7,
        )
        .await;
        peer_eof(&mut socket).await;
        request.headers[SESSION].clone()
    });
    let second = tokio::spawn(async move {
        let (mut socket, _) = listener_b.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        verify_request(
            &request,
            &b_host,
            "B",
            "SECONDTESTACCESS",
            "second-synthetic-secret",
            None,
        );
        response(
            &mut socket,
            "application/json",
            br#"{"response":"only-B"}"#,
            4,
        )
        .await;
        peer_eof(&mut socket).await;
        request.headers[SESSION].clone()
    });
    let a_out = recording();
    let b_out = recording();
    let a = AgentCoreTextApp::new(config_a, vec![context("A")], a_out.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let b = AgentCoreTextApp::new(config_b, vec![context("B")], b_out.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let (a_report, b_report) = tokio::join!(finish(&a), finish(&b));
    assert!(a_report.is_success(), "{a_report:?}");
    assert!(b_report.is_success(), "{b_report:?}");
    assert_ne!(first.await.unwrap(), second.await.unwrap());
    assert_eq!(
        a_out.snapshot(),
        [Event::Start, Event::Text("only-A".into()), Event::End]
    );
    assert_eq!(
        b_out.snapshot(),
        [Event::Start, Event::Text("only-B".into()), Event::End]
    );
}

#[tokio::test]
async fn no_op_contexts_and_empty_responses_have_no_fictional_bookends() {
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        let mut sessions = Vec::new();
        for body in [
            b"{}".as_slice(),
            br#"{"response":null}"#,
            br#"{"response":""}"#,
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            sessions.push(request.headers[SESSION].clone());
            response(&mut socket, "application/json", body, 2).await;
            peer_eof(&mut socket).await;
        }
        sessions.sort();
        sessions.dedup();
        assert_eq!(sessions.len(), 3);
        no_retry(&listener).await;
    });
    let mut empty = context("");
    empty.messages.clear();
    let mut assistant = context("ignored");
    assistant.messages[0].role = ChatRole::Assistant;
    let mut empty_parts = context("ignored");
    empty_parts.messages[0].content = MessageContent::Parts(vec![]);
    let contexts = vec![
        empty,
        context(""),
        assistant,
        empty_parts,
        context("one"),
        context("two"),
        context("three"),
    ];
    let output = recording();
    let app = AgentCoreTextApp::new(config, contexts, output.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let report = finish(&app).await;
    assert!(report.is_success(), "{report:?}");
    assert_eq!(report.progress.contexts_seen, 7);
    assert_eq!(report.progress.admitted, 3);
    assert!(output.snapshot().is_empty());
    peer.await.unwrap();
}

#[tokio::test]
async fn nonempty_text_parts_preserve_empty_parts_and_exact_single_space_joining() {
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        for expected in [" a  b ", ""] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                serde_json::json!({"prompt": expected})
            );
            response(&mut socket, "application/json", b"{}", 1).await;
            peer_eof(&mut socket).await;
        }
    });
    let mut parts = context("unused");
    parts.messages[0].content = MessageContent::Parts(
        ["", "a", "", "b", ""]
            .into_iter()
            .map(|text| MessagePart::Text { text: text.into() })
            .collect(),
    );
    let mut single_empty = context("unused");
    single_empty.messages[0].content = MessageContent::Parts(vec![MessagePart::Text {
        text: String::new(),
    }]);
    let app = AgentCoreTextApp::new(config, vec![parts, single_empty], recording())
        .unwrap()
        .start()
        .await
        .unwrap();
    assert!(finish(&app).await.is_success());
    peer.await.unwrap();
}

#[tokio::test]
async fn all_input_is_validated_before_first_network_or_output_effect() {
    let listener = listener().await;
    let cases = [
        MessagePart::Image {
            url: "https://invalid.test/private".into(),
            detail: None,
        },
        MessagePart::Audio {
            data: "fake".into(),
            format: "wav".into(),
        },
        MessagePart::File {
            data: "fake".into(),
            mime_type: "text/plain".into(),
            filename: None,
        },
        MessagePart::BedrockReasoning {
            text: "private".into(),
            signature: None,
        },
    ];
    for part in cases {
        let mut unsupported = context("must not send");
        unsupported.messages[0].content = MessageContent::Parts(vec![part]);
        let output = recording();
        assert!(AgentCoreTextApp::new(
            config(&listener),
            vec![context("first valid"), unsupported],
            output.clone()
        )
        .is_err());
        assert!(output.snapshot().is_empty());
    }
    let mut metadata = context("invalid metadata");
    metadata.messages[0].name = Some("name".into());
    assert!(AgentCoreTextApp::new(config(&listener), vec![metadata], recording()).is_err());
    let mut limits = limits();
    limits.max_contexts = 1;
    assert!(AgentCoreTextApp::new(
        config(&listener).with_limits(limits).unwrap(),
        vec![context("one"), context("two")],
        recording()
    )
    .is_err());
    no_retry(&listener).await;
}

#[tokio::test]
async fn malformed_response_matrix_fails_closed_and_never_retries() {
    let cases: Vec<(&str, &str, Vec<u8>)> = vec![
        ("redirect", "HTTP/1.1 302 Found\r\nContent-Type: application/json\r\nLocation: http://192.0.2.1/never\r\n", b"{}".to_vec()),
        ("server-error", "HTTP/1.1 503 Unavailable\r\nContent-Type: application/json\r\n", b"private-service-error".to_vec()),
        ("mime", "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n", b"{}".to_vec()),
        ("mime-param", "HTTP/1.1 200 OK\r\nContent-Type: application/json; arbitrary=yes\r\n", b"{}".to_vec()),
        ("missing-mime", "HTTP/1.1 200 OK\r\n", b"{}".to_vec()),
        ("duplicate-mime", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Type: application/json\r\n", b"{}".to_vec()),
        ("duplicate-session", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Amzn-Bedrock-AgentCore-Runtime-Session-Id: a\r\nX-Amzn-Bedrock-AgentCore-Runtime-Session-Id: b\r\n", b"{}".to_vec()),
        ("array", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n", b"[]".to_vec()),
        ("duplicate-response", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n", br#"{"response":"secret-body","response":"duplicate"}"#.to_vec()),
        ("wrong-response", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n", br#"{"response":23}"#.to_vec()),
        ("trailing-json", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n", b"{}{}".to_vec()),
        ("incomplete-json", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n", br#"{"response":"secret-body""#.to_vec()),
        ("invalid-utf8", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n", vec![0xff]),
        ("done-not-sentinel", "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n", b"data: [DONE]\n".to_vec()),
        ("multiline-not-standard-sse", "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n", b"data: {\"response\":\ndata: \"wrong\"}\n\n".to_vec()),
        ("partial-utf8-at-eof", "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n", b"data: {\"response\":\"\xe9".to_vec()),
    ];
    for (label, head, body) in cases {
        let listener = listener().await;
        let config = config(&listener);
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            let mut wire = format!(
                "{head}Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .into_bytes();
            wire.extend(body);
            socket.write_all(&wire).await.unwrap();
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = recording();
        let app = AgentCoreTextApp::new(config, vec![context("secret-prompt")], output.clone())
            .unwrap()
            .start()
            .await
            .unwrap();
        let report = finish(&app).await;
        assert!(
            !report.is_success(),
            "accepted malformed case {label}: {report:?}"
        );
        joined(&report);
        assert_eq!(report.progress.completed_at_eof, 0, "{label}");
        assert!(output.snapshot().is_empty(), "{label}");
        for secret in ["secret-body", "secret-prompt", SECRET, TOKEN, ARN] {
            assert!(
                !format!("{report:?}").contains(secret),
                "error leaks {label}"
            );
        }
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn truncated_content_length_and_chunked_stream_are_not_successful_eof() {
    for wire in [b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{}".as_slice(), b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n20\r\ndata: {}\n"] {
        let listener = listener().await;
        let config = config(&listener);
        let wire = wire.to_vec();
        let peer = tokio::spawn(async move { let (mut socket, _) = listener.accept().await.unwrap(); read_request(&mut socket).await; socket.write_all(&wire).await.unwrap(); socket.shutdown().await.unwrap(); peer_closed(&mut socket).await; no_retry(&listener).await; });
        let app = AgentCoreTextApp::new(config, vec![context("prompt")], recording()).unwrap().start().await.unwrap();
        let report = finish(&app).await;
        assert!(!report.is_success(), "{report:?}"); joined(&report);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn response_body_line_output_and_header_budgets_are_enforced() {
    for kind in [
        "body",
        "line",
        "output",
        "header",
        "header-count",
        "session",
    ] {
        let listener = listener().await;
        let mut limits = limits();
        let (mime, body) = match kind {
            "body" => {
                limits.max_response_bytes = 32;
                (
                    "application/json",
                    format!("{{\"response\":\"{}\"}}", "a".repeat(60)),
                )
            }
            "line" => {
                limits.max_line_bytes = 16;
                ("text/event-stream", format!(":{}\n", "ignored".repeat(10)))
            }
            "output" => {
                limits.max_output_bytes = 4;
                (
                    "text/event-stream",
                    "data: {\"response\":\"abc\"}\ndata: {\"response\":\"def\"}\n".into(),
                )
            }
            _ => ("application/json", "{}".into()),
        };
        let config = config(&listener).with_limits(limits).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            let extra = match kind {
                "header" => format!("X-Large: {}\r\n", "x".repeat(20_000)),
                "header-count" => (0..40).map(|n| format!("X-{n}: a\r\n")).collect(),
                "session" => format!(
                    "X-Amzn-Bedrock-AgentCore-Runtime-Session-Id: {}\r\n",
                    "s".repeat(101)
                ),
                _ => String::new(),
            };
            let wire = format!("HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            // Large header rejection may close while a write is still pending.
            let _ = socket.write_all(wire.as_bytes()).await;
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let app = AgentCoreTextApp::new(config, vec![context("prompt")], recording())
            .unwrap()
            .start()
            .await
            .unwrap();
        let report = finish(&app).await;
        assert!(!report.is_success(), "unbounded {kind}: {report:?}");
        joined(&report);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn response_session_is_bounded_metadata_not_mandatory_request_equality() {
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        assert_ne!(request.headers[SESSION], "different-response-id");
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Amzn-Bedrock-AgentCore-Runtime-Session-Id: different-response-id\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
        peer_eof(&mut socket).await;
    });
    let app = AgentCoreTextApp::new(config, vec![context("prompt")], recording())
        .unwrap()
        .start()
        .await
        .unwrap();
    assert!(finish(&app).await.is_success());
    peer.await.unwrap();
}

#[tokio::test]
async fn headers_idle_and_total_deadlines_close_the_actual_peer_without_retry() {
    for mode in ["headers", "idle", "total"] {
        let listener = listener().await;
        let mut limits = limits();
        match mode {
            "headers" => limits.write_timeout = Duration::from_millis(100),
            "idle" => limits.idle_timeout = Duration::from_millis(100),
            _ => limits.total_timeout = Duration::from_millis(100),
        }
        let config = config(&listener).with_limits(limits).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            if mode != "headers" {
                head(&mut socket, "text/event-stream").await;
            }
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let app = AgentCoreTextApp::new(config, vec![context("prompt")], recording())
            .unwrap()
            .start()
            .await
            .unwrap();
        let report = finish(&app).await;
        assert!(!report.is_success(), "{mode}: {report:?}");
        joined(&report);
        assert!(
            matches!(report.primary, Some(Error::Timeout(_))),
            "{mode}: {report:?}"
        );
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn cancel_stop_and_drop_join_before_connect_and_during_headers_or_body() {
    for phase in ["before-connect", "headers", "body"] {
        for action in ["cancel", "stop", "drop"] {
            let listener = listener().await;
            let mut bounded = limits();
            bounded.max_pending_invocations = 1;
            let config = config(&listener).with_limits(bounded).unwrap();
            let (ready_tx, ready_rx) = oneshot::channel();
            let peer = if phase == "before-connect" {
                None
            } else {
                Some(tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    read_request(&mut socket).await;
                    if phase == "body" {
                        head(&mut socket, "text/event-stream").await;
                        chunk(&mut socket, b"data: {\"response\":\"partial\"}\n").await;
                    }
                    ready_tx.send(()).unwrap();
                    peer_closed(&mut socket).await;
                    no_retry(&listener).await;
                }))
            };
            let app =
                AgentCoreTextApp::new(config, vec![context("one"), context("queued")], recording())
                    .unwrap()
                    .start()
                    .await
                    .unwrap();
            let completion = app.completion();
            if phase != "before-connect" {
                ready_rx.await.unwrap();
            }
            match action {
                "cancel" => app.cancel(),
                "stop" => app.stop(),
                _ => {}
            }
            // Preserve Stop until its result is observed; dropping the running
            // owner first would replace Stop with the owner's Cancel request.
            let report = if action == "drop" {
                drop(app);
                timeout(WAIT, completion.wait()).await.unwrap()
            } else {
                let report = finish(&app).await;
                drop(app);
                report
            };
            assert!(!report.is_success(), "{phase}/{action}: {report:?}");
            joined(&report);
            assert_ne!(
                report.primary,
                Some(Error::Task),
                "capacity-wait cancellation must not become admission failure: {report:?}"
            );
            if let Some(peer) = peer {
                peer.await.unwrap();
            }
        }
    }
}

#[tokio::test]
async fn cancelled_and_concurrent_completion_waiters_retain_the_join_handle() {
    let listener = listener().await;
    let config = config(&listener);
    let (release_tx, release_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        response(
            &mut socket,
            "application/json",
            br#"{"response":"joined"}"#,
            2,
        )
        .await;
        peer_eof(&mut socket).await;
    });
    let app = AgentCoreTextApp::new(config, vec![context("prompt")], recording())
        .unwrap()
        .start()
        .await
        .unwrap();
    ready_rx.await.unwrap();
    let completion = app.completion();
    assert!(timeout(Duration::from_millis(30), completion.wait())
        .await
        .is_err());
    let a = completion.clone();
    let b = completion.clone();
    let one = tokio::spawn(async move { a.wait().await });
    let two = tokio::spawn(async move { b.wait().await });
    release_tx.send(()).unwrap();
    let first = timeout(WAIT, one).await.unwrap().unwrap();
    let second = timeout(WAIT, two).await.unwrap().unwrap();
    assert!(first.is_success(), "{first:?}");
    assert!(second.is_success(), "{second:?}");
    assert_eq!(first.progress, second.progress);
    joined(&first);
    joined(&second);
    assert_eq!(completion.wait().await.progress, first.progress);
    peer.await.unwrap();
}

struct ControlledOutput {
    entered: Notify,
    permits: Semaphore,
    fail: bool,
    panic: bool,
    cleaned: AtomicBool,
}
impl ControlledOutput {
    fn new(fail: bool, panic: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            permits: Semaphore::new(0),
            fail,
            panic,
            cleaned: AtomicBool::new(false),
        })
    }
}
#[async_trait]
impl FrameProcessor for ControlledOutput {
    fn name(&self) -> &str {
        "ControlledAgentCoreOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmText(_)) {
            self.entered.notify_one();
            assert!(!self.panic, "synthetic output receiver panic");
            if self.fail {
                return Err(ProcessorError::fatal("synthetic output receiver failure"));
            }
            let permit = self.permits.acquire().await.unwrap();
            permit.forget();
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.store(true, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn bounded_held_output_is_cancellable_and_output_failures_or_panics_are_joined() {
    for mode in ["cancel", "stop", "drop", "failure", "panic", "timeout"] {
        let listener = listener().await;
        let mut limits = limits();
        limits.max_pending_invocations = 1;
        let config = config(&listener).with_limits(limits).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            head(&mut socket, "text/event-stream").await;
            let body = "data: {\"response\":\"held\"}\n".repeat(100);
            chunk(&mut socket, body.as_bytes()).await;
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = ControlledOutput::new(mode == "failure", mode == "panic");
        let app = AgentCoreTextApp::new(
            config,
            vec![context("prompt"), context("queued")],
            output.clone(),
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        if timeout(WAIT, output.entered.notified()).await.is_err() {
            let progress = app.completion().progress();
            let report = timeout(Duration::from_millis(100), app.finish()).await;
            panic!("output never entered for {mode}: {progress:?}; report={report:?}");
        }
        let completion = app.completion();
        match mode {
            "cancel" => {
                app.cancel();
                let report = finish(&app).await;
                joined(&report);
            }
            "stop" => {
                app.stop();
                let report = finish(&app).await;
                joined(&report);
                assert_eq!(report.state, WorkerState::Stopped, "{report:?}");
            }
            "drop" => {}
            _ => {
                let report = finish(&app).await;
                assert!(!report.is_success(), "{mode}: {report:?}");
                joined(&report);
            }
        }
        drop(app);
        let report = timeout(WAIT, completion.wait()).await.unwrap();
        assert!(!report.is_success(), "{mode}: {report:?}");
        joined(&report);
        assert!(
            output.cleaned.load(Ordering::SeqCst),
            "output cleanup did not run: {mode}"
        );
        peer.await.unwrap();
    }
}

async fn send(worker: &pipecat_core::PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
fn detailed(context: LlmContextData) -> FrameData {
    FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context,
        speculation: false,
    })
}
#[tokio::test]
async fn actual_processor_interruption_closes_old_socket_and_allows_fresh_work() {
    let listener = listener().await;
    let config = config(&listener);
    let (ready_tx, ready_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let old = read_request(&mut first).await;
        ready_tx.send(()).unwrap();
        peer_closed(&mut first).await;
        let (mut second, _) = listener.accept().await.unwrap();
        let fresh = read_request(&mut second).await;
        assert_ne!(old.headers[SESSION], fresh.headers[SESSION]);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fresh.body).unwrap(),
            serde_json::json!({"prompt":"fresh"})
        );
        response(
            &mut second,
            "application/json",
            br#"{"response":"new generation"}"#,
            2,
        )
        .await;
        peer_eof(&mut second).await;
        no_retry(&listener).await;
    });
    let provider = Arc::new(AgentCoreProcessor::new(
        AgentCoreClient::new(config).unwrap(),
    ));
    let output = recording();
    let mut worker = Pipeline::new(vec![
        provider.ingress_guard(),
        provider.clone(),
        provider.output_guard(output.clone()),
    ])
    .start(PipelineConfig {
        processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
        ..Default::default()
    })
    .await
    .unwrap();
    send(&worker, detailed(context("old"))).await;
    ready_rx.await.unwrap();
    send(&worker, FrameData::Interruption).await;
    send(&worker, detailed(context("fresh"))).await;
    worker.end().await.unwrap();
    timeout(WAIT, async { while worker.recv().await.is_some() {} })
        .await
        .unwrap();
    let report = worker.join().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        output.snapshot(),
        [
            Event::Start,
            Event::Text("new generation".into()),
            Event::End
        ]
    );
    let progress = provider.progress();
    assert_eq!(progress.admitted, 2);
    assert_eq!(progress.owners_released, 2);
    assert_eq!(progress.completed_at_eof, 1);
    peer.await.unwrap();
}

#[tokio::test]
async fn processor_rejects_speculation_control_and_native_authority_without_network() {
    for case in 0..3 {
        let listener = listener().await;
        let provider = Arc::new(AgentCoreProcessor::new(
            AgentCoreClient::new(config(&listener)).unwrap(),
        ));
        let mut worker = Pipeline::new(vec![
            provider.ingress_guard(),
            provider.clone(),
            recording(),
        ])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut frame = match case {
            0 => Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
                context: context("forbidden"),
                speculation: true,
            })),
            1 => Frame::new(FrameData::LlmRun),
            _ => Frame::new(FrameData::LlmResponseStart),
        };
        if case == 2 {
            frame
                .set_native_llm_continuation(Arc::new(
                    NativeLlmContinuation::start(149, LlmContinuationProvider::AwsBedrock).unwrap(),
                ))
                .unwrap();
        }
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        timeout(WAIT, async { while worker.recv().await.is_some() {} })
            .await
            .unwrap();
        let _result = worker.join().await;
        assert!(worker.total_errors() > 0);
        assert_ne!(worker.state(), WorkerState::Ended);
        assert_eq!(provider.progress().admitted, 0);
        no_retry(&listener).await;
    }
}

struct LifetimeOutput {
    cleaned: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}
impl Drop for LifetimeOutput {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for LifetimeOutput {
    fn name(&self) -> &str {
        "AgentCoreOutputLifetimeWitness"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.store(true, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn dropping_running_and_every_completion_owner_closes_socket_and_reaps_output() {
    let listener = listener().await;
    let config = config(&listener);
    let (ready_tx, ready_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        ready_tx.send(()).unwrap();
        peer_closed(&mut socket).await;
        no_retry(&listener).await;
    });
    let cleaned = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let output = Arc::new(LifetimeOutput {
        cleaned: cleaned.clone(),
        dropped: dropped.clone(),
    });
    let weak = Arc::downgrade(&output);
    let app = AgentCoreTextApp::new(config, vec![context("held")], output)
        .unwrap()
        .start()
        .await
        .unwrap();
    let completion = app.completion();
    let second_completion = completion.clone();
    ready_rx.await.unwrap();
    drop(completion);
    drop(second_completion);
    drop(app);
    timeout(WAIT, async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all-owner drop retained the actual output/pipeline owner");
    assert!(weak.upgrade().is_none());
    assert!(cleaned.load(Ordering::SeqCst));
    peer.await.unwrap();
}

#[tokio::test]
async fn pending_capacity_stops_feeder_without_task_failure_and_runs_cleanup() {
    for stop in [true, false] {
        let listener = listener().await;
        let mut limits = limits();
        limits.max_pending_invocations = 1;
        let config = config(&listener).with_limits(limits).unwrap();
        let (ready_tx, ready_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            ready_tx.send(()).unwrap();
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = ControlledOutput::new(false, false);
        let app = AgentCoreTextApp::new(
            config,
            vec![context("held first"), context("must not start")],
            output.clone(),
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        ready_rx.await.unwrap();
        assert_eq!(app.completion().progress().admitted, 1);
        if stop {
            app.stop();
        } else {
            app.cancel();
        }
        let report = finish(&app).await;
        joined(&report);
        assert!(!report.is_success());
        assert_ne!(report.primary, Some(Error::Task));
        assert_eq!(report.progress.admitted, 1);
        if stop {
            assert_eq!(report.state, WorkerState::Stopped, "{report:?}");
        }
        assert!(output.cleaned.load(Ordering::SeqCst));
        drop(app);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn actual_processor_preserves_unrelated_and_upstream_frames_without_invoking() {
    let listener = listener().await;
    let provider = Arc::new(AgentCoreProcessor::new(
        AgentCoreClient::new(config(&listener)).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![provider.ingress_guard(), provider.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::Text(pipecat_core::TextFrame::new("ordinary"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(detailed(context("upstream must not invoke"))),
            Direction::Upstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut ordinary = false;
    let mut upstream = false;
    timeout(WAIT, async {
        while let Some(frame) = worker.recv().await {
            match &frame.frame.data {
                FrameData::Text(text) if text.text == "ordinary" => {
                    assert_eq!(frame.direction, Direction::Downstream);
                    ordinary = true;
                }
                FrameData::LlmContextDetailed(context) => {
                    assert_eq!(frame.direction, Direction::Upstream);
                    assert_eq!(
                        context.context.messages[0].content,
                        MessageContent::Text("upstream must not invoke".into())
                    );
                    upstream = true;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    worker.join().await.unwrap();
    assert!(ordinary && upstream);
    assert_eq!(provider.progress().admitted, 0);
    no_retry(&listener).await;
}

#[tokio::test]
async fn peer_disconnect_after_written_request_fails_without_replay() {
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        drop(socket);
        no_retry(&listener).await;
    });
    let app = AgentCoreTextApp::new(
        config,
        vec![context("uncertain remote execution")],
        recording(),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let report = finish(&app).await;
    joined(&report);
    assert!(!report.is_success());
    assert_eq!(report.progress.admitted, 1);
    assert_eq!(report.progress.completed_at_eof, 0);
    peer.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn valid_sse_prefix_before_failure_is_independent_of_http_chunking_and_blocks_queue() {
    for split in [false, true] {
        let listener = listener().await;
        let config = config(&listener);
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            head(&mut socket, "text/event-stream").await;
            if split {
                chunk(&mut socket, b"data:{\"response\":\"accepted\"}\n").await;
                tokio::time::sleep(Duration::from_millis(20)).await;
                chunk(&mut socket, b"data:[DONE]\n").await;
            } else {
                chunk(
                    &mut socket,
                    b"data:{\"response\":\"accepted\"}\ndata:[DONE]\n",
                )
                .await;
            }
            peer_eof(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = recording();
        let app = AgentCoreTextApp::new(
            config,
            vec![context("one"), context("must not send")],
            output.clone(),
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        let report = finish(&app).await;
        assert!(!report.is_success(), "{report:?}");
        joined(&report);
        assert_eq!(
            output.snapshot(),
            [Event::Start, Event::Text("accepted".into())]
        );
        assert_eq!(report.progress.completed_at_eof, 0);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn cancelled_withheld_tls_uses_only_literal_loopback_and_joins() {
    use pipecat_aws_agentcore::AgentCoreConfig;
    use pipecat_aws_common::AwsCredentials;
    use tokio::io::AsyncReadExt;
    // Unprivileged literal-loopback TLS, with unchanged certificate verification.
    // This tests withheld handshake cancellation, not hosted AWS SNI success.
    let listener = listener().await;
    let config = AgentCoreConfig::new(
        "us-east-1",
        ARN,
        AwsCredentials::new(ACCESS, SECRET, Some(TOKEN.into())).unwrap(),
    )
    .unwrap()
    .with_loopback_endpoint(&format!("https://{}", listener.local_addr().unwrap()))
    .unwrap()
    .with_limits(limits())
    .unwrap();
    let (ready, observed) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut header = [0u8; 5];
        timeout(WAIT, socket.read_exact(&mut header))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(header[0], 22, "expected TLS handshake record");
        let size = u16::from_be_bytes([header[3], header[4]]) as usize;
        assert!((1..=16384).contains(&size));
        let mut hello = vec![0u8; size];
        timeout(WAIT, socket.read_exact(&mut hello))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(hello[0], 1, "expected actual ClientHello");
        ready.send(()).unwrap();
        peer_eof(&mut socket).await;
    });
    let app = AgentCoreTextApp::new(config, vec![context("fake TLS prompt")], recording())
        .unwrap()
        .start()
        .await
        .unwrap();
    observed.await.unwrap();
    app.cancel();
    let report = finish(&app).await;
    assert!(!report.is_success());
    joined(&report);
    peer.await.unwrap();
}

struct GatedRecording {
    output: Arc<Recording>,
    entered: Notify,
    credit: Semaphore,
}
#[async_trait]
impl FrameProcessor for GatedRecording {
    fn name(&self) -> &str {
        "GatedRecording"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmText(_)) {
            self.entered.notify_one();
            self.credit.acquire().await.unwrap().forget();
        }
        self.output.process(frame, direction, ctx).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_valid_sse_lines_coalesced_or_split_wait_for_real_output_credit() {
    for split in [false, true] {
        let listener = listener().await;
        let mut limits = limits();
        limits.max_pending_invocations = 1;
        limits.idle_timeout = Duration::from_millis(100);
        limits.output_timeout = Duration::from_secs(1);
        let config = config(&listener).with_limits(limits).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            head(&mut socket, "text/event-stream").await;
            if split {
                for _ in 0..100 {
                    chunk(&mut socket, b"data:{\"response\":\"credit\"}\n").await;
                }
            } else {
                chunk(
                    &mut socket,
                    "data:{\"response\":\"credit\"}\n".repeat(100).as_bytes(),
                )
                .await;
            }
            end_chunks(&mut socket).await;
            peer_eof(&mut socket).await;
            no_retry(&listener).await;
        });
        let recorded = recording();
        let output = Arc::new(GatedRecording {
            output: recorded.clone(),
            entered: Notify::new(),
            credit: Semaphore::new(0),
        });
        let app = AgentCoreTextApp::new(config, vec![context("prompt")], output.clone())
            .unwrap()
            .start()
            .await
            .unwrap();
        timeout(WAIT, output.entered.notified()).await.unwrap();
        assert!(timeout(Duration::from_millis(200), app.finish())
            .await
            .is_err());
        assert_eq!(recorded.snapshot(), [Event::Start]);
        assert_eq!(app.completion().progress().completed_at_eof, 0);
        output.credit.add_permits(100);
        let report = finish(&app).await;
        assert!(report.is_success(), "{split}: {report:?}");
        joined(&report);
        let mut expected = vec![Event::Start];
        expected.extend((0..100).map(|_| Event::Text("credit".into())));
        expected.push(Event::End);
        assert_eq!(recorded.snapshot(), expected);
        peer.await.unwrap();
    }
}

struct SecondaryCleanupFailure {
    fail_output: bool,
    timeout_cleanup: bool,
}
#[async_trait]
impl FrameProcessor for SecondaryCleanupFailure {
    fn name(&self) -> &str {
        "SecondaryCleanupFailure"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.fail_output && matches!(frame.data, FrameData::LlmText(_)) {
            return Err(ProcessorError::fatal("synthetic primary output"));
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if self.timeout_cleanup {
            std::future::pending().await
        } else {
            Err(ProcessorError::fatal("synthetic secondary cleanup"))
        }
    }
}
#[tokio::test]
async fn primary_provider_or_output_failure_survives_distinct_cleanup_failure_or_timeout() {
    for fail_output in [false, true] {
        for timeout_cleanup in [false, true] {
            let listener = listener().await;
            let mut limits = limits();
            limits.shutdown_timeout = Duration::from_millis(80);
            let config = config(&listener).with_limits(limits).unwrap();
            let peer = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                response(
                    &mut socket,
                    "text/event-stream",
                    if fail_output {
                        b"data:{\"response\":\"primary\"}\n"
                    } else {
                        b"data:[DONE]\n"
                    },
                    65536,
                )
                .await;
                peer_closed(&mut socket).await;
            });
            let output = Arc::new(SecondaryCleanupFailure {
                fail_output,
                timeout_cleanup,
            });
            let app = AgentCoreTextApp::new(config, vec![context("prompt")], output)
                .unwrap()
                .start()
                .await
                .unwrap();
            let report = finish(&app).await;
            joined(&report);
            assert!(!report.is_success());
            assert!(
                if fail_output {
                    matches!(report.primary, Some(Error::Output))
                } else {
                    matches!(report.primary, Some(Error::Protocol(_)))
                },
                "{report:?}"
            );
            assert!(report.first_pipeline_error.is_some(), "{report:?}");
            assert_eq!(report.cleanup_errors.len(), 1, "{report:?}");
            assert!(
                report.cleanup_errors[0].contains(if timeout_cleanup {
                    "cleanup deadline elapsed"
                } else {
                    "cleanup failed"
                }),
                "{report:?}"
            );
            assert_ne!(
                report.first_pipeline_error.as_ref().unwrap(),
                &report.cleanup_errors[0]
            );
            peer.await.unwrap();
        }
    }
}

#[tokio::test]
async fn constructed_but_uninstalled_output_guard_rejects_before_network() {
    let listener = listener().await;
    let provider = Arc::new(AgentCoreProcessor::new(
        AgentCoreClient::new(config(&listener)).unwrap(),
    ));
    let _unused = provider.output_guard(recording());
    let mut worker = Pipeline::new(vec![
        provider.ingress_guard(),
        provider.clone(),
        recording(),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(&worker, detailed(context("must not send"))).await;
    while worker.recv().await.is_some() {}
    assert!(worker.join().await.is_err());
    assert_eq!(provider.progress().admitted, 0);
    no_retry(&listener).await;
}

#[tokio::test]
async fn stalled_request_body_write_is_cancelled_or_timed_out_and_never_replayed() {
    use tokio::{
        io::AsyncReadExt,
        net::{TcpSocket, TcpStream},
    };
    for cancelled in [true, false] {
        // This is a setting on this synthetic peer socket only. No host-wide
        // sysctl, production override, additional crate or raw fd access is used.
        let listen_socket = TcpSocket::new_v4().unwrap();
        listen_socket.set_recv_buffer_size(1024).unwrap();
        listen_socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listen_socket.listen(1).unwrap();
        let mut limits = limits();
        limits.max_context_bytes = 4 * 1024 * 1024;
        limits.max_string_bytes = 1024 * 1024;
        limits.max_pending_bytes = 16 * 1024 * 1024;
        limits.write_timeout = Duration::from_millis(350);
        limits.total_timeout = Duration::from_secs(10);
        let config = config(&listener).with_limits(limits).unwrap();
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let standard = socket.into_std().unwrap();
            let options = TcpSocket::from_std_stream(standard.try_clone().unwrap());
            let mut socket = TcpStream::from_std(standard).unwrap();
            let mut header = Vec::new();
            timeout(WAIT, async {
                while !header.ends_with(b"\r\n\r\n") {
                    assert!(header.len() < 16384);
                    header.push(socket.read_u8().await.unwrap());
                }
            })
            .await
            .unwrap();
            let header = std::str::from_utf8(&header).unwrap();
            let declared: usize = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(str::trim)
                        .map(str::parse)
                })
                .unwrap()
                .unwrap();
            assert!(declared > 3 * 1024 * 1024);
            // Confirm actual body arrival, then stop consuming its bounded window.
            let mut first = [0u8; 256];
            socket.read_exact(&mut first).await.unwrap();
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
            // After production ownership has joined, drain its already queued
            // kernel bytes promptly and prove the complete body was never sent.
            options.set_recv_buffer_size(4 * 1024 * 1024).unwrap();
            let mut delivered = first.len();
            let mut buffer = [0u8; 65536];
            timeout(WAIT, async {
                loop {
                    let n = socket.read(&mut buffer).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    delivered += n;
                    assert!(delivered <= declared);
                }
            })
            .await
            .unwrap();
            assert!(
                delivered < declared,
                "fixture did not stall the body: {delivered}/{declared}"
            );
            assert!(delivered > first.len());
            eprintln!("stalled-body cancelled={cancelled} delivered={delivered} declared={declared}; app joined before drain");
            drop(options);
            drop(socket);
            no_retry(&listener).await;
        });
        // Escape expansion creates a ~3.9 MiB bounded wire body while the
        // logical prompt remains below the fixed 1 MiB string ceiling.
        let input = context(&"\u{1}".repeat(650_000));
        let app = AgentCoreTextApp::new(config, vec![input], recording())
            .unwrap()
            .start()
            .await
            .unwrap();
        timeout(WAIT, ready_rx).await.unwrap().unwrap();
        if cancelled {
            tokio::time::sleep(Duration::from_millis(120)).await;
            app.cancel();
        }
        let report = finish(&app).await;
        joined(&report);
        assert!(!report.is_success(), "{report:?}");
        assert_eq!(report.progress.completed_at_eof, 0);
        if !cancelled {
            assert_eq!(
                report.primary,
                Some(Error::Timeout("request and response head")),
                "{report:?}"
            );
        }
        release_tx.send(()).unwrap();
        peer.await.unwrap();
    }
}
