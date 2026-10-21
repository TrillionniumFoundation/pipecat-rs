use crate::realtime_support::*;
use pipecat_client_server::{server::BotApi, SessionMethod};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::json;
use std::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

#[tokio::test]
async fn created_is_not_ready_updated_is_ready_and_close_is_terminal() {
    let mut provider = Provider::new(false).await;
    let mut host = Host::new(provider.config.clone()).await;
    let start = host.start().await;
    let mut browser = host.browser(&start, 81).await;
    provider.command().await;
    provider
        .event(json!({"type":"session.created","session":{"id":"local-session","type":"realtime"}}))
        .await;
    // Non-readiness RTVI controls may arrive, but no bot-ready may precede update.
    let observation = timeout(Duration::from_millis(150), async {
        loop {
            if let Incoming::Json(value) = browser.recv().await {
                assert_ne!(value["type"], "bot-ready");
            }
        }
    })
    .await;
    assert!(observation.is_err());
    provider.event(ready()).await;
    browser.ready().await;
    provider.event(json!({"fixture_close":true})).await;
    assert!(!host.report().await.successful);
    assert_eq!(host.runner.active_sessions(), 0);
    host.close().await;
    let _ = browser.socket.finish().await;
    provider.joined().await;
}

#[tokio::test]
async fn readiness_timeout_joins_provider_and_releases_session_slot() {
    let mut provider = Provider::new(false).await;
    let mut host = Host::new(provider.config.clone()).await;
    let start = host.start().await;
    let browser = host.browser(&start, 82).await;
    provider.command().await;
    assert!(!host.report().await.successful);
    assert_eq!(host.runner.active_sessions(), 0);
    host.close().await;
    let _ = browser.socket.finish().await;
    provider.joined().await;
}

#[tokio::test]
async fn cancellation_during_readiness_joins_even_when_waiting_shutdown_caller_is_dropped() {
    let mut provider = Provider::new(false).await;
    let mut host = Host::new(provider.config.clone()).await;
    let start = host.start().await;
    let browser = host.browser(&start, 83).await;
    provider.command().await;
    let runner = host.runner.clone();
    let first = tokio::spawn(async move {
        runner.shutdown().await;
    });
    tokio::task::yield_now().await;
    first.abort();
    let _ = first.await;
    host.close().await;
    let _ = browser.socket.finish().await;
    provider.joined().await;
    assert_eq!(host.runner.active_sessions(), 0);
}

#[tokio::test]
async fn origin_path_and_second_attach_cannot_start_additional_provider_work() {
    let mut provider = Provider::new(true).await;
    let mut host = Host::new(provider.config.clone()).await;
    let start = host.start().await;
    let url = start["wsUrl"].as_str().unwrap();
    for (path, origin) in [
        (url.to_string(), "http://127.0.0.1:1"),
        (format!("{url}/wrong"), host.origin.as_str()),
        (format!("{url}?extra=1"), host.origin.as_str()),
    ] {
        let mut request = path.into_client_request().unwrap();
        request
            .headers_mut()
            .insert("origin", origin.parse().unwrap());
        assert!(
            WebSocketSession::connect_with_request(request, SessionConfig::default())
                .await
                .is_err()
        );
    }
    provider.quiet().await;
    let mut browser = host.browser(&start, 84).await;
    provider.command().await;
    browser.ready().await;
    let mut second = url.into_client_request().unwrap();
    second
        .headers_mut()
        .insert("origin", host.origin.parse().unwrap());
    assert!(
        WebSocketSession::connect_with_request(second, SessionConfig::default())
            .await
            .is_err()
    );
    provider.quiet().await;
    host.close().await;
    let _ = browser.socket.finish().await;
    provider.joined().await;
}

#[tokio::test]
async fn unsupported_rich_tool_and_output_mode_requests_fail_before_inference() {
    for (kind, data) in [
        (
            "send-file",
            json!({"content":"x","file":{"format":"image/png","source":"https://invalid.example/x"}}),
        ),
        (
            "llm-function-call-result",
            json!({"function_name":"bad","tool_call_id":"bad","result":{}}),
        ),
        (
            "send-text",
            json!({"content":"x","options":{"audio_response":false}}),
        ),
        ("update-config", json!({"config":[]})),
    ] {
        let mut provider = Provider::new(true).await;
        let mut host = Host::new(provider.config.clone()).await;
        let start = host.start().await;
        let mut browser = host.browser(&start, 85).await;
        provider.command().await;
        browser.ready().await;
        browser
            .message(json!({"label":"rtvi-ai","id":"unsupported","type":kind,"data":data}))
            .await;
        assert!(!host.report().await.successful);
        assert!(provider.commands.try_recv().is_err());
        host.close().await;
        let _ = browser.socket.finish().await;
        provider.joined().await;
    }
}

#[tokio::test]
async fn malformed_odd_oversized_and_rate_changed_audio_fail_closed() {
    use pipecat_protocol::protobuf::{self as pb, frame::Kind};
    for case in 0..4 {
        let mut provider = Provider::new(true).await;
        let mut host = Host::new(provider.config.clone()).await;
        let start = host.start().await;
        let mut browser = host.browser(&start, 86).await;
        provider.command().await;
        browser.ready().await;
        if case == 3 {
            browser.audio(&[0.; 1024], 48000).await;
            assert_eq!(
                provider.command().await["type"],
                "input_audio_buffer.append"
            );
        }
        let packet = if case == 0 {
            Packet::Binary(vec![255; 7].into())
        } else if case == 1 {
            // Hostile wire bytes deliberately bypass the checked outbound encoder:
            // Frame.audio { audio: [0,0,0], sample_rate: 24000, num_channels: 1 }.
            Packet::Binary(vec![0x12, 0x0b, 0x1a, 3, 0, 0, 0, 0x20, 0xc0, 0xbb, 1, 0x28, 1].into())
        } else {
            let audio = pb::AudioRawFrame {
                id: 1,
                name: String::new(),
                audio: vec![
                    0;
                    match case {
                        2 => 65538,
                        _ => 1024,
                    }
                ],
                sample_rate: 24000,
                num_channels: 1,
                pts: None,
            };
            pipecat_runner::wire::packet(Kind::Audio(audio)).unwrap()
        };
        browser.socket.sender().send(packet).await.unwrap();
        assert!(!host.report().await.successful);
        assert_eq!(host.runner.active_sessions(), 0);
        host.close().await;
        let _ = browser.socket.finish().await;
        provider.joined().await;
    }
}

#[tokio::test]
async fn provider_error_and_odd_output_fail_without_success_report() {
    for odd in [false, true] {
        let mut provider = Provider::new(true).await;
        let mut host = Host::new(provider.config.clone()).await;
        let start = host.start().await;
        let mut browser = host.browser(&start, 87).await;
        provider.command().await;
        browser.ready().await;
        if odd {
            browser.text("audio").await;
            provider.inference().await;
            response(&provider, "odd", "bad", &[1]).await;
        } else {
            provider.event(json!({"type":"error","error":{"type":"invalid_request_error","code":"synthetic","message":"synthetic fixture error"}})).await;
        }
        assert!(!host.report().await.successful);
        host.close().await;
        let _ = browser.socket.finish().await;
        provider.joined().await;
    }
}

#[tokio::test]
async fn disconnect_end_and_api_stop_join_owners_and_close_capability() {
    for graceful in [true, false] {
        let mut provider = Provider::new(true).await;
        let mut host = Host::new(provider.config.clone()).await;
        let start = host.start().await;
        let mut browser = host.browser(&start, 88).await;
        provider.command().await;
        browser.ready().await;
        if graceful {
            browser
                .message(json!({"label":"rtvi-ai","id":"end","type":"disconnect-bot"}))
                .await;
        } else {
            host.runner
                .session(
                    SessionMethod::Post,
                    start["sessionId"].as_str().unwrap(),
                    &["stop"],
                    &json!({}),
                )
                .await
                .unwrap();
        }
        let report = host.report().await;
        assert!(report.successful);
        assert_eq!(host.runner.active_sessions(), 0);
        provider.joined().await;
        host.close().await;
        let _ = browser.socket.finish().await;
    }
}

#[tokio::test]
async fn failed_handshake_and_cancelled_handshake_retain_startup_owner() {
    use tokio::io::AsyncReadExt;
    for cancel in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = pipecat_runner::OpenAiRealtimeVoiceConfig::new("dummy-realtime-key");
        config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        config.limits.connect_timeout = Duration::from_millis(200);
        let (accepted, connected) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            accepted.send(()).unwrap();
            let mut bytes = vec![];
            timeout(WAIT, socket.read_to_end(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            assert!(bytes.starts_with(b"GET "));
        });
        let mut host = Host::new(config).await;
        let start = host.start().await;
        let browser = host.browser(&start, 89).await;
        connected.await.unwrap();
        if cancel {
            host.close().await;
        } else {
            assert!(!host.report().await.successful);
            host.close().await;
        }
        timeout(WAIT, task).await.unwrap().unwrap();
        assert_eq!(host.runner.active_sessions(), 0);
        let _ = browser.socket.finish().await;
    }
}

#[tokio::test]
async fn browser_cannot_override_provider_endpoint_or_mode() {
    let provider = Provider::new(true).await;
    let mut host = Host::new(provider.config.clone()).await;
    for body in [
        json!({"transport":"websocket","endpoint":"ws://127.0.0.1:9"}),
        json!({"transport":"websocket","body":{"realtime":"other"}}),
        json!({"transport":"webrtc"}),
    ] {
        assert!(host.runner.start(&body).await.is_err());
    }
    assert_eq!(host.runner.active_sessions(), 0);
    host.close().await;
}

#[tokio::test]
async fn repeated_upstream_transcripts_cannot_accumulate_unbounded_history() {
    let mut provider = Provider::new(true).await;
    let mut host = Host::new(provider.config.clone()).await;
    let start = host.start().await;
    let mut browser = host.browser(&start, 90).await;
    provider.command().await;
    browser.ready().await;
    // Provider messages each fit the 128 KiB wire bound; cumulative context
    // growth must fail explicitly rather than surviving without limit.
    let send = provider.events.clone();
    let feeder = tokio::spawn(async move {
        for id in 0..80 {
            let event = json!({"type":"conversation.item.input_audio_transcription.completed","item_id":format!("user-{id}"),"content_index":0,"transcript":"x".repeat(60*1024)});
            if send.send(event).await.is_err() {
                break;
            }
        }
    });
    let draining = tokio::spawn(async move {
        while browser.socket.recv().await.is_some() {}
        let _ = browser.socket.finish().await;
    });
    assert!(!host.report().await.successful);
    host.close().await;
    // Fixture writer is allowed to see the deliberate peer close.
    feeder.abort();
    let _ = feeder.await;
    timeout(WAIT, draining).await.unwrap().unwrap();
    provider.joined().await;
}

#[tokio::test]
async fn malformed_disconnect_cannot_mask_provider_closure_as_graceful_end() {
    let mut provider = Provider::new(true).await;
    let mut host = Host::new(provider.config.clone()).await;
    let start = host.start().await;
    let mut browser = host.browser(&start, 91).await;
    provider.command().await;
    browser.ready().await;
    browser
        .message(json!({"label":"rtvi-ai","type":"disconnect-bot"}))
        .await;
    assert!(!host.report().await.successful);
    host.close().await;
    let _ = browser.socket.finish().await;
    provider.joined().await;
}
