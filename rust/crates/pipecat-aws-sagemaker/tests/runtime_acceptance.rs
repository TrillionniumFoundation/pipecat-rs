//! Public-constructor and pipeline acceptance against independently owned TCP peers.
//! Synthetic credentials only; no live provider, SDK, shell or Python bridge.
mod common;
use common::*;
use pipecat_aws_sagemaker::{
    SageMakerClient, SageMakerConfig, SageMakerProcessor, SageMakerReport, SageMakerRunningApp,
    SageMakerTtsApp, SynthesisSettings,
};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig,
    ProcessorContext, ProcessorError, ProcessorUnusablePolicy, ServiceUpdateSettingsFrame, Setting,
    StartFrame, TtsSettings, TtsSpeakFrame, WorkerState,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{oneshot, Notify, Semaphore},
    time::timeout,
};

fn joined(report: &SageMakerReport) {
    assert!(report.pipeline_joined, "{report:?}");
    assert_eq!(
        report.progress.admitted, report.progress.owners_released,
        "{report:?}"
    );
}
async fn finish(app: &SageMakerRunningApp) -> SageMakerReport {
    timeout(WAIT, app.finish())
        .await
        .expect("actual app did not join")
}
async fn send(worker: &pipecat_core::PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
fn speak(text: &str) -> FrameData {
    FrameData::TtsSpeak(TtsSpeakFrame::new(text))
}
async fn drain(worker: &mut pipecat_core::PipelineWorker) {
    timeout(WAIT, async { while worker.recv().await.is_some() {} })
        .await
        .unwrap();
}

#[tokio::test]
async fn exact_magpie_json_signed_accept_body_token_route_and_pcm_lifecycle() {
    let listener = listener().await;
    let config = config(&listener);
    let host = listener.local_addr().unwrap().to_string();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let request = read_request(&mut socket).await;
        verify_request(
            &request,
            &host,
            "Hello 雪",
            &SynthesisSettings::default(),
            ACCESS,
            SECRET,
            Some(TOKEN),
        );
        response(
            &mut socket,
            "application/octet-stream",
            &[0, 0, 255, 127, 0, 128, 255, 255],
            1,
        )
        .await;
        peer_closed(&mut socket).await;
        no_retry(&listener).await;
    });
    let output = recording();
    let app = SageMakerTtsApp::new(config, vec![utterance(" \nHello 雪\t ")], output.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let report = finish(&app).await;
    assert!(report.is_success(), "{report:?}");
    joined(&report);
    assert_eq!(report.progress.utterances_seen, 1);
    assert_eq!(report.progress.admitted, 1);
    assert_eq!(report.progress.completed_at_eof, 1);
    assert_eq!(report.progress.pcm_bytes_admitted, 8);
    assert_eq!(report.progress.pcm_bytes_delivered, 8);
    assert_eq!(report.progress.wire_bytes, 8);
    assert_eq!(report.progress.usage_text_characters, 7);
    assert_eq!(*output.append_flags.lock().unwrap(), [false]);
    assert_eq!(report.progress.ttfb_observations, 1);
    assert!(report.progress.last_ttfb_micros.is_some());
    assert!(!output
        .snapshot()
        .iter()
        .any(|event| matches!(event, Event::Text(_))));
    output.assert_complete(&[0, 0, 255, 127, 0, 128, 255, 255], 24_000, 1);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

#[tokio::test]
async fn content_length_and_every_small_odd_even_http_split_conserve_samples() {
    for split in 0..=9 {
        let listener = listener().await;
        let config = config(&listener);
        let bytes: Vec<u8> = (0..40).collect();
        let expected = bytes.clone();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            read_request(&mut socket).await;
            if split == 0 {
                content_length_response(&mut socket, &bytes).await;
            } else {
                response(&mut socket, "application/octet-stream", &bytes, split).await;
            }
            peer_closed(&mut socket).await;
        });
        let output = recording();
        let app = SageMakerTtsApp::new(
            config,
            vec![utterance("arbitrary transport fragments")],
            output.clone(),
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        let report = finish(&app).await;
        assert!(report.is_success(), "split={split}: {report:?}");
        joined(&report);
        output.assert_complete(&expected, 24_000, 1);
        assert_eq!(report.progress.pcm_bytes_delivered, expected.len() as u64);
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

#[tokio::test]
async fn streaming_precedes_eof_but_queued_utterance_and_end_do_not() {
    let listener = listener().await;
    let config = config(&listener);
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let request = read_request(&mut first).await;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["text"],
            "first"
        );
        head(&mut first, "application/octet-stream").await;
        chunk(&mut first, &[1, 2, 3]).await;
        timeout(WAIT, release_rx)
            .await
            .expect("peer coordination timed out")
            .unwrap();
        no_retry(&listener).await;
        chunk(&mut first, &[4, 5, 6]).await;
        end_chunks(&mut first).await;
        peer_closed(&mut first).await;
        let (mut second, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let request = read_request(&mut second).await;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["text"],
            "second"
        );
        assert!(!request.headers.contains_key("x-amzn-sagemaker-session-id"));
        response(&mut second, "application/octet-stream", &[7, 8], 1).await;
        peer_closed(&mut second).await;
        no_retry(&listener).await;
    });
    let output = recording();
    let app = SageMakerTtsApp::new(
        config,
        vec![utterance("first"), utterance("second")],
        output.clone(),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    output.wait_pcm(2).await;
    assert_eq!(output.pcm(), [1, 2]);
    assert_eq!(app.completion().progress().completed_at_eof, 0);
    assert!(timeout(Duration::from_millis(80), app.finish())
        .await
        .is_err());
    assert!(!output
        .snapshot()
        .iter()
        .any(|event| matches!(event, Event::Stop(_))));
    release_tx.send(()).unwrap();
    let report = finish(&app).await;
    assert!(report.is_success(), "{report:?}");
    joined(&report);
    assert_eq!(report.progress.admitted, 2);
    assert_eq!(report.progress.completed_at_eof, 2);
    output.assert_complete(&[1, 2, 3, 4, 5, 6, 7, 8], 24_000, 2);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

#[tokio::test]
async fn independent_apps_isolate_credentials_voice_rate_output_and_completion() {
    let listener_a = listener().await;
    let listener_b = listener().await;
    let config_a = config(&listener_a);
    let settings_b = SynthesisSettings {
        voice_name: "Different-Voice".into(),
        language_code: "fr-FR".into(),
        sample_rate_hz: 16_000,
    };
    let config_b = config_with_credentials(
        &listener_b,
        "SECONDTESTACCESS",
        "second-synthetic-secret",
        None,
    )
    .with_settings(settings_b.clone())
    .unwrap();
    let a_host = listener_a.local_addr().unwrap().to_string();
    let b_host = listener_b.local_addr().unwrap().to_string();
    let first = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener_a.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let request = read_request(&mut socket).await;
        verify_request(
            &request,
            &a_host,
            "A",
            &SynthesisSettings::default(),
            ACCESS,
            SECRET,
            Some(TOKEN),
        );
        response(&mut socket, "application/octet-stream", &[1, 2], 1).await;
        peer_closed(&mut socket).await;
    });
    let second = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener_b.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let request = read_request(&mut socket).await;
        verify_request(
            &request,
            &b_host,
            "B",
            &settings_b,
            "SECONDTESTACCESS",
            "second-synthetic-secret",
            None,
        );
        response(&mut socket, "application/octet-stream", &[9, 8, 7, 6], 3).await;
        peer_closed(&mut socket).await;
    });
    let a_out = recording();
    let b_out = recording();
    let a = SageMakerTtsApp::new(config_a, vec![utterance("A")], a_out.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let b = SageMakerTtsApp::new(config_b, vec![utterance("B")], b_out.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let (ar, br) = tokio::join!(finish(&a), finish(&b));
    assert!(ar.is_success(), "{ar:?}");
    assert!(br.is_success(), "{br:?}");
    joined(&ar);
    joined(&br);
    a_out.assert_complete(&[1, 2], 24_000, 1);
    b_out.assert_complete(&[9, 8, 7, 6], 16_000, 1);
    first.await.unwrap();
    second.await.unwrap();
}

#[tokio::test]
async fn python_reference_trim_cjk_numeric_and_non_alphanumeric_skip_are_distinct() {
    // CPython Unicode reference: str.strip treats U+001C..U+001F as whitespace;
    // CJK letters, superscript, Roman numeral and vulgar fraction are alphanumeric.
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        for expected in ["雪", "²Ⅷ½", "A\u{1f}B"] {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            let request = read_request(&mut socket).await;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["text"],
                expected
            );
            response(&mut socket, "application/octet-stream", &[1, 0], 1).await;
            peer_closed(&mut socket).await;
        }
        no_retry(&listener).await;
    });
    let output = recording();
    let inputs = [
        "",
        " \t\n\r",
        "!?…",
        "😀🧑‍💻",
        "\u{301}\u{308}",
        "\u{1c}\u{1d}雪\u{1e}\u{1f}",
        "\u{a0}²Ⅷ½\u{2003}",
        "A\u{1f}B",
    ];
    let app = SageMakerTtsApp::new(
        config,
        inputs.into_iter().map(utterance).collect(),
        output.clone(),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let report = finish(&app).await;
    assert!(report.is_success(), "{report:?}");
    joined(&report);
    assert_eq!(report.progress.utterances_seen, 8);
    assert_eq!(report.progress.skipped, 5);
    assert_eq!(report.progress.admitted, 3);
    assert_eq!(report.progress.completed_at_eof, 3);
    output.assert_complete(&[1, 0, 1, 0, 1, 0], 24_000, 3);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

#[tokio::test]
async fn all_skipped_input_has_no_socket_audio_or_successful_synthesis() {
    let listener = listener().await;
    let output = recording();
    let app = SageMakerTtsApp::new(
        config(&listener),
        vec![utterance(" "), utterance("!!!")],
        output.clone(),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let report = finish(&app).await;
    joined(&report);
    assert_eq!(report.progress.utterances_seen, 2);
    assert_eq!(report.progress.skipped, 2);
    assert_eq!(report.progress.admitted, 0);
    assert_eq!(report.progress.completed_at_eof, 0);
    assert_eq!(report.progress.pcm_bytes_delivered, 0);
    assert!(output.snapshot().is_empty());
    no_retry(&listener).await;
}

#[test]
fn endpoint_name_region_origin_and_hosted_address_validation_is_local() {
    for name in ["A".to_owned(), "a".repeat(63), "a--b".into(), "9-Z".into()] {
        assert!(SageMakerConfig::new("us-east-1", name, credentials()).is_ok());
    }
    for name in [
        "", "-a", "a-", "a/b", "a%2Fb", "a?b", "a#b", "a@b", "a.b", "a_b", "a\nb", "é",
    ] {
        assert!(
            SageMakerConfig::new("us-east-1", name, credentials()).is_err(),
            "accepted {name:?}"
        );
    }
    assert!(SageMakerConfig::new("us-east-1", "a".repeat(64), credentials()).is_err());
    for region in [
        "",
        "https://evil.invalid",
        "us-east-1/path",
        "us-east-1\n",
        "US-EAST-1",
    ] {
        assert!(
            SageMakerConfig::new(region, ENDPOINT, credentials()).is_err(),
            "accepted {region:?}"
        );
    }
    for origin in [
        "http://localhost:1234",
        "http://192.0.2.1",
        "https://example.com",
        "http://127.0.0.1:1234/path",
        "http://user@127.0.0.1:1234",
        "http://127.0.0.1:1234/?q=1",
        "http://127.0.0.1:1234/#fragment",
    ] {
        assert!(
            SageMakerConfig::new("us-east-1", ENDPOINT, credentials())
                .unwrap()
                .with_loopback_endpoint(origin)
                .is_err(),
            "accepted {origin}"
        );
    }
    assert!(SageMakerConfig::new("us-east-1", ENDPOINT, credentials())
        .unwrap()
        .with_connect_address("192.0.2.1:80".parse().unwrap())
        .is_err());
}

struct SetupWitness {
    called: AtomicBool,
}
#[async_trait]
impl FrameProcessor for SetupWitness {
    fn name(&self) -> &str {
        "PreflightOutputWitness"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.called.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn all_finite_input_and_settings_are_validated_before_socket_or_output_setup() {
    let listener = listener().await;
    for bound in [
        "text",
        "request",
        "count",
        "pending-bytes",
        "serialized-pending",
    ] {
        let mut limits = limits();
        let inputs = match bound {
            "text" => {
                limits.max_text_bytes = 8;
                vec![utterance("valid"), utterance("far too long")]
            }
            "request" => {
                limits.max_request_bytes = 128;
                vec![utterance("valid"), utterance(&"x".repeat(200))]
            }
            "count" => {
                limits.max_utterances = 1;
                vec![utterance("first"), utterance("second")]
            }
            "serialized-pending" => {
                limits.max_pending_bytes = 64;
                vec![utterance("A")]
            }
            _ => {
                limits.max_pending_bytes = 64;
                vec![utterance(&"x".repeat(100))]
            }
        };
        let output = Arc::new(SetupWitness {
            called: AtomicBool::new(false),
        });
        let candidate = config(&listener).with_limits(limits);
        if let Ok(candidate) = candidate {
            assert!(
                SageMakerTtsApp::new(candidate, inputs, output.clone()).is_err(),
                "accepted invalid {bound}"
            );
        }
        assert!(
            !output.called.load(Ordering::SeqCst),
            "preflight executed output setup"
        );
    }
    for settings in [
        SynthesisSettings {
            sample_rate_hz: 0,
            ..Default::default()
        },
        SynthesisSettings {
            voice_name: String::new(),
            ..Default::default()
        },
        SynthesisSettings {
            language_code: String::new(),
            ..Default::default()
        },
    ] {
        assert!(config(&listener).with_settings(settings).is_err());
    }
    no_retry(&listener).await;
}

#[tokio::test]
async fn drop_before_start_has_no_network_or_output_setup_effect() {
    let listener = listener().await;
    let output = Arc::new(SetupWitness {
        called: AtomicBool::new(false),
    });
    let app = SageMakerTtsApp::new(
        config(&listener),
        vec![utterance("not started")],
        output.clone(),
    )
    .unwrap();
    drop(app);
    assert!(!output.called.load(Ordering::SeqCst));
    no_retry(&listener).await;
}

#[tokio::test]
async fn malformed_status_mime_header_and_detectable_container_matrix_fails_without_retry() {
    let cases: Vec<(&str, String, Vec<u8>)> = vec![
        ("model-error-424", "HTTP/1.1 424 Model Error\r\nContent-Type: application/json\r\n".into(), b"private-provider-body".to_vec()),
        ("not-ready-429", "HTTP/1.1 429 Not Ready\r\nContent-Type: application/json\r\n".into(), b"private-provider-body".to_vec()),
        ("redirect", "HTTP/1.1 302 Found\r\nLocation: http://192.0.2.1/never\r\nContent-Type: application/octet-stream\r\n".into(), vec![1, 2]),
        ("missing-mime", "HTTP/1.1 200 OK\r\n".into(), vec![1, 2]),
        ("json-mime", "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n".into(), b"{}".to_vec()),
        ("wav-mime", "HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\n".into(), vec![1, 2]),
        ("eventstream-mime", "HTTP/1.1 200 OK\r\nContent-Type: application/vnd.amazon.eventstream\r\n".into(), vec![1, 2]),
        ("duplicate-mime", "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Type: application/octet-stream\r\n".into(), vec![1, 2]),
        ("conflicting-mime", "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Type: application/json\r\n".into(), vec![1, 2]),
        ("duplicate-length", "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 99\r\n".into(), vec![1, 2]),
        ("invalid-length", "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: not-a-number\r\n".into(), vec![1, 2]),
        ("wav-container", "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n".into(), b"RIFF\x10\x00\x00\x00WAVEfmt ".to_vec()),
    ];
    for (label, head, body) in cases {
        let listener = listener().await;
        let config = config(&listener);
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            read_request(&mut socket).await;
            let mut wire = format!(
                "{head}Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .into_bytes();
            wire.extend(body);
            let _ = socket.write_all(&wire).await;
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = recording();
        let app = SageMakerTtsApp::new(
            config,
            vec![
                utterance("private-request-text"),
                utterance("must not retry"),
            ],
            output.clone(),
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        let report = finish(&app).await;
        assert!(!report.is_success(), "accepted {label}: {report:?}");
        joined(&report);
        assert_eq!(report.progress.completed_at_eof, 0, "{label}");
        if label != "wav-container" {
            assert_eq!(report.progress.pcm_bytes_delivered, 0, "{label}");
            assert!(output.pcm().is_empty(), "{label}");
        }
        let diagnostic = format!("{report:?}");
        for secret in [
            "private-provider-body",
            "private-request-text",
            SECRET,
            TOKEN,
        ] {
            assert!(!diagnostic.contains(secret), "diagnostic leaks {label}");
        }
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

#[tokio::test]
async fn empty_length_empty_chunked_and_trailing_partial_sample_are_failed_not_skipped() {
    for (body, chunked) in [
        (vec![], false),
        (vec![], true),
        (vec![1], true),
        (vec![1, 2, 3], true),
    ] {
        let listener = listener().await;
        let config = config(&listener);
        let sent = body.clone();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            read_request(&mut socket).await;
            if chunked {
                response(&mut socket, "application/octet-stream", &sent, 1).await;
            } else {
                content_length_response(&mut socket, &sent).await;
            }
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = recording();
        let app = SageMakerTtsApp::new(config, vec![utterance("real input")], output.clone())
            .unwrap()
            .start()
            .await
            .unwrap();
        let report = finish(&app).await;
        assert!(!report.is_success(), "accepted {body:?}: {report:?}");
        joined(&report);
        assert_eq!(report.progress.skipped, 0);
        assert_eq!(report.progress.admitted, 1);
        assert_eq!(report.progress.completed_at_eof, 0);
        assert_eq!(report.progress.failed, 1);
        assert_eq!(output.pcm(), body[..body.len() / 2 * 2]);
        let events = output.snapshot();
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Event::Start(_)))
                .count(),
            events
                .iter()
                .filter(|event| matches!(event, Event::Stop(_)))
                .count(),
            "failed invocation did not close its TTS context"
        );
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

#[tokio::test]
async fn truncated_content_length_and_chunked_bodies_never_count_as_successful_eof() {
    for wire in [
        b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 100\r\nConnection: close\r\n\r\n\x01\x02".as_slice(),
        b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\n\x01\x02",
    ] {
        let listener = listener().await;
        let config = config(&listener);
        let wire = wire.to_vec();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept()).await.expect("peer accept timed out").unwrap();
            read_request(&mut socket).await;
            socket.write_all(&wire).await.unwrap(); socket.shutdown().await.unwrap();
            peer_closed(&mut socket).await; no_retry(&listener).await;
        });
        let app = SageMakerTtsApp::new(config, vec![utterance("truncated")], recording()).unwrap().start().await.unwrap();
        let report = finish(&app).await;
        assert!(!report.is_success(), "{report:?}"); joined(&report);
        assert_eq!(report.progress.completed_at_eof, 0);
        assert_eq!(report.progress.failed, 1);
        timeout(WAIT, peer).await.expect("peer join timed out").unwrap();
    }
}

#[tokio::test]
async fn response_output_header_and_frame_budgets_are_enforced() {
    for kind in ["response", "output", "header", "frame"] {
        let listener = listener().await;
        let mut bounded = limits();
        match kind {
            "response" => bounded.max_response_bytes = 16,
            "output" => bounded.max_output_bytes = 16,
            "header" => bounded.max_header_bytes = 8192,
            _ => bounded.max_frame_bytes = 6,
        }
        let config = config(&listener).with_limits(bounded).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            read_request(&mut socket).await;
            if kind == "header" {
                let wire = format!("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nX-Large: {}\r\nContent-Length: 2\r\nConnection: close\r\n\r\nab", "x".repeat(20_000));
                let _ = socket.write_all(wire.as_bytes()).await;
            } else {
                content_length_response(&mut socket, &[42; 40]).await;
            }
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = recording();
        let app = SageMakerTtsApp::new(config, vec![utterance("bounded")], output.clone())
            .unwrap()
            .start()
            .await
            .unwrap();
        let report = finish(&app).await;
        joined(&report);
        if kind == "frame" {
            assert!(
                report.is_success(),
                "frame limit should split whole samples: {report:?}"
            );
            output.assert_complete(&[42; 40], 24_000, 1);
            assert!(output
                .snapshot()
                .iter()
                .all(|event| !matches!(event, Event::Audio { bytes, .. } if bytes.len() > 6)));
        } else {
            assert!(!report.is_success(), "unbounded {kind}: {report:?}");
        }
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

#[tokio::test]
async fn total_output_budget_applies_across_utterances_not_per_connection() {
    let listener = listener().await;
    let mut bounded = limits();
    bounded.max_output_bytes = 6;
    let config = config(&listener).with_limits(bounded).unwrap();
    let peer = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            read_request(&mut socket).await;
            response(&mut socket, "application/octet-stream", &[1, 2, 3, 4], 4).await;
            peer_closed(&mut socket).await;
        }
        no_retry(&listener).await;
    });
    let output = recording();
    let app = SageMakerTtsApp::new(
        config,
        vec![utterance("one"), utterance("two")],
        output.clone(),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let report = finish(&app).await;
    assert!(
        !report.is_success(),
        "session output budget reset: {report:?}"
    );
    joined(&report);
    assert_eq!(report.progress.completed_at_eof, 1);
    assert!(report.progress.pcm_bytes_delivered <= 6);
    assert!(output.pcm().len() <= 6);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

#[tokio::test]
async fn header_idle_and_total_deadlines_close_actual_socket_without_retry() {
    for mode in ["headers", "idle", "total"] {
        let listener = listener().await;
        let mut bounded = limits();
        match mode {
            "headers" => bounded.write_timeout = Duration::from_millis(100),
            "idle" => bounded.idle_timeout = Duration::from_millis(100),
            _ => bounded.total_timeout = Duration::from_millis(100),
        }
        let config = config(&listener).with_limits(bounded).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            read_request(&mut socket).await;
            if mode != "headers" {
                head(&mut socket, "application/octet-stream").await;
            }
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let app = SageMakerTtsApp::new(config, vec![utterance("timeout")], recording())
            .unwrap()
            .start()
            .await
            .unwrap();
        let report = finish(&app).await;
        assert!(!report.is_success(), "{mode}: {report:?}");
        joined(&report);
        assert_eq!(report.progress.completed_at_eof, 0);
        assert!(
            format!("{:?}", report.primary).contains("Timeout"),
            "{mode}: {report:?}"
        );
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

#[tokio::test]
async fn peer_disconnect_after_request_is_failed_without_hidden_replay() {
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        read_request(&mut socket).await;
        drop(socket);
        no_retry(&listener).await;
    });
    let app = SageMakerTtsApp::new(
        config,
        vec![utterance("uncertain remote execution")],
        recording(),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let report = finish(&app).await;
    joined(&report);
    assert!(!report.is_success());
    assert_eq!(report.progress.completed_at_eof, 0);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

struct ControlledOutput {
    entered: Notify,
    permits: Semaphore,
    fail: bool,
    panic: bool,
    gate_stop: bool,
    cleaned: AtomicBool,
    output: Arc<Recording>,
}
impl ControlledOutput {
    fn new(fail: bool, panic: bool, gate_stop: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            permits: Semaphore::new(0),
            fail,
            panic,
            gate_stop,
            cleaned: AtomicBool::new(false),
            output: recording(),
        })
    }
}
#[async_trait]
impl FrameProcessor for ControlledOutput {
    fn name(&self) -> &str {
        "ControlledSageMakerOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let gated = if self.gate_stop {
            matches!(frame.data, FrameData::TtsStopped(_))
        } else {
            matches!(frame.data, FrameData::TtsAudio(_))
        };
        if gated {
            self.entered.notify_one();
            assert!(!self.panic, "synthetic output receiver panic");
            if self.fail {
                return Err(ProcessorError::fatal("synthetic output receiver failure"));
            }
            self.permits.acquire().await.unwrap().forget();
        }
        self.output.process(frame, direction, ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn queued_pcm_and_final_stop_are_not_complete_before_real_output_acknowledgment() {
    for gate_stop in [false, true] {
        let listener = listener().await;
        let mut bounded = limits();
        bounded.output_timeout = Duration::from_secs(2);
        let config = config(&listener).with_limits(bounded).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            read_request(&mut socket).await;
            content_length_response(&mut socket, &[7, 8]).await;
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = ControlledOutput::new(false, false, gate_stop);
        let app = SageMakerTtsApp::new(config, vec![utterance("held output")], output.clone())
            .unwrap()
            .start()
            .await
            .unwrap();
        timeout(WAIT, output.entered.notified()).await.unwrap();
        assert!(timeout(Duration::from_millis(100), app.finish())
            .await
            .is_err());
        let progress = app.completion().progress();
        assert_eq!(
            progress.completed_at_eof, 0,
            "completion overtook final acknowledgment"
        );
        assert_eq!(progress.pcm_bytes_admitted, 2);
        assert_eq!(progress.pcm_bytes_delivered, if gate_stop { 2 } else { 0 });
        assert_eq!(output.output.pcm().len(), if gate_stop { 2 } else { 0 });
        output.permits.add_permits(1);
        let report = finish(&app).await;
        assert!(report.is_success(), "{report:?}");
        joined(&report);
        assert_eq!(report.progress.pcm_bytes_delivered, 2);
        output.output.assert_complete(&[7, 8], 24_000, 1);
        assert!(output.cleaned.load(Ordering::SeqCst));
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

#[tokio::test]
async fn cancel_stop_and_drop_release_response_headers_and_partial_pcm_owners() {
    for phase in ["headers", "partial-pcm"] {
        for action in ["cancel", "stop", "drop"] {
            let listener = listener().await;
            let mut bounded = limits();
            bounded.max_pending_invocations = 1;
            let config = config(&listener).with_limits(bounded).unwrap();
            let (ready_tx, ready_rx) = oneshot::channel();
            let peer = tokio::spawn(async move {
                let (mut socket, _) = timeout(WAIT, listener.accept())
                    .await
                    .expect("peer accept timed out")
                    .unwrap();
                read_request(&mut socket).await;
                if phase == "partial-pcm" {
                    head(&mut socket, "application/octet-stream").await;
                    chunk(&mut socket, &[1, 2, 3]).await;
                }
                ready_tx.send(()).unwrap();
                peer_closed(&mut socket).await;
                no_retry(&listener).await;
            });
            let app = SageMakerTtsApp::new(
                config,
                vec![utterance("held first"), utterance("queued must not start")],
                recording(),
            )
            .unwrap()
            .start()
            .await
            .unwrap();
            timeout(WAIT, ready_rx)
                .await
                .expect("peer coordination timed out")
                .unwrap();
            let completion = app.completion();
            match action {
                "cancel" => app.cancel(),
                "stop" => app.stop(),
                _ => {}
            }
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
            assert_eq!(
                report.progress.admitted, 1,
                "capacity-wait feeder admitted queued work"
            );
            assert_eq!(report.progress.completed_at_eof, 0);
            assert_eq!(report.progress.cancelled_or_aborted, 1);
            if action == "stop" {
                assert_eq!(report.state, WorkerState::Stopped, "{report:?}");
            }
            timeout(WAIT, peer)
                .await
                .expect("peer join timed out")
                .unwrap();
        }
    }
}

#[tokio::test]
async fn held_output_cancel_stop_drop_failure_panic_and_timeout_join_actual_owners() {
    for mode in ["cancel", "stop", "drop", "failure", "panic", "timeout"] {
        let listener = listener().await;
        let mut bounded = limits();
        bounded.max_pending_invocations = 1;
        let config = config(&listener).with_limits(bounded).unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            read_request(&mut socket).await;
            head(&mut socket, "application/octet-stream").await;
            chunk(&mut socket, &[1, 2]).await;
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = ControlledOutput::new(mode == "failure", mode == "panic", false);
        let app = SageMakerTtsApp::new(
            config,
            vec![utterance("blocked"), utterance("queued")],
            output.clone(),
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        timeout(WAIT, output.entered.notified()).await.unwrap();
        let completion = app.completion();
        match mode {
            "cancel" => app.cancel(),
            "stop" => app.stop(),
            _ => {}
        }
        let report = if mode == "drop" {
            drop(app);
            timeout(WAIT, completion.wait()).await.unwrap()
        } else {
            let report = finish(&app).await;
            drop(app);
            report
        };
        assert!(!report.is_success(), "{mode}: {report:?}");
        joined(&report);
        assert_eq!(report.progress.pcm_bytes_admitted, 2);
        assert_eq!(report.progress.pcm_bytes_delivered, 0);
        assert_eq!(report.progress.completed_at_eof, 0);
        assert!(output.output.pcm().is_empty());
        assert!(
            output.cleaned.load(Ordering::SeqCst),
            "cleanup did not run: {mode}"
        );
        if mode == "stop" {
            assert_eq!(report.state, WorkerState::Stopped, "{report:?}");
        }
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

#[tokio::test]
async fn canceled_retried_and_concurrent_completion_waiters_retain_real_join_handle() {
    let listener = listener().await;
    let config = config(&listener);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        read_request(&mut socket).await;
        ready_tx.send(()).unwrap();
        timeout(WAIT, release_rx)
            .await
            .expect("peer coordination timed out")
            .unwrap();
        response(&mut socket, "application/octet-stream", &[1, 2], 1).await;
        peer_closed(&mut socket).await;
    });
    let app = SageMakerTtsApp::new(config, vec![utterance("retained completion")], recording())
        .unwrap()
        .start()
        .await
        .unwrap();
    timeout(WAIT, ready_rx)
        .await
        .expect("peer coordination timed out")
        .unwrap();
    let completion = app.completion();
    for _ in 0..2 {
        assert!(timeout(Duration::from_millis(30), completion.wait())
            .await
            .is_err());
    }
    let a = completion.clone();
    let b = completion.clone();
    let one = tokio::spawn(async move { a.wait().await });
    let two = tokio::spawn(async move { b.wait().await });
    release_tx.send(()).unwrap();
    let first = timeout(WAIT, one).await.unwrap().unwrap();
    let second = timeout(WAIT, two).await.unwrap().unwrap();
    assert!(first.is_success(), "{first:?}");
    assert!(second.is_success(), "{second:?}");
    joined(&first);
    joined(&second);
    assert_eq!(first.progress, second.progress);
    assert_eq!(completion.wait().await.progress, first.progress);
    drop(app);
    assert_eq!(completion.wait().await.progress, first.progress);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
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
        "SageMakerOutputLifetimeWitness"
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
async fn dropping_running_and_all_completion_observers_reaps_pipeline_output_and_peer() {
    let listener = listener().await;
    let config = config(&listener);
    let (ready_tx, ready_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
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
    let app = SageMakerTtsApp::new(config, vec![utterance("held")], output)
        .unwrap()
        .start()
        .await
        .unwrap();
    let completion = app.completion();
    let second_completion = completion.clone();
    timeout(WAIT, ready_rx)
        .await
        .expect("peer coordination timed out")
        .unwrap();
    drop(completion);
    drop(second_completion);
    drop(app);
    timeout(WAIT, async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all-owner drop retained output owner");
    assert!(weak.upgrade().is_none());
    assert!(cleaned.load(Ordering::SeqCst));
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

#[tokio::test]
async fn cancellation_during_withheld_tls_client_hello_joins_only_local_test_peer() {
    let listener = listener().await;
    let config = SageMakerConfig::new("us-east-1", ENDPOINT, credentials())
        .unwrap()
        .with_loopback_endpoint(&format!("https://{}", listener.local_addr().unwrap()))
        .unwrap()
        .with_limits(limits())
        .unwrap();
    let (ready_tx, ready_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let mut header = [0; 5];
        timeout(WAIT, socket.read_exact(&mut header))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(header[0], 22, "actual TLS handshake record required");
        let size = u16::from_be_bytes([header[3], header[4]]) as usize;
        assert!((1..=16_384).contains(&size));
        let mut hello = vec![0; size];
        socket.read_exact(&mut hello).await.unwrap();
        assert_eq!(hello[0], 1, "actual ClientHello required");
        ready_tx.send(()).unwrap();
        peer_closed(&mut socket).await;
    });
    let app = SageMakerTtsApp::new(config, vec![utterance("withheld TLS")], recording())
        .unwrap()
        .start()
        .await
        .unwrap();
    timeout(WAIT, ready_rx)
        .await
        .expect("peer coordination timed out")
        .unwrap();
    app.cancel();
    let report = finish(&app).await;
    joined(&report);
    assert!(!report.is_success());
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

struct FailingOutput {
    setup_failure: bool,
    process_failure: bool,
    cleanup_failure: bool,
    cleanup_timeout: bool,
    setup_called: AtomicBool,
}
#[async_trait]
impl FrameProcessor for FailingOutput {
    fn name(&self) -> &str {
        "SageMakerOutputFailures"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.setup_called.store(true, Ordering::SeqCst);
        if self.setup_failure {
            Err(ProcessorError::fatal("synthetic setup failure"))
        } else {
            Ok(())
        }
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.process_failure && matches!(frame.data, FrameData::TtsAudio(_)) {
            return Err(ProcessorError::fatal("synthetic primary output"));
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if self.cleanup_timeout {
            std::future::pending().await
        } else if self.cleanup_failure {
            Err(ProcessorError::fatal("synthetic secondary cleanup"))
        } else {
            Ok(())
        }
    }
}
#[tokio::test]
async fn output_setup_failure_prevents_network_admission() {
    let listener = listener().await;
    let output = Arc::new(FailingOutput {
        setup_failure: true,
        process_failure: false,
        cleanup_failure: false,
        cleanup_timeout: false,
        setup_called: AtomicBool::new(false),
    });
    let app = SageMakerTtsApp::new(
        config(&listener),
        vec![utterance("not admitted")],
        output.clone(),
    )
    .unwrap();
    let running = app.start().await.unwrap();
    let report = finish(&running).await;
    assert!(!report.is_success(), "{report:?}");
    joined(&report);
    assert_eq!(report.progress.admitted, 0);
    assert!(output.setup_called.load(Ordering::SeqCst));
    no_retry(&listener).await;
}

#[tokio::test]
async fn primary_provider_or_output_failure_survives_secondary_cleanup_failure_or_timeout() {
    for process_failure in [false, true] {
        for cleanup_timeout in [false, true] {
            let listener = listener().await;
            let mut bounded = limits();
            bounded.shutdown_timeout = Duration::from_millis(100);
            let config = config(&listener).with_limits(bounded).unwrap();
            let peer = tokio::spawn(async move {
                let (mut socket, _) = timeout(WAIT, listener.accept())
                    .await
                    .expect("peer accept timed out")
                    .unwrap();
                read_request(&mut socket).await;
                if process_failure {
                    content_length_response(&mut socket, &[1, 2]).await;
                } else {
                    content_length_response(&mut socket, &[]).await;
                }
                peer_closed(&mut socket).await;
            });
            let output = Arc::new(FailingOutput {
                setup_failure: false,
                process_failure,
                cleanup_failure: true,
                cleanup_timeout,
                setup_called: AtomicBool::new(false),
            });
            let app = SageMakerTtsApp::new(config, vec![utterance("primary and cleanup")], output)
                .unwrap()
                .start()
                .await
                .unwrap();
            let report = finish(&app).await;
            assert!(!report.is_success(), "{report:?}");
            joined(&report);
            assert!(report.primary.is_some());
            let mut expected_cleanup = vec![format!(
                "SageMaker caller output cleanup {}",
                if cleanup_timeout {
                    "deadline elapsed"
                } else {
                    "failed"
                }
            )];
            if process_failure {
                // The fatal audio callback also rejects the subsequent lifecycle
                // close acknowledgment; retain both distinct secondary failures.
                expected_cleanup.push("SageMaker caller output failed".into());
            }
            assert_eq!(report.cleanup_errors, expected_cleanup, "{report:?}");
            assert_eq!(report.forced_cleanup, cleanup_timeout, "{report:?}");
            let primary = format!("{:?}", report.primary);
            assert!(
                primary.contains(if process_failure {
                    "Output"
                } else {
                    "Protocol"
                }),
                "{report:?}"
            );
            timeout(WAIT, peer)
                .await
                .expect("peer join timed out")
                .unwrap();
        }
    }
}

#[tokio::test]
async fn successful_pcm_does_not_hide_output_cleanup_failure() {
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        read_request(&mut socket).await;
        content_length_response(&mut socket, &[1, 2]).await;
        peer_closed(&mut socket).await;
    });
    let output = Arc::new(FailingOutput {
        setup_failure: false,
        process_failure: false,
        cleanup_failure: true,
        cleanup_timeout: false,
        setup_called: AtomicBool::new(false),
    });
    let app = SageMakerTtsApp::new(config, vec![utterance("cleanup matters")], output)
        .unwrap()
        .start()
        .await
        .unwrap();
    let report = finish(&app).await;
    assert!(!report.is_success(), "{report:?}");
    joined(&report);
    assert_eq!(report.progress.pcm_bytes_delivered, 2);
    assert_eq!(report.cleanup_errors.len(), 1);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

#[tokio::test]
async fn actual_processor_interruption_releases_old_socket_and_queued_work_then_accepts_fresh() {
    let listener = listener().await;
    let config = config(&listener);
    let (ready_tx, ready_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let old = read_request(&mut first).await;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&old.body).unwrap()["text"],
            "old"
        );
        ready_tx.send(()).unwrap();
        peer_closed(&mut first).await;
        let (mut second, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let fresh = read_request(&mut second).await;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fresh.body).unwrap()["text"],
            "fresh"
        );
        response(&mut second, "application/octet-stream", &[8, 9], 1).await;
        peer_closed(&mut second).await;
        no_retry(&listener).await;
    });
    let provider = Arc::new(SageMakerProcessor::new(
        SageMakerClient::new(config).unwrap(),
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
    send(&worker, speak("old")).await;
    timeout(WAIT, ready_rx)
        .await
        .expect("peer coordination timed out")
        .unwrap();
    send(&worker, speak("stale queued")).await;
    timeout(WAIT, async {
        while provider.progress().admitted < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    send(&worker, FrameData::Interruption).await;
    send(&worker, speak("fresh")).await;
    worker.end().await.unwrap();
    drain(&mut worker).await;
    let report = timeout(WAIT, worker.join())
        .await
        .expect("pipeline join timed out")
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(output.pcm(), [8, 9]);
    let progress = provider.progress();
    assert_eq!(progress.admitted, 3);
    assert_eq!(progress.owners_released, 3);
    assert_eq!(progress.completed_at_eof, 1);
    assert_eq!(progress.cancelled_or_aborted, 2);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

struct DroppedFrameWitness(Arc<AtomicBool>);
impl Drop for DroppedFrameWitness {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
struct InterruptibleOutput {
    first: AtomicBool,
    entered: Notify,
    cancelled: Arc<AtomicBool>,
    output: Arc<Recording>,
}
#[async_trait]
impl FrameProcessor for InterruptibleOutput {
    fn name(&self) -> &str {
        "InterruptibleSageMakerOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::TtsAudio(_)) && !self.first.swap(true, Ordering::SeqCst)
        {
            let _lifetime = DroppedFrameWitness(self.cancelled.clone());
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        self.output.process(frame, direction, ctx).await
    }
}
#[tokio::test]
async fn interruption_cancels_blocked_stale_output_without_crediting_it_to_fresh_audio() {
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        let (mut first, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        read_request(&mut first).await;
        head(&mut first, "application/octet-stream").await;
        chunk(&mut first, &[1, 2]).await;
        peer_closed(&mut first).await;
        let (mut second, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let request = read_request(&mut second).await;
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["text"],
            "fresh"
        );
        content_length_response(&mut second, &[8, 9]).await;
        peer_closed(&mut second).await;
    });
    let provider = Arc::new(SageMakerProcessor::new(
        SageMakerClient::new(config).unwrap(),
    ));
    let output = Arc::new(InterruptibleOutput {
        first: AtomicBool::new(false),
        entered: Notify::new(),
        cancelled: Arc::new(AtomicBool::new(false)),
        output: recording(),
    });
    let mut worker = Pipeline::new(vec![
        provider.ingress_guard(),
        provider.clone(),
        provider.output_guard(output.clone()),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(&worker, speak("old")).await;
    timeout(WAIT, output.entered.notified()).await.unwrap();
    send(&worker, FrameData::Interruption).await;
    timeout(WAIT, async {
        while !output.cancelled.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    send(&worker, speak("fresh")).await;
    worker.end().await.unwrap();
    drain(&mut worker).await;
    let report = timeout(WAIT, worker.join())
        .await
        .expect("pipeline join timed out")
        .unwrap();
    assert!(report.errors.is_empty());
    assert_eq!(output.output.pcm(), [8, 9]);
    let progress = provider.progress();
    assert_eq!(progress.pcm_bytes_admitted, 4);
    assert_eq!(progress.pcm_bytes_delivered, 2);
    assert_eq!(progress.completed_at_eof, 1);
    assert_eq!(progress.owners_released, 2);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

#[tokio::test]
async fn settings_updates_affect_only_future_admissions_not_queued_snapshots() {
    let listener = listener().await;
    let config = config(&listener);
    let host = listener.local_addr().unwrap().to_string();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        let request = read_request(&mut first).await;
        verify_request(
            &request,
            &host,
            "first",
            &SynthesisSettings::default(),
            ACCESS,
            SECRET,
            Some(TOKEN),
        );
        ready_tx.send(()).unwrap();
        timeout(WAIT, release_rx)
            .await
            .expect("peer coordination timed out")
            .unwrap();
        content_length_response(&mut first, &[1, 2]).await;
        peer_closed(&mut first).await;
        for (text, settings) in [
            ("queued old settings", SynthesisSettings::default()),
            (
                "future new settings",
                SynthesisSettings {
                    voice_name: "Changed-Voice".into(),
                    language_code: "fr-FR".into(),
                    sample_rate_hz: 24_000,
                },
            ),
        ] {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            let request = read_request(&mut socket).await;
            verify_request(
                &request,
                &host,
                text,
                &settings,
                ACCESS,
                SECRET,
                Some(TOKEN),
            );
            content_length_response(&mut socket, &[3, 4]).await;
            peer_closed(&mut socket).await;
        }
    });
    let provider = Arc::new(SageMakerProcessor::new(
        SageMakerClient::new(config).unwrap(),
    ));
    let output = recording();
    let mut worker = Pipeline::new(vec![
        provider.ingress_guard(),
        provider.clone(),
        provider.output_guard(output.clone()),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(&worker, speak("first")).await;
    timeout(WAIT, ready_rx)
        .await
        .expect("peer coordination timed out")
        .unwrap();
    send(&worker, speak("queued old settings")).await;
    timeout(WAIT, async {
        while provider.progress().admitted < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            delta: Some(TtsSettings {
                voice: Setting::Value("Changed-Voice".into()),
                language: Setting::Value("fr-FR".into()),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await;
    send(&worker, speak("future new settings")).await;
    timeout(WAIT, async {
        while provider.progress().admitted < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    release_tx.send(()).unwrap();
    worker.end().await.unwrap();
    drain(&mut worker).await;
    let report = timeout(WAIT, worker.join())
        .await
        .expect("pipeline join timed out")
        .unwrap();
    assert!(report.errors.is_empty());
    output.assert_complete(&[1, 2, 3, 4, 3, 4], 24_000, 3);
    assert_eq!(provider.progress().completed_at_eof, 3);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

#[tokio::test]
async fn malformed_settings_are_atomic_and_cannot_replace_voice_rate_endpoint_or_credentials() {
    for kind in [
        "unknown",
        "rate",
        "endpoint",
        "credentials",
        "null",
        "model",
        "mixed",
        "bad-language",
    ] {
        let listener = listener().await;
        let config = config(&listener);
        let host = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            let request = read_request(&mut socket).await;
            verify_request(
                &request,
                &host,
                "after rejected update",
                &SynthesisSettings::default(),
                ACCESS,
                SECRET,
                Some(TOKEN),
            );
            content_length_response(&mut socket, &[1, 2]).await;
            peer_closed(&mut socket).await;
        });
        let provider = Arc::new(SageMakerProcessor::new(
            SageMakerClient::new(config).unwrap(),
        ));
        let mut worker = Pipeline::new(vec![
            provider.ingress_guard(),
            provider.clone(),
            provider.output_guard(recording()),
        ])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let mut update = ServiceUpdateSettingsFrame::<TtsSettings>::default();
        update
            .settings
            .insert("voice".into(), serde_json::json!("Must-Not-Commit"));
        match kind {
            "null" => {
                update
                    .settings
                    .insert("language".into(), serde_json::Value::Null);
            }
            "bad-language" => {
                update
                    .settings
                    .insert("language".into(), serde_json::json!("not valid"));
            }
            "model" => {
                update.settings.clear();
                update.delta = Some(TtsSettings {
                    voice: Setting::Value("Must-Not-Commit".into()),
                    base: pipecat_core::ServiceSettings {
                        model: Setting::Value("other".into()),
                        ..Default::default()
                    },
                    ..Default::default()
                });
            }
            "mixed" => {
                update.delta = Some(TtsSettings {
                    language: Setting::Value("fr-FR".into()),
                    ..Default::default()
                });
            }
            field => {
                update
                    .settings
                    .insert(field.into(), serde_json::json!("forbidden"));
            }
        }
        send(&worker, FrameData::TtsUpdateSettings(Box::new(update))).await;
        send(&worker, speak("after rejected update")).await;
        worker.end().await.unwrap();
        drain(&mut worker).await;
        let _ = timeout(WAIT, worker.join())
            .await
            .expect("pipeline join timed out");
        assert!(worker.total_errors() > 0, "invalid {kind} accepted");
        assert_eq!(provider.progress().completed_at_eof, 1, "{kind}");
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

#[tokio::test]
async fn ordinary_text_and_upstream_speak_are_forwarded_without_synthesis_or_history_claim() {
    let listener = listener().await;
    let provider = Arc::new(SageMakerProcessor::new(
        SageMakerClient::new(config(&listener)).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![provider.ingress_guard(), provider.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(
        &worker,
        FrameData::Text(pipecat_core::TextFrame::new("ordinary")),
    )
    .await;
    worker
        .sender()
        .send(
            Frame::new(speak("upstream must not invoke")),
            Direction::Upstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut ordinary = false;
    let mut upstream = false;
    timeout(WAIT, async {
        while let Some(frame) = worker.recv().await {
            match frame.frame.data {
                FrameData::Text(text) if text.text == "ordinary" => {
                    assert_eq!(frame.direction, Direction::Downstream);
                    ordinary = true;
                }
                FrameData::TtsSpeak(speak) => {
                    assert_eq!(frame.direction, Direction::Upstream);
                    assert_eq!(speak.text, "upstream must not invoke");
                    upstream = true;
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    timeout(WAIT, worker.join())
        .await
        .expect("pipeline join timed out")
        .unwrap();
    assert!(ordinary && upstream);
    assert_eq!(provider.progress().admitted, 0);
    no_retry(&listener).await;
}

#[tokio::test]
async fn merely_constructed_but_uninstalled_output_guard_rejects_before_network() {
    let listener = listener().await;
    let provider = Arc::new(SageMakerProcessor::new(
        SageMakerClient::new(config(&listener)).unwrap(),
    ));
    let _unused = provider.output_guard(recording());
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
    send(&worker, speak("must not send")).await;
    drain(&mut worker).await;
    let _ = timeout(WAIT, worker.join())
        .await
        .expect("pipeline join timed out");
    assert!(worker.total_errors() > 0);
    assert_eq!(provider.progress().admitted, 0);
    no_retry(&listener).await;
}

#[tokio::test]
async fn stalled_large_request_body_write_is_cancelled_or_times_out_without_replay() {
    use tokio::net::{TcpSocket, TcpStream};
    for cancelled in [true, false] {
        // Reduce only this synthetic peer's receive window, never host settings.
        let listen_socket = TcpSocket::new_v4().unwrap();
        listen_socket.set_recv_buffer_size(1024).unwrap();
        listen_socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listen_socket.listen(1).unwrap();
        let mut bounded = limits();
        bounded.max_text_bytes = 1_048_576;
        bounded.max_request_bytes = 6_291_456;
        bounded.max_pending_bytes = 16 * 1024 * 1024;
        bounded.write_timeout = Duration::from_millis(350);
        bounded.total_timeout = Duration::from_secs(10);
        let config = config(&listener).with_limits(bounded).unwrap();
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            let standard = socket.into_std().unwrap();
            let options = TcpSocket::from_std_stream(standard.try_clone().unwrap());
            let mut socket = TcpStream::from_std(standard).unwrap();
            let mut header = Vec::new();
            timeout(WAIT, async {
                while !header.ends_with(b"\r\n\r\n") {
                    assert!(header.len() < 16_384);
                    header.push(socket.read_u8().await.unwrap());
                }
            })
            .await
            .unwrap();
            let declared: usize = std::str::from_utf8(&header)
                .unwrap()
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
            let mut first = [0; 256];
            socket.read_exact(&mut first).await.unwrap();
            ready_tx.send(()).unwrap();
            timeout(WAIT, release_rx)
                .await
                .expect("peer coordination timed out")
                .unwrap();
            // App owner has joined before buffered kernel bytes are drained.
            options.set_recv_buffer_size(4 * 1024 * 1024).unwrap();
            let mut delivered = first.len();
            let mut buffer = [0; 65_536];
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
                "fixture did not stall actual body: {delivered}/{declared}"
            );
            assert!(delivered > first.len());
            eprintln!("stalled-write cancelled={cancelled}: delivered={delivered}, declared={declared}; app joined before peer drain");
            drop(options);
            drop(socket);
            no_retry(&listener).await;
        });
        // JSON escape expansion gives ~3.9 MiB wire, bounded <1 MiB logical text.
        let text = format!("A{}", "\u{1}".repeat(650_000));
        let app = SageMakerTtsApp::new(config, vec![utterance(&text)], recording())
            .unwrap()
            .start()
            .await
            .unwrap();
        timeout(WAIT, ready_rx).await.unwrap().unwrap();
        if cancelled {
            tokio::time::sleep(Duration::from_millis(80)).await;
            app.cancel();
        }
        let report = finish(&app).await;
        assert!(!report.is_success(), "{report:?}");
        joined(&report);
        assert_eq!(report.progress.completed_at_eof, 0);
        if !cancelled {
            assert!(
                format!("{:?}", report.primary).contains("Timeout"),
                "{report:?}"
            );
        }
        release_tx.send(()).unwrap();
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

struct CleanupGate {
    entered: Notify,
    permits: Semaphore,
    output: Arc<Recording>,
}
#[async_trait]
impl FrameProcessor for CleanupGate {
    fn name(&self) -> &str {
        "SageMakerFinalCleanupGate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.output.process(frame, direction, ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.entered.notify_one();
        self.permits.acquire().await.unwrap().forget();
        Ok(())
    }
}
#[tokio::test]
async fn eof_and_final_pcm_ack_do_not_complete_before_real_output_cleanup_joins() {
    let listener = listener().await;
    let mut bounded = limits();
    bounded.shutdown_timeout = Duration::from_secs(2);
    let config = config(&listener).with_limits(bounded).unwrap();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        read_request(&mut socket).await;
        content_length_response(&mut socket, &[1, 2]).await;
        peer_closed(&mut socket).await;
    });
    let output = Arc::new(CleanupGate {
        entered: Notify::new(),
        permits: Semaphore::new(0),
        output: recording(),
    });
    let app = SageMakerTtsApp::new(
        config,
        vec![utterance("wait for real cleanup")],
        output.clone(),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    timeout(WAIT, output.entered.notified()).await.unwrap();
    assert_eq!(app.completion().progress().pcm_bytes_delivered, 2);
    assert!(timeout(Duration::from_millis(100), app.finish())
        .await
        .is_err());
    output.permits.add_permits(1);
    let report = finish(&app).await;
    assert!(report.is_success(), "{report:?}");
    joined(&report);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

#[tokio::test]
async fn fragmented_detectable_encoded_audio_prefix_fails_and_retains_only_actual_pcm_prefix() {
    for body in [b"RIFF0000WAVEfmt ".as_slice(), b"OggS0000", b"fLaC0000"] {
        let listener = listener().await;
        let config = config(&listener);
        let bytes = body.to_vec();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = timeout(WAIT, listener.accept())
                .await
                .expect("peer accept timed out")
                .unwrap();
            read_request(&mut socket).await;
            head(&mut socket, "application/octet-stream").await;
            for byte in bytes {
                // A recognisable encoded prefix may be rejected before all bytes.
                let wire = [b'1', b'\r', b'\n', byte, b'\r', b'\n'];
                if socket.write_all(&wire).await.is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            let _ = socket.write_all(b"0\r\n\r\n").await;
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let output = recording();
        let app = SageMakerTtsApp::new(config, vec![utterance("encoded not PCM")], output.clone())
            .unwrap()
            .start()
            .await
            .unwrap();
        let report = finish(&app).await;
        assert!(!report.is_success(), "{report:?}");
        joined(&report);
        assert_eq!(report.progress.completed_at_eof, 0);
        assert_eq!(output.pcm().len() % 2, 0);
        assert_eq!(
            report.progress.pcm_bytes_delivered,
            output.pcm().len() as u64
        );
        assert!(body.starts_with(&output.pcm()));
        timeout(WAIT, peer)
            .await
            .expect("peer join timed out")
            .unwrap();
    }
}

struct StopFailure;
#[async_trait]
impl FrameProcessor for StopFailure {
    fn name(&self) -> &str {
        "SageMakerStopFailure"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::TtsStopped(_)) {
            return Err(ProcessorError::fatal("synthetic stop output failure"));
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn provider_primary_survives_failed_stop_output_acknowledgment() {
    let listener = listener().await;
    let config = config(&listener);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = timeout(WAIT, listener.accept())
            .await
            .expect("peer accept timed out")
            .unwrap();
        read_request(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 424 Model Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        peer_closed(&mut socket).await;
    });
    let app = SageMakerTtsApp::new(
        config,
        vec![utterance("primary retained")],
        Arc::new(StopFailure),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let report = finish(&app).await;
    joined(&report);
    assert_eq!(
        report.primary,
        Some(pipecat_aws_sagemaker::Error::HttpStatus(424))
    );
    assert!(!report.is_success());
    // Either the awaited stop error or the core's bounded process-error report is
    // retained even if its fatal policy cancels the invocation before it resumes.
    assert!(!report.cleanup_errors.is_empty() || report.total_pipeline_errors > 0);
    timeout(WAIT, peer)
        .await
        .expect("peer join timed out")
        .unwrap();
}

struct HeldSetup {
    entered: Notify,
    cleaned: AtomicBool,
}
#[async_trait]
impl FrameProcessor for HeldSetup {
    fn name(&self) -> &str {
        "SageMakerHeldSetup"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.entered.notify_one();
        std::future::pending().await
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
async fn cancel_and_drop_during_actual_output_setup_retain_startup_and_cleanup_owners() {
    for drop_running in [false, true] {
        let listener = listener().await;
        let output = Arc::new(HeldSetup {
            entered: Notify::new(),
            cleaned: AtomicBool::new(false),
        });
        let app = SageMakerTtsApp::new(
            config(&listener),
            vec![utterance("never network")],
            output.clone(),
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        let completion = app.completion();
        timeout(WAIT, output.entered.notified()).await.unwrap();
        if drop_running {
            drop(app);
        } else {
            app.cancel();
        }
        let report = timeout(WAIT, completion.wait()).await.unwrap();
        joined(&report);
        assert!(!report.is_success());
        assert_eq!(report.progress.admitted, 0);
        assert!(output.cleaned.load(Ordering::SeqCst));
        no_retry(&listener).await;
    }
}
