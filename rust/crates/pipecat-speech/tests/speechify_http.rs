#![cfg(feature = "speechify")]
//! Synthetic loopback protocol contracts. No provider calls or real credentials.
#[allow(dead_code)]
mod support;
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_speech::{speechify::*, *};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn event(name: &str, data: Value) -> Vec<u8> {
    format!("event: {name}\ndata: {data}\n\n").into_bytes()
}
fn audio(bytes: &[u8]) -> Vec<u8> {
    event("speech.chunk", json!({"audio": STANDARD.encode(bytes)}))
}
fn done(ms: f64) -> Vec<u8> {
    event("speech.done", json!({"audio_duration_ms": ms}))
}
fn mark(value: &str, start: usize, end: usize, start_time: f64, end_time: f64) -> Value {
    json!({"type":"word","value":value,"start":start,"end":end,"start_time":start_time,"end_time":end_time})
}
async fn observe(mut stream: SpeechifyStream) -> (Vec<SpeechifyEvent>, Result<(), SpeechError>) {
    let mut events = vec![];
    while let Some(result) = stream.next().await {
        match result {
            Ok(event) => events.push(event),
            Err(error) => return (events, Err(error)),
        }
    }
    (events, Ok(()))
}
async fn run(
    chunks: Vec<Vec<u8>>,
    request: SpeechifySynthesisRequest,
    limits: SpeechifyLimits,
    config_edit: impl FnOnce(&mut SpeechConfig),
) -> (
    Vec<SpeechifyEvent>,
    Result<(), SpeechError>,
    support::Request,
) {
    let (mut config, capture, owner) = support::server(
        "200 OK",
        "text/event-stream; charset=utf-8",
        chunks,
        false,
        Duration::ZERO,
    )
    .await;
    config_edit(&mut config);
    let (events, result) = observe(
        SpeechifyHttpClient::with_limits(config, limits)
            .unwrap()
            .synthesize(request)
            .unwrap(),
    )
    .await;
    // Always join the server owner before making assertions, including failures.
    owner.await.unwrap();
    (events, result, capture.await.unwrap())
}
fn words(events: &[SpeechifyEvent]) -> Vec<(String, f64)> {
    events
        .iter()
        .filter_map(|event| match event {
            SpeechifyEvent::Word {
                word,
                offset_seconds,
            } => Some((word.clone(), *offset_seconds)),
            _ => None,
        })
        .collect()
}
fn pcm(events: &[SpeechifyEvent]) -> Vec<u8> {
    events
        .iter()
        .filter_map(|event| match event {
            SpeechifyEvent::Audio(chunk) => Some(chunk.pcm.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect()
}

#[tokio::test]
async fn exact_headers_payload_and_source_defaults() {
    let request = SpeechifySynthesisRequest::new("你好\nhello");
    let (events, result, captured) = run(
        vec![audio(&[0, 1]), done(7.0)],
        request,
        SpeechifyLimits::default(),
        |_| {},
    )
    .await;
    result.unwrap();
    assert_eq!(
        captured.url().path(),
        "/proxy/v1/audio/stream/with-timestamps"
    );
    assert_eq!(captured.url().query(), None);
    assert!(captured.head.starts_with("POST "));
    assert_eq!(
        captured.header("authorization"),
        Some("Bearer synthetic-secret")
    );
    assert_eq!(captured.header("content-type"), Some("application/json"));
    assert_eq!(captured.header("accept"), Some("text/event-stream"));
    assert_eq!(captured.header("speechify-version"), Some("2026-09-30"));
    assert_eq!(captured.header("speechify-caller"), Some("pipecat-rs"));
    assert_eq!(
        captured.header("speechify-caller-version"),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        captured.json(),
        json!({"input":"你好\nhello","voice_id":"geffen_32","model":"simba-3.2","output_format":"pcm_24000"})
    );
    assert_eq!(
        events,
        vec![
            SpeechifyEvent::Audio(SpeechChunk {
                pcm: vec![0, 1],
                sample_rate: 24000,
                num_channels: 1
            }),
            SpeechifyEvent::Done {
                duration_seconds: 0.007
            }
        ]
    );
}

#[tokio::test]
async fn language_options_and_supported_rates_have_source_semantics() {
    for (language, expected) in [
        ("de", "de-DE"),
        ("en", "en-US"),
        ("es", "es-ES"),
        ("fr", "fr-FR"),
        ("it", "it-IT"),
        ("pt", "pt-BR"),
        ("es-MX", "es-MX"),
        ("en-GB", "en-GB"),
        ("ja-JP", "ja-JP"),
    ] {
        assert_eq!(language_to_speechify_language(language), expected);
    }
    for requested in [8000, 16000, 22050, 24000, 44100, 48000, 32000] {
        let mut request = SpeechifySynthesisRequest::new("options");
        request.sample_rate = requested;
        request.model = "simba-3.0".into();
        request.voice = "custom voice".into();
        request.language = Some("pt".into());
        request.loudness_normalization = Some(false);
        request.text_normalization = Some(true);
        let actual = if requested == 32000 { 24000 } else { requested };
        assert_eq!(request.output_sample_rate(), actual);
        let (events, result, captured) = run(
            vec![audio(&[1, 2]), done(1.0)],
            request,
            SpeechifyLimits::default(),
            |_| {},
        )
        .await;
        result.unwrap();
        assert_eq!(
            captured.json(),
            json!({"input":"options","voice_id":"custom voice","model":"simba-3.0","output_format":format!("pcm_{actual}"),"language":"pt-BR","options":{"loudness_normalization":false,"text_normalization":true}})
        );
        assert!(
            matches!(&events[0],SpeechifyEvent::Audio(chunk) if chunk.sample_rate==actual && chunk.num_channels==1)
        );
    }
    let mut request = SpeechifySynthesisRequest::new("empty language");
    request.language = Some(String::new());
    let (_, result, captured) = run(
        vec![audio(&[1, 2]), done(1.0)],
        request,
        SpeechifyLimits::default(),
        |_| {},
    )
    .await;
    result.unwrap();
    assert!(captured.json().get("language").is_none());
    assert!(captured.json().get("options").is_none());
}

#[tokio::test]
async fn independent_fixed_python_reference_words_and_end_times() {
    // This fixture was produced by executing AST-extracted, unmodified helpers
    // from the pinned source, not by this Rust implementation. See provenance.
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/speechify_source.json")).unwrap();
    assert_eq!(
        fixture["provenance"]["git_blob"],
        "8af3abc008195182d26afe5105300831a8d9908d"
    );
    for case in fixture["word_cases"].as_array().unwrap() {
        let mut wire = audio(&[1]);
        wire.extend(audio(&[2, 3]));
        wire.extend(audio(&[4]));
        for marks in case["mark_chunks"].as_array().unwrap() {
            wire.extend(event("speech.chunk", json!({"speech_marks":marks})));
        }
        wire.extend(done(0.0));
        let (events, result, _) = run(
            wire.chunks(3).map(<[u8]>::to_vec).collect(),
            SpeechifySynthesisRequest::new(case["text"].as_str().unwrap()),
            SpeechifyLimits::default(),
            |_| {},
        )
        .await;
        assert!(result.is_ok(), "{}: {result:?}", case["name"]);
        let expected: Vec<_> = case["expected_words"]
            .as_array()
            .unwrap()
            .iter()
            .map(|word| {
                (
                    word[0].as_str().unwrap().to_owned(),
                    word[1].as_f64().unwrap() - case["reference_time_offset"].as_f64().unwrap(),
                )
            })
            .collect();
        let actual = words(&events);
        assert_eq!(actual.len(), expected.len(), "{}", case["name"]);
        for ((word, time), (expected_word, expected_time)) in actual.iter().zip(expected.iter()) {
            assert_eq!(word, expected_word, "{}", case["name"]);
            assert!((time - expected_time).abs() < 1e-12, "{}", case["name"]);
        }
        assert_eq!(pcm(&events), [1, 2, 3, 4]);
        assert_eq!(
            events.last(),
            Some(&SpeechifyEvent::Done {
                duration_seconds: case["expected_end_seconds"].as_f64().unwrap()
            })
        );
    }
}

#[test]
fn independent_fixed_python_reference_sse_and_rates() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/speechify_source.json")).unwrap();
    for case in fixture["sse_cases"].as_array().unwrap() {
        for suffix in ["", "\n", "\r", "\r\n"] {
            let mut parser = pipecat_services::sse::SseDecoder::new(4096);
            let bytes = format!("{}{suffix}", case["block"].as_str().unwrap());
            let mut events = vec![];
            for byte in bytes.as_bytes() {
                events.extend(parser.feed(&[*byte]).unwrap());
            }
            events.extend(parser.feed(b"\n\n").unwrap());
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event.as_deref(), case["expected"][0].as_str());
            assert_eq!(
                serde_json::from_str::<Value>(&events[0].data).unwrap(),
                case["expected"][1]
            );
        }
    }
    for case in fixture["rates"].as_array().unwrap() {
        let mut request = SpeechifySynthesisRequest::new("x");
        request.sample_rate = case["requested"].as_u64().unwrap() as u32;
        assert_eq!(
            u64::from(request.output_sample_rate()),
            case["expected"][1].as_u64().unwrap()
        );
        assert_eq!(
            format!("pcm_{}", request.output_sample_rate()),
            case["expected"][0].as_str().unwrap()
        );
    }
}

#[tokio::test]
async fn split_utf8_crlf_comments_multidata_and_eof_last_block() {
    let wire = concat!(
        ": keep-alive\r\n\r\n",
        "event: speech.chunk\r\ndata: {\r\ndata: \"audio\":\"AQI=\",\r\ndata: \"speech_marks\":[{\"value\":\"你🙂\",\"start\":0,\"end\":2,\"start_time\":120,\"end_time\":200}]}\r\n\r\n",
        "event: speech.future\r\ndata: {}\r\n\r\n",
        "event: speech.done\r\ndata: {\"audio_duration_ms\":\r\ndata: 4350}"
    );
    let (events, result, _) = run(
        wire.as_bytes().chunks(1).map(<[u8]>::to_vec).collect(),
        SpeechifySynthesisRequest::new("你🙂"),
        SpeechifyLimits::default(),
        |_| {},
    )
    .await;
    result.unwrap();
    assert_eq!(words(&events), [("你🙂".into(), 0.12)]);
    assert_eq!(pcm(&events), [1, 2]);
    assert!(matches!(events[0], SpeechifyEvent::Audio(_)));
    assert_eq!(
        events.last(),
        Some(&SpeechifyEvent::Done {
            duration_seconds: 4.35
        })
    );
}

#[tokio::test]
async fn late_marks_max_done_duration_and_each_request_restarts_at_zero() {
    for done_ms in [2500.0, 3500.0] {
        let events = vec![
            event(
                "speech.chunk",
                json!({"audio":"AQI=","speech_marks":[mark("one",0,3,0.0,1200.0)]}),
            ),
            done(done_ms),
            done(100.0),
            event(
                "speech.chunk",
                json!({"speech_marks":[mark("two",4,7,1200.0,3000.0)]}),
            ),
        ];
        let (events, result, _) = run(
            events,
            SpeechifySynthesisRequest::new("one two"),
            SpeechifyLimits::default(),
            |_| {},
        )
        .await;
        result.unwrap();
        assert!(matches!(events[0], SpeechifyEvent::Audio(_)));
        assert_eq!(words(&events), [("one".into(), 0.0), ("two".into(), 1.2)]);
        assert_eq!(
            events.last(),
            Some(&SpeechifyEvent::Done {
                duration_seconds: done_ms.max(3000.0) / 1000.0
            })
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, SpeechifyEvent::Done { .. }))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn word_release_happens_before_eof_and_split_word_uses_first_timestamp() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechify("synthetic-secret");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let (release, waiting) = tokio::sync::oneshot::channel();
    let owner = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
        let first = event(
            "speech.chunk",
            json!({"audio":"AQI=","speech_marks":[mark("I",0,1,0.0,100.0),mark("text",2,6,100.0,300.0),mark("-",6,7,300.0,400.0)]}),
        );
        socket
            .write_all(format!("{:x}\r\n", first.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(&first).await.unwrap();
        socket.write_all(b"\r\n").await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), waiting).await;
        let mut tail = event(
            "speech.chunk",
            json!({"speech_marks":[mark("to",7,9,500.0,600.0)]}),
        );
        tail.extend(done(700.0));
        let _ = socket
            .write_all(format!("{:x}\r\n", tail.len()).as_bytes())
            .await;
        let _ = socket.write_all(&tail).await;
        let _ = socket.write_all(b"\r\n0\r\n\r\n").await;
    });
    let mut stream = SpeechifyHttpClient::new(config)
        .unwrap()
        .synthesize(SpeechifySynthesisRequest::new("I text-to"))
        .unwrap();
    let first = stream.next().await;
    let second = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
    let _ = release.send(());
    let (tail, result) = observe(stream).await;
    owner.await.unwrap();
    assert!(matches!(first, Some(Ok(SpeechifyEvent::Audio(_)))));
    assert!(
        matches!(second, Ok(Some(Ok(SpeechifyEvent::Word { word, offset_seconds:0.0 }))) if word=="I")
    );
    result.unwrap();
    assert_eq!(words(&tail), [("text-to".into(), 0.1)]);
}

#[tokio::test]
async fn malformed_json_midstream_error_missing_done_and_odd_pcm_fail_closed() {
    let cases = [
        (vec![audio(&[1, 2])], "missing"),
        (
            vec![
                audio(&[1, 2]),
                b"event: speech.chunk\ndata: {\"synthetic-secret\":\n\n".to_vec(),
                done(1.0),
            ],
            "json",
        ),
        (
            vec![
                audio(&[1, 2]),
                event(
                    "speech.error",
                    json!({"error":{"code":"secret","message":"synthetic-secret"}}),
                ),
                done(1.0),
            ],
            "provider",
        ),
        (vec![audio(&[1]), done(1.0)], "odd"),
        (vec![done(1.0)], "empty"),
        (
            vec![
                event("speech.chunk", json!({"audio":"not base64"})),
                done(1.0),
            ],
            "base64",
        ),
        (
            vec![
                audio(&[1, 2]),
                event("speech.chunk", json!({"speech_marks":"bad"})),
                done(1.0),
            ],
            "marks",
        ),
        (
            vec![
                audio(&[1, 2]),
                event("speech.chunk", json!({"speech_marks":[1]})),
                done(1.0),
            ],
            "mark",
        ),
        (
            vec![
                audio(&[1, 2]),
                event("speech.chunk", json!({"audio":false})),
                done(1.0),
            ],
            "audio",
        ),
        (
            vec![
                audio(&[1, 2]),
                b"event: speech.done\ndata: {\"audio_duration_ms\":1e400}\n\n".to_vec(),
            ],
            "json",
        ),
        (
            vec![
                audio(&[1, 2]),
                event("speech.done", json!({"audio_duration_ms":-1})),
                done(1.0),
            ],
            "timestamp",
        ),
        (
            vec![
                audio(&[1, 2]),
                b"event: speech.done\ndata: \xff\n\n".to_vec(),
            ],
            "utf8",
        ),
        (
            vec![audio(&[1, 2]), event("speech.done", json!([]))],
            "object",
        ),
    ];
    for (chunks, reason) in cases {
        let (events, result, _) = run(
            chunks,
            SpeechifySynthesisRequest::new("sensitive-text"),
            SpeechifyLimits::default(),
            |_| {},
        )
        .await;
        let error = result.unwrap_err();
        assert!(!format!("{error:?} {error}").contains("synthetic-secret"));
        assert!(!format!("{error:?} {error}").contains("sensitive-text"));
        assert!(!events
            .iter()
            .any(|event| matches!(event, SpeechifyEvent::Done { .. })));
        match reason {
            "json" => assert!(matches!(error, SpeechError::Json(_))),
            "odd" => assert!(matches!(error, SpeechError::PartialSample)),
            _ => assert!(
                matches!(error, SpeechError::Protocol(_)),
                "{reason}: {error:?}"
            ),
        }
    }
}

#[tokio::test]
async fn timestamps_must_be_nonnegative_finite_numbers_but_nonword_marks_are_ignored() {
    for field in ["start_time", "end_time"] {
        for value in [json!(-1), json!("NaN"), json!(null), json!(false)] {
            let mut value_mark = mark("x", 0, 1, 0.0, 1.0);
            value_mark[field] = value;
            let (events, result, _) = run(
                vec![
                    audio(&[1, 2]),
                    event("speech.chunk", json!({"speech_marks":[value_mark]})),
                    done(1.0),
                ],
                SpeechifySynthesisRequest::new("x"),
                SpeechifyLimits::default(),
                |_| {},
            )
            .await;
            assert!(matches!(
                result,
                Err(SpeechError::Protocol("invalid Speechify timestamp"))
            ));
            assert!(!events
                .iter()
                .any(|event| matches!(event, SpeechifyEvent::Done { .. })));
        }
    }
    let (_, result, _) = run(
        vec![
            audio(&[1, 2]),
            event(
                "speech.chunk",
                json!({"speech_marks":[{"type":"sentence","start_time":"bad","end_time":-1}]}),
            ),
            done(0.0),
        ],
        SpeechifySynthesisRequest::new("x"),
        SpeechifyLimits::default(),
        |_| {},
    )
    .await;
    result.unwrap();
}

#[tokio::test]
async fn independent_event_wire_pcm_word_count_word_bytes_and_text_field_budgets() {
    for kind in [
        "event",
        "wire",
        "pcm",
        "word_count",
        "word_bytes",
        "text_field",
    ] {
        let mut limits = SpeechifyLimits::default();
        let mut chunks = vec![audio(&[1, 2])];
        match kind {
            "event" => chunks.push(event("speech.future", json!({"ignored":"a".repeat(300)}))),
            "wire" => {
                limits.max_wire_bytes = 256;
                chunks.extend((0..30).map(|_| b": heartbeat\n\n".to_vec()));
            }
            "pcm" => chunks.push(audio(&[3, 4])),
            "word_count" => {
                limits.max_word_count = 1;
                chunks.push(event(
                    "speech.chunk",
                    json!({"speech_marks":[{"value":"one"},{"value":"two"}]}),
                ));
            }
            "word_bytes" => {
                limits.max_word_bytes = 3;
                chunks.push(event(
                    "speech.chunk",
                    json!({"speech_marks":[{"value":"one"},{"value":"two"}]}),
                ));
            }
            "text_field" => {
                limits.max_text_field_bytes = 16;
                chunks.push(event(
                    "speech.chunk",
                    json!({"speech_marks":[{"value":"x".repeat(17)}]}),
                ));
            }
            _ => unreachable!(),
        }
        chunks.push(done(0.0));
        let (events, result, _) = run(
            chunks,
            SpeechifySynthesisRequest::new("x"),
            limits,
            |config| {
                if kind == "event" {
                    config.max_json_bytes = 256;
                }
                if kind == "pcm" {
                    config.max_audio_bytes = 2;
                }
            },
        )
        .await;
        assert!(
            matches!(result, Err(SpeechError::TooLarge)),
            "{kind}: {result:?}"
        );
        assert!(!events
            .iter()
            .any(|event| matches!(event, SpeechifyEvent::Done { .. })));
    }
    // One word can consist of many adjacent fragments: input marks are bounded too.
    let limits = SpeechifyLimits {
        max_word_count: 1,
        ..SpeechifyLimits::default()
    };
    let (_, result, _) = run(
        vec![
            audio(&[1, 2]),
            event(
                "speech.chunk",
                json!({"speech_marks":[mark("a",0,1,0.0,1.0),mark("b",1,2,1.0,2.0)]}),
            ),
            done(2.0),
        ],
        SpeechifySynthesisRequest::new("abc"),
        limits,
        |_| {},
    )
    .await;
    assert!(matches!(result, Err(SpeechError::TooLarge)));
}

#[tokio::test]
async fn http_truncation_and_content_type_do_not_report_done() {
    for (mime, truncated, expected) in [
        ("text/event-stream", true, "transport"),
        ("application/json", false, "mime"),
        ("audio/pcm", false, "mime"),
    ] {
        let (config, _, owner) = support::server(
            "200 OK",
            mime,
            vec![audio(&[1, 2]), done(0.0)],
            truncated,
            Duration::ZERO,
        )
        .await;
        let (events, result) = observe(
            SpeechifyHttpClient::new(config)
                .unwrap()
                .synthesize(SpeechifySynthesisRequest::new("x"))
                .unwrap(),
        )
        .await;
        owner.await.unwrap();
        assert!(!events
            .iter()
            .any(|event| matches!(event, SpeechifyEvent::Done { .. })));
        match expected {
            "transport" => assert!(matches!(result, Err(SpeechError::Transport))),
            _ => assert!(matches!(result, Err(SpeechError::ContentType))),
        }
    }
}

#[tokio::test]
async fn http_error_and_redirect_have_one_attempt_and_redacted_body() {
    for status in [201, 204, 301, 307, 401, 429, 500, 503] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::speechify("synthetic-secret");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let owner = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = support::read_request(&mut socket).await;
            let reply=format!("HTTP/1.1 {status} Fixture\r\nLocation: /second-attempt\r\nContent-Type: text/event-stream\r\nContent-Length: 16\r\nConnection: close\r\n\r\nsynthetic-secret");
            let _ = socket.write_all(reply.as_bytes()).await;
            drop(socket);
            let retry = tokio::time::timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_ok();
            (request, retry)
        });
        let (_, result) = observe(
            SpeechifyHttpClient::new(config)
                .unwrap()
                .synthesize(SpeechifySynthesisRequest::new("x"))
                .unwrap(),
        )
        .await;
        let (_, retry) = owner.await.unwrap();
        assert!(!retry);
        let error = result.unwrap_err();
        assert!(matches!(error,SpeechError::Http(code) if code==status));
        assert!(!format!("{error:?} {error}").contains("synthetic-secret"));
    }
}

#[tokio::test]
async fn timeout_cancel_drop_and_backpressure_release_the_owned_socket() {
    for mode in ["headers", "timeout", "cancel", "drop", "backpressure"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::speechify("synthetic-secret");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        config.timeout = Duration::from_millis(150);
        config.queue_capacity = 1;
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let owner = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            if mode != "headers" {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
                let mut body = audio(&[1, 2]);
                body.extend(audio(&[3, 4]));
                body.extend(audio(&[5, 6]));
                socket
                    .write_all(format!("{:x}\r\n", body.len()).as_bytes())
                    .await
                    .unwrap();
                socket.write_all(&body).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
            }
            let _ = ready.send(());
            let mut byte = [0];
            let closed = matches!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte)).await,
                Ok(Ok(0)) | Ok(Err(_))
            );
            closed
        });
        let mut stream = SpeechifyHttpClient::new(config)
            .unwrap()
            .synthesize(SpeechifySynthesisRequest::new("x"))
            .unwrap();
        let _ = waiting.await;
        let result = match mode {
            "drop" => {
                drop(stream);
                None
            }
            "cancel" => {
                stream.cancel();
                let result = stream.next().await;
                drop(stream);
                Some(result)
            }
            "backpressure" => {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let (_, result) = observe(stream).await;
                Some(Some(result.map(|()| SpeechifyEvent::Done {
                    duration_seconds: 0.0,
                })))
            }
            _ => {
                let (_, result) = observe(stream).await;
                Some(Some(result.map(|()| SpeechifyEvent::Done {
                    duration_seconds: 0.0,
                })))
            }
        };
        let closed = owner.await.unwrap();
        assert!(closed, "{mode}: socket stayed open");
        match mode {
            "drop" => {}
            "cancel" => assert!(matches!(result, Some(Some(Err(SpeechError::Cancelled))))),
            _ => assert!(matches!(result, Some(Some(Err(SpeechError::Timeout))))),
        }
    }
}

#[test]
fn local_validation_and_debug_do_not_expose_private_values() {
    let mut config = SpeechConfig::speechify("synthetic-secret");
    config.base_url = "http://127.0.0.1:1".into();
    let client = SpeechifyHttpClient::new(config.clone()).unwrap();
    for kind in ["empty", "model", "voice", "rate", "language", "text_limit"] {
        let mut request = SpeechifySynthesisRequest::new("sensitive-text");
        match kind {
            "empty" => request.text.clear(),
            "model" => request.model.clear(),
            "voice" => request.voice.clear(),
            "rate" => request.sample_rate = 0,
            "language" => request.language = Some("x".repeat(129)),
            _ => request.text = "x".repeat(65_537),
        }
        assert!(client.synthesize(request).is_err());
    }
    assert!(
        !format!("{:?}", SpeechifySynthesisRequest::new("sensitive-text"))
            .contains("sensitive-text")
    );
    assert!(!format!("{config:?}").contains("synthetic-secret"));
    let limits = SpeechifyLimits {
        max_wire_bytes: 0,
        ..SpeechifyLimits::default()
    };
    assert!(SpeechifyHttpClient::with_limits(config, limits).is_err());
}
