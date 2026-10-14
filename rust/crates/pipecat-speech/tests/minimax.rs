#![cfg(feature = "minimax")]
//! Real loopback HTTP with synthetic credentials. No live MiniMax qualification.
#[allow(dead_code)]
mod support;
use pipecat_speech::{minimax::*, *};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const GROUP_ID: &str = "synthetic group +/?&=测试";
fn event(value: Value) -> Vec<u8> {
    format!("data: {value}\n\n").into_bytes()
}
fn audio(hex: &str) -> Vec<u8> {
    event(json!({"data":{"audio":hex,"status":1}}))
}
fn final_event(hex: &str) -> Vec<u8> {
    event(json!({"data":{"audio":hex,"status":2},"extra_info":{},"base_resp":{"status_code":0}}))
}
async fn observe(mut stream: SpeechStream) -> (Vec<SpeechChunk>, Result<(), SpeechError>) {
    let mut chunks = vec![];
    while let Some(result) = stream.next().await {
        match result {
            Ok(chunk) => chunks.push(chunk),
            Err(error) => {
                assert!(stream.next().await.is_none(), "terminal errors occur once");
                return (chunks, Err(error));
            }
        }
    }
    assert!(stream.next().await.is_none());
    (chunks, Ok(()))
}
async fn run(
    chunks: Vec<Vec<u8>>,
    request: MiniMaxSynthesisRequest,
    limits: MiniMaxLimits,
    config_edit: impl FnOnce(&mut SpeechConfig),
) -> (Vec<SpeechChunk>, Result<(), SpeechError>, support::Request) {
    let (mut config, capture, owner) = support::server(
        "200 OK",
        "text/event-stream; charset=utf-8",
        chunks,
        false,
        Duration::ZERO,
    )
    .await;
    config_edit(&mut config);
    let (chunks, result) = observe(
        MiniMaxHttpClient::with_limits(config, GROUP_ID, limits)
            .unwrap()
            .synthesize(request)
            .unwrap(),
    )
    .await;
    owner.await.unwrap();
    (chunks, result, capture.await.unwrap())
}
fn pcm(chunks: &[SpeechChunk]) -> Vec<u8> {
    chunks.iter().flat_map(|chunk| chunk.pcm.clone()).collect()
}
fn request() -> MiniMaxSynthesisRequest {
    MiniMaxSynthesisRequest::new("你好\nhello")
}

#[tokio::test]
async fn source_defaults_exact_payload_headers_and_encoded_group_query() {
    let (chunks, result, captured) = run(
        vec![audio("00000100"), final_event("00000100")],
        request(),
        MiniMaxLimits::default(),
        |_| {},
    )
    .await;
    result.unwrap();
    assert!(captured.head.starts_with("POST "));
    assert_eq!(captured.url().path(), "/proxy/v1/t2a_v2");
    assert_eq!(
        captured.url().query_pairs().collect::<Vec<_>>(),
        [("GroupId".into(), GROUP_ID.into())]
    );
    assert_eq!(captured.url().fragment(), None);
    assert_eq!(
        captured.header("authorization"),
        Some("Bearer synthetic-secret")
    );
    assert_eq!(captured.header("content-type"), Some("application/json"));
    assert_eq!(
        captured.header("accept"),
        Some("application/json, text/plain, */*")
    );
    assert_eq!(
        captured.json(),
        json!({
            "stream":true,"model":"speech-2.8-turbo","text":"你好\nhello",
            "voice_setting":{"voice_id":"Calm_Woman","speed":1.0,"vol":1.0,"pitch":0},
            "audio_setting":{"bitrate":128000,"format":"pcm","channel":1,"sample_rate":24000}
        })
    );
    assert_eq!(
        chunks,
        [SpeechChunk {
            pcm: vec![0, 0, 1, 0],
            sample_rate: 24000,
            num_channels: 1
        }]
    );
    assert_eq!(
        SpeechConfig::minimax("synthetic").base_url,
        "https://api.minimax.io"
    );
}

#[tokio::test]
async fn typed_voice_settings_optional_false_and_supported_sample_rates() {
    for rate in MINIMAX_PCM_SAMPLE_RATES {
        let mut request = request();
        request.sample_rate = rate;
        request.model = "speech-2.6-hd".into();
        request.voice = "synthetic-custom-voice".into();
        request.language = Some("pt-BR".into());
        request.voice_settings = MiniMaxVoiceSettings {
            speed: 0.5,
            volume: 2.0,
            pitch: -12,
            emotion: Some(MiniMaxEmotion::Calm),
            text_normalization: Some(false),
            latex_read: Some(true),
        };
        let (chunks, result, captured) = run(
            vec![audio("aBcD"), final_event("aBcD")],
            request,
            MiniMaxLimits::default(),
            |_| {},
        )
        .await;
        result.unwrap();
        assert_eq!(chunks[0].sample_rate, rate);
        assert_eq!(chunks[0].num_channels, 1);
        assert_eq!(chunks[0].pcm, [0xab, 0xcd]);
        assert_eq!(
            captured.json(),
            json!({
                "stream":true,"model":"speech-2.6-hd","text":"你好\nhello","language_boost":"Portuguese",
                "voice_setting":{"voice_id":"synthetic-custom-voice","speed":0.5,"vol":2.0,"pitch":-12,"emotion":"calm","text_normalization":false,"latex_read":true},
                "audio_setting":{"bitrate":128000,"format":"pcm","channel":1,"sample_rate":rate}
            })
        );
    }
    for language in ["", "xx-ZZ", "auto", "PT-BR", "pt_BR"] {
        let mut request = request();
        request.language = Some(language.into());
        let (_, result, captured) = run(
            vec![audio("0102"), final_event("")],
            request,
            MiniMaxLimits::default(),
            |_| {},
        )
        .await;
        result.unwrap();
        assert_eq!(
            captured.json()["language_boost"],
            language_to_minimax_language(language)
        );
    }
}

#[tokio::test]
async fn documented_emotions_and_numeric_boundaries_are_typed_and_exact() {
    for (emotion, name) in [
        (MiniMaxEmotion::Happy, "happy"),
        (MiniMaxEmotion::Sad, "sad"),
        (MiniMaxEmotion::Angry, "angry"),
        (MiniMaxEmotion::Fearful, "fearful"),
        (MiniMaxEmotion::Disgusted, "disgusted"),
        (MiniMaxEmotion::Surprised, "surprised"),
        (MiniMaxEmotion::Calm, "calm"),
        (MiniMaxEmotion::Fluent, "fluent"),
        (MiniMaxEmotion::Whisper, "whisper"),
    ] {
        assert_eq!(serde_json::to_value(emotion).unwrap(), name);
    }
    for volume in [f64::EPSILON, 10.0] {
        let mut request = request();
        request.voice_settings.speed = 2.0;
        request.voice_settings.volume = volume;
        request.voice_settings.pitch = 12;
        request.voice_settings.emotion = Some(MiniMaxEmotion::Whisper);
        request.voice_settings.text_normalization = Some(false);
        request.voice_settings.latex_read = Some(false);
        let (_, result, captured) = run(
            vec![audio("0102"), final_event("")],
            request,
            MiniMaxLimits::default(),
            |_| {},
        )
        .await;
        result.unwrap();
        let voice = &captured.json()["voice_setting"];
        assert_eq!(voice["speed"], 2.0);
        assert_eq!(voice["vol"], volume);
        assert_eq!(voice["pitch"], 12);
        assert_eq!(voice["emotion"], "whisper");
        assert_eq!(voice["text_normalization"], false);
        assert_eq!(voice["latex_read"], false);
    }
}

#[test]
fn independent_unchanged_python_language_oracle_replays_every_mapping() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/minimax_language.json")).unwrap();
    assert_eq!(
        fixture["baseline"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    assert_eq!(fixture["base_mapping"].as_array().unwrap().len(), 40);
    for name in [
        "base_mapping",
        "regional_cases",
        "unmapped_enum_fallbacks",
        "annotation_unenforced_raw_string_cases",
    ] {
        for case in fixture[name].as_array().unwrap() {
            assert_eq!(
                language_to_minimax_language(case["input"].as_str().unwrap()),
                case["output"].as_str().unwrap(),
                "{name}: {case}"
            );
        }
    }
}

#[tokio::test]
async fn bytewise_bom_utf8_multiline_crlf_comments_and_clean_eof_tail() {
    let wire = concat!(
        "\u{feff}: keep-alive\r\n\r\n",
        "data: {\r\ndata: \"data\":null,\r\ndata: \"base_resp\":{\"status_code\":0},\"trace\":\"你好🙂 data: text\"}\r\n\r\n",
        "data: {\"data\":{\"audio\":\"01\",\"status\":1}}\r\n\r\n",
        "data: {\"data\":{\"audio\":\"0203\",\"status\":1}}\r\n\r\n",
        "data: {\"data\":{\"audio\":\"04\",\"status\":1}}\r\n\r\n",
        "data: {\"data\":{\"audio\":\"01020304\",\"status\":2},\"extra_info\":{\"audio_sample_rate\":24000,\"audio_channel\":1,\"audio_format\":\"pcm\"}}"
    );
    for suffix in ["", "\n", "\r", "\r\n", "\n\n"] {
        let (chunks, result, _) = run(
            format!("{wire}{suffix}")
                .as_bytes()
                .chunks(1)
                .map(<[u8]>::to_vec)
                .collect(),
            request(),
            MiniMaxLimits::default(),
            |_| {},
        )
        .await;
        result.unwrap();
        assert_eq!(pcm(&chunks), [1, 2, 3, 4]);
        assert!(chunks.iter().all(|chunk| chunk.pcm.len() % 2 == 0));
    }
}

#[tokio::test]
async fn aggregate_audio_is_never_replayed_or_charged_as_incremental_pcm() {
    for extra in [None, Some(Value::Null), Some(json!({}))] {
        let mut final_record = json!({"data":{"audio":"01020304".repeat(32),"status":2}});
        if let Some(extra) = extra {
            final_record["extra_info"] = extra;
        }
        let (chunks, result, _) = run(
            vec![
                audio("0102"),
                audio("0304"),
                event(final_record),
                b": trailing heartbeat\n\n".to_vec(),
                event(json!({"data":null})),
            ],
            request(),
            MiniMaxLimits::default(),
            |config| config.max_audio_bytes = 4,
        )
        .await;
        result.unwrap();
        assert_eq!(pcm(&chunks), [1, 2, 3, 4]);
    }
    // Ignored aggregate is not decoded. Its raw bytes are still bounded below.
    let (chunks, result, _) = run(
        vec![audio("0102"), final_event("not incremental hex")],
        request(),
        MiniMaxLimits::default(),
        |config| config.max_audio_bytes = 2,
    )
    .await;
    result.unwrap();
    assert_eq!(pcm(&chunks), [1, 2]);
}

#[tokio::test]
async fn extra_info_key_presence_skips_audio_but_cannot_hide_status_errors() {
    for extra in [Value::Null, json!({})] {
        let skipped = event(json!({"data":{"audio":"fefe","status":1},"extra_info":extra}));
        let (chunks, result, _) = run(
            vec![skipped.clone(), audio("0102"), final_event("0102")],
            request(),
            MiniMaxLimits::default(),
            |_| {},
        )
        .await;
        result.unwrap();
        assert_eq!(pcm(&chunks), [1, 2]);
        let (_, result, _) = run(
            vec![audio("0102"), skipped],
            request(),
            MiniMaxLimits::default(),
            |_| {},
        )
        .await;
        assert!(matches!(
            result,
            Err(SpeechError::Protocol("missing MiniMax final status"))
        ));
    }
    for value in [
        json!({"data":{"audio":"fefe","status":1},"extra_info":{},"base_resp":{"status_code":1004}}),
        json!({"data":{"audio":"fefe","status":0},"extra_info":{}}),
    ] {
        let (chunks, result, _) = run(
            vec![event(value), final_event("")],
            request(),
            MiniMaxLimits::default(),
            |_| {},
        )
        .await;
        assert!(chunks.is_empty());
        assert!(matches!(result, Err(SpeechError::Protocol(_))));
    }
}

#[tokio::test]
async fn malformed_json_hex_utf8_and_typed_status_fail_closed_with_redacted_errors() {
    let cases = vec![
        b"data: {\"synthetic-secret\":\n\n".to_vec(),
        b"data: \xff\n\n".to_vec(),
        event(json!([])),
        event(json!({"data":false})),
        event(json!({"data":{"audio":"0102"}})),
        event(json!({"data":{"audio":"0102","status":"1"}})),
        event(json!({"data":{"audio":"0102","status":0}})),
        event(json!({"data":{"audio":"0102","status":-1}})),
        event(json!({"data":{"audio":"0102","status":3}})),
        event(json!({"data":{"audio":false,"status":1}})),
        event(json!({"data":{"audio":[],"status":2}})),
        event(json!({"base_resp":null})),
        event(json!({"base_resp":{}})),
        event(json!({"base_resp":{"status_code":"0"}})),
        event(json!({"base_resp":{"status_code":1004,"status_msg":"synthetic-secret"}})),
        event(json!({"error":"synthetic-secret"})),
        b"event: error\ndata: {}\n\n".to_vec(),
        audio("0"),
        audio("000"),
        audio("gg"),
        audio("00 00"),
        audio("00\t00"),
        audio("🙂"),
    ];
    for bad in cases {
        let (chunks, result, _) = run(
            vec![bad.clone(), final_event("")],
            request(),
            MiniMaxLimits::default(),
            |_| {},
        )
        .await;
        assert!(chunks.is_empty(), "{bad:?}");
        let error = result.unwrap_err();
        assert!(
            matches!(error, SpeechError::Protocol(_)),
            "{bad:?}: {error:?}"
        );
        assert!(!format!("{error:?} {error}").contains("synthetic-secret"));
        assert!(!format!("{error:?} {error}").contains("你好"));
        assert!(!format!("{error:?} {error}").contains(GROUP_ID));
    }
}

#[tokio::test]
async fn final_requires_incremental_audio_clean_sample_and_no_later_data() {
    let cases = [
        (vec![audio("0102")], "final"),
        (vec![final_event("0102")], "empty"),
        (vec![event(json!({"data":null})), final_event("")], "empty"),
        (vec![audio(""), final_event("")], "empty"),
        (vec![audio("01"), final_event("01")], "odd"),
        (vec![audio("010203"), final_event("010203")], "odd"),
        (
            vec![audio("0102"), final_event("0102"), audio("0304")],
            "late",
        ),
        (
            vec![audio("0102"), final_event("0102"), final_event("0102")],
            "duplicate",
        ),
        (
            vec![
                audio("0102"),
                final_event("0102"),
                event(json!({"base_resp":{"status_code":1004}})),
            ],
            "error",
        ),
        (
            vec![audio("0102"), b"data: {\"data\":{\"status\":2".to_vec()],
            "truncated",
        ),
        (
            vec![audio("0102"), final_event("0102"), b"data: {".to_vec()],
            "trailing",
        ),
    ];
    for (wire, kind) in cases {
        let (_, result, _) = run(wire, request(), MiniMaxLimits::default(), |_| {}).await;
        match kind {
            "odd" => assert!(matches!(result, Err(SpeechError::PartialSample))),
            _ => assert!(
                matches!(result, Err(SpeechError::Protocol(_))),
                "{kind}: {result:?}"
            ),
        }
    }
}

#[tokio::test]
async fn optional_final_audio_format_metadata_must_match_when_present() {
    for extra in [
        json!({"audio_sample_rate":16000}),
        json!({"audio_sample_rate":"24000"}),
        json!({"audio_channel":2}),
        json!({"audio_channel":1.0}),
        json!({"audio_channel":false}),
        json!({"audio_format":"mp3"}),
        json!({"audio_format":1}),
        json!([]),
    ] {
        let (_, result, _) = run(
            vec![
                audio("0102"),
                event(json!({"data":{"status":2},"extra_info":extra})),
            ],
            request(),
            MiniMaxLimits::default(),
            |_| {},
        )
        .await;
        assert!(matches!(result, Err(SpeechError::Protocol(_))));
    }
    let (_,result,_) = run(
        vec![audio("0102"),event(json!({"data":{"status":2},"extra_info":{"audio_sample_rate":null,"audio_channel":null,"audio_format":null}}))],
        request(),MiniMaxLimits::default(),|_|{},
    ).await;
    result.unwrap();
}

#[tokio::test]
async fn independent_request_event_wire_event_count_and_pcm_budgets() {
    for kind in [
        "event",
        "comment_event",
        "newlines_event",
        "wire",
        "events",
        "pcm",
        "partial_pcm",
        "aggregate_event",
        "aggregate_wire",
    ] {
        let mut limits = MiniMaxLimits::default();
        let mut chunks = vec![audio("0102")];
        match kind {
            "event" => chunks.push(event(json!({"ignored":"x".repeat(2048)}))),
            "comment_event" => chunks.push(format!(":{}\r\n\r\n", "x".repeat(2048)).into_bytes()),
            "newlines_event" => chunks.push(format!("{}\r\n", ":\r\n".repeat(400)).into_bytes()),
            "wire" => {
                limits.max_wire_bytes = 256;
                chunks.extend((0..64).map(|_| b": heartbeat\n\n".to_vec()));
            }
            "events" => {
                limits.max_events = 2;
                chunks.push(event(json!({})));
            }
            "pcm" => chunks.push(audio("0304")),
            "partial_pcm" => chunks.push(audio("03")),
            "aggregate_event" => chunks.push(final_event(&"00".repeat(1024))),
            "aggregate_wire" => {
                limits.max_wire_bytes = 256;
                chunks.push(final_event(&"00".repeat(256)));
            }
            _ => unreachable!(),
        }
        if !kind.starts_with("aggregate") {
            chunks.push(final_event("0102"));
        }
        let (_, result, _) = run(chunks, request(), limits, |config| {
            if matches!(
                kind,
                "event" | "comment_event" | "newlines_event" | "aggregate_event"
            ) {
                config.max_json_bytes = 1024;
            }
            if matches!(kind, "pcm" | "partial_pcm") {
                config.max_audio_bytes = 2;
            }
        })
        .await;
        assert!(
            matches!(result, Err(SpeechError::TooLarge)),
            "{kind}: {result:?}"
        );
    }
    // An odd pending byte consumes the decoded budget before any full sample.
    let (_, result, _) = run(
        vec![audio("01"), audio("02"), final_event("0102")],
        request(),
        MiniMaxLimits::default(),
        |config| config.max_audio_bytes = 1,
    )
    .await;
    assert!(matches!(result, Err(SpeechError::TooLarge)));
    // Exact whole-event and PCM boundaries remain usable.
    let (chunks, result, _) = run(
        vec![audio("0102"), final_event(&"00".repeat(300))],
        request(),
        MiniMaxLimits::default(),
        |config| config.max_audio_bytes = 2,
    )
    .await;
    result.unwrap();
    assert_eq!(pcm(&chunks), [1, 2]);
}

#[tokio::test]
async fn incremental_audio_is_observable_before_final_and_http_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::minimax("synthetic");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let (release, waiting) = tokio::sync::oneshot::channel();
    let owner = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
        let first = audio("0102");
        socket
            .write_all(format!("{:x}\r\n", first.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(&first).await.unwrap();
        socket.write_all(b"\r\n").await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), waiting).await;
        let tail = final_event("0102");
        let _ = socket
            .write_all(format!("{:x}\r\n", tail.len()).as_bytes())
            .await;
        let _ = socket.write_all(&tail).await;
        let _ = socket.write_all(b"\r\n0\r\n\r\n").await;
    });
    let mut stream = MiniMaxHttpClient::new(config, GROUP_ID)
        .unwrap()
        .synthesize(request())
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(1), stream.next()).await;
    let _ = release.send(());
    let (tail, result) = observe(stream).await;
    owner.await.unwrap();
    result.unwrap();
    assert!(matches!(first,Ok(Some(Ok(chunk))) if chunk.pcm==[1,2]));
    assert!(tail.is_empty());
}

#[tokio::test]
async fn truncated_http_body_and_non_sse_types_fail_even_after_final() {
    for (mime, truncated, expected) in [
        ("text/event-stream", true, "transport"),
        ("application/json", false, "type"),
        ("audio/pcm", false, "type"),
    ] {
        let (config, _, owner) = support::server(
            "200 OK",
            mime,
            vec![audio("0102"), final_event("0102")],
            truncated,
            Duration::ZERO,
        )
        .await;
        let (_, result) = observe(
            MiniMaxHttpClient::new(config, GROUP_ID)
                .unwrap()
                .synthesize(request())
                .unwrap(),
        )
        .await;
        owner.await.unwrap();
        match expected {
            "transport" => assert!(matches!(result, Err(SpeechError::Transport))),
            _ => assert!(matches!(result, Err(SpeechError::ContentType))),
        }
    }
}

#[tokio::test]
async fn http_errors_and_redirects_make_one_attempt_without_body_disclosure() {
    for status in [201, 204, 301, 302, 307, 401, 429, 500, 503] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::minimax("synthetic-secret");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let owner = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            let reply=format!("HTTP/1.1 {status} Fixture\r\nLocation: /retry\r\nContent-Type: text/event-stream\r\nContent-Length: 16\r\nConnection: close\r\n\r\nsynthetic-secret");
            let _ = socket.write_all(reply.as_bytes()).await;
            drop(socket);
            tokio::time::timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_ok()
        });
        let (_, result) = observe(
            MiniMaxHttpClient::new(config, GROUP_ID)
                .unwrap()
                .synthesize(request())
                .unwrap(),
        )
        .await;
        let retried = owner.await.unwrap();
        assert!(!retried);
        let error = result.unwrap_err();
        assert!(matches!(error,SpeechError::Http(code) if code==status));
        assert!(!format!("{error:?} {error}").contains("synthetic-secret"));
    }
}

#[tokio::test]
async fn timeout_cancel_drop_backpressure_and_final_without_eof_release_socket() {
    for mode in [
        "headers",
        "timeout",
        "cancel",
        "drop",
        "backpressure",
        "final_without_eof",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::minimax("synthetic-secret");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        config.timeout = Duration::from_millis(150);
        config.queue_capacity = 1;
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let owner = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            if mode != "headers" {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
                let mut body = audio("0102");
                if mode == "final_without_eof" {
                    body.extend(final_event("0102"));
                } else {
                    body.extend(audio("0304"));
                    body.extend(audio("0506"));
                }
                socket
                    .write_all(format!("{:x}\r\n", body.len()).as_bytes())
                    .await
                    .unwrap();
                socket.write_all(&body).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
            }
            let _ = ready.send(());
            let mut byte = [0];
            matches!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte)).await,
                Ok(Ok(0)) | Ok(Err(_))
            )
        });
        let mut stream = MiniMaxHttpClient::new(config, GROUP_ID)
            .unwrap()
            .synthesize(request())
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
                assert!(stream.next().await.is_none());
                drop(stream);
                Some(result.map(|result| result.map(|_| ())))
            }
            _ => {
                if mode == "backpressure" {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                let (_, result) = observe(stream).await;
                Some(Some(result))
            }
        };
        assert!(owner.await.unwrap(), "{mode}: socket remained open");
        match mode {
            "drop" => {}
            "cancel" => assert!(matches!(result, Some(Some(Err(SpeechError::Cancelled))))),
            _ => assert!(
                matches!(result, Some(Some(Err(SpeechError::Timeout)))),
                "{mode}: {result:?}"
            ),
        }
    }
}

#[tokio::test]
async fn cancelled_stream_discards_already_buffered_audio() {
    let (config, _, owner) = support::server(
        "200 OK",
        "text/event-stream",
        vec![audio("0102"), audio("0304"), final_event("01020304")],
        false,
        Duration::ZERO,
    )
    .await;
    let mut stream = MiniMaxHttpClient::new(config, GROUP_ID)
        .unwrap()
        .synthesize(request())
        .unwrap();
    assert!(matches!(stream.next().await, Some(Ok(_))));
    owner.await.unwrap();
    stream.cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::Cancelled))
    ));
    assert!(stream.next().await.is_none());
}

#[test]
fn local_validation_bounds_and_debug_redact_private_fields() {
    let mut config = SpeechConfig::minimax("synthetic-secret");
    config.base_url = "http://127.0.0.1:1".into();
    let client = MiniMaxHttpClient::new(config.clone(), GROUP_ID).unwrap();
    for kind in [
        "text",
        "model",
        "voice",
        "rate_zero",
        "rate_unsupported",
        "language",
        "speed_nan",
        "speed_infinity",
        "speed_low",
        "speed_high",
        "volume_nan",
        "volume_zero",
        "volume_low",
        "volume_high",
        "pitch_low",
        "pitch_high",
        "text_budget",
    ] {
        let mut request = request();
        match kind {
            "text" => request.text.clear(),
            "model" => request.model.clear(),
            "voice" => request.voice.clear(),
            "rate_zero" => request.sample_rate = 0,
            "rate_unsupported" => request.sample_rate = 48000,
            "language" => request.language = Some("x".repeat(129)),
            "speed_nan" => request.voice_settings.speed = f64::NAN,
            "speed_infinity" => request.voice_settings.speed = f64::INFINITY,
            "speed_low" => request.voice_settings.speed = 0.49,
            "speed_high" => request.voice_settings.speed = 2.01,
            "volume_nan" => request.voice_settings.volume = f64::NAN,
            "volume_zero" => request.voice_settings.volume = 0.0,
            "volume_low" => request.voice_settings.volume = -0.01,
            "volume_high" => request.voice_settings.volume = 10.01,
            "pitch_low" => request.voice_settings.pitch = -13,
            "pitch_high" => request.voice_settings.pitch = 13,
            "text_budget" => request.text = "x".repeat(65_537),
            _ => unreachable!(),
        }
        assert!(client.synthesize(request).is_err(), "{kind}");
    }
    for group in [String::new(), "x".repeat(1025)] {
        assert!(MiniMaxHttpClient::new(config.clone(), group).is_err());
    }
    for bad_limits in [
        MiniMaxLimits {
            max_wire_bytes: 0,
            ..MiniMaxLimits::default()
        },
        MiniMaxLimits {
            max_events: 0,
            ..MiniMaxLimits::default()
        },
        MiniMaxLimits {
            max_text_field_bytes: 0,
            ..MiniMaxLimits::default()
        },
    ] {
        assert!(MiniMaxHttpClient::with_limits(config.clone(), GROUP_ID, bad_limits).is_err());
    }
    config.max_json_bytes = 100;
    assert!(matches!(
        MiniMaxHttpClient::new(config.clone(), GROUP_ID)
            .unwrap()
            .synthesize(request()),
        Err(SpeechError::TooLarge)
    ));
    assert!(!format!("{client:?}").contains(GROUP_ID));
    assert!(!format!("{config:?} {client:?}").contains("synthetic-secret"));
    assert!(!format!("{:?}", request()).contains("你好"));
    for base in [
        "http://example.com",
        "https://user:password@example.com",
        "https://example.com?GroupId=secret",
        "https://example.com#fragment",
    ] {
        config.base_url = base.into();
        assert!(MiniMaxHttpClient::new(config.clone(), GROUP_ID).is_err());
    }
}

#[tokio::test]
async fn retained_original_method_negative_controls_expose_deliberate_corrections() {
    let source: Value = serde_json::from_str(include_str!("fixtures/minimax_source.json")).unwrap();
    assert_eq!(
        source["source_blob"],
        "d9cd8e83f2a58a98669707b9794cc1b527b5ef07"
    );
    let case = |name: &str| {
        source["results"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["case"] == name)
            .unwrap()
    };
    // The unmodified original method drops a complete single audio record at EOF.
    assert_eq!(case("one_complete_audio_at_eof")["pcm_hex"], json!([]));
    let (chunks, result, _) = run(
        vec![audio("00000100")],
        request(),
        MiniMaxLimits::default(),
        |_| {},
    )
    .await;
    assert_eq!(pcm(&chunks), [0, 0, 1, 0]);
    assert!(matches!(
        result,
        Err(SpeechError::Protocol("missing MiniMax final status"))
    ));
    // It also ignores an error-only first packet; native behavior must fail.
    assert_eq!(case("first_packet_error_ignored")["error_count"], 0);
    let (chunks, result, _) = run(
        vec![
            event(json!({"base_resp":{"status_code":1004},"data":null})),
            event(json!({})),
        ],
        request(),
        MiniMaxLimits::default(),
        |_| {},
    )
    .await;
    assert!(chunks.is_empty());
    assert!(matches!(
        result,
        Err(SpeechError::Protocol("MiniMax provider error"))
    ));
    assert_eq!(
        case("audio_with_error_status_still_emitted")["pcm_hex"],
        json!(["00000100"])
    );
    let (chunks, result, _) = run(
        vec![
            event(json!({"base_resp":{"status_code":1004},"data":{"status":1,"audio":"00000100"}})),
            event(json!({})),
        ],
        request(),
        MiniMaxLimits::default(),
        |_| {},
    )
    .await;
    assert!(chunks.is_empty());
    assert!(matches!(
        result,
        Err(SpeechError::Protocol("MiniMax provider error"))
    ));
    // Status 2 without extra_info was emitted by Python. Native never duplicates.
    assert_eq!(
        case("final_without_extra_info_is_audio")["pcm_hex"],
        json!(["00000100"])
    );
    let (chunks, result, _) = run(
        vec![
            audio("00000100"),
            event(json!({"data":{"audio":"00000100","status":2}})),
        ],
        request(),
        MiniMaxLimits::default(),
        |_| {},
    )
    .await;
    result.unwrap();
    assert_eq!(pcm(&chunks), [0, 0, 1, 0]);
}

#[tokio::test]
async fn response_headers_and_declared_content_length_are_bounded_before_body() {
    for kind in [
        "header_count",
        "header_bytes",
        "content_length",
        "encoding",
        "duplicate_type",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::minimax("synthetic-secret");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let mut headers = String::from(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n",
        );
        match kind {
            "header_count" => {
                for index in 0..65 {
                    headers.push_str(&format!("X-Fixture-{index}: 0\r\n"));
                }
            }
            "header_bytes" => {
                headers.push_str(&format!("X-Fixture: {}\r\n", "x".repeat(17 * 1024)))
            }
            "content_length" => {
                config.max_audio_bytes = 2;
                headers.push_str("Content-Length: 134217729\r\n");
            }
            "encoding" => headers.push_str("Content-Encoding: gzip\r\n"),
            "duplicate_type" => headers.push_str("Content-Type: text/event-stream\r\n"),
            _ => unreachable!(),
        }
        headers.push_str("\r\n");
        let owner = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            let _ = socket.write_all(headers.as_bytes()).await;
            let mut byte = [0];
            matches!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte)).await,
                Ok(Ok(0)) | Ok(Err(_))
            )
        });
        let (_, result) = observe(
            MiniMaxHttpClient::new(config, GROUP_ID)
                .unwrap()
                .synthesize(request())
                .unwrap(),
        )
        .await;
        assert!(owner.await.unwrap(), "{kind}: socket stayed open");
        match kind {
            "encoding" | "duplicate_type" => assert!(
                matches!(result, Err(SpeechError::ContentType)),
                "{kind}: {result:?}"
            ),
            _ => assert!(
                matches!(result, Err(SpeechError::TooLarge)),
                "{kind}: {result:?}"
            ),
        }
    }
}

#[tokio::test]
async fn successful_body_and_protocol_error_release_http_owner_without_replay() {
    for bad in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::minimax("synthetic-secret");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let owner = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
            let mut wire = audio("0102");
            if bad {
                wire.extend(audio("zz"));
            } else {
                wire.extend(final_event("0102"));
            }
            socket
                .write_all(format!("{:x}\r\n", wire.len()).as_bytes())
                .await
                .unwrap();
            socket.write_all(&wire).await.unwrap();
            socket.write_all(b"\r\n").await.unwrap();
            if !bad {
                socket.write_all(b"0\r\n\r\n").await.unwrap();
            }
            let mut byte = [0];
            let closed = matches!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte)).await,
                Ok(Ok(0)) | Ok(Err(_))
            );
            drop(socket);
            let retried = tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_ok();
            (closed, retried)
        });
        let (chunks, result) = observe(
            MiniMaxHttpClient::new(config, GROUP_ID)
                .unwrap()
                .synthesize(request())
                .unwrap(),
        )
        .await;
        let (closed, retried) = owner.await.unwrap();
        assert!(closed, "bad={bad}: HTTP owner was not released");
        assert!(!retried);
        assert_eq!(pcm(&chunks), [1, 2]);
        if bad {
            assert!(matches!(result, Err(SpeechError::Protocol(_))));
        } else {
            result.unwrap();
        }
    }
}
