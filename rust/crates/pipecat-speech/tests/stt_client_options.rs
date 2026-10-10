//! Real loopback-only transcription client protocol tests with synthetic data.
use pipecat_services::CancellationToken;
use pipecat_speech::{
    elevenlabs::{ElevenLabsHttpClient, ElevenLabsTranscriptionRequest},
    OpenAiTranscriptionOptions, SpeechClient, SpeechConfig, SpeechError, TranscriptionRequest,
};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

async fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let header_end = loop {
        let n = stream.read(&mut chunk).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(i) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break i + 4;
        }
        assert!(bytes.len() < 16 * 1024);
    };
    let headers = std::str::from_utf8(&bytes[..header_end]).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse().unwrap())
        })
        .unwrap();
    assert!(length < 2 * 1024 * 1024);
    while bytes.len() < header_end + length {
        let n = stream.read(&mut chunk).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    assert_eq!(bytes.len(), header_end + length);
    bytes
}

async fn server(body: Vec<u8>) -> (SpeechConfig, oneshot::Receiver<Vec<u8>>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::openai("synthetic-extension-key");
    config.base_url = format!("http://{}/v1", listener.local_addr().unwrap());
    config.timeout = Duration::from_secs(2);
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = tx.send(read_request(&mut stream).await);
        stream
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let _ = stream.write_all(&body).await;
    });
    (config, rx, task)
}

#[derive(Debug)]
struct Part {
    name: String,
    headers: String,
    body: Vec<u8>,
}
fn multipart(bytes: &[u8]) -> (String, Vec<Part>) {
    let header_end = bytes
        .windows(4)
        .position(|part| part == b"\r\n\r\n")
        .unwrap()
        + 4;
    let headers = std::str::from_utf8(&bytes[..header_end])
        .unwrap()
        .to_owned();
    let boundary = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .starts_with("content-type:")
                .then(|| {
                    line.split_once("boundary=")
                        .unwrap()
                        .1
                        .trim()
                        .trim_matches('"')
                })
        })
        .unwrap();
    let initial = format!("--{boundary}\r\n");
    let delimiter = format!("\r\n--{boundary}");
    let mut body = &bytes[header_end..];
    assert!(body.starts_with(initial.as_bytes()));
    body = &body[initial.len()..];
    let mut parts = Vec::new();
    loop {
        let end = body
            .windows(4)
            .position(|part| part == b"\r\n\r\n")
            .unwrap();
        let part_headers = std::str::from_utf8(&body[..end]).unwrap().to_owned();
        let name = part_headers
            .lines()
            .find(|line| {
                line.to_ascii_lowercase()
                    .starts_with("content-disposition:")
            })
            .unwrap()
            .split(';')
            .find_map(|field| {
                field
                    .trim()
                    .strip_prefix("name=\"")
                    .map(|v| v.trim_end_matches('"'))
            })
            .unwrap()
            .to_owned();
        body = &body[end + 4..];
        let end = body
            .windows(delimiter.len())
            .position(|p| p == delimiter.as_bytes())
            .unwrap();
        parts.push(Part {
            name,
            headers: part_headers,
            body: body[..end].to_vec(),
        });
        body = &body[end + delimiter.len()..];
        if body == b"--\r\n" {
            break;
        }
        assert!(body.starts_with(b"\r\n"));
        body = &body[2..];
    }
    (headers, parts)
}
fn fields<'a>(parts: &'a [Part], name: &str) -> Vec<&'a [u8]> {
    parts
        .iter()
        .filter(|part| part.name == name)
        .map(|part| part.body.as_slice())
        .collect()
}
fn one<'a>(parts: &'a [Part], name: &str) -> &'a [u8] {
    let found = fields(parts, name);
    assert_eq!(found.len(), 1, "{name}");
    found[0]
}
fn request(model: &str) -> TranscriptionRequest {
    TranscriptionRequest {
        model: model.into(),
        language: Some("en".into()),
        prompt: None,
    }
}

#[tokio::test]
async fn openai_keywords_are_bracketed_repeated_parts_and_preserve_raw_json() {
    let response = json!({"text":"  words  ","languages":[{"code":"en"}],"unknown":{"items":[null,1]},"optional":null});
    let (config, captured, task) = server(serde_json::to_vec(&response).unwrap()).await;
    let client = SpeechClient::new(config).unwrap();
    let options = OpenAiTranscriptionOptions {
        temperature: Some(0.0),
        keywords: vec!["first hint".into(), "second, distinct hint".into()],
        include_prob_metrics: false,
    };
    let mut req = request("gpt-transcribe");
    req.prompt = Some(String::new());
    let result = client
        .transcribe_pcm_with_options(
            &[1, 0, 2, 0],
            16000,
            1,
            req,
            options,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.transcript.text, "  words  ");
    assert_eq!(result.result, response);
    assert!(!format!("{result:?}").contains("words"));
    let (headers, parts) = multipart(&captured.await.unwrap());
    assert!(headers.starts_with("POST /v1/audio/transcriptions "));
    assert!(headers
        .to_ascii_lowercase()
        .contains("authorization: bearer synthetic-extension-key"));
    assert_eq!(one(&parts, "model"), b"gpt-transcribe");
    assert_eq!(one(&parts, "response_format"), b"json");
    assert_eq!(one(&parts, "temperature"), b"0");
    assert_eq!(one(&parts, "prompt"), b"");
    assert_eq!(one(&parts, "language"), b"en");
    assert_eq!(
        fields(&parts, "keywords[]"),
        vec![
            b"first hint".as_slice(),
            b"second, distinct hint".as_slice()
        ]
    );
    assert!(fields(&parts, "keywords").is_empty());
    assert!(fields(&parts, "include[]").is_empty());
    let file = parts.iter().find(|part| part.name == "file").unwrap();
    assert!(file.headers.contains("filename=\"audio.wav\""));
    assert!(file
        .headers
        .to_ascii_lowercase()
        .contains("content-type: audio/wav"));
    assert_eq!(file.body.len(), 48);
    assert_eq!(&file.body[..4], b"RIFF");
    assert_eq!(&file.body[8..12], b"WAVE");
    assert_eq!(
        u32::from_le_bytes(file.body[24..28].try_into().unwrap()),
        16000
    );
    assert_eq!(u16::from_le_bytes(file.body[22..24].try_into().unwrap()), 1);
    assert_eq!(&file.body[44..], &[1, 0, 2, 0]);
    task.await.unwrap();
}

#[tokio::test]
async fn openai_probability_models_select_exact_wire_and_retain_metadata() {
    for model in [
        "whisper-1",
        "gpt-4o-transcribe",
        "gpt-4o-mini-transcribe",
        "gpt-4o-mini-transcribe-2025-12-15",
    ] {
        let response = if model == "whisper-1" {
            json!({"text":"word","segments":[{"avg_logprob":-0.1,"no_speech_prob":0.01}],"extra":null})
        } else {
            json!({"text":"word","logprobs":[{"token":"word","logprob":-0.1,"bytes":[119,111,114,100]}]})
        };
        let (config, captured, task) = server(serde_json::to_vec(&response).unwrap()).await;
        let options = OpenAiTranscriptionOptions {
            temperature: Some(0.75),
            include_prob_metrics: true,
            ..Default::default()
        };
        let result = SpeechClient::new(config)
            .unwrap()
            .transcribe_pcm_with_options(
                &[0, 0],
                8000,
                1,
                request(model),
                options,
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.result, response);
        let (_, parts) = multipart(&captured.await.unwrap());
        assert_eq!(one(&parts, "temperature"), b"0.75");
        assert!(fields(&parts, "keywords[]").is_empty());
        if model == "whisper-1" {
            assert_eq!(one(&parts, "response_format"), b"verbose_json");
            assert!(fields(&parts, "include[]").is_empty());
        } else {
            assert_eq!(one(&parts, "response_format"), b"json");
            assert_eq!(one(&parts, "include[]"), b"logprobs");
            assert!(fields(&parts, "include").is_empty());
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn openai_empty_options_keep_generic_models_and_omissions() {
    for model in [
        "gpt-transcribe",
        "compatible-endpoint-model",
        "gpt-4o-transcribe-diarize",
    ] {
        let (config, captured, task) = server(br#"{"text":"","logprobs":null}"#.to_vec()).await;
        let mut req = request(model);
        req.language = None;
        let result = SpeechClient::new(config)
            .unwrap()
            .transcribe_pcm_with_options(
                &[0, 0],
                8000,
                1,
                req,
                OpenAiTranscriptionOptions::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.transcript.text, "");
        assert_eq!(result.result.get("logprobs"), Some(&Value::Null));
        let (_, parts) = multipart(&captured.await.unwrap());
        assert_eq!(one(&parts, "model"), model.as_bytes());
        assert_eq!(one(&parts, "response_format"), b"json");
        for field in [
            "temperature",
            "prompt",
            "language",
            "keywords[]",
            "include[]",
        ] {
            assert!(fields(&parts, field).is_empty());
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn openai_legacy_struct_literals_and_json_behavior_are_unchanged() {
    let (config, captured, task) = server(br#"{"text":"ok","other":null}"#.to_vec()).await;
    let req = TranscriptionRequest {
        model: "whisper-private-compatible".into(),
        language: None,
        prompt: Some("legacy prompt".into()),
    };
    let result = SpeechClient::new(config)
        .unwrap()
        .transcribe_pcm(&[0, 0], 16000, 1, req, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result, pipecat_speech::Transcript { text: "ok".into() });
    assert_eq!(TranscriptionRequest::default().model, "gpt-transcribe");
    let (_, parts) = multipart(&captured.await.unwrap());
    assert_eq!(one(&parts, "response_format"), b"json");
    assert_eq!(one(&parts, "prompt"), b"legacy prompt");
    for field in ["temperature", "keywords[]", "include[]"] {
        assert!(fields(&parts, field).is_empty());
    }
    task.await.unwrap();
}

#[tokio::test]
async fn openai_invalid_and_unsupported_options_fail_before_network() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::openai("unused-synthetic-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let client = SpeechClient::new(config).unwrap();
    let mut cases = Vec::new();
    for temperature in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.1, 1.1] {
        cases.push((
            request("whisper-1"),
            OpenAiTranscriptionOptions {
                temperature: Some(temperature),
                ..Default::default()
            },
        ));
    }
    for model in [
        "gpt-transcribe",
        "gpt-4o-transcribe-diarize",
        "whisper-alias",
        "gpt-4o-transcribe-custom",
        "private-model",
    ] {
        cases.push((
            request(model),
            OpenAiTranscriptionOptions {
                include_prob_metrics: true,
                ..Default::default()
            },
        ));
    }
    for model in [
        "whisper-1",
        "gpt-4o-transcribe",
        "gpt-4o-mini-transcribe",
        "gpt-4o-transcribe-diarize",
        "gpt-transcribe-alias",
    ] {
        cases.push((
            request(model),
            OpenAiTranscriptionOptions {
                keywords: vec!["SENTINEL_KEYWORD".into()],
                ..Default::default()
            },
        ));
    }
    cases.push((
        request("gpt-transcribe"),
        OpenAiTranscriptionOptions {
            keywords: vec![String::new()],
            ..Default::default()
        },
    ));
    cases.push((
        request("private-model"),
        OpenAiTranscriptionOptions {
            temperature: Some(0.0),
            ..Default::default()
        },
    ));
    cases.push((request(""), OpenAiTranscriptionOptions::default()));
    let mut req = request("gpt-4o-transcribe-diarize");
    req.prompt = Some("SENTINEL_PROMPT".into());
    cases.push((req, OpenAiTranscriptionOptions::default()));
    for (req, options) in cases {
        assert!(!format!("{options:?}").contains("SENTINEL_KEYWORD"));
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            client.transcribe_pcm_with_options(
                &[0, 0],
                16000,
                1,
                req,
                options,
                CancellationToken::new(),
            ),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(matches!(error, SpeechError::Invalid(_)));
        assert!(!format!("{error:?} {error}").contains("SENTINEL"));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn elevenlabs_raw_result_retains_unknown_null_and_validated_projection() {
    let response = json!({
        "text":" words ","language_code":null,"language_probability":0.9,
        "words":[{"text":"words","type":"word","start":0.0,"end":0.2,"speaker_id":"speaker_1","extra":null}],
        "unknown":{"nested":[1,null]},"transcription_id":"synthetic-id"
    });
    let (mut config, captured, task) = server(serde_json::to_vec(&response).unwrap()).await;
    config.base_url.truncate(config.base_url.len() - 3);
    let req = ElevenLabsTranscriptionRequest {
        model: "scribe_v2".into(),
        language_code: None,
        tag_audio_events: Some(false),
        keyterms: vec!["hint a".into(), "hint b".into()],
        no_verbatim: Some(false),
    };
    let result = ElevenLabsHttpClient::new(config)
        .unwrap()
        .transcribe_pcm_with_result(&[1, 0, 2, 0], 16000, 2, req, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.result, response);
    assert_eq!(result.transcript.text, " words ");
    assert_eq!(result.transcript.language_code, None);
    assert_eq!(result.transcript.language_probability, Some(0.9));
    assert_eq!(
        result.transcript.words[0].speaker_id.as_deref(),
        Some("speaker_1")
    );
    assert!(!format!("{result:?}").contains("words"));
    assert!(!format!("{result:?}").contains("synthetic-id"));
    let (headers, parts) = multipart(&captured.await.unwrap());
    assert!(headers.starts_with("POST /v1/speech-to-text "));
    assert!(headers
        .to_ascii_lowercase()
        .contains("xi-api-key: synthetic-extension-key"));
    assert_eq!(one(&parts, "model_id"), b"scribe_v2");
    assert_eq!(
        fields(&parts, "keyterms"),
        vec![b"hint a".as_slice(), b"hint b".as_slice()]
    );
    assert!(fields(&parts, "keyterms[]").is_empty());
    assert!(fields(&parts, "language_code").is_empty());
    assert_eq!(one(&parts, "tag_audio_events"), b"false");
    assert_eq!(one(&parts, "no_verbatim"), b"false");
    let file = parts.iter().find(|part| part.name == "file").unwrap();
    assert!(file
        .headers
        .to_ascii_lowercase()
        .contains("content-type: audio/x-wav"));
    assert_eq!(u16::from_le_bytes(file.body[22..24].try_into().unwrap()), 2);
    assert_eq!(&file.body[44..], &[1, 0, 2, 0]);
    task.await.unwrap();
}

#[tokio::test]
async fn elevenlabs_absent_language_is_not_replaced_with_null_in_raw_result() {
    let response = json!({"text":"ok","new_field":null});
    let (config, _, task) = server(serde_json::to_vec(&response).unwrap()).await;
    let result = ElevenLabsHttpClient::new(config)
        .unwrap()
        .transcribe_pcm_with_result(
            &[0, 0],
            8000,
            1,
            ElevenLabsTranscriptionRequest::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.transcript.language_code, None);
    assert!(!result
        .result
        .as_object()
        .unwrap()
        .contains_key("language_code"));
    assert!(!result.result.as_object().unwrap().contains_key("words"));
    assert_eq!(result.result, response);
    task.await.unwrap();
}

#[tokio::test]
async fn raw_results_still_reject_invalid_json_projection_and_timing() {
    for body in [
        br#"{"text":{"SENTINEL":1}}"#.as_slice(),
        br#"{"text":"ok","language_probability":1.1}"#.as_slice(),
        br#"{"text":"ok","words":[{"text":"x","type":"word","start":0.2,"end":0.1}]}"#.as_slice(),
        br#"{"text":"ok","words":[{"text":"x","type":"word","start":-0.1,"end":0.1}]}"#.as_slice(),
        br#"{"text":"ok","language_probability":1e999}"#.as_slice(),
    ] {
        let (config, _, task) = server(body.to_vec()).await;
        let error = ElevenLabsHttpClient::new(config)
            .unwrap()
            .transcribe_pcm_with_result(
                &[0, 0],
                8000,
                1,
                ElevenLabsTranscriptionRequest::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SpeechError::Json(_) | SpeechError::Protocol(_)
        ));
        assert!(!format!("{error:?} {error}").contains("SENTINEL"));
        task.await.unwrap();
    }
    for body in [
        br#"{"text":"SENTINEL""#.as_slice(),
        br#"{"text":null}"#.as_slice(),
        br#"{"missing":"SENTINEL"}"#.as_slice(),
    ] {
        let (config, _, task) = server(body.to_vec()).await;
        let error = SpeechClient::new(config)
            .unwrap()
            .transcribe_pcm_with_options(
                &[0, 0],
                8000,
                1,
                request("gpt-transcribe"),
                OpenAiTranscriptionOptions::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, SpeechError::Json(_)));
        assert!(!format!("{error:?} {error}").contains("SENTINEL"));
        task.await.unwrap();
    }
}

#[tokio::test]
async fn both_raw_methods_retain_response_and_wav_byte_limits() {
    for eleven in [false, true] {
        let (mut config, _, task) =
            server(br#"{"text":"ok","unknown":"oversized"}"#.to_vec()).await;
        config.max_json_bytes = 16;
        let error = if eleven {
            ElevenLabsHttpClient::new(config)
                .unwrap()
                .transcribe_pcm_with_result(
                    &[0, 0],
                    8000,
                    1,
                    ElevenLabsTranscriptionRequest::default(),
                    CancellationToken::new(),
                )
                .await
                .unwrap_err()
        } else {
            SpeechClient::new(config)
                .unwrap()
                .transcribe_pcm_with_options(
                    &[0, 0],
                    8000,
                    1,
                    request("gpt-transcribe"),
                    OpenAiTranscriptionOptions::default(),
                    CancellationToken::new(),
                )
                .await
                .unwrap_err()
        };
        assert!(matches!(error, SpeechError::TooLarge));
        task.await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::openai("unused-key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        config.max_audio_bytes = 45;
        let error = if eleven {
            ElevenLabsHttpClient::new(config)
                .unwrap()
                .transcribe_pcm_with_result(
                    &[0, 0],
                    8000,
                    1,
                    ElevenLabsTranscriptionRequest::default(),
                    CancellationToken::new(),
                )
                .await
                .unwrap_err()
        } else {
            SpeechClient::new(config)
                .unwrap()
                .transcribe_pcm_with_options(
                    &[0, 0],
                    8000,
                    1,
                    request("gpt-transcribe"),
                    OpenAiTranscriptionOptions::default(),
                    CancellationToken::new(),
                )
                .await
                .unwrap_err()
        };
        assert!(matches!(error, SpeechError::TooLarge));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn raw_methods_keep_cancellation_during_partial_response() {
    for eleven in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::openai("synthetic-key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        config.timeout = Duration::from_secs(2);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request(&mut stream).await;
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 40\r\n\r\n{\"text\":\"").await.unwrap();
            let _ = ready_tx.send(());
            let _ = release_rx.await;
        });
        let cancel = CancellationToken::new();
        let request_cancel = cancel.clone();
        let request = tokio::spawn(async move {
            if eleven {
                ElevenLabsHttpClient::new(config)
                    .unwrap()
                    .transcribe_pcm_with_result(
                        &[0, 0],
                        8000,
                        1,
                        ElevenLabsTranscriptionRequest::default(),
                        request_cancel,
                    )
                    .await
                    .map(|_| ())
            } else {
                SpeechClient::new(config)
                    .unwrap()
                    .transcribe_pcm_with_options(
                        &[0, 0],
                        8000,
                        1,
                        TranscriptionRequest::default(),
                        OpenAiTranscriptionOptions::default(),
                        request_cancel,
                    )
                    .await
                    .map(|_| ())
            }
        });
        ready_rx.await.unwrap();
        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(result, Err(SpeechError::Cancelled)));
        let _ = release_tx.send(());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn openai_temperature_upper_boundary_and_prompt_are_explicit() {
    let (config, captured, task) = server(br#"{"text":"ok"}"#.to_vec()).await;
    let req = TranscriptionRequest {
        model: "gpt-transcribe".into(),
        language: Some("fr".into()),
        prompt: Some("synthetic prompt".into()),
    };
    let options = OpenAiTranscriptionOptions {
        temperature: Some(1.0),
        ..Default::default()
    };
    SpeechClient::new(config)
        .unwrap()
        .transcribe_pcm_with_options(&[0, 0], 8000, 1, req, options, CancellationToken::new())
        .await
        .unwrap();
    let (_, parts) = multipart(&captured.await.unwrap());
    assert_eq!(one(&parts, "temperature"), b"1");
    assert_eq!(one(&parts, "prompt"), b"synthetic prompt");
    assert_eq!(one(&parts, "language"), b"fr");
    task.await.unwrap();
}

#[tokio::test]
async fn elevenlabs_raw_method_retains_request_validation_before_network() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::elevenlabs("unused-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let client = ElevenLabsHttpClient::new(config).unwrap();
    let invalid = [
        ElevenLabsTranscriptionRequest {
            model: String::new(),
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            language_code: Some(String::new()),
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            keyterms: vec![String::new()],
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            model: "scribe_v1".into(),
            no_verbatim: Some(false),
            ..Default::default()
        },
    ];
    for req in invalid {
        let error = client
            .transcribe_pcm_with_result(&[0, 0], 8000, 1, req, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(error, SpeechError::Invalid(_)));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn raw_methods_keep_timeout_during_partial_response() {
    for eleven in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::openai("synthetic-key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        config.timeout = Duration::from_millis(100);
        let (release_tx, release_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request(&mut stream).await;
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 40\r\n\r\n{\"text\":\"").await;
            let _ = release_rx.await;
        });
        let result = tokio::time::timeout(Duration::from_secs(2), async move {
            if eleven {
                ElevenLabsHttpClient::new(config)
                    .unwrap()
                    .transcribe_pcm_with_result(
                        &[0, 0],
                        8000,
                        1,
                        ElevenLabsTranscriptionRequest::default(),
                        CancellationToken::new(),
                    )
                    .await
                    .map(|_| ())
            } else {
                SpeechClient::new(config)
                    .unwrap()
                    .transcribe_pcm_with_options(
                        &[0, 0],
                        8000,
                        1,
                        TranscriptionRequest::default(),
                        OpenAiTranscriptionOptions::default(),
                        CancellationToken::new(),
                    )
                    .await
                    .map(|_| ())
            }
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(SpeechError::Timeout)));
        let _ = release_tx.send(());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn openai_new_method_accepts_exact_text_and_list_boundaries() {
    let mut cases = Vec::new();
    cases.push((
        request(&"m".repeat(256)),
        OpenAiTranscriptionOptions::default(),
    ));
    let mut req = request("gpt-transcribe");
    req.language = Some("l".repeat(64));
    cases.push((req, OpenAiTranscriptionOptions::default()));
    let mut req = request("gpt-transcribe");
    req.prompt = Some("p".repeat(8192));
    cases.push((req, OpenAiTranscriptionOptions::default()));
    for keywords in [
        vec!["x".into(); 100],
        vec!["é".repeat(128)],
        vec!["x".repeat(256); 32],
    ] {
        cases.push((
            request("gpt-transcribe"),
            OpenAiTranscriptionOptions {
                keywords,
                ..Default::default()
            },
        ));
    }
    let mut req = request("gpt-transcribe");
    req.prompt = Some("p".repeat(16 * 1024 - req.model.len() - 2 - 8192));
    cases.push((
        req,
        OpenAiTranscriptionOptions {
            keywords: vec!["x".repeat(256); 32],
            ..Default::default()
        },
    ));
    for (req, options) in cases {
        let expected = req.clone();
        let expected_options = options.clone();
        let (config, captured, task) = server(br#"{"text":"ok"}"#.to_vec()).await;
        SpeechClient::new(config)
            .unwrap()
            .transcribe_pcm_with_options(&[0, 0], 8000, 1, req, options, CancellationToken::new())
            .await
            .unwrap();
        let (_, parts) = multipart(&captured.await.unwrap());
        assert_eq!(one(&parts, "model"), expected.model.as_bytes());
        assert_eq!(
            one(&parts, "language"),
            expected.language.as_deref().unwrap().as_bytes()
        );
        if let Some(prompt) = expected.prompt {
            assert_eq!(one(&parts, "prompt"), prompt.as_bytes());
        }
        assert_eq!(
            fields(&parts, "keywords[]"),
            expected_options
                .keywords
                .iter()
                .map(String::as_bytes)
                .collect::<Vec<_>>()
        );
        task.await.unwrap();
    }
}

#[tokio::test]
async fn openai_new_method_rejects_each_text_budget_before_network() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::openai("unused-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let client = SpeechClient::new(config).unwrap();
    let mut cases = Vec::new();
    cases.push((
        request(&"m".repeat(257)),
        OpenAiTranscriptionOptions::default(),
    ));
    let mut req = request("gpt-transcribe");
    req.language = Some("l".repeat(65));
    cases.push((req, OpenAiTranscriptionOptions::default()));
    let mut req = request("gpt-transcribe");
    req.prompt = Some("p".repeat(8193));
    cases.push((req, OpenAiTranscriptionOptions::default()));
    let mut excessive_total = vec!["x".repeat(256); 32];
    excessive_total.push("x".into());
    for keywords in [
        vec!["x".into(); 101],
        vec!["x".repeat(257)],
        vec!["é".repeat(129)],
        excessive_total,
    ] {
        cases.push((
            request("gpt-transcribe"),
            OpenAiTranscriptionOptions {
                keywords,
                ..Default::default()
            },
        ));
    }
    let mut req = request("gpt-transcribe");
    req.prompt = Some("p".repeat(16 * 1024 + 1 - req.model.len() - 2 - 8192));
    cases.push((
        req,
        OpenAiTranscriptionOptions {
            keywords: vec!["x".repeat(256); 32],
            ..Default::default()
        },
    ));
    for (req, options) in cases {
        let error = client
            .transcribe_pcm_with_options(&[0, 0], 8000, 1, req, options, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SpeechError::Invalid("OpenAI transcription settings limits")
        ));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn elevenlabs_new_method_has_matching_text_and_keyterm_bounds() {
    let valid = [
        ElevenLabsTranscriptionRequest {
            model: "m".repeat(256),
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            language_code: Some("l".repeat(64)),
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            keyterms: vec!["x".into(); 100],
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            keyterms: vec!["é".repeat(128)],
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            keyterms: vec!["x".repeat(256); 32],
            ..Default::default()
        },
    ];
    for req in valid {
        let (config, _, task) = server(br#"{"text":"ok"}"#.to_vec()).await;
        ElevenLabsHttpClient::new(config)
            .unwrap()
            .transcribe_pcm_with_result(&[0, 0], 8000, 1, req, CancellationToken::new())
            .await
            .unwrap();
        task.await.unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::elevenlabs("unused-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let client = ElevenLabsHttpClient::new(config).unwrap();
    let mut excessive_total = vec!["x".repeat(256); 32];
    excessive_total.push("x".into());
    let invalid = [
        ElevenLabsTranscriptionRequest {
            model: "m".repeat(257),
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            language_code: Some("l".repeat(65)),
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            keyterms: vec!["x".into(); 101],
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            keyterms: vec!["x".repeat(257)],
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            keyterms: vec!["é".repeat(129)],
            ..Default::default()
        },
        ElevenLabsTranscriptionRequest {
            keyterms: excessive_total,
            ..Default::default()
        },
    ];
    for req in invalid {
        let error = client
            .transcribe_pcm_with_result(&[0, 0], 8000, 1, req, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SpeechError::Invalid("ElevenLabs transcription settings limits")
        ));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn legacy_methods_keep_accepting_requests_above_new_text_budgets() {
    let (config, captured, task) =
        server(br#"{"text":"ok","logprobs":[{"logprob":1}]}"#.to_vec()).await;
    let req = TranscriptionRequest {
        model: "m".repeat(257),
        language: Some("l".repeat(65)),
        prompt: Some("p".repeat(8193)),
    };
    SpeechClient::new(config)
        .unwrap()
        .transcribe_pcm(&[0, 0], 8000, 1, req, CancellationToken::new())
        .await
        .unwrap();
    let (_, parts) = multipart(&captured.await.unwrap());
    assert_eq!(one(&parts, "model").len(), 257);
    assert_eq!(one(&parts, "language").len(), 65);
    assert_eq!(one(&parts, "prompt").len(), 8193);
    task.await.unwrap();

    let (config, captured, task) = server(br#"{"text":"ok"}"#.to_vec()).await;
    let req = ElevenLabsTranscriptionRequest {
        model: "m".repeat(257),
        language_code: Some("l".repeat(65)),
        keyterms: vec!["x".repeat(257); 101],
        ..Default::default()
    };
    ElevenLabsHttpClient::new(config)
        .unwrap()
        .transcribe_pcm(&[0, 0], 8000, 1, req, CancellationToken::new())
        .await
        .unwrap();
    let (_, parts) = multipart(&captured.await.unwrap());
    assert_eq!(one(&parts, "model_id").len(), 257);
    assert_eq!(one(&parts, "language_code").len(), 65);
    assert_eq!(fields(&parts, "keyterms").len(), 101);
    assert_eq!(fields(&parts, "keyterms")[0].len(), 257);
    task.await.unwrap();
}

#[tokio::test]
async fn openai_raw_probability_metadata_is_validated_without_fabrication() {
    let invalid = [
        json!({"text":"ok","logprobs":[{"logprob":0.01}]}),
        json!({"text":"ok","logprobs":[{"logprob":"SENTINEL_BAD_LOGPROB"}]}),
        json!({"text":"ok","logprobs":{}}),
        json!({"text":"ok","logprobs":[null]}),
        json!({"text":"ok","segments":[{"avg_logprob":0.01}]}),
        json!({"text":"ok","segments":[{"no_speech_prob":-0.01}]}),
        json!({"text":"ok","segments":[{"no_speech_prob":1.01}]}),
        json!({"text":"ok","segments":[{"avg_logprob":false}]}),
        json!({"text":"ok","segments":[{"no_speech_prob":"SENTINEL_BAD_PROB"}]}),
        json!({"text":"ok","segments":false}),
    ];
    for response in invalid {
        let (config, _, task) = server(serde_json::to_vec(&response).unwrap()).await;
        let error = SpeechClient::new(config)
            .unwrap()
            .transcribe_pcm_with_options(
                &[0, 0],
                8000,
                1,
                TranscriptionRequest::default(),
                OpenAiTranscriptionOptions::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SpeechError::Protocol("invalid transcription probability metadata")
        ));
        assert!(!format!("{error:?} {error}").contains("SENTINEL"));
        task.await.unwrap();
    }
    for response in [
        json!({"text":"ok","logprobs":null,"segments":null}),
        json!({"text":"ok","logprobs":[{}, {"logprob":null}, {"logprob":0.0}, {"logprob":-1000.0}]}),
        json!({"text":"ok","segments":[{}, {"avg_logprob":null,"no_speech_prob":null}, {"avg_logprob":0.0,"no_speech_prob":0.0}, {"avg_logprob":-1000.0,"no_speech_prob":1.0}]}),
        json!({"text":"ok","unknown_probability":7.0,"unknown":{"logprob":"opaque"}}),
    ] {
        let (config, _, task) = server(serde_json::to_vec(&response).unwrap()).await;
        let result = SpeechClient::new(config)
            .unwrap()
            .transcribe_pcm_with_options(
                &[0, 0],
                8000,
                1,
                TranscriptionRequest::default(),
                OpenAiTranscriptionOptions::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(result.result, response);
        task.await.unwrap();
    }
    for body in [
        br#"{"text":"ok","logprobs":[{"logprob":-1e999}]}"#.as_slice(),
        br#"{"text":"ok","segments":[{"no_speech_prob":1e999}]}"#.as_slice(),
    ] {
        let (config, _, task) = server(body.to_vec()).await;
        let error = SpeechClient::new(config)
            .unwrap()
            .transcribe_pcm_with_options(
                &[0, 0],
                8000,
                1,
                TranscriptionRequest::default(),
                OpenAiTranscriptionOptions::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, SpeechError::Json(_)));
        task.await.unwrap();
    }
}

#[tokio::test]
async fn diarization_duration_boundary_uses_actual_pcm_geometry() {
    const RATE: u32 = 8000;
    for channels in [1u16, 2] {
        for frames in [RATE as usize * 30 - 1, RATE as usize * 30] {
            let pcm = vec![0; frames * usize::from(channels) * 2];
            let (config, captured, task) = server(br#"{"text":"ok"}"#.to_vec()).await;
            SpeechClient::new(config)
                .unwrap()
                .transcribe_pcm_with_options(
                    &pcm,
                    RATE,
                    channels,
                    request("gpt-4o-transcribe-diarize"),
                    OpenAiTranscriptionOptions::default(),
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            let (_, parts) = multipart(&captured.await.unwrap());
            assert_eq!(one(&parts, "file").len(), pcm.len() + 44);
            assert!(fields(&parts, "chunking_strategy").is_empty());
            assert_eq!(one(&parts, "response_format"), b"json");
            task.await.unwrap();
        }
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::openai("unused-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let client = SpeechClient::new(config).unwrap();
    for channels in [1u16, 2] {
        let pcm = vec![0; (RATE as usize * 30 + 1) * usize::from(channels) * 2];
        let error = client
            .transcribe_pcm_with_options(
                &pcm,
                RATE,
                channels,
                request("gpt-4o-transcribe-diarize"),
                OpenAiTranscriptionOptions::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SpeechError::Invalid(
                "OpenAI diarization above 30 seconds requires a chunking strategy"
            )
        ));
    }
    for (pcm, rate, channels) in [
        (vec![0], RATE, 1),
        (vec![0, 0], RATE, 2),
        (vec![0, 0], 0, 1),
        (vec![0, 0], RATE, 0),
    ] {
        let error = client
            .transcribe_pcm_with_options(
                &pcm,
                rate,
                channels,
                request("gpt-4o-transcribe-diarize"),
                OpenAiTranscriptionOptions::default(),
                CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, SpeechError::Audio(_)));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn legacy_diarization_method_keeps_existing_long_input_behavior() {
    let (config, captured, task) = server(br#"{"text":"ok"}"#.to_vec()).await;
    let pcm = vec![0; (8000 * 30 + 1) * 2];
    SpeechClient::new(config)
        .unwrap()
        .transcribe_pcm(
            &pcm,
            8000,
            1,
            request("gpt-4o-transcribe-diarize"),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    let (_, parts) = multipart(&captured.await.unwrap());
    assert_eq!(one(&parts, "file").len(), pcm.len() + 44);
    task.await.unwrap();
}
