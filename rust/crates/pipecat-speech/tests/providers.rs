//! Wire fixtures exercise real loopback HTTP, never live speech accounts.
mod support;
use pipecat_services::CancellationToken;
use pipecat_speech::{cartesia::*, deepgram::*, elevenlabs::*, *};
use serde_json::json;
use std::{collections::HashMap, time::Duration};
use support::server;

async fn collect(mut stream: SpeechStream) -> Result<Vec<u8>, SpeechError> {
    let mut output = vec![];
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        assert_eq!(chunk.num_channels, 1);
        assert_eq!(chunk.pcm.len() % 2, 0);
        output.extend(chunk.pcm);
    }
    Ok(output)
}
#[tokio::test]
async fn elevenlabs_tts_uses_encoded_path_ndjson_and_typed_alignment() {
    let records = concat!(
        "\r\n",
        r#"{"audio_base64":"AQ==","alignment":{"characters":["你"],"character_start_times_seconds":[0.0],"character_end_times_seconds":[0.1]}}"#,
        "\n",
        r#"{"audio_base64":"AgME","normalized_alignment":{"characters":["好"],"character_start_times_seconds":[0.1],"character_end_times_seconds":[0.2]}}"#
    );
    let (config, capture, server) = server(
        "200 OK",
        "application/json",
        records.as_bytes().chunks(7).map(<[u8]>::to_vec).collect(),
        false,
        Duration::ZERO,
    )
    .await;
    let mut request = ElevenLabsSynthesisRequest::new("你好", "voice /?#%中文");
    request.sample_rate = 44100;
    request.language_code = Some("zh".into());
    request.previous_text = Some("hello".into());
    request.voice_settings = Some(ElevenLabsVoiceSettings {
        stability: Some(0.5),
        similarity_boost: Some(0.8),
        style: Some(0.2),
        speed: Some(1.2),
        use_speaker_boost: Some(false),
    });
    request.optimize_streaming_latency = Some(2);
    request.enable_logging = Some(false);
    request.apply_text_normalization = Some(TextNormalization::Off);
    let mut stream = ElevenLabsHttpClient::new(config)
        .unwrap()
        .synthesize(request)
        .unwrap();
    let mut pcm = vec![];
    let mut alignments = vec![];
    while let Some(event) = stream.next().await {
        match event.unwrap() {
            ElevenLabsEvent::Audio(audio) => {
                assert_eq!(audio.sample_rate, 44100);
                assert_eq!(audio.num_channels, 1);
                pcm.extend(audio.pcm);
            }
            ElevenLabsEvent::Alignment {
                alignment,
                normalized_alignment,
            } => alignments.push(alignment.or(normalized_alignment).unwrap()),
        }
    }
    assert_eq!(pcm, [1, 2, 3, 4]);
    assert_eq!(alignments[0].characters, ["你"]);
    assert_eq!(alignments[1].character_end_times_seconds, [0.2]);
    let req = capture.await.unwrap();
    assert_eq!(req.header("xi-api-key"), Some("synthetic-secret"));
    assert_eq!(req.header("authorization"), None);
    assert_eq!(req.header("content-type"), Some("application/json"));
    assert_eq!(
        req.url().path(),
        "/proxy/v1/text-to-speech/voice%20%2F%3F%23%25%E4%B8%AD%E6%96%87/stream/with-timestamps"
    );
    let query: HashMap<_, _> = req.url().query_pairs().into_owned().collect();
    assert_eq!(query["output_format"], "pcm_44100");
    assert_eq!(query["enable_logging"], "false");
    assert_eq!(query["optimize_streaming_latency"], "2");
    let body = req.json();
    assert_eq!(body["model_id"], "eleven_flash_v2_5");
    assert_eq!(body["text"], "你好");
    assert_eq!(body["previous_text"], "hello");
    assert_eq!(body["language_code"], "zh");
    assert_eq!(body["voice_settings"]["speed"], 1.2);
    assert_eq!(body["apply_text_normalization"], "off");
    server.await.unwrap();
}
#[tokio::test]
async fn elevenlabs_stt_sends_repeated_fields_and_stereo_wav() {
    let body = json!({"text":"hello", "language_code":"eng", "language_probability":0.95, "words":[{"text":"hello", "type":"word", "start":0.0, "end":0.5, "speaker_id":"speaker_0"}]}).to_string();
    let (config, capture, server) = server(
        "200 OK",
        "application/json",
        vec![body.into_bytes()],
        false,
        Duration::ZERO,
    )
    .await;
    let request = ElevenLabsTranscriptionRequest {
        keyterms: vec!["Pipecat".into(), "Rust & speech".into()],
        tag_audio_events: Some(false),
        no_verbatim: Some(true),
        ..Default::default()
    };
    let result = ElevenLabsHttpClient::new(config)
        .unwrap()
        .transcribe_pcm(
            &[1, 0, 2, 0, 3, 0, 4, 0],
            16000,
            2,
            request,
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.language_code.as_deref(), Some("eng"));
    assert_eq!(result.words[0].speaker_id.as_deref(), Some("speaker_0"));
    let req = capture.await.unwrap();
    assert_eq!(req.url().path(), "/proxy/v1/speech-to-text");
    assert_eq!(req.header("xi-api-key"), Some("synthetic-secret"));
    let body = String::from_utf8_lossy(&req.body);
    assert!(req
        .header("content-type")
        .unwrap()
        .starts_with("multipart/form-data; boundary="));
    for field in [
        "model_id",
        "language_code",
        "tag_audio_events",
        "no_verbatim",
    ] {
        assert!(body.contains(&format!("name=\"{field}\"")));
    }
    assert_eq!(body.matches("name=\"keyterms\"").count(), 2);
    assert!(body.contains("Rust & speech"));
    assert!(body.contains("scribe_v2"));
    assert!(body.contains("\r\n\r\neng\r\n"));
    assert!(
        body.contains("Content-Type: audio/x-wav") || body.contains("content-type: audio/x-wav")
    );
    let start = req.body.windows(4).position(|s| s == b"RIFF").unwrap();
    let wav = &req.body[start..start + 52];
    let wav = pipecat_audio::read_pcm16_wav(wav).unwrap();
    assert_eq!(wav.format.sample_rate(), 16000);
    assert_eq!(wav.format.channels(), 2);
    assert_eq!(wav.pcm, [1, 0, 2, 0, 3, 0, 4, 0]);
    server.await.unwrap();
}
#[tokio::test]
async fn elevenlabs_v3_omits_unsupported_context_but_preserves_dictionary() {
    let (config, capture, server) = server(
        "200 OK",
        "application/x-ndjson",
        vec![b"{\"audio_base64\":\"AAA=\"}\n".to_vec()],
        false,
        Duration::ZERO,
    )
    .await;
    let mut request = ElevenLabsSynthesisRequest::new("hi", "id");
    request.model = "eleven_v3".into();
    request.previous_text = Some("prior".into());
    request
        .pronunciation_dictionary_locators
        .push(PronunciationDictionaryLocator {
            pronunciation_dictionary_id: "dict".into(),
            version_id: "version".into(),
        });
    let mut stream = ElevenLabsHttpClient::new(config)
        .unwrap()
        .synthesize(request)
        .unwrap();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    let body = capture.await.unwrap().json();
    assert!(body.get("previous_text").is_none());
    assert!(body.get("voice_settings").is_none());
    assert_eq!(
        body["pronunciation_dictionary_locators"][0]["version_id"],
        "version"
    );
    server.await.unwrap();
}
#[tokio::test]
async fn deepgram_tts_sends_token_and_query_and_reassembles_linear16() {
    let (config, capture, server) = server(
        "200 OK",
        "audio/l16;rate=16000;channels=1",
        vec![vec![1], vec![2, 3], vec![4]],
        false,
        Duration::ZERO,
    )
    .await;
    let mut request = DeepgramSynthesisRequest::new("one");
    request.voice = "voice & special".into();
    request.sample_rate = 16000;
    request.speed = Some(1.1);
    request.mip_opt_out = Some(true);
    assert_eq!(
        collect(
            DeepgramHttpClient::new(config)
                .unwrap()
                .synthesize(request)
                .unwrap()
        )
        .await
        .unwrap(),
        [1, 2, 3, 4]
    );
    let req = capture.await.unwrap();
    assert_eq!(req.header("authorization"), Some("Token synthetic-secret"));
    assert_eq!(req.header("content-type"), Some("application/json"));
    assert_eq!(req.url().path(), "/proxy/v1/speak");
    let query: HashMap<_, _> = req.url().query_pairs().into_owned().collect();
    assert_eq!(query["encoding"], "linear16");
    assert_eq!(query["container"], "none");
    assert_eq!(query["model"], "voice & special");
    assert_eq!(query["speed"], "1.1");
    assert_eq!(query["mip_opt_out"], "true");
    assert_eq!(req.json(), json!({"text":"one"}));
    server.await.unwrap();
}
#[tokio::test]
async fn deepgram_g711_is_decoded_to_pcm_not_relabelled() {
    for (encoding, mime, expected) in [
        (
            DeepgramEncoding::Mulaw,
            "audio/mulaw;rate=8000",
            pipecat_audio::ulaw_to_pcm(&[0x00, 0xff, 0x80]),
        ),
        (
            DeepgramEncoding::Alaw,
            "audio/alaw;rate=8000",
            pipecat_audio::alaw_to_pcm(&[0x00, 0xff, 0x80]),
        ),
    ] {
        let (config, capture, server) = server(
            "200 OK",
            mime,
            vec![vec![0x00], vec![0xff, 0x80]],
            false,
            Duration::ZERO,
        )
        .await;
        let mut request = DeepgramSynthesisRequest::new("hi");
        request.encoding = encoding;
        request.sample_rate = 8000;
        assert_eq!(
            collect(
                DeepgramHttpClient::new(config)
                    .unwrap()
                    .synthesize(request)
                    .unwrap()
            )
            .await
            .unwrap(),
            expected
        );
        assert!(capture
            .await
            .unwrap()
            .url()
            .query()
            .unwrap()
            .contains("sample_rate=8000"));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn cartesia_pins_version_and_exact_voice_generation_and_output_shape() {
    let (config, capture, server) = server(
        "200 OK",
        "application/octet-stream",
        vec![vec![0, 1, 2], vec![3]],
        false,
        Duration::ZERO,
    )
    .await;
    let mut request = CartesiaSynthesisRequest::new("hello", "voice/id");
    request.sample_rate = 48000;
    request.generation_config = Some(GenerationConfig {
        volume: Some(1.4),
        speed: Some(0.8),
        emotion: Some("content".into()),
    });
    request.pronunciation_dict_id = Some("dict".into());
    let mut stream = CartesiaHttpClient::new(config)
        .unwrap()
        .synthesize(request)
        .unwrap();
    let mut pcm = vec![];
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!(chunk.sample_rate, 48000);
        pcm.extend(chunk.pcm);
    }
    assert_eq!(pcm, [0, 1, 2, 3]);
    let req = capture.await.unwrap();
    assert_eq!(req.url().path(), "/proxy/tts/bytes");
    assert_eq!(req.header("x-api-key"), Some("synthetic-secret"));
    assert_eq!(req.header("cartesia-version"), Some("2026-03-01"));
    assert_eq!(req.header("authorization"), None);
    assert_eq!(
        req.json(),
        json!({"model_id":"sonic-3.6","transcript":"hello","voice":{"mode":"id","id":"voice/id"},"output_format":{"container":"raw","encoding":"pcm_s16le","sample_rate":48000},"language":"en","generation_config":{"volume":1.4,"speed":0.8,"emotion":"content"},"pronunciation_dict_id":"dict"})
    );
    server.await.unwrap();
}
#[tokio::test]
async fn ndjson_rejects_bad_base64_alignment_partial_samples_and_truncation() {
    for (body, kind) in [
        (r#"{"audio_base64":"%%%"}"#, "base64"),
        (r#"{"audio_base64":"AQ=="}"#, "partial"),
        (
            r#"{"audio_base64":"AAA=","alignment":{"characters":["x"],"character_start_times_seconds":[],"character_end_times_seconds":[1.0]}}"#,
            "alignment",
        ),
        (
            r#"{"audio_base64":"AAA=","alignment":{"characters":["x"],"character_start_times_seconds":[2.0],"character_end_times_seconds":[1.0]}}"#,
            "alignment",
        ),
        (r#"{"audio_base64":"AAA=""#, "json"),
        (r#"{"detail":"synthetic-secret"}"#, "missing"),
    ] {
        let (config, _, server) = server(
            "200 OK",
            "application/json",
            vec![body.as_bytes().to_vec()],
            false,
            Duration::ZERO,
        )
        .await;
        let mut stream = ElevenLabsHttpClient::new(config)
            .unwrap()
            .synthesize(ElevenLabsSynthesisRequest::new("x", "id"))
            .unwrap();
        let mut error = None;
        while let Some(event) = stream.next().await {
            if let Err(e) = event {
                error = Some(e);
            }
        }
        let error = error.unwrap();
        assert!(!format!("{error:?} {error}").contains("synthetic-secret"));
        match kind {
            "partial" => assert!(matches!(error, SpeechError::PartialSample)),
            "json" => assert!(matches!(error, SpeechError::Json(_))),
            _ => assert!(matches!(error, SpeechError::Protocol(_))),
        }
        server.await.unwrap();
    }
}
#[tokio::test]
async fn streaming_http_truncation_is_never_successful_eof() {
    for elevenlabs in [true, false] {
        let body = if elevenlabs {
            b"{\"audio_base64\":\"AAA=\"}\n".to_vec()
        } else {
            vec![0, 0]
        };
        let (config, _, server) = server(
            "200 OK",
            if elevenlabs {
                "application/json"
            } else {
                "audio/pcm"
            },
            vec![body],
            true,
            Duration::ZERO,
        )
        .await;
        let error = if elevenlabs {
            let mut stream = ElevenLabsHttpClient::new(config)
                .unwrap()
                .synthesize(ElevenLabsSynthesisRequest::new("x", "id"))
                .unwrap();
            let mut error = None;
            while let Some(event) = stream.next().await {
                if let Err(e) = event {
                    error = Some(e);
                }
            }
            error.unwrap()
        } else {
            collect(
                CartesiaHttpClient::new(config)
                    .unwrap()
                    .synthesize(CartesiaSynthesisRequest::new("x", "id"))
                    .unwrap(),
            )
            .await
            .unwrap_err()
        };
        assert!(matches!(error, SpeechError::Transport));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn typed_settings_reject_invalid_values_before_network() {
    let eleven = ElevenLabsHttpClient::new(SpeechConfig::elevenlabs("key")).unwrap();
    for rate in [0, 12000, 44101] {
        let mut request = ElevenLabsSynthesisRequest::new("x", "id");
        request.sample_rate = rate;
        assert!(matches!(
            eleven.synthesize(request),
            Err(SpeechError::Invalid(_))
        ));
    }
    for voice in ["", ".", ".."] {
        assert!(matches!(
            eleven.synthesize(ElevenLabsSynthesisRequest::new("x", voice)),
            Err(SpeechError::Invalid(_))
        ));
    }
    let mut request = ElevenLabsSynthesisRequest::new("x", "id");
    request.voice_settings = Some(ElevenLabsVoiceSettings {
        speed: Some(f64::NAN),
        ..Default::default()
    });
    assert!(matches!(
        eleven.synthesize(request),
        Err(SpeechError::Invalid(_))
    ));
    let mut request = ElevenLabsSynthesisRequest::new("x", "id");
    request.language_code = Some("unsupported".into());
    assert!(matches!(
        eleven.synthesize(request),
        Err(SpeechError::Invalid(_))
    ));
    let deepgram = DeepgramHttpClient::new(SpeechConfig::deepgram("key")).unwrap();
    let mut request = DeepgramSynthesisRequest::new("x");
    request.encoding = DeepgramEncoding::Mulaw;
    assert!(matches!(
        deepgram.synthesize(request),
        Err(SpeechError::Invalid(_))
    ));
    let mut request = DeepgramSynthesisRequest::new("x");
    request.speed = Some(2.0);
    assert!(matches!(
        deepgram.synthesize(request),
        Err(SpeechError::Invalid(_))
    ));
    let cartesia = CartesiaHttpClient::new(SpeechConfig::cartesia("key")).unwrap();
    let mut request = CartesiaSynthesisRequest::new("x", "id");
    request.generation_config = Some(GenerationConfig {
        volume: Some(f64::INFINITY),
        ..Default::default()
    });
    assert!(matches!(
        cartesia.synthesize(request),
        Err(SpeechError::Invalid(_))
    ));
    let result = eleven
        .transcribe_pcm(
            &[0, 0],
            16000,
            2,
            ElevenLabsTranscriptionRequest::default(),
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(result, Err(SpeechError::Audio(_))));
}
#[tokio::test]
async fn response_limits_cover_json_record_transcript_and_decoded_g711() {
    let (mut config, _, server) = server(
        "200 OK",
        "application/json",
        vec![b"{\"audio_base64\":\"AAAAAAAAAAAA\"}\n".to_vec()],
        false,
        Duration::ZERO,
    )
    .await;
    config.max_json_bytes = 12;
    let mut stream = ElevenLabsHttpClient::new(config)
        .unwrap()
        .synthesize(ElevenLabsSynthesisRequest::new("x", "id"))
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::TooLarge))
    ));
    server.await.unwrap();
    let (mut config, _, server) = support::server(
        "200 OK",
        "application/json",
        vec![b"{\"text\":\"hello world\"}".to_vec()],
        false,
        Duration::ZERO,
    )
    .await;
    config.max_json_bytes = 10;
    let result = ElevenLabsHttpClient::new(config)
        .unwrap()
        .transcribe_pcm(
            &[0, 0],
            16000,
            1,
            ElevenLabsTranscriptionRequest::default(),
            CancellationToken::new(),
        )
        .await;
    assert!(matches!(result, Err(SpeechError::TooLarge)));
    server.await.unwrap();
    let (mut config, _, server) = support::server(
        "200 OK",
        "audio/mulaw",
        vec![vec![255; 3]],
        false,
        Duration::ZERO,
    )
    .await;
    config.max_audio_bytes = 4;
    let mut request = DeepgramSynthesisRequest::new("x");
    request.encoding = DeepgramEncoding::Mulaw;
    request.sample_rate = 8000;
    assert!(matches!(
        collect(
            DeepgramHttpClient::new(config)
                .unwrap()
                .synthesize(request)
                .unwrap()
        )
        .await,
        Err(SpeechError::TooLarge)
    ));
    server.await.unwrap();
}
#[tokio::test]
async fn explicit_mime_rate_or_channels_mismatch_fails() {
    for mime in [
        "audio/wav",
        "application/json",
        "audio/l16;rate=48000",
        "audio/l16;channels=2",
    ] {
        let (config, _, server) =
            server("200 OK", mime, vec![vec![0, 0]], false, Duration::ZERO).await;
        assert!(matches!(
            collect(
                DeepgramHttpClient::new(config)
                    .unwrap()
                    .synthesize(DeepgramSynthesisRequest::new("x"))
                    .unwrap()
            )
            .await,
            Err(SpeechError::ContentType)
        ));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn cancelling_ndjson_discards_queued_audio_and_alignment() {
    let (config, capture, server) = server(
        "200 OK",
        "application/json",
        vec![b"{\"audio_base64\":\"AAA=\"}\n".repeat(8)],
        false,
        Duration::ZERO,
    )
    .await;
    let mut stream = ElevenLabsHttpClient::new(config)
        .unwrap()
        .synthesize(ElevenLabsSynthesisRequest::new("x", "id"))
        .unwrap();
    capture.await.unwrap();
    server.await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    stream.cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::Cancelled))
    ));
    assert!(stream.next().await.is_none());
}
#[tokio::test]
async fn failed_provider_bodies_are_never_exposed_in_errors() {
    for status in [
        "401 Unauthorized",
        "429 Too Many Requests",
        "500 Internal Server Error",
    ] {
        let (config, _, server) = server(
            status,
            "application/json",
            vec![b"{\"secret\":\"synthetic-secret\"}".to_vec()],
            false,
            Duration::ZERO,
        )
        .await;
        let error = collect(
            CartesiaHttpClient::new(config)
                .unwrap()
                .synthesize(CartesiaSynthesisRequest::new("x", "id"))
                .unwrap(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SpeechError::Http(_)));
        assert!(!format!("{error} {error:?}").contains("synthetic-secret"));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn blocked_consumer_time_counts_against_deadline() {
    let records = b"{\"audio_base64\":\"AAA=\"}\n".repeat(10);
    let (mut config, capture, server) = server(
        "200 OK",
        "application/json",
        vec![records],
        false,
        Duration::ZERO,
    )
    .await;
    config.queue_capacity = 1;
    config.timeout = Duration::from_millis(50);
    let mut stream = ElevenLabsHttpClient::new(config)
        .unwrap()
        .synthesize(ElevenLabsSynthesisRequest::new("x", "id"))
        .unwrap();
    capture.await.unwrap();
    server.await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        stream.next().await,
        Some(Ok(ElevenLabsEvent::Audio(_)))
    ));
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::Timeout))
    ));
    assert!(stream.next().await.is_none());
}
#[tokio::test]
async fn redirects_are_not_followed_and_credentials_stay_on_original_host() {
    use tokio::{io::AsyncWriteExt, net::TcpListener};
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("key");
    config.base_url = format!("http://{}", source.local_addr().unwrap());
    let location = format!("http://{}/leak", target.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = source.accept().await.unwrap();
        support::read_request(&mut socket).await;
        socket.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    });
    assert!(matches!(
        collect(
            CartesiaHttpClient::new(config)
                .unwrap()
                .synthesize(CartesiaSynthesisRequest::new("x", "id"))
                .unwrap()
        )
        .await,
        Err(SpeechError::Http(307))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), target.accept())
            .await
            .is_err()
    );
    server.await.unwrap();
}
#[test]
fn all_clients_reject_plaintext_non_loopback_and_credential_injection() {
    for key in ["", "key\r\nX-Injected: bad"] {
        assert!(ElevenLabsHttpClient::new(SpeechConfig::elevenlabs(key)).is_err());
    }
    for url in [
        "http://example.com",
        "https://user:pass@example.com",
        "https://example.com?secret=key",
        "https://example.com#fragment",
    ] {
        let mut config = SpeechConfig::cartesia("key");
        config.base_url = url.into();
        assert!(CartesiaHttpClient::new(config.clone()).is_err());
        assert!(DeepgramHttpClient::new(config.clone()).is_err());
        assert!(ElevenLabsHttpClient::new(config).is_err());
    }
}

#[tokio::test]
async fn transcription_cancellation_timeout_and_bad_json_are_typed() {
    let (config, captured, server) = server(
        "200 OK",
        "application/json",
        vec![b"{\"text\":\"hi\"}".to_vec()],
        false,
        Duration::from_millis(50),
    )
    .await;
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let task = tokio::spawn(async move {
        ElevenLabsHttpClient::new(config)
            .unwrap()
            .transcribe_pcm(
                &[0, 0],
                16000,
                1,
                ElevenLabsTranscriptionRequest::default(),
                task_cancel,
            )
            .await
    });
    captured.await.unwrap();
    cancel.cancel();
    assert!(matches!(task.await.unwrap(), Err(SpeechError::Cancelled)));
    server.await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::elevenlabs("synthetic-secret");
    config.base_url = format!("http://{}/proxy", listener.local_addr().unwrap());
    config.timeout = Duration::from_millis(10);
    let stop_peer = CancellationToken::new();
    let peer_cancel = stop_peer.clone();
    // A deadline may expire before accept or a complete request. Never respond,
    // and retain any accepted socket without requiring bytes before cleanup.
    let mut timeout_peer = tokio::task::JoinSet::new();
    timeout_peer.spawn(async move {
        let socket = tokio::select! {
            _ = peer_cancel.cancelled() => None,
            accepted = listener.accept() => Some(accepted.unwrap().0),
        };
        peer_cancel.cancelled().await;
        drop(socket);
    });
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        ElevenLabsHttpClient::new(config).unwrap().transcribe_pcm(
            &[0, 0],
            16000,
            1,
            ElevenLabsTranscriptionRequest::default(),
            CancellationToken::new(),
        ),
    )
    .await;
    // Retire the peer even when the independent client watchdog expires.
    stop_peer.cancel();
    tokio::time::timeout(Duration::from_secs(1), timeout_peer.join_next())
        .await
        .expect("timeout peer did not stop after cancellation")
        .expect("timeout peer task missing")
        .expect("timeout peer panicked");
    assert!(timeout_peer.is_empty());
    assert!(matches!(
        result.expect("transcription exceeded the independent test watchdog"),
        Err(SpeechError::Timeout)
    ));
    let (config, _, server) = support::server(
        "200 OK",
        "application/json",
        vec![br#"{"text":"hi","language_probability":"synthetic-secret"}"#.to_vec()],
        false,
        Duration::ZERO,
    )
    .await;
    let error = ElevenLabsHttpClient::new(config)
        .unwrap()
        .transcribe_pcm(
            &[0, 0],
            16000,
            1,
            ElevenLabsTranscriptionRequest::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, SpeechError::Json(_)));
    assert!(!format!("{error} {error:?}").contains("synthetic-secret"));
    server.await.unwrap();
}
#[tokio::test]
async fn elevenlabs_decoded_audio_and_wav_upload_include_all_bytes_in_limit() {
    let (mut config, _, server) = server(
        "200 OK",
        "application/json",
        vec![b"{\"audio_base64\":\"AQIDBA==\"}\n".to_vec()],
        false,
        Duration::ZERO,
    )
    .await;
    config.max_audio_bytes = 2;
    let mut stream = ElevenLabsHttpClient::new(config)
        .unwrap()
        .synthesize(ElevenLabsSynthesisRequest::new("x", "id"))
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::TooLarge))
    ));
    server.await.unwrap();
    let mut config = SpeechConfig::elevenlabs("key");
    config.max_audio_bytes = 45;
    assert!(matches!(
        ElevenLabsHttpClient::new(config)
            .unwrap()
            .transcribe_pcm(
                &[0, 0],
                16000,
                1,
                ElevenLabsTranscriptionRequest::default(),
                CancellationToken::new()
            )
            .await,
        Err(SpeechError::TooLarge)
    ));
}
