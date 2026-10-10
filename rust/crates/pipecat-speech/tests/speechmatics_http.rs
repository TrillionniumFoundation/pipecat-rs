#![cfg(feature = "speechmatics")]
//! Native HTTP fixtures, using synthetic credentials and actual loopback sockets.
#[allow(dead_code)]
mod support;
use pipecat_speech::{speechmatics::*, *};
use std::{collections::HashMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn collect(mut stream: SpeechStream) -> Result<Vec<u8>, SpeechError> {
    let mut pcm = vec![];
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        assert_eq!(chunk.num_channels, 1);
        assert_eq!(chunk.pcm.len() % 2, 0);
        pcm.extend(chunk.pcm);
    }
    Ok(pcm)
}
#[tokio::test]
async fn exact_encoded_wire_and_fragmented_pcm_at_nondefault_rate() {
    let (config, capture, server) = support::server(
        "200 OK",
        "audio/pcm;rate=22050;channels=1;bits=16",
        vec![vec![1], vec![2, 3], vec![4]],
        false,
        Duration::ZERO,
    )
    .await;
    let mut request = SpeechmaticsSynthesisRequest::new("你好\nhello");
    request.voice = "voice /?#%中文".into();
    request.sample_rate = 22050;
    let mut stream = SpeechmaticsHttpClient::new(config)
        .unwrap()
        .synthesize(request)
        .unwrap();
    let mut pcm = vec![];
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!((chunk.sample_rate, chunk.num_channels), (22050, 1));
        assert_eq!(chunk.pcm.len() % 2, 0);
        pcm.extend(chunk.pcm);
    }
    assert_eq!(pcm, [1, 2, 3, 4]);
    let req = capture.await.unwrap();
    assert!(req.head.starts_with("POST "));
    assert_eq!(req.header("authorization"), Some("Bearer synthetic-secret"));
    assert_eq!(req.header("content-type"), Some("application/json"));
    assert_eq!(
        req.url().path(),
        "/proxy/generate/voice%20%2F%3F%23%25%E4%B8%AD%E6%96%87"
    );
    let query: HashMap<_, _> = req.url().query_pairs().into_owned().collect();
    assert_eq!(query.len(), 2);
    assert_eq!(query["output_format"], "pcm_22050");
    assert_eq!(query["sm-app"], SPEECHMATICS_APP);
    assert_eq!(req.json(), serde_json::json!({"text":"你好\nhello"}));
    server.await.unwrap();
}
async fn statuses(
    values: Vec<u16>,
) -> (SpeechConfig, tokio::task::JoinHandle<Vec<support::Request>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("synthetic-secret");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.timeout = Duration::from_secs(10);
    let task = tokio::spawn(async move {
        let mut requests = vec![];
        for status in values {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
                .await
                .unwrap()
                .unwrap();
            requests.push(support::read_request(&mut socket).await);
            let body = if status == 200 {
                vec![1, 2]
            } else {
                b"synthetic-secret".to_vec()
            };
            socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(350), listener.accept())
                .await
                .is_err(),
            "unexpected replay"
        );
        requests
    });
    (config, task)
}
#[tokio::test]
async fn source_retry_edges_and_exact_replayed_request() {
    for maximum in [0, 1, 2, 5] {
        let count = usize::from(maximum.max(1));
        let (config, server) = statuses(vec![503; count]).await;
        let mut request = SpeechmaticsSynthesisRequest::new("same text");
        request.max_retries = maximum;
        let error = collect(
            SpeechmaticsHttpClient::new(config)
                .unwrap()
                .synthesize(request)
                .unwrap(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SpeechError::Http(503)));
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), count);
        assert!(requests
            .windows(2)
            .all(|pair| pair[0].body == pair[1].body && pair[0].url() == pair[1].url()));
    }
    let (config, server) = statuses(vec![503, 200]).await;
    assert_eq!(
        collect(
            SpeechmaticsHttpClient::new(config)
                .unwrap()
                .synthesize(SpeechmaticsSynthesisRequest::new("recovered"))
                .unwrap()
        )
        .await
        .unwrap(),
        [1, 2]
    );
    assert_eq!(server.await.unwrap().len(), 2);
}
#[tokio::test]
async fn only_200_succeeds_and_only_503_retries_with_redacted_errors() {
    for status in [201, 204, 301, 307, 400, 401, 429, 500, 502, 504] {
        let (config, server) = statuses(vec![status]).await;
        let error = collect(
            SpeechmaticsHttpClient::new(config)
                .unwrap()
                .synthesize(SpeechmaticsSynthesisRequest::new("sensitive-text"))
                .unwrap(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error,SpeechError::Http(value) if value==status));
        assert!(!format!("{error:?} {error}").contains("synthetic-secret"));
        assert_eq!(server.await.unwrap().len(), 1);
    }
}
#[tokio::test]
async fn odd_empty_truncated_and_oversized_audio_never_complete_successfully() {
    for (chunks, truncated, limit, expected) in [
        (vec![vec![1]], false, 1024, "odd"),
        (vec![], false, 1024, "empty"),
        (vec![vec![1, 2]], true, 1024, "truncated"),
        (vec![vec![1, 2, 3, 4]], false, 2, "large"),
    ] {
        let (mut config, _, server) = support::server(
            "200 OK",
            "application/octet-stream",
            chunks,
            truncated,
            Duration::ZERO,
        )
        .await;
        config.max_audio_bytes = limit;
        let error = collect(
            SpeechmaticsHttpClient::new(config)
                .unwrap()
                .synthesize(SpeechmaticsSynthesisRequest::new("x"))
                .unwrap(),
        )
        .await
        .unwrap_err();
        match expected {
            "odd" => assert!(matches!(error, SpeechError::PartialSample)),
            "empty" => assert!(matches!(error, SpeechError::Protocol("empty PCM response"))),
            "truncated" => assert!(matches!(error, SpeechError::Transport)),
            _ => assert!(matches!(error, SpeechError::TooLarge)),
        }
        server.await.unwrap();
    }
}
#[tokio::test]
async fn rejects_explicit_incompatible_pcm_metadata() {
    for mime in [
        "audio/wav",
        "audio/mpeg",
        "audio/l16",
        "application/json",
        "audio/pcm;rate=24000",
        "audio/pcm;channels=2",
        "audio/pcm;bits=32",
        "audio/pcm;encoding=pcm_f32le",
        "audio/pcm;endianness=big",
    ] {
        let (config, _, server) =
            support::server("200 OK", mime, vec![vec![0, 0]], false, Duration::ZERO).await;
        assert!(matches!(
            collect(
                SpeechmaticsHttpClient::new(config)
                    .unwrap()
                    .synthesize(SpeechmaticsSynthesisRequest::new("x"))
                    .unwrap()
            )
            .await,
            Err(SpeechError::ContentType)
        ));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn overall_deadline_includes_retry_wait_and_socket_wait() {
    for initial_status in [Some(503), None] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::speechmatics("key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        config.timeout = Duration::from_millis(80);
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            if let Some(status) = initial_status {
                socket.write_all(format!("HTTP/1.1 {status} Busy\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            }
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(300), listener.accept())
                    .await
                    .is_err()
            );
        });
        assert!(matches!(
            collect(
                SpeechmaticsHttpClient::new(config)
                    .unwrap()
                    .synthesize(SpeechmaticsSynthesisRequest::new("x"))
                    .unwrap()
            )
            .await,
            Err(SpeechError::Timeout)
        ));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn cancellation_and_drop_close_socket_and_fence_buffered_audio() {
    for drop_stream in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::speechmatics("key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        config.queue_capacity = 1;
        let (ready_tx, ready) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: audio/pcm\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n\x01\x02\r\n2\r\n\x03\x04\r\n").await.unwrap();
            ready_tx.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        let mut stream = SpeechmaticsHttpClient::new(config)
            .unwrap()
            .synthesize(SpeechmaticsSynthesisRequest::new("x"))
            .unwrap();
        ready.await.unwrap();
        if drop_stream {
            drop(stream);
        } else {
            stream.cancel();
            assert!(matches!(
                stream.next().await,
                Some(Err(SpeechError::Cancelled))
            ));
            assert!(stream.next().await.is_none());
        }
        server.await.unwrap();
    }
}
#[tokio::test]
async fn request_bounds_and_invalid_settings_reject_before_network() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.max_json_bytes = 16;
    let client = SpeechmaticsHttpClient::new(config).unwrap();
    for text in ["x".repeat(17), "\n".repeat(5)] {
        assert!(matches!(
            client.synthesize(SpeechmaticsSynthesisRequest::new(text)),
            Err(SpeechError::TooLarge)
        ));
    }
    for request in [
        SpeechmaticsSynthesisRequest {
            voice: "..".into(),
            ..SpeechmaticsSynthesisRequest::new("x")
        },
        SpeechmaticsSynthesisRequest {
            sample_rate: 0,
            ..SpeechmaticsSynthesisRequest::new("x")
        },
        SpeechmaticsSynthesisRequest {
            max_retries: 33,
            ..SpeechmaticsSynthesisRequest::new("x")
        },
    ] {
        assert!(matches!(
            client.synthesize(request),
            Err(SpeechError::Invalid(_))
        ));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
    assert!(
        !format!("{:?}", SpeechmaticsSynthesisRequest::new("sensitive-text"))
            .contains("sensitive-text")
    );
}
#[test]
fn config_is_direct_secure_and_defaults_are_source_shaped() {
    let request = SpeechmaticsSynthesisRequest::new("x");
    assert_eq!(
        (
            request.voice.as_str(),
            request.sample_rate,
            request.max_retries
        ),
        ("sarah", 16000, 5)
    );
    for url in [
        "http://example.com",
        "https://user:pass@example.com",
        "https://example.com?key=secret",
        "https://example.com#secret",
    ] {
        let mut config = SpeechConfig::speechmatics("key");
        config.base_url = url.into();
        assert!(SpeechmaticsHttpClient::new(config).is_err());
    }
    for key in ["", "a\r\nb"] {
        assert!(SpeechmaticsHttpClient::new(SpeechConfig::speechmatics(key)).is_err());
    }
}
#[tokio::test]
async fn header_budget_encoding_and_redirect_do_not_expose_or_forward_secrets() {
    for extra in [
        format!("X-Long: {}\r\n", "x".repeat(17000)),
        "Content-Encoding: gzip\r\n".into(),
        "Content-Type: audio/pcm\r\nContent-Type: audio/pcm\r\n".into(),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::speechmatics("synthetic-secret");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let large = extra.len() > 16000;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            socket.write_all(format!("HTTP/1.1 200 OK\r\n{extra}Content-Length: 2\r\nConnection: close\r\n\r\n\x01\x02").as_bytes()).await.unwrap();
        });
        let error = collect(
            SpeechmaticsHttpClient::new(config)
                .unwrap()
                .synthesize(SpeechmaticsSynthesisRequest::new("x"))
                .unwrap(),
        )
        .await
        .unwrap_err();
        if large {
            assert!(matches!(error, SpeechError::TooLarge));
        } else {
            assert!(matches!(error, SpeechError::ContentType));
        }
        server.await.unwrap();
    }
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::speechmatics("synthetic-secret");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let location = format!("http://{}/leak", target.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        socket.write_all(format!("HTTP/1.1 307 Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    });
    assert!(matches!(
        collect(
            SpeechmaticsHttpClient::new(config)
                .unwrap()
                .synthesize(SpeechmaticsSynthesisRequest::new("x"))
                .unwrap()
        )
        .await,
        Err(SpeechError::Http(307))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), target.accept())
            .await
            .is_err()
    );
    server.await.unwrap();
}
#[tokio::test]
async fn proxy_environment_child() {
    let Ok(base) = std::env::var("PIPECAT_SPEECHMATICS_PROXY_TEST_BASE") else {
        return;
    };
    let mut config = SpeechConfig::speechmatics("synthetic-secret");
    config.base_url = base;
    config.timeout = Duration::from_secs(2);
    assert_eq!(
        collect(
            SpeechmaticsHttpClient::new(config)
                .unwrap()
                .synthesize(SpeechmaticsSynthesisRequest::new("direct"))
                .unwrap()
        )
        .await
        .unwrap(),
        [1, 2]
    );
}
#[tokio::test]
async fn environment_proxy_is_ignored_in_isolated_process() {
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    let (config, server) = statuses(vec![200]).await;
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "proxy_environment_child", "--nocapture"])
        .env("PIPECAT_SPEECHMATICS_PROXY_TEST_BASE", config.base_url)
        .env("HTTP_PROXY", &proxy_url)
        .env("http_proxy", &proxy_url)
        .env("ALL_PROXY", &proxy_url)
        .env("all_proxy", &proxy_url)
        .env("HTTPS_PROXY", &proxy_url)
        .env("https_proxy", &proxy_url)
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let output = tokio::task::spawn_blocking(move || child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(server.await.unwrap().len(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), proxy.accept())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn transport_failure_never_retries_before_or_after_emitted_pcm() {
    for audio_first in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::speechmatics("key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            if audio_first {
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: audio/pcm\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n\x01\x02\r\n").await.unwrap();
            }
            drop(socket);
            assert!(
                tokio::time::timeout(Duration::from_millis(350), listener.accept())
                    .await
                    .is_err(),
                "ambiguous or partial request replayed"
            );
        });
        let mut stream = SpeechmaticsHttpClient::new(config)
            .unwrap()
            .synthesize(SpeechmaticsSynthesisRequest::new("x"))
            .unwrap();
        if audio_first {
            assert_eq!(stream.next().await.unwrap().unwrap().pcm, [1, 2]);
        }
        assert!(matches!(
            stream.next().await,
            Some(Err(SpeechError::Transport))
        ));
        assert!(stream.next().await.is_none());
        server.await.unwrap();
    }
}
