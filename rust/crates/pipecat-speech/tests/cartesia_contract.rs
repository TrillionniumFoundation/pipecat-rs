//! Cartesia transport fixtures use literal-loopback endpoints and synthetic keys.
mod support;
use pipecat_speech::{cartesia::*, deepgram::*, *};
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpListener};

async fn collect(mut stream: SpeechStream) -> Result<Vec<u8>, SpeechError> {
    let mut pcm = Vec::new();
    while let Some(chunk) = stream.next().await {
        pcm.extend(chunk?.pcm);
    }
    stream.cancel_and_join().await?;
    Ok(pcm)
}

#[tokio::test]
async fn cartesia_rejects_non_200_before_any_pcm_with_both_constructors() {
    for direct in [false, true] {
        for (code, status) in [(201, "201 Created"), (206, "206 Partial Content")] {
            let (config, capture, server) = support::server(
                status,
                "audio/pcm",
                vec![vec![1, 2, 3, 4]],
                false,
                Duration::ZERO,
            )
            .await;
            let client = if direct {
                CartesiaHttpClient::new_direct(config)
            } else {
                CartesiaHttpClient::new(config)
            }
            .unwrap();
            let mut stream = client
                .synthesize(CartesiaSynthesisRequest::new("synthetic text", "voice"))
                .unwrap();
            assert!(matches!(
                stream.next().await,
                Some(Err(SpeechError::Http(status))) if status == code
            ));
            assert!(stream.next().await.is_none());
            stream.cancel_and_join().await.unwrap();
            assert_eq!(capture.await.unwrap().url().path(), "/proxy/tts/bytes");
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn direct_cartesia_preserves_wire_defaults_and_http_200_pcm() {
    let (config, capture, server) = support::server(
        "200 OK",
        "audio/pcm;rate=24000;channels=1",
        vec![vec![1], vec![2, 3], vec![4]],
        false,
        Duration::ZERO,
    )
    .await;
    let mut stream = CartesiaHttpClient::new_direct(config)
        .unwrap()
        .synthesize(CartesiaSynthesisRequest::new("synthetic text", "voice"))
        .unwrap();
    let mut pcm = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!(chunk.sample_rate, 24000);
        assert_eq!(chunk.num_channels, 1);
        assert_eq!(chunk.pcm.len() % 2, 0);
        pcm.extend(chunk.pcm);
    }
    assert_eq!(pcm, [1, 2, 3, 4]);
    stream.cancel_and_join().await.unwrap();
    let request = capture.await.unwrap();
    assert!(request
        .head
        .starts_with("POST /proxy/tts/bytes HTTP/1.1\r\n"));
    assert_eq!(request.header("x-api-key"), Some("synthetic-secret"));
    assert_eq!(
        request.header("cartesia-version"),
        Some(CARTESIA_API_VERSION)
    );
    let body = request.json();
    assert_eq!(body["model_id"], "sonic-3.6");
    assert_eq!(body["language"], "en");
    assert_eq!(body["transcript"], "synthetic text");
    assert_eq!(body["output_format"]["sample_rate"], 24000);
    server.await.unwrap();
}

#[tokio::test]
async fn other_raw_providers_keep_non_200_success_policy() {
    for cartesia_alternative in ["openai", "deepgram"] {
        for status in ["201 Created", "206 Partial Content"] {
            let (config, _, server) = support::server(
                status,
                "audio/pcm",
                vec![vec![1, 2, 3, 4]],
                false,
                Duration::ZERO,
            )
            .await;
            let stream = match cartesia_alternative {
                "openai" => SpeechClient::new(config)
                    .unwrap()
                    .synthesize(SynthesisRequest::new("synthetic text"))
                    .unwrap(),
                _ => DeepgramHttpClient::new(config)
                    .unwrap()
                    .synthesize(DeepgramSynthesisRequest::new("synthetic text"))
                    .unwrap(),
            };
            assert_eq!(collect(stream).await.unwrap(), [1, 2, 3, 4]);
            server.await.unwrap();
        }
    }
}

#[tokio::test]
async fn direct_cartesia_proxy_environment_child() {
    let Ok(base) = std::env::var("PIPECAT_CARTESIA_DIRECT_TEST_BASE") else {
        return;
    };
    let mut config = SpeechConfig::cartesia("synthetic-secret");
    config.base_url = base;
    config.timeout = Duration::from_secs(2);
    assert_eq!(
        collect(
            CartesiaHttpClient::new_direct(config)
                .unwrap()
                .synthesize(CartesiaSynthesisRequest::new("synthetic text", "voice"))
                .unwrap()
        )
        .await
        .unwrap(),
        [1, 2]
    );
}

#[tokio::test]
async fn direct_cartesia_ignores_proxy_environment_in_isolated_process() {
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
    let (config, capture, server) = support::server(
        "200 OK",
        "audio/pcm",
        vec![vec![1, 2]],
        false,
        Duration::ZERO,
    )
    .await;
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "direct_cartesia_proxy_environment_child",
            "--nocapture",
        ])
        .env("PIPECAT_CARTESIA_DIRECT_TEST_BASE", config.base_url)
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
    capture.await.unwrap();
    server.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), proxy.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn direct_cartesia_does_not_follow_redirects() {
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("synthetic-secret");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let location = format!("http://{}/leak", destination.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        socket.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    });
    assert!(matches!(
        collect(
            CartesiaHttpClient::new_direct(config)
                .unwrap()
                .synthesize(CartesiaSynthesisRequest::new("synthetic text", "voice"))
                .unwrap()
        )
        .await,
        Err(SpeechError::Http(307))
    ));
    server.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), destination.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn direct_cartesia_does_not_retry_transport_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("synthetic-secret");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.timeout = Duration::from_secs(2);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        drop(socket);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    assert!(matches!(
        collect(
            CartesiaHttpClient::new_direct(config)
                .unwrap()
                .synthesize(CartesiaSynthesisRequest::new("synthetic text", "voice"))
                .unwrap()
        )
        .await,
        Err(SpeechError::Transport)
    ));
    server.await.unwrap();
}
