#[allow(dead_code)]
mod support;
use pipecat_aws_speech::*;
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpListener};
fn config() -> PollyConfig {
    let mut c = PollyConfig::new(
        AwsCredentials::new(
            "AKIDEXAMPLE",
            "SYNTHETICSECRET",
            Some("SESSIONTOKEN".into()),
        )
        .unwrap(),
        "us-east-1",
    );
    c.timeout = Duration::from_secs(2);
    c.sample_rate = 16000;
    c
}
async fn server(
    status: &str,
    mime: &str,
    chunks: Vec<Vec<u8>>,
) -> (PollyConfig, tokio::task::JoinHandle<support::Request>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = config();
    c.endpoint = format!("http://{}/v1/speech", listener.local_addr().unwrap());
    let head=format!("HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = support::read_request(&mut socket).await;
        socket.write_all(head.as_bytes()).await.unwrap();
        for chunk in chunks {
            let _ = socket
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await;
            let _ = socket.write_all(&chunk).await;
            let _ = socket.write_all(b"\r\n").await;
        }
        let _ = socket.write_all(b"0\r\n\r\n").await;
        request
    });
    (c, task)
}
#[tokio::test]
async fn polly_signs_real_route_and_reassembles_odd_pcm_http_chunks() {
    let (c, task) = server("200 OK", "audio/pcm", vec![vec![1], vec![2, 3], vec![4]]).await;
    let mut stream = PollyClient::new(c)
        .unwrap()
        .synthesize("Hello & <world>")
        .unwrap();
    let mut pcm = vec![];
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!(chunk.sample_rate, 16000);
        assert_eq!(chunk.num_channels, 1);
        assert_eq!(chunk.pcm.len() % 2, 0);
        pcm.extend(chunk.pcm);
    }
    assert_eq!(pcm, [1, 2, 3, 4]);
    let request = task.await.unwrap();
    assert!(request.head.starts_with("POST /v1/speech HTTP/1.1"));
    assert_eq!(request.header("content-type"), Some("application/json"));
    assert!(request
        .header("authorization")
        .unwrap()
        .contains("/us-east-1/polly/aws4_request"));
    assert_eq!(request.header("x-amz-security-token"), Some("SESSIONTOKEN"));
    assert_eq!(request.json()["SampleRate"], "16000");
    assert!(request.json()["Text"]
        .as_str()
        .unwrap()
        .contains("&amp; &lt;world&gt;"));
}
#[tokio::test]
async fn polly_resamples_pcm_and_checks_output_byte_budget() {
    let input = vec![0u8; 640];
    let (mut c, task) = server("200 OK", "audio/pcm", vec![input]).await;
    c.sample_rate = 24000;
    let mut stream = PollyClient::new(c).unwrap().synthesize("hi").unwrap();
    let mut count = 0;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!(chunk.sample_rate, 24000);
        count += chunk.pcm.len();
    }
    assert_eq!(count, 960);
    task.await.unwrap();
    let (mut c, task) = server("200 OK", "audio/pcm", vec![vec![0; 640]]).await;
    c.sample_rate = 48000;
    c.max_audio_bytes = 1000;
    let mut stream = PollyClient::new(c).unwrap().synthesize("hi").unwrap();
    let mut error = false;
    while let Some(chunk) = stream.next().await {
        if matches!(chunk, Err(SpeechError::TooLarge)) {
            error = true;
            break;
        }
        chunk.unwrap();
    }
    assert!(error);
    task.await.unwrap();
}
#[tokio::test]
async fn polly_rejects_truncated_pcm_wrong_type_and_unbounded_bodies() {
    for (mime, chunks, kind) in [
        ("audio/pcm", vec![vec![1]], "partial"),
        ("audio/mpeg", vec![vec![0, 0]], "type"),
        ("audio/pcm", vec![], "empty"),
    ] {
        let (c, task) = server("200 OK", mime, chunks).await;
        let mut stream = PollyClient::new(c).unwrap().synthesize("hi").unwrap();
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(match kind {
            "partial" => matches!(error, SpeechError::PartialSample),
            "type" => matches!(error, SpeechError::ContentType),
            _ => matches!(error, SpeechError::EmptyAudio),
        });
        task.await.unwrap();
    }
}
#[tokio::test]
async fn polly_http_errors_redact_provider_body_and_redirects_are_not_followed() {
    for status in ["403 Forbidden", "307 Temporary Redirect"] {
        let (c, task) = server(
            status,
            "application/json",
            vec![b"{\"Message\":\"SESSIONTOKEN SYNTHETICSECRET\"}".to_vec()],
        )
        .await;
        let mut stream = PollyClient::new(c).unwrap().synthesize("hi").unwrap();
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(matches!(error, SpeechError::Http(403 | 307)));
        assert!(!format!("{error:?}").contains("SECRET"));
        assert!(!error.to_string().contains("SESSION"));
        task.await.unwrap();
    }
}
#[tokio::test]
async fn polly_cancel_discards_queued_audio_and_drop_closes_request() {
    let (c, task) = server("200 OK", "audio/pcm", vec![vec![0; 32000]]).await;
    let mut stream = PollyClient::new(c).unwrap().synthesize("hi").unwrap();
    stream.next().await.unwrap().unwrap();
    stream.cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::Cancelled))
    ));
    assert!(stream.next().await.is_none());
    task.await.unwrap();
}
