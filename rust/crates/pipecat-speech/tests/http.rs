//! HTTP tests use synthetic responses on real loopback sockets, not paid providers.
use pipecat_services::CancellationToken;
use pipecat_speech::*;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
async fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut buffer = vec![];
    let mut part = [0; 2048];
    let end = loop {
        let n = stream.read(&mut part).await.unwrap();
        assert!(n > 0);
        buffer.extend_from_slice(&part[..n]);
        if let Some(i) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = String::from_utf8_lossy(&buffer[..end]);
    let size = headers
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while buffer.len() < end + size {
        let n = stream.read(&mut part).await.unwrap();
        assert!(n > 0);
        buffer.extend_from_slice(&part[..n]);
    }
    buffer
}
async fn fake_server(
    parts: Vec<Vec<u8>>,
    status: &str,
    delay: Duration,
) -> (
    SpeechConfig,
    oneshot::Receiver<Vec<u8>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = SpeechConfig::openai("synthetic-key");
    c.base_url = format!("http://{}", listener.local_addr().unwrap());
    c.timeout = Duration::from_secs(2);
    let (tx, rx) = oneshot::channel();
    let status = status.to_owned();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_request(&mut stream).await;
        let _ = tx.send(request);
        tokio::time::sleep(delay).await;
        if stream
            .write_all(
                format!(
                    "HTTP/1.1 {status}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await
            .is_err()
        {
            return;
        }
        for chunk in parts {
            if stream
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await
                .is_err()
            {
                return;
            }
            if stream.write_all(&chunk).await.is_err() {
                return;
            }
            if stream.write_all(b"\r\n").await.is_err() {
                return;
            }
            tokio::task::yield_now().await;
        }
        let _ = stream.write_all(b"0\r\n\r\n").await;
    });
    (c, rx, task)
}
#[tokio::test]
async fn synthesis_reassembles_partial_pcm_samples() {
    let (c, request, server) =
        fake_server(vec![vec![1], vec![2, 3], vec![4]], "200 OK", Duration::ZERO).await;
    let client = SpeechClient::new(c).unwrap();
    let mut stream = client.synthesize(SynthesisRequest::new("hello")).unwrap();
    let mut bytes = vec![];
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!(chunk.pcm.len() % 2, 0);
        assert_eq!(chunk.sample_rate, 24000);
        bytes.extend(chunk.pcm);
    }
    assert_eq!(bytes, vec![1, 2, 3, 4]);
    let req = String::from_utf8(request.await.unwrap()).unwrap();
    assert!(req.starts_with("POST /audio/speech"));
    assert!(req.contains("\"response_format\":\"pcm\""));
    server.await.unwrap();
}
#[tokio::test]
async fn incomplete_pcm_is_not_silently_discarded() {
    let (c, _, server) = fake_server(vec![vec![1]], "200 OK", Duration::ZERO).await;
    let mut stream = SpeechClient::new(c)
        .unwrap()
        .synthesize(SynthesisRequest::new("x"))
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::PartialSample))
    ));
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}
#[tokio::test]
async fn transcription_uploads_wav_multipart() {
    let (c, request, server) = fake_server(
        vec![br#"{"text":"synthetic transcript"}"#.to_vec()],
        "200 OK",
        Duration::ZERO,
    )
    .await;
    let result = SpeechClient::new(c)
        .unwrap()
        .transcribe_pcm(
            &[0, 0, 1, 0],
            16000,
            1,
            TranscriptionRequest::default(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.text, "synthetic transcript");
    let req = request.await.unwrap();
    assert!(req.windows(4).any(|w| w == b"RIFF"));
    assert!(req.windows(9).any(|w| w == b"audio.wav"));
    assert!(req.starts_with(b"POST /audio/transcriptions"));
    server.await.unwrap();
}
#[tokio::test]
async fn timeout_and_http_failure_are_typed() {
    let (mut c, _, server) = fake_server(vec![], "200 OK", Duration::from_millis(100)).await;
    c.timeout = Duration::from_millis(20);
    let mut stream = SpeechClient::new(c)
        .unwrap()
        .synthesize(SynthesisRequest::new("x"))
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::Timeout))
    ));
    server.await.unwrap();
    let (c, _, server) = fake_server(vec![], "429 Too Many Requests", Duration::ZERO).await;
    let mut stream = SpeechClient::new(c)
        .unwrap()
        .synthesize(SynthesisRequest::new("x"))
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::Http(429)))
    ));
    server.await.unwrap();
}
#[tokio::test]
async fn cancellation_is_not_successful_eof() {
    let (c, request, server) = fake_server(vec![], "200 OK", Duration::from_millis(100)).await;
    let mut stream = SpeechClient::new(c)
        .unwrap()
        .synthesize(SynthesisRequest::new("x"))
        .unwrap();
    request.await.unwrap();
    stream.cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::Cancelled))
    ));
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}
#[tokio::test]
async fn output_limit_and_input_alignment() {
    let (mut c, _, server) = fake_server(vec![vec![0; 6]], "200 OK", Duration::ZERO).await;
    c.max_audio_bytes = 4;
    let client = SpeechClient::new(c).unwrap();
    let mut stream = client.synthesize(SynthesisRequest::new("x")).unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::TooLarge))
    ));
    server.await.unwrap();
    assert!(matches!(
        client
            .transcribe_pcm(
                &[1],
                16000,
                1,
                TranscriptionRequest::default(),
                CancellationToken::new()
            )
            .await,
        Err(SpeechError::Audio(_))
    ));
}
#[test]
fn credential_debug_redacts_and_speed_validates() {
    let c = SpeechConfig::openai("super-secret");
    assert!(!format!("{c:?}").contains("super-secret"));
    let client = SpeechClient::new(c).unwrap();
    let mut req = SynthesisRequest::new("x");
    req.speed = f64::NAN;
    assert!(matches!(
        client.synthesize(req),
        Err(SpeechError::Invalid(_))
    ));
}

#[tokio::test]
async fn cancelling_discards_already_buffered_audio() {
    let (c, _, server) = fake_server(vec![vec![1, 2]; 8], "200 OK", Duration::ZERO).await;
    let client = SpeechClient::new(c).unwrap();
    let mut stream = client.synthesize(SynthesisRequest::new("hello")).unwrap();
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
async fn empty_success_is_not_synthesized_audio() {
    let (c, _, server) = fake_server(vec![], "200 OK", Duration::ZERO).await;
    let mut stream = SpeechClient::new(c)
        .unwrap()
        .synthesize(SynthesisRequest::new("hello"))
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::EmptyAudio))
    ));
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn json_success_is_not_pcm() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = SpeechConfig::openai("synthetic");
    c.base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
    });
    let mut stream = SpeechClient::new(c)
        .unwrap()
        .synthesize(SynthesisRequest::new("hello"))
        .unwrap();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::ContentType))
    ));
    server.await.unwrap();
}

#[test]
fn plaintext_credentials_cannot_leave_loopback() {
    let mut config = SpeechConfig::openai("synthetic");
    config.base_url = "http://example.com/v1".into();
    assert!(matches!(
        SpeechClient::new(config),
        Err(SpeechError::Invalid(_))
    ));
}
