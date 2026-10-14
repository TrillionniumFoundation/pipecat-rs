#![cfg(feature = "http-smart-turn")]

use pipecat_vad::{
    EndOfTurnState, HttpSmartTurnConfig, HttpSmartTurnPredictor, SmartTurnAnalyzer,
    SmartTurnCompletionReason, SmartTurnParams, SmartTurnPredictor, VadError,
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

const DEADLINE: Duration = Duration::from_secs(4);
#[derive(Debug)]
struct Request {
    header: String,
    body: Vec<u8>,
}
enum Reply {
    Body(u16, &'static str, Vec<u8>),
    HeldHeaders,
    HeldBody,
    Raw(Vec<u8>),
}
struct Server {
    url: String,
    requests: mpsc::UnboundedReceiver<Request>,
    closed: oneshot::Receiver<()>,
    task: JoinHandle<()>,
}
impl Server {
    async fn new(reply: Reply) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/predict", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::unbounded_channel();
        let (done, closed) = oneshot::channel();
        let task = tokio::spawn(async move {
            let work = async {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut raw = Vec::new();
                let boundary = loop {
                    let mut chunk = [0; 1024];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0, "request terminated before headers");
                    raw.extend_from_slice(&chunk[..n]);
                    if let Some(index) = raw.windows(4).position(|x| x == b"\r\n\r\n") {
                        break index + 4;
                    }
                    assert!(raw.len() < 16384);
                };
                let header = String::from_utf8(raw[..boundary].to_vec()).unwrap();
                let length: usize = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|x| x.trim().parse().unwrap())
                    })
                    .unwrap();
                while raw.len() - boundary < length {
                    let mut chunk = [0; 8192];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    raw.extend_from_slice(&chunk[..n]);
                }
                tx.send(Request {
                    header,
                    body: raw[boundary..boundary + length].to_vec(),
                })
                .unwrap();
                match reply {
                    Reply::Body(status, mime, body) => {
                        let head = format!("HTTP/1.1 {status} Synthetic\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                        socket.write_all(head.as_bytes()).await.unwrap();
                        let _ = socket.write_all(&body).await;
                    }
                    Reply::Raw(bytes) => {
                        let _ = socket.write_all(&bytes).await;
                    }
                    Reply::HeldHeaders | Reply::HeldBody => {
                        if matches!(reply, Reply::HeldBody) {
                            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 500\r\n\r\n{").await.unwrap();
                        }
                        let mut byte = [0];
                        match socket.read(&mut byte).await {
                            Ok(0) | Err(_) => {}
                            other => panic!("expected peer closure: {other:?}"),
                        }
                    }
                }
                drop(socket);
                let _ = done.send(());
            };
            tokio::time::timeout(DEADLINE, work)
                .await
                .expect("server deadline");
        });
        Self {
            url,
            requests,
            closed,
            task,
        }
    }
    async fn request(&mut self) -> Request {
        tokio::time::timeout(DEADLINE, self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn join(self) {
        tokio::time::timeout(DEADLINE, self.closed)
            .await
            .unwrap()
            .unwrap();
        self.task.await.unwrap();
    }
}
fn analyzer(url: &str, stop_secs: f64) -> SmartTurnAnalyzer {
    HttpSmartTurnPredictor::new(
        HttpSmartTurnConfig::new(
            url,
            16_000,
            SmartTurnParams {
                stop_secs,
                ..Default::default()
            },
        )
        .unwrap(),
    )
    .unwrap()
    .into_analyzer()
    .unwrap()
}
fn reply(prediction: i32, probability: f64) -> Reply {
    Reply::Body(200, "application/json", format!("{{\"prediction\":{prediction},\"probability\":{probability},\"metrics\":{{\"inference_time\":999999,\"total_time\":999999}}}}").into_bytes())
}
async fn analyze(a: &mut SmartTurnAnalyzer) -> pipecat_vad::Result<pipecat_vad::SmartTurnAnalysis> {
    tokio::time::timeout(DEADLINE, a.analyze_end_of_turn())
        .await
        .unwrap()
}
async fn cleanup(a: &mut SmartTurnAnalyzer) {
    tokio::time::timeout(DEADLINE, a.cleanup())
        .await
        .unwrap()
        .unwrap();
}
fn npy_samples(body: &[u8]) -> Vec<f32> {
    assert_eq!(&body[..8], b"\x93NUMPY\x01\x00");
    let len = u16::from_le_bytes(body[8..10].try_into().unwrap()) as usize;
    let header = std::str::from_utf8(&body[10..10 + len]).unwrap();
    assert!(header.contains("'descr': '<f4'"));
    assert!(header.contains("'fortran_order': False"));
    assert!(header.ends_with('\n'));
    assert_eq!((10 + len) % 64, 0);
    let samples: Vec<_> = body[10 + len..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|x| f32::from_le_bytes(*x))
        .collect();
    assert!(header.contains(&format!("'shape': ({},)", samples.len())));
    assert_eq!(body.len(), 10 + len + 4 * samples.len());
    samples
}

#[tokio::test]
async fn actual_npy_pcm_and_server_decision_are_independent_of_probability() {
    for (decision, probability) in [(1, 0.01), (0, 0.99)] {
        let mut server = Server::new(reply(decision, probability)).await;
        let mut a = analyzer(&server.url, 1.0);
        a.append_pcm16(&[-32768, -1, 0, 1, 32767], true).unwrap();
        let result = analyze(&mut a).await.unwrap();
        cleanup(&mut a).await;
        let request = server.request().await;
        server.join().await;
        assert!(request.header.starts_with("POST /predict HTTP/1.1"));
        assert!(request
            .header
            .to_ascii_lowercase()
            .contains("content-type: application/octet-stream"));
        assert_eq!(request.body.len(), 148);
        let oracle: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/http_smart_turn_npy.json")).unwrap();
        let actual_hex: String = request
            .body
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(actual_hex, oracle["npy_hex"].as_str().unwrap());
        assert_eq!(
            npy_samples(&request.body),
            [-1.0, -1.0 / 32768.0, 0.0, 1.0 / 32768.0, 32767.0 / 32768.0]
        );
        assert_eq!(result.state == EndOfTurnState::Complete, decision == 1);
        assert_eq!(result.reason, SmartTurnCompletionReason::Model);
        let metric = result.metrics.unwrap();
        assert_eq!(metric.probability, probability as f32);
        assert_eq!(metric.is_complete, decision == 1);
        assert!(metric.e2e_processing_time_ms >= 0.0 && metric.e2e_processing_time_ms < 4000.0);
    }
}
#[tokio::test]
async fn transport_status_mime_and_json_failures_fall_back_incomplete() {
    for response in [
        Reply::Body(500,"application/json", b"sensitive upstream diagnostic".to_vec()),
        Reply::Body(201,"application/json", b"{\"prediction\":1,\"probability\":1}".to_vec()),
        Reply::Body(200,"text/plain", b"{\"prediction\":1,\"probability\":1}".to_vec()),
        Reply::Body(200,"application/json", b"not-json secret".to_vec()),
        Reply::Body(200,"application/json", vec![b' '; 65537]),
        Reply::Raw(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"prediction\":1".to_vec()),
    ] {
        let mut server = Server::new(response).await;
        let mut a = analyzer(&server.url, 0.3);
        a.append_pcm16(&[8; 160], true).unwrap();
        let result = analyze(&mut a).await.unwrap();
        cleanup(&mut a).await;
        server.request().await;
        server.join().await;
        assert_eq!(result.state, EndOfTurnState::Incomplete);
        assert_eq!(result.reason, SmartTurnCompletionReason::Model);
        let metric = result.metrics.unwrap();
        assert_eq!(metric.probability, 0.0);
        assert!(!metric.is_complete);
    }
}
#[tokio::test]
async fn header_and_body_deadlines_are_incomplete_and_close_the_connection() {
    for response in [Reply::HeldHeaders, Reply::HeldBody] {
        let mut server = Server::new(response).await;
        let mut a = analyzer(&server.url, 0.06);
        a.append_pcm16(&[1; 160], true).unwrap();
        let result = analyze(&mut a).await.unwrap();
        cleanup(&mut a).await;
        server.request().await;
        server.join().await;
        assert_eq!(result.state, EndOfTurnState::Incomplete);
        assert_eq!(result.reason, SmartTurnCompletionReason::Model);
        let metric = result.metrics.unwrap();
        assert_eq!(metric.probability, 0.0);
        assert!(metric.e2e_processing_time_ms >= 30.0);
    }
}
#[tokio::test]
async fn valid_json_schema_failures_are_explicit_and_content_free() {
    for body in [
        r#"{}"#,
        r#"{"prediction":1}"#,
        r#"{"prediction":2,"probability":0.5}"#,
        r#"{"prediction":true,"probability":0.5}"#,
        r#"{"prediction":1,"probability":1.01}"#,
        r#"{"prediction":1,"probability":-0.01}"#,
        r#"{"prediction":1,"probability":"private diagnostic"}"#,
    ] {
        let mut server = Server::new(Reply::Body(
            200,
            "application/json",
            body.as_bytes().to_vec(),
        ))
        .await;
        let mut a = analyzer(&server.url, 1.0);
        a.append_pcm16(&[1], true).unwrap();
        let result = analyze(&mut a).await;
        cleanup(&mut a).await;
        server.request().await;
        server.join().await;
        let error = result.unwrap_err().to_string();
        assert!(!error.contains("private diagnostic"));
        assert!(!error.contains("127.0.0.1"));
    }
}
#[tokio::test]
async fn cancellation_pending_drop_cleanup_and_owner_drop_close_actual_socket() {
    for mode in 0..4 {
        let mut server = Server::new(Reply::HeldBody).await;
        let mut a = analyzer(&server.url, 3.0);
        a.append_pcm16(&[1; 160], true).unwrap();
        let pending = a.begin_analysis().unwrap();
        server.request().await;
        match mode {
            0 => {
                pending.cancellation().cancel();
                assert!(matches!(
                    tokio::time::timeout(DEADLINE, pending.wait())
                        .await
                        .unwrap(),
                    Err(VadError::Cancelled)
                ));
                cleanup(&mut a).await;
            }
            1 => {
                drop(pending);
                cleanup(&mut a).await;
            }
            2 => {
                cleanup(&mut a).await;
                assert!(matches!(pending.wait().await, Err(VadError::Cancelled)));
            }
            _ => {
                drop(a);
                assert!(matches!(pending.wait().await, Err(VadError::Cancelled)));
            }
        }
        server.join().await;
    }
}
#[tokio::test]
async fn no_audio_makes_no_network_request_and_direct_tokio_predict_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let mut a = analyzer(&url, 1.0);
    let result = analyze(&mut a).await.unwrap();
    cleanup(&mut a).await;
    assert_eq!(result.reason, SmartTurnCompletionReason::Empty);
    assert!(result.metrics.is_none());
    let pending = a.begin_analysis().unwrap();
    let cancel = pending.cancellation();
    let mut predictor = HttpSmartTurnPredictor::new(
        HttpSmartTurnConfig::new(&url, 16000, SmartTurnParams::default()).unwrap(),
    )
    .unwrap();
    assert!(predictor.predict(&[0], 16000, &cancel).is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn redirect_is_not_followed_or_forwarded_with_custom_header() {
    let trap = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let location = format!("http://{}/trap", trap.local_addr().unwrap());
    let mut server = Server::new(Reply::Raw(format!("HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").into_bytes())).await;
    let config = HttpSmartTurnConfig::new(&server.url, 16000, SmartTurnParams::default())
        .unwrap()
        .with_header("X-Test-Key", "synthetic-only")
        .unwrap();
    let mut a = HttpSmartTurnPredictor::new(config)
        .unwrap()
        .into_analyzer()
        .unwrap();
    a.append_pcm16(&[1], true).unwrap();
    let result = analyze(&mut a).await.unwrap();
    cleanup(&mut a).await;
    let request = server.request().await;
    server.join().await;
    assert!(request
        .header
        .to_ascii_lowercase()
        .contains("x-test-key: synthetic-only"));
    assert_eq!(result.state, EndOfTurnState::Incomplete);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), trap.accept())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn configured_rate_tail_window_and_json_suffix_are_preserved() {
    let mut server = Server::new(Reply::Body(
        200,
        "application/vnd.test+json",
        br#"{"prediction":1,"probability":0}"#.to_vec(),
    ))
    .await;
    let config = HttpSmartTurnConfig::new(
        &server.url,
        8000,
        SmartTurnParams {
            stop_secs: 1.0,
            max_duration_secs: 0.001,
            pre_speech_ms: 0.0,
        },
    )
    .unwrap();
    let mut a = HttpSmartTurnPredictor::new(config)
        .unwrap()
        .into_analyzer()
        .unwrap();
    a.append_pcm16(&(0..12).collect::<Vec<_>>(), true).unwrap();
    let result = analyze(&mut a).await.unwrap();
    cleanup(&mut a).await;
    let request = server.request().await;
    server.join().await;
    assert_eq!(
        npy_samples(&request.body),
        (4..12).map(|x| x as f32 / 32768.0).collect::<Vec<_>>()
    );
    assert_eq!(result.state, EndOfTurnState::Complete);
    assert_eq!(result.metrics.unwrap().probability, 0.0);
}
#[tokio::test]
async fn refusal_falls_back_and_analyzer_can_restart_after_joined_cleanup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let mut a = analyzer(&format!("http://{addr}/predict"), 0.3);
    a.append_pcm16(&[2], true).unwrap();
    for _ in 0..2 {
        let result = analyze(&mut a).await.unwrap();
        cleanup(&mut a).await;
        assert_eq!(result.state, EndOfTurnState::Incomplete);
        assert_eq!(result.metrics.unwrap().probability, 0.0);
    }
}

#[tokio::test]
async fn chunked_body_is_bounded_without_content_length() {
    for (body, complete) in [
        (br#"{"prediction":1,"probability":0.2}"#.to_vec(), true),
        (vec![b' '; 65537], false),
    ] {
        let mut wire = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
        for chunk in body.chunks(4096) {
            wire.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            wire.extend_from_slice(chunk);
            wire.extend_from_slice(b"\r\n");
        }
        wire.extend_from_slice(b"0\r\n\r\n");
        let mut server = Server::new(Reply::Raw(wire)).await;
        let mut a = analyzer(&server.url, 1.0);
        a.append_pcm16(&[3], true).unwrap();
        let result = analyze(&mut a).await.unwrap();
        cleanup(&mut a).await;
        server.request().await;
        server.join().await;
        assert_eq!(result.state == EndOfTurnState::Complete, complete);
        assert_eq!(
            result.metrics.unwrap().probability,
            if complete { 0.2 } else { 0.0 }
        );
    }
}
