//! Actual runner/transport cleanup outcomes; all fixture owners close before assertions.
use pipecat_browser_client::{client_ready, protocol, websocket_endpoint};
use pipecat_client_server::server::{serve_with_api, ServerConfig};
use pipecat_runner::{NativeRunner, Options, ProviderConfig, RunnerConfig, SessionSummary};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, oneshot},
    task::JoinHandle,
};
type Outcome<T = ()> = Result<T, String>;
macro_rules! get {
    ($v:expr) => {
        $v.map_err(|error| format!("{error:?}"))?
    };
}
macro_rules! check {
    ($c:expr,$m:expr) => {
        if !$c {
            return Err($m.into());
        }
    };
}
#[derive(Clone, Copy, Debug)]
enum Ending {
    Graceful,
    UnmaskedFrame,
    AbruptDisconnect,
}
async fn provider(listener: TcpListener, stt: bool) -> Outcome {
    let (stream, _) = get!(listener.accept().await);
    let mut peer = get!(WebSocketSession::accept(stream, SessionConfig::default()).await);
    let observation: Outcome=async {
        while let Some(packet)=peer.recv().await {
            let Packet::Text(text)=packet else {return Err("unexpected provider audio".into());};
            let value:Value=get!(serde_json::from_str(&text));
            if stt && value["type"]=="CloseStream" {break;}
            if stt && value["type"]=="Finalize" {
                get!(peer.sender().send(Packet::Text(json!({"type":"Results","is_final":true,"from_finalize":true,"speech_final":true,"channel":{"alternatives":[{"transcript":""}]}}).to_string())).await);
            } else if !stt {return Err(format!("unexpected TTS synthesis: {value}"));}
        }
        Ok(())
    }.await;
    let cleanup = peer.finish().await;
    eprintln!("cleanup-provider stt={stt}; observation={observation:?}; finish={cleanup:?}");
    observation?;
    get!(cleanup);
    Ok(())
}
async fn send_masked(peer: &mut TcpStream, opcode: u8, payload: &[u8]) -> Outcome {
    check!(payload.len() <= 16384, "fixture frame budget");
    let mask = [0x31, 0x62, 0x93, 0xc4];
    let mut bytes = vec![0x80 | opcode];
    if payload.len() < 126 {
        bytes.push(0x80 | payload.len() as u8);
    } else {
        bytes.push(0xfe);
        bytes.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    bytes.extend_from_slice(&mask);
    bytes.extend(
        payload
            .iter()
            .enumerate()
            .map(|(index, b)| b ^ mask[index % 4]),
    );
    get!(peer.write_all(&bytes).await);
    Ok(())
}
async fn receive_frame(peer: &mut TcpStream) -> Outcome<(u8, Vec<u8>)> {
    let mut header = [0u8; 2];
    get!(peer.read_exact(&mut header).await);
    check!(
        header[0] & 0xf0 == 0x80 && header[1] & 0x80 == 0,
        "expected complete unmasked server frame"
    );
    let size = match header[1] & 0x7f {
        126 => {
            let mut b = [0; 2];
            get!(peer.read_exact(&mut b).await);
            u16::from_be_bytes(b) as usize
        }
        127 => return Err("server frame exceeds fixture length class".into()),
        n => n as usize,
    };
    check!(size <= 16384, "server frame budget");
    let mut bytes = vec![0; size];
    get!(peer.read_exact(&mut bytes).await);
    Ok((header[0] & 15, bytes))
}
struct Fixture {
    runner: Arc<NativeRunner>,
    reports: broadcast::Receiver<SessionSummary>,
    stop: Option<oneshot::Sender<()>>,
    tasks: Vec<JoinHandle<Outcome>>,
    http: String,
    peer: Option<TcpStream>,
    addresses: Vec<SocketAddr>,
    unused_providers: Vec<TcpListener>,
    outcomes: Vec<String>,
}
impl Fixture {
    async fn new() -> Outcome<Self> {
        let dg = get!(TcpListener::bind("127.0.0.1:0").await);
        let tts = get!(TcpListener::bind("127.0.0.1:0").await);
        let llm = get!(TcpListener::bind("127.0.0.1:0").await);
        let host = get!(TcpListener::bind("127.0.0.1:0").await);
        let addresses = [&dg, &tts, &llm, &host]
            .iter()
            .map(|p| p.local_addr().map_err(|e| e.to_string()))
            .collect::<Outcome<Vec<_>>>()?;
        let origin = format!("http://{}", addresses[3]);
        let runner = Arc::new(get!(NativeRunner::new(
            RunnerConfig {
                allowed_origins: vec![origin.clone()],
                options: Options {
                    introduce: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            ProviderConfig {
                deepgram_key: "synthetic-only".into(),
                openai_key: "synthetic-only".into(),
                cartesia_key: "synthetic-only".into(),
                deepgram_endpoint: format!("ws://{}/stt", addresses[0]),
                openai_endpoint: format!("http://{}/llm", addresses[2]),
                cartesia_endpoint: format!("ws://{}/tts", addresses[1]),
                model: "synthetic".into(),
                voice_id: "synthetic".into(),
                system_prompt: "fixture".into(),
                input_rate: 16000,
                output_rate: 24000,
            }
        )));
        let reports = runner.subscribe();
        let (stop, stopped) = oneshot::channel();
        let api = runner.clone();
        let server_origin = origin.clone();
        let server = tokio::spawn(async move {
            get!(
                serve_with_api(
                    host,
                    api,
                    ServerConfig {
                        allowed_origins: vec![server_origin],
                        ..Default::default()
                    },
                    async {
                        let _ = stopped.await;
                    }
                )
                .await
            );
            Ok(())
        });
        let tasks = vec![tokio::spawn(provider(dg, true)), server];
        Ok(Self {
            runner,
            reports,
            stop: Some(stop),
            tasks,
            http: origin,
            peer: None,
            addresses,
            unused_providers: vec![llm, tts],
            outcomes: vec![],
        })
    }
    async fn observe(&mut self, ending: Ending) -> Outcome<bool> {
        let start = format!("{}/api/start", self.http);
        let response: Value = get!(
            get!(get!(
                reqwest::Client::new()
                    .post(&start)
                    .header("Origin", &self.http)
                    .json(&json!({"transport":"websocket"}))
                    .send()
                    .await
            )
            .error_for_status())
            .json()
            .await
        );
        let url = get!(websocket_endpoint(
            &get!(url::Url::parse(&start)),
            &response
        ));
        let address = get!(format!(
            "{}:{}",
            url.host_str().ok_or("missing host")?,
            url.port().ok_or("missing port")?
        )
        .parse::<SocketAddr>());
        self.addresses.push(address);
        self.peer = Some(get!(TcpStream::connect(address).await));
        let peer = self.peer.as_mut().unwrap();
        // Read the HTTP response one byte at a time: no codec read-ahead is discarded.
        let request=format!("GET {} HTTP/1.1\r\nHost: {}\r\nOrigin: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",url.path(),address,self.http);
        get!(peer.write_all(request.as_bytes()).await);
        let mut response_header = vec![];
        while !response_header.ends_with(b"\r\n\r\n") {
            check!(response_header.len() < 16384, "upgrade header overflow");
            let mut b = [0];
            get!(peer.read_exact(&mut b).await);
            response_header.push(b[0]);
        }
        let response_header = get!(std::str::from_utf8(&response_header));
        check!(
            response_header.starts_with("HTTP/1.1 101 "),
            "upgrade rejected"
        );
        send_masked(peer, 2, &protocol::encode_message(&client_ready(701))).await?;
        loop {
            let (opcode, bytes) = receive_frame(peer).await?;
            check!(opcode == 2, "unexpected pre-ready frame");
            let frame = get!(protocol::decode(&bytes));
            let Some(protocol::frame::Body::Message(message)) = frame.body else {
                return Err("expected ready message".into());
            };
            let value: Value = get!(serde_json::from_str(&message.data));
            if value["type"] == "bot-ready" {
                check!(
                    value["label"] == "rtvi-ai"
                        && value["id"] == "701"
                        && value["data"]["version"] == "2.2.0",
                    "ready identity mismatch"
                );
                break;
            }
        }
        check!(
            self.runner.active_sessions() == 1,
            "missing active session before fault"
        );
        match ending {
            Ending::Graceful => {
                send_masked(peer, 8, &[]).await?;
                let (opcode, bytes) = receive_frame(peer).await?;
                check!(opcode == 8 && bytes.is_empty(), "close reply mismatch");
            }
            Ending::AbruptDisconnect => {
                get!(peer.shutdown().await);
                self.outcomes
                    .push("deliberate TCP write EOF without WebSocket Close".into());
            }
            Ending::UnmaskedFrame => {
                get!(peer.write_all(&[0x82, 0x00]).await);
                self.outcomes.push(
                    "deliberate malformed client frame bytes=8200 (binary, unmasked, empty)".into(),
                );
            }
        }
        let mut byte = [0];
        let terminal = peer.read(&mut byte).await;
        self.outcomes.push(format!("terminal={terminal:?}"));
        check!(
            matches!(&terminal, Ok(0))
                || matches!(&terminal,Err(error) if matches!(ending,Ending::UnmaskedFrame) && error.kind()==std::io::ErrorKind::ConnectionReset),
            "unexpected socket terminal or extra bytes"
        );
        let report = get!(self.reports.recv().await);
        self.outcomes.push(format!(
            "summary session_match={}; successful={}; pipeline_errors={}",
            report.session_id == response["sessionId"].as_str().unwrap_or(""),
            report.successful,
            report.pipeline_errors
        ));
        check!(
            report.session_id == response["sessionId"].as_str().ok_or("missing id")?,
            "summary identity mismatch"
        );
        check!(
            report.pipeline_errors == 0,
            "summary pipeline_errors projection changed"
        );
        check!(
            self.runner.active_sessions() == 0,
            "session admission not retired"
        );
        Ok(report.successful)
    }
    async fn cleanup(mut self) -> Vec<String> {
        let mut failures = vec![];
        drop(self.peer.take());
        let stopped = tokio::time::timeout(Duration::from_secs(8), self.runner.shutdown()).await;
        self.outcomes.push(format!("runner shutdown={stopped:?}"));
        if stopped.is_err() {
            failures.push("runner shutdown deadline; join unverified".into());
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        for listener in self.unused_providers {
            let unexpected =
                tokio::time::timeout(Duration::from_millis(20), listener.accept()).await;
            self.outcomes.push(format!(
                "unused LLM/TTS endpoint idle={}",
                unexpected.is_err()
            ));
            if unexpected.is_ok() {
                failures.push("unexpected LLM/TTS request in ready-only fixture".into());
            }
            drop(listener);
        }
        for (index, mut task) in self.tasks.into_iter().enumerate() {
            match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(result) => {
                    self.outcomes.push(format!("owned task {index}={result:?}"));
                    if !matches!(result, Ok(Ok(()))) {
                        failures.push(format!("task{index}: {result:?}"));
                    }
                }
                Err(_) => {
                    task.abort();
                    let result = task.await;
                    failures.push(format!(
                        "task{index} deadline; fallback abort joined={result:?}"
                    ));
                }
            }
        }
        let active = self.runner.active_sessions();
        if active != 0 {
            failures.push(format!("active sessions={active}"));
        }
        let rebind = tokio::time::timeout(Duration::from_secs(2), async {
            for address in &self.addresses {
                loop {
                    if let Ok(listener) = TcpListener::bind(address).await {
                        drop(listener);
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
        })
        .await;
        self.outcomes
            .push(format!("all listener ports rebound={}", rebind.is_ok()));
        if rebind.is_err() {
            failures.push("listener release unverified".into());
        }
        eprintln!(
            "session-cleanup endpoint outcomes={:?}; cleanup={failures:?}",
            self.outcomes
        );
        failures
    }
}
async fn exercise(ending: Ending) {
    let mut fixture = Fixture::new().await.expect("owned fixture setup");
    let observation = tokio::time::timeout(Duration::from_secs(20), fixture.observe(ending)).await;
    let cleanup = fixture.cleanup().await;
    eprintln!(
        "session-cleanup ending={ending:?}; observation={observation:?}; cleanup={cleanup:?}"
    );
    assert!(cleanup.is_empty(), "{cleanup:?}");
    let successful = observation
        .expect("bounded observation")
        .expect("actual ready/socket/report boundary");
    assert_eq!(
        successful,
        matches!(ending, Ending::Graceful),
        "transport failure must not become a successful runner summary"
    );
}
#[tokio::test]
async fn normal_websocket_close_preserves_success_and_joins_all_owners() {
    exercise(Ending::Graceful).await;
}
#[tokio::test]
async fn malformed_websocket_frame_cannot_report_success_after_cleanup() {
    exercise(Ending::UnmaskedFrame).await;
}

#[tokio::test]
async fn abrupt_websocket_eof_cannot_report_success_after_cleanup() {
    exercise(Ending::AbruptDisconnect).await;
}
