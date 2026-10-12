//! Shared InputTransport to Gemini Live over a real localhost WebSocket.
#![cfg(feature = "gemini-live")]

use futures_util::{SinkExt, StreamExt};
use pipecat::{
    audio::{encode_pcm16_le, PcmFormat},
    core::*,
    gemini_live::{GeminiClient, GeminiConfig, GeminiLiveProcessor},
    media_transport::{InputBackend, InputIngress, InputTransport, InputTransportConfig},
};
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, Mutex as AsyncMutex},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::tungstenite::{handshake::server::Request, Message};

// Test deadlines only: production capacities and deadlines remain at defaults.
const WAIT: Duration = Duration::from_secs(45);
const MARKER_BASE64: &str = "BQYHCA==";
type TestResult<T> = Result<T, String>;

async fn bounded<T, E: std::fmt::Display>(
    label: &str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    timeout(WAIT, future)
        .await
        .map_err(|_| format!("{label}: deadline"))?
        .map_err(|error| format!("{label}: {error}"))
}

#[derive(Debug, Clone)]
struct Handshake {
    path: String,
    query: Option<String>,
    authorization: bool,
}
#[derive(Debug)]
struct PeerReport {
    handshake: Handshake,
    setup: Value,
    commands: Vec<Value>,
    clean_close: bool,
}
struct Peer(JoinHandle<TestResult<PeerReport>>);
impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite fixes the callback error type.
    async fn new() -> TestResult<(Self, String)> {
        let listener = bounded("bind localhost", TcpListener::bind("127.0.0.1:0")).await?;
        let endpoint = format!(
            "ws://{}/live",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let task = tokio::spawn(async move {
            let (tcp, _) = bounded("accept localhost", listener.accept()).await?;
            let facts = Arc::new(Mutex::new(None));
            let callback_facts = facts.clone();
            let mut ws = bounded(
                "WebSocket handshake",
                tokio_tungstenite::accept_hdr_async(tcp, move |request: &Request, response| {
                    *callback_facts.lock().unwrap() = Some(Handshake {
                        path: request.uri().path().into(),
                        query: request.uri().query().map(str::to_owned),
                        authorization: request.headers().contains_key("authorization"),
                    });
                    Ok(response)
                }),
            )
            .await?;
            let mut setup = None;
            let mut commands = Vec::new();
            let mut clean_close = false;
            loop {
                let next = timeout(WAIT, ws.next())
                    .await
                    .map_err(|_| "peer read deadline".to_owned())?;
                match next {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value =
                            serde_json::from_str(&text).map_err(|e| e.to_string())?;
                        if setup.is_none() {
                            setup = Some(value);
                            bounded(
                                "send setupComplete",
                                ws.send(Message::Text(
                                    json!({"setupComplete":{}}).to_string().into(),
                                )),
                            )
                            .await?;
                        } else {
                            let is_marker =
                                value["realtimeInput"]["audio"]["data"] == MARKER_BASE64;
                            commands.push(value);
                            if is_marker {
                                // This response depends only on plain InputAudio, never UserAudio.
                                bounded(
                                    "send marker terminal response",
                                    ws.send(Message::Text(
                                        json!({"serverContent":{
                                            "modelTurn":{"role":"model","parts":[{"text":"barrier"}]},
                                            "turnComplete":true
                                        }})
                                        .to_string()
                                        .into(),
                                    )),
                                )
                                .await?;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        bounded("answer Ping", ws.send(Message::Pong(bytes))).await?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        clean_close = true;
                        // Reading Close queues tungstenite's reply; flush it before joining.
                        bounded("flush Close", ws.flush()).await?;
                        break;
                    }
                    None => break,
                    Some(Err(error)) => return Err(format!("peer socket: {error}")),
                    Some(Ok(other)) => return Err(format!("unexpected peer message: {other:?}")),
                }
            }
            let handshake = facts.lock().unwrap().clone().ok_or("missing handshake")?;
            Ok(PeerReport {
                handshake,
                setup: setup.ok_or("missing setup")?,
                commands,
                clean_close,
            })
        });
        Ok((Self(task), endpoint))
    }
    async fn joined(mut self) -> TestResult<PeerReport> {
        match timeout(WAIT, &mut self.0).await {
            Ok(result) => result.map_err(|e| format!("peer task: {e}"))?,
            Err(_) => {
                self.0.abort();
                let _ = self.0.await;
                Err("peer join deadline; aborted and joined".into())
            }
        }
    }
}

struct CaptureCommand {
    frame: Frame,
    admitted: oneshot::Sender<TestResult<()>>,
}
struct Backend {
    started: AtomicUsize,
    stopped: AtomicUsize,
    captures: AtomicUsize,
    format: Mutex<Option<PcmFormat>>,
    commands: AsyncMutex<mpsc::Receiver<CaptureCommand>>,
}
#[async_trait]
impl InputBackend for Backend {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        *self.format.lock().unwrap() = Some(format);
        Ok(())
    }
    async fn capture(&self, ingress: InputIngress) -> Result<(), ProcessorError> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        let mut commands = self.commands.lock().await;
        while let Some(command) = commands.recv().await {
            // The actual processor-owned backend capture task performs admission.
            let result = ingress.push(command.frame).map_err(|e| e.to_string());
            let _ = command.admitted.send(result);
        }
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Case {
    worker: PipelineWorker,
    capture: mpsc::Sender<CaptureCommand>,
    backend: Arc<Backend>,
    peer: Peer,
}
impl Case {
    async fn start() -> TestResult<Self> {
        let (peer, endpoint) = Peer::new().await?;
        let mut config = GeminiConfig::new("shared-gemini-loopback-key", "source-model");
        config.endpoint = endpoint;
        let gemini = Arc::new(GeminiLiveProcessor::new(
            GeminiClient::new(config).map_err(|e| e.to_string())?,
        ));
        let (capture, commands) = mpsc::channel(32);
        let backend = Arc::new(Backend {
            started: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
            captures: AtomicUsize::new(0),
            format: Mutex::new(None),
            commands: AsyncMutex::new(commands),
        });
        let input = Arc::new(
            InputTransport::new(
                "shared-gemini-input",
                InputTransportConfig::default(),
                backend.clone(),
                None,
            )
            .map_err(|e| e.to_string())?,
        );
        let mut pipeline = PipelineConfig::default();
        pipeline.start.audio_in_sample_rate = 16_000;
        pipeline.start.audio_out_sample_rate = 24_000;
        let pipeline_gemini = gemini.clone();
        let mut startup = tokio::spawn(async move {
            Pipeline::new(vec![input, pipeline_gemini])
                .start(pipeline)
                .await
        });
        let result = match timeout(WAIT, &mut startup).await {
            Ok(result) => result
                .map_err(|e| e.to_string())
                .and_then(|r| r.map_err(|e| e.to_string())),
            Err(_) => {
                if let Ok(sender) = gemini.sender() {
                    sender.cancel();
                }
                startup.abort();
                let _ = startup.await;
                Err("pipeline startup deadline; aborted and joined".into())
            }
        };
        match result {
            Ok(worker) => Ok(Self {
                worker,
                capture,
                backend,
                peer,
            }),
            Err(error) => {
                if let Ok(sender) = gemini.sender() {
                    sender.cancel();
                }
                let peer_result = peer.joined().await;
                Err(format!("{error}; peer cleanup: {peer_result:?}"))
            }
        }
    }
    async fn submit(&self, frame: Frame, capture: bool) -> TestResult<()> {
        if capture {
            let (admitted, result) = oneshot::channel();
            bounded(
                "backend command",
                self.capture.send(CaptureCommand { frame, admitted }),
            )
            .await?;
            bounded("backend admission", result).await?
        } else {
            bounded(
                "pipeline injection",
                self.worker.sender().send(frame, Direction::Downstream),
            )
            .await
        }
    }
    async fn finish(
        self,
        graceful: bool,
    ) -> (
        TestResult<(Vec<DirectedFrame>, WorkerReport)>,
        TestResult<PeerReport>,
    ) {
        let sender = self.worker.sender();
        let terminal = if graceful {
            bounded("normal End", sender.end()).await
        } else {
            bounded(
                "cancel failed scenario",
                sender.cancel("facade scenario failed"),
            )
            .await
        };
        if terminal.is_err() {
            let _ = bounded(
                "cancel failed terminal",
                sender.cancel("facade terminal failed"),
            )
            .await;
        }
        let mut collection = tokio::spawn(self.worker.collect());
        let collected = match timeout(WAIT, &mut collection).await {
            Ok(result) => result
                .map_err(|e| e.to_string())
                .and_then(|r| r.map_err(|e| e.to_string())),
            Err(_) => {
                let _ = bounded(
                    "cancel overdue collection",
                    sender.cancel("facade collect deadline"),
                )
                .await;
                match timeout(WAIT, &mut collection).await {
                    Ok(result) => {
                        let result = result
                            .map_err(|e| e.to_string())
                            .and_then(|r| r.map_err(|e| e.to_string()));
                        Err(format!(
                            "collection exceeded deadline; cancelled and joined: {result:?}"
                        ))
                    }
                    Err(_) => {
                        collection.abort();
                        let _ = collection.await;
                        Err("collection exceeded cancellation deadline; aborted and joined".into())
                    }
                }
            }
        };
        let peer = self.peer.joined().await;
        (terminal.and(collected), peer)
    }
}

fn audio(user: Option<&str>, rate: u32, samples: &[i16]) -> Frame {
    let audio = AudioRawFrame::new(encode_pcm16_le(samples), rate, 1).unwrap();
    Frame::new(match user {
        Some(user_id) => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: user_id.into(),
        }),
        None => FrameData::InputAudio(audio),
    })
}
async fn receive(
    worker: &mut PipelineWorker,
    observed: &mut Vec<DirectedFrame>,
    wanted: impl Fn(&DirectedFrame) -> bool,
) -> TestResult<()> {
    timeout(WAIT, async {
        loop {
            let frame = worker
                .recv()
                .await
                .ok_or("pipeline closed before barrier")?;
            let error = match &frame.frame.data {
                FrameData::Error(error) => Some(format!("pipeline error: {error:?}")),
                _ => None,
            };
            let found = wanted(&frame);
            observed.push(frame);
            if let Some(error) = error {
                return Err(error);
            }
            if found {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| "pipeline frame barrier deadline".to_owned())?
}
fn assert_clean(report: &WorkerReport, peer: &PeerReport, backend: &Backend) {
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.captures.load(Ordering::SeqCst), 1);
    assert_eq!(
        *backend.format.lock().unwrap(),
        Some(PcmFormat::new(16_000, 1).unwrap())
    );
    assert_eq!(peer.handshake.path, "/live");
    assert_eq!(
        peer.handshake.query.as_deref(),
        Some("key=shared-gemini-loopback-key")
    );
    assert!(!peer.handshake.authorization);
    assert!(peer.setup["setup"].is_object());
    assert_eq!(peer.setup["setup"]["model"], "models/source-model");
    assert_eq!(
        peer.setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"]["disabled"],
        false
    );
    assert!(peer
        .commands
        .iter()
        .all(|command| command.get("setup").is_none()));
    assert!(peer.clean_close, "peer did not observe a WebSocket Close");
}

async fn assert_shared_user_audio_case(capture: bool) {
    let mut user = audio(Some("captured-speaker"), 16_000, &[513, 1027]);
    user.name = "captured-user-pcm".into();
    user.set_pipeline_pts(12345);
    user.transport_source = Some("microphone".into());
    user.transport_destination = Some("gemini-input".into());
    user.broadcast_sibling_id = Some(987654);
    user.interruptible = false;
    user.metadata.insert("capture".into(), json!(capture));
    let tag = LocalFrameTag::new();
    tag.mark(&mut user).unwrap();
    let marker = audio(None, 16_000, &[1541, 2055]);
    let mut case = Case::start()
        .await
        .expect("start actual shared transport and Gemini");
    let backend = case.backend.clone();
    let mut observed = Vec::new();
    let scenario: TestResult<()> = async {
        case.submit(user.clone(), capture).await?;
        receive(&mut case.worker, &mut observed, |frame| {
            frame.frame.id == user.id
        })
        .await?;
        // The plain marker is sent only after UserAudio passed the actual adapter.
        case.submit(marker.clone(), capture).await?;
        receive(&mut case.worker, &mut observed, |frame| {
            frame.frame.id == marker.id
        })
        .await?;
        // Text opens the model response, so turnComplete produces LlmResponseEnd.
        if !observed
            .iter()
            .any(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd))
        {
            receive(&mut case.worker, &mut observed, |frame| {
                matches!(frame.frame.data, FrameData::LlmResponseEnd)
            })
            .await?;
        }
        Ok(())
    }
    .await;
    let (collected, peer) = case.finish(scenario.is_ok()).await;
    // No expected-UserAudio wire wait or assertion can prevent cleanup.
    scenario.expect("traversal and marker terminal boundary");
    let (remaining, report) = collected.expect("pipeline joined after normal End");
    observed.extend(remaining);
    let peer = peer.expect("localhost peer joined");
    assert_clean(&report, &peer, &backend);
    let forwarded: Vec<_> = observed
        .iter()
        .filter(|frame| {
            matches!(
                frame.frame.data,
                FrameData::InputAudio(_) | FrameData::UserAudio(_)
            )
        })
        .collect();
    assert_eq!(forwarded.len(), 2);
    assert_eq!(forwarded[0].direction, Direction::Downstream);
    assert_eq!(forwarded[0].frame, user);
    assert!(tag.contains(&forwarded[0].frame));
    assert_eq!(forwarded[1].direction, Direction::Downstream);
    assert_eq!(forwarded[1].frame, marker);
    assert_eq!(
            peer.commands,
            vec![
                json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":"AQIDBA=="}}}),
                json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":MARKER_BASE64}}}),
            ],
            "InputTransport UserAudio PCM must reach Gemini; old code forwards only the independent plain marker (capture={capture})"
        );
}

#[tokio::test]
async fn shared_backend_capture_preserves_user_audio_on_gemini_wire() {
    assert_shared_user_audio_case(true).await;
}

#[tokio::test]
async fn shared_pipeline_injection_preserves_user_audio_on_gemini_wire() {
    assert_shared_user_audio_case(false).await;
}

// Additive control: all stream conversion belongs to the actual shared transport.
// Gemini receives only the resulting configured 16 kHz mono PCM.
fn pcm_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::new();
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        encoded.push(char::from(ALPHABET[usize::from(a >> 2)]));
        encoded.push(char::from(ALPHABET[usize::from(((a & 3) << 4) | (b >> 4))]));
        encoded.push(if chunk.len() > 1 {
            char::from(ALPHABET[usize::from(((b & 15) << 2) | (c >> 6))])
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            char::from(ALPHABET[usize::from(c & 63)])
        } else {
            '='
        });
    }
    encoded
}
fn user_audio_count(frames: &[DirectedFrame]) -> usize {
    frames
        .iter()
        .filter(|frame| matches!(frame.frame.data, FrameData::UserAudio(_)))
        .count()
}

#[tokio::test]
async fn shared_captured_user_audio_resamples_24k_to_16k_with_tail_before_marker() {
    use pipecat::audio::{resample_pcm16, SincResampler};

    // Independent calls to the public shared-audio API establish stream/tail
    // expectations; these do not replace the InputTransport instance below.
    let samples: Vec<i16> = (0..501).map(|sample| sample * 40 - 10_000).collect();
    let pcm = encode_pcm16_le(&samples);
    let expected = resample_pcm16(&pcm, 24_000, 16_000, 1).unwrap();
    let mut reference = SincResampler::new(24_000, 16_000, 1).unwrap();
    let mut expected_chunks = Vec::new();
    for chunk in samples.chunks(33) {
        let output = reference.process(&encode_pcm16_le(chunk)).unwrap();
        if !output.is_empty() {
            expected_chunks.push(output);
        }
    }
    let streamed_bytes: usize = expected_chunks.iter().map(Vec::len).sum();
    let tail = reference.flush().unwrap();
    assert!(
        !tail.is_empty(),
        "fixture must exercise a retained resampler tail"
    );
    assert!(streamed_bytes < expected.len());
    expected_chunks.push(tail.clone());
    assert_eq!(expected_chunks.concat(), expected);
    // Known vectors guard the small local encoder used only for wire assertions.
    assert_eq!(pcm_base64(&[1, 2, 3, 4]), "AQIDBA==");
    assert_eq!(pcm_base64(&[5, 6, 7, 8]), MARKER_BASE64);

    let tag = LocalFrameTag::new();
    let mut source_frames = Vec::new();
    for chunk in samples.chunks(33) {
        let mut frame = audio(Some("resampled-speaker"), 24_000, chunk);
        let FrameData::UserAudio(user) = &mut frame.data else {
            unreachable!()
        };
        user.audio.context_id = Some("resampling-context".into());
        frame.transport_source = Some("microphone".into());
        frame.transport_destination = Some("gemini-input".into());
        frame
            .metadata
            .insert("owner".into(), json!("resampled-speaker"));
        tag.mark(&mut frame).unwrap();
        source_frames.push(frame);
    }
    let source_ids: Vec<_> = source_frames.iter().map(|frame| frame.id).collect();
    let flush = Frame::new(FrameData::FilterControl);
    let marker = audio(None, 16_000, &[1541, 2055]);
    let mut case = Case::start()
        .await
        .expect("start actual resampling input transport");
    let backend = case.backend.clone();
    let mut observed = Vec::new();
    let scenario: TestResult<()> = async {
        for frame in source_frames {
            case.submit(frame, true).await?;
        }
        // Every backend command was admitted before the queued flush control.
        bounded(
            "enqueue resampler flush",
            case.worker
                .sender()
                .send(flush.clone(), Direction::Downstream),
        )
        .await?;
        receive(&mut case.worker, &mut observed, |frame| {
            frame.frame.id == flush.id
        })
        .await?;
        // System audio and normal FilterControl may run concurrently downstream.
        // Wait for each expected forwarded chunk, independent of provider PCM.
        while user_audio_count(&observed) < expected_chunks.len() {
            receive(&mut case.worker, &mut observed, |frame| {
                matches!(frame.frame.data, FrameData::UserAudio(_))
            })
            .await?;
        }
        case.submit(marker.clone(), true).await?;
        receive(&mut case.worker, &mut observed, |frame| {
            frame.frame.id == marker.id
        })
        .await?;
        if !observed
            .iter()
            .any(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd))
        {
            receive(&mut case.worker, &mut observed, |frame| {
                matches!(frame.frame.data, FrameData::LlmResponseEnd)
            })
            .await?;
        }
        Ok(())
    }
    .await;
    let (collected, peer) = case.finish(scenario.is_ok()).await;
    scenario.expect("flush traversal and independent marker terminal boundary");
    let (remaining, report) = collected.expect("pipeline joined after normal End");
    observed.extend(remaining);
    let peer = peer.expect("localhost peer joined");
    assert_clean(&report, &peer, &backend);

    let users: Vec<_> = observed
        .iter()
        .filter(|frame| matches!(frame.frame.data, FrameData::UserAudio(_)))
        .collect();
    assert_eq!(users.len(), expected_chunks.len());
    let mut forwarded = Vec::new();
    for (frame, expected_chunk) in users.iter().zip(&expected_chunks) {
        let FrameData::UserAudio(user) = &frame.frame.data else {
            unreachable!()
        };
        assert_eq!(frame.direction, Direction::Downstream);
        assert_eq!(user.user_id, "resampled-speaker");
        assert_eq!(user.audio.sample_rate, 16_000);
        assert_eq!(user.audio.num_channels, 1);
        assert_eq!(user.audio.context_id.as_deref(), Some("resampling-context"));
        assert_eq!(user.audio.audio, *expected_chunk);
        assert_eq!(frame.frame.transport_source.as_deref(), Some("microphone"));
        assert_eq!(
            frame.frame.transport_destination.as_deref(),
            Some("gemini-input")
        );
        assert_eq!(frame.frame.metadata["owner"], "resampled-speaker");
        assert!(tag.contains(&frame.frame));
        forwarded.extend_from_slice(&user.audio.audio);
    }
    assert_eq!(forwarded, expected);
    assert!(
        !source_ids.contains(&users.last().unwrap().frame.id),
        "tail must be a new frame created by shared transport flush"
    );
    let FrameData::UserAudio(last) = &users.last().unwrap().frame.data else {
        unreachable!()
    };
    assert_eq!(last.audio.audio, tail);
    let marker_frames: Vec<_> = observed
        .iter()
        .filter(|frame| matches!(frame.frame.data, FrameData::InputAudio(_)))
        .collect();
    assert_eq!(marker_frames.len(), 1);
    assert_eq!(marker_frames[0].frame, marker);
    assert_eq!(marker_frames[0].direction, Direction::Downstream);
    let mut expected_commands: Vec<_> = expected_chunks.iter().map(|chunk| {
        json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":pcm_base64(chunk)}}})
    }).collect();
    expected_commands.push(
        json!({"realtimeInput":{"audio":{"mimeType":"audio/pcm;rate=16000","data":MARKER_BASE64}}}),
    );
    assert_eq!(peer.commands, expected_commands, "wire must contain every shared-resampler output including its tail before the plain marker");
}
