//! Real localhost Gemini Live frame-family contract; no Google connection.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::Message;

const DEADLINE: Duration = Duration::from_secs(5);
const MARKER: &[u8] = &[91, 0, 92, 0];

#[derive(Debug)]
struct Trace {
    path: String,
    query: Option<String>,
    authorization: bool,
    setup: Value,
    wire: Vec<Value>,
    closed: bool,
}
struct Peer {
    task: JoinHandle<Result<Trace, String>>,
}
impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite's handshake callback error type.
    async fn open(mut config: GeminiConfig, manual_barrier: bool) -> (Self, GeminiConfig) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (tcp, _) = timeout(DEADLINE, listener.accept())
                .await
                .map_err(|_| "accept deadline")?
                .map_err(|e| e.to_string())?;
            let mut request_info = None;
            let mut ws = timeout(
                DEADLINE,
                tokio_tungstenite::accept_hdr_async(
                    tcp,
                    |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     response| {
                        request_info = Some((
                            request.uri().path().to_owned(),
                            request.uri().query().map(str::to_owned),
                            request.headers().contains_key("authorization"),
                        ));
                        Ok(response)
                    },
                ),
            )
            .await
            .map_err(|_| "upgrade deadline")?
            .map_err(|e| e.to_string())?;
            let first = timeout(DEADLINE, ws.next())
                .await
                .map_err(|_| "setup deadline")?
                .ok_or("closed before setup")?
                .map_err(|e| e.to_string())?;
            let setup = serde_json::from_str(first.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            let (path, query, authorization) = request_info.ok_or("missing request")?;
            let mut trace = Trace {
                path,
                query,
                authorization,
                setup,
                wire: vec![],
                closed: false,
            };
            timeout(
                DEADLINE,
                ws.send(Message::Text(
                    json!({"setupComplete":{}}).to_string().into(),
                )),
            )
            .await
            .map_err(|_| "ready deadline")?
            .map_err(|e| e.to_string())?;
            loop {
                let message = timeout(DEADLINE, ws.next())
                    .await
                    .map_err(|_| "peer read deadline")?;
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value =
                            serde_json::from_str(&text).map_err(|e| e.to_string())?;
                        let barrier = if manual_barrier {
                            value["realtimeInput"].get("activityEnd").is_some()
                        } else {
                            value["realtimeInput"]["audio"]["data"] == STANDARD.encode(MARKER)
                        };
                        trace.wire.push(value);
                        if barrier {
                            // A model part opens the response, so turnComplete maps to an
                            // observable LlmResponseEnd even when UserAudio never reached wire.
                            let event = json!({"serverContent":{"modelTurn":{"parts":[{"text":"barrier"}]},"turnComplete":true}});
                            timeout(DEADLINE, ws.send(Message::Text(event.to_string().into())))
                                .await
                                .map_err(|_| "barrier write deadline")?
                                .map_err(|e| e.to_string())?;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        timeout(DEADLINE, ws.send(Message::Pong(bytes)))
                            .await
                            .map_err(|_| "pong deadline")?
                            .map_err(|e| e.to_string())?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        trace.closed = true;
                        let _ = timeout(DEADLINE, ws.flush()).await;
                        return Ok(trace);
                    }
                    None => return Ok(trace),
                    Some(Err(error)) => return Err(error.to_string()),
                    Some(Ok(other)) => return Err(format!("unexpected peer message: {other:?}")),
                }
            }
        });
        (Self { task }, config)
    }
    async fn join(mut self) -> Result<Trace, String> {
        match timeout(DEADLINE, &mut self.task).await {
            Ok(result) => result.map_err(|e| e.to_string())?,
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                Err("peer join deadline; aborted and joined".into())
            }
        }
    }
}
fn configuration(manual: bool) -> GeminiConfig {
    let mut config = GeminiConfig::new("family-loopback-key", "family-loopback-model");
    config.session.activity_detection.disabled = manual;
    config
}
fn audio(user: Option<&str>, bytes: Vec<u8>) -> Frame {
    let audio = AudioRawFrame::new(bytes, 16_000, 1).unwrap();
    Frame::new(match user {
        Some(user_id) => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: user_id.into(),
        }),
        None => FrameData::InputAudio(audio),
    })
}
fn decorated(mut frame: Frame) -> (Frame, LocalFrameTag) {
    frame.name = "retained-family-envelope".into();
    frame.set_pipeline_pts(12345);
    frame.transport_source = Some("microphone".into());
    frame.transport_destination = Some("route".into());
    frame.broadcast_sibling_id = Some(9876);
    frame.interruptible = true;
    frame
        .metadata
        .insert("application".into(), json!({"sequence":7}));
    let tag = LocalFrameTag::new();
    tag.mark(&mut frame).unwrap();
    (frame, tag)
}
struct Run {
    worker: PipelineWorker,
    peer: Peer,
    seen: Vec<DirectedFrame>,
}
impl Run {
    async fn start(config: GeminiConfig, preroll_ms: Option<u32>, manual_barrier: bool) -> Self {
        let (peer, config) = Peer::open(config, manual_barrier).await;
        let rate = config.input_sample_rate;
        let client = GeminiClient::new(config).unwrap();
        let mut processor = GeminiLiveProcessor::new(client);
        if let Some(milliseconds) = preroll_ms {
            processor = processor.with_preroll_ms(milliseconds).unwrap();
        }
        let mut config = PipelineConfig::default();
        config.start.audio_in_sample_rate = rate;
        config.start.audio_out_sample_rate = 24_000;
        match timeout(
            DEADLINE,
            Pipeline::new(vec![Arc::new(processor)]).start(config),
        )
        .await
        {
            Ok(Ok(worker)) => Self {
                worker,
                peer,
                seen: vec![],
            },
            other => {
                let trace = peer.join().await;
                panic!("pipeline setup failed: {:?}; peer: {trace:?}", other.err());
            }
        }
    }
    async fn receive(
        &mut self,
        wanted: impl Fn(&DirectedFrame) -> bool,
    ) -> Result<DirectedFrame, String> {
        timeout(DEADLINE, async {
            loop {
                let frame = self
                    .worker
                    .recv()
                    .await
                    .ok_or("pipeline closed before frame")?;
                let found = wanted(&frame);
                self.seen.push(frame.clone());
                if found {
                    return Ok(frame);
                }
            }
        })
        .await
        .map_err(|_| "pipeline frame deadline".to_owned())?
    }
    async fn send(&mut self, frame: Frame, direction: Direction) -> Result<DirectedFrame, String> {
        let id = frame.id;
        timeout(DEADLINE, self.worker.sender().send(frame, direction))
            .await
            .map_err(|_| "pipeline admission deadline")?
            .map_err(|e| e.to_string())?;
        self.receive(|frame| frame.frame.id == id && frame.direction == direction)
            .await
    }
    async fn barrier(&mut self) -> Result<(), String> {
        if self
            .seen
            .iter()
            .any(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd))
        {
            return Ok(());
        }
        self.receive(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd))
            .await?;
        Ok(())
    }
    async fn finish(
        mut self,
        normal: bool,
    ) -> (
        Result<WorkerReport, String>,
        Result<Trace, String>,
        Vec<DirectedFrame>,
    ) {
        let admission = timeout(DEADLINE, async {
            if normal {
                self.worker.end().await
            } else {
                self.worker.cancel("test cleanup").await
            }
        })
        .await
        .map_err(|_| "terminal admission deadline".to_owned())
        .and_then(|result| result.map_err(|error| error.to_string()));
        let mut report = match admission {
            Ok(()) => timeout(DEADLINE, async {
                while let Some(frame) = self.worker.recv().await {
                    self.seen.push(frame);
                }
                self.worker.join().await.map_err(|e| e.to_string())
            })
            .await
            .unwrap_or_else(|_| Err("pipeline finish deadline".into())),
            Err(error) => Err(error.to_string()),
        };
        if let Err(original_error) = &report {
            let original_error = original_error.clone();
            let _ = timeout(DEADLINE, self.worker.cancel("test deadline cleanup")).await;
            if let Ok(Ok(cleaned)) = timeout(DEADLINE, async {
                while let Some(frame) = self.worker.recv().await {
                    self.seen.push(frame);
                }
                self.worker.join().await
            })
            .await
            {
                report = Err(format!(
                    "{original_error}; cancellation joined: {:?}",
                    cleaned.state
                ));
            }
        }
        let trace = self.peer.join().await;
        (report, trace, self.seen)
    }
}
fn assert_clean(report: &WorkerReport, trace: &Trace) {
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(trace.path, "/live");
    assert_eq!(trace.query.as_deref(), Some("key=family-loopback-key"));
    assert!(!trace.authorization);
    assert_eq!(
        trace.setup["setup"]["model"],
        "models/family-loopback-model"
    );
    assert!(trace.closed, "peer must observe normal WebSocket Close");
    assert!(trace.wire.iter().all(|value| value.get("setup").is_none()));
}
fn pcm_messages(trace: &Trace) -> Vec<Vec<u8>> {
    trace
        .wire
        .iter()
        .filter_map(|value| {
            value["realtimeInput"]["audio"]["data"]
                .as_str()
                .map(|data| {
                    assert_eq!(
                        value["realtimeInput"]["audio"]["mimeType"],
                        "audio/pcm;rate=16000"
                    );
                    assert_eq!(value.as_object().unwrap().len(), 1);
                    assert_eq!(value["realtimeInput"].as_object().unwrap().len(), 1);
                    assert_eq!(
                        value["realtimeInput"]["audio"].as_object().unwrap().len(),
                        2
                    );
                    STANDARD.decode(data).unwrap()
                })
        })
        .collect()
}

#[tokio::test]
async fn user_audio_server_vad_reaches_wire_after_clean_terminal_barrier() {
    let mut run = Run::start(configuration(false), None, false).await;
    let (frame, tag) = decorated(audio(Some("captured-speaker"), vec![1, 2, 3, 4]));
    let expected = frame.clone();
    let steps = async {
        let forwarded = run.send(frame, Direction::Downstream).await?;
        let marker = audio(None, MARKER.to_vec());
        let expected_marker = marker.clone();
        let forwarded_marker = run.send(marker, Direction::Downstream).await?;
        run.barrier().await?;
        Ok::<_, String>((forwarded, expected_marker, forwarded_marker))
    }
    .await;
    let (report, trace, _) = run.finish(steps.is_ok()).await;
    let (forwarded, expected_marker, forwarded_marker) = steps.unwrap();
    let report = report.unwrap();
    let trace = trace.unwrap();
    assert_clean(&report, &trace);
    assert_eq!(forwarded.frame, expected);
    assert_eq!(forwarded.direction, Direction::Downstream);
    assert!(tag.contains(&forwarded.frame));
    assert_eq!(forwarded_marker.frame, expected_marker);
    assert_eq!(
        pcm_messages(&trace),
        vec![vec![1, 2, 3, 4], MARKER.to_vec()],
        "UserAudio PCM must precede the independent plain-InputAudio marker after clean End/join"
    );
    assert_eq!(trace.wire.len(), 2);
}

#[tokio::test]
async fn user_audio_manual_preroll_and_active_pcm_reach_wire_after_clean_end() {
    let mut run = Run::start(configuration(true), Some(1), true).await;
    let first: Vec<u8> = (0..20).collect();
    let second: Vec<u8> = (20..40).collect();
    let active = vec![41, 0, 42, 0];
    let steps = async {
        let mut preserved = vec![];
        for frame in [
            audio(Some("first-speaker"), first.clone()),
            audio(Some("second-speaker"), second.clone()),
            Frame::new(FrameData::UserStartedSpeaking),
            audio(Some("active-speaker"), active.clone()),
            audio(None, MARKER.to_vec()),
            Frame::new(FrameData::UserStoppedSpeaking),
        ] {
            let expected = frame.clone();
            let forwarded = run.send(frame, Direction::Downstream).await?;
            preserved.push((expected, forwarded));
        }
        run.barrier().await?;
        Ok::<_, String>(preserved)
    }
    .await;
    let (report, trace, _) = run.finish(steps.is_ok()).await;
    let preserved = steps.unwrap();
    let report = report.unwrap();
    let trace = trace.unwrap();
    assert_clean(&report, &trace);
    for (expected, actual) in preserved {
        assert_eq!(actual.frame, expected);
        assert_eq!(actual.direction, Direction::Downstream);
    }
    assert_eq!(
        trace.wire.first().unwrap(),
        &json!({"realtimeInput":{"activityStart":{}}})
    );
    assert_eq!(
        trace.wire.last().unwrap(),
        &json!({"realtimeInput":{"activityEnd":{}}})
    );
    assert_eq!(
        pcm_messages(&trace),
        vec![(8..40).collect::<Vec<u8>>(), active, MARKER.to_vec()],
        "newest 32 pre-roll bytes and active UserAudio must be present after clean End/join"
    );
    assert_eq!(trace.wire.len(), 5);
}

// Additional controls below; frozen old-code regressions and helpers above are unchanged.
fn wrapped_audio(user: Option<&str>, audio: AudioRawFrame) -> Frame {
    Frame::new(match user {
        Some(user_id) => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: user_id.into(),
        }),
        None => FrameData::InputAudio(audio),
    })
}
impl Run {
    async fn submit(&mut self, frame: Frame, direction: Direction) -> Result<(), String> {
        timeout(DEADLINE, self.worker.sender().send(frame, direction))
            .await
            .map_err(|_| "pipeline submission deadline")?
            .map_err(|e| e.to_string())
    }
    async fn error(&mut self) -> Result<ErrorFrame, String> {
        let frame = self
            .receive(|frame| matches!(frame.frame.data, FrameData::Error(_)))
            .await?;
        if frame.direction != Direction::Upstream {
            return Err("error direction changed".into());
        }
        match frame.frame.data {
            FrameData::Error(error) => Ok(error),
            _ => Err("expected Error frame".into()),
        }
    }
    async fn barriers(&mut self, expected: usize) -> Result<(), String> {
        while self
            .seen
            .iter()
            .filter(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd))
            .count()
            < expected
        {
            self.receive(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd))
                .await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn empty_upstream_and_output_families_keep_envelopes_without_wire_pcm() {
    let mut run = Run::start(configuration(false), None, false).await;
    let steps = async {
        let mut preserved = vec![];
        for user in [None, Some("wire-private-speaker")] {
            for pcm in [
                AudioRawFrame {
                    audio: vec![],
                    sample_rate: 16_000,
                    num_channels: 1,
                    context_id: Some("x".repeat(1025)),
                },
                AudioRawFrame {
                    audio: vec![1, 2, 3],
                    sample_rate: 0,
                    num_channels: 0,
                    context_id: None,
                },
                AudioRawFrame {
                    audio: vec![0; 192_002],
                    sample_rate: 16_000,
                    num_channels: 1,
                    context_id: Some("x".repeat(1025)),
                },
            ] {
                let (frame, tag) = decorated(wrapped_audio(user, pcm));
                let expected = frame.clone();
                let output = run.send(frame, Direction::Upstream).await?;
                preserved.push((expected, output, tag, Direction::Upstream));
            }
            let (frame, tag) = decorated(audio(user, vec![]));
            let expected = frame.clone();
            let output = run.send(frame, Direction::Downstream).await?;
            preserved.push((expected, output, tag, Direction::Downstream));
        }
        for data in [
            FrameData::OutputAudio(AudioRawFrame::new(vec![1, 2], 24_000, 1).unwrap()),
            FrameData::TtsAudio(AudioRawFrame::new(vec![3, 4], 24_000, 1).unwrap()),
        ] {
            let (frame, tag) = decorated(Frame::new(data));
            let expected = frame.clone();
            let output = run.send(frame, Direction::Downstream).await?;
            preserved.push((expected, output, tag, Direction::Downstream));
        }
        run.send(audio(None, MARKER.to_vec()), Direction::Downstream)
            .await?;
        run.barrier().await?;
        Ok::<_, String>(preserved)
    }
    .await;
    let (report, trace, _) = run.finish(steps.is_ok()).await;
    let preserved = steps.unwrap();
    let report = report.unwrap();
    let trace = trace.unwrap();
    assert_clean(&report, &trace);
    for (expected, output, tag, direction) in preserved {
        assert_eq!(output.frame, expected);
        assert_eq!(output.direction, direction);
        assert!(tag.contains(&output.frame));
    }
    assert_eq!(pcm_messages(&trace), vec![MARKER.to_vec()]);
    assert_eq!(trace.wire.len(), 1);
}

#[tokio::test]
async fn both_families_validate_geometry_format_raw_encoded_and_direct_context_limits() {
    for user in [None, Some("captured-speaker")] {
        for case in 0..10 {
            let mut config = configuration(false);
            let mut pcm = AudioRawFrame::new(vec![1, 2, 3, 4], 16_000, 1).unwrap();
            let (expected_category, expected_error) = match case {
                0 => {
                    pcm.sample_rate = 0;
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
                }
                1 => {
                    pcm.num_channels = 0;
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
                }
                2 => {
                    pcm.audio.pop();
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
                }
                3 => {
                    pcm.num_channels = 2;
                    (ErrorCategory::InvalidRequest, "configured mono PCM16 rate")
                }
                4 => {
                    pcm.sample_rate = 8_000;
                    (ErrorCategory::InvalidRequest, "configured mono PCM16 rate")
                }
                5 => {
                    config.limits.max_audio_bytes = 32;
                    pcm.audio = vec![0; 34];
                    (ErrorCategory::Application, "configured limit")
                }
                6 => {
                    // Equal limits are legal Gemini configuration. Raw PCM fits, base64+JSON does not.
                    config.limits.max_audio_bytes = 1024;
                    config.limits.max_message_bytes = 1024;
                    pcm.audio = vec![0; 768];
                    (ErrorCategory::Application, "configured limit")
                }
                7 => {
                    pcm.context_id = Some("x".repeat(1025));
                    (ErrorCategory::Application, "configured limit")
                }
                8 => {
                    pcm.audio.clear();
                    pcm.sample_rate = 0;
                    (ErrorCategory::Unknown, "invalid Gemini PCM")
                }
                _ => {
                    pcm.audio.clear();
                    pcm.sample_rate = 8_000;
                    (ErrorCategory::InvalidRequest, "configured mono PCM16 rate")
                }
            };
            // Construction/setup must succeed before interpreting any cap failure.
            let mut run = Run::start(config, None, false).await;
            let invalid = wrapped_audio(user, pcm);
            let invalid_id = invalid.id;
            let steps = async {
                run.submit(invalid, Direction::Downstream).await?;
                let error = run.error().await?;
                // These System-frame controls retain Gemini's existing health/admission boundary.
                run.send(audio(None, MARKER.to_vec()), Direction::Downstream)
                    .await?;
                run.barrier().await?;
                Ok::<_, String>(error)
            }
            .await;
            let (report, trace, seen) = run.finish(steps.is_ok()).await;
            let error = steps.unwrap_or_else(|error| panic!("case {case}, user {user:?}: {error}"));
            let report = report.unwrap();
            let trace = trace.unwrap();
            assert_eq!(report.state, WorkerState::Ended);
            assert_eq!(report.errors.len(), 1, "case {case}: {:?}", report.errors);
            assert_eq!(error.category, expected_category, "case {case}");
            assert!(
                error.error.contains(expected_error),
                "case {case}: {error:?}"
            );
            assert!(!error.fatal);
            assert!(!seen.iter().any(|frame| frame.frame.id == invalid_id));
            assert!(trace.closed);
            assert_eq!(pcm_messages(&trace), vec![MARKER.to_vec()], "case {case}");
            assert_eq!(trace.wire.len(), 1);
        }
    }
}

#[tokio::test]
async fn manual_mixed_preroll_is_bounded_and_duplicate_boundaries_clear_idle_bytes() {
    let mut run = Run::start(configuration(true), Some(1), true).await;
    let steps = async {
        for frame in [
            audio(None, (0..20).collect()),
            audio(Some("second"), (20..40).collect()),
            Frame::new(FrameData::UserStartedSpeaking),
            Frame::new(FrameData::UserStartedSpeaking),
            audio(Some("active"), vec![44, 0]),
            Frame::new(FrameData::UserStoppedSpeaking),
        ] {
            run.send(frame, Direction::Downstream).await?;
        }
        run.barriers(1).await?;
        for frame in [
            audio(Some("discarded"), vec![77, 0, 78, 0]),
            Frame::new(FrameData::UserStoppedSpeaking),
            Frame::new(FrameData::UserStartedSpeaking),
            audio(None, MARKER.to_vec()),
            Frame::new(FrameData::UserStoppedSpeaking),
            Frame::new(FrameData::UserStoppedSpeaking),
        ] {
            run.send(frame, Direction::Downstream).await?;
        }
        run.barriers(2).await
    }
    .await;
    let (report, trace, _) = run.finish(steps.is_ok()).await;
    steps.unwrap();
    let report = report.unwrap();
    let trace = trace.unwrap();
    assert_clean(&report, &trace);
    assert_eq!(
        pcm_messages(&trace),
        vec![(8..40).collect::<Vec<u8>>(), vec![44, 0], MARKER.to_vec()]
    );
    assert_eq!(trace.wire.len(), 7);
    for index in [0, 4] {
        assert_eq!(
            trace.wire[index],
            json!({"realtimeInput":{"activityStart":{}}})
        );
    }
    for index in [3, 6] {
        assert_eq!(
            trace.wire[index],
            json!({"realtimeInput":{"activityEnd":{}}})
        );
    }
}

#[tokio::test]
async fn zero_preroll_disables_retention_for_both_input_families() {
    let mut run = Run::start(configuration(true), Some(0), true).await;
    let steps = async {
        for frame in [
            audio(Some("idle"), vec![1; 80]),
            audio(None, vec![2; 80]),
            Frame::new(FrameData::UserStartedSpeaking),
            audio(None, MARKER.to_vec()),
            Frame::new(FrameData::UserStoppedSpeaking),
        ] {
            run.send(frame, Direction::Downstream).await?;
        }
        run.barrier().await
    }
    .await;
    let (report, trace, _) = run.finish(steps.is_ok()).await;
    steps.unwrap();
    let report = report.unwrap();
    let trace = trace.unwrap();
    assert_clean(&report, &trace);
    assert_eq!(pcm_messages(&trace), vec![MARKER.to_vec()]);
    assert_eq!(trace.wire.len(), 3);
}

#[tokio::test]
async fn idle_raw_limit_precedes_preroll_truncation_but_idle_context_is_not_admitted() {
    for user in [None, Some("captured")] {
        let mut config = configuration(true);
        config.limits.max_audio_bytes = 32;
        let mut run = Run::start(config, Some(1), true).await;
        let mut idle = AudioRawFrame::new(vec![7; 8], 16_000, 1).unwrap();
        idle.context_id = Some("x".repeat(1025));
        let steps = async {
            run.submit(audio(user, vec![1; 34]), Direction::Downstream)
                .await?;
            let error = run.error().await?;
            run.send(wrapped_audio(user, idle), Direction::Downstream)
                .await?;
            run.send(
                Frame::new(FrameData::UserStartedSpeaking),
                Direction::Downstream,
            )
            .await?;
            run.send(audio(None, MARKER.to_vec()), Direction::Downstream)
                .await?;
            run.send(
                Frame::new(FrameData::UserStoppedSpeaking),
                Direction::Downstream,
            )
            .await?;
            run.barrier().await?;
            Ok::<_, String>(error)
        }
        .await;
        let (report, trace, _) = run.finish(steps.is_ok()).await;
        let error = steps.unwrap();
        let report = report.unwrap();
        let trace = trace.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.errors.len(), 1);
        assert_eq!(error.category, ErrorCategory::Application);
        assert!(error.error.contains("configured limit"));
        assert!(trace.closed);
        assert_eq!(pcm_messages(&trace), vec![vec![7; 8], MARKER.to_vec()]);
        assert_eq!(trace.wire.len(), 4);
    }
}

impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite's handshake callback error type.
    async fn responding(mut config: GeminiConfig, reply: Value) -> (Self, GeminiConfig) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (tcp, _) = timeout(DEADLINE, listener.accept())
                .await
                .map_err(|_| "accept deadline")?
                .map_err(|e| e.to_string())?;
            let mut request_info = None;
            let mut ws = timeout(
                DEADLINE,
                tokio_tungstenite::accept_hdr_async(
                    tcp,
                    |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                     response| {
                        request_info = Some((
                            request.uri().path().to_owned(),
                            request.uri().query().map(str::to_owned),
                            request.headers().contains_key("authorization"),
                        ));
                        Ok(response)
                    },
                ),
            )
            .await
            .map_err(|_| "upgrade deadline")?
            .map_err(|e| e.to_string())?;
            let first = timeout(DEADLINE, ws.next())
                .await
                .map_err(|_| "setup deadline")?
                .ok_or("closed before setup")?
                .map_err(|e| e.to_string())?;
            let setup = serde_json::from_str(first.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            let (path, query, authorization) = request_info.ok_or("missing request")?;
            let mut trace = Trace {
                path,
                query,
                authorization,
                setup,
                wire: vec![],
                closed: false,
            };
            timeout(
                DEADLINE,
                ws.send(Message::Text(
                    json!({"setupComplete":{}}).to_string().into(),
                )),
            )
            .await
            .map_err(|_| "ready deadline")?
            .map_err(|e| e.to_string())?;
            loop {
                let message = timeout(DEADLINE, ws.next())
                    .await
                    .map_err(|_| "peer read deadline")?;
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value =
                            serde_json::from_str(&text).map_err(|e| e.to_string())?;
                        let barrier = value["realtimeInput"].get("audio").is_some()
                            || value["realtimeInput"].get("activityEnd").is_some();
                        trace.wire.push(value);
                        if barrier {
                            let event = reply.clone();
                            timeout(DEADLINE, ws.send(Message::Text(event.to_string().into())))
                                .await
                                .map_err(|_| "barrier write deadline")?
                                .map_err(|e| e.to_string())?;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        timeout(DEADLINE, ws.send(Message::Pong(bytes)))
                            .await
                            .map_err(|_| "pong deadline")?
                            .map_err(|e| e.to_string())?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        trace.closed = true;
                        let _ = timeout(DEADLINE, ws.flush()).await;
                        return Ok(trace);
                    }
                    None => return Ok(trace),
                    Some(Err(tokio_tungstenite::tungstenite::Error::Protocol(
                        tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
                    ))) => return Ok(trace),
                    Some(Err(error)) => return Err(error.to_string()),
                    Some(Ok(other)) => return Err(format!("unexpected peer message: {other:?}")),
                }
            }
        });
        (Self { task }, config)
    }
}
impl Run {
    async fn responding(
        config: GeminiConfig,
        user_id: Option<&str>,
        preroll_ms: Option<u32>,
        reply: Value,
    ) -> Self {
        let (peer, config) = Peer::responding(config, reply).await;
        let rate = config.input_sample_rate;
        let mut processor = GeminiLiveProcessor::new(GeminiClient::new(config).unwrap());
        if let Some(user_id) = user_id {
            processor = processor.with_user_id(user_id);
        }
        if let Some(milliseconds) = preroll_ms {
            processor = processor.with_preroll_ms(milliseconds).unwrap();
        }
        let mut config = PipelineConfig::default();
        config.start.audio_in_sample_rate = rate;
        config.start.audio_out_sample_rate = 24_000;
        match timeout(
            DEADLINE,
            Pipeline::new(vec![Arc::new(processor)]).start(config),
        )
        .await
        {
            Ok(Ok(worker)) => Self {
                worker,
                peer,
                seen: vec![],
            },
            other => {
                let trace = peer.join().await;
                panic!("pipeline setup failed: {:?}; peer: {trace:?}", other.err());
            }
        }
    }
    async fn transcripts(&mut self, expected: usize) -> Result<(), String> {
        while self
            .seen
            .iter()
            .filter(|frame| {
                matches!(
                    frame.frame.data,
                    FrameData::Transcription(_) | FrameData::InterimTranscription(_)
                )
            })
            .count()
            < expected
        {
            self.receive(|frame| {
                matches!(
                    frame.frame.data,
                    FrameData::Transcription(_) | FrameData::InterimTranscription(_)
                )
            })
            .await?;
        }
        Ok(())
    }
}
fn model_reply() -> Value {
    json!({"serverContent":{"modelTurn":{"parts":[{"text":"immediate audio reply"}]},"turnComplete":true}})
}
fn assert_pcm_at_rate(trace: &Trace, rate: u32, bytes: &[Vec<u8>]) {
    let expected: Vec<_> = bytes.iter().map(|bytes| {
        json!({"realtimeInput":{"audio":{"mimeType":format!("audio/pcm;rate={rate}"),"data":STANDARD.encode(bytes)}}})
    }).collect();
    assert_eq!(trace.wire, expected);
}

#[tokio::test]
async fn configured_rates_pass_exact_pcm_without_implicit_resampling() {
    for rate in [8_000, 24_000, 48_000, 96_000] {
        let mut config = configuration(false);
        config.input_sample_rate = rate;
        let mut run = Run::responding(config, None, None, model_reply()).await;
        let bytes = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8]];
        let steps = async {
            for (index, user) in [None, Some("speaker")].into_iter().enumerate() {
                let frame = wrapped_audio(
                    user,
                    AudioRawFrame::new(bytes[index].clone(), rate, 1).unwrap(),
                );
                run.send(frame, Direction::Downstream).await?;
                run.barriers(index + 1).await?;
            }
            Ok::<_, String>(())
        }
        .await;
        let (report, trace, _) = run.finish(steps.is_ok()).await;
        steps.unwrap();
        let report = report.unwrap();
        let trace = trace.unwrap();
        assert_clean(&report, &trace);
        assert_pcm_at_rate(&trace, rate, &bytes);
    }
}

#[tokio::test]
async fn inner_context_overrides_metadata_including_empty_and_output_origin_is_immediate() {
    let mut run = Run::responding(configuration(false), None, None, model_reply()).await;
    let steps = async {
        let mut expected = vec![];
        for user in [None, Some("wire-private-speaker")] {
            for inner in [
                None,
                Some("inner-context".to_owned()),
                Some(String::new()),
                Some("x".repeat(1024)),
            ] {
                let mut pcm = AudioRawFrame::new(vec![1, 2, 3, 4], 16_000, 1).unwrap();
                pcm.context_id = inner.clone();
                let mut frame = wrapped_audio(user, pcm);
                frame
                    .metadata
                    .insert("context_id".into(), json!("metadata-context"));
                let origin = inner.unwrap_or_else(|| "metadata-context".to_owned());
                expected.push((frame.id, origin));
                run.send(frame, Direction::Downstream).await?;
                // No later input/marker can replace this input's origin before its reply.
                run.barriers(expected.len()).await?;
            }
        }
        Ok::<_, String>(expected)
    }
    .await;
    let (report, trace, seen) = run.finish(steps.is_ok()).await;
    let expected = steps.unwrap();
    let report = report.unwrap();
    let trace = trace.unwrap();
    assert_clean(&report, &trace);
    let response_frames: Vec<_> = seen
        .iter()
        .filter(|frame| {
            matches!(
                frame.frame.data,
                FrameData::LlmResponseStart | FrameData::LlmText(_) | FrameData::LlmResponseEnd
            )
        })
        .collect();
    assert_eq!(response_frames.len(), expected.len() * 3);
    for ((id, context), turn) in expected.iter().zip(response_frames.as_chunks::<3>().0) {
        for frame in turn {
            assert_eq!(frame.direction, Direction::Downstream);
            assert_eq!(frame.frame.metadata["source_frame_id"], json!(id));
            assert_eq!(frame.frame.metadata["context_id"], json!(context));
        }
    }
    assert_eq!(pcm_messages(&trace), vec![vec![1, 2, 3, 4]; 8]);
}

#[tokio::test]
async fn both_native_modes_preserve_default_and_fixed_transcript_identity() {
    for transcription in [false, true] {
        for fixed_id in [None, Some("fixed-user")] {
            let mut config = configuration(false);
            if transcription {
                config =
                    GeminiConfig::transcription("family-loopback-key", "family-loopback-model");
            }
            let mut reply = json!({"serverContent":{"inputTranscription":{"text":"heard user","languageCode":"en-US"},"turnComplete":true}});
            if !transcription {
                reply["serverContent"]["modelTurn"] = json!({"parts":[{"text":"reply"}]});
            }
            let mut run = Run::responding(config, fixed_id, None, reply).await;
            let steps = async {
                for (index, user) in [Some("first-speaker"), None, Some("second-speaker")]
                    .into_iter()
                    .enumerate()
                {
                    let mut frame = audio(user, vec![1, 2, 3, 4]);
                    frame
                        .metadata
                        .insert("context_id".into(), json!("must-not-label-transcript"));
                    run.send(frame, Direction::Downstream).await?;
                    run.transcripts(index + 1).await?;
                    if !transcription {
                        run.barriers(index + 1).await?;
                    }
                }
                Ok::<_, String>(())
            }
            .await;
            let (report, trace, seen) = run.finish(steps.is_ok()).await;
            steps.unwrap();
            let report = report.unwrap();
            let trace = trace.unwrap();
            assert_clean(&report, &trace);
            let transcripts: Vec<_> = seen
                .iter()
                .filter_map(|frame| match &frame.frame.data {
                    FrameData::Transcription(text) | FrameData::InterimTranscription(text) => {
                        Some((frame, text))
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(transcripts.len(), 3);
            for (frame, text) in transcripts {
                assert_eq!(frame.direction, Direction::Downstream);
                assert_eq!(text.user_id, fixed_id.unwrap_or(""));
                assert_eq!(text.text, "heard user");
                assert_eq!(text.language.as_deref(), Some("en-US"));
                assert!(!text.timestamp.is_empty());
                assert_eq!(text.finalized, transcription);
                assert_eq!(
                    matches!(frame.frame.data, FrameData::Transcription(_)),
                    transcription
                );
                assert!(!frame.frame.metadata.contains_key("source_frame_id"));
                assert!(!frame.frame.metadata.contains_key("context_id"));
            }
            assert_pcm_at_rate(
                &trace,
                16_000,
                &[vec![1, 2, 3, 4], vec![1, 2, 3, 4], vec![1, 2, 3, 4]],
            );
        }
    }
}

#[tokio::test]
async fn manual_preroll_uses_start_origin_and_idle_context_does_not_leak() {
    let mut run = Run::responding(configuration(true), None, Some(1), model_reply()).await;
    let mut pcm = AudioRawFrame::new(vec![1, 2, 3, 4], 16_000, 1).unwrap();
    pcm.context_id = Some("x".repeat(1025));
    let mut start = Frame::new(FrameData::UserStartedSpeaking);
    start
        .metadata
        .insert("context_id".into(), json!("start-origin"));
    let start_id = start.id;
    let steps = async {
        run.send(
            wrapped_audio(Some("idle-speaker"), pcm),
            Direction::Downstream,
        )
        .await?;
        run.send(start, Direction::Downstream).await?;
        run.barriers(1).await?;
        run.send(
            Frame::new(FrameData::UserStoppedSpeaking),
            Direction::Downstream,
        )
        .await?;
        run.barriers(2).await
    }
    .await;
    let (report, trace, seen) = run.finish(steps.is_ok()).await;
    steps.unwrap();
    let report = report.unwrap();
    let trace = trace.unwrap();
    assert_clean(&report, &trace);
    let first = seen
        .iter()
        .find(|frame| matches!(frame.frame.data, FrameData::LlmText(_)))
        .unwrap();
    assert_eq!(first.frame.metadata["source_frame_id"], json!(start_id));
    assert_eq!(first.frame.metadata["context_id"], json!("start-origin"));
    assert_eq!(pcm_messages(&trace), vec![vec![1, 2, 3, 4]]);
    assert_eq!(trace.wire.len(), 3);
}

#[tokio::test]
async fn cancelling_idle_user_preroll_joins_without_replaying_audio() {
    let mut run = Run::responding(configuration(true), None, Some(1), model_reply()).await;
    let steps = async {
        run.send(
            audio(Some("cancelled-speaker"), vec![1; 40]),
            Direction::Downstream,
        )
        .await?;
        Ok::<_, String>(())
    }
    .await;
    let (report, trace, _) = run.finish(false).await;
    steps.unwrap();
    let report = report.unwrap();
    let trace = trace.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(trace.wire.is_empty());
}

#[tokio::test]
async fn valid_empty_manual_idle_and_active_audio_is_forwarded_without_context_admission() {
    let mut run = Run::start(configuration(true), Some(1), true).await;
    let steps = async {
        let mut preserved = vec![];
        for active in [false, true] {
            if active {
                run.send(
                    Frame::new(FrameData::UserStartedSpeaking),
                    Direction::Downstream,
                )
                .await?;
            }
            for user in [None, Some("empty-speaker")] {
                let mut pcm = AudioRawFrame::new(vec![], 16_000, 1).unwrap();
                pcm.context_id = Some("x".repeat(1025));
                let frame = wrapped_audio(user, pcm);
                let expected = frame.clone();
                let output = run.send(frame, Direction::Downstream).await?;
                preserved.push((expected, output));
            }
        }
        run.send(audio(None, MARKER.to_vec()), Direction::Downstream)
            .await?;
        run.send(
            Frame::new(FrameData::UserStoppedSpeaking),
            Direction::Downstream,
        )
        .await?;
        run.barrier().await?;
        Ok::<_, String>(preserved)
    }
    .await;
    let (report, trace, _) = run.finish(steps.is_ok()).await;
    let preserved = steps.unwrap();
    let report = report.unwrap();
    let trace = trace.unwrap();
    assert_clean(&report, &trace);
    for (expected, output) in preserved {
        assert_eq!(output.frame, expected);
    }
    assert_eq!(pcm_messages(&trace), vec![MARKER.to_vec()]);
    assert_eq!(trace.wire.len(), 3);
}
