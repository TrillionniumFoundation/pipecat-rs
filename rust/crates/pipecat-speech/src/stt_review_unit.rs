//! Publication-barrier review tests use real HTTP outcomes and owned pipelines.
use super::*;
use pipecat_core::{
    Pipeline, PipelineConfig, PipelineWorker, UserAudioRawFrame, VadSpeakingFrame, WorkerState,
};
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

const DEADLINE: Duration = Duration::from_secs(3);
async fn seen(
    worker: &mut PipelineWorker,
    frame: Frame,
    frames: &mut Vec<pipecat_core::DirectedFrame>,
) {
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    loop {
        let f = tokio::time::timeout(DEADLINE, worker.recv())
            .await
            .unwrap()
            .unwrap();
        let found = f.frame.id == id;
        frames.push(f);
        if found {
            return;
        }
    }
}
fn boundary(start: bool) -> Frame {
    let event = VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    };
    Frame::new(if start {
        FrameData::VadUserStartedSpeaking(event)
    } else {
        FrameData::VadUserStoppedSpeaking(event)
    })
}
fn pcm(sample: u8, user: &str) -> Frame {
    Frame::new(FrameData::UserAudio(UserAudioRawFrame {
        audio: AudioRawFrame {
            audio: vec![sample; 32],
            sample_rate: 16000,
            num_channels: 1,
            context_id: None,
        },
        user_id: user.into(),
    }))
}
async fn segment(
    worker: &mut PipelineWorker,
    sample: u8,
    user: &str,
    frames: &mut Vec<pipecat_core::DirectedFrame>,
) {
    seen(worker, boundary(true), frames).await;
    seen(worker, pcm(sample, user), frames).await;
    seen(worker, boundary(false), frames).await;
}
async fn peer(listener: TcpListener, outcomes: Vec<(u16, Vec<u8>)>) {
    for (status, body) in outcomes {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![];
        let header_end = loop {
            let mut chunk = [0; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            assert_ne!(n, 0);
            request.extend_from_slice(&chunk[..n]);
            if let Some(n) = request.windows(4).position(|p| p == b"\r\n\r\n") {
                break n + 4;
            }
        };
        let length: usize = std::str::from_utf8(&request[..header_end])
            .unwrap()
            .lines()
            .filter_map(|l| l.split_once(':'))
            .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .unwrap()
            .1
            .trim()
            .parse()
            .unwrap();
        while request.len() < header_end + length {
            let mut chunk = [0; 4096];
            let n = socket.read(&mut chunk).await.unwrap();
            assert_ne!(n, 0);
            request.extend_from_slice(&chunk[..n]);
        }
        let head=format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len());
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
    }
}
fn backend(base: String, eleven: bool) -> HttpSttBackend {
    let mut config = if eleven {
        SpeechConfig::elevenlabs("synthetic-key")
    } else {
        SpeechConfig::openai("synthetic-key")
    };
    config.base_url = base;
    if eleven {
        HttpSttBackend::ElevenLabs {
            client: ElevenLabsHttpClient::new(config).unwrap(),
            request: ElevenLabsTranscriptionRequest::default(),
        }
    } else {
        HttpSttBackend::OpenAi {
            client: SpeechClient::new(config).unwrap(),
            request: TranscriptionRequest::default(),
        }
    }
}
fn text(f: &pipecat_core::DirectedFrame) -> Option<&str> {
    match &f.frame.data {
        FrameData::Transcription(t) => Some(&t.text),
        FrameData::TranscriptionDetailed(t) => Some(&t.text.text),
        _ => None,
    }
}
async fn complete(
    mut worker: PipelineWorker,
    mut frames: Vec<pipecat_core::DirectedFrame>,
) -> Vec<pipecat_core::DirectedFrame> {
    worker.end().await.unwrap();
    tokio::time::timeout(DEADLINE, async {
        while let Some(f) = worker.recv().await {
            frames.push(f);
        }
        assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
        frames
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn review_parsed_outcome_cannot_publish_after_mute_then_unmute() {
    for eleven in [false, true] {
        for status in [200, 401, 422] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let old = if status == 422 {
                br#"{"text":{"sensitive":"malformed-old"}}"#.to_vec()
            } else {
                serde_json::to_vec(&json!({"text":"revoked-old","words":[]})).unwrap()
            };
            let server = tokio::spawn(peer(
                listener,
                vec![
                    (if status == 422 { 200 } else { status }, old),
                    (
                        200,
                        serde_json::to_vec(&json!({"text":"fresh-new","words":[]})).unwrap(),
                    ),
                ],
            ));
            let stt = Arc::new(
                HttpSttProcessor::new(
                    backend(base, eleven),
                    HttpSttOptions {
                        trailing_silence: Duration::ZERO,
                        ..Default::default()
                    },
                )
                .unwrap(),
            );
            let gate = Arc::new(TestPublicationGate {
                reached: Notify::new(),
                release: Notify::new(),
            });
            stt.state.lock().unwrap().publication_gate = Some(gate.clone());
            let mut config = PipelineConfig::default();
            config.start.enable_metrics = true;
            config.start.enable_usage_metrics = true;
            let mut worker = Pipeline::new(vec![stt.clone()])
                .start(config)
                .await
                .unwrap();
            let mut frames = vec![];
            segment(&mut worker, 1, "revoked-user", &mut frames).await;
            tokio::time::timeout(DEADLINE, gate.reached.notified())
                .await
                .expect("HTTP outcome parsed before revocation");
            stt.state.lock().unwrap().publication_gate = None;
            worker
                .sender()
                .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
                .await
                .unwrap();
            seen(&mut worker, boundary(false), &mut frames).await;
            worker
                .sender()
                .send(Frame::new(FrameData::SttMute(false)), Direction::Downstream)
                .await
                .unwrap();
            seen(&mut worker, boundary(false), &mut frames).await;
            segment(&mut worker, 2, "fresh-user", &mut frames).await;
            gate.release.notify_one();
            loop {
                let f = tokio::time::timeout(DEADLINE, worker.recv())
                    .await
                    .unwrap()
                    .unwrap();
                let done = text(&f) == Some("fresh-new");
                frames.push(f);
                if done {
                    break;
                }
            }
            let frames = complete(worker, frames).await;
            server.await.unwrap();
            assert_eq!(
                frames.iter().filter_map(text).collect::<Vec<_>>(),
                ["fresh-new"],
                "a parsed old response regained authority after unmute"
            );
            assert!(
                !frames
                    .iter()
                    .any(|f| matches!(f.frame.data, FrameData::Error(_))),
                "revoked outcome admitted an error"
            );
            assert!(
                stt.health.is_usable(),
                "old permanent outcome poisoned a new privacy epoch"
            );
            let processing = frames
                .iter()
                .filter_map(|f| match &f.frame.data {
                    FrameData::Metrics(m) => Some(m),
                    _ => None,
                })
                .flatten()
                .filter(|m| matches!(m, Metric::Processing { .. }))
                .count();
            assert_eq!(processing, 1, "revoked work admitted processing output");
        }
    }
}

#[tokio::test]
async fn review_mute_remains_effective_while_prepare_end_drains_parsed_result() {
    for eleven in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(peer(
            listener,
            vec![(
                200,
                serde_json::to_vec(&json!({"text":"old-draining","words":[]})).unwrap(),
            )],
        ));
        let stt = Arc::new(
            HttpSttProcessor::new(
                backend(base, eleven),
                HttpSttOptions {
                    trailing_silence: Duration::ZERO,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let gate = Arc::new(TestPublicationGate {
            reached: Notify::new(),
            release: Notify::new(),
        });
        stt.state.lock().unwrap().publication_gate = Some(gate.clone());
        let mut worker = Pipeline::new(vec![stt.clone(), Arc::new(MuteOnInterruption)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let mut frames = vec![];
        segment(&mut worker, 1, "old-user", &mut frames).await;
        tokio::time::timeout(DEADLINE, gate.reached.notified())
            .await
            .unwrap();
        worker.end().await.unwrap();
        tokio::time::timeout(DEADLINE, async {
            loop {
                if stt.state.lock().unwrap().closing {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            worker
                .sender()
                .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
                .await
                .is_err(),
            "external nonadministrative input is closed after End"
        );
        worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .expect("administrative interruption remains accepted while draining");
        tokio::time::timeout(DEADLINE, async {
            loop {
                if stt.state.lock().unwrap().muted {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("internal upstream mute must be processed during drain");
        gate.release.notify_one();
        tokio::time::timeout(DEADLINE, async {
            while let Some(f) = worker.recv().await {
                frames.push(f);
            }
            assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
        })
        .await
        .unwrap();
        server.await.unwrap();
        assert!(
            frames.iter().filter_map(text).next().is_none(),
            "prepare_end incorrectly revoked mute authority"
        );
    }
}

struct HoldTranscript {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl FrameProcessor for HoldTranscript {
    fn name(&self) -> &str {
        "review-hold-admitted-transcript"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::Transcription(_) | FrameData::TranscriptionDetailed(_)
        ) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn review_mute_does_not_claim_to_retract_already_admitted_downstream_effects() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(peer(
        listener,
        vec![(
            200,
            serde_json::to_vec(&json!({"text":"already-admitted"})).unwrap(),
        )],
    ));
    let stt = Arc::new(
        HttpSttProcessor::new(
            backend(base, false),
            HttpSttOptions {
                trailing_silence: Duration::ZERO,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut worker = Pipeline::new(vec![
        stt.clone(),
        Arc::new(HoldTranscript {
            entered: entered.clone(),
            release: release.clone(),
        }),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    let mut frames = vec![];
    segment(&mut worker, 1, "u", &mut frames).await;
    tokio::time::timeout(DEADLINE, entered.notified())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
        .await
        .unwrap();
    seen(&mut worker, boundary(false), &mut frames).await;
    release.notify_one();
    let frames = complete(worker, frames).await;
    server.await.unwrap();
    assert_eq!(
        frames.iter().filter_map(text).collect::<Vec<_>>(),
        ["already-admitted"]
    );
}

struct MuteOnInterruption;
#[async_trait]
impl FrameProcessor for MuteOnInterruption {
    fn name(&self) -> &str {
        "review-internal-mute-control"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Interruption) {
            ctx.push(Frame::new(FrameData::SttMute(true)), Direction::Upstream)
                .await?;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn review_parsed_outcome_is_fenced_by_full_health_restoration_generation() {
    for eleven in [false, true] {
        for status in [200, 401] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(peer(
                listener,
                vec![(
                    status,
                    serde_json::to_vec(&json!({"text":"stale-health","words":[]})).unwrap(),
                )],
            ));
            let stt = Arc::new(
                HttpSttProcessor::new(
                    backend(base, eleven),
                    HttpSttOptions {
                        trailing_silence: Duration::ZERO,
                        ..Default::default()
                    },
                )
                .unwrap(),
            );
            let gate = Arc::new(TestPublicationGate {
                reached: Notify::new(),
                release: Notify::new(),
            });
            stt.state.lock().unwrap().publication_gate = Some(gate.clone());
            let mut worker = Pipeline::new(vec![stt.clone()])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            let mut frames = vec![];
            segment(&mut worker, 1, "old", &mut frames).await;
            tokio::time::timeout(DEADLINE, gate.reached.notified())
                .await
                .unwrap();
            let old = stt.health.generation();
            assert!(!stt.health.set_usable(true));
            assert_eq!(old.snapshot, stt.health.generation().snapshot);
            assert_ne!(old, stt.health.generation());
            gate.release.notify_one();
            let frames = complete(worker, frames).await;
            server.await.unwrap();
            assert!(frames.iter().filter_map(text).next().is_none());
            assert!(stt.health.is_usable());
            assert!(!frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::Error(_))));
        }
    }
}

#[tokio::test]
async fn review_publication_output_backpressure_fails_instead_of_successful_lost_result() {
    for eleven in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(peer(
            listener,
            vec![(
                200,
                serde_json::to_vec(&json!({"text":"must-not-be-silently-lost","words":[]}))
                    .unwrap(),
            )],
        ));
        let stt = Arc::new(
            HttpSttProcessor::new(
                backend(base, eleven),
                HttpSttOptions {
                    trailing_silence: Duration::ZERO,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let gate = Arc::new(TestPublicationGate {
            reached: Notify::new(),
            release: Notify::new(),
        });
        stt.state.lock().unwrap().publication_gate = Some(gate.clone());
        let config = PipelineConfig {
            output_capacity: 4,
            ..Default::default()
        };
        let mut worker = Pipeline::new(vec![stt.clone()])
            .start(config)
            .await
            .unwrap();
        let mut frames = vec![];
        segment(&mut worker, 1, "u", &mut frames).await;
        tokio::time::timeout(DEADLINE, gate.reached.notified())
            .await
            .unwrap();
        while let Ok(f) = worker.try_recv() {
            frames.push(f);
        }
        // The retained native setup context is not needed: each marker traverses
        // the real processor. A fifth marker would itself fail before publication.
        for index in 0..4 {
            worker
                .sender()
                .send(Frame::text(format!("fill-{index}")), Direction::Downstream)
                .await
                .unwrap();
        }
        // A system-lane marker cannot prove normal-lane progress. Observe the
        // bounded output queue through four pending normal frames indirectly by
        // letting the pipeline process an End behind those exact normal markers.
        worker.end().await.unwrap();
        tokio::time::timeout(DEADLINE, async {
            loop {
                if stt.state.lock().unwrap().closing {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        gate.release.notify_one();
        let report = tokio::time::timeout(DEADLINE, worker.join()).await.unwrap();
        assert!(
            matches!(report, Err(pipecat_core::PipelineError::Backpressure(_))),
            "full output must be an explicit pipeline failure"
        );
        assert_eq!(worker.state(), WorkerState::Failed);
        server.await.unwrap();
    }
}
