//! Segmentation, local resource policy and terminal outcomes on real pipelines.
#[allow(dead_code)]
mod support;
use pipecat_core::*;
use pipecat_speech::{elevenlabs::*, *};
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot};

fn backend(config: SpeechConfig, eleven: bool) -> HttpSttBackend {
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
fn audio(bytes: Vec<u8>) -> FrameData {
    FrameData::InputAudio(AudioRawFrame {
        audio: bytes,
        sample_rate: 16000,
        num_channels: 1,
        context_id: None,
    })
}
fn vad(start: bool) -> FrameData {
    let payload = VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    };
    if start {
        FrameData::VadUserStartedSpeaking(payload)
    } else {
        FrameData::VadUserStoppedSpeaking(payload)
    }
}
struct Driver {
    worker: PipelineWorker,
    frames: Vec<DirectedFrame>,
}
impl Driver {
    async fn new(processor: Arc<HttpSttProcessor>, config: PipelineConfig) -> Self {
        let worker = Pipeline::new(vec![processor]).start(config).await.unwrap();
        Self {
            worker,
            frames: vec![],
        }
    }
    async fn frame(&mut self, data: FrameData, direction: Direction) {
        let frame = Frame::new(data);
        let id = frame.id;
        self.worker.sender().send(frame, direction).await.unwrap();
        self.until(|f| f.frame.id == id).await;
    }
    async fn until(&mut self, p: impl Fn(&DirectedFrame) -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let f = self.worker.recv().await.expect("pipeline open");
                let done = p(&f);
                self.frames.push(f);
                if done {
                    break;
                }
            }
        })
        .await
        .expect("output deadline");
    }
    async fn segment(&mut self) {
        self.frame(vad(true), Direction::Downstream).await;
        self.frame(audio(vec![1, 0, 2, 0]), Direction::Downstream)
            .await;
        self.frame(vad(false), Direction::Downstream).await;
    }
    async fn finish(mut self, terminal: FrameData) -> (Vec<DirectedFrame>, WorkerReport) {
        self.worker
            .sender()
            .send(Frame::new(terminal), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(f) = self.worker.recv().await {
                self.frames.push(f);
            }
            let report = self.worker.join().await.unwrap();
            (self.frames, report)
        })
        .await
        .expect("terminal deadline")
    }
}
fn is_transcript(frame: &DirectedFrame) -> bool {
    matches!(
        frame.frame.data,
        FrameData::Transcription(_) | FrameData::TranscriptionDetailed(_)
    )
}
fn end() -> FrameData {
    FrameData::End(EndFrame::default())
}
async fn idle(eleven: bool, options: HttpSttOptions) -> (TcpListener, Arc<HttpSttProcessor>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::openai("synthetic-private-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let processor = Arc::new(HttpSttProcessor::new(backend(config, eleven), options).unwrap());
    (listener, processor)
}
#[tokio::test]
async fn idle_duplicate_empty_and_unrelated_frames_never_upload() {
    for eleven in [false, true] {
        let (listener, stt) = idle(eleven, HttpSttOptions::default()).await;
        let mut d = Driver::new(stt, PipelineConfig::default()).await;
        d.frame(vad(false), Direction::Downstream).await;
        d.frame(vad(true), Direction::Downstream).await;
        d.frame(audio(vec![]), Direction::Downstream).await;
        d.frame(vad(false), Direction::Downstream).await;
        d.frame(vad(false), Direction::Downstream).await;
        d.frame(audio(vec![0; 32000]), Direction::Downstream).await;
        d.frame(FrameData::UserStoppedSpeaking, Direction::Downstream)
            .await;
        d.frame(
            FrameData::ProposedUserStoppedSpeaking,
            Direction::Downstream,
        )
        .await;
        d.frame(audio(vec![4; 64]), Direction::Upstream).await;
        d.frame(
            FrameData::OutputAudio(AudioRawFrame {
                audio: vec![5; 64],
                sample_rate: 16000,
                num_channels: 1,
                context_id: None,
            }),
            Direction::Downstream,
        )
        .await;
        let (frames, report) = d.finish(end()).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert!(!frames.iter().any(is_transcript));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn empty_provider_text_and_push_empty_policy_are_explicit() {
    for (eleven, push, count) in [(false, false, 0), (false, true, 1), (true, false, 0)] {
        let (config, request, server) = support::server(
            "200 OK",
            "application/json",
            vec![br#"{"text":"  ","words":[]}"#.to_vec()],
            false,
            Duration::ZERO,
        )
        .await;
        let stt = Arc::new(
            HttpSttProcessor::new(
                backend(config, eleven),
                HttpSttOptions {
                    push_empty_transcripts: push,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let mut d = Driver::new(stt, PipelineConfig::default()).await;
        d.segment().await;
        request.await.unwrap();
        let (frames, report) = d.finish(end()).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(frames.iter().filter(|f| is_transcript(f)).count(), count);
        server.await.unwrap();
    }
}
#[tokio::test]
async fn dispatch_usage_is_independent_and_precedes_sanitized_provider_outcome() {
    for eleven in [false, true] {
        for (metrics, usage) in [(false, false), (true, false), (false, true), (true, true)] {
            let (config, request, server) = support::server(
                "429 Test",
                "application/json",
                vec![br#"{"error":"private-body"}"#.to_vec()],
                false,
                Duration::ZERO,
            )
            .await;
            let stt = Arc::new(
                HttpSttProcessor::new(backend(config, eleven), HttpSttOptions::default()).unwrap(),
            );
            let mut pipeline = PipelineConfig::default();
            pipeline.start.enable_metrics = metrics;
            pipeline.start.enable_usage_metrics = usage;
            let mut d = Driver::new(stt, pipeline).await;
            d.segment().await;
            request.await.unwrap();
            let (frames, report) = d.finish(end()).await;
            assert_eq!(report.state, WorkerState::Ended);
            let usage_indices: Vec<_> = frames
                .iter()
                .enumerate()
                .filter_map(|(i, f)| match &f.frame.data {
                    FrameData::Metrics(ms)
                        if ms.iter().any(|m| matches!(m, Metric::SttUsage { .. })) =>
                    {
                        Some(i)
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(usage_indices.len(), usize::from(usage));
            let processing=frames.iter().filter(|f|matches!(&f.frame.data,FrameData::Metrics(ms) if ms.iter().any(|m|matches!(m,Metric::Processing{..})))).count();
            assert_eq!(processing, usize::from(metrics));
            let error = frames
                .iter()
                .position(|f| matches!(f.frame.data, FrameData::Error(_)))
                .unwrap();
            if usage {
                assert!(usage_indices[0] < error);
            }
            assert!(!format!("{report:?}").contains("private-body"));
            server.await.unwrap();
        }
    }
}
#[tokio::test]
async fn status_matrix_never_retries_and_permanence_is_explicit() {
    for eleven in [false, true] {
        for status in [400, 401, 403, 413, 422, 429, 500, 503, 302] {
            let (config, request, server) = support::server(
                &format!("{status} Test"),
                "application/json",
                vec![br#"{"error":"sensitive-sentinel"}"#.to_vec()],
                false,
                Duration::ZERO,
            )
            .await;
            let stt = Arc::new(
                HttpSttProcessor::new(backend(config, eleven), HttpSttOptions::default()).unwrap(),
            );
            let health = stt.health().unwrap();
            let mut d = Driver::new(stt, PipelineConfig::default()).await;
            d.segment().await;
            request.await.unwrap();
            d.until(|f| matches!(f.frame.data, FrameData::Error(_)))
                .await;
            let (frames, report) = d.finish(end()).await;
            assert_eq!(report.state, WorkerState::Ended);
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                    .count(),
                1
            );
            assert!(!frames.iter().any(is_transcript));
            assert_eq!(health.is_usable(), !matches!(status, 400 | 401 | 403 | 422));
            assert!(!format!("{report:?}").contains("sensitive-sentinel"));
            server.await.unwrap();
        }
    }
}
#[tokio::test]
async fn malformed_content_types_bodies_and_text_limits_are_outcomes() {
    for eleven in [false, true] {
        for (mime, body, truncated, limit) in [
            ("text/plain", br#"{"text":"secret"}"#.to_vec(), false, 128),
            ("application/json", b"{\"text\":".to_vec(), false, 128),
            ("application/json", b"{\"text\":".to_vec(), true, 128),
            (
                "application/json",
                br#"{"text":"secret-too-long"}"#.to_vec(),
                false,
                3,
            ),
            (
                "application/json",
                br#"{"text":{"secret":"type"}}"#.to_vec(),
                false,
                128,
            ),
        ] {
            let (config, request, server) =
                support::server("200 OK", mime, vec![body], truncated, Duration::ZERO).await;
            let stt = Arc::new(
                HttpSttProcessor::new(
                    backend(config, eleven),
                    HttpSttOptions {
                        max_transcript_bytes: limit,
                        ..Default::default()
                    },
                )
                .unwrap(),
            );
            let mut d = Driver::new(stt, PipelineConfig::default()).await;
            d.segment().await;
            request.await.unwrap();
            let (frames, report) = d.finish(end()).await;
            assert_eq!(report.state, WorkerState::Ended);
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                    .count(),
                1
            );
            assert!(!frames.iter().any(is_transcript));
            assert!(!format!("{report:?}").contains("secret"));
            server.await.unwrap();
        }
    }
}
#[tokio::test]
async fn end_detailed_and_stop_drain_closed_but_discard_open_audio() {
    for eleven in [false, true] {
        for terminal in [
            FrameData::EndDetailed(ReasonFrame::default()),
            FrameData::Stop,
        ] {
            let (config, request, server) = support::server(
                "200 OK",
                "application/json",
                vec![br#"{"text":"closed","words":[]}"#.to_vec()],
                false,
                Duration::ZERO,
            )
            .await;
            let stt = Arc::new(
                HttpSttProcessor::new(backend(config, eleven), HttpSttOptions::default()).unwrap(),
            );
            let mut d = Driver::new(stt, PipelineConfig::default()).await;
            d.segment().await;
            request.await.unwrap();
            d.frame(vad(true), Direction::Downstream).await;
            d.frame(audio(vec![2; 64]), Direction::Downstream).await;
            let stop = matches!(terminal, FrameData::Stop);
            let (frames, report) = d.finish(terminal).await;
            assert_eq!(
                report.state,
                if stop {
                    WorkerState::Stopped
                } else {
                    WorkerState::Ended
                }
            );
            assert_eq!(frames.iter().filter(|f| is_transcript(f)).count(), 1);
            server.await.unwrap();
        }
    }
}
#[tokio::test]
async fn shutdown_deadline_is_failure_and_drops_stalled_request() {
    for eleven in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::openai("key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        config.timeout = Duration::from_secs(5);
        let (stalled, observed) = oneshot::channel();
        let (release, wait) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            stalled.send(()).unwrap();
            let _ = wait.await;
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\n{\"text\":\"x\"}")
                .await;
        });
        let stt = Arc::new(
            HttpSttProcessor::new(backend(config, eleven), HttpSttOptions::default()).unwrap(),
        );
        let pipeline = PipelineConfig {
            shutdown_timeout: Duration::from_millis(30),
            ..Default::default()
        };
        let mut d = Driver::new(stt, pipeline).await;
        d.segment().await;
        observed.await.unwrap();
        d.worker.end().await.unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), async {
            while d.worker.recv().await.is_some() {}
            d.worker.join().await
        })
        .await
        .unwrap();
        assert!(
            outcome.is_err(),
            "shutdown timeout cannot be successful End"
        );
        drop(release);
        server.await.unwrap();
    }
}
#[tokio::test]
async fn single_use_cleanup_idempotence_and_configuration_limits() {
    let (listener, stt) = idle(false, HttpSttOptions::default()).await;
    let d = Driver::new(stt.clone(), PipelineConfig::default()).await;
    let (_, report) = d.finish(end()).await;
    assert_eq!(report.state, WorkerState::Ended);
    stt.cleanup().await.unwrap();
    stt.cleanup().await.unwrap();
    assert_setup_failed(
        Pipeline::new(vec![stt])
            .start(PipelineConfig::default())
            .await,
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    for options in [
        HttpSttOptions {
            queue_capacity: 0,
            ..Default::default()
        },
        HttpSttOptions {
            queue_capacity: 17,
            ..Default::default()
        },
        HttpSttOptions {
            max_segment_duration: Duration::MAX,
            ..Default::default()
        },
        HttpSttOptions {
            trailing_silence: Duration::MAX,
            ..Default::default()
        },
        HttpSttOptions {
            sample_rate: Some(u32::MAX),
            ..Default::default()
        },
        HttpSttOptions {
            max_frame_bytes: usize::MAX,
            ..Default::default()
        },
        HttpSttOptions {
            max_segment_age: Duration::MAX,
            ..Default::default()
        },
    ] {
        assert!(
            HttpSttProcessor::new(backend(SpeechConfig::openai("key"), false), options).is_err()
        );
    }
}

struct CountText(Arc<std::sync::atomic::AtomicUsize>);
#[async_trait]
impl FrameProcessor for CountText {
    fn name(&self) -> &str {
        "stt-output-capacity-probe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn full_output_capacity_fails_explicitly_without_detached_http_work() {
    for eleven in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::openai("key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let (request, observed) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            request.send(()).unwrap();
            wait.await.unwrap();
            let _=socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 12\r\nConnection: close\r\n\r\n{\"text\":\"x\"}").await;
        });
        let stt = Arc::new(
            HttpSttProcessor::new(backend(config, eleven), HttpSttOptions::default()).unwrap(),
        );
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cfg = PipelineConfig {
            output_capacity: 8,
            ..Default::default()
        };
        let worker = Pipeline::new(vec![stt, Arc::new(CountText(count.clone()))])
            .start(cfg)
            .await
            .unwrap();
        let mut d = Driver {
            worker,
            frames: vec![],
        };
        d.segment().await;
        observed.await.unwrap();
        // A normal-frame marker drains startup metadata and all earlier output.
        d.frame(
            FrameData::Text(TextFrame::new("barrier")),
            Direction::Downstream,
        )
        .await;
        for _ in 0..8 {
            d.worker
                .sender()
                .send(Frame::text("fill"), Direction::Downstream)
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while count.load(std::sync::atomic::Ordering::SeqCst) < 9 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release.send(()).unwrap();
        let mut state = d.worker.sender().watch();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if state.borrow().is_finished() {
                    break;
                }
                state.changed().await.unwrap();
            }
        })
        .await
        .expect("bounded backpressure failure");
        assert_eq!(d.worker.state(), WorkerState::Failed);
        while d.worker.recv().await.is_some() {}
        assert!(d.worker.join().await.is_err());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn each_geometry_and_frame_bound_rejects_entire_segment_without_http() {
    for eleven in [false, true] {
        for case in 0..6 {
            let (listener, stt) = idle(
                eleven,
                HttpSttOptions {
                    max_frame_bytes: 64,
                    ..Default::default()
                },
            )
            .await;
            let mut d = Driver::new(stt, PipelineConfig::default()).await;
            d.frame(vad(true), Direction::Downstream).await;
            d.frame(audio(vec![1; 4]), Direction::Downstream).await;
            let mut raw = AudioRawFrame {
                audio: vec![2; 4],
                sample_rate: 16000,
                num_channels: 1,
                context_id: None,
            };
            match case {
                0 => raw.audio.push(1),
                1 => raw.sample_rate = 8000,
                2 => raw.num_channels = 0,
                3 => raw.num_channels = 2,
                4 => raw.audio = vec![2; 66],
                _ => {}
            }
            let invalid = if case == 5 {
                FrameData::UserAudio(UserAudioRawFrame {
                    audio: raw,
                    user_id: "u".repeat(257),
                })
            } else {
                FrameData::InputAudio(raw)
            };
            d.frame(invalid, Direction::Downstream).await;
            d.frame(audio(vec![3; 4]), Direction::Downstream).await;
            d.frame(vad(false), Direction::Downstream).await;
            let (frames, report) = d.finish(end()).await;
            assert_eq!(report.state, WorkerState::Ended);
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                    .count(),
                1
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(20), listener.accept())
                    .await
                    .is_err()
            );
        }
    }
    let (listener, stt) = idle(
        false,
        HttpSttOptions {
            sample_rate: Some(16000),
            ..Default::default()
        },
    )
    .await;
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 8000;
    assert_setup_failed(Pipeline::new(vec![stt]).start(config).await).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn exact_padded_duration_is_accepted_and_one_sample_over_is_not_truncated() {
    for eleven in [false, true] {
        let (config, request, server) = support::server(
            "200 OK",
            "application/json",
            vec![br#"{"text":"boundary","words":[]}"#.to_vec()],
            false,
            Duration::ZERO,
        )
        .await;
        let options = HttpSttOptions {
            max_segment_duration: Duration::from_millis(1500),
            ..Default::default()
        };
        let stt =
            Arc::new(HttpSttProcessor::new(backend(config, eleven), options.clone()).unwrap());
        let mut d = Driver::new(stt, PipelineConfig::default()).await;
        d.frame(vad(true), Direction::Downstream).await;
        d.frame(audio(vec![1; 32000]), Direction::Downstream).await;
        d.frame(vad(false), Direction::Downstream).await;
        let captured = request.await.unwrap();
        let position = captured.body.windows(4).position(|b| b == b"RIFF").unwrap();
        let wav =
            pipecat_audio::read_pcm16_wav(&captured.body[position..position + 48044]).unwrap();
        assert_eq!(wav.pcm.len(), 48000);
        assert_eq!(&wav.pcm[..32000], vec![1; 32000]);
        assert!(wav.pcm[32000..].iter().all(|b| *b == 0));
        let (frames, report) = d.finish(end()).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(frames.iter().filter(|f| is_transcript(f)).count(), 1);
        server.await.unwrap();
        let (listener, stt) = idle(eleven, options).await;
        let mut d = Driver::new(stt, PipelineConfig::default()).await;
        d.frame(vad(true), Direction::Downstream).await;
        d.frame(audio(vec![1; 32002]), Direction::Downstream).await;
        d.frame(vad(false), Direction::Downstream).await;
        let (frames, report) = d.finish(end()).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                .count(),
            1
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn fractional_padding_floors_samples_and_counts_pcm_not_wav() {
    for eleven in [false, true] {
        let (config, request, server) = support::server(
            "200 OK",
            "application/json",
            vec![br#"{"text":"fraction","words":[]}"#.to_vec()],
            false,
            Duration::ZERO,
        )
        .await;
        let stt = Arc::new(
            HttpSttProcessor::new(
                backend(config, eleven),
                HttpSttOptions {
                    trailing_silence: Duration::from_nanos(156_250),
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let mut cfg = PipelineConfig::default();
        cfg.start.enable_usage_metrics = true;
        let mut d = Driver::new(stt, cfg).await;
        d.segment().await;
        let request = request.await.unwrap();
        let start = request.body.windows(4).position(|b| b == b"RIFF").unwrap();
        let wav = pipecat_audio::read_pcm16_wav(&request.body[start..start + 52]).unwrap();
        assert_eq!(wav.pcm, [1, 0, 2, 0, 0, 0, 0, 0]);
        let (frames, report) = d.finish(end()).await;
        assert_eq!(report.state, WorkerState::Ended);
        let usages: Vec<_> = frames
            .iter()
            .filter_map(|f| match &f.frame.data {
                FrameData::Metrics(ms) => Some(ms),
                _ => None,
            })
            .flatten()
            .filter_map(|m| match m {
                Metric::SttUsage { audio_seconds, .. } => Some(*audio_seconds),
                _ => None,
            })
            .collect();
        assert_eq!(usages, [8.0 / 32000.0]);
        server.await.unwrap();
    }
}
#[tokio::test]
async fn typed_shadowed_malformed_extras_are_ignored_and_open_segment_uses_close_snapshot() {
    for eleven in [false, true] {
        let (config, request, server) = support::server(
            "200 OK",
            "application/json",
            vec![br#"{"text":"settings","words":[]}"#.to_vec()],
            false,
            Duration::ZERO,
        )
        .await;
        let stt = Arc::new(
            HttpSttProcessor::new(backend(config, eleven), HttpSttOptions::default()).unwrap(),
        );
        let mut d = Driver::new(stt, PipelineConfig::default()).await;
        d.frame(vad(true), Direction::Downstream).await;
        d.frame(audio(vec![1, 0, 2, 0]), Direction::Downstream)
            .await;
        let delta = SttSettings {
            base: ServiceSettings {
                model: Setting::Value(
                    if eleven {
                        "scribe_v2"
                    } else {
                        "gpt-transcribe"
                    }
                    .into(),
                ),
                extra: [
                    ("model_id".into(), serde_json::json!({"invalid":"ignored"})),
                    ("language".into(), serde_json::json!({"bad":"ignored"})),
                    ("language_code".into(), serde_json::json!([false])),
                ]
                .into_iter()
                .collect(),
            },
            language: Setting::Value("fr-FR".into()),
        };
        d.worker
            .sender()
            .send(
                Frame::new(FrameData::SttUpdateSettings(Box::new(
                    ServiceUpdateSettingsFrame {
                        settings: [("unknown_legacy".into(), true.into())]
                            .into_iter()
                            .collect(),
                        delta: Some(delta),
                        ..Default::default()
                    },
                ))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        d.frame(
            FrameData::Text(TextFrame::new("update barrier")),
            Direction::Downstream,
        )
        .await;
        d.frame(vad(false), Direction::Downstream).await;
        let request = request.await.unwrap();
        let wire = String::from_utf8_lossy(&request.body);
        assert!(wire.contains(if eleven {
            "name=\"language_code\"\r\n\r\nfra\r\n"
        } else {
            "name=\"language\"\r\n\r\nfr\r\n"
        }));
        let (frames, report) = d.finish(end()).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::Error(_))));
        server.await.unwrap();
    }
}

async fn assert_setup_failed(result: Result<PipelineWorker, PipelineError>) {
    if let Ok(mut worker) = result {
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut error = false;
            while let Some(frame) = worker.recv().await {
                error |= matches!(frame.frame.data, FrameData::Error(_));
            }
            assert!(error, "setup rejection must be observable");
            assert_eq!(worker.state(), WorkerState::Failed);
            assert!(
                worker
                    .sender()
                    .send(Frame::new(audio(vec![1, 0])), Direction::Downstream)
                    .await
                    .is_err(),
                "failed setup cannot admit later audio"
            );
            assert!(worker.join().await.is_err());
        })
        .await
        .expect("failed setup must terminate");
    }
}
