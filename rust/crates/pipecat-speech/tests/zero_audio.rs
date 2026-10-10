//! Source completed-context watchdog over real local HTTP responses.
#[allow(dead_code)]
mod support;
use pipecat_core::*;
use pipecat_speech::{cartesia::*, elevenlabs::*, *};
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncWriteExt, net::TcpListener};

async fn fixture(
    replies: Vec<(&'static str, &'static str, &'static [u8])>,
) -> (SpeechConfig, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::openai("test-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.timeout = Duration::from_secs(2);
    let server = tokio::spawn(async move {
        for (status, mime, body) in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
            socket.write_all(body).await.unwrap();
        }
    });
    (config, server)
}
fn silent() -> (&'static str, &'static str, &'static [u8]) {
    ("200 OK", "audio/pcm", b"")
}
fn audio() -> (&'static str, &'static str, &'static [u8]) {
    ("200 OK", "audio/pcm", b"\x01\x02")
}
fn processor(config: SpeechConfig) -> HttpTtsProcessor {
    HttpTtsProcessor::new(
        HttpTtsBackend::OpenAi {
            client: SpeechClient::new(config).unwrap(),
            request: SynthesisRequest::new(""),
        },
        16,
    )
    .unwrap()
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("operation stalled")
}
async fn utterance(worker: &mut PipelineWorker, text: &str) -> Vec<DirectedFrame> {
    let end = Frame::new(FrameData::LlmResponseEnd);
    let id = end.id;
    for frame in [
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new(text))),
        end,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    let mut frames = vec![];
    loop {
        let frame = bounded(worker.recv()).await.unwrap();
        let done = frame.frame.id == id;
        frames.push(frame);
        if done {
            return frames;
        }
    }
}
fn errors(frames: &[DirectedFrame]) -> Vec<&Frame> {
    frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
        .map(|f| &f.frame)
        .collect()
}
fn assert_no_playout(frames: &[DirectedFrame]) {
    assert!(!frames.iter().any(|f| matches!(
        f.frame.data,
        FrameData::TtsAudio(_)
            | FrameData::TtsText(_)
            | FrameData::BotStartedSpeaking
            | FrameData::BotStoppedSpeaking
    )));
    assert!(!frames.iter().any(|f| matches!(&f.frame.data, FrameData::TtsAlignmentDiagnostic(d) if d.evidence != TtsAlignmentEvidence::Unreported)));
}

#[tokio::test]
async fn default_threshold_reports_each_context_once_then_skips_and_recovers() {
    let (config, server) = fixture(vec![silent(); 4]).await;
    let mut worker = Pipeline::new(vec![Arc::new(processor(config))])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    for index in 0..3 {
        let frames = utterance(&mut worker, "silent").await;
        assert_no_playout(&frames);
        let errors = errors(&frames);
        assert_eq!(errors.len(), 1);
        assert_eq!(
            errors[0].error_origin().unwrap().reported_health.is_usable,
            index < 2
        );
        assert_eq!(health.is_usable(), index < 2);
        assert_eq!(
            frames.last().unwrap().frame.metadata.get("failed"),
            Some(&serde_json::json!(true))
        );
    }
    assert!(errors(&utterance(&mut worker, "unusable skipped").await).is_empty());
    health.set_usable(true);
    assert_eq!(
        errors(&utterance(&mut worker, "fresh silence").await).len(),
        1
    );
    assert!(health.is_usable());
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.total_errors, 4);
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn zero_cutoff_keeps_per_context_errors_and_audio_resets_a_streak() {
    for maximum in [0, 2] {
        let (config, server) = fixture(vec![silent(), audio(), silent(), silent()]).await;
        let mut worker = Pipeline::new(vec![Arc::new(
            processor(config).with_max_consecutive_zero_audio_contexts(maximum),
        )])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let health = worker.processor_health(0).unwrap();
        for i in 0..4 {
            let frames = utterance(&mut worker, "test").await;
            assert_eq!(errors(&frames).len(), usize::from(i != 1));
            assert_eq!(health.is_usable(), maximum == 0 || i < 3);
        }
        worker.end().await.unwrap();
        assert_eq!(bounded(worker.collect()).await.unwrap().1.total_errors, 3);
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn explicit_true_resets_usable_streak_but_noop_and_invalid_settings_do_not() {
    let (config, server) = fixture(vec![silent(); 3]).await;
    let processor = Arc::new(processor(config).with_max_consecutive_zero_audio_contexts(2));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    utterance(&mut worker, "one").await;
    assert!(!health.set_usable(true));
    utterance(&mut worker, "fresh one").await;
    assert!(health.is_usable());
    assert!(!processor.update_settings(&TtsSettings::default()).unwrap());
    assert!(processor
        .update_settings(&TtsSettings {
            voice: Setting::Null,
            ..Default::default()
        })
        .is_err());
    utterance(&mut worker, "fresh two").await;
    assert!(!health.is_usable());
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn source_empty_payload_presence_is_distinct_from_playable_pcm() {
    // Cartesia emits a source audio frame for an empty successful body;
    // ElevenLabs HTTP emits one for an explicit audio_base64 key, but not for
    // an absent audio field. The public clients still reject empty PCM streams.
    for variant in 0..3 {
        let reply = if variant == 0 {
            silent()
        } else if variant == 1 {
            (
                "200 OK",
                "application/json",
                &b"{\"audio_base64\":\"\"}\n"[..],
            )
        } else {
            ("200 OK", "application/json", &b""[..])
        };
        let (config, server) = fixture(vec![reply]).await;
        let backend = if variant == 0 {
            HttpTtsBackend::Cartesia {
                client: CartesiaHttpClient::new(config).unwrap(),
                request: CartesiaSynthesisRequest::new("", "voice"),
            }
        } else {
            HttpTtsBackend::ElevenLabs {
                client: ElevenLabsHttpClient::new(config).unwrap(),
                request: ElevenLabsSynthesisRequest::new("", "voice"),
            }
        };
        let processor = HttpTtsProcessor::new(backend, 4)
            .unwrap()
            .with_max_consecutive_zero_audio_contexts(1);
        let mut worker = Pipeline::new(vec![Arc::new(processor)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let frames = utterance(&mut worker, "silent").await;
        assert_no_playout(&frames);
        assert_eq!(errors(&frames).len(), usize::from(variant == 2));
        assert_eq!(
            worker.processor_health(0).unwrap().is_usable(),
            variant != 2
        );
        worker.end().await.unwrap();
        bounded(worker.collect()).await.unwrap();
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn recoverable_provider_errors_also_complete_silent_contexts() {
    let (config, server) = fixture(vec![("503 Unavailable", "application/json", b"{}"); 2]).await;
    let mut worker = Pipeline::new(vec![Arc::new(
        processor(config).with_max_consecutive_zero_audio_contexts(2),
    )])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    for i in 0..2 {
        let frames = utterance(&mut worker, "failed").await;
        assert_eq!(errors(&frames).len(), 2);
        assert_eq!(worker.processor_health(0).unwrap().is_usable(), i == 0);
    }
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn permanent_zero_outcome_obeys_continue_end_and_cancel_policies() {
    for policy in [
        ProcessorUnusablePolicy::Continue,
        ProcessorUnusablePolicy::End,
        ProcessorUnusablePolicy::Cancel,
    ] {
        let (config, server) = fixture(vec![silent()]).await;
        let mut worker = Pipeline::new(vec![Arc::new(
            processor(config).with_max_consecutive_zero_audio_contexts(1),
        )])
        .start(PipelineConfig {
            processor_unusable_policy: policy,
            ..Default::default()
        })
        .await
        .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("silent"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        if policy == ProcessorUnusablePolicy::Continue {
            loop {
                if matches!(
                    bounded(worker.recv()).await.unwrap().frame.data,
                    FrameData::Error(_)
                ) {
                    break;
                }
            }
            worker.end().await.unwrap();
        }
        let (frames, report) = bounded(worker.collect()).await.unwrap();
        assert_eq!(
            report.state,
            if policy == ProcessorUnusablePolicy::Cancel {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
        assert_eq!(report.total_errors, 1);
        if policy == ProcessorUnusablePolicy::Cancel {
            assert!(!frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::End(_))));
        }
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn zero_audio_cutoff_fails_over_without_replay_before_parent_cancel_policy() {
    use pipecat_pipeline::{ServiceSwitcher, ServiceSwitcherStrategy};
    let (primary, primary_server) = fixture(vec![silent(); 3]).await;
    let (backup, backup_server) = fixture(vec![audio()]).await;
    let primary = Arc::new(processor(primary));
    let primary_health = primary.health().unwrap();
    let backup = Arc::new(processor(backup));
    let switcher = Arc::new(
        ServiceSwitcher::with_strategy(vec![primary, backup], ServiceSwitcherStrategy::Failover)
            .unwrap(),
    );
    let mut selected = switcher.watch_selection();
    let mut worker = Pipeline::new(vec![switcher.clone()])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..Default::default()
        })
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            errors(&utterance(&mut worker, "primary silent").await).len(),
            1
        );
        assert_eq!(switcher.active_index(), 0);
    }
    // A service switch can retire the old context's trailing boundary. Wait on
    // the switcher's explicit selection fence before submitting fresh work.
    worker
        .sender()
        .send(
            Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("third silence"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    bounded(async {
        while selected.borrow().active_index != 1 {
            selected.changed().await.unwrap();
        }
    })
    .await;
    assert!(!primary_health.is_usable());
    assert!(switcher.is_usable());
    assert!(worker.unusable_processors().is_empty());
    let frames = utterance(&mut worker, "fresh backup work").await;
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsAudio(_)))
            .count(),
        1
    );
    assert_eq!(switcher.active_index(), 1);
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    bounded(primary_server).await.unwrap();
    bounded(backup_server).await.unwrap();
}

#[tokio::test]
async fn interrupted_and_retired_silent_http_contexts_do_not_advance_a_fresh_streak() {
    for interrupted in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechConfig::openai("test-key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let (ready, entered) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut old, _) = listener.accept().await.unwrap();
            support::read_request(&mut old).await;
            ready.send(()).unwrap();
            released.await.unwrap();
            let _ = old
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
            drop(old);
            let (mut fresh, _) = listener.accept().await.unwrap();
            support::read_request(&mut fresh).await;
            fresh
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });
        let mut worker = Pipeline::new(vec![Arc::new(
            processor(config).with_max_consecutive_zero_audio_contexts(2),
        )])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let health = worker.processor_health(0).unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("old"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        bounded(entered).await.unwrap();
        if interrupted {
            worker
                .sender()
                .send(Frame::new(FrameData::Interruption), Direction::Downstream)
                .await
                .unwrap();
            loop {
                if matches!(
                    bounded(worker.recv()).await.unwrap().frame.data,
                    FrameData::Interruption
                ) {
                    break;
                }
            }
        } else {
            health.set_usable(false);
            health.set_usable(true);
        }
        release.send(()).unwrap();
        let frames = utterance(&mut worker, "fresh").await;
        assert_eq!(errors(&frames).len(), 1);
        assert!(health.is_usable());
        worker.end().await.unwrap();
        assert_eq!(bounded(worker.collect()).await.unwrap().1.total_errors, 1);
        bounded(server).await.unwrap();
    }
}
