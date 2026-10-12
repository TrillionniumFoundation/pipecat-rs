//! Shared input transport to Grok Realtime over a real localhost WebSocket.
#![cfg(feature = "grok-realtime")]
use futures_util::{SinkExt, StreamExt};
use pipecat::{
    audio::{encode_pcm16_le, resample_pcm16, PcmFormat},
    core::*,
    grok_realtime::{Action, GrokRealtimeProcessor, RealtimeClient, RealtimeConfig},
    media_transport::*,
    protocol::rtvi::RawAudioData,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::{net::TcpListener, sync::Semaphore, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::Message;

struct Peer(JoinHandle<Vec<Value>>);
impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite fixes the handshake callback error type.
    async fn new() -> (Self, Arc<GrokRealtimeProcessor>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("shared-audio-loopback-key");
        config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                tcp,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    assert_eq!(
                        request.headers()["authorization"],
                        "Bearer shared-audio-loopback-key"
                    );
                    assert_eq!(request.uri().query(), Some("model=grok-voice-latest"));
                    assert!(!request.headers().contains_key("openai-beta"));
                    Ok(response)
                },
            )
            .await
            .unwrap();
            for event in [
                json!({"event_id":"session-created","type":"session.created","session":{"id":"shared-audio-session"}}),
                json!({"event_id":"conversation-created","type":"conversation.created","conversation":{"id":"shared-audio-conversation"}}),
            ] {
                ws.send(Message::Text(event.to_string().into()))
                    .await
                    .unwrap();
            }
            let message = ws.next().await.unwrap().unwrap();
            let update: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            assert_eq!(update["type"], "session.update");
            ws.send(Message::Text(json!({"type":"session.updated","event_id":"ready","session":{"id":"shared-audio-session"}}).to_string().into())).await.unwrap();
            let mut commands = Vec::new();
            loop {
                match ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        commands.push(serde_json::from_str(&text).unwrap())
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        let _ = ws.close(None).await;
                        break;
                    }
                    Some(Err(error)) => panic!("unexpected localhost socket error: {error}"),
                    _ => {}
                }
            }
            commands
        });
        let processor =
            Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
        (Self(task), processor)
    }
    async fn closed(self) -> Vec<Value> {
        timeout(Duration::from_secs(3), self.0)
            .await
            .expect("socket must close")
            .unwrap()
    }
}
#[derive(Default)]
struct Backend {
    started: AtomicUsize,
    stopped: AtomicUsize,
    format: Mutex<Option<PcmFormat>>,
}
#[async_trait]
impl InputBackend for Backend {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        *self.format.lock().unwrap() = Some(format);
        Ok(())
    }
    async fn capture(&self, _: InputIngress) -> Result<(), ProcessorError> {
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Filter {
    calls: AtomicUsize,
    controls: AtomicUsize,
    stopped: AtomicUsize,
    drop_pcm: bool,
    gate: Option<Semaphore>,
    entered: Semaphore,
}
impl Default for Filter {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            controls: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
            drop_pcm: false,
            gate: None,
            entered: Semaphore::new(0),
        }
    }
}
#[async_trait]
impl AudioFilter for Filter {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn filter(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.add_permits(1);
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        Ok(if self.drop_pcm { Vec::new() } else { audio })
    }
    async fn control(&self, _: &FrameData) -> Result<(), ProcessorError> {
        self.controls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn audio(user: Option<&str>, rate: u32, samples: &[i16]) -> Frame {
    let audio = AudioRawFrame::new(encode_pcm16_le(samples), rate, 1).unwrap();
    Frame::new(match user {
        None => FrameData::InputAudio(audio),
        Some(user) => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: user.into(),
        }),
    })
}
fn raw(frame: &Frame) -> Option<&AudioRawFrame> {
    match &frame.data {
        FrameData::InputAudio(audio) => Some(audio),
        FrameData::UserAudio(audio) => Some(&audio.audio),
        _ => None,
    }
}
async fn start(
    config: InputTransportConfig,
    filter: Option<Arc<Filter>>,
) -> (
    PipelineWorker,
    InputIngress,
    Arc<Backend>,
    Peer,
    Arc<GrokRealtimeProcessor>,
) {
    let (peer, realtime) = Peer::new().await;
    let backend = Arc::new(Backend::default());
    let input = Arc::new(
        InputTransport::new(
            "shared-realtime-input",
            config,
            backend.clone(),
            filter.map(|filter| filter as Arc<dyn AudioFilter>),
        )
        .unwrap(),
    );
    let ingress = input.ingress();
    let mut pipeline = PipelineConfig::default();
    pipeline.start.audio_in_sample_rate = 24_000;
    pipeline.start.audio_out_sample_rate = 24_000;
    let worker = Pipeline::new(vec![input, realtime.clone()])
        .start(pipeline)
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while !ingress.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    (worker, ingress, backend, peer, realtime)
}
async fn receive(
    worker: &mut PipelineWorker,
    wanted: impl Fn(&DirectedFrame) -> bool,
) -> Vec<DirectedFrame> {
    timeout(Duration::from_secs(2), async {
        let mut observed = Vec::new();
        loop {
            let frame = worker
                .recv()
                .await
                .expect("pipeline closed before expected frame");
            assert!(
                !matches!(frame.frame.data, FrameData::Error(_)),
                "{:?}",
                frame.frame.data
            );
            let found = wanted(&frame);
            observed.push(frame);
            if found {
                return observed;
            }
        }
    })
    .await
    .expect("expected pipeline frame before deadline")
}
async fn filter_barrier(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    worker
        .sender()
        .send(Frame::new(FrameData::FilterControl), Direction::Downstream)
        .await
        .unwrap();
    receive(worker, |frame| {
        matches!(frame.frame.data, FrameData::FilterControl)
    })
    .await
}
async fn finish(worker: PipelineWorker) -> (Vec<DirectedFrame>, WorkerReport) {
    worker.end().await.unwrap();
    timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap()
}
fn wire_pcm(commands: &[Value]) -> Vec<u8> {
    commands
        .iter()
        .filter(|command| command["type"] == "input_audio_buffer.append")
        .flat_map(|command| {
            assert!(command.get("user_id").is_none());
            RawAudioData {
                base64_audio: command["audio"].as_str().unwrap().into(),
                sample_rate: 24_000,
                num_channels: 1,
            }
            .decode()
            .unwrap()
        })
        .collect()
}
fn assert_clean(report: &WorkerReport, backend: &Backend) {
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(
        *backend.format.lock().unwrap(),
        Some(PcmFormat::new(24_000, 1).unwrap())
    );
}

#[tokio::test]
async fn shared_input_capture_and_injection_preserve_both_audio_classes_on_wire() {
    for capture in [true, false] {
        let (mut worker, ingress, backend, peer, realtime) = start(Default::default(), None).await;
        let mut observed = Vec::new();
        for user in [None, Some("captured-speaker")] {
            let mut frame = audio(user, 24_000, &[513, 1027]);
            frame.name = "captured-pcm".into();
            frame.set_pipeline_pts(12345);
            frame.transport_source = Some("microphone".into());
            frame.metadata.insert("capture".into(), json!(capture));
            let tag = LocalFrameTag::new();
            tag.mark(&mut frame).unwrap();
            let expected = frame.clone();
            if capture {
                ingress.push(frame).unwrap();
            } else {
                worker
                    .sender()
                    .send(frame, Direction::Downstream)
                    .await
                    .unwrap();
            }
            let frames = receive(&mut worker, |frame| raw(&frame.frame).is_some()).await;
            let actual = frames.last().unwrap();
            assert_eq!(actual.direction, Direction::Downstream);
            assert_eq!(actual.frame.id, expected.id);
            assert_eq!(actual.frame.data, expected.data);
            assert_eq!(actual.frame.name, expected.name);
            assert_eq!(actual.frame.pts, expected.pts);
            assert_eq!(actual.frame.transport_source, expected.transport_source);
            assert_eq!(actual.frame.metadata, expected.metadata);
            assert!(tag.contains(&actual.frame));
            observed.extend(frames);
        }
        // All transport PCM has reached the adapter before this socket barrier.
        observed.extend(filter_barrier(&mut worker).await);
        realtime
            .sender()
            .unwrap()
            .send(Action::CommitAudio)
            .await
            .unwrap();
        let (_, report) = finish(worker).await;
        let commands = peer.closed().await;
        assert_clean(&report, &backend);
        assert_eq!(
            commands.last().unwrap()["type"],
            "input_audio_buffer.commit"
        );
        assert_eq!(
            wire_pcm(&commands),
            vec![1, 2, 3, 4, 1, 2, 3, 4],
            "InputTransport must feed UserAudio PCM through the Grok adapter"
        );
        assert_eq!(commands.len(), 3);
        assert_eq!(
            observed
                .iter()
                .filter(|frame| raw(&frame.frame).is_some())
                .count(),
            2
        );
    }
}

#[tokio::test]
async fn shared_user_audio_resamples_16k_to_24k_including_tail_before_socket_end() {
    let (mut worker, ingress, backend, peer, realtime) = start(Default::default(), None).await;
    let samples: Vec<i16> = (0..501).map(|sample| sample * 40 - 10000).collect();
    for chunk in samples.chunks(33) {
        let mut frame = audio(Some("captured-speaker"), 16_000, chunk);
        frame.transport_source = Some("microphone".into());
        frame
            .metadata
            .insert("owner".into(), json!("captured-speaker"));
        ingress.push(frame).unwrap();
    }
    // A queued filter control flushes the resampler tail before its forwarding.
    let frames = filter_barrier(&mut worker).await;
    realtime
        .sender()
        .unwrap()
        .send(Action::CommitAudio)
        .await
        .unwrap();
    let (_, report) = finish(worker).await;
    let commands = peer.closed().await;
    assert_clean(&report, &backend);
    let expected = resample_pcm16(&encode_pcm16_le(&samples), 16_000, 24_000, 1).unwrap();
    assert_eq!(wire_pcm(&commands), expected);
    let mut forwarded = Vec::new();
    for frame in frames.iter().filter(|frame| raw(&frame.frame).is_some()) {
        let FrameData::UserAudio(audio) = &frame.frame.data else {
            panic!("UserAudio identity lost")
        };
        assert_eq!(audio.user_id, "captured-speaker");
        assert_eq!(audio.audio.sample_rate, 24_000);
        assert_eq!(audio.audio.num_channels, 1);
        assert_eq!(frame.direction, Direction::Downstream);
        assert_eq!(frame.frame.transport_source.as_deref(), Some("microphone"));
        assert_eq!(frame.frame.metadata["owner"], "captured-speaker");
        forwarded.extend_from_slice(&audio.audio.audio);
    }
    assert_eq!(forwarded, expected);
}

#[tokio::test]
async fn shared_audio_disable_pause_empty_and_passthrough_gates_suppress_wire_pcm() {
    for gate in [
        "disabled",
        "paused",
        "empty",
        "filtered-empty",
        "passthrough",
    ] {
        let config = InputTransportConfig {
            audio_enabled: gate != "disabled",
            passthrough: gate != "passthrough",
            ..Default::default()
        };
        let filter = Arc::new(Filter {
            drop_pcm: gate == "filtered-empty",
            ..Default::default()
        });
        let (mut worker, ingress, backend, peer, _) = start(config, Some(filter.clone())).await;
        if gate == "paused" {
            ingress.pause();
        }
        for user in [None, Some("captured-speaker")] {
            let samples: &[i16] = if gate == "empty" { &[] } else { &[513, 1027] };
            ingress.push(audio(user, 24_000, samples)).unwrap();
        }
        let mut frames = filter_barrier(&mut worker).await;
        let (remaining, report) = finish(worker).await;
        frames.extend(remaining);
        let commands = peer.closed().await;
        assert_clean(&report, &backend);
        assert!(wire_pcm(&commands).is_empty(), "gate: {gate}");
        assert!(
            frames.iter().all(|frame| raw(&frame.frame).is_none()),
            "gate: {gate}"
        );
        assert_eq!(
            filter.calls.load(Ordering::SeqCst),
            usize::from(matches!(gate, "filtered-empty" | "passthrough")) * 2
        );
        assert_eq!(filter.controls.load(Ordering::SeqCst), 1);
        assert_eq!(filter.stopped.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn shared_capture_resume_accepts_new_user_audio_after_muted_audio_is_dropped() {
    let (mut worker, ingress, backend, peer, realtime) = start(Default::default(), None).await;
    ingress.pause();
    ingress
        .push(audio(Some("muted-speaker"), 24_000, &[99]))
        .unwrap();
    ingress.resume();
    ingress
        .push(audio(Some("resumed-speaker"), 24_000, &[513, 1027]))
        .unwrap();
    let frames = filter_barrier(&mut worker).await;
    realtime
        .sender()
        .unwrap()
        .send(Action::CommitAudio)
        .await
        .unwrap();
    let (_, report) = finish(worker).await;
    let commands = peer.closed().await;
    assert_clean(&report, &backend);
    assert_eq!(wire_pcm(&commands), vec![1, 2, 3, 4]);
    let audio: Vec<_> = frames
        .iter()
        .filter(|frame| raw(&frame.frame).is_some())
        .collect();
    assert_eq!(audio.len(), 1);
    assert!(
        matches!(&audio[0].frame.data, FrameData::UserAudio(audio) if audio.user_id == "resumed-speaker")
    );
}

#[tokio::test]
async fn cancel_during_shared_filter_discards_audio_and_closes_realtime_socket() {
    let filter = Arc::new(Filter {
        gate: Some(Semaphore::new(0)),
        ..Default::default()
    });
    let (worker, ingress, backend, peer, _) = start(Default::default(), Some(filter.clone())).await;
    ingress
        .push(audio(Some("first-speaker"), 24_000, &[513]))
        .unwrap();
    timeout(Duration::from_secs(2), filter.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    ingress
        .push(audio(Some("queued-speaker"), 24_000, &[1027]))
        .unwrap();
    worker.cancel("cancel blocked input filter").await.unwrap();
    let (frames, report) = timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(peer.closed().await.is_empty());
    assert!(frames.iter().all(|frame| raw(&frame.frame).is_none()));
    assert_eq!(filter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(filter.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert!(!ingress.is_ready());
    assert!(ingress
        .push(audio(Some("late-speaker"), 24_000, &[99]))
        .is_err());
}

#[tokio::test]
async fn shared_input_obeys_grok_pause_without_changing_forwarded_speaker() {
    let (mut worker, ingress, backend, peer, realtime) = start(Default::default(), None).await;
    realtime.set_audio_input_paused(true);
    ingress
        .push(audio(Some("paused-speaker"), 24_000, &[99]))
        .unwrap();
    let mut frames = filter_barrier(&mut worker).await;
    realtime.set_audio_input_paused(false);
    ingress
        .push(audio(Some("resumed-speaker"), 24_000, &[513, 1027]))
        .unwrap();
    frames.extend(filter_barrier(&mut worker).await);
    realtime
        .sender()
        .unwrap()
        .send(Action::CommitAudio)
        .await
        .unwrap();
    let (_, report) = finish(worker).await;
    let commands = peer.closed().await;
    assert_clean(&report, &backend);
    assert_eq!(wire_pcm(&commands), vec![1, 2, 3, 4]);
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[1]["type"], "input_audio_buffer.commit");
    let speakers: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::UserAudio(audio) => Some(audio.user_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(speakers, vec!["paused-speaker", "resumed-speaker"]);
}
