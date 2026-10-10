use super::*;
use crate::{MoqTransport, MoqTransportConfig, MoqTransportState};
use pipecat_core::{Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig};

fn pcm(rate: u32, samples: usize, channels: u16) -> AudioRawFrame {
    AudioRawFrame::new(
        (0..samples)
            .flat_map(|n| {
                (0..channels).flat_map(move |_| (((n % 80) as i16 - 40) * 200).to_le_bytes())
            })
            .collect(),
        rate,
        channels,
    )
    .unwrap()
}
async fn start(transport: &MoqTransport, duplex: bool) -> pipecat_core::PipelineWorker {
    let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![];
    if duplex {
        processors.push(transport.input());
    }
    processors.push(transport.output());
    Pipeline::new(processors)
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(10),
            ..Default::default()
        })
        .await
        .unwrap()
}
async fn track(peer: &Peer) -> moq_net::track::Subscriber {
    let remote = peer.remote().await;
    bounded(remote.track("bot-audio").unwrap().subscribe(None))
        .await
        .unwrap()
}
async fn packet(track: &mut moq_net::track::Subscriber) -> hang::container::Frame {
    let mut group = bounded(track.recv_group()).await.unwrap().unwrap();
    let frame = bounded(group.read_frame()).await.unwrap().unwrap();
    hang::container::Frame::decode(frame.payload).unwrap()
}

#[tokio::test]
async fn generation_fences_reject_stale_audio_and_json_but_plain_json_survives() {
    let (mut connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let sender = connection.sender();
    let generation = sender.generation();
    let mut messages = peer.incoming_json().await;
    sender.interrupt().await.unwrap();
    assert_eq!(
        sender
            .publish_audio_for_generation(audio(), generation)
            .await,
        Err(Error::Interrupted)
    );
    assert_eq!(
        sender
            .publish_json_for_generation(json!({"stale":true}), generation)
            .await,
        Err(Error::Interrupted)
    );
    sender.publish_json(json!({"plain":true})).await.unwrap();
    assert_eq!(
        bounded(messages.next()).await.unwrap().unwrap()["plain"],
        true
    );
    sender
        .publish_json_for_generation(json!({"current":true}), sender.generation())
        .await
        .unwrap();
    assert_eq!(
        bounded(messages.next()).await.unwrap().unwrap()["current"],
        true
    );
    connection.cancel().await.unwrap();
}
#[tokio::test]
async fn event_receiver_transfers_once_without_transferring_shutdown_ownership() {
    let (mut connection, _peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let mut events = connection.take_events().unwrap();
    assert!(connection.take_events().is_none());
    assert!(connection.recv().await.is_none());
    assert!(matches!(events.recv().await, Some(MoqEvent::PeerConnected)));
    connection.cancel().await.unwrap();
    assert_eq!(connection.monitor().active_tasks(), 0);
}
#[tokio::test]
async fn real_pipeline_resamples_22050_stereo_and_end_preserves_both_holders() {
    let (connection, peer) = pair(|_| {}, 1).await;
    let monitor = connection.monitor();
    let transport = MoqTransport::new(
        connection,
        MoqTransportConfig {
            queue_capacity: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut audio_track = track(&peer).await;
    let mut messages = peer.incoming_json().await;
    let worker = start(&transport, true).await;
    let reader = tokio::spawn(async move {
        let mut samples = 0;
        let mut nonzero = false;
        let mut decoder = opus::Decoder::new(48_000, opus::Channels::Mono).unwrap();
        for n in 0..10 {
            let packet = packet(&mut audio_track).await;
            assert_eq!(packet.timestamp.value(), n * 20_000);
            let mut pcm = [0i16; 5760];
            let size = decoder.decode(&packet.payload, &mut pcm, false).unwrap();
            samples += size;
            nonzero |= pcm[..size].iter().any(|s| *s != 0);
        }
        (samples, nonzero)
    });
    for _ in 0..20 {
        worker
            .sender()
            .send(
                Frame::new(FrameData::TtsAudio(pcm(22_050, 220, 2))),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    for data in [
        FrameData::TtsText(pipecat_core::AggregatedTextFrame {
            text: pipecat_core::TextFrame::new("submitted is not heard"),
            aggregated_by: "sentence".into(),
            context_id: Some("speech".into()),
            raw_text: None,
            will_be_spoken: true,
        }),
        FrameData::AggregatedTextProgress(pipecat_core::AggregatedTextProgressFrame {
            segment_id: 1,
            context_id: Some("speech".into()),
            text: "submitted".into(),
            aggregated_by: "sentence".into(),
            accumulated_text: "submitted".into(),
            remaining_text: " is not heard".into(),
        }),
    ] {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    worker
        .sender()
        .send(
            Frame::new(FrameData::OutputTransportMessage(json!({"type":"done"}))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let collect = tokio::spawn(worker.collect());
    assert_eq!(
        bounded(messages.next()).await.unwrap().unwrap()["type"],
        "done"
    );
    assert_eq!(
        bounded(messages.next()).await.unwrap().unwrap()["type"],
        "session-ending"
    );
    assert_eq!(bounded(reader).await.unwrap(), (9600, true));
    let (frames, report) = bounded(collect).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.dropped_frames, 0);
    assert!(!frames.iter().any(|f| matches!(
        f.frame.data,
        FrameData::BotStartedSpeaking
            | FrameData::BotSpeaking
            | FrameData::BotStoppedSpeaking
            | FrameData::TtsText(_)
            | FrameData::AggregatedTextProgress(_)
            | FrameData::LlmContextAssistantTurn(_)
    )));
    assert_eq!(transport.state(), MoqTransportState::Closed);
    assert_eq!(monitor.active_tasks(), 0);
    transport.input().cleanup().await.unwrap();
    transport.output().cleanup().await.unwrap();
}
#[tokio::test]
async fn real_pipeline_input_resamples_to_start_rate_and_routes_rtvi_upstream() {
    let (connection, mut peer) = pair(|_| {}, 2).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let worker = Pipeline::new(vec![transport.input()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    peer.transcript
        .append(&json!({"label":"rtvi","type":"test","seq":0,"epoch":"remote"}))
        .unwrap();
    peer.send_audio(2, 0);
    peer.send_audio(2, 20_000);
    tokio::time::sleep(Duration::from_millis(100)).await;
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(frames.iter().any(|f| f.direction == Direction::Upstream && matches!(&f.frame.data, FrameData::InputTransportMessage(m) if m["type"] == "test" && m.get("seq").is_none())));
    let mut samples = 0;
    for directed in &frames {
        if let FrameData::InputAudio(audio) = &directed.frame.data {
            assert_eq!(directed.direction, Direction::Downstream);
            assert_eq!((audio.sample_rate, audio.num_channels), (16_000, 1));
            assert!(directed.frame.pts.is_none());
            samples += audio.num_frames();
        }
    }
    assert_eq!(samples, 640);
    assert_eq!(transport.monitor().active_tasks(), 0);
}
#[tokio::test]
async fn output_only_drains_peer_events_and_exposes_messages_upstream() {
    let (connection, mut peer) = pair(|_| {}, 1).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let worker = start(&transport, false).await;
    for n in 0..8 {
        peer.transcript.append(&json!({"n":n})).unwrap();
    }
    peer.send_audio(1, 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::InputTransportMessage(_)))
            .count(),
        8
    );
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::InputAudio(_))));
    assert_eq!(transport.monitor().active_tasks(), 0);
}
#[tokio::test]
async fn cancel_during_audio_pacing_joins_network_and_pipeline_tasks() {
    let (connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(
        connection,
        MoqTransportConfig {
            max_frame_bytes: 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    let mut outgoing = track(&peer).await;
    let worker = start(&transport, false).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::OutputAudio(pcm(48_000, 48_000, 1))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let _ = packet(&mut outgoing).await;
    worker.cancel("test hard stop").await.unwrap();
    let _ = bounded(worker.collect()).await;
    bounded(transport.monitor().wait_idle()).await;
    assert_eq!(transport.monitor().active_tasks(), 0);
}
#[tokio::test]
async fn unsupported_rich_output_is_typed_without_claiming_permanent_network_failure() {
    let (connection, _peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let worker = start(&transport, false).await;
    worker
        .sender()
        .send(Frame::new(FrameData::Volume(0.5)), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(!report.errors.is_empty());
    assert!(frames.iter().any(|f| matches!(&f.frame.data, FrameData::Error(error) if error.category == pipecat_core::ErrorCategory::InvalidRequest)));
    assert_eq!(transport.monitor().active_tasks(), 0);
}

#[tokio::test]
async fn interrupted_pipeline_discards_old_tail_and_new_generation_publishes() {
    let (connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(
        connection,
        MoqTransportConfig {
            max_frame_bytes: 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    let mut outgoing = track(&peer).await;
    let mut messages = peer.incoming_json().await;
    let mut worker = start(&transport, false).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::OutputAudio(pcm(48_000, 48_000, 1))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let _ = packet(&mut outgoing).await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    bounded(async {
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::Interruption) {
                return;
            }
        }
        panic!("interruption not forwarded");
    })
    .await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::OutputAudio(pcm(22050, 441, 1))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::OutputTransportMessage(json!({"new":true}))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let collect = tokio::spawn(worker.collect());
    assert_eq!(
        bounded(messages.next()).await.unwrap().unwrap()["new"],
        true
    );
    assert_eq!(
        bounded(messages.next()).await.unwrap().unwrap()["type"],
        "session-ending"
    );
    let (_frames, report) = bounded(collect).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(transport.state(), MoqTransportState::Closed);
    assert_eq!(transport.monitor().active_tasks(), 0);
}
#[tokio::test]
async fn urgent_json_bypasses_slow_audio_lane_and_normal_json_stays_ordered() {
    let (connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(
        connection,
        MoqTransportConfig {
            max_frame_bytes: 1024 * 1024,
            queue_capacity: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut outgoing = track(&peer).await;
    let mut messages = peer.incoming_json().await;
    let worker = start(&transport, false).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::OutputAudio(pcm(48_000, 24000, 1))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let _ = packet(&mut outgoing).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::OutputTransportMessage(json!({"normal":true}))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::OutputTransportMessageUrgent(
                json!({"urgent":true}),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_millis(300), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first["urgent"], true);
    worker.end().await.unwrap();
    let collect = tokio::spawn(worker.collect());
    assert_eq!(
        bounded(messages.next()).await.unwrap().unwrap()["normal"],
        true
    );
    assert_eq!(
        bounded(messages.next()).await.unwrap().unwrap()["type"],
        "session-ending"
    );
    let (_, report) = bounded(collect).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test]
async fn remote_disconnect_sets_shared_read_only_health_and_typed_pipeline_error() {
    let (connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let worker = Pipeline::new(vec![transport.input(), transport.output()])
        .start(PipelineConfig {
            processor_unusable_policy: pipecat_core::ProcessorUnusablePolicy::End,
            ..Default::default()
        })
        .await
        .unwrap();
    drop(peer);
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(!report.errors.is_empty());
    assert!(!transport.health().is_usable());
    transport.health().set_usable(true);
    assert!(!transport.health().is_usable());
    assert!(matches!(transport.state(), MoqTransportState::Failed(_)));
    assert!(frames.iter().any(|f| matches!(&f.frame.data, FrameData::Error(error) if error.category == pipecat_core::ErrorCategory::Connectivity || error.category == pipecat_core::ErrorCategory::Server)));
    assert_eq!(transport.monitor().active_tasks(), 0);
}
#[tokio::test]
async fn cancellation_of_idle_event_pump_does_not_hold_connection_lock() {
    let (connection, _peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let worker = Pipeline::new(vec![transport.input()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    worker.cancel("idle input").await.unwrap();
    bounded(worker.collect()).await.unwrap();
    assert_eq!(transport.monitor().active_tasks(), 0);
}
#[tokio::test]
async fn failed_setup_closes_the_already_owned_native_session() {
    let (connection, _peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let mut config = PipelineConfig::default();
    config.start.audio_out_sample_rate = 100;
    assert!(Pipeline::new(vec![transport.input(), transport.output()])
        .start(config)
        .await
        .is_err());
    bounded(transport.monitor().wait_idle()).await;
    assert_eq!(transport.monitor().active_tasks(), 0);
}
#[tokio::test]
async fn late_cancel_fence_does_not_invalidate_a_later_native_generation() {
    let (mut connection, _peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let sender = connection.sender();
    let old = sender.generation();
    sender.interrupt().await.unwrap();
    let next = sender.generation();
    sender.interrupt_if_generation(old).unwrap();
    assert_eq!(sender.generation(), next);
    sender.interrupt_if_generation(next).unwrap();
    assert_eq!(sender.generation(), next + 1);
    connection.cancel().await.unwrap();
}

#[tokio::test]
async fn reversed_pipeline_endpoints_fail_before_start_and_release_the_session() {
    let (connection, _peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    assert!(Pipeline::new(vec![transport.output(), transport.input()])
        .start(PipelineConfig::default())
        .await
        .is_err());
    bounded(transport.monitor().wait_idle()).await;
    assert_eq!(transport.monitor().active_tasks(), 0);
}

#[tokio::test]
async fn tiny_pcm_chunks_keep_state_without_one_background_task_per_sample() {
    let (connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let mut outgoing = track(&peer).await;
    let worker = Pipeline::new(vec![transport.output()])
        .start(PipelineConfig {
            max_background_tasks: 8,
            ..Default::default()
        })
        .await
        .unwrap();
    let reader = tokio::spawn(async move {
        let first = packet(&mut outgoing).await;
        let second = packet(&mut outgoing).await;
        assert_eq!(first.timestamp.value(), 0);
        assert_eq!(second.timestamp.value(), 20_000);
    });
    for _ in 0..1200 {
        worker
            .sender()
            .send(
                Frame::new(FrameData::OutputAudio(pcm(48_000, 1, 1))),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    bounded(reader).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(transport.monitor().active_tasks(), 0);
}

#[tokio::test]
async fn explicit_input_rate_converts_wire_audio_at_a_non_opus_rate() {
    let (connection, peer) = pair(|_| {}, 1).await;
    let transport = MoqTransport::new(
        connection,
        MoqTransportConfig {
            input_sample_rate: Some(22_050),
            ..Default::default()
        },
    )
    .unwrap();
    let mut worker = Pipeline::new(vec![transport.input()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    peer.send_audio(1, 0);
    let first = bounded(async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::InputAudio(audio) = frame.frame.data {
                assert_eq!(audio.sample_rate, 22_050);
                return audio.num_frames();
            }
        }
        panic!("input audio never arrived");
    })
    .await;
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let tail: usize = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::InputAudio(audio) => Some(audio.num_frames()),
            _ => None,
        })
        .sum();
    assert_eq!(first + tail, 441);
    assert_eq!(transport.monitor().active_tasks(), 0);
}

#[tokio::test]
async fn invalid_individual_output_does_not_poison_a_healthy_json_session() {
    let (connection, peer) = pair(
        |c| {
            c.audio_in_enabled = false;
            c.audio_out_enabled = false;
        },
        1,
    )
    .await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let mut messages = peer.incoming_json().await;
    let worker = start(&transport, false).await;
    for data in [
        FrameData::OutputAudio(pcm(48_000, 960, 1)),
        FrameData::OutputTransportMessage(json!({"label":"moq-transport","type":"bad"})),
        FrameData::OutputTransportMessage(json!({"large":"x".repeat(70_000)})),
        FrameData::OutputTransportMessage(json!({"valid":true})),
    ] {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    assert_eq!(
        bounded(messages.next()).await.unwrap().unwrap()["valid"],
        true
    );
    assert!(transport.health().is_usable());
    assert_eq!(transport.state(), MoqTransportState::Connected);
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.errors.len(), 3, "{:?}", report.errors);
    assert_eq!(transport.monitor().active_tasks(), 0);
}

#[tokio::test]
async fn failed_input_setup_cannot_start_a_receive_pump() {
    let (connection, _peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 100;
    assert!(Pipeline::new(vec![transport.input(), transport.output()])
        .start(config)
        .await
        .is_err());
    bounded(transport.monitor().wait_idle()).await;
    assert_eq!(transport.monitor().active_tasks(), 0);
}

#[tokio::test]
async fn incoming_json_respects_the_pipeline_frame_limit_before_delivery() {
    let (connection, mut peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(
        connection,
        MoqTransportConfig {
            max_frame_bytes: 1024,
            ..Default::default()
        },
    )
    .unwrap();
    let worker = Pipeline::new(vec![transport.input()])
        .start(PipelineConfig {
            processor_unusable_policy: pipecat_core::ProcessorUnusablePolicy::End,
            ..Default::default()
        })
        .await
        .unwrap();
    peer.transcript
        .append(&json!({"larger_than_pipeline_bound":"x".repeat(2048)}))
        .unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(!report.errors.is_empty());
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.frame.data, FrameData::InputTransportMessage(_))));
    assert!(matches!(
        transport.state(),
        MoqTransportState::Failed(Error::Limit("pipeline frame bytes"))
    ));
    assert_eq!(transport.monitor().active_tasks(), 0);
}

struct CaptureOrigin(std::sync::Mutex<Option<pipecat_core::ProcessorContext>>);
#[async_trait::async_trait]
impl FrameProcessor for CaptureOrigin {
    fn name(&self) -> &str {
        "origin"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &pipecat_core::ProcessorContext,
    ) -> std::result::Result<(), pipecat_core::ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.0.lock().unwrap() = Some(ctx.clone());
        }
        ctx.push(frame, direction).await
    }
}
async fn origin_context(
    worker: &mut pipecat_core::PipelineWorker,
    capture: &CaptureOrigin,
) -> pipecat_core::ProcessorContext {
    worker
        .sender()
        .send(Frame::text("capture"), Direction::Downstream)
        .await
        .unwrap();
    bounded(async {
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::Text(_)) {
                return;
            }
        }
        panic!("origin capture closed");
    })
    .await;
    capture.0.lock().unwrap().as_ref().unwrap().clone()
}
async fn processed_audio(progress: &mut tokio::sync::watch::Receiver<u64>, count: u64) {
    bounded(async {
        loop {
            if *progress.borrow_and_update() >= count {
                return;
            }
            progress.changed().await.unwrap();
        }
    })
    .await;
}
struct ContextOutput {
    output: Arc<crate::MoqOutputTransport>,
    context: std::sync::Mutex<Option<pipecat_core::ProcessorContext>>,
}
#[async_trait::async_trait]
impl FrameProcessor for ContextOutput {
    fn name(&self) -> &str {
        self.output.name()
    }
    fn health(&self) -> Option<pipecat_core::ProcessorHealth> {
        self.output.health()
    }
    async fn setup(
        &self,
        start: &pipecat_core::StartFrame,
        ctx: &pipecat_core::ProcessorContext,
    ) -> std::result::Result<(), pipecat_core::ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        self.output.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &pipecat_core::ProcessorContext,
    ) -> std::result::Result<(), pipecat_core::ProcessorError> {
        self.output.process(frame, direction, ctx).await
    }
    async fn prepare_end(
        &self,
        ctx: &pipecat_core::ProcessorContext,
    ) -> std::result::Result<(), pipecat_core::ProcessorError> {
        self.output.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> std::result::Result<(), pipecat_core::ProcessorError> {
        self.output.cleanup().await
    }
}
#[tokio::test]
async fn tiny_pcm_keeps_its_original_causal_fence_across_source_interruption() {
    let capture = Arc::new(CaptureOrigin(std::sync::Mutex::new(None)));
    let mut source = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let old = origin_context(&mut source, &capture).await;
    let (connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let mut progress = transport.audio_progress_for_test();
    let outgoing = track(&peer).await;
    let reader = tokio::spawn(read_all_audio(outgoing));
    let output = Arc::new(ContextOutput {
        output: transport.output(),
        context: std::sync::Mutex::new(None),
    });
    let worker = Pipeline::new(vec![output.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let destination = output.context.lock().unwrap().as_ref().unwrap().clone();
    output
        .output
        .process(
            Frame::new(FrameData::OutputAudio(tagged_pcm(-12_000, 480))),
            Direction::Downstream,
            &destination.forwarding_context(&old),
        )
        .await
        .unwrap();
    processed_audio(&mut progress, 1).await;
    // The source epoch advances while the MoQ destination deliberately receives
    // no interruption frame. Its native generation therefore cannot mask the
    // saved-context bug. The second half alone is valid under its new origin.
    source
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    bounded(async {
        while let Some(frame) = source.recv().await {
            if matches!(frame.frame.data, FrameData::Interruption) {
                return;
            }
        }
        panic!("source interruption not seen");
    })
    .await;
    let current = origin_context(&mut source, &capture).await;
    output
        .output
        .process(
            Frame::new(FrameData::OutputAudio(tagged_pcm(12_000, 480))),
            Direction::Downstream,
            &destination.forwarding_context(&current),
        )
        .await
        .unwrap();
    processed_audio(&mut progress, 2).await;
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_wire_audio(bounded(reader).await.unwrap(), &[tagged_pcm(12_000, 480)]);
    source.end().await.unwrap();
    bounded(source.collect()).await.unwrap();
    assert_eq!(transport.monitor().active_tasks(), 0);
}

fn tagged_pcm(value: i16, frames: usize) -> AudioRawFrame {
    AudioRawFrame::new(value.to_le_bytes().repeat(frames), 48_000, 1).unwrap()
}
async fn read_all_audio(mut track: moq_net::track::Subscriber) -> Vec<hang::container::Frame> {
    let mut packets = Vec::new();
    // Session close can follow track completion; exact expected payloads below
    // still reject a truncated or extra publication sequence.
    while let Ok(Some(mut group)) = bounded(track.recv_group()).await {
        let frame = bounded(group.read_frame()).await.unwrap().unwrap();
        packets.push(hang::container::Frame::decode(frame.payload).unwrap());
    }
    packets
}
fn assert_wire_audio(actual: Vec<hang::container::Frame>, expected: &[AudioRawFrame]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "wrong number of causal-scope packets"
    );
    let mut encoder = crate::media::Encoder::new().unwrap();
    for (index, (actual, expected)) in actual.into_iter().zip(expected).enumerate() {
        let mut padded = expected.clone();
        padded.audio.resize(1920, 0);
        assert_eq!(actual.timestamp.value(), index as u64 * 20_000);
        assert_eq!(
            actual.payload,
            encoder.encode(&padded).unwrap(),
            "wrong PCM contributor in packet {index}"
        );
    }
}
async fn three_source_causal_boundary(survive_middle: bool) {
    let mut sources = Vec::new();
    let mut contexts = Vec::new();
    for _ in 0..3 {
        let capture = Arc::new(CaptureOrigin(std::sync::Mutex::new(None)));
        let mut source = Pipeline::new(vec![capture.clone()])
            .start(Default::default())
            .await
            .unwrap();
        contexts.push(origin_context(&mut source, &capture).await);
        sources.push(source);
    }
    let (connection, peer) = pair(|c| c.audio_in_enabled = false, 1).await;
    let native = connection.sender();
    let generation = native.generation();
    let transport = MoqTransport::new(connection, Default::default()).unwrap();
    let mut progress = transport.audio_progress_for_test();
    let reader = tokio::spawn(read_all_audio(track(&peer).await));
    let output = Arc::new(ContextOutput {
        output: transport.output(),
        context: std::sync::Mutex::new(None),
    });
    let worker = Pipeline::new(vec![output.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let destination = output.context.lock().unwrap().as_ref().unwrap().clone();
    let chunks = [
        tagged_pcm(1_000, 320),
        tagged_pcm(-12_000, 320),
        tagged_pcm(24_000, 320),
    ];
    assert!(chunks.iter().all(|audio| audio.context_id.is_none()));
    for index in 0..2 {
        let frame = Frame::new(FrameData::OutputAudio(chunks[index].clone()))
            .with_interruptible(index != 1 || !survive_middle);
        output
            .output
            .process(
                frame,
                Direction::Downstream,
                &destination.forwarding_context(&contexts[index]),
            )
            .await
            .unwrap();
        processed_audio(&mut progress, index as u64 + 1).await;
    }
    // All sources have numeric epoch zero, but separate runtime identities.
    // Advance only B, with A/current C still valid and no MoQ interruption.
    sources[1]
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    bounded(async {
        while let Some(frame) = sources[1].recv().await {
            if matches!(frame.frame.data, FrameData::Interruption) {
                return;
            }
        }
        panic!("middle-origin interruption not forwarded");
    })
    .await;
    output
        .output
        .process(
            Frame::new(FrameData::OutputAudio(chunks[2].clone())),
            Direction::Downstream,
            &destination.forwarding_context(&contexts[2]),
        )
        .await
        .unwrap();
    processed_audio(&mut progress, 3).await;
    assert_eq!(
        native.generation(),
        generation,
        "native interruption masked the causal test"
    );
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let expected = if survive_middle {
        chunks.to_vec()
    } else {
        vec![chunks[0].clone(), chunks[2].clone()]
    };
    assert_wire_audio(bounded(reader).await.unwrap(), &expected);
    for source in sources {
        source.end().await.unwrap();
        bounded(source.collect()).await.unwrap();
    }
    assert_eq!(transport.monitor().active_tasks(), 0);
}
#[tokio::test]
async fn three_origins_with_no_audio_id_cannot_hide_a_cancelled_middle_contributor() {
    three_source_causal_boundary(false).await;
}
#[tokio::test]
async fn different_causal_scopes_preserve_explicit_uninterruptible_audio() {
    three_source_causal_boundary(true).await;
}
