use pipecat_core::*;
use pipecat_observers::*;
use std::sync::Arc;

fn processor(index: usize) -> ObservedProcessor {
    ObservedProcessor {
        runtime_id: 1,
        index,
        name: Arc::from(format!("p{index}")),
    }
}
fn frame(kind: &'static str, id: u64) -> ObservedFrame {
    ObservedFrame {
        token: None,
        id,
        kind,
        interruptible: true,
        vad_stop_secs: None,
        metrics: vec![],
        omitted_metrics: 0,
    }
}
fn process(index: usize, at: u64) -> PipelineObservation {
    PipelineObservation::Process {
        processor: processor(index),
        frame: frame("StartFrame", 1),
        direction: Direction::Downstream,
        timestamp_ns: at,
    }
}
fn push(kind: &'static str, at: u64) -> PipelineObservation {
    PipelineObservation::Push {
        source: processor(0),
        destination: None,
        frame: frame(kind, at),
        direction: Direction::Downstream,
        first_push: true,
        timestamp_ns: at,
    }
}
fn vad(at: u64, silence: f64) -> PipelineObservation {
    let mut event = push("VADUserStoppedSpeakingFrame", at);
    if let PipelineObservation::Push { frame, .. } = &mut event {
        frame.vad_stop_secs = Some(silence);
    }
    event
}
fn start() -> PipelineObservation {
    PipelineObservation::RuntimeStarted {
        runtime_id: 1,
        timestamp_ns: 0,
    }
}
fn ready(at: u64) -> PipelineObservation {
    PipelineObservation::PipelineStarted {
        runtime_id: 1,
        timestamp_ns: at,
    }
}
fn setup(index: usize, begin: u64, end: u64) -> PipelineObservation {
    PipelineObservation::ProcessorSetup {
        processor: processor(index),
        started_ns: begin,
        finished_ns: end,
        outcome: SetupOutcome::Completed,
    }
}
fn start_push(index: usize, at: u64) -> PipelineObservation {
    PipelineObservation::Push {
        source: processor(index),
        destination: None,
        frame: frame("StartFrame", 1),
        direction: Direction::Downstream,
        first_push: index == 0,
        timestamp_ns: at,
    }
}

#[test]
fn startup_uses_real_spans_and_each_processor_start_hop() {
    let mut timing = StartupTiming::default();
    timing.observe(&start());
    timing.observe(&setup(1, 0, 200_000_000));
    timing.observe(&setup(0, 0, 100_000_000));
    timing.observe(&process(0, 200_000_000));
    timing.observe(&start_push(0, 230_000_000));
    timing.observe(&process(1, 230_000_000));
    timing.observe(&start_push(1, 280_000_000));
    let Some(StartupMeasurement::Startup(report)) = timing.observe(&ready(300_000_000)) else {
        panic!("startup report")
    };
    assert!(report.complete);
    assert_eq!(report.total_seconds, 0.3);
    assert_eq!(report.setup_phase_seconds, 0.2);
    assert!((report.start_phase_seconds - 0.1).abs() < 1e-12);
    assert_eq!(report.processors[0].setup_seconds, Some(0.1));
    assert_eq!(report.processors[1].setup_seconds, Some(0.2));
    assert_eq!(report.processors[0].start_seconds, Some(0.03));
    assert_eq!(report.processors[1].start_seconds, Some(0.05));
    assert_eq!(report.processors[1].start_offset_seconds, Some(0.03));
    assert!(timing.observe(&ready(900_000_000)).is_none());
}

#[test]
fn missing_or_aborted_startup_never_becomes_a_zero_duration_success() {
    let mut timing = StartupTiming::default();
    timing.observe(&start());
    timing.observe(&PipelineObservation::ProcessorSetup {
        processor: processor(0),
        started_ns: 10,
        finished_ns: 9,
        outcome: SetupOutcome::Aborted,
    });
    let Some(StartupMeasurement::Startup(report)) = timing.observe(&ready(20)) else {
        panic!()
    };
    assert!(!report.complete);
    assert_eq!(report.processors[0].setup_seconds, None);
    assert_eq!(report.processors[0].start_seconds, None);
}

#[test]
fn transport_timing_can_arrive_during_setup_and_reports_only_once() {
    let mut timing = StartupTiming::default();
    timing.observe(&start());
    timing.observe(&push("BotConnectedFrame", 20_000_000));
    let Some(StartupMeasurement::Transport(report)) =
        timing.observe(&push("ClientConnectedFrame", 50_000_000))
    else {
        panic!()
    };
    assert_eq!(report.bot_connected_seconds, Some(0.02));
    assert_eq!(report.client_connected_seconds, 0.05);
    assert!(timing
        .observe(&push("ClientConnectedFrame", 60_000_000))
        .is_none());
}

#[test]
fn startup_pending_state_has_explicit_runtime_and_processor_bounds() {
    let mut timing = StartupTiming::new(1, 1);
    timing.observe(&start());
    timing.observe(&PipelineObservation::RuntimeStarted {
        runtime_id: 2,
        timestamp_ns: 10,
    });
    assert_eq!(timing.evicted_runtimes, 1);
    assert!(timing.observe(&ready(100)).is_none());
    timing.observe(&start());
    timing.observe(&setup(0, 0, 1));
    timing.observe(&setup(1, 0, 2));
    let Some(StartupMeasurement::Startup(report)) = timing.observe(&ready(20)) else {
        panic!()
    };
    assert!(!report.complete);
    assert_eq!(report.processors.len(), 1);
}

#[test]
fn latency_subtracts_vad_silence_and_tiles_every_observed_stage() {
    let mut latency = UserBotLatency::default();
    latency.observe(&start());
    latency.observe(&ready(1));
    latency.observe(&vad(1_000_000_000, 0.2));
    latency.observe(&push("TranscriptionFrame", 1_100_000_000));
    latency.observe(&push("UserStoppedSpeakingFrame", 1_150_000_000));
    latency.observe(&push("LLMFullResponseStartFrame", 1_200_000_000));
    latency.observe(&push("LLMTextFrame", 1_400_000_000));
    latency.observe(&push("TTSAudioRawFrame", 1_600_000_000));
    let report = latency
        .observe(&push("BotStartedSpeakingFrame", 1_800_000_000))
        .unwrap();
    assert_eq!(report.measured_from, LatencyAnchor::UserSilence);
    assert_eq!(report.seconds, 1.0);
    assert_eq!(report.user_turn_seconds, Some(0.35));
    assert_eq!(
        report.contributions.iter().map(|c| c.seconds).sum::<f64>(),
        report.seconds
    );
    assert_eq!(report.contributions[0].kind, "endpointing_wait");
    assert_eq!(report.contributions[0].seconds, 0.2);
    assert!(latency
        .observe(&push("BotStartedSpeakingFrame", 2_000_000_000))
        .is_none());
}

#[test]
fn greeting_starts_when_both_client_and_pipeline_are_ready() {
    let mut latency = UserBotLatency::default();
    latency.observe(&start());
    latency.observe(&push("ClientConnectedFrame", 100));
    latency.observe(&ready(200));
    latency.observe(&push("LLMFullResponseStartFrame", 250));
    let report = latency
        .observe(&push("BotStartedSpeakingFrame", 500))
        .unwrap();
    assert_eq!(report.measured_from, LatencyAnchor::ClientConnected);
    assert_eq!(report.started_ns, 200);
    assert_eq!(report.seconds, 300.0 / 1e9);
}

#[test]
fn user_speech_abandons_greeting_and_interruption_abandons_open_turn() {
    let mut latency = UserBotLatency::default();
    latency.observe(&start());
    latency.observe(&push("ClientConnectedFrame", 100));
    latency.observe(&push("VADUserStartedSpeakingFrame", 150));
    assert!(latency
        .observe(&push("BotStartedSpeakingFrame", 200))
        .is_none());
    latency.observe(&vad(300, 0.0));
    latency.observe(&push("InterruptionFrame", 350));
    assert!(latency
        .observe(&push("BotStartedSpeakingFrame", 400))
        .is_none());
    latency.observe(&vad(500, 0.0));
    assert!(latency
        .observe(&push("BotStartedSpeakingFrame", 600))
        .is_some());
}

#[test]
fn repeated_hops_and_upstream_speech_do_not_duplicate_latency() {
    let mut latency = UserBotLatency::default();
    latency.observe(&vad(100, 0.0));
    let mut event = push("BotStartedSpeakingFrame", 200);
    if let PipelineObservation::Push { first_push, .. } = &mut event {
        *first_push = false;
    }
    assert!(latency.observe(&event).is_none());
    if let PipelineObservation::Push {
        first_push,
        direction,
        ..
    } = &mut event
    {
        *first_push = true;
        *direction = Direction::Upstream;
    }
    assert!(latency.observe(&event).is_none());
    assert!(latency
        .observe(&push("BotStartedSpeakingFrame", 300))
        .is_some());
}

#[tokio::test]
async fn observation_loss_discards_incomplete_intervals_but_next_turn_recovers() {
    let (publisher, mut stream) = metrics_stream(8).unwrap();
    let observer = MetricsObserver::new(publisher);
    observer
        .on_event(ObservationEvent {
            sequence: 0,
            dropped_before: 0,
            observation: Arc::new(vad(100, 0.0)),
        })
        .await;
    observer
        .on_event(ObservationEvent {
            sequence: 2,
            dropped_before: 1,
            observation: Arc::new(push("BotStartedSpeakingFrame", 200)),
        })
        .await;
    assert_eq!(
        stream.try_recv().unwrap(),
        Measurement::ObservationLoss { dropped_total: 1 }
    );
    assert!(stream.try_recv().is_err());
    observer
        .on_event(ObservationEvent {
            sequence: 3,
            dropped_before: 1,
            observation: Arc::new(vad(300, 0.0)),
        })
        .await;
    observer
        .on_event(ObservationEvent {
            sequence: 4,
            dropped_before: 1,
            observation: Arc::new(push("BotStartedSpeakingFrame", 500)),
        })
        .await;
    assert!(matches!(
        stream.try_recv().unwrap(),
        Measurement::UserBotLatency(_)
    ));
}

#[tokio::test]
async fn measured_service_frames_stream_once_and_reject_invalid_numeric_values() {
    let (publisher, mut stream) = metrics_stream(8).unwrap();
    let observer = MetricsObserver::new(publisher);
    let mut event = push("MetricsFrame", 100);
    if let PipelineObservation::Push { frame, .. } = &mut event {
        frame.metrics = vec![
            Metric::Ttfb {
                processor: "service".into(),
                seconds: 0.25,
            },
            Metric::SttUsage {
                processor: "stt".into(),
                audio_seconds: f64::NAN,
            },
        ];
        frame.omitted_metrics = 3;
    }
    observer
        .on_event(ObservationEvent {
            sequence: 0,
            dropped_before: 0,
            observation: Arc::new(event.clone()),
        })
        .await;
    assert!(matches!(
        stream.try_recv().unwrap(),
        Measurement::ReportedService {
            timestamp_ns: 100,
            metric: Metric::Ttfb { seconds: 0.25, .. }
        }
    ));
    assert!(matches!(
        stream.try_recv().unwrap(),
        Measurement::TruncatedMetrics { omitted: 3, .. }
    ));
    if let PipelineObservation::Push { first_push, .. } = &mut event {
        *first_push = false;
    }
    observer
        .on_event(ObservationEvent {
            sequence: 1,
            dropped_before: 0,
            observation: Arc::new(event),
        })
        .await;
    assert!(stream.try_recv().is_err());
}

#[tokio::test]
async fn metric_stream_is_bounded_nonblocking_and_can_close_and_drain() {
    assert!(metrics_stream(0).is_err());
    let (publisher, mut stream) = metrics_stream(1).unwrap();
    assert!(publisher.publish(Measurement::ObservationLoss { dropped_total: 1 }));
    assert!(!publisher.publish(Measurement::ObservationLoss { dropped_total: 2 }));
    stream.close();
    assert!(stream.recv().await.is_some());
    assert!(stream.recv().await.is_none());
    assert!(!publisher.publish(Measurement::ObservationLoss { dropped_total: 3 }));
    assert_eq!(
        publisher.stats(),
        MetricsStreamStats {
            published: 1,
            dropped: 2
        }
    );
}
