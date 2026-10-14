use pipecat_core::*;
use pipecat_rtvi::*;
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;
const WAIT: Duration = Duration::from_secs(3);
fn enabled() -> RtviObserver {
    RtviObserver::new(Default::default(), None)
        .unwrap()
        .with_audio_levels(AudioLevelConfig {
            user_enabled: true,
            bot_enabled: true,
            period: Duration::from_millis(1),
            ..Default::default()
        })
        .unwrap()
}
fn pcm(rate: u32, count: usize, sample: i16) -> AudioRawFrame {
    AudioRawFrame::new(
        (0..count).flat_map(|_| sample.to_le_bytes()).collect(),
        rate,
        1,
    )
    .unwrap()
}
fn wire(observer: &RtviObserver, f: &Frame, direction: Direction) -> Vec<Value> {
    observer
        .observe(f, Observation::new("media", direction))
        .unwrap()
        .into_iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}
#[test]
fn default_off_and_disabled_streams_ignore_even_invalid_pcm() {
    let mut bad = Frame::new(FrameData::InputAudio(pcm(8000, 1, 0)));
    let FrameData::InputAudio(a) = &mut bad.data else {
        unreachable!()
    };
    a.audio.push(1);
    a.num_channels = 0;
    let default = RtviObserver::new(Default::default(), None).unwrap();
    assert!(wire(&default, &bad, Direction::Downstream).is_empty());
    let bot_only = RtviObserver::new(Default::default(), None)
        .unwrap()
        .with_audio_levels(AudioLevelConfig {
            bot_enabled: true,
            ..Default::default()
        })
        .unwrap();
    assert!(wire(&bot_only, &bad, Direction::Downstream).is_empty());
    for config in [
        AudioLevelConfig {
            period: Duration::ZERO,
            ..Default::default()
        },
        AudioLevelConfig {
            period: Duration::from_secs(61),
            ..Default::default()
        },
        AudioLevelConfig {
            max_frame_bytes: 192001,
            ..Default::default()
        },
    ] {
        assert!(RtviObserver::new(Default::default(), None)
            .unwrap()
            .with_audio_levels(config)
            .is_err());
    }
}
#[test]
fn routes_both_source_families_and_keeps_streams_independent() {
    let observer = enabled();
    let user = Frame::new(FrameData::UserAudio(UserAudioRawFrame {
        audio: pcm(48000, 19200, 3000),
        user_id: "speaker".into(),
    }));
    let original = user.clone();
    let u = wire(&observer, &user, Direction::Upstream);
    assert_eq!(u.len(), 1);
    assert_eq!(u[0]["type"], "user-audio-level");
    assert!((u[0]["data"]["value"].as_f64().unwrap() - 0.6305).abs() < 0.003);
    assert_eq!(user, original);
    assert!(wire(&observer, &user, Direction::Downstream).is_empty());
    let bot = Frame::new(FrameData::TtsAudio(pcm(24000, 9600, 0)));
    let b = wire(&observer, &bot, Direction::Downstream);
    assert_eq!(b[0]["type"], "bot-audio-level");
    assert_eq!(b[0]["data"]["value"], 0.0);
    let output = Frame::new(FrameData::OutputAudio(pcm(8000, 3200, 3000)));
    assert!(wire(&observer, &output, Direction::Downstream).is_empty());
    observer.reset();
    assert_eq!(wire(&observer, &user, Direction::Downstream).len(), 1);
}
#[test]
fn first_push_ignored_and_broadcast_filters_precede_window_updates() {
    let observer = RtviObserver::new(
        RtviObserverParams {
            ignored_sources: ["ignored".into()].into(),
            ..Default::default()
        },
        None,
    )
    .unwrap()
    .with_audio_levels(AudioLevelConfig {
        user_enabled: true,
        ..Default::default()
    })
    .unwrap();
    let mut f = Frame::new(FrameData::InputAudio(pcm(8000, 3200, 3000)));
    let mut observation = Observation::new("media", Direction::Downstream);
    observation.first_push = false;
    assert!(observer.observe(&f, observation).unwrap().is_empty());
    assert!(observer
        .observe(&f, Observation::new("ignored", Direction::Downstream))
        .unwrap()
        .is_empty());
    f.broadcast_sibling_id = Some(123);
    assert!(wire(&observer, &f, Direction::Upstream).is_empty());
    let events = wire(&observer, &f, Direction::Downstream);
    assert_eq!(events.len(), 1);
    assert!(events[0]["data"]["value"].as_f64().unwrap() > 0.6);
}
#[test]
fn malformed_selected_audio_is_atomic_and_same_identity_can_be_repaired() {
    for kind in 0..4 {
        let observer = enabled();
        let mut f = Frame::new(FrameData::InputAudio(pcm(8000, 3200, 3000)));
        let valid = f.data.clone();
        let FrameData::InputAudio(a) = &mut f.data else {
            unreachable!()
        };
        match kind {
            0 => a.audio.push(1),
            1 => a.num_channels = 2,
            2 => a.sample_rate = 44100,
            _ => a.audio = vec![0; 192002],
        };
        let err = observer
            .observe(&f, Observation::new("media", Direction::Downstream))
            .unwrap_err();
        assert_eq!(err.permanence_override, Some(false));
        f.data = valid;
        assert_eq!(wire(&observer, &f, Direction::Downstream).len(), 1);
    }
}
#[test]
fn every_short_frame_feeds_window_even_between_reports() {
    let observer = enabled();
    let mut events = Vec::new();
    for _ in 0..4 {
        events.extend(wire(
            &observer,
            &Frame::new(FrameData::InputAudio(pcm(8000, 800, 3000))),
            Direction::Downstream,
        ));
    }
    assert_eq!(events[0]["data"]["value"], 0.0);
    std::thread::sleep(Duration::from_millis(3));
    let e = wire(
        &observer,
        &Frame::new(FrameData::InputAudio(pcm(8000, 0, 0))),
        Direction::Downstream,
    );
    assert_eq!(e.len(), 1);
    assert!((e[0]["data"]["value"].as_f64().unwrap() - 0.6315).abs() < 1e-10);
}

async fn run_pipeline(observer: Arc<RtviObserver>, cancel: bool, reject_first: bool) -> Vec<Value> {
    let mut worker = Pipeline::new(vec![Arc::new(observer.tap("media"))])
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_millis(500),
            ..Default::default()
        })
        .await
        .unwrap();
    let audio = Frame::new(FrameData::InputAudio(pcm(48000, 19200, 3000)));
    let original = audio.clone();
    let body = timeout(WAIT, async {
        loop {
            let f = worker.recv().await.ok_or("closed before Start")?;
            if matches!(f.frame.data, FrameData::Start(_)) {
                break;
            }
        }
        if reject_first {
            let mut bad = audio.clone();
            let FrameData::InputAudio(a) = &mut bad.data else {
                unreachable!()
            };
            a.audio.push(1);
            worker
                .sender()
                .send(bad, Direction::Downstream)
                .await
                .map_err(|_| "invalid admission")?;
            loop {
                let f = worker.recv().await.ok_or("closed before rejection")?;
                match f.frame.data {
                    FrameData::Error(_) | FrameData::ErrorDetailed(_) => break,
                    FrameData::InputAudio(_) => return Err("invalid audio forwarded"),
                    _ => {}
                }
            }
            if !worker
                .processor_health(0)
                .ok_or("missing health")?
                .is_usable()
                || !worker.unusable_processors().is_empty()
            {
                return Err("rejection changed usability");
            }
        }
        worker
            .sender()
            .send(audio, Direction::Downstream)
            .await
            .map_err(|_| "audio admission")?;
        let mut frames = Vec::new();
        let mut output = Vec::new();
        while output.is_empty() {
            let f = worker.recv().await.ok_or("closed before level")?;
            if let FrameData::OutputTransportMessageUrgent(v) = &f.frame.data {
                if v["type"] == "user-audio-level" {
                    output.push(v.clone());
                }
            }
            frames.push(f.frame);
        }
        if !frames.contains(&original) {
            return Err("original audio not preserved");
        }
        Ok(output)
    })
    .await;
    let mut cleanup = Vec::new();
    let terminal = if cancel {
        timeout(WAIT, worker.cancel("fixture cancel")).await
    } else {
        timeout(WAIT, worker.end()).await
    };
    if !matches!(terminal, Ok(Ok(()))) {
        cleanup.push(format!("terminal: {terminal:?}"));
        let result = timeout(WAIT, worker.cancel("terminal fallback")).await;
        if !matches!(result, Ok(Ok(()))) {
            cleanup.push(format!("cancel: {result:?}"));
        }
    }
    let report = match timeout(WAIT, worker.join()).await {
        Ok(r) => r,
        Err(_) => {
            cleanup.push("join deadline".into());
            let result = timeout(WAIT, worker.cancel("join fallback")).await;
            if !matches!(result, Ok(Ok(()))) {
                cleanup.push(format!("fallback cancel: {result:?}"));
            }
            worker.join().await
        }
    };
    let report = report.unwrap();
    assert!(cleanup.is_empty(), "{cleanup:?}");
    assert_eq!(report.total_errors, u64::from(reject_first));
    assert_eq!(report.errors.len(), usize::from(reject_first));
    if reject_first {
        assert!(report.errors[0].error.contains("audio levels require"));
    }
    assert_eq!(
        report.state,
        if cancel {
            WorkerState::Cancelled
        } else {
            WorkerState::Ended
        }
    );
    body.expect("observation deadline")
        .expect("observation failed")
}
#[tokio::test]
async fn real_pipeline_serializes_level_preserves_audio_and_reuses_after_cancel() {
    let observer = Arc::new(enabled());
    let first = run_pipeline(observer.clone(), true, false).await;
    assert_eq!(first.len(), 1);
    observer.reset();
    let next = run_pipeline(observer, false, false).await;
    assert_eq!(first, next);
}

#[tokio::test]
async fn malformed_audio_rejection_keeps_real_pipeline_usable_and_same_id_retry_works() {
    let wire = run_pipeline(Arc::new(enabled()), false, true).await;
    assert_eq!(wire.len(), 1);
}
