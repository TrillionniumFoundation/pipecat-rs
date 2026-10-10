//! Native acceptance of af9f9da tests/test_video_in_sources.py sampler cases.
use pipecat_media_transport::video::*;
use std::time::Duration;
fn time(t: f64) -> Duration {
    Duration::from_secs_f64(t)
}
fn sampler(rate: Option<u32>) -> VideoInSampler<u64> {
    VideoInSampler::new(VideoInSourceParams { framerate: rate }, 8).unwrap()
}
fn passed(rate: Option<u32>, times: &[f64]) -> Vec<f64> {
    let mut sampler = sampler(rate);
    times
        .iter()
        .copied()
        .filter(|t| sampler.sample(time(*t)).0)
        .collect()
}
fn key(participant: &str, source: &str) -> VideoSourceKey {
    VideoSourceKey::new(
        VideoParticipantId::new(participant).unwrap(),
        VideoSourceId::new(source).unwrap(),
    )
}
#[test]
fn all_request_only_fifo_and_rate_updates() {
    assert_eq!(passed(None, &[1.0, 1.01, 1.02]), [1.0, 1.01, 1.02]);
    let mut sample = sampler(Some(0));
    assert!(!sample.sample(time(1.0)).0);
    sample.add_request(11).unwrap();
    sample.add_request(22).unwrap();
    assert_eq!(sample.sample(time(1.1)), (true, Some(11)));
    sample
        .set_params(VideoInSourceParams { framerate: Some(1) })
        .unwrap();
    assert_eq!(sample.sample(time(1.2)), (true, Some(22)));
    assert!(!sample.sample(time(1.3)).0);
    sample
        .set_params(VideoInSourceParams::all_frames())
        .unwrap();
    assert!(sample.sample(time(1.4)).0);
}
#[test]
fn fixed_schedule_does_not_drift_and_covers_input_jitter() {
    let times: Vec<_> = (0..300).map(|i| 10.0 + f64::from(i) / 30.0).collect();
    let samples = passed(Some(5), &times);
    assert!((samples.len() as f64 / 10.0 - 5.0).abs() <= 0.1);
    assert!(samples[1..]
        .windows(2)
        .all(|p| ((p[1] - p[0]) - 0.2).abs() < 1.0 / 30.0));
    let mut t = 10.0;
    let times: Vec<_> = (0..90)
        .map(|i| {
            t += (1.0 / 29.5) * if i % 2 == 0 { 1.2 } else { 0.8 };
            t
        })
        .collect();
    assert_eq!(passed(Some(30), &times), times);
}
#[test]
fn fresh_origin_requests_late_arrivals_and_stalls_never_burst() {
    let mut sample = sampler(Some(1));
    assert!(sample.sample(Duration::ZERO).0);
    assert!(!sample.sample(time(0.949)).0);
    assert!(sample.sample(time(0.95)).0);
    let mut sample = sampler(Some(1));
    sample.sample(time(10.0));
    sample.add_request(99).unwrap();
    assert_eq!(sample.sample(time(10.2)), (true, Some(99)));
    assert!(!sample.sample(time(11.0)).0);
    assert!(sample.sample(time(11.2)).0);
    let times: Vec<_> = (0..31)
        .map(|i| 10.0 + f64::from(i) / 30.0)
        .chain((0..60).map(|i| 11.2 + f64::from(i) / 30.0))
        .collect();
    assert!(passed(Some(1), &times)
        .windows(2)
        .all(|p| p[1] - p[0] >= 0.95));
    let times: Vec<_> = (0..30)
        .map(|i| 10.0 + f64::from(i) / 30.0)
        .chain((0..30).map(|i| 20.0 + f64::from(i) / 30.0))
        .collect();
    assert_eq!(
        passed(Some(1), &times)
            .into_iter()
            .filter(|t| *t >= 20.0 && *t < 20.9)
            .count(),
        1
    );
    let mut fast = sampler(Some(20));
    assert!(fast.sample(time(1.0)).0);
    assert!(!fast.sample(time(1.024)).0);
    assert!(fast.sample(time(1.025)).0);
}
#[test]
fn collection_isolates_sources_and_retains_requests_on_capture_updates() {
    let mut sources = VideoInSamplers::new(4, 4).unwrap();
    let camera = key("p1", "camera");
    let screen = key("p1", "screenVideo");
    let other = key("p2", "camera");
    sources
        .capture(camera.clone(), VideoInSourceParams::on_request_only())
        .unwrap();
    assert_eq!(sources.sample(&screen, time(1.0)), (false, None));
    assert_eq!(sources.sample(&other, time(1.0)), (false, None));
    assert!(sources.add_request(&screen, 1).is_err());
    sources
        .capture(screen.clone(), VideoInSourceParams::on_request_only())
        .unwrap();
    sources
        .capture(other.clone(), VideoInSourceParams::all_frames())
        .unwrap();
    sources.add_request(&camera, 1).unwrap();
    sources.add_request(&screen, 2).unwrap();
    sources
        .capture(camera.clone(), VideoInSourceParams { framerate: Some(1) })
        .unwrap();
    assert_eq!(sources.sample(&camera, time(1.0)), (true, Some(1)));
    assert_eq!(
        sources.take_requests(&screen.participant, Some(&screen.source)),
        [2]
    );
    assert!(sources.is_capturing(&screen));
    sources.add_request(&camera, 3).unwrap();
    sources.add_request(&other, 4).unwrap();
    assert_eq!(sources.remove_participant(&camera.participant), [3]);
    assert_eq!(sources.sample(&camera, time(2.0)), (false, None));
    assert_eq!(sources.sample(&screen, time(2.0)), (false, None));
    assert_eq!(sources.clear(), [4]);
    assert!(!sources.is_capturing(&other));
}
#[test]
fn capacities_identifiers_and_invalid_rates_fail_explicitly() {
    assert!(VideoParticipantId::new("").is_err());
    assert!(VideoSourceId::new("x".repeat(257)).is_err());
    assert!(VideoInSampler::<u64>::new(
        VideoInSourceParams {
            framerate: Some(u32::MAX)
        },
        1
    )
    .is_err());
    let mut sources = VideoInSamplers::new(1, 1).unwrap();
    let camera = key("p1", "camera");
    sources
        .capture(camera.clone(), VideoInSourceParams::all_frames())
        .unwrap();
    assert!(sources
        .capture(key("p2", "camera"), VideoInSourceParams::all_frames())
        .is_err());
    sources.add_request(&camera, 1).unwrap();
    assert!(sources.add_request(&camera, 2).is_err());
    assert_eq!(sources.take_requests(&camera.participant, None), [1]);
}
