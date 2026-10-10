use super::*;
use serde_json::json;

fn audio(rate: u32, count: usize, channels: u16) -> AudioRawFrame {
    AudioRawFrame::new(
        (0..count)
            .flat_map(|n| {
                (0..channels).flat_map(move |_| ((n as i16).wrapping_mul(17)).to_le_bytes())
            })
            .collect(),
        rate,
        channels,
    )
    .unwrap()
}
fn convert(parts: &[AudioRawFrame]) -> Vec<u8> {
    let mut stream = audio::Conversion::new(&parts[0], 0, true).unwrap();
    let mut bytes = vec![];
    for part in parts {
        stream.push(part).unwrap();
        while let Some(packet) = stream.packet() {
            bytes.extend(packet.audio);
        }
    }
    stream.finish().unwrap();
    while let Some(packet) = stream.packet() {
        bytes.extend(packet.audio);
    }
    bytes
}
#[test]
fn arbitrary_supported_rates_have_exact_duration_and_streaming_chunk_invariance() {
    for rate in [
        8000, 11025, 12000, 16000, 22050, 24000, 32000, 44100, 44117, 48000, 88200, 96000, 192000,
    ] {
        let input = audio(rate, rate as usize / 10, 2);
        let whole = convert(std::slice::from_ref(&input));
        let mut parts = vec![];
        for bytes in input.audio.chunks(4 * 37) {
            parts.push(AudioRawFrame::new(bytes.to_vec(), rate, 2).unwrap());
        }
        assert_eq!(
            convert(&parts),
            whole,
            "chunk-dependent conversion at {rate}"
        );
        let expected_samples =
            (input.num_frames() as u64 * 48000 + u64::from(rate) / 2) / u64::from(rate);
        assert_eq!(
            whole.len(),
            (expected_samples as usize * 2).next_multiple_of(1920),
            "duration at {rate}"
        );
        assert!(whole[expected_samples as usize * 2..]
            .iter()
            .all(|byte| *byte == 0));
    }
}
#[test]
fn downmix_uses_wide_sum_and_preserves_full_scale_dc() {
    for channels in 1..=8 {
        let pcm = [32767i16.to_le_bytes()]
            .repeat(960 * channels as usize)
            .into_iter()
            .flatten()
            .collect();
        let converted = convert(&[AudioRawFrame::new(pcm, 48000, channels).unwrap()]);
        assert_eq!(
            converted,
            [32767i16.to_le_bytes()]
                .repeat(960)
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
        );
    }
}
#[test]
fn invalid_rates_channels_alignment_and_duration_are_explicit() {
    for rate in [0, 7999, 192001, u32::MAX] {
        assert!(audio::validate_rate(rate).is_err());
    }
    let mut pcm = audio(16000, 1, 1);
    pcm.audio.push(0);
    let error = audio::validate_audio(&pcm).unwrap_err();
    assert!(matches!(error, Error::InvalidAudio(_)));
    assert_eq!(
        processor_error(error, false).category,
        ErrorCategory::InvalidRequest
    );
    pcm.audio.clear();
    pcm.num_channels = 0;
    assert_eq!(
        processor_error(audio::validate_audio(&pcm).unwrap_err(), false).category,
        ErrorCategory::InvalidRequest
    );
    assert!(audio::validate_audio(&audio(16000, 1, 9)).is_err());
    assert!(audio::validate_audio(&audio(16000, 16001, 1)).is_err());
}
#[test]
fn independent_context_and_generation_boundaries_do_not_match() {
    let mut pcm = audio(22050, 25, 1);
    let stream = audio::Conversion::new(&pcm, 7, true).unwrap();
    assert!(stream.matches(&pcm, 7, true));
    assert!(!stream.matches(&pcm, 8, true));
    assert!(!stream.matches(&pcm, 7, false));
    pcm.context_id = Some("next".into());
    assert!(!stream.matches(&pcm, 7, true));
}
fn message(n: usize) -> Frame {
    Frame::new(FrameData::OutputTransportMessage(json!({"n":n})))
}
#[tokio::test]
async fn bounded_queue_waits_for_capacity_and_end_drains_in_order() {
    let q = Arc::new(queue::Queue::new(
        &MoqTransportConfig {
            queue_capacity: 1,
            ..Default::default()
        },
        0,
    ));
    q.push(message(1), 0, false).await.unwrap();
    let mut waiting = Box::pin(q.push(message(2), 0, false));
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut waiting)
            .await
            .is_err()
    );
    let mut generation = 0;
    assert!(matches!(
        q.next(&mut generation).await,
        queue::Next::Item(_)
    ));
    waiting.await.unwrap();
    q.end();
    assert!(q.push(message(3), 0, false).await.is_err());
    assert!(matches!(
        q.next(&mut generation).await,
        queue::Next::Item(_)
    ));
    assert!(matches!(q.next(&mut generation).await, queue::Next::End));
}
#[tokio::test]
async fn byte_pressure_urgent_fail_fast_and_admission_timeout_are_bounded() {
    let q = queue::Queue::new(
        &MoqTransportConfig {
            queue_capacity: 1,
            admission_timeout: Duration::from_millis(10),
            ..Default::default()
        },
        0,
    );
    q.push(message(1), 0, false).await.unwrap();
    assert_eq!(q.push(message(2), 0, true).await, Err(Error::Backpressure));
    assert_eq!(
        q.push(message(2), 0, false).await,
        Err(Error::Timeout("pipeline admission"))
    );
    let oversized = Frame::new(FrameData::OutputTransportMessage(
        json!({"huge":"x".repeat(262144)}),
    ));
    assert_eq!(
        q.push(oversized, 0, true).await,
        Err(Error::Limit("pipeline frame bytes"))
    );
}
#[tokio::test]
async fn queue_interrupt_removes_old_frames_retains_uninterruptible_and_wakes_waiter() {
    let q = queue::Queue::new(
        &MoqTransportConfig {
            queue_capacity: 2,
            ..Default::default()
        },
        0,
    );
    q.push(message(1), 0, false).await.unwrap();
    let mut retained = message(2);
    retained.interruptible = false;
    q.push(retained, 0, false).await.unwrap();
    let mut waiting = Box::pin(q.push(message(3), 0, false));
    assert!(tokio::time::timeout(Duration::from_millis(5), &mut waiting)
        .await
        .is_err());
    q.interrupt(1);
    waiting.await.unwrap();
    let mut generation = 0;
    assert!(matches!(
        q.next(&mut generation).await,
        queue::Next::Interrupted(1)
    ));
    let queue::Next::Item(retained) = q.next(&mut generation).await else {
        panic!("missing survivor")
    };
    assert!(!retained.frame.interruptible);
    q.end();
    assert!(matches!(q.next(&mut generation).await, queue::Next::End));
}
#[tokio::test]
async fn cancelled_queue_releases_backpressured_admission_without_waiting_for_timeout() {
    let q = queue::Queue::new(
        &MoqTransportConfig {
            queue_capacity: 1,
            ..Default::default()
        },
        0,
    );
    q.push(message(1), 0, false).await.unwrap();
    let waiting = q.push(message(2), 0, false);
    tokio::pin!(waiting);
    assert!(tokio::time::timeout(Duration::from_millis(5), &mut waiting)
        .await
        .is_err());
    q.cancel();
    assert_eq!(waiting.await, Err(Error::Closed));
}
#[test]
fn rich_controls_and_timed_or_routed_audio_fail_explicitly() {
    let mut pcm = Frame::new(FrameData::TtsAudio(audio(22050, 37, 1)));
    pcm.pts = Some(1);
    assert_eq!(
        validate_output(&pcm),
        Err(Error::Unsupported("scheduled pipeline output PTS"))
    );
    pcm.pts = None;
    pcm.transport_destination = Some("other".into());
    assert_eq!(
        validate_output(&pcm),
        Err(Error::Unsupported("MoQ output destinations"))
    );
    for data in [
        FrameData::Volume(1.0),
        FrameData::MixerEnable(true),
        FrameData::FilterEnable(true),
    ] {
        assert_eq!(
            validate_output(&Frame::new(data)),
            Err(Error::Unsupported("rich media output control"))
        );
    }
}
#[test]
fn invalid_pipeline_settings_fail_before_connection_use() {
    for config in [
        MoqTransportConfig {
            queue_capacity: 0,
            ..Default::default()
        },
        MoqTransportConfig {
            input_sample_rate: Some(100),
            ..Default::default()
        },
        MoqTransportConfig {
            operation_timeout: Duration::ZERO,
            ..Default::default()
        },
    ] {
        assert!(config.validate().is_err());
    }
}

#[test]
fn local_continuation_is_budgeted_and_preserved_without_wire_serialization() {
    let continuation = Arc::new(
        pipecat_core::NativeLlmContinuation::start(
            1,
            pipecat_core::LlmContinuationProvider::Anthropic,
        )
        .unwrap(),
    );
    let mut frame = Frame::new(FrameData::LlmResponseStart);
    frame
        .set_native_llm_continuation(continuation.clone())
        .unwrap();
    assert!(serde_json::to_vec(&frame).is_err());
    let size = queue::frame_size(&frame, 4096).unwrap();
    assert!(size >= continuation.retained_bytes());
    assert!(Arc::ptr_eq(
        frame.native_llm_continuation().unwrap().unwrap(),
        &continuation
    ));
    assert!(queue::frame_size(&frame, 1).is_err());
}
#[tokio::test]
async fn queue_catches_up_to_generation_changed_by_cancelled_publication() {
    let q = queue::Queue::new(&MoqTransportConfig::default(), 0);
    q.push(message(1), 0, false).await.unwrap();
    q.push(message(2), 1, false).await.unwrap();
    let mut generation = 0;
    assert!(matches!(
        q.next(&mut generation).await,
        queue::Next::Interrupted(1)
    ));
    let queue::Next::Item(item) = q.next(&mut generation).await else {
        panic!("missing new-generation item")
    };
    assert_eq!(item.generation, 1);
    q.end();
    assert!(matches!(q.next(&mut generation).await, queue::Next::End));
}
