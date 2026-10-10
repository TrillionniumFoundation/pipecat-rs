//! Deterministic native callback fixtures; these are not hardware acceptance tests.
use super::*;
use crate::capture::CaptureCallback;
use crate::playback::{OutputCallback, OutputQueue};
use cpal::{SampleFormat, StreamInstant, SupportedBufferSize, SupportedStreamConfigRange};
use pipecat_audio::{decode_pcm16_le, encode_pcm16_le, PcmFormat};
use pipecat_core::{AudioRawFrame, Frame, FrameData};
use std::sync::Mutex;

fn format() -> PcmFormat {
    PcmFormat::new(16_000, 1).unwrap()
}
fn audio(samples: &[i16]) -> AudioRawFrame {
    AudioRawFrame::new(encode_pcm16_le(samples), 16_000, 1).unwrap()
}
fn output(limits: AudioLimits) -> (OutputCallback, Arc<Mutex<OutputQueue>>, Arc<Signals>) {
    let signals = Signals::new();
    let queue = Arc::new(Mutex::new(OutputQueue::new(format(), limits)));
    (
        OutputCallback {
            queue: queue.clone(),
            signals: signals.clone(),
            format: format(),
            max_callback_frames: 65_536,
        },
        queue,
        signals,
    )
}
fn render<T: cpal::Sample + cpal::FromSample<i16>>(callback: &mut OutputCallback, data: &mut [T]) {
    callback.render(
        data,
        StreamInstant::from_millis(10),
        StreamInstant::from_millis(30),
    );
}

#[test]
fn rejects_unbounded_or_zero_settings() {
    for limits in [
        AudioLimits {
            queue_capacity: 0,
            ..AudioLimits::default()
        },
        AudioLimits {
            queue_byte_capacity: usize::MAX,
            ..AudioLimits::default()
        },
        AudioLimits {
            max_callback_frames: 0,
            ..AudioLimits::default()
        },
        AudioLimits {
            operation_timeout: Duration::ZERO,
            ..AudioLimits::default()
        },
    ] {
        assert!(LocalAudioOutput::new(DeviceSelector::Default, limits.clone()).is_err());
        assert!(LocalAudioInput::new(DeviceSelector::Default, limits).is_err());
    }
}

#[test]
fn construction_does_not_access_devices() {
    let params = LocalAudioTransportParams {
        input_device: DeviceSelector::Index(usize::MAX),
        output_device: DeviceSelector::Index(usize::MAX),
        ..Default::default()
    };
    let transport = LocalAudioTransport::new(params).unwrap();
    assert!(!transport.input_backend().is_open());
    assert!(!transport.output().is_open());
    transport.input_backend().close();
    transport.output().close();
}

#[test]
fn local_transport_rejects_video() {
    let mut params = LocalAudioTransportParams::default();
    params.input.video_enabled = true;
    assert!(LocalAudioTransport::new(params).is_err());
}

#[test]
fn exact_device_format_selection_prefers_pcm16() {
    let ranges = [
        SupportedStreamConfigRange::new(
            1,
            8_000,
            48_000,
            SupportedBufferSize::Unknown,
            SampleFormat::F32,
        ),
        SupportedStreamConfigRange::new(
            1,
            16_000,
            16_000,
            SupportedBufferSize::Unknown,
            SampleFormat::I16,
        ),
    ];
    let (config, sample) = device::choose_config(ranges, format()).unwrap();
    assert_eq!(config.sample_rate, 16_000);
    assert_eq!(sample, SampleFormat::I16);
    assert!(device::choose_config(ranges, PcmFormat::new(44_100, 2).unwrap()).is_err());
    assert!(device::choose_config(ranges, PcmFormat::new(96_000, 1).unwrap()).is_err());
}

#[test]
fn capture_reblocks_variable_callbacks_without_losing_samples() {
    let signals = Signals::new();
    let (mut callback, mut receiver) =
        CaptureCallback::new(format(), &AudioLimits::default(), signals.clone()).unwrap();
    let samples: Vec<i16> = (0..960).map(|n| n - 480).collect();
    for part in samples.chunks(113) {
        callback.receive(part);
    }
    let mut actual = vec![];
    for _ in 0..3 {
        let chunk = receiver.try_recv().unwrap();
        assert_eq!(chunk.len(), 640);
        actual.extend(decode_pcm16_le(&chunk).unwrap());
    }
    assert_eq!(samples, actual);
    assert!(receiver.try_recv().is_err());
    assert!(signals.check().is_ok());
}

#[test]
fn capture_stereo_preserves_channel_order_and_alignment() {
    let format = PcmFormat::new(16_000, 2).unwrap();
    let (mut callback, mut receiver) =
        CaptureCallback::new(format, &AudioLimits::default(), Signals::new()).unwrap();
    callback.receive(&[i16::MIN, i16::MAX].repeat(320));
    let chunk = receiver.try_recv().unwrap();
    assert_eq!(
        decode_pcm16_le(&chunk).unwrap(),
        [i16::MIN, i16::MAX].repeat(320)
    );
}

#[test]
fn capture_float_saturates_and_unsigned_centers() {
    let (mut callback, mut receiver) =
        CaptureCallback::new(format(), &AudioLimits::default(), Signals::new()).unwrap();
    callback.receive(&[-2.0f32, -1.0, 0.0, 0.5, 1.0, 2.0, 0.0, 0.0].repeat(40));
    assert_eq!(
        &decode_pcm16_le(&receiver.try_recv().unwrap()).unwrap()[..8],
        &[-32768, -32768, 0, 16384, 32767, 32767, 0, 0]
    );
    callback.receive(&[0u16, 32768, 65535, 32768].repeat(80));
    assert_eq!(
        &decode_pcm16_le(&receiver.try_recv().unwrap()).unwrap()[..4],
        &[-32768, 0, 32767, 0]
    );
}

#[test]
fn capture_rejects_nonfinite_native_data() {
    for sample in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let signals = Signals::new();
        let (mut callback, mut receiver) =
            CaptureCallback::new(format(), &AudioLimits::default(), signals.clone()).unwrap();
        callback.receive(&[sample; 320]);
        assert!(matches!(
            signals.check(),
            Err(LocalAudioError::InvalidAudio(_))
        ));
        assert!(receiver.try_recv().is_err());
    }
}

#[test]
fn capture_overflow_is_explicit_and_stops_callback_admission() {
    let limits = AudioLimits {
        queue_capacity: 1,
        ..Default::default()
    };
    let signals = Signals::new();
    let (mut callback, mut receiver) =
        CaptureCallback::new(format(), &limits, signals.clone()).unwrap();
    callback.receive(&[1i16; 320]);
    callback.receive(&[2i16; 320]);
    assert_eq!(signals.check(), Err(LocalAudioError::QueueFull));
    receiver.try_recv().unwrap();
    callback.receive(&[3i16; 320]);
    assert!(receiver.try_recv().is_err());
}

#[test]
fn capture_byte_budget_and_callback_size_are_checked() {
    let limits = AudioLimits {
        queue_byte_capacity: 1919,
        ..Default::default()
    };
    assert!(CaptureCallback::new(format(), &limits, Signals::new()).is_err());
    let signals = Signals::new();
    let (mut callback, _) = CaptureCallback::new(
        format(),
        &AudioLimits {
            max_callback_frames: 319,
            ..Default::default()
        },
        signals.clone(),
    )
    .unwrap();
    callback.receive(&[0i16; 320]);
    assert!(matches!(
        signals.check(),
        Err(LocalAudioError::InvalidAudio(_))
    ));
}

#[test]
fn capture_closed_consumer_and_stereo_misalignment_fail() {
    let signals = Signals::new();
    let (mut callback, receiver) =
        CaptureCallback::new(format(), &AudioLimits::default(), signals.clone()).unwrap();
    drop(receiver);
    callback.receive(&[0i16; 320]);
    assert_eq!(signals.check(), Err(LocalAudioError::Stopped));
    let signals = Signals::new();
    let (mut callback, _) = CaptureCallback::new(
        PcmFormat::new(16_000, 2).unwrap(),
        &AudioLimits::default(),
        signals.clone(),
    )
    .unwrap();
    callback.receive(&[1i16]);
    assert!(matches!(
        signals.check(),
        Err(LocalAudioError::InvalidAudio(_))
    ));
}

#[tokio::test]
async fn output_receipt_waits_for_all_callback_samples() {
    let (mut callback, queue, signals) = output(AudioLimits::default());
    let pending = queue
        .lock()
        .unwrap()
        .enqueue(&audio(&[1, 2, 3, 4]))
        .unwrap();
    let wait = pending.wait(signals, Duration::from_secs(1));
    tokio::pin!(wait);
    assert!(tokio::time::timeout(Duration::from_millis(1), &mut wait)
        .await
        .is_err());
    let mut first = [99i16; 2];
    render(&mut callback, &mut first);
    assert_eq!(first, [1, 2]);
    assert!(tokio::time::timeout(Duration::from_millis(1), &mut wait)
        .await
        .is_err());
    let mut last = [99i16; 4];
    render(&mut callback, &mut last);
    assert_eq!(last, [3, 4, 0, 0]);
    let receipt = wait.await.unwrap();
    assert_eq!(receipt.frames, 4);
    assert_eq!(receipt.callback_time, Duration::from_millis(10));
    assert_eq!(
        receipt.predicted_playback_end,
        Duration::from_micros(30_125)
    );
}

#[tokio::test]
async fn output_preserves_fifo_and_converts_native_types() {
    let (mut callback, queue, signals) = output(AudioLimits::default());
    let a = queue
        .lock()
        .unwrap()
        .enqueue(&audio(&[i16::MIN, 0]))
        .unwrap();
    let b = queue
        .lock()
        .unwrap()
        .enqueue(&audio(&[16384, i16::MAX]))
        .unwrap();
    let mut samples = [0.0f32; 4];
    render(&mut callback, &mut samples);
    assert_eq!(samples, [-1.0, 0.0, 0.5, 32767.0 / 32768.0]);
    assert_eq!(
        a.wait(signals.clone(), Duration::from_secs(1))
            .await
            .unwrap()
            .frames,
        2
    );
    assert_eq!(
        b.wait(signals, Duration::from_secs(1))
            .await
            .unwrap()
            .frames,
        2
    );
    let (mut callback, queue, _) = output(AudioLimits::default());
    let _pending = queue
        .lock()
        .unwrap()
        .enqueue(&audio(&[i16::MIN, 0, i16::MAX]))
        .unwrap();
    let mut samples = [0u16; 4];
    render(&mut callback, &mut samples);
    assert_eq!(samples, [0, 32768, 65535, 32768]);
}

#[test]
fn queue_count_bytes_and_inflight_bytes_are_bounded() {
    let (mut callback, queue, _) = output(AudioLimits {
        queue_capacity: 2,
        queue_byte_capacity: 8,
        ..Default::default()
    });
    let _pending = queue
        .lock()
        .unwrap()
        .enqueue(&audio(&[1, 2, 3, 4]))
        .unwrap();
    render(&mut callback, &mut [0i16; 2]);
    assert!(matches!(
        queue.lock().unwrap().enqueue(&audio(&[5])),
        Err(LocalAudioError::QueueFull)
    ));
    let (_, queue, _) = output(AudioLimits {
        queue_capacity: 1,
        ..Default::default()
    });
    let _pending = queue.lock().unwrap().enqueue(&audio(&[1])).unwrap();
    assert!(matches!(
        queue.lock().unwrap().enqueue(&audio(&[2])),
        Err(LocalAudioError::QueueFull)
    ));
}

#[test]
fn dropped_output_future_cancels_remaining_audio_and_releases_budget() {
    let (mut callback, queue, _) = output(AudioLimits {
        queue_capacity: 1,
        ..Default::default()
    });
    let pending = queue
        .lock()
        .unwrap()
        .enqueue(&audio(&[1, 2, 3, 4]))
        .unwrap();
    render(&mut callback, &mut [0i16; 2]);
    drop(pending);
    let next = queue.lock().unwrap().enqueue(&audio(&[8, 9])).unwrap();
    let mut samples = [99i16; 4];
    render(&mut callback, &mut samples);
    assert_eq!(samples, [8, 9, 0, 0]);
    drop(next);
}

#[tokio::test]
async fn timeout_cancels_audio_without_claiming_submission() {
    let (mut callback, queue, signals) = output(AudioLimits::default());
    let pending = queue.lock().unwrap().enqueue(&audio(&[1, 2])).unwrap();
    assert_eq!(
        pending.wait(signals, Duration::from_millis(1)).await,
        Err(LocalAudioError::Timeout)
    );
    let mut samples = [99i16; 2];
    render(&mut callback, &mut samples);
    assert_eq!(samples, [0, 0]);
}

#[tokio::test]
async fn stream_error_has_priority_over_buffer_acceptance() {
    let (mut callback, queue, signals) = output(AudioLimits::default());
    let pending = queue.lock().unwrap().enqueue(&audio(&[1])).unwrap();
    render(&mut callback, &mut [0i16; 1]);
    signals.fail(LocalAudioError::Device("device disconnected".into()));
    assert_eq!(
        pending.wait(signals, Duration::from_secs(1)).await,
        Err(LocalAudioError::Device("device disconnected".into()))
    );
}

#[tokio::test]
async fn cancellation_releases_waiters_and_prevents_stale_playback() {
    let (mut callback, queue, signals) = output(AudioLimits::default());
    let a = queue.lock().unwrap().enqueue(&audio(&[1, 2])).unwrap();
    let b = queue.lock().unwrap().enqueue(&audio(&[3, 4])).unwrap();
    queue.lock().unwrap().cancel_all(LocalAudioError::Cancelled);
    assert_eq!(
        a.wait(signals.clone(), Duration::from_secs(1)).await,
        Err(LocalAudioError::Cancelled)
    );
    assert_eq!(
        b.wait(signals, Duration::from_secs(1)).await,
        Err(LocalAudioError::Cancelled)
    );
    let mut samples = [99i16; 4];
    render(&mut callback, &mut samples);
    assert_eq!(samples, [0; 4]);
    assert!(matches!(
        queue.lock().unwrap().enqueue(&audio(&[5])),
        Err(LocalAudioError::Stopped)
    ));
}

#[test]
fn callback_lock_contention_fills_silence_without_dropping_queued_audio() {
    let (mut callback, queue, _) = output(AudioLimits::default());
    let _pending = queue.lock().unwrap().enqueue(&audio(&[1, 2])).unwrap();
    let guard = queue.lock().unwrap();
    let mut samples = [99i16; 2];
    render(&mut callback, &mut samples);
    assert_eq!(samples, [0, 0]);
    drop(guard);
    render(&mut callback, &mut samples);
    assert_eq!(samples, [1, 2]);
}

#[test]
fn output_rejects_mismatched_malformed_empty_and_oversized_audio() {
    let (_, queue, _) = output(AudioLimits::default());
    let mut wrong = audio(&[1]);
    wrong.sample_rate = 8000;
    assert!(queue.lock().unwrap().enqueue(&wrong).is_err());
    wrong.sample_rate = 16000;
    wrong.audio.push(0);
    assert!(queue.lock().unwrap().enqueue(&wrong).is_err());
    assert!(queue.lock().unwrap().enqueue(&audio(&[])).is_err());
    let (mut callback, _, signals) = output(AudioLimits::default());
    callback.max_callback_frames = 1;
    let mut data = [99i16; 2];
    render(&mut callback, &mut data);
    assert_eq!(data, [0, 0]);
    assert!(matches!(
        signals.check(),
        Err(LocalAudioError::InvalidAudio(_))
    ));
}

#[tokio::test]
async fn closed_output_and_wrong_frame_types_fail_explicitly() {
    let output = LocalAudioOutput::new(DeviceSelector::Default, AudioLimits::default()).unwrap();
    assert_eq!(
        output.write_audio(&audio(&[1])).await,
        Err(LocalAudioError::Stopped)
    );
    assert_eq!(output.interrupt(), Err(LocalAudioError::Stopped));
    assert!(output
        .write_frame(&Frame::new(FrameData::InputAudio(audio(&[1]))))
        .await
        .is_err());
    let mut frame = Frame::new(FrameData::OutputAudio(audio(&[1])));
    frame.transport_destination = Some("other-speaker".into());
    assert!(output.write_frame(&frame).await.is_err());
}

#[test]
fn first_native_failure_is_preserved_during_shutdown() {
    let signals = Signals::new();
    signals.fail(LocalAudioError::Device("first".into()));
    signals.fail(LocalAudioError::Stopped);
    assert_eq!(
        signals.check(),
        Err(LocalAudioError::Device("first".into()))
    );
}

#[test]
fn native_callback_panics_are_contained_and_invalidate_the_stream() {
    let signals = Signals::new();
    assert!(!guard_callback(&signals, || panic!(
        "deterministic callback failure"
    )));
    assert_eq!(
        signals.check(),
        Err(LocalAudioError::Device(
            "native audio callback panicked".into()
        ))
    );
}

#[tokio::test]
async fn poisoned_callback_state_propagates_failure_and_shutdown_recovers() {
    let (mut callback, queue, signals) = output(AudioLimits::default());
    let pending = lock(&queue).enqueue(&audio(&[1, 2])).unwrap();
    assert!(!guard_callback(&signals, || {
        let _guard = queue.lock().unwrap();
        panic!("deterministic panic while owning callback state");
    }));
    assert!(queue.is_poisoned());
    let mut samples = [99i16; 2];
    render(&mut callback, &mut samples);
    assert_eq!(samples, [0, 0]);
    lock(&queue).cancel_all(LocalAudioError::Stopped);
    assert!(matches!(
        pending.wait(signals, Duration::from_secs(1)).await,
        Err(LocalAudioError::Device(_))
    ));
}

#[tokio::test]
async fn output_cancellation_wakes_a_blocked_writer() {
    let (_, queue, signals) = output(AudioLimits::default());
    let pending = lock(&queue).enqueue(&audio(&[1, 2])).unwrap();
    let wait = pending.wait(signals.clone(), Duration::from_secs(1));
    tokio::pin!(wait);
    assert!(tokio::time::timeout(Duration::from_millis(1), &mut wait)
        .await
        .is_err());
    signals.fail(LocalAudioError::Cancelled);
    lock(&queue).cancel_all(LocalAudioError::Cancelled);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(50), wait)
            .await
            .unwrap(),
        Err(LocalAudioError::Cancelled)
    );
}

#[test]
fn all_native_sample_types_preserve_exact_representable_pcm() {
    fn roundtrip<T>()
    where
        T: cpal::SizedSample + cpal::FromSample<i16>,
        f64: cpal::FromSample<T>,
    {
        let samples = [-32768, -16384, 0, 16384].repeat(80);
        let (mut callback, queue, _) = output(AudioLimits::default());
        let _pending = lock(&queue).enqueue(&audio(&samples)).unwrap();
        let mut native = vec![T::EQUILIBRIUM; 320];
        render(&mut callback, &mut native);
        let (mut capture, mut receiver) =
            CaptureCallback::new(format(), &AudioLimits::default(), Signals::new()).unwrap();
        capture.receive(&native);
        assert_eq!(
            decode_pcm16_le(&receiver.try_recv().unwrap()).unwrap(),
            samples
        );
    }
    roundtrip::<i8>();
    roundtrip::<i16>();
    roundtrip::<cpal::I24>();
    roundtrip::<i32>();
    roundtrip::<i64>();
    roundtrip::<u8>();
    roundtrip::<u16>();
    roundtrip::<cpal::U24>();
    roundtrip::<u32>();
    roundtrip::<u64>();
    roundtrip::<f32>();
    roundtrip::<f64>();
}

// A conversion hook schedules cancellation at an exact point in the same
// callback engine. Native device tests continue to use only CPAL sample types.
mod cancellation_sample {
    use std::cell::RefCell;

    thread_local! {
        static CONVERT: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
    }

    #[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
    pub(super) struct Sample(pub(super) i16);

    pub(super) fn on_next_conversion(callback: impl FnOnce() + 'static) {
        CONVERT.with(|slot| *slot.borrow_mut() = Some(Box::new(callback)));
    }

    impl cpal::Sample for Sample {
        type Signed = i16;
        type Float = f64;
        const EQUILIBRIUM: Self = Self(0);
    }
    impl cpal::FromSample<i16> for Sample {
        fn from_sample_(sample: i16) -> Self {
            CONVERT.with(|slot| {
                if let Some(callback) = slot.borrow_mut().take() {
                    callback();
                }
            });
            Self(sample)
        }
    }
    impl cpal::FromSample<Sample> for i16 {
        fn from_sample_(sample: Sample) -> Self {
            sample.0
        }
    }
    impl cpal::FromSample<Sample> for f64 {
        fn from_sample_(sample: Sample) -> Self {
            f64::from(sample.0) / 32768.0
        }
    }
    impl cpal::FromSample<f64> for Sample {
        fn from_sample_(sample: f64) -> Self {
            Self((sample * 32768.0) as i16)
        }
    }
}

#[test]
fn cancellation_during_callback_stops_at_next_interleaved_frame() {
    use cancellation_sample::{on_next_conversion, Sample};
    for channels in [1, 2] {
        let (mut callback, queue, _) = output(AudioLimits::default());
        let format = PcmFormat::new(16_000, channels).unwrap();
        callback.format = format;
        *lock(&queue) = OutputQueue::new(format, AudioLimits::default());
        let audio = AudioRawFrame::new(encode_pcm16_le(&[1, 2, 3, 4]), 16_000, channels).unwrap();
        let pending = lock(&queue).enqueue(&audio).unwrap();
        on_next_conversion(move || drop(pending));
        let mut samples = [Sample(99); 4];
        render(&mut callback, &mut samples);
        let expected = if channels == 1 {
            [1, 0, 0, 0]
        } else {
            [1, 2, 0, 0]
        };
        assert_eq!(samples.map(|sample| sample.0), expected);
        let mut next = [99i16; 4];
        render(&mut callback, &mut next);
        assert_eq!(next, [0; 4]);
    }
}

#[test]
fn native_failure_during_callback_stops_at_next_interleaved_frame() {
    use cancellation_sample::{on_next_conversion, Sample};
    let (mut callback, queue, signals) = output(AudioLimits::default());
    let _pending = lock(&queue).enqueue(&audio(&[1, 2, 3, 4])).unwrap();
    on_next_conversion(move || signals.fail(LocalAudioError::Device("disconnected".into())));
    let mut samples = [Sample(99); 4];
    render(&mut callback, &mut samples);
    assert_eq!(samples.map(|sample| sample.0), [1, 0, 0, 0]);
}
