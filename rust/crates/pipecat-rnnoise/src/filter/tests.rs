//! Orchestration tests use synthetic engines; they do not demonstrate denoising.
use super::*;
use crate::{RateConversion, FRAME_SAMPLES};
use pipecat_audio::{decode_pcm16_le, encode_pcm16_le};
use std::sync::{atomic::AtomicUsize, Condvar};

struct CountingEngine(usize);
impl Engine for CountingEngine {
    fn process(&mut self, samples: &[i16; FRAME_SAMPLES]) -> Result<[i16; FRAME_SAMPLES]> {
        self.0 += 1;
        Ok(samples.map(|sample| sample.saturating_add(self.0 as i16)))
    }
}
fn config() -> RnNoiseConfig {
    RnNoiseConfig::new("/unused/trusted.so", "0".repeat(64))
}
fn filter() -> RnNoiseFilter {
    RnNoiseFilter::with_factory(config(), Box::new(|_| Ok(Box::new(CountingEngine(0))))).unwrap()
}
fn format() -> PcmFormat {
    PcmFormat::new(48_000, 1).unwrap()
}
fn pcm(samples: usize) -> Vec<u8> {
    encode_pcm16_le(&vec![100; samples])
}

#[test]
fn rejects_invalid_configuration_before_starting_worker() {
    let mut cfg = config();
    cfg.library_path = "relative.so".into();
    assert!(RnNoiseFilter::new(cfg).is_err());
    let mut cfg = config();
    cfg.library_sha256 = "z".repeat(64);
    assert!(RnNoiseFilter::new(cfg).is_err());
    for maximum in [0, 192_001] {
        let mut cfg = config();
        cfg.max_input_bytes = maximum;
        assert!(RnNoiseFilter::new(cfg).is_err());
    }
}
#[tokio::test]
async fn initialization_failure_is_explicit() {
    let filter = RnNoiseFilter::with_factory(
        config(),
        Box::new(|_| Err(RnNoiseError::Native("missing ABI".into()))),
    )
    .unwrap();
    assert!(filter
        .start(format())
        .await
        .unwrap_err()
        .message
        .contains("missing ABI"));
    assert!(filter
        .filter(pcm(480))
        .await
        .unwrap_err()
        .message
        .contains("not started"));
}
#[cfg(not(feature = "native"))]
#[tokio::test]
async fn disabled_native_feature_never_passes_audio_as_denoised() {
    let filter = RnNoiseFilter::new(config()).unwrap();
    assert!(filter
        .start(format())
        .await
        .unwrap_err()
        .message
        .contains("native feature is disabled"));
}
#[tokio::test]
async fn rejects_stereo_unsupported_rates_and_oversized_input() {
    let filter = filter();
    assert!(filter
        .start(PcmFormat::new(48_000, 2).unwrap())
        .await
        .is_err());
    assert!(filter
        .start(PcmFormat::new(16_000, 1).unwrap())
        .await
        .is_err());
    filter.start(format()).await.unwrap();
    assert!(filter
        .filter(vec![0; 96_001])
        .await
        .unwrap_err()
        .message
        .contains("byte budget"));
    assert_eq!(
        filter.filter(pcm(480)).await.unwrap(),
        encode_pcm16_le(&vec![101; 480])
    );
}
#[tokio::test]
async fn arbitrary_byte_partitions_buffer_only_complete_native_frames() {
    let input = pcm(480 * 5 + 479);
    let whole = filter();
    whole.start(format()).await.unwrap();
    let expected = whole.filter(input.clone()).await.unwrap();
    assert_eq!(expected.len(), 480 * 5 * 2);
    for chunk_bytes in [1, 3, 137, 959, 960, 961, 2001] {
        let split = filter();
        split.start(format()).await.unwrap();
        let mut actual = Vec::new();
        for chunk in input.chunks(chunk_bytes) {
            actual.extend(split.filter(chunk.to_vec()).await.unwrap());
        }
        assert_eq!(actual, expected, "partition {chunk_bytes}");
        assert!(split.filter(Vec::new()).await.unwrap().is_empty());
        split.stop().await.unwrap();
    }
}
#[tokio::test]
async fn toggles_preserve_pending_samples_and_engine_history() {
    let filter = filter();
    filter.start(format()).await.unwrap();
    assert!(filter.filter(pcm(100)).await.unwrap().is_empty());
    filter
        .control(&FrameData::FilterEnable(false))
        .await
        .unwrap();
    let bypass = vec![5, 8, 13];
    assert_eq!(filter.filter(bypass.clone()).await.unwrap(), bypass);
    filter.control(&FrameData::FilterControl).await.unwrap();
    filter
        .control(&FrameData::FilterEnable(true))
        .await
        .unwrap();
    assert_eq!(
        decode_pcm16_le(&filter.filter(pcm(380)).await.unwrap()).unwrap(),
        vec![101; 480]
    );
    assert_eq!(
        decode_pcm16_le(&filter.filter(pcm(480)).await.unwrap()).unwrap(),
        vec![102; 480]
    );
}
#[tokio::test]
async fn stop_and_restart_discard_tail_and_reset_engine_but_keep_toggle() {
    let filter = filter();
    filter.start(format()).await.unwrap();
    filter.filter(pcm(500)).await.unwrap();
    filter.stop().await.unwrap();
    filter.stop().await.unwrap();
    assert!(filter.filter(pcm(480)).await.is_err());
    filter.start(format()).await.unwrap();
    assert!(filter.filter(pcm(460)).await.unwrap().is_empty());
    assert_eq!(
        filter.filter(pcm(20)).await.unwrap(),
        encode_pcm16_le(&vec![101; 480])
    );
    filter
        .control(&FrameData::FilterEnable(false))
        .await
        .unwrap();
    filter.stop().await.unwrap();
    filter.start(format()).await.unwrap();
    assert_eq!(filter.filter(pcm(7)).await.unwrap(), pcm(7));
}
#[tokio::test]
async fn explicit_sinc_resampling_is_partition_invariant_and_buffers_empty_output() {
    for rate in [8_000, 16_000, 24_000, 44_100, 96_000] {
        let mut cfg = config();
        cfg.rate_conversion = RateConversion::NativeSinc;
        let input: Vec<i16> = (0..rate / 5).map(|i| (i % 200) as i16 * 50).collect();
        let bytes = encode_pcm16_le(&input);
        let whole =
            RnNoiseFilter::with_factory(cfg.clone(), Box::new(|_| Ok(Box::new(CountingEngine(0)))))
                .unwrap();
        whole.start(PcmFormat::new(rate, 1).unwrap()).await.unwrap();
        let expected = whole.filter(bytes.clone()).await.unwrap();
        assert!(expected.len() > bytes.len() * 8 / 10);
        let split = RnNoiseFilter::with_factory(cfg, Box::new(|_| Ok(Box::new(CountingEngine(0)))))
            .unwrap();
        split.start(PcmFormat::new(rate, 1).unwrap()).await.unwrap();
        assert!(split.filter(bytes[..1].to_vec()).await.unwrap().is_empty());
        let mut actual = Vec::new();
        for chunk in bytes[1..].chunks(317) {
            actual.extend(split.filter(chunk.to_vec()).await.unwrap());
        }
        assert_eq!(actual, expected, "rate {rate}");
    }
}

struct BlockingEngine {
    entered: std::sync::mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
    dropped: Arc<AtomicUsize>,
}
impl Engine for BlockingEngine {
    fn process(&mut self, samples: &[i16; FRAME_SAMPLES]) -> Result<[i16; FRAME_SAMPLES]> {
        self.entered.send(()).unwrap();
        let (lock, changed) = &*self.release;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        Ok(*samples)
    }
}
impl Drop for BlockingEngine {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_fences_reply_stop_waits_and_releases_native_state() {
    let (entered, receive) = std::sync::mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let dropped = Arc::new(AtomicUsize::new(0));
    let factory_release = release.clone();
    let factory_dropped = dropped.clone();
    let filter = Arc::new(
        RnNoiseFilter::with_factory(
            config(),
            Box::new(move |_| {
                Ok(Box::new(BlockingEngine {
                    entered: entered.clone(),
                    release: factory_release.clone(),
                    dropped: factory_dropped.clone(),
                }))
            }),
        )
        .unwrap(),
    );
    filter.start(format()).await.unwrap();
    let task_filter = filter.clone();
    let task = tokio::spawn(async move { task_filter.filter(pcm(960)).await });
    receive
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    assert!(filter
        .filter(pcm(1))
        .await
        .unwrap_err()
        .message
        .contains("admitted request"));
    task.abort();
    let _ = task.await;
    let stop_filter = filter.clone();
    let stop = tokio::spawn(async move { stop_filter.stop().await });
    tokio::task::yield_now().await;
    assert!(!stop.is_finished());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    stop.await.unwrap().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(
        receive.try_recv().is_err(),
        "cancel must prevent the second native frame"
    );
    filter.start(format()).await.unwrap();
    assert_eq!(filter.filter(pcm(480)).await.unwrap(), pcm(480));
}
#[tokio::test]
async fn dropped_completed_reply_invalidates_stream_and_holds_admission_until_collected() {
    let filter = filter();
    filter.start(format()).await.unwrap();
    let permit = filter.admission.clone().try_acquire_owned().unwrap();
    let completion = Arc::new(Completion::new(permit));
    let (reply, receive) = oneshot::channel();
    filter
        .sender
        .as_ref()
        .unwrap()
        .try_send(Request {
            operation: Some(Operation::Filter(pcm(100))),
            completion: completion.clone(),
            reply: Some(reply),
        })
        .unwrap();
    receive.await.unwrap();
    assert!(filter
        .filter(pcm(480))
        .await
        .unwrap_err()
        .message
        .contains("admitted request"));
    completion.cancel();
    assert!(filter
        .filter(pcm(480))
        .await
        .unwrap_err()
        .message
        .contains("not started"));
    filter.start(format()).await.unwrap();
    assert!(filter.filter(pcm(380)).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_filter_joins_abandoned_native_work() {
    let (entered, receive) = std::sync::mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let dropped = Arc::new(AtomicUsize::new(0));
    let factory_release = release.clone();
    let factory_dropped = dropped.clone();
    let filter = Arc::new(
        RnNoiseFilter::with_factory(
            config(),
            Box::new(move |_| {
                Ok(Box::new(BlockingEngine {
                    entered: entered.clone(),
                    release: factory_release.clone(),
                    dropped: factory_dropped.clone(),
                }))
            }),
        )
        .unwrap(),
    );
    filter.start(format()).await.unwrap();
    let task_filter = filter.clone();
    let task = tokio::spawn(async move { task_filter.filter(pcm(960)).await });
    receive
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    task.abort();
    let _ = task.await;
    let (joining, join_started) = std::sync::mpsc::channel();
    let (finished, finish_receive) = std::sync::mpsc::channel();
    let dropper = std::thread::spawn(move || {
        joining.send(()).unwrap();
        drop(filter);
        finished.send(()).unwrap();
    });
    join_started
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    assert!(finish_receive.try_recv().is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    finish_receive
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    dropper.join().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(receive.try_recv().is_err());
}

#[tokio::test]
async fn byte_stream_extension_reassembles_odd_fragments_and_stop_discards_odd_tail() {
    let filter = filter();
    filter.start(format()).await.unwrap();
    assert!(filter.filter(pcm(100)).await.unwrap().is_empty());
    assert!(filter.filter(vec![0x34]).await.unwrap().is_empty());
    let mut continuation = vec![0x12];
    continuation.extend(pcm(379));
    let actual = decode_pcm16_le(&filter.filter(continuation).await.unwrap()).unwrap();
    let mut expected = vec![101; 100];
    expected.push(0x1235);
    expected.extend(vec![101; 379]);
    assert_eq!(actual, expected);
    assert!(filter.filter(vec![0xab]).await.unwrap().is_empty());
    assert!(filter.filter(Vec::new()).await.unwrap().is_empty());
    filter.stop().await.unwrap();
    filter.start(format()).await.unwrap();
    assert!(filter.filter(pcm(479)).await.unwrap().is_empty());
    assert_eq!(
        filter.filter(pcm(1)).await.unwrap(),
        encode_pcm16_le(&vec![101; 480])
    );
}

#[tokio::test]
async fn lifecycle_revokes_completed_uncollected_audio_and_late_poll_is_cancelled() {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    for restart in [false, true] {
        let (entered, _receive) = std::sync::mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let factory_release = release.clone();
        let filter = RnNoiseFilter::with_factory(
            config(),
            Box::new(move |_| {
                Ok(Box::new(BlockingEngine {
                    entered: entered.clone(),
                    release: factory_release.clone(),
                    dropped: Arc::new(AtomicUsize::new(0)),
                }))
            }),
        )
        .unwrap();
        filter.start(format()).await.unwrap();
        let mut future = Box::pin(filter.filter(pcm(480)));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        let completion = filter.current.lock().unwrap().upgrade().unwrap();
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if completion.slot.lock().unwrap().completed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(completion.slot.lock().unwrap().result.is_some());
        // Never poll or drop the audio future while the lifecycle operation runs.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            if restart {
                filter.start(format()).await
            } else {
                filter.stop().await
            }
        })
        .await
        .expect("uncollected audio must not retain lifecycle admission")
        .unwrap();
        assert!(completion.slot.lock().unwrap().result.is_none());
        assert!(future.await.unwrap_err().message.contains("cancelled"));
        if restart {
            assert_eq!(filter.filter(pcm(480)).await.unwrap(), pcm(480));
        } else {
            assert!(filter.filter(pcm(480)).await.is_err());
        }
    }
}

#[tokio::test]
async fn worker_panic_releases_admission_and_stop_reports_failure_without_hanging() {
    struct PanicEngine;
    impl Engine for PanicEngine {
        fn process(&mut self, _: &[i16; FRAME_SAMPLES]) -> Result<[i16; FRAME_SAMPLES]> {
            panic!("synthetic worker panic")
        }
    }
    let filter =
        RnNoiseFilter::with_factory(config(), Box::new(|_| Ok(Box::new(PanicEngine)))).unwrap();
    filter.start(format()).await.unwrap();
    assert!(filter.filter(pcm(480)).await.is_err());
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), filter.stop())
            .await
            .unwrap()
            .is_err()
    );
}

#[tokio::test]
async fn overlapping_unpolled_lifecycle_returns_explicit_error_instead_of_waiting() {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let factory_release = release.clone();
    let filter = RnNoiseFilter::with_factory(
        config(),
        Box::new(move |_| {
            let mut released = factory_release.0.lock().unwrap();
            while !*released {
                released = factory_release.1.wait(released).unwrap();
            }
            Ok(Box::new(CountingEngine(0)))
        }),
    )
    .unwrap();
    let mut start = Box::pin(filter.start(format()));
    assert!(matches!(
        start.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    let completion = filter.current.lock().unwrap().upgrade().unwrap();
    *release.0.lock().unwrap() = true;
    release.1.notify_all();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !completion.slot.lock().unwrap().completed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), filter.stop())
            .await
            .unwrap()
            .unwrap_err()
            .message
            .contains("lifecycle operation")
    );
    start.await.unwrap();
    filter.stop().await.unwrap();
}
