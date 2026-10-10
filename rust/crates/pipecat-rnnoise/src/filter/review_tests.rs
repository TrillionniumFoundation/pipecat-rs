//! Independent adversarial ownership checks using synthetic engines only.
use super::*;
use crate::FRAME_SAMPLES;
use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicUsize, Barrier, Condvar},
    task::{Context, Poll, Waker},
    time::Duration,
};

fn config() -> RnNoiseConfig {
    RnNoiseConfig::new("/unused/trusted.so", "0".repeat(64))
}
fn format() -> PcmFormat {
    PcmFormat::new(48_000, 1).unwrap()
}
fn pcm() -> Vec<u8> {
    pipecat_audio::encode_pcm16_le(&vec![123; FRAME_SAMPLES])
}
struct Probe {
    calls: usize,
    dropped: Arc<AtomicUsize>,
    process_gate: Option<Arc<Gate>>,
    drop_gate: Option<Arc<Gate>>,
}
impl Engine for Probe {
    fn process(&mut self, samples: &[i16; FRAME_SAMPLES]) -> Result<[i16; FRAME_SAMPLES]> {
        if let Some(gate) = &self.process_gate {
            gate.wait();
        }
        self.calls += 1;
        Ok(samples.map(|sample| sample + self.calls as i16))
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        if let Some(gate) = &self.drop_gate {
            gate.wait();
        }
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Default)]
struct Gate(Mutex<bool>, Condvar, AtomicBool);
impl Gate {
    fn wait(&self) {
        self.2.store(true, Ordering::Release);
        let mut open = self.0.lock().unwrap();
        while !*open {
            open = self.1.wait(open).unwrap();
        }
    }
    fn open(&self) {
        *self.0.lock().unwrap() = true;
        self.1.notify_all();
    }
}
// Declare after the filter so failed assertions cannot leave Drop blocked.
struct Release(Arc<Gate>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.open();
    }
}
fn assert_pending<T>(future: Pin<&mut impl Future<Output = T>>) {
    assert!(matches!(
        future.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}
async fn completed(completion: &Completion) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !completion.slot.lock().unwrap().completed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker acknowledgement");
}

#[test]
fn concurrent_slot_completion_and_cancellation_revoke_pcm_and_release_once() {
    for _ in 0..64 {
        let admission = Arc::new(Semaphore::new(1));
        let completion = Arc::new(Completion::new(
            admission.clone().try_acquire_owned().unwrap(),
        ));
        let barrier = Arc::new(Barrier::new(2));
        let worker_slot = completion.clone();
        let worker_barrier = barrier.clone();
        let worker = std::thread::spawn(move || {
            worker_barrier.wait();
            worker_slot.finish(Ok(vec![42; 960]));
        });
        barrier.wait();
        completion.cancel();
        worker.join().unwrap();
        assert!(matches!(completion.take(), Err(RnNoiseError::Cancelled)));
        assert!(completion.slot.lock().unwrap().result.is_none());
        assert_eq!(admission.available_permits(), 1);
        completion.cancel();
        completion.finish(Ok(vec![9; 960]));
        assert_eq!(admission.available_permits(), 1);
        assert!(matches!(completion.take(), Err(RnNoiseError::Cancelled)));
    }
}

#[tokio::test]
async fn uncollected_start_abandonment_releases_admission_and_restart_replaces_state() {
    let gate = Arc::new(Gate::default());
    let dropped = Arc::new(AtomicUsize::new(0));
    let factory_gate = gate.clone();
    let factory_dropped = dropped.clone();
    let filter = RnNoiseFilter::with_factory(
        config(),
        Box::new(move |_| {
            factory_gate.wait();
            Ok(Box::new(Probe {
                calls: 0,
                dropped: factory_dropped.clone(),
                process_gate: None,
                drop_gate: None,
            }))
        }),
    )
    .unwrap();
    let release = Release(gate);
    let mut start = Box::pin(filter.start(format()));
    assert_pending(start.as_mut());
    let completion = filter.current.lock().unwrap().upgrade().unwrap();
    release.0.open();
    completed(&completion).await;
    assert_eq!(filter.admission.available_permits(), 0);
    drop(start);
    assert!(completion.slot.lock().unwrap().result.is_none());
    assert_eq!(filter.admission.available_permits(), 1);
    assert!(filter.filter(pcm()).await.is_err());
    filter.start(format()).await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        filter.filter(pcm()).await.unwrap(),
        pipecat_audio::encode_pcm16_le(&vec![124; FRAME_SAMPLES])
    );
    filter.stop().await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn uncollected_stop_abandonment_does_not_prevent_another_stop_or_restart() {
    let gate = Arc::new(Gate::default());
    let dropped = Arc::new(AtomicUsize::new(0));
    let factory_gate = gate.clone();
    let factory_dropped = dropped.clone();
    let filter = RnNoiseFilter::with_factory(
        config(),
        Box::new(move |_| {
            Ok(Box::new(Probe {
                calls: 0,
                dropped: factory_dropped.clone(),
                process_gate: None,
                drop_gate: Some(factory_gate.clone()),
            }))
        }),
    )
    .unwrap();
    let release = Release(gate);
    filter.start(format()).await.unwrap();
    let mut stop = Box::pin(filter.stop());
    assert_pending(stop.as_mut());
    let completion = filter.current.lock().unwrap().upgrade().unwrap();
    release.0.open();
    completed(&completion).await;
    drop(stop);
    assert_eq!(filter.admission.available_permits(), 1);
    tokio::time::timeout(Duration::from_secs(2), filter.stop())
        .await
        .unwrap()
        .unwrap();
    filter.start(format()).await.unwrap();
    assert_eq!(filter.filter(pcm()).await.unwrap().len(), 960);
    filter.stop().await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn abandoned_lifecycle_waiting_for_native_ack_cannot_restore_or_leak_old_stream() {
    for restart in [false, true] {
        let gate = Arc::new(Gate::default());
        let factory_gate = gate.clone();
        let dropped = Arc::new(AtomicUsize::new(0));
        let factory_dropped = dropped.clone();
        let filter = RnNoiseFilter::with_factory(
            config(),
            Box::new(move |_| {
                Ok(Box::new(Probe {
                    calls: 0,
                    dropped: factory_dropped.clone(),
                    process_gate: Some(factory_gate.clone()),
                    drop_gate: None,
                }))
            }),
        )
        .unwrap();
        let release = Release(gate);
        filter.start(format()).await.unwrap();
        let mut audio = Box::pin(filter.filter(pcm()));
        assert_pending(audio.as_mut());
        let completion = filter.current.lock().unwrap().upgrade().unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !release.0 .2.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut lifecycle = Box::pin(async {
            if restart {
                filter.start(format()).await
            } else {
                filter.stop().await
            }
        });
        assert_pending(lifecycle.as_mut());
        assert!(completion.cancelled.load(Ordering::Acquire));
        assert_eq!(filter.admission.available_permits(), 0);
        drop(lifecycle);
        release.0.open();
        completed(&completion).await;
        assert!(audio.await.unwrap_err().message.contains("cancelled"));
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(filter.filter(pcm()).await.is_err());
        tokio::time::timeout(Duration::from_secs(2), filter.stop())
            .await
            .unwrap()
            .unwrap();
        filter.start(format()).await.unwrap();
        assert_eq!(
            filter.filter(pcm()).await.unwrap(),
            pipecat_audio::encode_pcm16_le(&vec![124; FRAME_SAMPLES])
        );
        filter.stop().await.unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
    }
}

#[tokio::test]
async fn failed_replacement_start_destroys_prior_state_and_allows_later_recovery() {
    let generations = Arc::new(AtomicUsize::new(0));
    let factory_generations = generations.clone();
    let dropped = Arc::new(AtomicUsize::new(0));
    let factory_dropped = dropped.clone();
    let filter = RnNoiseFilter::with_factory(
        config(),
        Box::new(move |_| {
            if factory_generations.fetch_add(1, Ordering::SeqCst) == 1 {
                return Err(RnNoiseError::Native("replacement failed".into()));
            }
            Ok(Box::new(Probe {
                calls: 0,
                dropped: factory_dropped.clone(),
                process_gate: None,
                drop_gate: None,
            }))
        }),
    )
    .unwrap();
    filter.start(format()).await.unwrap();
    filter.filter(pcm()).await.unwrap();
    assert!(filter.start(format()).await.is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(filter.filter(pcm()).await.is_err());
    filter.start(format()).await.unwrap();
    assert_eq!(
        filter.filter(pcm()).await.unwrap(),
        pipecat_audio::encode_pcm16_le(&vec![124; FRAME_SAMPLES])
    );
    filter.stop().await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn late_drop_of_revoked_old_audio_does_not_cancel_restarted_stream() {
    let gate = Arc::new(Gate::default());
    let factory_gate = gate.clone();
    let filter = RnNoiseFilter::with_factory(
        config(),
        Box::new(move |_| {
            Ok(Box::new(Probe {
                calls: 0,
                dropped: Arc::new(AtomicUsize::new(0)),
                process_gate: Some(factory_gate.clone()),
                drop_gate: None,
            }))
        }),
    )
    .unwrap();
    let release = Release(gate);
    filter.start(format()).await.unwrap();
    let mut audio = Box::pin(filter.filter(pcm()));
    assert_pending(audio.as_mut());
    let completion = filter.current.lock().unwrap().upgrade().unwrap();
    release.0.open();
    completed(&completion).await;
    filter.start(format()).await.unwrap();
    assert_eq!(
        filter.filter(pcm()).await.unwrap(),
        pipecat_audio::encode_pcm16_le(&vec![124; FRAME_SAMPLES])
    );
    drop(audio);
    assert_eq!(
        filter.filter(pcm()).await.unwrap(),
        pipecat_audio::encode_pcm16_le(&vec![125; FRAME_SAMPLES])
    );
    filter.stop().await.unwrap();
}

#[tokio::test]
async fn enable_after_stop_preserves_toggle_without_allocating_state_and_overlap_is_bounded() {
    let gate = Arc::new(Gate::default());
    let factory_gate = gate.clone();
    let generations = Arc::new(AtomicUsize::new(0));
    let factory_generations = generations.clone();
    let dropped = Arc::new(AtomicUsize::new(0));
    let factory_dropped = dropped.clone();
    let filter = RnNoiseFilter::with_factory(
        config(),
        Box::new(move |_| {
            factory_generations.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(Probe {
                calls: 0,
                dropped: factory_dropped.clone(),
                process_gate: None,
                drop_gate: Some(factory_gate.clone()),
            }))
        }),
    )
    .unwrap();
    let release = Release(gate);
    filter.start(format()).await.unwrap();
    assert!(filter.filter(vec![0; 200]).await.unwrap().is_empty());
    let mut stop = Box::pin(filter.stop());
    assert_pending(stop.as_mut());
    assert!(filter
        .control(&FrameData::FilterEnable(false))
        .await
        .unwrap_err()
        .message
        .contains("admitted request"));
    release.0.open();
    stop.await.unwrap();
    filter
        .control(&FrameData::FilterEnable(false))
        .await
        .unwrap();
    assert_eq!(generations.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(filter.filter(pcm()).await.is_err());
    filter.start(format()).await.unwrap();
    assert_eq!(filter.filter(vec![7, 9, 11]).await.unwrap(), vec![7, 9, 11]);
    filter
        .control(&FrameData::FilterEnable(true))
        .await
        .unwrap();
    assert!(filter.filter(vec![0; 760]).await.unwrap().is_empty());
    assert_eq!(
        filter.filter(vec![0; 200]).await.unwrap(),
        pipecat_audio::encode_pcm16_le(&vec![1; FRAME_SAMPLES])
    );
    filter.stop().await.unwrap();
    assert_eq!(generations.load(Ordering::SeqCst), 2);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn enable_after_failed_start_remains_sticky_without_implicitly_retrying_initialization() {
    let generations = Arc::new(AtomicUsize::new(0));
    let factory_generations = generations.clone();
    let filter = RnNoiseFilter::with_factory(
        config(),
        Box::new(move |_| {
            if factory_generations.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(RnNoiseError::Native("first initialization failed".into()));
            }
            Ok(Box::new(Probe {
                calls: 0,
                dropped: Arc::new(AtomicUsize::new(0)),
                process_gate: None,
                drop_gate: None,
            }))
        }),
    )
    .unwrap();
    assert!(filter.start(format()).await.is_err());
    filter
        .control(&FrameData::FilterEnable(false))
        .await
        .unwrap();
    assert_eq!(generations.load(Ordering::SeqCst), 1);
    assert!(filter.filter(pcm()).await.is_err());
    filter.start(format()).await.unwrap();
    assert_eq!(filter.filter(vec![7, 9, 11]).await.unwrap(), vec![7, 9, 11]);
    filter
        .control(&FrameData::FilterEnable(true))
        .await
        .unwrap();
    assert_eq!(
        filter.filter(pcm()).await.unwrap(),
        pipecat_audio::encode_pcm16_le(&vec![124; FRAME_SAMPLES])
    );
    filter.stop().await.unwrap();
    assert_eq!(generations.load(Ordering::SeqCst), 2);
}
