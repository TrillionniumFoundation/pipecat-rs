//! Explicit, bounded, offline ownership for the existing PNG/JPEG decoder.
use crate::{
    decode::{inspect_encoded, DecodeControl, DecoderWorker},
    CancellationToken, ImageError, ImageGenerationLimits, Result,
};
use pipecat_core::ImageRawFrame;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Limits for the small offline RGB route. Values may be lowered, never raised
/// beyond these defaults. Constructing limits does not start a worker.
#[derive(Clone, Debug)]
pub struct OfflineRgbDecodeLimits {
    pub max_encoded_image_bytes: usize,
    pub max_pixels: usize,
    pub max_dimension: u32,
    /// Conservative codec scratch, separate from the final RGB output.
    pub max_decode_bytes: usize,
    /// Total native jobs, including active work; the pending lane holds one job.
    /// Completed results retained by callers are not native jobs; applications
    /// must bound those independently (for example with `submit_retained`).
    pub max_jobs: usize,
    /// Starts at admission, including queue wait.
    pub decode_timeout: Duration,
    /// Cooperative diagnostic interval, not a native-thread termination deadline.
    pub shutdown_timeout: Duration,
}
impl Default for OfflineRgbDecodeLimits {
    fn default() -> Self {
        Self {
            max_encoded_image_bytes: 64 * 1024,
            max_pixels: 262_144,
            max_dimension: 1024,
            max_decode_bytes: 32 * 1024 * 1024,
            max_jobs: 2,
            decode_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}
impl OfflineRgbDecodeLimits {
    pub fn validate(&self) -> Result<()> {
        let cap = Self::default();
        if self.max_encoded_image_bytes == 0
            || self.max_encoded_image_bytes > cap.max_encoded_image_bytes
            || self.max_pixels == 0
            || self.max_pixels > cap.max_pixels
            || self.max_dimension == 0
            || self.max_dimension > cap.max_dimension
            || self.max_decode_bytes == 0
            || self.max_decode_bytes > cap.max_decode_bytes
            || self.max_jobs == 0
            || self.max_jobs > cap.max_jobs
            || self.decode_timeout.is_zero()
            || self.decode_timeout > cap.decode_timeout
            || self.shutdown_timeout.is_zero()
            || self.shutdown_timeout > cap.shutdown_timeout
        {
            return Err(ImageError::Invalid("invalid offline RGB resource limit"));
        }
        self.max_pixels.checked_mul(3).ok_or(ImageError::TooLarge)?;
        Ok(())
    }
    fn codec_limits(&self) -> ImageGenerationLimits {
        ImageGenerationLimits {
            max_encoded_image_bytes: self.max_encoded_image_bytes,
            max_pixels: self.max_pixels,
            max_dimension: self.max_dimension,
            max_decode_bytes: self.max_decode_bytes,
            max_pending: self.max_jobs,
            request_timeout: self.decode_timeout,
            ..Default::default()
        }
    }
    /// Bounded magic, container, and dimensions preflight without pixel decoding,
    /// allocation of image buffers, or construction of a native worker. MIME must
    /// be exactly `image/png` or `image/jpeg` and agree with the bytes.
    ///
    /// Success is not full codec/CRC certification. Decoding still validates the
    /// stream. This borrowed check cannot measure an owned Vec's spare capacity;
    /// `OfflineRgbDecoder::submit` independently checks that capacity.
    pub fn inspect_encoded_image(&self, bytes: &[u8], mime: &str) -> Result<(u32, u32)> {
        self.validate()?;
        if bytes.len() > self.max_encoded_image_bytes {
            return Err(ImageError::TooLarge);
        }
        let matches = match mime {
            "image/png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
            "image/jpeg" => bytes.starts_with(&[0xff, 0xd8]),
            _ => false,
        };
        if !matches {
            return Err(ImageError::Decode);
        }
        inspect_encoded(
            bytes,
            &self.codec_limits(),
            &DecodeControl {
                cancel: CancellationToken::new(),
                caller: CancellationToken::new(),
                deadline: Instant::now() + self.decode_timeout,
            },
        )
    }
    /// The same header/container validation as `inspect_encoded_image`.
    pub fn validate_encoded_image(&self, bytes: &[u8], mime: &str) -> Result<()> {
        self.inspect_encoded_image(bytes, mime).map(|_| ())
    }
}

/// Native ownership status, not request-future completion. A cancelled future
/// can finish while `outstanding_jobs` remains nonzero. Only `joined` establishes
/// that the owned decoder thread was actually joined.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OfflineRgbDecoderStatus {
    pub closed: bool,
    pub outstanding_jobs: usize,
    pub joined: bool,
}
struct Registration {
    permit: Weak<OwnedSemaphorePermit>,
    cancel: CancellationToken,
}
struct State {
    worker: DecoderWorker,
    registrations: Vec<Registration>,
    closed: bool,
    join_result: Option<bool>,
}

/// An explicitly constructed owner of one native decoder thread. It performs no
/// networking, starts no per-request threads, and exports no admission permits.
/// Call `shutdown` to observe actual joined cleanup. Drop is a synchronous final
/// close/cancel/join fallback and can wait for a native codec operation to finish.
pub struct OfflineRgbDecoder {
    limits: OfflineRgbDecodeLimits,
    permits: Arc<Semaphore>,
    state: Mutex<State>,
}
impl OfflineRgbDecoder {
    pub fn new(limits: OfflineRgbDecodeLimits) -> Result<Self> {
        limits.validate()?;
        let worker = DecoderWorker::with_queue_capacity(limits.codec_limits(), 1)?;
        Ok(Self::from_worker(limits, worker))
    }
    fn from_worker(limits: OfflineRgbDecodeLimits, worker: DecoderWorker) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limits.max_jobs)),
            state: Mutex::new(State {
                worker,
                registrations: Vec::with_capacity(limits.max_jobs),
                closed: false,
                join_result: None,
            }),
            limits,
        }
    }
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
    /// Admit one owned input without waiting for capacity. Both length and
    /// allocated capacity count against the encoded bound. The job starts at
    /// admission, even if the returned future is never polled.
    pub fn submit(&self, bytes: Vec<u8>) -> Result<OfflineRgbDecodeJob> {
        self.submit_inner(bytes, None)
    }
    /// As `submit`, retaining a caller-owned accounting/snapshot anchor in both
    /// the native job and returned future. Dropping the future cannot release
    /// this anchor before native job storage is gone. An application can retain
    /// the same anchor in a subsequent image carrier for downstream accounting.
    /// The caller remains responsible for bounding the anchor's own allocations.
    /// The anchor must not retain this decoder owner or block its native worker
    /// from exiting (including through its own Drop implementation).
    pub fn submit_retained(
        &self,
        bytes: Vec<u8>,
        retained: Arc<dyn Send + Sync>,
    ) -> Result<OfflineRgbDecodeJob> {
        self.submit_inner(bytes, Some(retained))
    }
    fn submit_inner(
        &self,
        bytes: Vec<u8>,
        retained: Option<Arc<dyn Send + Sync>>,
    ) -> Result<OfflineRgbDecodeJob> {
        if bytes.len() > self.limits.max_encoded_image_bytes
            || bytes.capacity() > self.limits.max_encoded_image_bytes
        {
            return Err(ImageError::TooLarge);
        }
        if bytes.is_empty() {
            return Err(ImageError::Decode);
        }
        let mut state = self.state();
        if state.closed {
            return Err(ImageError::Closed);
        }
        let permit = Arc::new(
            self.permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| ImageError::Backpressure)?,
        );
        state
            .registrations
            .retain(|entry| entry.permit.strong_count() != 0);
        let cancel = CancellationToken::new();
        let deadline = Instant::now() + self.limits.decode_timeout;
        let receive = state.worker.submit_retained(
            bytes,
            DecodeControl {
                cancel: cancel.clone(),
                caller: CancellationToken::new(),
                deadline,
            },
            permit.clone(),
            self.limits.max_pixels * 3,
            retained.clone(),
        )?;
        state.registrations.push(Registration {
            permit: Arc::downgrade(&permit),
            cancel: cancel.clone(),
        });
        drop(state);
        let wait_cancel = cancel.clone();
        let future = Box::pin(async move {
            tokio::select! {
                biased;
                _ = wait_cancel.cancelled() => Err(ImageError::Cancelled),
                _ = tokio::time::sleep_until(deadline.into()) => Err(ImageError::Timeout),
                result = receive => result.map_err(|_| ImageError::Closed)?,
            }
        });
        Ok(OfflineRgbDecodeJob {
            future,
            cancel,
            _retained: retained,
        })
    }
    /// Close admission and signal every active/pending native job. This does not
    /// claim that jobs have exited or that the thread has been joined.
    pub fn close(&self) {
        let cancels = {
            let mut state = self.state();
            state.closed = true;
            self.permits.close();
            state.worker.close();
            state
                .registrations
                .iter()
                .map(|entry| entry.cancel.clone())
                .collect::<Vec<_>>()
        };
        for cancel in cancels {
            cancel.cancel();
        }
    }
    pub fn status(&self) -> OfflineRgbDecoderStatus {
        let state = self.state();
        OfflineRgbDecoderStatus {
            closed: state.closed,
            outstanding_jobs: self.limits.max_jobs - self.permits.available_permits(),
            joined: state.join_result.is_some(),
        }
    }
    /// Close, cancel, and actually join the native owner. Uses the Tokio timer.
    /// `Timeout` is a cooperative diagnostic: the owner retains its handle and
    /// cancellation registry, and another call resumes the same shutdown. Dropping
    /// this future also retains ownership. No detached join task is started.
    pub async fn shutdown(&self) -> Result<()> {
        self.close();
        let deadline = tokio::time::Instant::now() + self.limits.shutdown_timeout;
        loop {
            {
                let mut state = self.state();
                if let Some(success) = state.join_result {
                    return if success {
                        Ok(())
                    } else {
                        Err(ImageError::Closed)
                    };
                }
                if state.worker.is_finished() {
                    let result = state.worker.join();
                    state.join_result = Some(result.is_ok());
                    state.registrations.clear();
                    return result;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ImageError::Timeout);
            }
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + Duration::from_millis(1)).min(deadline),
            )
            .await;
        }
    }
}
impl Drop for OfflineRgbDecoder {
    fn drop(&mut self) {
        self.close();
        // Exclusive owner access: no registry/state mutex is held while joining.
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        if state.join_result.is_none() {
            let result = state.worker.join();
            state.join_result = Some(result.is_ok());
        }
    }
}

/// An already-admitted decode. Explicit cancellation, timeout, or dropping this
/// future can finish request ownership before native job exit. Native admission
/// and any retention anchor remain held until the native job actually releases
/// its storage. The output is tightly packed RGB8 without alpha compositing.
#[must_use = "dropping an offline decode job cancels it"]
pub struct OfflineRgbDecodeJob {
    future: Pin<Box<dyn Future<Output = Result<ImageRawFrame>> + Send>>,
    cancel: CancellationToken,
    // Drop the result receiver (and any queued RGB) before caller accounting.
    _retained: Option<Arc<dyn Send + Sync>>,
}
impl OfflineRgbDecodeJob {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
}
impl Future for OfflineRgbDecodeJob {
    type Output = Result<ImageRawFrame>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.future.as_mut().poll(cx)
    }
}
impl Drop for OfflineRgbDecodeJob {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Condvar,
    };

    #[derive(Default)]
    struct Gate {
        entered: AtomicBool,
        released: Mutex<bool>,
        wake: Condvar,
    }
    impl Gate {
        fn hold(&self) {
            self.entered.store(true, Ordering::Release);
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
        }
        fn release(&self) {
            *self.released.lock().unwrap() = true;
            self.wake.notify_all();
        }
        async fn entered(&self) {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !self.entered.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("native worker reached deterministic hold");
        }
    }
    struct HeldDecoder {
        decoder: OfflineRgbDecoder,
        gate: Arc<Gate>,
    }
    impl HeldDecoder {
        fn new(limits: OfflineRgbDecodeLimits) -> Self {
            let gate = Arc::new(Gate::default());
            let held = gate.clone();
            let worker =
                DecoderWorker::with_test_hook(limits.codec_limits(), move |_| held.hold()).unwrap();
            Self {
                decoder: OfflineRgbDecoder::from_worker(limits, worker),
                gate,
            }
        }
    }
    impl Drop for HeldDecoder {
        fn drop(&mut self) {
            // Even a failed assertion must unblock the owner's mandatory Drop join.
            self.gate.release();
        }
    }

    #[tokio::test]
    async fn unpolled_drop_retains_native_admission_and_caller_anchor() {
        let held = HeldDecoder::new(Default::default());
        let anchor = Arc::new(());
        let weak = Arc::downgrade(&anchor);
        let active = held.decoder.submit_retained(vec![1], anchor).unwrap();
        held.gate.entered().await;
        drop(active); // Never polled, yet the native job already owns its input.
        assert_eq!(held.decoder.status().outstanding_jobs, 1);
        assert!(weak.upgrade().is_some());
        let queued = held.decoder.submit(vec![2]).unwrap();
        drop(queued);
        assert_eq!(held.decoder.status().outstanding_jobs, 2);
        assert!(matches!(
            held.decoder.submit(vec![3]),
            Err(ImageError::Backpressure)
        ));
        held.gate.release();
        held.decoder.shutdown().await.unwrap();
        assert_eq!(
            held.decoder.status(),
            OfflineRgbDecoderStatus {
                closed: true,
                outstanding_jobs: 0,
                joined: true,
            }
        );
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn queued_and_active_cancel_finish_futures_before_native_job_exit() {
        let held = HeldDecoder::new(Default::default());
        let active = held.decoder.submit(vec![1]).unwrap();
        held.gate.entered().await;
        let queued = held.decoder.submit(vec![2]).unwrap();
        queued.cancel();
        assert_eq!(queued.await, Err(ImageError::Cancelled));
        assert_eq!(held.decoder.status().outstanding_jobs, 2);
        active.cancel();
        assert_eq!(active.await, Err(ImageError::Cancelled));
        assert_eq!(held.decoder.status().outstanding_jobs, 2);
        assert!(matches!(
            held.decoder.submit(vec![3]),
            Err(ImageError::Backpressure)
        ));
        held.gate.release();
        held.decoder.shutdown().await.unwrap();
        assert_eq!(held.decoder.status().outstanding_jobs, 0);
    }

    #[tokio::test]
    async fn cancelled_shutdown_wait_and_diagnostic_timeout_retain_same_join_owner() {
        let held = HeldDecoder::new(OfflineRgbDecodeLimits {
            shutdown_timeout: Duration::from_millis(20),
            ..Default::default()
        });
        let active = held.decoder.submit(vec![1]).unwrap();
        held.gate.entered().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(1), held.decoder.shutdown())
                .await
                .is_err()
        );
        assert_eq!(
            held.decoder.status(),
            OfflineRgbDecoderStatus {
                closed: true,
                outstanding_jobs: 1,
                joined: false,
            }
        );
        assert_eq!(active.await, Err(ImageError::Cancelled));
        assert_eq!(held.decoder.shutdown().await, Err(ImageError::Timeout));
        assert!(!held.decoder.status().joined);
        assert_eq!(held.decoder.status().outstanding_jobs, 1);
        assert!(matches!(
            held.decoder.submit(vec![2]),
            Err(ImageError::Closed)
        ));
        held.gate.release();
        held.decoder.shutdown().await.unwrap();
        held.decoder.shutdown().await.unwrap();
        assert!(held.decoder.status().joined);
        assert_eq!(held.decoder.status().outstanding_jobs, 0);
    }

    #[tokio::test]
    async fn queued_deadline_includes_time_before_native_decode() {
        let held = HeldDecoder::new(OfflineRgbDecodeLimits {
            decode_timeout: Duration::from_millis(20),
            ..Default::default()
        });
        let active = held.decoder.submit(vec![1]).unwrap();
        held.gate.entered().await;
        let queued = held.decoder.submit(vec![2]).unwrap();
        assert_eq!(queued.await, Err(ImageError::Timeout));
        assert_eq!(held.decoder.status().outstanding_jobs, 2);
        drop(active);
        held.gate.release();
        held.decoder.shutdown().await.unwrap();
        assert_eq!(held.decoder.status().outstanding_jobs, 0);
    }

    #[tokio::test]
    async fn last_owner_drop_cancels_then_synchronously_joins_native_worker() {
        let limits = OfflineRgbDecodeLimits::default();
        let entered = Arc::new(AtomicBool::new(false));
        let exited = Arc::new(AtomicBool::new(false));
        let worker_entered = entered.clone();
        let worker_exited = exited.clone();
        let worker = DecoderWorker::with_test_hook(limits.codec_limits(), move |control| {
            worker_entered.store(true, Ordering::Release);
            while !control.cancel.is_cancelled() {
                std::thread::sleep(Duration::from_millis(1));
            }
            worker_exited.store(true, Ordering::Release);
        })
        .unwrap();
        let decoder = OfflineRgbDecoder::from_worker(limits, worker);
        let permits = decoder.permits.clone();
        let job = decoder.submit(vec![1]).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !entered.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        drop(decoder); // Must initiate cancellation itself, before its actual join.
        assert!(exited.load(Ordering::Acquire));
        assert_eq!(permits.available_permits(), 2);
        assert_eq!(job.await, Err(ImageError::Cancelled));
    }
}
