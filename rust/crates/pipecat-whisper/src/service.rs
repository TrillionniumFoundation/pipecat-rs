use crate::{
    model::decode_pcm, Cancellation, Result, Transcript, WhisperConfig, WhisperError, WhisperModel,
    WhisperSettings,
};
use crate::{
    source::{attach_source, SourceCapture},
    AudioSourceRetainer,
};
use pipecat_core::AudioRawFrame;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex,
};
use tokio::sync::{mpsc, oneshot, Notify, OwnedSemaphorePermit, Semaphore};

struct Request {
    samples: Vec<f32>,
    settings: WhisperSettings,
    cancel: Cancellation,
    reply: oneshot::Sender<Result<Transcript>>,
    permit: Arc<OwnedSemaphorePermit>,
    source: Option<SourceCapture>,
}
struct Worker {
    sender: Option<mpsc::Sender<Request>>,
    thread: Option<std::thread::JoinHandle<()>>,
    reaper: Option<std::thread::JoinHandle<()>>,
}

#[derive(Default)]
struct JoinCompletion {
    result: Mutex<Option<std::result::Result<(), String>>>,
    ready: Condvar,
    notified: Notify,
}
impl JoinCompletion {
    fn complete(&self, result: std::result::Result<(), String>) {
        *self
            .result
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(result);
        self.ready.notify_all();
        self.notified.notify_waiters();
    }
    fn wait_blocking(&self) {
        let mut result = self
            .result
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while result.is_none() {
            result = self
                .ready
                .wait(result)
                .unwrap_or_else(|poison| poison.into_inner());
        }
    }
    async fn wait(&self) -> Result<()> {
        loop {
            let notification = self.notified.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            if let Some(result) = self
                .result
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone()
            {
                return result.map_err(WhisperError::Worker);
            }
            notification.await;
        }
    }
}
// Ownership survives cancellation of any async join waiter. If a reaper cannot
// be started, dropping its guard synchronously joins the native thread instead.
struct JoinGuard {
    thread: Option<std::thread::JoinHandle<()>>,
    completion: Arc<JoinCompletion>,
}
impl JoinGuard {
    fn run(mut self) {
        self.finish();
    }
    fn finish(&mut self) {
        if let Some(thread) = self.thread.take() {
            self.completion.complete(
                thread
                    .join()
                    .map_err(|_| "native inference thread panicked".into()),
            );
        }
    }
}
impl Drop for JoinGuard {
    fn drop(&mut self) {
        self.finish();
    }
}

fn launch_reaper(
    guard: JoinGuard,
    builder: std::thread::Builder,
) -> Option<std::thread::JoinHandle<()>> {
    builder.spawn(move || guard.run()).ok()
}

/// One inference thread owns one model. Capacity includes outstanding replies,
/// and submission fails immediately with Backpressure instead of blocking the
/// pipeline audio path. Drop cancels and joins the native thread synchronously;
/// call shutdown().await to join without blocking an async executor thread.
pub struct WhisperService {
    config: WhisperConfig,
    multilingual: bool,
    worker: Mutex<Worker>,
    stop: Cancellation,
    running: Arc<AtomicBool>,
    permits: Arc<Semaphore>,
    joined: Arc<JoinCompletion>,
}
pub struct PendingTranscription {
    receiver: oneshot::Receiver<Result<Transcript>>,
    cancel: Cancellation,
    stop: Cancellation,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl PendingTranscription {
    pub fn cancellation(&self) -> Cancellation {
        self.cancel.clone()
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    /// Resolves after the native call acknowledges cancellation, not merely
    /// after its consumer loses interest. Dropping this future also cancels it.
    pub async fn result(mut self) -> Result<Transcript> {
        let result = (&mut self.receiver).await;
        if self.cancel.is_cancelled() || self.stop.is_cancelled() {
            return Err(WhisperError::Cancelled);
        }
        result.map_err(|_| WhisperError::Closed)?
    }
}
impl Drop for PendingTranscription {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
impl WhisperService {
    /// Loads on the owned inference thread. Native model loading itself has no
    /// abort hook; cancellation joins after loading returns. Decoding is abortable.
    pub async fn load(config: WhisperConfig) -> Result<Self> {
        config.validate()?;
        let (sender, mut receiver) = mpsc::channel::<Request>(config.queue_capacity);
        let (ready_tx, ready_rx) = oneshot::channel();
        let stop = Cancellation::new();
        let running = Arc::new(AtomicBool::new(true));
        let thread_stop = stop.clone();
        let thread_running = running.clone();
        let thread_config = config.clone();
        let thread = std::thread::Builder::new()
            .name("pipecat-whisper".into())
            .spawn(move || {
                struct Running(Arc<AtomicBool>);
                impl Drop for Running {
                    fn drop(&mut self) {
                        self.0.store(false, Ordering::SeqCst);
                    }
                }
                let _running = Running(thread_running);
                let mut model = match WhisperModel::load(thread_config) {
                    Ok(model) => model,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                };
                if ready_tx.send(Ok(model.is_multilingual())).is_err() {
                    return;
                }
                while let Some(request) = receiver.blocking_recv() {
                    if thread_stop.is_cancelled() {
                        break;
                    }
                    if request.reply.is_closed() {
                        continue;
                    }
                    let result = model
                        .transcribe_samples(
                            &request.samples,
                            &request.settings,
                            &request.cancel,
                            &thread_stop,
                        )
                        .and_then(|transcript| {
                            attach_source(transcript, request.source, &request.cancel, &thread_stop)
                        });
                    // Consumers may drop their handle before native cancellation
                    // is acknowledged. Hold admission until both owners finish.
                    drop(request.permit);
                    let _ = request.reply.send(result);
                }
            })
            .map_err(|e| WhisperError::Worker(e.to_string()))?;
        let mut service = Self {
            permits: Arc::new(Semaphore::new(config.queue_capacity)),
            config,
            multilingual: false,
            stop,
            running,
            joined: Arc::default(),
            worker: Mutex::new(Worker {
                sender: Some(sender),
                thread: Some(thread),
                reaper: None,
            }),
        };
        service.multilingual = ready_rx
            .await
            .map_err(|_| WhisperError::Worker("model loader exited".into()))??;
        Ok(service)
    }
    pub fn config(&self) -> &WhisperConfig {
        &self.config
    }
    pub fn is_multilingual(&self) -> bool {
        self.multilingual
    }
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    pub fn validate_settings(&self, settings: &WhisperSettings) -> Result<()> {
        settings.validate_model(self.multilingual)
    }
    pub fn submit(
        &self,
        audio: &AudioRawFrame,
        settings: WhisperSettings,
    ) -> Result<PendingTranscription> {
        self.submit_inner(audio, settings, None)
    }
    /// Preserve only the captured prefix, before any decoder padding. Storage
    /// executes on the same joined worker as inference, with the same permit;
    /// no blocking task or file writer is detached during cancellation.
    pub fn submit_with_source(
        &self,
        audio: &AudioRawFrame,
        settings: WhisperSettings,
        captured_samples: usize,
        sequence: u64,
        retainer: Arc<dyn AudioSourceRetainer>,
    ) -> Result<PendingTranscription> {
        if captured_samples == 0 || captured_samples > audio.audio.len() / 2 || sequence == 0 {
            return Err(WhisperError::Invalid("captured audio prefix or sequence"));
        }
        self.submit_inner(
            audio,
            settings,
            Some((captured_samples, sequence, retainer)),
        )
    }
    fn submit_inner(
        &self,
        audio: &AudioRawFrame,
        settings: WhisperSettings,
        source: Option<(usize, u64, Arc<dyn AudioSourceRetainer>)>,
    ) -> Result<PendingTranscription> {
        self.validate_settings(&settings)?;
        if self.stop.is_cancelled() {
            return Err(WhisperError::Closed);
        }
        let worker = self
            .worker
            .lock()
            .map_err(|_| WhisperError::Worker("worker lock poisoned".into()))?;
        if worker.sender.is_none() {
            return Err(WhisperError::Closed);
        }
        let permit = Arc::new(
            self.permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| WhisperError::Backpressure)?,
        );
        let samples = decode_pcm(audio, self.config.max_samples())?;
        let source = source.map(|(samples, sequence, retainer)| SourceCapture {
            pcm16: audio.audio[..samples * 2].to_vec(),
            sequence,
            retainer,
        });
        let cancel = Cancellation::new();
        let (reply, receiver) = oneshot::channel();
        worker
            .sender
            .as_ref()
            .ok_or(WhisperError::Closed)?
            .try_send(Request {
                samples,
                settings,
                cancel: cancel.clone(),
                reply,
                permit: permit.clone(),
                source,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => WhisperError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => WhisperError::Closed,
            })?;
        Ok(PendingTranscription {
            receiver,
            cancel,
            stop: self.stop.clone(),
            _permit: permit,
        })
    }
    /// Close admission and drain already accepted requests. This does not join;
    /// drain() waits for the native thread, shutdown() cancels it instead.
    pub fn finish(&self) -> Result<()> {
        self.worker
            .lock()
            .map_err(|_| WhisperError::Worker("worker lock poisoned".into()))?
            .sender
            .take();
        Ok(())
    }
    pub fn cancel(&self) -> Result<()> {
        self.stop.cancel();
        self.finish()
    }
    pub async fn drain(&self) -> Result<()> {
        self.finish()?;
        self.join().await
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.cancel()?;
        self.join().await
    }
    async fn join(&self) -> Result<()> {
        {
            let mut worker = self
                .worker
                .lock()
                .map_err(|_| WhisperError::Worker("worker lock poisoned".into()))?;
            if let Some(handle) = worker.thread.take() {
                let guard = JoinGuard {
                    thread: Some(handle),
                    completion: self.joined.clone(),
                };
                // Drop may run inside a saturated Tokio blocking pool, so native
                // completion must not depend on any job queued to that executor.
                worker.reaper = launch_reaper(
                    guard,
                    std::thread::Builder::new().name("pipecat-whisper-join".into()),
                );
            }
        }
        self.joined.wait().await
    }
}
impl Drop for WhisperService {
    fn drop(&mut self) {
        self.stop.cancel();
        let worker = self
            .worker
            .get_mut()
            .unwrap_or_else(|poison| poison.into_inner());
        worker.sender.take();
        if let Some(handle) = worker.thread.take() {
            JoinGuard {
                thread: Some(handle),
                completion: self.joined.clone(),
            }
            .run();
        }
        if let Some(reaper) = worker.reaper.take() {
            let _ = reaper.join();
        }
        self.joined.wait_blocking();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn dropping_pending_keeps_capacity_reserved_until_native_request_exits() {
        let (sender, mut receiver) = mpsc::channel::<Request>(1);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let request = receiver.blocking_recv().unwrap();
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(request);
        });
        let service = WhisperService {
            config: WhisperConfig::new("unused-model"),
            multilingual: false,
            worker: Mutex::new(Worker {
                sender: Some(sender),
                thread: Some(thread),
                reaper: None,
            }),
            stop: Cancellation::new(),
            running: Arc::new(AtomicBool::new(true)),
            permits: Arc::new(Semaphore::new(1)),
            joined: Arc::default(),
        };
        let audio = AudioRawFrame::new(vec![0; 2], 16_000, 1).unwrap();
        let pending = service.submit(&audio, WhisperSettings::default()).unwrap();
        entered_rx.recv().unwrap();
        drop(pending);
        let reserved = matches!(
            service.submit(&audio, WhisperSettings::default()),
            Err(WhisperError::Backpressure)
        );
        release_tx.send(()).unwrap();
        drop(service);
        assert!(
            reserved,
            "cancelled native request released capacity before exiting"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rejected_reaper_spawn_still_joins_the_native_thread() {
        let completed = Arc::new(AtomicBool::new(false));
        let thread_completed = completed.clone();
        let thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            thread_completed.store(true, Ordering::SeqCst);
        });
        let joined = Arc::new(JoinCompletion::default());
        let reaper = launch_reaper(
            JoinGuard {
                thread: Some(thread),
                completion: joined.clone(),
            },
            // Linux rejects this impossible stack allocation with EINVAL.
            std::thread::Builder::new().stack_size(usize::MAX),
        );
        assert!(reaper.is_none());
        assert!(completed.load(Ordering::SeqCst));
        assert_eq!(*joined.result.lock().unwrap(), Some(Ok(())));
    }

    #[test]
    fn panicking_reaper_spawn_still_joins_the_native_thread() {
        let completed = Arc::new(AtomicBool::new(false));
        let thread_completed = completed.clone();
        let thread = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            thread_completed.store(true, Ordering::SeqCst);
        });
        let joined = Arc::new(JoinCompletion::default());
        let guard = JoinGuard {
            thread: Some(thread),
            completion: joined.clone(),
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            launch_reaper(guard, std::thread::Builder::new().name("bad\0name".into()))
        }));
        assert!(result.is_err());
        assert!(completed.load(Ordering::SeqCst));
        assert_eq!(*joined.result.lock().unwrap(), Some(Ok(())));
    }

    #[tokio::test]
    async fn panicked_native_thread_has_a_persistent_shared_join_error() {
        let joined = Arc::new(JoinCompletion::default());
        let reaper = launch_reaper(
            JoinGuard {
                thread: Some(std::thread::spawn(|| panic!("native worker failed"))),
                completion: joined.clone(),
            },
            std::thread::Builder::new(),
        )
        .unwrap();
        let (first, second) = tokio::join!(joined.wait(), joined.wait());
        assert!(matches!(first, Err(WhisperError::Worker(_))));
        assert!(matches!(second, Err(WhisperError::Worker(_))));
        assert!(matches!(joined.wait().await, Err(WhisperError::Worker(_))));
        reaper.join().unwrap();
    }

    #[tokio::test]
    #[ignore = "requires explicit local GGML model"]
    async fn last_owner_drop_waits_for_inference_after_aborted_join_waiter() {
        let model =
            std::env::var_os("PIPECAT_WHISPER_MODEL").expect("explicit GGML model required");
        let service = Arc::new(
            WhisperService::load(WhisperConfig::new(model))
                .await
                .unwrap(),
        );
        let running = service.running.clone();
        let audio = AudioRawFrame::new(vec![0; 16_000 * 2 * 30], 16_000, 1).unwrap();
        let pending = service.submit(&audio, WhisperSettings::default()).unwrap();
        let waiter_service = service.clone();
        let waiter = tokio::spawn(async move { waiter_service.drain().await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        tokio::task::spawn_blocking(move || drop(service))
            .await
            .unwrap();
        assert!(
            !running.load(Ordering::SeqCst),
            "Drop returned while native model was still running"
        );
        assert!(matches!(
            pending.result().await,
            Err(WhisperError::Cancelled)
        ));
    }
}
