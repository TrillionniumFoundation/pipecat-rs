use crate::{PiperConfig, PiperError, PiperSettings, Result, VoiceConfig};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Condvar, Mutex,
};
use tokio::sync::{mpsc, oneshot, Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Default)]
pub struct Cancellation(Arc<CancelState>);
#[derive(Default)]
struct CancelState {
    cancelled: AtomicBool,
    #[cfg(feature = "onnx")]
    options: Mutex<Option<Arc<ort::session::RunOptions>>>,
}
impl Cancellation {
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::SeqCst);
        #[cfg(feature = "onnx")]
        if let Some(options) = self
            .0
            .options
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            let _ = options.terminate();
        }
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::SeqCst)
    }
    pub(crate) fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(PiperError::Cancelled)
        } else {
            Ok(())
        }
    }
    #[cfg(feature = "onnx")]
    pub(crate) fn install(&self, options: Arc<ort::session::RunOptions>) -> Result<()> {
        let mut active = self.0.options.lock().map_err(|_| PiperError::Worker)?;
        self.check()?;
        *active = Some(options);
        Ok(())
    }
    #[cfg(feature = "onnx")]
    pub(crate) fn uninstall(&self) {
        self.0
            .options
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
    }
}
#[derive(Debug)]
pub struct Synthesis {
    pub context_id: String,
    pub generation: u64,
    pub voice: String,
    pub pcm: Vec<u8>,
    pub sample_rate: u32,
    pub num_channels: u16,
}
#[cfg_attr(not(any(feature = "onnx", test)), allow(dead_code))]
struct Request {
    text: String,
    context_id: String,
    epoch: u64,
    settings: PiperSettings,
    cancel: Cancellation,
    reply: oneshot::Sender<Result<Synthesis>>,
    _permit: Arc<OwnedSemaphorePermit>,
}
struct Worker {
    sender: Option<mpsc::Sender<Request>>,
    thread: Option<std::thread::JoinHandle<()>>,
    reaper: Option<std::thread::JoinHandle<()>>,
}
#[derive(Default)]
struct Joined {
    result: Mutex<Option<bool>>,
    ready: Condvar,
    notified: Notify,
}
impl Joined {
    fn complete(&self, ok: bool) {
        *self.result.lock().unwrap_or_else(|p| p.into_inner()) = Some(ok);
        self.ready.notify_all();
        self.notified.notify_waiters();
    }
    fn blocking(&self) {
        let mut r = self.result.lock().unwrap_or_else(|p| p.into_inner());
        while r.is_none() {
            r = self.ready.wait(r).unwrap_or_else(|p| p.into_inner());
        }
    }
    async fn wait(&self) -> Result<()> {
        loop {
            let ready = self.notified.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if let Some(ok) = *self.result.lock().unwrap_or_else(|p| p.into_inner()) {
                return if ok { Ok(()) } else { Err(PiperError::Worker) };
            }
            ready.await;
        }
    }
}
struct JoinGuard {
    handle: Option<std::thread::JoinHandle<()>>,
    joined: Arc<Joined>,
}
impl JoinGuard {
    fn join(&mut self) {
        if let Some(h) = self.handle.take() {
            self.joined.complete(h.join().is_ok());
        }
    }
}
impl Drop for JoinGuard {
    fn drop(&mut self) {
        self.join();
    }
}

/// Single owned CPU worker, immediate bounded admission, cancel-on-drop requests.
/// Each result buffers a bounded complete utterance. Output is synthesis, not playout.
/// Drop cancels and synchronously joins. Prefer shutdown().await in an async task.
pub struct PiperService {
    config: PiperConfig,
    voice: VoiceConfig,
    worker: Mutex<Worker>,
    stop: Cancellation,
    epoch: Arc<AtomicU64>,
    current: Arc<Mutex<Option<Cancellation>>>,
    permits: Arc<Semaphore>,
    joined: Arc<Joined>,
    running: Arc<AtomicBool>,
}
pub struct PendingSynthesis {
    receiver: oneshot::Receiver<Result<Synthesis>>,
    cancel: Cancellation,
    stop: Cancellation,
    epoch: Arc<AtomicU64>,
    accepted_epoch: u64,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl PendingSynthesis {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub fn cancellation(&self) -> Cancellation {
        self.cancel.clone()
    }
    /// Resolves only after the worker acknowledges completion/cancellation.
    pub async fn result(mut self) -> Result<Synthesis> {
        let result = (&mut self.receiver).await;
        self.cancel.check()?;
        self.stop.check()?;
        if self.epoch.load(Ordering::SeqCst) != self.accepted_epoch {
            return Err(PiperError::Cancelled);
        }
        result.map_err(|_| PiperError::Closed)?
    }
}
impl Drop for PendingSynthesis {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl PiperService {
    pub async fn load(config: PiperConfig) -> Result<Self> {
        config.validate()?;
        #[cfg(not(feature = "onnx"))]
        {
            let _ = config;
            Err(PiperError::Unsupported("onnx feature is required"))
        }
        #[cfg(feature = "onnx")]
        {
            let (ready_tx, ready_rx) = oneshot::channel();
            let service = Self::spawn(
                config,
                move |config, ready, receiver, shared| {
                    let voice = match config
                        .config
                        .read(1024 * 1024)
                        .and_then(|bytes| VoiceConfig::parse(&bytes))
                    {
                        Ok(voice) => voice,
                        Err(error) => {
                            let _ = ready.send(Err(error));
                            return;
                        }
                    };
                    match crate::model::Model::load(config.clone(), voice.clone()) {
                        Ok(mut model) => {
                            let mut config = config;
                            config.output_sample_rate =
                                Some(config.output_sample_rate.unwrap_or(voice.audio.sample_rate));
                            let _ = ready.send(Ok(voice));
                            work(receiver, shared, &config, |text, settings, cancel, stop| {
                                model.synthesize(text, settings, cancel, stop)
                            });
                        }
                        Err(error) => {
                            let _ = ready.send(Err(error));
                        }
                    }
                },
                ready_tx,
            )?;
            let mut service = service;
            service.voice = ready_rx.await.map_err(|_| PiperError::Worker)??;
            Ok(service)
        }
    }
    #[cfg(any(feature = "onnx", test))]
    fn spawn<F>(
        config: PiperConfig,
        run: F,
        ready: oneshot::Sender<Result<VoiceConfig>>,
    ) -> Result<Self>
    where
        F: FnOnce(
                PiperConfig,
                oneshot::Sender<Result<VoiceConfig>>,
                mpsc::Receiver<Request>,
                Shared,
            ) + Send
            + 'static,
    {
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let stop = Cancellation::default();
        let epoch = Arc::new(AtomicU64::new(0));
        let current = Arc::new(Mutex::new(None));
        let running = Arc::new(AtomicBool::new(true));
        let shared = Shared {
            stop: stop.clone(),
            epoch: epoch.clone(),
            current: current.clone(),
            running: running.clone(),
        };
        let worker_config = config.clone();
        let thread = std::thread::Builder::new()
            .name("pipecat-piper".into())
            .spawn(move || {
                struct Running(Arc<AtomicBool>);
                impl Drop for Running {
                    fn drop(&mut self) {
                        self.0.store(false, Ordering::SeqCst);
                    }
                }
                let _running = Running(shared.running.clone());
                run(worker_config, ready, receiver, shared);
            })?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.queue_capacity)),
            config,
            voice: placeholder_voice(),
            worker: Mutex::new(Worker {
                sender: Some(sender),
                thread: Some(thread),
                reaper: None,
            }),
            stop,
            epoch,
            current,
            running,
            joined: Arc::default(),
        })
    }
    pub fn config(&self) -> &PiperConfig {
        &self.config
    }
    pub fn voice_config(&self) -> &VoiceConfig {
        &self.voice
    }
    pub fn sample_rate(&self) -> u32 {
        self.config
            .output_sample_rate
            .unwrap_or(self.voice.audio.sample_rate)
    }
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }
    pub fn validate_settings(&self, settings: &PiperSettings) -> Result<()> {
        settings.validate(&self.voice)
    }
    /// Loaded voice/model/language are immutable. Create another service to change them;
    /// never report a Python-style stored setting as applied when the model is unchanged.
    pub fn submit(
        &self,
        text: impl AsRef<str>,
        context_id: impl AsRef<str>,
        settings: PiperSettings,
    ) -> Result<PendingSynthesis> {
        let text = text.as_ref();
        let context_id = context_id.as_ref();
        self.validate_settings(&settings)?;
        if text.len() > self.config.max_text_bytes || text.trim().is_empty() || text.contains('\0')
        {
            return Err(PiperError::Invalid(
                "text must be nonblank, NUL-free, and within max_text_bytes",
            ));
        }
        if self.voice.phoneme_type == "espeak" && (text.contains("[[") || text.contains("]]")) {
            return Err(PiperError::Unsupported("inline IPA phoneme blocks"));
        }
        if context_id.is_empty() || context_id.len() > 256 || context_id.contains('\0') {
            return Err(PiperError::Invalid("invalid context ID"));
        }
        if self.stop.is_cancelled() {
            return Err(PiperError::Closed);
        }
        let worker = self.worker.lock().map_err(|_| PiperError::Worker)?;
        let sender = worker.sender.as_ref().ok_or(PiperError::Closed)?;
        let permit = Arc::new(
            self.permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| PiperError::Backpressure)?,
        );
        let accepted_epoch = self.epoch.load(Ordering::SeqCst);
        let cancel = Cancellation::default();
        let (reply, receiver) = oneshot::channel();
        sender
            .try_send(Request {
                text: text.to_owned(),
                context_id: context_id.to_owned(),
                epoch: accepted_epoch,
                settings,
                cancel: cancel.clone(),
                reply,
                _permit: permit.clone(),
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => PiperError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => PiperError::Closed,
            })?;
        Ok(PendingSynthesis {
            receiver,
            cancel,
            stop: self.stop.clone(),
            epoch: self.epoch.clone(),
            accepted_epoch,
            _permit: permit,
        })
    }
    /// Invalidates queued/running/completed old contexts; future submissions remain open.
    pub fn interrupt(&self) {
        let current = self.current.lock().unwrap_or_else(|p| p.into_inner());
        if self
            .epoch
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |epoch| {
                epoch.checked_add(1)
            })
            .is_err()
        {
            // Never wrap to a generation that could still be held by a consumer.
            self.stop.cancel();
            let _ = self.finish();
        }
        if let Some(cancel) = &*current {
            cancel.cancel();
        }
    }
    pub fn finish(&self) -> Result<()> {
        self.worker
            .lock()
            .map_err(|_| PiperError::Worker)?
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
            let mut worker = self.worker.lock().map_err(|_| PiperError::Worker)?;
            if let Some(handle) = worker.thread.take() {
                let mut guard = JoinGuard {
                    handle: Some(handle),
                    joined: self.joined.clone(),
                };
                worker.reaper = std::thread::Builder::new()
                    .name("pipecat-piper-join".into())
                    .spawn(move || guard.join())
                    .ok();
            }
        }
        self.joined.wait().await
    }
}
impl Drop for PiperService {
    fn drop(&mut self) {
        self.stop.cancel();
        let worker = self.worker.get_mut().unwrap_or_else(|p| p.into_inner());
        worker.sender.take();
        if let Some(handle) = worker.thread.take() {
            JoinGuard {
                handle: Some(handle),
                joined: self.joined.clone(),
            }
            .join();
        }
        if let Some(reaper) = worker.reaper.take() {
            let _ = reaper.join();
        }
        self.joined.blocking();
    }
}
#[cfg(any(feature = "onnx", test))]
struct Shared {
    stop: Cancellation,
    epoch: Arc<AtomicU64>,
    current: Arc<Mutex<Option<Cancellation>>>,
    running: Arc<AtomicBool>,
}
#[cfg(any(feature = "onnx", test))]
fn work<F>(
    mut receiver: mpsc::Receiver<Request>,
    shared: Shared,
    config: &PiperConfig,
    mut synthesize: F,
) where
    F: FnMut(&str, &PiperSettings, &Cancellation, &Cancellation) -> Result<Vec<u8>>,
{
    while let Some(request) = receiver.blocking_recv() {
        {
            let mut current = shared.current.lock().unwrap_or_else(|p| p.into_inner());
            *current = Some(request.cancel.clone());
            if request.epoch != shared.epoch.load(Ordering::SeqCst) {
                request.cancel.cancel();
            }
        }
        let result = request
            .cancel
            .check()
            .and_then(|()| shared.stop.check())
            .and_then(|()| {
                synthesize(
                    &request.text,
                    &request.settings,
                    &request.cancel,
                    &shared.stop,
                )
            });
        shared
            .current
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        let result = result.and_then(|pcm| {
            request.cancel.check()?;
            shared.stop.check()?;
            if request.epoch != shared.epoch.load(Ordering::SeqCst) {
                return Err(PiperError::Cancelled);
            }
            Ok(Synthesis {
                context_id: request.context_id,
                generation: request.epoch,
                voice: config.voice.clone(),
                pcm,
                sample_rate: config.output_sample_rate.unwrap_or(0),
                num_channels: 1,
            })
        });
        // Release the worker share before waking result(); the consumer still
        // owns its permit, so a completed uncollected result remains bounded.
        drop(request._permit);
        let _ = request.reply.send(result);
    }
}
#[cfg(any(feature = "onnx", test))]
fn placeholder_voice() -> VoiceConfig {
    VoiceConfig {
        audio: crate::AudioConfig { sample_rate: 16000 },
        phoneme_type: "text".into(),
        espeak: None,
        num_symbols: 3,
        num_speakers: 1,
        inference: crate::InferenceConfig {
            length_scale: 1.0,
            noise_scale: 0.667,
            noise_w: 0.8,
        },
        phoneme_id_map: Default::default(),
        phoneme_map: Default::default(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Artifact;
    use std::time::Duration;
    pub(crate) struct AdmissionProbe(Arc<Semaphore>);
    impl AdmissionProbe {
        pub(crate) fn available(&self) -> usize {
            self.0.available_permits()
        }
    }
    pub(crate) fn admission_probe(service: &PiperService) -> AdmissionProbe {
        AdmissionProbe(service.permits.clone())
    }
    fn config(capacity: usize) -> PiperConfig {
        let a = Artifact::new("unused", "0".repeat(64));
        let mut c = PiperConfig::new("test-voice", a.clone(), a.clone(), a);
        c.queue_capacity = capacity;
        c.output_sample_rate = Some(16000);
        c
    }
    pub(crate) async fn fake<F>(capacity: usize, backend: F) -> PiperService
    where
        F: FnMut(&str, &PiperSettings, &Cancellation, &Cancellation) -> Result<Vec<u8>>
            + Send
            + 'static,
    {
        let (ready_tx, ready_rx) = oneshot::channel();
        let mut service = PiperService::spawn(
            config(capacity),
            move |config, ready, receiver, shared| {
                let _ = ready.send(Ok(placeholder_voice()));
                work(receiver, shared, &config, backend);
            },
            ready_tx,
        )
        .unwrap();
        service.voice = ready_rx.await.unwrap().unwrap();
        service
    }
    #[tokio::test]
    async fn drain_preserves_context_voice_rate_and_closes_admission() {
        let s = fake(2, |_, _, _, _| Ok(vec![0, 1, 2, 3])).await;
        let pending = s
            .submit("hello", "context-1", PiperSettings::default())
            .unwrap();
        s.finish().unwrap();
        let result = pending.result().await.unwrap();
        assert_eq!(result.context_id, "context-1");
        assert_eq!(result.voice, "test-voice");
        assert_eq!(result.sample_rate, 16000);
        assert_eq!(result.num_channels, 1);
        s.drain().await.unwrap();
        assert!(!s.is_running());
        assert!(matches!(
            s.submit("hello", "new", PiperSettings::default()),
            Err(PiperError::Closed)
        ));
    }
    #[tokio::test]
    async fn capacity_includes_completed_uncollected_and_cancelled_native_work() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let mut entered_tx = Some(entered_tx);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let s = fake(1, move |_, _, _, _| {
            if let Some(tx) = entered_tx.take() {
                let _ = tx.send(());
                release_rx.recv().unwrap();
            }
            Ok(vec![1, 0])
        })
        .await;
        let pending = s.submit("hello", "old", PiperSettings::default()).unwrap();
        entered_rx.await.unwrap();
        drop(pending);
        assert!(matches!(
            s.submit("hi", "full", PiperSettings::default()),
            Err(PiperError::Backpressure)
        ));
        release_tx.send(()).unwrap();
        let p = loop {
            match s.submit("hi", "next", PiperSettings::default()) {
                Ok(p) => break p,
                Err(PiperError::Backpressure) => tokio::time::sleep(Duration::from_millis(1)).await,
                Err(e) => panic!("{e}"),
            }
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(matches!(
            s.submit("hi", "full", PiperSettings::default()),
            Err(PiperError::Backpressure)
        ));
        p.result().await.unwrap();
        s.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn interruption_cancels_active_queued_and_completed_but_allows_new_context() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let mut entered_tx = Some(entered_tx);
        let s = fake(3, move |text, _, cancel, _| {
            if text == "long" {
                if let Some(tx) = entered_tx.take() {
                    let _ = tx.send(());
                }
                while !cancel.is_cancelled() {
                    std::thread::sleep(Duration::from_millis(1));
                }
                cancel.check()?;
            }
            Ok(vec![1, 0])
        })
        .await;
        let active = s.submit("long", "a", PiperSettings::default()).unwrap();
        entered_rx.await.unwrap();
        let queued = s.submit("queued", "b", PiperSettings::default()).unwrap();
        s.interrupt();
        assert!(matches!(active.result().await, Err(PiperError::Cancelled)));
        assert!(matches!(queued.result().await, Err(PiperError::Cancelled)));
        let done = s.submit("ok", "c", PiperSettings::default()).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        s.interrupt();
        assert!(matches!(done.result().await, Err(PiperError::Cancelled)));
        let result = s
            .submit("fresh", "d", PiperSettings::default())
            .unwrap()
            .result()
            .await
            .unwrap();
        assert_eq!(result.generation, 2);
        s.drain().await.unwrap();
    }
    #[tokio::test]
    async fn shutdown_cancels_work_and_is_repeatable() {
        let s = fake(2, |_, _, _, stop| {
            while !stop.is_cancelled() {
                std::thread::sleep(Duration::from_millis(1));
            }
            stop.check()?;
            Ok(vec![])
        })
        .await;
        let p = s.submit("hi", "a", PiperSettings::default()).unwrap();
        let q = s.submit("hi", "b", PiperSettings::default()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), s.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(p.result().await, Err(PiperError::Cancelled)));
        assert!(matches!(q.result().await, Err(PiperError::Cancelled)));
        s.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn invalid_requests_and_empty_context_are_rejected_before_admission() {
        let s = fake(1, |_, _, _, _| panic!("must not run")).await;
        for text in ["", " ", "a\0b"] {
            assert!(s.submit(text, "ctx", PiperSettings::default()).is_err());
        }
        assert!(s.submit("hello", "", PiperSettings::default()).is_err());
        assert!(s
            .submit("x".repeat(4097), "ctx", PiperSettings::default())
            .is_err());
        s.drain().await.unwrap();
    }
    #[tokio::test]
    async fn drop_joins_without_orphaned_worker() {
        let finished = Arc::new(AtomicBool::new(false));
        let f = finished.clone();
        let (entered_tx, entered_rx) = oneshot::channel();
        let mut entered = Some(entered_tx);
        let s = fake(1, move |_, _, _, stop| {
            let _ = entered.take().unwrap().send(());
            while !stop.is_cancelled() {
                std::thread::sleep(Duration::from_millis(1));
            }
            f.store(true, Ordering::SeqCst);
            Err(PiperError::Cancelled)
        })
        .await;
        let p = s.submit("long", "ctx", PiperSettings::default()).unwrap();
        entered_rx.await.unwrap();
        drop(s);
        assert!(finished.load(Ordering::SeqCst));
        assert!(matches!(p.result().await, Err(PiperError::Cancelled)));
    }
    #[cfg(not(feature = "onnx"))]
    #[tokio::test]
    async fn feature_disabled_returns_error_without_loading_files() {
        assert!(matches!(
            PiperService::load(config(1)).await,
            Err(PiperError::Unsupported(_))
        ));
    }
    #[tokio::test]
    async fn collected_results_release_capacity_before_next_immediate_submission() {
        let s = fake(1, |_, _, _, _| Ok(vec![1, 0])).await;
        for _ in 0..5000 {
            s.submit("hi", "same-context", PiperSettings::default())
                .unwrap()
                .result()
                .await
                .unwrap();
        }
        s.drain().await.unwrap();
    }
    #[tokio::test]
    async fn context_generation_exhaustion_closes_instead_of_wrapping() {
        let s = fake(1, |_, _, _, _| Ok(vec![1, 0])).await;
        s.epoch.store(u64::MAX, Ordering::SeqCst);
        s.interrupt();
        assert_eq!(s.epoch.load(Ordering::SeqCst), u64::MAX);
        assert!(matches!(
            s.submit("hi", "last", PiperSettings::default()),
            Err(PiperError::Closed)
        ));
        s.shutdown().await.unwrap();
    }
}
