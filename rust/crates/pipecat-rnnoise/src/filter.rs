use crate::{
    create_engine,
    stream::{check_cancel, validate_format, Stream},
    Engine, Result, RnNoiseConfig, RnNoiseError,
};
use async_trait::async_trait;
use pipecat_audio::PcmFormat;
use pipecat_core::{FrameData, ProcessorError};
use pipecat_media_transport::AudioFilter;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex, Weak,
};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

type Factory = Box<dyn Fn(&RnNoiseConfig) -> Result<Box<dyn Engine>> + Send>;
enum Operation {
    Start(PcmFormat),
    Filter(Vec<u8>),
    Enable(bool),
    Stop,
}

struct ReplySlot {
    result: Option<Result<Vec<u8>>>,
    permit: Option<OwnedSemaphorePermit>,
    completed: bool,
}
struct Completion {
    cancelled: AtomicBool,
    slot: Mutex<ReplySlot>,
}
impl Completion {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            slot: Mutex::new(ReplySlot {
                result: None,
                permit: Some(permit),
                completed: false,
            }),
        }
    }
    fn cancel(&self) {
        // Serialize revocation against reply publication and final delivery.
        let mut slot = self.slot.lock().unwrap_or_else(|error| error.into_inner());
        self.cancelled.store(true, Ordering::Release);
        if slot.completed {
            slot.result = None;
            slot.permit = None;
        }
        // Running work keeps admission until the worker acknowledges completion.
    }
    fn finish(&self, result: Result<Vec<u8>>) {
        let mut slot = self.slot.lock().unwrap_or_else(|error| error.into_inner());
        if slot.completed {
            return;
        }
        slot.completed = true;
        if self.cancelled.load(Ordering::Acquire) {
            slot.permit = None;
        } else {
            slot.result = Some(result);
        }
    }
    fn take(&self) -> Result<Vec<u8>> {
        let mut slot = self.slot.lock().unwrap_or_else(|error| error.into_inner());
        slot.permit = None;
        if self.cancelled.load(Ordering::Acquire) {
            return Err(RnNoiseError::Cancelled);
        }
        slot.result.take().unwrap_or(Err(RnNoiseError::Worker))
    }
}
struct Request {
    operation: Option<Operation>,
    completion: Arc<Completion>,
    reply: Option<oneshot::Sender<()>>,
}
impl Drop for Request {
    fn drop(&mut self) {
        // Also releases admission if native-worker Rust code unwinds or a send
        // fails. Completion already published by the worker is not overwritten.
        self.completion.finish(Err(RnNoiseError::Worker));
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(());
        }
    }
}
struct Cancellation {
    completion: Arc<Completion>,
    armed: bool,
}
impl Drop for Cancellation {
    fn drop(&mut self) {
        if self.armed {
            self.completion.cancel();
        }
    }
}

/// Bounded, owned native worker implementing the media transport's audio filter.
///
/// One request, including an uncollected reply, is admitted at a time. Overlap
/// returns backpressure. Start/stop revoke the previous reply and cancel running
/// work before waiting for admission. Cancelled native work retains admission
/// until the worker acknowledges it; an unpolled receiver cannot delay lifecycle.
/// Dropping an operation future invalidates that stream; call start to restart.
/// Stop discards incomplete frames and resampler tails, matching source stop.
/// The trait has no flush method. Empty audio does not pad or flush a tail.
/// Drop joins the worker and can wait for shared native-call lock contention and
/// current trusted C work. C cannot be preempted or protected from native faults.
pub struct RnNoiseFilter {
    config: RnNoiseConfig,
    sender: Option<mpsc::Sender<Request>>,
    worker: Option<std::thread::JoinHandle<()>>,
    admission: Arc<Semaphore>,
    current: Mutex<Weak<Completion>>,
    stopping: AtomicBool,
    lifecycle: tokio::sync::Mutex<()>,
}
impl RnNoiseFilter {
    pub fn new(config: RnNoiseConfig) -> Result<Self> {
        Self::with_factory(config, Box::new(create_engine))
    }
    fn with_factory(config: RnNoiseConfig, factory: Factory) -> Result<Self> {
        config.validate()?;
        let (sender, receiver) = mpsc::channel(1);
        let worker_config = config.clone();
        let worker = std::thread::Builder::new()
            .name("pipecat-rnnoise".into())
            .spawn(move || worker(receiver, worker_config, factory))?;
        Ok(Self {
            config,
            sender: Some(sender),
            worker: Some(worker),
            admission: Arc::new(Semaphore::new(1)),
            current: Mutex::new(Weak::new()),
            stopping: AtomicBool::new(false),
            lifecycle: tokio::sync::Mutex::new(()),
        })
    }
    async fn request(&self, operation: Operation, wait: bool) -> Result<Vec<u8>> {
        let permit = if wait {
            self.admission
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| RnNoiseError::Worker)?
        } else {
            self.admission
                .clone()
                .try_acquire_owned()
                .map_err(|_| RnNoiseError::Backpressure)?
        };
        let completion = Arc::new(Completion::new(permit));
        {
            let mut current = self.current.lock().map_err(|_| RnNoiseError::Worker)?;
            if !wait
                && matches!(&operation, Operation::Filter(_))
                && self.stopping.load(Ordering::Acquire)
            {
                return Err(RnNoiseError::NotStarted);
            }
            *current = Arc::downgrade(&completion);
        }
        let mut guard = Cancellation {
            completion: completion.clone(),
            armed: true,
        };
        let (reply, response) = oneshot::channel();
        self.sender
            .as_ref()
            .ok_or(RnNoiseError::Worker)?
            .try_send(Request {
                operation: Some(operation),
                completion: completion.clone(),
                reply: Some(reply),
            })
            .map_err(|_| RnNoiseError::Worker)?;
        response.await.map_err(|_| RnNoiseError::Worker)?;
        let result = completion.take();
        guard.armed = false;
        result
    }
    fn begin_lifecycle(&self) {
        let current = self
            .current
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.stopping.store(true, Ordering::Release);
        if let Some(completion) = current.upgrade() {
            completion.cancel();
        }
    }
    fn cancel_current(&self) {
        if let Some(completion) = self
            .current
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .upgrade()
        {
            completion.cancel();
        }
    }
}
#[async_trait]
impl AudioFilter for RnNoiseFilter {
    async fn start(&self, format: PcmFormat) -> std::result::Result<(), ProcessorError> {
        validate_format(format, self.config.rate_conversion).map_err(error)?;
        let _lifecycle = self
            .lifecycle
            .try_lock()
            .map_err(|_| error(RnNoiseError::LifecycleBusy))?;
        self.begin_lifecycle();
        self.request(Operation::Start(format), true)
            .await
            .map_err(error)?;
        self.stopping.store(false, Ordering::Release);
        Ok(())
    }
    async fn filter(&self, audio: Vec<u8>) -> std::result::Result<Vec<u8>, ProcessorError> {
        if audio.len() > self.config.max_input_bytes {
            return Err(error(RnNoiseError::TooLarge));
        }
        self.request(Operation::Filter(audio), false)
            .await
            .map_err(error)
    }
    async fn control(&self, frame: &FrameData) -> std::result::Result<(), ProcessorError> {
        if let FrameData::FilterEnable(enable) = frame {
            self.request(Operation::Enable(*enable), false)
                .await
                .map_err(error)?;
        }
        Ok(())
    }
    async fn stop(&self) -> std::result::Result<(), ProcessorError> {
        let _lifecycle = self
            .lifecycle
            .try_lock()
            .map_err(|_| error(RnNoiseError::LifecycleBusy))?;
        self.begin_lifecycle();
        self.request(Operation::Stop, true).await.map_err(error)?;
        Ok(())
    }
}
impl Drop for RnNoiseFilter {
    fn drop(&mut self) {
        self.cancel_current();
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
fn worker(mut receiver: mpsc::Receiver<Request>, config: RnNoiseConfig, factory: Factory) {
    let mut stream: Option<Stream> = None;
    let mut enabled = true;
    let mut previous: Option<Arc<Completion>> = None;
    while let Some(mut request) = receiver.blocking_recv() {
        if previous
            .as_ref()
            .is_some_and(|completion| completion.cancelled.load(Ordering::Acquire))
        {
            stream = None;
        }
        previous = Some(request.completion.clone());
        let cancelled = &request.completion.cancelled;
        let operation = request.operation.take().expect("owned worker operation");
        let mut result = check_cancel(cancelled).and_then(|()| match operation {
            Operation::Start(format) => {
                stream = None;
                stream = Some(Stream::new(factory(&config)?, format)?);
                Ok(Vec::new())
            }
            Operation::Filter(audio) => {
                let active = stream.as_mut().ok_or(RnNoiseError::NotStarted)?;
                if enabled {
                    active.process(audio, cancelled)
                } else {
                    Ok(audio)
                }
            }
            Operation::Enable(value) => {
                enabled = value;
                Ok(Vec::new())
            }
            Operation::Stop => {
                stream = None;
                Ok(Vec::new())
            }
        });
        if cancelled.load(Ordering::Acquire) {
            result = Err(RnNoiseError::Cancelled);
        }
        if matches!(
            result,
            Err(RnNoiseError::Cancelled | RnNoiseError::Native(_) | RnNoiseError::Worker)
        ) {
            stream = None;
        }
        request.completion.finish(result);
        // Request Drop sends only a completion notification, never owned PCM.
    }
}
fn error(error: RnNoiseError) -> ProcessorError {
    ProcessorError::new(error.to_string())
}
#[cfg(test)]
mod review_tests;
#[cfg(test)]
mod tests;
