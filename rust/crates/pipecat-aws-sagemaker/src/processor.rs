use crate::{protocol, transport, Error, SageMakerConfig, SynthesisSettings};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
    ServiceUpdateSettingsFrame, Setting, TtsSettings,
};
use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub struct SageMakerClient {
    pub(crate) config: SageMakerConfig,
}
impl SageMakerClient {
    pub fn new(config: SageMakerConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self { config })
    }
}
/// Counts describe local work only. Delivered means caller process() returned OK,
/// not physical playback or a durable file. Skipped utterances have no HTTP/TTS.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SageMakerProgress {
    pub utterances_seen: u64,
    pub admitted: u64,
    pub skipped: u64,
    pub completed_at_eof: u64,
    pub owners_released: u64,
    pub failed: u64,
    pub cancelled_or_aborted: u64,
    pub pcm_bytes_admitted: u64,
    pub pcm_bytes_delivered: u64,
    pub wire_bytes: u64,
    pub frames_admitted: u64,
    pub frames_delivered: u64,
    pub partial_utterances: u64,
    /// Stripped characters accepted into the local FIFO (including later cancels).
    pub admitted_text_characters: u64,
    /// Stripped characters whose request attempt returned; not billed/remote usage.
    pub usage_text_characters: u64,
    pub ttfb_observations: u64,
    /// Request-path first nonempty body byte, excluding initial caller Start wait.
    pub last_ttfb_micros: Option<u64>,
    pub primary: Option<Error>,
}
#[derive(Default)]
pub(crate) struct InvocationStats {
    pub delivered: u64,
    pub admitted: u64,
}
pub(crate) async fn wait_cancel(receiver: &mut watch::Receiver<bool>) {
    loop {
        if *receiver.borrow_and_update() || receiver.changed().await.is_err() {
            return;
        }
    }
}
struct Pending {
    id: u64,
    bytes: usize,
    stats: Arc<Mutex<InvocationStats>>,
    ack: watch::Sender<Option<bool>>,
}
pub(crate) struct OutputFlow {
    pub installed: AtomicBool,
    pending: Mutex<Option<Pending>>,
    progress: watch::Sender<SageMakerProgress>,
    max_bytes: usize,
}
impl OutputFlow {
    fn new(progress: watch::Sender<SageMakerProgress>, max_bytes: usize) -> Self {
        Self {
            installed: AtomicBool::new(false),
            pending: Mutex::new(None),
            progress,
            max_bytes,
        }
    }
    pub fn begin(
        &self,
        id: u64,
        bytes: usize,
        stats: Arc<Mutex<InvocationStats>>,
    ) -> Result<OutputTicket<'_>, Error> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.is_some() {
            return Err(Error::Output);
        }
        let p = self.progress.borrow();
        if bytes as u64 > (self.max_bytes as u64).saturating_sub(p.pcm_bytes_admitted)
            || (bytes > 0 && p.frames_admitted >= 262_144)
        {
            return Err(Error::TooLarge);
        }
        drop(p);
        let (ack, receiver) = watch::channel(None);
        *pending = Some(Pending {
            id,
            bytes,
            stats: stats.clone(),
            ack,
        });
        Ok(OutputTicket {
            flow: self,
            id,
            bytes,
            stats,
            receiver,
        })
    }
    pub fn acknowledge(&self, id: u64, success: bool) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.as_ref().is_some_and(|p| p.id == id) {
            if let Some(p) = pending.take() {
                if success {
                    p.stats.lock().unwrap_or_else(|e| e.into_inner()).delivered += p.bytes as u64;
                    self.progress.send_modify(|s| {
                        s.pcm_bytes_delivered += p.bytes as u64;
                        s.frames_delivered += 1;
                    });
                }
                p.ack.send_replace(Some(success));
            }
        }
    }
}
pub(crate) struct OutputTicket<'a> {
    flow: &'a OutputFlow,
    id: u64,
    bytes: usize,
    stats: Arc<Mutex<InvocationStats>>,
    receiver: watch::Receiver<Option<bool>>,
}
impl OutputTicket<'_> {
    pub fn admitted(&self) {
        self.stats
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .admitted += self.bytes as u64;
        self.flow.progress.send_modify(|s| {
            s.pcm_bytes_admitted += self.bytes as u64;
            s.frames_admitted += 1;
        });
    }
    pub async fn wait(&mut self) -> Result<(), Error> {
        loop {
            if let Some(ok) = *self.receiver.borrow_and_update() {
                return if ok { Ok(()) } else { Err(Error::Output) };
            }
            self.receiver.changed().await.map_err(|_| Error::Output)?;
        }
    }
}
impl Drop for OutputTicket<'_> {
    fn drop(&mut self) {
        let mut p = self.flow.pending.lock().unwrap_or_else(|e| e.into_inner());
        if p.as_ref().is_some_and(|p| p.id == self.id) {
            p.take();
        }
    }
}
pub(crate) struct Shared {
    pub progress: watch::Sender<SageMakerProgress>,
    pub cancel: watch::Sender<bool>,
    pub output: OutputFlow,
    pub cleanup: Mutex<Vec<Error>>,
}
impl Shared {
    fn new(max_bytes: usize) -> Self {
        let progress = watch::channel(SageMakerProgress::default()).0;
        Self {
            output: OutputFlow::new(progress.clone(), max_bytes),
            progress,
            cancel: watch::channel(false).0,
            cleanup: Mutex::new(Vec::new()),
        }
    }
    pub fn cancel(&self) {
        self.cancel.send_replace(true);
    }
}
struct Owner {
    shared: Arc<Shared>,
    stats: Arc<Mutex<InvocationStats>>,
    _count: OwnedSemaphorePermit,
    _bytes: OwnedSemaphorePermit,
    finished: bool,
    succeeded: bool,
}
impl Owner {
    fn finish(&mut self, result: &Result<(), Error>) {
        self.finished = true;
        self.succeeded = result.is_ok();
        self.shared.progress.send_modify(|p| match result {
            Ok(()) => p.completed_at_eof += 1,
            Err(Error::Cancelled) => {
                p.cancelled_or_aborted += 1;
                if p.primary.is_none() {
                    p.primary = Some(Error::Cancelled);
                }
            }
            Err(error) => {
                p.failed += 1;
                if p.primary.is_none() {
                    p.primary = Some(error.clone());
                }
            }
        });
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let stats = self.stats.lock().unwrap_or_else(|e| e.into_inner());
        self.shared.progress.send_modify(|p| {
            p.owners_released += 1;
            if !self.finished {
                p.cancelled_or_aborted += 1;
            }
            // An aborted/failed invocation may have delivered a genuine prefix.
            if stats.delivered > 0 && !self.succeeded {
                p.partial_utterances += 1;
            }
        });
    }
}
/// Complete TTSSpeak processor. Immediately upstream ingress and downstream output
/// guards are required. Each interruptible HTTP future belongs to the core registry.
pub struct SageMakerProcessor {
    client: SageMakerClient,
    shared: Arc<Shared>,
    settings: Mutex<SynthesisSettings>,
    count: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    fifo: Arc<tokio::sync::Mutex<()>>,
}
impl SageMakerProcessor {
    pub fn new(client: SageMakerClient) -> Self {
        let limits = client.config.limits();
        Self {
            count: Arc::new(Semaphore::new(limits.max_pending_invocations)),
            bytes: Arc::new(Semaphore::new(limits.max_pending_bytes)),
            shared: Arc::new(Shared::new(limits.max_output_bytes)),
            settings: Mutex::new(client.config.settings().clone()),
            client,
            fifo: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
    pub fn progress(&self) -> SageMakerProgress {
        self.shared.progress.borrow().clone()
    }
    pub fn cancel(&self) {
        self.shared.cancel();
    }
    pub fn ingress_guard(&self) -> Arc<dyn FrameProcessor> {
        Arc::new(Ingress(self.shared.clone()))
    }
    pub fn output_guard(&self, output: Arc<dyn FrameProcessor>) -> Arc<dyn FrameProcessor> {
        crate::app::BoundedOutput::new(output, self.client.config.limits(), self.shared.clone())
    }
    pub(crate) fn shared(&self) -> Arc<Shared> {
        self.shared.clone()
    }
    fn update(&self, update: &ServiceUpdateSettingsFrame<TtsSettings>) -> Result<(), Error> {
        if update.service.is_some()
            || update.reach_inactive_services
            || update.delta.is_some() && !update.settings.is_empty()
        {
            return Err(Error::Unsupported("targeted or mixed settings"));
        }
        let mut current = self.settings.lock().unwrap_or_else(|e| e.into_inner());
        let mut next = current.clone();
        if let Some(delta) = &update.delta {
            if !matches!(delta.base.model, Setting::NotGiven) || !delta.base.extra.is_empty() {
                return Err(Error::Unsupported("model/extra settings"));
            }
            match &delta.voice {
                Setting::NotGiven => {}
                Setting::Value(v) => {
                    if v.len() > 256 {
                        return Err(Error::TooLarge);
                    }
                    next.voice_name = v.clone()
                }
                Setting::Null => return Err(Error::Invalid("null voice")),
            }
            match &delta.language {
                Setting::NotGiven => {}
                Setting::Value(v) => {
                    if v.len() > 63 {
                        return Err(Error::TooLarge);
                    }
                    next.language_code = v.clone()
                }
                Setting::Null => return Err(Error::Invalid("null language")),
            }
        } else {
            if update.settings.len() > 2 {
                return Err(Error::Invalid("unknown settings"));
            }
            for (key, value) in &update.settings {
                let value = value.as_str().ok_or(Error::Invalid("settings value"))?;
                if value.len() > 256 {
                    return Err(Error::TooLarge);
                }
                match key.as_str() {
                    "voice" => next.voice_name = value.to_owned(),
                    "language" => next.language_code = value.to_owned(),
                    _ => return Err(Error::Invalid("unknown settings")),
                }
            }
        }
        next.validate()?;
        *current = next;
        Ok(())
    }
    async fn admit(
        &self,
        text: &str,
        append_to_context: bool,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.shared.progress.send_modify(|p| p.utterances_seen += 1);
        if *self.shared.cancel.borrow() {
            return Err(
                ProcessorError::new(Error::Cancelled.to_string()).with_permanence_override(false)
            );
        }
        let limits = self.client.config.limits();
        if self.shared.progress.borrow().utterances_seen > limits.max_utterances as u64 {
            return Err(ProcessorError::fatal(
                "SageMaker finite utterance count exceeded",
            ));
        }
        let settings = self
            .settings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let payload = protocol::payload(text, &settings, limits)
            .map_err(|e| ProcessorError::fatal(e.to_string()))?;
        let Some(payload) = payload else {
            self.shared.progress.send_modify(|p| {
                p.skipped += 1;
            });
            return Ok(());
        };
        if !self.shared.output.installed.load(Ordering::Acquire) {
            return Err(ProcessorError::fatal(
                "SageMaker requires immediately downstream output guard",
            ));
        }
        let count =
            self.count.clone().try_acquire_owned().map_err(|_| {
                ProcessorError::fatal("SageMaker pending invocation count exceeded")
            })?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(payload.capacity().max(1) as u32)
            .map_err(|_| ProcessorError::fatal("SageMaker pending bytes exceeded"))?;
        let mut lock = Box::pin(self.fifo.clone().lock_owned());
        let acquired = poll_fn(|cx| Poll::Ready(lock.as_mut().poll(cx))).await;
        let client = self.client.clone();
        let shared = self.shared.clone();
        let output = ctx.clone();
        let mut cancelled = shared.cancel.subscribe();
        let stats = Arc::new(Mutex::new(InvocationStats::default()));
        let mut owner = Owner {
            shared: shared.clone(),
            stats: stats.clone(),
            _count: count,
            _bytes: bytes,
            finished: false,
            succeeded: false,
        };
        let sequence = self.shared.progress.borrow().utterances_seen;
        let characters = protocol::normalized(text, limits)
            .map_err(|e| ProcessorError::fatal(e.to_string()))?
            .map_or(0, |t| t.chars().count());
        self.shared.progress.send_modify(|p| {
            p.admitted += 1;
            p.admitted_text_characters += characters as u64;
        });
        ctx.spawn("sagemaker-invocation",true,async move{
            let work=async{
                let _fifo=match acquired{Poll::Ready(guard)=>guard,Poll::Pending=>lock.await};
                if !output.is_current()||*shared.cancel.borrow(){return Err(Error::Cancelled);}
                let result=transport::invoke(&client.config,transport::Invocation{body:payload,rate:settings.sample_rate_hz,sequence,append_to_context,text_characters:characters as u64},&output,&shared,&stats).await;
                if let Err(error)=&result{if *error!=Error::Cancelled{
                    shared.progress.send_modify(|p|{if p.primary.is_none(){p.primary=Some(error.clone());}});
                    shared.cancel();
                }}
                result
            };
            let result=tokio::select!{biased;_=wait_cancel(&mut cancelled)=>Err(Error::Cancelled),result=work=>result};
            owner.finish(&result);
            match result{Err(Error::Cancelled)=>Ok(()),other=>other.map_err(|e|ProcessorError::new(e.to_string()).with_permanence_override(false))}
        })?;
        Ok(())
    }
}
struct Ingress(Arc<Shared>);
#[async_trait]
impl FrameProcessor for Ingress {
    fn name(&self) -> &str {
        "SageMakerLifecycleIngress"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && (matches!(frame.data, FrameData::Stop) || frame.data.is_cancel())
        {
            self.0.cancel();
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cancel();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for SageMakerProcessor {
    fn name(&self) -> &str {
        "AWSSageMakerProcessor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame
            .require_no_native_llm_continuation()
            .map_err(|_| ProcessorError::fatal("SageMaker rejects native authority carriers"))?;
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        match &frame.data {
            FrameData::TtsSpeak(speak) => {
                self.admit(&speak.text, speak.append_to_context, ctx).await
            }
            FrameData::TtsUpdateSettings(update) => self
                .update(update)
                .map_err(|e| ProcessorError::new(e.to_string()).with_permanence_override(false)),
            FrameData::ServiceUpdateSettings(_) => {
                Err(ProcessorError::new("SageMaker requires typed TTS settings")
                    .with_permanence_override(false))
            }
            _ => ctx.push(frame, direction).await,
        }
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.shared.cancel();
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancelled_credit_cannot_acknowledge_fresh_output() {
        let progress = watch::channel(SageMakerProgress::default()).0;
        let flow = OutputFlow::new(progress.clone(), 32);
        let old = flow
            .begin(1, 2, Arc::new(Mutex::new(InvocationStats::default())))
            .unwrap();
        drop(old);
        let mut fresh = flow
            .begin(2, 2, Arc::new(Mutex::new(InvocationStats::default())))
            .unwrap();
        fresh.admitted();
        flow.acknowledge(1, true);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), fresh.wait())
                .await
                .is_err()
        );
        flow.acknowledge(2, true);
        fresh.wait().await.unwrap();
        assert_eq!(progress.borrow().pcm_bytes_delivered, 2);
    }
    #[tokio::test]
    async fn exhausted_audio_frame_budget_retains_terminal_control_credit() {
        let progress = watch::channel(SageMakerProgress::default()).0;
        let flow = OutputFlow::new(progress.clone(), 2);
        progress.send_modify(|p| {
            p.frames_admitted = 262_144;
            p.pcm_bytes_admitted = 2;
        });
        let stats = Arc::new(Mutex::new(InvocationStats::default()));
        assert!(matches!(
            flow.begin(1, 2, stats.clone()),
            Err(Error::TooLarge)
        ));
        let mut stop = flow.begin(2, 0, stats).unwrap();
        stop.admitted();
        flow.acknowledge(2, true);
        stop.wait().await.unwrap();
        assert_eq!(progress.borrow().frames_delivered, 1);
        assert_eq!(progress.borrow().pcm_bytes_delivered, 0);
    }
    #[test]
    fn typed_and_legacy_updates_are_atomic() {
        let config = SageMakerConfig::new(
            "us-east-1",
            "test",
            pipecat_aws_common::AwsCredentials::new("KEY", "secret", None).unwrap(),
        )
        .unwrap()
        .with_loopback_endpoint("http://127.0.0.1:1")
        .unwrap();
        let p = SageMakerProcessor::new(SageMakerClient::new(config).unwrap());
        let mut update = ServiceUpdateSettingsFrame::default();
        update
            .settings
            .insert("voice".into(), serde_json::json!("other"));
        update
            .settings
            .insert("rate".into(), serde_json::json!(24000));
        assert!(p.update(&update).is_err());
        assert_eq!(*p.settings.lock().unwrap(), SynthesisSettings::default());
        update.settings.remove("rate");
        p.update(&update).unwrap();
        assert_eq!(p.settings.lock().unwrap().voice_name, "other");
        update.settings.clear();
        update.delta = Some(TtsSettings {
            voice: Setting::Value("next".into()),
            language: Setting::Null,
            ..Default::default()
        });
        assert!(p.update(&update).is_err());
        assert_eq!(p.settings.lock().unwrap().voice_name, "other");
    }
}
