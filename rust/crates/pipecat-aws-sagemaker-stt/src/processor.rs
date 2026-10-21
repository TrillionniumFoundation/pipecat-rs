use crate::{
    protocol,
    transport::{Transport, TransportConfig, TransportSender},
    Error, SageMakerSttConfig, TransportReport,
};
use pipecat_aws_common::auth;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
    StartFrame,
};
use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};
use tokio::{sync::watch, time::Instant};

#[derive(Clone, Debug, Default, Serialize)]
pub struct Progress {
    pub pcm_bytes_captured: u64,
    pub pcm_bytes_admitted: u64,
    /// Local append bytes written to HTTP/2, not a server processing/billing receipt.
    pub pcm_bytes_uploaded: u64,
    pub chunks_admitted: u64,
    pub response_events: u64,
    pub ignored_events: u64,
    pub interim_frames: u64,
    pub final_frames: u64,
    pub delivered_interim_frames: u64,
    pub delivered_final_frames: u64,
    pub transcript_bytes: u64,
    pub output_bytes: u64,
    pub first_interim_micros: Option<u64>,
    pub first_final_micros: Option<u64>,
    pub response_task_returned: bool,
    #[serde(skip)]
    pub primary: Option<Error>,
}
pub(crate) struct Shared {
    pub progress: watch::Sender<Progress>,
    pub cancelled: watch::Sender<bool>,
    session: Mutex<Option<Transport>>,
    sender: Mutex<Option<TransportSender>>,
    admission: tokio::sync::Mutex<()>,
}
impl Shared {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            progress: watch::channel(Progress::default()).0,
            cancelled: watch::channel(false).0,
            session: Mutex::new(None),
            sender: Mutex::new(None),
            admission: tokio::sync::Mutex::new(()),
        })
    }
    pub fn cancel(&self) {
        self.cancelled.send_replace(true);
        if let Some(sender) = self
            .sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            sender.cancel();
        }
    }
    pub fn fail(&self, error: Error) {
        self.progress.send_modify(|p| {
            if p.primary.is_none() {
                p.primary = Some(error);
            }
        });
        self.cancel();
    }
    fn sender(&self) -> Result<TransportSender, Error> {
        self.sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or(Error::Task)
    }
    pub async fn transport_report(&self) -> Option<TransportReport> {
        let completion = self
            .session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(Transport::completion);
        let report = match completion {
            Some(c) => Some(c.wait().await),
            None => None,
        };
        if let Ok(sender) = self.sender() {
            self.progress
                .send_modify(|p| p.pcm_bytes_uploaded = sender.uploaded_pcm_bytes());
        }
        report
    }
}
/// Immutable one-session processor. Start/InputAudio/End are the actual native path.
/// Settings replacements fail atomically; construct another processor for another session.
pub struct SageMakerSttProcessor {
    config: SageMakerSttConfig,
    pub(crate) shared: Arc<Shared>,
}
impl SageMakerSttProcessor {
    pub fn new(config: SageMakerSttConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            config,
            shared: Shared::new(),
        })
    }
    pub fn progress(&self) -> Progress {
        self.shared.progress.borrow().clone()
    }
    pub fn cancel(&self) {
        self.shared.cancel();
    }
    async fn audio(&self, audio: &pipecat_core::AudioRawFrame) -> Result<(), Error> {
        let _admission = self.shared.admission.lock().await;
        if *self.shared.cancelled.borrow() {
            return Err(Error::Cancelled);
        }
        if audio.sample_rate != self.config.settings.sample_rate_hz
            || audio.num_channels != 1
            || audio.context_id.is_some()
        {
            return Err(Error::Invalid("immutable PCM format"));
        }
        let frames = protocol::audio(&audio.audio, &self.config.limits)?;
        let p = self.progress();
        let next = p
            .pcm_bytes_admitted
            .checked_add(audio.audio.len() as u64)
            .ok_or(Error::TooLarge)?;
        if next > self.config.limits.max_pcm_bytes as u64
            || next
                > u64::from(self.config.settings.sample_rate_hz)
                    * 2
                    * u64::from(self.config.limits.max_audio_seconds)
            || p.chunks_admitted >= self.config.limits.max_chunks as u64
        {
            return Err(Error::TooLarge);
        }
        self.shared
            .sender()?
            .send_audio(frames, audio.audio.len())
            .await?;
        self.shared.progress.send_modify(|p| {
            p.pcm_bytes_admitted = next;
            p.chunks_admitted += 1;
        });
        Ok(())
    }
}
fn processor_error(error: Error) -> ProcessorError {
    ProcessorError::fatal(error.to_string())
}
#[async_trait]
impl FrameProcessor for SageMakerSttProcessor {
    fn name(&self) -> &str {
        "NvidiaSageMakerStt"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if start.audio_in_sample_rate != self.config.settings.sample_rate_hz {
            return Err(processor_error(Error::Invalid("pipeline sample rate")));
        }
        if *self.shared.cancelled.borrow() {
            return Err(processor_error(Error::Cancelled));
        }
        let url = self.config.url().map_err(processor_error)?;
        let (headers, signer) = auth::signed_event_stream_post_headers(
            &url,
            &self.config.region,
            &self.config.credentials,
            SystemTime::now(),
            "sagemaker",
            self.config.limits.duplex.max_input_frame_bytes + 128,
        )
        .map_err(|_| processor_error(Error::Invalid("signing")))?;
        let startup = protocol::session(
            &self.config.settings,
            self.config.limits.duplex.max_input_frame_bytes,
        )
        .map_err(processor_error)?;
        let mut slot = self
            .shared
            .session
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if slot.is_some() {
            return Err(processor_error(Error::Invalid("single-use processor")));
        }
        let mut transport = Transport::start(
            TransportConfig {
                endpoint: url,
                connect_address: self.config.address,
                headers,
                limits: self.config.limits.duplex.clone(),
            },
            vec![startup],
            signer,
        )
        .map_err(|e| processor_error(e.into()))?;
        let mut receiver = transport
            .take_receiver()
            .ok_or_else(|| processor_error(Error::Task))?;
        *self.shared.sender.lock().unwrap_or_else(|e| e.into_inner()) = Some(transport.sender());
        *slot = Some(transport);
        drop(slot);
        let shared = self.shared.clone();
        let config = self.config.clone();
        let output = ctx.clone();
        let began = Instant::now();
        ctx.spawn("sagemaker-stt-responses", false, async move {
            let result: Result<(), Error> = async {
                while let Some(raw) = receiver.recv().await {
                    let count = shared.progress.borrow().response_events;
                    if count >= config.limits.max_response_events as u64 {
                        return Err(Error::TooLarge);
                    }
                    shared.progress.send_modify(|p| p.response_events += 1);
                    if let Some(data) = protocol::response(&raw, &config.settings, &config.limits)?
                    {
                        let (text_bytes, finalized) = match &data {
                            FrameData::TranscriptionDetailed(f)
                            | FrameData::InterimTranscriptionDetailed(f) => {
                                (f.text.text.len(), f.finalized)
                            }
                            _ => unreachable!(),
                        };
                        let serialized = serde_json::to_vec(&data).map_err(|_| Error::Output)?;
                        let p = shared.progress.borrow().clone();
                        if p.transcript_bytes + text_bytes as u64
                            > config.limits.max_transcript_bytes as u64
                            || p.output_bytes + serialized.len() as u64
                                > config.limits.max_output_bytes as u64
                        {
                            return Err(Error::TooLarge);
                        }
                        let elapsed = began.elapsed().as_micros().min(u64::MAX as u128) as u64;
                        shared.progress.send_modify(|p| {
                            p.transcript_bytes += text_bytes as u64;
                            p.output_bytes += serialized.len() as u64;
                            if finalized {
                                p.final_frames += 1;
                                p.first_final_micros.get_or_insert(elapsed);
                            } else {
                                p.interim_frames += 1;
                                p.first_interim_micros.get_or_insert(elapsed);
                            }
                        });
                        tokio::time::timeout(
                            config.limits.output_timeout,
                            output.push(Frame::new(data), Direction::Downstream),
                        )
                        .await
                        .map_err(|_| Error::Output)?
                        .map_err(|_| Error::Output)?;
                    } else {
                        shared.progress.send_modify(|p| p.ignored_events += 1);
                    }
                }
                Ok(())
            }
            .await;
            shared
                .progress
                .send_modify(|p| p.response_task_returned = true);
            if let Err(error) = result {
                shared.fail(error.clone());
                return Err(processor_error(error));
            }
            Ok(())
        })?;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame
            .require_no_native_llm_continuation()
            .map_err(|_| processor_error(Error::Invalid("native continuation")))?;
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        match &frame.data {
            FrameData::InputAudio(audio) => {
                let result = self.audio(audio).await;
                if let Err(error) = &result {
                    self.shared.fail(error.clone());
                }
                result.map_err(processor_error)
            }
            FrameData::SttUpdateSettings(_) | FrameData::ServiceUpdateSettings(_) => {
                Err(ProcessorError::new("finite STT settings are immutable")
                    .with_permanence_override(false))
            }
            FrameData::Stop => {
                self.shared.cancel();
                ctx.push(frame, direction).await
            }
            data if data.is_cancel() => {
                self.shared.cancel();
                ctx.push(frame, direction).await
            }
            _ => ctx.push(frame, direction).await,
        }
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if *self.shared.cancelled.borrow() {
            return Ok(());
        }
        let _admission = self.shared.admission.lock().await;
        let sender = self.shared.sender().map_err(processor_error)?;
        if let Err(error) = sender
            .end(vec![
                protocol::end(&self.config.limits).map_err(processor_error)?
            ])
            .await
        {
            self.shared.fail(error.clone().into());
            return Err(processor_error(error.into()));
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        // On graceful End the runtime has already drained the response task.
        // Await the transport before cancelling, so a completed EOF remains success.
        if !self.progress().response_task_returned {
            self.shared.cancel();
        }
        let report = self.shared.transport_report().await;
        if let Some(report) = report {
            if let Some(error) = report.primary.or(report.cleanup) {
                self.shared.progress.send_modify(|p| {
                    if p.primary.is_none() {
                        p.primary = Some(error.into());
                    }
                });
            }
        }
        Ok(())
    }
}
