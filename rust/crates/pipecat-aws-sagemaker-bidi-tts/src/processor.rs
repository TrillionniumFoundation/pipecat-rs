use crate::{
    protocol::{self, Response},
    transport::{Transport, TransportConfig, TransportSender},
    Error, SageMakerBidiTtsConfig, TransportReport,
};
use pipecat_aws_common::auth;
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, ProcessorContext,
    ProcessorError, TtsStartedFrame, TtsStoppedFrame,
};
use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::SystemTime,
};
use tokio::{
    sync::watch,
    time::{timeout, Instant},
};

#[derive(Clone, Debug, Default, Serialize)]
pub struct Progress {
    pub admitted: bool,
    pub skipped: bool,
    pub interrupted: bool,
    pub speech_completed: bool,
    /// Receipt of any response while interrupting is NOT proof of remote stop.
    pub interruption_response_observed: bool,
    pub response_events: u64,
    pub pcm_bytes_admitted: u64,
    pub pcm_bytes_delivered: u64,
    pub response_task_returned: bool,
    #[serde(skip)]
    pub primary: Option<Error>,
}
pub(crate) struct Shared {
    pub progress: watch::Sender<Progress>,
    pub control: watch::Sender<u8>,
    session: Mutex<Option<Transport>>,
    sender: Mutex<Option<TransportSender>>,
}
impl Shared {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            progress: watch::channel(Progress::default()).0,
            control: watch::channel(0).0,
            session: Mutex::new(None),
            sender: Mutex::new(None),
        })
    }
    pub fn interrupt(&self) {
        self.control.send_modify(|v| {
            if *v == 0 {
                *v = 1;
            }
        });
    }
    pub fn cancel(&self) {
        self.control.send_replace(2);
        if let Some(s) = self
            .sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            s.cancel();
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
    pub async fn transport_report(&self) -> Option<TransportReport> {
        let c = self
            .session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(Transport::completion);
        match c {
            Some(c) => Some(c.wait().await),
            None => None,
        }
    }
}
pub(crate) async fn changed_control(rx: &mut watch::Receiver<u8>) -> u8 {
    loop {
        let v = *rx.borrow_and_update();
        if v != 0 {
            return v;
        }
        if rx.changed().await.is_err() {
            return 2;
        }
    }
}
/// Single-use utterance processor. The finite app constructs one per context and
/// joins all actual owners before starting the next. No automatic replay.
pub struct SageMakerBidiTtsProcessor {
    config: SageMakerBidiTtsConfig,
    context_id: String,
    pub(crate) shared: Arc<Shared>,
}
impl SageMakerBidiTtsProcessor {
    pub fn new(
        config: SageMakerBidiTtsConfig,
        context_id: impl Into<String>,
    ) -> Result<Self, Error> {
        config.validate()?;
        let context_id = context_id.into();
        if context_id.is_empty()
            || context_id.len() > 128
            || context_id.chars().any(char::is_control)
        {
            return Err(Error::Invalid("context ID"));
        }
        Ok(Self {
            config,
            context_id,
            shared: Shared::new(),
        })
    }
    pub fn progress(&self) -> Progress {
        self.shared.progress.borrow().clone()
    }
    pub fn cancel(&self) {
        self.shared.cancel();
    }
    async fn admit(
        &self,
        text: &str,
        append_to_context: bool,
        ctx: &ProcessorContext,
    ) -> Result<(), Error> {
        if self.progress().admitted || self.progress().skipped {
            return Err(Error::Invalid("single-use utterance"));
        }
        if *self.shared.control.borrow() != 0 {
            return Err(Error::Cancelled);
        }
        let Some(startup) = protocol::startup(text, &self.config.settings, &self.config.limits)?
        else {
            self.shared.progress.send_modify(|p| {
                p.skipped = true;
                p.response_task_returned = true;
            });
            return Ok(());
        };
        let url = self.config.url()?;
        let (headers, signer) = auth::signed_event_stream_post_headers(
            &url,
            &self.config.region,
            &self.config.credentials,
            SystemTime::now(),
            "sagemaker",
            self.config.limits.duplex.max_input_frame_bytes + 128,
        )
        .map_err(|_| Error::Invalid("signing"))?;
        let mut transport = Transport::start(
            TransportConfig {
                endpoint: url,
                connect_address: self.config.address,
                headers,
                limits: self.config.limits.duplex.clone(),
            },
            startup,
            signer,
        )?;
        let receiver = transport.take_receiver().ok_or(Error::Task)?;
        let sender = transport.sender();
        *self.shared.sender.lock().unwrap_or_else(|e| e.into_inner()) = Some(sender.clone());
        *self
            .shared
            .session
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(transport);
        self.shared.progress.send_modify(|p| p.admitted = true);
        let shared = self.shared.clone();
        let config = self.config.clone();
        let output = ctx.clone();
        let id = self.context_id.clone();
        // Non-interruptible registry owner performs protocol close and joins. Its
        // old-generation output is still fenced by the native core context.
        ctx.spawn("sagemaker-bidi-tts-response", false, async move {
            let result = run(
                config,
                shared.clone(),
                output,
                id,
                append_to_context,
                sender,
                receiver,
            )
            .await;
            if let Err(e) = result {
                shared.fail(e);
            }
            let _ = shared.transport_report().await;
            shared
                .progress
                .send_modify(|p| p.response_task_returned = true);
            Ok(())
        })
        .map_err(|_| Error::Task)?;
        Ok(())
    }
}
async fn emit(
    shared: &Shared,
    ctx: &ProcessorContext,
    data: FrameData,
    limit: std::time::Duration,
) -> Result<(), Error> {
    let mut control = shared.control.subscribe();
    if *control.borrow() != 0 || !ctx.is_current() {
        return Err(Error::Cancelled);
    }
    let bytes = if let FrameData::TtsAudio(a) = &data {
        a.audio.len()
    } else {
        0
    };
    tokio::select! {biased;
        _=changed_control(&mut control)=>Err(Error::Cancelled),
        result=timeout(limit,ctx.push(Frame::new(data),Direction::Downstream))=>{
            result.map_err(|_|Error::Output)?.map_err(|_|Error::Output)?;
            shared.progress.send_modify(|p|p.pcm_bytes_admitted+=bytes as u64);Ok(())
        }
    }
}
async fn audio(
    shared: &Shared,
    ctx: &ProcessorContext,
    id: &str,
    bytes: Vec<u8>,
    config: &SageMakerBidiTtsConfig,
) -> Result<(), Error> {
    let mut pcm = AudioRawFrame::new(bytes, config.settings.sample_rate_hz, 1)
        .map_err(|_| Error::Envelope)?;
    pcm.context_id = Some(id.to_owned());
    emit(
        shared,
        ctx,
        FrameData::TtsAudio(pcm),
        config.limits.output_timeout,
    )
    .await
}
async fn run(
    config: SageMakerBidiTtsConfig,
    shared: Arc<Shared>,
    ctx: ProcessorContext,
    id: String,
    append: bool,
    sender: TransportSender,
    mut receiver: crate::transport::TransportReceiver,
) -> Result<(), Error> {
    let mut control = shared.control.subscribe();
    let mut pcm = protocol::Pcm::new(
        config.settings.sample_rate_hz,
        config.limits.max_output_bytes,
    );
    let work = async {
        emit(
            &shared,
            &ctx,
            FrameData::TtsStarted(TtsStartedFrame {
                context_id: Some(id.clone()),
                append_to_context: append,
            }),
            config.limits.output_timeout,
        )
        .await?;
        loop {
            let raw = receiver.recv().await.ok_or(Error::Incomplete)?;
            if shared.progress.borrow().response_events >= config.limits.max_response_events as u64
            {
                return Err(Error::TooLarge);
            }
            shared.progress.send_modify(|p| p.response_events += 1);
            match protocol::response(&raw, &config.limits)? {
                Response::Audio(bytes) => {
                    if let Some(bytes) = pcm.feed(&bytes)? {
                        audio(&shared, &ctx, &id, bytes, &config).await?;
                    }
                }
                Response::Completed => {
                    shared.progress.send_modify(|p| p.speech_completed = true);
                    if let Some(bytes) = pcm.finish()? {
                        audio(&shared, &ctx, &id, bytes, &config).await?;
                    }
                    emit(
                        &shared,
                        &ctx,
                        FrameData::TtsStopped(TtsStoppedFrame {
                            context_id: Some(id.clone()),
                        }),
                        config.limits.output_timeout,
                    )
                    .await?;
                    return Ok(());
                }
                Response::Ignored => {}
            }
        }
    };
    let result = tokio::select! {biased;code=changed_control(&mut control)=>if code==1{Ok(true)}else{Err(Error::Cancelled)},result=work=>result.map(|()|false)};
    let interrupted = *shared.control.borrow() == 1;
    let result = match result {
        Err(Error::Cancelled) if interrupted => Ok(true),
        other => other,
    };
    if interrupted {
        shared.progress.send_modify(|p| p.interrupted = true);
        // Fence first, then request termination; a response observed here does
        // not establish remote cancellation. All buffered old PCM is discarded.
        let grace = Instant::now() + config.limits.interruption_grace;
        let request = async {
            sender
                .send(protocol::control("input_text.done", &config.limits)?)
                .await?;
            if let Some(raw) = receiver.recv().await {
                shared
                    .progress
                    .send_modify(|p| p.interruption_response_observed = true);
                // Validate wire/provider status; never emit old-session audio.
                let _ = protocol::response(&raw, &config.limits)?;
            }
            Ok::<(), Error>(())
        };
        if let Ok(Err(e)) = tokio::time::timeout_at(grace, request).await {
            sender.cancel();
            return Err(e);
        }
    }
    if result.is_err() {
        sender.cancel();
        return result.map(|_| ());
    }
    sender
        .end(vec![protocol::control("session.end", &config.limits)?])
        .await?;
    // Discard post-terminal audio under existing bounded transport deadlines.
    // Keeping the receiver alive permits a real EOF and actual task joins.
    let close = async {
        while let Some(raw) = receiver.recv().await {
            shared.progress.send_modify(|p| p.response_events += 1);
            if shared.progress.borrow().response_events > config.limits.max_response_events as u64 {
                return Err(Error::TooLarge);
            }
            let _ = protocol::response(&raw, &config.limits)?;
        }
        Ok::<(), Error>(())
    };
    if interrupted {
        match tokio::time::timeout(config.limits.interruption_grace, close).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                sender.cancel();
                return Err(e);
            }
            Err(_) => sender.cancel(),
        }
    } else {
        close.await?;
    }
    Ok(())
}
#[async_trait]
impl FrameProcessor for SageMakerBidiTtsProcessor {
    fn name(&self) -> &str {
        "SageMakerBidiTts"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame
            .require_no_native_llm_continuation()
            .map_err(|_| ProcessorError::fatal("TTS rejects native authority carriers"))?;
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        match &frame.data {
            FrameData::TtsSpeak(s) => {
                self.admit(&s.text, s.append_to_context, ctx)
                    .await
                    .map_err(|e| {
                        self.shared.fail(e.clone());
                        ProcessorError::fatal(e.to_string())
                    })
            }
            FrameData::Interruption => {
                self.shared.interrupt();
                ctx.push(frame, direction).await
            }
            FrameData::Stop | FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                self.shared.cancel();
                ctx.push(frame, direction).await
            }
            FrameData::TtsUpdateSettings(_) | FrameData::ServiceUpdateSettings(_) => {
                Err(ProcessorError::fatal("immutable finite TTS settings"))
            }
            _ => ctx.push(frame, direction).await,
        }
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if !self.progress().response_task_returned {
            self.shared.cancel();
        }
        let _ = self.shared.transport_report().await;
        Ok(())
    }
}
