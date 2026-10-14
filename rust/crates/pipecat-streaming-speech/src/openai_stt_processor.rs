//! Owned, local-VAD-only OpenAI transcription session.
//!
//! User audio and its transcripts survive bot interruptions. This adapter does
//! not attach conversational turn, tool, history or bot-epoch authority. A run
//! identity and captured health generation select each synchronous publication;
//! the subsequent fail-fast queue admission is a separate point. A health
//! setter cannot retract output already selected for publication. Retirement
//! prevents later selections, and joined cleanup leaves no future output.
use crate::{
    openai_stt::{
        OpenAiSttClient, OpenAiSttConfig, OpenAiSttEvent, OpenAiSttSender, OpenAiSttSession,
    },
    processor::{error_frame, processor_error, timestamp},
    settings, CancellationToken, StreamingError,
};
use pipecat_core::{
    async_trait, AudioRawFrame, DetailedTranscriptionFrame, Direction, ErrorCategory, ErrorFrame,
    Frame, FrameData, FrameProcessor, ObservedProcessor, ProcessorContext, ProcessorError,
    ProcessorHealth, ProcessorHealthGeneration, StartFrame, TextFrame,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Mutex as AsyncMutex;

// Explicit true-to-true health restoration has no transition-watch wake.
const HEALTH_POLL: Duration = Duration::from_millis(100);
type Runs = Arc<Mutex<Option<Arc<Run>>>>;

struct Run {
    owner: ObservedProcessor,
    health: ProcessorHealthGeneration,
    cancel: CancellationToken,
    accepting: AtomicBool,
    sender: Mutex<Option<OpenAiSttSender>>,
    // The tracked reader only borrows this slot. Aborting that reader cannot
    // detach the client's socket task or move its join handle out of the owner.
    session: AsyncMutex<Option<OpenAiSttSession>>,
    reader_started: AtomicBool,
    reader_done: CancellationToken,
    observed_terminal: Mutex<Option<StreamingError>>,
    cleanup_error: Mutex<Option<ProcessorError>>,
}
impl Run {
    fn new(ctx: &ProcessorContext) -> Self {
        Self {
            owner: ctx.processor_identity(),
            health: ctx.health().generation(),
            cancel: CancellationToken::new(),
            accepting: AtomicBool::new(true),
            sender: Mutex::new(None),
            session: AsyncMutex::new(None),
            reader_started: AtomicBool::new(false),
            reader_done: CancellationToken::new(),
            observed_terminal: Mutex::new(None),
            cleanup_error: Mutex::new(None),
        }
    }
    fn cancel(&self) {
        self.accepting.store(false, Ordering::Release);
        self.cancel.cancel();
    }
    fn current(&self, ctx: &ProcessorContext) -> bool {
        !self.cancel.is_cancelled()
            && self.owner == ctx.processor_identity()
            && ctx.is_usable()
            && self.health == ctx.health().generation()
    }
    fn sender(&self) -> Result<Option<OpenAiSttSender>, ProcessorError> {
        Ok(self.sender.lock().map_err(|_| state_error())?.clone())
    }
    async fn shutdown(&self) -> Result<(), ProcessorError> {
        self.cancel();
        let mut slot = self.session.lock().await;
        let result = if let Some(session) = slot.as_mut() {
            // Keep ownership while awaiting. Cancellation of this cleanup leaves
            // the slot and its join handle available for the next cleanup.
            match session.shutdown().await {
                Ok(()) | Err(StreamingError::Cancelled) => Ok(()),
                Err(error) => {
                    // next() joined this same actor and exposed its stored
                    // terminal result, including close, before returning Err.
                    // Never report it again, even if retirement suppressed it.
                    if self
                        .observed_terminal
                        .lock()
                        .map_err(|_| state_error())?
                        .as_ref()
                        == Some(&error)
                    {
                        Ok(())
                    } else {
                        Err(nonpermanent(error))
                    }
                }
            }
        } else {
            Ok(())
        };
        // An actor failure is still a completed join. Only a cancelled await
        // keeps ownership in the slot for a later cleanup to resume.
        *slot = None;
        *self.sender.lock().map_err(|_| state_error())? = None;
        result
    }
    async fn join(&self) -> Result<(), ProcessorError> {
        self.cancel();
        let result = self.shutdown().await;
        if self.reader_started.load(Ordering::Acquire) {
            self.reader_done.cancelled().await;
        }
        let deferred = self.cleanup_error.lock().map_err(|_| state_error())?.take();
        result.and(deferred.map_or(Ok(()), Err))
    }
}

// Constructed before spawning, so spawn rejection and an unpolled task abort
// signal cancellation even when never polled. The published owner then joins.
struct OwnedReaderGuard {
    run: Arc<Run>,
}
impl Drop for OwnedReaderGuard {
    fn drop(&mut self) {
        self.run.cancel();
        self.run.reader_done.cancel();
    }
}
struct SetupGuard {
    run: Arc<Run>,
    installed: bool,
}
impl Drop for SetupGuard {
    fn drop(&mut self) {
        if !self.installed {
            self.run.cancel();
        }
    }
}

/// A fixed 24 kHz mono PCM16LE transcription adapter, with no reconnect/replay.
/// Resample upstream when the pipeline's source rate differs. Settings are
/// immutable for this slice; construct a fresh processor to change them.
///
/// `SttMute` consumes the control and suppresses new downstream PCM only. It
/// neither closes the socket nor suppresses outstanding results or local-VAD
/// commits, matching the source STT base class and OpenAI subclass ordering.
pub struct OpenAiRealtimeSttProcessor {
    client: OpenAiSttClient,
    runs: Runs,
    setup_gate: AsyncMutex<()>,
    health: ProcessorHealth,
    muted: AtomicBool,
    user_id: String,
    audio_passthrough: bool,
}
impl OpenAiRealtimeSttProcessor {
    pub fn new(client: OpenAiSttClient) -> Self {
        Self {
            client,
            runs: Arc::new(Mutex::new(None)),
            setup_gate: AsyncMutex::new(()),
            health: ProcessorHealth::default(),
            muted: AtomicBool::new(false),
            user_id: String::new(),
            audio_passthrough: true,
        }
    }
    /// Fix the result speaker, including an explicitly empty ID. Input frame
    /// speaker IDs are preserved by passthrough, never adopted by this session.
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Result<Self, StreamingError> {
        let user_id = user_id.into();
        if user_id.len() > 1024 {
            return Err(StreamingError::TooLarge);
        }
        self.user_id = user_id;
        Ok(self)
    }
    pub fn with_audio_passthrough(mut self, value: bool) -> Self {
        self.audio_passthrough = value;
        self
    }
    pub fn config_snapshot(&self) -> OpenAiSttConfig {
        self.client.config().clone()
    }
    fn cancel_current(&self) -> Result<(), ProcessorError> {
        if let Some(run) = self.runs.lock().map_err(|_| state_error())?.as_ref() {
            run.cancel();
        }
        Ok(())
    }
    fn submit_audio(
        &self,
        frame: &Frame,
        audio: &AudioRawFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        // Reject sealed native effects before retaining/copying PCM or speaker
        // data, including a public payload changed after native attachment.
        frame.require_no_native_llm_continuation().map_err(|_| {
            nonpermanent(StreamingError::Invalid(
                "OpenAI STT cannot consume native authority",
            ))
        })?;
        if self.muted.load(Ordering::Acquire) || audio.audio.is_empty() || !ctx.is_usable() {
            return Ok(());
        }
        audio.validate().map_err(|_| {
            nonpermanent(StreamingError::Invalid(
                "OpenAI STT requires complete PCM16 samples",
            ))
        })?;
        if audio.sample_rate != 24_000 || audio.num_channels != 1 {
            return Err(nonpermanent(StreamingError::Invalid(
                "OpenAI STT requires upstream-resampled 24000 Hz mono PCM16LE",
            )));
        }
        if audio.audio.len() > self.client.config().max_audio_bytes
            || audio.audio.len() > self.client.config().limits.max_message_bytes
        {
            return Err(nonpermanent(StreamingError::TooLarge));
        }
        let runs = self.runs.lock().map_err(|_| state_error())?;
        let run = runs
            .as_ref()
            .ok_or_else(|| nonpermanent(StreamingError::Closed))?;
        if !run.current(ctx) || !run.accepting.load(Ordering::Acquire) {
            run.cancel();
            return Ok(());
        }
        // Current input invocation admission does not bind the user segment to
        // the Start frame's old bot epoch. Accepted PCM survives interruption.
        let fence = ctx.invocation_fence()?;
        fence
            .with_current(|| {
                run.sender()?
                    .ok_or_else(|| nonpermanent(StreamingError::Closed))?
                    .try_send_audio(audio.clone())
                    .map_err(nonpermanent)
            })
            .unwrap_or(Ok(()))
    }
    fn commit(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let runs = self.runs.lock().map_err(|_| state_error())?;
        let Some(run) = runs.as_ref() else {
            return Ok(());
        };
        if !run.current(ctx) || !run.accepting.load(Ordering::Acquire) {
            run.cancel();
            return Ok(());
        }
        ctx.invocation_fence()?
            .with_current(|| {
                run.sender()?
                    .ok_or_else(|| nonpermanent(StreamingError::Closed))?
                    .try_commit()
                    .map_err(nonpermanent)
            })
            .unwrap_or(Ok(()))
    }
}

#[async_trait]
impl FrameProcessor for OpenAiRealtimeSttProcessor {
    fn name(&self) -> &str {
        "OpenAIRealtimeSTTService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        _start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let _setup = self.setup_gate.lock().await;
        // Start describes the pipeline source rate. An upstream resampler may
        // legitimately convert it; validate the actual PCM at this boundary.
        if !ctx.is_usable() || !ctx.invocation_fence()?.is_current() {
            return Ok(());
        }
        let old = {
            let runs = self.runs.lock().map_err(|_| state_error())?;
            if runs.as_ref().is_some_and(|run| !run.cancel.is_cancelled()) {
                return Err(nonpermanent(StreamingError::Invalid(
                    "OpenAI STT run is already active",
                )));
            }
            runs.clone()
        };
        if let Some(old) = old {
            old.join().await?;
        }
        let run = Arc::new(Run::new(ctx));
        let mut setup_guard = SetupGuard {
            run: run.clone(),
            installed: false,
        };
        *self.runs.lock().map_err(|_| state_error())? = Some(run.clone());
        let mut health = ctx.health().watch();
        let connected = {
            let mut slot = run.session.lock().await;
            let result = tokio::select! { biased;
                _ = run.cancel.cancelled() => Err(nonpermanent(StreamingError::Cancelled)),
                _ = health.changed() => Err(nonpermanent(StreamingError::Cancelled)),
                _ = wait_for_retirement(&run, ctx) => Err(nonpermanent(StreamingError::Cancelled)),
                result = self.client.connect_with_cancel(run.cancel.clone()) => result.map_err(nonpermanent),
            };
            match result {
                Ok(session) => {
                    // No await between connect returning and owned installation.
                    *slot = Some(session);
                    *run.sender.lock().map_err(|_| state_error())? =
                        slot.as_ref().map(OpenAiSttSession::sender);
                    Ok(())
                }
                Err(error) => Err(error),
            }
        };
        if let Err(error) = connected {
            run.join().await?;
            return Err(error);
        }
        if !run.current(ctx) {
            run.join().await?;
            return Err(nonpermanent(StreamingError::Cancelled));
        }
        let output = ctx.clone();
        let runs = self.runs.clone();
        let user_id = self.user_id.clone();
        let reader_run = run.clone();
        let guard = OwnedReaderGuard { run: run.clone() };
        run.reader_started.store(true, Ordering::Release);
        let spawned = ctx.spawn("openai-stt-transcripts", false, async move {
            let _guard = guard;
            if let Err(error) = read_transcripts(&reader_run, &runs, &output, &user_id).await {
                // Report now under the run fence. Returning a delayed worker
                // error would let runtime drain report it after retirement.
                let selected = runs.lock().is_ok_and(|current| {
                    current
                        .as_ref()
                        .is_some_and(|active| Arc::ptr_eq(active, &reader_run))
                        && reader_run.current(&output)
                });
                if selected {
                    let _ = output.try_push_error_if_health_generation(
                        ErrorFrame {
                            error: error.message,
                            category: error.category,
                            fatal: error.fatal,
                            ..ErrorFrame::new("")
                        },
                        false,
                        reader_run.health,
                    );
                }
            }
            if let Err(error) = reader_run.shutdown().await {
                // Unobserved shutdown failures belong to the explicit cleanup
                // result, not a late background error emitted by core drain.
                *reader_run.cleanup_error.lock().map_err(|_| state_error())? = Some(error);
            }
            Ok(())
        });
        if let Err(error) = spawned {
            run.join().await?;
            return Err(error);
        }
        setup_guard.installed = true;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => self.cancel_current()?,
            FrameData::SttMute(value) => {
                self.muted.store(*value, Ordering::Release);
                return Ok(());
            }
            FrameData::SttUpdateSettings(update) if settings::targets(self, &update.service) => {
                return Err(nonpermanent(StreamingError::Invalid(
                    "OpenAI STT settings are immutable; construct a fresh processor",
                )));
            }
            FrameData::ServiceUpdateSettings(update)
                if settings::targets(self, &update.service) =>
            {
                return Err(nonpermanent(StreamingError::Invalid(
                    "OpenAI STT settings are immutable; construct a fresh processor",
                )));
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_) => {
                frame.require_no_native_llm_continuation().map_err(|_| {
                    nonpermanent(StreamingError::Invalid(
                        "OpenAI STT cannot consume native authority",
                    ))
                })?;
                // Source dispatch forwards through its base STT service first,
                // then the local-VAD subclass commits, in either direction.
                ctx.push(frame, direction).await?;
                return self.commit(ctx);
            }
            FrameData::InputAudio(audio) if direction == Direction::Downstream => {
                self.submit_audio(&frame, audio, ctx)?;
                if !self.audio_passthrough {
                    return Ok(());
                }
            }
            FrameData::UserAudio(user) if direction == Direction::Downstream => {
                self.submit_audio(&frame, &user.audio, ctx)?;
                if !self.audio_passthrough {
                    return Ok(());
                }
            }
            _ => {}
        }
        // In particular, upstream audio, UserStoppedSpeaking, Interruption,
        // conversation and tool frames retain their original envelope/direction.
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let run = {
            let runs = self.runs.lock().map_err(|_| state_error())?;
            let Some(run) = runs.as_ref() else {
                return Ok(());
            };
            run.accepting.store(false, Ordering::Release);
            if !run.current(ctx) {
                run.cancel();
                return Ok(());
            }
            run.clone()
        };
        let Some(sender) = run.sender()? else {
            run.cancel();
            return Ok(());
        };
        // Finish is only a bounded drain of explicitly committed items. It must
        // never turn trailing uncommitted user audio into a new segment.
        let result = tokio::select! { biased;
            _ = run.cancel.cancelled() => return Ok(()),
            result = tokio::time::timeout(self.client.config().limits.write_timeout, sender.finish()) => {
                match result {
                    Ok(result) => result.map_err(nonpermanent),
                    Err(_) => Err(nonpermanent(StreamingError::Timeout("OpenAI STT finish admission"))),
                }
            },
        };
        if let Err(error) = result {
            run.join().await?;
            return Err(error);
        }
        // The client bounds readiness, committed-item drain and close. Keep an
        // outer bound as well, so End/Stop never waits indefinitely on a reader.
        let limits = &self.client.config().limits;
        let drain_timeout = self
            .client
            .config()
            .ready_timeout
            .saturating_add(limits.response_timeout)
            .saturating_add(limits.close_timeout)
            .saturating_add(limits.write_timeout);
        let drained = tokio::time::timeout(drain_timeout, run.reader_done.cancelled()).await;
        run.join().await?;
        if drained.is_err() {
            return Err(nonpermanent(StreamingError::Timeout("OpenAI STT drain")));
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        // Wake a setup handshake before waiting for its gate.
        self.cancel_current()?;
        let _setup = self.setup_gate.lock().await;
        let run = self.runs.lock().map_err(|_| state_error())?.clone();
        if let Some(run) = run {
            let result = run.join().await;
            // Keep the published owner until joining has actually completed.
            let mut runs = self.runs.lock().map_err(|_| state_error())?;
            if runs
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, &run))
            {
                *runs = None;
            }
            result?;
        }
        self.muted.store(false, Ordering::Release);
        Ok(())
    }
}
impl Drop for OpenAiRealtimeSttProcessor {
    fn drop(&mut self) {
        if let Ok(mut runs) = self.runs.lock() {
            if let Some(run) = runs.take() {
                // Standalone Drop can only request cancellation. PipelineWorker
                // Drop retains the processor until core cleanup joins this Run.
                run.cancel();
            }
        }
    }
}
async fn wait_for_retirement(run: &Run, output: &ProcessorContext) {
    loop {
        tokio::time::sleep(HEALTH_POLL).await;
        if !run.current(output) {
            return;
        }
    }
}
async fn read_transcripts(
    run: &Arc<Run>,
    runs: &Runs,
    output: &ProcessorContext,
    user_id: &str,
) -> Result<(), ProcessorError> {
    let mut health = output.health().watch();
    loop {
        if !run.current(output) {
            return Ok(());
        }
        let event = {
            let mut slot = run.session.lock().await;
            let Some(session) = slot.as_mut() else {
                return Ok(());
            };
            let event = tokio::select! { biased;
                _ = run.cancel.cancelled() => return Ok(()),
                _ = health.changed() => return Ok(()),
                _ = wait_for_retirement(run, output) => return Ok(()),
                event = session.next() => event,
            };
            if let Some(Err(error)) = &event {
                // Mark before releasing session ownership or checking health:
                // stale terminal outcomes must not reappear during cleanup.
                *run.observed_terminal.lock().map_err(|_| state_error())? = Some(error.clone());
            }
            event
        };
        // Select under the owner lock, then release it before core publication:
        // synchronous queue wakers may reenter processor controls. Publication
        // already selected here may race subsequent Cancel or health retirement.
        let selected = {
            let current = runs.lock().map_err(|_| state_error())?;
            current
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, run))
                && run.current(output)
        };
        if !selected {
            return Ok(());
        }
        match event {
            Some(Ok(OpenAiSttEvent::Delta { delta, result, .. })) => {
                if !delta.is_empty() {
                    output.try_push(
                        transcript(delta, result, user_id, false),
                        Direction::Downstream,
                    )?;
                }
            }
            Some(Ok(OpenAiSttEvent::Completed {
                transcript: text,
                result,
                ..
            })) => {
                if !text.is_empty() {
                    output.try_push(
                        transcript(text, result, user_id, true),
                        Direction::Downstream,
                    )?;
                }
            }
            Some(Ok(OpenAiSttEvent::Failed { .. })) => {
                // Provider text may contain secrets/control-looking content;
                // never interpolate it into an error or a pipeline instruction.
                output.try_push_error_if_health_generation(
                    ErrorFrame {
                        error: "OpenAI Realtime STT transcription failed".into(),
                        category: ErrorCategory::Application,
                        ..ErrorFrame::new("")
                    },
                    false,
                    run.health,
                )?;
            }
            Some(Err(StreamingError::Cancelled)) | None => return Ok(()),
            Some(Err(error)) => {
                output.try_push_error_if_health_generation(
                    error_frame(error),
                    false,
                    run.health,
                )?;
                return Ok(());
            }
        }
    }
}
fn transcript(text: String, result: serde_json::Value, user_id: &str, completed: bool) -> Frame {
    let value = DetailedTranscriptionFrame {
        text: TextFrame::new(text),
        user_id: user_id.into(),
        timestamp: timestamp(),
        language: None,
        result: Some(result),
        // Provider completion is not native finalization/turn ownership.
        finalized: false,
    };
    Frame::new(if completed {
        FrameData::TranscriptionDetailed(value)
    } else {
        FrameData::InterimTranscriptionDetailed(value)
    })
}
fn nonpermanent(error: StreamingError) -> ProcessorError {
    processor_error(error).with_permanence_override(false)
}
fn state_error() -> ProcessorError {
    ProcessorError::fatal("OpenAI STT owner lock poisoned")
}

/// Short alias for callers which do not need to distinguish REST STT services.
pub type OpenAiSttProcessor = OpenAiRealtimeSttProcessor;
