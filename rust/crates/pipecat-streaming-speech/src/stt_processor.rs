//! VAD-aware reconnect supervision for native Deepgram sessions.
use crate::{
    common::Input,
    processor::{error_frame, processor_error, timestamp},
    settings, CancellationToken, DeepgramClient, DeepgramConfig, DeepgramEvent, DeepgramSession,
    StreamingError,
};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, ErrorCategory, Frame, FrameData, FrameProcessor, Metric,
    ProcessorContext, ProcessorError, ProcessorHealth, ServiceUpdateSettingsFrame, StartFrame,
    SttSettings, TranscriptionFrame,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::mpsc;

/// Bounds rapid connection failures. A stable connection resets the budget.
/// Audio already submitted to a socket is never replayed after an outage.
#[derive(Clone, Debug)]
pub struct ReconnectPolicy {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub stable_after: Duration,
}
impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
            stable_after: Duration::from_secs(10),
        }
    }
}
enum Command {
    Audio(AudioRawFrame),
    Finalize,
    Update(Box<DeepgramClient>),
    Speaking(bool),
    Finish,
}
struct State {
    client: DeepgramClient,
    input: Option<Input<Command>>,
    health_revision: u64,
}
pub struct DeepgramSttProcessor {
    state: Mutex<State>,
    health: ProcessorHealth,
    user_id: String,
    mute: AtomicBool,
    audio_passthrough: bool,
    reconnect: ReconnectPolicy,
}
impl DeepgramSttProcessor {
    pub fn new(client: DeepgramClient) -> Self {
        Self {
            state: Mutex::new(State {
                client,
                input: None,
                health_revision: 0,
            }),
            health: ProcessorHealth::default(),
            user_id: String::new(),
            mute: AtomicBool::new(false),
            audio_passthrough: true,
            reconnect: ReconnectPolicy::default(),
        }
    }
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }
    pub fn with_audio_passthrough(mut self, enabled: bool) -> Self {
        self.audio_passthrough = enabled;
        self
    }
    /// Set max_attempts to zero to disable recovery after an established socket fails.
    pub fn with_reconnect_policy(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }
    pub fn config_snapshot(&self) -> Result<DeepgramConfig, StreamingError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| StreamingError::Closed)?
            .client
            .config()
            .clone())
    }
    /// Validate and admit a delta atomically. During speech it applies to the
    /// next socket after UserStoppedSpeaking; VAD stop only finalizes the old one.
    pub fn update_settings(&self, delta: &SttSettings) -> Result<(), StreamingError> {
        let mut state = self.state.lock().map_err(|_| StreamingError::Closed)?;
        let candidate = settings::deepgram(&state.client, delta)?;
        if equivalent_config(candidate.config(), state.client.config()) {
            return Ok(());
        }
        let snapshot = self.health.snapshot();
        if !snapshot.is_usable
            || state.health_revision != snapshot.revision
            || state
                .input
                .as_ref()
                .is_some_and(|input| input.tx.is_closed())
        {
            if let Some(input) = state.input.take() {
                input.cancel.cancel();
            }
        }
        if let Some(input) = &state.input {
            input.try_send(Command::Update(Box::new(candidate.clone())), false)?;
        }
        state.client = candidate;
        self.health.set_usable(true);
        state.health_revision = self.health.snapshot().revision;
        Ok(())
    }
    fn sync_health(&self) -> Result<(), ProcessorError> {
        let snapshot = self.health.snapshot();
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("STT state lock poisoned"))?;
        if !snapshot.is_usable || state.health_revision != snapshot.revision {
            if let Some(input) = state.input.take() {
                input.cancel.cancel();
            }
            state.health_revision = snapshot.revision;
        }
        Ok(())
    }
    // A new input generation has a fresh queue and socket. Discarded or already
    // admitted audio from the retired generation is never replayed here.
    fn start_session(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.install_session(ctx, None)
    }
    fn install_session(
        &self,
        ctx: &ProcessorContext,
        ready: Option<DeepgramSession>,
    ) -> Result<(), ProcessorError> {
        self.sync_health()?;
        if !ctx.is_usable() {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("STT state lock poisoned"))?;
        if state
            .input
            .as_ref()
            .is_some_and(|input| !input.tx.is_closed() && !input.cancel.is_cancelled())
        {
            return Ok(());
        }
        let client = state.client.clone();
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(client.config().limits.queue_capacity);
        state.input = Some(Input {
            tx,
            cancel: cancel.clone(),
            limits: client.config().limits.clone(),
            finished: Arc::new(Mutex::new(false)),
        });
        let output = ctx.clone();
        let user_id = self.user_id.clone();
        let revision = self.health.snapshot().revision;
        let mut health = self.health.watch();
        let policy = self.reconnect.clone();
        ctx.spawn("deepgram-transcripts", false, async move {
            let operation = async {
                if !same_generation(&output, revision) { return Ok(()); }
                let session = match ready {
                    Some(session) => session,
                    None => tokio::select! {
                        biased;
                        _ = health.changed() => return Ok(()),
                        result = client.connect() => match result {
                            Ok(session) => session,
                            Err(error) => { output.push_error(error_frame(error)).await?; return Ok(()); }
                        }
                    }
                };
                supervise(client, session, rx, &output, &user_id, policy, revision).await
            };
            tokio::select! { biased; _ = cancel.cancelled() => Ok(()), result = operation => result }
        })?;
        Ok(())
    }
    fn input(&self) -> Result<Input<Command>, ProcessorError> {
        self.state
            .lock()
            .map_err(|_| ProcessorError::fatal("STT state lock poisoned"))?
            .input
            .clone()
            .ok_or_else(|| ProcessorError::new("Deepgram is not connected"))
    }
    fn update_frame(
        &self,
        update: &ServiceUpdateSettingsFrame<SttSettings>,
    ) -> Result<bool, ProcessorError> {
        if !settings::targets(self, &update.service) {
            return Ok(false);
        }
        let delta = match &update.delta {
            Some(delta) => delta.clone(),
            None => settings::stt_mapping(&update.settings).map_err(settings_error)?,
        };
        self.update_settings(&delta).map_err(settings_error)?;
        Ok(true)
    }
}
#[async_trait]
impl FrameProcessor for DeepgramSttProcessor {
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    fn name(&self) -> &str {
        "DeepgramSttService"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let client = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("STT state lock poisoned"))?
            .client
            .clone();
        if config.audio_in_sample_rate != client.config().sample_rate {
            return Err(ProcessorError::new(
                "pipeline input rate differs from Deepgram configuration",
            )
            .with_category(ErrorCategory::InvalidRequest));
        }
        if !ctx.is_usable() {
            return Ok(());
        }
        let session = client.connect().await.map_err(processor_error)?;
        self.install_session(ctx, Some(session))
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.sync_health()?;
        match &frame.data {
            FrameData::SttUpdateSettings(update) if self.update_frame(update)? => return Ok(()),
            FrameData::ServiceUpdateSettings(update)
                if settings::targets(self, &update.service) =>
            {
                let delta = match &update.delta {
                    Some(delta) => SttSettings {
                        base: delta.clone(),
                        ..Default::default()
                    },
                    None => settings::stt_mapping(&update.settings).map_err(settings_error)?,
                };
                self.update_settings(&delta).map_err(settings_error)?;
                return Ok(());
            }
            FrameData::VadUserStartedSpeaking(_) | FrameData::VadUserStartedSpeakingDetailed(_)
                if ctx.is_usable() =>
            {
                self.start_session(ctx)?;
                self.input()?
                    .try_send(Command::Speaking(true), false)
                    .map_err(processor_error)?
            }
            FrameData::UserStoppedSpeaking if ctx.is_usable() => {
                if let Ok(input) = self.input() {
                    if !input.tx.is_closed() && !input.cancel.is_cancelled() {
                        if let Err(error) = input.try_send(Command::Speaking(false), false) {
                            ctx.push_error(error_frame(error)).await?;
                        }
                    }
                }
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_)
                if ctx.is_usable() =>
            {
                self.start_session(ctx)?;
                self.input()?
                    .try_send(Command::Finalize, false)
                    .map_err(processor_error)?
            }
            FrameData::SttMute(mute) => self.mute.store(*mute, Ordering::Release),
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                if let Ok(input) = self.input() {
                    input.cancel.cancel();
                }
            }
            _ => {}
        }
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        if let FrameData::InputAudio(audio) = &frame.data {
            if ctx.is_usable() && !self.mute.load(Ordering::Acquire) && !audio.audio.is_empty() {
                let config = self.config_snapshot().map_err(processor_error)?;
                audio.validate().map_err(|_| {
                    processor_error(StreamingError::Invalid(
                        "PCM must contain complete signed 16-bit samples",
                    ))
                })?;
                if audio.sample_rate != config.sample_rate || audio.num_channels != config.channels
                {
                    return Err(processor_error(StreamingError::Invalid(
                        "PCM format differs from Deepgram configuration",
                    )));
                }
                if audio.audio.len() > config.limits.max_message_bytes {
                    return Err(processor_error(StreamingError::TooLarge));
                }
                self.start_session(ctx)?;
                self.input()?
                    .try_send(Command::Audio(audio.clone()), false)
                    .map_err(processor_error)?;
                if ctx.configuration().enable_usage_metrics {
                    ctx.push(
                        Frame::new(FrameData::Metrics(vec![Metric::SttUsage {
                            processor: self.name().into(),
                            audio_seconds: audio.duration_seconds(),
                        }])),
                        direction,
                    )
                    .await?;
                }
            }
            if !self.audio_passthrough {
                return Ok(());
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.sync_health()?;
        if let Ok(input) = self.input() {
            if input.tx.is_closed() || input.cancel.is_cancelled() {
                return Ok(());
            }
            input
                .send(Command::Finish, true)
                .await
                .map_err(processor_error)?;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let Some(input) = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("STT state lock poisoned"))?
            .input
            .take()
        {
            input.cancel.cancel();
        }
        Ok(())
    }
}

async fn supervise(
    mut client: DeepgramClient,
    mut session: DeepgramSession,
    mut commands: mpsc::Receiver<Command>,
    ctx: &ProcessorContext,
    user_id: &str,
    policy: ReconnectPolicy,
    revision: u64,
) -> Result<(), ProcessorError> {
    let mut speaking = false;
    let mut pending = false;
    let mut closing = false;
    let mut failures = 0;
    let mut connected_at = tokio::time::Instant::now();
    let mut health = ctx.health().watch();
    loop {
        if !same_generation(ctx, revision) {
            return Ok(());
        }
        tokio::select! {
            biased;
            _ = health.changed() => return Ok(()),
            event = session.next() => match event {
                Some(Ok(event)) => emit(event, ctx, user_id, revision).await?,
                Some(Err(StreamingError::Cancelled)) => return Ok(()),
                event => {
                    let error = match event { Some(Err(error)) => error, None if closing => return Ok(()), _ => StreamingError::Closed };
                    if !same_generation(ctx, revision) { return Ok(()); }
                    ctx.push_error(error_frame(error.clone())).await?;
                    if closing || !retryable(&error) { return rejected_buffer(&mut commands, ctx).await; }
                    if connected_at.elapsed() >= policy.stable_after { failures = 0; }
                    match recover(&client, &policy, &mut failures, ctx, revision).await? {
                        Some(replacement) => { session = replacement; connected_at = tokio::time::Instant::now(); pending = false; },
                        None => return rejected_buffer(&mut commands, ctx).await,
                    }
                },
            },
            command = commands.recv(), if !closing => {
                let rotate = match command {
                    Some(command @ (Command::Audio(_) | Command::Finalize)) => {
                        loop {
                            match submit(&mut session, &command, ctx, user_id, revision).await? {
                                Ok(()) => break,
                                Err(error) => {
                                    if !same_generation(ctx, revision) { return Ok(()); }
                    ctx.push_error(error_frame(error.clone())).await?;
                                    if connected_at.elapsed() >= policy.stable_after { failures = 0; }
                                    let replacement = if retryable(&error) { recover(&client, &policy, &mut failures, ctx, revision).await? } else { None };
                                    match replacement {
                                        Some(replacement) => { session = replacement; connected_at = tokio::time::Instant::now(); pending = false; },
                                        None => {
                                            if let Command::Audio(audio) = &command { report_unsent(audio.audio.len(), ctx).await?; }
                                            return rejected_buffer(&mut commands, ctx).await;
                                        },
                                    }
                                },
                            }
                        }
                        false
                    },
                    Some(Command::Update(updated)) => { client = *updated; pending = true; !speaking },
                    Some(Command::Speaking(value)) => { speaking = value; !speaking && pending },
                    Some(Command::Finish) | None => {
                        if let Err(error) = submit(&mut session, &Command::Finish, ctx, user_id, revision).await? {
                            if !same_generation(ctx, revision) { return Ok(()); } ctx.push_error(error_frame(error)).await?;
                            return rejected_buffer(&mut commands, ctx).await;
                        }
                        closing = true;
                        false
                    },
                };
                if rotate {
                    // Preserve final transcripts from the old settings before opening
                    // the next socket. Only commands still in this bounded queue wait.
                    if let Err(error) = submit(&mut session, &Command::Finish, ctx, user_id, revision).await? { if !same_generation(ctx, revision) { return Ok(()); } ctx.push_error(error_frame(error)).await?; }
                    loop {
                        if !same_generation(ctx, revision) { return Ok(()); }
                        let event = tokio::select! { biased; _ = health.changed() => return Ok(()), event = session.next() => event };
                        let Some(event) = event else { break; };
                        match event { Ok(event) => emit(event, ctx, user_id, revision).await?, Err(error) => { if !same_generation(ctx, revision) { return Ok(()); } ctx.push_error(error_frame(error)).await?; break; } }
                    }
                    if !same_generation(ctx, revision) { return Ok(()); }
                    let replacement = tokio::select! { biased; _ = health.changed() => return Ok(()), result = client.connect() => result };
                    match replacement {
                        Ok(replacement) => { session = replacement; connected_at = tokio::time::Instant::now(); pending = false; },
                        Err(error) => {
                            if !same_generation(ctx, revision) { return Ok(()); }
                    ctx.push_error(error_frame(error.clone())).await?;
                            if !retryable(&error) { return rejected_buffer(&mut commands, ctx).await; }
                            match recover(&client, &policy, &mut failures, ctx, revision).await? {
                                Some(replacement) => { session = replacement; connected_at = tokio::time::Instant::now(); pending = false; },
                                None => return rejected_buffer(&mut commands, ctx).await,
                            }
                        },
                    }
                }
            },
        }
    }
}
/// The provider queue admission races its output drain. If this returns an
/// error, the command was never admitted and may be retried safely. Once the
/// send future returns success, delivery is ambiguous after a socket outage.
async fn submit(
    session: &mut DeepgramSession,
    command: &Command,
    ctx: &ProcessorContext,
    user_id: &str,
    revision: u64,
) -> Result<Result<(), StreamingError>, ProcessorError> {
    let mut health = ctx.health().watch();
    if !same_generation(ctx, revision) {
        return Ok(Err(StreamingError::Cancelled));
    }
    let sender = session.sender();
    let send = async {
        match command {
            Command::Audio(audio) => sender.send_audio(audio.clone()).await,
            Command::Finalize => sender.finalize().await,
            Command::Finish => sender.finish().await,
            _ => unreachable!(),
        }
    };
    tokio::pin!(send);
    loop {
        tokio::select! {
            biased;
            _ = health.changed() => return Ok(Err(StreamingError::Cancelled)),
            result = &mut send => return Ok(result),
            event = session.next() => match event {
                Some(Ok(event)) => emit(event, ctx, user_id, revision).await?,
                Some(Err(error)) => return Ok(Err(error)),
                None => return Ok(Err(StreamingError::Closed)),
            },
        }
    }
}
fn retryable(error: &StreamingError) -> bool {
    matches!(
        error,
        StreamingError::Transport | StreamingError::Closed | StreamingError::Timeout(_)
    ) || matches!(error, StreamingError::Http(status) | StreamingError::Provider { status: Some(status) } if *status >= 500)
}
async fn recover(
    client: &DeepgramClient,
    policy: &ReconnectPolicy,
    failures: &mut u32,
    ctx: &ProcessorContext,
    revision: u64,
) -> Result<Option<DeepgramSession>, ProcessorError> {
    let mut health = ctx.health().watch();
    while *failures < policy.max_attempts {
        if !same_generation(ctx, revision) {
            return Ok(None);
        }
        let factor = 1u32.checked_shl((*failures).min(31)).unwrap_or(u32::MAX);
        tokio::select! {
            biased;
            _ = health.changed() => return Ok(None),
            _ = tokio::time::sleep(policy.initial_delay.saturating_mul(factor).min(policy.max_delay)) => {}
        }
        if !same_generation(ctx, revision) {
            return Ok(None);
        }
        *failures += 1;
        let replacement = tokio::select! { biased; _ = health.changed() => return Ok(None), result = client.connect() => result };
        if !same_generation(ctx, revision) {
            return Ok(None);
        }
        match replacement {
            Ok(session) => return Ok(Some(session)),
            Err(error) => {
                let retry = retryable(&error);
                ctx.push_error(error_frame(error)).await?;
                if !retry {
                    break;
                }
            }
        }
    }
    if policy.max_attempts > 0 && *failures >= policy.max_attempts && same_generation(ctx, revision)
    {
        ctx.push_error_with_permanence(
            pipecat_core::ErrorFrame {
                error: format!(
                    "Deepgram recovery exhausted after {} attempts",
                    policy.max_attempts
                ),
                category: ErrorCategory::Connectivity,
                ..pipecat_core::ErrorFrame::new("")
            },
            true,
        )
        .await?;
    }
    Ok(None)
}
async fn rejected_buffer(
    commands: &mut mpsc::Receiver<Command>,
    ctx: &ProcessorContext,
) -> Result<(), ProcessorError> {
    commands.close();
    let mut bytes = 0usize;
    while let Some(command) = commands.recv().await {
        if let Command::Audio(audio) = command {
            bytes = bytes.saturating_add(audio.audio.len());
        }
    }
    report_unsent(bytes, ctx).await
}
async fn report_unsent(bytes: usize, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
    if bytes > 0 {
        ctx.push_error(pipecat_core::ErrorFrame {
            error: format!(
                "Deepgram recovery exhausted; {bytes} buffered PCM bytes were not submitted"
            ),
            category: ErrorCategory::Connectivity,
            ..pipecat_core::ErrorFrame::new("")
        })
        .await?;
    }
    Ok(())
}
async fn emit(
    event: DeepgramEvent,
    output: &ProcessorContext,
    user_id: &str,
    revision: u64,
) -> Result<(), ProcessorError> {
    if !same_generation(output, revision) {
        return Ok(());
    }
    let frame = match event {
        DeepgramEvent::Transcript(transcript) => {
            let value = TranscriptionFrame {
                text: transcript.text,
                user_id: user_id.into(),
                timestamp: timestamp(),
                language: transcript.language,
                result: Some(transcript.result),
                finalized: transcript.finalized,
            };
            let mut frame = Frame::new(if transcript.is_final {
                FrameData::Transcription(value)
            } else {
                FrameData::InterimTranscription(value)
            });
            frame
                .metadata
                .insert("speech_final".into(), transcript.speech_final.into());
            frame
        }
        DeepgramEvent::SpeechStarted { timestamp } => {
            let mut frame = Frame::new(FrameData::UserStartedSpeaking);
            frame
                .metadata
                .insert("provider_timestamp".into(), timestamp.into());
            frame
        }
        DeepgramEvent::UtteranceEnd { last_word_end } => {
            let mut frame = Frame::new(FrameData::UserStoppedSpeaking);
            frame
                .metadata
                .insert("last_word_end".into(), last_word_end.into());
            frame
        }
        DeepgramEvent::Finalized | DeepgramEvent::Metadata(_) => return Ok(()),
    };
    output.push(frame, Direction::Downstream).await
}

fn same_generation(ctx: &ProcessorContext, revision: u64) -> bool {
    let snapshot = ctx.health().snapshot();
    snapshot.is_usable && snapshot.revision == revision
}

fn settings_error(error: StreamingError) -> ProcessorError {
    processor_error(error).with_permanence_override(false)
}

// Query-key ordering does not change settings, while repeated values for one
// key retain their order (for example multiple replacement rules).
fn equivalent_config(left: &DeepgramConfig, right: &DeepgramConfig) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.extra_query.sort_by(|a, b| a.0.cmp(&b.0));
    right.extra_query.sort_by(|a, b| a.0.cmp(&b.0));
    left == right
}
