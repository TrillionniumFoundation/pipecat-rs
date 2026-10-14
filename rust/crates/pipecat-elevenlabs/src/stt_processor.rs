//! Nonblocking STT pipeline adapter. Settings changes drain the old session.
use crate::{
    common::Input,
    settings,
    tts_processor::{error_frame, processor_error, timestamp},
    CancellationToken, CommitStrategy, ElevenLabsSttClient, ElevenLabsSttConfig,
    ElevenLabsSttEvent, ElevenLabsSttSession, StreamingError,
};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, Metric,
    ProcessorContext, ProcessorError, ProcessorHealth, StartFrame, SttSettings, TranscriptionFrame,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::mpsc;
enum Command {
    Audio {
        audio: AudioRawFrame,
        user_id: Option<String>,
    },
    Commit,
    Update(Box<ElevenLabsSttClient>),
    Finish,
}
struct State {
    client: ElevenLabsSttClient,
    input: Option<Input<Command>>,
    health_revision: u64,
    idle_user_id: String,
}
pub struct ElevenLabsSttProcessor {
    state: Mutex<State>,
    health: ProcessorHealth,
    user_id: Option<String>,
    mute: AtomicBool,
    audio_passthrough: bool,
}
impl ElevenLabsSttProcessor {
    pub fn new(client: ElevenLabsSttClient) -> Self {
        Self {
            state: Mutex::new(State {
                client,
                input: None,
                health_revision: 0,
                idle_user_id: String::new(),
            }),
            health: ProcessorHealth::default(),
            user_id: None,
            mute: AtomicBool::new(false),
            audio_passthrough: true,
        }
    }
    /// Fix the speaker on all results, including an explicitly empty ID.
    /// Without this override, accepted input audio supplies the current speaker.
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }
    pub fn with_audio_passthrough(mut self, enabled: bool) -> Self {
        self.audio_passthrough = enabled;
        self
    }
    pub fn config_snapshot(&self) -> Result<ElevenLabsSttConfig, StreamingError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| StreamingError::Closed)?
            .client
            .config()
            .clone())
    }
    pub fn update_settings(&self, delta: &SttSettings) -> Result<(), StreamingError> {
        let mut state = self.state.lock().map_err(|_| StreamingError::Closed)?;
        let candidate = settings::stt(&state.client, delta)?;
        if candidate.config() == state.client.config() {
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
            state.idle_user_id.clear();
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
            state.idle_user_id.clear();
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
        ready: Option<ElevenLabsSttSession>,
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
        let user_id = self
            .user_id
            .clone()
            .unwrap_or_else(|| std::mem::take(&mut state.idle_user_id));
        let revision = self.health.snapshot().revision;
        let mut health = self.health.watch();
        ctx.spawn("elevenlabs-transcripts", false, async move {
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
                run(session, rx, &output, user_id, revision).await
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
            .ok_or_else(|| processor_error(StreamingError::Closed))
    }
}
#[async_trait]
impl FrameProcessor for ElevenLabsSttProcessor {
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    fn name(&self) -> &str {
        "ElevenLabsRealtimeSTTService"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let client = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("STT state lock poisoned"))?
            .client
            .clone();
        if start.audio_in_sample_rate != client.config().sample_rate {
            return Err(processor_error(StreamingError::Invalid(
                "pipeline input rate differs from ElevenLabs",
            )));
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
            FrameData::SttUpdateSettings(update) if settings::targets(self, &update.service) => {
                let delta = match &update.delta {
                    Some(delta) => delta.clone(),
                    None => settings::stt_mapping(&update.settings).map_err(settings_error)?,
                };
                self.update_settings(&delta).map_err(settings_error)?;
                return Ok(());
            }
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
            FrameData::SttMute(mute) => self.mute.store(*mute, Ordering::Release),
            _ if direction == Direction::Upstream => return ctx.push(frame, direction).await,
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_)
                if ctx.is_usable() =>
            {
                self.start_session(ctx)?;
                if self
                    .config_snapshot()
                    .map_err(processor_error)?
                    .commit_strategy
                    == CommitStrategy::Manual
                {
                    self.input()?
                        .try_send(Command::Commit, false)
                        .map_err(processor_error)?;
                }
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                if let Ok(input) = self.input() {
                    input.cancel.cancel();
                }
            }
            _ => {}
        }
        let input_audio = match &frame.data {
            FrameData::InputAudio(audio) => Some((audio, "")),
            FrameData::UserAudio(user) => Some((&user.audio, user.user_id.as_str())),
            _ => None,
        };
        if let Some((audio, speaker)) = input_audio {
            if ctx.is_usable()
                && !self.mute.load(Ordering::Acquire)
                && (!audio.audio.is_empty() || self.user_id.is_none())
            {
                let config = self.config_snapshot().map_err(processor_error)?;
                // Empty audio only advances dynamic identity, retaining the
                // existing geometry bypass and fixed-override no-op behavior.
                if !audio.audio.is_empty() {
                    audio.validate().map_err(|_| {
                        processor_error(StreamingError::Invalid("incomplete PCM16 samples"))
                    })?;
                    if audio.sample_rate != config.sample_rate || audio.num_channels != 1 {
                        return Err(processor_error(StreamingError::Invalid(
                            "STT requires configured mono PCM16",
                        )));
                    }
                }
                let dynamic_speaker = self.user_id.is_none().then_some(speaker);
                // Bound newly retained PCM and identity independently from the
                // low-level base64/JSON wire-message limit. This budget excludes
                // pre-existing context IDs, the forwarded frame envelope and
                // the already configured fixed override.
                if audio.audio.len() > config.max_audio_chunk_bytes
                    || audio
                        .audio
                        .len()
                        .checked_add(dynamic_speaker.map_or(0, str::len))
                        .is_none_or(|bytes| bytes > config.limits.max_message_bytes)
                {
                    return Err(processor_error(StreamingError::TooLarge));
                }
                let user_id = dynamic_speaker.map(str::to_owned);
                if audio.audio.is_empty() {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("STT state lock poisoned"))?;
                    if let Some(input) = state
                        .input
                        .as_ref()
                        .filter(|input| !input.tx.is_closed() && !input.cancel.is_cancelled())
                    {
                        input
                            .try_send(
                                Command::Audio {
                                    audio: audio.clone(),
                                    user_id,
                                },
                                false,
                            )
                            .map_err(processor_error)?;
                    } else if let Some(user_id) = user_id {
                        // Identity-only input cannot start a connection.
                        state.idle_user_id = user_id;
                    }
                } else {
                    self.start_session(ctx)?;
                    self.input()?
                        .try_send(
                            Command::Audio {
                                audio: audio.clone(),
                                user_id,
                            },
                            false,
                        )
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
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("STT state lock poisoned"))?;
        if let Some(input) = state.input.take() {
            input.cancel.cancel();
        }
        state.idle_user_id.clear();
        Ok(())
    }
}
async fn run(
    mut session: ElevenLabsSttSession,
    mut commands: mpsc::Receiver<Command>,
    ctx: &ProcessorContext,
    mut user_id: String,
    revision: u64,
) -> Result<(), ProcessorError> {
    let mut closing = false;
    let mut health = ctx.health().watch();
    loop {
        if !same_generation(ctx, revision) {
            return Ok(());
        }
        tokio::select! {
            biased;
            _ = health.changed() => return Ok(()),
            event=session.next()=>match event {Some(Ok(event))=>emit(event,ctx,&user_id,revision).await?,Some(Err(StreamingError::Cancelled))=>return Ok(()),Some(Err(error))=>{if !same_generation(ctx, revision) { return Ok(()); } ctx.push_error(error_frame(error)).await?;return Ok(());},None=>return Ok(())},
            command=commands.recv(),if !closing=>{
                match command {
                    Some(Command::Update(client))=>{
                        if let Err(error)=submit(&mut session,Command::Finish,ctx,&user_id,revision).await? {if !same_generation(ctx, revision) { return Ok(()); } ctx.push_error(error_frame(error)).await?;return Ok(());}
                        loop {
                            if !same_generation(ctx, revision) { return Ok(()); }
                            let event = tokio::select! { biased; _ = health.changed() => return Ok(()), event = session.next() => event };
                            let Some(event) = event else { break; };
                            match event {Ok(event)=>emit(event,ctx,&user_id,revision).await?,Err(error)=>{if !same_generation(ctx, revision) { return Ok(()); } ctx.push_error(error_frame(error)).await?;return Ok(());}}}
                        if !same_generation(ctx, revision) { return Ok(()); }
                        let replacement = tokio::select! { biased; _ = health.changed() => return Ok(()), result = client.connect() => result };
                        session=match replacement {Ok(session)=>session,Err(error)=>{if !same_generation(ctx, revision) { return Ok(()); } ctx.push_error(error_frame(error)).await?;return Ok(());}};
                    },
                    command=>{
                        let command = match command.unwrap_or(Command::Finish) {
                            Command::Audio { audio, user_id: next } => {
                                if let Some(next) = next { user_id = next; }
                                if audio.audio.is_empty() { continue; }
                                Command::Audio { audio, user_id: None }
                            }
                            command => command,
                        };
                        closing=matches!(command,Command::Finish);
                        if let Err(error)=submit(&mut session,command,ctx,&user_id,revision).await? {if !same_generation(ctx, revision) { return Ok(()); } ctx.push_error(error_frame(error)).await?;return Ok(());}
                    }
                }
            }
        }
    }
}
async fn submit(
    session: &mut ElevenLabsSttSession,
    command: Command,
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
            Command::Audio { audio, .. } => sender.send_audio(audio).await,
            Command::Commit => sender.commit().await,
            Command::Finish => sender.finish().await,
            Command::Update(_) => unreachable!(),
        }
    };
    tokio::pin!(send);
    loop {
        tokio::select! {biased;_=health.changed()=>return Ok(Err(StreamingError::Cancelled)),result=&mut send=>return Ok(result),event=session.next()=>match event {Some(Ok(event))=>emit(event,ctx,user_id,revision).await?,Some(Err(error))=>return Ok(Err(error)),None=>return Ok(Err(StreamingError::Closed))}}
    }
}
async fn emit(
    event: ElevenLabsSttEvent,
    ctx: &ProcessorContext,
    user_id: &str,
    revision: u64,
) -> Result<(), ProcessorError> {
    if !same_generation(ctx, revision) {
        return Ok(());
    }
    if let ElevenLabsSttEvent::Transcript(transcript) = event {
        let frame = TranscriptionFrame {
            text: transcript.text,
            user_id: user_id.into(),
            timestamp: timestamp(),
            language: transcript.language,
            result: Some(transcript.result),
            finalized: transcript.finalized,
        };
        ctx.push(
            Frame::new(if transcript.is_final {
                FrameData::Transcription(frame)
            } else {
                FrameData::InterimTranscription(frame)
            }),
            Direction::Downstream,
        )
        .await?;
    }
    Ok(())
}

fn same_generation(ctx: &ProcessorContext, revision: u64) -> bool {
    let snapshot = ctx.health().snapshot();
    snapshot.is_usable && snapshot.revision == revision
}

fn settings_error(error: StreamingError) -> ProcessorError {
    processor_error(error).with_permanence_override(false)
}
