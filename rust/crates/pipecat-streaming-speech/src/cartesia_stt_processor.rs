//! Owned Cartesia Live STT lifecycle. No implicit reconnect or audio replay.
use crate::{
    cartesia_stt::CartesiaSttSettings,
    processor::{error_frame, processor_error, timestamp},
    settings, CancellationToken, CartesiaSttClient, CartesiaSttConfig, CartesiaSttEvent,
    CartesiaSttSender, StreamingError,
};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, Metric,
    ProcessorContext, ProcessorError, ProcessorHealth, ProcessorHealthGeneration,
    ServiceMetadataFrame, StartFrame, SttMetadataFrame, SttSettings, TranscriptionFrame,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
// Cleanup cadence only; every PCM admission, output and error has its own
// immediate generation fence. Quiet established sockets need no 100Hz timer.
const HANDSHAKE_ADMISSION_POLL: std::time::Duration = std::time::Duration::from_millis(100);
const IDLE_HEALTH_POLL: std::time::Duration = std::time::Duration::from_secs(1);
struct State {
    client: CartesiaSttClient,
    sender: Option<CartesiaSttSender>,
    cancel: Option<CancellationToken>,
    serial: u64,
    admission: Option<ProcessorHealthGeneration>,
    connecting: bool,
    ending: bool,
    owner: Option<pipecat_core::ObservedProcessor>,
}
/// One immutable recognizer/speaker per admitted socket. Effective settings
/// updates require `restart_with_settings`, or cleanup and a fresh processor.
/// No-op deltas neither reconnect nor restore health. Failures never replay PCM.
pub struct CartesiaSttProcessor {
    state: Arc<Mutex<State>>,
    emission: Arc<Mutex<()>>,
    setup_gate: tokio::sync::Mutex<()>,
    health: ProcessorHealth,
    muted: Arc<AtomicBool>,
    user_id: String,
    audio_passthrough: bool,
}
impl CartesiaSttProcessor {
    pub fn new(client: CartesiaSttClient) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                client,
                sender: None,
                cancel: None,
                serial: 0,
                admission: None,
                connecting: false,
                ending: false,
                owner: None,
            })),
            emission: Arc::default(),
            setup_gate: tokio::sync::Mutex::new(()),
            health: Default::default(),
            muted: Arc::new(AtomicBool::new(false)),
            user_id: String::new(),
            audio_passthrough: true,
        }
    }
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
    pub fn config_snapshot(&self) -> Result<CartesiaSttConfig, StreamingError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| StreamingError::Closed)?
            .client
            .config()
            .clone())
    }
    /// This does not restore usability, reconnect or replay buffered work.
    /// Effective connection changes during connect/active operation are rejected.
    pub fn update_settings(&self, delta: &SttSettings) -> Result<(), StreamingError> {
        let mut state = self.state.lock().map_err(|_| StreamingError::Closed)?;
        let mut config = state.client.config().clone();
        config.settings = config.settings.merge(delta)?;
        let candidate = CartesiaSttClient::new(config)?;
        if candidate.config().settings == state.client.config().settings {
            return Ok(());
        }
        if ineffective(
            &candidate.config().settings,
            &state.client.config().settings,
        ) {
            return Err(StreamingError::Invalid(
                "Cartesia keyterms have no effect for this model or normalized limits",
            ));
        }
        if state.connecting
            || state
                .sender
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
        {
            return Err(StreamingError::Invalid(
                "active Cartesia settings require explicit restart",
            ));
        }
        if state.ending {
            return Err(StreamingError::Closed);
        }
        state.client = candidate;
        Ok(())
    }
    /// Explicit destructive replacement: validate the complete delta, retire the
    /// old socket and its queued audio/results, then open one fresh connection.
    /// Caller must restore health separately if unusable. End/Cancel is terminal.
    /// This owner operation needs its live processor context (for example from a
    /// controlling processor); ordinary settings frames never call it implicitly.
    pub async fn restart_with_settings(
        &self,
        delta: &SttSettings,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let _setup = self.setup_gate.lock().await;
        let admission = ctx.health().generation();
        {
            let _emit = self
                .emission
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            if !live_owner(&state, ctx) || self.muted.load(Ordering::Acquire) {
                return Err(processor_error(StreamingError::Closed).with_permanence_override(false));
            }
            let mut config = state.client.config().clone();
            config.settings = config.settings.merge(delta).map_err(nonpermanent)?;
            let candidate = CartesiaSttClient::new(config).map_err(nonpermanent)?;
            if candidate.config().settings != state.client.config().settings
                && ineffective(
                    &candidate.config().settings,
                    &state.client.config().settings,
                )
            {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia keyterms have no effect for this model or normalized limits",
                )));
            }
            if ctx.health().generation() != admission {
                return Err(nonpermanent(StreamingError::Cancelled));
            }
            retire(&mut state).map_err(nonpermanent)?;
            state.client = candidate;
        }
        self.connect(ctx.configuration(), ctx, admission).await
    }
    fn cancel_current(&self, terminal: bool) -> Result<(), ProcessorError> {
        let _emit = self
            .emission
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?;
        state.ending |= terminal;
        retire(&mut state).map_err(processor_error)
    }
    async fn connect(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
        admission: ProcessorHealthGeneration,
    ) -> Result<(), ProcessorError> {
        if !ctx.is_usable() || self.muted.load(Ordering::Acquire) {
            return Err(nonpermanent(StreamingError::Cancelled));
        }
        let mut health = ctx.health().watch();
        let cancel = CancellationToken::new();
        let (serial, client) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            if !live_owner(&state, ctx) || ctx.health().generation() != admission {
                return Err(nonpermanent(StreamingError::Closed));
            }
            if start.audio_in_sample_rate != state.client.config().sample_rate {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia pipeline sample rate",
                )));
            }
            if state.sender.as_ref().is_some_and(|s| !s.is_closed())
                && state.admission == Some(admission)
            {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia already connected",
                )));
            }
            retire(&mut state).map_err(processor_error)?;
            state.cancel = Some(cancel.clone());
            state.admission = Some(admission);
            state.connecting = true;
            (state.serial, state.client.clone())
        };
        // A true-to-true explicit health restoration has no transition-watch
        // notification. While a handshake is pending, poll only its admission
        // token/current invocation so that retirement never waits for the peer.
        let retired = async {
            loop {
                tokio::time::sleep(HANDSHAKE_ADMISSION_POLL).await;
                if ctx.health().generation() != admission || !current_invocation(ctx) {
                    return;
                }
            }
        };
        let result = tokio::select! { biased;
            _ = health.changed() => Err(StreamingError::Cancelled),
            _ = retired => Err(StreamingError::Cancelled),
            result = client.connect_with_cancel(cancel.clone()) => result,
        };
        let mut session = match result {
            Ok(session) => session,
            Err(error) => {
                cancel.cancel();
                let _emit = self
                    .emission
                    .lock()
                    .map_err(|_| processor_error(StreamingError::Closed))?;
                let current = {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| processor_error(StreamingError::Closed))?;
                    if state.serial == serial {
                        state.connecting = false;
                        true
                    } else {
                        false
                    }
                };
                if current && error != StreamingError::Cancelled {
                    ctx.try_push_error_if_health_generation(
                        error_frame(error.clone()),
                        false,
                        admission,
                    )?;
                }
                return Err(nonpermanent(error));
            }
        };
        {
            let _emit = self
                .emission
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            if state.serial != serial
                || !live_owner(&state, ctx)
                || cancel.is_cancelled()
                || self.muted.load(Ordering::Acquire)
                || ctx.health().generation() != admission
            {
                session.cancel();
                if state.serial == serial {
                    retire(&mut state).map_err(nonpermanent)?;
                }
                return Err(nonpermanent(StreamingError::Cancelled));
            }
            state.connecting = false;
            state.sender = Some(session.sender());
        }
        let state = self.state.clone();
        let emission = self.emission.clone();
        let muted = self.muted.clone();
        let output = ctx.clone();
        let user_id = self.user_id.clone();
        ctx.spawn("cartesia-live-stt", false, async move {
            loop {
                let event = tokio::select! { biased;
                    _ = cancel.cancelled() => { session.cancel(); return Ok(()); },
                    _ = health.changed() => { session.cancel(); return Ok(()); },
                    _ = tokio::time::sleep(IDLE_HEALTH_POLL) => {
                        if output.health().generation() != admission || !output.is_usable() { session.cancel(); return Ok(()); }
                        continue;
                    },
                    event = session.next() => event,
                };
                // No await while publication is serialized with retirement. The
                // runtime enqueue is bounded/nonblocking; accepted output cannot
                // be retracted by a later restart.
                let _emit = emission.lock().map_err(|_| processor_error(StreamingError::Closed))?;
                let current = state.lock().map_err(|_| processor_error(StreamingError::Closed))?.serial == serial;
                if !current || cancel.is_cancelled() || muted.load(Ordering::Acquire) || !output.is_usable() || output.health().generation() != admission { session.cancel(); return Ok(()); }
                match event {
                    Some(Ok(CartesiaSttEvent::Transcript(t))) => {
                        if t.is_final { usage(&output, t.audio_seconds)?; }
                        let frame = TranscriptionFrame { text: t.text, user_id: user_id.clone(), timestamp: timestamp(), language: t.language, result: Some(t.result), finalized: false };
                        output.try_push(Frame::new(if t.is_final { FrameData::Transcription(frame) } else { FrameData::InterimTranscription(frame) }), Direction::Downstream)?;
                    },
                    Some(Ok(CartesiaSttEvent::Usage { audio_seconds })) => usage(&output, audio_seconds)?,
                    Some(Err(StreamingError::Cancelled)) | None => return Ok(()),
                    Some(Err(error)) => { output.try_push_error_if_health_generation(error_frame(error), false, admission)?; return Ok(()); },
                }
            }
        })?;
        Ok(())
    }
    fn audio(
        &self,
        audio: &AudioRawFrame,
        user_id: &str,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !ctx.is_usable() || self.muted.load(Ordering::Acquire) || audio.audio.is_empty() {
            return Ok(());
        }
        if user_id != self.user_id {
            return Err(nonpermanent(StreamingError::Invalid(
                "Cartesia speaker changed within session",
            )));
        }
        let state = self
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?;
        if state.ending || state.admission != Some(ctx.health().generation()) {
            return Err(nonpermanent(StreamingError::Closed));
        }
        state
            .sender
            .as_ref()
            .ok_or_else(|| nonpermanent(StreamingError::Closed))?
            .try_send_audio(audio.clone())
            .map_err(nonpermanent)
    }
}
fn ineffective(next: &CartesiaSttSettings, old: &CartesiaSttSettings) -> bool {
    next.model == old.model
        && next.language == old.language
        && next.prepared_keyterms().terms == old.prepared_keyterms().terms
}
fn live_owner(state: &State, ctx: &ProcessorContext) -> bool {
    !state.ending
        && ctx.is_usable()
        && current_invocation(ctx)
        && state.owner.as_ref() == Some(&ctx.processor_identity())
}
fn current_invocation(ctx: &ProcessorContext) -> bool {
    // Raw causal identity must survive a caller's noninterruptible task-local
    // override. is_current() alone intentionally honors that override.
    ctx.invocation_fence().is_ok_and(|fence| fence.is_current())
}
fn retire(state: &mut State) -> Result<(), StreamingError> {
    state.serial = state
        .serial
        .checked_add(1)
        .ok_or(StreamingError::TooLarge)?;
    if let Some(cancel) = state.cancel.take() {
        cancel.cancel();
    }
    if let Some(sender) = state.sender.take() {
        sender.cancel();
    }
    state.connecting = false;
    Ok(())
}
fn nonpermanent(error: StreamingError) -> ProcessorError {
    processor_error(error).with_permanence_override(false)
}
fn usage(ctx: &ProcessorContext, seconds: f64) -> Result<(), ProcessorError> {
    if seconds > 0.0 && ctx.configuration().enable_usage_metrics {
        ctx.try_push(
            Frame::new(FrameData::Metrics(vec![Metric::SttUsage {
                processor: "CartesiaSTTService".into(),
                audio_seconds: seconds,
            }])),
            Direction::Downstream,
        )?;
    }
    Ok(())
}
impl Drop for CartesiaSttProcessor {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            let _ = retire(&mut state);
        }
    }
}
#[async_trait]
impl FrameProcessor for CartesiaSttProcessor {
    fn name(&self) -> &str {
        "CartesiaSTTService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let _setup = self.setup_gate.lock().await;
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            let identity = ctx.processor_identity();
            if !current_invocation(ctx)
                || state.owner.as_ref().is_some_and(|owner| owner != &identity)
            {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia context is not the live owner",
                )));
            }
            state.owner = Some(identity);
        }
        if !ctx.is_usable() || self.muted.load(Ordering::Acquire) {
            return Ok(());
        }
        self.connect(start, ctx, ctx.health().generation()).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => self.cancel_current(true)?,
            FrameData::SttMute(value) => {
                if self.muted.swap(*value, Ordering::AcqRel) != *value {
                    if *value {
                        self.cancel_current(false)?;
                    } else {
                        self.setup(ctx.configuration(), ctx).await?;
                    }
                }
            }
            FrameData::SttUpdateSettings(update) if settings::targets(self, &update.service) => {
                let delta = match &update.delta {
                    Some(delta) => delta.clone(),
                    None => settings::stt_mapping(&update.settings).map_err(nonpermanent)?,
                };
                self.update_settings(&delta).map_err(nonpermanent)?;
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
                    None => settings::stt_mapping(&update.settings).map_err(nonpermanent)?,
                };
                self.update_settings(&delta).map_err(nonpermanent)?;
                return Ok(());
            }
            FrameData::Start(_) if direction == Direction::Downstream => {
                ctx.push(
                    Frame::new(FrameData::SttMetadata(SttMetadataFrame {
                        base: ServiceMetadataFrame {
                            service_name: self.name().into(),
                            user_turn_strategies: None,
                        },
                        ttfs_p99_latency: 0.81,
                    })),
                    direction,
                )
                .await?
            }
            FrameData::InputAudio(audio) if direction == Direction::Downstream => {
                self.audio(audio, &self.user_id, ctx)?;
                if !self.audio_passthrough {
                    return Ok(());
                }
            }
            FrameData::UserAudio(user) if direction == Direction::Downstream => {
                self.audio(&user.audio, &user.user_id, ctx)?;
                if !self.audio_passthrough {
                    return Ok(());
                }
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_)
                if ctx.is_usable() && !self.muted.load(Ordering::Acquire) =>
            {
                let state = self
                    .state
                    .lock()
                    .map_err(|_| processor_error(StreamingError::Closed))?;
                if !state.ending && state.admission == Some(ctx.health().generation()) {
                    state
                        .sender
                        .as_ref()
                        .ok_or_else(|| nonpermanent(StreamingError::Closed))?
                        .try_finalize()
                        .map_err(nonpermanent)?;
                }
            }
            _ => (),
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let sender = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            state.ending = true;
            if state.connecting
                || state.admission != Some(ctx.health().generation())
                || !ctx.is_usable()
            {
                if let Some(cancel) = &state.cancel {
                    cancel.cancel();
                }
                None
            } else {
                state.sender.clone()
            }
        };
        if let Some(sender) = sender {
            if !sender.is_closed() {
                sender.finish().await.map_err(nonpermanent)?;
            }
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cancel_current(true)
    }
}
