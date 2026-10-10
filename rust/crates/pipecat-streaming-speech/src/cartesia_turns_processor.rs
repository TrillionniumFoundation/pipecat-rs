//! Owned Cartesia Turns lifecycle with committed turns and no audio replay.
use crate::{
    cartesia_turns::{
        CartesiaTurnsClient, CartesiaTurnsConfig, CartesiaTurnsEvent, CartesiaTurnsSender,
        CartesiaTurnsSettings,
    },
    processor::{error_frame, processor_error, timestamp},
    settings, CancellationToken, StreamingError,
};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, ErrorFrame, Frame, FrameData, FrameProcessor,
    InvocationFence, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorHealthGeneration, ServiceMetadataFrame, StartFrame, SttMetadataFrame, SttSettings,
    TranscriptionFrame, UserTurnStrategies, UserTurnStrategy,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};

// Cleanup cadence only. PCM, readiness and every output segment have separate
// immediate admission checks, including a true-to-true health restoration.
const HANDSHAKE_ADMISSION_POLL: std::time::Duration = std::time::Duration::from_millis(100);
const IDLE_HEALTH_POLL: std::time::Duration = std::time::Duration::from_secs(1);

struct State {
    client: CartesiaTurnsClient,
    sender: Option<CartesiaTurnsSender>,
    cancel: Option<CancellationToken>,
    barrier_cancel: Option<CancellationToken>,
    serial: u64,
    timeline: Arc<AtomicU64>,
    admission: Option<ProcessorHealthGeneration>,
    connecting: bool,
    ending: bool,
    owner: Option<pipecat_core::ObservedProcessor>,
    // Initial setup has lifecycle scope, not one microphone frame's epoch.
    // Retaining it lets an admitted recognizer survive ordinary interruption
    // without laundering a stale restart invocation into a new connection.
    owner_context: Option<ProcessorContext>,
}

/// One immutable recognizer and speaker per admitted socket. This committed-only
/// processor exposes External strategy recommendations; eager/resume events do
/// not emit speculative pipeline output. Effective active settings changes need
/// an explicit owner restart. No-op updates never reconnect or restore health.
pub struct CartesiaTurnsProcessor {
    state: Arc<Mutex<State>>,
    emission: Arc<Mutex<()>>,
    setup_gate: tokio::sync::Mutex<()>,
    health: ProcessorHealth,
    muted: Arc<AtomicBool>,
    user_id: String,
    audio_passthrough: bool,
    should_interrupt: bool,
}

impl CartesiaTurnsProcessor {
    pub fn new(client: CartesiaTurnsClient) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                client,
                sender: None,
                cancel: None,
                barrier_cancel: None,
                serial: 0,
                timeline: Arc::default(),
                admission: None,
                connecting: false,
                ending: false,
                owner: None,
                owner_context: None,
            })),
            emission: Arc::default(),
            setup_gate: tokio::sync::Mutex::new(()),
            health: Default::default(),
            muted: Arc::new(AtomicBool::new(false)),
            user_id: String::new(),
            audio_passthrough: true,
            should_interrupt: true,
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

    /// Only the recommended External start strategy decides interruption.
    pub fn with_should_interrupt(mut self, value: bool) -> Self {
        self.should_interrupt = value;
        self
    }

    pub fn config_snapshot(&self) -> Result<CartesiaTurnsConfig, StreamingError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| StreamingError::Closed)?
            .client
            .config()
            .clone())
    }

    /// Validate transactionally. Dormant changes do not start a socket or
    /// restore health; effective active changes require explicit replacement.
    pub fn update_settings(&self, delta: &SttSettings) -> Result<(), StreamingError> {
        let mut state = self.state.lock().map_err(|_| StreamingError::Closed)?;
        let mut config = state.client.config().clone();
        config.settings = config.settings.merge(delta)?;
        let candidate = CartesiaTurnsClient::new(config)?;
        if state.ending {
            return Err(StreamingError::Closed);
        }
        if candidate.config().settings == state.client.config().settings {
            return Ok(());
        }
        if ineffective(
            &candidate.config().settings,
            &state.client.config().settings,
        ) {
            return Err(StreamingError::Invalid(
                "Cartesia Turns keyterms have no effect after normalization",
            ));
        }
        if state.connecting
            || state
                .sender
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
        {
            return Err(StreamingError::Invalid(
                "active Cartesia Turns settings require explicit restart",
            ));
        }
        state.client = candidate;
        Ok(())
    }

    /// Validate the complete candidate before retiring old PCM/results, then
    /// await application-level connected readiness on one fresh socket. This
    /// requires the live owner context and never restores health or replays PCM.
    /// Exact no-ops do not replace even a dormant or failed socket.
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
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            if !live_owner(&state, ctx) || self.muted.load(Ordering::Acquire) {
                return Err(nonpermanent(StreamingError::Closed));
            }
            let mut config = state.client.config().clone();
            config.settings = config.settings.merge(delta).map_err(nonpermanent)?;
            let candidate = CartesiaTurnsClient::new(config).map_err(nonpermanent)?;
            if candidate.config().settings == state.client.config().settings {
                return Ok(());
            }
            if ineffective(
                &candidate.config().settings,
                &state.client.config().settings,
            ) {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia Turns keyterms have no effect after normalization",
                )));
            }
            if ctx.health().generation() != admission || !current_invocation(ctx) {
                return Err(nonpermanent(StreamingError::Cancelled));
            }
            retire(&mut state).map_err(nonpermanent)?;
            state.client = candidate;
        }
        self.connect(ctx.configuration(), ctx, admission, true)
            .await
    }

    fn cancel_current(&self, terminal: bool) -> Result<(), ProcessorError> {
        let _emit = self
            .emission
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?;
        state.ending |= terminal;
        retire(&mut state).map_err(nonpermanent)
    }

    async fn connect(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
        admission: ProcessorHealthGeneration,
        report_provider_error: bool,
    ) -> Result<(), ProcessorError> {
        if !ctx.is_usable() || self.muted.load(Ordering::Acquire) || !current_invocation(ctx) {
            return Err(nonpermanent(StreamingError::Cancelled));
        }
        let mut health = ctx.health().watch();
        let cancel = CancellationToken::new();
        let barrier_cancel = CancellationToken::new();
        let (serial, timeline, client, output) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            if !live_owner(&state, ctx) || ctx.health().generation() != admission {
                return Err(nonpermanent(StreamingError::Closed));
            }
            if start.audio_in_sample_rate != state.client.config().sample_rate {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia Turns pipeline sample rate",
                )));
            }
            if state
                .sender
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
                && state.admission == Some(admission)
            {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia Turns already connected",
                )));
            }
            let output = state
                .owner_context
                .clone()
                .ok_or_else(|| nonpermanent(StreamingError::Closed))?;
            retire(&mut state).map_err(nonpermanent)?;
            state.cancel = Some(cancel.clone());
            state.barrier_cancel = Some(barrier_cancel.clone());
            state.admission = Some(admission);
            state.connecting = true;
            (
                state.serial,
                state.timeline.clone(),
                state.client.clone(),
                output,
            )
        };
        let fence = output.invocation_fence()?;
        let barrier_timeout = client
            .config()
            .limits
            .response_timeout
            .min(std::time::Duration::from_secs(5));
        let ready = Arc::new(AtomicBool::new(false));
        let admitted: Arc<dyn Fn() -> bool + Send + Sync> = {
            let muted = self.muted.clone();
            let output = output.clone();
            let caller = ctx.clone();
            let cancel = cancel.clone();
            let ready = ready.clone();
            Arc::new(move || {
                !cancel.is_cancelled()
                    && !muted.load(Ordering::Acquire)
                    && timeline.load(Ordering::Acquire) == serial
                    && output.is_usable()
                    && output.health().generation() == admission
                    && output.is_current()
                    && (ready.load(Ordering::Acquire) || current_invocation(&caller))
            })
        };
        // A same-value restoration does not send a transition notification.
        // The guard fences every actor action; this only bounds quiet cleanup.
        let retired = async {
            loop {
                tokio::time::sleep(HANDSHAKE_ADMISSION_POLL).await;
                if !admitted() || !current_invocation(ctx) {
                    return;
                }
            }
        };
        let result = tokio::select! { biased;
            _ = health.changed() => Err(StreamingError::Cancelled),
            _ = retired => Err(StreamingError::Cancelled),
            result = client.connect_guarded(cancel.clone(), admitted.clone()) => result,
        };
        let mut session = match result {
            Ok(session) => session,
            Err(error) => {
                // Check before canceling our token: cancellation itself must
                // not hide an admitted provider failure from the owner.
                let _emit = self
                    .emission
                    .lock()
                    .map_err(|_| nonpermanent(StreamingError::Closed))?;
                let current = admitted() && current_invocation(ctx);
                cancel.cancel();
                if let Ok(mut state) = self.state.lock() {
                    if state.serial == serial {
                        state.connecting = false;
                    }
                }
                if !current {
                    return Err(nonpermanent(StreamingError::Cancelled));
                }
                if report_provider_error && error != StreamingError::Cancelled {
                    ctx.try_push_error_if_health_generation(
                        error_frame(error.clone()),
                        false,
                        admission,
                    )?;
                }
                return Err(if report_provider_error {
                    nonpermanent(error)
                } else {
                    // Setup/process owns one current-generation runtime report.
                    // Explicit owner restart reports through its guarded path.
                    processor_error(error)
                });
            }
        };
        {
            let _emit = self
                .emission
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
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
            ready.store(true, Ordering::Release);
        }
        let output = SessionOutput {
            ctx: output,
            fence,
            state: self.state.clone(),
            admitted,
            admission,
            user_id: self.user_id.clone(),
        };
        let emission = self.emission.clone();
        ctx.spawn("cartesia-turns-stt", false, async move {
            loop {
                let event = tokio::select! { biased;
                    _ = cancel.cancelled() => { session.cancel(); return Ok(()); },
                    _ = health.changed() => { session.cancel(); return Ok(()); },
                    _ = tokio::time::sleep(IDLE_HEALTH_POLL) => {
                        if !(output.admitted)() || !output.fence.is_current() { session.cancel(); return Ok(()); }
                        continue;
                    },
                    event = session.next() => event,
                };
                let settle = {
                    // Retirement cannot interleave with this sequence. Every
                    // segment rechecks health: an observer may restore health
                    // synchronously during the preceding enqueue.
                    let Ok(_emit) = emission.lock() else {
                        session.cancel();
                        return Ok(());
                    };
                    if !(output.admitted)() || !output.fence.is_current() {
                        session.cancel();
                        return Ok(());
                    }
                    let result = match event {
                        Some(Ok(CartesiaTurnsEvent::Start { .. })) => output.start(),
                        Some(Ok(event)) => output.event(event).map(|current| (current, None)),
                        Some(Err(StreamingError::Cancelled)) | None => return Ok(()),
                        Some(Err(error)) => {
                            output.report(error_frame(error));
                            return Ok(());
                        }
                    };
                    match result {
                        Ok((true, settle)) => settle,
                        Ok((false, _)) => { session.cancel(); return Ok(()); },
                        Err(error) => {
                            output.report(ErrorFrame {
                                error: error.message,
                                category: error.category,
                                fatal: error.fatal,
                                ..ErrorFrame::new("")
                            });
                            session.cancel();
                            // No second, unfenced runtime error-reporting path.
                            return Ok(());
                        }
                    }
                };
                if let Some(settle) = settle {
                    // External start may interrupt normal data already queued
                    // downstream. Complete the existing flush roundtrip before
                    // admitting this turn's interim/final/stop. Never hold the
                    // emission lock across this bounded wait. End releases it
                    // too, so a returning flush cannot deadlock terminal drain.
                    tokio::select! { biased;
                        _ = cancel.cancelled() => { session.cancel(); return Ok(()); },
                        _ = barrier_cancel.cancelled() => (),
                        _ = health.changed() => { session.cancel(); return Ok(()); },
                        result = tokio::time::timeout(barrier_timeout, settle.wait()) => {
                            if result.is_err() {
                                if let Ok(_emit) = emission.lock() {
                                    output.report(error_frame(StreamingError::Timeout("Turns turn-start barrier")));
                                }
                                session.cancel();
                                return Ok(());
                            }
                        }
                    }
                    if !(output.admitted)() || !output.fence.is_current() {
                        session.cancel();
                        return Ok(());
                    }
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
                "Cartesia Turns speaker changed within session",
            )));
        }
        let _emit = self
            .emission
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?;
        let state = self
            .state
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?;
        if !live_owner(&state, ctx) || state.admission != Some(ctx.health().generation()) {
            return Err(nonpermanent(StreamingError::Closed));
        }
        let sender = state
            .sender
            .as_ref()
            .ok_or_else(|| nonpermanent(StreamingError::Closed))?;
        // Unlike a task's interruption-survival override, the raw invocation
        // is required for each new piece of PCM admitted by its caller.
        ctx.invocation_fence()?
            .with_current(|| sender.try_send_audio(audio.clone()))
            .ok_or_else(|| nonpermanent(StreamingError::Cancelled))?
            .map_err(nonpermanent)
    }

    fn metadata(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let admission = ctx.health().generation();
        let fence = ctx.invocation_fence()?;
        let frame = Frame::new(FrameData::SttMetadata(SttMetadataFrame {
            base: ServiceMetadataFrame {
                service_name: self.name().into(),
                user_turn_strategies: Some(UserTurnStrategies {
                    start: Some(vec![UserTurnStrategy::External {
                        enable_interruptions: self.should_interrupt,
                    }]),
                    stop: Some(vec![UserTurnStrategy::External {
                        enable_interruptions: self.should_interrupt,
                    }]),
                }),
            },
            ttfs_p99_latency: 0.0,
        }));
        let (down, up) = frame.broadcast_pair();
        let _emit = self
            .emission
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?;
        for (frame, direction) in [(down, Direction::Downstream), (up, Direction::Upstream)] {
            if ctx.health().generation() != admission || !fence.is_current() {
                break;
            }
            ctx.try_push_with_invocation_fence(frame, direction, &fence)?;
        }
        Ok(())
    }
}

struct SessionOutput {
    ctx: ProcessorContext,
    fence: InvocationFence,
    state: Arc<Mutex<State>>,
    admitted: Arc<dyn Fn() -> bool + Send + Sync>,
    admission: ProcessorHealthGeneration,
    user_id: String,
}

impl SessionOutput {
    fn start(&self) -> Result<(bool, Option<pipecat_core::LocalFlushEvent>), ProcessorError> {
        if self
            .state
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?
            .ending
        {
            return Ok(((self.admitted)(), None));
        }
        if !self.broadcast(FrameData::ProposedUserStartedSpeaking)? {
            return Ok((false, None));
        }
        let settled = pipecat_core::LocalFlushEvent::default();
        let current = self.push(
            Frame::new(FrameData::FlushDetailed(pipecat_core::PipelineFlushFrame {
                event: Some(settled.clone()),
                returning: false,
                origin: Some("pipecat.cartesia-turns.turn-start".into()),
            })),
            Direction::Downstream,
        )?;
        Ok((current, current.then_some(settled)))
    }

    fn push(&self, frame: Frame, direction: Direction) -> Result<bool, ProcessorError> {
        if !(self.admitted)() {
            return Ok(false);
        }
        self.ctx
            .try_push_with_invocation_fence(frame, direction, &self.fence)?;
        Ok(true)
    }

    fn broadcast(&self, data: FrameData) -> Result<bool, ProcessorError> {
        let (down, up) = Frame::new(data).broadcast_pair();
        if !self.push(down, Direction::Downstream)? {
            return Ok(false);
        }
        self.push(up, Direction::Upstream)
    }

    fn usage(&self, seconds: f64) -> Result<bool, ProcessorError> {
        if seconds > 0.0 && self.ctx.configuration().enable_usage_metrics {
            return self.push(
                Frame::new(FrameData::Metrics(vec![Metric::SttUsage {
                    processor: "CartesiaTurnsSTTService".into(),
                    audio_seconds: seconds,
                }])),
                Direction::Downstream,
            );
        }
        Ok((self.admitted)())
    }

    fn transcription(&self, text: String, result: serde_json::Value, finalized: bool) -> Frame {
        let transcript = TranscriptionFrame {
            text,
            user_id: self.user_id.clone(),
            timestamp: timestamp(),
            language: Some("en".into()),
            result: Some(result),
            finalized,
        };
        Frame::new(if finalized {
            FrameData::Transcription(transcript)
        } else {
            FrameData::InterimTranscription(transcript)
        })
    }

    fn event(&self, event: CartesiaTurnsEvent) -> Result<bool, ProcessorError> {
        let ending = self
            .state
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?
            .ending;
        match event {
            CartesiaTurnsEvent::Update { transcript, result }
                if !ending && !transcript.is_empty() =>
            {
                self.push(
                    self.transcription(transcript, result, false),
                    Direction::Downstream,
                )
            }
            CartesiaTurnsEvent::End {
                transcript,
                result,
                audio_seconds,
            } => {
                // Empty means exactly empty: whitespace has source truthiness.
                // Never invent final text to make an aggregator close sooner.
                if !transcript.is_empty()
                    && (!self.usage(audio_seconds)?
                        || !self.push(
                            self.transcription(transcript, result, true),
                            Direction::Downstream,
                        )?)
                {
                    return Ok(false);
                }
                // The stop's normal priority preserves usage -> final -> stop.
                self.broadcast(FrameData::ProposedUserStoppedSpeaking)
            }
            CartesiaTurnsEvent::Usage { audio_seconds } => self.usage(audio_seconds),
            CartesiaTurnsEvent::Connected { .. } => Err(nonpermanent(StreamingError::Invalid(
                "duplicate Cartesia Turns connected event",
            ))),
            // Eager predictions and resumes remain visible through the session
            // API, with no speculative or committed pipeline output here.
            CartesiaTurnsEvent::Start { .. }
            | CartesiaTurnsEvent::Update { .. }
            | CartesiaTurnsEvent::EagerEnd { .. }
            | CartesiaTurnsEvent::Resume { .. } => Ok((self.admitted)()),
        }
    }

    fn report(&self, error: ErrorFrame) {
        if (self.admitted)() && self.fence.is_current() {
            let _ = self
                .ctx
                .try_push_error_if_health_generation(error, false, self.admission);
        }
    }
}

fn ineffective(next: &CartesiaTurnsSettings, old: &CartesiaTurnsSettings) -> bool {
    next.model == old.model
        && next.turn_start_threshold == old.turn_start_threshold
        && next.turn_eager_end_threshold == old.turn_eager_end_threshold
        && next.turn_end_threshold == old.turn_end_threshold
        && next.turn_end_timeout_ms == old.turn_end_timeout_ms
        && next.prepared_keyterms().terms == old.prepared_keyterms().terms
}

fn live_owner(state: &State, ctx: &ProcessorContext) -> bool {
    !state.ending
        && ctx.is_usable()
        && current_invocation(ctx)
        && state.owner.as_ref() == Some(&ctx.processor_identity())
}

fn current_invocation(ctx: &ProcessorContext) -> bool {
    ctx.invocation_fence().is_ok_and(|fence| fence.is_current())
}

fn retire(state: &mut State) -> Result<(), StreamingError> {
    state.serial = state
        .serial
        .checked_add(1)
        .ok_or(StreamingError::TooLarge)?;
    state.timeline.store(state.serial, Ordering::Release);
    if let Some(cancel) = state.cancel.take() {
        cancel.cancel();
    }
    if let Some(cancel) = state.barrier_cancel.take() {
        cancel.cancel();
    }
    if let Some(sender) = state.sender.take() {
        sender.cancel();
    }
    state.connecting = false;
    state.admission = None;
    Ok(())
}

fn nonpermanent(error: StreamingError) -> ProcessorError {
    processor_error(error).with_permanence_override(false)
}

impl Drop for CartesiaTurnsProcessor {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            let _ = retire(&mut state);
            state.owner_context = None;
        }
    }
}

#[async_trait]
impl FrameProcessor for CartesiaTurnsProcessor {
    fn name(&self) -> &str {
        "CartesiaTurnsSTTService"
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
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            let identity = ctx.processor_identity();
            if state.ending
                || !current_invocation(ctx)
                || state.owner.as_ref().is_some_and(|owner| owner != &identity)
            {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia Turns context is not the live owner",
                )));
            }
            if state.owner.is_none() {
                state.owner = Some(identity);
                state.owner_context = Some(ctx.clone());
            }
        }
        if !ctx.is_usable() || self.muted.load(Ordering::Acquire) {
            return Ok(());
        }
        let admission = ctx.health().generation();
        let result = self.connect(start, ctx, admission, false).await;
        if result.is_err() {
            let retired = ctx.health().generation() != admission
                || !current_invocation(ctx)
                || self.muted.load(Ordering::Acquire)
                || self.state.lock().map_or(true, |state| state.ending);
            if retired {
                // A canceled/retired setup must not reclassify an old provider
                // error through the runtime's fresh lifecycle context.
                return Ok(());
            }
        }
        result
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
                {
                    let state = self
                        .state
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    if state.ending
                        || !current_invocation(ctx)
                        || (!*value && !ctx.is_usable())
                        || state.owner.as_ref() != Some(&ctx.processor_identity())
                    {
                        return Err(nonpermanent(StreamingError::Closed));
                    }
                }
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
                // The aggregator must receive Start before recommendations.
                ctx.push(frame, direction).await?;
                return self.metadata(ctx);
            }
            FrameData::ServiceSwitcherRequestMetadata(target)
                if settings::targets(self, &Some(target.clone())) =>
            {
                self.metadata(ctx)?;
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
            // VAD, actual speaking, interruption and output audio keep their
            // normal route. Turns has no finalize command or local turn buffer.
            _ => (),
        }
        ctx.push(frame, direction).await
    }

    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let sender = {
            let _emit = self
                .emission
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            if !current_invocation(ctx) || state.owner.as_ref() != Some(&ctx.processor_identity()) {
                return Err(nonpermanent(StreamingError::Closed));
            }
            state.ending = true;
            if let Some(cancel) = &state.barrier_cancel {
                cancel.cancel();
            }
            if state.connecting
                || state.admission != Some(ctx.health().generation())
                || !ctx.is_usable()
            {
                retire(&mut state).map_err(nonpermanent)?;
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
        // The runtime drains the tracked reader before forwarding End/Stop.
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cancel_current(true)?;
        self.state
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?
            .owner_context = None;
        Ok(())
    }
}
