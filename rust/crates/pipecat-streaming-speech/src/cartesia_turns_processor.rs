//! Owned Cartesia Turns lifecycle with opt-in source-bound eager turns.
#[cfg(feature = "cartesia-turns-eager")]
use crate::cartesia_turns_eager::EagerSession;
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
    #[cfg(feature = "cartesia-turns-eager")]
    eager: Option<Arc<EagerSession>>,
}

/// One immutable recognizer and speaker per admitted socket. The default emits
/// committed turns and External strategy recommendations. Source-bound eager
/// output is construction-only opt-in. Effective active settings changes need
/// an explicit owner restart; no-op updates never reconnect or restore health.
pub struct CartesiaTurnsProcessor {
    state: Arc<Mutex<State>>,
    emission: Arc<Mutex<()>>,
    setup_gate: tokio::sync::Mutex<()>,
    health: ProcessorHealth,
    muted: Arc<AtomicBool>,
    user_id: String,
    audio_passthrough: bool,
    should_interrupt: bool,
    #[cfg(feature = "cartesia-turns-eager")]
    eager_turns: bool,
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
                #[cfg(feature = "cartesia-turns-eager")]
                eager: None,
            })),
            emission: Arc::default(),
            setup_gate: tokio::sync::Mutex::new(()),
            health: Default::default(),
            muted: Arc::new(AtomicBool::new(false)),
            user_id: String::new(),
            audio_passthrough: true,
            should_interrupt: true,
            #[cfg(feature = "cartesia-turns-eager")]
            eager_turns: false,
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

    /// Emit exact source-bound eager turns. The user aggregator and supported
    /// gated LLM must be configured for speculation separately. Partial
    /// transcriptions and raw upstream speaking proposals are suppressed.
    /// Concurrent effective eager setup/restart returns "already connecting"
    /// while a connection is reserved, rather than awaiting legacy serialization.
    #[cfg(feature = "cartesia-turns-eager")]
    pub fn with_eager_turns(mut self) -> Self {
        self.eager_turns = true;
        self
    }

    fn eager_enabled(&self) -> bool {
        #[cfg(feature = "cartesia-turns-eager")]
        {
            self.eager_turns
        }
        #[cfg(not(feature = "cartesia-turns-eager"))]
        {
            false
        }
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
    /// In eager mode a no-op during connection setup acknowledges only unchanged
    /// settings; it does not certify that the reserved socket is ready.
    pub async fn restart_with_settings(
        &self,
        delta: &SttSettings,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let setup = self.setup_gate.lock().await;
        let admission = ctx.health().generation();
        let reservation = {
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
            if self.eager_enabled() && state.connecting {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia Turns already connecting",
                )));
            }
            if ctx.configuration().audio_in_sample_rate != candidate.config().sample_rate {
                return Err(nonpermanent(StreamingError::Invalid(
                    "Cartesia Turns pipeline sample rate",
                )));
            }
            reserve_connection(&mut state, admission, Some(candidate), self.eager_enabled())?
        };
        self.connect(ctx, true, reservation, setup).await
    }

    fn cancel_current(&self, terminal: bool) -> Result<(), ProcessorError> {
        let retired = {
            let _emit = self
                .emission
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            state.ending |= terminal;
            detach(&mut state, self.eager_enabled()).map_err(nonpermanent)?
        };
        retired.retire().map_err(nonpermanent)?;
        Ok(())
    }

    fn retire_connection(&self, serial: u64) -> Result<(), ProcessorError> {
        let retired = {
            let _emit = self
                .emission
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            if state.serial == serial {
                Some(detach(&mut state, self.eager_enabled()).map_err(nonpermanent)?)
            } else {
                None
            }
        };
        if let Some(retired) = retired {
            retired.retire().map_err(nonpermanent)?;
        }
        Ok(())
    }

    fn reserve_initial(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
        admission: ProcessorHealthGeneration,
    ) -> Result<ConnectionReservation, ProcessorError> {
        let _emit = self
            .emission
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?;
        if !live_owner(&state, ctx)
            || ctx.health().generation() != admission
            || self.muted.load(Ordering::Acquire)
        {
            return Err(nonpermanent(StreamingError::Closed));
        }
        if start.audio_in_sample_rate != state.client.config().sample_rate {
            return Err(nonpermanent(StreamingError::Invalid(
                "Cartesia Turns pipeline sample rate",
            )));
        }
        if self.eager_enabled() && state.connecting {
            return Err(nonpermanent(StreamingError::Invalid(
                "Cartesia Turns already connecting",
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
        reserve_connection(&mut state, admission, None, self.eager_enabled())
    }

    fn connect<'a>(
        &'a self,
        ctx: &'a ProcessorContext,
        report_provider_error: bool,
        reservation: ConnectionReservation,
        setup: tokio::sync::MutexGuard<'a, ()>,
    ) -> impl std::future::Future<Output = Result<(), ProcessorError>> + Send + 'a {
        let mut setup = Some(setup);
        // Committed-only connections preserve their serialized setup behavior.
        // Eager ownership may notify child tasks, so its gate ends at reservation.
        if self.eager_enabled() {
            drop(setup.take());
        }
        let mut reservation_guard = ReservationGuard {
            processor: self,
            serial: reservation.serial,
            armed: true,
        };
        async move {
            // The gate and guard enter the future only after eager notification is
            // safe, including destruction before this future's first poll.
            let _setup = setup;
            let ConnectionReservation {
                serial,
                timeline,
                client,
                output,
                admission,
                cancel,
                barrier_cancel,
                retired,
            } = reservation;
            retired.retire().map_err(nonpermanent)?;
            if !ctx.is_usable()
                || self.muted.load(Ordering::Acquire)
                || !current_invocation(ctx)
                || ctx.health().generation() != admission
            {
                return Err(nonpermanent(StreamingError::Cancelled));
            }
            let mut health = ctx.health().watch();
            let fence = output.invocation_fence()?;
            #[cfg(feature = "cartesia-turns-eager")]
            let eager = if self.eager_turns {
                Some(EagerSession::from_context(&output)?)
            } else {
                None
            };
            #[cfg(feature = "cartesia-turns-eager")]
            let mut reader_guard = ReaderGuard::new(eager.clone());
            // Creating a source observes the retained owner, but it cannot resample
            // the socket admission into a newly restored health generation.
            let installed = {
                let _emit = self
                    .emission
                    .lock()
                    .map_err(|_| nonpermanent(StreamingError::Closed))?;
                let state = self
                    .state
                    .lock()
                    .map_err(|_| nonpermanent(StreamingError::Closed))?;
                let current = state.serial == serial
                    && live_owner(&state, ctx)
                    && ctx.health().generation() == admission
                    && !self.muted.load(Ordering::Acquire)
                    && !cancel.is_cancelled();
                #[cfg(feature = "cartesia-turns-eager")]
                if current {
                    let mut state = state;
                    state.eager = eager.clone();
                }
                current
            };
            if !installed {
                #[cfg(feature = "cartesia-turns-eager")]
                reader_guard.revoke();
                self.retire_connection(serial)?;
                cancel.cancel();
                return Err(nonpermanent(StreamingError::Cancelled));
            }
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
            let connection = async {
                #[cfg(feature = "cartesia-turns-eager")]
                if let Some(eager) = &eager {
                    return client
                        .connect_eager_guarded(cancel.clone(), admitted.clone(), eager.clone())
                        .await;
                }
                client
                    .connect_guarded(cancel.clone(), admitted.clone())
                    .await
            };
            let result = tokio::select! { biased;
                _ = health.changed() => Err(StreamingError::Cancelled),
                _ = retired => Err(StreamingError::Cancelled),
                result = connection => result,
            };
            let session = match result {
                Ok(session) => session,
                Err(error) => {
                    // Check before canceling our token: cancellation itself must
                    // not hide an admitted provider failure from the owner.
                    let current = admitted() && current_invocation(ctx);
                    #[cfg(feature = "cartesia-turns-eager")]
                    reader_guard.revoke();
                    cancel.cancel();
                    let _emit = self
                        .emission
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    let mut exact = false;
                    if let Ok(mut state) = self.state.lock() {
                        if state.serial == serial {
                            state.connecting = false;
                            exact = live_owner(&state, ctx)
                                && ctx.health().generation() == admission
                                && !self.muted.load(Ordering::Acquire);
                        }
                    }
                    if !current || !exact {
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
            let mut reader = SessionReader {
                #[cfg(feature = "cartesia-turns-eager")]
                guard: reader_guard,
                session,
                #[cfg(all(test, feature = "cartesia-turns-eager"))]
                ended: ReaderDropWitness {
                    eager: eager.clone(),
                    normal: false,
                },
            };
            let installed = {
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
                    false
                } else {
                    if !self.eager_enabled() {
                        state.connecting = false;
                    }
                    state.sender = Some(reader.session.sender());
                    ready.store(true, Ordering::Release);
                    true
                }
            };
            if !installed {
                self.retire_connection(serial)?;
                reader.cancel();
                return Err(nonpermanent(StreamingError::Cancelled));
            }
            let output = SessionOutput {
                ctx: output,
                fence,
                state: self.state.clone(),
                admitted,
                admission,
                user_id: self.user_id.clone(),
                #[cfg(feature = "cartesia-turns-eager")]
                eager,
            };
            let emission = self.emission.clone();
            spawn_reader(ctx, async move {
                #[cfg(feature = "cartesia-turns-eager")]
                let mut prediction = None;
                loop {
                    #[cfg(all(test, feature = "cartesia-turns-eager"))]
                    if let Some(eager) = &output.eager {
                        eager.reader_before_next(&cancel).await;
                    }
                    let event = tokio::select! { biased;
                        _ = cancel.cancelled() => { reader.cancel(); return Ok(()); },
                        _ = health.changed() => { reader.cancel(); return Ok(()); },
                        _ = tokio::time::sleep(IDLE_HEALTH_POLL) => {
                            if !(output.admitted)() || !output.fence.is_current() { reader.cancel(); return Ok(()); }
                            continue;
                        },
                        event = reader.session.next() => event,
                    };
                    let event = match event {
                        Some(Ok(event)) => event,
                        None => {
                            #[cfg(feature = "cartesia-turns-eager")]
                            reader.guard.complete_drain();
                            #[cfg(all(test, feature = "cartesia-turns-eager"))]
                            {
                                reader.ended.record_completion(reader.guard.completed);
                            }
                            return Ok(());
                        }
                        Some(Err(StreamingError::Cancelled)) => {
                            reader.cancel();
                            return Ok(());
                        }
                        Some(Err(error)) => {
                            reader.revoke();
                            if let Ok(_emit) = emission.lock() {
                                output.report(error_frame(error));
                            }
                            reader.cancel();
                            return Ok(());
                        }
                    };
                    let result = output.dispatch(
                        event,
                        &emission,
                        #[cfg(feature = "cartesia-turns-eager")]
                        &mut prediction,
                    );
                    let settle = match result {
                        Ok((true, settle)) => settle,
                        Ok((false, _)) => {
                            reader.cancel();
                            return Ok(());
                        }
                        Err(error) => {
                            reader.revoke();
                            if let Ok(_emit) = emission.lock() {
                                output.report(ErrorFrame {
                                    error: error.message,
                                    category: error.category,
                                    fatal: error.fatal,
                                    ..ErrorFrame::new("")
                                });
                            }
                            reader.cancel();
                            return Ok(());
                        }
                    };
                    if let Some(settle) = settle {
                        // External start may interrupt normal data already queued
                        // downstream. Complete the existing flush roundtrip before
                        // admitting this turn's interim/final/stop. Never hold the
                        // emission lock across this bounded wait. End releases it
                        // too, so a returning flush cannot deadlock terminal drain.
                        let acknowledged = tokio::select! { biased;
                            _ = cancel.cancelled() => { reader.cancel(); return Ok(()); },
                            _ = barrier_cancel.cancelled() => false,
                            _ = health.changed() => { reader.cancel(); return Ok(()); },
                            result = tokio::time::timeout(barrier_timeout, settle.event.wait()) => {
                                if result.is_err() {
                                    reader.revoke();
                                    if let Ok(_emit) = emission.lock() {
                                        output.report(error_frame(StreamingError::Timeout("Turns turn-start barrier")));
                                    }
                                    reader.cancel();
                                    return Ok(());
                                }
                                true
                            }
                        };
                        if !(output.admitted)() || !output.fence.is_current() {
                            reader.cancel();
                            return Ok(());
                        }
                        #[cfg(feature = "cartesia-turns-eager")]
                        if let (Some(eager), Some(turn)) = (&output.eager, &settle.turn) {
                            let ready = if acknowledged {
                                let _emit = emission
                                    .lock()
                                    .map_err(|_| nonpermanent(StreamingError::Closed))?;
                                output.open()? && eager.mark_ready(turn)
                            } else {
                                false
                            };
                            if !ready {
                                // Cancellation is not a completed Start roundtrip.
                                eager.prepare_end();
                            }
                        }
                        #[cfg(not(feature = "cartesia-turns-eager"))]
                        let _ = acknowledged;
                    }
                }
            })?;
            if self.eager_enabled() {
                let current = {
                    let _emit = self
                        .emission
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    if state.serial == serial
                        && !state.ending
                        && state.owner.as_ref() == Some(&ctx.processor_identity())
                        && ctx.is_usable()
                        && ctx.health().generation() == admission
                        && !self.muted.load(Ordering::Acquire)
                        && state
                            .sender
                            .as_ref()
                            .is_some_and(|sender| !sender.is_closed())
                    {
                        // Socket readiness already admitted the retained lifecycle
                        // owner; an ordinary Start interruption cannot revoke it.
                        state.connecting = false;
                        true
                    } else {
                        false
                    }
                };
                if !current {
                    return Err(nonpermanent(StreamingError::Cancelled));
                }
            }
            reservation_guard.disarm();
            Ok(())
        }
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

// The guard precedes the stream so unwinding revokes source rights before
// EventStream's mechanical cancellation, including an abandoned reader future.
struct SessionReader {
    #[cfg(feature = "cartesia-turns-eager")]
    guard: ReaderGuard,
    session: crate::cartesia_turns::CartesiaTurnsSession,
    // Rust drops fields in declaration order. This witness observes the real
    // ended EventStream Drop, never substitutes for its production lifecycle.
    #[cfg(all(test, feature = "cartesia-turns-eager"))]
    ended: ReaderDropWitness,
}

#[cfg(all(test, feature = "cartesia-turns-eager"))]
struct ReaderDropWitness {
    eager: Option<Arc<EagerSession>>,
    normal: bool,
}
#[cfg(all(test, feature = "cartesia-turns-eager"))]
impl ReaderDropWitness {
    fn record_completion(&mut self, normal: bool) {
        self.normal = normal;
    }
}
#[cfg(all(test, feature = "cartesia-turns-eager"))]
impl Drop for ReaderDropWitness {
    fn drop(&mut self) {
        if let Some(eager) = &self.eager {
            eager.reader_dropped(self.normal);
        }
    }
}

impl SessionReader {
    fn revoke(&mut self) {
        #[cfg(feature = "cartesia-turns-eager")]
        self.guard.revoke();
    }

    fn cancel(&mut self) {
        self.revoke();
        self.session.cancel();
    }
}

#[cfg(feature = "cartesia-turns-eager")]
struct ReaderGuard {
    eager: Option<Arc<EagerSession>>,
    completed: bool,
}

#[cfg(feature = "cartesia-turns-eager")]
impl ReaderGuard {
    fn new(eager: Option<Arc<EagerSession>>) -> Self {
        Self {
            eager,
            completed: false,
        }
    }

    fn revoke(&mut self) {
        if let Some(eager) = &self.eager {
            eager.revoke();
        }
    }

    fn complete_drain(&mut self) {
        if let Some(eager) = &self.eager {
            self.completed = eager.complete_drain();
        }
    }
}

#[cfg(feature = "cartesia-turns-eager")]
impl Drop for ReaderGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.revoke();
        }
    }
}

fn spawn_reader<F>(ctx: &ProcessorContext, future: F) -> Result<(), ProcessorError>
where
    F: std::future::Future<Output = Result<(), ProcessorError>> + Send + 'static,
{
    // Keep the caller's Arc until spawn returns: a rejected runtime admission
    // may drop the trampoline under its own lock, never the owning reader.
    let work = Arc::new(Mutex::new(Some(future)));
    let worker = work.clone();
    let result = ctx.spawn("cartesia-turns-stt", false, async move {
        let future = worker
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?
            .take();
        match future {
            Some(future) => future.await,
            None => Ok(()),
        }
    });
    drop(work);
    result.map(|_| ())
}

struct StartBarrier {
    event: pipecat_core::LocalFlushEvent,
    #[cfg(feature = "cartesia-turns-eager")]
    turn: Option<pipecat_core::NativeEagerTurn>,
}

struct SessionOutput {
    ctx: ProcessorContext,
    fence: InvocationFence,
    state: Arc<Mutex<State>>,
    admitted: Arc<dyn Fn() -> bool + Send + Sync>,
    admission: ProcessorHealthGeneration,
    user_id: String,
    #[cfg(feature = "cartesia-turns-eager")]
    eager: Option<Arc<EagerSession>>,
}

impl SessionOutput {
    fn dispatch(
        &self,
        event: CartesiaTurnsEvent,
        emission: &Mutex<()>,
        #[cfg(feature = "cartesia-turns-eager")] prediction: &mut Option<
            pipecat_core::NativeEagerPrediction,
        >,
    ) -> Result<(bool, Option<StartBarrier>), ProcessorError> {
        #[cfg(feature = "cartesia-turns-eager")]
        if let Some(eager) = &self.eager {
            return self.eager_event(eager, event, emission, prediction);
        }
        let _emit = emission
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?;
        if !(self.admitted)() || !self.fence.is_current() {
            return Ok((false, None));
        }
        if matches!(event, CartesiaTurnsEvent::Start { .. }) {
            self.start().map(|(current, event)| {
                (
                    current,
                    event.map(|event| StartBarrier {
                        event,
                        #[cfg(feature = "cartesia-turns-eager")]
                        turn: None,
                    }),
                )
            })
        } else {
            self.event(event).map(|current| (current, None))
        }
    }

    #[cfg(feature = "cartesia-turns-eager")]
    fn open(&self) -> Result<bool, ProcessorError> {
        Ok((self.admitted)()
            && self.fence.is_current()
            && !self
                .state
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?
                .ending)
    }

    #[cfg(feature = "cartesia-turns-eager")]
    fn eager_event(
        &self,
        eager: &EagerSession,
        event: CartesiaTurnsEvent,
        emission: &Mutex<()>,
        prediction: &mut Option<pipecat_core::NativeEagerPrediction>,
    ) -> Result<(bool, Option<StartBarrier>), ProcessorError> {
        use pipecat_core::NativeEagerSource;

        if !(self.admitted)() || !self.fence.is_current() {
            return Ok((false, None));
        }
        match event {
            CartesiaTurnsEvent::Start { .. } => {
                let claimed = {
                    let _emit = emission
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    self.open()? && eager.claim_start()
                };
                if !claimed {
                    eager.skipped_start();
                    return Ok(((self.admitted)(), None));
                }
                // Supersession and child cancellation can notify downstream.
                // Never perform them under the adapter's emission/state locks.
                let Some(turn) = eager.begin_start()? else {
                    return Ok(((self.admitted)(), None));
                };
                *prediction = None;
                let mut frame = Frame::new(FrameData::ProposedUserStartedSpeaking);
                frame.set_native_eager_source(NativeEagerSource::Start(turn.clone()))?;
                let settled = pipecat_core::LocalFlushEvent::default();
                let current = {
                    let _emit = emission
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    if !self.open()? || !eager.accepts_start(&turn) {
                        None
                    } else if !self.push(frame, Direction::Downstream)? {
                        Some(false)
                    } else {
                        Some(self.push(
                            Frame::new(FrameData::FlushDetailed(
                                pipecat_core::PipelineFlushFrame {
                                    event: Some(settled.clone()),
                                    returning: false,
                                    origin: Some("pipecat.cartesia-turns.turn-start".into()),
                                },
                            )),
                            Direction::Downstream,
                        )?)
                    }
                };
                match current {
                    Some(true) => Ok((
                        true,
                        Some(StartBarrier {
                            event: settled,
                            turn: Some(turn),
                        }),
                    )),
                    Some(false) => Ok((false, None)),
                    None => {
                        eager.skipped_start();
                        Ok(((self.admitted)(), None))
                    }
                }
            }
            CartesiaTurnsEvent::Update { .. } => Ok(((self.admitted)(), None)),
            CartesiaTurnsEvent::EagerEnd { transcript, result } => {
                let turn = {
                    let _emit = emission
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    if self.open()? {
                        eager.ready_turn()
                    } else {
                        None
                    }
                };
                let Some(turn) = turn else {
                    return Ok(((self.admitted)(), None));
                };
                if transcript.trim().is_empty() {
                    return self.eager_resume(eager, emission, prediction);
                }
                let mut payload = self.transcription(transcript, result, false).data;
                if let FrameData::InterimTranscription(transcript) = payload {
                    payload = FrameData::EagerTranscription(transcript);
                }
                let Some(next) = eager.predict(&turn, payload)? else {
                    return Ok(((self.admitted)(), None));
                };
                let mut frame = Frame::new(next.payload().clone());
                frame.set_native_eager_source(NativeEagerSource::Prediction(next.clone()))?;
                let published = {
                    let _emit = emission
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    if self.open()? && eager.accepts_prediction(&turn) {
                        Some(self.push(frame, Direction::Downstream)?)
                    } else {
                        None
                    }
                };
                match published {
                    Some(true) => {
                        *prediction = Some(next);
                        Ok((true, None))
                    }
                    Some(false) => Ok((false, None)),
                    None => {
                        eager.resume(&next);
                        *prediction = None;
                        Ok(((self.admitted)(), None))
                    }
                }
            }
            CartesiaTurnsEvent::Resume { .. } => self.eager_resume(eager, emission, prediction),
            CartesiaTurnsEvent::End {
                transcript,
                result,
                audio_seconds,
            } => {
                let turn = {
                    let _emit = emission
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    eager.ready_turn()
                };
                let Some(turn) = turn else {
                    return Ok(((self.admitted)(), None));
                };
                if transcript.is_empty() {
                    if !eager.offer_empty_stop(&turn)? {
                        return Ok(((self.admitted)(), None));
                    }
                    *prediction = None;
                    let mut stop = Frame::new(FrameData::ProposedUserStoppedSpeaking);
                    stop.set_native_eager_source(NativeEagerSource::Stop {
                        turn: turn.clone(),
                        commit: None,
                    })?;
                    let _emit = emission
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    let current = if eager.accepts_empty_stop(&turn) {
                        self.push(stop, Direction::Downstream)?
                    } else {
                        (self.admitted)()
                    };
                    return Ok((current, None));
                }
                let payload = self.transcription(transcript, result, true).data;
                let Some(commit) = eager.offer_final(&turn, payload)? else {
                    return Ok(((self.admitted)(), None));
                };
                *prediction = None;
                let mut final_frame = Frame::new(commit.payload().clone());
                final_frame.set_native_eager_source(NativeEagerSource::Final(commit.clone()))?;
                let mut stop = Frame::new(FrameData::ProposedUserStoppedSpeaking);
                stop.set_native_eager_source(NativeEagerSource::Stop {
                    turn,
                    commit: Some(commit.clone()),
                })?;
                let current = {
                    let _emit = emission
                        .lock()
                        .map_err(|_| nonpermanent(StreamingError::Closed))?;
                    if !eager.accepts_final(&commit) {
                        (self.admitted)()
                    } else if !self.usage(audio_seconds)? {
                        false
                    } else if !eager.accepts_final(&commit) {
                        (self.admitted)()
                    } else if !self.push(final_frame, Direction::Downstream)? {
                        false
                    } else if !eager.accepts_final(&commit) {
                        (self.admitted)()
                    } else {
                        self.push(stop, Direction::Downstream)?
                    }
                };
                // No reader-only commit clone survives into EOF completion.
                drop(commit);
                Ok((current, None))
            }
            CartesiaTurnsEvent::Usage { audio_seconds } => {
                let _emit = emission
                    .lock()
                    .map_err(|_| nonpermanent(StreamingError::Closed))?;
                self.usage(audio_seconds).map(|current| (current, None))
            }
            CartesiaTurnsEvent::Connected { .. } => Err(nonpermanent(StreamingError::Invalid(
                "duplicate Cartesia Turns connected event",
            ))),
        }
    }

    #[cfg(feature = "cartesia-turns-eager")]
    fn eager_resume(
        &self,
        eager: &EagerSession,
        emission: &Mutex<()>,
        prediction: &mut Option<pipecat_core::NativeEagerPrediction>,
    ) -> Result<(bool, Option<StartBarrier>), ProcessorError> {
        let current = {
            let _emit = emission
                .lock()
                .map_err(|_| nonpermanent(StreamingError::Closed))?;
            self.open()?
                && prediction
                    .as_ref()
                    .is_some_and(|prediction| eager.accepts_prediction(prediction.turn()))
        };
        if !current {
            return Ok(((self.admitted)(), None));
        }
        let Some(previous) = prediction.take() else {
            return Ok(((self.admitted)(), None));
        };
        if !eager.resume(&previous) {
            return Ok(((self.admitted)(), None));
        }
        let mut frame = Frame::new(FrameData::EagerEndOfTurnCancel);
        frame.set_native_eager_source(pipecat_core::NativeEagerSource::Cancel(previous.clone()))?;
        let _emit = emission
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?;
        let current = if self.open()? && eager.accepts_prediction(previous.turn()) {
            self.push(frame, Direction::Downstream)?
        } else {
            (self.admitted)()
        };
        Ok((current, None))
    }

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

struct DetachedSession {
    exhausted: bool,
    retired: bool,
    cancel: Option<CancellationToken>,
    barrier_cancel: Option<CancellationToken>,
    sender: Option<CartesiaTurnsSender>,
    #[cfg(feature = "cartesia-turns-eager")]
    eager: Option<Arc<EagerSession>>,
}

struct ConnectionReservation {
    serial: u64,
    timeline: Arc<AtomicU64>,
    client: CartesiaTurnsClient,
    output: ProcessorContext,
    admission: ProcessorHealthGeneration,
    cancel: CancellationToken,
    barrier_cancel: CancellationToken,
    retired: DetachedSession,
}

fn reserve_connection(
    state: &mut State,
    admission: ProcessorHealthGeneration,
    replacement: Option<CartesiaTurnsClient>,
    eager: bool,
) -> Result<ConnectionReservation, ProcessorError> {
    let output = state
        .owner_context
        .clone()
        .ok_or_else(|| nonpermanent(StreamingError::Closed))?;
    let cancel = CancellationToken::new();
    let barrier_cancel = CancellationToken::new();
    let retired = detach(state, eager).map_err(nonpermanent)?;
    if let Some(client) = replacement {
        state.client = client;
    }
    state.cancel = Some(cancel.clone());
    state.barrier_cancel = Some(barrier_cancel.clone());
    state.admission = Some(admission);
    state.connecting = true;
    Ok(ConnectionReservation {
        serial: state.serial,
        timeline: state.timeline.clone(),
        client: state.client.clone(),
        output,
        admission,
        cancel,
        barrier_cancel,
        retired,
    })
}

struct ReservationGuard<'a> {
    processor: &'a CartesiaTurnsProcessor,
    serial: u64,
    armed: bool,
}

impl ReservationGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReservationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.processor.retire_connection(self.serial);
        }
    }
}

struct PreparingEnd {
    sender: Option<CartesiaTurnsSender>,
    barrier_cancel: Option<CancellationToken>,
    retired: Option<DetachedSession>,
    #[cfg(feature = "cartesia-turns-eager")]
    eager: Option<Arc<EagerSession>>,
}

impl DetachedSession {
    fn retire(mut self) -> Result<(), StreamingError> {
        self.teardown();
        if self.exhausted {
            Err(StreamingError::TooLarge)
        } else {
            Ok(())
        }
    }

    fn teardown(&mut self) {
        if self.retired {
            return;
        }
        self.retired = true;
        // Detachment fences new adapter output. Source revoke is the actual
        // authority cutoff and must precede every adapter-owned notification.
        #[cfg(feature = "cartesia-turns-eager")]
        if let Some(eager) = &self.eager {
            eager.revoke();
        }
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        if let Some(cancel) = &self.barrier_cancel {
            cancel.cancel();
        }
        if let Some(sender) = &self.sender {
            sender.cancel();
        }
    }
}

impl Drop for DetachedSession {
    fn drop(&mut self) {
        self.teardown();
    }
}

fn detach(state: &mut State, eager: bool) -> Result<DetachedSession, StreamingError> {
    let exhausted = match state.serial.checked_add(1) {
        Some(serial) => {
            state.serial = serial;
            state.timeline.store(serial, Ordering::Release);
            false
        }
        None if !eager => return Err(StreamingError::TooLarge),
        None => {
            // Exhaustion is terminal, but still detach and revoke the current
            // socket before returning an error to its caller.
            state.ending = true;
            state.timeline.store(0, Ordering::Release);
            true
        }
    };
    state.connecting = false;
    state.admission = None;
    Ok(DetachedSession {
        exhausted,
        retired: false,
        cancel: state.cancel.take(),
        barrier_cancel: state.barrier_cancel.take(),
        sender: state.sender.take(),
        #[cfg(feature = "cartesia-turns-eager")]
        eager: state.eager.take(),
    })
}

fn nonpermanent(error: StreamingError) -> ProcessorError {
    processor_error(error).with_permanence_override(false)
}

impl Drop for CartesiaTurnsProcessor {
    fn drop(&mut self) {
        let (retired, context) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.ending = true;
            (
                detach(&mut state, self.eager_enabled()).ok(),
                state.owner_context.take(),
            )
        };
        if let Some(retired) = retired {
            let _ = retired.retire();
        }
        drop(context);
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
        let setup = self.setup_gate.lock().await;
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
        let result = match self.reserve_initial(start, ctx, admission) {
            Ok(reservation) => self.connect(ctx, false, reservation, setup).await,
            Err(error) => Err(error),
        };
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
        let action = {
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
            let barrier_cancel = state.barrier_cancel.clone();
            #[cfg(feature = "cartesia-turns-eager")]
            let eager = state.eager.clone();
            if state.connecting
                || state.admission != Some(ctx.health().generation())
                || !ctx.is_usable()
            {
                PreparingEnd {
                    sender: None,
                    barrier_cancel,
                    retired: Some(detach(&mut state, self.eager_enabled()).map_err(nonpermanent)?),
                    #[cfg(feature = "cartesia-turns-eager")]
                    eager,
                }
            } else {
                PreparingEnd {
                    sender: state.sender.clone(),
                    barrier_cancel,
                    retired: None,
                    #[cfg(feature = "cartesia-turns-eager")]
                    eager,
                }
            }
        };
        if let Some(retired) = action.retired {
            retired.retire().map_err(nonpermanent)?;
        } else {
            #[cfg(feature = "cartesia-turns-eager")]
            if let Some(eager) = &action.eager {
                eager.prepare_end();
            }
        }
        if let Some(cancel) = action.barrier_cancel {
            cancel.cancel();
        }
        if let Some(sender) = action.sender {
            if !sender.is_closed() {
                if let Err(error) = sender.finish().await {
                    #[cfg(feature = "cartesia-turns-eager")]
                    if let Some(eager) = &action.eager {
                        eager.revoke();
                    }
                    sender.cancel();
                    return Err(nonpermanent(error));
                }
            }
        }
        // The runtime drains the tracked reader before forwarding End/Stop.
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cancel_current(true)?;
        let context = self
            .state
            .lock()
            .map_err(|_| nonpermanent(StreamingError::Closed))?
            .owner_context
            .take();
        drop(context);
        Ok(())
    }
}

#[cfg(all(test, feature = "cartesia-turns-eager"))]
mod eager_reservation_tests {
    use super::*;
    use pipecat_core::{NativeEagerConsumer, Pipeline, PipelineWorker, SpeculationOwner};
    use std::{
        future::Future,
        task::{Context, Wake, Waker},
    };

    struct Capture {
        health: ProcessorHealth,
        context: Arc<Mutex<Option<ProcessorContext>>>,
    }

    #[async_trait]
    impl FrameProcessor for Capture {
        fn name(&self) -> &str {
            "CartesiaReservationFixture"
        }
        fn health(&self) -> Option<ProcessorHealth> {
            Some(self.health.clone())
        }
        async fn setup(
            &self,
            _: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            *self.context.lock().unwrap() = Some(ctx.clone());
            Ok(())
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            ctx.push(frame, direction).await
        }
    }

    async fn fixture(
        eager: bool,
    ) -> (
        Arc<CartesiaTurnsProcessor>,
        PipelineWorker,
        ProcessorContext,
    ) {
        let client =
            CartesiaTurnsClient::new(CartesiaTurnsConfig::new("reservation-fixture")).unwrap();
        let processor = CartesiaTurnsProcessor::new(client);
        let processor = Arc::new(if eager {
            processor.with_eager_turns()
        } else {
            processor
        });
        let context = Arc::new(Mutex::new(None));
        let worker = Pipeline::new(vec![Arc::new(Capture {
            health: processor.health.clone(),
            context: context.clone(),
        })])
        .start(Default::default())
        .await
        .unwrap();
        let context = context.lock().unwrap().take().unwrap();
        {
            let mut state = processor.state.lock().unwrap();
            state.owner = Some(context.processor_identity());
            state.owner_context = Some(context.clone());
        }
        (processor, worker, context)
    }

    async fn finish(mut worker: PipelineWorker) {
        worker.cancel("reservation fixture complete").await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), worker.join())
            .await
            .unwrap()
            .unwrap();
    }

    struct Reenter {
        processor: std::sync::Weak<CartesiaTurnsProcessor>,
        wakes: AtomicU64,
        all_unlocked: AtomicBool,
    }

    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::AcqRel);
            let Some(processor) = self.processor.upgrade() else {
                return;
            };
            let unlocked = processor.setup_gate.try_lock().is_ok()
                && processor.emission.try_lock().is_ok()
                && processor.state.try_lock().is_ok();
            if !unlocked || processor.config_snapshot().is_err() {
                self.all_unlocked.store(false, Ordering::Release);
            }
        }
    }

    #[tokio::test]
    async fn reservation_retirement_wakes_source_children_outside_adapter_locks() {
        let (processor, worker, ctx) = fixture(true).await;
        let eager = EagerSession::from_context(&ctx).unwrap();
        assert!(eager.claim_start());
        let turn = eager.begin_start().unwrap().unwrap();
        assert!(eager.mark_ready(&turn));
        let consumer = NativeEagerConsumer::new();
        assert!(turn.claim_start(&consumer));
        let prediction = eager
            .predict(
                &turn,
                FrameData::EagerTranscription(TranscriptionFrame {
                    text: "a held prediction".into(),
                    user_id: "fixture".into(),
                    timestamp: "one timestamp".into(),
                    language: Some("en".into()),
                    result: None,
                    finalized: false,
                }),
            )
            .unwrap()
            .unwrap();
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        let capability = owner.capability();
        processor.state.lock().unwrap().eager = Some(eager);
        let probe = Arc::new(Reenter {
            processor: Arc::downgrade(&processor),
            wakes: AtomicU64::new(0),
            all_unlocked: AtomicBool::new(true),
        });
        let waker = Waker::from(probe.clone());
        let mut watch = capability.watch();
        let mut changed = Box::pin(watch.changed());
        assert!(changed
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        let setup = processor.setup_gate.lock().await;
        let reservation = processor
            .reserve_initial(ctx.configuration(), &ctx, ctx.health().generation())
            .unwrap();
        // Connect retires the old exact source before this admission rejection;
        // no socket is opened by this fixture.
        processor.muted.store(true, Ordering::Release);
        let result = processor.connect(&ctx, false, reservation, setup).await;
        let cleared = !processor.state.lock().unwrap().connecting;
        let woke = probe.wakes.load(Ordering::Acquire) > 0;
        let unlocked = probe.all_unlocked.load(Ordering::Acquire);
        let canceled = !capability.admits_completion();
        drop(changed);
        finish(worker).await;
        assert!(result.is_err());
        assert!(cleared && canceled);
        assert!(woke && unlocked, "source wake reentered an adapter lock");
    }

    #[tokio::test]
    async fn unpolled_eager_connection_releases_its_exact_reservation() {
        let (processor, worker, ctx) = fixture(true).await;
        let setup = processor.setup_gate.lock().await;
        let reservation = processor
            .reserve_initial(ctx.configuration(), &ctx, ctx.health().generation())
            .unwrap();
        let cancel = reservation.cancel.clone();
        let connecting = processor.connect(&ctx, false, reservation, setup);
        let gate_released = processor.setup_gate.try_lock().is_ok();
        drop(connecting);
        let cleared = {
            let state = processor.state.lock().unwrap();
            !state.connecting && state.cancel.is_none()
        };
        finish(worker).await;
        assert!(gate_released && cleared && cancel.is_cancelled());
    }

    #[tokio::test]
    async fn stale_reservation_drop_cannot_cancel_a_replacement() {
        let (processor, worker, ctx) = fixture(true).await;
        let setup = processor.setup_gate.lock().await;
        let first = processor
            .reserve_initial(ctx.configuration(), &ctx, ctx.health().generation())
            .unwrap();
        let first = processor.connect(&ctx, false, first, setup);
        processor.cancel_current(false).unwrap();
        let setup = processor.setup_gate.lock().await;
        let second = processor
            .reserve_initial(ctx.configuration(), &ctx, ctx.health().generation())
            .unwrap();
        let second_serial = second.serial;
        let cancel = second.cancel.clone();
        let second = processor.connect(&ctx, false, second, setup);
        drop(first);
        let preserved = {
            let state = processor.state.lock().unwrap();
            state.connecting && state.serial == second_serial && !cancel.is_cancelled()
        };
        drop(second);
        finish(worker).await;
        assert!(preserved);
    }

    #[tokio::test]
    async fn busy_eager_setup_and_restart_preserve_the_reserved_candidate() {
        let (processor, worker, ctx) = fixture(true).await;
        let setup = processor.setup_gate.lock().await;
        let reservation = processor
            .reserve_initial(ctx.configuration(), &ctx, ctx.health().generation())
            .unwrap();
        let serial = reservation.serial;
        let cancel = reservation.cancel.clone();
        let connecting = processor.connect(&ctx, false, reservation, setup);
        let setup_result = processor.setup(ctx.configuration(), &ctx).await;
        let before = processor.config_snapshot().unwrap().settings;
        let noop_result = processor
            .restart_with_settings(&SttSettings::default(), &ctx)
            .await;
        let mut invalid = SttSettings::default();
        invalid
            .base
            .extra
            .insert("turn_start_threshold".into(), serde_json::json!(-1));
        let invalid_result = processor.restart_with_settings(&invalid, &ctx).await;
        let mut delta = SttSettings::default();
        delta
            .base
            .extra
            .insert("keyterm".into(), serde_json::json!(["a new keyterm"]));
        let restart_result = processor.restart_with_settings(&delta, &ctx).await;
        let unchanged = before == processor.config_snapshot().unwrap().settings;
        let preserved = {
            let state = processor.state.lock().unwrap();
            state.connecting && state.serial == serial && !cancel.is_cancelled()
        };
        drop(connecting);
        finish(worker).await;
        assert!(setup_result.is_err() && restart_result.is_err() && invalid_result.is_err());
        assert!(noop_result.is_ok());
        assert!(unchanged && preserved);
    }

    #[tokio::test]
    async fn runtime_disabled_connection_keeps_setup_serialization() {
        let (processor, worker, ctx) = fixture(false).await;
        let setup = processor.setup_gate.lock().await;
        let reservation = processor
            .reserve_initial(ctx.configuration(), &ctx, ctx.health().generation())
            .unwrap();
        let connecting = processor.connect(&ctx, false, reservation, setup);
        let serialized = processor.setup_gate.try_lock().is_err();
        drop(connecting);
        let released = processor.setup_gate.try_lock().is_ok();
        finish(worker).await;
        assert!(serialized && released);
    }
    include!("reviewer_reservations.rs");
}

#[cfg(all(test, feature = "cartesia-turns-eager"))]
mod eager_reader_pipeline_tests {
    use super::{CartesiaTurnsClient, CartesiaTurnsConfig, CartesiaTurnsProcessor, EagerSession};
    use futures_util::{SinkExt, StreamExt};
    use pipecat_core::*;
    use pipecat_services::{
        ClientConfig, CompletionRequest, Content, Context, LlmClient, LlmProcessor, Provider, Role,
    };
    use pipecat_turns::{
        EagerMatchPolicy, EagerUserTurnConfig, LLMContextAggregatorPair, LLMUserAggregator,
        UserAggregatorConfig, DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT,
    };
    use serde_json::{json, Value};
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{mpsc, oneshot},
        task::{JoinHandle, JoinSet},
    };
    use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

    const TRANSCRIPT: &str = "the genuine queued provider turn";
    const ANSWER: &str = "The genuine HTTP answer.";

    async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .expect("reader pipeline fixture deadline")
    }

    async fn wait(mut predicate: impl FnMut() -> bool) {
        within(async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }

    fn answer() -> String {
        format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({
                "choices": [{"delta": {"content": ANSWER}, "finish_reason": "stop"}]
            })
        )
    }

    async fn request_body(socket: &mut TcpStream) -> Value {
        let mut bytes = Vec::new();
        let mut buffer = [0; 4096];
        let header_end = loop {
            let read = socket.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0, "HTTP request ended before its headers");
            bytes.extend_from_slice(&buffer[..read]);
            if let Some(offset) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                break offset + 4;
            }
        };
        let length: usize = String::from_utf8_lossy(&bytes[..header_end])
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|value| value.trim().parse().unwrap())
            })
            .expect("HTTP request content length");
        while bytes.len() < header_end + length {
            let read = socket.read(&mut buffer).await.unwrap();
            assert_ne!(read, 0, "HTTP request ended before its body");
            bytes.extend_from_slice(&buffer[..read]);
        }
        serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
    }

    /// Every request except an explicitly held first one is answered, including
    /// forbidden mutant requests. Failure must be a post-join count assertion.
    struct HttpPeer {
        endpoint: String,
        count: Arc<AtomicUsize>,
        bodies: Arc<Mutex<Vec<Value>>>,
        held: mpsc::Receiver<oneshot::Sender<()>>,
        stop: Option<oneshot::Sender<()>>,
        task: Option<JoinHandle<()>>,
    }

    impl HttpPeer {
        async fn new(hold_first: bool) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
            let count = Arc::new(AtomicUsize::new(0));
            let counter = count.clone();
            let bodies = Arc::new(Mutex::new(Vec::new()));
            let requests = bodies.clone();
            let (held_tx, held) = mpsc::channel(1);
            let (stop_tx, mut stop_rx) = oneshot::channel();
            let task = tokio::spawn(async move {
                let mut children = JoinSet::new();
                loop {
                    tokio::select! {
                        _ = &mut stop_rx => break,
                        accepted = listener.accept() => {
                            let (mut socket, _) = accepted.unwrap();
                            let counter = counter.clone();
                            let requests = requests.clone();
                            let held_tx = held_tx.clone();
                            children.spawn(async move {
                                let body = request_body(&mut socket).await;
                                requests.lock().unwrap().push(body);
                                let index = counter.fetch_add(1, Ordering::SeqCst);
                                if hold_first && index == 0 {
                                    let (release, released) = oneshot::channel();
                                    held_tx.send(release).await.unwrap();
                                    let _ = released.await;
                                }
                                let body = answer();
                                let response = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len(),
                                );
                                // Retirement may close a speculative request.
                                let _ = socket.write_all(response.as_bytes()).await;
                            });
                        }
                        result = children.join_next(), if !children.is_empty() => {
                            result.unwrap().unwrap();
                        }
                    }
                }
                while let Some(result) = children.join_next().await {
                    result.unwrap();
                }
            });
            Self {
                endpoint,
                count,
                bodies,
                held,
                stop: Some(stop_tx),
                task: Some(task),
            }
        }

        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }

        async fn finish(mut self) -> Vec<Value> {
            let _ = self.stop.take().unwrap().send(());
            within(self.task.take().unwrap()).await.unwrap();
            let bodies = self.bodies.lock().unwrap().clone();
            bodies
        }
    }

    impl Drop for HttpPeer {
        fn drop(&mut self) {
            if let Some(task) = &self.task {
                task.abort();
            }
        }
    }

    struct Recorder {
        name: &'static str,
        frames: Mutex<Vec<DirectedFrame>>,
        hold_prediction: AtomicBool,
        hold_receipts: AtomicBool,
    }

    impl Recorder {
        fn new(name: &'static str, hold_prediction: bool, hold_receipts: bool) -> Self {
            Self {
                name,
                frames: Mutex::new(Vec::new()),
                hold_prediction: AtomicBool::new(hold_prediction),
                hold_receipts: AtomicBool::new(hold_receipts),
            }
        }

        fn native(&self, predicate: impl Fn(&NativeEagerSource) -> bool) -> Option<Frame> {
            self.frames
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find_map(|directed| {
                    (directed.direction == Direction::Downstream)
                        .then(|| directed.frame.native_eager_source())
                        .flatten()
                        .filter(|source| predicate(source))
                        .map(|_| directed.frame.clone())
                })
        }

        fn observed(&self, id: u64) -> bool {
            self.frames.lock().unwrap().iter().any(|directed| {
                directed.direction == Direction::Downstream && directed.frame.id == id
            })
        }

        fn count(&self, predicate: impl Fn(&FrameData) -> bool) -> usize {
            self.frames
                .lock()
                .unwrap()
                .iter()
                .filter(|directed| {
                    directed.direction == Direction::Downstream && predicate(&directed.frame.data)
                })
                .count()
        }

        fn capability(&self) -> Option<NativeSpeculation> {
            self.frames
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find_map(|directed| {
                    matches!(directed.frame.data, FrameData::LlmContextDetailed(_))
                        .then(|| directed.frame.native_speculation().cloned())
                        .flatten()
                })
        }
    }

    #[async_trait]
    impl FrameProcessor for Recorder {
        fn name(&self) -> &str {
            self.name
        }

        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.frames
                .lock()
                .unwrap()
                .push(DirectedFrame::new(frame.clone(), direction));
            if direction == Direction::Downstream {
                let hold = match frame.native_eager_source() {
                    Some(NativeEagerSource::Prediction(_)) => {
                        self.hold_prediction.load(Ordering::SeqCst)
                    }
                    Some(NativeEagerSource::Final(_) | NativeEagerSource::Stop { .. }) => {
                        self.hold_receipts.load(Ordering::SeqCst)
                    }
                    _ => false,
                };
                if hold {
                    return Ok(());
                }
            }
            ctx.push(frame, direction).await
        }
    }

    /// Captures the actual installed processor's owner, not a synthetic source.
    struct Owner {
        processor: Arc<CartesiaTurnsProcessor>,
        context: Mutex<Option<ProcessorContext>>,
    }

    #[async_trait]
    impl FrameProcessor for Owner {
        fn name(&self) -> &str {
            self.processor.name()
        }
        fn health(&self) -> Option<ProcessorHealth> {
            self.processor.health()
        }

        async fn setup(
            &self,
            start: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            *self.context.lock().unwrap() = Some(ctx.clone());
            self.processor.setup(start, ctx).await
        }

        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.processor.process(frame, direction, ctx).await
        }

        async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
            self.processor.prepare_end(ctx).await
        }

        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.processor.cleanup().await
        }
    }

    /// Uses the actual eager aggregator and its real state-machine clock.
    struct ClockedUser {
        user: Arc<LLMUserAggregator>,
        applied: Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl FrameProcessor for ClockedUser {
        fn name(&self) -> &str {
            self.user.name()
        }

        async fn setup(
            &self,
            start: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.user.setup(start, ctx).await
        }

        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            let id = frame.id;
            if direction == Direction::Downstream
                && frame.metadata.contains_key("reader-pipeline-advance")
            {
                let millis = frame.metadata["reader-pipeline-advance"].as_u64().unwrap();
                self.user
                    .advance(Duration::from_millis(millis))?
                    .emit(ctx)
                    .await?;
                ctx.push(frame, direction).await?;
            } else if direction == Direction::Downstream
                && frame.metadata.contains_key("reader-pipeline-time")
            {
                let millis = frame.metadata["reader-pipeline-time"].as_u64().unwrap();
                self.user
                    .process_at(frame, direction, Duration::from_millis(millis))?
                    .emit(ctx)
                    .await?;
            } else {
                self.user.process(frame, direction, ctx).await?;
            }
            self.applied.lock().unwrap().push(id);
            Ok(())
        }

        async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
            self.user.prepare_end(ctx).await
        }

        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.user.cleanup().await
        }
    }

    struct Harness {
        owner: Arc<Owner>,
        source: Arc<EagerSession>,
        before: Arc<Recorder>,
        after: Arc<Recorder>,
        output: Arc<Recorder>,
        settled: Arc<Recorder>,
        user: Arc<ClockedUser>,
        pair: LLMContextAggregatorPair,
        llm: Arc<LlmProcessor>,
        worker: PipelineWorker,
        peer: WebSocketStream<TcpStream>,
        listener: TcpListener,
    }

    impl Harness {
        async fn new(http: &HttpPeer, hold_prediction: bool, hold_receipts: bool) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut config = CartesiaTurnsConfig::new("reader-pipeline-loopback-key");
            config.endpoint = format!("ws://{}/turns", listener.local_addr().unwrap());
            config.limits.queue_capacity = 1;
            config.limits.connect_timeout = Duration::from_secs(2);
            config.limits.close_timeout = Duration::from_secs(2);
            let processor = CartesiaTurnsProcessor::new(CartesiaTurnsClient::new(config).unwrap())
                .with_user_id("provider-speaker")
                .unwrap()
                .with_should_interrupt(false)
                .with_eager_turns();
            let owner = Arc::new(Owner {
                processor: Arc::new(processor),
                context: Mutex::new(None),
            });
            let before = Arc::new(Recorder::new(
                "ReaderPipelineBeforeAdoption",
                hold_prediction,
                hold_receipts,
            ));
            let after = Arc::new(Recorder::new("ReaderPipelineAfterAdoption", false, false));
            let output = Arc::new(Recorder::new("ReaderPipelineOutput", false, false));
            let settled = Arc::new(Recorder::new("ReaderPipelineSettled", false, false));
            let pair = LLMContextAggregatorPair::new(
                Context::default(),
                UserAggregatorConfig {
                    // Keep the eager-expiry timer later than the 5s controller
                    // watchdog, so deadline() cannot mistake it for non-speaking.
                    eager: Some(
                        EagerUserTurnConfig::new(
                            EagerMatchPolicy::Exact,
                            Duration::from_secs(60),
                            65536,
                        )
                        .unwrap(),
                    ),
                    // In particular retain the default interrupted-turn recovery.
                    ..Default::default()
                },
                Default::default(),
            );
            let user = Arc::new(ClockedUser {
                user: pair.user(),
                applied: Mutex::new(Vec::new()),
            });
            let mut llm_config = ClientConfig::new(Provider::OpenAi, "reader-pipeline-llm-key");
            llm_config.endpoint = http.endpoint.clone();
            let llm = Arc::new(LlmProcessor::new(
                LlmClient::new(llm_config).unwrap(),
                CompletionRequest::new("loopback", Context::default()),
            ));
            let processors: Vec<Arc<dyn FrameProcessor>> = vec![
                owner.clone(),
                before.clone(),
                user.clone(),
                after.clone(),
                llm.clone(),
                output.clone(),
                pair.assistant(),
                settled.clone(),
            ];
            let starting = tokio::spawn(async move {
                Pipeline::new(processors)
                    .start(PipelineConfig::default())
                    .await
                    .unwrap()
            });
            let (socket, _) = within(listener.accept()).await.unwrap();
            let mut peer = within(accept_async(socket)).await.unwrap();
            within(
                peer.send(Message::Text(
                    json!({
                        "type": "connected", "request_id": "reused-provider-id"
                    })
                    .to_string()
                    .into(),
                )),
            )
            .await
            .unwrap();
            let worker = within(starting).await.unwrap();
            let source = owner.processor.state.lock().unwrap().eager.clone().unwrap();
            let harness = Self {
                owner,
                source,
                before,
                after,
                output,
                settled,
                user,
                pair,
                llm,
                worker,
                peer,
                listener,
            };
            harness.barrier().await;
            harness
        }

        async fn event(&mut self, value: Value) {
            within(self.peer.send(Message::Text(value.to_string().into())))
                .await
                .unwrap();
        }

        fn restart_task(&self, keyterm: &str) -> JoinHandle<Result<(), ProcessorError>> {
            let processor = self.owner.processor.clone();
            let context = self.owner.context.lock().unwrap().clone().unwrap();
            let mut delta = SttSettings::default();
            delta.base.extra.insert("keyterm".into(), json!([keyterm]));
            tokio::spawn(async move { processor.restart_with_settings(&delta, &context).await })
        }

        async fn accept_replacement(&self) -> WebSocketStream<TcpStream> {
            let (socket, _) = within(self.listener.accept()).await.unwrap();
            within(accept_async(socket)).await.unwrap()
        }

        async fn wire(&mut self) -> Message {
            within(self.peer.next())
                .await
                .expect("Cartesia wire event")
                .unwrap()
        }

        async fn parsed(&mut self, marker: u8) -> bool {
            within(self.peer.send(Message::Ping(vec![marker].into())))
                .await
                .unwrap();
            self.wire().await == Message::Pong(vec![marker].into())
        }

        async fn send_applied(&self, frame: Frame) {
            let id = frame.id;
            let prior = self
                .user
                .applied
                .lock()
                .unwrap()
                .iter()
                .filter(|&&value| value == id)
                .count();
            within(self.worker.sender().send(frame, Direction::Downstream))
                .await
                .unwrap();
            wait(|| {
                self.user
                    .applied
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|&&value| value == id)
                    .count()
                    > prior
            })
            .await;
        }

        fn flush() -> Frame {
            Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                event: None,
                returning: false,
                origin: Some("pipecat.flows".into()),
            }))
        }

        async fn barrier(&self) {
            let frame = Self::flush();
            let id = frame.id;
            self.send_applied(frame).await;
            wait(|| self.settled.observed(id)).await;
        }

        async fn aggregation_barrier(&self, stage: &str) -> Result<(), String> {
            let frame = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                event: None,
                returning: false,
                origin: Some("reader-pipeline-aggregation".into()),
            }));
            let id = frame.id;
            let result = tokio::time::timeout(Duration::from_secs(5), async {
                self.worker
                    .sender()
                    .send(frame, Direction::Downstream)
                    .await
                    .map_err(|error| format!("reader-pipeline/{stage}: marker enqueue: {error}"))?;
                loop {
                    let applied = self.user.applied.lock().unwrap().contains(&id);
                    if applied && self.after.observed(id) {
                        return Ok(());
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await;
            result.unwrap_or_else(|_| {
                Err(format!(
                    "reader-pipeline/{stage}: user application / pre-LLM marker deadline"
                ))
            })
        }

        async fn buffered_at(&self, stage: &str) -> Result<(), String> {
            tokio::time::timeout(Duration::from_secs(5), async {
                while self.llm.speculation_snapshot().await.buffered_frames < 3 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(|_| format!("reader-pipeline/{stage}: real SSE buffering deadline"))
        }

        async fn drain_worker(worker: &mut PipelineWorker) -> Result<(), String> {
            while worker.recv().await.is_some() {}
            worker
                .join()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }

        async fn fail_after_cleanup(
            mut self,
            mut http: HttpPeer,
            gates: Vec<oneshot::Sender<()>>,
            stage_failure: String,
        ) -> ! {
            // Never let a failed fixture predicate strand either actual gate.
            // Record cleanup failures, but attempt every remaining cleanup.
            for gate in gates {
                let _ = gate.send(());
            }
            let mut failures = Vec::new();
            match tokio::time::timeout(Duration::from_secs(5), self.peer.send(Message::Close(None)))
                .await
            {
                Ok(Ok(())) => (),
                Ok(Err(error)) => failures.push(format!("peer close: {error}")),
                Err(_) => failures.push("peer close deadline".into()),
            }
            match tokio::time::timeout(Duration::from_secs(5), self.worker.end()).await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => failures.push(format!("End enqueue: {error}")),
                Err(_) => failures.push("End enqueue deadline".into()),
            }
            match tokio::time::timeout(Duration::from_secs(5), Self::drain_worker(&mut self.worker))
                .await
            {
                Ok(Ok(())) => (),
                Ok(Err(error)) => failures.push(format!("worker join: {error}")),
                Err(_) => {
                    failures.push("worker join deadline".into());
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        self.worker.cancel("reader fixture cleanup"),
                    )
                    .await
                    {
                        Ok(Ok(())) => (),
                        Ok(Err(error)) => failures.push(format!("worker cancel: {error}")),
                        Err(_) => failures.push("worker cancel deadline".into()),
                    }
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        Self::drain_worker(&mut self.worker),
                    )
                    .await
                    {
                        Ok(Ok(())) => (),
                        Ok(Err(error)) => failures.push(format!("cancelled worker join: {error}")),
                        Err(_) => failures.push("cancelled worker join deadline".into()),
                    }
                }
            }
            if let Some(stop) = http.stop.take() {
                let _ = stop.send(());
            }
            if let Some(mut task) = http.task.take() {
                match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                    Ok(Ok(())) => (),
                    Ok(Err(error)) => failures.push(format!("HTTP join: {error}")),
                    Err(_) => {
                        failures.push("HTTP join deadline".into());
                        task.abort();
                        match tokio::time::timeout(Duration::from_secs(5), task).await {
                            Ok(Ok(())) => (),
                            Ok(Err(error)) if error.is_cancelled() => (),
                            Ok(Err(error)) => failures.push(format!("aborted HTTP join: {error}")),
                            Err(_) => failures.push("aborted HTTP join deadline".into()),
                        }
                    }
                }
            }
            panic!("{stage_failure}; cleanup outcomes: {failures:?}");
        }

        async fn start_and_predict(&mut self) -> Frame {
            self.event(json!({"type": "turn.start", "turn_id": "untrusted"}))
                .await;
            wait(|| {
                self.before
                    .native(|source| matches!(source, NativeEagerSource::Start(_)))
                    .is_some()
            })
            .await;
            // Reader admission itself enforces the real Start flush roundtrip.
            // Receiving this provider-created prediction proves Ready installed.
            self.event(json!({"type": "turn.eager_end", "transcript": TRANSCRIPT}))
                .await;
            wait(|| {
                self.before
                    .native(|source| matches!(source, NativeEagerSource::Prediction(_)))
                    .is_some()
            })
            .await;
            self.before
                .native(|source| matches!(source, NativeEagerSource::Prediction(_)))
                .unwrap()
        }

        async fn hold_reader(&mut self) -> oneshot::Sender<()> {
            let (entered, release) = self.source.hold_reader_next();
            // These tests are current-thread. Once the downstream recorder has
            // observed Prediction, the synchronous reader dispatch has yielded
            // in next(). Update wakes that wait and reaches the next-loop gate.
            self.event(json!({"type": "turn.update", "transcript": "reach reader gate"}))
                .await;
            within(entered).await.unwrap();
            release
        }

        async fn prepare_source_end(&mut self) -> bool {
            let context = self.owner.context.lock().unwrap().clone().unwrap();
            self.owner.processor.prepare_end(&context).await.unwrap();
            self.wire()
                .await
                .to_text()
                .is_ok_and(|text| text == r#"{"type":"close"}"#)
        }

        async fn close_peer(&mut self) {
            within(self.peer.send(Message::Close(None))).await.unwrap();
        }

        async fn non_speaking(&self) -> bool {
            let mut stop = Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.0,
                timestamp: 0.0,
            }));
            stop.metadata
                .insert("reader-pipeline-time".into(), 1000.into());
            // A VAD stop does not replace A's already claimed scoped binding.
            self.send_applied(stop).await;
            // No final/external stop has been sent and eager expiry is 60s.
            // Thus this exact deadline is the controller's real 5s watchdog.
            self.user.user.deadline().unwrap() == Some(Duration::from_millis(6000))
        }

        async fn advance_watchdog(&self) {
            let mut frame = Self::flush();
            frame
                .metadata
                .insert("reader-pipeline-advance".into(), 7000.into());
            let id = frame.id;
            self.send_applied(frame).await;
            wait(|| self.settled.observed(id)).await;
        }

        async fn buffered(&self) {
            within(async {
                while self.llm.speculation_snapshot().await.buffered_frames < 3 {
                    tokio::task::yield_now().await;
                }
            })
            .await;
        }

        fn history(&self, role: Role) -> Vec<String> {
            self.pair
                .context()
                .snapshot()
                .unwrap()
                .messages
                .iter()
                .filter(|message| message.role == role)
                .map(|message| match &message.content {
                    Some(Content::Text(text)) => text.clone(),
                    value => panic!("unexpected history content: {value:?}"),
                })
                .collect()
        }

        fn output_count(&self) -> usize {
            self.output.count(|data| {
                matches!(
                    data,
                    FrameData::LlmText(_)
                        | FrameData::LlmThoughtText(_)
                        | FrameData::LlmContinuationCheckpoint
                        | FrameData::FunctionCallInProgress(_)
                )
            })
        }

        async fn finish(self) -> bool {
            self.worker.end().await.unwrap();
            let (_, report) = within(self.worker.collect()).await.unwrap();
            report.errors.is_empty()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actor_success_cannot_complete_drain_before_queued_raw_final_conversion() {
        let mut http = HttpPeer::new(true).await;
        let mut harness = Harness::new(&http, false, true).await;
        let prediction_frame = harness.start_and_predict().await;
        let NativeEagerSource::Prediction(prediction) =
            prediction_frame.native_eager_source().unwrap()
        else {
            unreachable!()
        };
        let release_answer = within(http.held.recv()).await.unwrap();
        let capability = harness
            .after
            .capability()
            .expect("real adopted prediction context");
        let initially_empty = harness
            .pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .is_empty();
        release_answer.send(()).unwrap();
        harness.buffered().await;
        let release_reader = harness.hold_reader().await;
        let actor_return = harness.source.watch_actor_return();
        let reader_drop = harness.source.watch_reader_drop();
        harness
            .event(json!({"type": "turn.end", "transcript": TRANSCRIPT}))
            .await;
        // Capacity one, held reader, and this ordered Pong prove raw End is
        // queued in the actor output, rather than already mapped to a receipt.
        let raw_final_queued = harness.parsed(71).await;
        let close_command = harness.prepare_source_end().await;
        harness.close_peer().await;
        let actor_ok = within(actor_return).await.unwrap();
        let pending_before_reader = capability.status() == SpeculationStatus::Pending;
        let live_before_reader = prediction.is_live();
        let no_native_final_before_reader = harness
            .before
            .native(|source| matches!(source, NativeEagerSource::Final(_)))
            .is_none();
        let no_output_before_reader = harness.output_count() == 0;
        let _ = release_reader.send(());
        // This witness also reports abnormal reader exit. A premature-complete
        // mutant can lose Final entirely; it must not fail as an arrival timeout.
        let normal_reader_drop = within(reader_drop).await.unwrap();
        if let Err(error) = harness.aggregation_barrier("case1-after-reader-drop").await {
            harness.fail_after_cleanup(http, Vec::new(), error).await;
        }
        let final_frame = harness
            .before
            .native(|source| matches!(source, NativeEagerSource::Final(_)));
        let stop_frame = harness
            .before
            .native(|source| matches!(source, NativeEagerSource::Stop { .. }));
        let exact_retained_receipt = match (&final_frame, &stop_frame) {
            (Some(final_frame), Some(stop_frame)) => match (
                final_frame.native_eager_source(),
                stop_frame.native_eager_source(),
            ) {
                (
                    Some(NativeEagerSource::Final(commit)),
                    Some(NativeEagerSource::Stop {
                        turn,
                        commit: Some(stop),
                    }),
                ) => {
                    commit == stop
                        && turn == prediction.turn()
                        && commit.is_live()
                        && commit.matches_prediction(prediction)
                        && commit.payload() == &final_frame.data
                }
                _ => false,
            },
            _ => false,
        };
        let pending_after_mechanical_drop = capability.status() == SpeculationStatus::Pending;
        let no_output_after_mechanical_drop = harness.output_count() == 0;
        let empty_after_mechanical_drop = harness
            .pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .is_empty();
        harness.before.hold_receipts.store(false, Ordering::SeqCst);
        for frame in [final_frame, stop_frame].into_iter().flatten() {
            harness.send_applied(frame).await;
        }
        harness.barrier().await;
        let released = capability.status() == SpeculationStatus::Released;
        let user_history = harness.history(Role::User);
        let assistant_history = harness.history(Role::Assistant);
        let output_count = harness.output_count();
        let exact_output = harness
            .output
            .count(|data| matches!(data, FrameData::LlmText(text) if text.text == ANSWER));
        let count_before_end = http.count();
        // Global End is teardown only, after every source-only observation.
        let clean_pipeline = harness.finish().await;
        let requests = http.finish().await;

        assert!(initially_empty && raw_final_queued && close_command && actor_ok);
        assert!(
            pending_before_reader && live_before_reader,
            "successful actor completed source drain before queued raw End conversion"
        );
        assert!(no_native_final_before_reader && no_output_before_reader);
        assert!(
            normal_reader_drop,
            "clean raw drain did not finish before EventStream Drop"
        );
        assert!(
            exact_retained_receipt && pending_after_mechanical_drop,
            "genuine offered Final/Stop lost authority during ended EventStream Drop"
        );
        assert!(no_output_after_mechanical_drop && empty_after_mechanical_drop);
        assert!(released && clean_pipeline);
        assert_eq!(count_before_end, 1);
        assert_eq!(
            requests.len(),
            1,
            "matching final must reuse the real speculative inference"
        );
        assert_eq!(requests[0]["messages"][0]["content"], TRANSCRIPT);
        assert_eq!(user_history, [TRANSCRIPT]);
        assert_eq!(assistant_history, [ANSWER]);
        assert_eq!(output_count, 1);
        assert_eq!(exact_output, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reader_skipped_queued_start_preserves_installed_a_receipt_without_mapping_b_tail() {
        let mut http = HttpPeer::new(true).await;
        let mut harness = Harness::new(&http, false, true).await;
        let prediction_frame = harness.start_and_predict().await;
        let NativeEagerSource::Prediction(prediction) =
            prediction_frame.native_eager_source().unwrap()
        else {
            unreachable!()
        };
        let release_answer = within(http.held.recv()).await.unwrap();
        let capability = harness
            .after
            .capability()
            .expect("actual A speculative context");
        release_answer.send(()).unwrap();
        harness.buffered().await;
        harness
            .event(json!({"type": "turn.end", "transcript": TRANSCRIPT}))
            .await;
        wait(|| {
            harness
                .before
                .native(|source| matches!(source, NativeEagerSource::Stop { .. }))
                .is_some()
        })
        .await;
        if let Err(error) = harness
            .aggregation_barrier("queued-b-before-reader-hold")
            .await
        {
            harness.fail_after_cleanup(http, Vec::new(), error).await;
        }
        let final_a = harness
            .before
            .native(|source| matches!(source, NativeEagerSource::Final(_)))
            .unwrap();
        let stop_a = harness
            .before
            .native(|source| matches!(source, NativeEagerSource::Stop { .. }))
            .unwrap();
        let NativeEagerSource::Final(commit_a) = final_a.native_eager_source().unwrap() else {
            unreachable!()
        };
        let installed_before_b = harness.source.accepts_final(commit_a) && commit_a.is_live();
        let release_reader = harness.hold_reader().await;
        let actor_return = harness.source.watch_actor_return();
        let reader_drop = harness.source.watch_reader_drop();
        harness
            .event(json!({"type": "turn.start", "turn_id": "B-queued-before-ending"}))
            .await;
        // Unlike actor-hidden Start, B arrives while the actor is still open.
        // The sole raw slot and ordered Pong prove B was actually enqueued;
        // only the held processor reader can subsequently skip its mapping.
        let b_start_queued = harness.parsed(91).await;
        let close_command = harness.prepare_source_end().await;
        let installed_after_prepare = harness.source.accepts_final(commit_a) && commit_a.is_live();
        let pending_before_release = capability.status() == SpeculationStatus::Pending;
        harness
            .event(json!({"type": "turn.end", "transcript": "B must never replace A"}))
            .await;
        let _ = release_reader.send(());
        let b_end_parsed = harness.parsed(92).await;
        harness.close_peer().await;
        let actor_ok = within(actor_return).await.unwrap();
        let normal_reader_drop = within(reader_drop).await.unwrap();
        if let Err(error) = harness
            .aggregation_barrier("queued-b-after-reader-drop")
            .await
        {
            harness.fail_after_cleanup(http, Vec::new(), error).await;
        }
        let starts_after_drop = harness
            .before
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter(|directed| {
                directed.direction == Direction::Downstream
                    && matches!(
                        directed.frame.native_eager_source(),
                        Some(NativeEagerSource::Start(_))
                    )
            })
            .count();
        let finals_after_drop = harness
            .before
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter(|directed| {
                directed.direction == Direction::Downstream
                    && matches!(
                        directed.frame.native_eager_source(),
                        Some(NativeEagerSource::Final(_))
                    )
            })
            .count();
        let stops_after_drop = harness
            .before
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter(|directed| {
                directed.direction == Direction::Downstream
                    && matches!(
                        directed.frame.native_eager_source(),
                        Some(NativeEagerSource::Stop { .. })
                    )
            })
            .count();
        let b_tail_absent = harness.before.count(|data| {
            matches!(data,
            FrameData::Transcription(text) | FrameData::EagerTranscription(text)
                if text.text == "B must never replace A")
        }) == 0;
        let retained_a_after_drop = commit_a.is_live()
            && commit_a.matches_prediction(prediction)
            && matches!(stop_a.native_eager_source(), Some(NativeEagerSource::Stop { turn, commit: Some(stop) })
                if turn == prediction.turn() && stop == commit_a);
        let pending_after_drop = capability.status() == SpeculationStatus::Pending;
        let empty_before_transfer = harness
            .pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .is_empty();
        let quiet_before_transfer = harness.output_count() == 0;
        harness.before.hold_receipts.store(false, Ordering::SeqCst);
        harness.send_applied(final_a).await;
        harness.send_applied(stop_a).await;
        harness.barrier().await;
        let released = capability.status() == SpeculationStatus::Released;
        let user_history = harness.history(Role::User);
        let assistant_history = harness.history(Role::Assistant);
        let exact_output = harness
            .output
            .count(|data| matches!(data, FrameData::LlmText(text) if text.text == ANSWER));
        let count_before_end = http.count();
        let clean_pipeline = harness.finish().await;
        let requests = http.finish().await;

        assert!(installed_before_b && b_start_queued && close_command && installed_after_prepare);
        assert!(pending_before_release && b_end_parsed && actor_ok && normal_reader_drop);
        assert_eq!(
            starts_after_drop, 1,
            "queued raw B Start was mapped after prepare_end won"
        );
        assert_eq!(
            finals_after_drop, 1,
            "B End minted a replacement native final"
        );
        assert_eq!(
            stops_after_drop, 1,
            "B End minted a replacement native stop"
        );
        assert!(b_tail_absent && retained_a_after_drop && pending_after_drop);
        assert!(empty_before_transfer && quiet_before_transfer && released && clean_pipeline);
        assert_eq!(count_before_end, 1);
        assert_eq!(
            requests.len(),
            1,
            "A must reuse its already-started real inference"
        );
        assert_eq!(requests[0]["messages"][0]["content"], TRANSCRIPT);
        assert_eq!(user_history, [TRANSCRIPT]);
        assert_eq!(assistant_history, [ANSWER]);
        assert_eq!(exact_output, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn restored_health_cannot_admit_held_connected_ack_and_fresh_restart_recovers() {
        // None is the otherwise-identical healthy held-connected control;
        // Some(false) restores true -> true, Some(true) false -> true.
        for restoration in [None, Some(false), Some(true)] {
            let http = HttpPeer::new(false).await;
            let mut harness = Harness::new(&http, false, false).await;
            let replacement = harness.restart_task("held-readiness");
            let mut held = harness.accept_replacement().await;
            let old_close = matches!(harness.wire().await, Message::Close(_));
            // The real application's readiness loop responds to Ping before
            // connected. No private pause substitutes for that provider state.
            within(held.send(Message::Ping(vec![101].into())))
                .await
                .unwrap();
            let handshake_parsed = within(held.next()).await.is_some_and(|message| {
                message.is_ok_and(|message| message == Message::Pong(vec![101].into()))
            });
            let health = harness.owner.processor.health().unwrap();
            let generation = health.generation();
            let (held_source, reserved_serial, socket_cancel, barrier_cancel, exact_reservation) = {
                let state = harness.owner.processor.state.lock().unwrap();
                (
                    state.eager.clone().unwrap(),
                    state.serial,
                    state.cancel.clone().unwrap(),
                    state.barrier_cancel.clone().unwrap(),
                    state.connecting
                        && state.sender.is_none()
                        && state.admission == Some(generation)
                        && state.timeline.load(Ordering::Acquire) == state.serial,
                )
            };
            let pending_before_connected = !replacement.is_finished();
            let native_before_connected = harness
                .before
                .frames
                .lock()
                .unwrap()
                .iter()
                .filter(|directed| directed.frame.native_eager_source().is_some())
                .count();
            let requests_before_connected = http.count();
            if let Some(false_then_true) = restoration {
                if false_then_true {
                    health.set_usable(false);
                }
                health.set_usable(true);
            }
            let new_generation = health.generation();
            // Release the same connected + real provider turn batch in all
            // schedules. A rejected old generation cannot publish its tail.
            let mut batch_sent = true;
            for value in [
                json!({"type": "connected", "request_id": "held-generation"}),
                json!({"type": "turn.start", "turn_id": "held-generation-turn"}),
                json!({"type": "turn.eager_end", "transcript": TRANSCRIPT}),
                json!({"type": "turn.end", "transcript": TRANSCRIPT}),
            ] {
                if within(held.feed(Message::Text(value.to_string().into())))
                    .await
                    .is_err()
                {
                    batch_sent = false;
                    break;
                }
            }
            if batch_sent {
                batch_sent = within(held.flush()).await.is_ok();
            }
            let result = within(replacement).await.unwrap();
            let accepted = result.is_ok();
            let cancelled = result
                .as_ref()
                .is_err_and(|error| error.message == crate::StreamingError::Cancelled.to_string());
            let (reservation_cleaned, exact_ready) = {
                let state = harness.owner.processor.state.lock().unwrap();
                (
                    !state.connecting
                        && state.sender.is_none()
                        && state.eager.is_none()
                        && state.cancel.is_none()
                        && state.barrier_cancel.is_none()
                        && state.admission.is_none()
                        && state.serial > reserved_serial
                        && state.timeline.load(Ordering::Acquire) == state.serial,
                    !state.connecting
                        && state
                            .sender
                            .as_ref()
                            .is_some_and(|sender| !sender.is_closed())
                        && state.serial == reserved_serial
                        && state.admission == Some(generation)
                        && state
                            .eager
                            .as_ref()
                            .is_some_and(|source| Arc::ptr_eq(source, &held_source)),
                )
            };
            let both_tokens_cancelled =
                socket_cancel.is_cancelled() && barrier_cancel.is_cancelled();
            harness.peer = held;
            harness.source = held_source.clone();
            harness.barrier().await;
            let native_after_old_ack = harness
                .before
                .frames
                .lock()
                .unwrap()
                .iter()
                .filter(|directed| directed.frame.native_eager_source().is_some())
                .count();
            let requests_after_old_ack = http.count();
            let empty_after_old_ack = harness
                .pair
                .context()
                .snapshot()
                .unwrap()
                .messages
                .is_empty();
            let quiet_after_old_ack = harness.output_count() == 0;
            let mut fresh_source = true;
            let mut fresh_ready = true;
            if restoration.is_some() {
                // Exact cleanup must allow an explicit fresh owner restart.
                // This uses a distinct settings change and a new real socket.
                let fresh = harness.restart_task("fresh-readiness");
                let mut peer = harness.accept_replacement().await;
                within(
                    peer.send(Message::Text(
                        json!({
                            "type": "connected", "request_id": "fresh-generation"
                        })
                        .to_string()
                        .into(),
                    )),
                )
                .await
                .unwrap();
                let fresh_result = within(fresh).await.unwrap();
                let current_source = {
                    let state = harness.owner.processor.state.lock().unwrap();
                    let source = state.eager.clone().unwrap();
                    fresh_source = !Arc::ptr_eq(&source, &held_source);
                    fresh_ready = fresh_result.is_ok()
                        && !state.connecting
                        && state
                            .sender
                            .as_ref()
                            .is_some_and(|sender| !sender.is_closed())
                        && state.admission == Some(new_generation)
                        && state.serial > reserved_serial;
                    source
                };
                harness.source = current_source;
                harness.peer = peer;
                harness.start_and_predict().await;
                harness
                    .event(json!({"type": "turn.end", "transcript": TRANSCRIPT}))
                    .await;
            }
            wait(|| {
                harness
                    .before
                    .native(|source| matches!(source, NativeEagerSource::Stop { .. }))
                    .is_some()
            })
            .await;
            harness.barrier().await;
            let user_history = harness.history(Role::User);
            let assistant_history = harness.history(Role::Assistant);
            let exact_output = harness
                .output
                .count(|data| matches!(data, FrameData::LlmText(text) if text.text == ANSWER));
            let requests_before_end = http.count();
            let actor_return = harness.source.watch_actor_return();
            let reader_drop = harness.source.watch_reader_drop();
            let close_command = harness.prepare_source_end().await;
            harness.close_peer().await;
            let actor_ok = within(actor_return).await.unwrap();
            let normal_reader_drop = within(reader_drop).await.unwrap();
            let clean_pipeline = harness.finish().await;
            let requests = http.finish().await;

            assert!(old_close && handshake_parsed && exact_reservation && pending_before_connected);
            assert_eq!(native_before_connected, 0);
            assert_eq!(requests_before_connected, 0);
            assert_eq!(new_generation != generation, restoration.is_some());
            if restoration.is_some() {
                assert!(
                    !accepted && cancelled,
                    "old connected acknowledgment admitted restored health"
                );
                assert!(
                    reservation_cleaned && both_tokens_cancelled,
                    "stale readiness left its exact reservation or notifications installed"
                );
                assert_eq!(
                    native_after_old_ack, 0,
                    "stale eager readiness published native frames"
                );
                assert_eq!(
                    requests_after_old_ack, 0,
                    "stale eager readiness made HTTP inference"
                );
                assert!(empty_after_old_ack && quiet_after_old_ack);
                assert!(
                    fresh_source && fresh_ready,
                    "fresh explicit restart did not recover after exact cleanup"
                );
            } else {
                assert!(
                    batch_sent && accepted && exact_ready && !both_tokens_cancelled,
                    "healthy held-connected control failed readiness admission"
                );
            }
            assert!(close_command && actor_ok && normal_reader_drop && clean_pipeline);
            assert_eq!(requests_before_end, 1);
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0]["messages"][0]["content"], TRANSCRIPT);
            assert_eq!(user_history, [TRANSCRIPT]);
            assert_eq!(assistant_history, [ANSWER]);
            assert_eq!(exact_output, 1);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn observed_fault_with_full_raw_queue_rejects_real_adoption_and_watchdog_recovery() {
        for fault in [false, true] {
            let http = HttpPeer::new(false).await;
            let mut harness = Harness::new(&http, true, false).await;
            let prediction_frame = harness.start_and_predict().await;
            let NativeEagerSource::Prediction(prediction) =
                prediction_frame.native_eager_source().unwrap()
            else {
                unreachable!()
            };
            let turn = prediction.turn().clone();
            let no_request_before_release = http.count() == 0;
            let release_reader = harness.hold_reader().await;
            let (close_entered, release_close) = harness.source.hold_actor_close();
            let mut actor_return = harness.source.watch_actor_return();
            let mut reader_drop = harness.source.watch_reader_drop();
            harness
                .event(json!({"type": "turn.update", "transcript": "occupy the sole raw slot"}))
                .await;
            let full_queue_parsed = harness.parsed(81).await;
            if fault {
                harness
                    .event(json!({"type": "error", "message": "reader-pipeline-provider-fault"}))
                    .await;
                // Reached only after run actually reads Error and retires its
                // source; the Error cannot be hidden behind a full event queue.
                within(close_entered).await.unwrap();
            } else {
                // Same held reader/full queue; the healthy actor is still live.
                let _ = harness.parsed(82).await;
                drop(close_entered);
            }
            let live_before_adoption = turn.is_live() && prediction.is_live();
            harness
                .before
                .hold_prediction
                .store(false, Ordering::SeqCst);
            harness.send_applied(prediction_frame.clone()).await;
            if let Err(error) = harness.aggregation_barrier("fault-after-adoption").await {
                harness
                    .fail_after_cleanup(http, vec![release_reader, release_close], error)
                    .await;
            }
            let adopted_after_release = harness.after.capability().is_some();
            if adopted_after_release {
                if let Err(error) = harness.buffered_at("fault-adopted-real-sse").await {
                    harness
                        .fail_after_cleanup(http, vec![release_reader, release_close], error)
                        .await;
                }
            }
            let after_adoption_count = http.count();
            let watchdog_armed = harness.non_speaking().await;
            harness.advance_watchdog().await;
            let count_while_held = http.count();
            let history_while_held = harness.pair.context().snapshot().unwrap().messages;
            let user_history = harness.history(Role::User);
            let assistant_history = harness.history(Role::Assistant);
            let output_while_held = harness.output_count();
            let genuine_starts = harness
                .before
                .frames
                .lock()
                .unwrap()
                .iter()
                .filter(|directed| {
                    directed.direction == Direction::Downstream
                        && matches!(
                            directed.frame.native_eager_source(),
                            Some(NativeEagerSource::Start(_))
                        )
                })
                .count();
            let early_actor = match actor_return.try_recv() {
                Ok(result) => Some(result),
                Err(oneshot::error::TryRecvError::Empty) => None,
                Err(error) => panic!("actor return witness closed: {error}"),
            };
            let early_reader = match reader_drop.try_recv() {
                Ok(result) => Some(result),
                Err(oneshot::error::TryRecvError::Empty) => None,
                Err(error) => panic!("reader drop witness closed: {error}"),
            };
            // Mutants have already received automatic HTTP/SSE answers. Release
            // both gates and join everything before making negative assertions.
            let _ = release_close.send(());
            let _ = release_reader.send(());
            let close_observed = if fault {
                matches!(harness.wire().await, Message::Close(_))
            } else {
                let close_command = harness.prepare_source_end().await;
                harness.close_peer().await;
                close_command
            };
            let actor_ok = match early_actor {
                Some(result) => result,
                None => within(actor_return).await.unwrap(),
            };
            let normal_reader_drop = match early_reader {
                Some(result) => result,
                None => within(reader_drop).await.unwrap(),
            };
            harness.barrier().await;
            let count_before_end = http.count();
            let clean_pipeline = harness.finish().await;
            let requests = http.finish().await;

            assert!(no_request_before_release && full_queue_parsed && close_observed);
            assert!(
                early_actor.is_none() && early_reader.is_none(),
                "source observations must precede actor-close and reader cleanup"
            );
            assert!(
                watchdog_armed,
                "VAD stop must arm the actual no-final watchdog"
            );
            assert_eq!(
                genuine_starts, 1,
                "watchdog setup must retain A's scoped turn"
            );
            assert_eq!(live_before_adoption, !fault);
            assert_eq!(
                adopted_after_release, !fault,
                "real prediction adoption disagreed with the observed source fault"
            );
            assert_eq!(actor_ok, !fault);
            assert_eq!(normal_reader_drop, !fault);
            if fault {
                assert_eq!(
                    after_adoption_count, 0,
                    "observed actor fault admitted held real prediction"
                );
                assert_eq!(
                    count_while_held, 0,
                    "observed fault allowed default watchdog recovery before cleanup"
                );
                assert_eq!(count_before_end, 0);
                assert!(
                    requests.is_empty(),
                    "faulted source made real HTTP inference"
                );
                assert!(history_while_held.is_empty());
                assert!(user_history.is_empty() && assistant_history.is_empty());
                assert_eq!(output_while_held, 0);
            } else {
                assert!(clean_pipeline);
                assert_eq!(
                    after_adoption_count, 1,
                    "healthy twin never exercised real prediction adoption"
                );
                assert_eq!(
                    count_while_held, 2,
                    "healthy twin never exercised default recovery"
                );
                assert_eq!(count_before_end, 2);
                assert_eq!(requests.len(), 2);
                assert_eq!(requests[0]["messages"][0]["content"], TRANSCRIPT);
                assert_eq!(requests[1]["messages"][0]["role"], "developer");
                assert_eq!(
                    requests[1]["messages"][0]["content"],
                    DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT
                );
                assert!(user_history.is_empty());
                assert_eq!(assistant_history, [ANSWER]);
                assert_eq!(output_while_held, 1);
            }
        }
    }
}
