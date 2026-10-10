// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{FluxClient, FluxEvent, FluxSender, StreamingError, TurnEvent};
use pipecat_core::{
    async_trait, DetailedTranscriptionFrame, Direction, ErrorCategory, ErrorFrame, Frame,
    FrameData, FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorReference, ServiceMetadataFrame, ServiceSettings, Setting, StartFrame,
    SttMetadataFrame, SttSettings, TextFrame,
};
use pipecat_turns::{
    EagerEndOfTurn, UserAggregatorConfig, UserTurnStartStrategies, UserTurnStartStrategy,
    UserTurnStopStrategies, UserTurnStopStrategy,
};
use serde_json::{Map, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

struct State {
    sender: Option<FluxSender>,
    user_id: String,
    eager: EagerEndOfTurn,
    ending: bool,
    session_id: u64,
}
/// A real native pipeline service. Provider callbacks are drained in one owned
/// task; audio admission is nonblocking and cannot stall system-frame handling.
pub struct FluxSttProcessor {
    client: FluxClient,
    state: Arc<Mutex<State>>,
    health: ProcessorHealth,
    muted: Arc<AtomicBool>,
    emission: Arc<tokio::sync::Mutex<()>>,
    audio_passthrough: bool,
    user_id: String,
}
impl FluxSttProcessor {
    pub fn new(client: FluxClient) -> Self {
        let eager = EagerEndOfTurn::new(client.config().eager_end_of_turn);
        Self {
            client,
            state: Arc::new(Mutex::new(State {
                sender: None,
                user_id: String::new(),
                eager,
                ending: false,
                session_id: 0,
            })),
            health: ProcessorHealth::default(),
            muted: Arc::new(AtomicBool::new(false)),
            emission: Arc::new(tokio::sync::Mutex::new(())),
            audio_passthrough: true,
            user_id: String::new(),
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
    /// Native aggregators require explicit strategy configuration. This avoids
    /// accepting arbitrary service metadata as authority to replace policies.
    pub fn recommended_user_config(&self) -> UserAggregatorConfig {
        let mut result = UserAggregatorConfig::default();
        result.turn.enable_interruptions = self.client.config().should_interrupt;
        result.turn.start_on_transcription = false;
        result.turn.require_model_completion = false;
        result.turn.start_strategies = Some(
            UserTurnStartStrategies::new(vec![UserTurnStartStrategy::external()
                .with_interruptions(self.client.config().should_interrupt)])
            .expect("one strategy"),
        );
        result.turn.stop_strategies = Some(
            UserTurnStopStrategies::new(vec![UserTurnStopStrategy::external(
                Duration::from_millis(500),
                true,
            )])
            .expect("one strategy"),
        );
        result.eager =
            EagerEndOfTurn::new(self.client.config().eager_end_of_turn).recommended_eager_config();
        result
    }
    pub fn sender(&self) -> Result<FluxSender, StreamingError> {
        self.state
            .lock()
            .map_err(|_| StreamingError::Closed)?
            .sender
            .clone()
            .ok_or(StreamingError::Closed)
    }
    fn targets(&self, target: &Option<ProcessorReference>) -> bool {
        match target {
            None => true,
            Some(ProcessorReference::Name(name)) => name == self.name(),
            Some(ProcessorReference::Local(target)) => {
                std::ptr::addr_eq(target.0.as_ref(), self as &dyn FrameProcessor)
            }
        }
    }
    async fn audio(
        &self,
        audio: &pipecat_core::AudioRawFrame,
        user_id: &str,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !ctx.is_usable() || self.muted.load(Ordering::Acquire) || audio.audio.is_empty() {
            return Ok(());
        }
        if user_id.len() > 1024 {
            return Err(processor_error(StreamingError::TooLarge));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?;
        let sender = state
            .sender
            .as_ref()
            .ok_or_else(|| processor_error(StreamingError::Closed))?;
        sender.validate_audio(audio).map_err(processor_error)?;
        sender
            .try_send_audio(audio.clone())
            .map_err(processor_error)?;
        state.user_id = user_id.into();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for FluxSttProcessor {
    fn name(&self) -> &str {
        "DeepgramFluxSTTService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if start.audio_in_sample_rate != self.client.config().sample_rate {
            return Err(processor_error(StreamingError::Invalid(
                "pipeline and Flux sample rates differ",
            )));
        }
        if !ctx.is_usable() {
            return Ok(());
        }
        let generation = ctx.health().generation();
        let mut health = ctx.health().watch();
        let mut session = self.client.connect().await.map_err(processor_error)?;
        if !ctx.is_usable() || ctx.health().generation() != generation {
            session.cancel();
            return Ok(());
        }
        let session_id = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            if state
                .sender
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
            {
                session.cancel();
                return Err(processor_error(StreamingError::Invalid(
                    "Flux processor already has an active session",
                )));
            }
            state.session_id = state
                .session_id
                .checked_add(1)
                .ok_or_else(|| ProcessorError::fatal("Flux session generation exhausted"))?;
            state.sender = Some(session.sender());
            state.user_id = self.user_id.clone();
            state.eager.clear();
            state.ending = false;
            state.session_id
        };
        let output = ctx.clone();
        let state = self.state.clone();
        let muted = self.muted.clone();
        let emission = self.emission.clone();
        ctx.spawn("deepgram-flux-events", false, async move {
            loop {
                let event = tokio::select! {
                    biased;
                    _ = health.changed() => None,
                    event = session.next() => event,
                };
                // Keep prediction emission and lifecycle withdrawal in one order.
                // No network I/O occurs while this lock is held.
                let _emission = emission.lock().await;
                if state
                    .lock()
                    .map_err(|_| processor_error(StreamingError::Closed))?
                    .session_id
                    != session_id
                {
                    session.cancel();
                    return Ok(());
                }
                if !output.is_usable() || output.health().generation() != generation {
                    session.cancel();
                    cancel_eager(&state, &output).await?;
                    return Ok(());
                }
                match event {
                    Some(Ok(event)) => {
                        emit(event, &state, muted.load(Ordering::Acquire), &output).await?
                    }
                    Some(Err(StreamingError::Cancelled)) | None => {
                        session.cancel();
                        cancel_eager(&state, &output).await?;
                        return Ok(());
                    }
                    Some(Err(error)) => {
                        cancel_eager(&state, &output).await?;
                        output
                            .push_error_if_health_generation(error_frame(error), false, generation)
                            .await?;
                        return Ok(());
                    }
                }
            }
        })?;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            FrameData::Start(_) if direction == Direction::Downstream => {
                ctx.push(
                    Frame::new(FrameData::SttMetadata(SttMetadataFrame {
                        base: ServiceMetadataFrame {
                            service_name: self.name().into(),
                            user_turn_strategies: None,
                        },
                        ttfs_p99_latency: 0.0,
                    })),
                    Direction::Downstream,
                )
                .await?;
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                let _emission = self.emission.lock().await;
                self.state
                    .lock()
                    .map_err(|_| processor_error(StreamingError::Closed))?
                    .ending = true;
                cancel_eager(&self.state, ctx).await?;
                if let Ok(sender) = self.sender() {
                    sender.cancel();
                }
            }
            FrameData::SttMute(value) => {
                let _emission = self.emission.lock().await;
                self.muted.store(*value, Ordering::Release);
                if *value {
                    cancel_eager(&self.state, ctx).await?;
                }
            }
            FrameData::SttUpdateSettings(update) if self.targets(&update.service) => {
                let delta = match &update.delta {
                    Some(delta) => stt_delta(delta),
                    None => update.settings.clone().into_iter().collect(),
                };
                self.sender()
                    .and_then(|s| s.merge_settings(&delta))
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            FrameData::ServiceUpdateSettings(update) if self.targets(&update.service) => {
                let delta = match &update.delta {
                    Some(delta) => base_delta(delta),
                    None => update.settings.clone().into_iter().collect(),
                };
                self.sender()
                    .and_then(|s| s.merge_settings(&delta))
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            FrameData::InputAudio(audio) if direction == Direction::Downstream => {
                self.audio(audio, &self.user_id, ctx).await?;
                if !self.audio_passthrough {
                    return Ok(());
                }
            }
            FrameData::UserAudio(user) if direction == Direction::Downstream => {
                self.audio(&user.audio, &user.user_id, ctx).await?;
                if !self.audio_passthrough {
                    return Ok(());
                }
            }
            _ => (),
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        {
            let _emission = self.emission.lock().await;
            self.state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?
                .ending = true;
            cancel_eager(&self.state, ctx).await?;
        }
        if let Ok(sender) = self.sender() {
            match sender.finish().await {
                Ok(()) | Err(StreamingError::Closed | StreamingError::Cancelled) => (),
                Err(error) => return Err(processor_error(error)),
            }
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let _emission = self.emission.lock().await;
        let mut state = self
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?;
        if let Some(sender) = state.sender.take() {
            sender.cancel();
        }
        state.eager.clear();
        state.ending = true;
        Ok(())
    }
}
async fn cancel_eager(state: &Mutex<State>, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
    let frame = state
        .lock()
        .map_err(|_| processor_error(StreamingError::Closed))?
        .eager
        .cancel();
    if let Some(frame) = frame {
        ctx.broadcast(frame).await?;
    }
    Ok(())
}
async fn emit(
    event: FluxEvent,
    state: &Mutex<State>,
    muted: bool,
    ctx: &ProcessorContext,
) -> Result<(), ProcessorError> {
    match event {
        FluxEvent::Turn {
            turn,
            model,
            min_confidence,
        } => {
            match turn.event {
                TurnEvent::StartOfTurn
                    if !muted
                        && !state
                            .lock()
                            .map_err(|_| processor_error(StreamingError::Closed))?
                            .ending =>
                {
                    ctx.broadcast(Frame::new(FrameData::ProposedUserStartedSpeaking))
                        .await?
                }
                TurnEvent::TurnResumed => cancel_eager(state, ctx).await?,
                TurnEvent::EagerEndOfTurn if !muted && !turn.transcript.trim().is_empty() => {
                    let frame = {
                        let mut state = state
                            .lock()
                            .map_err(|_| processor_error(StreamingError::Closed))?;
                        let transcript = DetailedTranscriptionFrame {
                            text: TextFrame::new(&turn.transcript),
                            user_id: state.user_id.clone(),
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            language: turn.language(model),
                            result: Some(turn.result.clone()),
                            finalized: false,
                        };
                        if state.ending {
                            None
                        } else {
                            state.eager.predict(transcript)
                        }
                    };
                    if let Some(frame) = frame {
                        ctx.push(frame, Direction::Downstream).await?;
                    }
                }
                TurnEvent::EndOfTurn => {
                    let accepted = !muted && turn.accepts(min_confidence);
                    let (user_id, withdrawal) = {
                        let mut state = state
                            .lock()
                            .map_err(|_| processor_error(StreamingError::Closed))?;
                        let withdrawal = if accepted {
                            state.eager.clear();
                            None
                        } else {
                            state.eager.cancel()
                        };
                        (state.user_id.clone(), withdrawal)
                    };
                    if accepted {
                        let transcript = DetailedTranscriptionFrame {
                            text: TextFrame::new(&turn.transcript),
                            user_id,
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            language: turn.language(model),
                            result: Some(turn.result),
                            finalized: true,
                        };
                        ctx.push(
                            Frame::new(FrameData::TranscriptionDetailed(transcript)),
                            Direction::Downstream,
                        )
                        .await?;
                    } else if let Some(frame) = withdrawal {
                        // With no accepted final, the provisional transcript cannot
                        // prove equality. Revoke it before sending the turn boundary.
                        ctx.broadcast(frame).await?;
                    }
                    ctx.broadcast(Frame::new(FrameData::ProposedUserStoppedSpeaking))
                        .await?;
                }
                _ => (), // Update is observable on FluxSession, never a fabricated final.
            }
        }
        FluxEvent::Reset { interrupted_turn } => {
            cancel_eager(state, ctx).await?;
            if interrupted_turn {
                ctx.broadcast(Frame::new(FrameData::ProposedUserStoppedSpeaking))
                    .await?;
            }
        }
        FluxEvent::ConfigureRejected => {
            ctx.push_error_with_permanence_override(
                error_frame(StreamingError::Invalid("Flux Configure was rejected")),
                Some(false),
            )
            .await?
        }
        FluxEvent::WatchdogAudio { seconds } | FluxEvent::AudioSent { seconds }
            if ctx.configuration().enable_usage_metrics =>
        {
            ctx.push(
                Frame::new(FrameData::Metrics(vec![Metric::SttUsage {
                    processor: "DeepgramFluxSTTService".into(),
                    audio_seconds: seconds,
                }])),
                Direction::Downstream,
            )
            .await?;
        }
        _ => (),
    }
    Ok(())
}
fn setting(values: &mut Map<String, Value>, key: &str, value: &Setting<String>) {
    match value {
        Setting::NotGiven => (),
        Setting::Null => {
            values.insert(key.into(), Value::Null);
        }
        Setting::Value(value) => {
            values.insert(key.into(), value.clone().into());
        }
    }
}
fn base_delta(value: &ServiceSettings) -> Map<String, Value> {
    let mut values = value.extra.clone().into_iter().collect();
    setting(&mut values, "model", &value.model);
    values
}
fn stt_delta(value: &SttSettings) -> Map<String, Value> {
    let mut values = base_delta(&value.base);
    setting(&mut values, "language", &value.language);
    values
}
fn processor_error(error: StreamingError) -> ProcessorError {
    let frame = error_frame(error);
    ProcessorError::new(frame.error).with_category(frame.category)
}
fn error_frame(error: StreamingError) -> ErrorFrame {
    let category = match &error {
        StreamingError::Http(status) => ErrorCategory::from_http_status(*status),
        StreamingError::Timeout("Flux Connected")
        | StreamingError::Invalid(_)
        | StreamingError::TooLarge
        | StreamingError::PartialSample => ErrorCategory::InvalidRequest,
        StreamingError::Protocol(_) | StreamingError::Provider { .. } => ErrorCategory::Server,
        _ => ErrorCategory::Connectivity,
    };
    ErrorFrame {
        category,
        ..ErrorFrame::new(error.to_string())
    }
}
