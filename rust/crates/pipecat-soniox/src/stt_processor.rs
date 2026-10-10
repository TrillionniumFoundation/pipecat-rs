// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    settings,
    tts_processor::{error_frame, processor_error},
    CancellationToken, SonioxSttClient, SonioxSttEvent, SonioxSttSender, StreamingError,
};
use pipecat_core::{
    async_trait, DetailedTranscriptionFrame, Direction, Frame, FrameData, FrameProcessor, Metric,
    ProcessorContext, ProcessorError, ProcessorHealth, ServiceMetadataFrame, StartFrame,
    SttMetadataFrame, TextFrame,
};
use pipecat_turns::{
    UserAggregatorConfig, UserTurnStartStrategies, UserTurnStartStrategy, UserTurnStopStrategies,
    UserTurnStopStrategy,
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
    sender: Option<SonioxSttSender>,
    user_id: String,
    ending: bool,
    generation: u64,
    config: crate::SonioxSttConfig,
    barrier_cancel: CancellationToken,
}
/// Nonblocking audio admission with provider token and turn callbacks. Transport
/// failures are reported without replay; settings replace the socket in order.
pub struct SonioxSttProcessor {
    client: SonioxSttClient,
    state: Arc<Mutex<State>>,
    health: ProcessorHealth,
    muted: Arc<AtomicBool>,
    emission: Arc<tokio::sync::Mutex<()>>,
    user_id: String,
    audio_passthrough: bool,
    setup_gate: tokio::sync::Mutex<()>,
}
impl SonioxSttProcessor {
    pub fn new(client: SonioxSttClient) -> Self {
        let config = client.config().clone();
        Self {
            client,
            state: Arc::new(Mutex::new(State {
                sender: None,
                user_id: String::new(),
                ending: false,
                generation: 0,
                config,
                barrier_cancel: CancellationToken::new(),
            })),
            health: ProcessorHealth::default(),
            muted: Arc::new(AtomicBool::new(false)),
            emission: Arc::new(tokio::sync::Mutex::new(())),
            user_id: String::new(),
            audio_passthrough: true,
            setup_gate: tokio::sync::Mutex::new(()),
        }
    }
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Result<Self, StreamingError> {
        let id = user_id.into();
        if id.len() > 1024 {
            return Err(StreamingError::TooLarge);
        }
        self.user_id = id;
        Ok(self)
    }
    pub fn with_audio_passthrough(mut self, value: bool) -> Self {
        self.audio_passthrough = value;
        self
    }
    pub fn sender(&self) -> Result<SonioxSttSender, StreamingError> {
        self.state
            .lock()
            .map_err(|_| StreamingError::Closed)?
            .sender
            .clone()
            .ok_or(StreamingError::Closed)
    }
    pub fn config_snapshot(&self) -> Result<crate::SonioxSttConfig, StreamingError> {
        let state = self.state.lock().map_err(|_| StreamingError::Closed)?;
        match &state.sender {
            Some(sender) => sender.config_snapshot(),
            None => Ok(state.config.clone()),
        }
    }
    fn merge_settings(&self, delta: &Map<String, Value>) -> Result<(), StreamingError> {
        let mut state = self.state.lock().map_err(|_| StreamingError::Closed)?;
        if state.ending {
            return Err(StreamingError::Closed);
        }
        let sender = state.sender.as_ref().ok_or(StreamingError::Closed)?;
        sender.merge_settings(delta)?;
        state.config = sender.config_snapshot()?;
        Ok(())
    }
    /// Native turn strategies are explicit configuration, not auto-applied
    /// from untrusted frame metadata. Pipecat/VAD mode keeps native defaults.
    pub fn recommended_user_config(&self) -> UserAggregatorConfig {
        let mut result = UserAggregatorConfig::default();
        if !self.client.config().vad_force_turn_endpoint {
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
        }
        result
    }
}
#[async_trait]
impl FrameProcessor for SonioxSttProcessor {
    fn name(&self) -> &str {
        "SonioxSTTService"
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
                "pipeline and Soniox sample rates differ",
            )));
        }
        if !ctx.is_usable() {
            return Ok(());
        }
        let generation = ctx.health().generation();
        let mut health = ctx.health().watch();
        let _setup = tokio::select! { biased;
            _ = health.changed() => return Ok(()),
            lock = tokio::time::timeout(self.client.config().limits.connect_timeout, self.setup_gate.lock()) => lock.map_err(|_| processor_error(StreamingError::Timeout("STT setup")))?,
        };
        let (serial, client) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            if state.sender.as_ref().is_some_and(|s| !s.is_closed()) {
                return Err(processor_error(StreamingError::Invalid(
                    "Soniox already connected",
                )));
            }
            if let Some(sender) = &state.sender {
                state.config = sender.config_snapshot().map_err(processor_error)?;
            }
            state.generation = state
                .generation
                .checked_add(1)
                .ok_or_else(|| ProcessorError::fatal("STT generation exhausted"))?;
            state.ending = false;
            (
                state.generation,
                SonioxSttClient::new(state.config.clone()).map_err(processor_error)?,
            )
        };
        let mut session = tokio::select! { biased;
            _ = health.changed() => return Ok(()),
            result = client.connect() => result.map_err(processor_error)?,
        };
        if !ctx.is_usable() || ctx.health().generation() != generation {
            session.cancel();
            return Ok(());
        }
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            if state.ending || state.generation != serial {
                session.cancel();
                return Ok(());
            }
            state.user_id = self.user_id.clone();
            state.sender = Some(session.sender());
            state.barrier_cancel.cancel();
            state.barrier_cancel = CancellationToken::new();
        }
        let barrier_cancel = self
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?
            .barrier_cancel
            .clone();
        let barrier_timeout = self
            .client
            .config()
            .limits
            .response_timeout
            .min(Duration::from_secs(5));
        let output = ctx.clone();
        let state = self.state.clone();
        let muted = self.muted.clone();
        let emission = self.emission.clone();
        ctx.spawn("soniox-stt-events",false,async move{
            loop{
                let event=tokio::select!{biased;_=health.changed()=>{session.cancel();return Ok(());},event=session.next()=>event};
                let _guard=emission.lock().await;
                let (current,ending,user_id)={let state=state.lock().map_err(|_|processor_error(StreamingError::Closed))?;(state.generation==serial,state.ending,state.user_id.clone())};
                if !current||!output.is_usable()||output.health().generation()!=generation{session.cancel();return Ok(());}
                match event{
                    Some(Ok(SonioxSttEvent::Transcript(transcript))) if !muted.load(Ordering::Acquire)&&!ending=>{
                        let data=DetailedTranscriptionFrame{text:TextFrame::new(transcript.text),user_id,timestamp:chrono::Utc::now().to_rfc3339(),language:transcript.language,result:Some(Value::Array(transcript.tokens)),finalized:transcript.finalized};
                        let frame=if data.finalized{FrameData::TranscriptionDetailed(data)}else{FrameData::InterimTranscriptionDetailed(data)};
                        output.push(Frame::new(frame),Direction::Downstream).await?;
                    }
                    Some(Ok(SonioxSttEvent::TurnStarted)) if !muted.load(Ordering::Acquire)&&!ending=>{
                        output.broadcast(Frame::new(FrameData::ProposedUserStartedSpeaking)).await?;
                        // The start resolver can interrupt queued normal input. Complete
                        // its existing pipeline roundtrip before admitting this turn's
                        // transcript and stop, without changing their interruptibility.
                        let settled = pipecat_core::LocalFlushEvent::default();
                        output.push(Frame::new(FrameData::FlushDetailed(pipecat_core::PipelineFlushFrame {
                            event: Some(settled.clone()),
                            returning: false,
                            origin: Some("pipecat.soniox.turn-start".into()),
                        })), Direction::Downstream).await?;
                        drop(_guard);
                        tokio::select! { biased;
                            _ = barrier_cancel.cancelled() => continue,
                            _ = health.changed() => { session.cancel(); return Ok(()); },
                            result = tokio::time::timeout(barrier_timeout, settled.wait()) => {
                                if result.is_err() {
                                    output.push_error_if_health_generation(error_frame(StreamingError::Timeout("STT turn-start barrier")),false,generation).await?;
                                    session.cancel();
                                    return Ok(());
                                }
                            }
                        }
                        let (current, ending) = { let state = state.lock().map_err(|_| processor_error(StreamingError::Closed))?; (state.generation == serial, state.ending) };
                        if !current || !output.is_usable() || output.health().generation() != generation {
                            session.cancel();
                            return Ok(());
                        }
                        if ending { continue; }
                    },
                    Some(Ok(SonioxSttEvent::TurnStopped)) if !muted.load(Ordering::Acquire)&&!ending=>output.broadcast(Frame::new(FrameData::ProposedUserStoppedSpeaking)).await?,
                    Some(Ok(SonioxSttEvent::AudioSent{seconds})) if output.configuration().enable_usage_metrics=>output.push(Frame::new(FrameData::Metrics(vec![Metric::SttUsage{processor:"SonioxSTTService".into(),audio_seconds:seconds}])),Direction::Downstream).await?,
                    Some(Ok(_))=>(),
                    Some(Err(StreamingError::Cancelled))|None=>return Ok(()),
                    Some(Err(error))=>{output.push_error_if_health_generation(error_frame(error),false,generation).await?;return Ok(());}
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
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                let _guard = self.emission.lock().await;
                {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| processor_error(StreamingError::Closed))?;
                    state.ending = true;
                    state.barrier_cancel.cancel();
                }
                if let Ok(sender) = self.sender() {
                    sender.cancel();
                }
            }
            FrameData::SttMute(value) => self.muted.store(*value, Ordering::Release),
            FrameData::SttUpdateSettings(update) if settings::targets(self, &update.service) => {
                settings::validate_values(&update.settings)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                if let Some(typed) = &update.delta {
                    settings::validate_values(&typed.base.extra)
                        .map_err(|e| processor_error(e).with_permanence_override(false))?;
                    settings::validate_setting(&typed.base.model)
                        .map_err(|e| processor_error(e).with_permanence_override(false))?;
                    settings::validate_setting(&typed.language)
                        .map_err(|e| processor_error(e).with_permanence_override(false))?;
                }
                let mut delta: Map<String, Value> = update.settings.clone().into_iter().collect();
                if let Some(typed) = &update.delta {
                    delta = typed.base.extra.clone().into_iter().collect();
                    settings::add(&mut delta, "model", &typed.base.model);
                    settings::add(&mut delta, "language", &typed.language);
                }
                self.merge_settings(&delta)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            FrameData::ServiceUpdateSettings(update)
                if settings::targets(self, &update.service) =>
            {
                settings::validate_values(&update.settings)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                if let Some(typed) = &update.delta {
                    settings::validate_values(&typed.extra)
                        .map_err(|e| processor_error(e).with_permanence_override(false))?;
                    settings::validate_setting(&typed.model)
                        .map_err(|e| processor_error(e).with_permanence_override(false))?;
                }
                let mut delta: Map<String, Value> = update.settings.clone().into_iter().collect();
                if let Some(typed) = &update.delta {
                    delta = typed.extra.clone().into_iter().collect();
                    settings::add(&mut delta, "model", &typed.model);
                }
                self.merge_settings(&delta)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            _ => (),
        }
        if direction == Direction::Downstream {
            match &frame.data {
                FrameData::Start(_) => {
                    ctx.push(
                        Frame::new(FrameData::SttMetadata(SttMetadataFrame {
                            base: ServiceMetadataFrame {
                                service_name: self.name().into(),
                                user_turn_strategies: None,
                            },
                            ttfs_p99_latency: 0.35,
                        })),
                        direction,
                    )
                    .await?
                }
                FrameData::VadUserStartedSpeaking(_)
                | FrameData::VadUserStartedSpeakingDetailed(_) => {
                    if !self.muted.load(Ordering::Acquire) {
                        self.sender()
                            .and_then(|s| s.try_start_turn())
                            .map_err(processor_error)?;
                    }
                }
                FrameData::VadUserStoppedSpeaking(_)
                | FrameData::VadUserStoppedSpeakingDetailed(_) => self
                    .sender()
                    .and_then(|s| s.try_finalize())
                    .map_err(processor_error)?,
                FrameData::InputAudio(audio) => {
                    self.audio(audio, &self.user_id, ctx)?;
                    if !self.audio_passthrough {
                        return Ok(());
                    }
                }
                FrameData::UserAudio(user) => {
                    self.audio(&user.audio, &user.user_id, ctx)?;
                    if !self.audio_passthrough {
                        return Ok(());
                    }
                }
                _ => (),
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        {
            let _guard = self.emission.lock().await;
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            state.ending = true;
            state.barrier_cancel.cancel();
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
        let _guard = self.emission.lock().await;
        let mut state = self
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?;
        state.ending = true;
        state.barrier_cancel.cancel();
        if let Some(sender) = state.sender.take() {
            state.config = sender.config_snapshot().map_err(processor_error)?;
            sender.cancel();
        }
        Ok(())
    }
}
impl SonioxSttProcessor {
    fn audio(
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
        if state.ending {
            return Err(processor_error(StreamingError::Closed));
        }
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
