// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{GladiaError, GladiaSttClient, GladiaSttConfig, GladiaSttEvent, GladiaSttSender};
use pipecat_core::{
    async_trait, AudioRawFrame, DetailedTranscriptionFrame, Direction, ErrorCategory, ErrorFrame,
    Frame, FrameData, FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorReference, ServiceMetadataFrame, Setting, StartFrame, SttMetadataFrame, TextFrame,
    UserTurnStrategies, UserTurnStrategy,
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
    sender: Option<GladiaSttSender>,
    config: GladiaSttConfig,
    user_id: String,
    ending: bool,
    generation: u64,
}
/// Gladia callbacks feed detailed transcripts, translations and provider turn
/// proposals into the native pipeline. Audio admission never blocks on the wire.
pub struct GladiaSttProcessor {
    state: Arc<Mutex<State>>,
    health: ProcessorHealth,
    muted: Arc<AtomicBool>,
    emission: Arc<tokio::sync::Mutex<()>>,
    setup_gate: tokio::sync::Mutex<()>,
    user_id: String,
    audio_passthrough: bool,
    ttfs_p99_latency: f64,
}
impl GladiaSttProcessor {
    pub fn new(client: GladiaSttClient) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                sender: None,
                config: client.config().clone(),
                user_id: String::new(),
                ending: false,
                generation: 0,
            })),
            health: ProcessorHealth::default(),
            muted: Arc::new(AtomicBool::new(false)),
            emission: Arc::new(tokio::sync::Mutex::new(())),
            setup_gate: tokio::sync::Mutex::new(()),
            user_id: String::new(),
            audio_passthrough: true,
            ttfs_p99_latency: 1.49,
        }
    }
    pub fn with_user_id(mut self, id: impl Into<String>) -> Result<Self, GladiaError> {
        let id = id.into();
        if id.len() > 1024 {
            return Err(GladiaError::TooLarge);
        }
        self.user_id = id;
        Ok(self)
    }
    pub fn with_audio_passthrough(mut self, value: bool) -> Self {
        self.audio_passthrough = value;
        self
    }
    pub fn with_ttfs_p99_latency(mut self, value: f64) -> Result<Self, GladiaError> {
        if !value.is_finite() || !(0.0..=300.0).contains(&value) {
            return Err(GladiaError::Invalid("TTFS latency"));
        }
        self.ttfs_p99_latency = value;
        Ok(self)
    }
    pub fn sender(&self) -> Result<GladiaSttSender, GladiaError> {
        self.state
            .lock()
            .map_err(|_| GladiaError::Closed)?
            .sender
            .clone()
            .ok_or(GladiaError::Closed)
    }
    pub fn config_snapshot(&self) -> Result<GladiaSttConfig, GladiaError> {
        let state = self.state.lock().map_err(|_| GladiaError::Closed)?;
        match &state.sender {
            Some(sender) => sender.config_snapshot(),
            None => Ok(state.config.clone()),
        }
    }
    pub fn merge_settings(&self, delta: &Map<String, Value>) -> Result<(), GladiaError> {
        let mut state = self.state.lock().map_err(|_| GladiaError::Closed)?;
        if let Some(sender) = &state.sender {
            sender.merge_settings(delta)?;
            state.config = sender.config_snapshot()?;
        } else {
            let mut candidate = state.config.clone();
            candidate.settings = candidate.settings.merge(delta)?;
            candidate.validate()?;
            state.config = candidate;
        }
        Ok(())
    }
    pub fn recommended_user_config(&self) -> Result<UserAggregatorConfig, GladiaError> {
        let config = self.config_snapshot()?;
        let mut result = UserAggregatorConfig::default();
        let active_vad = self
            .sender()
            .map(|s| s.active_settings().enable_vad)
            .unwrap_or(config.settings.enable_vad);
        if active_vad {
            result.turn.enable_interruptions = config.should_interrupt;
            result.turn.start_on_transcription = false;
            result.turn.require_model_completion = false;
            result.turn.start_strategies = Some(
                UserTurnStartStrategies::new(vec![
                    UserTurnStartStrategy::external().with_interruptions(config.should_interrupt)
                ])
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
        Ok(result)
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
    fn audio(
        &self,
        audio: &AudioRawFrame,
        id: &str,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !ctx.is_usable() || self.muted.load(Ordering::Acquire) || audio.audio.is_empty() {
            return Ok(());
        }
        if id.len() > 1024 {
            return Err(processor_error(GladiaError::TooLarge));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| processor_error(GladiaError::Closed))?;
        if state.ending {
            return Ok(());
        }
        state
            .sender
            .as_ref()
            .ok_or_else(|| processor_error(GladiaError::Closed))?
            .try_send_audio(audio.clone())
            .map_err(processor_error)?;
        state.user_id = id.into();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for GladiaSttProcessor {
    fn name(&self) -> &str {
        "GladiaSTTService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !ctx.is_usable() {
            return Ok(());
        }
        let health_generation = ctx.health().generation();
        let mut health = ctx.health().watch();
        let _setup = tokio::select! {biased;_=health.changed()=>return Ok(()),lock=self.setup_gate.lock()=>lock};
        let (serial, client) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(GladiaError::Closed))?;
            if state.sender.as_ref().is_some_and(|s| !s.is_closed()) {
                return Err(processor_error(GladiaError::Invalid(
                    "processor already connected",
                )));
            }
            if let Some(sender) = state.sender.take() {
                state.config = sender.config_snapshot().map_err(processor_error)?;
            }
            if start.audio_in_sample_rate != state.config.sample_rate {
                return Err(processor_error(GladiaError::Invalid(
                    "pipeline sample rate",
                )));
            }
            state.generation = state
                .generation
                .checked_add(1)
                .ok_or_else(|| processor_error(GladiaError::TooLarge))?;
            state.ending = false;
            (
                state.generation,
                GladiaSttClient::new(state.config.clone()).map_err(processor_error)?,
            )
        };
        let mut session = tokio::select! {biased;_=health.changed()=>return Ok(()),result=client.connect()=>result.map_err(processor_error)?};
        if !ctx.is_usable() || ctx.health().generation() != health_generation {
            session.cancel();
            return Ok(());
        }
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(GladiaError::Closed))?;
            if state.ending || state.generation != serial {
                session.cancel();
                return Ok(());
            }
            let sender = session.sender();
            // Settings accepted while REST/WS setup was in flight are future
            // settings, not a replacement for the active wire snapshot.
            let future = serde_json::to_value(&state.config.settings)
                .map_err(|_| processor_error(GladiaError::Invalid("settings")))?;
            sender
                .merge_settings(future.as_object().unwrap())
                .map_err(processor_error)?;
            state.sender = Some(sender);
            state.user_id = self.user_id.clone();
        }
        let output = ctx.clone();
        let state = self.state.clone();
        let muted = self.muted.clone();
        let emission = self.emission.clone();
        ctx.spawn("gladia-stt-events",false,async move {
            loop {
                let event=tokio::select!{biased;_=health.changed()=>{session.cancel();return Ok(())},event=session.next()=>event};
                let _guard=emission.lock().await;
                let (current,ending,user_id)={let s=state.lock().map_err(|_|processor_error(GladiaError::Closed))?;(s.generation==serial,s.ending,s.user_id.clone())};
                if !current||!output.is_usable()||output.health().generation()!=health_generation{session.cancel();return Ok(())}
                if ending||muted.load(Ordering::Acquire){match event{None|Some(Err(_))=>return Ok(()),_=>continue}}
                match event {
                    Some(Ok(GladiaSttEvent::Transcript(t)))=>{
                        if t.is_final&&output.configuration().enable_usage_metrics {output.push(Frame::new(FrameData::Metrics(vec![Metric::SttUsage{processor:"GladiaSTTService".into(),audio_seconds:t.audio_seconds}])),Direction::Downstream).await?;}
                        let data=DetailedTranscriptionFrame{text:TextFrame::new(t.text),user_id,timestamp:chrono::Utc::now().to_rfc3339(),language:t.language,result:Some(t.result),finalized:t.is_final};
                        let frame=if t.is_final{FrameData::TranscriptionDetailed(data)}else{FrameData::InterimTranscriptionDetailed(data)};output.push(Frame::new(frame),Direction::Downstream).await?;
                    },
                    Some(Ok(GladiaSttEvent::Translation(t)))=>{output.push(Frame::new(FrameData::TranslationDetailed(DetailedTranscriptionFrame{text:TextFrame::new(t.text),user_id:String::new(),timestamp:chrono::Utc::now().to_rfc3339(),language:Some(t.language),result:Some(t.result),finalized:true})),Direction::Downstream).await?;},
                    Some(Ok(GladiaSttEvent::TurnStarted))=>output.broadcast(Frame::new(FrameData::ProposedUserStartedSpeaking)).await?,
                    Some(Ok(GladiaSttEvent::TurnStopped))=>output.broadcast(Frame::new(FrameData::ProposedUserStoppedSpeaking)).await?,
                    Some(Ok(GladiaSttEvent::Error(error)))=>{output.push_error_if_health_generation(error_frame(error),false,health_generation).await?;},
                    Some(Ok(_))=>(),
                    Some(Err(GladiaError::Cancelled))|None=>return Ok(()),
                    Some(Err(error))=>{output.push_error_if_health_generation(error_frame(error),false,health_generation).await?;return Ok(())},
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
                self.state
                    .lock()
                    .map_err(|_| processor_error(GladiaError::Closed))?
                    .ending = true;
                if let Ok(sender) = self.sender() {
                    sender.cancel();
                }
            }
            FrameData::SttMute(value) => {
                let _guard = self.emission.lock().await;
                self.muted.store(*value, Ordering::Release);
            }
            FrameData::SttUpdateSettings(update) if self.targets(&update.service) => {
                let mut delta: Map<String, Value> = update.settings.clone().into_iter().collect();
                if let Some(typed) = &update.delta {
                    delta = typed.base.extra.clone().into_iter().collect();
                    add(&mut delta, "model", &typed.base.model);
                    add(&mut delta, "language", &typed.language);
                }
                self.merge_settings(&delta)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            FrameData::ServiceUpdateSettings(update) if self.targets(&update.service) => {
                let mut delta: Map<String, Value> = update.settings.clone().into_iter().collect();
                if let Some(typed) = &update.delta {
                    delta = typed.extra.clone().into_iter().collect();
                    add(&mut delta, "model", &typed.model);
                }
                self.merge_settings(&delta)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            FrameData::Start(_) if direction == Direction::Downstream => {
                let c = self
                    .state
                    .lock()
                    .map_err(|_| processor_error(GladiaError::Closed))?
                    .config
                    .clone();
                let active_vad = self
                    .sender()
                    .map(|s| s.active_settings().enable_vad)
                    .unwrap_or(c.settings.enable_vad);
                let strategies = if active_vad {
                    let strategy = UserTurnStrategy::External {
                        enable_interruptions: c.should_interrupt,
                    };
                    Some(UserTurnStrategies {
                        start: Some(vec![strategy.clone()]),
                        stop: Some(vec![strategy]),
                    })
                } else {
                    None
                };
                ctx.push(
                    Frame::new(FrameData::SttMetadata(SttMetadataFrame {
                        base: ServiceMetadataFrame {
                            service_name: self.name().into(),
                            user_turn_strategies: strategies,
                        },
                        ttfs_p99_latency: self.ttfs_p99_latency,
                    })),
                    direction,
                )
                .await?;
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
            _ => (),
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        {
            let _guard = self.emission.lock().await;
            self.state
                .lock()
                .map_err(|_| processor_error(GladiaError::Closed))?
                .ending = true;
        }
        if let Ok(sender) = self.sender() {
            match sender.finish().await {
                Ok(()) | Err(GladiaError::Closed | GladiaError::Cancelled) => (),
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
            .map_err(|_| processor_error(GladiaError::Closed))?;
        state.ending = true;
        if let Some(sender) = state.sender.take() {
            state.config = sender.config_snapshot().map_err(processor_error)?;
            sender.cancel();
        }
        Ok(())
    }
}
fn add(values: &mut Map<String, Value>, name: &str, setting: &Setting<String>) {
    match setting {
        Setting::NotGiven => (),
        Setting::Null => {
            values.insert(name.into(), Value::Null);
        }
        Setting::Value(v) => {
            values.insert(name.into(), Value::String(v.clone()));
        }
    }
}
fn error_frame(error: GladiaError) -> ErrorFrame {
    let category = match error {
        GladiaError::Http(status) => ErrorCategory::from_http_status(status),
        GladiaError::Invalid(_) | GladiaError::Endpoint => ErrorCategory::InvalidRequest,
        GladiaError::Transport | GladiaError::Timeout(_) | GladiaError::Closed => {
            ErrorCategory::Connectivity
        }
        _ => ErrorCategory::Application,
    };
    ErrorFrame {
        error: error.to_string(),
        category,
        ..ErrorFrame::new("")
    }
}
fn processor_error(error: GladiaError) -> ProcessorError {
    let error = error_frame(error);
    ProcessorError::new(error.error).with_category(error.category)
}
