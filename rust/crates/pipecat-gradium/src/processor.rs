// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{GradiumError, GradiumSttClient, GradiumSttConfig, GradiumSttEvent, GradiumSttSender};
use pipecat_core::{
    async_trait, AudioRawFrame, DetailedTranscriptionFrame, Direction, ErrorCategory, ErrorFrame,
    Frame, FrameData, FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorHealthGeneration, ProcessorReference, ServiceMetadataFrame, Setting, StartFrame,
    SttMetadataFrame, TextFrame, UserTurnStrategies, UserTurnStrategy,
};
use serde_json::{Map, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
struct State {
    sender: Option<GradiumSttSender>,
    config: GradiumSttConfig,
    generation: u64,
    health_generation: Option<ProcessorHealthGeneration>,
    user_id: String,
    ending: bool,
}
/// Native Gradium recognition, provider turn proposals and STT metadata.
/// Terminal drain waits for EOS and consumes final events before forwarding End.
/// Settings tied to a connection require an explicit new session; only turn
/// threshold/horizon/cooldown updates are accepted on a running stream.
pub struct GradiumSttProcessor {
    state: Arc<Mutex<State>>,
    health: ProcessorHealth,
    muted: Arc<AtomicBool>,
    emission: Arc<tokio::sync::Mutex<()>>,
    setup_gate: tokio::sync::Mutex<()>,
    user_id: String,
    audio_passthrough: bool,
    ttfs_p99_latency: f64,
}
impl GradiumSttProcessor {
    pub fn new(client: GradiumSttClient) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                sender: None,
                config: client.config().clone(),
                generation: 0,
                health_generation: None,
                user_id: String::new(),
                ending: false,
            })),
            health: Default::default(),
            muted: Arc::new(AtomicBool::new(false)),
            emission: Arc::new(tokio::sync::Mutex::new(())),
            setup_gate: tokio::sync::Mutex::new(()),
            user_id: String::new(),
            audio_passthrough: true,
            ttfs_p99_latency: 0.62,
        }
    }
    pub fn with_user_id(mut self, id: impl Into<String>) -> Result<Self, GradiumError> {
        let id = id.into();
        if id.len() > 1024 {
            return Err(GradiumError::TooLarge);
        }
        self.user_id = id;
        Ok(self)
    }
    pub fn with_audio_passthrough(mut self, value: bool) -> Self {
        self.audio_passthrough = value;
        self
    }
    pub fn with_ttfs_p99_latency(mut self, value: f64) -> Result<Self, GradiumError> {
        if !value.is_finite() || !(0.0..=300.0).contains(&value) {
            return Err(GradiumError::Invalid("TTFS latency"));
        }
        self.ttfs_p99_latency = value;
        Ok(self)
    }
    pub fn sender(&self) -> Result<GradiumSttSender, GradiumError> {
        self.state
            .lock()
            .map_err(|_| GradiumError::Closed)?
            .sender
            .clone()
            .ok_or(GradiumError::Closed)
    }
    pub fn config_snapshot(&self) -> Result<GradiumSttConfig, GradiumError> {
        let state = self.state.lock().map_err(|_| GradiumError::Closed)?;
        match &state.sender {
            Some(sender) => sender.config_snapshot(),
            None => Ok(state.config.clone()),
        }
    }
    pub fn merge_settings(&self, delta: &Map<String, Value>) -> Result<(), GradiumError> {
        let mut state = self.state.lock().map_err(|_| GradiumError::Closed)?;
        if let Some(sender) = &state.sender {
            sender.merge_settings(delta)?;
            state.config = sender.config_snapshot()?
        } else {
            let mut next = state.config.clone();
            next.settings = next.settings.merge(delta)?;
            next.validate()?;
            if state.generation > 0 && !state.config.settings.same_connection(&next.settings) {
                return Err(GradiumError::RequiresRestart);
            }
            state.config = next
        }
        Ok(())
    }
    fn metadata(&self) -> Result<Frame, ProcessorError> {
        let config = self.config_snapshot().map_err(processor_error)?;
        let strategies = if config.enable_turn_detection {
            let strategy = UserTurnStrategy::External {
                enable_interruptions: config.enable_interruptions,
            };
            Some(UserTurnStrategies {
                start: Some(vec![strategy.clone()]),
                stop: Some(vec![strategy]),
            })
        } else {
            None
        };
        Ok(Frame::new(FrameData::SttMetadata(SttMetadataFrame {
            base: ServiceMetadataFrame {
                service_name: self.name().into(),
                user_turn_strategies: strategies,
            },
            ttfs_p99_latency: self.ttfs_p99_latency,
        })))
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
            return Err(processor_error(GradiumError::TooLarge));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| processor_error(GradiumError::Closed))?;
        if state.ending {
            return Ok(());
        }
        // One connection has one speaker. Reassigning a speaker while decoder
        // text is pending would misattribute the old speaker's transcript.
        if state.user_id != id {
            return Err(processor_error(GradiumError::Invalid(
                "speaker changed within session",
            )));
        }
        state
            .sender
            .as_ref()
            .ok_or_else(|| processor_error(GradiumError::Closed))?
            .try_send_audio(audio.clone())
            .map_err(processor_error)?;
        state.user_id = id.into();
        Ok(())
    }
    async fn cancel_current(&self) -> Result<(), ProcessorError> {
        let _guard = self.emission.lock().await;
        let mut state = self
            .state
            .lock()
            .map_err(|_| processor_error(GradiumError::Closed))?;
        state.ending = true;
        if let Some(sender) = state.sender.take() {
            state.config = sender.config_snapshot().map_err(processor_error)?;
            sender.cancel()
        }
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for GradiumSttProcessor {
    fn name(&self) -> &str {
        "GradiumSTTService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !ctx.is_usable() || self.muted.load(Ordering::Acquire) {
            return Ok(());
        }
        let health_generation = ctx.health().generation();
        let mut health = ctx.health().watch();
        let _setup = tokio::select! {biased;_=health.changed()=>return Ok(()),lock=self.setup_gate.lock()=>lock};
        let (serial, client) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(GradiumError::Closed))?;
            if let Some(sender) = &state.sender {
                if !sender.is_closed() {
                    if state.health_generation == Some(health_generation) {
                        return Err(processor_error(GradiumError::Invalid(
                            "processor already connected",
                        )));
                    }
                    sender.cancel();
                }
            }
            if let Some(sender) = state.sender.take() {
                state.config = sender.config_snapshot().map_err(processor_error)?
            }
            if start.audio_in_sample_rate != state.config.sample_rate {
                return Err(processor_error(GradiumError::Invalid(
                    "pipeline sample rate",
                )));
            }
            state.generation = state
                .generation
                .checked_add(1)
                .ok_or_else(|| processor_error(GradiumError::TooLarge))?;
            state.ending = false;
            state.health_generation = Some(health_generation);
            (
                state.generation,
                GradiumSttClient::new(state.config.clone()).map_err(processor_error)?,
            )
        };
        let mut session = tokio::select! {biased;_=health.changed()=>return Ok(()),r=client.connect()=>r.map_err(processor_error)?};
        if !ctx.is_usable() || ctx.health().generation() != health_generation {
            session.cancel();
            return Ok(());
        }
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(GradiumError::Closed))?;
            if state.ending || state.generation != serial {
                session.cancel();
                return Ok(());
            }
            let sender = session.sender();
            let current = &state.config.settings.turn;
            let delta = serde_json::json!({"eot_horizon_s":current.eot_horizon_s,"eot_threshold":current.eot_threshold,"post_flush_cooldown_frames":current.post_flush_cooldown_frames});
            sender
                .merge_settings(delta.as_object().unwrap())
                .map_err(processor_error)?;
            state.sender = Some(sender);
            state.user_id = self.user_id.clone();
        }
        let output = ctx.clone();
        let state = self.state.clone();
        let muted = self.muted.clone();
        let emission = self.emission.clone();
        ctx.spawn("gradium-stt-events",false,async move{
            loop{
                let event=tokio::select!{biased;_=health.changed()=>{session.cancel();return Ok(())},event=session.next()=>event};
                let _guard=emission.lock().await;
                let(current,ending,user_id)={let s=state.lock().map_err(|_|processor_error(GradiumError::Closed))?;(s.generation==serial,s.ending,s.user_id.clone())};
                if !current||ending||!output.is_usable()||output.health().generation()!=health_generation||muted.load(Ordering::Acquire){session.cancel();return Ok(())}
                match event{
                    Some(Ok(GradiumSttEvent::Transcript(t)))=>{
                        if t.is_final&&output.configuration().enable_usage_metrics{output.push(Frame::new(FrameData::Metrics(vec![Metric::SttUsage{processor:"GradiumSTTService".into(),audio_seconds:t.audio_seconds}])),Direction::Downstream).await?;}
                        let data=DetailedTranscriptionFrame{text:TextFrame::new(t.text),user_id,timestamp:chrono::Utc::now().to_rfc3339(),language:t.language,result:None,finalized:t.is_final};
                        let data=if t.is_final{FrameData::TranscriptionDetailed(data)}else{FrameData::InterimTranscriptionDetailed(data)};output.push(Frame::new(data),Direction::Downstream).await?;
                    },
                    Some(Ok(GradiumSttEvent::TurnStarted))=>output.broadcast(Frame::new(FrameData::ProposedUserStartedSpeaking)).await?,
                    Some(Ok(GradiumSttEvent::TurnStopped))=>output.broadcast(Frame::new(FrameData::ProposedUserStoppedSpeaking)).await?,
                    Some(Ok(_))=>(),Some(Err(GradiumError::Cancelled))|None=>return Ok(()),
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
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => self.cancel_current().await?,
            FrameData::SttMute(value) => {
                if self.muted.swap(*value, Ordering::AcqRel) != *value {
                    if *value {
                        self.cancel_current().await?
                    } else {
                        self.setup(ctx.configuration(), ctx).await?
                    }
                }
            }
            FrameData::SttUpdateSettings(update) if self.targets(&update.service) => {
                let mut delta: Map<String, Value> = update.settings.clone().into_iter().collect();
                if let Some(typed) = &update.delta {
                    delta = typed.base.extra.clone().into_iter().collect();
                    add(&mut delta, "model", &typed.base.model);
                    add(&mut delta, "language", &typed.language)
                }
                self.merge_settings(&delta)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            FrameData::ServiceUpdateSettings(update) if self.targets(&update.service) => {
                let mut delta: Map<String, Value> = update.settings.clone().into_iter().collect();
                if let Some(typed) = &update.delta {
                    delta = typed.extra.clone().into_iter().collect();
                    add(&mut delta, "model", &typed.model)
                }
                self.merge_settings(&delta)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            FrameData::Start(_) if direction == Direction::Downstream => {
                ctx.push(self.metadata()?, direction).await?
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
                if ctx.is_usable()
                    && !self.muted.load(Ordering::Acquire)
                    && !self
                        .config_snapshot()
                        .map_err(processor_error)?
                        .enable_turn_detection =>
            {
                self.sender()
                    .and_then(|s| s.try_flush())
                    .map_err(|e| processor_error(e).with_permanence_override(false))?
            }
            _ => (),
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Ok(sender) = self.sender() {
            match sender.finish().await {
                Ok(()) | Err(GradiumError::Cancelled) => (),
                Err(e) => return Err(processor_error(e)),
            }
        }
        // The runtime drains the owned event task before forwarding End/Stop.
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cancel_current().await
    }
}
fn add(values: &mut Map<String, Value>, key: &str, setting: &Setting<String>) {
    match setting {
        Setting::NotGiven => (),
        Setting::Null => {
            values.insert(key.into(), Value::Null);
        }
        Setting::Value(v) => {
            values.insert(key.into(), Value::String(v.clone()));
        }
    }
}
fn error_frame(error: GradiumError) -> ErrorFrame {
    let category = match error {
        GradiumError::Http(status) => ErrorCategory::from_http_status(status),
        GradiumError::Invalid(_) | GradiumError::Endpoint | GradiumError::RequiresRestart => {
            ErrorCategory::InvalidRequest
        }
        GradiumError::Transport | GradiumError::Timeout(_) | GradiumError::Closed => {
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
fn processor_error(error: GradiumError) -> ProcessorError {
    let error = error_frame(error);
    ProcessorError::new(error.error).with_category(error.category)
}
