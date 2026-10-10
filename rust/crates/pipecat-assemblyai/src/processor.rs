// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{AssemblyAiClient, AssemblyAiEvent, AssemblyAiSender, Encoding, StreamingError};
use pipecat_core::{
    async_trait, Direction, ErrorCategory, ErrorFrame, Frame, FrameData, FrameProcessor, Metric,
    ProcessorContext, ProcessorError, ProcessorHealth, ProcessorReference, ServiceMetadataFrame,
    ServiceSettings, Setting, StartFrame, SttMetadataFrame, SttSettings, TranscriptionFrame,
    UserTurnStrategies, UserTurnStrategy,
};
use serde_json::{Map, Value};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

struct State {
    user_id: String,
    sender: Option<AssemblyAiSender>,
    rate: u32,
}
/// Native frame adapter. The pipeline owns End/Cancel; provider termination is
/// observed without injecting a second terminal frame or an interruption.
pub struct AssemblyAiSttProcessor {
    client: AssemblyAiClient,
    state: Arc<Mutex<State>>,
    health: ProcessorHealth,
    mute: AtomicBool,
    audio_passthrough: bool,
    user_id: String,
}
impl AssemblyAiSttProcessor {
    pub fn new(client: AssemblyAiClient) -> Self {
        Self {
            client,
            state: Arc::new(Mutex::new(State {
                user_id: String::new(),
                sender: None,
                rate: 16000,
            })),
            health: ProcessorHealth::default(),
            mute: AtomicBool::new(false),
            audio_passthrough: true,
            user_id: String::new(),
        }
    }
    pub fn with_user_id(mut self, value: impl Into<String>) -> Self {
        self.user_id = value.into();
        self
    }
    pub fn with_audio_passthrough(mut self, value: bool) -> Self {
        self.audio_passthrough = value;
        self
    }
    pub fn sender(&self) -> Result<AssemblyAiSender, StreamingError> {
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
    async fn accept_audio(
        &self,
        audio: &pipecat_core::AudioRawFrame,
        user_id: &str,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !ctx.is_usable() || self.mute.load(Ordering::Acquire) || audio.audio.is_empty() {
            return Ok(());
        }
        let rate = self
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?
            .rate;
        audio
            .validate()
            .map_err(|_| processor_error(StreamingError::PartialSample))?;
        if audio.sample_rate != rate || audio.num_channels != 1 || user_id.len() > 1024 {
            return Err(processor_error(StreamingError::Invalid(
                "AssemblyAI requires configured-rate mono PCM and bounded identity",
            )));
        }
        if audio.audio.len() > self.client.config().limits.max_message_bytes {
            return Err(processor_error(StreamingError::TooLarge));
        }
        let bytes = if self.client.config().encoding == Encoding::PcmMulaw {
            pipecat_audio::pcm_to_ulaw(&audio.audio)
                .map_err(|_| processor_error(StreamingError::PartialSample))?
        } else {
            audio.audio.to_vec()
        };
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            state
                .sender
                .as_ref()
                .ok_or_else(|| processor_error(StreamingError::Closed))?
                .try_send_audio(bytes)
                .map_err(processor_error)?;
            state.user_id = user_id.into();
        }
        if ctx.configuration().enable_usage_metrics {
            ctx.push(
                Frame::new(FrameData::Metrics(vec![Metric::SttUsage {
                    processor: self.name().into(),
                    audio_seconds: audio.duration_seconds(),
                }])),
                Direction::Downstream,
            )
            .await?;
        }
        Ok(())
    }
    fn metadata(&self) -> Frame {
        let strategies = if self.client.config().vad_force_turn_endpoint {
            None
        } else {
            let strategy = UserTurnStrategy::External {
                enable_interruptions: self.client.config().should_interrupt,
            };
            Some(UserTurnStrategies {
                start: Some(vec![strategy.clone()]),
                stop: Some(vec![strategy]),
            })
        };
        Frame::new(FrameData::SttMetadata(SttMetadataFrame {
            base: ServiceMetadataFrame {
                service_name: self.name().into(),
                user_turn_strategies: strategies,
            },
            ttfs_p99_latency: self.client.config().ttfs_p99_latency,
        }))
    }
}
#[async_trait]
impl FrameProcessor for AssemblyAiSttProcessor {
    fn name(&self) -> &str {
        "AssemblyAISTTService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut config = self.client.config().clone();
        let rate = config.sample_rate.unwrap_or(start.audio_in_sample_rate);
        config.sample_rate = Some(rate);
        let mut session = AssemblyAiClient::new(config)
            .map_err(processor_error)?
            .connect()
            .await
            .map_err(processor_error)?;
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            if let Some(old) = state.sender.take() {
                old.cancel();
            }
            state.sender = Some(session.sender());
            state.rate = rate;
            state.user_id = self.user_id.clone();
        }
        let output = ctx.clone();
        let shared_state = self.state.clone();
        let generation = self.health.generation();
        let mut health = self.health.watch();
        ctx.spawn("assemblyai-transcripts", false, async move {
            loop {
                let event = tokio::select! { biased; _ = health.changed() => { session.cancel(); return Ok(()); }, event = session.next() => event };
                if !output.is_usable() || output.health().generation() != generation { session.cancel(); return Ok(()); }
                match event {
                    Some(Ok(event)) => { let user_id = shared_state.lock().map_err(|_| processor_error(StreamingError::Closed))?.user_id.clone(); tokio::select! { biased; _ = health.changed() => { session.cancel(); return Ok(()); }, result = emit(event, &output, &user_id) => result? } },
                    Some(Err(StreamingError::Cancelled)) | None => return Ok(()),
                    Some(Err(error)) => { output.push_error(error_frame(error)).await?; return Ok(()); }
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
        // VAD and assistant-turn frames may originate downstream and travel
        // upstream to this service, as in the source pipeline.
        if matches!(
            &frame.data,
            FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            if let Ok(sender) = self.sender() {
                sender.cancel();
            }
        }
        match &frame.data {
            FrameData::Start(_) if direction == Direction::Downstream => {
                ctx.push(self.metadata(), Direction::Downstream).await?;
            }
            FrameData::SttMute(mute) => self.mute.store(*mute, Ordering::Release),
            FrameData::SttUpdateSettings(update) if self.targets(&update.service) => {
                let values = match &update.delta {
                    Some(delta) => typed_delta(delta),
                    None => update.settings.clone().into_iter().collect(),
                };
                self.sender()
                    .map_err(processor_error)?
                    .try_update_settings(values)
                    .map_err(processor_error)?;
                return Ok(());
            }
            FrameData::ServiceUpdateSettings(update) if self.targets(&update.service) => {
                let values = match &update.delta {
                    Some(delta) => base_delta(delta),
                    None => update.settings.clone().into_iter().collect(),
                };
                self.sender()
                    .map_err(processor_error)?
                    .try_update_settings(values)
                    .map_err(processor_error)?;
                return Ok(());
            }
            FrameData::LlmContextAssistantTurn(turn) if ctx.is_usable() => {
                self.sender()
                    .map_err(processor_error)?
                    .assistant_turn(&turn.text)
                    .map_err(processor_error)?;
            }
            FrameData::VadUserStartedSpeaking(_) | FrameData::VadUserStartedSpeakingDetailed(_)
                if ctx.is_usable() =>
            {
                self.sender()
                    .map_err(processor_error)?
                    .start_utterance()
                    .map_err(processor_error)?;
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_)
                if ctx.is_usable() =>
            {
                self.sender()
                    .map_err(processor_error)?
                    .try_finalize()
                    .map_err(processor_error)?;
            }
            FrameData::InputAudio(audio) if direction == Direction::Downstream => {
                self.accept_audio(audio, &self.user_id, ctx).await?;
                if !self.audio_passthrough {
                    return Ok(());
                }
            }
            FrameData::UserAudio(user) if direction == Direction::Downstream => {
                self.accept_audio(&user.audio, &user.user_id, ctx).await?;
                if !self.audio_passthrough {
                    return Ok(());
                }
            }
            _ => (),
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Ok(sender) = self.sender() {
            match sender.finish().await {
                Ok(()) | Err(StreamingError::Closed | StreamingError::Cancelled) => (),
                Err(error) => return Err(processor_error(error)),
            }
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let Some(sender) = self
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?
            .sender
            .take()
        {
            sender.cancel();
        }
        Ok(())
    }
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
fn typed_delta(value: &SttSettings) -> Map<String, Value> {
    let mut values = base_delta(&value.base);
    setting(&mut values, "language", &value.language);
    values
}
async fn emit(
    event: AssemblyAiEvent,
    ctx: &ProcessorContext,
    user_id: &str,
) -> Result<(), ProcessorError> {
    match event {
        AssemblyAiEvent::Transcript(t) => {
            let value = TranscriptionFrame {
                text: t.text,
                user_id: t
                    .speaker
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| user_id.into()),
                timestamp: chrono::Utc::now().to_rfc3339(),
                language: Some(t.language),
                result: Some(t.result),
                finalized: t.finalized,
            };
            ctx.push(
                Frame::new(if t.is_final {
                    FrameData::Transcription(value)
                } else {
                    FrameData::InterimTranscription(value)
                }),
                Direction::Downstream,
            )
            .await
        }
        AssemblyAiEvent::ProposedUserStartedSpeaking
        | AssemblyAiEvent::ProposedUserStoppedSpeaking => {
            let data = if matches!(event, AssemblyAiEvent::ProposedUserStartedSpeaking) {
                FrameData::ProposedUserStartedSpeaking
            } else {
                FrameData::ProposedUserStoppedSpeaking
            };
            ctx.push(Frame::new(data.clone()), Direction::Downstream)
                .await?;
            ctx.push(Frame::new(data), Direction::Upstream).await
        }
        AssemblyAiEvent::SettingsRejected(error) => {
            ctx.push_error_with_permanence(error_frame(error), false)
                .await
        }
        _ => Ok(()),
    }
}
fn processor_error(error: StreamingError) -> ProcessorError {
    let frame = error_frame(error);
    ProcessorError::new(frame.error)
        .with_category(frame.category)
        .with_permanence_override(
            frame.category.is_permanent() && frame.category != ErrorCategory::InvalidRequest,
        )
}
fn error_frame(error: StreamingError) -> ErrorFrame {
    let category = match &error {
        StreamingError::Http(status) => ErrorCategory::from_http_status(*status),
        StreamingError::Invalid(_) | StreamingError::TooLarge | StreamingError::PartialSample => {
            ErrorCategory::InvalidRequest
        }
        StreamingError::Protocol(_) => ErrorCategory::Server,
        _ => ErrorCategory::Connectivity,
    };
    ErrorFrame {
        category,
        ..ErrorFrame::new(error.to_string())
    }
}
