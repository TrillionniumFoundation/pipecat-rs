// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::*;
use pipecat_core::{
    async_trait, AudioRawFrame, DetailedTranscriptionFrame, Direction, ErrorCategory, ErrorFrame,
    Frame, FrameData, FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorReference, ServiceMetadataFrame, Setting, StartFrame, SttMetadataFrame, TextFrame,
};
use pipecat_turns::{
    UserAggregatorConfig, UserTurnStartStrategies, UserTurnStartStrategy, UserTurnStopStrategies,
    UserTurnStopStrategy,
};
use serde_json::{Map, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::broadcast;
struct State {
    sender: Option<SpeechmaticsSttSender>,
    connect_cancel: Option<CancellationToken>,
    config: SpeechmaticsSttConfig,
    serial: u64,
    health_generation: Option<pipecat_core::ProcessorHealthGeneration>,
    ending: bool,
    muted: bool,
}
struct Inner {
    state: Mutex<State>,
    health: ProcessorHealth,
    emission: tokio::sync::Mutex<()>,
    setup: tokio::sync::Mutex<()>,
    speakers: broadcast::Sender<SpeakersResult>,
}
/// Persistent STT with explicit turn strategy ownership. Mute cancels the old
/// session; unmute explicitly starts a fresh one, without audio replay.
#[derive(Clone)]
pub struct SpeechmaticsSttProcessor {
    inner: Arc<Inner>,
    audio_passthrough: bool,
}
impl SpeechmaticsSttProcessor {
    pub fn new(client: SpeechmaticsSttClient) -> Self {
        let (speakers, _) = broadcast::channel(client.config().limits.queue_capacity);
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    sender: None,
                    connect_cancel: None,
                    config: client.config().clone(),
                    serial: 0,
                    health_generation: None,
                    ending: false,
                    muted: false,
                }),
                health: ProcessorHealth::default(),
                emission: tokio::sync::Mutex::new(()),
                setup: tokio::sync::Mutex::new(()),
                speakers,
            }),
            audio_passthrough: true,
        }
    }
    pub fn with_audio_passthrough(mut self, value: bool) -> Self {
        self.audio_passthrough = value;
        self
    }
    /// Bounded subscription; slow subscribers receive Tokio's explicit Lagged error.
    pub fn subscribe_speakers(&self) -> broadcast::Receiver<SpeakersResult> {
        self.inner.speakers.subscribe()
    }
    pub fn sender(&self) -> Result<SpeechmaticsSttSender, StreamingError> {
        self.inner
            .state
            .lock()
            .map_err(|_| StreamingError::Closed)?
            .sender
            .clone()
            .ok_or(StreamingError::Closed)
    }
    pub fn config_snapshot(&self) -> Result<SpeechmaticsSttConfig, StreamingError> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| StreamingError::Closed)?;
        let mut config = state.config.clone();
        if let Some(sender) = &state.sender {
            config.settings = sender.settings_snapshot()?;
        }
        Ok(config)
    }
    /// Apply this snapshot when constructing the user aggregator. Provider
    /// frames never replace caller-selected strategies or interruption policy.
    pub fn recommended_user_config(&self) -> UserAggregatorConfig {
        let config = self
            .inner
            .state
            .lock()
            .expect("processor state")
            .config
            .clone();
        let mut result = UserAggregatorConfig::default();
        result.turn.enable_interruptions = config.should_interrupt;
        if config.settings.turn_detection_mode == TurnDetectionMode::Vad {
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
        result
    }
    fn update(&self, delta: &Map<String, Value>) -> Result<(), StreamingError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| StreamingError::Closed)?;
        if state.ending {
            return Err(StreamingError::Closed);
        }
        if let Some(sender) = &state.sender {
            sender.merge_settings(delta)?;
            state.config.settings = sender.settings_snapshot()?;
        } else {
            let next = state.config.settings.merge(delta)?;
            if next.turn_detection_mode != state.config.settings.turn_detection_mode {
                return Err(StreamingError::RequiresRestart);
            }
            if state.connect_cancel.is_some() {
                return Err(StreamingError::RequiresRestart);
            }
            let mut config = state.config.clone();
            config.settings = next;
            config.validate()?;
            state.config = config;
        }
        Ok(())
    }
    async fn start_session(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if ctx.configuration().audio_in_sample_rate != 16000 {
            return Err(processor_error(StreamingError::Invalid(
                "pipeline input must be 16 kHz",
            )));
        }
        if !ctx.is_usable() {
            return Ok(());
        }
        let generation = ctx.health().generation();
        let mut health = ctx.health().watch();
        let timeout = self
            .config_snapshot()
            .map_err(processor_error)?
            .limits
            .connect_timeout;
        let _setup = tokio::select! {biased;_=health.changed()=>return Ok(()),lock=tokio::time::timeout(timeout,self.inner.setup.lock())=>lock.map_err(|_|processor_error(StreamingError::Timeout("processor setup")))?};
        let (serial, config, cancel) = {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            if state.ending || state.muted {
                return Ok(());
            }
            if state.sender.as_ref().is_some_and(|s| !s.is_closed()) {
                return Ok(());
            }
            if let Some(old) = state.sender.take() {
                state.config.settings = old.settings_snapshot().map_err(processor_error)?;
                old.cancel();
            }
            state.serial = state
                .serial
                .checked_add(1)
                .ok_or_else(|| ProcessorError::fatal("STT session generation exhausted"))?;
            let cancel = CancellationToken::new();
            state.connect_cancel = Some(cancel.clone());
            state.health_generation = Some(generation);
            (state.serial, state.config.clone(), cancel)
        };
        let client = SpeechmaticsSttClient::new(config.clone()).map_err(processor_error)?;
        let connected = tokio::select! {biased;_=health.changed()=>{cancel.cancel();return Ok(())},value=client.connect_with_cancel(cancel.clone())=>value};
        let mut session = match connected {
            Ok(s) => s,
            Err(StreamingError::Cancelled) => return Ok(()),
            Err(error) => {
                let mut state = self
                    .inner
                    .state
                    .lock()
                    .map_err(|_| processor_error(StreamingError::Closed))?;
                if state.serial != serial
                    || state.ending
                    || state.muted
                    || !ctx.is_usable()
                    || ctx.health().generation() != generation
                {
                    return Ok(());
                }
                state.connect_cancel = None;
                return Err(if matches!(&error, StreamingError::Provider { .. }) {
                    processor_error(error).with_permanence_override(true)
                } else {
                    processor_error(error)
                });
            }
        };
        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| processor_error(StreamingError::Closed))?;
            if state.serial != serial
                || state.ending
                || state.muted
                || !ctx.is_usable()
                || ctx.health().generation() != generation
            {
                session.cancel();
                return Ok(());
            }
            state.connect_cancel = None;
            state.sender = Some(session.sender());
        }
        let output = ctx.clone();
        let inner = self.inner.clone();
        let mode = config.settings.turn_detection_mode;
        ctx.spawn("speechmatics-stt-events",false,async move{
            let mut processing=None::<tokio::time::Instant>;
            let mut usage_seconds=0.0;
            loop{
                let event=tokio::select!{biased;_=health.changed()=>{session.cancel();return Ok(())},value=session.next()=>value};
                let _emission=tokio::select!{biased;_=cancel.cancelled()=>return Ok(()),_=health.changed()=>{session.cancel();return Ok(())},value=tokio::time::timeout(config.limits.output_timeout,inner.emission.lock())=>value.map_err(|_|processor_error(StreamingError::Timeout("processor emission")))?};
                let(current,allowed,turn_epoch)={let state=inner.state.lock().map_err(|_|processor_error(StreamingError::Closed))?;(state.serial==serial,!state.ending&&!state.muted,state.sender.as_ref().map(|s|s.turn_generation()).unwrap_or(0))};
                if !output.is_usable()||output.health().generation()!=generation{session.cancel();return Ok(());}
                if !current{session.cancel();return Ok(());}
                if !allowed&&matches!(event,Some(Ok(_))){continue;}
                let terminal=matches!(event,None|Some(Err(_)));
                let result=tokio::select!{biased;_=cancel.cancelled()=>return Ok(()),_=health.changed()=>{session.cancel();return Ok(())},value=tokio::time::timeout(config.limits.output_timeout,async{
                    match event{
                        Some(Ok(SpeechmaticsSttEvent::Transcript(transcript)))=>{
                            if transcript.is_final&&output.configuration().enable_usage_metrics&&usage_seconds>0.0{output.push(Frame::new(FrameData::Metrics(vec![Metric::SttUsage{processor:"SpeechmaticsSTTService".into(),audio_seconds:usage_seconds}])),Direction::Downstream).await?;usage_seconds=0.0;}
                            let data=DetailedTranscriptionFrame{text:TextFrame::new(transcript.text),user_id:transcript.speaker.unwrap_or_default(),timestamp:chrono::Utc::now().to_rfc3339(),language:Some(transcript.language),result:None,finalized:transcript.finalized&&transcript.turn_generation==turn_epoch};
                            let frame=if transcript.is_final{FrameData::TranscriptionDetailed(data)}else{FrameData::InterimTranscriptionDetailed(data)};output.push(Frame::new(frame),Direction::Downstream).await?;
                            if transcript.is_final&&output.configuration().enable_metrics{if let Some(start)=processing.take(){output.push(Frame::new(FrameData::Metrics(vec![Metric::Processing{processor:"SpeechmaticsSTTService".into(),seconds:start.elapsed().as_secs_f64()}])),Direction::Downstream).await?;}}
                        }
                        Some(Ok(SpeechmaticsSttEvent::TurnStarted{..})) if mode==TurnDetectionMode::Vad=>{if output.configuration().enable_metrics{processing=Some(tokio::time::Instant::now());}output.broadcast(Frame::new(FrameData::ProposedUserStartedSpeaking)).await?;}
                        Some(Ok(SpeechmaticsSttEvent::TurnStopped{..})) if mode==TurnDetectionMode::Vad=>output.broadcast(Frame::new(FrameData::ProposedUserStoppedSpeaking)).await?,
                        Some(Ok(SpeechmaticsSttEvent::AudioSent{seconds,..}))=>usage_seconds+=seconds,
                        Some(Ok(SpeechmaticsSttEvent::Speakers(result))) if config.settings.enable_diarization=>{let _=inner.speakers.send(result);}
                        Some(Err(StreamingError::Cancelled))|None=>(),
                        Some(Err(error))=>{let permanent=matches!(error,StreamingError::Provider{kind} if kind.is_permanent());output.push_error_if_health_generation(error_frame(error),permanent,generation).await?;}
                        Some(Ok(_))=>(),
                    }Ok::<(),ProcessorError>(())
                })=>value.map_err(|_|processor_error(StreamingError::Timeout("processor output")))?};
                result?;if terminal{return Ok(());}
            }
        })?;
        Ok(())
    }
    fn fence(
        &self,
        ending: bool,
        mute: Option<bool>,
    ) -> Result<Option<SpeechmaticsSttSender>, StreamingError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| StreamingError::Closed)?;
        if ending {
            state.ending = true;
        }
        if let Some(mute) = mute {
            state.muted = mute;
        }
        if let Some(cancel) = state.connect_cancel.take() {
            cancel.cancel();
        }
        if let Some(sender) = &state.sender {
            state.config.settings = sender.settings_snapshot()?;
        }
        Ok(state.sender.clone())
    }
    fn audio(&self, audio: &AudioRawFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if !ctx.is_usable() || audio.audio.is_empty() {
            return Ok(());
        }
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?;
        if state.muted || state.ending {
            return Ok(());
        }
        if state.health_generation != Some(ctx.health().generation()) {
            if let Some(sender) = &state.sender {
                sender.cancel();
            }
            return Err(processor_error(StreamingError::Closed).with_permanence_override(false));
        }
        state
            .sender
            .as_ref()
            .ok_or_else(|| processor_error(StreamingError::Backpressure))?
            .try_send_audio(audio.clone())
            .map_err(|e| processor_error(e).with_permanence_override(false))
    }
}
#[async_trait]
impl FrameProcessor for SpeechmaticsSttProcessor {
    fn name(&self) -> &str {
        "SpeechmaticsSTTService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.inner.health.clone())
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.start_session(ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                if let Some(sender) = self.fence(true, None).map_err(processor_error)? {
                    sender.cancel();
                }
                let _guard = self.inner.emission.lock().await;
            }
            FrameData::SttMute(mute) => {
                let changed = {
                    let state = self
                        .inner
                        .state
                        .lock()
                        .map_err(|_| processor_error(StreamingError::Closed))?;
                    state.muted != *mute
                };
                if changed {
                    if let Some(sender) = self.fence(false, Some(*mute)).map_err(processor_error)? {
                        sender.cancel();
                    }
                    {
                        let _guard = self.inner.emission.lock().await;
                    }
                    if !*mute {
                        let this = self.clone();
                        let output = ctx.clone();
                        ctx.spawn("speechmatics-unmute-session", false, async move {
                            this.start_session(&output).await
                        })?;
                    }
                }
            }
            FrameData::SttUpdateSettings(update) if targets(self, &update.service) => {
                let mut delta: Map<String, Value> = update.settings.clone().into_iter().collect();
                if let Some(typed) = &update.delta {
                    delta = typed.base.extra.clone().into_iter().collect();
                    add(&mut delta, "model", &typed.base.model);
                    add(&mut delta, "language", &typed.language);
                }
                self.update(&delta)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            FrameData::ServiceUpdateSettings(update) if targets(self, &update.service) => {
                let mut delta: Map<String, Value> = update.settings.clone().into_iter().collect();
                if let Some(typed) = &update.delta {
                    delta = typed.extra.clone().into_iter().collect();
                    add(&mut delta, "model", &typed.model);
                }
                self.update(&delta)
                    .map_err(|e| processor_error(e).with_permanence_override(false))?;
                return Ok(());
            }
            _ => (),
        }
        if direction == Direction::Downstream {
            match &frame.data {
                FrameData::Start(_) | FrameData::StartDetailed(_) => {
                    ctx.push(
                        Frame::new(FrameData::SttMetadata(SttMetadataFrame {
                            base: ServiceMetadataFrame {
                                service_name: self.name().into(),
                                user_turn_strategies: None,
                            },
                            ttfs_p99_latency: self
                                .config_snapshot()
                                .map_err(processor_error)?
                                .ttfs_p99_latency,
                        })),
                        direction,
                    )
                    .await?
                }
                FrameData::InputAudio(audio) => {
                    self.audio(audio, ctx)?;
                    if !self.audio_passthrough {
                        return Ok(());
                    }
                }
                FrameData::UserAudio(user) => {
                    self.audio(&user.audio, ctx)?;
                    if !self.audio_passthrough {
                        return Ok(());
                    }
                }
                FrameData::VadUserStartedSpeaking(_)
                | FrameData::VadUserStartedSpeakingDetailed(_) => {
                    let state = self
                        .inner
                        .state
                        .lock()
                        .map_err(|_| processor_error(StreamingError::Closed))?;
                    if !state.muted && !state.ending {
                        if let Some(sender) = &state.sender {
                            sender
                                .try_start_turn()
                                .map_err(|e| processor_error(e).with_permanence_override(false))?;
                        }
                    }
                }
                FrameData::VadUserStoppedSpeaking(_)
                | FrameData::VadUserStoppedSpeakingDetailed(_) => {
                    let state = self
                        .inner
                        .state
                        .lock()
                        .map_err(|_| processor_error(StreamingError::Closed))?;
                    if !state.muted
                        && !state.ending
                        && state.config.settings.turn_detection_mode == TurnDetectionMode::External
                    {
                        if let Some(sender) = &state.sender {
                            match sender.try_finalize() {
                                Ok(())
                                | Err(StreamingError::Invalid("finalize requires new audio")) => (),
                                Err(e) => {
                                    return Err(processor_error(e).with_permanence_override(false))
                                }
                            }
                        }
                    }
                }
                _ => (),
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        let sender = self.fence(true, None).map_err(processor_error)?;
        {
            let _guard = self.inner.emission.lock().await;
        }
        if let Some(sender) = sender {
            match sender.finish().await {
                Ok(()) | Err(StreamingError::Closed | StreamingError::Cancelled) => (),
                Err(error) => {
                    sender.cancel();
                    return Err(processor_error(error));
                }
            }
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let Some(sender) = self.fence(true, None).map_err(processor_error)? {
            sender.cancel();
        }
        let _guard = self.inner.emission.lock().await;
        self.inner
            .state
            .lock()
            .map_err(|_| processor_error(StreamingError::Closed))?
            .sender = None;
        Ok(())
    }
}
fn targets(processor: &dyn FrameProcessor, target: &Option<ProcessorReference>) -> bool {
    match target {
        None => true,
        Some(ProcessorReference::Name(n)) => n == processor.name(),
        Some(ProcessorReference::Local(p)) => std::ptr::addr_eq(p.0.as_ref(), processor),
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
fn error_frame(error: StreamingError) -> ErrorFrame {
    let category = match error {
        StreamingError::Http(status) => ErrorCategory::from_http_status(status),
        StreamingError::Provider {
            kind: ProviderErrorKind::NotAuthorised,
        } => ErrorCategory::Authentication,
        StreamingError::Provider {
            kind: ProviderErrorKind::InsufficientFunds | ProviderErrorKind::QuotaExceeded,
        } => ErrorCategory::Quota,
        StreamingError::Provider { kind } if kind.is_permanent() => ErrorCategory::InvalidRequest,
        StreamingError::Invalid(_) => ErrorCategory::InvalidRequest,
        StreamingError::Transport | StreamingError::Timeout(_) | StreamingError::Closed => {
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
fn processor_error(error: StreamingError) -> ProcessorError {
    let error = error_frame(error);
    ProcessorError::new(error.error).with_category(error.category)
}
