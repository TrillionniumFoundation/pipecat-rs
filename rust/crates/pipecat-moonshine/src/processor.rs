use crate::{
    model::validate_audio, Cancellation, MoonshineError, MoonshineService, MoonshineSettings,
    PendingTranscription, Result, SAMPLE_RATE,
};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, ErrorCategory, ErrorFrame, Frame, FrameData,
    FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorHealthSnapshot, ServiceUpdateSettingsFrame, Setting, StartFrame, SttSettings,
    TranscriptionFrame,
};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentationMode {
    /// Keep one second of pre-roll; VAD stop submits the complete speech segment.
    Vad,
    /// Each input audio frame is already a complete segment, useful for batch I/O.
    AudioFrames,
}
#[derive(Debug, Clone)]
pub struct MoonshineProcessorOptions {
    pub settings: MoonshineSettings,
    pub segmentation: SegmentationMode,
    pub pre_roll_duration: Duration,
    pub trailing_silence: Duration,
    pub audio_passthrough: bool,
    pub push_empty_transcripts: bool,
    pub emit_metrics: bool,
}
impl Default for MoonshineProcessorOptions {
    fn default() -> Self {
        Self {
            settings: MoonshineSettings::default(),
            segmentation: SegmentationMode::Vad,
            pre_roll_duration: Duration::from_secs(1),
            trailing_silence: Duration::from_millis(500),
            audio_passthrough: true,
            push_empty_transcripts: false,
            emit_metrics: true,
        }
    }
}
struct Completion {
    pending: PendingTranscription,
    user_id: String,
    submitted: Instant,
    id: u64,
    revision: u64,
}
struct State {
    settings: MoonshineSettings,
    audio: Vec<u8>,
    user_id: String,
    speaking: bool,
    discarded: bool,
    muted: bool,
    ending: bool,
    started: bool,
    sender: Option<mpsc::Sender<Completion>>,
    health_revision: u64,
    next_request_id: u64,
    pending: Vec<(u64, Cancellation)>,
}
impl State {
    fn sync_health(&mut self, health: ProcessorHealthSnapshot) -> bool {
        // A false/true transition can coalesce before either lane observes it.
        // Every revision invalidates the whole old segment and accepted work.
        if health.revision != self.health_revision || !health.is_usable {
            self.health_revision = health.revision;
            self.audio.clear();
            self.user_id.clear();
            self.speaking = false;
            self.discarded = true;
            for (_, cancellation) in self.pending.drain(..) {
                cancellation.cancel();
            }
        }
        health.is_usable
    }
}
/// Native segmented STT. Audio is forwarded immediately and decoding happens on
/// the service's owned thread. One tracked relay emits transcripts in accepted
/// segment order. Bot interruptions preserve user STT; Cancel aborts it, End/Stop
/// flush active speech and drain accepted work. This processor is single-use.
pub struct MoonshineSttProcessor {
    service: Arc<MoonshineService>,
    options: MoonshineProcessorOptions,
    state: Arc<Mutex<State>>,
    health: ProcessorHealth,
}
impl MoonshineSttProcessor {
    pub fn new(service: MoonshineService, options: MoonshineProcessorOptions) -> Result<Self> {
        service.validate_settings(&options.settings)?;
        let mut options = options;
        options.settings = options.settings.resolved();
        if options.pre_roll_duration > Duration::from_secs(5)
            || options.trailing_silence > Duration::from_secs(5)
            || options.pre_roll_duration + options.trailing_silence
                >= service.config().max_segment_duration
        {
            return Err(MoonshineError::Invalid("pre-roll and trailing silence must fit the segment duration limit (each at most 5 seconds)"));
        }
        Ok(Self {
            service: Arc::new(service),
            state: Arc::new(Mutex::new(State {
                settings: options.settings.clone(),
                audio: vec![],
                user_id: String::new(),
                speaking: false,
                discarded: false,
                muted: false,
                ending: false,
                started: false,
                sender: None,
                health_revision: 0,
                next_request_id: 0,
                pending: vec![],
            })),
            health: ProcessorHealth::default(),
            options,
        })
    }
    pub fn is_running(&self) -> bool {
        self.service.is_running()
    }
    pub fn settings(&self) -> Result<MoonshineSettings> {
        Ok(self.lock()?.settings.clone())
    }
    /// Apply validated settings atomically. Only an effective change restores
    /// usability; a rejected or identical update leaves health unchanged.
    pub fn update_settings(&self, settings: MoonshineSettings) -> Result<()> {
        self.service.validate_settings(&settings)?;
        let mut state = self.lock()?;
        if state.ending {
            return Err(MoonshineError::Closed);
        }
        let settings = settings.resolved();
        let changed = state.settings != settings;
        state.settings = settings;
        state.sync_health(self.health.snapshot());
        drop(state);
        if changed {
            self.health.set_usable(true);
        }
        Ok(())
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| MoonshineError::Worker("processor state poisoned".into()))
    }
    fn submit_locked(&self, state: &mut State, mut bytes: Vec<u8>, user_id: String) -> Result<()> {
        let revision = state.health_revision;
        if !state.sync_health(self.health.snapshot())
            || state.health_revision != revision
            || bytes.is_empty()
        {
            return Ok(());
        }
        let sender = state.sender.as_ref().ok_or(MoonshineError::Closed)?;
        // Reserve the result lane first, so accepted inference always has a relay.
        let slot = sender.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => MoonshineError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => MoonshineError::Closed,
        })?;
        let silence = (self.options.trailing_silence.as_secs_f64() * f64::from(SAMPLE_RATE)).floor()
            as usize
            * 2;
        if bytes.len().saturating_add(silence) > self.service.config().max_samples() * 2 {
            return Err(MoonshineError::Invalid(
                "speech plus trailing silence exceeds segment duration limit",
            ));
        }
        bytes.resize(bytes.len() + silence, 0);
        let audio = AudioRawFrame {
            audio: bytes,
            sample_rate: SAMPLE_RATE,
            num_channels: 1,
            context_id: None,
        };
        let pending = self.service.submit(&audio, state.settings.clone())?;
        let id = state.next_request_id;
        state.next_request_id = state.next_request_id.wrapping_add(1);
        state.pending.push((id, pending.cancellation()));
        slot.send(Completion {
            pending,
            user_id,
            submitted: Instant::now(),
            id,
            revision: state.health_revision,
        });
        Ok(())
    }
    fn buffer_audio(&self, audio: &AudioRawFrame, user_id: &str) -> Result<()> {
        let mut state = self.lock()?;
        if state.ending {
            return Err(MoonshineError::Closed);
        }
        if !state.sync_health(self.health.snapshot()) || state.muted {
            return Ok(());
        }
        if let Err(error) = validate_audio(audio, self.service.config().max_samples()) {
            if self.options.segmentation == SegmentationMode::Vad {
                state.audio.clear();
                state.speaking = false;
                state.discarded = true;
            }
            return Err(error);
        }
        if self.options.segmentation == SegmentationMode::AudioFrames {
            return self.submit_locked(&mut state, audio.audio.clone(), user_id.into());
        }
        if state.discarded {
            return Ok(());
        }
        let silence = (self.options.trailing_silence.as_secs_f64() * f64::from(SAMPLE_RATE)).floor()
            as usize
            * 2;
        let max = self.service.config().max_samples() * 2 - silence;
        if state.speaking && state.audio.len().saturating_add(audio.audio.len()) > max {
            state.audio.clear();
            state.speaking = false;
            state.discarded = true;
            return Err(MoonshineError::Invalid(
                "speech segment overflow; segment discarded",
            ));
        }
        state.user_id = user_id.into();
        if state.speaking {
            state.audio.extend_from_slice(&audio.audio);
        } else {
            let pre_roll = (self.options.pre_roll_duration.as_secs_f64() * f64::from(SAMPLE_RATE))
                .floor() as usize
                * 2;
            // Keep only the bounded suffix, avoiding even a temporary large buffer.
            if audio.audio.len() >= pre_roll {
                state.audio.clear();
                state
                    .audio
                    .extend_from_slice(&audio.audio[audio.audio.len() - pre_roll..]);
            } else {
                let discard = state
                    .audio
                    .len()
                    .saturating_add(audio.audio.len())
                    .saturating_sub(pre_roll);
                state.audio.drain(..discard);
                state.audio.extend_from_slice(&audio.audio);
            }
        }
        Ok(())
    }
    fn stop_speech(&self) -> Result<()> {
        let mut state = self.lock()?;
        if !state.sync_health(self.health.snapshot())
            || state.ending
            || state.muted
            || !state.speaking
            || state.discarded
            || self.options.segmentation != SegmentationMode::Vad
        {
            return Ok(());
        }
        state.speaking = false;
        let bytes = std::mem::take(&mut state.audio);
        let user_id = state.user_id.clone();
        self.submit_locked(&mut state, bytes, user_id)
    }
    fn finish(&self, cancel: bool) -> Result<()> {
        let mut state = self.lock()?;
        let usable = state.sync_health(self.health.snapshot());
        if !state.ending {
            state.ending = true;
            let bytes = std::mem::take(&mut state.audio);
            let result = if !cancel && usable && state.speaking && !state.muted && !state.discarded
            {
                let user_id = state.user_id.clone();
                self.submit_locked(&mut state, bytes, user_id)
            } else {
                Ok(())
            };
            state.sender.take();
            if cancel {
                self.service.cancel()?;
            } else {
                self.service.finish()?;
            }
            result?;
        } else if cancel {
            self.service.cancel()?;
        }
        Ok(())
    }
    fn apply_frame_settings(&self, update: &ServiceUpdateSettingsFrame<SttSettings>) -> Result<()> {
        let mut state = self.lock()?;
        if state.ending {
            return Err(MoonshineError::Closed);
        }
        let mut value = serde_json::to_value(&state.settings)
            .map_err(|_| MoonshineError::Invalid("settings serialization failed"))?;
        let object = value.as_object_mut().unwrap();
        let mut changes = update.settings.clone();
        if let Some(delta) = &update.delta {
            match &delta.base.model {
                Setting::NotGiven => {}
                Setting::Null => {
                    changes.insert("model".into(), serde_json::Value::Null);
                }
                Setting::Value(model) => {
                    changes.insert("model".into(), model.clone().into());
                }
            }
            changes.extend(delta.base.extra.clone());
            match &delta.language {
                Setting::NotGiven => {}
                Setting::Null => {
                    changes.insert("language".into(), serde_json::Value::Null);
                }
                Setting::Value(language) => {
                    changes.insert("language".into(), language.clone().into());
                }
            }
        }
        for (key, val) in changes {
            if !object.contains_key(&key) {
                return Err(MoonshineError::Invalid("unsupported Moonshine setting"));
            }
            object.insert(key, val);
        }
        let settings: MoonshineSettings = serde_json::from_value(value)
            .map_err(|_| MoonshineError::Invalid("invalid Moonshine settings delta"))?;
        self.service.validate_settings(&settings)?;
        let settings = settings.resolved();
        let changed = state.settings != settings;
        state.settings = settings;
        state.sync_health(self.health.snapshot());
        drop(state);
        if changed {
            self.health.set_usable(true);
        }
        Ok(())
    }
}
fn processor_error(error: MoonshineError) -> ProcessorError {
    ProcessorError::new(error.to_string()).with_category(error_category(&error))
}
fn error_category(error: &MoonshineError) -> ErrorCategory {
    match error {
        MoonshineError::Invalid(_)
        | MoonshineError::UnsupportedLanguage(_)
        | MoonshineError::MissingModel(_)
        | MoonshineError::LicenseRequired
        | MoonshineError::TooLarge => ErrorCategory::InvalidRequest,
        MoonshineError::Cancelled
        | MoonshineError::Backpressure
        | MoonshineError::Closed
        | MoonshineError::Worker(_)
        | MoonshineError::Native(_)
        | MoonshineError::HashMismatch
        | MoonshineError::Io(_) => ErrorCategory::Application,
    }
}
fn sync_shared_health(state: &Mutex<State>, health: &ProcessorHealth) -> Result<()> {
    state
        .lock()
        .map_err(|_| MoonshineError::Worker("processor state poisoned".into()))?
        .sync_health(health.snapshot());
    Ok(())
}
fn revision_is_usable(health: &ProcessorHealth, revision: u64) -> bool {
    let current = health.snapshot();
    current.is_usable && current.revision == revision
}
#[async_trait]
impl FrameProcessor for MoonshineSttProcessor {
    fn name(&self) -> &str {
        "MoonshineSttProcessor"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if config.audio_in_sample_rate != SAMPLE_RATE {
            return Err(processor_error(MoonshineError::Invalid(
                "Moonshine pipeline input must be 16000 Hz",
            )));
        }
        let (sender, mut receiver) =
            mpsc::channel::<Completion>(self.service.config().queue_capacity);
        {
            let mut state = self.lock().map_err(processor_error)?;
            if state.started || state.ending {
                return Err(processor_error(MoonshineError::Closed));
            }
            state.started = true;
            state.sender = Some(sender);
        }
        let output = ctx.clone();
        let options = self.options.clone();
        let health = self.health.clone();
        let state = self.state.clone();
        let mut health_updates = health.watch();
        ctx.spawn("moonshine-transcripts", false, async move {
            loop {
                sync_shared_health(&state, &health).map_err(processor_error)?;
                let completion = tokio::select! {
                    biased;
                    _ = health_updates.changed() => continue,
                    completion = receiver.recv() => completion,
                };
                let Some(completion) = completion else { break };
                sync_shared_health(&state, &health).map_err(processor_error)?;
                let result = completion.pending.result();
                tokio::pin!(result);
                let result = loop {
                    tokio::select! {
                        biased;
                        _ = health_updates.changed() => {
                            sync_shared_health(&state, &health).map_err(processor_error)?;
                        }
                        result = &mut result => break result,
                    }
                };
                {
                    let mut state = state.lock().map_err(|_| {
                        processor_error(MoonshineError::Worker("processor state poisoned".into()))
                    })?;
                    state.pending.retain(|(id, _)| *id != completion.id);
                    state.sync_health(health.snapshot());
                }
                if !revision_is_usable(&health, completion.revision) {
                    continue;
                }
                match result {
                    Ok(transcript) => {
                        if options.emit_metrics && revision_is_usable(&health, completion.revision)
                        {
                            output
                                .push(
                                    Frame::new(FrameData::Metrics(vec![
                                        Metric::Processing {
                                            processor: "MoonshineSttProcessor".into(),
                                            seconds: completion.submitted.elapsed().as_secs_f64(),
                                        },
                                        Metric::SttUsage {
                                            processor: "MoonshineSttProcessor".into(),
                                            audio_seconds: transcript.audio_duration_seconds,
                                        },
                                    ])),
                                    Direction::Downstream,
                                )
                                .await?;
                        }
                        if (options.push_empty_transcripts || !transcript.text.is_empty())
                            && revision_is_usable(&health, completion.revision)
                        {
                            let frame = TranscriptionFrame {
                                text: transcript.text.clone(),
                                user_id: completion.user_id,
                                timestamp: chrono::Utc::now().to_rfc3339(),
                                language: Some(transcript.language.as_str().into()),
                                result: Some(
                                    serde_json::to_value(&transcript)
                                        .map_err(|e| ProcessorError::new(e.to_string()))?,
                                ),
                                finalized: true,
                            };
                            if !revision_is_usable(&health, completion.revision) {
                                continue;
                            }
                            output
                                .push(
                                    Frame::new(FrameData::Transcription(frame)),
                                    Direction::Downstream,
                                )
                                .await?;
                        }
                    }
                    Err(MoonshineError::Cancelled) => {}
                    Err(error) => {
                        if !revision_is_usable(&health, completion.revision) {
                            continue;
                        }
                        output
                            .push_error(ErrorFrame {
                                error: error.to_string(),
                                fatal: false,
                                processor: None,
                                category: error_category(&error),
                            })
                            .await?;
                    }
                }
            }
            Ok(())
        })?;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        match &frame.data {
            FrameData::InputAudio(audio) if direction == Direction::Downstream => {
                self.buffer_audio(audio, "").map_err(processor_error)?;
                if !self.options.audio_passthrough {
                    return Ok(());
                }
            }
            FrameData::UserAudio(audio) if direction == Direction::Downstream => {
                self.buffer_audio(&audio.audio, &audio.user_id)
                    .map_err(processor_error)?;
                if !self.options.audio_passthrough {
                    return Ok(());
                }
            }
            FrameData::VadUserStartedSpeaking(_) | FrameData::VadUserStartedSpeakingDetailed(_) => {
                let mut state = self.lock().map_err(processor_error)?;
                if state.sync_health(self.health.snapshot()) && !state.ending && !state.muted {
                    state.discarded = false;
                    state.speaking = true;
                }
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_) => {
                self.stop_speech().map_err(processor_error)?
            }
            FrameData::SttMute(muted) => {
                let mut state = self.lock().map_err(processor_error)?;
                state.sync_health(self.health.snapshot());
                state.muted = *muted;
                if *muted {
                    state.audio.clear();
                    state.speaking = false;
                }
            }
            FrameData::SttUpdateSettings(update)
                if update
                    .service
                    .as_ref()
                    .is_none_or(|s| s.name() == self.name()) =>
            {
                // A rejected proposal leaves the active configuration intact.
                // Only failures of admitted work can poison its usability.
                self.apply_frame_settings(update)
                    .map_err(|error| processor_error(error).with_permanence_override(false))?
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                self.finish(true).map_err(processor_error)?
            }
            FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop => {
                self.finish(false).map_err(processor_error)?
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(
        &self,
        _ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        self.finish(false).map_err(processor_error)?;
        // Stop does not invoke cleanup. The single-use native worker must be
        // joined before either graceful terminal frame can finish the pipeline.
        self.service.drain().await.map_err(processor_error)
    }
    async fn cleanup(&self) -> std::result::Result<(), ProcessorError> {
        self.finish(true).map_err(processor_error)?;
        self.service.shutdown().await.map_err(processor_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_failures_classify_only_invalid_requests_as_permanent() {
        for error in [
            MoonshineError::Invalid("invalid request"),
            MoonshineError::UnsupportedLanguage("es".into()),
        ] {
            let error = processor_error(error);
            assert_eq!(error.category, ErrorCategory::InvalidRequest);
            assert!(!error.fatal);
        }
        for error in [
            MoonshineError::Cancelled,
            MoonshineError::Backpressure,
            MoonshineError::Closed,
            MoonshineError::Worker("native worker failed".into()),
            MoonshineError::Native("native failure".into()),
        ] {
            let error = processor_error(error);
            assert_eq!(error.category, ErrorCategory::Application);
            assert!(!error.fatal);
        }
    }

    #[test]
    fn coalesced_revisions_retire_buffer_and_every_native_cancellation() {
        let health = ProcessorHealth::default();
        let cancellations = [Cancellation::new(), Cancellation::new()];
        let mut state = State {
            settings: MoonshineSettings::default(),
            audio: vec![0; 100],
            user_id: "retired-user".into(),
            speaking: true,
            discarded: false,
            muted: true,
            ending: false,
            started: true,
            sender: None,
            health_revision: 0,
            next_request_id: 2,
            pending: cancellations
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, cancel)| (index as u64, cancel))
                .collect(),
        };
        health.set_usable(false);
        health.set_usable(true);
        assert!(state.sync_health(health.snapshot()));
        assert_eq!(state.health_revision, 2);
        assert!(state.audio.is_empty());
        assert!(state.user_id.is_empty());
        assert!(!state.speaking);
        assert!(state.discarded);
        assert!(state.muted);
        assert!(state.pending.is_empty());
        assert!(cancellations.iter().all(Cancellation::is_cancelled));
        assert!(!revision_is_usable(&health, 0));
        assert!(revision_is_usable(&health, 2));
    }
}
