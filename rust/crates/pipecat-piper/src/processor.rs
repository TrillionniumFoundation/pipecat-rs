//! Complete-utterance pipeline bridge. Native synthesis is buffered; bounded
//! asynchronous relays preserve response order and never imply audible playout.
use crate::{PendingSynthesis, PiperError, PiperService, PiperSettings};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, ErrorCategory, ErrorFrame, Frame, FrameData,
    FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorHealthGeneration, ProcessorHealthSnapshot, ProcessorReference, Setting, StartFrame,
    TtsSettings, TtsStartedFrame, TtsStoppedFrame,
};
use pipecat_services::{CancellationToken, TtsAudioWatchdog};
use pipecat_text::{
    TokenReporting, TtsAlignment, TtsAlignmentError, TtsAlignmentEvent, TtsAlignmentFrames,
    TtsAlignmentLimits, TtsTextChannels,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::sync::watch;
use tokio::time::Instant;

static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct PiperProcessorOptions {
    /// Includes native work, completed uncollected results and active PCM relays.
    pub max_pending: usize,
    /// PCM16 mono samples per frame. This never enlarges downstream queues.
    pub chunk_samples: usize,
}
impl Default for PiperProcessorOptions {
    fn default() -> Self {
        Self {
            max_pending: 4,
            chunk_samples: 1024,
        }
    }
}
struct Relay {
    done: CancellationToken,
    stop: CancellationToken,
    interruptible: bool,
    speech: bool,
}
struct State {
    settings: PiperSettings,
    pending: Vec<Relay>,
    failure: Arc<AtomicBool>,
    ending: bool,
}
/// Single-use adapter for a loaded, caller-owned local CPU model. Model, voice,
/// phonemizer, language and sample rate remain immutable for its lifetime.
/// End/Stop drain; Cancel joins the native worker; interruption preserves
/// explicitly uninterruptible work. No runtime downloads or network calls.
pub struct PiperTtsProcessor {
    service: Arc<PiperService>,
    options: PiperProcessorOptions,
    health: ProcessorHealth,
    state: Mutex<State>,
    timeline: Arc<AtomicU64>,
    started: AtomicBool,
    watchdog: TtsAudioWatchdog,
}
impl PiperTtsProcessor {
    pub fn new(
        service: PiperService,
        settings: PiperSettings,
        options: PiperProcessorOptions,
    ) -> crate::Result<Self> {
        if options.max_pending == 0
            || options.max_pending > service.config().queue_capacity
            || options
                .max_pending
                .checked_mul(2)
                .and_then(|n| n.checked_add(4))
                .is_none()
            || !(1..=16384).contains(&options.chunk_samples)
        {
            return Err(PiperError::Invalid("invalid pipeline pending/chunk bounds"));
        }
        service.validate_settings(&settings)?;
        let settings = resolved_settings(&service, settings);
        Ok(Self {
            service: Arc::new(service),
            options,
            health: ProcessorHealth::default(),
            state: Mutex::new(State {
                settings,
                pending: Vec::new(),
                failure: Arc::default(),
                ending: false,
            }),
            timeline: Arc::default(),
            started: AtomicBool::new(false),
            watchdog: TtsAudioWatchdog::default(),
        })
    }
    pub fn with_max_consecutive_zero_audio_contexts(mut self, maximum: usize) -> Self {
        self.watchdog = TtsAudioWatchdog::new(maximum);
        self
    }
    pub fn update_settings(&self, delta: &TtsSettings) -> crate::Result<bool> {
        if !matches!(delta.base.model, Setting::NotGiven)
            || !matches!(delta.voice, Setting::NotGiven)
            || !matches!(delta.language, Setting::NotGiven)
        {
            return Err(PiperError::Unsupported(
                "loaded model, voice and language are immutable",
            ));
        }
        let mut state = self.state.lock().map_err(|_| PiperError::Worker)?;
        if state.ending {
            return Err(PiperError::Closed);
        }
        let mut next = state.settings.clone();
        for (key, value) in &delta.base.extra {
            match key.as_str() {
                "speaker_id" => {
                    next.speaker_id = if value.is_null() {
                        None
                    } else {
                        Some(
                            value
                                .as_i64()
                                .ok_or(PiperError::Invalid("speaker_id must be an integer"))?,
                        )
                    };
                }
                "length_scale" | "noise_scale" | "noise_w" => {
                    let number = if value.is_null() {
                        None
                    } else {
                        Some(
                            value
                                .as_f64()
                                .ok_or(PiperError::Invalid("synthesis scale must be numeric"))?
                                as f32,
                        )
                    };
                    match key.as_str() {
                        "length_scale" => next.length_scale = number,
                        "noise_scale" => next.noise_scale = number,
                        _ => next.noise_w = number,
                    }
                }
                _ => return Err(PiperError::Unsupported("Piper pipeline setting")),
            }
        }
        self.service.validate_settings(&next)?;
        let next = resolved_settings(&self.service, next);
        if next == state.settings {
            return Ok(false);
        }
        state.settings = next;
        self.health.set_usable(true);
        Ok(true)
    }
    fn cancel_relays(&self, interruption_only: bool) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().map_err(|_| state_error())?;
        // Keep predecessor tokens until their actual completion. Removing a
        // cancelled middle relay must never let a surviving fence overtake audio.
        for relay in &state.pending {
            if !interruption_only || relay.interruptible {
                relay.stop.cancel();
            }
        }
        state.failure = Arc::default();
        if !interruption_only {
            state.ending = true;
            self.service.cancel().map_err(request_error)?;
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn unfinished_speech_relays(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .pending
            .iter()
            .filter(|relay| relay.speech && !relay.done.is_cancelled())
            .count()
    }
    fn enqueue(&self, input: Input, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().map_err(|_| state_error())?;
        state.pending.retain(|relay| !relay.done.is_cancelled());
        let speech = matches!(&input, Input::Speak(speech) if !speech.skipped);
        // Closing speech admission must not prevent an admitted Flush probe's
        // returning pass from completing after prepare_end.
        if state.ending && matches!(&input, Input::Speak(_)) {
            return Err(request_error(PiperError::Closed));
        }
        if speech && !ctx.is_usable() {
            state.failure.store(true, Ordering::Release);
            return Ok(());
        }
        let limit = if speech {
            self.options.max_pending
        } else {
            self.options.max_pending * 2 + 4
        };
        if state.pending.iter().filter(|p| p.speech == speech).count() >= limit {
            state.failure.store(true, Ordering::Release);
            return Err(request_error(PiperError::Backpressure));
        }
        if matches!(&input, Input::Fence(frame) if matches!(frame.data, FrameData::LlmResponseStart))
        {
            state.failure = Arc::new(AtomicBool::new(!ctx.is_usable()));
        }
        let generation = ctx.health().generation();
        let command = match input {
            Input::Speak(input) => {
                let prepared = prepare(&self.service, &state.settings, *input, generation);
                match prepared {
                    Ok(prepared) => Command::Speak(Box::new(prepared)),
                    Err(error) => {
                        // A locally rejected request is not a completed native
                        // context and must not spend the silent-context budget.
                        state.failure.store(true, Ordering::Release);
                        return Err(error);
                    }
                }
            }
            Input::Fence(frame) => Command::Fence(frame),
        };
        let interruptible = match &command {
            Command::Speak(prepared) => prepared.input.interruptible,
            Command::Fence(frame) => frame.interruptible,
        };
        let predecessors: Vec<_> = state
            .pending
            .iter()
            .map(|relay| relay.done.clone())
            .collect();
        let done = CancellationToken::new();
        let stop = CancellationToken::new();
        let failure = state.failure.clone();
        let guard = RelayGuard {
            done: done.clone(),
            failure: failure.clone(),
            complete: false,
        };
        let output = ctx.clone();
        let cancelled = stop.clone();
        let timeline = self.timeline.clone();
        let watchdog = self.watchdog.clone();
        let chunk_samples = self.options.chunk_samples;
        ctx.spawn("piper-relay", interruptible, async move {
            let mut guard = guard;
            let operation = async {
                // Native ownership starts at synchronous admission, before this
                // relay may wait for an earlier output fence. Fence pressure
                // must not defer health cancellation of that admitted request.
                let native_revision = match &command {
                    Command::Speak(prepared) if prepared.pending.is_some() => {
                        Some(prepared.generation.snapshot.revision)
                    }
                    _ => None,
                };
                let mut health = output.health().watch();
                for previous in predecessors {
                    loop {
                        if native_revision.is_some_and(|revision| !current(&output, revision)) {
                            failure.store(true, Ordering::Release);
                            return Ok(());
                        }
                        tokio::select! {
                            biased;
                            changed = health.changed(), if native_revision.is_some() => {
                                if changed.is_err() {
                                    failure.store(true, Ordering::Release);
                                    return Ok(());
                                }
                            }
                            _ = previous.cancelled() => break,
                        }
                    }
                }
                match command {
                    Command::Speak(prepared) => {
                        synthesize(
                            *prepared,
                            &output,
                            &failure,
                            &timeline,
                            &watchdog,
                            chunk_samples,
                        )
                        .await
                    }
                    Command::Fence(mut frame) => {
                        if matches!(frame.data, FrameData::LlmResponseEnd) {
                            if failure.load(Ordering::Acquire) {
                                frame.metadata.insert("failed".into(), true.into());
                            }
                            frame.set_pipeline_pts(timeline.load(Ordering::Acquire));
                        }
                        if matches!(
                            frame.data,
                            FrameData::Flush(_) | FrameData::FlushDetailed(_)
                        ) {
                            if output.has_downstream_processor() {
                                output.push_downstream_flush_wait(*frame).await
                            } else {
                                output.push(*frame, Direction::Downstream).await
                            }
                        } else {
                            output.push_downstream_wait(*frame).await
                        }
                    }
                }
            };
            let result = tokio::select! {
                biased;
                _ = cancelled.cancelled() => return Ok(()),
                result = operation => result,
            };
            if result.is_err() {
                failure.store(true, Ordering::Release);
            }
            guard.complete = true;
            result
        })?;
        state.pending.push(Relay {
            done,
            stop,
            interruptible,
            speech,
        });
        Ok(())
    }
}
struct RelayGuard {
    done: CancellationToken,
    failure: Arc<AtomicBool>,
    complete: bool,
}
impl Drop for RelayGuard {
    fn drop(&mut self) {
        if !self.complete {
            self.failure.store(true, Ordering::Release);
        }
        self.done.cancel();
    }
}
struct SpeechInput {
    text: TtsTextChannels,
    append_to_context: bool,
    source_id: u64,
    destination: Option<String>,
    aggregated_by: String,
    includes_inter_frame_spaces: bool,
    skipped: bool,
    interruptible: bool,
}
enum Input {
    Speak(Box<SpeechInput>),
    Fence(Box<Frame>),
}
enum Command {
    Speak(Box<Prepared>),
    Fence(Box<Frame>),
}
struct Prepared {
    input: SpeechInput,
    context_id: String,
    generation: ProcessorHealthGeneration,
    alignment: TtsAlignment,
    mapper: TtsAlignmentFrames,
    initial: Vec<Frame>,
    pending: Option<PendingSynthesis>,
    admitted: Instant,
}
fn prepare(
    service: &PiperService,
    settings: &PiperSettings,
    input: SpeechInput,
    generation: ProcessorHealthGeneration,
) -> Result<Prepared, ProcessorError> {
    let limits = TtsAlignmentLimits::default();
    if [
        &input.text.synthesis,
        &input.text.display,
        &input.text.original,
    ]
    .iter()
    .any(|text| text.len() > limits.sequencer.sequencer.alignment.max_text_bytes)
        || input.aggregated_by.len() > 128
        || input
            .destination
            .as_ref()
            .is_some_and(|name| name.len() > 1024)
    {
        return Err(request_error(PiperError::TooLarge));
    }
    let serial = NEXT_CONTEXT
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| ProcessorError::fatal("Piper context identity exhausted"))?;
    let context_id = format!("piper-{serial}");
    let language = service
        .voice_config()
        .espeak
        .as_ref()
        .map(|voice| voice.voice.as_str());
    let mut alignment = TtsAlignment::new(
        generation.snapshot.revision,
        input.append_to_context,
        TokenReporting::SynthesisCompletion,
        language,
        limits,
    )
    .map_err(alignment_error)?;
    let mut mapper = TtsAlignmentFrames::new(
        context_id.clone(),
        alignment.context(),
        input.append_to_context,
        limits.sequencer.sequencer.max_slots,
    )
    .map_err(alignment_error)?;
    let events = if input.skipped {
        alignment.skipped_prepared(
            &input.text.display,
            &input.text.original,
            input.destination.as_deref(),
            &input.aggregated_by,
            input.includes_inter_frame_spaces,
        )
    } else {
        alignment.register_prepared(
            &input.text,
            true,
            &input.aggregated_by,
            input.includes_inter_frame_spaces,
        )
    }
    .map_err(alignment_error)?;
    let initial = mapper.convert(events).map_err(alignment_error)?;
    // Native admission is synchronous and follows bounded alignment admission.
    // prepare_end can therefore close and drain native input without racing a
    // relay that has not yet submitted its accepted utterance.
    let pending = if input.skipped {
        None
    } else {
        Some(
            service
                .submit(&input.text.synthesis, &context_id, settings.clone())
                .map_err(request_error)?,
        )
    };
    Ok(Prepared {
        input,
        context_id,
        generation,
        alignment,
        mapper,
        initial,
        pending,
        admitted: Instant::now(),
    })
}
#[async_trait]
impl FrameProcessor for PiperTtsProcessor {
    fn name(&self) -> &str {
        "PiperTTSService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Err(ProcessorError::fatal("Piper processor is single-use"));
        }
        if start.audio_out_sample_rate != self.service.sample_rate() {
            return Err(ProcessorError::fatal(
                "pipeline output sample rate differs from loaded Piper model",
            )
            .with_category(ErrorCategory::InvalidRequest));
        }
        Ok(())
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.state.lock().map_err(|_| state_error())?.ending = true;
        self.service.drain().await.map_err(request_error)
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Interruption) {
            self.cancel_relays(true)?;
        }
        if frame.data.is_cancel() {
            self.cancel_relays(false)?;
        }
        let delta = match &frame.data {
            FrameData::TtsUpdateSettings(update) if targets(self, &update.service) => {
                Some(match &update.delta {
                    Some(delta) => delta.clone(),
                    None => settings_mapping(&update.settings)?,
                })
            }
            FrameData::ServiceUpdateSettings(update) if targets(self, &update.service) => {
                Some(match &update.delta {
                    Some(delta) => TtsSettings {
                        base: delta.clone(),
                        ..Default::default()
                    },
                    None => settings_mapping(&update.settings)?,
                })
            }
            _ => None,
        };
        if let Some(delta) = delta {
            self.update_settings(&delta).map_err(request_error)?;
            return Ok(());
        }
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        let input = match &frame.data {
            FrameData::TtsSpeak(speak) => Some(SpeechInput {
                text: TtsTextChannels::identical(speak.text.clone()),
                append_to_context: speak.append_to_context,
                source_id: frame.id,
                destination: frame.transport_destination.clone(),
                aggregated_by: "sentence".into(),
                includes_inter_frame_spaces: false,
                skipped: false,
                interruptible: frame.interruptible,
            }),
            FrameData::AggregatedText(text) if text.text.skip_tts != Some(true) => {
                Some(SpeechInput {
                    text: TtsTextChannels {
                        synthesis: text.text.text.clone(),
                        display: text.text.text.clone(),
                        original: text
                            .raw_text
                            .clone()
                            .unwrap_or_else(|| text.text.text.clone()),
                    },
                    append_to_context: text.text.append_to_context,
                    source_id: frame.id,
                    destination: frame.transport_destination.clone(),
                    aggregated_by: text.aggregated_by.clone(),
                    includes_inter_frame_spaces: text.text.includes_inter_frame_spaces,
                    skipped: false,
                    interruptible: frame.interruptible,
                })
            }
            FrameData::TtsTextInput(text) => Some(SpeechInput {
                text: TtsTextChannels {
                    synthesis: text.synthesis_text.clone(),
                    display: text.display_text.clone(),
                    original: text.original_text.clone(),
                },
                append_to_context: text.append_to_context,
                source_id: frame.id,
                destination: frame.transport_destination.clone(),
                aggregated_by: text.aggregated_by.clone(),
                includes_inter_frame_spaces: text.includes_inter_frame_spaces,
                skipped: text.skip_tts,
                interruptible: frame.interruptible,
            }),
            _ => None,
        };
        if let Some(input) = input {
            return self.enqueue(Input::Speak(Box::new(input)), ctx);
        }
        match frame.data {
            FrameData::LlmResponseStart
            | FrameData::LlmResponseEnd
            | FrameData::Flush(_)
            | FrameData::FlushDetailed(_)
            | FrameData::AggregatedText(_) => self.enqueue(Input::Fence(Box::new(frame)), ctx),
            FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop
                if ctx.has_downstream_processor() =>
            {
                // Existing work has been drained by the runtime. Register a new
                // owned relay, return immediately, and let global quiescence see
                // this task. Awaiting our own task from prepare_end would deadlock.
                let output = ctx.clone();
                ctx.spawn("piper-terminal", false, async move {
                    output.push_downstream_terminal_wait(frame).await
                })?;
                Ok(())
            }
            _ => ctx.push(frame, direction).await,
        }
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cancel_relays(false)?;
        self.service.shutdown().await.map_err(request_error)
    }
}

fn resolved_settings(service: &PiperService, settings: PiperSettings) -> PiperSettings {
    let defaults = &service.voice_config().inference;
    PiperSettings {
        speaker_id: Some(settings.speaker_id.unwrap_or(0)),
        length_scale: Some(settings.length_scale.unwrap_or(defaults.length_scale)),
        noise_scale: Some(settings.noise_scale.unwrap_or(defaults.noise_scale)),
        noise_w: Some(settings.noise_w.unwrap_or(defaults.noise_w)),
    }
}

fn current(ctx: &ProcessorContext, revision: u64) -> bool {
    let health = ctx.health().snapshot();
    ctx.is_current() && health.is_usable && health.revision == revision
}
async fn push_revision(
    ctx: &ProcessorContext,
    frame: Frame,
    revision: u64,
    health: &mut watch::Receiver<ProcessorHealthSnapshot>,
) -> Result<bool, ProcessorError> {
    if !current(ctx, revision) {
        return Ok(false);
    }
    let push = ctx.push_downstream_wait(frame);
    tokio::pin!(push);
    loop {
        tokio::select! {
            biased;
            changed = health.changed() => {
                if changed.is_err() || !current(ctx, revision) { return Ok(false); }
            }
            result = &mut push => { result?; return Ok(current(ctx, revision)); }
        }
    }
}
fn decorate(frame: &mut Frame, input: &SpeechInput) {
    frame.transport_destination = input.destination.clone();
    frame.interruptible = input.interruptible;
    frame
        .metadata
        .insert("source_frame_id".into(), input.source_id.into());
}
async fn emit(
    frames: Vec<Frame>,
    input: &SpeechInput,
    ctx: &ProcessorContext,
    revision: u64,
    health: &mut watch::Receiver<ProcessorHealthSnapshot>,
) -> Result<bool, ProcessorError> {
    for mut frame in frames {
        decorate(&mut frame, input);
        if !push_revision(ctx, frame, revision, health).await? {
            return Ok(false);
        }
    }
    Ok(true)
}
async fn alignment_frames(
    events: Result<Vec<TtsAlignmentEvent>, TtsAlignmentError>,
    prepared: &mut Prepared,
    ctx: &ProcessorContext,
    health: &mut watch::Receiver<ProcessorHealthSnapshot>,
) -> Result<bool, ProcessorError> {
    let frames = prepared
        .mapper
        .convert(events.map_err(alignment_error)?)
        .map_err(alignment_error)?;
    emit(
        frames,
        &prepared.input,
        ctx,
        prepared.generation.snapshot.revision,
        health,
    )
    .await
}
async fn synthesize(
    mut prepared: Prepared,
    ctx: &ProcessorContext,
    failure: &AtomicBool,
    timeline: &AtomicU64,
    watchdog: &TtsAudioWatchdog,
    chunk_samples: usize,
) -> Result<(), ProcessorError> {
    // Subscribe before any clock callback or output publication can change
    // health. Recheck after Started and immediately before awaiting native work.
    // Skipped input carries display/source provenance without invoking a
    // provider, so an unusable native backend must not swallow that metadata.
    if prepared.pending.is_none() {
        for mut frame in std::mem::take(&mut prepared.initial) {
            decorate(&mut frame, &prepared.input);
            ctx.push_downstream_wait(frame).await?;
        }
        return Ok(());
    }
    let mut health = ctx.health().watch();
    let revision = prepared.generation.snapshot.revision;
    if !current(ctx, revision) {
        failure.store(true, Ordering::Release);
        return Ok(());
    }
    let initial = std::mem::take(&mut prepared.initial);
    if !emit(initial, &prepared.input, ctx, revision, &mut health).await? {
        failure.store(true, Ordering::Release);
        prepared.alignment.cancel();
        return Ok(());
    }
    let Some(pending) = prepared.pending.take() else {
        return Ok(());
    };
    let mut started = Frame::new(FrameData::TtsStarted(TtsStartedFrame {
        context_id: Some(prepared.context_id.clone()),
        append_to_context: prepared.input.append_to_context,
    }));
    decorate(&mut started, &prepared.input);
    if !push_revision(ctx, started, revision, &mut health).await? || !current(ctx, revision) {
        failure.store(true, Ordering::Release);
        prepared.alignment.cancel();
        return Ok(());
    }
    let native = pending.result();
    tokio::pin!(native);
    let result = loop {
        tokio::select! {
            biased;
            changed = health.changed() => {
                if changed.is_err() || !current(ctx, revision) {
                    failure.store(true, Ordering::Release); prepared.alignment.cancel(); return Ok(());
                }
            }
            result = &mut native => break result,
        }
    };
    if !current(ctx, revision) {
        failure.store(true, Ordering::Release);
        prepared.alignment.cancel();
        return Ok(());
    }
    let result = result.and_then(|audio| {
        if audio.context_id != prepared.context_id
            || audio.sample_rate != ctx.configuration().audio_out_sample_rate
            || audio.num_channels != 1
            || !audio.pcm.len().is_multiple_of(2)
        {
            return Err(PiperError::Invalid("native PCM/context contract mismatch"));
        }
        Ok(audio)
    });
    let mut received_audio = false;
    let mut succeeded = false;
    match result {
        Ok(audio) => {
            received_audio = !audio.pcm.is_empty();
            if received_audio {
                let events = prepared.alignment.start_audio(
                    ctx.observer_clock().now_ns(),
                    timeline.load(Ordering::Acquire),
                );
                if !alignment_frames(events, &mut prepared, ctx, &mut health).await? {
                    failure.store(true, Ordering::Release);
                    prepared.alignment.cancel();
                    return Ok(());
                }
                if ctx.configuration().enable_metrics {
                    let metric = Frame::new(FrameData::Metrics(vec![Metric::Ttfb {
                        processor: ctx.name().into(),
                        seconds: prepared.admitted.elapsed().as_secs_f64(),
                    }]));
                    if !push_revision(ctx, metric, revision, &mut health).await? {
                        failure.store(true, Ordering::Release);
                        return Ok(());
                    }
                }
                for pcm in audio.pcm.chunks(chunk_samples * 2) {
                    let mut frame = Frame::new(FrameData::TtsAudio(AudioRawFrame {
                        audio: pcm.to_vec(),
                        sample_rate: audio.sample_rate,
                        num_channels: 1,
                        context_id: Some(prepared.context_id.clone()),
                    }));
                    decorate(&mut frame, &prepared.input);
                    if !push_revision(ctx, frame, revision, &mut health).await? {
                        failure.store(true, Ordering::Release);
                        prepared.alignment.cancel();
                        return Ok(());
                    }
                }
                succeeded = true;
            }
        }
        Err(error) => {
            ctx.push_error(error_frame(error)).await?;
        }
    }
    // Only completed native contexts count. Local admission rejection and any
    // aborted relay return before this point.
    let silent = watchdog
        .complete(
            ctx,
            prepared.generation,
            &prepared.context_id,
            received_audio,
        )
        .await?;
    if silent || !received_audio {
        succeeded = false;
    }
    if !current(ctx, revision) {
        failure.store(true, Ordering::Release);
        prepared.alignment.cancel();
        return Ok(());
    }
    if succeeded {
        let events = prepared.alignment.finish_success(
            ctx.observer_clock().now_ns(),
            timeline.load(Ordering::Acquire),
        );
        succeeded = alignment_frames(events, &mut prepared, ctx, &mut health).await?;
    }
    let mut stopped = Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
        context_id: Some(prepared.context_id.clone()),
    }));
    decorate(&mut stopped, &prepared.input);
    if succeeded {
        let pts = prepared.alignment.boundary_pts().map_err(alignment_error)?;
        stopped.set_pipeline_pts(pts);
        timeline.fetch_max(pts, Ordering::AcqRel);
    } else {
        failure.store(true, Ordering::Release);
        prepared.alignment.cancel();
        stopped.metadata.insert("failed".into(), true.into());
    }
    if !push_revision(ctx, stopped, revision, &mut health).await? {
        failure.store(true, Ordering::Release);
        return Ok(());
    }
    if succeeded {
        let mut metrics = Vec::new();
        if ctx.configuration().enable_metrics {
            metrics.push(Metric::Processing {
                processor: ctx.name().into(),
                seconds: prepared.admitted.elapsed().as_secs_f64(),
            });
        }
        if ctx.configuration().enable_usage_metrics {
            metrics.push(Metric::TtsUsage {
                processor: ctx.name().into(),
                characters: prepared.input.text.synthesis.chars().count() as u64,
            });
        }
        if !metrics.is_empty()
            && !push_revision(
                ctx,
                Frame::new(FrameData::Metrics(metrics)),
                revision,
                &mut health,
            )
            .await?
        {
            failure.store(true, Ordering::Release);
        }
    }
    Ok(())
}
fn error_frame(error: PiperError) -> ErrorFrame {
    let category = match error {
        PiperError::Invalid(_)
        | PiperError::Unsupported(_)
        | PiperError::TooLarge
        | PiperError::UnknownPhoneme(_) => ErrorCategory::InvalidRequest,
        _ => ErrorCategory::Application,
    };
    ErrorFrame {
        category,
        ..ErrorFrame::new(error.to_string())
    }
}
fn request_error(error: PiperError) -> ProcessorError {
    let error = error_frame(error);
    ProcessorError::new(error.error)
        .with_category(error.category)
        .with_permanence_override(false)
}
fn state_error() -> ProcessorError {
    ProcessorError::fatal("Piper pipeline state lock poisoned")
}
fn alignment_error(_: TtsAlignmentError) -> ProcessorError {
    ProcessorError::new("Piper alignment validation failed")
        .with_category(ErrorCategory::Application)
}
fn targets(processor: &dyn FrameProcessor, target: &Option<ProcessorReference>) -> bool {
    match target {
        None => true,
        Some(ProcessorReference::Name(name)) => name == processor.name(),
        Some(ProcessorReference::Local(target)) => std::ptr::addr_eq(target.0.as_ref(), processor),
    }
}
fn settings_mapping(
    values: &std::collections::BTreeMap<String, serde_json::Value>,
) -> Result<TtsSettings, ProcessorError> {
    let mut delta = TtsSettings::default();
    for (key, value) in values {
        match key.as_str() {
            "model" | "voice" | "language" => {
                let setting = if value.is_null() {
                    Setting::Null
                } else {
                    Setting::Value(
                        value
                            .as_str()
                            .ok_or_else(|| {
                                request_error(PiperError::Invalid("string setting expected"))
                            })?
                            .to_owned(),
                    )
                };
                match key.as_str() {
                    "model" => delta.base.model = setting,
                    "voice" => delta.voice = setting,
                    _ => delta.language = setting,
                }
            }
            _ => {
                delta.base.extra.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(delta)
}
