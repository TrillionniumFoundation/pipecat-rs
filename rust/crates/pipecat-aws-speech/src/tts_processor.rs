//! Ordered Polly pipeline synthesis, sentence aggregation, alignment and lifecycle.
use crate::{settings, PollyClient, SpeechError};
use pipecat_core::{
    async_trait, AggregatedTextFrame, AudioRawFrame, Direction, ErrorCategory, ErrorFrame, Frame,
    FrameData, FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorHealthGeneration, ProcessorReference, StartFrame, TextFrame, TtsSettings,
    TtsStartedFrame, TtsStoppedFrame,
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

static NEXT_TTS_CONTEXT: AtomicU64 = AtomicU64::new(1);

struct Pending {
    done: CancellationToken,
    stop: CancellationToken,
    interruptible: bool,
    speech: bool,
}
struct State {
    backend: PollyClient,
    pending: Vec<Pending>,
    failure: Arc<AtomicBool>,
}
/// Synthesizes downstream complete utterances, including three-channel `TtsTextInput`.
/// All other frames pass through; `LlmResponseEnd` and downstream flush probes
/// wait behind accepted utterances. Requests are sequential, bounded and owned by
/// the core runtime. End drains requests; interruption/Cancel discard stale audio.
/// Emits lifecycle, PCM, source anchors and completion diagnostics, never spoken text.
pub struct PollyTtsProcessor {
    health: ProcessorHealth,
    max_pending: usize,
    state: Mutex<State>,
    timeline: Arc<AtomicU64>,
    audio_watchdog: TtsAudioWatchdog,
    text: Mutex<crate::text::TextState>,
}
impl PollyTtsProcessor {
    pub fn new(backend: PollyClient, max_pending: usize) -> Result<Self, SpeechError> {
        if max_pending == 0 {
            return Err(SpeechError::Invalid("pending utterance limit"));
        }
        backend.config().settings.validate()?;
        Ok(Self {
            health: ProcessorHealth::default(),
            max_pending,
            timeline: Arc::default(),
            audio_watchdog: TtsAudioWatchdog::default(),
            text: Mutex::new(crate::text::TextState::new(
                &backend.config().settings.language,
            )),
            state: Mutex::new(State {
                backend,
                pending: vec![],
                failure: Arc::default(),
            }),
        })
    }
    /// Zero disables only the permanent cutoff; each silent context is reported.
    pub fn with_max_consecutive_zero_audio_contexts(mut self, maximum: usize) -> Self {
        self.audio_watchdog = TtsAudioWatchdog::new(maximum);
        self
    }
    /// Apply supported Polly voice, language and prosody changes to future utterances.
    /// An invalid or ineffective delta does not restore provider usability.
    pub fn update_settings(&self, delta: &TtsSettings) -> Result<bool, SpeechError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SpeechError::Invalid("TTS state lock poisoned"))?;
        let Some(candidate) = settings::polly(&state.backend, delta)? else {
            return Ok(false);
        };
        self.text
            .lock()
            .map_err(|_| SpeechError::Invalid("Polly text lock poisoned"))?
            .set_language(&candidate.config().settings.language);
        state.backend = candidate;
        self.health.set_usable(true);
        Ok(true)
    }
    fn cancel(&self, interruption_only: bool) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("Polly TTS state lock poisoned"))?;
        state.pending.retain(|task| {
            if !interruption_only || task.interruptible {
                task.stop.cancel();
                false
            } else {
                true
            }
        });
        state.failure = Arc::default();
        Ok(())
    }
    fn enqueue(&self, command: Command, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("Polly TTS state lock poisoned"))?;
        state.pending.retain(|p| !p.done.is_cancelled());
        if let Command::Speak(input) = &command {
            let max_text = TtsAlignmentLimits::default()
                .sequencer
                .sequencer
                .alignment
                .max_text_bytes;
            if [
                &input.text.synthesis,
                &input.text.display,
                &input.text.original,
            ]
            .iter()
            .any(|text| text.len() > max_text)
                || input.aggregated_by.len() > 128
                || input
                    .destination
                    .as_ref()
                    .is_some_and(|destination| destination.len() > 1024)
            {
                state.failure.store(true, Ordering::Release);
                return Err(ProcessorError::new("Polly TTS provenance limit exceeded")
                    .with_category(ErrorCategory::Application));
            }
        }
        let speech = matches!(&command, Command::Speak(input) if !input.skipped);
        if speech && !ctx.is_usable() {
            state.failure.store(true, Ordering::Release);
            return Ok(());
        }
        if state
            .pending
            .iter()
            .filter(|pending| pending.speech == speech)
            .count()
            >= self.max_pending
        {
            state.failure.store(true, Ordering::Release);
            return Err(
                ProcessorError::new("Polly TTS pending utterance limit reached")
                    .with_category(ErrorCategory::Application),
            );
        }
        let previous = state.pending.last().map(|p| p.done.clone());
        let done = CancellationToken::new();
        let stop = CancellationToken::new();
        let guard = DoneOnDrop(done.clone());
        let cancel = stop.clone();
        let backend = state.backend.clone();
        let generation = ctx.health().generation();
        let audio_watchdog = self.audio_watchdog.clone();
        let timeline = self.timeline.clone();
        let ctx_output = ctx.clone();
        let failure = state.failure.clone();
        let interruptible = !matches!(&command, Command::Fence(frame) if !frame.interruptible);
        ctx.spawn("aws-polly-utterance", interruptible, async move {
            let _guard = guard;
            let operation = async {
                if let Some(previous) = previous { previous.cancelled().await; }
                match command {
                    Command::Speak(input) => {
                        synthesize(backend, *input, ctx_output, failure, generation, timeline, audio_watchdog).await
                    }
                    Command::Fence(mut frame) => {
                        if matches!(frame.data, FrameData::LlmResponseEnd) {
                            if failure.load(Ordering::Acquire) { frame.metadata.insert("failed".into(), true.into()); }
                            frame.set_pipeline_pts(timeline.load(Ordering::Acquire));
                        }
                        ctx_output.push(*frame, Direction::Downstream).await
                    }
                }
            };
            tokio::select! { biased; _ = cancel.cancelled() => Ok(()), result = operation => result }
        })?;
        state.pending.push(Pending {
            done,
            stop,
            interruptible,
            speech,
        });
        Ok(())
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
}
enum Command {
    Speak(Box<SpeechInput>),
    Fence(Box<Frame>),
}
struct DoneOnDrop(CancellationToken);
impl Drop for DoneOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
#[async_trait]
impl FrameProcessor for PollyTtsProcessor {
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    fn name(&self) -> &str {
        "AWSPollyTTSService"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let rate = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?
            .backend
            .config()
            .sample_rate;
        if config.audio_out_sample_rate != rate {
            return Err(ProcessorError::fatal(
                "pipeline output sample rate differs from Polly TTS configuration",
            )
            .with_category(ErrorCategory::InvalidRequest));
        }
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let foreign_settings = match &frame.data {
            FrameData::TtsUpdateSettings(update) => !targets(self, &update.service),
            FrameData::ServiceUpdateSettings(update) => !targets(self, &update.service),
            _ => false,
        };
        if foreign_settings {
            return ctx.push(frame, direction).await;
        }
        let frames = self
            .text
            .lock()
            .map_err(|_| ProcessorError::fatal("Polly text lock poisoned"))?
            .prepare(frame, direction)?;
        for frame in frames {
            self.process_prepared(frame, direction, ctx).await?;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.text
            .lock()
            .map_err(|_| ProcessorError::fatal("Polly text lock poisoned"))?
            .reset();
        self.cancel(false)
    }
}
impl PollyTtsProcessor {
    async fn process_prepared(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::Interruption | FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            self.cancel(matches!(frame.data, FrameData::Interruption))?;
        }
        let delta = match &frame.data {
            FrameData::TtsUpdateSettings(update) if targets(self, &update.service) => {
                Some(match &update.delta {
                    Some(delta) => delta.clone(),
                    None => settings_mapping(&update.settings).map_err(settings_error)?,
                })
            }
            FrameData::ServiceUpdateSettings(update) if targets(self, &update.service) => {
                Some(match &update.delta {
                    Some(delta) => TtsSettings {
                        base: delta.clone(),
                        ..Default::default()
                    },
                    None => settings_mapping(&update.settings).map_err(settings_error)?,
                })
            }
            _ => None,
        };
        if let Some(delta) = delta {
            self.update_settings(&delta).map_err(settings_error)?;
            return Ok(());
        }
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        match &frame.data {
            FrameData::TtsSpeak(speak) if !speak.text.is_empty() => {
                self.enqueue(
                    Command::Speak(Box::new(SpeechInput {
                        text: TtsTextChannels::identical(speak.text.clone()),
                        append_to_context: speak.append_to_context,
                        source_id: frame
                            .metadata
                            .get("source_frame_id")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(frame.id),
                        destination: frame.transport_destination.clone(),
                        aggregated_by: "sentence".into(),
                        includes_inter_frame_spaces: false,
                        skipped: false,
                    })),
                    ctx,
                )?;
                return Ok(());
            }
            FrameData::AggregatedText(text)
                if text.text.skip_tts != Some(true) && !text.text.text.is_empty() =>
            {
                self.enqueue(
                    Command::Speak(Box::new(SpeechInput {
                        text: TtsTextChannels {
                            synthesis: text.text.text.clone(),
                            display: text.text.text.clone(),
                            original: text
                                .raw_text
                                .clone()
                                .unwrap_or_else(|| text.text.text.clone()),
                        },
                        append_to_context: text.text.append_to_context,
                        source_id: frame
                            .metadata
                            .get("source_frame_id")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(frame.id),
                        destination: frame.transport_destination.clone(),
                        aggregated_by: text.aggregated_by.clone(),
                        includes_inter_frame_spaces: text.text.includes_inter_frame_spaces,
                        skipped: false,
                    })),
                    ctx,
                )?;
                return Ok(());
            }
            FrameData::AggregatedText(text) if text.text.skip_tts == Some(true) => {
                // Preserve an existing skipped frame's identity, content and routing.
                self.enqueue(Command::Fence(Box::new(frame)), ctx)?;
                return Ok(());
            }
            FrameData::TtsTextInput(text) => {
                self.enqueue(
                    Command::Speak(Box::new(SpeechInput {
                        text: TtsTextChannels {
                            synthesis: text.synthesis_text.clone(),
                            display: text.display_text.clone(),
                            original: text.original_text.clone(),
                        },
                        append_to_context: text.append_to_context,
                        source_id: frame
                            .metadata
                            .get("source_frame_id")
                            .and_then(serde_json::Value::as_u64)
                            .unwrap_or(frame.id),
                        destination: frame.transport_destination.clone(),
                        aggregated_by: text.aggregated_by.clone(),
                        includes_inter_frame_spaces: text.includes_inter_frame_spaces,
                        skipped: text.skip_tts,
                    })),
                    ctx,
                )?;
                return Ok(());
            }
            FrameData::LlmResponseStart => {
                self.state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("Polly TTS state lock poisoned"))?
                    .failure = Arc::new(AtomicBool::new(!ctx.is_usable()));
            }
            FrameData::LlmResponseEnd
            | FrameData::LlmFullResponseEnd(_)
            | FrameData::VisionFullResponseEnd(_) => {
                self.enqueue(Command::Fence(Box::new(frame)), ctx)?;
                return Ok(());
            }
            FrameData::FlushDetailed(probe) if !probe.returning => {
                self.enqueue(Command::Fence(Box::new(frame)), ctx)?;
                return Ok(());
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
}
/// Each complete utterance owns an immutable provider/alignment context. Even
/// adjacent utterances cannot inherit another destination, spacing or append policy.
#[allow(clippy::too_many_arguments)]
async fn synthesize(
    backend: PollyClient,
    input: SpeechInput,
    ctx: ProcessorContext,
    failure: Arc<AtomicBool>,
    generation: ProcessorHealthGeneration,
    timeline: Arc<AtomicU64>,
    audio_watchdog: TtsAudioWatchdog,
) -> Result<(), ProcessorError> {
    let revision = generation.snapshot.revision;
    let mut health = ctx.health().watch();
    let serial = NEXT_TTS_CONTEXT
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| ProcessorError::fatal("Polly TTS context identity exhausted"))?;
    let context_id = format!("aws-polly-{serial}");
    if input.skipped {
        let mut text = TextFrame::llm(input.text.display);
        text.append_to_context = input.append_to_context;
        text.includes_inter_frame_spaces = input.includes_inter_frame_spaces;
        text.skip_tts = Some(true);
        let mut frame = Frame::new(FrameData::AggregatedText(AggregatedTextFrame {
            text,
            aggregated_by: input.aggregated_by,
            raw_text: Some(input.text.original),
            context_id: Some(context_id),
            will_be_spoken: false,
        }));
        frame.transport_destination = input.destination;
        frame
            .metadata
            .insert("source_frame_id".into(), input.source_id.into());
        frame.set_pipeline_pts(timeline.load(Ordering::Acquire));
        return ctx.push(frame, Direction::Downstream).await;
    }
    if !same_generation(&ctx, revision) {
        failure.store(true, Ordering::Release);
        return Ok(());
    }
    if input.text.synthesis.is_empty() {
        return Ok(());
    }
    let characters = input.text.synthesis.chars().count() as u64;
    let limits = TtsAlignmentLimits::default();
    let mut alignment = match TtsAlignment::new(
        revision,
        input.append_to_context,
        TokenReporting::SynthesisCompletion,
        None,
        limits,
    ) {
        Ok(alignment) => alignment,
        Err(_) => return alignment_error(&ctx, &failure).await,
    };
    let mut mapper = match TtsAlignmentFrames::new(
        context_id.clone(),
        alignment.context(),
        input.append_to_context,
        limits.sequencer.sequencer.max_slots,
    ) {
        Ok(mapper) => mapper,
        Err(_) => return alignment_error(&ctx, &failure).await,
    };
    // Admission precedes network submission. Oversized provenance never sends
    // a request that cannot be represented by the bounded alignment bridge.
    let registered = alignment.register_prepared(
        &input.text,
        true,
        &input.aggregated_by,
        input.includes_inter_frame_spaces,
    );
    if !emit_alignment(registered, &mut mapper, &input, &ctx, &failure, revision).await? {
        return Ok(());
    }
    let mut stream = match backend.synthesize(input.text.synthesis.clone()) {
        Ok(stream) => stream,
        Err(error) => {
            failure.store(true, Ordering::Release);
            return ctx.push_error(error_frame(error)).await;
        }
    };
    let started = tokio::time::Instant::now();
    let mut first_audio = true;
    let mut source_audio = false;
    let mut succeeded = true;
    let mut start = Frame::new(FrameData::TtsStarted(TtsStartedFrame {
        context_id: Some(context_id.clone()),
        append_to_context: input.append_to_context,
    }));
    start.transport_destination = input.destination.clone();
    ctx.push(start, Direction::Downstream).await?;
    loop {
        let chunk = tokio::select! {
            biased;
            _ = health.changed() => {
                failure.store(true, Ordering::Release);
                succeeded = false;
                break;
            }
            chunk = stream.next() => chunk,
        };
        if !same_generation(&ctx, revision) {
            failure.store(true, Ordering::Release);
            succeeded = false;
            break;
        }
        let Some(chunk) = chunk else {
            break;
        };
        match chunk {
            Ok(chunk) => {
                if first_audio && ctx.configuration().enable_metrics {
                    ctx.push(
                        Frame::new(FrameData::Metrics(vec![Metric::Ttfb {
                            processor: ctx.name().into(),
                            seconds: started.elapsed().as_secs_f64(),
                        }])),
                        Direction::Downstream,
                    )
                    .await?;
                }
                if first_audio {
                    let events = alignment.start_audio(
                        ctx.observer_clock().now_ns(),
                        timeline.load(Ordering::Acquire),
                    );
                    if !emit_alignment(events, &mut mapper, &input, &ctx, &failure, revision)
                        .await?
                    {
                        succeeded = false;
                        break;
                    }
                }
                if !same_generation(&ctx, revision) {
                    failure.store(true, Ordering::Release);
                    succeeded = false;
                    break;
                }
                first_audio = false;
                let mut frame = Frame::new(FrameData::TtsAudio(AudioRawFrame {
                    audio: chunk.pcm,
                    sample_rate: chunk.sample_rate,
                    num_channels: chunk.num_channels,
                    context_id: Some(context_id.clone()),
                }));
                frame.transport_destination = input.destination.clone();
                frame
                    .metadata
                    .insert("source_frame_id".into(), input.source_id.into());
                ctx.push(frame, Direction::Downstream).await?;
            }
            Err(SpeechError::EmptyAudio) => break,
            Err(error) => {
                failure.store(true, Ordering::Release);
                succeeded = false;
                ctx.push_error(error_frame(error)).await?;
                break;
            }
        }
    }
    source_audio |= stream.source_audio_observed();
    drop(stream);
    if succeeded {
        let events = alignment.finish_success(
            ctx.observer_clock().now_ns(),
            timeline.load(Ordering::Acquire),
        );
        succeeded = emit_alignment(events, &mut mapper, &input, &ctx, &failure, revision).await?;
    }
    if !same_generation(&ctx, revision) {
        failure.store(true, Ordering::Release);
        succeeded = false;
    }
    let mut stopped = Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
        context_id: Some(context_id.clone()),
    }));
    stopped.transport_destination = input.destination;
    if succeeded {
        let pts = alignment
            .boundary_pts()
            .map_err(|_| ProcessorError::new("Polly TTS timestamp overflow"))?;
        stopped.set_pipeline_pts(pts);
        timeline.fetch_max(pts, Ordering::AcqRel);
    } else {
        alignment.cancel();
        stopped.metadata.insert("failed".into(), true.into());
    }
    ctx.push(stopped, Direction::Downstream).await?;
    if audio_watchdog
        .complete(&ctx, generation, &context_id, !first_audio || source_audio)
        .await?
    {
        failure.store(true, Ordering::Release);
    }
    if succeeded && same_generation(&ctx, revision) && ctx.configuration().enable_usage_metrics {
        ctx.push(
            Frame::new(FrameData::Metrics(vec![Metric::TtsUsage {
                processor: ctx.name().into(),
                characters,
            }])),
            Direction::Downstream,
        )
        .await?;
    }
    Ok(())
}
async fn alignment_error(
    ctx: &ProcessorContext,
    failure: &AtomicBool,
) -> Result<(), ProcessorError> {
    failure.store(true, Ordering::Release);
    ctx.push_error(error_frame(SpeechError::Protocol(
        "TTS alignment validation failed",
    )))
    .await
}
async fn emit_alignment(
    events: Result<Vec<TtsAlignmentEvent>, TtsAlignmentError>,
    mapper: &mut TtsAlignmentFrames,
    input: &SpeechInput,
    ctx: &ProcessorContext,
    failure: &AtomicBool,
    revision: u64,
) -> Result<bool, ProcessorError> {
    if !same_generation(ctx, revision) {
        failure.store(true, Ordering::Release);
        return Ok(false);
    }
    let frames = match events.and_then(|events| mapper.convert(events)) {
        Ok(frames) => frames,
        Err(_) => {
            alignment_error(ctx, failure).await?;
            return Ok(false);
        }
    };
    for mut frame in frames {
        if !same_generation(ctx, revision) {
            failure.store(true, Ordering::Release);
            return Ok(false);
        }
        frame.transport_destination = input.destination.clone();
        frame
            .metadata
            .insert("source_frame_id".into(), input.source_id.into());
        ctx.push(frame, Direction::Downstream).await?;
    }
    Ok(true)
}
fn error_frame(error: SpeechError) -> ErrorFrame {
    let category = match error {
        SpeechError::Http(status) => ErrorCategory::from_http_status(status),
        SpeechError::Invalid(_) | SpeechError::Audio(_) => ErrorCategory::InvalidRequest,
        SpeechError::Transport | SpeechError::Timeout => ErrorCategory::Connectivity,
        _ => ErrorCategory::Application,
    };
    ErrorFrame {
        error: error.to_string(),
        category,
        ..ErrorFrame::new("")
    }
}

fn same_generation(ctx: &ProcessorContext, revision: u64) -> bool {
    let snapshot = ctx.health().snapshot();
    ctx.is_current() && snapshot.is_usable && snapshot.revision == revision
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
) -> Result<TtsSettings, SpeechError> {
    settings::tts_mapping(values).map_err(|_| SpeechError::Invalid("Polly settings"))
}
fn settings_error(error: SpeechError) -> ProcessorError {
    let error = error_frame(error);
    ProcessorError::new(error.error)
        .with_category(error.category)
        .with_permanence_override(false)
}
