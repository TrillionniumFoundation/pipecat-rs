//! Pipeline integration for complete utterances. A text aggregator should precede
//! this processor when consuming incremental LLM text.
use crate::{
    cartesia::{CartesiaHttpClient, CartesiaSynthesisRequest},
    deepgram::{DeepgramHttpClient, DeepgramSynthesisRequest},
    elevenlabs::{
        ElevenLabsEvent, ElevenLabsHttpClient, ElevenLabsStream, ElevenLabsSynthesisRequest,
    },
    SpeechChunk, SpeechClient, SpeechError, SpeechStream, SynthesisRequest,
};
use pipecat_core::{
    async_trait, AggregatedTextFrame, AudioRawFrame, Direction, ErrorCategory, ErrorFrame, Frame,
    FrameData, FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorHealthGeneration, ProcessorReference, Setting, StartFrame, TextFrame, TtsSettings,
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

#[cfg(feature = "speechmatics")]
use crate::speechmatics::{SpeechmaticsHttpClient, SpeechmaticsSynthesisRequest};

#[cfg(feature = "speechify")]
use crate::speechify::{SpeechifyEvent, SpeechifyHttpClient, SpeechifySynthesisRequest};

#[cfg(feature = "minimax")]
use crate::minimax::{MiniMaxHttpClient, MiniMaxSynthesisRequest};

#[cfg(feature = "deepgram-ws")]
use crate::{
    deepgram::DeepgramEncoding,
    deepgram_ws::{DeepgramWsClient, DeepgramWsStream},
};
#[cfg(feature = "deepgram-ws")]
use pipecat_core::{InvocationFence, ObservedProcessor};

static NEXT_TTS_CONTEXT: AtomicU64 = AtomicU64::new(1);

/// Immutable provider settings copied for every accepted complete utterance.
/// The request's text field is overwritten with the incoming frame's text.
#[derive(Clone)]
pub enum HttpTtsBackend {
    #[cfg(feature = "minimax")]
    MiniMax {
        client: MiniMaxHttpClient,
        request: MiniMaxSynthesisRequest,
    },
    #[cfg(feature = "speechify")]
    Speechify {
        client: SpeechifyHttpClient,
        request: SpeechifySynthesisRequest,
    },
    #[cfg(feature = "speechmatics")]
    Speechmatics {
        client: SpeechmaticsHttpClient,
        request: SpeechmaticsSynthesisRequest,
    },
    OpenAi {
        client: SpeechClient,
        request: SynthesisRequest,
    },
    ElevenLabs {
        client: ElevenLabsHttpClient,
        request: ElevenLabsSynthesisRequest,
    },
    Deepgram {
        client: DeepgramHttpClient,
        request: DeepgramSynthesisRequest,
    },
    /// One fresh socket per complete utterance; PCM16 mono only.
    #[cfg(feature = "deepgram-ws")]
    DeepgramWebSocket {
        client: DeepgramWsClient,
        request: DeepgramSynthesisRequest,
    },
    Cartesia {
        client: CartesiaHttpClient,
        request: CartesiaSynthesisRequest,
    },
}
impl HttpTtsBackend {
    pub fn sample_rate(&self) -> u32 {
        match self {
            #[cfg(feature = "minimax")]
            Self::MiniMax { request, .. } => request.sample_rate,
            #[cfg(feature = "speechify")]
            Self::Speechify { request, .. } => request.output_sample_rate(),
            #[cfg(feature = "speechmatics")]
            Self::Speechmatics { request, .. } => request.sample_rate,
            Self::OpenAi { .. } => 24000,
            Self::ElevenLabs { request, .. } => request.sample_rate,
            Self::Deepgram { request, .. } => request.sample_rate,
            #[cfg(feature = "deepgram-ws")]
            Self::DeepgramWebSocket { request, .. } => request.sample_rate,
            Self::Cartesia { request, .. } => request.sample_rate,
        }
    }
    fn reporting(&self) -> TokenReporting {
        match self {
            #[cfg(feature = "speechify")]
            Self::Speechify { .. } => TokenReporting::WordTimestamps,
            _ => TokenReporting::SynthesisCompletion,
        }
    }
    fn validate(&self) -> Result<(), SpeechError> {
        match self {
            #[cfg(feature = "minimax")]
            Self::MiniMax { request, .. } => {
                let mut request = request.clone();
                request.text = "validation".into();
                request.validate()
            }
            #[cfg(feature = "speechify")]
            Self::Speechify { request, .. } => {
                let mut request = request.clone();
                request.text = "validation".into();
                request.validate()
            }
            #[cfg(feature = "speechmatics")]
            Self::Speechmatics { request, .. } => {
                let mut request = request.clone();
                request.text = "validation".into();
                request.validate()
            }
            Self::OpenAi { request, .. } => {
                let mut request = request.clone();
                request.input = "validation".into();
                request.validate()
            }
            Self::ElevenLabs { request, .. } => {
                let mut request = request.clone();
                request.text = "validation".into();
                request.validate()
            }
            Self::Deepgram { request, .. } => {
                let mut request = request.clone();
                request.text = "validation".into();
                request.validate()
            }
            #[cfg(feature = "deepgram-ws")]
            Self::DeepgramWebSocket { request, .. } => {
                if request.encoding != DeepgramEncoding::Linear16 {
                    return Err(SpeechError::Invalid(
                        "Deepgram WS supports only mono linear16",
                    ));
                }
                if request.voice.len() > 256 {
                    return Err(SpeechError::TooLarge);
                }
                let mut request = request.clone();
                request.text = "validation".into();
                request.validate()
            }
            Self::Cartesia { request, .. } => {
                let mut request = request.clone();
                request.text = "validation".into();
                request.validate()
            }
        }
    }
    fn updated(&self, delta: &TtsSettings) -> Result<Option<Self>, SpeechError> {
        if !delta.base.extra.is_empty() {
            #[cfg(feature = "speechmatics")]
            let supported = matches!(self, Self::Speechmatics { .. })
                && delta.base.extra.keys().all(|key| key == "max_retries");
            #[cfg(not(feature = "speechmatics"))]
            let supported = false;
            if !supported {
                return Err(SpeechError::Invalid("unsupported HTTP TTS setting"));
            }
        }
        let mut candidate = self.clone();
        let mut changed = false;
        match &mut candidate {
            #[cfg(feature = "minimax")]
            Self::MiniMax { request, .. } => {
                changed |= required(&mut request.model, &delta.base.model)?;
                changed |= required(&mut request.voice, &delta.voice)?;
                changed |= optional(&mut request.language, &delta.language)?;
            }
            #[cfg(feature = "speechify")]
            Self::Speechify { request, .. } => {
                changed |= required(&mut request.model, &delta.base.model)?;
                changed |= required(&mut request.voice, &delta.voice)?;
                changed |= optional(&mut request.language, &delta.language)?;
            }
            #[cfg(feature = "speechmatics")]
            Self::Speechmatics { request, .. } => {
                if !matches!(delta.base.model, Setting::NotGiven)
                    || !matches!(delta.language, Setting::NotGiven)
                {
                    return Err(SpeechError::Invalid(
                        "Speechmatics voice determines model and language",
                    ));
                }
                changed |= required(&mut request.voice, &delta.voice)?;
                if let Some(value) = delta.base.extra.get("max_retries") {
                    let retries = value
                        .as_u64()
                        .and_then(|value| u8::try_from(value).ok())
                        .filter(|value| *value <= 32)
                        .ok_or(SpeechError::Invalid("Speechmatics max_retries"))?;
                    changed |= request.max_retries != retries;
                    request.max_retries = retries;
                }
            }
            Self::OpenAi { request, .. } => {
                if !matches!(delta.language, Setting::NotGiven) {
                    return Err(SpeechError::Invalid(
                        "OpenAI HTTP TTS does not accept language",
                    ));
                }
                changed |= required(&mut request.model, &delta.base.model)?;
                changed |= required(&mut request.voice, &delta.voice)?;
            }
            Self::ElevenLabs { request, .. } => {
                changed |= required(&mut request.model, &delta.base.model)?;
                changed |= required(&mut request.voice_id, &delta.voice)?;
                changed |= optional(&mut request.language_code, &delta.language)?;
            }
            Self::Cartesia { request, .. } => {
                changed |= required(&mut request.model, &delta.base.model)?;
                changed |= required(&mut request.voice_id, &delta.voice)?;
                changed |= optional(&mut request.language, &delta.language)?;
            }
            Self::Deepgram { request, .. } => {
                if !matches!(delta.language, Setting::NotGiven)
                    || (!matches!(delta.base.model, Setting::NotGiven)
                        && !matches!(delta.voice, Setting::NotGiven)
                        && delta.base.model != delta.voice)
                {
                    return Err(SpeechError::Invalid(
                        "Deepgram voice determines model and language",
                    ));
                }
                changed |= required(&mut request.voice, &delta.base.model)?;
                changed |= required(&mut request.voice, &delta.voice)?;
            }
            #[cfg(feature = "deepgram-ws")]
            Self::DeepgramWebSocket { request, .. } => {
                if !matches!(delta.language, Setting::NotGiven)
                    || (!matches!(delta.base.model, Setting::NotGiven)
                        && !matches!(delta.voice, Setting::NotGiven)
                        && delta.base.model != delta.voice)
                {
                    return Err(SpeechError::Invalid(
                        "Deepgram voice determines model and language",
                    ));
                }
                changed |= required(&mut request.voice, &delta.base.model)?;
                changed |= required(&mut request.voice, &delta.voice)?;
            }
        }
        candidate.validate()?;
        Ok(changed.then_some(candidate))
    }
    fn synthesize(&self, text: String) -> Result<AudioStream, SpeechError> {
        match self {
            #[cfg(feature = "minimax")]
            Self::MiniMax { client, request } => {
                let mut request = request.clone();
                request.text = text;
                client.synthesize(request).map(AudioStream::Raw)
            }
            #[cfg(feature = "speechify")]
            Self::Speechify { client, request } => {
                let mut request = request.clone();
                request.text = text;
                client.synthesize(request).map(AudioStream::Speechify)
            }
            #[cfg(feature = "speechmatics")]
            Self::Speechmatics { client, request } => {
                let mut request = request.clone();
                request.text = text;
                client.synthesize(request).map(AudioStream::Raw)
            }
            Self::OpenAi { client, request } => {
                let mut request = request.clone();
                request.input = text;
                client.synthesize(request).map(AudioStream::Raw)
            }
            Self::ElevenLabs { client, request } => {
                let mut request = request.clone();
                request.text = text;
                client.synthesize(request).map(AudioStream::ElevenLabs)
            }
            Self::Deepgram { client, request } => {
                let mut request = request.clone();
                request.text = text;
                client.synthesize(request).map(AudioStream::Raw)
            }
            #[cfg(feature = "deepgram-ws")]
            Self::DeepgramWebSocket { client, request } => {
                let mut request = request.clone();
                request.text = text;
                client.synthesize(request).map(AudioStream::DeepgramWs)
            }
            Self::Cartesia { client, request } => {
                let mut request = request.clone();
                request.text = text;
                client.synthesize(request).map(AudioStream::Raw)
            }
        }
    }
}
enum AudioStream {
    #[cfg(feature = "deepgram-ws")]
    DeepgramWs(DeepgramWsStream),
    #[cfg(feature = "speechify")]
    Speechify(crate::ResponseStream<SpeechifyEvent>),
    Raw(SpeechStream),
    ElevenLabs(ElevenLabsStream),
}
enum AudioEvent {
    Audio(SpeechChunk),
    #[cfg(feature = "speechify")]
    Word {
        word: String,
        offset_seconds: f64,
    },
    #[cfg(feature = "speechify")]
    Done {
        duration_seconds: f64,
    },
}
impl AudioStream {
    fn source_audio_observed(&self) -> bool {
        match self {
            #[cfg(feature = "deepgram-ws")]
            Self::DeepgramWs(stream) => stream.source_audio_observed(),
            #[cfg(feature = "speechify")]
            Self::Speechify(stream) => stream.source_audio_observed(),
            Self::Raw(stream) => stream.source_audio_observed(),
            Self::ElevenLabs(stream) => stream.source_audio_observed(),
        }
    }
    async fn next(&mut self) -> Option<Result<AudioEvent, SpeechError>> {
        match self {
            #[cfg(feature = "deepgram-ws")]
            Self::DeepgramWs(stream) => stream
                .next()
                .await
                .map(|chunk| chunk.map(AudioEvent::Audio)),
            #[cfg(feature = "speechify")]
            Self::Speechify(stream) => stream.next().await.map(|event| {
                event.map(|event| match event {
                    SpeechifyEvent::Audio(audio) => AudioEvent::Audio(audio),
                    SpeechifyEvent::Word {
                        word,
                        offset_seconds,
                    } => AudioEvent::Word {
                        word,
                        offset_seconds,
                    },
                    SpeechifyEvent::Done { duration_seconds } => {
                        AudioEvent::Done { duration_seconds }
                    }
                })
            }),
            Self::Raw(stream) => stream
                .next()
                .await
                .map(|chunk| chunk.map(AudioEvent::Audio)),
            Self::ElevenLabs(stream) => loop {
                match stream.next().await {
                    Some(Ok(ElevenLabsEvent::Audio(audio))) => {
                        return Some(Ok(AudioEvent::Audio(audio)))
                    }
                    Some(Ok(ElevenLabsEvent::Alignment { .. })) => continue,
                    Some(Err(error)) => return Some(Err(error)),
                    None => return None,
                }
            },
        }
    }
}
struct Pending {
    done: CancellationToken,
    stop: CancellationToken,
    interruptible: bool,
    speech: bool,
}
struct State {
    backend: HttpTtsBackend,
    #[cfg(feature = "deepgram-ws")]
    websocket_owner: Option<WebSocketOwner>,
    pending: Vec<Pending>,
    failure: Arc<AtomicBool>,
    // Replace on Speechify interruption instead of resetting a shared atomic:
    // retiring tasks must never advance a new generation's baseline.
    timeline: Arc<AtomicU64>,
}
#[cfg(feature = "deepgram-ws")]
struct WebSocketOwner {
    identity: ObservedProcessor,
    // Capture setup's raw, generation-free scope, never a Start-frame epoch.
    // This weak fence does not keep the owning runtime alive.
    lifetime: InvocationFence,
}
/// Synthesizes downstream complete utterances, including three-channel `TtsTextInput`.
/// Normal downstream frames wait behind accepted utterances; explicit skipped
/// incremental text, system and upstream frames bypass synthesis. Requests are
/// sequential, bounded and owned by
/// the core runtime. End drains requests; interruption/Cancel discard stale audio.
/// Emits lifecycle, PCM, source anchors and completion diagnostics. Speechify
/// additionally emits provider-reported word timing; none of these are playback
/// receipts. Character alignment remains available through the ElevenLabs client API.
/// The opt-in Deepgram WebSocket backend has the name `DeepgramWebSocketTtsService`
/// and permits one owning pipeline at a time, with a fresh client owner per setup.
pub struct HttpTtsProcessor {
    service_name: &'static str,
    health: ProcessorHealth,
    max_pending: usize,
    state: Mutex<State>,
    audio_watchdog: TtsAudioWatchdog,
    #[cfg(feature = "deepgram-ws")]
    websocket_active: AtomicBool,
    #[cfg(feature = "deepgram-ws")]
    websocket_cleanup: tokio::sync::Mutex<()>,
}
impl HttpTtsProcessor {
    pub fn new(backend: HttpTtsBackend, max_pending: usize) -> Result<Self, SpeechError> {
        if max_pending == 0 {
            return Err(SpeechError::Invalid("pending utterance limit"));
        }
        backend.validate()?;
        let service_name = "HttpTtsService";
        #[cfg(feature = "deepgram-ws")]
        let service_name = if matches!(&backend, HttpTtsBackend::DeepgramWebSocket { .. }) {
            "DeepgramWebSocketTtsService"
        } else {
            service_name
        };
        Ok(Self {
            service_name,
            health: ProcessorHealth::default(),
            max_pending,
            audio_watchdog: TtsAudioWatchdog::default(),
            #[cfg(feature = "deepgram-ws")]
            websocket_active: AtomicBool::new(false),
            #[cfg(feature = "deepgram-ws")]
            websocket_cleanup: tokio::sync::Mutex::new(()),
            state: Mutex::new(State {
                backend,
                #[cfg(feature = "deepgram-ws")]
                websocket_owner: None,
                pending: vec![],
                failure: Arc::default(),
                timeline: Arc::default(),
            }),
        })
    }
    /// Zero disables only the permanent cutoff; each silent context is reported.
    pub fn with_max_consecutive_zero_audio_contexts(mut self, maximum: usize) -> Self {
        self.audio_watchdog = TtsAudioWatchdog::new(maximum);
        self
    }
    /// Apply supported model, voice and language changes to future utterances.
    /// An invalid or ineffective delta does not restore provider usability.
    pub fn update_settings(&self, delta: &TtsSettings) -> Result<bool, SpeechError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| SpeechError::Invalid("TTS state lock poisoned"))?;
        let Some(candidate) = state.backend.updated(delta)? else {
            return Ok(false);
        };
        state.backend = candidate;
        self.health.set_usable(true);
        Ok(true)
    }
    #[cfg(feature = "deepgram-ws")]
    async fn cleanup_websocket(
        &self,
        terminal_owner: Option<&ObservedProcessor>,
    ) -> Result<(), ProcessorError> {
        // Serialize retries and Stop/cleanup overlap. No state lock crosses an
        // await, and the durable backend retains the client if this is dropped.
        let _cleanup = self.websocket_cleanup.lock().await;
        let client = {
            let state = self
                .state
                .lock()
                .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
            let HttpTtsBackend::DeepgramWebSocket { client, .. } = &state.backend else {
                return Ok(());
            };
            let Some(owner) = &state.websocket_owner else {
                // No successful setup: this is still the caller's template client.
                return Ok(());
            };
            if let Some(identity) = terminal_owner {
                if identity != &owner.identity {
                    return Err(websocket_owner_error());
                }
            } else if owner.lifetime.is_current() {
                // Core also calls contextless cleanup for failed setup. A
                // rejected second runtime must not cancel the live first one.
                // Core's legitimate cleanup follows runtime finish/sealing.
                return Ok(());
            }
            client.clone()
        };
        self.cancel(false)?;
        client.cancel_all();
        client.cleanup().await.map_err(settings_error)?;
        // Hold the state lock through release so setup cannot observe half a
        // transition. Keep the weak identity until the old runtime has ended,
        // including the gap between Stop's joined cleanup and forwarding.
        let _state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
        self.websocket_active.store(false, Ordering::Release);
        Ok(())
    }
    fn cancel(&self, interruption_only: bool) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("HTTP TTS state lock poisoned"))?;
        state.pending.retain(|task| {
            if !interruption_only || task.interruptible {
                task.stop.cancel();
                false
            } else {
                true
            }
        });
        state.failure = Arc::default();
        #[cfg(feature = "speechify")]
        if matches!(state.backend, HttpTtsBackend::Speechify { .. }) {
            state.timeline = Arc::default();
        }
        Ok(())
    }
    fn enqueue(&self, command: Command, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("HTTP TTS state lock poisoned"))?;
        state.pending.retain(|p| !p.done.is_cancelled());
        if state.pending.is_empty() {
            if let Command::Generic(frame) = command {
                // Serialize idle routing with admission/cancellation. try_push
                // is nonblocking; its clock/wakers must obey core's contract
                // against reentering or waiting for locks held by the caller.
                let result = ctx.try_push(*frame, Direction::Downstream);
                if result.is_err() {
                    state.failure.store(true, Ordering::Release);
                }
                return result;
            }
        }
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
                return Err(ProcessorError::new("HTTP TTS provenance limit exceeded")
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
                ProcessorError::new("HTTP TTS pending utterance limit reached")
                    .with_category(ErrorCategory::Application),
            );
        }
        // Waiting only for the last task is insufficient: interruption can drop
        // that task while an earlier uninterruptible fence is still pending.
        // At most two bounded pending pools contribute these dependencies.
        let previous: Vec<_> = state.pending.iter().map(|p| p.done.clone()).collect();
        let done = CancellationToken::new();
        let stop = CancellationToken::new();
        let guard = DoneOnDrop {
            done: done.clone(),
            failure: state.failure.clone(),
            completed: false,
        };
        let cancel = stop.clone();
        let backend = state.backend.clone();
        let generation = ctx.health().generation();
        let audio_watchdog = self.audio_watchdog.clone();
        let timeline = state.timeline.clone();
        let ctx_output = ctx.clone();
        let failure = state.failure.clone();
        let interruptible = !matches!(
            &command,
            Command::Fence(frame) | Command::Generic(frame) if !frame.interruptible
        );
        ctx.spawn("http-tts-utterance", interruptible, async move {
            let mut guard = guard;
            let operation = async {
                for previous in previous {
                    previous.cancelled().await;
                }
                match command {
                    Command::Speak(input) => {
                        synthesize(
                            backend,
                            *input,
                            ctx_output,
                            failure.clone(),
                            generation,
                            timeline,
                            audio_watchdog,
                        )
                        .await
                    }
                    Command::Fence(mut frame) | Command::Generic(mut frame) => {
                        if matches!(frame.data, FrameData::LlmResponseEnd) {
                            if failure.load(Ordering::Acquire) {
                                frame.metadata.insert("failed".into(), true.into());
                            }
                            frame.set_pipeline_pts(timeline.load(Ordering::Acquire));
                        }
                        ctx_output.push(*frame, Direction::Downstream).await
                    }
                }
            };
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    failure.store(true, Ordering::Release);
                    Ok(())
                },
                result = operation => result,
            };
            guard.completed = result.is_ok();
            result
        })?;
        // A stale invocation can be accepted as a no-op by the core. Do not
        // publish a dependency for a future already dropped at admission.
        if !done.is_cancelled() {
            state.pending.push(Pending {
                done,
                stop,
                interruptible,
                speech,
            });
        }
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
    Generic(Box<Frame>),
}
struct DoneOnDrop {
    done: CancellationToken,
    failure: Arc<AtomicBool>,
    completed: bool,
}
impl Drop for DoneOnDrop {
    fn drop(&mut self) {
        // Publish failure before releasing later completion fences. This also
        // covers rejected task admission, panics and cancellation by the core.
        // The captured response generation cannot poison a later response.
        if !self.completed {
            self.failure.store(true, Ordering::Release);
        }
        self.done.cancel();
    }
}
#[async_trait]
impl FrameProcessor for HttpTtsProcessor {
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    fn name(&self) -> &str {
        self.service_name
    }
    async fn setup(
        &self,
        config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        #[cfg(feature = "deepgram-ws")]
        let _cleanup = if self.service_name == "DeepgramWebSocketTtsService" {
            Some(
                self.websocket_cleanup
                    .try_lock()
                    .map_err(|_| websocket_owner_error())?,
            )
        } else {
            None
        };
        #[allow(unused_mut)]
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
        #[cfg(feature = "deepgram-ws")]
        if matches!(state.backend, HttpTtsBackend::DeepgramWebSocket { .. })
            && (self.websocket_active.load(Ordering::Acquire)
                || state
                    .websocket_owner
                    .as_ref()
                    .is_some_and(|owner| owner.lifetime.is_current()))
        {
            return Err(websocket_owner_error());
        }
        let compatible = config.audio_out_sample_rate == state.backend.sample_rate();
        #[cfg(feature = "speechify")]
        let compatible = compatible
            || matches!(
                &state.backend,
                HttpTtsBackend::Speechify { request, .. }
                    if config.audio_out_sample_rate == request.sample_rate
            );
        if !compatible {
            let error = ProcessorError::fatal(
                "pipeline output sample rate differs from HTTP TTS configuration",
            )
            .with_category(ErrorCategory::InvalidRequest);
            #[cfg(feature = "deepgram-ws")]
            let error = if matches!(state.backend, HttpTtsBackend::DeepgramWebSocket { .. }) {
                error.with_permanence_override(false)
            } else {
                error
            };
            return Err(error);
        }
        #[cfg(feature = "deepgram-ws")]
        let retired_client =
            if let HttpTtsBackend::DeepgramWebSocket { client, .. } = &mut state.backend {
                let owner = WebSocketOwner {
                    identity: _ctx.processor_identity(),
                    lifetime: _ctx.invocation_fence()?,
                };
                self.websocket_active
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .map_err(|_| websocket_owner_error())?;
                // Fork only after validation and admission. Task/backend clones
                // share this private owner; other processors and the caller do not.
                let fresh = client.fork_owner();
                let retired = std::mem::replace(client, fresh);
                state.websocket_owner = Some(owner);
                state.timeline = Arc::default();
                Some(retired)
            } else {
                None
            };
        #[cfg(feature = "speechify")]
        if matches!(state.backend, HttpTtsBackend::Speechify { .. }) {
            state.timeline = Arc::default();
        }
        drop(state);
        #[cfg(feature = "deepgram-ws")]
        drop(retired_client);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        #[cfg(feature = "deepgram-ws")]
        if self.service_name == "DeepgramWebSocketTtsService" {
            let state = self
                .state
                .lock()
                .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
            if !state
                .websocket_owner
                .as_ref()
                .is_some_and(|owner| owner.identity == ctx.processor_identity())
            {
                return Err(websocket_owner_error());
            }
        }
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
        #[cfg(feature = "deepgram-ws")]
        if self.service_name == "DeepgramWebSocketTtsService"
            && matches!(frame.data, FrameData::Stop)
        {
            // Core drains this node's accepted work before invoking terminal
            // processing, but deliberately skips cleanup for a stopped runtime.
            // Join retired actors before Stop can cross the output boundary.
            self.cleanup_websocket(Some(&ctx.processor_identity()))
                .await?;
        }
        match &frame.data {
            FrameData::TtsSpeak(speak) if !speak.text.is_empty() => {
                self.enqueue(
                    Command::Speak(Box::new(SpeechInput {
                        text: TtsTextChannels::identical(speak.text.clone()),
                        append_to_context: speak.append_to_context,
                        source_id: frame.id,
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
                        source_id: frame.id,
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
                        source_id: frame.id,
                        destination: frame.transport_destination.clone(),
                        aggregated_by: text.aggregated_by.clone(),
                        includes_inter_frame_spaces: text.includes_inter_frame_spaces,
                        skipped: text.skip_tts,
                    })),
                    ctx,
                )?;
                return Ok(());
            }
            FrameData::Text(text) | FrameData::LlmText(text) if text.skip_tts == Some(true) => {
                return ctx.push(frame, direction).await;
            }
            FrameData::LlmResponseStart => {
                self.state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("HTTP TTS state lock poisoned"))?
                    .failure = Arc::new(AtomicBool::new(!ctx.is_usable()));
            }
            FrameData::LlmResponseEnd => {
                self.enqueue(Command::Fence(Box::new(frame)), ctx)?;
                return Ok(());
            }
            FrameData::FlushDetailed(probe) if !probe.returning => {
                self.enqueue(Command::Fence(Box::new(frame)), ctx)?;
                return Ok(());
            }
            _ => {}
        }
        if !frame.is_system() && !frame.is_terminal() {
            self.enqueue(Command::Generic(Box::new(frame)), ctx)?;
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        #[cfg(feature = "deepgram-ws")]
        if self.service_name == "DeepgramWebSocketTtsService" {
            return self.cleanup_websocket(None).await;
        }
        self.cancel(false)
    }
}
#[cfg(feature = "deepgram-ws")]
fn websocket_owner_error() -> ProcessorError {
    ProcessorError::fatal("Deepgram WebSocket TTS already belongs to another active pipeline")
        .with_category(ErrorCategory::Application)
        .with_permanence_override(false)
}
/// Each complete utterance owns an immutable provider/alignment context. Even
/// adjacent utterances cannot inherit another destination, spacing or append policy.
#[allow(clippy::too_many_arguments)]
async fn synthesize(
    backend: HttpTtsBackend,
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
        .map_err(|_| ProcessorError::fatal("HTTP TTS context identity exhausted"))?;
    let context_id = format!("http-tts-{serial}");
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
        backend.reporting(),
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
    #[allow(unused_mut)]
    let mut audio_offset_seconds = None;
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
            Ok(AudioEvent::Audio(chunk)) => {
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
            #[cfg(feature = "speechify")]
            Ok(AudioEvent::Word {
                word,
                offset_seconds,
            }) => {
                let events = alignment.word(&word, offset_seconds, false);
                if !emit_alignment(events, &mut mapper, &input, &ctx, &failure, revision).await? {
                    succeeded = false;
                    break;
                }
            }
            #[cfg(feature = "speechify")]
            Ok(AudioEvent::Done { duration_seconds }) => {
                // The provider client emits this once, after EOF and all late
                // marks. It is request-relative, never a cumulative timestamp.
                audio_offset_seconds = Some(duration_seconds);
            }
            Err(SpeechError::EmptyAudio)
                if backend.reporting() == TokenReporting::SynthesisCompletion =>
            {
                break
            }
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
        let events = alignment.finish_success_with_audio_offset(
            ctx.observer_clock().now_ns(),
            timeline.load(Ordering::Acquire),
            audio_offset_seconds,
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
            .map_err(|_| ProcessorError::new("HTTP TTS timestamp overflow"))?;
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
fn required(target: &mut String, value: &Setting<String>) -> Result<bool, SpeechError> {
    match value {
        Setting::NotGiven => Ok(false),
        Setting::Value(value) if !value.is_empty() => {
            let changed = target != value;
            *target = value.clone();
            Ok(changed)
        }
        _ => Err(SpeechError::Invalid("required nonempty TTS setting")),
    }
}
fn optional(target: &mut Option<String>, value: &Setting<String>) -> Result<bool, SpeechError> {
    let candidate = match value {
        Setting::NotGiven => return Ok(false),
        Setting::Null => None,
        Setting::Value(value) if !value.is_empty() => Some(value.clone()),
        _ => return Err(SpeechError::Invalid("optional nonempty TTS setting")),
    };
    let changed = *target != candidate;
    *target = candidate;
    Ok(changed)
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
    let mut result = TtsSettings::default();
    for (key, value) in values {
        #[cfg(feature = "speechmatics")]
        if key == "max_retries" {
            result.base.extra.insert(key.clone(), value.clone());
            continue;
        }
        let value = match value {
            serde_json::Value::Null => Setting::Null,
            serde_json::Value::String(value) => Setting::Value(value.clone()),
            _ => return Err(SpeechError::Invalid("HTTP TTS string setting expected")),
        };
        match key.as_str() {
            "model" => result.base.model = value,
            "voice" => result.voice = value,
            "language" => result.language = value,
            _ => return Err(SpeechError::Invalid("unsupported HTTP TTS setting")),
        }
    }
    Ok(result)
}
fn settings_error(error: SpeechError) -> ProcessorError {
    let error = error_frame(error);
    ProcessorError::new(error.error)
        .with_category(error.category)
        .with_permanence_override(false)
}

#[cfg(all(test, feature = "deepgram-ws"))]
mod deepgram_websocket_settings_tests {
    use super::*;
    use crate::{deepgram_ws::DeepgramWsConfig, SpeechConfig};

    fn backend(request: DeepgramSynthesisRequest) -> HttpTtsBackend {
        HttpTtsBackend::DeepgramWebSocket {
            client: DeepgramWsClient::new(DeepgramWsConfig::new("synthetic-test-key")).unwrap(),
            request,
        }
    }

    #[test]
    fn websocket_profile_has_distinct_name_and_completion_evidence() {
        let websocket = backend(DeepgramSynthesisRequest::new(""));
        assert_eq!(websocket.reporting(), TokenReporting::SynthesisCompletion);
        let websocket = HttpTtsProcessor::new(websocket, 2).unwrap();
        assert_eq!(websocket.name(), "DeepgramWebSocketTtsService");
        let http = HttpTtsProcessor::new(
            HttpTtsBackend::Deepgram {
                client: DeepgramHttpClient::new(SpeechConfig::deepgram("synthetic-test-key"))
                    .unwrap(),
                request: DeepgramSynthesisRequest::new(""),
            },
            2,
        )
        .unwrap();
        assert_eq!(http.name(), "HttpTtsService");
    }

    #[test]
    fn websocket_templates_require_linear16_at_supported_rates() {
        for rate in [8000, 16000, 24000, 32000, 48000] {
            let mut request = DeepgramSynthesisRequest::new("");
            request.sample_rate = rate;
            let candidate = backend(request);
            candidate.validate().unwrap();
            assert_eq!(candidate.sample_rate(), rate);
        }
        for encoding in [DeepgramEncoding::Mulaw, DeepgramEncoding::Alaw] {
            let mut request = DeepgramSynthesisRequest::new("");
            request.encoding = encoding;
            request.sample_rate = 8000;
            assert!(HttpTtsProcessor::new(backend(request), 1).is_err());
        }
        let mut request = DeepgramSynthesisRequest::new("");
        request.sample_rate = 44100;
        assert!(HttpTtsProcessor::new(backend(request), 1).is_err());
    }

    #[test]
    fn websocket_voice_and_model_alias_preserve_immutable_options() {
        let mut request = DeepgramSynthesisRequest::new("");
        request.speed = Some(1.1);
        request.mip_opt_out = Some(false);
        let original = backend(request);
        assert!(original.updated(&TtsSettings::default()).unwrap().is_none());
        let mut delta = TtsSettings {
            voice: Setting::Value("new-voice".into()),
            ..Default::default()
        };
        let candidate = original.updated(&delta).unwrap().unwrap();
        let HttpTtsBackend::DeepgramWebSocket { request, .. } = &candidate else {
            panic!("WebSocket profile changed");
        };
        assert_eq!(request.voice, "new-voice");
        assert_eq!(request.speed, Some(1.1));
        assert_eq!(request.mip_opt_out, Some(false));
        delta.base.model = Setting::Value("new-voice".into());
        assert!(candidate.updated(&delta).unwrap().is_none());
        delta.base.model = Setting::Value("conflicting-voice".into());
        assert!(candidate.updated(&delta).is_err());
        delta.voice = Setting::NotGiven;
        assert!(candidate.updated(&delta).unwrap().is_some());
        for language in [Setting::Null, Setting::Value("en".into())] {
            assert!(candidate
                .updated(&TtsSettings {
                    language,
                    ..Default::default()
                })
                .is_err());
        }
        for (key, value) in [
            ("speed", serde_json::json!(1.2)),
            ("mip_opt_out", false.into()),
        ] {
            let mut delta = TtsSettings::default();
            delta.base.extra.insert(key.into(), value);
            assert!(candidate.updated(&delta).is_err());
        }
    }
}

#[cfg(test)]
mod ordering_survivor_tests {
    use super::*;
    use pipecat_core::{DirectedFrame, Pipeline, PipelineConfig, PipelineWorker};
    use std::time::Duration;
    use tokio::sync::{mpsc, oneshot};

    const DEADLINE: Duration = Duration::from_secs(3);

    // The public HTTP backend can hold speech, but its passthrough ctx.push is
    // intentionally nonblocking. Install one real owned noninterruptible task
    // and its matching pending record to hold predecessor A before admission.
    // No private runtime state or production hooks are changed by this fixture.
    struct HeldPredecessor {
        tts: Arc<HttpTtsProcessor>,
        release: Mutex<Option<oneshot::Receiver<()>>>,
        accepted: mpsc::Sender<u64>,
    }
    struct TestDone(CancellationToken);
    impl Drop for TestDone {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }
    #[async_trait]
    impl FrameProcessor for HeldPredecessor {
        fn name(&self) -> &str {
            self.tts.name()
        }
        fn health(&self) -> Option<ProcessorHealth> {
            self.tts.health()
        }
        async fn setup(
            &self,
            config: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.tts.setup(config, ctx).await
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            let id = frame.id;
            if matches!(frame.data, FrameData::Data) {
                assert!(!frame.interruptible);
                let release = self.release.lock().unwrap().take().unwrap();
                let done = CancellationToken::new();
                let stop = CancellationToken::new();
                let guard = TestDone(done.clone());
                let output = ctx.clone();
                ctx.spawn("held-noninterruptible-predecessor", false, async move {
                    let _guard = guard;
                    release.await.unwrap();
                    output.push(frame, direction).await
                })?;
                self.tts.state.lock().unwrap().pending.push(Pending {
                    done,
                    stop,
                    interruptible: false,
                    speech: false,
                });
            } else {
                self.tts.process(frame, direction, ctx).await?;
            }
            self.accepted.try_send(id).unwrap();
            Ok(())
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.tts.cleanup().await
        }
    }
    async fn admit(worker: &PipelineWorker, accepted: &mut mpsc::Receiver<u64>, frame: Frame) {
        let id = frame.id;
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(DEADLINE, async {
            while accepted.recv().await.unwrap() != id {}
        })
        .await
        .unwrap();
    }
    async fn fixture() -> (
        PipelineWorker,
        mpsc::Receiver<u64>,
        oneshot::Sender<()>,
        tokio::net::TcpListener,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = crate::SpeechConfig::cartesia("synthetic-key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let tts = Arc::new(
            HttpTtsProcessor::new(
                HttpTtsBackend::Cartesia {
                    client: CartesiaHttpClient::new(config).unwrap(),
                    request: CartesiaSynthesisRequest::new("", "voice"),
                },
                8,
            )
            .unwrap(),
        );
        let (release, receiver) = oneshot::channel();
        let (accepted, admitted) = mpsc::channel(16);
        let processor = HeldPredecessor {
            tts,
            release: Mutex::new(Some(receiver)),
            accepted,
        };
        let worker = Pipeline::new(vec![Arc::new(processor)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        (worker, admitted, release, listener)
    }
    fn final_marker() -> Frame {
        let mut text = TextFrame::new("surviving last fence");
        text.skip_tts = Some(true);
        Frame::new(FrameData::AggregatedText(AggregatedTextFrame {
            text,
            aggregated_by: "sentence".into(),
            raw_text: None,
            context_id: None,
            will_be_spoken: false,
        }))
        .with_interruptible(false)
    }
    #[tokio::test]
    async fn cancelled_middle_task_does_not_release_a_later_fence_before_held_survivor() {
        let (mut worker, mut accepted, release, _listener) = fixture().await;
        let first = Frame::new(FrameData::Data).with_interruptible(false);
        let last = final_marker();
        admit(&worker, &mut accepted, first.clone()).await;
        admit(
            &worker,
            &mut accepted,
            Frame::new(FrameData::TtsSpeak(pipecat_core::TtsSpeakFrame::new(
                "cancelled middle",
            ))),
        )
        .await;
        admit(&worker, &mut accepted, last.clone()).await;
        admit(&worker, &mut accepted, Frame::interruption()).await;
        let mut prefix: Vec<DirectedFrame> = vec![];
        let overtook = tokio::time::timeout(Duration::from_millis(50), async {
            loop {
                let frame = worker.recv().await.unwrap();
                let done = frame.frame.id == last.id;
                prefix.push(frame);
                if done {
                    return;
                }
            }
        })
        .await
        .is_ok();
        // Release and join even on the baseline regression.
        release.send(()).unwrap();
        worker.end().await.unwrap();
        let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
            .await
            .unwrap()
            .unwrap();
        prefix.extend(frames);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(
            !overtook,
            "cancelled B released C while uninterruptible A was held"
        );
        let first_index = prefix.iter().position(|f| f.frame.id == first.id).unwrap();
        let last_index = prefix.iter().position(|f| f.frame.id == last.id).unwrap();
        assert!(first_index < last_index);
        assert!(!prefix.iter().any(|f| matches!(
            f.frame.data,
            FrameData::TtsStarted(_) | FrameData::TtsAudio(_)
        )));
    }
    #[tokio::test]
    async fn cancel_reaps_all_held_surviving_dependencies_without_release() {
        let (worker, mut accepted, release, _listener) = fixture().await;
        let first = Frame::new(FrameData::Data).with_interruptible(false);
        let last = final_marker();
        admit(&worker, &mut accepted, first.clone()).await;
        admit(
            &worker,
            &mut accepted,
            Frame::new(FrameData::TtsSpeak(pipecat_core::TtsSpeakFrame::new(
                "cancelled middle",
            ))),
        )
        .await;
        admit(&worker, &mut accepted, last.clone()).await;
        worker.cancel("cancel all dependency owners").await.unwrap();
        let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.state, pipecat_core::WorkerState::Cancelled);
        assert!(
            release.send(()).is_err(),
            "owned predecessor was not reaped"
        );
        assert!(!frames
            .iter()
            .any(|f| [first.id, last.id].contains(&f.frame.id)));
        assert!(!frames.iter().any(|f| matches!(
            f.frame.data,
            FrameData::TtsStarted(_) | FrameData::TtsAudio(_) | FrameData::End(_)
        )));
    }
}

#[cfg(test)]
mod idle_forwarding_tests {
    use super::*;
    use pipecat_core::{Pipeline, PipelineConfig, PipelineError};
    use std::time::Duration;
    use tokio::sync::mpsc;

    const DEADLINE: Duration = Duration::from_secs(3);

    struct ObserveIdleFailure {
        tts: HttpTtsProcessor,
        observed: mpsc::Sender<(Result<(), ProcessorError>, bool)>,
    }
    #[async_trait]
    impl FrameProcessor for ObserveIdleFailure {
        fn name(&self) -> &str {
            self.tts.name()
        }
        fn health(&self) -> Option<ProcessorHealth> {
            self.tts.health()
        }
        async fn setup(
            &self,
            config: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.tts.setup(config, ctx).await
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::Data) {
                let failure = self.tts.state.lock().unwrap().failure.clone();
                let result = self.tts.process(frame, direction, ctx).await;
                // Observe before cleanup can replace the current response and
                // before a current-thread background task can poll.
                self.observed
                    .try_send((result.clone(), failure.load(Ordering::Acquire)))
                    .unwrap();
                result
            } else {
                self.tts.process(frame, direction, ctx).await
            }
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.tts.cleanup().await
        }
    }

    #[tokio::test]
    async fn idle_output_failure_marks_captured_response_before_returning() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = crate::SpeechConfig::cartesia("synthetic-key");
        config.base_url = format!("http://{}", listener.local_addr().unwrap());
        let backend = HttpTtsBackend::Cartesia {
            client: CartesiaHttpClient::new(config).unwrap(),
            request: CartesiaSynthesisRequest::new("", "voice"),
        };
        let (observed, mut receiver) = mpsc::channel(1);
        let processor = ObserveIdleFailure {
            tts: HttpTtsProcessor::new(backend.clone(), 4).unwrap(),
            observed,
        };
        let worker = Pipeline::new(vec![Arc::new(processor)])
            .start(PipelineConfig {
                output_capacity: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        // Start owns the sole output slot until after the real idle push fails.
        let mut state = worker.sender().watch();
        worker
            .sender()
            .send(Frame::new(FrameData::Data), Direction::Downstream)
            .await
            .unwrap();
        let (admission, captured_failure) = tokio::time::timeout(DEADLINE, receiver.recv())
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(DEADLINE, async {
            while !state.borrow_and_update().is_finished() {
                state.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let result = tokio::time::timeout(DEADLINE, worker.collect())
            .await
            .unwrap();
        assert!(
            matches!(result, Err(PipelineError::Backpressure(ref message))
            if message == "worker output queue is full; consume output concurrently")
        );

        // Output overflow is terminal. This is an independent new worker, not
        // a claim that the failed worker can recover or accept another End.
        let fresh = Pipeline::new(vec![Arc::new(HttpTtsProcessor::new(backend, 4).unwrap())])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let end = Frame::new(FrameData::LlmResponseEnd);
        fresh
            .sender()
            .send(end.clone(), Direction::Downstream)
            .await
            .unwrap();
        fresh.end().await.unwrap();
        let (frames, report) = tokio::time::timeout(DEADLINE, fresh.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let completion = frames
            .iter()
            .find(|frame| frame.frame.id == end.id)
            .unwrap();
        assert!(!completion.frame.metadata.contains_key("failed"));

        let error = admission.expect_err("idle output admission must fail synchronously");
        assert!(error.fatal);
        assert_eq!(
            error.message,
            "backpressure: worker output queue is full; consume output concurrently"
        );
        assert!(
            captured_failure,
            "idle output error did not mark its captured response"
        );
    }
}
