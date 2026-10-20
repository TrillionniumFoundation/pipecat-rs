//! Owned native sessions with bounded transport, handshake, runtime, and cancellation.
use crate::bot_audio::{BotAudioLimits, BotAudioRun, Capture};
#[cfg(feature = "bot-audio-transcription")]
use crate::bot_transcription::{BotTranscription, BotTranscriptionConfig};
use crate::recorded_audio::{Playback, RecordedAudio, TimelineRecorder, MAX_TOTAL_BYTES};
use crate::timeline_recording::{
    Capture as TimelineCapture, TimelineRecordingLimits, TimelineRecordingRun,
};
#[cfg(feature = "user-speech")]
use crate::user_speech::{
    CartesiaUserSpeechSettings, CartesiaUserSpeechVoice, PreparedHostedSpeech, UserSpeechBudget,
    UserSpeechOwner,
};
use crate::{
    events::{EventLimits, EventStream},
    judge::JudgeRequest,
    matcher::{
        AssertionFailure, ExpectationMatcher, ExpectationResult, MatchOutcome, PendingCheck,
        ScriptResult,
    },
    serializer, EvalError, EvalJudge, Expectation, JudgeDecision, JudgeVerdict, ScriptScenario,
    ScriptTurn,
};
use pipecat_classifiers::{CallOptions, ClassifierError};
use pipecat_protocol::rtvi::{Envelope, PROTOCOL_VERSION};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

#[derive(Debug, Clone)]
pub struct Cancellation {
    tx: watch::Sender<bool>,
}
impl Default for Cancellation {
    fn default() -> Self {
        Self::new()
    }
}
impl Cancellation {
    pub fn new() -> Self {
        let (tx, _) = watch::channel(false);
        Self { tx }
    }
    pub fn cancel(&self) {
        self.tx.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.tx.borrow()
    }
    pub async fn cancelled(&self) {
        let mut rx = self.tx.subscribe();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}
#[derive(Debug, Clone)]
pub struct SessionOptions {
    pub transport: SessionConfig,
    pub handshake_timeout: Duration,
    pub default_timeout: Duration,
    pub send_after_timeout: Duration,
    pub bot_quiet_timeout: Duration,
    pub max_duration: Duration,
    pub event_limits: EventLimits,
    pub max_wire_messages: usize,
    pub max_wire_bytes: usize,
    /// Send eval-cancel on every exit, including assertion failure or cancellation.
    pub stop_bot: bool,
}
impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            transport: SessionConfig::default(),
            handshake_timeout: Duration::from_secs(10),
            default_timeout: Duration::from_secs(60),
            send_after_timeout: Duration::from_secs(30),
            bot_quiet_timeout: Duration::from_secs(30),
            max_duration: Duration::from_secs(600),
            event_limits: EventLimits::default(),
            max_wire_messages: 65_536,
            max_wire_bytes: 64 * 1024 * 1024,
            stop_bot: false,
        }
    }
}
impl SessionOptions {
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    pub(crate) fn validate_audio_to_audio(&self) -> Result<(), EvalError> {
        self.validate()?;
        let defaults = Self::default();
        if self.transport.queue_capacity > defaults.transport.queue_capacity
            || self.transport.max_message_bytes > defaults.transport.max_message_bytes
            || self.max_wire_messages > defaults.max_wire_messages
            || self.max_wire_bytes > defaults.max_wire_bytes
            || self.event_limits.max_events > defaults.event_limits.max_events
            || self.event_limits.max_event_bytes > defaults.event_limits.max_event_bytes
            || self.event_limits.max_text_bytes > defaults.event_limits.max_text_bytes
            || self.handshake_timeout > defaults.handshake_timeout
            || self.default_timeout > defaults.default_timeout
            || self.send_after_timeout > defaults.send_after_timeout
            || self.bot_quiet_timeout > defaults.bot_quiet_timeout
            || self.max_duration > defaults.max_duration
            || self.transport.connect_timeout > defaults.transport.connect_timeout
            || self.transport.write_timeout > defaults.transport.write_timeout
            || self.transport.close_timeout > defaults.transport.close_timeout
        {
            return Err(EvalError::Scenario(
                "audio-to-audio options exceed the finite profile".into(),
            ));
        }
        Ok(())
    }
    fn validate(&self) -> Result<(), EvalError> {
        let durations = [
            self.handshake_timeout,
            self.default_timeout,
            self.send_after_timeout,
            self.bot_quiet_timeout,
            self.max_duration,
            self.transport.connect_timeout,
            self.transport.write_timeout,
            self.transport.close_timeout,
        ];
        if durations
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(86_400))
            || self.transport.queue_capacity == 0
            || self.transport.max_message_bytes == 0
            || self.max_wire_messages == 0
            || self.max_wire_bytes == 0
            || self.event_limits.max_events == 0
            || self.event_limits.max_event_bytes == 0
            || self.event_limits.max_text_bytes == 0
        {
            return Err(EvalError::Scenario(
                "limits must be positive; timeouts must be <=24h".into(),
            ));
        }
        Ok(())
    }
}
#[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
struct AudioToAudioState {
    run: Option<(Cancellation, Instant)>,
    admission_open: bool,
    stop_bot_attempted: bool,
}

/// Private selection state, separate from direct speech and duplex sessions.
#[cfg(feature = "user-speech")]
struct HostedSpeechState {
    admission_closed: bool,
    stop_bot_attempted: bool,
}

/// Passive recording metadata only; Playback and the existing socket retain
/// their original owner/task responsibilities.
struct TimelineRecordingState {
    recorder: TimelineRecorder,
    closed: bool,
    stop_bot_attempted: bool,
}

enum RecordedCompletion {
    Ordinary(tokio::sync::oneshot::Receiver<()>),
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    Timed(tokio::sync::oneshot::Receiver<Instant>),
}
impl RecordedCompletion {
    async fn receive(&mut self) -> Result<Option<Instant>, tokio::sync::oneshot::error::RecvError> {
        match self {
            Self::Ordinary(done) => {
                done.await?;
                Ok(None)
            }
            #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
            Self::Timed(done) => done.await.map(Some),
        }
    }
}

pub struct EvalSession {
    scenario: ScriptScenario,
    url: String,
    options: SessionOptions,
    socket: Option<WebSocketSession>,
    stream: EventStream,
    matcher: ExpectationMatcher,
    result: ScriptResult,
    next_id: u64,
    active_turn: i64,
    active_turn_started: Option<Instant>,
    phase: &'static str,
    wire_messages: usize,
    bot_ready_received: bool,
    wire_bytes: usize,
    judge: Option<EvalJudge>,
    active_expectation: i64,
    recordings: BTreeMap<usize, RecordedAudio>,
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    audio_to_audio: Option<AudioToAudioState>,
    #[cfg(all(test, feature = "user-speech", feature = "bot-audio-transcription"))]
    fence_receiver: Option<tokio::sync::mpsc::Receiver<Packet>>,
    #[cfg(all(test, feature = "user-speech", feature = "bot-audio-transcription"))]
    fence_rounds: usize,
    #[cfg(feature = "user-images")]
    user_images: Option<BTreeMap<usize, crate::EncodedUserImage>>,
    #[cfg(feature = "user-images")]
    images_frozen: bool,
    #[cfg(feature = "user-images")]
    image_wire_bytes: usize,
    #[cfg(feature = "user-images")]
    image_wire_messages: usize,
    playback: Option<Playback>,
    #[cfg(feature = "user-speech")]
    user_speech: Option<UserSpeechOwner>,
    #[cfg(feature = "user-speech")]
    user_speech_budget: Option<UserSpeechBudget>,
    #[cfg(feature = "user-speech")]
    hosted_speech: Option<HostedSpeechState>,
    capture: Option<Capture>,
    timeline: Option<TimelineRecordingState>,
    #[cfg(feature = "bot-audio-transcription")]
    transcription_config: Option<BotTranscriptionConfig>,
    #[cfg(feature = "bot-audio-transcription")]
    transcription: Option<BotTranscription>,
    #[cfg(feature = "bot-audio-transcription")]
    pending_recognition: Option<(u64, String)>,
    #[cfg(all(test, feature = "bot-audio-transcription"))]
    recognition_drain_gate: Option<RecognitionDrainGate>,
}
#[cfg(all(test, feature = "bot-audio-transcription"))]
struct RecognitionDrainGate {
    entered: Option<tokio::sync::oneshot::Sender<()>>,
    release: std::sync::Arc<tokio::sync::Notify>,
}
impl EvalSession {
    pub fn new(
        scenario: ScriptScenario,
        url: impl Into<String>,
        options: SessionOptions,
    ) -> Result<Self, EvalError> {
        scenario.validate()?;
        let judge = if scenario.needs_judge() {
            Some(scenario.judge.as_ref().and_then(|j| j.eval.as_ref())
                .ok_or_else(|| EvalError::Scenario("eval assertions require judge.eval configuration or EvalSession::with_judge".into()))?.build()?)
        } else {
            None
        };
        Self::construct(scenario, url.into(), options, judge)
    }
    /// Use a native classifier-backed judge owned exclusively by this run.
    /// The supplied judge takes precedence over any scenario provider configuration.
    pub fn with_judge(
        scenario: ScriptScenario,
        url: impl Into<String>,
        options: SessionOptions,
        judge: EvalJudge,
    ) -> Result<Self, EvalError> {
        Self::construct(scenario, url.into(), options, Some(judge))
    }
    fn construct(
        scenario: ScriptScenario,
        url: String,
        options: SessionOptions,
        judge: Option<EvalJudge>,
    ) -> Result<Self, EvalError> {
        scenario.validate()?;
        options.validate()?;
        let url = connect_url(&url, &scenario)?;
        let result = ScriptResult::new(&scenario.name, scenario.turns.len());
        Ok(Self {
            scenario,
            url,
            stream: EventStream::new(options.event_limits.clone()),
            matcher: ExpectationMatcher::new(
                options.event_limits.max_text_bytes,
                options.event_limits.max_events,
            ),
            options,
            socket: None,
            result,
            next_id: 0,
            active_turn: -1,
            active_turn_started: None,
            phase: "connect",
            wire_messages: 0,
            bot_ready_received: false,
            wire_bytes: 0,
            judge,
            active_expectation: -1,
            recordings: BTreeMap::new(),
            #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
            audio_to_audio: None,
            #[cfg(all(test, feature = "user-speech", feature = "bot-audio-transcription"))]
            fence_receiver: None,
            #[cfg(all(test, feature = "user-speech", feature = "bot-audio-transcription"))]
            fence_rounds: 0,
            #[cfg(feature = "user-images")]
            user_images: None,
            #[cfg(feature = "user-images")]
            images_frozen: false,
            #[cfg(feature = "user-images")]
            image_wire_bytes: 0,
            #[cfg(feature = "user-images")]
            image_wire_messages: 0,
            playback: None,
            #[cfg(feature = "user-speech")]
            user_speech: None,
            #[cfg(feature = "user-speech")]
            user_speech_budget: None,
            #[cfg(feature = "user-speech")]
            hosted_speech: None,
            capture: None,
            timeline: None,
            #[cfg(feature = "bot-audio-transcription")]
            transcription_config: None,
            #[cfg(feature = "bot-audio-transcription")]
            transcription: None,
            #[cfg(feature = "bot-audio-transcription")]
            pending_recognition: None,
            #[cfg(all(test, feature = "bot-audio-transcription"))]
            recognition_drain_gate: None,
        })
    }
    /// Evaluate `response` predicates from real received 16 kHz mono PCM.
    /// This opt-in library path uses local Silero segmentation, HTTP STT and
    /// native speech-timeout turn aggregation; remote speaking reports never
    /// close an audio segment. Supply a typed scenario with `response` events.
    /// The normal file loader retains its text-mode `response` alias.
    /// Classifier judgments use only completed spoken responses, with the
    /// scenario's explicit judge configuration. All resources are session-owned.
    #[cfg(feature = "bot-audio-transcription")]
    pub fn with_bot_transcription(
        scenario: ScriptScenario,
        url: impl Into<String>,
        options: SessionOptions,
        config: BotTranscriptionConfig,
    ) -> Result<Self, EvalError> {
        let validated = scenario.audio_response_validation_copy()?;
        let judge = if scenario.needs_judge() {
            Some(validated.judge.as_ref().and_then(|settings| settings.eval.as_ref())
                .ok_or_else(|| EvalError::Scenario("audio eval assertions require judge.eval configuration or EvalSession::with_bot_transcription_and_judge".into()))?.build()?)
        } else {
            None
        };
        Self::construct_bot_transcription(scenario, url.into(), options, config, judge)
    }
    /// Evaluate spoken responses using an explicitly supplied, session-owned judge.
    /// Its transcript contains STT responses, never parallel LLM text observations.
    #[cfg(feature = "bot-audio-transcription")]
    pub fn with_bot_transcription_and_judge(
        scenario: ScriptScenario,
        url: impl Into<String>,
        options: SessionOptions,
        config: BotTranscriptionConfig,
        judge: EvalJudge,
    ) -> Result<Self, EvalError> {
        Self::construct_bot_transcription(scenario, url.into(), options, config, Some(judge))
    }
    #[cfg(feature = "bot-audio-transcription")]
    fn construct_bot_transcription(
        scenario: ScriptScenario,
        url: String,
        options: SessionOptions,
        config: BotTranscriptionConfig,
        judge: Option<EvalJudge>,
    ) -> Result<Self, EvalError> {
        config.validate()?;
        let validated = scenario.audio_response_validation_copy()?;
        let mut session = Self::construct(validated, url, options, judge)?;
        session.scenario = scenario;
        session.enable_bot_audio_url()?;
        session.capture = Some(Capture::new(BotAudioLimits::default()));
        session.transcription_config = Some(config);
        Ok(session)
    }
    /// Validate the frozen joint plan before any model is entered. This builds
    /// only ordinary metadata and an optional configured judge client.
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    pub(crate) fn validate_audio_to_audio_plan(
        scenario: &ScriptScenario,
        url: &str,
        options: &SessionOptions,
        clips: &[(usize, RecordedAudio)],
    ) -> Result<(), EvalError> {
        options.validate_audio_to_audio()?;
        if scenario.needs_images() || scenario.turns.iter().any(|turn| turn.dtmf.is_some()) {
            return Err(EvalError::Unsupported(
                "audio-to-audio images or DTMF".into(),
            ));
        }
        let mut spoken = 0usize;
        for turn in &scenario.turns {
            if let Some(text) = &turn.user {
                UserSpeechOwner::validate_text(text)?;
                spoken += 1;
            }
        }
        if spoken > 128 {
            return Err(EvalError::Limit("user speech turns"));
        }
        let validated = scenario.audio_response_validation_copy()?;
        connect_url(url, &validated)?;
        if scenario.needs_judge() {
            validated
                .judge
                .as_ref()
                .and_then(|settings| settings.eval.as_ref())
                .ok_or_else(|| {
                    EvalError::Scenario(
                        "audio eval assertions require judge.eval configuration".into(),
                    )
                })?
                .build()?;
        }
        validate_recording_inputs(scenario, clips.iter().cloned())?;
        Ok(())
    }
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    pub(crate) fn from_audio_to_audio_plan(
        scenario: ScriptScenario,
        url: &str,
        options: SessionOptions,
        transcription: BotTranscriptionConfig,
        speech: Option<(CartesiaUserSpeechSettings, CartesiaUserSpeechVoice)>,
        clips: Vec<(usize, RecordedAudio)>,
        budget: UserSpeechBudget,
    ) -> Result<Self, EvalError> {
        Self::validate_audio_to_audio_plan(&scenario, url, &options, &clips)?;
        transcription.validate_audio_to_audio()?;
        if speech.is_none()
            && scenario.turns.iter().enumerate().any(|(index, turn)| {
                turn.user.is_some() && !clips.iter().any(|(recorded, _)| *recorded == index)
            })
        {
            return Err(EvalError::Scenario(
                "audio-to-audio input coverage is incomplete".into(),
            ));
        }
        let mut session = Self::with_bot_transcription(scenario, url, options, transcription)?
            .with_recorded_audio_inputs(clips)?;
        if let Some((settings, voice)) = speech {
            session.user_speech = Some(UserSpeechOwner::new(settings, voice));
        }
        session = session.with_user_speech_budget(budget);
        session.audio_to_audio = Some(AudioToAudioState {
            run: None,
            admission_open: true,
            stop_bot_attempted: false,
        });
        Ok(session)
    }
    fn is_audio_to_audio(&self) -> bool {
        #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
        return self.audio_to_audio.is_some();
        #[cfg(not(all(feature = "user-speech", feature = "bot-audio-transcription")))]
        false
    }
    /// Check the original caller and run limits without creating a new timer.
    fn check_audio_to_audio_run(&mut self) -> Result<(), EvalError> {
        self.check_user_speech_deadline()?;
        #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
        if let Some(state) = &self.audio_to_audio {
            if !state.admission_open {
                return Err(EvalError::Cancelled);
            }
            if let Some((cancel, session_deadline)) = &state.run {
                if cancel.is_cancelled() {
                    return Err(EvalError::Cancelled);
                }
                if Instant::now() >= *session_deadline {
                    return Err(EvalError::Timeout("session"));
                }
            }
        }
        Ok(())
    }
    fn check_audio_to_audio_deadline(&mut self, deadline: Instant) -> Result<(), EvalError> {
        self.check_audio_to_audio_run()?;
        if Instant::now() >= deadline {
            return Err(EvalError::Timeout("event"));
        }
        Ok(())
    }
    fn audio_to_audio_deadline(&self, deadline: Instant) -> Instant {
        #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
        if let Some((_, session_deadline)) = self
            .audio_to_audio
            .as_ref()
            .and_then(|state| state.run.as_ref())
        {
            return deadline.min(*session_deadline);
        }
        deadline
    }
    fn enable_bot_audio_url(&mut self) -> Result<(), EvalError> {
        let mut url = url::Url::parse(&self.url)
            .map_err(|_| EvalError::Scenario("invalid bot URL".into()))?;
        let pairs: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "skip_tts" && key != "capture_bot_audio")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.set_query(None);
        {
            let mut query = url.query_pairs_mut();
            for (key, value) in pairs {
                query.append_pair(&key, &value);
            }
            query.append_pair("capture_bot_audio", "true");
        }
        self.url = url.into();
        Ok(())
    }
    /// Synthesize uncovered user turns with explicitly configured Cartesia.
    /// Recordings always win, in either builder order. Reference text is never
    /// sent as text. The provider/cache starts only after each turn admission.
    #[cfg(feature = "user-speech")]
    pub fn with_cartesia_user_speech(
        mut self,
        settings: CartesiaUserSpeechSettings,
        voice: CartesiaUserSpeechVoice,
    ) -> Result<Self, EvalError> {
        #[cfg(feature = "user-images")]
        if self.user_images.is_some() {
            return Err(EvalError::Unsupported(
                "images combined with user speech".into(),
            ));
        }
        #[cfg(feature = "bot-audio-transcription")]
        if self.transcription_config.is_some() || self.transcription.is_some() {
            return Err(EvalError::Unsupported(
                "user speech combined with bot audio transcription".into(),
            ));
        }
        let mut count = 0usize;
        for turn in &self.scenario.turns {
            if let Some(text) = &turn.user {
                UserSpeechOwner::validate_text(text)?;
                count += 1;
            }
        }
        if count > 128 {
            return Err(EvalError::Limit("user speech turns"));
        }
        let mut owner = UserSpeechOwner::new(settings, voice);
        if let Some(budget) = &self.user_speech_budget {
            owner.with_budget(budget.clone());
        }
        self.user_speech = Some(owner);
        Ok(self)
    }
    /// Loader-precharged recordings are frozen so later public builder calls
    /// cannot replace them with larger clips and evade the shared run budget.
    #[cfg(feature = "user-speech")]
    pub(crate) fn with_user_speech_budget(mut self, budget: UserSpeechBudget) -> Self {
        if let Some(owner) = &mut self.user_speech {
            owner.with_budget(budget.clone());
        }
        self.user_speech_budget = Some(budget);
        self
    }
    /// Attach only after immutable recordings and complete release preflight.
    /// There is no public setter for the selected plan or its execution budget.
    #[cfg(feature = "user-speech")]
    pub(crate) fn with_hosted_user_speech(
        mut self,
        prepared: PreparedHostedSpeech,
    ) -> Result<Self, EvalError> {
        #[cfg(feature = "bot-audio-transcription")]
        if self.transcription_config.is_some() || self.transcription.is_some() {
            return Err(EvalError::Unsupported(
                "hosted user speech combined with bot audio transcription".into(),
            ));
        }
        #[cfg(feature = "user-images")]
        if self.user_images.is_some() {
            return Err(EvalError::Unsupported(
                "images combined with hosted user speech".into(),
            ));
        }
        let mut uncovered = false;
        let mut count = 0usize;
        for (index, turn) in self.scenario.turns.iter().enumerate() {
            if turn.dtmf.is_some() {
                return Err(EvalError::Unsupported(
                    "hosted user speech does not support DTMF".into(),
                ));
            }
            if let Some(text) = &turn.user {
                UserSpeechOwner::validate_text(text)?;
                count += 1;
                uncovered |= !self.recordings.contains_key(&index);
            }
        }
        if count > 128 {
            return Err(EvalError::Limit("user speech turns"));
        }
        if uncovered {
            let voice = prepared.voice.ok_or_else(|| {
                EvalError::Scenario("hosted user speech input coverage is incomplete".into())
            })?;
            self = self.with_cartesia_user_speech(prepared.settings, voice)?;
        }
        // All-WAV plans still freeze recordings and select actual joined cleanup,
        // without constructing an HTTP/cache owner or charging WAVs a second time.
        self = self.with_user_speech_budget(prepared.budget);
        self.hosted_speech = Some(HostedSpeechState {
            admission_closed: false,
            stop_bot_attempted: false,
        });
        Ok(self)
    }

    /// A failed post-spawn setup keeps the original prepared session, records its
    /// primary failure before the first cleanup await, and exposes cleanup errors.
    #[cfg(feature = "user-speech")]
    pub(crate) async fn stop_hosted_before_run(mut self, kind: &str, reason: &str) -> ScriptResult {
        self.result.fail(AssertionFailure {
            turn_index: -1,
            expectation_index: -1,
            event_name: "process".into(),
            kind: kind.into(),
            reason: reason.into(),
        });
        self.shutdown().await;
        self.result
    }

    /// Replace selected user turns with real recorded PCM input. The user text
    /// remains a judge reference and is never sent as text for these turns.
    /// Requires a compatible endpoint, such as EvalBotHost with_raw_audio_input(true).
    /// Validate all indices and aggregate storage before any connection.
    pub fn with_recorded_audio_inputs(
        mut self,
        inputs: impl IntoIterator<Item = (usize, RecordedAudio)>,
    ) -> Result<Self, EvalError> {
        #[cfg(feature = "user-images")]
        if self.user_images.is_some() {
            return Err(EvalError::Unsupported(
                "images combined with recorded input".into(),
            ));
        }
        #[cfg(feature = "user-speech")]
        if self.user_speech_budget.is_some() {
            return Err(EvalError::Scenario(
                "preflighted user speech recording plan is immutable".into(),
            ));
        }
        let recordings = validate_recording_inputs(&self.scenario, inputs)?;
        self.recordings = recordings;
        Ok(self)
    }
    /// Explicit passive registration sidecars; text-only image route, validated before connection.
    #[cfg(feature = "user-images")]
    pub fn with_user_images(
        mut self,
        inputs: impl IntoIterator<Item = (usize, crate::EncodedUserImage)>,
    ) -> Result<Self, EvalError> {
        if self.images_frozen {
            return Err(EvalError::Scenario(
                "preflighted image plan is immutable".into(),
            ));
        }
        if !self.recordings.is_empty() || self.capture.is_some() {
            return Err(EvalError::Unsupported(
                "images combined with audio input/output".into(),
            ));
        }
        #[cfg(feature = "user-speech")]
        if self.user_speech.is_some() || self.user_speech_budget.is_some() {
            return Err(EvalError::Unsupported(
                "images combined with user speech".into(),
            ));
        }
        #[cfg(feature = "bot-audio-transcription")]
        if self.transcription_config.is_some() {
            return Err(EvalError::Unsupported(
                "images combined with bot transcription".into(),
            ));
        }
        self.user_images = Some(crate::user_images::validate_inputs(
            &self.scenario,
            &self.options,
            inputs,
        )?);
        Ok(self)
    }
    #[cfg(feature = "user-images")]
    pub(crate) fn freeze_user_images(mut self) -> Self {
        self.images_frozen = true;
        self
    }
    /// Dropping this future also drops its transport owner; no reader task is detached.
    pub async fn run(mut self, cancel: Cancellation) -> ScriptResult {
        self.run_inner(cancel).await;
        self.result
    }
    /// Capture bounded original-rate bot PCM while evaluating. No STT or judging
    /// is performed on the audio. A successful text assertion can end capture
    /// before synthesis finishes; use an explicit scenario completion event.
    /// Invalid limits fail before connection. Runtime failures retain admitted chunks.
    pub async fn run_with_bot_audio(
        mut self,
        cancel: Cancellation,
        limits: BotAudioLimits,
    ) -> Result<BotAudioRun, EvalError> {
        #[cfg(feature = "user-images")]
        if self.user_images.is_some() {
            return Err(EvalError::Unsupported(
                "images combined with bot audio capture".into(),
            ));
        }
        limits.validate()?;
        #[cfg(feature = "user-speech")]
        if self.user_speech.is_some() {
            return Err(EvalError::Unsupported(
                "user speech combined with bot audio capture".into(),
            ));
        }
        #[cfg(feature = "bot-audio-transcription")]
        if self.transcription_config.is_some() {
            return Err(EvalError::Unsupported(
                "use run for a transcription session".into(),
            ));
        }
        self.enable_bot_audio_url()?;
        self.capture = Some(Capture::new(limits));
        self.run_inner(cancel).await;
        Ok(BotAudioRun {
            result: self.result,
            chunks: self.capture.take().unwrap().chunks,
        })
    }
    /// Check the opt-in direct recording selection without starting any owner,
    /// connection, synthesis, model, or output file. Callers may use this before
    /// reserving a recording destination; `run_with_timeline_recording` repeats it.
    pub fn validate_timeline_recording(
        &self,
        limits: TimelineRecordingLimits,
    ) -> Result<(), EvalError> {
        limits.validate()?;
        if self.scenario.needs_images()
            || self.scenario.turns.iter().any(|turn| turn.dtmf.is_some())
        {
            return Err(EvalError::Unsupported(
                "timeline recording with images or DTMF".into(),
            ));
        }
        #[cfg(feature = "user-images")]
        if self.user_images.is_some() {
            return Err(EvalError::Unsupported(
                "timeline recording with images".into(),
            ));
        }
        #[cfg(feature = "bot-audio-transcription")]
        if !self.is_audio_to_audio()
            && (self.transcription_config.is_some() || self.transcription.is_some())
        {
            return Err(EvalError::Unsupported(
                "timeline recording with bot audio response or transcription".into(),
            ));
        }
        // Joint construction installs the bounded capture consumed by STT. It
        // is not an independently requested bot-only export.
        if !self.is_audio_to_audio() && self.capture.is_some() {
            return Err(EvalError::Unsupported(
                "timeline recording with existing bot-only capture".into(),
            ));
        }
        #[cfg(feature = "user-speech")]
        if self.hosted_speech.is_some() {
            return Err(EvalError::Unsupported(
                "timeline recording with hosted user speech".into(),
            ));
        }
        let audio_input = !self.recordings.is_empty();
        #[cfg(feature = "user-speech")]
        let audio_input = audio_input || self.user_speech.is_some();
        if !audio_input {
            return Err(EvalError::Unsupported(
                "timeline recording requires selected recorded or user-speech input".into(),
            ));
        }
        Ok(())
    }
    /// Record both locally admitted user source PCM and simulated bot playout
    /// on one clock. Wire idle and padding are excluded; bot future tails are
    /// cropped at interruption/end. This is not a remote playback receipt.
    /// Awaiting cancellation returns a diagnostic prefix after actual cleanup;
    /// dropping the future cannot promise a recording or asynchronous joins.
    pub async fn run_with_timeline_recording(
        mut self,
        cancel: Cancellation,
        limits: TimelineRecordingLimits,
    ) -> Result<TimelineRecordingRun, EvalError> {
        self.validate_timeline_recording(limits)?;
        self.enable_bot_audio_url()?;
        self.timeline = Some(TimelineRecordingState {
            recorder: TimelineRecorder {
                // run_inner resets this to its exact shared pre-connect epoch.
                origin: Instant::now(),
                capture: Arc::new(Mutex::new(TimelineCapture::new(limits)?)),
            },
            closed: false,
            stop_bot_attempted: false,
        });
        self.run_inner(cancel).await;
        // Playback/socket/speech have actually retired before taking the sole
        // remaining ledger reference. No producer may alter the final snapshot.
        let state = self.timeline.take().expect("selected timeline recording");
        let capture = Arc::try_unwrap(state.recorder.capture)
            .map_err(|_| EvalError::Scenario("timeline recording still has an owner".into()))?
            .into_inner()
            .map_err(|_| EvalError::Scenario("timeline recording ledger poisoned".into()))?;
        Ok(capture.into_run(self.result))
    }
    async fn run_inner(&mut self, cancel: Cancellation) {
        let started = Instant::now();
        if let Some(state) = &mut self.timeline {
            state.recorder.origin = started;
        }
        self.stream = EventStream::new(self.options.event_limits.clone());
        self.bot_ready_received = false;
        #[cfg(feature = "bot-audio-transcription")]
        {
            self.pending_recognition = None;
        }
        let max_duration = self.options.max_duration;
        #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
        if let Some(state) = &mut self.audio_to_audio {
            state.run = Some((cancel.clone(), started + max_duration));
        }
        #[cfg(feature = "user-speech")]
        let prepared = if let Some(owner) = &mut self.user_speech {
            owner.configure_run(
                cancel.clone(),
                started + max_duration,
                self.recordings
                    .values()
                    .map(|clip| clip.pcm_bytes().len())
                    .sum(),
            )
        } else {
            Ok(())
        };
        #[cfg(not(feature = "user-speech"))]
        let prepared: Result<(), EvalError> = Ok(());
        let outcome = tokio::select! {
            biased;
            _=cancel.cancelled()=>Err(EvalError::Cancelled),
            _=tokio::time::sleep(max_duration)=>Err(EvalError::Timeout("session")),
            result=async { prepared?; self.drive().await }=>result,
        };
        // A classifier can finish synchronously in the same poll that the
        // caller cancels. Fence final publication as well as the select branch.
        let outcome = if cancel.is_cancelled() {
            Err(EvalError::Cancelled)
        } else {
            outcome
        };
        // Freeze the semantic bot cutoff before formatting errors or awaiting
        // cleanup. Existing user receipts remain writable until Playback joins.
        self.close_timeline_recording();
        if let Err(error) = outcome {
            if let (Ok(index), Some(started)) =
                (usize::try_from(self.active_turn), self.active_turn_started)
            {
                self.result.turns[index].duration_ms = started.elapsed().as_millis() as u64;
            }
            let kind = match &error {
                EvalError::Cancelled => "cancelled",
                EvalError::Judge(ClassifierError::Cancelled) => "cancelled",
                #[cfg(feature = "user-speech")]
                _ if self
                    .user_speech
                    .as_ref()
                    .and_then(UserSpeechOwner::failure_kind)
                    .is_some() =>
                {
                    self.user_speech
                        .as_ref()
                        .and_then(UserSpeechOwner::failure_kind)
                        .unwrap()
                }
                EvalError::Judge(ClassifierError::Deadline) => "judge_timeout",
                EvalError::Judge(ClassifierError::Limit(_)) => "limit_exceeded",
                EvalError::Judge(_) => "judge_error",
                EvalError::Limit(_) => "limit_exceeded",
                EvalError::Disconnected => "disconnected",
                _ if self.phase == "connect" => "connect_failed",
                EvalError::Timeout("handshake") => "handshake_timeout",
                EvalError::Timeout("send_after") => "send_after_timeout",
                EvalError::Timeout(
                    "bot transcription HTTP"
                    | "bot transcription settlement"
                    | "final bot transcription settlement",
                ) if self.is_audio_to_audio() => "bot_transcription_timeout",
                EvalError::Timeout(_) => "timeout",
                _ => "error",
            };
            self.result.fail(AssertionFailure {
                turn_index: self.active_turn,
                expectation_index: self.active_expectation,
                event_name: usize::try_from(self.active_turn)
                    .ok()
                    .and_then(|ti| self.scenario.turns.get(ti))
                    .and_then(|turn| {
                        usize::try_from(self.active_expectation)
                            .ok()
                            .and_then(|ei| turn.expect.get(ei))
                    })
                    .map_or_else(|| self.phase.into(), |exp| exp.event.clone()),
                reason: error.to_string(),
                kind: kind.into(),
            });
        }
        self.shutdown().await;
        self.result.events_seen = std::mem::take(&mut self.stream.events_seen);
        self.result.duration_ms = started.elapsed().as_millis() as u64;
        self.result.passed = self.result.failures.is_empty()
            && self.result.turns.iter().all(|t| t.status == "passed");
    }
    /// The hosted runner retries only before a WebSocket is established. No
    /// user turn or RTVI handshake is replayed on a fresh connection.
    pub(crate) async fn connect_hosted(&mut self) -> Result<(), EvalError> {
        loop {
            match WebSocketSession::connect(&self.url, self.options.transport.clone()).await {
                Ok(socket) => {
                    self.socket = Some(socket);
                    return Ok(());
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    }
    async fn drive(&mut self) -> Result<(), EvalError> {
        #[cfg(feature = "bot-audio-transcription")]
        if let Some(config) = self.transcription_config.take() {
            self.transcription = Some(BotTranscription::new(config)?);
        }
        if self.socket.is_none() {
            self.socket =
                Some(WebSocketSession::connect(&self.url, self.options.transport.clone()).await?);
        }
        self.phase = "handshake";
        self.send("client-ready",json!({"version":PROTOCOL_VERSION,"about":{"library":"pipecat-evals-rust","library_version":env!("CARGO_PKG_VERSION")}})).await?;
        let deadline = Instant::now() + self.options.handshake_timeout;
        loop {
            if self.pump(deadline).await.map_err(|e| {
                if matches!(e, EvalError::Timeout("event")) {
                    EvalError::Timeout("handshake")
                } else {
                    e
                }
            })? {
                break;
            }
        }
        let config = self.scenario.observer_config();
        if config.as_object().is_some_and(|v| !v.is_empty()) {
            self.send_control(serializer::EVAL_CONFIGURE, config)
                .await?;
        }
        if !self.scenario.context.is_empty() {
            self.send_control(
                serializer::EVAL_CONTEXT,
                json!({"messages":self.scenario.context}),
            )
            .await?;
        }
        let audio_input = !self.recordings.is_empty();
        #[cfg(feature = "user-speech")]
        let audio_input = audio_input || self.user_speech.is_some();
        if audio_input {
            let sender = self
                .socket
                .as_ref()
                .ok_or(EvalError::Disconnected)?
                .sender();
            self.playback = Some(if let Some(state) = &self.timeline {
                Playback::start_with_timeline(sender, &self.options, Some(state.recorder.clone()))
            } else {
                Playback::start(sender, &self.options)
            });
        }
        self.phase = "turn";
        for index in 0..self.scenario.turns.len() {
            self.active_turn = index as i64;
            self.active_expectation = -1;
            let started = Instant::now();
            self.active_turn_started = Some(started);
            let turn = self.scenario.turns[index].clone();
            let send_after_event = turn
                .send_after
                .as_ref()
                .and_then(|after| after.event.clone());
            match self.run_turn(index, turn).await {
                Err(EvalError::Timeout("send_after")) => self.result.fail(AssertionFailure {
                    turn_index: index as i64,
                    expectation_index: -1,
                    event_name: send_after_event.unwrap_or_else(|| "send_after".into()),
                    reason: "send_after event was not observed before its deadline".into(),
                    kind: "send_after_timeout".into(),
                }),
                result => result?,
            }
            let record = &mut self.result.turns[index];
            record.duration_ms = started.elapsed().as_millis() as u64;
            record.status = if record.failures.is_empty() {
                "passed"
            } else {
                "failed"
            }
            .into();
            if !record.failures.is_empty() && self.scenario.stop_on_failure {
                break;
            }
        }
        #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
        if self.is_audio_to_audio() && self.result.failures.is_empty() {
            self.settle_audio_to_audio_prefix().await?;
        }
        self.active_turn = -1;
        self.active_expectation = -1;
        self.active_turn_started = None;
        Ok(())
    }
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    async fn settle_audio_to_audio_prefix(&mut self) -> Result<(), EvalError> {
        let deadline =
            self.audio_to_audio_deadline(Instant::now() + self.options.bot_quiet_timeout);
        if let Some(engine) = &mut self.transcription {
            engine.request_idle_notification();
        }
        loop {
            let settled = async {
                self.drain_received_before(deadline).await?;
                self.check_audio_to_audio_deadline(deadline)?;
                self.publish_pending_recognition()?;
                if self
                    .transcription
                    .as_ref()
                    .is_some_and(BotTranscription::is_idle)
                {
                    // No await between Empty, epoch publication, idle proof and
                    // closing this successful locally admitted prefix.
                    self.check_audio_to_audio_deadline(deadline)?;
                    return Ok(true);
                }
                self.pump(deadline).await?;
                Ok(false)
            }
            .await;
            match settled {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(EvalError::Timeout("event")) => {
                    return Err(EvalError::Timeout("final bot transcription settlement"))
                }
                Err(error) => return Err(error),
            }
        }
    }
    fn turn_boundary(&mut self) -> Result<(), EvalError> {
        self.stream.turn_boundary();
        #[cfg(feature = "bot-audio-transcription")]
        {
            self.pending_recognition = None;
        }
        #[cfg(feature = "bot-audio-transcription")]
        if let Some(engine) = &mut self.transcription {
            engine.set_epoch(self.stream.epoch())?;
        }
        Ok(())
    }
    async fn run_turn(&mut self, index: usize, turn: ScriptTurn) -> Result<(), EvalError> {
        self.matcher.reset_turn();
        // Drain the bounded transport admission queue before the send boundary so
        // already received output cannot satisfy a subsequent user turn.
        let fence_deadline =
            self.audio_to_audio_deadline(Instant::now() + self.options.bot_quiet_timeout);
        self.drain_received_before(fence_deadline).await?;
        #[cfg(feature = "bot-audio-transcription")]
        self.publish_pending_recognition()?;
        #[cfg(feature = "bot-audio-transcription")]
        if turn.send_after.is_none() && (turn.user.is_some() || turn.dtmf.is_some()) {
            let deadline = Instant::now() + self.options.bot_quiet_timeout;
            if let Some(engine) = &mut self.transcription {
                engine.request_idle_notification();
            }
            while self
                .transcription
                .as_ref()
                .is_some_and(|engine| !engine.is_idle())
            {
                if Instant::now() >= deadline {
                    return Err(EvalError::Timeout("bot transcription settlement"));
                }
                // Do not periodically cancel pump: it may already own a
                // completed transcript while draining admitted interruption
                // packets before publication. The engine's tick emits an idle
                // notification, and pump enforces the same absolute deadline.
                self.pump(deadline).await.map_err(|error| {
                    if matches!(error, EvalError::Timeout("event")) {
                        EvalError::Timeout("bot transcription settlement")
                    } else {
                        error
                    }
                })?;
            }
        }
        if let Some(after) = turn.send_after {
            let target = if let Some(event) = after.event {
                let deadline = Instant::now() + self.options.send_after_timeout;
                loop {
                    if let Some(at) = self.stream.latest_event_times.get(&event) {
                        break *at + Duration::from_millis(after.delay_ms);
                    }
                    self.pump(deadline).await.map_err(|e| {
                        if matches!(e, EvalError::Timeout("event")) {
                            EvalError::Timeout("send_after")
                        } else {
                            e
                        }
                    })?;
                }
            } else {
                Instant::now() + Duration::from_millis(after.delay_ms)
            };
            self.pump_until(target).await?;
        } else if self.stream.bot_speaking
            && (turn.user.is_some() || turn.dtmf.is_some() || index > 0)
        {
            let deadline = Instant::now() + self.options.bot_quiet_timeout;
            while self.stream.bot_speaking {
                match self.pump(deadline).await {
                    Err(EvalError::Timeout("event")) => break,
                    other => {
                        other?;
                    }
                }
            }
            if turn.user.is_none() && turn.dtmf.is_none() && index > 0 {
                self.turn_boundary()?;
            }
        }
        #[cfg(feature = "user-images")]
        if let Some(image) = self
            .user_images
            .as_mut()
            .and_then(|images| images.remove(&index))
        {
            self.send_user_image(image).await?;
        }
        let mut audio_completion = None;
        #[cfg(feature = "user-speech")]
        let mut speech_anchor = None;
        if let Some(user) = turn.user {
            self.record_pending_reply()?;
            self.turn_boundary()?;
            #[cfg(feature = "user-speech")]
            let admitted = Instant::now();
            #[cfg(feature = "user-speech")]
            if self.is_audio_to_audio() {
                speech_anchor = Some(admitted);
            }
            if let Some(judge) = &mut self.judge {
                judge.add_user_message(&user)?;
            }
            if let Some(clip) = self.recordings.remove(&index) {
                let playback = self.playback.as_ref().ok_or_else(|| {
                    EvalError::Scenario("recording playback is not running".into())
                })?;
                #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
                let completion = if self.is_audio_to_audio() {
                    RecordedCompletion::Timed(playback.queue_timed(clip)?)
                } else {
                    RecordedCompletion::Ordinary(playback.queue(clip)?)
                };
                #[cfg(not(all(feature = "user-speech", feature = "bot-audio-transcription")))]
                let completion = RecordedCompletion::Ordinary(playback.queue(clip)?);
                audio_completion = Some((completion, Instant::now() + Duration::from_secs(61)));
            } else {
                #[cfg(feature = "user-speech")]
                if let Some(owner) = &mut self.user_speech {
                    // Assertion deadlines include cache/HTTP production time.
                    speech_anchor = Some(admitted);
                    owner.begin(user, admitted)?;
                } else if self.hosted_speech.is_some() {
                    return Err(EvalError::Scenario(
                        "hosted user speech input coverage is incomplete".into(),
                    ));
                } else {
                    self.send_user_text(user).await?;
                }
                #[cfg(not(feature = "user-speech"))]
                self.send_user_text(user).await?;
            }
        } else if let Some(dtmf) = turn.dtmf {
            self.record_pending_reply()?;
            self.turn_boundary()?;
            if let Some(judge) = &mut self.judge {
                judge.add_user_message(&format!("(DTMF keypad input: {dtmf})"))?;
            }
            self.send(
                "dtmf",
                json!({"buttons":dtmf.chars().map(|c|c.to_string()).collect::<Vec<_>>()}),
            )
            .await?;
        }
        let anchor = Instant::now();
        #[cfg(feature = "user-speech")]
        let anchor = speech_anchor.unwrap_or(anchor);
        for (ei, expectation) in turn.expect.into_iter().enumerate() {
            self.active_expectation = ei as i64;
            let deadline = anchor
                + expectation
                    .within_ms
                    .map(Duration::from_millis)
                    .unwrap_or(self.options.default_timeout);
            let name = expectation.event.clone();
            let (outcome, judgments) = self.match_expectation(expectation, deadline).await?;
            let (passed, matched, timeout) = match outcome {
                MatchOutcome::Passed(text) => (true, text, false),
                MatchOutcome::Failed { kind, reason } => {
                    let timeout = kind == "timeout";
                    self.result.fail(AssertionFailure {
                        turn_index: index as i64,
                        expectation_index: ei as i64,
                        event_name: name.clone(),
                        reason,
                        kind,
                    });
                    (false, String::new(), timeout)
                }
                MatchOutcome::Pending => unreachable!("matching loop must resolve"),
            };
            self.result.turns[index]
                .expectations
                .push(ExpectationResult {
                    expectation_index: ei,
                    event_name: name,
                    passed,
                    matched,
                    judgments,
                });
            if timeout {
                break;
            }
        }
        if !(self.scenario.stop_on_failure && !self.result.turns[index].failures.is_empty()) {
            if let Some((completed, playback_deadline)) = audio_completion {
                self.settle_recorded_input(completed, playback_deadline)
                    .await?;
            }
            #[cfg(feature = "user-speech")]
            while self
                .user_speech
                .as_ref()
                .is_some_and(|owner| !owner.settled())
            {
                self.check_user_speech_deadline()?;
                let deadline = self
                    .user_speech
                    .as_ref()
                    .and_then(UserSpeechOwner::deadline)
                    .expect("active user speech has a deadline");
                self.pump(deadline).await?;
            }
        }
        Ok(())
    }
    async fn settle_recorded_input(
        &mut self,
        mut completed: RecordedCompletion,
        playback_deadline: Instant,
    ) -> Result<(), EvalError> {
        loop {
            if self.is_audio_to_audio() {
                self.check_audio_to_audio_run()?;
            }
            tokio::select! {
                biased;
                result = completed.receive() => {
                    let receipt = result.map_err(|_| EvalError::Scenario("recording ended before playback completion".into()))?;
                    if self.is_audio_to_audio() {
                        self.check_audio_to_audio_run()?;
                        if receipt.is_none_or(|at| at >= playback_deadline) {
                            return Err(EvalError::Timeout("recording playback"));
                        }
                    }
                    return Ok(());
                }
                result = self.pump(self.recorded_playback_deadline(playback_deadline)) => {
                    result.map_err(|error| {
                        if self.is_audio_to_audio() && matches!(error, EvalError::Timeout("event")) {
                            EvalError::Timeout("recording playback")
                        } else { error }
                    })?;
                }
            }
        }
    }
    fn recorded_playback_deadline(&self, fixed: Instant) -> Instant {
        if self.is_audio_to_audio() {
            return self.audio_to_audio_deadline(fixed);
        }
        #[cfg(feature = "user-speech")]
        if self.user_speech.is_some() || self.user_speech_budget.is_some() {
            return fixed;
        }
        // Keep the established recorded-only path's behavior unchanged.
        let _ = fixed;
        Instant::now() + Duration::from_secs(61)
    }
    async fn send_user_text(&mut self, user: String) -> Result<(), EvalError> {
        self.send("send-text", json!({"content":user,"options":{"run_immediately":true,"audio_response":self.capture.is_some()}})).await
    }
    fn check_user_speech_deadline(&mut self) -> Result<(), EvalError> {
        #[cfg(feature = "user-speech")]
        if self
            .hosted_speech
            .as_ref()
            .is_some_and(|state| state.admission_closed)
        {
            return Err(EvalError::Cancelled);
        }
        #[cfg(feature = "user-speech")]
        if let Some(owner) = &mut self.user_speech {
            owner.check_deadline()?;
        }
        Ok(())
    }
    fn judged_response_event(&self) -> &'static str {
        #[cfg(feature = "bot-audio-transcription")]
        if self.transcription_config.is_some() || self.transcription.is_some() {
            return "response";
        }
        "llm_response"
    }
    fn judge_reply_busy(&self) -> bool {
        #[cfg(feature = "bot-audio-transcription")]
        if let Some(engine) = &self.transcription {
            return engine.reply_in_progress();
        }
        self.stream.bot_speaking
    }
    fn record_event(&mut self, event: &Value) -> Result<(), EvalError> {
        if event["type"] == self.judged_response_event() {
            if let Some(judge) = &mut self.judge {
                judge.add_assistant_message(event["text"].as_str().unwrap_or(""))?;
            }
        }
        Ok(())
    }
    fn record_pending_reply(&mut self) -> Result<(), EvalError> {
        if self.judge.is_some() {
            // Only output before the send boundary is attributed to the old turn.
            // Preserve other pending events for the normal turn-boundary filter.
            for event in self.stream.pending_responses(self.judged_response_event()) {
                self.record_event(&event)?;
            }
        }
        Ok(())
    }
    async fn match_expectation(
        &mut self,
        expectation: Expectation,
        deadline: Instant,
    ) -> Result<(MatchOutcome, Vec<JudgeVerdict>), EvalError> {
        let criterion = expectation.eval.clone();
        let is_call = expectation.is_call();
        let mut judgments = Vec::new();
        let mut check = PendingCheck::for_session(expectation);
        let mut outcome = self.matcher.buffered(&mut check);
        let mut no_deadline = None;
        let mut last_decision = None;
        loop {
            self.check_user_speech_deadline()?;
            if let Some(criterion) = &criterion {
                for event in check.take_judged_calls() {
                    let request = self.judge.as_mut().expect("validated judge").prepare_call(
                        event["name"].as_str().unwrap_or("?"),
                        &event["args"],
                        criterion,
                    )?;
                    let Some(verdict) = self.run_judgment(request, deadline).await? else {
                        return Ok((judge_interrupted(), judgments));
                    };
                    let decision = verdict.decision;
                    judgments.push(verdict);
                    if decision != JudgeDecision::Yes {
                        return Ok((judge_no(), judgments));
                    }
                }
                if !is_call && matches!(outcome, MatchOutcome::Passed(_)) {
                    let request = self
                        .judge
                        .as_mut()
                        .expect("validated judge")
                        .prepare_reply(criterion)?;
                    let Some(verdict) = self.run_judgment(request, deadline).await? else {
                        return Ok((judge_interrupted(), judgments));
                    };
                    let decision = verdict.decision;
                    judgments.push(verdict);
                    if decision == JudgeDecision::Yes {
                        return Ok((outcome, judgments));
                    }
                    last_decision = Some(decision);
                    no_deadline = if decision == JudgeDecision::No {
                        Some(if self.judge_reply_busy() {
                            deadline
                        } else {
                            (Instant::now() + self.judge.as_ref().unwrap().no_grace()).min(deadline)
                        })
                    } else {
                        None
                    };
                    outcome = MatchOutcome::Pending;
                }
            }
            if outcome != MatchOutcome::Pending {
                break;
            }
            let until = no_deadline.unwrap_or(deadline);
            if Instant::now() >= until {
                outcome = if last_decision == Some(JudgeDecision::No) {
                    judge_no()
                } else if last_decision == Some(JudgeDecision::Continue) {
                    MatchOutcome::Failed {
                        kind: "judge_continue".into(),
                        reason: "judge still required more reply at the shared turn deadline"
                            .into(),
                    }
                } else {
                    self.matcher.timeout(&check)
                };
                break;
            }
            if let Some(event) = self.stream.pop() {
                if criterion.is_some()
                    && !is_call
                    && event["type"] == self.judged_response_event()
                    && event["text"].as_str().is_none_or(|s| s.trim().is_empty())
                {
                    continue;
                }
                self.record_event(&event)?;
                outcome = self.matcher.feed(&mut check, event)?;
                continue;
            }
            match self.pump(until).await {
                Err(EvalError::Timeout("event")) => {}
                other => {
                    other?;
                }
            }
        }
        Ok((outcome, judgments))
    }
    async fn run_judgment(
        &mut self,
        request: JudgeRequest,
        deadline: Instant,
    ) -> Result<Option<JudgeVerdict>, EvalError> {
        let epoch = self.stream.epoch();
        let future = request.run(CallOptions {
            deadline: Some(deadline),
            ..Default::default()
        });
        tokio::pin!(future);
        loop {
            self.check_user_speech_deadline()?;
            let verdict = tokio::select! {
                biased;
                result = &mut future => Some(result?),
                packet = self.pump(deadline) => {
                    match packet {
                        Err(EvalError::Timeout("event")) => return Err(EvalError::Judge(ClassifierError::Deadline)),
                        other => { other?; }
                    }
                    None
                }
            };
            if self.stream.epoch() != epoch {
                return Ok(None);
            }
            if let Some(verdict) = verdict {
                self.check_user_speech_deadline()?;
                tokio::time::timeout_at(deadline, self.drain_received_before(deadline))
                    .await
                    .map_err(|_| EvalError::Judge(ClassifierError::Deadline))?
                    .map_err(|error| {
                        if self.is_audio_to_audio() && matches!(error, EvalError::Timeout("event"))
                        {
                            EvalError::Judge(ClassifierError::Deadline)
                        } else {
                            error
                        }
                    })?;
                #[cfg(feature = "bot-audio-transcription")]
                self.publish_pending_recognition()?;
                if Instant::now() >= deadline {
                    return Err(EvalError::Judge(ClassifierError::Deadline));
                }
                if self.stream.epoch() != epoch {
                    return Ok(None);
                }
                return self
                    .judge
                    .as_mut()
                    .expect("validated judge")
                    .commit(&request, verdict)
                    .map(Some);
            }
        }
    }
    #[cfg(feature = "user-images")]
    async fn send_user_image(&mut self, image: crate::EncodedUserImage) -> Result<(), EvalError> {
        use base64::Engine;
        let bound = image.wire_bound()?;
        let planned = self
            .image_wire_bytes
            .checked_add(bound)
            .ok_or(EvalError::Limit("outgoing image bytes"))?;
        if bound > self.options.transport.max_message_bytes
            || planned > self.options.max_wire_bytes
            || self.image_wire_messages >= self.options.max_wire_messages
        {
            return Err(EvalError::Limit("outgoing image wire budget"));
        }
        let id = self
            .next_id
            .checked_add(1)
            .ok_or(EvalError::Limit("message identities"))?;
        let encoded = base64::engine::general_purpose::STANDARD.encode(image.bytes());
        let wire = serializer::control(
            id,
            serializer::EVAL_IMAGE,
            serde_json::json!({"image":encoded,"format":image.format()}),
        )
        .encode()?;
        if wire.len() > bound {
            return Err(EvalError::Limit("outgoing image envelope"));
        }
        self.next_id = id;
        self.image_wire_bytes += wire.len();
        self.image_wire_messages += 1;
        let sender = self
            .socket
            .as_ref()
            .ok_or(EvalError::Disconnected)?
            .sender();
        tokio::time::timeout(
            self.options.transport.write_timeout,
            sender.send(Packet::Text(wire)),
        )
        .await
        .map_err(|_| EvalError::Timeout("send image"))??;
        Ok(())
    }
    async fn send(&mut self, kind: &str, data: Value) -> Result<(), EvalError> {
        self.next_id += 1;
        self.send_envelope(serializer::message(self.next_id, kind, data))
            .await
    }
    async fn send_control(&mut self, kind: &str, data: Value) -> Result<(), EvalError> {
        self.next_id += 1;
        self.send_envelope(serializer::control(self.next_id, kind, data))
            .await
    }
    async fn send_envelope(&mut self, envelope: Envelope) -> Result<(), EvalError> {
        let wire = envelope.encode()?;
        let sender = self
            .socket
            .as_ref()
            .ok_or(EvalError::Disconnected)?
            .sender();
        tokio::time::timeout(
            self.options.transport.write_timeout,
            sender.send(Packet::Text(wire)),
        )
        .await
        .map_err(|_| EvalError::Timeout("send"))??;
        Ok(())
    }
    async fn pump(&mut self, deadline: Instant) -> Result<bool, EvalError> {
        if self.is_audio_to_audio() {
            self.check_audio_to_audio_deadline(deadline)?;
        }
        self.check_user_speech_deadline()?;
        if Instant::now() >= deadline {
            return Err(EvalError::Timeout("event"));
        }
        // Internal drains may consume readiness while another select branch
        // wins. Preserve the handshake fact independently of that return value.
        if self.phase == "handshake" && self.bot_ready_received {
            return Ok(true);
        }
        #[cfg(feature = "bot-audio-transcription")]
        if self.pending_recognition.is_some() {
            self.drain_recognition(deadline).await?;
            return Ok(self.phase == "handshake" && self.bot_ready_received);
        }
        let mut health = self.playback.as_ref().map(Playback::health);
        let audio_error = async {
            if let Some(rx) = &mut health {
                loop {
                    if let Some(error) = rx.borrow_and_update().clone() {
                        return error;
                    }
                    if rx.changed().await.is_err() {
                        return "recording task ended unexpectedly".into();
                    }
                }
            }
            std::future::pending::<String>().await
        };
        #[cfg(feature = "user-speech")]
        let speech_deadline = self
            .user_speech
            .as_ref()
            .and_then(UserSpeechOwner::deadline);
        #[cfg(not(feature = "user-speech"))]
        let speech_deadline: Option<Instant> = None;
        let wake = speech_deadline.map_or(deadline, |at| at.min(deadline));
        #[cfg(feature = "user-speech")]
        let speech_enabled = self.user_speech.is_some() || self.is_audio_to_audio();
        #[cfg(not(feature = "user-speech"))]
        let speech_enabled = false;
        let (recognized, packet, speech_progress) = {
            let transcription = async {
                #[cfg(feature = "bot-audio-transcription")]
                if let Some(engine) = &mut self.transcription {
                    return engine.next().await;
                }
                std::future::pending::<Result<(u64, String), EvalError>>().await
            };
            let speech = async {
                #[cfg(feature = "user-speech")]
                if let Some(owner) = &mut self.user_speech {
                    return owner.progress(self.playback.as_ref()).await;
                }
                std::future::pending::<Result<(), EvalError>>().await
            };
            if speech_enabled {
                // One owner step per pump. Fair selection keeps socket/control
                // and producer live when either side is continuously ready.
                tokio::select! {
                    _ = tokio::time::sleep_until(wake) => (None, None, Some(Ok(()))),
                    result = speech => (None, None, Some(result)),
                    result = transcription => (Some(result), None, None),
                    error = audio_error => {
                        #[cfg(feature = "user-speech")]
                        if let Some(owner) = &mut self.user_speech { return Err(owner.playback_failed()); }
                        return Err(EvalError::Scenario(error));
                    },
                    packet = self.socket.as_mut().ok_or(EvalError::Disconnected)?.recv() => {
                        (None, Some(packet.ok_or(EvalError::Disconnected)?), None)
                    }
                }
            } else {
                // Preserve the original non-speech scheduling priority.
                tokio::select! { biased;
                    _ = tokio::time::sleep_until(deadline) => return Err(EvalError::Timeout("event")),
                    result = transcription => (Some(result), None, None),
                    error = audio_error => return Err(EvalError::Scenario(error)),
                    packet = self.socket.as_mut().ok_or(EvalError::Disconnected)?.recv() => {
                        (None, Some(packet.ok_or(EvalError::Disconnected)?), None)
                    }
                }
            }
        };
        self.check_user_speech_deadline()?;
        if let Some(progress) = speech_progress {
            progress?;
            if Instant::now() >= deadline {
                return Err(EvalError::Timeout("event"));
            }
            return Ok(self.phase == "handshake" && self.bot_ready_received);
        }
        if let Some(result) = recognized {
            let recognition = result?;
            #[cfg(feature = "bot-audio-transcription")]
            {
                // Own the popped result before the first await: classifier
                // completion may cancel this pump while the fence is pending.
                self.pending_recognition = Some(recognition);
                self.drain_recognition(deadline).await?;
            }
            #[cfg(not(feature = "bot-audio-transcription"))]
            let _ = recognition;
            return Ok(self.phase == "handshake" && self.bot_ready_received);
        }
        self.ingest(packet.expect("pump selects either recognition or transport"))
    }
    #[cfg(feature = "bot-audio-transcription")]
    async fn drain_recognition(&mut self, deadline: Instant) -> Result<(), EvalError> {
        #[cfg(test)]
        {
            if let Some(gate) = &mut self.recognition_drain_gate {
                if let Some(entered) = gate.entered.take() {
                    let _ = entered.send(());
                }
                gate.release.notified().await;
            }
            self.recognition_drain_gate = None;
        }
        // Fence admitted interruption packets before publication. This cannot
        // authenticate untagged remote PCM that arrives after a new epoch.
        self.drain_received_before(deadline).await?;
        if self.is_audio_to_audio() {
            self.check_audio_to_audio_deadline(deadline)?;
        }
        self.publish_pending_recognition()
    }
    #[cfg(feature = "bot-audio-transcription")]
    fn publish_pending_recognition(&mut self) -> Result<(), EvalError> {
        if let Some((epoch, text)) = &self.pending_recognition {
            // Keep ownership on a budget/error path. Append is atomic; the
            // bounded copy is released on successful admission or stale drop.
            self.stream.transcribed_response(*epoch, text.clone())?;
            self.pending_recognition = None;
        }
        Ok(())
    }
    fn ingest(&mut self, packet: Packet) -> Result<bool, EvalError> {
        let bytes = match packet {
            Packet::Text(text) => text.into_bytes(),
            Packet::Binary(bytes) => bytes.to_vec(),
        };
        self.wire_messages += 1;
        if self.wire_messages > self.options.max_wire_messages
            || bytes.len() > self.options.max_wire_bytes.saturating_sub(self.wire_bytes)
        {
            return Err(EvalError::Limit("wire message traffic"));
        }
        self.wire_bytes += bytes.len();
        // Invalid envelopes and unrelated protocol traffic are ignored, as by
        // EvalClientSerializer; traffic still counts toward the session limits.
        let envelope =
            match Envelope::parse_with_limit(&bytes, self.options.transport.max_message_bytes) {
                Ok(message) => message,
                Err(_) => return Ok(false),
            };
        if envelope.message_type == "bot-ready" {
            self.bot_ready_received = true;
            return Ok(true);
        }
        if envelope.message_type == "eval-bot-audio" {
            let data = envelope.data.as_ref().unwrap_or(&Value::Null);
            // Joint STT and recording share the narrow mono 16 kHz contract.
            // Validate it before either consumer can admit this packet.
            if self.is_audio_to_audio() {
                if data.get("sampleRate").and_then(Value::as_u64) != Some(16_000) {
                    return Err(EvalError::Unsupported(
                        "audio response requires 16 kHz PCM".into(),
                    ));
                }
                for key in ["numChannels", "channels"] {
                    if data
                        .get(key)
                        .is_some_and(|channels| channels.as_u64() != Some(1))
                    {
                        return Err(EvalError::Unsupported(
                            "audio response requires mono PCM16".into(),
                        ));
                    }
                }
            }
            if let Some(state) = &self.timeline {
                ingest_timeline_bot(&state.recorder, data)?;
                // Recording is an additional bounded consumer, not a terminal
                // route: joint capture and STT must receive the same packet.
            }
            if let Some(capture) = &mut self.capture {
                capture.ingest(data)?;
                #[cfg(feature = "bot-audio-transcription")]
                if let Some(engine) = &mut self.transcription {
                    let chunk = capture
                        .chunks
                        .last()
                        .expect("successful capture is nonempty");
                    if chunk.sample_rate != 16_000 {
                        return Err(EvalError::Unsupported(
                            "audio response requires 16 kHz PCM".into(),
                        ));
                    }
                    if self.stream.accepts_audio_reply() {
                        engine.ingest(
                            chunk.audio.clone(),
                            chunk.sample_rate,
                            self.stream.epoch(),
                        )?;
                    }
                }
                return Ok(false);
            }
            if self.timeline.is_some() {
                return Ok(false);
            }
        }
        self.stream.ingest(&envelope)?;
        if envelope.message_type == "bot-interrupted" {
            if let Some(state) = &self.timeline {
                state
                    .recorder
                    .lock()?
                    .interrupt_bot(state.recorder.elapsed_ns()?)?;
            }
        }
        #[cfg(feature = "bot-audio-transcription")]
        if let Some(engine) = &mut self.transcription {
            engine.set_epoch(self.stream.epoch())?;
        }
        Ok(false)
    }
    async fn drain_received_before(&mut self, deadline: Instant) -> Result<(), EvalError> {
        #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
        if self.is_audio_to_audio() {
            return self.drain_audio_to_audio(deadline).await;
        }
        let _ = deadline;
        self.drain_received().await
    }
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    fn try_received(&mut self) -> Result<Packet, tokio::sync::mpsc::error::TryRecvError> {
        #[cfg(test)]
        if let Some(receiver) = &mut self.fence_receiver {
            return receiver.try_recv();
        }
        self.socket
            .as_mut()
            .ok_or(tokio::sync::mpsc::error::TryRecvError::Disconnected)?
            .try_recv()
    }
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    async fn drain_audio_to_audio(&mut self, deadline: Instant) -> Result<(), EvalError> {
        const MAX_PACKETS: usize = 256;
        const ROUND_PACKETS: usize = 32;
        for count in 0..=MAX_PACKETS {
            self.check_audio_to_audio_deadline(deadline)?;
            if count > 0 && count.is_multiple_of(ROUND_PACKETS) {
                // Explicitly poll once. Pending is not completion, and all
                // popped producer resources remain in their existing owner.
                if let Some(owner) = &mut self.user_speech {
                    use std::{future::Future, task::Poll};
                    let progress = owner.progress(self.playback.as_ref());
                    tokio::pin!(progress);
                    if let Poll::Ready(result) =
                        std::future::poll_fn(|cx| Poll::Ready(progress.as_mut().poll(cx))).await
                    {
                        result?;
                    }
                }
                #[cfg(test)]
                {
                    self.fence_rounds += 1;
                }
                tokio::task::yield_now().await;
                self.check_audio_to_audio_deadline(deadline)?;
            }
            match self.try_received() {
                Ok(packet) => {
                    // The saturation probe must use ordinary wire/PCM/event
                    // accounting before it can terminate this fence.
                    self.ingest(packet)?;
                    self.check_audio_to_audio_deadline(deadline)?;
                    if count == MAX_PACKETS {
                        return Err(EvalError::Limit("audio-to-audio receive drain"));
                    }
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    self.check_audio_to_audio_deadline(deadline)?;
                    return Ok(());
                }
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                    return Err(EvalError::Disconnected)
                }
            }
        }
        unreachable!("the accounted saturation probe always terminates")
    }
    async fn drain_received(&mut self) -> Result<(), EvalError> {
        #[cfg(feature = "user-speech")]
        let mut drained = 0usize;
        loop {
            self.check_user_speech_deadline()?;
            #[cfg(feature = "user-speech")]
            if self.user_speech.is_some() && drained > 0 && drained.is_multiple_of(32) {
                tokio::task::yield_now().await;
            }
            match tokio::time::timeout(
                Duration::ZERO,
                self.socket.as_mut().ok_or(EvalError::Disconnected)?.recv(),
            )
            .await
            {
                Ok(Some(packet)) => {
                    self.ingest(packet)?;
                    #[cfg(feature = "user-speech")]
                    {
                        drained += 1;
                    }
                }
                Ok(None) => return Err(EvalError::Disconnected),
                Err(_) => return Ok(()),
            }
        }
    }
    async fn pump_until(&mut self, target: Instant) -> Result<(), EvalError> {
        while Instant::now() < target {
            match self.pump(target).await {
                Err(EvalError::Timeout("event")) => break,
                other => {
                    other?;
                }
            }
        }
        Ok(())
    }
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    async fn shutdown_audio_to_audio(&mut self) {
        self.close_timeline_recording();
        // Phase A has no await or native join. Revoke every producer before a
        // held model/cache call can delay any owner's stop signal.
        self.audio_to_audio.as_mut().unwrap().admission_open = false;
        self.pending_recognition = None;
        if let Some(owner) = &mut self.user_speech {
            owner.cancel_work();
        }
        if let Some(playback) = &self.playback {
            playback.request_stop();
        }
        if let Some(engine) = &mut self.transcription {
            engine.request_stop();
        }
        // This is the last possible application send. A cancelled cleanup wait
        // may resume after borrowed socket shutdown took its sender.
        let state = self.audio_to_audio.as_mut().unwrap();
        let send_stop = self.options.stop_bot && !state.stop_bot_attempted && self.socket.is_some();
        state.stop_bot_attempted = true;
        if send_stop {
            if let Err(error) = self.send_control(serializer::EVAL_CANCEL, json!({})).await {
                self.result
                    .debug_log
                    .push(format!("eval-cancel could not be sent: {error}"));
            }
        }
        // Phase B keeps every owner in its field across cancellable awaits.
        if let Some(playback) = &mut self.playback {
            if let Err(error) = playback.finish().await {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "recording".into(),
                    reason: error.to_string(),
                    kind: "audio_cleanup_error".into(),
                });
            }
        }
        self.playback.take();
        if let Some(socket) = &mut self.socket {
            match tokio::time::timeout(
                self.options.transport.close_timeout + self.options.transport.write_timeout,
                socket.finish_and_join(),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "transport_cleanup".into(),
                    reason: format!("transport cleanup: {error}"),
                    kind: "transport_cleanup_error".into(),
                }),
                Err(_) => {
                    self.result.fail(AssertionFailure {
                        turn_index: self.active_turn,
                        expectation_index: -1,
                        event_name: "transport_cleanup".into(),
                        reason:
                            "transport graceful cleanup exceeded deadline; cancelling and joining"
                                .into(),
                        kind: "transport_cleanup_error".into(),
                    });
                    if let Err(error) = socket.cancel_and_join().await {
                        self.result.fail(AssertionFailure {
                            turn_index: self.active_turn,
                            expectation_index: -1,
                            event_name: "transport_cleanup".into(),
                            reason: format!("transport cancellation cleanup: {error}"),
                            kind: "transport_cleanup_error".into(),
                        });
                    }
                }
            }
        }
        self.socket.take();
        if let Some(owner) = &mut self.user_speech {
            if let Err(error) = owner.finish().await {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "user_speech_cleanup".into(),
                    reason: error.to_string(),
                    kind: "user_speech_cleanup_error".into(),
                });
            }
            self.result.debug_log.extend(owner.take_cache_diagnostics());
        }
        if let Some(engine) = &mut self.transcription {
            if let Err(error) = engine.finish() {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "response".into(),
                    reason: error.to_string(),
                    kind: "audio_cleanup_error".into(),
                });
            }
        }
        self.transcription.take();
    }
    #[cfg(feature = "user-speech")]
    async fn shutdown_hosted_speech(&mut self) {
        // No await or native join before every input producer is stopped.
        self.hosted_speech.as_mut().unwrap().admission_closed = true;
        if let Some(owner) = &mut self.user_speech {
            owner.cancel_work();
        }
        if let Some(playback) = &self.playback {
            playback.request_stop();
        }
        // This is the last possible application send. A cancelled cleanup wait
        // may resume after borrowed socket shutdown took its sender.
        let state = self.hosted_speech.as_mut().unwrap();
        let send_stop = self.options.stop_bot && !state.stop_bot_attempted && self.socket.is_some();
        state.stop_bot_attempted = true;
        if send_stop {
            if let Err(error) = self.send_control(serializer::EVAL_CANCEL, json!({})).await {
                self.result
                    .debug_log
                    .push(format!("eval-cancel could not be sent: {error}"));
            }
        }
        // Phase B keeps every owner in its field across cancellable awaits.
        if let Some(playback) = &mut self.playback {
            if let Err(error) = playback.finish().await {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "recording".into(),
                    reason: error.to_string(),
                    kind: "audio_cleanup_error".into(),
                });
            }
        }
        self.playback.take();
        if let Some(socket) = &mut self.socket {
            match tokio::time::timeout(
                self.options.transport.close_timeout + self.options.transport.write_timeout,
                socket.finish_and_join(),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "transport_cleanup".into(),
                    reason: format!("transport cleanup: {error}"),
                    kind: "transport_cleanup_error".into(),
                }),
                Err(_) => {
                    self.result.fail(AssertionFailure {
                        turn_index: self.active_turn,
                        expectation_index: -1,
                        event_name: "transport_cleanup".into(),
                        reason:
                            "transport graceful cleanup exceeded deadline; cancelling and joining"
                                .into(),
                        kind: "transport_cleanup_error".into(),
                    });
                    if let Err(error) = socket.cancel_and_join().await {
                        self.result.fail(AssertionFailure {
                            turn_index: self.active_turn,
                            expectation_index: -1,
                            event_name: "transport_cleanup".into(),
                            reason: format!("transport cancellation cleanup: {error}"),
                            kind: "transport_cleanup_error".into(),
                        });
                    }
                }
            }
        }
        self.socket.take();
        if let Some(owner) = &mut self.user_speech {
            if let Err(error) = owner.finish().await {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "user_speech_cleanup".into(),
                    reason: error.to_string(),
                    kind: "user_speech_cleanup_error".into(),
                });
            }
            self.result.debug_log.extend(owner.take_cache_diagnostics());
        }
        self.user_speech.take();
    }
    fn close_timeline_recording(&mut self) {
        let Some(state) = &mut self.timeline else {
            return;
        };
        if state.closed {
            return;
        }
        state.closed = true;
        let closed = state
            .recorder
            .elapsed_ns()
            .and_then(|cutoff| state.recorder.lock()?.close(cutoff));
        if let Some(playback) = &self.playback {
            playback.request_stop();
        }
        #[cfg(feature = "user-speech")]
        if let Some(owner) = &mut self.user_speech {
            owner.cancel_work();
        }
        if let Err(error) = closed {
            self.result.fail(AssertionFailure {
                turn_index: self.active_turn,
                expectation_index: -1,
                event_name: "recording".into(),
                reason: error.to_string(),
                kind: "audio_recording_error".into(),
            });
        }
    }
    async fn shutdown_timeline_recording(&mut self) {
        // Freeze once and signal every existing producer before the first await.
        // Closing recording admission intentionally does not close receipt writes.
        self.close_timeline_recording();
        #[cfg(feature = "user-speech")]
        if let Some(owner) = &mut self.user_speech {
            owner.cancel_work();
        }
        if let Some(playback) = &self.playback {
            playback.request_stop();
        }
        let state = self.timeline.as_mut().expect("selected timeline recording");
        let send_stop = self.options.stop_bot && !state.stop_bot_attempted && self.socket.is_some();
        state.stop_bot_attempted = true;
        if send_stop {
            if let Err(error) = self.send_control(serializer::EVAL_CANCEL, json!({})).await {
                self.result
                    .debug_log
                    .push(format!("eval-cancel could not be sent: {error}"));
            }
        }
        // Keep handles in their fields across cancellation, as in the existing
        // joined hosted/joint paths. A stop/abort request is never a join receipt.
        if let Some(playback) = &mut self.playback {
            if let Err(error) = playback.finish().await {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "recording".into(),
                    reason: error.to_string(),
                    kind: "audio_cleanup_error".into(),
                });
            }
        }
        self.playback.take();
        if let Some(socket) = &mut self.socket {
            match tokio::time::timeout(
                self.options.transport.close_timeout + self.options.transport.write_timeout,
                socket.finish_and_join(),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "transport_cleanup".into(),
                    reason: format!("transport cleanup: {error}"),
                    kind: "transport_cleanup_error".into(),
                }),
                Err(_) => {
                    self.result.fail(AssertionFailure {
                        turn_index: self.active_turn,
                        expectation_index: -1,
                        event_name: "transport_cleanup".into(),
                        reason:
                            "transport graceful cleanup exceeded deadline; cancelling and joining"
                                .into(),
                        kind: "transport_cleanup_error".into(),
                    });
                    if let Err(error) = socket.cancel_and_join().await {
                        self.result.fail(AssertionFailure {
                            turn_index: self.active_turn,
                            expectation_index: -1,
                            event_name: "transport_cleanup".into(),
                            reason: format!("transport cancellation cleanup: {error}"),
                            kind: "transport_cleanup_error".into(),
                        });
                    }
                }
            }
        }
        self.socket.take();
        #[cfg(feature = "user-speech")]
        if let Some(owner) = &mut self.user_speech {
            if let Err(error) = owner.finish().await {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "user_speech_cleanup".into(),
                    reason: error.to_string(),
                    kind: "user_speech_cleanup_error".into(),
                });
            }
            self.result.debug_log.extend(owner.take_cache_diagnostics());
        }
        #[cfg(feature = "user-speech")]
        self.user_speech.take();
    }
    pub(crate) async fn shutdown(&mut self) {
        // Joint cleanup owns transcription as well as Playback and speech.
        // Its stop-once state remains authoritative when recording is enabled.
        #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
        if self.is_audio_to_audio() {
            self.shutdown_audio_to_audio().await;
            return;
        }
        if self.timeline.is_some() {
            self.shutdown_timeline_recording().await;
            return;
        }
        #[cfg(feature = "user-speech")]
        if self.hosted_speech.is_some() {
            self.shutdown_hosted_speech().await;
            return;
        }
        #[cfg(feature = "user-speech")]
        if let Some(owner) = &mut self.user_speech {
            owner.cancel_work();
        }
        #[cfg(feature = "bot-audio-transcription")]
        {
            self.pending_recognition = None;
        }
        #[cfg(feature = "bot-audio-transcription")]
        if let Some(mut engine) = self.transcription.take() {
            if let Err(error) = engine.finish() {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "response".into(),
                    reason: error.to_string(),
                    kind: "audio_cleanup_error".into(),
                });
            }
        }
        if let Some(playback) = &mut self.playback {
            if let Err(error) = playback.finish().await {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "recording".into(),
                    reason: error.to_string(),
                    kind: "audio_error".into(),
                });
            }
        }
        self.playback.take();
        #[cfg(feature = "user-speech")]
        if let Some(owner) = &mut self.user_speech {
            if let Err(error) = owner.finish().await {
                self.result.fail(AssertionFailure {
                    turn_index: self.active_turn,
                    expectation_index: -1,
                    event_name: "user_speech_cleanup".into(),
                    reason: error.to_string(),
                    kind: "user_speech_cleanup_error".into(),
                });
            }
            self.result.debug_log.extend(owner.take_cache_diagnostics());
        }
        if self.socket.is_none() {
            return;
        }
        if self.options.stop_bot {
            if let Err(error) = self.send_control(serializer::EVAL_CANCEL, json!({})).await {
                self.result
                    .debug_log
                    .push(format!("eval-cancel could not be sent: {error}"));
            }
        }
        if let Some(socket) = self.socket.take() {
            // Finish drains admitted control messages; dropping a timed-out finish
            // aborts and drops the transport owner rather than detaching its task.
            match tokio::time::timeout(
                self.options.transport.close_timeout + self.options.transport.write_timeout,
                socket.finish(),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => self
                    .result
                    .debug_log
                    .push(format!("transport cleanup: {error}")),
                Err(_) => self
                    .result
                    .debug_log
                    .push("transport cleanup exceeded deadline; connection aborted".into()),
            }
        }
    }
}
/// Decode only after bounded metadata checks, under the same admission lock.
/// The old original-rate capture route has its own unchanged decoder/limits.
fn ingest_timeline_bot(recorder: &TimelineRecorder, data: &Value) -> Result<(), EvalError> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let encoded = data
        .get("audio")
        .and_then(Value::as_str)
        .ok_or_else(|| EvalError::Scenario("bot audio requires base64 audio".into()))?;
    let rate = data
        .get("sampleRate")
        .and_then(Value::as_u64)
        .filter(|rate| matches!(*rate, 8_000 | 16_000 | 24_000 | 48_000))
        .ok_or_else(|| {
            EvalError::Scenario("bot audio sampleRate must be 8000, 16000, 24000 or 48000".into())
        })? as u32;
    // The host's existing wire contract omits channels for mono. Explicit
    // channel metadata cannot silently reinterpret stereo or invalid values.
    for key in ["numChannels", "channels"] {
        if data
            .get(key)
            .is_some_and(|channels| channels.as_u64() != Some(1))
        {
            return Err(EvalError::Unsupported(
                "timeline bot audio requires mono PCM16".into(),
            ));
        }
    }
    let mut capture = recorder.lock()?;
    if capture.remaining_records() == 0 {
        return Err(EvalError::Limit("timeline recording records"));
    }
    if encoded.is_empty() || !encoded.len().is_multiple_of(4) {
        return Err(EvalError::Scenario("invalid bot audio base64".into()));
    }
    let padding = if encoded.ends_with("==") {
        2
    } else {
        usize::from(encoded.ends_with('='))
    };
    let decoded_len = encoded
        .len()
        .checked_div(4)
        .and_then(|groups| groups.checked_mul(3))
        .and_then(|bytes| bytes.checked_sub(padding))
        .ok_or(EvalError::Limit("timeline recording raw bytes"))?;
    if decoded_len > capture.remaining_raw_bytes() {
        return Err(EvalError::Limit("timeline recording raw bytes"));
    }
    if decoded_len == 0 || !decoded_len.is_multiple_of(2) {
        return Err(EvalError::Scenario(
            "bot audio requires complete PCM16 samples".into(),
        ));
    }
    // Exact-sized decoding avoids base64's rounded-up capacity crossing the
    // fixed raw limit by up to two spare bytes at its boundary.
    let mut pcm = vec![0; decoded_len];
    let decoded = STANDARD
        .decode_slice(encoded, &mut pcm)
        .map_err(|_| EvalError::Scenario("invalid bot audio base64".into()))?;
    if decoded != decoded_len {
        return Err(EvalError::Scenario(
            "invalid bot audio decoded length".into(),
        ));
    }
    capture.ingest_bot(recorder.elapsed_ns()?, pcm, rate, 1)
}

fn validate_recording_inputs(
    scenario: &ScriptScenario,
    inputs: impl IntoIterator<Item = (usize, RecordedAudio)>,
) -> Result<BTreeMap<usize, RecordedAudio>, EvalError> {
    let mut recordings = BTreeMap::new();
    let mut total = 0usize;
    for (index, clip) in inputs {
        let turn = scenario
            .turns
            .get(index)
            .ok_or_else(|| EvalError::Scenario("recording turn index out of range".into()))?;
        if turn.user.as_ref().is_none_or(|text| text.trim().is_empty()) || turn.dtmf.is_some() {
            return Err(EvalError::Scenario(
                "recording needs a nonempty user reference and no DTMF".into(),
            ));
        }
        total = total
            .checked_add(clip.pcm_bytes().len())
            .ok_or(EvalError::Limit("recording bytes"))?;
        if total > MAX_TOTAL_BYTES || recordings.len() >= 128 {
            return Err(EvalError::Limit("recording inputs"));
        }
        if recordings.insert(index, clip).is_some() {
            return Err(EvalError::Scenario("duplicate recording turn index".into()));
        }
    }
    Ok(recordings)
}

fn judge_no() -> MatchOutcome {
    MatchOutcome::Failed {
        kind: "judge_no".into(),
        reason: "classifier judged that the criterion was not satisfied".into(),
    }
}
fn judge_interrupted() -> MatchOutcome {
    MatchOutcome::Failed {
        kind: "judge_interrupted".into(),
        reason: "reply was interrupted before its judgment could be accepted".into(),
    }
}
fn connect_url(raw: &str, scenario: &ScriptScenario) -> Result<String, EvalError> {
    let mut url = url::Url::parse(raw).map_err(|e| EvalError::Scenario(format!("bot URL: {e}")))?;
    if !matches!(url.scheme(), "ws" | "wss") || url.host_str().is_none() || url.fragment().is_some()
    {
        return Err(EvalError::Scenario(
            "bot URL must be ws:// or wss:// without a fragment".into(),
        ));
    }
    let pairs: Vec<_> = url
        .query_pairs()
        .filter(|(key, _)| {
            !matches!(
                key.as_ref(),
                "skip_tts" | "capture_bot_audio" | "capture_bot_images" | "trigger_disconnect"
            )
        })
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (key, value) in pairs {
            query.append_pair(&key, &value);
        }
        query.append_pair("skip_tts", "true");
        if scenario.needs_images() {
            query.append_pair("capture_bot_images", "true");
        }
        if scenario.trigger_disconnect {
            query.append_pair("trigger_disconnect", "true");
        }
    }
    Ok(url.into())
}

#[cfg(test)]
mod readiness_tests {
    use super::*;

    #[tokio::test]
    async fn internally_drained_ready_survives_discarded_ingest_return() {
        let scenario = crate::ScenarioFile::from_value(json!({"name":"ready", "turns":[]}))
            .unwrap()
            .scenarios
            .remove(0);
        let mut session =
            EvalSession::new(scenario, "ws://127.0.0.1:1", SessionOptions::default()).unwrap();
        session.phase = "handshake";
        assert!(!session.bot_ready_received);
        // Exactly the old drain behavior: consume the packet but discard its
        // bool. No new socket packet exists to rescue the handshake afterward.
        let _ = session
            .ingest(Packet::Text(
                json!({"label":"rtvi-ai", "type":"bot-ready", "data":{"version":"2.2.0"}})
                    .to_string(),
            ))
            .unwrap();
        assert!(session
            .pump(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap());
        assert!(
            session.socket.is_none(),
            "the remembered fact must not need another receive"
        );
        assert!(matches!(
            session.pump(Instant::now()).await,
            Err(EvalError::Timeout("event"))
        ));
        // A pre-cancelled run still resets transient readiness before selecting
        // cancellation and never attempts to connect.
        let cancel = Cancellation::new();
        cancel.cancel();
        session.run_inner(cancel).await;
        assert!(!session.bot_ready_received);
        assert!(!session.result.passed);
        assert!(session.socket.is_none());
    }
}

#[cfg(all(test, feature = "bot-audio-transcription"))]
mod recognition_ownership_tests {
    use super::*;
    use std::{future::Future, sync::Arc, task::Poll};
    use tokio::sync::{oneshot, Notify};

    const WAIT: Duration = Duration::from_secs(2);

    fn session(limits: EventLimits) -> EvalSession {
        let scenario = crate::ScenarioFile::from_value(json!({
            "name": "recognition ownership", "turns": []
        }))
        .unwrap()
        .scenarios
        .remove(0);
        EvalSession::new(
            scenario,
            "ws://127.0.0.1:1",
            SessionOptions {
                event_limits: limits,
                ..Default::default()
            },
        )
        .unwrap()
    }

    pub(super) async fn socket_pair() -> (WebSocketSession, WebSocketSession) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let config = SessionConfig {
            connect_timeout: WAIT,
            write_timeout: Duration::from_millis(250),
            close_timeout: Duration::from_millis(250),
            ..Default::default()
        };
        // Keep both owners here instead of spawning a peer that an assertion
        // could strand. The listener wait is bounded even if connect fails.
        let accept = async {
            let (stream, _) = tokio::time::timeout(WAIT, listener.accept())
                .await
                .map_err(|error| error.to_string())?
                .map_err(|error| error.to_string())?;
            WebSocketSession::accept(stream, config.clone())
                .await
                .map_err(|error| error.to_string())
        };
        let connect = async {
            WebSocketSession::connect(&url, config.clone())
                .await
                .map_err(|error| error.to_string())
        };
        let (client, peer) = tokio::join!(connect, accept);
        match (client, peer) {
            (Ok(client), Ok(peer)) => (client, peer),
            (client, peer) => {
                let errors = (client.as_ref().err().cloned(), peer.as_ref().err().cloned());
                // A partially established connection still owns its transport
                // task. Join it before reporting setup failure.
                tokio::join!(
                    async {
                        if let Ok(client) = client {
                            let _ = client.cancel().await;
                        }
                    },
                    async {
                        if let Ok(peer) = peer {
                            let _ = peer.cancel().await;
                        }
                    }
                );
                panic!("loopback socket setup failed: {errors:?}");
            }
        }
    }

    #[tokio::test]
    async fn cancelled_recognition_drain_keeps_owned_result_and_resumes_once() {
        let mut session = session(EventLimits::default());
        let (client, peer) = socket_pair().await;
        session.socket = Some(client);
        // Seed the ownership state immediately after engine.next() has popped
        // a result. This exercises cancellation of the publication fence, not
        // a particular native STT scheduling order.
        let recognition = (session.stream.epoch(), "owned spoken reply".to_owned());
        session.pending_recognition = Some(recognition.clone());
        let (entered_tx, mut entered_rx) = oneshot::channel();
        let release = Arc::new(Notify::new());
        session.recognition_drain_gate = Some(RecognitionDrainGate {
            entered: Some(entered_tx),
            release: release.clone(),
        });

        let first_poll = {
            let mut pump = Box::pin(session.pump(Instant::now() + WAIT));
            // Exactly one poll reaches the gate and must be Pending. Dropping
            // this future models the competing judge select branch winning.
            std::future::poll_fn(|cx| Poll::Ready(pump.as_mut().poll(cx))).await
        };
        let entered = entered_rx.try_recv();
        let retained_after_cancel = session.pending_recognition.clone();
        let events_before_resume = session.stream.events_seen.len();

        // Always release before any behavioral assertion, including if the
        // first poll unexpectedly completed. Real transport drain follows.
        release.notify_one();
        let resumed = tokio::time::timeout(WAIT, session.pump(Instant::now() + WAIT)).await;
        let retained_after_resume = session.pending_recognition.clone();
        let repeated_publication = session.publish_pending_recognition();
        let events = session.stream.events_seen.clone();
        let queued = session.stream.pop();
        let duplicate = session.stream.pop();

        let (client_closed, peer_closed) = tokio::join!(
            async {
                match session.socket.take() {
                    Some(client) => Some(client.cancel().await),
                    None => None,
                }
            },
            peer.cancel()
        );
        // Both transport tasks have actually joined before checking results.
        assert!(matches!(client_closed, Some(Ok(()))));
        assert!(peer_closed.is_ok());
        assert!(first_poll.is_pending());
        assert!(entered.is_ok());
        assert_eq!(retained_after_cancel, Some(recognition));
        assert_eq!(events_before_resume, 0);
        assert!(matches!(resumed, Ok(Ok(false))));
        assert!(retained_after_resume.is_none());
        assert!(repeated_publication.is_ok());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["type"], "response");
        assert_eq!(events[0]["text"], "owned spoken reply");
        assert_eq!(queued.as_ref(), events.first());
        assert!(duplicate.is_none());
    }

    #[test]
    fn stale_recognition_is_dropped_without_blocking_current_epoch() {
        let mut session = session(EventLimits::default());
        session.pending_recognition = Some((session.stream.epoch(), "stale".into()));
        session.stream.drop_pending_bot_output();
        let stale = session.publish_pending_recognition();
        let stale_retained = session.pending_recognition.clone();
        let stale_events = session.stream.events_seen.len();
        session.pending_recognition = Some((session.stream.epoch(), "current".into()));
        let current = session.publish_pending_recognition();
        let repeated = session.publish_pending_recognition();

        assert!(stale.is_ok());
        assert!(stale_retained.is_none());
        assert_eq!(stale_events, 0);
        assert!(current.is_ok());
        assert!(repeated.is_ok());
        assert!(session.pending_recognition.is_none());
        assert_eq!(session.stream.events_seen.len(), 1);
        assert_eq!(session.stream.events_seen[0]["text"], "current");
    }

    #[test]
    fn event_budget_failure_retains_recognition_without_partial_append() {
        let mut session = session(EventLimits {
            max_events: 1,
            ..Default::default()
        });
        session.pending_recognition = Some((session.stream.epoch(), "first".into()));
        let first = session.publish_pending_recognition();
        let admitted = session.stream.events_seen.clone();
        let pending = (session.stream.epoch(), "retained".to_owned());
        session.pending_recognition = Some(pending.clone());
        let failed = session.publish_pending_recognition();
        let failed_again = session.publish_pending_recognition();
        let queued = session.stream.pop();
        let duplicate = session.stream.pop();

        assert!(first.is_ok());
        assert!(matches!(failed, Err(EvalError::Limit("event history"))));
        assert!(matches!(
            failed_again,
            Err(EvalError::Limit("event history"))
        ));
        assert_eq!(session.pending_recognition, Some(pending));
        assert_eq!(session.stream.events_seen, admitted);
        assert_eq!(admitted.len(), 1);
        assert_eq!(queued.as_ref(), admitted.first());
        assert!(duplicate.is_none());
    }

    #[test]
    fn turn_boundary_clears_owned_recognition() {
        let mut session = session(EventLimits::default());
        let epoch = session.stream.epoch();
        session.pending_recognition = Some((epoch, "previous turn".into()));
        let boundary = session.turn_boundary();
        let pending_after_boundary = session.pending_recognition.clone();
        let published = session.publish_pending_recognition();

        assert!(boundary.is_ok());
        assert_eq!(session.stream.epoch(), epoch + 1);
        assert!(pending_after_boundary.is_none());
        assert!(session.pending_recognition.is_none());
        assert!(published.is_ok());
        assert!(session.stream.events_seen.is_empty());
    }

    #[tokio::test]
    async fn shutdown_clears_owned_recognition_without_publication() {
        let mut session = session(EventLimits::default());
        session.pending_recognition = Some((session.stream.epoch(), "unfinished".into()));
        session.shutdown().await;

        assert!(session.pending_recognition.is_none());
        assert!(session.stream.events_seen.is_empty());
        assert!(session.socket.is_none());
        assert!(session.transcription.is_none());
    }
}

#[cfg(all(test, feature = "user-speech"))]
mod speech_builder_tests {
    use super::*;
    use crate::{ScenarioFile, UserSpeechCachePolicy};
    use pipecat_speech::SpeechConfig;

    fn session() -> EvalSession {
        let scenario = ScenarioFile::from_value(
            json!({"name":"speech budget", "turns":[{"user":"reference"}]}),
        )
        .unwrap()
        .scenarios
        .remove(0);
        EvalSession::new(scenario, "ws://127.0.0.1:1", SessionOptions::default()).unwrap()
    }
    fn settings() -> CartesiaUserSpeechSettings {
        let mut config = SpeechConfig::cartesia("synthetic-test-key");
        config.base_url = "http://127.0.0.1:1".into();
        CartesiaUserSpeechSettings::new(config, UserSpeechCachePolicy::Disabled).unwrap()
    }
    fn voice() -> CartesiaUserSpeechVoice {
        CartesiaUserSpeechVoice::new("voice", None, None).unwrap()
    }
    #[test]
    fn recorded_overrides_survive_both_public_builder_orders() {
        let clip = RecordedAudio::from_pcm16(&[1, 0, 2, 0]).unwrap();
        let first = session()
            .with_recorded_audio_inputs([(0, clip.clone())])
            .unwrap()
            .with_cartesia_user_speech(settings(), voice())
            .unwrap();
        let second = session()
            .with_cartesia_user_speech(settings(), voice())
            .unwrap()
            .with_recorded_audio_inputs([(0, clip)])
            .unwrap();
        assert_eq!(
            first.recordings[&0].pcm_bytes(),
            second.recordings[&0].pcm_bytes()
        );
        assert!(first.user_speech.is_some() && second.user_speech.is_some());
    }
    #[test]
    fn loader_precharged_plan_is_frozen_even_without_a_speech_owner() {
        let clip = RecordedAudio::from_pcm16(&[1, 0]).unwrap();
        let loaded = session()
            .with_recorded_audio_inputs([(0, clip.clone())])
            .unwrap()
            .with_user_speech_budget(UserSpeechBudget::new(2).unwrap());
        assert!(loaded.user_speech.is_none());
        assert!(loaded
            .with_recorded_audio_inputs([(0, clip.clone())])
            .is_err());
        let replaced = session()
            .with_recorded_audio_inputs([(0, clip.clone())])
            .unwrap()
            .with_user_speech_budget(UserSpeechBudget::new(2).unwrap())
            .with_cartesia_user_speech(settings(), voice())
            .unwrap();
        assert!(replaced.user_speech_budget.is_some());
        assert!(replaced.with_recorded_audio_inputs([(0, clip)]).is_err());
    }
}

#[cfg(all(test, feature = "user-speech"))]
mod hosted_speech_lifecycle_tests {
    use super::*;
    use pipecat_speech::SpeechConfig;
    use std::sync::mpsc;
    use tokio::sync::oneshot;

    const WAIT: Duration = Duration::from_secs(3);

    fn settings() -> CartesiaUserSpeechSettings {
        let mut config = SpeechConfig::cartesia("synthetic-hosted-cleanup-key");
        config.base_url = "http://127.0.0.1:1".into();
        CartesiaUserSpeechSettings::new(config, crate::UserSpeechCachePolicy::Disabled).unwrap()
    }
    fn session() -> EvalSession {
        let scenario = crate::ScenarioFile::from_value(json!({
            "name": "hosted lifecycle", "turns": [{"user": "WAV reference"}]
        }))
        .unwrap()
        .scenarios
        .remove(0);
        EvalSession::new(scenario, "ws://127.0.0.1:1", SessionOptions::default())
            .unwrap()
            .with_recorded_audio_inputs([(0, RecordedAudio::from_pcm16(&[1, 0]).unwrap())])
            .unwrap()
            .with_hosted_user_speech(PreparedHostedSpeech {
                settings: settings(),
                voice: Some(CartesiaUserSpeechVoice::new("unused-voice", None, None).unwrap()),
                budget: UserSpeechBudget::new(2).unwrap(),
            })
            .unwrap()
    }
    fn primary(session: &mut EvalSession) {
        session.result.fail(AssertionFailure {
            turn_index: -1,
            expectation_index: -1,
            event_name: "original".into(),
            kind: "original_error".into(),
            reason: "primary failure".into(),
        });
    }
    #[test]
    fn all_wav_is_selected_frozen_and_has_no_http_or_cache_owner() {
        let session = session();
        assert!(session.hosted_speech.is_some());
        assert!(session.user_speech_budget.is_some());
        assert!(session.user_speech.is_none());
        assert!(session
            .with_recorded_audio_inputs([(0, RecordedAudio::from_pcm16(&[2, 0]).unwrap())])
            .is_err());
    }
    #[tokio::test]
    async fn selected_pre_run_failure_keeps_primary_and_closes_admission() {
        let result = session()
            .stop_hosted_before_run("process_wait", "original wait failure")
            .await;
        assert!(!result.passed);
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.failures[0].kind, "process_wait");
        assert_eq!(result.failures[0].reason, "original wait failure");
        let mut session = session();
        session.shutdown().await;
        session.shutdown().await;
        assert!(session.hosted_speech.as_ref().unwrap().admission_closed);
        assert!(session.hosted_speech.as_ref().unwrap().stop_bot_attempted);
        assert!(matches!(
            session.check_user_speech_deadline(),
            Err(EvalError::Cancelled)
        ));
    }
    #[tokio::test]
    async fn actual_socket_error_is_joined_and_appended_after_primary() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let accept = async {
            let (stream, _) = tokio::time::timeout(WAIT, listener.accept())
                .await
                .unwrap()
                .unwrap();
            WebSocketSession::accept(stream, SessionConfig::default())
                .await
                .unwrap()
        };
        let connect = WebSocketSession::connect(
            &url,
            SessionConfig {
                max_message_bytes: 4,
                ..Default::default()
            },
        );
        let (client, mut peer) = tokio::join!(connect, accept);
        let mut session = session();
        session.socket = Some(client.unwrap());
        peer.sender()
            .send(Packet::Text("12345".into()))
            .await
            .unwrap();
        let disconnected =
            tokio::time::timeout(WAIT, session.socket.as_mut().unwrap().recv()).await;
        let peer_disconnected = tokio::time::timeout(WAIT, peer.recv()).await;
        primary(&mut session);
        let (_, peer_joined) = tokio::join!(session.shutdown(), peer.cancel_and_join());
        assert!(matches!(disconnected, Ok(None)));
        assert!(matches!(peer_disconnected, Ok(None)));
        assert!(
            matches!(&peer_joined,
                Err(pipecat_transports::TransportError::WebSocket(inner))
                    if format!("{inner:?}") == "Protocol(ResetWithoutClosingHandshake)"
            ),
            "actual peer terminal result: {peer_joined:?}"
        );
        assert!(session.socket.is_none());
        assert_eq!(session.result.failures.len(), 2);
        assert_eq!(session.result.failures[0].kind, "original_error");
        assert_eq!(session.result.failures[1].kind, "transport_cleanup_error");
        assert!(!session.result.passed);
    }

    /// The real peer actor's runtime is paused after its handshake. A small TCP
    /// receive window and bounded large frame hold the real client write/join,
    /// rather than using completion notification as a substitute for ownership.
    struct HeldPeer {
        release: Option<mpsc::Sender<()>>,
        thread: Option<std::thread::JoinHandle<Result<(), String>>>,
    }
    impl HeldPeer {
        fn finish(&mut self) -> Result<(), String> {
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
            match self.thread.take() {
                Some(thread) => thread
                    .join()
                    .map_err(|_| "peer thread panicked".to_owned())?,
                None => Ok(()),
            }
        }
    }
    impl Drop for HeldPeer {
        fn drop(&mut self) {
            let _ = self.finish();
        }
    }
    async fn held_socket() -> (WebSocketSession, HeldPeer) {
        let (address_tx, address_rx) = oneshot::channel();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (release, held) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            let mut peer = runtime.block_on(async {
                let socket = tokio::net::TcpSocket::new_v4().map_err(|error| error.to_string())?;
                socket
                    .set_recv_buffer_size(1024)
                    .map_err(|error| error.to_string())?;
                socket
                    .bind("127.0.0.1:0".parse().unwrap())
                    .map_err(|error| error.to_string())?;
                let listener = socket.listen(1).map_err(|error| error.to_string())?;
                let _ = address_tx.send(listener.local_addr().map_err(|error| error.to_string())?);
                let (stream, _) = tokio::time::timeout(WAIT, listener.accept())
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())?;
                WebSocketSession::accept(
                    stream,
                    SessionConfig {
                        max_message_bytes: 4 * 1024 * 1024,
                        write_timeout: WAIT,
                        close_timeout: Duration::from_millis(50),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| error.to_string())
            })?;
            let _ = accepted_tx.send(());
            let _ = held.recv_timeout(WAIT + WAIT);
            // Both success and protocol failure are terminal here: the client
            // deliberately interrupts a real partially written frame. Actual
            // cancel_and_join is awaited regardless; the peer is never detached.
            let _terminal = runtime.block_on(peer.cancel_and_join());
            Ok(())
        });
        let peer = HeldPeer {
            release: Some(release),
            thread: Some(thread),
        };
        let address = tokio::time::timeout(WAIT, address_rx)
            .await
            .unwrap()
            .unwrap();
        let client = WebSocketSession::connect(
            &format!("ws://{address}"),
            SessionConfig {
                queue_capacity: 1,
                max_message_bytes: 4 * 1024 * 1024,
                write_timeout: WAIT,
                close_timeout: Duration::from_millis(50),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        tokio::time::timeout(WAIT, accepted_rx)
            .await
            .unwrap()
            .unwrap();
        (client, peer)
    }
    #[tokio::test]
    async fn borrowed_shutdown_retains_real_socket_and_timeout_cancels_then_joins() {
        let (socket, mut peer) = held_socket().await;
        let sender = socket.sender();
        sender
            .send(Packet::Binary(vec![1; 4 * 1024 * 1024].into()))
            .await
            .unwrap();
        sender
            .send(Packet::Binary(vec![2; 4 * 1024 * 1024].into()))
            .await
            .unwrap();
        let mut session = session();
        session.options.transport.write_timeout = Duration::from_millis(250);
        session.options.transport.close_timeout = Duration::from_millis(250);
        session.socket = Some(socket);
        primary(&mut session);
        // Stop waiting on only this borrow, retaining the exact selected owner.
        let first = tokio::time::timeout(Duration::from_millis(10), session.shutdown()).await;
        let retained = session.socket.is_some();
        let stopped = session.hosted_speech.as_ref().unwrap().stop_bot_attempted;
        // Re-entry must not call sender() after the first finish removed it.
        session.options.stop_bot = true;
        session.options.transport.write_timeout = Duration::from_millis(1);
        session.options.transport.close_timeout = Duration::from_millis(1);
        session.shutdown().await;
        let peer_joined = peer.finish();
        assert!(
            first.is_err(),
            "real backpressured socket unexpectedly finished"
        );
        assert!(retained && stopped);
        assert!(session.socket.is_none() && session.playback.is_none());
        assert!(peer_joined.is_ok(), "peer actual join: {peer_joined:?}");
        assert_eq!(session.result.failures[0].kind, "original_error");
        assert!(session.result.failures.iter().any(|failure| {
            failure.kind == "transport_cleanup_error"
                && failure.reason.contains("cancelling and joining")
        }));
        assert!(!session.result.passed);
    }
    #[tokio::test]
    async fn timeline_cutoff_survives_cancelled_real_socket_join_and_reentry() {
        timeline_cutoff_reentry(false).await;
    }
    #[cfg(feature = "bot-audio-transcription")]
    #[tokio::test]
    async fn joint_timeline_cutoff_survives_cancelled_socket_join_and_reentry() {
        timeline_cutoff_reentry(true).await;
    }
    async fn timeline_cutoff_reentry(joint: bool) {
        let (socket, mut peer) = held_socket().await;
        let sender = socket.sender();
        let mut session = session();
        session.hosted_speech = None;
        #[cfg(feature = "bot-audio-transcription")]
        if joint {
            session.audio_to_audio = Some(AudioToAudioState {
                run: None,
                admission_open: true,
                stop_bot_attempted: false,
            });
            session.transcription = Some(BotTranscription::lifecycle_test_engine(
                false,
                None,
                None,
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ));
        }
        #[cfg(not(feature = "bot-audio-transcription"))]
        assert!(!joint);
        session.options.transport.write_timeout = Duration::from_millis(250);
        session.options.transport.close_timeout = Duration::from_millis(250);
        session.socket = Some(socket);
        let mut capture = TimelineCapture::new(TimelineRecordingLimits::default()).unwrap();
        capture.ingest_bot(0, vec![1; 32_000], 16_000, 1).unwrap();
        let origin = Instant::now() - Duration::from_millis(50);
        session.timeline = Some(TimelineRecordingState {
            recorder: TimelineRecorder {
                origin,
                capture: Arc::new(Mutex::new(capture)),
            },
            closed: false,
            stop_bot_attempted: false,
        });
        let recorder = session.timeline.as_ref().unwrap().recorder.clone();
        session.playback = Some(Playback::start_with_timeline(
            sender.clone(),
            &session.options,
            Some(recorder),
        ));
        let receipt = session
            .playback
            .as_ref()
            .unwrap()
            .queue(RecordedAudio::from_pcm16(&[2, 0].repeat(640)).unwrap())
            .unwrap();
        tokio::time::timeout(WAIT, receipt).await.unwrap().unwrap();
        let user_end_upper = origin.elapsed().as_nanos() + 40_000_000;
        sender
            .send(Packet::Binary(vec![1; 4 * 1024 * 1024].into()))
            .await
            .unwrap();
        sender
            .send(Packet::Binary(vec![2; 4 * 1024 * 1024].into()))
            .await
            .unwrap();
        primary(&mut session);
        let first = tokio::time::timeout(Duration::from_millis(10), session.shutdown()).await;
        let upper_cutoff = origin.elapsed().as_nanos();
        let retained = session.socket.is_some();
        let state = session.timeline.as_ref().unwrap();
        assert!(state.closed);
        if !joint {
            assert!(state.stop_bot_attempted);
        }
        #[cfg(feature = "bot-audio-transcription")]
        if joint {
            assert!(session.audio_to_audio.as_ref().unwrap().stop_bot_attempted);
            assert!(!session.audio_to_audio.as_ref().unwrap().admission_open);
            assert!(session.transcription.is_some());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        session.options.stop_bot = true;
        session.options.transport.write_timeout = Duration::from_millis(1);
        session.options.transport.close_timeout = Duration::from_millis(1);
        session.shutdown().await;
        let peer_joined = peer.finish();
        assert!(first.is_err() && retained);
        assert!(
            session.socket.is_none() && session.playback.is_none() && session.user_speech.is_none()
        );
        assert!(peer_joined.is_ok(), "peer actual join: {peer_joined:?}");
        #[cfg(feature = "bot-audio-transcription")]
        assert!(session.transcription.is_none());
        assert_eq!(session.result.failures[0].kind, "original_error");
        let state = session.timeline.take().unwrap();
        let run = Arc::try_unwrap(state.recorder.capture)
            .unwrap()
            .into_inner()
            .unwrap()
            .into_run(session.result);
        let wav = run.to_wav().unwrap().unwrap();
        let pcm = pipecat_audio::read_pcm16_wav(&wav).unwrap().pcm;
        let frames = pcm.len() / 2;
        // Bot is 257; admitted user is 2, producing either 2 or mixed 259.
        assert!(pcm
            .as_chunks::<2>()
            .0
            .iter()
            .any(|sample| { matches!(i16::from_le_bytes([sample[0], sample[1]]), 2 | 259) }));
        assert!(frames as u128 <= upper_cutoff.max(user_end_upper) * 16_000 / 1_000_000_000);
        assert!(!run.result.passed);
    }
}

#[cfg(all(test, feature = "user-speech", feature = "bot-audio-transcription"))]
mod audio_to_audio_lifecycle_tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use tokio::sync::{mpsc, oneshot};

    const WAIT: Duration = Duration::from_secs(2);

    fn session() -> EvalSession {
        let scenario =
            crate::ScenarioFile::from_value(json!({"name":"joint lifecycle", "turns":[]}))
                .unwrap()
                .scenarios
                .remove(0);
        let mut session =
            EvalSession::new(scenario, "ws://127.0.0.1:1", SessionOptions::default()).unwrap();
        session.audio_to_audio = Some(AudioToAudioState {
            run: Some((Cancellation::new(), Instant::now() + WAIT)),
            admission_open: true,
            stop_bot_attempted: false,
        });
        session.phase = "turn";
        session
    }
    fn queue(
        session: &mut EvalSession,
        packets: impl IntoIterator<Item = Packet>,
    ) -> mpsc::Sender<Packet> {
        let (tx, rx) = mpsc::channel(512);
        for packet in packets {
            tx.try_send(packet).unwrap();
        }
        session.fence_receiver = Some(rx);
        tx
    }
    fn ignored() -> Packet {
        Packet::Text("ignored but accounted".into())
    }
    fn interruption() -> Packet {
        Packet::Text(json!({"label":"rtvi-ai","type":"bot-interrupted","data":{}}).to_string())
    }

    #[tokio::test]
    async fn exact_fence_bounds_preserve_recognition_and_account_saturation_probe() {
        for count in [31usize, 32, 33, 255, 256, 257] {
            let mut session = session();
            let _tx = queue(&mut session, (0..count).map(|_| ignored()));
            session.pending_recognition = Some((0, "spoken truth".into()));
            let result = session.drain_recognition(Instant::now() + WAIT).await;
            assert_eq!(session.wire_messages, count);
            assert_eq!(session.fence_rounds, count.min(256) / 32);
            if count <= 256 {
                assert!(result.is_ok());
                assert!(session.pending_recognition.is_none());
                assert_eq!(session.stream.events_seen.len(), 1);
            } else {
                assert!(matches!(
                    result,
                    Err(EvalError::Limit("audio-to-audio receive drain"))
                ));
                assert!(session.pending_recognition.is_some());
                assert!(session.stream.events_seen.is_empty());
            }
        }
    }
    #[tokio::test]
    async fn last_permitted_interruption_fences_stale_recognition() {
        let mut session = session();
        let _tx = queue(
            &mut session,
            (0..255).map(|_| ignored()).chain([interruption()]),
        );
        session.pending_recognition = Some((0, "stale".into()));
        session
            .drain_recognition(Instant::now() + WAIT)
            .await
            .unwrap();
        assert_eq!(session.wire_messages, 256);
        assert!(session.pending_recognition.is_none());
        assert!(session
            .stream
            .events_seen
            .iter()
            .all(|event| event["type"] != "response"));
    }
    #[tokio::test]
    async fn round_yield_admits_new_interruption_before_empty_fence() {
        let mut session = session();
        let tx = queue(&mut session, (0..32).map(|_| ignored()));
        session.pending_recognition = Some((0, "stale".into()));
        let producer = tokio::spawn(async move {
            tx.send(interruption()).await.unwrap();
            tx
        });
        let result = session.drain_recognition(Instant::now() + WAIT).await;
        let _tx = producer.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(session.wire_messages, 33);
        assert!(session
            .stream
            .events_seen
            .iter()
            .all(|event| event["type"] != "response"));
    }
    #[tokio::test]
    async fn disconnected_or_cancelled_fence_never_publishes() {
        let mut disconnected = session();
        drop(queue(&mut disconnected, [ignored()]));
        disconnected.pending_recognition = Some((0, "retained".into()));
        assert!(matches!(
            disconnected.drain_recognition(Instant::now() + WAIT).await,
            Err(EvalError::Disconnected)
        ));
        assert!(disconnected.pending_recognition.is_some());
        assert!(disconnected.stream.events_seen.is_empty());

        let mut cancelled = session();
        let _tx = queue(&mut cancelled, (0..256).map(|_| ignored()));
        cancelled.pending_recognition = Some((0, "retained".into()));
        let cancel = cancelled
            .audio_to_audio
            .as_ref()
            .unwrap()
            .run
            .as_ref()
            .unwrap()
            .0
            .clone();
        let worker = tokio::spawn(async move {
            cancel.cancel();
        });
        let result = cancelled.drain_recognition(Instant::now() + WAIT).await;
        worker.await.unwrap();
        assert!(matches!(result, Err(EvalError::Cancelled)));
        assert_eq!(cancelled.wire_messages, 32);
        assert!(cancelled.pending_recognition.is_some());
        assert!(cancelled.stream.events_seen.is_empty());
    }
    #[tokio::test]
    async fn expired_caller_fence_and_probe_ingestion_error_keep_primary_error() {
        let mut expired = session();
        let _tx = queue(&mut expired, [ignored()]);
        expired.pending_recognition = Some((0, "retained".into()));
        assert!(matches!(
            expired.drain_recognition(Instant::now()).await,
            Err(EvalError::Timeout("event"))
        ));
        assert_eq!(expired.wire_messages, 0);
        assert!(expired.pending_recognition.is_some());

        let mut capped = session();
        capped.options.max_wire_messages = 256;
        let _tx = queue(&mut capped, (0..257).map(|_| ignored()));
        capped.pending_recognition = Some((0, "retained".into()));
        assert!(matches!(
            capped.drain_recognition(Instant::now() + WAIT).await,
            Err(EvalError::Limit("wire message traffic"))
        ));
        assert_eq!(capped.wire_messages, 257);
        assert!(capped.pending_recognition.is_some());
    }
    #[tokio::test]
    async fn all_wav_playback_uses_actual_completion_time_and_fixed_deadline() {
        let mut session = session();
        assert!(session.user_speech.is_none());
        let fixed = Instant::now() - Duration::from_millis(2);
        assert_eq!(session.recorded_playback_deadline(fixed), fixed);
        let (tx, rx) = oneshot::channel();
        tx.send(fixed - Duration::from_millis(1)).unwrap();
        assert!(
            session
                .settle_recorded_input(RecordedCompletion::Timed(rx), fixed)
                .await
                .is_ok(),
            "a timely actual admission remains timely when observed late"
        );
        let (tx, rx) = oneshot::channel();
        tx.send(fixed + Duration::from_millis(1)).unwrap();
        assert!(matches!(
            session
                .settle_recorded_input(RecordedCompletion::Timed(rx), fixed)
                .await,
            Err(EvalError::Timeout("recording playback"))
        ));
        let (_tx, rx) = oneshot::channel();
        assert!(matches!(
            session
                .settle_recorded_input(RecordedCompletion::Timed(rx), fixed)
                .await,
            Err(EvalError::Timeout("recording playback"))
        ));
    }
    #[tokio::test]
    async fn final_prefix_requires_local_idle_and_retained_publication() {
        let mut session = session();
        let _tx = queue(&mut session, []);
        let exited = Arc::new(AtomicBool::new(false));
        session.transcription = Some(BotTranscription::lifecycle_test_engine(
            false,
            None,
            None,
            exited.clone(),
        ));
        session.pending_recognition = Some((0, "final actual recognition".into()));
        let result = session.settle_audio_to_audio_prefix().await;
        session.shutdown().await;
        assert!(result.is_ok());
        assert!(session.pending_recognition.is_none());
        assert_eq!(
            session.stream.events_seen[0]["text"],
            "final actual recognition"
        );
        assert!(session.transcription.is_none());
        assert!(exited.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn final_prefix_busy_timeout_is_fixed_and_native_owner_is_joined() {
        let mut session = session();
        let (client, mut peer) = super::recognition_ownership_tests::socket_pair().await;
        session.socket = Some(client);
        let _tx = queue(&mut session, []);
        session.options.bot_quiet_timeout = Duration::from_millis(30);
        let exited = Arc::new(AtomicBool::new(false));
        session.transcription = Some(BotTranscription::lifecycle_test_engine(
            true,
            None,
            None,
            exited.clone(),
        ));
        let result = session.settle_audio_to_audio_prefix().await;
        let (_, peer_joined) = tokio::join!(session.shutdown(), peer.cancel_and_join());
        assert!(peer_joined.is_ok());
        assert!(matches!(
            result,
            Err(EvalError::Timeout("final bot transcription settlement"))
        ));
        assert!(exited.load(Ordering::Acquire));
        assert!(session.transcription.is_none() && session.socket.is_none());
    }
    #[tokio::test]
    async fn joint_stop_reaches_native_before_socket_and_playback_join_precede_native_wait() {
        joint_stop_with_recording(false).await;
    }
    #[tokio::test]
    async fn joint_recording_stop_reaches_native_and_freezes_before_held_native_join() {
        joint_stop_with_recording(true).await;
    }
    async fn joint_stop_with_recording(recording: bool) {
        let mut session = session();
        let origin = Instant::now() - Duration::from_millis(50);
        if recording {
            let mut capture = TimelineCapture::new(TimelineRecordingLimits::default()).unwrap();
            capture.ingest_bot(0, vec![1; 32_000], 16_000, 1).unwrap();
            session.timeline = Some(TimelineRecordingState {
                recorder: TimelineRecorder {
                    origin,
                    capture: Arc::new(Mutex::new(capture)),
                },
                closed: false,
                stop_bot_attempted: false,
            });
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let config = SessionConfig {
            connect_timeout: WAIT,
            write_timeout: Duration::from_millis(250),
            close_timeout: Duration::from_millis(250),
            ..Default::default()
        };
        let peer_config = config.clone();
        let (peer_closed_tx, peer_closed_rx) = std::sync::mpsc::channel();
        // The actual peer and its socket actor are created on an independent
        // runtime. Merely moving a peer created on this current-thread runtime
        // would leave its actor unable to poll during the native join below.
        let peer_thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener)
                    .map_err(|error| error.to_string())?;
                let (stream, _) = tokio::time::timeout(WAIT, listener.accept())
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(|error| error.to_string())?;
                let mut peer = WebSocketSession::accept(stream, peer_config)
                    .await
                    .map_err(|error| error.to_string())?;
                let eof_observed =
                    tokio::time::timeout(WAIT, async { while peer.recv().await.is_some() {} })
                        .await
                        .is_ok();
                let joined = peer.finish_and_join().await;
                if eof_observed && joined.is_ok() {
                    let _ = peer_closed_tx.send(());
                }
                Ok::<_, String>((eof_observed, joined))
            })
        });
        let client = match WebSocketSession::connect(&url, config).await {
            Ok(client) => client,
            Err(error) => {
                let peer_outcome = peer_thread.join();
                panic!("client setup failed: {error}; joined peer: {peer_outcome:?}");
            }
        };
        session.playback = Some(Playback::start(client.sender(), &session.options));
        session.socket = Some(client);
        let (stopped_tx, stopped_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let exited = Arc::new(AtomicBool::new(false));
        session.transcription = Some(BotTranscription::lifecycle_test_engine(
            false,
            Some(stopped_tx),
            Some(release_rx),
            exited.clone(),
        ));
        // The independent observer always releases the native gate, including
        // failure. A successful notice proves actual peer-owner retirement
        // before release; its observation time is not the local socket join's
        // linearization point.
        let observer = std::thread::spawn(move || {
            let stopped = stopped_rx.recv_timeout(WAIT).is_ok();
            let peer_joined = peer_closed_rx.recv_timeout(WAIT).is_ok();
            // Keep the actual native owner held well beyond the semantic cutoff.
            std::thread::sleep(Duration::from_millis(100));
            let _ = release_tx.send(());
            (stopped, peer_joined)
        });
        let cutoff_upper = origin.elapsed().as_nanos() + 10_000_000;
        session.shutdown().await;
        if recording {
            let state = session.timeline.take().unwrap();
            assert!(state.closed && !state.stop_bot_attempted);
            let run = Arc::try_unwrap(state.recorder.capture)
                .unwrap()
                .into_inner()
                .unwrap()
                .into_run(session.result.clone());
            let wav = run.to_wav().unwrap().unwrap();
            let pcm = pipecat_audio::read_pcm16_wav(&wav).unwrap().pcm;
            assert!(pcm.len() as u128 / 2 <= cutoff_upper * 16_000 / 1_000_000_000);
        }
        let peer_outcome = peer_thread.join();
        let observer_outcome = observer.join();
        let (stop_observed, peer_joined_before_native_release) = observer_outcome.unwrap();
        let (eof_observed, peer_joined) = peer_outcome.unwrap().unwrap();
        assert!(
            stop_observed && peer_joined_before_native_release,
            "stop observed: {stop_observed}; peer joined before native release: {peer_joined_before_native_release}; peer EOF: {eof_observed}; peer result: {peer_joined:?}"
        );
        assert!(eof_observed);
        assert!(peer_joined.is_ok(), "peer join: {peer_joined:?}");
        assert!(exited.load(Ordering::Acquire));
        assert!(
            session.playback.is_none()
                && session.socket.is_none()
                && session.transcription.is_none()
        );
    }
    #[tokio::test]
    async fn actual_transport_cleanup_error_appends_without_replacing_primary_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let accept = async {
            let (stream, _) = listener.accept().await.unwrap();
            WebSocketSession::accept(stream, SessionConfig::default())
                .await
                .unwrap()
        };
        let connect = WebSocketSession::connect(
            &url,
            SessionConfig {
                max_message_bytes: 4,
                ..Default::default()
            },
        );
        let (client, mut peer) = tokio::join!(connect, accept);
        let mut session = session();
        session.socket = Some(client.unwrap());
        peer.sender()
            .send(Packet::Text("12345".into()))
            .await
            .unwrap();
        let disconnected =
            tokio::time::timeout(WAIT, session.socket.as_mut().unwrap().recv()).await;
        // Observe the peer actor's natural terminal result before signalling
        // cancellation, so cancellation cannot race an abnormal remote EOF.
        // A failed/expired barrier still proceeds through both actual joins.
        let peer_disconnected = tokio::time::timeout(WAIT, peer.recv()).await;
        session.result.fail(AssertionFailure {
            turn_index: -1,
            expectation_index: -1,
            event_name: "original".into(),
            reason: "primary failure".into(),
            kind: "original_error".into(),
        });
        let (_, peer_joined) = tokio::join!(session.shutdown(), peer.cancel_and_join());
        // Rejecting the oversized frame closes the client transport without a
        // graceful Close exchange. Assert the observed exact peer protocol
        // error rather than accepting arbitrary WebSocket failures.
        assert!(
            matches!(
                &peer_joined,
                Err(pipecat_transports::TransportError::WebSocket(inner))
                    if format!("{inner:?}") == "Protocol(ResetWithoutClosingHandshake)"
            ),
            "peer join: {peer_joined:?}; peer terminal receive: {peer_disconnected:?}; selected failures: {:?}",
            session.result.failures
        );
        assert!(matches!(disconnected, Ok(None)));
        assert!(matches!(peer_disconnected, Ok(None)));
        assert!(session.socket.is_none());
        assert_eq!(session.result.failures.len(), 2);
        assert_eq!(session.result.failures[0].kind, "original_error");
        assert_eq!(session.result.failures[0].reason, "primary failure");
        assert_eq!(session.result.failures[1].kind, "transport_cleanup_error");
        assert_eq!(
            session.result.failures[1].reason,
            "transport cleanup: WebSocket: Space limit exceeded: Message too long: 5 > 4"
        );
    }

    #[test]
    fn combined_options_reject_expansion_without_changing_ordinary_options() {
        assert!(SessionOptions::default().validate_audio_to_audio().is_ok());
        let expansions: [fn(&mut SessionOptions); 10] = [
            |options| options.transport.queue_capacity += 1,
            |options| options.transport.max_message_bytes += 1,
            |options| options.max_wire_messages += 1,
            |options| options.max_wire_bytes += 1,
            |options| options.event_limits.max_events += 1,
            |options| options.event_limits.max_event_bytes += 1,
            |options| options.event_limits.max_text_bytes += 1,
            |options| options.max_duration += Duration::from_secs(1),
            |options| options.bot_quiet_timeout += Duration::from_secs(1),
            |options| options.transport.close_timeout += Duration::from_secs(1),
        ];
        for expand in expansions {
            let mut options = SessionOptions::default();
            expand(&mut options);
            assert!(options.validate().is_ok());
            assert!(options.validate_audio_to_audio().is_err());
        }
    }
}

#[cfg(test)]
mod timeline_session_tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine};

    fn session() -> EvalSession {
        let scenario = crate::ScenarioFile::from_value(json!({
            "name": "direct timeline", "turns": [{"user": "recorded reference"}]
        }))
        .unwrap()
        .scenarios
        .remove(0);
        EvalSession::new(scenario, "ws://127.0.0.1:1", SessionOptions::default()).unwrap()
    }
    fn recorded() -> EvalSession {
        session()
            .with_recorded_audio_inputs([(0, RecordedAudio::from_pcm16(&[7, 0]).unwrap())])
            .unwrap()
    }
    fn select(session: &mut EvalSession, limits: TimelineRecordingLimits) {
        session.timeline = Some(TimelineRecordingState {
            recorder: TimelineRecorder {
                origin: Instant::now(),
                capture: Arc::new(Mutex::new(TimelineCapture::new(limits).unwrap())),
            },
            closed: false,
            stop_bot_attempted: false,
        });
    }

    #[test]
    fn direct_preflight_rejects_text_dtmf_existing_capture_and_invalid_limits() {
        let limits = TimelineRecordingLimits::default();
        assert!(matches!(
            session().validate_timeline_recording(limits),
            Err(EvalError::Unsupported(_))
        ));
        assert!(recorded().validate_timeline_recording(limits).is_ok());
        let mut dtmf = recorded();
        dtmf.scenario.turns[0].dtmf = Some("1".into());
        assert!(matches!(
            dtmf.validate_timeline_recording(limits),
            Err(EvalError::Unsupported(_))
        ));
        let mut existing = recorded();
        existing.capture = Some(Capture::new(BotAudioLimits::default()));
        assert!(matches!(
            existing.validate_timeline_recording(limits),
            Err(EvalError::Unsupported(_))
        ));
        assert!(matches!(
            recorded().validate_timeline_recording(TimelineRecordingLimits {
                max_records: 0,
                ..limits
            }),
            Err(EvalError::Scenario(_))
        ));
    }

    #[tokio::test]
    async fn already_cancelled_direct_recording_returns_empty_failure_without_connecting() {
        let cancellation = Cancellation::new();
        cancellation.cancel();
        let run = recorded()
            .run_with_timeline_recording(cancellation, TimelineRecordingLimits::default())
            .await
            .unwrap();
        assert!(!run.result.passed);
        assert_eq!(run.result.failures[0].kind, "cancelled");
        assert!(run.to_wav().unwrap().is_none());
    }

    #[test]
    fn bot_decoder_has_exact_raw_boundary_and_rejects_explicit_nonmono() {
        let mut session = recorded();
        select(
            &mut session,
            TimelineRecordingLimits {
                max_raw_bytes: 2,
                ..Default::default()
            },
        );
        let recorder = &session.timeline.as_ref().unwrap().recorder;
        let stereo =
            json!({"audio": STANDARD.encode([1, 0]), "sampleRate": 16000, "numChannels": 2});
        assert!(matches!(
            ingest_timeline_bot(recorder, &stereo),
            Err(EvalError::Unsupported(_))
        ));
        assert_eq!(recorder.lock().unwrap().remaining_raw_bytes(), 2);
        let admitted =
            json!({"audio": STANDARD.encode([1, 0]), "sampleRate": 16000, "channels": 1});
        ingest_timeline_bot(recorder, &admitted).unwrap();
        assert_eq!(recorder.lock().unwrap().remaining_raw_bytes(), 0);
        assert!(matches!(
            ingest_timeline_bot(recorder, &admitted),
            Err(EvalError::Limit("timeline recording raw bytes"))
        ));
    }

    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    fn joint() -> EvalSession {
        let mut session = recorded();
        session.audio_to_audio = Some(AudioToAudioState {
            run: None,
            admission_open: true,
            stop_bot_attempted: false,
        });
        session.capture = Some(Capture::new(BotAudioLimits::default()));
        session
    }
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    fn audio(data: Value) -> Packet {
        Packet::Text(
            json!({"label": "rtvi-ai", "type": "eval-bot-audio", "data": data}).to_string(),
        )
    }
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    #[test]
    fn joint_shared_pcm_admission_and_independent_capture_limits() {
        let limits = TimelineRecordingLimits::default();
        for recording in [false, true] {
            for data in [
                json!({"audio": STANDARD.encode([1, 0]), "sampleRate": 24000}),
                json!({"audio": STANDARD.encode([1, 0]), "sampleRate": 16000, "channels": 2}),
                json!({"audio": STANDARD.encode([1, 0]), "sampleRate": 16000, "numChannels": 0}),
            ] {
                let mut session = joint();
                assert!(session.validate_timeline_recording(limits).is_ok());
                if recording {
                    select(&mut session, limits);
                }
                assert!(matches!(
                    session.ingest(audio(data)),
                    Err(EvalError::Unsupported(_))
                ));
                assert!(session.capture.as_ref().unwrap().chunks.is_empty());
                if let Some(state) = &session.timeline {
                    assert_eq!(
                        state.recorder.lock().unwrap().remaining_raw_bytes(),
                        limits.max_raw_bytes
                    );
                }
            }
        }
        // The second consumer may fail after recording has admitted a prefix.
        // Preserve that bounded diagnostic, without pretending atomic success.
        let mut session = joint();
        session.capture = Some(Capture::new(BotAudioLimits {
            max_bytes: 2,
            ..Default::default()
        }));
        select(&mut session, limits);
        let data = json!({"audio": STANDARD.encode([1, 0, 2, 0]), "sampleRate": 16000});
        assert!(matches!(
            session.ingest(audio(data)),
            Err(EvalError::Limit("bot audio capture"))
        ));
        assert!(session.capture.as_ref().unwrap().chunks.is_empty());
        assert_eq!(
            session
                .timeline
                .as_ref()
                .unwrap()
                .recorder
                .lock()
                .unwrap()
                .remaining_raw_bytes(),
            limits.max_raw_bytes - 4
        );
    }

    #[cfg(feature = "user-speech")]
    #[test]
    fn configured_hosted_recording_is_refused_by_local_state_guard() {
        let mut session = recorded();
        session.hosted_speech = Some(HostedSpeechState {
            admission_closed: false,
            stop_bot_attempted: false,
        });
        assert!(matches!(
            session.validate_timeline_recording(TimelineRecordingLimits::default()),
            Err(EvalError::Unsupported(_))
        ));
    }
}
