//! Owned native sessions with bounded transport, handshake, runtime, and cancellation.
use crate::bot_audio::{BotAudioLimits, BotAudioRun, Capture};
#[cfg(feature = "bot-audio-transcription")]
use crate::bot_transcription::{BotTranscription, BotTranscriptionConfig};
use crate::recorded_audio::{Playback, RecordedAudio, MAX_TOTAL_BYTES};
#[cfg(feature = "user-speech")]
use crate::user_speech::{
    CartesiaUserSpeechSettings, CartesiaUserSpeechVoice, UserSpeechBudget, UserSpeechOwner,
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
use std::{collections::BTreeMap, time::Duration};
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
    playback: Option<Playback>,
    #[cfg(feature = "user-speech")]
    user_speech: Option<UserSpeechOwner>,
    #[cfg(feature = "user-speech")]
    user_speech_budget: Option<UserSpeechBudget>,
    capture: Option<Capture>,
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
            playback: None,
            #[cfg(feature = "user-speech")]
            user_speech: None,
            #[cfg(feature = "user-speech")]
            user_speech_budget: None,
            capture: None,
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
    /// Replace selected user turns with real recorded PCM input. The user text
    /// remains a judge reference and is never sent as text for these turns.
    /// Requires a compatible endpoint, such as EvalBotHost with_raw_audio_input(true).
    /// Validate all indices and aggregate storage before any connection.
    pub fn with_recorded_audio_inputs(
        mut self,
        inputs: impl IntoIterator<Item = (usize, RecordedAudio)>,
    ) -> Result<Self, EvalError> {
        #[cfg(feature = "user-speech")]
        if self.user_speech_budget.is_some() {
            return Err(EvalError::Scenario(
                "preflighted user speech recording plan is immutable".into(),
            ));
        }
        let mut recordings = BTreeMap::new();
        let mut total = 0usize;
        for (index, clip) in inputs {
            let turn =
                self.scenario.turns.get(index).ok_or_else(|| {
                    EvalError::Scenario("recording turn index out of range".into())
                })?;
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
        self.recordings = recordings;
        Ok(self)
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
    async fn run_inner(&mut self, cancel: Cancellation) {
        let started = Instant::now();
        self.stream = EventStream::new(self.options.event_limits.clone());
        self.bot_ready_received = false;
        #[cfg(feature = "bot-audio-transcription")]
        {
            self.pending_recognition = None;
        }
        let max_duration = self.options.max_duration;
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
            self.playback = Some(Playback::start(
                self.socket
                    .as_ref()
                    .ok_or(EvalError::Disconnected)?
                    .sender(),
                &self.options,
            ));
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
        self.active_turn = -1;
        self.active_expectation = -1;
        self.active_turn_started = None;
        Ok(())
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
        self.drain_received().await?;
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
        let mut audio_completion = None;
        #[cfg(feature = "user-speech")]
        let mut speech_anchor = None;
        if let Some(user) = turn.user {
            self.record_pending_reply()?;
            self.turn_boundary()?;
            #[cfg(feature = "user-speech")]
            let admitted = Instant::now();
            if let Some(judge) = &mut self.judge {
                judge.add_user_message(&user)?;
            }
            if let Some(clip) = self.recordings.remove(&index) {
                audio_completion = Some((
                    self.playback
                        .as_ref()
                        .ok_or_else(|| {
                            EvalError::Scenario("recording playback is not running".into())
                        })?
                        .queue(clip)?,
                    Instant::now() + Duration::from_secs(61),
                ));
            } else {
                #[cfg(feature = "user-speech")]
                if let Some(owner) = &mut self.user_speech {
                    // Assertion deadlines include cache/HTTP production time.
                    speech_anchor = Some(admitted);
                    owner.begin(user, admitted)?;
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
            if let Some((mut completed, playback_deadline)) = audio_completion {
                loop {
                    tokio::select! {
                        biased;
                        result = &mut completed => { result.map_err(|_| EvalError::Scenario("recording ended before playback completion".into()))?; break; }
                        result = self.pump(self.recorded_playback_deadline(playback_deadline)) => { result?; }
                    }
                }
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
    fn recorded_playback_deadline(&self, fixed: Instant) -> Instant {
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
                tokio::time::timeout_at(deadline, self.drain_received())
                    .await
                    .map_err(|_| EvalError::Judge(ClassifierError::Deadline))??;
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
            self.drain_recognition().await?;
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
        let speech_enabled = self.user_speech.is_some();
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
                self.drain_recognition().await?;
            }
            #[cfg(not(feature = "bot-audio-transcription"))]
            let _ = recognition;
            return Ok(self.phase == "handshake" && self.bot_ready_received);
        }
        self.ingest(packet.expect("pump selects either recognition or transport"))
    }
    #[cfg(feature = "bot-audio-transcription")]
    async fn drain_recognition(&mut self) -> Result<(), EvalError> {
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
        self.drain_received().await?;
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
            if let Some(capture) = &mut self.capture {
                capture.ingest(envelope.data.as_ref().unwrap_or(&Value::Null))?;
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
        }
        self.stream.ingest(&envelope)?;
        #[cfg(feature = "bot-audio-transcription")]
        if let Some(engine) = &mut self.transcription {
            engine.set_epoch(self.stream.epoch())?;
        }
        Ok(false)
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
    pub(crate) async fn shutdown(&mut self) {
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

    async fn socket_pair() -> (WebSocketSession, WebSocketSession) {
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
