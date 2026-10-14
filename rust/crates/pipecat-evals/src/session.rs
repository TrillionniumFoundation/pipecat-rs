//! Owned native sessions with bounded transport, handshake, runtime, and cancellation.
use crate::recorded_audio::{Playback, RecordedAudio, MAX_TOTAL_BYTES};
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
    wire_bytes: usize,
    judge: Option<EvalJudge>,
    active_expectation: i64,
    recordings: BTreeMap<usize, RecordedAudio>,
    playback: Option<Playback>,
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
            wire_bytes: 0,
            judge,
            active_expectation: -1,
            recordings: BTreeMap::new(),
            playback: None,
        })
    }
    /// Replace selected user turns with real recorded PCM input. The user text
    /// remains a judge reference and is never sent as text for these turns.
    /// Requires a compatible raw-audio endpoint; EvalBotHost remains text-only.
    /// Validate all indices and aggregate storage before any connection.
    pub fn with_recorded_audio_inputs(
        mut self,
        inputs: impl IntoIterator<Item = (usize, RecordedAudio)>,
    ) -> Result<Self, EvalError> {
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
        let started = Instant::now();
        self.stream = EventStream::new(self.options.event_limits.clone());
        let max_duration = self.options.max_duration;
        let outcome = tokio::select! {
            biased;
            _=cancel.cancelled()=>Err(EvalError::Cancelled),
            _=tokio::time::sleep(max_duration)=>Err(EvalError::Timeout("session")),
            result=self.drive()=>result,
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
        self.result
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
        if self.socket.is_none() {
            self.socket =
                Some(WebSocketSession::connect(&self.url, self.options.transport.clone()).await?);
        }
        self.phase = "handshake";
        self.send("client-ready",json!({"version":PROTOCOL_VERSION,"about":{"library":"pipecat-evals-rust","library_version":env!("CARGO_PKG_VERSION")}})).await?;
        let deadline = Instant::now() + self.options.handshake_timeout;
        loop {
            if self.pump(deadline).await.map_err(|e| {
                if matches!(e, EvalError::Timeout(_)) {
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
        if !self.recordings.is_empty() {
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
    async fn run_turn(&mut self, index: usize, turn: ScriptTurn) -> Result<(), EvalError> {
        self.matcher.reset_turn();
        // Drain the bounded transport admission queue before the send boundary so
        // already received output cannot satisfy a subsequent user turn.
        self.drain_received().await?;
        if let Some(after) = turn.send_after {
            let target = if let Some(event) = after.event {
                let deadline = Instant::now() + self.options.send_after_timeout;
                loop {
                    if let Some(at) = self.stream.latest_event_times.get(&event) {
                        break *at + Duration::from_millis(after.delay_ms);
                    }
                    self.pump(deadline).await.map_err(|e| {
                        if matches!(e, EvalError::Timeout(_)) {
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
                    Err(EvalError::Timeout(_)) => break,
                    other => {
                        other?;
                    }
                }
            }
            if turn.user.is_none() && turn.dtmf.is_none() && index > 0 {
                self.stream.turn_boundary();
            }
        }
        let mut audio_completion = None;
        if let Some(user) = turn.user {
            self.record_pending_reply()?;
            self.stream.turn_boundary();
            if let Some(judge) = &mut self.judge {
                judge.add_user_message(&user)?;
            }
            if let Some(clip) = self.recordings.remove(&index) {
                audio_completion = Some(
                    self.playback
                        .as_ref()
                        .ok_or_else(|| {
                            EvalError::Scenario("recording playback is not running".into())
                        })?
                        .queue(clip)?,
                );
            } else {
                self.send(
                    "send-text",
                    json!({"content":user,"options":{"run_immediately":true,"audio_response":false}}),
                ).await?;
            }
        } else if let Some(dtmf) = turn.dtmf {
            self.record_pending_reply()?;
            self.stream.turn_boundary();
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
            if let Some(mut completed) = audio_completion {
                loop {
                    tokio::select! {
                        biased;
                        result = &mut completed => { result.map_err(|_| EvalError::Scenario("recording ended before playback completion".into()))?; break; }
                        result = self.pump(Instant::now() + Duration::from_secs(61)) => { result?; }
                    }
                }
            }
        }
        Ok(())
    }
    fn record_event(&mut self, event: &Value) -> Result<(), EvalError> {
        if event["type"] == "llm_response" {
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
            for event in self.stream.pending_responses() {
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
                        Some(if self.stream.bot_speaking {
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
                    && event["type"] == "llm_response"
                    && event["text"].as_str().is_none_or(|s| s.trim().is_empty())
                {
                    continue;
                }
                self.record_event(&event)?;
                outcome = self.matcher.feed(&mut check, event)?;
                continue;
            }
            match self.pump(until).await {
                Err(EvalError::Timeout(_)) => {}
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
            let verdict = tokio::select! {
                biased;
                result = &mut future => Some(result?),
                packet = self.pump(deadline) => {
                    match packet {
                        Err(EvalError::Timeout(_)) => return Err(EvalError::Judge(ClassifierError::Deadline)),
                        other => { other?; }
                    }
                    None
                }
            };
            if self.stream.epoch() != epoch {
                return Ok(None);
            }
            if let Some(verdict) = verdict {
                tokio::time::timeout_at(deadline, self.drain_received())
                    .await
                    .map_err(|_| EvalError::Judge(ClassifierError::Deadline))??;
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
    async fn send_envelope(&self, envelope: Envelope) -> Result<(), EvalError> {
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
        if Instant::now() >= deadline {
            return Err(EvalError::Timeout("event"));
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
        let packet = tokio::select! {
            biased;
            error = audio_error => return Err(EvalError::Scenario(error)),
            packet = tokio::time::timeout_at(deadline, self.socket.as_mut().ok_or(EvalError::Disconnected)?.recv()) => packet.map_err(|_| EvalError::Timeout("event"))?.ok_or(EvalError::Disconnected)?,
        };
        self.ingest(packet)
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
            return Ok(true);
        }
        self.stream.ingest(&envelope)?;
        Ok(false)
    }
    async fn drain_received(&mut self) -> Result<(), EvalError> {
        loop {
            match tokio::time::timeout(
                Duration::ZERO,
                self.socket.as_mut().ok_or(EvalError::Disconnected)?.recv(),
            )
            .await
            {
                Ok(Some(packet)) => {
                    self.ingest(packet)?;
                }
                Ok(None) => return Err(EvalError::Disconnected),
                Err(_) => return Ok(()),
            }
        }
    }
    async fn pump_until(&mut self, target: Instant) -> Result<(), EvalError> {
        while Instant::now() < target {
            match self.pump(target).await {
                Err(EvalError::Timeout(_)) => break,
                other => {
                    other?;
                }
            }
        }
        Ok(())
    }
    pub(crate) async fn shutdown(&mut self) {
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
