//! Bounded telephone-keypad aggregation into finalized user transcriptions.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use pipecat_core::{
    async_trait, DetailedTranscriptionFrame, Direction, Frame, FrameData, FrameProcessor,
    KeypadEntry, ProcessorContext, ProcessorError, StartFrame, TextFrame,
};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, Notify},
    time::Instant,
};

const MAX_DIGITS: usize = 4096;
const MAX_PREFIX_BYTES: usize = 4096;
const MAX_DURATION: Duration = Duration::from_secs(3600);
const MAX_SELF_SCOPES: usize = 64;

/// Telephone keypad entry policy. Values are validated atomically by `new`.
#[derive(Clone, Debug)]
pub struct DtmfAggregatorConfig {
    /// Idle period after the most recently admitted digit (default: two seconds).
    pub idle_timeout: Duration,
    /// Included in the finalized text. Only the twelve telephone keys are valid.
    pub termination_digit: KeypadEntry,
    /// UTF-8 prefix; at most 4096 bytes. Empty is valid.
    pub prefix: String,
    /// At most 4096 single-byte digits. Overflow discards the entire entry and
    /// reports an error; it never publishes a silently truncated entry.
    pub max_digits: usize,
    /// Absolute lifetime of one entry, independent of repeated key presses.
    /// Must be at least `idle_timeout`; both durations are at most one hour.
    pub max_sequence_duration: Duration,
}
impl Default for DtmfAggregatorConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(2),
            termination_digit: KeypadEntry::Pound,
            prefix: "DTMF: ".into(),
            max_digits: 256,
            max_sequence_duration: Duration::from_secs(60),
        }
    }
}
impl DtmfAggregatorConfig {
    fn validate(&self) -> Result<(), ProcessorError> {
        if self.idle_timeout < Duration::from_millis(1)
            || self.idle_timeout > MAX_DURATION
            || self.max_sequence_duration < self.idle_timeout
            || self.max_sequence_duration > MAX_DURATION
            || self.max_digits == 0
            || self.max_digits > MAX_DIGITS
            || self.prefix.len() > MAX_PREFIX_BYTES
            || digit(self.termination_digit).is_none()
        {
            return Err(ProcessorError::new(
                "invalid DTMF aggregation configuration",
            ));
        }
        Ok(())
    }
}
struct Sequence {
    digits: String,
    source: ProcessorContext,
    deadline: Instant,
    hard_deadline: Instant,
}
#[derive(Default)]
struct State {
    active: bool,
    session: u64,
    sequence: Option<Sequence>,
    timer: Option<(ProcessorContext, u64)>,
    /// Exact raw scopes carried by system digits queued before our broadcasts.
    previous_scopes: VecDeque<ProcessorContext>,
    current_scope: Option<ProcessorContext>,
    scope_deadline: Option<Instant>,
}
impl State {
    fn clear_entry(&mut self) {
        self.sequence = None;
    }
    fn clear_scopes(&mut self) {
        self.clear_entry();
        self.previous_scopes.clear();
        self.current_scope = None;
        self.scope_deadline = None;
    }
    fn expire_stale(&mut self) -> Result<(), ProcessorError> {
        if self
            .scope_deadline
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            self.previous_scopes.clear();
            self.current_scope = None;
            self.scope_deadline = None;
        }
        if let Some(source) = &self.current_scope {
            if !source.invocation_fence()?.is_current() {
                self.clear_scopes();
            }
        }
        if let Some(sequence) = &self.sequence {
            if !sequence.source.invocation_fence()?.is_current() {
                self.clear_entry();
            }
        }
        Ok(())
    }
    fn admitted_scope(
        &mut self,
        ctx: &ProcessorContext,
    ) -> Result<Option<ProcessorContext>, ProcessorError> {
        self.expire_stale()?;
        if ctx.invocation_fence()?.is_current() {
            // A fresh caller with different inherited origins starts an
            // independent chain. Never substitute its authority for an old
            // caller simply because both arrived at the same processor.
            if self
                .current_scope
                .as_ref()
                .is_some_and(|current| !current.same_causal_scope(ctx))
            {
                self.clear_scopes();
            }
            return Ok(Some(ctx.clone()));
        }
        // System input survives interruption. Only our own exact epoch chain
        // can bridge an old input invocation to the newly admitted sequence.
        if let Some(index) = self
            .previous_scopes
            .iter()
            .position(|old| old.same_causal_scope(ctx))
        {
            let scope = self
                .previous_scopes
                .remove(index)
                .expect("existing DTMF scope");
            self.previous_scopes.push_back(scope);
            Ok(self.current_scope.clone())
        } else {
            Ok(None)
        }
    }
    fn remember_self_interruption(
        &mut self,
        before: ProcessorContext,
        after: ProcessorContext,
        lifetime: Duration,
    ) {
        if !self
            .previous_scopes
            .iter()
            .any(|old| old.same_causal_scope(&before))
        {
            if self.previous_scopes.len() == MAX_SELF_SCOPES {
                self.previous_scopes.pop_front();
            }
            self.previous_scopes.push_back(before);
        }
        self.current_scope = Some(after);
        self.scope_deadline
            .get_or_insert_with(|| Instant::now() + lifetime);
    }
    fn stop_timer(&mut self) {
        self.active = false;
        if let Some((ctx, id)) = self.timer.take() {
            ctx.cancel_task(id);
        }
    }
}
/// Place before `LLMUserAggregator`. Every input key is forwarded in its original
/// direction before any derived work; finalized transcriptions go downstream.
///
/// One owned timer is reused for the entire session. External interruptions,
/// Cancel, Stop and cleanup discard pending entries. End flushes before its
/// terminal frame. The first valid digit broadcasts one interruption in both
/// directions, retaining only its own newly admitted sequence across that event.
/// Reuse after cleanup is supported; concurrent pipelines need separate instances.
pub struct DtmfAggregator {
    config: Arc<DtmfAggregatorConfig>,
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
}
impl Default for DtmfAggregator {
    fn default() -> Self {
        Self::new(DtmfAggregatorConfig::default()).expect("valid default DTMF configuration")
    }
}
impl DtmfAggregator {
    pub fn new(config: DtmfAggregatorConfig) -> Result<Self, ProcessorError> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
            state: Arc::new(Mutex::new(State::default())),
            changed: Arc::new(Notify::new()),
        })
    }
    pub fn config(&self) -> &DtmfAggregatorConfig {
        &self.config
    }
    /// Current retained digit count, excluding the configured prefix.
    pub async fn buffered_len(&self) -> usize {
        self.state
            .lock()
            .await
            .sequence
            .as_ref()
            .map_or(0, |s| s.digits.len())
    }
    async fn start_timer(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        if !state.active || state.timer.is_some() {
            return Ok(());
        }
        let session = state.session;
        let shared = self.state.clone();
        let changed = self.changed.clone();
        let config = self.config.clone();
        let id = ctx.spawn("dtmf-idle-timeout", false, async move {
            loop {
                let deadline = {
                    let mut state = shared.lock().await;
                    if !state.active || state.session != session {
                        return Ok(());
                    }
                    state.expire_stale()?;
                    state.sequence.as_ref().map(|sequence| sequence.deadline)
                        .into_iter().chain(state.scope_deadline).min()
                };
                tokio::select! {
                    biased;
                    _ = changed.notified() => {},
                    _ = async {
                        match deadline {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => {
                        let mut state = shared.lock().await;
                        if !state.active || state.session != session {
                            return Ok(());
                        }
                        state.expire_stale()?;
                        if state.sequence.as_ref().is_some_and(|sequence| sequence.deadline <= Instant::now()) {
                            flush(&mut state, &config)?;
                        }
                    }
                }
            }
        })?;
        state.timer = Some((ctx.clone(), id));
        Ok(())
    }
    async fn input(
        &self,
        button: KeypadEntry,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        if !state.active {
            return Ok(());
        }
        let Some(source) = state.admitted_scope(ctx)? else {
            return Ok(());
        };
        let Some(key) = digit(button) else {
            state.clear_entry();
            self.changed.notify_one();
            return Err(ProcessorError::new(
                "DTMF aggregator accepts only telephone keypad keys",
            ));
        };
        let now = Instant::now();
        // A digit at the deadline starts a new entry; delayed task scheduling
        // cannot extend the idle or absolute lifetime of the previous entry.
        if state
            .sequence
            .as_ref()
            .is_some_and(|sequence| sequence.deadline <= now)
        {
            flush(&mut state, &self.config)?;
        }
        if let Some(sequence) = &state.sequence {
            if !sequence.source.same_causal_scope(&source) {
                state.clear_entry();
            }
        }
        if state
            .sequence
            .as_ref()
            .is_some_and(|sequence| sequence.digits.len() >= self.config.max_digits)
        {
            state.clear_entry();
            self.changed.notify_one();
            return Err(ProcessorError::new(
                "DTMF digit capacity exceeded; entry discarded",
            ));
        }
        if state.sequence.is_none() {
            let before = source.clone();
            if !source
                .broadcast_interruption_frame(&Frame::interruption())
                .await?
            {
                return Ok(());
            }
            state.remember_self_interruption(
                before,
                source.clone(),
                self.config.max_sequence_duration,
            );
            let hard_deadline = now + self.config.max_sequence_duration;
            state.sequence = Some(Sequence {
                digits: String::new(),
                source,
                deadline: (now + self.config.idle_timeout).min(hard_deadline),
                hard_deadline,
            });
        }
        let sequence = state.sequence.as_mut().expect("admitted DTMF entry");
        sequence.digits.push(key);
        sequence.deadline = (now + self.config.idle_timeout).min(sequence.hard_deadline);
        if button == self.config.termination_digit {
            flush(&mut state, &self.config)?;
        }
        self.changed.notify_one();
        Ok(())
    }
}
fn flush(state: &mut State, config: &DtmfAggregatorConfig) -> Result<(), ProcessorError> {
    let Some(sequence) = state.sequence.take() else {
        return Ok(());
    };
    let frame = Frame::new(FrameData::TranscriptionDetailed(
        DetailedTranscriptionFrame {
            text: TextFrame::new(format!("{}{}", config.prefix, sequence.digits)),
            user_id: String::new(),
            timestamp: timestamp(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default(),
            ),
            language: None,
            result: None,
            finalized: true,
        },
    ));
    // A non-interruptible timer must not launder a stale input's exact epoch.
    sequence.source.try_push_with_invocation_fence(
        frame,
        Direction::Downstream,
        &sequence.source.invocation_fence()?,
    )
}
#[async_trait]
impl FrameProcessor for DtmfAggregator {
    fn name(&self) -> &str {
        "DtmfAggregator"
    }
    async fn setup(
        &self,
        _config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        state.stop_timer();
        state.clear_scopes();
        state.session = state
            .session
            .checked_add(1)
            .ok_or_else(|| ProcessorError::fatal("DTMF session counter exhausted"))?;
        state.active = true;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match frame.data {
            FrameData::Start(_) | FrameData::StartDetailed(_) => {
                ctx.push(frame, direction).await?;
                self.start_timer(ctx).await
            }
            FrameData::InputDtmf(button) => {
                ctx.push(frame, direction).await?;
                self.input(button, ctx).await
            }
            FrameData::Interruption => {
                self.state.lock().await.clear_scopes();
                self.changed.notify_one();
                ctx.push(frame, direction).await
            }
            FrameData::End(_) | FrameData::EndDetailed(_) => {
                let mut state = self.state.lock().await;
                state.stop_timer();
                flush(&mut state, &self.config)?;
                state.clear_scopes();
                ctx.try_push(frame, direction)
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) | FrameData::Stop => {
                let mut state = self.state.lock().await;
                state.stop_timer();
                state.clear_scopes();
                ctx.try_push(frame, direction)
            }
            _ => ctx.push(frame, direction).await,
        }
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.state.lock().await.stop_timer();
        self.changed.notify_one();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        state.stop_timer();
        state.clear_scopes();
        self.changed.notify_one();
        Ok(())
    }
}
fn digit(button: KeypadEntry) -> Option<char> {
    Some(match button {
        KeypadEntry::Zero => '0',
        KeypadEntry::One => '1',
        KeypadEntry::Two => '2',
        KeypadEntry::Three => '3',
        KeypadEntry::Four => '4',
        KeypadEntry::Five => '5',
        KeypadEntry::Six => '6',
        KeypadEntry::Seven => '7',
        KeypadEntry::Eight => '8',
        KeypadEntry::Nine => '9',
        KeypadEntry::Star => '*',
        KeypadEntry::Pound => '#',
        KeypadEntry::A | KeypadEntry::B | KeypadEntry::C | KeypadEntry::D => return None,
    })
}
fn timestamp(time: Duration) -> String {
    // Gregorian civil date from days since the Unix epoch, matching native STT.
    let days = (time.as_secs() / 86400) as i64;
    let z = days + 719468;
    let era = z / 146097;
    let day_of_era = z - era * 146097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = month_position + if month_position < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let seconds = time.as_secs() % 86400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:06}+00:00",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60,
        time.subsec_micros()
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn utc_timestamp_matches_source_format() {
        assert_eq!(
            timestamp(Duration::ZERO),
            "1970-01-01T00:00:00.000000+00:00"
        );
        assert_eq!(
            timestamp(Duration::from_secs(1709164800)),
            "2024-02-29T00:00:00.000000+00:00"
        );
    }
}
