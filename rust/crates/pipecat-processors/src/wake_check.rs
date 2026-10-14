//! Bounded migration profile for the deprecated Python WakeCheckFilter.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, InvocationFence, ObservedProcessor,
    ProcessorContext, ProcessorError, StartFrame,
};
use pipecat_text::wake_check::{WakeCheckLimits, WakeCheckMatcher};
use std::{collections::BTreeMap, sync::Mutex, time::Duration};
use tokio::time::Instant;

/// Native bounds, not additional behavior of the unbounded Python processor.
#[derive(Clone, Debug)]
pub struct WakeCheckFilterConfig {
    /// Strict, sliding awake interval. Positive and at most 24 hours.
    pub keepalive_timeout: Duration,
    /// Positive and at most 64. Participants are never silently evicted.
    pub max_participants: usize,
    /// Positive and at most 512 UTF-8 bytes. The empty anonymous ID is valid.
    pub max_user_id_bytes: usize,
    /// Positive and at most 16 KiB of accumulated idle text per participant.
    pub max_participant_bytes: usize,
    /// Positive and at most 128 KiB, counting retained user IDs and idle text.
    /// A prospective idle candidate must fit before matching, even if a match
    /// would immediately release it. Allocation overhead is not counted.
    pub max_total_retained_bytes: usize,
    /// Pure matcher's additional phrase, input and work bounds.
    pub matcher_limits: WakeCheckLimits,
}
impl Default for WakeCheckFilterConfig {
    fn default() -> Self {
        Self {
            keepalive_timeout: Duration::from_secs(3),
            max_participants: 64,
            max_user_id_bytes: 512,
            max_participant_bytes: 16 * 1024,
            max_total_retained_bytes: 128 * 1024,
            matcher_limits: WakeCheckLimits::default(),
        }
    }
}
impl WakeCheckFilterConfig {
    fn validate(&self) -> Result<(), ProcessorError> {
        if self.keepalive_timeout.is_zero()
            || self.keepalive_timeout > Duration::from_secs(24 * 60 * 60)
            || !(1..=64).contains(&self.max_participants)
            || !(1..=512).contains(&self.max_user_id_bytes)
            || !(1..=16 * 1024).contains(&self.max_participant_bytes)
            || !(1..=128 * 1024).contains(&self.max_total_retained_bytes)
        {
            return Err(error("invalid configuration"));
        }
        Ok(())
    }
}
#[derive(Default)]
struct Participant {
    last_accepted: Option<Instant>,
    accumulator: String,
}
#[derive(Clone)]
struct Owner {
    identity: ObservedProcessor,
    // Setup's raw generation-free fence internally holds only weak runtimes.
    lifetime: InvocationFence,
}
#[derive(Default)]
struct State {
    owner: Option<Owner>,
    closed: bool,
    participants: BTreeMap<String, Participant>,
    retained_bytes: usize,
}
impl State {
    fn clear(&mut self) {
        self.participants.clear();
        self.retained_bytes = 0;
        self.closed = true;
    }
}
enum Decision {
    Suppress,
    Forward,
    Rewrite(String),
}

/// Migration adapter for the deprecated WakeCheckFilter, not the recommended
/// WakePhrase user-turn strategy or a general user-mute policy.
///
/// Only Transcription and TranscriptionDetailed participate, regardless of
/// finalized. Other variants (including interim, eager and translation) retain
/// their original frame and direction. Each user ID has an independent rolling
/// awake window. Idle chunks concatenate without separators or normalization;
/// configuration order selects the wake phrase. The triggering current frame
/// retains its identity, metadata, payload fields and wake phrase, with only its
/// text replaced by the accumulated matching suffix. Accepted transcriptions
/// always travel downstream, including ones received upstream.
///
/// Native differences are deliberate: bounded ASCII-token phrase configuration
/// with Unicode-aware text boundaries (see WakeCheckMatcher), explicit atomic
/// capacity/work/authority errors, positive finite Duration, and one monotonic
/// Tokio Instant sampled outside the state lock per transcription. Python samples
/// wall time separately for the awake check and successful timer refresh.
/// Concurrent out-of-order samples never move the stored instant backward.
/// A deadline equality is expired; an accepted empty awake frame refreshes it.
/// Bounds cover retained and matched text, not untouched awake/bypass payloads.
///
/// Idle frames carrying native semantic authority are rejected before copying,
/// retaining or rewriting text. Awake frames can preserve that authority because
/// the complete original frame is forwarded without payload changes. Rejections
/// leave all participant state unchanged, including expiration and new IDs.
/// State is committed before push, as in the source; push failure is returned,
/// never a delivery receipt, and cannot roll back already committed state.
///
/// Unlike the source's lifetime map, each complete native setup starts fresh;
/// actual End, Stop and Cancel clear state. Interruption preserves the source's
/// input accumulator/window: these are not bot-output generation capabilities.
/// A weak setup-lifetime owner rejects concurrent reuse until the previous
/// runtime finishes, including the gap after processing Stop. Failed concurrent
/// setup cleanup cannot erase the live owner's data. No tasks or timers are made.
pub struct WakeCheckFilter {
    config: WakeCheckFilterConfig,
    matcher: WakeCheckMatcher,
    state: Mutex<State>,
}
impl WakeCheckFilter {
    pub fn new<I, S>(config: WakeCheckFilterConfig, wake_phrases: I) -> Result<Self, ProcessorError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        config.validate()?;
        let matcher = WakeCheckMatcher::new(wake_phrases, config.matcher_limits)
            .map_err(|_| error("invalid wake phrase configuration"))?;
        Ok(Self {
            config,
            matcher,
            state: Mutex::new(State::default()),
        })
    }
    fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>, ProcessorError> {
        self.state.lock().map_err(|_| error("state lock poisoned"))
    }
    fn decide(
        &self,
        state: &mut State,
        frame: &Frame,
        user_id: &str,
        text: &str,
        now: Instant,
    ) -> Result<Decision, ProcessorError> {
        if user_id.len() > self.config.max_user_id_bytes {
            return Err(error("participant ID byte limit exceeded"));
        }
        let prior = state.participants.get(user_id);
        if prior.is_some_and(|p| {
            p.last_accepted.is_some_and(|last| {
                now.saturating_duration_since(last) < self.config.keepalive_timeout
            })
        }) {
            let participant = state.participants.get_mut(user_id).unwrap();
            // Concurrent invocations may acquire this lock in a different order
            // from their outside-lock clock samples. Never rewind the window.
            participant.last_accepted = Some(participant.last_accepted.unwrap().max(now));
            return Ok(Decision::Forward);
        }
        // This existing core guard checks every native carrier, despite its
        // historical LLM-specific name. Never copy a payload to shed authority.
        require_no_native_semantic_authority(frame)?;
        if prior.is_none() && state.participants.len() >= self.config.max_participants {
            return Err(error("participant capacity exceeded"));
        }
        let old_bytes = prior.map_or(0, |p| p.accumulator.len());
        let candidate_bytes = old_bytes
            .checked_add(text.len())
            .filter(|len| *len <= self.config.max_participant_bytes)
            .ok_or_else(|| error("participant text byte limit exceeded"))?;
        let new_id_bytes = if prior.is_none() { user_id.len() } else { 0 };
        let retained = state
            .retained_bytes
            .checked_add(new_id_bytes)
            .and_then(|n| n.checked_add(text.len()))
            .filter(|n| *n <= self.config.max_total_retained_bytes)
            .ok_or_else(|| error("total retained byte limit exceeded"))?;
        if candidate_bytes > self.config.matcher_limits.max_input_bytes {
            return Err(error("wake match input byte limit exceeded"));
        }
        // Byte bounds and the authority check precede the first copy.
        let mut candidate = String::with_capacity(candidate_bytes);
        if let Some(prior) = prior {
            candidate.push_str(&prior.accumulator);
        }
        candidate.push_str(text);
        let found = self
            .matcher
            .find(&candidate)
            .map_err(|_| error("wake match input or work limit exceeded"))?;
        let (participant, retained, decision) = if let Some(found) = found {
            candidate.drain(..found.suffix_range.start);
            (
                Participant {
                    last_accepted: Some(now),
                    accumulator: String::new(),
                },
                retained - candidate_bytes,
                Decision::Rewrite(candidate),
            )
        } else {
            (
                Participant {
                    last_accepted: None,
                    accumulator: candidate,
                },
                retained,
                Decision::Suppress,
            )
        };
        // Commit only after every fallible profile check, including matching.
        if let Some(prior) = state.participants.get_mut(user_id) {
            *prior = participant;
        } else {
            state.participants.insert(user_id.to_owned(), participant);
        }
        state.retained_bytes = retained;
        Ok(decision)
    }
}
#[async_trait]
impl FrameProcessor for WakeCheckFilter {
    fn name(&self) -> &str {
        "WakeCheckFilter"
    }
    async fn setup(
        &self,
        _config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let incoming = Owner {
            identity: ctx.processor_identity(),
            lifetime: ctx.invocation_fence()?,
        };
        loop {
            let old = self.lock()?.owner.clone();
            // Runtime admission and its possible destruction stay outside state.
            if old
                .as_ref()
                .is_some_and(|owner| owner.lifetime.is_current())
            {
                return Err(error("already belongs to an active pipeline"));
            }
            let mut state = self.lock()?;
            if state.owner.as_ref().map(|o| &o.identity) != old.as_ref().map(|o| &o.identity) {
                continue;
            }
            *state = State {
                owner: Some(incoming),
                ..State::default()
            };
            return Ok(());
        }
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let identity = ctx.processor_identity();
        let input = match &frame.data {
            FrameData::Transcription(f) => Some((f.user_id.as_str(), f.text.as_str())),
            FrameData::TranscriptionDetailed(f) => Some((f.user_id.as_str(), f.text.text.as_str())),
            _ => None,
        };
        let selected = input.is_some();
        let now = input.map(|_| Instant::now());
        let decision = {
            let mut state = self.lock()?;
            if state.owner.as_ref().map(|o| &o.identity) != Some(&identity) {
                return Err(error("frame belongs to a different pipeline"));
            }
            if matches!(
                frame.data,
                FrameData::End(_)
                    | FrameData::EndDetailed(_)
                    | FrameData::Cancel(_)
                    | FrameData::CancelDetailed(_)
                    | FrameData::Stop
            ) {
                state.clear();
            }
            match input {
                Some(_) if state.closed => return Err(error("pipeline session is closed")),
                Some((user_id, text)) => {
                    self.decide(&mut state, &frame, user_id, text, now.unwrap())?
                }
                None => Decision::Forward,
            }
        };
        match decision {
            Decision::Suppress => Ok(()),
            Decision::Forward => {
                ctx.push(
                    frame,
                    if selected {
                        Direction::Downstream
                    } else {
                        direction
                    },
                )
                .await
            }
            Decision::Rewrite(text) => {
                match &mut frame.data {
                    FrameData::Transcription(f) => f.text = text,
                    FrameData::TranscriptionDetailed(f) => f.text.text = text,
                    _ => unreachable!("only selected transcription frames can be rewritten"),
                }
                ctx.push(frame, Direction::Downstream).await
            }
        }
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let owner = self.lock()?.owner.clone();
        if owner
            .as_ref()
            .is_some_and(|owner| owner.lifetime.is_current())
        {
            // Core also calls this after failed setup in a second pipeline.
            return Ok(());
        }
        let mut state = self.lock()?;
        if state.owner.as_ref().map(|o| &o.identity) == owner.as_ref().map(|o| &o.identity) {
            state.clear();
        }
        Ok(())
    }
}
fn require_no_native_semantic_authority(frame: &Frame) -> Result<(), ProcessorError> {
    frame
        .require_no_native_llm_continuation()
        .map_err(|_| error("idle text cannot consume native semantic authority"))
}
fn error(message: &str) -> ProcessorError {
    ProcessorError::new(format!("wake check: {message}")).with_permanence_override(false)
}

#[cfg(test)]
mod atomic_tests {
    use super::*;
    use pipecat_core::{SpeculationOwner, TranscriptionFrame};

    fn frame(text: &str) -> Frame {
        Frame::new(FrameData::Transcription(TranscriptionFrame {
            text: text.into(),
            user_id: "u".into(),
            ..Default::default()
        }))
    }
    fn snapshot(state: &State) -> (usize, Vec<(String, Option<Instant>, String)>) {
        (
            state.retained_bytes,
            state
                .participants
                .iter()
                .map(|(id, p)| (id.clone(), p.last_accepted, p.accumulator.clone()))
                .collect(),
        )
    }
    fn decide(
        filter: &WakeCheckFilter,
        state: &mut State,
        id: &str,
        text: &str,
        now: Instant,
    ) -> Result<Decision, ProcessorError> {
        filter.decide(state, &frame(text), id, text, now)
    }
    #[test]
    fn expired_and_new_participant_capacity_failures_are_fully_atomic() {
        let filter = WakeCheckFilter::new(
            WakeCheckFilterConfig {
                max_participants: 1,
                max_user_id_bytes: 2,
                max_participant_bytes: 3,
                max_total_retained_bytes: 4,
                ..Default::default()
            },
            ["go"],
        )
        .unwrap();
        let mut state = State::default();
        let now = Instant::now();
        let before = snapshot(&state);
        assert!(decide(&filter, &mut state, "long", "go", now).is_err());
        assert_eq!(snapshot(&state), before);
        assert!(decide(&filter, &mut state, "u", "long", now).is_err());
        assert_eq!(snapshot(&state), before);
        assert!(matches!(
            decide(&filter, &mut state, "u", "go", now).unwrap(),
            Decision::Rewrite(_)
        ));
        let before = snapshot(&state);
        let expired = now + Duration::from_secs(3);
        assert!(decide(&filter, &mut state, "u", "long", expired).is_err());
        assert_eq!(
            snapshot(&state),
            before,
            "even expired last-accepted must survive rejection"
        );
        assert!(decide(&filter, &mut state, "v", "go", expired).is_err());
        assert_eq!(snapshot(&state), before);
        assert!(matches!(
            decide(&filter, &mut state, "u", "g", expired).unwrap(),
            Decision::Suppress
        ));
        assert_eq!(state.participants["u"].last_accepted, None);
        assert!(matches!(
            decide(&filter, &mut state, "u", "o", expired).unwrap(),
            Decision::Rewrite(_)
        ));
    }
    #[test]
    fn total_bytes_and_match_input_limit_never_insert_or_expire_on_error() {
        let now = Instant::now();
        let filter = WakeCheckFilter::new(
            WakeCheckFilterConfig {
                max_total_retained_bytes: 3,
                matcher_limits: WakeCheckLimits {
                    max_input_bytes: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
            ["go"],
        )
        .unwrap();
        let mut state = State::default();
        assert!(decide(&filter, &mut state, "ab", "go", now).is_err());
        assert!(state.participants.is_empty());
        decide(&filter, &mut state, "u", "go", now).unwrap();
        let before = snapshot(&state);
        assert!(decide(&filter, &mut state, "", "xx", now).is_ok());
        let total_before = snapshot(&state);
        assert!(decide(&filter, &mut state, "v", "g", now).is_err());
        assert_eq!(snapshot(&state), total_before);
        // Separate matcher input-budget failure from the global text budget.
        let filter = WakeCheckFilter::new(
            WakeCheckFilterConfig {
                matcher_limits: WakeCheckLimits {
                    max_input_bytes: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
            ["go"],
        )
        .unwrap();
        let mut state = State::default();
        decide(&filter, &mut state, "u", "go", now).unwrap();
        assert_eq!(snapshot(&state), before);
        assert!(decide(
            &filter,
            &mut state,
            "u",
            "xxx",
            now + Duration::from_secs(3)
        )
        .is_err());
        assert_eq!(snapshot(&state), before);
    }
    #[test]
    fn matcher_work_and_authority_preserve_expired_state_and_new_ids() {
        let filter = WakeCheckFilter::new(
            WakeCheckFilterConfig {
                matcher_limits: WakeCheckLimits {
                    max_match_work: 20,
                    ..Default::default()
                },
                ..Default::default()
            },
            ["go"],
        )
        .unwrap();
        let now = Instant::now();
        let mut state = State::default();
        let empty = snapshot(&state);
        assert!(decide(&filter, &mut state, "new", &"x".repeat(100), now).is_err());
        assert_eq!(snapshot(&state), empty);
        decide(&filter, &mut state, "u", "go", now).unwrap();
        let before = snapshot(&state);
        let expired = now + Duration::from_secs(3);
        assert!(decide(&filter, &mut state, "u", &"x".repeat(100), expired).is_err());
        assert_eq!(snapshot(&state), before);
        let owner = SpeculationOwner::new();
        let mut authorized = frame("go");
        authorized
            .set_native_speculation(owner.capability())
            .unwrap();
        for id in ["u", "new"] {
            assert!(filter
                .decide(&mut state, &authorized, id, "go", expired)
                .is_err());
            assert_eq!(snapshot(&state), before);
        }
        assert!(matches!(
            decide(&filter, &mut state, "u", "go", expired).unwrap(),
            Decision::Rewrite(_)
        ));
    }
    #[test]
    fn out_of_order_clock_samples_never_shorten_an_awake_window() {
        let filter = WakeCheckFilter::new(Default::default(), ["go"]).unwrap();
        let mut state = State::default();
        let now = Instant::now();
        decide(&filter, &mut state, "u", "go", now).unwrap();
        let later = now + Duration::from_secs(2);
        assert!(matches!(
            decide(&filter, &mut state, "u", "later", later).unwrap(),
            Decision::Forward
        ));
        assert!(matches!(
            decide(&filter, &mut state, "u", "earlier sample", now).unwrap(),
            Decision::Forward
        ));
        assert_eq!(state.participants["u"].last_accepted, Some(later));
        assert!(matches!(
            decide(
                &filter,
                &mut state,
                "u",
                "still awake",
                now + Duration::from_secs(4)
            )
            .unwrap(),
            Decision::Forward
        ));
    }
}
