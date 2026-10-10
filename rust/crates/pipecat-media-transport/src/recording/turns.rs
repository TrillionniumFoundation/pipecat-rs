//! Joins ordered tracker events to sealed recorder prefixes by native identity.
use super::*;
use pipecat_core::FrameObservationToken;
use pipecat_observers::{
    is_turn_recording_control, TurnRecordingUpdate, TurnTrackingEvent, TurnTrackingObserver,
};
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnRecordingLoss {
    TrackerLagged,
    ObservationLoss,
    TrackerInvalidated,
    MissingNativeBoundary,
    RepeatedBoundary,
    BoundaryCapacity,
    LineageCapacity,
    TurnCapacity,
    EventCapacity,
    ControlCapacity,
    InvalidBoundaryOrder,
    RecordingRestarted,
    RecordingError,
    TrackerClosed,
    Cleanup,
}

/// Fail-closed status is permanent for one attachment. Session IDs distinguish
/// independently numbered recorder instances within this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnRecordingStatus {
    Unattached,
    Attached {
        session_id: u64,
    },
    Closed {
        session_id: u64,
    },
    Disabled {
        session_id: u64,
        reason: TurnRecordingLoss,
    },
}
struct Cut {
    token: FrameObservationToken,
    user: u64,
    bot: u64,
}
struct Control {
    token: FrameObservationToken,
    kind: &'static str,
}
pub(super) struct AutomaticTurns {
    status: TurnRecordingStatus,
    session_id: u64,
    cuts: VecDeque<Cut>,
    seen: HashSet<FrameObservationToken>,
    pending: VecDeque<TurnRecordingUpdate>,
    controls: VecDeque<Control>,
    user_base: u64,
    bot_base: u64,
    active: Option<u64>,
    last_started: u64,
    started_recording: bool,
    graceful_terminal: bool,
    stream_closed: bool,
}
impl AutomaticTurns {
    pub(super) fn enabled(&self) -> bool {
        matches!(self.status, TurnRecordingStatus::Attached { .. })
    }
}
pub(super) struct OwnedSubscription(JoinHandle<()>);
impl Drop for OwnedSubscription {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl AudioBufferProcessor {
    /// Attach exactly once, before tracker startup and before recording starts.
    /// Install both the recorder processor and `tracker.register(...)` in the
    /// same native pipeline. A Tokio runtime must be active.
    ///
    /// Events and control-frame cuts are each bounded by `event_capacity`;
    /// outstanding PCM, including delayed turns, shares `max_turn_bytes`.
    /// Loss or ambiguity permanently disables numbered audio for this pair and
    /// emits `TurnAttributionLost`. Primary recording continues. No reconnect
    /// attempts to reconstruct missing conversation history.
    pub fn attach_turn_tracker(
        self: &Arc<Self>,
        tracker: &TurnTrackingObserver,
    ) -> Result<u64, ProcessorError> {
        static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
        if !self.config.enable_turn_audio {
            return Err(ProcessorError::new("turn audio is disabled"));
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(audio_error)?;
        let mut state = self.lock()?;
        if state.automatic.is_some() || state.recording {
            return Err(ProcessorError::new(
                "attach one tracker before recording starts",
            ));
        }
        let stats = tracker.stats();
        if stats.closed || stats.events_emitted != 0 || !stats.numbering_complete {
            return Err(ProcessorError::new(
                "turn tracker must be fresh before attachment",
            ));
        }
        let session_id = NEXT_SESSION
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| ProcessorError::new("turn recording session identity exhausted"))?;
        let mut receiver = tracker.subscribe_recording();
        state.automatic = Some(AutomaticTurns {
            status: TurnRecordingStatus::Attached { session_id },
            session_id,
            cuts: VecDeque::new(),
            seen: HashSet::new(),
            pending: VecDeque::new(),
            controls: VecDeque::new(),
            user_base: 0,
            bot_base: 0,
            active: None,
            last_started: 0,
            started_recording: false,
            graceful_terminal: false,
            stream_closed: false,
        });
        let weak = Arc::downgrade(self);
        let task = runtime.spawn(async move {
            loop {
                let event = receiver.recv().await;
                let Some(recorder) = weak.upgrade() else {
                    return;
                };
                let Ok(mut state) = recorder.lock() else {
                    return;
                };
                let Some(mut automatic) = state.automatic.take() else {
                    return;
                };
                match event {
                    Ok(event) => recorder.receive_turn_update(&mut state, &mut automatic, event),
                    Err(broadcast::error::RecvError::Lagged(_)) => recorder.lose_attribution(
                        &mut state,
                        &mut automatic,
                        TurnRecordingLoss::TrackerLagged,
                    ),
                    Err(broadcast::error::RecvError::Closed) => {
                        automatic.stream_closed = true;
                        recorder.finish_closed_stream(&mut state, &mut automatic);
                    }
                }
                let enabled = automatic.enabled() && !automatic.stream_closed;
                state.automatic = Some(automatic);
                if !enabled {
                    return;
                }
            }
        });
        *self
            .subscription
            .lock()
            .map_err(|_| ProcessorError::fatal("recording subscription lock poisoned"))? =
            Some(OwnedSubscription(task));
        Ok(session_id)
    }

    pub fn turn_recording_status(&self) -> Result<TurnRecordingStatus, ProcessorError> {
        Ok(self
            .lock()?
            .automatic
            .as_ref()
            .map_or(TurnRecordingStatus::Unattached, |a| a.status))
    }
    fn clear_turn_audio(&self, state: &mut RecordingState) {
        state.user.turn.clear();
        state.user.run.clear();
        state.bot.turn.clear();
        state.bot.run.clear();
    }
    fn finish_closed_stream(&self, state: &mut RecordingState, automatic: &mut AutomaticTurns) {
        // The tracker can see a first push before the recorder sees that frame.
        // Keep bounded pending boundaries until processing catches up or cleanup.
        if automatic.stream_closed && automatic.enabled() && automatic.pending.is_empty() {
            if automatic.active.is_some() {
                self.lose_attribution(state, automatic, TurnRecordingLoss::TrackerClosed);
            } else {
                automatic.status = TurnRecordingStatus::Closed {
                    session_id: automatic.session_id,
                };
                automatic.cuts.clear();
                automatic.seen.clear();
                automatic.controls.clear();
                self.clear_turn_audio(state);
            }
        }
    }
    fn lose_attribution(
        &self,
        state: &mut RecordingState,
        automatic: &mut AutomaticTurns,
        reason: TurnRecordingLoss,
    ) {
        if !automatic.enabled() {
            return;
        }
        automatic.status = TurnRecordingStatus::Disabled {
            session_id: automatic.session_id,
            reason,
        };
        automatic.active = None;
        automatic.pending.clear();
        automatic.cuts.clear();
        automatic.seen.clear();
        automatic.controls.clear();
        self.clear_turn_audio(state);
        self.emit(RecordingEvent::TurnAttributionLost(reason));
        // A local failure may have no later tracker event to wake the receiver.
        // Keep the handle owned for cleanup, but release its subscription now.
        if let Ok(subscription) = self.subscription.lock() {
            if let Some(subscription) = subscription.as_ref() {
                subscription.0.abort();
            }
        }
    }
    pub(super) fn discard_disabled_turn_audio(&self, state: &mut RecordingState) {
        if state.automatic.as_ref().is_some_and(|a| !a.enabled()) {
            self.clear_turn_audio(state);
        }
    }
    pub(super) fn recording_restarted(&self, state: &mut RecordingState) {
        if let Some(mut automatic) = state.automatic.take() {
            if automatic.started_recording {
                self.lose_attribution(state, &mut automatic, TurnRecordingLoss::RecordingRestarted);
            }
            automatic.started_recording = true;
            state.automatic = Some(automatic);
        }
    }
    pub(super) fn enforce_turn_capacity(
        &self,
        state: &mut RecordingState,
        user: bool,
        size: usize,
    ) {
        let track = if user { &state.user } else { &state.bot };
        if self.turn_append_fits(track, size, state.sample_rate, user) {
            return;
        }
        if let Some(mut automatic) = state.automatic.take() {
            self.lose_attribution(state, &mut automatic, TurnRecordingLoss::TurnCapacity);
            state.automatic = Some(automatic);
        }
    }
    fn receive_turn_update(
        &self,
        state: &mut RecordingState,
        automatic: &mut AutomaticTurns,
        event: TurnRecordingUpdate,
    ) {
        if !automatic.enabled() {
            return;
        }
        let reason = match &event {
            TurnRecordingUpdate::Control { token: None, .. } => {
                Some(TurnRecordingLoss::MissingNativeBoundary)
            }
            TurnRecordingUpdate::Turn(TurnTrackingEvent::ObservationLoss { .. }) => {
                Some(TurnRecordingLoss::ObservationLoss)
            }
            TurnRecordingUpdate::Turn(TurnTrackingEvent::Invalidated { .. }) => {
                Some(TurnRecordingLoss::TrackerInvalidated)
            }
            TurnRecordingUpdate::Turn(TurnTrackingEvent::Started {
                numbering_complete: false,
                ..
            })
            | TurnRecordingUpdate::Turn(TurnTrackingEvent::Ended {
                numbering_complete: false,
                ..
            }) => Some(TurnRecordingLoss::ObservationLoss),
            TurnRecordingUpdate::Turn(TurnTrackingEvent::Started { boundary: None, .. })
            | TurnRecordingUpdate::Turn(TurnTrackingEvent::Ended { boundary: None, .. }) => {
                Some(TurnRecordingLoss::MissingNativeBoundary)
            }
            _ => None,
        };
        if let Some(reason) = reason {
            self.lose_attribution(state, automatic, reason);
            return;
        }
        if automatic.pending.len() == self.config.event_capacity {
            self.lose_attribution(state, automatic, TurnRecordingLoss::EventCapacity);
            return;
        }
        automatic.pending.push_back(event);
        self.resolve_turns(state, automatic);
    }
    fn resolve_turns(&self, state: &mut RecordingState, automatic: &mut AutomaticTurns) {
        while let Some(event) = automatic.pending.front() {
            if let TurnRecordingUpdate::Control { token, kind } = event {
                let Some(control) = automatic.controls.front() else {
                    return;
                };
                if token.as_ref() != Some(&control.token) || *kind != control.kind {
                    self.lose_attribution(
                        state,
                        automatic,
                        TurnRecordingLoss::InvalidBoundaryOrder,
                    );
                    return;
                }
                automatic.controls.pop_front();
                automatic.pending.pop_front();
                continue;
            }
            let TurnRecordingUpdate::Turn(event) = event else {
                unreachable!()
            };
            let (token, number, starting) = match event {
                TurnTrackingEvent::Started {
                    boundary: Some(token),
                    turn_number,
                    ..
                } => (token, *turn_number, true),
                TurnTrackingEvent::Ended {
                    boundary: Some(token),
                    turn_number,
                    ..
                } => (token, *turn_number, false),
                _ => unreachable!("non-boundary events are rejected before buffering"),
            };
            let Some(index) = automatic.cuts.iter().position(|cut| &cut.token == token) else {
                return;
            };
            let cut = &automatic.cuts[index];
            let counts = cut.user.checked_sub(automatic.user_base).and_then(|user| {
                cut.bot
                    .checked_sub(automatic.bot_base)
                    .map(|bot| (user, bot))
            });
            let Some((user, bot)) = counts.filter(|(user, bot)| {
                *user <= state.user.turn.len() as u64 && *bot <= state.bot.turn.len() as u64
            }) else {
                self.lose_attribution(state, automatic, TurnRecordingLoss::InvalidBoundaryOrder);
                return;
            };
            if (starting
                && (automatic.active.is_some()
                    || automatic.last_started.checked_add(1) != Some(number)))
                || (!starting && automatic.active != Some(number))
            {
                self.lose_attribution(state, automatic, TurnRecordingLoss::InvalidBoundaryOrder);
                return;
            }
            for (is_user, count, track) in [
                (true, user as usize, &mut state.user),
                (false, bot as usize, &mut state.bot),
            ] {
                let audio: Vec<u8> = track.turn.drain(..count).collect();
                if !starting && !audio.is_empty() {
                    let data = TurnAudioData {
                        turn_number: number,
                        session_id: Some(automatic.session_id),
                        audio: audio.into(),
                        sample_rate: state.sample_rate,
                        num_channels: 1,
                    };
                    self.emit(if is_user {
                        RecordingEvent::UserTurnAudio(data)
                    } else {
                        RecordingEvent::BotTurnAudio(data)
                    });
                }
            }
            automatic.user_base += user;
            automatic.bot_base += bot;
            automatic.active = starting.then_some(number);
            if starting {
                automatic.last_started = number;
            }
            automatic.pending.pop_front();
            // Retain the matched cut for the Ended/Started pair at a barge-in,
            // and to reject reprocessing of that same native logical frame.
            automatic.cuts.drain(..index);
        }
        self.finish_closed_stream(state, automatic);
    }
    pub(super) fn handle_native_frame(&self, frame: &Frame) -> Result<(), ProcessorError> {
        let now = self.clock.now();
        let mut state = self.lock()?;
        let boundary = matches!(
            frame.data,
            FrameData::Start(_)
                | FrameData::StartDetailed(_)
                | FrameData::UserStartedSpeaking
                | FrameData::BotStoppedSpeaking
                | FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
                | FrameData::Stop
        );
        let control = is_turn_recording_control(frame.data.source_class_name());
        if control {
            if let Some(mut automatic) = state.automatic.take() {
                if automatic.enabled() {
                    automatic.seen.retain(FrameObservationToken::is_alive);
                    let token = frame.observation_token();
                    if automatic.seen.contains(&token) {
                        self.lose_attribution(
                            &mut state,
                            &mut automatic,
                            TurnRecordingLoss::RepeatedBoundary,
                        );
                    } else if automatic.seen.len() == self.config.event_capacity {
                        self.lose_attribution(
                            &mut state,
                            &mut automatic,
                            TurnRecordingLoss::LineageCapacity,
                        );
                    } else {
                        automatic.seen.insert(token);
                    }
                }
                state.automatic = Some(automatic);
            }
        }
        let result = self.handle_locked(&mut state, &frame.data, now);
        let Some(mut automatic) = state.automatic.take() else {
            return result;
        };
        if result.is_err() {
            self.lose_attribution(
                &mut state,
                &mut automatic,
                TurnRecordingLoss::RecordingError,
            );
        }
        let mut result = result;
        if control && automatic.enabled() {
            if automatic.controls.len() == self.config.event_capacity {
                self.lose_attribution(
                    &mut state,
                    &mut automatic,
                    TurnRecordingLoss::ControlCapacity,
                );
            } else {
                automatic.controls.push_back(Control {
                    token: frame.observation_token(),
                    kind: frame.data.source_class_name(),
                });
            }
        }
        if boundary && automatic.enabled() {
            // A user interruption closes only the bot's current run. Keeping
            // the open user run preserves microphone pre-roll for the new turn.
            if matches!(frame.data, FrameData::UserStartedSpeaking) {
                if let Err(error) = self.drain_tail(&mut state, false, now) {
                    self.lose_attribution(
                        &mut state,
                        &mut automatic,
                        TurnRecordingLoss::RecordingError,
                    );
                    result = Err(error);
                } else {
                    Self::finish_run(&mut state.bot);
                    state.bot.speaking = false;
                }
            }
            let token = frame.observation_token();
            if automatic.cuts.iter().any(|cut| cut.token == token) {
                self.lose_attribution(
                    &mut state,
                    &mut automatic,
                    TurnRecordingLoss::RepeatedBoundary,
                );
            } else if automatic.cuts.len() == self.config.event_capacity {
                self.lose_attribution(
                    &mut state,
                    &mut automatic,
                    TurnRecordingLoss::BoundaryCapacity,
                );
            } else if automatic.enabled() {
                let offsets = automatic
                    .user_base
                    .checked_add(state.user.turn.len() as u64)
                    .zip(automatic.bot_base.checked_add(state.bot.turn.len() as u64));
                if let Some((user, bot)) = offsets {
                    automatic.cuts.push_back(Cut { token, user, bot });
                    automatic.graceful_terminal |= matches!(
                        frame.data,
                        FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop
                    );
                    self.resolve_turns(&mut state, &mut automatic);
                } else {
                    self.lose_attribution(
                        &mut state,
                        &mut automatic,
                        TurnRecordingLoss::InvalidBoundaryOrder,
                    );
                }
            }
        }
        if automatic.enabled() {
            self.resolve_turns(&mut state, &mut automatic);
        }
        state.automatic = Some(automatic);
        self.discard_disabled_turn_audio(&mut state);
        result
    }
    pub(super) async fn close_turn_subscription(&self) {
        let graceful = self.lock().ok().is_some_and(|state| {
            state
                .automatic
                .as_ref()
                .is_some_and(|a| a.graceful_terminal)
        });
        let subscription = self
            .subscription
            .lock()
            .ok()
            .and_then(|mut subscription| subscription.take());
        if let Some(mut subscription) = subscription {
            if !graceful
                || tokio::time::timeout(Duration::from_millis(100), &mut subscription.0)
                    .await
                    .is_err()
            {
                subscription.0.abort();
                let _ = (&mut subscription.0).await;
            }
        }
        if let Ok(mut state) = self.lock() {
            if let Some(mut automatic) = state.automatic.take() {
                self.lose_attribution(&mut state, &mut automatic, TurnRecordingLoss::Cleanup);
                state.automatic = Some(automatic);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_audio::encode_pcm16_le;
    use pipecat_observers::TurnEndReason;

    fn recorder() -> Arc<AudioBufferProcessor> {
        Arc::new(
            AudioBufferProcessor::new(AudioBufferConfig {
                sample_rate: Some(16000),
                enable_turn_audio: true,
                auto_start_recording: true,
                ..Default::default()
            })
            .unwrap(),
        )
    }
    fn receive(recorder: &AudioBufferProcessor, event: TurnTrackingEvent) {
        let mut state = recorder.lock().unwrap();
        let mut automatic = state.automatic.take().unwrap();
        let controls: Vec<_> = automatic
            .controls
            .iter()
            .map(|control| TurnRecordingUpdate::Control {
                token: Some(control.token.clone()),
                kind: control.kind,
            })
            .collect();
        for control in controls {
            recorder.receive_turn_update(&mut state, &mut automatic, control);
        }
        recorder.receive_turn_update(&mut state, &mut automatic, TurnRecordingUpdate::Turn(event));
        state.automatic = Some(automatic);
    }
    fn acknowledge_controls(recorder: &AudioBufferProcessor) {
        let mut state = recorder.lock().unwrap();
        let mut automatic = state.automatic.take().unwrap();
        let controls: Vec<_> = automatic
            .controls
            .iter()
            .map(|control| TurnRecordingUpdate::Control {
                token: Some(control.token.clone()),
                kind: control.kind,
            })
            .collect();
        for control in controls {
            recorder.receive_turn_update(&mut state, &mut automatic, control);
        }
        state.automatic = Some(automatic);
    }
    fn start_event(frame: &Frame) -> TurnTrackingEvent {
        TurnTrackingEvent::Started {
            turn_number: 1,
            boundary: Some(frame.observation_token()),
            timestamp_ns: 0,
            numbering_complete: true,
        }
    }
    #[tokio::test]
    async fn saturated_or_repeated_turn_numbers_cannot_attribute_another_turn() {
        for previous in [1, u64::MAX] {
            let tracker = TurnTrackingObserver::new();
            let recorder = recorder();
            recorder.attach_turn_tracker(&tracker).unwrap();
            let frame = Frame::start();
            recorder.handle_native_frame(&frame).unwrap();
            recorder
                .lock()
                .unwrap()
                .automatic
                .as_mut()
                .unwrap()
                .last_started = previous;
            let mut event = start_event(&frame);
            if let TurnTrackingEvent::Started { turn_number, .. } = &mut event {
                *turn_number = previous;
            }
            receive(&recorder, event);
            assert!(matches!(
                recorder.turn_recording_status().unwrap(),
                TurnRecordingStatus::Disabled {
                    reason: TurnRecordingLoss::InvalidBoundaryOrder,
                    ..
                }
            ));
        }
    }
    #[tokio::test]
    async fn expired_control_lineages_are_pruned_without_retaining_their_frames() {
        let tracker = TurnTrackingObserver::new();
        let recorder = Arc::new(
            AudioBufferProcessor::new(AudioBufferConfig {
                event_capacity: 1,
                sample_rate: Some(16000),
                enable_turn_audio: true,
                auto_start_recording: true,
                ..Default::default()
            })
            .unwrap(),
        );
        recorder.attach_turn_tracker(&tracker).unwrap();
        let start = Frame::start();
        recorder.handle_native_frame(&start).unwrap();
        receive(&recorder, start_event(&start));
        let token = start.observation_token();
        drop(start);
        assert!(!token.is_alive());
        for _ in 0..10 {
            let control = Frame::new(FrameData::UserStoppedSpeaking);
            recorder.handle_native_frame(&control).unwrap();
            acknowledge_controls(&recorder);
            let state = recorder.lock().unwrap();
            assert_eq!(state.automatic.as_ref().unwrap().seen.len(), 1);
            assert!(state.automatic.as_ref().unwrap().enabled());
        }
    }
    #[tokio::test]
    async fn retained_clone_exhausts_the_bounded_lineage_history() {
        let tracker = TurnTrackingObserver::new();
        let recorder = Arc::new(
            AudioBufferProcessor::new(AudioBufferConfig {
                event_capacity: 1,
                sample_rate: Some(16000),
                enable_turn_audio: true,
                auto_start_recording: true,
                ..Default::default()
            })
            .unwrap(),
        );
        recorder.attach_turn_tracker(&tracker).unwrap();
        let start = Frame::start();
        let retained = start.clone();
        recorder.handle_native_frame(&start).unwrap();
        receive(&recorder, start_event(&start));
        drop(start);
        assert!(retained.observation_token().is_alive());
        recorder
            .handle_native_frame(&Frame::new(FrameData::BotStartedSpeaking))
            .unwrap();
        assert!(matches!(
            recorder.turn_recording_status().unwrap(),
            TurnRecordingStatus::Disabled {
                reason: TurnRecordingLoss::LineageCapacity,
                ..
            }
        ));
        assert!(recorder
            .lock()
            .unwrap()
            .automatic
            .as_ref()
            .unwrap()
            .seen
            .is_empty());
    }
    #[tokio::test]
    async fn dropping_recorder_aborts_its_weak_owned_subscription() {
        let tracker = TurnTrackingObserver::new();
        let recorder = recorder();
        recorder.attach_turn_tracker(&tracker).unwrap();
        let weak = Arc::downgrade(&recorder);
        let abort = recorder
            .subscription
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .0
            .abort_handle();
        drop(recorder);
        assert!(weak.upgrade().is_none());
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(abort.is_finished());
        assert!(!tracker.stats().closed);
    }
    #[tokio::test]
    async fn local_failure_aborts_an_idle_subscription_without_a_later_tracker_event() {
        let tracker = TurnTrackingObserver::new();
        let recorder = recorder();
        recorder.attach_turn_tracker(&tracker).unwrap();
        let abort = recorder
            .subscription
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .0
            .abort_handle();
        recorder.start_recording().unwrap();
        recorder.stop_recording().unwrap();
        recorder.start_recording().unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(abort.is_finished());
        assert!(!tracker.stats().closed);
    }
    #[tokio::test]
    async fn serialized_boundary_cannot_attribute_native_audio() {
        let tracker = TurnTrackingObserver::new();
        let recorder = recorder();
        recorder.attach_turn_tracker(&tracker).unwrap();
        let frame = Frame::start();
        recorder.handle_native_frame(&frame).unwrap();
        let wire = serde_json::to_string(&start_event(&frame)).unwrap();
        assert!(!wire.contains("boundary"));
        let restored = serde_json::from_str(&wire).unwrap();
        receive(&recorder, restored);
        assert!(matches!(
            recorder.turn_recording_status().unwrap(),
            TurnRecordingStatus::Disabled {
                reason: TurnRecordingLoss::MissingNativeBoundary,
                ..
            }
        ));
    }
    #[tokio::test]
    async fn terminal_event_and_stream_close_wait_for_the_exact_native_cut() {
        let tracker = TurnTrackingObserver::new();
        let recorder = recorder();
        let mut events = recorder.subscribe();
        recorder.attach_turn_tracker(&tracker).unwrap();
        let start = Frame::start();
        recorder.handle_native_frame(&start).unwrap();
        receive(&recorder, start_event(&start));
        recorder
            .handle_native_frame(&Frame::new(FrameData::UserStartedSpeaking))
            .unwrap();
        recorder
            .handle_native_frame(&Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(encode_pcm16_le(&[1, 2]), 16000, 1).unwrap(),
            )))
            .unwrap();
        let end = Frame::end();
        receive(
            &recorder,
            TurnTrackingEvent::Ended {
                turn_number: 1,
                boundary: Some(end.observation_token()),
                started_ns: 0,
                timestamp_ns: 1,
                duration_seconds: 1e-9,
                was_interrupted: true,
                reason: TurnEndReason::End,
                numbering_complete: true,
            },
        );
        {
            let mut state = recorder.lock().unwrap();
            let mut automatic = state.automatic.take().unwrap();
            automatic.stream_closed = true;
            recorder.finish_closed_stream(&mut state, &mut automatic);
            state.automatic = Some(automatic);
        }
        assert!(matches!(
            recorder.turn_recording_status().unwrap(),
            TurnRecordingStatus::Attached { .. }
        ));
        recorder.handle_native_frame(&end).unwrap();
        assert!(matches!(
            recorder.turn_recording_status().unwrap(),
            TurnRecordingStatus::Closed { .. }
        ));
        let mut turns = vec![];
        while let Ok(event) = events.try_recv() {
            if let RecordingEvent::UserTurnAudio(data) = event {
                turns.push(data);
            }
        }
        assert_eq!(turns.len(), 1);
        assert_eq!(&*turns[0].audio, encode_pcm16_le(&[1, 2]));
    }
    #[tokio::test]
    async fn unmatched_events_are_bounded_and_recording_restart_is_fail_closed() {
        let tracker = TurnTrackingObserver::new();
        let recorder = Arc::new(
            AudioBufferProcessor::new(AudioBufferConfig {
                event_capacity: 1,
                enable_turn_audio: true,
                sample_rate: Some(16000),
                ..Default::default()
            })
            .unwrap(),
        );
        recorder.attach_turn_tracker(&tracker).unwrap();
        receive(&recorder, start_event(&Frame::start()));
        receive(&recorder, start_event(&Frame::start()));
        assert!(matches!(
            recorder.turn_recording_status().unwrap(),
            TurnRecordingStatus::Disabled {
                reason: TurnRecordingLoss::EventCapacity,
                ..
            }
        ));
        let state = recorder.lock().unwrap();
        assert!(state.automatic.as_ref().unwrap().pending.is_empty());
        drop(state);
        let another = super::tests::recorder();
        another.attach_turn_tracker(&tracker).unwrap();
        another.start_recording().unwrap();
        another.stop_recording().unwrap();
        another.start_recording().unwrap();
        assert!(matches!(
            another.turn_recording_status().unwrap(),
            TurnRecordingStatus::Disabled {
                reason: TurnRecordingLoss::RecordingRestarted,
                ..
            }
        ));
    }
}
