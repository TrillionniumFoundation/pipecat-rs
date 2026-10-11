//! Bounded source-session authority for native eager input. Tokens have passive
//! Drop; only the source owner can withdraw a session. No wire representation
//! or runtime-owning callback is retained here.
use super::{Inner, NativeSpeculation, SpeculationOwner, SpeculationStatus};
use crate::{Direction, FrameData, InvocationFence, ProcessorContext, ProcessorError};
use std::sync::{Arc, Mutex, Weak};

const MAX_RECORDS: usize = 64;
const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
struct PayloadBudget(usize);
impl std::io::Write for PayloadBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.0 {
            return Err(std::io::Error::other(
                "native eager source payload exceeds its byte limit",
            ));
        }
        self.0 -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct NativeEagerConsumer(Arc<()>);
impl NativeEagerConsumer {
    pub fn new() -> Self {
        Self::default()
    }
    fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl std::fmt::Debug for NativeEagerConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeEagerConsumer(<local>)")
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Open,
    Draining,
    NormallyClosed,
    Revoked,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum PredictionStatus {
    Live,
    Revoked,
    Transferred,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum TurnStatus {
    Active,
    FinalOnly,
    Retired,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum StopDisposition {
    NotOffered,
    Final(u64),
    Empty { claimed: bool },
}
struct TurnEntry {
    id: u64,
    record: Weak<TurnRecord>,
    status: TurnStatus,
    consumer: Option<NativeEagerConsumer>,
    stop: StopDisposition,
}
struct PredictionEntry {
    id: u64,
    turn: u64,
    record: Weak<PredictionRecord>,
    status: PredictionStatus,
    consumer: Option<NativeEagerConsumer>,
    child: Weak<Inner>,
}
struct CommitEntry {
    id: u64,
    turn: u64,
    record: Weak<CommitRecord>,
    prediction: Option<u64>,
    consumer: Option<NativeEagerConsumer>,
    final_claimed: bool,
    stop_claimed: bool,
    consumed: bool,
}
struct SourceState {
    phase: Phase,
    next_id: u64,
    current_turn: Option<u64>,
    current_prediction: Option<u64>,
    turns: Vec<TurnEntry>,
    predictions: Vec<PredictionEntry>,
    commits: Vec<CommitEntry>,
}
struct Source {
    state: Mutex<SourceState>,
    freshness: Option<(
        InvocationFence,
        crate::health::WeakProcessorHealth,
        crate::ProcessorHealthGeneration,
    )>,
    max_payload_bytes: usize,
}
impl Source {
    fn observe(
        &self,
    ) -> Option<(
        crate::processor::InvocationObservation,
        crate::ProcessorHealth,
    )> {
        let (fence, health, _) = self.freshness.as_ref()?;
        Some((fence.observe()?, health.upgrade()?))
    }
    // Queue generations use published atomic observations; health uses a short
    // watch read guard. Neither acquires runtime admission or a queue lock.
    fn live(
        &self,
        state: &SourceState,
        observation: &Option<(
            crate::processor::InvocationObservation,
            crate::ProcessorHealth,
        )>,
    ) -> bool {
        state.phase != Phase::Revoked
            && (state.phase == Phase::NormallyClosed
                || self.freshness.as_ref().is_none_or(|(_, _, generation)| {
                    observation.as_ref().is_some_and(|(fence, health)| {
                        fence.is_current()
                            && generation.snapshot.is_usable
                            && health.generation() == *generation
                    })
                }))
    }
    fn reserve(state: &mut SourceState) -> Result<u64, ProcessorError> {
        state
            .commits
            .retain(|entry| entry.record.strong_count() != 0);
        state.predictions.retain(|entry| {
            entry.record.strong_count() != 0
                || entry.child.strong_count() != 0
                || state
                    .commits
                    .iter()
                    .any(|commit| commit.prediction == Some(entry.id))
        });
        state.turns.retain(|entry| {
            entry.record.strong_count() != 0
                || state
                    .predictions
                    .iter()
                    .any(|prediction| prediction.turn == entry.id)
                || state.commits.iter().any(|commit| commit.turn == entry.id)
        });
        if state.turns.len() + state.predictions.len() + state.commits.len() >= MAX_RECORDS {
            return Err(ProcessorError::new(
                "native eager source record capacity exceeded",
            ));
        }
        let id = state.next_id;
        state.next_id = id
            .checked_add(1)
            .ok_or_else(|| ProcessorError::new("native eager source identity exhausted"))?;
        Ok(id)
    }
    fn turn_admits_data(state: &SourceState, id: u64) -> bool {
        state
            .turns
            .iter()
            .find(|entry| entry.id == id)
            .is_some_and(|turn| match turn.status {
                TurnStatus::Active => true,
                TurnStatus::FinalOnly => match turn.stop {
                    StopDisposition::Final(commit) => state.commits.iter().any(|entry| {
                        entry.id == commit && !entry.consumed && entry.record.strong_count() != 0
                    }),
                    _ => false,
                },
                TurnStatus::Retired => false,
            })
    }
    fn start_admitted(state: &SourceState, id: u64, consumer: &NativeEagerConsumer) -> bool {
        state
            .turns
            .iter()
            .find(|entry| entry.id == id)
            .and_then(|entry| entry.consumer.as_ref())
            .is_some_and(|bound| bound.same(consumer))
    }
    fn turn_accepts_production(state: &SourceState, id: u64) -> bool {
        state.current_turn == Some(id)
            && state.turns.iter().any(|entry| {
                entry.id == id
                    && entry.status == TurnStatus::Active
                    && entry.stop == StopDisposition::NotOffered
            })
    }
    fn validate_payload(&self, payload: &FrameData, eager: bool) -> Result<(), ProcessorError> {
        let text = match (eager, payload) {
            (true, FrameData::EagerTranscription(value))
            | (false, FrameData::Transcription(value)) => &value.text,
            (true, FrameData::EagerTranscriptionDetailed(value))
            | (false, FrameData::TranscriptionDetailed(value)) => &value.text.text,
            _ => {
                return Err(ProcessorError::new(
                    "native eager source payload role mismatch",
                ))
            }
        };
        if eager && text.trim().is_empty() {
            return Err(ProcessorError::new(
                "native eager prediction requires nonempty text",
            ));
        }
        // The bound includes result JSON, timestamps and speaker metadata, not
        // only the transcript. Serialization here never changes retained bytes.
        serde_json::to_writer(PayloadBudget(self.max_payload_bytes), payload)
            .map_err(|error| ProcessorError::new(error.to_string()))?;
        Ok(())
    }
}

/// One source socket/lifecycle owner. Normal completion preserves offered final
/// receipts; restart, mute, fault and abandoned ownership call destructive revoke.
/// Local identities reflect trusted receive order, not absent provider turn IDs.
pub struct EagerSourceSessionOwner(Arc<Source>);
impl std::fmt::Debug for EagerSourceSessionOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EagerSourceSessionOwner(<local>)")
    }
}
impl EagerSourceSessionOwner {
    pub fn new(max_payload_bytes: usize) -> Result<Self, ProcessorError> {
        Self::construct(max_payload_bytes, None)
    }
    pub fn from_context(
        max_payload_bytes: usize,
        context: &ProcessorContext,
    ) -> Result<Self, ProcessorError> {
        Self::construct(
            max_payload_bytes,
            Some((
                context.invocation_fence()?,
                context.health(),
                context.health().generation(),
            )),
        )
    }
    fn construct(
        max_payload_bytes: usize,
        freshness: Option<(
            InvocationFence,
            crate::ProcessorHealth,
            crate::ProcessorHealthGeneration,
        )>,
    ) -> Result<Self, ProcessorError> {
        if max_payload_bytes == 0 || max_payload_bytes > MAX_PAYLOAD_BYTES {
            return Err(ProcessorError::new(
                "native eager source payload bound requires 1..1048576 bytes",
            ));
        }
        Ok(Self(Arc::new(Source {
            state: Mutex::new(SourceState {
                phase: Phase::Open,
                next_id: 1,
                current_turn: None,
                current_prediction: None,
                turns: vec![],
                predictions: vec![],
                commits: vec![],
            }),
            freshness: freshness
                .map(|(fence, health, generation)| (fence, health.downgrade(), generation)),
            max_payload_bytes,
        })))
    }
    /// Begin a fresh locally interpreted turn. Capacity is reserved before the
    /// prior turn is retired, so a rejected start cannot revoke existing work.
    pub fn begin_turn(&self) -> Result<NativeEagerTurn, ProcessorError> {
        let observation = self.0.observe();
        let (turn, retired) = {
            let mut state = self
                .0
                .state
                .lock()
                .map_err(|_| ProcessorError::fatal("native eager source lock poisoned"))?;
            if state.phase != Phase::Open || !self.0.live(&state, &observation) {
                return Err(ProcessorError::new("native eager source is not open"));
            }
            let id = Source::reserve(&mut state)?;
            let old = state.current_turn;
            if let Some(entry) = state.turns.iter_mut().find(|entry| Some(entry.id) == old) {
                entry.status = TurnStatus::Retired;
            }
            let retired = state
                .predictions
                .iter_mut()
                .filter_map(|entry| {
                    if Some(entry.turn) == old && entry.status == PredictionStatus::Live {
                        entry.status = PredictionStatus::Revoked;
                        Some(entry.child.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let turn = NativeEagerTurn(Arc::new(TurnRecord {
                source: self.0.clone(),
                id,
            }));
            state.turns.push(TurnEntry {
                id,
                record: Arc::downgrade(&turn.0),
                status: TurnStatus::Active,
                consumer: None,
                stop: StopDisposition::NotOffered,
            });
            state.current_turn = Some(id);
            state.current_prediction = None;
            (turn, retired)
        };
        notify_cancel(retired);
        Ok(turn)
    }
    pub fn predict(
        &self,
        turn: &NativeEagerTurn,
        payload: FrameData,
    ) -> Result<NativeEagerPrediction, ProcessorError> {
        self.0.validate_payload(&payload, true)?;
        // Retain bounded content, not arbitrary spare String/Vec capacity from
        // the native caller. All admitted transcript fields have passive Clone.
        let compact = payload.clone();
        drop(payload);
        let payload = compact;
        let observation = self.0.observe();
        let (prediction, retired) = {
            let mut state = self
                .0
                .state
                .lock()
                .map_err(|_| ProcessorError::fatal("native eager source lock poisoned"))?;
            if !Arc::ptr_eq(&self.0, &turn.0.source)
                || state.phase != Phase::Open
                || !self.0.live(&state, &observation)
                || !Source::turn_accepts_production(&state, turn.0.id)
            {
                return Err(ProcessorError::new("native eager source is not open"));
            }
            let id = Source::reserve(&mut state)?;
            let retired = state
                .current_prediction
                .take()
                .and_then(|old| state.predictions.iter_mut().find(|entry| entry.id == old))
                .map(|entry| {
                    entry.status = PredictionStatus::Revoked;
                    entry.child.clone()
                });
            let prediction = NativeEagerPrediction(Arc::new(PredictionRecord {
                source: self.0.clone(),
                turn: turn.clone(),
                id,
                payload,
            }));
            state.predictions.push(PredictionEntry {
                id,
                turn: turn.0.id,
                record: Arc::downgrade(&prediction.0),
                status: PredictionStatus::Live,
                consumer: None,
                child: Weak::new(),
            });
            state.current_prediction = Some(id);
            (prediction, retired)
        };
        notify_cancel(retired);
        Ok(prediction)
    }
    /// Withdraw only this exact prediction; the listening source remains open.
    pub fn resume(&self, prediction: &NativeEagerPrediction) -> bool {
        if !Arc::ptr_eq(&self.0, &prediction.0.source) {
            return false;
        }
        let retired = {
            let Ok(mut state) = self.0.state.lock() else {
                return false;
            };
            if state.current_turn != Some(prediction.0.turn.0.id)
                || state.current_prediction != Some(prediction.0.id)
            {
                return false;
            }
            let Some(entry) = state.predictions.iter_mut().find(|entry| {
                entry.id == prediction.0.id && entry.status == PredictionStatus::Live
            }) else {
                return false;
            };
            entry.status = PredictionStatus::Revoked;
            let retired = entry.child.clone();
            if state.current_prediction == Some(prediction.0.id) {
                state.current_prediction = None;
            }
            retired
        };
        notify_cancel([retired]);
        true
    }
    /// Offer one cumulative final for the current locally interpreted turn.
    /// Offering does not confirm speculation or consume the downstream decision.
    pub fn offer_final(
        &self,
        turn: &NativeEagerTurn,
        payload: FrameData,
    ) -> Result<NativeEagerCommit, ProcessorError> {
        self.0.validate_payload(&payload, false)?;
        let compact = payload.clone();
        drop(payload);
        let payload = compact;
        let observation = self.0.observe();
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("native eager source lock poisoned"))?;
        if !Arc::ptr_eq(&self.0, &turn.0.source)
            || !matches!(state.phase, Phase::Open | Phase::Draining)
            || !self.0.live(&state, &observation)
            || !Source::turn_accepts_production(&state, turn.0.id)
        {
            return Err(ProcessorError::new(
                "native eager source cannot offer a final",
            ));
        }
        let id = Source::reserve(&mut state)?;
        let prediction = state.current_prediction.take();
        let commit = NativeEagerCommit(Arc::new(CommitRecord {
            source: self.0.clone(),
            turn: turn.clone(),
            id,
            prediction,
            payload,
        }));
        state.commits.push(CommitEntry {
            id,
            turn: turn.0.id,
            record: Arc::downgrade(&commit.0),
            prediction,
            consumer: None,
            final_claimed: false,
            stop_claimed: false,
            consumed: false,
        });
        state
            .turns
            .iter_mut()
            .find(|entry| entry.id == turn.0.id)
            .unwrap()
            .stop = StopDisposition::Final(id);
        Ok(commit)
    }
    /// Record a source-authorized empty End without inventing transcript data.
    pub fn offer_empty_stop(&self, turn: &NativeEagerTurn) -> Result<(), ProcessorError> {
        let observation = self.0.observe();
        let retired = {
            let mut state = self
                .0
                .state
                .lock()
                .map_err(|_| ProcessorError::fatal("native eager source lock poisoned"))?;
            if !Arc::ptr_eq(&self.0, &turn.0.source)
                || !matches!(state.phase, Phase::Open | Phase::Draining)
                || !self.0.live(&state, &observation)
                || !Source::turn_accepts_production(&state, turn.0.id)
            {
                return Err(ProcessorError::new(
                    "native eager source cannot offer an empty stop",
                ));
            }
            state
                .turns
                .iter_mut()
                .find(|entry| entry.id == turn.0.id)
                .unwrap()
                .stop = StopDisposition::Empty { claimed: false };
            state
                .current_prediction
                .take()
                .and_then(|id| state.predictions.iter_mut().find(|entry| entry.id == id))
                .map(|entry| {
                    entry.status = PredictionStatus::Revoked;
                    entry.child.clone()
                })
        };
        notify_cancel(retired);
        Ok(())
    }
    pub fn begin_drain(&self) -> bool {
        let observation = self.0.observe();
        let Ok(mut state) = self.0.state.lock() else {
            return false;
        };
        if state.phase != Phase::Open || !self.0.live(&state, &observation) {
            return false;
        }
        state.phase = Phase::Draining;
        true
    }
    /// Disarm mechanical actor EOF cancellation while preserving offered finals.
    /// This does not prevent a later explicit destructive revoke.
    pub fn complete_drain(&self) -> bool {
        let observation = self.0.observe();
        let retired = {
            let Ok(mut state) = self.0.state.lock() else {
                return false;
            };
            if state.phase != Phase::Draining || !self.0.live(&state, &observation) {
                return false;
            }
            state.phase = Phase::NormallyClosed;
            state.current_prediction = None;
            let final_turns: Vec<_> = state
                .commits
                .iter()
                .filter(|entry| !entry.consumed && entry.record.strong_count() != 0)
                .map(|entry| entry.turn)
                .collect();
            for turn in &mut state.turns {
                if turn.status == TurnStatus::Active {
                    turn.status = if final_turns.contains(&turn.id) {
                        TurnStatus::FinalOnly
                    } else {
                        TurnStatus::Retired
                    };
                }
            }
            let preserved: Vec<_> = state
                .commits
                .iter()
                .filter(|entry| {
                    !entry.consumed
                        && entry.record.strong_count() != 0
                        && Source::turn_admits_data(&state, entry.turn)
                })
                .filter_map(|entry| entry.prediction)
                .collect();
            state
                .predictions
                .iter_mut()
                .filter_map(|entry| {
                    if entry.status == PredictionStatus::Live && !preserved.contains(&entry.id) {
                        entry.status = PredictionStatus::Revoked;
                        Some(entry.child.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        notify_cancel(retired);
        true
    }
    fn mark_revoked(&self) -> Vec<Weak<Inner>> {
        let Ok(mut state) = self.0.state.lock() else {
            return vec![];
        };
        state.phase = Phase::Revoked;
        state.current_prediction = None;
        for turn in &mut state.turns {
            turn.status = TurnStatus::Retired;
        }
        state
            .predictions
            .iter_mut()
            .filter_map(|entry| {
                if entry.status == PredictionStatus::Live {
                    entry.status = PredictionStatus::Revoked;
                    Some(entry.child.clone())
                } else {
                    None
                }
            })
            .collect()
    }
    pub fn revoke(&self) {
        notify_cancel(self.mark_revoked());
    }
}
impl Drop for EagerSourceSessionOwner {
    fn drop(&mut self) {
        let normal = self
            .0
            .state
            .lock()
            .is_ok_and(|state| state.phase == Phase::NormallyClosed);
        if !normal {
            self.revoke();
        }
    }
}
fn notify_cancel(children: impl IntoIterator<Item = Weak<Inner>>) {
    for child in children {
        if let Some(child) = child.upgrade() {
            NativeSpeculation(child).cancel();
        }
    }
}

struct TurnRecord {
    source: Arc<Source>,
    id: u64,
}
/// Passive identity for one local source turn. Owning a token does not grant
/// Start replay, manufacture a stop, or retain a runtime or task.
#[derive(Clone)]
pub struct NativeEagerTurn(Arc<TurnRecord>);
impl std::fmt::Debug for NativeEagerTurn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeEagerTurn(<local>)")
    }
}
impl PartialEq for NativeEagerTurn {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for NativeEagerTurn {}
impl NativeEagerTurn {
    /// Any untransferred input rights, including already-offered final delivery
    /// after normal EOF. This is not permission for empty-turn recovery.
    pub fn is_live(&self) -> bool {
        let observation = self.0.source.observe();
        self.0.source.state.lock().is_ok_and(|state| {
            self.0.source.live(&state, &observation) && Source::turn_admits_data(&state, self.0.id)
        })
    }
    /// Effective ordinary completion/recovery authority. Normal EOF preserves
    /// only offered-final rights; destructive retirement preserves neither.
    pub fn is_active(&self) -> bool {
        let observation = self.0.source.observe();
        self.0.source.state.lock().is_ok_and(|state| {
            self.0.source.live(&state, &observation)
                && state
                    .turns
                    .iter()
                    .any(|entry| entry.id == self.0.id && entry.status == TurnStatus::Active)
        })
    }
    /// Claim Start once across every clone, for one exact downstream consumer.
    pub fn claim_start(&self, consumer: &NativeEagerConsumer) -> bool {
        let observation = self.0.source.observe();
        let Ok(mut state) = self.0.source.state.lock() else {
            return false;
        };
        if !self.0.source.live(&state, &observation) || !Source::turn_admits_data(&state, self.0.id)
        {
            return false;
        }
        let Some(entry) = state.turns.iter_mut().find(|entry| entry.id == self.0.id) else {
            return false;
        };
        if entry.consumer.is_some() {
            return false;
        }
        entry.consumer = Some(consumer.clone());
        true
    }
    pub fn claim_stop(
        &self,
        consumer: &NativeEagerConsumer,
        commit: Option<&NativeEagerCommit>,
    ) -> bool {
        match commit {
            Some(commit) => self == commit.turn() && commit.claim_stop(consumer),
            None => self.claim_empty_stop(consumer),
        }
    }
    /// A passive turn token cannot authorize this until the owner offered the
    /// exact empty-End disposition. The once-only claim keeps recovery active.
    pub fn claim_empty_stop(&self, consumer: &NativeEagerConsumer) -> bool {
        let observation = self.0.source.observe();
        let Ok(mut state) = self.0.source.state.lock() else {
            return false;
        };
        if !self.0.source.live(&state, &observation)
            || !Source::start_admitted(&state, self.0.id, consumer)
        {
            return false;
        }
        let Some(entry) = state.turns.iter_mut().find(|entry| entry.id == self.0.id) else {
            return false;
        };
        if entry.status != TurnStatus::Active
            || entry.stop != (StopDisposition::Empty { claimed: false })
        {
            return false;
        }
        entry.stop = StopDisposition::Empty { claimed: true };
        true
    }
}

struct PredictionRecord {
    source: Arc<Source>,
    turn: NativeEagerTurn,
    id: u64,
    payload: FrameData,
}
/// Capability origin retains only lease/identity, never the transcript payload.
pub(super) struct SourceOrigin {
    source: Arc<Source>,
    turn: NativeEagerTurn,
    id: u64,
}
impl SourceOrigin {
    fn new(prediction: &NativeEagerPrediction) -> Self {
        Self {
            source: prediction.0.source.clone(),
            turn: prediction.0.turn.clone(),
            id: prediction.0.id,
        }
    }
    pub(super) fn matches(&self, prediction: &NativeEagerPrediction) -> bool {
        Arc::ptr_eq(&self.source, &prediction.0.source) && self.id == prediction.0.id
    }
    pub(super) fn is_live(&self) -> bool {
        self.with_live(|live| live).unwrap_or(false)
    }
    pub(super) fn with_live<T>(&self, operation: impl FnOnce(bool) -> T) -> Option<T> {
        let observation = self.source.observe();
        let state = self.source.state.lock().ok()?;
        let entry = state.predictions.iter().find(|entry| entry.id == self.id)?;
        let live = entry.status == PredictionStatus::Transferred
            || (entry.status == PredictionStatus::Live
                && self.source.live(&state, &observation)
                && Source::turn_admits_data(&state, self.turn.0.id));
        Some(operation(live))
    }
}
#[derive(Clone)]
pub struct NativeEagerPrediction(Arc<PredictionRecord>);
impl std::fmt::Debug for NativeEagerPrediction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeEagerPrediction(<local>)")
    }
}
impl PartialEq for NativeEagerPrediction {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for NativeEagerPrediction {}
impl NativeEagerPrediction {
    pub fn turn(&self) -> &NativeEagerTurn {
        &self.0.turn
    }
    pub fn payload(&self) -> &FrameData {
        &self.0.payload
    }
    pub fn text(&self) -> &str {
        text(&self.0.payload)
    }
    pub fn is_live(&self) -> bool {
        self.with_live(|live| live).unwrap_or(false)
    }
    pub(super) fn with_live<T>(&self, operation: impl FnOnce(bool) -> T) -> Option<T> {
        SourceOrigin::new(self).with_live(operation)
    }
}
struct CommitRecord {
    source: Arc<Source>,
    turn: NativeEagerTurn,
    id: u64,
    prediction: Option<u64>,
    payload: FrameData,
}
#[derive(Clone)]
pub struct NativeEagerCommit(Arc<CommitRecord>);
impl std::fmt::Debug for NativeEagerCommit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeEagerCommit(<local>)")
    }
}
impl PartialEq for NativeEagerCommit {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for NativeEagerCommit {}
impl NativeEagerCommit {
    pub fn turn(&self) -> &NativeEagerTurn {
        &self.0.turn
    }
    pub fn payload(&self) -> &FrameData {
        &self.0.payload
    }
    pub fn text(&self) -> &str {
        text(&self.0.payload)
    }
    pub fn matches_prediction(&self, prediction: &NativeEagerPrediction) -> bool {
        Arc::ptr_eq(&self.0.source, &prediction.0.source)
            && self.0.turn == prediction.0.turn
            && self.0.prediction == Some(prediction.0.id)
    }
    /// Whether this receipt belongs to a prediction already adopted by this
    /// consumer. Retained receipts keep that small provenance after token Drop.
    pub fn was_adopted_by(&self, consumer: &NativeEagerConsumer) -> bool {
        self.0.source.state.lock().is_ok_and(|state| {
            self.0
                .prediction
                .and_then(|id| state.predictions.iter().find(|entry| entry.id == id))
                .and_then(|entry| entry.consumer.as_ref())
                .is_some_and(|bound| bound.same(consumer))
        })
    }
    pub fn is_live(&self) -> bool {
        let observation = self.0.source.observe();
        self.0.source.state.lock().is_ok_and(|state| {
            self.0.source.live(&state, &observation)
                && Source::turn_admits_data(&state, self.0.turn.0.id)
                && state
                    .commits
                    .iter()
                    .any(|entry| entry.id == self.0.id && !entry.consumed)
        })
    }
    /// Claim staging once after this consumer's exact Start admission. Retired
    /// inputs cannot create staging or controller ownership.
    pub fn claim_final(&self, consumer: &NativeEagerConsumer) -> bool {
        self.claim_role(consumer, false)
    }
    /// Claim the matching stop role once, even when it overtakes its final.
    pub fn claim_stop(&self, consumer: &NativeEagerConsumer) -> bool {
        self.claim_role(consumer, true)
    }
    fn claim_role(&self, consumer: &NativeEagerConsumer, stop: bool) -> bool {
        let observation = self.0.source.observe();
        let Ok(mut state) = self.0.source.state.lock() else {
            return false;
        };
        if !self.0.source.live(&state, &observation)
            || !Source::turn_admits_data(&state, self.0.turn.0.id)
            || !Source::start_admitted(&state, self.0.turn.0.id, consumer)
            || !state.turns.iter().any(|entry| {
                entry.id == self.0.turn.0.id && entry.stop == StopDisposition::Final(self.0.id)
            })
        {
            return false;
        }
        if self
            .0
            .prediction
            .and_then(|id| state.predictions.iter().find(|entry| entry.id == id))
            .and_then(|entry| entry.consumer.as_ref())
            .is_some_and(|bound| !bound.same(consumer))
        {
            return false;
        }
        let Some(entry) = state.commits.iter_mut().find(|entry| entry.id == self.0.id) else {
            return false;
        };
        if (if stop {
            entry.stop_claimed
        } else {
            entry.final_claimed
        }) || entry.consumed
            || entry
                .consumer
                .as_ref()
                .is_some_and(|bound| !bound.same(consumer))
        {
            return false;
        }
        entry.consumer = Some(consumer.clone());
        if stop {
            entry.stop_claimed = true;
        } else {
            entry.final_claimed = true;
        }
        true
    }
    /// Admit an ordinary final exactly once. Source retirement before this
    /// observation denies it; retirement afterward cannot roll it back.
    pub fn consume_fallback(&self, consumer: &NativeEagerConsumer) -> bool {
        let observation = self.0.source.observe();
        let Ok(mut state) = self.0.source.state.lock() else {
            return false;
        };
        if !self.0.source.live(&state, &observation)
            || !Source::turn_admits_data(&state, self.0.turn.0.id)
            || !Source::start_admitted(&state, self.0.turn.0.id, consumer)
        {
            return false;
        }
        let Some(entry) = state.commits.iter_mut().find(|entry| entry.id == self.0.id) else {
            return false;
        };
        if entry.consumed
            || !entry.final_claimed
            || !entry
                .consumer
                .as_ref()
                .is_some_and(|bound| bound.same(consumer))
        {
            return false;
        }
        entry.consumed = true;
        state
            .turns
            .iter_mut()
            .find(|entry| entry.id == self.0.turn.0.id)
            .unwrap()
            .status = TurnStatus::Retired;
        true
    }
}
fn text(payload: &FrameData) -> &str {
    match payload {
        FrameData::EagerTranscription(value) | FrameData::Transcription(value) => &value.text,
        FrameData::EagerTranscriptionDetailed(value) | FrameData::TranscriptionDetailed(value) => {
            &value.text.text
        }
        _ => unreachable!("validated source transcript payload"),
    }
}

/// Downstream-only source authority. Public frame IDs and broadcast sibling IDs
/// are intentionally not authority; duplicated tokens share one decision right.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeEagerSource {
    Start(NativeEagerTurn),
    Prediction(NativeEagerPrediction),
    Cancel(NativeEagerPrediction),
    Final(NativeEagerCommit),
    Stop {
        turn: NativeEagerTurn,
        commit: Option<NativeEagerCommit>,
    },
}
impl NativeEagerSource {
    pub(crate) fn validate(
        &self,
        payload: &FrameData,
        direction: Option<Direction>,
    ) -> Result<(), ProcessorError> {
        if direction == Some(Direction::Upstream) {
            return Err(ProcessorError::new(
                "native eager source authority is downstream-only",
            ));
        }
        let valid = match self {
            Self::Start(_) => matches!(
                payload,
                FrameData::UserStartedSpeaking | FrameData::ProposedUserStartedSpeaking
            ),
            Self::Prediction(prediction) => prediction.payload() == payload,
            Self::Final(commit) => commit.payload() == payload,
            Self::Cancel(_) => matches!(payload, FrameData::EagerEndOfTurnCancel),
            Self::Stop { turn, commit } => {
                commit.as_ref().is_none_or(|commit| commit.turn() == turn)
                    && matches!(
                        payload,
                        FrameData::UserStoppedSpeaking | FrameData::ProposedUserStoppedSpeaking
                    )
            }
        };
        if valid {
            Ok(())
        } else {
            Err(ProcessorError::new(
                "native eager source payload does not match its receipt",
            ))
        }
    }
}

impl SpeculationOwner {
    /// Atomically reserve adoption and register the actual aggregator-owned
    /// capability. Validation precedes replacement of any existing prediction.
    pub fn try_adopt_source_prediction(
        prediction: &NativeEagerPrediction,
        consumer: &NativeEagerConsumer,
    ) -> Option<Self> {
        let owner = Self::with_origin(Some(SourceOrigin::new(prediction)));
        let observation = prediction.0.source.observe();
        let adopted = {
            let mut state = prediction.0.source.state.lock().ok()?;
            if !prediction.0.source.live(&state, &observation)
                || !Source::turn_admits_data(&state, prediction.0.turn.0.id)
                || !Source::start_admitted(&state, prediction.0.turn.0.id, consumer)
            {
                return None;
            }
            if state
                .commits
                .iter()
                .filter(|entry| entry.prediction == Some(prediction.0.id))
                .any(|entry| {
                    entry.consumed
                        || entry
                            .consumer
                            .as_ref()
                            .is_some_and(|bound| !bound.same(consumer))
                })
            {
                return None;
            }
            let entry = state
                .predictions
                .iter_mut()
                .find(|entry| entry.id == prediction.0.id)?;
            if entry.status != PredictionStatus::Live || entry.consumer.is_some() {
                false
            } else {
                entry.consumer = Some(consumer.clone());
                entry.child = Arc::downgrade(&owner.capability.0);
                true
            }
        };
        adopted.then_some(owner)
    }
    /// The successful L→C transition consumes this exact final and transfers
    /// authority. Source-only revocation cannot cancel the capability afterward.
    pub fn confirm_source_with_context(
        &self,
        commit: &NativeEagerCommit,
        consumer: &NativeEagerConsumer,
        context: crate::LlmContextData,
    ) -> bool {
        let Some(origin) = self.capability.0.origin.as_ref() else {
            return false;
        };
        if !Arc::ptr_eq(&commit.0.source, &origin.source)
            || commit.0.turn != origin.turn
            || commit.0.prediction != Some(origin.id)
        {
            return false;
        }
        let observation = origin.source.observe();
        let confirmed = {
            let Ok(mut lease) = origin.source.state.lock() else {
                return false;
            };
            if !origin.source.live(&lease, &observation)
                || !Source::turn_admits_data(&lease, origin.turn.0.id)
                || !Source::start_admitted(&lease, origin.turn.0.id, consumer)
            {
                return false;
            }
            let Some(prediction_index) = lease.predictions.iter().position(|entry| {
                entry.id == origin.id
                    && entry.status == PredictionStatus::Live
                    && entry
                        .consumer
                        .as_ref()
                        .is_some_and(|bound| bound.same(consumer))
                    && Weak::ptr_eq(&entry.child, &Arc::downgrade(&self.capability.0))
            }) else {
                return false;
            };
            let Some(commit_index) = lease.commits.iter().position(|entry| {
                entry.id == commit.0.id
                    && !entry.consumed
                    && entry.final_claimed
                    && entry.stop_claimed
                    && entry
                        .consumer
                        .as_ref()
                        .is_some_and(|bound| bound.same(consumer))
            }) else {
                return false;
            };
            let Ok(mut state) = self.capability.0.state.lock() else {
                return false;
            };
            if state.status != SpeculationStatus::Pending
                || !super::bound_admission_current(&state)
                || state
                    .admission
                    .as_ref()
                    .is_some_and(|(_, health, _)| !health.snapshot.is_usable)
            {
                return false;
            }
            state.confirmed_context = Some(context);
            state.status = SpeculationStatus::Confirmed;
            lease.commits[commit_index].consumed = true;
            lease.predictions[prediction_index].status = PredictionStatus::Transferred;
            lease
                .turns
                .iter_mut()
                .find(|entry| entry.id == origin.turn.0.id)
                .unwrap()
                .status = TurnStatus::Retired;
            true
        };
        if confirmed {
            self.changed.send_replace(());
        }
        confirmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Frame, FrameProcessor, Pipeline, PipelineConfig, SpeculationStatus, TranscriptionFrame,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    fn eager(value: &str) -> FrameData {
        FrameData::EagerTranscription(TranscriptionFrame {
            text: value.into(),
            user_id: "speaker".into(),
            timestamp: "t".into(),
            ..Default::default()
        })
    }
    fn final_text(value: &str) -> FrameData {
        FrameData::Transcription(TranscriptionFrame {
            text: value.into(),
            user_id: "speaker".into(),
            timestamp: "t".into(),
            finalized: true,
            ..Default::default()
        })
    }
    fn session() -> EagerSourceSessionOwner {
        EagerSourceSessionOwner::new(4096).unwrap()
    }
    fn empty_context() -> crate::LlmContextData {
        crate::LlmContextData {
            messages: vec![],
            tools: crate::ToolSet::Functions(vec![]),
            tool_choice: None,
        }
    }
    fn claims(commit: &NativeEagerCommit, consumer: &NativeEagerConsumer) {
        assert!(commit.claim_final(consumer));
        assert!(commit.claim_stop(consumer));
    }
    fn begin(source: &EagerSourceSessionOwner, consumer: &NativeEagerConsumer) -> NativeEagerTurn {
        let turn = source.begin_turn().unwrap();
        assert!(turn.claim_start(consumer));
        turn
    }
    #[test]
    fn exact_start_is_required_once_before_prediction_final_and_stop_admission() {
        let source = session();
        let foreign = session();
        let consumer = NativeEagerConsumer::new();
        let other = NativeEagerConsumer::new();
        let turn = source.begin_turn().unwrap();
        let clone = turn.clone();
        assert_eq!(turn, clone);
        let foreign_turn = foreign.begin_turn().unwrap();
        assert_ne!(turn, foreign_turn);
        assert!(source.predict(&foreign_turn, eager("foreign")).is_err());
        assert!(source
            .offer_final(&foreign_turn, final_text("foreign"))
            .is_err());
        assert!(source.offer_empty_stop(&foreign_turn).is_err());
        let prediction = source.predict(&turn, eager("hello")).unwrap();
        let commit = source.offer_final(&turn, final_text("hello")).unwrap();
        assert_eq!(prediction.turn(), &turn);
        assert_eq!(commit.turn(), &turn);
        assert!(SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).is_none());
        assert!(!commit.claim_final(&consumer));
        assert!(!turn.claim_stop(&consumer, Some(&commit)));
        assert!(!commit.consume_fallback(&consumer));
        assert!(turn.claim_start(&consumer));
        assert!(!clone.claim_start(&consumer));
        assert!(!clone.claim_start(&other));
        assert!(SpeculationOwner::try_adopt_source_prediction(&prediction, &other).is_none());
        assert!(!commit.claim_final(&other));
        assert!(!commit.claim_stop(&other));
        assert!(!foreign_turn.claim_stop(&consumer, Some(&commit)));
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        claims(&commit, &consumer);
        assert!(owner.confirm_source_with_context(&commit, &consumer, empty_context()));
        assert!(!clone.claim_start(&consumer));
    }
    #[test]
    fn empty_stop_requires_owner_disposition_same_start_and_once_only_claim() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let other = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("withdrawn")).unwrap();
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        assert!(
            !turn.claim_stop(&consumer, None),
            "token possession is not an End"
        );
        assert!(source.offer_empty_stop(&turn).is_ok());
        assert!(!prediction.is_live());
        assert_eq!(owner.capability().status(), SpeculationStatus::Cancelled);
        assert!(
            turn.is_active(),
            "healthy empty End permits ordinary recovery"
        );
        assert!(!turn.claim_empty_stop(&other));
        assert!(turn.claim_stop(&consumer, None));
        assert!(!turn.clone().claim_stop(&consumer, None));
        assert!(source.offer_empty_stop(&turn).is_err());
        assert!(source.offer_final(&turn, final_text("too late")).is_err());
        assert!(source.predict(&turn, eager("too late")).is_err());
        let next = source.begin_turn().unwrap();
        assert!(!turn.is_active());
        assert!(source.offer_empty_stop(&next).is_ok());
        assert!(
            !next.claim_empty_stop(&consumer),
            "Start must precede an empty Stop"
        );
        assert!(next.claim_start(&consumer));
        assert!(next.claim_empty_stop(&consumer));
        let final_turn = begin(&source, &consumer);
        let receipt = source
            .offer_final(&final_turn, final_text("result"))
            .unwrap();
        assert!(source.offer_empty_stop(&final_turn).is_err());
        assert!(source
            .offer_final(&final_turn, final_text("duplicate"))
            .is_err());
        assert!(!final_turn.claim_empty_stop(&consumer));
        assert!(final_turn.claim_stop(&consumer, Some(&receipt)));
    }
    #[test]
    fn supersession_retires_unadopted_receipts_empty_stops_and_no_result_turns() {
        for disposition in 0..3 {
            let source = session();
            let consumer = NativeEagerConsumer::new();
            let a = begin(&source, &consumer);
            let receipt = match disposition {
                0 => Some(source.offer_final(&a, final_text("A")).unwrap()),
                1 => {
                    source.offer_empty_stop(&a).unwrap();
                    None
                }
                _ => None,
            };
            let b = begin(&source, &consumer);
            assert_ne!(a, b);
            assert!(!a.is_live());
            assert!(!a.is_active());
            assert!(!a.claim_start(&consumer));
            assert!(!a.claim_stop(&consumer, receipt.as_ref()));
            if let Some(receipt) = receipt {
                assert!(!receipt.claim_final(&consumer));
                assert!(!receipt.consume_fallback(&consumer));
            }
            assert!(b.is_active());
            let own = source.offer_final(&b, final_text("B")).unwrap();
            claims(&own, &consumer);
            assert!(own.consume_fallback(&consumer));
            source.revoke();
            assert!(!own.consume_fallback(&consumer));
        }
    }
    #[test]
    fn no_result_turn_recovery_rights_follow_source_lifetime_without_receipt_arrival() {
        for normal_eof in [false, true] {
            let source = session();
            let consumer = NativeEagerConsumer::new();
            let turn = begin(&source, &consumer);
            assert!(turn.is_active());
            if normal_eof {
                assert!(source.begin_drain());
                assert!(source.complete_drain());
            } else {
                source.revoke();
            }
            assert!(!turn.is_live());
            assert!(!turn.is_active());
            assert!(!turn.claim_stop(&consumer, None));
            assert!(!turn.claim_start(&consumer));
        }
        let source = session();
        let turn = source.begin_turn().unwrap();
        drop(source);
        assert!(!turn.is_active());
        assert!(!turn.claim_start(&NativeEagerConsumer::new()));
    }
    #[test]
    fn failed_shared_capacity_reservation_preserves_current_receipt_and_bound_transfer() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let mut held = Vec::new();
        for _ in 0..(MAX_RECORDS - 3) {
            held.push(source.begin_turn().unwrap());
        }
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("current")).unwrap();
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        let receipt = source.offer_final(&turn, final_text("current")).unwrap();
        assert!(source.begin_turn().is_err());
        assert!(turn.is_active());
        assert!(receipt.is_live());
        assert_eq!(owner.capability().status(), SpeculationStatus::Pending);
        claims(&receipt, &consumer);
        assert!(owner.confirm_source_with_context(&receipt, &consumer, empty_context()));
        held.clear();
        let replacement = begin(&source, &consumer);
        assert!(replacement.is_active());
        assert_eq!(owner.capability().status(), SpeculationStatus::Confirmed);
        source.revoke();
        assert_eq!(owner.capability().status(), SpeculationStatus::Confirmed);
        assert!(owner.capability().release_after_boundary());
    }
    #[test]
    fn bounded_turn_identity_survives_child_or_receipt_without_prediction_payload() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let identity = Arc::downgrade(&turn.0);
        let prediction = source.predict(&turn, eager("held")).unwrap();
        let payload = Arc::downgrade(&prediction.0);
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        let receipt = source.offer_final(&turn, final_text("held")).unwrap();
        claims(&receipt, &consumer);
        assert!(owner.confirm_source_with_context(&receipt, &consumer, empty_context()));
        drop(turn);
        drop(prediction);
        assert_eq!(payload.strong_count(), 0);
        assert_ne!(identity.strong_count(), 0);
        let replacement = source.begin_turn().unwrap();
        assert_eq!(owner.capability().status(), SpeculationStatus::Confirmed);
        drop(owner);
        assert_ne!(
            identity.strong_count(),
            0,
            "receipt retains exact turn identity"
        );
        drop(receipt);
        assert_eq!(identity.strong_count(), 0);
        drop(replacement);
        let _fresh = source.begin_turn().unwrap();
        let state = source.0.state.lock().unwrap();
        assert_eq!(state.turns.len(), 1);
        assert!(state.predictions.is_empty());
        assert!(state.commits.is_empty());
    }
    #[test]
    fn normal_eof_dropped_receipt_cannot_restore_unrelated_prediction_or_recovery() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("queued")).unwrap();
        let receipt = source.offer_final(&turn, final_text("queued")).unwrap();
        assert!(source.begin_drain());
        assert!(source.complete_drain());
        assert!(turn.is_live());
        assert!(!turn.is_active());
        drop(receipt);
        assert!(!turn.is_live());
        assert!(!turn.is_active());
        assert!(!prediction.is_live());
        assert!(SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).is_none());
    }
    #[test]
    fn ordinary_consumption_observes_retirement_after_live_role_claims() {
        for retire_before in [false, true] {
            let source = session();
            let consumer = NativeEagerConsumer::new();
            let turn = begin(&source, &consumer);
            let receipt = source.offer_final(&turn, final_text("ordinary")).unwrap();
            claims(&receipt, &consumer);
            if retire_before {
                source.revoke();
            }
            assert_eq!(receipt.consume_fallback(&consumer), !retire_before);
            source.revoke();
            let state = source.0.state.lock().unwrap();
            assert_eq!(
                state
                    .commits
                    .iter()
                    .find(|entry| entry.id == receipt.0.id)
                    .unwrap()
                    .consumed,
                !retire_before,
                "later retirement cannot roll back successful admission"
            );
        }
    }
    #[test]
    fn aligned_live_sessions_require_exact_receipt_identity_for_transfer() {
        let a = session();
        let b = session();
        let consumer = NativeEagerConsumer::new();
        let turn_a = begin(&a, &consumer);
        let turn_b = begin(&b, &consumer);
        let prediction_a = a.predict(&turn_a, eager("same")).unwrap();
        let prediction_b = b.predict(&turn_b, eager("same")).unwrap();
        let owner_a =
            SpeculationOwner::try_adopt_source_prediction(&prediction_a, &consumer).unwrap();
        let owner_b =
            SpeculationOwner::try_adopt_source_prediction(&prediction_b, &consumer).unwrap();
        let receipt_a = a.offer_final(&turn_a, final_text("same")).unwrap();
        let receipt_b = b.offer_final(&turn_b, final_text("same")).unwrap();
        claims(&receipt_a, &consumer);
        claims(&receipt_b, &consumer);
        assert_eq!(prediction_a.0.id, prediction_b.0.id);
        assert_eq!(receipt_a.0.id, receipt_b.0.id);
        assert!(!owner_a.confirm_source_with_context(&receipt_b, &consumer, empty_context()));
        assert!(!owner_b.confirm_source_with_context(&receipt_a, &consumer, empty_context()));
        assert!(receipt_a.is_live());
        assert!(receipt_b.is_live());
        assert!(owner_a.confirm_source_with_context(&receipt_a, &consumer, empty_context()));
        assert!(owner_b.confirm_source_with_context(&receipt_b, &consumer, empty_context()));
    }
    #[test]
    fn all_source_carriers_reject_wire_and_seal_boundary_role_and_direction() {
        let source = session();
        let turn = source.begin_turn().unwrap();
        let prediction = source.predict(&turn, eager("hello")).unwrap();
        let receipt = source.offer_final(&turn, final_text("hello")).unwrap();
        let cases = vec![
            (
                FrameData::UserStartedSpeaking,
                NativeEagerSource::Start(turn.clone()),
            ),
            (
                FrameData::ProposedUserStartedSpeaking,
                NativeEagerSource::Start(turn.clone()),
            ),
            (
                prediction.payload().clone(),
                NativeEagerSource::Prediction(prediction.clone()),
            ),
            (
                FrameData::EagerEndOfTurnCancel,
                NativeEagerSource::Cancel(prediction),
            ),
            (
                receipt.payload().clone(),
                NativeEagerSource::Final(receipt.clone()),
            ),
            (
                FrameData::UserStoppedSpeaking,
                NativeEagerSource::Stop {
                    turn: turn.clone(),
                    commit: Some(receipt.clone()),
                },
            ),
            (
                FrameData::ProposedUserStoppedSpeaking,
                NativeEagerSource::Stop {
                    turn: turn.clone(),
                    commit: Some(receipt),
                },
            ),
            (
                FrameData::UserStoppedSpeaking,
                NativeEagerSource::Stop {
                    turn: turn.clone(),
                    commit: None,
                },
            ),
            (
                FrameData::ProposedUserStoppedSpeaking,
                NativeEagerSource::Stop { turn, commit: None },
            ),
        ];
        for (payload, carrier) in cases {
            let (mut down, up) = Frame::new(payload.clone()).broadcast_pair();
            down.set_native_eager_source(carrier.clone()).unwrap();
            assert!(up.native_eager_source().is_none());
            assert!(serde_json::to_vec(&up).is_ok());
            assert!(serde_json::to_vec(&down).is_err());
            assert!(down.require_no_native_llm_continuation().is_err());
            assert!(down
                .clone()
                .take_native_eager_source(Direction::Upstream)
                .is_err());
            let mut changed = down.duplicate();
            changed.data = match payload {
                FrameData::UserStartedSpeaking => FrameData::ProposedUserStartedSpeaking,
                FrameData::ProposedUserStartedSpeaking => FrameData::UserStartedSpeaking,
                FrameData::UserStoppedSpeaking => FrameData::ProposedUserStoppedSpeaking,
                FrameData::ProposedUserStoppedSpeaking => FrameData::UserStoppedSpeaking,
                _ => FrameData::LlmRun,
            };
            assert!(changed
                .take_native_eager_source(Direction::Downstream)
                .is_err());
            assert_eq!(
                down.take_native_eager_source(Direction::Downstream)
                    .unwrap(),
                Some(carrier)
            );
            assert!(serde_json::to_vec(&down).is_ok());
        }
        let wrong_source = session();
        let wrong_turn = wrong_source.begin_turn().unwrap();
        let wrong_receipt = wrong_source
            .offer_final(&wrong_turn, final_text("wrong"))
            .unwrap();
        let actual = source.begin_turn().unwrap();
        let mut mismatched = Frame::new(FrameData::UserStoppedSpeaking);
        assert!(mismatched
            .set_native_eager_source(NativeEagerSource::Stop {
                turn: actual,
                commit: Some(wrong_receipt),
            })
            .is_err());
    }
    #[test]
    fn resume_and_source_retirement_before_adoption_deny_replayed_predictions() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let old = source.predict(&turn, eager("same")).unwrap();
        assert!(source.resume(&old));
        assert!(
            turn.is_active(),
            "Resume does not retire recovery authority"
        );
        assert!(SpeculationOwner::try_adopt_source_prediction(&old, &consumer).is_none());
        let current = source.predict(&turn, eager("same")).unwrap();
        source.revoke();
        assert!(!turn.is_active());
        assert!(SpeculationOwner::try_adopt_source_prediction(&current, &consumer).is_none());
    }
    #[test]
    fn adoption_is_once_only_consumer_bound_and_validated_before_replacement() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let other = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("hello")).unwrap();
        let commit = source.offer_final(&turn, final_text("hello")).unwrap();
        claims(&commit, &consumer);
        assert!(SpeculationOwner::try_adopt_source_prediction(&prediction, &other).is_none());
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        assert!(
            SpeculationOwner::try_adopt_source_prediction(&prediction.clone(), &consumer).is_none()
        );
        assert!(!owner.confirm());
        assert!(!owner.confirm_with_context(empty_context()));
        assert!(!owner.confirm_source_with_context(&commit, &other, empty_context()));
        assert!(owner.confirm_source_with_context(&commit, &consumer, empty_context()));
        assert!(!owner.confirm_source_with_context(&commit, &consumer, empty_context()));
        assert!(!commit.consume_fallback(&consumer));
    }
    #[test]
    fn effective_revoke_precedes_delayed_child_notification() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("hello")).unwrap();
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        let capability = owner.capability();
        let commit = source.offer_final(&turn, final_text("hello")).unwrap();
        claims(&commit, &consumer);
        let deferred = source.mark_revoked();
        assert_eq!(
            capability.0.state.lock().unwrap().status,
            SpeculationStatus::Pending
        );
        assert_eq!(capability.status(), SpeculationStatus::Cancelled);
        assert!(!capability.source_is_current());
        assert!(!capability.admits_completion());
        assert!(!capability.claim_inference());
        assert!(!capability.admit_tool_intent());
        assert!(!capability.release_after_boundary());
        assert!(!owner.confirm_source_with_context(&commit, &consumer, empty_context()));
        assert!(!commit.consume_fallback(&consumer));
        notify_cancel(deferred);
        assert_eq!(
            capability.0.state.lock().unwrap().status,
            SpeculationStatus::Cancelled
        );
    }
    #[test]
    fn old_cancel_cannot_withdraw_replacement_and_equal_text_is_not_receipt_identity() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let old = source.predict(&turn, eager("hello")).unwrap();
        let old_owner = SpeculationOwner::try_adopt_source_prediction(&old, &consumer).unwrap();
        let current = source.predict(&turn, eager("hello")).unwrap();
        let owner = SpeculationOwner::try_adopt_source_prediction(&current, &consumer).unwrap();
        assert!(!source.resume(&old));
        assert_eq!(
            old_owner.capability().status(),
            SpeculationStatus::Cancelled
        );
        assert_eq!(owner.capability().status(), SpeculationStatus::Pending);
        let other = session();
        let other_turn = begin(&other, &consumer);
        let wrong = other.offer_final(&other_turn, final_text("hello")).unwrap();
        claims(&wrong, &consumer);
        assert!(!owner.confirm_source_with_context(&wrong, &consumer, empty_context()));
        let correct = source.offer_final(&turn, final_text("hello")).unwrap();
        claims(&correct, &consumer);
        assert!(owner.confirm_source_with_context(&correct, &consumer, empty_context()));
    }
    #[test]
    fn transferred_authority_survives_source_revoke_until_real_owner_cancels() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("hello")).unwrap();
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        let capability = owner.capability();
        let receipt = source.offer_final(&turn, final_text("hello")).unwrap();
        claims(&receipt, &consumer);
        assert!(owner.confirm_source_with_context(&receipt, &consumer, empty_context()));
        source.revoke();
        drop(source);
        assert_eq!(capability.status(), SpeculationStatus::Confirmed);
        assert!(capability.source_is_current());
        assert!(capability.claim_inference());
        assert!(capability.release_after_boundary());
        drop(owner);
        assert_eq!(capability.status(), SpeculationStatus::Cancelled);
    }
    #[test]
    fn normal_drain_preserves_only_offered_final_rights_including_queued_start() {
        for offer in [false, true] {
            let source = session();
            let consumer = NativeEagerConsumer::new();
            let turn = source.begin_turn().unwrap();
            let prediction = source.predict(&turn, eager("offered")).unwrap();
            let receipt = offer.then(|| source.offer_final(&turn, final_text("offered")).unwrap());
            assert!(source.begin_drain());
            assert!(source.predict(&turn, eager("too late")).is_err());
            assert!(source.complete_drain());
            assert!(
                !turn.is_active(),
                "normal EOF cannot admit unrelated recovery"
            );
            assert!(source.begin_turn().is_err());
            assert!(source.offer_empty_stop(&turn).is_err());
            drop(source);
            assert_eq!(turn.is_live(), offer);
            assert_eq!(prediction.is_live(), offer);
            assert_eq!(turn.claim_start(&consumer), offer);
            assert!(!turn.claim_empty_stop(&consumer));
            if let Some(receipt) = receipt {
                assert!(receipt.is_live());
                let owner =
                    SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
                claims(&receipt, &consumer);
                assert!(owner.confirm_source_with_context(&receipt, &consumer, empty_context()));
                assert_eq!(owner.capability().status(), SpeculationStatus::Confirmed);
            } else {
                assert!(
                    SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).is_none()
                );
            }
        }
    }
    #[test]
    fn fallback_is_exact_once_and_retired_role_claims_are_rejected() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let receipt = source.offer_final(&turn, final_text("ordinary")).unwrap();
        assert!(receipt.claim_stop(&consumer));
        assert!(!receipt.claim_stop(&consumer));
        assert!(receipt.claim_final(&consumer));
        assert!(!receipt.claim_final(&consumer));
        assert!(receipt.consume_fallback(&consumer));
        assert!(!receipt.is_live());
        assert!(!receipt.consume_fallback(&consumer));
        let turn = begin(&source, &consumer);
        let retired = source.offer_final(&turn, final_text("retired")).unwrap();
        source.revoke();
        assert!(!retired.claim_final(&consumer));
        assert!(!retired.claim_stop(&consumer));
        assert!(!retired.consume_fallback(&consumer));
    }
    #[test]
    fn receipt_retains_consumer_provenance_without_retaining_prediction_payload() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let other = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("old")).unwrap();
        let payload = Arc::downgrade(&prediction.0);
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        let receipt = source.offer_final(&turn, final_text("old")).unwrap();
        drop(prediction);
        assert_eq!(
            payload.strong_count(),
            0,
            "capability origin must not retain transcript payload"
        );
        assert_eq!(owner.capability().status(), SpeculationStatus::Pending);
        drop(owner);
        assert!(receipt.was_adopted_by(&consumer));
        assert!(!receipt.claim_final(&other));
        assert!(receipt.claim_final(&consumer));
        let next = begin(&source, &consumer);
        let _new = source.predict(&next, eager("new")).unwrap();
        assert!(receipt.was_adopted_by(&consumer));
        assert!(!receipt.is_live());
    }
    #[test]
    fn same_session_supersession_retires_prior_receipt_before_replacement_transfer() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let first_turn = begin(&source, &consumer);
        let first = source.predict(&first_turn, eager("same")).unwrap();
        let first_owner = SpeculationOwner::try_adopt_source_prediction(&first, &consumer).unwrap();
        let first_receipt = source.offer_final(&first_turn, final_text("same")).unwrap();
        claims(&first_receipt, &consumer);
        let second_turn = begin(&source, &consumer);
        let second = source.predict(&second_turn, eager("same")).unwrap();
        let second_owner =
            SpeculationOwner::try_adopt_source_prediction(&second, &consumer).unwrap();
        let second_receipt = source
            .offer_final(&second_turn, final_text("same"))
            .unwrap();
        claims(&second_receipt, &consumer);
        assert!(!first_owner.confirm_source_with_context(
            &second_receipt,
            &consumer,
            empty_context()
        ));
        assert!(second_receipt.is_live());
        assert!(!first_owner.confirm_source_with_context(
            &first_receipt,
            &consumer,
            empty_context()
        ));
        assert!(second_owner.confirm_source_with_context(
            &second_receipt,
            &consumer,
            empty_context()
        ));
    }
    #[test]
    fn payload_budget_does_not_retain_caller_spare_capacity() {
        let source = session();
        let turn = source.begin_turn().unwrap();
        let mut text = String::with_capacity(1024 * 1024);
        text.push_str("small");
        let mut result = Vec::with_capacity(16384);
        result.push(serde_json::Value::String("exact+/==".into()));
        let make = |eager| {
            let payload = TranscriptionFrame {
                text: text.clone(),
                result: Some(serde_json::Value::Array(result.clone())),
                ..Default::default()
            };
            if eager {
                FrameData::EagerTranscription(payload)
            } else {
                FrameData::Transcription(payload)
            }
        };
        // Restore excessive caller capacity after constructing each owned frame.
        for eager in [true, false] {
            let mut payload = make(eager);
            let transcript = match &mut payload {
                FrameData::EagerTranscription(value) | FrameData::Transcription(value) => value,
                _ => unreachable!(),
            };
            transcript.text.reserve(1024 * 1024);
            if let Some(serde_json::Value::Array(values)) = &mut transcript.result {
                values.reserve(16384);
            }
            let check = |retained: &FrameData| {
                let transcript = match retained {
                    FrameData::EagerTranscription(value) | FrameData::Transcription(value) => value,
                    _ => unreachable!(),
                };
                assert_eq!(transcript.text, "small");
                assert!(transcript.text.capacity() <= 4096);
                let Some(serde_json::Value::Array(values)) = &transcript.result else {
                    panic!("retained exact result");
                };
                assert_eq!(*values, vec![serde_json::Value::String("exact+/==".into())]);
                assert!(values.capacity() <= 4096);
            };
            if eager {
                check(source.predict(&turn, payload).unwrap().payload());
            } else {
                check(source.offer_final(&turn, payload).unwrap().payload());
            }
        }
    }
    #[test]
    fn bounded_passive_retention_releases_capacity_without_a_preadoption_timer() {
        let source = session();
        let turn = source.begin_turn().unwrap();
        let mut retained = Vec::new();
        for _ in 1..MAX_RECORDS {
            retained.push(source.predict(&turn, eager("queued")).unwrap());
        }
        let current = retained.last().unwrap().clone();
        assert!(source.predict(&turn, eager("over capacity")).is_err());
        assert!(source.begin_turn().is_err());
        assert!(
            turn.is_active(),
            "failed reservation must not retire the old turn"
        );
        assert!(
            current.is_live(),
            "capacity failure must not replace current prediction"
        );
        retained.clear();
        assert!(source.predict(&turn, eager("new")).is_ok());
    }
    #[test]
    fn payload_role_direction_wire_and_duplicate_rights_are_enforced() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("hello")).unwrap();
        let mut frame = Frame::new(prediction.payload().clone());
        frame
            .set_native_eager_source(NativeEagerSource::Prediction(prediction.clone()))
            .unwrap();
        assert!(serde_json::to_vec(&frame).is_err());
        assert!(frame.require_no_native_llm_continuation().is_err());
        let mut duplicate = frame.duplicate();
        assert!(duplicate
            .take_native_eager_source(Direction::Upstream)
            .is_err());
        duplicate.data = eager("mutated");
        assert!(duplicate
            .take_native_eager_source(Direction::Downstream)
            .is_err());
        let token = frame
            .take_native_eager_source(Direction::Downstream)
            .unwrap()
            .unwrap();
        assert_eq!(token, NativeEagerSource::Prediction(prediction.clone()));
        let _owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        assert!(
            SpeculationOwner::try_adopt_source_prediction(&prediction.clone(), &consumer).is_none()
        );
        let receipt = source.offer_final(&turn, final_text("hello")).unwrap();
        let (mut down, up) = Frame::new(FrameData::UserStoppedSpeaking).broadcast_pair();
        down.set_native_eager_source(NativeEagerSource::Stop {
            turn,
            commit: Some(receipt),
        })
        .unwrap();
        assert!(up.native_eager_source().is_none());
        assert!(serde_json::to_vec(&up).is_ok());
        let mut changed = down.clone();
        changed.data = FrameData::ProposedUserStoppedSpeaking;
        assert!(changed
            .take_native_eager_source(Direction::Downstream)
            .is_err());
        assert!(down
            .take_native_eager_source(Direction::Downstream)
            .unwrap()
            .is_some());
    }
    #[test]
    fn queued_source_carrier_drop_does_not_acquire_lease() {
        let (sent, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let source = session();
            let turn = source.begin_turn().unwrap();
            let prediction = source.predict(&turn, eager("queued")).unwrap();
            let mut frame = Frame::new(prediction.payload().clone());
            frame
                .set_native_eager_source(NativeEagerSource::Prediction(prediction))
                .unwrap();
            let mut start = Frame::new(FrameData::ProposedUserStartedSpeaking);
            start
                .set_native_eager_source(NativeEagerSource::Start(turn.clone()))
                .unwrap();
            let mut stop = Frame::new(FrameData::ProposedUserStoppedSpeaking);
            stop.set_native_eager_source(NativeEagerSource::Stop { turn, commit: None })
                .unwrap();
            let queue = crate::FrameQueue::new(3);
            queue
                .try_send(crate::DirectedFrame::new(frame, Direction::Downstream))
                .unwrap();
            queue
                .try_send(crate::DirectedFrame::new(start, Direction::Downstream))
                .unwrap();
            queue
                .try_send(crate::DirectedFrame::new(stop, Direction::Downstream))
                .unwrap();
            let lease = source.0.state.lock().unwrap();
            // Reset drops the normal prediction and proposed Stop; queue Drop
            // below drops the remaining system Start while the same L is held.
            assert_eq!(queue.reset_interruptible(), 2);
            drop(queue);
            drop(lease);
            sent.send(()).unwrap();
        });
        received
            .recv_timeout(Duration::from_secs(2))
            .expect("queued carrier Drop reentered lease");
    }
    #[test]
    fn queued_capability_drop_does_not_wake_watchers_after_owner_retirement() {
        use std::future::Future;
        use std::task::{Context, Wake, Waker};
        struct Count(std::sync::atomic::AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let owner = SpeculationOwner::new();
        let capability = owner.capability();
        let mut watch = capability.watch();
        let count = Arc::new(Count(std::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(count.clone());
        let mut context = Context::from_waker(&waker);
        let mut waiting = Box::pin(watch.changed());
        assert!(waiting.as_mut().poll(&mut context).is_pending());
        let mut frame = Frame::new(FrameData::LlmRun);
        frame.set_native_speculation(capability).unwrap();
        let queue = crate::FrameQueue::new(1);
        queue
            .try_send(crate::DirectedFrame::new(frame, Direction::Downstream))
            .unwrap();
        drop(owner);
        let after_owner = count.0.load(Ordering::SeqCst);
        assert!(after_owner > 0);
        queue.close();
        drop(queue);
        assert_eq!(count.0.load(Ordering::SeqCst), after_owner);
    }
    #[test]
    fn self_cancel_preserves_current_error_authority_but_source_retirement_does_not() {
        let source = session();
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("hello")).unwrap();
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        let capability = owner.capability();
        capability.cancel();
        assert!(capability.source_is_current());
        assert!(!capability.admits_completion());
        source.revoke();
        assert!(!capability.source_is_current());
    }

    #[derive(Default)]
    struct Capture(Mutex<Option<ProcessorContext>>);
    #[crate::async_trait]
    impl FrameProcessor for Capture {
        fn name(&self) -> &str {
            "source-lease-capture"
        }
        async fn setup(
            &self,
            _: &crate::StartFrame,
            context: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            *self.0.lock().unwrap() = Some(context.clone());
            Ok(())
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            context: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            context.push(frame, direction).await
        }
    }
    struct DropObserver {
        source: Mutex<Option<Weak<Source>>>,
        dropped: Arc<AtomicBool>,
    }
    #[crate::async_trait]
    impl crate::PipelineObserver for DropObserver {
        async fn on_event(&self, _: crate::ObservationEvent) {}
    }
    impl Drop for DropObserver {
        fn drop(&mut self) {
            if let Some(source) = self
                .source
                .get_mut()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade)
            {
                assert!(
                    source.state.try_lock().is_ok(),
                    "last runtime owner destroyed under source lease"
                );
            }
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn last_runtime_owner_is_destroyed_after_source_lease_guard() {
        let capture = Arc::new(Capture::default());
        let dropped = Arc::new(AtomicBool::new(false));
        let observer = Arc::new(DropObserver {
            source: Mutex::new(None),
            dropped: dropped.clone(),
        });
        let mut pipeline = Pipeline::new(vec![capture.clone()]);
        pipeline.add_observer(observer.clone(), Default::default());
        let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
        worker.recv().await.unwrap();
        let context = capture.0.lock().unwrap().take().unwrap();
        let source = EagerSourceSessionOwner::from_context(4096, &context).unwrap();
        let turn = source.begin_turn().unwrap();
        let prediction = source.predict(&turn, eager("held")).unwrap();
        *observer.source.lock().unwrap() = Some(Arc::downgrade(&source.0));
        drop(observer);
        let runtime = Arc::downgrade(&context.runtime);
        worker.end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        drop(capture);
        assert_eq!(
            runtime.strong_count(),
            1,
            "only captured context retains ended runtime"
        );
        prediction
            .with_live(|live| {
                assert!(!live);
                drop(context);
                assert_eq!(
                    runtime.strong_count(),
                    1,
                    "observation pins runtime through locked read"
                );
                assert!(!dropped.load(Ordering::SeqCst));
            })
            .unwrap();
        assert_eq!(runtime.strong_count(), 0);
        assert!(dropped.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn health_observation_denies_pretransfer_restoration_but_not_transferred_output() {
        for restore_same in [false, true] {
            let capture = Arc::new(Capture::default());
            let mut worker = Pipeline::new(vec![capture.clone()])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            worker.recv().await.unwrap();
            let context = capture.0.lock().unwrap().take().unwrap();
            let health = context.health();
            let source = EagerSourceSessionOwner::from_context(4096, &context).unwrap();
            let consumer = NativeEagerConsumer::new();
            let turn = begin(&source, &consumer);
            let prediction = source.predict(&turn, eager("pending")).unwrap();
            let owner =
                SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
            let receipt = source.offer_final(&turn, final_text("pending")).unwrap();
            claims(&receipt, &consumer);
            if !restore_same {
                health.set_usable(false);
            }
            health.set_usable(true);
            assert!(!turn.is_live());
            assert!(!turn.is_active());
            assert!(!owner.confirm_source_with_context(&receipt, &consumer, empty_context()));
            assert!(!owner.capability().admits_completion());
            let transferred = EagerSourceSessionOwner::from_context(4096, &context).unwrap();
            let turn = begin(&transferred, &consumer);
            let prediction = transferred.predict(&turn, eager("confirmed")).unwrap();
            let owner =
                SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
            let receipt = transferred
                .offer_final(&turn, final_text("confirmed"))
                .unwrap();
            claims(&receipt, &consumer);
            assert!(owner.confirm_source_with_context(&receipt, &consumer, empty_context()));
            health.set_usable(false);
            transferred.revoke();
            assert_eq!(owner.capability().status(), SpeculationStatus::Confirmed);
            assert!(owner.capability().release_after_boundary());
            worker.end().await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), worker.collect())
                .await
                .unwrap()
                .unwrap();
        }
    }
    #[tokio::test]
    async fn normal_completion_disarms_ended_runtime_and_dropped_health() {
        let capture = Arc::new(Capture::default());
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker.recv().await.unwrap();
        let context = capture.0.lock().unwrap().take().unwrap();
        let source = EagerSourceSessionOwner::from_context(4096, &context).unwrap();
        let consumer = NativeEagerConsumer::new();
        let turn = source.begin_turn().unwrap();
        let prediction = source.predict(&turn, eager("offered")).unwrap();
        let receipt = source.offer_final(&turn, final_text("offered")).unwrap();
        assert!(source.begin_drain());
        assert!(source.complete_drain());
        worker.end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        drop(context);
        drop(capture);
        drop(source);
        assert!(!turn.is_active());
        assert!(turn.claim_start(&consumer));
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        claims(&receipt, &consumer);
        assert!(owner.confirm_source_with_context(&receipt, &consumer, empty_context()));
    }
    #[tokio::test]
    async fn bound_effect_constructs_and_polls_reentrant_callback_outside_source_and_capability_locks(
    ) {
        let capture = Arc::new(Capture::default());
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker.recv().await.unwrap();
        let context = capture.0.lock().unwrap().take().unwrap();
        let source = Arc::new(session());
        let consumer = NativeEagerConsumer::new();
        let turn = begin(&source, &consumer);
        let prediction = source.predict(&turn, eager("callback")).unwrap();
        let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
        let capability = owner.capability();
        assert!(capability.claim_inference());
        assert!(capability.bind_admission(context.clone(), context.health().generation()));
        let receipt = source.offer_final(&turn, final_text("callback")).unwrap();
        claims(&receipt, &consumer);
        assert!(owner.confirm_source_with_context(&receipt, &consumer, empty_context()));
        assert!(capability.release_after_boundary());
        let constructed = Arc::new(AtomicBool::new(false));
        let polled = Arc::new(AtomicBool::new(false));
        let inspect = {
            let source = source.clone();
            let capability = capability.clone();
            let prediction = prediction.clone();
            move || {
                assert!(
                    source.0.state.try_lock().is_ok(),
                    "callback construction/poll retained L"
                );
                assert!(
                    capability.0.state.try_lock().is_ok(),
                    "callback construction/poll retained C"
                );
                // Public reentry follows the try-lock assertions, so a control
                // fails immediately instead of blocking on a recursive lock.
                assert!(prediction.is_live());
                assert_eq!(capability.status(), SpeculationStatus::Released);
                assert!(capability.admits_effect());
            }
        };
        let callback = crate::LocalAsyncCallback::new({
            let constructed = constructed.clone();
            let polled = polled.clone();
            move || {
                inspect();
                constructed.store(true, Ordering::SeqCst);
                let inspect = inspect.clone();
                let polled = polled.clone();
                std::future::poll_fn(move |_| {
                    inspect();
                    polled.store(true, Ordering::SeqCst);
                    std::task::Poll::Ready(Ok(()))
                })
            }
        });
        let invocation = callback.call();
        assert!(!constructed.load(Ordering::SeqCst));
        assert!(!polled.load(Ordering::SeqCst));
        let outcome =
            tokio::time::timeout(Duration::from_secs(3), capability.run_effect(invocation))
                .await
                .unwrap()
                .unwrap();
        assert!(outcome.is_ok());
        assert!(constructed.load(Ordering::SeqCst));
        assert!(polled.load(Ordering::SeqCst));
        worker.end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
    }
}
