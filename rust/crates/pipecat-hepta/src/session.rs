use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamIdentity {
    pub principal_id: String,
    pub device_id: String,
    pub device_generation: u64,
    pub organ_id: String,
    pub organ_generation: u64,
    pub logical_session_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputIdentity {
    pub stream: StreamIdentity,
    pub turn_id: String,
    pub utterance_id: String,
    pub sequence: u64,
    pub output_epoch: u64,
}

/// Scope copied from the existing Hepta HMI bridge's source-bound event.
/// Complete fields are not an output grant: a trusted owner must also admit the
/// event kind and check its current permits_output boundary. Nulls reject audio.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HeptaOutputScope {
    pub principal_id: String,
    pub device_id: String,
    pub device_generation: u64,
    pub organ_id: String,
    pub organ_generation: u64,
    pub logical_session_id: String,
    pub utterance_sequence: u64,
    pub connection_id: u64,
    pub output_epoch: u64,
    pub turn_id: Option<String>,
    pub utterance_id: Option<String>,
}
impl TryFrom<HeptaOutputScope> for OutputIdentity {
    type Error = HmiError;
    fn try_from(scope: HeptaOutputScope) -> Result<Self, Self::Error> {
        let turn_id = scope.turn_id.ok_or(HmiError::InvalidIdentity)?;
        let utterance_id = scope.utterance_id.ok_or(HmiError::InvalidIdentity)?;
        if scope.connection_id == 0 || !valid_id(&turn_id) || !valid_id(&utterance_id) { return Err(HmiError::InvalidIdentity); }
        Ok(Self { stream: StreamIdentity { principal_id: scope.principal_id, device_id: scope.device_id, device_generation: scope.device_generation, organ_id: scope.organ_id, organ_generation: scope.organ_generation, logical_session_id: scope.logical_session_id }, turn_id, utterance_id, sequence: scope.utterance_sequence, output_epoch: scope.output_epoch })
    }
}

/// CPAL submission never transitions to Played or Heard.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresentationState {
    Generated,
    Queued,
    Submitted,
    Interrupted,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresentationRecord {
    pub identity: OutputIdentity,
    pub state: PresentationState,
}

/// A request to the existing Hepta turn owner, never a tool-effect rollback.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnInterrupt {
    pub logical_session_id: String,
    pub turn_id: String,
    pub output_epoch: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct InterruptOutcome {
    pub request: TurnInterrupt,
    /// Failure is retained while the caller still receives the remote interrupt.
    pub local_error: Option<String>,
}

/// Must retire old local media before returning. No upstream RPC belongs here.
pub trait MediaControl {
    fn interrupt(&self, new_epoch: u64) -> Result<(), String>;
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HmiError {
    #[error("invalid bounded HMI identity or capacity")]
    InvalidIdentity,
    #[error("foreign session, stale turn or output epoch")]
    StaleOutput,
    #[error("repeated or out-of-order output sequence")]
    Replay,
    #[error("presentation ledger is full; active records cannot be evicted")]
    Backpressure,
    #[error("unknown utterance or invalid presentation transition")]
    InvalidTransition,
    #[error("output epoch exhausted")]
    EpochExhausted,
}

pub struct HmiSession {
    identity: StreamIdentity,
    turn_id: String,
    epoch: u64,
    retired: bool,
    last_sequence: Option<u64>,
    limit: usize,
    records: BTreeMap<String, PresentationRecord>,
}

pub(crate) fn valid_id(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

impl HmiSession {
    pub fn new(identity: StreamIdentity, turn_id: String, limit: usize) -> Result<Self, HmiError> {
        Self::with_epoch(identity, turn_id, 0, limit)
    }

    /// Initial epoch is supplied by the existing Hepta session owner on attach.
    pub fn with_epoch(identity: StreamIdentity, turn_id: String, epoch: u64, limit: usize) -> Result<Self, HmiError> {
        if [
            &identity.principal_id,
            &identity.device_id,
            &identity.organ_id,
            &identity.logical_session_id,
            &turn_id,
        ]
        .into_iter()
        .any(|value| !valid_id(value))
            || identity.device_generation == 0 || identity.organ_generation == 0
            || !(1..=4096).contains(&limit)
        {
            return Err(HmiError::InvalidIdentity);
        }
        Ok(Self { identity, turn_id, epoch, retired: false, last_sequence: None, limit, records: BTreeMap::new() })
    }

    pub fn output_epoch(&self) -> u64 { self.epoch }
    pub fn identity(&self) -> &StreamIdentity { &self.identity }
    pub fn records(&self) -> impl Iterator<Item = &PresentationRecord> { self.records.values() }

    pub fn check_current(&self, identity: &OutputIdentity) -> Result<(), HmiError> {
        if self.retired || identity.stream != self.identity || identity.turn_id != self.turn_id || identity.output_epoch != self.epoch {
            return Err(HmiError::StaleOutput);
        }
        Ok(())
    }

    pub fn generated(&mut self, identity: OutputIdentity) -> Result<(), HmiError> {
        self.check_current(&identity)?;
        if !valid_id(&identity.utterance_id) { return Err(HmiError::InvalidIdentity); }
        if self.last_sequence.is_some_and(|last| identity.sequence <= last) || self.records.contains_key(&identity.utterance_id) {
            return Err(HmiError::Replay);
        }
        if self.records.len() == self.limit { return Err(HmiError::Backpressure); }
        self.last_sequence = Some(identity.sequence);
        self.records.insert(identity.utterance_id.clone(), PresentationRecord { identity, state: PresentationState::Generated });
        Ok(())
    }

    pub fn transition(&mut self, identity: &OutputIdentity, next: PresentationState) -> Result<(), HmiError> {
        self.check_current(identity)?;
        let record = self.records.get_mut(&identity.utterance_id).ok_or(HmiError::InvalidTransition)?;
        if record.identity != *identity || !matches!((record.state, next),
            (PresentationState::Generated, PresentationState::Queued | PresentationState::Unknown)
            | (PresentationState::Queued, PresentationState::Submitted | PresentationState::Unknown)
            | (PresentationState::Submitted, PresentationState::Unknown)) {
            return Err(HmiError::InvalidTransition);
        }
        record.state = next;
        Ok(())
    }

    pub fn barge_in(&mut self, media: &impl MediaControl) -> Result<InterruptOutcome, HmiError> {
        self.epoch = self.epoch.checked_add(1).ok_or(HmiError::EpochExhausted)?;
        self.retired = true;
        // Fence first: even a device teardown error must never revive old audio.
        let local_error = media.interrupt(self.epoch).err().map(|text| text.chars().take(512).collect());
        for record in self.records.values_mut() {
            if matches!(record.state, PresentationState::Generated | PresentationState::Queued) {
                record.state = if local_error.is_some() { PresentationState::Unknown } else { PresentationState::Interrupted };
            }
        }
        Ok(InterruptOutcome { request: TurnInterrupt { logical_session_id: self.identity.logical_session_id.clone(), turn_id: self.turn_id.clone(), output_epoch: self.epoch }, local_error })
    }

    /// Call only when the existing Hepta owner has admitted the next turn.
    pub fn next_turn(&mut self, turn_id: String, media: &impl MediaControl) -> Result<InterruptOutcome, HmiError> {
        if !valid_id(&turn_id) || turn_id == self.turn_id { return Err(HmiError::InvalidIdentity); }
        let outcome = self.barge_in(media)?;
        self.turn_id = turn_id;
        self.retired = false;
        Ok(outcome)
    }

    /// Attach an admitted Hepta turn using its exact epoch. A local stop may
    /// already have raised the retirement floor to that value; lower epochs
    /// cannot restore old output. This does not grant or authenticate a turn.
    pub fn attach_turn(&mut self, turn_id: String, owner_epoch: u64, media: &impl MediaControl) -> Result<Option<String>, HmiError> {
        if !valid_id(&turn_id) || owner_epoch < self.epoch { return Err(HmiError::StaleOutput); }
        if turn_id != self.turn_id && owner_epoch == self.epoch && !self.retired {
            return Err(HmiError::StaleOutput);
        }
        let local_error = if owner_epoch > self.epoch {
            self.epoch = owner_epoch;
            let error = media.interrupt(owner_epoch).err().map(|text| text.chars().take(512).collect());
            for record in self.records.values_mut() {
                if matches!(record.state, PresentationState::Generated | PresentationState::Queued) {
                    record.state = if error.is_some() { PresentationState::Unknown } else { PresentationState::Interrupted };
                }
            }
            error
        } else { None };
        self.turn_id = turn_id;
        self.retired = false;
        Ok(local_error)
    }

    /// Explicit release after the caller has retained the terminal observation.
    /// Sequence high-water survives release; this is not durable request dedupe.
    pub fn release_terminal(&mut self, utterance_id: &str) -> Result<PresentationRecord, HmiError> {
        if !self.records.get(utterance_id).is_some_and(|record| matches!(record.state, PresentationState::Submitted | PresentationState::Interrupted | PresentationState::Unknown)) {
            return Err(HmiError::InvalidTransition);
        }
        Ok(self.records.remove(utterance_id).expect("checked membership"))
    }
}
