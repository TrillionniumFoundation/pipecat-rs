use pipecat_hepta::*;
use std::cell::RefCell;

fn stream() -> StreamIdentity {
    StreamIdentity { principal_id: "owner".into(), device_id: "mic-1".into(), device_generation: 4, organ_id: "human-interface".into(), organ_generation: 8, logical_session_id: "session-1".into() }
}
fn output(sequence: u64, epoch: u64, utterance: &str) -> OutputIdentity {
    OutputIdentity { stream: stream(), turn_id: "turn-1".into(), utterance_id: utterance.into(), sequence, output_epoch: epoch }
}
#[derive(Default)]
struct LocalStop { calls: RefCell<Vec<u64>>, failure: bool }
impl MediaControl for LocalStop {
    fn interrupt(&self, epoch: u64) -> Result<(), String> {
        self.calls.borrow_mut().push(epoch);
        if self.failure { Err("device disconnected".into()) } else { Ok(()) }
    }
}

#[test]
fn old_synthesis_cannot_reenter_after_local_barge_in() {
    let mut session = HmiSession::new(stream(), "turn-1".into(), 4).unwrap();
    let old = output(1, 0, "old");
    session.generated(old.clone()).unwrap();
    session.transition(&old, PresentationState::Queued).unwrap();
    let stop = LocalStop::default();
    let pending_remote = session.barge_in(&stop).unwrap();
    // Local retirement already happened before the caller can send this request.
    assert_eq!(*stop.calls.borrow(), vec![1]);
    assert_eq!(pending_remote.request.turn_id, "turn-1");
    assert_eq!(pending_remote.request.output_epoch, 1);
    assert_eq!(session.transition(&old, PresentationState::Submitted), Err(HmiError::StaleOutput));
    assert_eq!(session.generated(output(2, 0, "late")), Err(HmiError::StaleOutput));
    assert_eq!(session.generated(output(2, 1, "unadmitted")), Err(HmiError::StaleOutput));
    session.attach_turn("turn-2".into(), 1, &stop).unwrap();
    let mut replacement = output(2, 1, "replacement");
    replacement.turn_id = "turn-2".into();
    session.generated(replacement).unwrap();
    assert_eq!(session.records().find(|record| record.identity == old).unwrap().state, PresentationState::Interrupted);
}

#[test]
fn device_failure_fences_old_output_and_still_returns_remote_interrupt() {
    let mut session = HmiSession::new(stream(), "turn-1".into(), 4).unwrap();
    session.generated(output(1, 0, "old")).unwrap();
    let stop = LocalStop { failure: true, ..Default::default() };
    let outcome = session.barge_in(&stop).unwrap();
    assert_eq!(outcome.local_error.as_deref(), Some("device disconnected"));
    assert_eq!(outcome.request.output_epoch, 1);
    assert_eq!(session.records().next().unwrap().state, PresentationState::Unknown);
    assert_eq!(session.check_current(&output(2, 0, "late")), Err(HmiError::StaleOutput));
}

#[test]
fn same_epoch_foreign_generation_and_replayed_sequences_reject_atomically() {
    let mut session = HmiSession::new(stream(), "turn-1".into(), 2).unwrap();
    let mut foreign = output(4, 0, "foreign");
    foreign.stream.device_generation += 1;
    assert_eq!(session.generated(foreign), Err(HmiError::StaleOutput));
    session.generated(output(4, 0, "valid")).unwrap();
    assert_eq!(session.generated(output(4, 0, "replayed")), Err(HmiError::Replay));
    assert_eq!(session.generated(output(3, 0, "reordered")), Err(HmiError::Replay));
    assert_eq!(session.records().count(), 1);
}

#[test]
fn count_pressure_never_evicts_live_state_or_consumes_rejected_sequence() {
    let mut session = HmiSession::new(stream(), "turn-1".into(), 1).unwrap();
    let old = output(1, 0, "first");
    session.generated(old.clone()).unwrap();
    assert_eq!(session.generated(output(2, 0, "second")), Err(HmiError::Backpressure));
    assert_eq!(session.release_terminal("first"), Err(HmiError::InvalidTransition));
    session.transition(&old, PresentationState::Unknown).unwrap();
    session.release_terminal("first").unwrap();
    session.generated(output(2, 0, "second")).unwrap();
    assert_eq!(session.records().count(), 1);
}

#[test]
fn callback_submission_cannot_be_promoted_to_played_or_heard() {
    let mut session = HmiSession::new(stream(), "turn-1".into(), 1).unwrap();
    let id = output(1, 0, "speech");
    session.generated(id.clone()).unwrap();
    assert_eq!(session.transition(&id, PresentationState::Submitted), Err(HmiError::InvalidTransition));
    session.transition(&id, PresentationState::Queued).unwrap();
    session.transition(&id, PresentationState::Submitted).unwrap();
    assert!(serde_json::from_str::<PresentationState>("\"played\"").is_err());
    assert!(serde_json::from_str::<PresentationState>("\"heard\"").is_err());
    assert_eq!(session.records().next().unwrap().state, PresentationState::Submitted);
    session.barge_in(&LocalStop::default()).unwrap();
    // Full callback submission stays a retained fact when the device tail is stopped.
    assert_eq!(session.records().next().unwrap().state, PresentationState::Submitted);
    assert_eq!(session.release_terminal("speech").unwrap().state, PresentationState::Submitted);
}

#[test]
fn attached_hepta_epoch_is_preserved_and_zero_device_generations_reject() {
    let mut invalid = stream();
    invalid.device_generation = 0;
    assert!(matches!(HmiSession::new(invalid, "turn-1".into(), 1), Err(HmiError::InvalidIdentity)));
    let session = HmiSession::with_epoch(stream(), "turn-1".into(), 42, 1).unwrap();
    assert_eq!(session.output_epoch(), 42);
    assert!(session.check_current(&output(1, 42, "speech")).is_ok());
}

#[test]
fn exact_owner_epoch_attaches_after_local_stop_without_minting_another_epoch() {
    let mut session = HmiSession::with_epoch(stream(), "turn-1".into(), 42, 4).unwrap();
    let stop = LocalStop::default();
    session.barge_in(&stop).unwrap();
    assert_eq!(session.output_epoch(), 43);
    assert_eq!(session.attach_turn("turn-2".into(), 42, &stop), Err(HmiError::StaleOutput));
    session.attach_turn("turn-2".into(), 43, &stop).unwrap();
    assert_eq!(*stop.calls.borrow(), vec![43]);
    let mut id = output(1, 43, "accepted");
    id.turn_id = "turn-2".into();
    session.generated(id).unwrap();
}

#[test]
fn unknown_hepta_terminal_scope_cannot_become_speech_identity() {
    let scope = HeptaOutputScope { principal_id: "owner".into(), device_id: "mic-1".into(), device_generation: 4, organ_id: "human-interface".into(), organ_generation: 8, logical_session_id: "session-1".into(), utterance_sequence: 1, connection_id: 1, output_epoch: 0, turn_id: None, utterance_id: None };
    assert_eq!(OutputIdentity::try_from(scope), Err(HmiError::InvalidIdentity));
}

#[test]
fn hepta_bridge_numeric_connection_scope_decodes_without_string_coercion() {
    // Source-derived JSON contract of hepta-codex-adapter::HmiOutputScopeV1,
    // not a claim that the bridge or media engines executed in this environment.
    let wire = r#"{"principalId":"owner","deviceId":"mic-1","deviceGeneration":4,"organId":"human-interface","logicalSessionId":"session-1","utteranceSequence":12,"utteranceId":"hepta.hmi:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","organGeneration":8,"connectionId":17,"outputEpoch":42,"turnId":"turn-1"}"#;
    let scope: HeptaOutputScope = serde_json::from_str(wire).unwrap();
    assert_eq!(scope.connection_id, 17);
    let identity = OutputIdentity::try_from(scope).unwrap();
    assert_eq!(identity.sequence, 12);
    assert_eq!(identity.output_epoch, 42);
    assert_eq!(identity.turn_id, "turn-1");
    let wrong_type = wire.replace("\"connectionId\":17", "\"connectionId\":\"17\"");
    assert!(serde_json::from_str::<HeptaOutputScope>(&wrong_type).is_err());
}

#[test]
fn new_turn_retires_previous_turn_without_resetting_replay_high_water() {
    let mut session = HmiSession::new(stream(), "turn-1".into(), 4).unwrap();
    session.generated(output(7, 0, "speech")).unwrap();
    let outcome = session.next_turn("turn-2".into(), &LocalStop::default()).unwrap();
    assert_eq!(outcome.request.turn_id, "turn-1");
    let mut next = output(7, 1, "new-speech");
    next.turn_id = "turn-2".into();
    assert_eq!(session.generated(next.clone()), Err(HmiError::Replay));
    next.sequence = 8;
    session.generated(next).unwrap();
    assert_eq!(session.generated(output(9, 1, "wrong-turn")), Err(HmiError::StaleOutput));
}
