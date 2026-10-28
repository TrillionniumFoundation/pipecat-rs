//! Exercise the example's ledger helper without loading engines or opening devices.
use pipecat_hepta::{HmiError, HmiSession, MediaControl, OutputIdentity, PresentationState, StreamIdentity};
use std::cell::RefCell;

#[path = "../examples/local_speech/ledger.rs"]
mod ledger;

#[derive(Default)]
struct Stop {
    calls: RefCell<Vec<u64>>,
    fail: bool,
}
impl MediaControl for Stop {
    fn interrupt(&self, epoch: u64) -> Result<(), String> {
        self.calls.borrow_mut().push(epoch);
        if self.fail { Err("stop failed".into()) } else { Ok(()) }
    }
}

fn identity(sequence: u64, epoch: u64) -> OutputIdentity {
    OutputIdentity {
        stream: StreamIdentity {
            principal_id: "owner".into(),
            device_id: "device".into(),
            device_generation: 2,
            organ_id: "organ".into(),
            organ_generation: 7,
            logical_session_id: "session".into(),
        },
        turn_id: format!("turn-{sequence}"),
        utterance_id: format!("utterance-{sequence}"),
        sequence,
        output_epoch: epoch,
    }
}

#[test]
fn interrupted_render_retains_terminal_once_and_reuses_capacity() {
    let first = identity(1, 0);
    let mut session = HmiSession::with_epoch(first.stream.clone(), first.turn_id.clone(), 0, 2).unwrap();
    let stop = Stop::default();
    let mut observations = Vec::new();
    for sequence in 1..=260 {
        let current = identity(sequence, sequence - 1);
        if sequence > 1 {
            session.attach_turn(current.turn_id.clone(), current.output_epoch, &stop).unwrap();
        }
        session.generated(current.clone()).unwrap();
        session.transition(&current, PresentationState::Queued).unwrap();
        session.barge_in(&stop).unwrap();
        ledger::retain_terminal(&mut session, &current, |record| {
            observations.push(record.clone());
            Ok(())
        }).unwrap();
        assert_eq!(session.records().count(), 0, "terminal lease leaked at sequence {sequence}");
        assert_eq!(observations.len(), sequence as usize);
        assert_eq!(observations.last().unwrap().identity, current);
        assert_eq!(observations.last().unwrap().state, PresentationState::Interrupted);
    }
    let replay = identity(260, 260);
    session.attach_turn(replay.turn_id.clone(), replay.output_epoch, &stop).unwrap();
    assert_eq!(session.generated(replay), Err(HmiError::Replay));
}

#[test]
fn terminal_retention_failure_keeps_the_record_and_repeat_release_is_rejected() {
    let current = identity(1, 0);
    let mut session = HmiSession::with_epoch(current.stream.clone(), current.turn_id.clone(), 0, 1).unwrap();
    session.generated(current.clone()).unwrap();
    session.transition(&current, PresentationState::Queued).unwrap();
    let stop = Stop { fail: true, ..Default::default() };
    session.barge_in(&stop).unwrap();
    assert!(ledger::retain_terminal(&mut session, &current, |_| Err("receiver closed".into())).is_err());
    assert_eq!(session.records().next().unwrap().state, PresentationState::Unknown);
    assert_eq!(session.records().count(), 1);
    let mut retained = Vec::new();
    ledger::retain_terminal(&mut session, &current, |record| { retained.push(record.clone()); Ok(()) }).unwrap();
    assert_eq!(retained.len(), 1);
    assert!(ledger::retain_terminal(&mut session, &current, |_| panic!("duplicate terminal retained")).is_err());
    assert_eq!(session.records().count(), 0);
}

#[test]
fn a_delayed_terminal_cannot_release_a_reused_utterance_identity() {
    let old = identity(1, 0);
    let mut session = HmiSession::with_epoch(old.stream.clone(), old.turn_id.clone(), 0, 1).unwrap();
    session.generated(old.clone()).unwrap();
    session.transition(&old, PresentationState::Unknown).unwrap();
    ledger::retain_terminal(&mut session, &old, |_| Ok(())).unwrap();
    let mut new = old.clone();
    new.sequence = 2;
    session.generated(new.clone()).unwrap();
    session.transition(&new, PresentationState::Unknown).unwrap();
    assert!(ledger::retain_terminal(&mut session, &old, |_| panic!("stale identity was retained")).is_err());
    assert_eq!(session.records().next().unwrap().identity, new);
    ledger::retain_terminal(&mut session, &new, |_| Ok(())).unwrap();
    assert_eq!(session.records().count(), 0);
}

#[test]
fn interrupted_terminal_preserves_local_interruption_without_claiming_playback() {
    let current = identity(1, 0);
    let record = pipecat_hepta::PresentationRecord { identity: current.clone(), state: PresentationState::Interrupted };
    assert_eq!(ledger::terminal_event(&record, None), serde_json::json!({
        "type":"presentation", "identity":current, "state":"interrupted", "error":null
    }));
    assert_eq!(record.state, PresentationState::Interrupted);
}
