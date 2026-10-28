use pipecat_hepta::{HmiSession, OutputIdentity, PresentationRecord, PresentationState};

pub(super) fn terminal_event(
    record: &PresentationRecord,
    error: Option<&str>,
) -> serde_json::Value {
    // Report local interruption distinctly. This says nothing about how much
    // PCM reached a device or whether a user heard it.
    serde_json::json!({"type":"presentation", "identity":record.identity, "state":record.state, "error":error})
}

pub(super) fn retain_terminal(
    session: &mut HmiSession,
    identity: &OutputIdentity,
    retain: impl FnOnce(&PresentationRecord) -> Result<(), String>,
) -> Result<(), String> {
    let record = session
        .records()
        .find(|record| {
            record.identity == *identity
                && matches!(
                    record.state,
                    PresentationState::Submitted
                        | PresentationState::Interrupted
                        | PresentationState::Unknown
                )
        })
        .cloned()
        .ok_or("missing retired presentation")?;
    retain(&record)?;
    session
        .release_terminal(&identity.utterance_id)
        .map(|_| ())
        .map_err(|error| error.to_string())
}
