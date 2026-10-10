use pipecat_core::*;
#[test]
fn owner_drop_cancel_and_late_confirmation_are_terminal() {
    let owner = SpeculationOwner::new();
    let capability = owner.capability();
    assert!(!capability.release_after_boundary());
    assert!(capability.claim_inference());
    assert!(!capability.claim_inference());
    assert!(owner.cancel());
    assert!(!owner.confirm());
    assert!(!capability.release_after_boundary());
    assert!(!capability.admits_effect());
    let owner = SpeculationOwner::new();
    let capability = owner.capability();
    assert!(owner.confirm());
    assert!(capability.release_after_boundary());
    assert!(capability.is_released());
    assert!(!capability.admits_effect());
    drop(owner);
    assert!(!capability.admits_effect());
}
#[test]
fn pending_tools_withdraw_and_confirmed_tools_wait_for_boundary() {
    let owner = SpeculationOwner::new();
    let capability = owner.capability();
    assert!(!capability.admit_tool_intent());
    assert!(!owner.confirm());
    let owner = SpeculationOwner::new();
    let capability = owner.capability();
    assert!(owner.confirm());
    assert!(capability.admit_tool_intent());
    assert!(!capability.admits_effect());
    assert!(capability.release_after_boundary());
    assert!(capability.is_released());
    assert!(!capability.admits_effect());
}
#[test]
fn capability_survives_native_clones_and_cannot_cross_wire() {
    let owner = SpeculationOwner::new();
    let mut frame = Frame::new(FrameData::EagerEndOfTurnCancel);
    frame.set_native_speculation(owner.capability()).unwrap();
    assert_eq!(
        frame.clone().native_speculation(),
        frame.native_speculation()
    );
    assert!(serde_json::to_value(&frame).is_err());
    let mut ordinary = Frame::new(FrameData::UserStoppedSpeaking);
    ordinary
        .metadata
        .insert("speculation_confirmed".into(), true.into());
    let wire: Frame = serde_json::from_value(serde_json::to_value(ordinary).unwrap()).unwrap();
    assert!(wire.native_speculation().is_none());
}
