//! Independent public source-session API contracts, typed Start contract c561eccf.
use pipecat_core::*;

fn payload(eager: bool, value: &str) -> FrameData {
    let data = TranscriptionFrame {
        text: value.into(),
        user_id: "independent".into(),
        timestamp: "0".into(),
        language: None,
        result: None,
        finalized: true,
    };
    if eager {
        FrameData::EagerTranscription(data)
    } else {
        FrameData::Transcription(data)
    }
}
fn context() -> LlmContextData {
    LlmContextData {
        messages: vec![ChatMessage::new(ChatRole::User, "same").into()],
        tools: ToolSet::NotGiven,
        tool_choice: None,
    }
}
fn owner() -> EagerSourceSessionOwner {
    EagerSourceSessionOwner::new(4096).unwrap()
}
fn claimed_turn(
    source: &EagerSourceSessionOwner,
    consumer: &NativeEagerConsumer,
) -> NativeEagerTurn {
    let turn = source.begin_turn().unwrap();
    assert!(turn.claim_start(consumer));
    turn
}
fn claim_receipt(receipt: &NativeEagerCommit, consumer: &NativeEagerConsumer) {
    assert!(receipt.claim_final(consumer));
    assert!(receipt.claim_stop(consumer));
}

#[test]
fn review_bound_legacy_confirm_cannot_bypass_exact_receipt() {
    let source = owner();
    let consumer = NativeEagerConsumer::new();
    let turn = claimed_turn(&source, &consumer);
    let token = source.predict(&turn, payload(true, "same")).unwrap();
    let owner = SpeculationOwner::try_adopt_source_prediction(&token, &consumer).unwrap();
    assert!(!owner.confirm());
    assert!(!owner.confirm_with_context(context()));
    assert_eq!(owner.capability().status(), SpeculationStatus::Pending);
    let receipt = source.offer_final(&turn, payload(false, "same")).unwrap();
    assert!(!owner.confirm_source_with_context(&receipt, &consumer, context()));
    assert!(receipt.claim_final(&consumer));
    assert!(
        !owner.confirm_source_with_context(&receipt, &consumer, context()),
        "final without stop confirmed"
    );
    assert!(receipt.claim_stop(&consumer));
    assert!(owner.confirm_source_with_context(&receipt, &consumer, context()));
    assert!(!receipt.consume_fallback(&consumer));
}

#[test]
fn review_final_before_adoption_binds_consumer_once_and_preserves_correct_retry() {
    let source = owner();
    let turn = source.begin_turn().unwrap();
    let prediction = source.predict(&turn, payload(true, "same")).unwrap();
    let receipt = source.offer_final(&turn, payload(false, "same")).unwrap();
    let intended = NativeEagerConsumer::new();
    let foreign = NativeEagerConsumer::new();
    assert!(
        !receipt.claim_final(&intended),
        "receipt admitted without source Start"
    );
    assert!(SpeculationOwner::try_adopt_source_prediction(&prediction, &intended).is_none());
    assert!(turn.claim_start(&intended));
    assert!(!turn.claim_start(&foreign));
    assert!(!turn.claim_start(&intended.clone()));
    assert!(receipt.claim_final(&intended));
    assert!(SpeculationOwner::try_adopt_source_prediction(&prediction, &foreign).is_none());
    let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &intended).unwrap();
    assert!(!receipt.claim_final(&intended.clone()));
    assert!(receipt.claim_stop(&intended));
    assert!(!owner.confirm_source_with_context(&receipt, &foreign, context()));
    assert!(owner.confirm_source_with_context(&receipt, &intended, context()));
    assert!(
        SpeculationOwner::try_adopt_source_prediction(&prediction.clone(), &intended).is_none()
    );
}

#[test]
fn review_equal_text_foreign_source_receipt_cannot_confirm_or_consume_local_receipt() {
    let source = owner();
    let foreign_source = owner();
    let consumer = NativeEagerConsumer::new();
    let turn = claimed_turn(&source, &consumer);
    let foreign_turn = claimed_turn(&foreign_source, &consumer);
    let token = source.predict(&turn, payload(true, "same")).unwrap();
    let owner = SpeculationOwner::try_adopt_source_prediction(&token, &consumer).unwrap();
    let foreign = foreign_source
        .offer_final(&foreign_turn, payload(false, "same"))
        .unwrap();
    claim_receipt(&foreign, &consumer);
    assert!(!owner.confirm_source_with_context(&foreign, &consumer, context()));
    assert_eq!(owner.capability().status(), SpeculationStatus::Pending);
    let receipt = source.offer_final(&turn, payload(false, "same")).unwrap();
    claim_receipt(&receipt, &consumer);
    assert!(owner.confirm_source_with_context(&receipt, &consumer, context()));
}

#[test]
fn review_normal_drop_preserves_offered_receipt_but_abandoned_owner_does_not() {
    let consumer = NativeEagerConsumer::new();
    let source = owner();
    let turn = source.begin_turn().unwrap();
    let prediction = source.predict(&turn, payload(true, "same")).unwrap();
    let receipt = source.offer_final(&turn, payload(false, "same")).unwrap();
    assert!(source.begin_drain());
    assert!(source.complete_drain());
    drop(source);
    assert!(
        !turn.is_active(),
        "normal EOF must not grant unrelated empty recovery"
    );
    assert!(turn.claim_start(&consumer));
    let speculation =
        SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
    claim_receipt(&receipt, &consumer);
    assert!(speculation.confirm_source_with_context(&receipt, &consumer, context()));
    let source = owner();
    let turn = claimed_turn(&source, &consumer);
    let receipt = source.offer_final(&turn, payload(false, "same")).unwrap();
    drop(source);
    assert!(!receipt.claim_final(&consumer));
    assert!(!receipt.consume_fallback(&consumer));
}

#[test]
fn review_transfer_relinquishes_source_authority_but_not_aggregator_authority() {
    let source = owner();
    let consumer = NativeEagerConsumer::new();
    let turn = claimed_turn(&source, &consumer);
    let prediction = source.predict(&turn, payload(true, "same")).unwrap();
    let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
    let capability = owner.capability();
    let receipt = source.offer_final(&turn, payload(false, "same")).unwrap();
    claim_receipt(&receipt, &consumer);
    assert!(owner.confirm_source_with_context(&receipt, &consumer, context()));
    let _replacement = source.begin_turn().unwrap();
    source.revoke();
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    assert!(capability.source_is_current());
    assert!(capability.claim_inference());
    assert!(capability.release_after_boundary());
    drop(owner);
    assert_eq!(capability.status(), SpeculationStatus::Cancelled);
}

#[test]
fn review_old_replay_and_resume_cannot_cancel_new_prediction() {
    let source = owner();
    let consumer = NativeEagerConsumer::new();
    let turn = claimed_turn(&source, &consumer);
    let old = source.predict(&turn, payload(true, "old")).unwrap();
    let old_owner = SpeculationOwner::try_adopt_source_prediction(&old, &consumer).unwrap();
    let new = source.predict(&turn, payload(true, "new")).unwrap();
    let new_owner = SpeculationOwner::try_adopt_source_prediction(&new, &consumer).unwrap();
    assert_eq!(
        old_owner.capability().status(),
        SpeculationStatus::Cancelled
    );
    assert!(SpeculationOwner::try_adopt_source_prediction(&old, &consumer).is_none());
    assert!(!source.resume(&old));
    assert_eq!(new_owner.capability().status(), SpeculationStatus::Pending);
    assert!(new_owner.capability().is_source_prediction(&new));
    assert!(source.resume(&new));
    assert!(
        turn.is_active(),
        "healthy resume retired ordinary turn/recovery authority"
    );
}

#[test]
fn review_mutated_payload_and_upstream_carrier_fail_closed_without_consumption() {
    let source = owner();
    let turn = source.begin_turn().unwrap();
    let token = source.predict(&turn, payload(true, "same")).unwrap();
    let mut frame = Frame::new(token.payload().clone());
    frame
        .set_native_eager_source(NativeEagerSource::Prediction(token.clone()))
        .unwrap();
    assert!(serde_json::to_value(&frame).is_err());
    assert!(frame.require_no_native_llm_continuation().is_err());
    assert!(frame.take_native_eager_source(Direction::Upstream).is_err());
    assert!(frame.native_eager_source().is_some());
    let mut duplicate = frame.duplicate();
    assert_ne!(duplicate.id, frame.id);
    duplicate.data = payload(true, "mutated");
    assert!(duplicate
        .take_native_eager_source(Direction::Downstream)
        .is_err());
    assert!(frame
        .take_native_eager_source(Direction::Downstream)
        .unwrap()
        .is_some());
    assert!(frame.native_eager_source().is_none());
}

#[test]
fn review_passive_retention_cap_is_recoverable_after_tokens_are_dropped() {
    let source = owner();
    let mut tokens = Vec::new();
    for _ in 0..32 {
        let turn = source.begin_turn().unwrap();
        tokens.push(source.offer_final(&turn, payload(false, "same")).unwrap());
    }
    assert!(
        source.begin_turn().is_err(),
        "turn and receipt did not share the 64-record budget"
    );
    tokens.clear();
    let turn = source.begin_turn().unwrap();
    assert!(source
        .offer_final(&turn, payload(false, "after-drop"))
        .is_ok());
}

#[test]
fn review_foreign_receipt_with_same_local_allocation_order_cannot_consume_own_receipt() {
    let a = owner();
    let b = owner();
    let consumer = NativeEagerConsumer::new();
    let at = claimed_turn(&a, &consumer);
    let bt = claimed_turn(&b, &consumer);
    let ap = a.predict(&at, payload(true, "same")).unwrap();
    let _bp = b.predict(&bt, payload(true, "same")).unwrap();
    let owner = SpeculationOwner::try_adopt_source_prediction(&ap, &consumer).unwrap();
    let ar = a.offer_final(&at, payload(false, "same")).unwrap();
    let br = b.offer_final(&bt, payload(false, "same")).unwrap();
    for receipt in [&ar, &br] {
        claim_receipt(receipt, &consumer);
    }
    // Aligned turn/prediction/receipt local IDs prevent lookup mismatch masking.
    assert!(!owner.confirm_source_with_context(&br, &consumer, context()));
    assert!(
        ar.is_live(),
        "foreign receipt consumed the matching-number local receipt"
    );
    assert!(owner.confirm_source_with_context(&ar, &consumer, context()));
}

#[test]
fn review_empty_stop_requires_owner_offer_and_same_consumer_start() {
    let source = owner();
    let turn = source.begin_turn().unwrap();
    let consumer = NativeEagerConsumer::new();
    let foreign = NativeEagerConsumer::new();
    assert!(!turn.claim_empty_stop(&consumer));
    assert!(turn.claim_start(&consumer));
    assert!(
        !turn.claim_empty_stop(&consumer),
        "passive Start token manufactured closing authority"
    );
    source.offer_empty_stop(&turn).unwrap();
    assert!(!turn.claim_empty_stop(&foreign));
    assert!(turn.claim_empty_stop(&consumer));
    assert!(!turn.claim_empty_stop(&consumer));
    assert!(source.offer_final(&turn, payload(false, "late")).is_err());
    assert!(source.predict(&turn, payload(true, "late")).is_err());
}

#[test]
fn review_retired_claimed_receipt_cannot_consume_fallback() {
    let source = owner();
    let consumer = NativeEagerConsumer::new();
    let turn = claimed_turn(&source, &consumer);
    let receipt = source
        .offer_final(&turn, payload(false, "retired"))
        .unwrap();
    claim_receipt(&receipt, &consumer);
    source.revoke();
    assert!(
        !receipt.consume_fallback(&consumer),
        "core admitted already-claimed retired ordinary final"
    );
}
