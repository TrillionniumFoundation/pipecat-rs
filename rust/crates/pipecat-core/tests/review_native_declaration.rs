//! Native declaration ownership is local, immutable, and explicitly consumed.
use pipecat_core::*;
use serde_json::json;
use std::sync::Arc;
fn call(id: &str) -> Arc<FunctionCall> {
    Arc::new(FunctionCall {
        function_name: "effect".into(),
        tool_call_id: id.into(),
        arguments: json!({"id":id}),
    })
}
fn batch() -> Arc<NativeToolCallBatch> {
    NativeToolCallBatch::new("group".into(), vec![(call("a"), true), (call("b"), false)]).unwrap()
}
fn started(batch: Arc<NativeToolCallBatch>) -> Frame {
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(
        batch
            .members()
            .iter()
            .map(|(call, _)| call.as_ref().clone())
            .collect(),
    ));
    frame.set_native_tool_batch(batch).unwrap();
    frame
}
#[test]
fn independent_native_started_receipt_binds_ordered_full_membership_and_wire() {
    let batch = batch();
    let original = started(batch.clone());
    assert!(Arc::ptr_eq(
        original.native_tool_batch().unwrap().unwrap(),
        &batch
    ));
    assert!(serde_json::to_vec(&original).is_err());
    let mut duplicate = original.duplicate();
    duplicate.id = u64::MAX;
    assert_eq!(
        duplicate.native_tool_batch().unwrap().unwrap().id(),
        batch.id()
    );
    for mutation in 0..5 {
        let mut frame = original.duplicate();
        let FrameData::FunctionCallsStarted(calls) = &mut frame.data else {
            unreachable!()
        };
        match mutation {
            0 => calls.swap(0, 1),
            1 => {
                calls.pop();
            }
            2 => calls[0].function_name = "forged".into(),
            3 => calls[0].tool_call_id = "forged".into(),
            _ => calls[0].arguments = json!({"forged":true}),
        }
        assert!(
            frame.native_tool_batch().is_err(),
            "declaration mutation {mutation} retained native ownership"
        );
        assert!(frame.take_native_tool_batch().is_err());
        assert!(serde_json::to_vec(&frame).is_err());
    }
    assert!(duplicate.set_native_tool_batch(batch.clone()).is_err());
    assert!(Arc::ptr_eq(
        &duplicate.take_native_tool_batch().unwrap().unwrap(),
        &batch
    ));
    duplicate
        .metadata
        .insert("native_tool_batch".into(), json!({"id":batch.id()}));
    let decoded: Frame = serde_json::from_slice(&serde_json::to_vec(&duplicate).unwrap()).unwrap();
    assert!(decoded.native_tool_batch().unwrap().is_none());
}
#[test]
fn independent_native_inprogress_receipt_binds_member_policy_and_group() {
    let batch = batch();
    let mut original = Frame::new(FrameData::FunctionCallInProgress(
        FunctionCallInProgressFrame {
            function_name: "effect".into(),
            tool_call_id: "a".into(),
            arguments: json!({"id":"a"}),
            cancel_on_interruption: true,
            group_id: Some("group".into()),
        },
    ));
    original.set_native_tool_batch(batch.clone()).unwrap();
    for mutation in 0..5 {
        let mut frame = original.duplicate();
        let FrameData::FunctionCallInProgress(call) = &mut frame.data else {
            unreachable!()
        };
        match mutation {
            0 => call.cancel_on_interruption = false,
            1 => call.group_id = Some("forged".into()),
            2 => call.function_name = "forged".into(),
            3 => call.tool_call_id = "b".into(),
            _ => call.arguments = json!({"forged":true}),
        }
        assert!(frame.native_tool_batch().is_err());
        assert!(frame.take_native_tool_batch().is_err());
        assert!(serde_json::to_vec(&frame).is_err());
    }
    assert!(serde_json::to_vec(&original).is_err());
    original.take_native_tool_batch().unwrap().unwrap();
    assert!(serde_json::to_vec(&original).is_ok());
    assert!(Frame::text("not a declaration")
        .set_native_tool_batch(batch)
        .is_err());
}
