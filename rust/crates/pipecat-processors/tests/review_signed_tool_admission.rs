//! Independent atomic effect-admission checks for bounded signed histories.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

fn signed(response: u64, id: &str, retained_text: usize) -> Frame {
    let call = FunctionCall {
        function_name: "effect".into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    };
    let history = NativeLlmContinuation::new(
        response,
        LlmContinuationProvider::Anthropic,
        vec![
            LlmContinuationBlock::Text("x".repeat(retained_text)),
            LlmContinuationBlock::Thinking {
                text: String::new(),
                signature: "synthetic-signature".into(),
            },
            LlmContinuationBlock::ToolCall(call.clone()),
        ],
    )
    .unwrap();
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
    frame
        .set_native_llm_continuation(Arc::new(history))
        .unwrap();
    frame
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn recv(worker: &mut PipelineWorker, predicate: impl Fn(&FrameData) -> bool) -> Frame {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let output = worker.recv().await.unwrap();
            if predicate(&output.frame.data) {
                return output.frame;
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn review_carrier_history_capacity_rejection_has_no_effect_or_identity_poisoning() {
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let mut tools = ToolExecutor::new(1, 8, 4096).unwrap();
    tools
        .register(
            "effect",
            RegisteredTool::new(Arc::new(move |_| {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(json!({"done":true}))
                })
            })),
        )
        .unwrap();
    let mut worker = Pipeline::new(vec![Arc::new(tools)])
        .start(Default::default())
        .await
        .unwrap();
    let first = signed(801, "first", 5 * 1024 * 1024);
    send(&worker, first.clone()).await;
    recv(&mut worker, |data| {
        matches!(data, FrameData::FunctionCallResult(_))
    })
    .await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    send(&worker, signed(802, "second", 5 * 1024 * 1024)).await;
    let error = recv(&mut worker, |data| matches!(data, FrameData::Error(_))).await;
    let FrameData::Error(error) = error.data else {
        unreachable!()
    };
    assert!(error.error.contains("byte limit"));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let corrected = signed(802, "second", 0);
    send(&worker, corrected.clone()).await;
    recv(&mut worker, |data| {
        matches!(data, FrameData::FunctionCallResult(_))
    })
    .await;
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    for frame in [first.duplicate(), corrected.duplicate()] {
        send(&worker, frame).await;
        let replay = recv(&mut worker, |data| {
            matches!(data, FrameData::FunctionCallsStarted(_))
        })
        .await;
        assert!(replay.native_llm_continuation().unwrap().is_some());
    }
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(effects.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn review_mutated_signed_carrier_cannot_cancel_an_already_running_tool() {
    let effects = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let count = effects.clone();
    let released = release.clone();
    let mut tools = ToolExecutor::new(1, 8, 4096).unwrap();
    tools
        .register(
            "effect",
            RegisteredTool::new(Arc::new(move |_| {
                let count = count.clone();
                let released = released.clone();
                Box::pin(async move {
                    released.notified().await;
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(json!({"completed": true}))
                })
            })),
        )
        .unwrap();
    let mut worker = Pipeline::new(vec![Arc::new(tools)])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, signed(803, "running", 0)).await;
    recv(&mut worker, |data| {
        matches!(data, FrameData::FunctionCallInProgress(_))
    })
    .await;
    let mut invalid = signed(804, "unrelated", 0);
    invalid.data = FrameData::FunctionCallCancel(FunctionCallCancelFrame {
        function_name: "effect".into(),
        tool_call_id: "running".into(),
        run_llm: false,
    });
    send(&worker, invalid).await;
    recv(&mut worker, |data| matches!(data, FrameData::Error(_))).await;
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    release.notify_one();
    let completed = recv(&mut worker, |data| {
        matches!(data, FrameData::FunctionCallResult(_))
    })
    .await;
    let FrameData::FunctionCallResult(result) = completed.data else {
        unreachable!()
    };
    assert_eq!(result.tool_call_id, "running");
    assert_eq!(result.result, json!({"completed": true}));
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
}
