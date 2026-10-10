//! Actual NativeWorker cancellation/replacement with delayed committed records.
//! HTTP inference is covered separately; this isolates job ownership semantics.
use pipecat_agent_workers::AssistantJobReply;
use pipecat_core::{Direction, Frame, FrameData, TextFrame};
use pipecat_turns::{AggregationAction, AggregationOutput, LLMAssistantAggregator, SharedContext};
use pipecat_workers::{JobParams, JobResponse, JobStatus, NativeWorker, WorkerRunner};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("native job ownership test stalled")
}
fn turn(assistant: &LLMAssistantAggregator, text: &str, interrupted: bool) -> AggregationOutput {
    assistant
        .process_frame(
            Frame::new(FrameData::LlmResponseStart),
            Direction::Downstream,
        )
        .unwrap();
    if !text.is_empty() {
        assistant
            .process_frame(
                Frame::new(FrameData::LlmText(TextFrame::llm(text))),
                Direction::Downstream,
            )
            .unwrap();
    }
    assistant
        .process_frame(
            Frame::new(if interrupted {
                FrameData::Interruption
            } else {
                FrameData::LlmResponseEnd
            }),
            Direction::Downstream,
        )
        .unwrap()
}
fn admit(output: AggregationOutput) {
    for action in output.actions {
        if let AggregationAction::AssistantStopped(action) = action {
            action.admit().unwrap();
        }
    }
}
struct Dropped(Arc<Notify>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn cancelled_native_job_cannot_answer_replacement_when_old_completion_arrives_late() {
    let assistant = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        Default::default(),
    ));
    let receiver = Arc::new(tokio::sync::Mutex::new(
        assistant.subscribe_stopped(Default::default()).unwrap(),
    ));
    let replacement_assistant = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        Default::default(),
    ));
    let replacement_receiver = Arc::new(tokio::sync::Mutex::new(
        replacement_assistant
            .subscribe_stopped(Default::default())
            .unwrap(),
    ));
    let delayed = Arc::new(Mutex::new(None));
    let first_ready = Arc::new(Notify::new());
    let first_dropped = Arc::new(Notify::new());
    let runner = WorkerRunner::new("independent-completion-owner", 16).unwrap();
    let a = assistant.clone();
    let r = receiver.clone();
    let replacement_a = replacement_assistant.clone();
    let replacement_r = replacement_receiver.clone();
    let d = delayed.clone();
    let ready = first_ready.clone();
    let dropped = first_dropped.clone();
    runner.add(NativeWorker::new("sensor").active(true).max_jobs(1).on_job("answer", true, move |job| {
        let assistant = a.clone();
        let receiver = r.clone();
        let replacement_assistant = replacement_a.clone();
        let replacement_receiver = replacement_r.clone();
        let delayed = d.clone();
        let ready = ready.clone();
        let dropped = dropped.clone();
        async move {
            if job.payload == Some(json!("old")) {
                let _drop = Dropped(dropped);
                let _reply = AssistantJobReply::capture(&job, &assistant, 1).unwrap();
                *delayed.lock().unwrap() = Some(turn(&assistant, "old committed answer", false));
                ready.notify_one();
                return std::future::pending::<JobResponse>().await;
            }
            assert!(assistant.bind_job_generation(2).is_err(), "old sessions cannot be rebound even after their native job was canceled");
            let mut reply = AssistantJobReply::capture(&job, &replacement_assistant, 1).unwrap();
            admit(delayed.lock().unwrap().take().expect("old committed output is held"));
            let mut receiver = receiver.lock().await;
            let old = receiver.recv().await.unwrap();
            assert_eq!(old.completion().text(), "old committed answer");
            assert_eq!(old.completion().job_generation(), Some(1));
            assert_ne!(old.completion().session_id(), reply.correlation().session_id);
            assert!(reply.try_response(&job, old.completion()).unwrap().is_none(), "delayed old data cannot answer the replacement native job");
            drop(old);
            drop(receiver);
            let mut receiver = replacement_receiver.lock().await;
            admit(turn(&replacement_assistant, "", false));
            let empty = receiver.recv().await.unwrap();
            assert!(reply.try_response(&job, empty.completion()).unwrap().is_none(), "empty tool-only round cannot finish the native job");
            drop(empty);
            admit(turn(&replacement_assistant, "interrupted partial", true));
            let interrupted = receiver.recv().await.unwrap();
            assert!(reply.try_response(&job, interrupted.completion()).unwrap().is_none());
            drop(interrupted);
            admit(turn(&replacement_assistant, "replacement committed answer", false));
            let fresh = receiver.recv().await.unwrap();
            assert_eq!(fresh.completion().job_generation(), Some(1));
            let mut wrong = job.clone();
            wrong.job_id.push_str("-another-job");
            assert!(reply.try_response(&wrong, fresh.completion()).is_err());
            let response = reply.try_response(&job, fresh.completion()).unwrap().expect("fresh correlated record answers native job");
            assert!(reply.try_response(&job, fresh.completion()).unwrap().is_none(), "one binding returns one successful response");
            response
        }
    }).unwrap()).unwrap();
    let client = runner.client("requester");
    let params = |payload| JobParams {
        name: Some("answer".into()),
        payload: Some(json!(payload)),
        timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    let mut old = client.request("sensor", params("old")).unwrap();
    bounded(first_ready.notified()).await;
    old.cancel(Some("replace request".into())).await.unwrap();
    assert_eq!(
        bounded(old.result()).await.unwrap().status,
        JobStatus::Cancelled
    );
    bounded(first_dropped.notified()).await;
    assert_eq!(assistant.stopped_stats().unwrap().retained_records, 1);
    let mut replacement = client.request("sensor", params("replacement")).unwrap();
    assert_eq!(
        bounded(replacement.result()).await.unwrap(),
        JobResponse::completed("replacement committed answer")
    );
    assert_eq!(
        bounded(old.result()).await.unwrap().status,
        JobStatus::Cancelled
    );
    let stats = assistant.stopped_stats().unwrap();
    assert_eq!(stats.delivered, 1);
    assert_eq!(stats.retained_records, 0);
    assert_eq!(stats.retained_bytes, 0);
    let replacement_stats = replacement_assistant.stopped_stats().unwrap();
    assert_eq!(replacement_stats.delivered, 3);
    assert_eq!(replacement_stats.retained_records, 0);
    assert_eq!(replacement_stats.retained_bytes, 0);
    bounded(runner.end()).await.unwrap();
}
