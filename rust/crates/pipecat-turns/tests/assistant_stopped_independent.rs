//! Independent bounded-record, ordering, and lifecycle controls.
//! Source-prepared during the global execution hold; run only on a frozen candidate.
use pipecat_core::*;
use pipecat_services::{Content, Role};
use pipecat_turns::*;
use std::sync::Arc;
use std::time::Duration;

fn configured(
    config: AssistantStoppedConfig,
) -> (LLMAssistantAggregator, AssistantStoppedReceiver) {
    let assistant = LLMAssistantAggregator::new(SharedContext::default(), Default::default());
    let receiver = assistant.subscribe_stopped(config).unwrap();
    (assistant, receiver)
}
fn reduce(assistant: &LLMAssistantAggregator, data: FrameData) -> AggregationOutput {
    assistant
        .process_frame(Frame::new(data), Direction::Downstream)
        .unwrap()
}
fn turn(assistant: &LLMAssistantAggregator, text: &str) -> AggregationOutput {
    reduce(assistant, FrameData::LlmResponseStart);
    if !text.is_empty() {
        reduce(assistant, FrameData::LlmText(TextFrame::llm(text)));
    }
    reduce(assistant, FrameData::LlmResponseEnd)
}
fn admissions(output: &AggregationOutput) -> Vec<&AssistantStoppedAction> {
    output
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::AssistantStopped(action) => Some(action),
            _ => None,
        })
        .collect()
}
fn admit(output: &AggregationOutput) {
    for action in admissions(output) {
        action.admit().unwrap();
    }
}
fn history(assistant: &LLMAssistantAggregator) -> Vec<String> {
    assistant
        .context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .filter_map(|message| {
            assert_eq!(message.role, Role::Assistant);
            match message.content {
                Some(Content::Text(text)) => Some(text),
                _ => None,
            }
        })
        .collect()
}
fn has_error(output: &AggregationOutput) -> bool {
    output.actions.iter().any(|action| matches!(action, AggregationAction::Push(frame) if matches!(frame.frame.data, FrameData::Error(_))))
}

#[test]
fn cloned_reducer_output_has_one_admission_and_one_budget_lease() {
    let (assistant, mut receiver) = configured(Default::default());
    let output = turn(&assistant, "one committed answer");
    assert_eq!(admissions(&output).len(), 1);
    let clone = output.clone();
    assert_eq!(receiver.stats().retained_records, 1);
    assert_eq!(receiver.stats().queued_records, 0);
    admit(&clone);
    admit(&output);
    admit(&clone);
    assert_eq!(receiver.stats().delivered, 1);
    assert_eq!(receiver.stats().queued_records, 1);
    drop(output);
    drop(clone);
    let record = receiver.try_recv().unwrap();
    assert_eq!(record.completion().text(), "one committed answer");
    assert_eq!(receiver.stats().retained_records, 1);
    assert_eq!(receiver.stats().queued_records, 0);
    assert!(receiver.try_recv().is_none());
    drop(record);
    assert_eq!(receiver.stats().retained_records, 0);
    assert_eq!(receiver.stats().retained_bytes, 0);
    assert_eq!(receiver.stats().abandoned, 0);
}

#[test]
fn unadmitted_reservation_is_released_only_by_last_output_clone() {
    let (assistant, receiver) = configured(Default::default());
    let output = turn(&assistant, "reserved");
    let clone = output.clone();
    drop(output);
    assert_eq!(receiver.stats().retained_records, 1);
    drop(clone);
    let stats = receiver.stats();
    assert_eq!(stats.retained_records, 0);
    assert_eq!(stats.retained_bytes, 0);
    assert_eq!(stats.abandoned, 1);
    assert_eq!(stats.delivered, 0);
    assert_eq!(history(&assistant), ["reserved"]);
}

#[test]
fn count_budget_covers_reserved_queued_and_borrowed_records() {
    let (assistant, mut receiver) = configured(AssistantStoppedConfig {
        max_records: 1,
        ..Default::default()
    });
    let first = turn(&assistant, "first");
    let reserved_rejection = turn(&assistant, "reserved rejection");
    assert!(has_error(&reserved_rejection));
    assert_eq!(receiver.stats().rejected, 1);
    admit(&first);
    let queued_rejection = turn(&assistant, "queued rejection");
    assert!(has_error(&queued_rejection));
    let record = receiver.try_recv().unwrap();
    assert_eq!(
        receiver.stats().retained_bytes,
        record.completion().text().len() + record.completion().started_at().len()
    );
    let borrowed_rejection = turn(&assistant, "borrowed rejection");
    assert!(has_error(&borrowed_rejection));
    assert_eq!(receiver.stats().rejected, 3);
    assert_eq!(
        history(&assistant),
        [
            "first",
            "reserved rejection",
            "queued rejection",
            "borrowed rejection"
        ]
    );
    drop(record);
    let next = turn(&assistant, "recovered");
    assert!(!has_error(&next));
    admit(&next);
    assert_eq!(
        receiver.try_recv().unwrap().completion().text(),
        "recovered"
    );
    assert_eq!(receiver.stats().retained_records, 0);
}

#[test]
fn byte_rejection_keeps_complete_context_without_successful_truncation() {
    let (assistant, mut receiver) = configured(AssistantStoppedConfig {
        max_bytes: 1,
        ..Default::default()
    });
    let output = turn(&assistant, "你好🙂 authoritative complete text");
    assert!(has_error(&output));
    assert!(admissions(&output).is_empty());
    assert!(receiver.try_recv().is_none());
    assert_eq!(receiver.stats().rejected, 1);
    assert_eq!(receiver.stats().retained_records, 0);
    assert_eq!(history(&assistant), ["你好🙂 authoritative complete text"]);
}

#[test]
fn committed_context_timestamp_admission_and_feedback_keep_exact_order() {
    let (assistant, mut receiver) = configured(Default::default());
    let before_start = chrono::Utc::now();
    reduce(&assistant, FrameData::LlmResponseStart);
    let after_start = chrono::Utc::now();
    reduce(
        &assistant,
        FrameData::LlmText(TextFrame::llm("●  kept ◐ text ○")),
    );
    let output = reduce(&assistant, FrameData::LlmResponseEnd);
    assert_eq!(history(&assistant), ["●  kept ◐ text ○"]);
    let index =
        |matches: fn(&AggregationAction) -> bool| output.actions.iter().position(matches).unwrap();
    let context_index = index(
        |a| matches!(a, AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_))),
    );
    let timestamp_index = index(
        |a| matches!(a, AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmContextAssistantTimestamp { .. })),
    );
    let admission_index = index(|a| matches!(a, AggregationAction::AssistantStopped(_)));
    let feedback_index = index(
        |a| matches!(a, AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_))),
    );
    assert!(
        context_index < timestamp_index
            && timestamp_index < admission_index
            && admission_index < feedback_index
    );
    admit(&output);
    let record = receiver.try_recv().unwrap();
    let completion = record.completion();
    let started = chrono::DateTime::parse_from_rfc3339(completion.started_at())
        .unwrap()
        .with_timezone(&chrono::Utc);
    assert!(started >= before_start && started <= after_start);
    assert_eq!(completion.text(), "kept  text");
    let feedback = output
        .actions
        .iter()
        .find_map(|action| match action {
            AggregationAction::Push(f) => match &f.frame.data {
                FrameData::LlmContextAssistantTurn(turn) => Some(turn),
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    assert_eq!(feedback.timestamp, completion.started_at());
    assert_eq!(feedback.text, completion.text());
}

#[test]
fn active_empty_marker_only_same_text_and_duplicate_terminal_are_distinct() {
    let (assistant, mut receiver) = configured(Default::default());
    admit(&turn(&assistant, ""));
    let empty = receiver.try_recv().unwrap();
    assert_eq!(empty.completion().text(), "");
    assert!(!empty.completion().interrupted());
    admit(&turn(&assistant, "●◐○"));
    let markers = receiver.try_recv().unwrap();
    assert_eq!(markers.completion().text(), "");
    assert_eq!(history(&assistant), ["●◐○"]);
    admit(&turn(&assistant, "same"));
    let first = receiver.try_recv().unwrap();
    let end = Frame::new(FrameData::LlmResponseEnd);
    assert!(admissions(
        &assistant
            .process_frame(end.clone(), Direction::Downstream)
            .unwrap()
    )
    .is_empty());
    assert!(admissions(&assistant.process_frame(end, Direction::Downstream).unwrap()).is_empty());
    admit(&turn(&assistant, "same"));
    let second = receiver.try_recv().unwrap();
    assert_eq!(first.completion().text(), second.completion().text());
    assert_eq!(
        first.completion().session_id(),
        second.completion().session_id()
    );
    assert_ne!(first.completion().turn_id(), second.completion().turn_id());
    assert!(empty.completion().turn_id() < markers.completion().turn_id());
    assert_eq!(receiver.stats().delivered, 4);
}

#[test]
fn delayed_old_record_keeps_captured_job_generation() {
    let (assistant, mut receiver) = configured(Default::default());
    let old_job = assistant.bind_job_generation(41).unwrap();
    let old = turn(&assistant, "old answer");
    assert!(
        assistant.bind_job_generation(42).is_err(),
        "one assistant session belongs to one job"
    );
    let (replacement_assistant, mut replacement_receiver) = configured(Default::default());
    let replacement_job = replacement_assistant.bind_job_generation(41).unwrap();
    let replacement = turn(&replacement_assistant, "replacement answer");
    admit(&old);
    let old_record = receiver.try_recv().unwrap();
    assert!(old_job.matches(old_record.completion()));
    assert!(!replacement_job.matches(old_record.completion()));
    admit(&replacement);
    let replacement_record = replacement_receiver.try_recv().unwrap();
    assert!(replacement_job.matches(replacement_record.completion()));
    assert!(!old_job.matches(replacement_record.completion()));
    assert_eq!(old_record.completion().job_generation(), Some(41));
    assert_eq!(replacement_record.completion().job_generation(), Some(41));
    assert_ne!(
        old_record.completion().session_id(),
        replacement_record.completion().session_id()
    );
    assert!(
        assistant.bind_job_generation(41).is_err(),
        "generation reuse must not revive old correlation"
    );
}

#[tokio::test]
async fn never_polled_handler_and_caller_cancellation_release_exact_ownership() {
    let (assistant, mut receiver) = configured(Default::default());
    admit(&turn(&assistant, "first"));
    let future = receiver.handle_next(|_| async {
        panic!("unpolled handler ran");
    });
    drop(future);
    assert_eq!(receiver.stats().queued_records, 1);
    assert_eq!(receiver.stats().in_flight_handlers, 0);
    assert_eq!(receiver.stats().handler_cancelled, 0);
    let entered = Arc::new(tokio::sync::Notify::new());
    let notify = entered.clone();
    let mut future = Box::pin(receiver.handle_next(move |_| async move {
        notify.notify_one();
        std::future::pending::<Result<(), ProcessorError>>().await
    }));
    tokio::select! {
        result = &mut future => panic!("handler unexpectedly ended: {result:?}"),
        _ = entered.notified() => {}
    }
    drop(future);
    let stats = receiver.stats();
    assert_eq!(stats.retained_records, 0);
    assert_eq!(stats.retained_bytes, 0);
    assert_eq!(stats.queued_records, 0);
    assert_eq!(stats.in_flight_handlers, 0);
    assert_eq!(stats.handler_cancelled, 1);
}

#[tokio::test]
async fn handler_errors_panics_timeout_and_success_are_sanitized_and_bounded() {
    let (assistant, mut receiver) = configured(AssistantStoppedConfig {
        handler_timeout: Duration::from_millis(100),
        ..Default::default()
    });
    admit(&turn(&assistant, "constructor panic"));
    let error = receiver
        .handle_next(|_| -> std::future::Ready<Result<(), ProcessorError>> {
            panic!("private constructor payload")
        })
        .await
        .unwrap_err();
    assert_eq!(error.message, "assistant-stopped handler panicked");
    admit(&turn(&assistant, "poll panic"));
    let error = receiver
        .handle_next(|_| async {
            tokio::task::yield_now().await;
            panic!("private poll payload");
        })
        .await
        .unwrap_err();
    assert_eq!(error.message, "assistant-stopped handler panicked");
    admit(&turn(&assistant, "ordinary error"));
    let error = receiver
        .handle_next(|_| async { Err(ProcessorError::fatal("private tool transcript")) })
        .await
        .unwrap_err();
    assert_eq!(error.message, "assistant-stopped handler failed");
    admit(&turn(&assistant, "timeout"));
    let error = receiver
        .handle_next(|_| std::future::pending())
        .await
        .unwrap_err();
    assert_eq!(error.message, "assistant-stopped handler timed out");
    admit(&turn(&assistant, "committed and visible"));
    let shared = assistant.context();
    assert!(receiver.handle_next(move |completion| async move {
        let snapshot = shared.snapshot()?;
        assert!(matches!(&snapshot.messages.last().unwrap().content, Some(Content::Text(text)) if text == completion.text()));
        Ok(())
    }).await.unwrap());
    let stats = receiver.stats();
    assert_eq!(stats.handler_panics, 2);
    assert_eq!(stats.handler_errors, 1);
    assert_eq!(stats.handler_timeouts, 1);
    assert_eq!(stats.handled, 1);
    assert_eq!(stats.handler_cancelled, 0);
    assert_eq!(stats.retained_records, 0);
    assert_eq!(stats.retained_bytes, 0);
    assert_eq!(stats.in_flight_handlers, 0);
}

#[tokio::test]
async fn terminal_record_precedes_close_and_repeated_cleanup_is_inert() {
    for (terminal, close, interrupted) in [
        (
            FrameData::End(Default::default()),
            AssistantStoppedClose::End,
            false,
        ),
        (
            FrameData::EndDetailed(Default::default()),
            AssistantStoppedClose::End,
            false,
        ),
        (FrameData::Stop, AssistantStoppedClose::Stop, false),
        (
            FrameData::Cancel(Default::default()),
            AssistantStoppedClose::Cancel,
            true,
        ),
        (
            FrameData::CancelDetailed(Default::default()),
            AssistantStoppedClose::Cancel,
            true,
        ),
    ] {
        let (assistant, mut receiver) = configured(Default::default());
        reduce(&assistant, FrameData::LlmResponseStart);
        reduce(
            &assistant,
            FrameData::LlmText(TextFrame::llm("terminal prefix")),
        );
        let output = reduce(&assistant, terminal);
        admit(&output);
        assert_eq!(receiver.closed_reason(), Some(close));
        let record = receiver.recv().await.unwrap();
        assert_eq!(record.completion().text(), "terminal prefix");
        assert_eq!(record.completion().interrupted(), interrupted);
        assert!(receiver.recv().await.is_none());
        assistant.cleanup().await.unwrap();
        assistant.cleanup().await.unwrap();
        assert_eq!(receiver.closed_reason(), Some(close));
        assert_eq!(receiver.stats().delivered, 1);
    }
}

#[tokio::test]
async fn abandoned_terminal_action_and_owner_drop_close_without_tasks() {
    let (assistant, mut receiver) = configured(Default::default());
    let output = reduce(&assistant, FrameData::End(Default::default()));
    let clone = output.clone();
    drop(output);
    assert_eq!(receiver.closed_reason(), None);
    drop(clone);
    assert_eq!(receiver.closed_reason(), Some(AssistantStoppedClose::End));
    assert!(receiver.recv().await.is_none());
    let (assistant, mut receiver) = configured(Default::default());
    drop(assistant);
    assert_eq!(
        receiver.closed_reason(),
        Some(AssistantStoppedClose::OwnerDropped)
    );
    assert!(receiver.recv().await.is_none());
}

#[test]
fn receiver_registration_is_single_and_receiver_drop_fails_without_rollback() {
    let (assistant, receiver) = configured(Default::default());
    assert!(assistant.subscribe_stopped(Default::default()).is_err());
    let output = turn(&assistant, "unread committed");
    admit(&output);
    drop(receiver);
    let stats = assistant.stopped_stats().unwrap();
    assert_eq!(stats.retained_records, 0);
    assert_eq!(stats.retained_bytes, 0);
    assert_eq!(stats.queued_records, 0);
    let rejected = turn(&assistant, "after receiver drop");
    assert!(has_error(&rejected));
    assert_eq!(
        history(&assistant),
        ["unread committed", "after receiver drop"]
    );
    assert!(assistant.subscribe_stopped(Default::default()).is_err());
}

#[test]
fn invalid_registration_and_active_rebinding_do_not_consume_identity() {
    let assistant = LLMAssistantAggregator::new(SharedContext::default(), Default::default());
    for config in [
        AssistantStoppedConfig {
            max_records: 0,
            ..Default::default()
        },
        AssistantStoppedConfig {
            max_records: 1025,
            ..Default::default()
        },
        AssistantStoppedConfig {
            max_bytes: 0,
            ..Default::default()
        },
        AssistantStoppedConfig {
            handler_timeout: Duration::ZERO,
            ..Default::default()
        },
        AssistantStoppedConfig {
            handler_timeout: Duration::from_secs(61),
            ..Default::default()
        },
    ] {
        assert!(assistant.subscribe_stopped(config).is_err());
    }
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let first = assistant.bind_job_generation(9).unwrap();
    assert!(
        assistant.bind_job_generation(10).is_err(),
        "the captured request has not started yet"
    );
    reduce(&assistant, FrameData::LlmResponseStart);
    assert!(assistant.bind_job_generation(10).is_err());
    admit(&reduce(&assistant, FrameData::LlmResponseEnd));
    let record = receiver.try_recv().unwrap();
    assert!(first.matches(record.completion()));
    assert!(
        assistant.bind_job_generation(10).is_err(),
        "even a completed turn cannot rebind its assistant session"
    );
    let late = LLMAssistantAggregator::new(SharedContext::default(), Default::default());
    reduce(&late, FrameData::LlmResponseStart);
    reduce(&late, FrameData::LlmResponseEnd);
    assert!(late.subscribe_stopped(Default::default()).is_err());
    assert!(
        late.bind_job_generation(1).is_err(),
        "a used unbound session cannot adopt a later job"
    );
    let (other, _) = configured(Default::default());
    let other_job = other.bind_job_generation(9).unwrap();
    assert_ne!(other_job.session_id, first.session_id);
    assert!(!other_job.matches(record.completion()));
}

async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let frame = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("independent-stopped-barrier".into()),
        returning: false,
    }));
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut seen = vec![];
        while let Some(frame) = worker.recv().await {
            if frame.frame.id == id {
                return seen;
            }
            seen.push(frame);
        }
        panic!("pipeline closed before independent barrier");
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn real_paired_user_interruptions_preserve_old_generation_record() {
    for trigger in [
        FrameData::InputText(TextFrame::new("new question")),
        FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        }),
    ] {
        let pair = LLMContextAggregatorPair::new(
            Default::default(),
            Default::default(),
            Default::default(),
        );
        let assistant = pair.assistant();
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let old_job = assistant.bind_job_generation(3).unwrap();
        let mut worker = Pipeline::new(vec![pair.user(), assistant.clone()])
            .start(Default::default())
            .await
            .unwrap();
        barrier(&mut worker).await;
        send(&worker, FrameData::LlmResponseStart).await;
        send(
            &worker,
            FrameData::LlmText(TextFrame::llm("● partial answer")),
        )
        .await;
        barrier(&mut worker).await;
        send(&worker, trigger).await;
        let frames = barrier(&mut worker).await;
        assert!(assistant.bind_job_generation(4).is_err());
        let replacement_assistant =
            LLMAssistantAggregator::new(SharedContext::default(), Default::default());
        let replacement_job = replacement_assistant.bind_job_generation(3).unwrap();
        let record = receiver.try_recv().unwrap();
        assert!(record.completion().interrupted());
        assert_eq!(record.completion().text(), "partial answer");
        assert!(old_job.matches(record.completion()));
        assert!(!replacement_job.matches(record.completion()));
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_)))
                .count(),
            2
        );
        assert!(receiver.try_recv().is_none());
        send(&worker, FrameData::Interruption).await;
        barrier(&mut worker).await;
        assert!(receiver.try_recv().is_none());
        worker.end().await.unwrap();
        let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty());
        assert_eq!(receiver.stats().delivered, 1);
    }
}

struct CancelAdmissionProbe {
    assistant: Arc<LLMAssistantAggregator>,
    rejected: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl FrameProcessor for CancelAdmissionProbe {
    fn name(&self) -> &str {
        "IndependentStoppedCancelProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            let rejected = ctx
                .spawn("must-not-run-after-cancel", false, async {
                    panic!("cancel accepted new task");
                })
                .is_err();
            self.rejected
                .store(rejected, std::sync::atomic::Ordering::SeqCst);
            assert!(rejected, "fixture must reach closed runtime task admission");
        }
        self.assistant.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.assistant.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.assistant.cleanup().await
    }
}

#[tokio::test]
async fn actual_cancel_delivers_terminal_record_after_task_admission_is_closed() {
    for cancel in [
        FrameData::Cancel(Default::default()),
        FrameData::CancelDetailed(Default::default()),
    ] {
        let assistant = Arc::new(LLMAssistantAggregator::new(
            SharedContext::default(),
            Default::default(),
        ));
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let rejected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = Arc::new(CancelAdmissionProbe {
            assistant: assistant.clone(),
            rejected: rejected.clone(),
        });
        let mut worker = Pipeline::new(vec![probe])
            .start(Default::default())
            .await
            .unwrap();
        barrier(&mut worker).await;
        send(&worker, FrameData::LlmResponseStart).await;
        send(&worker, FrameData::LlmText(TextFrame::llm("cancel prefix"))).await;
        barrier(&mut worker).await;
        send(&worker, cancel).await;
        let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty());
        assert!(rejected.load(std::sync::atomic::Ordering::SeqCst));
        let record = receiver.try_recv().unwrap();
        assert_eq!(record.completion().text(), "cancel prefix");
        assert!(record.completion().interrupted());
        assert_eq!(
            receiver.closed_reason(),
            Some(AssistantStoppedClose::Cancel)
        );
        assert!(receiver.recv().await.is_none());
        assert_eq!(receiver.stats().in_flight_handlers, 0);
        assert_eq!(history(&assistant), ["cancel prefix"]);
    }
}

#[derive(Default)]
struct CaptureFence(std::sync::Mutex<Option<InvocationFence>>);
#[async_trait]
impl FrameProcessor for CaptureFence {
    fn name(&self) -> &str {
        "IndependentStoppedFenceCapture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.0.lock().unwrap() = Some(ctx.invocation_fence()?);
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn stale_receipt_fence_suppresses_success_but_not_interrupted_transcript() {
    let capture = Arc::new(CaptureFence::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    barrier(&mut worker).await;
    send(
        &worker,
        FrameData::Text(TextFrame::new("capture old receipt scope")),
    )
    .await;
    barrier(&mut worker).await;
    let old_fence = capture.0.lock().unwrap().take().unwrap();
    assert!(old_fence.is_current());
    let (assistant, mut receiver) = configured(Default::default());
    let successful = turn(&assistant, "already committed success");
    send(&worker, FrameData::Interruption).await;
    barrier(&mut worker).await;
    assert!(!old_fence.is_current());
    for action in admissions(&successful) {
        action.admit_with_fence(&old_fence).unwrap();
    }
    assert!(receiver.try_recv().is_none());
    assert_eq!(receiver.stats().stale, 1);
    assert_eq!(receiver.stats().retained_records, 0);
    assert_eq!(history(&assistant), ["already committed success"]);
    reduce(&assistant, FrameData::LlmResponseStart);
    reduce(
        &assistant,
        FrameData::LlmText(TextFrame::llm("interrupted prefix")),
    );
    let interrupted = reduce(&assistant, FrameData::Interruption);
    for action in admissions(&interrupted) {
        action.admit_with_fence(&old_fence).unwrap();
    }
    let record = receiver.try_recv().unwrap();
    assert!(record.completion().interrupted());
    assert_eq!(record.completion().text(), "interrupted prefix");
    assert_eq!(receiver.stats().delivered, 1);
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty());
}
