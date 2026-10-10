use pipecat_core::*;
use pipecat_services::{Content, Context};
use pipecat_turns::*;
use std::{sync::Arc, time::Duration};
const LIMIT: Duration = Duration::from_secs(3);
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) {
    let frame = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("stopped-runtime-barrier".into()),
        returning: false,
    }));
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(LIMIT, async {
        while let Some(frame) = worker.recv().await {
            if frame.frame.id == id {
                return;
            }
        }
        panic!("pipeline ended before barrier");
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn full_completion_receiver_preserves_terminal_history_and_closes_on_real_lanes() {
    for (terminal, expected) in [
        (
            FrameData::End(Default::default()),
            AssistantStoppedClose::End,
        ),
        (FrameData::Stop, AssistantStoppedClose::Stop),
        (
            FrameData::Cancel(Default::default()),
            AssistantStoppedClose::Cancel,
        ),
    ] {
        let shared = SharedContext::new(Context::default());
        let assistant = Arc::new(LLMAssistantAggregator::new(
            shared.clone(),
            Default::default(),
        ));
        let mut receiver = assistant
            .subscribe_stopped(AssistantStoppedConfig {
                max_records: 1,
                ..Default::default()
            })
            .unwrap();
        let mut worker = Pipeline::new(vec![assistant])
            .start(Default::default())
            .await
            .unwrap();
        barrier(&mut worker).await;
        send(&worker, FrameData::LlmResponseStart).await;
        send(&worker, FrameData::LlmText(TextFrame::llm("queued answer"))).await;
        send(&worker, FrameData::LlmResponseEnd).await;
        barrier(&mut worker).await;
        assert_eq!(receiver.stats().queued_records, 1);
        send(&worker, FrameData::LlmResponseStart).await;
        send(
            &worker,
            FrameData::LlmText(TextFrame::llm("terminal answer")),
        )
        .await;
        barrier(&mut worker).await;
        send(&worker, terminal).await;
        let _ = tokio::time::timeout(LIMIT, worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receiver.closed_reason(), Some(expected));
        assert_eq!(receiver.stats().rejected, 1);
        assert_eq!(
            receiver.recv().await.unwrap().completion().text(),
            "queued answer"
        );
        assert!(receiver.recv().await.is_none());
        assert_eq!(receiver.stats().retained_records, 0);
        let text: Vec<_> = shared
            .snapshot()
            .unwrap()
            .messages
            .into_iter()
            .map(|message| message.content)
            .collect();
        assert_eq!(
            text,
            [
                Some(Content::Text("queued answer".into())),
                Some(Content::Text("terminal answer".into()))
            ]
        );
    }
}
#[tokio::test]
async fn caller_owned_handler_reads_committed_context_and_queues_without_held_locks() {
    let shared = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let mut worker = Pipeline::new(vec![assistant])
        .start(Default::default())
        .await
        .unwrap();
    barrier(&mut worker).await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::LlmText(TextFrame::llm("committed before handler")),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    barrier(&mut worker).await;
    let sender = worker.sender();
    assert!(tokio::time::timeout(
        LIMIT,
        receiver.handle_next(move |completion| async move {
            let snapshot = shared.snapshot()?;
            assert!(snapshot
                .messages
                .iter()
                .any(|message| message.content == Some(Content::Text(completion.text().into()))));
            sender
                .send(Frame::text("caller-owned effect"), Direction::Downstream)
                .await
                .map_err(|_| ProcessorError::new("caller pipeline closed"))?;
            Ok(())
        })
    )
    .await
    .unwrap()
    .unwrap());
    barrier(&mut worker).await;
    assert_eq!(receiver.stats().handled, 1);
    assert_eq!(receiver.stats().in_flight_handlers, 0);
    assert_eq!(receiver.stats().retained_records, 0);
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}
#[tokio::test]
async fn dropping_pipeline_owner_closes_dormant_receiver_without_dispatcher_task() {
    let assistant = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        Default::default(),
    ));
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let mut worker = Pipeline::new(vec![assistant.clone()])
        .start(Default::default())
        .await
        .unwrap();
    barrier(&mut worker).await;
    drop(worker);
    assert!(tokio::time::timeout(LIMIT, receiver.recv())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        receiver.closed_reason(),
        Some(AssistantStoppedClose::Cleanup)
    );
    assistant.cleanup().await.unwrap();
    assert_eq!(receiver.stats().retained_records, 0);
}
