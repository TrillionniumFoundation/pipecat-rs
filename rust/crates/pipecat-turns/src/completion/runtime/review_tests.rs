//! Deterministic review of the supervisor's post-wake admission boundary.
use super::*;
use pipecat_core::{FlushFrame, Pipeline, PipelineConfig, PipelineWorker, TextFrame, WorkerState};

async fn receive(worker: &mut PipelineWorker, predicate: impl Fn(&FrameData) -> bool) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let output = worker.recv().await.expect("pipeline output");
            if predicate(&output.frame.data) {
                return;
            }
            assert!(
                !matches!(output.frame.data, FrameData::Error(_)),
                "unexpected pipeline error"
            );
        }
    })
    .await
    .expect("pipeline output deadline");
}
fn reducer_response(state: &mut State, now: Duration) {
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::llm("◐")),
        FrameData::LlmResponseEnd,
    ] {
        state
            .strategy
            .process_frame(Frame::new(data), Direction::Downstream, now)
            .unwrap();
    }
    state.epoch = state.strategy.retry_epoch;
}

#[tokio::test(start_paused = true)]
async fn obsolete_wake_preserves_rearmed_deadline_emission_context() {
    let owner = Arc::new(LLMTurnCompletionProcessor::new(Default::default()).unwrap());
    let mut worker = Pipeline::new(vec![owner.clone()])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, FrameData::is_start).await;
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::llm("◐")),
        FrameData::LlmResponseEnd,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("completion-review".into()),
        }),
    ] {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    receive(&mut worker, |data| matches!(data, FrameData::Flush(_))).await;

    {
        let mut state = lock(&owner.state).unwrap();
        assert_eq!(state.strategy.deadline(), Some(Duration::from_secs(5)));
        let expected_target = state.output.as_ref().unwrap().processor_identity();

        // The old sleep may already have won selection when another thread
        // rearms the strategy before the supervisor acquires this state lock.
        reducer_response(&mut state, Duration::from_secs(4));
        assert_eq!(state.strategy.deadline(), Some(Duration::from_secs(9)));
        assert!(state
            .take_ready_prompt(Duration::from_secs(5))
            .unwrap()
            .is_none());
        assert_eq!(
            state.output.as_ref().unwrap().processor_identity(),
            expected_target
        );
        assert_eq!(state.strategy.deadline(), Some(Duration::from_secs(9)));

        let (retry, target, epoch) = state
            .take_ready_prompt(Duration::from_secs(9))
            .unwrap()
            .expect("current wake owns retry");
        assert_eq!(target.processor_identity(), expected_target);
        assert_eq!(epoch, state.strategy.retry_epoch);
        assert!(state.output.is_none());
        assert_eq!(retry.actions.len(), 1);
        assert!(
            matches!(&retry.actions[0], AggregationAction::Push(frame) if matches!(&frame.frame.data, FrameData::LlmMessagesAppend(messages) if messages.run_llm && messages.messages.len() == 1 && messages.messages[0].role == ChatRole::Developer))
        );
        assert!(state
            .take_ready_prompt(Duration::from_secs(100))
            .unwrap()
            .is_none());
    }

    worker.end().await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(1), worker.join())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
}

#[test]
fn ready_retry_without_emission_context_is_explicit_failure() {
    let mut state = State {
        strategy: LLMTurnCompletionStrategy::new(Default::default()).unwrap(),
        epoch: 0,
        task_id: None,
        output: None,
        closing: false,
    };
    reducer_response(&mut state, Duration::ZERO);
    let error = match state.take_ready_prompt(Duration::from_secs(5)) {
        Err(error) => error,
        Ok(_) => panic!("a retry without its emission owner cannot silently succeed"),
    };
    assert!(error.fatal);
    assert!(error.message.contains("emission context"));
}
