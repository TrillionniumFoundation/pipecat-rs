//! Committed feedback through native normal/system lanes; no callback dispatcher.
use pipecat_core::*;
use pipecat_services::{Content, Context};
use pipecat_turns::*;
use std::{sync::Arc, time::Duration};

async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let frame = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("feedback-test".into()),
        returning: false,
    }));
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut frames = vec![];
        while let Some(frame) = worker.recv().await {
            if frame.frame.id == id {
                return frames;
            }
            frames.push(frame);
        }
        panic!("worker ended before barrier");
    })
    .await
    .unwrap()
}
fn feedback(frames: &[DirectedFrame]) -> Vec<&DirectedFrame> {
    frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_)))
        .collect()
}
fn assert_pair(frames: &[DirectedFrame], text: &str) {
    let turns = feedback(frames);
    assert_eq!(turns.len(), 2);
    assert_ne!(turns[0].direction, turns[1].direction);
    assert_eq!(turns[0].frame.broadcast_sibling_id, Some(turns[1].frame.id));
    assert_eq!(turns[1].frame.broadcast_sibling_id, Some(turns[0].frame.id));
    assert!(turns
        .iter()
        .all(|f| matches!(&f.frame.data, FrameData::LlmContextAssistantTurn(t) if t.text == text)));
}

#[tokio::test]
async fn compact_detailed_and_vision_response_boundaries_use_one_commit_each() {
    let a = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        Default::default(),
    ));
    let mut worker = Pipeline::new(vec![a.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    barrier(&mut worker).await;
    for (start, text, end) in [
        (
            FrameData::LlmResponseStart,
            FrameData::LlmText(TextFrame::llm("accepted")),
            FrameData::LlmResponseEnd,
        ),
        (
            FrameData::LlmFullResponseStart(Default::default()),
            FrameData::LlmText(TextFrame::llm("accepted")),
            FrameData::LlmFullResponseEnd(Default::default()),
        ),
        (
            FrameData::VisionFullResponseStart(Default::default()),
            FrameData::VisionText(TextFrame::llm("accepted")),
            FrameData::VisionFullResponseEnd(Default::default()),
        ),
    ] {
        send(&worker, start).await;
        send(&worker, text).await;
        assert!(feedback(&barrier(&mut worker).await).is_empty());
        send(&worker, end).await;
        let frames = barrier(&mut worker).await;
        assert_pair(&frames, "accepted");
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTimestamp { .. }))
                .count(),
            1
        );
    }
    assert_eq!(a.context().snapshot().unwrap().messages.len(), 3);
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}

#[tokio::test]
async fn paired_typed_and_vad_interruptions_deliver_one_partial_turn_on_real_lanes() {
    for trigger in [
        FrameData::InputText(TextFrame::new("new user turn")),
        FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        }),
    ] {
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            Default::default(),
            Default::default(),
        );
        let mut worker = Pipeline::new(vec![pair.user(), pair.assistant()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        barrier(&mut worker).await;
        send(&worker, FrameData::LlmResponseStart).await;
        send(
            &worker,
            FrameData::LlmText(TextFrame::llm("partial answer")),
        )
        .await;
        barrier(&mut worker).await;
        send(&worker, trigger).await;
        let frames = barrier(&mut worker).await;
        assert_pair(&frames, "partial answer");
        assert_eq!(
            pair.context().snapshot().unwrap().messages[0].content,
            Some(Content::Text("partial answer".into()))
        );
        send(&worker, FrameData::Interruption).await;
        assert!(feedback(&barrier(&mut worker).await).is_empty());
        worker.end().await.unwrap();
        let (frames, report) = worker.collect().await.unwrap();
        assert!(feedback(&frames).is_empty());
        assert!(report.errors.is_empty());
    }
}

#[tokio::test]
async fn active_turn_terminal_flushes_commit_before_the_owned_terminal() {
    for terminal in [
        FrameData::End(EndFrame::default()),
        FrameData::EndDetailed(ReasonFrame::default()),
        FrameData::Stop,
        FrameData::Cancel(CancelFrame::default()),
        FrameData::CancelDetailed(ReasonFrame::default()),
    ] {
        let a = Arc::new(LLMAssistantAggregator::new(
            SharedContext::default(),
            Default::default(),
        ));
        let mut worker = Pipeline::new(vec![a.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        barrier(&mut worker).await;
        send(&worker, FrameData::LlmResponseStart).await;
        send(
            &worker,
            FrameData::LlmText(TextFrame::llm("accepted prefix")),
        )
        .await;
        barrier(&mut worker).await;
        send(&worker, terminal).await;
        let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty());
        assert_pair(&frames, "accepted prefix");
        let terminal_index = frames.iter().position(|f| f.frame.is_terminal()).unwrap();
        assert!(frames.iter().enumerate().all(|(i, f)| !matches!(
            f.frame.data,
            FrameData::LlmContextAssistantTurn(_) | FrameData::LlmContextAssistantTimestamp { .. }
        ) || i < terminal_index));
        assert_eq!(a.context().snapshot().unwrap().messages.len(), 1);
        a.cleanup().await.unwrap();
        a.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn full_feedback_output_fails_boundedly_without_undoing_committed_history() {
    let a = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        Default::default(),
    ));
    let mut worker = Pipeline::new(vec![a.clone()])
        .start(PipelineConfig {
            output_capacity: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Start(_)
    ));
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::LlmText(TextFrame::llm("committed before backpressure")),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let result = tokio::time::timeout(Duration::from_secs(3), worker.join())
        .await
        .expect("bounded output failure");
    assert!(matches!(result, Err(PipelineError::Backpressure(_))));
    assert_eq!(
        a.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("committed before backpressure".into()))
    );
}
