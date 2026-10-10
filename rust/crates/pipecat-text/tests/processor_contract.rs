use pipecat_core::*;
use pipecat_text::*;
use std::{sync::Arc, time::Duration};
fn llm(text: &str) -> Frame {
    Frame::new(FrameData::LlmText(TextFrame::llm(text)))
}
fn aggregated(frame: &DirectedFrame) -> &AggregatedTextFrame {
    if let FrameData::AggregatedText(a) = &frame.frame.data {
        a
    } else {
        panic!("expected aggregation: {frame:?}")
    }
}
#[test]
fn llm_processor_preserves_pattern_raw_text_and_skip_tts() {
    let mut patterns = PatternPairAggregator::default();
    patterns
        .add_pattern("code", "<code>", "</code>", MatchAction::Aggregate)
        .unwrap();
    let processor = LlmTextProcessor::new(patterns);
    let mut text = TextFrame::llm("Before <code>  x = 1  </code> after");
    text.skip_tts = Some(true);
    let out = processor
        .process_frame(Frame::new(FrameData::LlmText(text)), Direction::Upstream)
        .unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(aggregated(&out[0]).text.text, "Before");
    assert_eq!(aggregated(&out[1]).text.text, "x = 1");
    assert_eq!(
        aggregated(&out[1]).raw_text.as_deref(),
        Some("<code>  x = 1  </code>")
    );
    assert_eq!(aggregated(&out[1]).text.skip_tts, Some(true));
    assert!(out.iter().all(|f| f.direction == Direction::Downstream));
    let out = processor
        .process_frame(
            Frame::new(FrameData::LlmFullResponseEnd(LlmFullResponseFrame {
                skip_tts: Some(false),
            })),
            Direction::Upstream,
        )
        .unwrap();
    assert_eq!(aggregated(&out[0]).text.text, "after");
    assert_eq!(aggregated(&out[0]).text.skip_tts, Some(false));
    assert_eq!(out[1].direction, Direction::Upstream);
}
#[test]
fn interruption_drops_partial_and_end_flushes_in_order() {
    let processor = LlmTextProcessor::default();
    assert!(processor
        .process_frame(llm("stale partial"), Direction::Downstream)
        .unwrap()
        .is_empty());
    let interruption = Frame::interruption();
    let id = interruption.id;
    let output = processor
        .process_frame(interruption, Direction::Upstream)
        .unwrap();
    assert_eq!(output[0].frame.id, id);
    assert_eq!(output[0].direction, Direction::Upstream);
    assert!(processor
        .process_frame(llm("fresh text"), Direction::Downstream)
        .unwrap()
        .is_empty());
    let output = processor
        .process_frame(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
        .unwrap();
    assert_eq!(aggregated(&output[0]).text.text, "fresh text");
    assert!(matches!(output[1].frame.data, FrameData::LlmResponseEnd));
    assert!(
        processor
            .process_frame(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
            .unwrap()
            .len()
            == 1
    );
}
#[test]
fn sentence_processor_chunk_semantics_and_provisional_suppression() {
    let processor = SentenceAggregator::new();
    assert!(processor
        .process_frame(Frame::text("Hello,"), Direction::Downstream)
        .unwrap()
        .is_empty());
    let interim = Frame::new(FrameData::InterimTranscription(TranscriptionFrame {
        text: "ignored".into(),
        ..Default::default()
    }));
    assert!(processor
        .process_frame(interim, Direction::Downstream)
        .unwrap()
        .is_empty());
    let out = processor
        .process_frame(Frame::text(" world. Next fragment"), Direction::Upstream)
        .unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].direction, Direction::Downstream);
    assert!(
        matches!(&out[0].frame.data, FrameData::Text(t) if t.text == "Hello, world. Next fragment")
    );
    processor
        .process_frame(Frame::text("kept"), Direction::Downstream)
        .unwrap();
    processor
        .process_frame(Frame::interruption(), Direction::Downstream)
        .unwrap();
    let out = processor
        .process_frame(Frame::text(" after interruption."), Direction::Downstream)
        .unwrap();
    assert!(
        matches!(&out[0].frame.data, FrameData::Text(t) if t.text == "kept after interruption.")
    );
}
#[tokio::test]
async fn typed_pipeline_flushes_before_terminal_frame() {
    let mut worker = Pipeline::new(vec![Arc::new(LlmTextProcessor::default())])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Start(_)
    ));
    worker
        .sender()
        .send(llm("Hello. Next fragment"), Direction::Downstream)
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), worker.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(aggregated(&first).text.text, "Hello.");
    worker.end().await.unwrap();
    let last = tokio::time::timeout(Duration::from_secs(2), worker.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(aggregated(&last).text.text, "Next fragment");
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::End(_)
    ));
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}

#[test]
fn detailed_end_preserves_payload_and_flushes_like_legacy_end() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        let end = Frame::new(FrameData::EndDetailed(ReasonFrame {
            reason: Some(serde_json::json!({"status": "completed", "turn": 4})),
        }));
        let processor = LlmTextProcessor::default();
        processor
            .process_frame(llm("pending text"), direction)
            .unwrap();
        let output = processor.process_frame(end.clone(), direction).unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(aggregated(&output[0]).text.text, "pending text");
        assert_eq!(output[0].direction, Direction::Downstream);
        assert_eq!(output[1].frame, end);
        assert_eq!(output[1].direction, direction);
        assert_eq!(
            processor
                .process_frame(Frame::end(), direction)
                .unwrap()
                .len(),
            1
        );

        let sentence = SentenceAggregator::new();
        sentence
            .process_frame(Frame::text("pending text"), direction)
            .unwrap();
        let output = sentence.process_frame(end.clone(), direction).unwrap();
        assert_eq!(output.len(), 2);
        assert!(matches!(&output[0].frame.data, FrameData::Text(t) if t.text == "pending text"));
        assert_eq!(output[1].frame, end);
        assert!(output.iter().all(|f| f.direction == Direction::Downstream));
    }
}

#[test]
fn start_stop_and_cancel_preserve_pending_text_and_registered_patterns() {
    for data in [
        FrameData::Start(StartFrame::default()),
        FrameData::StartDetailed(DetailedStartFrame {
            configuration: StartFrame {
                audio_out_sample_rate: 48000,
                ..Default::default()
            },
            enable_tracing: true,
            tracing_context: Some(TracingContext {
                conversation_id: Some("text-conversation".into()),
                ..Default::default()
            }),
        }),
        FrameData::Stop,
        FrameData::Cancel(CancelFrame {
            reason: Some("cancelled".into()),
        }),
        FrameData::CancelDetailed(ReasonFrame {
            reason: Some(serde_json::json!({"status": "cancelled"})),
        }),
    ] {
        let mut patterns = PatternPairAggregator::default();
        patterns
            .add_pattern("code", "<code>", "</code>", MatchAction::Aggregate)
            .unwrap();
        let processor = LlmTextProcessor::new(patterns);
        assert!(processor
            .process_frame(llm("<code>pending"), Direction::Downstream)
            .unwrap()
            .is_empty());
        let mut control = Frame::new(data.clone());
        control.metadata.insert("session".into(), "text".into());
        let output = processor
            .process_frame(control.clone(), Direction::Upstream)
            .unwrap();
        assert_eq!(
            output,
            vec![DirectedFrame::new(control.clone(), Direction::Upstream)]
        );
        let output = processor
            .process_frame(llm(" text</code>"), Direction::Downstream)
            .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(aggregated(&output[0]).aggregated_by, "code");
        assert_eq!(aggregated(&output[0]).text.text, "pending text");
        assert_eq!(
            aggregated(&output[0]).raw_text.as_deref(),
            Some("<code>pending text</code>")
        );

        let sentence = SentenceAggregator::new();
        assert!(sentence
            .process_frame(Frame::text("pending"), Direction::Downstream)
            .unwrap()
            .is_empty());
        let output = sentence
            .process_frame(control.clone(), Direction::Upstream)
            .unwrap();
        assert_eq!(
            output,
            vec![DirectedFrame::new(control, Direction::Upstream)]
        );
        let output = sentence
            .process_frame(Frame::text(" sentence."), Direction::Downstream)
            .unwrap();
        assert_eq!(output.len(), 1);
        assert!(
            matches!(&output[0].frame.data, FrameData::Text(t) if t.text == "pending sentence.")
        );
    }
}

#[tokio::test]
async fn detailed_end_flushes_but_stop_and_cancel_do_not_emit_partial_text() {
    for data in [
        FrameData::EndDetailed(ReasonFrame {
            reason: Some("finished".into()),
        }),
        FrameData::Stop,
        FrameData::CancelDetailed(ReasonFrame {
            reason: Some("cancelled".into()),
        }),
    ] {
        let processor = Arc::new(LlmTextProcessor::default());
        let mut worker = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker.recv().await.unwrap();
        worker
            .sender()
            .send(llm("pending text"), Direction::Downstream)
            .await
            .unwrap();
        // The marker confirms the partial has reached the processor before a
        // high-priority cancel is admitted.
        let marker = Frame::new(FrameData::LlmResponseStart);
        worker
            .sender()
            .send(marker.clone(), Direction::Downstream)
            .await
            .unwrap();
        assert_eq!(worker.recv().await.unwrap().frame, marker);
        let terminal = Frame::new(data.clone());
        worker
            .sender()
            .send(terminal.clone(), Direction::Downstream)
            .await
            .unwrap();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty());
        assert_eq!(frames.last().unwrap().frame, terminal);
        let graceful = matches!(data, FrameData::EndDetailed(_));
        assert_eq!(frames.len(), if graceful { 2 } else { 1 });
        let after = processor
            .process_frame(Frame::end(), Direction::Downstream)
            .unwrap();
        if graceful {
            assert_eq!(aggregated(&frames[0]).text.text, "pending text");
            assert_eq!(report.state, WorkerState::Ended);
            assert_eq!(after.len(), 1);
        } else {
            assert_eq!(aggregated(&after[0]).text.text, "pending text");
            assert_eq!(
                report.state,
                if matches!(data, FrameData::Stop) {
                    WorkerState::Stopped
                } else {
                    WorkerState::Cancelled
                }
            );
        }
    }
}
