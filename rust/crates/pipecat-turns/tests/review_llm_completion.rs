//! Independent adversarial completion-filter and runtime boundary regressions.
use pipecat_core::*;
use pipecat_turns::*;
use std::{sync::Arc, time::Duration};

fn strategy() -> LLMTurnCompletionStrategy {
    LLMTurnCompletionStrategy::new(Default::default()).unwrap()
}
fn process(s: &mut LLMTurnCompletionStrategy, data: FrameData) -> AggregationOutput {
    s.process_frame(Frame::new(data), Direction::Downstream, Duration::ZERO)
        .unwrap()
}
fn text(s: &mut LLMTurnCompletionStrategy, value: &str) -> AggregationOutput {
    process(s, FrameData::LlmText(TextFrame::llm(value)))
}
fn words(out: &AggregationOutput) -> String {
    out.actions
        .iter()
        .filter_map(|a| match a {
            AggregationAction::Push(f) => match &f.frame.data {
                FrameData::LlmText(t) => Some(t.text.as_str()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}
fn custom_text(value: &str) -> Frame {
    let mut f = Frame::new(FrameData::LlmText(TextFrame {
        text: value.into(),
        skip_tts: Some(true),
        includes_inter_frame_spaces: false,
        append_to_context: false,
    }));
    f.pts = Some(123);
    f.transport_source = Some("source".into());
    f.transport_destination = Some("destination".into());
    f.broadcast_sibling_id = Some(987);
    f.interruptible = false;
    f.metadata
        .insert("custom".into(), serde_json::json!({"opaque": [1, 2]}));
    f
}
#[test]
fn unmarked_end_and_prefix_overflow_preserve_every_original_frame_flag() {
    for overflow in [false, true] {
        let mut s = LLMTurnCompletionStrategy::new(LLMTurnCompletionConfig {
            max_prefix_bytes: if overflow { 5 } else { 1024 },
            ..Default::default()
        })
        .unwrap();
        let original = [custom_text("plain"), custom_text(" words")];
        let mut actual = Vec::new();
        for f in &original {
            let out = s
                .process_frame(f.clone(), Direction::Downstream, Duration::ZERO)
                .unwrap();
            actual.extend(out.actions.into_iter().filter_map(|a| match a {
                AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmText(_)) => {
                    Some(f.frame)
                }
                _ => None,
            }));
        }
        actual.extend(
            process(&mut s, FrameData::LlmResponseEnd)
                .actions
                .into_iter()
                .filter_map(|a| match a {
                    AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmText(_)) => {
                        Some(f.frame)
                    }
                    _ => None,
                }),
        );
        assert_eq!(actual, original);
        assert_eq!(s.retained_text_bytes(), 0);
    }
}
#[test]
fn unicode_marker_split_preserves_envelopes_and_no_marker_bytes_escape() {
    let config = LLMTurnCompletionConfig {
        complete_marker: "完✅成".into(),
        incomplete_short_marker: "等🟡待".into(),
        incomplete_long_marker: "稍🔴后".into(),
        ..Default::default()
    };
    for split in [3, 6] {
        let mut s = LLMTurnCompletionStrategy::new(config.clone()).unwrap();
        assert!(text(&mut s, &config.complete_marker[..split])
            .actions
            .is_empty());
        let mut expected = custom_text("hello");
        let mut original = expected.clone();
        let FrameData::LlmText(t) = &mut original.data else {
            unreachable!()
        };
        t.text = format!("{} hello", &config.complete_marker[split..]);
        expected.id = original.id;
        let out = s
            .process_frame(original, Direction::Downstream, Duration::ZERO)
            .unwrap();
        let actual = out
            .actions
            .into_iter()
            .find_map(|a| match a {
                AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmText(_)) => {
                    Some(f.frame)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(actual, expected);
    }
}
#[test]
fn every_native_carrier_and_missing_checkpoint_fail_before_buffer_flush() {
    let signed = vec![LlmContinuationBlock::Thinking {
        text: "private reasoning".into(),
        signature: "opaque+/==".into(),
    }];
    let call = FunctionCall {
        function_name: "fixture".into(),
        tool_call_id: "review-call".into(),
        arguments: serde_json::json!({}),
    };
    let carriers = [
        (
            FrameData::LlmResponseStart,
            Some(NativeLlmContinuation::start(801, LlmContinuationProvider::Anthropic).unwrap()),
        ),
        (
            FrameData::LlmResponseStart,
            Some(NativeLlmContinuation::start(802, LlmContinuationProvider::Gemini).unwrap()),
        ),
        (
            FrameData::LlmResponseEnd,
            Some(
                NativeLlmContinuation::new(803, LlmContinuationProvider::Anthropic, signed.clone())
                    .unwrap(),
            ),
        ),
        (
            FrameData::LlmContinuationCheckpoint,
            Some(
                NativeLlmContinuation::checkpoint(804, LlmContinuationProvider::Anthropic, signed)
                    .unwrap(),
            ),
        ),
        (
            FrameData::FunctionCallsStarted(vec![call.clone()]),
            Some(
                NativeLlmContinuation::new(
                    805,
                    LlmContinuationProvider::Anthropic,
                    vec![LlmContinuationBlock::ToolCall(call)],
                )
                .unwrap(),
            ),
        ),
        (FrameData::LlmContinuationCheckpoint, None),
    ];
    for (data, carrier) in carriers {
        for direction in [Direction::Downstream, Direction::Upstream] {
            let mut s = strategy();
            text(&mut s, "unmarked pending text");
            let mut f = Frame::new(data.clone());
            if let Some(c) = carrier.clone() {
                f.set_native_llm_continuation(Arc::new(c)).unwrap();
            }
            let error = s.process_frame(f, direction, Duration::ZERO).unwrap_err();
            assert!(error.fatal);
            assert!(s.is_closed());
            assert_eq!(s.retained_text_bytes(), 0);
            assert!(text(&mut s, "● late").actions.is_empty());
        }
    }
}
#[test]
fn stalled_incomplete_response_closes_and_cannot_reopen_on_new_start() {
    let mut s = strategy();
    text(&mut s, "◐");
    let error = s.advance(Duration::from_secs(5)).unwrap_err();
    assert!(error.fatal);
    assert!(s.is_closed());
    assert!(process(&mut s, FrameData::LlmResponseStart)
        .actions
        .is_empty());
    assert!(text(&mut s, "● must not reopen").actions.is_empty());
}
#[test]
fn late_text_after_response_end_or_interruption_never_pollutes_new_response_report() {
    for boundary in [FrameData::LlmResponseEnd, FrameData::Interruption] {
        let mut s = strategy();
        text(&mut s, "◐");
        process(&mut s, boundary);
        assert!(text(&mut s, "old provider tail").actions.is_empty());
        process(&mut s, FrameData::LlmResponseStart);
        assert_eq!(words(&text(&mut s, "● current")), "current");
        let report = process(&mut s, FrameData::LlmResponseEnd);
        assert!(report.actions.iter().any(|a| matches!(a, AggregationAction::Push(f) if matches!(&f.frame.data, FrameData::LlmMarkerResponse(r) if r.raw == "● current"))));
    }
}
#[test]
fn replayed_or_upstream_response_start_keeps_deterministic_deadline() {
    for upstream in [false, true] {
        let mut s = strategy();
        let start = Frame::new(FrameData::LlmResponseStart);
        s.process_frame(start.clone(), Direction::Downstream, Duration::ZERO)
            .unwrap();
        text(&mut s, "◐");
        process(&mut s, FrameData::LlmResponseEnd);
        s.process_frame(
            start,
            if upstream {
                Direction::Upstream
            } else {
                Direction::Downstream
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(s.deadline(), Some(Duration::from_secs(5)));
    }
}

async fn send(worker: &PipelineWorker, frame: Frame, direction: Direction) {
    worker.sender().send(frame, direction).await.unwrap();
}
async fn receive(
    worker: &mut PipelineWorker,
    predicate: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let output = worker.recv().await.expect("pipeline output");
            if predicate(&output.frame.data) {
                return output;
            }
            assert!(
                !matches!(output.frame.data, FrameData::Error(_)),
                "unexpected error: {:?}",
                output.frame.data
            );
        }
    })
    .await
    .expect("expected pipeline output before deadline")
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        Frame::new(FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("completion-review".into()),
        })),
        Direction::Downstream,
    )
    .await;
    receive(worker, |d| matches!(d, FrameData::Flush(_))).await;
}
async fn deadline_worker() -> (PipelineWorker, Frame) {
    let mut worker = Pipeline::new(vec![Arc::new(
        LLMTurnCompletionProcessor::new(Default::default()).unwrap(),
    )])
    .start(PipelineConfig {
        max_background_tasks: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    receive(&mut worker, FrameData::is_start).await;
    let start = Frame::new(FrameData::LlmResponseStart);
    send(&worker, start.clone(), Direction::Downstream).await;
    send(
        &worker,
        Frame::new(FrameData::LlmText(TextFrame::llm("◐"))),
        Direction::Downstream,
    )
    .await;
    send(
        &worker,
        Frame::new(FrameData::LlmResponseEnd),
        Direction::Downstream,
    )
    .await;
    barrier(&mut worker).await;
    (worker, start)
}
async fn finish(mut worker: PipelineWorker) {
    worker.end().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), worker.join())
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerState::Ended
    );
}
#[tokio::test(start_paused = true)]
async fn replayed_response_start_cannot_disarm_runtime_incomplete_deadline() {
    let (mut worker, start) = deadline_worker().await;
    send(&worker, start, Direction::Downstream).await;
    barrier(&mut worker).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    receive(&mut worker, |d| {
        matches!(d, FrameData::LlmMessagesTransform(_))
    })
    .await;
    finish(worker).await;
}
#[tokio::test(start_paused = true)]
async fn upstream_response_start_cannot_disarm_runtime_incomplete_deadline() {
    let (mut worker, _) = deadline_worker().await;
    send(
        &worker,
        Frame::new(FrameData::LlmResponseStart),
        Direction::Upstream,
    )
    .await;
    barrier(&mut worker).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    receive(&mut worker, |d| {
        matches!(d, FrameData::LlmMessagesTransform(_))
    })
    .await;
    finish(worker).await;
}

#[test]
fn oversized_metadata_and_many_tiny_chunks_degrade_without_retaining_unbounded_input() {
    let mut s = LLMTurnCompletionStrategy::new(LLMTurnCompletionConfig {
        max_raw_bytes: 9,
        ..Default::default()
    })
    .unwrap();
    let mut huge = custom_text("unmarked");
    huge.metadata
        .insert("oversized".into(), "x".repeat(70 * 1024).into());
    let out = s
        .process_frame(huge.clone(), Direction::Downstream, Duration::ZERO)
        .unwrap();
    assert!(out
        .actions
        .iter()
        .any(|a| matches!(a, AggregationAction::Push(f) if f.frame == huge)));
    assert!(s.retained_text_bytes() <= 11);
    for _ in 0..3000 {
        assert_eq!(words(&text(&mut s, "x")), "x");
    }
    assert!(s.retained_text_bytes() <= 11);
    let end = process(&mut s, FrameData::LlmResponseEnd);
    assert!(end.actions.iter().any(|a| matches!(a, AggregationAction::Push(f) if f.frame.metadata.get("completion_prefix_exceeded") == Some(&serde_json::json!(true)))));
    assert_eq!(s.retained_text_bytes(), 0);

    let mut s = LLMTurnCompletionStrategy::new(LLMTurnCompletionConfig {
        max_raw_bytes: 9,
        ..Default::default()
    })
    .unwrap();
    let mut delivered = String::new();
    for _ in 0..3000 {
        delivered.push_str(&words(&text(&mut s, "x")));
        assert!(s.retained_text_bytes() <= 2 * 256 + 11);
    }
    delivered.push_str(&words(&process(&mut s, FrameData::LlmResponseEnd)));
    assert_eq!(delivered, "x".repeat(3000));
    assert_eq!(s.retained_text_bytes(), 0);
}

#[test]
fn mutated_continuation_cannot_hide_behind_replayed_frame_identity() {
    let mut s = strategy();
    let start = Frame::new(FrameData::LlmResponseStart);
    s.process_frame(start.clone(), Direction::Downstream, Duration::ZERO)
        .unwrap();
    let mut replay = start;
    replay
        .set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::start(806, LlmContinuationProvider::Anthropic).unwrap(),
        ))
        .unwrap();
    replay.data = FrameData::LlmText(TextFrame::llm("● rewritten signed start"));
    let error = s
        .process_frame(replay, Direction::Downstream, Duration::ZERO)
        .unwrap_err();
    assert!(error.fatal);
    assert!(s.is_closed());
}

#[test]
fn closed_strategy_rejects_mutated_terminal_continuation_before_forwarding() {
    for terminal in [
        FrameData::End(EndFrame { reason: None }),
        FrameData::Stop,
        FrameData::Cancel(CancelFrame { reason: None }),
    ] {
        let mut s = strategy();
        s.stop();
        let mut frame = Frame::new(FrameData::LlmResponseStart);
        frame
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(807, LlmContinuationProvider::Anthropic).unwrap(),
            ))
            .unwrap();
        frame.data = terminal;
        assert!(
            s.process_frame(frame, Direction::Downstream, Duration::ZERO)
                .unwrap_err()
                .fatal
        );
    }
}

fn stopped_text(output: &AggregationOutput) -> &str {
    output
        .events
        .iter()
        .find_map(|event| match event {
            AggregationEvent::AssistantTurnStopped { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .expect("assistant stopped event")
}
fn assistant_frame(assistant: &LLMAssistantAggregator, data: FrameData) -> AggregationOutput {
    assistant
        .process_frame(Frame::new(data), Direction::Downstream)
        .unwrap()
}
#[test]
fn spoken_transcript_projects_only_confirmed_delivery_and_preserves_context_markers() {
    let shared = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    assistant_frame(
        &assistant,
        FrameData::LlmMarker(LlmMarkerFrame {
            marker: "<DONE>".into(),
            append_to_context_immediately: false,
        }),
    );
    assistant_frame(
        &assistant,
        FrameData::LlmText(TextFrame::llm("generated but not heard")),
    );
    assistant.begin_playback("confirmed").unwrap();
    assert!(assistant_frame(&assistant, FrameData::LlmResponseEnd)
        .events
        .is_empty());
    let out = assistant
        .confirm_playback(
            "confirmed",
            &[AggregatedTextFrame {
                text: TextFrame::llm("heard <DONE> tail"),
                raw_text: None,
                context_id: Some("confirmed".into()),
                aggregated_by: "review".into(),
                will_be_spoken: true,
            }],
        )
        .unwrap();
    assert_eq!(stopped_text(&out), "heard  tail");
    let history = shared.snapshot().unwrap();
    assert_eq!(history.messages.len(), 1);
    assert_eq!(
        history.messages[0].content,
        Some(pipecat_services::Content::Text(
            "<DONE> heard <DONE> tail".into()
        ))
    );
    assert!(!format!("{history:?}").contains("generated but not heard"));
}
#[test]
fn rejected_transcript_marker_changes_neither_pending_text_nor_projection_set() {
    let shared = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(shared.clone(), Default::default());
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    let rejected = "z".repeat(257);
    let before = shared.snapshot().unwrap();
    assert!(assistant
        .process_frame(
            Frame::new(FrameData::LlmMarker(LlmMarkerFrame {
                marker: rejected.clone(),
                append_to_context_immediately: false,
            })),
            Direction::Downstream
        )
        .is_err());
    assert_eq!(shared.snapshot().unwrap(), before);
    assistant_frame(
        &assistant,
        FrameData::LlmText(TextFrame::llm(format!("literal {rejected}"))),
    );
    let out = assistant_frame(&assistant, FrameData::LlmResponseEnd);
    assert_eq!(stopped_text(&out), format!("literal {rejected}"));
    assert_eq!(shared.snapshot().unwrap().messages.len(), 1);
}
