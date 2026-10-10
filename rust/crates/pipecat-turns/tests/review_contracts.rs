//! Independent source-contract regressions for asynchronous tool continuations.
use pipecat_core::*;
use pipecat_turns::*;
use serde_json::json;

fn assistant() -> LLMAssistantAggregator {
    LLMAssistantAggregator::new(SharedContext::default(), Default::default())
}
fn process(assistant: &LLMAssistantAggregator, data: FrameData) -> AggregationOutput {
    assistant
        .process_frame(Frame::new(data), Direction::Downstream)
        .unwrap()
}
fn begin(assistant: &LLMAssistantAggregator, id: &str, group: Option<&str>) {
    process(
        assistant,
        FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
            function_name: "lookup".into(),
            tool_call_id: id.into(),
            arguments: json!({}),
            cancel_on_interruption: false,
            group_id: group.map(str::to_owned),
        }),
    );
}
fn result(id: &str, is_final: bool, run_llm: Option<bool>) -> FrameData {
    FrameData::FunctionCallResult(FunctionCallResultFrame {
        function_name: "lookup".into(),
        tool_call_id: id.into(),
        arguments: json!({}),
        result: json!({"progress":42}),
        run_llm,
        is_final,
        error: None,
    })
}
fn upstream(out: &AggregationOutput) -> usize {
    out.actions
        .iter()
        .filter(|action| {
            matches!(action, AggregationAction::Push(frame)
        if frame.direction == Direction::Upstream && matches!(frame.frame.data,
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)))
        })
        .count()
}

#[test]
fn nonempty_intermediate_result_defaults_to_notifying_the_llm() {
    // Source _handle_function_call_result: nonempty ungrouped results default
    // run_llm to true, independently of properties.is_final.
    let assistant = assistant();
    begin(&assistant, "one", None);
    let out = process(&assistant, result("one", false, None));
    assert_eq!(
        upstream(&out),
        1,
        "a useful partial result must reach the LLM before final completion"
    );
    assert!(assistant.has_function_calls_in_progress().unwrap());
}

#[test]
fn completed_tool_group_is_not_held_by_a_different_background_group() {
    // Source group_id checks only sibling calls from the same LLM response.
    let assistant = assistant();
    begin(&assistant, "older", Some("response-1"));
    begin(&assistant, "newer", Some("response-2"));
    let out = process(&assistant, result("newer", true, None));
    assert_eq!(
        upstream(&out),
        1,
        "an unrelated long-running group must not hide the completed group's result"
    );
    assert!(assistant.has_function_calls_in_progress().unwrap());
}

#[test]
fn explicit_partial_result_run_is_not_blocked_by_its_own_group_membership() {
    // The source property override allows a tool to request inference on updates.
    let assistant = assistant();
    begin(&assistant, "one", Some("response-1"));
    let out = process(&assistant, result("one", false, Some(true)));
    assert_eq!(
        upstream(&out),
        1,
        "run_llm=true must deliver a partial result while its tool is still running"
    );
}

#[test]
fn vision_response_inherits_assistant_context_aggregation() {
    // Python VisionFullResponse{Start,End}Frame subclass LLMFullResponse*, and
    // VisionTextFrame subclasses LLMTextFrame, so the same handlers consume them.
    let assistant = assistant();
    process(
        &assistant,
        FrameData::VisionFullResponseStart(Default::default()),
    );
    process(
        &assistant,
        FrameData::VisionText(TextFrame::llm("a blue bicycle")),
    );
    process(
        &assistant,
        FrameData::VisionFullResponseEnd(Default::default()),
    );
    let context = assistant.context().snapshot().unwrap();
    assert_eq!(
        context.messages.len(),
        1,
        "vision response text must be included in assistant history"
    );
}

#[test]
fn cancelled_group_runs_when_its_sibling_finishes_even_if_another_group_remains() {
    let assistant = assistant();
    begin(&assistant, "first", Some("response-1"));
    begin(&assistant, "sibling", Some("response-1"));
    begin(&assistant, "unrelated", Some("response-2"));
    let cancelled = process(
        &assistant,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "lookup".into(),
            tool_call_id: "first".into(),
            run_llm: true,
        }),
    );
    assert_eq!(upstream(&cancelled), 0);
    let settled = process(&assistant, result("sibling", true, Some(false)));
    assert_eq!(upstream(&settled), 1);
    assert!(assistant.has_function_calls_in_progress().unwrap());
}

#[test]
fn partial_result_override_waits_for_response_end_and_replay_cannot_run_it_twice() {
    let assistant = assistant();
    process(&assistant, FrameData::LlmResponseStart);
    begin(&assistant, "one", Some("response-1"));
    let partial = Frame::new(result("one", false, Some(true)));
    let update = assistant
        .process_frame(partial.clone(), Direction::Downstream)
        .unwrap();
    assert_eq!(upstream(&update), 0);
    assert_eq!(upstream(&process(&assistant, FrameData::LlmResponseEnd)), 1);
    assert_eq!(
        upstream(
            &assistant
                .process_frame(partial, Direction::Downstream)
                .unwrap()
        ),
        0
    );
    assert_eq!(
        upstream(&process(&assistant, FrameData::BotStoppedSpeaking)),
        0
    );
}

#[test]
fn single_member_group_intermediate_default_excludes_the_current_call() {
    let assistant = assistant();
    begin(&assistant, "one", Some("response-1"));
    assert_eq!(
        upstream(&process(&assistant, result("one", false, None))),
        1
    );
    assert!(assistant.has_function_calls_in_progress().unwrap());
}
