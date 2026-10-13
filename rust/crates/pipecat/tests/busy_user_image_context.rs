//! Frozen public baseline: a false-run encoded upload commits held text without
//! closing the assistant turn, both directly and in the real RTVI pipeline.
use pipecat::{
    core::*,
    rtvi::{RtviConfig, RtviProcessor},
    turns::{
        AggregationAction, AggregationEvent, AggregationOutput, AssistantStoppedReceiver,
        AssistantTurnStopped, LLMAssistantAggregator, SharedContext,
    },
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

const WAIT: Duration = Duration::from_secs(5);
const PNG: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0,
    0, 0, 144, 119, 83, 222, 0, 0, 0, 12, 73, 68, 65, 84, 120, 156, 99, 248, 207, 192, 0, 0, 3, 1,
    1, 0, 201, 254, 146, 239, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];
const PNG_BASE64: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";

fn upload(text: &str) -> Frame {
    Frame::new(FrameData::UserImage(UserImageRawFrame {
        image: ImageRawFrame {
            image: PNG.to_vec(),
            size: (0, 0),
            format: Some("image/png".into()),
        },
        user_id: String::new(),
        text: Some(text.into()),
        append_to_context: Some(true),
        request: None,
        run_llm: Some(false),
    }))
}
fn message(role: ChatRole, content: MessageContent) -> ContextMessage {
    ContextMessage {
        role,
        content,
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: Default::default(),
    }
}
fn assistant_message(text: &str) -> ContextMessage {
    message(ChatRole::Assistant, MessageContent::Text(text.into()))
}
fn image_message(text: &str) -> ContextMessage {
    let mut parts = vec![];
    if !text.is_empty() {
        parts.push(MessagePart::Text { text: text.into() });
    }
    parts.push(MessagePart::Image {
        url: format!("data:image/png;base64,{PNG_BASE64}"),
        detail: None,
    });
    message(ChatRole::User, MessageContent::Parts(parts))
}
fn consume(
    assistant: &LLMAssistantAggregator,
    frame: Frame,
) -> Result<AggregationOutput, ProcessorError> {
    assistant.process_at(frame, Direction::Downstream, Duration::ZERO)
}
fn text(text: &str) -> Frame {
    Frame::new(FrameData::LlmText(TextFrame::llm(text)))
}
fn silent(output: &AggregationOutput) {
    assert!(
        output.actions.is_empty(),
        "unexpected actions: {:?}",
        output.actions
    );
    assert!(
        output.events.is_empty(),
        "unexpected events: {:?}",
        output.events
    );
}
fn admit(output: &Result<AggregationOutput, ProcessorError>) -> Result<(), ProcessorError> {
    if let Ok(output) = output {
        for action in &output.actions {
            if let AggregationAction::AssistantStopped(action) = action {
                action.admit()?;
            }
        }
    }
    Ok(())
}
fn records(receiver: &mut AssistantStoppedReceiver) -> Vec<Arc<AssistantTurnStopped>> {
    let mut records = vec![];
    while let Some(record) = receiver.try_recv() {
        records.push(record.completion().clone());
    }
    records
}
fn end_effects(output: &AggregationOutput, text: &str, record: &AssistantTurnStopped) {
    assert_eq!(
        output.events,
        vec![AggregationEvent::AssistantTurnStopped {
            text: text.into(),
            interrupted: false,
        }]
    );
    assert_eq!(record.text(), text);
    assert_eq!(record.turn_id(), 1);
    assert!(!record.interrupted());
    let frames: Vec<_> = output
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(frame) => Some(frame),
            _ => None,
        })
        .collect();
    assert_eq!(
        output
            .actions
            .iter()
            .filter(|action| matches!(action, AggregationAction::AssistantStopped(_)))
            .count(),
        1
    );
    assert_eq!(frames.len(), if text.is_empty() { 0 } else { 4 });
    if !text.is_empty() {
        assert!(matches!(
            frames[0].frame.data,
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
        ));
        assert_eq!(frames[0].direction, Direction::Downstream);
        assert!(matches!(
            frames[1].frame.data,
            FrameData::LlmContextAssistantTimestamp { .. }
        ));
        assert_eq!(frames[1].direction, Direction::Downstream);
        for (frame, direction) in frames[2..]
            .iter()
            .zip([Direction::Downstream, Direction::Upstream])
        {
            assert_eq!(frame.direction, direction);
            let FrameData::LlmContextAssistantTurn(turn) = &frame.frame.data else {
                panic!("missing final assistant turn")
            };
            assert_eq!(turn.text, text);
            assert_eq!(turn.timestamp, record.started_at());
        }
    }
}

#[tokio::test]
async fn busy_baseline_direct_flushes_prefix_and_stops_once_with_suffix() {
    // The ordinary text-only case is a positive control against fixture failures.
    for with_image in [false, true] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let prefix = consume(&assistant, text("A"));
        let accepted = with_image.then(|| consume(&assistant, upload("inspect")));
        let during = context.snapshot_rich();
        let premature = records(&mut receiver);
        let during_stats = receiver.stats();
        let suffix = consume(&assistant, text("B"));
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let admitted = admit(&end);
        let duplicate_end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let again = admit(&duplicate_end);
        let completed = records(&mut receiver);
        let snapshot = context.snapshot_rich();
        let terminal = consume(&assistant, Frame::end());
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        // No behavior assertion (in particular no expected image success) precedes cleanup.
        cleaned.unwrap();
        terminal.unwrap();
        assert_eq!(
            start.unwrap().events,
            vec![AggregationEvent::AssistantTurnStarted]
        );
        silent(&prefix.unwrap());
        if let Some(accepted) = accepted {
            silent(&accepted.expect("ordinary active false-run image must be admitted"));
        }
        assert!(premature.is_empty());
        assert_eq!(during_stats.reserved, 0);
        assert_eq!(
            during.unwrap().messages,
            if with_image {
                vec![assistant_message("A"), image_message("inspect")]
            } else {
                vec![]
            }
        );
        silent(&suffix.unwrap());
        admitted.unwrap();
        again.unwrap();
        silent(&duplicate_end.unwrap());
        assert_eq!(completed.len(), 1);
        let expected = if with_image { "B" } else { "AB" };
        end_effects(&end.unwrap(), expected, &completed[0]);
        assert_eq!(
            snapshot.unwrap().messages,
            if with_image {
                vec![
                    assistant_message("A"),
                    image_message("inspect"),
                    assistant_message("B"),
                ]
            } else {
                vec![assistant_message("AB")]
            }
        );
    }
}

async fn send(worker: &PipelineWorker, frame: Frame) -> Result<(), String> {
    tokio::time::timeout(WAIT, worker.sender().send(frame, Direction::Downstream))
        .await
        .map_err(|_| "send deadline".to_string())?
        .map_err(|e| e.to_string())
}
async fn drain_to(
    worker: &mut PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
    matches: impl Fn(&Frame) -> bool,
) -> Result<(), String> {
    tokio::time::timeout(WAIT, async {
        while let Some(directed) = worker.recv().await {
            let done = matches(&directed.frame);
            frames.push(directed);
            if done {
                return Ok(());
            }
        }
        Err("pipeline closed before FIFO marker".to_string())
    })
    .await
    .map_err(|_| "FIFO marker deadline".to_string())?
}
async fn fence(worker: &mut PipelineWorker, frames: &mut Vec<DirectedFrame>) -> Result<(), String> {
    let frame = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("busy-image-fixture".into()),
        returning: false,
    }));
    let id = frame.id;
    send(worker, frame).await?;
    drain_to(worker, frames, |frame| frame.id == id).await
}
struct PipelineOutcome {
    frames: Vec<DirectedFrame>,
    report: WorkerReport,
    during: LlmContextData,
    during_frames: usize,
    premature: Vec<Arc<AssistantTurnStopped>>,
    reserved_during: u64,
    completed: Vec<Arc<AssistantTurnStopped>>,
}
async fn rtvi_pipeline(context: SharedContext, busy: bool) -> Result<PipelineOutcome, String> {
    let rtvi = RtviProcessor::new(RtviConfig::default()).map_err(|e| e.to_string())?;
    let assistant = Arc::new(LLMAssistantAggregator::new(
        context.clone(),
        Default::default(),
    ));
    let mut receiver = assistant
        .subscribe_stopped(Default::default())
        .map_err(|e| e.to_string())?;
    let mut worker = Pipeline::new(vec![Arc::new(rtvi), assistant])
        .start(PipelineConfig {
            startup_timeout: WAIT,
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    let mut frames = vec![];
    let operation = async {
        if busy {
            send(&worker, Frame::new(FrameData::LlmResponseStart)).await?;
            send(&worker, text("A")).await?;
        }
        fence(&mut worker, &mut frames).await?;
        for value in [
            json!({"label": "rtvi-ai", "id": "busy-upload", "type": "send-file", "data": {
                "content": "inspect", "file": {"format": "image/png", "source": {"type": "bytes", "bytes": PNG_BASE64}},
                "options": {"run_immediately": false, "audio_response": true}
            }}),
            json!({"label": "rtvi-ai", "id": "after-busy-upload", "type": "client-message", "data": {"t": "test-fence", "d": null}}),
        ] {
            send(&worker, Frame::new(FrameData::InputTransportMessage(value))).await?;
        }
        // This command-loop FIFO marker follows the actual generated UserImage.
        // It is reached on the old rejection path too; no missing-success timeout.
        drain_to(&mut worker, &mut frames, |frame| matches!(&frame.data,
            FrameData::RtviClientMessage(message) if message.msg_id == "after-busy-upload")).await?;
        let during = context.snapshot_rich().map_err(|e| e.to_string())?;
        let during_frames = frames.len();
        let premature = records(&mut receiver);
        let reserved_during = receiver.stats().reserved;
        if busy {
            send(&worker, text("B")).await?;
            send(&worker, Frame::new(FrameData::LlmResponseEnd)).await?;
            send(&worker, Frame::new(FrameData::LlmResponseEnd)).await?;
        }
        fence(&mut worker, &mut frames).await?;
        Ok::<_, String>((during, during_frames, premature, reserved_during))
    }.await;
    // End, drain and join even when the old implementation rejects the image.
    let end = tokio::time::timeout(WAIT, worker.end()).await;
    if !matches!(&end, Ok(Ok(()))) {
        let _ = tokio::time::timeout(WAIT, worker.cancel("busy upload End failed")).await;
    }
    let drain = tokio::time::timeout(WAIT, async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
    })
    .await;
    if drain.is_err() {
        let _ = tokio::time::timeout(WAIT, worker.cancel("busy upload drain failed")).await;
    }
    let join = tokio::time::timeout(WAIT, worker.join()).await;
    if !matches!(&join, Ok(Ok(_))) {
        let _ = tokio::time::timeout(WAIT, worker.cancel("busy upload join failed")).await;
        let _ = tokio::time::timeout(WAIT, worker.join()).await;
    }
    let report = join
        .map_err(|_| "join deadline".to_string())?
        .map_err(|e| e.to_string())?;
    end.map_err(|_| "End deadline".to_string())?
        .map_err(|e| e.to_string())?;
    drain.map_err(|_| "drain deadline".to_string())?;
    let (during, during_frames, premature, reserved_during) = operation?;
    Ok(PipelineOutcome {
        frames,
        report,
        during,
        during_frames,
        premature,
        reserved_during,
        completed: records(&mut receiver),
    })
}
fn turn_effect(frame: &Frame) -> bool {
    matches!(
        frame.data,
        FrameData::LlmContext(_)
            | FrameData::LlmContextDetailed(_)
            | FrameData::LlmContextAssistantTimestamp { .. }
            | FrameData::LlmContextAssistantTurn(_)
    )
}

#[tokio::test]
async fn busy_baseline_rtvi_false_run_upload_flushes_without_premature_turn_effects() {
    // Idle upload is the existing positive control in exactly the same real topology.
    for busy in [false, true] {
        let context = SharedContext::default();
        let outcome = rtvi_pipeline(context.clone(), busy)
            .await
            .expect("RTVI fixture after cleanup");
        assert_eq!(outcome.report.state, WorkerState::Ended);
        assert!(
            outcome.report.errors.is_empty(),
            "actual pipeline rejection: {:?}",
            outcome.report
        );
        assert!(outcome.premature.is_empty());
        assert_eq!(outcome.reserved_during, 0);
        assert!(!outcome.frames[..outcome.during_frames]
            .iter()
            .any(|f| turn_effect(&f.frame)));
        assert!(!outcome.frames.iter().any(|f| matches!(
            f.frame.data,
            FrameData::Interruption | FrameData::UserImage(_) | FrameData::Error(_)
        )));
        assert_eq!(
            outcome.during.messages,
            if busy {
                vec![assistant_message("A"), image_message("inspect")]
            } else {
                vec![image_message("inspect")]
            }
        );
        assert_eq!(
            context.snapshot_rich().unwrap().messages,
            if busy {
                vec![
                    assistant_message("A"),
                    image_message("inspect"),
                    assistant_message("B"),
                ]
            } else {
                vec![image_message("inspect")]
            }
        );
        assert_eq!(outcome.completed.len(), usize::from(busy));
        let effects: Vec<_> = outcome
            .frames
            .iter()
            .filter(|f| turn_effect(&f.frame))
            .collect();
        assert_eq!(effects.len(), if busy { 4 } else { 0 });
        if busy {
            let record = &outcome.completed[0];
            assert_eq!(record.text(), "B");
            assert_eq!(record.turn_id(), 1);
            assert!(!record.interrupted());
            let contexts: Vec<_> = effects
                .iter()
                .filter(|f| {
                    matches!(
                        f.frame.data,
                        FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                    )
                })
                .collect();
            assert_eq!(contexts.len(), 1);
            assert_eq!(contexts[0].direction, Direction::Downstream);
            for direction in [Direction::Downstream, Direction::Upstream] {
                let turns: Vec<_> = effects
                    .iter()
                    .filter_map(|f| match &f.frame.data {
                        FrameData::LlmContextAssistantTurn(turn) if f.direction == direction => {
                            Some(turn)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(turns.len(), 1);
                assert_eq!(turns[0].text, "B");
                assert_eq!(turns[0].timestamp, record.started_at());
            }
        }
    }
}

// Candidate-only controls. The entire two-test baseline above is byte-frozen.
use pipecat::turns::{AssistantAggregatorConfig, AssistantTextMode};

async fn finish_direct(
    assistant: LLMAssistantAggregator,
    receiver: &mut AssistantStoppedReceiver,
) -> (
    AggregationOutput,
    Vec<Arc<AssistantTurnStopped>>,
    LlmContextData,
) {
    let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let admitted = admit(&end);
    let duplicate = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let admitted_again = admit(&duplicate);
    let completed = records(receiver);
    let snapshot = assistant.context().snapshot_rich();
    let terminal = consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    terminal.unwrap();
    admitted.unwrap();
    admitted_again.unwrap();
    silent(&duplicate.unwrap());
    (end.unwrap(), completed, snapshot.unwrap())
}

#[tokio::test]
async fn busy_empty_and_whitespace_prefixes_keep_source_presence_semantics() {
    for prefix in ["", " \n\t ", " A "] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let held = consume(&assistant, text(prefix));
        let image = consume(&assistant, upload("inspect"));
        let during = context.snapshot_rich();
        let premature = records(&mut receiver);
        let (end, completed, final_context) = finish_direct(assistant, &mut receiver).await;
        start.unwrap();
        silent(&held.unwrap());
        silent(&image.unwrap());
        assert!(premature.is_empty());
        let mut expected = vec![];
        if !prefix.is_empty() {
            expected.push(assistant_message(prefix.trim()));
        }
        expected.push(image_message("inspect"));
        assert_eq!(during.unwrap().messages, expected);
        assert_eq!(final_context.messages, expected);
        assert_eq!(completed.len(), 1);
        end_effects(&end, "", &completed[0]);
    }
}

#[tokio::test]
async fn busy_multiple_distinct_uploads_commit_each_segment_in_order() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let mut outputs = vec![consume(&assistant, Frame::new(FrameData::LlmResponseStart))];
    let mut snapshots = vec![];
    for (prefix, caption) in [(" A ", "first"), (" B ", "second")] {
        outputs.push(consume(&assistant, text(prefix)));
        outputs.push(consume(&assistant, upload(caption)));
        snapshots.push(context.snapshot_rich());
    }
    outputs.push(consume(&assistant, text(" C ")));
    let premature = records(&mut receiver);
    let (end, completed, snapshot) = finish_direct(assistant, &mut receiver).await;
    outputs.remove(0).unwrap();
    for output in outputs {
        silent(&output.unwrap());
    }
    assert!(premature.is_empty());
    assert_eq!(
        snapshots.remove(0).unwrap().messages,
        vec![assistant_message("A"), image_message("first")]
    );
    assert_eq!(
        snapshots.remove(0).unwrap().messages,
        vec![
            assistant_message("A"),
            image_message("first"),
            assistant_message("B"),
            image_message("second")
        ]
    );
    assert_eq!(
        snapshot.messages,
        vec![
            assistant_message("A"),
            image_message("first"),
            assistant_message("B"),
            image_message("second"),
            assistant_message("C")
        ]
    );
    assert_eq!(completed.len(), 1);
    end_effects(&end, "C", &completed[0]);
}

#[tokio::test]
async fn busy_same_id_replay_cannot_flush_later_suffix_or_skip_validation() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let a = consume(&assistant, text("A"));
    let frame = upload("inspect");
    let first = consume(&assistant, frame.clone());
    let b = consume(&assistant, text("B"));
    let before = context.snapshot_rich();
    let replay = consume(&assistant, frame.clone());
    let mut malformed = frame;
    if let FrameData::UserImage(image) = &mut malformed.data {
        image.image.image.clear();
    }
    let invalid = consume(&assistant, malformed);
    let after = context.snapshot_rich();
    let c = consume(&assistant, text("C"));
    let (end, completed, snapshot) = finish_direct(assistant, &mut receiver).await;
    start.unwrap();
    for result in [a, first, b, replay, c] {
        silent(&result.unwrap());
    }
    assert!(invalid.is_err());
    assert_eq!(before.unwrap(), after.unwrap());
    assert_eq!(
        snapshot.messages,
        vec![
            assistant_message("A"),
            image_message("inspect"),
            assistant_message("BC")
        ]
    );
    assert_eq!(completed.len(), 1);
    end_effects(&end, "BC", &completed[0]);
}

#[tokio::test]
async fn busy_invalid_image_preserves_prefix_and_allows_repaired_same_id() {
    for invalid_kind in ["empty", "mismatch", "overcap"] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let a = consume(&assistant, text("A"));
        let repaired = upload("inspect");
        let mut malformed = repaired.clone();
        if let FrameData::UserImage(image) = &mut malformed.data {
            match invalid_kind {
                "empty" => image.image.image.clear(),
                "mismatch" => image.image.format = Some("image/jpeg".into()),
                _ => image.image.image.resize(8 * 1024 * 1024 + 1, 0),
            }
        }
        let rejected = consume(&assistant, malformed);
        let unchanged = context.snapshot_rich();
        let b = consume(&assistant, text("B"));
        let accepted = consume(&assistant, repaired);
        let after = context.snapshot_rich();
        let c = consume(&assistant, text("C"));
        let (end, completed, snapshot) = finish_direct(assistant, &mut receiver).await;
        start.unwrap();
        for result in [a, b, accepted, c] {
            silent(&result.unwrap());
        }
        assert!(rejected.is_err(), "{invalid_kind}");
        assert!(unchanged.unwrap().messages.is_empty());
        assert_eq!(
            after.unwrap().messages,
            vec![assistant_message("AB"), image_message("inspect")]
        );
        assert_eq!(
            snapshot.messages,
            vec![
                assistant_message("AB"),
                image_message("inspect"),
                assistant_message("C")
            ]
        );
        assert_eq!(completed.len(), 1);
        end_effects(&end, "C", &completed[0]);
    }
}

#[tokio::test]
async fn busy_append_only_does_not_project_unrelated_native_history() {
    use pipecat::services::{Content, ContentPart, Context, Message, Role};
    let prior = Message {
        role: Role::User,
        content: Some(Content::Parts(vec![ContentPart::InlineData {
            mime_type: "application/pdf".into(),
            data: "JVBERg==".into(),
        }])),
        name: None,
        tool_calls: vec![],
        tool_call_id: None,
    };
    let context = SharedContext::new(Context::new(vec![prior.clone()]));
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let held = consume(&assistant, text("A"));
    let before = context.frame_data();
    let accepted = consume(&assistant, upload("inspect"));
    let after = context.frame_data();
    let native = context.snapshot();
    // This history deliberately cannot provide a lossless rich snapshot. Complete
    // the actual no-suffix turn without the ordinary rich-snapshot test helper.
    let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let admitted = admit(&end);
    let duplicate = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let admitted_again = admit(&duplicate);
    let completed = records(&mut receiver);
    let final_native = context.snapshot();
    let final_rich = context.snapshot_rich();
    let terminal = consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    terminal.unwrap();
    admitted.unwrap();
    admitted_again.unwrap();
    silent(&duplicate.unwrap());
    start.unwrap();
    silent(&held.unwrap());
    silent(&accepted.unwrap());
    assert!(before.is_err());
    assert!(after.is_err());
    assert!(final_rich.is_err());
    let native = native.unwrap();
    assert_eq!(
        native.messages,
        vec![
            prior,
            Message::text(Role::Assistant, "A"),
            Message {
                role: Role::User,
                content: Some(Content::Parts(vec![
                    ContentPart::Text {
                        text: "inspect".into()
                    },
                    ContentPart::ImageUrl {
                        url: format!("data:image/png;base64,{PNG_BASE64}")
                    },
                ])),
                name: None,
                tool_calls: vec![],
                tool_call_id: None,
            },
        ]
    );
    assert_eq!(final_native.unwrap(), native);
    assert_eq!(completed.len(), 1);
    end_effects(&end.unwrap(), "", &completed[0]);
}

#[tokio::test]
async fn busy_marker_projection_and_job_identity_survive_midturn_flush() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let correlation = assistant.bind_job_generation(73).unwrap();
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let a = consume(&assistant, text("A"));
    let marker = consume(
        &assistant,
        Frame::new(FrameData::LlmMarker(LlmMarkerFrame {
            marker: "<DONE>".into(),
            append_to_context_immediately: false,
        })),
    );
    let image = consume(&assistant, upload("inspect"));
    let premature = records(&mut receiver);
    let b = consume(&assistant, text("B<DONE>"));
    let (end, completed, snapshot) = finish_direct(assistant, &mut receiver).await;
    assert_eq!(
        start.unwrap().events,
        vec![AggregationEvent::AssistantTurnStarted]
    );
    for result in [a, marker, image, b] {
        silent(&result.unwrap());
    }
    assert!(premature.is_empty());
    assert_eq!(
        snapshot.messages,
        vec![
            assistant_message("A <DONE>"),
            image_message("inspect"),
            assistant_message("B<DONE>")
        ]
    );
    assert_eq!(completed.len(), 1);
    assert!(correlation.matches(&completed[0]));
    assert_eq!(completed[0].job_generation(), Some(73));
    end_effects(&end, "B", &completed[0]);
}

#[tokio::test]
async fn busy_interruption_cancel_and_end_only_commit_unflushed_suffix() {
    for terminal in [
        Frame::interruption(),
        Frame::cancel("fixture"),
        Frame::end(),
    ] {
        let interrupted = !matches!(terminal.data, FrameData::End(_));
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let a = consume(&assistant, text("A"));
        let image = consume(&assistant, upload("inspect"));
        let b = consume(&assistant, text("B"));
        let stopped = consume(&assistant, terminal);
        let admitted = admit(&stopped);
        let repeated = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let again = admit(&repeated);
        let completed = records(&mut receiver);
        let snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
        for result in [a, image, b] {
            silent(&result.unwrap());
        }
        admitted.unwrap();
        again.unwrap();
        silent(&repeated.unwrap());
        assert_eq!(
            stopped.unwrap().events,
            vec![AggregationEvent::AssistantTurnStopped {
                text: "B".into(),
                interrupted
            }]
        );
        assert_eq!(
            snapshot.unwrap().messages,
            vec![
                assistant_message("A"),
                image_message("inspect"),
                assistant_message("B")
            ]
        );
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].text(), "B");
        assert_eq!(completed[0].interrupted(), interrupted);
        assert_eq!(completed[0].turn_id(), 1);
    }
}

#[tokio::test]
async fn busy_spoken_tts_only_thought_speech_tools_and_inference_debt_still_reject() {
    for gate in [
        "spoken",
        "tts-only",
        "thought",
        "thought-append",
        "user",
        "bot",
        "awaited",
        "playback",
        "tool",
        "run",
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(
            context.clone(),
            AssistantAggregatorConfig {
                text_mode: if matches!(gate, "spoken" | "playback") {
                    AssistantTextMode::Spoken
                } else {
                    AssistantTextMode::Text
                },
            },
        );
        let mut setup = vec![];
        setup.push(consume(
            &assistant,
            Frame::new(if gate == "tts-only" {
                FrameData::TtsStarted(Default::default())
            } else {
                FrameData::LlmResponseStart
            }),
        ));
        let mut prefix = TextFrame::llm("A");
        prefix.skip_tts = Some(true);
        setup.push(consume(&assistant, Frame::new(FrameData::LlmText(prefix))));
        let prelude = match gate {
            "thought" => Some(Frame::new(FrameData::LlmThoughtText(TextFrame::llm(
                "held thought",
            )))),
            "thought-append" => Some(Frame::new(FrameData::LlmThoughtStartDetailed(
                LlmThoughtStartFrame {
                    append_to_context: true,
                    llm: Some("fixture".into()),
                },
            ))),
            "user" => Some(Frame::new(FrameData::UserStartedSpeaking)),
            "bot" => Some(Frame::new(FrameData::BotStartedSpeaking)),
            "awaited" => {
                let mut f = Frame::new(FrameData::UserStoppedSpeaking);
                f.metadata
                    .insert("user_context_follows".into(), json!(true));
                Some(f)
            }
            "tool" => Some(Frame::new(FrameData::FunctionCallInProgress(
                FunctionCallInProgressFrame {
                    function_name: "pending".into(),
                    tool_call_id: "pending".into(),
                    arguments: json!({}),
                    cancel_on_interruption: false,
                    group_id: None,
                },
            ))),
            "run" => Some(Frame::new(FrameData::LlmRun)),
            _ => None,
        };
        if let Some(prelude) = prelude {
            setup.push(consume(&assistant, prelude));
        }
        let playback = (gate == "playback").then(|| assistant.begin_playback("pending"));
        let before = context.snapshot_rich();
        let rejected = consume(&assistant, upload("inspect"));
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        for result in setup {
            result.unwrap();
        }
        if let Some(playback) = playback {
            playback.unwrap();
        }
        assert!(
            rejected.unwrap_err().message.contains("idle assistant"),
            "{gate}"
        );
        assert_eq!(before.unwrap(), after.unwrap(), "{gate}");
    }
}

#[tokio::test]
async fn busy_realtime_and_unsigned_so_far_private_continuations_still_reject() {
    use pipecat::{
        services::Context,
        turns::{LLMContextAggregatorPair, RealtimeServiceMode},
    };
    for mode in [RealtimeServiceMode::Enabled, RealtimeServiceMode::Auto] {
        let pair = LLMContextAggregatorPair::with_realtime_mode(
            Context::default(),
            Default::default(),
            Default::default(),
            mode,
        )
        .unwrap();
        let assistant = pair.assistant();
        let announced = (mode == RealtimeServiceMode::Auto).then(|| {
            consume(
                &assistant,
                Frame::new(FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
                    base: ServiceMetadataFrame {
                        service_name: "busy-realtime".into(),
                        user_turn_strategies: None,
                    },
                    is_realtime_service: true,
                })),
            )
        });
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let a = consume(&assistant, text("A"));
        let before = pair.context().snapshot_rich();
        let rejected = consume(&assistant, upload("inspect"));
        let after = pair.context().snapshot_rich();
        let ac = assistant.cleanup().await;
        let uc = pair.user().cleanup().await;
        drop(assistant);
        drop(pair);
        ac.unwrap();
        uc.unwrap();
        if let Some(announced) = announced {
            announced.unwrap();
        }
        start.unwrap();
        a.unwrap();
        assert!(rejected.unwrap_err().message.contains("realtime"));
        assert_eq!(before.unwrap(), after.unwrap());
    }
    for provider in [
        LlmContinuationProvider::Gemini,
        LlmContinuationProvider::Anthropic,
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut frame = Frame::new(FrameData::LlmResponseStart);
        frame
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(773, provider).unwrap(),
            ))
            .unwrap();
        let start = consume(&assistant, frame);
        let a = consume(&assistant, text("A"));
        let before = context.snapshot_rich();
        let rejected = consume(&assistant, upload("inspect"));
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
        a.unwrap();
        assert!(rejected
            .unwrap_err()
            .message
            .contains("signed continuation"));
        assert_eq!(before.unwrap(), after.unwrap());
    }
}

#[tokio::test]
async fn busy_png_and_jpeg_are_consumed_in_either_direction_without_inference() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        for jpeg in [false, true] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
            let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
            let a = consume(&assistant, text("A"));
            let mut frame = upload("inspect");
            if jpeg {
                if let FrameData::UserImage(image) = &mut frame.data {
                    // The existing admission contract validates the encoded signature,
                    // not full decoding; this control does not claim provider acceptance.
                    image.image.image = vec![255, 216, 255, 217];
                    image.image.format = Some("image/jpeg".into());
                }
            }
            let accepted = assistant.process_at(frame, direction, Duration::ZERO);
            let (end, completed, snapshot) = finish_direct(assistant, &mut receiver).await;
            start.unwrap();
            silent(&a.unwrap());
            silent(&accepted.unwrap());
            let mut expected = image_message("inspect");
            if jpeg {
                if let MessageContent::Parts(parts) = &mut expected.content {
                    parts[1] = MessagePart::Image {
                        url: "data:image/jpeg;base64,/9j/2Q==".into(),
                        detail: None,
                    };
                }
            }
            assert_eq!(snapshot.messages, vec![assistant_message("A"), expected]);
            assert_eq!(completed.len(), 1);
            end_effects(&end, "", &completed[0]);
        }
    }
}
