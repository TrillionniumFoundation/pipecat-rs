//! Passive sampled images are consumed without image context or inference.
//! Encoded fixture bytes isolate consumer behavior from camera decoding.
use pipecat::{
    audio::PcmFormat,
    core::*,
    media_transport::{video::*, InputBackend, InputIngress, InputTransport, InputTransportConfig},
    turns::{
        AggregationAction, AggregationEvent, AggregationOutput, AssistantStoppedReceiver,
        AssistantTurnStopped, LLMAssistantAggregator, SharedContext,
    },
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};

const WAIT: Duration = Duration::from_secs(5);
const PNG: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0,
    0, 0, 144, 119, 83, 222, 0, 0, 0, 12, 73, 68, 65, 84, 120, 156, 99, 248, 207, 192, 0, 0, 3, 1,
    1, 0, 201, 254, 146, 239, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];
fn encoded_image() -> ImageRawFrame {
    ImageRawFrame {
        image: PNG.to_vec(),
        size: (1, 1),
        format: Some("image/png".into()),
    }
}
fn passive_image(append: Option<bool>, run: Option<bool>) -> Frame {
    Frame::new(FrameData::UserImage(UserImageRawFrame {
        image: encoded_image(),
        user_id: "peer".into(),
        text: Some("ignored image text".into()),
        append_to_context: append,
        request: None,
        run_llm: run,
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
#[derive(Default)]
struct Backend {
    started: AtomicUsize,
    active: AtomicUsize,
    stopped: AtomicUsize,
    connected: Notify,
}
struct ActiveCapture<'a>(&'a AtomicUsize);
impl Drop for ActiveCapture<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl InputBackend for Backend {
    fn supports_video_source(&self, source: &VideoSourceId) -> bool {
        source == &VideoSourceId::camera()
    }
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn capture(&self, ingress: InputIngress) -> Result<(), ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveCapture(&self.active);
        // The fixture backend owns an application-provided encoded image source.
        ingress.video_participant_connected(
            VideoParticipantId::new("peer").unwrap(),
            &[VideoSourceId::camera()],
        )?;
        self.connected.notify_one();
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct InputTap {
    barrier_id: u64,
    barrier: Notify,
    images: Mutex<Vec<DirectedFrame>>,
}
#[async_trait]
impl FrameProcessor for InputTap {
    fn name(&self) -> &str {
        "passive-image-input-tap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.id == self.barrier_id {
            // This marker went through the same real input queue after the image.
            self.barrier.notify_one();
            return Ok(());
        }
        if matches!(frame.data, FrameData::UserImage(_)) {
            self.images
                .lock()
                .unwrap()
                .push(DirectedFrame::new(frame.clone(), direction));
        }
        ctx.push(frame, direction).await
    }
}
async fn finish_input(
    worker: PipelineWorker,
) -> Result<(Vec<DirectedFrame>, WorkerReport), String> {
    let sender = worker.sender();
    let terminal = timeout(WAIT, sender.end())
        .await
        .map_err(|_| "End deadline".to_owned())
        .and_then(|r| r.map_err(|e| e.to_string()));
    if terminal.is_err() {
        let _ = timeout(WAIT, sender.cancel("End failed")).await;
    }
    // Collect drains the public output before joining; cancellation also joins.
    let mut collection = tokio::spawn(worker.collect());
    let collected = match timeout(WAIT, &mut collection).await {
        Ok(result) => result
            .map_err(|e| e.to_string())
            .and_then(|r| r.map_err(|e| e.to_string())),
        Err(_) => {
            let _ = timeout(WAIT, sender.cancel("input collection deadline")).await;
            match timeout(WAIT, &mut collection).await {
                Ok(result) => Err(format!(
                    "collection deadline; cancelled and joined: {result:?}"
                )),
                Err(_) => {
                    collection.abort();
                    let _ = collection.await;
                    Err("collection deadline after cancellation; aborted and joined".into())
                }
            }
        }
    };
    terminal.and(collected)
}

#[tokio::test]
async fn all_frames_input_without_request_is_consumed_without_context_or_inference() {
    let context = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        context.clone(),
        Default::default(),
    ));
    let backend = Arc::new(Backend::default());
    let mut config = InputTransportConfig {
        audio_enabled: false,
        video_enabled: true,
        operation_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    config
        .video
        .sources
        .insert(VideoSourceId::camera(), VideoInSourceParams::all_frames());
    let input =
        Arc::new(InputTransport::new("passive-input", config, backend.clone(), None).unwrap());
    let ingress = input.ingress();
    let marker = Frame::new(FrameData::InputImage(encoded_image()));
    let tap = Arc::new(InputTap {
        barrier_id: marker.id,
        barrier: Notify::new(),
        images: Mutex::new(vec![]),
    });
    let worker = Pipeline::new(vec![input, tap.clone(), assistant])
        .start(PipelineConfig {
            startup_timeout: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .await
        .unwrap();
    let key = VideoSourceKey::new(
        VideoParticipantId::new("peer").unwrap(),
        VideoSourceId::camera(),
    );
    let encoded = Frame::new(FrameData::InputImage(encoded_image()));
    let image_id = encoded.id;
    let scenario: Result<(bool, usize), String> = async {
        timeout(WAIT, backend.connected.notified())
            .await
            .map_err(|_| "source registration deadline")?;
        let ready = ingress.is_ready();
        let pending = ingress.pending_image_requests();
        ingress
            .push_video_frame(&key, encoded)
            .map_err(|e| e.to_string())?;
        ingress.push(marker).map_err(|e| e.to_string())?;
        timeout(WAIT, tap.barrier.notified())
            .await
            .map_err(|_| "input queue barrier deadline")?;
        Ok((ready, pending))
    }
    .await;
    // A failure still ends, drains and joins the real capture pipeline first.
    let finished = finish_input(worker).await;
    let snapshot = context.snapshot_rich();
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    let (frames, report) = finished.expect("passive input pipeline must drain and join");
    assert_eq!(scenario.unwrap(), (true, 0));
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(ingress.pending_image_requests(), 0);
    let observed = tap.images.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].direction, Direction::Downstream);
    assert_eq!(observed[0].frame.id, image_id);
    assert_eq!(
        observed[0].frame.transport_source.as_deref(),
        Some("camera")
    );
    let FrameData::UserImage(image) = &observed[0].frame.data else {
        panic!("expected actual sampled UserImage")
    };
    assert_eq!(image.image, encoded_image());
    assert_eq!(image.user_id, "peer");
    assert_eq!(image.text, None);
    assert_eq!(image.append_to_context, None);
    assert_eq!(image.run_llm, None);
    assert!(image.request.is_none());
    assert!(
        report.errors.is_empty(),
        "consumer rejected passive sampled image: {:?}",
        report.errors
    );
    assert!(snapshot.unwrap().messages.is_empty());
    assert!(!frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::UserImage(_)
            | FrameData::LlmContext(_)
            | FrameData::LlmContextDetailed(_)
            | FrameData::LlmContextAssistantTimestamp { .. }
            | FrameData::LlmContextAssistantTurn(_)
    )));
}

#[tokio::test]
async fn busy_passive_image_flushes_only_prefix_and_preserves_suffix_turn_lifecycle() {
    for with_image in [false, true] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let prefix = consume(&assistant, text("A"));
        let passive = with_image.then(|| consume(&assistant, passive_image(None, Some(true))));
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
        cleaned.unwrap();
        terminal.unwrap();
        assert_eq!(
            start.unwrap().events,
            vec![AggregationEvent::AssistantTurnStarted]
        );
        silent(&prefix.unwrap());
        if let Some(passive) = passive {
            silent(&passive.expect("ordinary active passive image must flush its held prefix"));
        }
        assert!(premature.is_empty());
        assert_eq!(during_stats.reserved, 0);
        assert_eq!(
            during.unwrap().messages,
            if with_image {
                vec![assistant_message("A")]
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
                vec![assistant_message("A"), assistant_message("B")]
            } else {
                vec![assistant_message("AB")]
            }
        );
    }
}

// Candidate controls. The preceding two tests and helpers are byte-frozen.
fn payload(frame: &mut Frame) -> &mut UserImageRawFrame {
    let FrameData::UserImage(image) = &mut frame.data else {
        panic!("not a user image")
    };
    image
}
fn plain_request() -> UserImageRequestFrame {
    UserImageRequestFrame {
        user_id: "unrelated request user".into(),
        text: Some("request text must not enter context".into()),
        append_to_context: Some(true),
        video_source: Some("camera".into()),
        function_name: None,
        tool_call_id: None,
        result_callback: None,
    }
}
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
    let again = admit(&duplicate);
    let completed = records(receiver);
    let snapshot = assistant.context().snapshot_rich();
    let terminal = consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    terminal.unwrap();
    admitted.unwrap();
    again.unwrap();
    silent(&duplicate.unwrap());
    (end.unwrap(), completed, snapshot.unwrap())
}

#[tokio::test]
async fn falsey_append_ignores_run_and_plain_request_with_empty_or_whitespace_prefix() {
    for append in [None, Some(false)] {
        for run in [None, Some(false), Some(true)] {
            for direction in [Direction::Downstream, Direction::Upstream] {
                for plain in [false, true] {
                    for prefix in [None, Some(""), Some(" \n\t "), Some(" A ")] {
                        let context = SharedContext::default();
                        let assistant =
                            LLMAssistantAggregator::new(context.clone(), Default::default());
                        let start = prefix
                            .map(|_| consume(&assistant, Frame::new(FrameData::LlmResponseStart)));
                        let held = prefix.map(|p| consume(&assistant, text(p)));
                        let mut image = passive_image(append, run);
                        // Falsey append precedes image construction, including its fields.
                        payload(&mut image).image = ImageRawFrame {
                            image: vec![],
                            size: (0, 0),
                            format: None,
                        };
                        payload(&mut image).request = plain.then(|| Box::new(plain_request()));
                        let result = assistant.process_at(image, direction, Duration::ZERO);
                        let during = context.snapshot_rich();
                        let suffix = prefix.map(|_| consume(&assistant, text("B")));
                        let end = prefix
                            .map(|_| consume(&assistant, Frame::new(FrameData::LlmResponseEnd)));
                        let after = context.snapshot_rich();
                        let terminal = consume(&assistant, Frame::end());
                        let cleaned = assistant.cleanup().await;
                        drop(assistant);
                        cleaned.unwrap();
                        terminal.unwrap();
                        if let Some(start) = start {
                            assert_eq!(
                                start.unwrap().events,
                                vec![AggregationEvent::AssistantTurnStarted]
                            );
                        }
                        if let Some(held) = held {
                            silent(&held.unwrap());
                        }
                        silent(
                            &result
                                .expect("passive frame flags and empty encoding must be consumed"),
                        );
                        let mut expected = vec![];
                        if let Some(p) = prefix.filter(|p| !p.is_empty()) {
                            expected.push(assistant_message(p.trim()));
                        }
                        assert_eq!(during.unwrap().messages, expected);
                        if let Some(suffix) = suffix {
                            silent(&suffix.unwrap());
                            expected.push(assistant_message("B"));
                        }
                        if let Some(end) = end {
                            assert_eq!(
                                end.unwrap().events,
                                vec![AggregationEvent::AssistantTurnStopped {
                                    text: "B".into(),
                                    interrupted: false
                                }]
                            );
                        }
                        assert_eq!(after.unwrap().messages, expected);
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn passive_raw_unknown_mime_mismatched_signature_and_overcap_bytes_are_not_encoded() {
    for kind in ["raw", "unknown", "mismatch", "overcap"] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut image = passive_image(Some(false), Some(true));
        let raw = &mut payload(&mut image).image;
        match kind {
            "raw" => {
                raw.image = vec![255, 0, 0];
                raw.format = Some("RGB".into());
            }
            "unknown" => {
                raw.image = vec![1];
                raw.format = Some("application/unknown".into());
                raw.size = (0, 0);
            }
            "mismatch" => {
                raw.format = Some("image/jpeg".into());
            }
            _ => {
                raw.image.resize(8 * 1024 * 1024 + 1, 0);
            }
        }
        let id = image.id;
        let result = consume(&assistant, image);
        let before = context.snapshot_rich();
        // A corrected upload with that identity is still new content.
        let mut corrected = passive_image(Some(true), Some(false));
        corrected.id = id;
        let accepted = consume(&assistant, corrected.clone());
        let replay = consume(&assistant, corrected);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(&result.unwrap());
        assert!(before.unwrap().messages.is_empty(), "{kind}");
        silent(&accepted.unwrap());
        silent(&replay.unwrap());
        let after = after.unwrap();
        assert_eq!(after.messages.len(), 1, "{kind}");
        assert_eq!(after.messages[0].role, ChatRole::User);
        let MessageContent::Parts(parts) = &after.messages[0].content else {
            panic!("expected corrected image")
        };
        assert!(
            matches!(&parts[1], MessagePart::Image { url, .. } if url.starts_with("data:image/png;base64,"))
        );
    }
}

#[tokio::test]
async fn repeated_passive_id_flushes_each_new_prefix_even_after_successful_image_admission() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let image = passive_image(Some(true), Some(false));
    let content = consume(&assistant, image.clone());
    let initial = context.snapshot_rich();
    let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let mut outputs = vec![];
    let mut snapshots = vec![];
    let mut passive = image;
    payload(&mut passive).append_to_context = None;
    payload(&mut passive).run_llm = Some(true);
    payload(&mut passive).image.image.clear();
    for prefix in ["A", "B"] {
        outputs.push(consume(&assistant, text(prefix)));
        outputs.push(consume(&assistant, passive.clone()));
        snapshots.push(context.snapshot_rich());
    }
    let premature = records(&mut receiver);
    let suffix = consume(&assistant, text("C"));
    let (end, completed, after) = finish_direct(assistant, &mut receiver).await;
    silent(&content.unwrap());
    start.unwrap();
    for output in outputs {
        silent(&output.unwrap());
    }
    silent(&suffix.unwrap());
    assert!(premature.is_empty());
    let mut expected = initial.unwrap().messages;
    assert_eq!(expected.len(), 1);
    expected.push(assistant_message("A"));
    assert_eq!(snapshots.remove(0).unwrap().messages, expected);
    expected.push(assistant_message("B"));
    assert_eq!(snapshots.remove(0).unwrap().messages, expected);
    expected.push(assistant_message("C"));
    assert_eq!(after.messages, expected);
    assert_eq!(completed.len(), 1);
    end_effects(&end, "C", &completed[0]);
}

#[tokio::test]
async fn passive_unique_ids_do_not_evict_successful_content_from_bounded_replay_cache() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let image = passive_image(Some(true), Some(false));
    let accepted = consume(&assistant, image.clone());
    let before = context.snapshot_rich();
    let mut passives = vec![];
    for _ in 0..513 {
        passives.push(consume(&assistant, passive_image(None, None)));
    }
    let replay = consume(&assistant, image);
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(&accepted.unwrap());
    for passive in passives {
        silent(&passive.unwrap());
    }
    silent(&replay.unwrap());
    assert_eq!(before.unwrap(), after.unwrap());
}

#[tokio::test]
async fn passive_prefix_commit_does_not_project_unrelated_native_history() {
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
    let accepted = consume(&assistant, passive_image(None, Some(true)));
    let after = context.frame_data();
    let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let admitted = admit(&end);
    let completed = records(&mut receiver);
    let native = context.snapshot();
    let terminal = consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    terminal.unwrap();
    start.unwrap();
    silent(&held.unwrap());
    silent(&accepted.unwrap());
    admitted.unwrap();
    assert!(before.is_err());
    assert!(after.is_err());
    assert_eq!(
        native.unwrap().messages,
        vec![prior, Message::text(Role::Assistant, "A")]
    );
    assert_eq!(completed.len(), 1);
    end_effects(&end.unwrap(), "", &completed[0]);
}

#[tokio::test]
async fn passive_flush_preserves_marker_and_original_job_turn_identity() {
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
    let image = consume(&assistant, passive_image(Some(false), None));
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
        vec![assistant_message("A <DONE>"), assistant_message("B<DONE>")]
    );
    assert_eq!(completed.len(), 1);
    assert!(correlation.matches(&completed[0]));
    assert_eq!(completed[0].job_generation(), Some(73));
    end_effects(&end, "B", &completed[0]);
}

#[tokio::test]
async fn passive_images_preserve_all_protected_activity_and_inference_debt_gates() {
    use pipecat::turns::{AssistantAggregatorConfig, AssistantTextMode};
    use serde_json::json;
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
        "tool-run",
    ] {
        for append in [None, Some(false)] {
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
            let mut setup = vec![consume(
                &assistant,
                Frame::new(if gate == "tts-only" {
                    FrameData::TtsStarted(Default::default())
                } else {
                    FrameData::LlmResponseStart
                }),
            )];
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
                "tool" | "tool-run" => Some(Frame::new(FrameData::FunctionCallInProgress(
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
            if gate == "tool-run" {
                setup.push(consume(
                    &assistant,
                    Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
                        function_name: "pending".into(),
                        tool_call_id: "pending".into(),
                        arguments: json!({}),
                        result: json!("done"),
                        run_llm: Some(true),
                        is_final: true,
                        error: None,
                    })),
                ));
            }
            let playback = (gate == "playback").then(|| assistant.begin_playback("pending"));
            let before = context.snapshot_rich();
            let pending_before = assistant.has_function_calls_in_progress();
            let mut rejected = vec![];
            for run in [None, Some(true), Some(false)] {
                let mut image = passive_image(append, run);
                payload(&mut image).image.image.clear();
                payload(&mut image).image.format = None;
                rejected.push(consume(&assistant, image));
            }
            let after = context.snapshot_rich();
            let pending_after = assistant.has_function_calls_in_progress();
            let debt_end = matches!(gate, "run" | "tool-run")
                .then(|| consume(&assistant, Frame::new(FrameData::LlmResponseEnd)));
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            for result in setup {
                result.unwrap();
            }
            if let Some(playback) = playback {
                playback.unwrap();
            }
            for result in rejected {
                assert!(
                    result.unwrap_err().message.contains("idle assistant"),
                    "{gate}"
                );
            }
            assert_eq!(before.unwrap(), after.unwrap(), "{gate}");
            assert_eq!(pending_before.unwrap(), pending_after.unwrap(), "{gate}");
            if let Some(end) = debt_end {
                let end = end.unwrap();
                assert_eq!(
                    end.events,
                    vec![AggregationEvent::AssistantTurnStopped {
                        text: "A".into(),
                        interrupted: false
                    }]
                );
                assert_eq!(end.actions.iter().filter(|action| matches!(action,
                    AggregationAction::Push(frame) if frame.direction == Direction::Upstream && matches!(frame.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_))
                )).count(), 1, "{gate} debt must remain owed");
            }
        }
    }
}

#[tokio::test]
async fn passive_request_authority_never_invokes_callback_or_settles_pending_tool() {
    use serde_json::json;
    for append in [None, Some(false)] {
        for (function, tool, callback, pending) in [
            (Some(""), None, false, false),
            (Some("capture"), None, false, false),
            (None, Some(""), false, false),
            (None, Some("unknown"), false, false),
            (None, Some("owned"), false, true),
            (None, None, true, false),
            (None, None, true, true),
        ] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let calls = Arc::new(AtomicUsize::new(0));
            let log = calls.clone();
            let setup = pending.then(|| {
                consume(
                    &assistant,
                    Frame::new(FrameData::FunctionCallInProgress(
                        FunctionCallInProgressFrame {
                            function_name: "capture".into(),
                            tool_call_id: "owned".into(),
                            arguments: json!({}),
                            cancel_on_interruption: false,
                            group_id: None,
                        },
                    )),
                )
            });
            let before = context.snapshot();
            let mut request = plain_request();
            request.function_name = function.map(str::to_owned);
            request.tool_call_id = tool.map(str::to_owned);
            request.result_callback = callback.then(|| {
                LocalResultCallback::new(move |_| {
                    log.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                })
            });
            let mut image = passive_image(append, Some(false));
            payload(&mut image).request = Some(Box::new(request));
            payload(&mut image).image.image.clear();
            payload(&mut image).image.format = None;
            let rejected = consume(&assistant, image);
            let after = context.snapshot();
            let still_pending = assistant.has_function_calls_in_progress();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            if let Some(setup) = setup {
                setup.unwrap();
            }
            assert!(rejected.unwrap_err().message.contains("raw image/file"));
            assert_eq!(before.unwrap(), after.unwrap());
            assert_eq!(still_pending.unwrap(), pending);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn passive_images_preserve_private_carrier_signed_owner_and_realtime_guards() {
    use pipecat::{
        services::Context,
        turns::{LLMContextAggregatorPair, RealtimeServiceMode},
    };
    use serde_json::json;
    for append in [None, Some(false)] {
        let mut continuation = Frame::new(FrameData::LlmResponseStart);
        continuation
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(994201, LlmContinuationProvider::Gemini).unwrap(),
            ))
            .unwrap();
        let call = FunctionCall {
            function_name: "capture".into(),
            tool_call_id: "owned".into(),
            arguments: json!({}),
        };
        let mut tool = Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()]));
        tool.set_native_tool_batch(
            NativeToolCallBatch::new("owned-group".into(), vec![(Arc::new(call), false)]).unwrap(),
        )
        .unwrap();
        for mut carrier in [continuation.clone(), tool] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let image = passive_image(Some(true), Some(false));
            carrier.id = image.id;
            let admitted = consume(&assistant, image);
            let mut passive = passive_image(append, None);
            payload(&mut passive).image.image.clear();
            carrier.data = passive.data;
            let before = context.snapshot();
            let rejected = consume(&assistant, carrier);
            let after = context.snapshot();
            let pending = assistant.has_function_calls_in_progress();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            silent(&admitted.unwrap());
            assert!(
                rejected.is_err(),
                "private authority precedes passive handling and cached ID"
            );
            assert_eq!(before.unwrap(), after.unwrap());
            assert!(!pending.unwrap());
        }
        for provider in [
            LlmContinuationProvider::Gemini,
            LlmContinuationProvider::Anthropic,
        ] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let mut start = Frame::new(FrameData::LlmResponseStart);
            start
                .set_native_llm_continuation(Arc::new(
                    NativeLlmContinuation::start(994202, provider).unwrap(),
                ))
                .unwrap();
            let started = consume(&assistant, start);
            let held = consume(&assistant, text("A"));
            let before = context.snapshot();
            let rejected = consume(&assistant, passive_image(append, Some(false)));
            let after = context.snapshot();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            started.unwrap();
            held.unwrap();
            assert!(rejected
                .unwrap_err()
                .message
                .contains("signed continuation"));
            assert_eq!(before.unwrap(), after.unwrap());
        }
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
                            service_name: "passive-realtime".into(),
                            user_turn_strategies: None,
                        },
                        is_realtime_service: true,
                    })),
                )
            });
            let before = pair.context().snapshot();
            let rejected = consume(&assistant, passive_image(append, None));
            let after = pair.context().snapshot();
            let ac = assistant.cleanup().await;
            let uc = pair.user().cleanup().await;
            drop(assistant);
            drop(pair);
            ac.unwrap();
            uc.unwrap();
            if let Some(announced) = announced {
                announced.unwrap();
            }
            assert!(rejected.unwrap_err().message.contains("realtime"));
            assert_eq!(before.unwrap(), after.unwrap());
        }
    }
}

#[tokio::test]
async fn terminal_passive_frames_keep_existing_ignore_and_tool_carrier_order() {
    use serde_json::json;
    for terminal in [
        Frame::end(),
        Frame::new(FrameData::Stop),
        Frame::cancel("fixture"),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let ended = consume(&assistant, terminal);
        let calls = Arc::new(AtomicUsize::new(0));
        let log = calls.clone();
        let mut late = passive_image(None, Some(true));
        payload(&mut late).image.image.clear();
        payload(&mut late).image.format = None;
        let mut request = plain_request();
        request.tool_call_id = Some("owned".into());
        request.result_callback = Some(LocalResultCallback::new(move |_| {
            log.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }));
        payload(&mut late).request = Some(Box::new(request));
        let ignored = consume(&assistant, late.clone());
        let mut continuation = Frame::new(FrameData::LlmResponseStart);
        continuation
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(994203, LlmContinuationProvider::Gemini).unwrap(),
            ))
            .unwrap();
        continuation.data = late.data.clone();
        let private_ignored = consume(&assistant, continuation);
        let call = FunctionCall {
            function_name: "capture".into(),
            tool_call_id: "owned".into(),
            arguments: json!({}),
        };
        let mut tool = Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()]));
        tool.set_native_tool_batch(
            NativeToolCallBatch::new("late-group".into(), vec![(Arc::new(call), false)]).unwrap(),
        )
        .unwrap();
        tool.data = late.data;
        let invalid_tool = consume(&assistant, tool);
        let after = context.snapshot_rich();
        let pending = assistant.has_function_calls_in_progress();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        ended.unwrap();
        silent(&ignored.unwrap());
        silent(&private_ignored.unwrap());
        assert!(
            invalid_tool.is_err(),
            "common tool-carrier validation still precedes ended branch"
        );
        assert!(after.unwrap().messages.is_empty());
        assert!(!pending.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
