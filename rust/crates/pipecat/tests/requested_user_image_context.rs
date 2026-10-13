//! Public plain-request content contract and real InputTransport-owned production.
//! Application-supplied encoded bytes, not a camera decoder or provider I/O test.
use pipecat::{
    audio::PcmFormat,
    core::*,
    media_transport::{video::*, InputBackend, InputIngress, InputTransport, InputTransportConfig},
    services::{adapters::request_body, CompletionRequest, Provider},
    turns::{AggregationAction, AggregationOutput, LLMAssistantAggregator, SharedContext},
};
use serde_json::{json, Value};
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
const PNG_BASE64: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
fn plain_request() -> UserImageRequestFrame {
    UserImageRequestFrame {
        user_id: "peer".into(),
        text: Some("inspect".into()),
        append_to_context: Some(true),
        video_source: Some("camera".into()),
        function_name: None,
        tool_call_id: None,
        result_callback: None,
    }
}
fn encoded_image() -> ImageRawFrame {
    ImageRawFrame {
        image: PNG.to_vec(),
        size: (1, 1),
        format: Some("image/png".into()),
    }
}
fn direct_image(run: Option<bool>) -> Frame {
    Frame::new(FrameData::UserImage(UserImageRawFrame {
        image: encoded_image(),
        user_id: "peer".into(),
        text: Some("inspect".into()),
        append_to_context: Some(true),
        request: Some(Box::new(plain_request())),
        run_llm: run,
    }))
}
fn png_message() -> ContextMessage {
    ContextMessage {
        role: ChatRole::User,
        content: MessageContent::Parts(vec![
            MessagePart::Text {
                text: "inspect".into(),
            },
            MessagePart::Image {
                url: format!("data:image/png;base64,{PNG_BASE64}"),
                detail: None,
            },
        ]),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: Default::default(),
    }
}
fn one_context(output: AggregationOutput) -> DetailedLlmContextFrame {
    assert!(output.events.is_empty());
    assert_eq!(output.actions.len(), 1);
    let AggregationAction::Push(directed) = &output.actions[0] else {
        panic!("expected one upstream context push")
    };
    assert_eq!(directed.direction, Direction::Upstream);
    let FrameData::LlmContextDetailed(context) = &directed.frame.data else {
        panic!("expected detailed context, not the original image")
    };
    assert!(!context.speculation);
    context.clone()
}
fn assert_png_body(context: &LlmContextData) {
    let native = pipecat::services::frame_context::context(context).unwrap();
    let body = request_body(Provider::Gemini, &CompletionRequest::new("fixture", native)).unwrap();
    assert_eq!(
        body["contents"],
        json!([
            {"role":"user", "parts":[
                {"text":"inspect"},
                {"inlineData":{"mimeType":"image/png", "data":PNG_BASE64}}
            ]}
        ])
    );
}

#[tokio::test]
async fn direct_plain_request_true_and_none_use_existing_image_content_contract() {
    // Hand-constructed content only: this does not attest InputTransport ownership.
    for run in [Some(true), None] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let result = assistant.process_at(direct_image(run), Direction::Downstream, Duration::ZERO);
        let snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        let output =
            one_context(result.expect("plain request must not block encoded image content"));
        assert_eq!(output.context, snapshot.unwrap());
        assert_eq!(output.context.messages, vec![png_message()]);
        assert_png_body(&output.context);
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
        "requested-image-input-tap"
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
async fn owned_plain_input_request_reaches_shared_context_and_gemini_contents() {
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
    config.video.sources.insert(
        VideoSourceId::camera(),
        VideoInSourceParams::on_request_only(),
    );
    let input =
        Arc::new(InputTransport::new("encoded-input", config, backend.clone(), None).unwrap());
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
    let admission = Arc::new(Mutex::new(Vec::new()));
    let log = admission.clone();
    let mut request = plain_request();
    // Input alone owns this receipt. Null means downstream queue admission only.
    request.result_callback = Some(LocalResultCallback::new(move |value| {
        log.lock().unwrap().push(value);
        async { Ok(()) }
    }));
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
        ingress
            .request_image(Frame::new(FrameData::UserImageRequest(request)))
            .map_err(|e| e.to_string())?;
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
    // Every scenario error still goes through End/drain/join before assertions.
    let finished = finish_input(worker).await;
    let snapshot = context.snapshot_rich();
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    let (frames, report) = finished.expect("input pipeline must drain and join");
    assert_eq!(scenario.unwrap(), (true, 1));
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(ingress.pending_image_requests(), 0);
    assert_eq!(*admission.lock().unwrap(), vec![Value::Null]);
    let observed = tap.images.lock().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].direction, Direction::Downstream);
    assert_eq!(observed[0].frame.id, image_id);
    assert_eq!(
        observed[0].frame.transport_source.as_deref(),
        Some("camera")
    );
    let FrameData::UserImage(image) = &observed[0].frame.data else {
        panic!("expected owned image")
    };
    assert_eq!(image.image, encoded_image());
    assert_eq!(image.user_id, "peer");
    assert_eq!(image.text.as_deref(), Some("inspect"));
    assert_eq!(image.append_to_context, Some(true));
    assert_eq!(image.run_llm, None);
    let request = image
        .request
        .as_ref()
        .expect("Input must retain request association");
    assert_eq!(**request, plain_request());
    assert!(request.result_callback.is_none());
    // On the old implementation this assertion exposes the exact unsupported error,
    // after proving real production, admission receipt and complete cleanup.
    assert!(
        report.errors.is_empty(),
        "consumer rejected owned plain image: {:?}",
        report.errors
    );
    let contexts: Vec<_> = frames
        .iter()
        .filter(|frame| {
            matches!(
                frame.frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .collect();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].direction, Direction::Upstream);
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.frame.data, FrameData::UserImage(_))));
    let FrameData::LlmContextDetailed(emitted) = &contexts[0].frame.data else {
        panic!("expected detailed context")
    };
    assert!(!emitted.speculation);
    assert_eq!(emitted.context, snapshot.unwrap());
    assert_eq!(emitted.context.messages, vec![png_message()]);
    assert_png_body(&emitted.context);
}

// Candidate controls. The preceding two tests and helpers are byte-frozen.
fn consume(
    assistant: &LLMAssistantAggregator,
    frame: Frame,
) -> Result<AggregationOutput, ProcessorError> {
    assistant.process_at(frame, Direction::Downstream, Duration::ZERO)
}
fn silent(output: AggregationOutput) {
    assert!(output.actions.is_empty());
    assert!(output.events.is_empty());
}
fn payload(frame: &mut Frame) -> &mut UserImageRawFrame {
    let FrameData::UserImage(image) = &mut frame.data else {
        panic!("not a user image")
    };
    image
}

#[tokio::test]
async fn frame_fields_win_over_mismatched_request_for_png_jpeg_and_run_flags() {
    let jpeg = include_bytes!("../../pipecat-images/tests/fixtures/source-cat.jpg");
    for (bytes, mime) in [(PNG, "image/png"), (jpeg.as_slice(), "image/jpeg")] {
        for run in [None, Some(true), Some(false)] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let mut frame = direct_image(run);
            let image = payload(&mut frame);
            image.image.image = bytes.to_vec();
            image.image.format = Some(mime.into());
            image.text = Some("frame content".into());
            let request = image.request.as_mut().unwrap();
            request.text = Some("do not substitute this request text".into());
            request.append_to_context = Some(false);
            request.user_id = "different request user".into();
            let result = consume(&assistant, frame);
            let rich = context.snapshot_rich();
            let native = context.snapshot();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            let rich = rich.unwrap();
            if run == Some(false) {
                silent(result.unwrap());
            } else {
                assert_eq!(one_context(result.unwrap()).context, rich);
            }
            assert_eq!(rich.messages.len(), 1);
            assert_eq!(rich.messages[0].role, ChatRole::User);
            assert!(rich.messages[0].name.is_none());
            let body = request_body(
                Provider::Gemini,
                &CompletionRequest::new("fixture", native.unwrap()),
            )
            .unwrap();
            let data = body["contents"][0]["parts"][1]["inlineData"]["data"]
                .as_str()
                .unwrap();
            assert_eq!(
                body["contents"],
                json!([{"role":"user", "parts":[
                    {"text":"frame content"}, {"inlineData":{"mimeType":mime,"data":data}}
                ]}])
            );
            if mime == "image/png" {
                assert_eq!(data, PNG_BASE64);
            }
            // Decode the whole provider inline-data payload through the existing
            // public codec; this request-free envelope is not ownership evidence.
            let wire = json!({"__type__":"pipecat.bus.messages.BusFrameMessage", "__data__":{
                "source":"verify","target":"verify",
                "direction":{"__type__":"pipecat.processors.frame_processor.FrameDirection","__data__":"DOWNSTREAM"},
                "frame":{"__type__":"pipecat.frames.frames.UserImageRawFrame","__data__":{
                    "id":994190, "name":"verify", "image":{"__type__":"bytes","__data__":data},
                    "size":[1,1],"format":mime,"user_id":"","append_to_context":true,"run_llm":false
                }}
            }});
            let decoded = pipecat::network_bus::PythonJsonCodec::default()
                .decode(&serde_json::to_vec(&wire).unwrap())
                .unwrap();
            let pipecat::workers::MessageData::Frame { frame, .. } =
                &decoded.as_bus_message().unwrap().data
            else {
                panic!("not a decoded frame")
            };
            let FrameData::UserImage(image) = &frame.data else {
                panic!("not a decoded image")
            };
            assert_eq!(image.image.image, bytes);
        }
    }
    for append in [None, Some(false)] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut frame = direct_image(None);
        payload(&mut frame).append_to_context = append;
        let rejected = consume(&assistant, frame.clone());
        let before = context.snapshot_rich();
        payload(&mut frame).append_to_context = Some(true);
        let retry = consume(&assistant, frame);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(
            rejected.is_err(),
            "request true must not override frame {append:?}"
        );
        assert!(before.unwrap().messages.is_empty());
        assert_eq!(one_context(retry.unwrap()).context, after.unwrap());
    }
}

#[tokio::test]
async fn plain_request_validation_replay_and_projection_repair_remain_atomic() {
    use pipecat::services::{Content, ContentPart, Context, Message, Role};
    for run in [Some(true), None] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let good = direct_image(run);
        let mut bad = good.clone();
        payload(&mut bad).image.image.clear();
        let invalid = consume(&assistant, bad.clone());
        let before = context.snapshot_rich();
        let accepted = consume(&assistant, good.clone());
        let replay = consume(&assistant, good.clone());
        let bad_replay = consume(&assistant, bad);
        let after = context.snapshot_rich();
        let mut next = direct_image(run);
        payload(&mut next).text = Some("next".into());
        let distinct = consume(&assistant, next);
        let final_context = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(invalid.is_err());
        assert!(before.unwrap().messages.is_empty());
        assert_eq!(one_context(accepted.unwrap()).context, after.unwrap());
        silent(replay.unwrap());
        assert!(bad_replay.is_err(), "validate before successful-ID lookup");
        let final_context = final_context.unwrap();
        assert_eq!(one_context(distinct.unwrap()).context, final_context);
        assert_eq!(final_context.messages.len(), 2);

        let prefix = Message {
            role: Role::User,
            content: Some(Content::Parts(vec![ContentPart::InlineData {
                mime_type: "application/pdf".into(),
                data: "JVBERg==".into(),
            }])),
            name: None,
            tool_calls: vec![],
            tool_call_id: None,
        };
        let context = SharedContext::new(Context::new(vec![prefix.clone()]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let image = direct_image(run);
        let rejected = consume(&assistant, image.clone());
        let unchanged = context.snapshot();
        let unprojectable = context.frame_data();
        let repair = context.replace(Context::default());
        let retry = consume(&assistant, image.clone());
        let replay = consume(&assistant, image);
        let final_context = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected.unwrap_err().message.contains("InlineData"));
        assert_eq!(unchanged.unwrap(), Context::new(vec![prefix]));
        assert!(unprojectable.is_err());
        repair.unwrap();
        let output = one_context(retry.unwrap());
        assert_eq!(output.context, final_context.unwrap());
        assert_eq!(output.context.messages, vec![png_message()]);
        silent(replay.unwrap());
    }
}

#[tokio::test]
async fn each_request_authority_field_rejects_without_callback_or_pending_tool_settlement() {
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
        let mut image = direct_image(None);
        let request = payload(&mut image).request.as_mut().unwrap();
        request.function_name = function.map(str::to_owned);
        request.tool_call_id = tool.map(str::to_owned);
        request.result_callback = callback.then(|| {
            LocalResultCallback::new(move |_| {
                log.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })
        });
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

#[tokio::test]
async fn plain_request_preserves_private_carrier_realtime_and_signed_owner_guards() {
    use pipecat::{
        services::Context,
        turns::{LLMContextAggregatorPair, RealtimeServiceMode},
    };
    let mut continuation = Frame::new(FrameData::LlmResponseStart);
    continuation
        .set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::start(994191, LlmContinuationProvider::Gemini).unwrap(),
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
        let image = direct_image(Some(false));
        carrier.id = image.id;
        carrier.data = image.data.clone();
        let admitted = consume(&assistant, image);
        let before = context.snapshot();
        let rejected = consume(&assistant, carrier);
        let after = context.snapshot();
        let pending = assistant.has_function_calls_in_progress();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(admitted.unwrap());
        assert!(rejected.is_err(), "authority must be checked before replay");
        assert_eq!(before.unwrap(), after.unwrap());
        assert!(!pending.unwrap());
    }
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let signed_start = consume(&assistant, continuation);
    let rejected = consume(&assistant, direct_image(Some(false)));
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    signed_start.unwrap();
    assert!(rejected
        .unwrap_err()
        .message
        .contains("signed continuation"));
    assert!(after.unwrap().messages.is_empty());

    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    let rejected = consume(&pair.assistant(), direct_image(None));
    let after = pair.context().snapshot_rich();
    let assistant_cleaned = pair.assistant().cleanup().await;
    let user_cleaned = pair.user().cleanup().await;
    drop(pair);
    assistant_cleaned.unwrap();
    user_cleaned.unwrap();
    assert!(rejected.unwrap_err().message.contains("realtime"));
    assert!(after.unwrap().messages.is_empty());
}

#[tokio::test]
async fn direct_plain_request_false_uses_only_existing_busy_text_path() {
    use pipecat::turns::{AggregationEvent, AssistantAggregatorConfig, AssistantTextMode};
    for mode in [AssistantTextMode::Text, AssistantTextMode::Spoken] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(
            context.clone(),
            AssistantAggregatorConfig { text_mode: mode },
        );
        let started = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let prefix = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm("before"))),
        );
        let image = consume(&assistant, direct_image(Some(false)));
        let middle = context.snapshot_rich();
        let suffix = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm(" after"))),
        );
        let ended = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        started.unwrap();
        prefix.unwrap();
        suffix.unwrap();
        if mode == AssistantTextMode::Text {
            silent(image.unwrap());
            let middle = middle.unwrap();
            assert_eq!(middle.messages.len(), 2);
            assert_eq!(middle.messages[0].role, ChatRole::Assistant);
            assert_eq!(
                middle.messages[0].content,
                MessageContent::Text("before".into())
            );
            assert_eq!(middle.messages[1], png_message());
            assert_eq!(
                ended.unwrap().events,
                vec![AggregationEvent::AssistantTurnStopped {
                    text: "after".into(),
                    interrupted: false
                }]
            );
            let after = after.unwrap();
            assert_eq!(after.messages.len(), 3);
            assert_eq!(
                after.messages[2].content,
                MessageContent::Text("after".into())
            );
        } else {
            assert!(image.unwrap_err().message.contains("idle assistant"));
            assert!(middle.unwrap().messages.is_empty());
            ended.unwrap();
            assert!(after
                .unwrap()
                .messages
                .iter()
                .all(|message| message.role != ChatRole::User));
        }
    }
}

#[tokio::test]
async fn real_input_rejects_forgery_preserves_owned_request_and_busy_none_still_rejects() {
    for busy in [false, true] {
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
        config.video.sources.insert(
            VideoSourceId::camera(),
            VideoInSourceParams::on_request_only(),
        );
        let input = Arc::new(
            InputTransport::new("owned-image-control", config, backend.clone(), None).unwrap(),
        );
        let ingress = input.ingress();
        let marker = Frame::new(FrameData::InputImage(encoded_image()));
        let tap = Arc::new(InputTap {
            barrier_id: marker.id,
            barrier: Notify::new(),
            images: Mutex::new(vec![]),
        });
        let mut worker = Pipeline::new(vec![input, tap.clone(), assistant.clone()])
            .start(PipelineConfig {
                startup_timeout: Duration::from_secs(2),
                shutdown_timeout: Duration::from_secs(2),
                ..Default::default()
            })
            .await
            .unwrap();
        let admission = Arc::new(Mutex::new(Vec::new()));
        let log = admission.clone();
        let mut request = plain_request();
        request.result_callback = Some(LocalResultCallback::new(move |value| {
            log.lock().unwrap().push(value);
            async { Ok(()) }
        }));
        let key = VideoSourceKey::new(
            VideoParticipantId::new("peer").unwrap(),
            VideoSourceId::camera(),
        );
        let owned = Frame::new(FrameData::InputImage(encoded_image()));
        let owned_id = owned.id;
        let forged = direct_image(None);
        let forged_id = forged.id;
        let scenario: Result<(bool, bool, usize, usize), String> = async {
            timeout(WAIT, backend.connected.notified())
                .await
                .map_err(|_| "source registration deadline")?;
            // Observe the actual pipeline Start at its output before setting up
            // busy state, rather than infer processing from a scheduler delay.
            timeout(WAIT, async {
                while let Some(frame) = worker.recv().await {
                    if frame.direction == Direction::Downstream && frame.frame.data.is_start() {
                        return Ok(());
                    }
                    if matches!(frame.frame.data, FrameData::Error(_)) {
                        return Err("error before Start receipt");
                    }
                }
                Err("closed before Start receipt")
            })
            .await
            .map_err(|_| "Start receipt deadline")??;
            if busy {
                consume(&assistant, Frame::new(FrameData::LlmResponseStart))
                    .map_err(|e| e.to_string())?;
            }
            ingress
                .request_image(Frame::new(FrameData::UserImageRequest(request)))
                .map_err(|e| e.to_string())?;
            let generic_rejected = ingress.push(forged.clone()).is_err();
            let keyed_rejected = ingress.push_video_frame(&key, forged).is_err();
            let pending = ingress.pending_image_requests();
            let callbacks_before = admission.lock().unwrap().len();
            ingress
                .push_video_frame(&key, owned)
                .map_err(|e| e.to_string())?;
            ingress.push(marker).map_err(|e| e.to_string())?;
            timeout(WAIT, tap.barrier.notified())
                .await
                .map_err(|_| "input queue barrier deadline")?;
            Ok((generic_rejected, keyed_rejected, pending, callbacks_before))
        }
        .await;
        let finished = finish_input(worker).await;
        let snapshot = context.snapshot_rich();
        assert_eq!(backend.started.load(Ordering::SeqCst), 1);
        assert_eq!(backend.active.load(Ordering::SeqCst), 0);
        assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
        let (frames, report) = finished.expect("ownership-control pipeline drained and joined");
        assert_eq!(scenario.unwrap(), (true, true, 1, 0));
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(ingress.pending_image_requests(), 0);
        // This remains Null even when the assistant rejects the busy None-run
        // image: callback success attests only the downstream queue admission.
        assert_eq!(*admission.lock().unwrap(), vec![Value::Null]);
        let observed = tap.images.lock().unwrap();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].frame.id, owned_id);
        assert_ne!(observed[0].frame.id, forged_id);
        assert_eq!(observed[0].direction, Direction::Downstream);
        let FrameData::UserImage(image) = &observed[0].frame.data else {
            panic!("expected owned user image")
        };
        assert_eq!(image.image, encoded_image());
        assert_eq!(image.run_llm, None);
        assert_eq!(**image.request.as_ref().unwrap(), plain_request());
        let contexts: Vec<_> = frames
            .iter()
            .filter(|frame| {
                matches!(
                    frame.frame.data,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                )
            })
            .collect();
        let snapshot = snapshot.unwrap();
        if busy {
            assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
            assert!(report.errors[0].error.contains("idle assistant"));
            assert!(contexts.is_empty());
            assert!(snapshot.messages.is_empty());
        } else {
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert_eq!(contexts.len(), 1);
            assert_eq!(contexts[0].direction, Direction::Upstream);
            let FrameData::LlmContextDetailed(emitted) = &contexts[0].frame.data else {
                panic!("expected detailed context")
            };
            assert_eq!(emitted.context, snapshot);
            assert_eq!(snapshot.messages, vec![png_message()]);
            assert_png_body(&snapshot);
        }
    }
}
