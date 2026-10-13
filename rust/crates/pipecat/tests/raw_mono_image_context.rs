//! Exact packed mode-1 consumption through AssistantImage and owned InputTransport.
//! Two baseline witnesses frozen before controls; no camera or provider I/O.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use jpeg_decoder::{CodingProcess, Decoder, PixelFormat};
use pipecat::{
    audio::PcmFormat,
    core::*,
    media_transport::{video::*, InputBackend, InputIngress, InputTransport, InputTransportConfig},
    services::{adapters::request_body, CompletionRequest, Provider},
    turns::{AggregationOutput, LLMAssistantAggregator, SharedContext},
};
use serde_json::{json, Value};
use std::{
    io::Cursor,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};

const WAIT: Duration = Duration::from_secs(5);
const IMAGE_CAP: usize = 8 * 1024 * 1024;
fn mono_image() -> ImageRawFrame {
    // Three independently authored top-down rows, 17 valid bits per 3-byte row.
    ImageRawFrame {
        image: vec![0xa1, 0x46, 0x80, 0x16, 0x89, 0x00, 0xc8, 0x31, 0x80],
        size: (17, 3),
        format: Some("1".into()),
    }
}
fn literal_luma() -> ImageRawFrame {
    // Literal L rows, deliberately not computed with the proposed bit unpacker.
    ImageRawFrame {
        image: vec![
            255, 0, 255, 0, 0, 0, 0, 255, 0, 255, 0, 0, 0, 255, 255, 0, 255, 0, 0, 0, 255, 0, 255,
            255, 0, 255, 0, 0, 0, 255, 0, 0, 255, 0, 255, 255, 0, 0, 255, 0, 0, 0, 0, 0, 255, 255,
            0, 0, 0, 255, 255,
        ],
        size: (17, 3),
        format: Some("L".into()),
    }
}
fn block_image(packed: bool) -> ImageRawFrame {
    // Separate broad, asymmetric blocks keep the interior pixel oracle meaningful
    // despite JPEG loss. These literal rows do not unpack the packed fixture.
    let (upper, lower) = if packed {
        (
            vec![255, 255, 0, 0, 255, 255, 128],
            vec![0, 0, 255, 255, 0, 0, 0],
        )
    } else {
        let mut upper = vec![255; 16];
        upper.extend_from_slice(&[0; 16]);
        upper.extend_from_slice(&[255; 17]);
        let mut lower = vec![0; 16];
        lower.extend_from_slice(&[255; 16]);
        lower.extend_from_slice(&[0; 17]);
        (upper, lower)
    };
    let mut image = upper.repeat(16);
    image.extend(lower.repeat(17));
    ImageRawFrame {
        image,
        size: (49, 33),
        format: Some(if packed { "1" } else { "L" }.into()),
    }
}
fn image_frame(image: ImageRawFrame, role: ChatRole) -> Frame {
    if role == ChatRole::User {
        Frame::new(FrameData::UserImage(UserImageRawFrame {
            image,
            user_id: "peer".into(),
            text: Some("inspect".into()),
            append_to_context: Some(true),
            request: Some(Box::new(plain_request())),
            run_llm: Some(false),
        }))
    } else {
        Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
            image: OutputImageRawFrame {
                image,
                sync_with_audio: true,
            },
            original_data: None,
            original_mime_type: None,
        }))
    }
}
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
fn assistant_image() -> Frame {
    Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
        image: OutputImageRawFrame {
            image: mono_image(),
            sync_with_audio: true,
        },
        original_data: None,
        original_mime_type: None,
    }))
}
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
fn jpeg_data(message: &ContextMessage, role: ChatRole, text: Option<&str>) -> String {
    assert_eq!(message.role, role);
    assert!(message.name.is_none());
    assert!(message.tool_call_id.is_none());
    assert!(message.tool_calls.is_empty());
    assert!(message.extra.is_empty());
    let MessageContent::Parts(parts) = &message.content else {
        panic!("expected rich image")
    };
    assert_eq!(parts.len(), if text.is_some() { 2 } else { 1 });
    if let Some(text) = text {
        assert_eq!(parts[0], MessagePart::Text { text: text.into() });
    }
    let MessagePart::Image { url, detail } = parts.last().unwrap() else {
        panic!("expected final image")
    };
    assert!(detail.is_none());
    url.strip_prefix("data:image/jpeg;base64,")
        .expect("1 must become a JPEG data URL")
        .to_owned()
}
fn decoded_luma(
    message: &ContextMessage,
    role: ChatRole,
    text: Option<&str>,
    width: u16,
    height: u16,
) -> Vec<u8> {
    let jpeg = STANDARD.decode(jpeg_data(message, role, text)).unwrap();
    assert!(!jpeg.is_empty() && jpeg.len() <= IMAGE_CAP);
    assert!(jpeg.starts_with(b"\xff\xd8\xff"));
    assert!(jpeg.ends_with(b"\xff\xd9"));
    let expected = usize::from(width) * usize::from(height);
    let mut decoder = Decoder::new(Cursor::new(jpeg));
    decoder.set_max_decoding_buffer_size(expected);
    decoder.read_info().unwrap();
    let info = decoder.info().unwrap();
    assert_eq!((info.width, info.height), (width, height));
    assert_eq!(info.pixel_format, PixelFormat::L8);
    assert_eq!(info.coding_process, CodingProcess::DctSequential);
    let pixels = decoder.decode().unwrap();
    assert_eq!(pixels.len(), expected);
    pixels
}
fn asymmetric_pixels(message: &ContextMessage, role: ChatRole, text: Option<&str>) {
    // Sharp 17x3 bit patterns are checked by native-L equality, not lossless pixels.
    decoded_luma(message, role, text, 17, 3);
}
fn block_pixels(message: &ContextMessage, role: ChatRole, text: Option<&str>) {
    let pixels = decoded_luma(message, role, text, 49, 33);
    // Predeclared absolute interior tolerance <=8; never tune after execution.
    for (x, y, expected) in [
        (8, 8, 255),
        (24, 8, 0),
        (40, 8, 255),
        (8, 24, 0),
        (24, 24, 255),
        (40, 24, 0),
    ] {
        let actual = pixels[y * 49 + x];
        assert!(
            actual.abs_diff(expected) <= 8,
            "({x}, {y}): {actual} differs from {expected}, tolerance 8"
        );
    }
}
async fn native_l_controls(role: ChatRole) -> (String, String) {
    // Existing-L controls execute on old source before either new-mode witness.
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let odd = consume(&assistant, image_frame(literal_luma(), role));
    let block = consume(&assistant, image_frame(block_image(false), role));
    let snapshot = context.snapshot_rich();
    let ended = consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    ended.unwrap();
    cleaned.unwrap();
    silent(odd.unwrap());
    silent(block.unwrap());
    let rich = snapshot.unwrap();
    assert_eq!(rich.messages.len(), 2);
    let text = (role == ChatRole::User).then_some("inspect");
    asymmetric_pixels(&rich.messages[0], role, text);
    block_pixels(&rich.messages[1], role, text);
    (
        jpeg_data(&rich.messages[0], role, text),
        jpeg_data(&rich.messages[1], role, text),
    )
}
fn gemini_context(context: &LlmContextData, role: ChatRole, text: Option<&str>) {
    assert_eq!(context.messages.len(), 1);
    let data = jpeg_data(&context.messages[0], role, text);
    let native = pipecat::services::frame_context::context(context).unwrap();
    let body = request_body(Provider::Gemini, &CompletionRequest::new("fixture", native)).unwrap();
    let mut parts = vec![];
    if let Some(text) = text {
        parts.push(json!({"text":text}));
    }
    parts.push(json!({"inlineData":{"mimeType":"image/jpeg", "data":data}}));
    let expected = if role == ChatRole::Assistant {
        json!([{"role":"model", "parts":parts}, {"role":"user", "parts":[{"text":"."}]}])
    } else {
        json!([{"role":"user", "parts":parts}])
    };
    assert_eq!(body["contents"], expected);
}
#[tokio::test]
async fn direct_mono_without_originals_appends_decodable_assistant_jpeg() {
    let (odd_l, block_l) = native_l_controls(ChatRole::Assistant).await;
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = assistant.process_frame(assistant_image(), Direction::Downstream);
    let rich = context.snapshot_rich();
    let native = context.snapshot();
    let projected = context.frame_data();
    let block = consume(
        &assistant,
        image_frame(block_image(true), ChatRole::Assistant),
    );
    let blocks = context.snapshot_rich();
    let ended = consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    ended.unwrap();
    cleaned.unwrap();
    // Old unsupported boundary is asserted only after normal End and cleanup.
    silent(result.expect("exact 1 fallback must append an assistant JPEG"));
    let rich = rich.unwrap();
    assert_eq!(rich.messages.len(), 1);
    asymmetric_pixels(&rich.messages[0], ChatRole::Assistant, None);
    assert_eq!(
        jpeg_data(&rich.messages[0], ChatRole::Assistant, None),
        odd_l
    );
    silent(block.unwrap());
    let blocks = blocks.unwrap();
    assert_eq!(blocks.messages.len(), 2);
    block_pixels(&blocks.messages[1], ChatRole::Assistant, None);
    assert_eq!(
        jpeg_data(&blocks.messages[1], ChatRole::Assistant, None),
        block_l
    );
    let FrameData::LlmContextDetailed(projected) = projected.unwrap() else {
        panic!("expected rich projection")
    };
    assert_eq!(projected.context, rich);
    assert_eq!(
        native.unwrap(),
        pipecat::services::frame_context::context(&rich).unwrap()
    );
    gemini_context(&rich, ChatRole::Assistant, None);
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
        "mono-image-input-tap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.id == self.barrier_id {
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
async fn actual_input_mono_capture_prepares_upstream_decodable_user_context() {
    let (odd_l, _block_l) = native_l_controls(ChatRole::User).await;
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
    let input = Arc::new(InputTransport::new("mono-input", config, backend.clone(), None).unwrap());
    let ingress = input.ingress();
    let marker = Frame::new(FrameData::InputImage(mono_image()));
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
    // Input's callback acknowledges queue admission, never context consumption.
    request.result_callback = Some(LocalResultCallback::new(move |value| {
        log.lock().unwrap().push(value);
        async { Ok(()) }
    }));
    let key = VideoSourceKey::new(
        VideoParticipantId::new("peer").unwrap(),
        VideoSourceId::camera(),
    );
    let captured = Frame::new(FrameData::InputImage(mono_image()));
    let image_id = captured.id;
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
            .push_video_frame(&key, captured)
            .map_err(|e| e.to_string())?;
        ingress.push(marker).map_err(|e| e.to_string())?;
        timeout(WAIT, tap.barrier.notified())
            .await
            .map_err(|_| "input queue barrier deadline")?;
        Ok((ready, pending))
    }
    .await;
    // Admission or barrier failure still receives normal End, output drain and join.
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
    assert_eq!(image.image, mono_image());
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
    // The old unsupported consumer error is checked after owned production and cleanup.
    assert!(
        report.errors.is_empty(),
        "consumer rejected owned 1 image: {:?}",
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
    assert_eq!(emitted.context.messages.len(), 1);
    asymmetric_pixels(
        &emitted.context.messages[0],
        ChatRole::User,
        Some("inspect"),
    );
    assert_eq!(
        jpeg_data(
            &emitted.context.messages[0],
            ChatRole::User,
            Some("inspect")
        ),
        odd_l
    );
    gemini_context(&emitted.context, ChatRole::User, Some("inspect"));
}
// Candidate-only controls. The entire edition-2021 baseline prefix is unchanged.
fn one_context(output: AggregationOutput) -> DetailedLlmContextFrame {
    assert!(output.events.is_empty());
    assert_eq!(output.actions.len(), 1);
    let pipecat::turns::AggregationAction::Push(push) = &output.actions[0] else {
        panic!("expected one context push")
    };
    assert_eq!(push.direction, Direction::Upstream);
    let FrameData::LlmContextDetailed(context) = &push.frame.data else {
        panic!("expected rich context")
    };
    assert!(!context.speculation);
    context.clone()
}

fn user_image(run: Option<bool>) -> Frame {
    Frame::new(FrameData::UserImage(UserImageRawFrame {
        image: mono_image(),
        user_id: "peer".into(),
        text: Some("inspect".into()),
        append_to_context: Some(true),
        request: Some(Box::new(plain_request())),
        run_llm: run,
    }))
}
fn raw_image(frame: &mut Frame) -> &mut ImageRawFrame {
    match &mut frame.data {
        FrameData::UserImage(image) => &mut image.image,
        FrameData::AssistantImage(image) => &mut image.image.image,
        _ => panic!("expected image frame"),
    }
}
fn user_payload(frame: &mut Frame) -> &mut UserImageRawFrame {
    let FrameData::UserImage(image) = &mut frame.data else {
        panic!("expected UserImage")
    };
    image
}
fn assistant_payload(frame: &mut Frame) -> &mut AssistantImageRawFrame {
    let FrameData::AssistantImage(image) = &mut frame.data else {
        panic!("expected AssistantImage")
    };
    image
}
fn frame_for(role: ChatRole) -> Frame {
    if role == ChatRole::User {
        user_image(Some(false))
    } else {
        assistant_image()
    }
}
fn text_for(role: ChatRole) -> Option<&'static str> {
    (role == ChatRole::User).then_some("inspect")
}
async fn admitted(frame: Frame) -> LlmContextData {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = consume(&assistant, frame);
    let snapshot = context.snapshot_rich();
    let ended = consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    ended.unwrap();
    cleaned.unwrap();
    silent(result.unwrap());
    let snapshot = snapshot.unwrap();
    assert_eq!(snapshot.messages.len(), 1);
    snapshot
}

#[tokio::test]
async fn direct_user_mono_true_none_and_false_preserve_content_and_run_policy() {
    for run in [Some(true), None, Some(false)] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut image = user_image(run);
        // The request associates capture only; frame content and flags are authoritative.
        let request = user_payload(&mut image).request.as_mut().unwrap();
        request.text = Some("ignored request text".into());
        request.append_to_context = Some(false);
        let result = consume(&assistant, image);
        let snapshot = context.snapshot_rich();
        let ended = consume(&assistant, Frame::end());
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        ended.unwrap();
        cleaned.unwrap();
        let snapshot = snapshot.unwrap();
        assert_eq!(snapshot.messages.len(), 1);
        if run == Some(false) {
            silent(result.unwrap());
        } else {
            assert_eq!(one_context(result.unwrap()).context, snapshot);
        }
        asymmetric_pixels(&snapshot.messages[0], ChatRole::User, Some("inspect"));
        gemini_context(&snapshot, ChatRole::User, Some("inspect"));
    }
}

#[tokio::test]
async fn mono_mode_is_exact_and_rejections_do_not_mutate_history() {
    for role in [ChatRole::User, ChatRole::Assistant] {
        for mode in [
            None,
            Some(""),
            Some("1;I"),
            Some("1;R"),
            Some("1;IR"),
            Some("1;8"),
            Some("01"),
            Some(" 1"),
            Some("1 "),
            Some("1\0"),
            Some("1\n"),
            Some("L8"),
            Some("LA"),
            Some("P"),
            Some("I;16"),
            Some("image/png"),
        ] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let mut frame = frame_for(role);
            raw_image(&mut frame).format = mode.map(str::to_owned);
            let result = consume(&assistant, frame);
            let snapshot = context.snapshot_rich();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            let error = result.unwrap_err();
            if role == ChatRole::Assistant {
                assert!(error.message.starts_with("assistant image"));
            } else {
                assert!(
                    error.message.contains("raw image/file")
                        || error.message.starts_with("user image")
                );
            }
            assert!(snapshot.unwrap().messages.is_empty());
        }
    }
}

async fn rejected_then_repaired(role: ChatRole, malformed: ImageRawFrame, diagnostic: &str) {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let good = frame_for(role);
    let mut bad = good.clone();
    *raw_image(&mut bad) = malformed;
    let rejected = consume(&assistant, bad);
    let before = context.snapshot_rich();
    let repaired = consume(&assistant, good);
    let after = context.snapshot_rich();
    let ended = consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    ended.unwrap();
    cleaned.unwrap();
    let owner = if role == ChatRole::User {
        "user image 1"
    } else {
        "assistant image 1"
    };
    assert_eq!(
        rejected.unwrap_err().message,
        format!("{owner} {diagnostic}")
    );
    assert!(before.unwrap().messages.is_empty());
    silent(repaired.unwrap());
    let after = after.unwrap();
    assert_eq!(after.messages.len(), 1);
    asymmetric_pixels(&after.messages[0], role, text_for(role));
}

#[tokio::test]
async fn mono_geometry_short_and_expanded_caps_are_atomic_and_repairable() {
    for role in [ChatRole::User, ChatRole::Assistant] {
        for (size, diagnostic) in [
            ((0, 1), "dimensions must be 1..=65500"),
            ((1, 0), "dimensions must be 1..=65500"),
            ((65_501, 1), "dimensions must be 1..=65500"),
            ((1, 65_501), "dimensions must be 1..=65500"),
            ((u32::MAX, u32::MAX), "dimensions must be 1..=65500"),
            ((65_500, 65_500), "expanded pixel bytes exceed 8388608"),
        ] {
            let mut image = mono_image();
            image.size = size;
            rejected_then_repaired(role, image, diagnostic).await;
        }
        let mut empty = mono_image();
        empty.image.clear();
        rejected_then_repaired(role, empty, "buffer is shorter than its dimensions").await;
        let mut short = mono_image();
        short.image.pop();
        rejected_then_repaired(role, short, "buffer is shorter than its dimensions").await;
        // 27 continuously packed bits occupy four bytes; three rounded rows need six.
        let continuous = ImageRawFrame {
            image: vec![0xa1, 0x8b, 0x64, 0x80],
            size: (9, 3),
            format: Some("1".into()),
        };
        rejected_then_repaired(role, continuous, "buffer is shorter than its dimensions").await;
        let mut over = mono_image();
        over.image.resize(IMAGE_CAP + 1, 0);
        rejected_then_repaired(role, over, "buffer exceeds 8388608 bytes").await;
        // Packed/full-buffer limits allow this input; the distinct expansion limit rejects.
        let over_expansion = ImageRawFrame {
            image: vec![0; 512 * 2049],
            size: (4096, 2049),
            format: Some("1".into()),
        };
        rejected_then_repaired(role, over_expansion, "expanded pixel bytes exceed 8388608").await;
    }
}

fn literal_widths() -> Vec<(u32, Vec<u8>, Vec<u8>)> {
    // Every expected row below is hand-written; no bit-index formula is shared.
    vec![
        (1, vec![0x80, 0x00, 0x80], vec![255, 0, 255]),
        (
            7,
            vec![0xa2, 0x16, 0xc8],
            vec![
                255, 0, 255, 0, 0, 0, 255, 0, 0, 0, 255, 0, 255, 255, 255, 255, 0, 0, 255, 0, 0,
            ],
        ),
        (
            8,
            vec![0xa1, 0x16, 0xc9],
            vec![
                255, 0, 255, 0, 0, 0, 0, 255, 0, 0, 0, 255, 0, 255, 255, 0, 255, 255, 0, 0, 255, 0,
                0, 255,
            ],
        ),
        (
            9,
            vec![0xa1, 0x80, 0x16, 0x00, 0xc8, 0x80],
            vec![
                255, 0, 255, 0, 0, 0, 0, 255, 255, 0, 0, 0, 255, 0, 255, 255, 0, 0, 255, 255, 0, 0,
                255, 0, 0, 0, 255,
            ],
        ),
        (17, mono_image().image, literal_luma().image),
    ]
}

#[tokio::test]
async fn mono_msb_first_rounded_rows_match_literal_native_l_at_five_widths() {
    for role in [ChatRole::User, ChatRole::Assistant] {
        for (width, packed, expected) in literal_widths() {
            assert_eq!(expected.len(), width as usize * 3);
            let mono = admitted(image_frame(
                ImageRawFrame {
                    image: packed,
                    size: (width, 3),
                    format: Some("1".into()),
                },
                role,
            ))
            .await;
            let luma = admitted(image_frame(
                ImageRawFrame {
                    image: expected,
                    size: (width, 3),
                    format: Some("L".into()),
                },
                role,
            ))
            .await;
            assert_eq!(
                mono, luma,
                "native L equality at width {width}; not Pillow equality"
            );
            decoded_luma(&mono.messages[0], role, text_for(role), width as u16, 3);
        }
        let mono = admitted(image_frame(block_image(true), role)).await;
        let luma = admitted(image_frame(block_image(false), role)).await;
        assert_eq!(mono, luma);
        block_pixels(&mono.messages[0], role, text_for(role));
    }
}

#[tokio::test]
async fn mono_unused_low_bits_do_not_change_rows_but_significant_bits_do() {
    for role in [ChatRole::User, ChatRole::Assistant] {
        // Literal per-row final-byte positions and unused-bit masks, independent
        // of the production row-byte computation. Width 8 has no unused bits.
        for (width, last, mask) in [
            (1, vec![0, 1, 2], 0x7f),
            (7, vec![0, 1, 2], 0x01),
            (9, vec![1, 3, 5], 0x7f),
            (17, vec![2, 5, 8], 0x7f),
        ] {
            let (_, packed, _) = literal_widths().into_iter().find(|f| f.0 == width).unwrap();
            let image = ImageRawFrame {
                image: packed,
                size: (width, 3),
                format: Some("1".into()),
            };
            let exact = admitted(image_frame(image.clone(), role)).await;
            let mut unused = image.clone();
            for offset in last {
                unused.image[offset] ^= mask;
            }
            assert_eq!(admitted(image_frame(unused, role)).await, exact);
            let mut significant = image;
            significant.image[0] ^= 0x80;
            assert_ne!(admitted(image_frame(significant, role)).await, exact);
        }
        let mut byte_aligned = mono_image();
        byte_aligned.size = (8, 3);
        byte_aligned.image = vec![0xa1, 0x16, 0xc9];
        let exact = admitted(image_frame(byte_aligned.clone(), role)).await;
        byte_aligned.image[1] ^= 0x01;
        assert_ne!(admitted(image_frame(byte_aligned, role)).await, exact);
    }
}

#[tokio::test]
async fn mono_bounded_trailing_bytes_include_exact_full_buffer_cap() {
    for role in [ChatRole::User, ChatRole::Assistant] {
        let plain = admitted(frame_for(role)).await;
        for length in [16, IMAGE_CAP - 1, IMAGE_CAP] {
            let mut image = mono_image();
            image.image.resize(length, 251);
            let tailed = admitted(image_frame(image, role)).await;
            assert_eq!(
                tailed, plain,
                "same native packed prefix must preserve JPEG bytes"
            );
            drop(tailed);
        }
    }
}

#[tokio::test]
async fn mono_exact_expanded_cap_and_dimension_edges_decode_as_l8() {
    // Keep large fixtures sequential; do not retain decoded or input buffers.
    // 4096x2048 expands to exactly 8 MiB from only 1 MiB of packed bytes.
    for role in [ChatRole::User, ChatRole::Assistant] {
        for (width, height, packed_bytes) in [
            (1_u16, 1_u16, 1),
            (65_500, 1, 8188),
            (1, 65_500, 65_500),
            (4096, 2048, 1_048_576),
        ] {
            let image = ImageRawFrame {
                image: vec![255; packed_bytes],
                size: (u32::from(width), u32::from(height)),
                format: Some("1".into()),
            };
            let context = admitted(image_frame(image, role)).await;
            let pixels = decoded_luma(&context.messages[0], role, text_for(role), width, height);
            assert!(pixels.iter().all(|pixel| pixel.abs_diff(255) <= 8));
            drop(pixels);
            drop(context);
        }
    }
}

#[tokio::test]
async fn mono_replay_order_and_buffered_text_keep_distinct_user_assistant_rules() {
    use pipecat::turns::AggregationEvent;
    for role in [ChatRole::User, ChatRole::Assistant] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let prefix = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm("before"))),
        );
        let good = frame_for(role);
        let mut bad = good.clone();
        raw_image(&mut bad).image.pop();
        let invalid = consume(&assistant, bad.clone());
        let before = context.snapshot_rich();
        let repaired = consume(&assistant, good.clone());
        let replay = consume(&assistant, good);
        let invalid_replay = consume(&assistant, bad);
        let middle = context.snapshot_rich();
        let suffix = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm(" after"))),
        );
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
        silent(prefix.unwrap());
        silent(suffix.unwrap());
        assert!(invalid.is_err());
        assert!(before.unwrap().messages.is_empty());
        silent(repaired.unwrap());
        silent(replay.unwrap());
        if role == ChatRole::User {
            assert!(invalid_replay.is_err(), "User validates before seen");
        } else {
            silent(invalid_replay.unwrap());
        }
        let middle = middle.unwrap();
        if role == ChatRole::User {
            assert_eq!(middle.messages.len(), 2);
            assert_eq!(middle.messages[0].role, ChatRole::Assistant);
            assert_eq!(
                middle.messages[0].content,
                MessageContent::Text("before".into())
            );
        } else {
            assert_eq!(
                middle.messages.len(),
                1,
                "Assistant image keeps prefix buffered"
            );
        }
        asymmetric_pixels(middle.messages.last().unwrap(), role, text_for(role));
        let expected_text = if role == ChatRole::User {
            "after"
        } else {
            "before after"
        };
        assert_eq!(
            end.unwrap().events,
            vec![AggregationEvent::AssistantTurnStopped {
                text: expected_text.into(),
                interrupted: false
            }]
        );
        let after = after.unwrap();
        assert_eq!(after.messages.len(), middle.messages.len() + 1);
        assert_eq!(&after.messages[..middle.messages.len()], &middle.messages);
        assert_eq!(
            after.messages.last().unwrap().content,
            MessageContent::Text(expected_text.into())
        );
    }
}

#[tokio::test]
async fn mono_assistant_original_priority_truthiness_and_failed_original_no_rescue() {
    let png = b"\x89PNG\r\n\x1a\noriginal";
    for data in [None, Some(vec![]), Some(png.to_vec())] {
        for mime in [None, Some(""), Some("image/png")] {
            let originals_win = data.as_ref().is_some_and(|data| !data.is_empty())
                && mime.is_some_and(|mime| !mime.is_empty());
            let mut frame = assistant_image();
            let image = assistant_payload(&mut frame);
            image.original_data = data.clone();
            image.original_mime_type = mime.map(str::to_owned);
            if originals_win {
                image.image.image.image.clear();
                image.image.image.size = (0, u32::MAX);
            }
            let rich = admitted(frame).await;
            if originals_win {
                assert_eq!(
                    rich.messages[0].content,
                    MessageContent::Parts(vec![MessagePart::Image {
                        url: format!("data:image/png;base64,{}", STANDARD.encode(png)),
                        detail: None
                    }])
                );
            } else {
                asymmetric_pixels(&rich.messages[0], ChatRole::Assistant, None);
            }
        }
    }
    let mut ignored = assistant_image();
    assistant_payload(&mut ignored).original_data = Some(vec![17; IMAGE_CAP + 1]);
    asymmetric_pixels(
        &admitted(ignored).await.messages[0],
        ChatRole::Assistant,
        None,
    );
    for (data, mime, diagnostic) in [
        (
            vec![1, 2, 3],
            "image/png",
            "requires image/png or image/jpeg MIME",
        ),
        (png.to_vec(), " ", "requires image/png or image/jpeg MIME"),
        (
            png.to_vec(),
            "image/jpeg",
            "requires image/png or image/jpeg MIME",
        ),
        (png.to_vec(), "1", "requires image/png or image/jpeg MIME"),
        (
            vec![17; IMAGE_CAP + 1],
            "image/png",
            "original_data must contain 1..=8388608 bytes",
        ),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let good = assistant_image();
        let mut bad = good.clone();
        assistant_payload(&mut bad).original_data = Some(data);
        assistant_payload(&mut bad).original_mime_type = Some(mime.into());
        let result = consume(&assistant, bad.clone());
        let before = context.snapshot_rich();
        let repaired = consume(&assistant, good);
        let replay = consume(&assistant, bad);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(result
            .unwrap_err()
            .message
            .contains(&format!("assistant image {diagnostic}")));
        assert!(before.unwrap().messages.is_empty());
        silent(repaired.unwrap());
        silent(replay.unwrap());
        let after = after.unwrap();
        assert_eq!(after.messages.len(), 1);
        asymmetric_pixels(&after.messages[0], ChatRole::Assistant, None);
    }
}

#[tokio::test]
async fn mono_passive_flags_skip_pixels_without_consuming_correctable_id() {
    for append in [None, Some(false)] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut frame = user_image(None);
        user_payload(&mut frame).append_to_context = append;
        raw_image(&mut frame).image.clear();
        raw_image(&mut frame).size = (0, u32::MAX);
        let passive = consume(&assistant, frame.clone());
        let before = context.snapshot_rich();
        user_payload(&mut frame).append_to_context = Some(true);
        *raw_image(&mut frame) = mono_image();
        let retry = consume(&assistant, frame);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(passive.unwrap());
        assert!(before.unwrap().messages.is_empty());
        let after = after.unwrap();
        assert_eq!(one_context(retry.unwrap()).context, after);
        asymmetric_pixels(&after.messages[0], ChatRole::User, Some("inspect"));
    }
}

#[tokio::test]
async fn mono_user_request_authority_and_busy_inference_stay_rejected() {
    for authority in 0_usize..3 {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let log = calls.clone();
        let good = user_image(Some(false));
        let mut frame = good.clone();
        let request = user_payload(&mut frame).request.as_mut().unwrap();
        match authority {
            0 => request.function_name = Some(String::new()),
            1 => request.tool_call_id = Some("unknown".into()),
            _ => {
                request.result_callback = Some(LocalResultCallback::new(move |_| {
                    log.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                }))
            }
        }
        let rejected = consume(&assistant, frame.clone());
        let before = context.snapshot_rich();
        let admitted = consume(&assistant, good);
        let accepted = context.snapshot_rich();
        let rejected_replay = consume(&assistant, frame);
        let after = context.snapshot_rich();
        let pending = assistant.has_function_calls_in_progress();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected.unwrap_err().message.contains("raw image/file"));
        assert!(rejected_replay
            .unwrap_err()
            .message
            .contains("raw image/file"));
        assert!(before.unwrap().messages.is_empty());
        silent(admitted.unwrap());
        assert_eq!(accepted.unwrap(), after.unwrap());
        assert!(!pending.unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    for run in [Some(true), None] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let prefix = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm("held"))),
        );
        let frame = user_image(run);
        let rejected = consume(&assistant, frame.clone());
        let before = context.snapshot_rich();
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let repaired = consume(&assistant, frame);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
        silent(prefix.unwrap());
        assert!(rejected.unwrap_err().message.contains("idle assistant"));
        assert!(before.unwrap().messages.is_empty());
        end.unwrap();
        let after = after.unwrap();
        assert_eq!(one_context(repaired.unwrap()).context, after);
        assert_eq!(after.messages.len(), 2);
        assert_eq!(
            after.messages[0].content,
            MessageContent::Text("held".into())
        );
        asymmetric_pixels(&after.messages[1], ChatRole::User, Some("inspect"));
    }
}

fn private_carriers(frame: &Frame) -> [Frame; 2] {
    let mut continuation = Frame::new(FrameData::LlmResponseStart);
    continuation
        .set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::start(982330, LlmContinuationProvider::Gemini).unwrap(),
        ))
        .unwrap();
    let call = FunctionCall {
        function_name: "capture".into(),
        tool_call_id: "owned".into(),
        arguments: json!({}),
    };
    let mut tool = Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()]));
    tool.set_native_tool_batch(
        NativeToolCallBatch::new("mono-owned".into(), vec![(Arc::new(call), false)]).unwrap(),
    )
    .unwrap();
    for carrier in [&mut continuation, &mut tool] {
        carrier.id = frame.id;
        carrier.data = frame.data.clone();
    }
    [continuation, tool]
}
#[tokio::test]
async fn mono_private_signed_realtime_and_terminal_guards_keep_existing_order() {
    use pipecat::{
        services::Context,
        turns::{LLMContextAggregatorPair, RealtimeServiceMode},
    };
    for role in [ChatRole::User, ChatRole::Assistant] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let good = frame_for(role);
        let carriers = private_carriers(&good);
        let admitted = consume(&assistant, good);
        let before = context.snapshot_rich();
        let private = carriers
            .into_iter()
            .map(|frame| consume(&assistant, frame))
            .collect::<Vec<_>>();
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(admitted.unwrap());
        assert!(private.into_iter().all(|result| result.is_err()));
        assert_eq!(before.unwrap(), after.unwrap());

        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut start = Frame::new(FrameData::LlmResponseStart);
        start
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(982331, LlmContinuationProvider::Gemini).unwrap(),
            ))
            .unwrap();
        let start = consume(&assistant, start);
        let rejected = consume(&assistant, frame_for(role));
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
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
        let rejected = consume(&pair.assistant(), frame_for(role));
        let after = pair.context().snapshot_rich();
        let assistant_cleaned = pair.assistant().cleanup().await;
        let user_cleaned = pair.user().cleanup().await;
        drop(pair);
        assistant_cleaned.unwrap();
        user_cleaned.unwrap();
        assert!(rejected.unwrap_err().message.contains("realtime"));
        assert!(after.unwrap().messages.is_empty());

        for terminal in [
            Frame::end(),
            Frame::cancel("test"),
            Frame::new(FrameData::Stop),
        ] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let good = frame_for(role);
            let [continuation, tool] = private_carriers(&good);
            let mut bad = good.clone();
            raw_image(&mut bad).image.clear();
            let end = consume(&assistant, terminal);
            let valid = consume(&assistant, good);
            let invalid = consume(&assistant, bad);
            let private = consume(&assistant, continuation);
            let tool = consume(&assistant, tool);
            let after = context.snapshot_rich();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            end.unwrap();
            silent(valid.unwrap());
            silent(invalid.unwrap());
            silent(private.unwrap());
            assert!(tool.is_err());
            assert!(after.unwrap().messages.is_empty());
        }
    }
}

#[tokio::test]
async fn mono_user_projection_failure_keeps_history_and_success_id_retryable() {
    use pipecat::services::{Content, ContentPart, Context, Message, Role};
    for run in [Some(true), None, Some(false)] {
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
        let frame = user_image(run);
        let result = consume(&assistant, frame.clone());
        let before = context.snapshot();
        let repaired = if run == Some(false) {
            None
        } else {
            Some(context.replace(Context::default()))
        };
        let retry = if run == Some(false) {
            None
        } else {
            Some(consume(&assistant, frame.clone()))
        };
        let replay = consume(&assistant, frame);
        let native = context.snapshot();
        let rich = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        if run == Some(false) {
            silent(result.unwrap());
            let before = before.unwrap();
            assert_eq!(before.messages.len(), 2);
            assert_eq!(before.messages[0], prefix);
            assert_eq!(before.messages[1].role, Role::User);
            assert_eq!(native.unwrap(), before);
            assert!(rich.is_err());
        } else {
            assert!(result.unwrap_err().message.contains("InlineData"));
            assert_eq!(before.unwrap(), Context::new(vec![prefix]));
            repaired.unwrap().unwrap();
            let rich = rich.unwrap();
            assert_eq!(one_context(retry.unwrap().unwrap()).context, rich);
            asymmetric_pixels(&rich.messages[0], ChatRole::User, Some("inspect"));
        }
        silent(replay.unwrap());
    }
}
