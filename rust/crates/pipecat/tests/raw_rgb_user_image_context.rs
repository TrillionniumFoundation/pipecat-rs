//! Bounded RGB user-image context and real InputTransport-owned production.
//! Application-supplied RGB24 pixels; no camera hardware or provider I/O.
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
fn rgb_image() -> ImageRawFrame {
    // Asymmetric 48x32 image: 16x16 MCU-aligned red/green/blue above
    // white/black/gray, providing independent orientation and color samples.
    let colors = [
        [240, 16, 16],
        [16, 240, 16],
        [16, 16, 240],
        [240, 240, 240],
        [16, 16, 16],
        [128, 128, 128],
    ];
    let mut image = Vec::with_capacity(48 * 32 * 3);
    for y in 0..32 {
        for x in 0..48 {
            image.extend_from_slice(&colors[(y / 16) * 3 + x / 16]);
        }
    }
    ImageRawFrame {
        image,
        size: (48, 32),
        format: Some("RGB".into()),
    }
}
fn direct_image(run: Option<bool>) -> Frame {
    Frame::new(FrameData::UserImage(UserImageRawFrame {
        image: rgb_image(),
        user_id: "peer".into(),
        text: Some("inspect".into()),
        append_to_context: Some(true),
        request: Some(Box::new(plain_request())),
        run_llm: run,
    }))
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
fn assert_jpeg_context(context: &LlmContextData) -> &str {
    assert_eq!(context.messages.len(), 1);
    let message = &context.messages[0];
    assert_eq!(message.role, ChatRole::User);
    assert!(message.name.is_none());
    assert!(message.tool_call_id.is_none());
    assert!(message.tool_calls.is_empty());
    assert!(message.extra.is_empty());
    let MessageContent::Parts(parts) = &message.content else {
        panic!("expected one user text/image message")
    };
    assert_eq!(parts.len(), 2);
    assert_eq!(
        parts[0],
        MessagePart::Text {
            text: "inspect".into()
        }
    );
    let MessagePart::Image { url, detail } = &parts[1] else {
        panic!("expected exactly one image after text")
    };
    assert!(detail.is_none());
    let data = url
        .strip_prefix("data:image/jpeg;base64,")
        .expect("RGB must become a JPEG data URL");
    assert!(!data.is_empty());
    let native = pipecat::services::frame_context::context(context).unwrap();
    let body = request_body(Provider::Gemini, &CompletionRequest::new("fixture", native)).unwrap();
    assert_eq!(
        body["contents"],
        json!([
            {"role":"user", "parts":[{"text":"inspect"},
                {"inlineData":{"mimeType":"image/jpeg", "data":data}}]}
        ])
    );
    data
}

#[tokio::test]
async fn direct_rgb_true_and_none_prepare_one_jpeg_context() {
    // Hand-constructed content only: this does not attest InputTransport ownership.
    for run in [Some(true), None] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let result = assistant.process_at(direct_image(run), Direction::Downstream, Duration::ZERO);
        let snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        let output = one_context(result.expect("RGB user image must prepare JPEG context"));
        assert_eq!(output.context, snapshot.unwrap());
        assert_jpeg_context(&output.context);
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
        // The fixture backend owns an application-provided RGB24 source.
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
        "rgb-image-input-tap"
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

async fn owned_rgb_context() -> LlmContextData {
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
    let input = Arc::new(InputTransport::new("rgb-input", config, backend.clone(), None).unwrap());
    let ingress = input.ingress();
    let marker = Frame::new(FrameData::InputImage(rgb_image()));
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
    let captured = Frame::new(FrameData::InputImage(rgb_image()));
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
    assert_eq!(image.image, rgb_image());
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
        "consumer rejected owned RGB image: {:?}",
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
    emitted.context.clone()
}

#[tokio::test]
async fn actual_input_rgb_capture_prepares_context_and_same_gemini_jpeg() {
    let context = owned_rgb_context().await;
    assert_jpeg_context(&context);
}
// Candidate controls. The baseline above has edition-2021 formatting only;
// the two tests and the Input lifecycle retain their original behavior.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use jpeg_decoder::{CodingProcess, Decoder, PixelFormat};
use pipecat::turns::{AssistantAggregatorConfig, AssistantTextMode};
use std::io::Cursor;

const RGB_CAP: usize = 8 * 1024 * 1024;
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
fn decoded_rgb(context: &LlmContextData, width: u16, height: u16) -> Vec<u8> {
    let encoded = STANDARD.decode(assert_jpeg_context(context)).unwrap();
    assert!(encoded.len() <= RGB_CAP);
    assert!(encoded.starts_with(b"\xff\xd8\xff"));
    assert!(encoded.ends_with(b"\xff\xd9"));
    let expected = usize::from(width) * usize::from(height) * 3;
    let mut decoder = Decoder::new(Cursor::new(encoded));
    decoder.set_max_decoding_buffer_size(expected);
    decoder.read_info().unwrap();
    let info = decoder.info().unwrap();
    assert_eq!((info.width, info.height), (width, height));
    assert_eq!(info.pixel_format, PixelFormat::RGB24);
    assert_eq!(info.coding_process, CodingProcess::DctSequential);
    let pixels = decoder.decode().unwrap();
    assert_eq!(pixels.len(), expected);
    pixels
}
fn close_pixel(actual: &[u8], expected: [u8; 3]) {
    // Fixture-specific interior tolerance; not a promise for arbitrary JPEGs.
    assert_eq!(actual.len(), 3);
    for (actual, expected) in actual.iter().zip(expected) {
        assert!(
            actual.abs_diff(expected) <= 12,
            "{actual} differs from {expected}"
        );
    }
}
fn asymmetric_pixels(context: &LlmContextData) {
    let pixels = decoded_rgb(context, 48, 32);
    for (x, y, rgb) in [
        (8, 8, [240, 16, 16]),
        (24, 8, [16, 240, 16]),
        (40, 8, [16, 16, 240]),
        (8, 24, [240, 240, 240]),
        (24, 24, [16, 16, 16]),
        (40, 24, [128, 128, 128]),
    ] {
        let offset = (y * 48 + x) * 3;
        close_pixel(&pixels[offset..offset + 3], rgb);
    }
}
async fn admitted_context(frame: Frame) -> LlmContextData {
    let run = match &frame.data {
        FrameData::UserImage(image) => image.run_llm,
        _ => panic!("not a user image"),
    };
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = consume(&assistant, frame);
    let snapshot = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    let snapshot = snapshot.unwrap();
    if run == Some(false) {
        silent(result.unwrap());
    } else {
        assert_eq!(one_context(result.unwrap()).context, snapshot);
    }
    snapshot
}

#[tokio::test]
async fn actual_input_jpeg_decodes_with_exact_dimensions_and_lossy_orientation() {
    let context = owned_rgb_context().await;
    asymmetric_pixels(&context);
}

#[tokio::test]
async fn odd_non_square_rgb_and_largest_side_have_independently_decoded_dimensions() {
    for (width, height) in [(17, 9), (65_500, 1)] {
        let mut frame = direct_image(Some(false));
        payload(&mut frame).image = ImageRawFrame {
            image: [32, 96, 176].repeat(width * height),
            size: (width as u32, height as u32),
            format: Some("RGB".into()),
        };
        let context = admitted_context(frame).await;
        let pixels = decoded_rgb(&context, width as u16, height as u16);
        for pixel in pixels.as_chunks::<3>().0 {
            close_pixel(pixel, [32, 96, 176]);
        }
    }
}

#[tokio::test]
async fn bounded_surplus_including_full_buffer_cap_encodes_only_required_prefix() {
    let exact = admitted_context(direct_image(Some(false))).await;
    asymmetric_pixels(&exact);
    for length in [48 * 32 * 3 + 7, RGB_CAP] {
        let mut frame = direct_image(Some(false));
        payload(&mut frame).image.image.resize(length, 219);
        let surplus = admitted_context(frame).await;
        assert_eq!(surplus, exact, "bounded surplus must not alter the JPEG");
    }
}

#[tokio::test]
async fn rgb_invalid_dimensions_bytes_and_modes_are_atomic_and_same_id_repairs() {
    let good_image = rgb_image();
    let mut cases = vec![];
    for size in [
        (0, 1),
        (1, 0),
        (65_501, 1),
        (1, 65_501),
        (u32::MAX, u32::MAX),
        (65_500, 65_500),
        (2048, 1366),
    ] {
        let mut image = good_image.clone();
        image.size = size;
        cases.push(image);
    }
    let mut empty = good_image.clone();
    empty.image.clear();
    cases.push(empty);
    let mut short = good_image.clone();
    short.image.pop();
    cases.push(short);
    let mut overcap = good_image.clone();
    overcap.image.resize(RGB_CAP + 1, 0);
    cases.push(overcap);
    for mode in [None, Some("rgb"), Some("RGBA"), Some("BGR"), Some("RGBX")] {
        let mut image = good_image.clone();
        image.format = mode.map(str::to_owned);
        cases.push(image);
    }
    for bad_image in cases {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let good = direct_image(Some(false));
        let mut bad = good.clone();
        payload(&mut bad).image = bad_image;
        let invalid = consume(&assistant, bad.clone());
        let before = context.snapshot_rich();
        let repaired = consume(&assistant, good.clone());
        let replay = consume(&assistant, good);
        let malformed_replay = consume(&assistant, bad);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(invalid.is_err());
        assert!(before.unwrap().messages.is_empty());
        silent(repaired.unwrap());
        silent(replay.unwrap());
        assert!(
            malformed_replay.is_err(),
            "validation must still precede replay lookup"
        );
        asymmetric_pixels(&after.unwrap());
    }
}

#[tokio::test]
async fn passive_rgb_skips_encoding_without_consuming_repairable_id() {
    for append in [None, Some(false)] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut image = direct_image(None);
        payload(&mut image).append_to_context = append;
        payload(&mut image).image.size = (0, u32::MAX);
        payload(&mut image).image.image.clear();
        let passive = consume(&assistant, image.clone());
        let before = context.snapshot_rich();
        payload(&mut image).append_to_context = Some(true);
        payload(&mut image).image = rgb_image();
        let retry = consume(&assistant, image);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(passive.unwrap());
        assert!(before.unwrap().messages.is_empty());
        let after = after.unwrap();
        assert_eq!(one_context(retry.unwrap()).context, after);
        asymmetric_pixels(&after);
    }
}

#[tokio::test]
async fn rgb_false_run_keeps_failed_prefix_then_commits_it_once_on_same_id_repair() {
    use pipecat::turns::AggregationEvent;
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let prefix = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm("before"))),
    );
    let good = direct_image(Some(false));
    let mut bad = good.clone();
    payload(&mut bad).image.image.pop();
    let rejected = consume(&assistant, bad);
    let unchanged = context.snapshot_rich();
    let repaired = consume(&assistant, good.clone());
    let replay = consume(&assistant, good);
    let middle = context.snapshot_rich();
    let suffix = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm(" after"))),
    );
    let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let final_context = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    start.unwrap();
    prefix.unwrap();
    suffix.unwrap();
    assert!(rejected.is_err());
    assert!(unchanged.unwrap().messages.is_empty());
    silent(repaired.unwrap());
    silent(replay.unwrap());
    let middle = middle.unwrap();
    assert_eq!(middle.messages.len(), 2);
    assert_eq!(middle.messages[0].role, ChatRole::Assistant);
    assert_eq!(
        middle.messages[0].content,
        MessageContent::Text("before".into())
    );
    let mut image_only = middle.clone();
    image_only.messages.remove(0);
    asymmetric_pixels(&image_only);
    assert_eq!(
        end.unwrap().events,
        vec![AggregationEvent::AssistantTurnStopped {
            text: "after".into(),
            interrupted: false
        }]
    );
    let final_context = final_context.unwrap();
    assert_eq!(&final_context.messages[..2], &middle.messages);
    assert_eq!(final_context.messages.len(), 3);
    assert_eq!(
        final_context.messages[2].content,
        MessageContent::Text("after".into())
    );
}

#[tokio::test]
async fn rgb_inference_projection_failure_keeps_history_and_replay_id_retryable() {
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
        let image = direct_image(run);
        let result = consume(&assistant, image.clone());
        let retained = context.snapshot();
        let repair = if run == Some(false) {
            None
        } else {
            Some(context.replace(Context::default()))
        };
        let retry = if run == Some(false) {
            None
        } else {
            Some(consume(&assistant, image.clone()))
        };
        let replay = consume(&assistant, image);
        let final_native = context.snapshot();
        let final_rich = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        if run == Some(false) {
            silent(result.unwrap());
            let retained = retained.unwrap();
            assert_eq!(retained.messages.len(), 2);
            assert_eq!(retained.messages[0], prefix);
            assert_eq!(retained.messages[1].role, Role::User);
            assert_eq!(final_native.unwrap(), retained);
            assert!(final_rich.is_err());
        } else {
            assert!(result.unwrap_err().message.contains("InlineData"));
            assert_eq!(retained.unwrap(), Context::new(vec![prefix]));
            repair.unwrap().unwrap();
            let rich = final_rich.unwrap();
            assert_eq!(one_context(retry.unwrap().unwrap()).context, rich);
            asymmetric_pixels(&rich);
        }
        silent(replay.unwrap());
    }
}
#[tokio::test]
async fn rgb_request_authority_rejects_without_callback_or_tool_settlement() {
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
async fn rgb_preserves_private_carrier_realtime_and_signed_owner_guards() {
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
async fn rgb_busy_inference_preserves_buffered_text_and_normal_turn_end() {
    for (buffered, run) in [
        (false, Some(true)),
        (true, Some(true)),
        (false, None),
        (true, None),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let text = buffered.then(|| {
            consume(
                &assistant,
                Frame::new(FrameData::LlmText(TextFrame::llm("before"))),
            )
        });
        let mut frame = direct_image(Some(false));
        if let FrameData::UserImage(image) = &mut frame.data {
            image.run_llm = run;
        }
        let rejected = consume(&assistant, frame.clone());
        let unchanged = context.snapshot_rich();
        let suffix = buffered.then(|| {
            consume(
                &assistant,
                Frame::new(FrameData::LlmText(TextFrame::llm(" after"))),
            )
        });
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        if let FrameData::UserImage(image) = &mut frame.data {
            image.run_llm = Some(false);
        }
        let retry = consume(&assistant, frame);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
        if let Some(text) = text {
            silent(text.unwrap());
        }
        assert!(rejected.unwrap_err().message.contains("idle assistant"));
        assert!(
            unchanged.unwrap().messages.is_empty(),
            "must not flush partial text"
        );
        if let Some(suffix) = suffix {
            silent(suffix.unwrap());
        }
        assert_eq!(
            end.unwrap().events,
            vec![pipecat::turns::AggregationEvent::AssistantTurnStopped {
                text: if buffered {
                    "before after".into()
                } else {
                    String::new()
                },
                interrupted: false,
            }]
        );
        silent(retry.unwrap());
        let after = after.unwrap();
        assert_eq!(after.messages.len(), if buffered { 2 } else { 1 });
        if buffered {
            assert_eq!(
                after.messages[0].content,
                MessageContent::Text("before after".into())
            );
        }
        assert_eq!(after.messages.last().unwrap().role, ChatRole::User);
    }
}

#[tokio::test]
async fn rgb_preserves_speech_tool_thought_inference_and_playback_gates() {
    let mut awaited = Frame::new(FrameData::UserStoppedSpeaking);
    awaited
        .metadata
        .insert("user_context_follows".into(), json!(true));
    let preludes = [
        Frame::new(FrameData::UserStartedSpeaking),
        Frame::new(FrameData::BotStartedSpeaking),
        awaited,
        Frame::new(FrameData::FunctionCallInProgress(
            FunctionCallInProgressFrame {
                function_name: "pending".into(),
                tool_call_id: "pending".into(),
                arguments: json!({}),
                cancel_on_interruption: false,
                group_id: None,
            },
        )),
        Frame::new(FrameData::LlmThoughtText(TextFrame::llm("held thought"))),
    ];
    for prelude in preludes {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let setup = consume(&assistant, prelude);
        let before = context.snapshot();
        let rejected = consume(&assistant, direct_image(Some(false)));
        let after = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        setup.unwrap();
        assert!(rejected.unwrap_err().message.contains("idle assistant"));
        assert_eq!(before.unwrap(), after.unwrap());
    }
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    let started = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let spoken_busy = consume(&assistant, direct_image(Some(false)));
    let ended = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let playback = assistant.begin_playback("held");
    let rejected = consume(&assistant, direct_image(Some(false)));
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    started.unwrap();
    assert!(spoken_busy.unwrap_err().message.contains("idle assistant"));
    ended.unwrap();
    playback.unwrap();
    assert!(rejected.unwrap_err().message.contains("idle assistant"));
    assert!(after.unwrap().messages.is_empty());
}

#[tokio::test]
async fn rgb_terminal_handling_keeps_private_carrier_order() {
    let mut continuation = Frame::new(FrameData::LlmResponseStart);
    continuation
        .set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::start(994192, LlmContinuationProvider::Gemini).unwrap(),
        ))
        .unwrap();
    continuation.data = direct_image(None).data;
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
    tool.data = direct_image(None).data;
    for terminal in [
        Frame::end(),
        Frame::cancel("test"),
        Frame::new(FrameData::Stop),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let end = consume(&assistant, terminal);
        let valid = consume(&assistant, direct_image(None));
        let mut malformed = direct_image(None);
        payload(&mut malformed).image.image.clear();
        let invalid = consume(&assistant, malformed);
        let ignored_continuation = consume(&assistant, continuation.clone());
        let rejected_tool = consume(&assistant, tool.clone());
        let snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        end.unwrap();
        silent(valid.unwrap());
        silent(invalid.unwrap());
        silent(ignored_continuation.unwrap());
        assert!(rejected_tool.is_err());
        assert!(snapshot.unwrap().messages.is_empty());
    }
}
