//! Exact-YCbCr color consumption through AssistantImage and owned InputTransport.
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
fn ycbcr_image() -> ImageRawFrame {
    // Odd-width, non-square packed top-down Y, Cb, Cr with no row padding.
    // Full-range JPEG-domain values; six 16x16 tiles extend one column/row.
    let colors = [
        [96, 96, 176],
        [160, 160, 80],
        [128, 176, 112],
        [192, 112, 144],
        [16, 128, 128],
        [235, 128, 128],
    ];
    let mut image = Vec::with_capacity(49 * 33 * 3);
    for y in 0_usize..33 {
        for x in 0_usize..49 {
            image.extend_from_slice(&colors[(y / 16).min(1) * 3 + (x / 16).min(2)]);
        }
    }
    ImageRawFrame {
        image,
        size: (49, 33),
        format: Some("YCbCr".into()),
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
            image: ycbcr_image(),
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
        .expect("YCbCr must become a JPEG data URL")
        .to_owned()
}
fn decoded_rgb(
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
    let expected = usize::from(width) * usize::from(height) * 3;
    let mut decoder = Decoder::new(Cursor::new(jpeg));
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
fn asymmetric_pixels(message: &ContextMessage, role: ChatRole, text: Option<&str>) {
    let pixels = decoded_rgb(message, role, text, 49, 33);
    // Predeclared from full-range YCbCr equations, independent of native JPEG bytes.
    // Interiors are away from chroma boundaries; do not widen on a failed run.
    for (x, y, rgb, tolerance) in [
        (8, 8, [163, 73, 39], 12),
        (24, 8, [93, 183, 217], 12),
        (40, 8, [106, 123, 213], 12),
        (8, 24, [214, 186, 164], 12),
        (24, 24, [16, 16, 16], 4),
        (40, 24, [235, 235, 235], 4),
    ] {
        let offset = (y * 49 + x) * 3;
        for (actual, expected) in pixels[offset..offset + 3].iter().zip(rgb) {
            assert!(
                actual.abs_diff(expected) <= tolerance,
                "({x}, {y}): {actual} differs from {expected}, tolerance {tolerance}"
            );
        }
    }
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
async fn direct_ycbcr_without_originals_appends_decodable_assistant_jpeg() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = assistant.process_frame(assistant_image(), Direction::Downstream);
    let rich = context.snapshot_rich();
    let native = context.snapshot();
    let projected = context.frame_data();
    let ended = consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    ended.unwrap();
    cleaned.unwrap();
    // Old unsupported boundary is asserted only after normal End and cleanup.
    silent(result.expect("exact YCbCr fallback must append an assistant JPEG"));
    let rich = rich.unwrap();
    assert_eq!(rich.messages.len(), 1);
    asymmetric_pixels(&rich.messages[0], ChatRole::Assistant, None);
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
        "ycbcr-image-input-tap"
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
async fn actual_input_ycbcr_capture_prepares_upstream_decodable_user_context() {
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
        Arc::new(InputTransport::new("ycbcr-input", config, backend.clone(), None).unwrap());
    let ingress = input.ingress();
    let marker = Frame::new(FrameData::InputImage(ycbcr_image()));
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
    let captured = Frame::new(FrameData::InputImage(ycbcr_image()));
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
    assert_eq!(image.image, ycbcr_image());
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
        "consumer rejected owned YCbCr image: {:?}",
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
        image: ycbcr_image(),
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
async fn direct_user_ycbcr_true_none_and_false_preserve_content_and_run_policy() {
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
async fn ycbcr_mode_is_exact_and_rejections_do_not_mutate_history() {
    for role in [ChatRole::User, ChatRole::Assistant] {
        for mode in [
            None,
            Some(""),
            Some("ycbcr"),
            Some("YCBCR"),
            Some(" YCbCr"),
            Some("YCbCr "),
            Some("YCbCr\0"),
            Some("YCbCrX"),
            Some("YCbCr;L"),
            Some("YCrCb"),
            Some("YUV"),
            Some("YUV444"),
            Some("NV12"),
            Some("I420"),
            Some("YUV422"),
            Some("RGBA"),
            Some("RGBa"),
            Some("BGRX"),
            Some("CMYK"),
            Some("L8"),
            Some("LA"),
            Some("1"),
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

#[tokio::test]
async fn ycbcr_geometry_short_and_whole_buffer_caps_are_atomic_and_repairable() {
    let mut invalid = vec![];
    for (size, diagnostic) in [
        ((0, 1), "dimensions must be 1..=65500"),
        ((1, 0), "dimensions must be 1..=65500"),
        ((65_501, 1), "dimensions must be 1..=65500"),
        ((1, 65_501), "dimensions must be 1..=65500"),
        ((u32::MAX, u32::MAX), "dimensions must be 1..=65500"),
        ((65_500, 65_500), "pixel bytes exceed 8388608"),
        ((2048, 1366), "pixel bytes exceed 8388608"),
    ] {
        let mut image = ycbcr_image();
        image.size = size;
        invalid.push((image, diagnostic));
    }
    let mut empty = ycbcr_image();
    empty.image.clear();
    invalid.push((empty, "buffer is shorter than its dimensions"));
    let mut short = ycbcr_image();
    short.image.pop();
    invalid.push((short, "buffer is shorter than its dimensions"));
    let mut two_byte_pixels = ycbcr_image();
    two_byte_pixels.image.truncate(49 * 33 * 2);
    invalid.push((two_byte_pixels, "buffer is shorter than its dimensions"));
    let mut over = ycbcr_image();
    over.image.resize(IMAGE_CAP + 1, 0);
    invalid.push((over, "buffer exceeds 8388608 bytes"));
    for role in [ChatRole::User, ChatRole::Assistant] {
        for (malformed, diagnostic) in &invalid {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let good = frame_for(role);
            let mut bad = good.clone();
            *raw_image(&mut bad) = malformed.clone();
            let rejected = consume(&assistant, bad);
            let before = context.snapshot_rich();
            let repaired = consume(&assistant, good);
            let after = context.snapshot_rich();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            let owner = if role == ChatRole::User {
                "user image YCbCr"
            } else {
                "assistant image YCbCr"
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
    }
}

fn color_sample(pixels: &[u8], x: usize, y: usize, expected: [u8; 3], tolerance: u8) {
    let offset = (y * 49 + x) * 3;
    for (actual, expected) in pixels[offset..offset + 3].iter().zip(expected) {
        assert!(
            actual.abs_diff(expected) <= tolerance,
            "({x}, {y}): {actual} differs from {expected}, tolerance {tolerance}"
        );
    }
}

#[tokio::test]
async fn ycbcr_is_full_range_direct_chroma_with_odd_packed_rows() {
    for role in [ChatRole::User, ChatRole::Assistant] {
        let exact = admitted(frame_for(role)).await;
        asymmetric_pixels(&exact.messages[0], role, text_for(role));
        let pixels = decoded_rgb(&exact.messages[0], role, text_for(role), 49, 33);
        // Extra right/bottom samples check the odd final column/row extensions.
        color_sample(&pixels, 48, 8, [106, 123, 213], 12);
        color_sample(&pixels, 8, 32, [214, 186, 164], 12);
        color_sample(&pixels, 24, 24, [16, 16, 16], 4);
        color_sample(&pixels, 40, 24, [235, 235, 235], 4);

        let mut rgb = frame_for(role);
        raw_image(&mut rgb).format = Some("RGB".into());
        let rgb = admitted(rgb).await;
        let pixels = decoded_rgb(&rgb.messages[0], role, text_for(role), 49, 33);
        color_sample(&pixels, 8, 8, [96, 96, 176], 12);
        color_sample(&pixels, 24, 8, [160, 160, 80], 12);
        assert_ne!(rgb, exact, "YCbCr components must not be treated as RGB");

        let mut swapped = frame_for(role);
        for pixel in raw_image(&mut swapped).image.as_chunks_mut::<3>().0 {
            pixel.swap(1, 2);
        }
        let swapped = admitted(swapped).await;
        let pixels = decoded_rgb(&swapped.messages[0], role, text_for(role), 49, 33);
        // Independently derived full-range RGB for (96,176,96), not native bytes.
        color_sample(&pixels, 8, 8, [51, 102, 181], 12);
        assert_ne!(swapped, exact, "Cb and Cr retain their distinct positions");
    }
}

#[tokio::test]
async fn ycbcr_uses_three_byte_pixels_ignores_bounded_tail_and_decodes_edge_geometry() {
    for role in [ChatRole::User, ChatRole::Assistant] {
        let plain = admitted(frame_for(role)).await;
        for length in [49 * 33 * 3 + 7, IMAGE_CAP] {
            let mut image = frame_for(role);
            raw_image(&mut image).image.resize(length, 251);
            let tailed = admitted(image).await;
            assert_eq!(
                tailed, plain,
                "surplus must not change the encoded pixel prefix"
            );
        }
        // Three-byte pixels cannot use exactly 8 MiB; last case is near the cap.
        for (width, height) in [
            (1_u16, 1_u16),
            (17, 9),
            (65_500, 1),
            (1, 65_500),
            (2048, 1365),
        ] {
            let mut image = frame_for(role);
            let pixels = usize::from(width) * usize::from(height);
            *raw_image(&mut image) = ImageRawFrame {
                image: [112, 128, 128].repeat(pixels),
                size: (u32::from(width), u32::from(height)),
                format: Some("YCbCr".into()),
            };
            let context = admitted(image).await;
            let pixels = decoded_rgb(&context.messages[0], role, text_for(role), width, height);
            assert!(pixels.iter().all(|pixel| pixel.abs_diff(112) <= 4));
        }
    }
}

#[tokio::test]
async fn ycbcr_replay_order_and_buffered_text_keep_distinct_user_assistant_rules() {
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
async fn ycbcr_assistant_original_priority_truthiness_and_failed_original_no_rescue() {
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
    for (data, mime) in [
        (vec![1, 2, 3], "image/png"),
        (png.to_vec(), " "),
        (png.to_vec(), "image/jpeg"),
        (png.to_vec(), "YCbCr"),
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
            .contains("assistant image requires image/png or image/jpeg MIME"));
        assert!(before.unwrap().messages.is_empty());
        silent(repaired.unwrap());
        silent(replay.unwrap());
        let after = after.unwrap();
        assert_eq!(after.messages.len(), 1);
        asymmetric_pixels(&after.messages[0], ChatRole::Assistant, None);
    }
}

#[tokio::test]
async fn ycbcr_passive_flags_skip_pixels_without_consuming_correctable_id() {
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
        *raw_image(&mut frame) = ycbcr_image();
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
async fn ycbcr_user_request_authority_and_busy_inference_stay_rejected() {
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
        NativeToolCallBatch::new("ycbcr-owned".into(), vec![(Arc::new(call), false)]).unwrap(),
    )
    .unwrap();
    for carrier in [&mut continuation, &mut tool] {
        carrier.id = frame.id;
        carrier.data = frame.data.clone();
    }
    [continuation, tool]
}
#[tokio::test]
async fn ycbcr_private_signed_realtime_and_terminal_guards_keep_existing_order() {
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
async fn ycbcr_user_projection_failure_keeps_history_and_success_id_retryable() {
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
