//! Exact-RGB assistant-image fallback: application/public codec -> context -> Gemini body.
//! Two old-red witnesses frozen before candidate-only controls; no provider or camera I/O.
use base64::{engine::general_purpose::STANDARD, Engine as _};
use jpeg_decoder::{CodingProcess, Decoder, PixelFormat};
use pipecat::{
    core::*,
    network_bus::PythonJsonCodec,
    services::{adapters::request_body, CompletionRequest, Context, Provider},
    turns::{
        AggregationAction, AggregationEvent, AggregationOutput, LLMAssistantAggregator,
        SharedContext,
    },
    workers::MessageData,
};
use serde_json::{json, Value};
use std::{io::Cursor, time::Duration};

const RGB_CAP: usize = 8 * 1024 * 1024;
fn rgb_image() -> ImageRawFrame {
    // Asymmetric, non-square, top-down RGB24. Each tile is 16x16 (MCU-aligned).
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
fn image_frame(original: Option<Vec<u8>>, mime: Option<&str>) -> Frame {
    Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
        image: OutputImageRawFrame {
            image: rgb_image(),
            sync_with_audio: true,
        },
        original_data: original,
        original_mime_type: mime.map(str::to_owned),
    }))
}
fn consume(
    assistant: &LLMAssistantAggregator,
    frame: Frame,
) -> Result<AggregationOutput, ProcessorError> {
    assistant.process_at(frame, Direction::Downstream, Duration::ZERO)
}
fn silent(output: AggregationOutput) {
    assert!(
        output.actions.is_empty(),
        "image emitted actions: {:?}",
        output.actions
    );
    assert!(
        output.events.is_empty(),
        "image changed turn: {:?}",
        output.events
    );
}
fn jpeg_data(message: &ContextMessage) -> &str {
    assert_eq!(message.role, ChatRole::Assistant);
    assert!(message.name.is_none());
    assert!(message.tool_call_id.is_none());
    assert!(message.tool_calls.is_empty());
    assert!(message.extra.is_empty());
    let MessageContent::Parts(parts) = &message.content else {
        panic!("expected rich image")
    };
    assert_eq!(parts.len(), 1);
    let MessagePart::Image { url, detail } = &parts[0] else {
        panic!("expected image only")
    };
    assert!(detail.is_none());
    url.strip_prefix("data:image/jpeg;base64,")
        .expect("RGB must become JPEG")
}
fn decoded_rgb(message: &ContextMessage, width: u16, height: u16) -> Vec<u8> {
    let jpeg = STANDARD.decode(jpeg_data(message)).unwrap();
    assert!(!jpeg.is_empty() && jpeg.len() <= RGB_CAP);
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
fn asymmetric_pixels(message: &ContextMessage) {
    let pixels = decoded_rgb(message, 48, 32);
    for (x, y, rgb) in [
        (8, 8, [240, 16, 16]),
        (24, 8, [16, 240, 16]),
        (40, 8, [16, 16, 240]),
        (8, 24, [240, 240, 240]),
        (24, 24, [16, 16, 16]),
        (40, 24, [128, 128, 128]),
    ] {
        let offset = (y * 48 + x) * 3;
        for (actual, expected) in pixels[offset..offset + 3].iter().zip(rgb) {
            // Fixture-specific interior tolerance, not arbitrary JPEG pixel parity.
            assert!(
                actual.abs_diff(expected) <= 12,
                "{actual} differs from {expected}"
            );
        }
    }
}
fn gemini_contents(context: Context, data: &str, text: Option<&str>) {
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("fixture", context),
    )
    .unwrap();
    let mut parts = vec![json!({"inlineData":{"mimeType":"image/jpeg", "data":data}})];
    if let Some(text) = text {
        parts.push(json!({"text":text}));
    }
    assert_eq!(
        body["contents"],
        json!([
            {"role":"model", "parts":parts},
            {"role":"user", "parts":[{"text":"."}]}
        ])
    );
}
fn codec_frame(original: Value, mime: Value) -> (Frame, Direction) {
    let wire = json!({
        "__type__":"pipecat.bus.messages.BusFrameMessage", "__data__":{
            "source":"application-rgb", "target":"assistant-context",
            "frame":{"__type__":"pipecat.frames.frames.AssistantImageRawFrame", "__data__":{
                "id":982301, "name":"AssistantImageRawFrame#RGB",
                "metadata":{"tool_call_id":"untrusted", "run_llm":true},
                "image":{"__type__":"bytes", "__data__":STANDARD.encode(rgb_image().image)},
                "size":[48,32], "format":"RGB", "sync_with_audio":true,
                "original_data":original, "original_mime_type":mime
            }},
            "direction":{"__type__":"pipecat.processors.frame_processor.FrameDirection", "__data__":"UPSTREAM"}
        }
    });
    let envelope = PythonJsonCodec::default()
        .decode(&serde_json::to_vec(&wire).unwrap())
        .unwrap();
    let MessageData::Frame {
        frame, direction, ..
    } = &envelope.as_bus_message().unwrap().data
    else {
        panic!("public codec did not produce a frame")
    };
    ((**frame).clone(), *direction)
}

#[tokio::test]
async fn direct_rgb_without_originals_appends_decodable_assistant_jpeg() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = assistant.process_frame(image_frame(None, None), Direction::Downstream);
    let rich = context.snapshot_rich();
    let snapshot = context.snapshot();
    let frame_data = context.frame_data();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    // First unsupported boundary on the old implementation: missing original_data.
    silent(result.expect("exact RGB with no originals must append an assistant JPEG"));
    let rich = rich.unwrap();
    assert_eq!(rich.messages.len(), 1);
    asymmetric_pixels(&rich.messages[0]);
    let FrameData::LlmContextDetailed(projected) = frame_data.unwrap() else {
        panic!("expected rich projection")
    };
    assert_eq!(projected.context, rich);
    gemini_contents(snapshot.unwrap(), jpeg_data(&rich.messages[0]), None);
}

#[tokio::test]
async fn public_codec_partial_empty_originals_use_rgb_without_flushing_text() {
    // Nonempty deliberately invalid encoded originals plus absent/empty MIME,
    // and empty originals with nonempty MIME, must select display RGB instead.
    for (original, mime) in [
        (json!({"__type__":"bytes", "__data__":"AQID"}), json!("")),
        (json!({"__type__":"bytes", "__data__":"AQID"}), Value::Null),
        (
            json!({"__type__":"bytes", "__data__":""}),
            json!("image/png"),
        ),
    ] {
        let (frame, direction) = codec_frame(original, mime);
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let first = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm("before"))),
        );
        let image = assistant.process_at(frame, direction, Duration::ZERO);
        let during = context.snapshot_rich();
        let second = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm(" after"))),
        );
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let after = context.snapshot_rich();
        let snapshot = context.snapshot();
        let frame_data = context.frame_data();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert_eq!(direction, Direction::Upstream);
        assert_eq!(
            start.unwrap().events,
            vec![AggregationEvent::AssistantTurnStarted]
        );
        silent(first.unwrap());
        // First unsupported boundary on the old implementation: original MIME/signature.
        silent(image.expect("public-codec partial originals must fall back to RGB"));
        silent(second.unwrap());
        let during = during.unwrap();
        assert_eq!(
            during.messages.len(),
            1,
            "image must not flush buffered prefix"
        );
        asymmetric_pixels(&during.messages[0]);
        let end = end.unwrap();
        assert_eq!(
            end.events,
            vec![AggregationEvent::AssistantTurnStopped {
                text: "before after".into(),
                interrupted: false
            }]
        );
        let after = after.unwrap();
        assert_eq!(after.messages.len(), 2);
        assert_eq!(after.messages[0], during.messages[0]);
        assert_eq!(after.messages[1].role, ChatRole::Assistant);
        assert_eq!(
            after.messages[1].content,
            MessageContent::Text("before after".into())
        );
        let FrameData::LlmContextDetailed(projected) = frame_data.unwrap() else {
            panic!("expected rich projection")
        };
        assert_eq!(projected.context, after);
        // Normal nonempty text completion emits context, timestamp and a turn
        // broadcast pair. The image itself emitted no action or event above.
        assert_eq!(end.actions.len(), 4);
        let pushes: Vec<_> = end
            .actions
            .iter()
            .map(|action| {
                let AggregationAction::Push(push) = action else {
                    panic!("unexpected end action")
                };
                push
            })
            .collect();
        assert_eq!(
            pushes.iter().map(|push| push.direction).collect::<Vec<_>>(),
            vec![
                Direction::Downstream,
                Direction::Downstream,
                Direction::Downstream,
                Direction::Upstream
            ]
        );
        let FrameData::LlmContextDetailed(committed) = &pushes[0].frame.data else {
            panic!("expected end context")
        };
        assert_eq!(committed.context, after);
        let FrameData::LlmContextAssistantTimestamp { timestamp } = &pushes[1].frame.data else {
            panic!("expected completion timestamp")
        };
        assert!(!timestamp.is_empty());
        for push in &pushes[2..] {
            let FrameData::LlmContextAssistantTurn(turn) = &push.frame.data else {
                panic!("expected turn broadcast")
            };
            assert_eq!(turn.text, "before after");
            assert!(!turn.timestamp.is_empty());
        }
        assert_eq!(pushes[2].frame.data, pushes[3].frame.data);
        assert_eq!(
            pushes[2].frame.broadcast_sibling_id,
            Some(pushes[3].frame.id)
        );
        assert_eq!(
            pushes[3].frame.broadcast_sibling_id,
            Some(pushes[2].frame.id)
        );
        gemini_contents(
            snapshot.unwrap(),
            jpeg_data(&after.messages[0]),
            Some("before after"),
        );
    }
}

// Candidate-only controls. The edition-2021 formatted baseline above is byte-preserved.
fn payload(frame: &mut Frame) -> &mut AssistantImageRawFrame {
    let FrameData::AssistantImage(image) = &mut frame.data else {
        panic!("expected AssistantImage")
    };
    image
}
async fn admitted(frame: Frame) -> LlmContextData {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = consume(&assistant, frame);
    let rich = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(result.unwrap());
    let rich = rich.unwrap();
    assert_eq!(rich.messages.len(), 1);
    rich
}
fn signed_boundary(data: FrameData, history: NativeLlmContinuation) -> Frame {
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(std::sync::Arc::new(history))
        .unwrap();
    frame
}
fn private_images(id: u64) -> [Frame; 2] {
    let mut continuation = signed_boundary(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(982321, LlmContinuationProvider::Anthropic).unwrap(),
    );
    continuation.id = id;
    continuation.data = image_frame(None, None).data;
    let call = FunctionCall {
        function_name: "owned".into(),
        tool_call_id: "rgb-private".into(),
        arguments: json!({}),
    };
    let batch = NativeToolCallBatch::new(
        "rgb-group".into(),
        vec![(std::sync::Arc::new(call.clone()), false)],
    )
    .unwrap();
    let mut tool = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
    tool.set_native_tool_batch(batch).unwrap();
    tool.id = id;
    tool.data = image_frame(None, None).data;
    [continuation, tool]
}

#[tokio::test]
async fn original_truthiness_cross_product_selects_rgb_or_original_without_rescue() {
    // Signature-only PNG is sufficient for the existing original admission path;
    // original success here is byte pass-through, not complete PNG validation.
    let png = b"\x89PNG\r\n\x1a\noriginal";
    for data in [None, Some(vec![]), Some(png.to_vec())] {
        for mime in [None, Some(""), Some("image/png")] {
            let originals_win = data.as_ref().is_some_and(|data| !data.is_empty())
                && mime.is_some_and(|mime| !mime.is_empty());
            let mut frame = image_frame(data.clone(), mime);
            if originals_win {
                payload(&mut frame).image.image = ImageRawFrame {
                    image: vec![],
                    size: (0, u32::MAX),
                    format: Some("bad-display".into()),
                };
            }
            let rich = admitted(frame).await;
            if originals_win {
                assert_eq!(
                    rich.messages[0].content,
                    MessageContent::Parts(vec![MessagePart::Image {
                        url: format!("data:image/png;base64,{}", STANDARD.encode(png)),
                        detail: None,
                    }])
                );
            } else {
                asymmetric_pixels(&rich.messages[0]);
            }
        }
    }
    for (data, mime) in [
        (vec![1, 2, 3], "image/png"),
        (png.to_vec(), " "),
        (png.to_vec(), "image/jpeg"),
        (png.to_vec(), "RGB"),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let bad = image_frame(Some(data), Some(mime));
        let mut fixed = image_frame(None, None);
        fixed.id = bad.id;
        let rejected = consume(&assistant, bad);
        let before = context.snapshot_rich();
        let repair = consume(&assistant, fixed);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected
            .unwrap_err()
            .message
            .contains("assistant image requires image/png or image/jpeg MIME"));
        assert!(before.unwrap().messages.is_empty());
        silent(repair.unwrap());
        asymmetric_pixels(&after.unwrap().messages[0]);
    }
}

#[tokio::test]
async fn ignored_original_storage_is_not_a_selected_pixel_cap() {
    // The bounded RGB input/output policy is not a total-frame retention bound.
    for mime in [None, Some("")] {
        let rich = admitted(image_frame(Some(vec![17; RGB_CAP + 1]), mime)).await;
        asymmetric_pixels(&rich.messages[0]);
    }
    let ignored_mime = "x".repeat(RGB_CAP + 1);
    let rich = admitted(image_frame(None, Some(&ignored_mime))).await;
    asymmetric_pixels(&rich.messages[0]);
}

#[tokio::test]
async fn malformed_fallback_is_atomic_repairable_and_keeps_buffered_text() {
    let mut invalid = vec![];
    let mut short = rgb_image();
    short.image.pop();
    invalid.push(short);
    let mut zero = rgb_image();
    zero.size = (0, 32);
    invalid.push(zero);
    let mut over = rgb_image();
    over.image.resize(RGB_CAP + 1, 0);
    invalid.push(over);
    for mode in [None, Some(""), Some("rgb"), Some("RGBA"), Some("image/png")] {
        let mut image = rgb_image();
        image.format = mode.map(str::to_owned);
        invalid.push(image);
    }
    for display in invalid {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let prefix = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm("before"))),
        );
        let good = image_frame(Some(vec![1, 2, 3]), None);
        let mut bad = good.clone();
        payload(&mut bad).image.image = display;
        let rejection = consume(&assistant, bad.clone());
        let before = context.snapshot_rich();
        let repair = consume(&assistant, good);
        // AssistantImage intentionally checks successful identity before validation.
        let malformed_replay = consume(&assistant, bad);
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
        assert!(rejection
            .unwrap_err()
            .message
            .starts_with("assistant image"));
        assert!(before.unwrap().messages.is_empty());
        silent(repair.unwrap());
        silent(malformed_replay.unwrap());
        let middle = middle.unwrap();
        assert_eq!(middle.messages.len(), 1);
        asymmetric_pixels(&middle.messages[0]);
        assert_eq!(
            end.unwrap().events,
            vec![AggregationEvent::AssistantTurnStopped {
                text: "before after".into(),
                interrupted: false
            }]
        );
        let after = after.unwrap();
        assert_eq!(after.messages.len(), 2);
        assert_eq!(after.messages[0], middle.messages[0]);
        assert_eq!(
            after.messages[1].content,
            MessageContent::Text("before after".into())
        );
    }
}

#[tokio::test]
async fn successful_rgb_identity_skips_bad_originals_but_not_private_authority() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let good = image_frame(None, None);
    let admitted = consume(&assistant, good.clone());
    let before = context.snapshot_rich();
    let mut malformed = good.clone();
    payload(&mut malformed).original_data = Some(vec![1, 2, 3]);
    payload(&mut malformed).original_mime_type = Some("image/png".into());
    let skipped = consume(&assistant, malformed);
    let [continuation, tool] = private_images(good.id);
    let private = [consume(&assistant, continuation), consume(&assistant, tool)];
    let replay = consume(&assistant, good);
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(admitted.unwrap());
    silent(skipped.unwrap());
    silent(replay.unwrap());
    for rejection in private {
        assert!(rejection.is_err(), "private carrier bypassed seen-ID guard");
    }
    assert_eq!(before.unwrap(), after.unwrap());
}

#[tokio::test]
async fn rgb_terminal_order_preserves_tool_revalidation_and_continuation_noop() {
    for terminal in [
        Frame::end(),
        Frame::cancel("fixture"),
        Frame::new(FrameData::Stop),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let good = image_frame(None, None);
        let [continuation, tool] = private_images(good.id);
        let ended = consume(&assistant, terminal);
        let valid = consume(&assistant, good);
        let mut bad = image_frame(None, None);
        payload(&mut bad).image.image.image.clear();
        let malformed = consume(&assistant, bad);
        let ignored_continuation = consume(&assistant, continuation);
        let rejected_tool = consume(&assistant, tool);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        ended.unwrap();
        silent(valid.unwrap());
        silent(malformed.unwrap());
        silent(ignored_continuation.unwrap());
        assert!(rejected_tool.is_err());
        assert!(after.unwrap().messages.is_empty());
    }
}

#[tokio::test]
async fn rgb_realtime_guard_precedes_encoding_and_seen_identity() {
    use pipecat::turns::{LLMContextAggregatorPair, RealtimeServiceMode};
    for mode in [RealtimeServiceMode::Enabled, RealtimeServiceMode::Auto] {
        let pair = LLMContextAggregatorPair::with_realtime_mode(
            Context::default(),
            Default::default(),
            Default::default(),
            mode,
        )
        .unwrap();
        let assistant = pair.assistant();
        let image = image_frame(None, None);
        let first = (mode == RealtimeServiceMode::Auto).then(|| consume(&assistant, image.clone()));
        let before = pair.context().snapshot_rich();
        let announcement = (mode == RealtimeServiceMode::Auto).then(|| {
            consume(
                &assistant,
                Frame::new(FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
                    base: ServiceMetadataFrame {
                        service_name: "fixture".into(),
                        user_turn_strategies: None,
                    },
                    is_realtime_service: true,
                })),
            )
        });
        let mut malformed = image;
        payload(&mut malformed).image.image.image.clear();
        let rejection = consume(&assistant, malformed);
        let after = pair.context().snapshot_rich();
        let cleaned = assistant.cleanup().await;
        let user_cleaned = pair.user().cleanup().await;
        drop(assistant);
        drop(pair);
        cleaned.unwrap();
        user_cleaned.unwrap();
        if let Some(first) = first {
            silent(first.unwrap());
        }
        if let Some(announcement) = announcement {
            announcement.unwrap();
        }
        assert!(rejection.unwrap_err().message.contains("realtime"));
        assert_eq!(before.unwrap(), after.unwrap());
    }
}

#[tokio::test]
async fn rgb_rejects_active_and_completed_signed_owner_until_playback_closes() {
    use pipecat::turns::{AssistantAggregatorConfig, AssistantTextMode};
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    let seen = image_frame(None, None);
    let initial = consume(&assistant, seen.clone());
    let start = consume(
        &assistant,
        signed_boundary(
            FrameData::LlmResponseStart,
            NativeLlmContinuation::start(982331, LlmContinuationProvider::Anthropic).unwrap(),
        ),
    );
    let playback = assistant.begin_playback("held");
    let generated = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm("heard"))),
    );
    let active_rejection = consume(&assistant, seen.clone());
    let end = consume(
        &assistant,
        signed_boundary(
            FrameData::LlmResponseEnd,
            NativeLlmContinuation::new(
                982331,
                LlmContinuationProvider::Anthropic,
                vec![LlmContinuationBlock::Text("heard".into())],
            )
            .unwrap(),
        ),
    );
    let before = context.snapshot_rich();
    let unseen = image_frame(None, None);
    let retained_rejection = consume(&assistant, unseen.clone());
    let seen_rejection = consume(&assistant, seen.clone());
    let unchanged = context.snapshot_rich();
    let receipt = assistant.confirm_playback(
        "held",
        &[AggregatedTextFrame {
            text: TextFrame::llm("heard"),
            raw_text: Some("heard".into()),
            context_id: Some("held".into()),
            aggregated_by: "fixture".into(),
            will_be_spoken: true,
        }],
    );
    let repaired = consume(&assistant, unseen);
    let replay = consume(&assistant, seen);
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(initial.unwrap());
    start.unwrap();
    playback.unwrap();
    generated.unwrap();
    end.unwrap();
    for rejected in [active_rejection, retained_rejection, seen_rejection] {
        assert!(rejected
            .unwrap_err()
            .message
            .contains("signed continuation"));
    }
    assert_eq!(before.unwrap(), unchanged.unwrap());
    receipt.unwrap();
    silent(repaired.unwrap());
    silent(replay.unwrap());
    let after = after.unwrap();
    assert_eq!(after.messages.len(), 3);
    asymmetric_pixels(&after.messages[0]);
    assert_eq!(
        after.messages[1].content,
        MessageContent::Text("heard".into())
    );
    asymmetric_pixels(&after.messages[2]);
}

#[tokio::test]
async fn rgb_preserves_completed_historical_signed_prefix_in_gemini_request() {
    use pipecat::services::{Content, ContentPart, Message, Role};
    let prefix = Message {
        role: Role::Assistant,
        content: Some(Content::Parts(vec![ContentPart::GeminiText {
            text: "prior".into(),
            thought: false,
            signature: Some("opaque".into()),
            incomplete: false,
        }])),
        name: None,
        tool_calls: vec![],
        tool_call_id: None,
    };
    let context = SharedContext::new(Context::new(vec![prefix.clone()]));
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = consume(&assistant, image_frame(None, None));
    let rich = context.snapshot_rich();
    let snapshot = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(result.unwrap());
    let rich = rich.unwrap();
    assert_eq!(rich.messages.len(), 2);
    asymmetric_pixels(&rich.messages[1]);
    let snapshot = snapshot.unwrap();
    assert_eq!(snapshot.messages[0], prefix);
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("fixture", snapshot),
    )
    .unwrap();
    assert_eq!(
        body["contents"],
        json!([
            {"role":"model", "parts":[{"text":"prior", "thoughtSignature":"opaque"},
                {"inlineData":{"mimeType":"image/jpeg", "data":jpeg_data(&rich.messages[1])}}]},
            {"role":"user", "parts":[{"text":"."}]}
        ])
    );
}

#[tokio::test]
async fn rgb_bounded_surplus_exact_side_and_odd_dimensions_reuse_encoder() {
    let exact = admitted(image_frame(None, None)).await;
    for length in [48 * 32 * 3 + 7, RGB_CAP] {
        let mut surplus = image_frame(None, None);
        payload(&mut surplus).image.image.image.resize(length, 219);
        assert_eq!(admitted(surplus).await, exact);
    }
    for (width, height) in [(17, 9), (65_500, 1)] {
        let mut image = image_frame(None, None);
        payload(&mut image).image.image = ImageRawFrame {
            image: [32, 96, 176].repeat(width * height),
            size: (width as u32, height as u32),
            format: Some("RGB".into()),
        };
        let rich = admitted(image).await;
        let pixels = decoded_rgb(&rich.messages[0], width as u16, height as u16);
        for pixel in pixels.as_chunks::<3>().0 {
            for (actual, expected) in pixel.iter().zip([32, 96, 176]) {
                assert!(actual.abs_diff(expected) <= 12);
            }
        }
    }
    // The independently decoded baseline proves RGB24 sequential DCT. Header
    // checks additionally pin 4:2:0 and quality-75 default quantization here.
    let jpeg = STANDARD.decode(jpeg_data(&exact.messages[0])).unwrap();
    let mut cursor = 2;
    let mut sof = None;
    let mut dqt = vec![];
    let mut dht = vec![];
    while cursor + 4 <= jpeg.len() {
        assert_eq!(jpeg[cursor], 0xff);
        let marker = jpeg[cursor + 1];
        if marker == 0xda {
            break;
        }
        let length = usize::from(u16::from_be_bytes([jpeg[cursor + 2], jpeg[cursor + 3]]));
        assert!(length >= 2 && cursor + 2 + length <= jpeg.len());
        let data = &jpeg[cursor + 4..cursor + 2 + length];
        match marker {
            0xc0 => sof = Some(data),
            0xdb => dqt.push(data),
            0xc4 => dht.push(data),
            _ => {}
        }
        cursor += length + 2;
    }
    let sof = sof.unwrap();
    assert_eq!(sof[0], 8);
    assert_eq!(sof[5], 3);
    assert_eq!([sof[7], sof[10], sof[13]], [0x22, 0x11, 0x11]);
    assert!(dqt
        .iter()
        .any(|table| table.first() == Some(&0) && table.get(1) == Some(&8)));
    // Standard fixed tables, rather than fixture-optimized Huffman tables.
    assert_eq!(dht.len(), 4);
    assert!(dht.iter().any(|table| table.len() == 29 && table[0] == 0));
    assert!(dht
        .iter()
        .any(|table| table.len() == 179 && table[0] == 0x10));
}
