//! Bounded encoded-display assistant-image fallback: public frame/codec -> context -> Gemini.
//! Two frozen old-red witnesses; no image producer, provider request or camera I/O.
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

// Complete, CRC-correct 1x1 RGB PNG.
const PNG: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0,
    0, 0, 144, 119, 83, 222, 0, 0, 0, 12, 73, 68, 65, 84, 120, 156, 99, 248, 207, 192, 0, 0, 3, 1,
    1, 0, 201, 254, 146, 239, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];
const PNG_BASE64: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
// Complete 1x1 baseline grayscale JPEG: DC zero and EOB produce pixel 128.
const JPEG: &[u8] = &[
    255, 216, 255, 219, 0, 67, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 255, 192, 0, 11, 8, 0, 1, 0, 1, 1, 1, 17, 0, 255, 196, 0, 38, 0,
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 255, 218, 0, 8, 1, 1, 0, 0, 63, 0, 63, 255, 217,
];
const JPEG_BASE64: &str = "/9j/2wBDAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQH/wAALCAABAAEBAREA/8QAJgABAAAAAAAAAAAAAAAAAAAAABABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQAAPwA//9k=";
fn image_frame(bytes: &[u8], mime: &str) -> Frame {
    Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
        image: OutputImageRawFrame {
            image: ImageRawFrame {
                image: bytes.to_vec(),
                // Encoded bytes ignore display dimensions entirely.
                size: (0, u32::MAX),
                format: Some(mime.into()),
            },
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
fn assert_image(message: &ContextMessage, bytes: &[u8], mime: &str, base64: &str) {
    assert_eq!(message.role, ChatRole::Assistant);
    assert_eq!(
        message.content,
        MessageContent::Parts(vec![MessagePart::Image {
            url: format!("data:{mime};base64,{base64}"),
            detail: None,
        }])
    );
    assert!(message.name.is_none());
    assert!(message.tool_call_id.is_none());
    assert!(message.tool_calls.is_empty());
    assert!(message.extra.is_empty());
    assert_eq!(STANDARD.decode(base64).unwrap(), bytes);
    assert_eq!(STANDARD.encode(bytes), base64);
}
fn assert_complete_jpeg_fixture() {
    let mut decoder = Decoder::new(Cursor::new(JPEG));
    decoder.set_max_decoding_buffer_size(1);
    let pixels = decoder.decode().unwrap();
    let info = decoder.info().unwrap();
    assert_eq!((info.width, info.height), (1, 1));
    assert_eq!(info.pixel_format, PixelFormat::L8);
    assert_eq!(info.coding_process, CodingProcess::DctSequential);
    assert_eq!(pixels, vec![128]);
}
fn gemini_contents(context: Context, mime: &str, data: &str, text: Option<&str>) {
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("fixture", context),
    )
    .unwrap();
    let mut parts = vec![json!({"inlineData":{"mimeType":mime,"data":data}})];
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
fn codec_jpeg(original: Option<Value>, mime: Option<Value>) -> (Frame, Direction) {
    let mut wire = json!({
        "__type__":"pipecat.bus.messages.BusFrameMessage", "__data__":{
            "source":"application-encoded-display", "target":"assistant-context",
            "frame":{"__type__":"pipecat.frames.frames.AssistantImageRawFrame", "__data__":{
                "id":982401, "name":"AssistantImageRawFrame#encoded-display",
                "metadata":{"tool_call_id":"untrusted", "run_llm":true},
                "image":{"__type__":"bytes", "__data__":JPEG_BASE64},
                "size":[0,4294967295_u32], "format":"image/jpeg", "sync_with_audio":true
            }},
            "direction":{"__type__":"pipecat.processors.frame_processor.FrameDirection", "__data__":"UPSTREAM"}
        }
    });
    let fields = wire["__data__"]["frame"]["__data__"]
        .as_object_mut()
        .unwrap();
    if let Some(original) = original {
        fields.insert("original_data".into(), original);
    }
    if let Some(mime) = mime {
        fields.insert("original_mime_type".into(), mime);
    }
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
async fn direct_complete_png_display_without_originals_is_preserved() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = assistant.process_frame(image_frame(PNG, "image/png"), Direction::Downstream);
    let rich = context.snapshot_rich();
    let snapshot = context.snapshot();
    let frame_data = context.frame_data();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    // Old RGB-only implementation rejects at its exact-RGB fallback check.
    silent(result.expect("complete PNG display without originals must append unchanged"));
    let rich = rich.unwrap();
    assert_eq!(rich.messages.len(), 1);
    assert_image(&rich.messages[0], PNG, "image/png", PNG_BASE64);
    let FrameData::LlmContextDetailed(projected) = frame_data.unwrap() else {
        panic!("expected rich projection")
    };
    assert_eq!(projected.context, rich);
    gemini_contents(snapshot.unwrap(), "image/png", PNG_BASE64, None);
}

#[tokio::test]
async fn public_codec_complete_jpeg_falsey_originals_do_not_flush_buffered_text() {
    // First old-red row stops this test; later rows are candidate controls until green.
    // Invalid nonempty original bytes with empty/null MIME, empty tagged bytes
    // with nonempty MIME, then actually omitted fields all select display JPEG.
    for (original, mime) in [
        (
            Some(json!({"__type__":"bytes", "__data__":"AQID"})),
            Some(json!("")),
        ),
        (
            Some(json!({"__type__":"bytes", "__data__":"AQID"})),
            Some(Value::Null),
        ),
        (
            Some(json!({"__type__":"bytes", "__data__":""})),
            Some(json!("image/png")),
        ),
        (None, None),
    ] {
        let (frame, direction) = codec_jpeg(original, mime);
        let decoded = frame.clone();
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let first = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm("A"))),
        );
        let image = assistant.process_at(frame, direction, Duration::ZERO);
        let during = context.snapshot_rich();
        let second = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm(" B"))),
        );
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let after = context.snapshot_rich();
        let snapshot = context.snapshot();
        let frame_data = context.frame_data();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        // First unsupported consumer assertion after complete cleanup/drop.
        silent(image.expect("public-codec falsey originals must select unchanged JPEG display"));
        assert_eq!(direction, Direction::Upstream);
        let FrameData::AssistantImage(decoded) = decoded.data else {
            panic!("expected image");
        };
        assert_eq!(decoded.image.image.image, JPEG);
        assert_eq!(decoded.image.image.format.as_deref(), Some("image/jpeg"));
        assert_eq!(decoded.image.image.size, (0, u32::MAX));
        assert_complete_jpeg_fixture();
        assert_eq!(
            start.unwrap().events,
            vec![AggregationEvent::AssistantTurnStarted]
        );
        silent(first.unwrap());
        silent(second.unwrap());
        let during = during.unwrap();
        assert_eq!(during.messages.len(), 1, "image must not flush buffered A");
        assert_image(&during.messages[0], JPEG, "image/jpeg", JPEG_BASE64);
        let end = end.unwrap();
        assert_eq!(
            end.events,
            vec![AggregationEvent::AssistantTurnStopped {
                text: "A B".into(),
                interrupted: false
            }]
        );
        let after = after.unwrap();
        assert_eq!(after.messages.len(), 2);
        assert_eq!(after.messages[0], during.messages[0]);
        assert_eq!(after.messages[1].role, ChatRole::Assistant);
        assert_eq!(
            after.messages[1].content,
            MessageContent::Text("A B".into())
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
            assert_eq!(turn.text, "A B");
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
        gemini_contents(snapshot.unwrap(), "image/jpeg", JPEG_BASE64, Some("A B"));
    }
}

// Candidate-only controls. The full edition-2021 baseline above is byte-preserved.
const ENCODED_CAP: usize = 8 * 1024 * 1024;
fn payload(frame: &mut Frame) -> &mut AssistantImageRawFrame {
    let FrameData::AssistantImage(image) = &mut frame.data else {
        panic!("expected image");
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
async fn rejected(frame: Frame, diagnostic: &str) {
    let context = SharedContext::default();
    let initial = context.snapshot_rich().unwrap();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = consume(&assistant, frame);
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert_eq!(result.unwrap_err().message, diagnostic);
    assert_eq!(after.unwrap(), initial);
}

#[tokio::test]
async fn display_selection_cross_product_keeps_truthy_originals_without_rescue() {
    for (display, mime, base64, original, original_mime, original_base64) in [
        (
            PNG,
            "image/png",
            PNG_BASE64,
            JPEG,
            "image/jpeg",
            JPEG_BASE64,
        ),
        (
            JPEG,
            "image/jpeg",
            JPEG_BASE64,
            PNG,
            "image/png",
            PNG_BASE64,
        ),
    ] {
        for data in [None, Some(vec![]), Some(original.to_vec())] {
            for selected_mime in [None, Some(""), Some(original_mime)] {
                let originals_win = data.as_ref().is_some_and(|data| !data.is_empty())
                    && selected_mime.is_some_and(|mime| !mime.is_empty());
                let mut frame = image_frame(display, mime);
                payload(&mut frame).original_data = data.clone();
                payload(&mut frame).original_mime_type = selected_mime.map(str::to_owned);
                let rich = admitted(frame).await;
                if originals_win {
                    assert_image(&rich.messages[0], original, original_mime, original_base64);
                } else {
                    assert_image(&rich.messages[0], display, mime, base64);
                }
            }
        }
        for (data, bad_mime) in [
            (vec![1, 2, 3], original_mime),
            (original.to_vec(), mime),
            (original.to_vec(), "RGB"),
            (original.to_vec(), " "),
        ] {
            let mut frame = image_frame(display, mime);
            payload(&mut frame).original_data = Some(data);
            payload(&mut frame).original_mime_type = Some(bad_mime.into());
            rejected(
                frame,
                "assistant image requires image/png or image/jpeg MIME with a matching signature",
            )
            .await;
        }
    }
}

#[tokio::test]
async fn unselected_original_and_display_storage_is_ignored() {
    for (display, mime, base64) in [
        (PNG, "image/png", PNG_BASE64),
        (JPEG, "image/jpeg", JPEG_BASE64),
    ] {
        for original_mime in [None, Some("")] {
            let mut frame = image_frame(display, mime);
            payload(&mut frame).original_data = Some(vec![17; ENCODED_CAP + 1]);
            payload(&mut frame).original_mime_type = original_mime.map(str::to_owned);
            let rich = admitted(frame).await;
            assert_image(&rich.messages[0], display, mime, base64);
        }
        for data in [None, Some(vec![])] {
            let mut frame = image_frame(display, mime);
            payload(&mut frame).original_data = data;
            payload(&mut frame).original_mime_type = Some("x".repeat(ENCODED_CAP + 1));
            let rich = admitted(frame).await;
            assert_image(&rich.messages[0], display, mime, base64);
        }
        // Valid truthy originals ignore even over-cap, wrongly typed display storage.
        let mut frame = image_frame(&[], "not-a-format");
        payload(&mut frame).image.image.image = vec![17; ENCODED_CAP + 1];
        payload(&mut frame).original_data = Some(display.to_vec());
        payload(&mut frame).original_mime_type = Some(mime.into());
        let rich = admitted(frame).await;
        assert_image(&rich.messages[0], display, mime, base64);
    }
}

#[tokio::test]
async fn encoded_display_bounds_formats_and_diagnostics_are_selected_only() {
    for (signature, mime) in [
        (&b"\x89PNG\r\n\x1a\n"[..], "image/png"),
        (&b"\xff\xd8\xff"[..], "image/jpeg"),
    ] {
        // Signature-only and cap-sized positives are admission-policy controls,
        // not complete images and never passed to an image decoder.
        for length in [signature.len(), ENCODED_CAP] {
            let mut bytes = signature.to_vec();
            bytes.resize(length, 0x61);
            let rich = admitted(image_frame(&bytes, mime)).await;
            assert_image(&rich.messages[0], &bytes, mime, &STANDARD.encode(&bytes));
        }
        for length in [0, ENCODED_CAP + 1] {
            let mut bytes = signature.to_vec();
            bytes.resize(length, 0x61);
            rejected(
                image_frame(&bytes, mime),
                "assistant image image must contain 1..=8388608 bytes",
            )
            .await;
        }
        let mut bad_signature = signature.to_vec();
        bad_signature[0] ^= 1;
        rejected(
            image_frame(&bad_signature, mime),
            "assistant image requires image/png or image/jpeg MIME with a matching signature",
        )
        .await;
    }
    for (bytes, mode) in [
        (PNG, Some("image/jpeg")),
        (JPEG, Some("image/png")),
        (PNG, Some("image/PNG")),
        (JPEG, Some("image/jpg")),
        (JPEG, Some("IMAGE/JPEG")),
        (PNG, Some("image/png; charset=binary")),
        (PNG, Some("image/png ")),
        (PNG, Some(" image/png")),
        (PNG, Some("image/webp")),
        (PNG, None),
        (PNG, Some("")),
        (PNG, Some("RGBA")),
        (PNG, Some("rgb")),
        (PNG, Some("BGR")),
    ] {
        let mut frame = image_frame(bytes, "unused");
        payload(&mut frame).image.image.format = mode.map(str::to_owned);
        rejected(
            frame,
            "assistant image requires image/png or image/jpeg MIME with a matching signature",
        )
        .await;
    }
    // Truthy original over-cap diagnostic still names original_data, with no rescue.
    let mut original_over = image_frame(PNG, "image/png");
    payload(&mut original_over).original_data = Some(vec![0; ENCODED_CAP + 1]);
    payload(&mut original_over).original_mime_type = Some("image/png".into());
    rejected(
        original_over,
        "assistant image original_data must contain 1..=8388608 bytes",
    )
    .await;
}

#[tokio::test]
async fn malformed_display_is_atomic_same_id_repairable_and_seen_replay_is_silent() {
    let context = SharedContext::default();
    let initial = context.snapshot_rich().unwrap();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let first = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm("A"))),
    );
    let good = image_frame(PNG, "image/png");
    let mut bad = good.clone();
    payload(&mut bad).image.image.image = JPEG.to_vec();
    let rejection = consume(&assistant, bad.clone());
    let before = context.snapshot_rich();
    let repair = consume(&assistant, good.clone());
    let malformed_replay = consume(&assistant, bad);
    let mut bad_original_replay = good;
    payload(&mut bad_original_replay).original_data = Some(vec![1, 2, 3]);
    payload(&mut bad_original_replay).original_mime_type = Some(" ".into());
    let original_replay = consume(&assistant, bad_original_replay);
    let middle = context.snapshot_rich();
    let second = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm(" B"))),
    );
    let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let after = context.snapshot_rich();
    let native = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    start.unwrap();
    silent(first.unwrap());
    silent(second.unwrap());
    assert_eq!(
        rejection.unwrap_err().message,
        "assistant image requires image/png or image/jpeg MIME with a matching signature"
    );
    assert_eq!(before.unwrap(), initial);
    silent(repair.unwrap());
    silent(malformed_replay.unwrap());
    silent(original_replay.unwrap());
    let middle = middle.unwrap();
    assert_eq!(middle.messages.len(), 1);
    assert_image(&middle.messages[0], PNG, "image/png", PNG_BASE64);
    assert_eq!(
        end.unwrap().events,
        vec![AggregationEvent::AssistantTurnStopped {
            text: "A B".into(),
            interrupted: false
        }]
    );
    let after = after.unwrap();
    assert_eq!(after.messages.len(), 2);
    assert_eq!(after.messages[0], middle.messages[0]);
    assert_eq!(after.messages[1].role, ChatRole::Assistant);
    assert_eq!(
        after.messages[1].content,
        MessageContent::Text("A B".into())
    );
    gemini_contents(native.unwrap(), "image/png", PNG_BASE64, Some("A B"));
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
    continuation.data = image_frame(PNG, "image/png").data;
    let call = FunctionCall {
        function_name: "owned".into(),
        tool_call_id: "encoded-display-private".into(),
        arguments: json!({}),
    };
    let batch = NativeToolCallBatch::new(
        "encoded-display-group".into(),
        vec![(std::sync::Arc::new(call.clone()), false)],
    )
    .unwrap();
    let mut tool = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
    tool.set_native_tool_batch(batch).unwrap();
    tool.id = id;
    tool.data = image_frame(PNG, "image/png").data;
    [continuation, tool]
}

#[tokio::test]
async fn successful_encoded_display_identity_skips_bad_originals_but_not_private_authority() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let good = image_frame(PNG, "image/png");
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
async fn encoded_display_terminal_order_preserves_tool_revalidation_and_continuation_noop() {
    for terminal in [
        Frame::end(),
        Frame::cancel("fixture"),
        Frame::new(FrameData::Stop),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let good = image_frame(PNG, "image/png");
        let [continuation, tool] = private_images(good.id);
        let ended = consume(&assistant, terminal);
        let valid = consume(&assistant, good);
        let mut bad = image_frame(PNG, "image/png");
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
async fn encoded_display_realtime_guard_precedes_encoding_and_seen_identity() {
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
        let image = image_frame(PNG, "image/png");
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
async fn encoded_display_rejects_active_and_completed_signed_owner_until_playback_closes() {
    use pipecat::turns::{AssistantAggregatorConfig, AssistantTextMode};
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    let seen = image_frame(PNG, "image/png");
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
    let unseen = image_frame(PNG, "image/png");
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
    assert_image(&after.messages[0], PNG, "image/png", PNG_BASE64);
    assert_eq!(
        after.messages[1].content,
        MessageContent::Text("heard".into())
    );
    assert_image(&after.messages[2], PNG, "image/png", PNG_BASE64);
}

#[tokio::test]
async fn encoded_display_preserves_completed_historical_signed_prefix_in_gemini_request() {
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
    let result = consume(&assistant, image_frame(PNG, "image/png"));
    let rich = context.snapshot_rich();
    let snapshot = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(result.unwrap());
    let rich = rich.unwrap();
    assert_eq!(rich.messages.len(), 2);
    assert_image(&rich.messages[1], PNG, "image/png", PNG_BASE64);
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
                {"inlineData":{"mimeType":"image/png", "data":PNG_BASE64}}]},
            {"role":"user", "parts":[{"text":"."}]}
        ])
    );
}
