//! Public encoded assistant-image ingress: application/codec -> context -> Gemini wire.
//! These two baseline regression tests are frozen before appending candidate controls.
use pipecat::{
    core::*,
    network_bus::PythonJsonCodec,
    services::{adapters::request_body, CompletionRequest, Provider},
    turns::{AggregationOutput, LLMAssistantAggregator, SharedContext},
    workers::MessageData,
};
use serde_json::json;
use std::time::Duration;

// Complete, CRC-correct 1x1 RGB PNG; display pixels below deliberately differ.
const PNG: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0,
    0, 0, 144, 119, 83, 222, 0, 0, 0, 12, 73, 68, 65, 84, 120, 156, 99, 248, 207, 192, 0, 0, 3, 1,
    1, 0, 201, 254, 146, 239, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];
const PNG_BASE64: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
fn image_frame(original: Option<Vec<u8>>, mime: Option<&str>) -> Frame {
    Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
        image: OutputImageRawFrame {
            image: ImageRawFrame {
                image: vec![1, 2, 3],
                size: (1, 1),
                format: Some("RGB".into()),
            },
            sync_with_audio: true,
        },
        original_data: original,
        original_mime_type: mime.map(str::to_owned),
    }))
}
fn png_frame() -> Frame {
    image_frame(Some(PNG.to_vec()), Some("image/png"))
}
fn consume(
    assistant: &LLMAssistantAggregator,
    frame: Frame,
) -> Result<AggregationOutput, ProcessorError> {
    assistant.process_at(frame, Direction::Downstream, Duration::ZERO)
}
fn assert_silent(output: AggregationOutput) {
    assert!(
        output.actions.is_empty(),
        "image emitted an action: {:?}",
        output.actions
    );
    assert!(
        output.events.is_empty(),
        "image changed the turn: {:?}",
        output.events
    );
}
fn assert_image_message(message: &ContextMessage, base64: &str, mime: &str) {
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
}
fn codec_png() -> (Frame, Direction) {
    let wire = json!({
        "__type__": "pipecat.bus.messages.BusFrameMessage",
        "__data__": {
            "source": "application-image", "target": "assistant-context",
            "frame": {"__type__": "pipecat.frames.frames.AssistantImageRawFrame", "__data__": {
                "id": 982101, "name": "AssistantImageRawFrame#context",
                "metadata": {"tool_call_id": "untrusted-label", "run_llm": true},
                "image": {"__type__": "bytes", "__data__": "AQID"},
                "size": [1, 1], "format": "RGB", "sync_with_audio": true,
                "original_data": {"__type__": "bytes", "__data__": PNG_BASE64},
                "original_mime_type": "image/png"
            }},
            "direction": {"__type__": "pipecat.processors.frame_processor.FrameDirection", "__data__": "DOWNSTREAM"}
        }
    });
    let envelope = PythonJsonCodec::default()
        .decode(&serde_json::to_vec(&wire).unwrap())
        .unwrap();
    let MessageData::Frame {
        frame, direction, ..
    } = &envelope.as_bus_message().unwrap().data
    else {
        panic!("codec did not produce a frame")
    };
    ((**frame).clone(), *direction)
}

#[tokio::test]
async fn encoded_assistant_image_idle_appends_original_without_actions() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = consume(&assistant, png_frame());
    let snapshot = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert_silent(result.expect("encoded originals must be admitted"));
    let snapshot = snapshot.unwrap();
    assert_eq!(snapshot.messages.len(), 1);
    assert_image_message(&snapshot.messages[0], PNG_BASE64, "image/png");
}

#[tokio::test]
async fn python_codec_assistant_image_reaches_gemini_model_inline_data() {
    let (frame, direction) = codec_png();
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = assistant.process_at(frame, direction, Duration::ZERO);
    let rich = context.snapshot_rich();
    let frame_data = context.frame_data();
    let snapshot = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert_silent(result.expect("codec-produced originals must be admitted"));
    let rich = rich.unwrap();
    assert_eq!(rich.messages.len(), 1);
    assert_image_message(&rich.messages[0], PNG_BASE64, "image/png");
    let FrameData::LlmContextDetailed(projected) = frame_data.unwrap() else {
        panic!("image context must retain its rich representation")
    };
    assert_eq!(projected.context, rich);
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("wire-fixture", snapshot.unwrap()),
    )
    .unwrap();
    assert_eq!(
        body["contents"],
        json!([
            {"role": "model", "parts": [{"inlineData": {
                "mimeType": "image/png", "data": PNG_BASE64
            }}]},
            {"role": "user", "parts": [{"text": "."}]}
        ])
    );
}

// Candidate controls appended only after the two frozen baseline tests.
#[tokio::test]
async fn image_preserves_buffered_text_order_and_turn_events() {
    use pipecat::turns::AggregationEvent;
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let first = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm("before"))),
    );
    let image = consume(&assistant, png_frame());
    let during = context.snapshot_rich();
    let second = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm(" after"))),
    );
    let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert_eq!(
        start.unwrap().events,
        vec![AggregationEvent::AssistantTurnStarted]
    );
    assert_silent(first.unwrap());
    assert_silent(image.unwrap());
    assert_silent(second.unwrap());
    let during = during.unwrap();
    assert_eq!(
        during.messages.len(),
        1,
        "image must not flush prior buffered text"
    );
    assert_image_message(&during.messages[0], PNG_BASE64, "image/png");
    assert_eq!(
        end.unwrap().events,
        vec![AggregationEvent::AssistantTurnStopped {
            text: "before after".into(),
            interrupted: false,
        }]
    );
    let after = after.unwrap();
    assert_eq!(after.messages.len(), 2);
    assert_eq!(after.messages[0], during.messages[0]);
    assert_eq!(
        after.messages[1].content,
        MessageContent::Text("before after".into())
    );
}

#[tokio::test]
async fn invalid_originals_leave_context_unchanged_and_same_id_retry_works() {
    let invalid = [
        (None, None),
        (None, Some("image/png")),
        (Some(vec![]), Some("image/png")),
        (Some(PNG.to_vec()), None),
        (Some(PNG.to_vec()), Some("")),
        (Some(PNG.to_vec()), Some("application/octet-stream")),
        (Some(PNG.to_vec()), Some("image/webp")),
        (Some(PNG.to_vec()), Some("image/PNG")),
        (Some(PNG.to_vec()), Some("image/png; charset=binary")),
        (Some(PNG.to_vec()), Some("image/jpeg")),
        (Some(vec![255, 216, 255]), Some("image/png")),
        (Some(vec![1, 2, 3]), Some("image/png")),
        (Some(vec![137, 80, 78, 71]), Some("image/png")),
    ];
    for (original, mime) in invalid {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let bad = image_frame(original, mime);
        let mut fixed = png_frame();
        fixed.id = bad.id;
        let rejected = consume(&assistant, bad);
        let unchanged = context.snapshot_rich();
        let retried = consume(&assistant, fixed);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected.is_err(), "invalid original descriptor admitted");
        assert!(unchanged.unwrap().messages.is_empty());
        assert_silent(retried.expect("validation failure poisoned frame ID"));
        let after = after.unwrap();
        assert_eq!(after.messages.len(), 1);
        assert_image_message(&after.messages[0], PNG_BASE64, "image/png");
    }
}

#[tokio::test]
async fn successful_image_replay_uses_shared_bounded_frame_identity_cache() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let image = png_frame();
    let first = consume(&assistant, image.clone());
    let repeat = consume(&assistant, image.clone());
    // A retyped clone cannot re-use an admitted identity to append another family.
    let mut retyped = image;
    retyped.data = FrameData::LlmMessagesAppendDetailed(DetailedMessagesFrame {
        messages: vec![ContextMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Text("should be ignored".into()),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
            extra: Default::default(),
        }],
        run_llm: Some(true),
    });
    let cross_family = consume(&assistant, retyped);
    // The reverse collision also obeys the same cache: a previously consumed text ID.
    let text = Frame::new(FrameData::LlmText(TextFrame::llm("idle text")));
    let mut text_as_image = png_frame();
    text_as_image.id = text.id;
    let text_result = consume(&assistant, text);
    let reverse = consume(&assistant, text_as_image);
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    for result in [first, repeat, cross_family, text_result, reverse] {
        assert_silent(result.unwrap());
    }
    assert_eq!(after.unwrap().messages.len(), 1);
}

#[tokio::test]
async fn terminal_state_consumes_images_without_mutating_context() {
    for terminal in [
        Frame::end(),
        Frame::cancel("test"),
        Frame::new(FrameData::Stop),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let ended = consume(&assistant, terminal);
        let image = consume(&assistant, png_frame());
        let invalid_after_end = consume(&assistant, image_frame(None, None));
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        ended.unwrap();
        assert_silent(image.unwrap());
        assert_silent(invalid_after_end.unwrap());
        assert!(after.unwrap().messages.is_empty());
    }
}

#[tokio::test]
async fn jpeg_originals_ignore_display_dimensions_and_upstream_direction() {
    let original = include_bytes!("../../pipecat-images/tests/fixtures/source-cat.jpg");
    let mut frame = image_frame(Some(original.to_vec()), Some("image/jpeg"));
    if let FrameData::AssistantImage(image) = &mut frame.data {
        image.image.image.image.clear();
        image.image.image.size = (0, 0);
        image.image.image.format = Some("unrelated-display-format".into());
    }
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        pipecat::turns::AssistantAggregatorConfig {
            text_mode: pipecat::turns::AssistantTextMode::Spoken,
        },
    );
    let result = assistant.process_at(frame, Direction::Upstream, Duration::ZERO);
    let snapshot = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert_silent(result.unwrap());
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("wire-fixture", snapshot.unwrap()),
    )
    .unwrap();
    assert_eq!(body["contents"][0]["role"], "model");
    assert_eq!(
        body["contents"][0]["parts"][0]["inlineData"]["mimeType"],
        "image/jpeg"
    );
    let data = body["contents"][0]["parts"][0]["inlineData"]["data"]
        .as_str()
        .unwrap();
    // Reuse the public codec's byte decoder to compare exact original JPEG bytes.
    let wire = json!({"__type__":"pipecat.bus.messages.BusFrameMessage", "__data__":{
        "source":"verify", "target":"verify", "direction":{"__type__":"pipecat.processors.frame_processor.FrameDirection","__data__":"DOWNSTREAM"},
        "frame":{"__type__":"pipecat.frames.frames.AssistantImageRawFrame","__data__":{
            "id":982111,"name":"verify", "image":{"__type__":"bytes","__data__":"AQID"}, "size":[1,1], "format":"RGB",
            "original_data":{"__type__":"bytes","__data__":data}, "original_mime_type":"image/jpeg"
        }}
    }});
    let decoded = PythonJsonCodec::default()
        .decode(&serde_json::to_vec(&wire).unwrap())
        .unwrap();
    let MessageData::Frame { frame, .. } = &decoded.as_bus_message().unwrap().data else {
        panic!()
    };
    let FrameData::AssistantImage(image) = &frame.data else {
        panic!()
    };
    assert_eq!(image.original_data.as_deref(), Some(original.as_slice()));
}

#[tokio::test]
async fn original_byte_limit_is_inclusive_and_failures_do_not_poison_retry() {
    const LIMIT: usize = 8 * 1024 * 1024;
    // Signature-only boundary fixture: this test establishes the admission limit,
    // not decoding, pixel validity, or live-provider acceptance.
    let mut bytes = vec![0; LIMIT + 1];
    bytes[..8].copy_from_slice(&PNG[..8]);
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let oversized = image_frame(Some(bytes), Some("image/png"));
    let id = oversized.id;
    let rejected = consume(&assistant, oversized);
    let unchanged = context.snapshot_rich();
    let mut bytes = vec![0; LIMIT];
    bytes[..8].copy_from_slice(&PNG[..8]);
    let mut boundary = image_frame(Some(bytes), Some("image/png"));
    boundary.id = id;
    let accepted = consume(&assistant, boundary);
    let snapshot = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert!(rejected.unwrap_err().message.contains("8388608"));
    assert!(unchanged.unwrap().messages.is_empty());
    assert_silent(accepted.unwrap());
    let snapshot = snapshot.unwrap();
    assert_eq!(snapshot.messages.len(), 1);
    let MessageContent::Parts(parts) = &snapshot.messages[0].content else {
        panic!()
    };
    let MessagePart::Image { url, .. } = &parts[0] else {
        panic!()
    };
    assert_eq!(url.len(), 22 + 4 * LIMIT.div_ceil(3));
    assert!(url.starts_with("data:image/png;base64,iVBORw0KGgo"));
    assert!(url.ends_with("AAA="));
}

fn signed_boundary(data: FrameData, history: NativeLlmContinuation) -> Frame {
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(std::sync::Arc::new(history))
        .unwrap();
    frame
}
#[tokio::test]
async fn local_signed_overlap_rejects_image_without_breaking_legal_completion() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let start = consume(
        &assistant,
        signed_boundary(
            FrameData::LlmResponseStart,
            NativeLlmContinuation::start(982121, LlmContinuationProvider::Anthropic).unwrap(),
        ),
    );
    let text = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm("signed text"))),
    );
    let image = png_frame();
    let before = context.snapshot_rich();
    let rejected = consume(&assistant, image.clone());
    let after_rejection = context.snapshot_rich();
    let end = consume(
        &assistant,
        signed_boundary(
            FrameData::LlmResponseEnd,
            NativeLlmContinuation::new(
                982121,
                LlmContinuationProvider::Anthropic,
                vec![LlmContinuationBlock::Text("signed text".into())],
            )
            .unwrap(),
        ),
    );
    let after_end = context.snapshot_rich();
    let retry = consume(&assistant, image);
    let after_retry = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    start.unwrap();
    text.unwrap();
    assert!(rejected
        .unwrap_err()
        .message
        .contains("signed continuation"));
    assert_eq!(before.unwrap(), after_rejection.unwrap());
    end.unwrap();
    let after_end = after_end.unwrap();
    assert_eq!(after_end.messages.len(), 1);
    assert_eq!(
        after_end.messages[0].content,
        MessageContent::Text("signed text".into())
    );
    assert_silent(retry.expect("closed continuation must permit corrected retry"));
    let after_retry = after_retry.unwrap();
    assert_eq!(after_retry.messages.len(), 2);
    assert_eq!(after_retry.messages[0], after_end.messages[0]);
    assert_image_message(&after_retry.messages[1], PNG_BASE64, "image/png");
}

#[tokio::test]
async fn realtime_enabled_and_auto_announced_images_reject_without_context_changes() {
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
        let announced = if mode == RealtimeServiceMode::Auto {
            Some(consume(
                &assistant,
                Frame::new(FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
                    base: ServiceMetadataFrame {
                        service_name: "realtime-test".into(),
                        user_turn_strategies: None,
                    },
                    is_realtime_service: true,
                })),
            ))
        } else {
            None
        };
        let rejected = consume(&assistant, png_frame());
        let snapshot = pair.context().snapshot_rich();
        let assistant_cleaned = assistant.cleanup().await;
        let user_cleaned = pair.user().cleanup().await;
        drop(assistant);
        drop(pair);
        assistant_cleaned.unwrap();
        user_cleaned.unwrap();
        if let Some(announced) = announced {
            announced.unwrap();
        }
        assert!(rejected.unwrap_err().message.contains("realtime"));
        assert!(snapshot.unwrap().messages.is_empty());
    }
}

#[tokio::test]
async fn retyped_private_continuation_and_tool_batch_reject_before_admission() {
    use std::sync::Arc;
    let mut continuation = signed_boundary(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(982131, LlmContinuationProvider::Anthropic).unwrap(),
    );
    continuation.data = png_frame().data;
    let call = FunctionCall {
        function_name: "owned".into(),
        tool_call_id: "owned-id".into(),
        arguments: json!({}),
    };
    let batch =
        NativeToolCallBatch::new("owned-group".into(), vec![(Arc::new(call.clone()), false)])
            .unwrap();
    let mut declaration = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
    declaration.set_native_tool_batch(batch).unwrap();
    declaration.data = png_frame().data;
    for malformed in [continuation, declaration] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut retry = png_frame();
        retry.id = malformed.id;
        let rejected = consume(&assistant, malformed);
        let before = context.snapshot_rich();
        let pending = assistant.has_function_calls_in_progress();
        let admitted = consume(&assistant, retry);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected.is_err());
        assert!(before.unwrap().messages.is_empty());
        assert!(!pending.unwrap());
        assert_silent(admitted.expect("private carrier rejection mutated state"));
        assert_eq!(after.unwrap().messages.len(), 1);
    }
}

#[tokio::test]
async fn completed_signed_prefix_is_preserved_by_ordinary_image_append() {
    use pipecat::services::{Content, ContentPart, Context, Message, Role};
    let prefix = Message {
        role: Role::Assistant,
        content: Some(Content::Parts(vec![ContentPart::GeminiText {
            text: "prior signed answer".into(),
            thought: false,
            signature: Some("opaque-signature".into()),
            incomplete: false,
        }])),
        name: None,
        tool_calls: vec![],
        tool_call_id: None,
    };
    let context = SharedContext::new(Context::new(vec![prefix.clone()]));
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let accepted = consume(&assistant, png_frame());
    let snapshot = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert_silent(accepted.unwrap());
    let snapshot = snapshot.unwrap();
    assert_eq!(snapshot.messages.len(), 2);
    assert_eq!(snapshot.messages[0], prefix);
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("wire-fixture", snapshot),
    )
    .unwrap();
    assert_eq!(
        body["contents"],
        json!([
            {"role": "model", "parts": [
                {"text": "prior signed answer", "thoughtSignature": "opaque-signature"},
                {"inlineData": {"mimeType": "image/png", "data": PNG_BASE64}}
            ]},
            {"role": "user", "parts": [{"text": "."}]}
        ])
    );
}

#[tokio::test]
async fn passive_user_images_consume_rgb_appends_and_authority_remains_unsupported() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    // Passive RGB carries no content to encode; the bounded RGB UserImage path
    // accepts this legal 1x1 input. Every authority-bearing request still rejects.
    for append in [None, Some(false), Some(true)] {
        for run in [None, Some(false), Some(true)] {
            for requested in [false, true] {
                let calls = Arc::new(AtomicUsize::new(0));
                let target = calls.clone();
                let callback = LocalResultCallback::new(move |_| {
                    target.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                });
                let context = SharedContext::default();
                let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
                let frame = Frame::new(FrameData::UserImage(UserImageRawFrame {
                    image: ImageRawFrame {
                        image: vec![1, 2, 3],
                        size: (1, 1),
                        format: Some("RGB".into()),
                    },
                    user_id: "speaker".into(),
                    text: Some("describe".into()),
                    append_to_context: append,
                    run_llm: run,
                    request: requested.then(|| {
                        Box::new(UserImageRequestFrame {
                            user_id: "speaker".into(),
                            text: Some("describe".into()),
                            append_to_context: append,
                            video_source: None,
                            function_name: Some("capture".into()),
                            tool_call_id: Some("unowned".into()),
                            result_callback: Some(callback),
                        })
                    }),
                }));
                let result = consume(&assistant, frame);
                let snapshot = context.snapshot_rich();
                let cleaned = assistant.cleanup().await;
                drop(assistant);
                cleaned.unwrap();
                let snapshot = snapshot.unwrap();
                if requested {
                    assert!(result
                        .unwrap_err()
                        .message
                        .contains("raw image/file context conversion"));
                    assert!(snapshot.messages.is_empty());
                } else if append != Some(true) {
                    assert_silent(result.unwrap());
                    assert!(snapshot.messages.is_empty());
                } else {
                    let output = result.unwrap();
                    assert!(output.events.is_empty());
                    if run == Some(false) {
                        assert!(output.actions.is_empty());
                    } else {
                        assert_eq!(output.actions.len(), 1);
                        let pipecat::turns::AggregationAction::Push(directed) = &output.actions[0]
                        else {
                            panic!("expected context")
                        };
                        assert_eq!(directed.direction, Direction::Upstream);
                        let FrameData::LlmContextDetailed(emitted) = &directed.frame.data else {
                            panic!("expected detailed context")
                        };
                        assert_eq!(emitted.context, snapshot);
                    }
                    assert_eq!(snapshot.messages.len(), 1);
                    assert_eq!(snapshot.messages[0].role, ChatRole::User);
                    let MessageContent::Parts(parts) = &snapshot.messages[0].content else {
                        panic!("expected text and image")
                    };
                    assert_eq!(parts.len(), 2);
                    assert_eq!(
                        parts[0],
                        MessagePart::Text {
                            text: "describe".into()
                        }
                    );
                    let MessagePart::Image { url, detail } = &parts[1] else {
                        panic!("expected JPEG")
                    };
                    assert!(url.starts_with("data:image/jpeg;base64,"));
                    assert!(detail.is_none());
                }
                assert_eq!(calls.load(Ordering::SeqCst), 0);
            }
        }
    }
}

#[tokio::test]
async fn image_replay_expires_with_existing_512_identity_window() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let image = png_frame();
    let first = consume(&assistant, image.clone());
    let mut filler = Vec::new();
    for _ in 0..512 {
        filler.push(consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm("idle"))),
        ));
    }
    let expired = consume(&assistant, image);
    let snapshot = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert_silent(first.unwrap());
    for result in filler {
        assert_silent(result.unwrap());
    }
    assert_silent(expired.unwrap());
    assert_eq!(
        snapshot.unwrap().messages.len(),
        2,
        "dedup is bounded, not lifetime exactly-once"
    );
}

#[tokio::test]
async fn terminal_retyped_carriers_keep_existing_validation_order() {
    use std::sync::Arc;
    let mut continuation = signed_boundary(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(982151, LlmContinuationProvider::Anthropic).unwrap(),
    );
    continuation.data = png_frame().data;
    let call = FunctionCall {
        function_name: "owned".into(),
        tool_call_id: "owned-terminal".into(),
        arguments: json!({}),
    };
    let batch = NativeToolCallBatch::new(
        "terminal-group".into(),
        vec![(Arc::new(call.clone()), false)],
    )
    .unwrap();
    let mut declaration = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
    declaration.set_native_tool_batch(batch).unwrap();
    declaration.data = png_frame().data;
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let ended = consume(&assistant, Frame::end());
    let continuation_ignored = consume(&assistant, continuation);
    let declaration_rejected = consume(&assistant, declaration);
    let snapshot = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    ended.unwrap();
    assert_silent(continuation_ignored.unwrap());
    assert!(declaration_rejected.is_err());
    assert!(snapshot.unwrap().messages.is_empty());
}

#[tokio::test]
async fn legacy_user_files_remain_explicitly_unsupported() {
    let file = FileRawFrame {
        file: "data:application/pdf;base64,JVBERg==".into(),
        source_type: FileSourceType::Bytes,
        filename: Some("document.pdf".into()),
        format: "application/pdf".into(),
    };
    let data = FrameData::UserFile(UserFileRawFrame {
        file,
        user_id: "user".into(),
    });
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let rejected = consume(&assistant, Frame::new(data));
    let snapshot = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert!(rejected
        .unwrap_err()
        .message
        .contains("raw image/file context conversion"));
    assert!(snapshot.unwrap().messages.is_empty());
}

struct PublishReceipt;
#[async_trait]
impl FrameProcessor for PublishReceipt {
    fn name(&self) -> &str {
        "encoded-image-test-receipt-publisher"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            let receipt = ctx
                .tool_outcome_publisher(
                    FunctionCall {
                        function_name: "fixture".into(),
                        tool_call_id: "receipt-id".into(),
                        arguments: json!({}),
                    },
                    false,
                )?
                .complete(
                    Ok(json!({"fixture":true})),
                    ToolCallEffectStatus::NotStarted,
                    ToolCallEndReason::CallbackReturned,
                );
            if receipt.admission != ToolOutcomeAdmission::Admitted {
                return Err(ProcessorError::new("fixture receipt was not admitted"));
            }
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}
struct CaptureReceipt(tokio::sync::mpsc::Sender<Frame>);
#[async_trait]
impl FrameProcessor for CaptureReceipt {
    fn name(&self) -> &str {
        "encoded-image-test-receipt-capture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.native_tool_outcome()?.is_some() {
            self.0
                .send(frame)
                .await
                .map_err(|_| ProcessorError::new("fixture receiver closed"))?;
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}
async fn receipt_fixture() -> Result<Frame, String> {
    use std::sync::Arc;
    const WAIT: Duration = Duration::from_secs(5);
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    // Let the pipeline's own startup deadline complete its cleanup before
    // returning an error; an outer timeout could drop its cleanup future.
    let mut worker = Pipeline::new(vec![Arc::new(PublishReceipt), Arc::new(CaptureReceipt(tx))])
        .start(PipelineConfig {
            startup_timeout: WAIT,
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    let operation = async {
        tokio::time::timeout(
            WAIT,
            worker.sender().send(
                Frame::new(FrameData::Text(TextFrame::llm("publish"))),
                Direction::Downstream,
            ),
        )
        .await
        .map_err(|_| "fixture send deadline".to_string())?
        .map_err(|e| e.to_string())?;
        tokio::time::timeout(WAIT, rx.recv())
            .await
            .map_err(|_| "fixture receipt deadline".to_string())?
            .ok_or_else(|| "fixture receipt channel closed".to_string())
    }
    .await;
    // No behavior assertion or early return before worker shutdown/join attempts.
    let end = tokio::time::timeout(WAIT, worker.end()).await;
    if !matches!(&end, Ok(Ok(()))) {
        let _ = tokio::time::timeout(WAIT, worker.cancel("fixture end failed")).await;
    }
    let join = tokio::time::timeout(WAIT, worker.join()).await;
    if !matches!(&join, Ok(Ok(_))) {
        let _ = tokio::time::timeout(WAIT, worker.cancel("fixture join failed")).await;
        let _ = tokio::time::timeout(WAIT, worker.join()).await;
    }
    let report = join
        .map_err(|_| "fixture join deadline".to_string())?
        .map_err(|e| e.to_string())?;
    end.map_err(|_| "fixture end deadline".to_string())?
        .map_err(|e| e.to_string())?;
    if report.state != WorkerState::Ended || !report.errors.is_empty() {
        return Err(format!("fixture shutdown: {report:?}"));
    }
    operation
}
#[tokio::test]
async fn retyped_native_settlement_rejects_before_tool_history_mutation() {
    let terminal = receipt_fixture()
        .await
        .expect("public receipt fixture failed after cleanup");
    let mut retyped = terminal.clone();
    retyped.data = png_frame().data;
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let rejected = consume(&assistant, retyped.clone());
    let unchanged = context.snapshot_rich();
    let pending = assistant.has_function_calls_in_progress();
    let legal = consume(&assistant, terminal);
    let completed = context.snapshot_rich();
    let ended = consume(&assistant, Frame::end());
    let terminal_rejected = consume(&assistant, retyped);
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert!(rejected.is_err());
    assert!(unchanged.unwrap().messages.is_empty());
    assert!(!pending.unwrap());
    legal.unwrap();
    ended.unwrap();
    let completed = completed.unwrap();
    assert_eq!(completed.messages.len(), 2);
    assert_eq!(
        completed.messages[0].tool_calls[0].tool_call_id,
        "receipt-id"
    );
    assert_eq!(
        completed.messages[1].tool_call_id.as_deref(),
        Some("receipt-id")
    );
    assert!(
        terminal_rejected.is_err(),
        "existing tool-carrier validation precedes terminal no-op"
    );
}

#[tokio::test]
async fn completed_signed_response_still_rejects_image_until_playback_tracker_closes() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        pipecat::turns::AssistantAggregatorConfig {
            text_mode: pipecat::turns::AssistantTextMode::Spoken,
        },
    );
    let start = consume(
        &assistant,
        signed_boundary(
            FrameData::LlmResponseStart,
            NativeLlmContinuation::start(982161, LlmContinuationProvider::Anthropic).unwrap(),
        ),
    );
    let playback = assistant.begin_playback("held");
    let generated = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm("heard"))),
    );
    let end = consume(
        &assistant,
        signed_boundary(
            FrameData::LlmResponseEnd,
            NativeLlmContinuation::new(
                982161,
                LlmContinuationProvider::Anthropic,
                vec![LlmContinuationBlock::Text("heard".into())],
            )
            .unwrap(),
        ),
    );
    let image = png_frame();
    let before = context.snapshot_rich();
    let rejected = consume(&assistant, image.clone());
    let after_rejection = context.snapshot_rich();
    let receipt = assistant.confirm_playback(
        "held",
        &[AggregatedTextFrame {
            text: TextFrame::llm("heard"),
            raw_text: Some("heard".into()),
            context_id: Some("held".into()),
            aggregated_by: "playback-fixture".into(),
            will_be_spoken: true,
        }],
    );
    let retried = consume(&assistant, image);
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    start.unwrap();
    playback.unwrap();
    generated.unwrap();
    end.unwrap();
    assert!(rejected
        .unwrap_err()
        .message
        .contains("signed continuation"));
    assert_eq!(before.unwrap(), after_rejection.unwrap());
    receipt.unwrap();
    assert_silent(retried.unwrap());
    let after = after.unwrap();
    assert_eq!(after.messages.len(), 2);
    assert_eq!(
        after.messages[0].content,
        MessageContent::Text("heard".into())
    );
    assert_image_message(&after.messages[1], PNG_BASE64, "image/png");
}
