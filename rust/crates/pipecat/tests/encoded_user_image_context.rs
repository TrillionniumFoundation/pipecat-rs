//! Frozen public-API baseline: real RTVI upload -> context -> Gemini request body.
//! Capture and reducer are deliberate separate stages; no provider call is made.
use pipecat::{
    core::*,
    rtvi::{RtviConfig, RtviProcessor},
    services::{adapters::request_body, CompletionRequest, Provider},
    turns::LLMAssistantAggregator,
    turns::SharedContext,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

const PNG: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 2, 0,
    0, 0, 144, 119, 83, 222, 0, 0, 0, 12, 73, 68, 65, 84, 120, 156, 99, 248, 207, 192, 0, 0, 3, 1,
    1, 0, 201, 254, 146, 239, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];
const PNG_BASE64: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";

#[tokio::test]
async fn idle_encoded_user_image_appends_user_text_then_image_without_actions() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let accepted = assistant.process_at(
        Frame::new(FrameData::UserImage(UserImageRawFrame {
            image: ImageRawFrame {
                image: PNG.to_vec(),
                size: (0, 0),
                format: Some("image/png".into()),
            },
            user_id: String::new(),
            text: Some("inspect".into()),
            append_to_context: Some(true),
            request: None,
            run_llm: Some(false),
        })),
        Direction::Downstream,
        Duration::ZERO,
    );
    let snapshot = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    let accepted = accepted.expect("idle explicit-false encoded upload must be admitted");
    assert!(accepted.actions.is_empty());
    assert!(accepted.events.is_empty());
    assert_eq!(
        snapshot.unwrap().messages,
        vec![ContextMessage {
            role: ChatRole::User,
            content: MessageContent::Parts(vec![
                MessagePart::Text {
                    text: "inspect".into()
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
        }]
    );
}

async fn rtvi_upload() -> Result<Frame, String> {
    const WAIT: Duration = Duration::from_secs(5);
    let rtvi = RtviProcessor::new(RtviConfig::default()).map_err(|e| e.to_string())?;
    let mut worker = Pipeline::new(vec![Arc::new(rtvi)])
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
                Frame::new(FrameData::InputTransportMessage(json!({
                    "label": "rtvi-ai", "id": "encoded-user-image-minimal", "type": "send-file",
                    "data": {
                        "content": "inspect",
                        "file": {"format": "image/png", "source": {"type": "bytes", "bytes": PNG_BASE64}},
                        "options": {"run_immediately": false, "audio_response": true}
                    }
                }))),
                Direction::Downstream,
            ),
        )
        .await
        .map_err(|_| "RTVI send deadline".to_string())?
        .map_err(|e| e.to_string())?;
        tokio::time::timeout(WAIT, async {
            while let Some(directed) = worker.recv().await {
                if matches!(directed.frame.data, FrameData::Interruption) {
                    return Err("false-run upload unexpectedly interrupted".to_string());
                }
                if matches!(directed.frame.data, FrameData::UserImage(_)) {
                    return Ok(directed.frame);
                }
            }
            Err("RTVI closed without a user image".to_string())
        })
        .await
        .map_err(|_| "RTVI image deadline".to_string())?
    }
    .await;
    // Always attempt end/join, including on send/capture failure, before returning.
    let end = tokio::time::timeout(WAIT, worker.end()).await;
    if !matches!(&end, Ok(Ok(()))) {
        let _ = tokio::time::timeout(WAIT, worker.cancel("RTVI fixture end failed")).await;
    }
    let join = tokio::time::timeout(WAIT, worker.join()).await;
    if !matches!(&join, Ok(Ok(_))) {
        let _ = tokio::time::timeout(WAIT, worker.cancel("RTVI fixture join failed")).await;
        let _ = tokio::time::timeout(WAIT, worker.join()).await;
    }
    let report = join
        .map_err(|_| "RTVI join deadline".to_string())?
        .map_err(|e| e.to_string())?;
    end.map_err(|_| "RTVI end deadline".to_string())?
        .map_err(|e| e.to_string())?;
    if report.state != WorkerState::Ended || !report.errors.is_empty() {
        return Err(format!("RTVI shutdown: {report:?}"));
    }
    operation
}

#[tokio::test]
async fn rtvi_send_file_encoded_user_image_reaches_exact_gemini_contents() {
    // The producer is fully shut down before any behavior assertion or reducer use.
    let frame = rtvi_upload()
        .await
        .expect("RTVI fixture failed after cleanup");
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let producer_data = frame.data.clone();
    let accepted = assistant.process_at(frame, Direction::Downstream, Duration::ZERO);
    let rich = context.snapshot_rich();
    let projected = context.frame_data();
    let snapshot = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    let FrameData::UserImage(image) = producer_data else {
        panic!("RTVI did not emit a user image")
    };
    assert_eq!(image.image.image, PNG);
    assert_eq!(image.image.size, (0, 0));
    assert_eq!(image.image.format.as_deref(), Some("image/png"));
    assert_eq!(image.text.as_deref(), Some("inspect"));
    assert_eq!(image.append_to_context, Some(true));
    assert_eq!(image.run_llm, Some(false));
    assert!(image.request.is_none());
    let accepted = accepted.expect("actual RTVI encoded upload must be admitted");
    assert!(accepted.actions.is_empty());
    assert!(accepted.events.is_empty());
    let FrameData::LlmContextDetailed(projected) = projected.unwrap() else {
        panic!("user image context must retain its rich representation")
    };
    assert_eq!(projected.context, rich.unwrap());
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("wire-fixture", snapshot.unwrap()),
    )
    .unwrap();
    assert_eq!(
        body["contents"],
        json!([
            {"role": "user", "parts": [
                {"text": "inspect"},
                {"inlineData": {"mimeType": "image/png", "data": PNG_BASE64}}
            ]}
        ])
    );
}

// Candidate-only controls. The preceding two baseline tests remain byte-frozen.
use pipecat::turns::{AggregationOutput, AssistantAggregatorConfig, AssistantTextMode};

fn upload(bytes: Vec<u8>, mime: Option<&str>, text: Option<&str>) -> Frame {
    Frame::new(FrameData::UserImage(UserImageRawFrame {
        image: ImageRawFrame {
            image: bytes,
            size: (0, 0),
            format: mime.map(str::to_owned),
        },
        user_id: "not-a-message-name".into(),
        text: text.map(str::to_owned),
        append_to_context: Some(true),
        request: None,
        run_llm: Some(false),
    }))
}
fn png_upload() -> Frame {
    upload(PNG.to_vec(), Some("image/png"), Some("inspect"))
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
        "unexpected actions: {:?}",
        output.actions
    );
    assert!(
        output.events.is_empty(),
        "unexpected events: {:?}",
        output.events
    );
}

#[tokio::test]
async fn upload_text_presence_and_jpeg_preserve_exact_user_contents_upstream() {
    for text in [None, Some(""), Some(" "), Some("inspect")] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut frame = upload(PNG.to_vec(), Some("image/png"), text);
        frame
            .metadata
            .insert("tool_call_id".into(), json!("untrusted"));
        let result = assistant.process_at(frame, Direction::Upstream, Duration::ZERO);
        let rich = context.snapshot_rich();
        let native = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(result.unwrap());
        let rich = rich.unwrap();
        assert_eq!(rich.messages.len(), 1);
        let mut parts = vec![];
        let mut wire = vec![];
        if let Some(text) = text.filter(|s| !s.is_empty()) {
            parts.push(MessagePart::Text { text: text.into() });
            wire.push(json!({"text": text}));
        }
        parts.push(MessagePart::Image {
            url: format!("data:image/png;base64,{PNG_BASE64}"),
            detail: None,
        });
        wire.push(json!({"inlineData": {"mimeType": "image/png", "data": PNG_BASE64}}));
        assert_eq!(
            rich.messages[0],
            ContextMessage {
                role: ChatRole::User,
                content: MessageContent::Parts(parts),
                name: None,
                tool_call_id: None,
                tool_calls: vec![],
                extra: Default::default(),
            }
        );
        let body = request_body(
            Provider::Gemini,
            &CompletionRequest::new("wire-fixture", native.unwrap()),
        )
        .unwrap();
        assert_eq!(body["contents"], json!([{"role": "user", "parts": wire}]));
    }
    let jpeg = include_bytes!("../../pipecat-images/tests/fixtures/source-cat.jpg");
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let result = assistant.process_at(
        upload(jpeg.to_vec(), Some("image/jpeg"), Some("inspect JPEG")),
        Direction::Upstream,
        Duration::ZERO,
    );
    let native = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(result.unwrap());
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("wire-fixture", native.unwrap()),
    )
    .unwrap();
    let data = body["contents"][0]["parts"][1]["inlineData"]["data"]
        .as_str()
        .unwrap();
    assert_eq!(
        body["contents"],
        json!([{"role": "user", "parts": [
            {"text": "inspect JPEG"}, {"inlineData": {"mimeType": "image/jpeg", "data": data}}
        ]}])
    );
    // Existing public codec decodes the wire base64 back to the complete fixture.
    let envelope = json!({"__type__": "pipecat.bus.messages.BusFrameMessage", "__data__": {
        "source": "verify", "target": "verify",
        "direction": {"__type__": "pipecat.processors.frame_processor.FrameDirection", "__data__": "DOWNSTREAM"},
        "frame": {"__type__": "pipecat.frames.frames.UserImageRawFrame", "__data__": {
            "id": 993101, "name": "verify", "image": {"__type__": "bytes", "__data__": data},
            "size": [0, 0], "format": "image/jpeg", "user_id": "", "append_to_context": true, "run_llm": false
        }}
    }});
    let decoded = pipecat::network_bus::PythonJsonCodec::default()
        .decode(&serde_json::to_vec(&envelope).unwrap())
        .unwrap();
    let pipecat::workers::MessageData::Frame { frame, .. } =
        &decoded.as_bus_message().unwrap().data
    else {
        panic!()
    };
    let FrameData::UserImage(image) = &frame.data else {
        panic!()
    };
    assert_eq!(image.image.image, jpeg.as_slice());
}

#[tokio::test]
async fn nonselected_upload_flags_reject_and_leave_same_id_retryable() {
    for append in [None, Some(false), Some(true)] {
        for run in [None, Some(false), Some(true)] {
            if append == Some(true) {
                continue;
            }
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let mut frame = png_upload();
            let id = frame.id;
            if let FrameData::UserImage(image) = &mut frame.data {
                image.append_to_context = append;
                image.run_llm = run;
            }
            let rejected = consume(&assistant, frame);
            let before = context.snapshot_rich();
            let mut corrected = png_upload();
            corrected.id = id;
            let retry = consume(&assistant, corrected);
            let after = context.snapshot_rich();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            assert!(rejected.is_err(), "append={append:?} run={run:?}");
            assert!(before.unwrap().messages.is_empty());
            silent(retry.unwrap());
            assert_eq!(after.unwrap().messages.len(), 1);
        }
    }
}

#[tokio::test]
async fn every_owned_request_rejects_without_callback_or_tool_settlement() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for (tool_id, pending, callback) in [
        (None, false, false),
        (Some("unknown"), false, true),
        (Some("owned"), true, true),
    ] {
        let callbacks = Arc::new(AtomicUsize::new(0));
        let calls = callbacks.clone();
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let started = pending.then(|| {
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
        let mut frame = png_upload();
        if let FrameData::UserImage(image) = &mut frame.data {
            image.request = Some(Box::new(UserImageRequestFrame {
                user_id: "speaker".into(),
                text: None,
                append_to_context: Some(true),
                video_source: None,
                function_name: Some("capture".into()),
                tool_call_id: tool_id.map(str::to_owned),
                result_callback: callback.then(|| {
                    LocalResultCallback::new(move |_| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async { Ok(()) }
                    })
                }),
            }));
        }
        let rejected = consume(&assistant, frame);
        let after = context.snapshot();
        let still_pending = assistant.has_function_calls_in_progress();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        if let Some(started) = started {
            started.unwrap();
        }
        assert!(rejected.is_err());
        assert_eq!(before.unwrap(), after.unwrap());
        assert_eq!(still_pending.unwrap(), pending);
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn invalid_upload_bytes_mime_and_overcap_fail_before_mutation_or_replay() {
    let mut oversized = vec![0; 8 * 1024 * 1024 + 1];
    oversized[..8].copy_from_slice(&PNG[..8]);
    for (bytes, mime) in [
        (vec![], Some("image/png")),
        (vec![1, 2, 3], Some("image/png")),
        (PNG[..4].to_vec(), Some("image/png")),
        (PNG.to_vec(), Some("image/jpeg")),
        (vec![255, 216, 255], Some("image/png")),
        (PNG.to_vec(), None),
        (PNG.to_vec(), Some("image/webp")),
        (PNG.to_vec(), Some("image/PNG")),
        (PNG.to_vec(), Some("image/png; charset=binary")),
        (PNG.to_vec(), Some("application/octet-stream")),
        (vec![1, 2, 3], Some("RGB")),
        (oversized, Some("image/png")),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let frame = upload(bytes, mime, Some("inspect"));
        let id = frame.id;
        let rejected = consume(&assistant, frame);
        let before = context.snapshot_rich();
        let mut corrected = png_upload();
        corrected.id = id;
        let retry = consume(&assistant, corrected.clone());
        let replay = consume(&assistant, corrected);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected.is_err(), "{mime:?}");
        assert!(before.unwrap().messages.is_empty());
        silent(retry.unwrap());
        silent(replay.unwrap());
        assert_eq!(after.unwrap().messages.len(), 1);
    }
}

#[tokio::test]
async fn image_replays_share_frame_ids_across_families_but_not_equal_bytes() {
    let assistant_image = || {
        Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
            image: OutputImageRawFrame {
                image: ImageRawFrame {
                    image: vec![],
                    size: (0, 0),
                    format: None,
                },
                sync_with_audio: false,
            },
            original_data: Some(PNG.to_vec()),
            original_mime_type: Some("image/png".into()),
        }))
    };
    let append = || {
        Frame::new(FrameData::LlmMessagesAppendDetailed(
            DetailedMessagesFrame {
                messages: vec![ContextMessage {
                    role: ChatRole::User,
                    content: MessageContent::Text("prior".into()),
                    name: None,
                    tool_call_id: None,
                    tool_calls: vec![],
                    extra: Default::default(),
                }],
                run_llm: Some(false),
            },
        ))
    };
    for (first, mut second) in [
        (png_upload(), assistant_image()),
        (assistant_image(), png_upload()),
        (png_upload(), append()),
        (append(), png_upload()),
    ] {
        second.id = first.id;
        let mut malformed_replay = upload(vec![], Some("image/png"), None);
        malformed_replay.id = first.id;
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let admitted = consume(&assistant, first.clone());
        let before = context.snapshot_rich();
        let replay = consume(&assistant, first);
        let collision = consume(&assistant, second);
        let invalid = consume(&assistant, malformed_replay);
        let unchanged = context.snapshot_rich();
        let distinct = consume(&assistant, png_upload());
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        for result in [admitted, replay, collision, distinct] {
            silent(result.unwrap());
        }
        assert!(
            invalid.is_err(),
            "successful ID must not bypass upload validation"
        );
        assert_eq!(before.unwrap(), unchanged.unwrap());
        assert_eq!(after.unwrap().messages.len(), 2);
    }
}

#[tokio::test]
async fn active_upload_rejection_preserves_buffered_text_and_normal_turn_end() {
    for buffered in [false, true] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let text = buffered.then(|| {
            consume(
                &assistant,
                Frame::new(FrameData::LlmText(TextFrame::llm("before"))),
            )
        });
        let frame = png_upload();
        let rejected = consume(&assistant, frame.clone());
        let unchanged = context.snapshot_rich();
        let suffix = buffered.then(|| {
            consume(
                &assistant,
                Frame::new(FrameData::LlmText(TextFrame::llm(" after"))),
            )
        });
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
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
async fn speaking_pending_inference_tools_thought_and_playback_are_not_idle() {
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
        let rejected = consume(&assistant, png_upload());
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
    let playback = assistant.begin_playback("held");
    let rejected = consume(&assistant, png_upload());
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    playback.unwrap();
    assert!(rejected.unwrap_err().message.contains("idle assistant"));
    assert!(after.unwrap().messages.is_empty());
}

#[tokio::test]
async fn enabled_and_announced_realtime_uploads_reject_without_mutation() {
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
                        service_name: "realtime-test".into(),
                        user_turn_strategies: None,
                    },
                    is_realtime_service: true,
                })),
            )
        });
        let rejected = consume(&assistant, png_upload());
        let after = pair.context().snapshot_rich();
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
        assert!(after.unwrap().messages.is_empty());
    }
}

fn signed_frame(data: FrameData, history: NativeLlmContinuation) -> Frame {
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(Arc::new(history))
        .unwrap();
    frame
}
#[tokio::test]
async fn retained_signed_owner_rejects_until_playback_retires_then_preserves_gemini_signature() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    let start = consume(
        &assistant,
        signed_frame(
            FrameData::LlmResponseStart,
            NativeLlmContinuation::start(993111, LlmContinuationProvider::Gemini).unwrap(),
        ),
    );
    let playback = assistant.begin_playback("held");
    let text = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm("signed answer"))),
    );
    let frame = png_upload();
    let active_rejection = consume(&assistant, frame.clone());
    let end = consume(
        &assistant,
        signed_frame(
            FrameData::LlmResponseEnd,
            NativeLlmContinuation::new(
                993111,
                LlmContinuationProvider::Gemini,
                vec![LlmContinuationBlock::GeminiText {
                    text: "signed answer".into(),
                    thought: false,
                    signature: Some("opaque-signature".into()),
                }],
            )
            .unwrap(),
        ),
    );
    let before = context.snapshot();
    let retained_rejection = consume(&assistant, frame.clone());
    let unchanged = context.snapshot();
    let receipt = assistant.confirm_playback(
        "held",
        &[AggregatedTextFrame {
            text: TextFrame::llm("signed answer"),
            raw_text: Some("signed answer".into()),
            context_id: Some("held".into()),
            aggregated_by: "fixture".into(),
            will_be_spoken: true,
        }],
    );
    let prefix = context.snapshot();
    let retry = consume(&assistant, frame);
    let after = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    start.unwrap();
    playback.unwrap();
    text.unwrap();
    end.unwrap();
    assert!(active_rejection
        .unwrap_err()
        .message
        .contains("signed continuation"));
    assert!(retained_rejection
        .unwrap_err()
        .message
        .contains("signed continuation"));
    assert_eq!(before.unwrap(), unchanged.unwrap());
    receipt.unwrap();
    silent(retry.unwrap());
    let prefix = prefix.unwrap();
    let after = after.unwrap();
    assert_eq!(prefix.messages.len(), 1);
    assert_eq!(after.messages.len(), 2);
    assert_eq!(prefix.messages[0], after.messages[0]);
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("wire-fixture", after),
    )
    .unwrap();
    assert_eq!(
        body["contents"],
        json!([
            {"role": "model", "parts": [{"text": "signed answer", "thoughtSignature": "opaque-signature"}]},
            {"role": "user", "parts": [{"text": "inspect"}, {"inlineData": {"mimeType": "image/png", "data": PNG_BASE64}}]}
        ])
    );
}

#[tokio::test]
async fn append_only_upload_does_not_require_preexisting_history_to_project() {
    use pipecat::services::{Content, ContentPart, Context, Message, Role};
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
    let unprojectable_before = context.frame_data();
    let frame = png_upload();
    let accepted = consume(&assistant, frame.clone());
    let unprojectable_after = context.frame_data();
    let replay = consume(&assistant, frame);
    let after = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert!(unprojectable_before.is_err());
    assert!(unprojectable_after.is_err());
    silent(accepted.expect("preexisting projection limitation is not an append failure"));
    silent(replay.unwrap());
    let after = after.unwrap();
    assert_eq!(after.messages.len(), 2);
    assert_eq!(after.messages[0], prefix);
    assert_eq!(after.messages[1].role, Role::User);
}

#[tokio::test]
async fn private_carriers_reject_before_replay_and_terminal_order_stays_unchanged() {
    let mut continuation = signed_frame(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(993121, LlmContinuationProvider::Gemini).unwrap(),
    );
    continuation.data = png_upload().data;
    let call = FunctionCall {
        function_name: "owned".into(),
        tool_call_id: "owned".into(),
        arguments: json!({}),
    };
    let mut declaration = Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()]));
    declaration
        .set_native_tool_batch(
            NativeToolCallBatch::new("owned-group".into(), vec![(Arc::new(call), false)]).unwrap(),
        )
        .unwrap();
    declaration.data = png_upload().data;
    for mut malformed in [continuation.clone(), declaration.clone()] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let ordinary = png_upload();
        malformed.id = ordinary.id;
        let accepted = consume(&assistant, ordinary);
        let before = context.snapshot();
        let rejected = consume(&assistant, malformed);
        let after = context.snapshot();
        let pending = assistant.has_function_calls_in_progress();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(accepted.unwrap());
        assert!(rejected.is_err());
        assert!(!pending.unwrap());
        assert_eq!(before.unwrap(), after.unwrap());
    }
    for terminal in [
        Frame::end(),
        Frame::cancel("test"),
        Frame::new(FrameData::Stop),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let ended = consume(&assistant, terminal);
        let valid = consume(&assistant, png_upload());
        let invalid = consume(&assistant, upload(vec![], Some("RGB"), None));
        let continuation_ignored = consume(&assistant, continuation.clone());
        let tool_rejected = consume(&assistant, declaration.clone());
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        ended.unwrap();
        silent(valid.unwrap());
        silent(invalid.unwrap());
        silent(continuation_ignored.unwrap());
        assert!(tool_rejected.is_err());
        assert!(after.unwrap().messages.is_empty());
    }
}

// Frozen automatic-inference counterexamples. These exercise public APIs only.
fn inference_upload(run: Option<bool>) -> Frame {
    let mut frame = png_upload();
    if let FrameData::UserImage(image) = &mut frame.data {
        image.run_llm = run;
    }
    frame
}

fn inference_context(output: AggregationOutput) -> DetailedLlmContextFrame {
    use pipecat::turns::AggregationAction;
    assert!(output.events.is_empty());
    assert_eq!(output.actions.len(), 1, "one prepared context action only");
    let AggregationAction::Push(directed) = &output.actions[0] else {
        panic!("expected a context push")
    };
    assert_eq!(directed.direction, Direction::Upstream);
    let FrameData::LlmContextDetailed(context) = &directed.frame.data else {
        panic!("expected a detailed context, not a forwarded upload")
    };
    assert!(!context.speculation);
    context.clone()
}

fn inference_png_message() -> ContextMessage {
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

#[tokio::test]
async fn inference_baseline_idle_true_and_none_prepare_one_upstream_context() {
    for run in [Some(true), None] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let result = consume(&assistant, inference_upload(run));
        let snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        let projected = inference_context(
            result.expect("idle request-free true/None encoded upload must be admitted"),
        );
        assert_eq!(projected.context, snapshot.unwrap());
        assert_eq!(projected.context.messages, vec![inference_png_message()]);
    }
}

async fn inference_default_rtvi_pipeline(
    context: SharedContext,
) -> Result<(Vec<DirectedFrame>, WorkerReport), String> {
    const WAIT: Duration = Duration::from_secs(5);
    const MARKER: &str = "after-default-image";
    let rtvi = RtviProcessor::new(RtviConfig::default()).map_err(|e| e.to_string())?;
    let assistant = LLMAssistantAggregator::new(context, Default::default());
    let mut worker = Pipeline::new(vec![Arc::new(rtvi), Arc::new(assistant)])
        .start(PipelineConfig {
            startup_timeout: WAIT,
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    let mut frames = Vec::new();
    let operation = async {
        // The second command is a FIFO marker in the same real RTVI command loop.
        // It permits a bounded old-failure run without waiting for a missing image/context.
        for value in [
            json!({
                "label": "rtvi-ai", "id": "default-image", "type": "send-file",
                "data": {
                    "content": "inspect",
                    "file": {"format": "image/png", "source": {"type": "bytes", "bytes": PNG_BASE64}}
                }
            }),
            json!({
                "label": "rtvi-ai", "id": MARKER, "type": "client-message",
                "data": {"t": "test-fence", "d": null}
            }),
        ] {
            tokio::time::timeout(
                WAIT,
                worker.sender().send(
                    Frame::new(FrameData::InputTransportMessage(value)),
                    Direction::Downstream,
                ),
            )
            .await
            .map_err(|_| "default upload send deadline".to_string())?
            .map_err(|e| e.to_string())?;
        }
        tokio::time::timeout(WAIT, async {
            while let Some(directed) = worker.recv().await {
                let marked = matches!(&directed.frame.data, FrameData::RtviClientMessage(message)
                    if message.msg_id == MARKER);
                frames.push(directed);
                if marked {
                    return Ok(());
                }
            }
            Err("pipeline closed before the RTVI FIFO marker".to_string())
        })
        .await
        .map_err(|_| "default upload FIFO marker deadline".to_string())?
    }
    .await;
    // End, drain and join before any behavioral assertion, even on an old failure.
    let end = tokio::time::timeout(WAIT, worker.end()).await;
    if !matches!(&end, Ok(Ok(()))) {
        let _ = tokio::time::timeout(WAIT, worker.cancel("default upload end failed")).await;
    }
    let drain = tokio::time::timeout(WAIT, async {
        while let Some(directed) = worker.recv().await {
            frames.push(directed);
        }
    })
    .await;
    if drain.is_err() {
        let _ = tokio::time::timeout(WAIT, worker.cancel("default upload drain failed")).await;
    }
    let join = tokio::time::timeout(WAIT, worker.join()).await;
    if !matches!(&join, Ok(Ok(_))) {
        let _ = tokio::time::timeout(WAIT, worker.cancel("default upload join failed")).await;
        let _ = tokio::time::timeout(WAIT, worker.join()).await;
    }
    let report = join
        .map_err(|_| "default upload join deadline".to_string())?
        .map_err(|e| e.to_string())?;
    end.map_err(|_| "default upload end deadline".to_string())?
        .map_err(|e| e.to_string())?;
    drain.map_err(|_| "default upload drain deadline".to_string())?;
    operation?;
    Ok((frames, report))
}

#[tokio::test]
async fn inference_baseline_rtvi_default_emits_one_exact_gemini_context() {
    let context = SharedContext::default();
    let (frames, report) = inference_default_rtvi_pipeline(context.clone())
        .await
        .expect("default RTVI fixture failed after cleanup");
    let contexts: Vec<_> = frames
        .iter()
        .filter(|directed| {
            matches!(
                directed.frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .collect();
    assert_eq!(
        contexts.len(),
        1,
        "one actual pipeline context; report={report:?}"
    );
    assert_eq!(contexts[0].direction, Direction::Upstream);
    let FrameData::LlmContextDetailed(projected) = &contexts[0].frame.data else {
        panic!("default upload must emit a detailed context")
    };
    assert!(!projected.speculation);
    assert_eq!(projected.context, context.snapshot_rich().unwrap());
    assert_eq!(projected.context.messages, vec![inference_png_message()]);
    assert!(!frames
        .iter()
        .any(|directed| matches!(directed.frame.data, FrameData::UserImage(_))));
    for direction in [Direction::Downstream, Direction::Upstream] {
        assert_eq!(
            frames
                .iter()
                .filter(|directed| directed.direction == direction
                    && matches!(directed.frame.data, FrameData::Interruption))
                .count(),
            1
        );
    }
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{report:?}");
    let native = pipecat::services::frame_context::context(&projected.context).unwrap();
    assert_eq!(native, context.snapshot().unwrap());
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("wire-fixture", native),
    )
    .unwrap();
    assert_eq!(
        body["contents"],
        json!([
            {"role": "user", "parts": [
                {"text": "inspect"},
                {"inlineData": {"mimeType": "image/png", "data": PNG_BASE64}}
            ]}
        ])
    );
}

// Candidate-only controls. The two inference baselines and their helpers above
// remain exact bytes; the old flag-rejection matrix now excludes all append=true.
fn inference_no_context(output: AggregationOutput) {
    use pipecat::turns::AggregationAction;
    assert!(output.events.is_empty());
    assert_eq!(output.actions.len(), 1);
    let AggregationAction::Push(directed) = &output.actions[0] else {
        panic!("expected only the ordinary speaking-state frame")
    };
    assert!(matches!(directed.frame.data, FrameData::BotStoppedSpeaking));
}

fn inference_unprojectable_prefix() -> pipecat::services::Message {
    use pipecat::services::{Content, ContentPart, Message, Role};
    Message {
        role: Role::User,
        content: Some(Content::Parts(vec![ContentPart::InlineData {
            mime_type: "application/pdf".into(),
            data: "JVBERg==".into(),
        }])),
        name: None,
        tool_calls: vec![],
        tool_call_id: None,
    }
}

#[tokio::test]
async fn inference_replay_and_distinct_ids_keep_one_action_per_admitted_image() {
    let jpeg = include_bytes!("../../pipecat-images/tests/fixtures/source-cat.jpg");
    for (run, direction, bytes, mime) in [
        (Some(true), Direction::Downstream, PNG, "image/png"),
        (None, Direction::Upstream, PNG, "image/png"),
        (
            Some(true),
            Direction::Upstream,
            jpeg.as_slice(),
            "image/jpeg",
        ),
        (None, Direction::Downstream, jpeg.as_slice(), "image/jpeg"),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut frame = upload(bytes.to_vec(), Some(mime), Some("inspect"));
        if let FrameData::UserImage(image) = &mut frame.data {
            image.run_llm = run;
        }
        let first = assistant.process_at(frame.clone(), direction, Duration::ZERO);
        let first_snapshot = context.snapshot_rich();
        let replay = assistant.process_at(frame.clone(), direction, Duration::ZERO);
        let mut malformed = frame.clone();
        if let FrameData::UserImage(image) = &mut malformed.data {
            image.image.image.clear();
        }
        let invalid_replay = assistant.process_at(malformed, direction, Duration::ZERO);
        let after_replays = context.snapshot_rich();
        let next = assistant.process_at(Frame::new(frame.data), direction, Duration::ZERO);
        let final_snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        let first = inference_context(first.unwrap());
        assert_eq!(first.context, first_snapshot.unwrap());
        assert_eq!(first.context.messages.len(), 1);
        silent(replay.unwrap());
        assert!(
            invalid_replay.is_err(),
            "validation must precede successful-ID replay"
        );
        assert_eq!(first.context, after_replays.unwrap());
        let next = inference_context(next.unwrap());
        assert_eq!(next.context, final_snapshot.unwrap());
        assert_eq!(
            next.context.messages,
            vec![first.context.messages[0].clone(); 2]
        );
        let native = pipecat::services::frame_context::context(&next.context).unwrap();
        let body = request_body(
            Provider::Gemini,
            &CompletionRequest::new("wire-fixture", native),
        )
        .unwrap();
        let part = &body["contents"][0]["parts"][1]["inlineData"];
        assert_eq!(part["mimeType"], mime);
        assert!(!part["data"].as_str().unwrap().is_empty());
    }
}

#[tokio::test]
async fn inference_projection_failure_is_atomic_and_same_id_recovers_after_repair() {
    use pipecat::services::{Context, Message, Role};
    for run in [Some(true), None] {
        let prefix = inference_unprojectable_prefix();
        let context = SharedContext::new(Context::new(vec![prefix.clone()]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let frame = inference_upload(run);
        let rejected = consume(&assistant, frame.clone());
        let after_rejection = context.snapshot();
        let projection_still_fails = context.frame_data();
        let repaired = context.replace(Context::new(vec![Message::text(Role::User, "repaired")]));
        let no_debt = consume(&assistant, Frame::new(FrameData::BotStoppedSpeaking));
        let retried = consume(&assistant, frame.clone());
        let after_retry = context.snapshot_rich();
        let replay = consume(&assistant, frame);
        let later_run = consume(&assistant, Frame::new(FrameData::LlmRun));
        let no_later_debt = consume(&assistant, Frame::new(FrameData::BotStoppedSpeaking));
        let final_snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected.unwrap_err().message.contains("InlineData"));
        assert_eq!(after_rejection.unwrap(), Context::new(vec![prefix]));
        assert!(projection_still_fails.is_err());
        repaired.unwrap();
        inference_no_context(no_debt.unwrap());
        let retried = inference_context(retried.unwrap());
        assert_eq!(retried.context, after_retry.unwrap());
        assert_eq!(retried.context.messages.len(), 2);
        assert_eq!(
            retried.context.messages[0].content,
            MessageContent::Text("repaired".into())
        );
        assert_eq!(retried.context.messages[1], inference_png_message());
        silent(replay.unwrap());
        assert_eq!(
            inference_context(later_run.unwrap()).context,
            retried.context
        );
        inference_no_context(no_later_debt.unwrap());
        assert_eq!(final_snapshot.unwrap(), retried.context);
    }
}

#[tokio::test]
async fn inference_failed_run_can_retry_false_and_accepted_false_cannot_be_rearmed() {
    use pipecat::services::Context;
    for run in [Some(true), None] {
        let prefix = inference_unprojectable_prefix();
        let context = SharedContext::new(Context::new(vec![prefix.clone()]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let frame = inference_upload(run);
        let rejected = consume(&assistant, frame.clone());
        let after_rejection = context.snapshot();
        let mut append_only = frame.clone();
        if let FrameData::UserImage(image) = &mut append_only.data {
            image.run_llm = Some(false);
        }
        let recovered = consume(&assistant, append_only);
        let after_append = context.snapshot();
        // This must hit successful replay before requiring a frame projection.
        let rearmed = consume(&assistant, frame);
        let after_rearm = context.snapshot();
        let unprojectable = context.frame_data();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected.unwrap_err().message.contains("InlineData"));
        assert_eq!(after_rejection.unwrap(), Context::new(vec![prefix.clone()]));
        silent(recovered.unwrap());
        silent(rearmed.unwrap());
        let after_append = after_append.unwrap();
        assert_eq!(after_append.messages.len(), 2);
        assert_eq!(after_append.messages[0], prefix);
        assert_eq!(after_append, after_rearm.unwrap());
        assert!(unprojectable.is_err());
    }
    // The same false -> true/None replay rule also holds in projectable history.
    for run in [Some(true), None] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut frame = png_upload();
        let accepted = consume(&assistant, frame.clone());
        if let FrameData::UserImage(image) = &mut frame.data {
            image.run_llm = run;
        }
        let rearmed = consume(&assistant, frame);
        let snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(accepted.unwrap());
        silent(rearmed.unwrap());
        assert_eq!(snapshot.unwrap().messages, vec![inference_png_message()]);
    }
}

#[tokio::test]
async fn inference_emitted_context_preserves_completed_gemini_signature_prefix() {
    use pipecat::services::{Content, ContentPart, Context, Message, Role};
    for run in [Some(true), None] {
        let prefix = Message {
            role: Role::Assistant,
            content: Some(Content::Parts(vec![ContentPart::GeminiText {
                text: "signed answer".into(),
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
        let result = consume(&assistant, inference_upload(run));
        let native_snapshot = context.snapshot();
        let rich_snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        let emitted = inference_context(result.unwrap());
        assert_eq!(emitted.context, rich_snapshot.unwrap());
        let native = pipecat::services::frame_context::context(&emitted.context).unwrap();
        assert_eq!(native, native_snapshot.unwrap());
        assert_eq!(native.messages[0], prefix);
        assert_eq!(native.messages.len(), 2);
        let body = request_body(
            Provider::Gemini,
            &CompletionRequest::new("wire-fixture", native),
        )
        .unwrap();
        assert_eq!(
            body["contents"],
            json!([
                {"role": "model", "parts": [{"text": "signed answer", "thoughtSignature": "opaque-signature"}]},
                {"role": "user", "parts": [{"text": "inspect"}, {"inlineData": {"mimeType": "image/png", "data": PNG_BASE64}}]}
            ])
        );
    }
}

// Real loopback completion regression. The earlier 19-test prefix is unchanged.
// This is an unsigned text reply to an image input, not a live vision-quality test.
#[derive(Debug, Default)]
struct DefaultImageHttpLog {
    connections: usize,
    requests: Vec<(String, Vec<u8>)>,
    errors: Vec<String>,
}

async fn default_image_http_request(
    socket: &mut tokio::net::TcpStream,
) -> Result<(String, Vec<u8>), String> {
    use tokio::io::AsyncReadExt;
    const LIMIT: usize = 64 * 1024;
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let header_end = loop {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 || bytes.len() + n > LIMIT {
            return Err("incomplete or oversized HTTP request headers".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec()).map_err(|e| e.to_string())?;
    let length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .ok_or("HTTP request has no Content-Length")?
        .1
        .trim()
        .parse::<usize>()
        .map_err(|e| e.to_string())?;
    if length > LIMIT - header_end {
        return Err("oversized HTTP request body".into());
    }
    while bytes.len() < header_end + length {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 || bytes.len() + n > LIMIT {
            return Err("incomplete or oversized HTTP request body".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    Ok((headers, bytes[header_end..header_end + length].to_vec()))
}

async fn default_image_http_reply(socket: &mut tokio::net::TcpStream) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    socket
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
        .await
        .map_err(|e| e.to_string())?;
    // Two separately framed HTTP chunks carry two SSE events; only the second
    // ends the Gemini response. No timer is used as evidence of consumption.
    for event in [
        json!({"candidates":[{"content":{"role":"model","parts":[{"text":"a red "}]}}]}),
        json!({"candidates":[{"content":{"role":"model","parts":[{"text":"pixel"}]},"finishReason":"STOP"}]}),
    ] {
        let event = format!("data: {event}\n\n");
        socket
            .write_all(format!("{:x}\r\n{event}\r\n", event.len()).as_bytes())
            .await
            .map_err(|e| e.to_string())?;
    }
    socket
        .write_all(b"0\r\n\r\n")
        .await
        .map_err(|e| e.to_string())?;
    socket.shutdown().await.map_err(|e| e.to_string())
}

#[tokio::test]
async fn default_encoded_upload_completes_one_real_http_request_and_commits_reply() {
    use pipecat::services::{ClientConfig, Context, LlmClient, LlmProcessor};
    const WAIT: Duration = Duration::from_secs(5);
    const REPLY: &str = "a red pixel";
    const FAKE_KEY: &str = "synthetic-default-image-key";
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the loopback fixture");
    let address = listener.local_addr().unwrap();
    let mut config = ClientConfig::new(Provider::Gemini, FAKE_KEY);
    config.endpoint =
        format!("http://{address}/v1beta/models/{{model}}:streamGenerateContent?alt=sse");
    config.request_timeout = WAIT;
    config.idle_timeout = WAIT;
    config.connect_timeout = WAIT;
    let context = SharedContext::default();
    let pipeline = Pipeline::new(vec![
        Arc::new(RtviProcessor::new(RtviConfig::default()).unwrap()),
        Arc::new(LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            CompletionRequest::new("wire-fixture", Context::default()),
        )),
        Arc::new(LLMAssistantAggregator::new(
            context.clone(),
            Default::default(),
        )),
    ]);
    let peer_log = Arc::new(std::sync::Mutex::new(DefaultImageHttpLog::default()));
    let server_log = peer_log.clone();
    let (stop_peer, mut stopping) = tokio::sync::oneshot::channel();
    let mut peer = tokio::spawn(async move {
        // Keep accepting and replying until the pipeline has drained and joined.
        // A duplicate request must be counted, never hidden by a one-shot server.
        loop {
            let accepted = tokio::select! {
                _ = &mut stopping => break,
                accepted = listener.accept() => accepted,
            };
            let mut socket = match accepted {
                Ok((socket, _)) => socket,
                Err(error) => {
                    server_log.lock().unwrap().errors.push(error.to_string());
                    break;
                }
            };
            server_log.lock().unwrap().connections += 1;
            let exchange = async {
                let request = default_image_http_request(&mut socket).await?;
                server_log.lock().unwrap().requests.push(request);
                default_image_http_reply(&mut socket).await
            };
            // Finish an accepted exchange before consuming stop at the accept
            // boundary. Normal shutdown must not race a completed response's
            // final socket shutdown; incomplete requests still hit this deadline.
            let result = tokio::time::timeout(WAIT, exchange).await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => server_log.lock().unwrap().errors.push(error),
                Err(_) => server_log
                    .lock()
                    .unwrap()
                    .errors
                    .push("HTTP exchange deadline".into()),
            }
        }
    });
    // No assertions or early returns after spawning the peer until both owners
    // have been stopped/joined. Keep operation and cleanup failures distinct.
    let mut frames = Vec::new();
    let mut failures = Vec::new();
    let mut report = None;
    match pipeline
        .start(PipelineConfig {
            startup_timeout: WAIT,
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await
    {
        Ok(mut worker) => {
            let operation = tokio::time::timeout(WAIT, async {
                worker.sender().send(
                    Frame::new(FrameData::InputTransportMessage(json!({
                        "label":"rtvi-ai", "id":"default-image-http", "type":"send-file",
                        "data": {
                            "content":"inspect",
                            "file":{"format":"image/png","source":{"type":"bytes","bytes":PNG_BASE64}}
                        }
                    }))),
                    Direction::Downstream,
                ).await.map_err(|e| e.to_string())?;
                // The assistant consumes raw response frames. Its downstream
                // turn receipt is emitted only after committing the reply. Wait
                // for that positive receipt before End can flush pending text.
                while let Some(frame) = worker.recv().await {
                    let receipt = frame.direction == Direction::Downstream
                        && matches!(&frame.frame.data, FrameData::LlmContextAssistantTurn(turn) if turn.text == REPLY);
                    let error = matches!(&frame.frame.data, FrameData::Error(_));
                    frames.push(frame);
                    if error {
                        return Err("pipeline emitted an error before the reply receipt".to_string());
                    }
                    if receipt {
                        return Ok(());
                    }
                }
                Err("pipeline closed before the expected reply receipt".to_string())
            }).await;
            match operation {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(error),
                Err(_) => {
                    failures.push("reply receipt deadline (not a zero-request result)".into())
                }
            }
            let end = tokio::time::timeout(WAIT, worker.end()).await;
            if !matches!(&end, Ok(Ok(()))) {
                failures.push(format!("pipeline End: {end:?}"));
                let cancelled =
                    tokio::time::timeout(WAIT, worker.cancel("HTTP fixture End failed")).await;
                if !matches!(&cancelled, Ok(Ok(()))) {
                    failures.push(format!("pipeline cancel after End failure: {cancelled:?}"));
                }
            }
            let drain = tokio::time::timeout(WAIT, async {
                while let Some(frame) = worker.recv().await {
                    frames.push(frame);
                }
            })
            .await;
            if drain.is_err() {
                failures.push("pipeline output drain deadline".into());
                let cancelled =
                    tokio::time::timeout(WAIT, worker.cancel("HTTP fixture drain failed")).await;
                if !matches!(&cancelled, Ok(Ok(()))) {
                    failures.push(format!(
                        "pipeline cancel after drain failure: {cancelled:?}"
                    ));
                }
            }
            match tokio::time::timeout(WAIT, worker.join()).await {
                Ok(Ok(joined)) => report = Some(joined),
                Ok(Err(error)) => failures.push(format!("pipeline join: {error}")),
                Err(_) => {
                    failures.push("pipeline join deadline".into());
                    let cancelled =
                        tokio::time::timeout(WAIT, worker.cancel("HTTP fixture join failed")).await;
                    if !matches!(&cancelled, Ok(Ok(()))) {
                        failures.push(format!("pipeline cancel after join failure: {cancelled:?}"));
                    }
                    match tokio::time::timeout(WAIT, worker.join()).await {
                        Ok(Ok(joined)) => report = Some(joined),
                        other => failures.push(format!("pipeline fallback join: {other:?}")),
                    }
                }
            }
        }
        Err(error) => failures.push(format!("pipeline startup: {error}")),
    }
    let _ = stop_peer.send(());
    match tokio::time::timeout(WAIT, &mut peer).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.push(format!("HTTP peer join: {error}")),
        Err(_) => {
            failures.push("HTTP peer join deadline".into());
            peer.abort();
            let aborted = tokio::time::timeout(WAIT, &mut peer).await;
            if !matches!(&aborted, Ok(Err(error)) if error.is_cancelled()) {
                failures.push(format!("HTTP peer abort/join: {aborted:?}"));
            }
        }
    }
    let log = peer_log.lock().unwrap();
    let rich = context.snapshot_rich();
    assert!(failures.is_empty(), "fixture failures={failures:?}; peer={log:?}; frames={frames:?}; report={report:?}; history={rich:?}");
    assert!(log.errors.is_empty(), "{log:?}");
    assert_eq!(log.connections, 1, "{log:?}");
    assert_eq!(log.requests.len(), 1, "{log:?}");
    let (headers, bytes) = &log.requests[0];
    assert_eq!(
        headers.lines().next(),
        Some("POST /v1beta/models/wire-fixture:streamGenerateContent?alt=sse HTTP/1.1")
    );
    let header = |name: &str| -> Vec<&str> {
        headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
            .collect()
    };
    assert_eq!(header("host"), vec![address.to_string()]);
    assert_eq!(header("x-goog-api-key"), vec![FAKE_KEY]);
    assert_eq!(header("accept"), vec!["text/event-stream"]);
    assert_eq!(header("content-type"), vec!["application/json"]);
    assert_eq!(header("content-length"), vec![bytes.len().to_string()]);
    assert!(header("authorization").is_empty());
    let body: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(
        body,
        json!({"contents":[{"role":"user","parts":[
            {"text":"inspect"}, {"inlineData":{"mimeType":"image/png","data":PNG_BASE64}}
        ]}]})
    );
    let rich = rich.unwrap();
    assert_eq!(
        rich.messages,
        vec![
            inference_png_message(),
            ChatMessage::new(ChatRole::Assistant, REPLY).into(),
        ]
    );
    assert_eq!(
        pipecat::services::frame_context::context(&rich).unwrap(),
        context.snapshot().unwrap()
    );
    let returned: Vec<_> = frames
        .iter()
        .filter_map(|frame| {
            if let FrameData::LlmContextDetailed(value) = &frame.frame.data {
                Some((frame.direction, value))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(returned.len(), 1, "returned contexts: {returned:?}");
    assert_eq!(returned[0].0, Direction::Downstream);
    assert!(!returned[0].1.speculation);
    assert_eq!(returned[0].1.context, rich);
    let replies: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::LlmContextAssistantTurn(turn)
                if frame.direction == Direction::Downstream =>
            {
                Some(turn.text.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(replies, vec![REPLY]);
    assert!(!frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::Error(_) | FrameData::UserImage(_)
    )));
    let report = report.expect("the stopped pipeline must have a join report");
    assert_eq!(report.state, WorkerState::Ended, "{report:?}");
    assert!(report.errors.is_empty(), "{report:?}");
    assert_eq!(report.total_errors, 0, "{report:?}");
}
