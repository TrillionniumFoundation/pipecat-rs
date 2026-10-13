//! Frozen public-API baseline: real RTVI PDF upload -> context -> OpenAI request body.
//! The producer is ended and joined before reducer admission or behavior assertions.
use pipecat::{
    core::*,
    rtvi::{RtviConfig, RtviProcessor},
    services::{
        adapters::request_body, CompletionRequest, Content, ContentPart, Context, Message,
        Provider, Role,
    },
    turns::{LLMAssistantAggregator, SharedContext},
};
use serde_json::json;
use std::{sync::Arc, time::Duration};

// Self-created valid PDF 1.4: one blank page, four objects, and exact xref offsets.
// Encoding/header admission is not complete PDF validation or provider acceptance.
const PDF_BASE64: &str = "JVBERi0xLjQKMSAwIG9iago8PCAvVHlwZSAvQ2F0YWxvZyAvUGFnZXMgMiAwIFIgPj4KZW5kb2JqCjIgMCBvYmoKPDwgL1R5cGUgL1BhZ2VzIC9LaWRzIFszIDAgUl0gL0NvdW50IDEgPj4KZW5kb2JqCjMgMCBvYmoKPDwgL1R5cGUgL1BhZ2UgL1BhcmVudCAyIDAgUiAvTWVkaWFCb3ggWzAgMCA3MiA3Ml0gL1Jlc291cmNlcyA8PCA+PiAvQ29udGVudHMgNCAwIFIgPj4KZW5kb2JqCjQgMCBvYmoKPDwgL0xlbmd0aCAwID4+CnN0cmVhbQoKZW5kc3RyZWFtCmVuZG9iagp4cmVmCjAgNQowMDAwMDAwMDAwIDY1NTM1IGYgCjAwMDAwMDAwMDkgMDAwMDAgbiAKMDAwMDAwMDA1OCAwMDAwMCBuIAowMDAwMDAwMTE1IDAwMDAwIG4gCjAwMDAwMDAyMTcgMDAwMDAgbiAKdHJhaWxlcgo8PCAvU2l6ZSA1IC9Sb290IDEgMCBSID4+CnN0YXJ0eHJlZgoyNjYKJSVFT0YK";

fn pdf_data_url() -> String {
    format!("data:application/pdf;base64,{PDF_BASE64}")
}
fn pdf_upload() -> Frame {
    Frame::new(FrameData::UserFileDetailed(DetailedUserFileRawFrame {
        file: FileRawFrame {
            file: pdf_data_url(),
            source_type: FileSourceType::Bytes,
            filename: Some("document.pdf".into()),
            format: "application/pdf".into(),
        },
        user_id: "not-a-message-name".into(),
        text: "read".into(),
        append_to_context: Some(true),
        custom_options: None,
        run_llm: Some(false),
    }))
}
fn expected_rich() -> LlmContextData {
    LlmContextData {
        messages: vec![ContextMessage {
            role: ChatRole::User,
            content: MessageContent::Parts(vec![
                MessagePart::Text {
                    text: "read".into(),
                },
                MessagePart::File {
                    data: PDF_BASE64.into(),
                    mime_type: "application/pdf".into(),
                    filename: Some("document.pdf".into()),
                },
            ]),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
            extra: Default::default(),
        }],
        tools: ToolSet::NotGiven,
        tool_choice: None,
    }
}
fn expected_native() -> Context {
    Context::new(vec![Message {
        role: Role::User,
        content: Some(Content::Parts(vec![
            ContentPart::Text {
                text: "read".into(),
            },
            ContentPart::File {
                data: PDF_BASE64.into(),
                mime_type: "application/pdf".into(),
                filename: Some("document.pdf".into()),
            },
        ])),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
    }])
}
fn expected_openai() -> serde_json::Value {
    json!({
        "model": "wire-fixture",
        "stream": true,
        "stream_options": {"include_usage": true},
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "read"},
            {"type": "file", "file": {"filename": "document.pdf", "file_data": pdf_data_url()}}
        ]}]
    })
}

#[tokio::test]
async fn idle_detailed_pdf_appends_exact_file_without_actions() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let accepted = assistant.process_at(pdf_upload(), Direction::Downstream, Duration::ZERO);
    let rich = context.snapshot_rich();
    let native = context.snapshot();
    let projected = context.frame_data();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    let accepted = accepted.expect("idle explicit-false detailed PDF must be admitted");
    assert!(accepted.actions.is_empty());
    assert!(accepted.events.is_empty());
    assert_eq!(rich.unwrap(), expected_rich());
    assert_eq!(native.as_ref().unwrap(), &expected_native());
    assert_eq!(
        projected.unwrap(),
        FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: expected_rich(),
            speculation: false
        })
    );
    let body = request_body(
        Provider::OpenAi,
        &CompletionRequest::new("wire-fixture", native.unwrap()),
    )
    .unwrap();
    assert_eq!(body, expected_openai());
}

async fn rtvi_pdf_upload(bytes: &str, text: &str, name: Option<&str>) -> Result<Frame, String> {
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
        tokio::time::timeout(WAIT, worker.sender().send(
            Frame::new(FrameData::InputTransportMessage(json!({
                "label": "rtvi-ai", "id": "encoded-user-file-minimal", "type": "send-file",
                "data": {
                    "content": text,
                    "file": {"format": "application/pdf", "name": name, "source": {"type": "bytes", "bytes": bytes}},
                    "options": {"run_immediately": false, "audio_response": true}
                }
            }))), Direction::Downstream,
        )).await.map_err(|_| "RTVI send deadline".to_string())?.map_err(|e| e.to_string())?;
        tokio::time::timeout(WAIT, async {
            while let Some(directed) = worker.recv().await {
                if matches!(directed.frame.data, FrameData::Interruption) {
                    return Err("false-run file unexpectedly interrupted".to_string());
                }
                if matches!(directed.frame.data, FrameData::UserFileDetailed(_)) {
                    return Ok(directed.frame);
                }
            }
            Err("RTVI closed without a detailed user file".to_string())
        }).await.map_err(|_| "RTVI file deadline".to_string())?
    }.await;
    // Always attempt End/join before returning, even on send/capture failure.
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
async fn actual_rtvi_pdf_reaches_exact_openai_request_body() {
    // This actual producer has completely ended and joined before consumer use.
    let frame = rtvi_pdf_upload(PDF_BASE64, "read", Some("document.pdf"))
        .await
        .expect("RTVI fixture failed after cleanup");
    let producer_data = frame.data.clone();
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let accepted = assistant.process_at(frame, Direction::Downstream, Duration::ZERO);
    let rich = context.snapshot_rich();
    let native = context.snapshot();
    let projected = context.frame_data();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert_eq!(
        producer_data,
        FrameData::UserFileDetailed(DetailedUserFileRawFrame {
            file: FileRawFrame {
                file: pdf_data_url(),
                source_type: FileSourceType::Bytes,
                filename: Some("document.pdf".into()),
                format: "application/pdf".into()
            },
            user_id: String::new(),
            text: "read".into(),
            append_to_context: Some(true),
            custom_options: None,
            run_llm: Some(false),
        })
    );
    let accepted = accepted.expect("actual RTVI explicit-false PDF must be admitted");
    assert!(accepted.actions.is_empty());
    assert!(accepted.events.is_empty());
    assert_eq!(rich.unwrap(), expected_rich());
    assert_eq!(native.as_ref().unwrap(), &expected_native());
    assert_eq!(
        projected.unwrap(),
        FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: expected_rich(),
            speculation: false
        })
    );
    let body = request_body(
        Provider::OpenAi,
        &CompletionRequest::new("wire-fixture", native.unwrap()),
    )
    .unwrap();
    assert_eq!(body, expected_openai());
}

// Candidate-only controls. All preceding baseline bytes remain frozen.
use pipecat::turns::{
    AggregationOutput, AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair,
    RealtimeServiceMode,
};

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
fn edit_upload(edit: impl FnOnce(&mut DetailedUserFileRawFrame)) -> Frame {
    let mut frame = pdf_upload();
    let FrameData::UserFileDetailed(file) = &mut frame.data else {
        unreachable!()
    };
    edit(file);
    frame
}

#[tokio::test]
async fn actual_rtvi_raw_and_full_urls_preserve_text_and_normalize_filename() {
    for source in [PDF_BASE64.to_owned(), pdf_data_url()] {
        for (text, name) in [("", None), ("", Some("")), (" \n", Some(" \n.pdf"))] {
            let mut frame = rtvi_pdf_upload(&source, text, name)
                .await
                .expect("RTVI cleanup");
            frame
                .metadata
                .insert("tool_call_id".into(), json!("untrusted"));
            frame
                .metadata
                .insert("thoughtSignature".into(), json!("untrusted"));
            let producer = frame.data.clone();
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let accepted = assistant.process_at(frame, Direction::Upstream, Duration::ZERO);
            let rich = context.snapshot_rich();
            let native = context.snapshot();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            assert_eq!(
                producer,
                FrameData::UserFileDetailed(DetailedUserFileRawFrame {
                    file: FileRawFrame {
                        file: pdf_data_url(),
                        source_type: FileSourceType::Bytes,
                        filename: name.map(str::to_owned),
                        format: "application/pdf".into()
                    },
                    user_id: String::new(),
                    text: text.into(),
                    append_to_context: Some(true),
                    custom_options: None,
                    run_llm: Some(false),
                })
            );
            silent(accepted.unwrap());
            let mut rich_expected = expected_rich();
            let mut native_expected = expected_native();
            let mut wire_expected = expected_openai();
            let MessageContent::Parts(parts) = &mut rich_expected.messages[0].content else {
                unreachable!()
            };
            let Some(Content::Parts(native_parts)) = &mut native_expected.messages[0].content
            else {
                unreachable!()
            };
            let wire = wire_expected["messages"][0]["content"]
                .as_array_mut()
                .unwrap();
            if text.is_empty() {
                parts.remove(0);
                native_parts.remove(0);
                wire.remove(0);
            } else {
                parts[0] = MessagePart::Text { text: text.into() };
                native_parts[0] = ContentPart::Text { text: text.into() };
                wire[0] = json!({"type": "text", "text": text});
            }
            *parts.last_mut().unwrap() = MessagePart::File {
                data: PDF_BASE64.into(),
                mime_type: "application/pdf".into(),
                filename: Some(name.unwrap_or("").into()),
            };
            *native_parts.last_mut().unwrap() = ContentPart::File {
                data: PDF_BASE64.into(),
                mime_type: "application/pdf".into(),
                filename: Some(name.unwrap_or("").into()),
            };
            wire.last_mut().unwrap()["file"]["filename"] = json!(name.unwrap_or(""));
            assert_eq!(rich.unwrap(), rich_expected);
            assert_eq!(native.as_ref().unwrap(), &native_expected);
            assert_eq!(
                request_body(
                    Provider::OpenAi,
                    &CompletionRequest::new("wire-fixture", native.unwrap())
                )
                .unwrap(),
                wire_expected
            );
        }
    }
    // The actual producer intentionally preserves this inconsistent header.
    let mismatched = format!("data:text/plain;base64,{PDF_BASE64}");
    let frame = rtvi_pdf_upload(&mismatched, "read", None)
        .await
        .expect("RTVI cleanup");
    let producer = frame.data.clone();
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let rejected = consume(&assistant, frame);
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    let FrameData::UserFileDetailed(file) = producer else {
        panic!("expected actual file")
    };
    assert_eq!(file.file.file, mismatched);
    assert_eq!(file.file.format, "application/pdf");
    assert!(rejected.is_err());
    assert!(after.unwrap().messages.is_empty());
}

#[tokio::test]
async fn invalid_encoding_header_signature_and_nonselected_flags_preserve_retry() {
    let mut cases = vec![];
    for payload in [
        "",
        "JVBERi0=",
        "data:application/pdf;base64,",
        "data:application/pdf;base64,!private-payload!",
        "data:application/pdf;base64,JVBERi1=",
        "data:application/pdf;base64,JVBERi0",
        "data:application/pdf;base64,JVBERi0===",
        "data:application/pdf;base64,JVBERi0=\n",
        "data:application/pdf;base64,JVBERg==",
        "data:application/pdf;base64,AAAA",
        "data:application/pdf;BASE64,JVBERi0=",
        "data:application/pdf;charset=binary;base64,JVBERi0=",
        "data:text/plain;base64,JVBERi0=",
        "data:application/pdf;base64,____",
    ] {
        cases.push(edit_upload(|file| file.file.file = payload.into()));
    }
    for mime in [
        "",
        "application/PDF",
        "application/pdf; charset=binary",
        "text/plain",
        "image/png",
    ] {
        cases.push(edit_upload(|file| file.file.format = mime.into()));
    }
    cases.push(edit_upload(|file| {
        file.file.source_type = FileSourceType::Url;
        file.file.file = "https://example.invalid/private-payload.pdf".into();
    }));
    for append in [None, Some(false), Some(true)] {
        for run in [None, Some(false), Some(true)] {
            if append == Some(true) && run == Some(false) {
                continue;
            }
            cases.push(edit_upload(|file| {
                file.append_to_context = append;
                file.run_llm = run;
            }));
        }
    }
    cases.push(edit_upload(|file| {
        file.custom_options = Some(Default::default())
    }));
    cases.push(edit_upload(|file| {
        file.custom_options = Some([("private-payload".into(), json!(true))].into())
    }));
    let FrameData::UserFileDetailed(legacy) = pdf_upload().data else {
        unreachable!()
    };
    cases.push(Frame::new(FrameData::UserFile(UserFileRawFrame {
        file: legacy.file,
        user_id: legacy.user_id,
    })));
    for frame in cases {
        let context = SharedContext::new(Context::new(vec![Message::text(Role::User, "existing")]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let before = context.snapshot();
        let id = frame.id;
        let rejected = consume(&assistant, frame);
        let unchanged = context.snapshot();
        let mut corrected = pdf_upload();
        corrected.id = id;
        let retry = consume(&assistant, corrected.clone());
        let replay = consume(&assistant, corrected);
        let after = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        let error = rejected.expect_err("outside the bounded PDF subset");
        assert!(!error.message.contains("private-payload"));
        assert_eq!(before.unwrap(), unchanged.unwrap());
        silent(retry.unwrap());
        silent(replay.unwrap());
        let mut expected = expected_native();
        expected
            .messages
            .insert(0, Message::text(Role::User, "existing"));
        assert_eq!(after.unwrap(), expected);
    }
}

// Encoding of %PDF- followed by zeros. Boundary fixtures establish logical
// admission, not complete PDF syntax. The two positive baseline fixtures do.
fn sized_pdf_url(bytes: usize) -> String {
    assert!(bytes >= 6);
    let encoded_len = 4 * bytes.div_ceil(3);
    let padding = (3 - bytes % 3) % 3;
    let mut data = String::from("data:application/pdf;base64,JVBERi0A");
    data.push_str(&"A".repeat(encoded_len - 8 - padding));
    data.push_str(&"=".repeat(padding));
    data
}
#[tokio::test]
async fn direct_decoded_cap_is_inclusive_and_checks_same_encoded_length_overflow() {
    const LIMIT: usize = 8 * 1024 * 1024;
    assert_eq!(sized_pdf_url(LIMIT).len(), sized_pdf_url(LIMIT + 1).len());
    for over in [LIMIT + 1, LIMIT + 4] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let frame = edit_upload(|file| file.file.file = sized_pdf_url(over));
        let id = frame.id;
        let rejected = consume(&assistant, frame);
        let before = context.snapshot_rich();
        let mut corrected = edit_upload(|file| {
            file.file.file = sized_pdf_url(LIMIT);
            file.text.clear();
        });
        corrected.id = id;
        let accepted = consume(&assistant, corrected);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected.unwrap_err().message.contains("8388608"));
        assert!(before.unwrap().messages.is_empty());
        silent(accepted.unwrap());
        let mut expected = expected_rich();
        expected.messages[0].content = MessageContent::Parts(vec![MessagePart::File {
            data: sized_pdf_url(LIMIT)
                .strip_prefix("data:application/pdf;base64,")
                .unwrap()
                .into(),
            mime_type: "application/pdf".into(),
            filename: Some("document.pdf".into()),
        }]);
        assert_eq!(after.unwrap(), expected);
    }
}

#[tokio::test]
async fn replay_validation_and_terminal_order_preserve_context_without_restart() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let frame = pdf_upload();
    let accepted = consume(&assistant, frame.clone());
    let replay = consume(&assistant, frame.clone());
    let mut bad = edit_upload(|file| file.file.file.clear());
    bad.id = frame.id;
    let malformed_duplicate = consume(&assistant, bad);
    let distinct = consume(&assistant, pdf_upload());
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(accepted.unwrap());
    silent(replay.unwrap());
    silent(distinct.unwrap());
    assert!(malformed_duplicate.is_err());
    let mut expected = expected_rich();
    expected.messages.push(expected.messages[0].clone());
    assert_eq!(after.unwrap(), expected);
    for terminal in [
        Frame::end(),
        Frame::cancel("test"),
        Frame::new(FrameData::Stop),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let before = consume(&assistant, pdf_upload());
        let ended = consume(&assistant, terminal);
        let valid = consume(&assistant, pdf_upload());
        let invalid = consume(&assistant, edit_upload(|file| file.file.file.clear()));
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(before.unwrap());
        ended.unwrap();
        silent(valid.unwrap());
        silent(invalid.unwrap());
        assert_eq!(after.unwrap(), expected_rich());
    }
}

#[tokio::test]
async fn busy_pdf_rejection_preserves_buffered_text_and_normal_completion() {
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
        let frame = pdf_upload();
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
            "must not flush the prefix"
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
                interrupted: false
            }]
        );
        silent(retry.unwrap());
        let mut expected = expected_rich();
        if buffered {
            expected.messages.insert(
                0,
                ContextMessage {
                    role: ChatRole::Assistant,
                    content: MessageContent::Text("before after".into()),
                    name: None,
                    tool_call_id: None,
                    tool_calls: vec![],
                    extra: Default::default(),
                },
            );
        }
        assert_eq!(after.unwrap(), expected);
    }
}

#[tokio::test]
async fn speech_thought_awaited_inference_tools_and_playback_are_not_idle() {
    let mut awaited = Frame::new(FrameData::UserStoppedSpeaking);
    awaited
        .metadata
        .insert("user_context_follows".into(), json!(true));
    for prelude in [
        Frame::new(FrameData::UserStartedSpeaking),
        Frame::new(FrameData::BotStartedSpeaking),
        awaited,
        Frame::new(FrameData::LlmThoughtText(TextFrame::llm("held thought"))),
        Frame::new(FrameData::LlmThoughtStartDetailed(LlmThoughtStartFrame {
            append_to_context: true,
            llm: Some("test".into()),
        })),
        Frame::new(FrameData::FunctionCallInProgress(
            FunctionCallInProgressFrame {
                function_name: "pending".into(),
                tool_call_id: "pending".into(),
                arguments: json!({}),
                cancel_on_interruption: false,
                group_id: None,
            },
        )),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let setup = consume(&assistant, prelude);
        let before = context.snapshot();
        let rejected = consume(&assistant, pdf_upload());
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
    let rejected = consume(&assistant, pdf_upload());
    let after = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    playback.unwrap();
    assert!(rejected.unwrap_err().message.contains("idle assistant"));
    assert!(after.unwrap().messages.is_empty());
}

#[tokio::test]
async fn enabled_and_announced_realtime_pdf_rejects_without_mutation() {
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
        let rejected = consume(&assistant, pdf_upload());
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
async fn active_and_retained_signed_owner_reject_until_normal_playback_retirement() {
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
            NativeLlmContinuation::start(994111, LlmContinuationProvider::Gemini).unwrap(),
        ),
    );
    let playback = assistant.begin_playback("held");
    let text = consume(
        &assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm("signed answer"))),
    );
    let frame = pdf_upload();
    let active = consume(&assistant, frame.clone());
    let end = consume(
        &assistant,
        signed_frame(
            FrameData::LlmResponseEnd,
            NativeLlmContinuation::new(
                994111,
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
    let retained = consume(&assistant, frame.clone());
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
    receipt.unwrap();
    assert!(active.unwrap_err().message.contains("signed continuation"));
    assert!(retained
        .unwrap_err()
        .message
        .contains("signed continuation"));
    assert_eq!(before.unwrap(), unchanged.unwrap());
    silent(retry.unwrap());
    let prefix = prefix.unwrap();
    assert_eq!(prefix.messages.len(), 1);
    assert_eq!(
        prefix.messages[0].content,
        Some(Content::Parts(vec![ContentPart::GeminiText {
            text: "signed answer".into(),
            thought: false,
            signature: Some("opaque-signature".into()),
            incomplete: false
        }]))
    );
    let mut expected = expected_native();
    expected.messages.insert(0, prefix.messages[0].clone());
    assert_eq!(after.unwrap(), expected);
}

#[tokio::test]
async fn private_carriers_reject_before_replay_with_image_terminal_order() {
    let mut continuation = signed_frame(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(994121, LlmContinuationProvider::Gemini).unwrap(),
    );
    continuation.data = pdf_upload().data;
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
    declaration.data = pdf_upload().data;
    let source = EagerSourceSessionOwner::new(4096).unwrap();
    let mut eager = Frame::new(FrameData::ProposedUserStartedSpeaking);
    eager
        .set_native_eager_source(NativeEagerSource::Start(source.begin_turn().unwrap()))
        .unwrap();
    eager.data = pdf_upload().data;
    let owner = SpeculationOwner::new();
    let mut speculation = pdf_upload();
    speculation
        .set_native_speculation(owner.capability())
        .unwrap();
    let producer = NativeRealtimeContextProducer::default();
    let mut realtime = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: expected_rich(),
        speculation: false,
    }));
    producer
        .attach(&mut realtime, RealtimeContextIntent::Run, true)
        .unwrap();
    realtime.data = pdf_upload().data;
    for carrier in [
        continuation.clone(),
        declaration.clone(),
        eager,
        speculation,
        realtime,
    ] {
        for replay in [false, true] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let ordinary = pdf_upload();
            let mut bad = carrier.clone();
            bad.id = ordinary.id;
            let first = replay.then(|| consume(&assistant, ordinary.clone()));
            let before = context.snapshot();
            let rejected = consume(&assistant, bad);
            let unchanged = context.snapshot();
            let pending = assistant.has_function_calls_in_progress();
            let retry = consume(&assistant, ordinary);
            let after = context.snapshot();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            if let Some(first) = first {
                silent(first.unwrap());
            }
            assert!(rejected.is_err());
            assert!(!pending.unwrap());
            assert_eq!(before.unwrap(), unchanged.unwrap());
            silent(retry.unwrap());
            assert_eq!(after.unwrap(), expected_native());
        }
    }
    for terminal in [
        Frame::end(),
        Frame::cancel("test"),
        Frame::new(FrameData::Stop),
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let end = consume(&assistant, terminal);
        let ignored_continuation = consume(&assistant, continuation.clone());
        let rejected_tool = consume(&assistant, declaration.clone());
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        end.unwrap();
        silent(ignored_continuation.unwrap());
        assert!(rejected_tool.is_err());
        assert!(after.unwrap().messages.is_empty());
    }
}

#[tokio::test]
async fn rich_extras_and_direct_tools_keep_exact_storage_and_projection_restrictions() {
    let prefix = ContextMessage {
        role: ChatRole::User,
        content: MessageContent::Text("prior".into()),
        name: Some("prior name".into()),
        tool_call_id: None,
        tool_calls: vec![],
        extra: [("provider_extra".into(), json!({"exact": true}))].into(),
    };
    let original = LlmContextData {
        messages: vec![prefix],
        tools: ToolSet::Direct(vec![DirectFunction {
            schema: ToolDefinition {
                name: "local".into(),
                description: "retain identity".into(),
                parameters: json!({}),
            },
            handler: LocalToolFunction::new(|_| async { Ok(json!(7)) }),
        }]),
        tool_choice: Some(ToolChoice::Required),
    };
    let context = SharedContext::new_rich(original.clone());
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let before_native = context.snapshot();
    let accepted = consume(&assistant, pdf_upload());
    let after_rich = context.snapshot_rich();
    let after_native = context.snapshot();
    let after_frame = context.frame_data();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(accepted.unwrap());
    let mut expected = original;
    expected.messages.extend(expected_rich().messages);
    assert_eq!(after_rich.unwrap(), expected);
    assert_eq!(before_native.unwrap_err(), after_native.unwrap_err());
    assert_eq!(
        after_frame.unwrap(),
        FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: expected,
            speculation: false
        })
    );
}

#[tokio::test]
async fn native_signed_prefix_inline_data_and_tools_survive_without_rich_projection() {
    let signed = Message {
        role: Role::Assistant,
        content: Some(Content::Parts(vec![ContentPart::GeminiText {
            text: "prior signed answer".into(),
            thought: false,
            signature: Some("opaque-signature".into()),
            incomplete: false,
        }])),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
    };
    let inline = Message {
        role: Role::User,
        content: Some(Content::Parts(vec![ContentPart::InlineData {
            mime_type: "application/pdf".into(),
            data: "JVBERg==".into(),
        }])),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
    };
    let original = Context {
        messages: vec![signed, inline],
        tools: Some(vec![pipecat::services::ToolDefinition {
            name: "existing".into(),
            description: "retained".into(),
            parameters: json!({"type":"object"}),
        }]),
        tool_choice: Some(pipecat::services::ToolChoice::Required),
    };
    let context = SharedContext::new(original.clone());
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let before_rich = context.snapshot_rich();
    let before_frame = context.frame_data();
    let accepted = consume(&assistant, pdf_upload());
    let after_rich = context.snapshot_rich();
    let after_frame = context.frame_data();
    let after_native = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(accepted.unwrap());
    // InlineData is intentionally not representable as a rich Image versus File.
    let rich_error = before_rich.unwrap_err();
    assert!(rich_error.message.contains("InlineData"));
    assert_eq!(rich_error, after_rich.unwrap_err());
    assert_eq!(before_frame.unwrap_err(), after_frame.unwrap_err());
    let mut expected = original;
    expected.messages.extend(expected_native().messages);
    assert_eq!(after_native.unwrap(), expected);
}

#[tokio::test]
async fn failed_context_projection_leaves_plain_and_tool_inference_debt_blocking_files() {
    for tool_debt in [false, true] {
        let inline = Message {
            role: Role::User,
            content: Some(Content::Parts(vec![ContentPart::InlineData {
                mime_type: "application/pdf".into(),
                data: "JVBERg==".into(),
            }])),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
        };
        let context = SharedContext::new(Context::new(vec![inline]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let started = tool_debt.then(|| {
            consume(
                &assistant,
                Frame::new(FrameData::FunctionCallInProgress(
                    FunctionCallInProgressFrame {
                        function_name: "owned".into(),
                        tool_call_id: "owed".into(),
                        arguments: json!({}),
                        cancel_on_interruption: false,
                        group_id: None,
                    },
                )),
            )
        });
        let scheduling = if tool_debt {
            consume(
                &assistant,
                Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                    function_name: "owned".into(),
                    tool_call_id: "owed".into(),
                    run_llm: true,
                })),
            )
        } else {
            consume(&assistant, Frame::new(FrameData::LlmRun))
        };
        let pending = assistant.has_function_calls_in_progress();
        let before = context.snapshot();
        let rejected = consume(&assistant, pdf_upload());
        let after = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        if let Some(started) = started {
            started.unwrap();
        }
        assert!(scheduling.unwrap_err().message.contains("InlineData"));
        assert!(!pending.unwrap(), "debt survives without a pending tool");
        assert!(rejected.unwrap_err().message.contains("idle assistant"));
        assert_eq!(before.unwrap(), after.unwrap());
    }
}
