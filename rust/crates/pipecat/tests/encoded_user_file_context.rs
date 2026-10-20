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
async fn busy_pdf_true_and_none_rejection_preserves_buffered_text_and_normal_completion() {
    for run in [Some(true), None] {
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
            let frame = pdf_inference_upload(run);
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
            assert_eq!(pdf_inference_context(retry.unwrap()).context, expected);
            assert_eq!(after.unwrap(), expected);
        }
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

// Frozen PDF inference baselines. Only the earlier append/run rejection matrix changes.
fn pdf_inference_upload(run: Option<bool>) -> Frame {
    edit_upload(|file| file.run_llm = run)
}

fn pdf_inference_context(output: AggregationOutput) -> DetailedLlmContextFrame {
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

#[tokio::test]
async fn pdf_inference_baseline_idle_true_and_none_prepare_one_upstream_context() {
    for run in [Some(true), None] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let result = consume(&assistant, pdf_inference_upload(run));
        let rich = context.snapshot_rich();
        let native = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        // The frozen old implementation fails on Some(true), before reaching None.
        let projected = pdf_inference_context(
            result.expect("idle true/None detailed PDF must prepare one upstream context"),
        );
        assert_eq!(projected.context, expected_rich());
        assert_eq!(rich.unwrap(), expected_rich());
        assert_eq!(native.as_ref().unwrap(), &expected_native());
        assert_eq!(
            pipecat::services::frame_context::context(&projected.context).unwrap(),
            expected_native()
        );
        assert_eq!(
            request_body(
                Provider::OpenAi,
                &CompletionRequest::new("wire-fixture", native.unwrap())
            )
            .unwrap(),
            expected_openai()
        );
    }
}
async fn pdf_inference_default_rtvi_pipeline(
    context: SharedContext,
) -> Result<(Vec<DirectedFrame>, WorkerReport), String> {
    const WAIT: Duration = Duration::from_secs(5);
    const MARKER: &str = "after-default-pdf";
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
        // It permits a bounded old-failure run without waiting for a missing file/context.
        for value in [
            json!({
                "label": "rtvi-ai", "id": "default-pdf", "type": "send-file",
                "data": {
                    "content": "read",
                    "file": {"format": "application/pdf", "name": "document.pdf", "source": {"type": "bytes", "bytes": PDF_BASE64}}
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
async fn pdf_inference_baseline_rtvi_default_emits_one_exact_openai_context() {
    let context = SharedContext::default();
    let (frames, report) = pdf_inference_default_rtvi_pipeline(context.clone())
        .await
        .expect("default RTVI PDF fixture failed after cleanup");
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
        panic!("default PDF must emit a detailed context")
    };
    assert!(!projected.speculation);
    assert_eq!(projected.context, expected_rich());
    assert_eq!(context.snapshot_rich().unwrap(), expected_rich());
    assert!(!frames.iter().any(|directed| matches!(
        directed.frame.data,
        FrameData::UserFile(_) | FrameData::UserFileDetailed(_) | FrameData::UserImage(_)
    )));
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
    assert_eq!(native, expected_native());
    assert_eq!(native, context.snapshot().unwrap());
    assert_eq!(
        request_body(
            Provider::OpenAi,
            &CompletionRequest::new("wire-fixture", native)
        )
        .unwrap(),
        expected_openai()
    );
}

// Candidate controls. The two PDF inference baselines above remain frozen.
fn pdf_inference_no_context(output: AggregationOutput) {
    use pipecat::turns::AggregationAction;
    assert!(output.events.is_empty());
    assert_eq!(output.actions.len(), 1);
    let AggregationAction::Push(directed) = &output.actions[0] else {
        panic!("expected only the ordinary speaking-state frame")
    };
    assert!(matches!(directed.frame.data, FrameData::BotStoppedSpeaking));
}

fn pdf_inference_unprojectable_prefix() -> Message {
    Message {
        role: Role::User,
        content: Some(Content::Parts(vec![ContentPart::InlineData {
            mime_type: "application/pdf".into(),
            data: "JVBERg==".into(),
        }])),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
    }
}

#[tokio::test]
async fn pdf_inference_construction_retry_replay_and_distinct_ids() {
    for run in [Some(true), None] {
        for direction in [Direction::Downstream, Direction::Upstream] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let frame = pdf_inference_upload(run);
            let mut malformed = frame.clone();
            if let FrameData::UserFileDetailed(file) = &mut malformed.data {
                file.file.file = "data:application/pdf;base64,JVBERg==".into();
            }
            let rejected = assistant.process_at(malformed.clone(), direction, Duration::ZERO);
            let after_rejection = context.snapshot_rich();
            let first = assistant.process_at(frame.clone(), direction, Duration::ZERO);
            let replay = assistant.process_at(frame.clone(), direction, Duration::ZERO);
            let invalid_replay = assistant.process_at(malformed, direction, Duration::ZERO);
            let after_replay = context.snapshot_rich();
            let distinct = assistant.process_at(Frame::new(frame.data), direction, Duration::ZERO);
            let final_snapshot = context.snapshot_rich();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            assert!(rejected.is_err());
            assert!(after_rejection.unwrap().messages.is_empty());
            assert_eq!(
                pdf_inference_context(first.unwrap()).context,
                expected_rich()
            );
            silent(replay.unwrap());
            assert!(invalid_replay.is_err(), "validation still precedes replay");
            assert_eq!(after_replay.unwrap(), expected_rich());
            let mut expected = expected_rich();
            expected.messages.push(expected.messages[0].clone());
            assert_eq!(pdf_inference_context(distinct.unwrap()).context, expected);
            assert_eq!(final_snapshot.unwrap(), expected);
        }
    }
}

#[tokio::test]
async fn pdf_inference_projection_failure_is_atomic_and_same_id_recovers_after_repair() {
    for run in [Some(true), None] {
        let prefix = pdf_inference_unprojectable_prefix();
        let context = SharedContext::new(Context::new(vec![prefix.clone()]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let frame = pdf_inference_upload(run);
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
        pdf_inference_no_context(no_debt.unwrap());
        let retried = pdf_inference_context(retried.unwrap());
        let mut expected = expected_native();
        expected
            .messages
            .insert(0, Message::text(Role::User, "repaired"));
        assert_eq!(retried.context, after_retry.unwrap());
        assert_eq!(
            pipecat::services::frame_context::context(&retried.context).unwrap(),
            expected
        );
        silent(replay.unwrap());
        assert_eq!(
            pdf_inference_context(later_run.unwrap()).context,
            retried.context
        );
        pdf_inference_no_context(no_later_debt.unwrap());
        assert_eq!(final_snapshot.unwrap(), retried.context);
    }
}

#[tokio::test]
async fn pdf_inference_failed_run_can_retry_false_but_accepted_false_cannot_be_rearmed() {
    for run in [Some(true), None] {
        for unprojectable in [true, false] {
            let original = if unprojectable {
                Context::new(vec![pdf_inference_unprojectable_prefix()])
            } else {
                Context::default()
            };
            let context = SharedContext::new(original.clone());
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let frame = pdf_inference_upload(run);
            let rejected = unprojectable.then(|| consume(&assistant, frame.clone()));
            let after_rejection = context.snapshot();
            let mut append_only = frame.clone();
            if let FrameData::UserFileDetailed(file) = &mut append_only.data {
                file.run_llm = Some(false);
            }
            let recovered = consume(&assistant, append_only);
            let rearmed = consume(&assistant, frame);
            let after_rearm = context.snapshot();
            let projection = context.frame_data();
            let no_debt = consume(&assistant, Frame::new(FrameData::BotStoppedSpeaking));
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            if let Some(rejected) = rejected {
                assert!(rejected.unwrap_err().message.contains("InlineData"));
            }
            assert_eq!(after_rejection.unwrap(), original);
            silent(recovered.unwrap());
            silent(rearmed.unwrap());
            let mut expected = original;
            expected.messages.extend(expected_native().messages);
            assert_eq!(after_rearm.unwrap(), expected);
            assert_eq!(projection.is_err(), unprojectable);
            pdf_inference_no_context(no_debt.unwrap());
        }
    }
}

#[tokio::test]
async fn pdf_inference_completed_signed_prefix_keeps_exact_context() {
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
            tool_call_id: None,
            tool_calls: vec![],
        };
        let context = SharedContext::new(Context::new(vec![prefix.clone()]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let before = context.snapshot_rich();
        let accepted = consume(&assistant, pdf_inference_upload(run));
        let after = context.snapshot_rich();
        let native = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        let mut expected = before.unwrap();
        expected.messages.extend(expected_rich().messages);
        assert_eq!(pdf_inference_context(accepted.unwrap()).context, expected);
        assert_eq!(after.unwrap(), expected);
        let mut expected_native = expected_native();
        expected_native.messages.insert(0, prefix);
        assert_eq!(native.unwrap(), expected_native);
        assert_eq!(
            pipecat::services::frame_context::context(&expected).unwrap(),
            expected_native
        );
    }
}

#[tokio::test]
async fn pdf_inference_prepares_rich_extensions_and_direct_tools_before_service_rejection() {
    for run in [Some(true), None] {
        for direct_tool in [false, true] {
            let mut original = LlmContextData {
                messages: vec![ContextMessage {
                    role: ChatRole::User,
                    content: MessageContent::Text("prior".into()),
                    name: Some("prior name".into()),
                    tool_call_id: None,
                    tool_calls: vec![],
                    extra: Default::default(),
                }],
                tools: ToolSet::NotGiven,
                tool_choice: None,
            };
            if direct_tool {
                original.tools = ToolSet::Direct(vec![DirectFunction {
                    schema: ToolDefinition {
                        name: "local".into(),
                        description: "retain identity".into(),
                        parameters: json!({}),
                    },
                    handler: LocalToolFunction::new(|_| async { Ok(json!(7)) }),
                }]);
                original.tool_choice = Some(ToolChoice::Required);
            } else {
                original.messages[0].extra =
                    [("provider_extra".into(), json!({"exact": true}))].into();
            }
            let context = SharedContext::new_rich(original.clone());
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let frame = pdf_inference_upload(run);
            let prepared = consume(&assistant, frame.clone());
            let after = context.snapshot_rich();
            let native = context.snapshot();
            // No emit is attempted. Even an un-emitted prepared action is committed.
            let replay = consume(&assistant, frame);
            let after_replay = context.snapshot_rich();
            let no_debt = consume(&assistant, Frame::new(FrameData::BotStoppedSpeaking));
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            let mut expected = original;
            expected.messages.extend(expected_rich().messages);
            let prepared = pdf_inference_context(prepared.unwrap());
            assert_eq!(prepared.context, expected);
            assert_eq!(after.unwrap(), expected);
            let service_error =
                pipecat::services::frame_context::context(&prepared.context).unwrap_err();
            assert_eq!(native.unwrap_err().message, service_error.to_string());
            silent(replay.unwrap());
            assert_eq!(after_replay.unwrap(), expected);
            pdf_inference_no_context(no_debt.unwrap());
        }
    }
}

#[tokio::test]
async fn pdf_inference_true_and_none_keep_idle_realtime_authority_and_terminal_gates() {
    for run in [Some(true), None] {
        let mut awaited = Frame::new(FrameData::UserStoppedSpeaking);
        awaited
            .metadata
            .insert("user_context_follows".into(), json!(true));
        for prelude in [
            Frame::new(FrameData::LlmResponseStart),
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
            let rejected = consume(&assistant, pdf_inference_upload(run));
            let after = context.snapshot();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            setup.unwrap();
            assert!(rejected.unwrap_err().message.contains("idle assistant"));
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
                            service_name: "realtime-test".into(),
                            user_turn_strategies: None,
                        },
                        is_realtime_service: true,
                    })),
                )
            });
            let rejected = consume(&assistant, pdf_inference_upload(run));
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
        // Private authority is checked before the successful-ID replay fast path.
        let owner = SpeculationOwner::new();
        let mut private = pdf_inference_upload(run);
        private.set_native_speculation(owner.capability()).unwrap();
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut ordinary = pdf_inference_upload(run);
        ordinary.id = private.id;
        let accepted = consume(&assistant, ordinary);
        let rejected = consume(&assistant, private);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert_eq!(
            pdf_inference_context(accepted.unwrap()).context,
            expected_rich()
        );
        assert!(rejected.is_err());
        assert_eq!(after.unwrap(), expected_rich());
        for terminal in [
            Frame::end(),
            Frame::cancel("test"),
            Frame::new(FrameData::Stop),
        ] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let ended = consume(&assistant, terminal);
            let ignored = consume(&assistant, pdf_inference_upload(run));
            let mut malformed = pdf_inference_upload(run);
            if let FrameData::UserFileDetailed(file) = &mut malformed.data {
                file.file.file.clear();
            }
            let invalid_ignored = consume(&assistant, malformed);
            let after = context.snapshot_rich();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            ended.unwrap();
            silent(ignored.unwrap());
            silent(invalid_ignored.unwrap());
            assert!(after.unwrap().messages.is_empty());
        }
    }
}

#[tokio::test]
async fn pdf_inference_keeps_buffered_playback_signed_owner_and_inference_debt_gates() {
    for run in [Some(true), None] {
        // An active unsigned text response must neither flush nor append a PDF.
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let text = consume(
            &assistant,
            Frame::new(FrameData::LlmText(TextFrame::llm("held"))),
        );
        let frame = pdf_inference_upload(run);
        let rejected = consume(&assistant, frame.clone());
        let before_end = context.snapshot_rich();
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let accepted = consume(&assistant, frame);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
        silent(text.unwrap());
        assert!(rejected.unwrap_err().message.contains("idle assistant"));
        assert!(before_end.unwrap().messages.is_empty());
        end.unwrap();
        let projected = pdf_inference_context(accepted.unwrap());
        assert_eq!(projected.context, after.unwrap());
        assert_eq!(projected.context.messages.len(), 2);
        assert_eq!(projected.context.messages[0].role, ChatRole::Assistant);
        assert_eq!(
            projected.context.messages[0].content,
            MessageContent::Text("held".into())
        );
        assert_eq!(projected.context.messages[1], expected_rich().messages[0]);

        for signed in [false, true] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(
                context.clone(),
                AssistantAggregatorConfig {
                    text_mode: AssistantTextMode::Spoken,
                },
            );
            let start = signed.then(|| {
                consume(
                    &assistant,
                    signed_frame(
                        FrameData::LlmResponseStart,
                        NativeLlmContinuation::start(994171, LlmContinuationProvider::Gemini)
                            .unwrap(),
                    ),
                )
            });
            let playback = assistant.begin_playback("held");
            let text = signed.then(|| {
                consume(
                    &assistant,
                    Frame::new(FrameData::LlmText(TextFrame::llm("signed answer"))),
                )
            });
            let frame = pdf_inference_upload(run);
            let active = consume(&assistant, frame.clone());
            let end = signed.then(|| {
                consume(
                    &assistant,
                    signed_frame(
                        FrameData::LlmResponseEnd,
                        NativeLlmContinuation::new(
                            994171,
                            LlmContinuationProvider::Gemini,
                            vec![LlmContinuationBlock::GeminiText {
                                text: "signed answer".into(),
                                thought: false,
                                signature: Some("opaque-signature".into()),
                            }],
                        )
                        .unwrap(),
                    ),
                )
            });
            let before = context.snapshot();
            let retained = consume(&assistant, frame);
            let after = context.snapshot();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            if let Some(start) = start {
                start.unwrap();
            }
            if let Some(end) = end {
                end.unwrap();
            }
            if let Some(text) = text {
                text.unwrap();
            }
            playback.unwrap();
            let reason = if signed {
                "signed continuation"
            } else {
                "idle assistant"
            };
            assert!(active.unwrap_err().message.contains(reason));
            assert!(retained.unwrap_err().message.contains(reason));
            assert_eq!(before.unwrap(), after.unwrap());
        }
        for tool_debt in [false, true] {
            let context =
                SharedContext::new(Context::new(vec![pdf_inference_unprojectable_prefix()]));
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
            let before = context.snapshot();
            let rejected = consume(&assistant, pdf_inference_upload(run));
            let after = context.snapshot();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            if let Some(started) = started {
                started.unwrap();
            }
            assert!(scheduling.unwrap_err().message.contains("InlineData"));
            assert!(rejected.unwrap_err().message.contains("idle assistant"));
            assert_eq!(before.unwrap(), after.unwrap());
        }
    }
}

// Busy-PDF baselines. Preserve these two tests and helpers byte-for-byte.
use pipecat::turns::{
    AggregationAction, AggregationEvent, AssistantStoppedReceiver, AssistantTurnStopped,
};
const BUSY_PDF_WAIT: Duration = Duration::from_secs(5);

fn busy_pdf_message(role: ChatRole, content: MessageContent) -> ContextMessage {
    ContextMessage {
        role,
        content,
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: Default::default(),
    }
}
fn busy_pdf_assistant_message(text: &str) -> ContextMessage {
    busy_pdf_message(ChatRole::Assistant, MessageContent::Text(text.into()))
}
fn busy_pdf_file_message() -> ContextMessage {
    expected_rich().messages.remove(0)
}
fn busy_pdf_consume(
    assistant: &LLMAssistantAggregator,
    frame: Frame,
) -> Result<AggregationOutput, ProcessorError> {
    assistant.process_at(frame, Direction::Downstream, Duration::ZERO)
}
fn busy_pdf_text(text: &str) -> Frame {
    Frame::new(FrameData::LlmText(TextFrame::llm(text)))
}
fn busy_pdf_silent(output: &AggregationOutput) {
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
fn busy_pdf_admit(
    output: &Result<AggregationOutput, ProcessorError>,
) -> Result<(), ProcessorError> {
    if let Ok(output) = output {
        for action in &output.actions {
            if let AggregationAction::AssistantStopped(action) = action {
                action.admit()?;
            }
        }
    }
    Ok(())
}
fn busy_pdf_records(receiver: &mut AssistantStoppedReceiver) -> Vec<Arc<AssistantTurnStopped>> {
    let mut records = vec![];
    while let Some(record) = receiver.try_recv() {
        records.push(record.completion().clone());
    }
    records
}
fn busy_pdf_end_effects(output: &AggregationOutput, text: &str, record: &AssistantTurnStopped) {
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

#[tokio::test]
async fn busy_pdf_baseline_direct_flushes_prefix_and_stops_once_with_suffix() {
    // The ordinary text-only case is a positive control against fixture failures.
    for with_file in [false, true] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let prefix = busy_pdf_consume(&assistant, busy_pdf_text("A"));
        let accepted = with_file.then(|| busy_pdf_consume(&assistant, pdf_upload()));
        let during = context.snapshot_rich();
        let premature = busy_pdf_records(&mut receiver);
        let during_stats = receiver.stats();
        let suffix = busy_pdf_consume(&assistant, busy_pdf_text("B"));
        let end = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let admitted = busy_pdf_admit(&end);
        let duplicate_end = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let again = busy_pdf_admit(&duplicate_end);
        let completed = busy_pdf_records(&mut receiver);
        let snapshot = context.snapshot_rich();
        let terminal = busy_pdf_consume(&assistant, Frame::end());
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        // No behavior assertion (in particular no expected PDF success) precedes cleanup.
        cleaned.unwrap();
        terminal.unwrap();
        assert_eq!(
            start.unwrap().events,
            vec![AggregationEvent::AssistantTurnStarted]
        );
        busy_pdf_silent(&prefix.unwrap());
        if let Some(accepted) = accepted {
            busy_pdf_silent(&accepted.expect("ordinary active false-run PDF must be admitted"));
        }
        assert!(premature.is_empty());
        assert_eq!(during_stats.reserved, 0);
        assert_eq!(
            during.unwrap().messages,
            if with_file {
                vec![busy_pdf_assistant_message("A"), busy_pdf_file_message()]
            } else {
                vec![]
            }
        );
        busy_pdf_silent(&suffix.unwrap());
        admitted.unwrap();
        again.unwrap();
        busy_pdf_silent(&duplicate_end.unwrap());
        assert_eq!(completed.len(), 1);
        let expected = if with_file { "B" } else { "AB" };
        busy_pdf_end_effects(&end.unwrap(), expected, &completed[0]);
        assert_eq!(
            snapshot.unwrap().messages,
            if with_file {
                vec![
                    busy_pdf_assistant_message("A"),
                    busy_pdf_file_message(),
                    busy_pdf_assistant_message("B"),
                ]
            } else {
                vec![busy_pdf_assistant_message("AB")]
            }
        );
    }
}

async fn busy_pdf_send(worker: &PipelineWorker, frame: Frame) -> Result<(), String> {
    tokio::time::timeout(
        BUSY_PDF_WAIT,
        worker.sender().send(frame, Direction::Downstream),
    )
    .await
    .map_err(|_| "send deadline".to_string())?
    .map_err(|e| e.to_string())
}
async fn busy_pdf_drain_to(
    worker: &mut PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
    matches: impl Fn(&Frame) -> bool,
) -> Result<(), String> {
    tokio::time::timeout(BUSY_PDF_WAIT, async {
        while let Some(directed) = worker.recv().await {
            let done = matches(&directed.frame);
            frames.push(directed);
            if done {
                return Ok(());
            }
        }
        Err("pipeline closed before FIFO marker".to_string())
    })
    .await
    .map_err(|_| "FIFO marker deadline".to_string())?
}
async fn busy_pdf_fence(
    worker: &mut PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
) -> Result<(), String> {
    let frame = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("busy-pdf-fixture".into()),
        returning: false,
    }));
    let id = frame.id;
    busy_pdf_send(worker, frame).await?;
    busy_pdf_drain_to(worker, frames, |frame| {
        frame.id == id && matches!(&frame.data, FrameData::Flush(flush) if flush.returning)
    })
    .await
}
struct BusyPdfPipelineOutcome {
    frames: Vec<DirectedFrame>,
    report: WorkerReport,
    during: LlmContextData,
    during_frames: usize,
    premature: Vec<Arc<AssistantTurnStopped>>,
    reserved_during: u64,
    completed: Vec<Arc<AssistantTurnStopped>>,
}
async fn busy_pdf_rtvi_pipeline(
    context: SharedContext,
    busy: bool,
) -> Result<BusyPdfPipelineOutcome, String> {
    let rtvi = RtviProcessor::new(RtviConfig::default()).map_err(|e| e.to_string())?;
    let assistant = Arc::new(LLMAssistantAggregator::new(
        context.clone(),
        Default::default(),
    ));
    let mut receiver = assistant
        .subscribe_stopped(Default::default())
        .map_err(|e| e.to_string())?;
    let mut worker = Pipeline::new(vec![Arc::new(rtvi), assistant])
        .start(PipelineConfig {
            startup_timeout: BUSY_PDF_WAIT,
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    let mut frames = vec![];
    let operation = async {
        if busy {
            busy_pdf_send(&worker, Frame::new(FrameData::LlmResponseStart)).await?;
            busy_pdf_send(&worker, busy_pdf_text("A")).await?;
        }
        busy_pdf_fence(&mut worker, &mut frames).await?;
        for value in [
            json!({"label": "rtvi-ai", "id": "busy-upload", "type": "send-file", "data": {
                "content": "read", "file": {"format": "application/pdf", "name": "document.pdf", "source": {"type": "bytes", "bytes": PDF_BASE64}},
                "options": {"run_immediately": false, "audio_response": true}
            }}),
            json!({"label": "rtvi-ai", "id": "after-busy-upload", "type": "client-message", "data": {"t": "test-fence", "d": null}}),
        ] {
            busy_pdf_send(&worker, Frame::new(FrameData::InputTransportMessage(value))).await?;
        }
        // This System command-loop FIFO marker proves the generated UserFileDetailed
        // traversed the assistant, not that any context or turn completed.
        // It is reached on the old rejection path too; no missing-success timeout.
        busy_pdf_drain_to(&mut worker, &mut frames, |frame| matches!(&frame.data,
            FrameData::RtviClientMessage(message) if message.msg_id == "after-busy-upload")).await?;
        let during = context.snapshot_rich().map_err(|e| e.to_string())?;
        let during_frames = frames.len();
        let premature = busy_pdf_records(&mut receiver);
        let reserved_during = receiver.stats().reserved;
        if busy {
            busy_pdf_send(&worker, busy_pdf_text("B")).await?;
            busy_pdf_send(&worker, Frame::new(FrameData::LlmResponseEnd)).await?;
            busy_pdf_send(&worker, Frame::new(FrameData::LlmResponseEnd)).await?;
        }
        busy_pdf_fence(&mut worker, &mut frames).await?;
        Ok::<_, String>((during, during_frames, premature, reserved_during))
    }.await;
    // End, drain and join even when the old implementation rejects the PDF.
    let end = tokio::time::timeout(BUSY_PDF_WAIT, worker.end()).await;
    if !matches!(&end, Ok(Ok(()))) {
        let _ = tokio::time::timeout(BUSY_PDF_WAIT, worker.cancel("busy upload End failed")).await;
    }
    let drain = tokio::time::timeout(BUSY_PDF_WAIT, async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
    })
    .await;
    if drain.is_err() {
        let _ =
            tokio::time::timeout(BUSY_PDF_WAIT, worker.cancel("busy upload drain failed")).await;
    }
    let join = tokio::time::timeout(BUSY_PDF_WAIT, worker.join()).await;
    if !matches!(&join, Ok(Ok(_))) {
        let _ = tokio::time::timeout(BUSY_PDF_WAIT, worker.cancel("busy upload join failed")).await;
        let _ = tokio::time::timeout(BUSY_PDF_WAIT, worker.join()).await;
    }
    let report = join
        .map_err(|_| "join deadline".to_string())?
        .map_err(|e| e.to_string())?;
    end.map_err(|_| "End deadline".to_string())?
        .map_err(|e| e.to_string())?;
    drain.map_err(|_| "drain deadline".to_string())?;
    let (during, during_frames, premature, reserved_during) = operation?;
    Ok(BusyPdfPipelineOutcome {
        frames,
        report,
        during,
        during_frames,
        premature,
        reserved_during,
        completed: busy_pdf_records(&mut receiver),
    })
}
fn busy_pdf_turn_effect(frame: &Frame) -> bool {
    matches!(
        frame.data,
        FrameData::LlmContext(_)
            | FrameData::LlmContextDetailed(_)
            | FrameData::LlmContextAssistantTimestamp { .. }
            | FrameData::LlmContextAssistantTurn(_)
    )
}

#[tokio::test]
async fn busy_pdf_baseline_rtvi_false_run_upload_flushes_without_premature_turn_effects() {
    // Idle upload is the existing positive control in exactly the same real topology.
    for busy in [false, true] {
        let context = SharedContext::default();
        let outcome = busy_pdf_rtvi_pipeline(context.clone(), busy)
            .await
            .expect("RTVI fixture after cleanup");
        assert_eq!(outcome.report.state, WorkerState::Ended);
        assert!(
            outcome.report.errors.is_empty(),
            "actual pipeline rejection: {:?}",
            outcome.report
        );
        assert!(outcome.premature.is_empty());
        assert_eq!(outcome.reserved_during, 0);
        assert!(!outcome.frames[..outcome.during_frames]
            .iter()
            .any(|f| busy_pdf_turn_effect(&f.frame)));
        assert!(!outcome.frames.iter().any(|f| matches!(
            f.frame.data,
            FrameData::Interruption | FrameData::UserFileDetailed(_) | FrameData::Error(_)
        )));
        assert_eq!(
            outcome.during.messages,
            if busy {
                vec![busy_pdf_assistant_message("A"), busy_pdf_file_message()]
            } else {
                vec![busy_pdf_file_message()]
            }
        );
        assert_eq!(
            context.snapshot_rich().unwrap().messages,
            if busy {
                vec![
                    busy_pdf_assistant_message("A"),
                    busy_pdf_file_message(),
                    busy_pdf_assistant_message("B"),
                ]
            } else {
                vec![busy_pdf_file_message()]
            }
        );
        assert_eq!(outcome.completed.len(), usize::from(busy));
        let effects: Vec<_> = outcome
            .frames
            .iter()
            .filter(|f| busy_pdf_turn_effect(&f.frame))
            .collect();
        assert_eq!(effects.len(), if busy { 4 } else { 0 });
        if busy {
            let record = &outcome.completed[0];
            assert_eq!(record.text(), "B");
            assert_eq!(record.turn_id(), 1);
            assert!(!record.interrupted());
            let contexts: Vec<_> = effects
                .iter()
                .filter(|f| {
                    matches!(
                        f.frame.data,
                        FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                    )
                })
                .collect();
            assert_eq!(contexts.len(), 1);
            assert_eq!(contexts[0].direction, Direction::Downstream);
            for direction in [Direction::Downstream, Direction::Upstream] {
                let turns: Vec<_> = effects
                    .iter()
                    .filter_map(|f| match &f.frame.data {
                        FrameData::LlmContextAssistantTurn(turn) if f.direction == direction => {
                            Some(turn)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(turns.len(), 1);
                assert_eq!(turns[0].text, "B");
                assert_eq!(turns[0].timestamp, record.started_at());
            }
        }
    }
}

// Candidate-only busy-PDF controls; the two appended baselines remain byte-frozen.
fn busy_pdf_upload_with_text(text: &str) -> Frame {
    edit_upload(|file| file.text = text.into())
}
fn busy_pdf_file_with_text(text: &str) -> ContextMessage {
    let mut message = busy_pdf_file_message();
    if let MessageContent::Parts(parts) = &mut message.content {
        parts[0] = MessagePart::Text { text: text.into() };
    }
    message
}

async fn busy_pdf_finish_direct(
    assistant: LLMAssistantAggregator,
    receiver: &mut AssistantStoppedReceiver,
) -> (
    AggregationOutput,
    Vec<Arc<AssistantTurnStopped>>,
    LlmContextData,
) {
    let end = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let admitted = busy_pdf_admit(&end);
    let duplicate = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let admitted_again = busy_pdf_admit(&duplicate);
    let completed = busy_pdf_records(receiver);
    let snapshot = assistant.context().snapshot_rich();
    let terminal = busy_pdf_consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    terminal.unwrap();
    admitted.unwrap();
    admitted_again.unwrap();
    busy_pdf_silent(&duplicate.unwrap());
    (end.unwrap(), completed, snapshot.unwrap())
}

#[tokio::test]
async fn busy_pdf_empty_and_whitespace_prefixes_keep_source_presence_semantics() {
    for prefix in ["", " \n\t ", " A "] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let held = busy_pdf_consume(&assistant, busy_pdf_text(prefix));
        let file = busy_pdf_consume(&assistant, busy_pdf_upload_with_text("inspect"));
        let during = context.snapshot_rich();
        let premature = busy_pdf_records(&mut receiver);
        let (end, completed, final_context) =
            busy_pdf_finish_direct(assistant, &mut receiver).await;
        start.unwrap();
        busy_pdf_silent(&held.unwrap());
        busy_pdf_silent(&file.unwrap());
        assert!(premature.is_empty());
        let mut expected = vec![];
        if !prefix.is_empty() {
            expected.push(busy_pdf_assistant_message(prefix.trim()));
        }
        expected.push(busy_pdf_file_with_text("inspect"));
        assert_eq!(during.unwrap().messages, expected);
        assert_eq!(final_context.messages, expected);
        assert_eq!(completed.len(), 1);
        busy_pdf_end_effects(&end, "", &completed[0]);
    }
}

#[tokio::test]
async fn busy_pdf_multiple_distinct_uploads_commit_each_segment_in_order() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let mut outputs = vec![busy_pdf_consume(
        &assistant,
        Frame::new(FrameData::LlmResponseStart),
    )];
    let mut snapshots = vec![];
    for (prefix, caption) in [(" A ", "first"), (" B ", "second")] {
        outputs.push(busy_pdf_consume(&assistant, busy_pdf_text(prefix)));
        outputs.push(busy_pdf_consume(
            &assistant,
            busy_pdf_upload_with_text(caption),
        ));
        snapshots.push(context.snapshot_rich());
    }
    outputs.push(busy_pdf_consume(&assistant, busy_pdf_text(" C ")));
    let premature = busy_pdf_records(&mut receiver);
    let (end, completed, snapshot) = busy_pdf_finish_direct(assistant, &mut receiver).await;
    outputs.remove(0).unwrap();
    for output in outputs {
        busy_pdf_silent(&output.unwrap());
    }
    assert!(premature.is_empty());
    assert_eq!(
        snapshots.remove(0).unwrap().messages,
        vec![
            busy_pdf_assistant_message("A"),
            busy_pdf_file_with_text("first")
        ]
    );
    assert_eq!(
        snapshots.remove(0).unwrap().messages,
        vec![
            busy_pdf_assistant_message("A"),
            busy_pdf_file_with_text("first"),
            busy_pdf_assistant_message("B"),
            busy_pdf_file_with_text("second")
        ]
    );
    assert_eq!(
        snapshot.messages,
        vec![
            busy_pdf_assistant_message("A"),
            busy_pdf_file_with_text("first"),
            busy_pdf_assistant_message("B"),
            busy_pdf_file_with_text("second"),
            busy_pdf_assistant_message("C")
        ]
    );
    assert_eq!(completed.len(), 1);
    busy_pdf_end_effects(&end, "C", &completed[0]);
}

#[tokio::test]
async fn busy_pdf_same_id_replay_cannot_flush_later_suffix_or_skip_validation() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let start = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let a = busy_pdf_consume(&assistant, busy_pdf_text("A"));
    let frame = busy_pdf_upload_with_text("inspect");
    let first = busy_pdf_consume(&assistant, frame.clone());
    let b = busy_pdf_consume(&assistant, busy_pdf_text("B"));
    let before = context.snapshot_rich();
    let replay = busy_pdf_consume(&assistant, frame.clone());
    let mut malformed = frame;
    if let FrameData::UserFileDetailed(file) = &mut malformed.data {
        file.file.file.clear();
    }
    let invalid = busy_pdf_consume(&assistant, malformed);
    let after = context.snapshot_rich();
    let c = busy_pdf_consume(&assistant, busy_pdf_text("C"));
    let (end, completed, snapshot) = busy_pdf_finish_direct(assistant, &mut receiver).await;
    start.unwrap();
    for result in [a, first, b, replay, c] {
        busy_pdf_silent(&result.unwrap());
    }
    assert!(invalid
        .unwrap_err()
        .message
        .contains("exact PDF base64 data URL header"));
    assert_eq!(before.unwrap(), after.unwrap());
    assert_eq!(
        snapshot.messages,
        vec![
            busy_pdf_assistant_message("A"),
            busy_pdf_file_with_text("inspect"),
            busy_pdf_assistant_message("BC")
        ]
    );
    assert_eq!(completed.len(), 1);
    busy_pdf_end_effects(&end, "BC", &completed[0]);
}

#[tokio::test]
async fn busy_pdf_invalid_file_preserves_prefix_and_allows_repaired_same_id() {
    for invalid_kind in ["empty", "header", "noncanonical"] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let a = busy_pdf_consume(&assistant, busy_pdf_text("A"));
        let repaired = busy_pdf_upload_with_text("inspect");
        let mut malformed = repaired.clone();
        if let FrameData::UserFileDetailed(file) = &mut malformed.data {
            match invalid_kind {
                "empty" => file.file.file.clear(),
                "header" => {
                    file.file.file = format!("data:application/octet-stream;base64,{PDF_BASE64}")
                }
                _ => file.file.file.push(' '),
            }
        }
        let rejected = busy_pdf_consume(&assistant, malformed);
        let unchanged = context.snapshot_rich();
        let b = busy_pdf_consume(&assistant, busy_pdf_text("B"));
        let accepted = busy_pdf_consume(&assistant, repaired);
        let after = context.snapshot_rich();
        let c = busy_pdf_consume(&assistant, busy_pdf_text("C"));
        let (end, completed, snapshot) = busy_pdf_finish_direct(assistant, &mut receiver).await;
        start.unwrap();
        for result in [a, b, accepted, c] {
            busy_pdf_silent(&result.unwrap());
        }
        let error = rejected.unwrap_err().message;
        let expected_error = if invalid_kind == "noncanonical" {
            "canonical STANDARD base64"
        } else {
            "exact PDF base64 data URL header"
        };
        assert!(error.contains(expected_error), "{invalid_kind}: {error}");
        assert!(unchanged.unwrap().messages.is_empty());
        assert_eq!(
            after.unwrap().messages,
            vec![
                busy_pdf_assistant_message("AB"),
                busy_pdf_file_with_text("inspect")
            ]
        );
        assert_eq!(
            snapshot.messages,
            vec![
                busy_pdf_assistant_message("AB"),
                busy_pdf_file_with_text("inspect"),
                busy_pdf_assistant_message("C")
            ]
        );
        assert_eq!(completed.len(), 1);
        busy_pdf_end_effects(&end, "C", &completed[0]);
    }
}

#[tokio::test]
async fn busy_pdf_append_only_preserves_completed_signed_and_unprojectable_native_history() {
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
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let start = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let held = busy_pdf_consume(&assistant, busy_pdf_text("A"));
    let before = context.frame_data();
    let accepted = busy_pdf_consume(&assistant, busy_pdf_upload_with_text("read"));
    let after = context.frame_data();
    let native = context.snapshot();
    // No suffix: final-end emission must not introduce an independent projection.
    let end = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let admitted = busy_pdf_admit(&end);
    let duplicate = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
    let admitted_again = busy_pdf_admit(&duplicate);
    let completed = busy_pdf_records(&mut receiver);
    let final_native = context.snapshot();
    let final_rich = context.snapshot_rich();
    let terminal = busy_pdf_consume(&assistant, Frame::end());
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    terminal.unwrap();
    admitted.unwrap();
    admitted_again.unwrap();
    busy_pdf_silent(&duplicate.unwrap());
    start.unwrap();
    busy_pdf_silent(&held.unwrap());
    busy_pdf_silent(&accepted.unwrap());
    assert!(before.unwrap_err().message.contains("InlineData"));
    assert!(after.unwrap_err().message.contains("InlineData"));
    assert!(final_rich.unwrap_err().message.contains("InlineData"));
    let mut expected = original;
    expected.messages.push(Message::text(Role::Assistant, "A"));
    expected.messages.extend(expected_native().messages);
    assert_eq!(native.unwrap(), expected);
    assert_eq!(final_native.unwrap(), expected);
    assert_eq!(completed.len(), 1);
    busy_pdf_end_effects(&end.unwrap(), "", &completed[0]);
}

#[tokio::test]
async fn busy_pdf_marker_projection_and_job_identity_survive_midturn_flush() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let correlation = assistant.bind_job_generation(73).unwrap();
    let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
    let start = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseStart));
    let a = busy_pdf_consume(&assistant, busy_pdf_text("A"));
    let marker = busy_pdf_consume(
        &assistant,
        Frame::new(FrameData::LlmMarker(LlmMarkerFrame {
            marker: "<DONE>".into(),
            append_to_context_immediately: false,
        })),
    );
    let file = busy_pdf_consume(&assistant, busy_pdf_upload_with_text("inspect"));
    let premature = busy_pdf_records(&mut receiver);
    let b = busy_pdf_consume(&assistant, busy_pdf_text("B<DONE>"));
    let (end, completed, snapshot) = busy_pdf_finish_direct(assistant, &mut receiver).await;
    assert_eq!(
        start.unwrap().events,
        vec![AggregationEvent::AssistantTurnStarted]
    );
    for result in [a, marker, file, b] {
        busy_pdf_silent(&result.unwrap());
    }
    assert!(premature.is_empty());
    assert_eq!(
        snapshot.messages,
        vec![
            busy_pdf_assistant_message("A <DONE>"),
            busy_pdf_file_with_text("inspect"),
            busy_pdf_assistant_message("B<DONE>")
        ]
    );
    assert_eq!(completed.len(), 1);
    assert!(correlation.matches(&completed[0]));
    assert_eq!(completed[0].job_generation(), Some(73));
    busy_pdf_end_effects(&end, "B", &completed[0]);
}

#[tokio::test]
async fn busy_pdf_interruption_cancel_and_end_only_commit_unflushed_suffix() {
    for terminal in [
        Frame::interruption(),
        Frame::cancel("fixture"),
        Frame::end(),
    ] {
        let interrupted = !matches!(terminal.data, FrameData::End(_));
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let a = busy_pdf_consume(&assistant, busy_pdf_text("A"));
        let file = busy_pdf_consume(&assistant, busy_pdf_upload_with_text("inspect"));
        let b = busy_pdf_consume(&assistant, busy_pdf_text("B"));
        let stopped = busy_pdf_consume(&assistant, terminal);
        let admitted = busy_pdf_admit(&stopped);
        let repeated = busy_pdf_consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let again = busy_pdf_admit(&repeated);
        let completed = busy_pdf_records(&mut receiver);
        let snapshot = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
        for result in [a, file, b] {
            busy_pdf_silent(&result.unwrap());
        }
        admitted.unwrap();
        again.unwrap();
        busy_pdf_silent(&repeated.unwrap());
        assert_eq!(
            stopped.unwrap().events,
            vec![AggregationEvent::AssistantTurnStopped {
                text: "B".into(),
                interrupted
            }]
        );
        assert_eq!(
            snapshot.unwrap().messages,
            vec![
                busy_pdf_assistant_message("A"),
                busy_pdf_file_with_text("inspect"),
                busy_pdf_assistant_message("B")
            ]
        );
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].text(), "B");
        assert_eq!(completed[0].interrupted(), interrupted);
        assert_eq!(completed[0].turn_id(), 1);
    }
}

#[tokio::test]
async fn busy_pdf_spoken_tts_only_thought_speech_tools_and_inference_debt_still_reject() {
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
    ] {
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
        let mut setup = vec![];
        setup.push(busy_pdf_consume(
            &assistant,
            Frame::new(if gate == "tts-only" {
                FrameData::TtsStarted(Default::default())
            } else {
                FrameData::LlmResponseStart
            }),
        ));
        let mut prefix = TextFrame::llm("A");
        prefix.skip_tts = Some(true);
        setup.push(busy_pdf_consume(
            &assistant,
            Frame::new(FrameData::LlmText(prefix)),
        ));
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
            "tool" => Some(Frame::new(FrameData::FunctionCallInProgress(
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
            setup.push(busy_pdf_consume(&assistant, prelude));
        }
        let playback = (gate == "playback").then(|| assistant.begin_playback("pending"));
        let before = context.snapshot_rich();
        let rejected = busy_pdf_consume(&assistant, busy_pdf_upload_with_text("inspect"));
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        for result in setup {
            result.unwrap();
        }
        if let Some(playback) = playback {
            playback.unwrap();
        }
        assert!(
            rejected.unwrap_err().message.contains("idle assistant"),
            "{gate}"
        );
        assert_eq!(before.unwrap(), after.unwrap(), "{gate}");
    }
}

#[tokio::test]
async fn busy_pdf_unsigned_so_far_native_owner_still_rejects() {
    for provider in [
        LlmContinuationProvider::Gemini,
        LlmContinuationProvider::Anthropic,
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let frame = signed_frame(
            FrameData::LlmResponseStart,
            NativeLlmContinuation::start(773, provider).unwrap(),
        );
        let start = busy_pdf_consume(&assistant, frame);
        let held = busy_pdf_consume(&assistant, busy_pdf_text("A"));
        let before = context.snapshot_rich();
        let rejected = busy_pdf_consume(&assistant, busy_pdf_upload_with_text("read"));
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
        held.unwrap();
        assert!(rejected
            .unwrap_err()
            .message
            .contains("signed continuation"));
        assert_eq!(before.unwrap(), after.unwrap());
    }
}

#[tokio::test]
async fn actual_rtvi_pdf_reaches_anthropic_media_first_without_mutating_context() {
    let frame = rtvi_pdf_upload(PDF_BASE64, "read", Some("document.pdf"))
        .await
        .expect("RTVI fixture failed after cleanup");
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let accepted = assistant.process_at(frame, Direction::Downstream, Duration::ZERO);
    let native = context.snapshot();
    let rich = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(accepted.unwrap());
    assert_eq!(rich.unwrap(), expected_rich());
    assert_eq!(native.as_ref().unwrap(), &expected_native());
    let request = CompletionRequest::new("wire-fixture", native.unwrap());
    let before = serde_json::to_value(&request).unwrap();
    let body = request_body(Provider::Anthropic, &request).unwrap();
    assert_eq!(
        body,
        json!({
            "model":"wire-fixture","stream":true,"max_tokens":4096,
            "messages":[{"role":"user","content":[
                {"type":"document","source":{"type":"base64","media_type":"application/pdf","data":PDF_BASE64},"title":"document.pdf"},
                {"type":"text","text":"read"}
            ]}]
        })
    );
    assert_eq!(request_body(Provider::Anthropic, &request).unwrap(), body);
    assert_eq!(serde_json::to_value(&request).unwrap(), before);
    assert_eq!(
        request_body(Provider::OpenAi, &request).unwrap(),
        expected_openai()
    );
}
