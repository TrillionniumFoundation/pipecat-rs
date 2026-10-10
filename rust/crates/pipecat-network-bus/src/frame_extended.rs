//! Explicit conversions for network-safe source payloads. Executable state is
//! rejected even when Python's serializer would silently omit it.
use super::*;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub(super) const TYPES: &[&str] = &[
    "Frame",
    "SystemFrame",
    "DataFrame",
    "ControlFrame",
    "SpeechOutputAudioRawFrame",
    "UserAudioRawFrame",
    "InputImageRawFrame",
    "OutputImageRawFrame",
    "URLImageRawFrame",
    "AssistantImageRawFrame",
    "InputFileRawFrame",
    "UserFileRawFrame",
    "UserImageRawFrame",
    "UserImageRequestFrame",
    "AggregatedTextFrame",
    "TTSTextFrame",
    "AggregatedTextProgressFrame",
    "TranscriptionFrame",
    "InterimTranscriptionFrame",
    "EagerTranscriptionFrame",
    "TranslationFrame",
    "LLMMarkerFrame",
    "LLMMarkerResponseFrame",
    "LLMContextAssistantTimestampFrame",
    "LLMContextAssistantTurnFrame",
    "LLMMessagesAppendFrame",
    "LLMMessagesUpdateFrame",
    "LLMSetToolsFrame",
    "LLMSetToolChoiceFrame",
    "LLMEnablePromptCachingFrame",
    "LLMConfigureOutputFrame",
    "FunctionCallResultFrame",
    "FunctionCallInProgressFrame",
    "FunctionCallCancelFrame",
    "FunctionCallsStartedFrame",
    "ExternalFunctionCallsStartedFrame",
    "ExternalFunctionCallInProgressFrame",
    "ExternalFunctionCallResultFrame",
    "ExternalFunctionCallCancelFrame",
    "InputDTMFFrame",
    "OutputDTMFFrame",
    "OutputDTMFUrgentFrame",
    "STTMuteFrame",
    "ErrorFrame",
    "FatalErrorFrame",
    "UserTurnInferenceCompletedFrame",
    "VADUserStartedSpeakingFrame",
    "VADUserStoppedSpeakingFrame",
    "SpeechControlParamsFrame",
    "ServiceMetadataFrame",
    "STTMetadataFrame",
    "LLMServiceMetadataFrame",
    "WorkerFrame",
    "WorkerSystemFrame",
    "EndWorkerFrame",
    "StopWorkerFrame",
    "CancelWorkerFrame",
    "InterruptionWorkerFrame",
    "TaskFrame",
    "TaskSystemFrame",
    "PipelineFlushFrame",
    "LLMAssistantPushAggregationFrame",
    "LLMSummarizeContextFrame",
    "LLMContextSummaryRequestFrame",
    "LLMContextSummaryResultFrame",
    "VisionFullResponseStartFrame",
    "VisionFullResponseEndFrame",
    "ServiceUpdateSettingsFrame",
    "LLMUpdateSettingsFrame",
    "TTSUpdateSettingsFrame",
    "STTUpdateSettingsFrame",
    "UserIdleTimeoutUpdateFrame",
    "VADParamsUpdateFrame",
    "FilterControlFrame",
    "FilterUpdateSettingsFrame",
    "FilterEnableFrame",
    "MixerControlFrame",
    "MixerUpdateSettingsFrame",
    "MixerEnableFrame",
    "VolumeFrame",
    "VolumeGainFrame",
    "ServiceSwitcherFrame",
];
pub(super) const VAD_PARAMS: &str = "pipecat.audio.vad.vad_analyzer.VADParams";
pub(super) const TURN_PARAMS: &str = "pipecat.audio.turn.base_turn_analyzer.BaseTurnParams";
pub(super) const ERROR_CATEGORY: &str = "pipecat.utils.errors.ErrorCategory";
pub(super) const SUMMARY_CONFIG: &str =
    "pipecat.utils.context.llm_context_summarization.LLMContextSummaryConfig";

pub(super) fn parse<T: DeserializeOwned>(
    f: &Map<String, Value>,
    defaults: Value,
) -> Result<T, CodecError> {
    let mut payload = defaults.as_object().cloned().unwrap_or_default();
    payload.extend(f.clone());
    serde_json::from_value(payload.into()).map_err(|_| invalid("frame payload"))
}
pub(super) fn plain<T: Serialize>(value: &T) -> Result<Value, CodecError> {
    let mut value = serde_json::to_value(value)?;
    if let Some(fields) = value.as_object_mut() {
        fields.retain(|_, value| !value.is_null());
    }
    Ok(value)
}
pub(super) fn no_local(f: &Map<String, Value>, fields: &[&str]) -> Result<(), CodecError> {
    for key in fields {
        if f.get(*key).is_some_and(|value| !value.is_null()) {
            return Err(invalid(&format!("{key} is local-only")));
        }
    }
    Ok(())
}
pub(super) fn tagged<'a>(
    value: &'a Value,
    expected: &str,
) -> Result<&'a Map<String, Value>, CodecError> {
    let (name, fields) = object_tag(value)?;
    if name != expected {
        return Err(CodecError::UnsupportedType(name.into()));
    }
    Ok(fields)
}
pub(super) fn field<'a>(f: &'a Map<String, Value>, key: &str) -> Result<&'a Value, CodecError> {
    f.get(key).ok_or_else(|| invalid(key))
}
fn payload_tag<T: Serialize>(name: &str, value: &T) -> Result<Value, CodecError> {
    Ok(tag(name, plain(value)?))
}
fn text(f: &Map<String, Value>) -> Result<TextFrame, CodecError> {
    parse(
        f,
        json!({"includes_inter_frame_spaces":false,"append_to_context":true}),
    )
}
fn transcription(f: &Map<String, Value>) -> Result<DetailedTranscriptionFrame, CodecError> {
    Ok(DetailedTranscriptionFrame {
        text: text(f)?,
        user_id: required(f, "user_id")?,
        timestamp: required(f, "timestamp")?,
        language: optional(f, "language")?,
        result: optional(f, "result")?,
        finalized: defaulted(f, "finalized", false)?,
    })
}
fn encode_transcription(value: &DetailedTranscriptionFrame) -> Result<Value, CodecError> {
    let mut f = plain(value)?.as_object().unwrap().clone();
    f.extend(plain(&value.text)?.as_object().unwrap().clone());
    Ok(f.into())
}
fn audio(f: &Map<String, Value>) -> Result<AudioRawFrame, CodecError> {
    let mut value = AudioRawFrame::new(
        decode_bytes(field(f, "audio")?)?,
        required(f, "sample_rate")?,
        required(f, "num_channels")?,
    )
    .map_err(|_| invalid("audio format"))?;
    value.context_id = optional(f, "context_id")?;
    if optional::<usize>(f, "num_frames")?.is_some_and(|n| n != value.num_frames()) {
        return Err(invalid("num_frames"));
    }
    Ok(value)
}
fn encode_audio(value: &AudioRawFrame) -> Result<Value, CodecError> {
    value.validate().map_err(|_| invalid("audio format"))?;
    Ok(
        json!({"audio":tag("bytes",STANDARD.encode(&value.audio).into()),"sample_rate":value.sample_rate,"num_channels":value.num_channels,"num_frames":value.num_frames(),"context_id":value.context_id}),
    )
}
fn image(f: &Map<String, Value>) -> Result<ImageRawFrame, CodecError> {
    Ok(ImageRawFrame {
        image: decode_bytes(field(f, "image")?)?,
        size: required(f, "size")?,
        format: optional(f, "format")?,
    })
}
fn output_image(f: &Map<String, Value>) -> Result<OutputImageRawFrame, CodecError> {
    Ok(OutputImageRawFrame {
        image: image(f)?,
        sync_with_audio: defaulted(f, "sync_with_audio", false)?,
    })
}
fn encode_image(value: &ImageRawFrame) -> Value {
    json!({"image":tag("bytes",STANDARD.encode(&value.image).into()),"size":value.size,"format":value.format})
}
fn encode_output_image(value: &OutputImageRawFrame) -> Value {
    let mut f = encode_image(&value.image);
    f["sync_with_audio"] = value.sync_with_audio.into();
    f
}
fn file(f: &Map<String, Value>) -> Result<FileRawFrame, CodecError> {
    Ok(FileRawFrame {
        file: required(f, "file")?,
        source_type: required(f, "type")?,
        filename: optional(f, "filename")?,
        format: required(f, "format")?,
    })
}
fn encode_file(value: &FileRawFrame) -> Value {
    json!({"file":value.file,"type":value.source_type,"filename":value.filename,"format":value.format})
}
fn user_image_request(f: &Map<String, Value>) -> Result<UserImageRequestFrame, CodecError> {
    no_local(f, &["result_callback"])?;
    parse(f, json!({}))
}
fn dtmf(f: &Map<String, Value>) -> Result<DtmfSequence, CodecError> {
    DtmfSequence::new(optional(f, "button")?, optional(f, "buttons")?)
        .map_err(|_| invalid("DTMF sequence"))
}
fn category(value: &Value) -> Result<ErrorCategory, CodecError> {
    if value.get("__type__").and_then(Value::as_str) != Some(ERROR_CATEGORY) {
        return Err(invalid("error category"));
    }
    let category = value["__data__"]
        .as_str()
        .ok_or_else(|| invalid("error category"))?;
    if category != category.to_ascii_uppercase() {
        return Err(invalid("error category"));
    }
    serde_json::from_value(category.to_ascii_lowercase().into())
        .map_err(|_| invalid("error category"))
}
pub(super) fn validate_aux(name: &str, data: &Value) -> Result<bool, CodecError> {
    if name == ERROR_CATEGORY {
        category(&tag(name, data.clone()))?;
        return Ok(true);
    }
    Ok(matches!(
        name,
        VAD_PARAMS
            | TURN_PARAMS
            | SUMMARY_CONFIG
            | "pipecat.frames.frames.FunctionCallResultProperties"
            | "pipecat.frames.frames.FunctionCallFromLLM"
            | "pipecat.frames.frames.ExternalFunctionCall"
    ) || super::context::supported_type(name)
        || super::settings::supported_type(name))
}
fn error(f: &Map<String, Value>) -> Result<DetailedErrorFrame, CodecError> {
    no_local(f, &["processor", "exception"])?;
    Ok(DetailedErrorFrame {
        error: required(f, "error")?,
        fatal: defaulted(f, "fatal", false)?,
        processor: None,
        exception: None,
        category: f
            .get("category")
            .filter(|v| !v.is_null())
            .map(category)
            .transpose()?,
    })
}
fn encode_error(value: &DetailedErrorFrame) -> Result<Value, CodecError> {
    if value.processor.is_some() || value.exception.is_some() {
        return Err(invalid("error processor/exception is local-only"));
    }
    let mut out = json!({"error":value.error,"fatal":value.fatal});
    if let Some(category) = value.category {
        out["category"] = tag(
            ERROR_CATEGORY,
            serde_json::to_value(category)?
                .as_str()
                .unwrap()
                .to_ascii_uppercase()
                .into(),
        );
    }
    Ok(out)
}
fn metadata(f: &Map<String, Value>) -> Result<ServiceMetadataFrame, CodecError> {
    no_local(f, &["user_turn_strategies"])?;
    parse(f, json!({}))
}
fn summary_config(value: &Value) -> Result<LlmContextSummaryConfig, CodecError> {
    let f = tagged(value, SUMMARY_CONFIG)?;
    no_local(f, &["llm"])?;
    let config: LlmContextSummaryConfig =
        parse(f, serde_json::to_value(LlmContextSummaryConfig::default())?)?;
    if config.target_context_tokens == 0 {
        return Err(invalid("target_context_tokens"));
    }
    Ok(config)
}
fn vad(value: &Value) -> Result<VadParams, CodecError> {
    parse(
        tagged(value, VAD_PARAMS)?,
        serde_json::to_value(VadParams::default())?,
    )
}
fn result_properties(value: &Value) -> Result<FunctionCallResultProperties, CodecError> {
    let f = tagged(value, "pipecat.frames.frames.FunctionCallResultProperties")?;
    no_local(f, &["on_context_updated"])?;
    parse(f, json!({"is_final":true}))
}
fn function_result(f: &Map<String, Value>) -> Result<DetailedFunctionCallResultFrame, CodecError> {
    Ok(DetailedFunctionCallResultFrame {
        function_name: required(f, "function_name")?,
        tool_call_id: required(f, "tool_call_id")?,
        arguments: f.get("arguments").cloned().unwrap_or(Value::Null),
        result: f.get("result").cloned().unwrap_or(Value::Null),
        run_llm: optional(f, "run_llm")?,
        properties: f
            .get("properties")
            .filter(|v| !v.is_null())
            .map(result_properties)
            .transpose()?,
        error: optional(f, "error")?,
    })
}
fn encode_function_result(value: &DetailedFunctionCallResultFrame) -> Result<Value, CodecError> {
    if value
        .properties
        .as_ref()
        .is_some_and(|p| p.on_context_updated.is_some())
    {
        return Err(invalid("on_context_updated is local-only"));
    }
    let mut f = plain(value)?;
    if let Some(properties) = &value.properties {
        f["properties"] = payload_tag(
            "pipecat.frames.frames.FunctionCallResultProperties",
            properties,
        )?;
    }
    Ok(f)
}
fn f32_field(f: &Map<String, Value>, key: &str) -> Result<f32, CodecError> {
    let value: f64 = required(f, key)?;
    let narrowed = value as f32;
    if f64::from(narrowed) != value {
        return Err(invalid("float exceeds native f32 precision"));
    }
    Ok(narrowed)
}

pub(super) fn decode(name: &str, f: &Map<String, Value>) -> Result<Option<FrameData>, CodecError> {
    let data = match name {
        "Frame" => FrameData::Base,
        "SystemFrame" => FrameData::System,
        "DataFrame" => FrameData::Data,
        "ControlFrame" => FrameData::Control,
        "SpeechOutputAudioRawFrame" => FrameData::SpeechOutputAudio(audio(f)?),
        "UserAudioRawFrame" => FrameData::UserAudio(UserAudioRawFrame {
            audio: audio(f)?,
            user_id: defaulted(f, "user_id", String::new())?,
        }),
        "InputImageRawFrame" => FrameData::InputImage(image(f)?),
        "OutputImageRawFrame" => FrameData::OutputImageDetailed(output_image(f)?),
        "URLImageRawFrame" => FrameData::UrlImage(UrlImageRawFrame {
            image: output_image(f)?,
            url: optional(f, "url")?,
        }),
        "AssistantImageRawFrame" => FrameData::AssistantImage(AssistantImageRawFrame {
            image: output_image(f)?,
            original_data: f
                .get("original_data")
                .filter(|v| !v.is_null())
                .map(decode_bytes)
                .transpose()?,
            original_mime_type: optional(f, "original_mime_type")?,
        }),
        "InputFileRawFrame" => FrameData::InputFile(file(f)?),
        "UserFileRawFrame" => FrameData::UserFileDetailed(DetailedUserFileRawFrame {
            file: file(f)?,
            user_id: defaulted(f, "user_id", String::new())?,
            text: defaulted(f, "text", String::new())?,
            append_to_context: optional(f, "append_to_context")?,
            custom_options: optional(f, "custom_options")?,
            run_llm: optional(f, "run_llm")?,
        }),
        "UserImageRequestFrame" => FrameData::UserImageRequest(user_image_request(f)?),
        "UserImageRawFrame" => {
            if f.get("request").is_some_and(|v| !v.is_null()) {
                return Err(invalid(
                    "nested request frame identity cannot be represented",
                ));
            }
            FrameData::UserImage(UserImageRawFrame {
                image: image(f)?,
                user_id: defaulted(f, "user_id", String::new())?,
                text: optional(f, "text")?,
                append_to_context: optional(f, "append_to_context")?,
                request: None,
                run_llm: optional(f, "run_llm")?,
            })
        }
        "AggregatedTextFrame" | "TTSTextFrame" => {
            let value = AggregatedTextFrame {
                text: text(f)?,
                aggregated_by: required(f, "aggregated_by")?,
                context_id: optional(f, "context_id")?,
                raw_text: optional(f, "raw_text")?,
                will_be_spoken: defaulted(f, "will_be_spoken", false)?,
            };
            if name == "TTSTextFrame" {
                FrameData::TtsText(value)
            } else {
                FrameData::AggregatedText(value)
            }
        }
        "AggregatedTextProgressFrame" => FrameData::AggregatedTextProgress(parse(f, json!({}))?),
        "TranscriptionFrame" => FrameData::TranscriptionDetailed(transcription(f)?),
        "InterimTranscriptionFrame" => FrameData::InterimTranscriptionDetailed(transcription(f)?),
        "EagerTranscriptionFrame" => FrameData::EagerTranscriptionDetailed(transcription(f)?),
        "TranslationFrame" => FrameData::TranslationDetailed(transcription(f)?),
        "LLMMarkerFrame" => {
            FrameData::LlmMarker(parse(f, json!({"append_to_context_immediately":true}))?)
        }
        "LLMMarkerResponseFrame" => FrameData::LlmMarkerResponse(parse(f, json!({"markers":[]}))?),
        "LLMContextAssistantTimestampFrame" => FrameData::LlmContextAssistantTimestamp {
            timestamp: required(f, "timestamp")?,
        },
        "LLMContextAssistantTurnFrame" => FrameData::LlmContextAssistantTurn(parse(f, json!({}))?),
        "LLMMessagesAppendFrame" | "LLMMessagesUpdateFrame" => {
            let value = DetailedMessagesFrame {
                messages: super::context::decode_messages(field(f, "messages")?)?,
                run_llm: optional(f, "run_llm")?,
            };
            if name == "LLMMessagesAppendFrame" {
                FrameData::LlmMessagesAppendDetailed(value)
            } else {
                FrameData::LlmMessagesUpdateDetailed(value)
            }
        }
        "LLMSetToolsFrame" => FrameData::LlmSetTools(match f.get("tools") {
            None | Some(Value::Null) => ToolSet::NotGiven,
            Some(value) => super::context::decode_tools(value)?,
        }),
        "LLMSetToolChoiceFrame" => {
            FrameData::LlmSetToolChoice(super::context::decode_choice(field(f, "tool_choice")?)?)
        }
        "LLMEnablePromptCachingFrame" => FrameData::LlmEnablePromptCaching {
            enable: required(f, "enable")?,
        },
        "LLMConfigureOutputFrame" => FrameData::LlmConfigureOutput {
            skip_tts: required(f, "skip_tts")?,
        },
        "FunctionCallResultFrame" => FrameData::FunctionCallResultDetailed(function_result(f)?),
        "FunctionCallInProgressFrame" => FrameData::FunctionCallInProgress(parse(
            f,
            json!({"arguments":null,"cancel_on_interruption":false}),
        )?),
        "FunctionCallCancelFrame" => {
            FrameData::FunctionCallCancel(parse(f, json!({"run_llm":false}))?)
        }
        "FunctionCallsStartedFrame" => {
            let calls = field(f, "function_calls")?
                .as_array()
                .ok_or_else(|| invalid("function_calls"))?;
            FrameData::FunctionCallsStartedDetailed(
                calls
                    .iter()
                    .map(|call| {
                        let f = tagged(call, "pipecat.frames.frames.FunctionCallFromLLM")?;
                        Ok(FunctionCallFromLlm {
                            function_name: required(f, "function_name")?,
                            tool_call_id: required(f, "tool_call_id")?,
                            arguments: required(f, "arguments")?,
                            context: f
                                .get("context")
                                .filter(|v| !v.is_null())
                                .map(super::context::decode)
                                .transpose()?,
                        })
                    })
                    .collect::<Result<_, CodecError>>()?,
            )
        }
        "ExternalFunctionCallsStartedFrame" => {
            let calls = field(f, "function_calls")?
                .as_array()
                .ok_or_else(|| invalid("function_calls"))?;
            FrameData::ExternalFunctionCallsStarted(
                calls
                    .iter()
                    .map(|c| {
                        parse(
                            tagged(c, "pipecat.frames.frames.ExternalFunctionCall")?,
                            json!({}),
                        )
                    })
                    .collect::<Result<_, CodecError>>()?,
            )
        }
        "ExternalFunctionCallInProgressFrame" => {
            FrameData::ExternalFunctionCallInProgress(parse(f, json!({"arguments":null}))?)
        }
        "ExternalFunctionCallResultFrame" => FrameData::ExternalFunctionCallResult(parse(
            f,
            json!({"arguments":null,"result":null,"is_final":true}),
        )?),
        "ExternalFunctionCallCancelFrame" => {
            FrameData::ExternalFunctionCallCancel(parse(f, json!({}))?)
        }
        "InputDTMFFrame" => {
            let key = required(f, "button")?;
            DtmfSequence::new(Some(key), None).map_err(|_| invalid("button"))?;
            FrameData::InputDtmf(key)
        }
        "OutputDTMFFrame" => FrameData::OutputDtmfSequence(dtmf(f)?),
        "OutputDTMFUrgentFrame" => FrameData::OutputDtmfUrgentSequence(dtmf(f)?),
        "STTMuteFrame" => FrameData::SttMute(required(f, "mute")?),
        "ErrorFrame" => FrameData::ErrorDetailed(error(f)?),
        "FatalErrorFrame" => {
            let e = error(f)?;
            if !defaulted(f, "fatal", true)? {
                return Err(invalid("FatalErrorFrame.fatal"));
            }
            FrameData::FatalError(ErrorFrame {
                error: e.error,
                fatal: true,
                processor: None,
                category: e.category.unwrap_or_default(),
            })
        }
        "UserTurnInferenceCompletedFrame" => FrameData::UserTurnInferenceCompleted,
        "VADUserStartedSpeakingFrame" => FrameData::VadUserStartedSpeakingDetailed(parse(
            f,
            serde_json::to_value(VadUserStartedSpeakingFrame::default())?,
        )?),
        "VADUserStoppedSpeakingFrame" => FrameData::VadUserStoppedSpeakingDetailed(parse(
            f,
            serde_json::to_value(VadUserStoppedSpeakingFrame::default())?,
        )?),
        "SpeechControlParamsFrame" => FrameData::SpeechControlParams(SpeechControlParamsFrame {
            vad_params: f
                .get("vad_params")
                .filter(|v| !v.is_null())
                .map(vad)
                .transpose()?,
            turn_params: f
                .get("turn_params")
                .filter(|v| !v.is_null())
                .map(|v| parse(tagged(v, TURN_PARAMS)?, json!({})))
                .transpose()?,
        }),
        "ServiceMetadataFrame" => FrameData::ServiceMetadata(metadata(f)?),
        "STTMetadataFrame" => FrameData::SttMetadata(SttMetadataFrame {
            base: metadata(f)?,
            ttfs_p99_latency: required(f, "ttfs_p99_latency")?,
        }),
        "LLMServiceMetadataFrame" => FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
            base: metadata(f)?,
            is_realtime_service: defaulted(f, "is_realtime_service", false)?,
        }),
        "WorkerFrame" => FrameData::Worker,
        "WorkerSystemFrame" => FrameData::WorkerSystem,
        "TaskFrame" => FrameData::Task,
        "TaskSystemFrame" => FrameData::TaskSystem,
        "EndWorkerFrame" => FrameData::EndWorker(parse(f, json!({}))?),
        "CancelWorkerFrame" => FrameData::CancelWorker(parse(f, json!({}))?),
        "StopWorkerFrame" => FrameData::StopWorker,
        "InterruptionWorkerFrame" => FrameData::InterruptionWorker,
        "PipelineFlushFrame" => {
            no_local(f, &["event"])?;
            FrameData::FlushDetailed(parse(f, json!({"returning":false}))?)
        }
        "LLMAssistantPushAggregationFrame" => FrameData::LlmAssistantPushAggregation,
        "LLMSummarizeContextFrame" => FrameData::LlmSummarizeContext(
            f.get("config")
                .filter(|v| !v.is_null())
                .map(summary_config)
                .transpose()?,
        ),
        "LLMContextSummaryRequestFrame" => {
            FrameData::LlmContextSummaryRequest(LlmContextSummaryRequestFrame {
                request_id: required(f, "request_id")?,
                context: super::context::decode(field(f, "context")?)?,
                min_messages_to_keep: required(f, "min_messages_to_keep")?,
                target_context_tokens: required(f, "target_context_tokens")?,
                summarization_prompt: required(f, "summarization_prompt")?,
                summarization_timeout: optional(f, "summarization_timeout")?,
            })
        }
        "LLMContextSummaryResultFrame" => FrameData::LlmContextSummaryResult(parse(f, json!({}))?),
        "VisionFullResponseStartFrame" => FrameData::VisionFullResponseStart(parse(f, json!({}))?),
        "VisionFullResponseEndFrame" => FrameData::VisionFullResponseEnd(parse(f, json!({}))?),
        "ServiceUpdateSettingsFrame"
        | "LLMUpdateSettingsFrame"
        | "TTSUpdateSettingsFrame"
        | "STTUpdateSettingsFrame" => super::settings::decode(name, f)?,
        "UserIdleTimeoutUpdateFrame" => FrameData::UserIdleTimeoutUpdate {
            timeout: required(f, "timeout")?,
        },
        "VADParamsUpdateFrame" => FrameData::VadParamsUpdate(vad(field(f, "params")?)?),
        "FilterControlFrame" => FrameData::FilterControl,
        "FilterUpdateSettingsFrame" => FrameData::FilterUpdateSettings(required(f, "settings")?),
        "FilterEnableFrame" => FrameData::FilterEnable(required(f, "enable")?),
        "MixerControlFrame" => FrameData::MixerControl,
        "MixerUpdateSettingsFrame" => FrameData::MixerUpdateSettings(required(f, "settings")?),
        "MixerEnableFrame" => FrameData::MixerEnable(required(f, "enable")?),
        "VolumeFrame" => FrameData::Volume(f32_field(f, "volume")?),
        "VolumeGainFrame" => FrameData::VolumeGain(f32_field(f, "gain")?),
        "ServiceSwitcherFrame" => FrameData::ServiceSwitcher,
        _ => return Ok(None),
    };
    Ok(Some(data))
}
fn finite(values: &[f64]) -> Result<(), CodecError> {
    if values.iter().all(|v| v.is_finite()) {
        Ok(())
    } else {
        Err(invalid("nonfinite native float"))
    }
}
fn finite_vad(value: &VadParams) -> Result<(), CodecError> {
    finite(&[
        value.confidence,
        value.start_secs,
        value.stop_secs,
        value.min_volume,
    ])
}
fn finite_frame(data: &FrameData) -> Result<(), CodecError> {
    match data {
        FrameData::VadUserStartedSpeaking(v) | FrameData::VadUserStoppedSpeaking(v) => {
            finite(&[v.duration_secs, v.timestamp])
        }
        FrameData::VadUserStartedSpeakingDetailed(v) => finite(&[v.start_secs, v.timestamp]),
        FrameData::VadUserStoppedSpeakingDetailed(v) => finite(&[v.stop_secs, v.timestamp]),
        FrameData::SpeechControlParams(v) => {
            v.vad_params.as_ref().map(finite_vad).unwrap_or(Ok(()))
        }
        FrameData::VadParamsUpdate(v) => finite_vad(v),
        FrameData::SttMetadata(v) => finite(&[v.ttfs_p99_latency]),
        FrameData::LlmSummarizeContext(Some(v)) => finite(&[v.summarization_timeout]),
        FrameData::LlmContextSummaryRequest(v) => finite(&[v.summarization_timeout.unwrap_or(0.0)]),
        FrameData::UserIdleTimeoutUpdate { timeout } => finite(&[*timeout]),
        FrameData::Volume(v) | FrameData::VolumeGain(v) => finite(&[f64::from(*v)]),
        _ => Ok(()),
    }
}
pub(super) fn encode(data: &FrameData) -> Result<Option<(&'static str, Value)>, CodecError> {
    finite_frame(data)?;
    let (name, payload) = match data {
        FrameData::Base => ("Frame", json!({})),
        FrameData::System => ("SystemFrame", json!({})),
        FrameData::Data => ("DataFrame", json!({})),
        FrameData::Control => ("ControlFrame", json!({})),
        FrameData::SpeechOutputAudio(v) => ("SpeechOutputAudioRawFrame", encode_audio(v)?),
        FrameData::UserAudio(v) => {
            let mut p = encode_audio(&v.audio)?;
            p["user_id"] = v.user_id.clone().into();
            ("UserAudioRawFrame", p)
        }
        FrameData::InputImage(v) => ("InputImageRawFrame", encode_image(v)),
        FrameData::OutputImage(v) => ("OutputImageRawFrame", encode_image(v)),
        FrameData::OutputImageDetailed(v) => ("OutputImageRawFrame", encode_output_image(v)),
        FrameData::UrlImage(v) => {
            let mut p = encode_output_image(&v.image);
            p["url"] = json!(v.url);
            ("URLImageRawFrame", p)
        }
        FrameData::AssistantImage(v) => {
            let mut p = encode_output_image(&v.image);
            p["original_mime_type"] = json!(v.original_mime_type);
            if let Some(bytes) = &v.original_data {
                p["original_data"] = tag("bytes", STANDARD.encode(bytes).into());
            }
            ("AssistantImageRawFrame", p)
        }
        FrameData::InputFile(v) => ("InputFileRawFrame", encode_file(v)),
        FrameData::UserFile(v) => {
            let mut p = encode_file(&v.file);
            p["user_id"] = v.user_id.clone().into();
            ("UserFileRawFrame", p)
        }
        FrameData::UserFileDetailed(v) => {
            let mut p = encode_file(&v.file);
            p.as_object_mut().unwrap().extend(
                plain(v)?
                    .as_object()
                    .unwrap()
                    .iter()
                    .filter(|(key, _)| key.as_str() != "file")
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
            ("UserFileRawFrame", p)
        }
        FrameData::UserImageRequest(v) => {
            if v.result_callback.is_some() {
                return Err(invalid("result_callback is local-only"));
            }
            ("UserImageRequestFrame", plain(v)?)
        }
        FrameData::UserImage(v) => {
            if v.request.is_some() {
                return Err(invalid(
                    "nested request frame identity cannot be represented",
                ));
            }
            let mut p = encode_image(&v.image);
            p.as_object_mut().unwrap().extend(
                plain(v)?
                    .as_object()
                    .unwrap()
                    .iter()
                    .filter(|(key, _)| key.as_str() != "image")
                    .map(|(k, v)| (k.clone(), v.clone())),
            );
            ("UserImageRawFrame", p)
        }
        FrameData::AggregatedText(v) | FrameData::TtsText(v) => {
            let mut p = plain(v)?;
            p.as_object_mut()
                .unwrap()
                .extend(plain(&v.text)?.as_object().unwrap().clone());
            (
                if matches!(data, FrameData::TtsText(_)) {
                    "TTSTextFrame"
                } else {
                    "AggregatedTextFrame"
                },
                p,
            )
        }
        FrameData::AggregatedTextProgress(v) => ("AggregatedTextProgressFrame", plain(v)?),
        FrameData::TranscriptionDetailed(v)
        | FrameData::InterimTranscriptionDetailed(v)
        | FrameData::EagerTranscriptionDetailed(v)
        | FrameData::TranslationDetailed(v) => (data.source_class_name(), encode_transcription(v)?),
        FrameData::Transcription(v)
        | FrameData::InterimTranscription(v)
        | FrameData::EagerTranscription(v)
        | FrameData::Translation(v) => (
            data.source_class_name(),
            encode_transcription(&DetailedTranscriptionFrame {
                text: TextFrame::new(&v.text),
                user_id: v.user_id.clone(),
                timestamp: v.timestamp.clone(),
                language: v.language.clone(),
                result: v.result.clone(),
                finalized: v.finalized,
            })?,
        ),
        FrameData::LlmMarker(v) => ("LLMMarkerFrame", plain(v)?),
        FrameData::LlmMarkerResponse(v) => ("LLMMarkerResponseFrame", plain(v)?),
        FrameData::LlmContextAssistantTimestamp { timestamp } => (
            "LLMContextAssistantTimestampFrame",
            json!({"timestamp":timestamp}),
        ),
        FrameData::LlmContextAssistantTurn(v) => ("LLMContextAssistantTurnFrame", plain(v)?),
        FrameData::LlmMessagesAppendDetailed(v) | FrameData::LlmMessagesUpdateDetailed(v) => (
            data.source_class_name(),
            json!({"messages":super::context::encode_messages(&v.messages)?,"run_llm":v.run_llm}),
        ),
        FrameData::LlmMessagesAppend(v) | FrameData::LlmMessagesUpdate(v) => (
            data.source_class_name(),
            json!({"messages":super::context::encode_messages(&v.messages.iter().cloned().map(Into::into).collect::<Vec<_>>())?,"run_llm":v.run_llm}),
        ),
        FrameData::LlmSetTools(v) => (
            "LLMSetToolsFrame",
            match super::context::encode_tools(v)? {
                Some(tools) => json!({"tools":tools}),
                None => json!({}),
            },
        ),
        FrameData::LlmSetToolChoice(v) => (
            "LLMSetToolChoiceFrame",
            json!({"tool_choice":super::context::encode_choice(v)}),
        ),
        FrameData::LlmEnablePromptCaching { enable } => {
            ("LLMEnablePromptCachingFrame", json!({"enable":enable}))
        }
        FrameData::LlmConfigureOutput { skip_tts } => {
            ("LLMConfigureOutputFrame", json!({"skip_tts":skip_tts}))
        }
        FrameData::FunctionCallResultDetailed(v) => {
            ("FunctionCallResultFrame", encode_function_result(v)?)
        }
        FrameData::FunctionCallResult(v) => (
            "FunctionCallResultFrame",
            encode_function_result(&DetailedFunctionCallResultFrame {
                function_name: v.function_name.clone(),
                tool_call_id: v.tool_call_id.clone(),
                arguments: v.arguments.clone(),
                result: v.result.clone(),
                run_llm: v.run_llm,
                properties: Some(FunctionCallResultProperties {
                    run_llm: None,
                    on_context_updated: None,
                    is_final: v.is_final,
                }),
                error: v.error.clone(),
            })?,
        ),
        FrameData::FunctionCallInProgress(v) => ("FunctionCallInProgressFrame", plain(v)?),
        FrameData::FunctionCallCancel(v) => ("FunctionCallCancelFrame", plain(v)?),
        FrameData::FunctionCallsStartedDetailed(v) => (
            "FunctionCallsStartedFrame",
            json!({"function_calls":v.iter().map(|call|{let mut p=plain(call)?;if let Some(context)=&call.context{p["context"]=super::context::encode(context)?;}Ok(tag("pipecat.frames.frames.FunctionCallFromLLM",p))}).collect::<Result<Vec<_>,CodecError>>()?}),
        ),
        FrameData::FunctionCallsStarted(v) => (
            "FunctionCallsStartedFrame",
            json!({"function_calls":v.iter().map(|call|{if !call.arguments.is_object(){return Err(invalid("function call arguments must be a mapping"));}payload_tag("pipecat.frames.frames.FunctionCallFromLLM",call)}).collect::<Result<Vec<_>,CodecError>>()?}),
        ),
        FrameData::ExternalFunctionCallsStarted(v) => (
            "ExternalFunctionCallsStartedFrame",
            json!({"function_calls":v.iter().map(|call|payload_tag("pipecat.frames.frames.ExternalFunctionCall",call)).collect::<Result<Vec<_>,CodecError>>()?}),
        ),
        FrameData::ExternalFunctionCallInProgress(v) => {
            ("ExternalFunctionCallInProgressFrame", plain(v)?)
        }
        FrameData::ExternalFunctionCallResult(v) => ("ExternalFunctionCallResultFrame", plain(v)?),
        FrameData::ExternalFunctionCallCancel(v) => ("ExternalFunctionCallCancelFrame", plain(v)?),
        FrameData::InputDtmf(v) => {
            DtmfSequence::new(Some(*v), None).map_err(|_| invalid("button"))?;
            ("InputDTMFFrame", json!({"button":v}))
        }
        FrameData::OutputDtmf(v) | FrameData::OutputDtmfUrgent(v) => (
            data.source_class_name(),
            plain(&DtmfSequence::new(Some(*v), None).map_err(|_| invalid("button"))?)?,
        ),
        FrameData::OutputDtmfSequence(v) | FrameData::OutputDtmfUrgentSequence(v) => {
            DtmfSequence::new(v.button, Some(v.buttons.clone())).map_err(|_| invalid("buttons"))?;
            (data.source_class_name(), plain(v)?)
        }
        FrameData::SttMute(v) => ("STTMuteFrame", json!({"mute":v})),
        FrameData::ErrorDetailed(v) => ("ErrorFrame", encode_error(v)?),
        FrameData::Error(v) | FrameData::FatalError(v) => {
            if v.processor.is_some() {
                return Err(invalid("error processor is local-only"));
            }
            (
                data.source_class_name(),
                encode_error(&DetailedErrorFrame {
                    error: v.error.clone(),
                    fatal: v.fatal,
                    processor: None,
                    exception: None,
                    category: Some(v.category),
                })?,
            )
        }
        FrameData::UserTurnInferenceCompleted => ("UserTurnInferenceCompletedFrame", json!({})),
        FrameData::VadUserStartedSpeakingDetailed(v) => ("VADUserStartedSpeakingFrame", plain(v)?),
        FrameData::VadUserStoppedSpeakingDetailed(v) => ("VADUserStoppedSpeakingFrame", plain(v)?),
        FrameData::VadUserStartedSpeaking(v) => (
            "VADUserStartedSpeakingFrame",
            json!({"start_secs":v.duration_secs,"timestamp":v.timestamp}),
        ),
        FrameData::VadUserStoppedSpeaking(v) => (
            "VADUserStoppedSpeakingFrame",
            json!({"stop_secs":v.duration_secs,"timestamp":v.timestamp}),
        ),
        FrameData::SpeechControlParams(v) => {
            let mut p = json!({});
            if let Some(v) = &v.vad_params {
                p["vad_params"] = payload_tag(VAD_PARAMS, v)?;
            }
            if let Some(v) = &v.turn_params {
                p["turn_params"] = payload_tag(TURN_PARAMS, v)?;
            }
            ("SpeechControlParamsFrame", p)
        }
        FrameData::ServiceMetadata(v) => {
            if v.user_turn_strategies.is_some() {
                return Err(invalid(
                    "user_turn_strategies contains local strategy objects",
                ));
            }
            ("ServiceMetadataFrame", plain(v)?)
        }
        FrameData::SttMetadata(v) => {
            if v.base.user_turn_strategies.is_some() {
                return Err(invalid(
                    "user_turn_strategies contains local strategy objects",
                ));
            }
            (
                "STTMetadataFrame",
                json!({"service_name":v.base.service_name,"ttfs_p99_latency":v.ttfs_p99_latency}),
            )
        }
        FrameData::LlmServiceMetadata(v) => {
            if v.base.user_turn_strategies.is_some() {
                return Err(invalid(
                    "user_turn_strategies contains local strategy objects",
                ));
            }
            (
                "LLMServiceMetadataFrame",
                json!({"service_name":v.base.service_name,"is_realtime_service":v.is_realtime_service}),
            )
        }
        FrameData::Worker
        | FrameData::WorkerSystem
        | FrameData::Task
        | FrameData::TaskSystem
        | FrameData::StopWorker
        | FrameData::InterruptionWorker
        | FrameData::LlmAssistantPushAggregation
        | FrameData::FilterControl
        | FrameData::MixerControl
        | FrameData::ServiceSwitcher => (data.source_class_name(), json!({})),
        FrameData::EndWorker(v) | FrameData::CancelWorker(v) => {
            (data.source_class_name(), plain(v)?)
        }
        FrameData::Flush(v) => ("PipelineFlushFrame", plain(v)?),
        FrameData::FlushDetailed(v) => {
            if v.event.is_some() {
                return Err(invalid("flush event is local-only"));
            }
            ("PipelineFlushFrame", plain(v)?)
        }
        FrameData::LlmSummarizeContext(v) => {
            let mut p = json!({});
            if let Some(v) = v {
                if v.llm.is_some() {
                    return Err(invalid("summary llm is local-only"));
                }
                if v.target_context_tokens == 0 {
                    return Err(invalid("target_context_tokens"));
                }
                p["config"] = payload_tag(SUMMARY_CONFIG, v)?;
            }
            ("LLMSummarizeContextFrame", p)
        }
        FrameData::LlmContextSummaryRequest(v) => {
            let mut p = plain(v)?;
            p["context"] = super::context::encode(&v.context)?;
            ("LLMContextSummaryRequestFrame", p)
        }
        FrameData::LlmContextSummaryResult(v) => ("LLMContextSummaryResultFrame", plain(v)?),
        FrameData::VisionFullResponseStart(v) | FrameData::VisionFullResponseEnd(v) => {
            (data.source_class_name(), plain(v)?)
        }
        FrameData::ServiceUpdateSettings(_)
        | FrameData::LlmUpdateSettings(_)
        | FrameData::TtsUpdateSettings(_)
        | FrameData::SttUpdateSettings(_) => {
            (data.source_class_name(), super::settings::encode(data)?)
        }
        FrameData::UserIdleTimeoutUpdate { timeout } => {
            ("UserIdleTimeoutUpdateFrame", json!({"timeout":timeout}))
        }
        FrameData::VadParamsUpdate(v) => (
            "VADParamsUpdateFrame",
            json!({"params":payload_tag(VAD_PARAMS,v)?}),
        ),
        FrameData::FilterUpdateSettings(v) | FrameData::MixerUpdateSettings(v) => {
            (data.source_class_name(), json!({"settings":v}))
        }
        FrameData::FilterEnable(v) | FrameData::MixerEnable(v) => {
            (data.source_class_name(), json!({"enable":v}))
        }
        FrameData::Volume(v) => ("VolumeFrame", json!({"volume":v})),
        FrameData::VolumeGain(v) => ("VolumeGainFrame", json!({"gain":v})),
        _ => return Ok(None),
    };
    Ok(Some((name, payload)))
}
