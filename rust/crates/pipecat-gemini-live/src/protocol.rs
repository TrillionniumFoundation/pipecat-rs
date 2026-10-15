use crate::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::Deserialize;
use serde_json::{json, Value};

pub(crate) fn setup(config: &GeminiConfig) -> Value {
    let s = &config.session;
    let mut generation = json!({"responseModalities": [s.modality]});
    for (key, value) in [
        ("temperature", s.temperature.map(|v| json!(v))),
        ("maxOutputTokens", s.max_output_tokens.map(|v| json!(v))),
        ("topP", s.top_p.map(|v| json!(v))),
        ("topK", s.top_k.map(|v| json!(v))),
    ] {
        if let Some(value) = value {
            generation[key] = value;
        }
    }
    if s.voice.is_some() || s.language_code.is_some() {
        let mut speech = json!({});
        if let Some(voice) = &s.voice {
            speech["voiceConfig"] = json!({"prebuiltVoiceConfig": {"voiceName": voice}});
        }
        if let Some(language) = &s.language_code {
            speech["languageCode"] = json!(language);
        }
        generation["speechConfig"] = speech;
    }
    let model = if s.model.starts_with("models/") {
        s.model.clone()
    } else {
        format!("models/{}", s.model)
    };
    let mut setup = json!({"model": model, "generationConfig": generation,
        "realtimeInputConfig": {"automaticActivityDetection": s.activity_detection}});
    if let Some(text) = &s.system_instruction {
        setup["systemInstruction"] = json!({"parts": [{"text": text}]});
    }
    if s.input_transcription {
        setup["inputAudioTranscription"] = json!({});
    }
    if s.output_transcription {
        setup["outputAudioTranscription"] = json!({});
    }
    if !s.tools.is_empty() {
        setup["tools"] = json!([{"functionDeclarations": s.tools}]);
    }
    json!({"setup": setup})
}

pub(crate) fn action_messages(
    action: &Action,
    config: &GeminiConfig,
) -> Result<Vec<Value>, GeminiError> {
    let max = config.limits.max_message_bytes;
    match action {
        Action::RealtimeText(text) if text.len() > max => return Err(GeminiError::TooLarge),
        Action::ClientContent { turns, .. }
            if turns
                .iter()
                .try_fold(0usize, |n, t| n.checked_add(t.text.len()))
                .is_none_or(|n| n > max) =>
        {
            return Err(GeminiError::TooLarge);
        }
        Action::ToolResponses(responses) => check_json_size(responses, max)?,
        _ => {}
    }
    let audio = |data: &[u8]| -> Result<Value, GeminiError> {
        if data.is_empty() || !data.len().is_multiple_of(2) {
            return Err(GeminiError::Invalid(
                "PCM16 must contain nonempty whole samples",
            ));
        }
        if data.len() > config.limits.max_audio_bytes {
            return Err(GeminiError::TooLarge);
        }
        Ok(
            json!({"realtimeInput":{"audio":{"mimeType":format!("audio/pcm;rate={}", config.input_sample_rate),"data": STANDARD.encode(data)}}}),
        )
    };
    let messages = match action {
        Action::Audio(bytes) => vec![audio(bytes)?],
        Action::VideoJpeg(bytes) => {
            let overhead = serde_json::to_vec(
                &json!({"realtimeInput":{"video":{"mimeType":"image/jpeg","data":""}}}),
            )
            .map_err(|_| GeminiError::Invalid("JSON"))?
            .len();
            let size = bytes
                .len()
                .checked_add(2)
                .and_then(|n| (n / 3).checked_mul(4))
                .and_then(|n| n.checked_add(overhead));
            if size.is_none_or(|n| n > max) {
                return Err(GeminiError::TooLarge);
            }
            if !bytes.starts_with(&[0xff, 0xd8]) || !bytes.ends_with(&[0xff, 0xd9]) {
                return Err(GeminiError::Invalid("video requires JPEG bytes"));
            }
            vec![
                json!({"realtimeInput":{"video":{"mimeType":"image/jpeg","data":STANDARD.encode(bytes)}}}),
            ]
        }
        Action::RealtimeText(text) => {
            if text.is_empty() {
                return Err(GeminiError::Invalid("empty text"));
            }
            vec![json!({"realtimeInput":{"text": text}})]
        }
        Action::ClientContent {
            turns,
            turn_complete,
        } => {
            if turns.len() > 1024 {
                return Err(GeminiError::TooLarge);
            }
            vec![
                json!({"clientContent":{"turns":turns.iter().map(|t| json!({"role":t.role,"parts":[{"text":t.text}]})).collect::<Vec<_>>(),"turnComplete":turn_complete}}),
            ]
        }
        Action::ActivityStart { preroll } => {
            if !config.session.activity_detection.disabled {
                return Err(GeminiError::Invalid("activityStart requires manual VAD"));
            }
            let mut messages = vec![json!({"realtimeInput":{"activityStart":{}}})];
            if !preroll.is_empty() {
                messages.push(audio(preroll)?);
            }
            messages
        }
        Action::ActivityEnd => {
            if !config.session.activity_detection.disabled {
                return Err(GeminiError::Invalid("activityEnd requires manual VAD"));
            }
            vec![json!({"realtimeInput":{"activityEnd":{}}})]
        }
        Action::AudioStreamEnd => {
            if config.session.activity_detection.disabled {
                return Err(GeminiError::Invalid("audioStreamEnd requires server VAD"));
            }
            vec![json!({"realtimeInput":{"audioStreamEnd":true}})]
        }
        Action::ToolResponses(responses) => {
            if responses.is_empty() || responses.len() > config.limits.max_pending_tools {
                return Err(GeminiError::Invalid("tool response count"));
            }
            let mut ids = std::collections::HashSet::new();
            for r in responses {
                if r.id.is_empty()
                    || r.name.is_empty()
                    || !r.response.is_object()
                    || !ids.insert(&r.id)
                {
                    return Err(GeminiError::Invalid(
                        "tool responses need unique IDs, names, and JSON objects",
                    ));
                }
            }
            vec![json!({"toolResponse":{"functionResponses":responses}})]
        }
    };
    if config.mode == SessionMode::Transcription
        && !matches!(action, Action::Audio(_) | Action::AudioStreamEnd)
    {
        return Err(GeminiError::Unsupported(
            "text, history, manual activity, or tools in transcription mode",
        ));
    }
    for message in &messages {
        if serde_json::to_vec(message)
            .map_err(|_| GeminiError::Invalid("JSON"))?
            .len()
            > config.limits.max_message_bytes
        {
            return Err(GeminiError::TooLarge);
        }
    }
    Ok(messages)
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Packet {
    setup_complete: Option<Value>,
    server_content: Option<Content>,
    tool_call: Option<ToolCalls>,
    tool_call_cancellation: Option<Cancellation>,
    usage_metadata: Option<Usage>,
    go_away: Option<GoAway>,
    session_resumption_update: Option<Value>,
    error: Option<ProviderError>,
}
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct Content {
    model_turn: Option<ModelTurn>,
    #[serde(default)]
    turn_complete: bool,
    #[serde(default)]
    generation_complete: bool,
    #[serde(default)]
    interrupted: bool,
    input_transcription: Option<Transcript>,
    interim_input_transcription: Option<Transcript>,
    output_transcription: Option<Transcript>,
    interaction_status: Option<String>,
    grounding_metadata: Option<Value>,
    url_context_metadata: Option<Value>,
}
#[derive(Deserialize)]
struct ModelTurn {
    parts: Vec<Part>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Part {
    text: Option<String>,
    #[serde(default)]
    thought: bool,
    inline_data: Option<Blob>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Blob {
    mime_type: String,
    data: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolCalls {
    function_calls: Vec<WireFunctionCall>,
}
#[derive(Deserialize)]
struct WireFunctionCall {
    id: Option<String>,
    name: String,
    args: Option<Value>,
}
#[derive(Deserialize)]
struct Cancellation {
    ids: Vec<String>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GoAway {
    time_left: String,
}
#[derive(Deserialize)]
struct ProviderError {
    #[serde(default)]
    code: i64,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResumptionUpdate {
    pub resumable: bool,
    pub new_handle: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InteractionStatus {
    InProgress,
    Idle,
}
pub(crate) struct Parsed {
    pub interaction_status: Option<InteractionStatus>,
    pub resumption: Option<ResumptionUpdate>,
    pub setup_complete: bool,
    pub events: Vec<Event>,
    pub cancellations: Vec<String>,
}
pub(crate) fn parse(
    bytes: &[u8],
    config: &GeminiConfig,
    policy: Option<&ResumptionPolicy>,
) -> Result<Parsed, GeminiError> {
    parse_with_thinking(bytes, config, policy, false)
}

pub(crate) fn parse_with_thinking(
    bytes: &[u8],
    config: &GeminiConfig,
    policy: Option<&ResumptionPolicy>,
    thinking: bool,
) -> Result<Parsed, GeminiError> {
    if bytes.len() > config.limits.max_message_bytes {
        return Err(GeminiError::TooLarge);
    }
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| GeminiError::Protocol("invalid JSON"))?;
    let object = value
        .as_object()
        .ok_or(GeminiError::Protocol("expected JSON object"))?;
    for field in object.keys() {
        if ![
            "setupComplete",
            "serverContent",
            "toolCall",
            "toolCallCancellation",
            "usageMetadata",
            "goAway",
            "sessionResumptionUpdate",
            "error",
        ]
        .contains(&field.as_str())
        {
            return Err(GeminiError::Unsupported("unknown server envelope"));
        }
    }
    if object.is_empty() {
        return Err(GeminiError::Protocol("empty server envelope"));
    }
    if object.get("setupComplete").is_some_and(|v| !v.is_object()) {
        return Err(GeminiError::Protocol("setupComplete must be an object"));
    }
    let packet: Packet = serde_json::from_value(value)
        .map_err(|_| GeminiError::Protocol("invalid or unsupported typed server message"))?;
    if let Some(error) = packet.error {
        return Err(GeminiError::Provider {
            code: error.code,
            message: redact_key(&error.message, &config.api_key),
        });
    }
    let resumption = match packet.session_resumption_update {
        Some(value) => {
            let policy = policy.ok_or(GeminiError::Unsupported("session resumption"))?;
            let update: ResumptionUpdate = serde_json::from_value(value)
                .map_err(|_| GeminiError::Protocol("invalid session resumption update"))?;
            if update
                .new_handle
                .as_ref()
                .is_some_and(|h| h.len() > policy.max_handle_bytes)
            {
                return Err(GeminiError::TooLarge);
            }
            if update.resumable && update.new_handle.as_ref().is_none_or(|h| h.is_empty()) {
                return Err(GeminiError::Protocol("resumable update requires a handle"));
            }
            Some(update)
        }
        None => None,
    };
    let mut events = Vec::new();
    let mut generation_complete = false;
    let mut turn_complete = false;
    let mut interaction_status = None;
    if let Some(content) = packet.server_content {
        interaction_status = match content.interaction_status.as_deref() {
            Some("IN_PROGRESS") if thinking => Some(InteractionStatus::InProgress),
            Some("REQUIRES_ACTION") if thinking => Some(InteractionStatus::Idle),
            Some("IDLE") => Some(InteractionStatus::Idle),
            None | Some("INTERACTION_STATUS_UNSPECIFIED") => None,
            Some(_) => {
                return Err(GeminiError::Unsupported(
                    "background reasoning interaction status",
                ))
            }
        };
        if content.grounding_metadata.is_some() || content.url_context_metadata.is_some() {
            return Err(GeminiError::Unsupported("search/URL grounding"));
        }
        if content.interrupted {
            events.push(Event::Interrupted);
        }
        if let Some(turn) = content.model_turn {
            if turn.parts.len() > 1024 {
                return Err(GeminiError::TooLarge);
            }
            for part in turn.parts {
                if let Some(text) = part.text.filter(|t| !t.is_empty()) {
                    events.push(Event::Text {
                        text,
                        thought: part.thought,
                    });
                }
                if let Some(blob) = part.inline_data {
                    if blob.mime_type != "audio/pcm;rate=24000" && blob.mime_type != "audio/pcm" {
                        return Err(GeminiError::Unsupported(
                            "output media must be mono PCM16 at 24000 Hz",
                        ));
                    }
                    let pcm = STANDARD
                        .decode(blob.data)
                        .map_err(|_| GeminiError::Protocol("invalid base64 audio"))?;
                    if pcm.len() > config.limits.max_audio_bytes {
                        return Err(GeminiError::TooLarge);
                    }
                    if !pcm.len().is_multiple_of(2) {
                        return Err(GeminiError::Protocol("partial PCM16 output sample"));
                    }
                    if !pcm.is_empty() {
                        events.push(Event::Audio {
                            pcm,
                            sample_rate: 24_000,
                        });
                    }
                }
            }
        }
        if let Some(transcript) = content
            .interim_input_transcription
            .filter(|t| !t.text.is_empty())
        {
            events.push(Event::InputTranscription {
                transcript,
                is_final: false,
            });
        }
        if let Some(transcript) = content.input_transcription.filter(|t| !t.text.is_empty()) {
            events.push(Event::InputTranscription {
                transcript,
                is_final: config.mode == SessionMode::Transcription,
            });
        }
        if let Some(transcript) = content.output_transcription.filter(|t| !t.text.is_empty()) {
            events.push(Event::OutputTranscription(transcript));
        }
        generation_complete = content.generation_complete;
        turn_complete = content.turn_complete;
    }
    if let Some(tool_call) = packet.tool_call {
        if tool_call.function_calls.is_empty()
            || tool_call.function_calls.len() > config.limits.max_pending_tools
        {
            return Err(GeminiError::TooLarge);
        }
        let mut calls = Vec::with_capacity(tool_call.function_calls.len());
        for call in tool_call.function_calls {
            let args = call.args.unwrap_or_else(|| json!({}));
            if call.name.is_empty() || !args.is_object() {
                return Err(GeminiError::Protocol("invalid tool call"));
            }
            let id = call.id.unwrap_or_default();
            let id_source = if id.is_empty() {
                ToolCallIdSource::LocalGenerated
            } else {
                ToolCallIdSource::Provider
            };
            calls.push(FunctionCall {
                id,
                id_source,
                name: call.name,
                args,
            });
        }
        events.push(Event::ToolCalls(calls));
    }
    if generation_complete {
        events.push(Event::GenerationComplete);
    }
    if turn_complete {
        events.push(Event::TurnComplete);
    }
    if let Some(usage) = packet.usage_metadata {
        events.push(Event::Usage(usage));
    }
    if let Some(go_away) = packet.go_away {
        events.push(Event::GoAway {
            time_left: go_away.time_left,
        });
    }
    let cancellations = packet
        .tool_call_cancellation
        .map(|c| c.ids)
        .unwrap_or_default();
    if cancellations.len() > config.limits.max_pending_tools {
        return Err(GeminiError::TooLarge);
    }
    Ok(Parsed {
        interaction_status,
        resumption,
        setup_complete: packet.setup_complete.is_some(),
        events,
        cancellations,
    })
}

fn redact_key(message: &str, key: &str) -> String {
    // Match query encoding used by Url::query_pairs_mut, including '+' spaces.
    // Sanitize the complete bounded message before any downstream truncation.
    let encoded: String = url::form_urlencoded::byte_serialize(key.as_bytes()).collect();
    let percent_spaces = encoded.replace('+', "%20");
    message
        .replace(&encoded, "[REDACTED]")
        .replace(&percent_spaces, "[REDACTED]")
        .replace(key, "[REDACTED]")
}

/// Bound serialized open tool/schema payloads without allocating a second copy.
pub(crate) fn check_json_size(
    value: &impl serde::Serialize,
    max: usize,
) -> Result<(), GeminiError> {
    struct Counter {
        remaining: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.remaining = self
                .remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("size limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter { remaining: max }, value).map_err(|_| GeminiError::TooLarge)
}

#[cfg(test)]
mod video_tests {
    use super::*;
    #[test]
    fn jpeg_action_checks_mode_signature_and_exact_full_json_budget() {
        let mut config = GeminiConfig::new("fixture", "model");
        let mut bytes = vec![0xff, 0xd8];
        bytes.extend(vec![1; 180]);
        bytes.extend([0xff, 0xd9]);
        let action = Action::VideoJpeg(bytes);
        let message = action_messages(&action, &config).unwrap().remove(0);
        let exact = serde_json::to_vec(&message).unwrap().len();
        config.limits.max_message_bytes = exact;
        assert_eq!(action_messages(&action, &config).unwrap(), vec![message]);
        config.limits.max_message_bytes = exact - 1;
        assert!(matches!(
            action_messages(&action, &config),
            Err(GeminiError::TooLarge)
        ));
        config.limits.max_message_bytes = 1024;
        assert!(matches!(
            action_messages(&Action::VideoJpeg(vec![1, 2, 3]), &config),
            Err(GeminiError::Invalid(_))
        ));
        config.mode = SessionMode::Transcription;
        assert!(matches!(
            action_messages(&action, &config),
            Err(GeminiError::Unsupported(_))
        ));
    }
}

#[cfg(test)]
mod thinking_tests {
    use super::*;

    #[test]
    fn thinking_status_is_explicit_and_default_stays_strict() {
        let config = GeminiConfig::new("fixture", "model");
        for (status, expected) in [
            ("IN_PROGRESS", Some(InteractionStatus::InProgress)),
            ("IDLE", Some(InteractionStatus::Idle)),
            ("REQUIRES_ACTION", Some(InteractionStatus::Idle)),
            ("INTERACTION_STATUS_UNSPECIFIED", None),
        ] {
            let bytes = serde_json::to_vec(&json!({"serverContent": {
                "interactionStatus": status, "turnComplete": true,
                "generationComplete": true
            }}))
            .unwrap();
            let parsed = parse_with_thinking(&bytes, &config, None, true).unwrap();
            assert_eq!(parsed.interaction_status, expected);
            assert_eq!(
                parsed.events,
                [Event::GenerationComplete, Event::TurnComplete]
            );
            assert_eq!(
                parse(&bytes, &config, None).is_err(),
                matches!(status, "IN_PROGRESS" | "REQUIRES_ACTION")
            );
        }
        let bytes = br#"{"serverContent":{"interactionStatus":"UNKNOWN","modelTurn":{"parts":[{"text":"not admitted"}]}}}"#;
        assert!(matches!(
            parse_with_thinking(bytes, &config, None, true),
            Err(GeminiError::Unsupported(_))
        ));
    }

    #[test]
    fn thinking_parser_preserves_packet_tool_order_before_completion() {
        let config = GeminiConfig::new("fixture", "model");
        let bytes = br#"{"serverContent":{"interactionStatus":"IDLE","turnComplete":true},"toolCall":{"functionCalls":[{"name":"unexpected","id":"call"}]}}"#;
        let parsed = parse_with_thinking(bytes, &config, None, true).unwrap();
        assert!(matches!(parsed.events[0], Event::ToolCalls(_)));
        assert_eq!(parsed.events[1], Event::TurnComplete);
        // The session rejects this undeclared tool before applying completion.
    }
}
