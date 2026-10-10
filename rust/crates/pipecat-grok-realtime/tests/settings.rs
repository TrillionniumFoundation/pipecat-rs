use pipecat_grok_realtime::*;
use serde_json::json;
use std::time::Duration;

#[test]
fn grok_root_settings_and_explicit_manual_null_are_distinct_from_omission() {
    let empty = serde_json::to_value(SessionProperties::new()).unwrap();
    assert_eq!(empty, json!({}));
    let manual: SessionProperties =
        serde_json::from_value(json!({"turn_detection":null,"voice":"Custom-Voice-ID"})).unwrap();
    assert_eq!(manual.turn_detection, Some(None));
    assert_eq!(
        serde_json::to_value(manual).unwrap()["turn_detection"],
        json!(null)
    );
    let defaults = serde_json::to_value(SessionProperties::pcm()).unwrap();
    assert_eq!(defaults["voice"], "eve");
    assert_eq!(defaults["turn_detection"]["type"], "server_vad");
    assert!(defaults.get("type").is_none());
    assert!(defaults["audio"]["input"].get("turn_detection").is_none());
    let response = serde_json::to_value(ResponseOptions::default()).unwrap();
    assert_eq!(response["modalities"], json!(["text", "audio"]));
    assert!(response.get("output_modalities").is_none());
}

#[test]
fn every_pinned_pcm_rate_is_valid_and_frame_processor_requires_pcm() {
    for rate in [8000, 11025, 16000, 22050, 24000, 32000, 44100, 48000] {
        let mut config = RealtimeConfig::new("synthetic-key");
        config
            .session
            .audio
            .as_mut()
            .unwrap()
            .input
            .as_mut()
            .unwrap()
            .format = AudioFormat::Pcm { rate };
        assert!(config.validate().is_ok());
        assert!(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).is_ok());
    }
    for format in [AudioFormat::Pcmu, AudioFormat::Pcma] {
        let mut config = RealtimeConfig::new("synthetic-key");
        config
            .session
            .audio
            .as_mut()
            .unwrap()
            .input
            .as_mut()
            .unwrap()
            .format = format;
        assert!(config.validate().is_ok());
        assert!(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).is_err());
    }
}

#[test]
fn unsupported_binary_and_invalid_settings_fail_before_connection() {
    for patch in [
        json!({"voice":""}),
        json!({"turn_detection":{"type":"semantic_vad"}}),
        json!({"turn_detection":{"type":"server_vad","threshold":1.0}}),
        json!({"audio":{"output":{"speed":0.6}}}),
        json!({"audio":{"input":{"transport":"binary"}}}),
        json!({"audio":{"input":{"transcription":{"keyterms":["x".repeat(51)]}}}}),
        json!({"tools":[{"type":"function","name":"f","description":"f","parameters":{ }},{"type":"function","name":"f","description":"f","parameters":{ }}]}),
    ] {
        let mut config = RealtimeConfig::new("synthetic-key");
        config.session = serde_json::from_value(patch).unwrap();
        assert!(config.validate().is_err());
    }
    let mut config = RealtimeConfig::new("synthetic-key");
    config.limits.operation_timeout = Duration::MAX;
    assert!(config.validate().is_err());
}

#[test]
fn hosted_tools_transcription_and_custom_voice_round_trip() {
    let input = json!({
        "voice":"Custom-Voice-ID",
        "turn_detection":{"type":"server_vad","idle_timeout_ms":1000},
        "audio":{"input":{"format":{"type":"audio/pcm","rate":16000},"transcription":{"model":"grok-transcribe","language_hint":"zh-CN","keyterms":["专业词"]},"transport":"json"},"output":{"format":{"type":"audio/pcm","rate":48000},"speed":1.2,"transport":"json"}},
        "tools":[{"type":"web_search"},{"type":"x_search","allowed_x_handles":["example"]},{"type":"file_search","vector_store_ids":["collection"],"max_num_results":3},{"type":"mcp","server_url":"https://example.invalid/mcp","server_label":"hosted"}],
        "reasoning":{"effort":"high"},"resumption":{"enabled":true},"replace":{"Pipecat":"pipe cat"}
    });
    let mut config = RealtimeConfig::new("synthetic-key");
    config.session = serde_json::from_value(input.clone()).unwrap();
    assert!(config.validate().is_ok());
    assert_eq!(serde_json::to_value(config.session).unwrap(), input);
}

#[test]
fn absent_usage_is_not_reported_as_zero_and_top_level_usage_wins() {
    fn event(value: serde_json::Value) -> RealtimeEvent {
        RealtimeEvent {
            event_id: "e".into(),
            event_type: "response.done".into(),
            session_id: None,
            conversation_id: None,
            context_id: None,
            generation: 0,
            event: serde_json::from_value(value).unwrap(),
        }
    }
    let mut mapper = RealtimeFrameMapper::default();
    let frames = mapper
        .frames(&event(
            json!({"type":"response.done","response":{"id":"r","status":"completed","usage":{}}}),
        ))
        .unwrap();
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.data, pipecat_core::FrameData::Metrics(_))));
    let frames = mapper.frames(&event(json!({"type":"response.done","response":{"id":"r","status":"completed","usage":{"total_tokens":99,"input_tokens":90,"output_tokens":9}},"usage":{"total_tokens":7,"input_tokens":3,"output_tokens":4}}))).unwrap();
    assert!(frames.iter().any(|frame| matches!(&frame.data, pipecat_core::FrameData::Metrics(metrics) if matches!(metrics[0],pipecat_core::Metric::LlmUsage { total_tokens:7, prompt_tokens:3, completion_tokens:4, .. }))));
}
