//! Actual public browser configuration entry, not browser permission execution.
use pipecat_browser_client::{Config, Transport};
use serde_json::json;

#[test]
fn legacy_voice_configuration_still_accepts_both_existing_transports() {
    let config: Config = serde_json::from_value(json!({
        "project_name": "Voice control",
        "start_endpoint": "/api/start",
        "transports": ["websocket", "smallwebrtc"]
    }))
    .unwrap();
    assert_eq!(
        config.validate("http://127.0.0.1:8080/").unwrap().as_str(),
        "http://127.0.0.1:8080/api/start"
    );
    assert_eq!(
        config.transports,
        vec![Transport::WebSocket, Transport::SmallWebRtc]
    );
    eprintln!("legacy voice Config accepted both transports; no browser or session opened");
}

#[test]
fn explicit_microphone_free_websocket_configuration_is_usable() {
    let parsed = serde_json::from_value::<Config>(json!({
        "project_name": "Text input",
        "start_endpoint": "/api/start",
        "transports": ["websocket"],
        "input_mode": "text"
    }));
    eprintln!("explicit text Config parse outcome: {parsed:?}");
    let config = parsed.expect("browser Config lacks the explicit microphone-free text entry");
    assert_eq!(
        config.validate("http://127.0.0.1:8080/").unwrap().as_str(),
        "http://127.0.0.1:8080/api/start"
    );
    assert_eq!(config.transports, vec![Transport::WebSocket]);
}

#[test]
fn explicit_text_config_supports_receive_only_rtc_and_mixed_selection() {
    use pipecat_browser_client::InputMode;
    for transports in [json!(["smallwebrtc"]), json!(["websocket", "smallwebrtc"])] {
        let config: Config = serde_json::from_value(json!({"project_name":"Receive only", "start_endpoint":"/api/start", "transports":transports, "input_mode":"text"})).unwrap();
        config.validate("http://127.0.0.1:8080/").unwrap();
        assert_eq!(config.input_mode, InputMode::Text);
        assert!(!config.input_mode.captures_microphone());
        assert!(config.transports.contains(&Transport::SmallWebRtc));
        for transport in config.transports {
            config.input_mode.validate_transport(transport).unwrap();
        }
    }
}
