//! Browser-independent configuration, RTVI projection, and Pipecat protobuf wire format.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
#[cfg(target_arch = "wasm32")]
mod browser;
pub mod lifecycle;
pub mod protocol;
pub mod text_entry;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use url::Url;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    SmallWebRtc,
    WebSocket,
    Daily,
    LiveKit,
}
impl Transport {
    pub fn key(self) -> &'static str {
        match self {
            Self::SmallWebRtc => "smallwebrtc",
            Self::WebSocket => "websocket",
            Self::Daily => "daily",
            Self::LiveKit => "livekit",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::SmallWebRtc => "SmallWebRTC",
            Self::WebSocket => "WebSocket",
            Self::Daily => "Daily",
            Self::LiveKit => "LiveKit",
        }
    }
    pub fn implemented(self) -> bool {
        matches!(self, Self::SmallWebRtc | Self::WebSocket)
    }
    pub fn start_body(self) -> Value {
        match self {
            Self::SmallWebRtc => {
                json!({"transport":"webrtc","createDailyRoom":false,"enableDefaultIceServers":true})
            }
            _ => json!({"transport":self.key()}),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub project_name: String,
    pub start_endpoint: String,
    pub transports: Vec<Transport>,
}
impl Config {
    pub fn validate(&self, page_url: &str) -> Result<Url, String> {
        if self.project_name.is_empty() || self.project_name.len() > 256 {
            return Err("project_name must contain 1–256 bytes".into());
        }
        if self.transports.is_empty()
            || self.transports.len() > 4
            || self.transports.iter().any(|t| !t.implemented())
        {
            return Err("only smallwebrtc and websocket adapters are implemented".into());
        }
        let url = Url::parse(page_url)
            .and_then(|p| p.join(&self.start_endpoint))
            .map_err(|_| "invalid start endpoint")?;
        validate_http(&url)?;
        if !url.path().ends_with("/start") || url.query().is_some() {
            return Err("start endpoint must end in /start without a query".into());
        }
        Ok(url)
    }
}
fn validate_http(url: &Url) -> Result<(), String> {
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err("endpoint must be an HTTP(S) URL without credentials or fragment".into());
    }
    Ok(())
}
/// Matches both direct `/start` and same-origin proxy `/api/start` routes.
pub fn offer_endpoint(start: &Url, session: &str) -> Result<Url, String> {
    if session.is_empty()
        || session.len() > 256
        || !session
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err("invalid sessionId".into());
    }
    let base = start
        .path()
        .strip_suffix("/start")
        .ok_or("invalid start endpoint")?;
    let mut result = start.clone();
    result.set_path(&format!("{base}/sessions/{session}/api/offer"));
    Ok(result)
}
/// Preserves existing query parameters and percent-encodes tokens exactly once.
pub fn websocket_endpoint(start: &Url, value: &Value) -> Result<Url, String> {
    let mut url = start
        .join(
            value["wsUrl"]
                .as_str()
                .ok_or("start response lacks wsUrl")?,
        )
        .map_err(|_| "invalid wsUrl")?;
    if !matches!(url.scheme(), "ws" | "wss")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err("wsUrl must be a WebSocket URL without credentials or fragment".into());
    }
    if start.scheme() == "https" && url.scheme() != "wss" {
        return Err("HTTPS pages require a secure WebSocket endpoint".into());
    }
    if let Some(token) = value["token"].as_str().filter(|s| !s.is_empty()) {
        if url.query_pairs().any(|(k, _)| k == "token") {
            return Err("wsUrl already contains a token".into());
        }
        url.query_pairs_mut().append_pair("token", token);
    }
    Ok(url)
}

pub fn client_ready(id: u64) -> Value {
    json!({"id":id.to_string(),"label":"rtvi-ai","type":"client-ready","data":{"version":"2.2.0","about":{"library":"pipecat-rust-browser","library_version":env!("CARGO_PKG_VERSION")}}})
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transcript {
    pub role: &'static str,
    pub text: String,
    pub segment_id: Option<String>,
}
/// Only final user transcripts and current v2 bot output enter the conversation.
/// Other RTVI messages remain available in the events panel.
pub fn transcript(message: &Value) -> Option<Transcript> {
    if message["label"] != "rtvi-ai" {
        return None;
    }
    let data = &message["data"];
    let role = match message["type"].as_str()? {
        "user-transcription" if data["final"] == true => "user",
        "bot-output" => "bot",
        _ => return None,
    };
    let text = data["text"].as_str()?.to_owned();
    if text.len() > 64 * 1024 {
        return None;
    }
    let segment_id = data
        .get("segment_id")
        .filter(|v| !v.is_null())
        .map(Value::to_string);
    Some(Transcript {
        role,
        text,
        segment_id,
    })
}

/// Hide connection credentials and SDP in the diagnostic panel without modifying transcripts.
pub fn diagnostic_data(value: &Value) -> Value {
    fn redact(value: &Value, depth: usize) -> Value {
        if depth > 16 {
            return Value::String("[depth limit]".into());
        }
        match value {
            Value::Object(fields) => Value::Object(
                fields
                    .iter()
                    .map(|(key, value)| {
                        let normalized = key.to_ascii_lowercase();
                        let private = normalized.contains("token")
                            || normalized.contains("secret")
                            || matches!(
                                normalized.as_str(),
                                "authorization" | "password" | "credential" | "sdp" | "wsurl"
                            );
                        (
                            key.clone(),
                            if private {
                                Value::String("[redacted]".into())
                            } else {
                                redact(value, depth + 1)
                            },
                        )
                    })
                    .collect(),
            ),
            Value::Array(items) => Value::Array(
                items
                    .iter()
                    .take(100)
                    .map(|value| redact(value, depth + 1))
                    .collect(),
            ),
            _ => value.clone(),
        }
    }
    redact(value, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostics_redact_connection_credentials() {
        let value = diagnostic_data(
            &json!({"dailyToken":"private", "peer":{"sdp":"private-sdp","credential":"turn-secret","text":"visible"}}),
        );
        assert_eq!(value["dailyToken"], "[redacted]");
        assert_eq!(value["peer"]["sdp"], "[redacted]");
        assert_eq!(value["peer"]["credential"], "[redacted]");
        assert_eq!(value["peer"]["text"], "visible");
    }

    #[test]
    fn config_and_routes() {
        let config: Config = serde_json::from_str(r#"{"project_name":"Bot","start_endpoint":"/api/start","transports":["smallwebrtc","websocket"]}"#).unwrap();
        let start = config.validate("https://example.test/").unwrap();
        assert_eq!(
            offer_endpoint(&start, "s-123").unwrap().as_str(),
            "https://example.test/api/sessions/s-123/api/offer"
        );
        assert!(offer_endpoint(&start, "../other").is_err());
        assert!(offer_endpoint(&start, "%2e").is_err());
        let mut unsupported = config.clone();
        unsupported.transports = vec![Transport::Daily];
        assert!(unsupported.validate("https://example.test").is_err());
    }
    #[test]
    fn tokens_keep_existing_query() {
        let url = websocket_endpoint(
            &Url::parse("https://bot.test/start").unwrap(),
            &json!({"wsUrl":"wss://bot.test/ws?session=abc","token":"a?b&c"}),
        )
        .unwrap();
        assert_eq!(
            url.query_pairs().collect::<Vec<_>>(),
            vec![
                ("session".into(), "abc".into()),
                ("token".into(), "a?b&c".into())
            ]
        );
        assert!(websocket_endpoint(
            &Url::parse("https://bot.test/start").unwrap(),
            &json!({"wsUrl":"ws://bot.test/ws"})
        )
        .is_err());
    }
    #[test]
    fn transcripts_are_final_and_segment_aware() {
        assert!(transcript(&json!({"label":"rtvi-ai","type":"user-transcription","data":{"text":"hi","final":false}})).is_none());
        assert_eq!(transcript(&json!({"label":"rtvi-ai","type":"bot-output","data":{"text":"hi","segment_id":1,"spoken_status":"completed"}})).unwrap(), Transcript { role:"bot",text:"hi".into(),segment_id:Some("1".into()) });
        assert!(transcript(&json!({"type":"bot-output","data":{"text":"hi"}})).is_none());
    }
}
