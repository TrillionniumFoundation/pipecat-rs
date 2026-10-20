use pipecat_aws_common::{auth, AwsCredentials};
use pipecat_core::{AudioRawFrame, ChatRole, LlmContextFrame, ProcessorError};
use serde_json::{json, Value};
use std::{
    net::{IpAddr, SocketAddr},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

fn error(message: &'static str) -> ProcessorError {
    ProcessorError::fatal(message)
}

/// Independent count and byte bounds apply at each retained protocol layer.
#[derive(Clone, Debug)]
pub struct NovaSonicLimits {
    pub session_timeout: Duration,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
    pub response_timeout: Duration,
    pub consumer_timeout: Duration,
    pub drain_timeout: Duration,
    pub close_timeout: Duration,
    pub max_event_bytes: usize,
    pub max_audio_bytes: usize,
    pub max_history_bytes: usize,
    pub max_history_messages: usize,
    pub max_held_events: usize,
    pub max_held_bytes: usize,
    pub queue_events: usize,
    pub queue_bytes: usize,
}
impl Default for NovaSonicLimits {
    fn default() -> Self {
        Self {
            session_timeout: Duration::from_secs(300),
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(30),
            consumer_timeout: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(10),
            max_event_bytes: 256 * 1024,
            max_audio_bytes: 32 * 1024,
            max_history_bytes: 128 * 1024,
            max_history_messages: 32,
            max_held_events: 128,
            max_held_bytes: 2 * 1024 * 1024,
            queue_events: 16,
            queue_bytes: 2 * 1024 * 1024,
        }
    }
}
/// Immutable-after-construction profile. Model availability is not an entitlement claim.
#[derive(Clone)]
pub struct NovaSonicConfig {
    pub region: String,
    pub model: String,
    pub voice: String,
    pub credentials: AwsCredentials,
    pub endpoint: Option<String>,
    /// One caller-resolved address for regional HTTPS. No DNS or address retry.
    /// Literal-loopback endpoints reject this override. TLS identity remains the URL host.
    pub connect_address: Option<SocketAddr>,
    pub system_instruction: Option<String>,
    pub temperature: f64,
    pub top_p: f64,
    pub max_tokens: u32,
    pub endpointing_sensitivity: Option<String>,
    pub limits: NovaSonicLimits,
}
impl std::fmt::Debug for NovaSonicConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NovaSonicConfig")
            .field("credentials", &"[redacted]")
            .field("session_timeout", &self.limits.session_timeout)
            .finish_non_exhaustive()
    }
}
impl NovaSonicConfig {
    pub fn new(
        region: impl Into<String>,
        model: impl Into<String>,
        voice: impl Into<String>,
        credentials: AwsCredentials,
    ) -> Result<Self, ProcessorError> {
        let value = Self {
            region: region.into(),
            model: model.into(),
            voice: voice.into(),
            credentials,
            endpoint: None,
            connect_address: None,
            system_instruction: None,
            temperature: 0.7,
            top_p: 0.9,
            max_tokens: 1024,
            endpointing_sensitivity: None,
            limits: Default::default(),
        };
        value.validate_fields()?;
        Ok(value)
    }
    /// Validate the complete finite connection plan before app construction or I/O.
    pub fn validate(&self) -> Result<(), ProcessorError> {
        self.validate_fields()?;
        let url = self.url()?;
        if url.port_or_known_default() == Some(0) {
            return Err(error("invalid Nova endpoint port"));
        }
        if url
            .host_str()
            .and_then(|h| h.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            .is_some()
        {
            if self.connect_address.is_some() {
                return Err(error(
                    "Nova literal-loopback endpoint rejects connect-address override",
                ));
            }
        } else {
            let address = self.connect_address.ok_or_else(|| {
                error("Nova regional HTTPS requires caller-resolved connect address")
            })?;
            if address.port() != 443
                || url.port_or_known_default() != Some(address.port())
                || address.ip().is_unspecified()
                || address.ip().is_multicast()
                || matches!(address.ip(),IpAddr::V4(ip) if ip.is_broadcast())
                || matches!(address,SocketAddr::V6(ip) if ip.scope_id()!=0 || ip.flowinfo()!=0)
            {
                return Err(error("invalid Nova caller-resolved connect address"));
            }
        }
        Ok(())
    }
    fn validate_fields(&self) -> Result<(), ProcessorError> {
        auth::region(&self.region).map_err(|_| error("invalid Nova region"))?;
        if !matches!(
            self.model.as_str(),
            "amazon.nova-2-sonic-v1:0" | "amazon.nova-2-5-sonic"
        ) {
            return Err(error(
                "unsupported Nova model; select the finite Nova 2 profile",
            ));
        }
        if self.voice.is_empty()
            || self.voice.len() > 64
            || !self
                .voice
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(error("invalid Nova voice identifier"));
        }
        if !self.temperature.is_finite()
            || !(0.0..=1.0).contains(&self.temperature)
            || !self.top_p.is_finite()
            || !(0.0..=1.0).contains(&self.top_p)
            || self.top_p == 0.0
            || self.max_tokens == 0
            || self.max_tokens > 4096
            || self
                .endpointing_sensitivity
                .as_ref()
                .is_some_and(|v| !matches!(v.as_str(), "LOW" | "MEDIUM" | "HIGH"))
        {
            return Err(error("unsupported Nova inference settings"));
        }
        let l = &self.limits;
        if l.session_timeout.is_zero()
            || l.session_timeout > Duration::from_secs(420)
            || l.drain_timeout.is_zero()
            || l.drain_timeout >= l.session_timeout
            || [
                l.connect_timeout,
                l.operation_timeout,
                l.response_timeout,
                l.consumer_timeout,
                l.close_timeout,
            ]
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(60) || *d > l.session_timeout)
            || l.drain_timeout > Duration::from_secs(120)
            || !(1024..=1024 * 1024).contains(&l.max_event_bytes)
            || l.max_audio_bytes == 0
            || l.max_audio_bytes > 64 * 1024
            || !l.max_audio_bytes.is_multiple_of(2)
            || l.max_history_bytes == 0
            || l.max_history_bytes > 256 * 1024
            || l.max_history_messages > 32
            || l.max_held_events == 0
            || l.max_held_events > 128
            || l.max_held_bytes == 0
            || l.max_held_bytes > 4 * 1024 * 1024
            || l.queue_events == 0
            || l.queue_events > 64
            || l.queue_bytes < l.max_event_bytes
            || l.queue_bytes > 4 * 1024 * 1024
            || self
                .system_instruction
                .as_ref()
                .is_some_and(|s| s.len() > l.max_history_bytes)
        {
            return Err(error("invalid finite Nova resource limits"));
        }
        self.protocol_limits()
            .validate()
            .map_err(|_| error("invalid Nova derived protocol limits"))?;
        if l.max_audio_bytes > (self.protocol_limits().max_event_bytes.saturating_sub(512) / 4) * 3
        {
            return Err(error("Nova PCM bound exceeds nested encoding capacity"));
        }
        self.url()?;
        Ok(())
    }
    pub(crate) fn url(&self) -> Result<url::Url, ProcessorError> {
        let encoded = self.model.replace(':', "%3A");
        let path = format!("/model/{encoded}/invoke-with-bidirectional-stream");
        let expected = format!(
            "https://bedrock-runtime.{}.{}{path}",
            self.region,
            auth::domain(&self.region)
        );
        auth::endpoint(
            self.endpoint.as_deref().unwrap_or(&expected),
            &expected,
            &path,
            false,
        )
        .map_err(|_| error("invalid Nova regional or literal-loopback endpoint"))
    }
    pub(crate) fn audio(&self, audio: &AudioRawFrame) -> Result<(), ProcessorError> {
        if audio.sample_rate != 16000
            || audio.num_channels != 1
            || !audio.audio.len().is_multiple_of(2)
            || audio.audio.is_empty()
            || audio.audio.len() > self.limits.max_audio_bytes
        {
            return Err(error("Nova input requires bounded PCM16LE mono 16000 Hz"));
        }
        Ok(())
    }
    pub(crate) fn protocol_limits(&self) -> crate::protocol::ProtocolLimits {
        let raw = (self.limits.max_event_bytes.saturating_sub(128) / 4) * 3 - 32;
        crate::protocol::ProtocolLimits {
            max_frame_bytes: self.limits.max_event_bytes,
            max_event_bytes: raw,
            max_string_bytes: raw,
            max_content_text_bytes: raw.min(64 * 1024),
            max_audio_bytes: self.limits.max_audio_bytes,
            ..Default::default()
        }
    }
}
static ID: AtomicU64 = AtomicU64::new(1);
pub(crate) struct InputPlan {
    pub prompt: String,
    pub audio: String,
    pub startup: Vec<Vec<u8>>,
    pub finish: Vec<Vec<u8>>,
}
pub(crate) fn encode(
    value: Value,
    limits: &crate::protocol::ProtocolLimits,
) -> Result<Vec<u8>, ProcessorError> {
    let bytes = serde_json::to_vec(&value).map_err(|_| error("Nova serialization failed"))?;
    crate::protocol::encode_chunk(&bytes, limits)
        .map_err(|_| error("Nova encoded input exceeds limits"))
}
impl InputPlan {
    pub(crate) fn new(
        config: &NovaSonicConfig,
        seed: &LlmContextFrame,
    ) -> Result<Self, ProcessorError> {
        config.validate()?;
        if !seed.tools.is_empty()
            || seed.tool_choice.is_some()
            || seed.messages.len() > config.limits.max_history_messages
        {
            return Err(error("Nova seed requires bounded tool-free text history"));
        }
        let mut system = config.system_instruction.clone();
        let mut bytes = system.as_ref().map_or(0, String::len);
        let mut messages = Vec::new();
        let mut systems = 0;
        for m in &seed.messages {
            bytes = bytes
                .checked_add(m.content.len())
                .ok_or_else(|| error("Nova seed byte overflow"))?;
            if bytes > config.limits.max_history_bytes
                || m.name.is_some()
                || m.tool_call_id.is_some()
                || !m.tool_calls.is_empty()
            {
                return Err(error(
                    "Nova seed contains unsupported metadata or exceeds bounds",
                ));
            }
            match m.role {
                ChatRole::System => {
                    systems += 1;
                    if systems > 1 || system.as_ref().is_some_and(|s| s != &m.content) {
                        return Err(error("Nova seed system instructions conflict"));
                    }
                    system = Some(m.content.clone());
                }
                ChatRole::User => messages.push(("USER", m.content.clone())),
                ChatRole::Assistant => messages.push(("ASSISTANT", m.content.clone())),
                _ => return Err(error("Nova seed supports only system/user/assistant text")),
            }
        }
        let id = ID
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_add(1))
            .map_err(|_| error("Nova identity exhausted"))?;
        let prompt = format!("nova-{id}");
        let audio = format!("audio-{id}");
        let l = config.protocol_limits();
        let mut session = json!({"inferenceConfiguration":{"maxTokens":config.max_tokens,"topP":config.top_p,"temperature":config.temperature}});
        if let Some(v) = &config.endpointing_sensitivity {
            session["turnDetectionConfiguration"] = json!({"endpointingSensitivity":v});
        }
        let mut startup = vec![
            encode(json!({"event":{"sessionStart":session}}), &l)?,
            encode(
                json!({"event":{"promptStart":{"promptName":prompt,"textOutputConfiguration":{"mediaType":"text/plain"},"audioOutputConfiguration":{"mediaType":"audio/lpcm","sampleRateHertz":24000,"sampleSizeBits":16,"channelCount":1,"voiceId":config.voice,"encoding":"base64","audioType":"SPEECH"}}}}),
                &l,
            )?,
        ];
        if let Some(s) = system {
            messages.insert(0, ("SYSTEM", s));
        }
        for (i, (role, text)) in messages.into_iter().enumerate() {
            if text.is_empty() {
                continue;
            }
            let content = format!("seed-{id}-{i}");
            startup.push(encode(json!({"event":{"contentStart":{"promptName":prompt,"contentName":content,"type":"TEXT","interactive":false,"role":role,"textInputConfiguration":{"mediaType":"text/plain"}}}}),&l)?);
            startup.push(encode(json!({"event":{"textInput":{"promptName":prompt,"contentName":content,"content":text}}}),&l)?);
            startup.push(encode(
                json!({"event":{"contentEnd":{"promptName":prompt,"contentName":content}}}),
                &l,
            )?);
        }
        startup.push(encode(json!({"event":{"contentStart":{"promptName":prompt,"contentName":audio,"type":"AUDIO","interactive":true,"role":"USER","audioInputConfiguration":{"mediaType":"audio/lpcm","sampleRateHertz":16000,"sampleSizeBits":16,"channelCount":1,"audioType":"SPEECH","encoding":"base64"}}}}),&l)?);
        let total: usize = startup.iter().map(Vec::len).sum();
        if startup.len() > 128 || total > 1024 * 1024 {
            return Err(error("Nova startup expansion exceeds bounds"));
        }
        let finish = vec![
            encode(
                json!({"event":{"contentEnd":{"promptName":prompt,"contentName":audio}}}),
                &l,
            )?,
            encode(json!({"event":{"promptEnd":{"promptName":prompt}}}), &l)?,
            encode(json!({"event":{"sessionEnd":{}}}), &l)?,
        ];
        Ok(Self {
            prompt,
            audio,
            startup,
            finish,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> NovaSonicConfig {
        let mut c = NovaSonicConfig::new(
            "us-east-1",
            "amazon.nova-2-sonic-v1:0",
            "matthew",
            AwsCredentials::new("AKIDEXAMPLE", "syntheticsecret", None).unwrap(),
        )
        .unwrap();
        c.endpoint=Some("http://127.0.0.1:1234/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream".into());
        c
    }
    #[test]
    fn endpoint_profile_rejects_before_io() {
        let mut c = config();
        assert_eq!(
            c.url().unwrap().path(),
            "/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream"
        );
        for endpoint in ["http://bedrock-runtime.us-east-1.amazonaws.com/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream","https://example.com/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream","http://localhost/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream","http://127.0.0.1/wrong","http://user:secret@127.0.0.1/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream","http://127.0.0.1/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream?secret=x"] {c.endpoint=Some(endpoint.into());assert!(c.validate().is_err());}
        c.endpoint=Some("http://127.0.0.1:1234/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream".into());
        assert!(c.validate().is_ok());
    }
    #[test]
    fn explicit_hosted_address_preserves_identity_and_rejects_invalid_plan() {
        let mut c = config();
        c.endpoint = None;
        assert!(c.validate().is_err());
        for address in [
            "192.0.2.1:80",
            "0.0.0.0:443",
            "224.0.0.1:443",
            "255.255.255.255:443",
            "[::]:443",
            "[ff02::1]:443",
        ] {
            c.connect_address = Some(address.parse().unwrap());
            assert!(c.validate().is_err());
        }
        c.connect_address = Some("192.0.2.1:443".parse().unwrap());
        assert!(c.validate().is_ok());
        assert_eq!(
            c.url().unwrap().host_str(),
            Some("bedrock-runtime.us-east-1.amazonaws.com")
        );
        c.endpoint = config().endpoint;
        assert!(c.validate().is_err());
        c.connect_address = None;
        assert!(c.validate().is_ok());
    }
    #[test]
    fn models_inference_and_limits_fail_closed() {
        for model in [
            "amazon.nova-sonic-v1:0",
            "../model",
            "arn:aws:bedrock:x",
            "amazon.nova-2-sonic-v1:0/other",
        ] {
            let mut c = config();
            c.model = model.into();
            assert!(c.validate().is_err());
        }
        let mut c = config();
        c.temperature = f64::NAN;
        assert!(c.validate().is_err());
        c = config();
        c.top_p = 0.0;
        assert!(c.validate().is_err());
        c = config();
        c.limits.session_timeout = Duration::from_secs(421);
        assert!(c.validate().is_err());
        c = config();
        c.limits.session_timeout = c.limits.drain_timeout;
        assert!(c.validate().is_err());
        c = config();
        c.limits.max_event_bytes = 1024;
        assert!(c.validate().is_err());
        c = config();
        c.limits.session_timeout = Duration::from_secs(5);
        c.limits.drain_timeout = Duration::from_secs(1);
        assert!(c.validate().is_err());
        let d = format!("{:?}", config());
        assert!(!d.contains("syntheticsecret"));
        assert!(!d.contains("AKIDEXAMPLE"));
    }
    #[test]
    fn exact_pcm_format_and_nested_bounds() {
        let c = config();
        let mut a = AudioRawFrame::new(vec![0, 1], 16000, 1).unwrap();
        assert!(c.audio(&a).is_ok());
        a.sample_rate = 24000;
        assert!(c.audio(&a).is_err());
        a.sample_rate = 16000;
        a.num_channels = 2;
        assert!(c.audio(&a).is_err());
        a.num_channels = 1;
        a.audio.push(0);
        assert!(c.audio(&a).is_err());
        a.audio = vec![0; c.limits.max_audio_bytes + 2];
        assert!(c.audio(&a).is_err());
    }
    #[test]
    fn seed_projection_is_bounded_single_noninteractive_system_history() {
        let c = config();
        let mut seed = LlmContextFrame::default();
        seed.messages.push(pipecat_core::ChatMessage {
            role: ChatRole::User,
            content: "seed user".into(),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
        });
        let plan = InputPlan::new(&c, &seed).unwrap();
        assert_eq!(plan.startup.len(), 6);
        assert_eq!(plan.finish.len(), 3);
        let e = pipecat_aws_common::eventstream::decode(&plan.startup[2], c.limits.max_event_bytes)
            .unwrap();
        let b = crate::protocol::decode_chunk(e.payload, &c.protocol_limits()).unwrap();
        let v: Value = serde_json::from_slice(&b).unwrap();
        assert_eq!(v["event"]["contentStart"]["interactive"], false);
        seed.messages[0].role = ChatRole::Developer;
        assert!(InputPlan::new(&c, &seed).is_err());
        seed.messages[0].role = ChatRole::System;
        let mut conflict = c;
        conflict.system_instruction = Some("different".into());
        assert!(InputPlan::new(&conflict, &seed).is_err());
    }
}
