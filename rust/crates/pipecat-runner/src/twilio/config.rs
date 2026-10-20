use super::{security, TwilioError};
use std::{fmt, time::Duration};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HangupPolicy {
    #[default]
    Disabled,
    /// Complete a verified call once on local End/Cancel, disconnect, or failure.
    /// A Twilio stop event already denotes a completed stream and is excluded.
    OnLocalTermination,
}

/// Explicit trusted host configuration. No field is read from inbound metadata.
#[derive(Clone)]
pub struct TwilioConfig {
    pub account_sid: String,
    pub auth_token: String,
    pub webhook_url: String,
    /// Canonical base URL, without the generated capability suffix.
    pub media_url: String,
    pub hangup_policy: HangupPolicy,
    /// Must end in `/2010-04-01`; the app appends a validated account/call path.
    pub api_base: String,
    pub max_calls: usize,
    pub max_connections: usize,
    pub queue_capacity: usize,
    pub max_message_bytes: usize,
    pub max_form_bytes: usize,
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
    pub session_timeout: Duration,
    pub io_timeout: Duration,
    pub replay_retention: Duration,
    pub turn_timeout: Duration,
    pub introduce: bool,
}
impl fmt::Debug for TwilioConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TwilioConfig")
            .field("auth_token", &"[redacted]")
            .field("hangup_policy", &self.hangup_policy)
            .field("max_calls", &self.max_calls)
            .finish_non_exhaustive()
    }
}
impl TwilioConfig {
    pub fn new(
        account_sid: String,
        auth_token: String,
        webhook_url: String,
        media_url: String,
    ) -> Self {
        Self {
            account_sid,
            auth_token,
            webhook_url,
            media_url,
            hangup_policy: HangupPolicy::Disabled,
            api_base: "https://api.twilio.com/2010-04-01".into(),
            max_calls: 8,
            max_connections: 32,
            queue_capacity: 8,
            max_message_bytes: 64 * 1024,
            max_form_bytes: 16 * 1024,
            connect_timeout: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(300),
            session_timeout: Duration::from_secs(3600),
            io_timeout: Duration::from_secs(5),
            replay_retention: Duration::from_secs(300),
            turn_timeout: Duration::from_millis(600),
            introduce: true,
        }
    }
    /// Called only after explicit enablement. Tests inject a lookup, never ambient credentials.
    pub fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self, TwilioError> {
        let mut required = |name| {
            get(name)
                .filter(|s| !s.is_empty())
                .ok_or(TwilioError::Configuration("missing Twilio setting"))
        };
        let mut config = Self::new(
            required("TWILIO_ACCOUNT_SID")?,
            required("TWILIO_AUTH_TOKEN")?,
            required("PIPECAT_TWILIO_WEBHOOK_URL")?,
            required("PIPECAT_TWILIO_MEDIA_URL")?,
        );
        config.hangup_policy = match get("PIPECAT_TWILIO_HANGUP")
            .as_deref()
            .unwrap_or("disabled")
        {
            "disabled" => HangupPolicy::Disabled,
            "local" => HangupPolicy::OnLocalTermination,
            _ => {
                return Err(TwilioError::Configuration(
                    "hangup policy must be disabled or local",
                ));
            }
        };
        if let Some(base) = get("PIPECAT_TWILIO_API_BASE") {
            config.api_base = base;
        }
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), TwilioError> {
        let fail = TwilioError::Configuration;
        if !security::valid_sid(&self.account_sid, "AC")
            || self.auth_token.trim().is_empty()
            || self.auth_token.len() > 1024
        {
            return Err(fail("invalid Twilio account or secret"));
        }
        if security::validate_url(&self.webhook_url, "http")
            .map_err(fail)?
            .path()
            != "/twilio/voice"
            || security::validate_url(&self.media_url, "ws")
                .map_err(fail)?
                .path()
                != "/twilio/media"
            || security::validate_url(&self.api_base, "http")
                .map_err(fail)?
                .path()
                != "/2010-04-01"
        {
            return Err(fail("canonical URLs require the documented exact paths"));
        }
        if !(1..=128).contains(&self.max_calls)
            || !(1..=512).contains(&self.max_connections)
            || self.max_connections < self.max_calls
            || !(1..=128).contains(&self.queue_capacity)
            || !(1024..=256 * 1024).contains(&self.max_message_bytes)
            || !(128..=64 * 1024).contains(&self.max_form_bytes)
            || [
                self.connect_timeout,
                self.idle_timeout,
                self.session_timeout,
                self.io_timeout,
                self.replay_retention,
                self.turn_timeout,
            ]
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(86400))
        {
            return Err(fail("invalid Twilio resource limits"));
        }
        Ok(())
    }
}
