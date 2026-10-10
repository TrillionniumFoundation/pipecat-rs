use crate::{Error, Result};
use std::{net::SocketAddr, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoqMode {
    Client,
    Serve,
    Browser,
}

/// Trust roots are scoped to this connection; no global certificate settings change.
#[derive(Clone)]
pub enum TlsTrust {
    WebPki,
    /// A nonempty, explicitly supplied set of DER trust roots.
    Roots(Vec<Vec<u8>>),
    /// Deliberately rejected before opening a socket in this client slice.
    MutualTls,
}

#[derive(Debug, Clone)]
pub struct MoqLimits {
    pub outgoing_queue: usize,
    pub incoming_queue: usize,
    pub compressed_record_bytes: usize,
    pub expanded_record_bytes: usize,
    pub transcript_bytes: usize,
    pub transcript_records: usize,
    pub catalog_bytes: usize,
    pub connect_timeout: Duration,
    pub end_timeout: Duration,
    pub close_grace: Duration,
    pub audio_max_age: Duration,
}
impl Default for MoqLimits {
    fn default() -> Self {
        Self {
            outgoing_queue: 64,
            incoming_queue: 64,
            compressed_record_bytes: 64 * 1024,
            expanded_record_bytes: 64 * 1024,
            transcript_bytes: 4 * 1024 * 1024,
            transcript_records: 4096,
            catalog_bytes: 64 * 1024,
            connect_timeout: Duration::from_secs(10),
            end_timeout: Duration::from_secs(3),
            close_grace: Duration::from_millis(100),
            audio_max_age: Duration::from_millis(500),
        }
    }
}

/// One peer, one client session, and one instance-owned transcript epoch.
/// URLs are not included in Debug output, since relay queries may contain tokens.
#[derive(Clone)]
pub struct MoqConfig {
    pub relay_url: String,
    pub publish_path: String,
    pub peer_path: String,
    pub audio_track: String,
    pub transcript_track: String,
    pub bind: SocketAddr,
    pub trust: TlsTrust,
    pub limits: MoqLimits,
    pub mode: MoqMode,
    pub video_enabled: bool,
    pub reconnect: bool,
    pub audio_in_enabled: bool,
    pub audio_out_enabled: bool,
    pub audio_out_sample_rate: u32,
}
impl MoqConfig {
    pub fn new(relay_url: impl Into<String>) -> Self {
        Self {
            relay_url: relay_url.into(),
            publish_path: "pipecat/response".into(),
            peer_path: "pipecat/request".into(),
            audio_track: "bot-audio".into(),
            transcript_track: "transcript.json.z".into(),
            bind: "0.0.0.0:0".parse().expect("constant address"),
            trust: TlsTrust::WebPki,
            limits: MoqLimits::default(),
            mode: MoqMode::Client,
            video_enabled: false,
            reconnect: false,
            audio_in_enabled: true,
            audio_out_enabled: true,
            audio_out_sample_rate: 48_000,
        }
    }
    pub(crate) fn validate(&self) -> Result<url::Url> {
        match self.mode {
            MoqMode::Client => {}
            MoqMode::Serve => return Err(Error::Unsupported("serve")),
            MoqMode::Browser => return Err(Error::Unsupported("browser runtime")),
        }
        if self.video_enabled {
            return Err(Error::Unsupported("video"));
        }
        if self.reconnect {
            return Err(Error::Unsupported("reconnect"));
        }
        if matches!(self.trust, TlsTrust::MutualTls) {
            return Err(Error::Unsupported("mutual TLS"));
        }
        if self.audio_out_sample_rate != 48_000 {
            return Err(Error::Unsupported("output sample rate; requires 48000 Hz"));
        }
        if matches!(&self.trust, TlsTrust::Roots(roots) if roots.is_empty() || roots.len() > 32 || roots.iter().any(|r| r.is_empty() || r.len() > 64 * 1024))
        {
            return Err(Error::InvalidConfig("trust root bounds"));
        }
        let url =
            url::Url::parse(&self.relay_url).map_err(|_| Error::InvalidConfig("relay URL"))?;
        if url.scheme() != "https"
            || url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(Error::InvalidConfig(
                "requires an HTTPS URL without userinfo or fragment",
            ));
        }
        for path in [&self.publish_path, &self.peer_path] {
            if path.len() > 256
                || path.is_empty()
                || path.split('/').any(|s| {
                    s.is_empty()
                        || s == "."
                        || s == ".."
                        || !s
                            .bytes()
                            .all(|c| c.is_ascii_alphanumeric() || b"_- .".contains(&c))
                        || s.contains(' ')
                })
            {
                return Err(Error::InvalidConfig("broadcast path"));
            }
        }
        if self.publish_path == self.peer_path {
            return Err(Error::InvalidConfig("publish and peer paths must differ"));
        }
        for name in [&self.audio_track, &self.transcript_track] {
            if name.is_empty()
                || name.len() > 128
                || !name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"_- .".contains(&c))
                || name.contains(' ')
            {
                return Err(Error::InvalidConfig("track name"));
            }
        }
        if self.audio_track == self.transcript_track
            || ["catalog.json", "catalog.json.z"].contains(&self.audio_track.as_str())
            || ["catalog.json", "catalog.json.z"].contains(&self.transcript_track.as_str())
        {
            return Err(Error::InvalidConfig("track name collision"));
        }
        let l = &self.limits;
        if !(1..=1024).contains(&l.outgoing_queue)
            || !(1..=1024).contains(&l.incoming_queue)
            || !(1024..=256 * 1024).contains(&l.compressed_record_bytes)
            || !(1024..=256 * 1024).contains(&l.expanded_record_bytes)
            || !(4096..=16 * 1024 * 1024).contains(&l.transcript_bytes)
            || !(2..=4096).contains(&l.transcript_records)
            || !(1024..=256 * 1024).contains(&l.catalog_bytes)
            || l.connect_timeout.is_zero()
            || l.connect_timeout > Duration::from_secs(120)
            || l.end_timeout.is_zero()
            || l.end_timeout > Duration::from_secs(30)
            || l.close_grace > l.end_timeout
            || l.audio_max_age > Duration::from_secs(2)
        {
            return Err(Error::InvalidConfig("limits"));
        }
        Ok(url)
    }
}
