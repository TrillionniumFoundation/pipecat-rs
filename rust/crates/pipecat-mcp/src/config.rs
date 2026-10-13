use crate::McpError;
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

/// A native executable selected by the application. No shell or PATH lookup is used.
/// Environment inheritance is deliberately disabled; pass each required variable explicitly.
#[derive(Clone)]
pub struct StdioConfig {
    pub executable: PathBuf,
    pub arguments: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub current_directory: Option<PathBuf>,
}
impl StdioConfig {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            arguments: vec![],
            environment: BTreeMap::new(),
            current_directory: None,
        }
    }
}
impl fmt::Debug for StdioConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StdioConfig")
            .field("executable", &"[redacted]")
            .field("argument_count", &self.arguments.len())
            .field("environment_count", &self.environment.len())
            .finish_non_exhaustive()
    }
}

/// Streamable HTTP only. Redirects and environment proxies are disabled. HTTPS
/// uses rustls with bundled public roots; plaintext is limited to loopback.
#[derive(Clone)]
pub struct HttpConfig {
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub terminate_on_close: bool,
}
impl HttpConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: BTreeMap::new(),
            terminate_on_close: true,
        }
    }
}
impl fmt::Debug for HttpConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpConfig")
            .field("url", &"[redacted]")
            .field("header_count", &self.headers.len())
            .field("terminate_on_close", &self.terminate_on_close)
            .finish()
    }
}
#[derive(Clone, Debug)]
pub enum ServerConfig {
    Stdio(StdioConfig),
    StreamableHttp(HttpConfig),
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub close_timeout: Duration,
    pub max_in_flight: usize,
    /// Maximum JSON message or raw SSE event, before deserialization.
    pub max_wire_bytes: usize,
    pub max_argument_bytes: usize,
    pub max_output_bytes: usize,
    pub max_schema_bytes: usize,
    pub max_tools: usize,
    pub max_pages: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            close_timeout: Duration::from_secs(3),
            max_in_flight: 8,
            max_wire_bytes: 1024 * 1024,
            max_argument_bytes: 64 * 1024,
            max_output_bytes: 256 * 1024,
            max_schema_bytes: 1024 * 1024,
            max_tools: 128,
            max_pages: 32,
        }
    }
}
impl Limits {
    fn checked_tool_timeout(&self) -> Option<Duration> {
        self.connect_timeout
            .checked_add(self.request_timeout)
            .and_then(|timeout| timeout.checked_add(self.close_timeout))
    }
    pub(crate) fn tool_timeout(&self) -> Duration {
        self.checked_tool_timeout()
            .expect("MCP client construction validated the combined tool deadline")
    }
}
/// Filters run synchronously and must be fast. A filter error or panic becomes a
/// redacted tool failure. It is never passed to a model verbatim.
pub type OutputFilter = Arc<dyn Fn(String) -> Result<String, String> + Send + Sync>;
#[derive(Clone)]
pub struct McpConfig {
    pub server: ServerConfig,
    pub limits: Limits,
    /// Like the source implementation, None and an empty set expose all tools.
    pub tools_filter: Option<BTreeSet<String>>,
    pub tools_arguments: BTreeMap<String, Map<String, Value>>,
    pub tools_output_filters: BTreeMap<String, OutputFilter>,
}
impl McpConfig {
    pub fn new(server: ServerConfig) -> Self {
        Self {
            server,
            limits: Limits::default(),
            tools_filter: None,
            tools_arguments: BTreeMap::new(),
            tools_output_filters: BTreeMap::new(),
        }
    }
    pub(crate) fn allowed(&self, name: &str) -> bool {
        self.tools_filter
            .as_ref()
            .is_none_or(|names| names.is_empty() || names.contains(name))
    }
    pub(crate) fn validate(&self) -> Result<(), McpError> {
        let l = &self.limits;
        if l.checked_tool_timeout()
            .is_none_or(|timeout| std::time::Instant::now().checked_add(timeout).is_none())
        {
            return Err(McpError::Config("combined tool timeout must be finite"));
        }
        if [l.connect_timeout, l.request_timeout, l.close_timeout]
            .iter()
            .any(|d| d.is_zero() || tokio::time::Instant::now().checked_add(*d).is_none())
            || [
                l.max_in_flight,
                l.max_wire_bytes,
                l.max_argument_bytes,
                l.max_output_bytes,
                l.max_schema_bytes,
                l.max_tools,
                l.max_pages,
            ]
            .contains(&0)
            || l.max_in_flight > 1024
        {
            return Err(McpError::Config(
                "limits must be positive, finite, and concurrency at most 1024",
            ));
        }
        match &self.server {
            ServerConfig::Stdio(s) => {
                if !s.executable.is_absolute()
                    || s.current_directory
                        .as_ref()
                        .is_some_and(|p| !p.is_absolute())
                    || s.arguments.iter().any(|a| a.contains('\0'))
                    || s.environment
                        .iter()
                        .any(|(k, v)| k.is_empty() || k.contains(['=', '\0']) || v.contains('\0'))
                {
                    return Err(McpError::Config(
                        "stdio requires absolute paths and valid explicit arguments/environment",
                    ));
                }
            }
            ServerConfig::StreamableHttp(h) => {
                let url =
                    reqwest::Url::parse(&h.url).map_err(|_| McpError::Config("invalid MCP URL"))?;
                let loopback = url.host_str().is_some_and(|s| {
                    s == "localhost"
                        || s.trim_matches(['[', ']'])
                            .parse::<std::net::IpAddr>()
                            .is_ok_and(|a| a.is_loopback())
                });
                if url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                    || url.fragment().is_some()
                    || !(url.scheme() == "https" || url.scheme() == "http" && loopback)
                {
                    return Err(McpError::Config(
                        "MCP URL requires HTTPS or loopback HTTP, with no userinfo or fragment",
                    ));
                }
                for (key, value) in &h.headers {
                    let name = http::HeaderName::from_bytes(key.as_bytes())
                        .map_err(|_| McpError::Config("invalid MCP header"))?;
                    http::HeaderValue::from_str(value)
                        .map_err(|_| McpError::Config("invalid MCP header"))?;
                    if matches!(
                        name.as_str(),
                        "host"
                            | "content-type"
                            | "content-length"
                            | "transfer-encoding"
                            | "connection"
                            | "accept"
                            | "last-event-id"
                    ) || name.as_str().starts_with("mcp-")
                    {
                        return Err(McpError::Config("MCP protocol header cannot be overridden"));
                    }
                }
            }
        }
        for args in self.tools_arguments.values() {
            if serde_json::to_vec(args)
                .map_err(|_| McpError::InvalidArguments)?
                .len()
                > l.max_argument_bytes
            {
                return Err(McpError::Limit("fixed arguments"));
            }
        }
        Ok(())
    }
}
impl fmt::Debug for McpConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpConfig")
            .field("server", &self.server)
            .field("limits", &self.limits)
            .field(
                "tools_filter_count",
                &self.tools_filter.as_ref().map(BTreeSet::len),
            )
            .field("fixed_argument_tool_count", &self.tools_arguments.len())
            .field("output_filter_count", &self.tools_output_filters.len())
            .finish()
    }
}
