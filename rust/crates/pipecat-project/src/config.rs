//! Strict, credential-free project configuration.
use crate::ProjectError;
use serde::{Deserialize, Serialize};

/// Configuration of the supported native voice-agent scaffold.
/// Unknown fields are rejected, including credential values and unsupported features.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProjectConfig {
    pub name: String,
    pub transport: String,
    pub mode: String,
    pub stt: String,
    pub llm: String,
    pub tts: String,
    pub client: String,
    pub eval: String,
    pub deploy_to_cloud: bool,
}
impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            transport: "websocket".into(),
            mode: "cascade".into(),
            stt: "deepgram".into(),
            llm: "openai".into(),
            tts: "cartesia".into(),
            client: "none".into(),
            eval: "none".into(),
            deploy_to_cloud: false,
        }
    }
}
impl ProjectConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    /// Parses at most 16 KiB without echoing unknown keys or supplied values.
    pub fn from_json(bytes: &[u8]) -> Result<Self, ProjectError> {
        if bytes.len() > 16 * 1024 {
            return Err(ProjectError::Configuration("config exceeds 16 KiB"));
        }
        if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
            return Err(ProjectError::Configuration("config must be a JSON object"));
        }
        serde_json::from_slice(bytes).map_err(|_| {
            ProjectError::Configuration("invalid JSON config or unknown fields; use --list-options")
        })
    }

    pub fn validate(&self) -> Result<(), ProjectError> {
        validate_name(&self.name)?;
        if !matches!(
            self.transport.as_str(),
            "websocket" | "smallwebrtc" | "twilio"
        ) {
            return Err(ProjectError::Configuration("only websocket, smallwebrtc and twilio have integrated native scaffolds; Daily, LiveKit and other carriers are unsupported"));
        }
        if matches!(self.transport.as_str(), "smallwebrtc" | "twilio")
            && matches!(self.name.as_str(), "std" | "core")
        {
            return Err(ProjectError::Configuration(
                "name conflicts with the generated library and Rust standard library",
            ));
        }
        if self.mode != "cascade" {
            return Err(ProjectError::Configuration(
                "only cascade mode is supported; no realtime scaffold is implemented",
            ));
        }
        if self.stt != "deepgram" || self.llm != "openai" || self.tts != "cartesia" {
            return Err(ProjectError::Configuration(
                "this native runner supports exactly --stt deepgram --llm openai --tts cartesia",
            ));
        }
        if self.client != "none" && self.client != "rust-wasm" {
            return Err(ProjectError::Configuration(
                "client must be none or rust-wasm",
            ));
        }
        if self.deploy_to_cloud {
            return Err(ProjectError::Configuration("cloud deployment has no native implementation; no deployment files will be generated"));
        }
        if self.eval != "none" && self.eval != "text" {
            return Err(ProjectError::Configuration("eval must be none or text"));
        }
        if self.transport == "twilio" && (self.client != "none" || self.eval != "none") {
            return Err(ProjectError::Configuration(
                "twilio requires client=none and eval=none; browser clients and phone eval are not generated",
            ));
        }
        if self.eval == "text" && EVAL_RESERVED.contains(&self.name.replace('-', "_").as_str()) {
            return Err(ProjectError::Configuration(
                "name conflicts with the eval binary or a direct eval dependency",
            ));
        }
        Ok(())
    }
}

const EVAL_RESERVED: &[&str] = &[
    "eval_bot",
    "pipecat_core",
    "pipecat_evals",
    "pipecat_processors",
    "pipecat_services",
    "pipecat_turns",
    "reqwest",
    "serde",
    "serde_json",
    "url",
];

/// Restricts Cargo package names to portable single directory components.
pub fn validate_name(name: &str) -> Result<(), ProjectError> {
    let mut chars = name.bytes();
    if name.is_empty()
        || name.len() > 64
        || !chars.next().is_some_and(|c| c.is_ascii_lowercase())
        || !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, b'-' | b'_'))
        || name.ends_with(['-', '_'])
    {
        return Err(ProjectError::Configuration("name must be 1–64 ASCII characters, start with a lowercase letter, contain only lowercase letters/digits/-/_, and end with a letter or digit"));
    }
    let normalized = name.replace('-', "_");
    const RESERVED: &[&str] = &[
        "as",
        "async",
        "await",
        "break",
        "const",
        "continue",
        "crate",
        "dyn",
        "else",
        "enum",
        "extern",
        "false",
        "fn",
        "for",
        "if",
        "impl",
        "in",
        "let",
        "loop",
        "match",
        "mod",
        "move",
        "mut",
        "pub",
        "ref",
        "return",
        "self",
        "static",
        "struct",
        "super",
        "trait",
        "true",
        "type",
        "unsafe",
        "use",
        "where",
        "while",
        "abstract",
        "become",
        "box",
        "do",
        "final",
        "gen",
        "macro",
        "override",
        "priv",
        "try",
        "typeof",
        "unsized",
        "virtual",
        "yield",
        "test",
        "build",
        "deps",
        "examples",
        "incremental",
        "con",
        "prn",
        "aux",
        "nul",
        "com1",
        "com2",
        "com3",
        "com4",
        "com5",
        "com6",
        "com7",
        "com8",
        "com9",
        "lpt1",
        "lpt2",
        "lpt3",
        "lpt4",
        "lpt5",
        "lpt6",
        "lpt7",
        "lpt8",
        "lpt9",
        "pipecat_runner",
        "tokio",
    ];
    if RESERVED.contains(&normalized.as_str()) {
        return Err(ProjectError::Configuration(
            "name is reserved by Rust, Cargo, the platform, or a direct dependency",
        ));
    }
    Ok(())
}

/// Machine-readable capabilities; absent integrations must never be substituted.
pub fn supported_options() -> serde_json::Value {
    serde_json::json!({
        "transport": ["websocket", "smallwebrtc", "twilio"], "mode": ["cascade"],
        "stt": ["deepgram"], "llm": ["openai"], "tts": ["cartesia"],
        "client": ["none", "rust-wasm"], "deploy_to_cloud": [false],
        "eval": ["none", "text"],
        "transport_constraints": {"twilio": {"client": ["none"], "eval": ["none"]}},
        "source": "existing local Rust workspace (--source-rust)",
        "overwrite": false
    })
}
