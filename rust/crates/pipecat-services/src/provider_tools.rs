//! Bounded provider declarations carried separately from executable tools.

use crate::{CompletionRequest, ServiceError};
use serde_json::Value;
use std::{collections::BTreeMap, fmt, io};

/// Validated OpenAI declarations. Their fields are forwarded unchanged and do
/// not register local callbacks or establish provider support for a tool type.
#[derive(Clone, Default)]
pub struct OpenAiToolDeclarations {
    tools: Vec<BTreeMap<String, Value>>,
}

impl OpenAiToolDeclarations {
    pub const MAX_TOOLS: usize = 128;
    pub const MAX_JSON_BYTES: usize = 64 * 1024;

    pub fn new(tools: Vec<BTreeMap<String, Value>>) -> Result<Self, ServiceError> {
        Self::validate(&tools)?;
        Ok(Self { tools })
    }

    /// Validate borrowed declarations before allocating their owned copy.
    pub fn from_slice(tools: &[BTreeMap<String, Value>]) -> Result<Self, ServiceError> {
        Self::validate(tools)?;
        Ok(Self {
            tools: tools.to_vec(),
        })
    }

    pub fn as_slice(&self) -> &[BTreeMap<String, Value>] {
        &self.tools
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    fn validate(tools: &[BTreeMap<String, Value>]) -> Result<(), ServiceError> {
        if tools.len() > Self::MAX_TOOLS {
            return Err(ServiceError::InvalidRequest(
                "OpenAI declarations exceed tool count limit".into(),
            ));
        }
        for tool in tools {
            if tool
                .get("type")
                .and_then(Value::as_str)
                .is_none_or(|kind| kind.trim().is_empty())
            {
                return Err(ServiceError::InvalidRequest(
                    "OpenAI declarations require a nonempty string type".into(),
                ));
            }
            for value in tool.values() {
                validate_depth(value, 1)?;
            }
        }
        serde_json::to_writer(
            JsonBudget {
                remaining: Self::MAX_JSON_BYTES,
            },
            tools,
        )
        .map_err(|_| {
            ServiceError::InvalidRequest("OpenAI declarations exceed JSON byte limit".into())
        })
    }
}

impl fmt::Debug for OpenAiToolDeclarations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiToolDeclarations")
            .field("count", &self.tools.len())
            .finish()
    }
}

/// Validated Gemini search declarations. This initial contract accepts only
/// `{"google_search": {}}`; declarations advertise tools without local execution.
#[derive(Clone, Default)]
pub struct GeminiToolDeclarations {
    tools: Vec<BTreeMap<String, Value>>,
}

impl GeminiToolDeclarations {
    pub const MAX_TOOLS: usize = 128;
    pub const MAX_JSON_BYTES: usize = 64 * 1024;

    pub fn new(tools: Vec<BTreeMap<String, Value>>) -> Result<Self, ServiceError> {
        Self::validate(&tools)?;
        Ok(Self { tools })
    }

    /// Validate borrowed declarations before allocating their owned copy.
    pub fn from_slice(tools: &[BTreeMap<String, Value>]) -> Result<Self, ServiceError> {
        Self::validate(tools)?;
        Ok(Self {
            tools: tools.to_vec(),
        })
    }

    pub fn as_slice(&self) -> &[BTreeMap<String, Value>] {
        &self.tools
    }

    pub fn len(&self) -> usize {
        self.tools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    fn validate(tools: &[BTreeMap<String, Value>]) -> Result<(), ServiceError> {
        if tools.len() > Self::MAX_TOOLS {
            return Err(ServiceError::InvalidRequest(
                "Gemini declarations exceed tool count limit".into(),
            ));
        }
        for tool in tools {
            if tool.len() != 1
                || !tool
                    .get("google_search")
                    .and_then(Value::as_object)
                    .is_some_and(|config| config.is_empty())
            {
                return Err(ServiceError::Unsupported(
                    "Gemini declarations require an empty google_search configuration".into(),
                ));
            }
        }
        serde_json::to_writer(
            JsonBudget {
                remaining: Self::MAX_JSON_BYTES,
            },
            tools,
        )
        .map_err(|_| {
            ServiceError::InvalidRequest("Gemini declarations exceed JSON byte limit".into())
        })
    }
}

impl fmt::Debug for GeminiToolDeclarations {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeminiToolDeclarations")
            .field("count", &self.tools.len())
            .finish()
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) enum NativeToolDeclarations {
    #[default]
    None,
    OpenAi(OpenAiToolDeclarations),
    Gemini(GeminiToolDeclarations),
}

fn validate_depth(value: &Value, depth: usize) -> Result<(), ServiceError> {
    if depth > pipecat_core::NativeLlmContinuation::MAX_ARGUMENT_DEPTH {
        return Err(ServiceError::InvalidRequest(
            "OpenAI declarations exceed nesting limit".into(),
        ));
    }
    match value {
        Value::Array(values) => {
            for value in values {
                validate_depth(value, depth + 1)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_depth(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

struct JsonBudget {
    remaining: usize,
}

impl io::Write for JsonBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(io::Error::other("declaration JSON limit exceeded"));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A completion request with validated advertisement-only declarations.
/// The existing `CompletionRequest` fields and serialization remain unchanged.
#[derive(Clone)]
pub struct DeclaredRequest {
    request: CompletionRequest,
    openai_tools: OpenAiToolDeclarations,
}

impl DeclaredRequest {
    pub fn new(request: CompletionRequest, openai_tools: OpenAiToolDeclarations) -> Self {
        Self {
            request,
            openai_tools,
        }
    }

    pub fn request(&self) -> &CompletionRequest {
        &self.request
    }

    pub fn openai_tools(&self) -> &OpenAiToolDeclarations {
        &self.openai_tools
    }

    pub fn into_parts(self) -> (CompletionRequest, OpenAiToolDeclarations) {
        (self.request, self.openai_tools)
    }
}

impl From<CompletionRequest> for DeclaredRequest {
    fn from(request: CompletionRequest) -> Self {
        Self::new(request, OpenAiToolDeclarations::default())
    }
}

impl fmt::Debug for DeclaredRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeclaredRequest")
            .field("openai_tools", &self.openai_tools)
            .finish_non_exhaustive()
    }
}

/// A completion request with validated Gemini search advertisements. The
/// existing OpenAI `DeclaredRequest` representation remains a distinct type.
#[derive(Clone)]
pub struct GeminiDeclaredRequest {
    request: CompletionRequest,
    gemini_tools: GeminiToolDeclarations,
}

impl GeminiDeclaredRequest {
    pub fn new(request: CompletionRequest, gemini_tools: GeminiToolDeclarations) -> Self {
        Self {
            request,
            gemini_tools,
        }
    }

    pub fn request(&self) -> &CompletionRequest {
        &self.request
    }

    pub fn gemini_tools(&self) -> &GeminiToolDeclarations {
        &self.gemini_tools
    }

    pub fn into_parts(self) -> (CompletionRequest, GeminiToolDeclarations) {
        (self.request, self.gemini_tools)
    }
}

impl From<CompletionRequest> for GeminiDeclaredRequest {
    fn from(request: CompletionRequest) -> Self {
        Self::new(request, GeminiToolDeclarations::default())
    }
}

impl fmt::Debug for GeminiDeclaredRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeminiDeclaredRequest")
            .field("gemini_tools", &self.gemini_tools)
            .finish_non_exhaustive()
    }
}
