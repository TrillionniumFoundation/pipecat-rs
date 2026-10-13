use serde::{Deserialize, Serialize};

/// One provider grounding segment, preserving absent text and confidence scores.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmSearchResult {
    pub text: Option<String>,
    pub confidence: Option<Vec<f64>>,
}

/// A grounding source and the response segments attributed to it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LlmSearchOrigin {
    pub site_uri: Option<String>,
    pub site_title: Option<String>,
    pub results: Vec<LlmSearchResult>,
}

/// Google provider search metadata associated with a completed LLM response.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct LlmSearchResponseFrame {
    pub search_result: Option<String>,
    pub rendered_content: Option<String>,
    pub origins: Vec<LlmSearchOrigin>,
}
