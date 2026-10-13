//! Google's RTVI search response extension, separate from the closed base event enum.
//!
//! Normal provider results require text and a confidence vector. Core payloads
//! preserve nullable provider metadata, so projecting `None` is an error rather
//! than an empty-value fallback. Non-finite scores are rejected as well: native
//! JSON cannot represent them, and serde_json would otherwise write `null`.
use crate::{rtvi::Envelope, rtvi::MESSAGE_LABEL, ProtocolError, DEFAULT_MAX_MESSAGE_BYTES};
use pipecat_core::{LlmSearchOrigin, LlmSearchResponseFrame};
use serde::{ser::SerializeMap, ser::SerializeSeq, Deserialize, Serialize, Serializer};
use std::io::{self, Write};

pub const GOOGLE_SEARCH_RESPONSE_TYPE: &str = "bot-llm-search-response";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GoogleSearchResponseData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rendered_content: Option<String>,
    pub origins: Vec<GoogleSearchOrigin>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GoogleSearchOrigin {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site_title: Option<String>,
    #[serde(default)]
    pub results: Vec<GoogleSearchResult>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GoogleSearchResult {
    pub text: String,
    #[serde(default, serialize_with = "serialize_confidence")]
    pub confidence: Vec<f64>,
}

impl GoogleSearchResponseData {
    /// Construct a complete, bounded server envelope without a request ID.
    /// The complete JSON envelope is counted before allocating its data value.
    pub fn envelope(&self) -> Result<Envelope, ProtocolError> {
        validate_scores(self.origins.iter().flat_map(|origin| {
            origin
                .results
                .iter()
                .flat_map(|result| result.confidence.iter())
        }))?;
        bounded_json(&SearchEnvelopeRef::new(self))?;
        Ok(Envelope {
            label: MESSAGE_LABEL.into(),
            message_type: GOOGLE_SEARCH_RESPONSE_TYPE.into(),
            id: None,
            data: Some(serde_json::to_value(self)?),
        })
    }

    /// Decode this extension independently of the base `ServerEvent` enum.
    /// Count the complete borrowed envelope, including any ID and unknown data,
    /// before allocating owned strings or vectors for the typed payload.
    pub fn from_envelope(envelope: &Envelope) -> Result<Self, ProtocolError> {
        envelope.validate()?;
        if envelope.message_type != GOOGLE_SEARCH_RESPONSE_TYPE {
            return Err(ProtocolError::InvalidField("RTVI Google search type"));
        }
        let data = envelope
            .data
            .as_ref()
            .ok_or(ProtocolError::MissingField("RTVI Google search data"))?;
        if !data.is_object() {
            return Err(ProtocolError::InvalidField("RTVI Google search data"));
        }
        bounded_json(envelope)?;
        let value = Self::deserialize(data)?;
        validate_scores(value.origins.iter().flat_map(|origin| {
            origin
                .results
                .iter()
                .flat_map(|result| result.confidence.iter())
        }))?;
        Ok(value)
    }
}

impl TryFrom<&LlmSearchResponseFrame> for GoogleSearchResponseData {
    type Error = ProtocolError;

    fn try_from(frame: &LlmSearchResponseFrame) -> Result<Self, Self::Error> {
        for origin in &frame.origins {
            for result in &origin.results {
                if result.text.is_none() {
                    return Err(ProtocolError::MissingField(
                        "RTVI Google search result text",
                    ));
                }
                let scores = result
                    .confidence
                    .as_ref()
                    .ok_or(ProtocolError::MissingField(
                        "RTVI Google search result confidence",
                    ))?;
                validate_scores(scores.iter())?;
            }
        }
        // Borrowing adapters omit the same optional fields as the owned DTO.
        // Counting the projected envelope first bounds every following clone.
        bounded_json(&SearchEnvelopeRef::new(&CoreSearchData(frame)))?;
        Ok(Self {
            search_result: frame.search_result.clone(),
            rendered_content: frame.rendered_content.clone(),
            origins: frame
                .origins
                .iter()
                .map(|origin| GoogleSearchOrigin {
                    site_uri: origin.site_uri.clone(),
                    site_title: origin.site_title.clone(),
                    results: origin
                        .results
                        .iter()
                        .map(|result| GoogleSearchResult {
                            text: result.text.as_ref().expect("validated text").clone(),
                            confidence: result
                                .confidence
                                .as_ref()
                                .expect("validated confidence")
                                .clone(),
                        })
                        .collect(),
                })
                .collect(),
        })
    }
}

fn validate_scores<'a>(mut scores: impl Iterator<Item = &'a f64>) -> Result<(), ProtocolError> {
    if scores.any(|score| !score.is_finite()) {
        return Err(ProtocolError::InvalidField("RTVI Google search confidence"));
    }
    Ok(())
}

fn serialize_confidence<S: Serializer>(scores: &[f64], serializer: S) -> Result<S::Ok, S::Error> {
    validate_scores(scores.iter()).map_err(serde::ser::Error::custom)?;
    scores.serialize(serializer)
}

#[derive(Serialize)]
struct SearchEnvelopeRef<'a, T: Serialize> {
    label: &'static str,
    #[serde(rename = "type")]
    message_type: &'static str,
    data: &'a T,
}
impl<'a, T: Serialize> SearchEnvelopeRef<'a, T> {
    fn new(data: &'a T) -> Self {
        Self {
            label: MESSAGE_LABEL,
            message_type: GOOGLE_SEARCH_RESPONSE_TYPE,
            data,
        }
    }
}

struct CoreSearchData<'a>(&'a LlmSearchResponseFrame);
impl Serialize for CoreSearchData<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let frame = self.0;
        let mut map = serializer.serialize_map(None)?;
        if let Some(value) = &frame.search_result {
            map.serialize_entry("search_result", value)?;
        }
        if let Some(value) = &frame.rendered_content {
            map.serialize_entry("rendered_content", value)?;
        }
        map.serialize_entry("origins", &CoreOrigins(&frame.origins))?;
        map.end()
    }
}

struct CoreOrigins<'a>(&'a [LlmSearchOrigin]);
impl Serialize for CoreOrigins<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for origin in self.0 {
            sequence.serialize_element(&CoreOrigin(origin))?;
        }
        sequence.end()
    }
}

struct CoreOrigin<'a>(&'a LlmSearchOrigin);
impl Serialize for CoreOrigin<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        if let Some(value) = &self.0.site_uri {
            map.serialize_entry("site_uri", value)?;
        }
        if let Some(value) = &self.0.site_title {
            map.serialize_entry("site_title", value)?;
        }
        // Validated Some values serialize directly as strings and score arrays.
        map.serialize_entry("results", &self.0.results)?;
        map.end()
    }
}

fn bounded_json(value: &impl Serialize) -> Result<(), ProtocolError> {
    let mut writer = JsonBudget {
        remaining: DEFAULT_MAX_MESSAGE_BYTES,
        exceeded: false,
    };
    match serde_json::to_writer(&mut writer, value) {
        Ok(()) => Ok(()),
        Err(_) if writer.exceeded => Err(ProtocolError::MessageTooLarge {
            limit: DEFAULT_MAX_MESSAGE_BYTES,
        }),
        Err(error) => Err(error.into()),
    }
}

struct JsonBudget {
    remaining: usize,
    exceeded: bool,
}
impl Write for JsonBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            self.exceeded = true;
            return Err(io::Error::other("RTVI Google envelope exceeds byte limit"));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
