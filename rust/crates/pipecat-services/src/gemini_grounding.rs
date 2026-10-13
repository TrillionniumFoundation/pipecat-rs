use crate::ServiceError;
use pipecat_core::{LlmSearchOrigin, LlmSearchResponseFrame, LlmSearchResult};
use serde_json::{Map, Value};
use std::collections::HashSet;

struct Origin<'a> {
    site_uri: Option<&'a str>,
    site_title: Option<&'a str>,
    results: Vec<ResultRef<'a>>,
}

#[derive(Clone, Copy)]
struct ResultRef<'a> {
    text: Option<&'a str>,
    confidence: Option<&'a [Value]>,
}

struct Budget {
    bytes: usize,
    blocks: usize,
    max_bytes: usize,
    max_blocks: usize,
}
impl Budget {
    fn add(&mut self, bytes: usize, blocks: usize) -> Result<(), ServiceError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|total| *total <= self.max_bytes)
            .ok_or_else(|| invalid("expanded metadata exceeds size limit"))?;
        self.blocks = self
            .blocks
            .checked_add(blocks)
            .filter(|total| *total <= self.max_blocks)
            .ok_or_else(|| invalid("expanded metadata has too many entries"))?;
        Ok(())
    }
}

/// Borrow and budget the complete source projection before cloning its strings.
pub(crate) fn project(
    metadata: &Value,
    max_bytes: usize,
    max_blocks: usize,
) -> Result<Option<LlmSearchResponseFrame>, ServiceError> {
    if metadata.is_null() {
        return Ok(None);
    }
    let metadata = metadata
        .as_object()
        .ok_or_else(|| invalid("metadata must be an object"))?;
    let Some(chunks) = array(metadata, "groundingChunks")?.filter(|chunks| !chunks.is_empty())
    else {
        return Ok(None);
    };
    if chunks.len() > max_blocks {
        return Err(invalid("too many grounding chunks"));
    }
    let mut budget = Budget {
        bytes: 0,
        blocks: 0,
        max_bytes,
        max_blocks,
    };
    let mut origins = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let chunk = chunk
            .as_object()
            .ok_or_else(|| invalid("grounding chunk must be an object"))?;
        let web = object(chunk, "web")?;
        let site_uri = web.map(|web| string(web, "uri")).transpose()?.flatten();
        let site_title = web.map(|web| string(web, "title")).transpose()?.flatten();
        budget.add(std::mem::size_of::<LlmSearchOrigin>(), 1)?;
        budget.add(site_uri.map_or(0, str::len), 0)?;
        budget.add(site_title.map_or(0, str::len), 0)?;
        origins.push(Origin {
            site_uri,
            site_title,
            results: vec![],
        });
    }
    let entry_point = object(metadata, "searchEntryPoint")?;
    let rendered_content = entry_point
        .map(|entry_point| string(entry_point, "renderedContent"))
        .transpose()?
        .flatten();
    budget.add(rendered_content.map_or(0, str::len), 0)?;

    if let Some(supports) = array(metadata, "groundingSupports")? {
        if supports.len() > max_blocks {
            return Err(invalid("too many grounding supports"));
        }
        for support in supports {
            let support = support
                .as_object()
                .ok_or_else(|| invalid("grounding support must be an object"))?;
            let text = match object(support, "segment")? {
                Some(segment) => string(segment, "text")?,
                None => Some(""),
            };
            let confidence = array(support, "confidenceScores")?;
            if confidence.is_some_and(|scores| scores.iter().any(|score| score.as_f64().is_none()))
            {
                return Err(invalid("confidence scores must be numbers"));
            }
            let Some(indices) = array(support, "groundingChunkIndices")? else {
                continue;
            };
            // Python tests membership once per origin, even for duplicate indices.
            let mut matched = HashSet::new();
            for index in indices {
                let index = match index.as_u64() {
                    Some(index) => usize::try_from(index).ok(),
                    None if index.as_i64().is_some() => None,
                    None => return Err(invalid("grounding chunk index must be an integer")),
                };
                let Some(index) = index.filter(|index| *index < origins.len()) else {
                    continue;
                };
                if !matched.insert(index) {
                    continue;
                }
                budget.add(std::mem::size_of::<LlmSearchResult>(), 1)?;
                budget.add(text.map_or(0, str::len), 0)?;
                let confidence_bytes = confidence
                    .map_or(0, <[Value]>::len)
                    .checked_mul(std::mem::size_of::<f64>())
                    .ok_or_else(|| invalid("expanded metadata exceeds size limit"))?;
                budget.add(confidence_bytes, 0)?;
                origins[index].results.push(ResultRef { text, confidence });
            }
        }
    }

    Ok(Some(LlmSearchResponseFrame {
        search_result: None,
        rendered_content: rendered_content.map(str::to_owned),
        origins: origins
            .into_iter()
            .map(|origin| LlmSearchOrigin {
                site_uri: origin.site_uri.map(str::to_owned),
                site_title: origin.site_title.map(str::to_owned),
                results: origin
                    .results
                    .into_iter()
                    .map(|result| LlmSearchResult {
                        text: result.text.map(str::to_owned),
                        confidence: result.confidence.map(|scores| {
                            scores
                                .iter()
                                .map(|score| score.as_f64().expect("validated grounding score"))
                                .collect()
                        }),
                    })
                    .collect(),
            })
            .collect(),
    }))
}

fn array<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<Option<&'a [Value]>, ServiceError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => Ok(Some(values)),
        Some(_) => Err(invalid("expected an array")),
    }
}

fn object<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<Option<&'a Map<String, Value>>, ServiceError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(value)) => Ok(Some(value)),
        Some(_) => Err(invalid("expected an object")),
    }
}

fn string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
) -> Result<Option<&'a str>, ServiceError> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(invalid("expected a string")),
    }
}

fn invalid(message: &str) -> ServiceError {
    ServiceError::Protocol(format!("invalid Gemini grounding: {message}"))
}
