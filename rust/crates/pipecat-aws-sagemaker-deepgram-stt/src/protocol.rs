use crate::{Error, SttLimits, TranscriptionSettings};
use pipecat_aws_common::eventstream::{self, HeaderValue};
use pipecat_core::{DetailedTranscriptionFrame, FrameData, TextFrame};
use serde_json::{json, Value};

pub(crate) fn payload(raw: &[u8], datatype: &str, limit: usize) -> Result<Vec<u8>, Error> {
    eventstream::encode(
        &[
            (":message-type", "event"),
            (":event-type", "PayloadPart"),
            (":content-type", "application/octet-stream"),
            ("DataType", datatype),
        ],
        raw,
        limit,
    )
    .map_err(|_| Error::TooLarge)
}
pub(crate) fn envelope(value: &Value, limit: usize) -> Result<Vec<u8>, Error> {
    payload(
        &serde_json::to_vec(value).map_err(|_| Error::Envelope)?,
        "UTF8",
        limit,
    )
}
pub(crate) fn control(kind: &str, limit: usize) -> Result<Vec<u8>, Error> {
    envelope(&json!({"type": kind}), limit)
}
pub(crate) fn audio(pcm: &[u8], limits: &SttLimits) -> Result<Vec<Vec<u8>>, Error> {
    if pcm.is_empty() || !pcm.len().is_multiple_of(2) || pcm.len() > limits.chunk_bytes {
        return Err(Error::Invalid("PCM chunk"));
    }
    Ok(vec![payload(
        pcm,
        "BINARY",
        limits.duplex.max_input_frame_bytes,
    )?])
}
pub(crate) fn end(limits: &SttLimits) -> Result<Vec<u8>, Error> {
    control("CloseStream", limits.duplex.max_input_frame_bytes)
}
/// Bound nesting before serde constructs any tree. Strings and escapes are ignored.
pub(crate) fn check_json_depth(raw: &[u8], maximum: usize) -> Result<(), Error> {
    let (mut depth, mut string, mut escape) = (0usize, false, false);
    for &b in raw {
        if string {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                string = false;
            }
        } else {
            match b {
                b'"' => string = true,
                b'{' | b'[' => {
                    depth += 1;
                    if depth > maximum {
                        return Err(Error::TooLarge);
                    }
                }
                b'}' | b']' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    Ok(())
}
fn python_space(c: char) -> bool {
    matches!(c, '\u{9}'..='\u{d}' | '\u{1c}'..='\u{20}' | '\u{85}' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}')
}
/// Pending confirmation is a boolean, not a request/transcript FIFO. Empty and
/// interim results never consume it. Caller serializes state with VAD admission.
pub(crate) fn response(
    raw: &[u8],
    settings: &TranscriptionSettings,
    limits: &SttLimits,
    awaiting_finalize: &mut bool,
) -> Result<Option<FrameData>, Error> {
    let event = eventstream::decode(raw, limits.duplex.max_response_frame_bytes)
        .map_err(|_| Error::Envelope)?;
    if matches!(event.header(":message-type"), Some("exception" | "error")) {
        return Err(Error::Provider);
    }
    if event.header(":message-type") != Some("event")
        || event.header(":event-type") != Some("PayloadPart")
        || event.header(":content-type") != Some("application/octet-stream")
        || event.headers.len() > 6
        || event.headers.iter().any(|(name, value)| {
            !matches!(
                *name,
                ":message-type"
                    | ":event-type"
                    | ":content-type"
                    | "DataType"
                    | "CompletionState"
                    | "P"
            ) || !matches!(value, HeaderValue::String(s) if s.len() <= 256)
        })
        || event.header("DataType").is_some_and(|v| v != "UTF8")
    {
        return Err(Error::Envelope);
    }
    if event.payload.len() > limits.max_result_bytes {
        return Err(Error::TooLarge);
    }
    std::str::from_utf8(event.payload).map_err(|_| Error::Envelope)?;
    check_json_depth(event.payload, limits.max_json_depth)?;
    let value: Value = match serde_json::from_slice(event.payload) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    if value.get("type").and_then(Value::as_str) == Some("Error") {
        return Err(Error::Provider);
    }
    let Some(channel) = value.get("channel") else {
        return Ok(None);
    };
    let Some(first) = channel
        .get("alternatives")
        .and_then(Value::as_array)
        .and_then(|a| a.first())
    else {
        return Ok(None);
    };
    let text = first
        .get("transcript")
        .and_then(Value::as_str)
        .unwrap_or("");
    if text.is_empty() || text.chars().all(python_space) {
        return Ok(None);
    }
    if text.len() > limits.max_transcript_bytes {
        return Err(Error::TooLarge);
    }
    let language = match first.get("languages") {
        None => None,
        Some(Value::Array(values)) if values.is_empty() => None,
        Some(Value::Array(values)) => {
            let lang = values[0]
                .as_str()
                .filter(|s| crate::language::accepted(s))
                .ok_or(Error::Provider)?;
            Some(lang.to_owned())
        }
        _ => return Err(Error::Provider),
    };
    let final_result = value
        .get("is_final")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let confirmed = final_result
        && *awaiting_finalize
        && value.get("from_finalize").and_then(Value::as_bool) == Some(true);
    if confirmed {
        *awaiting_finalize = false;
    }
    let detailed = DetailedTranscriptionFrame {
        text: TextFrame::new(text),
        user_id: settings.user_id.clone(),
        timestamp: chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now()).to_rfc3339(),
        language,
        result: Some(value),
        finalized: confirmed,
    };
    Ok(Some(if final_result {
        FrameData::TranscriptionDetailed(detailed)
    } else {
        FrameData::InterimTranscriptionDetailed(detailed)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn result(text: &str, final_result: bool, confirm: bool, language: Value) -> Value {
        json!({"channel":{"alternatives":[{"transcript":text,"languages":language}]},"is_final":final_result,"from_finalize":confirm})
    }
    #[test]
    fn finalization_truth_table_and_language_are_independent_of_final_variant() {
        let settings = TranscriptionSettings::default();
        let limits = SttLimits::default();
        let mut pending = false;
        for (request, text, final_result, confirm, expected, remaining) in [
            (false, "unsolicited", true, true, false, false),
            (true, "ordinary", true, false, false, true),
            (false, "interim", false, true, false, true),
            (false, " \t\n\u{1c}", true, true, false, true),
            (false, " confirmed ", true, true, true, false),
            (false, "again", true, true, false, false),
            (true, "", true, true, false, true),
            (true, "coalesced", true, true, true, false),
        ] {
            if request {
                pending = true;
            }
            let value = result(text, final_result, confirm, json!(["en-US"]));
            let raw = envelope(&value, 1 << 20).unwrap();
            let data = response(&raw, &settings, &limits, &mut pending).unwrap();
            assert_eq!(pending, remaining);
            if text.is_empty() || text.chars().all(python_space) {
                assert!(data.is_none());
                continue;
            }
            let data = data.unwrap();
            assert_eq!(
                matches!(&data, FrameData::TranscriptionDetailed(_)),
                final_result
            );
            let f = match data {
                FrameData::TranscriptionDetailed(f)
                | FrameData::InterimTranscriptionDetailed(f) => f,
                _ => panic!(),
            };
            assert_eq!(f.finalized, expected);
            assert_eq!(f.text.text, text);
            assert_eq!(f.language.as_deref(), Some("en-US"));
            assert_eq!(f.result, Some(value));
        }
        for language in [
            json!(["not-a-language"]),
            json!([1]),
            json!(null),
            json!("en"),
        ] {
            let raw = envelope(&result("text", true, true, language), 1 << 20).unwrap();
            pending = true;
            assert_eq!(
                response(&raw, &settings, &limits, &mut pending),
                Err(Error::Provider)
            );
            assert!(pending);
        }
        let raw = envelope(&result("text", false, false, json!([])), 1 << 20).unwrap();
        let Some(FrameData::InterimTranscriptionDetailed(f)) =
            response(&raw, &settings, &limits, &mut pending).unwrap()
        else {
            panic!()
        };
        assert_eq!(f.language, None);
    }
    #[test]
    fn raw_pcm_not_base64_and_controls_are_utf8() {
        let limits = SttLimits::default();
        let raw = audio(&[0, 255, 1, 254], &limits).unwrap();
        assert_eq!(raw.len(), 1);
        let decoded = eventstream::decode(&raw[0], 1 << 20).unwrap();
        assert_eq!(decoded.payload, [0, 255, 1, 254]);
        assert_eq!(decoded.header("DataType"), Some("BINARY"));
        for kind in ["KeepAlive", "Finalize", "CloseStream"] {
            let wire = control(kind, 4096).unwrap();
            let decoded = eventstream::decode(&wire, 4096).unwrap();
            assert_eq!(decoded.header("DataType"), Some("UTF8"));
            assert_eq!(
                serde_json::from_slice::<Value>(decoded.payload).unwrap(),
                json!({"type":kind})
            );
            assert_eq!(decoded.headers.len(), 4);
        }
    }
    #[test]
    fn malformed_utf8_wire_and_budget_fail_closed_but_invalid_json_is_ignored() {
        let settings = TranscriptionSettings::default();
        let limits = SttLimits::default();
        let mut pending = false;
        let raw = payload(b"\xff", "UTF8", 4096).unwrap();
        assert_eq!(
            response(&raw, &settings, &limits, &mut pending),
            Err(Error::Envelope)
        );
        let raw = payload(b"not json", "UTF8", 4096).unwrap();
        assert_eq!(response(&raw, &settings, &limits, &mut pending), Ok(None));
        assert_eq!(check_json_depth(b"[[[[]]]]", 3), Err(Error::TooLarge));
        let mut raw = envelope(&json!({}), 4096).unwrap();
        raw[0] ^= 1;
        assert_eq!(
            response(&raw, &settings, &limits, &mut pending),
            Err(Error::Envelope)
        );
    }
}
