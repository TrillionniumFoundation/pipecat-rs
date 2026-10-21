use crate::{Error, SttLimits, TranscriptionSettings};
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_aws_common::eventstream::{self, HeaderValue};
use pipecat_core::{DetailedTranscriptionFrame, FrameData, TextFrame};
use serde_json::{json, Value};

pub(crate) fn envelope(value: &Value, limit: usize) -> Result<Vec<u8>, Error> {
    let raw = serde_json::to_vec(value).map_err(|_| Error::Envelope)?;
    eventstream::encode(
        &[
            (":message-type", "event"),
            (":event-type", "PayloadPart"),
            (":content-type", "application/octet-stream"),
            ("DataType", "UTF8"),
        ],
        &raw,
        limit,
    )
    .map_err(|_| Error::TooLarge)
}
pub(crate) fn session(settings: &TranscriptionSettings, limit: usize) -> Result<Vec<u8>, Error> {
    envelope(
        &json!({"type":"transcription_session.update", "session": {
        "input_audio_format":"pcm16", "input_audio_params":{"sample_rate_hz":settings.sample_rate_hz,"num_channels":1},
        "input_audio_transcription":{"language":settings.language,"model":settings.model},
        "recognition_config":{"enable_automatic_punctuation":true}}}),
        limit,
    )
}
pub(crate) fn audio(pcm: &[u8], limits: &SttLimits) -> Result<Vec<Vec<u8>>, Error> {
    if pcm.is_empty() || !pcm.len().is_multiple_of(2) || pcm.len() > limits.chunk_bytes {
        return Err(Error::Invalid("PCM chunk"));
    }
    Ok(vec![
        envelope(
            &json!({"type":"input_audio_buffer.append","audio":STANDARD.encode(pcm)}),
            limits.duplex.max_input_frame_bytes,
        )?,
        envelope(
            &json!({"type":"input_audio_buffer.commit"}),
            limits.duplex.max_input_frame_bytes,
        )?,
    ])
}
pub(crate) fn end(limits: &SttLimits) -> Result<Vec<u8>, Error> {
    envelope(
        &json!({"type":"session.end"}),
        limits.duplex.max_input_frame_bytes,
    )
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
pub(crate) fn response(
    raw: &[u8],
    settings: &TranscriptionSettings,
    limits: &SttLimits,
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
            ) || !matches!(value, HeaderValue::String(s) if s.len()<=256)
        })
        || event.header("DataType").is_some_and(|v| v != "UTF8")
    {
        return Err(Error::Envelope);
    }
    if event.payload.len() > limits.max_result_bytes {
        return Err(Error::TooLarge);
    }
    check_json_depth(event.payload, limits.max_json_depth)?;
    let value: Value = match serde_json::from_slice(event.payload) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
    if matches!(
        kind,
        "error" | "conversation.item.input_audio_transcription.failed"
    ) {
        return Err(Error::Provider);
    }
    let (text, finalized) = match kind {
        "conversation.item.input_audio_transcription.delta" => (
            value.get("delta").and_then(Value::as_str).unwrap_or(""),
            false,
        ),
        "conversation.item.input_audio_transcription.completed" => (
            value
                .get("transcript")
                .and_then(Value::as_str)
                .unwrap_or(""),
            true,
        ),
        _ => return Ok(None),
    };
    if text.is_empty() || (finalized && text.chars().all(python_space)) {
        return Ok(None);
    }
    if text.len() > limits.max_transcript_bytes {
        return Err(Error::TooLarge);
    }
    let detailed = DetailedTranscriptionFrame {
        text: TextFrame::new(text),
        user_id: settings.user_id.clone(),
        timestamp: chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now()).to_rfc3339(),
        language: Some(settings.language.clone()),
        result: Some(value),
        finalized,
    };
    Ok(Some(if finalized {
        FrameData::TranscriptionDetailed(detailed)
    } else {
        FrameData::InterimTranscriptionDetailed(detailed)
    }))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn request_raw_blob_and_pair() {
        let l = SttLimits::default();
        let s = TranscriptionSettings::default();
        let config = session(&s, l.duplex.max_input_frame_bytes).unwrap();
        let e = eventstream::decode(&config, 1 << 20).unwrap();
        assert_eq!(e.header(":event-type"), Some("PayloadPart"));
        assert_eq!(e.header("DataType"), Some("UTF8"));
        assert_eq!(e.headers.len(), 4);
        let v: Value = serde_json::from_slice(e.payload).unwrap();
        assert_eq!(v["session"]["input_audio_transcription"]["model"], s.model);
        assert!(v.get("bytes").is_none());
        let pair = audio(&[0, 1, 254, 255], &l).unwrap();
        assert_eq!(pair.len(), 2);
        let a = eventstream::decode(&pair[0], 1 << 20).unwrap();
        let a: Value = serde_json::from_slice(a.payload).unwrap();
        assert_eq!(
            STANDARD.decode(a["audio"].as_str().unwrap()).unwrap(),
            [0, 1, 254, 255]
        );
        let c = eventstream::decode(&pair[1], 1 << 20).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(c.payload).unwrap(),
            json!({"type":"input_audio_buffer.commit"})
        );
    }
    #[test]
    fn detailed_metadata_and_python_gate() {
        let l = SttLimits::default();
        let s = TranscriptionSettings {
            user_id: "speaker".into(),
            ..Default::default()
        };
        for text in ["", " \t\n", "\u{1c}\u{1d}\u{1e}\u{1f}", "\u{3000}"] {
            let raw=envelope(&json!({"type":"conversation.item.input_audio_transcription.completed","transcript":text}),1<<20).unwrap();
            assert!(response(&raw, &s, &l).unwrap().is_none());
        }
        let value = json!({"type":"conversation.item.input_audio_transcription.completed","transcript":"  hello  ","extra":42});
        let raw = envelope(&value, 1 << 20).unwrap();
        let FrameData::TranscriptionDetailed(f) = response(&raw, &s, &l).unwrap().unwrap() else {
            panic!()
        };
        assert_eq!(f.text.text, "  hello  ");
        assert!(f.finalized);
        assert_eq!(f.result, Some(value));
        assert_eq!(f.language.as_deref(), Some("en-US"));
        assert_eq!(f.user_id, "speaker");
    }
    #[test]
    fn failures_and_ignored_json_are_separate() {
        let l = SttLimits::default();
        let s = TranscriptionSettings::default();
        for raw in [b"".as_slice(), b"not-json", b"\xff"] {
            let wire = eventstream::encode(
                &[
                    (":message-type", "event"),
                    (":event-type", "PayloadPart"),
                    (":content-type", "application/octet-stream"),
                ],
                raw,
                1 << 20,
            )
            .unwrap();
            assert!(response(&wire, &s, &l).unwrap().is_none());
        }
        for kind in [
            "error",
            "conversation.item.input_audio_transcription.failed",
        ] {
            let wire = envelope(&json!({"type":kind,"secret":"MUST-NOT-LEAK"}), 1 << 20).unwrap();
            assert_eq!(response(&wire, &s, &l), Err(Error::Provider));
        }
        let mut wire = envelope(&json!({"type":"unknown"}), 1 << 20).unwrap();
        wire[0] ^= 1;
        assert_eq!(response(&wire, &s, &l), Err(Error::Envelope));
        assert_eq!(check_json_depth(b"[[[[]]]]", 3), Err(Error::TooLarge));
        assert!(check_json_depth(br#"{"x":"[[["}"#, 1).is_ok());
    }
}
