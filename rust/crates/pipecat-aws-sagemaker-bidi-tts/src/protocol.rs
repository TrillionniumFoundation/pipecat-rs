use crate::{unicode15, Error, SynthesisSettings, TtsLimits};
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_aws_common::eventstream::{self, HeaderValue};
use serde_json::{json, Value};

pub(crate) fn normalized<'a>(text: &'a str, limits: &TtsLimits) -> Result<Option<&'a str>, Error> {
    if text.len() > limits.max_text_bytes {
        return Err(Error::TooLarge);
    }
    let text = text.trim_matches(unicode15::is_space);
    Ok((!text.is_empty() && text.chars().any(unicode15::is_alnum)).then_some(text))
}
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
pub(crate) fn session(settings: &SynthesisSettings, limit: usize) -> Result<Vec<u8>, Error> {
    envelope(
        &json!({"type":"synthesize_session.update","session":{
        "input_text_synthesis":{"voice_name":settings.voice_name,"language_code":settings.language_code},
        "output_audio_params":{"sample_rate_hz":settings.sample_rate_hz}}}),
        limit,
    )
}
pub(crate) fn startup(
    text: &str,
    settings: &SynthesisSettings,
    limits: &TtsLimits,
) -> Result<Option<Vec<Vec<u8>>>, Error> {
    let Some(text) = normalized(text, limits)? else {
        return Ok(None);
    };
    let limit = limits.duplex.max_input_frame_bytes;
    let frames = vec![
        session(settings, limit)?,
        envelope(&json!({"type":"input_text.append","text":text}), limit)?,
        envelope(&json!({"type":"input_text.commit"}), limit)?,
    ];
    if frames.len() > limits.duplex.startup_max_events
        || frames.iter().map(Vec::len).sum::<usize>() > limits.duplex.startup_max_bytes
    {
        return Err(Error::TooLarge);
    }
    Ok(Some(frames))
}
pub(crate) fn control(kind: &str, limits: &TtsLimits) -> Result<Vec<u8>, Error> {
    envelope(&json!({"type":kind}), limits.duplex.max_input_frame_bytes)
}
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
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Response {
    Audio(Vec<u8>),
    Completed,
    Ignored,
}
pub(crate) fn response(raw: &[u8], limits: &TtsLimits) -> Result<Response, Error> {
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
        || event
            .header("DataType")
            .is_some_and(|v| !matches!(v, "UTF8" | "BINARY"))
    {
        return Err(Error::Envelope);
    }
    if event.payload.len() > limits.max_result_bytes {
        return Err(Error::TooLarge);
    }
    if event.payload.is_empty() {
        return Ok(Response::Ignored);
    }
    check_json_depth(event.payload, limits.max_json_depth)?;
    let value: Value = match serde_json::from_slice(event.payload) {
        Ok(value) => value,
        Err(_) => return Ok(Response::Audio(event.payload.to_vec())),
    };
    let object = value.as_object().ok_or(Error::Envelope)?;
    let kind = match object.get("type") {
        None => return Ok(Response::Ignored),
        Some(v) => v.as_str().ok_or(Error::Envelope)?,
    };
    match kind {
        "error" => Err(Error::Provider),
        "conversation.item.speech.completed" => Ok(Response::Completed),
        "conversation.item.speech.data" => {
            let audio = match object.get("audio") {
                None => return Ok(Response::Ignored),
                Some(v) => v.as_str().ok_or(Error::Envelope)?,
            };
            if audio.len().div_ceil(4).saturating_mul(3) > limits.max_result_bytes {
                return Err(Error::TooLarge);
            }
            Ok(Response::Audio(
                STANDARD.decode(audio).map_err(|_| Error::Envelope)?,
            ))
        }
        _ => Ok(Response::Ignored),
    }
}
/// Initial half-second jitter buffer, then whole-sample chunks. Unlike Python,
/// completion flushes a valid short initial tail; interruption drops it.
pub(crate) struct Pcm {
    pending: Vec<u8>,
    threshold: usize,
    started: bool,
    total: usize,
    maximum: usize,
}
impl Pcm {
    pub fn new(rate: u32, maximum: usize) -> Self {
        Self {
            pending: Vec::new(),
            threshold: rate as usize,
            started: false,
            total: 0,
            maximum,
        }
    }
    pub fn feed(&mut self, bytes: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.total = self.total.checked_add(bytes.len()).ok_or(Error::TooLarge)?;
        if self.total > self.maximum {
            return Err(Error::TooLarge);
        }
        self.pending.extend_from_slice(bytes);
        if !self.started && self.pending.len() < self.threshold {
            return Ok(None);
        }
        let even = self.pending.len() & !1;
        if even == 0 {
            return Ok(None);
        }
        self.started = true;
        let tail = self.pending.split_off(even);
        Ok(Some(std::mem::replace(&mut self.pending, tail)))
    }
    pub fn finish(&mut self) -> Result<Option<Vec<u8>>, Error> {
        if !self.pending.len().is_multiple_of(2) {
            return Err(Error::Envelope);
        }
        Ok((!self.pending.is_empty()).then(|| std::mem::take(&mut self.pending)))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn text_wire_and_unicode_gates() {
        let l = TtsLimits::default();
        let s = SynthesisSettings::default();
        for text in ["", "\u{1c} \u{3000}", "!? 🎉"] {
            assert!(startup(text, &s, &l).unwrap().is_none());
        }
        for text in ["中", "²", "𞓐", "hello"] {
            assert!(normalized(text, &l).unwrap().is_some());
        }
        let frames = startup(" \u{1c}Hello  ", &s, &l).unwrap().unwrap();
        let values: Vec<Value> = frames
            .iter()
            .map(|f| {
                serde_json::from_slice(eventstream::decode(f, 1 << 20).unwrap().payload).unwrap()
            })
            .collect();
        assert_eq!(
            values[0],
            json!({"type":"synthesize_session.update","session":{"input_text_synthesis":{"voice_name":s.voice_name,"language_code":"en-US"},"output_audio_params":{"sample_rate_hz":24000}}})
        );
        assert_eq!(
            values[1],
            json!({"type":"input_text.append","text":"Hello"})
        );
        assert_eq!(values[2], json!({"type":"input_text.commit"}));
    }
    #[test]
    fn jitter_and_terminal_samples() {
        let mut pcm = Pcm::new(8000, 16000);
        assert_eq!(pcm.feed(&vec![0; 7999]).unwrap(), None);
        assert_eq!(pcm.feed(&[1, 2]).unwrap().unwrap().len(), 8000);
        assert_eq!(pcm.feed(&[3]).unwrap(), Some(vec![2, 3]));
        assert_eq!(pcm.finish().unwrap(), None);
        let mut short = Pcm::new(8000, 4);
        short.feed(&[1, 2]).unwrap();
        assert_eq!(short.finish().unwrap(), Some(vec![1, 2]));
        short.feed(&[3]).unwrap();
        assert_eq!(short.finish(), Err(Error::Envelope));
    }
    #[test]
    fn bounded_json_raw_fallback_and_errors() {
        let l = TtsLimits::default();
        for value in [
            json!(null),
            json!(42),
            json!([]),
            json!({"type":1}),
            json!({"type":"conversation.item.speech.data","audio":"!"}),
        ] {
            assert_eq!(
                response(&envelope(&value, 1 << 20).unwrap(), &l),
                Err(Error::Envelope)
            );
        }
        assert_eq!(
            response(
                &envelope(&json!({"type":"error","message":"SECRET"}), 1 << 20).unwrap(),
                &l
            ),
            Err(Error::Provider)
        );
        let raw = eventstream::encode(
            &[
                (":message-type", "event"),
                (":event-type", "PayloadPart"),
                (":content-type", "application/octet-stream"),
                ("DataType", "BINARY"),
            ],
            &[255, 0],
            1 << 20,
        )
        .unwrap();
        assert_eq!(response(&raw, &l).unwrap(), Response::Audio(vec![255, 0]));
        let mut bad = raw;
        bad[12] ^= 1;
        assert_eq!(response(&bad, &l), Err(Error::Envelope));
        assert_eq!(check_json_depth(b"[[[[]]]]", 3), Err(Error::TooLarge));
    }
}
