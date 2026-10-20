//! Bounded default prompt/response schema, not an arbitrary agent tool protocol.
use crate::{AgentCoreLimits, Error};
use pipecat_core::{ChatRole, LlmContextData, MessageContent, MessagePart, ToolSet};
use serde::{
    de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor},
    Serialize,
};
use std::{fmt, io};

/// Validate the entire ordinary text context, then map only its last user/system
/// message to a fresh bounded `{"prompt": ...}` allocation. Older messages are not
/// replayed and the caller's spare vector/string capacity is never retained.
///
/// Empty context, last assistant, empty scalar text, and empty part arrays are no-ops.
/// A *nonempty* text-part array joins with one ASCII space, including empty parts;
/// even a single empty part therefore produces an invocation with an empty prompt.
/// This strict profile rejects all names, extra metadata, tools, tool choice, other
/// roles and rich/native parts, including those in older messages. Unlike Python's
/// loose dictionaries, unsupported content is not silently discarded.
pub fn default_payload(
    context: &LlmContextData,
    limits: &AgentCoreLimits,
) -> Result<Option<Vec<u8>>, Error> {
    limits.validate()?;
    if context.messages.len() > limits.max_messages {
        return Err(Error::TooLarge);
    }
    if !matches!(&context.tools, ToolSet::NotGiven)
        && !matches!(&context.tools, ToolSet::Functions(tools) if tools.is_empty())
    {
        return Err(Error::Unsupported("AgentCore tools"));
    }
    if context.tool_choice.is_some() {
        return Err(Error::Unsupported("AgentCore tool choice"));
    }
    let mut part_count = 0usize;
    for message in &context.messages {
        if !matches!(
            message.role,
            ChatRole::User | ChatRole::System | ChatRole::Assistant
        ) || message.name.is_some()
            || message.tool_call_id.is_some()
            || !message.tool_calls.is_empty()
            || !message.extra.is_empty()
        {
            return Err(Error::Unsupported("AgentCore ordinary text context"));
        }
        match &message.content {
            MessageContent::Text(text) => check_string(text, limits.max_string_bytes)?,
            MessageContent::Parts(parts) => {
                part_count = part_count.checked_add(parts.len()).ok_or(Error::TooLarge)?;
                if part_count > limits.max_messages {
                    return Err(Error::TooLarge);
                }
                for part in parts {
                    match part {
                        MessagePart::Text { text } => check_string(text, limits.max_string_bytes)?,
                        _ => return Err(Error::Unsupported("AgentCore rich or native content")),
                    }
                }
            }
        }
    }
    // Include serialized structural overhead, not just the visible prompt's bytes.
    serialized_len(context, limits.max_context_bytes)?;
    let Some(last) = context.messages.last() else {
        return Ok(None);
    };
    if !matches!(last.role, ChatRole::User | ChatRole::System) {
        return Ok(None);
    }
    let joined;
    let prompt = match &last.content {
        MessageContent::Text(text) if text.is_empty() => return Ok(None),
        MessageContent::Text(text) => text.as_str(),
        MessageContent::Parts(parts) if parts.is_empty() => return Ok(None),
        MessageContent::Parts(parts) => {
            let mut bytes = parts.len() - 1;
            for part in parts {
                let MessagePart::Text { text } = part else {
                    return Err(Error::Unsupported("AgentCore rich or native content"));
                };
                bytes = bytes.checked_add(text.len()).ok_or(Error::TooLarge)?;
            }
            if bytes > limits.max_string_bytes {
                return Err(Error::TooLarge);
            }
            let mut text = String::with_capacity(bytes);
            for (index, part) in parts.iter().enumerate() {
                let MessagePart::Text { text: part } = part else {
                    return Err(Error::Unsupported("AgentCore rich or native content"));
                };
                if index != 0 {
                    text.push(' ');
                }
                text.push_str(part);
            }
            joined = text;
            joined.as_str()
        }
    };
    #[derive(Serialize)]
    struct Prompt<'a> {
        prompt: &'a str,
    }
    let payload = Prompt { prompt };
    let len = serialized_len(
        &payload,
        limits.max_context_bytes.min(limits.max_pending_bytes),
    )?;
    let mut bytes = Vec::with_capacity(len);
    serde_json::to_writer(&mut bytes, &payload)
        .map_err(|_| Error::Invalid("AgentCore prompt encoding"))?;
    Ok(Some(bytes))
}

fn check_string(text: &str, limit: usize) -> Result<(), Error> {
    if text.len() > limit {
        Err(Error::TooLarge)
    } else {
        Ok(())
    }
}

fn serialized_len<T: Serialize>(value: &T, limit: usize) -> Result<usize, Error> {
    struct Count {
        len: usize,
        limit: usize,
    }
    impl io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let len = self
                .len
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| io::Error::other("bounded context"))?;
            self.len = len;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count { len: 0, limit };
    serde_json::to_writer(&mut count, value).map_err(|_| Error::TooLarge)?;
    Ok(count.len)
}

#[derive(Clone, Copy)]
enum Mode {
    Json,
    Sse,
}

/// JSON is parsed only at actual body EOF. SSE is deliberately the source's
/// independent-JSON-data-line dialect, not standard multiline SSE dispatch.
///
/// LF and CRLF are accepted; line bounds include a possible CR, but not LF. Exactly
/// one initial UTF-8 BOM is accepted for SSE, not JSON; a BOM starting a later line
/// is rejected. `data:` is case sensitive, and one optional ASCII space is removed.
/// JSON's own leading/trailing whitespace remains legal. All lines, including
/// ignored comments, blank and non-data fields, must be valid UTF-8 and bounded.
/// A valid final unterminated data line is parsed at EOF. `[DONE]` is not special.
/// MIME tokens are ASCII case-insensitive, with optional surrounding SP/HTAB and at
/// most one `charset=utf-8` parameter (an optionally quoted value is supported).
/// Unknown parameters, duplicate parameters and non-ASCII/control bytes fail.
pub(crate) struct Decoder {
    mode: Mode,
    limits: AgentCoreLimits,
    buffer: Vec<u8>,
    body_bytes: usize,
    output_bytes: usize,
    first_line: bool,
    closed: bool,
    deferred_error: Option<Error>,
}
impl Decoder {
    pub(crate) fn new(content_type: &str, limits: &AgentCoreLimits) -> Result<Self, Error> {
        limits.validate()?;
        let mode = parse_mime(content_type)?;
        Ok(Self {
            mode,
            limits: limits.clone(),
            buffer: Vec::new(),
            body_bytes: 0,
            output_bytes: 0,
            first_line: true,
            closed: false,
            deferred_error: None,
        })
    }

    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Result<Vec<String>, Error> {
        if self.closed {
            return Err(self
                .deferred_error
                .clone()
                .unwrap_or(Error::Protocol("AgentCore response decoder is closed")));
        }
        let result = self.feed_open(bytes);
        if result.is_err() {
            self.closed = true;
            self.buffer.clear();
        }
        result
    }

    // Accepted lines must not disappear merely because the same transport chunk
    // also contains a later bad line. The transport emits this prefix, then checks
    // the deferred failure before requesting another HTTP chunk.
    pub(crate) fn check_error(&self) -> Result<(), Error> {
        self.deferred_error.clone().map_or(Ok(()), Err)
    }
    fn fail_after_prefix(
        &mut self,
        error: Error,
        output: Vec<String>,
    ) -> Result<Vec<String>, Error> {
        self.closed = true;
        self.buffer.clear();
        if output.is_empty() {
            return Err(error);
        }
        self.deferred_error = Some(error);
        Ok(output)
    }
    fn feed_open(&mut self, bytes: &[u8]) -> Result<Vec<String>, Error> {
        let allowed = bytes.len().min(
            self.limits
                .max_response_bytes
                .saturating_sub(self.body_bytes),
        );
        let oversized = allowed < bytes.len();
        self.body_bytes += allowed;
        let bytes = &bytes[..allowed];
        match self.mode {
            Mode::Json => {
                append_bounded(&mut self.buffer, bytes, self.limits.max_response_bytes)?;
                if oversized {
                    return Err(Error::TooLarge);
                }
                Ok(Vec::new())
            }
            Mode::Sse => {
                let mut output = Vec::new();
                let mut remaining = bytes;
                while let Some(newline) = remaining.iter().position(|b| *b == b'\n') {
                    if let Err(error) = append_bounded(
                        &mut self.buffer,
                        &remaining[..newline],
                        self.limits.max_line_bytes,
                    ) {
                        return self.fail_after_prefix(error, output);
                    }
                    match self.line() {
                        Ok(Some(text)) => output.push(text),
                        Ok(None) => {}
                        Err(error) => return self.fail_after_prefix(error, output),
                    }
                    self.buffer.clear();
                    remaining = &remaining[newline + 1..];
                }
                if let Err(error) =
                    append_bounded(&mut self.buffer, remaining, self.limits.max_line_bytes)
                {
                    return self.fail_after_prefix(error, output);
                }
                if oversized {
                    return self.fail_after_prefix(Error::TooLarge, output);
                }
                Ok(output)
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<Vec<String>, Error> {
        if self.closed {
            return Err(self
                .deferred_error
                .clone()
                .unwrap_or(Error::Protocol("AgentCore response decoder is closed")));
        }
        self.closed = true;
        let result = match self.mode {
            Mode::Json => parse_response(&self.buffer, &self.limits)
                .and_then(|text| self.account_output(text)),
            Mode::Sse if self.buffer.is_empty() => Ok(None),
            Mode::Sse => self.line(),
        };
        self.buffer.clear();
        result.map(|text| text.into_iter().collect())
    }

    fn line(&mut self) -> Result<Option<String>, Error> {
        let line = self.buffer.strip_suffix(b"\r").unwrap_or(&self.buffer);
        let mut line =
            std::str::from_utf8(line).map_err(|_| Error::Protocol("AgentCore response UTF-8"))?;
        if self.first_line {
            self.first_line = false;
            line = line.strip_prefix('\u{feff}').unwrap_or(line);
        }
        if line.starts_with('\u{feff}') {
            return Err(Error::Protocol("AgentCore misplaced SSE BOM"));
        }
        let Some(data) = line.strip_prefix("data:") else {
            return Ok(None);
        };
        let data = data.strip_prefix(' ').unwrap_or(data);
        let text = parse_response(data.as_bytes(), &self.limits)?;
        self.account_output(text)
    }

    fn account_output(&mut self, text: Option<String>) -> Result<Option<String>, Error> {
        if let Some(text) = &text {
            self.output_bytes = self
                .output_bytes
                .checked_add(text.len())
                .ok_or(Error::TooLarge)?;
            if self.output_bytes > self.limits.max_output_bytes {
                return Err(Error::TooLarge);
            }
        }
        Ok(text)
    }
}

fn append_bounded(buffer: &mut Vec<u8>, bytes: &[u8], limit: usize) -> Result<(), Error> {
    let length = buffer
        .len()
        .checked_add(bytes.len())
        .ok_or(Error::TooLarge)?;
    if length > limit {
        return Err(Error::TooLarge);
    }
    // Geometric growth avoids per-byte reallocation while never retaining capacity
    // above the configured buffer bound (unlike Vec's unconstrained growth policy).
    if length > buffer.capacity() {
        let capacity = length
            .max(buffer.capacity().saturating_mul(2).max(64))
            .min(limit);
        buffer.reserve_exact(capacity - buffer.len());
    }
    buffer.extend_from_slice(bytes);
    Ok(())
}

fn parse_mime(value: &str) -> Result<Mode, Error> {
    let bad = || Error::Protocol("AgentCore response content type");
    if value.is_empty()
        || value.len() > 256
        || value
            .bytes()
            .any(|b| !b.is_ascii() || (b.is_ascii_control() && b != b'\t'))
    {
        return Err(bad());
    }
    let trim = |s: &str| s.trim_matches([' ', '\t']).to_owned();
    let mut pieces = value.split(';');
    let media = trim(pieces.next().unwrap_or_default());
    let mode = if media.eq_ignore_ascii_case("application/json") {
        Mode::Json
    } else if media.eq_ignore_ascii_case("text/event-stream") {
        Mode::Sse
    } else {
        return Err(bad());
    };
    if let Some(parameter) = pieces.next() {
        let (name, value) = parameter.split_once('=').ok_or_else(bad)?;
        let value = trim(value);
        let value = value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .unwrap_or(&value);
        if !trim(name).eq_ignore_ascii_case("charset") || !value.eq_ignore_ascii_case("utf-8") {
            return Err(bad());
        }
    }
    if pieces.next().is_some() {
        return Err(bad());
    }
    Ok(mode)
}

fn parse_response(bytes: &[u8], limits: &AgentCoreLimits) -> Result<Option<String>, Error> {
    std::str::from_utf8(bytes).map_err(|_| Error::Protocol("AgentCore response UTF-8"))?;
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let response = ResponseSeed {
        max_string: limits.max_string_bytes,
    }
    .deserialize(&mut deserializer)
    .map_err(|_| Error::Protocol("AgentCore response JSON"))?;
    deserializer
        .end()
        .map_err(|_| Error::Protocol("AgentCore trailing response data"))?;
    Ok(response.filter(|text| !text.is_empty()))
}

struct ResponseSeed {
    max_string: usize,
}
impl<'de> DeserializeSeed<'de> for ResponseSeed {
    type Value = Option<String>;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for ResponseSeed {
    type Value = Option<String>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a default-schema response object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut seen = false;
        let mut response = None;
        while let Some(is_response) = map.next_key_seed(KeySeed {
            max_string: self.max_string,
        })? {
            if is_response {
                if seen {
                    return Err(de::Error::custom("duplicate response field"));
                }
                seen = true;
                response = map.next_value_seed(TextSeed {
                    max_string: self.max_string,
                })?;
            } else {
                map.next_value_seed(BoundedValue {
                    max_string: self.max_string,
                    depth: 0,
                })?;
            }
        }
        Ok(response)
    }
}

struct KeySeed {
    max_string: usize,
}
impl<'de> DeserializeSeed<'de> for KeySeed {
    type Value = bool;
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<bool, D::Error> {
        deserializer.deserialize_str(self)
    }
}
impl Visitor<'_> for KeySeed {
    type Value = bool;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded object key")
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<bool, E> {
        if value.len() > self.max_string {
            return Err(E::custom("response key exceeds bound"));
        }
        Ok(value == "response")
    }
}

struct TextSeed {
    max_string: usize,
}
impl<'de> DeserializeSeed<'de> for TextSeed {
    type Value = Option<String>;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl Visitor<'_> for TextSeed {
    type Value = Option<String>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded string or null")
    }
    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        if value.len() > self.max_string {
            return Err(E::custom("response string exceeds bound"));
        }
        Ok(Some(value.to_owned()))
    }
}

/// Validate unknown members without building or retaining their JSON trees.
#[derive(Clone, Copy)]
struct BoundedValue {
    max_string: usize,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for BoundedValue {
    type Value = IgnoredAny;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        if self.depth > 64 {
            return Err(de::Error::custom("response nesting exceeds bound"));
        }
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for BoundedValue {
    type Value = IgnoredAny;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded ordinary JSON")
    }
    fn visit_unit<E: de::Error>(self) -> Result<IgnoredAny, E> {
        Ok(IgnoredAny)
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> Result<IgnoredAny, E> {
        Ok(IgnoredAny)
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<IgnoredAny, E> {
        Ok(IgnoredAny)
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<IgnoredAny, E> {
        Ok(IgnoredAny)
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<IgnoredAny, E> {
        Ok(IgnoredAny)
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<IgnoredAny, E> {
        if value.len() > self.max_string {
            return Err(E::custom("unknown response string exceeds bound"));
        }
        Ok(IgnoredAny)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<IgnoredAny, A::Error> {
        let child = Self {
            depth: self.depth + 1,
            ..self
        };
        while seq.next_element_seed(child)?.is_some() {}
        Ok(IgnoredAny)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<IgnoredAny, A::Error> {
        let child = Self {
            depth: self.depth + 1,
            ..self
        };
        while map
            .next_key_seed(KeySeed {
                max_string: self.max_string,
            })?
            .is_some()
        {
            map.next_value_seed(child)?;
        }
        Ok(IgnoredAny)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::{ContextMessage, FunctionCall, ToolChoice, ToolDefinition};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn context(role: ChatRole, content: MessageContent) -> LlmContextData {
        LlmContextData {
            messages: vec![ContextMessage {
                role,
                content,
                name: None,
                tool_call_id: None,
                tool_calls: Vec::new(),
                extra: BTreeMap::new(),
            }],
            tools: ToolSet::NotGiven,
            tool_choice: None,
        }
    }
    fn scalar(role: ChatRole, text: &str) -> LlmContextData {
        context(role, MessageContent::Text(text.to_owned()))
    }
    fn parts(texts: &[&str]) -> LlmContextData {
        context(
            ChatRole::User,
            MessageContent::Parts(
                texts
                    .iter()
                    .map(|text| MessagePart::Text {
                        text: (*text).into(),
                    })
                    .collect(),
            ),
        )
    }
    fn payload(context: &LlmContextData) -> Option<Vec<u8>> {
        default_payload(context, &AgentCoreLimits::default()).unwrap()
    }
    fn decode(mime: &str, bytes: &[u8], split: usize) -> Result<Vec<String>, Error> {
        let mut decoder = Decoder::new(mime, &AgentCoreLimits::default())?;
        let mut text = decoder.feed(&bytes[..split])?;
        text.extend(decoder.feed(&bytes[split..])?);
        text.extend(decoder.finish()?);
        Ok(text)
    }

    #[test]
    fn only_last_message_is_selected_and_whitespace_is_preserved() {
        let mut c = scalar(ChatRole::User, "old private prompt");
        c.messages.push(
            scalar(ChatRole::System, "  new\n\t世界  ")
                .messages
                .remove(0),
        );
        assert_eq!(
            payload(&c).unwrap(),
            "{\"prompt\":\"  new\\n\\t世界  \"}".as_bytes()
        );
        c.messages.push(
            scalar(ChatRole::Assistant, "old assistant")
                .messages
                .remove(0),
        );
        assert_eq!(payload(&c), None);
        c.messages.clear();
        assert_eq!(payload(&c), None);
    }

    #[test]
    fn empty_scalars_and_parts_follow_source_distinction() {
        assert_eq!(payload(&scalar(ChatRole::User, "")), None);
        assert_eq!(payload(&scalar(ChatRole::System, "")), None);
        assert_eq!(payload(&parts(&[])), None);
        assert_eq!(payload(&parts(&[""])).unwrap(), br#"{"prompt":""}"#);
        assert_eq!(
            payload(&parts(&["", "a ", "", " b", ""])).unwrap(),
            br#"{"prompt":" a    b "}"#
        );
        assert_eq!(
            payload(&scalar(ChatRole::User, " ")).unwrap(),
            br#"{"prompt":" "}"#
        );
    }

    #[test]
    fn all_history_is_validated_even_when_last_message_would_be_noop() {
        for role in [ChatRole::Developer, ChatRole::Tool] {
            let mut c = scalar(role, "unsupported");
            c.messages
                .push(scalar(ChatRole::Assistant, "").messages.remove(0));
            assert!(default_payload(&c, &AgentCoreLimits::default()).is_err());
        }
        let unsupported = vec![
            MessagePart::Image {
                url: "https://example.invalid".into(),
                detail: None,
            },
            MessagePart::Audio {
                data: "encoded".into(),
                format: "pcm".into(),
            },
            MessagePart::File {
                data: "encoded".into(),
                mime_type: "text/plain".into(),
                filename: None,
            },
            MessagePart::BedrockReasoning {
                text: "private".into(),
                signature: None,
            },
            MessagePart::BedrockRedacted { data: vec![] },
            MessagePart::GeminiText {
                text: "text".into(),
                thought: false,
                signature: None,
                incomplete: false,
            },
            MessagePart::GeminiToolMetadata {
                tool_call_id: "id".into(),
                signature: None,
            },
            MessagePart::RedactedThinking {
                data: "private".into(),
            },
            MessagePart::Thinking {
                text: "private".into(),
                signature: None,
            },
            MessagePart::Refusal {
                text: "refusal".into(),
            },
        ];
        for part in unsupported {
            let mut c = context(ChatRole::User, MessageContent::Parts(vec![part]));
            c.messages
                .push(scalar(ChatRole::Assistant, "").messages.remove(0));
            assert!(default_payload(&c, &AgentCoreLimits::default()).is_err());
        }
    }

    #[test]
    fn tool_authority_and_metadata_are_not_silently_discarded() {
        let limits = AgentCoreLimits::default();
        for tools in [
            ToolSet::Functions(vec![ToolDefinition {
                name: "tool".into(),
                description: "x".into(),
                parameters: json!({}),
            }]),
            ToolSet::Grouped(BTreeMap::new()),
            ToolSet::Direct(vec![]),
        ] {
            let mut c = scalar(ChatRole::User, "x");
            c.tools = tools;
            assert!(default_payload(&c, &limits).is_err());
        }
        let mut c = scalar(ChatRole::User, "x");
        c.tools = ToolSet::Functions(vec![]);
        assert!(default_payload(&c, &limits).is_ok());
        for choice in [
            ToolChoice::None,
            ToolChoice::Auto,
            ToolChoice::Required,
            ToolChoice::Function("tool".into()),
        ] {
            c.tool_choice = Some(choice);
            assert!(default_payload(&c, &limits).is_err());
        }
        c.tool_choice = None;
        c.messages[0].name = Some(String::new());
        assert!(default_payload(&c, &limits).is_err());
        c.messages[0].name = None;
        c.messages[0].tool_call_id = Some(String::new());
        assert!(default_payload(&c, &limits).is_err());
        c.messages[0].tool_call_id = None;
        c.messages[0]
            .extra
            .insert("native_receipt".into(), json!(null));
        assert!(default_payload(&c, &limits).is_err());
        c.messages[0].extra.clear();
        c.messages[0].tool_calls.push(FunctionCall {
            function_name: "tool".into(),
            tool_call_id: "id".into(),
            arguments: json!({}),
        });
        assert!(default_payload(&c, &limits).is_err());
    }

    #[test]
    fn context_string_parts_and_encoded_structural_bytes_are_bounded() {
        let c = scalar(ChatRole::User, "abcd");
        let limit = AgentCoreLimits {
            max_string_bytes: 3,
            ..Default::default()
        };
        assert!(matches!(default_payload(&c, &limit), Err(Error::TooLarge)));
        let c = parts(&["ab", "cd"]);
        let limit = AgentCoreLimits {
            max_string_bytes: 4,
            ..Default::default()
        };
        assert!(matches!(default_payload(&c, &limit), Err(Error::TooLarge)));
        let limit = AgentCoreLimits {
            max_messages: 1,
            ..Default::default()
        };
        assert!(matches!(default_payload(&c, &limit), Err(Error::TooLarge)));
        let mut c = scalar(ChatRole::User, "x");
        c.messages.push(c.messages[0].clone());
        assert!(matches!(default_payload(&c, &limit), Err(Error::TooLarge)));
        let limit = AgentCoreLimits {
            max_context_bytes: 10,
            ..Default::default()
        };
        assert!(matches!(
            default_payload(&scalar(ChatRole::User, "x"), &limit),
            Err(Error::TooLarge)
        ));
        let limit = AgentCoreLimits {
            max_pending_bytes: 13,
            ..Default::default()
        };
        assert!(matches!(
            default_payload(&scalar(ChatRole::User, "\u{0}"), &limit),
            Err(Error::TooLarge)
        ));
    }

    #[test]
    fn payload_normalizes_spare_allocation_capacity() {
        let mut text = String::with_capacity(1_000_000);
        text.push_str("small");
        let mut c = context(ChatRole::User, MessageContent::Text(text));
        c.messages.reserve_exact(10_000);
        let bytes = payload(&c).unwrap();
        assert_eq!(bytes, br#"{"prompt":"small"}"#);
        assert_eq!(bytes.capacity(), bytes.len());
    }

    #[test]
    fn json_waits_for_actual_eof_across_every_split_boundary() {
        let body =
            " \n{\"unknown\":{\"a\":[null,true,-2,3.4,\"ok\"]},\"response\":\"你好 🦀\\ntext\"}\t";
        for split in 0..=body.len() {
            let mut d = Decoder::new("application/json", &AgentCoreLimits::default()).unwrap();
            assert!(d.feed(&body.as_bytes()[..split]).unwrap().is_empty());
            assert!(d.feed(&body.as_bytes()[split..]).unwrap().is_empty());
            assert_eq!(d.finish().unwrap(), vec!["你好 🦀\ntext"]);
        }
    }

    #[test]
    fn sse_independent_lines_preserve_order_across_every_boundary() {
        let body = "\u{feff}: comment\r\nevent: ignored\nid: ignored\nretry: ignored\n\ndata:{\"response\":\"你好\"}\r\ndata: {\"response\":\" 🦀 \"}\ndata: {\"response\":null}\ndata: {}\ndata: {\"response\":\"\"}\ndata: {\"response\":\"last\"}";
        for split in 0..=body.len() {
            assert_eq!(
                decode("text/event-stream", body.as_bytes(), split).unwrap(),
                vec!["你好", " 🦀 ", "last"]
            );
        }
        let mut d = Decoder::new("text/event-stream", &AgentCoreLimits::default()).unwrap();
        let mut output = Vec::new();
        for byte in body.as_bytes() {
            output.extend(d.feed(&[*byte]).unwrap());
        }
        output.extend(d.finish().unwrap());
        assert_eq!(output, vec!["你好", " 🦀 ", "last"]);
    }

    #[test]
    fn adjacent_data_lines_do_not_require_record_delimiters() {
        let mut d = Decoder::new("text/event-stream", &AgentCoreLimits::default()).unwrap();
        assert_eq!(
            d.feed(b"data: {\"response\":\"a\"}\ndata: {\"response\":\"b\"}\n")
                .unwrap(),
            vec!["a", "b"]
        );
        assert!(d.finish().unwrap().is_empty());
        for bytes in [
            b"data: {\n".as_slice(),
            b"data: [DONE]\n",
            b"data:\n",
            b"data: null\n",
        ] {
            assert!(decode("text/event-stream", bytes, 0).is_err());
        }
    }

    #[test]
    fn missing_null_and_empty_response_are_no_output() {
        for body in [
            b"{}".as_slice(),
            br#"{"response":null}"#,
            br#"{"response":""}"#,
            br#"{"unknown": [1, true, {}, []]}"#,
        ] {
            assert!(decode("application/json", body, 0).unwrap().is_empty());
        }
        assert!(decode("text/event-stream", b"", 0).unwrap().is_empty());
        assert!(decode(
            "text/event-stream",
            b": comment\r\n\ndataignored: anything\n",
            0
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn malformed_known_fields_duplicates_and_trailing_values_fail() {
        for body in [
            br#"{"response":1}"#.as_slice(),
            br#"{"response":true}"#,
            br#"{"response":[]}"#,
            br#"{"response":{}}"#,
            br#"{"response":"a","response":"b"}"#,
            br#"{"response":null,"response":null}"#,
            br#"{"response":"a","\u0072esponse":"b"}"#,
            br#"{"response":"a"}{}"#,
            br#"{"response":"a"} trailing"#,
            br#"{"response":"partial""#,
            br#"{"response":"\ud800"}"#,
            b"",
            b"[]",
            b"null",
            b"\xef\xbb\xbf{}",
        ] {
            assert!(
                decode("application/json", body, body.len() / 2).is_err(),
                "{body:?}"
            );
        }
    }

    #[test]
    fn invalid_utf8_also_fails_on_ignored_lines_and_final_fragments() {
        for (mime, bytes) in [
            ("application/json", b"{\"response\":\"\xff\"}".as_slice()),
            ("text/event-stream", b": bad\xff\n"),
            ("text/event-stream", b": incomplete\xe4\xbd"),
            ("text/event-stream", b"data: {\"response\":\"\xe4\xbd"),
            ("text/event-stream", b"\n\xef\xbb\xbfdata: {}\n"),
            ("text/event-stream", b"\xef\xbb\xbf\xef\xbb\xbfdata: {}\n"),
        ] {
            for split in 0..=bytes.len() {
                assert!(decode(mime, bytes, split).is_err());
            }
        }
    }

    #[test]
    fn mime_is_normalized_without_substring_or_parameter_confusion() {
        for mime in [
            "application/json",
            " APPLICATION/JSON \t",
            "text/event-stream; charset=utf-8",
            "Text/Event-Stream ; CHARSET = \"UTF-8\"",
        ] {
            assert!(
                Decoder::new(mime, &AgentCoreLimits::default()).is_ok(),
                "{mime}"
            );
        }
        for mime in [
            "",
            "text/plain",
            "xapplication/json",
            "application/json-evil",
            "application/json;",
            "application/json;charset=latin1",
            "application/json;boundary=x",
            "application/json;charset=utf-8;charset=utf-8",
            "application/json\r\nx: y",
            "application/json; charset=\"utf-8",
            "application/json; charset=utf-8\"",
            "application/json, text/event-stream",
            "têxt/event-stream",
        ] {
            assert!(
                Decoder::new(mime, &AgentCoreLimits::default()).is_err(),
                "{mime}"
            );
        }
        assert!(Decoder::new(
            &format!("application/json{}", " ".repeat(256)),
            &AgentCoreLimits::default()
        )
        .is_err());
    }

    #[test]
    fn body_line_output_and_string_limits_are_independent() {
        let mut d = Decoder::new(
            "application/json",
            &AgentCoreLimits {
                max_response_bytes: 2,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(d.feed(b"{}").unwrap().is_empty());
        assert!(matches!(d.feed(b" "), Err(Error::TooLarge)));
        let mut d = Decoder::new(
            "text/event-stream",
            &AgentCoreLimits {
                max_line_bytes: 3,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(d.feed(b":ab\n").unwrap().is_empty());
        assert!(matches!(d.feed(b":abc\n"), Err(Error::TooLarge)));
        let mut d = Decoder::new(
            "text/event-stream",
            &AgentCoreLimits {
                max_output_bytes: 3,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(d.feed(b"data:{\"response\":\"ab\"}\n").unwrap(), vec!["ab"]);
        assert!(matches!(
            d.feed(b"data:{\"response\":\"cd\"}\n"),
            Err(Error::TooLarge)
        ));
        for body in [
            br#"{"response":"123456789"}"#.as_slice(),
            br#"{"unknown":"123456789"}"#,
            br#"{"123456789":0}"#,
            br#"{"unknown":{"key":["123456789"]}}"#,
        ] {
            let mut d = Decoder::new(
                "application/json",
                &AgentCoreLimits {
                    max_string_bytes: 8,
                    ..Default::default()
                },
            )
            .unwrap();
            d.feed(body).unwrap();
            assert!(d.finish().is_err());
        }
    }

    #[test]
    fn parser_depth_buffer_capacity_and_terminal_state_are_bounded() {
        let body = format!("{{\"unknown\":{}null{}}}", "[".repeat(66), "]".repeat(66));
        assert!(decode("application/json", body.as_bytes(), 0).is_err());
        let mut d = Decoder::new(
            "text/event-stream",
            &AgentCoreLimits {
                max_line_bytes: 70,
                ..Default::default()
            },
        )
        .unwrap();
        for _ in 0..70 {
            d.feed(b":").unwrap();
            assert!(d.buffer.capacity() <= 70);
        }
        assert!(d.feed(b"x").is_err());
        assert!(d.feed(b"\n").is_err());
        assert!(d.finish().is_err());
        let mut d = Decoder::new("application/json", &AgentCoreLimits::default()).unwrap();
        d.feed(b"{}").unwrap();
        d.finish().unwrap();
        assert!(d.finish().is_err());
        assert!(d.feed(b"{}").is_err());
    }

    #[test]
    fn response_errors_do_not_contain_prompt_or_provider_text() {
        let mut d = Decoder::new("application/json", &AgentCoreLimits::default()).unwrap();
        d.feed(br#"{"response":"PRIVATE_PROVIDER_DATA","response":42}"#)
            .unwrap();
        let error = d.finish().unwrap_err();
        assert!(!format!("{error:?}").contains("PRIVATE_PROVIDER_DATA"));
    }
    #[test]
    fn accepted_sse_prefix_survives_later_failure_in_same_chunk() {
        let first = b"data:{\"response\":\"ok\"}\n";
        for tail in [
            b"data:[DONE]\n".as_slice(),
            b":bad\xff\n",
            b"data:{\"response\":7}\n",
        ] {
            let all = [first.as_slice(), tail].concat();
            for split in 0..=all.len() {
                let mut decoder =
                    Decoder::new("text/event-stream", &AgentCoreLimits::default()).unwrap();
                let mut output = Vec::new();
                let mut failed = false;
                for chunk in [&all[..split], &all[split..]] {
                    match decoder.feed(chunk) {
                        Ok(text) => output.extend(text),
                        Err(_) => {
                            failed = true;
                            break;
                        }
                    }
                    if decoder.check_error().is_err() {
                        failed = true;
                        break;
                    }
                }
                assert!(failed);
                assert_eq!(output, ["ok"]);
            }
        }
    }
}
