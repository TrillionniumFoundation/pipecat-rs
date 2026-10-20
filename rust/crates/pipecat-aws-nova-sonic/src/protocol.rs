//! Strict, bounded Nova Sonic output mapping and the SDK JSON blob envelope.
//!
//! This finite profile accepts sequential content blocks, as the pinned Python
//! service does. Each speculative TEXT block is associated with exactly the
//! next AUDIO block in the same completion. Another speculative block before
//! that audio is unsupported, rather than silently replacing generated text.
//! Provider FINAL text is an observation, never a second history append or a
//! playback receipt. A completion envelope may contain many spoken turns.
//! FINAL blocks are paired FIFO with speculative blocks. A new turn may start
//! only after prior FINAL obligations settle; cross-turn out-of-order FINAL
//! delivery is unsupported because the wire has no per-turn correlation id.
//! Unknown fields within recognized objects are ignored only
//! after the entire object has passed duplicate-key, depth, node and byte limits.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use pipecat_aws_common::eventstream;
use pipecat_core::{
    AggregatedTextFrame, AudioRawFrame, Frame, FrameData, TextFrame, TtsStartedFrame,
    TtsStoppedFrame,
};
use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::{
    collections::{BTreeSet, VecDeque},
    fmt,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProtocolLimits {
    pub max_frame_bytes: usize,
    pub max_event_bytes: usize,
    pub max_json_depth: usize,
    pub max_json_nodes: usize,
    pub max_string_bytes: usize,
    pub max_content_text_bytes: usize,
    pub max_audio_bytes: usize,
    pub max_content_ids: usize,
    pub max_completion_ids: usize,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 512 * 1024,
            max_event_bytes: 256 * 1024,
            max_json_depth: 16,
            max_json_nodes: 4096,
            max_string_bytes: 192 * 1024,
            max_content_text_bytes: 64 * 1024,
            max_audio_bytes: 64 * 1024,
            max_content_ids: 4096,
            max_completion_ids: 512,
        }
    }
}

impl ProtocolLimits {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_frame_bytes < 256
            || self.max_frame_bytes > 16 * 1024 * 1024
            || self.max_event_bytes == 0
            || self.max_event_bytes > self.max_frame_bytes
            || self.max_json_depth == 0
            || self.max_json_depth > 64
            || self.max_json_nodes == 0
            || self.max_json_nodes > 65_536
            || self.max_string_bytes == 0
            || self.max_string_bytes > self.max_event_bytes
            || self.max_content_text_bytes == 0
            || self.max_content_text_bytes > 1024 * 1024
            || self.max_audio_bytes == 0
            || self.max_audio_bytes > 1024 * 1024
            || self.max_content_ids == 0
            || self.max_content_ids > 16_384
            || self.max_completion_ids == 0
            || self.max_completion_ids > 16_384
        {
            return Err(ProtocolError("invalid protocol limits"));
        }
        Ok(())
    }
}

/// Errors never contain provider JSON, text, identifiers, audio or credentials.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProtocolError(&'static str);

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Nova Sonic protocol: {}", self.0)
    }
}

impl std::error::Error for ProtocolError {}

type Result<T> = std::result::Result<T, ProtocolError>;

/// Incremental usage only. The cumulative provider counters are validated but
/// are not emitted as additional usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UsageDelta {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug)]
pub(crate) enum Action {
    /// A complete raw USER FINAL block. The parent retains these actual received
    /// blocks until Boundary (or clean EOF) and obtains real history receipts.
    UserFinal(String),
    /// Commit the actual received user prefix before releasing following output.
    Boundary,
    Output(Box<Frame>),
    /// Broadcast through the actual causal interruption API, including when the
    /// response frames already ended but its output queue is still draining.
    Interrupt,
    Usage(UsageDelta),
    /// Generated response end, not speaker playback or completionEnd receipt.
    ResponseCompleted,
}

impl Action {
    fn output(frame: Frame) -> Self {
        Self::Output(Box::new(frame))
    }
    /// Conservative owned-content accounting for the finite action queue. The
    /// parent must separately bound queue entries and its receipt/text buffers.
    pub(crate) fn retained_bytes(&self) -> usize {
        const OVERHEAD: usize = std::mem::size_of::<Action>() + std::mem::size_of::<Frame>() + 512;
        OVERHEAD.saturating_add(match self {
            Self::UserFinal(text) => text.capacity(),
            Self::Output(frame) => match &frame.data {
                FrameData::LlmText(text) => text.text.capacity(),
                FrameData::TtsText(text) => text.text.text.capacity(),
                FrameData::TtsAudio(audio) => audio.audio.capacity(),
                _ => 0,
            },
            _ => 0,
        })
    }
}

struct JsonBudget<'a> {
    limits: &'a ProtocolLimits,
    nodes: usize,
}

struct JsonSeed<'a, 'b> {
    budget: &'a mut JsonBudget<'b>,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for JsonSeed<'_, '_> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if self.depth > self.budget.limits.max_json_depth
            || self.budget.nodes >= self.budget.limits.max_json_nodes
        {
            return Err(D::Error::custom("JSON budget exceeded"));
        }
        self.budget.nodes += 1;
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for JsonSeed<'_, '_> {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded JSON")
    }

    fn visit_bool<E: serde::de::Error>(self, value: bool) -> std::result::Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<Value, E> {
        Ok(Value::Number(value.into()))
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> std::result::Result<Value, E> {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("nonfinite JSON number"))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Value, E> {
        if value.len() > self.budget.limits.max_string_bytes {
            return Err(E::custom("JSON string budget exceeded"));
        }
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> std::result::Result<Value, E> {
        if value.len() > self.budget.limits.max_string_bytes {
            return Err(E::custom("JSON string budget exceeded"));
        }
        Ok(Value::String(value))
    }

    fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_none<E: serde::de::Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(JsonSeed {
            budget: &mut *self.budget,
            depth: self.depth + 1,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Value, A::Error> {
        let mut values = Map::new();
        while let Some(key) = map.next_key::<String>()? {
            if key.len() > self.budget.limits.max_string_bytes
                || self.budget.nodes >= self.budget.limits.max_json_nodes
            {
                return Err(A::Error::custom("JSON key budget exceeded"));
            }
            self.budget.nodes += 1;
            if values.contains_key(&key) {
                return Err(A::Error::custom("duplicate JSON key"));
            }
            let value = map.next_value_seed(JsonSeed {
                budget: &mut *self.budget,
                depth: self.depth + 1,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

/// Parse before converting to Value: serde_json's ordinary Value deserializer
/// otherwise silently overwrites duplicate object members.
pub(crate) fn strict_json(bytes: &[u8], limits: &ProtocolLimits) -> Result<Value> {
    limits.validate()?;
    if bytes.is_empty() || bytes.len() > limits.max_event_bytes {
        return Err(ProtocolError("JSON byte limit"));
    }
    let mut budget = JsonBudget { limits, nodes: 0 };
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = JsonSeed {
        budget: &mut budget,
        depth: 1,
    }
    .deserialize(&mut deserializer)
    .map_err(|_| ProtocolError("invalid or over-budget JSON"))?;
    deserializer
        .end()
        .map_err(|_| ProtocolError("trailing JSON data"))?;
    Ok(value)
}

fn object(value: &Value) -> Result<&Map<String, Value>> {
    value
        .as_object()
        .ok_or(ProtocolError("expected JSON object"))
}

fn string<'a>(obj: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    obj.get(key)
        .and_then(Value::as_str)
        .ok_or(ProtocolError("missing or invalid string field"))
}

fn number(obj: &Map<String, Value>, key: &str) -> Result<u64> {
    obj.get(key)
        .and_then(Value::as_u64)
        .ok_or(ProtocolError("missing or invalid unsigned field"))
}

fn id<'a>(obj: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    let value = string(obj, key)?;
    validate_id(value)?;
    Ok(value)
}

fn validate_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.:".contains(&c))
    {
        return Err(ProtocolError("invalid identity"));
    }
    Ok(())
}

fn decode_base64(value: &str, max_decoded: usize) -> Result<Vec<u8>> {
    // Check allocation upper bound first; STANDARD rejects bad padding and
    // noncanonical trailing bits, unlike permissive decoders.
    let max_encoded = max_decoded
        .checked_add(2)
        .and_then(|n| n.checked_div(3))
        .and_then(|n| n.checked_mul(4))
        .ok_or(ProtocolError("base64 size overflow"))?;
    if value.len() > max_encoded {
        return Err(ProtocolError("base64 decoded byte limit"));
    }
    let decoded = STANDARD
        .decode(value)
        .map_err(|_| ProtocolError("invalid canonical base64"))?;
    if decoded.len() > max_decoded {
        return Err(ProtocolError("base64 decoded byte limit"));
    }
    Ok(decoded)
}

/// Build a complete unsigned inner EventStream event/chunk frame. SDK blob
/// serialization adds a second base64 layer around the full Nova JSON; audio
/// content inside that JSON already has its own independent base64 layer. The
/// request signer wraps this complete frame, not the JSON payload alone.
pub(crate) fn encode_chunk(nova: &[u8], limits: &ProtocolLimits) -> Result<Vec<u8>> {
    strict_json(nova, limits)?;
    let encoded = STANDARD.encode(nova);
    let payload_len = encoded
        .len()
        .checked_add(12)
        .ok_or(ProtocolError("chunk byte limit"))?;
    if payload_len > limits.max_frame_bytes {
        return Err(ProtocolError("chunk byte limit"));
    }
    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(b"{\"bytes\":\"");
    payload.extend_from_slice(encoded.as_bytes());
    payload.extend_from_slice(b"\"}");
    eventstream::encode(
        &[
            (":message-type", "event"),
            (":event-type", "chunk"),
            (":content-type", "application/json"),
        ],
        &payload,
        limits.max_frame_bytes,
    )
    .map_err(|_| ProtocolError("chunk EventStream byte limit"))
}

pub(crate) fn decode_chunk(payload: &[u8], limits: &ProtocolLimits) -> Result<Vec<u8>> {
    limits.validate()?;
    let envelope_limits = ProtocolLimits {
        max_event_bytes: limits.max_frame_bytes,
        max_string_bytes: limits.max_frame_bytes,
        ..*limits
    };
    let value = strict_json(payload, &envelope_limits)?;
    let obj = object(&value)?;
    if obj.len() != 1 {
        return Err(ProtocolError("invalid SDK chunk object"));
    }
    let bytes = decode_base64(string(obj, "bytes")?, limits.max_event_bytes)?;
    strict_json(&bytes, limits)?;
    Ok(bytes)
}

/// Response EventStream messages are unsigned ordinary event/chunk envelopes,
/// NOT the nested signed request envelope. Framing/CRC is checked first.
pub(crate) fn decode_response(frame: &[u8], limits: &ProtocolLimits) -> Result<Vec<u8>> {
    limits.validate()?;
    let event = eventstream::decode(frame, limits.max_frame_bytes)
        .map_err(|_| ProtocolError("invalid response EventStream frame"))?;
    if event.headers.len() != 3 || event.header(":content-type") != Some("application/json") {
        return Err(ProtocolError("invalid response EventStream headers"));
    }
    match event.header(":message-type") {
        Some("event") if event.header(":event-type") == Some("chunk") => {
            decode_chunk(event.payload, limits)
        }
        Some("exception") => {
            let value = strict_json(event.payload, limits)?;
            let obj = object(&value)?;
            for key in ["message", "originalMessage", "__type"] {
                if obj.contains_key(key) {
                    string(obj, key)?;
                }
            }
            if obj.contains_key("originalStatusCode") {
                number(obj, "originalStatusCode")?;
            }
            let message = match event.header(":exception-type") {
                Some("internalServerException") => "service internalServerException",
                Some("modelStreamErrorException") => "service modelStreamErrorException",
                Some("validationException") => "service validationException",
                Some("throttlingException") => "service throttlingException",
                Some("modelTimeoutException") => "service modelTimeoutException",
                Some("serviceUnavailableException") => "service serviceUnavailableException",
                _ => "unknown service exception",
            };
            Err(ProtocolError(message))
        }
        _ => Err(ProtocolError("unsupported response EventStream union")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentKind {
    UserFinal,
    AssistantSpeculative,
    AssistantFinal,
    AssistantAudio,
}

impl ContentKind {
    fn wire_type(self) -> &'static str {
        if self == Self::AssistantAudio {
            "AUDIO"
        } else {
            "TEXT"
        }
    }
    fn role(self) -> &'static str {
        if self == Self::UserFinal {
            "USER"
        } else {
            "ASSISTANT"
        }
    }
}

#[derive(Debug)]
struct GeneratedText {
    /// Explicit association, retained for lifetime/identity auditing.
    source_content_id: String,
    generation: u64,
    text: String,
}

#[derive(Debug)]
struct FinalObligation {
    source_content_id: String,
    generation: u64,
    audio_completed: bool,
}

#[derive(Debug)]
struct Content {
    id: String,
    kind: ContentKind,
    text: String,
    generated: Option<GeneratedText>,
    generation: u64,
    final_obligation: Option<FinalObligation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseState {
    NotStarted,
    Open,
    Ended,
    Interrupted,
}

#[derive(Debug)]
struct Completion {
    id: String,
    response: ResponseState,
}

/// A mapper is single-session, single-writer and permanently poisoned by a
/// malformed event. A failed session cannot continue from partly admitted data.
pub(crate) struct Mapper {
    prompt_name: String,
    session_id: Option<String>,
    limits: ProtocolLimits,
    completion: Option<Completion>,
    content: Option<Content>,
    pending_text: Option<GeneratedText>,
    pending_finals: VecDeque<FinalObligation>,
    response_generation: u64,
    seen_contents: BTreeSet<String>,
    seen_completions: BTreeSet<String>,
    failed: bool,
}

impl Mapper {
    pub(crate) fn new(prompt_name: String, limits: ProtocolLimits) -> Result<Self> {
        limits.validate()?;
        validate_id(&prompt_name)?;
        Ok(Self {
            prompt_name,
            session_id: None,
            limits,
            completion: None,
            content: None,
            pending_text: None,
            pending_finals: VecDeque::new(),
            response_generation: 0,
            seen_contents: BTreeSet::new(),
            seen_completions: BTreeSet::new(),
            failed: false,
        })
    }

    pub(crate) fn map_nova_json(&mut self, bytes: &[u8]) -> Result<Vec<Action>> {
        if self.failed {
            return Err(ProtocolError("mapper already failed"));
        }
        let result = self.map_inner(bytes);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn map_inner(&mut self, bytes: &[u8]) -> Result<Vec<Action>> {
        let value = strict_json(bytes, &self.limits)?;
        let root = object(&value)?;
        if root.len() != 1 {
            return Err(ProtocolError("invalid Nova event envelope"));
        }
        let event = object(
            root.get("event")
                .ok_or(ProtocolError("missing Nova event"))?,
        )?;
        if event.len() != 1 {
            return Err(ProtocolError("invalid Nova event union"));
        }
        let (name, body) = event
            .iter()
            .next()
            .ok_or(ProtocolError("missing Nova event"))?;
        let body = object(body)?;
        match name.as_str() {
            "completionStart" => self.completion_start(body),
            "contentStart" => self.content_start(body),
            "textOutput" => self.text_output(body),
            "audioOutput" => self.audio_output(body),
            "contentEnd" => self.content_end(body),
            "completionEnd" => self.completion_end(body),
            "usageEvent" => self.usage(body),
            "toolUse" | "toolResult" => Err(ProtocolError("tools unsupported")),
            _ => Err(ProtocolError("unsupported Nova event")),
        }
    }

    fn base_identity(&self, body: &Map<String, Value>) -> Result<()> {
        if id(body, "promptName")? != self.prompt_name {
            return Err(ProtocolError("prompt identity mismatch"));
        }
        let session = id(body, "sessionId")?;
        if self
            .session_id
            .as_deref()
            .is_some_and(|known| known != session)
        {
            return Err(ProtocolError("session identity mismatch"));
        }
        Ok(())
    }

    fn completion_identity(&self, body: &Map<String, Value>) -> Result<()> {
        self.base_identity(body)?;
        let completion = self
            .completion
            .as_ref()
            .ok_or(ProtocolError("no active completion"))?;
        if id(body, "completionId")? != completion.id {
            return Err(ProtocolError("completion identity mismatch"));
        }
        Ok(())
    }

    fn content_identity(&self, body: &Map<String, Value>) -> Result<()> {
        self.completion_identity(body)?;
        let content = self
            .content
            .as_ref()
            .ok_or(ProtocolError("no active content"))?;
        if id(body, "contentId")? != content.id {
            return Err(ProtocolError("content identity mismatch"));
        }
        if content.kind != ContentKind::UserFinal && content.generation != self.response_generation
        {
            return Err(ProtocolError("stale content generation"));
        }
        if body.contains_key("type") && string(body, "type")? != content.kind.wire_type() {
            return Err(ProtocolError("content type mismatch"));
        }
        if body.contains_key("role") && string(body, "role")? != content.kind.role() {
            return Err(ProtocolError("content role mismatch"));
        }
        if body.contains_key("additionalModelFields") {
            let stage = self.generation_stage(body)?;
            if stage
                != match content.kind {
                    ContentKind::UserFinal | ContentKind::AssistantFinal => Some("FINAL"),
                    ContentKind::AssistantSpeculative => Some("SPECULATIVE"),
                    ContentKind::AssistantAudio => None,
                }
            {
                return Err(ProtocolError("content generation stage mismatch"));
            }
        }
        Ok(())
    }

    fn completion_start(&mut self, body: &Map<String, Value>) -> Result<Vec<Action>> {
        self.base_identity(body)?;
        let completion_id = id(body, "completionId")?;
        if self.completion.is_some()
            || self.content.is_some()
            || self.pending_text.is_some()
            || !self.pending_finals.is_empty()
        {
            return Err(ProtocolError("overlapping completion"));
        }
        if self.seen_completions.contains(completion_id) {
            return Err(ProtocolError("reused completion identity"));
        }
        if self.seen_completions.len() >= self.limits.max_completion_ids {
            return Err(ProtocolError("completion identity limit"));
        }
        let session_id = id(body, "sessionId")?.to_owned();
        self.session_id.get_or_insert(session_id);
        self.seen_completions.insert(completion_id.to_owned());
        self.completion = Some(Completion {
            id: completion_id.to_owned(),
            response: ResponseState::NotStarted,
        });
        Ok(Vec::new())
    }

    fn generation_stage(&self, body: &Map<String, Value>) -> Result<Option<&'static str>> {
        let Some(fields) = body.get("additionalModelFields") else {
            return Ok(None);
        };
        let encoded = fields
            .as_str()
            .ok_or(ProtocolError("invalid additional model fields"))?;
        let value = strict_json(encoded.as_bytes(), &self.limits)?;
        let obj = object(&value)?;
        match obj.get("generationStage") {
            None => Ok(None),
            Some(Value::String(stage)) if stage == "FINAL" => Ok(Some("FINAL")),
            Some(Value::String(stage)) if stage == "SPECULATIVE" => Ok(Some("SPECULATIVE")),
            _ => Err(ProtocolError("invalid generation stage")),
        }
    }

    fn content_start(&mut self, body: &Map<String, Value>) -> Result<Vec<Action>> {
        self.completion_identity(body)?;
        let content_id = id(body, "contentId")?;
        if self.content.is_some() {
            return Err(ProtocolError("overlapping content unsupported"));
        }
        if self.seen_contents.contains(content_id) {
            return Err(ProtocolError("reused content identity"));
        }
        if self.seen_contents.len() >= self.limits.max_content_ids {
            return Err(ProtocolError("content identity limit"));
        }
        let typ = string(body, "type")?;
        let role = string(body, "role")?;
        if typ == "TOOL" || role == "TOOL" || body.contains_key("toolUseOutputConfiguration") {
            return Err(ProtocolError("tools unsupported"));
        }
        let stage = self.generation_stage(body)?;
        let kind = match (typ, role, stage) {
            ("TEXT", "USER", Some("FINAL")) => ContentKind::UserFinal,
            ("TEXT", "ASSISTANT", Some("SPECULATIVE")) => ContentKind::AssistantSpeculative,
            ("TEXT", "ASSISTANT", Some("FINAL")) => ContentKind::AssistantFinal,
            ("AUDIO", "ASSISTANT", None) => ContentKind::AssistantAudio,
            _ => return Err(ProtocolError("unsupported content type, role or stage")),
        };
        if kind == ContentKind::AssistantAudio {
            if body.contains_key("textOutputConfiguration") {
                return Err(ProtocolError("conflicting content configuration"));
            }
            let config = object(
                body.get("audioOutputConfiguration")
                    .ok_or(ProtocolError("missing audio configuration"))?,
            )?;
            if string(config, "mediaType")? != "audio/lpcm"
                || string(config, "encoding")? != "base64"
                || number(config, "sampleRateHertz")? != 24_000
                || number(config, "sampleSizeBits")? != 16
                || number(config, "channelCount")? != 1
            {
                return Err(ProtocolError("unsupported output PCM format"));
            }
            if config.contains_key("audioType") && string(config, "audioType")? != "SPEECH" {
                return Err(ProtocolError("unsupported audio type"));
            }
        } else {
            if body.contains_key("audioOutputConfiguration") {
                return Err(ProtocolError("conflicting content configuration"));
            }
            let config = object(
                body.get("textOutputConfiguration")
                    .ok_or(ProtocolError("missing text configuration"))?,
            )?;
            if string(config, "mediaType")? != "text/plain" {
                return Err(ProtocolError("unsupported text format"));
            }
        }
        let response = self
            .completion
            .as_ref()
            .ok_or(ProtocolError("no active completion"))?
            .response;
        match kind {
            ContentKind::AssistantSpeculative => {
                if self.pending_text.is_some() {
                    return Err(ProtocolError("ambiguous speculative audio association"));
                }
                if response != ResponseState::Open && !self.pending_finals.is_empty() {
                    return Err(ProtocolError(
                        "new response before prior FINAL association settled",
                    ));
                }
                if self.pending_finals.len() >= self.limits.max_content_ids {
                    return Err(ProtocolError("pending FINAL association limit"));
                }
            }
            ContentKind::AssistantAudio => {
                if response != ResponseState::Open || self.pending_text.is_none() {
                    return Err(ProtocolError("audio without associated speculative text"));
                }
            }
            ContentKind::AssistantFinal => {
                if response == ResponseState::NotStarted {
                    return Err(ProtocolError("unassociated assistant final text"));
                }
                let obligation = self
                    .pending_finals
                    .front()
                    .ok_or(ProtocolError("unassociated assistant final text"))?;
                if obligation.generation != self.response_generation {
                    return Err(ProtocolError("stale assistant FINAL generation"));
                }
            }
            ContentKind::UserFinal => {}
        }
        let mut actions = Vec::new();
        if matches!(
            kind,
            ContentKind::AssistantSpeculative | ContentKind::AssistantAudio
        ) {
            actions.push(Action::Boundary);
        }
        if kind == ContentKind::AssistantSpeculative && response != ResponseState::Open {
            self.response_generation = self
                .response_generation
                .checked_add(1)
                .ok_or(ProtocolError("response generation overflow"))?;
            actions.push(Action::output(Frame::new(FrameData::LlmResponseStart)));
            actions.push(Action::output(Frame::new(FrameData::TtsStarted(
                TtsStartedFrame::default(),
            ))));
            self.completion
                .as_mut()
                .ok_or(ProtocolError("no active completion"))?
                .response = ResponseState::Open;
        }
        let generated = if kind == ContentKind::AssistantAudio {
            self.pending_text.take()
        } else {
            None
        };
        let final_obligation = if kind == ContentKind::AssistantFinal {
            self.pending_finals.pop_front()
        } else {
            None
        };
        if kind == ContentKind::AssistantSpeculative {
            self.pending_finals.push_back(FinalObligation {
                source_content_id: content_id.to_owned(),
                generation: self.response_generation,
                audio_completed: false,
            });
        }
        self.seen_contents.insert(content_id.to_owned());
        self.content = Some(Content {
            id: content_id.to_owned(),
            kind,
            text: String::new(),
            generated,
            generation: self.response_generation,
            final_obligation,
        });
        Ok(actions)
    }

    fn text_output(&mut self, body: &Map<String, Value>) -> Result<Vec<Action>> {
        self.content_identity(body)?;
        let text = string(body, "content")?;
        let content = self
            .content
            .as_mut()
            .ok_or(ProtocolError("no active content"))?;
        if content.kind == ContentKind::AssistantAudio {
            return Err(ProtocolError("text in audio content"));
        }
        let total = content
            .text
            .len()
            .checked_add(text.len())
            .ok_or(ProtocolError("content text byte limit"))?;
        if total > self.limits.max_content_text_bytes {
            return Err(ProtocolError("content text byte limit"));
        }
        content.text.push_str(text);
        Ok(Vec::new())
    }

    fn audio_output(&mut self, body: &Map<String, Value>) -> Result<Vec<Action>> {
        self.content_identity(body)?;
        if self
            .content
            .as_ref()
            .ok_or(ProtocolError("no active content"))?
            .kind
            != ContentKind::AssistantAudio
        {
            return Err(ProtocolError("audio in non-audio content"));
        }
        if self
            .completion
            .as_ref()
            .ok_or(ProtocolError("no active completion"))?
            .response
            != ResponseState::Open
        {
            return Err(ProtocolError("stale audio output"));
        }
        let audio = decode_base64(string(body, "content")?, self.limits.max_audio_bytes)?;
        if audio.is_empty() || !audio.len().is_multiple_of(2) {
            return Err(ProtocolError("invalid PCM alignment or empty audio"));
        }
        let frame =
            AudioRawFrame::new(audio, 24_000, 1).map_err(|_| ProtocolError("invalid PCM"))?;
        Ok(vec![Action::output(Frame::new(FrameData::TtsAudio(frame)))])
    }

    fn content_end(&mut self, body: &Map<String, Value>) -> Result<Vec<Action>> {
        self.content_identity(body)?;
        let content = self
            .content
            .as_ref()
            .ok_or(ProtocolError("no active content"))?;
        if string(body, "type")? != content.kind.wire_type() {
            return Err(ProtocolError("content end type mismatch"));
        }
        let reason = string(body, "stopReason")?;
        if reason == "TOOL_USE" {
            return Err(ProtocolError("tools unsupported"));
        }
        if !matches!(reason, "PARTIAL_TURN" | "END_TURN" | "INTERRUPTED") {
            return Err(ProtocolError("unsupported content stop reason"));
        }
        if content.kind == ContentKind::UserFinal && reason == "INTERRUPTED" {
            return Err(ProtocolError("interrupted user final unsupported"));
        }
        if content.kind == ContentKind::AssistantFinal {
            let obligation = content
                .final_obligation
                .as_ref()
                .ok_or(ProtocolError("missing FINAL association"))?;
            if obligation.generation != self.response_generation
                || !self.seen_contents.contains(&obligation.source_content_id)
            {
                return Err(ProtocolError("stale FINAL association"));
            }
            if reason != "INTERRUPTED" && !obligation.audio_completed {
                return Err(ProtocolError("FINAL before associated audio completion"));
            }
        }
        let content = self
            .content
            .take()
            .ok_or(ProtocolError("no active content"))?;
        let mut actions = Vec::new();
        match content.kind {
            ContentKind::UserFinal => actions.push(Action::UserFinal(content.text)),
            ContentKind::AssistantSpeculative if reason != "INTERRUPTED" => {
                let mut text = TextFrame::llm(content.text.clone());
                text.append_to_context = false;
                actions.push(Action::output(Frame::new(FrameData::LlmText(text))));
                self.pending_text = Some(GeneratedText {
                    source_content_id: content.id,
                    generation: content.generation,
                    text: content.text,
                });
            }
            ContentKind::AssistantSpeculative if reason == "INTERRUPTED" => {
                self.interruption(&mut actions)?;
            }
            ContentKind::AssistantFinal => {
                if reason == "INTERRUPTED" {
                    self.interruption(&mut actions)?;
                    // A canonical final interruption retires every remaining
                    // speculative segment of this generation, never a future one.
                    self.pending_finals.clear();
                }
            }
            ContentKind::AssistantAudio => {
                let generated = content
                    .generated
                    .ok_or(ProtocolError("missing explicit audio text association"))?;
                // The retained source id proves this is the exact consumed
                // speculative block, not whichever text most recently arrived.
                if generated.generation != self.response_generation
                    || !self.seen_contents.contains(&generated.source_content_id)
                {
                    return Err(ProtocolError("lost audio text ownership"));
                }
                let obligation = self
                    .pending_finals
                    .iter_mut()
                    .find(|pending| {
                        pending.source_content_id == generated.source_content_id
                            && pending.generation == generated.generation
                    })
                    .ok_or(ProtocolError("missing audio FINAL obligation"))?;
                obligation.audio_completed = true;
                if !generated.text.is_empty() {
                    actions.push(Action::output(Frame::new(FrameData::TtsText(
                        AggregatedTextFrame {
                            text: TextFrame::llm(generated.text),
                            aggregated_by: "sentence".to_owned(),
                            context_id: None,
                            raw_text: None,
                            will_be_spoken: true,
                        },
                    ))));
                }
                if reason == "INTERRUPTED" {
                    self.interruption(&mut actions)?;
                } else if reason == "END_TURN" {
                    self.end_response(&mut actions)?;
                }
            }
            ContentKind::AssistantSpeculative => {
                unreachable!("all speculative stop reasons handled")
            }
        }
        Ok(actions)
    }

    fn end_response(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        let completion = self
            .completion
            .as_mut()
            .ok_or(ProtocolError("no active completion"))?;
        if completion.response == ResponseState::Open {
            completion.response = ResponseState::Ended;
            actions.push(Action::output(Frame::new(FrameData::LlmResponseEnd)));
            actions.push(Action::output(Frame::new(FrameData::TtsStopped(
                TtsStoppedFrame::default(),
            ))));
            actions.push(Action::ResponseCompleted);
        }
        Ok(())
    }

    fn interruption(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        actions.push(Action::Interrupt);
        self.pending_text = None;
        self.end_response(actions)?;
        self.completion
            .as_mut()
            .ok_or(ProtocolError("no active completion"))?
            .response = ResponseState::Interrupted;
        Ok(())
    }

    fn completion_end(&mut self, body: &Map<String, Value>) -> Result<Vec<Action>> {
        self.completion_identity(body)?;
        let reason = string(body, "stopReason")?;
        if reason == "TOOL_USE" {
            return Err(ProtocolError("tools unsupported"));
        }
        if !matches!(reason, "END_TURN" | "INTERRUPTED") {
            return Err(ProtocolError("unsupported completion stop reason"));
        }
        if self.content.is_some() || self.pending_text.is_some() {
            return Err(ProtocolError("completion ended with open content"));
        }
        let mut actions = Vec::new();
        if reason == "INTERRUPTED"
            && self
                .completion
                .as_ref()
                .is_some_and(|c| c.response != ResponseState::Interrupted)
        {
            self.interruption(&mut actions)?;
        }
        if self
            .completion
            .as_ref()
            .is_some_and(|c| c.response == ResponseState::Open)
        {
            return Err(ProtocolError("completion ended with open response"));
        }
        // completionEnd conclusively closes the whole enclosing exchange. No
        // further FINAL can arrive, so diagnostic obligations may be retired.
        self.pending_finals.clear();
        self.completion = None;
        Ok(actions)
    }

    fn usage(&self, body: &Map<String, Value>) -> Result<Vec<Action>> {
        self.completion_identity(body)?;
        // These cumulative counters are validated, but never added to deltas.
        for key in ["totalInputTokens", "totalOutputTokens", "totalTokens"] {
            if body.contains_key(key) {
                number(body, key)?;
            }
        }
        if body.contains_key("totalInputTokens") && body.contains_key("totalOutputTokens") {
            number(body, "totalInputTokens")?
                .checked_add(number(body, "totalOutputTokens")?)
                .ok_or(ProtocolError("usage counter overflow"))?;
        }
        let Some(details) = body.get("details") else {
            return Ok(Vec::new());
        };
        let details = object(details)?;
        if let Some(total) = details.get("total") {
            validate_usage_buckets(object(total)?)?;
        }
        let Some(delta) = details.get("delta") else {
            return Ok(Vec::new());
        };
        let (prompt_tokens, completion_tokens) = validate_usage_buckets(object(delta)?)?;
        let total_tokens = prompt_tokens
            .checked_add(completion_tokens)
            .ok_or(ProtocolError("usage counter overflow"))?;
        if total_tokens == 0 {
            return Ok(Vec::new());
        }
        Ok(vec![Action::Usage(UsageDelta {
            prompt_tokens,
            completion_tokens,
            total_tokens,
        })])
    }

    /// Call only at the response stream's actual EOF. UserFinal blocks have
    /// already been returned; the parent must commit any remaining received
    /// prefix and settle its genuine receipt before claiming clean shutdown.
    pub(crate) fn finish(&self) -> Result<()> {
        if self.failed {
            return Err(ProtocolError("mapper already failed"));
        }
        if self.seen_completions.is_empty() {
            return Err(ProtocolError("EOF before completion envelope"));
        }
        if self.content.is_some()
            || self.pending_text.is_some()
            || self.completion.is_some()
            || !self.pending_finals.is_empty()
        {
            return Err(ProtocolError("EOF with incomplete Nova completion"));
        }
        Ok(())
    }
}

fn validate_usage_buckets(obj: &Map<String, Value>) -> Result<(u64, u64)> {
    let mut result = [0_u64; 2];
    for (index, key) in ["input", "output"].iter().enumerate() {
        if let Some(bucket) = obj.get(*key) {
            let bucket = object(bucket)?;
            for token_type in ["speechTokens", "textTokens"] {
                if bucket.contains_key(token_type) {
                    result[index] = result[index]
                        .checked_add(number(bucket, token_type)?)
                        .ok_or(ProtocolError("usage counter overflow"))?;
                }
            }
        }
    }
    result[0]
        .checked_add(result[1])
        .ok_or(ProtocolError("usage counter overflow"))?;
    Ok((result[0], result[1]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(name: &str, fields: Value) -> Vec<u8> {
        let mut body =
            json!({"sessionId":"session-1","promptName":"prompt-1","completionId":"completion-1"});
        body.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        serde_json::to_vec(&json!({"event":{name:body}})).unwrap()
    }

    fn send(mapper: &mut Mapper, name: &str, fields: Value) -> Result<Vec<Action>> {
        mapper.map_nova_json(&event(name, fields))
    }

    fn mapper() -> Mapper {
        let mut mapper = Mapper::new("prompt-1".to_owned(), ProtocolLimits::default()).unwrap();
        send(&mut mapper, "completionStart", json!({})).unwrap();
        mapper
    }

    fn text_start(
        mapper: &mut Mapper,
        content: &str,
        role: &str,
        stage: &str,
    ) -> Result<Vec<Action>> {
        send(
            mapper,
            "contentStart",
            json!({
                "contentId":content,"type":"TEXT","role":role,
                "additionalModelFields":format!("{{\"generationStage\":\"{stage}\"}}"),
                "textOutputConfiguration":{"mediaType":"text/plain"}
            }),
        )
    }

    fn text_block(
        mapper: &mut Mapper,
        content: &str,
        role: &str,
        stage: &str,
        text: &str,
        reason: &str,
    ) -> Vec<Action> {
        let mut actions = text_start(mapper, content, role, stage).unwrap();
        actions.extend(
            send(
                mapper,
                "textOutput",
                json!({"contentId":content,"content":text}),
            )
            .unwrap(),
        );
        actions.extend(
            send(
                mapper,
                "contentEnd",
                json!({"contentId":content,"type":"TEXT","stopReason":reason}),
            )
            .unwrap(),
        );
        actions
    }

    fn audio_start(mapper: &mut Mapper, content: &str) -> Result<Vec<Action>> {
        send(
            mapper,
            "contentStart",
            json!({
                "contentId":content,"type":"AUDIO","role":"ASSISTANT",
                "audioOutputConfiguration":{"mediaType":"audio/lpcm","encoding":"base64","sampleRateHertz":24000,"sampleSizeBits":16,"channelCount":1}
            }),
        )
    }

    fn audio_end(mapper: &mut Mapper, content: &str, reason: &str) -> Result<Vec<Action>> {
        send(
            mapper,
            "contentEnd",
            json!({"contentId":content,"type":"AUDIO","stopReason":reason}),
        )
    }

    fn speculative(mapper: &mut Mapper, id: &str, text: &str) -> Vec<Action> {
        text_block(mapper, id, "ASSISTANT", "SPECULATIVE", text, "PARTIAL_TURN")
    }

    fn completed_response(mapper: &mut Mapper) -> Vec<Action> {
        let mut actions = speculative(mapper, "spec-1", "Generated sentence.");
        actions.extend(audio_start(mapper, "audio-1").unwrap());
        actions.extend(
            send(
                mapper,
                "audioOutput",
                json!({"contentId":"audio-1","content":"AAEC/w=="}),
            )
            .unwrap(),
        );
        actions.extend(audio_end(mapper, "audio-1", "END_TURN").unwrap());
        actions
    }

    fn output(actions: &[Action]) -> Vec<&FrameData> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Output(f) => Some(&f.data),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn strict_json_rejects_duplicate_keys_at_every_depth_and_escaped_aliases() {
        for bytes in [
            br#"{"event":{},"event":{}}"#.as_slice(),
            br#"{"outer":{"known":1,"known":2}}"#,
            br#"{"unknown":{"a":1,"\u0061":2}}"#,
            br#"{"array":[{"id":1,"id":2}]}"#,
        ] {
            assert!(strict_json(bytes, &ProtocolLimits::default()).is_err());
        }
        assert!(strict_json(
            br#"{"a":1,"b":{"a":2},"c":[true,null,-2,1.25]}"#,
            &ProtocolLimits::default()
        )
        .is_ok());
    }

    #[test]
    fn strict_json_enforces_bytes_depth_nodes_strings_and_trailing_data() {
        let mut limits = ProtocolLimits {
            max_event_bytes: 10,
            max_string_bytes: 10,
            ..ProtocolLimits::default()
        };
        assert!(strict_json(b"            {}", &limits).is_err());
        limits = ProtocolLimits::default();
        limits.max_json_depth = 2;
        assert!(strict_json(br#"{"a":{"b":1}}"#, &limits).is_err());
        limits = ProtocolLimits::default();
        limits.max_json_nodes = 2;
        assert!(strict_json(br#"{"a":1}"#, &limits).is_err());
        limits = ProtocolLimits::default();
        limits.max_string_bytes = 3;
        assert!(strict_json(br#"{"a":"1234"}"#, &limits).is_err());
        assert!(strict_json(br#"{"long":1}"#, &limits).is_err());
        for invalid in [b"{}{}".as_slice(), b"{} garbage", b"", b"\xff", b"1e999"] {
            assert!(strict_json(invalid, &ProtocolLimits::default()).is_err());
        }
    }

    #[test]
    fn chunk_is_full_eventstream_with_sdk_blob_not_raw_nova() {
        let limits = ProtocolLimits::default();
        let inner = encode_chunk(br#"{"event":{}}"#, &limits).unwrap();
        let decoded = eventstream::decode(&inner, limits.max_frame_bytes).unwrap();
        assert_eq!(decoded.header(":message-type"), Some("event"));
        assert_eq!(decoded.header(":event-type"), Some("chunk"));
        assert_eq!(decoded.header(":content-type"), Some("application/json"));
        assert_eq!(decoded.payload, br#"{"bytes":"eyJldmVudCI6e319"}"#);
        assert_eq!(
            decode_response(&inner, &limits).unwrap(),
            br#"{"event":{}}"#
        );

        let nova = br#"{"event":{"audioOutput":{"content":"AAEC/w=="}}}"#;
        let frame = encode_chunk(nova, &limits).unwrap();
        let json = decode_response(&frame, &limits).unwrap();
        assert_eq!(json, nova);
        let value = strict_json(&json, &limits).unwrap();
        assert_eq!(
            decode_base64(
                value["event"]["audioOutput"]["content"].as_str().unwrap(),
                4
            )
            .unwrap(),
            [0, 1, 2, 255]
        );
    }

    #[test]
    fn chunk_and_audio_base64_are_canonical_and_bounded() {
        let limits = ProtocolLimits::default();
        for bad in ["Zg", "Zg=", "Zh==", "Zg===", "Zg==\n", "_w==", "*==="] {
            assert!(decode_base64(bad, 16).is_err(), "{bad}");
        }
        assert!(decode_base64("AAEC/w==", 3).is_err());
        assert_eq!(decode_base64("AAEC/w==", 4).unwrap().len(), 4);
        for bad in [
            br#"{"bytes":"e30=","bytes":"e30="}"#.as_slice(),
            br#"{"bytes":null}"#,
            br#"{"bytes":"e30=","other":1}"#,
            br#"{"event":{}}"#,
            br#"{"bytes":"//8="}"#,
            br#"{"bytes":"e30=e30="}"#,
        ] {
            assert!(decode_chunk(bad, &limits).is_err());
        }
    }

    #[test]
    fn response_rejects_crc_and_signed_or_wrong_header_envelopes() {
        let limits = ProtocolLimits::default();
        let frame = encode_chunk(br#"{"event":{}}"#, &limits).unwrap();
        for index in [0, 8, 12, frame.len() - 1] {
            let mut corrupt = frame.clone();
            corrupt[index] ^= 1;
            assert!(decode_response(&corrupt, &limits).is_err());
        }
        for headers in [
            vec![
                (":message-type", "event"),
                (":event-type", "other"),
                (":content-type", "application/json"),
            ],
            vec![
                (":message-type", "event"),
                (":event-type", "chunk"),
                (":content-type", "application/octet-stream"),
            ],
            vec![(":message-type", "event"), (":event-type", "chunk")],
            vec![
                (":message-type", "event"),
                (":event-type", "chunk"),
                (":content-type", "application/json"),
                (":date", "1"),
            ],
        ] {
            let wrong =
                eventstream::encode(&headers, br#"{"bytes":"e30="}"#, limits.max_frame_bytes)
                    .unwrap();
            assert!(decode_response(&wrong, &limits).is_err());
        }
        let signed = eventstream::encode_typed(
            &[
                (":date", eventstream::HeaderValue::Timestamp(0)),
                (
                    ":chunk-signature",
                    eventstream::HeaderValue::Bytes(&[0; 32]),
                ),
            ],
            &frame,
            limits.max_frame_bytes,
        )
        .unwrap();
        assert!(decode_response(&signed, &limits).is_err());
    }

    #[test]
    fn exception_details_never_leak_and_malformed_fields_reject() {
        let limits = ProtocolLimits::default();
        for kind in [
            "internalServerException",
            "modelStreamErrorException",
            "validationException",
            "throttlingException",
            "modelTimeoutException",
            "serviceUnavailableException",
            "unrecognizedException",
        ] {
            let frame = eventstream::encode(
                &[
                    (":message-type", "exception"),
                    (":exception-type", kind),
                    (":content-type", "application/json"),
                ],
                br#"{"message":"sensitive-provider-payload"}"#,
                limits.max_frame_bytes,
            )
            .unwrap();
            let error = decode_response(&frame, &limits).unwrap_err();
            assert!(!error.to_string().contains("sensitive-provider-payload"));
            assert!(!format!("{error:?}").contains("sensitive-provider-payload"));
        }
        for body in [
            br#"{"message":2}"#.as_slice(),
            br#"{"message":"x","message":"y"}"#,
            br#"{"originalStatusCode":-1}"#,
        ] {
            let frame = eventstream::encode(
                &[
                    (":message-type", "exception"),
                    (":exception-type", "validationException"),
                    (":content-type", "application/json"),
                ],
                body,
                limits.max_frame_bytes,
            )
            .unwrap();
            assert!(decode_response(&frame, &limits).is_err());
        }
    }

    #[test]
    fn raw_user_finals_arrive_before_the_assistant_boundary_without_synthetic_output() {
        let mut mapper = mapper();
        let one = text_block(
            &mut mapper,
            "user-1",
            "USER",
            "FINAL",
            "First",
            "PARTIAL_TURN",
        );
        let two = text_block(&mut mapper, "user-2", "USER", "FINAL", "second", "END_TURN");
        let blank = text_block(&mut mapper, "user-3", "USER", "FINAL", "  ", "END_TURN");
        assert!(matches!(&one[..], [Action::UserFinal(text)] if text == "First"));
        assert!(matches!(&two[..], [Action::UserFinal(text)] if text == "second"));
        assert!(matches!(&blank[..], [Action::UserFinal(text)] if text == "  "));
        let starts = text_start(&mut mapper, "spec", "ASSISTANT", "SPECULATIVE").unwrap();
        assert!(matches!(starts.first(), Some(Action::Boundary)));
        assert!(matches!(
            output(&starts)[..],
            [FrameData::LlmResponseStart, FrameData::TtsStarted(_)]
        ));
    }

    #[test]
    fn user_text_deltas_append_not_overwrite_and_remaining_prefix_is_visible_at_eof() {
        let mut mapper = mapper();
        text_start(&mut mapper, "user", "USER", "FINAL").unwrap();
        for text in ["I ", "heard", " this"] {
            send(
                &mut mapper,
                "textOutput",
                json!({"contentId":"user","content":text}),
            )
            .unwrap();
        }
        let actions = send(
            &mut mapper,
            "contentEnd",
            json!({"contentId":"user","type":"TEXT","stopReason":"END_TURN"}),
        )
        .unwrap();
        assert!(matches!(&actions[..],[Action::UserFinal(text)] if text == "I heard this"));
        assert!(mapper.finish().is_err());
        send(
            &mut mapper,
            "completionEnd",
            json!({"stopReason":"END_TURN"}),
        )
        .unwrap();
        assert!(mapper.finish().is_ok());
    }

    #[test]
    fn generated_text_is_appended_once_after_audio_and_final_never_duplicates_history() {
        let mut mapper = mapper();
        let actions = completed_response(&mut mapper);
        let output = output(&actions);
        assert!(matches!(output[0], FrameData::LlmResponseStart));
        assert!(matches!(output[1], FrameData::TtsStarted(_)));
        assert!(
            matches!(output[2],FrameData::LlmText(text) if !text.append_to_context && text.text == "Generated sentence.")
        );
        assert!(
            matches!(output[3],FrameData::TtsAudio(audio) if audio.audio == [0,1,2,255] && audio.sample_rate == 24000 && audio.num_channels == 1)
        );
        assert!(
            matches!(output[4],FrameData::TtsText(text) if text.text.append_to_context && text.text.includes_inter_frame_spaces && text.text.text == "Generated sentence.")
        );
        assert!(matches!(output[5], FrameData::LlmResponseEnd));
        assert!(matches!(output[6], FrameData::TtsStopped(_)));
        assert_eq!(
            actions
                .iter()
                .filter(|a| matches!(a, Action::ResponseCompleted))
                .count(),
            1
        );
        let finals = text_block(
            &mut mapper,
            "final-1",
            "ASSISTANT",
            "FINAL",
            "Provider final differs.",
            "END_TURN",
        );
        assert!(finals.is_empty());
        assert!(send(
            &mut mapper,
            "completionEnd",
            json!({"stopReason":"END_TURN"})
        )
        .unwrap()
        .is_empty());
        mapper.finish().unwrap();
    }

    #[test]
    fn multiple_segments_have_exact_association_and_only_one_response_start_end() {
        let mut mapper = mapper();
        let first = speculative(&mut mapper, "spec-a", "First.");
        audio_start(&mut mapper, "audio-a").unwrap();
        let first_audio = audio_end(&mut mapper, "audio-a", "PARTIAL_TURN").unwrap();
        let second = speculative(&mut mapper, "spec-b", "Second.");
        audio_start(&mut mapper, "audio-b").unwrap();
        let second_audio = audio_end(&mut mapper, "audio-b", "END_TURN").unwrap();
        assert_eq!(
            first
                .iter()
                .filter(|a| matches!(a, Action::Boundary))
                .count(),
            1
        );
        assert_eq!(
            second
                .iter()
                .filter(|a| matches!(a, Action::Boundary))
                .count(),
            1
        );
        assert!(
            matches!(output(&first_audio)[..],[FrameData::TtsText(text)] if text.text.text == "First.")
        );
        assert!(
            matches!(output(&second_audio)[0],FrameData::TtsText(text) if text.text.text == "Second.")
        );
        assert_eq!(
            second_audio
                .iter()
                .filter(|a| matches!(a, Action::ResponseCompleted))
                .count(),
            1
        );
    }

    #[test]
    fn ambiguous_second_speculative_block_fails_instead_of_overwriting_pending_text() {
        let mut mapper = mapper();
        speculative(&mut mapper, "spec-a", "First.");
        assert!(text_start(&mut mapper, "spec-b", "ASSISTANT", "SPECULATIVE").is_err());
        assert!(audio_start(&mut mapper, "audio-a").is_err());
        assert!(mapper.finish().is_err());
    }

    #[test]
    fn canonical_late_final_interruption_flushes_without_reopening_ended_response() {
        let mut mapper = mapper();
        completed_response(&mut mapper);
        let actions = text_block(
            &mut mapper,
            "late-final",
            "ASSISTANT",
            "FINAL",
            "Generated sentence.",
            "INTERRUPTED",
        );
        assert!(matches!(actions[..], [Action::Interrupt]));
        send(
            &mut mapper,
            "completionEnd",
            json!({"stopReason":"END_TURN"}),
        )
        .unwrap();
        mapper.finish().unwrap();
    }

    #[test]
    fn audio_interruption_precedes_balanced_response_end_and_late_final_still_interrupts() {
        let mut mapper = mapper();
        speculative(&mut mapper, "spec", "Hello.");
        audio_start(&mut mapper, "audio").unwrap();
        let actions = audio_end(&mut mapper, "audio", "INTERRUPTED").unwrap();
        assert!(
            matches!(actions[0],Action::Output(ref frame) if matches!(frame.data,FrameData::TtsText(_)))
        );
        assert!(matches!(actions[1], Action::Interrupt));
        assert!(
            matches!(actions[2],Action::Output(ref frame) if matches!(frame.data,FrameData::LlmResponseEnd))
        );
        assert!(
            matches!(actions[3],Action::Output(ref frame) if matches!(frame.data,FrameData::TtsStopped(_)))
        );
        assert!(matches!(actions[4], Action::ResponseCompleted));
        let late = text_block(
            &mut mapper,
            "late-final",
            "ASSISTANT",
            "FINAL",
            "Hello.",
            "INTERRUPTED",
        );
        assert!(matches!(late[..], [Action::Interrupt]));
    }

    #[test]
    fn speculative_interruption_closes_response_without_committing_speculative_history() {
        let mut mapper = mapper();
        text_start(&mut mapper, "spec", "ASSISTANT", "SPECULATIVE").unwrap();
        send(
            &mut mapper,
            "textOutput",
            json!({"contentId":"spec","content":"Discard me"}),
        )
        .unwrap();
        let actions = send(
            &mut mapper,
            "contentEnd",
            json!({"contentId":"spec","type":"TEXT","stopReason":"INTERRUPTED"}),
        )
        .unwrap();
        assert!(matches!(actions[0], Action::Interrupt));
        assert!(!output(&actions)
            .iter()
            .any(|f| matches!(f, FrameData::LlmText(_) | FrameData::TtsText(_))));
        assert!(text_start(&mut mapper, "spec-new", "ASSISTANT", "SPECULATIVE").is_err());
    }

    #[test]
    fn new_completion_has_new_boundary_and_old_ids_cannot_enter_it() {
        let mut mapper = mapper();
        completed_response(&mut mapper);
        send(
            &mut mapper,
            "completionEnd",
            json!({"stopReason":"END_TURN"}),
        )
        .unwrap();
        send(
            &mut mapper,
            "completionStart",
            json!({"completionId":"completion-2"}),
        )
        .unwrap();
        let next = send(&mut mapper,"contentStart",json!({
            "completionId":"completion-2","contentId":"fresh","type":"TEXT","role":"ASSISTANT",
            "additionalModelFields":"{\"generationStage\":\"SPECULATIVE\"}","textOutputConfiguration":{"mediaType":"text/plain"}
        })).unwrap();
        assert!(matches!(next.first(), Some(Action::Boundary)));
        assert!(send(
            &mut mapper,
            "audioOutput",
            json!({"contentId":"audio-1","content":"AAA="})
        )
        .is_err());
    }

    #[test]
    fn all_response_identity_fields_are_required_and_mismatches_poison() {
        for field in ["sessionId", "promptName", "completionId", "contentId"] {
            let mut mapper = mapper();
            text_start(&mut mapper, "user", "USER", "FINAL").unwrap();
            let bytes = event("textOutput", json!({"contentId":"user","content":"text"}));
            let mut value: Value = serde_json::from_slice(&bytes).unwrap();
            value["event"]["textOutput"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(mapper
                .map_nova_json(&serde_json::to_vec(&value).unwrap())
                .is_err());
            assert!(send(
                &mut mapper,
                "textOutput",
                json!({"contentId":"user","content":"text"})
            )
            .is_err());
        }
        for (field, bad) in [
            ("sessionId", "other-session"),
            ("promptName", "other-prompt"),
            ("completionId", "other-completion"),
            ("contentId", "other-content"),
        ] {
            let mut mapper = mapper();
            text_start(&mut mapper, "user", "USER", "FINAL").unwrap();
            let mut fields = json!({"contentId":"user","content":"text"});
            fields[field] = Value::String(bad.to_owned());
            assert!(send(&mut mapper, "textOutput", fields).is_err());
        }
    }

    #[test]
    fn content_name_is_not_a_response_content_id() {
        let mut mapper = mapper();
        assert!(send(&mut mapper,"contentStart",json!({"contentName":"wrong","type":"TEXT","role":"USER","additionalModelFields":"{\"generationStage\":\"FINAL\"}","textOutputConfiguration":{"mediaType":"text/plain"}})).is_err());
    }

    #[test]
    fn configurations_roles_stages_and_content_end_types_are_strict() {
        for fields in [
            json!({"type":"TEXT","role":"SYSTEM","additionalModelFields":"{\"generationStage\":\"FINAL\"}","textOutputConfiguration":{"mediaType":"text/plain"}}),
            json!({"type":"TEXT","role":"USER","additionalModelFields":"{\"generationStage\":\"SPECULATIVE\"}","textOutputConfiguration":{"mediaType":"text/plain"}}),
            json!({"type":"TEXT","role":"USER","additionalModelFields":{"generationStage":"FINAL"},"textOutputConfiguration":{"mediaType":"text/plain"}}),
            json!({"type":"TEXT","role":"USER","additionalModelFields":"{\"generationStage\":\"FINAL\",\"generationStage\":\"FINAL\"}","textOutputConfiguration":{"mediaType":"text/plain"}}),
            json!({"type":"TEXT","role":"USER","additionalModelFields":"{\"generationStage\":\"FINAL\"}","textOutputConfiguration":{"mediaType":"text/html"}}),
            json!({"type":"TEXT","role":"USER","textOutputConfiguration":{"mediaType":"text/plain"}}),
        ] {
            let mut mapper = mapper();
            let mut fields = fields;
            fields["contentId"] = json!("content");
            assert!(send(&mut mapper, "contentStart", fields).is_err());
        }
        let mut mapper = mapper();
        text_start(&mut mapper, "user", "USER", "FINAL").unwrap();
        assert!(send(
            &mut mapper,
            "contentEnd",
            json!({"contentId":"user","type":"AUDIO","stopReason":"END_TURN"})
        )
        .is_err());
    }

    #[test]
    fn audio_configuration_and_pcm_alignment_cannot_silently_change() {
        for (key, value) in [
            ("mediaType", json!("audio/wav")),
            ("sampleRateHertz", json!(16000)),
            ("sampleSizeBits", json!(8)),
            ("channelCount", json!(2)),
            ("encoding", json!("raw")),
        ] {
            let mut mapper = mapper();
            speculative(&mut mapper, "spec", "Hello");
            let mut config = json!({"mediaType":"audio/lpcm","sampleRateHertz":24000,"sampleSizeBits":16,"channelCount":1,"encoding":"base64"});
            config[key] = value;
            assert!(send(&mut mapper,"contentStart",json!({"contentId":"audio","type":"AUDIO","role":"ASSISTANT","audioOutputConfiguration":config})).is_err());
        }
        for bad in ["", "AA==", "AAA", "AAB="] {
            let mut mapper = mapper();
            speculative(&mut mapper, "spec", "Hello");
            audio_start(&mut mapper, "audio").unwrap();
            assert!(send(
                &mut mapper,
                "audioOutput",
                json!({"contentId":"audio","content":bad})
            )
            .is_err());
        }
    }

    #[test]
    fn impossible_order_and_content_reuse_fail_closed() {
        let mut no_completion =
            Mapper::new("prompt-1".to_owned(), ProtocolLimits::default()).unwrap();
        assert!(text_start(&mut no_completion, "user", "USER", "FINAL").is_err());
        let mut overlapping = mapper();
        text_start(&mut overlapping, "one", "USER", "FINAL").unwrap();
        assert!(text_start(&mut overlapping, "two", "USER", "FINAL").is_err());
        let mut reused = mapper();
        text_block(&mut reused, "same", "USER", "FINAL", "hello", "END_TURN");
        assert!(text_start(&mut reused, "same", "USER", "FINAL").is_err());
        let mut no_spec = mapper();
        assert!(audio_start(&mut no_spec, "audio").is_err());
        let mut text_only = mapper();
        speculative(&mut text_only, "spec", "hello");
        assert!(send(
            &mut text_only,
            "completionEnd",
            json!({"stopReason":"END_TURN"})
        )
        .is_err());
    }

    #[test]
    fn tool_unions_content_and_continuation_never_produce_actions() {
        for (kind, fields) in [
            (
                "toolUse",
                json!({"contentId":"tool","toolName":"execute","toolUseId":"tool-id","content":"{}"}),
            ),
            (
                "contentStart",
                json!({"contentId":"tool","type":"TOOL","role":"TOOL"}),
            ),
            ("completionEnd", json!({"stopReason":"TOOL_USE"})),
        ] {
            let mut mapper = mapper();
            assert!(send(&mut mapper, kind, fields).is_err());
            assert!(mapper.finish().is_err());
        }
        let mut mapper = mapper();
        let multi = br#"{"event":{"toolUse":{},"contentStart":{"type":"TEXT"}}}"#;
        assert!(mapper.map_nova_json(multi).is_err());
        let mut mapper =
            super::Mapper::new("prompt-1".to_owned(), ProtocolLimits::default()).unwrap();
        assert!(mapper
            .map_nova_json(br#"{"event":{"sessionUpdated":{}}}"#)
            .is_err());
    }

    #[test]
    fn usage_emits_only_incremental_delta_with_checked_arithmetic() {
        let mut mapper = mapper();
        let actions = send(&mut mapper,"usageEvent",json!({
            "totalInputTokens":100,"totalOutputTokens":200,"totalTokens":300,
            "details":{"delta":{"input":{"speechTokens":2,"textTokens":3},"output":{"speechTokens":5,"textTokens":7}},"total":{"input":{"speechTokens":40,"textTokens":60},"output":{"speechTokens":100,"textTokens":100}}}
        })).unwrap();
        assert!(matches!(
            actions[..],
            [Action::Usage(UsageDelta {
                prompt_tokens: 5,
                completion_tokens: 12,
                total_tokens: 17
            })]
        ));
        assert!(send(&mut mapper, "usageEvent", json!({}))
            .unwrap()
            .is_empty());
        assert!(send(
            &mut mapper,
            "usageEvent",
            json!({"details":{"delta":{"input":{}}}})
        )
        .unwrap()
        .is_empty());
        for fields in [
            json!({"details":null}),
            json!({"details":{"delta":[]}}),
            json!({"details":{"delta":{"input":{"speechTokens":-1}}}}),
            json!({"details":{"delta":{"input":{"speechTokens":1.5}}}}),
            json!({"details":{"delta":{"input":{"speechTokens":u64::MAX,"textTokens":1}}}}),
            json!({"details":{"delta":{"input":{"speechTokens":u64::MAX},"output":{"textTokens":1}}}}),
            json!({"totalInputTokens":"100"}),
        ] {
            let mut bad = self::mapper();
            assert!(send(&mut bad, "usageEvent", fields).is_err());
        }
    }

    #[test]
    fn finite_text_audio_and_identity_budgets_apply_without_eviction() {
        let limits = ProtocolLimits {
            max_content_text_bytes: 3,
            max_audio_bytes: 2,
            max_content_ids: 1,
            max_completion_ids: 1,
            ..ProtocolLimits::default()
        };
        let mut text = Mapper::new("prompt-1".to_owned(), limits).unwrap();
        send(&mut text, "completionStart", json!({})).unwrap();
        text_start(&mut text, "user", "USER", "FINAL").unwrap();
        assert!(send(
            &mut text,
            "textOutput",
            json!({"contentId":"user","content":"four"})
        )
        .is_err());
        let mut content = Mapper::new("prompt-1".to_owned(), limits).unwrap();
        send(&mut content, "completionStart", json!({})).unwrap();
        text_block(&mut content, "user", "USER", "FINAL", "one", "END_TURN");
        assert!(text_start(&mut content, "two", "USER", "FINAL").is_err());
        let mut completion = Mapper::new("prompt-1".to_owned(), limits).unwrap();
        send(&mut completion, "completionStart", json!({})).unwrap();
        send(
            &mut completion,
            "completionEnd",
            json!({"stopReason":"END_TURN"}),
        )
        .unwrap();
        assert!(send(
            &mut completion,
            "completionStart",
            json!({"completionId":"completion-2"})
        )
        .is_err());
        let mut audio = Mapper::new(
            "prompt-1".to_owned(),
            ProtocolLimits {
                max_audio_bytes: 2,
                ..ProtocolLimits::default()
            },
        )
        .unwrap();
        send(&mut audio, "completionStart", json!({})).unwrap();
        speculative(&mut audio, "spec", "ok");
        audio_start(&mut audio, "audio").unwrap();
        assert!(send(
            &mut audio,
            "audioOutput",
            json!({"contentId":"audio","content":"AAEC/w=="})
        )
        .is_err());
    }

    #[test]
    fn eof_never_synthesizes_missing_close_or_discards_open_content() {
        let unopened = Mapper::new("prompt-1".to_owned(), ProtocolLimits::default()).unwrap();
        assert!(unopened.finish().is_err());
        let completion_open = mapper();
        assert!(completion_open.finish().is_err());
        let mut content_open = mapper();
        text_start(&mut content_open, "user", "USER", "FINAL").unwrap();
        assert!(content_open.finish().is_err());
        let mut response_open = mapper();
        speculative(&mut response_open, "spec", "hello");
        audio_start(&mut response_open, "audio").unwrap();
        audio_end(&mut response_open, "audio", "PARTIAL_TURN").unwrap();
        assert!(response_open.finish().is_err());
        assert!(send(
            &mut response_open,
            "completionEnd",
            json!({"stopReason":"END_TURN"})
        )
        .is_err());
    }

    #[test]
    fn multiple_spoken_turns_share_one_completion_envelope() {
        let mut mapper = mapper();
        completed_response(&mut mapper);
        text_block(
            &mut mapper,
            "final-first",
            "ASSISTANT",
            "FINAL",
            "Generated sentence.",
            "END_TURN",
        );
        let user = text_block(
            &mut mapper,
            "user-second",
            "USER",
            "FINAL",
            "Another question",
            "END_TURN",
        );
        assert!(matches!(user[..], [Action::UserFinal(_)]));
        let next = speculative(&mut mapper, "spec-second", "Another answer.");
        assert!(matches!(next.first(), Some(Action::Boundary)));
        assert!(matches!(output(&next)[0], FrameData::LlmResponseStart));
        audio_start(&mut mapper, "audio-second").unwrap();
        let end = audio_end(&mut mapper, "audio-second", "END_TURN").unwrap();
        assert_eq!(
            end.iter()
                .filter(|a| matches!(a, Action::ResponseCompleted))
                .count(),
            1
        );
        text_block(
            &mut mapper,
            "final-second",
            "ASSISTANT",
            "FINAL",
            "Another answer.",
            "END_TURN",
        );
        send(
            &mut mapper,
            "completionEnd",
            json!({"stopReason":"END_TURN"}),
        )
        .unwrap();
        mapper.finish().unwrap();
    }

    #[test]
    fn interleaved_user_prefix_gets_boundary_before_each_next_assistant_production() {
        let mut mapper = mapper();
        speculative(&mut mapper, "spec-first", "First.");
        let user = text_block(
            &mut mapper,
            "user-between",
            "USER",
            "FINAL",
            "Interleaved",
            "END_TURN",
        );
        assert!(matches!(user[..], [Action::UserFinal(_)]));
        let audio = audio_start(&mut mapper, "audio-first").unwrap();
        assert!(matches!(audio[..], [Action::Boundary]));
        audio_end(&mut mapper, "audio-first", "PARTIAL_TURN").unwrap();
        text_block(
            &mut mapper,
            "user-between-segments",
            "USER",
            "FINAL",
            "More",
            "END_TURN",
        );
        let next = speculative(&mut mapper, "spec-second", "Second.");
        assert!(matches!(next.first(), Some(Action::Boundary)));
        assert!(!output(&next)
            .iter()
            .any(|frame| matches!(frame, FrameData::LlmResponseStart)));
    }

    #[test]
    fn final_interruption_without_audio_closes_and_retires_speculative_text() {
        let mut mapper = mapper();
        speculative(&mut mapper, "spec-first", "Never generated as audio.");
        let final_actions = text_block(
            &mut mapper,
            "final-first",
            "ASSISTANT",
            "FINAL",
            "",
            "INTERRUPTED",
        );
        assert!(matches!(final_actions[0], Action::Interrupt));
        assert_eq!(
            final_actions
                .iter()
                .filter(|a| matches!(a, Action::ResponseCompleted))
                .count(),
            1
        );
        assert!(!output(&final_actions)
            .iter()
            .any(|frame| matches!(frame, FrameData::TtsText(_))));
        let fresh = speculative(&mut mapper, "spec-fresh", "Fresh response.");
        assert!(matches!(output(&fresh)[0], FrameData::LlmResponseStart));
    }

    #[test]
    fn ambiguous_cross_turn_final_and_reused_old_final_are_rejected() {
        let mut unresolved = mapper();
        completed_response(&mut unresolved);
        assert!(text_start(&mut unresolved, "new-turn", "ASSISTANT", "SPECULATIVE").is_err());
        let mut stale = mapper();
        completed_response(&mut stale);
        text_block(
            &mut stale,
            "final-old",
            "ASSISTANT",
            "FINAL",
            "Generated sentence.",
            "END_TURN",
        );
        speculative(&mut stale, "spec-new", "New response.");
        assert!(text_start(&mut stale, "final-old", "ASSISTANT", "FINAL").is_err());
    }

    #[test]
    fn final_before_audio_is_not_allowed_to_commit_or_complete_normally() {
        let mut mapper = mapper();
        speculative(&mut mapper, "spec", "No audio yet.");
        text_start(&mut mapper, "final", "ASSISTANT", "FINAL").unwrap();
        send(
            &mut mapper,
            "textOutput",
            json!({"contentId":"final","content":"No audio yet."}),
        )
        .unwrap();
        assert!(send(
            &mut mapper,
            "contentEnd",
            json!({"contentId":"final","type":"TEXT","stopReason":"END_TURN"})
        )
        .is_err());
    }
}
