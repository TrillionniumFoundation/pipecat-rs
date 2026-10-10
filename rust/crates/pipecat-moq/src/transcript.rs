use crate::{Error, MoqLimits, Result};
use bytes::Bytes;
use serde_json::{json, Value};
use std::io::Write;

const END_RESERVE: usize = 512;

/// Refuse oversized JSON while serializing, before allocating the complete value.
pub(crate) fn json_bytes(value: &Value, max: usize) -> Result<Vec<u8>> {
    struct Limited {
        bytes: Vec<u8>,
        max: usize,
    }
    impl Write for Limited {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if buf.len() > self.max.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("JSON byte limit"));
            }
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Limited {
        bytes: Vec::new(),
        max,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| Error::Limit("expanded JSON record"))?;
    Ok(writer.bytes)
}

pub(crate) struct Publisher {
    encoder: moq_json::stream::Encoder<Value>,
    track: moq_net::track::Producer,
    group: moq_net::group::Producer,
    epoch: String,
    sequence: u64,
    plain_bytes: usize,
    compressed_bytes: usize,
    limits: MoqLimits,
}
impl Publisher {
    pub fn new(track: moq_net::track::Producer, limits: MoqLimits) -> Result<Self> {
        let group = track.append_group().map_err(Error::protocol)?;
        let mut config = moq_json::stream::Config::default();
        config.compression = moq_json::Compression::Deflate;
        Ok(Self {
            encoder: moq_json::stream::Encoder::new(config),
            track,
            group,
            epoch: format!("{}-{}", moq_net::Hop::random(), moq_net::Hop::random()),
            sequence: 0,
            plain_bytes: 0,
            compressed_bytes: 0,
            limits,
        })
    }
    pub fn append(&mut self, mut message: Value, ending: bool) -> Result<()> {
        let object = message
            .as_object_mut()
            .ok_or(Error::InvalidConfig("transcript message must be an object"))?;
        object.insert("seq".into(), self.sequence.into());
        object.insert("epoch".into(), self.epoch.clone().into());
        let plain = json_bytes(&message, self.limits.expanded_record_bytes)?;
        let records = self.limits.transcript_records - usize::from(!ending);
        let budget = self.limits.transcript_bytes - if ending { 0 } else { END_RESERVE };
        if self.sequence >= records as u64 || plain.len() > budget.saturating_sub(self.plain_bytes)
        {
            return Err(Error::Limit("transcript history"));
        }
        let record = self.encoder.encode(&message).map_err(Error::protocol)?;
        let size = record.payload().len();
        if size > self.limits.compressed_record_bytes
            || size > budget.saturating_sub(self.compressed_bytes)
        {
            return Err(Error::Limit("compressed transcript history"));
        }
        self.group
            .write_frame(moq_net::Timestamp::now(), record.payload().clone())
            .map_err(Error::protocol)?;
        record.commit();
        self.sequence += 1;
        self.plain_bytes += plain.len();
        self.compressed_bytes += size;
        Ok(())
    }
    pub fn finish(&mut self) -> Result<()> {
        self.append(
            json!({"label":"moq-transport", "type":"session-ending"}),
            true,
        )?;
        self.group.finish().map_err(Error::protocol)?;
        self.track.finish().map_err(Error::protocol)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum Record {
    Message(Value),
    Replay,
    Internal,
    Goodbye,
}

pub(crate) struct Subscriber {
    decoder: moq_flate::Decoder,
    limits: MoqLimits,
    records: usize,
    compressed_bytes: usize,
    expanded_bytes: usize,
    epoch: Option<String>,
    sequence: Option<String>,
}
impl Subscriber {
    pub fn new(limits: MoqLimits) -> Self {
        Self {
            decoder: moq_flate::Decoder::with_max_frame_size(limits.expanded_record_bytes as u64),
            limits,
            records: 0,
            compressed_bytes: 0,
            expanded_bytes: 0,
            epoch: None,
            sequence: None,
        }
    }
    pub fn decode(&mut self, bytes: &Bytes) -> Result<Record> {
        if bytes.len() > self.limits.compressed_record_bytes
            || bytes.len()
                > self
                    .limits
                    .transcript_bytes
                    .saturating_sub(self.compressed_bytes)
            || self.records >= self.limits.transcript_records
        {
            return Err(Error::Limit("incoming compressed transcript history"));
        }
        let plain = self.decoder.frame(bytes).map_err(|e| match e {
            moq_flate::Error::TooLarge(_) => Error::Limit("expanded JSON record"),
            _ => Error::protocol(e),
        })?;
        if plain.len()
            > self
                .limits
                .transcript_bytes
                .saturating_sub(self.expanded_bytes)
        {
            return Err(Error::Limit("incoming expanded transcript history"));
        }
        self.records += 1;
        self.compressed_bytes += bytes.len();
        self.expanded_bytes += plain.len();
        let mut fields = object_fields(&plain)?;
        // Preserve the JSON token: Value's default numeric representation rounds
        // integers above u64, which must not turn sequenced records into legacy ones.
        let integer = fields.get("seq").and_then(|value| {
            let token = value.trim();
            let magnitude = token.strip_prefix('-').unwrap_or(token);
            (!magnitude.is_empty() && magnitude.bytes().all(|byte| byte.is_ascii_digit()))
                .then_some((token.starts_with('-'), magnitude))
        });
        if let Some((negative, magnitude)) = integer {
            let epoch = match fields.get("epoch") {
                None => None,
                Some(raw) => match serde_json::from_str(raw).map_err(Error::protocol)? {
                    Value::Null => None,
                    Value::String(epoch) => Some(epoch),
                    _ => {
                        return Err(Error::Protocol(
                            "sequenced epoch must be a string or null".into(),
                        ))
                    }
                },
            };
            if epoch != self.epoch {
                self.epoch = epoch;
                self.sequence = None;
            }
            if negative && magnitude != "0" {
                return Ok(Record::Replay);
            }
            if self
                .sequence
                .as_ref()
                .is_some_and(|last| (magnitude.len(), magnitude) <= (last.len(), last.as_str()))
            {
                return Ok(Record::Replay);
            }
            self.sequence = Some(magnitude.to_owned());
            fields.remove("seq");
            fields.remove("epoch");
        }
        let map = fields
            .into_iter()
            .map(|(key, raw)| {
                serde_json::from_str(raw)
                    .map(|value| (key, value))
                    .map_err(Error::protocol)
            })
            .collect::<Result<serde_json::Map<String, Value>>>()?;
        if map.get("label").and_then(Value::as_str) == Some("moq-transport") {
            return Ok(
                if map.get("type").and_then(Value::as_str) == Some("session-ending") {
                    Record::Goodbye
                } else {
                    Record::Internal
                },
            );
        }
        Ok(Record::Message(Value::Object(map)))
    }
}

/// Extract spans only after serde validates the complete, byte-bounded JSON.
/// IgnoredAny checks syntax without narrowing arbitrary-width integer tokens.
/// Unlike serde_json's raw_value feature, it does not change the interpretation
/// of literal user map keys throughout the workspace.
fn object_fields(plain: &[u8]) -> Result<std::collections::BTreeMap<String, &str>> {
    serde_json::from_slice::<serde::de::IgnoredAny>(plain).map_err(Error::protocol)?;
    let text = std::str::from_utf8(plain).map_err(Error::protocol)?;
    fn whitespace(bytes: &[u8], position: &mut usize) {
        while bytes
            .get(*position)
            .is_some_and(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
        {
            *position += 1;
        }
    }
    let mut position = 0;
    whitespace(plain, &mut position);
    if plain.get(position) != Some(&b'{') {
        return Err(Error::Protocol(
            "transcript record must be an object".into(),
        ));
    }
    position += 1;
    let mut fields = std::collections::BTreeMap::new();
    loop {
        whitespace(plain, &mut position);
        if plain.get(position) == Some(&b'}') {
            break;
        }
        let mut key =
            serde_json::Deserializer::from_slice(&plain[position..]).into_iter::<String>();
        let name = key
            .next()
            .ok_or_else(|| Error::Protocol("missing JSON object key".into()))?
            .map_err(Error::protocol)?;
        position += key.byte_offset();
        whitespace(plain, &mut position);
        if plain.get(position) != Some(&b':') {
            return Err(Error::Protocol("missing JSON object colon".into()));
        }
        position += 1;
        whitespace(plain, &mut position);
        let start = position;
        let mut value = serde_json::Deserializer::from_slice(&plain[position..])
            .into_iter::<serde::de::IgnoredAny>();
        value
            .next()
            .ok_or_else(|| Error::Protocol("missing JSON object value".into()))?
            .map_err(Error::protocol)?;
        position += value.byte_offset();
        // Match serde map semantics: duplicate decoded keys use the last value.
        fields.insert(name, &text[start..position]);
        whitespace(plain, &mut position);
        match plain.get(position) {
            Some(b',') => position += 1,
            Some(b'}') => break,
            _ => return Err(Error::Protocol("invalid JSON object delimiter".into())),
        }
    }
    Ok(fields)
}
