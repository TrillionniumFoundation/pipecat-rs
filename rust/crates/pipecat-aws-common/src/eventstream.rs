//! Bounded AWS eventstream framing. CRCs and lengths are checked before parsing.
use crate::AwsError;
use std::collections::BTreeMap;
#[derive(Debug, Clone, PartialEq)]
pub enum HeaderValue<'a> {
    Bool(bool),
    Byte(i8),
    Short(i16),
    Integer(i32),
    Long(i64),
    Bytes(&'a [u8]),
    String(&'a str),
    Timestamp(i64),
    Uuid([u8; 16]),
}
#[derive(Debug, Clone, PartialEq)]
pub struct Event<'a> {
    pub headers: BTreeMap<&'a str, HeaderValue<'a>>,
    pub payload: &'a [u8],
}
impl Event<'_> {
    pub fn header(&self, name: &str) -> Option<&str> {
        match self.headers.get(name) {
            Some(HeaderValue::String(value)) => Some(value),
            _ => None,
        }
    }
}
fn invalid() -> AwsError {
    AwsError::Protocol("invalid AWS eventstream")
}
fn take<'a>(data: &mut &'a [u8], n: usize) -> Result<&'a [u8], AwsError> {
    if data.len() < n {
        return Err(invalid());
    }
    let (head, tail) = data.split_at(n);
    *data = tail;
    Ok(head)
}
fn length(data: &mut &[u8]) -> Result<usize, AwsError> {
    Ok(u16::from_be_bytes(take(data, 2)?.try_into().map_err(|_| invalid())?) as usize)
}
fn utf8(data: &[u8]) -> Result<&str, AwsError> {
    std::str::from_utf8(data).map_err(|_| invalid())
}
/// Decodes exactly one message. Unknown header types and duplicate names fail closed.
pub fn decode(message: &[u8], max_bytes: usize) -> Result<Event<'_>, AwsError> {
    if message.len() > max_bytes {
        return Err(AwsError::TooLarge);
    }
    if message.len() < 16 {
        return Err(invalid());
    }
    let total = u32::from_be_bytes(message[..4].try_into().map_err(|_| invalid())?) as usize;
    let header_length =
        u32::from_be_bytes(message[4..8].try_into().map_err(|_| invalid())?) as usize;
    let prelude_crc = u32::from_be_bytes(message[8..12].try_into().map_err(|_| invalid())?);
    if total != message.len()
        || header_length > total - 16
        || header_length > 128 * 1024
        || crc32fast::hash(&message[..8]) != prelude_crc
    {
        return Err(invalid());
    }
    let message_crc = u32::from_be_bytes(message[total - 4..].try_into().map_err(|_| invalid())?);
    if crc32fast::hash(&message[..total - 4]) != message_crc {
        return Err(invalid());
    }
    let mut raw = &message[12..12 + header_length];
    let mut headers = BTreeMap::new();
    while !raw.is_empty() {
        if headers.len() >= 128 {
            return Err(AwsError::TooLarge);
        }
        let n = take(&mut raw, 1)?[0] as usize;
        if n == 0 {
            return Err(invalid());
        }
        let name = utf8(take(&mut raw, n)?)?;
        let typ = take(&mut raw, 1)?[0];
        let value = match typ {
            0 => HeaderValue::Bool(true),
            1 => HeaderValue::Bool(false),
            2 => HeaderValue::Byte(take(&mut raw, 1)?[0] as i8),
            3 => HeaderValue::Short(i16::from_be_bytes(
                take(&mut raw, 2)?.try_into().map_err(|_| invalid())?,
            )),
            4 => HeaderValue::Integer(i32::from_be_bytes(
                take(&mut raw, 4)?.try_into().map_err(|_| invalid())?,
            )),
            5 => HeaderValue::Long(i64::from_be_bytes(
                take(&mut raw, 8)?.try_into().map_err(|_| invalid())?,
            )),
            6 => {
                let n = length(&mut raw)?;
                HeaderValue::Bytes(take(&mut raw, n)?)
            }
            7 => {
                let n = length(&mut raw)?;
                HeaderValue::String(utf8(take(&mut raw, n)?)?)
            }
            8 => HeaderValue::Timestamp(i64::from_be_bytes(
                take(&mut raw, 8)?.try_into().map_err(|_| invalid())?,
            )),
            9 => HeaderValue::Uuid(take(&mut raw, 16)?.try_into().map_err(|_| invalid())?),
            _ => return Err(invalid()),
        };
        if headers.insert(name, value).is_some() {
            return Err(invalid());
        }
    }
    Ok(Event {
        headers,
        payload: &message[12 + header_length..total - 4],
    })
}
/// Encodes string headers and a raw payload in the supplied order.
///
/// This preserves the original string-only wire format. For binary or numeric
/// headers, use [`encode_typed`].
pub fn encode(
    headers: &[(&str, &str)],
    payload: &[u8],
    max_bytes: usize,
) -> Result<Vec<u8>, AwsError> {
    if headers.len() > 128 {
        return Err(AwsError::TooLarge);
    }
    let typed: Vec<_> = headers
        .iter()
        .map(|(name, value)| (*name, HeaderValue::String(value)))
        .collect();
    encode_typed(&typed, payload, max_bytes)
}

/// Encodes every primitive AWS EventStream header type, preserving caller order.
///
/// Header names must be unique and nonempty. A message has at most 128 headers,
/// 128 KiB of encoded headers, and `max_bytes` total bytes (including both CRCs).
/// Strings and byte arrays have the protocol's 16-bit length bound. Lengths are
/// checked before payload allocation or copying; integer and timestamp values
/// use big-endian representation. A timestamp is signed milliseconds since the
/// Unix epoch, not a textual date.
pub fn encode_typed(
    headers: &[(&str, HeaderValue<'_>)],
    payload: &[u8],
    max_bytes: usize,
) -> Result<Vec<u8>, AwsError> {
    let raw = encode_headers_typed(
        headers,
        max_bytes.saturating_sub(payload.len()).saturating_sub(16),
    )?;
    let total = raw
        .len()
        .checked_add(payload.len())
        .and_then(|v| v.checked_add(16))
        .ok_or(AwsError::TooLarge)?;
    if total > max_bytes || total > u32::MAX as usize {
        return Err(AwsError::TooLarge);
    }
    let mut message = Vec::with_capacity(total);
    message.extend_from_slice(&(total as u32).to_be_bytes());
    message.extend_from_slice(&(raw.len() as u32).to_be_bytes());
    message.extend_from_slice(&crc32fast::hash(&message).to_be_bytes());
    message.extend_from_slice(&raw);
    message.extend_from_slice(payload);
    message.extend_from_slice(&crc32fast::hash(&message).to_be_bytes());
    Ok(message)
}

// Shared with the opaque stream signer so the hashed timestamp header uses the
// exact same typed representation as the eventual signed outer message.
pub(crate) fn encode_headers_typed(
    headers: &[(&str, HeaderValue<'_>)],
    max_bytes: usize,
) -> Result<Vec<u8>, AwsError> {
    let mut raw = Vec::new();
    let mut names = std::collections::BTreeSet::new();
    if headers.len() > 128 {
        return Err(AwsError::TooLarge);
    }
    for (name, value) in headers {
        if name.is_empty() || name.len() > 255 || !names.insert(name) {
            return Err(invalid());
        }
        let value_len = match value {
            HeaderValue::Bool(_) => 0,
            HeaderValue::Byte(_) => 1,
            HeaderValue::Short(_) => 2,
            HeaderValue::Integer(_) => 4,
            HeaderValue::Long(_) | HeaderValue::Timestamp(_) => 8,
            HeaderValue::Uuid(_) => 16,
            HeaderValue::Bytes(value) => {
                if value.len() > u16::MAX as usize {
                    return Err(invalid());
                }
                value.len() + 2
            }
            HeaderValue::String(value) => {
                if value.len() > u16::MAX as usize {
                    return Err(invalid());
                }
                value.len() + 2
            }
        };
        let size = raw
            .len()
            .checked_add(name.len() + value_len + 2)
            .ok_or(AwsError::TooLarge)?;
        if size > 128 * 1024 || size > max_bytes {
            return Err(AwsError::TooLarge);
        }
        raw.push(name.len() as u8);
        raw.extend_from_slice(name.as_bytes());
        match value {
            HeaderValue::Bool(value) => raw.push(u8::from(!*value)),
            HeaderValue::Byte(value) => {
                raw.push(2);
                raw.push(*value as u8);
            }
            HeaderValue::Short(value) => {
                raw.push(3);
                raw.extend_from_slice(&value.to_be_bytes());
            }
            HeaderValue::Integer(value) => {
                raw.push(4);
                raw.extend_from_slice(&value.to_be_bytes());
            }
            HeaderValue::Long(value) => {
                raw.push(5);
                raw.extend_from_slice(&value.to_be_bytes());
            }
            HeaderValue::Bytes(value) => {
                raw.push(6);
                raw.extend_from_slice(&(value.len() as u16).to_be_bytes());
                raw.extend_from_slice(value);
            }
            HeaderValue::String(value) => {
                raw.push(7);
                raw.extend_from_slice(&(value.len() as u16).to_be_bytes());
                raw.extend_from_slice(value.as_bytes());
            }
            HeaderValue::Timestamp(value) => {
                raw.push(8);
                raw.extend_from_slice(&value.to_be_bytes());
            }
            HeaderValue::Uuid(value) => {
                raw.push(9);
                raw.extend_from_slice(value);
            }
        }
    }
    Ok(raw)
}
pub fn audio(payload: &[u8], max_bytes: usize) -> Result<Vec<u8>, AwsError> {
    encode(
        &[
            (":content-type", "application/octet-stream"),
            (":event-type", "AudioEvent"),
            (":message-type", "event"),
        ],
        payload,
        max_bytes,
    )
}
/// Validates a complete 12-byte prelude before an incremental reader allocates.
pub fn validate_prelude(prelude: &[u8], max_bytes: usize) -> Result<usize, AwsError> {
    if prelude.len() != 12 {
        return Err(invalid());
    }
    let total = u32::from_be_bytes(prelude[..4].try_into().map_err(|_| invalid())?) as usize;
    let headers = u32::from_be_bytes(prelude[4..8].try_into().map_err(|_| invalid())?) as usize;
    let crc = u32::from_be_bytes(prelude[8..12].try_into().map_err(|_| invalid())?);
    if total < 16
        || headers > total - 16
        || headers > 128 * 1024
        || crc32fast::hash(&prelude[..8]) != crc
    {
        return Err(invalid());
    }
    if total > max_bytes {
        return Err(AwsError::TooLarge);
    }
    Ok(total)
}
