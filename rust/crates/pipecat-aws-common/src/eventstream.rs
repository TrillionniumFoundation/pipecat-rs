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
/// Encodes string headers and a raw payload. Intended for fixture generation too.
pub fn encode(
    headers: &[(&str, &str)],
    payload: &[u8],
    max_bytes: usize,
) -> Result<Vec<u8>, AwsError> {
    let mut raw = Vec::new();
    let mut names = std::collections::BTreeSet::new();
    if headers.len() > 128 {
        return Err(AwsError::TooLarge);
    }
    for (name, value) in headers {
        if name.is_empty() || name.len() > 255 || value.len() > 65535 || !names.insert(name) {
            return Err(invalid());
        }
        let size = raw
            .len()
            .checked_add(name.len() + value.len() + 4)
            .ok_or(AwsError::TooLarge)?;
        if size > 128 * 1024 || size.saturating_add(payload.len()).saturating_add(16) > max_bytes {
            return Err(AwsError::TooLarge);
        }
        raw.push(name.len() as u8);
        raw.extend_from_slice(name.as_bytes());
        raw.push(7);
        raw.extend_from_slice(&(value.len() as u16).to_be_bytes());
        raw.extend_from_slice(value.as_bytes());
    }
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
