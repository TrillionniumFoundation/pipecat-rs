//! SigV4 primitives with explicit credentials and caller-selected fixed routes.
use crate::AwsError;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    net::IpAddr,
    time::{SystemTime, UNIX_EPOCH},
};

/// Explicit application-owned credentials. No ambient provider chain is consulted.
#[derive(Clone, PartialEq, Eq)]
pub struct AwsCredentials {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}
impl fmt::Debug for AwsCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AwsCredentials([redacted])")
    }
}
impl AwsCredentials {
    pub fn new(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Result<Self, AwsError> {
        let value = Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token,
        };
        if value.access_key.is_empty()
            || value.access_key.len() > 256
            || !value.access_key.bytes().all(|b| b.is_ascii_alphanumeric())
            || value.secret_key.is_empty()
            || value.secret_key.len() > 4096
            || !value.secret_key.bytes().all(|b| b.is_ascii_graphic())
            || value.session_token.as_ref().is_some_and(|s| {
                s.is_empty() || s.len() > 16384 || !s.bytes().all(|b| b.is_ascii_graphic())
            })
        {
            return Err(AwsError::Invalid("AWS credentials"));
        }
        Ok(value)
    }
}
/// Signed URLs contain temporary authentication material. Debug never prints it.
#[derive(Clone)]
pub struct PresignedUrl(String);
impl PresignedUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Debug for PresignedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PresignedUrl([redacted])")
    }
}
pub fn region(value: &str) -> Result<(), AwsError> {
    if value.is_empty()
        || value.len() > 63
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || value.starts_with('-')
        || value.ends_with('-')
    {
        return Err(AwsError::Invalid("AWS region"));
    }
    Ok(())
}
pub fn domain(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    }
}
pub fn endpoint(
    value: &str,
    expected: &str,
    path: &str,
    websocket: bool,
) -> Result<url::Url, AwsError> {
    let url = url::Url::parse(value).map_err(|_| AwsError::Invalid("AWS endpoint"))?;
    if url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != path
    {
        return Err(AwsError::Invalid("AWS endpoint route"));
    }
    let local = url.host_str().is_some_and(|h| {
        h.trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    });
    let secure = if websocket { "wss" } else { "https" };
    let plain = if websocket { "ws" } else { "http" };
    if value != expected && !(local && matches!(url.scheme(), s if s == secure || s == plain)) {
        return Err(AwsError::Invalid(
            "endpoint must be the regional AWS route or literal loopback",
        ));
    }
    Ok(url)
}
pub fn host(url: &url::Url) -> String {
    match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().into(),
    }
}
fn date(value: &str) -> Result<&str, AwsError> {
    if value.len() != 16
        || value.as_bytes()[8] != b'T'
        || value.as_bytes()[15] != b'Z'
        || !value
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 8 || i == 15 || b.is_ascii_digit())
        || chrono::NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ").is_err()
    {
        return Err(AwsError::Invalid("SigV4 timestamp"));
    }
    Ok(&value[..8])
}
pub fn now() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn hash(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}
fn mac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}
fn signature(
    credentials: &AwsCredentials,
    region: &str,
    service: &str,
    timestamp: &str,
    canonical: &str,
) -> Result<(String, String), AwsError> {
    let day = date(timestamp)?;
    let scope = format!("{day}/{region}/{service}/aws4_request");
    let kdate = mac(
        format!("AWS4{}", credentials.secret_key).as_bytes(),
        day.as_bytes(),
    );
    let kregion = mac(&kdate, region.as_bytes());
    let kservice = mac(&kregion, service.as_bytes());
    let ksigning = mac(&kservice, b"aws4_request");
    let text = format!(
        "AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{}",
        hash(canonical.as_bytes())
    );
    Ok((scope, hex(&mac(&ksigning, text.as_bytes()))))
}
fn encode(value: &str, double_equals: bool) -> String {
    let mut result = String::new();
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            result.push(b as char);
        } else if b == b'=' && double_equals {
            result.push_str("%253D");
        } else {
            result.push_str(&format!("%{b:02X}"));
        }
    }
    result
}
pub fn presign(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    mut params: BTreeMap<String, String>,
    timestamp: &str,
) -> Result<PresignedUrl, AwsError> {
    let day = date(timestamp)?;
    params.insert("X-Amz-Algorithm".into(), "AWS4-HMAC-SHA256".into());
    params.insert(
        "X-Amz-Credential".into(),
        format!(
            "{}/{day}/{region}/transcribe/aws4_request",
            credentials.access_key
        ),
    );
    params.insert("X-Amz-Date".into(), timestamp.into());
    params.insert("X-Amz-Expires".into(), "300".into());
    params.insert("X-Amz-SignedHeaders".into(), "host".into());
    if let Some(token) = &credentials.session_token {
        params.insert("X-Amz-Security-Token".into(), token.clone());
    }
    let query = params
        .iter()
        .map(|(k, v)| format!("{}={}", encode(k, false), encode(v, true)))
        .collect::<Vec<_>>()
        .join("&");
    let canonical = format!(
        "GET\n{}\n{query}\nhost:{}\n\nhost\n{}",
        url.path(),
        host(url),
        hash(b"")
    );
    let (_, sig) = signature(credentials, region, "transcribe", timestamp, &canonical)?;
    Ok(PresignedUrl(format!("{url}?{query}&X-Amz-Signature={sig}")))
}
pub fn signed_post_headers(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    body: &[u8],
    timestamp: &str,
    service: &str,
    canonical_uri: &str,
) -> Result<reqwest::header::HeaderMap, AwsError> {
    region_validate_service(region, service)?;
    if !canonical_uri.starts_with('/')
        || canonical_uri.contains(['\r', '\n', '?', '#'])
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(AwsError::Invalid("SigV4 canonical URI"));
    }
    let mut values = BTreeMap::from([
        ("content-type", "application/json".to_string()),
        ("host", host(url)),
        ("x-amz-content-sha256", hash(body)),
        ("x-amz-date", timestamp.to_string()),
    ]);
    if let Some(token) = &credentials.session_token {
        values.insert("x-amz-security-token", token.clone());
    }
    let names = values.keys().copied().collect::<Vec<_>>().join(";");
    let headers = values
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect::<String>();
    let canonical = format!(
        "POST\n{canonical_uri}\n\n{headers}\n{names}\n{}",
        hash(body)
    );
    let (scope, sig) = signature(credentials, region, service, timestamp, &canonical)?;
    values.insert(
        "authorization",
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={sig}",
            credentials.access_key
        ),
    );
    let mut headers = reqwest::header::HeaderMap::new();
    for (key, value) in values {
        let mut value = reqwest::header::HeaderValue::from_str(&value)
            .map_err(|_| AwsError::Invalid("AWS signed header"))?;
        if matches!(key, "authorization" | "x-amz-security-token") {
            value.set_sensitive(true);
        }
        headers.insert(key, value);
    }
    Ok(headers)
}

/// Signs an ordinary JSON POST using its actual body hash and extra headers.
///
/// This is a distinct profile from both [`signed_post_headers`] (which adds a
/// content SHA header) and [`signed_event_stream_post_headers`] (empty payload).
/// It emits JSON content type, host, date, optional security token, all supplied
/// extra headers, and authorization. No content SHA or framing header is added.
///
/// `canonical_uri` must exactly match the single-encoded wire path encoded once
/// more for SigV4, preserving `/` separators. The wire path must use uppercase
/// RFC3986 escapes for reserved/non-ASCII bytes; malformed or redundant escapes,
/// encoded percent/query/fragment/backslash/control bytes, dot segments, repeated
/// slashes, userinfo, queries, fragments, and non-HTTP URLs are rejected. A parsed
/// URL cannot reveal normalization performed before this call: callers must
/// separately validate their original route and regional/loopback endpoint policy.
/// Signing is not endpoint authorization.
///
/// Body bytes are limited to 16 MiB, URLs to 8192 bytes, and extra headers to 16,
/// with 128-byte names, 4096-byte values, and 8192 total name/value bytes. Names
/// are case-insensitive; duplicates, signing/framing overrides and SDK-excluded
/// transport headers are rejected. Values must be printable ASCII; spaces are
/// trimmed and collapsed before both signing and emission. Extra values, tokens,
/// and authorization are marked sensitive to prevent HeaderMap Debug disclosure.
#[allow(clippy::too_many_arguments)]
pub fn signed_body_post_headers(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    body: &[u8],
    timestamp: &str,
    service: &str,
    canonical_uri: &str,
    extra_headers: &[(&str, &str)],
) -> Result<reqwest::header::HeaderMap, AwsError> {
    region_validate_service(region, service)?;
    date(timestamp)?;
    if body.len() > 16 * 1024 * 1024 || extra_headers.len() > 16 || canonical_uri.len() > 3 * 8192 {
        return Err(AwsError::TooLarge);
    }
    if body_post_canonical_uri(url)? != canonical_uri {
        return Err(AwsError::Invalid("SigV4 canonical URI"));
    }
    let mut values = BTreeMap::from([
        ("content-type".to_string(), "application/json".to_string()),
        ("host".to_string(), host(url)),
        ("x-amz-date".to_string(), timestamp.to_string()),
    ]);
    if let Some(token) = &credentials.session_token {
        values.insert("x-amz-security-token".into(), token.clone());
    }
    let mut extra_bytes = 0usize;
    for &(name, value) in extra_headers {
        if name.len() > 128 || value.len() > 4096 {
            return Err(AwsError::TooLarge);
        }
        extra_bytes += name.len() + value.len();
        if extra_bytes > 8192 {
            return Err(AwsError::TooLarge);
        }
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| AwsError::Invalid("AWS signed header name"))?;
        let name = name.as_str();
        if matches!(
            name,
            "authorization"
                | "content-type"
                | "host"
                | "date"
                | "x-amz-date"
                | "x-amz-content-sha256"
                | "x-amz-security-token"
                | "content-length"
                | "connection"
                | "expect"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
                | "user-agent"
                | "x-amzn-trace-id"
        ) || values.contains_key(name)
        {
            return Err(AwsError::Invalid("AWS reserved or duplicate signed header"));
        }
        if !value.bytes().all(|byte| (b' '..=b'~').contains(&byte)) {
            return Err(AwsError::Invalid("AWS signed header value"));
        }
        let value = value
            .split(' ')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join(" ");
        if value.is_empty() {
            return Err(AwsError::Invalid("AWS signed header value"));
        }
        values.insert(name.to_string(), value);
    }
    let names = values
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(";");
    let header_text = values
        .iter()
        .map(|(key, value)| format!("{key}:{value}\n"))
        .collect::<String>();
    let canonical = format!(
        "POST\n{canonical_uri}\n\n{header_text}\n{names}\n{}",
        hash(body)
    );
    let (scope, signature) = signature(credentials, region, service, timestamp, &canonical)?;
    values.insert(
        "authorization".into(),
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={signature}",
            credentials.access_key
        ),
    );
    let mut headers = reqwest::header::HeaderMap::new();
    for (key, value) in values {
        let mut value = reqwest::header::HeaderValue::from_str(&value)
            .map_err(|_| AwsError::Invalid("AWS signed header"))?;
        if !matches!(key.as_str(), "content-type" | "host" | "x-amz-date") {
            value.set_sensitive(true);
        }
        let key = reqwest::header::HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| AwsError::Invalid("AWS signed header name"))?;
        headers.insert(key, value);
    }
    Ok(headers)
}

fn body_post_canonical_uri(url: &url::Url) -> Result<String, AwsError> {
    if url.as_str().len() > 8192 {
        return Err(AwsError::TooLarge);
    }
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.path().starts_with('/')
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(AwsError::Invalid("SigV4 body POST URI"));
    }
    let unreserved =
        |byte: u8| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~');
    let mut decoded = Vec::with_capacity(url.path().len());
    let mut bytes = url.path().bytes();
    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let high = bytes.next().and_then(uppercase_hex_digit);
            let low = bytes.next().and_then(uppercase_hex_digit);
            let value = match (high, low) {
                (Some(high), Some(low)) => high * 16 + low,
                _ => return Err(AwsError::Invalid("SigV4 body POST URI escape")),
            };
            if unreserved(value)
                || value.is_ascii_control()
                || matches!(value, b'%' | b'?' | b'#' | b'\\')
            {
                return Err(AwsError::Invalid("SigV4 body POST URI escape"));
            }
            decoded.push(value);
        } else if byte == b'/' || unreserved(byte) {
            decoded.push(byte);
        } else {
            return Err(AwsError::Invalid("SigV4 body POST URI byte"));
        }
    }
    let decoded = std::str::from_utf8(&decoded)
        .map_err(|_| AwsError::Invalid("SigV4 body POST URI UTF-8"))?;
    if decoded.contains("//") || decoded.split('/').any(|part| matches!(part, "." | "..")) {
        return Err(AwsError::Invalid("SigV4 body POST URI segments"));
    }
    Ok(url
        .path()
        .split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/"))
}

fn uppercase_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

// 16-byte frame overhead + the binary signature (52 bytes) and timestamp
// (15 bytes) headers. This is independent of the nested message's size.
const SIGNED_EVENT_OVERHEAD: usize = 83;
const MAX_SIGNED_EVENT_BYTES: usize = 16 * 1024 * 1024;

/// Single-session SigV4 EventStream chain with opaque, redacted credentials.
///
/// Construct this together with its signed HTTP headers using
/// [`signed_event_stream_post_headers`]. The state is deliberately not `Clone`:
/// one writer owns one chain. Every successful signing call advances the chain;
/// a transport must not retry a possibly admitted frame or reuse this state for
/// another connection. This type does not send, retry, or reconnect anything.
///
/// The secret is retained privately to derive each event's signing key from that
/// event's UTC day, including a session crossing midnight. No secret or signing
/// key accessor is exposed. Debug output never includes credentials/signatures.
pub struct EventStreamSigner {
    secret_key: String,
    region: String,
    service: String,
    previous_signature: String,
    max_message_bytes: usize,
    finalized: bool,
}

impl fmt::Debug for EventStreamSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EventStreamSigner([redacted])")
    }
}

impl EventStreamSigner {
    /// Signs one complete encoded inner EventStream message, not bare JSON/PCM.
    ///
    /// Validates both inner CRCs, headers, and the complete outer size before
    /// advancing the chain. A local validation failure leaves the chain intact.
    /// The signed outer headers are binary `:chunk-signature`, then `:date`.
    /// Both the timestamp header and string-to-sign truncate subsecond time.
    pub fn sign_message(
        &mut self,
        encoded_inner: &[u8],
        timestamp: SystemTime,
    ) -> Result<Vec<u8>, AwsError> {
        self.ensure_open()?;
        if encoded_inner.len() > self.max_message_bytes - SIGNED_EVENT_OVERHEAD {
            return Err(AwsError::TooLarge);
        }
        crate::eventstream::decode(encoded_inner, self.max_message_bytes)?;
        self.sign_payload(encoded_inner, timestamp, false)
    }

    /// Emits the one signed terminal with an actually zero-byte outer payload.
    ///
    /// This is not an encoded empty inner message. Successful finalization is
    /// irreversible: later message or terminal signing fails. If transmission
    /// subsequently fails or is canceled, the transport must invalidate the
    /// session instead of emitting another terminal.
    pub fn sign_empty(&mut self, timestamp: SystemTime) -> Result<Vec<u8>, AwsError> {
        self.ensure_open()?;
        self.sign_payload(&[], timestamp, true)
    }

    fn ensure_open(&self) -> Result<(), AwsError> {
        if self.finalized {
            return Err(AwsError::Protocol("AWS eventstream signer finalized"));
        }
        Ok(())
    }

    fn sign_payload(
        &mut self,
        payload: &[u8],
        time: SystemTime,
        terminal: bool,
    ) -> Result<Vec<u8>, AwsError> {
        use crate::eventstream::{encode_typed, HeaderValue};
        let (timestamp, millis) = stream_timestamp(time)?;
        let day = date(&timestamp)?;
        let kdate = mac(
            format!("AWS4{}", self.secret_key).as_bytes(),
            day.as_bytes(),
        );
        let kregion = mac(&kdate, self.region.as_bytes());
        let kservice = mac(&kregion, self.service.as_bytes());
        let ksigning = mac(&kservice, b"aws4_request");
        let text = stream_string_to_sign(
            payload,
            &self.previous_signature,
            &timestamp,
            millis,
            &self.region,
            &self.service,
        )?;
        let signature = mac(&ksigning, text.as_bytes());
        let message = encode_typed(
            &[
                (":chunk-signature", HeaderValue::Bytes(&signature)),
                (":date", HeaderValue::Timestamp(millis)),
            ],
            payload,
            self.max_message_bytes,
        )?;
        self.previous_signature = hex(&signature);
        self.finalized = terminal;
        Ok(message)
    }
}

/// Signs a POST request with the SDK's empty-body EventStream signing profile.
///
/// This is additive to [`signed_post_headers`], whose JSON/body-hash behavior is
/// unchanged. The returned sensitive headers have EventStream MIME, host, date,
/// optional session token and authorization, but no `x-amz-content-sha256`.
/// The URI is derived from the already single-encoded wire URL and encoded again
/// for SigV4 while preserving `/` separators (e.g. `%3A` becomes `%253A`).
/// Query, fragment, userinfo, repeated-slash paths and non-HTTP URLs are rejected.
/// The caller must separately enforce its regional or literal-loopback endpoint
/// policy before network use; signing is not endpoint authorization.
///
/// `max_message_bytes` bounds every complete signed outer frame, from 83 bytes
/// (terminal only) through 16 MiB. Request URLs are bounded to 8192 bytes. Times
/// must be on/after the Unix epoch and have a four-digit UTC year; subseconds are
/// truncated consistently for request and event signing.
pub fn signed_event_stream_post_headers(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    timestamp: SystemTime,
    service: &str,
    max_message_bytes: usize,
) -> Result<(reqwest::header::HeaderMap, EventStreamSigner), AwsError> {
    event_stream_post_headers(
        url,
        region,
        credentials,
        timestamp,
        service,
        max_message_bytes,
        None,
    )
}

/// Sign the two SageMaker model-routing headers as part of the stream seed.
/// This finite helper rejects duplicate keys, empty values and ambiguous routing
/// grammar. It does not authorize the endpoint. Routing values are sensitive.
#[allow(clippy::too_many_arguments)]
pub fn signed_sagemaker_event_stream_post_headers(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    timestamp: SystemTime,
    max_message_bytes: usize,
    model_invocation_path: &str,
    model_query_string: &str,
) -> Result<(reqwest::header::HeaderMap, EventStreamSigner), AwsError> {
    let unreserved = |b: u8| b.is_ascii_alphanumeric() || b"-._~".contains(&b);
    if model_invocation_path.is_empty()
        || model_invocation_path.len() > 100
        || model_invocation_path.split('/').any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._".contains(&b))
        })
        || model_query_string.is_empty()
        || model_query_string.len() > 2048
    {
        return Err(AwsError::Invalid("SageMaker model routing"));
    }
    let mut keys = std::collections::BTreeSet::new();
    for pair in model_query_string.split('&') {
        let (key, value) = pair
            .split_once('=')
            .ok_or(AwsError::Invalid("SageMaker model query"))?;
        if key.is_empty() || !key.bytes().all(unreserved) || value.is_empty() || !keys.insert(key) {
            return Err(AwsError::Invalid("SageMaker model query"));
        }
        let bytes = value.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            if unreserved(bytes[index]) {
                index += 1;
            } else if bytes[index] == b'%'
                && index + 2 < bytes.len()
                && bytes[index + 1].is_ascii_hexdigit()
                && bytes[index + 2].is_ascii_hexdigit()
            {
                index += 3;
            } else {
                return Err(AwsError::Invalid("SageMaker model query"));
            }
        }
    }
    event_stream_post_headers(
        url,
        region,
        credentials,
        timestamp,
        "sagemaker",
        max_message_bytes,
        Some((model_invocation_path, model_query_string)),
    )
}

#[allow(clippy::too_many_arguments)]
fn event_stream_post_headers(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    timestamp: SystemTime,
    service: &str,
    max_message_bytes: usize,
    routing: Option<(&str, &str)>,
) -> Result<(reqwest::header::HeaderMap, EventStreamSigner), AwsError> {
    region_validate_service(region, service)?;
    if !(SIGNED_EVENT_OVERHEAD..=MAX_SIGNED_EVENT_BYTES).contains(&max_message_bytes) {
        return Err(AwsError::Invalid("AWS eventstream message limit"));
    }
    if url.as_str().len() > 8192
        || !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.path().starts_with('/')
        || url.path().contains("//")
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(AwsError::Invalid("SigV4 eventstream URI"));
    }
    let (timestamp, _) = stream_timestamp(timestamp)?;
    let canonical_uri = url
        .path()
        .split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/");
    let mut values = BTreeMap::from([
        (
            "content-type",
            "application/vnd.amazon.eventstream".to_string(),
        ),
        ("host", host(url)),
        ("x-amz-date", timestamp.clone()),
    ]);
    if let Some(token) = &credentials.session_token {
        values.insert("x-amz-security-token", token.clone());
    }
    if let Some((path, query)) = routing {
        values.insert("x-amzn-sagemaker-model-invocation-path", path.into());
        values.insert("x-amzn-sagemaker-model-query-string", query.into());
    }
    let names = values.keys().copied().collect::<Vec<_>>().join(";");
    let header_text = values
        .iter()
        .map(|(key, value)| format!("{key}:{value}\n"))
        .collect::<String>();
    let canonical = format!(
        "POST\n{canonical_uri}\n\n{header_text}\n{names}\n{}",
        hash(b"")
    );
    let (scope, signature) = signature(credentials, region, service, &timestamp, &canonical)?;
    values.insert(
        "authorization",
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={names}, Signature={signature}",
            credentials.access_key
        ),
    );
    let mut headers = reqwest::header::HeaderMap::new();
    for (key, value) in values {
        let mut value = reqwest::header::HeaderValue::from_str(&value)
            .map_err(|_| AwsError::Invalid("AWS signed header"))?;
        if matches!(
            key,
            "authorization"
                | "x-amz-security-token"
                | "x-amzn-sagemaker-model-invocation-path"
                | "x-amzn-sagemaker-model-query-string"
        ) {
            value.set_sensitive(true);
        }
        headers.insert(key, value);
    }
    Ok((
        headers,
        EventStreamSigner {
            secret_key: credentials.secret_key.clone(),
            region: region.into(),
            service: service.into(),
            previous_signature: signature,
            max_message_bytes,
            finalized: false,
        },
    ))
}

fn stream_timestamp(time: SystemTime) -> Result<(String, i64), AwsError> {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AwsError::Invalid("SigV4 timestamp"))?
        .as_secs();
    let seconds = i64::try_from(seconds).map_err(|_| AwsError::Invalid("SigV4 timestamp"))?;
    let value = chrono::DateTime::<chrono::Utc>::from_timestamp(seconds, 0)
        .ok_or(AwsError::Invalid("SigV4 timestamp"))?;
    let timestamp = value.format("%Y%m%dT%H%M%SZ").to_string();
    date(&timestamp)?;
    let millis = seconds
        .checked_mul(1000)
        .ok_or(AwsError::Invalid("SigV4 timestamp"))?;
    Ok((timestamp, millis))
}

fn stream_string_to_sign(
    payload: &[u8],
    previous_signature: &str,
    timestamp: &str,
    millis: i64,
    region: &str,
    service: &str,
) -> Result<String, AwsError> {
    use crate::eventstream::{encode_headers_typed, HeaderValue};
    let day = date(timestamp)?;
    let date_header = encode_headers_typed(&[(":date", HeaderValue::Timestamp(millis))], 15)?;
    Ok(format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{day}/{region}/{service}/aws4_request\n{previous_signature}\n{}\n{}",
        hash(&date_header),
        hash(payload)
    ))
}

fn region_validate_service(value: &str, service: &str) -> Result<(), AwsError> {
    region(value)?;
    if service.is_empty()
        || service.len() > 63
        || !service
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(AwsError::Invalid("SigV4 service"));
    }
    Ok(())
}
/// RFC3986 component encoding. Slashes are encoded as data.
pub fn encode_component(value: &str) -> String {
    encode(value, false)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn credentials() -> AwsCredentials {
        AwsCredentials::new(
            "AKIDEXAMPLE",
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            Some("TOKEN/+=".into()),
        )
        .unwrap()
    }
    #[test]
    fn event_stream_string_to_sign_matches_published_aws_sdk_vector() {
        // Retained primary source: aws-sigv4 1.6.0, event_stream.rs::tests::string_to_sign.
        // The source's 1234 nanoseconds truncate to zero before signing.
        let inner =
            crate::eventstream::encode(&[("some-header", "value")], b"test payload", 1024).unwrap();
        let (timestamp, millis) =
            stream_timestamp(UNIX_EPOCH + std::time::Duration::new(123_456_789, 1234)).unwrap();
        let actual = stream_string_to_sign(
            &inner,
            &hash(b"last message sts"),
            &timestamp,
            millis,
            "us-east-1",
            "testservice",
        )
        .unwrap();
        assert_eq!(
            actual,
            include_str!("../tests/fixtures/aws_sdk_event_stream_string_to_sign.txt").trim_end()
        );
    }
    #[test]
    fn polly_sigv4_matches_independent_fixed_date_fixture() {
        // Independently generated with stdlib HMAC/SHA256 over the documented canonical request.
        let url = url::Url::parse("https://polly.us-east-1.amazonaws.com/v1/speech").unwrap();
        let headers = signed_post_headers(
            &url,
            "us-east-1",
            &credentials(),
            br#"{"Text":"hello","OutputFormat":"pcm"}"#,
            "20241010T010203Z",
            "polly",
            "/v1/speech",
        )
        .unwrap();
        assert!(headers["authorization"].to_str().unwrap().ends_with(
            "Signature=a5b8fbe19200b38348ce654d270277b2996cfeedccba4648c91bc9d51970b3c4"
        ));
        assert!(headers["authorization"].is_sensitive());
        assert!(headers["x-amz-security-token"].is_sensitive());
        let debug = format!("{headers:?}");
        assert!(!debug.contains("AKIDEXAMPLE"));
        assert!(!debug.contains("TOKEN"));
    }
    #[test]
    fn credential_and_timestamp_validation_never_echo_input() {
        let error = AwsCredentials::new("INJECT\nHEADER", "secret", None).unwrap_err();
        assert!(!format!("{error:?}").contains("INJECT"));
        let c = credentials();
        assert!(!format!("{c:?}").contains("AKID"));
        for invalid in [
            "",
            "20240230T010203Z",
            "20241010T256161Z",
            "20241010T010203Zjunk",
            "秘密20241010T01",
        ] {
            assert!(date(invalid).is_err());
        }
    }
}
