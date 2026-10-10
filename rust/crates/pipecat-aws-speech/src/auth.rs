//! SigV4 primitives for the fixed Polly and Transcribe service routes.
use crate::StreamingError;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt, net::IpAddr};

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
    ) -> Result<Self, StreamingError> {
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
            return Err(StreamingError::Invalid("AWS credentials"));
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
pub(crate) fn region(value: &str) -> Result<(), StreamingError> {
    if value.is_empty()
        || value.len() > 63
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || value.starts_with('-')
        || value.ends_with('-')
    {
        return Err(StreamingError::Invalid("AWS region"));
    }
    Ok(())
}
pub(crate) fn domain(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    }
}
pub(crate) fn endpoint(
    value: &str,
    expected: &str,
    path: &str,
    websocket: bool,
) -> Result<url::Url, StreamingError> {
    let url = url::Url::parse(value).map_err(|_| StreamingError::Invalid("AWS endpoint"))?;
    if url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != path
    {
        return Err(StreamingError::Invalid("AWS endpoint route"));
    }
    let local = url.host_str().is_some_and(|h| {
        h.trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
    });
    let secure = if websocket { "wss" } else { "https" };
    let plain = if websocket { "ws" } else { "http" };
    if value != expected && !(local && matches!(url.scheme(), s if s == secure || s == plain)) {
        return Err(StreamingError::Invalid(
            "endpoint must be the regional AWS route or literal loopback",
        ));
    }
    Ok(url)
}
pub(crate) fn host(url: &url::Url) -> String {
    match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().into(),
    }
}
fn date(value: &str) -> Result<&str, StreamingError> {
    if value.len() != 16
        || value.as_bytes()[8] != b'T'
        || value.as_bytes()[15] != b'Z'
        || !value
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 8 || i == 15 || b.is_ascii_digit())
        || chrono::NaiveDateTime::parse_from_str(value, "%Y%m%dT%H%M%SZ").is_err()
    {
        return Err(StreamingError::Invalid("SigV4 timestamp"));
    }
    Ok(&value[..8])
}
pub(crate) fn now() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub(crate) fn hash(bytes: &[u8]) -> String {
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
) -> Result<(String, String), StreamingError> {
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
pub(crate) fn presign(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    mut params: BTreeMap<String, String>,
    timestamp: &str,
) -> Result<PresignedUrl, StreamingError> {
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
pub(crate) fn polly_headers(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    body: &[u8],
    timestamp: &str,
) -> Result<reqwest::header::HeaderMap, StreamingError> {
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
    let canonical = format!("POST\n{}\n\n{headers}\n{names}\n{}", url.path(), hash(body));
    let (scope, sig) = signature(credentials, region, "polly", timestamp, &canonical)?;
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
            .map_err(|_| StreamingError::Invalid("AWS signed header"))?;
        if matches!(key, "authorization" | "x-amz-security-token") {
            value.set_sensitive(true);
        }
        headers.insert(key, value);
    }
    Ok(headers)
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
    fn polly_sigv4_matches_independent_fixed_date_fixture() {
        // Independently generated with stdlib HMAC/SHA256 over the documented canonical request.
        let url = url::Url::parse("https://polly.us-east-1.amazonaws.com/v1/speech").unwrap();
        let headers = polly_headers(
            &url,
            "us-east-1",
            &credentials(),
            br#"{"Text":"hello","OutputFormat":"pcm"}"#,
            "20241010T010203Z",
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
