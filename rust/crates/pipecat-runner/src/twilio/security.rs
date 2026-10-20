//! Narrow, deterministic Twilio request authentication.
//!
//! Endpoint URLs come from trusted configuration, never request headers. Only
//! one exact configured URL is signed: no proxy, port, scheme, or trailing-slash
//! alternatives are attempted. Callers enforce route paths, request/body limits,
//! content type, account/call binding, and bounded capability/replay policy.
//! A valid Twilio signature by itself provides no freshness guarantee.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use std::collections::BTreeMap;
use tokio_tungstenite::tungstenite::http::HeaderMap;
use url::{Host, Url};

/// Validate a configured HTTP (`kind == "http"`) or WebSocket (`kind == "ws"`)
/// endpoint without silently changing the exact spelling used for signatures.
///
/// Parsing can normalize host case, default ports, IPv4 aliases, dot segments,
/// or a missing root slash. Those spellings are deliberately rejected rather
/// than signing a normalized URL that differs from the configured public URL.
/// Percent escapes that the URL parser preserves remain unchanged; callers must
/// separately require their exact supported path.
pub(super) fn validate_url(raw: &str, kind: &str) -> Result<Url, &'static str> {
    let url = Url::parse(raw).map_err(|_| "invalid endpoint URL")?;
    let secure = match (kind, url.scheme()) {
        ("http", "https") | ("ws", "wss") => true,
        ("http", "http") | ("ws", "ws") => false,
        _ => return Err("endpoint URL has an unsupported scheme"),
    };
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("endpoint URL must not contain credentials, query, or fragment");
    }
    let loopback = match url.host() {
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        Some(Host::Domain(_)) => false,
        None => return Err("endpoint URL must have a host"),
    };
    if !secure && !loopback {
        return Err("insecure endpoint URL requires a literal loopback IP address");
    }
    if raw != url.as_str() {
        return Err("endpoint URL must use exact canonical spelling");
    }
    Ok(url)
}

/// Require the expected two-letter ASCII prefix followed by 32 ASCII hex digits.
pub(super) fn valid_sid(value: &str, prefix: &str) -> bool {
    prefix.len() == 2
        && prefix.bytes().all(|byte| byte.is_ascii_alphabetic())
        && value.strip_prefix(prefix).is_some_and(|suffix| {
            suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

/// Decode a bounded-by-caller application/x-www-form-urlencoded body.
///
/// All received fields are retained, including unknown fields and empty values.
/// A bare nonempty field name has an empty value. An empty body has no fields;
/// empty field names/segments and duplicate *decoded* names are rejected. This
/// intentionally does not support repeated-value form parameters.
pub(super) fn parse_form(body: &[u8]) -> Result<BTreeMap<String, String>, &'static str> {
    let mut fields = BTreeMap::new();
    if body.is_empty() {
        return Ok(fields);
    }
    for field in body.split(|byte| *byte == b'&') {
        let (name, value) = match field.iter().position(|byte| *byte == b'=') {
            Some(index) => (&field[..index], &field[index + 1..]),
            None => (field, &[][..]),
        };
        let name = decode_form_component(name)?;
        if name.is_empty() {
            return Err("form field name must not be empty");
        }
        let value = decode_form_component(value)?;
        if fields.insert(name, value).is_some() {
            return Err("duplicate form field names are unsupported");
        }
    }
    Ok(fields)
}

fn decode_form_component(raw: &[u8]) -> Result<String, &'static str> {
    let mut decoded = Vec::with_capacity(raw.len());
    let mut index = 0;
    while index < raw.len() {
        match raw[index] {
            b'+' => decoded.push(b' '),
            b'%' => {
                let high = raw.get(index + 1).and_then(|byte| hex_digit(*byte));
                let low = raw.get(index + 2).and_then(|byte| hex_digit(*byte));
                match (high, low) {
                    (Some(high), Some(low)) => decoded.push((high << 4) | low),
                    _ => return Err("malformed form percent encoding"),
                }
                index += 2;
            }
            byte => decoded.push(byte),
        }
        index += 1;
    }
    String::from_utf8(decoded).map_err(|_| "form fields must be valid UTF-8")
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Authenticate the exact configured URL and every decoded field, sorted by
/// name. HTTP header names are case-insensitive, but duplicate header values and
/// comma-joined signatures are rejected, even if the signatures are identical.
///
/// Only standard, canonically padded Base64 encoding of a full SHA1 MAC is
/// supported. HMAC's `verify_slice` performs the MAC comparison in constant time;
/// the preceding format checks operate only on the public supplied signature.
pub(super) fn verify_signature(
    secret: &str,
    canonical_url: &str,
    fields: &BTreeMap<String, String>,
    headers: &HeaderMap,
) -> bool {
    if secret.is_empty() {
        return false;
    }
    let mut values = headers.get_all("x-twilio-signature").iter();
    let Some(value) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let Ok(signature) = value.to_str() else {
        return false;
    };
    // SHA1 is exactly 20 bytes, or 28 Base64 characters with one padding byte.
    if signature.len() != 28 {
        return false;
    }
    let Ok(decoded) = STANDARD.decode(signature) else {
        return false;
    };
    if decoded.len() != 20 || STANDARD.encode(&decoded) != signature {
        return false;
    }
    let Ok(mut mac) = Hmac::<Sha1>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(canonical_url.as_bytes());
    for (name, value) in fields {
        mac.update(name.as_bytes());
        mac.update(value.as_bytes());
    }
    mac.verify_slice(&decoded).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::http::HeaderValue;

    // Official illustrative vector, not credentials for any real account:
    // https://www.twilio.com/docs/usage/security#explore-the-algorithm-yourself
    // Retrieved 2026-10-20. The pure verifier also accepts this query-bearing
    // vector; application configuration intentionally rejects query-bearing URLs.
    const VECTOR_SECRET: &str = "12345";
    const VECTOR_URL: &str = "https://example.com/myapp.php?foo=1&bar=2";
    const VECTOR_SIGNATURE: &str = "L/OH5YylLD5NRKLltdqwSvS0BnU=";

    fn vector_fields() -> BTreeMap<String, String> {
        parse_form(
            b"To=%2B18005551212&From=%2B14158675310&Caller=%2B14158675310\
              &Digits=1234&CallSid=CA1234567890ABCDE",
        )
        .unwrap()
    }

    fn signature_headers(signature: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Twilio-Signature",
            HeaderValue::from_str(signature).unwrap(),
        );
        headers
    }

    fn signed_headers(secret: &str, url: &str, fields: &BTreeMap<String, String>) -> HeaderMap {
        let mut mac = Hmac::<Sha1>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(url.as_bytes());
        for (name, value) in fields {
            mac.update(name.as_bytes());
            mac.update(value.as_bytes());
        }
        signature_headers(&STANDARD.encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn accepts_official_twilio_signature_vector() {
        assert!(verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &vector_fields(),
            &signature_headers(VECTOR_SIGNATURE)
        ));
    }

    #[test]
    fn rejects_changed_fields_secret_and_exact_url() {
        let headers = signature_headers(VECTOR_SIGNATURE);
        let fields = vector_fields();
        assert!(!verify_signature("54321", VECTOR_URL, &fields, &headers));
        assert!(!verify_signature("", VECTOR_URL, &fields, &headers));
        for url in [
            "http://example.com/myapp.php?foo=1&bar=2",
            "https://example.com/myapp.php?bar=2&foo=1",
            "https://example.com/myapp.php/?foo=1&bar=2",
            "https://example.com:443/myapp.php?foo=1&bar=2",
        ] {
            assert!(!verify_signature(VECTOR_SECRET, url, &fields, &headers));
        }
        let mut changed = fields.clone();
        changed.insert("Digits".into(), "1235".into());
        assert!(!verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &changed,
            &headers
        ));
        changed = fields.clone();
        changed.remove("Caller");
        assert!(!verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &changed,
            &headers
        ));
    }

    #[test]
    fn unknown_fields_and_untrimmed_unicode_values_participate_in_signature() {
        let mut fields = vector_fields();
        fields.insert("FutureTwilioField".into(), " + café \n".into());
        let headers = signed_headers(VECTOR_SECRET, VECTOR_URL, &fields);
        assert!(verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &fields,
            &headers
        ));
        assert!(!verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &fields,
            &signature_headers(VECTOR_SIGNATURE)
        ));
        fields.insert("FutureTwilioField".into(), "+ café".into());
        assert!(!verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &fields,
            &headers
        ));
    }

    #[test]
    fn empty_fields_sign_exact_websocket_url_without_http_rewrite_or_slash_guess() {
        let fields = BTreeMap::new();
        let url = "wss://example.com/twilio/media";
        let headers = signed_headers(VECTOR_SECRET, url, &fields);
        assert!(verify_signature(VECTOR_SECRET, url, &fields, &headers));
        for changed in [
            "https://example.com/twilio/media",
            "wss://example.com/twilio/media/",
        ] {
            assert!(!verify_signature(VECTOR_SECRET, changed, &fields, &headers));
        }
    }

    #[test]
    fn rejects_missing_duplicate_or_coalesced_signature_headers() {
        let fields = vector_fields();
        assert!(!verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &fields,
            &HeaderMap::new()
        ));
        let mut headers = signature_headers(VECTOR_SIGNATURE);
        headers.append(
            "x-twilio-signature",
            HeaderValue::from_static(VECTOR_SIGNATURE),
        );
        assert!(!verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &fields,
            &headers
        ));
        let combined = format!("{VECTOR_SIGNATURE}, {VECTOR_SIGNATURE}");
        assert!(!verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &fields,
            &signature_headers(&combined)
        ));
    }

    #[test]
    fn rejects_noncanonical_base64_wrong_lengths_and_mac_mismatch() {
        let fields = vector_fields();
        for signature in [
            "",
            "L/OH5YylLD5NRKLltdqwSvS0BnU",   // Missing padding.
            "L/OH5YylLD5NRKLltdqwSvS0BnU==", // Excess padding.
            "L_OH5YylLD5NRKLltdqwSvS0BnU=",  // URL-safe alphabet.
            "L/OH5YylLD5NRKLltdqwSvS0BnV=",  // Nonzero unused trailing bits.
            "L/OH5YylLD5NRKLltdqwSvS0BnU ",  // Whitespace instead of padding.
            "L/OH5YylLD5NRKLltdqwSvS0BnUA",  // Decodes to 21 bytes.
            "L/OH5YylLD5NRKLltdqwSvS0Bw==",  // Decodes to 19 bytes.
            "AAAAAAAAAAAAAAAAAAAAAAAAAAA=",  // Canonical but wrong MAC.
        ] {
            assert!(!verify_signature(
                VECTOR_SECRET,
                VECTOR_URL,
                &fields,
                &signature_headers(signature)
            ));
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-twilio-signature",
            HeaderValue::from_bytes(&[0xff; 28]).unwrap(),
        );
        assert!(!verify_signature(
            VECTOR_SECRET,
            VECTOR_URL,
            &fields,
            &headers
        ));
    }

    #[test]
    fn form_decoding_is_lossless_and_sorts_decoded_names() {
        let fields = parse_form(
            b"z=last&A=%2B+%20&Empty=&Bare&Equals=a=b%3Dc&Encoded%4Eame=%E9%9B%AA\
              &Whitespace=+%09%0D%0A+&Reserved=%26%3D%25&Lowercase=%2b",
        )
        .unwrap();
        assert_eq!(fields["A"], "+  ");
        assert_eq!(fields["Empty"], "");
        assert_eq!(fields["Bare"], "");
        assert_eq!(fields["Equals"], "a=b=c");
        assert_eq!(fields["EncodedName"], "雪");
        assert_eq!(fields["Whitespace"], " \t\r\n ");
        assert_eq!(fields["Reserved"], "&=%");
        assert_eq!(fields["Lowercase"], "+");
        assert_eq!(fields.keys().next().unwrap(), "A");
        assert_eq!(fields.keys().next_back().unwrap(), "z");
        assert!(parse_form(b"").unwrap().is_empty());
        assert_eq!(parse_form("Name=café".as_bytes()).unwrap()["Name"], "café");
    }

    #[test]
    fn form_rejects_duplicates_after_decoding_and_empty_names() {
        for body in [
            "CallSid=a&CallSid=b",
            "CallSid=a&%43allSid=b",
            "Future=a&Future=a",
            "a+b=1&a%20b=2",
            "=value",
            "&",
            "a=1&",
            "&a=1",
            "a=1&&b=2",
        ] {
            assert!(parse_form(body.as_bytes()).is_err(), "{body}");
        }
    }

    #[test]
    fn form_rejects_malformed_escapes_and_invalid_utf8_without_replacement() {
        for body in [
            &b"Name=%"[..],
            &b"Name=%2"[..],
            &b"Name=%GG"[..],
            &b"%GG=value"[..],
            &b"Name=%FF"[..],
            &b"%FF=value"[..],
            &b"Name=%C0%AF"[..],
            &b"Name=%ED%A0%80"[..],
            &b"Name=%E9%9B"[..],
            &b"Name=\xff"[..],
        ] {
            assert!(parse_form(body).is_err());
        }
    }

    #[test]
    fn url_policy_accepts_secure_or_literal_loopback_without_rewriting() {
        for (url, kind) in [
            ("https://example.com/twilio/voice", "http"),
            ("wss://example.com/twilio/media", "ws"),
            ("https://example.com:8443/twilio/voice", "http"),
            ("http://127.0.0.1:8080/twilio/voice", "http"),
            ("http://127.0.0.2/twilio/voice", "http"),
            ("ws://127.0.0.1:8080/twilio/media", "ws"),
            ("http://[::1]:8080/twilio/voice", "http"),
            ("ws://[::1]/twilio/media", "ws"),
        ] {
            assert_eq!(validate_url(url, kind).unwrap().as_str(), url);
        }
    }

    #[test]
    fn url_policy_rejects_insecure_dns_and_nonloopback_ips() {
        for url in [
            "http://example.com/twilio/voice",
            "http://localhost/twilio/voice",
            "http://localhost./twilio/voice",
            "http://127.0.0.1.example.com/twilio/voice",
            "http://0.0.0.0/twilio/voice",
            "http://10.0.0.1/twilio/voice",
            "http://169.254.169.254/twilio/voice",
            "http://[::]/twilio/voice",
            "http://[::ffff:7f00:1]/twilio/voice",
        ] {
            assert!(validate_url(url, "http").is_err(), "{url}");
        }
        assert!(validate_url("ws://example.com/twilio/media", "ws").is_err());
        assert!(validate_url("ws://localhost/twilio/media", "ws").is_err());
    }

    #[test]
    fn url_policy_rejects_credentials_query_fragment_and_wrong_kinds() {
        for url in [
            "https://user@example.com/twilio/voice",
            "https://:password@example.com/twilio/voice",
            "https://example.com/twilio/voice?x=1",
            "https://example.com/twilio/voice?",
            "https://example.com/twilio/voice#fragment",
            "https://example.com/twilio/voice#",
            "file:///twilio/voice",
            "/twilio/voice",
            "",
        ] {
            assert!(validate_url(url, "http").is_err(), "{url}");
        }
        assert!(validate_url("wss://example.com/twilio/media", "http").is_err());
        assert!(validate_url("https://example.com/twilio/voice", "ws").is_err());
        assert!(validate_url("https://example.com/twilio/voice", "unknown").is_err());
    }

    #[test]
    fn url_policy_rejects_url_parser_normalization() {
        for url in [
            "HTTPS://example.com/twilio/voice",
            "https://EXAMPLE.com/twilio/voice",
            "https://example.com:443/twilio/voice",
            "https://example.com",
            "https://example.com/other/../twilio/voice",
            "https://example.com/%2e/twilio/voice",
            " https://example.com/twilio/voice",
            "https://example.com/twilio/voice\n",
            "http://127.1/twilio/voice",
            "http://2130706433/twilio/voice",
            "http://0x7f000001/twilio/voice",
            "http://0177.0.0.1/twilio/voice",
            "http://[0:0:0:0:0:0:0:1]/twilio/voice",
        ] {
            assert!(validate_url(url, "http").is_err(), "{url}");
        }
    }

    #[test]
    fn sid_requires_exact_expected_prefix_and_32_ascii_hex_digits() {
        for prefix in ["AC", "CA", "MZ"] {
            assert!(valid_sid(
                &format!("{prefix}0123456789abcdefABCDEF0123456789"),
                prefix
            ));
            assert!(!valid_sid(
                &format!("{prefix}0123456789abcdefABCDEF012345678"),
                prefix
            ));
            assert!(!valid_sid(
                &format!("{prefix}0123456789abcdefABCDEF01234567890"),
                prefix
            ));
        }
        for value in [
            "CA0123456789abcdefABCDEF0123456789",
            "ac0123456789abcdefABCDEF0123456789",
            "AC0123456789abcdefABCDEF012345678g",
            "AC0123456789abcdefABCDEF01234567é",
            "AC0123456789abcdefABCDEF012345678 ",
            "AC",
            "",
        ] {
            assert!(!valid_sid(value, "AC"), "{value}");
        }
        for prefix in ["", "A", "ACC", "12", "é"] {
            assert!(!valid_sid("AC0123456789abcdefABCDEF0123456789", prefix));
        }
    }
}
