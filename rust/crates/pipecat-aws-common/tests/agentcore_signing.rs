//! Fixed expected signatures come from an independent Python stdlib generator.
//! Retained audit artifact: rootpipecat149-evidence/generate_agentcore_signing_vectors.py.
//! Wire reference: boto/botocore 22be6841647761fb4ae0f8bef6b1a55e882039f5,
//! auth.py (canonical_request and _normalize_url_path), serialize.py (URI labels).
//! These are synthetic offline vectors, not a claim of an accepted AWS request.
use pipecat_aws_common::{
    auth::{encode_component, signed_body_post_headers, signed_post_headers},
    AwsCredentials, AwsError,
};
use reqwest::header::HeaderMap;

const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const SESSION_HEADER: &str = "x-amzn-bedrock-agentcore-runtime-session-id";
const ACCEPT: &str = "application/json, text/event-stream";

struct Vector {
    url: &'static str,
    canonical_uri: &'static str,
    region: &'static str,
    timestamp: &'static str,
    body: &'static str,
    token: Option<&'static str>,
    session: &'static str,
    host: &'static str,
    authorization: &'static str,
}

const VECTORS: &[Vector] = &[
    Vector {
        url: "https://bedrock-agentcore.us-east-1.amazonaws.com/runtimes/arn%3Aaws%3Abedrock-agentcore%3Aus-east-1%3A123456789012%3Aruntime%2FExample_Agent-0123456789/invocations",
        canonical_uri: "/runtimes/arn%253Aaws%253Abedrock-agentcore%253Aus-east-1%253A123456789012%253Aruntime%252FExample_Agent-0123456789/invocations",
        region: "us-east-1",
        timestamp: "20261010T010203Z",
        body: r#"{"prompt":"hello"}"#,
        token: None,
        session: "123e4567-e89b-42d3-a456-426614174000",
        host: "bedrock-agentcore.us-east-1.amazonaws.com",
        authorization: "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261010/us-east-1/bedrock-agentcore/aws4_request, SignedHeaders=accept;content-type;host;x-amz-date;x-amzn-bedrock-agentcore-runtime-session-id, Signature=cf331da77ef13b301f614091e199d5a4ddf677d1ff2703f744855d682dace36c",
    },
    Vector {
        url: "https://bedrock-agentcore.eu-west-2.amazonaws.com/runtimes/arn%3Aaws%3Abedrock-agentcore%3Aeu-west-2%3A987654321098%3Aruntime%2FZ9_a-AbCdEf0123/invocations",
        canonical_uri: "/runtimes/arn%253Aaws%253Abedrock-agentcore%253Aeu-west-2%253A987654321098%253Aruntime%252FZ9_a-AbCdEf0123/invocations",
        region: "eu-west-2",
        timestamp: "20260228T235959Z",
        body: r#"{"prompt":"Hello 世界\n next"}"#,
        token: Some("SYNTHETIC/TOKEN+="),
        session: "01234567-89ab-4cde-8f01-23456789abcd",
        host: "bedrock-agentcore.eu-west-2.amazonaws.com",
        authorization: "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260228/eu-west-2/bedrock-agentcore/aws4_request, SignedHeaders=accept;content-type;host;x-amz-date;x-amz-security-token;x-amzn-bedrock-agentcore-runtime-session-id, Signature=8a23632d273ab4eebff78a3a6d7405ff9af56baa884d3d26037287fe614c0d4a",
    },
    Vector {
        url: "http://[::1]:9101/runtimes/arn%3Aaws%3Abedrock-agentcore%3Aap-southeast-2%3A000000000000%3Aruntime%2FA-a0b1c2d3e4/invocations",
        canonical_uri: "/runtimes/arn%253Aaws%253Abedrock-agentcore%253Aap-southeast-2%253A000000000000%253Aruntime%252FA-a0b1c2d3e4/invocations",
        region: "ap-southeast-2",
        timestamp: "20240101T000000Z",
        body: r#"{"prompt":"  exact whitespace  "}"#,
        token: Some("SYNTHETIC/TOKEN+="),
        session: "fedcba98-7654-4321-8abc-def012345678",
        host: "[::1]:9101",
        authorization: "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20240101/ap-southeast-2/bedrock-agentcore/aws4_request, SignedHeaders=accept;content-type;host;x-amz-date;x-amz-security-token;x-amzn-bedrock-agentcore-runtime-session-id, Signature=c8cd11ee371a814004e0f0c424dcc65fe60a9c6b848c20b194d6f98c81ed301f",
    },
];

fn credentials(token: Option<&str>) -> AwsCredentials {
    AwsCredentials::new("AKIDEXAMPLE", SECRET, token.map(str::to_owned)).unwrap()
}

fn vector_headers(vector: &Vector) -> HeaderMap {
    signed_body_post_headers(
        &url::Url::parse(vector.url).unwrap(),
        vector.region,
        &credentials(vector.token),
        vector.body.as_bytes(),
        vector.timestamp,
        "bedrock-agentcore",
        vector.canonical_uri,
        &[("accept", ACCEPT), (SESSION_HEADER, vector.session)],
    )
    .unwrap()
}

fn canonical_path(url: &url::Url) -> String {
    url.path()
        .split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

fn with_extras(extra: &[(&str, &str)]) -> Result<HeaderMap, AwsError> {
    let vector = &VECTORS[0];
    signed_body_post_headers(
        &url::Url::parse(vector.url).unwrap(),
        vector.region,
        &credentials(None),
        vector.body.as_bytes(),
        vector.timestamp,
        "bedrock-agentcore",
        vector.canonical_uri,
        extra,
    )
}

#[test]
fn fixed_body_signatures_match_independent_vectors_with_and_without_token() {
    for vector in VECTORS {
        let headers = vector_headers(vector);
        assert_eq!(headers.len(), if vector.token.is_some() { 7 } else { 6 });
        assert_eq!(headers["authorization"], vector.authorization);
        assert_eq!(headers["host"], vector.host);
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(headers["x-amz-date"], vector.timestamp);
        assert_eq!(headers["accept"], ACCEPT);
        assert_eq!(headers[SESSION_HEADER], vector.session);
        assert!(headers["authorization"].is_sensitive());
        assert!(headers[SESSION_HEADER].is_sensitive());
        if let Some(token) = vector.token {
            assert_eq!(headers["x-amz-security-token"], token);
            assert!(headers["x-amz-security-token"].is_sensitive());
        } else {
            assert!(!headers.contains_key("x-amz-security-token"));
        }
        for absent in [
            "x-amz-content-sha256",
            "content-length",
            "transfer-encoding",
            "connection",
        ] {
            assert!(!headers.contains_key(absent));
        }
        // Expected paths and authorizations are fixed independently, rather than
        // being computed from the production helper under test.
        assert!(vector.url.contains("%3A"));
        assert!(vector.url.contains("%2F"));
        assert!(!vector.url.contains("%253A"));
        assert!(vector.canonical_uri.contains("%253A"));
        assert!(vector.canonical_uri.contains("%252F"));
        let debug = format!("{headers:?}");
        for private in [
            "AKIDEXAMPLE",
            SECRET,
            "SYNTHETIC/TOKEN+=",
            vector.session,
            vector.body,
            "arn:aws:",
        ] {
            assert!(!debug.contains(private));
        }
    }
}

#[test]
fn body_path_session_accept_and_date_are_bound_to_fixed_signature() {
    let vector = &VECTORS[0];
    let original = url::Url::parse(vector.url).unwrap();
    let changed_path = url::Url::parse(&vector.url.replace("0123456789", "9876543210")).unwrap();
    let changed_uri = canonical_path(&changed_path);
    let variants = [
        (
            &original,
            vector.canonical_uri,
            r#"{"prompt":"tampered"}"#,
            vector.session,
            ACCEPT,
            vector.timestamp,
        ),
        (
            &changed_path,
            changed_uri.as_str(),
            vector.body,
            vector.session,
            ACCEPT,
            vector.timestamp,
        ),
        (
            &original,
            vector.canonical_uri,
            vector.body,
            "123e4567-e89b-42d3-a456-426614174001",
            ACCEPT,
            vector.timestamp,
        ),
        (
            &original,
            vector.canonical_uri,
            vector.body,
            vector.session,
            "application/json",
            vector.timestamp,
        ),
        (
            &original,
            vector.canonical_uri,
            vector.body,
            vector.session,
            ACCEPT,
            "20261010T010204Z",
        ),
    ];
    for (url, canonical, body, session, accept, timestamp) in variants {
        let headers = signed_body_post_headers(
            url,
            vector.region,
            &credentials(None),
            body.as_bytes(),
            timestamp,
            "bedrock-agentcore",
            canonical,
            &[("accept", accept), (SESSION_HEADER, session)],
        )
        .unwrap();
        assert_ne!(headers["authorization"], vector.authorization);
    }
    let empty = signed_body_post_headers(
        &original,
        vector.region,
        &credentials(None),
        b"",
        vector.timestamp,
        "bedrock-agentcore",
        vector.canonical_uri,
        &[("accept", ACCEPT), (SESSION_HEADER, vector.session)],
    )
    .unwrap();
    assert_ne!(empty["authorization"], vector.authorization);
}

#[test]
fn original_host_and_security_token_are_bound_to_fixed_signature() {
    let vector = &VECTORS[1];
    let original = url::Url::parse(vector.url).unwrap();
    let mut altered_host = original.clone();
    altered_host.set_host(Some("example.test")).unwrap();
    for (url, token) in [
        (&altered_host, vector.token),
        (&original, Some("DIFFERENT/SYNTHETIC/TOKEN+=")),
        (&original, None),
    ] {
        let headers = signed_body_post_headers(
            url,
            vector.region,
            &credentials(token),
            vector.body.as_bytes(),
            vector.timestamp,
            "bedrock-agentcore",
            vector.canonical_uri,
            &[("accept", ACCEPT), (SESSION_HEADER, vector.session)],
        )
        .unwrap();
        assert_ne!(headers["authorization"], vector.authorization);
    }
}

#[test]
fn header_order_and_case_normalize_to_the_fixed_signature() {
    let vector = &VECTORS[0];
    let headers = with_extras(&[
        (
            "X-Amzn-Bedrock-AgentCore-Runtime-Session-Id",
            vector.session,
        ),
        ("AcCePt", "  application/json,   text/event-stream  "),
    ])
    .unwrap();
    assert_eq!(headers["authorization"], vector.authorization);
    assert_eq!(headers["accept"], ACCEPT);
    assert!(with_extras(&[("X-Test", "first"), ("x-test", "second")]).is_err());
}

#[test]
fn reject_reserved_headers_invalid_names_and_control_bytes() {
    for reserved in [
        "Authorization",
        "HOST",
        "Content-Type",
        "Date",
        "X-Amz-Date",
        "X-Amz-Security-Token",
        "X-Amz-Content-SHA256",
        "Content-Length",
        "Transfer-Encoding",
        "Connection",
        "Keep-Alive",
        "TE",
        "Trailer",
        "Upgrade",
        "Expect",
        "Proxy-Authenticate",
        "Proxy-Authorization",
        "User-Agent",
        "X-Amzn-Trace-Id",
    ] {
        assert!(with_extras(&[(reserved, "unsafe")]).is_err(), "{reserved}");
    }
    for name in [
        "",
        "has space",
        "has:colon",
        "x\r\ninjected",
        "秘密",
        "x\0bad",
    ] {
        assert!(with_extras(&[(name, "value")]).is_err());
    }
    for value in ["", "   ", "bad\r\nvalue", "\tvalue", "\0", "\x7f", "秘密"] {
        assert!(with_extras(&[("x-test", value)]).is_err());
    }
    for control in 0u8..=31 {
        let value = format!("value{}tail", char::from(control));
        assert!(with_extras(&[("x-test", &value)]).is_err());
    }
}

#[test]
fn extra_header_count_individual_and_total_byte_limits_are_enforced() {
    let names: Vec<String> = (0..17).map(|index| format!("x-header-{index}")).collect();
    let extra: Vec<(&str, &str)> = names.iter().map(|name| (name.as_str(), "value")).collect();
    assert!(with_extras(&extra[..16]).is_ok());
    assert!(matches!(with_extras(&extra), Err(AwsError::TooLarge)));
    assert!(with_extras(&[(&"n".repeat(128), &"v".repeat(4096))]).is_ok());
    assert!(matches!(
        with_extras(&[(&"n".repeat(129), "value")]),
        Err(AwsError::TooLarge)
    ));
    assert!(matches!(
        with_extras(&[("name", &"v".repeat(4097))]),
        Err(AwsError::TooLarge)
    ));
    let limit_value = "v".repeat(4094);
    assert!(with_extras(&[("x1", &limit_value), ("x2", &limit_value)]).is_ok());
    assert!(matches!(
        with_extras(&[("x1", &limit_value), ("xxx", &limit_value)]),
        Err(AwsError::TooLarge)
    ));
}

#[test]
fn reject_wire_uri_ambiguity_and_mismatched_double_encoding() {
    let vector = &VECTORS[0];
    for value in [
        "https://example.test/runtimes//invocations",
        "https://example.test/runtimes/%",
        "https://example.test/runtimes/%3",
        "https://example.test/runtimes/%GG",
        "https://example.test/runtimes/arn%3aaws",
        "https://example.test/runtimes/arn:aws",
        "https://example.test/runtimes/%41",
        "https://example.test/runtimes/a%2Eb",
        "https://example.test/runtimes/%253A",
        "https://example.test/runtimes/%00",
        "https://example.test/runtimes/%1F",
        "https://example.test/runtimes/%7F",
        "https://example.test/runtimes/%5C",
        "https://example.test/runtimes/%3F",
        "https://example.test/runtimes/%23",
        "https://example.test/runtimes/%FF",
        "https://example.test/runtimes/a%2F..%2Fb",
        "https://example.test/runtimes/a%2F%2Fb",
        "https://user:password@example.test/path",
        "https://example.test/path?qualifier=test",
        "https://example.test/path?",
        "https://example.test/path#fragment",
        "https://example.test/path#",
        "ftp://example.test/path",
    ] {
        let url = url::Url::parse(value).unwrap();
        let result = signed_body_post_headers(
            &url,
            vector.region,
            &credentials(None),
            vector.body.as_bytes(),
            vector.timestamp,
            "bedrock-agentcore",
            &canonical_path(&url),
            &[],
        );
        assert!(result.is_err(), "{value}");
    }
    let url = url::Url::parse(vector.url).unwrap();
    for incorrect in [
        url.path(),
        "/different",
        "/runtimes/../invocations",
        "/runtimes/?query",
        "/runtimes/#fragment",
        "/runtimes/\r\ninjected",
    ] {
        assert!(signed_body_post_headers(
            &url,
            vector.region,
            &credentials(None),
            vector.body.as_bytes(),
            vector.timestamp,
            "bedrock-agentcore",
            incorrect,
            &[],
        )
        .is_err());
    }
}

#[test]
fn body_url_and_canonical_uri_size_limits_are_enforced() {
    let vector = &VECTORS[0];
    let original = url::Url::parse(vector.url).unwrap();
    let body = vec![b'x'; 16 * 1024 * 1024 + 1];
    let sign = |url: &url::Url, body: &[u8], uri: &str| {
        signed_body_post_headers(
            url,
            vector.region,
            &credentials(None),
            body,
            vector.timestamp,
            "bedrock-agentcore",
            uri,
            &[],
        )
    };
    assert!(sign(&original, &body[..body.len() - 1], vector.canonical_uri).is_ok());
    assert!(matches!(
        sign(&original, &body, vector.canonical_uri),
        Err(AwsError::TooLarge)
    ));
    let origin = "https://example.test/";
    let bounded = url::Url::parse(&format!("{origin}{}", "a".repeat(8192 - origin.len()))).unwrap();
    assert!(sign(&bounded, b"{}", bounded.path()).is_ok());
    let oversized = url::Url::parse(&format!("{bounded}a")).unwrap();
    assert!(matches!(
        sign(&oversized, b"{}", oversized.path()),
        Err(AwsError::TooLarge)
    ));
    assert!(matches!(
        sign(&original, b"{}", &"a".repeat(3 * 8192 + 1)),
        Err(AwsError::TooLarge)
    ));
}

#[test]
fn invalid_timestamp_region_and_service_errors_do_not_echo_inputs() {
    let vector = &VECTORS[0];
    for (region, timestamp, service) in [
        ("us-east-1", "20260230T010203Z", "bedrock-agentcore"),
        ("us-east-1", "20261010T250203Z", "bedrock-agentcore"),
        ("us-east-1", "20261010T010203Zextra", "bedrock-agentcore"),
        ("INJECTED-REGION", vector.timestamp, "bedrock-agentcore"),
        (vector.region, vector.timestamp, "INJECTED-SERVICE"),
        (vector.region, vector.timestamp, ""),
    ] {
        let error = signed_body_post_headers(
            &url::Url::parse(vector.url).unwrap(),
            region,
            &credentials(Some("SYNTHETIC/TOKEN+=")),
            vector.body.as_bytes(),
            timestamp,
            service,
            vector.canonical_uri,
            &[],
        )
        .unwrap_err();
        let text = format!("{error:?} {error}");
        for private in [
            "AKIDEXAMPLE",
            SECRET,
            "SYNTHETIC/TOKEN+=",
            "INJECTED-REGION",
            "INJECTED-SERVICE",
            vector.body,
            "arn:aws:",
        ] {
            assert!(!text.contains(private));
        }
    }
}

#[test]
fn legacy_polly_and_bedrock_body_sha_headers_remain_byte_identical() {
    for (url, region, timestamp, service, body, token, expected, body_sha) in [
        (
            "https://polly.us-east-1.amazonaws.com/v1/speech",
            "us-east-1",
            "20241010T010203Z",
            "polly",
            r#"{"Text":"hello","OutputFormat":"pcm"}"#,
            Some("TOKEN/+="),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20241010/us-east-1/polly/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token, Signature=a5b8fbe19200b38348ce654d270277b2996cfeedccba4648c91bc9d51970b3c4",
            "0b50f3972f0f60eba1eef2a467587e068bdd0046e2d757c296cff38b95184955",
        ),
        (
            "https://bedrock-runtime.us-west-2.amazonaws.com/model/test-model/converse-stream",
            "us-west-2",
            "20260506T070809Z",
            "bedrock",
            r#"{"messages":[{"role":"user","content":[{"text":"hello"}]}]}"#,
            None,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20260506/us-west-2/bedrock/aws4_request, SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, Signature=1df04c32ed1239f0b0724fc9e5f172991051e104c57b72356cacb22be340b58e",
            "331b626fbec967399a3c05643d22e56af43ee9839874a2db641bd025f84436d8",
        ),
    ] {
        let url = url::Url::parse(url).unwrap();
        let headers = signed_post_headers(
            &url,
            region,
            &credentials(token),
            body.as_bytes(),
            timestamp,
            service,
            url.path(),
        )
        .unwrap();
        assert_eq!(headers["authorization"], expected);
        assert_eq!(headers["x-amz-content-sha256"], body_sha);
        assert_eq!(headers.len(), if token.is_some() { 6 } else { 5 });
        assert!(!headers.contains_key(SESSION_HEADER));
    }
}
