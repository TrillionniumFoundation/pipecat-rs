//! Fixed vectors are generated with Python stdlib, independently of this crate.
use pipecat_aws_common::{
    auth::signed_event_stream_post_headers,
    eventstream::{self, HeaderValue},
    AwsCredentials, AwsError,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

mod fixtures {
    include!("fixtures/event_stream_vectors.rs");
}

const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

fn credentials(token: Option<&str>) -> AwsCredentials {
    AwsCredentials::new("AKIDEXAMPLE", SECRET, token.map(str::to_owned)).unwrap()
}

fn time(seconds: u64, nanos: u32) -> SystemTime {
    UNIX_EPOCH + Duration::new(seconds, nanos)
}

fn unhex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|part| {
            let part = std::str::from_utf8(part).unwrap();
            u8::from_str_radix(part, 16).unwrap()
        })
        .collect()
}

fn setup(
    limit: usize,
) -> (
    reqwest::header::HeaderMap,
    pipecat_aws_common::EventStreamSigner,
) {
    let vector = &fixtures::VECTORS[0];
    signed_event_stream_post_headers(
        &url::Url::parse(vector.url).unwrap(),
        vector.region,
        &credentials(vector.token),
        time(vector.request_seconds, vector.request_nanos),
        "bedrock",
        limit,
    )
    .unwrap()
}

#[test]
fn fixed_requests_and_chains_match_independent_stdlib_vectors() {
    for vector in fixtures::VECTORS {
        let (headers, mut signer) = signed_event_stream_post_headers(
            &url::Url::parse(vector.url).unwrap(),
            vector.region,
            &credentials(vector.token),
            time(vector.request_seconds, vector.request_nanos),
            "bedrock",
            4096,
        )
        .unwrap();
        assert_eq!(headers.len(), if vector.token.is_some() { 5 } else { 4 });
        assert_eq!(
            headers["authorization"], vector.authorization,
            "{}",
            vector.name
        );
        assert_eq!(
            headers["content-type"],
            "application/vnd.amazon.eventstream"
        );
        assert_eq!(headers["host"], vector.host);
        assert_eq!(headers["x-amz-date"], vector.timestamp);
        assert!(!headers.contains_key("x-amz-content-sha256"));
        assert!(!headers.contains_key("content-length"));
        assert!(!headers.contains_key("transfer-encoding"));
        assert!(!headers.contains_key("connection"));
        assert!(headers["authorization"].is_sensitive());
        if let Some(token) = vector.token {
            assert_eq!(headers["x-amz-security-token"], token);
            assert!(headers["x-amz-security-token"].is_sensitive());
        } else {
            assert!(!headers.contains_key("x-amz-security-token"));
        }
        let debug = format!("{headers:?} {signer:?}");
        for secret in ["AKIDEXAMPLE", SECRET, "SYNTHETIC/TOKEN+="] {
            assert!(!debug.contains(secret));
        }
        for event in vector.events {
            let inner = unhex(event.inner_hex);
            let at = time(event.seconds, event.nanos);
            let message = if event.terminal {
                signer.sign_empty(at).unwrap()
            } else {
                signer.sign_message(&inner, at).unwrap()
            };
            assert_eq!(message, unhex(event.outer_hex), "{}", vector.name);
            let decoded = eventstream::decode(&message, 4096).unwrap();
            assert_eq!(decoded.payload, inner);
            assert_eq!(
                decoded.headers.get(":date"),
                Some(&HeaderValue::Timestamp(event.truncated_millis))
            );
            let signature = unhex(event.signature);
            assert_eq!(signature.len(), 32);
            assert_eq!(
                decoded.headers.get(":chunk-signature"),
                Some(&HeaderValue::Bytes(&signature))
            );
            // Fixed wire order matters independently of the decoder's map order.
            assert_eq!(&message[12..30], b"\x10:chunk-signature\x06");
            assert_eq!(&message[64..71], b"\x05:date\x08");
            assert!(!format!("{signer:?}").contains(event.signature));
            if event.terminal {
                assert_eq!(message.len(), 83);
                assert!(decoded.payload.is_empty());
                assert!(signer.sign_empty(at).is_err());
                assert!(signer
                    .sign_message(&unhex(vector.events[0].inner_hex), at)
                    .is_err());
            }
        }
    }
}

#[test]
fn typed_encoder_matches_all_primitive_types_and_caller_order() {
    let uuid = std::array::from_fn(|index| index as u8);
    let headers = [
        ("true", HeaderValue::Bool(true)),
        ("false", HeaderValue::Bool(false)),
        ("byte", HeaderValue::Byte(i8::MIN)),
        ("short", HeaderValue::Short(i16::MIN)),
        ("integer", HeaderValue::Integer(i32::MIN)),
        ("long", HeaderValue::Long(i64::MIN)),
        ("bytes", HeaderValue::Bytes(&[0, 255, 128])),
        ("string", HeaderValue::String("hello / 世界")),
        ("timestamp", HeaderValue::Timestamp(1_234_567_890_123)),
        ("uuid", HeaderValue::Uuid(uuid)),
    ];
    let expected = unhex(fixtures::ALL_TYPED_HEADERS_FRAME_HEX);
    let encoded = eventstream::encode_typed(&headers, b"typed payload", expected.len()).unwrap();
    assert_eq!(encoded, expected);
    assert!(matches!(
        eventstream::encode_typed(&headers, b"typed payload", expected.len() - 1),
        Err(AwsError::TooLarge)
    ));
    let decoded = eventstream::decode(&encoded, expected.len()).unwrap();
    for (name, value) in &headers {
        assert_eq!(decoded.headers.get(name), Some(value));
    }
    assert_eq!(decoded.payload, b"typed payload");
}

#[test]
fn string_encoder_remains_byte_identical_to_independent_inner_frames() {
    for event in &fixtures::VECTORS[0].events[..3] {
        let expected = unhex(event.inner_hex);
        let decoded = eventstream::decode(&expected, 4096).unwrap();
        let encoded = eventstream::encode(
            &[
                (":message-type", "event"),
                (":event-type", "chunk"),
                (":content-type", "application/json"),
            ],
            decoded.payload,
            expected.len(),
        )
        .unwrap();
        assert_eq!(encoded, expected);
    }
}

#[test]
fn typed_header_name_value_count_and_encoded_size_bounds() {
    let duplicate = [
        ("same", HeaderValue::Bool(true)),
        ("same", HeaderValue::Byte(1)),
    ];
    assert!(eventstream::encode_typed(&duplicate, b"", 1024).is_err());
    for name in ["".to_string(), "n".repeat(256)] {
        assert!(eventstream::encode_typed(&[(&name, HeaderValue::Bool(true))], b"", 4096).is_err());
    }
    let long_name = "n".repeat(255);
    assert!(eventstream::encode_typed(&[(&long_name, HeaderValue::Bool(true))], b"", 4096).is_ok());
    let bytes = vec![0; 65_535];
    assert!(
        eventstream::encode_typed(&[("bytes", HeaderValue::Bytes(&bytes))], b"", 70_000).is_ok()
    );
    let oversized = vec![0; 65_536];
    assert!(
        eventstream::encode_typed(&[("bytes", HeaderValue::Bytes(&oversized))], b"", 70_000)
            .is_err()
    );
    let oversized_string = "s".repeat(65_536);
    assert!(eventstream::encode_typed(
        &[("string", HeaderValue::String(&oversized_string))],
        b"",
        70_000
    )
    .is_err());
    let too_many_bytes = [
        ("one", HeaderValue::Bytes(&bytes)),
        ("two", HeaderValue::Bytes(&bytes)),
    ];
    assert!(matches!(
        eventstream::encode_typed(&too_many_bytes, b"", 200_000),
        Err(AwsError::TooLarge)
    ));
    let names: Vec<_> = (0..129).map(|index| format!("h{index}")).collect();
    let many: Vec<_> = names
        .iter()
        .map(|name| (name.as_str(), HeaderValue::Bool(false)))
        .collect();
    assert!(eventstream::encode_typed(&many[..128], b"", 4096).is_ok());
    assert!(matches!(
        eventstream::encode_typed(&many, b"", 4096),
        Err(AwsError::TooLarge)
    ));
    assert!(matches!(
        eventstream::encode_typed(&[], &[0; 17], 32),
        Err(AwsError::TooLarge)
    ));
    assert!(eventstream::encode_typed(&[], b"", 16).is_ok());
    assert!(eventstream::encode_typed(&[], b"", 15).is_err());
}

#[test]
fn invalid_inner_frames_and_timestamps_do_not_advance_chain() {
    let event = &fixtures::VECTORS[0].events[0];
    let inner = unhex(event.inner_hex);
    let at = time(event.seconds, event.nanos);
    let (_, mut signer) = setup(4096);
    for cut in 0..16 {
        assert!(signer.sign_message(&inner[..cut], at).is_err());
    }
    for index in [0, 8, 12, inner.len() - 6, inner.len() - 1] {
        let mut corrupt = inner.clone();
        corrupt[index] ^= 1;
        assert!(signer.sign_message(&corrupt, at).is_err());
    }
    // Invalid header type with repaired outer integrity is still rejected.
    let mut corrupt = inner.clone();
    let type_index = 12 + 1 + usize::from(corrupt[12]);
    corrupt[type_index] = 255;
    let len = corrupt.len();
    let crc = crc32fast::hash(&corrupt[..len - 4]).to_be_bytes();
    corrupt[len - 4..].copy_from_slice(&crc);
    assert!(signer.sign_message(&corrupt, at).is_err());
    let mut concatenated = inner.clone();
    concatenated.extend_from_slice(&inner);
    assert!(signer.sign_message(&concatenated, at).is_err());
    let before_epoch = UNIX_EPOCH.checked_sub(Duration::from_nanos(1)).unwrap();
    assert!(signer.sign_message(&inner, before_epoch).is_err());
    assert!(signer.sign_empty(before_epoch).is_err());
    let year_10000 = time(253_402_300_800, 0);
    assert!(signer.sign_message(&inner, year_10000).is_err());
    assert_eq!(
        signer.sign_message(&inner, at).unwrap(),
        unhex(event.outer_hex)
    );
}

#[test]
fn complete_outer_bound_includes_nested_frame_and_typed_headers() {
    let event = &fixtures::VECTORS[0].events[0];
    let inner = unhex(event.inner_hex);
    let expected = unhex(event.outer_hex);
    assert_eq!(expected.len(), inner.len() + 83);
    let at = time(event.seconds, event.nanos);
    let (_, mut exact) = setup(expected.len());
    assert_eq!(exact.sign_message(&inner, at).unwrap(), expected);
    let (_, mut short) = setup(expected.len() - 1);
    assert!(matches!(
        short.sign_message(&inner, at),
        Err(AwsError::TooLarge)
    ));
    // A failed oversized frame has not changed the seed or consumed the terminal.
    let (_, mut untouched) = setup(83);
    assert_eq!(
        short.sign_empty(at).unwrap(),
        untouched.sign_empty(at).unwrap()
    );
    assert!(short.sign_empty(at).is_err());
    assert!(untouched.sign_empty(at).is_err());
}

#[test]
fn request_validation_is_bounded_and_errors_never_echo_inputs() {
    let fixture = &fixtures::VECTORS[0];
    let at = time(fixture.request_seconds, fixture.request_nanos);
    let credentials = credentials(Some("SYNTHETIC/TOKEN+="));
    for raw in [
        "https://SENTINEL_SECRET@example.com/model/test",
        "https://user:SENTINEL_SECRET@example.com/model/test",
        "https://example.com/model/test?SENTINEL_SECRET",
        "https://example.com/model/test#SENTINEL_SECRET",
        "https://example.com/model//SENTINEL_SECRET",
        "ftp://example.com/model/SENTINEL_SECRET",
        "file:///SENTINEL_SECRET",
    ] {
        let url = url::Url::parse(raw).unwrap();
        let result =
            signed_event_stream_post_headers(&url, "us-east-1", &credentials, at, "bedrock", 4096);
        let error = result.unwrap_err();
        assert!(!format!("{error:?} {error}").contains("SENTINEL_SECRET"));
    }
    let too_long = url::Url::parse(&format!("https://example.com/{}", "a".repeat(8192))).unwrap();
    assert!(signed_event_stream_post_headers(
        &too_long,
        "us-east-1",
        &credentials,
        at,
        "bedrock",
        4096
    )
    .is_err());
    let url = url::Url::parse(fixture.url).unwrap();
    for limit in [0, 16, 82, 16 * 1024 * 1024 + 1, usize::MAX] {
        assert!(signed_event_stream_post_headers(
            &url,
            "us-east-1",
            &credentials,
            at,
            "bedrock",
            limit
        )
        .is_err());
    }
    for (region, service) in [
        ("bad\nregion", "bedrock"),
        ("us-east-1", "bad\nservice"),
        ("", "bedrock"),
    ] {
        assert!(
            signed_event_stream_post_headers(&url, region, &credentials, at, service, 4096)
                .is_err()
        );
    }
    let before_epoch = UNIX_EPOCH.checked_sub(Duration::from_secs(1)).unwrap();
    assert!(signed_event_stream_post_headers(
        &url,
        "us-east-1",
        &credentials,
        before_epoch,
        "bedrock",
        4096
    )
    .is_err());
    assert!(signed_event_stream_post_headers(
        &url,
        "us-east-1",
        &credentials,
        time(253_402_300_800, 0),
        "bedrock",
        4096
    )
    .is_err());
}
