//! Independent canonical request/HMAC oracle; no production signature helper is
//! used to construct expected signatures. Legacy fixed vectors remain separate.
use hmac::{Hmac, Mac};
use pipecat_aws_common::{
    auth,
    eventstream::{self, HeaderValue},
    AwsCredentials,
};
use sha2::{Digest, Sha256};
use std::time::{Duration, UNIX_EPOCH};
fn mac(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut h = Hmac::<Sha256>::new_from_slice(key).unwrap();
    h.update(bytes);
    h.finalize().into_bytes().to_vec()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
fn hash(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}
#[test]
fn both_routing_headers_bind_request_seed_and_stream_chain() {
    let url = url::Url::parse("https://runtime.sagemaker.us-east-1.amazonaws.com:8443/endpoints/test/invocations-bidirectional-stream").unwrap();
    let time = UNIX_EPOCH + Duration::from_secs(1_704_067_200);
    let creds = AwsCredentials::new(
        "EXAMPLE",
        "synthetic-secret",
        Some("synthetic-token".into()),
    )
    .unwrap();
    let mut signatures = std::collections::BTreeSet::new();
    for (path, query) in [
        ("v1/listen", "model=nova-3&language=en"),
        ("v2/listen", "model=nova-3&language=en"),
        ("v1/listen", "model=nova-2&language=en"),
    ] {
        let (headers, mut signer) = auth::signed_sagemaker_event_stream_post_headers(
            &url,
            "us-east-1",
            &creds,
            time,
            4096,
            path,
            query,
        )
        .unwrap();
        let names = "content-type;host;x-amz-date;x-amz-security-token;x-amzn-sagemaker-model-invocation-path;x-amzn-sagemaker-model-query-string";
        let canonical = format!("POST\n/endpoints/test/invocations-bidirectional-stream\n\ncontent-type:application/vnd.amazon.eventstream\nhost:runtime.sagemaker.us-east-1.amazonaws.com:8443\nx-amz-date:20240101T000000Z\nx-amz-security-token:synthetic-token\nx-amzn-sagemaker-model-invocation-path:{path}\nx-amzn-sagemaker-model-query-string:{query}\n\n{names}\n{}",hash(b""));
        let key = mac(
            &mac(
                &mac(&mac(b"AWS4synthetic-secret", b"20240101"), b"us-east-1"),
                b"sagemaker",
            ),
            b"aws4_request",
        );
        let scope = "20240101/us-east-1/sagemaker/aws4_request";
        let request_signature = hex(&mac(
            &key,
            format!(
                "AWS4-HMAC-SHA256\n20240101T000000Z\n{scope}\n{}",
                hash(canonical.as_bytes())
            )
            .as_bytes(),
        ));
        assert_eq!(headers["authorization"],format!("AWS4-HMAC-SHA256 Credential=EXAMPLE/{scope}, SignedHeaders={names}, Signature={request_signature}"));
        assert!(headers["x-amzn-sagemaker-model-invocation-path"].is_sensitive());
        assert!(headers["x-amzn-sagemaker-model-query-string"].is_sensitive());
        let inner = eventstream::encode(&[(":message-type", "event")], &[0, 255], 4096).unwrap();
        let mut previous = request_signature;
        for payload in [inner.as_slice(), b"".as_slice()] {
            let wire = if payload.is_empty() {
                signer.sign_empty(time).unwrap()
            } else {
                signer.sign_message(payload, time).unwrap()
            };
            let parsed = eventstream::decode(&wire, 4096).unwrap();
            let mut date = vec![5];
            date.extend_from_slice(b":date");
            date.push(8);
            date.extend_from_slice(&1_704_067_200_000i64.to_be_bytes());
            let expected = mac(
                &key,
                format!(
                    "AWS4-HMAC-SHA256-PAYLOAD\n20240101T000000Z\n{scope}\n{previous}\n{}\n{}",
                    hash(&date),
                    hash(payload)
                )
                .as_bytes(),
            );
            assert!(parsed
                .headers
                .iter()
                .any(|(name, value)| *name == ":chunk-signature"
                    && matches!(value,HeaderValue::Bytes(v) if *v == expected.as_slice())));
            previous = hex(&expected);
        }
        assert!(signatures.insert(previous));
    }
}
#[test]
fn routing_grammar_and_existing_validation_are_closed() {
    let url =
        url::Url::parse("http://127.0.0.1:8443/endpoints/test/invocations-bidirectional-stream")
            .unwrap();
    let creds = AwsCredentials::new("EXAMPLE", "synthetic", None).unwrap();
    let time = UNIX_EPOCH + Duration::from_secs(1_704_067_200);
    for path in [
        "",
        "/v1/listen",
        "v1//listen",
        "v1/listen/",
        "v1/listen?x",
        "v1/é",
        "v1/listen\n",
    ] {
        assert!(auth::signed_sagemaker_event_stream_post_headers(
            &url,
            "us-east-1",
            &creds,
            time,
            4096,
            path,
            "a=b"
        )
        .is_err());
    }
    for query in [
        "",
        "a",
        "=b",
        "a=",
        "a=b&a=c",
        "a=b=c",
        "a=b;c=d",
        "a=b c",
        "a=é",
        "a=%",
        "a=%0",
        "a=%GG",
        "a=b\r\nx=x",
    ] {
        assert!(auth::signed_sagemaker_event_stream_post_headers(
            &url,
            "us-east-1",
            &creds,
            time,
            4096,
            "v1/listen",
            query
        )
        .is_err());
    }
    for query in ["a=b", "a=%20&b=x-_.~", "a=%2f"] {
        assert!(auth::signed_sagemaker_event_stream_post_headers(
            &url,
            "us-east-1",
            &creds,
            time,
            4096,
            "v1/listen",
            query
        )
        .is_ok());
    }
    assert!(auth::signed_sagemaker_event_stream_post_headers(
        &url,
        "us-east-1",
        &creds,
        time,
        4096,
        &"a".repeat(101),
        "a=b"
    )
    .is_err());
    assert!(auth::signed_sagemaker_event_stream_post_headers(
        &url,
        "us-east-1",
        &creds,
        time,
        4096,
        "a",
        &format!("a={}", "b".repeat(2047))
    )
    .is_err());
    assert!(auth::signed_sagemaker_event_stream_post_headers(
        &url,
        "bad region",
        &creds,
        time,
        4096,
        "a",
        "a=b"
    )
    .is_err());
    assert!(auth::signed_sagemaker_event_stream_post_headers(
        &url,
        "us-east-1",
        &creds,
        time,
        82,
        "a",
        "a=b"
    )
    .is_err());
    let (old, _) =
        auth::signed_event_stream_post_headers(&url, "us-east-1", &creds, time, "sagemaker", 4096)
            .unwrap();
    assert!(!old.contains_key("x-amzn-sagemaker-model-query-string"));
    assert_eq!(old.len(), 4);
}
