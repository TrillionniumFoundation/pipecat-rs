mod common;
use common::*;
use pipecat_aws_common::{auth, eventstream, AwsCredentials};
use pipecat_aws_sagemaker_deepgram_stt::TranscriptionSettings;
use std::time::{Duration, UNIX_EPOCH};
#[test]
fn pinned_default_query_request_and_raw_stream_independent_oracle() {
    assert_eq!(
        TranscriptionSettings::default()
            .model_query_string()
            .unwrap(),
        QUERY
    );
    let url = url::Url::parse(&format!(
        "https://runtime.sagemaker.us-east-1.amazonaws.com:8443{PATH}"
    ))
    .unwrap();
    let time = UNIX_EPOCH + Duration::from_secs(1_704_067_200);
    let creds = AwsCredentials::new(ACCESS, SECRET, Some(TOKEN.into())).unwrap();
    let (headers, mut signer) = auth::signed_sagemaker_event_stream_post_headers(
        &url,
        "us-east-1",
        &creds,
        time,
        4096,
        "v1/listen",
        QUERY,
    )
    .unwrap();
    let mut request = http::Request::builder()
        .method("POST")
        .uri(url.as_str())
        .version(http::Version::HTTP_2)
        .body(())
        .unwrap();
    *request.headers_mut() = headers;
    let mut verify = Verify::request(&request, true);
    let raw = eventstream::encode(
        &[
            (":message-type", "event"),
            (":event-type", "PayloadPart"),
            (":content-type", "application/octet-stream"),
            ("DataType", "BINARY"),
        ],
        &[0, 255, 1, 254],
        4096,
    )
    .unwrap();
    let value = verify
        .outer(&signer.sign_message(&raw, time).unwrap())
        .unwrap();
    assert_eq!(value["binary"], serde_json::json!([0, 255, 1, 254]));
    for kind in ["KeepAlive", "Finalize", "CloseStream"] {
        let inner = event(serde_json::json!({"type":kind}));
        assert_eq!(
            verify
                .outer(&signer.sign_message(&inner, time).unwrap())
                .unwrap()["type"],
            kind
        );
    }
    assert!(verify.outer(&signer.sign_empty(time).unwrap()).is_none());
    assert!(verify.terminal);
}
