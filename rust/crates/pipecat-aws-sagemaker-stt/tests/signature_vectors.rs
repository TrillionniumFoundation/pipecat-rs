mod common;
use common::*;
use pipecat_aws_common::{auth, AwsCredentials};
use std::time::{Duration, UNIX_EPOCH};
#[test]
fn independent_host_scope_raw_blob_chains_and_midnight_terminal() {
    for token in [false, true] {
        let url=url::Url::parse("https://runtime.sagemaker.us-east-1.amazonaws.com:8443/endpoints/test-endpoint/invocations-bidirectional-stream").unwrap();
        let credentials = AwsCredentials::new(ACCESS, SECRET, token.then(|| TOKEN.into())).unwrap();
        let t = UNIX_EPOCH + Duration::from_secs(1_704_153_599); // 2024-01-01 23:59:59 UTC
        let (headers, mut signer) = auth::signed_event_stream_post_headers(
            &url,
            "us-east-1",
            &credentials,
            t,
            "sagemaker",
            1024 * 1024,
        )
        .unwrap();
        let mut request = http::Request::builder()
            .method("POST")
            .version(http::Version::HTTP_2)
            .uri(url.as_str())
            .body(())
            .unwrap();
        *request.headers_mut() = headers;
        assert_eq!(
            request.headers()["host"],
            "runtime.sagemaker.us-east-1.amazonaws.com:8443"
        );
        let mut verifier = Verify::request(&request, token);
        let payload = event(serde_json::json!({"type":"session.end"}));
        let wire = signer.sign_message(&payload, t).unwrap();
        assert_eq!(verifier.outer(&wire).unwrap()["type"], "session.end");
        let terminal = signer.sign_empty(t + Duration::from_secs(2)).unwrap();
        assert!(verifier.outer(&terminal).is_none());
        assert!(verifier.terminal);
        assert!(signer.sign_empty(t).is_err());
        assert!(signer.sign_message(&payload, t).is_err());
    }
}
