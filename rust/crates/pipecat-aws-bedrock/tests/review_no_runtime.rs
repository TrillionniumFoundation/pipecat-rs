mod support;
use pipecat_aws_bedrock::*;
#[test]
fn valid_stream_admission_outside_tokio_returns_an_error_without_panicking() {
    let client =
        BedrockClient::new(BedrockConfig::new(support::credentials(), "us-east-1")).unwrap();
    assert!(matches!(
        client.stream(support::request()),
        Err(ServiceError::InvalidRequest(_))
    ));
}
