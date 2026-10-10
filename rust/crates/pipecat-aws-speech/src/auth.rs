//! Speech API compatibility wrappers over shared AWS signing.
use crate::StreamingError;
use pipecat_aws_common::{auth as shared, AwsError};
pub use shared::PresignedUrl;
use std::collections::BTreeMap;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AwsCredentials(shared::AwsCredentials);
impl AwsCredentials {
    pub fn new(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Result<Self, StreamingError> {
        shared::AwsCredentials::new(access_key, secret_key, session_token)
            .map(Self)
            .map_err(error)
    }
}
pub(crate) fn error(value: AwsError) -> StreamingError {
    match value {
        AwsError::Invalid(s) => StreamingError::Invalid(s),
        AwsError::Protocol(s) => StreamingError::Protocol(s),
        AwsError::TooLarge => StreamingError::TooLarge,
    }
}
pub(crate) fn region(value: &str) -> Result<(), StreamingError> {
    shared::region(value).map_err(error)
}
pub(crate) use shared::{domain, now};
pub(crate) fn endpoint(
    value: &str,
    expected: &str,
    path: &str,
    websocket: bool,
) -> Result<url::Url, StreamingError> {
    shared::endpoint(value, expected, path, websocket).map_err(error)
}
pub(crate) fn presign(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    params: BTreeMap<String, String>,
    timestamp: &str,
) -> Result<PresignedUrl, StreamingError> {
    shared::presign(url, region, &credentials.0, params, timestamp).map_err(error)
}
pub(crate) fn polly_headers(
    url: &url::Url,
    region: &str,
    credentials: &AwsCredentials,
    body: &[u8],
    timestamp: &str,
) -> Result<reqwest::header::HeaderMap, StreamingError> {
    shared::signed_post_headers(
        url,
        region,
        &credentials.0,
        body,
        timestamp,
        "polly",
        url.path(),
    )
    .map_err(error)
}
