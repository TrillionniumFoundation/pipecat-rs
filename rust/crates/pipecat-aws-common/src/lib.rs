//! Shared explicit AWS signing and bounded binary EventStream primitives.
pub mod auth;
pub mod eventstream;
pub use auth::{AwsCredentials, PresignedUrl};
#[derive(Debug, thiserror::Error)]
pub enum AwsError {
    #[error("invalid AWS configuration: {0}")]
    Invalid(&'static str),
    #[error("invalid AWS protocol: {0}")]
    Protocol(&'static str),
    #[error("AWS input exceeds configured limit")]
    TooLarge,
}
