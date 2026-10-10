//! Native Amazon Bedrock Converse and ConverseStream with explicit AWS credentials.
//! Nova Sonic, AgentCore and SageMaker are separate protocols and are not implemented.
pub mod adapter;
pub mod client;
pub mod processor;
pub mod protocol;
pub mod settings;
pub use client::{BedrockClient, BedrockConfig, BedrockLimits, BedrockStream};
pub use pipecat_aws_common::AwsCredentials;
pub use pipecat_services::{
    CancellationToken, CompletionRequest, Content, ContentPart, Context, Message, Role,
    ServiceError, ToolCall, ToolChoice, ToolDefinition,
};
pub use processor::BedrockProcessor;
pub use settings::BedrockSettings;
