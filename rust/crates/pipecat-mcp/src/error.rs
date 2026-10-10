/// Public failures never embed upstream messages, arguments, URLs, headers, or
/// subprocess output. A transport failure leaves remote execution ambiguous.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum McpError {
    #[error("invalid MCP configuration: {0}")]
    Config(&'static str),
    #[error("MCP client is closed; call start() or tools() first")]
    NotConnected,
    #[error("MCP connection initialization failed")]
    Initialization,
    #[error("MCP operation timed out; remote outcome may be unknown")]
    Timeout,
    #[error("MCP operation cancelled; remote outcome may be unknown")]
    Cancelled,
    #[error("MCP transport failed; remote outcome may be unknown; call was not replayed")]
    Transport,
    #[error("MCP server rejected the request (code {0})")]
    Remote(i32),
    #[error("MCP protocol response was invalid")]
    Protocol,
    #[error("MCP tool is not advertised or is filtered out")]
    UnknownTool,
    #[error("MCP arguments must be a JSON object or null")]
    InvalidArguments,
    #[error("MCP capacity exceeded: {0}")]
    Limit(&'static str),
    #[error("MCP tool output filter failed")]
    OutputFilter,
    #[error("MCP transport cleanup failed or did not complete within the deadline")]
    Cleanup,
}
impl McpError {
    pub(crate) fn from_service(e: rmcp::ServiceError) -> Self {
        match e {
            rmcp::ServiceError::McpError(error) => Self::Remote(error.code.0),
            rmcp::ServiceError::TransportClosed | rmcp::ServiceError::TransportSend(_) => {
                Self::Transport
            }
            rmcp::ServiceError::Timeout { .. } => Self::Timeout,
            rmcp::ServiceError::Cancelled { .. } => Self::Cancelled,
            _ => Self::Protocol,
        }
    }
}
