use crate::{cleanup::CleanupStatus, config::StdioConfig, Limits, McpError};
use futures_util::StreamExt;
use rmcp::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::Transport,
    RoleClient,
};
use std::{future::Future, io, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::AsyncWriteExt,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};
use tokio_util::codec::{FramedRead, LinesCodec};

pub(crate) struct NativeStdio {
    child: Child,
    writer: Arc<Mutex<ChildStdin>>,
    reader: FramedRead<ChildStdout, LinesCodec>,
    max_bytes: usize,
    timeout: Duration,
    cleanup: CleanupStatus,
}
impl NativeStdio {
    pub(crate) fn spawn(
        config: &StdioConfig,
        limits: &Limits,
        cleanup: CleanupStatus,
    ) -> Result<Self, McpError> {
        let mut command = Command::new(&config.executable);
        command
            .args(&config.arguments)
            .env_clear()
            .envs(&config.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if let Some(cwd) = &config.current_directory {
            command.current_dir(cwd);
        }
        let mut child = command.spawn().map_err(|_| McpError::Initialization)?;
        let writer = child.stdin.take().ok_or(McpError::Initialization)?;
        let reader = child.stdout.take().ok_or(McpError::Initialization)?;
        Ok(Self {
            child,
            writer: Arc::new(Mutex::new(writer)),
            reader: FramedRead::new(
                reader,
                LinesCodec::new_with_max_length(limits.max_wire_bytes),
            ),
            max_bytes: limits.max_wire_bytes,
            timeout: limits.close_timeout,
            cleanup,
        })
    }
}
impl Transport<RoleClient> for NativeStdio {
    type Error = io::Error;
    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), io::Error>> + Send + 'static {
        let writer = self.writer.clone();
        let max_bytes = self.max_bytes;
        let timeout = self.timeout;
        async move {
            let mut bytes = serde_json::to_vec(&message)
                .map_err(|_| io::Error::other("invalid MCP outbound message"))?;
            if bytes.len() > max_bytes {
                return Err(io::Error::other("MCP outbound message exceeds limit"));
            }
            bytes.push(b'\n');
            tokio::time::timeout(timeout, async {
                let mut writer = writer.lock().await;
                writer.write_all(&bytes).await?;
                writer.flush().await
            })
            .await
            .map_err(|_| io::Error::other("MCP write timed out"))?
        }
    }
    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        match self.reader.next().await {
            Some(Ok(line)) => serde_json::from_str(&line).ok(),
            _ => None,
        }
    }
    async fn close(&mut self) -> Result<(), io::Error> {
        let attempt = self.cleanup.begin();
        // Killing before awaiting stdin also unblocks a child that stopped reading.
        if self.child.try_wait()?.is_none() {
            self.child.start_kill()?;
        }
        tokio::time::timeout(self.timeout, self.child.wait())
            .await
            .map_err(|_| io::Error::other("MCP child cleanup timed out"))??;
        attempt.success();
        Ok(())
    }
}
