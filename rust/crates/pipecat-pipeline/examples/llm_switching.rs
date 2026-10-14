//! Credential-free failover with two real loopback HTTP/SSE LLM services.
use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, LlmContextFrame, Pipeline, PipelineConfig,
    PipelineWorker, ProcessorUnusablePolicy, WorkerState,
};
use pipecat_pipeline::{LlmSwitcher, ServiceSwitcherStrategy};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use serde_json::{Value, json};
use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(3);

#[derive(Default)]
struct Ownership {
    server_joins: AtomicUsize,
    worker_joins: AtomicUsize,
    stopped_before_accept: AtomicUsize,
    peer_eof: AtomicUsize,
}
// Fault injection stays inside this example; no runtime API is added.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy)]
enum Injection {
    None,
    AfterPrimary,
    AfterStart,
    StreamingBackup,
}

async fn read_request(socket: &mut TcpStream) -> Result<Value> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await?;
        if n == 0 {
            return Err("request closed before headers".into());
        }
        bytes.extend_from_slice(&buffer[..n]);
        if bytes.len() > 65536 {
            return Err("fixture request exceeds 64 KiB".into());
        }
        if let Some(p) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let length: usize = std::str::from_utf8(&bytes[..end])?
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(str::trim)
                .map(str::to_owned)
        })
        .ok_or("missing content length")?
        .parse()?;
    if length > 65536 - end {
        return Err("fixture body exceeds 64 KiB".into());
    }
    while bytes.len() < end + length {
        let n = socket.read(&mut buffer).await?;
        if n == 0 {
            return Err("request closed before body".into());
        }
        bytes.extend_from_slice(&buffer[..n]);
        if bytes.len() > 65536 {
            return Err("fixture request exceeds 64 KiB".into());
        }
    }
    Ok(serde_json::from_slice(&bytes[end..end + length])?)
}
struct LocalProvider {
    service: Arc<LlmProcessor>,
    stop: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}
impl LocalProvider {
    async fn start(
        name: &str,
        fail: bool,
        streaming: bool,
        ownership: Arc<Ownership>,
    ) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut config = ClientConfig::new(Provider::OpenAi, "");
        config.api_key = None;
        config.endpoint = format!("http://{}/completion", listener.local_addr()?);
        config.request_timeout = WAIT;
        let service = Arc::new(
            LlmProcessor::new(
                LlmClient::new(config)?,
                CompletionRequest::new(
                    name,
                    Context::new(vec![Message::text(Role::User, "initial")]),
                ),
            )
            .with_name(name),
        );
        let (stop, mut stopped) = watch::channel(false);
        let task = tokio::spawn(async move {
            let accepted = Arc::new(AtomicUsize::new(0));
            let seen = accepted.clone();
            let operation = async move {
                let (mut socket, _) = listener.accept().await?;
                seen.store(1, Ordering::SeqCst);
                let request = tokio::time::timeout(WAIT, read_request(&mut socket)).await??;
                if !request["messages"].is_array() {
                    return Err("missing request messages".into());
                }
                if streaming {
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await?;
                    socket.flush().await?;
                    let mut byte = [0];
                    if socket.read(&mut byte).await? != 0 {
                        return Err("expected cancellation peer EOF".into());
                    }
                    return Ok(true);
                }
                let (status, kind, body) = if fail {
                    (
                        401,
                        "application/json",
                        json!({"error":{"message":"simulated authentication failure"}}).to_string(),
                    )
                } else {
                    (
                        200,
                        "text/event-stream",
                        format!(
                            "data: {}\n\ndata: [DONE]\n\n",
                            json!({"choices":[{"delta":{"content":"The backup handled your fresh request."},"finish_reason":"stop"}]})
                        ),
                    )
                };
                socket.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
                Ok::<_, Box<dyn Error + Send + Sync>>(false)
            };
            tokio::select! {
                result=operation => { if result? { ownership.peer_eof.fetch_add(1,Ordering::SeqCst); } Ok(()) },
                _=stopped.changed() => { if accepted.load(Ordering::SeqCst)==0 { ownership.stopped_before_accept.fetch_add(1,Ordering::SeqCst); } Ok(()) }
            }
        });
        Ok(Self {
            service,
            stop,
            task,
        })
    }
    async fn shutdown(mut self) -> Result<()> {
        // Allow an already closed client socket to produce its EOF witness.
        let result = match tokio::time::timeout(Duration::from_millis(100), &mut self.task).await {
            Ok(result) => result,
            Err(_) => {
                let _ = self.stop.send(true);
                match tokio::time::timeout(WAIT, &mut self.task).await {
                    Ok(result) => result,
                    Err(error) => {
                        self.task.abort();
                        let _ = self.task.await;
                        return Err(error.into());
                    }
                }
            }
        };
        result?
    }
}
fn prompt(text: &str) -> Frame {
    Frame::new(FrameData::LlmContext(LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, text)],
        ..Default::default()
    }))
}
async fn output(worker: &mut PipelineWorker) -> Result<FrameData> {
    Ok(worker
        .recv()
        .await
        .ok_or("pipeline closed before expected output")?
        .frame
        .data)
}

async fn run(injection: Injection, deadline: Duration, ownership: Arc<Ownership>) -> Result<()> {
    let mut providers = Vec::new();
    let mut worker: Option<PipelineWorker> = None;
    let operation = async {
        providers.push(LocalProvider::start("primary",true,false,ownership.clone()).await?);
        if matches!(injection,Injection::AfterPrimary) { return Err("injected failure before second provider".into()); }
        providers.push(LocalProvider::start("backup",false,matches!(injection,Injection::StreamingBackup),ownership.clone()).await?);
        let switcher=Arc::new(LlmSwitcher::with_strategy(providers.iter().map(|p|p.service.clone() as Arc<dyn pipecat_services::LlmInference>).collect(),ServiceSwitcherStrategy::Failover)?);
        worker=Some(Pipeline::new(vec![switcher]).start(PipelineConfig {processor_unusable_policy:ProcessorUnusablePolicy::Cancel,shutdown_timeout:Duration::from_secs(1),..Default::default()}).await?);
        // Creation is not wrapped in an outer cancellable timeout. Core owns its startup deadline.
        tokio::time::timeout(deadline, async {
        let worker=worker.as_mut().ok_or("missing worker")?;
        if matches!(injection,Injection::AfterStart) { return Err("injected observation failure after worker start".into()); }
        worker.sender().send(prompt("This attempt will fail."),Direction::Downstream).await?;
        loop {
            if matches!(output(worker).await?,FrameData::LlmServiceMetadata(m) if m.base.service_name=="backup") { break; }
        }
        println!("Primary failed; backup selected. The failed request was not replayed.");
        worker.sender().send(prompt("A new explicitly requested turn."),Direction::Downstream).await?;
        let mut visible=String::new();
        loop { match output(worker).await? { FrameData::LlmText(text)=>visible.push_str(&text.text), FrameData::LlmResponseEnd|FrameData::LlmFullResponseEnd(_)=>break, _=>{} } }
        if visible!="The backup handled your fresh request." { return Err(format!("unexpected backup response: {visible:?}").into()); }
        println!("{visible}");
        Ok::<_,Box<dyn Error+Send+Sync>>(())
        }).await.map_err(|error| format!("example observation deadline: {error}"))?
    }.await;
    let mut errors = Vec::new();
    match operation {
        Ok(()) => {}
        Err(error) => errors.push(error.to_string()),
    }
    let success = errors.is_empty();
    if let Some(mut worker) = worker {
        if !worker.state().is_finished() {
            let terminal = tokio::time::timeout(Duration::from_secs(3), async {
                if success {
                    worker.end().await
                } else {
                    worker.cancel("switching example failed").await
                }
            })
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
            if let Err(error) = terminal {
                errors.push(format!("terminal admission: {error}"));
                let _ = worker.cancel("terminal admission failed").await;
            }
        }
        let joined = match tokio::time::timeout(WAIT, worker.join()).await {
            Ok(result) => result,
            Err(error) => {
                errors.push(format!("join deadline: {error}"));
                let _ = worker.cancel("switching join deadline").await;
                worker.join().await
            }
        };
        ownership.worker_joins.fetch_add(1, Ordering::SeqCst);
        match joined {
            Ok(report) => {
                if report.total_errors != 0 || !report.errors.is_empty() {
                    errors.push(format!("worker errors: {:?}", report.errors));
                }
                let wanted = if success {
                    WorkerState::Ended
                } else {
                    WorkerState::Cancelled
                };
                if report.state != wanted {
                    errors.push(format!("unexpected worker state: {:?}", report.state));
                }
            }
            Err(error) => errors.push(format!("worker join: {error}")),
        }
    }
    // Never let one server error skip the other server's shutdown and join.
    for provider in providers {
        let result = provider.shutdown().await;
        ownership.server_joins.fetch_add(1, Ordering::SeqCst);
        if let Err(error) = result {
            errors.push(format!("provider join: {error}"));
        }
    }
    if !errors.is_empty() {
        return Err(errors.join("; ").into());
    }
    println!("Both native services and their owned work are cleaned up.");
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    run(
        Injection::None,
        Duration::from_secs(8),
        Arc::new(Ownership::default()),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn success_joins_worker_and_both_servers() {
        let s = Arc::new(Ownership::default());
        run(Injection::None, Duration::from_secs(5), s.clone())
            .await
            .unwrap();
        assert_eq!(s.worker_joins.load(Ordering::SeqCst), 1);
        assert_eq!(s.server_joins.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn failure_before_second_provider_stops_and_joins_first_idle_server() {
        let s = Arc::new(Ownership::default());
        let r = run(Injection::AfterPrimary, WAIT, s.clone()).await;
        assert_eq!(
            r.unwrap_err().to_string(),
            "injected failure before second provider"
        );
        assert_eq!(s.worker_joins.load(Ordering::SeqCst), 0);
        assert_eq!(s.server_joins.load(Ordering::SeqCst), 1);
        assert_eq!(s.stopped_before_accept.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn observation_failure_after_start_joins_all_owners() {
        let s = Arc::new(Ownership::default());
        let r = run(Injection::AfterStart, WAIT, s.clone()).await;
        assert_eq!(
            r.unwrap_err().to_string(),
            "injected observation failure after worker start"
        );
        assert_eq!(s.worker_joins.load(Ordering::SeqCst), 1);
        assert_eq!(s.server_joins.load(Ordering::SeqCst), 2);
        assert_eq!(s.stopped_before_accept.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn streaming_timeout_closes_provider_socket_and_joins_all_owners() {
        let s = Arc::new(Ownership::default());
        let r = run(
            Injection::StreamingBackup,
            Duration::from_secs(1),
            s.clone(),
        )
        .await;
        let error = r.unwrap_err().to_string();
        assert!(
            error.starts_with("example observation deadline:"),
            "{error}"
        );
        assert!(
            !error.contains(';'),
            "unexpected additional cleanup error: {error}"
        );
        assert_eq!(s.worker_joins.load(Ordering::SeqCst), 1);
        assert_eq!(s.server_joins.load(Ordering::SeqCst), 2);
        assert_eq!(s.peer_eof.load(Ordering::SeqCst), 1);
    }
}
