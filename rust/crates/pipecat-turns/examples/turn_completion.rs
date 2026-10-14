//! Credential-free demonstration of the existing unsigned completion topology.
//! The loopback server scripts replies; it does not perform turn detection.
use pipecat_core::{
    Direction, FlushFrame, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig,
    PipelineWorker, ProcessorContext, ProcessorError, TextFrame, async_trait,
};
use pipecat_services::{
    ClientConfig, CompletionRequest, Content, Context, LlmClient, LlmProcessor, Message, Provider,
    Role,
};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair,
    LLMTurnCompletionConfig, LLMTurnCompletionProcessor,
};
use serde_json::{Value, json};
use std::{
    error::Error,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const WAIT: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Complete,
    Short,
    Long,
    Cancel,
}
impl Scenario {
    fn marker(self) -> &'static str {
        match self {
            Self::Complete => "●",
            Self::Long => "○",
            _ => "◐",
        }
    }
    fn responses(self) -> usize {
        if matches!(self, Self::Short | Self::Long) {
            2
        } else {
            1
        }
    }
}

// This bounded parser is fixture-only, not a general-purpose HTTP server.
async fn request(socket: &mut TcpStream) -> Result<Value> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 2048];
    let end = loop {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Err("request ended before headers".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > 64 * 1024 {
            return Err("fixture request too large".into());
        }
        if let Some(p) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let header = std::str::from_utf8(&bytes[..end])?;
    let length: usize = header
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(str::trim)
                .map(str::to_owned)
        })
        .ok_or("missing content length")?
        .parse()?;
    if length > 64 * 1024 - end {
        return Err("fixture body too large".into());
    }
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Err("request ended before body".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > 64 * 1024 {
            return Err("fixture request too large".into());
        }
    }
    Ok(serde_json::from_slice(&bytes[end..end + length])?)
}

struct Fixture {
    endpoint: String,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
    eof: oneshot::Receiver<()>,
}
impl Fixture {
    async fn start(scenario: Scenario) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/completion", listener.local_addr()?);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let (stop, mut stopped) = watch::channel(false);
        let (eof_tx, eof) = oneshot::channel();
        let task = tokio::spawn(async move {
            let operation = async move {
                let mut eof_tx = Some(eof_tx);
                loop {
                    let (mut socket, _) = listener.accept().await?;
                    let body = tokio::time::timeout(WAIT, request(&mut socket)).await??;
                    {
                        let mut seen = seen.lock().map_err(|_| "request lock poisoned")?;
                        if seen.len() >= 4 {
                            return Err("unexpected unbounded requests".into());
                        }
                        seen.push(body);
                    }
                    let text = format!(
                        "{} {}",
                        scenario.marker(),
                        if matches!(scenario, Scenario::Complete) {
                            "Hello from the fixture."
                        } else {
                            "suppressed fixture words"
                        }
                    );
                    let data = format!(
                        "data: {}\n\n",
                        json!({"choices":[{"delta":{"content":text},"finish_reason":null}]})
                    );
                    if matches!(scenario, Scenario::Cancel) {
                        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await?;
                        socket.write_all(data.as_bytes()).await?;
                        socket.flush().await?;
                        let mut byte = [0];
                        if socket.read(&mut byte).await? != 0 {
                            return Err("expected provider peer EOF".into());
                        }
                        if let Some(tx) = eof_tx.take() {
                            let _ = tx.send(());
                        }
                    } else {
                        let data = format!(
                            "{data}data: {}\n\ndata: [DONE]\n\n",
                            json!({"choices":[{"delta":{},"finish_reason":"stop"}]})
                        );
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{data}",
                            data.len()
                        );
                        socket.write_all(response.as_bytes()).await?;
                    }
                }
            };
            tokio::select! { result = operation => result, _ = stopped.changed() => Ok(()) }
        });
        Ok(Self {
            endpoint,
            requests,
            stop,
            task,
            eof,
        })
    }
    async fn shutdown(mut self) -> Result<()> {
        let _ = self.stop.send(true);
        match tokio::time::timeout(WAIT, &mut self.task).await {
            Ok(result) => result?,
            Err(error) => {
                self.task.abort();
                let _ = self.task.await;
                Err(error.into())
            }
        }
    }
}

async fn next(worker: &mut PipelineWorker) -> Result<FrameData> {
    let out = tokio::time::timeout(WAIT, worker.recv())
        .await?
        .ok_or("pipeline ended unexpectedly")?;
    if matches!(
        out.frame.data,
        FrameData::Error(_) | FrameData::ErrorDetailed(_)
    ) {
        return Err(format!("pipeline error: {:?}", out.frame.data).into());
    }
    Ok(out.frame.data)
}
// Observe the delivery boundary before the assistant consumes text/end frames.
struct DeliveryTap(mpsc::Sender<FrameData>);
#[async_trait]
impl FrameProcessor for DeliveryTap {
    fn name(&self) -> &str {
        "CompletionDemoDeliveryTap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(
                frame.data,
                FrameData::LlmText(_)
                    | FrameData::LlmMarker(_)
                    | FrameData::LlmResponseEnd
                    | FrameData::LlmFullResponseEnd(_)
            )
        {
            self.0
                .try_send(frame.data.clone())
                .map_err(|e| ProcessorError::new(format!("demo observation queue: {e}")))?;
        }
        ctx.push(frame, direction).await
    }
}
async fn delivery(
    worker: &mut PipelineWorker,
    events: &mut mpsc::Receiver<FrameData>,
) -> Result<FrameData> {
    tokio::time::timeout(WAIT, async {
        loop {
            tokio::select! {
                event = events.recv() => return event.ok_or_else(|| "delivery observation ended".into()),
                output = worker.recv() => {
                    let out = output.ok_or("pipeline ended before delivery")?;
                    if matches!(out.frame.data, FrameData::Error(_) | FrameData::ErrorDetailed(_)) { return Err(format!("pipeline error: {:?}", out.frame.data).into()); }
                }
            }
        }
    }).await?
}

async fn send(worker: &PipelineWorker, data: FrameData) -> Result<()> {
    tokio::time::timeout(
        WAIT,
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream),
    )
    .await??;
    Ok(())
}

async fn demonstrate(scenario: Scenario) -> Result<()> {
    let config = LLMTurnCompletionConfig {
        incomplete_short_timeout: Duration::from_millis(100),
        incomplete_long_timeout: Duration::from_millis(150),
        max_reprompts_per_segment: 1,
        ..Default::default()
    };
    let instructions = config.completion_instructions();
    let retry_prompt = if matches!(scenario, Scenario::Long) {
        config.long_prompt()
    } else {
        config.short_prompt()
    };
    let context = Context::new(vec![
        Message::text(Role::System, &instructions),
        Message::text(Role::User, "Show the scripted completion behavior."),
    ]);
    let pair = LLMContextAggregatorPair::new(
        context,
        Default::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Text,
        },
    );
    let shared = pair.context();
    let mut fixture = Fixture::start(scenario).await?;
    let (tap, mut events) = mpsc::channel(32);
    let started = async {
        let mut client = ClientConfig::new(Provider::OpenAi, "local-fixture-not-a-credential");
        client.endpoint = fixture.endpoint.clone();
        let llm = LlmProcessor::new(
            LlmClient::new(client)?,
            CompletionRequest::new("scripted-fixture", Context::default()),
        );
        Ok::<_, Box<dyn Error + Send + Sync>>(
            Pipeline::new(vec![
                pair.user(),
                Arc::new(llm),
                Arc::new(LLMTurnCompletionProcessor::new(config)?),
                Arc::new(DeliveryTap(tap)),
                pair.assistant(),
            ])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_secs(1),
                ..Default::default()
            })
            .await?,
        )
    }
    .await;
    let mut worker = match started {
        Ok(worker) => worker,
        Err(error) => {
            let cleanup = fixture.shutdown().await;
            cleanup?;
            return Err(error);
        }
    };
    let mut cancel_admitted = false;
    let observed = async {
        while !next(&mut worker).await?.is_start() {}
        send(&worker, shared.frame_data()?).await?;
        let mut visible = String::new();
        let mut ends = 0;
        loop {
            match delivery(&mut worker, &mut events).await? {
                FrameData::LlmText(TextFrame { text, .. }) => visible.push_str(&text),
                FrameData::LlmResponseEnd | FrameData::LlmFullResponseEnd(_) => {
                    ends += 1;
                    if ends == scenario.responses() {
                        break;
                    }
                }
                FrameData::LlmMarker(_) if matches!(scenario, Scenario::Cancel) => {
                    break;
                }
                _ => {}
            }
        }
        if matches!(scenario, Scenario::Cancel) {
            send(
                &worker,
                FrameData::Flush(FlushFrame {
                    returning: false,
                    origin: Some("cancel-marker-history".into()),
                }),
            )
            .await?;
            while !matches!(next(&mut worker).await?, FrameData::Flush(_)) {}
            worker.cancel("cancel scripted unfinished response").await?;
            cancel_admitted = true;
            tokio::time::timeout(WAIT, &mut fixture.eof).await??;
        } else {
            // A FIFO barrier proves assistant history has consumed response end.
            send(
                &worker,
                FrameData::Flush(FlushFrame {
                    returning: false,
                    origin: Some("completion-demo-barrier".into()),
                }),
            )
            .await?;
            while !matches!(next(&mut worker).await?, FrameData::Flush(_)) {}
            if matches!(scenario, Scenario::Short | Scenario::Long) {
                // Observation window exceeds another configured retry interval.
                tokio::time::sleep(Duration::from_millis(350)).await;
            }
        }
        let history = shared.snapshot()?;
        let assistant: Vec<_> = history
            .messages
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .filter_map(|m| match &m.content {
                Some(Content::Text(t)) => Some(t.as_str()),
                _ => None,
            })
            .collect();
        if matches!(scenario, Scenario::Complete) {
            if visible != "Hello from the fixture." || assistant != ["● Hello from the fixture."]
            {
                return Err(
                    format!("unexpected complete projection: {visible:?}, {assistant:?}").into(),
                );
            }
        } else if !visible.is_empty() || assistant.iter().any(|text| *text != scenario.marker()) {
            return Err("incomplete text escaped or marker history changed".into());
        }
        if matches!(scenario, Scenario::Cancel) && assistant != ["◐"] {
            return Err("cancel did not preserve exactly one incomplete marker".into());
        }
        let requests = fixture
            .requests
            .lock()
            .map_err(|_| "request lock poisoned")?;
        if requests.len() != scenario.responses() {
            return Err(format!("unexpected request count: {}", requests.len()).into());
        }
        if requests[0]["messages"][0]["content"] != instructions {
            return Err("completion instructions missing from actual HTTP request".into());
        }
        if matches!(scenario, Scenario::Short | Scenario::Long)
            && (assistant.len() != 2
                || !requests[1]["messages"]
                    .as_array()
                    .ok_or("missing messages")?
                    .iter()
                    .any(|m| m["role"] == "developer" && m["content"] == retry_prompt))
        {
            return Err("automatic retry did not retain marker and exact developer prompt".into());
        }
        if matches!(scenario, Scenario::Short | Scenario::Long)
            && !requests[1]["messages"]
                .as_array()
                .ok_or("missing retry messages")?
                .iter()
                .any(|m| m["role"] == "assistant" && m["content"] == scenario.marker())
        {
            return Err("retry wire request omitted assistant marker".into());
        }
        Ok::<_, Box<dyn Error + Send + Sync>>((requests.len(), visible))
    }
    .await;
    // Always close and join both owners before returning any observation error.
    let cancelled = if cancel_admitted || worker.state().is_finished() {
        Ok(())
    } else {
        worker.cancel("completion demonstration finished").await
    };
    let (joined, join_deadline) = match tokio::time::timeout(WAIT, worker.join()).await {
        Ok(result) => (result, None),
        Err(error) => {
            let _ = worker.cancel("join deadline").await;
            (worker.join().await, Some(error))
        }
    };
    let server = fixture.shutdown().await;
    cancelled?;
    let report = joined?;
    server?;
    if let Some(error) = join_deadline {
        return Err(error.into());
    }
    if report.state != pipecat_core::WorkerState::Cancelled
        || report.total_errors != 0
        || !report.errors.is_empty()
    {
        return Err(format!("worker errors: {report:?}").into());
    }
    if matches!(scenario, Scenario::Cancel) {
        while let Ok(event) = events.try_recv() {
            if matches!(
                event,
                FrameData::LlmText(_)
                    | FrameData::LlmResponseEnd
                    | FrameData::LlmFullResponseEnd(_)
            ) {
                return Err(
                    "cancelled incomplete stream leaked text or fabricated response end".into(),
                );
            }
        }
    }
    let (requests, visible) = observed?;
    println!(
        "{scenario:?}: requests={requests}, visible={visible:?}, pipeline_and_server_joined=true"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!(
        "Local scripted HTTP/SSE demonstration; no model, provider account, microphone, or credentials are used."
    );
    for scenario in [
        Scenario::Complete,
        Scenario::Short,
        Scenario::Long,
        Scenario::Cancel,
    ] {
        demonstrate(scenario).await?;
    }
    Ok(())
}
