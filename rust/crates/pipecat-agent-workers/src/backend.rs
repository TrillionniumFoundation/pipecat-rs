use crate::{
    AgentError, LlmWorker, LlmWorkerHandle, LlmWorkerOptions, ToolRegistry, IDLE, TURN_TEXT,
    TURN_THOUGHT,
};
use pipecat_core::FrameData;
use pipecat_services::{
    CancellationToken, CompletionRequest, Content, ContentPart, LlmClient, Message, Role,
};
use pipecat_workers::{
    JobClient, JobContext, JobEvent, JobHandle, JobParams, JobResponse, JobStatus, LocalBus,
    NativeWorker, WorkerHandle, WorkerRegistry, WorkerState,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

pub const BACKEND_PAIRING_INSTRUCTION: &str = include_str!("backend_pairing.txt");
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BackendOutput {
    pub text: String,
    #[serde(default)]
    pub is_thought: bool,
    #[serde(default = "yes")]
    pub prefers_spoken: bool,
}
fn yes() -> bool {
    true
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolPhase {
    Started,
    InProgress,
    Result,
    Cancelled,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BackendToolCall {
    pub phase: ToolPhase,
    pub function_name: String,
    pub tool_call_id: String,
    pub arguments: Option<Value>,
    pub result: Option<Value>,
    pub is_final: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendEvent {
    Output(BackendOutput),
    ToolCall(BackendToolCall),
    Idle,
    Error { error: String },
    Attached { capabilities: BackendCapabilities },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    pub steering: bool,
    pub progress: bool,
}
type TransformFuture =
    Pin<Box<dyn Future<Output = Result<Option<BackendOutput>, AgentError>> + Send>>;
#[derive(Clone)]
pub struct OutputTransform(Arc<dyn Fn(BackendOutput) -> TransformFuture + Send + Sync>);
impl OutputTransform {
    pub fn new<F, Fut>(transform: F) -> Self
    where
        F: Fn(BackendOutput) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<BackendOutput>, AgentError>> + Send + 'static,
    {
        Self(Arc::new(move |o| Box::pin(transform(o))))
    }
}
#[derive(Clone)]
struct Attachment {
    context: JobContext,
    detached: CancellationToken,
    generation: u64,
}
struct AttachmentGuard {
    attachment: Attachment,
    slot: Arc<Mutex<Option<Attachment>>>,
    engine: LlmWorkerHandle,
}
impl Drop for AttachmentGuard {
    fn drop(&mut self) {
        self.attachment.detached.cancel();
        let mut slot = self.slot.lock().unwrap_or_else(|e| e.into_inner());
        if slot.as_ref().is_some_and(|a| {
            a.context.job_id == self.attachment.context.job_id
                && a.context.requester == self.attachment.context.requester
        }) {
            *slot = None;
            self.engine.interrupt_all();
        }
    }
}
/// Host for one attached frontend and an isolated backend conversation. `message`
/// acknowledges request admission; subsequent output travels on the `attach` job.
pub struct BackendLlmWorker {
    worker: WorkerHandle,
    engine: LlmWorkerHandle,
    attached: Arc<Mutex<Option<Attachment>>>,
    done: watch::Receiver<Option<Result<(), AgentError>>>,
}
impl BackendLlmWorker {
    pub async fn start(
        client: LlmClient,
        mut request: CompletionRequest,
        tools: ToolRegistry,
        options: LlmWorkerOptions,
        bus: LocalBus,
        registry: WorkerRegistry,
        transform: Option<OutputTransform>,
    ) -> Result<Self, AgentError> {
        append_instruction(&mut request, BACKEND_PAIRING_INSTRUCTION);
        let name = options.name.clone();
        let mut llm = LlmWorker::start(client, request, tools, options).await?;
        let engine = llm.handle();
        let attached: Arc<Mutex<Option<Attachment>>> = Arc::new(Mutex::new(None));
        let attach_slot = attached.clone();
        let attach_engine = engine.clone();
        let message_slot = attached.clone();
        let message_engine = engine.clone();
        let native=NativeWorker::new(name).active(true)
            .on_job("attach",false,move|context|{let slot=attach_slot.clone();let engine=attach_engine.clone();async move{
                let mut attachment=Attachment{context:context.clone(),detached:CancellationToken::new(),generation:0};
                {
                    let mut current=slot.lock().unwrap_or_else(|e|e.into_inner());
                    if current.is_some(){return JobResponse::error("a frontend is already attached");}
                    attachment.generation=engine.generation();*current=Some(attachment.clone());
                }
                let _guard=AttachmentGuard{attachment:attachment.clone(),slot,engine};
                let update=tokio::select!{biased;_=attachment.detached.cancelled()=>return JobResponse::cancelled(),result=context.update(Some(json!({"type":"attached","capabilities":{"steering":true,"progress":true}})),false)=>result};
                if let Err(error)=update{return JobResponse::error(error.to_string());}
                attachment.detached.cancelled().await;
                JobResponse::cancelled()
            }})?
            .on_job("message",false,move|context|{let slot=message_slot.clone();let engine=message_engine.clone();async move{
                let attachment=slot.lock().unwrap_or_else(|e|e.into_inner()).clone();
                let Some(attachment)=attachment else{return JobResponse::error("no frontend is attached; open an attach job first");};
                let request=context.payload.as_ref().and_then(|p|p.get("request")).and_then(Value::as_str).unwrap_or("").trim();
                if request.is_empty(){return JobResponse::error("the message has no request");}
                let result=tokio::select!{biased;_=attachment.detached.cancelled()=>Err(AgentError::Cancelled),result=engine.append(vec![Message::text(Role::User,request)],true)=>result};
                match result{Ok(busy)=>JobResponse::completed(json!({"backend":if busy{"working"}else{"idle"}})),Err(error)=>JobResponse::error(error.to_string())}
            }})?;
        let worker = native.start(bus, registry)?;
        let (done_tx, done) = watch::channel(None);
        let pump_slot = attached.clone();
        let pump_engine = engine.clone();
        let supervisor_worker = worker.clone();
        let supervisor_slot = attached.clone();
        let control_engine = engine.clone();
        tokio::spawn(async move {
            let mut pump = tokio::spawn(async move {
                while let Some(directed) = llm.recv().await {
                    let frame = directed.frame;
                    let mut events = vec![];
                    match frame.data {
                        FrameData::LlmResponseEnd => {
                            if let Some(thought) = frame
                                .metadata
                                .get(TURN_THOUGHT)
                                .and_then(Value::as_str)
                                .filter(|t| !t.trim().is_empty())
                            {
                                events.push(BackendEvent::Output(BackendOutput {
                                    text: thought.trim().into(),
                                    is_thought: true,
                                    prefers_spoken: false,
                                }));
                            }
                            if let Some(text) =
                                frame.metadata.get(TURN_TEXT).and_then(Value::as_str)
                            {
                                let (note, spoken) = split_spoken(text.trim());
                                if !note.is_empty() {
                                    events.push(BackendEvent::Output(BackendOutput {
                                        text: note,
                                        is_thought: false,
                                        prefers_spoken: false,
                                    }));
                                }
                                if !spoken.is_empty() {
                                    events.push(BackendEvent::Output(BackendOutput {
                                        text: spoken,
                                        is_thought: false,
                                        prefers_spoken: true,
                                    }));
                                }
                            }
                        }
                        FrameData::FunctionCallsStarted(calls) => {
                            for call in calls {
                                events.push(BackendEvent::ToolCall(BackendToolCall {
                                    phase: ToolPhase::Started,
                                    function_name: call.function_name,
                                    tool_call_id: call.tool_call_id,
                                    arguments: None,
                                    result: None,
                                    is_final: true,
                                }));
                            }
                        }
                        FrameData::FunctionCallInProgress(call) => {
                            events.push(BackendEvent::ToolCall(BackendToolCall {
                                phase: ToolPhase::InProgress,
                                function_name: call.function_name,
                                tool_call_id: call.tool_call_id,
                                arguments: Some(call.arguments),
                                result: None,
                                is_final: true,
                            }))
                        }
                        FrameData::FunctionCallResult(call) => {
                            events.push(BackendEvent::ToolCall(BackendToolCall {
                                phase: ToolPhase::Result,
                                function_name: call.function_name,
                                tool_call_id: call.tool_call_id,
                                arguments: Some(call.arguments),
                                result: Some(call.result),
                                is_final: call.is_final,
                            }))
                        }
                        FrameData::FunctionCallCancel(call) => {
                            events.push(BackendEvent::ToolCall(BackendToolCall {
                                phase: ToolPhase::Cancelled,
                                function_name: call.function_name,
                                tool_call_id: call.tool_call_id,
                                arguments: None,
                                result: None,
                                is_final: true,
                            }))
                        }
                        _ => {}
                    }
                    if frame.metadata.get(IDLE).and_then(Value::as_bool) == Some(true) {
                        events.push(BackendEvent::Idle);
                    }
                    for event in events {
                        let attachment =
                            pump_slot.lock().unwrap_or_else(|e| e.into_inner()).clone();
                        if let Some(attachment) = attachment {
                            if frame
                                .metadata
                                .get(crate::GENERATION)
                                .and_then(Value::as_u64)
                                .is_some_and(|generation| generation < attachment.generation)
                            {
                                continue;
                            }
                            let event = if let (BackendEvent::Output(output), Some(transform)) =
                                (&event, &transform)
                            {
                                let shaped = tokio::select! {biased;_=attachment.detached.cancelled()=>continue,result=(transform.0)(output.clone())=>result};
                                match shaped {
                                    Ok(Some(output)) => BackendEvent::Output(output),
                                    Ok(None) => continue,
                                    Err(error) => BackendEvent::Error {
                                        error: error.to_string(),
                                    },
                                }
                            } else {
                                event
                            };
                            publish_event(&attachment, event).await?;
                        }
                    }
                }
                if let Some(error) = pump_engine.snapshot().error {
                    let attachment = pump_slot.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    if let Some(attachment) = attachment {
                        publish_event(&attachment, BackendEvent::Error { error }).await?;
                    }
                }
                llm.join().await.map(|_| ())
            });
            let mut state = supervisor_worker.watch();
            let result = loop {
                let snapshot = state.borrow_and_update().clone();
                if matches!(
                    snapshot.state,
                    WorkerState::Ending | WorkerState::Cancelling
                ) || snapshot.state.is_terminal()
                {
                    if let Some(attachment) = supervisor_slot
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .clone()
                    {
                        attachment.detached.cancel();
                    }
                }
                if snapshot.state.is_terminal() {
                    break None;
                }
                tokio::select! {biased;result=&mut pump=>break Some(result),changed=state.changed()=>if changed.is_err(){break None;}}
            };
            let result = match result {
                Some(result) => {
                    supervisor_worker.cancel("backend pipeline stopped");
                    result
                        .map_err(|e| AgentError::Failed(e.to_string()))
                        .and_then(|r| r)
                }
                None => {
                    control_engine.cancel();
                    pump.await
                        .map_err(|e| AgentError::Failed(e.to_string()))
                        .and_then(|r| r)
                }
            };
            let _ = supervisor_worker.wait().await;
            done_tx.send_replace(Some(result));
        });
        Ok(Self {
            worker,
            engine,
            attached,
            done,
        })
    }
    pub fn worker(&self) -> &WorkerHandle {
        &self.worker
    }
    pub fn engine(&self) -> LlmWorkerHandle {
        self.engine.clone()
    }
    pub async fn send_output(&self, output: BackendOutput) -> Result<(), AgentError> {
        let attachment = self
            .attached
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(attachment) = attachment {
            publish_event(&attachment, BackendEvent::Output(output)).await?;
        }
        Ok(())
    }
    pub async fn wait(&mut self) -> Result<(), AgentError> {
        loop {
            if let Some(result) = self.done.borrow_and_update().clone() {
                return result;
            }
            self.done.changed().await.map_err(|_| AgentError::Closed)?;
        }
    }
}
impl Drop for BackendLlmWorker {
    fn drop(&mut self) {
        self.worker.cancel("backend owner dropped");
        self.engine.cancel();
        if let Some(attachment) = self
            .attached
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            attachment.detached.cancel();
        }
    }
}
async fn publish_event(attachment: &Attachment, event: BackendEvent) -> Result<(), AgentError> {
    tokio::select! {biased;_=attachment.detached.cancelled()=>Ok(()),result=attachment.context.update(Some(serde_json::to_value(event).map_err(|e|AgentError::Failed(e.to_string()))?),false)=>result.map_err(Into::into)}
}
pub(crate) fn append_instruction(request: &mut CompletionRequest, instruction: &str) {
    let existing = request.system_instruction.take().unwrap_or_default();
    request.system_instruction = Some(if existing.is_empty() {
        instruction.into()
    } else {
        format!("{existing}\n\n{instruction}")
    });
}
pub fn split_spoken(text: &str) -> (String, String) {
    let mut note = vec![];
    let mut spoken = vec![];
    let mut marked = false;
    for line in text.lines() {
        let trimmed = line.trim_start_matches([' ', '\t']);
        if let Some(rest) = trimmed.strip_prefix(">>") {
            marked = true;
            spoken.push(rest.trim_start_matches([' ', '\t']));
        } else if marked {
            spoken.push(line);
        } else {
            note.push(line);
        }
    }
    (
        note.join("\n").trim().into(),
        spoken.join("\n").trim().into(),
    )
}
/// Frontend speech is flattened into a user request, never copied as backend
/// assistant history. Non-text content and tool messages do not cross.
pub fn render_transcript_request(messages: &[Message], first: bool, instruction: &str) -> String {
    let mut lines = vec![];
    if !messages.is_empty() {
        lines.push(
            if first {
                "Voice conversation so far:"
            } else {
                "Voice conversation since the previous delegation:"
            }
            .into(),
        );
        for message in messages {
            let role = match message.role {
                Role::User => "USER",
                Role::Assistant => "ASSISTANT",
                _ => continue,
            };
            let text = match &message.content {
                Some(Content::Text(text)) => text.clone(),
                Some(Content::Parts(parts)) => parts
                    .iter()
                    .filter_map(|p| {
                        if let ContentPart::Text { text } = p {
                            Some(text.as_str())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n"),
                None => String::new(),
            };
            if !text.is_empty() {
                lines.push(format!("{role}: {text}"));
            }
        }
        lines.push(String::new());
    }
    lines.push(instruction.into());
    lines.join("\n")
}
pub struct BackendSession {
    client: JobClient,
    backend: String,
    attachment: JobHandle,
    pub capabilities: BackendCapabilities,
    message_timeout: Duration,
}
impl BackendSession {
    pub async fn attach(
        client: JobClient,
        backend: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, AgentError> {
        let backend = backend.into();
        let mut attachment = client
            .request(
                backend.clone(),
                JobParams {
                    name: Some("attach".into()),
                    ..Default::default()
                },
            )
            .map_err(|e| AgentError::Failed(e.to_string()))?;
        let capabilities = tokio::time::timeout(timeout, async {
            loop {
                match attachment
                    .next_event()
                    .await
                    .map_err(|e| AgentError::Failed(e.to_string()))?
                {
                    Some(JobEvent::Update(Some(value))) => {
                        if let Ok(BackendEvent::Attached { capabilities }) =
                            serde_json::from_value(value)
                        {
                            return Ok(capabilities);
                        }
                    }
                    None => {
                        let result = attachment
                            .result()
                            .await
                            .map_err(|e| AgentError::Failed(e.to_string()))?;
                        return Err(AgentError::Failed(format!(
                            "attachment ended: {:?}",
                            result.response
                        )));
                    }
                    _ => {}
                }
            }
        })
        .await
        .map_err(|_| AgentError::Failed("backend attachment timed out".into()))??;
        Ok(Self {
            client,
            backend,
            attachment,
            capabilities,
            message_timeout: timeout,
        })
    }
    pub async fn send(&self, request: impl Into<String>) -> Result<bool, AgentError> {
        let mut job = self
            .client
            .request(
                self.backend.clone(),
                JobParams {
                    name: Some("message".into()),
                    payload: Some(json!({"request":request.into()})),
                    timeout: Some(self.message_timeout),
                    ..Default::default()
                },
            )
            .map_err(|e| AgentError::Failed(e.to_string()))?;
        let result = job
            .result()
            .await
            .map_err(|e| AgentError::Failed(e.to_string()))?;
        if result.status != JobStatus::Completed {
            return Err(AgentError::Failed(format!(
                "backend refused message: {:?}",
                result.response
            )));
        }
        Ok(result
            .response
            .and_then(|p| p.get("backend").cloned())
            .as_ref()
            .and_then(Value::as_str)
            == Some("working"))
    }
    pub async fn next(&mut self) -> Result<Option<BackendEvent>, AgentError> {
        loop {
            match self
                .attachment
                .next_event()
                .await
                .map_err(|e| AgentError::Failed(e.to_string()))?
            {
                Some(JobEvent::Update(Some(value))) => {
                    return serde_json::from_value(value)
                        .map(Some)
                        .map_err(|e| AgentError::Failed(e.to_string()))
                }
                None => return Ok(None),
                _ => {}
            }
        }
    }
    pub async fn close(&self) -> Result<(), AgentError> {
        self.attachment
            .cancel(Some("frontend detached".into()))
            .await
            .map_err(Into::into)
    }
}
