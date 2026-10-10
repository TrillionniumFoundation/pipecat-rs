use crate::{AgentError, ToolContext, ToolRegistry, ToolResult};
use pipecat_core::{
    DirectedFrame, Direction, Frame, FrameData, FrameProcessor, FunctionCall,
    FunctionCallCancelFrame, FunctionCallInProgressFrame, FunctionCallResultFrame, Pipeline,
    PipelineConfig, PipelineSender, PipelineWorker, TextFrame, WorkerReport,
};
use pipecat_services::{
    CancellationToken, CompletionRequest, Context, LlmClient, Message, Role, StreamEvent,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};

pub const TURN_TEXT: &str = "pipecat.agent.turn_text";
pub const TURN_THOUGHT: &str = "pipecat.agent.turn_thought";
pub const HAS_TOOLS: &str = "pipecat.agent.has_tools";
pub const GENERATION: &str = "pipecat.agent.generation";
pub const IDLE: &str = "pipecat.agent.idle";
pub(crate) const FULL_GENERATION: &str = "pipecat.agent.full_generation";
pub(crate) const SURVIVES_INTERRUPTION: &str = "pipecat.agent.survives_interruption";

#[derive(Clone, Debug)]
pub struct LlmWorkerOptions {
    pub name: String,
    pub command_capacity: usize,
    pub max_context_bytes: usize,
    pub max_rounds: usize,
    pub max_tools_per_round: usize,
    pub max_deferred_frames: usize,
    pub pipeline: PipelineConfig,
}
impl Default for LlmWorkerOptions {
    fn default() -> Self {
        Self {
            name: "llm".into(),
            command_capacity: 32,
            max_context_bytes: 4 * 1024 * 1024,
            max_rounds: 32,
            max_tools_per_round: 64,
            max_deferred_frames: 128,
            pipeline: PipelineConfig::default(),
        }
    }
}
#[derive(Clone, Debug)]
pub struct LlmWorkerSnapshot {
    pub context: Context,
    pub working: bool,
    pub closed: bool,
    pub error: Option<String>,
}
#[derive(Clone, Debug, Default)]
struct Control {
    epoch: u64,
    full_epoch: u64,
    stop: bool,
}
enum Command {
    Messages {
        messages: Vec<Message>,
        replace: bool,
        run: bool,
        epoch: u64,
        reply: oneshot::Sender<Result<bool, AgentError>>,
    },
    End,
}
#[derive(Clone)]
pub struct LlmWorkerHandle {
    commands: mpsc::Sender<Command>,
    control: watch::Sender<Control>,
    state: watch::Receiver<LlmWorkerSnapshot>,
    output: PipelineSender,
}
impl LlmWorkerHandle {
    pub fn generation(&self) -> u64 {
        self.control.borrow().epoch
    }
    pub fn snapshot(&self) -> LlmWorkerSnapshot {
        self.state.borrow().clone()
    }
    pub fn watch(&self) -> watch::Receiver<LlmWorkerSnapshot> {
        self.state.clone()
    }
    /// Returns whether work was already in progress when these messages landed.
    pub async fn append(&self, messages: Vec<Message>, run: bool) -> Result<bool, AgentError> {
        self.messages(messages, false, run).await
    }
    pub async fn replace(&self, messages: Vec<Message>, run: bool) -> Result<bool, AgentError> {
        self.messages(messages, true, run).await
    }
    async fn messages(
        &self,
        messages: Vec<Message>,
        replace: bool,
        run: bool,
    ) -> Result<bool, AgentError> {
        if self.snapshot().closed {
            return Err(AgentError::Closed);
        }
        let epoch = self.control.borrow().epoch;
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(Command::Messages {
                messages,
                replace,
                run,
                epoch,
                reply,
            })
            .await
            .map_err(|_| AgentError::Closed)?;
        rx.await.map_err(|_| AgentError::Closed)?
    }
    /// Injects ordinary external traffic directly; it is never held for a tool.
    pub async fn queue_frame(&self, frame: Frame, direction: Direction) -> Result<(), AgentError> {
        if direction == Direction::Downstream {
            if matches!(frame.data, FrameData::Interruption) {
                self.interrupt();
                return Ok(());
            }
            if let Some((messages, replace, run)) = message_command(&frame)? {
                self.messages(messages, replace, run).await?;
                return Ok(());
            }
        }
        self.output.send(frame, direction).await.map_err(Into::into)
    }
    pub fn interrupt(&self) {
        self.control.send_modify(|c| {
            c.epoch = c.epoch.wrapping_add(1);
        });
    }
    /// Detaching a backend cancels even tools that survive ordinary interruptions.
    pub fn interrupt_all(&self) {
        self.control.send_modify(|c| {
            c.epoch = c.epoch.wrapping_add(1);
            c.full_epoch = c.full_epoch.wrapping_add(1);
        });
    }
    pub fn cancel(&self) {
        self.control.send_modify(|c| {
            c.epoch = c.epoch.wrapping_add(1);
            c.full_epoch = c.full_epoch.wrapping_add(1);
            c.stop = true;
        });
    }
    pub async fn end(&self) -> Result<(), AgentError> {
        self.commands
            .send(Command::End)
            .await
            .map_err(|_| AgentError::Closed)
    }
}
/// Owns both native model execution and its output pipeline. Each instance owns
/// independent history. Read output while running; output admission is bounded.
pub struct LlmWorker {
    handle: LlmWorkerHandle,
    pipeline: PipelineWorker,
    driver: Option<JoinHandle<Result<(), AgentError>>>,
}
impl LlmWorker {
    pub async fn start(
        client: LlmClient,
        mut request: CompletionRequest,
        tools: ToolRegistry,
        options: LlmWorkerOptions,
    ) -> Result<Self, AgentError> {
        Self::start_with_output(
            client,
            {
                request.context.tools = Some(tools.schemas());
                request
            },
            tools,
            options,
            vec![],
        )
        .await
    }
    /// Additional native processors run after the generated LLM/tool frames.
    pub async fn start_with_output(
        client: LlmClient,
        mut request: CompletionRequest,
        tools: ToolRegistry,
        options: LlmWorkerOptions,
        processors: Vec<Arc<dyn FrameProcessor>>,
    ) -> Result<Self, AgentError> {
        if options.name.is_empty()
            || [
                options.command_capacity,
                options.max_context_bytes,
                options.max_rounds,
                options.max_tools_per_round,
                options.max_deferred_frames,
            ]
            .contains(&0)
        {
            return Err(AgentError::Config(
                "worker name and capacities must be nonempty".into(),
            ));
        }
        request.context.tools = Some(tools.schemas());
        client.request_body(&request)?;
        check_context(&request.context, options.max_context_bytes)?;
        let processors = if processors.is_empty() {
            vec![Arc::new(pipecat_core::IdentityProcessor) as Arc<dyn FrameProcessor>]
        } else {
            processors
        };
        let pipeline = Pipeline::new(processors)
            .start(options.pipeline.clone())
            .await?;
        let output = pipeline.sender();
        let (commands, rx) = mpsc::channel(options.command_capacity);
        let (control, control_rx) = watch::channel(Control::default());
        let (state_tx, state) = watch::channel(LlmWorkerSnapshot {
            context: request.context.clone(),
            working: false,
            closed: false,
            error: None,
        });
        let handle = LlmWorkerHandle {
            commands,
            control,
            state,
            output: output.clone(),
        };
        let driver = tokio::spawn(drive(
            client, request, tools, options, rx, control_rx, state_tx, output,
        ));
        Ok(Self {
            handle,
            pipeline,
            driver: Some(driver),
        })
    }
    pub fn handle(&self) -> LlmWorkerHandle {
        self.handle.clone()
    }
    pub async fn recv(&mut self) -> Option<DirectedFrame> {
        while let Some(frame) = self.pipeline.recv().await {
            let control = self.handle.control.borrow().clone();
            let stale = frame
                .frame
                .metadata
                .get(GENERATION)
                .and_then(serde_json::Value::as_u64)
                .is_some_and(|generation| generation != control.epoch);
            // Tool settlements survive ordinary interruption for calls whose
            // handler explicitly opted out; text from the interrupted run does not.
            let settlement = frame
                .frame
                .metadata
                .get(FULL_GENERATION)
                .and_then(serde_json::Value::as_u64)
                == Some(control.full_epoch)
                && match &frame.frame.data {
                    FrameData::FunctionCallResult(_) => {
                        frame
                            .frame
                            .metadata
                            .get(SURVIVES_INTERRUPTION)
                            .and_then(serde_json::Value::as_bool)
                            == Some(true)
                    }
                    FrameData::FunctionCallCancel(_) => true,
                    _ => false,
                };
            if stale && !settlement {
                continue;
            }
            return Some(frame);
        }
        None
    }
    /// Drains remaining output before joining, so backpressure cannot deadlock shutdown.
    pub async fn join(&mut self) -> Result<WorkerReport, AgentError> {
        while self.pipeline.recv().await.is_some() {}
        let report = self.pipeline.join().await?;
        if let Some(driver) = self.driver.take() {
            driver
                .await
                .map_err(|e| AgentError::Failed(e.to_string()))??;
        }
        Ok(report)
    }
}
impl Drop for LlmWorker {
    fn drop(&mut self) {
        self.handle.cancel();
    }
}
fn check_context(context: &Context, max: usize) -> Result<(), AgentError> {
    let size = serde_json::to_vec(context)
        .map_err(|e| AgentError::Failed(e.to_string()))?
        .len();
    if size > max {
        return Err(AgentError::Capacity("conversation history".into()));
    }
    context.validate()?;
    Ok(())
}
#[derive(Clone, Default)]
struct RoundCancellation {
    model: CancellationToken,
    interrupt: CancellationToken,
    all: CancellationToken,
}
struct InvocationCancellation(CancellationToken);
impl Drop for InvocationCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
struct RoundResult {
    messages: Vec<Message>,
    run: bool,
    end: bool,
    deferred: Vec<(Frame, Direction)>,
}
struct Running {
    task: JoinHandle<Result<RoundResult, AgentError>>,
    cancel: RoundCancellation,
    generation: u64,
    full_generation: u64,
}
impl Drop for Running {
    fn drop(&mut self) {
        self.cancel.all.cancel();
        self.cancel.model.cancel();
        self.task.abort();
    }
}
#[allow(clippy::too_many_arguments)]
async fn drive(
    client: LlmClient,
    mut request: CompletionRequest,
    tools: ToolRegistry,
    options: LlmWorkerOptions,
    mut commands: mpsc::Receiver<Command>,
    mut control: watch::Receiver<Control>,
    state: watch::Sender<LlmWorkerSnapshot>,
    output: PipelineSender,
) -> Result<(), AgentError> {
    let mut running: Option<Running> = None;
    let mut pending = false;
    let mut ending = false;
    let mut rounds = 0;
    let mut pipeline_state = output.watch();
    let result=async {
        loop {
            if control.borrow().stop {return Err(AgentError::Cancelled);}
            if pending && running.is_none() && !control.has_changed().unwrap_or(true) {
                if rounds>=options.max_rounds { return Err(AgentError::Capacity("model/tool round limit".into())); }
                rounds+=1; pending=false;
                let cancel=RoundCancellation::default();
                let round_control=control.borrow().clone();
                let task=tokio::spawn(run_round(client.clone(),request.clone(),tools.clone(),options.clone(),output.clone(),cancel.clone(),round_control.clone()));
                running=Some(Running{task,cancel,generation:round_control.epoch,full_generation:round_control.full_epoch});
                state.send_modify(|s|s.working=true);
            }
            if ending && running.is_none() {
                tokio::select! { biased; _=control.changed()=>continue, result=output.end()=>result? }
                return Ok(());
            }
            enum Event { Control, Command(Option<Command>), Done(Result<Result<RoundResult,AgentError>,tokio::task::JoinError>), Pipeline }
            let event=tokio::select! { biased;
                _=control.changed()=>Event::Control,
                _=pipeline_state.changed()=>Event::Pipeline,
                result=async {(&mut running.as_mut().unwrap().task).await},if running.is_some()=>Event::Done(result),
                command=commands.recv(),if !ending=>Event::Command(command),
            };
            match event {
                Event::Control=>{
                    let ctl=control.borrow_and_update().clone();
                    if ctl.stop || control.has_changed().is_err() { return Err(AgentError::Cancelled); }
                    pending=false;
                    if let Some(running)=&running { running.cancel.model.cancel();running.cancel.interrupt.cancel();if ctl.full_epoch != running.full_generation {running.cancel.all.cancel();} }
                    // Tokens cancel work before pipeline backpressure can delay interruption.
                    let mut changed=control.clone();
                    tokio::select! {biased;_=changed.changed()=>{},result=output.send(Frame::new(FrameData::Interruption),Direction::Downstream)=>result?}
                },
                Event::Pipeline=>if pipeline_state.borrow_and_update().is_finished(){return Err(AgentError::Closed);},
                Event::Command(None)=>return Err(AgentError::Cancelled),
                Event::Command(Some(Command::End))=>ending=true,
                Event::Command(Some(Command::Messages{messages,replace,run,epoch,reply}))=>{
                    if epoch!=control.borrow().epoch {let _=reply.send(Err(AgentError::Cancelled));continue;}
                    if replace && running.is_some(){let _=reply.send(Err(AgentError::Failed("cannot replace conversation during a turn".into())));continue;}
                    let mut candidate=request.context.clone();
                    if replace {candidate.messages=messages;}else{candidate.messages.extend(messages);}
                    let checked=check_context(&candidate,options.max_context_bytes).and_then(|()|{let mut req=request.clone();req.context=candidate.clone();client.request_body(&req).map(|_|()).map_err(Into::into)});
                    if let Err(error)=checked {let _=reply.send(Err(error));continue;}
                    let busy=running.is_some()||pending;
                    request.context=candidate;
                    state.send_modify(|s|s.context=request.context.clone());
                    pending|=run;
                    if !busy && run {rounds=0;}
                    let _=reply.send(Ok(busy));
                },
                Event::Done(result)=>{
                    let generation=running.as_ref().unwrap().generation;
                    let interrupted=running.as_ref().is_some_and(|r|r.cancel.interrupt.is_cancelled()) || generation != control.borrow().epoch;
                    running.take();
                    match result.map_err(|e|AgentError::Failed(format!("model task failed: {e}")))? {
                        Ok(round)=>{
                            let mut candidate=request.context.clone();candidate.messages.extend(round.messages);
                            check_context(&candidate,options.max_context_bytes)?;
                            request.context=candidate;
                            state.send_modify(|s|s.context=request.context.clone());
                            pending|=round.run && !interrupted;
                            ending|=round.end;
                            for (mut frame,direction) in round.deferred {
                                if interrupted || generation != control.borrow().epoch {break;}
                                frame.metadata.insert(GENERATION.into(),generation.into());
                                if direction==Direction::Downstream {
                                    if let Some((messages,replace,run))=message_command(&frame)? {
                                        let mut candidate=request.context.clone();
                                        if replace {candidate.messages=messages;}else{candidate.messages.extend(messages);}
                                        check_context(&candidate,options.max_context_bytes)?;
                                        request.context=candidate;
                                        pending|=run;
                                        state.send_modify(|s|s.context=request.context.clone());
                                        continue;
                                    }
                                }
                                let mut changed=control.clone();
                                tokio::select! {biased;_=changed.changed()=>break,result=output.send(frame,direction)=>result?}
                            }
                        },
                        Err(AgentError::Cancelled)=>{},
                        Err(error)=>return Err(error),
                    }
                    if !pending {
                        state.send_modify(|s|s.working=false);
                        rounds=0;
                        let mut idle=Frame::new(FrameData::Data);idle.metadata.insert(IDLE.into(),true.into());idle.metadata.insert(GENERATION.into(),control.borrow().epoch.into());
                        tokio::select! {biased; _=control.changed()=>{}, result=output.send(idle,Direction::Downstream)=>result?}
                    }
                }
            }
        }
    }.await;
    drop(running);
    if let Err(error) = &result {
        state.send_modify(|s| {
            s.error = if matches!(error, AgentError::Cancelled | AgentError::Closed) {
                None
            } else {
                Some(error.to_string())
            }
        });
        let _ = output.cancel(error.to_string()).await;
    }
    state.send_modify(|s| {
        s.closed = true;
        s.working = false;
    });
    match result {
        Err(AgentError::Cancelled | AgentError::Closed) => Ok(()),
        other => other,
    }
}
async fn emit(
    output: &PipelineSender,
    cancel: &CancellationToken,
    mut frame: Frame,
    generation: u64,
) -> Result<(), AgentError> {
    frame.metadata.insert(GENERATION.into(), generation.into());
    tokio::select! {biased; _=cancel.cancelled()=>Err(AgentError::Cancelled),result=output.send(frame,Direction::Downstream)=>result.map_err(Into::into)}
}
async fn run_round(
    client: LlmClient,
    request: CompletionRequest,
    tools: ToolRegistry,
    options: LlmWorkerOptions,
    output: PipelineSender,
    cancel: RoundCancellation,
    control: Control,
) -> Result<RoundResult, AgentError> {
    let generation = control.epoch;
    let full_generation = control.full_epoch;
    let mut history = request.context.clone();
    let mut stream = client.stream_with_cancel(request, cancel.model.clone())?;
    emit(
        &output,
        &cancel.model,
        Frame::new(FrameData::LlmResponseStart),
        generation,
    )
    .await?;
    let mut text = String::new();
    let mut thought = String::new();
    let mut calls = Vec::new();
    let mut ended = false;
    while let Some(event) = tokio::select! {biased; _=cancel.model.cancelled()=>return Err(AgentError::Cancelled),event=stream.next()=>event}
    {
        match event? {
            StreamEvent::TextDelta(delta) => {
                text.push_str(&delta);
                emit(
                    &output,
                    &cancel.model,
                    Frame::new(FrameData::LlmText(TextFrame::llm(delta))),
                    generation,
                )
                .await?;
            }
            StreamEvent::ThoughtDelta(delta) => {
                thought.push_str(&delta);
                let mut frame = TextFrame::llm(delta);
                frame.skip_tts = Some(true);
                frame.append_to_context = false;
                emit(
                    &output,
                    &cancel.model,
                    Frame::new(FrameData::LlmThoughtText(frame)),
                    generation,
                )
                .await?;
            }
            StreamEvent::ToolCall(call) => {
                if calls.len() >= options.max_tools_per_round {
                    return Err(AgentError::Capacity("tools per round".into()));
                }
                calls.push(call);
            }
            StreamEvent::Finished { .. } => {
                ended = true;
                break;
            }
            _ => {}
        }
        if text.len() + thought.len() > options.max_context_bytes {
            return Err(AgentError::Capacity("model response".into()));
        }
    }
    if !ended {
        return Err(AgentError::Failed(
            "model stream ended without terminal event".into(),
        ));
    }
    if !calls.is_empty() {
        emit(
            &output,
            &cancel.model,
            Frame::new(FrameData::FunctionCallsStarted(
                calls
                    .iter()
                    .map(|c| FunctionCall {
                        function_name: c.name.clone(),
                        tool_call_id: c.id.clone(),
                        arguments: c.arguments.clone(),
                    })
                    .collect(),
            )),
            generation,
        )
        .await?;
    }
    let mut end = Frame::new(FrameData::LlmResponseEnd);
    end.metadata.insert(TURN_TEXT.into(), text.clone().into());
    end.metadata.insert(TURN_THOUGHT.into(), thought.into());
    end.metadata
        .insert(HAS_TOOLS.into(), (!calls.is_empty()).into());
    emit(&output, &cancel.model, end, generation).await?;
    let mut messages = vec![];
    if !text.is_empty() || !calls.is_empty() {
        let mut message = Message::text(Role::Assistant, text);
        message.tool_calls = calls.clone();
        messages.push(message);
    }
    if calls.is_empty() {
        return Ok(RoundResult {
            messages,
            run: false,
            end: false,
            deferred: vec![],
        });
    }
    // Validate the entire tool batch before any executable handler is invoked.
    history.messages.extend(messages.clone());
    check_context(&history, options.max_context_bytes)?;
    let mut tasks = JoinSet::new();
    let deferred = Arc::new(Mutex::new(vec![]));
    for (index, call) in calls.iter().cloned().enumerate() {
        let handler = tools.tools.get(&call.name).cloned();
        let output = output.clone();
        let cancel = cancel.clone();
        let deferred = deferred.clone();
        let worker = options.name.clone();
        let max_deferred = options.max_deferred_frames;
        tasks.spawn(async move {
            let options=handler.as_ref().map(|h|h.options.clone()).unwrap_or_default();
            let token=CancellationToken::new();
            let _invocation=InvocationCancellation(token.clone());
            let survives_interruption=!options.cancel_on_interruption;
            let ctx=ToolContext{worker,call:call.clone(),output:output.clone(),cancelled:token.clone(),deferred,max_deferred,generation,full_generation,survives_interruption};
            let invoke=async {
                emit(&output,&cancel.all,Frame::new(FrameData::FunctionCallInProgress(FunctionCallInProgressFrame{function_name:call.name.clone(),tool_call_id:call.id.clone(),arguments:call.arguments.clone(),cancel_on_interruption:options.cancel_on_interruption,group_id:None})),generation).await?;
                match handler {Some(handler)=>handler.call(call.arguments.clone(),ctx).await,None=>Err(AgentError::Failed(format!("unregistered tool: {}",call.name)))}
            };
            let result=tokio::select! {biased;
                _=cancel.all.cancelled()=>Err(AgentError::Cancelled),
                _=cancel.interrupt.cancelled(),if options.cancel_on_interruption=>Err(AgentError::Cancelled),
                _=async {match options.timeout {Some(t)=>tokio::time::sleep(t).await,None=>std::future::pending().await}}=>Err(AgentError::Failed(format!("tool timed out: {}",call.name))),
                result=invoke=>result,
            };
            (index,call,result,survives_interruption)
        });
    }
    let mut results = vec![None; calls.len()];
    let mut run = false;
    let mut ending = false;
    while let Some(joined) = tasks.join_next().await {
        let (index, call, result, survives_interruption) =
            joined.map_err(|e| AgentError::Failed(format!("tool task failed: {e}")))?;
        let (value, error, cancelled) = match result {
            Ok(ToolResult {
                result,
                run_llm,
                end,
            }) => {
                run |= run_llm;
                ending |= end;
                (result, None, false)
            }
            Err(AgentError::Cancelled) => (json!({"cancelled":true}), None, true),
            Err(error) => {
                run = true;
                (
                    json!({"error":error.to_string()}),
                    Some(error.to_string()),
                    false,
                )
            }
        };
        results[index] = Some(Message::tool_result(&call.id, value.to_string()));
        let data = if cancelled {
            FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: call.name,
                tool_call_id: call.id,
                run_llm: false,
            })
        } else {
            FrameData::FunctionCallResult(FunctionCallResultFrame {
                function_name: call.name,
                tool_call_id: call.id,
                arguments: call.arguments,
                result: value,
                run_llm: Some(false),
                is_final: true,
                error,
            })
        };
        if !cancel.all.is_cancelled() {
            let mut frame = Frame::new(data);
            frame
                .metadata
                .insert(FULL_GENERATION.into(), full_generation.into());
            frame
                .metadata
                .insert(SURVIVES_INTERRUPTION.into(), survives_interruption.into());
            emit(&output, &cancel.all, frame, generation).await?;
        }
    }
    messages.extend(results.into_iter().flatten());
    let frames = std::mem::take(
        &mut *deferred
            .lock()
            .map_err(|_| AgentError::Failed("tool frame lock poisoned".into()))?,
    );
    let deferred = if !cancel.interrupt.is_cancelled() && !cancel.all.is_cancelled() && !ending {
        frames
    } else {
        vec![]
    };
    Ok(RoundResult {
        messages,
        run,
        end: ending,
        deferred,
    })
}

fn message_command(frame: &Frame) -> Result<Option<(Vec<Message>, bool, bool)>, AgentError> {
    use pipecat_services::frame_context;
    Ok(Some(match &frame.data {
        FrameData::LlmRun => (vec![], false, true),
        FrameData::LlmMessagesAppend(messages) => (
            messages
                .messages
                .iter()
                .map(frame_context::legacy_message)
                .collect(),
            false,
            messages.run_llm,
        ),
        FrameData::LlmMessagesUpdate(messages) => (
            messages
                .messages
                .iter()
                .map(frame_context::legacy_message)
                .collect(),
            true,
            messages.run_llm,
        ),
        FrameData::LlmMessagesAppendDetailed(messages) => (
            messages
                .messages
                .iter()
                .map(frame_context::message)
                .collect::<Result<_, _>>()?,
            false,
            messages.run_llm.unwrap_or(false),
        ),
        FrameData::LlmMessagesUpdateDetailed(messages) => (
            messages
                .messages
                .iter()
                .map(frame_context::message)
                .collect::<Result<_, _>>()?,
            true,
            messages.run_llm.unwrap_or(false),
        ),
        FrameData::LlmSetTools(_)
        | FrameData::LlmMessagesTransform(_)
        | FrameData::LlmContext(_)
        | FrameData::LlmContextDetailed(_) => {
            return Err(AgentError::Unsupported(
                "use registered tools and owned context append/replace APIs".into(),
            ))
        }
        _ => return Ok(None),
    }))
}
