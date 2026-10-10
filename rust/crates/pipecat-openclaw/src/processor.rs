use crate::{
    GatewayClient, GatewayConfig, GatewayError, RemoteOutcome, RunEvent, RunHandle, RunStatus,
};
use pipecat_core::{
    async_trait, Direction, ErrorFrame, Frame, FrameData, FrameProcessor, OpenClawEndFrame,
    OpenClawStartedFrame, OpenClawTextFrame, ProcessorContext, ProcessorError, ProcessorHealth,
    StartFrame,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::{oneshot, Semaphore};

struct Active {
    serial: u64,
    handle: RunHandle,
    ended: bool,
}
#[derive(Default)]
struct State {
    client: Option<GatewayClient>,
    active: Option<Active>,
    serial: u64,
    tail: Option<oneshot::Receiver<()>>,
}
/// Typed agent-run processor. Commands are bounded and ordered; abort uses a
/// separate tracked task and never waits behind a normal request or output.
/// End/Stop close observation before pipeline terminal output. Cancel/drop stop
/// ownership without claiming the remote agent is no longer executing.
pub struct OpenClawGatewayProcessor {
    config: GatewayConfig,
    state: Arc<Mutex<State>>,
    emission: Arc<tokio::sync::Mutex<()>>,
    stopping: Arc<AtomicBool>,
    permits: Arc<Semaphore>,
    health: ProcessorHealth,
}
impl OpenClawGatewayProcessor {
    pub fn new(config: GatewayConfig) -> crate::Result<Self> {
        config.validate()?;
        Ok(Self {
            permits: Arc::new(Semaphore::new(config.limits.max_pending)),
            config,
            state: Arc::new(Mutex::new(State::default())),
            emission: Arc::new(tokio::sync::Mutex::new(())),
            stopping: Arc::new(AtomicBool::new(false)),
            health: Default::default(),
        })
    }
    pub fn client(&self) -> Option<GatewayClient> {
        self.state.lock().unwrap().client.clone()
    }
    fn client_required(&self) -> std::result::Result<GatewayClient, ProcessorError> {
        self.client()
            .ok_or_else(|| processor_error(GatewayError::Closed))
    }
    async fn shutdown_without_output(&self) {
        self.stopping.store(true, Ordering::Release);
        let (active, client) = {
            let mut state = self.state.lock().unwrap();
            (state.active.take(), state.client.take())
        };
        if let Some(client) = client {
            if let Some(active) = active.filter(|a| !a.ended) {
                let _ = tokio::time::timeout(
                    self.config.limits.close_timeout,
                    client.abort(&active.handle),
                )
                .await;
            }
            client.disconnect().await;
        }
    }
    fn cancel(&self) {
        self.stopping.store(true, Ordering::Release);
        let mut state = self.state.lock().unwrap();
        state.active = None;
        if let Some(client) = state.client.take() {
            client.cancel();
        }
    }
}
impl Drop for OpenClawGatewayProcessor {
    fn drop(&mut self) {
        self.cancel();
    }
}
#[async_trait]
impl FrameProcessor for OpenClawGatewayProcessor {
    fn name(&self) -> &str {
        "OpenClawGatewayService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    fn process_when_unusable(&self, frame: &Frame, _: Direction) -> bool {
        !matches!(
            frame.data,
            FrameData::OpenClawSend(_) | FrameData::OpenClawSteer(_)
        )
    }
    async fn setup(
        &self,
        _: &StartFrame,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if self.stopping.load(Ordering::Acquire) || self.client().is_some() {
            return Err(processor_error(GatewayError::Invalid(
                "processor already used",
            )));
        }
        let client = GatewayClient::connect(self.config.clone())
            .await
            .map_err(processor_error)?;
        if !ctx.is_usable() {
            client.disconnect().await;
            return Ok(());
        }
        self.state.lock().unwrap().client = Some(client);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            self.shutdown_without_output().await;
            return ctx.push(frame, direction).await;
        }
        if direction != Direction::Downstream {
            return ctx.push(frame, direction).await;
        }
        if matches!(frame.data, FrameData::OpenClawAbort(_)) {
            let client = self.client_required()?;
            let handle = self
                .state
                .lock()
                .unwrap()
                .active
                .as_ref()
                .filter(|a| !a.ended)
                .map(|a| a.handle.clone());
            if let Some(handle) = handle {
                let output = ctx.clone();
                let generation = ctx.health().generation();
                ctx.spawn("openclaw-abort", false, async move {
                    if let Err(error) = client.abort(&handle).await {
                        report(&output, error, generation).await?;
                    }
                    Ok(())
                })?;
            }
            return Ok(());
        }
        if !matches!(
            frame.data,
            FrameData::OpenClawSend(_) | FrameData::OpenClawSteer(_)
        ) {
            return ctx.push(frame, direction).await;
        }
        if self.stopping.load(Ordering::Acquire) {
            return Err(processor_error(GatewayError::Closed));
        }
        let client = self.client_required()?;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| processor_error(GatewayError::Capacity("processor commands")))?;
        let (finished, tail) = oneshot::channel();
        let previous = self.state.lock().unwrap().tail.replace(tail);
        let state = self.state.clone();
        let emission = self.emission.clone();
        let stopping = self.stopping.clone();
        let output = ctx.clone();
        let generation = ctx.health().generation();
        let output_timeout = self.config.limits.output_timeout;
        ctx.spawn("openclaw-command",false,async move {
            let _permit=permit;let _finished=finished;
            if let Some(previous)=previous {let _=previous.await;}
            if stopping.load(Ordering::Acquire)||!output.is_usable()||output.health().generation()!=generation{return Ok(());}
            match frame.data {
                FrameData::OpenClawSend(send)=>{
                    let old=state.lock().unwrap().active.as_ref().filter(|a|!a.ended).map(|a|a.handle.clone());
                    if let Some(old)=old {
                        let aborted=client.abort(&old).await;
                        if let Err(error)=&aborted {report(&output,error.clone(),generation).await?;}
                        let _guard=emission.lock().await;
                        if stopping.load(Ordering::Acquire){return Ok(());}
                        let terminal={let mut s=state.lock().unwrap();s.active.take().filter(|a|!a.ended)};
                        if let Some(active)=terminal {
                            let remote_outcome=if matches!(aborted,Ok(true)){RemoteOutcome::Settled}else{RemoteOutcome::Unknown};
                            emit(&output,Frame::new(FrameData::OpenClawEnd(OpenClawEndFrame{run_id:active.handle.identity().run_id,status:RunStatus::Cancelled,text:String::new(),remote_outcome})),output_timeout).await?;
                        }
                        let _=client.end_observation(&old);
                    }
                    if stopping.load(Ordering::Acquire){return Ok(());}
                    let mut run=match client.start(send.message,send.session_key.as_deref()).await {
                        Ok(run)=>run,Err(error)=>{report(&output,error,generation).await?;return Ok(());}
                    };
                    let _guard=emission.lock().await;
                    if stopping.load(Ordering::Acquire)||!output.is_usable()||output.health().generation()!=generation{let _=client.end_observation(&run.handle());return Ok(());}
                    let serial={let mut s=state.lock().unwrap();s.serial=s.serial.checked_add(1).ok_or_else(||ProcessorError::fatal("OpenClaw generation exhausted"))?;let serial=s.serial;s.active=Some(Active{serial,handle:run.handle(),ended:false});serial};
                    emit(&output,Frame::new(FrameData::OpenClawStarted(OpenClawStartedFrame{run_id:run.identity().run_id})),output_timeout).await?;
                    let events_output=output.clone();let events_state=state.clone();let events_emission=emission.clone();let events_stop=stopping.clone();
                    output.spawn("openclaw-run-events",false,async move {
                        let mut health=events_output.health().watch();
                        loop {
                            let event=tokio::select!{biased;_=health.changed()=>{let _=client.end_observation(&run.handle());return Ok(());},event=run.next()=>event};
                            let Some(event)=event else{return Ok(());};
                            let _guard=events_emission.lock().await;
                            if events_stop.load(Ordering::Acquire)||!events_output.is_usable()||events_output.health().generation()!=generation{return Ok(());}
                            let current=events_state.lock().unwrap().active.as_ref().is_some_and(|a|a.serial==serial&&!a.ended);
                            if !current{return Ok(());}
                            match event {
                                RunEvent::Text{text,..}=>emit(&events_output,Frame::new(FrameData::OpenClawText(OpenClawTextFrame{run_id:run.identity().run_id,text})),output_timeout).await?,
                                RunEvent::End(end)=>{
                                    if let Some(active)=events_state.lock().unwrap().active.as_mut(){if active.serial==serial{active.ended=true;}}
                                    emit(&events_output,Frame::new(FrameData::OpenClawEnd(OpenClawEndFrame{run_id:end.run_id,status:end.status,text:end.text,remote_outcome:end.remote_outcome})),output_timeout).await?;
                                    if end.status==RunStatus::Failed {
                                        let error=if end.remote_outcome==RemoteOutcome::Settled{GatewayError::Rejected("remote run failed")}else{GatewayError::OutcomeUnknown("run observation ended")};
                                        report(&events_output,error,generation).await?;
                                    }
                                    return Ok(());
                                }
                            }
                        }
                    })?;
                },
                FrameData::OpenClawSteer(steer)=>{
                    let handle=state.lock().unwrap().active.as_ref().filter(|a|!a.ended).map(|a|a.handle.clone());
                    if let Some(handle)=handle {if let Err(error)=client.steer(&handle,steer.message).await {report(&output,error,generation).await?;}}
                },
                _=>(),
            }
            Ok(())
        })?;
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> std::result::Result<(), ProcessorError> {
        self.stopping.store(true, Ordering::Release);
        let _guard = self.emission.lock().await;
        let (active, client) = {
            let mut state = self.state.lock().unwrap();
            (state.active.take(), state.client.take())
        };
        if let Some(active) = active.filter(|a| !a.ended) {
            let remote_outcome = if let Some(client) = &client {
                if matches!(
                    tokio::time::timeout(
                        self.config.limits.close_timeout,
                        client.abort(&active.handle)
                    )
                    .await,
                    Ok(Ok(true))
                ) {
                    RemoteOutcome::Settled
                } else {
                    RemoteOutcome::Unknown
                }
            } else {
                RemoteOutcome::Unknown
            };
            let output = ctx.clone();
            let deadline = self.config.limits.output_timeout;
            ctx.spawn("openclaw-shutdown-end", false, async move {
                emit(
                    &output,
                    Frame::new(FrameData::OpenClawEnd(OpenClawEndFrame {
                        run_id: active.handle.identity().run_id,
                        status: RunStatus::Cancelled,
                        text: String::new(),
                        remote_outcome,
                    })),
                    deadline,
                )
                .await
            })?;
        }
        if let Some(client) = client {
            client.disconnect().await;
        }
        Ok(())
    }
    async fn cleanup(&self) -> std::result::Result<(), ProcessorError> {
        self.shutdown_without_output().await;
        Ok(())
    }
}
fn processor_error(error: GatewayError) -> ProcessorError {
    ProcessorError::new(error.to_string()).with_permanence_override(false)
}
async fn report(
    ctx: &ProcessorContext,
    error: GatewayError,
    generation: pipecat_core::ProcessorHealthGeneration,
) -> std::result::Result<(), ProcessorError> {
    let mut frame = ErrorFrame::new(error.to_string());
    frame.fatal = false;
    ctx.push_error_if_health_generation(frame, false, generation)
        .await
        .map(|_| ())
}

async fn emit(
    ctx: &ProcessorContext,
    frame: Frame,
    deadline: std::time::Duration,
) -> std::result::Result<(), ProcessorError> {
    tokio::time::timeout(deadline, ctx.push_downstream_wait(frame))
        .await
        .map_err(|_| processor_error(GatewayError::Timeout("pipeline output")))?
}
