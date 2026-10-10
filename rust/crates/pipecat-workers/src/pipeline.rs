//! Receive-side bus adapter for a core pipeline. Outputs remain on the original
//! `pipecat_core::PipelineWorker`; this adapter never rebroadcasts input frames.
use crate::{BusError, LocalBus, MessageData, WorkerReady, WorkerRegistration, WorkerRegistry};
use pipecat_core::{Direction, Frame, FrameData, PipelineError, PipelineSender, TtsSpeakFrame};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::watch;
use tokio::task::JoinSet;

#[derive(Debug, Clone)]
pub struct PipelineInputOptions {
    pub active: bool,
    /// None disables frame inputs, an empty list accepts every bridge.
    pub bridges: Option<Vec<String>>,
}
impl Default for PipelineInputOptions {
    fn default() -> Self {
        Self {
            active: false,
            bridges: Some(vec![]),
        }
    }
}
#[derive(Debug, Clone, thiserror::Error)]
pub enum PipelineInputError {
    #[error(transparent)]
    Bus(#[from] BusError),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error("pipeline input task failed: {0}")]
    Task(String),
    #[error("worker already registered: {0}")]
    Duplicate(String),
}
/// Owns two dispatch lanes. A blocked normal frame enqueue cannot block bus
/// cancellation. Keep this handle and continue consuming the pipeline's output.
/// Dropping the handle cancels the adapter and its pipeline.
pub struct PipelineBusInput {
    active: Arc<AtomicBool>,
    stop: watch::Sender<bool>,
    done: watch::Receiver<Option<Result<(), PipelineInputError>>>,
}
impl PipelineBusInput {
    pub fn attach(
        name: impl Into<String>,
        bus: LocalBus,
        registry: WorkerRegistry,
        sender: PipelineSender,
        options: PipelineInputOptions,
    ) -> Result<Self, PipelineInputError> {
        let name = name.into();
        let subscription = Arc::new(bus.subscribe_target(name.clone())?);
        if !registry.register_entry(WorkerRegistration {
            ready: WorkerReady {
                worker_name: name.clone(),
                runner: registry.runner_name().into(),
            },
            parent: None,
            active: options.active,
            bridged: options.bridges.is_some(),
        }) {
            return Err(PipelineInputError::Duplicate(name));
        }
        let active = Arc::new(AtomicBool::new(options.active));
        let (stop, mut stop_rx) = watch::channel(false);
        let (done_tx, done) = watch::channel(None);
        let handle = Self {
            active: active.clone(),
            stop,
            done,
        };
        tokio::spawn(async move {
            let mut tasks: JoinSet<Result<(), PipelineInputError>> = JoinSet::new();
            let data_sub = subscription.clone();
            let data_sender = sender.clone();
            let data_active = active.clone();
            let data_name = name.clone();
            let data_registry = registry.clone();
            tasks.spawn(async move {
                while let Some(message) = data_sub.recv_data().await? {
                    match message.data {
                        MessageData::Activate { .. } => {
                            data_active.store(true, Ordering::SeqCst);
                            data_registry.set_active(&data_name, true);
                        }
                        MessageData::Deactivate => {
                            data_active.store(false, Ordering::SeqCst);
                            data_registry.set_active(&data_name, false);
                        }
                        MessageData::EndWorker { .. } | MessageData::End { .. } => {
                            data_sender.end().await?;
                            return Ok(());
                        }
                        MessageData::Speak {
                            text,
                            append_to_context,
                        } if data_active.load(Ordering::SeqCst) => {
                            data_sender
                                .send(
                                    Frame::new(FrameData::TtsSpeak(TtsSpeakFrame {
                                        text,
                                        append_to_context,
                                    })),
                                    Direction::Downstream,
                                )
                                .await?
                        }
                        MessageData::Frame {
                            mut frame,
                            direction,
                            bridge,
                        } if data_active.load(Ordering::SeqCst)
                            && message.source != data_name
                            && options.bridges.as_ref().is_some_and(|bridges| {
                                bridges.is_empty()
                                    || bridge
                                        .as_ref()
                                        .is_some_and(|bridge| bridges.contains(bridge))
                            }) =>
                        {
                            frame
                                .metadata
                                .insert(crate::bridge::BUS_ORIGIN.into(), message.source.into());
                            data_sender.send(*frame, direction).await?;
                        }
                        _ => {}
                    }
                }
                Err(PipelineInputError::Bus(BusError::Closed))
            });
            let system_sub = subscription.clone();
            let system_sender = sender.clone();
            tasks.spawn(async move {
                while let Some(message) = system_sub.recv_system().await? {
                    if let MessageData::CancelWorker { reason } | MessageData::Cancel { reason } =
                        message.data
                    {
                        system_sender
                            .cancel(reason.unwrap_or_else(|| "bus cancellation".into()))
                            .await?;
                        return Ok(());
                    }
                }
                Err(PipelineInputError::Bus(BusError::Closed))
            });
            let mut pipeline_state = sender.watch();
            let result = loop {
                if pipeline_state.borrow_and_update().is_finished() {
                    break Ok(());
                }
                tokio::select! {
                    biased;
                    _=stop_rx.changed()=>{let result=sender.cancel("pipeline bus input dropped").await.map_err(Into::into);break result;},
                    changed=pipeline_state.changed()=>if changed.is_err() {break Err(PipelineInputError::Task("pipeline state closed".into()));},
                    result=tasks.join_next(),if !tasks.is_empty()=>match result {
                        Some(Ok(Err(error)))=>{let _=sender.cancel("pipeline bus input failed").await;break Err(error);},
                        Some(Err(error))=>{let _=sender.cancel("pipeline bus input panicked").await;break Err(PipelineInputError::Task(error.to_string()));},_=>{},
                    },
                }
            };
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            drop(subscription);
            registry.unregister(&name, registry.runner_name());
            active.store(false, Ordering::SeqCst);
            done_tx.send_replace(Some(result));
        });
        Ok(handle)
    }
    pub fn active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
    pub fn cancel(&self) {
        self.stop.send_replace(true);
    }
    /// Waits for adapter cleanup. The core worker's `join` remains the authority
    /// for pipeline execution errors and its final report.
    pub async fn wait(&mut self) -> Result<(), PipelineInputError> {
        loop {
            if let Some(result) = self.done.borrow_and_update().clone() {
                return result;
            }
            self.done
                .changed()
                .await
                .map_err(|_| PipelineInputError::Task("adapter stopped".into()))?;
        }
    }
}
