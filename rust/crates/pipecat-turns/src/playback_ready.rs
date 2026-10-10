//! A bounded, local acknowledgment of an assistant's real downstream boundary.
use pipecat_core::{
    Direction, Frame, FrameData, FrameObservationToken, InvocationFence, ProcessorContext,
    ProcessorError,
};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::oneshot;

const MAX_PENDING: usize = 16;
const MAX_CONTEXT_BYTES: usize = 256;

#[derive(Clone, PartialEq, Eq)]
enum Boundary {
    Source,
    Playback(String),
}
impl Boundary {
    fn from_frame(frame: &Frame) -> Result<Option<Self>, ProcessorError> {
        match &frame.data {
            FrameData::LlmResponseStart
            | FrameData::LlmFullResponseStart(_)
            | FrameData::VisionFullResponseStart(_) => Ok(Some(Self::Source)),
            FrameData::TtsStarted(start) => match &start.context_id {
                Some(id) if !id.is_empty() && id.len() <= MAX_CONTEXT_BYTES => {
                    Ok(Some(Self::Playback(id.clone())))
                }
                _ => Err(ProcessorError::new(
                    "assistant readiness requires a bounded TTS context ID",
                )),
            },
            _ => Ok(None),
        }
    }
}
struct Pending {
    id: u64,
    identity: FrameObservationToken,
    boundary: Boundary,
    cause: InvocationFence,
    accepted: bool,
    processing: bool,
    sender: oneshot::Sender<Result<InvocationFence, ProcessorError>>,
}
#[derive(Default)]
struct State {
    pending: Vec<Pending>,
    closed: bool,
    next_id: u64,
}
#[derive(Default)]
pub(crate) struct PlaybackReadiness(Arc<Mutex<State>>);

/// A single-use acknowledgment of one exact native source/TTS start. Register
/// before forwarding the frame to the assistant, then await outside all locks.
/// No pipeline task is created. Dropping this value releases its reservation.
/// The caller must impose an overall deadline and must not submit audio if the
/// boundary is rejected, purged, cancelled, or fails to become ready.
pub struct AssistantPlaybackReady {
    id: u64,
    state: Weak<Mutex<State>>,
    identity: FrameObservationToken,
    cause: InvocationFence,
    receiver: oneshot::Receiver<Result<InvocationFence, ProcessorError>>,
}
impl AssistantPlaybackReady {
    pub async fn wait(mut self) -> Result<(), ProcessorError> {
        loop {
            tokio::select! { biased;
                result = &mut self.receiver => {
                    let fence = result.map_err(|_| ProcessorError::new("assistant readiness owner closed"))??;
                    return if fence.is_current() { Ok(()) } else { Err(ProcessorError::new("assistant readiness scope expired before delivery")) };
                },
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                    let processing = self.state.upgrade().is_some_and(|state| state.lock().unwrap_or_else(|error| error.into_inner()).pending.iter().any(|pending| pending.id == self.id && pending.processing));
                    if (!processing && !self.identity.is_alive()) || !self.cause.is_current() {
                        return Err(ProcessorError::new("assistant readiness boundary was purged or cancelled"));
                    }
                }
            }
        }
    }
}
impl Drop for AssistantPlaybackReady {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .pending
                .retain(|pending| pending.id != self.id);
        }
    }
}
impl PlaybackReadiness {
    pub(crate) fn register(
        &self,
        frame: &Frame,
        cause: InvocationFence,
    ) -> Result<Option<AssistantPlaybackReady>, ProcessorError> {
        let Some(boundary) = Boundary::from_frame(frame)? else {
            return Ok(None);
        };
        if !cause.is_current() {
            return Err(ProcessorError::new("assistant readiness scope is stale"));
        }
        let identity = frame.observation_token();
        let mut state = self
            .0
            .lock()
            .map_err(|_| ProcessorError::fatal("assistant readiness lock poisoned"))?;
        state.pending.retain(|pending| {
            !pending.sender.is_closed() && (pending.processing || pending.identity.is_alive())
        });
        if state.closed
            || state.pending.len() >= MAX_PENDING
            || state
                .pending
                .iter()
                .any(|pending| pending.identity == identity)
        {
            return Err(ProcessorError::new(
                "assistant readiness is closed, duplicate, or full",
            ));
        }
        let id = state
            .next_id
            .checked_add(1)
            .ok_or_else(|| ProcessorError::new("assistant readiness identity exhausted"))?;
        state.next_id = id;
        let (sender, receiver) = oneshot::channel();
        state.pending.push(Pending {
            id,
            identity: identity.clone(),
            boundary,
            cause: cause.clone(),
            accepted: false,
            processing: false,
            sender,
        });
        Ok(Some(AssistantPlaybackReady {
            id,
            state: Arc::downgrade(&self.0),
            identity,
            cause,
            receiver,
        }))
    }
    pub(crate) fn processing(&self, frame: &Frame, direction: Direction) -> Processing {
        let identity = frame.observation_token();
        let id = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .pending
            .iter_mut()
            .find(|pending| pending.identity == identity)
            .map(|pending| {
                pending.processing = true;
                pending.id
            });
        Processing {
            state: Arc::downgrade(&self.0),
            id,
            downstream: direction == Direction::Downstream,
            finished: false,
        }
    }
    pub(crate) fn invalidate(&self, close: bool) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.closed |= close;
        state.pending.clear();
    }
}

pub(crate) struct Processing {
    state: Weak<Mutex<State>>,
    id: Option<u64>,
    downstream: bool,
    finished: bool,
}
impl Processing {
    pub(crate) fn accepted(
        &self,
        frame: &Frame,
        cause: Option<&ProcessorContext>,
    ) -> Result<(), ProcessorError> {
        if !self.downstream {
            return Ok(());
        }
        let (Some(id), Some(cause), Some(state)) = (self.id, cause, self.state.upgrade()) else {
            return Ok(());
        };
        let mut state = state
            .lock()
            .map_err(|_| ProcessorError::fatal("assistant readiness lock poisoned"))?;
        if let Some(pending) = state.pending.iter_mut().find(|pending| pending.id == id) {
            if pending.identity != frame.observation_token() {
                return Err(ProcessorError::new("assistant readiness identity changed"));
            }
            if Some(&pending.boundary) != Boundary::from_frame(frame)?.as_ref() {
                return Err(ProcessorError::new("assistant readiness boundary changed"));
            }
            let fence = pending.cause.intersection(&cause.invocation_fence()?)?;
            pending.cause = fence;
            pending.accepted = true;
        }
        Ok(())
    }
    pub(crate) fn finish(mut self, succeeded: bool) {
        self.resolve(succeeded);
        self.finished = true;
    }
    fn resolve(&self, succeeded: bool) {
        let (Some(id), Some(state)) = (self.id, self.state.upgrade()) else {
            return;
        };
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(index) = state.pending.iter().position(|pending| pending.id == id) else {
            return;
        };
        let pending = state.pending.remove(index);
        drop(state);
        let result =
            if self.downstream && succeeded && pending.accepted && pending.cause.is_current() {
                Ok(pending.cause.clone())
            } else {
                Err(ProcessorError::new(
                    "assistant did not accept the playback boundary",
                ))
            };
        let _ = pending.sender.send(result);
    }
}
impl Drop for Processing {
    fn drop(&mut self) {
        if !self.finished {
            self.resolve(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::{async_trait, FrameProcessor, Pipeline, PipelineWorker};
    #[derive(Default)]
    struct Capture(Mutex<Option<ProcessorContext>>);
    #[async_trait]
    impl FrameProcessor for Capture {
        fn name(&self) -> &str {
            "readiness-unit-origin"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::Text(_)) {
                *self.0.lock().unwrap() = Some(ctx.clone());
            }
            ctx.push(frame, direction).await
        }
    }
    async fn origin() -> (PipelineWorker, ProcessorContext) {
        let capture = Arc::new(Capture::default());
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(Default::default())
            .await
            .unwrap();
        let frame = Frame::text("origin");
        let id = frame.id;
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while let Some(frame) = worker.recv().await {
                if frame.frame.id == id {
                    break;
                }
            }
        })
        .await
        .unwrap();
        let ctx = capture.0.lock().unwrap().clone().unwrap();
        (worker, ctx)
    }
    #[tokio::test]
    async fn consumed_identity_remains_pending_until_its_processing_guard_resolves() {
        let (worker, ctx) = origin().await;
        let hub = PlaybackReadiness::default();
        let frame = Frame::new(FrameData::LlmResponseStart);
        let ready = hub
            .register(&frame, ctx.invocation_fence().unwrap())
            .unwrap()
            .unwrap();
        let processing = hub.processing(&frame, Direction::Downstream);
        processing.accepted(&frame, Some(&ctx)).unwrap();
        drop(frame);
        let other = Frame::new(FrameData::LlmResponseStart);
        let _other_ready = hub
            .register(&other, ctx.invocation_fence().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(hub.0.lock().unwrap().pending.len(), 2);
        let wait = ready.wait();
        tokio::pin!(wait);
        tokio::select! { result = &mut wait => panic!("consumed in-progress frame was rejected: {result:?}"), _ = tokio::time::sleep(std::time::Duration::from_millis(30)) => {} }
        processing.finish(true);
        wait.await.unwrap();
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
    }
    #[tokio::test]
    async fn old_ticket_and_old_processing_cannot_retire_or_accept_a_replacement_reservation() {
        let (worker, ctx) = origin().await;
        let hub = PlaybackReadiness::default();
        let frame = Frame::new(FrameData::LlmResponseStart);
        let first = hub
            .register(&frame, ctx.invocation_fence().unwrap())
            .unwrap()
            .unwrap();
        let processing = hub.processing(&frame, Direction::Downstream);
        processing.accepted(&frame, Some(&ctx)).unwrap();
        processing.finish(true);
        let second = hub
            .register(&frame, ctx.invocation_fence().unwrap())
            .unwrap()
            .unwrap();
        drop(first);
        assert_eq!(hub.0.lock().unwrap().pending.len(), 1);
        let old_processing = hub.processing(&frame, Direction::Downstream);
        drop(second);
        let third = hub
            .register(&frame, ctx.invocation_fence().unwrap())
            .unwrap()
            .unwrap();
        old_processing.accepted(&frame, Some(&ctx)).unwrap();
        old_processing.finish(true);
        {
            let state = hub.0.lock().unwrap();
            assert_eq!(state.pending.len(), 1);
            assert!(!state.pending[0].accepted);
        }
        let current = hub.processing(&frame, Direction::Downstream);
        current.accepted(&frame, Some(&ctx)).unwrap();
        current.finish(true);
        third.wait().await.unwrap();
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
    }
}
