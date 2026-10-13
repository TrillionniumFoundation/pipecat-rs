//! Capacity waiting is restricted to runtime-owned background work. It must not
//! turn ordinary synchronous bidirectional forwarding into a blocking cycle.
use super::*;
use crate::pipeline::stream::StreamAttempt;
use crate::FrameQueue;
use std::pin::Pin;
use std::task::Poll;
use tokio::sync::futures::Notified;

impl ProcessorContext {
    /// Whether downstream routing has a real adjacent processor. Boundaries
    /// can carry special roundtrips and terminal coordination instead.
    pub fn has_downstream_processor(&self) -> bool {
        self.index + 1 < self.runtime.queues.len()
    }
    /// Await finite downstream capacity from an owned `ctx.spawn` task.
    /// Only data/metadata is supported; Start, lifecycle/worker controls,
    /// settings, errors, interruption and Flush retain their dedicated routes.
    /// The pending frame belongs to this future, so dropping it admits nothing.
    /// Ordinary `push` remains nonblocking at every other pipeline hop.
    pub async fn push_downstream_wait(&self, frame: Frame) -> Result<(), ProcessorError> {
        self.require_owned_stream()?;
        if !stream_data(&frame.data) {
            return Err(ProcessorError::new(
                "bounded stream route only accepts data and metadata",
            ));
        }
        self.stream_wait(frame, false).await
    }
    /// Await an adjacent downstream lane for a compact or detailed Flush.
    /// Both returning states are accepted. Final-boundary Flush routing is
    /// deliberately excluded; callers must retain the ordinary roundtrip path.
    pub async fn push_downstream_flush_wait(&self, frame: Frame) -> Result<(), ProcessorError> {
        self.require_owned_stream()?;
        if !self.has_downstream_processor()
            || !matches!(
                frame.data,
                FrameData::Flush(_) | FrameData::FlushDetailed(_)
            )
        {
            return Err(ProcessorError::new(
                "bounded Flush route requires a Flush and an adjacent downstream processor",
            ));
        }
        self.stream_wait(frame, true).await
    }
    /// Await an adjacent downstream lane for End/EndDetailed/Stop from an owned
    /// task. Register that task after the runtime's prior-work drain, then return
    /// from `process`; global quiescence includes newly registered owned tasks.
    /// Never await this task from its own prepare_end/drain. Final-boundary
    /// terminal coordination and Cancel/worker requests keep ordinary routing.
    pub async fn push_downstream_terminal_wait(&self, frame: Frame) -> Result<(), ProcessorError> {
        self.require_owned_stream()?;
        if !self.has_downstream_processor()
            || !matches!(
                frame.data,
                FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop
            )
        {
            return Err(ProcessorError::new(
                "bounded terminal route requires End or Stop and an adjacent downstream processor",
            ));
        }
        self.stream_wait(frame, true).await
    }
    fn require_owned_stream(&self) -> Result<(), ProcessorError> {
        if BACKGROUND_INTERRUPTIBLE.try_with(|_| ()).is_err() {
            return Err(ProcessorError::new(
                "bounded downstream admission requires an owned background task",
            ));
        }
        Ok(())
    }
    fn stream_queues(&self) -> Vec<Arc<FrameQueue>> {
        let mut queues = vec![self.runtime.queues[self.index].clone()];
        if self.has_downstream_processor() {
            queues.push(self.runtime.queues[self.index + 1].clone());
        }
        queues.extend(
            self.origins
                .iter()
                .map(|origin| origin.runtime.queues[origin.index].clone()),
        );
        queues.sort_unstable_by_key(Arc::as_ptr);
        queues.dedup_by(|a, b| Arc::ptr_eq(a, b));
        queues
    }
    fn stream_is_stale(&self, frame: &Frame, generation: Option<u64>) -> bool {
        frame.interruptible && self.source_is_stale(generation)
    }
    async fn stream_wait(
        &self,
        mut frame: Frame,
        adjacent_only: bool,
    ) -> Result<(), ProcessorError> {
        let generation = self.frame_generation(&frame);
        let queues = self.stream_queues();
        if self.has_downstream_processor() {
            loop {
                tokio::task::consume_budget().await;
                let mut changed = subscribed(&queues);
                let attempt = self.with_admission(&self.runtime, || {
                    if self.stream_is_stale(&frame, generation) {
                        self.runtime.drop_frame();
                        StreamAttempt::Accepted
                    } else {
                        self.runtime
                            .stream_adjacent_unlocked(self.index, generation, frame)
                    }
                });
                match attempt {
                    StreamAttempt::Accepted => return Ok(()),
                    StreamAttempt::Full(returned) => frame = *returned,
                    StreamAttempt::Failed(error) => return Err(error),
                }
                any_change(&mut changed).await;
            }
        }
        if adjacent_only {
            return Err(ProcessorError::new(
                "bounded control route has no adjacent destination",
            ));
        }
        // Reservation is outside causal admission locks. Queue notifications
        // wake a full output wait when an inherited origin becomes stale.
        let output = self.runtime.stream_output_sender()?;
        let reservation = output.reserve_owned();
        tokio::pin!(reservation);
        loop {
            tokio::task::consume_budget().await;
            let mut changed = subscribed(&queues);
            let current =
                self.with_admission(&self.runtime, || -> Result<bool, ProcessorError> {
                    if self.stream_is_stale(&frame, generation) {
                        self.runtime.drop_frame();
                        return Ok(false);
                    }
                    self.runtime.stream_open()?;
                    Ok(true)
                })?;
            if !current {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = any_change(&mut changed) => {},
                permit = &mut reservation => {
                    let permit = permit.map_err(|_| ProcessorError::new("output is closed"))?;
                    return self.with_admission(&self.runtime, || {
                        if self.stream_is_stale(&frame, generation) { self.runtime.drop_frame(); return Ok(()); }
                        self.runtime.stream_open()?;
                        self.runtime.stream_output_unlocked(self.index, frame, permit);
                        Ok(())
                    });
                }
            }
        }
    }
}
fn subscribed(queues: &[Arc<FrameQueue>]) -> Vec<Pin<Box<Notified<'_>>>> {
    queues
        .iter()
        .map(|queue| {
            let mut changed = Box::pin(queue.admission_changed());
            changed.as_mut().enable();
            changed
        })
        .collect()
}
async fn any_change(changed: &mut [Pin<Box<Notified<'_>>>]) {
    std::future::poll_fn(|cx| {
        for future in changed.iter_mut() {
            if future.as_mut().poll(cx).is_ready() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    })
    .await
}
fn stream_data(data: &FrameData) -> bool {
    matches!(
        data,
        FrameData::OpenClawStarted(_)
            | FrameData::OpenClawText(_)
            | FrameData::OpenClawEnd(_)
            | FrameData::Text(_)
            | FrameData::LlmText(_)
            | FrameData::LlmSearchResponse(_)
            | FrameData::InputText(_)
            | FrameData::VisionText(_)
            | FrameData::AggregatedText(_)
            | FrameData::TtsText(_)
            | FrameData::TtsTextInput(_)
            | FrameData::TtsAlignmentDiagnostic(_)
            | FrameData::AggregatedTextProgress(_)
            | FrameData::Transcription(_)
            | FrameData::InterimTranscription(_)
            | FrameData::EagerTranscription(_)
            | FrameData::Translation(_)
            | FrameData::TranscriptionDetailed(_)
            | FrameData::InterimTranscriptionDetailed(_)
            | FrameData::EagerTranscriptionDetailed(_)
            | FrameData::TranslationDetailed(_)
            | FrameData::InputAudio(_)
            | FrameData::OutputAudio(_)
            | FrameData::TtsAudio(_)
            | FrameData::SpeechOutputAudio(_)
            | FrameData::InputImage(_)
            | FrameData::OutputImage(_)
            | FrameData::OutputImageDetailed(_)
            | FrameData::UrlImage(_)
            | FrameData::Sprite(_)
            | FrameData::InputFile(_)
            | FrameData::UserFile(_)
            | FrameData::TtsStarted(_)
            | FrameData::TtsStopped(_)
            | FrameData::TtsSpeak(_)
            | FrameData::LlmResponseStart
            | FrameData::LlmResponseEnd
            | FrameData::LlmMarker(_)
            | FrameData::LlmMarkerResponse(_)
            | FrameData::LlmThoughtStart
            | FrameData::LlmThoughtText(_)
            | FrameData::LlmThoughtEnd
            | FrameData::LlmThoughtStartDetailed(_)
            | FrameData::LlmThoughtEndDetailed(_)
            | FrameData::LlmFullResponseStart(_)
            | FrameData::LlmFullResponseEnd(_)
            | FrameData::VisionFullResponseStart(_)
            | FrameData::VisionFullResponseEnd(_)
            | FrameData::Metrics(_)
            | FrameData::ServiceMetadata(_)
            | FrameData::SttMetadata(_)
            | FrameData::LlmServiceMetadata(_)
    )
}
