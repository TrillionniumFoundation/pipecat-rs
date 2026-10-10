//! Narrow routing primitives for owned downstream capacity waits. Callers hold
//! the same ordered causal admission locks used by ordinary routing.
use super::*;

pub(crate) enum StreamAttempt {
    Accepted,
    Full(Box<Frame>),
    Failed(ProcessorError),
}
impl Runtime {
    pub(crate) fn stream_open(&self) -> Result<(), ProcessorError> {
        if self.is_finished() {
            Err(ProcessorError::new("pipeline is closed"))
        } else {
            Ok(())
        }
    }
    pub(crate) fn stream_output_sender(
        &self,
    ) -> Result<mpsc::Sender<DirectedFrame>, ProcessorError> {
        self.output
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| ProcessorError::new("output is closed"))
    }
    pub(crate) fn stream_adjacent_unlocked(
        &self,
        from: usize,
        generation: Option<u64>,
        frame: Frame,
    ) -> StreamAttempt {
        if let Err(error) = self.stream_open() {
            return StreamAttempt::Failed(error);
        }
        let destination = from + 1;
        if destination >= self.queues.len() {
            return StreamAttempt::Failed(ProcessorError::new("no adjacent downstream processor"));
        }
        if frame.interruptible
            && generation.is_some_and(|epoch| self.queues[destination].generation() > epoch)
        {
            self.drop_frame();
            return StreamAttempt::Accepted;
        }
        let observation = self.stream_observation(from, &frame);
        match self.queues[destination]
            .try_send_unlocked(DirectedFrame::new(frame, Direction::Downstream), generation)
        {
            Ok(()) => {
                self.stream_observe(from, Some(destination), observation);
                StreamAttempt::Accepted
            }
            Err((QueueError::Full, returned)) => StreamAttempt::Full(Box::new(returned.frame)),
            Err((QueueError::Closed, _)) => {
                StreamAttempt::Failed(ProcessorError::new("downstream queue is closed"))
            }
        }
    }
    pub(crate) fn stream_output_unlocked(
        &self,
        from: usize,
        frame: Frame,
        permit: mpsc::OwnedPermit<DirectedFrame>,
    ) {
        let observation = self.stream_observation(from, &frame);
        permit.send(DirectedFrame::new(frame, Direction::Downstream));
        self.stream_observe(from, None, observation);
    }
    fn stream_observation(&self, from: usize, frame: &Frame) -> Option<StreamObservation> {
        self.observers.enabled().then(|| StreamObservation {
            frame: ObservedFrame::capture(frame),
            runtimes: frame.observed_runtimes.clone(),
            timestamp_ns: self.observers.clock.now_ns(),
            captures: self.observers.capture_push(
                frame,
                &self.processor_info[from],
                self.nodes[from].health.snapshot(),
            ),
        })
    }
    fn stream_observe(
        &self,
        from: usize,
        destination: Option<usize>,
        observation: Option<StreamObservation>,
    ) {
        if let Some(observation) = observation {
            let first_push = observation
                .runtimes
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .insert(self.observers.id);
            self.observers.emit_captured(
                PipelineObservation::Push {
                    source: self.processor_info[from].clone(),
                    destination: destination.map(|index| self.processor_info[index].clone()),
                    frame: observation.frame,
                    direction: Direction::Downstream,
                    first_push,
                    timestamp_ns: observation.timestamp_ns,
                },
                observation.captures,
            );
        }
    }
}
struct StreamObservation {
    captures: Vec<Option<crate::CapturedPush>>,
    frame: ObservedFrame,
    runtimes: Arc<std::sync::OnceLock<Mutex<BTreeSet<u64>>>>,
    timestamp_ns: u64,
}
