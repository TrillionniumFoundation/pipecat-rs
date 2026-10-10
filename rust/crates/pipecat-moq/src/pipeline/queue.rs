use super::*;
use std::{collections::VecDeque, io::Write};

pub(super) struct Item {
    pub frame: Frame,
    pub generation: u64,
    pub context: Option<ProcessorContext>,
    bytes: usize,
}
pub(super) enum Next {
    Item(Box<Item>),
    Interrupted(u64),
    End,
    Cancel,
}
struct State {
    items: VecDeque<Item>,
    bytes: usize,
    generation: u64,
    ending: bool,
    cancelled: bool,
}
/// Both count and serialized payload bytes are bounded. One popped item may
/// additionally be held by its single consumer, and one by a waiting producer.
pub(super) struct Queue {
    state: Mutex<State>,
    changed: watch::Sender<u64>,
    capacity: usize,
    bytes: usize,
    max_frame_bytes: usize,
    timeout: Duration,
}
impl Queue {
    pub fn new(config: &MoqTransportConfig, generation: u64) -> Self {
        Self {
            state: Mutex::new(State {
                items: VecDeque::new(),
                bytes: 0,
                generation,
                ending: false,
                cancelled: false,
            }),
            changed: watch::channel(0).0,
            capacity: config.queue_capacity,
            bytes: config.queue_byte_capacity,
            max_frame_bytes: config.max_frame_bytes,
            timeout: config.admission_timeout,
        }
    }
    fn notify(&self) {
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
    }
    #[cfg(test)]
    pub async fn push(&self, frame: Frame, generation: u64, urgent: bool) -> Result<()> {
        self.push_with_context(frame, generation, urgent, None)
            .await
    }
    pub async fn push_with_context(
        &self,
        frame: Frame,
        generation: u64,
        urgent: bool,
        context: Option<ProcessorContext>,
    ) -> Result<()> {
        let bytes = frame_size(&frame, self.max_frame_bytes)?;
        if bytes > self.bytes {
            return Err(Error::Limit("pipeline queue frame bytes"));
        }
        let mut changed = self.changed.subscribe();
        let deadline = tokio::time::Instant::now() + self.timeout;
        loop {
            {
                let mut state = self.state.lock().unwrap();
                if state.cancelled || state.ending {
                    return Err(Error::Closed);
                }
                if generation > state.generation {
                    state.generation = generation;
                    state
                        .items
                        .retain(|item| !item.frame.interruptible || item.generation == generation);
                    state.bytes = state.items.iter().map(|item| item.bytes).sum();
                }
                if frame.interruptible && generation < state.generation {
                    return Ok(());
                }
                if state.items.len() < self.capacity && bytes <= self.bytes - state.bytes {
                    state.bytes += bytes;
                    state.items.push_back(Item {
                        frame,
                        generation,
                        bytes,
                        context,
                    });
                    drop(state);
                    self.notify();
                    return Ok(());
                }
            }
            if urgent {
                return Err(Error::Backpressure);
            }
            tokio::time::timeout_at(deadline, changed.changed())
                .await
                .map_err(|_| Error::Timeout("pipeline admission"))?
                .map_err(|_| Error::Closed)?;
        }
    }
    pub async fn next(&self, seen: &mut u64) -> Next {
        let mut changed = self.changed.subscribe();
        loop {
            {
                let mut state = self.state.lock().unwrap();
                if state.cancelled {
                    return Next::Cancel;
                }
                if state.generation != *seen {
                    *seen = state.generation;
                    return Next::Interrupted(*seen);
                }
                if let Some(item) = state.items.pop_front() {
                    state.bytes -= item.bytes;
                    drop(state);
                    self.notify();
                    return Next::Item(Box::new(item));
                }
                if state.ending {
                    return Next::End;
                }
            }
            let _ = changed.changed().await;
        }
    }
    pub fn interrupt(&self, generation: u64) {
        let mut state = self.state.lock().unwrap();
        state.generation = state.generation.max(generation);
        let current = state.generation;
        state
            .items
            .retain(|item| !item.frame.interruptible || item.generation == current);
        state.bytes = state.items.iter().map(|item| item.bytes).sum();
        drop(state);
        self.notify();
    }
    pub fn end(&self) {
        self.state.lock().unwrap().ending = true;
        self.notify();
    }
    pub fn cancel(&self) {
        let mut state = self.state.lock().unwrap();
        state.cancelled = true;
        state.items.clear();
        state.bytes = 0;
        drop(state);
        self.notify();
    }
}
pub(super) fn frame_size(frame: &Frame, limit: usize) -> Result<usize> {
    struct Counter {
        used: usize,
        limit: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.used) {
                return Err(std::io::Error::other("frame limit"));
            }
            self.used += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    frame
        .native_llm_continuation()
        .map_err(|error| Error::Protocol(error.to_string()))?;
    let retained = frame.native_llm_continuation_retained_bytes();
    if retained > limit {
        return Err(Error::Limit("pipeline native continuation bytes"));
    }
    // Count the local frame envelope without invoking a wire encoder: an opaque
    // native continuation must remain attached and must never be serialized.
    #[derive(serde::Serialize)]
    struct Envelope<'a> {
        name: &'a str,
        metadata: &'a std::collections::BTreeMap<String, serde_json::Value>,
        source: &'a Option<String>,
        destination: &'a Option<String>,
        data: Option<&'a FrameData>,
    }
    let envelope = Envelope {
        name: &frame.name,
        metadata: &frame.metadata,
        source: &frame.transport_source,
        destination: &frame.transport_destination,
        data: (!matches!(frame.data, FrameData::LlmContinuationCheckpoint)).then_some(&frame.data),
    };
    let mut counter = Counter {
        used: retained.saturating_add(64),
        limit,
    };
    serde_json::to_writer(&mut counter, &envelope)
        .map_err(|_| Error::Limit("pipeline frame bytes"))?;
    Ok(counter.used)
}
