//! Explicit payload admission for pipelines which carry serializable data.
//!
//! The unit is compact JSON bytes, including public metadata and routing labels.
//! It is not allocator capacity or RSS. Native executable/opaque payloads require
//! a different resource contract and are rejected when this profile is enabled.
use crate::{Frame, QueueError};
use serde::Serialize;
use std::io::{self, Write};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;

/// Optional strict limits on the real pipeline queues. Start and cancellation/interruption each have a single
/// frame reservation. Media, including system-priority input, cannot consume
/// those reservations.
/// Worker output is separately item-bounded and uses `max_frame_bytes` per item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadLimits {
    pub max_frame_bytes: usize,
    pub normal_queue_bytes: usize,
    pub system_queue_bytes: usize,
}
impl Default for PayloadLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            normal_queue_bytes: 8 * 1024 * 1024,
            system_queue_bytes: 1024 * 1024,
        }
    }
}
impl PayloadLimits {
    pub(crate) fn validate(self) -> Result<(), &'static str> {
        if self.max_frame_bytes == 0
            || self.normal_queue_bytes < self.max_frame_bytes
            || self.system_queue_bytes < self.max_frame_bytes
        {
            return Err("payload limits require positive per-frame bytes fitting each queue lane");
        }
        Ok(())
    }
    /// Measure without allocating a serialized copy. Pipeline-clock timestamps
    /// are local and are not being exported by this measurement.
    pub fn measure(self, frame: &Frame) -> Result<usize, QueueError> {
        if frame.has_unbounded_native_payload() {
            return Err(QueueError::UnmeasurablePayload);
        }
        #[derive(Serialize)]
        struct Payload<'a> {
            name: &'a str,
            metadata: &'a std::collections::BTreeMap<String, serde_json::Value>,
            transport_source: &'a Option<String>,
            transport_destination: &'a Option<String>,
            data: &'a crate::FrameData,
        }
        let mut counter = Counter {
            remaining: self.max_frame_bytes,
            used: 0,
            depth: 0,
            quoted: false,
            escaped: false,
            failure: None,
        };
        let result = serde_json::to_writer(
            &mut counter,
            &Payload {
                name: &frame.name,
                metadata: &frame.metadata,
                transport_source: &frame.transport_source,
                transport_destination: &frame.transport_destination,
                data: &frame.data,
            },
        );
        result.map_err(|_| counter.failure.unwrap_or(QueueError::UnmeasurablePayload))?;
        Ok(counter.used)
    }
}
struct Counter {
    remaining: usize,
    used: usize,
    depth: usize,
    quoted: bool,
    escaped: bool,
    failure: Option<QueueError>,
}
impl Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.remaining {
            self.failure = Some(QueueError::PayloadTooLarge);
            return Err(io::Error::other("frame payload exceeds its byte limit"));
        }
        // Bound traversal of locally constructed JSON, not only input JSON.
        for &byte in bytes {
            if self.quoted {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.quoted = false;
                }
            } else {
                match byte {
                    b'"' => self.quoted = true,
                    b'[' | b'{' => {
                        self.depth += 1;
                        if self.depth > 128 {
                            self.failure = Some(QueueError::UnmeasurablePayload);
                            return Err(io::Error::other("payload nesting exceeds 128"));
                        }
                    }
                    b']' | b'}' => self.depth = self.depth.saturating_sub(1),
                    _ => {}
                }
            }
        }
        self.remaining -= bytes.len();
        self.used += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) struct ByteLane {
    used: AtomicUsize,
    limit: usize,
    changed: Arc<Notify>,
}
impl ByteLane {
    pub(crate) fn new(limit: usize, changed: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit,
            changed,
        })
    }
    pub(crate) fn reserve(self: &Arc<Self>, bytes: usize) -> Option<ByteLease> {
        self.used
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .ok()?;
        Some(ByteLease {
            lane: self.clone(),
            bytes,
        })
    }
    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }
}
pub(crate) struct ByteLease {
    lane: Arc<ByteLane>,
    bytes: usize,
}
impl Drop for ByteLease {
    fn drop(&mut self) {
        self.lane.used.fetch_sub(self.bytes, Ordering::SeqCst);
        self.lane.changed.notify_waiters();
    }
}
