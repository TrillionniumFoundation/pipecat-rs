//! Native RTVI 2.2 processor and explicit frame-observer taps.
//! See the README for output-transport timing and observer integration contracts.
mod google;
mod observer;
mod processor;
pub use google::*;
pub use observer::*;
pub use pipecat_protocol::rtvi as wire;
pub use processor::*;

use pipecat_core::{Frame, FrameData, ProcessorError};
use serde::Serialize;
use std::io;

fn bounded_size(value: &impl Serialize, limit: usize) -> Result<(), ProcessorError> {
    struct Counter {
        size: usize,
        limit: usize,
    }
    impl io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.size = self.size.saturating_add(bytes.len());
            if self.size > self.limit {
                return Err(io::Error::other("RTVI payload too large"));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter { size: 0, limit }, value).map_err(error)
}
fn error(e: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::new(e.to_string())
}
fn transport_frame(envelope: wire::Envelope, limit: usize) -> Result<Frame, ProcessorError> {
    bounded_size(&envelope, limit)?;
    Ok(Frame::new(FrameData::OutputTransportMessageUrgent(
        serde_json::to_value(envelope).map_err(error)?,
    )))
}

fn check_json_depth(value: &serde_json::Value) -> Result<(), ProcessorError> {
    fn visit(value: &serde_json::Value, depth: usize) -> Result<(), ProcessorError> {
        if depth > 64 {
            return Err(error("RTVI JSON nesting limit exceeded"));
        }
        match value {
            serde_json::Value::Array(values) => {
                for value in values {
                    visit(value, depth + 1)?;
                }
            }
            serde_json::Value::Object(values) => {
                for value in values.values() {
                    visit(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    visit(value, 0)
}
