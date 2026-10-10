use pipecat_core::{Direction, Frame, ProcessorContext, ProcessorError};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Pending,
    Released,
    Aborted,
}
struct State {
    remaining: usize,
    frame: Option<Frame>,
}
struct Fence {
    state: Mutex<State>,
    description: String,
    status: watch::Sender<Status>,
}

/// An owned queue entry: discarding any participant invalidates the whole fence.
pub(crate) struct FenceParticipant {
    fence: Arc<Fence>,
    completed: bool,
}
impl FenceParticipant {
    pub fn group(frame: Frame, destinations: usize) -> Vec<Self> {
        let (status, _) = watch::channel(Status::Pending);
        let fence = Arc::new(Fence {
            description: description(&frame),
            state: Mutex::new(State {
                remaining: destinations,
                frame: Some(frame),
            }),
            status,
        });
        (0..destinations)
            .map(|_| Self {
                fence: fence.clone(),
                completed: false,
            })
            .collect()
    }
    pub fn error(&self, message: &str) -> ProcessorError {
        ProcessorError::fatal(format!("{}: {message}", self.fence.description))
    }
    /// Wait for every destination's preceding media, forwarding the marker once
    /// before releasing any destination to process its later queue entries.
    pub async fn complete(&mut self, ctx: &ProcessorContext) -> Result<bool, ProcessorError> {
        let mut status = self.fence.status.subscribe();
        let frame = {
            let mut state = self.fence.state.lock().unwrap();
            if *status.borrow() == Status::Aborted {
                return Ok(false);
            }
            state.remaining -= 1;
            if state.remaining == 0 {
                state.frame.take()
            } else {
                None
            }
        };
        if let Some(frame) = frame {
            ctx.push(frame, Direction::Downstream).await?;
            self.fence.status.send_replace(Status::Released);
        }
        loop {
            match *status.borrow_and_update() {
                Status::Released => {
                    self.completed = true;
                    return Ok(true);
                }
                Status::Aborted => return Ok(false),
                Status::Pending => {}
            }
            status
                .changed()
                .await
                .map_err(|_| self.error("coordinator closed before completion"))?;
        }
    }
}
impl Drop for FenceParticipant {
    fn drop(&mut self) {
        if !self.completed {
            self.fence.status.send_if_modified(|status| {
                if *status == Status::Pending {
                    *status = Status::Aborted;
                    true
                } else {
                    false
                }
            });
        }
    }
}
pub(crate) fn description(frame: &Frame) -> String {
    let flow_id = frame
        .metadata
        .get("pipecat.flows.barrier")
        .and_then(serde_json::Value::as_u64);
    format!(
        "media flush fence frame {} (flow barrier {:?})",
        frame.id, flow_id
    )
}
