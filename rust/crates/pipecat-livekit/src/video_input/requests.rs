//! Plain pipeline requests for the next post-drain SDK sink observation.
//! No claim of camera acquisition freshness: SDK keepalive/replay is indistinguishable.
use super::*;
use pipecat_core::{
    Direction, Frame, FrameData, InvocationFence, NativeUserImageSource, ProcessorContext,
};
use std::{sync::Weak, time::Duration};
use tokio::time::Instant as Deadline;

const RGB_LIMIT: usize = 786_432;
const HEADER_LIMIT: usize = 16 * 1024;

/// Opt-in, pipeline-only plain requests. `None` destination matches exactly `None`.
/// A response observes a native sink frame after a reader-established empty-queue
/// fence following acceptance. Pixels may be SDK keepalive or sender replay;
/// this is NOT proof of a new capture, changed pixels or remote delivery.
#[derive(Debug, Clone)]
pub struct ImageRequestConfig {
    pub destination: Option<String>,
    pub max_requests: usize,
    pub max_requests_per_source: usize,
    pub max_request_ids: usize,
    /// Conservatively charged header copies retained through the final carrier.
    pub max_header_bytes: usize,
    /// Maximum reserved requested RGB storage, separate from ordinary video.
    pub max_rgb_bytes: usize,
    pub timeout: Duration,
}
impl Default for ImageRequestConfig {
    fn default() -> Self {
        Self {
            destination: None,
            max_requests: 32,
            max_requests_per_source: 8,
            max_request_ids: 4096,
            max_header_bytes: 65_536,
            max_rgb_bytes: 8 * RGB_LIMIT,
            timeout: Duration::from_secs(5),
        }
    }
}
impl ImageRequestConfig {
    pub(super) fn validate(&self) -> Result<()> {
        if !(1..=32).contains(&self.max_requests)
            || !(1..=8).contains(&self.max_requests_per_source)
            || self.max_requests_per_source > self.max_requests
            || !(1..=4096).contains(&self.max_request_ids)
            || !(1..=65_536).contains(&self.max_header_bytes)
            || !(RGB_LIMIT..=32 * RGB_LIMIT).contains(&self.max_rgb_bytes)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(60)
            || self.destination.as_deref().is_some_and(|s| !valid_id(s))
        {
            return Err(Error::InvalidConfig("invalid plain image request bounds"));
        }
        Ok(())
    }
}
fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

pub(super) struct Lease {
    request: Frame,
    _slot: OwnedSemaphorePermit,
    _header: OwnedSemaphorePermit,
    _rgb: OwnedSemaphorePermit,
}
struct Entry {
    token: u64,
    key: Key,
    stamp: Stamp,
    deadline: Deadline,
    source: NativeUserImageSource,
    fence: InvocationFence,
    waiting: Option<Arc<Lease>>,
    output: Option<ProcessorContext>,
    retained: Weak<Lease>,
    closed: bool,
}
#[derive(Default)]
pub(super) struct Requests {
    entries: Vec<Entry>,
    seen: Vec<u64>,
    closed: bool,
    revision: u64,
}
#[derive(Clone)]
pub(super) struct RequestResources {
    slots: Arc<Semaphore>,
    headers: Arc<Semaphore>,
    rgb: Arc<Semaphore>,
}
impl RequestResources {
    pub(super) fn new(config: Option<&ImageRequestConfig>) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(config.map_or(0, |c| c.max_requests))),
            headers: Arc::new(Semaphore::new(config.map_or(0, |c| c.max_header_bytes))),
            rgb: Arc::new(Semaphore::new(config.map_or(0, |c| c.max_rgb_bytes))),
        }
    }
}
pub(super) struct Selected {
    pub(super) token: u64,
    source: NativeUserImageSource,
    pub(super) fence: InvocationFence,
    pub(super) output: ProcessorContext,
    lease: Arc<Lease>,
}
impl Selected {
    pub(super) fn bind(self, frame: &mut Frame) -> Result<ProcessorContext> {
        let FrameData::UserImageRequest(request) = &self.lease.request.data else {
            unreachable!()
        };
        let FrameData::UserImage(image) = &mut frame.data else {
            unreachable!()
        };
        image.text = request.text.clone();
        image.append_to_context = request.append_to_context;
        image.request = Some(Box::new(request.clone()));
        self.source
            .bind(
                frame,
                &self.lease.request,
                self.fence.clone(),
                self.lease.clone(),
            )
            .map_err(|_| Error::Video("plain image binding rejected"))?;
        Ok(self.output)
    }
}

impl CaptureHandle {
    pub(crate) fn handles_request(&self, frame: &Frame, direction: Direction) -> bool {
        direction == Direction::Upstream
            && matches!(frame.data, FrameData::UserImageRequest(_))
            && self
                .config
                .requests
                .as_ref()
                .is_some_and(|c| c.destination == frame.transport_destination)
    }
    /// All request errors are local diagnostics. No callback authority is accepted.
    pub(crate) fn request(&self, frame: Frame, ctx: &ProcessorContext) {
        let result = self.admit_request(frame, ctx);
        if let Err(error) = result {
            self.stats.reject_request(error.to_string());
        }
    }
    fn admit_request(&self, frame: Frame, ctx: &ProcessorContext) -> Result<()> {
        let config = self
            .config
            .requests
            .as_ref()
            .ok_or(Error::Video("image requests disabled"))?;
        let FrameData::UserImageRequest(request) = &frame.data else {
            return Err(Error::Video("not an image request"));
        };
        if request.function_name.is_some()
            || request.tool_call_id.is_some()
            || request.result_callback.is_some()
        {
            return Err(Error::Video(
                "plain image requests reject tool/callback authority",
            ));
        }
        if !valid_id(&request.user_id)
            || request.user_id.capacity() > 256
            || request
                .text
                .as_ref()
                .is_some_and(|s| s.len() > 8192 || s.capacity() > 8192)
            || request
                .video_source
                .as_ref()
                .is_some_and(|s| s.capacity() > 256)
            || frame
                .transport_destination
                .as_ref()
                .is_some_and(|s| s.capacity() > 256)
            || !frame.metadata.is_empty()
            || frame.transport_source.is_some()
            || frame.name.len() > 256
            || frame.name.capacity() > 256
            || frame.broadcast_sibling_id.is_some()
            || frame.require_no_native_llm_continuation().is_err()
        {
            return Err(Error::Video("unsupported plain image request fields"));
        }
        let source = match request.video_source.as_deref() {
            None | Some("") | Some("camera") => VideoSource::Camera,
            Some("screenVideo") => VideoSource::ScreenVideo,
            _ => return Err(Error::Video("unsupported image request source")),
        };
        let key = Key {
            identity: request.user_id.clone(),
            source,
        };
        // Retain only the accepted public request header/payload, not arbitrary
        // upstream local tags or observer bookkeeping. This snapshot grants no
        // authority: private token + original invocation + source binding do.
        let mut snapshot = Frame::new(frame.data);
        snapshot.id = frame.id;
        snapshot.name = frame.name;
        snapshot.pts = frame.pts;
        snapshot.transport_destination = frame.transport_destination;
        snapshot.interruptible = frame.interruptible;
        let frame = snapshot;
        // The accepted subset is flat and every string has already been bounded.
        let header = serde_json::to_vec(&frame)?.len();
        if header > HEADER_LIMIT {
            return Err(Error::Limit("image request header"));
        }
        // Original Frame + response fields + nested request + private seal/header.
        // This conservative charge includes fixed bounded metadata/name storage.
        let FrameData::UserImageRequest(payload) = &frame.data else {
            unreachable!()
        };
        let original_capacity = frame.name.capacity()
            + frame
                .transport_destination
                .as_ref()
                .map_or(0, String::capacity)
            + payload.user_id.capacity()
            + payload.text.as_ref().map_or(0, String::capacity)
            + payload.video_source.as_ref().map_or(0, String::capacity);
        let charged = header
            .checked_mul(4)
            .and_then(|n| n.checked_add(original_capacity))
            .and_then(|n| n.checked_add(2048))
            .ok_or(Error::Limit("image request header copies"))?;
        self.maintain_requests();
        let fence = ctx.invocation_fence().map_err(|_| Error::Interrupted)?;
        let accepted = fence
            .with_current(|| {
                let mut state = self.gates.lock().unwrap();
                if state.closed || state.requests.closed || self.cancel.is_cancelled() {
                    return Err(Error::Closed);
                }
                let stamp = state
                    .active
                    .get(&key)
                    .cloned()
                    .ok_or(Error::Video("image source unavailable"))?;
                if !valid_id(&stamp.sid) {
                    return Err(Error::Video("unsupported requested track SID"));
                }
                if !state.policies.contains_key(&key) {
                    let mode = if self.config.sources.is_empty() {
                        VideoCaptureMode::AllFrames
                    } else {
                        *self
                            .config
                            .sources
                            .get(&source)
                            .ok_or(Error::Video("image source not captured"))?
                    };
                    self.insert(&mut state, key.clone(), mode.params()?)?;
                }
                if state.requests.seen.contains(&frame.id) {
                    return Err(Error::Video("duplicate image request"));
                }
                if state.requests.seen.len() >= config.max_request_ids {
                    return Err(Error::Limit("image request replay ledger"));
                }
                if state
                    .requests
                    .entries
                    .iter()
                    .filter(|e| e.key == key)
                    .count()
                    >= config.max_requests_per_source
                {
                    return Err(Error::Limit("retained image requests per source"));
                }
                let slot = self
                    .request_resources
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Limit("retained image requests"))?;
                let header = self
                    .request_resources
                    .headers
                    .clone()
                    .try_acquire_many_owned(charged as u32)
                    .map_err(|_| Error::Limit("retained image request headers"))?;
                let rgb = self
                    .request_resources
                    .rgb
                    .clone()
                    .try_acquire_many_owned(RGB_LIMIT as u32)
                    .map_err(|_| Error::Limit("retained requested RGB"))?;
                let token = state
                    .requests
                    .revision
                    .checked_add(1)
                    .ok_or(Error::Limit("image request sequence"))?;
                state.requests.seen.push(frame.id);
                let lease = Arc::new(Lease {
                    request: frame,
                    _slot: slot,
                    _header: header,
                    _rgb: rgb,
                });
                state.requests.entries.push(Entry {
                    token,
                    key,
                    stamp,
                    deadline: Deadline::now() + config.timeout,
                    source: NativeUserImageSource::new(),
                    fence: fence.clone(),
                    retained: Arc::downgrade(&lease),
                    waiting: Some(lease),
                    output: Some(ctx.clone()),
                    closed: false,
                });
                state.requests.revision = token;
                Ok(())
            })
            .unwrap_or(Err(Error::Interrupted));
        if accepted.is_ok() {
            self.request_changed.notify_waiters();
        }
        accepted
    }
    /// Snapshot fences first: never acquire runtime admission or wake source
    /// watchers while holding the transport gate.
    pub(crate) fn maintain_requests(&self) {
        let fences: Vec<_> = self
            .gates
            .lock()
            .unwrap()
            .requests
            .entries
            .iter()
            .filter(|e| !e.closed)
            .map(|e| (e.token, e.fence.clone()))
            .collect();
        let stale: Vec<_> = fences
            .into_iter()
            .filter_map(|(id, fence)| (!fence.is_current()).then_some(id))
            .collect();
        let (sources, expired, last_failure) = {
            let mut state = self.gates.lock().unwrap();
            let now = Deadline::now();
            let mut sources = Vec::new();
            let mut expired = 0;
            let mut last_failure = None;
            let closed = state.closed || self.cancel.is_cancelled() || state.requests.closed;
            let active = state.active.clone();
            for entry in &mut state.requests.entries {
                if !entry.closed
                    && (closed
                        || entry.deadline <= now
                        || stale.contains(&entry.token)
                        || active.get(&entry.key) != Some(&entry.stamp))
                {
                    last_failure = Some(if closed {
                        "image request input closed"
                    } else if entry.deadline <= now {
                        "image request expired"
                    } else if stale.contains(&entry.token) {
                        "image request invocation expired"
                    } else {
                        "image request source invalidated"
                    });
                    entry.closed = true;
                    entry.waiting.take();
                    entry.output.take();
                    expired += 1;
                }
                // Closing is deliberately idempotent. Another maintenance call
                // may mark closed and be descheduled before publishing closure;
                // lifecycle callers must still close these retained sources
                // themselves before returning. No transport lock spans close.
                if entry.closed {
                    sources.push(entry.source.clone());
                }
            }
            state
                .requests
                .entries
                .retain(|entry| entry.retained.strong_count() > 0);
            (sources, expired, last_failure)
        };
        for source in sources {
            source.close();
        }
        if let Some(reason) = last_failure {
            *self.stats.last.lock().unwrap() = Some(reason.into());
        }
        if expired > 0 {
            self.stats
                .failed_requests
                .fetch_add(expired, Ordering::Relaxed);
        }
    }
    pub(crate) fn dispatch_request(&self, request: super::RequestedImage) {
        if !self.current_request(request.token) {
            return;
        }
        self.stats.dispatch_attempts.fetch_add(1, Ordering::Relaxed);
        // No transport/source lock spans runtime routing or its wakeups.
        if request
            .output
            .try_push_with_invocation_fence(request.frame, Direction::Downstream, &request.fence)
            .is_err()
        {
            self.fail_request(request.token, "requested image routing rejected");
        }
    }
    pub(crate) fn close_requests(&self) {
        self.gates.lock().unwrap().requests.closed = true;
        self.maintain_requests();
        self.request_changed.notify_waiters();
    }
    pub(super) fn fail_request(&self, token: u64, reason: &'static str) {
        let source = {
            let mut state = self.gates.lock().unwrap();
            state
                .requests
                .entries
                .iter_mut()
                .find(|e| e.token == token)
                .map(|e| {
                    let newly_closed = !e.closed;
                    e.closed = true;
                    e.waiting.take();
                    e.output.take();
                    (e.source.clone(), newly_closed)
                })
        };
        if let Some((source, newly_closed)) = source {
            source.close();
            if newly_closed {
                self.stats.failed_requests.fetch_add(1, Ordering::Relaxed);
                self.stats.reject_request(reason.into());
            }
        }
    }
    pub(super) fn request_cutoff(&self, key: &Key, stamp: &Stamp) -> u64 {
        let state = self.gates.lock().unwrap();
        state
            .requests
            .entries
            .iter()
            .filter(|e| e.key == *key && e.stamp == *stamp && !e.closed && e.waiting.is_some())
            .map(|e| e.token)
            .max()
            .unwrap_or(0)
    }
    /// Selection is fenced before transport locking; cadence changes only for
    /// a current eligible request. Later acceptance never rebinds old frames.
    pub(super) fn sample_requested(
        &self,
        key: &Key,
        stamp: &Stamp,
        cutoff: u64,
    ) -> Result<(bool, Option<Selected>)> {
        self.maintain_requests();
        let candidate = {
            let state = self.gates.lock().unwrap();
            state
                .requests
                .entries
                .iter()
                .find(|e| {
                    e.key == *key
                        && e.stamp == *stamp
                        && !e.closed
                        && e.waiting.is_some()
                        && e.token <= cutoff
                })
                .map(|e| (e.token, e.fence.clone()))
        };
        if let Some((token, fence)) = candidate {
            if let Some(result) = fence.with_current(|| {
                let mut state = self.gates.lock().unwrap();
                if state.closed
                    || self.cancel.is_cancelled()
                    || state.active.get(key) != Some(stamp)
                {
                    return Ok((false, None));
                }
                let Some(index) = state.requests.entries.iter().position(|e| {
                    e.token == token
                        && !e.closed
                        && e.waiting.is_some()
                        && e.deadline > Deadline::now()
                }) else {
                    return Ok((false, None));
                };
                let now = self.origin.elapsed();
                #[cfg(test)]
                let now = state.now.unwrap_or(now);
                let sampler = state
                    .policies
                    .get_mut(key)
                    .expect("accepted request policy");
                sampler
                    .add_request(())
                    .map_err(|_| Error::Video("request sampler invariant"))?;
                assert!(sampler.sample(now).0);
                let entry = &mut state.requests.entries[index];
                Ok((
                    true,
                    Some(Selected {
                        token,
                        source: entry.source.clone(),
                        fence: entry.fence.clone(),
                        output: entry.output.take().expect("waiting invocation"),
                        lease: entry.waiting.take().expect("waiting lease"),
                    }),
                ))
            }) {
                return result;
            }
        }
        self.sample(key, stamp).map(|due| (due, None))
    }
    pub(super) fn current_request(&self, token: u64) -> bool {
        self.maintain_requests();
        self.gates
            .lock()
            .unwrap()
            .requests
            .entries
            .iter()
            .any(|e| e.token == token && !e.closed)
    }
}

#[cfg(test)]
#[path = "request_ledger_tests.rs"]
mod tests;
