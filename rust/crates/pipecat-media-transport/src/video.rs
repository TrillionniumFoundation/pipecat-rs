//! Bounded source sampling driven by monotonic arrival time, not wall time.
use pipecat_core::ProcessorError;
use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

/// Maximum retained participant/source identifier length, in UTF-8 bytes.
pub const VIDEO_IDENTIFIER_BYTES: usize = 256;
macro_rules! identifier {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);
        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ProcessorError> {
                let value = value.into();
                if value.is_empty() || value.len() > VIDEO_IDENTIFIER_BYTES {
                    return Err(ProcessorError::new("invalid video identifier length"));
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}
identifier!(VideoParticipantId);
identifier!(VideoSourceId);
impl VideoSourceId {
    pub fn camera() -> Self {
        Self("camera".into())
    }
    pub fn screen() -> Self {
        Self("screenVideo".into())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct VideoSourceKey {
    pub participant: VideoParticipantId,
    pub source: VideoSourceId,
}
impl VideoSourceKey {
    pub fn new(participant: VideoParticipantId, source: VideoSourceId) -> Self {
        Self {
            participant,
            source,
        }
    }
}

/// `None` passes every frame; zero captures on request; positive values sample
/// that many frames per second. Rates above nanosecond resolution are rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VideoInSourceParams {
    pub framerate: Option<u32>,
}
impl Default for VideoInSourceParams {
    fn default() -> Self {
        Self {
            framerate: Some(30),
        }
    }
}
impl VideoInSourceParams {
    pub const fn all_frames() -> Self {
        Self { framerate: None }
    }
    pub const fn on_request_only() -> Self {
        Self { framerate: Some(0) }
    }
    pub fn validate(self) -> Result<(), ProcessorError> {
        if self.framerate.is_some_and(|rate| rate > 1_000_000_000) {
            Err(ProcessorError::new(
                "video framerate exceeds clock resolution",
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
pub struct VideoInputConfig {
    /// Sources a backend must capture before announcing a participant connected.
    pub sources: BTreeMap<VideoSourceId, VideoInSourceParams>,
    /// Only requests with this exact transport destination belong to this input.
    pub request_destination: Option<String>,
    pub max_sources: usize,
    pub max_pending_requests: usize,
    /// Session lifetime replay identities. Exhaustion rejects new requests;
    /// old identities are never evicted, including on participant removal.
    pub max_request_ids: usize,
    pub max_request_bytes: usize,
}
impl Default for VideoInputConfig {
    fn default() -> Self {
        Self {
            sources: BTreeMap::new(),
            request_destination: None,
            max_sources: 128,
            max_pending_requests: 128,
            max_request_ids: 4096,
            max_request_bytes: 256 * 1024,
        }
    }
}
impl VideoInputConfig {
    pub(crate) fn validate(&self) -> Result<(), ProcessorError> {
        if self.max_sources == 0
            || self.max_pending_requests == 0
            || self.max_request_ids == 0
            || self.max_request_bytes == 0
            || self.sources.len() > self.max_sources
            || self
                .request_destination
                .as_ref()
                .is_some_and(|id| id.is_empty() || id.len() > VIDEO_IDENTIFIER_BYTES)
        {
            return Err(ProcessorError::new("invalid video input bounds"));
        }
        for params in self.sources.values() {
            params.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct VideoInSampler<T> {
    params: VideoInSourceParams,
    next_time: Option<Duration>,
    requests: VecDeque<T>,
    request_limit: usize,
}
impl<T> VideoInSampler<T> {
    pub fn new(params: VideoInSourceParams, request_limit: usize) -> Result<Self, ProcessorError> {
        params.validate()?;
        if request_limit == 0 {
            return Err(ProcessorError::new("video request limit is zero"));
        }
        Ok(Self {
            params,
            next_time: None,
            requests: VecDeque::new(),
            request_limit,
        })
    }
    pub fn params(&self) -> VideoInSourceParams {
        self.params
    }
    /// Updates preserve the schedule and every waiting request.
    pub fn set_params(&mut self, params: VideoInSourceParams) -> Result<(), ProcessorError> {
        params.validate()?;
        self.params = params;
        Ok(())
    }
    pub fn add_request(&mut self, request: T) -> Result<(), ProcessorError> {
        if self.requests.len() >= self.request_limit {
            return Err(ProcessorError::new("video request queue is full"));
        }
        self.requests.push_back(request);
        Ok(())
    }
    pub fn take_requests(&mut self) -> Vec<T> {
        self.requests.drain(..).collect()
    }
    /// Supply a nondecreasing monotonic timestamp. Requests reset the schedule;
    /// early frames use min(50ms, half interval) jitter tolerance.
    pub fn sample(&mut self, now: Duration) -> (bool, Option<T>) {
        let requested = !self.requests.is_empty();
        let due = match self.params.framerate {
            None => true,
            Some(0) => requested,
            Some(rate) => {
                let interval = Duration::from_secs_f64(1.0 / f64::from(rate));
                let tolerance = Duration::from_millis(50).min(interval / 2);
                if !requested
                    && self
                        .next_time
                        .is_some_and(|next| now < next.saturating_sub(tolerance))
                {
                    false
                } else {
                    let next = match self.next_time {
                        Some(next) if !requested && now.saturating_sub(next) <= interval => next
                            .max(now.saturating_sub(tolerance))
                            .saturating_add(interval),
                        _ => now.saturating_add(interval),
                    };
                    self.next_time = Some(next);
                    true
                }
            }
        };
        (due, if due { self.requests.pop_front() } else { None })
    }
}

#[derive(Debug, Clone)]
pub struct VideoInSamplers<T> {
    pub(crate) samplers: BTreeMap<VideoSourceKey, VideoInSampler<T>>,
    source_limit: usize,
    request_limit: usize,
}
impl<T> VideoInSamplers<T> {
    pub fn new(source_limit: usize, request_limit: usize) -> Result<Self, ProcessorError> {
        if source_limit == 0 || request_limit == 0 {
            return Err(ProcessorError::new("video sampler bounds must be positive"));
        }
        Ok(Self {
            samplers: BTreeMap::new(),
            source_limit,
            request_limit,
        })
    }
    pub fn capture(
        &mut self,
        key: VideoSourceKey,
        params: VideoInSourceParams,
    ) -> Result<(), ProcessorError> {
        if let Some(sampler) = self.samplers.get_mut(&key) {
            return sampler.set_params(params);
        }
        if self.samplers.len() >= self.source_limit {
            return Err(ProcessorError::new("video source limit reached"));
        }
        self.samplers
            .insert(key, VideoInSampler::new(params, self.request_limit)?);
        Ok(())
    }
    pub fn is_capturing(&self, key: &VideoSourceKey) -> bool {
        self.samplers.contains_key(key)
    }
    pub fn add_request(&mut self, key: &VideoSourceKey, request: T) -> Result<(), ProcessorError> {
        let count: usize = self.samplers.values().map(|s| s.requests.len()).sum();
        if count >= self.request_limit {
            return Err(ProcessorError::new("video request limit reached"));
        }
        self.samplers
            .get_mut(key)
            .ok_or_else(|| ProcessorError::new("video source is not captured"))?
            .add_request(request)
    }
    pub fn sample(&mut self, key: &VideoSourceKey, now: Duration) -> (bool, Option<T>) {
        self.samplers
            .get_mut(key)
            .map_or((false, None), |sampler| sampler.sample(now))
    }
    pub fn take_requests(
        &mut self,
        participant: &VideoParticipantId,
        source: Option<&VideoSourceId>,
    ) -> Vec<T> {
        self.samplers
            .iter_mut()
            .filter(|(key, _)| {
                &key.participant == participant && source.is_none_or(|s| s == &key.source)
            })
            .flat_map(|(_, sampler)| sampler.take_requests())
            .collect()
    }
    pub fn remove_participant(&mut self, participant: &VideoParticipantId) -> Vec<T> {
        let requests = self.take_requests(participant, None);
        self.samplers
            .retain(|key, _| &key.participant != participant);
        requests
    }
    pub fn clear(&mut self) -> Vec<T> {
        std::mem::take(&mut self.samplers)
            .into_values()
            .flat_map(|mut sampler| sampler.take_requests())
            .collect()
    }
}
