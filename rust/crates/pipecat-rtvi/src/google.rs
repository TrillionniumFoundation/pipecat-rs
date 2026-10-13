//! Google search projection alongside the ordinary RTVI observer.
use crate::{
    bounded_size, error, transport_frame, wire::Envelope, Observation, RtviHandle, RtviObserver,
    RtviObserverParams,
};
use async_trait::async_trait;
use pipecat_core::{Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError};
use pipecat_protocol::rtvi_google::GoogleSearchResponseData;
use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct SearchState {
    seen: HashSet<u64>,
    order: VecDeque<u64>,
}

/// Project Google search metadata and delegate ordinary events to `RtviObserver`.
/// Search responses are emitted independently of `bot_llm_enabled`, matching the
/// source Google observer. Native search projection applies first-push/source
/// filters and bounded FIFO deduplication; the Python Google override can emit
/// repeated projections after its base observer has filtered a frame.
pub struct GoogleRtviObserver {
    base: RtviObserver,
    params: RtviObserverParams,
    search: Mutex<SearchState>,
}
impl GoogleRtviObserver {
    pub fn new(
        params: RtviObserverParams,
        session: Option<RtviHandle>,
    ) -> Result<Self, ProcessorError> {
        let base = RtviObserver::new(params.clone(), session)?;
        Ok(Self {
            base,
            params,
            search: Mutex::new(SearchState::default()),
        })
    }

    /// Clear both ordinary observer state and the independent search seen cache.
    pub fn reset(&self) {
        self.base.reset();
        let mut state = self.search.lock().unwrap();
        state.seen.clear();
        state.order.clear();
    }

    pub fn observe(
        &self,
        frame: &Frame,
        observation: Observation<'_>,
    ) -> Result<Vec<Envelope>, ProcessorError> {
        let FrameData::LlmSearchResponse(response) = &frame.data else {
            return self.base.observe(frame, observation);
        };
        // Search IDs never enter the base observer's seen cache before projection.
        if frame.native_eager_source().is_some()
            || self.params.ignored_sources.contains(observation.source)
            || (frame.broadcast_sibling_id.is_some()
                && observation.direction != Direction::Downstream)
            || !observation.first_push
        {
            return Ok(vec![]);
        }
        let mut state = self.search.lock().unwrap();
        if state.seen.contains(&frame.id) {
            return Ok(vec![]);
        }
        bounded_size(&frame.data, self.params.max_message_bytes)?;
        let data = GoogleSearchResponseData::try_from(response).map_err(error)?;
        let envelope = data.envelope().map_err(error)?;
        bounded_size(&envelope, self.params.max_message_bytes)?;
        // Invalid or oversized projections do not consume the frame's identity.
        state.seen.insert(frame.id);
        state.order.push_back(frame.id);
        if state.order.len() > self.params.dedup_capacity {
            if let Some(id) = state.order.pop_front() {
                state.seen.remove(&id);
            }
        }
        Ok(vec![envelope])
    }

    /// Create a frame-preserving tap without a background sender or session queue.
    pub fn tap(self: &Arc<Self>, source: impl Into<String>) -> GoogleRtviObserverTap {
        GoogleRtviObserverTap {
            observer: self.clone(),
            source: source.into(),
        }
    }
}

pub struct GoogleRtviObserverTap {
    observer: Arc<GoogleRtviObserver>,
    source: String,
}
#[async_trait]
impl FrameProcessor for GoogleRtviObserverTap {
    fn name(&self) -> &str {
        &self.source
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let events = self
            .observer
            .observe(&frame, Observation::new(&self.source, direction));
        ctx.push(frame, direction).await?;
        for envelope in events? {
            ctx.push(
                transport_frame(envelope, self.observer.params.max_message_bytes)?,
                Direction::Downstream,
            )
            .await?;
        }
        Ok(())
    }
}
