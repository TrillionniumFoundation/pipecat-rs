//! Frame payloads are strongly typed. JSON is used only for open metadata,
//! tool arguments/results, and transport-specific messages.
pub use crate::frame_payloads::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

mod tool_context_updated;
mod tool_progress_context_updated;

/// Semantic intent of a paired realtime trailing-history snapshot. This is
/// not authorization or provider provenance. Explicit inference requests must
/// use a fresh context frame without this marker.
pub const REALTIME_CONTEXT_UPDATE_METADATA: &str = "realtime_context_update";

fn next_name(class: &'static str) -> String {
    static COUNTS: std::sync::OnceLock<std::sync::Mutex<BTreeMap<&'static str, u64>>> =
        std::sync::OnceLock::new();
    let mut counts = COUNTS.get_or_init(Default::default).lock().unwrap();
    let count = counts.entry(class).or_default();
    let name = format!("{class}#{count}");
    *count += 1;
    name
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Downstream,
    Upstream,
}
pub type FrameDirection = Direction;

impl Direction {
    pub fn reversed(self) -> Self {
        match self {
            Self::Downstream => Self::Upstream,
            Self::Upstream => Self::Downstream,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FramePriority {
    Start,
    System,
    Normal,
}

/// Opaque process-local provenance. A tag owns no runtime, survives clone and
/// duplicate, and is deliberately absent from wire frames and frame equality.
#[derive(Debug, Clone, Default)]
pub struct LocalFrameTag(Arc<()>);
impl LocalFrameTag {
    pub const MAX_TAGS_PER_FRAME: usize = 64;
    pub fn new() -> Self {
        Self::default()
    }
    pub fn contains(&self, frame: &Frame) -> bool {
        frame
            .local_tags
            .iter()
            .any(|tag| Arc::ptr_eq(&self.0, &tag.0))
    }
    /// Idempotently mark a frame. Excessive nested provenance fails explicitly
    /// instead of evicting a tag which still protects a private frame copy.
    pub fn mark(&self, frame: &mut Frame) -> Result<(), crate::ProcessorError> {
        if self.contains(frame) {
            return Ok(());
        }
        if frame.local_tags.len() >= Self::MAX_TAGS_PER_FRAME {
            return Err(crate::ProcessorError::fatal(
                "local frame tag capacity exceeded",
            ));
        }
        frame.local_tags.push(self.clone());
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub id: u64,
    pub name: String,
    pub pts: Option<u64>,
    pub metadata: BTreeMap<String, Value>,
    pub transport_source: Option<String>,
    pub transport_destination: Option<String>,
    pub broadcast_sibling_id: Option<u64>,
    pub interruptible: bool,
    pub data: FrameData,
    local_tags: Vec<LocalFrameTag>,
    pipeline_pts: bool,
    native_llm_continuation: Option<Arc<crate::NativeLlmContinuation>>,
    native_speculation: Option<crate::NativeSpeculation>,
    native_eager_source: Option<NativeEagerSourceFrame>,
    pub(crate) native_realtime_context: Option<Arc<crate::NativeRealtimeContextCause>>,
    native_tool_outcome: Option<Arc<crate::NativeToolCallOutcome>>,
    native_tool_progress: Option<Arc<crate::NativeToolCallProgress>>,
    native_tool_context_updated: Option<tool_context_updated::ContextUpdatedBinding>,
    native_tool_progress_context_updated:
        Option<tool_progress_context_updated::ProgressContextUpdatedBinding>,
    native_tool_timeout_projected: bool,
    native_tool_batch: Option<Arc<crate::NativeToolCallBatch>>,
    pub(crate) reported_runtimes: Option<Arc<Mutex<BTreeSet<u64>>>>,
    pub(crate) observed_runtimes: crate::observer::ObservedRuntimes,
    pub(crate) interruption_epoch: Option<Arc<AtomicU64>>,
    pub(crate) error_origin: Option<Arc<crate::ProcessorErrorOrigin>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeEagerSourceFrame {
    source: crate::NativeEagerSource,
    // Start and Stop each seal their actual/proposed form at attachment.
    proposed_boundary: Option<bool>,
}

/// Opaque identity of one native logical frame. Clones preserve it; duplicate
/// and wire decoding create fresh identities, regardless of the public ID.
/// This weak token retains neither a frame nor its observer bookkeeping.
#[derive(Clone)]
pub struct FrameObservationToken(std::sync::Weak<std::sync::OnceLock<Mutex<BTreeSet<u64>>>>);
impl FrameObservationToken {
    /// False only after the last native frame carrying this identity is gone.
    /// An expired identity cannot be restored through this token.
    pub fn is_alive(&self) -> bool {
        self.0.strong_count() != 0
    }
}
impl std::fmt::Debug for FrameObservationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FrameObservationToken(<local>)")
    }
}
impl PartialEq for FrameObservationToken {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Weak::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for FrameObservationToken {}
impl std::hash::Hash for FrameObservationToken {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.0.as_ptr(), state);
    }
}
impl Frame {
    pub fn observation_token(&self) -> FrameObservationToken {
        FrameObservationToken(Arc::downgrade(&self.observed_runtimes))
    }
}

/// A local clock timestamp cannot cross a wire without an explicit rebase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameTimingError {
    #[error("frame uses a pipeline clock; rebase PTS before wire encoding")]
    PipelineClockOnWire,
    #[error("frame PTS precedes the requested destination origin")]
    BeforeOrigin,
    #[error("frame does not carry pipeline-clock PTS")]
    NotPipelineClock,
}
impl Serialize for Frame {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.require_wire_pts().map_err(serde::ser::Error::custom)?;
        if self.native_realtime_context.is_some() {
            return Err(serde::ser::Error::custom(
                "unconsumed native realtime context cause on wire",
            ));
        }
        if self.native_speculation.is_some() {
            return Err(serde::ser::Error::custom(
                "unconsumed native speculation on wire",
            ));
        }
        self.require_no_native_llm_continuation()
            .map_err(serde::ser::Error::custom)?;
        #[derive(Serialize)]
        #[serde(rename = "Frame")]
        struct WireFrame<'a> {
            id: u64,
            name: &'a str,
            pts: Option<u64>,
            metadata: &'a BTreeMap<String, Value>,
            transport_source: &'a Option<String>,
            transport_destination: &'a Option<String>,
            broadcast_sibling_id: Option<u64>,
            interruptible: bool,
            data: &'a FrameData,
        }
        WireFrame {
            id: self.id,
            name: &self.name,
            pts: self.pts,
            metadata: &self.metadata,
            transport_source: &self.transport_source,
            transport_destination: &self.transport_destination,
            broadcast_sibling_id: self.broadcast_sibling_id,
            interruptible: self.interruptible,
            data: &self.data,
        }
        .serialize(serializer)
    }
}

// Serialized frames begin a new in-memory identity. Rebuild their private
// bookkeeping before callers can clone them, without importing remote epochs or
// runtime identifiers and without consuming a new public frame ID.
impl<'de> Deserialize<'de> for Frame {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "Frame")]
        struct WireFrame {
            id: u64,
            name: String,
            pts: Option<u64>,
            metadata: BTreeMap<String, Value>,
            transport_source: Option<String>,
            transport_destination: Option<String>,
            broadcast_sibling_id: Option<u64>,
            interruptible: bool,
            data: FrameData,
        }
        let mut wire = WireFrame::deserialize(deserializer)?;
        if let FrameData::FatalError(error) = &mut wire.data {
            error.fatal = true;
        }
        let interruption_epoch =
            matches!(wire.data, FrameData::Interruption).then(|| Arc::new(AtomicU64::new(0)));
        let reported_runtimes = matches!(
            wire.data,
            FrameData::Error(_) | FrameData::FatalError(_) | FrameData::ErrorDetailed(_)
        )
        .then(Default::default);
        Ok(Self {
            id: wire.id,
            name: wire.name,
            pts: wire.pts,
            metadata: wire.metadata,
            transport_source: wire.transport_source,
            transport_destination: wire.transport_destination,
            broadcast_sibling_id: wire.broadcast_sibling_id,
            interruptible: wire.interruptible,
            data: wire.data,
            local_tags: vec![],
            pipeline_pts: false,
            native_llm_continuation: None,
            native_speculation: None,
            native_eager_source: None,
            native_realtime_context: None,
            native_tool_outcome: None,
            native_tool_progress: None,
            native_tool_context_updated: None,
            native_tool_progress_context_updated: None,
            native_tool_timeout_projected: false,
            native_tool_batch: None,
            reported_runtimes,
            observed_runtimes: Default::default(),
            interruption_epoch,
            error_origin: None,
        })
    }
}

impl PartialEq for Frame {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.name == other.name
            && self.pts == other.pts
            && self.pipeline_pts == other.pipeline_pts
            && self.native_llm_continuation == other.native_llm_continuation
            && self.native_speculation == other.native_speculation
            && self.native_eager_source == other.native_eager_source
            && self.native_realtime_context == other.native_realtime_context
            && self.native_tool_outcome == other.native_tool_outcome
            && self.native_tool_progress == other.native_tool_progress
            && self.native_tool_context_updated == other.native_tool_context_updated
            && self.native_tool_progress_context_updated
                == other.native_tool_progress_context_updated
            && self.native_tool_timeout_projected == other.native_tool_timeout_projected
            && self.native_tool_batch == other.native_tool_batch
            && self.metadata == other.metadata
            && self.transport_source == other.transport_source
            && self.transport_destination == other.transport_destination
            && self.broadcast_sibling_id == other.broadcast_sibling_id
            && self.interruptible == other.interruptible
            && self.data == other.data
    }
}

impl Frame {
    /// Attach the immutable executor ownership descriptor to a matching declaration.
    pub fn set_native_tool_batch(
        &mut self,
        batch: Arc<crate::NativeToolCallBatch>,
    ) -> Result<(), crate::ProcessorError> {
        batch.validate(&self.data)?;
        if self.native_tool_batch.is_some() {
            return Err(crate::ProcessorError::new(
                "native tool batch already attached",
            ));
        }
        self.native_tool_batch = Some(batch);
        Ok(())
    }
    pub fn native_tool_batch(
        &self,
    ) -> Result<Option<&Arc<crate::NativeToolCallBatch>>, crate::ProcessorError> {
        if let Some(batch) = &self.native_tool_batch {
            batch.validate(&self.data)?;
        }
        Ok(self.native_tool_batch.as_ref())
    }
    pub fn take_native_tool_batch(
        &mut self,
    ) -> Result<Option<Arc<crate::NativeToolCallBatch>>, crate::ProcessorError> {
        self.native_tool_batch()?;
        Ok(self.native_tool_batch.take())
    }
    pub(crate) fn attach_tool_progress(&mut self, progress: Arc<crate::NativeToolCallProgress>) {
        self.native_tool_progress = Some(progress);
    }
    /// Validated nonterminal update from an admitted asynchronous tool call.
    pub fn native_tool_progress(
        &self,
    ) -> Result<Option<&Arc<crate::NativeToolCallProgress>>, crate::ProcessorError> {
        if let Some(progress) = &self.native_tool_progress {
            if self.native_tool_outcome.is_some()
                || self.native_tool_context_updated.is_some()
                || self.native_tool_timeout_projected
                || self.native_tool_batch.is_some()
            {
                return Err(crate::ProcessorError::new(
                    "frame cannot carry terminal and nonterminal tool receipts",
                ));
            }
            progress.validate(&self.data)?;
            if let Some(binding) = &self.native_tool_progress_context_updated {
                binding.validate(progress, &self.data)?;
            }
        }
        Ok(self.native_tool_progress.as_ref())
    }
    /// Consume nonterminal provenance at an explicit native or wire boundary.
    pub fn take_native_tool_progress(
        &mut self,
    ) -> Result<Option<Arc<crate::NativeToolCallProgress>>, crate::ProcessorError> {
        self.native_tool_progress()?;
        Ok(self.native_tool_progress.take())
    }
    pub(crate) fn attach_tool_outcome(&mut self, outcome: Arc<crate::NativeToolCallOutcome>) {
        self.native_tool_timeout_projected = false;
        self.native_tool_outcome = Some(outcome);
    }
    /// Validated local terminal acknowledgment, distinct from an external cancel request.
    pub fn native_tool_outcome(
        &self,
    ) -> Result<Option<&Arc<crate::NativeToolCallOutcome>>, crate::ProcessorError> {
        if self.native_tool_context_updated.is_some()
            && self.native_tool_progress_context_updated.is_some()
        {
            return Err(crate::ProcessorError::new(
                "frame cannot carry terminal and progress context-update callbacks",
            ));
        }
        if let Some(outcome) = &self.native_tool_outcome {
            if self.native_tool_progress.is_some()
                || self.native_tool_progress_context_updated.is_some()
            {
                return Err(crate::ProcessorError::new(
                    "frame cannot carry terminal and nonterminal tool receipts",
                ));
            }
            outcome.validate(&self.data, self.native_tool_timeout_projected)?;
            if let Some(binding) = &self.native_tool_context_updated {
                binding.validate(outcome, &self.data, self.native_tool_timeout_projected)?;
            }
        }
        Ok(self.native_tool_outcome.as_ref())
    }
    /// Consume the local acknowledgment only at an explicit native/wire boundary.
    pub fn take_native_tool_outcome(
        &mut self,
    ) -> Result<Option<Arc<crate::NativeToolCallOutcome>>, crate::ProcessorError> {
        self.native_tool_outcome()?;
        self.native_tool_timeout_projected = false;
        Ok(self.native_tool_outcome.take())
    }
    /// Project a verified timeout to the provider cancellation convention while
    /// retaining its original Failed/TimedOut receipt for the native consumer.
    pub fn project_native_tool_timeout_as_cancel(&mut self) -> Result<(), crate::ProcessorError> {
        let outcome = self.native_tool_outcome()?.ok_or_else(|| {
            crate::ProcessorError::new("timeout projection requires a native receipt")
        })?;
        if outcome.state() != crate::ToolCallTerminalState::Failed
            || outcome.reason() != crate::ToolCallEndReason::TimedOut
        {
            return Err(crate::ProcessorError::new(
                "only a native failed timeout can project to cancellation",
            ));
        }
        self.data = FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: outcome.call().function_name.clone(),
            tool_call_id: outcome.call().tool_call_id.clone(),
            run_llm: true,
        });
        self.native_tool_timeout_projected = true;
        Ok(())
    }
    /// Attach a native eager-turn capability. It is never decoded from metadata.
    pub fn set_native_speculation(
        &mut self,
        capability: crate::NativeSpeculation,
    ) -> Result<(), crate::ProcessorError> {
        if self.native_speculation.is_some() {
            return Err(crate::ProcessorError::new(
                "speculation capability already attached",
            ));
        }
        self.native_speculation = Some(capability);
        Ok(())
    }
    pub fn native_speculation(&self) -> Option<&crate::NativeSpeculation> {
        self.native_speculation.as_ref()
    }
    /// Consume the effect capability only at an explicitly admitted local boundary.
    pub fn take_native_speculation(&mut self) -> Option<crate::NativeSpeculation> {
        self.native_speculation.take()
    }
    /// Attach source authority only to the downstream sibling after creating
    /// any broadcast pair. The carrier and its immutable payload stay native.
    pub fn set_native_eager_source(
        &mut self,
        source: crate::NativeEagerSource,
    ) -> Result<(), crate::ProcessorError> {
        if self.native_eager_source.is_some() {
            return Err(crate::ProcessorError::new(
                "native eager source already attached",
            ));
        }
        source.validate(&self.data, None)?;
        let proposed_boundary = matches!(
            source,
            crate::NativeEagerSource::Start(_) | crate::NativeEagerSource::Stop { .. }
        )
        .then(|| {
            matches!(
                self.data,
                FrameData::ProposedUserStartedSpeaking | FrameData::ProposedUserStoppedSpeaking
            )
        });
        self.native_eager_source = Some(NativeEagerSourceFrame {
            source,
            proposed_boundary,
        });
        Ok(())
    }
    pub fn native_eager_source(&self) -> Option<&crate::NativeEagerSource> {
        self.native_eager_source
            .as_ref()
            .map(|carrier| &carrier.source)
    }
    /// Consume only at the source-aware downstream user aggregator. Revalidate
    /// role and payload because public fields may have changed since attachment.
    pub fn take_native_eager_source(
        &mut self,
        direction: Direction,
    ) -> Result<Option<crate::NativeEagerSource>, crate::ProcessorError> {
        if let Some(carrier) = &self.native_eager_source {
            carrier.source.validate(&self.data, Some(direction))?;
            if carrier.proposed_boundary.is_some_and(|proposed| {
                proposed
                    != matches!(
                        self.data,
                        FrameData::ProposedUserStartedSpeaking
                            | FrameData::ProposedUserStoppedSpeaking
                    )
            }) {
                return Err(crate::ProcessorError::new(
                    "native eager source boundary payload was changed",
                ));
            }
        }
        Ok(self
            .native_eager_source
            .take()
            .map(|carrier| carrier.source))
    }
    /// Attach validated provider history only to its matching native boundary.
    /// Existing semantic history cannot be silently replaced.
    pub fn set_native_llm_continuation(
        &mut self,
        continuation: Arc<crate::NativeLlmContinuation>,
    ) -> Result<(), crate::NativeLlmContinuationError> {
        if self.native_llm_continuation.is_some() {
            return Err(crate::NativeLlmContinuationError::AlreadyAttached);
        }
        continuation.validate_frame(&self.data)?;
        self.native_llm_continuation = Some(continuation);
        Ok(())
    }
    /// Revalidate because the public frame payload may have been changed since
    /// attachment. Consumers must check before admitting any native effect.
    pub fn native_llm_continuation(
        &self,
    ) -> Result<Option<&Arc<crate::NativeLlmContinuation>>, crate::NativeLlmContinuationError> {
        if let Some(continuation) = &self.native_llm_continuation {
            continuation.validate_frame(&self.data)?;
        } else if matches!(self.data, FrameData::LlmContinuationCheckpoint) {
            return Err(crate::NativeLlmContinuationError::MissingCarrier);
        }
        Ok(self.native_llm_continuation.as_ref())
    }
    /// Consume local semantic history after checking its current boundary.
    pub fn take_native_llm_continuation(
        &mut self,
    ) -> Result<Option<Arc<crate::NativeLlmContinuation>>, crate::NativeLlmContinuationError> {
        self.native_llm_continuation()?;
        Ok(self.native_llm_continuation.take())
    }
    /// Retained memory must still be budgeted if a caller changed the payload.
    pub fn native_llm_continuation_retained_bytes(&self) -> usize {
        self.native_llm_continuation
            .as_ref()
            .map_or(0, |continuation| continuation.retained_bytes())
    }
    /// Every manual frame encoder must reject unconsumed native semantics,
    /// including frame kinds which that encoder otherwise ignores.
    pub fn require_no_native_llm_continuation(
        &self,
    ) -> Result<(), crate::NativeLlmContinuationError> {
        if self.native_eager_source.is_some() {
            return Err(crate::NativeLlmContinuationError::UnconsumedEagerSource);
        }
        if self.native_tool_batch.is_some() {
            return Err(crate::NativeLlmContinuationError::UnconsumedToolBatch);
        }
        if self.native_tool_outcome.is_some()
            || self.native_tool_progress.is_some()
            || self.native_tool_context_updated.is_some()
            || self.native_tool_progress_context_updated.is_some()
        {
            return Err(crate::NativeLlmContinuationError::UnconsumedToolOutcome);
        }
        if self.native_speculation.is_some() {
            return Err(crate::NativeLlmContinuationError::UnconsumedSpeculation);
        }
        if self.native_llm_continuation.is_some()
            || self.native_realtime_context.is_some()
            || matches!(self.data, FrameData::LlmContinuationCheckpoint)
        {
            Err(crate::NativeLlmContinuationError::UnconsumedOnWire)
        } else {
            Ok(())
        }
    }
    /// Set nanoseconds on the current pipeline's shared observer timeline.
    /// Clone/duplicate retain this local marker; all wire encoders must reject
    /// it until an output boundary explicitly rebases to its relative timeline.
    pub fn set_pipeline_pts(&mut self, pts: u64) {
        self.pts = Some(pts);
        self.pipeline_pts = true;
    }
    pub fn has_pipeline_pts(&self) -> bool {
        self.pipeline_pts
    }
    /// Explicitly replace the timestamp with legacy destination-relative PTS.
    pub fn set_relative_pts(&mut self, pts: Option<u64>) {
        self.pts = pts;
        self.pipeline_pts = false;
    }
    /// Checked origin subtraction; overdue pre-origin work is reported to the
    /// caller, which may explicitly choose zero at its transport boundary.
    pub fn rebase_pipeline_pts(&mut self, origin_ns: u64) -> Result<(), FrameTimingError> {
        if !self.pipeline_pts {
            return Err(FrameTimingError::NotPipelineClock);
        }
        let pts = self.pts.ok_or(FrameTimingError::NotPipelineClock)?;
        let relative = pts
            .checked_sub(origin_ns)
            .ok_or(FrameTimingError::BeforeOrigin)?;
        self.set_relative_pts(Some(relative));
        Ok(())
    }
    pub fn require_wire_pts(&self) -> Result<(), FrameTimingError> {
        if self.pipeline_pts {
            Err(FrameTimingError::PipelineClockOnWire)
        } else {
            Ok(())
        }
    }
    pub fn new(data: FrameData) -> Self {
        let mut data = data;
        if let FrameData::FatalError(error) = &mut data {
            error.fatal = true;
        }
        let interruption_epoch =
            matches!(data, FrameData::Interruption).then(|| Arc::new(AtomicU64::new(0)));
        let reported_runtimes = matches!(
            data,
            FrameData::Error(_) | FrameData::FatalError(_) | FrameData::ErrorDetailed(_)
        )
        .then(Default::default);
        let interruptible = data.default_interruptible();
        let name = next_name(data.source_class_name());
        Self {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            name,
            pts: None,
            metadata: BTreeMap::new(),
            transport_source: None,
            transport_destination: None,
            broadcast_sibling_id: None,
            interruptible,
            data,
            local_tags: vec![],
            pipeline_pts: false,
            native_llm_continuation: None,
            native_speculation: None,
            native_eager_source: None,
            native_realtime_context: None,
            native_tool_outcome: None,
            native_tool_progress: None,
            native_tool_context_updated: None,
            native_tool_progress_context_updated: None,
            native_tool_timeout_projected: false,
            native_tool_batch: None,
            reported_runtimes,
            observed_runtimes: Default::default(),
            interruption_epoch,
            error_origin: None,
        }
    }
    pub fn text(text: impl Into<String>) -> Self {
        Self::new(FrameData::Text(TextFrame::new(text)))
    }
    /// Local error attribution, preserved by cloning/duplication and absent on
    /// decoded wire frames. Remote processor names never confer local identity.
    pub fn error_origin(&self) -> Option<&crate::ProcessorErrorOrigin> {
        self.error_origin.as_deref()
    }
    pub fn start() -> Self {
        Self::new(FrameData::Start(StartFrame::default()))
    }
    pub fn end() -> Self {
        Self::new(FrameData::End(EndFrame::default()))
    }
    pub fn cancel(reason: impl Into<String>) -> Self {
        Self::new(FrameData::Cancel(CancelFrame {
            reason: Some(reason.into()),
        }))
    }
    pub fn interruption() -> Self {
        Self::new(FrameData::Interruption)
    }
    pub fn with_interruptible(mut self, interruptible: bool) -> Self {
        self.interruptible = interruptible;
        self
    }
    pub fn priority(&self) -> FramePriority {
        self.data.priority()
    }
    pub fn is_system(&self) -> bool {
        self.priority() != FramePriority::Normal
    }
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.data,
            FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Stop
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
        )
    }
    /// A fresh logical frame with the same payload and metadata. `Clone` instead
    /// preserves identity for forwarding the same frame through a pipeline.
    pub fn duplicate(&self) -> Self {
        let mut frame = self.clone();
        frame.id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        frame.name = next_name(frame.data.source_class_name());
        frame.reported_runtimes = self.reported_runtimes.as_ref().map(|_| Default::default());
        frame.observed_runtimes = Default::default();
        frame.broadcast_sibling_id = None;
        frame.interruption_epoch =
            matches!(frame.data, FrameData::Interruption).then(|| Arc::new(AtomicU64::new(0)));
        frame
    }
    /// Whether this is a non-speculative state-only realtime context update.
    /// Metadata on input text, run requests or other payloads cannot set it.
    pub fn is_realtime_context_update(&self) -> bool {
        (matches!(self.data, FrameData::LlmContext(_))
            || matches!(&self.data, FrameData::LlmContextDetailed(context) if !context.speculation))
            && self
                .metadata
                .get(REALTIME_CONTEXT_UPDATE_METADATA)
                .and_then(Value::as_bool)
                == Some(true)
    }
    pub fn broadcast_pair(&self) -> (Self, Self) {
        let mut down = self.duplicate();
        let mut up = self.duplicate();
        down.broadcast_sibling_id = Some(up.id);
        up.broadcast_sibling_id = Some(down.id);
        if matches!(self.data, FrameData::Interruption) {
            let epoch = self
                .interruption_epoch
                .clone()
                .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
            down.interruption_epoch = Some(epoch.clone());
            up.interruption_epoch = Some(epoch);
        }
        (down, up)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirectedFrame {
    pub frame: Frame,
    pub direction: Direction,
}
impl DirectedFrame {
    pub fn new(frame: Frame, direction: Direction) -> Self {
        Self { frame, direction }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
#[non_exhaustive]
pub enum FrameData {
    OpenClawSend(OpenClawSendFrame),
    OpenClawSteer(OpenClawSteerFrame),
    OpenClawAbort(OpenClawAbortFrame),
    OpenClawStarted(OpenClawStartedFrame),
    OpenClawText(OpenClawTextFrame),
    OpenClawEnd(OpenClawEndFrame),
    Start(StartFrame),
    End(EndFrame),
    Stop,
    Cancel(CancelFrame),
    Interruption,
    Error(ErrorFrame),
    Text(TextFrame),
    LlmText(TextFrame),
    LlmSearchResponse(LlmSearchResponseFrame),
    InputText(TextFrame),
    VisionText(TextFrame),
    AggregatedText(AggregatedTextFrame),
    TtsText(AggregatedTextFrame),
    /// Native extension carrying explicit synthesis/display/source provenance.
    TtsTextInput(TtsTextInputFrame),
    /// Native reconstruction diagnostics; never a speech/playback attestation.
    TtsAlignmentDiagnostic(TtsAlignmentDiagnosticFrame),
    Transcription(TranscriptionFrame),
    InterimTranscription(TranscriptionFrame),
    EagerTranscription(TranscriptionFrame),
    Translation(TranscriptionFrame),
    InputAudio(AudioRawFrame),
    OutputAudio(AudioRawFrame),
    TtsAudio(AudioRawFrame),
    InputImage(ImageRawFrame),
    OutputImage(ImageRawFrame),
    InputFile(FileRawFrame),
    UserFile(UserFileRawFrame),
    TtsSpeak(TtsSpeakFrame),
    TtsStarted(TtsStartedFrame),
    TtsStopped(TtsStoppedFrame),
    LlmResponseStart,
    LlmResponseEnd,
    /// Structurally complete native provider prefix; never admits tool calls.
    #[serde(skip)]
    LlmContinuationCheckpoint,
    LlmRun,
    LlmContext(LlmContextFrame),
    LlmMarker(LlmMarkerFrame),
    LlmMarkerResponse(LlmMarkerResponseFrame),
    LlmThoughtStart,
    LlmThoughtText(TextFrame),
    LlmThoughtEnd,
    LlmMessagesAppend(MessagesFrame),
    LlmMessagesUpdate(MessagesFrame),
    FunctionCallsStarted(Vec<FunctionCall>),
    FunctionCallInProgress(FunctionCallInProgressFrame),
    FunctionCallResult(FunctionCallResultFrame),
    FunctionCallCancel(FunctionCallCancelFrame),
    UserStartedSpeaking,
    UserStoppedSpeaking,
    UserSpeaking,
    BotStartedSpeaking,
    BotStoppedSpeaking,
    BotSpeaking,
    UserMuteStarted,
    UserMuteStopped,
    SttMute(bool),
    EagerEndOfTurnCancel,
    VadUserStartedSpeaking(VadSpeakingFrame),
    VadUserStoppedSpeaking(VadSpeakingFrame),
    ProposedUserStartedSpeaking,
    ProposedUserStoppedSpeaking,
    InputTransportMessage(Value),
    OutputTransportMessage(Value),
    OutputTransportMessageUrgent(Value),
    InputDtmf(KeypadEntry),
    OutputDtmf(KeypadEntry),
    OutputDtmfUrgent(KeypadEntry),
    BotConnected,
    ClientConnected,
    OutputTransportReady,
    InputTransportStartAudioStreaming,
    Heartbeat {
        timestamp: u64,
    },
    Flush(FlushFrame),
    FlushDetailed(PipelineFlushFrame),
    VadUserStartedSpeakingDetailed(VadUserStartedSpeakingFrame),
    VadUserStoppedSpeakingDetailed(VadUserStoppedSpeakingFrame),
    ProcessorPause {
        processor: String,
    },
    ProcessorResume {
        processor: String,
    },
    ProcessorPauseUrgent {
        processor: String,
    },
    AudioBufferStartRecording,
    AudioBufferStopRecording,
    FilterEnable(bool),
    MixerEnable(bool),
    Volume(f32),
    VolumeGain(f32),
    Metrics(Vec<Metric>),
    Base,
    System,
    Data,
    Control,
    SpeechOutputAudio(AudioRawFrame),
    OutputImageDetailed(OutputImageRawFrame),
    UrlImage(UrlImageRawFrame),
    Sprite(SpriteFrame),
    AggregatedTextProgress(AggregatedTextProgressFrame),
    TranscriptionDetailed(DetailedTranscriptionFrame),
    InterimTranscriptionDetailed(DetailedTranscriptionFrame),
    EagerTranscriptionDetailed(DetailedTranscriptionFrame),
    TranslationDetailed(DetailedTranscriptionFrame),
    LlmContextAssistantTimestamp {
        timestamp: String,
    },
    LlmContextAssistantTurn(LlmContextAssistantTurnFrame),
    LlmContextDetailed(DetailedLlmContextFrame),
    LlmThoughtStartDetailed(LlmThoughtStartFrame),
    LlmThoughtEndDetailed(LlmThoughtEndFrame),
    LlmMessagesAppendDetailed(DetailedMessagesFrame),
    LlmMessagesUpdateDetailed(DetailedMessagesFrame),
    LlmMessagesTransform(LlmMessagesTransformFrame),
    LlmSetTools(ToolSet),
    LlmSetToolChoice(ToolChoice),
    LlmEnablePromptCaching {
        enable: bool,
    },
    LlmConfigureOutput {
        skip_tts: bool,
    },
    FunctionCallResultDetailed(DetailedFunctionCallResultFrame),
    FunctionCallsStartedDetailed(Vec<FunctionCallFromLlm>),
    OutputDtmfSequence(DtmfSequence),
    OutputDtmfUrgentSequence(DtmfSequence),
    StartDetailed(DetailedStartFrame),
    EndDetailed(ReasonFrame),
    CancelDetailed(ReasonFrame),
    ErrorDetailed(DetailedErrorFrame),
    FatalError(ErrorFrame),
    ProcessorPauseTarget(ProcessorReference),
    ProcessorPauseUrgentTarget(ProcessorReference),
    ProcessorResumeTarget(ProcessorReference),
    ProcessorResumeUrgent {
        processor: String,
    },
    UserTurnInferenceCompleted,
    ExternalFunctionCallsStarted(Vec<ExternalFunctionCall>),
    ExternalFunctionCallInProgress(ExternalFunctionCallInProgressFrame),
    ExternalFunctionCallResult(ExternalFunctionCallResultFrame),
    ExternalFunctionCallCancel(ExternalFunctionCall),
    UserImageRequest(UserImageRequestFrame),
    UserAudio(UserAudioRawFrame),
    UserImage(UserImageRawFrame),
    UserFileDetailed(DetailedUserFileRawFrame),
    AssistantImage(AssistantImageRawFrame),
    SpeechControlParams(SpeechControlParamsFrame),
    ServiceMetadata(ServiceMetadataFrame),
    SttMetadata(SttMetadataFrame),
    LlmServiceMetadata(LlmServiceMetadataFrame),
    ServiceSwitcherRequestMetadata(ProcessorReference),
    Worker,
    WorkerSystem,
    EndWorker(ReasonFrame),
    StopWorker,
    CancelWorker(ReasonFrame),
    InterruptionWorker,
    Task,
    TaskSystem,
    LlmFullResponseStart(LlmFullResponseFrame),
    LlmFullResponseEnd(LlmFullResponseFrame),
    LlmAssistantPushAggregation,
    LlmSummarizeContext(Option<LlmContextSummaryConfig>),
    LlmContextSummaryRequest(LlmContextSummaryRequestFrame),
    LlmContextSummaryResult(LlmContextSummaryResultFrame),
    VisionFullResponseStart(LlmFullResponseFrame),
    VisionFullResponseEnd(LlmFullResponseFrame),
    ServiceUpdateSettings(Box<ServiceUpdateSettingsFrame<ServiceSettings>>),
    LlmUpdateSettings(Box<ServiceUpdateSettingsFrame<LlmSettings>>),
    TtsUpdateSettings(Box<ServiceUpdateSettingsFrame<TtsSettings>>),
    SttUpdateSettings(Box<ServiceUpdateSettingsFrame<SttSettings>>),
    UserIdleTimeoutUpdate {
        timeout: f64,
    },
    VadParamsUpdate(VadParams),
    FilterControl,
    FilterUpdateSettings(BTreeMap<String, Value>),
    MixerControl,
    MixerUpdateSettings(BTreeMap<String, Value>),
    ServiceSwitcher,
    ManuallySwitchService(ProcessorReference),
    RtviClientMessage(RtviClientMessageFrame),
    RtviServerMessage(serde_json::Value),
    RtviServerResponse(RtviServerResponseFrame),
    RtviConfigureObserver(RtviConfigureObserverFrame),
    RtviUiCommand(RtviUiCommandFrame),
    RtviUiJobGroup(Option<serde_json::Value>),
    RtviUiEvent(RtviUiEventFrame),
    RtviUiSnapshot(RtviUiSnapshotFrame),
    RtviUiCancelJobGroup(RtviUiCancelJobGroupFrame),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartFrame {
    pub audio_in_sample_rate: u32,
    pub audio_out_sample_rate: u32,
    pub enable_metrics: bool,
    pub enable_usage_metrics: bool,
    pub report_only_initial_ttfb: bool,
}
impl Default for StartFrame {
    fn default() -> Self {
        Self {
            audio_in_sample_rate: 16000,
            audio_out_sample_rate: 24000,
            enable_metrics: false,
            enable_usage_metrics: false,
            report_only_initial_ttfb: false,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndFrame {
    pub reason: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelFrame {
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    #[default]
    Unknown,
    Authentication,
    Authorization,
    InvalidRequest,
    RateLimit,
    Quota,
    Connectivity,
    Server,
    Application,
}
impl ErrorCategory {
    pub fn is_permanent(self) -> bool {
        matches!(
            self,
            Self::Authentication | Self::Authorization | Self::InvalidRequest
        )
    }
    pub fn from_http_status(status: u16) -> Self {
        match status {
            400 | 404 | 422 => Self::InvalidRequest,
            401 => Self::Authentication,
            402 => Self::Quota,
            403 => Self::Authorization,
            429 => Self::RateLimit,
            500..=599 => Self::Server,
            _ => Self::Unknown,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorFrame {
    pub error: String,
    pub fatal: bool,
    pub processor: Option<String>,
    pub category: ErrorCategory,
}
impl ErrorFrame {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            fatal: false,
            processor: None,
            category: ErrorCategory::Unknown,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextFrame {
    pub text: String,
    pub skip_tts: Option<bool>,
    pub includes_inter_frame_spaces: bool,
    pub append_to_context: bool,
}
impl TextFrame {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            skip_tts: None,
            includes_inter_frame_spaces: false,
            append_to_context: true,
        }
    }
    pub fn llm(text: impl Into<String>) -> Self {
        Self {
            includes_inter_frame_spaces: true,
            ..Self::new(text)
        }
    }
}
impl Default for TextFrame {
    fn default() -> Self {
        Self::new("")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregatedTextFrame {
    pub text: TextFrame,
    pub aggregated_by: String,
    pub context_id: Option<String>,
    pub raw_text: Option<String>,
    pub will_be_spoken: bool,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TranscriptionFrame {
    pub text: String,
    pub user_id: String,
    pub timestamp: String,
    pub language: Option<String>,
    pub result: Option<Value>,
    pub finalized: bool,
}

/// Signed 16-bit little-endian interleaved PCM. Sample count is calculated,
/// never stored separately from its source bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioRawFrame {
    pub audio: Vec<u8>,
    pub sample_rate: u32,
    pub num_channels: u16,
    pub context_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AudioFrameError {
    #[error("sample rate and channel count must be nonzero")]
    InvalidFormat,
    #[error("PCM data must contain whole signed 16-bit interleaved samples")]
    MisalignedSamples,
}
impl AudioRawFrame {
    pub fn new(
        audio: Vec<u8>,
        sample_rate: u32,
        num_channels: u16,
    ) -> Result<Self, AudioFrameError> {
        let frame = Self {
            audio,
            sample_rate,
            num_channels,
            context_id: None,
        };
        frame.validate()?;
        Ok(frame)
    }
    pub fn validate(&self) -> Result<(), AudioFrameError> {
        if self.sample_rate == 0 || self.num_channels == 0 {
            return Err(AudioFrameError::InvalidFormat);
        }
        if !self
            .audio
            .chunks_exact(usize::from(self.num_channels) * 2)
            .remainder()
            .is_empty()
        {
            return Err(AudioFrameError::MisalignedSamples);
        }
        Ok(())
    }
    pub fn num_frames(&self) -> usize {
        if self.num_channels == 0 {
            0
        } else {
            self.audio.len() / (usize::from(self.num_channels) * 2)
        }
    }
    pub fn duration_seconds(&self) -> f64 {
        if self.sample_rate == 0 {
            0.0
        } else {
            self.num_frames() as f64 / f64::from(self.sample_rate)
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageRawFrame {
    pub image: Vec<u8>,
    pub size: (u32, u32),
    pub format: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileSourceType {
    Bytes,
    Url,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileRawFrame {
    pub file: String,
    pub source_type: FileSourceType,
    pub filename: Option<String>,
    pub format: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserFileRawFrame {
    pub file: FileRawFrame,
    pub user_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtsSpeakFrame {
    pub text: String,
    pub append_to_context: bool,
}
impl TtsSpeakFrame {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            append_to_context: true,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtsStartedFrame {
    pub context_id: Option<String>,
    pub append_to_context: bool,
}
impl Default for TtsStartedFrame {
    fn default() -> Self {
        Self {
            context_id: None,
            append_to_context: true,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtsStoppedFrame {
    pub context_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmMarkerFrame {
    pub marker: String,
    pub append_to_context_immediately: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmMarkerResponseFrame {
    pub raw: String,
    pub marker: Option<String>,
    pub kind: Option<String>,
    pub markers: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: String,
    pub name: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_calls: Vec<FunctionCall>,
}
impl ChatMessage {
    pub fn new(role: ChatRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChatRole {
    System,
    Developer,
    User,
    Assistant,
    Tool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessagesFrame {
    pub messages: Vec<ChatMessage>,
    pub run_llm: bool,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LlmContextFrame {
    pub messages: Vec<ChatMessage>,
    pub tools: Vec<ToolDefinition>,
    pub tool_choice: Option<ToolChoice>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    Auto,
    None,
    Required,
    Function(String),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    pub function_name: String,
    pub tool_call_id: String,
    pub arguments: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallInProgressFrame {
    pub function_name: String,
    pub tool_call_id: String,
    pub arguments: Value,
    pub cancel_on_interruption: bool,
    pub group_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallResultFrame {
    pub function_name: String,
    pub tool_call_id: String,
    pub arguments: Value,
    pub result: Value,
    pub run_llm: Option<bool>,
    pub is_final: bool,
    pub error: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionCallCancelFrame {
    pub function_name: String,
    pub tool_call_id: String,
    pub run_llm: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct VadSpeakingFrame {
    pub duration_secs: f64,
    pub timestamp: f64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeypadEntry {
    #[serde(rename = "0")]
    Zero,
    #[serde(rename = "1")]
    One,
    #[serde(rename = "2")]
    Two,
    #[serde(rename = "3")]
    Three,
    #[serde(rename = "4")]
    Four,
    #[serde(rename = "5")]
    Five,
    #[serde(rename = "6")]
    Six,
    #[serde(rename = "7")]
    Seven,
    #[serde(rename = "8")]
    Eight,
    #[serde(rename = "9")]
    Nine,
    #[serde(rename = "*")]
    Star,
    #[serde(rename = "#")]
    Pound,
    A,
    B,
    C,
    D,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushFrame {
    pub returning: bool,
    pub origin: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Metric {
    Ttfb {
        processor: String,
        seconds: f64,
    },
    Processing {
        processor: String,
        seconds: f64,
    },
    LlmUsage {
        processor: String,
        prompt_tokens: u64,
        completion_tokens: u64,
        total_tokens: u64,
    },
    TtsUsage {
        processor: String,
        characters: u64,
    },
    SttUsage {
        processor: String,
        audio_seconds: f64,
    },
}

impl FrameData {
    pub fn is_start(&self) -> bool {
        matches!(self, Self::Start(_) | Self::StartDetailed(_))
    }
    pub fn is_cancel(&self) -> bool {
        matches!(self, Self::Cancel(_) | Self::CancelDetailed(_))
    }
    pub fn as_error_frame(&self) -> Option<crate::ErrorFrame> {
        match self {
            Self::Error(error) | Self::FatalError(error) => Some(error.clone()),
            Self::ErrorDetailed(error) => Some(crate::ErrorFrame {
                error: error.error.clone(),
                fatal: error.fatal,
                processor: error
                    .processor
                    .as_ref()
                    .map(|processor| processor.name().to_owned()),
                category: error.category.unwrap_or_default(),
            }),
            _ => None,
        }
    }
    pub fn is_dtmf(&self) -> bool {
        matches!(
            self,
            Self::InputDtmf(_)
                | Self::OutputDtmf(_)
                | Self::OutputDtmfUrgent(_)
                | Self::OutputDtmfSequence(_)
                | Self::OutputDtmfUrgentSequence(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn flags_and_identity() {
        assert!(Frame::text("hi").interruptible);
        assert!(!Frame::end().interruptible);
        assert!(Frame::end().with_interruptible(true).interruptible);
        let (down, up) = Frame::text("hello").broadcast_pair();
        assert_ne!(down.id, up.id);
        assert_eq!(down.broadcast_sibling_id, Some(up.id));
        assert_eq!(up.broadcast_sibling_id, Some(down.id));
    }
    #[test]
    fn audio_requires_complete_pcm_samples() {
        let audio = AudioRawFrame::new(vec![0; 640], 16000, 1).unwrap();
        assert_eq!(audio.num_frames(), 320);
        assert_eq!(audio.duration_seconds(), 0.02);
        assert!(AudioRawFrame::new(vec![0; 3], 16000, 1).is_err());
        assert!(AudioRawFrame::new(vec![], 0, 0).is_err());
    }
    #[test]
    fn frame_json_roundtrip() {
        let frame = Frame::text("unicode 文本");
        let encoded = serde_json::to_string(&frame).unwrap();
        assert_eq!(serde_json::from_str::<Frame>(&encoded).unwrap(), frame);
    }

    #[test]
    fn deserialized_interruption_clones_share_a_fresh_local_token() {
        let original = Frame::interruption();
        original
            .interruption_epoch
            .as_ref()
            .unwrap()
            .store(99, Ordering::SeqCst);
        let encoded = serde_json::to_value(&original).unwrap();
        let decoded: Frame = serde_json::from_value(encoded.clone()).unwrap();
        let cloned = decoded.clone();
        let token = decoded.interruption_epoch.as_ref().unwrap();
        assert_eq!(token.load(Ordering::SeqCst), 0);
        assert!(Arc::ptr_eq(
            token,
            cloned.interruption_epoch.as_ref().unwrap()
        ));
        assert!(!Arc::ptr_eq(
            token,
            original.interruption_epoch.as_ref().unwrap()
        ));
        assert!(!Arc::ptr_eq(
            token,
            decoded.duplicate().interruption_epoch.as_ref().unwrap()
        ));
        assert_eq!(serde_json::to_value(decoded).unwrap(), encoded);
        let text: Frame =
            serde_json::from_value(serde_json::to_value(Frame::text("plain")).unwrap()).unwrap();
        assert!(text.interruption_epoch.is_none());
        assert!(text.reported_runtimes.is_none());
    }

    #[test]
    fn deserialized_fatal_error_preserves_the_constructor_invariant() {
        let mut wire =
            serde_json::to_value(Frame::new(FrameData::FatalError(ErrorFrame::new("fatal"))))
                .unwrap();
        assert_eq!(wire["data"]["payload"]["fatal"], true);
        wire["data"]["payload"]["fatal"] = false.into();
        let frame: Frame = serde_json::from_value(wire).unwrap();
        assert!(frame.data.as_error_frame().unwrap().fatal);
    }
}
