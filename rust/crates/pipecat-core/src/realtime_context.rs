//! Explicit local edits carried with their exact realtime context snapshot.
use crate::frame_payloads::LocalToolFunctionIdentity;
use crate::{
    Direction, Frame, FrameData, FrameObservationToken, InvocationFence, ProcessorContext,
    ProcessorError,
};
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, OnceLock,
};

/// Maximum direct-tool entries retained by one sealed context, including clones.
pub const MAX_REALTIME_CONTEXT_TOOL_IDENTITIES: usize = 4096;

/// The operation producing a context, independent of equal message text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RealtimeContextIntent {
    Run,
    /// Assistant scheduling request, never an explicit user run or history replay.
    AssistantContinuation,
    /// Half-open range in this exact snapshot, containing only explicitly added messages.
    Append {
        start: usize,
        end: usize,
    },
    /// Replacement, deletion, or arbitrary transform; provider support is explicit.
    Replace,
}

/// Tiny immutable identity/policy copied only from a native terminal receipt.
/// The assistant selects this reason only after its existing eligibility gates
/// recognize newly owed inference; it retains no receipt, payload, or runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RealtimeNativeToolSchedulingReason {
    outcome_id: u64,
    cancel_on_interruption: bool,
}
impl RealtimeNativeToolSchedulingReason {
    pub fn from_outcome(outcome: &crate::NativeToolCallOutcome) -> Self {
        Self {
            outcome_id: outcome.id(),
            cancel_on_interruption: outcome.cancel_on_interruption(),
        }
    }
}

/// One ordered producer for one side of a realtime aggregator pair.
#[derive(Debug, Clone, Default)]
pub struct NativeRealtimeContextProducer(Arc<AtomicU64>);
impl NativeRealtimeContextProducer {
    /// Seals one bounded context payload without retaining another history copy.
    pub fn attach(
        &self,
        frame: &mut Frame,
        intent: RealtimeContextIntent,
        run_llm: bool,
    ) -> Result<(), ProcessorError> {
        self.attach_inner(frame, intent, run_llm, None)
    }
    /// The shared assistant remains the authority for deciding whether a newly
    /// validated native terminal actually creates inference intent.
    pub fn attach_assistant_continuation(
        &self,
        frame: &mut Frame,
        native_reason: Option<RealtimeNativeToolSchedulingReason>,
    ) -> Result<(), ProcessorError> {
        self.attach_inner(
            frame,
            RealtimeContextIntent::AssistantContinuation,
            true,
            native_reason,
        )
    }
    fn attach_inner(
        &self,
        frame: &mut Frame,
        intent: RealtimeContextIntent,
        run_llm: bool,
        native_reason: Option<RealtimeNativeToolSchedulingReason>,
    ) -> Result<(), ProcessorError> {
        if frame.native_realtime_context.is_some() || frame.is_realtime_context_update() {
            return Err(ProcessorError::new(
                "realtime context cause conflicts with existing intent",
            ));
        }
        if intent == RealtimeContextIntent::AssistantContinuation && !run_llm {
            return Err(ProcessorError::new(
                "assistant continuation requires inference intent",
            ));
        }
        let count = match &frame.data {
            FrameData::LlmContext(context) => context.messages.len(),
            FrameData::LlmContextDetailed(context) if !context.speculation => {
                context.context.messages.len()
            }
            _ => {
                return Err(ProcessorError::new(
                    "realtime cause requires non-speculative context",
                ))
            }
        };
        if let RealtimeContextIntent::Append { start, end } = intent {
            if start > end || end > count {
                return Err(ProcessorError::new("invalid realtime append range"));
            }
        }
        let direct_tool_identities = retain_direct_tool_identities(&frame.data)?;
        let digest = fingerprint(&frame.data)?;
        let previous = self
            .0
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ProcessorError::new("realtime context sequence exhausted"))?;
        frame.interruptible = false;
        frame.native_realtime_context = Some(Arc::new(NativeRealtimeContextCause {
            producer: self.0.clone(),
            sequence: previous + 1,
            intent,
            run_llm,
            native_reason,
            frame_id: frame.id,
            identity: frame.observation_token(),
            digest,
            direct_tool_identities,
            fence: OnceLock::new(),
        }));
        Ok(())
    }
}

/// Process-local intent. Neither metadata nor wire decoding can create one.
pub struct NativeRealtimeContextCause {
    producer: Arc<AtomicU64>,
    sequence: u64,
    intent: RealtimeContextIntent,
    run_llm: bool,
    native_reason: Option<RealtimeNativeToolSchedulingReason>,
    frame_id: u64,
    identity: FrameObservationToken,
    digest: [u8; 32],
    direct_tool_identities: Box<[LocalToolFunctionIdentity]>,
    fence: OnceLock<InvocationFence>,
}
impl std::fmt::Debug for NativeRealtimeContextCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeRealtimeContextCause")
            .field("sequence", &self.sequence)
            .field("intent", &self.intent)
            .field("run_llm", &self.run_llm)
            .field(
                "retained_tool_identities",
                &self.retained_tool_identity_count(),
            )
            .finish()
    }
}
impl PartialEq for NativeRealtimeContextCause {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl NativeRealtimeContextCause {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
    pub fn intent(&self) -> RealtimeContextIntent {
        self.intent
    }
    pub fn run_llm(&self) -> bool {
        self.run_llm
    }
    /// Newly eligible terminal selected by the shared assistant, not a provider
    /// reevaluation request. The corresponding receipt still needs admission.
    pub fn assistant_native_outcome_id(&self) -> Option<u64> {
        self.native_reason.map(|reason| reason.outcome_id)
    }
    pub(crate) fn schedules_surviving_native_terminal(&self) -> bool {
        self.native_reason
            .is_some_and(|reason| !reason.cancel_on_interruption)
    }
    /// Validate the side that produced this request before any provider effect.
    pub fn validate_direction(&self, direction: Direction) -> Result<(), ProcessorError> {
        let expected = if self.intent == RealtimeContextIntent::AssistantContinuation {
            Direction::Upstream
        } else {
            Direction::Downstream
        };
        if direction != expected {
            return Err(ProcessorError::new(
                "realtime context intent has the wrong direction",
            ));
        }
        Ok(())
    }
    /// Tiny identity tokens only; the sealed cause retains no executable callbacks.
    pub fn retained_tool_identity_count(&self) -> usize {
        self.direct_tool_identities.len()
    }
    pub fn inference_fence(&self) -> Option<&InvocationFence> {
        self.fence.get()
    }
    fn validate(&self, frame: &Frame) -> Result<(), ProcessorError> {
        if self.frame_id != frame.id
            || self.identity != frame.observation_token()
            || frame.interruptible
            || frame.is_realtime_context_update()
            || self.digest != fingerprint(&frame.data)?
        {
            return Err(ProcessorError::new(
                "realtime context cause does not match its sealed frame",
            ));
        }
        Ok(())
    }
}
impl Frame {
    pub fn native_realtime_context_cause(
        &self,
    ) -> Result<Option<&Arc<NativeRealtimeContextCause>>, ProcessorError> {
        if let Some(cause) = &self.native_realtime_context {
            cause.validate(self)?;
        }
        Ok(self.native_realtime_context.as_ref())
    }
    /// Bind once at original emission, after any explicit current-turn interruption.
    pub fn bind_realtime_context_invocation(
        &self,
        context: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.bind_realtime_context_fence(|| context.invocation_fence())
    }
    pub(crate) fn bind_realtime_context_fence(
        &self,
        capture: impl FnOnce() -> Result<InvocationFence, ProcessorError>,
    ) -> Result<(), ProcessorError> {
        if let Some(cause) = self.native_realtime_context_cause()? {
            if cause.fence.get().is_none() {
                if let Err(attempted) = cause.fence.set(capture()?) {
                    if !cause
                        .fence
                        .get()
                        .expect("concurrent fence binding")
                        .same_scope(&attempted)
                    {
                        return Err(ProcessorError::new(
                            "realtime context cause was concurrently bound to another invocation",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
    pub fn take_native_realtime_context_cause(
        &mut self,
    ) -> Result<Option<Arc<NativeRealtimeContextCause>>, ProcessorError> {
        self.native_realtime_context_cause()?;
        Ok(self.native_realtime_context.take())
    }
}

/// Provider admission cursor retaining one bounded cause, without its context history.
/// Commit only after successful local enqueue.
#[derive(Debug, Default)]
pub struct RealtimeContextAdmission {
    last: Option<Arc<NativeRealtimeContextCause>>,
}
impl RealtimeContextAdmission {
    /// True is a new ordered operation; false is the exact admitted clone.
    pub fn check(&self, cause: &Arc<NativeRealtimeContextCause>) -> Result<bool, ProcessorError> {
        match &self.last {
            None if cause.sequence == 1 => Ok(true),
            Some(last) if Arc::ptr_eq(last, cause) => Ok(false),
            Some(last)
                if Arc::ptr_eq(&last.producer, &cause.producer)
                    && last.sequence.checked_add(1) == Some(cause.sequence) =>
            {
                Ok(true)
            }
            _ => Err(ProcessorError::new(
                "realtime context cause is stale, out of order, or from another producer",
            )),
        }
    }
    pub fn commit(&mut self, cause: Arc<NativeRealtimeContextCause>) {
        self.last = Some(cause);
    }
}
fn retain_direct_tool_identities(
    data: &FrameData,
) -> Result<Box<[LocalToolFunctionIdentity]>, ProcessorError> {
    if let FrameData::LlmContextDetailed(context) = data {
        if let crate::ToolSet::Direct(tools) = &context.context.tools {
            if tools.len() > MAX_REALTIME_CONTEXT_TOOL_IDENTITIES {
                return Err(ProcessorError::new(
                    "realtime context exceeds 4096 direct-tool identities",
                ));
            }
            return Ok(tools
                .iter()
                .map(|function| function.handler.local_identity().clone())
                .collect());
        }
    }
    Ok(Box::default())
}
fn fingerprint(data: &FrameData) -> Result<[u8; 32], ProcessorError> {
    struct BoundedDigest {
        digest: Sha256,
        remaining: usize,
    }
    impl std::io::Write for BoundedDigest {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.remaining = self
                .remaining
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("context bound"))?;
            self.digest.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BoundedDigest {
        digest: Sha256::new(),
        remaining: 1024 * 1024,
    };
    let result = match data {
        FrameData::LlmContextDetailed(context) => {
            struct DirectTools<'a>(&'a [crate::DirectFunction]);
            impl serde::Serialize for DirectTools<'_> {
                fn serialize<S: serde::Serializer>(
                    &self,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    use serde::ser::SerializeSeq;
                    let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
                    for function in self.0 {
                        sequence.serialize_element(&(
                            &function.schema,
                            function.handler.local_identity().address(),
                        ))?;
                    }
                    sequence.end()
                }
            }
            match &context.context.tools {
                crate::ToolSet::Direct(tools) => serde_json::to_writer(
                    &mut writer,
                    &(
                        "LlmContextDetailedDirect",
                        &context.context.messages,
                        DirectTools(tools),
                        &context.context.tool_choice,
                        context.speculation,
                    ),
                ),
                _ => serde_json::to_writer(&mut writer, data),
            }
        }
        _ => serde_json::to_writer(&mut writer, data),
    };
    result.map_err(|_| ProcessorError::new("realtime context exceeds 1048576 serialized bytes"))?;
    Ok(writer.digest.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot(text: &str) -> Frame {
        Frame::new(FrameData::LlmContext(crate::LlmContextFrame {
            messages: vec![crate::ChatMessage::new(crate::ChatRole::User, text)],
            ..Default::default()
        }))
    }
    #[test]
    fn assistant_intent_requires_run_and_exact_direction_without_spending_sequence() {
        let producer = NativeRealtimeContextProducer::default();
        let mut frame = snapshot("same");
        assert!(producer
            .attach(
                &mut frame,
                RealtimeContextIntent::AssistantContinuation,
                false
            )
            .is_err());
        producer
            .attach(
                &mut frame,
                RealtimeContextIntent::AssistantContinuation,
                true,
            )
            .unwrap();
        let cause = frame.native_realtime_context_cause().unwrap().unwrap();
        assert_eq!(cause.sequence(), 1);
        cause.validate_direction(Direction::Upstream).unwrap();
        assert!(cause.validate_direction(Direction::Downstream).is_err());
        let mut user = snapshot("same");
        producer
            .attach(&mut user, RealtimeContextIntent::Run, true)
            .unwrap();
        let cause = user.native_realtime_context_cause().unwrap().unwrap();
        cause.validate_direction(Direction::Downstream).unwrap();
        assert!(cause.validate_direction(Direction::Upstream).is_err());
    }
    #[test]
    fn cause_seals_payload_identity_marker_and_wire() {
        let producer = NativeRealtimeContextProducer::default();
        let mut frame = snapshot("same");
        producer
            .attach(
                &mut frame,
                RealtimeContextIntent::Append { start: 0, end: 1 },
                false,
            )
            .unwrap();
        assert!(!frame.interruptible);
        assert!(frame.native_realtime_context_cause().unwrap().is_some());
        assert!(serde_json::to_vec(&frame).is_err());
        assert!(frame.require_no_native_llm_continuation().is_err());
        for mutation in 0..5 {
            let mut changed = frame.clone();
            match mutation {
                0 => {
                    if let FrameData::LlmContext(context) = &mut changed.data {
                        context.messages[0].content.push('!');
                    }
                }
                1 => changed.id += 1,
                2 => changed.interruptible = true,
                3 => {
                    changed
                        .metadata
                        .insert(crate::REALTIME_CONTEXT_UPDATE_METADATA.into(), true.into());
                }
                _ => changed = changed.duplicate(),
            }
            assert!(
                changed.native_realtime_context_cause().is_err(),
                "mutation {mutation}"
            );
        }
        frame.take_native_realtime_context_cause().unwrap();
        assert!(serde_json::to_vec(&frame).is_ok());
    }
    #[test]
    fn cause_orders_identical_text_and_rejects_gap_stale_other_producer() {
        let producer = NativeRealtimeContextProducer::default();
        let mut cursor = RealtimeContextAdmission::default();
        let mut frames = vec![];
        for _ in 0..3 {
            let mut frame = snapshot("same");
            producer
                .attach(
                    &mut frame,
                    RealtimeContextIntent::Append { start: 0, end: 1 },
                    true,
                )
                .unwrap();
            frames.push(frame);
        }
        let causes: Vec<_> = frames
            .iter()
            .map(|frame| {
                frame
                    .native_realtime_context_cause()
                    .unwrap()
                    .unwrap()
                    .clone()
            })
            .collect();
        assert!(cursor.check(&causes[1]).is_err());
        assert!(cursor.check(&causes[0]).unwrap());
        cursor.commit(causes[0].clone());
        assert!(!cursor.check(&causes[0]).unwrap());
        assert!(cursor.check(&causes[2]).is_err());
        assert!(cursor.check(&causes[1]).unwrap());
        cursor.commit(causes[1].clone());
        assert!(cursor.check(&causes[0]).is_err());
        let mut other = snapshot("same");
        NativeRealtimeContextProducer::default()
            .attach(&mut other, RealtimeContextIntent::Run, true)
            .unwrap();
        assert!(cursor
            .check(other.native_realtime_context_cause().unwrap().unwrap())
            .is_err());
    }
    #[test]
    fn cause_bound_and_checked_sequence_overflow_leave_frame_unmodified() {
        let producer = NativeRealtimeContextProducer::default();
        let mut oversized = snapshot(&"x".repeat(1024 * 1024));
        assert!(producer
            .attach(&mut oversized, RealtimeContextIntent::Run, true)
            .is_err());
        assert!(oversized.native_realtime_context_cause().unwrap().is_none());
        assert_eq!(producer.0.load(Ordering::SeqCst), 0);
        producer.0.store(u64::MAX, Ordering::SeqCst);
        let mut frame = snapshot("small");
        assert!(producer
            .attach(&mut frame, RealtimeContextIntent::Run, true)
            .is_err());
        assert!(frame.native_realtime_context_cause().unwrap().is_none());
    }
    #[test]
    fn direct_function_schema_and_callback_identity_are_sealed_without_serializing_closures() {
        let function = || crate::DirectFunction {
            schema: crate::ToolDefinition {
                name: "echo".into(),
                description: String::new(),
                parameters: serde_json::json!({"type":"object"}),
            },
            handler: crate::LocalToolFunction::new(|value| async { Ok(value) }),
        };
        let mut frame = Frame::new(FrameData::LlmContextDetailed(
            crate::DetailedLlmContextFrame {
                context: crate::LlmContextData {
                    messages: vec![],
                    tools: crate::ToolSet::Direct(vec![function()]),
                    tool_choice: None,
                },
                speculation: false,
            },
        ));
        NativeRealtimeContextProducer::default()
            .attach(&mut frame, RealtimeContextIntent::Run, true)
            .unwrap();
        assert!(frame.native_realtime_context_cause().unwrap().is_some());
        if let FrameData::LlmContextDetailed(context) = &mut frame.data {
            context.context.tools = crate::ToolSet::Direct(vec![function()]);
        }
        assert!(frame.native_realtime_context_cause().is_err());
    }
    #[test]
    fn direct_tool_identity_bound_counts_repeated_entries_and_rejects_atomically() {
        let function = crate::DirectFunction {
            schema: crate::ToolDefinition {
                name: "echo".into(),
                description: String::new(),
                parameters: serde_json::json!({}),
            },
            handler: crate::LocalToolFunction::new(|value| async { Ok(value) }),
        };
        let context = |count| {
            Frame::new(FrameData::LlmContextDetailed(
                crate::DetailedLlmContextFrame {
                    context: crate::LlmContextData {
                        messages: vec![],
                        tools: crate::ToolSet::Direct(vec![function.clone(); count]),
                        tool_choice: None,
                    },
                    speculation: false,
                },
            ))
        };
        let producer = NativeRealtimeContextProducer::default();
        let mut oversized = context(MAX_REALTIME_CONTEXT_TOOL_IDENTITIES + 1);
        let before = oversized.clone();
        let error = producer
            .attach(&mut oversized, RealtimeContextIntent::Run, true)
            .unwrap_err();
        assert!(error.to_string().contains("4096 direct-tool identities"));
        assert_eq!(oversized, before);
        assert!(oversized.native_realtime_context_cause().unwrap().is_none());
        assert_eq!(producer.0.load(Ordering::SeqCst), 0);

        let mut exact = context(MAX_REALTIME_CONTEXT_TOOL_IDENTITIES);
        producer
            .attach(&mut exact, RealtimeContextIntent::Run, false)
            .unwrap();
        let cause = exact.native_realtime_context_cause().unwrap().unwrap();
        assert_eq!(cause.sequence(), 1);
        assert_eq!(
            cause.retained_tool_identity_count(),
            MAX_REALTIME_CONTEXT_TOOL_IDENTITIES
        );

        let mut empty = context(0);
        producer
            .attach(&mut empty, RealtimeContextIntent::Run, false)
            .unwrap();
        assert_eq!(
            empty
                .native_realtime_context_cause()
                .unwrap()
                .unwrap()
                .retained_tool_identity_count(),
            0
        );
    }
}
